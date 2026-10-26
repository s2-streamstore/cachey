use std::{
    collections::{BTreeMap, btree_map::Entry},
    time::Duration,
};

use tokio::time::Instant;

use super::{OBSERVATION_SPACING, reservoir::Reservoir};

pub(super) const TIMER_RESOLUTION: Duration = Duration::from_millis(1);
const TIMER_MICROS: f64 = TIMER_RESOLUTION.as_micros() as f64;
const FIT_PRECISION_MICROS: f64 = 0.25;
const TAIL_QUANTILE: f64 = 0.99;
const HEDGE_TAIL_CAP: f64 = 1.5;
/// Samples a histogram retains, split evenly across its size bands.
const SAMPLES: usize = 1024;

/// Median latency by size; never below timer resolution.
#[derive(Debug, Clone, Copy)]
struct Line {
    fixed: f64,
    per_byte: f64,
}

impl Line {
    fn micros(self, bytes: u64) -> f64 {
        (self.fixed + self.per_byte * bytes as f64).max(TIMER_MICROS)
    }
}

#[derive(Debug, Clone, Copy)]
struct Estimate {
    median: Line,
    /// p99 latency as a multiple of the median, at least 1.
    tail_ratio: f64,
    smallest_bytes: u64,
    largest_bytes: u64,
}

/// A fitted latency model, exported as metrics.
#[derive(Debug, Clone, Copy)]
pub struct LatencyEstimate {
    /// Median latency of a zero-byte request.
    pub base: Duration,
    /// Median latency added per byte, in seconds.
    pub seconds_per_byte: f64,
    /// p99 latency as a multiple of the median.
    pub tail_ratio: f64,
    /// Largest retained size; larger sizes are predicted only from a measurable per-byte cost.
    pub largest_bytes: u64,
}

fn duration(micros: f64) -> Duration {
    Duration::try_from_secs_f64(micros.round() / 1_000_000.0).unwrap_or(Duration::MAX)
}

/// Median latency at a size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::object_store) enum Median {
    /// Within the range of retained sizes.
    Predicted(Duration),
    /// Outside the range of retained sizes, along the median line: an estimate that can be
    /// far off, so it is not evidence about the bucket.
    Extrapolated(Duration),
    /// Beyond the model's reach, the median at its largest retained size: latency grows
    /// with size, so this bounds it from below.
    AtLeast(Duration),
}

impl Median {
    pub fn value(self) -> Duration {
        match self {
            Self::Predicted(median) | Self::Extrapolated(median) | Self::AtLeast(median) => median,
        }
    }
}

/// Latency by size: a median line and a tail ratio shared by all sizes.
#[derive(Debug, Clone, Copy, Default)]
pub(in crate::object_store) struct SizeModel(Option<Estimate>);

impl SizeModel {
    fn micros(&self, bytes: u64) -> Option<(f64, f64)> {
        let estimate = self.0?;
        if bytes > estimate.largest_bytes
            && estimate.median.per_byte * estimate.largest_bytes as f64 <= TIMER_MICROS
        {
            return None;
        }
        let median = estimate.median.micros(bytes);
        Some((median, median * estimate.tail_ratio))
    }

    /// None until measured.
    pub fn median(&self, bytes: u64) -> Option<Median> {
        let estimate = self.0?;
        Some(match self.micros(bytes) {
            Some((median, _))
                if (estimate.smallest_bytes..=estimate.largest_bytes).contains(&bytes) =>
            {
                Median::Predicted(duration(median))
            }
            Some((median, _)) => Median::Extrapolated(duration(median)),
            None => Median::AtLeast(duration(estimate.median.micros(estimate.largest_bytes))),
        })
    }

    /// Uncapped p99.
    pub fn tail(&self, bytes: u64) -> Option<Duration> {
        self.micros(bytes).map(|(_, tail)| duration(tail))
    }

    /// p99, capped at 1.5 times the median.
    pub fn hedge_delay(&self, bytes: u64) -> Option<Duration> {
        self.micros(bytes)
            .map(|(median, tail)| duration(tail.min(median * HEDGE_TAIL_CAP)))
    }

    pub fn estimate(&self) -> Option<LatencyEstimate> {
        self.0.map(|estimate| LatencyEstimate {
            base: duration(estimate.median.fixed),
            seconds_per_byte: estimate.median.per_byte / 1_000_000.0,
            tail_ratio: estimate.tail_ratio,
            largest_bytes: estimate.largest_bytes,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct Measurement {
    bytes: u64,
    micros: f64,
}

#[derive(Debug, Clone, Copy)]
struct Observation {
    bytes: u64,
    micros: f64,
    weight: f64,
}

struct Fit {
    line: Line,
    loss: f64,
    gradient: f64,
}

impl Fit {
    fn at(observations: &[Observation], per_byte: f64, values: &mut Vec<(f64, f64)>) -> Self {
        values.clear();
        values.extend(observations.iter().map(|sample| {
            (
                sample.micros - per_byte * sample.bytes as f64,
                sample.weight,
            )
        }));
        let fixed = quantile(values, 0.5).max(0.0);
        let line = Line { fixed, per_byte };
        let mut loss = 0.0;
        let (mut below_weight, mut above_weight, mut tied_weight) = (0.0, 0.0, 0.0);
        let (mut below_bytes, mut above_bytes, mut tied_bytes) = (0.0, 0.0, 0.0);
        for sample in observations {
            let bytes = sample.bytes as f64;
            let residual = sample.micros - per_byte * bytes - fixed;
            loss += sample.weight * residual.abs();
            if residual < 0.0 {
                below_weight += sample.weight;
                below_bytes += sample.weight * bytes;
            } else if residual > 0.0 {
                above_weight += sample.weight;
                above_bytes += sample.weight * bytes;
            } else {
                tied_weight += sample.weight;
                tied_bytes += sample.weight * bytes;
            }
        }
        // Balance the intercept's subgradient at tied samples, respecting the zero boundary.
        let tied = if tied_weight > 0.0 {
            ((above_weight - below_weight) / tied_weight).clamp(-1.0, 1.0)
        } else {
            0.0
        };
        Fit {
            line,
            loss,
            gradient: below_bytes - above_bytes + tied * tied_bytes,
        }
    }
}

/// Least absolute deviations with nonnegative coefficients. For each slope the optimal
/// intercept is a weighted median of residuals, and the loss is convex in the slope.
/// Nonnegative fixed cost bounds the slope by the zero-intercept fit.
/// Supporting lines bracket the minimum; each step discards at least a quarter of the bracket.
fn median_line(
    observations: &[Observation],
    largest_bytes: u64,
    proportional: bool,
    values: &mut Vec<(f64, f64)>,
) -> Line {
    values.clear();
    values.extend(observations.iter().map(|sample| {
        (
            sample.micros / sample.bytes as f64,
            sample.weight * sample.bytes as f64,
        )
    }));
    let upper = quantile(values, 0.5);
    let bytes = observations[0].bytes;
    if observations.iter().all(|sample| sample.bytes == bytes) {
        return if proportional {
            Line {
                fixed: TIMER_MICROS,
                per_byte: (upper - TIMER_MICROS / bytes as f64).max(0.0),
            }
        } else {
            Line {
                fixed: upper * bytes as f64,
                per_byte: 0.0,
            }
        };
    }
    let largest_bytes = largest_bytes as f64;
    // Below this loss, every retained sample fits within the histogram's time precision.
    let resolved_loss = FIT_PRECISION_MICROS
        * observations
            .iter()
            .map(|sample| sample.weight)
            .fold(f64::INFINITY, f64::min);
    let mut evaluate = |per_byte| Fit::at(observations, per_byte, values);
    let mut lower = evaluate(0.0);
    if lower.gradient >= 0.0 {
        return lower.line;
    }
    let mut upper = evaluate(upper);
    if upper.gradient <= 0.0 {
        return upper.line;
    }
    loop {
        let width = upper.line.per_byte - lower.line.per_byte;
        let fraction = (((lower.loss - upper.loss) / width + upper.gradient)
            / (upper.gradient - lower.gradient))
            .clamp(0.25, 0.75);
        let per_byte = lower.line.per_byte + fraction * width;
        let fitted = evaluate(per_byte);
        if fitted.gradient == 0.0
            || fitted.loss <= resolved_loss
            || width * largest_bytes <= FIT_PRECISION_MICROS
            || per_byte <= lower.line.per_byte
            || per_byte >= upper.line.per_byte
        {
            return fitted.line;
        }
        if fitted.gradient > 0.0 {
            upper = fitted;
        } else {
            lower = fitted;
        }
    }
}

/// Latency observations by size. Size strata retain rare sizes for one continuous model.
///
/// Model time follows the clock but advances at most [`OBSERVATION_SPACING`] per
/// observation, so sparse traffic keeps enough observations for a tail estimate.
#[derive(Debug)]
pub(super) struct SizeHistogram {
    /// One observed size fits a transfer cost rather than a fixed cost.
    proportional: bool,
    bands: BTreeMap<u32, Reservoir<Measurement>>,
    clock: f64,
    observed_at: Option<Instant>,
    updates: u64,
    summarized_updates: u64,
    model: SizeModel,
    fitted_updates: u64,
}

impl SizeHistogram {
    pub fn new(proportional: bool) -> Self {
        Self {
            proportional,
            bands: BTreeMap::new(),
            clock: 0.0,
            observed_at: None,
            updates: 0,
            summarized_updates: 0,
            model: SizeModel::default(),
            fitted_updates: 0,
        }
    }

    fn band_capacity(&self) -> usize {
        #[cfg(test)]
        if let Some(capacity) = crate::object_store::simulation::histogram_capacity() {
            return capacity;
        }
        SAMPLES / self.bands.len().max(1)
    }

    fn rebalance(&mut self) {
        let capacity = self.band_capacity();
        for band in self.bands.values_mut() {
            band.set_capacity(capacity);
        }
    }

    pub fn update(&mut self, now: Instant, bytes: u64, latency: Duration) {
        if bytes == 0 {
            return;
        }
        if let Some(observed_at) = self.observed_at {
            self.clock += now
                .saturating_duration_since(observed_at)
                .min(OBSERVATION_SPACING)
                .as_secs_f64();
        }
        self.observed_at = Some(now);
        let band = bytes.ilog2() / 2;
        if let Entry::Vacant(entry) = self.bands.entry(band) {
            entry.insert(Reservoir::new(0));
            self.rebalance();
        }
        if let Some(reservoir) = self.bands.get_mut(&band) {
            reservoir.update(
                self.clock,
                Measurement {
                    bytes,
                    micros: (latency.as_secs_f64() * 1_000_000.0).max(TIMER_MICROS),
                },
            );
        }
        self.updates += 1;
    }

    #[cfg(test)]
    pub fn retention(&self) -> impl Iterator<Item = (u64, usize)> + '_ {
        self.bands.values().map(Reservoir::retention)
    }

    /// Observations the current model was fit from.
    #[cfg(test)]
    pub fn fitted_updates(&self) -> u64 {
        self.fitted_updates
    }

    /// Observations to refit once `due`, or as soon as the observation count doubles.
    pub fn refit(&mut self, due: bool) -> Option<Refit> {
        if self.updates == self.summarized_updates
            || !(due || self.updates >= self.summarized_updates.saturating_mul(2))
        {
            return None;
        }
        self.summarized_updates = self.updates;
        let clock = self.clock;
        let observations = self
            .bands
            .values_mut()
            .flat_map(|band| band.weighted(clock))
            .map(|(sample, weight)| Observation {
                bytes: sample.bytes,
                micros: sample.micros,
                weight,
            })
            .collect();
        let bands = self.bands.len();
        self.bands.retain(|_, band| !band.is_empty());
        if self.bands.len() < bands {
            self.rebalance();
        }
        Some(Refit {
            observations,
            proportional: self.proportional,
            updates: self.updates,
        })
    }

    pub fn model(&self) -> SizeModel {
        self.model
    }

    /// Keeps the fit of the most observations when refits finish out of order.
    pub fn store(&mut self, fitted: Fitted) {
        if fitted.updates > self.fitted_updates {
            self.model = fitted.model;
            self.fitted_updates = fitted.updates;
        }
    }

    #[cfg(test)]
    pub fn summary(&mut self, due: bool) -> SizeModel {
        if let Some(refit) = self.refit(due) {
            self.store(refit.fit());
        }
        self.model
    }
}

/// Observations copied for fitting without access to their histogram.
pub(super) struct Refit {
    observations: Vec<Observation>,
    proportional: bool,
    updates: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Fitted {
    model: SizeModel,
    updates: u64,
}

impl Refit {
    /// The tail ratio is the p99 quantile regression of latency on the median line. Weighting
    /// ratios by their median keeps small requests' constant stalls from scaling large ones.
    pub fn fit(self) -> Fitted {
        let Self {
            mut observations,
            proportional,
            updates,
        } = self;
        // Reservoirs return samples in random order; a fixed order makes rounding depend
        // only on which samples were kept.
        observations.sort_unstable_by(|left, right| {
            left.bytes
                .cmp(&right.bytes)
                .then(left.micros.total_cmp(&right.micros))
                .then(left.weight.total_cmp(&right.weight))
        });
        let model = observations
            .first()
            .zip(observations.last())
            .map(|(smallest, largest)| (smallest.bytes, largest.bytes))
            .map(|(smallest_bytes, largest_bytes)| {
                let mut values = Vec::with_capacity(observations.len());
                let median = median_line(&observations, largest_bytes, proportional, &mut values);
                values.clear();
                values.extend(observations.iter().map(|sample| {
                    let median = median.micros(sample.bytes);
                    (sample.micros / median, sample.weight * median)
                }));
                Estimate {
                    median,
                    tail_ratio: quantile(&mut values, TAIL_QUANTILE).max(1.0),
                    smallest_bytes,
                    largest_bytes,
                }
            });
        Fitted {
            model: SizeModel(model),
            updates,
        }
    }
}

/// Weighted nearest-rank quantile, including the selected value's own weight.
pub(super) fn quantile(mut values: &mut [(f64, f64)], q: f64) -> f64 {
    let mut target = values.iter().map(|(_, weight)| weight).sum::<f64>() * (q - 1e-9);
    while !values.is_empty() {
        let middle = (values.len() - 1) / 2;
        let (below, pivot, above) =
            values.select_nth_unstable_by(middle, |left, right| left.0.total_cmp(&right.0));
        let weight_below: f64 = below.iter().map(|(_, weight)| weight).sum();
        if target <= weight_below && !below.is_empty() {
            values = below;
        } else if target <= weight_below + pivot.1 || above.is_empty() {
            return pivot.0;
        } else {
            target -= weight_below + pivot.1;
            values = above;
        }
    }
    0.0
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytesize::MIB;
    use proptest::{collection, prop_assert, proptest};
    use tokio::time::{Instant, advance};

    use super::{
        Line, Median, Observation, SizeHistogram, TIMER_MICROS, TIMER_RESOLUTION, median_line,
        quantile,
    };

    fn body() -> SizeHistogram {
        SizeHistogram::new(true)
    }

    fn update(histogram: &mut SizeHistogram, bytes: u64, millis: u64) {
        histogram.update(Instant::now(), bytes, Duration::from_millis(millis));
    }

    #[tokio::test(start_paused = true)]
    async fn tiny_buffered_reads_do_not_erase_a_measured_transfer() {
        let mut histogram = body();
        update(&mut histogram, 4 * MIB, 41);
        for _ in 0..5000 {
            update(&mut histogram, 1024, 0);
        }
        let model = histogram.summary(true);
        assert_eq!(model.tail(1), Some(TIMER_RESOLUTION));
        assert_eq!(
            model.median(4 * MIB),
            Some(Median::Predicted(Duration::from_millis(41)))
        );
        assert!(model.tail(16 * MIB).unwrap() <= Duration::from_millis(162));
    }

    #[tokio::test(start_paused = true)]
    async fn idle_time_preserves_history_until_newer_observations_replace_it() {
        let mut histogram = body();
        for _ in 0..50 {
            update(&mut histogram, MIB, 11);
        }
        update(&mut histogram, MIB, 101);
        let model = histogram.summary(true);
        assert_eq!(model.tail(MIB), Some(Duration::from_millis(101)));
        assert_eq!(model.hedge_delay(MIB), Some(Duration::from_micros(16_500)));
        advance(Duration::from_secs(300)).await;
        update(&mut histogram, MIB, 21);
        assert_eq!(
            histogram.summary(true).tail(MIB),
            Some(Duration::from_millis(101))
        );
        for _ in 0..200 {
            advance(Duration::from_secs(1)).await;
            update(&mut histogram, MIB, 21);
        }
        let model = histogram.summary(true);
        assert_eq!(
            model.median(MIB),
            Some(Median::Predicted(Duration::from_millis(21)))
        );
        assert_eq!(model.tail(MIB), Some(Duration::from_millis(21)));
    }

    #[tokio::test(start_paused = true)]
    async fn busy_traffic_decays_with_the_clock() {
        let mut histogram = body();
        for _ in 0..1000 {
            advance(Duration::from_millis(10)).await;
            update(&mut histogram, MIB, 11);
        }
        for _ in 0..600 {
            advance(Duration::from_millis(100)).await;
            update(&mut histogram, MIB, 21);
        }
        let model = histogram.summary(true);
        assert_eq!(
            model.median(MIB),
            Some(Median::Predicted(Duration::from_millis(21)))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_refit_finishing_late_does_not_replace_a_newer_model() {
        let mut histogram = body();
        update(&mut histogram, MIB, 10);
        let older = histogram.refit(true).unwrap();
        for _ in 0..2 {
            update(&mut histogram, MIB, 30);
        }
        let newer = histogram.refit(true).unwrap();
        histogram.store(newer.fit());
        histogram.store(older.fit());
        assert_eq!(
            histogram.model().median(MIB),
            Some(Median::Predicted(Duration::from_millis(30)))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn size_bands_share_one_sample_budget() {
        let mut histogram = body();
        for _ in 0..2000 {
            update(&mut histogram, MIB, 10);
        }
        let retained = |histogram: &SizeHistogram| {
            histogram
                .retention()
                .map(|(_, retained)| retained)
                .collect::<Vec<_>>()
        };
        assert_eq!(retained(&histogram), [1024]);
        for _ in 0..2000 {
            update(&mut histogram, 4096, 1);
        }
        assert_eq!(retained(&histogram), [512, 512]);
    }

    #[tokio::test(start_paused = true)]
    async fn one_request_size_is_a_fixed_cost_outside_transfers() {
        let mut histogram = SizeHistogram::new(false);
        update(&mut histogram, 16 * MIB, 25);
        let model = histogram.summary(false);
        assert_eq!(
            model.median(4096),
            Some(Median::Extrapolated(Duration::from_millis(25)))
        );
        assert_eq!(
            model.median(16 * MIB),
            Some(Median::Predicted(Duration::from_millis(25)))
        );
        assert_eq!(
            model.median(32 * MIB),
            Some(Median::AtLeast(Duration::from_millis(25)))
        );
    }

    /// Buffered bodies of one size, or fixed overhead across sizes, measure no cost per byte.
    #[tokio::test(start_paused = true)]
    async fn flat_history_does_not_establish_larger_transfer_times() {
        for history in [&[(3000, 0)][..], &[(4096, 20), (65_536, 20)]] {
            let mut histogram = body();
            for &(bytes, millis) in history {
                for _ in 0..4 {
                    update(&mut histogram, bytes, millis);
                }
            }
            let model = histogram.summary(true);
            let (largest, millis) = history[history.len() - 1];
            assert_eq!(
                model.tail(largest),
                Some(Duration::from_millis(millis.max(1)))
            );
            assert!(model.tail(largest + 1).is_none(), "{history:?}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn empty_bodies_and_extreme_measurements_are_bounded() {
        let mut histogram = body();
        update(&mut histogram, 0, 100);
        assert!(histogram.summary(false).tail(0).is_none());
        for bytes in [1, u64::MAX] {
            histogram.update(Instant::now(), bytes, Duration::MAX);
        }
        assert!(histogram.summary(true).tail(u64::MAX).is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn isolated_large_stalls_borrow_history_and_age_out_with_other_sizes() {
        let mut histogram = body();
        for _ in 0..50 {
            update(&mut histogram, MIB, 11);
        }
        update(&mut histogram, 16 * MIB, 1001);
        let model = histogram.summary(true);
        assert_eq!(
            model.median(MIB),
            Some(Median::Predicted(Duration::from_millis(11)))
        );
        // With a nonnegative intercept, extrapolation grows no faster than response size.
        assert!(model.hedge_delay(16 * MIB).unwrap() <= Duration::from_millis(265));
        for _ in 0..800 {
            advance(Duration::from_secs(1)).await;
            update(&mut histogram, MIB, 11);
        }
        assert_eq!(
            histogram.summary(true).tail(MIB),
            Some(Duration::from_millis(11))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_rare_large_stall_does_not_become_the_body_median() {
        let mut histogram = body();
        for (bytes, body_micros, count) in [
            (1024, 20_010, 100),
            (65_536, 20_625, 10),
            (16 * MIB, 180_000, 1),
            (16 * MIB, 800_000, 1),
        ] {
            for _ in 0..count {
                histogram.update(Instant::now(), bytes, Duration::from_micros(body_micros));
            }
        }
        let model = histogram.summary(true);
        let near = |actual: Option<Duration>, expected: Duration, tolerance_micros| {
            actual.unwrap().abs_diff(expected) <= Duration::from_micros(tolerance_micros)
        };
        assert!(near(
            model.median(16 * MIB).map(Median::value),
            Duration::from_millis(180),
            1
        ));
        assert!(near(model.tail(16 * MIB), Duration::from_millis(800), 1));
        assert!(near(
            model.hedge_delay(16 * MIB),
            Duration::from_millis(270),
            2
        ));
    }

    #[test]
    fn quantiles_take_the_nearest_rank_including_the_selected_weight() {
        let mut values = [5, 1, 3, 3, 3, 9, 7, 7, 2, 8].map(|value| (f64::from(value), 1.0));
        for (q, expected) in [
            (0.0, 1),
            (0.1, 1),
            (0.15, 2),
            (0.25, 3),
            (0.5, 3),
            (0.55, 5),
            (0.6, 5),
            (0.65, 7),
            (0.8, 7),
            (0.85, 8),
            (0.9, 8),
            (0.95, 9),
            (0.99, 9),
            (1.0, 9),
        ] {
            assert_eq!(quantile(&mut values, q), f64::from(expected), "{q}");
        }
        // 0.4 × 0.75 rounds above the first weight in floating point.
        assert_eq!(quantile(&mut [(1.0, 0.3), (2.0, 0.1)], 0.75), 1.0);
    }

    proptest! {
        #[test]
        fn median_lines_match_exhaustive_least_absolute_deviations(
            samples in collection::vec((1_u64..=16 * MIB, 0_u32..=1_000_000, 1_u8..=20, 0_u8..=10), 2..12),
            proportional: bool,
        ) {
            let observations: Vec<_> = samples.iter().map(|&(bytes, micros, weight, age)| Observation {
                bytes: if age % 2 == 0 { bytes.next_power_of_two() } else { bytes },
                micros: f64::from(micros).max(TIMER_MICROS),
                weight: f64::from(weight) * 2.0_f64.powi(-i32::from(age)),
            }).collect();
            // A piecewise-linear loss attains a minimum where two sample lines intersect,
            // or where one sample line meets a nonnegative coefficient boundary.
            let mut candidates = Vec::new();
            for (index, left) in observations.iter().enumerate() {
                candidates.extend([
                    Line { fixed: left.micros, per_byte: 0.0 },
                    Line { fixed: 0.0, per_byte: left.micros / left.bytes as f64 },
                ]);
                for right in &observations[index + 1..] {
                    if left.bytes != right.bytes {
                        let per_byte = (right.micros - left.micros)
                            / (right.bytes as f64 - left.bytes as f64);
                        let fixed = left.micros - per_byte * left.bytes as f64;
                        if fixed >= 0.0 && per_byte >= 0.0 {
                            candidates.push(Line { fixed, per_byte });
                        }
                    }
                }
            }
            let loss = |line: Line| observations.iter().map(|sample| {
                sample.weight * (sample.micros - line.fixed - line.per_byte * sample.bytes as f64).abs()
            }).sum::<f64>();
            let optimal = candidates.iter().copied().map(loss).fold(f64::INFINITY, f64::min);
            let tolerance: f64 = observations.iter().map(|sample| sample.weight).sum();
            let largest_bytes = observations.iter().map(|sample| sample.bytes).max().unwrap();
            let fitted = median_line(&observations, largest_bytes, proportional, &mut Vec::new());
            prop_assert!(fitted.fixed.is_finite() && fitted.fixed >= 0.0);
            prop_assert!(fitted.per_byte.is_finite() && fitted.per_byte >= 0.0);
            prop_assert!(loss(fitted) <= optimal + tolerance, "{fitted:?}, loss={}, optimal={optimal}", loss(fitted));
        }
    }
}
