use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::Arc,
    time::Duration,
};

use tokio::time::Instant;

use super::{OBSERVATION_SPACING, reservoir::Reservoir};

pub(super) const TIMER_RESOLUTION: Duration = Duration::from_millis(1);
const TIMER_MICROS: f64 = TIMER_RESOLUTION.as_micros() as f64;
/// The median, p95, and p99.
pub const QUANTILES: [f64; 3] = [0.5, 0.95, 0.99];
const HEDGE_TAIL_CAP: f64 = 1.25;
/// Samples a histogram retains, split evenly across its size bands.
const SAMPLES: usize = 1024;

/// Size bands are four times apart.
fn band(bytes: u64) -> u32 {
    bytes.ilog2() / 2
}

/// Latency quantiles of one size band, at the band's median size.
#[derive(Debug, Clone, Copy, Default)]
struct Point {
    band: u32,
    bytes: f64,
    micros: [f64; 3],
}

/// Latency quantiles by size: each band's quantiles, nondecreasing with size and joined by
/// straight segments.
#[derive(Debug, Clone)]
struct Curve {
    points: Arc<[Point]>,
    /// Below the smallest band, latency falls toward the timer resolution at zero bytes, as
    /// a transfer cost, rather than holding flat as a fixed cost.
    proportional: bool,
    smallest_bytes: u64,
    largest_bytes: u64,
}

impl Curve {
    /// Quantiles at a size. Beyond the largest observed size, the last segment's slope
    /// predicts larger reads, and none are predicted where latency does not grow measurably.
    fn micros(&self, bytes: u64) -> Option<[f64; 3]> {
        let points = &self.points[..];
        let size = bytes as f64;
        let first = points[0];
        let last = points[points.len() - 1];
        let origin = Point {
            micros: [TIMER_MICROS; 3],
            ..Point::default()
        };
        let (from, to) = match points {
            [_] if self.proportional => (origin, first),
            [_] => (first, first),
            _ if size <= first.bytes && self.proportional => (origin, first),
            _ if size <= first.bytes => (first, first),
            _ => points
                .windows(2)
                .find(|pair| size <= pair[1].bytes)
                .map_or((points[points.len() - 2], last), |pair| (pair[0], pair[1])),
        };
        let span = to.bytes - from.bytes;
        let slope = |quantile: usize| {
            if span > 0.0 {
                (to.micros[quantile] - from.micros[quantile]) / span
            } else {
                0.0
            }
        };
        if bytes > self.largest_bytes && slope(0) * self.largest_bytes as f64 <= TIMER_MICROS {
            return None;
        }
        let along = (size - to.bytes).max(from.bytes - to.bytes);
        let mut micros: [f64; 3] = std::array::from_fn(|quantile| {
            (to.micros[quantile] + slope(quantile) * along).max(TIMER_MICROS)
        });
        // Quantiles extrapolated along their own slopes must not cross.
        micros[1] = micros[1].max(micros[0]);
        micros[2] = micros[2].max(micros[1]);
        Some(micros)
    }
}

/// How observations compared with the model in use when each arrived.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Predictions {
    /// Observations at sizes the model predicted.
    pub observations: u64,
    pub above_median: u64,
    pub above_tail: u64,
}

/// A size band's fitted latency quantiles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BandLatency {
    /// The smallest size in the band; bands are four times apart.
    pub bytes: u64,
    /// The median, p95, and p99, as [`QUANTILES`] lists them.
    pub quantiles: [Duration; 3],
}

/// A fitted latency model, exported as metrics.
#[derive(Debug, Clone)]
pub struct LatencyEstimate {
    pub bands: Vec<BandLatency>,
    /// Largest retained size; larger sizes are predicted only where latency grows measurably.
    pub largest_bytes: u64,
    pub predictions: Predictions,
}

fn duration(micros: f64) -> Duration {
    Duration::try_from_secs_f64(micros.round() / 1_000_000.0).unwrap_or(Duration::MAX)
}

/// Median latency at a size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::object_store) enum Median {
    /// Within the range of retained sizes.
    Observed(Duration),
    /// Outside it: along the last segment where latency grows measurably, or else the
    /// median at the largest retained size, a lower bound since latency grows with size.
    /// Either way an estimate that can be far off, so it is not evidence about the bucket.
    Unobserved(Duration),
}

impl Median {
    pub fn value(self) -> Duration {
        match self {
            Self::Observed(median) | Self::Unobserved(median) => median,
        }
    }
}

/// Latency quantiles by size band.
#[derive(Debug, Clone, Default)]
pub(in crate::object_store) struct SizeModel(Option<Curve>);

impl SizeModel {
    /// None until measured.
    pub fn median(&self, bytes: u64) -> Option<Median> {
        let curve = self.0.as_ref()?;
        Some(match curve.micros(bytes) {
            Some(micros) if (curve.smallest_bytes..=curve.largest_bytes).contains(&bytes) => {
                Median::Observed(duration(micros[0]))
            }
            Some(micros) => Median::Unobserved(duration(micros[0])),
            None => Median::Unobserved(duration(curve.micros(curve.largest_bytes)?[0])),
        })
    }

    /// Uncapped p99.
    pub fn tail(&self, bytes: u64) -> Option<Duration> {
        self.0
            .as_ref()?
            .micros(bytes)
            .map(|micros| duration(micros[2]))
    }

    /// p99, capped at 1.25 times the p95. Where stalls exceed 1% of reads they set the p99,
    /// and the cap keeps hedges early; with ±20% jitter it is 1.5 times the median. It caps
    /// the p95 rather than the median because reads of one size can split between fast and
    /// slow objects, like S3 Standard's warm and cold ones, and a hedge cannot speed up the
    /// slow mode once it is 5% of reads.
    pub fn hedge_delay(&self, bytes: u64) -> Option<Duration> {
        self.0
            .as_ref()?
            .micros(bytes)
            .map(|micros| duration(micros[2].min(micros[1] * HEDGE_TAIL_CAP)))
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

/// Makes values nondecreasing by pooling adjacent violators into their weighted mean.
fn nondecreasing(values: &mut [f64], weights: &[f64]) {
    let mut blocks: Vec<(f64, f64, usize)> = Vec::with_capacity(values.len());
    for (&value, &weight) in values.iter().zip(weights) {
        blocks.push((value, weight, 1));
        while let [
            ..,
            (left, left_weight, left_len),
            (right, right_weight, right_len),
        ] = blocks[..]
            && left > right
        {
            blocks.truncate(blocks.len() - 2);
            let weight = left_weight + right_weight;
            blocks.push((
                (left * left_weight + right * right_weight) / weight,
                weight,
                left_len + right_len,
            ));
        }
    }
    let mut index = 0;
    for (value, _, len) in blocks {
        values[index..index + len].fill(value);
        index += len;
    }
}

/// Each band's median, p95, and p99, made nondecreasing across bands. A band's quantiles
/// come only from its own samples, so a slow mode at one size never stretches another's.
/// Its sizes span a factor of four, so its p95 and p99 are taken as multiples of the
/// median at each sample's size, lest the band's largest sizes set its tail.
fn curve(observations: &[Observation], proportional: bool, values: &mut Vec<(f64, f64)>) -> Curve {
    let bands: Vec<&[Observation]> = observations
        .chunk_by(|left, right| band(left.bytes) == band(right.bytes))
        .collect();
    let weights: Vec<f64> = bands
        .iter()
        .map(|samples| samples.iter().map(|sample| sample.weight).sum())
        .collect();
    let mut medians: Vec<f64> = bands
        .iter()
        .map(|samples| {
            values.clear();
            values.extend(samples.iter().map(|sample| (sample.micros, sample.weight)));
            quantile(values, QUANTILES[0])
        })
        .collect();
    nondecreasing(&mut medians, &weights);
    let mut curve = Curve {
        points: bands
            .iter()
            .zip(&medians)
            .map(|(samples, &median)| {
                values.clear();
                values.extend(
                    samples
                        .iter()
                        .map(|sample| (sample.bytes as f64, sample.weight)),
                );
                Point {
                    band: band(samples[0].bytes),
                    bytes: quantile(values, 0.5),
                    micros: [median; 3],
                }
            })
            .collect(),
        proportional,
        smallest_bytes: observations[0].bytes,
        largest_bytes: observations[observations.len() - 1].bytes,
    };
    let mut tails: [Vec<f64>; 2] = Default::default();
    for (samples, median) in bands.iter().zip(&medians) {
        values.clear();
        values.extend(samples.iter().map(|sample| {
            let expected = curve
                .micros(sample.bytes)
                .map_or(*median, |micros| micros[0]);
            (sample.micros / expected, sample.weight)
        }));
        for (tail, q) in tails.iter_mut().zip(&QUANTILES[1..]) {
            tail.push(median * quantile(values, *q));
        }
    }
    for tail in &mut tails {
        nondecreasing(tail, &weights);
    }
    curve.points = curve
        .points
        .iter()
        .zip(tails[0].iter().zip(&tails[1]))
        .map(|(point, (&p95, &p99))| {
            let p95 = p95.max(point.micros[0]);
            Point {
                micros: [point.micros[0], p95, p99.max(p95)],
                ..*point
            }
        })
        .collect();
    curve
}

/// Latency observations by size. Size strata retain rare sizes for one continuous model.
///
/// Model time follows the clock but advances at most [`OBSERVATION_SPACING`] per
/// observation, so sparse traffic keeps enough observations for a tail estimate.
#[derive(Debug)]
pub(super) struct SizeHistogram {
    /// Body time falls toward zero below the smallest band, as a transfer cost.
    proportional: bool,
    bands: BTreeMap<u32, Reservoir<Measurement>>,
    clock: f64,
    observed_at: Option<Instant>,
    updates: u64,
    summarized_updates: u64,
    model: SizeModel,
    fitted_updates: u64,
    predictions: Predictions,
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
            predictions: Predictions::default(),
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
        let band = band(bytes);
        if let Entry::Vacant(entry) = self.bands.entry(band) {
            entry.insert(Reservoir::new(0));
            self.rebalance();
        }
        let micros = (latency.as_secs_f64() * 1_000_000.0).max(TIMER_MICROS);
        if let Some([median, _, tail]) = self.model.0.as_ref().and_then(|curve| curve.micros(bytes))
        {
            self.predictions.observations += 1;
            self.predictions.above_median += u64::from(micros > median);
            self.predictions.above_tail += u64::from(micros > tail);
        }
        if let Some(reservoir) = self.bands.get_mut(&band) {
            reservoir.update(self.clock, Measurement { bytes, micros });
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
        self.model.clone()
    }

    pub fn estimate(&self) -> Option<LatencyEstimate> {
        self.model.0.as_ref().map(|curve| LatencyEstimate {
            bands: curve
                .points
                .iter()
                .map(|point| BandLatency {
                    bytes: 1 << (2 * point.band),
                    quantiles: point.micros.map(duration),
                })
                .collect(),
            largest_bytes: curve.largest_bytes,
            predictions: self.predictions,
        })
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
        self.model.clone()
    }
}

/// Observations copied for fitting without access to their histogram.
pub(super) struct Refit {
    observations: Vec<Observation>,
    proportional: bool,
    updates: u64,
}

#[derive(Debug)]
pub(super) struct Fitted {
    model: SizeModel,
    updates: u64,
}

impl Refit {
    pub fn fit(self) -> Fitted {
        let Self {
            mut observations,
            proportional,
            updates,
        } = self;
        // Reservoirs return samples in random order; a fixed order makes rounding depend
        // only on which samples were kept, and groups each size band's samples.
        observations.sort_unstable_by(|left, right| {
            left.bytes
                .cmp(&right.bytes)
                .then(left.micros.total_cmp(&right.micros))
                .then(left.weight.total_cmp(&right.weight))
        });
        let model =
            (!observations.is_empty()).then(|| curve(&observations, proportional, &mut Vec::new()));
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
        Median, Observation, Predictions, SizeHistogram, TIMER_RESOLUTION, curve, nondecreasing,
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
            Some(Median::Observed(Duration::from_millis(41)))
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
        assert_eq!(model.hedge_delay(MIB), Some(Duration::from_micros(13_750)));
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
            Some(Median::Observed(Duration::from_millis(21)))
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
            Some(Median::Observed(Duration::from_millis(21)))
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
            Some(Median::Observed(Duration::from_millis(30)))
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
            Some(Median::Unobserved(Duration::from_millis(25)))
        );
        assert_eq!(
            model.median(16 * MIB),
            Some(Median::Observed(Duration::from_millis(25)))
        );
        assert_eq!(
            model.median(32 * MIB),
            Some(Median::Unobserved(Duration::from_millis(25)))
        );
    }

    /// Buffered bodies of one size, or fixed overhead across sizes, show no growth with size.
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

    /// A stall at a rarely read size holds that size's estimate, not smaller sizes', until
    /// more reads of the size arrive or it ages out.
    #[tokio::test(start_paused = true)]
    async fn a_stall_at_a_rare_size_holds_only_that_size_until_it_ages_out() {
        let mut histogram = body();
        for _ in 0..50 {
            update(&mut histogram, MIB, 11);
        }
        update(&mut histogram, 16 * MIB, 1001);
        let model = histogram.summary(true);
        assert_eq!(
            model.median(MIB),
            Some(Median::Observed(Duration::from_millis(11)))
        );
        assert_eq!(model.hedge_delay(MIB), Some(Duration::from_millis(11)));
        assert_eq!(model.tail(16 * MIB), Some(Duration::from_millis(1001)));
        for _ in 0..800 {
            advance(Duration::from_secs(1)).await;
            update(&mut histogram, MIB, 11);
        }
        let model = histogram.summary(true);
        assert_eq!(model.tail(MIB), Some(Duration::from_millis(11)));
        assert_eq!(
            model.median(16 * MIB),
            Some(Median::Unobserved(Duration::from_millis(161)))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn observations_count_against_the_predictions_in_use() {
        let mut histogram = SizeHistogram::new(false);
        for millis in [10; 98].into_iter().chain([30; 2]) {
            update(&mut histogram, MIB, millis);
        }
        let model = histogram.summary(true);
        assert_eq!(model.tail(MIB), Some(Duration::from_millis(30)));
        for (bytes, millis) in [(MIB, 5), (MIB, 20), (MIB, 40), (16 * MIB, 40)] {
            update(&mut histogram, bytes, millis);
        }
        assert_eq!(
            histogram.estimate().unwrap().predictions,
            Predictions {
                observations: 3,
                above_median: 2,
                above_tail: 1,
            }
        );
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

    fn fitted(samples: &[(u64, u64)], proportional: bool) -> super::SizeModel {
        let mut histogram = SizeHistogram::new(proportional);
        for &(bytes, millis) in samples {
            update(&mut histogram, bytes, millis);
        }
        histogram.summary(true)
    }

    /// Latency that steps up with size and then levels off, as S3 Standard's cold objects
    /// do, is not extended as a line through the step.
    #[tokio::test(start_paused = true)]
    async fn a_step_in_latency_is_not_extrapolated_as_a_line() {
        let samples = [
            (64 * 1024, 30),
            (256 * 1024, 32),
            (MIB, 110),
            (4 * MIB, 112),
        ]
        .repeat(30);
        let model = fitted(&samples, false);
        let median = |bytes| model.median(bytes).unwrap().value();
        assert!(median(64 * 1024) <= Duration::from_millis(31));
        assert!(median(4 * MIB) <= Duration::from_millis(113));
        assert!(
            median(16 * MIB) <= Duration::from_millis(120),
            "{:?}",
            median(16 * MIB)
        );
    }

    /// When reads of one size split between warm and cold objects, a hedge cannot speed up
    /// the cold ones, so it waits past them.
    #[tokio::test(start_paused = true)]
    async fn sizes_split_between_warm_and_cold_hedge_after_the_cold_mode() {
        let samples = [(MIB, 30); 60]
            .into_iter()
            .chain([(MIB, 110); 40])
            .chain([(64 * 1024, 30); 100])
            .collect::<Vec<_>>();
        let model = fitted(&samples, false);
        assert!(model.hedge_delay(MIB).unwrap() >= Duration::from_millis(110));
        assert!(model.hedge_delay(64 * 1024).unwrap() <= Duration::from_millis(45));
    }

    /// Capacitor reads a range before sending headers, so its time to first byte grows with
    /// size. A band spans sizes four times apart, and where latency is set by size alone,
    /// its tail follows its median rather than its largest sizes.
    #[tokio::test(start_paused = true)]
    async fn latency_set_by_size_alone_has_no_tail() {
        let samples: Vec<_> = (4..=64)
            .map(|quarters| (quarters * MIB / 4, 1 + quarters))
            .collect();
        let model = fitted(&samples, false);
        for bytes in [2 * MIB, 6 * MIB, 10 * MIB, 16 * MIB] {
            let median = model.median(bytes).unwrap().value();
            assert!(
                model.tail(bytes).unwrap() <= median.mul_f64(1.05),
                "{bytes}: {:?} over a {median:?} median",
                model.tail(bytes)
            );
        }
        assert_eq!(
            model.median(16 * MIB),
            Some(Median::Observed(Duration::from_millis(65)))
        );
    }

    proptest! {
        #[test]
        fn pooling_makes_values_nondecreasing_and_keeps_their_weighted_mean(
            samples in collection::vec((0.0_f64..1_000.0, 0.1_f64..10.0), 1..20),
        ) {
            let (mut values, weights): (Vec<_>, Vec<_>) = samples.into_iter().unzip();
            let mean = |values: &[f64]| {
                values.iter().zip(&weights).map(|(value, weight)| value * weight).sum::<f64>()
            };
            let before = mean(&values);
            nondecreasing(&mut values, &weights);
            prop_assert!(values.windows(2).all(|pair| pair[0] <= pair[1] + 1e-9), "{values:?}");
            prop_assert!((mean(&values) - before).abs() <= 1e-6 * before.max(1.0));
        }

        #[test]
        fn quantiles_never_decrease_with_size_or_cross(
            samples in collection::vec((1_u64..=16 * MIB, 1_000_u32..=1_000_000, 1_u8..=20), 1..60),
            sizes in collection::vec(1_u64..=32 * MIB, 1..20),
            proportional: bool,
        ) {
            let mut observations: Vec<_> = samples.iter().map(|&(bytes, micros, weight)| Observation {
                bytes,
                micros: f64::from(micros),
                weight: f64::from(weight),
            }).collect();
            observations.sort_unstable_by_key(|sample| sample.bytes);
            let curve = curve(&observations, proportional, &mut Vec::new());
            let mut sizes = sizes;
            sizes.sort_unstable();
            let mut previous = [0.0; 3];
            for bytes in sizes {
                let Some(micros) = curve.micros(bytes) else { break };
                prop_assert!(micros[0] <= micros[1] && micros[1] <= micros[2], "{micros:?} at {bytes}");
                for (value, before) in micros.iter().zip(previous) {
                    prop_assert!(*value + 1e-6 >= before, "{micros:?} after {previous:?} at {bytes}");
                }
                previous = micros;
            }
        }
    }
}
