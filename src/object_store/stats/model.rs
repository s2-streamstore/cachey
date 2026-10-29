use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::Arc,
    time::Duration,
};

use tokio::time::Instant;

use super::{OBSERVATION_SPACING, reservoir::Reservoir};

pub(super) const TIMER_RESOLUTION: Duration = Duration::from_millis(1);
const TIMER_MICROS: f64 = TIMER_RESOLUTION.as_micros() as f64;
pub const QUANTILES: [f64; 3] = [0.5, 0.95, 0.99];
const HEDGE_TAIL_CAP: f64 = 1.25;
/// S3 Standard's cold objects take about four times as long as its warm ones.
const HEDGE_MEDIAN_CAP: f64 = 5.0;
/// Samples a histogram retains, split evenly across its size bands.
const SAMPLES: usize = 1024;

/// Size bands are four times apart.
fn band(bytes: u64) -> u32 {
    bytes.ilog2() / 2
}

fn band_bytes(band: u32) -> u64 {
    1 << (2 * band)
}

/// Read sizes at which models report latency: 16 times apart, so fewer than their bands.
pub const REPORTED_SIZES: [u64; 4] = [4 << 10, 64 << 10, 1 << 20, 16 << 20];

/// A band's median, p95, and p99 at its median size.
#[derive(Debug, Clone, Copy)]
struct Point {
    bytes: f64,
    micros: [f64; 3],
}

/// Latency by size: quantiles joined by straight segments and flat below the first point.
/// Beyond the last, the median grows at `slope` and the tails keep their ratios to it.
#[derive(Debug, Clone)]
struct Curve {
    points: Arc<[Point]>,
    slope: f64,
    smallest_bytes: u64,
    largest_bytes: u64,
}

impl Curve {
    /// None beyond the largest size where the median does not grow measurably there.
    fn micros(&self, bytes: u64) -> Option<[f64; 3]> {
        if bytes > self.largest_bytes && self.slope * self.largest_bytes as f64 <= TIMER_MICROS {
            return None;
        }
        Some(interpolate(&self.points, self.slope, bytes as f64))
    }
}

fn interpolate(points: &[Point], slope: f64, size: f64) -> [f64; 3] {
    let next = points.partition_point(|point| point.bytes < size);
    let micros = match (points[..next].last(), points.get(next)) {
        (Some(from), Some(to)) => {
            let along = (size - from.bytes) / (to.bytes - from.bytes);
            std::array::from_fn(|quantile| {
                from.micros[quantile] + (to.micros[quantile] - from.micros[quantile]) * along
            })
        }
        (None, Some(first)) => first.micros,
        (Some(last), None) => {
            let scale = 1.0 + slope * (size - last.bytes) / last.micros[0];
            last.micros.map(|micros| micros * scale)
        }
        (None, None) => [TIMER_MICROS; 3],
    };
    micros.map(|micros| micros.max(TIMER_MICROS))
}

/// How observations compared with the model in use when each arrived.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Predictions {
    /// Observations at sizes the model predicted.
    pub observations: u64,
    pub above_median: u64,
    pub above_tail: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SizeLatency {
    pub bytes: u64,
    /// As [`QUANTILES`] lists them.
    pub quantiles: [Duration; 3],
}

/// A fitted latency model, exported as metrics.
#[derive(Debug, Clone)]
pub struct LatencyEstimate {
    /// At each of [`REPORTED_SIZES`] up to the largest retained size, so none is
    /// extrapolated.
    pub sizes: Vec<SizeLatency>,
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
    /// Outside it: beyond the last band where latency grows measurably, or else the
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
    fn micros(&self, bytes: u64) -> Option<[f64; 3]> {
        self.0.as_ref()?.micros(bytes)
    }

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
        self.micros(bytes).map(|micros| duration(micros[2]))
    }

    /// p99, capped at 1.25 times the p95 and five times the median. Where stalls exceed 1%
    /// of reads they set the p99, and with ±20% jitter the p95 cap is about 1.5 times the
    /// median. Reads of one size can split between fast and slow objects, like S3
    /// Standard's warm and cold ones, and a hedge cannot speed up the slow mode, so a slow
    /// mode above 5% of reads is waited out, unless it is more than five times the median,
    /// as stalls are, which a hedge can rescue.
    pub fn hedge_delay(&self, bytes: u64) -> Option<Duration> {
        self.micros(bytes).map(|[median, p95, p99]| {
            duration(p99.min(p95 * HEDGE_TAIL_CAP).min(median * HEDGE_MEDIAN_CAP))
        })
    }

    /// At each of [`REPORTED_SIZES`] up to the largest retained size, so none is extrapolated.
    pub fn reported_hedge_delays(&self) -> [Option<Duration>; 4] {
        REPORTED_SIZES.map(|bytes| {
            let curve = self.0.as_ref()?;
            (bytes <= curve.largest_bytes)
                .then(|| self.hedge_delay(bytes))
                .flatten()
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

fn weighted_median(
    samples: &[Observation],
    value: fn(&Observation) -> f64,
    values: &mut Vec<(f64, f64)>,
) -> f64 {
    values.clear();
    values.extend(samples.iter().map(|sample| (value(sample), sample.weight)));
    quantile(values, 0.5)
}

fn tail_ratios(values: &mut [(f64, f64)]) -> [f64; 2] {
    let p95 = quantile(values, QUANTILES[1]).max(1.0);
    [p95, quantile(values, QUANTILES[2]).max(p95)]
}

/// A point per size band at its median size, with medians made nondecreasing across bands.
/// Beyond the last point the median follows the last segment, at most in proportion to
/// size, and no faster than within the last band where its halves, each twice the other's
/// sizes, show it. A lone band follows its halves, or without them is a transfer cost if
/// `proportional`. Tail ratios are to the median at each sample's size, and at least to
/// its band's own, so neither a band's largest sizes nor a step in latency below it set
/// its tail, and come only from the band's own samples, so a slow mode in one band never
/// stretches another's.
fn curve(observations: &[Observation], proportional: bool) -> Curve {
    let mut values = Vec::new();
    let bands: Vec<_> = observations
        .chunk_by(|left, right| band(left.bytes) == band(right.bytes))
        .collect();
    let (mut medians, weights): (Vec<f64>, Vec<f64>) = bands
        .iter()
        .map(|samples| {
            (
                weighted_median(samples, |sample| sample.micros, &mut values),
                samples.iter().map(|sample| sample.weight).sum::<f64>(),
            )
        })
        .unzip();
    nondecreasing(&mut medians, &weights);
    let mut points: Vec<Point> = bands
        .iter()
        .zip(medians)
        .map(|(samples, median)| Point {
            bytes: weighted_median(samples, |sample| sample.bytes as f64, &mut values),
            micros: [median; 3],
        })
        .collect();
    let last = points[points.len() - 1];
    let last_band = bands[bands.len() - 1];
    let middle = 2 * band_bytes(band(last_band[0].bytes));
    let (lower, upper) =
        last_band.split_at(last_band.partition_point(|sample| sample.bytes < middle));
    let halves_slope = if lower.is_empty() || upper.is_empty() {
        f64::INFINITY
    } else {
        let mut median = |samples, value| weighted_median(samples, value, &mut values);
        (median(upper, |sample| sample.micros) - median(lower, |sample| sample.micros))
            / (median(upper, |sample| sample.bytes as f64)
                - median(lower, |sample| sample.bytes as f64))
    };
    if proportional && points.len() == 1 {
        points.insert(
            0,
            Point {
                bytes: 0.0,
                micros: [TIMER_MICROS; 3],
            },
        );
    }
    let segment = points.len().checked_sub(2).map_or(
        if halves_slope.is_finite() {
            f64::INFINITY
        } else {
            0.0
        },
        |previous| {
            (last.micros[0] - points[previous].micros[0]) / (last.bytes - points[previous].bytes)
        },
    );
    let slope = segment
        .min(last.micros[0] / last.bytes)
        .min(halves_slope)
        .max(0.0);
    let offset = points.len() - bands.len();
    let own: Vec<[f64; 2]> = bands
        .iter()
        .zip(&points[offset..])
        .map(|(samples, point)| {
            values.clear();
            values.extend(samples.iter().map(|sample| {
                let expected =
                    interpolate(&points, slope, sample.bytes as f64)[0].max(point.micros[0]);
                (sample.micros / expected, sample.weight)
            }));
            tail_ratios(&mut values)
        })
        .collect();
    for (point, [p95, p99]) in points[offset..].iter_mut().zip(own) {
        let median = point.micros[0];
        point.micros = [median, median * p95, median * p99];
    }
    let scale = points[0].micros[0] / points[offset].micros[0];
    points[0].micros = points[offset].micros.map(|micros| micros * scale);
    Curve {
        points: points.into(),
        slope,
        smallest_bytes: observations[0].bytes,
        largest_bytes: observations[observations.len() - 1].bytes,
    }
}

/// Latency observations by size. Size strata retain rare sizes for one continuous model.
///
/// Model time follows the clock but advances at most [`OBSERVATION_SPACING`] per
/// observation, so sparse traffic keeps enough observations for a tail estimate.
#[derive(Debug)]
pub(super) struct SizeHistogram {
    /// A lone band is a transfer cost rather than a fixed cost.
    proportional: bool,
    bands: BTreeMap<u32, Reservoir<Measurement>>,
    clock: f64,
    observed_at: Option<Instant>,
    updates: u64,
    fitted_updates: u64,
    model: SizeModel,
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
            fitted_updates: 0,
            model: SizeModel::default(),
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
        if let Some([median, _, tail]) = self.model.micros(bytes) {
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

    /// Refits once `due`, or as soon as the observation count doubles.
    pub fn refit(&mut self, due: bool) {
        if self.updates == self.fitted_updates
            || !(due || self.updates >= self.fitted_updates.saturating_mul(2))
        {
            return;
        }
        self.fitted_updates = self.updates;
        let clock = self.clock;
        let mut observations: Vec<_> = self
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
        // Reservoirs return samples in random order; a fixed order makes rounding depend
        // only on which samples were kept, and groups each size band's samples.
        observations.sort_unstable_by(|left, right| {
            left.bytes
                .cmp(&right.bytes)
                .then(left.micros.total_cmp(&right.micros))
                .then(left.weight.total_cmp(&right.weight))
        });
        self.model =
            SizeModel((!observations.is_empty()).then(|| curve(&observations, self.proportional)));
    }

    pub fn model(&self) -> &SizeModel {
        &self.model
    }

    pub fn is_measured(&self) -> bool {
        self.model.0.is_some()
    }

    pub fn estimate(&self) -> Option<LatencyEstimate> {
        let curve = self.model.0.as_ref()?;
        Some(LatencyEstimate {
            sizes: REPORTED_SIZES
                .into_iter()
                .take_while(|&bytes| bytes <= curve.largest_bytes)
                .filter_map(|bytes| {
                    Some(SizeLatency {
                        bytes,
                        quantiles: curve.micros(bytes)?.map(duration),
                    })
                })
                .collect(),
            largest_bytes: curve.largest_bytes,
            predictions: self.predictions,
        })
    }

    #[cfg(test)]
    pub fn summary(&mut self, due: bool) -> SizeModel {
        self.refit(due);
        self.model.clone()
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

    fn fitted(samples: &[(u64, u64)], proportional: bool) -> super::SizeModel {
        let mut histogram = SizeHistogram::new(proportional);
        for &(bytes, millis) in samples {
            update(&mut histogram, bytes, millis);
        }
        histogram.summary(true)
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
        let model = fitted(&[(16 * MIB, 25)], false);
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

    /// Latency that steps up with size and then levels off, as S3 Standard's cold objects
    /// do, is not extended as a line through the step, within the sizes read or beyond
    /// them, and the step sets no band's tail.
    #[tokio::test(start_paused = true)]
    async fn a_step_in_latency_is_not_extended_as_a_line() {
        let samples: Vec<_> = (64..1024)
            .step_by(64)
            .map(|kib| (kib * 1024, 30))
            .chain((4..16).map(|quarters| (quarters * MIB / 4, 110)))
            .chain([(4 * MIB, 110); 20])
            .chain([(15 * MIB, 110); 2])
            .collect::<Vec<_>>()
            .repeat(4);
        let model = fitted(&samples, false);
        assert_eq!(
            model.median(64 * 1024),
            Some(Median::Observed(Duration::from_millis(30)))
        );
        for bytes in [3 * MIB, 8 * MIB, 15 * MIB] {
            assert_eq!(
                model.median(bytes),
                Some(Median::Observed(Duration::from_millis(110)))
            );
            assert_eq!(
                model.tail(bytes),
                Some(Duration::from_millis(110)),
                "{bytes}"
            );
        }
        assert_eq!(
            model.median(64 * MIB),
            Some(Median::Unobserved(Duration::from_millis(110)))
        );
    }

    /// Stalls on many reads of one size leave another size's tail alone.
    #[tokio::test(start_paused = true)]
    async fn stalls_at_one_size_leave_other_sizes_alone() {
        let samples: Vec<_> = (0..1000)
            .map(|read| (4096, if read % 20 == 0 { 1000 } else { 30 }))
            .chain([(4 * MIB, 60); 100])
            .collect();
        let model = fitted(&samples, false);
        assert_eq!(model.tail(4 * MIB), Some(Duration::from_millis(60)));
    }

    /// When reads of one size split between warm and cold objects, a hedge cannot speed up
    /// the cold ones, so it waits past them, but not past stalls, which it can rescue.
    #[tokio::test(start_paused = true)]
    async fn hedges_wait_out_a_slow_mode_but_not_stalls() {
        let samples = [(MIB, 30); 60]
            .into_iter()
            .chain([(MIB, 110); 40])
            .chain([(64 * 1024, 30); 100])
            .collect::<Vec<_>>();
        let model = fitted(&samples, false);
        assert!(model.hedge_delay(MIB).unwrap() >= Duration::from_millis(110));
        assert!(model.hedge_delay(64 * 1024).unwrap() <= Duration::from_millis(45));
        let stalled: Vec<_> = (0..100)
            .map(|read| (MIB, if read % 12 == 0 { 1000 } else { 30 }))
            .collect();
        assert_eq!(
            fitted(&stalled, false).hedge_delay(MIB),
            Some(Duration::from_millis(150))
        );
    }

    /// Capacitor reads a range before sending headers, so its time to first byte grows with
    /// size. A band spans sizes four times apart, and where latency is set by size alone,
    /// its tail follows its median rather than its largest sizes, and a lone band's growth
    /// carries beyond the largest size read.
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
        let lone_band: Vec<_> = samples[..12].to_vec();
        assert_eq!(
            fitted(&lone_band, false).median(4 * MIB),
            Some(Median::Unobserved(Duration::from_millis(17)))
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
        fn quantiles_never_cross_or_exceed_the_bands_around_them(
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
            let curve = curve(&observations, proportional);
            let mut sizes = sizes;
            sizes.sort_unstable();
            let largest = observations[observations.len() - 1].bytes;
            if let Some(at_largest) = curve.micros(largest) {
                for &bytes in sizes.iter().filter(|&&bytes| bytes > largest) {
                    let Some(micros) = curve.micros(bytes) else { break };
                    for (value, limit) in micros.iter().zip(at_largest) {
                        prop_assert!(
                            *value <= limit * bytes as f64 / largest as f64 + 1e-6,
                            "{micros:?} at {bytes} grows faster than size from {at_largest:?}"
                        );
                    }
                }
            }
            let points: Vec<_> = curve
                .points
                .iter()
                .filter(|point| point.bytes > 0.0)
                .map(|point| (point.bytes, curve.micros(point.bytes as u64).unwrap()))
                .collect();
            let mut previous = 0.0;
            for bytes in sizes {
                let Some(micros) = curve.micros(bytes) else { break };
                prop_assert!(micros[0] <= micros[1] && micros[1] <= micros[2], "{micros:?} at {bytes}");
                prop_assert!(micros[0] + 1e-6 >= previous, "{micros:?} after {previous} at {bytes}");
                previous = micros[0];
                let next = points.partition_point(|(point, _)| *point < bytes as f64);
                if let (Some((_, from)), Some((_, to))) = (points[..next].last(), points.get(next)) {
                    for quantile in 0..3 {
                        prop_assert!(
                            micros[quantile] <= from[quantile].max(to[quantile]) + 1e-6,
                            "{micros:?} at {bytes} exceeds {from:?} and {to:?}"
                        );
                    }
                }
            }
        }
    }
}
