use std::{collections::BTreeMap, sync::Arc, time::Duration};

use exponential_decay_histogram::ExponentialDecayHistogram;
use tokio::time::Instant;

use super::{LATENCY_DECAY, Quantiles, Sample, decay, histogram, histogram_micros};

const TIMER_RESOLUTION: Duration = Duration::from_millis(1);
const TIMER_MICROS: f64 = TIMER_RESOLUTION.as_micros() as f64;

#[derive(Debug, Clone, Copy)]
struct Estimate {
    bytes: u64,
    quantiles: Quantiles,
}

#[derive(Debug, Clone, Default)]
pub(super) struct BodyModel {
    estimates: Arc<[Estimate]>,
    fixed: Duration,
}

impl BodyModel {
    pub fn predict(&self, bytes: u64) -> Option<Quantiles> {
        let upper = self
            .estimates
            .partition_point(|estimate| estimate.bytes < bytes);
        let first = self.estimates.first()?;
        let last = self.estimates.last()?;
        if bytes > last.bytes && last.quantiles.median <= self.fixed {
            return None;
        }
        // Share transfer-rate evidence without scaling away the learned fixed overhead.
        let transfer = scale(last.quantiles, bytes as f64 / last.bytes as f64, self.fixed);
        let quantiles = if upper == 0 {
            scale(
                first.quantiles,
                bytes as f64 / first.bytes as f64,
                self.fixed,
            )
        } else if upper == self.estimates.len() {
            transfer
        } else {
            let left = self.estimates[upper - 1];
            let right = self.estimates[upper];
            let fraction = (bytes - left.bytes) as f64 / (right.bytes - left.bytes) as f64;
            Quantiles {
                median: interpolate(left.quantiles.median, right.quantiles.median, fraction),
                p99: interpolate(left.quantiles.p99, right.quantiles.p99, fraction),
            }
        };
        Some(Quantiles {
            median: quantiles.median.max(transfer.median),
            p99: quantiles.p99.max(transfer.p99),
        })
    }

    pub fn largest_bytes(&self) -> Option<u64> {
        self.estimates.last().map(|estimate| estimate.bytes)
    }

    pub fn seconds_per_byte(&self) -> Option<f64> {
        self.estimates.last().map(|estimate| {
            estimate
                .quantiles
                .p99
                .saturating_sub(TIMER_RESOLUTION)
                .as_secs_f64()
                / estimate.bytes as f64
        })
    }
}

fn scale(quantiles: Quantiles, factor: f64, fixed: Duration) -> Quantiles {
    let duration = |value: Duration| {
        fixed.saturating_add(
            Duration::try_from_secs_f64(value.saturating_sub(fixed).as_secs_f64() * factor)
                .unwrap_or(Duration::MAX),
        )
    };
    Quantiles {
        median: duration(quantiles.median),
        p99: duration(quantiles.p99),
    }
}

fn interpolate(left: Duration, right: Duration, fraction: f64) -> Duration {
    if right >= left {
        left.saturating_add(right.saturating_sub(left).mul_f64(fraction))
    } else {
        left.saturating_sub(left.saturating_sub(right).mul_f64(fraction))
    }
}

// Adjacent sizes share observations until they can support their own median and tail.
const MIN_GROUP_WEIGHT: f64 = 3.0;
const MAX_GROUPS: usize = 16;

#[derive(Debug, Clone, Copy)]
struct Measurement {
    bytes: u64,
    at: Instant,
}

#[derive(Debug, Clone, Copy)]
struct Observation {
    bytes: u64,
    micros: f64,
    weight: f64,
}

fn quantile(values: &mut [(f64, f64)], q: f64) -> f64 {
    values.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
    let total: f64 = values.iter().map(|(_, weight)| weight).sum();
    let mut cumulative = 0.0;
    for &(value, weight) in values.iter() {
        cumulative += weight;
        if cumulative >= total * (q - 1e-9) {
            return value;
        }
    }
    values.last().map_or(0.0, |value| value.0)
}

fn estimate(observations: &[Observation], fixed: f64) -> Estimate {
    let mut sizes = observations
        .iter()
        .map(|sample| (sample.bytes as f64, sample.weight))
        .collect::<Vec<_>>();
    let bytes = quantile(&mut sizes, 0.5) as u64;
    let mut durations = observations
        .iter()
        .map(|sample| {
            let factor = bytes as f64 / sample.bytes as f64;
            (
                fixed + (sample.micros - fixed).max(0.0) * factor,
                sample.weight / factor.max(1.0),
            )
        })
        .collect::<Vec<_>>();
    let duration =
        |micros| Duration::try_from_secs_f64(micros / 1_000_000.0).unwrap_or(Duration::MAX);
    Estimate {
        bytes,
        quantiles: Quantiles {
            median: duration(quantile(&mut durations, 0.5)),
            p99: duration(quantile(&mut durations, 0.99)),
        },
    }
}

fn groups(observations: &[Observation]) -> Vec<std::ops::Range<usize>> {
    let (total, largest_size_weight) = observations
        .chunk_by(|left, right| left.bytes == right.bytes)
        .map(|samples| samples.iter().map(|sample| sample.weight).sum::<f64>())
        .fold((0.0, 0.0_f64), |(total, largest), weight| {
            (total + weight, largest.max(weight))
        });
    // A dominant page size takes one group without raising the evidence required of rare sizes.
    let minimum_weight =
        ((total - largest_size_weight) / (MAX_GROUPS - 1) as f64).max(MIN_GROUP_WEIGHT);
    let mut groups = Vec::new();
    let mut start = 0;
    let mut weight = 0.0;
    for index in 0..observations.len() {
        weight += observations[index].weight;
        if weight >= minimum_weight
            && observations
                .get(index + 1)
                .is_some_and(|next| next.bytes != observations[index].bytes)
        {
            groups.push(start..index + 1);
            start = index + 1;
            weight = 0.0;
        }
    }
    if start < observations.len() {
        if weight < minimum_weight
            && let Some(last) = groups.last_mut()
        {
            last.end = observations.len();
        } else {
            groups.push(start..observations.len());
        }
    }
    groups
}

fn fixed_cost(observations: &[Observation], groups: &[std::ops::Range<usize>]) -> f64 {
    let center = |range: std::ops::Range<usize>| {
        let samples = &observations[range];
        let mut sizes = samples
            .iter()
            .map(|sample| (sample.bytes as f64, sample.weight))
            .collect::<Vec<_>>();
        let mut times = samples
            .iter()
            .map(|sample| (sample.micros, sample.weight))
            .collect::<Vec<_>>();
        (quantile(&mut sizes, 0.5), quantile(&mut times, 0.5))
    };
    let centers = groups.iter().cloned().map(center).collect::<Vec<_>>();
    match (centers.first(), centers.last()) {
        (Some(left), Some(right)) if left.0 < right.0 => {
            let rate = ((right.1 - left.1) / (right.0 - left.0)).max(0.0);
            // Keep the shared fixed cost below every center after accounting for transfer time.
            centers
                .iter()
                .map(|(bytes, micros)| micros - rate * bytes)
                .fold(f64::INFINITY, f64::min)
                .max(TIMER_MICROS)
        }
        _ => TIMER_MICROS,
    }
}

fn model(mut observations: Vec<Observation>) -> BodyModel {
    observations.sort_unstable_by_key(|sample| sample.bytes);
    let groups = groups(&observations);
    let fixed = fixed_cost(&observations, &groups);
    BodyModel {
        fixed: Duration::from_secs_f64(fixed / 1_000_000.0),
        estimates: groups
            .into_iter()
            .map(|range| estimate(&observations[range], fixed))
            .collect(),
    }
}

#[derive(Debug)]
struct Band {
    samples: ExponentialDecayHistogram<Measurement>,
    weight: f64,
    updated_at: Instant,
}

impl Band {
    fn weight_at(&self, now: Instant) -> f64 {
        self.weight
            * decay(
                LATENCY_DECAY,
                now.saturating_duration_since(self.updated_at),
            )
    }
}

/// Size-stratified sampling preserves rare transfers; adjacent sizes pool their evidence when
/// building the model, rather than treating every populated reservoir as an independent estimate.
#[derive(Debug, Default)]
pub(super) struct BodyHistogram {
    bands: BTreeMap<u64, Band>,
    model: BodyModel,
    updates: u64,
    summarized_updates: u64,
}

impl BodyHistogram {
    pub fn update(&mut self, now: Instant, sample: &Sample) {
        if sample.body_bytes == 0 {
            return;
        }
        let bytes = sample
            .body_bytes
            .checked_next_power_of_two()
            .unwrap_or(u64::MAX);
        let band = self.bands.entry(bytes).or_insert_with(|| Band {
            samples: histogram(now),
            weight: 0.0,
            updated_at: now,
        });
        band.weight = band.weight_at(now) + 1.0;
        band.updated_at = now;
        band.samples.update_at_ex(
            now.into_std(),
            histogram_micros(sample.body),
            Measurement {
                bytes: sample.body_bytes,
                at: now,
            },
        );
        self.updates = self.updates.saturating_add(1);
    }

    #[cfg(test)]
    pub fn retention(&self) -> impl Iterator<Item = (u64, usize)> + '_ {
        self.bands.values().map(|band| {
            let snapshot = band.samples.snapshot();
            (snapshot.count(), snapshot.exemplars().count())
        })
    }

    pub fn summary(&mut self, due: bool) -> BodyModel {
        if self.updates != self.summarized_updates
            && (due || self.updates >= self.summarized_updates.saturating_mul(2))
        {
            self.summarized_updates = self.updates;
            let now = Instant::now();
            let mut observations = Vec::new();
            for band in self.bands.values() {
                let snapshot = band.samples.snapshot();
                let weight = |at| decay(LATENCY_DECAY, now.saturating_duration_since(at));
                let retained_weight: f64 = snapshot
                    .exemplars()
                    .map(|(_, sample)| weight(sample.at))
                    .sum();
                if retained_weight == 0.0 {
                    continue;
                }
                // Sampling must not give busy and infrequent size bands equal influence.
                let correction = band.weight_at(now) / retained_weight;
                observations.extend(snapshot.exemplars().map(|(micros, sample)| Observation {
                    bytes: sample.bytes,
                    micros: micros as f64,
                    weight: weight(sample.at) * correction,
                }));
            }
            self.model = model(observations);
        }
        self.model.clone()
    }
}
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytesize::MIB;
    use tokio::time::{Instant, advance};

    use super::{BodyHistogram, TIMER_RESOLUTION};
    use crate::object_store::stats::Sample;

    #[tokio::test(start_paused = true)]
    async fn interpolation_retains_fixed_overhead_and_is_continuous() {
        let mut histogram = BodyHistogram::default();
        for (bytes, body_ms) in [(MIB, 30), (4 * MIB, 60), (16 * MIB, 180)] {
            for _ in 0..4 {
                histogram.update(Instant::now(), &Sample::from_millis(10, bytes, body_ms));
            }
        }
        let model = histogram.summary(false);
        for bytes in [
            MIB,
            2 * MIB,
            4 * MIB - 1,
            4 * MIB,
            4 * MIB + 1,
            8 * MIB,
            16 * MIB,
        ] {
            let expected = Duration::from_secs_f64(0.020 + 0.010 * bytes as f64 / MIB as f64);
            assert_eq!(model.predict(bytes).unwrap().p99, expected);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn buffered_history_does_not_invent_throughput_after_timed_samples_are_outnumbered() {
        let mut histogram = BodyHistogram::default();
        histogram.update(Instant::now(), &Sample::from_millis(10, 4 * MIB, 41));
        for _ in 0..5000 {
            histogram.update(Instant::now(), &Sample::from_millis(10, 1024, 0));
        }
        let model = histogram.summary(true);
        assert_eq!(model.predict(1024).unwrap().p99, TIMER_RESOLUTION);
        assert!(model.predict(4 * MIB).is_none());
        for _ in 0..4 {
            histogram.update(Instant::now(), &Sample::from_millis(10, 4 * MIB, 41));
        }
        assert_eq!(
            histogram.summary(true).predict(4 * MIB).unwrap().median,
            Duration::from_millis(41)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn body_observations_decay_without_capping_the_tail() {
        let mut histogram = BodyHistogram::default();
        for _ in 0..50 {
            histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 11));
        }
        histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 101));
        let model = histogram.summary(true);
        let quantiles = model.predict(MIB).unwrap();
        assert_eq!(quantiles.p99, Duration::from_millis(101));
        assert_eq!(quantiles.hedge_delay(), Duration::from_micros(16_500));
        advance(Duration::from_secs(300)).await;
        histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 21));
        assert_eq!(
            histogram.summary(true).predict(MIB).unwrap().p99,
            Duration::from_millis(21)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn buffered_bodies_do_not_establish_throughput_for_larger_sizes() {
        let mut histogram = BodyHistogram::default();
        histogram.update(Instant::now(), &Sample::from_millis(10, 3000, 0));
        let model = histogram.summary(false);
        assert_eq!(model.predict(3000).unwrap().p99, TIMER_RESOLUTION);
        assert!(model.predict(3001).is_none());
        histogram.update(Instant::now(), &Sample::from_millis(10, 3000, 1000));
        let model = histogram.summary(true);
        assert!(model.predict(MIB).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn fixed_overhead_alone_does_not_establish_larger_transfer_times() {
        let mut histogram = BodyHistogram::default();
        for bytes in [4096, 65_536] {
            for _ in 0..4 {
                histogram.update(Instant::now(), &Sample::from_millis(10, bytes, 20));
            }
        }
        let model = histogram.summary(true);
        assert_eq!(
            model.predict(65_536).unwrap().p99,
            Duration::from_millis(20)
        );
        assert!(model.predict(MIB).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn empty_bodies_and_extreme_measurements_are_bounded() {
        let mut histogram = BodyHistogram::default();
        histogram.update(Instant::now(), &Sample::from_millis(10, 0, 100));
        assert!(histogram.summary(false).predict(0).is_none());
        for bytes in [1, u64::MAX] {
            histogram.update(
                Instant::now(),
                &Sample {
                    first_byte: Duration::ZERO,
                    body_bytes: bytes,
                    body: Duration::MAX,
                },
            );
        }
        assert!(histogram.summary(true).predict(u64::MAX).is_some());
    }
    #[tokio::test(start_paused = true)]
    async fn snapshots_refresh_as_evidence_doubles_without_changing_existing_readers() {
        let mut histogram = BodyHistogram::default();
        for _ in 0..4 {
            histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 11));
        }
        let snapshot = histogram.summary(false);
        histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 21));
        assert_eq!(
            histogram.summary(false).predict(MIB).unwrap().p99,
            Duration::from_millis(11)
        );
        for _ in 0..3 {
            histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 21));
        }
        assert_eq!(
            histogram.summary(false).predict(MIB).unwrap().p99,
            Duration::from_millis(21)
        );
        histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 41));
        assert_eq!(
            histogram.summary(true).predict(MIB).unwrap().p99,
            Duration::from_millis(41)
        );
        assert_eq!(
            snapshot.predict(MIB).unwrap().p99,
            Duration::from_millis(11)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn isolated_large_stalls_borrow_history_and_age_out_with_other_sizes() {
        let mut histogram = BodyHistogram::default();
        for _ in 0..50 {
            histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 11));
        }
        histogram.update(Instant::now(), &Sample::from_millis(10, 16 * MIB, 1001));
        let model = histogram.summary(true);
        assert_eq!(
            model.predict(MIB).unwrap().median,
            Duration::from_millis(11)
        );
        assert!(model.predict(MIB).unwrap().hedge_delay() <= Duration::from_micros(16_500));
        assert!(model.predict(16 * MIB).unwrap().hedge_delay() <= Duration::from_micros(241_500));
        advance(Duration::from_secs(120)).await;
        for _ in 0..4 {
            histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 11));
        }
        assert_eq!(
            histogram.summary(true).predict(MIB).unwrap().p99,
            Duration::from_millis(11)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn diverse_exact_sizes_share_fixed_overhead_from_nine_observations() {
        let mut histogram = BodyHistogram::default();
        for exponent in 10..=18 {
            let bytes = 1 << exponent;
            histogram.update(
                Instant::now(),
                &Sample {
                    first_byte: Duration::from_millis(25),
                    body_bytes: bytes,
                    body: Duration::from_secs_f64(0.020 + 0.010 * bytes as f64 / MIB as f64),
                },
            );
        }
        let model = histogram.summary(true);
        for bytes in [1, 512, 1024, 4096, 12_345, 65_536, 262_144] {
            let expected = 0.020 + 0.010 * bytes as f64 / MIB as f64;
            let quantiles = model.predict(bytes).unwrap();
            assert!(
                (quantiles.median.as_secs_f64() - expected).abs() < 0.000_005,
                "{bytes}: {quantiles:?}"
            );
            assert!(
                (quantiles.p99.as_secs_f64() - expected).abs() < 0.000_005,
                "{bytes}: {quantiles:?}"
            );
        }
    }
}
