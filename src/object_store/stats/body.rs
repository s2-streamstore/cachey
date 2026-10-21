use std::{collections::BTreeMap, time::Duration};

use exponential_decay_histogram::ExponentialDecayHistogram;
use tokio::time::Instant;

use super::{LATENCY_DECAY, Quantiles, Sample, decay, histogram, histogram_micros};

const TIMER_RESOLUTION: Duration = Duration::from_millis(1);
const TIMER_MICROS: f64 = TIMER_RESOLUTION.as_micros() as f64;

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
    p99: Line,
    largest_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct BodyModel(Option<Estimate>);

impl BodyModel {
    pub fn predict(&self, bytes: u64) -> Option<Quantiles> {
        let estimate = self.0?;
        if bytes > estimate.largest_bytes
            && estimate.median.per_byte * estimate.largest_bytes as f64 <= TIMER_MICROS
        {
            return None;
        }
        let duration = |micros: f64| {
            Duration::try_from_secs_f64(micros.round() / 1_000_000.0).unwrap_or(Duration::MAX)
        };
        let median = duration(estimate.median.micros(bytes));
        Some(Quantiles {
            median,
            p99: duration(estimate.p99.micros(bytes)).max(median),
        })
    }

    pub fn largest_bytes(&self) -> Option<u64> {
        self.0.map(|estimate| estimate.largest_bytes)
    }

    pub fn seconds_per_byte(&self) -> Option<f64> {
        self.0.map(|estimate| estimate.p99.per_byte / 1_000_000.0)
    }
}

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

/// Fit nonnegative coefficients with iteratively reweighted quantile regression.
/// Normalize sizes for stability; floor residuals at the stored precision of 1 µs.
fn fit(observations: &[Observation], largest_bytes: u64, q: f64, previous: Option<Line>) -> Line {
    let scale = largest_bytes as f64;
    let initial = || {
        let mut rates = observations
            .iter()
            .map(|sample| {
                let size = sample.bytes as f64 / scale;
                (
                    (sample.micros - TIMER_MICROS).max(0.0) / size,
                    sample.weight * size,
                )
            })
            .collect::<Vec<_>>();
        Line {
            fixed: TIMER_MICROS,
            per_byte: quantile(&mut rates, q) / scale,
        }
    };
    let start = previous.unwrap_or_else(initial);
    let mut fixed = start.fixed;
    let mut transfer = start.per_byte * scale;
    for _ in 0..32 {
        let (mut weight, mut size, mut time, mut square, mut product) = (0.0, 0.0, 0.0, 0.0, 0.0);
        for sample in observations {
            let x = sample.bytes as f64 / scale;
            let y = sample.micros.max(TIMER_MICROS);
            let residual = y - fixed - transfer * x;
            let w =
                sample.weight * if residual >= 0.0 { q } else { 1.0 - q } / residual.abs().max(1.0);
            weight += w;
            size += w * x;
            time += w * y;
            square += w * x * x;
            product += w * x * y;
        }
        let variance = square - size * size / weight;
        if variance <= square * 1e-12 {
            return initial();
        }
        let mut next_transfer = ((product - size * time / weight) / variance).max(0.0);
        let mut next_fixed = (time - next_transfer * size) / weight;
        if next_fixed < 0.0 {
            next_fixed = 0.0;
            next_transfer = (product / square).max(0.0);
        }
        let change = (next_fixed - fixed)
            .abs()
            .max((next_fixed + next_transfer - fixed - transfer).abs());
        fixed = next_fixed;
        transfer = next_transfer;
        if change < 1.0 {
            break;
        }
    }
    // Refit the intercept exactly so iteration limits do not underestimate the tail.
    let mut residuals = observations
        .iter()
        .map(|sample| {
            (
                sample.micros.max(TIMER_MICROS) - transfer * (sample.bytes as f64 / scale),
                sample.weight,
            )
        })
        .collect::<Vec<_>>();
    Line {
        fixed: quantile(&mut residuals, q).max(0.0),
        per_byte: transfer / scale,
    }
}

fn model(observations: &[Observation], previous: BodyModel) -> BodyModel {
    BodyModel(
        observations
            .iter()
            .map(|sample| sample.bytes)
            .max()
            .map(|largest_bytes| Estimate {
                median: fit(
                    observations,
                    largest_bytes,
                    0.5,
                    previous.0.map(|model| model.median),
                ),
                p99: fit(
                    observations,
                    largest_bytes,
                    0.99,
                    previous.0.map(|model| model.p99),
                ),
                largest_bytes,
            }),
    )
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

/// Size-stratified retention keeps rare transfers available to one continuous model.
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
                // Restore each stratum's observation mass after reservoir sampling.
                let correction = band.weight_at(now) / retained_weight;
                observations.extend(snapshot.exemplars().map(|(micros, sample)| Observation {
                    bytes: sample.bytes,
                    micros: micros as f64,
                    weight: weight(sample.at) * correction,
                }));
            }
            self.model = model(&observations, self.model);
        }
        self.model
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
    async fn two_sizes_identify_fixed_overhead_and_transfer_cost() {
        let mut histogram = BodyHistogram::default();
        for (bytes, body_ms) in [(MIB, 30), (16 * MIB, 180)] {
            histogram.update(Instant::now(), &Sample::from_millis(10, bytes, body_ms));
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
            let quantiles = model.predict(bytes).unwrap();
            assert!(quantiles.median.abs_diff(expected) <= Duration::from_micros(1));
            assert!(quantiles.p99.abs_diff(expected) <= Duration::from_micros(1));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn tiny_buffered_reads_do_not_erase_a_measured_transfer() {
        let mut histogram = BodyHistogram::default();
        histogram.update(Instant::now(), &Sample::from_millis(10, 4 * MIB, 41));
        for _ in 0..5000 {
            histogram.update(Instant::now(), &Sample::from_millis(10, 1024, 0));
        }
        let model = histogram.summary(true);
        assert_eq!(model.predict(1024).unwrap().p99, TIMER_RESOLUTION);
        assert_eq!(
            model.predict(4 * MIB).unwrap().median,
            Duration::from_millis(41)
        );
        assert!(model.predict(16 * MIB).unwrap().p99 <= Duration::from_millis(162));
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
        // With a nonnegative intercept, extrapolation grows no faster than response size.
        assert!(model.predict(16 * MIB).unwrap().hedge_delay() <= Duration::from_millis(265));
        advance(Duration::from_secs(180)).await;
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
