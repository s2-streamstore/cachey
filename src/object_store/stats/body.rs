use std::{collections::BTreeMap, sync::Arc, time::Duration};

use tokio::time::Instant;

use super::{PhaseHistogram, Quantiles, Sample, histogram_micros};

const TIMER_RESOLUTION: Duration = Duration::from_millis(1);

#[derive(Debug, Clone, Copy)]
struct Estimate {
    bytes: u64,
    quantiles: Quantiles,
}

#[derive(Debug, Clone, Default)]
pub(super) struct BodyModel {
    estimates: Arc<[Estimate]>,
    largest_observed: u64,
}

impl BodyModel {
    pub fn predict(&self, bytes: u64) -> Option<Quantiles> {
        let upper = self
            .estimates
            .partition_point(|estimate| estimate.bytes < bytes);
        let first = self.estimates.first()?;
        let last = self.estimates.last()?;
        if bytes > self.largest_observed && last.quantiles.median <= TIMER_RESOLUTION {
            return None;
        }
        // Large transfers inform sparse smaller bands without erasing their fixed overhead.
        let transfer = scale(last.quantiles, bytes as f64 / last.bytes as f64);
        let quantiles = if upper == 0 {
            scale(first.quantiles, bytes as f64 / first.bytes as f64)
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

fn scale(quantiles: Quantiles, factor: f64) -> Quantiles {
    let duration = |value: Duration| {
        TIMER_RESOLUTION.saturating_add(
            Duration::try_from_secs_f64(
                value.saturating_sub(TIMER_RESOLUTION).as_secs_f64() * factor,
            )
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

/// Each power-of-two size band retains its own history. Normalizing to its upper size bound
/// lets adjacent bands interpolate continuously without letting tiny reads evict large reads.
#[derive(Debug, Default)]
pub(super) struct BodyHistogram {
    bands: BTreeMap<u64, PhaseHistogram>,
    model: BodyModel,
    largest_observed: u64,
    changed: bool,
}

impl BodyHistogram {
    pub fn update(&mut self, now: Instant, sample: &Sample) {
        if sample.body_bytes == 0 {
            return;
        }
        self.largest_observed = self.largest_observed.max(sample.body_bytes);
        let bytes = sample
            .body_bytes
            .checked_next_power_of_two()
            .unwrap_or(u64::MAX);
        let micros = histogram_micros(sample.body.saturating_sub(TIMER_RESOLUTION)) as u128;
        let normalized = (micros * u128::from(bytes) / u128::from(sample.body_bytes))
            .min(i64::MAX as u128) as i64;
        self.bands
            .entry(bytes)
            .or_insert_with(|| PhaseHistogram::new(now))
            .update(now.into_std(), normalized);
        self.changed = true;
    }

    #[cfg(test)]
    pub fn retention(&self) -> impl Iterator<Item = (u64, usize)> + '_ {
        self.bands.values().map(PhaseHistogram::retention)
    }

    pub fn summary(&mut self, due: bool) -> BodyModel {
        if self.changed && (due || self.model.estimates.len() != self.bands.len()) {
            self.model = BodyModel {
                largest_observed: self.largest_observed,
                estimates: self
                    .bands
                    .iter_mut()
                    .filter_map(|(bytes, histogram)| {
                        let summary = histogram.summary(due)?;
                        Some(Estimate {
                            bytes: *bytes,
                            quantiles: Quantiles {
                                median: TIMER_RESOLUTION
                                    .saturating_add(Duration::from_micros(summary.median as u64)),
                                p99: TIMER_RESOLUTION.saturating_add(summary.p99_duration()),
                            },
                        })
                    })
                    .collect(),
            };
            self.changed = self.bands.values().any(|histogram| histogram.changed);
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
            assert_eq!(model.predict(bytes).unwrap().p99, expected);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn tiny_reads_do_not_evict_transfers_or_share_their_stalls() {
        let mut histogram = BodyHistogram::default();
        histogram.update(Instant::now(), &Sample::from_millis(10, 4 * MIB, 41));
        for index in 0..5000 {
            histogram.update(
                Instant::now(),
                &Sample::from_millis(10, 1024, if index < 20 { 100 } else { 0 }),
            );
        }
        let model = histogram.summary(true);
        assert_eq!(histogram.bands.len(), 2);
        assert_eq!(
            model.predict(4 * MIB).unwrap().p99,
            Duration::from_millis(41)
        );
        assert_eq!(
            model.predict(16 * MIB).unwrap().p99,
            Duration::from_millis(161)
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
    async fn new_bands_do_not_refresh_existing_history_before_the_snapshot_is_due() {
        let mut histogram = BodyHistogram::default();
        histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 11));
        let snapshot = histogram.summary(false);
        histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 21));
        histogram.update(Instant::now(), &Sample::from_millis(10, 4 * MIB, 41));
        let with_new_band = histogram.summary(false);
        assert_eq!(
            with_new_band.predict(MIB).unwrap().p99,
            Duration::from_millis(11)
        );
        assert_eq!(
            with_new_band.predict(4 * MIB).unwrap().p99,
            Duration::from_millis(41)
        );
        assert_eq!(
            histogram.summary(true).predict(MIB).unwrap().p99,
            Duration::from_millis(21)
        );
        assert_eq!(
            snapshot.predict(MIB).unwrap().p99,
            Duration::from_millis(11)
        );
    }
    #[tokio::test(start_paused = true)]
    async fn sparse_small_bands_share_transfer_evidence_and_retain_fixed_overhead() {
        for (small_body_ms, expected_ms) in [(11, 13), (30, 30)] {
            let mut histogram = BodyHistogram::default();
            histogram.update(Instant::now(), &Sample::from_millis(10, MIB, small_body_ms));
            for _ in 0..50 {
                histogram.update(Instant::now(), &Sample::from_millis(10, 16 * MIB, 161));
            }
            histogram.update(Instant::now(), &Sample::from_millis(10, 16 * MIB, 193));
            let prediction = histogram.summary(true).predict(MIB).unwrap();
            assert_eq!(prediction.p99, Duration::from_millis(expected_ms));
            assert_eq!(prediction.median, Duration::from_millis(small_body_ms));
        }
    }
}
