use std::{collections::BTreeMap, time::Duration};

use tokio::time::Instant;

use super::{LATENCY_DECAY, LATENCY_SNAPSHOT_INTERVAL, Sample, decay};

const SCHEDULING_ALLOWANCE: Duration = Duration::from_millis(1);
// Retain seven mantissa bits: rates in one bin differ by less than 1/128.
const DISCARDED_MANTISSA_BITS: u32 = 45;

#[derive(Debug, Clone, Copy)]
pub(super) struct BodyModel {
    pub p99_seconds_per_byte: f64,
    median_seconds_per_byte: f64,
    buffered_bytes: u64,
}

impl BodyModel {
    pub fn tail(self, bytes: u64) -> Option<Duration> {
        let tail = self.delay(bytes, self.p99_seconds_per_byte)?;
        let median = self.delay(bytes, self.median_seconds_per_byte)?;
        Some(tail.min(median.saturating_add(median / 2)))
    }

    fn delay(self, bytes: u64, seconds_per_byte: f64) -> Option<Duration> {
        if seconds_per_byte == 0.0 && bytes > self.buffered_bytes {
            return None;
        }
        let transfer =
            Duration::try_from_secs_f64(seconds_per_byte * bytes as f64).unwrap_or(Duration::MAX);
        Some(SCHEDULING_ALLOWANCE.saturating_add(transfer))
    }
}

#[derive(Debug, Default)]
struct Bin {
    weight: f64,
    timed_weight: f64,
    slowest: f64,
}

/// Timed transfers teach the p99 rate. Buffered bodies contribute conservative rate bounds to
/// the median, but cannot dilute timed transfers. Aggregating nearby rates preserves byte weights
/// with bounded storage; using each bin's slowest rate overestimates a quantile by at most 1/128.
#[derive(Debug)]
pub(super) struct BodyHistogram {
    bins: BTreeMap<u64, Bin>,
    updated_at: Instant,
    buffered_bytes: u64,
    summary: Option<BodyModel>,
    changed: bool,
}

impl BodyHistogram {
    pub fn new(now: Instant) -> Self {
        Self {
            bins: BTreeMap::new(),
            updated_at: now,
            buffered_bytes: 0,
            summary: None,
            changed: false,
        }
    }

    pub fn update(&mut self, now: Instant, sample: &Sample) {
        if sample.body_bytes == 0 {
            return;
        }
        let mut elapsed = now.saturating_duration_since(self.updated_at);
        if elapsed >= LATENCY_SNAPSHOT_INTERVAL {
            let factor = decay(LATENCY_DECAY, elapsed);
            self.bins.retain(|_, bin| {
                bin.weight *= factor;
                bin.timed_weight *= factor;
                bin.weight > 0.0
            });
            self.updated_at = now;
            elapsed = Duration::ZERO;
        }
        let timed = sample.body > SCHEDULING_ALLOWANCE;
        let transfer = if timed {
            sample.body.saturating_sub(SCHEDULING_ALLOWANCE)
        } else {
            self.buffered_bytes = self.buffered_bytes.max(sample.body_bytes);
            SCHEDULING_ALLOWANCE
        };
        let seconds_per_byte = transfer.as_secs_f64() / sample.body_bytes as f64;
        let bin = self
            .bins
            .entry(seconds_per_byte.to_bits() >> DISCARDED_MANTISSA_BITS)
            .or_default();
        // Weights share the epoch at updated_at; rescaling once per interval bounds their growth.
        let weight = sample.body_bytes as f64 / decay(LATENCY_DECAY, elapsed);
        bin.weight += weight;
        if timed {
            bin.timed_weight += weight;
        }
        bin.slowest = bin.slowest.max(seconds_per_byte);
        self.changed = true;
    }

    pub fn summary(&mut self, due: bool) -> Option<BodyModel> {
        if self.changed && (due || self.summary.is_none()) {
            self.changed = false;
            self.summary = Some(BodyModel {
                p99_seconds_per_byte: self.quantile(0.99, |bin| bin.timed_weight).unwrap_or(0.0),
                median_seconds_per_byte: self.quantile(0.5, |bin| bin.weight).unwrap_or(0.0),
                buffered_bytes: self.buffered_bytes,
            });
        }
        self.summary
    }

    fn quantile(&self, q: f64, weight: impl Fn(&Bin) -> f64) -> Option<f64> {
        let total: f64 = self.bins.values().map(&weight).sum();
        if total == 0.0 {
            return None;
        }
        let mut cumulative = 0.0;
        self.bins.values().find_map(|bin| {
            cumulative += weight(bin);
            (cumulative >= total * (q - 1e-9)).then_some(bin.slowest)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytesize::MIB;
    use tokio::time::{Instant, advance};

    use super::{BodyHistogram, DISCARDED_MANTISSA_BITS, SCHEDULING_ALLOWANCE};
    use crate::object_store::stats::Sample;

    #[tokio::test(start_paused = true)]
    async fn one_transfer_predicts_continuously_across_sizes() {
        let mut histogram = BodyHistogram::new(Instant::now());
        histogram.update(Instant::now(), &Sample::from_millis(10, 4 * MIB, 41));
        let model = histogram.summary(false).unwrap();
        for bytes in [0, 4096, 1023 * 1024, MIB, 1025 * 1024, 16 * MIB] {
            let expected =
                SCHEDULING_ALLOWANCE + Duration::from_secs_f64(0.01 * bytes as f64 / MIB as f64);
            assert_eq!(model.tail(bytes), Some(expected), "{bytes}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn tiny_reads_neither_evict_transfers_nor_amplify_rare_stalls() {
        let mut histogram = BodyHistogram::new(Instant::now());
        histogram.update(Instant::now(), &Sample::from_millis(10, 4 * MIB, 41));
        for index in 0..500_000 {
            let body_ms = if index < 20 { 100 } else { 0 };
            histogram.update(Instant::now(), &Sample::from_millis(10, 1024, body_ms));
        }
        assert_eq!(histogram.bins.len(), 3);
        let model = histogram.summary(false).unwrap();
        assert_eq!(model.tail(4 * MIB), Some(Duration::from_millis(41)));
        assert_eq!(model.tail(16 * MIB), Some(Duration::from_millis(161)));
    }

    #[tokio::test(start_paused = true)]
    async fn byte_weights_share_a_five_second_half_life() {
        let start = Instant::now();
        let mut histogram = BodyHistogram::new(start);
        histogram.update(start, &Sample::from_millis(10, MIB, 101));
        advance(Duration::from_millis(2500)).await;
        histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 11));
        advance(Duration::from_millis(250)).await;
        histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 11));
        advance(Duration::from_millis(250)).await;
        histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 11));
        let weights: Vec<_> = histogram.bins.values().map(|bin| bin.weight).collect();
        let slow = MIB as f64 * 0.5_f64.powf(0.5);
        let fast = MIB as f64 * (1.0 + 2.0_f64.powf(0.05) + 2.0_f64.powf(0.1));
        assert!((weights[0] - fast).abs() < 1e-6);
        assert!((weights[1] - slow).abs() < 1e-6);
        let model = histogram.summary(true).unwrap();
        assert!((model.p99_seconds_per_byte * MIB as f64 - 0.1).abs() < 1e-9);
        assert_eq!(model.tail(MIB), Some(Duration::from_micros(16_500)));

        advance(Duration::from_secs(40)).await;
        histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 11));
        assert_eq!(
            histogram.summary(true).unwrap().tail(MIB),
            Some(Duration::from_millis(11))
        );

        advance(Duration::from_secs(10_000)).await;
        histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 21));
        assert_eq!(histogram.bins.len(), 1);
        assert_eq!(
            histogram.summary(true).unwrap().tail(MIB),
            Some(Duration::from_millis(21))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn buffered_bodies_only_establish_the_sizes_they_cover() {
        let mut histogram = BodyHistogram::new(Instant::now());
        histogram.update(Instant::now(), &Sample::from_millis(10, 4096, 0));
        let model = histogram.summary(false).unwrap();
        assert_eq!(model.tail(4096), Some(SCHEDULING_ALLOWANCE));
        assert_eq!(model.tail(4097), None);
        histogram.update(Instant::now(), &Sample::from_millis(10, MIB, 1));
        let model = histogram.summary(true).unwrap();
        assert_eq!(model.tail(MIB), Some(SCHEDULING_ALLOWANCE));
        assert_eq!(model.tail(MIB + 1), None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stall_among_buffered_reads_does_not_set_the_body_delay() {
        let mut histogram = BodyHistogram::new(Instant::now());
        for _ in 0..50 {
            histogram.update(Instant::now(), &Sample::from_millis(5, MIB, 0));
        }
        histogram.update(Instant::now(), &Sample::from_millis(5, MIB, 1000));
        let model = histogram.summary(false).unwrap();
        assert!((model.p99_seconds_per_byte * MIB as f64 - 0.999).abs() < 1e-9);
        let below = model.tail(1023 * 1024).unwrap();
        let at = model.tail(MIB).unwrap();
        let above = model.tail(1025 * 1024).unwrap();
        assert_eq!(at, Duration::from_millis(3));
        assert!(at.checked_sub(below).unwrap() < Duration::from_micros(2));
        assert!(above.checked_sub(at).unwrap() < Duration::from_micros(2));
    }

    #[tokio::test(start_paused = true)]
    async fn empty_bodies_do_not_teach_a_transfer_rate() {
        let mut histogram = BodyHistogram::new(Instant::now());
        histogram.update(Instant::now(), &Sample::from_millis(10, 0, 100));
        assert!(histogram.summary(false).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn rate_bins_bound_error_and_handle_extreme_measurements() {
        for body in [
            SCHEDULING_ALLOWANCE + Duration::from_nanos(1),
            Duration::from_millis(41),
            Duration::MAX,
        ] {
            for bytes in [1, 4 * MIB, u64::MAX] {
                let sample = Sample {
                    first_byte: Duration::ZERO,
                    body_bytes: bytes,
                    body,
                };
                let mut histogram = BodyHistogram::new(Instant::now());
                histogram.update(Instant::now(), &sample);
                let model = histogram.summary(false).unwrap();
                let key = *histogram.bins.first_key_value().unwrap().0;
                let lower = f64::from_bits(key << DISCARDED_MANTISSA_BITS);
                let upper = f64::from_bits((key + 1) << DISCARDED_MANTISSA_BITS);
                assert!(model.p99_seconds_per_byte >= lower && model.p99_seconds_per_byte < upper);
                assert!(upper / lower <= 1.0 + 1.0 / 128.0);
                assert!(model.tail(u64::MAX).is_some());
            }
        }
    }
}
