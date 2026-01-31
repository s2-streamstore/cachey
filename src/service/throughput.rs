use std::time::Duration;

use tokio::time::Instant;

#[derive(Debug)]
pub struct SlidingThroughput<const NUM_BUCKETS: usize = 60> {
    buckets: [u64; NUM_BUCKETS],
    head_idx: usize,
    head_tick: u64,
    last_evicted_tick: Option<u64>,
    last_evicted_bytes: u64,
    base: Instant,
}

impl<const NUM_BUCKETS: usize> Default for SlidingThroughput<NUM_BUCKETS> {
    fn default() -> Self {
        Self {
            buckets: [0; NUM_BUCKETS],
            head_idx: 0,
            head_tick: 0,
            last_evicted_tick: None,
            last_evicted_bytes: 0,
            base: Instant::now(),
        }
    }
}

impl<const NUM_BUCKETS: usize> SlidingThroughput<NUM_BUCKETS> {
    fn advance_to(&mut self, now_tick: u64) {
        if now_tick <= self.head_tick {
            return;
        }
        let steps_u64 = now_tick - self.head_tick;
        let len = self.buckets.len();
        if steps_u64 as usize >= len {
            // Long gap: clear all buckets and jump head to the correct index
            self.buckets.fill(0);
            self.head_idx = ((self.head_idx as u64 + steps_u64) % len as u64) as usize;
            self.head_tick = now_tick;
            self.last_evicted_tick = None;
            self.last_evicted_bytes = 0;
            return;
        }
        let steps = steps_u64 as usize;
        for step in 1..=steps {
            self.head_idx = (self.head_idx + 1) % len;
            let evicted_bytes = self.buckets[self.head_idx];
            self.buckets[self.head_idx] = 0;
            if step == steps {
                if now_tick >= len as u64 {
                    self.last_evicted_tick = Some(now_tick - len as u64);
                    self.last_evicted_bytes = evicted_bytes;
                } else {
                    self.last_evicted_tick = None;
                    self.last_evicted_bytes = 0;
                }
            }
        }
        self.head_tick = now_tick;
    }

    pub fn record(&mut self, bytes: usize) {
        let now_tick = self.now_secs();
        self.advance_to(now_tick);
        self.buckets[self.head_idx] = self.buckets[self.head_idx].saturating_add(bytes as u64);
    }

    pub fn bps(&mut self, lookback: Duration) -> f64 {
        let lookback_secs = lookback.as_secs_f64();
        if lookback_secs <= 0.0 {
            return 0.0;
        }

        let len = self.buckets.len();
        if len == 0 {
            return 0.0;
        }

        let now = self.base.elapsed();
        let now_tick = now.as_secs();
        self.advance_to(now_tick);

        const MIN_SUBSEC: f64 = 1e-9;
        let mut now_subsec = f64::from(now.subsec_nanos()) * MIN_SUBSEC;
        if now_subsec == 0.0 && self.buckets[self.head_idx] > 0 {
            now_subsec = MIN_SUBSEC;
        }

        let window_end = now_tick as f64 + now_subsec;
        let window_start = window_end - lookback_secs;

        let earliest_tick = self.head_tick.saturating_sub((len - 1) as u64);
        let start_tick = if window_start <= earliest_tick as f64 {
            earliest_tick
        } else {
            window_start.floor() as u64
        };
        let start_tick = start_tick.min(self.head_tick);

        let mut total = 0.0;
        for tick in start_tick..=self.head_tick {
            let bucket_start = tick as f64;
            let bucket_end = bucket_start + 1.0;
            let overlap_start = window_start.max(bucket_start);
            let overlap_end = window_end.min(bucket_end);
            let overlap = overlap_end - overlap_start;
            if overlap <= 0.0 {
                continue;
            }

            let bucket_available = if tick == self.head_tick {
                now_subsec
            } else {
                1.0
            };
            if bucket_available <= 0.0 {
                continue;
            }
            let weight = (overlap / bucket_available).min(1.0);

            let offset = (self.head_tick - tick) as usize;
            let idx = (self.head_idx + len - offset) % len;
            total += self.buckets[idx] as f64 * weight;
        }

        if let Some(evicted_tick) = self.last_evicted_tick
            && evicted_tick + 1 == earliest_tick
        {
            let bucket_start = evicted_tick as f64;
            let bucket_end = bucket_start + 1.0;
            let overlap_start = window_start.max(bucket_start);
            let overlap_end = window_end.min(bucket_end);
            let overlap = overlap_end - overlap_start;
            if overlap > 0.0 {
                let weight = overlap.min(1.0);
                total += self.last_evicted_bytes as f64 * weight;
            }
        }

        total / lookback_secs
    }

    #[inline]
    fn now_secs(&self) -> u64 {
        self.base.elapsed().as_secs()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn assert_close(actual: f64, expected: f64) {
        let epsilon = 1e-9;
        assert!(
            (actual - expected).abs() < epsilon,
            "expected {expected}, got {actual}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn bps_is_zero_initially() {
        let mut t = SlidingThroughput::<60>::default();
        assert_close(t.bps(Duration::from_secs(60)), 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn accumulates_within_and_across_buckets() {
        let mut t = SlidingThroughput::<60>::default();

        // t = 0ms
        t.record(1_000);
        assert_close(t.bps(Duration::from_secs(60)), 1_000.0 / 60.0);

        // Still same 1s bucket
        tokio::time::advance(Duration::from_millis(400)).await;
        t.record(500);
        assert_close(t.bps(Duration::from_secs(60)), 1_500.0 / 60.0);

        // Advance into next bucket and add more
        tokio::time::advance(Duration::from_millis(600)).await; // cross to next 1s bucket
        t.record(500);
        assert_close(t.bps(Duration::from_secs(60)), 2_000.0 / 60.0);
    }

    #[tokio::test(start_paused = true)]
    async fn window_rolls_and_evicts_old_data() {
        let mut t = SlidingThroughput::<60>::default();

        // Bucket 0
        t.record(1_000);

        // Move to bucket 1 and add more
        tokio::time::advance(Duration::from_millis(1_000)).await;
        t.record(500);
        assert_close(t.bps(Duration::from_secs(60)), 1_500.0 / 60.0);

        // After exactly 60s from start, bucket 0 is still within the window
        tokio::time::advance(Duration::from_millis(59_000)).await; // total 60_000ms
        assert_close(t.bps(Duration::from_secs(60)), 1_500.0 / 60.0);

        // After 61s from start, bucket 0 falls out but bucket 1 remains
        tokio::time::advance(Duration::from_millis(1_000)).await;
        assert_close(t.bps(Duration::from_secs(60)), 500.0 / 60.0);

        // After another 1s, bucket 1 also evicted -> zero
        tokio::time::advance(Duration::from_millis(1_000)).await;
        assert_close(t.bps(Duration::from_secs(60)), 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn long_gap_clears_all_buckets() {
        let mut t = SlidingThroughput::<60>::default();
        t.record(42_000);
        assert_close(t.bps(Duration::from_secs(60)), 42_000.0 / 60.0);

        // Advance by more than the full window (61s), which should clear everything
        tokio::time::advance(Duration::from_millis(61_000)).await;
        assert_close(t.bps(Duration::from_secs(60)), 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn different_bucket_sizes() {
        // Test with 10 buckets
        let mut t10 = SlidingThroughput::<10>::default();
        t10.record(1_000);
        assert_close(t10.bps(Duration::from_secs(10)), 1_000.0 / 10.0);

        // Test with 120 buckets
        let mut t120 = SlidingThroughput::<120>::default();
        t120.record(2_000);
        assert_close(t120.bps(Duration::from_secs(120)), 2_000.0 / 120.0);

        // Verify window clamping works correctly with different sizes
        assert_close(t10.bps(Duration::from_secs(20)), 1_000.0 / 20.0);
        assert_close(t120.bps(Duration::from_secs(150)), 2_000.0 / 150.0);
    }

    #[tokio::test(start_paused = true)]
    async fn includes_previous_bucket_at_boundary() {
        let mut t = SlidingThroughput::<60>::default();
        for _ in 0..10 {
            t.record(100);
            tokio::time::advance(Duration::from_millis(100)).await;
        }

        assert_close(t.bps(Duration::from_secs(1)), 1_000.0);
    }

    #[tokio::test(start_paused = true)]
    async fn weights_partial_oldest_bucket() {
        let mut t = SlidingThroughput::<60>::default();
        t.record(1_000);

        tokio::time::advance(Duration::from_secs(1)).await;
        t.record(1_000);

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::time::advance(Duration::from_millis(500)).await;

        assert_close(t.bps(Duration::from_secs(2)), 1_500.0 / 2.0);
    }
}
