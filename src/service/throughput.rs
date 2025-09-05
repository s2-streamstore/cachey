use std::time::Duration;

use tokio::time::Instant;

#[derive(Debug)]
pub struct SlidingThroughput<const NUM_BUCKETS: usize = 60> {
    buckets: [u64; NUM_BUCKETS],
    head_idx: usize,
    head_tick: u64,
    base: Instant,
}

impl<const NUM_BUCKETS: usize> Default for SlidingThroughput<NUM_BUCKETS> {
    fn default() -> Self {
        Self {
            buckets: [0; NUM_BUCKETS],
            head_idx: 0,
            head_tick: 0,
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
            return;
        }
        let steps = steps_u64 as usize;
        for _ in 0..steps {
            self.head_idx = (self.head_idx + 1) % len;
            self.buckets[self.head_idx] = 0;
        }
        self.head_tick = now_tick;
    }

    pub fn record(&mut self, bytes: usize) {
        let now_tick = self.now_secs();
        self.advance_to(now_tick);
        self.buckets[self.head_idx] = self.buckets[self.head_idx].saturating_add(bytes as u64);
    }

    pub fn bps(&mut self, lookback: Duration) -> f64 {
        let now_tick = self.now_secs();
        self.advance_to(now_tick);

        let requested_secs = lookback.as_secs();
        if requested_secs == 0 {
            return 0.0;
        }

        let window_secs = requested_secs.clamp(1, NUM_BUCKETS as u64) as usize;

        // Sum last `window_secs` buckets starting from head (inclusive) going backwards
        let mut sum: u64 = 0;
        let mut idx = self.head_idx;
        for _ in 0..window_secs {
            sum = sum.saturating_add(self.buckets[idx]);
            idx = (idx + self.buckets.len() - 1) % self.buckets.len();
        }

        sum as f64 / window_secs as f64
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

        // After exactly 60s from start, bucket 0 should be evicted but bucket 1 remains
        tokio::time::advance(Duration::from_millis(59_000)).await; // total 60_000ms
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
        assert_close(t10.bps(Duration::from_secs(20)), 1_000.0 / 10.0); // clamped to 10
        assert_close(t120.bps(Duration::from_secs(150)), 2_000.0 / 120.0); // clamped to 120
    }
}
