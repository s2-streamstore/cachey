use std::time::Duration;

use tokio::time::Instant;

#[derive(Debug)]
pub struct SlidingThroughput<const NUM_BUCKETS: usize = 60> {
    buckets: Vec<u64>,
    head_idx: usize,
    head_tick: u64,
    base: Instant,
}

impl<const NUM_BUCKETS: usize> Default for SlidingThroughput<NUM_BUCKETS> {
    fn default() -> Self {
        Self {
            buckets: vec![0; NUM_BUCKETS + 1],
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

    /// Returns average bytes per second over the last `lookback` seconds using
    /// fully completed 1s buckets. Sub-second lookbacks clamp to 1s and missing
    /// history is treated as zero.
    pub fn bps(&mut self, lookback: Duration) -> f64 {
        if lookback.is_zero() || NUM_BUCKETS == 0 {
            return 0.0;
        }

        let lookback_secs = lookback.as_secs().max(1);

        let now_tick = self.now_secs();
        self.advance_to(now_tick);

        let len = self.buckets.len();
        let window_secs = lookback_secs.min(NUM_BUCKETS as u64) as usize;
        if window_secs == 0 {
            return 0.0;
        }

        let mut sum: u64 = 0;
        let mut idx = (self.head_idx + len - 1) % len;
        for _ in 0..window_secs {
            sum = sum.saturating_add(self.buckets[idx]);
            idx = (idx + len - 1) % len;
        }

        sum as f64 / lookback_secs as f64
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
        assert_close(t.bps(Duration::from_secs(60)), 0.0);

        tokio::time::advance(Duration::from_millis(1_000)).await;
        assert_close(t.bps(Duration::from_secs(60)), 1_000.0 / 60.0);

        tokio::time::advance(Duration::from_millis(400)).await;
        t.record(500);
        assert_close(t.bps(Duration::from_secs(60)), 1_000.0 / 60.0);

        tokio::time::advance(Duration::from_millis(600)).await;
        assert_close(t.bps(Duration::from_secs(60)), 1_500.0 / 60.0);
    }

    #[tokio::test(start_paused = true)]
    async fn window_rolls_and_evicts_old_data() {
        let mut t = SlidingThroughput::<60>::default();

        // Bucket 0
        t.record(1_000);

        // Move to bucket 1 and add more
        tokio::time::advance(Duration::from_millis(1_000)).await;
        t.record(500);
        tokio::time::advance(Duration::from_millis(1_000)).await;
        assert_close(t.bps(Duration::from_secs(60)), 1_500.0 / 60.0);

        // After exactly 60s from start, bucket 0 is still within the window
        tokio::time::advance(Duration::from_millis(58_000)).await; // total 60_000ms
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
        tokio::time::advance(Duration::from_millis(1_000)).await;
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
        tokio::time::advance(Duration::from_millis(1_000)).await;
        assert_close(t10.bps(Duration::from_secs(10)), 1_000.0 / 10.0);

        // Test with 120 buckets
        let mut t120 = SlidingThroughput::<120>::default();
        t120.record(2_000);
        tokio::time::advance(Duration::from_millis(1_000)).await;
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
    async fn sub_second_lookback_clamps_to_one_second() {
        let mut t = SlidingThroughput::<60>::default();
        t.record(1_000);
        tokio::time::advance(Duration::from_millis(1_000)).await;

        assert_close(t.bps(Duration::from_millis(500)), 1_000.0);
        assert_close(t.bps(Duration::from_secs(1)), 1_000.0);
    }

    #[tokio::test(start_paused = true)]
    async fn excludes_current_partial_bucket() {
        let mut t = SlidingThroughput::<60>::default();
        t.record(1_000);

        tokio::time::advance(Duration::from_millis(500)).await;
        assert_close(t.bps(Duration::from_secs(1)), 0.0);

        tokio::time::advance(Duration::from_millis(500)).await;
        assert_close(t.bps(Duration::from_secs(1)), 1_000.0);
    }
}
