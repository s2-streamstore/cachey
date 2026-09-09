use std::time::Duration;

use tokio::time::Instant;

#[derive(Debug)]
pub struct SlidingThroughput<const NUM_BUCKETS: usize = 60> {
    buckets: Box<[u64]>,
    head_tick: u64,
    base: Instant,
}

impl<const NUM_BUCKETS: usize> Default for SlidingThroughput<NUM_BUCKETS> {
    fn default() -> Self {
        Self {
            buckets: vec![0; NUM_BUCKETS + 1].into_boxed_slice(),
            head_tick: 0,
            base: Instant::now(),
        }
    }
}

impl<const NUM_BUCKETS: usize> SlidingThroughput<NUM_BUCKETS> {
    fn advance(&mut self) {
        let now_tick = self.base.elapsed().as_secs();
        if now_tick <= self.head_tick {
            return;
        }
        let steps = now_tick - self.head_tick;
        let len = self.buckets.len();
        if steps >= len as u64 {
            self.buckets.fill(0);
        } else {
            for tick in self.head_tick + 1..=now_tick {
                self.buckets[(tick % len as u64) as usize] = 0;
            }
        }
        self.head_tick = now_tick;
    }

    pub fn record(&mut self, bytes: usize) {
        self.advance();
        let index = (self.head_tick % self.buckets.len() as u64) as usize;
        self.buckets[index] = self.buckets[index].saturating_add(bytes as u64);
    }

    /// Returns average bytes per second over the last `lookback` seconds using
    /// fully completed 1s buckets. Sub-second lookbacks clamp to 1s and missing
    /// history is treated as zero.
    pub fn bps(&mut self, lookback: Duration) -> f64 {
        if lookback.is_zero() || NUM_BUCKETS == 0 {
            return 0.0;
        }

        let lookback_seconds_f64 = lookback.as_secs_f64().max(1.0);
        let lookback_secs = lookback.as_secs().max(1);

        self.advance();

        let len = self.buckets.len();
        let window_secs = lookback_secs.min(NUM_BUCKETS as u64) as usize;

        let mut sum: u64 = 0;
        let mut index = (self.head_tick % len as u64) as usize;
        for _ in 0..window_secs {
            index = (index + len - 1) % len;
            sum = sum.saturating_add(self.buckets[index]);
        }

        sum as f64 / lookback_seconds_f64
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::SlidingThroughput;

    fn assert_close(actual: f64, expected: f64) {
        let epsilon = 1e-9;
        assert!(
            (actual - expected).abs() < epsilon,
            "expected {expected}, got {actual}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn accumulates_within_and_across_buckets() {
        let mut t = SlidingThroughput::<60>::default();

        t.record(600);
        t.record(400);
        assert_close(t.bps(Duration::from_mins(1)), 0.0);

        tokio::time::advance(Duration::from_secs(1)).await;
        assert_close(t.bps(Duration::from_mins(1)), 1_000.0 / 60.0);

        tokio::time::advance(Duration::from_millis(400)).await;
        t.record(500);
        assert_close(t.bps(Duration::from_mins(1)), 1_000.0 / 60.0);

        tokio::time::advance(Duration::from_millis(600)).await;
        assert_close(t.bps(Duration::from_mins(1)), 1_500.0 / 60.0);
    }

    #[tokio::test(start_paused = true)]
    async fn window_rolls_and_evicts_old_data() {
        let mut t = SlidingThroughput::<60>::default();

        t.record(1_000);

        tokio::time::advance(Duration::from_secs(1)).await;
        t.record(500);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_close(t.bps(Duration::from_mins(1)), 1_500.0 / 60.0);

        // After exactly 60s from start, bucket 0 is still within the window
        tokio::time::advance(Duration::from_secs(58)).await;
        assert_close(t.bps(Duration::from_mins(1)), 1_500.0 / 60.0);

        // After 61s from start, bucket 0 falls out but bucket 1 remains
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_close(t.bps(Duration::from_mins(1)), 500.0 / 60.0);

        // After another 1s, bucket 1 also evicted -> zero
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_close(t.bps(Duration::from_mins(1)), 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn long_gap_clears_all_buckets() {
        let mut t = SlidingThroughput::<60>::default();
        t.record(42_000);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_close(t.bps(Duration::from_mins(1)), 42_000.0 / 60.0);

        // Advance by more than the full window (61s), which should clear everything
        tokio::time::advance(Duration::from_secs(61)).await;
        assert_close(t.bps(Duration::from_mins(1)), 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn lookback_beyond_retained_history_counts_missing_seconds_as_zero() {
        let mut t = SlidingThroughput::<10>::default();
        t.record(1_000);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_close(t.bps(Duration::from_secs(10)), 100.0);
        assert_close(t.bps(Duration::from_secs(20)), 50.0);
    }

    #[tokio::test(start_paused = true)]
    async fn fractional_lookback_clamps_to_one_second() {
        let mut t = SlidingThroughput::<60>::default();
        t.record(1_000);
        tokio::time::advance(Duration::from_millis(1_500)).await;

        for (millis, expected) in [(500, 1_000.0), (1_000, 1_000.0), (1_500, 1_000.0 / 1.5)] {
            assert_close(t.bps(Duration::from_millis(millis)), expected);
        }
    }
}
