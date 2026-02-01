use std::{sync::Arc, time::Duration};

use dashmap::DashMap;
use exponential_decay_histogram::ExponentialDecayHistogram;
use itertools::Itertools;
use parking_lot::Mutex;
use tokio::time::Instant;

use crate::types::BucketName;

const ALPHA: f64 = 0.015;
const LATENCY_SNAPSHOT_THRESHOLD: Duration = Duration::from_secs(1);
const CONSECUTIVE_FAILURE_THRESHOLD: u32 = 5;
const RECOVERY_TIME: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct BucketMetrics {
    pub error_rate: f64,
    pub circuit_breaker_open: bool,
    pub consecutive_failures: u32,
    pub latency_mean: Duration,
    pub latency_hedge: Duration,
}

#[derive(Debug, Clone, Copy)]
struct LatencyMicrosSnapshot {
    mean: u64,
    hedge: u64,
}

#[derive(Debug)]
struct BucketStats {
    last_update: Instant,
    // Simple error rate (0.0 to 1.0)
    error_rate: f64,
    // Circuit breaking
    consecutive_failures: u32,
    last_failure_time: Instant,
    // Latency tracking
    latency_micros_histogram: ExponentialDecayHistogram,
    latency_micros_snapshot: LatencyMicrosSnapshot,
    latency_micros_snapshot_at: Instant,
}

impl BucketStats {
    fn error_rate(&self, now: Instant) -> f64 {
        let elapsed = now.duration_since(self.last_update).as_secs_f64();
        self.error_rate * (-ALPHA * elapsed).exp()
    }

    fn is_circuit_open(&self, now: Instant) -> bool {
        self.consecutive_failures >= CONSECUTIVE_FAILURE_THRESHOLD
            && now.duration_since(self.last_failure_time) < RECOVERY_TIME
    }

    fn latency_micros_snapshot(
        &mut self,
        now: Instant,
        hedge_quantile: f64,
    ) -> LatencyMicrosSnapshot {
        if now.duration_since(self.latency_micros_snapshot_at) > LATENCY_SNAPSHOT_THRESHOLD {
            let new_snapshot = self.latency_micros_histogram.snapshot();
            let mean = new_snapshot.mean() as u64;
            let hedge = new_snapshot.value(hedge_quantile) as u64;
            self.latency_micros_snapshot = LatencyMicrosSnapshot { mean, hedge };
            self.latency_micros_snapshot_at = now;
        }
        self.latency_micros_snapshot
    }

    fn metrics(&mut self, now: Instant, hedge_quantile: f64) -> BucketMetrics {
        let error_rate = self.error_rate(now);
        let circuit_breaker_open = self.is_circuit_open(now);
        let consecutive_failures = self.consecutive_failures;
        let latency_micros_snapshot = self.latency_micros_snapshot(now, hedge_quantile);
        let latency_mean = Duration::from_micros(latency_micros_snapshot.mean);
        let latency_hedge = Duration::from_micros(latency_micros_snapshot.hedge);
        BucketMetrics {
            error_rate,
            circuit_breaker_open,
            consecutive_failures,
            latency_mean,
            latency_hedge,
        }
    }
}

impl Default for BucketStats {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            last_update: now,
            error_rate: 0.0,
            consecutive_failures: 0,
            last_failure_time: now,
            latency_micros_histogram: ExponentialDecayHistogram::builder().alpha(ALPHA).build(),
            latency_micros_snapshot: LatencyMicrosSnapshot { mean: 0, hedge: 0 },
            latency_micros_snapshot_at: now - LATENCY_SNAPSHOT_THRESHOLD,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BucketedStats {
    by_bucket: Arc<DashMap<BucketName, Mutex<BucketStats>>>,
    hedge_latency_quantile: f64,
}

impl BucketedStats {
    pub fn new(hedge_latency_quantile: f64) -> Self {
        let by_bucket = Arc::new(DashMap::new());
        Self {
            by_bucket,
            hedge_latency_quantile,
        }
    }

    pub fn observe(&self, bucket: BucketName, outcome: Result<Duration, ()>) {
        let now = Instant::now();
        let entry = self.by_bucket.entry(bucket).or_default();
        let mut stats = entry.lock();

        stats.error_rate = stats.error_rate(now) * (1.0 - ALPHA);
        if let Ok(lat) = outcome {
            // Success: update error rate and reset failures
            stats.consecutive_failures = 0;
            stats
                .latency_micros_histogram
                .update_at(now.into_std(), lat.as_micros() as i64);
        } else {
            // Failure: update error rate and increment failures
            stats.consecutive_failures += 1;
            stats.last_failure_time = now;
            stats.error_rate += ALPHA;
        }
        stats.last_update = now;
    }

    pub fn attempt_order<'a>(
        &'a self,
        buckets: impl Iterator<Item = &'a BucketName>,
    ) -> impl Iterator<Item = usize> {
        let now = Instant::now();
        buckets
            .enumerate()
            .sorted_by_cached_key(|(i, bucket)| self.score(now, bucket, *i))
            .map(|(i, _bucket)| i)
    }

    /// Calculate a score for bucket selection. Lower scores are preferred.
    ///
    /// The scoring formula balances three factors:
    /// 1. **Position penalty** (idx * 200): Respects client's bucket ordering preference
    /// 2. **Latency penalty** (µs / 100): Based on observed performance
    /// 3. **Error penalty**: Based on error rate or circuit breaker state
    ///
    /// For unknown buckets, we assume 500ms latency (5000 points) to ensure:
    /// - Known good buckets are preferred over unknown ones
    /// - Client ordering is preserved even with typical S3 latencies (~200ms same-region)
    /// - The system still explores unknown buckets when known ones fail
    ///
    /// When a circuit breaker is open (10000 points penalty), the bucket is strongly
    /// deprioritized to ensure failed buckets are avoided in favor of any healthy bucket.
    fn score(&self, now: Instant, bucket: &BucketName, idx: usize) -> u64 {
        let base = (idx as u64) * 200;
        self.by_bucket.get(bucket).map_or(base + 5000, |s| {
            let mut guard = s.lock();

            // Calculate latency component: 1 point per 100 µs = 0.1 ms
            // - S3 Express same-AZ: ~4ms → 40 points
            // - S3 Express cross-AZ: ~8ms → 80 points
            // - Standard S3 same-region: ~200ms → 2000 points
            // - Standard S3 cross-region: 300-1000ms → 3000-10000 points
            let lat = guard
                .latency_micros_snapshot(now, self.hedge_latency_quantile)
                .mean
                / 100;

            // Calculate error component based on circuit breaker state
            let err = if guard.is_circuit_open(now) {
                10000
            } else {
                (guard.error_rate(now) * 100.0).round() as u64
            };

            base + err + lat
        })
    }

    /// Returns `Duration::ZERO` if hedging is disabled or no latency datapoints are available.
    pub fn hedging_threshold(&self, bucket: &BucketName, now: Instant) -> Duration {
        if self.hedge_latency_quantile == 0.0 {
            // Disable hedging when quantile is 0
            return Duration::ZERO;
        }
        self.by_bucket.get(bucket).map_or(Duration::ZERO, |s| {
            Duration::from_micros(
                s.lock()
                    .latency_micros_snapshot(now, self.hedge_latency_quantile)
                    .hedge,
            )
        })
    }

    pub fn export_bucket_metrics(&self, mut f: impl FnMut(&BucketName, &BucketMetrics)) {
        let now = Instant::now();
        for entry in self.by_bucket.iter() {
            let bucket = entry.key();
            let metrics = entry
                .value()
                .lock()
                .metrics(now, self.hedge_latency_quantile);
            f(bucket, &metrics);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_stats() -> BucketedStats {
        BucketedStats::new(0.9) // 90th percentile for hedging
    }

    fn get_attempt_order(stats: &BucketedStats, buckets: &[BucketName]) -> Vec<BucketName> {
        stats
            .attempt_order(buckets.iter())
            .map(|i| buckets[i].clone())
            .collect()
    }

    #[test]
    fn test_basic_scoring() {
        let stats = make_test_stats();
        let bucket1 = BucketName::new("bucket1").unwrap();
        let bucket2 = BucketName::new("bucket2").unwrap();
        let bucket3 = BucketName::new("bucket3").unwrap();

        let now = Instant::now();

        // Test base scoring without any data (includes 5000 point penalty for unknown)
        assert_eq!(stats.score(now, &bucket1, 0), 5000);
        assert_eq!(stats.score(now, &bucket2, 1), 5200);
        assert_eq!(stats.score(now, &bucket3, 2), 5400);
    }

    #[test]
    fn test_latency_scoring() {
        let stats = make_test_stats();
        let bucket = BucketName::new("test-bucket").unwrap();

        // Record some successful requests with different latencies
        stats.observe(bucket.clone(), Ok(Duration::from_millis(10)));
        stats.observe(bucket.clone(), Ok(Duration::from_millis(20)));
        stats.observe(bucket.clone(), Ok(Duration::from_millis(30)));

        let now = Instant::now();
        let score = stats.score(now, &bucket, 0);

        // Score should be 0 (base) + 0 (no errors) + ~200 (20ms mean / 100μs)
        // Mean of 10, 20, 30 ms = 20ms = 20,000μs / 100 = 200 points
        assert!((150..=250).contains(&score), "Score was {score}");
    }

    #[test]
    fn test_error_rate_accumulation() {
        let stats = make_test_stats();
        let bucket = BucketName::new("error-bucket").unwrap();

        // Record a few errors
        stats.observe(bucket.clone(), Err(()));
        stats.observe(bucket.clone(), Err(()));
        stats.observe(bucket.clone(), Err(()));

        let now = Instant::now();
        let score = stats.score(now, &bucket, 0);

        // With 3 errors and ALPHA=0.015: error_rate ≈ 0.044
        // Score should be 0 (base) + ~4 (0.044 * 100) + 0 (no latency)
        assert!(score <= 10, "Score was {score}");
    }

    #[test]
    fn test_circuit_breaker_open() {
        let stats = make_test_stats();
        let bucket = BucketName::new("circuit-bucket").unwrap();

        // Trigger circuit breaker with 5 consecutive failures
        for _ in 0..5 {
            stats.observe(bucket.clone(), Err(()));
        }

        let now = Instant::now();
        let score = stats.score(now, &bucket, 0);

        // Score should be 0 (base) + 10000 (circuit open) + 0 (no latency)
        assert_eq!(
            score, 10000,
            "Circuit breaker didn't open, score was {score}",
        );
    }

    #[tokio::test]
    async fn test_error_rate_decay() {
        tokio::time::pause();

        let stats = make_test_stats();
        let bucket = BucketName::new("decay-bucket").unwrap();

        // Record some errors to build up error rate
        stats.observe(bucket.clone(), Err(()));
        stats.observe(bucket.clone(), Err(()));
        stats.observe(bucket.clone(), Err(()));

        // Check initial error penalty (error_rate ≈ 0.044 -> ~4 points)
        let now = Instant::now();
        let initial_score = stats.score(now, &bucket, 0);
        assert!(initial_score <= 10, "Initial score was {initial_score}");

        // Advance time by 10 seconds to allow decay
        // After ~46s (one half-life), error should decay by 50%
        tokio::time::advance(Duration::from_secs(46)).await;
        let now = Instant::now();
        let decayed_score = stats.score(now, &bucket, 0);
        // After one half-life (~46s), error rate should be ~50% of original
        // Initial score ~4, decayed should be ~2
        assert!(
            decayed_score <= initial_score / 2 + 1,
            "Error rate didn't decay by half: {initial_score} -> {decayed_score}",
        );

        // Advance time by another 50 seconds
        tokio::time::advance(Duration::from_secs(50)).await;

        // Score should be nearly zero (just base score)
        let now = Instant::now();
        let final_score = stats.score(now, &bucket, 0);
        assert!(
            final_score <= 6,
            "Score didn't decay sufficiently: {final_score}"
        );
    }

    #[tokio::test]
    async fn test_latency_snapshot_caching() {
        tokio::time::pause();

        let stats = make_test_stats();
        let bucket = BucketName::new("snapshot-bucket").unwrap();

        // Record initial latencies
        stats.observe(bucket.clone(), Ok(Duration::from_millis(10)));
        stats.observe(bucket.clone(), Ok(Duration::from_millis(20)));

        // Get initial hedging threshold (forces snapshot)
        let now = Instant::now();
        let threshold1 = stats.hedging_threshold(&bucket, now);

        // Record more latencies immediately
        stats.observe(bucket.clone(), Ok(Duration::from_millis(100)));
        stats.observe(bucket.clone(), Ok(Duration::from_millis(200)));

        // Threshold should be cached (not updated yet)
        let now = Instant::now();
        let threshold2 = stats.hedging_threshold(&bucket, now);
        assert_eq!(threshold1, threshold2, "Snapshot should be cached");

        // Advance time past snapshot threshold (1 second)
        tokio::time::advance(LATENCY_SNAPSHOT_THRESHOLD + Duration::from_millis(1)).await;

        // Now threshold should reflect new latencies
        let now = Instant::now();
        let threshold3 = stats.hedging_threshold(&bucket, now);
        assert!(
            threshold3 > threshold2,
            "Snapshot should be updated: {threshold2:?} -> {threshold3:?}",
        );
    }

    #[tokio::test]
    async fn test_simple_circuit_breaker_recovery() {
        tokio::time::pause();
        let stats = make_test_stats();
        let bucket = BucketName::new("simple-bucket").unwrap();

        // Trigger circuit breaker
        for _ in 0..5 {
            stats.observe(bucket.clone(), Err(()));
        }

        // Verify it's open
        let now = Instant::now();
        assert_eq!(stats.score(now, &bucket, 0), 10000);

        // Still open before recovery time
        tokio::time::advance(RECOVERY_TIME.checked_sub(Duration::from_secs(1)).unwrap()).await;
        let now = Instant::now();
        assert_eq!(stats.score(now, &bucket, 0), 10000);

        // After recovery time, should be closed (ready to try again)
        tokio::time::advance(Duration::from_secs(2)).await;
        let now = Instant::now();
        let score = stats.score(now, &bucket, 0);
        assert!(score < 100, "Circuit should be closed after recovery time");

        // If it fails again, back to open
        for _ in 0..5 {
            stats.observe(bucket.clone(), Err(()));
        }
        let now = Instant::now();
        assert_eq!(stats.score(now, &bucket, 0), 10000);
    }

    #[test]
    fn test_error_rate_simple() {
        let stats = make_test_stats();
        let bucket = BucketName::new("simple-bucket").unwrap();

        // Mix of successes and failures
        stats.observe(bucket.clone(), Ok(Duration::from_millis(10)));
        stats.observe(bucket.clone(), Err(()));
        stats.observe(bucket.clone(), Ok(Duration::from_millis(10)));
        stats.observe(bucket.clone(), Err(()));
        stats.observe(bucket.clone(), Ok(Duration::from_millis(10)));

        let now = Instant::now();
        let score = stats.score(now, &bucket, 0);

        // Mixed traffic (3 success, 2 failures) gives moderate error rate
        // With latency ~10ms = 100 points, error component should be small
        assert!(
            (100..=200).contains(&score),
            "Mixed traffic score was {score}",
        );
    }

    #[test]
    fn test_attempt_order() {
        let stats = make_test_stats();
        let bucket1 = BucketName::new("fast-bucket").unwrap();
        let bucket2 = BucketName::new("slow-bucket").unwrap();
        let bucket3 = BucketName::new("error-bucket").unwrap();

        // Set up different characteristics
        // bucket1: fast (5ms)
        for _ in 0..5 {
            stats.observe(bucket1.clone(), Ok(Duration::from_millis(5)));
        }

        // bucket2: slow (50ms)
        for _ in 0..5 {
            stats.observe(bucket2.clone(), Ok(Duration::from_millis(50)));
        }

        // bucket3: some errors
        stats.observe(bucket3.clone(), Ok(Duration::from_millis(10)));
        stats.observe(bucket3.clone(), Err(()));
        stats.observe(bucket3.clone(), Err(()));

        // Test ordering
        let buckets = vec![bucket1.clone(), bucket2.clone(), bucket3.clone()];
        let ordered = get_attempt_order(&stats, &buckets);

        // Should be ordered: fast, error (has errors but still better than slow), slow
        assert_eq!(ordered[0], bucket1);
        assert_eq!(ordered[1], bucket3);
        assert_eq!(ordered[2], bucket2);
    }

    #[test]
    fn test_hedging_threshold() {
        let stats = make_test_stats();
        let bucket = BucketName::new("hedge-bucket").unwrap();

        // Record latencies with some variance
        let latencies = vec![5, 10, 15, 20, 25, 30, 35, 40, 45, 50];
        for ms in latencies {
            stats.observe(bucket.clone(), Ok(Duration::from_millis(ms)));
        }

        let now = Instant::now();
        let threshold = stats.hedging_threshold(&bucket, now);

        // 90th percentile of 5-50ms should be around 45ms
        assert!(
            threshold >= Duration::from_millis(40) && threshold <= Duration::from_millis(50),
            "Hedging threshold was {threshold:?}",
        );
    }

    #[test]
    fn test_mixed_scenario() {
        let stats = make_test_stats();
        let bucket1 = BucketName::new("primary").unwrap();
        let bucket2 = BucketName::new("secondary").unwrap();
        let bucket3 = BucketName::new("tertiary").unwrap();

        // Simulate realistic traffic patterns
        // Primary: mostly good, occasional error
        for i in 0..20 {
            if i % 10 == 0 {
                stats.observe(bucket1.clone(), Err(()));
            } else {
                stats.observe(bucket1.clone(), Ok(Duration::from_millis(5 + i % 3)));
            }
        }

        // Secondary: slower but reliable
        for i in 0..20 {
            stats.observe(bucket2.clone(), Ok(Duration::from_millis(20 + i % 5)));
        }

        // Tertiary: experiencing issues
        for i in 0..20 {
            if i < 10 {
                stats.observe(bucket3.clone(), Ok(Duration::from_millis(15)));
            } else {
                stats.observe(bucket3.clone(), Err(()));
            }
        }

        let buckets = vec![bucket1.clone(), bucket2.clone(), bucket3.clone()];
        let ordered = get_attempt_order(&stats, &buckets);

        // Primary should still be first (low latency, few errors)
        assert_eq!(ordered[0], bucket1);
        // Secondary should be second (higher latency but no errors)
        assert_eq!(ordered[1], bucket2);
        // Tertiary should be last (circuit breaker triggered)
        assert_eq!(ordered[2], bucket3);
    }

    #[test]
    fn test_hedging_disabled_when_quantile_zero() {
        let stats = BucketedStats::new(0.0); // Quantile = 0 disables hedging
        let bucket = BucketName::new("test-bucket").unwrap();

        // Record some latencies
        for ms in [10, 20, 30, 40, 50] {
            stats.observe(bucket.clone(), Ok(Duration::from_millis(ms)));
        }

        let now = Instant::now();
        let threshold = stats.hedging_threshold(&bucket, now);

        // When quantile is 0, hedging should be disabled (threshold = 0)
        assert_eq!(
            threshold,
            Duration::ZERO,
            "Hedging should be disabled when quantile is 0"
        );
    }

    #[test]
    fn test_unknown_bucket_penalty() {
        let stats = make_test_stats();
        let local_bucket = BucketName::new("local").unwrap();
        let remote_bucket = BucketName::new("remote").unwrap();

        // Record some data for local bucket (5ms latency)
        for _ in 0..5 {
            stats.observe(local_bucket.clone(), Ok(Duration::from_millis(5)));
        }

        // Remote bucket has no data

        let now = Instant::now();

        // Local bucket: base(0) + err(0) + lat(50) = 50
        let local_score = stats.score(now, &local_bucket, 0);
        assert!(
            (40..=60).contains(&local_score),
            "Local score was {local_score}",
        );

        // Remote bucket: base(200) + default_penalty(5000) = 5200
        let remote_score = stats.score(now, &remote_bucket, 1);
        assert_eq!(remote_score, 5200);

        // Verify ordering - local should come first
        let buckets = vec![local_bucket.clone(), remote_bucket.clone()];
        let ordered = get_attempt_order(&stats, &buckets);
        assert_eq!(ordered[0], local_bucket);
        assert_eq!(ordered[1], remote_bucket);
    }

    #[test]
    fn test_regional_bucket_ordering_preserved() {
        let stats = make_test_stats();
        let us_east = BucketName::new("us-east-1").unwrap();
        let eu_west = BucketName::new("eu-west-1").unwrap();
        let ap_south = BucketName::new("ap-south-1").unwrap();

        // Simulate typical S3 latencies from US East Coast user
        // us-east-1: 200ms (same region S3 GET)
        for _ in 0..10 {
            stats.observe(us_east.clone(), Ok(Duration::from_millis(200)));
        }

        // eu-west-1 and ap-south-1 are unknown (no observations yet)

        // Client provides buckets in their preferred order
        let buckets = vec![us_east.clone(), eu_west.clone(), ap_south.clone()];
        let ordered = get_attempt_order(&stats, &buckets);

        // Should preserve client ordering - not prefer unknown eu-west over known us-east
        // even though us-east has 200ms latency (2000 points) vs unknown default (5000 points)
        assert_eq!(
            ordered[0], us_east,
            "Should keep us-east-1 first despite 200ms latency"
        );
        assert_eq!(ordered[1], eu_west, "Should keep eu-west-1 second");
        assert_eq!(ordered[2], ap_south, "Should keep ap-south-1 third");

        // Now simulate eu-west actually having higher latency when tried (cross-region)
        for _ in 0..5 {
            stats.observe(eu_west.clone(), Ok(Duration::from_millis(400)));
        }

        let buckets = vec![us_east.clone(), eu_west, ap_south];
        let ordered = get_attempt_order(&stats, &buckets);

        // Should still prefer us-east as it has lower latency
        assert_eq!(ordered[0], us_east, "us-east should remain preferred");
    }

    #[test]
    fn test_realistic_s3_latencies() {
        let stats = make_test_stats();
        let primary = BucketName::new("us-east-1").unwrap();
        let secondary = BucketName::new("us-west-2").unwrap();
        let tertiary = BucketName::new("eu-central-1").unwrap();

        // Simulate realistic S3 GET latencies
        // primary: 180-220ms (same region, varies with S3 load)
        stats.observe(primary.clone(), Ok(Duration::from_millis(180)));
        stats.observe(primary.clone(), Ok(Duration::from_millis(200)));
        stats.observe(primary.clone(), Ok(Duration::from_millis(220)));
        stats.observe(primary.clone(), Ok(Duration::from_millis(190)));
        stats.observe(primary.clone(), Ok(Duration::from_millis(210)));

        // secondary: 350-450ms (cross-region US)
        stats.observe(secondary.clone(), Ok(Duration::from_millis(350)));
        stats.observe(secondary.clone(), Ok(Duration::from_millis(400)));
        stats.observe(secondary.clone(), Ok(Duration::from_millis(450)));
        stats.observe(secondary.clone(), Ok(Duration::from_millis(380)));

        // tertiary is unknown (not yet tried)

        // With realistic latencies, client ordering should be preserved
        let buckets = vec![primary.clone(), secondary.clone(), tertiary.clone()];
        let ordered = get_attempt_order(&stats, &buckets);

        assert_eq!(ordered[0], primary, "Primary (200ms avg) should stay first");
        assert_eq!(
            ordered[1], secondary,
            "Secondary (395ms avg) should stay second"
        );
        assert_eq!(ordered[2], tertiary, "Unknown tertiary should stay third");

        // Even with some errors on primary, it should still be preferred if error rate is low
        stats.observe(primary.clone(), Err(())); // 1 error out of 6 attempts = ~16% error rate

        let buckets = vec![primary.clone(), secondary, tertiary];
        let ordered = get_attempt_order(&stats, &buckets);

        // Primary: base(0) + err(16) + lat(2000) = 2016
        // Secondary: base(200) + err(0) + lat(3950) = 4150
        // Tertiary: base(400) + unknown(5000) = 5400
        assert_eq!(
            ordered[0], primary,
            "Primary with 16% errors still better than 400ms secondary"
        );
    }

    #[test]
    fn test_circuit_breaker_prioritization() {
        let stats = make_test_stats();
        let primary = BucketName::new("us-east-1").unwrap();
        let secondary = BucketName::new("us-west-2").unwrap();
        let tertiary = BucketName::new("eu-central-1").unwrap();

        // Set up buckets with realistic S3 latencies
        // primary: 200ms (same region)
        for _ in 0..10 {
            stats.observe(primary.clone(), Ok(Duration::from_millis(200)));
        }

        // secondary: 400ms (cross-region) but then fails
        for _ in 0..5 {
            stats.observe(secondary.clone(), Ok(Duration::from_millis(400)));
        }
        // Trigger circuit breaker on secondary
        for _ in 0..5 {
            stats.observe(secondary.clone(), Err(()));
        }

        // tertiary: unknown

        let buckets = vec![primary.clone(), secondary.clone(), tertiary.clone()];
        let ordered = get_attempt_order(&stats, &buckets);

        // Even though secondary was the client's second choice, it should be last
        // because its circuit is open
        // Scores:
        // - primary: base(0) + lat(2000) = 2000
        // - secondary: base(200) + err(10000) + lat(4000) = 14200
        // - tertiary: base(400) + unknown(5000) = 5400
        assert_eq!(ordered[0], primary, "Healthy primary should be first");
        assert_eq!(ordered[1], tertiary, "Unknown tertiary should be second");
        assert_eq!(ordered[2], secondary, "Failed secondary should be last");
    }

    #[test]
    fn test_s3_express_buckets() {
        let stats = make_test_stats();
        let same_az = BucketName::new("s3express-use1-az1-same").unwrap();
        let nearby_az = BucketName::new("s3express-use1-az2-nearby").unwrap();
        let far_az = BucketName::new("s3express-use1-az3-far").unwrap();

        // All S3 Express, but with different AZ latencies
        // same_az: 3-5ms (average ~4ms) - same AZ as compute
        stats.observe(same_az.clone(), Ok(Duration::from_millis(3)));
        stats.observe(same_az.clone(), Ok(Duration::from_millis(4)));
        stats.observe(same_az.clone(), Ok(Duration::from_millis(4)));
        stats.observe(same_az.clone(), Ok(Duration::from_millis(5)));
        stats.observe(same_az.clone(), Ok(Duration::from_millis(4)));

        // nearby_az: 6-8ms (average ~7ms) - adjacent AZ
        stats.observe(nearby_az.clone(), Ok(Duration::from_millis(6)));
        stats.observe(nearby_az.clone(), Ok(Duration::from_millis(7)));
        stats.observe(nearby_az.clone(), Ok(Duration::from_millis(7)));
        stats.observe(nearby_az.clone(), Ok(Duration::from_millis(8)));
        stats.observe(nearby_az.clone(), Ok(Duration::from_millis(7)));

        // far_az: 10-14ms (average ~12ms) - furthest AZ in region
        stats.observe(far_az.clone(), Ok(Duration::from_millis(10)));
        stats.observe(far_az.clone(), Ok(Duration::from_millis(12)));
        stats.observe(far_az.clone(), Ok(Duration::from_millis(12)));
        stats.observe(far_az.clone(), Ok(Duration::from_millis(13)));
        stats.observe(far_az.clone(), Ok(Duration::from_millis(14)));

        // Test client preference ordering
        let buckets = vec![nearby_az.clone(), far_az.clone(), same_az.clone()];
        let ordered = get_attempt_order(&stats, &buckets);

        // With small latency differences, position penalties dominate
        // Scores:
        // - nearby_az: base(0) + lat(70) = 70
        // - far_az: base(200) + lat(120) = 320
        // - same_az: base(400) + lat(40) = 440
        assert_eq!(
            ordered[0], nearby_az,
            "Client preference preserved despite 3ms penalty"
        );
        assert_eq!(ordered[1], far_az);
        assert_eq!(ordered[2], same_az);

        // Test when latency differences are more significant
        let buckets = vec![far_az.clone(), same_az.clone(), nearby_az.clone()];
        let ordered = get_attempt_order(&stats, &buckets);

        // Even 8ms difference (12ms vs 4ms) isn't enough to override position 0 vs 1
        // Scores:
        // - far_az: base(0) + lat(120) = 120
        // - same_az: base(200) + lat(40) = 240
        // - nearby_az: base(400) + lat(70) = 470
        assert_eq!(
            ordered[0], far_az,
            "8ms latency difference doesn't override position preference"
        );
        assert_eq!(ordered[1], same_az);
        assert_eq!(ordered[2], nearby_az);

        // Print scores to show the relationships
        let now = Instant::now();
        println!(
            "All S3 Express - Same AZ: {} points, Nearby AZ: {} points, Far AZ: {} points",
            stats.score(now, &same_az, 0),
            stats.score(now, &nearby_az, 0),
            stats.score(now, &far_az, 0)
        );
        println!(
            "Latency differences: Same→Nearby: {}ms, Same→Far: {}ms",
            7 - 4,
            12 - 4
        );
    }

    #[test]
    fn test_s3_express_reordering_threshold() {
        let stats = make_test_stats();
        let same_az = BucketName::new("s3express-use1-az1-same").unwrap();
        let cross_az = BucketName::new("s3express-use1-az2-cross").unwrap();

        // Test the threshold where latency difference overcomes position penalty
        // Position penalty is 200 points, so we need >20ms difference to override

        // same_az: consistent 4ms
        for _ in 0..10 {
            stats.observe(same_az.clone(), Ok(Duration::from_millis(4)));
        }

        // cross_az: 25ms (21ms higher - just enough to override position penalty)
        for _ in 0..10 {
            stats.observe(cross_az.clone(), Ok(Duration::from_millis(25)));
        }

        // Test with cross_az in position 0
        let buckets = vec![cross_az.clone(), same_az.clone()];
        let ordered = get_attempt_order(&stats, &buckets);

        // With 21ms difference (210 points), same_az at position 1 should win
        // Scores:
        // - cross_az: base(0) + lat(250) = 250
        // - same_az: base(200) + lat(40) = 240
        assert_eq!(
            ordered[0], same_az,
            "21ms latency difference overrides position preference"
        );
        assert_eq!(ordered[1], cross_az);

        // Now test with only 19ms difference (not enough to override)
        let nearby_az = BucketName::new("s3express-use1-az3-nearby").unwrap();
        for _ in 0..10 {
            stats.observe(nearby_az.clone(), Ok(Duration::from_millis(23)));
        }

        let buckets = vec![nearby_az.clone(), same_az.clone()];
        let ordered = get_attempt_order(&stats, &buckets);

        // With 19ms difference (190 points), position penalty wins
        // Scores:
        // - nearby_az: base(0) + lat(230) = 230
        // - same_az: base(200) + lat(40) = 240
        assert_eq!(
            ordered[0], nearby_az,
            "19ms difference isn't enough to override position"
        );
        assert_eq!(ordered[1], same_az);

        // Show the exact threshold
        let now = Instant::now();
        println!(
            "Reordering threshold: >20ms latency difference needed to override position penalty of 200 points"
        );
        println!(
            "25ms bucket at pos 0: {} points, 4ms bucket at pos 1: {} points (reorders)",
            stats.score(now, &cross_az, 0),
            stats.score(now, &same_az, 1)
        );
        println!(
            "23ms bucket at pos 0: {} points, 4ms bucket at pos 1: {} points (no reorder)",
            stats.score(now, &nearby_az, 0),
            stats.score(now, &same_az, 1)
        );
    }
}
