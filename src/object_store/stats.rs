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
const POSITION_PENALTY: u64 = 2_000;
const UNKNOWN_BUCKET_PENALTY: u64 = 5000;
const ERROR_RATE_SCORE_MULTIPLIER: f64 = 100_000.0;
const ERROR_RATE_MAX: f64 = 1.0;
const CIRCUIT_OPEN_SCORE_PENALTY: u64 = 1_000_000;

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

    fn is_circuit_open(&mut self, now: Instant) -> bool {
        if self.consecutive_failures < CONSECUTIVE_FAILURE_THRESHOLD {
            return false;
        }

        if now.duration_since(self.last_failure_time) < RECOVERY_TIME {
            return true;
        }

        self.consecutive_failures = 0;
        false
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

        let decayed_error_rate = stats.error_rate(now);
        if let Ok(latency) = outcome {
            stats.error_rate = decayed_error_rate * (1.0 - ALPHA);
            stats.consecutive_failures = 0;
            stats
                .latency_micros_histogram
                .update_at(now.into_std(), latency.as_micros() as i64);
        } else {
            stats.error_rate = (decayed_error_rate * (1.0 - ALPHA) + ALPHA).min(ERROR_RATE_MAX);
            stats.consecutive_failures += 1;
            stats.last_failure_time = now;
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
            .sorted_by_cached_key(|(i, bucket)| (self.score(now, bucket, *i), *i))
            .map(|(i, _bucket)| i)
    }

    /// Calculate a score for bucket selection. Lower scores are preferred.
    ///
    /// The scoring formula balances three factors:
    /// 1. **Position penalty** (idx * 2000): Strongly respects client bucket ordering preference
    /// 2. **Latency penalty** (µs / 100): Based on observed performance
    /// 3. **Error penalty**: Based on error rate or circuit breaker state
    ///
    /// For unknown buckets, we assume 500ms latency (5000 points) to ensure:
    /// - Known good buckets are preferred over unknown ones
    /// - Client ordering is preserved even with typical S3 latencies (~200ms same-region)
    /// - The system still explores unknown buckets when known ones fail
    ///
    /// Error penalties are intentionally weighted orders of magnitude above latency so
    /// healthy buckets are preferred over slightly faster but failing ones.
    fn score(&self, now: Instant, bucket: &BucketName, idx: usize) -> u64 {
        let base = (idx as u64) * POSITION_PENALTY;
        self.by_bucket
            .get(bucket)
            .map_or(base + UNKNOWN_BUCKET_PENALTY, |s| {
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
                    CIRCUIT_OPEN_SCORE_PENALTY
                } else {
                    (guard.error_rate(now) * ERROR_RATE_SCORE_MULTIPLIER).round() as u64
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
        assert_eq!(stats.score(now, &bucket1, 0), UNKNOWN_BUCKET_PENALTY);
        assert_eq!(
            stats.score(now, &bucket2, 1),
            UNKNOWN_BUCKET_PENALTY + POSITION_PENALTY
        );
        assert_eq!(
            stats.score(now, &bucket3, 2),
            UNKNOWN_BUCKET_PENALTY + 2 * POSITION_PENALTY
        );
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

        // With 3 errors and ALPHA=0.015: error_rate = 0.045 (minus tiny time decay)
        // Score should be 0 (base) + ~4500 (0.045 * 100_000) + 0 (no latency)
        assert!((4300..=4500).contains(&score), "Score was {score}");
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

        // Score should be 0 (base) + 1_000_000 (circuit open) + 0 (no latency)
        assert_eq!(
            score, CIRCUIT_OPEN_SCORE_PENALTY,
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

        // Check initial error penalty (error_rate ≈ 0.045 -> ~4500 points)
        let now = Instant::now();
        let initial_score = stats.score(now, &bucket, 0);
        assert!(initial_score >= 4300, "Initial score was {initial_score}");

        // Advance time by 10 seconds to allow decay
        // After ~46s (one half-life), error should decay by 50%
        tokio::time::advance(Duration::from_secs(46)).await;
        let now = Instant::now();
        let decayed_score = stats.score(now, &bucket, 0);
        // After one half-life (~46s), error rate should be ~50% of original.
        assert!(
            decayed_score <= initial_score / 2 + 100,
            "Error rate didn't decay by half: {initial_score} -> {decayed_score}",
        );

        // Advance time by another 50 seconds
        tokio::time::advance(Duration::from_secs(50)).await;

        // Score should be nearly zero (just base score)
        let now = Instant::now();
        let final_score = stats.score(now, &bucket, 0);
        assert!(
            final_score < initial_score / 3,
            "Score didn't decay sufficiently: {final_score}"
        );
    }

    #[tokio::test]
    async fn test_success_without_elapsed_time_reduces_error_penalty() {
        tokio::time::pause();

        let stats = make_test_stats();
        let bucket = BucketName::new("no-elapsed-success").unwrap();

        for _ in 0..3 {
            stats.observe(bucket.clone(), Err(()));
        }

        let score_before = stats.score(Instant::now(), &bucket, 0);
        stats.observe(bucket.clone(), Ok(Duration::ZERO));
        let score_after = stats.score(Instant::now(), &bucket, 0);

        assert!(
            score_after < score_before,
            "Success should reduce error penalty even without elapsed time: {score_before} -> {score_after}"
        );
    }

    #[tokio::test]
    async fn test_success_traffic_drives_error_rate_down() {
        tokio::time::pause();

        let stats = make_test_stats();
        let bucket = BucketName::new("success-traffic-reduces-errors").unwrap();

        stats.observe(bucket.clone(), Err(()));

        let mut initial_error_rate = 0.0;
        stats.export_bucket_metrics(|name, metrics| {
            if name == &bucket {
                initial_error_rate = metrics.error_rate;
            }
        });
        assert!(initial_error_rate > 0.0);

        for _ in 0..1_000 {
            stats.observe(bucket.clone(), Ok(Duration::from_millis(1)));
        }

        let mut error_rate_after_successes = 0.0;
        stats.export_bucket_metrics(|name, metrics| {
            if name == &bucket {
                error_rate_after_successes = metrics.error_rate;
            }
        });

        assert!(
            error_rate_after_successes < initial_error_rate * 0.05,
            "Expected success traffic to suppress error rate, got {initial_error_rate} -> {error_rate_after_successes}"
        );
    }

    #[tokio::test]
    async fn test_error_rate_is_capped_after_repeated_failures() {
        tokio::time::pause();

        let stats = make_test_stats();
        let bucket = BucketName::new("error-rate-cap").unwrap();

        for _ in 0..200 {
            stats.observe(bucket.clone(), Err(()));
        }

        // Let circuit breaker recovery window expire so scoring uses error_rate again.
        tokio::time::advance(RECOVERY_TIME + Duration::from_secs(1)).await;
        let score = stats.score(Instant::now(), &bucket, 0);

        assert!(
            score <= ERROR_RATE_SCORE_MULTIPLIER as u64,
            "Score exceeded max error-rate penalty: {score}"
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
        assert_eq!(stats.score(now, &bucket, 0), CIRCUIT_OPEN_SCORE_PENALTY);

        // Still open before recovery time
        tokio::time::advance(RECOVERY_TIME.checked_sub(Duration::from_secs(1)).unwrap()).await;
        let now = Instant::now();
        assert_eq!(stats.score(now, &bucket, 0), CIRCUIT_OPEN_SCORE_PENALTY);

        // After recovery time, should be closed (ready to try again)
        tokio::time::advance(Duration::from_secs(2)).await;
        let now = Instant::now();
        let score = stats.score(now, &bucket, 0);
        assert!(
            score < CIRCUIT_OPEN_SCORE_PENALTY,
            "Circuit should be closed after recovery time"
        );

        // If it fails again, back to open
        for _ in 0..5 {
            stats.observe(bucket.clone(), Err(()));
        }
        let now = Instant::now();
        assert_eq!(stats.score(now, &bucket, 0), CIRCUIT_OPEN_SCORE_PENALTY);
    }

    #[tokio::test]
    async fn test_circuit_breaker_single_failure_after_recovery_requires_full_threshold() {
        tokio::time::pause();
        let stats = make_test_stats();
        let bucket = BucketName::new("recovery-threshold-bucket").unwrap();

        for _ in 0..CONSECUTIVE_FAILURE_THRESHOLD {
            stats.observe(bucket.clone(), Err(()));
        }

        tokio::time::advance(RECOVERY_TIME + Duration::from_secs(1)).await;

        let recovered_score = stats.score(Instant::now(), &bucket, 0);
        assert!(
            recovered_score < CIRCUIT_OPEN_SCORE_PENALTY,
            "Circuit should close after recovery time"
        );

        let mut recovered_failures = None;
        stats.export_bucket_metrics(|name, metrics| {
            if name == &bucket {
                recovered_failures = Some(metrics.consecutive_failures);
            }
        });
        assert_eq!(
            recovered_failures,
            Some(0),
            "Recovery should clear stale consecutive failures"
        );

        stats.observe(bucket.clone(), Err(()));
        let one_failure_score = stats.score(Instant::now(), &bucket, 0);
        assert!(
            one_failure_score < CIRCUIT_OPEN_SCORE_PENALTY,
            "A single post-recovery failure should not reopen the circuit"
        );

        for _ in 1..CONSECUTIVE_FAILURE_THRESHOLD {
            stats.observe(bucket.clone(), Err(()));
        }

        let reopened_score = stats.score(Instant::now(), &bucket, 0);
        assert_eq!(reopened_score, CIRCUIT_OPEN_SCORE_PENALTY);
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
        // Error component dominates latency once failures are observed.
        assert!(
            (2900..=3200).contains(&score),
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

        // Reliability now dominates latency for unhealthy buckets.
        assert_eq!(ordered[0], bucket1);
        assert_eq!(ordered[1], bucket2);
        assert_eq!(ordered[2], bucket3);
    }

    #[test]
    fn test_tie_break_prefers_client_order() {
        let stats = make_test_stats();
        let first_bucket = BucketName::new("first-bucket").unwrap();
        let second_bucket = BucketName::new("second-bucket").unwrap();

        for _ in 0..10 {
            stats.observe(first_bucket.clone(), Ok(Duration::from_millis(250)));
            stats.observe(second_bucket.clone(), Ok(Duration::from_millis(50)));
        }

        // Tie by score:
        // - first: base(0) + lat(2500) = 2500
        // - second: base(2000) + lat(500) = 2500
        // Client order should win ties.
        let buckets = vec![first_bucket.clone(), second_bucket.clone()];
        let ordered = get_attempt_order(&stats, &buckets);
        assert_eq!(ordered[0], first_bucket);
        assert_eq!(ordered[1], second_bucket);
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

        // Secondary should win because it stayed healthy.
        assert_eq!(ordered[0], bucket2);
        // Primary remains ahead of tertiary because tertiary's circuit opens.
        assert_eq!(ordered[1], bucket1);
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

        // Remote bucket: base(POSITION_PENALTY) + default_penalty(UNKNOWN_BUCKET_PENALTY)
        let remote_score = stats.score(now, &remote_bucket, 1);
        assert_eq!(remote_score, UNKNOWN_BUCKET_PENALTY + POSITION_PENALTY);

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
        stats.observe(primary.clone(), Err(())); // One observed error adds a 1.5% penalty weight

        let buckets = vec![primary.clone(), secondary, tertiary];
        let ordered = get_attempt_order(&stats, &buckets);

        // Primary: base(0) + err(1500) + lat(2000) = 3500
        // Secondary: base(2000) + err(0) + lat(3950) = 5950
        // Tertiary: base(4000) + unknown(5000) = 9000
        assert_eq!(
            ordered[0], primary,
            "Primary with a single error should still beat the much slower secondary"
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
        // - secondary: base(2000) + err(1_000_000) + lat(4000) = 1_006_000
        // - tertiary: base(4000) + unknown(5000) = 9000
        assert_eq!(ordered[0], primary, "Healthy primary should be first");
        assert_eq!(ordered[1], tertiary, "Unknown tertiary should be second");
        assert_eq!(ordered[2], secondary, "Failed secondary should be last");
    }

    #[test]
    fn test_failing_low_latency_bucket_is_deprioritized() {
        let stats = make_test_stats();
        let low_latency_failing = BucketName::new("low-latency-failing").unwrap();
        let high_latency_healthy = BucketName::new("high-latency-healthy").unwrap();

        stats.observe(low_latency_failing.clone(), Ok(Duration::from_millis(5)));
        for _ in 0..5 {
            stats.observe(low_latency_failing.clone(), Err(()));
        }

        for _ in 0..5 {
            stats.observe(high_latency_healthy.clone(), Ok(Duration::from_millis(50)));
        }

        let buckets = vec![low_latency_failing.clone(), high_latency_healthy.clone()];
        let ordered = get_attempt_order(&stats, &buckets);

        assert_eq!(ordered[0], high_latency_healthy);
        assert_eq!(ordered[1], low_latency_failing);
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
        // - far_az: base(2000) + lat(120) = 2120
        // - same_az: base(4000) + lat(40) = 4040
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
        // - same_az: base(2000) + lat(40) = 2040
        // - nearby_az: base(4000) + lat(70) = 4070
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

        // Test the threshold where latency difference overcomes position penalty.
        // With 0.1ms per score point, POSITION_PENALTY=2000 requires >200ms to override.

        // same_az: consistent 4ms
        for _ in 0..10 {
            stats.observe(same_az.clone(), Ok(Duration::from_millis(4)));
        }

        // cross_az: 250ms (246ms higher - enough to override position penalty)
        for _ in 0..10 {
            stats.observe(cross_az.clone(), Ok(Duration::from_millis(250)));
        }

        // Test with cross_az in position 0
        let buckets = vec![cross_az.clone(), same_az.clone()];
        let ordered = get_attempt_order(&stats, &buckets);

        // With 246ms difference (2460 points), same_az at position 1 should win.
        // Scores:
        // - cross_az: base(0) + lat(2500) = 2500
        // - same_az: base(2000) + lat(40) = 2040
        assert_eq!(
            ordered[0], same_az,
            "Large latency difference should override position preference"
        );
        assert_eq!(ordered[1], cross_az);

        // Now test with only 186ms difference (not enough to override)
        let nearby_az = BucketName::new("s3express-use1-az3-nearby").unwrap();
        for _ in 0..10 {
            stats.observe(nearby_az.clone(), Ok(Duration::from_millis(190)));
        }

        let buckets = vec![nearby_az.clone(), same_az.clone()];
        let ordered = get_attempt_order(&stats, &buckets);

        // With 186ms difference (1860 points), position penalty wins.
        // Scores:
        // - nearby_az: base(0) + lat(1900) = 1900
        // - same_az: base(2000) + lat(40) = 2040
        assert_eq!(
            ordered[0], nearby_az,
            "Sub-threshold latency difference should not override position"
        );
        assert_eq!(ordered[1], same_az);

        // Show the exact threshold
        let now = Instant::now();
        println!(
            "Reordering threshold: >200ms latency difference needed to override position penalty of 2000 points"
        );
        println!(
            "250ms bucket at pos 0: {} points, 4ms bucket at pos 1: {} points (reorders)",
            stats.score(now, &cross_az, 0),
            stats.score(now, &same_az, 1)
        );
        println!(
            "190ms bucket at pos 0: {} points, 4ms bucket at pos 1: {} points (no reorder)",
            stats.score(now, &nearby_az, 0),
            stats.score(now, &same_az, 1)
        );
    }
}
