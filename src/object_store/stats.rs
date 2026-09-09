use std::{collections::BTreeMap, sync::Arc, time::Duration};

use dashmap::DashMap;
use exponential_decay_histogram::ExponentialDecayHistogram;
use parking_lot::Mutex;
use tokio::time::Instant;

use crate::types::{BucketName, BucketNameSet};

const ERROR_ALPHA: f64 = 0.015;
const LATENCY_ALPHA: f64 = 0.1;
const LATENCY_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(1);
const RECOVERY_INTERVAL: Duration = Duration::from_secs(30);
const RECOVERY_SUCCESSES: u32 = 20;
const OVERLOAD_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct BucketMetrics {
    pub error_rate: f64,
    pub deprioritized: bool,
    pub consecutive_failures: u32,
    pub recovery_successes: u32,
    /// Successful complete bucket-operation latency, including retries and body validation.
    pub latency_mean: Duration,
    pub latency_hedge: Duration,
}

#[derive(Debug, Clone, Copy, Default)]
struct LatencySnapshot {
    mean: Duration,
    tail: Duration,
    hedge: Duration,
}

#[derive(Debug)]
struct BucketStats {
    last_outcome: Instant,
    error_rate: f64,
    deprioritized: bool,
    consecutive_failures: u32,
    recovery_successes: u32,
    generation: u64,
    probe_after: Instant,
    probing: bool,
    overload_until: Instant,
    active: BTreeMap<Instant, usize>,
    routing_latency: Duration,
    best_latency: Option<Duration>,
    histogram: ExponentialDecayHistogram,
    snapshot: LatencySnapshot,
    snapshot_at: Instant,
}

fn histogram(now: Instant) -> ExponentialDecayHistogram {
    let mut builder = ExponentialDecayHistogram::builder();
    builder
        .at(now.into_std())
        .alpha(std::f64::consts::LN_2 / 5.0);
    #[cfg(test)]
    if let Some(capacity) = super::simulation::histogram_capacity() {
        builder.size(capacity);
    }
    builder.build()
}

impl Default for BucketStats {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            last_outcome: now,
            error_rate: 0.0,
            deprioritized: false,
            consecutive_failures: 0,
            recovery_successes: 0,
            generation: 0,
            probe_after: now,
            probing: false,
            overload_until: now,
            active: BTreeMap::new(),
            routing_latency: Duration::ZERO,
            best_latency: None,
            histogram: histogram(now),
            snapshot: LatencySnapshot::default(),
            snapshot_at: now - LATENCY_SNAPSHOT_INTERVAL,
        }
    }
}

impl BucketStats {
    fn error_rate(&self, now: Instant) -> f64 {
        self.error_rate
            * (-ERROR_ALPHA
                * now
                    .saturating_duration_since(self.last_outcome)
                    .as_secs_f64())
            .exp()
    }

    fn snapshot(&mut self, now: Instant, hedge_quantile: f64) -> LatencySnapshot {
        if now.duration_since(self.snapshot_at) >= LATENCY_SNAPSHOT_INTERVAL {
            let snapshot = self.histogram.snapshot();
            self.snapshot = LatencySnapshot {
                mean: Duration::from_micros(snapshot.mean() as u64),
                tail: Duration::from_micros(snapshot.value(0.99) as u64),
                hedge: if hedge_quantile == 0.0 {
                    Duration::ZERO
                } else {
                    Duration::from_micros(snapshot.value(hedge_quantile) as u64)
                },
            };
            self.snapshot_at = now;
        }
        self.snapshot
    }

    fn routing_latency(&self, now: Instant, tail: Duration) -> Duration {
        let overdue = tail.saturating_mul(4).max(Duration::from_millis(1));
        let mut count = 0;
        for (started, active) in &self.active {
            let elapsed = now.duration_since(*started);
            if elapsed < overdue {
                break;
            }
            count += active;
            if count >= 3 {
                return self.routing_latency.max(elapsed);
            }
        }
        self.routing_latency
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum Outcome {
    Success,
    Failure,
    Overload,
    Neutral,
}

pub(super) struct ProbePermit {
    stats: Arc<Mutex<BucketStats>>,
}

impl Drop for ProbePermit {
    fn drop(&mut self) {
        self.stats.lock().probing = false;
    }
}

pub(super) struct BucketObservation {
    stats: Arc<Mutex<BucketStats>>,
    started: Instant,
    generation: u64,
    probe: Option<ProbePermit>,
    completed: bool,
}

impl BucketObservation {
    pub fn complete(mut self, outcome: Outcome) {
        let mut stats = self.stats.lock();
        let now = Instant::now();
        let latency = now.duration_since(self.started);
        let error_rate = stats.error_rate(now);
        match outcome {
            Outcome::Success => {
                if self.probe.is_some() {
                    stats.histogram = histogram(now);
                    stats.routing_latency = latency;
                } else if stats.best_latency.is_none() {
                    stats.routing_latency = latency;
                } else {
                    stats.routing_latency = stats.routing_latency.mul_f64(1.0 - LATENCY_ALPHA)
                        + latency.mul_f64(LATENCY_ALPHA);
                }
                stats.best_latency =
                    Some(stats.best_latency.map_or(latency, |best| best.min(latency)));
                stats.histogram.update_at(
                    now.into_std(),
                    latency.as_micros().min(i64::MAX as u128) as i64,
                );
                if self.generation == stats.generation {
                    stats.error_rate = error_rate * (1.0 - ERROR_ALPHA);
                    stats.consecutive_failures = 0;
                    if stats.deprioritized {
                        stats.recovery_successes += 1;
                        if stats.recovery_successes >= RECOVERY_SUCCESSES {
                            stats.deprioritized = false;
                        }
                    }
                    if self.probe.is_some() {
                        stats.probe_after = now;
                    }
                } else {
                    stats.error_rate = error_rate;
                }
                stats.last_outcome = now;
                if stats.snapshot.tail.is_zero() || self.probe.is_some() {
                    stats.snapshot_at = now - LATENCY_SNAPSHOT_INTERVAL;
                }
            }
            Outcome::Failure | Outcome::Overload => {
                stats.error_rate = error_rate * (1.0 - ERROR_ALPHA) + ERROR_ALPHA;
                stats.deprioritized = true;
                stats.consecutive_failures = stats.consecutive_failures.saturating_add(1);
                stats.recovery_successes = 0;
                stats.generation = stats.generation.wrapping_add(1);
                stats.probe_after = now + recovery_delay();
                stats.last_outcome = now;
                if matches!(outcome, Outcome::Overload) {
                    stats.overload_until = now + OVERLOAD_INTERVAL;
                }
            }
            Outcome::Neutral => {}
        }
        self.completed = true;
    }
}

impl Drop for BucketObservation {
    fn drop(&mut self) {
        let mut stats = self.stats.lock();
        if let Some(count) = stats.active.get_mut(&self.started) {
            *count -= 1;
            if *count == 0 {
                stats.active.remove(&self.started);
            }
        }
        if !self.completed {
            let elapsed = self.started.elapsed();
            if elapsed > stats.routing_latency {
                let increase = elapsed
                    .saturating_sub(stats.routing_latency)
                    .mul_f64(LATENCY_ALPHA);
                stats.routing_latency += increase;
            }
        }
    }
}

fn recovery_delay() -> Duration {
    use std::hash::{BuildHasher, RandomState};

    #[cfg(test)]
    if let Some(delay) = super::simulation::recovery_delay() {
        return delay;
    }
    // Each new hasher is randomly seeded, independently across processes.
    Duration::from_millis(24_000 + RandomState::new().hash_one(()) % 12_001)
}

#[derive(Debug, Clone)]
pub struct BucketedStats {
    by_bucket: Arc<DashMap<BucketName, Arc<Mutex<BucketStats>>>>,
    hedge_latency_quantile: f64,
}

impl BucketedStats {
    pub fn new(hedge_latency_quantile: f64) -> Self {
        Self {
            by_bucket: Arc::default(),
            hedge_latency_quantile,
        }
    }

    fn entry(&self, bucket: &BucketName) -> Arc<Mutex<BucketStats>> {
        self.by_bucket.entry(bucket.clone()).or_default().clone()
    }

    pub(super) fn begin(
        &self,
        bucket: &BucketName,
        probe: Option<ProbePermit>,
    ) -> BucketObservation {
        let stats = self.entry(bucket);
        let (started, generation) = {
            let mut stats = stats.lock();
            let started = Instant::now();
            *stats.active.entry(started).or_default() += 1;
            (started, stats.generation)
        };
        BucketObservation {
            stats,
            started,
            generation,
            probe,
            completed: false,
        }
    }

    pub(super) fn attempt_order(
        &self,
        buckets: &BucketNameSet,
        tried: &[bool],
        remaining: Duration,
    ) -> Vec<usize> {
        let now = Instant::now();
        let preferred_latency = self.entry(&buckets[0]).lock().routing_latency;
        let mut choices: Vec<_> = buckets
            .iter()
            .enumerate()
            .filter(|(index, _)| !tried[*index])
            .map(|(index, bucket)| {
                let entry = self.entry(bucket);
                let mut stats = entry.lock();
                let tail = stats.snapshot(now, self.hedge_latency_quantile).tail;
                let mut latency = stats.routing_latency(now, tail);
                if index > 0 && stats.best_latency.is_none() {
                    latency = latency.max(preferred_latency);
                }
                let latency = locality_adjusted(latency, index);
                (
                    index,
                    tail >= remaining,
                    stats.deprioritized,
                    stats.error_rate(now),
                    latency,
                )
            })
            .collect();
        choices.sort_by(|left, right| {
            (left.1, left.2)
                .cmp(&(right.1, right.2))
                .then_with(|| {
                    if left.2 {
                        left.3.total_cmp(&right.3)
                    } else {
                        std::cmp::Ordering::Equal
                    }
                })
                .then_with(|| (left.4, left.0).cmp(&(right.4, right.0)))
        });
        choices.into_iter().map(|choice| choice.0).collect()
    }

    pub(super) fn primary(
        &self,
        buckets: &BucketNameSet,
        remaining: Duration,
    ) -> (usize, Option<ProbePermit>) {
        let order = self.attempt_order(buckets, &vec![false; buckets.len()], remaining);
        let winner = order[0];
        let now = Instant::now();
        let winner_entry = self.entry(&buckets[winner]);
        let best = {
            let mut stats = winner_entry.lock();
            let tail = stats.snapshot(now, self.hedge_latency_quantile).tail;
            if stats.deprioritized || tail >= remaining {
                return (winner, None);
            }
            locality_adjusted(stats.routing_latency(now, tail), winner)
        };
        for (index, bucket) in buckets.iter().enumerate() {
            if index == winner {
                continue;
            }
            let entry = self.entry(bucket);
            let mut stats = entry.lock();
            let stale = now.duration_since(stats.last_outcome) >= RECOVERY_INTERVAL;
            if (stats.deprioritized || stale)
                && !stats.probing
                && stats.active.is_empty()
                && now >= stats.probe_after
                && locality_adjusted(stats.best_latency.unwrap_or_default(), index) < best
            {
                stats.probing = true;
                stats.probe_after = now + recovery_delay();
                drop(stats);
                return (index, Some(ProbePermit { stats: entry }));
            }
        }
        (winner, None)
    }

    pub(super) fn tail_latency(&self, bucket: &BucketName) -> Duration {
        self.entry(bucket)
            .lock()
            .snapshot(Instant::now(), self.hedge_latency_quantile)
            .tail
    }

    pub fn hedging_threshold(&self, bucket: &BucketName, now: Instant) -> Duration {
        self.entry(bucket)
            .lock()
            .snapshot(now, self.hedge_latency_quantile)
            .hedge
    }

    pub(super) fn all_overloaded(&self, buckets: &BucketNameSet) -> bool {
        buckets.iter().all(|bucket| self.overloaded(bucket))
    }

    pub(super) fn overloaded(&self, bucket: &BucketName) -> bool {
        Instant::now() < self.entry(bucket).lock().overload_until
    }

    #[cfg(test)]
    pub(super) fn simulation_histograms(&self) -> Vec<(u64, usize)> {
        self.by_bucket
            .iter()
            .map(|entry| {
                let snapshot = entry.value().lock().histogram.snapshot();
                (snapshot.count(), snapshot.exemplars().count())
            })
            .collect()
    }

    pub fn export_bucket_metrics(&self, mut f: impl FnMut(&BucketName, &BucketMetrics)) {
        let now = Instant::now();
        for entry in self.by_bucket.iter() {
            let mut stats = entry.value().lock();
            let snapshot = stats.snapshot(now, self.hedge_latency_quantile);
            f(
                entry.key(),
                &BucketMetrics {
                    error_rate: stats.error_rate(now),
                    deprioritized: stats.deprioritized,
                    consecutive_failures: stats.consecutive_failures,
                    recovery_successes: stats.recovery_successes,
                    latency_mean: snapshot.mean,
                    latency_hedge: snapshot.hedge,
                },
            );
        }
    }
}

fn locality_adjusted(latency: Duration, index: usize) -> Duration {
    if index == 0 {
        latency
    } else {
        latency.saturating_add(latency / 2)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::advance;

    use super::{BucketedStats, Outcome, RECOVERY_SUCCESSES};
    use crate::types::{BucketName, BucketNameSet};

    fn buckets() -> BucketNameSet {
        BucketNameSet::new(
            ["local", "peer", "third"]
                .into_iter()
                .map(|name| BucketName::new(name).unwrap()),
        )
        .unwrap()
    }

    async fn success(stats: &BucketedStats, bucket: &BucketName, millis: u64) {
        let observation = stats.begin(bucket, None);
        advance(Duration::from_millis(millis)).await;
        observation.complete(Outcome::Success);
    }

    #[tokio::test(start_paused = true)]
    async fn latency_and_relative_locality_choose_between_healthy_copies() {
        for (latencies, expected) in [
            ([3, 5, 6], 0),
            ([20, 5, 6], 1),
            ([20, 10, 4], 2),
            ([3, 2, 6], 0),
        ] {
            let stats = BucketedStats::new(0.99);
            let buckets = buckets();
            for (bucket, millis) in buckets.iter().zip(latencies) {
                success(&stats, bucket, millis).await;
            }
            assert_eq!(
                stats.attempt_order(&buckets, &[false; 3], Duration::from_secs(1))[0],
                expected
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn health_is_soft_and_deadline_feasibility_comes_first() {
        let stats = BucketedStats::new(0.99);
        let buckets = buckets();
        for (bucket, millis) in buckets.iter().zip([10, 60, 150]) {
            success(&stats, bucket, millis).await;
        }
        for bucket in buckets.iter().take(2) {
            stats.begin(bucket, None).complete(Outcome::Failure);
        }
        assert_eq!(
            stats.attempt_order(&buckets, &[false; 3], Duration::from_secs(1))[0],
            2
        );
        let order = stats.attempt_order(&buckets, &[false; 3], Duration::from_millis(100));
        assert_eq!(order[0], 0);
        assert_eq!(order.len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn elapsed_time_and_pre_failure_completions_do_not_restore_health() {
        let stats = BucketedStats::new(0.99);
        let bucket = &buckets()[0];
        let old = (0..RECOVERY_SUCCESSES)
            .map(|_| stats.begin(bucket, None))
            .collect::<Vec<_>>();
        stats.begin(bucket, None).complete(Outcome::Failure);
        for observation in old {
            observation.complete(Outcome::Success);
        }
        advance(Duration::from_secs(300)).await;
        assert!(stats.entry(bucket).lock().deprioritized);
        assert_eq!(stats.entry(bucket).lock().recovery_successes, 0);
        for _ in 0..RECOVERY_SUCCESSES - 1 {
            success(&stats, bucket, 3).await;
        }
        assert!(stats.entry(bucket).lock().deprioritized);
        success(&stats, bucket, 3).await;
        assert!(!stats.entry(bucket).lock().deprioritized);
    }

    #[tokio::test(start_paused = true)]
    async fn probing_is_exclusive_and_a_success_preserves_failure_history() {
        let stats = BucketedStats::new(0.99);
        let buckets = buckets();
        for (bucket, millis) in buckets.iter().zip([3, 5, 6]) {
            success(&stats, bucket, millis).await;
        }
        stats.begin(&buckets[0], None).complete(Outcome::Failure);
        advance(Duration::from_secs(37)).await;
        let (index, probe) = stats.primary(&buckets, Duration::from_secs(1));
        assert_eq!(index, 0);
        assert!(probe.is_some());
        assert_eq!(stats.primary(&buckets, Duration::from_secs(1)).0, 1);
        let observation = stats.begin(&buckets[0], probe);
        advance(Duration::from_millis(3)).await;
        observation.complete(Outcome::Success);
        assert!(stats.entry(&buckets[0]).lock().deprioritized);
        assert!(!stats.entry(&buckets[0]).lock().probing);
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_is_a_latency_lower_bound_not_a_health_failure() {
        let stats = BucketedStats::new(0.99);
        let bucket = &buckets()[0];
        success(&stats, bucket, 100).await;
        let observation = stats.begin(bucket, None);
        advance(Duration::from_millis(10)).await;
        drop(observation);
        assert_eq!(
            stats.entry(bucket).lock().routing_latency,
            Duration::from_millis(100)
        );
        let observation = stats.begin(bucket, None);
        advance(Duration::from_millis(200)).await;
        drop(observation);
        let entry = stats.entry(bucket);
        let state = entry.lock();
        assert!(state.routing_latency > Duration::from_millis(100));
        assert!(!state.deprioritized);
        assert!(state.active.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_stalls_affect_routing_before_timeouts() {
        let stats = BucketedStats::new(0.99);
        let buckets = buckets();
        for (bucket, millis) in buckets.iter().zip([3, 5, 6]) {
            success(&stats, bucket, millis).await;
        }
        let active = (0..3)
            .map(|_| stats.begin(&buckets[0], None))
            .collect::<Vec<_>>();
        advance(Duration::from_millis(20)).await;
        assert_eq!(
            stats.attempt_order(&buckets, &[false; 3], Duration::from_secs(1))[0],
            1
        );
        drop(active);
    }

    #[tokio::test(start_paused = true)]
    async fn neutral_outcomes_do_not_create_failures_and_idle_remote_regions_are_not_probed() {
        let stats = BucketedStats::new(0.99);
        let buckets = buckets();
        for (bucket, millis) in buckets.iter().zip([10, 60, 150]) {
            success(&stats, bucket, millis).await;
        }
        stats.begin(&buckets[0], None).complete(Outcome::Neutral);
        advance(Duration::from_secs(60)).await;
        let (index, probe) = stats.primary(&buckets, Duration::from_secs(1));
        assert_eq!(index, 0);
        assert!(probe.is_none());
        assert!(!stats.entry(&buckets[0]).lock().deprioritized);
    }
}
