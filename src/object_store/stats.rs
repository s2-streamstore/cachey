use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use dashmap::DashMap;
use exponential_decay_histogram::ExponentialDecayHistogram;
use parking_lot::Mutex;
use tokio::time::Instant;

use crate::{
    object_store::DownloadLimits,
    types::{BucketName, BucketNameSet},
};

const ERROR_ALPHA: f64 = 0.015;
const LATENCY_ALPHA: f64 = 0.1;
const LATENCY_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(1);
const RECOVERY_INTERVAL: Duration = Duration::from_secs(30);
const RECOVERY_SUCCESSES: u32 = 20;
const OVERLOAD_INTERVAL: Duration = Duration::from_secs(1);
/// Smaller bodies take a few round trips regardless of size, so their duration is tracked directly
/// rather than as throughput.
const MIN_THROUGHPUT_SAMPLE_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Clone)]
pub struct BucketMetrics {
    pub error_rate: f64,
    pub deprioritized: bool,
    pub consecutive_failures: u32,
    pub recovery_successes: u32,
    /// Time from starting a request to receiving successful response headers, including SDK
    /// retries.
    pub first_byte_mean: Duration,
    pub first_byte_p99: Duration,
    /// Time to read a body smaller than `MIN_THROUGHPUT_SAMPLE_BYTES` after its headers.
    pub small_body_mean: Duration,
    pub small_body_p99: Duration,
    /// Body bytes per second after headers, for larger bodies.
    pub throughput_mean: f64,
    pub throughput_p1: f64,
}

#[derive(Debug, Clone, Copy)]
struct Summary<T> {
    mean: T,
    slow: T,
}

/// Without a large-body measurement, a bucket is assumed to serve small bodies regardless of the
/// requested size.
fn body_time(bytes: u64, small_body: Option<Duration>, throughput: Option<f64>) -> Duration {
    let transfer = throughput.map(|throughput| {
        Duration::try_from_secs_f64(bytes as f64 / throughput).unwrap_or(Duration::MAX)
    });
    match transfer {
        Some(transfer) if bytes >= MIN_THROUGHPUT_SAMPLE_BYTES => transfer,
        _ => small_body.or(transfer).unwrap_or_default(),
    }
}

/// Request latency as time to first byte plus body time, so one set of statistics serves any
/// request size.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct LatencyModel {
    first_byte: Option<Summary<Duration>>,
    small_body: Option<Summary<Duration>>,
    throughput: Option<Summary<f64>>,
}

impl LatencyModel {
    fn summarize(
        histogram: &ExponentialDecayHistogram,
        slow_quantile: f64,
    ) -> Option<Summary<f64>> {
        let snapshot = histogram.snapshot();
        (snapshot.count() > 0).then(|| Summary {
            mean: snapshot.mean(),
            slow: snapshot.value(slow_quantile) as f64,
        })
    }

    fn from_histograms(
        first_byte: &ExponentialDecayHistogram,
        small_body: &ExponentialDecayHistogram,
        throughput: &ExponentialDecayHistogram,
    ) -> Self {
        let micros = |summary: Summary<f64>| Summary {
            mean: Duration::from_micros(summary.mean.round() as u64),
            slow: Duration::from_micros(summary.slow as u64),
        };
        Self {
            first_byte: Self::summarize(first_byte, 0.99).map(micros),
            small_body: Self::summarize(small_body, 0.99).map(micros),
            throughput: Self::summarize(throughput, 0.01),
        }
    }

    pub fn first_byte_tail(&self) -> Option<Duration> {
        self.first_byte.map(|first_byte| first_byte.slow)
    }

    /// Slow body time measured from response headers.
    pub fn body_tail(&self, bytes: u64) -> Duration {
        body_time(
            bytes,
            self.small_body.map(|small_body| small_body.slow),
            self.throughput.map(|throughput| throughput.slow),
        )
    }

    /// Slow request time, or `None` before any successful response headers.
    pub fn tail(&self, bytes: u64) -> Option<Duration> {
        Some(
            self.first_byte_tail()?
                .saturating_add(self.body_tail(bytes)),
        )
    }
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
    /// In-flight operations keyed by start time plus expected body time, so their age estimates
    /// time to first byte.
    active: BTreeMap<Instant, usize>,
    routing_first_byte: Duration,
    routing_small_body: Option<Duration>,
    routing_throughput: Option<f64>,
    best_first_byte: Option<Duration>,
    first_byte: ExponentialDecayHistogram,
    small_body: ExponentialDecayHistogram,
    throughput: ExponentialDecayHistogram,
    model: LatencyModel,
    model_at: Instant,
}

fn histogram_micros(duration: Duration) -> i64 {
    duration.as_micros().min(i64::MAX as u128) as i64
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
            routing_first_byte: Duration::ZERO,
            routing_small_body: None,
            routing_throughput: None,
            best_first_byte: None,
            first_byte: histogram(now),
            small_body: histogram(now),
            throughput: histogram(now),
            model: LatencyModel::default(),
            model_at: now - LATENCY_SNAPSHOT_INTERVAL,
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

    fn model(&mut self, now: Instant) -> LatencyModel {
        if now.duration_since(self.model_at) >= LATENCY_SNAPSHOT_INTERVAL {
            self.model =
                LatencyModel::from_histograms(&self.first_byte, &self.small_body, &self.throughput);
            self.model_at = now;
        }
        self.model
    }

    fn refresh_model(&mut self, now: Instant) {
        self.model_at = now - LATENCY_SNAPSHOT_INTERVAL;
    }

    fn reset_latency(&mut self, now: Instant) {
        self.first_byte = histogram(now);
        self.small_body = histogram(now);
        self.throughput = histogram(now);
        self.routing_small_body = None;
        self.routing_throughput = None;
        self.best_first_byte = None;
        self.refresh_model(now);
    }

    fn routing_body(&self, bytes: u64) -> Duration {
        body_time(bytes, self.routing_small_body, self.routing_throughput)
    }

    fn routing_first_byte(&self, now: Instant, tail: Duration) -> Duration {
        let overdue = tail.saturating_mul(4).max(Duration::from_millis(1));
        let mut count = 0;
        for (started, active) in &self.active {
            let elapsed = now.duration_since(*started);
            if elapsed < overdue {
                break;
            }
            count += active;
            if count >= 3 {
                return self.routing_first_byte.max(elapsed);
            }
        }
        self.routing_first_byte
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
    /// Start time plus expected body time.
    started: Instant,
    generation: u64,
    probe: Option<ProbePermit>,
    outcome: Option<Outcome>,
}

impl BucketObservation {
    pub fn complete(mut self, outcome: Outcome) {
        self.outcome = Some(outcome);
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
        let now = Instant::now();
        let Some(outcome) = self.outcome else {
            let increase = now
                .duration_since(self.started)
                .saturating_sub(stats.routing_first_byte)
                .mul_f64(LATENCY_ALPHA);
            stats.routing_first_byte += increase;
            return;
        };
        let error_rate = stats.error_rate(now);
        match outcome {
            Outcome::Success => {
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

#[derive(Debug, Clone, Default)]
pub struct BucketedStats {
    by_bucket: Arc<DashMap<BucketName, Arc<Mutex<BucketStats>>>>,
}

struct BucketSnapshot {
    stats: Arc<Mutex<BucketStats>>,
    model: LatencyModel,
    body: Duration,
    latency: Duration,
    tail: Option<Duration>,
    deprioritized: bool,
    error_rate: f64,
    overloaded: bool,
}

pub(super) struct RoutingSnapshot(Vec<BucketSnapshot>);

impl RoutingSnapshot {
    fn compare(&self, left: usize, right: usize, remaining: Duration) -> std::cmp::Ordering {
        let left_stats = &self.0[left];
        let right_stats = &self.0[right];
        (
            left_stats.tail.is_some_and(|tail| tail >= remaining),
            left_stats.deprioritized,
        )
            .cmp(&(
                right_stats.tail.is_some_and(|tail| tail >= remaining),
                right_stats.deprioritized,
            ))
            .then_with(|| {
                if left_stats.deprioritized {
                    left_stats.error_rate.total_cmp(&right_stats.error_rate)
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .then_with(|| (left_stats.latency, left).cmp(&(right_stats.latency, right)))
    }

    pub fn best(&self, tried: &[bool], remaining: Duration) -> Option<usize> {
        (0..self.0.len())
            .filter(|index| tried.get(*index) != Some(&true))
            .min_by(|left, right| self.compare(*left, *right, remaining))
    }

    pub fn rescue_schedule(
        &self,
        primary: usize,
        probing: bool,
        limits: DownloadLimits,
    ) -> VecDeque<(Duration, usize)> {
        let page_budget = limits.page_timeout;
        let unknown_latency = page_budget / u32::try_from(self.0.len()).unwrap_or(u32::MAX);
        let reserve_budget = page_budget / 3 * 2;
        let mut schedule = Vec::with_capacity(self.0.len());
        schedule.extend((0..self.0.len()).filter_map(|index| {
            let tail = self.tail(index);
            if index == primary || tail.is_some_and(|tail| tail >= page_budget) {
                return None;
            }
            let reserve = tail
                .map_or(unknown_latency, |tail| {
                    tail.saturating_mul(2)
                        .min(reserve_budget)
                        .max(tail.saturating_add(Duration::from_millis(1)))
                })
                .max(Duration::from_millis(20))
                .min(limits.bucket_timeout)
                .min(page_budget);
            Some((reserve, index))
        }));
        schedule.sort_unstable_by(|(_, left), (_, right)| self.compare(*left, *right, page_budget));
        let backup = schedule.first().filter(|_| probing).map(|(_, index)| {
            let grace = self
                .tail(*index)
                .unwrap_or(unknown_latency)
                .saturating_mul(2)
                .max(Duration::from_millis(1));
            (grace.min(page_budget), *index)
        });
        let total = schedule
            .iter()
            .map(|(reserve, _)| *reserve)
            .fold(Duration::ZERO, Duration::saturating_add);
        let scale = if total > reserve_budget && !total.is_zero() {
            reserve_budget.as_secs_f64() / total.as_secs_f64()
        } else {
            1.0
        };
        let primary_tail = self
            .tail(primary)
            .unwrap_or_default()
            .min(limits.bucket_timeout)
            .min(page_budget);
        let overloaded = self.0.iter().any(|bucket| bucket.overloaded);
        let mut remaining = total.mul_f64(scale);
        // Convert reservations to launch delays in place, retaining each destination.
        for (after, index) in &mut schedule {
            let reserve = *after;
            let latest_start = page_budget.saturating_sub(reserve);
            let planned = page_budget.saturating_sub(remaining).max(primary_tail);
            let expected = self.tail(*index).unwrap_or(reserve);
            *after = if !overloaded && planned.saturating_add(expected) >= page_budget {
                planned.min(latest_start)
            } else {
                planned
            };
            remaining = remaining.saturating_sub(reserve.mul_f64(scale));
        }
        schedule.extend(backup);
        schedule.sort_unstable();
        schedule.into()
    }

    pub fn primary(&self, remaining: Duration) -> (usize, Option<ProbePermit>) {
        let winner = self.best(&[], remaining).expect("nonempty bucket set");
        let best = &self.0[winner];
        if best.deprioritized || best.tail.is_some_and(|tail| tail >= remaining) {
            return (winner, None);
        }
        let now = Instant::now();
        for (index, bucket) in self.0.iter().enumerate() {
            if index == winner {
                continue;
            }
            let mut stats = bucket.stats.lock();
            let stale = stats.best_first_byte.is_some()
                && now.duration_since(stats.last_outcome) >= RECOVERY_INTERVAL;
            let best_latency = stats
                .best_first_byte
                .unwrap_or_default()
                .saturating_add(bucket.body);
            if (stats.deprioritized || stale)
                && !stats.probing
                && stats.active.is_empty()
                && now >= stats.probe_after
                && locality_adjusted(best_latency, index) < best.latency
            {
                stats.probing = true;
                return (
                    index,
                    Some(ProbePermit {
                        stats: bucket.stats.clone(),
                    }),
                );
            }
        }
        (winner, None)
    }

    pub fn tail(&self, index: usize) -> Option<Duration> {
        self.0[index].tail
    }

    pub fn model(&self, index: usize) -> LatencyModel {
        self.0[index].model
    }

    pub fn all_overloaded(&self) -> bool {
        self.0.iter().all(|bucket| bucket.overloaded)
    }
}

impl BucketedStats {
    fn entry(&self, bucket: &BucketName) -> Arc<Mutex<BucketStats>> {
        if let Some(stats) = self.by_bucket.get(bucket) {
            return stats.clone();
        }
        self.by_bucket.entry(bucket.clone()).or_default().clone()
    }

    pub(super) fn observe_first_byte(&self, bucket: &BucketName, latency: Duration) {
        let entry = self.entry(bucket);
        let mut stats = entry.lock();
        let now = Instant::now();
        stats
            .first_byte
            .update_at(now.into_std(), histogram_micros(latency));
        stats.routing_first_byte = if stats.best_first_byte.is_none() {
            latency
        } else {
            stats.routing_first_byte.mul_f64(1.0 - LATENCY_ALPHA) + latency.mul_f64(LATENCY_ALPHA)
        };
        stats.best_first_byte = Some(
            stats
                .best_first_byte
                .map_or(latency, |best| best.min(latency)),
        );
        if stats.model.first_byte.is_none() {
            stats.refresh_model(now);
        }
    }

    pub(super) fn observe_body(&self, bucket: &BucketName, bytes: u64, elapsed: Duration) {
        let entry = self.entry(bucket);
        let mut stats = entry.lock();
        let now = Instant::now();
        let first_sample = if bytes < MIN_THROUGHPUT_SAMPLE_BYTES {
            stats
                .small_body
                .update_at(now.into_std(), histogram_micros(elapsed));
            stats.routing_small_body = Some(stats.routing_small_body.map_or(elapsed, |routing| {
                routing.mul_f64(1.0 - LATENCY_ALPHA) + elapsed.mul_f64(LATENCY_ALPHA)
            }));
            stats.model.small_body.is_none()
        } else if !elapsed.is_zero() {
            let bytes_per_second = bytes as f64 / elapsed.as_secs_f64();
            stats
                .throughput
                .update_at(now.into_std(), bytes_per_second as i64);
            stats.routing_throughput = Some(
                stats
                    .routing_throughput
                    .map_or(bytes_per_second, |routing| {
                        routing * (1.0 - LATENCY_ALPHA) + bytes_per_second * LATENCY_ALPHA
                    }),
            );
            stats.model.throughput.is_none()
        } else {
            false
        };
        if first_sample {
            stats.refresh_model(now);
        }
    }

    pub(super) fn model(&self, bucket: &BucketName) -> LatencyModel {
        self.entry(bucket).lock().model(Instant::now())
    }

    /// A probe discards latency history, so the bucket is judged by its fresh measurements.
    pub(super) fn begin(
        &self,
        bucket: &BucketName,
        probe: Option<ProbePermit>,
        bytes: u64,
    ) -> BucketObservation {
        let stats = self.entry(bucket);
        let (started, generation) = {
            let mut stats = stats.lock();
            let now = Instant::now();
            if probe.is_some() {
                stats.probe_after = now + recovery_delay();
                stats.reset_latency(now);
            }
            let started = now.checked_add(stats.routing_body(bytes)).unwrap_or(now);
            *stats.active.entry(started).or_default() += 1;
            (started, stats.generation)
        };
        BucketObservation {
            stats,
            started,
            generation,
            probe,
            outcome: None,
        }
    }

    pub(super) fn snapshot(&self, buckets: &BucketNameSet, bytes: u64) -> RoutingSnapshot {
        let now = Instant::now();
        let mut preferred_latency = Duration::ZERO;
        RoutingSnapshot(
            buckets
                .iter()
                .enumerate()
                .map(|(index, bucket)| {
                    let entry = self.entry(bucket);
                    let mut stats = entry.lock();
                    let model = stats.model(now);
                    let body = stats.routing_body(bytes);
                    if index == 0 {
                        preferred_latency = stats.routing_first_byte.saturating_add(body);
                    }
                    let tail = model.tail(bytes);
                    let mut latency = stats
                        .routing_first_byte(now, model.first_byte_tail().unwrap_or_default())
                        .saturating_add(body);
                    if index > 0 && tail.is_none() {
                        latency = latency.max(preferred_latency);
                    }
                    BucketSnapshot {
                        stats: entry.clone(),
                        model,
                        body,
                        latency: locality_adjusted(latency, index),
                        tail,
                        deprioritized: stats.deprioritized,
                        error_rate: stats.error_rate(now),
                        overloaded: now < stats.overload_until,
                    }
                })
                .collect(),
        )
    }

    pub(super) fn overloaded(&self, bucket: &BucketName) -> bool {
        Instant::now() < self.entry(bucket).lock().overload_until
    }

    #[cfg(test)]
    pub(super) fn simulation_histograms(&self) -> Vec<(u64, usize)> {
        self.by_bucket
            .iter()
            .map(|entry| {
                let snapshot = entry.value().lock().first_byte.snapshot();
                (snapshot.count(), snapshot.exemplars().count())
            })
            .collect()
    }

    pub fn export_bucket_metrics(&self, mut f: impl FnMut(&BucketName, &BucketMetrics)) {
        let now = Instant::now();
        let metrics: Vec<_> = self
            .by_bucket
            .iter()
            .map(|entry| {
                let mut stats = entry.value().lock();
                let model = stats.model(now);
                let first_byte = model.first_byte.unwrap_or(Summary {
                    mean: Duration::ZERO,
                    slow: Duration::ZERO,
                });
                let small_body = model.small_body.unwrap_or(Summary {
                    mean: Duration::ZERO,
                    slow: Duration::ZERO,
                });
                let throughput = model.throughput.unwrap_or(Summary {
                    mean: 0.0,
                    slow: 0.0,
                });
                let metrics = BucketMetrics {
                    error_rate: stats.error_rate(now),
                    deprioritized: stats.deprioritized,
                    consecutive_failures: stats.consecutive_failures,
                    recovery_successes: stats.recovery_successes,
                    first_byte_mean: first_byte.mean,
                    first_byte_p99: first_byte.slow,
                    small_body_mean: small_body.mean,
                    small_body_p99: small_body.slow,
                    throughput_mean: throughput.mean,
                    throughput_p1: throughput.slow,
                };
                (entry.key().clone(), metrics)
            })
            .collect();
        for (bucket, metrics) in metrics {
            f(&bucket, &metrics);
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

    use super::{BucketedStats, MIN_THROUGHPUT_SAMPLE_BYTES, Outcome, RECOVERY_SUCCESSES};
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
        let observation = stats.begin(bucket, None, 0);
        advance(Duration::from_millis(millis)).await;
        stats.observe_first_byte(bucket, Duration::from_millis(millis));
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
            let stats = BucketedStats::default();
            let buckets = buckets();
            for (bucket, millis) in buckets.iter().zip(latencies) {
                success(&stats, bucket, millis).await;
            }
            assert_eq!(
                stats
                    .snapshot(&buckets, 0)
                    .best(&[], Duration::from_secs(1)),
                Some(expected)
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn elapsed_time_and_pre_failure_completions_do_not_restore_health() {
        let stats = BucketedStats::default();
        let bucket = &buckets()[0];
        let old = (0..RECOVERY_SUCCESSES)
            .map(|_| stats.begin(bucket, None, 0))
            .collect::<Vec<_>>();
        stats.begin(bucket, None, 0).complete(Outcome::Failure);
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
        for local_latency in [None, Some(3)] {
            let stats = BucketedStats::default();
            let buckets = buckets();
            for (bucket, millis) in buckets.iter().zip([local_latency, Some(5), Some(6)]) {
                if let Some(millis) = millis {
                    success(&stats, bucket, millis).await;
                }
            }
            stats.begin(&buckets[0], None, 0).complete(Outcome::Failure);
            advance(Duration::from_secs(37)).await;
            let (index, probe) = stats.snapshot(&buckets, 0).primary(Duration::from_secs(1));
            assert_eq!(index, 0);
            assert!(probe.is_some());
            assert_eq!(
                stats
                    .snapshot(&buckets, 0)
                    .primary(Duration::from_secs(1))
                    .0,
                1
            );
            let observation = stats.begin(&buckets[0], probe, 0);
            advance(Duration::from_millis(3)).await;
            stats.observe_first_byte(&buckets[0], Duration::from_millis(3));
            observation.complete(Outcome::Success);
            assert_eq!(
                stats
                    .snapshot(&buckets, 0)
                    .best(&[], Duration::from_secs(1)),
                Some(1)
            );
            let (index, probe) = stats.snapshot(&buckets, 0).primary(Duration::from_secs(1));
            assert_eq!(index, 0);
            assert!(probe.is_some());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn metrics_callbacks_run_without_holding_statistics_locks() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for bucket in buckets.iter() {
            success(&stats, bucket, 3).await;
        }
        let mut exported = 0;
        stats.export_bucket_metrics(|bucket, metrics| {
            assert_eq!(metrics.first_byte_mean, Duration::from_millis(3));
            let entry = stats
                .by_bucket
                .try_get_mut(bucket)
                .try_unwrap()
                .expect("metrics callbacks must not hold a map lock");
            assert!(entry.value().try_lock().is_some());
            exported += 1;
        });
        assert_eq!(exported, buckets.len());
    }

    #[tokio::test(start_paused = true)]
    async fn exports_first_byte_small_body_and_throughput() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        for millis in [10, 30] {
            stats.observe_first_byte(&bucket, Duration::from_millis(millis));
        }
        stats.observe_body(
            &bucket,
            MIN_THROUGHPUT_SAMPLE_BYTES - 1,
            Duration::from_millis(1),
        );
        stats.observe_body(&bucket, MIN_THROUGHPUT_SAMPLE_BYTES, Duration::ZERO);
        stats.observe_body(
            &bucket,
            4 * MIN_THROUGHPUT_SAMPLE_BYTES,
            Duration::from_secs(2),
        );

        let mut exported = None;
        stats.export_bucket_metrics(|_, metrics| exported = Some(metrics.clone()));
        let metrics = exported.expect("bucket metrics");
        assert_eq!(metrics.first_byte_mean, Duration::from_millis(20));
        assert_eq!(metrics.first_byte_p99, Duration::from_millis(30));
        assert_eq!(metrics.small_body_mean, Duration::from_millis(1));
        assert_eq!(metrics.small_body_p99, Duration::from_millis(1));
        let bytes_per_second = (2 * MIN_THROUGHPUT_SAMPLE_BYTES) as f64;
        assert!((metrics.throughput_mean - bytes_per_second).abs() < f64::EPSILON);
        assert!((metrics.throughput_p1 - bytes_per_second).abs() < f64::EPSILON);
    }

    #[tokio::test(start_paused = true)]
    async fn request_size_weighs_first_byte_against_throughput() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        let page = 16 * MIN_THROUGHPUT_SAMPLE_BYTES;
        for (bucket, first_byte_ms, page_ms) in [(&buckets[0], 30, 200), (&buckets[1], 2, 1000)] {
            stats.observe_first_byte(bucket, Duration::from_millis(first_byte_ms));
            stats.observe_body(bucket, page, Duration::from_millis(page_ms));
        }
        stats.observe_first_byte(&buckets[2], Duration::from_millis(300));
        stats.observe_body(&buckets[2], 1024, Duration::from_millis(4));

        let best = |bytes| {
            stats
                .snapshot(&buckets, bytes)
                .best(&[], Duration::from_secs(10))
        };
        assert_eq!(best(4096), Some(1));
        assert_eq!(best(page), Some(0));
        let tail_millis =
            |index: usize| stats.model(&buckets[index]).tail(page).unwrap().as_millis();
        assert_eq!(tail_millis(0), 230);
        assert_eq!(tail_millis(2), 304);
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_and_cancelled_reads_affect_routing_without_poisoning_health() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for (bucket, millis) in buckets.iter().zip([3, 5, 6]) {
            success(&stats, bucket, millis).await;
        }
        let active = (0..3)
            .map(|_| stats.begin(&buckets[0], None, 0))
            .collect::<Vec<_>>();
        advance(Duration::from_millis(20)).await;
        assert_eq!(
            stats
                .snapshot(&buckets, 0)
                .best(&[], Duration::from_secs(1)),
            Some(1)
        );
        drop(active);
        assert_eq!(
            stats
                .snapshot(&buckets, 0)
                .best(&[], Duration::from_secs(1)),
            Some(1)
        );
        success(&stats, &buckets[0], 3).await;
        assert_eq!(
            stats
                .snapshot(&buckets, 0)
                .best(&[], Duration::from_secs(1)),
            Some(0)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn neutral_outcomes_and_idle_peers_preserve_local_preference() {
        for peer_latencies in [None, Some([60, 150])] {
            let stats = BucketedStats::default();
            let buckets = buckets();
            stats.snapshot(&buckets, 0);
            success(&stats, &buckets[0], 10).await;
            if let Some(latencies) = peer_latencies {
                for (bucket, millis) in buckets.iter().skip(1).zip(latencies) {
                    success(&stats, bucket, millis).await;
                }
            }
            let active = (0..3)
                .map(|_| stats.begin(&buckets[0], None, 0))
                .collect::<Vec<_>>();
            advance(Duration::from_secs(1)).await;
            for observation in active {
                observation.complete(Outcome::Neutral);
            }
            advance(Duration::from_secs(60)).await;
            let (index, probe) = stats.snapshot(&buckets, 0).primary(Duration::from_secs(1));
            assert_eq!(index, 0);
            assert!(probe.is_none());
            assert!(!stats.entry(&buckets[0]).lock().deprioritized);
        }
    }
}
