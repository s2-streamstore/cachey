use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use bytesize::MIB;
use dashmap::DashMap;
use exponential_decay_histogram::{ExponentialDecayHistogram, Snapshot};
use parking_lot::{Mutex, MutexGuard};
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
const MIN_THROUGHPUT_SAMPLE_BYTES: u64 = MIB;

#[derive(Debug, Clone)]
pub struct BucketMetrics {
    pub error_rate: f64,
    pub deprioritized: bool,
    pub consecutive_failures: u32,
    pub recovery_successes: u32,
    /// Time from starting a request to its validated body, including SDK retries.
    pub latency_mean: Duration,
    pub latency_p99: Duration,
    /// Time from starting a request to receiving successful response headers, including SDK
    /// retries.
    pub first_byte_mean: Duration,
    pub first_byte_p99: Duration,
    /// Time to read a body smaller than `MIN_THROUGHPUT_SAMPLE_BYTES` after its headers.
    pub small_body_mean: Duration,
    pub small_body_p99: Duration,
    /// Body bytes per second after headers, for larger bodies, or `None` before one is measured or
    /// while bodies arrive too fast to time, as when already buffered with their headers. Derived
    /// from the time per byte, so the mean is harmonic and p1 reflects the slowest transfers.
    pub throughput_mean: Option<f64>,
    pub throughput_p1: Option<f64>,
}

#[derive(Debug, Clone, Copy, Default)]
struct Summary {
    mean: Duration,
    slow: Duration,
}

fn is_small_body(bytes: u64) -> bool {
    bytes < MIN_THROUGHPUT_SAMPLE_BYTES
}

#[derive(Debug, Clone, Copy)]
pub(super) struct BodySample {
    pub bytes: u64,
    /// Time from response headers to the end of the body.
    pub elapsed: Duration,
}

impl BodySample {
    /// Duration for a small body. A larger one is measured per MiB, so slow transfers form the
    /// upper tail like other durations.
    fn measure(self) -> Duration {
        if is_small_body(self.bytes) {
            self.elapsed
        } else {
            self.elapsed.mul_f64(MIB as f64 / self.bytes as f64)
        }
    }
}

/// Measurements from one backend request.
#[derive(Debug, Clone, Copy)]
pub(super) struct Sample {
    pub first_byte: Duration,
    pub body: BodySample,
}

impl Sample {
    #[cfg(test)]
    pub fn from_millis(first_byte_ms: u64, body_bytes: u64, body_ms: u64) -> Self {
        Self {
            first_byte: Duration::from_millis(first_byte_ms),
            body: BodySample {
                bytes: body_bytes,
                elapsed: Duration::from_millis(body_ms),
            },
        }
    }

    fn latency(&self) -> Duration {
        self.first_byte.saturating_add(self.body.elapsed)
    }
}

/// Latency summaries for one bucket. Pages are requested whole, so the body size is unknown until
/// response headers announce it: earlier decisions use complete request latency across the sizes
/// the bucket actually served, and later ones predict body time for the announced length.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct LatencyModel {
    latency: Option<Summary>,
    first_byte: Option<Summary>,
    small_body: Option<Summary>,
    body_pace: Option<Summary>,
}

impl LatencyModel {
    /// Slow complete request latency, or `None` before any measurement.
    fn tail(&self) -> Option<Duration> {
        self.latency.map(|latency| latency.slow)
    }

    pub fn first_byte_tail(&self) -> Option<Duration> {
        self.first_byte.map(|first_byte| first_byte.slow)
    }

    /// Slow body time after headers announce `bytes`, or `None` before the bucket has served a body
    /// in the same size class.
    pub fn body_tail(&self, bytes: u64) -> Option<Duration> {
        if is_small_body(bytes) {
            self.small_body.map(|small_body| small_body.slow)
        } else {
            self.body_pace.map(|pace| {
                Duration::try_from_secs_f64(pace.slow.as_secs_f64() * bytes as f64 / MIB as f64)
                    .unwrap_or(Duration::MAX)
            })
        }
    }

    /// Slow time to finish the phases a request reached: response headers, then any body whose
    /// length they announced. `None` until the bucket has measured each of those phases, including
    /// a body in the announced size class.
    pub fn reached_tail(&self, announced_body_bytes: Option<u64>) -> Option<Duration> {
        let first_byte = self.first_byte_tail()?;
        match announced_body_bytes {
            None => Some(first_byte),
            Some(bytes) => Some(first_byte.saturating_add(self.body_tail(bytes)?)),
        }
    }

    fn covers(&self, sample: &Sample) -> bool {
        self.latency.is_some() && self.body_tail(sample.body.bytes).is_some()
    }
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

/// The first sample whose preceding weight reaches `q`, as `Snapshot::value` defines it, but
/// independent of the reservoir's random order. Equal values are treated as equal-weight samples,
/// as they are when recorded in the same second, rather than in whatever order their weights fall.
/// A tolerance absorbs floating-point sums that land exactly on the quantile.
fn quantile(snapshot: &Snapshot, q: f64) -> i64 {
    const TOLERANCE: f64 = 1e-9;
    let mut samples = snapshot.exemplars().map(|(value, &())| value).peekable();
    let mut preceding = 0.0;
    let mut last = 0;
    for (value, weight) in snapshot.values() {
        let mut count = 0_u32;
        while samples.next_if_eq(&value).is_some() {
            count += 1;
        }
        let last_sample_start = preceding + weight * f64::from(count - 1) / f64::from(count);
        if last_sample_start >= q - TOLERANCE {
            return value;
        }
        preceding += weight;
        last = value;
    }
    last
}

fn summarize(histogram: &ExponentialDecayHistogram) -> Option<Summary> {
    let snapshot = histogram.snapshot();
    (snapshot.count() > 0).then(|| Summary {
        mean: Duration::from_micros(snapshot.mean().round() as u64),
        slow: Duration::from_micros(quantile(&snapshot, 0.99) as u64),
    })
}

/// One phase's histogram, summarized again only after it changes.
#[derive(Debug)]
struct PhaseHistogram {
    histogram: ExponentialDecayHistogram,
    summary: Option<Summary>,
    changed: bool,
}

impl PhaseHistogram {
    fn new(now: Instant) -> Self {
        Self {
            histogram: histogram(now),
            summary: None,
            changed: false,
        }
    }

    fn update(&mut self, now: std::time::Instant, value: Duration) {
        self.histogram.update_at(now, histogram_micros(value));
        self.changed = true;
    }

    fn summary(&mut self) -> Option<Summary> {
        if std::mem::take(&mut self.changed) {
            self.summary = summarize(&self.histogram);
        }
        self.summary
    }
}

#[derive(Debug)]
struct LatencyHistograms {
    latency: PhaseHistogram,
    first_byte: PhaseHistogram,
    small_body: PhaseHistogram,
    body_pace: PhaseHistogram,
}

impl Default for LatencyHistograms {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            latency: PhaseHistogram::new(now),
            first_byte: PhaseHistogram::new(now),
            small_body: PhaseHistogram::new(now),
            body_pace: PhaseHistogram::new(now),
        }
    }
}

impl LatencyHistograms {
    fn body_histogram(&mut self, bytes: u64) -> &mut PhaseHistogram {
        if is_small_body(bytes) {
            &mut self.small_body
        } else {
            &mut self.body_pace
        }
    }

    /// Replacing history starts over the histograms the sample measures, keeping the body size
    /// class it did not reach.
    fn record(&mut self, now: Instant, sample: &Sample, replace_history: bool) {
        if replace_history {
            self.latency = PhaseHistogram::new(now);
            self.first_byte = PhaseHistogram::new(now);
            *self.body_histogram(sample.body.bytes) = PhaseHistogram::new(now);
        }
        let now = now.into_std();
        self.latency.update(now, sample.latency());
        self.first_byte.update(now, sample.first_byte);
        self.body_histogram(sample.body.bytes)
            .update(now, sample.body.measure());
    }

    /// Summarizes only the histograms updated since the last model.
    fn model(&mut self) -> LatencyModel {
        LatencyModel {
            latency: self.latency.summary(),
            first_byte: self.first_byte.summary(),
            small_body: self.small_body.summary(),
            body_pace: self.body_pace.summary(),
        }
    }

    #[cfg(test)]
    fn retention(&self) -> [(u64, usize); 4] {
        [
            &self.latency,
            &self.first_byte,
            &self.small_body,
            &self.body_pace,
        ]
        .map(|phase| {
            let snapshot = phase.histogram.snapshot();
            (snapshot.count(), snapshot.exemplars().count())
        })
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
    active: BTreeMap<Instant, usize>,
    routing_latency: Duration,
    best_latency: Option<Duration>,
    model: LatencyModel,
    model_at: Instant,
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

    /// Whether the model is due to be summarized again, which the caller then takes on.
    fn claim_model_refresh(&mut self, now: Instant) -> bool {
        let due = now.duration_since(self.model_at) >= LATENCY_SNAPSHOT_INTERVAL;
        if due {
            self.model_at = now;
        }
        due
    }

    fn overloaded(&self, now: Instant) -> bool {
        now < self.overload_until
    }

    fn record(&mut self, now: Instant, sample: &Sample, replace_history: bool) {
        let latency = sample.latency();
        self.routing_latency = if replace_history || self.best_latency.is_none() {
            latency
        } else {
            self.routing_latency.mul_f64(1.0 - LATENCY_ALPHA) + latency.mul_f64(LATENCY_ALPHA)
        };
        self.best_latency = Some(self.best_latency.map_or(latency, |best| best.min(latency)));
        if replace_history || !self.model.covers(sample) {
            self.model_at = now - LATENCY_SNAPSHOT_INTERVAL;
        }
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

/// A bucket's statistics, with its latency histograms under their own lock so that summarizing
/// them does not block the bucket's operations.
#[derive(Debug, Default)]
struct BucketState {
    stats: Mutex<BucketStats>,
    histograms: Mutex<LatencyHistograms>,
}

impl BucketState {
    /// Locks the statistics, first summarizing the histograms again once the model is due, while
    /// concurrent operations keep the previous model. The histograms stay locked until the new
    /// model is stored, so an earlier summary never replaces a later one.
    fn lock_stats(&self, now: Instant) -> MutexGuard<'_, BucketStats> {
        let mut stats = self.stats.lock();
        if !stats.claim_model_refresh(now) {
            return stats;
        }
        drop(stats);
        let mut histograms = self.histograms.lock();
        let model = histograms.model();
        let mut stats = self.stats.lock();
        stats.model = model;
        stats
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Outcome {
    Success,
    Failure,
    Overload,
    Neutral,
}

pub(super) struct ProbePermit {
    bucket: Arc<BucketState>,
}

impl Drop for ProbePermit {
    fn drop(&mut self) {
        self.bucket.stats.lock().probing = false;
    }
}

pub(super) struct BucketObservation {
    bucket: Arc<BucketState>,
    /// The bucket's latency model when the operation began.
    model: LatencyModel,
    /// The validated request's measurements, applied when the observation ends.
    sample: Option<Sample>,
    started: Instant,
    generation: u64,
    probe: Option<ProbePermit>,
    outcome: Option<Outcome>,
}

impl BucketObservation {
    /// A probe's measurements replace the history of the phases and body size class they cover, so
    /// a recovering bucket is judged by how it performs now.
    pub fn record(&mut self, sample: &Sample) {
        self.sample = Some(*sample);
    }

    pub fn model(&self) -> LatencyModel {
        self.model
    }

    pub fn complete(mut self, outcome: Outcome) {
        self.outcome = Some(outcome);
    }
}

impl Drop for BucketObservation {
    fn drop(&mut self) {
        let now = Instant::now();
        let sample = self.sample.as_ref();
        if let Some(sample) = sample {
            self.bucket
                .histograms
                .lock()
                .record(now, sample, self.probe.is_some());
        }
        let mut stats = self.bucket.stats.lock();
        if let Some(count) = stats.active.get_mut(&self.started) {
            *count -= 1;
            if *count == 0 {
                stats.active.remove(&self.started);
            }
        }
        if let Some(sample) = sample {
            stats.record(now, sample, self.probe.is_some());
        }
        let Some(outcome) = self.outcome else {
            let increase = now
                .duration_since(self.started)
                .saturating_sub(stats.routing_latency)
                .mul_f64(LATENCY_ALPHA);
            stats.routing_latency += increase;
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
    by_bucket: Arc<DashMap<BucketName, Arc<BucketState>>>,
}

struct BucketSnapshot {
    bucket: Arc<BucketState>,
    model: LatencyModel,
    latency: Duration,
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
            self.tail(left).is_some_and(|tail| tail >= remaining),
            left_stats.deprioritized,
        )
            .cmp(&(
                self.tail(right).is_some_and(|tail| tail >= remaining),
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
        if best.deprioritized || self.tail(winner).is_some_and(|tail| tail >= remaining) {
            return (winner, None);
        }
        let now = Instant::now();
        for (index, snapshot) in self.0.iter().enumerate() {
            if index == winner {
                continue;
            }
            let mut stats = snapshot.bucket.stats.lock();
            let stale = stats.best_latency.is_some()
                && now.duration_since(stats.last_outcome) >= RECOVERY_INTERVAL;
            if (stats.deprioritized || stale)
                && !stats.probing
                && stats.active.is_empty()
                && now >= stats.probe_after
                && locality_adjusted(stats.best_latency.unwrap_or_default(), index) < best.latency
            {
                stats.probing = true;
                return (
                    index,
                    Some(ProbePermit {
                        bucket: snapshot.bucket.clone(),
                    }),
                );
            }
        }
        (winner, None)
    }

    fn tail(&self, index: usize) -> Option<Duration> {
        self.0[index].model.tail()
    }

    pub fn model(&self, index: usize) -> LatencyModel {
        self.0[index].model
    }

    pub fn all_overloaded(&self) -> bool {
        self.0.iter().all(|bucket| bucket.overloaded)
    }
}

impl BucketedStats {
    fn entry(&self, bucket: &BucketName) -> Arc<BucketState> {
        if let Some(state) = self.by_bucket.get(bucket) {
            return state.clone();
        }
        self.by_bucket.entry(bucket.clone()).or_default().clone()
    }

    pub(super) fn begin(
        &self,
        bucket: &BucketName,
        probe: Option<ProbePermit>,
    ) -> BucketObservation {
        let state = self.entry(bucket);
        let started = Instant::now();
        let (model, generation) = {
            let mut stats = state.lock_stats(started);
            if probe.is_some() {
                stats.probe_after = started + recovery_delay();
            }
            *stats.active.entry(started).or_default() += 1;
            (stats.model, stats.generation)
        };
        BucketObservation {
            bucket: state,
            model,
            sample: None,
            started,
            generation,
            probe,
            outcome: None,
        }
    }

    pub(super) fn snapshot(&self, buckets: &BucketNameSet) -> RoutingSnapshot {
        let now = Instant::now();
        let mut preferred_latency = Duration::ZERO;
        RoutingSnapshot(
            buckets
                .iter()
                .enumerate()
                .map(|(index, bucket)| {
                    let state = self.entry(bucket);
                    let stats = state.lock_stats(now);
                    if index == 0 {
                        preferred_latency = stats.routing_latency;
                    }
                    let model = stats.model;
                    let tail = model.tail();
                    let mut latency = stats.routing_latency(now, tail.unwrap_or_default());
                    if index > 0 && tail.is_none() {
                        latency = latency.max(preferred_latency);
                    }
                    BucketSnapshot {
                        bucket: state.clone(),
                        model,
                        latency: locality_adjusted(latency, index),
                        deprioritized: stats.deprioritized,
                        error_rate: stats.error_rate(now),
                        overloaded: stats.overloaded(now),
                    }
                })
                .collect(),
        )
    }

    #[cfg(test)]
    pub(super) fn seed(&self, bucket: &BucketName, sample: Sample) {
        let mut observation = self.begin(bucket, None);
        observation.record(&sample);
        observation.complete(Outcome::Success);
    }

    /// Seeds a request for the 4-byte objects tests read, whose latency is all time to first byte.
    #[cfg(test)]
    pub(super) fn seed_latency(&self, bucket: &BucketName, millis: u64) {
        self.seed(bucket, Sample::from_millis(millis, 4, 0));
    }

    #[cfg(test)]
    pub(super) fn model(&self, bucket: &BucketName) -> LatencyModel {
        self.entry(bucket).lock_stats(Instant::now()).model
    }

    #[cfg(test)]
    pub(super) fn simulation_histograms(&self) -> Vec<(u64, usize)> {
        self.by_bucket
            .iter()
            .flat_map(|entry| entry.value().histograms.lock().retention())
            .collect()
    }

    pub fn export_bucket_metrics(&self, mut f: impl FnMut(&BucketName, &BucketMetrics)) {
        let now = Instant::now();
        let metrics: Vec<_> = self
            .by_bucket
            .iter()
            .map(|entry| {
                let stats = entry.value().lock_stats(now);
                let model = stats.model;
                let latency = model.latency.unwrap_or_default();
                let first_byte = model.first_byte.unwrap_or_default();
                let small_body = model.small_body.unwrap_or_default();
                let throughput =
                    |pace: Duration| (!pace.is_zero()).then(|| MIB as f64 / pace.as_secs_f64());
                let metrics = BucketMetrics {
                    error_rate: stats.error_rate(now),
                    deprioritized: stats.deprioritized,
                    consecutive_failures: stats.consecutive_failures,
                    recovery_successes: stats.recovery_successes,
                    latency_mean: latency.mean,
                    latency_p99: latency.slow,
                    first_byte_mean: first_byte.mean,
                    first_byte_p99: first_byte.slow,
                    small_body_mean: small_body.mean,
                    small_body_p99: small_body.slow,
                    throughput_mean: model.body_pace.and_then(|pace| throughput(pace.mean)),
                    throughput_p1: model.body_pace.and_then(|pace| throughput(pace.slow)),
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

    use bytesize::MIB;
    use tokio::time::{Instant, advance};

    use super::{
        BucketMetrics, BucketedStats, MIN_THROUGHPUT_SAMPLE_BYTES, Outcome, ProbePermit,
        RECOVERY_SUCCESSES, Sample, histogram, quantile,
    };
    use crate::types::{BucketName, BucketNameSet};

    fn buckets() -> BucketNameSet {
        BucketNameSet::new(
            ["local", "peer", "third"]
                .into_iter()
                .map(|name| BucketName::new(name).unwrap()),
        )
        .unwrap()
    }

    fn exported(stats: &BucketedStats) -> BucketMetrics {
        let mut exported = None;
        stats.export_bucket_metrics(|_, metrics| exported = Some(metrics.clone()));
        exported.expect("bucket metrics")
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
                stats.seed_latency(bucket, millis);
            }
            assert_eq!(
                stats.snapshot(&buckets).best(&[], Duration::from_secs(1)),
                Some(expected)
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_occasional_large_body_does_not_displace_the_preferred_copy() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for _ in 0..10 {
            stats.seed(&buckets[0], Sample::from_millis(5, 4096, 2));
            stats.seed(&buckets[1], Sample::from_millis(20, 4096, 2));
        }
        stats.seed(
            &buckets[0],
            Sample::from_millis(5, 2 * MIN_THROUGHPUT_SAMPLE_BYTES, 100),
        );
        assert_eq!(
            stats.snapshot(&buckets).best(&[], Duration::from_secs(10)),
            Some(0)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn elapsed_time_and_pre_failure_completions_do_not_restore_health() {
        let stats = BucketedStats::default();
        let bucket = &buckets()[0];
        let old = (0..RECOVERY_SUCCESSES)
            .map(|_| stats.begin(bucket, None))
            .collect::<Vec<_>>();
        stats.begin(bucket, None).complete(Outcome::Failure);
        for observation in old {
            observation.complete(Outcome::Success);
        }
        advance(Duration::from_secs(300)).await;
        assert!(stats.entry(bucket).stats.lock().deprioritized);
        assert_eq!(stats.entry(bucket).stats.lock().recovery_successes, 0);
        for _ in 0..RECOVERY_SUCCESSES - 1 {
            stats.seed_latency(bucket, 3);
        }
        assert!(stats.entry(bucket).stats.lock().deprioritized);
        stats.seed_latency(bucket, 3);
        assert!(!stats.entry(bucket).stats.lock().deprioritized);
    }

    #[tokio::test(start_paused = true)]
    async fn probing_is_exclusive_and_a_success_preserves_failure_history() {
        for local_latency in [None, Some(3)] {
            let stats = BucketedStats::default();
            let buckets = buckets();
            for (bucket, millis) in buckets.iter().zip([local_latency, Some(5), Some(6)]) {
                if let Some(millis) = millis {
                    stats.seed_latency(bucket, millis);
                }
            }
            stats.begin(&buckets[0], None).complete(Outcome::Failure);
            advance(Duration::from_secs(37)).await;
            let (index, probe) = stats.snapshot(&buckets).primary(Duration::from_secs(1));
            assert_eq!(index, 0);
            assert!(probe.is_some());
            assert_eq!(
                stats.snapshot(&buckets).primary(Duration::from_secs(1)).0,
                1
            );
            let mut observation = stats.begin(&buckets[0], probe);
            advance(Duration::from_millis(3)).await;
            observation.record(&Sample::from_millis(3, 4, 0));
            observation.complete(Outcome::Success);
            assert_eq!(
                stats.snapshot(&buckets).best(&[], Duration::from_secs(1)),
                Some(1)
            );
            let (index, probe) = stats.snapshot(&buckets).primary(Duration::from_secs(1));
            assert_eq!(index, 0);
            assert!(probe.is_some());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn only_a_successful_probe_replaces_latency_history() {
        for probe_succeeds in [false, true] {
            let stats = BucketedStats::default();
            let buckets = buckets();
            for (bucket, millis) in buckets.iter().zip([3, 5, 6]) {
                stats.seed_latency(bucket, millis);
            }
            stats.begin(&buckets[0], None).complete(Outcome::Failure);
            advance(Duration::from_secs(37)).await;
            let (index, probe) = stats.snapshot(&buckets).primary(Duration::from_secs(1));
            assert_eq!(index, 0);
            let mut observation = stats.begin(&buckets[0], probe);
            advance(Duration::from_millis(9)).await;
            if probe_succeeds {
                observation.record(&Sample::from_millis(9, 4, 0));
                observation.complete(Outcome::Success);
            } else {
                observation.complete(Outcome::Failure);
            }
            let tail = if probe_succeeds { 9 } else { 3 };
            assert_eq!(
                stats.snapshot(&buckets).tail(0),
                Some(Duration::from_millis(tail))
            );
            assert_eq!(
                stats.entry(&buckets[0]).stats.lock().best_latency,
                Some(Duration::from_millis(3))
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn metrics_callbacks_run_without_holding_statistics_locks() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for bucket in buckets.iter() {
            stats.seed_latency(bucket, 3);
        }
        let mut exported = 0;
        stats.export_bucket_metrics(|bucket, metrics| {
            assert_eq!(metrics.latency_mean, Duration::from_millis(3));
            let entry = stats
                .by_bucket
                .try_get_mut(bucket)
                .try_unwrap()
                .expect("metrics callbacks must not hold a map lock");
            assert!(entry.value().stats.try_lock().is_some());
            exported += 1;
        });
        assert_eq!(exported, buckets.len());
    }

    #[tokio::test(start_paused = true)]
    async fn exports_latency_by_phase_and_size_class() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        stats.seed(
            &bucket,
            Sample::from_millis(10, MIN_THROUGHPUT_SAMPLE_BYTES - 1, 1),
        );
        stats.seed(
            &bucket,
            Sample::from_millis(30, MIN_THROUGHPUT_SAMPLE_BYTES, 300),
        );
        stats.seed(
            &bucket,
            Sample::from_millis(20, 4 * MIN_THROUGHPUT_SAMPLE_BYTES, 2000),
        );
        advance(Duration::from_secs(1)).await;

        let metrics = exported(&stats);
        assert_eq!(metrics.latency_mean, Duration::from_millis(787));
        assert_eq!(metrics.latency_p99, Duration::from_millis(2020));
        assert_eq!(metrics.first_byte_mean, Duration::from_millis(20));
        assert_eq!(metrics.first_byte_p99, Duration::from_millis(30));
        assert_eq!(metrics.small_body_mean, Duration::from_millis(1));
        assert_eq!(metrics.small_body_p99, Duration::from_millis(1));
        assert!((metrics.throughput_mean.unwrap() - 2.5 * MIB as f64).abs() < 1e-6);
        assert!((metrics.throughput_p1.unwrap() - 2.0 * MIB as f64).abs() < 1e-6);
    }

    #[tokio::test(start_paused = true)]
    async fn quantiles_match_the_snapshot_definition_for_samples_recorded_together() {
        let now = Instant::now();
        let mut samples = histogram(now);
        for value in [5, 1, 3, 3, 3, 9, 7, 7, 2, 8] {
            samples.update_at(now.into_std(), value);
        }
        let snapshot = samples.snapshot();
        for q in [
            0.0, 0.05, 0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85, 0.95, 0.99, 1.0,
        ] {
            assert_eq!(quantile(&snapshot, q), snapshot.value(q), "{q}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn one_slow_transfer_sets_the_throughput_tail() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        for _ in 0..50 {
            stats.seed(&bucket, Sample::from_millis(10, 16 * MIB, 16));
        }
        stats.seed(&bucket, Sample::from_millis(10, 16 * MIB, 16_000));
        advance(Duration::from_secs(1)).await;
        assert_eq!(
            stats.model(&bucket).body_tail(16 * MIB),
            Some(Duration::from_secs(16))
        );
        assert!((exported(&stats).throughput_p1.unwrap() - MIB as f64).abs() < 1e-6);
    }

    #[tokio::test(start_paused = true)]
    async fn bodies_too_fast_to_time_report_no_throughput() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        stats.seed(&bucket, Sample::from_millis(10, 16 * MIB, 0));
        advance(Duration::from_secs(1)).await;
        let metrics = exported(&stats);
        assert_eq!(metrics.throughput_mean, None);
        assert_eq!(metrics.throughput_p1, None);
    }

    #[test]
    fn operations_keep_the_previous_model_while_another_summarizes() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        stats.seed_latency(&bucket, 3);
        let previous = stats.model(&bucket);
        stats.seed(&bucket, Sample::from_millis(3, 4 * MIB, 10));
        let state = stats.entry(&bucket);
        assert!(state.stats.lock().claim_model_refresh(Instant::now()));
        let summarizing = state.histograms.lock();
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            scope.spawn(|| sender.send(stats.begin(&bucket, None).model()).unwrap());
            let model = receiver.recv_timeout(Duration::from_secs(5));
            drop(summarizing);
            let model = model.expect("an operation waited for the model summary");
            assert_eq!(model.tail(), previous.tail());
            assert_eq!(model.body_tail(4 * MIB), None);
        });
    }

    #[tokio::test(start_paused = true)]
    async fn a_probe_keeps_history_for_a_size_class_it_did_not_measure() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        stats.seed(&bucket, Sample::from_millis(3, 4 * MIB, 250));
        let probe = ProbePermit {
            bucket: stats.entry(&bucket),
        };
        let mut observation = stats.begin(&bucket, Some(probe));
        observation.record(&Sample::from_millis(9, 4096, 1));
        observation.complete(Outcome::Success);
        let model = stats.model(&bucket);
        assert_eq!(model.tail(), Some(Duration::from_millis(10)));
        assert_eq!(model.body_tail(4096), Some(Duration::from_millis(1)));
        assert_eq!(model.body_tail(16 * MIB), Some(Duration::from_secs(1)));
    }

    #[tokio::test(start_paused = true)]
    async fn body_tails_require_a_measurement_in_the_same_size_class() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        stats.seed(&bucket, Sample::from_millis(20, 4096, 3));
        let model = stats.model(&bucket);
        assert_eq!(model.body_tail(4096), Some(Duration::from_millis(3)));
        assert_eq!(model.body_tail(16 * MIN_THROUGHPUT_SAMPLE_BYTES), None);
        assert_eq!(model.reached_tail(None), Some(Duration::from_millis(20)));
        assert_eq!(
            model.reached_tail(Some(4096)),
            Some(Duration::from_millis(23))
        );
        assert_eq!(
            model.reached_tail(Some(16 * MIN_THROUGHPUT_SAMPLE_BYTES)),
            None
        );

        stats.seed(
            &bucket,
            Sample::from_millis(20, 4 * MIN_THROUGHPUT_SAMPLE_BYTES, 250),
        );
        let model = stats.model(&bucket);
        assert_eq!(
            model.body_tail(16 * MIN_THROUGHPUT_SAMPLE_BYTES),
            Some(Duration::from_secs(1))
        );
        assert_eq!(model.tail(), Some(Duration::from_millis(270)));
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_and_cancelled_reads_affect_routing_without_poisoning_health() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for (bucket, millis) in buckets.iter().zip([3, 5, 6]) {
            stats.seed_latency(bucket, millis);
        }
        let active = (0..3)
            .map(|_| stats.begin(&buckets[0], None))
            .collect::<Vec<_>>();
        advance(Duration::from_millis(20)).await;
        assert_eq!(
            stats.snapshot(&buckets).best(&[], Duration::from_secs(1)),
            Some(1)
        );
        drop(active);
        assert_eq!(
            stats.snapshot(&buckets).best(&[], Duration::from_secs(1)),
            Some(1)
        );
        stats.seed_latency(&buckets[0], 3);
        assert_eq!(
            stats.snapshot(&buckets).best(&[], Duration::from_secs(1)),
            Some(0)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn reads_within_the_request_tail_are_not_stalls() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        stats.seed(&buckets[0], Sample::from_millis(20, 4096, 180));
        for bucket in buckets.iter().skip(1) {
            stats.seed_latency(bucket, 250);
        }
        let _active = (0..3)
            .map(|_| stats.begin(&buckets[0], None))
            .collect::<Vec<_>>();
        advance(Duration::from_millis(300)).await;
        assert_eq!(
            stats.snapshot(&buckets).best(&[], Duration::from_secs(10)),
            Some(0)
        );
        advance(Duration::from_millis(600)).await;
        assert_eq!(
            stats.snapshot(&buckets).best(&[], Duration::from_secs(10)),
            Some(1)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn neutral_outcomes_and_idle_peers_preserve_local_preference() {
        for peer_latencies in [None, Some([60, 150])] {
            let stats = BucketedStats::default();
            let buckets = buckets();
            stats.snapshot(&buckets);
            stats.seed_latency(&buckets[0], 10);
            if let Some(latencies) = peer_latencies {
                for (bucket, millis) in buckets.iter().skip(1).zip(latencies) {
                    stats.seed_latency(bucket, millis);
                }
            }
            let active = (0..3)
                .map(|_| stats.begin(&buckets[0], None))
                .collect::<Vec<_>>();
            advance(Duration::from_secs(1)).await;
            for observation in active {
                observation.complete(Outcome::Neutral);
            }
            advance(Duration::from_secs(60)).await;
            let (index, probe) = stats.snapshot(&buckets).primary(Duration::from_secs(1));
            assert_eq!(index, 0);
            assert!(probe.is_none());
            assert!(!stats.entry(&buckets[0]).stats.lock().deprioritized);
        }
    }
}
