mod model;
mod reservoir;

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, OnceLock},
    time::Duration,
};

use dashmap::DashMap;
use parking_lot::{Mutex, MutexGuard};
use tokio::{
    sync::{Notify, futures::Notified},
    time::Instant,
};

pub use self::model::LatencyEstimate;
use self::model::{Refit, SizeHistogram, SizeModel};
use crate::{
    object_store::DownloadLimits,
    types::{BucketName, BucketNameSet},
};

const ERROR_ALPHA: f64 = 0.015;
const LATENCY_ALPHA: f64 = 0.1;
/// Latency observations have a 15-second half-life of model time, which advances at most
/// 150 ms per observation so that a half-life spans at least 100 observations.
const LATENCY_DECAY: f64 = std::f64::consts::LN_2 / 15.0;
const OBSERVATION_SPACING: Duration = Duration::from_millis(150);
/// Temporary routing penalties fade sooner than latency observations.
const ROUTING_DECAY: f64 = std::f64::consts::LN_2 / 5.0;
const LATENCY_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(1);
const RECOVERY_INTERVAL: Duration = Duration::from_secs(30);
const RECOVERY_SUCCESSES: u32 = 20;
const OVERLOAD_INTERVAL: Duration = Duration::from_secs(1);

/// Bucket health and latency. Estimates are `None` until measured.
#[derive(Debug, Clone)]
pub struct BucketMetrics {
    pub error_rate: f64,
    pub deprioritized: bool,
    pub consecutive_failures: u32,
    pub recovery_successes: u32,
    /// Request start through validated headers, by requested bytes, including SDK retries.
    pub first_byte: Option<LatencyEstimate>,
    /// Validated headers through the end of the body, by response bytes.
    pub body: Option<LatencyEstimate>,
    /// Request start through body validation, by requested bytes, including SDK retries.
    pub request: Option<LatencyEstimate>,
}

fn decay(rate: f64, elapsed: Duration) -> f64 {
    (-rate * elapsed.as_secs_f64()).exp()
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Headers {
    pub at: Instant,
    pub body_bytes: u64,
}

/// Request timing and body length from validated headers.
#[derive(Debug, Default)]
pub(super) struct RequestTimeline {
    started: OnceLock<Instant>,
    headers: OnceLock<Headers>,
    changed: Notify,
}

impl RequestTimeline {
    pub fn start(&self) -> Instant {
        let now = Instant::now();
        if self.started.set(now).is_ok() {
            self.changed.notify_waiters();
        }
        now
    }

    pub fn receive_headers(&self, body_bytes: u64) {
        let headers = Headers {
            at: Instant::now(),
            body_bytes,
        };
        if self.headers.set(headers).is_ok() {
            self.changed.notify_waiters();
        }
    }

    pub fn started(&self) -> Option<Instant> {
        self.started.get().copied()
    }

    pub fn headers(&self) -> Option<Headers> {
        self.headers.get().copied()
    }

    pub fn announced_body_bytes(&self) -> Option<u64> {
        self.headers().map(|headers| headers.body_bytes)
    }

    /// Resolves after the next change; create it before reading the timeline.
    pub fn changed(&self) -> Notified<'_> {
        self.changed.notified()
    }

    fn sample(&self, requested_bytes: u64) -> Option<Sample> {
        let started = self.started()?;
        let headers = self.headers()?;
        let now = Instant::now();
        Some(Sample {
            requested_bytes,
            first_byte: headers.at.saturating_duration_since(started),
            body_bytes: headers.body_bytes,
            body: now.saturating_duration_since(headers.at),
        })
    }
}

/// Measurements from one backend request.
#[derive(Debug, Clone, Copy)]
pub(super) struct Sample {
    pub requested_bytes: u64,
    pub first_byte: Duration,
    pub body_bytes: u64,
    /// Time from response headers to the end of the body.
    pub body: Duration,
}

impl Sample {
    /// An exact-range read, whose response is the requested size.
    #[cfg(test)]
    pub fn from_millis(first_byte_ms: u64, body_bytes: u64, body_ms: u64) -> Self {
        Self {
            requested_bytes: body_bytes,
            first_byte: Duration::from_millis(first_byte_ms),
            body_bytes,
            body: Duration::from_millis(body_ms),
        }
    }

    fn latency(&self) -> Duration {
        self.first_byte.saturating_add(self.body)
    }
}

/// Per-bucket latency by size. Time to first byte and complete requests are modeled by
/// requested bytes, bodies by the length announced in headers.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct LatencyModel {
    first_byte: SizeModel,
    body: SizeModel,
    request: SizeModel,
}

impl LatencyModel {
    pub fn first_byte_hedge_delay(&self, requested_bytes: u64) -> Option<Duration> {
        self.first_byte.hedge_delay(requested_bytes)
    }

    pub fn body_hedge_delay(&self, body_bytes: u64) -> Option<Duration> {
        self.body.hedge_delay(body_bytes)
    }

    #[cfg(test)]
    pub fn first_byte_tail(&self, requested_bytes: u64) -> Option<Duration> {
        self.first_byte.tail(requested_bytes)
    }

    #[cfg(test)]
    pub fn body_tail(&self, body_bytes: u64) -> Option<Duration> {
        self.body.tail(body_bytes)
    }

    /// p99 of a fresh request, by phase once the response size is known.
    pub fn request_tail(&self, requested_bytes: u64, body_bytes: Option<u64>) -> Option<Duration> {
        body_bytes
            .and_then(|bytes| {
                Some(
                    self.first_byte
                        .tail(requested_bytes)?
                        .saturating_add(self.body.tail(bytes)?),
                )
            })
            .or_else(|| self.request.tail(requested_bytes))
    }

    /// Timeouts affect health only after allowing time for each phase reached.
    /// Use `unmeasured` for unknown phases and the request tail as a floor.
    pub fn timeout_allowance(
        &self,
        requested_bytes: u64,
        announced_body_bytes: Option<u64>,
        unmeasured: Duration,
    ) -> Duration {
        let first_byte = self.first_byte.tail(requested_bytes).unwrap_or(unmeasured);
        let reached = announced_body_bytes.map_or(first_byte, |bytes| {
            first_byte.saturating_add(self.body.tail(bytes).unwrap_or(unmeasured))
        });
        self.request_tail(requested_bytes, announced_body_bytes)
            .map_or(reached, |tail| tail.max(reached))
    }
}

#[derive(Debug)]
struct LatencyEstimates {
    first_byte: SizeHistogram,
    body: SizeHistogram,
    request: SizeHistogram,
    refitting: bool,
}

impl Default for LatencyEstimates {
    fn default() -> Self {
        Self {
            first_byte: SizeHistogram::new(false),
            body: SizeHistogram::new(true),
            request: SizeHistogram::new(false),
            refitting: false,
        }
    }
}

impl LatencyEstimates {
    fn record(&mut self, now: Instant, sample: &Sample) {
        self.first_byte
            .update(now, sample.requested_bytes, sample.first_byte);
        self.body.update(now, sample.body_bytes, sample.body);
        self.request
            .update(now, sample.requested_bytes, sample.latency());
    }

    /// An unvalidated copy still measures any headers it received.
    fn record_headers(&mut self, now: Instant, requested_bytes: u64, timeline: &RequestTimeline) {
        if let (Some(started), Some(headers)) = (timeline.started(), timeline.headers()) {
            self.first_byte.update(
                now,
                requested_bytes,
                headers.at.saturating_duration_since(started),
            );
        }
    }

    fn histograms(&mut self) -> [&mut SizeHistogram; 3] {
        [&mut self.first_byte, &mut self.body, &mut self.request]
    }

    fn model(&self) -> LatencyModel {
        LatencyModel {
            first_byte: self.first_byte.model(),
            body: self.body.model(),
            request: self.request.model(),
        }
    }

    #[cfg(test)]
    fn retention(&self) -> impl Iterator<Item = (u64, usize)> + '_ {
        [&self.first_byte, &self.body, &self.request]
            .into_iter()
            .flat_map(SizeHistogram::retention)
    }
}

/// An active copy with a predicted tail, ordered by when it becomes overdue.
type TimedCopy = (Instant, Instant);

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
    active: usize,
    timed: BTreeMap<TimedCopy, usize>,
    /// Moving average of successful request latency.
    success_latency: Option<Duration>,
    /// Routing penalty above `success_latency` from stalled and canceled copies.
    routing_excess: Duration,
    routing_updated_at: Instant,
    best_latency: Option<Duration>,
    estimates: LatencyEstimates,
    summarized_at: Instant,
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
            active: 0,
            timed: BTreeMap::new(),
            success_latency: None,
            routing_excess: Duration::ZERO,
            routing_updated_at: now,
            best_latency: None,
            estimates: LatencyEstimates::default(),
            summarized_at: now,
        }
    }
}

impl BucketStats {
    fn error_rate(&self, now: Instant) -> f64 {
        self.error_rate
            * decay(
                ERROR_ALPHA,
                now.saturating_duration_since(self.last_outcome),
            )
    }

    /// A recovery probe replaces routing history; latency estimates keep theirs.
    fn succeed(&mut self, now: Instant, sample: &Sample, generation: u64, probe: bool) {
        let latency = sample.latency();
        match self.success_latency {
            Some(mean) if !probe => {
                self.routing_excess = self
                    .current_routing_latency(now)
                    .saturating_sub(mean)
                    .mul_f64(1.0 - LATENCY_ALPHA);
                self.success_latency =
                    Some(mean.mul_f64(1.0 - LATENCY_ALPHA) + latency.mul_f64(LATENCY_ALPHA));
            }
            _ => {
                self.routing_excess = Duration::ZERO;
                self.success_latency = Some(latency);
            }
        }
        self.routing_updated_at = now;
        self.best_latency = Some(self.best_latency.map_or(latency, |best| best.min(latency)));
        self.estimates.record(now, sample);
        let error_rate = self.error_rate(now);
        if generation == self.generation {
            self.error_rate = error_rate * (1.0 - ERROR_ALPHA);
            self.consecutive_failures = 0;
            if self.deprioritized {
                self.recovery_successes += 1;
                if self.recovery_successes >= RECOVERY_SUCCESSES {
                    self.deprioritized = false;
                }
            }
            if probe {
                self.probe_after = now;
            }
        } else {
            self.error_rate = error_rate;
        }
        self.last_outcome = now;
    }

    fn fail(&mut self, now: Instant, overload: bool) {
        self.error_rate = self.error_rate(now) * (1.0 - ERROR_ALPHA) + ERROR_ALPHA;
        self.deprioritized = true;
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.recovery_successes = 0;
        self.generation = self.generation.wrapping_add(1);
        self.probe_after = now + recovery_delay();
        self.last_outcome = now;
        if overload {
            self.overload_until = now + OVERLOAD_INTERVAL;
        }
    }

    /// An unfinished copy can raise routing latency toward its elapsed time but never lower it.
    fn raise_routing_latency(&mut self, now: Instant, elapsed: Duration) {
        let current = self.current_routing_latency(now);
        let raised = current + elapsed.saturating_sub(current).mul_f64(LATENCY_ALPHA);
        self.routing_excess = raised.saturating_sub(self.success_latency.unwrap_or_default());
        self.routing_updated_at = now;
    }

    /// Excess routing latency decays toward the success mean, even while the bucket is idle.
    /// Without a success, keep the penalty.
    fn current_routing_latency(&self, now: Instant) -> Duration {
        let Some(mean) = self.success_latency else {
            return self.routing_excess;
        };
        mean + self.routing_excess.mul_f64(decay(
            ROUTING_DECAY,
            now.saturating_duration_since(self.routing_updated_at),
        ))
    }

    /// Three overdue copies count as a stall lasting as long as the third.
    fn routing_latency(&self, now: Instant, current: Duration) -> Duration {
        let mut count = 0;
        for ((overdue_at, started), active) in &self.timed {
            if *overdue_at > now {
                break;
            }
            count += active;
            if count >= 3 {
                return current.max(now.duration_since(*started));
            }
        }
        current
    }
}

/// Refits once per interval, or sooner while observation counts double. Fitting runs
/// outside the lock, one refit at a time, so it does not delay the bucket's copies.
fn latency_model(stats: &mut MutexGuard<'_, BucketStats>, now: Instant) -> LatencyModel {
    if !stats.estimates.refitting {
        let due = now.duration_since(stats.summarized_at) >= LATENCY_SNAPSHOT_INTERVAL;
        if due {
            stats.summarized_at = now;
        }
        let refits: [Option<Refit>; 3] = stats
            .estimates
            .histograms()
            .map(|histogram| histogram.refit(due));
        if refits.iter().any(Option::is_some) {
            stats.estimates.refitting = true;
            let models =
                MutexGuard::unlocked(stats, || refits.map(|refit| refit.map(|refit| refit.fit())));
            for (histogram, model) in stats.estimates.histograms().into_iter().zip(models) {
                if let Some(model) = model {
                    histogram.set_model(model);
                }
            }
            stats.estimates.refitting = false;
        }
    }
    stats.estimates.model()
}

#[derive(Debug, Clone, Copy)]
pub(super) enum Outcome {
    /// A validated response.
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

/// Records a copy's outcome when dropped; a copy dropped without one was canceled.
pub(super) struct BucketObservation<'a> {
    stats: Arc<Mutex<BucketStats>>,
    started: Instant,
    timed: Option<TimedCopy>,
    timeline: &'a RequestTimeline,
    requested_bytes: u64,
    generation: u64,
    probe: Option<ProbePermit>,
    outcome: Option<Outcome>,
}

impl BucketObservation<'_> {
    pub fn complete(mut self, outcome: Outcome) {
        self.outcome = Some(outcome);
    }
}

impl Drop for BucketObservation<'_> {
    fn drop(&mut self) {
        let mut stats = self.stats.lock();
        let now = Instant::now();
        stats.active -= 1;
        if let Some(timed) = self.timed
            && let Some(count) = stats.timed.get_mut(&timed)
        {
            *count -= 1;
            if *count == 0 {
                stats.timed.remove(&timed);
            }
        }
        match self.outcome {
            Some(Outcome::Success) => {
                if let Some(sample) = self.timeline.sample(self.requested_bytes) {
                    stats.succeed(now, &sample, self.generation, self.probe.is_some());
                }
            }
            _ => stats
                .estimates
                .record_headers(now, self.requested_bytes, self.timeline),
        }
        match self.outcome {
            None => stats.raise_routing_latency(now, now.duration_since(self.started)),
            Some(Outcome::Failure) => stats.fail(now, false),
            Some(Outcome::Overload) => stats.fail(now, true),
            Some(Outcome::Success | Outcome::Neutral) => {}
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
    /// p99 of a request for the routed size.
    tail: Option<Duration>,
    latency: Duration,
    deprioritized: bool,
    error_rate: f64,
    overloaded: bool,
}

/// Bucket state for routing one request size.
pub(super) struct RoutingSnapshot(Vec<BucketSnapshot>);

impl RoutingSnapshot {
    fn compare(&self, left: usize, right: usize, remaining: Duration) -> std::cmp::Ordering {
        let left_stats = &self.0[left];
        let right_stats = &self.0[right];
        (!self.feasible(left, remaining), left_stats.deprioritized)
            .cmp(&(!self.feasible(right, remaining), right_stats.deprioritized))
            .then_with(|| {
                if left_stats.deprioritized {
                    left_stats.error_rate.total_cmp(&right_stats.error_rate)
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .then_with(|| (left_stats.latency, left).cmp(&(right_stats.latency, right)))
    }

    /// Whether a request is expected to finish in time; unmeasured buckets may.
    pub fn feasible(&self, index: usize, remaining: Duration) -> bool {
        self.tail(index).is_none_or(|tail| tail < remaining)
    }

    pub fn best(&self, eligible: impl Fn(usize) -> bool, remaining: Duration) -> Option<usize> {
        (0..self.0.len())
            .filter(|index| eligible(*index))
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
        let winner = self.best(|_| true, remaining).expect("nonempty bucket set");
        let best = &self.0[winner];
        if best.deprioritized || !self.feasible(winner, remaining) {
            return (winner, None);
        }
        let now = Instant::now();
        for (index, snapshot) in self.0.iter().enumerate() {
            if index == winner {
                continue;
            }
            let mut stats = snapshot.stats.lock();
            let stale = stats.best_latency.is_some()
                && now.duration_since(stats.last_outcome) >= RECOVERY_INTERVAL;
            if (stats.deprioritized || stale)
                && !stats.probing
                && stats.active == 0
                && now >= stats.probe_after
                && locality_adjusted(stats.best_latency.unwrap_or_default(), index) < best.latency
            {
                stats.probing = true;
                return (
                    index,
                    Some(ProbePermit {
                        stats: snapshot.stats.clone(),
                    }),
                );
            }
        }
        (winner, None)
    }

    fn tail(&self, index: usize) -> Option<Duration> {
        self.0[index].tail
    }

    pub fn model(&self, index: usize) -> LatencyModel {
        self.0[index].model
    }

    pub fn overloaded(&self, index: usize) -> bool {
        self.0[index].overloaded
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

    /// A copy becomes overdue after four times its predicted request tail; without a
    /// prediction for its size, it never does.
    pub(super) fn begin<'a>(
        &self,
        bucket: &BucketName,
        probe: Option<ProbePermit>,
        timeline: &'a RequestTimeline,
        requested_bytes: u64,
    ) -> BucketObservation<'a> {
        let stats = self.entry(bucket);
        let started = Instant::now();
        let (timed, generation) = {
            let mut stats = stats.lock();
            if probe.is_some() {
                stats.probe_after = started + recovery_delay();
            }
            let timed = latency_model(&mut stats, started)
                .request_tail(requested_bytes, None)
                .map(|tail| {
                    let overdue = tail.saturating_mul(4).max(Duration::from_millis(1));
                    (started + overdue, started)
                });
            stats.active += 1;
            if let Some(timed) = timed {
                *stats.timed.entry(timed).or_default() += 1;
            }
            (timed, stats.generation)
        };
        BucketObservation {
            stats,
            started,
            timed,
            timeline,
            requested_bytes,
            generation,
            probe,
            outcome: None,
        }
    }

    pub(super) fn snapshot(
        &self,
        buckets: &BucketNameSet,
        requested_bytes: u64,
    ) -> RoutingSnapshot {
        let now = Instant::now();
        let mut preferred_latency = Duration::ZERO;
        RoutingSnapshot(
            buckets
                .iter()
                .enumerate()
                .map(|(index, bucket)| {
                    let state = self.entry(bucket);
                    let mut stats = state.lock();
                    let model = latency_model(&mut stats, now);
                    let current = stats.current_routing_latency(now);
                    if index == 0 {
                        preferred_latency = current;
                    }
                    let mut latency = stats.routing_latency(now, current);
                    if index > 0 && stats.success_latency.is_none() {
                        latency = latency.max(preferred_latency);
                    }
                    BucketSnapshot {
                        stats: state.clone(),
                        model,
                        tail: model.request_tail(requested_bytes, None),
                        latency: locality_adjusted(latency, index),
                        deprioritized: stats.deprioritized,
                        error_rate: stats.error_rate(now),
                        overloaded: now < stats.overload_until,
                    }
                })
                .collect(),
        )
    }

    #[cfg(test)]
    pub(super) fn seed(&self, bucket: &BucketName, sample: Sample) {
        let stats = self.entry(bucket);
        let mut stats = stats.lock();
        let generation = stats.generation;
        stats.succeed(Instant::now(), &sample, generation, false);
    }

    /// Seeds a request for the 4-byte objects tests read, whose latency is all time to first byte.
    #[cfg(test)]
    pub(super) fn seed_latency(&self, bucket: &BucketName, millis: u64) {
        self.seed(bucket, Sample::from_millis(millis, 4, 0));
    }

    #[cfg(test)]
    pub(super) fn model(&self, bucket: &BucketName) -> LatencyModel {
        latency_model(&mut self.entry(bucket).lock(), Instant::now())
    }

    #[cfg(test)]
    pub(super) fn simulation_histograms(&self) -> Vec<(u64, usize)> {
        self.by_bucket
            .iter()
            .flat_map(|entry| {
                entry
                    .value()
                    .lock()
                    .estimates
                    .retention()
                    .collect::<Vec<_>>()
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
                let model = latency_model(&mut stats, now);
                let metrics = BucketMetrics {
                    error_rate: stats.error_rate(now),
                    deprioritized: stats.deprioritized,
                    consecutive_failures: stats.consecutive_failures,
                    recovery_successes: stats.recovery_successes,
                    first_byte: model.first_byte.estimate(),
                    body: model.body.estimate(),
                    request: model.request.estimate(),
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
    use tokio::time::advance;

    use super::{
        BucketObservation, BucketedStats, LATENCY_SNAPSHOT_INTERVAL, Outcome, ProbePermit,
        RECOVERY_SUCCESSES, RequestTimeline, RoutingSnapshot, Sample,
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

    /// A copy of the 4-byte objects most tests read.
    fn begin<'a>(
        stats: &BucketedStats,
        bucket: &BucketName,
        probe: Option<ProbePermit>,
        timeline: &'a RequestTimeline,
    ) -> BucketObservation<'a> {
        stats.begin(bucket, probe, timeline, 4)
    }

    fn snapshot(stats: &BucketedStats, buckets: &BucketNameSet) -> RoutingSnapshot {
        stats.snapshot(buckets, 4)
    }

    fn best(stats: &BucketedStats, buckets: &BucketNameSet, remaining: Duration) -> Option<usize> {
        snapshot(stats, buckets).best(|_| true, remaining)
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
                best(&stats, &buckets, Duration::from_secs(1)),
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
        stats.seed(&buckets[0], Sample::from_millis(5, 2 * MIB, 100));
        assert_eq!(best(&stats, &buckets, Duration::from_secs(10)), Some(0));
    }

    #[tokio::test(start_paused = true)]
    async fn request_tails_and_feasibility_follow_the_routed_size() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for _ in 0..4 {
            stats.seed(&buckets[0], Sample::from_millis(20, 4096, 1));
            stats.seed(&buckets[0], Sample::from_millis(20, 16 * MIB, 180));
        }
        let remaining = Duration::from_millis(150);
        for (bytes, tail, feasible) in [(4096, 21, true), (16 * MIB, 200, false)] {
            let routing = stats.snapshot(&buckets, bytes);
            let predicted = routing.tail(0).unwrap();
            assert!(
                predicted.abs_diff(Duration::from_millis(tail)) < Duration::from_millis(1),
                "{bytes}: {predicted:?}"
            );
            assert_eq!(routing.feasible(0, remaining), feasible);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn elapsed_time_and_pre_failure_completions_do_not_restore_health() {
        let stats = BucketedStats::default();
        let bucket = &buckets()[0];
        let timeline = RequestTimeline::default();
        timeline.start();
        timeline.receive_headers(4);
        let old = (0..RECOVERY_SUCCESSES)
            .map(|_| begin(&stats, bucket, None, &timeline))
            .collect::<Vec<_>>();
        begin(&stats, bucket, None, &timeline).complete(Outcome::Failure);
        for observation in old {
            observation.complete(Outcome::Success);
        }
        advance(Duration::from_secs(300)).await;
        assert!(stats.entry(bucket).lock().deprioritized);
        assert_eq!(stats.entry(bucket).lock().recovery_successes, 0);
        for _ in 0..RECOVERY_SUCCESSES - 1 {
            stats.seed_latency(bucket, 3);
        }
        assert!(stats.entry(bucket).lock().deprioritized);
        stats.seed_latency(bucket, 3);
        assert!(!stats.entry(bucket).lock().deprioritized);
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
            let timeline = RequestTimeline::default();
            begin(&stats, &buckets[0], None, &timeline).complete(Outcome::Failure);
            advance(Duration::from_secs(37)).await;
            let (index, probe) = snapshot(&stats, &buckets).primary(Duration::from_secs(1));
            assert_eq!(index, 0);
            assert!(probe.is_some());
            assert_eq!(
                snapshot(&stats, &buckets).primary(Duration::from_secs(1)).0,
                1
            );
            timeline.start();
            let observation = begin(&stats, &buckets[0], probe, &timeline);
            advance(Duration::from_millis(3)).await;
            timeline.receive_headers(4);
            observation.complete(Outcome::Success);
            assert_eq!(best(&stats, &buckets, Duration::from_secs(1)), Some(1));
            let (index, probe) = snapshot(&stats, &buckets).primary(Duration::from_secs(1));
            assert_eq!(index, 0);
            assert!(probe.is_some());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_successful_probe_replaces_routing_history_but_not_estimates() {
        for probe_succeeds in [false, true] {
            let stats = BucketedStats::default();
            let buckets = buckets();
            for (bucket, millis) in buckets.iter().zip([30, 50, 60]) {
                stats.seed_latency(bucket, millis);
            }
            for _ in 0..4 {
                stats.seed(&buckets[0], Sample::from_millis(9, 4 * MIB, 250));
            }
            let timeline = RequestTimeline::default();
            begin(&stats, &buckets[0], None, &timeline).complete(Outcome::Failure);
            advance(Duration::from_secs(37)).await;
            let (index, probe) = snapshot(&stats, &buckets).primary(Duration::from_secs(1));
            assert_eq!(index, 0);
            timeline.start();
            let observation = begin(&stats, &buckets[0], probe, &timeline);
            advance(Duration::from_millis(3)).await;
            if probe_succeeds {
                timeline.receive_headers(4);
                observation.complete(Outcome::Success);
            } else {
                observation.complete(Outcome::Failure);
            }
            let latency = snapshot(&stats, &buckets).0[0].latency;
            assert_eq!(
                latency == Duration::from_millis(3),
                probe_succeeds,
                "{latency:?}"
            );
            assert_eq!(
                stats.model(&buckets[0]).body_tail(4 * MIB),
                Some(Duration::from_millis(250))
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
            let request = metrics.request.unwrap();
            assert_eq!(request.base, Duration::from_millis(3));
            assert_eq!(request.largest_bytes, 4);
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

    /// A body streaming at 10ms per MiB after 10ms to first byte.
    fn paced_body(bytes: u64) -> Sample {
        Sample {
            requested_bytes: bytes,
            first_byte: Duration::from_millis(10),
            body_bytes: bytes,
            body: Duration::from_millis(1) + Duration::from_micros(10_000 * bytes / MIB),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn slow_short_bodies_do_not_inflate_the_tail_of_full_pages() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        for _ in 0..4 {
            for _ in 0..5 {
                stats.seed(&bucket, paced_body(16 * MIB));
            }
            stats.seed(&bucket, paced_body(MIB));
            stats.seed(&bucket, Sample::from_millis(10, MIB / 16, 20));
        }
        let model = stats.model(&bucket);
        assert_eq!(model.body_tail(16 * MIB), Some(Duration::from_millis(161)));
        assert!(model.body_tail(3 * MIB / 2).unwrap() <= Duration::from_millis(33));
        assert!(model.body_tail(4096).unwrap() <= Duration::from_millis(20));
    }

    #[tokio::test(start_paused = true)]
    async fn unmeasured_transfers_receive_the_cold_timeout_allowance() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        let page = 16 * MIB;
        // A page read of a small object.
        stats.seed(
            &bucket,
            Sample {
                requested_bytes: page,
                first_byte: Duration::from_millis(20),
                body_bytes: 4096,
                body: Duration::from_millis(1),
            },
        );
        let model = stats.model(&bucket);
        let unmeasured = Duration::from_millis(100);
        assert_eq!(model.body_tail(4096), Some(Duration::from_millis(1)));
        assert_eq!(model.body_tail(page), None);
        for (announced, allowance) in [(None, 21), (Some(4096), 21), (Some(page), 120)] {
            assert_eq!(
                model.timeout_allowance(page, announced, unmeasured),
                Duration::from_millis(allowance),
                "{announced:?}"
            );
        }
        let cold = BucketedStats::default().model(&bucket);
        assert_eq!(cold.timeout_allowance(page, None, unmeasured), unmeasured);
        assert_eq!(
            cold.timeout_allowance(page, Some(4096), unmeasured),
            Duration::from_millis(200)
        );

        for _ in 0..4 {
            stats.seed(
                &bucket,
                Sample {
                    requested_bytes: page,
                    first_byte: Duration::from_millis(20),
                    body_bytes: 4 * MIB,
                    body: Duration::from_millis(251),
                },
            );
        }
        advance(LATENCY_SNAPSHOT_INTERVAL).await;
        let model = stats.model(&bucket);
        assert!(
            model
                .body_tail(page)
                .unwrap()
                .abs_diff(Duration::from_millis(1001))
                <= Duration::from_millis(1)
        );
        assert_eq!(
            model.request_tail(page, None),
            Some(Duration::from_millis(271))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn canceled_copies_measure_only_headers_they_received() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        stats.seed(&bucket, Sample::from_millis(10, 4, 1));
        for headers_after in [None, Some(Duration::from_millis(30))] {
            let timeline = RequestTimeline::default();
            timeline.start();
            let observation = begin(&stats, &bucket, None, &timeline);
            if let Some(delay) = headers_after {
                advance(delay).await;
                timeline.receive_headers(4);
            }
            advance(Duration::from_millis(500)).await;
            drop(observation);
        }
        advance(LATENCY_SNAPSHOT_INTERVAL).await;
        let model = stats.model(&bucket);
        assert_eq!(model.first_byte_tail(4), Some(Duration::from_millis(30)));
        assert_eq!(model.body_tail(4), Some(Duration::from_millis(1)));
        assert_eq!(model.request_tail(4, None), Some(Duration::from_millis(11)));
    }

    #[tokio::test(start_paused = true)]
    async fn failed_copies_measure_headers_but_not_bodies() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        stats.seed(&bucket, Sample::from_millis(10, 4, 1));
        let timeline = RequestTimeline::default();
        timeline.start();
        let observation = begin(&stats, &bucket, None, &timeline);
        advance(Duration::from_millis(30)).await;
        timeline.receive_headers(4);
        advance(Duration::from_millis(500)).await;
        observation.complete(Outcome::Failure);
        advance(LATENCY_SNAPSHOT_INTERVAL).await;
        let model = stats.model(&bucket);
        assert_eq!(model.first_byte_tail(4), Some(Duration::from_millis(30)));
        assert_eq!(model.body_tail(4), Some(Duration::from_millis(1)));
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_and_cancelled_reads_affect_routing_without_poisoning_health() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for (bucket, millis) in buckets.iter().zip([3, 5, 6]) {
            stats.seed_latency(bucket, millis);
        }
        let timeline = RequestTimeline::default();
        let active = (0..3)
            .map(|_| begin(&stats, &buckets[0], None, &timeline))
            .collect::<Vec<_>>();
        advance(Duration::from_millis(20)).await;
        assert_eq!(best(&stats, &buckets, Duration::from_secs(1)), Some(1));
        drop(active);
        assert_eq!(best(&stats, &buckets, Duration::from_secs(1)), Some(1));
        stats.seed_latency(&buckets[0], 3);
        assert_eq!(best(&stats, &buckets, Duration::from_secs(1)), Some(0));
    }

    #[tokio::test(start_paused = true)]
    async fn an_outlier_stops_steering_reads_away_once_its_rise_decays() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for (bucket, millis) in buckets.iter().zip([3, 4, 5]) {
            stats.seed_latency(bucket, millis);
        }
        assert_eq!(best(&stats, &buckets, Duration::from_secs(1)), Some(0));
        let timeline = RequestTimeline::default();
        let stall = begin(&stats, &buckets[0], None, &timeline);
        advance(Duration::from_millis(100)).await;
        drop(stall);
        assert_eq!(best(&stats, &buckets, Duration::from_secs(1)), Some(1));
        advance(Duration::from_secs(10)).await;
        assert_eq!(best(&stats, &buckets, Duration::from_secs(1)), Some(0));
    }

    #[tokio::test(start_paused = true)]
    async fn reads_within_their_own_request_tail_are_not_stalls() {
        for (requested, stalled) in [(16 * MIB, false), (4096, true)] {
            let stats = BucketedStats::default();
            let buckets = buckets();
            for _ in 0..4 {
                stats.seed(&buckets[0], Sample::from_millis(20, 4096, 1));
                stats.seed(&buckets[0], Sample::from_millis(20, 16 * MIB, 180));
            }
            for bucket in buckets.iter().skip(1) {
                stats.seed_latency(bucket, 250);
            }
            let timeline = RequestTimeline::default();
            let _active = (0..3)
                .map(|_| stats.begin(&buckets[0], None, &timeline, requested))
                .collect::<Vec<_>>();
            advance(Duration::from_millis(500)).await;
            assert_eq!(
                best(&stats, &buckets, Duration::from_secs(10)),
                Some(usize::from(stalled)),
                "{requested}"
            );
            advance(Duration::from_millis(400)).await;
            assert_eq!(best(&stats, &buckets, Duration::from_secs(10)), Some(1));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn copies_of_unpredicted_sizes_are_not_stalls() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for _ in 0..4 {
            stats.seed(&buckets[0], Sample::from_millis(20, 4096, 1));
        }
        for bucket in buckets.iter().skip(1) {
            stats.seed_latency(bucket, 250);
        }
        let timeline = RequestTimeline::default();
        let _active = (0..3)
            .map(|_| stats.begin(&buckets[0], None, &timeline, 16 * MIB))
            .collect::<Vec<_>>();
        advance(Duration::from_millis(500)).await;
        assert_eq!(best(&stats, &buckets, Duration::from_secs(10)), Some(0));
    }

    #[tokio::test(start_paused = true)]
    async fn neutral_outcomes_and_idle_peers_preserve_local_preference() {
        for peer_latencies in [None, Some([60, 150])] {
            let stats = BucketedStats::default();
            let buckets = buckets();
            snapshot(&stats, &buckets);
            stats.seed_latency(&buckets[0], 10);
            if let Some(latencies) = peer_latencies {
                for (bucket, millis) in buckets.iter().skip(1).zip(latencies) {
                    stats.seed_latency(bucket, millis);
                }
            }
            let timeline = RequestTimeline::default();
            let active = (0..3)
                .map(|_| begin(&stats, &buckets[0], None, &timeline))
                .collect::<Vec<_>>();
            advance(Duration::from_secs(1)).await;
            for observation in active {
                observation.complete(Outcome::Neutral);
            }
            advance(Duration::from_secs(60)).await;
            let (index, probe) = snapshot(&stats, &buckets).primary(Duration::from_secs(1));
            assert_eq!(index, 0);
            assert!(probe.is_none());
            assert!(!stats.entry(&buckets[0]).lock().deprioritized);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_refit_in_progress_serves_the_previous_model() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        stats.seed_latency(&bucket, 10);
        assert_eq!(
            stats.model(&bucket).first_byte_tail(4),
            Some(Duration::from_millis(10))
        );
        stats.entry(&bucket).lock().estimates.refitting = true;
        stats.seed_latency(&bucket, 20);
        advance(LATENCY_SNAPSHOT_INTERVAL).await;
        assert_eq!(
            stats.model(&bucket).first_byte_tail(4),
            Some(Duration::from_millis(10))
        );
        stats.entry(&bucket).lock().estimates.refitting = false;
        assert_eq!(
            stats.model(&bucket).first_byte_tail(4),
            Some(Duration::from_millis(20))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn zero_median_headers_still_hedge_observed_stalls() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        for _ in 0..50 {
            stats.seed(&bucket, Sample::from_millis(0, 4096, 1));
        }
        stats.seed(&bucket, Sample::from_millis(1000, 4096, 1));
        let model = stats.model(&bucket);
        assert_eq!(model.first_byte_tail(4096), Some(Duration::from_secs(1)));
        assert_eq!(
            model.first_byte_hedge_delay(4096),
            Some(Duration::from_micros(1500))
        );
    }
}
