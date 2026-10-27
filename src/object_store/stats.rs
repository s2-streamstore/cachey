mod model;
mod reservoir;

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use dashmap::DashMap;
use parking_lot::{Mutex, MutexGuard};
use tokio::{sync::SetOnce, time::Instant};

pub use self::model::LatencyEstimate;
use self::model::{Median, Refit, SizeHistogram, SizeModel, TIMER_RESOLUTION};
use crate::{
    object_store::DownloadLimits,
    types::{BucketName, BucketNameSet},
};

const ERROR_ALPHA: f64 = 0.015;
const LATENCY_ALPHA: f64 = 0.1;
/// Reads the routing factor averages; older ones would weigh under 3.5%.
const RECENT_READS: usize = 32;
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
    started: SetOnce<Instant>,
    headers: SetOnce<Headers>,
}

impl RequestTimeline {
    pub fn start(&self) -> Instant {
        let now = Instant::now();
        let _ = self.started.set(now);
        now
    }

    pub fn receive_headers(&self, body_bytes: u64) {
        let _ = self.headers.set(Headers {
            at: Instant::now(),
            body_bytes,
        });
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

    pub async fn headers_received(&self) {
        self.headers.wait().await;
    }

    /// Resolves once the request starts, or once headers arrive if it has started.
    pub async fn changed(&self) {
        if self.started().is_none() {
            self.started.wait().await;
        } else {
            self.headers.wait().await;
        }
    }

    fn first_byte(&self) -> Option<Duration> {
        Some(
            self.headers()?
                .at
                .saturating_duration_since(self.started()?),
        )
    }

    fn sample(&self, requested_bytes: u64) -> Option<Sample> {
        let headers = self.headers()?;
        Some(Sample {
            requested_bytes,
            first_byte: self.first_byte()?,
            body_bytes: headers.body_bytes,
            body: Instant::now().saturating_duration_since(headers.at),
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

    /// At least the timer resolution, as the models observe it.
    fn latency(&self) -> Duration {
        self.first_byte
            .saturating_add(self.body)
            .max(TIMER_RESOLUTION)
    }
}

/// Per-bucket latency by size. Time to first byte and complete requests are modeled by
/// requested bytes, bodies by the length announced in headers.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct LatencyModel {
    pub first_byte: SizeModel,
    pub body: SizeModel,
    pub request: SizeModel,
}

impl LatencyModel {
    /// Timeouts affect health only after allowing time for each phase reached, using
    /// `unmeasured` for unknown phases. Unless both phases are measured, the request
    /// model is a floor.
    pub fn timeout_allowance(
        &self,
        requested_bytes: u64,
        announced_body_bytes: Option<u64>,
        unmeasured: Duration,
    ) -> Duration {
        let first_byte = self.first_byte.tail(requested_bytes);
        let body = announced_body_bytes.map(|bytes| self.body.tail(bytes));
        let reached = first_byte
            .unwrap_or(unmeasured)
            .saturating_add(body.map_or(Duration::ZERO, |tail| tail.unwrap_or(unmeasured)));
        match (first_byte, body) {
            (Some(_), Some(Some(_))) => reached,
            _ => self
                .request
                .tail(requested_bytes)
                .map_or(reached, |tail| tail.max(reached)),
        }
    }
}

#[derive(Debug)]
struct LatencyEstimates {
    first_byte: SizeHistogram,
    body: SizeHistogram,
    request: SizeHistogram,
}

impl Default for LatencyEstimates {
    fn default() -> Self {
        Self {
            first_byte: SizeHistogram::new(false),
            body: SizeHistogram::new(true),
            request: SizeHistogram::new(false),
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

#[derive(Debug, Clone, Copy)]
struct RecentRead {
    bytes: u64,
    latency: Duration,
    probe: bool,
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
    active: usize,
    timed: BTreeMap<TimedCopy, usize>,
    /// Successful reads the routing factor is taken from.
    recent: VecDeque<RecentRead>,
    /// Fraction by which canceled copies outlasted the scaled median.
    routing_excess: f64,
    routing_updated_at: Instant,
    /// Routing latency from canceled copies while no median is known.
    unmeasured_latency: Duration,
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
            recent: VecDeque::with_capacity(RECENT_READS),
            routing_excess: 0.0,
            routing_updated_at: now,
            unmeasured_latency: Duration::ZERO,
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

    /// Every success eases the excess; the first measurement and recovery probes clear it.
    fn record(&mut self, now: Instant, sample: &Sample, probe: bool) {
        if probe || self.estimates.request.model().estimate().is_none() {
            self.routing_excess = 0.0;
            self.unmeasured_latency = Duration::ZERO;
        } else {
            self.routing_excess = self.routing_excess(now) * (1.0 - LATENCY_ALPHA);
        }
        self.routing_updated_at = now;
        if self.recent.len() == RECENT_READS {
            self.recent.pop_front();
        }
        self.recent.push_back(RecentRead {
            bytes: sample.requested_bytes,
            latency: sample.latency(),
            probe,
        });
        self.estimates.record(now, sample);
    }

    /// Moving average of recent reads' latency over the request model's current median,
    /// which tracks a shift in latency faster than the model; taken against the current
    /// median, it never counts a shift the model has absorbed. Only sizes the model has
    /// observed count, since an extrapolated median can be far off. A recovery probe lowers
    /// the average at once, restoring the bucket's preference, but raises it only as any
    /// read does: one sample at a poorly fit size would otherwise make every size look slow,
    /// and a bucket that looks slow gets no reads to correct it.
    fn routing_factor(&self) -> f64 {
        let model = self.estimates.request.model();
        self.recent.iter().fold(1.0, |factor, read| {
            let ratio = match model.median(read.bytes) {
                Some(Median::Observed(median)) => Some(read.latency.div_duration_f64(median)),
                _ => None,
            };
            match ratio {
                _ if read.probe && ratio.unwrap_or(1.0) < factor => ratio.unwrap_or(1.0),
                Some(ratio) => factor * (1.0 - LATENCY_ALPHA) + ratio * LATENCY_ALPHA,
                None => factor,
            }
        })
    }

    /// Routing latency from a median, or from canceled copies while unmeasured.
    fn routing_latency(&self, now: Instant, median: Option<Duration>) -> Duration {
        median.map_or(self.unmeasured_latency, |median| {
            median.mul_f64(self.routing_factor() * (1.0 + self.routing_excess(now)))
        })
    }

    fn succeed(&mut self, now: Instant, generation: u64, probe: bool) {
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

    /// An unfinished copy raises routing latency toward its elapsed time, and never lowers
    /// it. Relative to the scaled median, the rise carries over to other sizes. A measured
    /// bucket's copy of a size its model has not observed is not judged.
    fn raise_routing_latency(&mut self, now: Instant, elapsed: Duration, requested_bytes: u64) {
        match self.estimates.request.model().median(requested_bytes) {
            Some(Median::Observed(median)) => {
                let excess = self.routing_excess(now);
                let outlasted = elapsed.div_duration_f64(median) / self.routing_factor() - 1.0;
                self.routing_excess = excess + (outlasted - excess).max(0.0) * LATENCY_ALPHA;
                self.routing_updated_at = now;
            }
            Some(Median::Unobserved(_)) => {}
            None => {
                self.unmeasured_latency += elapsed
                    .saturating_sub(self.unmeasured_latency)
                    .mul_f64(LATENCY_ALPHA);
            }
        }
    }

    /// The excess decays even while the bucket is idle.
    fn routing_excess(&self, now: Instant) -> f64 {
        self.routing_excess
            * decay(
                ROUTING_DECAY,
                now.saturating_duration_since(self.routing_updated_at),
            )
    }

    /// Three overdue copies count as a stall lasting as long as the third.
    fn stalled_latency(&self, now: Instant, current: Duration) -> Duration {
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

    fn track(&mut self, copy: Option<TimedCopy>) {
        if let Some(copy) = copy {
            *self.timed.entry(copy).or_default() += 1;
        }
    }

    fn untrack(&mut self, copy: Option<TimedCopy>) {
        if let Some(copy) = copy
            && let Some(count) = self.timed.get_mut(&copy)
        {
            *count -= 1;
            if *count == 0 {
                self.timed.remove(&copy);
            }
        }
    }
}

/// Refits once per interval if `interval` allows, or sooner while observation counts
/// double. Fitting runs outside the lock so it does not delay the bucket's copies;
/// claiming the refit under the lock leaves concurrent callers nothing to refit.
fn refit(stats: &mut MutexGuard<'_, BucketStats>, now: Instant, interval: bool) {
    let due = interval && now.duration_since(stats.summarized_at) >= LATENCY_SNAPSHOT_INTERVAL;
    if due {
        stats.summarized_at = now;
    }
    let refits: [Option<Refit>; 3] = stats
        .estimates
        .histograms()
        .map(|histogram| histogram.refit(due));
    if refits.iter().all(Option::is_none) {
        return;
    }
    let fits = MutexGuard::unlocked(stats, || refits.map(|refit| refit.map(Refit::fit)));
    for (histogram, fitted) in stats.estimates.histograms().into_iter().zip(fits) {
        if let Some(fitted) = fitted {
            histogram.store(fitted);
        }
    }
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
    /// Moves the copy's stall threshold to four times a new expectation.
    pub fn expect(&mut self, expected: Duration) {
        let mut stats = self.stats.lock();
        stats.untrack(self.timed);
        self.timed = timed(self.started, expected);
        stats.track(self.timed);
    }

    pub fn complete(mut self, outcome: Outcome) {
        self.outcome = Some(outcome);
    }
}

/// A copy becomes overdue after four times its expected latency.
fn timed(started: Instant, expected: Duration) -> Option<TimedCopy> {
    let overdue = expected.saturating_mul(4).max(Duration::from_millis(1));
    Some((started.checked_add(overdue)?, started))
}

impl Drop for BucketObservation<'_> {
    fn drop(&mut self) {
        let mut stats = self.stats.lock();
        let now = Instant::now();
        stats.active -= 1;
        stats.untrack(self.timed);
        let probe = self.probe.is_some();
        // A response longer than expected shows the request was at least that large.
        let requested_bytes = self
            .requested_bytes
            .max(self.timeline.announced_body_bytes().unwrap_or(0));
        match self.outcome {
            Some(Outcome::Success) => {
                if let Some(sample) = self.timeline.sample(requested_bytes) {
                    stats.record(now, &sample, probe);
                }
                stats.succeed(now, self.generation, probe);
            }
            // An unvalidated copy still measures any headers it received.
            _ => {
                if let Some(first_byte) = self.timeline.first_byte() {
                    stats
                        .estimates
                        .first_byte
                        .update(now, requested_bytes, first_byte);
                }
            }
        }
        match self.outcome {
            None => {
                stats.raise_routing_latency(now, now.duration_since(self.started), requested_bytes);
            }
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
    /// Median latency of a request for the routed size, or a lower bound beyond the
    /// model's reach.
    median: Option<Duration>,
    /// p99 of a request for the routed size.
    tail: Option<Duration>,
    latency: Duration,
    deprioritized: bool,
    error_rate: f64,
    overloaded: bool,
}

/// Bucket state for routing one request size.
pub(super) struct RoutingSnapshot {
    buckets: Vec<BucketSnapshot>,
    limits: DownloadLimits,
}

impl RoutingSnapshot {
    fn compare(&self, left: usize, right: usize, remaining: Duration) -> std::cmp::Ordering {
        let left_stats = &self.buckets[left];
        let right_stats = &self.buckets[right];
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

    /// Whether a new copy is expected to finish in the time remaining, within the bucket
    /// timeout; unmeasured buckets may.
    pub fn feasible(&self, index: usize, remaining: Duration) -> bool {
        self.tail(index)
            .is_none_or(|tail| tail < remaining.min(self.limits.bucket_timeout))
    }

    pub fn best(&self, eligible: impl Fn(usize) -> bool, remaining: Duration) -> Option<usize> {
        (0..self.buckets.len())
            .filter(|index| eligible(*index))
            .min_by(|left, right| self.compare(*left, *right, remaining))
    }

    pub fn rescue_schedule(&self, primary: usize, probing: bool) -> VecDeque<(Duration, usize)> {
        let limits = self.limits;
        let page_budget = limits.page_timeout;
        let unknown_latency = limits.bucket_share(self.buckets.len());
        let reserve_budget = page_budget / 3 * 2;
        let mut schedule = Vec::with_capacity(self.buckets.len());
        schedule.extend((0..self.buckets.len()).filter_map(|index| {
            if index == primary || !self.feasible(index, page_budget) {
                return None;
            }
            let reserve = self
                .tail(index)
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
        let overloaded = self.buckets.iter().any(|bucket| bucket.overloaded);
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
        let best = &self.buckets[winner];
        if best.deprioritized || !self.feasible(winner, remaining) {
            return (winner, None);
        }
        let now = Instant::now();
        for (index, snapshot) in self.buckets.iter().enumerate() {
            if index == winner {
                continue;
            }
            let mut stats = snapshot.stats.lock();
            let stale = snapshot.median.is_some()
                && now.duration_since(stats.last_outcome) >= RECOVERY_INTERVAL;
            // The first bucket is always worth rechecking; others only if they may win.
            if (stats.deprioritized || stale)
                && !stats.probing
                && stats.active == 0
                && now >= stats.probe_after
                && (index == 0
                    || locality_adjusted(snapshot.median.unwrap_or_default(), index) < best.latency)
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
        self.buckets[index].tail
    }

    pub fn model(&self, index: usize) -> LatencyModel {
        self.buckets[index].model
    }

    pub fn overloaded(&self, index: usize) -> bool {
        self.buckets[index].overloaded
    }

    pub fn all_overloaded(&self) -> bool {
        self.buckets.iter().all(|bucket| bucket.overloaded)
    }
}

impl BucketedStats {
    fn entry(&self, bucket: &BucketName) -> Arc<Mutex<BucketStats>> {
        if let Some(stats) = self.by_bucket.get(bucket) {
            return stats.clone();
        }
        self.by_bucket.entry(bucket.clone()).or_default().clone()
    }

    pub(super) fn begin<'a>(
        &self,
        bucket: &BucketName,
        probe: Option<ProbePermit>,
        timeline: &'a RequestTimeline,
        requested_bytes: u64,
        expected: Duration,
    ) -> BucketObservation<'a> {
        let stats = self.entry(bucket);
        let started = Instant::now();
        let timed = timed(started, expected);
        let generation = {
            let mut stats = stats.lock();
            if probe.is_some() {
                stats.probe_after = started + recovery_delay();
            }
            stats.active += 1;
            stats.track(timed);
            stats.generation
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
        limits: DownloadLimits,
    ) -> RoutingSnapshot {
        let now = Instant::now();
        let states: Vec<_> = buckets.iter().map(|bucket| self.entry(bucket)).collect();
        // Only the bucket longest without one may refit on the interval, so a snapshot pays
        // for at most one such fit and every bucket gets its turn.
        let oldest = states
            .iter()
            .enumerate()
            .min_by_key(|(_, state)| state.lock().summarized_at)
            .map(|(index, _)| index);
        let mut preferred_latency = Duration::ZERO;
        RoutingSnapshot {
            buckets: states
                .into_iter()
                .enumerate()
                .map(|(index, state)| {
                    let mut stats = state.lock();
                    refit(&mut stats, now, Some(index) == oldest);
                    let model = stats.estimates.model();
                    let median = model.request.median(requested_bytes);
                    let current = stats.routing_latency(now, median.map(Median::value));
                    if index == 0 {
                        preferred_latency = current;
                    }
                    let mut latency = stats.stalled_latency(now, current);
                    if index > 0 && !matches!(median, Some(Median::Observed(_))) {
                        latency = latency.max(preferred_latency);
                    }
                    BucketSnapshot {
                        stats: state.clone(),
                        model,
                        median: median.map(Median::value),
                        tail: model.request.tail(requested_bytes),
                        latency: locality_adjusted(latency, index),
                        deprioritized: stats.deprioritized,
                        error_rate: stats.error_rate(now),
                        overloaded: now < stats.overload_until,
                    }
                })
                .collect(),
            limits,
        }
    }

    #[cfg(test)]
    pub(super) fn seed(&self, bucket: &BucketName, sample: Sample) {
        let stats = self.entry(bucket);
        let mut stats = stats.lock();
        let now = Instant::now();
        let generation = stats.generation;
        stats.record(now, &sample, false);
        stats.succeed(now, generation, false);
    }

    /// Seeds a request for the 4-byte objects tests read, whose latency is all time to first byte.
    #[cfg(test)]
    pub(super) fn seed_latency(&self, bucket: &BucketName, millis: u64) {
        self.seed(bucket, Sample::from_millis(millis, 4, 0));
    }

    #[cfg(test)]
    pub(super) fn model(&self, bucket: &BucketName) -> LatencyModel {
        let stats = self.entry(bucket);
        let mut stats = stats.lock();
        refit(&mut stats, Instant::now(), true);
        stats.estimates.model()
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
                let stats = entry.value().lock();
                let model = stats.estimates.model();
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

    use bytesize::{KIB, MIB};
    use parking_lot::Mutex;
    use proptest::{
        collection, prop_assert, prop_oneof, proptest,
        strategy::Strategy,
        test_runner::{Config as ProptestConfig, TestCaseError},
    };
    use tokio::time::{Instant, advance};

    use super::{
        BucketObservation, BucketStats, BucketedStats, LATENCY_SNAPSHOT_INTERVAL, Median,
        OBSERVATION_SPACING, Outcome, ProbePermit, RECOVERY_INTERVAL, RECOVERY_SUCCESSES,
        RequestTimeline, RoutingSnapshot, Sample,
    };
    use crate::{
        object_store::DownloadLimits,
        types::{BucketName, BucketNameSet},
    };

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
        begin_sized(stats, bucket, probe, timeline, 4)
    }

    fn begin_sized<'a>(
        stats: &BucketedStats,
        bucket: &BucketName,
        probe: Option<ProbePermit>,
        timeline: &'a RequestTimeline,
        bytes: u64,
    ) -> BucketObservation<'a> {
        let expected = stats
            .model(bucket)
            .timeout_allowance(bytes, None, Duration::from_secs(1));
        stats.begin(bucket, probe, timeline, bytes, expected)
    }

    fn snapshot(stats: &BucketedStats, buckets: &BucketNameSet) -> RoutingSnapshot {
        stats.snapshot(buckets, 4, DownloadLimits::default())
    }

    fn best(stats: &BucketedStats, buckets: &BucketNameSet, remaining: Duration) -> Option<usize> {
        snapshot(stats, buckets).best(|_| true, remaining)
    }

    fn best_at(stats: &BucketedStats, buckets: &BucketNameSet, bytes: u64) -> Option<usize> {
        stats
            .snapshot(buckets, bytes, DownloadLimits::default())
            .best(|_| true, Duration::from_secs(10))
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
            let routing = stats.snapshot(&buckets, bytes, DownloadLimits::default());
            let predicted = routing.tail(0).unwrap();
            assert!(
                predicted.abs_diff(Duration::from_millis(tail)) < Duration::from_millis(1),
                "{bytes}: {predicted:?}"
            );
            assert_eq!(routing.feasible(0, remaining), feasible);
        }
    }

    /// With one snapshot per interval, each bucket's turn to refit still comes.
    #[tokio::test(start_paused = true)]
    async fn sparse_snapshots_refit_every_bucket_in_turn() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for bucket in buckets.iter() {
            for _ in 0..8 {
                stats.seed_latency(bucket, 10);
            }
        }
        snapshot(&stats, &buckets);
        let mut routing = None;
        for _ in 0..buckets.len() {
            advance(LATENCY_SNAPSHOT_INTERVAL).await;
            for bucket in buckets.iter() {
                stats.seed_latency(bucket, 30);
            }
            routing = Some(snapshot(&stats, &buckets));
        }
        let routing = routing.unwrap();
        for index in 0..buckets.len() {
            assert_eq!(
                routing.tail(index),
                Some(Duration::from_millis(30)),
                "{index}"
            );
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

    /// Before a bucket has a median, canceled copies raise its routing latency directly, or
    /// a bucket that never completes a read would keep looking instant.
    #[tokio::test(start_paused = true)]
    async fn canceled_copies_slow_a_bucket_with_no_measurements() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for bucket in buckets.iter().skip(1) {
            stats.seed_latency(bucket, 100);
        }
        assert_eq!(best(&stats, &buckets, Duration::from_secs(10)), Some(0));
        let timeline = RequestTimeline::default();
        for _ in 0..10 {
            let canceled = begin(&stats, &buckets[0], None, &timeline);
            advance(Duration::from_millis(500)).await;
            drop(canceled);
        }
        assert_eq!(best(&stats, &buckets, Duration::from_secs(10)), Some(1));
    }

    /// A slow recovery probe raises routing latency only as any read does, since a bucket
    /// that looks slow gets no reads to correct it.
    #[tokio::test(start_paused = true)]
    async fn a_slow_probe_does_not_send_reads_away_at_once() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for (bucket, millis) in buckets.iter().zip([10, 30, 60]) {
            for _ in 0..4 {
                stats.seed_latency(bucket, millis);
            }
        }
        assert_eq!(best(&stats, &buckets, Duration::from_secs(1)), Some(0));
        stats.entry(&buckets[0]).lock().record(
            Instant::now(),
            &Sample::from_millis(100, 4, 0),
            true,
        );
        assert_eq!(best(&stats, &buckets, Duration::from_secs(1)), Some(0));
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
            let latency = snapshot(&stats, &buckets).buckets[0].latency;
            assert_eq!(
                latency.abs_diff(Duration::from_millis(3)) < Duration::from_micros(1),
                probe_succeeds,
                "{latency:?}"
            );
            assert_eq!(
                stats.model(&buckets[0]).body.tail(4 * MIB),
                Some(Duration::from_millis(250))
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_slowdown_moves_routing_before_the_model_refits() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for (bucket, millis) in buckets.iter().zip([3, 5, 6]) {
            for _ in 0..16 {
                stats.seed_latency(bucket, millis);
            }
        }
        assert_eq!(best(&stats, &buckets, Duration::from_secs(1)), Some(0));
        for _ in 0..5 {
            stats.seed_latency(&buckets[0], 30);
        }
        assert_eq!(best(&stats, &buckets, Duration::from_secs(1)), Some(1));
    }

    /// A run of large reads on the local bucket does not send its small reads elsewhere.
    #[tokio::test(start_paused = true)]
    async fn routing_compares_latency_at_the_read_size() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for _ in 0..4 {
            stats.seed(&buckets[0], Sample::from_millis(5, 4096, 1));
            stats.seed(&buckets[0], Sample::from_millis(5, 8 * MIB, 95));
        }
        for bucket in buckets.iter().skip(1) {
            stats.seed(bucket, Sample::from_millis(19, 4096, 1));
        }
        assert_eq!(best_at(&stats, &buckets, 4096), Some(0));
    }

    /// Copies of a size beyond a bucket's history neither slow its routing at known sizes
    /// nor make it look fast at the new size.
    #[tokio::test(start_paused = true)]
    async fn sizes_a_bucket_cannot_predict_do_not_steer_routing() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for _ in 0..20 {
            stats.seed(&buckets[0], Sample::from_millis(5, 4096, 0));
            stats.seed(&buckets[1], Sample::from_millis(19, 4096, 0));
        }
        assert_eq!(best_at(&stats, &buckets, 4096), Some(0));
        let timeline = RequestTimeline::default();
        let canceled = begin_sized(&stats, &buckets[0], None, &timeline, 8 * MIB);
        advance(Duration::from_millis(300)).await;
        drop(canceled);
        assert_eq!(best_at(&stats, &buckets, 4096), Some(0));
        for _ in 0..6 {
            stats.seed(&buckets[0], Sample::from_millis(5, 8 * MIB, 95));
        }
        assert_eq!(best_at(&stats, &buckets, 4096), Some(0));
        advance(LATENCY_SNAPSHOT_INTERVAL).await;
        stats.model(&buckets[0]);
        assert_eq!(best_at(&stats, &buckets, 8 * MIB), Some(0));
    }

    #[tokio::test(start_paused = true)]
    async fn beyond_its_history_a_bucket_is_no_faster_than_at_its_largest_size() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for _ in 0..4 {
            stats.seed(&buckets[0], Sample::from_millis(300, 4096, 0));
            stats.seed(&buckets[1], Sample::from_millis(10, 8 * MIB, 50));
        }
        assert_eq!(best_at(&stats, &buckets, 8 * MIB), Some(1));
    }

    /// Once the median catches up to a shift, the routing factor no longer counts it.
    #[tokio::test(start_paused = true)]
    async fn a_refit_does_not_count_a_shift_twice() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for (bucket, millis) in buckets.iter().zip([10, 40, 50]) {
            for _ in 0..16 {
                stats.seed_latency(bucket, millis);
            }
        }
        assert_eq!(best(&stats, &buckets, Duration::from_secs(1)), Some(0));
        for _ in 0..40 {
            stats.seed_latency(&buckets[0], 30);
        }
        stats.model(&buckets[0]);
        assert_eq!(best(&stats, &buckets, Duration::from_secs(1)), Some(0));
    }

    /// A refit that learns how size affects latency explains the factor's deviation rather
    /// than inflating it to keep a stale prediction.
    #[tokio::test(start_paused = true)]
    async fn a_refit_learning_size_does_not_inflate_the_factor() {
        for slow_large_reads in [0, 3] {
            let stats = BucketedStats::default();
            let buckets = buckets();
            stats.seed(&buckets[0], Sample::from_millis(200, 16 * MIB, 0));
            best_at(&stats, &buckets, 4096);
            for _ in 0..slow_large_reads {
                stats.seed(&buckets[0], Sample::from_millis(400, 16 * MIB, 0));
            }
            stats.seed(&buckets[0], Sample::from_millis(5, 4096, 0));
            advance(LATENCY_SNAPSHOT_INTERVAL).await;
            let routing = stats.snapshot(&buckets, 4096, DownloadLimits::default());
            assert!(
                routing.buckets[0].latency < Duration::from_millis(20),
                "{slow_large_reads}: {:?}",
                routing.buckets[0].latency
            );
        }
    }

    /// A canceled copy raises routing latency in proportion at other sizes, rather than by
    /// its whole overrun.
    #[tokio::test(start_paused = true)]
    async fn a_canceled_large_copy_does_not_send_small_reads_away() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for _ in 0..4 {
            stats.seed(&buckets[0], Sample::from_millis(5, 4096, 0));
            stats.seed(&buckets[0], Sample::from_millis(5, 16 * MIB, 195));
        }
        for bucket in buckets.iter().skip(1) {
            stats.seed(bucket, Sample::from_millis(15, 4096, 0));
        }
        assert_eq!(best_at(&stats, &buckets, 4096), Some(0));
        let timeline = RequestTimeline::default();
        let canceled = begin_sized(&stats, &buckets[0], None, &timeline, 16 * MIB);
        advance(Duration::from_millis(600)).await;
        drop(canceled);
        assert_eq!(best_at(&stats, &buckets, 4096), Some(0));
    }

    #[tokio::test(start_paused = true)]
    async fn an_instant_probe_keeps_routing_latency_finite() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        stats.seed(&buckets[0], Sample::from_millis(5, 4096, 0));
        best_at(&stats, &buckets, 4096);
        stats.entry(&buckets[0]).lock().record(
            Instant::now(),
            &Sample::from_millis(0, 4096, 0),
            true,
        );
        let timeline = RequestTimeline::default();
        let canceled = begin_sized(&stats, &buckets[0], None, &timeline, 4096);
        advance(Duration::from_millis(50)).await;
        drop(canceled);
        best_at(&stats, &buckets, 4096);
    }

    /// A first bucket last measured as slow is still probed once stale, or it could never
    /// win its reads back after recovering.
    #[tokio::test(start_paused = true)]
    async fn a_stale_first_bucket_is_probed_however_slow_it_was() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for (bucket, millis) in buckets.iter().zip([60, 20, 25]) {
            stats.seed_latency(bucket, millis);
        }
        advance(RECOVERY_INTERVAL).await;
        let (index, probe) = snapshot(&stats, &buckets).primary(Duration::from_secs(1));
        assert_eq!(index, 0);
        assert!(probe.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn a_copys_stall_threshold_follows_its_announced_length() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for (bucket, millis) in buckets.iter().zip([3, 5, 6]) {
            stats.seed_latency(bucket, millis);
        }
        let timeline = RequestTimeline::default();
        let mut active = (0..3)
            .map(|_| begin(&stats, &buckets[0], None, &timeline))
            .collect::<Vec<_>>();
        for observation in &mut active {
            observation.expect(Duration::from_millis(100));
        }
        advance(Duration::from_millis(20)).await;
        assert_eq!(best(&stats, &buckets, Duration::from_secs(1)), Some(0));
    }

    #[tokio::test(start_paused = true)]
    async fn metrics_callbacks_run_without_holding_statistics_locks() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for bucket in buckets.iter() {
            stats.seed_latency(bucket, 3);
            stats.model(bucket);
        }
        let mut exported = 0;
        stats.export_bucket_metrics(|bucket, metrics| {
            assert_eq!(
                metrics.request.map(|estimate| estimate.base),
                Some(Duration::from_millis(3))
            );
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
        assert_eq!(model.body.tail(4096), Some(Duration::from_millis(1)));
        assert_eq!(model.body.tail(page), None);
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
                .body
                .tail(page)
                .unwrap()
                .abs_diff(Duration::from_millis(1001))
                <= Duration::from_millis(1)
        );
        assert_eq!(model.request.tail(page), Some(Duration::from_millis(271)));
    }

    #[tokio::test(start_paused = true)]
    async fn unfinished_copies_measure_only_headers_they_received() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        stats.seed(&bucket, Sample::from_millis(10, 4, 1));
        for (received_headers, outcome) in
            [(false, None), (true, None), (true, Some(Outcome::Failure))]
        {
            let timeline = RequestTimeline::default();
            timeline.start();
            let observation = begin(&stats, &bucket, None, &timeline);
            if received_headers {
                advance(Duration::from_millis(30)).await;
                timeline.receive_headers(4);
            }
            advance(Duration::from_millis(500)).await;
            match outcome {
                Some(outcome) => observation.complete(outcome),
                None => drop(observation),
            }
        }
        advance(LATENCY_SNAPSHOT_INTERVAL).await;
        let model = stats.model(&bucket);
        assert_eq!(model.first_byte.tail(4), Some(Duration::from_millis(30)));
        assert_eq!(model.body.tail(4), Some(Duration::from_millis(1)));
        assert_eq!(model.request.tail(4), Some(Duration::from_millis(11)));
    }

    /// One or two slow copies may be unlucky; three overdue copies are a stall.
    #[tokio::test(start_paused = true)]
    async fn a_stall_takes_three_overdue_copies() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for (bucket, millis) in buckets.iter().zip([3, 5, 6]) {
            stats.seed_latency(bucket, millis);
        }
        let timeline = RequestTimeline::default();
        let mut active = Vec::new();
        for stalled in [false, false, true] {
            active.push(begin(&stats, &buckets[0], None, &timeline));
            advance(Duration::from_millis(20)).await;
            assert_eq!(
                best(&stats, &buckets, Duration::from_secs(1)),
                Some(usize::from(stalled))
            );
        }
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
                .map(|_| begin_sized(&stats, &buckets[0], None, &timeline, requested))
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
            .map(|_| begin_sized(&stats, &buckets[0], None, &timeline, 16 * MIB))
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

    /// Sizes in distinct bands, from small reads to whole pages.
    const ROUTED_SIZES: [u64; 4] = [4 * KIB, 64 * KIB, MIB, 16 * MIB];
    /// Speed shifts span 4x and jitter 1.56x, so a size observed at one speed can route up
    /// to 6.25x from its observations once other sizes see another.
    const ROUTING_SLACK: f64 = 8.0;
    /// A size is judged once a fit includes this many of its observations, since a line fit
    /// to fewer can trade them for other sizes' larger deviations.
    const JUDGED_OBSERVATIONS: usize = 3;

    #[derive(Debug, Clone)]
    enum Finish {
        Success { jitter_percent: u32, probe: bool },
        Cancel { elapsed_percent: u32 },
    }

    #[derive(Debug, Clone)]
    enum Event {
        Copy {
            size: usize,
            finish: Finish,
        },
        /// Every size slows down or speeds up by the same factor.
        Shift {
            percent: u32,
        },
        Idle {
            secs: u64,
        },
    }

    fn event() -> impl Strategy<Value = Event> {
        let finish = prop_oneof![
            9 => (80_u32..=125, proptest::bool::weighted(0.1))
                .prop_map(|(jitter_percent, probe)| Finish::Success { jitter_percent, probe }),
            2 => (25_u32..=800).prop_map(|elapsed_percent| Finish::Cancel { elapsed_percent }),
        ];
        prop_oneof![
            11 => (0..ROUTED_SIZES.len(), finish)
                .prop_map(|(size, finish)| Event::Copy { size, finish }),
            1 => (50_u32..=200).prop_map(|percent| Event::Shift { percent }),
            1 => (1_u64..=60).prop_map(|secs| Event::Idle { secs }),
        ]
    }

    /// A bucket whose latency is linear in size, scaled by its current speed.
    struct Backend {
        fixed_ms: f64,
        ms_per_mib: f64,
        speed: f64,
    }

    impl Backend {
        fn latency(&self, size: usize, percent: u32) -> Duration {
            let ms = self.fixed_ms + self.ms_per_mib * ROUTED_SIZES[size] as f64 / MIB as f64;
            Duration::from_secs_f64(ms * self.speed * f64::from(percent) / 100_000.0)
        }
    }

    /// Routes a read as a download does, which is when models refit.
    fn route(stats: &BucketedStats, buckets: &BucketNameSet, size: usize) -> Duration {
        stats
            .snapshot(buckets, ROUTED_SIZES[size], DownloadLimits::default())
            .buckets[0]
            .latency
    }

    fn record_success(stats: &Mutex<BucketStats>, bytes: u64, latency: Duration, probe: bool) {
        let mut stats = stats.lock();
        let now = Instant::now();
        let generation = stats.generation;
        let sample = Sample {
            requested_bytes: bytes,
            first_byte: latency,
            body_bytes: bytes,
            body: Duration::ZERO,
        };
        stats.record(now, &sample, probe);
        stats.succeed(now, generation, probe);
    }

    async fn follow_latency(
        mut backend: Backend,
        events: Vec<(u64, Event)>,
    ) -> Result<(), TestCaseError> {
        let stats = BucketedStats::default();
        let buckets = BucketNameSet::from(BucketName::new("bucket").unwrap());
        let entry = stats.entry(&buckets[0]);
        let mut successes = 0;
        // The range of latencies observed at each size, and which successes observed it.
        let mut observed: [(Duration, Duration, Vec<u64>); ROUTED_SIZES.len()] =
            std::array::from_fn(|_| (Duration::MAX, Duration::ZERO, Vec::new()));
        for (gap_ms, event) in events {
            advance(Duration::from_millis(gap_ms)).await;
            let (size, finish) = match event {
                Event::Copy { size, finish } => (size, finish),
                Event::Shift { percent } => {
                    backend.speed = f64::from(percent) / 100.0;
                    continue;
                }
                Event::Idle { secs } => {
                    advance(Duration::from_secs(secs)).await;
                    continue;
                }
            };
            let latency = route(&stats, &buckets, size);
            {
                let stats = entry.lock();
                // The factor averages recent reads against the current model, starting at 1.
                let model = stats.estimates.request.model();
                let ratios = stats
                    .recent
                    .iter()
                    .filter_map(|read| match model.median(read.bytes) {
                        Some(Median::Observed(median)) => {
                            Some(read.latency.div_duration_f64(median))
                        }
                        _ => None,
                    })
                    .fold((1.0_f64, 1.0_f64), |(low, high), ratio| {
                        (low.min(ratio), high.max(ratio))
                    });
                let factor = stats.routing_factor();
                prop_assert!(
                    ratios.0 - 1e-9 <= factor && factor <= ratios.1 + 1e-9,
                    "factor {factor} outside {ratios:?}"
                );
                let (low, high, seen) = &observed[size];
                let judged = seen
                    .get(JUDGED_OBSERVATIONS - 1)
                    .is_some_and(|&nth| stats.estimates.request.fitted_updates() >= nth);
                let scaled = latency.div_f64(1.0 + stats.routing_excess(Instant::now()));
                prop_assert!(
                    !judged
                        || (low.div_f64(ROUTING_SLACK) <= scaled
                            && scaled <= high.mul_f64(ROUTING_SLACK)),
                    "{scaled:?} routed where {low:?}..={high:?} was observed"
                );
            }
            match finish {
                Finish::Success {
                    jitter_percent,
                    probe,
                } => {
                    let latency = backend.latency(size, jitter_percent);
                    successes += 1;
                    let (low, high, seen) = &mut observed[size];
                    *low = (*low).min(latency);
                    *high = (*high).max(latency);
                    seen.push(successes);
                    record_success(&entry, ROUTED_SIZES[size], latency, probe);
                }
                Finish::Cancel { elapsed_percent } => {
                    let timeline = RequestTimeline::default();
                    let copy =
                        begin_sized(&stats, &buckets[0], None, &timeline, ROUTED_SIZES[size]);
                    advance(backend.latency(size, elapsed_percent)).await;
                    drop(copy);
                }
            }
        }
        // Once the bucket settles, routing latency converges on its latency at each size.
        for _ in 0..50 {
            for (size, &bytes) in ROUTED_SIZES.iter().enumerate() {
                advance(OBSERVATION_SPACING).await;
                route(&stats, &buckets, size);
                let latency = backend.latency(size, 100);
                record_success(&entry, bytes, latency, false);
            }
        }
        advance(Duration::from_secs(60)).await;
        for size in 0..ROUTED_SIZES.len() {
            let routed = route(&stats, &buckets, size);
            let latency = backend.latency(size, 100);
            prop_assert!(
                latency.mul_f64(0.8) <= routed && routed <= latency.mul_f64(1.25),
                "{routed:?} routed for a settled {latency:?}"
            );
        }
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// The routing factor stays within the ratios of recent reads to the current model,
        /// routing latency stays near what each size has observed, and both settle once the
        /// bucket does.
        #[test]
        fn routing_latency_follows_observed_latency(
            fixed_ms in 3.0_f64..50.0,
            ms_per_mib in 0.0_f64..20.0,
            events in collection::vec((20_u64..=150, event()), 1..200),
        ) {
            let backend = Backend { fixed_ms, ms_per_mib, speed: 1.0 };
            tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .unwrap()
                .block_on(follow_latency(backend, events))?;
        }
    }

    /// Histories the property test found, each of which routed a size 8-50x from what it
    /// had observed.
    #[tokio::test(start_paused = true)]
    async fn routing_latency_follows_observed_latency_in_found_histories() {
        let success = |size, jitter_percent, probe| {
            let finish = Finish::Success {
                jitter_percent,
                probe,
            };
            (20, Event::Copy { size, finish })
        };
        let read = |size, jitter_percent| success(size, jitter_percent, false);
        let probe = |size, jitter_percent| success(size, jitter_percent, true);
        let shift = |percent| (20, Event::Shift { percent });
        let cancel = |size, elapsed_percent| {
            let finish = Finish::Cancel { elapsed_percent };
            (20, Event::Copy { size, finish })
        };
        for (fixed_ms, ms_per_mib, events) in [
            // A slope fit to noise extrapolated 16 MiB reads far beyond the largest size.
            (
                28.3,
                0.0,
                vec![
                    read(0, 80),
                    read(0, 80),
                    shift(122),
                    read(0, 80),
                    read(1, 80),
                    probe(3, 80),
                    read(0, 80),
                ],
            ),
            // A line fit to one size predicted smaller ones.
            (
                3.0,
                5.35,
                vec![
                    read(3, 80),
                    read(3, 80),
                    read(3, 80),
                    read(3, 80),
                    probe(0, 80),
                    read(3, 80),
                ],
            ),
            // A probe at a poorly fit size set the factor for every size.
            (
                6.17,
                19.89,
                vec![
                    read(2, 80),
                    read(2, 80),
                    read(0, 80),
                    read(2, 80),
                    read(2, 80),
                    shift(109),
                    read(0, 80),
                    read(0, 80),
                    read(3, 114),
                    probe(0, 98),
                    read(2, 80),
                ],
            ),
            // Reads at two speeds fit no line, and the plain fit dropped the fixed cost.
            (
                20.48,
                19.53,
                vec![
                    read(0, 80),
                    cancel(0, 84),
                    cancel(1, 538),
                    read(3, 110),
                    cancel(1, 146),
                    read(0, 80),
                    shift(50),
                    read(2, 80),
                    cancel(2, 792),
                    read(2, 80),
                    read(2, 80),
                    read(2, 80),
                    read(0, 80),
                    read(0, 80),
                ],
            ),
        ] {
            let backend = Backend {
                fixed_ms,
                ms_per_mib,
                speed: 1.0,
            };
            follow_latency(backend, events).await.unwrap();
        }
    }
}
