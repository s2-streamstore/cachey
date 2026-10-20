use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use bytesize::MIB;
use dashmap::DashMap;
use exponential_decay_histogram::{ExponentialDecayHistogram, Snapshot};
use parking_lot::Mutex;
use tokio::time::Instant;

use crate::{
    object_store::DownloadLimits,
    types::{BucketName, BucketNameSet},
};

const ERROR_ALPHA: f64 = 0.015;
const LATENCY_ALPHA: f64 = 0.1;
/// Latency estimates decay with a 5-second half-life.
const LATENCY_DECAY: f64 = std::f64::consts::LN_2 / 5.0;
const LATENCY_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(1);
const RECOVERY_INTERVAL: Duration = Duration::from_secs(30);
const RECOVERY_SUCCESSES: u32 = 20;
const OVERLOAD_INTERVAL: Duration = Duration::from_secs(1);
/// Body tails are tracked separately for shorter and longer bodies, so that a stall dominating a
/// small body's time does not scale up the tail of large transfers.
const LARGE_BODY_BYTES: u64 = MIB;
/// Hedge timers fire on millisecond ticks, so body times are compared relative to at least this.
const MIN_BODY_SCALE: Duration = Duration::from_millis(1);
/// The weighted variance of body lengths, relative to their mean square, needed to tell fixed body
/// time from transfer time.
const MIN_BODY_LENGTH_SPREAD: f64 = 0.01;
/// Ratios are stored in histograms as parts per million.
const RATIO_UNITS: f64 = 1e6;

#[derive(Debug, Clone)]
pub struct BucketMetrics {
    pub error_rate: f64,
    pub deprioritized: bool,
    pub consecutive_failures: u32,
    pub recovery_successes: u32,
    /// Time from starting a request to its validated body, including SDK retries, or `None` before
    /// one is measured, as for each estimate below.
    pub latency_mean: Option<Duration>,
    pub latency_p99: Option<Duration>,
    /// Time from starting a request to receiving successful response headers, including SDK
    /// retries.
    pub first_byte_mean: Option<Duration>,
    pub first_byte_p99: Option<Duration>,
    /// Fitted time to read a body after its headers regardless of its length. Until bodies of
    /// different lengths tell it apart from transfer time, it is their whole time if they are
    /// smaller than `LARGE_BODY_BYTES`, and zero otherwise.
    pub body_overhead: Option<Duration>,
    /// Fitted body bytes per second beyond the overhead, or `None` while the fit has no time per
    /// byte: bodies arrive too fast to time, as when already buffered with their headers, or all
    /// share nearly one length under `LARGE_BODY_BYTES`.
    pub body_throughput: Option<f64>,
    /// p99 of body time relative to the fitted time for its length, for bodies smaller than
    /// `LARGE_BODY_BYTES` and for the rest.
    pub small_body_p99_ratio: Option<f64>,
    pub large_body_p99_ratio: Option<f64>,
}

/// A histogram's mean and nearest-rank p99, in its units.
#[derive(Debug, Clone, Copy)]
struct Summary {
    mean: f64,
    slow: i64,
}

impl Summary {
    fn mean_duration(self) -> Duration {
        Duration::from_micros(self.mean.round() as u64)
    }

    fn slow_duration(self) -> Duration {
        Duration::from_micros(self.slow as u64)
    }

    fn slow_ratio(self) -> f64 {
        self.slow as f64 / RATIO_UNITS
    }
}

fn is_small_body(bytes: u64) -> bool {
    bytes < LARGE_BODY_BYTES
}

fn mebibytes(bytes: u64) -> f64 {
    bytes as f64 / MIB as f64
}

fn duration_from_secs(seconds: f64) -> Duration {
    Duration::from_micros((seconds * 1e6).round() as u64)
}

/// Measurements from one backend request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Sample {
    pub first_byte: Duration,
    pub body_bytes: u64,
    /// Time from response headers to the end of the body.
    pub body: Duration,
}

impl Sample {
    #[cfg(test)]
    pub fn from_millis(first_byte_ms: u64, body_bytes: u64, body_ms: u64) -> Self {
        Self {
            first_byte: Duration::from_millis(first_byte_ms),
            body_bytes,
            body: Duration::from_millis(body_ms),
        }
    }

    fn latency(&self) -> Duration {
        self.first_byte.saturating_add(self.body)
    }
}

/// Body time as a fixed time plus a time per MiB, in seconds.
#[derive(Debug, Clone, Copy)]
struct BodyLine {
    fixed: f64,
    per_mib: f64,
}

impl BodyLine {
    /// The fitted time for a body of `bytes`, at least `MIN_BODY_SCALE`.
    fn scale(&self, bytes: u64) -> f64 {
        self.per_mib
            .mul_add(mebibytes(bytes), self.fixed)
            .max(MIN_BODY_SCALE.as_secs_f64())
    }
}

/// Exponentially decayed least-squares fit of body time to a line in body length. Each sample is
/// weighted by the inverse square of the larger of its time and the fit's prediction, so errors
/// count relative to body time whatever the length, and a stall weighs no more than a body that
/// took as long as predicted.
#[derive(Debug, Clone, Copy, Default)]
struct BodyFit {
    updated_at: Option<Instant>,
    weight: f64,
    mebibytes: f64,
    mebibytes_squared: f64,
    seconds: f64,
    mebibyte_seconds: f64,
}

impl BodyFit {
    /// Adds a body to the fit and returns its time relative to the updated fit.
    fn record(&mut self, now: Instant, bytes: u64, time: Duration) -> f64 {
        if let Some(updated_at) = self.updated_at {
            let decay =
                (-LATENCY_DECAY * now.saturating_duration_since(updated_at).as_secs_f64()).exp();
            self.weight *= decay;
            self.mebibytes *= decay;
            self.mebibytes_squared *= decay;
            self.seconds *= decay;
            self.mebibyte_seconds *= decay;
        }
        self.updated_at = Some(now);
        let seconds = time.as_secs_f64();
        let predicted = self.line().map_or(0.0, |line| line.scale(bytes));
        let weight = seconds
            .max(predicted)
            .max(MIN_BODY_SCALE.as_secs_f64())
            .powi(-2);
        let length = mebibytes(bytes);
        self.weight += weight;
        self.mebibytes += weight * length;
        self.mebibytes_squared += weight * length * length;
        self.seconds += weight * seconds;
        self.mebibyte_seconds += weight * length * seconds;
        self.line().map_or(0.0, |line| seconds / line.scale(bytes))
    }

    /// Bodies of nearly one length cannot separate fixed from transfer time, so smaller bodies are
    /// then fitted by duration alone and larger ones by time per MiB alone. Neither coefficient is
    /// fitted below zero.
    fn line(&self) -> Option<BodyLine> {
        if self.weight <= 0.0 {
            return None;
        }
        let mean_length = self.mebibytes / self.weight;
        let mean_square_length = self.mebibytes_squared / self.weight;
        let mean_seconds = self.seconds / self.weight;
        let fixed_only = BodyLine {
            fixed: mean_seconds,
            per_mib: 0.0,
        };
        let proportional = || BodyLine {
            fixed: 0.0,
            per_mib: self.mebibyte_seconds / self.mebibytes_squared,
        };
        let variance = mean_length.mul_add(-mean_length, mean_square_length);
        if variance <= MIN_BODY_LENGTH_SPREAD * mean_square_length {
            return Some(if mean_length < mebibytes(LARGE_BODY_BYTES) {
                fixed_only
            } else {
                proportional()
            });
        }
        let covariance = mean_length.mul_add(-mean_seconds, self.mebibyte_seconds / self.weight);
        let per_mib = covariance / variance;
        let fixed = per_mib.mul_add(-mean_length, mean_seconds);
        Some(if per_mib < 0.0 {
            fixed_only
        } else if fixed < 0.0 {
            proportional()
        } else {
            BodyLine { fixed, per_mib }
        })
    }
}

/// Latency summaries for one bucket. Pages are requested whole, so the body size is unknown until
/// response headers announce it: earlier decisions use complete request latency across the sizes
/// the bucket actually served, and later ones predict body time for the announced length.
#[derive(Debug, Clone, Copy)]
pub(super) struct LatencyModel {
    latency: Option<Summary>,
    first_byte: Option<Summary>,
    body_time: Option<BodyLine>,
    /// Body time relative to `body_time`, for bodies smaller than `LARGE_BODY_BYTES` and the rest.
    small_body: Option<Summary>,
    large_body: Option<Summary>,
}

impl LatencyModel {
    /// Slow complete request latency, or `None` before any measurement.
    fn tail(&self) -> Option<Duration> {
        self.latency.map(Summary::slow_duration)
    }

    pub fn first_byte_tail(&self) -> Option<Duration> {
        self.first_byte.map(Summary::slow_duration)
    }

    /// Slow body time after headers announce `bytes`: the fitted time for that length, scaled by
    /// the slow ratio of body time to the fit in its size class. `None` before the bucket has
    /// served a body in the same size class.
    pub fn body_tail(&self, bytes: u64) -> Option<Duration> {
        let ratios = if is_small_body(bytes) {
            self.small_body
        } else {
            self.large_body
        }?;
        let line = self.body_time?;
        Some(duration_from_secs(ratios.slow_ratio() * line.scale(bytes)))
    }

    /// Time a request must have had before its timeout counts against the bucket: the slow
    /// complete request time, or the slow time to finish the phases it reached if longer. Those
    /// phases are response headers, then any body whose length they announced. The complete
    /// request time keeps a caller timeout shorter than a typical request, which the tail of an
    /// early phase alone would allow, from counting against the bucket. A phase the bucket has not
    /// measured, including a body in a size class it has not served, is allowed `unmeasured`,
    /// so reaching a later phase never shortens the allowance.
    pub fn timeout_allowance(
        &self,
        announced_body_bytes: Option<u64>,
        unmeasured: Duration,
    ) -> Duration {
        let first_byte = self.first_byte_tail().unwrap_or(unmeasured);
        let reached = announced_body_bytes.map_or(first_byte, |bytes| {
            first_byte.saturating_add(self.body_tail(bytes).unwrap_or(unmeasured))
        });
        self.tail().map_or(reached, |tail| tail.max(reached))
    }
}

fn histogram_micros(duration: Duration) -> i64 {
    duration.as_micros().min(i64::MAX as u128) as i64
}

fn histogram(now: Instant) -> ExponentialDecayHistogram {
    let mut builder = ExponentialDecayHistogram::builder();
    builder.at(now.into_std()).alpha(LATENCY_DECAY);
    #[cfg(test)]
    if let Some(capacity) = super::simulation::histogram_capacity() {
        builder.size(capacity);
    }
    builder.build()
}

/// The first value whose cumulative weight, including its own, reaches `q`: the nearest rank.
/// `Snapshot::value` instead takes the first value whose preceding weight reaches `q`, so once a
/// few recent samples carry nearly all the weight, whichever decayed value lies just above them
/// becomes the tail, however old. A tolerance absorbs floating-point sums that land exactly on the
/// quantile.
fn quantile(snapshot: &Snapshot, q: f64) -> i64 {
    const TOLERANCE: f64 = 1e-9;
    let mut cumulative = 0.0;
    let mut last = 0;
    for (value, weight) in snapshot.values() {
        cumulative += weight;
        if cumulative >= q - TOLERANCE {
            return value;
        }
        last = value;
    }
    last
}

fn summarize(histogram: &ExponentialDecayHistogram) -> Option<Summary> {
    let snapshot = histogram.snapshot();
    (snapshot.count() > 0).then(|| Summary {
        mean: snapshot.mean(),
        slow: quantile(&snapshot, 0.99),
    })
}

/// One histogram, summarized again only after it changes.
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

    fn update(&mut self, now: std::time::Instant, value: i64) {
        self.histogram.update_at(now, value);
        self.changed = true;
    }

    /// Summarizes a changed histogram once `due`, or as soon as it holds its first samples.
    fn summary(&mut self, due: bool) -> Option<Summary> {
        if self.changed && (due || self.summary.is_none()) {
            self.changed = false;
            self.summary = summarize(&self.histogram);
        }
        self.summary
    }
}

#[derive(Debug)]
struct LatencyEstimates {
    latency: PhaseHistogram,
    first_byte: PhaseHistogram,
    body_time: BodyFit,
    small_body: PhaseHistogram,
    large_body: PhaseHistogram,
}

impl LatencyEstimates {
    fn new(now: Instant) -> Self {
        Self {
            latency: PhaseHistogram::new(now),
            first_byte: PhaseHistogram::new(now),
            body_time: BodyFit::default(),
            small_body: PhaseHistogram::new(now),
            large_body: PhaseHistogram::new(now),
        }
    }

    fn record(&mut self, now: Instant, sample: &Sample) {
        let ratio = self.body_time.record(now, sample.body_bytes, sample.body);
        let now = now.into_std();
        self.latency.update(now, histogram_micros(sample.latency()));
        self.first_byte
            .update(now, histogram_micros(sample.first_byte));
        let ratios = if is_small_body(sample.body_bytes) {
            &mut self.small_body
        } else {
            &mut self.large_body
        };
        ratios.update(now, (ratio * RATIO_UNITS).round() as i64);
    }

    fn model(&mut self, due: bool) -> LatencyModel {
        LatencyModel {
            latency: self.latency.summary(due),
            first_byte: self.first_byte.summary(due),
            body_time: self.body_time.line(),
            small_body: self.small_body.summary(due),
            large_body: self.large_body.summary(due),
        }
    }

    #[cfg(test)]
    fn retention(&self) -> [(u64, usize); 4] {
        [
            &self.latency,
            &self.first_byte,
            &self.small_body,
            &self.large_body,
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
            active: BTreeMap::new(),
            routing_latency: Duration::ZERO,
            routing_updated_at: now,
            best_latency: None,
            estimates: LatencyEstimates::new(now),
            summarized_at: now,
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

    /// Changed histograms are summarized again at most once per interval, except that a phase is
    /// summarized as soon as it holds its first samples, including after a probe replaces its
    /// history.
    fn model(&mut self, now: Instant) -> LatencyModel {
        let due = now.duration_since(self.summarized_at) >= LATENCY_SNAPSHOT_INTERVAL;
        if due {
            self.summarized_at = now;
        }
        self.estimates.model(due)
    }

    fn overloaded(&self, now: Instant) -> bool {
        now < self.overload_until
    }

    /// Replacing history starts the latency model over from the sample, as for a bucket never
    /// measured, so a recovered bucket is judged by its probe.
    fn record(&mut self, now: Instant, sample: &Sample, replace_history: bool) {
        let latency = sample.latency();
        self.routing_latency = if replace_history || self.best_latency.is_none() {
            latency
        } else {
            self.current_routing_latency(now)
                .mul_f64(1.0 - LATENCY_ALPHA)
                + latency.mul_f64(LATENCY_ALPHA)
        };
        self.routing_updated_at = now;
        if replace_history {
            self.estimates = LatencyEstimates::new(now);
        }
        self.best_latency = Some(self.best_latency.map_or(latency, |best| best.min(latency)));
        self.estimates.record(now, sample);
    }

    /// A copy left unmeasured, whether unfinished or validated without a sample, can raise the
    /// routing latency toward its duration but never lower it.
    fn raise_routing_latency(&mut self, now: Instant, elapsed: Duration) {
        let current = self.current_routing_latency(now);
        self.routing_latency = current + elapsed.saturating_sub(current).mul_f64(LATENCY_ALPHA);
        self.routing_updated_at = now;
    }

    /// Routing latency above the bucket's measured mean decays toward it with the latency
    /// half-life, so a rise fades even while the bucket receives no copies to lower it. A bucket
    /// without measurements keeps its rise.
    fn current_routing_latency(&self, now: Instant) -> Duration {
        let Some(mean) = self.estimates.latency.summary.map(Summary::mean_duration) else {
            return self.routing_latency;
        };
        let excess = self.routing_latency.saturating_sub(mean);
        if excess.is_zero() {
            return self.routing_latency;
        }
        let elapsed = now.saturating_duration_since(self.routing_updated_at);
        mean + excess.mul_f64((-LATENCY_DECAY * elapsed.as_secs_f64()).exp())
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
                return self.current_routing_latency(now).max(elapsed);
            }
        }
        self.current_routing_latency(now)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Outcome {
    /// With the validated request's measurements, unless it was left unmeasured.
    Success(Option<Sample>),
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
        let now = Instant::now();
        if let Some(count) = stats.active.get_mut(&self.started) {
            *count -= 1;
            if *count == 0 {
                stats.active.remove(&self.started);
            }
        }
        let elapsed = now.duration_since(self.started);
        let Some(outcome) = self.outcome else {
            stats.raise_routing_latency(now, elapsed);
            return;
        };
        let error_rate = stats.error_rate(now);
        match outcome {
            Outcome::Success(sample) => {
                match &sample {
                    Some(sample) => stats.record(now, sample, self.probe.is_some()),
                    None => stats.raise_routing_latency(now, elapsed),
                }
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
            let mut stats = snapshot.stats.lock();
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
                        stats: snapshot.stats.clone(),
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
    fn entry(&self, bucket: &BucketName) -> Arc<Mutex<BucketStats>> {
        if let Some(stats) = self.by_bucket.get(bucket) {
            return stats.clone();
        }
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
            if probe.is_some() {
                stats.probe_after = started + recovery_delay();
            }
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

    pub(super) fn snapshot(&self, buckets: &BucketNameSet) -> RoutingSnapshot {
        let now = Instant::now();
        let mut preferred_latency = Duration::ZERO;
        RoutingSnapshot(
            buckets
                .iter()
                .enumerate()
                .map(|(index, bucket)| {
                    let state = self.entry(bucket);
                    let mut stats = state.lock();
                    let model = stats.model(now);
                    if index == 0 {
                        preferred_latency = stats.current_routing_latency(now);
                    }
                    let tail = model.tail();
                    let mut latency = stats.routing_latency(now, tail.unwrap_or_default());
                    if index > 0 && tail.is_none() {
                        latency = latency.max(preferred_latency);
                    }
                    BucketSnapshot {
                        stats: state.clone(),
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
        self.begin(bucket, None)
            .complete(Outcome::Success(Some(sample)));
    }

    /// Seeds a request for the 4-byte objects tests read, whose latency is all time to first byte.
    #[cfg(test)]
    pub(super) fn seed_latency(&self, bucket: &BucketName, millis: u64) {
        self.seed(bucket, Sample::from_millis(millis, 4, 0));
    }

    #[cfg(test)]
    pub(super) fn model(&self, bucket: &BucketName) -> LatencyModel {
        self.entry(bucket).lock().model(Instant::now())
    }

    #[cfg(test)]
    pub(super) fn simulation_histograms(&self) -> Vec<(u64, usize)> {
        self.by_bucket
            .iter()
            .flat_map(|entry| entry.value().lock().estimates.retention())
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
                let metrics = BucketMetrics {
                    error_rate: stats.error_rate(now),
                    deprioritized: stats.deprioritized,
                    consecutive_failures: stats.consecutive_failures,
                    recovery_successes: stats.recovery_successes,
                    latency_mean: model.latency.map(Summary::mean_duration),
                    latency_p99: model.latency.map(Summary::slow_duration),
                    first_byte_mean: model.first_byte.map(Summary::mean_duration),
                    first_byte_p99: model.first_byte.map(Summary::slow_duration),
                    body_overhead: model.body_time.map(|line| duration_from_secs(line.fixed)),
                    body_throughput: model
                        .body_time
                        .and_then(|line| (line.per_mib > 0.0).then(|| MIB as f64 / line.per_mib)),
                    small_body_p99_ratio: model.small_body.map(Summary::slow_ratio),
                    large_body_p99_ratio: model.large_body.map(Summary::slow_ratio),
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
        BucketMetrics, BucketedStats, LARGE_BODY_BYTES, LATENCY_SNAPSHOT_INTERVAL, Outcome,
        ProbePermit, RECOVERY_SUCCESSES, Sample, histogram, quantile,
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
            Sample::from_millis(5, 2 * LARGE_BODY_BYTES, 100),
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
            observation.complete(Outcome::Success(None));
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
            stats.begin(&buckets[0], None).complete(Outcome::Failure);
            advance(Duration::from_secs(37)).await;
            let (index, probe) = stats.snapshot(&buckets).primary(Duration::from_secs(1));
            assert_eq!(index, 0);
            assert!(probe.is_some());
            assert_eq!(
                stats.snapshot(&buckets).primary(Duration::from_secs(1)).0,
                1
            );
            let observation = stats.begin(&buckets[0], probe);
            advance(Duration::from_millis(3)).await;
            observation.complete(Outcome::Success(Some(Sample::from_millis(3, 4, 0))));
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
            for _ in 0..200 {
                stats.seed_latency(&buckets[0], 9);
            }
            stats.begin(&buckets[0], None).complete(Outcome::Failure);
            advance(Duration::from_secs(37)).await;
            let (index, probe) = stats.snapshot(&buckets).primary(Duration::from_secs(1));
            assert_eq!(index, 0);
            let observation = stats.begin(&buckets[0], probe);
            advance(Duration::from_millis(3)).await;
            if probe_succeeds {
                observation.complete(Outcome::Success(Some(Sample::from_millis(3, 4, 0))));
            } else {
                observation.complete(Outcome::Failure);
            }
            let tail = Some(Duration::from_millis(if probe_succeeds { 3 } else { 9 }));
            assert_eq!(stats.model(&buckets[0]).first_byte_tail(), tail);
            assert_eq!(stats.snapshot(&buckets).tail(0), tail);
            assert_eq!(
                stats.entry(&buckets[0]).lock().best_latency,
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
            assert_eq!(metrics.latency_mean, Some(Duration::from_millis(3)));
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

    /// A body taking 20ms plus 10ms per MiB after 10ms to first byte.
    fn paced_body(bytes: u64) -> Sample {
        Sample {
            first_byte: Duration::from_millis(10),
            body_bytes: bytes,
            body: Duration::from_micros(20_000 + 10_000 * bytes / MIB),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn exports_body_overhead_and_throughput_fitted_across_lengths() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        for bytes in [MIB / 2, MIB, 4 * MIB, 16 * MIB] {
            stats.seed(&bucket, paced_body(bytes));
        }
        advance(Duration::from_secs(1)).await;

        let metrics = exported(&stats);
        assert_eq!(metrics.latency_p99, Some(Duration::from_millis(190)));
        assert_eq!(metrics.first_byte_p99, Some(Duration::from_millis(10)));
        assert_eq!(metrics.body_overhead, Some(Duration::from_millis(20)));
        assert!((metrics.body_throughput.unwrap() - 100.0 * MIB as f64).abs() < 1e-3);
        assert_eq!(metrics.small_body_p99_ratio, Some(1.0));
        assert_eq!(metrics.large_body_p99_ratio, Some(1.0));
    }

    #[tokio::test(start_paused = true)]
    async fn short_bodies_do_not_inflate_the_tail_of_full_pages() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        for _ in 0..4 {
            for _ in 0..5 {
                stats.seed(&bucket, paced_body(16 * MIB));
            }
            stats.seed(&bucket, paced_body(MIB));
            stats.seed(&bucket, paced_body(MIB / 16));
        }
        let model = stats.model(&bucket);
        for (bytes, micros) in [(16 * MIB, 180_000), (3 * MIB / 2, 35_000), (4096, 20_039)] {
            assert_eq!(
                model.body_tail(bytes),
                Some(Duration::from_micros(micros)),
                "{bytes}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn quantiles_take_the_nearest_rank_of_samples_recorded_together() {
        let now = Instant::now();
        let mut samples = histogram(now);
        for value in [5, 1, 3, 3, 3, 9, 7, 7, 2, 8] {
            samples.update_at(now.into_std(), value);
        }
        let snapshot = samples.snapshot();
        for (q, expected) in [
            (0.0, 1),
            (0.1, 1),
            (0.15, 2),
            (0.25, 3),
            (0.5, 3),
            (0.55, 5),
            (0.6, 5),
            (0.65, 7),
            (0.8, 7),
            (0.85, 8),
            (0.9, 8),
            (0.95, 9),
            (0.99, 9),
            (1.0, 9),
        ] {
            assert_eq!(quantile(&snapshot, q), expected, "{q}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_decayed_outlier_does_not_become_the_tail_above_recent_samples() {
        let start = Instant::now();
        let mut samples = histogram(start);
        for value in (50..80).chain([1000]) {
            samples.update_at(start.into_std(), value);
        }
        advance(Duration::from_secs(300)).await;
        for value in [90, 91, 92, 93] {
            samples.update_at(Instant::now().into_std(), value);
        }
        assert_eq!(quantile(&samples.snapshot(), 0.99), 93);
    }

    #[tokio::test(start_paused = true)]
    async fn one_slow_transfer_sets_the_large_body_tail_without_moving_the_fit() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        for _ in 0..50 {
            stats.seed(&bucket, Sample::from_millis(10, 16 * MIB, 16));
        }
        stats.seed(&bucket, Sample::from_millis(10, 16 * MIB, 16_000));
        advance(Duration::from_secs(1)).await;
        let tail = stats.model(&bucket).body_tail(16 * MIB).unwrap();
        assert!(
            tail.abs_diff(Duration::from_secs(16)) < Duration::from_millis(1),
            "{tail:?}"
        );
        let throughput = exported(&stats).body_throughput.unwrap();
        assert!(
            (throughput / (1000.0 * MIB as f64) - 1.0).abs() < 1e-3,
            "{throughput}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn bodies_too_fast_to_time_report_no_throughput() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        stats.seed(&bucket, Sample::from_millis(10, 16 * MIB, 0));
        advance(Duration::from_secs(1)).await;
        assert_eq!(exported(&stats).body_throughput, None);
    }

    #[tokio::test(start_paused = true)]
    async fn phases_are_summarized_on_their_first_samples_then_once_per_interval() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        stats.seed(&bucket, Sample::from_millis(10, 4096, 2));
        assert_eq!(
            stats.model(&bucket).first_byte_tail(),
            Some(Duration::from_millis(10))
        );
        stats.seed(&bucket, Sample::from_millis(30, 4 * MIB, 40));
        let model = stats.model(&bucket);
        assert_eq!(model.first_byte_tail(), Some(Duration::from_millis(10)));
        assert_eq!(model.body_tail(4 * MIB), Some(Duration::from_millis(40)));
        advance(LATENCY_SNAPSHOT_INTERVAL).await;
        assert_eq!(
            stats.model(&bucket).first_byte_tail(),
            Some(Duration::from_millis(30))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_successful_probe_starts_the_latency_model_over() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        stats.seed(&bucket, Sample::from_millis(3, 4 * MIB, 250));
        let probe = ProbePermit {
            stats: stats.entry(&bucket),
        };
        let observation = stats.begin(&bucket, Some(probe));
        observation.complete(Outcome::Success(Some(Sample::from_millis(9, 4096, 1))));
        let model = stats.model(&bucket);
        assert_eq!(model.first_byte_tail(), Some(Duration::from_millis(9)));
        assert_eq!(model.body_tail(4096), Some(Duration::from_millis(1)));
        assert_eq!(model.body_tail(16 * MIB), None);
        assert_eq!(model.tail(), Some(Duration::from_millis(10)));
        assert_eq!(
            exported(&stats).body_overhead,
            Some(Duration::from_millis(1))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn body_tails_require_a_measurement_in_the_same_size_class() {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("local").unwrap();
        stats.seed(&bucket, Sample::from_millis(20, 4096, 3));
        let model = stats.model(&bucket);
        assert_eq!(model.body_tail(4096), Some(Duration::from_millis(3)));
        assert_eq!(model.body_tail(16 * LARGE_BODY_BYTES), None);
        let unmeasured = Duration::from_millis(100);
        assert_eq!(
            model.timeout_allowance(None, unmeasured),
            Duration::from_millis(23)
        );
        assert_eq!(
            model.timeout_allowance(Some(4096), unmeasured),
            Duration::from_millis(23)
        );
        assert_eq!(
            model.timeout_allowance(Some(16 * LARGE_BODY_BYTES), unmeasured),
            Duration::from_millis(120)
        );
        let cold = BucketedStats::default().model(&bucket);
        assert_eq!(cold.timeout_allowance(None, unmeasured), unmeasured);
        assert_eq!(
            cold.timeout_allowance(Some(4096), unmeasured),
            Duration::from_millis(200)
        );

        stats.seed(&bucket, Sample::from_millis(20, 4 * LARGE_BODY_BYTES, 250));
        let model = stats.model(&bucket);
        assert_eq!(
            model.body_tail(16 * LARGE_BODY_BYTES),
            Some(Duration::from_micros(991_724))
        );
        assert_eq!(model.tail(), Some(Duration::from_millis(23)));
        advance(LATENCY_SNAPSHOT_INTERVAL).await;
        assert_eq!(
            stats.model(&bucket).tail(),
            Some(Duration::from_millis(270))
        );
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
    async fn an_outlier_stops_steering_reads_away_once_its_rise_decays() {
        let stats = BucketedStats::default();
        let buckets = buckets();
        for (bucket, millis) in buckets.iter().zip([3, 4, 5]) {
            stats.seed_latency(bucket, millis);
        }
        let best = || stats.snapshot(&buckets).best(&[], Duration::from_secs(1));
        assert_eq!(best(), Some(0));
        let stall = stats.begin(&buckets[0], None);
        advance(Duration::from_millis(100)).await;
        drop(stall);
        assert_eq!(best(), Some(1));
        advance(Duration::from_secs(10)).await;
        assert_eq!(best(), Some(0));
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
            assert!(!stats.entry(&buckets[0]).lock().deprioritized);
        }
    }
}
