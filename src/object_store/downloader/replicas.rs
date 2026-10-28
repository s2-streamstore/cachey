use std::{
    collections::VecDeque,
    future::poll_fn,
    ops::Range,
    pin::Pin,
    sync::{
        OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use tokio::{
    select,
    time::{Instant, sleep_until},
};

use super::{
    BucketDeadline, BucketOperation, DownloadError, DownloadOutput, Downloader, HedgeTimer,
    ObjectPiece, PieceRequest, RequestConfig, RequestTimeline,
};
use crate::{
    object_store::{
        admission::DownloadPermit,
        budget::HedgePermit,
        stats::{LatencyModel, ProbePermit, RoutingSnapshot},
    },
    types::{BucketNameSet, ObjectKey},
};

const MAX_CONCURRENT_COPIES: usize = 3;

struct Completion {
    index: usize,
    is_primary: bool,
    result: Result<ObjectPiece, DownloadError>,
}

async fn next_completion<F: Future<Output = Completion>>(
    copies: &mut Vec<(usize, Pin<Box<F>>)>,
) -> Completion {
    let (position, completion) = poll_fn(|context| {
        for (position, (_, future)) in copies.iter_mut().enumerate() {
            if let Poll::Ready(completion) = future.as_mut().poll(context) {
                return Poll::Ready((position, completion));
            }
        }
        Poll::Pending
    })
    .await;
    drop(copies.remove(position));
    completion
}

fn priority(error: &DownloadError) -> u8 {
    match error {
        DownloadError::NoSuchKey => 0,
        DownloadError::AdmissionTimeout => 1,
        DownloadError::Timeout { .. } => 2,
        DownloadError::InvalidObjectState(_)
        | DownloadError::InvalidResponse(_)
        | DownloadError::BodyStreaming(_)
        | DownloadError::Overloaded(_)
        | DownloadError::Unknown(_)
        | DownloadError::RangeNotSatisfied { .. }
        | DownloadError::AdmissionExhausted { .. } => 3,
    }
}

#[derive(Default)]
struct BucketState {
    tried: bool,
    body_failed: bool,
    error: Option<(usize, DownloadError)>,
}

impl BucketState {
    fn settled(&self) -> bool {
        self.error
            .as_ref()
            .is_some_and(|(_, error)| error.repeats_within_bucket())
    }
}

struct ReplicaStates {
    buckets: Vec<BucketState>,
    failure_sequence: usize,
    largest_object_size: Option<u64>,
}

impl ReplicaStates {
    fn new(buckets: usize) -> Self {
        Self {
            buckets: std::iter::repeat_with(BucketState::default)
                .take(buckets)
                .collect(),
            failure_sequence: 0,
            largest_object_size: None,
        }
    }

    /// Conclusive errors settle their bucket; otherwise retain its highest-priority error.
    /// Sequence numbers preserve the latest diagnostic across equally relevant bucket errors.
    fn record(&mut self, index: usize, error: DownloadError) {
        if let DownloadError::RangeNotSatisfied { object_size, .. } = &error {
            self.largest_object_size = self.largest_object_size.max(*object_size);
        }
        let bucket = &mut self.buckets[index];
        if let Some((_, previous)) = &bucket.error
            && !error.repeats_within_bucket()
            && (previous.repeats_within_bucket() || priority(&error) < priority(previous))
        {
            return;
        }
        self.failure_sequence += 1;
        bucket.error = Some((self.failure_sequence, error));
    }

    fn into_error(self, retry_budget_exhausted: bool) -> DownloadError {
        let mut last_error = self
            .buckets
            .into_iter()
            .filter_map(|bucket| bucket.error)
            .max_by_key(|(sequence, error)| (priority(error), *sequence))
            .map(|(_, error)| error);
        if let Some(DownloadError::RangeNotSatisfied { object_size, .. }) = &mut last_error {
            *object_size = self.largest_object_size;
        }
        match last_error {
            None | Some(DownloadError::NoSuchKey) if retry_budget_exhausted => {
                DownloadError::Overloaded(
                    "Replica retry budget exhausted during widespread overload".to_owned(),
                )
            }
            Some(error) => error,
            None => DownloadError::Unknown("No replica could complete the read".to_owned()),
        }
    }
}

struct AttemptPermits {
    admission: DownloadPermit,
    hedge: Option<HedgePermit>,
}

struct InFlightCopy<'a>(&'a AtomicUsize);

impl Drop for InFlightCopy<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

struct ReplicaRequest<'a> {
    downloader: &'a Downloader,
    buckets: &'a BucketNameSet,
    object: &'a ObjectKey,
    range: &'a Range<u64>,
    bytes: u64,
    config: &'a RequestConfig,
    download_deadline: Instant,
    primary: usize,
    deadlines: Vec<BucketDeadline>,
    primary_timeline: RequestTimeline,
    /// Response length announced to a finished copy.
    finished_body_bytes: OnceLock<u64>,
    copies_in_flight: AtomicUsize,
    /// Whether a copy started while another was in flight.
    hedged: AtomicBool,
}

struct CopyAttempt {
    index: usize,
    model: LatencyModel,
    is_primary: bool,
    probe: Option<ProbePermit>,
    permits: Option<AttemptPermits>,
}

impl ReplicaRequest<'_> {
    fn body_bytes(&self) -> Option<u64> {
        self.primary_timeline
            .announced_body_bytes()
            .or_else(|| self.finished_body_bytes.get().copied())
    }

    /// Allow each unmeasured phase half of a bucket's share of the download timeout, within
    /// its own timeout.
    fn unmeasured(&self) -> Duration {
        let limits = self.downloader.limits;
        limits
            .bucket_share(self.buckets.len())
            .min(limits.bucket_timeout)
            / 2
    }

    fn output(&self, piece: ObjectPiece, used: usize, start: Instant) -> DownloadOutput {
        DownloadOutput {
            piece,
            primary_bucket_idx: self.primary,
            used_bucket_idx: used,
            latency: start.elapsed(),
            hedged: self.hedged.load(Ordering::Relaxed),
        }
    }

    async fn attempt(&self, copy: CopyAttempt) -> Completion {
        let CopyAttempt {
            index,
            model,
            is_primary,
            probe,
            permits,
        } = copy;
        let own_timeline = RequestTimeline::default();
        let timeline = if is_primary {
            &self.primary_timeline
        } else {
            &own_timeline
        };
        let (hedge, admission) = match permits {
            Some(permits) => (permits.hedge, Some(permits.admission)),
            None => (None, None),
        };
        let hedge_config = hedge.as_ref().map(|_| self.config.single_attempt());
        let _hedge = hedge;
        let request = PieceRequest {
            bucket: &self.buckets[index],
            object: self.object,
            range: self.range,
            bytes: self.bytes,
            config: hedge_config.as_ref().unwrap_or(self.config),
        };
        let result = BucketOperation {
            downloader: self.downloader,
            request,
            model,
            timeline,
            deadline: &self.deadlines[index],
            unmeasured: self.unmeasured(),
            probe,
        }
        .execute(admission, async || {
            if self.copies_in_flight.fetch_add(1, Ordering::Relaxed) > 0 {
                self.hedged.store(true, Ordering::Relaxed);
            }
            let _in_flight = InFlightCopy(&self.copies_in_flight);
            self.downloader.fetch_piece(request, timeline).await
        })
        .await;
        if let Some(bytes) = timeline.announced_body_bytes() {
            let _ = self.finished_body_bytes.set(bytes);
        }
        Completion {
            index,
            is_primary,
            result,
        }
    }
}

/// Completes with the bucket whose rescue timer fires next, or never once none remain.
async fn next_rescue(rescues: &mut VecDeque<(Duration, usize)>, start: Instant) -> usize {
    let Some(&(after, index)) = rescues.front() else {
        return std::future::pending().await;
    };
    sleep_until(start + after).await;
    rescues.pop_front();
    index
}

#[derive(Debug, Clone, Copy)]
enum Trigger {
    Fallback,
    Rescue(usize),
    Hedge,
}

struct Followups<'a> {
    request: &'a ReplicaRequest<'a>,
    states: ReplicaStates,
    /// Whether a failed body was retried against its bucket.
    retried: bool,
    retry_budget_exhausted: bool,
    backup_admission: Option<DownloadPermit>,
}

impl Followups<'_> {
    fn started(&mut self, index: usize) {
        self.states.buckets[index].tried = true;
    }

    fn failed(&mut self, index: usize, error: DownloadError) {
        let bucket = &mut self.states.buckets[index];
        bucket.body_failed |= error.should_retry_same_bucket();
        self.states.record(index, error);
    }

    fn into_error(self) -> DownloadError {
        self.states.into_error(self.retry_budget_exhausted)
    }

    /// A later copy needs the time its bucket would allow before a timeout counts as a
    /// failure, within the bucket's shared deadline.
    fn has_time(&self, routing: &RoutingSnapshot, index: usize, now: Instant) -> bool {
        let request = self.request;
        request.deadlines[index]
            .expires_at()
            .is_none_or(|deadline| {
                let expected = routing.model(index).timeout_allowance(
                    request.bytes,
                    request.body_bytes(),
                    request.unmeasured(),
                );
                deadline.saturating_duration_since(now) > expected
            })
    }

    /// Prefer untried buckets. Retry a failed body only after other copies finish,
    /// at most once per download and within the retry budget.
    fn next(&mut self, trigger: Trigger, idle: bool) -> Option<CopyAttempt> {
        if let Trigger::Rescue(index) = trigger
            && self.states.buckets[index].tried
        {
            return None;
        }
        let now = Instant::now();
        let remaining = self
            .request
            .download_deadline
            .saturating_duration_since(now);
        if remaining.is_zero() {
            return None;
        }
        let early = matches!(trigger, Trigger::Hedge);
        let downloader = self.request.downloader;
        let routing = downloader.bucketed_stats.snapshot(
            self.request.buckets,
            self.request.bytes,
            downloader.limits,
        );
        let untried = |index: usize| !self.states.buckets[index].tried;
        let index = match trigger {
            Trigger::Rescue(index) => index,
            Trigger::Hedge => routing
                .best(
                    |index| {
                        untried(index)
                            && !routing.overloaded(index)
                            && routing.feasible(index, remaining)
                    },
                    remaining,
                )
                .or_else(|| {
                    let primary = self.request.primary;
                    (!routing.overloaded(primary) && self.has_time(&routing, primary, now))
                        .then_some(primary)
                })?,
            Trigger::Fallback => routing.best(untried, remaining).or_else(|| {
                if self.retried || !idle {
                    return None;
                }
                routing.best(
                    |index| {
                        let bucket = &self.states.buckets[index];
                        bucket.body_failed
                            && !bucket.settled()
                            && self.has_time(&routing, index, now)
                    },
                    remaining,
                )
            })?,
        };
        let overloaded = routing.all_overloaded();
        let retrying = !early && self.states.buckets[index].tried;
        // Hedges and retries never queue for admission, so a retry cannot spend a credit
        // and then run out of time before it starts.
        let admission = if early || retrying {
            Some(downloader.admission.try_acquire(self.request.bytes)?)
        } else {
            None
        };
        if (overloaded || retrying) && !downloader.attempt_budget.try_retry() {
            self.retry_budget_exhausted |= overloaded;
            return None;
        }
        self.retried |= retrying;
        let hedge = if early {
            Some(
                downloader
                    .attempt_budget
                    .try_hedge(&self.request.buckets[index])?,
            )
        } else {
            None
        };
        let permits = admission
            .or_else(|| self.backup_admission.take())
            .map(|admission| AttemptPermits { admission, hedge });
        self.started(index);
        Some(CopyAttempt {
            index,
            model: routing.model(index).clone(),
            is_primary: false,
            probe: None,
            permits,
        })
    }
}

impl Downloader {
    /// Conclusive errors settle a bucket, so its remaining copies cannot keep the download waiting.
    pub(super) async fn download_replicas(
        &self,
        buckets: &BucketNameSet,
        object: &ObjectKey,
        range: &Range<u64>,
        bytes: u64,
        config: &RequestConfig,
    ) -> Result<DownloadOutput, DownloadError> {
        let start = Instant::now();
        let deadline = start
            .checked_add(self.limits.download_timeout)
            .ok_or_else(|| {
                DownloadError::Unknown(
                    "Download timeout exceeds the clock's supported range".to_owned(),
                )
            })?;
        let routing = self.bucketed_stats.snapshot(buckets, bytes, self.limits);
        let (primary_copy, backup_admission) = self.prepare_primary(
            bytes,
            deadline.saturating_duration_since(Instant::now()),
            &routing,
        );
        let primary = primary_copy.index;
        let probing = primary_copy.probe.is_some();
        let request = ReplicaRequest {
            downloader: self,
            buckets,
            object,
            range,
            bytes,
            config,
            download_deadline: deadline,
            primary,
            deadlines: (0..buckets.len())
                .map(|_| BucketDeadline::new(deadline))
                .collect(),
            primary_timeline: RequestTimeline::default(),
            finished_body_bytes: OnceLock::new(),
            copies_in_flight: AtomicUsize::new(0),
            hedged: AtomicBool::new(false),
        };
        let mut rescues = routing.rescue_schedule(primary, probing);
        let mut followups = Followups {
            request: &request,
            states: ReplicaStates::new(buckets.len()),
            retried: false,
            retry_budget_exhausted: false,
            backup_admission,
        };
        followups.started(primary);
        let mut hedge = HedgeTimer::new(
            primary_copy.model.clone(),
            &request.primary_timeline,
            bytes,
            self.limits.hedge_budget_percent > 0 && !probing,
        );
        let mut active = Vec::with_capacity(MAX_CONCURRENT_COPIES);
        active.push((primary, Box::pin(request.attempt(primary_copy))));
        while !active.is_empty() {
            let trigger = select! {
                biased;
                Completion { index, is_primary, result } = next_completion(&mut active) => {
                    let error = match result {
                        Ok(piece) => return Ok(request.output(piece, index, start)),
                        Err(error) => error,
                    };
                    if error.ends_read() {
                        return Err(error);
                    }
                    if error.repeats_within_bucket() {
                        active.retain(|(bucket, _)| *bucket != index);
                    }
                    if is_primary {
                        hedge.disarm();
                    }
                    let fallback = error.should_attempt_fallback_bucket();
                    followups.failed(index, error);
                    if !fallback {
                        continue;
                    }
                    Trigger::Fallback
                },
                index = next_rescue(&mut rescues, start) => Trigger::Rescue(index),
                () = hedge.due() => Trigger::Hedge,
            };
            if active.len() < MAX_CONCURRENT_COPIES
                && let Some(copy) = followups.next(trigger, active.is_empty())
            {
                if matches!(trigger, Trigger::Hedge) {
                    hedge.disarm();
                }
                active.push((copy.index, Box::pin(request.attempt(copy))));
            }
        }
        Err(followups.into_error())
    }

    /// The primary copy, and admission reserved for a backup while it probes recovery.
    fn prepare_primary(
        &self,
        bytes: u64,
        remaining: Duration,
        routing: &RoutingSnapshot,
    ) -> (CopyAttempt, Option<DownloadPermit>) {
        let (mut index, mut probe) = routing.primary(remaining);
        let reservations = probe.as_ref().and_then(|_| {
            Some((
                self.admission.try_acquire(bytes)?,
                self.admission.try_acquire(bytes)?,
            ))
        });
        if probe.is_some() && reservations.is_none() {
            probe = None;
            index = routing.best(|_| true, remaining).unwrap_or(index);
        }
        let (admission, backup_admission) = reservations.unzip();
        let copy = CopyAttempt {
            index,
            model: routing.model(index).clone(),
            is_primary: true,
            probe,
            permits: admission.map(|admission| AttemptPermits {
                admission,
                hedge: None,
            }),
        };
        (copy, backup_admission)
    }
}

#[cfg(test)]
mod tests {
    use std::{mem::discriminant, time::Duration};

    use itertools::Itertools;

    use super::{DownloadError, ReplicaStates};
    use crate::types::BucketName;

    fn timeout(bucket: &str) -> DownloadError {
        DownloadError::Timeout {
            bucket: BucketName::new(bucket).unwrap(),
            timeout: Duration::from_millis(50),
        }
    }

    fn backend_errors() -> [DownloadError; 5] {
        [
            DownloadError::InvalidObjectState("archived object".to_owned()),
            DownloadError::InvalidResponse("invalid content range".to_owned()),
            DownloadError::BodyStreaming("connection reset".to_owned()),
            DownloadError::Overloaded("slow down".to_owned()),
            DownloadError::Unknown("service failure".to_owned()),
        ]
    }

    fn assert_bucket_errors(
        errors: impl IntoIterator<Item = (usize, DownloadError)>,
        expected: &DownloadError,
    ) {
        let mut fallback_errors = ReplicaStates::new(3);
        for (index, error) in errors {
            fallback_errors.record(index, error);
        }
        let actual = fallback_errors.into_error(false);
        assert_eq!(discriminant(&actual), discriminant(expected), "{actual:?}");
        assert_eq!(actual.to_string(), expected.to_string());
    }

    /// Records each error from its own bucket.
    fn assert_accumulated_error(
        errors: impl IntoIterator<Item = DownloadError>,
        expected: &DownloadError,
    ) {
        assert_bucket_errors(errors.into_iter().enumerate(), expected);
    }

    fn assert_preferred_in_either_order(preferred: &DownloadError, other: &DownloadError) {
        assert_accumulated_error([preferred.clone(), other.clone()], preferred);
        assert_accumulated_error([other.clone(), preferred.clone()], preferred);
    }

    #[test]
    fn partial_misses_do_not_mask_other_replica_errors() {
        for error in backend_errors()
            .into_iter()
            .chain([DownloadError::AdmissionTimeout, timeout("peer")])
        {
            assert_preferred_in_either_order(&error, &DownloadError::NoSuchKey);
        }
    }

    #[test]
    fn backend_errors_survive_timeout_and_admission_failures() {
        for error in backend_errors() {
            for fallback in [DownloadError::AdmissionTimeout, timeout("peer")] {
                assert_preferred_in_either_order(&error, &fallback);
            }
        }
    }

    #[test]
    fn an_error_its_bucket_would_repeat_replaces_that_buckets_other_errors() {
        let overloaded = DownloadError::Overloaded("slow down".to_owned());
        for (errors, expected) in [
            (
                vec![(0, overloaded.clone()), (0, DownloadError::NoSuchKey)],
                &DownloadError::NoSuchKey,
            ),
            (
                vec![(0, DownloadError::NoSuchKey), (0, timeout("primary"))],
                &DownloadError::NoSuchKey,
            ),
            (
                vec![
                    (1, overloaded.clone()),
                    (0, timeout("primary")),
                    (0, DownloadError::NoSuchKey),
                ],
                &overloaded,
            ),
        ] {
            assert_bucket_errors(errors, expected);
        }
    }

    #[test]
    fn download_timeout_survives_admission_failure() {
        assert_preferred_in_either_order(&timeout("peer"), &DownloadError::AdmissionTimeout);
    }

    #[test]
    fn equally_relevant_errors_preserve_the_latest_diagnostic() {
        let primary = DownloadError::BodyStreaming("primary connection reset".to_owned());
        for fallback in [
            DownloadError::BodyStreaming("fallback stream truncated".to_owned()),
            DownloadError::Overloaded("fallback throttled".to_owned()),
        ] {
            assert_accumulated_error([primary.clone(), fallback.clone()], &fallback);
        }
        let fallback = timeout("fallback");
        assert_accumulated_error([timeout("primary"), fallback.clone()], &fallback);
    }

    fn range_error(object_size: Option<u64>) -> DownloadError {
        DownloadError::RangeNotSatisfied {
            requested: 32..64,
            object_size,
        }
    }

    #[test]
    fn unsatisfied_ranges_preserve_the_largest_known_size_in_any_order() {
        for sizes in [
            [Some(17), Some(4), Some(0)],
            [Some(17), None, Some(0)],
            [Some(0), None, None],
            [None, None, None],
        ] {
            let expected = range_error(sizes.into_iter().flatten().max());
            for ordering in sizes.into_iter().permutations(sizes.len()) {
                assert_accumulated_error(ordering.into_iter().map(range_error), &expected);
            }
        }
    }

    #[test]
    fn range_sizes_survive_intervening_backend_errors() {
        for backend_error in backend_errors() {
            for size in [Some(0), Some(4), None] {
                assert_accumulated_error(
                    [
                        range_error(Some(17)),
                        backend_error.clone(),
                        range_error(size),
                    ],
                    &range_error(Some(17)),
                );
            }
        }
    }

    #[test]
    fn range_size_evidence_preserves_error_precedence() {
        for backend_error in backend_errors() {
            assert_accumulated_error(
                [
                    range_error(Some(17)),
                    range_error(Some(0)),
                    backend_error.clone(),
                ],
                &backend_error,
            );
        }
        for error in [
            DownloadError::NoSuchKey,
            DownloadError::AdmissionTimeout,
            timeout("peer"),
        ] {
            assert_preferred_in_either_order(&range_error(Some(17)), &error);
        }
    }
}
