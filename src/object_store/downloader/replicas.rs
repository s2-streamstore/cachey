use std::{
    collections::VecDeque,
    ops::Range,
    sync::{
        OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures::{StreamExt, stream::FuturesUnordered};
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
struct FallbackErrors {
    /// Each bucket's kept error, in the order it was recorded.
    by_bucket: Vec<(usize, DownloadError)>,
    largest_object_size: Option<u64>,
}

impl FallbackErrors {
    /// An error that the bucket's other copies would repeat replaces its earlier errors and
    /// outlasts later ones. Otherwise the bucket keeps the more relevant error, or the later of
    /// two equally relevant.
    fn record(&mut self, index: usize, error: DownloadError) {
        if let DownloadError::RangeNotSatisfied { object_size, .. } = &error {
            self.largest_object_size = self.largest_object_size.max(*object_size);
        }
        if let Some(position) = self
            .by_bucket
            .iter()
            .position(|(bucket, _)| *bucket == index)
        {
            let previous = &self.by_bucket[position].1;
            if !error.repeats_within_bucket()
                && (previous.repeats_within_bucket() || priority(&error) < priority(previous))
            {
                return;
            }
            self.by_bucket.remove(position);
        }
        self.by_bucket.push((index, error));
    }

    /// Whether the bucket failed with an error its other copies would repeat.
    fn settled(&self, index: usize) -> bool {
        self.by_bucket
            .iter()
            .any(|(bucket, error)| *bucket == index && error.repeats_within_bucket())
    }

    /// The most relevant of the buckets' errors, or the latest of equally relevant ones.
    fn into_error(self, retry_budget_exhausted: bool) -> DownloadError {
        let mut last_error = self
            .by_bucket
            .into_iter()
            .map(|(_, error)| error)
            .max_by_key(priority);
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

struct PrimaryAttempt {
    index: usize,
    probe: Option<ProbePermit>,
    permits: Option<AttemptPermits>,
    backup_admission: Option<DownloadPermit>,
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
    config: &'a RequestConfig,
    page_deadline: Instant,
    /// Each bucket's deadline, shared by its copies once the first is admitted.
    bucket_deadlines: Vec<OnceLock<Instant>>,
    copies_in_flight: AtomicUsize,
    /// Whether a copy started while another was in flight.
    hedged: AtomicBool,
}

struct CopyAttempt<'a> {
    index: usize,
    model: LatencyModel,
    /// The primary's timeline, which the hedge timer follows; other copies keep their own.
    primary_timeline: Option<&'a RequestTimeline>,
    probe: Option<ProbePermit>,
    permits: Option<AttemptPermits>,
}

impl ReplicaRequest<'_> {
    fn bucket_deadline(&self, index: usize) -> BucketDeadline<'_> {
        BucketDeadline::new(self.page_deadline, &self.bucket_deadlines[index])
    }

    fn unknown_latency(&self) -> Duration {
        self.downloader.limits.page_timeout / u32::try_from(self.buckets.len()).unwrap_or(u32::MAX)
    }

    fn output(
        &self,
        piece: ObjectPiece,
        primary: usize,
        used: usize,
        start: Instant,
    ) -> DownloadOutput {
        DownloadOutput {
            piece,
            primary_bucket_idx: primary,
            used_bucket_idx: used,
            latency: start.elapsed(),
            hedged: self.hedged.load(Ordering::Relaxed),
        }
    }

    async fn attempt(&self, copy: CopyAttempt<'_>) -> Completion {
        let CopyAttempt {
            index,
            model,
            primary_timeline,
            probe,
            permits,
        } = copy;
        let own_timeline = RequestTimeline::default();
        let timeline = primary_timeline.unwrap_or(&own_timeline);
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
            config: hedge_config.as_ref().unwrap_or(self.config),
        };
        let result = BucketOperation {
            downloader: self.downloader,
            request,
            model,
            timeline,
            deadline: self.bucket_deadline(index),
            unknown_latency: self.unknown_latency(),
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
        Completion {
            index,
            is_primary: primary_timeline.is_some(),
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

/// What may start a copy after the primary.
#[derive(Debug, Clone, Copy)]
enum Trigger {
    /// A copy failed.
    Fallback,
    /// The rescue timer reserved for a bucket fired.
    Rescue(usize),
    /// The primary's hedge timer fired.
    Hedge,
}

/// Tracks each bucket's copies, and chooses the bucket and permits for each copy after the
/// primary.
struct Followups<'a> {
    request: &'a ReplicaRequest<'a>,
    bytes: u64,
    primary: usize,
    /// Buckets given a copy.
    tried: Vec<bool>,
    in_flight: Vec<usize>,
    errors: FallbackErrors,
    /// The bucket whose body last failed after its headers.
    body_failed: Option<usize>,
    /// Whether a failed body was retried against its bucket.
    retried: bool,
    retry_budget_exhausted: bool,
    backup_admission: Option<DownloadPermit>,
}

impl<'a> Followups<'a> {
    /// Whether a copy to an unsettled bucket is in flight.
    fn awaiting(&self) -> bool {
        self.in_flight
            .iter()
            .enumerate()
            .any(|(index, copies)| *copies > 0 && !self.errors.settled(index))
    }

    fn started(&mut self, index: usize) {
        self.tried[index] = true;
        self.in_flight[index] += 1;
    }

    fn failed(&mut self, index: usize, error: DownloadError) {
        self.in_flight[index] -= 1;
        if error.should_retry_same_bucket() {
            self.body_failed = Some(index);
        }
        self.errors.record(index, error);
    }

    fn into_error(self) -> DownloadError {
        self.errors.into_error(self.retry_budget_exhausted)
    }

    /// The copy to start for `trigger`, if any. A fallback goes to an untried bucket, or else
    /// retries a failed body against its bucket once per read when no copy is awaited; the
    /// retry, like any copy while every bucket reports overload, draws on the retry budget. An
    /// early hedge skips a bucket reporting overload. A later copy to a bucket starts only while
    /// more than the bucket's p99 request latency remains before its deadline.
    fn next(&mut self, trigger: Trigger, hedge: &mut HedgeTimer<'_>) -> Option<CopyAttempt<'a>> {
        if let Trigger::Rescue(index) = trigger
            && self.tried[index]
        {
            return None;
        }
        let now = Instant::now();
        let remaining = self.request.page_deadline.saturating_duration_since(now);
        if remaining.is_zero() {
            return None;
        }
        let early = matches!(trigger, Trigger::Hedge);
        let downloader = self.request.downloader;
        let routing = downloader.bucketed_stats.snapshot(self.request.buckets);
        let index = match trigger {
            Trigger::Rescue(index) => index,
            Trigger::Hedge => routing.best(&self.tried, remaining).unwrap_or(self.primary),
            Trigger::Fallback => routing.best(&self.tried, remaining).or_else(|| {
                self.body_failed.filter(|index| {
                    !self.retried && !self.errors.settled(*index) && !self.awaiting()
                })
            })?,
        };
        if early && routing.overloaded(index) {
            return None;
        }
        let model = routing.model(index);
        if let Some(bucket_deadline) = self.request.bucket_deadline(index).admitted()
            && bucket_deadline.saturating_duration_since(now) <= model.tail().unwrap_or_default()
        {
            return None;
        }
        let overloaded = routing.all_overloaded();
        let retrying = !early && self.tried[index];
        if (overloaded || retrying) && !downloader.attempt_budget.try_retry() {
            self.retry_budget_exhausted |= overloaded;
            return None;
        }
        self.retried |= retrying;
        let permits = if early {
            let admission = downloader.admission.try_acquire(self.bytes)?;
            let budget = downloader
                .attempt_budget
                .try_hedge(&self.request.buckets[index])?;
            hedge.start_hedge();
            Some(AttemptPermits {
                hedge: Some(budget),
                admission,
            })
        } else {
            self.backup_admission
                .take()
                .map(|admission| AttemptPermits {
                    hedge: None,
                    admission,
                })
        };
        self.started(index);
        Some(CopyAttempt {
            index,
            model,
            primary_timeline: None,
            probe: None,
            permits,
        })
    }
}

impl Downloader {
    /// A bucket's copies share the deadline its first copy got at admission. An early hedge goes
    /// to the primary's bucket once no other bucket is untried, so a single bucket hedges against
    /// itself. An error that another copy from the same bucket would repeat settles the bucket,
    /// whose other copies then no longer hold the read open. A body that fails after its headers
    /// is retried against its bucket once no untried bucket or awaited copy remains.
    pub(super) async fn download_replicas(
        &self,
        buckets: &BucketNameSet,
        object: &ObjectKey,
        range: &Range<u64>,
        config: &RequestConfig,
        start: Instant,
        deadline: Instant,
    ) -> Result<DownloadOutput, DownloadError> {
        let bytes = range.end - range.start;
        let request = ReplicaRequest {
            downloader: self,
            buckets,
            object,
            range,
            config,
            page_deadline: deadline,
            bucket_deadlines: vec![OnceLock::new(); buckets.len()],
            copies_in_flight: AtomicUsize::new(0),
            hedged: AtomicBool::new(false),
        };
        let primary_timeline = RequestTimeline::default();
        let routing = self.bucketed_stats.snapshot(buckets);
        let PrimaryAttempt {
            index: primary,
            probe,
            permits: primary_permits,
            backup_admission,
        } = self.prepare_primary(
            bytes,
            deadline.saturating_duration_since(Instant::now()),
            &routing,
        );
        let primary_model = routing.model(primary);
        let mut rescues = routing.rescue_schedule(primary, probe.is_some(), self.limits);
        let mut followups = Followups {
            request: &request,
            bytes,
            primary,
            tried: vec![false; buckets.len()],
            in_flight: vec![0; buckets.len()],
            errors: FallbackErrors::default(),
            body_failed: None,
            retried: false,
            retry_budget_exhausted: false,
            backup_admission,
        };
        followups.started(primary);
        let mut hedge = HedgeTimer::new(
            primary_model,
            &primary_timeline,
            request.bucket_deadline(primary),
            self.limits.hedge_budget_percent > 0 && probe.is_none(),
        );
        let mut active = FuturesUnordered::new();
        active.push(request.attempt(CopyAttempt {
            index: primary,
            model: primary_model,
            primary_timeline: Some(&primary_timeline),
            probe,
            permits: primary_permits,
        }));
        while followups.awaiting() {
            let trigger = select! {
                biased;
                Some(Completion { index, is_primary, result }) = active.next() => {
                    let error = match result {
                        Ok(piece) => return Ok(request.output(piece, primary, index, start)),
                        Err(error) => error,
                    };
                    if !error.should_attempt_fallback_bucket() {
                        return Err(error);
                    }
                    if is_primary {
                        hedge.disarm();
                    }
                    followups.failed(index, error);
                    Trigger::Fallback
                },
                index = next_rescue(&mut rescues, start) => Trigger::Rescue(index),
                () = hedge.due() => Trigger::Hedge,
            };
            if active.len() < MAX_CONCURRENT_COPIES
                && let Some(copy) = followups.next(trigger, &mut hedge)
            {
                active.push(request.attempt(copy));
            }
        }
        Err(followups.into_error())
    }

    fn prepare_primary(
        &self,
        bytes: u64,
        remaining: Duration,
        routing: &RoutingSnapshot,
    ) -> PrimaryAttempt {
        let (mut index, mut probe) = routing.primary(remaining);
        let reservations = probe.as_ref().and_then(|_| {
            Some((
                self.admission.try_acquire(bytes)?,
                self.admission.try_acquire(bytes)?,
            ))
        });
        if probe.is_some() && reservations.is_none() {
            probe = None;
            index = routing.best(&[], remaining).unwrap_or(index);
        }
        let (admission, backup_admission) = reservations.unzip();
        PrimaryAttempt {
            index,
            probe,
            permits: admission.map(|admission| AttemptPermits {
                admission,
                hedge: None,
            }),
            backup_admission,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{mem::discriminant, time::Duration};

    use itertools::Itertools;

    use super::{DownloadError, FallbackErrors};
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
        let mut fallback_errors = FallbackErrors::default();
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
