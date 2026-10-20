use std::{
    ops::Range,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration,
};

use futures::{StreamExt, stream::FuturesUnordered};
use tokio::{
    select,
    time::{Instant, sleep_until},
};

use super::{
    BucketOperation, DownloadError, DownloadOutput, Downloader, HedgeTimer, ObjectPiece,
    PieceRequest, RequestConfig, RequestTimeline,
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

type Completion = (usize, Result<ObjectPiece, DownloadError>);

#[derive(Default)]
struct FallbackErrors {
    last_error: Option<DownloadError>,
    largest_object_size: Option<u64>,
    retry_budget_exhausted: bool,
}

impl FallbackErrors {
    fn record(&mut self, error: DownloadError) {
        if let DownloadError::RangeNotSatisfied { object_size, .. } = &error {
            self.largest_object_size = self.largest_object_size.max(*object_size);
        }
        let priority = |error: &DownloadError| match error {
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
        };
        if self
            .last_error
            .as_ref()
            .is_none_or(|previous| priority(&error) >= priority(previous))
        {
            self.last_error = Some(error);
        }
    }

    fn into_error(mut self) -> DownloadError {
        if let Some(DownloadError::RangeNotSatisfied { object_size, .. }) = &mut self.last_error {
            *object_size = self.largest_object_size;
        }
        match self.last_error {
            None | Some(DownloadError::NoSuchKey) if self.retry_budget_exhausted => {
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
    copies_in_flight: AtomicUsize,
    /// Whether a copy started while another was in flight.
    hedged: AtomicBool,
}

/// One copy of the page from a bucket.
struct CopyAttempt<'a> {
    index: usize,
    model: LatencyModel,
    /// The primary's timeline, which the hedge timer follows; other copies keep their own.
    primary_timeline: Option<&'a RequestTimeline>,
    deadline: Instant,
    probe: Option<ProbePermit>,
    permits: Option<AttemptPermits>,
}

impl ReplicaRequest<'_> {
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
            deadline,
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
            deadline,
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
        (index, result)
    }
}

/// What may start a copy after the primary.
#[derive(Debug, Clone, Copy)]
enum Trigger {
    /// A copy failed. With no other copy in flight, `retry` names the bucket whose body last
    /// failed after headers.
    Fallback { retry: Option<usize> },
    /// The rescue timer reserved for a bucket fired.
    Rescue(usize),
    /// The primary's hedge timer fired.
    Hedge,
}

/// Chooses the bucket and permits for each copy after the primary.
struct Followups<'a> {
    downloader: &'a Downloader,
    buckets: &'a BucketNameSet,
    bytes: u64,
    deadline: Instant,
    primary: usize,
    primary_timeline: &'a RequestTimeline,
    /// Buckets given a copy.
    tried: Vec<bool>,
    /// Whether a failed body was retried against its bucket.
    retried: bool,
    backup_admission: Option<DownloadPermit>,
}

impl<'a> Followups<'a> {
    /// The copy to start for `trigger`, if any. A fallback goes to an untried bucket, or else
    /// retries a failed body against its bucket once per read; the retry, like any copy while
    /// every bucket reports overload, draws on the retry budget.
    fn next(
        &mut self,
        trigger: Trigger,
        hedge: &mut HedgeTimer<'_>,
        fallback_errors: &mut FallbackErrors,
    ) -> Option<CopyAttempt<'a>> {
        if let Trigger::Rescue(index) = trigger
            && self.tried[index]
        {
            return None;
        }
        let early = matches!(trigger, Trigger::Hedge);
        let routing = self.downloader.bucketed_stats.snapshot(self.buckets);
        let overloaded = routing.all_overloaded();
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() || (early && overloaded) {
            return None;
        }
        let index = match trigger {
            Trigger::Rescue(index) => index,
            Trigger::Hedge => routing.best(&self.tried, remaining).unwrap_or(self.primary),
            Trigger::Fallback { retry } => routing
                .best(&self.tried, remaining)
                .or(retry.filter(|_| !self.retried))?,
        };
        let retrying = !early && self.tried[index];
        if (overloaded || retrying) && !self.downloader.attempt_budget.try_retry() {
            fallback_errors.retry_budget_exhausted |= overloaded;
            return None;
        }
        self.retried |= retrying;
        let permits = if early {
            let admission = self.downloader.admission.try_acquire(self.bytes)?;
            let Some(budget) = self
                .downloader
                .attempt_budget
                .try_hedge(&self.buckets[index])
            else {
                hedge.deny();
                return None;
            };
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
        self.tried[index] = true;
        Some(CopyAttempt {
            index,
            model: routing.model(index),
            primary_timeline: None,
            deadline: if index == self.primary {
                self.primary_timeline
                    .bucket_deadline()
                    .unwrap_or(self.deadline)
            } else {
                self.deadline
            },
            probe: None,
            permits,
        })
    }
}

impl Downloader {
    /// Copies from the same bucket share its bucket deadline. An early hedge goes to the primary's
    /// bucket once no other bucket is untried, so a single bucket hedges against itself; there, an
    /// error that another copy from the same bucket would repeat ends the read. A body that fails
    /// after its headers is retried against its bucket once no untried bucket or other copy
    /// remains.
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
        let single_bucket = buckets.len() == 1;
        let request = ReplicaRequest {
            downloader: self,
            buckets,
            object,
            range,
            config,
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
        let mut tried = vec![false; buckets.len()];
        tried[primary] = true;
        let mut followups = Followups {
            downloader: self,
            buckets,
            bytes,
            deadline,
            primary,
            primary_timeline: &primary_timeline,
            tried,
            retried: false,
            backup_admission,
        };
        let mut hedge = HedgeTimer::new(
            primary_model,
            &primary_timeline,
            deadline,
            self.limits.hedge_budget_percent > 0 && probe.is_none(),
        );
        let mut active = FuturesUnordered::new();
        active.push(request.attempt(CopyAttempt {
            index: primary,
            model: primary_model,
            primary_timeline: Some(&primary_timeline),
            deadline,
            probe,
            permits: primary_permits,
        }));
        let mut fallback_errors = FallbackErrors::default();
        let mut body_failed = None;
        while !active.is_empty() {
            let next_rescue = rescues
                .front()
                .map_or(deadline, |(after, _)| start + *after);
            let trigger = select! {
                biased;
                Some((index, result)) = active.next() => {
                    let error = match result {
                        Ok(piece) => return Ok(request.output(piece, primary, index, start)),
                        Err(error) => error,
                    };
                    if !error.should_attempt_fallback_bucket()
                        || (single_bucket && !error.should_wait_for_hedged_peer())
                    {
                        return Err(error);
                    }
                    if error.should_retry_same_bucket() {
                        body_failed = Some(index);
                    }
                    fallback_errors.record(error);
                    if index == primary {
                        hedge.disarm();
                    }
                    Trigger::Fallback { retry: body_failed.filter(|_| active.is_empty()) }
                },
                () = sleep_until(next_rescue), if !rescues.is_empty() => {
                    let Some((_, index)) = rescues.pop_front() else {
                        continue;
                    };
                    Trigger::Rescue(index)
                },
                () = hedge.due() => Trigger::Hedge,
            };
            if active.len() < MAX_CONCURRENT_COPIES
                && let Some(copy) = followups.next(trigger, &mut hedge, &mut fallback_errors)
            {
                active.push(request.attempt(copy));
            }
        }
        Err(fallback_errors.into_error())
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

    fn assert_accumulated_error(
        errors: impl IntoIterator<Item = DownloadError>,
        expected: &DownloadError,
    ) {
        let mut fallback_errors = FallbackErrors::default();
        for error in errors {
            fallback_errors.record(error);
        }
        let actual = fallback_errors.into_error();
        assert_eq!(discriminant(&actual), discriminant(expected), "{actual:?}");
        assert_eq!(actual.to_string(), expected.to_string());
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
