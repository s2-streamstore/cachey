use std::{ops::Range, time::Duration};

use futures::{StreamExt, stream::FuturesUnordered};
use tokio::{
    select,
    time::{Instant, sleep_until},
};

use super::{
    BucketOperation, DownloadError, DownloadOutput, Downloader, ObjectPiece, RequestConfig,
};
use crate::{
    object_store::{
        admission::DownloadPermit,
        budget::HedgePermit,
        stats::{ProbePermit, RoutingSnapshot},
    },
    types::{BucketNameSet, ObjectKey},
};

const MAX_CONCURRENT_COPIES: usize = 3;

type Completion = (usize, Result<ObjectPiece, DownloadError>);

fn record_fallback_error(last_error: &mut Option<DownloadError>, error: DownloadError) {
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
    if last_error
        .as_ref()
        .is_none_or(|previous| priority(&error) >= priority(previous))
    {
        *last_error = Some(error);
    }
}

pub(super) struct AttemptPermits {
    admission: DownloadPermit,
    hedge: Option<HedgePermit>,
}

struct PrimaryAttempt {
    index: usize,
    probe: Option<ProbePermit>,
    permits: Option<AttemptPermits>,
    backup_admission: Option<DownloadPermit>,
}

#[derive(Default)]
struct Progress {
    active: usize,
    hedged: bool,
}

struct ActiveCopy<'a>(&'a parking_lot::Mutex<Progress>);

impl Drop for ActiveCopy<'_> {
    fn drop(&mut self) {
        self.0.lock().active -= 1;
    }
}

struct ReplicaRequest<'a> {
    downloader: &'a Downloader,
    buckets: &'a BucketNameSet,
    object: &'a ObjectKey,
    range: &'a Range<u64>,
    config: &'a RequestConfig,
    deadline: Instant,
    progress: parking_lot::Mutex<Progress>,
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
        let progress = self.progress.lock();
        DownloadOutput {
            piece,
            primary_bucket_idx: primary,
            used_bucket_idx: used,
            latency: start.elapsed(),
            hedged: progress.hedged,
        }
    }

    async fn attempt(
        &self,
        index: usize,
        probe: Option<ProbePermit>,
        permits: Option<AttemptPermits>,
    ) -> Completion {
        let bucket = &self.buckets[index];
        let (hedge, admission) = match permits {
            Some(permits) => (permits.hedge, Some(permits.admission)),
            None => (None, None),
        };
        let hedge_config = hedge.as_ref().map(|_| RequestConfig {
            max_attempts: Some(1),
            ..self.config.clone()
        });
        let _hedge = hedge;
        let config = hedge_config.as_ref().unwrap_or(self.config);
        let result = BucketOperation {
            downloader: self.downloader,
            bucket,
            deadline: self.deadline,
            unknown_latency: self.unknown_latency(),
            probe,
        }
        .execute(self.range.end - self.range.start, admission, |_| async {
            let _active = {
                let mut progress = self.progress.lock();
                progress.hedged |= progress.active > 0;
                progress.active += 1;
                ActiveCopy(&self.progress)
            };
            self.downloader
                .fetch_piece(bucket, self.object, self.range, config)
                .await
        })
        .await;
        (index, result)
    }
}

impl Downloader {
    pub(super) async fn download_replicas(
        &self,
        buckets: &BucketNameSet,
        object: &ObjectKey,
        range: &Range<u64>,
        config: &RequestConfig,
        start: Instant,
        deadline: Instant,
    ) -> Result<DownloadOutput, DownloadError> {
        let request = ReplicaRequest {
            downloader: self,
            buckets,
            object,
            range,
            config,
            deadline,
            progress: parking_lot::Mutex::default(),
        };
        let routing = self.bucketed_stats.snapshot(buckets);
        let PrimaryAttempt {
            index: primary,
            probe,
            permits: primary_permits,
            mut backup_admission,
        } = self.prepare_primary(
            range.end - range.start,
            deadline.saturating_duration_since(Instant::now()),
            &routing,
        );
        let mut tried = vec![false; buckets.len()];
        tried[primary] = true;
        let mut rescues = routing.rescue_schedule(primary, probe.is_some(), self.limits);
        let threshold = routing.tail(primary).unwrap_or_default();
        let mut hedge_at =
            (self.limits.hedge_budget_percent > 0 && !threshold.is_zero() && probe.is_none())
                .then(|| start + threshold.min(self.limits.page_timeout));
        let mut active = FuturesUnordered::new();
        active.push(request.attempt(primary, probe, primary_permits));
        let mut last_error = None;
        let mut retry_budget_exhausted = false;
        while !active.is_empty() {
            let next_rescue = rescues
                .front()
                .map_or(deadline, |(after, _)| start + *after);
            let next_hedge = hedge_at.unwrap_or(deadline);
            let (early, scheduled) = select! {
                biased;
                Some((index, result)) = active.next() => {
                    match result {
                        Ok(piece) => return Ok(request.output(piece, primary, index, start)),
                        Err(error) => {
                            if !error.should_attempt_fallback_bucket() { return Err(error); }
                            record_fallback_error(&mut last_error, error);
                            hedge_at = None;
                        }
                    }
                    (false, None)
                },
                () = sleep_until(next_rescue), if !rescues.is_empty() => {
                    (false, rescues.pop_front().map(|(_, index)| index))
                },
                () = sleep_until(next_hedge), if hedge_at.is_some() => {
                    hedge_at = None;
                    (true, None)
                },
            };
            if active.len() >= MAX_CONCURRENT_COPIES || scheduled.is_some_and(|index| tried[index])
            {
                continue;
            }
            let routing = self.bucketed_stats.snapshot(buckets);
            let overloaded = routing.all_overloaded();
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || (early && overloaded) {
                continue;
            }
            let Some(index) = scheduled.or_else(|| routing.best(&tried, remaining)) else {
                continue;
            };
            if overloaded && !self.attempt_budget.try_retry() {
                retry_budget_exhausted = true;
                continue;
            }
            let permits = if early {
                let Some(permits) = self.hedge_permits(&buckets[index], range.end - range.start)
                else {
                    continue;
                };
                Some(permits)
            } else {
                backup_admission.take().map(|admission| AttemptPermits {
                    hedge: None,
                    admission,
                })
            };
            tried[index] = true;
            active.push(request.attempt(index, None, permits));
        }
        Err(match last_error {
            None | Some(DownloadError::NoSuchKey) if retry_budget_exhausted => {
                DownloadError::Overloaded(
                    "Replica retry budget exhausted during widespread overload".to_owned(),
                )
            }
            Some(error) => error,
            None => DownloadError::Unknown("No replica could complete the read".to_owned()),
        })
    }

    pub(super) fn hedge_permits(
        &self,
        bucket: &crate::types::BucketName,
        bytes: u64,
    ) -> Option<AttemptPermits> {
        let admission = self.admission.try_acquire(bytes)?;
        let hedge = self.attempt_budget.try_hedge(bucket)?;
        Some(AttemptPermits {
            hedge: Some(hedge),
            admission,
        })
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

    use super::{DownloadError, record_fallback_error};
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

    fn assert_accumulated_error(errors: [DownloadError; 2], expected: &DownloadError) {
        let mut last_error = None;
        for error in errors {
            record_fallback_error(&mut last_error, error);
        }
        let actual = last_error.unwrap();
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
}
