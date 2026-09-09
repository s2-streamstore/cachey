use std::{collections::VecDeque, ops::Range, time::Duration};

use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use tokio::{
    select,
    time::{Instant, sleep_until},
};

use super::{DownloadError, DownloadOutput, Downloader, ObjectPiece, RequestConfig};
use crate::{
    object_store::{
        admission::DownloadPermit,
        hedging::HedgePermit,
        stats::{Outcome, ProbePermit},
    },
    types::{BucketNameSet, ObjectKey},
};

const MAX_CONCURRENT_COPIES: usize = 3;

type Completion = (usize, Result<ObjectPiece, DownloadError>);

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

#[derive(Default)]
struct Progress {
    started: usize,
    active: usize,
    secondary: Option<usize>,
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
    fn next_copy(&self, tried: &[bool], scheduled: Option<usize>) -> Option<usize> {
        scheduled
            .or_else(|| {
                self.downloader
                    .bucketed_stats
                    .attempt_order(
                        self.buckets,
                        tried,
                        self.deadline.saturating_duration_since(Instant::now()),
                    )
                    .first()
                    .copied()
            })
            .filter(|index| !tried[*index])
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
            secondary_bucket_idx: progress.secondary,
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
        let expected = self.downloader.bucketed_stats.tail_latency(bucket);
        let speculative = permits
            .as_ref()
            .is_some_and(|permits| permits.hedge.is_some());
        let (_hedge, admission) = match permits {
            Some(permits) => (permits.hedge, Ok(permits.admission)),
            None => (
                None,
                self.downloader
                    .admission
                    .acquire(self.range.end - self.range.start, self.deadline)
                    .await,
            ),
        };
        let _admission = match admission {
            Ok(permit) => permit,
            Err(error) => return (index, Err(error)),
        };
        let now = Instant::now();
        let deadline = now
            + self
                .downloader
                .limits
                .bucket_timeout
                .min(self.deadline.saturating_duration_since(now));
        if now >= deadline {
            return (index, Err(DownloadError::AdmissionTimeout));
        }
        let _active = {
            let mut progress = self.progress.lock();
            if progress.started == 1 {
                progress.secondary = Some(index);
            }
            progress.started += 1;
            progress.hedged |= progress.active > 0;
            progress.active += 1;
            ActiveCopy(&self.progress)
        };
        let observation = self.downloader.bucketed_stats.begin(bucket, probe);
        let budget = deadline.saturating_duration_since(Instant::now());
        let hedge_config = RequestConfig {
            max_attempts: Some(1),
            ..self.config.clone()
        };
        let config = if speculative {
            &hedge_config
        } else {
            self.config
        };
        let result = select! {
            biased;
            () = sleep_until(deadline) => Err(DownloadError::Timeout { bucket: bucket.clone(), timeout: budget }),
            result = self.downloader.fetch_piece(bucket, self.object, self.range, config) => result,
        };
        observation.complete(result.as_ref().map_or_else(
            |error| error.health_outcome(budget, expected),
            |_| Outcome::Success,
        ));
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
        let PrimaryAttempt {
            index: primary,
            probe,
            permits: primary_permits,
            mut backup_admission,
        } = self.prepare_primary(
            buckets,
            range.end - range.start,
            deadline.saturating_duration_since(Instant::now()),
        );
        let mut tried = vec![false; buckets.len()];
        tried[primary] = true;
        let mut rescues = self.rescue_schedule(buckets, primary, start, deadline, probe.is_some());
        let threshold = self
            .bucketed_stats
            .hedging_threshold(&buckets[primary], start);
        let mut hedge_at = (!threshold.is_zero() && probe.is_none())
            .then(|| start + threshold.min(self.limits.page_timeout));
        let mut active: FuturesUnordered<BoxFuture<'_, Completion>> = FuturesUnordered::new();
        active.push(request.attempt(primary, probe, primary_permits).boxed());
        let mut last_error = None;
        loop {
            let next_rescue = rescues.front().map_or(deadline, |(at, _)| *at);
            let next_hedge = hedge_at.unwrap_or(deadline);
            let (early, scheduled) = select! {
                biased;
                () = sleep_until(deadline) => return Err(DownloadError::Timeout {
                    bucket: buckets[primary].clone(), timeout: self.limits.page_timeout,
                }),
                completion = active.next(), if !active.is_empty() => {
                    if let Some((index, result)) = completion {
                        match result {
                            Ok(piece) => return Ok(request.output(piece, primary, index, start)),
                            Err(error) => {
                                if !error.should_attempt_fallback_bucket() { return Err(error); }
                                last_error = Some(error);
                                hedge_at = None;
                            }
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
            let overloaded = self.bucketed_stats.all_overloaded(buckets);
            let allowed = active.len() < MAX_CONCURRENT_COPIES && !(early && overloaded);
            let next = request.next_copy(&tried, scheduled);
            if allowed && let Some(index) = next {
                if overloaded && !self.overload_retry_budget.try_retry() {
                    if active.is_empty() {
                        break;
                    }
                    continue;
                }
                let permits = if early {
                    let Some(permits) =
                        self.hedge_permits(&buckets[index], range.end - range.start)
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
                active.push(request.attempt(index, None, permits).boxed());
            }
            if active.is_empty() {
                break;
            }
        }
        Err(last_error.unwrap_or_else(|| {
            DownloadError::Unknown("No replica could complete the read".to_owned())
        }))
    }

    fn hedge_permits(
        &self,
        bucket: &crate::types::BucketName,
        bytes: u64,
    ) -> Option<AttemptPermits> {
        let admission = self.admission.try_acquire(bytes)?;
        let hedge = self.hedge_budget.try_acquire(bucket)?;
        Some(AttemptPermits {
            hedge: Some(hedge),
            admission,
        })
    }

    fn prepare_primary(
        &self,
        buckets: &BucketNameSet,
        bytes: u64,
        remaining: Duration,
    ) -> PrimaryAttempt {
        let (mut index, mut probe) = self.bucketed_stats.primary(buckets, remaining);
        let mut backup_admission = None;
        let permits = if probe.is_some() {
            let pair = self.admission.try_acquire(bytes).and_then(|first| {
                self.admission
                    .try_acquire(bytes)
                    .map(|backup| (first, backup))
            });
            if let Some((first, backup)) = pair {
                backup_admission = Some(backup);
                Some(AttemptPermits {
                    admission: first,
                    hedge: None,
                })
            } else {
                probe = None;
                index = self.bucketed_stats.attempt_order(
                    buckets,
                    &vec![false; buckets.len()],
                    remaining,
                )[0];
                None
            }
        } else {
            None
        };
        PrimaryAttempt {
            index,
            probe,
            permits,
            backup_admission,
        }
    }

    fn rescue_schedule(
        &self,
        buckets: &BucketNameSet,
        primary: usize,
        start: Instant,
        deadline: Instant,
        probing: bool,
    ) -> VecDeque<(Instant, usize)> {
        let page_budget = deadline.saturating_duration_since(start);
        let mut excluded = vec![false; buckets.len()];
        excluded[primary] = true;
        let alternatives = self
            .bucketed_stats
            .attempt_order(buckets, &excluded, page_budget);
        let mut reserves: Vec<_> = alternatives
            .iter()
            .map(|index| {
                self.bucketed_stats
                    .tail_latency(&buckets[*index])
                    .saturating_mul(2)
                    .max(Duration::from_millis(20))
                    .min(self.limits.bucket_timeout)
            })
            .collect();
        let total = reserves
            .iter()
            .copied()
            .fold(Duration::ZERO, Duration::saturating_add);
        let reserve_budget = page_budget / 3 * 2;
        if total > reserve_budget && !total.is_zero() {
            let scale = reserve_budget.as_secs_f64() / total.as_secs_f64();
            for reserve in &mut reserves {
                *reserve = reserve.mul_f64(scale);
            }
        }
        let primary_tail = self
            .bucketed_stats
            .tail_latency(&buckets[primary])
            .min(self.limits.bucket_timeout)
            .min(page_budget);
        let mut remaining = reserves
            .iter()
            .copied()
            .fold(Duration::ZERO, Duration::saturating_add);
        let mut schedule = Vec::with_capacity(reserves.len() + 1);
        for (index, reserve) in alternatives.iter().copied().zip(reserves) {
            schedule.push((
                start + page_budget.saturating_sub(remaining).max(primary_tail),
                index,
            ));
            remaining = remaining.saturating_sub(reserve);
        }
        if probing && let Some(index) = alternatives.first() {
            let grace = self
                .bucketed_stats
                .tail_latency(&buckets[*index])
                .saturating_mul(2)
                .max(Duration::from_millis(1));
            schedule.push((start + grace.min(page_budget), *index));
        }
        schedule.sort_unstable();
        schedule.into()
    }
}
