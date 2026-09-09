use std::{collections::VecDeque, ops::Range, time::Duration};

use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
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
        hedging::HedgePermit,
        stats::{ProbePermit, RoutingSnapshot},
    },
    types::{BucketNameSet, ObjectKey},
};

const MAX_CONCURRENT_COPIES: usize = 3;

type Completion = (usize, Result<ObjectPiece, DownloadError>);

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
        .execute(
            async {
                match admission {
                    Some(permit) => Ok(permit),
                    None => {
                        self.downloader
                            .admission
                            .acquire(self.range.end - self.range.start, self.deadline)
                            .await
                    }
                }
            },
            |_| async {
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
                self.downloader
                    .fetch_piece(bucket, self.object, self.range, config)
                    .await
            },
        )
        .await;
        (index, result)
    }
}

impl Downloader {
    #[allow(clippy::too_many_lines)]
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
        let mut rescues = self.rescue_schedule(
            buckets.len(),
            primary,
            start,
            deadline,
            probe.is_some(),
            &routing,
        );
        let threshold = routing.tail(primary).unwrap_or_default();
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
            let routing = self.bucketed_stats.snapshot(buckets);
            let overloaded = routing.all_overloaded();
            let allowed = Instant::now() < deadline
                && active.len() < MAX_CONCURRENT_COPIES
                && !(early && overloaded);
            let next = scheduled
                .or_else(|| {
                    routing.best(&tried, deadline.saturating_duration_since(Instant::now()))
                })
                .filter(|index| !tried[*index]);
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

    pub(super) fn hedge_permits(
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
        bytes: u64,
        remaining: Duration,
        routing: &RoutingSnapshot,
    ) -> PrimaryAttempt {
        let (mut index, mut probe) = routing.primary(remaining);
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
                index = routing.best(&[], remaining).unwrap_or(index);
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
        bucket_count: usize,
        primary: usize,
        start: Instant,
        deadline: Instant,
        probing: bool,
        routing: &RoutingSnapshot,
    ) -> VecDeque<(Instant, usize)> {
        let page_budget = deadline.saturating_duration_since(start);
        let mut alternatives = routing.alternatives(primary, page_budget);
        alternatives.retain(|index| routing.tail(*index).is_none_or(|tail| tail < page_budget));
        let unknown_latency = page_budget / u32::try_from(bucket_count).unwrap_or(u32::MAX);
        let reserve_budget = page_budget / 3 * 2;
        let reserves: Vec<_> = alternatives
            .iter()
            .map(|index| {
                routing
                    .tail(*index)
                    .map_or(unknown_latency, |tail| {
                        tail.saturating_mul(2)
                            .min(reserve_budget)
                            .max(tail.saturating_add(Duration::from_millis(1)))
                    })
                    .max(Duration::from_millis(20))
                    .min(self.limits.bucket_timeout)
                    .min(page_budget)
            })
            .collect();
        let total = reserves
            .iter()
            .copied()
            .fold(Duration::ZERO, Duration::saturating_add);
        let scale = if total > reserve_budget && !total.is_zero() {
            reserve_budget.as_secs_f64() / total.as_secs_f64()
        } else {
            1.0
        };
        let primary_tail = routing
            .tail(primary)
            .unwrap_or_default()
            .min(self.limits.bucket_timeout)
            .min(page_budget);
        let mut remaining = total.mul_f64(scale);
        let mut schedule = Vec::with_capacity(reserves.len() + 1);
        for (index, reserve) in alternatives.iter().copied().zip(reserves) {
            let latest_start = page_budget.saturating_sub(reserve);
            let planned = page_budget.saturating_sub(remaining).max(primary_tail);
            let expected = routing.tail(index).unwrap_or(reserve);
            let at = if !routing.any_overloaded() && planned.saturating_add(expected) >= page_budget
            {
                planned.min(latest_start)
            } else {
                planned
            };
            schedule.push((start + at, index));
            remaining = remaining.saturating_sub(reserve.mul_f64(scale));
        }
        if probing && let Some(index) = alternatives.first() {
            let grace = routing
                .tail(*index)
                .unwrap_or(unknown_latency)
                .saturating_mul(2)
                .max(Duration::from_millis(1));
            schedule.push((start + grace.min(page_budget), *index));
        }
        schedule.sort_unstable();
        schedule.into()
    }
}
