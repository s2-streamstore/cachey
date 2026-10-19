use std::{
    sync::{
        OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::{
    select,
    sync::Notify,
    time::{Instant, sleep_until},
};

use crate::object_store::stats::{BodySample, LatencyModel, Sample};

#[derive(Debug, Clone, Copy)]
struct Headers {
    at: Instant,
    body_bytes: u64,
}

/// When a backend request started, when response headers with a validated range arrived with the
/// announced body length, and whether the request has finished.
#[derive(Debug, Default)]
pub(super) struct RequestTimeline {
    started: OnceLock<Instant>,
    headers: OnceLock<Headers>,
    finished: AtomicBool,
    changed: Notify,
}

/// Marks its request finished when dropped, whether it completed, failed or was cancelled.
#[must_use]
pub(super) struct InFlight<'a> {
    timeline: &'a RequestTimeline,
    pub started: Instant,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.timeline.finished.store(true, Ordering::Relaxed);
    }
}

impl RequestTimeline {
    pub fn start(&self) -> InFlight<'_> {
        let now = Instant::now();
        if self.started.set(now).is_ok() {
            self.changed.notify_one();
        }
        InFlight {
            timeline: self,
            started: now,
        }
    }

    pub fn in_flight(&self) -> bool {
        self.started.get().is_some() && !self.finished.load(Ordering::Relaxed)
    }

    pub fn receive_headers(&self, body_bytes: u64) {
        let headers = Headers {
            at: Instant::now(),
            body_bytes,
        };
        if self.headers.set(headers).is_ok() {
            self.changed.notify_one();
        }
    }

    pub fn announced_body_bytes(&self) -> Option<u64> {
        self.headers.get().map(|headers| headers.body_bytes)
    }

    /// Measurements of a request still in flight, where the time spent in its current phase is a
    /// lower bound.
    pub fn in_flight_sample(&self) -> Option<Sample> {
        if self.finished.load(Ordering::Relaxed) {
            return None;
        }
        self.sample()
    }

    /// Measurements so far. For a request that has not finished, the time spent in its current
    /// phase is a lower bound.
    pub fn sample(&self) -> Option<Sample> {
        let started = *self.started.get()?;
        Some(match self.headers.get() {
            None => Sample {
                first_byte: started.elapsed(),
                body: None,
            },
            Some(headers) => Sample {
                first_byte: headers.at.saturating_duration_since(started),
                body: Some(BodySample {
                    bytes: headers.body_bytes,
                    elapsed: headers.at.elapsed(),
                }),
            },
        })
    }
}

/// A phase is late only once it has taken strictly longer than its tail, so a response that arrives
/// exactly at the tail does not start a hedge.
const LATE_BY: Duration = Duration::from_micros(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    FirstByte,
    Body,
}

/// Times a page's early hedge against its primary request in two phases: the p98 time to first
/// byte after the request starts, then, once headers announce the body length, when the body has
/// taken longer than both its p98 time and the p98 time to first byte. Each phase is due at most
/// once, so a hedge that cannot start before headers may still start during the body. A body in a
/// size class the bucket has not served is not hedged early.
pub(super) struct HedgeTimer<'a> {
    model: LatencyModel,
    primary: &'a RequestTimeline,
    deadline: Instant,
    next: Option<Phase>,
}

impl<'a> HedgeTimer<'a> {
    pub fn new(
        model: LatencyModel,
        primary: &'a RequestTimeline,
        deadline: Instant,
        enabled: bool,
    ) -> Self {
        Self {
            model,
            primary,
            deadline,
            next: model
                .first_byte_hedge()
                .filter(|hedge| enabled && !hedge.is_zero())
                .map(|_| Phase::FirstByte),
        }
    }

    pub fn disarm(&mut self) {
        self.next = None;
    }

    fn due_at(&self) -> Option<(Phase, Instant)> {
        let first_byte_hedge = self.model.first_byte_hedge()?;
        let (phase, at) = match (self.next?, self.primary.headers.get()) {
            (_, Some(headers)) => {
                let body_hedge = self.model.body_hedge(headers.body_bytes)?;
                (
                    Phase::Body,
                    headers
                        .at
                        .checked_add(body_hedge.max(first_byte_hedge) + LATE_BY)?,
                )
            }
            (Phase::FirstByte, None) => (
                Phase::FirstByte,
                self.primary
                    .started
                    .get()?
                    .checked_add(first_byte_hedge + LATE_BY)?,
            ),
            (Phase::Body, None) => return None,
        };
        (at < self.deadline).then_some((phase, at))
    }

    /// Completes when a hedge is due, or never if none can be.
    pub async fn due(&mut self) {
        if self.next.is_none() {
            return std::future::pending().await;
        }
        let primary = self.primary;
        loop {
            let changed = primary.changed.notified();
            let Some((phase, at)) = self.due_at() else {
                changed.await;
                continue;
            };
            select! {
                biased;
                () = changed => {}
                () = sleep_until(at) => {
                    self.next = (phase == Phase::FirstByte).then_some(Phase::Body);
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::{Instant, sleep, timeout};

    use super::{HedgeTimer, RequestTimeline};
    use crate::{
        object_store::stats::{BucketedStats, LatencyModel, MIB, Sample},
        types::BucketName,
    };

    fn model(first_byte_ms: u64, body_bytes: u64, body_ms: u64) -> LatencyModel {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("bucket").unwrap();
        stats.seed(
            &bucket,
            Sample::from_millis(first_byte_ms, body_bytes, body_ms),
        );
        stats.model(&bucket)
    }

    async fn due_after(timer: &mut HedgeTimer<'_>) -> Option<Duration> {
        let start = Instant::now();
        timeout(Duration::from_secs(60), timer.due())
            .await
            .ok()
            .map(|()| start.elapsed())
    }

    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(10)
    }

    #[tokio::test(start_paused = true)]
    async fn first_byte_phase_is_timed_from_the_request_start() {
        let timeline = RequestTimeline::default();
        let mut timer = HedgeTimer::new(model(20, 4096, 3), &timeline, deadline(), true);
        let start = Instant::now();
        tokio::join!(
            async {
                sleep(Duration::from_millis(50)).await;
                let _in_flight = timeline.start();
            },
            timer.due(),
        );
        assert_eq!(start.elapsed(), Duration::from_millis(71));
    }

    #[tokio::test(start_paused = true)]
    async fn a_hedge_not_taken_before_headers_can_be_taken_during_the_body() {
        let timeline = RequestTimeline::default();
        let mut timer = HedgeTimer::new(model(20, 4096, 3), &timeline, deadline(), true);
        let _in_flight = timeline.start();
        assert_eq!(due_after(&mut timer).await, Some(Duration::from_millis(21)));
        sleep(Duration::from_millis(10)).await;
        timeline.receive_headers(4096);
        assert_eq!(due_after(&mut timer).await, Some(Duration::from_millis(21)));
        assert_eq!(due_after(&mut timer).await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn bodies_are_hedged_by_their_announced_size_class() {
        for (large_body_ms, expected) in
            [(None, None), (Some(250), Some(Duration::from_millis(1001)))]
        {
            let model = match large_body_ms {
                None => model(20, 4096, 3),
                Some(body_ms) => model(20, 4 * MIB, body_ms),
            };
            let timeline = RequestTimeline::default();
            let mut timer = HedgeTimer::new(model, &timeline, deadline(), true);
            let _in_flight = timeline.start();
            timeline.receive_headers(16 * MIB);
            assert_eq!(due_after(&mut timer).await, expected);
        }
    }
}
