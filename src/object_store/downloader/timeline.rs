use std::sync::OnceLock;

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

/// When a backend request started, and when its successful response headers arrived with the
/// announced body length.
#[derive(Debug, Default)]
pub(super) struct RequestTimeline {
    started: OnceLock<Instant>,
    headers: OnceLock<Headers>,
    changed: Notify,
}

impl RequestTimeline {
    pub fn start(&self, at: Instant) {
        if self.started.set(at).is_ok() {
            self.changed.notify_one();
        }
    }

    pub fn receive_headers(&self, at: Instant, body_bytes: u64) {
        if self.headers.set(Headers { at, body_bytes }).is_ok() {
            self.changed.notify_one();
        }
    }

    pub fn announced_body_bytes(&self) -> Option<u64> {
        self.headers.get().map(|headers| headers.body_bytes)
    }

    /// The time a superseded request had spent so far, a lower bound for its unfinished phase.
    pub fn unfinished_sample(&self) -> Option<Sample> {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    FirstByte,
    Body,
}

/// Times a page's early hedge against its primary request in two phases: the p99 time to first
/// byte after the request starts, then, once headers announce the body length, when the body has
/// taken longer than both its p99 time and the p99 time to first byte. Each phase is due at most
/// once, so a hedge that cannot start before headers may still start during the body. A body in a
/// size class the bucket has not served is not hedged early.
pub(super) struct HedgeTimer<'a> {
    model: LatencyModel,
    primary: &'a RequestTimeline,
    deadline: Instant,
    next: Option<Phase>,
}

impl<'a> HedgeTimer<'a> {
    pub fn new(model: LatencyModel, primary: &'a RequestTimeline, deadline: Instant) -> Self {
        Self {
            model,
            primary,
            deadline,
            next: model
                .first_byte_tail()
                .filter(|tail| !tail.is_zero())
                .map(|_| Phase::FirstByte),
        }
    }

    pub fn primary(&self) -> &'a RequestTimeline {
        self.primary
    }

    pub fn disarm(&mut self) {
        self.next = None;
    }

    fn due_at(&self) -> Option<(Phase, Instant)> {
        let first_byte_tail = self.model.first_byte_tail()?;
        let (phase, at) = match (self.next?, self.primary.headers.get()) {
            (_, Some(headers)) => {
                let body_tail = self.model.body_tail(headers.body_bytes)?;
                (
                    Phase::Body,
                    headers.at.checked_add(body_tail.max(first_byte_tail))?,
                )
            }
            (Phase::FirstByte, None) => (
                Phase::FirstByte,
                self.primary.started.get()?.checked_add(first_byte_tail)?,
            ),
            (Phase::Body, None) => return None,
        };
        (at < self.deadline).then_some((phase, at))
    }

    /// Completes when a hedge is due, or never if none can be.
    pub async fn due(&mut self) {
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
        object_store::stats::{BodySample, BucketedStats, LatencyModel, Outcome, Sample},
        types::BucketName,
    };

    const MIB: u64 = 1024 * 1024;

    fn model(first_byte_ms: u64, body_bytes: u64, body_ms: u64) -> LatencyModel {
        let stats = BucketedStats::default();
        let observation = stats.begin(&BucketName::new("bucket").unwrap(), None);
        observation.record(&Sample {
            first_byte: Duration::from_millis(first_byte_ms),
            body: Some(BodySample {
                bytes: body_bytes,
                elapsed: Duration::from_millis(body_ms),
            }),
        });
        let model = observation.model();
        observation.complete(Outcome::Success);
        model
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
        let mut timer = HedgeTimer::new(model(20, 4096, 3), &timeline, deadline());
        let start = Instant::now();
        tokio::join!(
            async {
                sleep(Duration::from_millis(50)).await;
                timeline.start(Instant::now());
            },
            timer.due(),
        );
        assert_eq!(start.elapsed(), Duration::from_millis(70));
    }

    #[tokio::test(start_paused = true)]
    async fn a_hedge_not_taken_before_headers_can_be_taken_during_the_body() {
        let timeline = RequestTimeline::default();
        let mut timer = HedgeTimer::new(model(20, 4096, 3), &timeline, deadline());
        timeline.start(Instant::now());
        assert_eq!(due_after(&mut timer).await, Some(Duration::from_millis(20)));
        sleep(Duration::from_millis(10)).await;
        timeline.receive_headers(Instant::now(), 4096);
        assert_eq!(due_after(&mut timer).await, Some(Duration::from_millis(20)));
        assert_eq!(due_after(&mut timer).await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn bodies_are_hedged_by_their_announced_size_class() {
        for (large_body_ms, expected) in [(None, None), (Some(250), Some(Duration::from_secs(1)))] {
            let model = match large_body_ms {
                None => model(20, 4096, 3),
                Some(body_ms) => model(20, 4 * MIB, body_ms),
            };
            let timeline = RequestTimeline::default();
            let mut timer = HedgeTimer::new(model, &timeline, deadline());
            timeline.start(Instant::now());
            timeline.receive_headers(Instant::now(), 16 * MIB);
            assert_eq!(due_after(&mut timer).await, expected);
        }
    }
}
