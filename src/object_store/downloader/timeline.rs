use std::{sync::OnceLock, time::Duration};

use tokio::{
    select,
    sync::Notify,
    time::{Instant, sleep_until},
};

use crate::object_store::stats::{LatencyModel, Sample};

#[derive(Debug, Clone, Copy)]
struct Headers {
    at: Instant,
    body_bytes: u64,
}

/// Request timing and body length from validated headers.
#[derive(Debug, Default)]
pub(super) struct RequestTimeline {
    started: OnceLock<Instant>,
    headers: OnceLock<Headers>,
    changed: Notify,
}

impl RequestTimeline {
    pub fn start(&self) -> Instant {
        let now = Instant::now();
        if self.started.set(now).is_ok() {
            self.changed.notify_waiters();
        }
        now
    }

    pub fn receive_headers(&self, body_bytes: u64) {
        let headers = Headers {
            at: Instant::now(),
            body_bytes,
        };
        if self.headers.set(headers).is_ok() {
            self.changed.notify_waiters();
        }
    }

    pub fn announced_body_bytes(&self) -> Option<u64> {
        self.headers.get().map(|headers| headers.body_bytes)
    }

    pub fn sample(&self) -> Option<Sample> {
        let started = *self.started.get()?;
        let headers = self.headers.get()?;
        let now = Instant::now();
        Some(Sample {
            first_byte: headers.at.saturating_duration_since(started),
            body_bytes: headers.body_bytes,
            body: now.saturating_duration_since(headers.at),
        })
    }
}

/// A deadline shared by a page's copies to one bucket, starting at first admission.
#[derive(Debug, Clone, Copy)]
pub(super) struct BucketDeadline<'a> {
    page: Instant,
    admitted: &'a OnceLock<Instant>,
}

impl<'a> BucketDeadline<'a> {
    pub fn new(page: Instant, admitted: &'a OnceLock<Instant>) -> Self {
        Self { page, admitted }
    }

    pub fn admitted(&self) -> Option<Instant> {
        self.admitted.get().copied()
    }

    /// The bucket's deadline once a copy is admitted, or else the page deadline.
    pub fn current(&self) -> Instant {
        self.admitted().unwrap_or(self.page)
    }

    /// First admission starts the bucket timeout, capped by the page deadline.
    pub fn admit(&self, now: Instant, bucket_timeout: Duration) -> Instant {
        *self
            .admitted
            .get_or_init(|| now + bucket_timeout.min(self.page.saturating_duration_since(now)))
    }
}

#[derive(Debug, Clone, Copy)]
enum Phase {
    FirstByte,
    Body,
}

/// Times hedges before headers and during the body. Each phase fires at most once,
/// allowing a body hedge if admission or budget blocked the header hedge.
pub(super) struct HedgeTimer<'a> {
    model: LatencyModel,
    primary: &'a RequestTimeline,
    deadline: BucketDeadline<'a>,
    next: Option<Phase>,
}

impl<'a> HedgeTimer<'a> {
    pub fn new(
        model: LatencyModel,
        primary: &'a RequestTimeline,
        deadline: BucketDeadline<'a>,
        enabled: bool,
    ) -> Self {
        let next =
            (enabled && model.first_byte_hedge_delay().is_some()).then_some(Phase::FirstByte);
        Self {
            model,
            primary,
            deadline,
            next,
        }
    }

    pub fn disarm(&mut self) {
        self.next = None;
    }

    fn due_at(&self, headers: Option<&Headers>) -> Option<Instant> {
        let next = self.next?;
        let first_byte_delay = self.model.first_byte_hedge_delay()?;
        let (since, tail) = match (next, headers) {
            (_, Some(headers)) => (headers.at, self.model.body_hedge_delay(headers.body_bytes)?),
            (Phase::FirstByte, None) => (*self.primary.started.get()?, first_byte_delay),
            (Phase::Body, None) => return None,
        };
        if tail.is_zero() {
            return None;
        }
        let at = since.checked_add(tail)?;
        (at < self.deadline.current()).then_some(at)
    }

    /// Waits until a hedge is due; stays pending once hedging is no longer possible.
    pub async fn due(&mut self) {
        if self.next.is_none() {
            return std::future::pending().await;
        }
        let primary = self.primary;
        loop {
            let changed = primary.changed.notified();
            let headers = primary.headers.get();
            let settled = headers.is_some();
            let Some(at) = self.due_at(headers) else {
                if settled {
                    return std::future::pending().await;
                }
                changed.await;
                continue;
            };
            select! {
                biased;
                () = changed, if !settled => {}
                () = sleep_until(at) => {
                    let now = Instant::now();
                    if now >= self.deadline.current() {
                        self.next = None;
                        return std::future::pending().await;
                    }
                    self.next = (!settled).then_some(Phase::Body);
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::OnceLock, time::Duration};

    use bytesize::MIB;
    use tokio::time::{Instant, advance, sleep, timeout};

    use super::{BucketDeadline, HedgeTimer, RequestTimeline};
    use crate::{
        object_store::stats::{BucketedStats, LatencyModel, Sample},
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

    static UNADMITTED: OnceLock<Instant> = OnceLock::new();

    fn unadmitted_timer(model: LatencyModel, timeline: &RequestTimeline) -> HedgeTimer<'_> {
        HedgeTimer::new(
            model,
            timeline,
            BucketDeadline::new(deadline(), &UNADMITTED),
            true,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn first_byte_phase_is_timed_from_the_request_start() {
        let timeline = RequestTimeline::default();
        let mut timer = unadmitted_timer(model(20, 4096, 3), &timeline);
        let start = Instant::now();
        tokio::join!(
            async {
                sleep(Duration::from_millis(50)).await;
                timeline.start();
            },
            timer.due(),
        );
        assert_eq!(start.elapsed(), Duration::from_millis(70));
    }

    #[tokio::test(start_paused = true)]
    async fn a_hedge_not_taken_before_headers_can_be_taken_during_the_body() {
        let timeline = RequestTimeline::default();
        let mut timer = unadmitted_timer(model(20, 4096, 3), &timeline);
        timeline.start();
        assert_eq!(due_after(&mut timer).await, Some(Duration::from_millis(20)));
        sleep(Duration::from_millis(10)).await;
        timeline.receive_headers(4096);
        assert_eq!(due_after(&mut timer).await, Some(Duration::from_millis(3)));
        assert_eq!(due_after(&mut timer).await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_zero_first_byte_tail_still_times_the_body() {
        let timeline = RequestTimeline::default();
        let mut timer = unadmitted_timer(model(0, 4096, 30), &timeline);
        timeline.start();
        let (due, ()) = tokio::join!(due_after(&mut timer), async {
            sleep(Duration::from_millis(5)).await;
            timeline.receive_headers(4096);
        });
        assert_eq!(due, Some(Duration::from_millis(35)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_timer_rounded_past_the_deadline_does_not_hedge() {
        advance(Duration::from_micros(500)).await;
        let timeline = RequestTimeline::default();
        let deadline = Instant::now() + Duration::from_micros(20_002);
        let mut timer = HedgeTimer::new(
            model(20, 4096, 3),
            &timeline,
            BucketDeadline::new(deadline, &UNADMITTED),
            true,
        );
        timeline.start();
        assert_eq!(due_after(&mut timer).await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_body_too_long_to_time_is_not_hedged() {
        let timeline = RequestTimeline::default();
        let mut timer = unadmitted_timer(model(20, 4 * MIB, 5_000_000_000), &timeline);
        timeline.start();
        timeline.receive_headers(u64::MAX);
        assert_eq!(due_after(&mut timer).await, None);
    }
}
