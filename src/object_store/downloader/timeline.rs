use std::{
    sync::{
        OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::{
    select,
    time::{Instant, sleep_until},
};

use crate::object_store::stats::{Headers, LatencyModel, RequestTimeline};

/// A deadline shared by a download's copies to one bucket, starting at first admission.
#[derive(Debug)]
pub(super) struct BucketDeadline {
    download: Instant,
    expires_at: OnceLock<Instant>,
    expiry_reported: AtomicBool,
}

impl BucketDeadline {
    pub fn new(download: Instant) -> Self {
        Self {
            download,
            expires_at: OnceLock::new(),
            expiry_reported: AtomicBool::new(false),
        }
    }

    pub fn download(&self) -> Instant {
        self.download
    }

    /// Set once a copy is admitted.
    pub fn expires_at(&self) -> Option<Instant> {
        self.expires_at.get().copied()
    }

    /// First admission starts the bucket timeout, capped by the download deadline.
    pub fn admit(&self, now: Instant, bucket_timeout: Duration) -> Instant {
        *self
            .expires_at
            .get_or_init(|| now + bucket_timeout.min(self.download.saturating_duration_since(now)))
    }

    /// Whether this is the first report of the deadline's expiry.
    pub fn report_expiry(&self) -> bool {
        !self.expiry_reported.swap(true, Ordering::Relaxed)
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
    requested_bytes: u64,
    next: Option<Phase>,
}

impl<'a> HedgeTimer<'a> {
    pub fn new(
        model: LatencyModel,
        primary: &'a RequestTimeline,
        requested_bytes: u64,
        enabled: bool,
    ) -> Self {
        Self {
            model,
            primary,
            requested_bytes,
            next: enabled.then_some(Phase::FirstByte),
        }
    }

    pub fn disarm(&mut self) {
        self.next = None;
    }

    /// Without an estimate for the current phase, waits for the next one.
    fn due_at(&self, headers: Option<Headers>) -> Option<Instant> {
        let (since, delay) = match (self.next?, headers) {
            (_, Some(headers)) => (headers.at, self.model.body.hedge_delay(headers.body_bytes)?),
            (Phase::FirstByte, None) => (
                self.primary.started()?,
                self.model.first_byte.hedge_delay(self.requested_bytes)?,
            ),
            (Phase::Body, None) => return None,
        };
        since.checked_add(delay)
    }

    /// Waits until a hedge is due; stays pending once no phase remains to time.
    pub async fn due(&mut self) {
        if self.next.is_none() {
            return std::future::pending().await;
        }
        let primary = self.primary;
        loop {
            let headers = primary.headers();
            let settled = headers.is_some();
            let Some(at) = self.due_at(headers) else {
                if settled {
                    return std::future::pending().await;
                }
                primary.changed().await;
                continue;
            };
            select! {
                biased;
                () = primary.changed(), if !settled => {}
                () = sleep_until(at) => {
                    self.next = (!settled).then_some(Phase::Body);
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytesize::MIB;
    use tokio::time::{Instant, sleep, timeout};

    use super::HedgeTimer;
    use crate::{
        object_store::stats::{BucketedStats, LatencyModel, RequestTimeline, Sample},
        types::BucketName,
    };

    fn seeded(samples: impl IntoIterator<Item = Sample>) -> LatencyModel {
        let stats = BucketedStats::default();
        let bucket = BucketName::new("bucket").unwrap();
        for sample in samples {
            stats.seed(&bucket, sample);
        }
        stats.model(&bucket)
    }

    fn model(first_byte_ms: u64, body_bytes: u64, body_ms: u64) -> LatencyModel {
        seeded([Sample::from_millis(first_byte_ms, body_bytes, body_ms)])
    }

    async fn due_after(timer: &mut HedgeTimer<'_>) -> Option<Duration> {
        let start = Instant::now();
        timeout(Duration::from_secs(60), timer.due())
            .await
            .ok()
            .map(|()| start.elapsed())
    }

    fn timer(model: LatencyModel, timeline: &RequestTimeline, bytes: u64) -> HedgeTimer<'_> {
        HedgeTimer::new(model, timeline, bytes, true)
    }

    /// Capacitor reads the range before sending headers.
    #[tokio::test(start_paused = true)]
    async fn header_hedges_scale_with_the_requested_size() {
        let model = seeded(
            [(64 * 1024, 1), (4 * MIB, 5)]
                .into_iter()
                .flat_map(|(bytes, millis)| [Sample::from_millis(millis, bytes, 1); 4]),
        );
        for (bytes, delay) in [(64 * 1024, 1), (4 * MIB, 5)] {
            let timeline = RequestTimeline::default();
            let mut timer = timer(model, &timeline, bytes);
            timeline.start();
            assert_eq!(
                due_after(&mut timer).await,
                Some(Duration::from_millis(delay))
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_unmeasured_request_size_still_times_the_body() {
        let timeline = RequestTimeline::default();
        let mut timer = timer(model(20, 4096, 3), &timeline, MIB);
        timeline.start();
        let (due, ()) = tokio::join!(due_after(&mut timer), async {
            sleep(Duration::from_millis(50)).await;
            timeline.receive_headers(4096);
        });
        assert_eq!(due, Some(Duration::from_millis(53)));
    }

    #[tokio::test(start_paused = true)]
    async fn first_byte_phase_is_timed_from_the_request_start() {
        let timeline = RequestTimeline::default();
        let mut timer = timer(model(20, 4096, 3), &timeline, 4096);
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
        let mut timer = timer(model(20, 4096, 3), &timeline, 4096);
        timeline.start();
        assert_eq!(due_after(&mut timer).await, Some(Duration::from_millis(20)));
        sleep(Duration::from_millis(10)).await;
        timeline.receive_headers(4096);
        assert_eq!(due_after(&mut timer).await, Some(Duration::from_millis(3)));
        assert_eq!(due_after(&mut timer).await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_body_too_long_to_time_is_not_hedged() {
        let timeline = RequestTimeline::default();
        let mut timer = timer(model(20, 4 * MIB, 5_000_000_000), &timeline, 4 * MIB);
        timeline.start();
        timeline.receive_headers(u64::MAX);
        assert_eq!(due_after(&mut timer).await, None);
    }
}
