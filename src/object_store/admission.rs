use std::{sync::Arc, time::Duration};

use parking_lot::Mutex;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, timeout_at},
};

use super::{DownloadError, DownloadLimits};

const INTERVAL_ALPHA: f64 = 0.1;
/// A queue younger than this is absorbing a burst rather than standing.
const BURST_WINDOW: Duration = Duration::from_millis(100);
/// The shortest a copy may wait in a standing queue, so short reads are not shed.
pub(super) const MIN_WAIT: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub(super) struct DownloadAdmission {
    requests: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    max_bytes: u64,
    queue: Mutex<Queue>,
}

/// Copies waiting for admission, which depart when admitted, expired, or cancelled.
#[derive(Debug, Default)]
struct Queue {
    waiting: u32,
    /// When the queue last became non-empty.
    formed_at: Option<Instant>,
    /// Moving average of the time between departures since the queue last emptied.
    interval: Duration,
    last_departure: Option<Instant>,
}

impl Queue {
    /// Each copy ahead, and then this one, takes an interval to depart, counting from the
    /// last departure.
    fn predicted_wait(&self, now: Instant) -> Duration {
        self.last_departure.map_or(Duration::ZERO, |last| {
            self.interval
                .saturating_mul(self.waiting + 1)
                .saturating_sub(now.saturating_duration_since(last))
        })
    }

    fn depart(&mut self, now: Instant) {
        self.waiting -= 1;
        if self.waiting == 0 {
            *self = Self::default();
        } else if let Some(last) = self.last_departure.replace(now) {
            let interval = now.saturating_duration_since(last);
            self.interval = if self.interval.is_zero() {
                interval
            } else {
                self.interval.mul_f64(1.0 - INTERVAL_ALPHA) + interval.mul_f64(INTERVAL_ALPHA)
            };
        }
    }
}

struct Waiting<'a>(&'a Mutex<Queue>);

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.lock().depart(Instant::now());
    }
}

pub(super) struct DownloadPermit {
    _request: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
}

impl DownloadAdmission {
    pub fn new(limits: DownloadLimits) -> Self {
        Self {
            requests: Arc::new(Semaphore::new(limits.max_inflight_requests as usize)),
            bytes: Arc::new(Semaphore::new(limits.max_inflight_bytes as usize)),
            max_bytes: limits.max_inflight_bytes,
            queue: Mutex::default(),
        }
    }

    fn byte_permits(&self, bytes: u64) -> Result<u32, DownloadError> {
        u32::try_from(bytes)
            .ok()
            .filter(|_| bytes <= self.max_bytes)
            .ok_or(DownloadError::AdmissionExhausted {
                requested_bytes: bytes,
                limit_bytes: self.max_bytes.min(u64::from(u32::MAX)),
            })
    }

    /// Queues while a copy can still start by `latest_start`. Copies joining a queue that
    /// is absorbing a burst wait for as long as that allows, since capacity will reach them;
    /// in a standing queue, waiting longer than a copy is `expected` to take only delays it.
    /// A copy that would not be admitted in time fails at once.
    pub async fn acquire(
        &self,
        bytes: u64,
        latest_start: Instant,
        expected: Duration,
    ) -> Result<DownloadPermit, DownloadError> {
        let bytes = self.byte_permits(bytes)?;
        if let Some(permit) = self.try_acquire_permits(bytes) {
            return Ok(permit);
        }
        let (_waiting, latest_start) = {
            let mut queue = self.queue.lock();
            let now = Instant::now();
            let standing = queue
                .formed_at
                .is_some_and(|formed_at| now.saturating_duration_since(formed_at) >= BURST_WINDOW);
            let latest_start = match now.checked_add(expected.max(MIN_WAIT)) {
                Some(patience) if standing => latest_start.min(patience),
                _ => latest_start,
            };
            if now + queue.predicted_wait(now) >= latest_start {
                return Err(DownloadError::AdmissionTimeout);
            }
            queue.formed_at.get_or_insert(now);
            queue.waiting += 1;
            (Waiting(&self.queue), latest_start)
        };
        timeout_at(latest_start, async {
            let request = self
                .requests
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| DownloadError::AdmissionTimeout)?;
            let bytes = self
                .bytes
                .clone()
                .acquire_many_owned(bytes)
                .await
                .map_err(|_| DownloadError::AdmissionTimeout)?;
            Ok(DownloadPermit {
                _request: request,
                _bytes: bytes,
            })
        })
        .await
        .map_err(|_| DownloadError::AdmissionTimeout)?
    }

    pub fn try_acquire(&self, bytes: u64) -> Option<DownloadPermit> {
        self.try_acquire_permits(self.byte_permits(bytes).ok()?)
    }

    fn try_acquire_permits(&self, bytes: u32) -> Option<DownloadPermit> {
        let request = self.requests.clone().try_acquire_owned().ok()?;
        let bytes = self.bytes.clone().try_acquire_many_owned(bytes).ok()?;
        Some(DownloadPermit {
            _request: request,
            _bytes: bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::{Instant, sleep};

    use super::{BURST_WINDOW, DownloadAdmission, MIN_WAIT};
    use crate::object_store::{DownloadError, DownloadLimits};

    /// An expected latency too long to limit any wait.
    const UNHURRIED: Duration = Duration::from_secs(60);

    /// Copies hold the only slot for 100 ms, so queued copies depart 100 ms apart.
    #[tokio::test(start_paused = true)]
    async fn copies_that_would_wait_too_long_fail_without_queueing() {
        let admission = &DownloadAdmission::new(DownloadLimits {
            max_inflight_requests: 1,
            ..DownloadLimits::default()
        });
        let hold = |held, latest_start| async move {
            let permit = admission.acquire(4, latest_start, UNHURRIED).await?;
            sleep(Duration::from_millis(held)).await;
            drop(permit);
            Ok::<_, DownloadError>(())
        };
        let start = Instant::now();
        let late = start + Duration::from_secs(1);
        let (first, second, third, fourth, (refused, refused_after, fifth)) = tokio::join!(
            hold(100, late),
            hold(100, late),
            hold(100, late),
            hold(100, late),
            async {
                sleep(Duration::from_millis(250)).await;
                let arrived = Instant::now();
                // Behind one waiting copy, the slot frees at 400 ms.
                let refused = admission
                    .acquire(4, start + Duration::from_millis(360), UNHURRIED)
                    .await;
                let refused_after = arrived.elapsed();
                let fifth = hold(100, start + Duration::from_millis(420)).await;
                (refused, refused_after, fifth)
            }
        );
        assert!(matches!(refused, Err(DownloadError::AdmissionTimeout)));
        assert_eq!(refused_after, Duration::ZERO);
        for held in [first, second, third, fourth, fifth] {
            held.unwrap();
        }

        // Once the queue empties, its departures no longer predict waits.
        let start = Instant::now();
        let late = start + Duration::from_secs(1);
        let (first, second, third) = tokio::join!(
            hold(10, late),
            hold(10, late),
            hold(10, start + Duration::from_millis(50)),
        );
        for held in [first, second, third] {
            held.unwrap();
        }
    }

    /// Copies joining a queue that is absorbing a burst wait as long as their deadline
    /// allows; once the queue has stood for the burst window, a copy joining it waits only
    /// as long as it is expected to take.
    #[tokio::test(start_paused = true)]
    async fn only_a_standing_queue_limits_waits_to_the_expected_latency() {
        let admission = &DownloadAdmission::new(DownloadLimits {
            max_inflight_requests: 1,
            ..DownloadLimits::default()
        });
        let held = admission.try_acquire(4).unwrap();
        let late = Instant::now() + Duration::from_secs(1);
        let expected = Duration::from_millis(10);
        let burst = |joins_after| async move {
            sleep(joins_after).await;
            admission.acquire(4, late, expected).await.map(drop)
        };
        let (first, second, (standing, waited), ()) = tokio::join!(
            burst(Duration::ZERO),
            burst(BURST_WINDOW / 2),
            async {
                sleep(BURST_WINDOW).await;
                let joined = Instant::now();
                let result = admission.acquire(4, late, expected).await;
                (result, joined.elapsed())
            },
            async {
                sleep(Duration::from_millis(300)).await;
                drop(held);
            }
        );
        first.unwrap();
        second.unwrap();
        assert!(matches!(standing, Err(DownloadError::AdmissionTimeout)));
        assert_eq!(waited, MIN_WAIT);
    }

    #[tokio::test(start_paused = true)]
    async fn waiting_and_cancellation_release_request_and_byte_capacity() {
        let admission = DownloadAdmission::new(DownloadLimits {
            max_inflight_bytes: 8,
            max_inflight_requests: 2,
            ..DownloadLimits::default()
        });
        let first = admission
            .acquire(8, Instant::now() + Duration::from_secs(1), UNHURRIED)
            .await
            .unwrap();
        assert!(admission.try_acquire(1).is_none());
        assert!(matches!(
            admission
                .acquire(4, Instant::now() + Duration::from_millis(10), UNHURRIED)
                .await,
            Err(DownloadError::AdmissionTimeout)
        ));
        drop(first);
        let first = admission.try_acquire(4).unwrap();
        let second = admission.try_acquire(4).unwrap();
        assert!(admission.try_acquire(1).is_none());
        drop((first, second));
        assert!(admission.try_acquire(8).is_some());
        assert!(matches!(
            admission
                .acquire(9, Instant::now() + Duration::from_secs(1), UNHURRIED)
                .await,
            Err(DownloadError::AdmissionExhausted { .. })
        ));
    }
}
