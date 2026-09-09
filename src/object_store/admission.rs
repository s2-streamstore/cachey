use std::sync::Arc;

use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, timeout_at},
};

use super::{DownloadError, DownloadLimits};

#[derive(Debug)]
pub(super) struct DownloadAdmission {
    requests: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    max_bytes: u64,
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

    pub async fn acquire(
        &self,
        bytes: u64,
        deadline: Instant,
    ) -> Result<DownloadPermit, DownloadError> {
        let bytes = self.byte_permits(bytes)?;
        timeout_at(deadline, async {
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
        let bytes = self.byte_permits(bytes).ok()?;
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

    use tokio::time::Instant;

    use super::DownloadAdmission;
    use crate::object_store::{DownloadError, DownloadLimits};

    #[tokio::test(start_paused = true)]
    async fn waiting_and_cancellation_release_request_and_byte_capacity() {
        let admission = DownloadAdmission::new(DownloadLimits {
            max_inflight_bytes: 8,
            max_inflight_requests: 2,
            ..DownloadLimits::default()
        });
        let first = admission
            .acquire(8, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(admission.try_acquire(1).is_none());
        assert!(matches!(
            admission
                .acquire(4, Instant::now() + Duration::from_millis(10))
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
                .acquire(9, Instant::now() + Duration::from_secs(1))
                .await,
            Err(DownloadError::AdmissionExhausted { .. })
        ));
    }
}
