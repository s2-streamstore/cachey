use std::sync::{Arc, OnceLock};

use aws_sdk_s3::{
    config::{
        ConfigBag, Intercept, RuntimeComponents,
        interceptors::{
            BeforeDeserializationInterceptorContextRef, BeforeSerializationInterceptorContextRef,
        },
    },
    error::BoxError,
};
use parking_lot::Mutex;
use tokio::{sync::Notify, time::Instant};

use crate::{object_store::stats::BucketedStats, types::BucketName};

/// Signals when a request's successful response headers arrive, with the announced body length.
#[derive(Debug, Default)]
pub(super) struct ResponseHeaders {
    body_bytes: OnceLock<u64>,
    notify: Notify,
}

impl ResponseHeaders {
    fn arrive(&self, body_bytes: u64) {
        if self.body_bytes.set(body_bytes).is_ok() {
            self.notify.notify_one();
        }
    }

    pub async fn arrived(&self) -> u64 {
        loop {
            if let Some(body_bytes) = self.body_bytes.get() {
                return *body_bytes;
            }
            self.notify.notified().await;
        }
    }
}

/// Records the time from starting a request to receiving successful response headers, including
/// SDK retries, matching how hedge and deadline timers measure it.
#[derive(Debug)]
pub(super) struct FirstByteObserver {
    stats: BucketedStats,
    bucket: BucketName,
    requested_bytes: u64,
    headers: Option<Arc<ResponseHeaders>>,
    started: Mutex<Option<Instant>>,
}

impl FirstByteObserver {
    pub fn new(
        stats: BucketedStats,
        bucket: BucketName,
        requested_bytes: u64,
        headers: Option<Arc<ResponseHeaders>>,
    ) -> Self {
        Self {
            stats,
            bucket,
            requested_bytes,
            headers,
            started: Mutex::new(None),
        }
    }
}

impl Intercept for FirstByteObserver {
    fn name(&self) -> &'static str {
        "FirstByteObserver"
    }

    fn read_before_execution(
        &self,
        _context: &BeforeSerializationInterceptorContextRef<'_>,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        *self.started.lock() = Some(Instant::now());
        Ok(())
    }

    fn read_after_transmit(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let response = context.response();
        if response.status().is_success()
            && let Some(started) = self.started.lock().take()
        {
            self.stats
                .observe_first_byte(&self.bucket, started.elapsed());
            if let Some(headers) = &self.headers {
                let body_bytes = response
                    .headers()
                    .get("content-length")
                    .and_then(|length| length.parse().ok())
                    .unwrap_or(self.requested_bytes);
                headers.arrive(body_bytes);
            }
        }
        Ok(())
    }
}
