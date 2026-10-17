use aws_sdk_s3::{
    config::{
        ConfigBag, Intercept, RuntimeComponents,
        interceptors::{
            BeforeDeserializationInterceptorContextRef, BeforeTransmitInterceptorContextRef,
        },
    },
    error::BoxError,
};
use parking_lot::Mutex;
use tokio::time::Instant;

use crate::{object_store::stats::BucketedStats, types::BucketName};

/// Records each attempt's time from transmitting the request to receiving successful response
/// headers, excluding SDK retry backoff.
#[derive(Debug)]
pub(super) struct FirstByteObserver {
    stats: BucketedStats,
    bucket: BucketName,
    transmitted_at: Mutex<Option<Instant>>,
}

impl FirstByteObserver {
    pub fn new(stats: BucketedStats, bucket: BucketName) -> Self {
        Self {
            stats,
            bucket,
            transmitted_at: Mutex::new(None),
        }
    }
}

impl Intercept for FirstByteObserver {
    fn name(&self) -> &'static str {
        "FirstByteObserver"
    }

    fn read_before_transmit(
        &self,
        _context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        *self.transmitted_at.lock() = Some(Instant::now());
        Ok(())
    }

    fn read_after_transmit(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _runtime_components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        if let Some(transmitted_at) = self.transmitted_at.lock().take()
            && context.response().status().is_success()
        {
            self.stats
                .observe_first_byte(&self.bucket, transmitted_at.elapsed());
        }
        Ok(())
    }
}
