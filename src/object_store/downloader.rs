use std::{ops::Range, sync::Arc, time::Duration};

use aws_sdk_s3::{
    error::ProvideErrorMetadata,
    operation::get_object::{GetObjectError, GetObjectOutput},
};
use bytes::Bytes;
use http_content_range::ContentRange;
use parking_lot::Mutex;
use tokio::{
    select,
    time::{Instant, sleep_until},
};

use crate::{
    object_store::{
        BucketMetrics,
        config::{DownloadLimits, RequestConfig},
        hedging::HedgeBudget,
        stats::BucketedStats,
    },
    service::SlidingThroughput,
    types::{BucketName, BucketNameSet, ObjectKey},
};

#[derive(Debug, Clone, thiserror::Error)]
pub enum DownloadError {
    #[error("Invalid object state: {0}")]
    InvalidObjectState(String),
    #[error("No such key")]
    NoSuchKey,
    #[error("Invalid range ({requested:?}) for object of size {object_size:?}")]
    RangeNotSatisfied {
        requested: Range<u64>,
        object_size: Option<u64>,
    },
    #[error("Body streaming: {0}")]
    BodyStreaming(String),
    #[error("Bucket {bucket} download exceeded its {timeout:?} budget")]
    Timeout {
        bucket: BucketName,
        timeout: Duration,
    },
    #[error("Unknown error: {0}")]
    Unknown(String),
}

impl DownloadError {
    fn should_attempt_fallback_bucket(&self) -> bool {
        match self {
            Self::RangeNotSatisfied { .. } => false,
            Self::InvalidObjectState(_)
            | Self::NoSuchKey
            | Self::BodyStreaming(_)
            | Self::Timeout { .. }
            | Self::Unknown(_) => true,
        }
    }

    fn should_wait_for_hedged_peer(&self) -> bool {
        match self {
            Self::BodyStreaming(_) | Self::Unknown(_) => true,
            Self::InvalidObjectState(_)
            | Self::NoSuchKey
            | Self::RangeNotSatisfied { .. }
            | Self::Timeout { .. } => false,
        }
    }
}

type GetObjectResult = Result<GetObjectOutput, Box<aws_sdk_s3::error::SdkError<GetObjectError>>>;

fn invalid_range_object_size(error: &aws_sdk_s3::error::SdkError<GetObjectError>) -> Option<u64> {
    error
        .raw_response()
        .and_then(|response| response.headers().get("content-range"))
        .and_then(ContentRange::parse)
        .and_then(|content_range| match content_range {
            ContentRange::Unsatisfied(range) => Some(range.complete_length),
            ContentRange::Bytes(_) | ContentRange::UnboundBytes(_) => None,
        })
}

fn map_get_object_error(
    req_range: &Range<u64>,
    object_size: Option<u64>,
    error: GetObjectError,
) -> DownloadError {
    match error {
        GetObjectError::InvalidObjectState(invalid_object_state) => {
            DownloadError::InvalidObjectState(invalid_object_state.message.unwrap_or_default())
        }
        GetObjectError::NoSuchKey(_) => DownloadError::NoSuchKey,
        service_error if service_error.code() == Some("InvalidRange") => {
            DownloadError::RangeNotSatisfied {
                requested: req_range.clone(),
                object_size,
            }
        }
        other => DownloadError::Unknown(format!("{other:?}")),
    }
}

#[derive(Debug, Clone)]
pub struct ObjectPiece {
    pub mtime: u32,
    pub data: Bytes,
    pub object_size: u64,
}

#[derive(Debug, Clone)]
pub struct DownloadOutput {
    pub piece: ObjectPiece,
    pub primary_bucket_idx: usize,
    pub secondary_bucket_idx: Option<usize>,
    pub used_bucket_idx: usize,
    /// Time to validated page data, including SDK retries, hedging, and bucket fallback.
    pub latency: Duration,
    /// Whether a hedge was started in either bucket attempt, regardless of which won.
    pub hedged: bool,
}

struct BucketAttempt {
    result: Result<ObjectPiece, DownloadError>,
    hedged: bool,
}

#[derive(Debug, Clone)]
pub struct Downloader {
    s3: aws_sdk_s3::Client,
    bucketed_stats: BucketedStats,
    throughput: Arc<Mutex<SlidingThroughput>>,
    limits: DownloadLimits,
    hedge_budget: HedgeBudget,
}

impl Downloader {
    pub fn new(
        s3: aws_sdk_s3::Client,
        hedge_latency_quantile: f64,
        throughput: Arc<Mutex<SlidingThroughput>>,
    ) -> Self {
        let limits = DownloadLimits::default();
        Self {
            s3,
            bucketed_stats: BucketedStats::new(hedge_latency_quantile),
            throughput,
            limits,
            hedge_budget: HedgeBudget::new(
                limits.max_concurrent_hedges,
                limits.hedge_budget_percent,
            ),
        }
    }

    /// Configure limits before sharing the downloader with other requests.
    pub fn with_limits(mut self, limits: DownloadLimits) -> eyre::Result<Self> {
        limits.validate()?;
        self.limits = limits;
        self.hedge_budget =
            HedgeBudget::new(limits.max_concurrent_hedges, limits.hedge_budget_percent);
        Ok(self)
    }

    pub fn observe_bucket_metrics(&self, f: impl FnMut(&BucketName, &BucketMetrics)) {
        self.bucketed_stats.export_bucket_metrics(f);
    }

    /// # Panics
    ///
    /// if `byterange.start > byterange.end`
    pub async fn download(
        &self,
        buckets: &BucketNameSet,
        object: ObjectKey,
        byterange: &Range<u64>,
        req_config: &RequestConfig,
    ) -> Result<DownloadOutput, DownloadError> {
        assert!(byterange.start < byterange.end);
        let start_time = Instant::now();
        let page_deadline = start_time
            .checked_add(self.limits.page_timeout)
            .ok_or_else(|| {
                DownloadError::Unknown(
                    "Page timeout exceeds the clock's supported range".to_owned(),
                )
            })?;
        let mut attempt_order = self.bucketed_stats.attempt_order(buckets.iter());
        let primary_bucket_idx = attempt_order.next().expect("non-empty");
        let secondary_bucket_idx = attempt_order.next();
        let primary_budget = self
            .limits
            .bucket_timeout
            .min(if secondary_bucket_idx.is_some() {
                self.limits.page_timeout / 2
            } else {
                self.limits.page_timeout
            });
        let primary = self
            .attempt(
                &buckets[primary_bucket_idx],
                &object,
                byterange,
                req_config,
                start_time + primary_budget,
            )
            .await;
        let mut hedged = primary.hedged;
        let (piece, secondary_bucket_idx, used_bucket_idx) =
            match (primary.result, secondary_bucket_idx) {
                (Ok(piece), secondary_bucket_idx) => {
                    (piece, secondary_bucket_idx, primary_bucket_idx)
                }
                (Err(e), Some(secondary_bucket_idx))
                    if e.should_attempt_fallback_bucket() && Instant::now() < page_deadline =>
                {
                    let now = Instant::now();
                    let deadline = now
                        + self
                            .limits
                            .bucket_timeout
                            .min(page_deadline.saturating_duration_since(now));
                    let secondary = self
                        .attempt(
                            &buckets[secondary_bucket_idx],
                            &object,
                            byterange,
                            req_config,
                            deadline,
                        )
                        .await;
                    hedged |= secondary.hedged;
                    (
                        secondary.result?,
                        Some(secondary_bucket_idx),
                        secondary_bucket_idx,
                    )
                }
                (Err(e), _) => return Err(e),
            };
        Ok(DownloadOutput {
            piece,
            primary_bucket_idx,
            secondary_bucket_idx,
            used_bucket_idx,
            latency: start_time.elapsed(),
            hedged,
        })
    }

    async fn attempt(
        &self,
        bucket: &BucketName,
        object: &ObjectKey,
        byterange: &Range<u64>,
        req_config: &RequestConfig,
        deadline: Instant,
    ) -> BucketAttempt {
        let start_time = Instant::now();
        let timeout = deadline.saturating_duration_since(start_time);
        if timeout.is_zero() {
            return BucketAttempt {
                result: Err(DownloadError::Timeout {
                    bucket: bucket.clone(),
                    timeout,
                }),
                hedged: false,
            };
        }
        let mut hedged = false;
        let result = select! {
            biased;
            () = sleep_until(deadline) => Err(DownloadError::Timeout { bucket: bucket.clone(), timeout }),
            result = async {
                let mut primary_attempt = Box::pin(self.fetch_piece(bucket, object, byterange, req_config));
                select! {
                    biased;
                    primary_result = &mut primary_attempt => primary_result,
                    _ = self.hedge_trigger(bucket, start_time) => {
                        let mut hedge_attempt = Box::pin(async {
                            if Instant::now() >= deadline {
                                return None;
                            }
                            let _permit = self.hedge_budget.try_acquire(bucket)?;
                            hedged = true;
                            let hedge_config = RequestConfig {
                                max_attempts: Some(1),
                                ..req_config.clone()
                            };
                            Some(self.fetch_piece(bucket, object, byterange, &hedge_config).await)
                        });
                        select! {
                            primary_result = &mut primary_attempt => match primary_result {
                                Ok(piece) => Ok(piece),
                                Err(error) if error.should_wait_for_hedged_peer() => {
                                    hedge_attempt.await.unwrap_or(Err(error))
                                },
                                Err(error) => Err(error),
                            },
                            hedge_result = &mut hedge_attempt => match hedge_result {
                                Some(Ok(piece)) => Ok(piece),
                                Some(Err(error)) if !error.should_wait_for_hedged_peer() => Err(error),
                                Some(Err(_)) | None => primary_attempt.await,
                            },
                        }
                    }
                }
            } => result,
        };
        let latency = start_time.elapsed();
        self.bucketed_stats.observe(
            bucket.clone(),
            result.as_ref().map(|_| latency).map_err(|_| ()),
        );
        if result.is_ok() {
            self.hedge_budget.observe_success(bucket);
        }
        BucketAttempt { result, hedged }
    }

    async fn fetch_piece(
        &self,
        bucket: &BucketName,
        object: &ObjectKey,
        byterange: &Range<u64>,
        req_config: &RequestConfig,
    ) -> Result<ObjectPiece, DownloadError> {
        let result = self
            .attempt_inner(bucket, object, byterange, req_config)
            .await;
        self.handle_result(byterange, result).await
    }

    async fn attempt_inner(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        byterange: &Range<u64>,
        req_config: &RequestConfig,
    ) -> GetObjectResult {
        let request = self
            .s3
            .get_object()
            .bucket(&**bucket)
            .key(&**key)
            .range(format!("bytes={}-{}", byterange.start, byterange.end - 1))
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled);

        if req_config.is_noop() {
            request.send().await.map_err(Box::new)
        } else {
            let client_config = self.s3.config();
            let mut config_override = client_config.to_builder();
            if let Some(timeout_config) =
                req_config.merged_timeout_config(client_config.timeout_config())
            {
                config_override = config_override.timeout_config(timeout_config);
            }
            if let Some(retry_config) = req_config.merged_retry_config(client_config.retry_config())
            {
                config_override = config_override.retry_config(retry_config);
            }
            if let Some(force_path_style) = req_config.force_path_style {
                config_override = config_override.force_path_style(force_path_style);
            }

            request
                .customize()
                .config_override(config_override)
                .send()
                .await
                .map_err(Box::new)
        }
    }

    async fn handle_result(
        &self,
        req_range: &Range<u64>,
        result: GetObjectResult,
    ) -> Result<ObjectPiece, DownloadError> {
        match result {
            Ok(output) => {
                async {
                    let content_range = match output.content_range().and_then(ContentRange::parse) {
                        Some(ContentRange::Bytes(rsp_range)) => {
                            let requested_last_byte = req_range.end - 1;
                            if rsp_range.first_byte != req_range.start {
                                return Err(DownloadError::RangeNotSatisfied {
                                    requested: req_range.clone(),
                                    object_size: Some(rsp_range.complete_length),
                                });
                            }
                            let is_exact_range_match = rsp_range.last_byte == requested_last_byte;
                            let is_truncated_at_eof = rsp_range.last_byte < requested_last_byte
                                && rsp_range.last_byte
                                    == rsp_range.complete_length.saturating_sub(1);
                            if !is_exact_range_match && !is_truncated_at_eof {
                                return Err(DownloadError::RangeNotSatisfied {
                                    requested: req_range.clone(),
                                    object_size: Some(rsp_range.complete_length),
                                });
                            }
                            rsp_range
                        }
                        Some(ContentRange::Unsatisfied(r)) => {
                            return Err(DownloadError::RangeNotSatisfied {
                                requested: req_range.clone(),
                                object_size: Some(r.complete_length),
                            });
                        }
                        Some(ContentRange::UnboundBytes(_)) | None => {
                            return Err(DownloadError::RangeNotSatisfied {
                                requested: req_range.clone(),
                                object_size: None,
                            });
                        }
                    };
                    let expected_data_len = content_range.last_byte - content_range.first_byte + 1;
                    let object_size = content_range.complete_length;
                    let mtime = output
                        .last_modified()
                        .and_then(|dt| dt.secs().try_into().ok())
                        .unwrap_or(0);
                    let data = output
                        .body
                        .collect()
                        .await
                        .map_err(|e| DownloadError::BodyStreaming(e.to_string()))?
                        .into_bytes();
                    self.throughput.lock().record(data.len());
                    if data.len() as u64 != expected_data_len {
                        return Err(DownloadError::BodyStreaming(format!(
                            "Expected {} bytes, got {}",
                            expected_data_len,
                            data.len()
                        )));
                    }
                    Ok(ObjectPiece {
                        mtime,
                        data,
                        object_size,
                    })
                }
                .await
            }
            Err(e) => {
                let object_size = invalid_range_object_size(&e);
                Err(map_get_object_error(
                    req_range,
                    object_size,
                    e.into_service_error(),
                ))
            }
        }
    }

    async fn hedge_trigger(&self, bucket: &BucketName, start_time: Instant) -> Option<Duration> {
        let threshold = self.bucketed_stats.hedging_threshold(bucket, start_time);
        if threshold > Duration::ZERO {
            let wait_time = threshold.saturating_sub(start_time.elapsed());
            if wait_time > Duration::ZERO {
                tokio::time::sleep(wait_time).await;
            }
            Some(threshold)
        } else {
            // No data yet, no backup request
            std::future::pending::<()>().await;
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::{
        error::ErrorMetadata,
        operation::get_object::{GetObjectError, GetObjectOutput},
        primitives::{DateTime, SdkBody},
    };
    use aws_smithy_runtime_api::{client::orchestrator::HttpResponse, http::StatusCode};
    use bytes::Bytes;

    use super::*;

    fn make_test_downloader() -> Downloader {
        // Create a dummy S3 client for testing
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test", "test", None, None, "test",
            ))
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .build();
        let client = aws_sdk_s3::Client::from_conf(config);
        let throughput = Arc::new(Mutex::new(crate::service::SlidingThroughput::default()));
        Downloader::new(client, 0.9, throughput)
    }

    #[tokio::test]
    async fn test_handle_result_success() {
        let downloader = make_test_downloader();
        let req_range = Range { start: 0, end: 10 };

        // Create a mock successful response
        let test_data = b"0123456789";
        let output = GetObjectOutput::builder()
            .content_range("bytes 0-9/100")
            .last_modified(DateTime::from_secs(1_234_567_890))
            .body(aws_sdk_s3::primitives::ByteStream::from(test_data.to_vec()))
            .build();

        let result = downloader
            .handle_result(&req_range, Ok(output))
            .await
            .unwrap();

        assert_eq!(result.data, Bytes::from(test_data.to_vec()));
        assert_eq!(result.object_size, 100);
        assert_eq!(result.mtime, 1_234_567_890);
    }

    #[tokio::test]
    async fn test_handle_result_range_mismatch() {
        let downloader = make_test_downloader();
        let req_range = Range { start: 10, end: 20 };

        // Response with mismatched start byte
        let output = GetObjectOutput::builder()
            .content_range("bytes 0-9/100")
            .body(aws_sdk_s3::primitives::ByteStream::from(vec![0; 10]))
            .build();

        let result = downloader.handle_result(&req_range, Ok(output)).await;

        match result {
            Err(DownloadError::RangeNotSatisfied {
                requested,
                object_size,
            }) => {
                assert_eq!(requested, req_range);
                assert_eq!(object_size, Some(100));
            }
            _ => panic!("Expected RangeNotSatisfied error"),
        }
    }

    #[tokio::test]
    async fn test_handle_result_rejects_oversized_response_ending_at_object_eof() {
        let downloader = make_test_downloader();
        let req_range = Range { start: 0, end: 10 };

        let output = GetObjectOutput::builder()
            .content_range("bytes 0-99/100")
            .body(aws_sdk_s3::primitives::ByteStream::from(vec![0; 100]))
            .build();

        let result = downloader.handle_result(&req_range, Ok(output)).await;

        match result {
            Err(DownloadError::RangeNotSatisfied {
                requested,
                object_size,
            }) => {
                assert_eq!(requested, req_range);
                assert_eq!(object_size, Some(100));
            }
            other => panic!("Expected RangeNotSatisfied error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_handle_result_accepts_truncated_at_eof() {
        let downloader = make_test_downloader();
        let req_range = Range { start: 0, end: 10 };

        let output = GetObjectOutput::builder()
            .content_range("bytes 0-4/5")
            .last_modified(DateTime::from_secs(1_234_567_890))
            .body(aws_sdk_s3::primitives::ByteStream::from(vec![0; 5]))
            .build();

        let piece = downloader
            .handle_result(&req_range, Ok(output))
            .await
            .expect("valid EOF truncation should be accepted");

        assert_eq!(piece.data, Bytes::from(vec![0; 5]));
        assert_eq!(piece.object_size, 5);
        assert_eq!(piece.mtime, 1_234_567_890);
    }

    #[tokio::test]
    async fn test_handle_result_no_such_key() {
        let downloader = make_test_downloader();
        let req_range = Range { start: 0, end: 10 };

        let error = aws_sdk_s3::operation::get_object::GetObjectError::NoSuchKey(
            aws_sdk_s3::types::error::NoSuchKey::builder()
                .message("The specified key does not exist.")
                .build(),
        );

        let sdk_error = aws_sdk_s3::error::SdkError::service_error(
            error,
            aws_smithy_runtime_api::client::orchestrator::HttpResponse::new(
                aws_smithy_runtime_api::http::StatusCode::try_from(404).unwrap(),
                aws_sdk_s3::primitives::SdkBody::empty(),
            ),
        );

        let result = downloader
            .handle_result(&req_range, Err(Box::new(sdk_error)))
            .await;

        match result {
            Err(DownloadError::NoSuchKey) => {}
            _ => panic!("Expected NoSuchKey error"),
        }
    }

    #[tokio::test]
    async fn test_handle_result_body_length_mismatch() {
        let downloader = make_test_downloader();
        let req_range = Range { start: 0, end: 10 };

        // Create output with content range indicating 10 bytes but only 5 bytes of data
        let output = GetObjectOutput::builder()
            .content_range("bytes 0-9/100")
            .body(aws_sdk_s3::primitives::ByteStream::from(vec![0; 5]))
            .build();

        let result = downloader.handle_result(&req_range, Ok(output)).await;

        match result {
            Err(DownloadError::BodyStreaming(msg)) => {
                assert!(msg.contains("Expected 10 bytes, got 5"));
            }
            _ => panic!("Expected BodyStreaming error"),
        }
    }

    #[tokio::test]
    async fn test_handle_result_invalid_range_service_error() {
        let downloader = make_test_downloader();
        let req_range = Range {
            start: 1024,
            end: 2048,
        };

        let service_error = GetObjectError::generic(
            ErrorMetadata::builder()
                .code("InvalidRange")
                .message("The requested range is not satisfiable")
                .build(),
        );
        let mut response = HttpResponse::new(StatusCode::try_from(416).unwrap(), SdkBody::empty());
        response
            .headers_mut()
            .insert("content-range", "bytes */512");
        let sdk_error = aws_sdk_s3::error::SdkError::service_error(service_error, response);

        let result = downloader
            .handle_result(&req_range, Err(Box::new(sdk_error)))
            .await;

        match result {
            Err(DownloadError::RangeNotSatisfied {
                requested,
                object_size,
            }) => {
                assert_eq!(requested, req_range);
                assert_eq!(object_size, Some(512));
            }
            other => panic!("Expected RangeNotSatisfied error, got {other:?}"),
        }
    }

    #[test]
    fn test_download_error_should_attempt_fallback() {
        // Test which errors should trigger fallback bucket attempts
        assert!(
            DownloadError::InvalidObjectState("test".to_string()).should_attempt_fallback_bucket()
        );
        assert!(DownloadError::NoSuchKey.should_attempt_fallback_bucket());
        assert!(
            !DownloadError::RangeNotSatisfied {
                requested: Range { start: 0, end: 10 },
                object_size: Some(5),
            }
            .should_attempt_fallback_bucket()
        );
        assert!(DownloadError::BodyStreaming("test".to_string()).should_attempt_fallback_bucket());
        assert!(DownloadError::Unknown("test".to_string()).should_attempt_fallback_bucket());
    }

    #[tokio::test]
    async fn test_hedge_trigger_no_data() {
        let downloader = make_test_downloader();
        let bucket = BucketName::new("test-bucket").unwrap();
        let start_time = Instant::now();

        // When there's no data, hedge_trigger should wait forever
        let hedge_future = downloader.hedge_trigger(&bucket, start_time);
        let timeout_future = tokio::time::sleep(Duration::from_millis(10));

        tokio::select! {
            _ = hedge_future => panic!("hedge_trigger should not complete when there's no data"),
            () = timeout_future => {} // Expected: timeout completes first
        }
    }

    #[tokio::test]
    #[should_panic(expected = "assertion failed")]
    async fn test_download_assertion_empty_range() {
        let downloader = make_test_downloader();
        let bucket = BucketName::new("test-bucket").unwrap();
        let buckets = BucketNameSet::new(std::iter::once(bucket)).unwrap();
        let key = ObjectKey::new("test-key").unwrap();

        // This should panic due to assertion
        let _ = downloader
            .download(
                &buckets,
                key,
                &Range { start: 10, end: 10 },
                &RequestConfig::default(),
            )
            .await;
    }

    #[tokio::test]
    async fn test_handle_result_missing_content_range() {
        let downloader = make_test_downloader();
        let req_range = Range { start: 0, end: 10 };

        // Create output without content range header
        let output = GetObjectOutput::builder()
            .body(aws_sdk_s3::primitives::ByteStream::from(vec![0; 10]))
            .build();

        let result = downloader.handle_result(&req_range, Ok(output)).await;

        match result {
            Err(DownloadError::RangeNotSatisfied {
                requested,
                object_size,
            }) => {
                assert_eq!(requested, req_range);
                assert_eq!(object_size, None);
            }
            _ => panic!("Expected RangeNotSatisfied error"),
        }
    }

    #[tokio::test]
    async fn test_handle_result_unsatisfied_range() {
        let downloader = make_test_downloader();
        let req_range = Range {
            start: 100,
            end: 200,
        };

        // Create output with unsatisfied range response
        let output = GetObjectOutput::builder()
            .content_range("bytes */50")
            .body(aws_sdk_s3::primitives::ByteStream::from(vec![]))
            .build();

        let result = downloader.handle_result(&req_range, Ok(output)).await;

        match result {
            Err(DownloadError::RangeNotSatisfied {
                requested,
                object_size,
            }) => {
                assert_eq!(requested, req_range);
                assert_eq!(object_size, Some(50));
            }
            _ => panic!("Expected RangeNotSatisfied error"),
        }
    }
}

#[cfg(test)]
mod latency_tests {
    use std::{
        collections::{HashMap, VecDeque},
        io,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use aws_sdk_s3::{
        config::{Credentials, Region, retry::RetryConfig},
        primitives::SdkBody,
    };
    use aws_smithy_runtime_api::client::{
        http::{HttpConnector, HttpConnectorFuture, SharedHttpConnector, http_client_fn},
        orchestrator::{HttpRequest, HttpResponse},
    };
    use bytes::Bytes;
    use http_body::Frame;
    use http_body_util::StreamBody;
    use parking_lot::Mutex;
    use tokio::time::{advance, sleep, timeout};

    use super::{
        BucketMetrics, DownloadError, DownloadLimits, DownloadOutput, Downloader, RequestConfig,
    };
    use crate::{
        service::SlidingThroughput,
        types::{BucketName, BucketNameSet, ObjectKey},
    };

    // Each response specifies its bucket, header delay, body delay, and body failure.
    type ResponseStep = (&'static str, u64, u64, bool);

    #[derive(Debug)]
    struct ScriptedConnector {
        responses: Arc<Mutex<VecDeque<ResponseStep>>>,
        requests: Arc<AtomicUsize>,
        active: Arc<AtomicUsize>,
        service_errors: Arc<Mutex<HashMap<usize, u16>>>,
    }

    struct ActiveRequest(Arc<AtomicUsize>);

    impl Drop for ActiveRequest {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[derive(Clone)]
    struct ScriptState {
        requests: Arc<AtomicUsize>,
        active: Arc<AtomicUsize>,
        service_errors: Arc<Mutex<HashMap<usize, u16>>>,
    }

    impl HttpConnector for ScriptedConnector {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let request_idx = self.requests.fetch_add(1, Ordering::SeqCst);
            self.active.fetch_add(1, Ordering::SeqCst);
            let active = ActiveRequest(self.active.clone());
            let service_error = self.service_errors.lock().get(&request_idx).copied();
            let (bucket, headers_ms, body_ms, fail_body) = self
                .responses
                .lock()
                .pop_front()
                .expect("unexpected object-store request");
            assert!(
                request.uri().contains(&format!("/{bucket}/")),
                "{}",
                request.uri()
            );
            HttpConnectorFuture::new(async move {
                sleep(Duration::from_millis(headers_ms)).await;
                if let Some(status) = service_error {
                    return Ok(HttpResponse::new(
                        status.try_into().unwrap(),
                        SdkBody::from("<Error><Code>SlowDown</Code></Error>"),
                    ));
                }
                let body = StreamBody::new(futures::stream::once(async move {
                    let _active = active;
                    sleep(Duration::from_millis(body_ms)).await;
                    if fail_body {
                        Err(io::Error::other("scripted body failure"))
                    } else {
                        Ok(Frame::data(Bytes::from_static(b"data")))
                    }
                }));
                let mut response =
                    HttpResponse::new(206.try_into().unwrap(), SdkBody::from_body_1_x(body));
                response
                    .headers_mut()
                    .insert("content-range", "bytes 0-3/4");
                response.headers_mut().insert("content-length", "4");
                Ok(response)
            })
        }
    }

    fn downloader(responses: impl IntoIterator<Item = ResponseStep>) -> Downloader {
        scripted_downloader(responses).0
    }

    fn scripted_downloader(
        responses: impl IntoIterator<Item = ResponseStep>,
    ) -> (Downloader, ScriptState) {
        let state = ScriptState {
            requests: Arc::new(AtomicUsize::new(0)),
            active: Arc::new(AtomicUsize::new(0)),
            service_errors: Arc::default(),
        };
        let connector = SharedHttpConnector::new(ScriptedConnector {
            responses: Arc::new(Mutex::new(responses.into_iter().collect())),
            requests: state.requests.clone(),
            active: state.active.clone(),
            service_errors: state.service_errors.clone(),
        });
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .credentials_provider(Credentials::new("test", "test", None, None, "test"))
            .region(Region::new("us-east-1"))
            .endpoint_url("http://s3.test")
            .force_path_style(true)
            .retry_config(RetryConfig::standard().with_max_attempts(1))
            .http_client(http_client_fn(move |_, _| connector.clone()))
            .build();
        let downloader = Downloader::new(
            aws_sdk_s3::Client::from_conf(config),
            0.99,
            Arc::new(Mutex::new(SlidingThroughput::default())),
        );
        (downloader, state)
    }

    async fn fetch(downloader: &Downloader, buckets: &[&str]) -> DownloadOutput {
        fetch_with_config(downloader, buckets, &RequestConfig::default())
            .await
            .unwrap()
    }

    async fn fetch_with_config(
        downloader: &Downloader,
        buckets: &[&str],
        config: &RequestConfig,
    ) -> Result<DownloadOutput, DownloadError> {
        let buckets =
            BucketNameSet::new(buckets.iter().map(|name| BucketName::new(*name).unwrap())).unwrap();
        downloader
            .download(&buckets, ObjectKey::new("object").unwrap(), &(0..4), config)
            .await
    }

    async fn metrics(downloader: &Downloader, bucket: &str) -> BucketMetrics {
        advance(Duration::from_secs(1)).await;
        let mut found = None;
        downloader.observe_bucket_metrics(|name, metrics| {
            if &**name == bucket {
                found = Some(metrics.clone());
            }
        });
        found.expect("bucket observation")
    }

    #[tokio::test(start_paused = true)]
    async fn body_transfer_sets_the_next_hedge_delay() {
        let downloader = downloader([("bucket", 10, 190, false), ("bucket", 10, 190, false)]);
        let first = fetch(&downloader, &["bucket"]).await;
        assert_eq!(first.latency, Duration::from_millis(200));
        assert_eq!(
            metrics(&downloader, "bucket").await.latency_hedge,
            first.latency
        );

        let second = fetch(&downloader, &["bucket"]).await;
        assert!(
            !second.hedged,
            "a normal body transfer should not trigger a hedge"
        );
        assert_eq!(second.piece.data, Bytes::from_static(b"data"));
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_hedge_wins_preserve_wait_and_body_time() {
        let mut responses = vec![("bucket", 10, 90, false)];
        for _ in 0..8 {
            responses.extend([("bucket", 1_000, 0, false), ("bucket", 10, 40, false)]);
        }
        let downloader = downloader(responses)
            .with_limits(DownloadLimits {
                hedge_budget_percent: 100,
                ..DownloadLimits::default()
            })
            .unwrap();
        fetch(&downloader, &["bucket"]).await;
        for _ in 0..8 {
            let threshold = metrics(&downloader, "bucket").await.latency_hedge;
            assert!(threshold >= Duration::from_millis(100));
            let output = fetch(&downloader, &["bucket"]).await;
            assert!(output.hedged);
            assert_eq!(output.latency, threshold + Duration::from_millis(50));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn primary_win_preserves_hedged_flag_and_cancels_pending_hedge() {
        let downloader = downloader([
            ("bucket", 10, 90, false),
            ("bucket", 10, 140, false),
            ("bucket", 10, 190, false),
        ]);
        fetch(&downloader, &["bucket"]).await;

        let output = fetch(&downloader, &["bucket"]).await;
        assert!(output.hedged);
        assert_eq!(output.latency, Duration::from_millis(150));
        assert_eq!(output.piece.data, Bytes::from_static(b"data"));

        let metrics = metrics(&downloader, "bucket").await;
        assert_eq!(metrics.latency_mean, Duration::from_millis(125));
        assert_eq!(metrics.latency_hedge, Duration::from_millis(150));
        assert!(metrics.error_rate.abs() < f64::EPSILON);
        assert_eq!(metrics.consecutive_failures, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn either_peer_can_recover_a_body_failure() {
        for (primary, hedge, elapsed_ms) in [
            (("bucket", 10, 100, true), ("bucket", 10, 20, false), 130),
            (("bucket", 10, 190, false), ("bucket", 5, 10, true), 200),
        ] {
            let downloader = downloader([("bucket", 10, 90, false), primary, hedge]);
            fetch(&downloader, &["bucket"]).await;
            let output = fetch(&downloader, &["bucket"]).await;
            assert!(output.hedged);
            assert_eq!(output.latency, Duration::from_millis(elapsed_ms));
            assert!(metrics(&downloader, "bucket").await.error_rate.abs() < f64::EPSILON);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn fallback_accounts_for_both_buckets_without_double_counting_failure() {
        let downloader = downloader([
            ("primary", 10, 90, false),
            ("primary", 10, 100, true),
            ("primary", 10, 20, true),
            ("fallback", 10, 20, false),
        ]);
        fetch(&downloader, &["primary"]).await;
        let output = fetch(&downloader, &["primary", "fallback"]).await;
        assert_eq!(output.used_bucket_idx, 1);
        assert!(output.hedged, "the failed first bucket started a hedge");
        assert_eq!(output.latency, Duration::from_millis(160));
        let primary = metrics(&downloader, "primary").await;
        assert_eq!(primary.consecutive_failures, 1);
        assert_eq!(primary.latency_mean, Duration::from_millis(100));
        assert_eq!(
            metrics(&downloader, "fallback").await.latency_mean,
            Duration::from_millis(30)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_does_not_record_an_incomplete_bucket_fetch() {
        let downloader = downloader([
            ("bucket", 10, 90, false),
            ("bucket", 1_000, 0, false),
            ("bucket", 1_000, 0, false),
        ]);
        fetch(&downloader, &["bucket"]).await;
        assert!(
            timeout(Duration::from_millis(150), fetch(&downloader, &["bucket"]))
                .await
                .is_err()
        );
        let metrics = metrics(&downloader, "bucket").await;
        assert!(metrics.error_rate.abs() < f64::EPSILON);
        assert_eq!(metrics.latency_mean, Duration::from_millis(100));
    }

    #[tokio::test(start_paused = true)]
    async fn bucket_deadline_covers_bodies_and_reserves_fallback_time() {
        let (downloader, script) = scripted_downloader([
            ("primary", 10, 90, false),
            ("primary", 10, 1_000, false),
            ("primary", 10, 1_000, false),
            ("fallback", 10, 20, false),
        ]);
        let downloader = downloader
            .with_limits(DownloadLimits {
                bucket_timeout: Duration::from_millis(500),
                page_timeout: Duration::from_millis(400),
                ..DownloadLimits::default()
            })
            .unwrap();
        fetch(&downloader, &["primary"]).await;
        let config = RequestConfig {
            operation_timeout: Some(Duration::from_millis(20)),
            operation_attempt_timeout: Some(Duration::from_millis(20)),
            ..RequestConfig::default()
        };
        let output = fetch_with_config(&downloader, &["primary", "fallback"], &config)
            .await
            .unwrap();
        assert_eq!(output.used_bucket_idx, 1);
        assert!(output.hedged);
        assert_eq!(output.latency, Duration::from_millis(230));
        assert_eq!(script.requests.load(Ordering::SeqCst), 4);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
        let primary = metrics(&downloader, "primary").await;
        assert_eq!(primary.consecutive_failures, 1);
        assert_eq!(primary.latency_mean, Duration::from_millis(100));
        assert_eq!(
            metrics(&downloader, "fallback").await.latency_mean,
            Duration::from_millis(30)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn page_deadline_caps_single_bucket_and_fallback() {
        for buckets in [&["primary"][..], &["primary", "fallback"][..]] {
            let (downloader, script) =
                scripted_downloader(buckets.iter().map(|bucket| (*bucket, 10, 1_000, false)));
            let downloader = downloader
                .with_limits(DownloadLimits {
                    bucket_timeout: Duration::from_millis(500),
                    page_timeout: Duration::from_millis(300),
                    ..DownloadLimits::default()
                })
                .unwrap();
            let start = tokio::time::Instant::now();
            let error = fetch_with_config(&downloader, buckets, &RequestConfig::default())
                .await
                .unwrap_err();
            assert!(matches!(error, DownloadError::Timeout { .. }));
            assert_eq!(start.elapsed(), Duration::from_millis(300));
            assert_eq!(script.requests.load(Ordering::SeqCst), buckets.len());
            assert_eq!(script.active.load(Ordering::SeqCst), 0);
            for bucket in buckets {
                let metrics = metrics(&downloader, bucket).await;
                assert_eq!(metrics.consecutive_failures, 1);
                assert_eq!(metrics.latency_mean, Duration::ZERO);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn bucket_deadline_includes_sdk_retries_and_backoff() {
        let (downloader, script) =
            scripted_downloader(std::iter::repeat_n(("bucket", 40, 0, false), 5));
        script
            .service_errors
            .lock()
            .extend((0..5).map(|index| (index, 503)));
        let downloader = downloader
            .with_limits(DownloadLimits {
                bucket_timeout: Duration::from_millis(50),
                ..DownloadLimits::default()
            })
            .unwrap();
        let config = RequestConfig {
            max_attempts: Some(5),
            initial_backoff: Some(Duration::from_secs(1)),
            max_backoff: Some(Duration::from_secs(1)),
            ..RequestConfig::default()
        };
        let start = tokio::time::Instant::now();
        let error = fetch_with_config(&downloader, &["bucket"], &config)
            .await
            .unwrap_err();
        assert!(matches!(error, DownloadError::Timeout { .. }));
        assert_eq!(start.elapsed(), Duration::from_millis(50));
        assert!(script.requests.load(Ordering::SeqCst) <= 2);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
        assert_eq!(metrics(&downloader, "bucket").await.consecutive_failures, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn cold_bucket_headers_are_bounded_without_hedging() {
        let (downloader, script) = scripted_downloader([("bucket", 1_000, 10, false)]);
        let downloader = downloader
            .with_limits(DownloadLimits {
                bucket_timeout: Duration::from_millis(50),
                ..DownloadLimits::default()
            })
            .unwrap();
        let start = tokio::time::Instant::now();
        assert!(matches!(
            fetch_with_config(&downloader, &["bucket"], &RequestConfig::default()).await,
            Err(DownloadError::Timeout { .. })
        ));
        assert_eq!(start.elapsed(), Duration::from_millis(50));
        assert_eq!(script.requests.load(Ordering::SeqCst), 1);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn bucket_deadline_does_not_launch_a_hedge_at_expiry() {
        let (downloader, script) =
            scripted_downloader([("bucket", 10, 90, false), ("bucket", 10, 200, false)]);
        fetch(&downloader, &["bucket"]).await;
        let downloader = downloader
            .with_limits(DownloadLimits {
                bucket_timeout: Duration::from_millis(100),
                ..DownloadLimits::default()
            })
            .unwrap();
        assert!(matches!(
            fetch_with_config(&downloader, &["bucket"], &RequestConfig::default()).await,
            Err(DownloadError::Timeout { .. })
        ));
        assert_eq!(script.requests.load(Ordering::SeqCst), 2);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn shared_slowdown_bounds_hedges_across_downloader_clones() {
        let responses = [("bucket", 10, 90, false)]
            .into_iter()
            .chain(std::iter::repeat_n(("bucket", 10, 490, false), 101));
        let (downloader, script) = scripted_downloader(responses);
        fetch(&downloader, &["bucket"]).await;
        let buckets = BucketNameSet::new([BucketName::new("bucket").unwrap()].into_iter()).unwrap();
        let requests = (0..100).map(|index| {
            let downloader = downloader.clone();
            let buckets = &buckets;
            async move {
                downloader
                    .download(
                        buckets,
                        ObjectKey::new(format!("object-{index}")).unwrap(),
                        &(0..4),
                        &RequestConfig::default(),
                    )
                    .await
                    .unwrap()
            }
        });
        let outputs = futures::future::join_all(requests).await;
        assert_eq!(outputs.iter().filter(|output| output.hedged).count(), 1);
        assert!(
            outputs
                .iter()
                .all(|output| output.latency == Duration::from_millis(500))
        );
        assert_eq!(script.requests.load(Ordering::SeqCst), 102);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn budget_denial_preserves_primary_failure_and_fallback() {
        let (downloader, script) = scripted_downloader([
            ("primary", 10, 90, false),
            ("primary", 10, 110, true),
            ("fallback", 10, 10, false),
        ]);
        fetch(&downloader, &["primary"]).await;
        drop(
            downloader
                .hedge_budget
                .try_acquire(&BucketName::new("primary").unwrap())
                .unwrap(),
        );
        let output = fetch(&downloader, &["primary", "fallback"]).await;
        assert!(!output.hedged);
        assert_eq!(output.used_bucket_idx, 1);
        assert_eq!(output.latency, Duration::from_millis(140));
        assert_eq!(script.requests.load(Ordering::SeqCst), 3);
        assert_eq!(
            metrics(&downloader, "primary").await.consecutive_failures,
            1
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_releases_hedge_capacity_for_later_downloads() {
        let (downloader, script) = scripted_downloader([
            ("bucket", 10, 90, false),
            ("bucket", 10, 1_000, false),
            ("bucket", 10, 1_000, false),
            ("bucket", 10, 90, false),
            ("bucket", 10, 190, false),
            ("bucket", 10, 20, false),
        ]);
        let downloader = downloader
            .with_limits(DownloadLimits {
                max_concurrent_hedges: 1,
                hedge_budget_percent: 100,
                ..DownloadLimits::default()
            })
            .unwrap();
        fetch(&downloader, &["bucket"]).await;
        assert!(
            timeout(Duration::from_millis(150), fetch(&downloader, &["bucket"]))
                .await
                .is_err()
        );
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
        let bucket_metrics = metrics(&downloader, "bucket").await;
        assert_eq!(bucket_metrics.consecutive_failures, 0);
        assert_eq!(bucket_metrics.latency_mean, Duration::from_millis(100));
        assert!(!fetch(&downloader, &["bucket"]).await.hedged);
        let output = fetch(&downloader, &["bucket"]).await;
        assert!(output.hedged);
        assert_eq!(output.latency, Duration::from_millis(130));
        assert_eq!(script.requests.load(Ordering::SeqCst), 6);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn hedges_do_not_retry_but_primary_sdk_retries_remain() {
        let (downloader, script) = scripted_downloader([
            ("bucket", 10, 0, false),
            ("bucket", 10, 200, false),
            ("bucket", 10, 0, false),
        ]);
        script.service_errors.lock().extend([(0, 503), (2, 503)]);
        downloader.bucketed_stats.observe(
            BucketName::new("bucket").unwrap(),
            Ok(Duration::from_millis(100)),
        );
        let config = RequestConfig {
            max_attempts: Some(3),
            initial_backoff: Some(Duration::from_millis(1)),
            max_backoff: Some(Duration::from_millis(1)),
            ..RequestConfig::default()
        };
        let output = fetch_with_config(&downloader, &["bucket"], &config)
            .await
            .unwrap();
        assert!(output.hedged);
        assert!(
            (Duration::from_millis(220)..=Duration::from_millis(225)).contains(&output.latency)
        );
        assert_eq!(script.requests.load(Ordering::SeqCst), 3);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
        assert_eq!(metrics(&downloader, "bucket").await.consecutive_failures, 0);
    }
}
