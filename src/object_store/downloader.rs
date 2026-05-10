use std::{ops::Range, sync::Arc, time::Duration};

use aws_sdk_s3::{
    error::ProvideErrorMetadata,
    operation::get_object::{GetObjectError, GetObjectOutput},
};
use bytes::Bytes;
use http_content_range::ContentRange;
use parking_lot::Mutex;
use tokio::{select, time::Instant};

use crate::{
    object_store::{BucketMetrics, config::RequestConfig, stats::BucketedStats},
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
            | Self::Unknown(_) => true,
        }
    }

    fn should_wait_for_hedged_peer(&self) -> bool {
        match self {
            Self::BodyStreaming(_) | Self::Unknown(_) => true,
            Self::InvalidObjectState(_) | Self::NoSuchKey | Self::RangeNotSatisfied { .. } => false,
        }
    }
}

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
    pub latency: Duration,
    pub hedged: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct DownloadOutput {
    pub piece: ObjectPiece,
    pub primary_bucket_idx: usize,
    pub secondary_bucket_idx: Option<usize>,
    pub used_bucket_idx: usize,
}

#[derive(Debug, Clone)]
pub struct Downloader {
    s3: aws_sdk_s3::Client,
    bucketed_stats: BucketedStats,
    throughput: Arc<Mutex<SlidingThroughput>>,
}

impl Downloader {
    pub fn new(
        s3: aws_sdk_s3::Client,
        hedge_latency_quantile: f64,
        throughput: Arc<Mutex<SlidingThroughput>>,
    ) -> Self {
        Self {
            s3,
            bucketed_stats: BucketedStats::new(hedge_latency_quantile),
            throughput,
        }
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
        let mut attempt_order = self.bucketed_stats.attempt_order(buckets.iter());
        let primary_bucket_idx = attempt_order.next().expect("non-empty");
        match (
            self.attempt(&buckets[primary_bucket_idx], &object, byterange, req_config)
                .await,
            attempt_order.next(),
        ) {
            (Ok(piece), secondary_bucket_idx) => Ok(DownloadOutput {
                piece,
                primary_bucket_idx,
                secondary_bucket_idx,
                used_bucket_idx: primary_bucket_idx,
            }),
            (Err(e), Some(secondary_bucket_idx)) if e.should_attempt_fallback_bucket() => {
                let piece = self
                    .attempt(
                        &buckets[secondary_bucket_idx],
                        &object,
                        byterange,
                        req_config,
                    )
                    .await?;
                Ok(DownloadOutput {
                    piece,
                    primary_bucket_idx,
                    secondary_bucket_idx: Some(secondary_bucket_idx),
                    used_bucket_idx: secondary_bucket_idx,
                })
            }
            (Err(e), _) => Err(e),
        }
    }

    async fn attempt(
        &self,
        bucket: &BucketName,
        object: &ObjectKey,
        byterange: &Range<u64>,
        req_config: &RequestConfig,
    ) -> Result<ObjectPiece, DownloadError> {
        let attempt_full = |start_time: Instant, hedged: Option<Duration>| {
            let bucket = bucket.clone();
            async move {
                let result = self
                    .attempt_inner(&bucket, object, byterange, req_config)
                    .await;
                let latency = start_time.elapsed();
                self.handle_result(bucket, byterange, result, latency, hedged)
                    .await
            }
        };
        let start_time = Instant::now();
        let mut primary_attempt = Box::pin(attempt_full(start_time, None));
        select! {
            primary_result = &mut primary_attempt => primary_result,
            hedge_threshold = self.hedge_trigger(bucket, start_time) => {
                let hedge_start_time = Instant::now();
                let mut hedge_attempt = Box::pin(attempt_full(hedge_start_time, hedge_threshold));
                select! {
                    primary_result = &mut primary_attempt => match primary_result {
                        Ok(piece) => Ok(piece),
                        Err(error) if error.should_wait_for_hedged_peer() => hedge_attempt.await,
                        Err(error) => Err(error),
                    },
                    hedge_result = &mut hedge_attempt => match hedge_result {
                        Ok(piece) => Ok(piece),
                        Err(error) if error.should_wait_for_hedged_peer() => primary_attempt.await,
                        Err(error) => Err(error),
                    },
                }
            }
        }
    }

    async fn attempt_inner(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        byterange: &Range<u64>,
        req_config: &RequestConfig,
    ) -> Result<
        GetObjectOutput,
        aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
    > {
        let request = self
            .s3
            .get_object()
            .bucket(&**bucket)
            .key(&**key)
            .range(format!("bytes={}-{}", byterange.start, byterange.end - 1))
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled);

        if req_config.is_noop() {
            request.send().await
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

            request
                .customize()
                .config_override(config_override)
                .send()
                .await
        }
    }

    async fn handle_result(
        &self,
        bucket: BucketName,
        req_range: &Range<u64>,
        result: Result<
            GetObjectOutput,
            aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
        >,
        latency: Duration,
        hedged: Option<Duration>,
    ) -> Result<ObjectPiece, DownloadError> {
        let final_result = match result {
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
                        latency,
                        hedged,
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
        };

        let observed_outcome = final_result.as_ref().map(|_| latency).map_err(|_| ());
        self.bucketed_stats.observe(bucket, observed_outcome);
        final_result
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
        let bucket = BucketName::new("test-bucket").unwrap();
        let req_range = Range { start: 0, end: 10 };

        // Create a mock successful response
        let test_data = b"0123456789";
        let output = GetObjectOutput::builder()
            .content_range("bytes 0-9/100")
            .last_modified(DateTime::from_secs(1_234_567_890))
            .body(aws_sdk_s3::primitives::ByteStream::from(test_data.to_vec()))
            .build();

        let result = downloader
            .handle_result(
                bucket.clone(),
                &req_range,
                Ok(output),
                Duration::from_millis(100),
                None,
            )
            .await
            .unwrap();

        assert_eq!(result.data, Bytes::from(test_data.to_vec()));
        assert_eq!(result.object_size, 100);
        assert_eq!(result.mtime, 1_234_567_890);
    }

    #[tokio::test]
    async fn test_handle_result_range_mismatch() {
        let downloader = make_test_downloader();
        let bucket = BucketName::new("test-bucket").unwrap();
        let req_range = Range { start: 10, end: 20 };

        // Response with mismatched start byte
        let output = GetObjectOutput::builder()
            .content_range("bytes 0-9/100")
            .body(aws_sdk_s3::primitives::ByteStream::from(vec![0; 10]))
            .build();

        let result = downloader
            .handle_result(
                bucket.clone(),
                &req_range,
                Ok(output),
                Duration::from_millis(100),
                None,
            )
            .await;

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
        let mut metrics_checked = false;
        downloader.observe_bucket_metrics(|name, metrics| {
            if name == &bucket {
                metrics_checked = true;
                assert_eq!(metrics.consecutive_failures, 1);
            }
        });
        assert!(metrics_checked);
    }

    #[tokio::test]
    async fn test_handle_result_rejects_oversized_response_ending_at_object_eof() {
        let downloader = make_test_downloader();
        let bucket = BucketName::new("test-bucket").unwrap();
        let req_range = Range { start: 0, end: 10 };

        let output = GetObjectOutput::builder()
            .content_range("bytes 0-99/100")
            .body(aws_sdk_s3::primitives::ByteStream::from(vec![0; 100]))
            .build();

        let result = downloader
            .handle_result(
                bucket,
                &req_range,
                Ok(output),
                Duration::from_millis(100),
                None,
            )
            .await;

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
        let bucket = BucketName::new("test-bucket").unwrap();
        let req_range = Range { start: 0, end: 10 };

        let output = GetObjectOutput::builder()
            .content_range("bytes 0-4/5")
            .last_modified(DateTime::from_secs(1_234_567_890))
            .body(aws_sdk_s3::primitives::ByteStream::from(vec![0; 5]))
            .build();

        let piece = downloader
            .handle_result(
                bucket,
                &req_range,
                Ok(output),
                Duration::from_millis(100),
                None,
            )
            .await
            .expect("valid EOF truncation should be accepted");

        assert_eq!(piece.data, Bytes::from(vec![0; 5]));
        assert_eq!(piece.object_size, 5);
        assert_eq!(piece.mtime, 1_234_567_890);
    }

    #[tokio::test]
    async fn test_handle_result_no_such_key() {
        let downloader = make_test_downloader();
        let bucket = BucketName::new("test-bucket").unwrap();
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
            .handle_result(
                bucket,
                &req_range,
                Err(sdk_error),
                Duration::from_millis(100),
                None,
            )
            .await;

        match result {
            Err(DownloadError::NoSuchKey) => {}
            _ => panic!("Expected NoSuchKey error"),
        }
    }

    #[tokio::test]
    async fn test_handle_result_body_length_mismatch() {
        let downloader = make_test_downloader();
        let bucket = BucketName::new("test-bucket").unwrap();
        let req_range = Range { start: 0, end: 10 };

        // Create output with content range indicating 10 bytes but only 5 bytes of data
        let output = GetObjectOutput::builder()
            .content_range("bytes 0-9/100")
            .body(aws_sdk_s3::primitives::ByteStream::from(vec![0; 5]))
            .build();

        let result = downloader
            .handle_result(
                bucket,
                &req_range,
                Ok(output),
                Duration::from_millis(100),
                None,
            )
            .await;

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
        let bucket = BucketName::new("test-bucket").unwrap();
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
            .handle_result(
                bucket,
                &req_range,
                Err(sdk_error),
                Duration::from_millis(100),
                None,
            )
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
        let bucket = BucketName::new("test-bucket").unwrap();
        let req_range = Range { start: 0, end: 10 };

        // Create output without content range header
        let output = GetObjectOutput::builder()
            .body(aws_sdk_s3::primitives::ByteStream::from(vec![0; 10]))
            .build();

        let result = downloader
            .handle_result(
                bucket,
                &req_range,
                Ok(output),
                Duration::from_millis(100),
                None,
            )
            .await;

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
        let bucket = BucketName::new("test-bucket").unwrap();
        let req_range = Range {
            start: 100,
            end: 200,
        };

        // Create output with unsatisfied range response
        let output = GetObjectOutput::builder()
            .content_range("bytes */50")
            .body(aws_sdk_s3::primitives::ByteStream::from(vec![]))
            .build();

        let result = downloader
            .handle_result(
                bucket,
                &req_range,
                Ok(output),
                Duration::from_millis(100),
                None,
            )
            .await;

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
