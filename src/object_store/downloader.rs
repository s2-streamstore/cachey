use std::{ops::Range, sync::Arc, time::Duration};

use aws_sdk_s3::{
    error::{ProvideErrorMetadata, SdkError},
    operation::get_object::{GetObjectError, GetObjectOutput},
};
use bytes::{Bytes, BytesMut};
use http_content_range::ContentRange;
use parking_lot::Mutex;
use tokio::{
    select,
    time::{Instant, sleep_until},
};

use crate::{
    object_store::{
        BucketMetrics,
        admission::{DownloadAdmission, DownloadPermit},
        budget::AttemptBudget,
        config::{DownloadLimits, RequestConfig},
        stats::{BucketedStats, Outcome, ProbePermit},
    },
    service::SlidingThroughput,
    types::{BucketName, BucketNameSet, ObjectKey},
};

mod replicas;

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
    #[error("Invalid object-store response: {0}")]
    InvalidResponse(String),
    #[error("Body streaming: {0}")]
    BodyStreaming(String),
    #[error("Object store overloaded: {0}")]
    Overloaded(String),
    #[error("Download admission did not become available before the deadline")]
    AdmissionTimeout,
    #[error("Download needs {requested_bytes} bytes; admission allows at most {limit_bytes}")]
    AdmissionExhausted {
        requested_bytes: u64,
        limit_bytes: u64,
    },
    #[error("Bucket {bucket} download timed out after {timeout:?}")]
    Timeout {
        bucket: BucketName,
        /// Duration before timeout, excluding admission.
        timeout: Duration,
    },
    #[error("Unknown error: {0}")]
    Unknown(String),
}

impl DownloadError {
    fn should_attempt_fallback_bucket(&self) -> bool {
        match self {
            Self::RangeNotSatisfied { .. } | Self::AdmissionExhausted { .. } => false,
            Self::InvalidObjectState(_)
            | Self::NoSuchKey
            | Self::InvalidResponse(_)
            | Self::BodyStreaming(_)
            | Self::Overloaded(_)
            | Self::AdmissionTimeout
            | Self::Timeout { .. }
            | Self::Unknown(_) => true,
        }
    }

    fn should_wait_for_hedged_peer(&self) -> bool {
        match self {
            Self::InvalidResponse(_)
            | Self::BodyStreaming(_)
            | Self::Unknown(_)
            | Self::Timeout { .. }
            | Self::Overloaded(_) => true,
            Self::InvalidObjectState(_)
            | Self::NoSuchKey
            | Self::RangeNotSatisfied { .. }
            | Self::AdmissionTimeout
            | Self::AdmissionExhausted { .. } => false,
        }
    }

    fn health_outcome(&self, expected: Duration) -> Outcome {
        match self {
            Self::Overloaded(_) => Outcome::Overload,
            Self::InvalidResponse(_) | Self::BodyStreaming(_) | Self::Unknown(_) => {
                Outcome::Failure
            }
            Self::Timeout { timeout, .. } if *timeout >= expected => Outcome::Failure,
            _ => Outcome::Neutral,
        }
    }
}

fn map_get_object_error(
    bucket: &BucketName,
    req_range: &Range<u64>,
    elapsed: Duration,
    error: SdkError<GetObjectError>,
) -> DownloadError {
    if match &error {
        SdkError::TimeoutError(_) => true,
        SdkError::DispatchFailure(error) => error.is_timeout(),
        _ => false,
    } {
        return DownloadError::Timeout {
            bucket: bucket.clone(),
            timeout: elapsed,
        };
    }
    let object_size = error
        .raw_response()
        .and_then(|response| response.headers().get("content-range"))
        .and_then(ContentRange::parse)
        .and_then(|content_range| match content_range {
            ContentRange::Unsatisfied(range) => Some(range.complete_length),
            ContentRange::Bytes(_) | ContentRange::UnboundBytes(_) => None,
        });
    match error.into_service_error() {
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
        service_error
            if matches!(
                service_error.code(),
                Some(
                    "SlowDown"
                        | "Throttling"
                        | "ThrottlingException"
                        | "TooManyRequests"
                        | "TooManyRequestsException"
                )
            ) =>
        {
            DownloadError::Overloaded(service_error.to_string())
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
    pub used_bucket_idx: usize,
    /// Time to validated page data, including SDK retries, hedging, and bucket fallback.
    pub latency: Duration,
    /// Whether an overlapping request was started, including deadline or recovery probes.
    pub hedged: bool,
}

struct BucketOperation<'a> {
    downloader: &'a Downloader,
    bucket: &'a BucketName,
    deadline: Instant,
    unknown_latency: Duration,
    probe: Option<ProbePermit>,
}

impl BucketOperation<'_> {
    async fn execute<T, F: Future<Output = Result<T, DownloadError>>>(
        self,
        bytes: u64,
        admission: Option<DownloadPermit>,
        fetch: impl FnOnce(Instant) -> F,
    ) -> Result<T, DownloadError> {
        let _admission = match admission {
            Some(permit) => permit,
            None => {
                self.downloader
                    .admission
                    .acquire(bytes, self.deadline)
                    .await?
            }
        };
        let now = Instant::now();
        let budget = self
            .downloader
            .limits
            .bucket_timeout
            .min(self.deadline.saturating_duration_since(now));
        if budget.is_zero() {
            return Err(DownloadError::Timeout {
                bucket: self.bucket.clone(),
                timeout: budget,
            });
        }
        let expected = self
            .downloader
            .bucketed_stats
            .tail_latency(self.bucket)
            .unwrap_or(
                self.unknown_latency
                    .min(self.downloader.limits.bucket_timeout)
                    / 2,
            );
        let observation = self
            .downloader
            .bucketed_stats
            .begin(self.bucket, self.probe);
        let result = select! {
            biased;
            result = fetch(now + budget) => result,
            () = sleep_until(now + budget) => Err(DownloadError::Timeout {
                bucket: self.bucket.clone(), timeout: budget,
            }),
        };
        observation.complete(
            result
                .as_ref()
                .map_or_else(|error| error.health_outcome(expected), |_| Outcome::Success),
        );
        result
    }
}

#[derive(Debug, Clone)]
pub struct Downloader {
    s3: aws_sdk_s3::Client,
    bucketed_stats: BucketedStats,
    throughput: Arc<Mutex<SlidingThroughput>>,
    limits: DownloadLimits,
    attempt_budget: AttemptBudget,
    admission: Arc<DownloadAdmission>,
}

impl Downloader {
    pub fn new(
        s3: aws_sdk_s3::Client,
        limits: DownloadLimits,
        throughput: Arc<Mutex<SlidingThroughput>>,
    ) -> eyre::Result<Self> {
        limits.validate()?;
        Ok(Self {
            s3,
            bucketed_stats: BucketedStats::default(),
            throughput,
            limits,
            attempt_budget: AttemptBudget::new(limits.hedge_budget_percent),
            admission: Arc::new(DownloadAdmission::new(limits)),
        })
    }

    #[cfg(test)]
    pub(super) fn with_test_limits(self, limits: DownloadLimits) -> eyre::Result<Self> {
        let mut configured = Self::new(self.s3, limits, self.throughput)?;
        configured.bucketed_stats = self.bucketed_stats;
        Ok(configured)
    }

    #[cfg(test)]
    pub(super) fn simulation_histograms(&self) -> Vec<(u64, usize)> {
        self.bucketed_stats.simulation_histograms()
    }

    pub fn observe_bucket_metrics(&self, f: impl FnMut(&BucketName, &BucketMetrics)) {
        self.bucketed_stats.export_bucket_metrics(f);
    }

    pub async fn download(
        &self,
        buckets: &BucketNameSet,
        object: ObjectKey,
        byterange: &Range<u64>,
        req_config: &RequestConfig,
    ) -> Result<DownloadOutput, DownloadError> {
        if byterange.start >= byterange.end {
            return Err(DownloadError::RangeNotSatisfied {
                requested: byterange.clone(),
                object_size: None,
            });
        }
        let start = Instant::now();
        let deadline = start.checked_add(self.limits.page_timeout).ok_or_else(|| {
            DownloadError::Unknown("Page timeout exceeds the clock's supported range".to_owned())
        })?;
        let output = if buckets.len() == 1 {
            let bucket = &buckets[0];
            let (piece, hedged) = BucketOperation {
                downloader: self,
                bucket,
                deadline,
                unknown_latency: self.limits.page_timeout,
                probe: None,
            }
            .execute(byterange.end - byterange.start, None, |deadline| {
                self.fetch_with_hedge(bucket, &object, byterange, req_config, deadline)
            })
            .await?;
            DownloadOutput {
                piece,
                primary_bucket_idx: 0,
                used_bucket_idx: 0,
                latency: start.elapsed(),
                hedged,
            }
        } else {
            self.download_replicas(buckets, &object, byterange, req_config, start, deadline)
                .await?
        };
        self.attempt_budget.observe_success();
        Ok(output)
    }

    async fn fetch_with_hedge(
        &self,
        bucket: &BucketName,
        object: &ObjectKey,
        byterange: &Range<u64>,
        config: &RequestConfig,
        deadline: Instant,
    ) -> Result<(ObjectPiece, bool), DownloadError> {
        let primary = self.fetch_piece(bucket, object, byterange, config);
        tokio::pin!(primary);
        let Some(hedge_delay) = self
            .bucketed_stats
            .tail_latency(bucket)
            .filter(|delay| !delay.is_zero() && self.limits.hedge_budget_percent > 0)
        else {
            return primary.await.map(|piece| (piece, false));
        };
        let now = Instant::now();
        let hedge_at = now + hedge_delay.min(deadline.saturating_duration_since(now));
        select! {
            biased;
            result = &mut primary => return result.map(|piece| (piece, false)),
            () = sleep_until(hedge_at) => {},
        }
        let mut hedged = false;
        let mut hedge = Box::pin(async {
            if Instant::now() >= deadline || self.bucketed_stats.overloaded(bucket) {
                return None;
            }
            let _permits = self.hedge_permits(bucket, byterange.end - byterange.start)?;
            hedged = true;
            let hedge_config = RequestConfig {
                max_attempts: Some(1),
                ..config.clone()
            };
            Some(
                self.fetch_piece(bucket, object, byterange, &hedge_config)
                    .await,
            )
        });
        let result = select! {
            result = &mut primary => match result {
                Err(error) if error.should_wait_for_hedged_peer() => hedge.as_mut().await.unwrap_or(Err(error)),
                result => result,
            },
            result = &mut hedge => match result {
                Some(Ok(piece)) => Ok(piece),
                Some(Err(error)) if !error.should_wait_for_hedged_peer() => Err(error),
                Some(Err(_)) | None => primary.as_mut().await,
            },
        };
        drop(hedge);
        result.map(|piece| (piece, hedged))
    }

    async fn fetch_piece(
        &self,
        bucket: &BucketName,
        object: &ObjectKey,
        byterange: &Range<u64>,
        req_config: &RequestConfig,
    ) -> Result<ObjectPiece, DownloadError> {
        #[cfg(test)]
        let _simulation_copy = crate::object_store::simulation::record_copy(object, bucket);
        let request = self
            .s3
            .get_object()
            .bucket(&**bucket)
            .key(&**object)
            .range(format!("bytes={}-{}", byterange.start, byterange.end - 1))
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled);

        let started = Instant::now();
        let output = if req_config.is_noop() {
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
            if let Some(force_path_style) = req_config.force_path_style {
                config_override = config_override.force_path_style(force_path_style);
            }

            request
                .customize()
                .config_override(config_override)
                .send()
                .await
        }
        .map_err(|error| map_get_object_error(bucket, byterange, started.elapsed(), error))?;
        self.read_response(byterange, output).await
    }

    async fn read_response(
        &self,
        req_range: &Range<u64>,
        output: GetObjectOutput,
    ) -> Result<ObjectPiece, DownloadError> {
        let invalid_range = || {
            DownloadError::InvalidResponse(format!(
                "Expected range {req_range:?}, received Content-Range {:?}",
                output.content_range()
            ))
        };
        let Some(ContentRange::Bytes(content_range)) =
            output.content_range().and_then(ContentRange::parse)
        else {
            return Err(invalid_range());
        };
        let expected_end = req_range.end.min(content_range.complete_length);
        if content_range.first_byte != req_range.start
            || content_range.last_byte + 1 != expected_end
        {
            return Err(invalid_range());
        }
        let expected_data_len = expected_end - req_range.start;
        let object_size = content_range.complete_length;
        let mtime = output
            .last_modified()
            .and_then(|dt| dt.secs().try_into().ok())
            .unwrap_or(0);
        let capacity = usize::try_from(expected_data_len).map_err(|_| {
            DownloadError::InvalidResponse("Response range exceeds addressable memory".to_owned())
        })?;
        let mut body = output.body;
        let mut data = BytesMut::with_capacity(capacity);
        while let Some(chunk) = body
            .try_next()
            .await
            .map_err(|error| DownloadError::BodyStreaming(error.to_string()))?
        {
            if chunk.len() as u64 > expected_data_len.saturating_sub(data.len() as u64) {
                return Err(DownloadError::BodyStreaming(format!(
                    "Body exceeds expected {expected_data_len} bytes"
                )));
            }
            data.extend_from_slice(&chunk);
        }
        if data.len() as u64 != expected_data_len {
            return Err(DownloadError::BodyStreaming(format!(
                "Expected {expected_data_len} bytes, got {}",
                data.len()
            )));
        }
        self.throughput.lock().record(data.len());
        Ok(ObjectPiece {
            mtime,
            data: data.freeze(),
            object_size,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, VecDeque},
        io,
        ops::Range,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use aws_sdk_s3::{
        config::{Credentials, Region, retry::RetryConfig},
        error::ErrorMetadata,
        operation::get_object::{GetObjectError, GetObjectOutput},
        primitives::{ByteStream, SdkBody},
    };
    use aws_smithy_runtime_api::client::{
        http::{HttpConnector, HttpConnectorFuture, SharedHttpConnector, http_client_fn},
        orchestrator::{HttpRequest, HttpResponse},
        result::ConnectorError,
    };
    use bytes::Bytes;
    use futures::{StreamExt, stream};
    use http_body::Frame;
    use http_body_util::StreamBody;
    use parking_lot::Mutex;
    use tokio::time::{advance, sleep, timeout};

    use super::{
        BucketMetrics, DownloadError, DownloadLimits, DownloadOutput, Downloader, RequestConfig,
        map_get_object_error,
    };
    use crate::{
        service::SlidingThroughput,
        types::{BucketName, BucketNameSet, ObjectKey},
    };

    // Each response specifies its bucket, header delay, body delay, and body failure.
    type ResponseStep = (&'static str, u64, u64, bool);

    #[derive(Debug)]
    struct ScriptedConnector {
        responses: Mutex<VecDeque<ResponseStep>>,
        state: Arc<ScriptState>,
    }

    struct ActiveRequest(Arc<ScriptState>);

    impl Drop for ActiveRequest {
        fn drop(&mut self) {
            self.0.active.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[derive(Debug, Default)]
    struct ScriptState {
        requests: AtomicUsize,
        active: AtomicUsize,
        service_errors: Mutex<HashMap<usize, u16>>,
    }

    impl HttpConnector for ScriptedConnector {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let request_idx = self.state.requests.fetch_add(1, Ordering::SeqCst);
            self.state.active.fetch_add(1, Ordering::SeqCst);
            let active = ActiveRequest(self.state.clone());
            let service_error = self.state.service_errors.lock().get(&request_idx).copied();
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
                    let code = if status == 404 {
                        "NoSuchKey"
                    } else {
                        "SlowDown"
                    };
                    return Ok(HttpResponse::new(
                        status.try_into().unwrap(),
                        SdkBody::from(format!("<Error><Code>{code}</Code></Error>")),
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
    ) -> (Downloader, Arc<ScriptState>) {
        let state = Arc::new(ScriptState::default());
        let connector = SharedHttpConnector::new(ScriptedConnector {
            responses: Mutex::new(responses.into_iter().collect()),
            state: state.clone(),
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
            DownloadLimits::default(),
            Arc::new(Mutex::new(SlidingThroughput::default())),
        )
        .unwrap();
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

    async fn seed_latency(downloader: &Downloader, bucket: &str, millis: u64) {
        let observation = downloader
            .bucketed_stats
            .begin(&BucketName::new(bucket).unwrap(), None);
        advance(Duration::from_millis(millis)).await;
        observation.complete(crate::object_store::stats::Outcome::Success);
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

    #[tokio::test]
    async fn rejects_mismatched_or_missing_response_ranges() {
        let downloader = downloader([]);
        for header in [
            None,
            Some("bytes 1-10/100"),
            Some("bytes 0-4/100"),
            Some("bytes 0-99/100"),
            Some("bytes */50"),
        ] {
            let output = GetObjectOutput::builder()
                .set_content_range(header.map(str::to_owned))
                .build();
            let result = downloader.read_response(&(0..10), output).await;
            assert!(
                matches!(result, Err(DownloadError::InvalidResponse(_))),
                "{header:?}: {result:?}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn rejects_short_bodies_and_stops_reading_excess_data() {
        let downloader = downloader([]);
        let excess = stream::iter([Ok::<_, io::Error>(Frame::data(Bytes::from_static(
            b"01234567890",
        )))])
        .chain(stream::pending());
        for body in [
            SdkBody::from("short"),
            SdkBody::from_body_1_x(StreamBody::new(excess)),
        ] {
            let output = GetObjectOutput::builder()
                .content_range("bytes 0-9/100")
                .body(ByteStream::new(body))
                .build();
            let result = timeout(
                Duration::from_millis(1),
                downloader.read_response(&(0..10), output),
            )
            .await
            .expect("excess data must be rejected without waiting for the rest of the body");
            assert!(matches!(result, Err(DownloadError::BodyStreaming(_))));
        }
    }

    #[test]
    fn invalid_range_preserves_the_backend_object_size() {
        let error = GetObjectError::generic(ErrorMetadata::builder().code("InvalidRange").build());
        let mut response = HttpResponse::new(416.try_into().unwrap(), SdkBody::empty());
        response
            .headers_mut()
            .insert("content-range", "bytes */512");
        let error = aws_sdk_s3::error::SdkError::service_error(error, response);
        let error = map_get_object_error(
            &BucketName::new("bucket").unwrap(),
            &(1024..2048),
            Duration::ZERO,
            error,
        );
        assert!(
            matches!(error, DownloadError::RangeNotSatisfied { requested, object_size: Some(512) } if requested == (1024..2048))
        );
    }

    #[test]
    fn sdk_and_connector_timeouts_preserve_bucket_and_duration() {
        let bucket = BucketName::new("bucket").unwrap();
        let elapsed = Duration::from_millis(10);
        for error in [
            aws_sdk_s3::error::SdkError::timeout_error(io::Error::from(io::ErrorKind::TimedOut)),
            aws_sdk_s3::error::SdkError::dispatch_failure(ConnectorError::timeout(Box::new(
                io::Error::from(io::ErrorKind::TimedOut),
            ))),
        ] {
            let error = map_get_object_error(&bucket, &(0..4), elapsed, error);
            assert!(matches!(
                error,
                DownloadError::Timeout { bucket: actual, timeout }
                    if actual == bucket && timeout == elapsed
            ));
        }
    }

    #[tokio::test]
    async fn invalid_request_ranges_do_not_reach_the_backend() {
        let (downloader, script) = scripted_downloader([]);
        let buckets = BucketNameSet::from(BucketName::new("bucket").unwrap());
        for range in [Range { start: 10, end: 10 }, Range { start: 10, end: 1 }] {
            let result = downloader
                .download(
                    &buckets,
                    ObjectKey::new("object").unwrap(),
                    &range,
                    &RequestConfig::default(),
                )
                .await;
            assert!(matches!(
                result,
                Err(DownloadError::RangeNotSatisfied { .. })
            ));
        }
        assert_eq!(script.requests.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn constructor_rejects_invalid_limits_without_panicking() {
        let downloader = downloader([]);
        for (max_inflight_bytes, page_timeout) in [
            (0, Duration::from_secs(1)),
            (u64::MAX, Duration::from_secs(1)),
            (4, Duration::ZERO),
            (4, Duration::MAX),
        ] {
            let limits = DownloadLimits {
                page_timeout,
                max_inflight_bytes,
                ..DownloadLimits::default()
            };
            assert!(
                Downloader::new(downloader.s3.clone(), limits, downloader.throughput.clone())
                    .is_err(),
                "{limits:?}"
            );
        }
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
            .with_test_limits(DownloadLimits {
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
            ("fallback", 10, 20, false),
        ]);
        fetch(&downloader, &["primary"]).await;
        let output = fetch(&downloader, &["primary", "fallback"]).await;
        assert_eq!(output.used_bucket_idx, 1);
        assert!(output.hedged, "the failed first bucket started a hedge");
        assert_eq!(output.latency, Duration::from_millis(130));
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
                .with_test_limits(DownloadLimits {
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
            .with_test_limits(DownloadLimits {
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
            .with_test_limits(DownloadLimits {
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
        for body_ms in [90, 200] {
            let (downloader, script) =
                scripted_downloader([("bucket", 10, 90, false), ("bucket", 10, body_ms, false)]);
            fetch(&downloader, &["bucket"]).await;
            let downloader = downloader
                .with_test_limits(DownloadLimits {
                    bucket_timeout: Duration::from_millis(100),
                    ..DownloadLimits::default()
                })
                .unwrap();
            let result =
                fetch_with_config(&downloader, &["bucket"], &RequestConfig::default()).await;
            if body_ms == 90 {
                assert_eq!(result.unwrap().latency, Duration::from_millis(100));
            } else {
                assert!(matches!(result, Err(DownloadError::Timeout { .. })));
            }
            assert_eq!(script.requests.load(Ordering::SeqCst), 2);
            assert_eq!(script.active.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn sdk_timeouts_respect_elapsed_time_and_allow_fallback() {
        for attempt_timeout in [false, true] {
            for timeout_ms in [10, 100] {
                let (downloader, script) =
                    scripted_downloader([("local", 1000, 0, false), ("peer", 0, 2, false)]);
                let downloader = downloader
                    .with_test_limits(DownloadLimits {
                        hedge_budget_percent: 0,
                        ..DownloadLimits::default()
                    })
                    .unwrap();
                seed_latency(&downloader, "local", 50).await;
                let duration = Some(Duration::from_millis(timeout_ms));
                let config = RequestConfig {
                    operation_timeout: if attempt_timeout { None } else { duration },
                    operation_attempt_timeout: if attempt_timeout { duration } else { None },
                    ..RequestConfig::default()
                };
                let output = fetch_with_config(&downloader, &["local", "peer"], &config)
                    .await
                    .unwrap();
                assert_eq!(output.used_bucket_idx, 1);
                assert_eq!(output.latency, Duration::from_millis(timeout_ms + 2));
                assert_eq!(
                    metrics(&downloader, "local").await.deprioritized,
                    timeout_ms >= 50
                );
                assert_eq!(script.requests.load(Ordering::SeqCst), 2);
                assert_eq!(script.active.load(Ordering::SeqCst), 0);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn sdk_timeout_does_not_cancel_a_viable_hedged_peer() {
        for (primary, hedge, elapsed_ms) in [
            (("local", 100, 0, false), ("local", 7, 0, false), 12),
            (("local", 0, 20, false), ("local", 100, 0, false), 20),
        ] {
            let downloader = downloader([primary, hedge]);
            seed_latency(&downloader, "local", 5).await;
            let output = fetch_with_config(
                &downloader,
                &["local"],
                &RequestConfig {
                    operation_timeout: Some(Duration::from_millis(10)),
                    ..RequestConfig::default()
                },
            )
            .await
            .unwrap();
            assert_eq!(output.latency, Duration::from_millis(elapsed_ms));
            assert!(output.hedged);
            assert!(!metrics(&downloader, "local").await.deprioritized);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shared_slowdown_bounds_hedges_across_downloader_clones() {
        let responses = [("bucket", 10, 90, false)]
            .into_iter()
            .chain(std::iter::repeat_n(("bucket", 10, 490, false), 101));
        let (downloader, script) = scripted_downloader(responses);
        fetch(&downloader, &["bucket"]).await;
        let buckets = BucketNameSet::from(BucketName::new("bucket").unwrap());
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
                .attempt_budget
                .try_hedge(&BucketName::new("primary").unwrap())
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
            .with_test_limits(DownloadLimits {
                hedge_budget_percent: 100,
                ..DownloadLimits::default()
            })
            .unwrap();
        let _occupied = downloader
            .attempt_budget
            .try_hedge(&BucketName::new("bucket").unwrap())
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
        assert!(bucket_metrics.error_rate.abs() < f64::EPSILON);
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
        seed_latency(&downloader, "bucket", 100).await;
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
    #[tokio::test(start_paused = true)]
    async fn two_failed_copies_leave_the_third_available_without_hedge_credits() {
        let (downloader, script) = scripted_downloader([
            ("local", 1, 1, true),
            ("peer", 1, 1, true),
            ("third", 1, 5, false),
        ]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                hedge_budget_percent: 0,
                ..DownloadLimits::default()
            })
            .unwrap();
        let output = fetch(&downloader, &["local", "peer", "third"]).await;
        assert_eq!(output.used_bucket_idx, 2);
        assert_eq!(output.latency, Duration::from_millis(10));
        assert!(!output.hedged);
        assert_eq!(script.requests.load(Ordering::SeqCst), 3);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_rescue_reaches_third_copy_while_two_others_stall() {
        let (downloader, script) = scripted_downloader([
            ("local", 0, 1000, false),
            ("peer", 0, 1000, false),
            ("third", 0, 150, false),
        ]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                bucket_timeout: Duration::from_millis(500),
                page_timeout: Duration::from_millis(500),
                hedge_budget_percent: 0,
                ..DownloadLimits::default()
            })
            .unwrap();
        for (bucket, millis) in [("local", 10), ("peer", 60), ("third", 150)] {
            seed_latency(&downloader, bucket, millis).await;
        }
        let output = fetch(&downloader, &["local", "peer", "third"]).await;
        assert_eq!(output.used_bucket_idx, 2);
        assert!(output.latency < Duration::from_millis(500));
        assert!(output.hedged);
        assert_eq!(script.requests.load(Ordering::SeqCst), 3);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
        assert!(downloader.admission.try_acquire(4).is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn tight_deadline_rescue_preserves_a_viable_primary_and_covers_a_stall() {
        for primary_body_ms in [250, 1000] {
            let (downloader, script) = scripted_downloader([
                ("local", 0, primary_body_ms, false),
                ("peer", 0, 300, false),
                ("third", 0, 300, false),
            ]);
            let downloader = downloader
                .with_test_limits(DownloadLimits {
                    page_timeout: Duration::from_millis(400),
                    hedge_budget_percent: 0,
                    ..DownloadLimits::default()
                })
                .unwrap();
            for (bucket, latency) in [("local", 250), ("peer", 300), ("third", 300)] {
                seed_latency(&downloader, bucket, latency).await;
            }
            let output = fetch(&downloader, &["local", "peer", "third"]).await;
            if primary_body_ms == 250 {
                assert_eq!(output.used_bucket_idx, 0);
                assert_eq!(output.latency, Duration::from_millis(250));
            } else {
                assert_ne!(output.used_bucket_idx, 0);
                assert!(output.latency < Duration::from_millis(400));
            }
            assert!(output.hedged);
            assert_eq!(script.requests.load(Ordering::SeqCst), 3);
            assert_eq!(script.active.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_probe_keeps_a_working_copy_available_without_hedge_credits() {
        let (downloader, script) =
            scripted_downloader([("local", 0, 1000, false), ("peer", 0, 5, false)]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                hedge_budget_percent: 0,
                ..DownloadLimits::default()
            })
            .unwrap();
        seed_latency(&downloader, "local", 3).await;
        seed_latency(&downloader, "peer", 5).await;
        downloader
            .bucketed_stats
            .begin(&BucketName::new("local").unwrap(), None)
            .complete(crate::object_store::stats::Outcome::Failure);
        advance(Duration::from_secs(37)).await;
        let output = fetch(&downloader, &["local", "peer"]).await;
        assert_eq!(output.primary_bucket_idx, 0);
        assert_eq!(output.used_bucket_idx, 1);
        assert_eq!(output.latency, Duration::from_millis(15));
        assert!(metrics(&downloader, "local").await.deprioritized);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_distant_healthy_copy_does_not_displace_feasible_impaired_copies() {
        let (downloader, script) = scripted_downloader([("local", 0, 10, false)]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                page_timeout: Duration::from_millis(100),
                hedge_budget_percent: 0,
                ..DownloadLimits::default()
            })
            .unwrap();
        for (bucket, millis) in [("local", 10), ("peer", 60), ("third", 150)] {
            seed_latency(&downloader, bucket, millis).await;
        }
        for bucket in ["local", "peer"] {
            downloader
                .bucketed_stats
                .begin(&BucketName::new(bucket).unwrap(), None)
                .complete(crate::object_store::stats::Outcome::Failure);
        }
        let output = fetch(&downloader, &["local", "peer", "third"]).await;
        assert_eq!(output.used_bucket_idx, 0);
        assert_eq!(script.requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn error_in_one_copy_does_not_cancel_an_existing_viable_copy() {
        let (downloader, script) = scripted_downloader([
            ("local", 0, 40, false),
            ("peer", 0, 1, true),
            ("third", 0, 50, false),
        ]);
        for (bucket, millis) in [("local", 10), ("peer", 20), ("third", 30)] {
            seed_latency(&downloader, bucket, millis).await;
        }
        let output = fetch(&downloader, &["local", "peer", "third"]).await;
        assert_eq!(output.used_bucket_idx, 0);
        assert_eq!(output.latency, Duration::from_millis(40));
        assert_eq!(script.requests.load(Ordering::SeqCst), 3);
        assert!(metrics(&downloader, "peer").await.deprioritized);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_fallback_with_too_little_time_does_not_poison_its_health() {
        let (downloader, script) =
            scripted_downloader([("local", 0, 99, true), ("peer", 0, 5, false)]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                page_timeout: Duration::from_millis(100),
                hedge_budget_percent: 0,
                ..DownloadLimits::default()
            })
            .unwrap();
        seed_latency(&downloader, "local", 99).await;
        seed_latency(&downloader, "peer", 100).await;
        assert!(
            fetch_with_config(&downloader, &["local", "peer"], &RequestConfig::default())
                .await
                .is_err()
        );
        assert!(!metrics(&downloader, "peer").await.deprioritized);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn admission_and_unstarted_expiry_do_not_affect_backend_health() {
        let (downloader, script) = scripted_downloader([]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                max_inflight_requests: 1,
                max_inflight_bytes: 4,
                page_timeout: Duration::from_millis(10),
                ..DownloadLimits::default()
            })
            .unwrap();
        let permit = downloader.admission.try_acquire(4).unwrap();
        let result = fetch_with_config(&downloader, &["local"], &RequestConfig::default()).await;
        assert!(matches!(result, Err(DownloadError::AdmissionTimeout)));
        let result = super::BucketOperation {
            downloader: &downloader,
            bucket: &BucketName::new("local").unwrap(),
            deadline: tokio::time::Instant::now(),
            unknown_latency: Duration::from_millis(10),
            probe: None,
        }
        .execute(4, Some(permit), |_| async { Ok(()) })
        .await;
        assert!(matches!(result, Err(DownloadError::Timeout { .. })));
        assert_eq!(script.requests.load(Ordering::SeqCst), 0);
        let mut count = 0;
        downloader.observe_bucket_metrics(|_, _| count += 1);
        assert_eq!(count, 0);
        assert!(downloader.admission.try_acquire(4).is_some());
    }
    #[tokio::test(start_paused = true)]
    async fn widespread_overload_limits_extra_attempts_but_keeps_initial_reads() {
        let (downloader, script) = scripted_downloader([
            ("local", 1, 0, false),
            ("peer", 1, 0, false),
            ("third", 1, 0, false),
        ]);
        script
            .service_errors
            .lock()
            .extend((0..3).map(|index| (index, 503)));
        for bucket in ["local", "peer", "third"] {
            downloader
                .bucketed_stats
                .begin(&BucketName::new(bucket).unwrap(), None)
                .complete(crate::object_store::stats::Outcome::Overload);
        }
        for _ in 0..2 {
            assert!(matches!(
                fetch_with_config(
                    &downloader,
                    &["local", "peer", "third"],
                    &RequestConfig::default()
                )
                .await,
                Err(DownloadError::Overloaded(_))
            ));
        }
        assert_eq!(script.requests.load(Ordering::SeqCst), 3);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn cross_copy_hedge_has_one_sdk_attempt_while_the_primary_can_finish() {
        let (downloader, script) =
            scripted_downloader([("local", 0, 40, false), ("peer", 1, 0, false)]);
        seed_latency(&downloader, "local", 10).await;
        seed_latency(&downloader, "peer", 20).await;
        script.service_errors.lock().insert(1, 503);
        let config = RequestConfig {
            max_attempts: Some(3),
            initial_backoff: Some(Duration::from_millis(1)),
            max_backoff: Some(Duration::from_millis(1)),
            ..RequestConfig::default()
        };
        let output = fetch_with_config(&downloader, &["local", "peer"], &config)
            .await
            .unwrap();
        assert_eq!(output.used_bucket_idx, 0);
        assert!(output.hedged);
        assert_eq!(script.requests.load(Ordering::SeqCst), 2);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }
    #[tokio::test(start_paused = true)]
    async fn recovery_probe_cannot_take_the_working_copys_only_admission_slot() {
        let (downloader, script) = scripted_downloader([("peer", 0, 5, false)]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                max_inflight_requests: 1,
                max_inflight_bytes: 4,
                ..DownloadLimits::default()
            })
            .unwrap();
        seed_latency(&downloader, "local", 3).await;
        seed_latency(&downloader, "peer", 5).await;
        downloader
            .bucketed_stats
            .begin(&BucketName::new("local").unwrap(), None)
            .complete(crate::object_store::stats::Outcome::Failure);
        advance(Duration::from_secs(37)).await;
        let output = fetch(&downloader, &["local", "peer"]).await;
        assert_eq!(output.primary_bucket_idx, 1);
        assert_eq!(output.used_bucket_idx, 1);
        assert_eq!(output.latency, Duration::from_millis(5));
        assert_eq!(script.requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_probe_denied_admission_can_use_the_next_opportunity() {
        let (downloader, script) =
            scripted_downloader([("peer", 0, 5, false), ("local", 0, 3, false)]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                max_inflight_requests: 2,
                max_inflight_bytes: 8,
                hedge_budget_percent: 0,
                ..DownloadLimits::default()
            })
            .unwrap();
        seed_latency(&downloader, "local", 3).await;
        seed_latency(&downloader, "peer", 5).await;
        downloader
            .bucketed_stats
            .begin(&BucketName::new("local").unwrap(), None)
            .complete(crate::object_store::stats::Outcome::Failure);
        advance(Duration::from_secs(37)).await;

        let occupied = downloader.admission.try_acquire(4).unwrap();
        assert_eq!(
            fetch(&downloader, &["local", "peer"]).await.used_bucket_idx,
            1
        );
        drop(occupied);

        let output = fetch(&downloader, &["local", "peer"]).await;
        assert_eq!(output.primary_bucket_idx, 0);
        assert_eq!(output.used_bucket_idx, 0);
        assert_eq!(output.latency, Duration::from_millis(3));
        assert_eq!(script.requests.load(Ordering::SeqCst), 2);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn queued_copies_that_never_start_are_not_reported_as_hedges() {
        let (downloader, script) = scripted_downloader([("local", 0, 90, false)]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                max_inflight_requests: 1,
                max_inflight_bytes: 4,
                page_timeout: Duration::from_millis(100),
                ..DownloadLimits::default()
            })
            .unwrap();
        seed_latency(&downloader, "local", 10).await;
        seed_latency(&downloader, "peer", 20).await;
        let output = fetch(&downloader, &["local", "peer"]).await;
        assert_eq!(output.used_bucket_idx, 0);
        assert!(!output.hedged);
        assert_eq!(script.requests.load(Ordering::SeqCst), 1);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }
    #[tokio::test(start_paused = true)]
    async fn primary_latency_does_not_consume_a_fallbacks_reserved_time() {
        let (downloader, script) =
            scripted_downloader([("local", 0, 1000, false), ("peer", 0, 150, false)]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                page_timeout: Duration::from_millis(500),
                bucket_timeout: Duration::from_millis(500),
                hedge_budget_percent: 0,
                ..DownloadLimits::default()
            })
            .unwrap();
        seed_latency(&downloader, "local", 400).await;
        seed_latency(&downloader, "peer", 150).await;
        downloader
            .bucketed_stats
            .begin(&BucketName::new("peer").unwrap(), None)
            .complete(crate::object_store::stats::Outcome::Failure);
        let output = fetch(&downloader, &["local", "peer"]).await;
        assert_eq!(output.used_bucket_idx, 1);
        assert!(output.latency < Duration::from_millis(500));
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn malformed_response_falls_back_and_deprioritizes_its_bucket() {
        let (downloader, script) =
            scripted_downloader([("local", 1, 2, false), ("peer", 1, 2, false)]);
        script.service_errors.lock().insert(0, 206);
        let output = fetch(&downloader, &["local", "peer"]).await;
        assert_eq!(output.used_bucket_idx, 1);
        assert_eq!(script.requests.load(Ordering::SeqCst), 2);
        assert!(metrics(&downloader, "local").await.deprioritized);
    }

    #[tokio::test(start_paused = true)]
    async fn missing_replicas_do_not_hide_backend_failures() {
        for missing in [&[0][..], &[1][..], &[0, 1][..]] {
            let (downloader, script) =
                scripted_downloader([("local", 1, 1, true), ("peer", 1, 1, true)]);
            script
                .service_errors
                .lock()
                .extend(missing.iter().map(|index| (*index, 404)));
            let result =
                fetch_with_config(&downloader, &["local", "peer"], &RequestConfig::default()).await;
            if missing.len() == 2 {
                assert!(matches!(result, Err(DownloadError::NoSuchKey)));
            } else {
                assert!(
                    matches!(result, Err(DownloadError::BodyStreaming(_))),
                    "{result:?}"
                );
            }
            assert_eq!(script.requests.load(Ordering::SeqCst), 2);
            assert_eq!(script.active.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn overload_retry_denial_does_not_report_a_partial_miss_as_absence() {
        let (downloader, script) = scripted_downloader([
            ("local", 1, 0, false),
            ("local", 1, 0, false),
            ("peer", 1, 0, false),
        ]);
        script
            .service_errors
            .lock()
            .extend((0..3).map(|index| (index, 404)));
        for bucket in ["local", "peer"] {
            downloader
                .bucketed_stats
                .begin(&BucketName::new(bucket).unwrap(), None)
                .complete(crate::object_store::stats::Outcome::Overload);
        }
        assert!(downloader.attempt_budget.try_retry());
        let result =
            fetch_with_config(&downloader, &["local", "peer"], &RequestConfig::default()).await;
        assert!(
            matches!(result, Err(DownloadError::Overloaded(_))),
            "{result:?}"
        );
        assert_eq!(script.requests.load(Ordering::SeqCst), 1);

        advance(Duration::from_secs(1)).await;
        let result =
            fetch_with_config(&downloader, &["local", "peer"], &RequestConfig::default()).await;
        assert!(matches!(result, Err(DownloadError::NoSuchKey)));
        assert_eq!(script.requests.load(Ordering::SeqCst), 3);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn page_expiry_records_timeouts_for_copies_given_enough_time() {
        let (downloader, script) = scripted_downloader([
            ("local", 1000, 0, false),
            ("peer", 1000, 0, false),
            ("third", 1000, 0, false),
        ]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                page_timeout: Duration::from_millis(100),
                bucket_timeout: Duration::from_millis(100),
                hedge_budget_percent: 0,
                ..DownloadLimits::default()
            })
            .unwrap();
        for (bucket, latency) in [("local", 3), ("peer", 5), ("third", 6)] {
            seed_latency(&downloader, bucket, latency).await;
        }
        let result = fetch_with_config(
            &downloader,
            &["local", "peer", "third"],
            &RequestConfig::default(),
        )
        .await;
        assert!(matches!(result, Err(DownloadError::Timeout { .. })));
        for bucket in ["local", "peer", "third"] {
            assert!(metrics(&downloader, bucket).await.deprioritized, "{bucket}");
        }
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }
}
