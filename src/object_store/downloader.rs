use std::{ops::Range, pin::pin, sync::Arc, time::Duration};

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
        stats::{BucketedStats, LatencyModel, Outcome, ProbePermit, RequestTimeline},
    },
    service::SlidingThroughput,
    types::{BucketName, BucketNameSet, ObjectKey},
};

mod replicas;
mod timeline;

pub use timeline::HedgePhase;
use timeline::{BucketDeadline, HedgeTimer};

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
    /// No copy could be admitted, so the read ends at once.
    fn ends_read(&self) -> bool {
        matches!(self, Self::AdmissionExhausted { .. })
    }

    /// Every bucket's copies wait in the same admission queue, so admission failures do
    /// not fall back.
    fn should_attempt_fallback_bucket(&self) -> bool {
        match self {
            Self::AdmissionTimeout | Self::AdmissionExhausted { .. } => false,
            Self::InvalidObjectState(_)
            | Self::NoSuchKey
            | Self::RangeNotSatisfied { .. }
            | Self::InvalidResponse(_)
            | Self::BodyStreaming(_)
            | Self::Overloaded(_)
            | Self::Timeout { .. }
            | Self::Unknown(_) => true,
        }
    }

    /// Another copy from the same bucket would return the same error.
    fn repeats_within_bucket(&self) -> bool {
        match self {
            Self::InvalidObjectState(_) | Self::NoSuchKey | Self::RangeNotSatisfied { .. } => true,
            Self::InvalidResponse(_)
            | Self::BodyStreaming(_)
            | Self::Overloaded(_)
            | Self::AdmissionTimeout
            | Self::AdmissionExhausted { .. }
            | Self::Timeout { .. }
            | Self::Unknown(_) => false,
        }
    }

    /// The SDK cannot retry body transfer failures. Length mismatches are not retried here.
    fn should_retry_same_bucket(&self) -> bool {
        matches!(self, Self::BodyStreaming(_))
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
    let status = error
        .raw_response()
        .map(|response| response.status().as_u16());
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
        // S3-compatible stores and proxies may answer a miss or an unsatisfiable range by
        // status alone. The SDK codes an empty 404 `NotFound`, and leaves other bodies
        // without XML uncoded.
        service_error
            if status == Some(404) && matches!(service_error.code(), None | Some("NotFound")) =>
        {
            DownloadError::NoSuchKey
        }
        service_error
            if service_error.code() == Some("InvalidRange")
                || (status == Some(416) && service_error.code().is_none()) =>
        {
            DownloadError::RangeNotSatisfied {
                requested: req_range.clone(),
                object_size,
            }
        }
        // S3-compatible stores and proxies may signal overload by status alone.
        service_error
            if matches!(status, Some(429 | 503))
                || matches!(
                    service_error.code(),
                    Some(
                        "SlowDown"
                            | "ServiceUnavailable"
                            | "Throttling"
                            | "ThrottlingException"
                            | "TooManyRequests"
                            | "TooManyRequestsException"
                    )
                ) =>
        {
            DownloadError::Overloaded(match status {
                Some(status) => format!("HTTP {status}: {service_error}"),
                None => service_error.to_string(),
            })
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
    /// Time to validated data, including SDK retries, hedging, and bucket fallback.
    pub latency: Duration,
    /// Whether an overlapping request was started, including deadline or recovery probes.
    pub hedged: bool,
    /// The early hedge, if one started.
    pub hedge: Option<Hedge>,
}

/// An early hedge: when it fired, where it went, and whether its response was served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hedge {
    pub phase: HedgePhase,
    /// Sent to the primary copy's bucket rather than a peer.
    pub same_bucket: bool,
    pub won: bool,
}

impl Hedge {
    /// Phase, target, and outcome.
    #[must_use]
    pub fn labels(self) -> [&'static str; 3] {
        [
            match self.phase {
                HedgePhase::FirstByte => "first_byte",
                HedgePhase::Body => "body",
            },
            if self.same_bucket { "primary" } else { "peer" },
            if self.won { "won" } else { "lost" },
        ]
    }
}

#[derive(Clone, Copy)]
struct PieceRequest<'a> {
    bucket: &'a BucketName,
    object: &'a ObjectKey,
    range: &'a Range<u64>,
    /// Bytes the range is expected to return.
    bytes: u64,
    config: &'a RequestConfig,
}

struct BucketOperation<'a> {
    downloader: &'a Downloader,
    request: PieceRequest<'a>,
    /// The bucket's latency model when the copy was routed.
    model: LatencyModel,
    timeline: &'a RequestTimeline,
    deadline: &'a BucketDeadline,
    /// Time allowed for each unmeasured phase.
    unmeasured: Duration,
    probe: Option<ProbePermit>,
}

impl BucketOperation<'_> {
    fn timeout_allowance(&self) -> Duration {
        self.model.timeout_allowance(
            self.request.bytes,
            self.timeline.announced_body_bytes(),
            self.unmeasured,
        )
    }

    async fn execute<T>(
        mut self,
        admission: Option<DownloadPermit>,
        fetch: impl AsyncFnOnce() -> Result<T, DownloadError>,
    ) -> Result<T, DownloadError> {
        let _admission = if let Some(permit) = admission {
            permit
        } else {
            // A copy queues while it can still finish by the deadline. Only a bucket's first
            // copy queues, so no bucket deadline applies.
            let deadline = self.deadline.download();
            let allowance = self.timeout_allowance();
            let latest_start = deadline.checked_sub(allowance).unwrap_or_else(Instant::now);
            self.downloader
                .admission
                .acquire(self.request.bytes, latest_start, allowance)
                .await?
        };
        let now = Instant::now();
        let bucket_deadline = self
            .deadline
            .admit(now, self.downloader.limits.bucket_timeout);
        let budget = bucket_deadline.saturating_duration_since(now);
        if budget.is_zero() {
            return Err(DownloadError::Timeout {
                bucket: self.request.bucket.clone(),
                timeout: budget,
            });
        }
        let mut observation = self.downloader.bucketed_stats.begin(
            self.request.bucket,
            self.probe.take(),
            self.timeline,
            self.request.bytes,
            self.timeout_allowance(),
        );
        let mut fetch = pin!(fetch());
        let mut expiry = pin!(sleep_until(bucket_deadline));
        let mut expired = false;
        let mut announced = false;
        let result = loop {
            select! {
                biased;
                result = &mut fetch => break result,
                () = &mut expiry => {
                    expired = true;
                    break Err(DownloadError::Timeout {
                        bucket: self.request.bucket.clone(),
                        timeout: budget,
                    });
                }
                // The announced length refines how long the copy should take.
                () = self.timeline.headers_received(), if !announced => {
                    announced = true;
                    observation.expect(self.timeout_allowance());
                }
            }
        };
        let outcome = match &result {
            Ok(_) => Outcome::Success,
            Err(error) => match error.health_outcome(self.timeout_allowance()) {
                // Copies to a bucket share its deadline, whose expiry counts once.
                Outcome::Failure if expired && !self.deadline.report_expiry() => Outcome::Neutral,
                outcome => outcome,
            },
        };
        observation.complete(outcome);
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

    /// A known object size sets how much of the range the read is expected to return, by
    /// which it is admitted and timed. The whole range is still requested, so a wrong size
    /// never shortens the download, but a range starting at or past it fails at once.
    pub async fn download(
        &self,
        buckets: &BucketNameSet,
        object: ObjectKey,
        byterange: &Range<u64>,
        object_size: Option<u64>,
        req_config: &RequestConfig,
    ) -> Result<DownloadOutput, DownloadError> {
        let end = byterange.end.min(object_size.unwrap_or(u64::MAX));
        if byterange.start >= end {
            return Err(DownloadError::RangeNotSatisfied {
                requested: byterange.clone(),
                object_size,
            });
        }
        let output = self
            .download_replicas(
                buckets,
                &object,
                byterange,
                end - byterange.start,
                req_config,
            )
            .await?;
        self.attempt_budget.observe_success();
        Ok(output)
    }

    async fn fetch_piece(
        &self,
        request: PieceRequest<'_>,
        timeline: &RequestTimeline,
    ) -> Result<ObjectPiece, DownloadError> {
        let PieceRequest {
            bucket,
            object,
            range,
            config,
            ..
        } = request;
        #[cfg(test)]
        let _simulation_copy =
            crate::object_store::simulation::record_copy(object, bucket, request.bytes);
        let get_object = self
            .s3
            .get_object()
            .bucket(&**bucket)
            .key(&**object)
            .range(format!("bytes={}-{}", range.start, range.end - 1))
            .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled);

        let started = timeline.start();
        let output = if config.is_noop() {
            get_object.send().await
        } else {
            let client_config = self.s3.config();
            let mut config_override = client_config.to_builder();
            if let Some(timeout_config) =
                config.merged_timeout_config(client_config.timeout_config())
            {
                config_override = config_override.timeout_config(timeout_config);
            }
            if let Some(retry_config) = config.merged_retry_config(client_config.retry_config()) {
                config_override = config_override.retry_config(retry_config);
            }
            if let Some(force_path_style) = config.force_path_style {
                config_override = config_override.force_path_style(force_path_style);
            }

            get_object
                .customize()
                .config_override(config_override)
                .send()
                .await
        }
        .map_err(|error| map_get_object_error(bucket, range, started.elapsed(), error))?;
        self.read_response(range, output, timeline).await
    }

    async fn read_response(
        &self,
        req_range: &Range<u64>,
        output: GetObjectOutput,
        timeline: &RequestTimeline,
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
        timeline.receive_headers(expected_data_len);
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
                return Err(DownloadError::InvalidResponse(format!(
                    "Body exceeds expected {expected_data_len} bytes"
                )));
            }
            data.extend_from_slice(&chunk);
        }
        if data.len() as u64 != expected_data_len {
            return Err(DownloadError::InvalidResponse(format!(
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
        mem::discriminant,
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
    use bytesize::MIB;
    use futures::{StreamExt, stream};
    use http_body::Frame;
    use http_body_util::StreamBody;
    use parking_lot::Mutex;
    use tokio::time::{advance, sleep, timeout};

    use super::{
        BucketDeadline, BucketMetrics, DownloadError, DownloadLimits, DownloadOutput, Downloader,
        Hedge, HedgePhase, RequestConfig, RequestTimeline, map_get_object_error,
    };
    use crate::{
        object_store::{
            admission::MIN_WAIT,
            stats::{LatencyModel, Outcome, Sample},
        },
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
        /// Replaces the default error body, whose code follows the status.
        service_error_bodies: Mutex<HashMap<usize, &'static str>>,
        range_error_sizes: Mutex<HashMap<usize, u64>>,
    }

    impl HttpConnector for ScriptedConnector {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let request_idx = self.state.requests.fetch_add(1, Ordering::SeqCst);
            self.state.active.fetch_add(1, Ordering::SeqCst);
            let active = ActiveRequest(self.state.clone());
            let service_error = self.state.service_errors.lock().get(&request_idx).copied();
            let service_error_body = self
                .state
                .service_error_bodies
                .lock()
                .get(&request_idx)
                .copied();
            let object_size = self
                .state
                .range_error_sizes
                .lock()
                .get(&request_idx)
                .copied();
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
                    let code = match status {
                        404 => "NoSuchKey",
                        416 => "InvalidRange",
                        _ => "SlowDown",
                    };
                    let body = service_error_body.map_or_else(
                        || format!("<Error><Code>{code}</Code></Error>"),
                        str::to_owned,
                    );
                    let mut response =
                        HttpResponse::new(status.try_into().unwrap(), SdkBody::from(body));
                    if let Some(object_size) = object_size {
                        response
                            .headers_mut()
                            .insert("content-range", format!("bytes */{object_size}"));
                    }
                    return Ok(response);
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
            .retry_partition(crate::object_store::isolated_retry_partition())
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

    /// Exhaust the retry budget to test errors without a body retry.
    fn spend_retry_budget(downloader: &Downloader) {
        while downloader.attempt_budget.try_retry() {}
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
        fetch_range(downloader, buckets, 0..4, None, config).await
    }

    async fn fetch_range(
        downloader: &Downloader,
        buckets: &[&str],
        range: Range<u64>,
        object_size: Option<u64>,
        config: &RequestConfig,
    ) -> Result<DownloadOutput, DownloadError> {
        let buckets =
            BucketNameSet::new(buckets.iter().map(|name| BucketName::new(*name).unwrap())).unwrap();
        downloader
            .download(
                &buckets,
                ObjectKey::new("object").unwrap(),
                &range,
                object_size,
                config,
            )
            .await
    }

    fn seed(downloader: &Downloader, bucket: &str, sample: Sample) {
        downloader
            .bucketed_stats
            .seed(&BucketName::new(bucket).unwrap(), sample);
    }

    fn seed_latency(downloader: &Downloader, bucket: &str, millis: u64) {
        downloader
            .bucketed_stats
            .seed_latency(&BucketName::new(bucket).unwrap(), millis);
    }

    fn record_outcome(downloader: &Downloader, bucket: &str, outcome: Outcome) {
        downloader
            .bucketed_stats
            .begin(
                &BucketName::new(bucket).unwrap(),
                None,
                &RequestTimeline::default(),
                4,
                Duration::ZERO,
            )
            .complete(outcome);
    }

    fn model(downloader: &Downloader, bucket: &str) -> LatencyModel {
        downloader
            .bucketed_stats
            .model(&BucketName::new(bucket).unwrap())
    }

    /// The uncapped body tail for the 4-byte objects tests read.
    fn object_body_tail(downloader: &Downloader, bucket: &str) -> Option<Duration> {
        model(downloader, bucket).body.tail(4)
    }

    /// The uncapped time to first byte for the 4-byte objects tests read.
    fn object_first_byte_tail(downloader: &Downloader, bucket: &str) -> Option<Duration> {
        model(downloader, bucket).first_byte.tail(4)
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
            let result = downloader
                .read_response(&(0..10), output, &RequestTimeline::default())
                .await;
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
                downloader.read_response(&(0..10), output, &RequestTimeline::default()),
            )
            .await
            .expect("excess data must be rejected without waiting for the rest of the body");
            assert!(matches!(result, Err(DownloadError::InvalidResponse(_))));
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

    #[tokio::test(start_paused = true)]
    async fn replica_range_errors_retain_size_across_backend_failures() {
        for fallback_size in [Some(0), None] {
            let (downloader, script) = scripted_downloader([
                ("primary", 1, 0, false),
                ("peer", 1, 0, false),
                ("fallback", 1, 0, false),
            ]);
            script
                .service_errors
                .lock()
                .extend([(0, 416), (1, 503), (2, 416)]);
            script.range_error_sizes.lock().insert(0, 17);
            if let Some(size) = fallback_size {
                script.range_error_sizes.lock().insert(2, size);
            }
            let buckets = BucketNameSet::new(
                ["primary", "peer", "fallback"]
                    .into_iter()
                    .map(|name| BucketName::new(name).unwrap()),
            )
            .unwrap();
            let error = downloader
                .download(
                    &buckets,
                    ObjectKey::new("object").unwrap(),
                    &(32..64),
                    None,
                    &RequestConfig::default(),
                )
                .await
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    DownloadError::RangeNotSatisfied { ref requested, object_size: Some(17) }
                        if requested == &(32..64)
                ),
                "{error:?}"
            );
            assert_eq!(script.requests.load(Ordering::SeqCst), 3);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn misses_ranges_and_overload_are_recognised_by_status_or_code() {
        let missing = DownloadError::NoSuchKey;
        let unsatisfied = DownloadError::RangeNotSatisfied {
            requested: 0..4,
            object_size: None,
        };
        let overloaded = DownloadError::Overloaded(String::new());
        let unknown = DownloadError::Unknown(String::new());
        for (status, body, expected) in [
            (503, "<Error><Code>SlowDown</Code></Error>", &overloaded),
            (
                503,
                "<Error><Code>ServiceUnavailable</Code></Error>",
                &overloaded,
            ),
            (503, "", &overloaded),
            (429, "", &overloaded),
            (
                429,
                "<html><body>Too Many Requests</body></html>",
                &overloaded,
            ),
            (
                400,
                "<Error><Code>TooManyRequests</Code></Error>",
                &overloaded,
            ),
            (404, "", &missing),
            (404, "<html><body>Not Found</body></html>", &missing),
            (404, "<Error><Code>NoSuchBucket</Code></Error>", &unknown),
            (416, "", &unsatisfied),
            (
                416,
                "<html><body>Range Not Satisfiable</body></html>",
                &unsatisfied,
            ),
            (500, "<Error><Code>InternalError</Code></Error>", &unknown),
            (500, "", &unknown),
            (502, "<html><body>Bad Gateway</body></html>", &unknown),
        ] {
            let (downloader, script) = scripted_downloader([("bucket", 1, 0, false)]);
            script.service_errors.lock().insert(0, status);
            script.service_error_bodies.lock().insert(0, body);
            let result =
                fetch_with_config(&downloader, &["bucket"], &RequestConfig::default()).await;
            let error = result.as_ref().err();
            assert_eq!(
                error.map(discriminant),
                Some(discriminant(expected)),
                "{status} {body:?}: {result:?}"
            );
            if let Some(DownloadError::Overloaded(message)) = error {
                assert!(
                    message.starts_with(&format!("HTTP {status}: ")),
                    "{message}"
                );
            }
            let failed = !matches!(
                expected,
                DownloadError::NoSuchKey | DownloadError::RangeNotSatisfied { .. }
            );
            assert_eq!(
                metrics(&downloader, "bucket").await.consecutive_failures,
                u32::from(failed),
                "{status} {body:?}"
            );
        }
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
        for (range, object_size) in [
            (Range { start: 10, end: 10 }, None),
            (Range { start: 10, end: 1 }, None),
            (10..20, Some(10)),
        ] {
            let result = fetch_range(
                &downloader,
                &["bucket"],
                range,
                object_size,
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
        for (max_inflight_bytes, download_timeout) in [
            (0, Duration::from_secs(1)),
            (u64::MAX, Duration::from_secs(1)),
            (4, Duration::ZERO),
            (4, Duration::MAX),
        ] {
            let limits = DownloadLimits {
                download_timeout,
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
        let second = fetch(&downloader, &["bucket"]).await;
        assert!(
            !second.hedged,
            "a normal body transfer should not trigger a hedge"
        );
        assert_eq!(second.piece.data, Bytes::from_static(b"data"));
    }

    #[tokio::test(start_paused = true)]
    async fn header_stalls_superseded_by_hedges_keep_being_hedged() {
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
            let output = fetch(&downloader, &["bucket"]).await;
            assert!(output.hedged);
            assert_eq!(output.latency, Duration::from_millis(60));
            advance(Duration::from_secs(1)).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_copy_releases_its_admission_while_its_peer_continues() {
        for (primary, hedge, elapsed_ms) in [
            (("bucket", 5, 300, false), ("bucket", 1, 1, true), 305),
            (("bucket", 5, 120, true), ("bucket", 1, 300, false), 396),
        ] {
            let (downloader, script) =
                scripted_downloader([("bucket", 10, 90, false), primary, hedge]);
            let downloader = downloader
                .with_test_limits(DownloadLimits {
                    max_inflight_requests: 2,
                    max_inflight_bytes: 8,
                    ..DownloadLimits::default()
                })
                .unwrap();
            fetch(&downloader, &["bucket"]).await;
            let (output, admitted_during_peer) =
                tokio::join!(fetch(&downloader, &["bucket"]), async {
                    sleep(Duration::from_millis(150)).await;
                    downloader.admission.try_acquire(4).is_some()
                });
            assert!(output.hedged);
            assert_eq!(output.latency, Duration::from_millis(elapsed_ms));
            assert!(admitted_during_peer);
            assert_eq!(script.requests.load(Ordering::SeqCst), 3);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_conclusive_hedge_error_does_not_hide_its_peers_overload() {
        let (downloader, script) = scripted_downloader([
            ("bucket", 10, 90, false),
            ("bucket", 50, 0, false),
            ("bucket", 60, 0, false),
        ]);
        script.service_errors.lock().extend([(1, 503), (2, 404)]);
        fetch(&downloader, &["bucket"]).await;
        let result = fetch_with_config(&downloader, &["bucket"], &RequestConfig::default()).await;
        assert!(
            matches!(result, Err(DownloadError::NoSuchKey)),
            "{result:?}"
        );
        let metrics = metrics(&downloader, "bucket").await;
        assert!(metrics.deprioritized);
        assert_eq!(metrics.consecutive_failures, 1);
        assert_eq!(script.requests.load(Ordering::SeqCst), 3);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_body_failing_after_headers_is_retried_against_its_bucket() {
        let (downloader, script) =
            scripted_downloader([("bucket", 5, 5, true), ("bucket", 5, 5, false)]);
        let output = fetch(&downloader, &["bucket"]).await;
        assert_eq!(output.latency, Duration::from_millis(20));
        assert!(!output.hedged);
        assert!(metrics(&downloader, "bucket").await.deprioritized);
        assert_eq!(script.requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_retries_a_failed_body_once_and_only_within_the_retry_budget() {
        for funded in [true, false] {
            let (downloader, script) =
                scripted_downloader([("bucket", 5, 5, true), ("bucket", 5, 5, true)]);
            if funded {
                for _ in 0..20 {
                    downloader.attempt_budget.observe_success();
                }
            } else {
                spend_retry_budget(&downloader);
            }
            let result =
                fetch_with_config(&downloader, &["bucket"], &RequestConfig::default()).await;
            assert!(
                matches!(result, Err(DownloadError::BodyStreaming(_))),
                "{result:?}"
            );
            assert_eq!(
                script.requests.load(Ordering::SeqCst),
                if funded { 2 } else { 1 }
            );
            assert_eq!(script.active.load(Ordering::SeqCst), 0);
        }
    }

    /// Another read queued for the only admission slot takes it when the body fails.
    #[tokio::test(start_paused = true)]
    async fn a_body_retry_never_queues_for_admission() {
        let (downloader, script) = scripted_downloader([("bucket", 5, 5, true)]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                max_inflight_requests: 1,
                ..DownloadLimits::default()
            })
            .unwrap();
        let config = RequestConfig::default();
        let start = tokio::time::Instant::now();
        let (result, _queued) = tokio::join!(
            fetch_with_config(&downloader, &["bucket"], &config),
            async {
                sleep(Duration::from_millis(1)).await;
                downloader
                    .admission
                    .acquire(4, start + Duration::from_secs(60), Duration::from_secs(60))
                    .await
                    .unwrap()
            },
        );
        assert!(
            matches!(result, Err(DownloadError::BodyStreaming(_))),
            "{result:?}"
        );
        assert_eq!(start.elapsed(), Duration::from_millis(10));
        assert_eq!(script.requests.load(Ordering::SeqCst), 1);
        assert!(downloader.attempt_budget.try_retry());
    }

    #[tokio::test(start_paused = true)]
    async fn a_stall_shared_by_a_same_bucket_hedge_counts_once() {
        let (downloader, script) =
            scripted_downloader([("bucket", 1_000, 0, false), ("bucket", 1_000, 0, false)]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                bucket_timeout: Duration::from_millis(200),
                ..DownloadLimits::default()
            })
            .unwrap();
        seed_latency(&downloader, "bucket", 10);
        let result = fetch_with_config(&downloader, &["bucket"], &RequestConfig::default()).await;
        assert!(
            matches!(result, Err(DownloadError::Timeout { .. })),
            "{result:?}"
        );
        assert_eq!(script.requests.load(Ordering::SeqCst), 2);
        assert_eq!(metrics(&downloader, "bucket").await.consecutive_failures, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_known_object_size_admits_a_page_read_at_the_size_it_returns() {
        let (downloader, _) = scripted_downloader([("local", 1, 0, false)]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                max_inflight_bytes: MIB,
                ..DownloadLimits::default()
            })
            .unwrap();
        let read = async |object_size| {
            fetch_range(
                &downloader,
                &["local"],
                0..16 * MIB,
                object_size,
                &RequestConfig::default(),
            )
            .await
        };
        assert!(matches!(
            read(None).await,
            Err(DownloadError::AdmissionExhausted { .. })
        ));
        assert_eq!(read(Some(4)).await.unwrap().piece.data.len(), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn an_understated_object_size_is_measured_at_the_returned_length() {
        let (downloader, _) = scripted_downloader([("local", 1, 0, false)]);
        fetch_range(
            &downloader,
            &["local"],
            0..16 * MIB,
            Some(2),
            &RequestConfig::default(),
        )
        .await
        .unwrap();
        let request = model(&downloader, "local").request;
        assert!(request.tail(4).is_some());
        assert_eq!(request.tail(16 * MIB), None);
    }

    /// Once headers announce a small object, a page read is overdue by the standard of
    /// small objects rather than of the large pages that dominate its request tail.
    #[tokio::test(start_paused = true)]
    async fn a_page_read_of_a_small_object_is_judged_by_its_announced_length() {
        let page = 16 * MIB;
        let (downloader, _) = scripted_downloader([
            ("local", 10, 1_000, false),
            ("local", 10, 1_000, false),
            ("local", 10, 1_000, false),
            ("peer", 10, 5, false),
        ]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                hedge_budget_percent: 0,
                ..DownloadLimits::default()
            })
            .unwrap();
        for (bucket, count, first_byte_ms, body_bytes, body_ms) in [
            ("local", 5, 10, 4, 1),
            ("local", 3, 10, page, 400),
            ("peer", 1, 45, 4, 5),
        ] {
            for _ in 0..count {
                seed(
                    &downloader,
                    bucket,
                    Sample {
                        requested_bytes: page,
                        first_byte: Duration::from_millis(first_byte_ms),
                        body_bytes,
                        body: Duration::from_millis(body_ms),
                    },
                );
            }
        }
        let read = async |buckets: &[&str]| {
            fetch_range(
                &downloader,
                buckets,
                0..page,
                None,
                &RequestConfig::default(),
            )
            .await
            .unwrap()
        };
        let (.., output) = tokio::join!(
            read(&["local"]),
            read(&["local"]),
            read(&["local"]),
            async {
                sleep(Duration::from_millis(200)).await;
                read(&["local", "peer"]).await
            }
        );
        assert_eq!(output.used_bucket_idx, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_primary_failure_survives_its_hedges_timeout() {
        let (downloader, script) =
            scripted_downloader([("bucket", 1, 92, true), ("bucket", 300, 0, false)]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                bucket_timeout: Duration::from_millis(200),
                ..DownloadLimits::default()
            })
            .unwrap();
        seed(&downloader, "bucket", Sample::from_millis(10, 4, 90));
        let start = tokio::time::Instant::now();
        let result = fetch_with_config(&downloader, &["bucket"], &RequestConfig::default()).await;
        assert!(
            matches!(result, Err(DownloadError::BodyStreaming(_))),
            "{result:?}"
        );
        assert_eq!(start.elapsed(), Duration::from_millis(200));
        assert!(metrics(&downloader, "bucket").await.deprioritized);
        assert_eq!(script.requests.load(Ordering::SeqCst), 2);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn timeouts_allow_an_unmeasured_body_beyond_the_first_byte_tail() {
        for (headers, budget_ms, counts) in
            [(false, 28, false), (true, 28, false), (true, 50, true)]
        {
            let downloader = downloader([]);
            seed_latency(&downloader, "bucket", 30);
            let bucket = BucketName::new("bucket").unwrap();
            let object = ObjectKey::new("object").unwrap();
            let timeline = RequestTimeline::default();
            let result = super::BucketOperation {
                downloader: &downloader,
                request: super::PieceRequest {
                    bucket: &bucket,
                    object: &object,
                    range: &(0..4),
                    bytes: 4,
                    config: &RequestConfig::default(),
                },
                model: model(&downloader, "bucket"),
                timeline: &timeline,
                deadline: &BucketDeadline::new(
                    tokio::time::Instant::now() + Duration::from_millis(budget_ms),
                ),
                unmeasured: Duration::from_millis(20),
                probe: None,
            }
            .execute(None, async || {
                timeline.start();
                sleep(Duration::from_millis(10)).await;
                if headers {
                    timeline.receive_headers(16 * MIB);
                }
                std::future::pending::<Result<(), DownloadError>>().await
            })
            .await;
            assert!(
                matches!(result, Err(DownloadError::Timeout { .. })),
                "{result:?}"
            );
            assert_eq!(
                metrics(&downloader, "bucket").await.deprioritized,
                counts,
                "headers: {headers}, budget: {budget_ms}ms"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_hedge_timeout_does_not_mask_the_primarys_failure() {
        let (downloader, script) = scripted_downloader([
            ("bucket", 10, 200, false),
            ("bucket", 20, 30, true),
            ("bucket", 1_000, 0, false),
        ]);
        fetch(&downloader, &["bucket"]).await;
        spend_retry_budget(&downloader);
        let config = RequestConfig {
            operation_timeout: Some(Duration::from_millis(50)),
            ..RequestConfig::default()
        };
        let result = fetch_with_config(&downloader, &["bucket"], &config).await;
        assert!(
            matches!(result, Err(DownloadError::BodyStreaming(_))),
            "{result:?}"
        );
        assert_eq!(metrics(&downloader, "bucket").await.consecutive_failures, 1);
        assert_eq!(script.requests.load(Ordering::SeqCst), 3);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_retry_past_its_buckets_deadline_spends_no_retry_credit() {
        let (downloader, script) =
            scripted_downloader([("local", 5, 5, true), ("peer", 1_000, 0, false)]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                bucket_timeout: Duration::from_millis(100),
                download_timeout: Duration::from_secs(1),
                ..DownloadLimits::default()
            })
            .unwrap();
        seed_latency(&downloader, "local", 2);
        let start = tokio::time::Instant::now();
        let result =
            fetch_with_config(&downloader, &["local", "peer"], &RequestConfig::default()).await;
        assert!(
            matches!(result, Err(DownloadError::BodyStreaming(_))),
            "{result:?}"
        );
        assert_eq!(start.elapsed(), Duration::from_millis(102));
        assert_eq!(script.requests.load(Ordering::SeqCst), 2);
        assert!(downloader.attempt_budget.try_retry());
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    /// An unmeasured body needs half the bucket's share of the download timeout.
    #[tokio::test(start_paused = true)]
    async fn a_body_retry_shares_its_buckets_deadline() {
        for (bucket_timeout_ms, elapsed_ms, requests) in [(100, 86, 2), (400, 401, 3)] {
            let (downloader, script) = scripted_downloader([
                ("local", 1, 0, false),
                ("peer", 5, 80, true),
                ("peer", 1_000, 0, false),
            ]);
            script.service_errors.lock().insert(0, 404);
            let downloader = downloader
                .with_test_limits(DownloadLimits {
                    bucket_timeout: Duration::from_millis(bucket_timeout_ms),
                    download_timeout: Duration::from_secs(1),
                    ..DownloadLimits::default()
                })
                .unwrap();
            let start = tokio::time::Instant::now();
            let result =
                fetch_with_config(&downloader, &["local", "peer"], &RequestConfig::default()).await;
            assert!(
                matches!(result, Err(DownloadError::BodyStreaming(_))),
                "{result:?}"
            );
            assert_eq!(start.elapsed(), Duration::from_millis(elapsed_ms));
            assert_eq!(script.requests.load(Ordering::SeqCst), requests);
            assert_eq!(script.active.load(Ordering::SeqCst), 0);
            assert_eq!(
                downloader.attempt_budget.try_retry(),
                requests == 2,
                "{bucket_timeout_ms}ms"
            );
        }
    }

    /// Both bodies fail, but only the peer has enough time left for another request.
    #[tokio::test(start_paused = true)]
    async fn a_body_retry_goes_to_a_failed_bucket_with_time_left() {
        let (downloader, script) = scripted_downloader([
            ("local", 10, 440, true),
            ("peer", 10, 100, true),
            ("peer", 10, 20, false),
        ]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                bucket_timeout: Duration::from_millis(500),
                ..DownloadLimits::default()
            })
            .unwrap();
        for bucket in ["local", "peer"] {
            seed(&downloader, bucket, Sample::from_millis(20, 4, 80));
        }
        let output = fetch(&downloader, &["local", "peer"]).await;
        assert_eq!(output.used_bucket_idx, 1);
        assert_eq!(output.latency, Duration::from_millis(480));
        assert_eq!(script.requests.load(Ordering::SeqCst), 3);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    /// The local primary's body hedge is due at 59ms, after a rescue gave the peer a copy at 58ms.
    fn hedge_after_rescue(responses: [ResponseStep; 3]) -> (Downloader, Arc<ScriptState>) {
        let (downloader, script) = scripted_downloader(responses);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                download_timeout: Duration::from_millis(150),
                ..DownloadLimits::default()
            })
            .unwrap();
        for (first_byte_ms, body_ms) in [(30, 2), (2, 30), (29, 29)] {
            seed(
                &downloader,
                "local",
                Sample::from_millis(first_byte_ms, 4, body_ms),
            );
        }
        seed_latency(&downloader, "peer", 55);
        (downloader, script)
    }

    #[tokio::test(start_paused = true)]
    async fn a_hedge_does_not_return_to_a_primary_reporting_overload() {
        let (downloader, script) = hedge_after_rescue([
            ("local", 29, 1_000, false),
            ("peer", 1_000, 0, false),
            ("local", 0, 0, false),
        ]);
        record_outcome(&downloader, "local", Outcome::Overload);
        for _ in 0..2 {
            record_outcome(&downloader, "peer", Outcome::Failure);
        }
        let result =
            fetch_with_config(&downloader, &["local", "peer"], &RequestConfig::default()).await;
        assert!(
            matches!(result, Err(DownloadError::Timeout { .. })),
            "{result:?}"
        );
        assert_eq!(script.requests.load(Ordering::SeqCst), 2);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_same_bucket_miss_ends_the_read_once_no_other_bucket_can_serve_it() {
        let (downloader, script) = hedge_after_rescue([
            ("local", 29, 1_000, false),
            ("peer", 1, 0, false),
            ("local", 1, 0, false),
        ]);
        script.service_errors.lock().extend([(1, 404), (2, 404)]);
        let start = tokio::time::Instant::now();
        let result =
            fetch_with_config(&downloader, &["local", "peer"], &RequestConfig::default()).await;
        assert!(
            matches!(result, Err(DownloadError::NoSuchKey)),
            "{result:?}"
        );
        assert_eq!(start.elapsed(), Duration::from_millis(60));
        assert_eq!(script.requests.load(Ordering::SeqCst), 3);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn settling_a_bucket_releases_capacity_for_another_replicas_rescue() {
        for (status, failed_copy) in [(404, 0), (404, 1), (416, 0), (416, 1)] {
            let (downloader, script) = scripted_downloader([
                ("local", if failed_copy == 0 { 20 } else { 1000 }, 0, false),
                ("local", if failed_copy == 1 { 1 } else { 1000 }, 0, false),
                ("peer", 1000, 0, false),
                ("third", 1, 1, false),
            ]);
            script.service_errors.lock().insert(failed_copy, status);
            let downloader = downloader
                .with_test_limits(DownloadLimits {
                    download_timeout: Duration::from_millis(300),
                    max_inflight_requests: 2,
                    ..DownloadLimits::default()
                })
                .unwrap();
            for (bucket, millis) in [("local", 10), ("peer", 20), ("third", 30)] {
                seed_latency(&downloader, bucket, millis);
            }
            for bucket in ["peer", "third"] {
                record_outcome(&downloader, bucket, Outcome::Overload);
            }
            let output = fetch(&downloader, &["local", "peer", "third"]).await;
            assert_eq!(output.used_bucket_idx, 2);
            assert!(output.latency < Duration::from_millis(300));
            assert_eq!(script.requests.load(Ordering::SeqCst), 4);
            assert_eq!(script.active.load(Ordering::SeqCst), 0);
            assert_eq!(metrics(&downloader, "local").await.consecutive_failures, 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_primary_finishing_with_or_before_its_hedge_wins_and_is_measured() {
        for hedge_body_ms in [190, 40] {
            let downloader = downloader([
                ("bucket", 10, 90, false),
                ("bucket", 10, 140, false),
                ("bucket", 10, hedge_body_ms, false),
            ]);
            fetch(&downloader, &["bucket"]).await;

            let output = fetch(&downloader, &["bucket"]).await;
            assert!(output.hedged);
            assert_eq!(
                output.hedge,
                Some(Hedge {
                    phase: HedgePhase::Body,
                    same_bucket: true,
                    won: false,
                })
            );
            assert_eq!(output.latency, Duration::from_millis(150));
            assert_eq!(output.piece.data, Bytes::from_static(b"data"));

            let metrics = metrics(&downloader, "bucket").await;
            assert_eq!(
                object_first_byte_tail(&downloader, "bucket"),
                Some(Duration::from_millis(10))
            );
            assert_eq!(
                object_body_tail(&downloader, "bucket"),
                Some(Duration::from_millis(140))
            );
            assert!(metrics.error_rate.abs() < f64::EPSILON);
            assert_eq!(metrics.consecutive_failures, 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn header_stalls_count_once_they_outlast_the_complete_request_tail() {
        for (bucket_timeout_ms, counts) in [(300, false), (500, true)] {
            let (downloader, script) = scripted_downloader([("bucket", 1_000, 0, false)]);
            let downloader = downloader
                .with_test_limits(DownloadLimits {
                    bucket_timeout: Duration::from_millis(bucket_timeout_ms),
                    hedge_budget_percent: 0,
                    ..DownloadLimits::default()
                })
                .unwrap();
            seed(&downloader, "bucket", Sample::from_millis(5, 4 * MIB, 400));
            assert!(matches!(
                fetch_with_config(&downloader, &["bucket"], &RequestConfig::default()).await,
                Err(DownloadError::Timeout { .. })
            ));
            assert_eq!(
                metrics(&downloader, "bucket").await.deprioritized,
                counts,
                "{bucket_timeout_ms}ms"
            );
            assert_eq!(script.active.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn either_peer_recovers_a_body_failure_that_still_counts_against_the_bucket() {
        for (primary, hedge, elapsed_ms) in [
            (("bucket", 10, 100, true), ("bucket", 10, 20, false), 130),
            (("bucket", 10, 190, false), ("bucket", 5, 10, true), 200),
        ] {
            let downloader = downloader([("bucket", 10, 90, false), primary, hedge]);
            fetch(&downloader, &["bucket"]).await;
            let output = fetch(&downloader, &["bucket"]).await;
            assert!(output.hedged);
            assert_eq!(output.latency, Duration::from_millis(elapsed_ms));
            assert_eq!(metrics(&downloader, "bucket").await.consecutive_failures, 1);
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
        assert_eq!(
            object_body_tail(&downloader, "primary"),
            Some(Duration::from_millis(90))
        );
        assert_eq!(
            object_body_tail(&downloader, "fallback"),
            Some(Duration::from_millis(20))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn download_deadline_caps_single_bucket_and_fallback() {
        for buckets in [&["primary"][..], &["primary", "fallback"][..]] {
            let (downloader, script) =
                scripted_downloader(buckets.iter().map(|bucket| (*bucket, 10, 1_000, false)));
            let downloader = downloader
                .with_test_limits(DownloadLimits {
                    bucket_timeout: Duration::from_millis(500),
                    download_timeout: Duration::from_millis(300),
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
                assert_eq!(metrics(&downloader, bucket).await.consecutive_failures, 1);
                let model = model(&downloader, bucket);
                assert!(model.request.tail(4).is_none());
                assert_eq!(model.first_byte.tail(4), Some(Duration::from_millis(10)));
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
    async fn same_bucket_hedges_need_the_request_tail_before_the_bucket_deadline() {
        // Timer rounding reaches the bucket deadline. A later deadline must also leave
        // time for a fresh request.
        for (body_tail_us, body_ms, bucket_timeout_ms, requests) in [
            (90_000, 90, 100, 1),
            (90_000, 400, 100, 1),
            (89_500, 90, 100, 1),
            (89_500, 400, 100, 1),
            (89_500, 400, 199, 1),
            (89_500, 400, 200, 2),
        ] {
            let responses = [("bucket", 10, body_ms, false), ("bucket", 0, 0, false)];
            let (downloader, script) = scripted_downloader(responses.into_iter().take(requests));
            let downloader = downloader
                .with_test_limits(DownloadLimits {
                    bucket_timeout: Duration::from_millis(bucket_timeout_ms),
                    ..DownloadLimits::default()
                })
                .unwrap();
            seed(
                &downloader,
                "bucket",
                Sample {
                    requested_bytes: 4,
                    first_byte: Duration::from_millis(10),
                    body_bytes: 4,
                    body: Duration::from_micros(body_tail_us),
                },
            );
            let result =
                fetch_with_config(&downloader, &["bucket"], &RequestConfig::default()).await;
            if body_ms == 90 {
                assert_eq!(result.unwrap().latency, Duration::from_millis(100));
            } else if requests == 1 {
                assert!(matches!(result, Err(DownloadError::Timeout { .. })));
            } else {
                assert!(result.unwrap().hedged);
            }
            assert_eq!(script.requests.load(Ordering::SeqCst), requests);
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
                seed_latency(&downloader, "local", 50);
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
            seed_latency(&downloader, "local", 5);
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
            assert_eq!(metrics(&downloader, "local").await.consecutive_failures, 1);
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
                        None,
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
        assert!(!fetch(&downloader, &["bucket"]).await.hedged);
        let output = fetch(&downloader, &["bucket"]).await;
        assert!(output.hedged);
        assert_eq!(output.latency, Duration::from_millis(130));
        assert_eq!(script.requests.load(Ordering::SeqCst), 6);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn small_announced_bodies_allow_hedges_and_retries_after_large_reads() {
        for fail_body in [false, true] {
            let (downloader, script) = scripted_downloader([
                ("bucket", 5, if fail_body { 10 } else { 1000 }, fail_body),
                ("bucket", 5, 1, false),
            ]);
            let downloader = downloader
                .with_test_limits(DownloadLimits {
                    bucket_timeout: Duration::from_millis(100),
                    hedge_budget_percent: if fail_body { 0 } else { 5 },
                    ..DownloadLimits::default()
                })
                .unwrap();
            seed(&downloader, "bucket", Sample::from_millis(5, 16 * MIB, 200));
            let output = fetch(&downloader, &["bucket"]).await;
            assert!(output.latency < Duration::from_millis(30));
            assert_eq!(output.hedged, !fail_body);
            assert_eq!(script.requests.load(Ordering::SeqCst), 2);
            assert_eq!(script.active.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn small_announced_bodies_can_time_out_after_large_reads() {
        let downloader = downloader([("bucket", 5, 1000, false)])
            .with_test_limits(DownloadLimits {
                bucket_timeout: Duration::from_millis(100),
                hedge_budget_percent: 0,
                ..DownloadLimits::default()
            })
            .unwrap();
        seed(&downloader, "bucket", Sample::from_millis(5, 16 * MIB, 200));
        let result = fetch_with_config(&downloader, &["bucket"], &RequestConfig::default()).await;
        assert!(matches!(result, Err(DownloadError::Timeout { .. })));
        assert_eq!(metrics(&downloader, "bucket").await.consecutive_failures, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn hedges_do_not_retry_but_primary_sdk_retries_remain() {
        let (downloader, script) = scripted_downloader([
            ("bucket", 10, 0, false),
            ("bucket", 10, 200, false),
            ("bucket", 10, 0, false),
        ]);
        script.service_errors.lock().extend([(0, 503), (2, 503)]);
        seed_latency(&downloader, "bucket", 100);
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
        assert_eq!(metrics(&downloader, "bucket").await.consecutive_failures, 1);
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
                download_timeout: Duration::from_millis(500),
                hedge_budget_percent: 0,
                ..DownloadLimits::default()
            })
            .unwrap();
        for (bucket, millis) in [("local", 10), ("peer", 60), ("third", 150)] {
            seed_latency(&downloader, bucket, millis);
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
                    download_timeout: Duration::from_millis(400),
                    hedge_budget_percent: 0,
                    ..DownloadLimits::default()
                })
                .unwrap();
            for (bucket, latency) in [("local", 250), ("peer", 300), ("third", 300)] {
                seed_latency(&downloader, bucket, latency);
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
        seed_latency(&downloader, "local", 3);
        seed_latency(&downloader, "peer", 5);
        record_outcome(&downloader, "local", Outcome::Failure);
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
                download_timeout: Duration::from_millis(100),
                hedge_budget_percent: 0,
                ..DownloadLimits::default()
            })
            .unwrap();
        for (bucket, millis) in [("local", 10), ("peer", 60), ("third", 150)] {
            seed_latency(&downloader, bucket, millis);
        }
        for bucket in ["local", "peer"] {
            record_outcome(&downloader, bucket, Outcome::Failure);
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
            seed_latency(&downloader, bucket, millis);
        }
        let output = fetch(&downloader, &["local", "peer", "third"]).await;
        assert_eq!(output.used_bucket_idx, 0);
        assert_eq!(output.latency, Duration::from_millis(40));
        assert_eq!(script.requests.load(Ordering::SeqCst), 3);
        assert!(metrics(&downloader, "peer").await.deprioritized);
        assert_eq!(
            model(&downloader, "peer").request.tail(4),
            Some(Duration::from_millis(20))
        );
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_rescue_does_not_cancel_the_primarys_body_hedge() {
        let (downloader, script) = scripted_downloader([
            ("local", 5, 1_000, false),
            ("peer", 1, 0, false),
            ("local", 0, 5, false),
        ]);
        script.service_errors.lock().insert(1, 404);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                download_timeout: Duration::from_millis(100),
                ..DownloadLimits::default()
            })
            .unwrap();
        seed(&downloader, "local", Sample::from_millis(10, 4, 40));
        seed_latency(&downloader, "peer", 60);
        let output = fetch(&downloader, &["local", "peer"]).await;
        assert_eq!(output.used_bucket_idx, 0);
        assert!(output.hedged);
        assert_eq!(output.latency, Duration::from_millis(50));
        assert_eq!(script.requests.load(Ordering::SeqCst), 3);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_fallback_with_too_little_time_does_not_poison_its_health() {
        let (downloader, script) =
            scripted_downloader([("local", 0, 99, true), ("peer", 0, 5, false)]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                download_timeout: Duration::from_millis(100),
                hedge_budget_percent: 0,
                ..DownloadLimits::default()
            })
            .unwrap();
        seed_latency(&downloader, "local", 99);
        seed_latency(&downloader, "peer", 100);
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
                download_timeout: Duration::from_millis(10),
                ..DownloadLimits::default()
            })
            .unwrap();
        let permit = downloader.admission.try_acquire(4).unwrap();
        let result = fetch_with_config(&downloader, &["local"], &RequestConfig::default()).await;
        assert!(matches!(result, Err(DownloadError::AdmissionTimeout)));
        let result = super::BucketOperation {
            downloader: &downloader,
            request: super::PieceRequest {
                bucket: &BucketName::new("local").unwrap(),
                object: &ObjectKey::new("object").unwrap(),
                range: &(0..4),
                bytes: 4,
                config: &RequestConfig::default(),
            },
            model: model(&downloader, "local"),
            timeline: &RequestTimeline::default(),
            deadline: &BucketDeadline::new(tokio::time::Instant::now()),
            unmeasured: Duration::from_millis(5),
            probe: None,
        }
        .execute(Some(permit), async || Ok(()))
        .await;
        assert!(matches!(result, Err(DownloadError::Timeout { .. })));
        assert_eq!(script.requests.load(Ordering::SeqCst), 0);
        let local = metrics(&downloader, "local").await;
        assert!(!local.deprioritized);
        assert_eq!(local.consecutive_failures, 0);
        assert!(local.error_rate.abs() < f64::EPSILON);
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
            record_outcome(&downloader, bucket, Outcome::Overload);
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
        seed_latency(&downloader, "local", 10);
        seed_latency(&downloader, "peer", 20);
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
        seed_latency(&downloader, "local", 3);
        seed_latency(&downloader, "peer", 5);
        record_outcome(&downloader, "local", Outcome::Failure);
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
        seed_latency(&downloader, "local", 3);
        seed_latency(&downloader, "peer", 5);
        record_outcome(&downloader, "local", Outcome::Failure);
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
                download_timeout: Duration::from_millis(100),
                ..DownloadLimits::default()
            })
            .unwrap();
        seed_latency(&downloader, "local", 10);
        seed_latency(&downloader, "peer", 20);
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
                download_timeout: Duration::from_millis(500),
                bucket_timeout: Duration::from_millis(500),
                hedge_budget_percent: 0,
                ..DownloadLimits::default()
            })
            .unwrap();
        seed_latency(&downloader, "local", 400);
        seed_latency(&downloader, "peer", 150);
        record_outcome(&downloader, "peer", Outcome::Failure);
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
            spend_retry_budget(&downloader);
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
            record_outcome(&downloader, bucket, Outcome::Overload);
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
    async fn denied_rescue_does_not_mask_the_active_copys_timeout() {
        let (downloader, script) = scripted_downloader([("local", 200, 0, false)]);
        let download_timeout = Duration::from_millis(100);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                download_timeout,
                bucket_timeout: download_timeout,
                hedge_budget_percent: 0,
                ..DownloadLimits::default()
            })
            .unwrap();
        for bucket in ["local", "peer"] {
            record_outcome(&downloader, bucket, Outcome::Overload);
        }
        assert!(downloader.attempt_budget.try_retry());
        let start = tokio::time::Instant::now();
        let result =
            fetch_with_config(&downloader, &["local", "peer"], &RequestConfig::default()).await;
        assert!(
            matches!(result, Err(DownloadError::Timeout { ref bucket, timeout })
                if &**bucket == "local" && timeout == download_timeout),
            "{result:?}"
        );
        assert_eq!(start.elapsed(), download_timeout);
        assert_eq!(script.requests.load(Ordering::SeqCst), 1);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn download_expiry_records_timeouts_for_copies_given_enough_time() {
        let (downloader, script) = scripted_downloader([
            ("local", 1000, 0, false),
            ("peer", 1000, 0, false),
            ("third", 1000, 0, false),
        ]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                download_timeout: Duration::from_millis(100),
                bucket_timeout: Duration::from_millis(100),
                hedge_budget_percent: 0,
                ..DownloadLimits::default()
            })
            .unwrap();
        for (bucket, latency) in [("local", 3), ("peer", 5), ("third", 6)] {
            seed_latency(&downloader, bucket, latency);
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

    #[tokio::test(start_paused = true)]
    async fn fallback_timeouts_preserve_backend_failures() {
        for peer_latency in [5, 150] {
            let (downloader, script) =
                scripted_downloader([("local", 0, 2, true), ("peer", 200, 0, false)]);
            let downloader = downloader
                .with_test_limits(DownloadLimits {
                    download_timeout: Duration::from_millis(100),
                    hedge_budget_percent: 0,
                    ..DownloadLimits::default()
                })
                .unwrap();
            seed_latency(&downloader, "local", 2);
            seed_latency(&downloader, "peer", peer_latency);
            let start = tokio::time::Instant::now();
            let result =
                fetch_with_config(&downloader, &["local", "peer"], &RequestConfig::default()).await;
            assert!(
                matches!(result, Err(DownloadError::BodyStreaming(_))),
                "{result:?}"
            );
            assert_eq!(start.elapsed(), Duration::from_millis(100));
            assert_eq!(script.requests.load(Ordering::SeqCst), 2);
            assert_eq!(script.active.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn fallback_can_succeed_despite_a_historical_tail_beyond_the_deadline() {
        let (downloader, script) =
            scripted_downloader([("local", 0, 2, true), ("peer", 5, 0, false)]);
        let downloader = downloader
            .with_test_limits(DownloadLimits {
                download_timeout: Duration::from_millis(100),
                hedge_budget_percent: 0,
                ..DownloadLimits::default()
            })
            .unwrap();
        seed_latency(&downloader, "local", 2);
        seed_latency(&downloader, "peer", 150);
        let output = fetch(&downloader, &["local", "peer"]).await;
        assert_eq!(output.used_bucket_idx, 1);
        assert_eq!(output.latency, Duration::from_millis(7));
        assert_eq!(output.piece.data, Bytes::from_static(b"data"));
        assert_eq!(script.requests.load(Ordering::SeqCst), 2);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    /// A fallback waits for admission while it could still finish in time; a body retry
    /// against the same bucket does not wait.
    #[tokio::test(start_paused = true)]
    async fn fallback_admission_timeout_preserves_backend_failure_but_overrides_missing() {
        let download_timeout = Duration::from_millis(100);
        // The peer's 5 ms reads must start 5 ms before the download deadline.
        let latest_start = Duration::from_millis(95);
        for (buckets, missing, elapsed) in [
            (&["local", "peer"][..], false, latest_start),
            (&["local", "peer"], true, latest_start),
            (&["local"], false, Duration::from_millis(2)),
        ] {
            let (downloader, script) = scripted_downloader([("local", 2, 0, true)]);
            if missing {
                script.service_errors.lock().insert(0, 404);
            }
            let downloader = downloader
                .with_test_limits(DownloadLimits {
                    download_timeout,
                    max_inflight_requests: 1,
                    max_inflight_bytes: 4,
                    hedge_budget_percent: 0,
                    ..DownloadLimits::default()
                })
                .unwrap();
            seed_latency(&downloader, "local", 2);
            seed_latency(&downloader, "peer", 5);
            let start = tokio::time::Instant::now();
            let read = async {
                let result =
                    fetch_with_config(&downloader, buckets, &RequestConfig::default()).await;
                assert_eq!(start.elapsed(), elapsed);
                result
            };
            let competing_read = async {
                sleep(Duration::from_millis(1)).await;
                let permit = downloader
                    .admission
                    .acquire(4, start + download_timeout, download_timeout)
                    .await
                    .unwrap();
                sleep(download_timeout).await;
                drop(permit);
            };
            let (result, ()) = tokio::join!(read, competing_read);
            if missing {
                assert!(
                    matches!(result, Err(DownloadError::AdmissionTimeout)),
                    "{result:?}"
                );
            } else {
                assert!(
                    matches!(result, Err(DownloadError::BodyStreaming(_))),
                    "{result:?}"
                );
            }
            assert_eq!(script.requests.load(Ordering::SeqCst), 1);
            assert_eq!(script.active.load(Ordering::SeqCst), 0);
            assert!(downloader.admission.try_acquire(4).is_some());
        }
    }

    /// Behind a standing queue, a read far from its deadline waits only as long as it is
    /// expected to take, or long enough not to shed short reads. Another bucket's copy
    /// would wait in the same queue, so it does not fall back.
    #[tokio::test(start_paused = true)]
    async fn a_read_behind_a_standing_queue_gives_up_long_before_a_distant_deadline() {
        for (latency, waited) in [(5, MIN_WAIT), (150, Duration::from_millis(150))] {
            let (downloader, _) = scripted_downloader([]);
            let downloader = downloader
                .with_test_limits(DownloadLimits {
                    max_inflight_requests: 1,
                    ..DownloadLimits::default()
                })
                .unwrap();
            for bucket in ["bucket", "peer"] {
                seed_latency(&downloader, bucket, latency);
            }
            let _held = downloader.admission.try_acquire(4).unwrap();
            let far = tokio::time::Instant::now() + Duration::from_secs(60);
            let read = async {
                sleep(Duration::from_millis(100)).await;
                let start = tokio::time::Instant::now();
                let result =
                    fetch_with_config(&downloader, &["bucket", "peer"], &RequestConfig::default())
                        .await;
                (result, start.elapsed())
            };
            let (result, elapsed) = tokio::select! {
                biased;
                read = read => read,
                _ = downloader.admission.acquire(4, far, Duration::from_secs(60)) => {
                    unreachable!("the only slot stays held")
                }
            };
            assert!(matches!(result, Err(DownloadError::AdmissionTimeout)));
            assert_eq!(elapsed, waited, "{latency}ms");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn completed_header_stalls_do_not_disable_future_hedges() {
        let (downloader, script) = scripted_downloader([
            ("bucket", 1000, 90, false),
            ("bucket", 1000, 90, false),
            ("bucket", 10, 90, false),
        ]);
        let bucket = BucketName::new("bucket").unwrap();
        for _ in 0..50 {
            seed(&downloader, "bucket", Sample::from_millis(10, 4, 90));
        }
        drop(downloader.attempt_budget.try_hedge(&bucket).unwrap());
        let first = fetch(&downloader, &["bucket"]).await;
        assert!(!first.hedged);
        advance(Duration::from_secs(1)).await;
        for _ in 0..20 {
            downloader.attempt_budget.observe_success();
        }
        let second = fetch(&downloader, &["bucket"]).await;
        assert_eq!(script.requests.load(Ordering::SeqCst), 3);
        assert!(second.hedged);
        assert_eq!(
            second.hedge,
            Some(Hedge {
                phase: HedgePhase::FirstByte,
                same_bucket: true,
                won: true,
            })
        );
        // The hedge waits 1.25 times the 10 ms p95, rounded up to the timer's millisecond.
        assert_eq!(second.latency, Duration::from_millis(113));
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_health_uses_uncapped_predictions() {
        let downloader = downloader([("bucket", 10, 400, false)])
            .with_test_limits(DownloadLimits {
                bucket_timeout: Duration::from_millis(100),
                hedge_budget_percent: 0,
                ..DownloadLimits::default()
            })
            .unwrap();
        for _ in 0..50 {
            seed(&downloader, "bucket", Sample::from_millis(10, 4, 40));
        }
        seed(&downloader, "bucket", Sample::from_millis(10, 4, 400));
        let result = fetch_with_config(&downloader, &["bucket"], &RequestConfig::default()).await;
        assert!(matches!(result, Err(DownloadError::Timeout { .. })));
        let observed = metrics(&downloader, "bucket").await;
        assert_eq!(observed.consecutive_failures, 0, "{observed:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn an_overloaded_peer_does_not_block_a_same_bucket_hedge() {
        let (downloader, script) =
            scripted_downloader([("local", 1000, 0, false), ("local", 10, 0, false)]);
        seed_latency(&downloader, "local", 10);
        seed_latency(&downloader, "peer", 10);
        record_outcome(&downloader, "peer", Outcome::Overload);

        let output = fetch(&downloader, &["local", "peer"]).await;
        assert_eq!(output.latency, Duration::from_millis(20));
        assert_eq!(output.used_bucket_idx, 0);
        assert!(output.hedged);
        assert_eq!(script.requests.load(Ordering::SeqCst), 2);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn hedging_prefers_an_eligible_untried_peer_to_the_primary() {
        let (downloader, script) =
            scripted_downloader([("local", 1000, 0, false), ("other", 10, 0, false)]);
        for bucket in ["local", "peer", "other"] {
            seed_latency(&downloader, bucket, 10);
        }
        record_outcome(&downloader, "peer", Outcome::Overload);
        for _ in 0..2 {
            record_outcome(&downloader, "other", Outcome::Failure);
        }

        let output = fetch(&downloader, &["local", "peer", "other"]).await;
        assert_eq!(output.latency, Duration::from_millis(20));
        assert_eq!(output.used_bucket_idx, 2);
        assert!(output.hedged);
        assert_eq!(script.requests.load(Ordering::SeqCst), 2);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn hedges_skip_peers_that_cannot_finish_within_the_download_or_bucket_timeout() {
        for limits in [
            DownloadLimits {
                download_timeout: Duration::from_millis(500),
                ..DownloadLimits::default()
            },
            DownloadLimits {
                bucket_timeout: Duration::from_millis(500),
                ..DownloadLimits::default()
            },
        ] {
            let (downloader, script) =
                scripted_downloader([("local", 1_000, 0, false), ("local", 10, 0, false)]);
            let downloader = downloader.with_test_limits(limits).unwrap();
            seed_latency(&downloader, "local", 10);
            seed_latency(&downloader, "distant", 2_000);
            let output = fetch(&downloader, &["local", "distant"]).await;
            assert_eq!(output.used_bucket_idx, 0);
            assert!(output.hedged);
            assert_eq!(output.latency, Duration::from_millis(20));
            assert_eq!(script.requests.load(Ordering::SeqCst), 2);
            assert_eq!(script.active.load(Ordering::SeqCst), 0);
        }
    }
}
