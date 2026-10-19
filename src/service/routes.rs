#![expect(
    clippy::unused_async_trait_impl,
    reason = "Axum extractors require futures; async keeps fallible parsing in place."
)]

use std::{
    num::{NonZeroU32, NonZeroUsize},
    ops::Range,
    time::{Duration, SystemTime},
};

use axum::{
    Json,
    extract::{FromRequestParts, Path, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header, request::Parts},
    response::{IntoResponse, Response},
};
use bytes::BytesMut;
use futures::StreamExt;
use http_body::Frame;
use http_body_util::StreamBody;
use tokio::time::Instant;
use tracing::{debug, instrument, warn};

use crate::{
    object_store::{DownloadError, RequestConfig},
    service::{CacheyService, Chunk, ServiceError, metrics},
    types::{BucketName, BucketNameSet, ObjectKey, ObjectKind},
};

const CONTENT_TYPE: &str = "application/octet-stream";
static C0_BUCKET_HEADER: HeaderName = HeaderName::from_static("c0-bucket");
static C0_CONFIG_HEADER: HeaderName = HeaderName::from_static("c0-config");

fn on_chunk_error(
    kind: &ObjectKind,
    method: &axum::http::Method,
    chunk_idx: usize,
    error: &ServiceError,
) -> (StatusCode, HeaderMap) {
    let mut headers = HeaderMap::new();
    let (status_code, metric_code) = match error {
        ServiceError::Download(DownloadError::NoSuchKey) => (StatusCode::NOT_FOUND, "not_found"),
        ServiceError::Download(DownloadError::RangeNotSatisfied { object_size, .. }) => {
            if let Some(object_size) = object_size {
                headers.insert(
                    header::CONTENT_RANGE,
                    HeaderValue::try_from(format!("bytes */{object_size}"))
                        .expect("valid content-range"),
                );
            }
            (StatusCode::RANGE_NOT_SATISFIABLE, "range_not_satisfiable")
        }
        ServiceError::Download(DownloadError::Timeout { .. }) => {
            (StatusCode::GATEWAY_TIMEOUT, "timeout")
        }
        ServiceError::Download(
            DownloadError::AdmissionTimeout
            | DownloadError::AdmissionExhausted { .. }
            | DownloadError::Overloaded(_),
        ) => (StatusCode::SERVICE_UNAVAILABLE, "overloaded"),
        ServiceError::ObjectSizeInconsistency { .. } => {
            (StatusCode::CONFLICT, "object_size_inconsistency")
        }
        err => {
            warn!(?err, ?chunk_idx, "chunk failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "internal")
        }
    };
    metrics::fetch_request_count(
        kind,
        method,
        &format!(
            "failed:{}:{metric_code}",
            if chunk_idx == 0 { "init" } else { "later" },
        ),
    );
    (status_code, headers)
}

#[derive(Debug)]
pub struct RangeHeader(pub Range<u64>);

impl<S> FromRequestParts<S> for RangeHeader
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let range_header = parts
            .headers
            .get(header::RANGE)
            .ok_or((StatusCode::BAD_REQUEST, "Range header is required"))?
            .to_str()
            .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid Range header encoding"))?;

        let parsed = http_range_header::parse_range_header(range_header)
            .map_err(|_| (StatusCode::RANGE_NOT_SATISFIABLE, "Invalid range format"))?;

        if parsed.ranges.len() != 1 {
            return Err((
                StatusCode::RANGE_NOT_SATISFIABLE,
                "Multiple ranges are not supported",
            ));
        }

        match (parsed.ranges[0].start, parsed.ranges[0].end) {
            (
                http_range_header::StartPosition::Index(first_byte),
                http_range_header::EndPosition::Index(last_byte),
            ) if first_byte <= last_byte && last_byte < super::MAX_RANGE_END => {
                Ok(Self(first_byte..(last_byte + 1)))
            }
            _ => Err((StatusCode::RANGE_NOT_SATISFIABLE, "Unsupported range")),
        }
    }
}

impl<S> FromRequestParts<S> for RequestConfig
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let Some(header_value) = parts.headers.get(&C0_CONFIG_HEADER) else {
            return Ok(RequestConfig::default());
        };

        let header_str = header_value
            .to_str()
            .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid C0-Config header encoding"))?;

        let mut config = RequestConfig::default();

        let parse_duration = |v: &str| -> Result<Duration, (StatusCode, &'static str)> {
            v.parse::<u64>().map(Duration::from_millis).map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    "Invalid duration value in C0-Config header",
                )
            })
        };

        for pair in header_str.split_whitespace() {
            let Some((key, value)) = pair.split_once('=') else {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "Malformed C0-Config header: missing '=' in key-value pair",
                ));
            };

            match key {
                "ct" => config.connect_timeout = Some(parse_duration(value)?),
                "rt" => config.read_timeout = Some(parse_duration(value)?),
                "ot" => config.operation_timeout = Some(parse_duration(value)?),
                "oat" => config.operation_attempt_timeout = Some(parse_duration(value)?),
                "ma" => {
                    config.max_attempts = Some(value.parse().map_err(|_| {
                        (
                            StatusCode::BAD_REQUEST,
                            "Invalid value for ma in C0-Config header",
                        )
                    })?);
                }
                "ib" => config.initial_backoff = Some(parse_duration(value)?),
                "mb" => config.max_backoff = Some(parse_duration(value)?),
                "fps" => {
                    config.force_path_style = Some(value.parse().map_err(|_| {
                        (
                            StatusCode::BAD_REQUEST,
                            "Invalid value for fps in C0-Config header",
                        )
                    })?);
                }
                _ => {}
            }
        }

        Ok(config)
    }
}

#[derive(Debug)]
pub struct BucketHeaders(pub Vec<BucketName>);

impl<S> FromRequestParts<S> for BucketHeaders
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let mut names = Vec::with_capacity(3);
        for value in parts.headers.get_all(&C0_BUCKET_HEADER) {
            let s = value
                .to_str()
                .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid bucket header encoding"))?;
            let bucket = BucketName::new(s).map_err(|msg| (StatusCode::BAD_REQUEST, msg))?;
            names.push(bucket);
        }
        Ok(Self(names))
    }
}

#[instrument(skip(service))]
#[allow(clippy::too_many_lines)]
pub async fn fetch(
    State(service): State<CacheyService>,
    Path((kind, object)): Path<(ObjectKind, ObjectKey)>,
    method: axum::http::Method,
    RangeHeader(byterange): RangeHeader,
    BucketHeaders(buckets): BucketHeaders,
    req_config: RequestConfig,
) -> Response {
    let start = Instant::now();

    let buckets = if buckets.is_empty() {
        BucketName::from(kind.clone()).into()
    } else {
        BucketNameSet::new(buckets.into_iter()).expect("non-empty set")
    };

    debug!(%kind, %object, ?buckets, ?byterange, "processing");

    metrics::fetch_request_count(&kind, &method, "start");

    let concurrency = if method == axum::http::Method::HEAD {
        NonZeroUsize::MIN
    } else {
        const { NonZeroUsize::new(2).unwrap() }
    };

    let mut chunks = Box::pin(
        service
            .get(
                kind.clone(),
                object,
                buckets,
                byterange.clone(),
                concurrency,
                req_config,
            )
            .peekable(),
    );

    let Some(first_chunk) = chunks.as_mut().peek().await else {
        metrics::fetch_request_count(&kind, &method, "failed:stream_empty");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "First chunk result missing",
        )
            .into_response();
    };

    let mut headers = HeaderMap::new();
    match first_chunk {
        Ok(chunk) => {
            metrics::first_chunk_latency(&kind, chunk.cached_at.is_some(), start.elapsed());
            let object_size = chunk.object_size;
            let first_byte = chunk.range.start;
            let last_byte = byterange.end.min(object_size) - 1;
            headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(CONTENT_TYPE));
            headers.insert(
                header::CONTENT_LENGTH,
                HeaderValue::from(last_byte - first_byte + 1),
            );
            headers.insert(
                header::CONTENT_RANGE,
                HeaderValue::try_from(format!("bytes {first_byte}-{last_byte}/{object_size}"))
                    .unwrap(),
            );
            headers.insert(
                header::LAST_MODIFIED,
                HeaderValue::try_from(httpdate::fmt_http_date(
                    SystemTime::UNIX_EPOCH + Duration::from_secs(u64::from(chunk.mtime)),
                ))
                .unwrap(),
            );
            headers.insert("c0-status", c0_status(chunk));
        }
        Err(e) => {
            let (status, headers) = on_chunk_error(&kind, &method, 0, e);
            return (status, headers, e.to_string()).into_response();
        }
    }

    if method == axum::http::Method::HEAD {
        metrics::fetch_request_count(&kind, &method, "success");
        return (StatusCode::PARTIAL_CONTENT, headers).into_response();
    }

    let body = StreamBody::new(async_stream::stream! {
        let mut trailers = HeaderMap::new();
        let mut chunk_idx = 0;
        while let Some(chunk) = chunks.next().await {
            match chunk {
                Ok(chunk) => {
                    if chunk_idx > 0 {
                        trailers.append("c0-status", c0_status(&chunk));
                    }
                    let is_last_chunk = chunk.range.end == byterange.end.min(chunk.object_size);
                    if is_last_chunk {
                        metrics::fetch_request_count(&kind, &method, "success");
                    }
                    yield Ok(Frame::data(chunk.data));
                    if is_last_chunk {
                        // Stop waiting for pages beyond EOF; shared cache fills can continue.
                        drop(chunks);
                        yield Ok(Frame::trailers(trailers));
                        break;
                    }
                },
                Err(err) => {
                    assert!(chunk_idx > 0, "first chunk cannot be an error since we peeked");
                    let _ = on_chunk_error(&kind, &method, chunk_idx, &err);
                    yield Err(err);
                    break;
                },
            }
            chunk_idx += 1;
        }
    });

    (
        StatusCode::PARTIAL_CONTENT,
        headers,
        axum::body::Body::new(body),
    )
        .into_response()
}

fn c0_status(chunk: &Chunk) -> HeaderValue {
    use std::fmt::Write;

    let mut buf = BytesMut::new();
    write!(
        &mut buf,
        "{}-{}; {}; {}",
        chunk.range.start,
        chunk.range.end - 1,
        chunk.bucket,
        chunk.cached_at.map_or(0, NonZeroU32::get)
    )
    .unwrap();

    HeaderValue::from_maybe_shared(buf.freeze()).expect("valid header value")
}

pub async fn metrics(State(service): State<CacheyService>) -> impl IntoResponse {
    service.observe_metrics();
    let metrics = metrics::gather();
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        metrics,
    )
}

#[derive(serde::Serialize)]
pub struct StatusBody {
    pub egress_throughput_10s_bps: f64,
    pub ingress_throughput_10s_bps: f64,
}

pub async fn stats(State(service): State<CacheyService>) -> impl IntoResponse {
    const LOOKBACK: Duration = Duration::from_secs(10);
    Json(StatusBody {
        egress_throughput_10s_bps: service.egress_throughput_bps(LOOKBACK),
        ingress_throughput_10s_bps: service.ingress_throughput_bps(LOOKBACK),
    })
}

#[cfg(feature = "jemalloc")]
pub async fn heap_profile() -> Result<impl IntoResponse, (StatusCode, String)> {
    let mut prof_ctl = jemalloc_pprof::PROF_CTL
        .as_ref()
        .ok_or((
            StatusCode::SERVICE_UNAVAILABLE,
            "Profiling not activated".to_string(),
        ))?
        .lock()
        .await;

    if !prof_ctl.activated() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "Profiling not activated".to_string(),
        ));
    }

    let pprof = prof_ctl
        .dump_pprof()
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

    Ok(([(header::CONTENT_TYPE, "application/octet-stream")], pprof))
}

#[cfg(not(feature = "jemalloc"))]
pub async fn heap_profile() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "jemalloc profiling not enabled")
}

#[cfg(feature = "jemalloc")]
pub async fn heap_flamegraph() -> Result<impl IntoResponse, (StatusCode, String)> {
    let mut prof_ctl = jemalloc_pprof::PROF_CTL
        .as_ref()
        .ok_or((
            StatusCode::SERVICE_UNAVAILABLE,
            "Profiling not activated".to_string(),
        ))?
        .lock()
        .await;

    if !prof_ctl.activated() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "Profiling not activated".to_string(),
        ));
    }

    let flamegraph = prof_ctl
        .dump_flamegraph()
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

    Ok(([(header::CONTENT_TYPE, "image/svg+xml")], flamegraph))
}

#[cfg(not(feature = "jemalloc"))]
pub async fn heap_flamegraph() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "jemalloc profiling not enabled")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::{
        extract::FromRequestParts,
        http::{HeaderValue, Method, Request, StatusCode, header},
    };

    use super::{C0_CONFIG_HEADER, RangeHeader, on_chunk_error};
    use crate::{
        object_store::{DownloadError, RequestConfig},
        service::{PAGE_SIZE, ServiceError, metrics},
        types::{BucketName, ObjectKind},
    };

    async fn parse_c0_config(
        header_value: Option<HeaderValue>,
    ) -> Result<RequestConfig, (StatusCode, &'static str)> {
        let mut request = Request::new(());
        if let Some(value) = header_value {
            request.headers_mut().insert(&C0_CONFIG_HEADER, value);
        }
        let (mut parts, ()) = request.into_parts();
        RequestConfig::from_request_parts(&mut parts, &()).await
    }

    #[tokio::test]
    async fn range_header_allows_the_last_page_within_one_tib() {
        let limit = 1 << 40;
        for (range, accepted) in [
            (limit - PAGE_SIZE..limit, true),
            (limit - 1..limit, true),
            (0..limit, true),
            (0..limit + 1, false),
            (limit..limit + 1, false),
        ] {
            let request = Request::builder()
                .header(
                    header::RANGE,
                    format!("bytes={}-{}", range.start, range.end - 1),
                )
                .body(())
                .unwrap();
            let (mut parts, ()) = request.into_parts();
            let result = RangeHeader::from_request_parts(&mut parts, &())
                .await
                .map(|header| header.0)
                .map_err(|(status, _)| status);
            assert_eq!(
                result,
                if accepted {
                    Ok(range)
                } else {
                    Err(StatusCode::RANGE_NOT_SATISFIABLE)
                }
            );
        }
    }

    #[tokio::test]
    async fn c0_config_preserves_unspecified_settings() {
        assert_eq!(
            parse_c0_config(None).await.unwrap(),
            RequestConfig::default()
        );
        assert_eq!(
            parse_c0_config(Some(HeaderValue::from_static("ct=1000")))
                .await
                .unwrap(),
            RequestConfig {
                connect_timeout: Some(Duration::from_secs(1)),
                ..RequestConfig::default()
            }
        );
    }

    #[test]
    fn download_timeout_returns_gateway_timeout() {
        let error = ServiceError::Download(DownloadError::Timeout {
            bucket: BucketName::new("bucket").unwrap(),
            timeout: Duration::from_secs(5),
        });
        let kind = ObjectKind::new("timeout-error").unwrap();
        for (chunk_idx, phase) in [(0, "init"), (1, "later")] {
            let (status, _) = on_chunk_error(&kind, &Method::GET, chunk_idx, &error);
            assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
            let snapshot = metrics::gather();
            assert!(std::str::from_utf8(&snapshot).unwrap().contains(&format!(
                "cachey_fetch_request_total{{kind=\"timeout-error\",method=\"GET\",status=\"failed:{phase}:timeout\"}} 1\n"
            )));
        }
    }

    #[test]
    fn download_admission_and_backend_overload_return_service_unavailable() {
        for error in [
            DownloadError::AdmissionTimeout,
            DownloadError::AdmissionExhausted {
                requested_bytes: 8,
                limit_bytes: 4,
            },
            DownloadError::Overloaded("SlowDown".to_owned()),
        ] {
            let error = ServiceError::Download(error);
            let (status, _) = on_chunk_error(
                &ObjectKind::new("overload-error").unwrap(),
                &Method::GET,
                0,
                &error,
            );
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        }
        let snapshot = metrics::gather();
        assert!(std::str::from_utf8(&snapshot).unwrap().contains(
            "cachey_fetch_request_total{kind=\"overload-error\",method=\"GET\",status=\"failed:init:overloaded\"} 3\n"
        ));
    }

    #[tokio::test]
    async fn c0_config_parses_settings_and_ignores_unknown_keys() {
        let config = parse_c0_config(Some(HeaderValue::from_static(
            "ct=1000 rt=2000 ot=3000 oat=1500 unknown=123 ib=100 mb=5000 ma=3 fps=true",
        )))
        .await
        .unwrap();
        assert_eq!(
            config,
            RequestConfig {
                connect_timeout: Some(Duration::from_secs(1)),
                read_timeout: Some(Duration::from_secs(2)),
                operation_timeout: Some(Duration::from_secs(3)),
                operation_attempt_timeout: Some(Duration::from_millis(1500)),
                initial_backoff: Some(Duration::from_millis(100)),
                max_backoff: Some(Duration::from_secs(5)),
                max_attempts: Some(3),
                force_path_style: Some(true),
            }
        );
    }

    #[tokio::test]
    async fn c0_config_rejects_invalid_headers() {
        for (value, message) in [
            (
                HeaderValue::from_static("ct1000"),
                "Malformed C0-Config header: missing '=' in key-value pair",
            ),
            (
                HeaderValue::from_static("ct=invalid"),
                "Invalid duration value in C0-Config header",
            ),
            (
                HeaderValue::from_static("ma=invalid"),
                "Invalid value for ma in C0-Config header",
            ),
            (
                HeaderValue::from_static("fps=1"),
                "Invalid value for fps in C0-Config header",
            ),
            (
                HeaderValue::from_bytes(&[0xff, 0xfe]).unwrap(),
                "Invalid C0-Config header encoding",
            ),
        ] {
            assert_eq!(
                parse_c0_config(Some(value)).await.unwrap_err(),
                (StatusCode::BAD_REQUEST, message)
            );
        }
    }
}
