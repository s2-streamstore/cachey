use std::{
    num::NonZeroU32,
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
use http_body_util::{BodyExt as _, StreamBody};
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
            return Ok(Self::default());
        };

        let header_str = header_value
            .to_str()
            .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid C0-Config header encoding"))?;

        let mut config = Self::default();

        for pair in header_str.split_whitespace() {
            let Some((key, value)) = pair.split_once('=') else {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "Malformed C0-Config header: missing '=' in key-value pair",
                ));
            };

            let parse_duration = |v: &str| -> Result<Duration, Self::Rejection> {
                v.parse::<u64>().map(Duration::from_millis).map_err(|_| {
                    (
                        StatusCode::BAD_REQUEST,
                        "Invalid duration value in C0-Config header",
                    )
                })
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
                            "Invalid max_attempts value in C0-Config hheader",
                        )
                    })?);
                }
                "ib" => config.initial_backoff = Some(parse_duration(value)?),
                "mb" => config.max_backoff = Some(parse_duration(value)?),
                _ => {} // Ignore unrecognized keys
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
        BucketNameSet::new(std::iter::once(kind.clone().into()))
    } else {
        BucketNameSet::new(buckets.into_iter())
    }
    .expect("non-empty set");

    debug!(%kind, %object, ?buckets, ?byterange, "processing");

    metrics::fetch_request_count(&kind, &method, "start");

    let concurrency = if method == axum::http::Method::HEAD {
        1
    } else {
        2
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
                HeaderValue::from_str(&(last_byte - first_byte + 1).to_string()).unwrap(),
            );
            headers.insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes {first_byte}-{last_byte}/{object_size}",))
                    .unwrap(),
            );
            headers.insert(
                header::LAST_MODIFIED,
                HeaderValue::from_str(&httpdate::fmt_http_date(
                    SystemTime::UNIX_EPOCH + Duration::from_secs(chunk.mtime as u64),
                ))
                .unwrap(),
            );
            headers.insert("c0-status", c0_status(chunk));
        }
        Err(e) => {
            let code = on_chunk_error(&kind, &method, 0, e);
            return (code, e.to_string()).into_response();
        }
    }

    if method == axum::http::Method::HEAD {
        metrics::fetch_request_count(&kind, &method, "success");
        return (StatusCode::PARTIAL_CONTENT, headers).into_response();
    }

    let (trailers_tx, trailers_rx) = tokio::sync::oneshot::channel::<HeaderMap>();

    let body = StreamBody::new(async_stream::stream! {
        let mut trailers = HeaderMap::new();
        let mut chunk_idx = 0;
        let mut errored = false;
        while let Some(chunk) = chunks.next().await {
            match chunk {
                Ok(chunk) => {
                    if chunk_idx > 0 {
                        trailers.append("c0-status", c0_status(&chunk));
                    }
                    yield Ok(Frame::data(chunk.data));
                    if chunk.range.end == chunk.object_size {
                        break;
                    }
                },
                Err(err) => {
                    assert!(chunk_idx > 0, "first chunk cannot be an error since we peeked");
                    on_chunk_error(&kind, &method, chunk_idx, &err);
                    yield Err(err);
                    errored = true;
                    break;
                },
            }
            chunk_idx += 1;
        }
        if !errored {
            metrics::fetch_request_count(&kind, &method, "success");
            let _ = trailers_tx.send(trailers);
        }
    })
    .with_trailers(async {
        let Ok(trailers) = trailers_rx.await else {
            return None;
        };
        Some(Ok(trailers))
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
        chunk.cached_at.map(NonZeroU32::get).unwrap_or(0)
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

fn on_chunk_error(
    kind: &ObjectKind,
    method: &axum::http::Method,
    chunk_idx: usize,
    e: &ServiceError,
) -> StatusCode {
    let (status_code, metric_code) = match e {
        ServiceError::Download(DownloadError::NoSuchKey) => (StatusCode::NOT_FOUND, "not_found"),
        ServiceError::Download(DownloadError::RangeNotSatisfied { .. }) => {
            (StatusCode::RANGE_NOT_SATISFIABLE, "range_not_satisfiable")
        }
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
            if chunk_idx == 0 { "init" } else { "later" }
        ),
    );
    status_code
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::http::{HeaderValue, Method, Request};

    use super::*;

    async fn parse_c0_config(
        header_value: &str,
    ) -> Result<RequestConfig, (StatusCode, &'static str)> {
        let req = Request::builder()
            .method(Method::GET)
            .uri("/")
            .header(&C0_CONFIG_HEADER, header_value)
            .body(())
            .unwrap();

        let (mut parts, _) = req.into_parts();
        RequestConfig::from_request_parts(&mut parts, &()).await
    }

    #[tokio::test]
    async fn test_c0_config_empty_returns_default() {
        let req = Request::builder()
            .method(Method::GET)
            .uri("/")
            .body(())
            .unwrap();

        let (mut parts, _) = req.into_parts();
        let config = RequestConfig::from_request_parts(&mut parts, &())
            .await
            .unwrap();
        assert_eq!(config, RequestConfig::default());
    }

    #[tokio::test]
    async fn test_c0_config_single_timeout() {
        let config = parse_c0_config("ct=1000").await.unwrap();
        assert_eq!(config.connect_timeout, Some(Duration::from_millis(1000)));
        assert_eq!(config.read_timeout, None);
    }

    #[tokio::test]
    async fn test_c0_config_all_timeouts() {
        let config = parse_c0_config("ct=1000 rt=2000 ot=3000 oat=1500")
            .await
            .unwrap();
        assert_eq!(config.connect_timeout, Some(Duration::from_millis(1000)));
        assert_eq!(config.read_timeout, Some(Duration::from_millis(2000)));
        assert_eq!(config.operation_timeout, Some(Duration::from_millis(3000)));
        assert_eq!(
            config.operation_attempt_timeout,
            Some(Duration::from_millis(1500))
        );
    }

    #[tokio::test]
    async fn test_c0_config_backoff_settings() {
        let config = parse_c0_config("ib=100 mb=5000 ma=3").await.unwrap();
        assert_eq!(config.initial_backoff, Some(Duration::from_millis(100)));
        assert_eq!(config.max_backoff, Some(Duration::from_millis(5000)));
        assert_eq!(config.max_attempts, Some(3));
    }

    #[tokio::test]
    async fn test_c0_config_mixed_settings() {
        let config = parse_c0_config("ct=1000 ma=5 ib=10 oat=1500")
            .await
            .unwrap();
        assert_eq!(config.connect_timeout, Some(Duration::from_millis(1000)));
        assert_eq!(config.max_attempts, Some(5));
        assert_eq!(config.initial_backoff, Some(Duration::from_millis(10)));
        assert_eq!(
            config.operation_attempt_timeout,
            Some(Duration::from_millis(1500))
        );
    }

    #[tokio::test]
    async fn test_c0_config_ignores_unknown_keys() {
        let config = parse_c0_config("ct=1000 unknown=123 rt=2000")
            .await
            .unwrap();
        assert_eq!(config.connect_timeout, Some(Duration::from_millis(1000)));
        assert_eq!(config.read_timeout, Some(Duration::from_millis(2000)));
    }

    #[tokio::test]
    async fn test_c0_config_missing_equals() {
        let result = parse_c0_config("ct1000").await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().1,
            "Malformed C0-Config header: missing '=' in key-value pair"
        );
    }

    #[tokio::test]
    async fn test_c0_config_invalid_duration() {
        let result = parse_c0_config("ct=invalid").await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().1,
            "Invalid duration value in C0-Config header"
        );
    }

    #[tokio::test]
    async fn test_c0_config_invalid_max_attempts() {
        let result = parse_c0_config("ma=invalid").await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().1,
            "Invalid max_attempts value in C0-Config hheader"
        );
    }

    #[tokio::test]
    async fn test_c0_config_invalid_header_encoding() {
        let mut req = Request::builder()
            .method(Method::GET)
            .uri("/")
            .body(())
            .unwrap();

        req.headers_mut().insert(
            &C0_CONFIG_HEADER,
            HeaderValue::from_bytes(&[0xFF, 0xFE]).unwrap(),
        );

        let (mut parts, _) = req.into_parts();
        let result = RequestConfig::from_request_parts(&mut parts, &()).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().1, "Invalid C0-Config header encoding");
    }
}
