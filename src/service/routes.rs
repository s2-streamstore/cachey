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
use tracing::{debug, instrument, warn};

use crate::{
    object_store::{DownloadError, S3RequestProfile},
    service::{CacheyService, Chunk, ServiceError, metrics},
    types::{BucketName, BucketNameSet, ObjectKey, ObjectKind},
};

const CONTENT_TYPE: &str = "application/octet-stream";
static C0_BUCKET_HEADER: HeaderName = HeaderName::from_static("c0-bucket");
static C0_UPSTREAM_HEADER: HeaderName = HeaderName::from_static("c0-upstream");

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
                Ok(RangeHeader(first_byte..(last_byte + 1)))
            }
            _ => Err((StatusCode::RANGE_NOT_SATISFIABLE, "Unsupported range")),
        }
    }
}

impl<S> FromRequestParts<S> for S3RequestProfile
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let Some(header_value) = parts.headers.get(&C0_UPSTREAM_HEADER) else {
            return Ok(S3RequestProfile::default());
        };

        let header_str = header_value.to_str().map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                "Invalid C0-Upstream header encoding",
            )
        })?;

        let mut profile = S3RequestProfile::default();

        for pair in header_str.split_whitespace() {
            let Some((key, value)) = pair.split_once('=') else {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "Malformed C0-Upstream header: missing '=' in key-value pair",
                ));
            };

            let parse_duration = |v: &str| -> Result<Duration, Self::Rejection> {
                v.parse::<u64>().map(Duration::from_millis).map_err(|_| {
                    (
                        StatusCode::BAD_REQUEST,
                        "Invalid duration value in C0-Upstream header",
                    )
                })
            };

            match key {
                "ot" => profile.operation_timeout = Some(parse_duration(value)?),
                "oat" => profile.operation_attempt_timeout = Some(parse_duration(value)?),
                "ma" => {
                    profile.max_attempts = Some(value.parse().map_err(|_| {
                        (
                            StatusCode::BAD_REQUEST,
                            "Invalid max_attempts value in C0-Upstream header",
                        )
                    })?)
                }
                "ib" => profile.initial_backoff = Some(parse_duration(value)?),
                "mb" => profile.max_backoff = Some(parse_duration(value)?),
                _ => return Err((StatusCode::BAD_REQUEST, "Unknown key in C0-Upstream header")),
            }
        }

        Ok(profile)
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
        Ok(BucketHeaders(names))
    }
}

#[instrument(skip(service))]
pub async fn fetch(
    State(service): State<CacheyService>,
    Path((kind, object)): Path<(ObjectKind, ObjectKey)>,
    method: axum::http::Method,
    RangeHeader(byterange): RangeHeader,
    BucketHeaders(buckets): BucketHeaders,
    s3_profile: S3RequestProfile,
) -> Response {
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
                s3_profile,
            )
            .await
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
                        trailers.insert("c0-status", c0_status(&chunk));
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
    use axum::http::Request;

    use super::*;

    fn create_parts_with_headers(headers: Vec<(&HeaderName, &str)>) -> Parts {
        let mut req = Request::builder();
        for (name, value) in headers {
            req = req.header(name, value);
        }
        let (parts, _) = req.body(()).unwrap().into_parts();
        parts
    }

    #[tokio::test]
    async fn test_s3_request_profile_default() {
        let mut parts = create_parts_with_headers(vec![]);
        let profile = S3RequestProfile::from_request_parts(&mut parts, &())
            .await
            .unwrap();

        assert!(profile.operation_timeout.is_none());
        assert!(profile.operation_attempt_timeout.is_none());
        assert!(profile.max_attempts.is_none());
        assert!(profile.initial_backoff.is_none());
        assert!(profile.max_backoff.is_none());
    }

    #[tokio::test]
    async fn test_s3_request_profile_parse() {
        let mut parts = create_parts_with_headers(vec![(
            &C0_PROFILE_HEADER,
            "ot=5000 oat=1000 ma=3 ib=100 mb=5000",
        )]);

        let profile = S3RequestProfile::from_request_parts(&mut parts, &())
            .await
            .unwrap();

        assert_eq!(profile.operation_timeout, Some(Duration::from_millis(5000)));
        assert_eq!(
            profile.operation_attempt_timeout,
            Some(Duration::from_millis(1000))
        );
        assert_eq!(profile.max_attempts, Some(3));
        assert_eq!(profile.initial_backoff, Some(Duration::from_millis(100)));
        assert_eq!(profile.max_backoff, Some(Duration::from_millis(5000)));
    }

    #[tokio::test]
    async fn test_s3_request_profile_partial() {
        let mut parts = create_parts_with_headers(vec![(&C0_PROFILE_HEADER, "ot=2000 ma=5")]);

        let profile = S3RequestProfile::from_request_parts(&mut parts, &())
            .await
            .unwrap();

        assert_eq!(profile.operation_timeout, Some(Duration::from_millis(2000)));
        assert_eq!(profile.operation_attempt_timeout, None);
        assert_eq!(profile.max_attempts, Some(5));
        assert_eq!(profile.initial_backoff, None);
        assert_eq!(profile.max_backoff, None);
    }

    #[tokio::test]
    async fn test_s3_request_profile_malformed_missing_equals() {
        let mut parts = create_parts_with_headers(vec![(&C0_PROFILE_HEADER, "ot5000 ma=3")]);

        let result = S3RequestProfile::from_request_parts(&mut parts, &()).await;

        assert!(result.is_err());
        let (status, msg) = result.unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(msg.contains("missing '='"));
    }

    #[tokio::test]
    async fn test_s3_request_profile_invalid_duration() {
        let mut parts = create_parts_with_headers(vec![(&C0_PROFILE_HEADER, "ot=abc")]);

        let result = S3RequestProfile::from_request_parts(&mut parts, &()).await;

        assert!(result.is_err());
        let (status, msg) = result.unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(msg.contains("Invalid duration"));
    }

    #[tokio::test]
    async fn test_s3_request_profile_unknown_key() {
        let mut parts = create_parts_with_headers(vec![(&C0_PROFILE_HEADER, "unknown=100")]);

        let result = S3RequestProfile::from_request_parts(&mut parts, &()).await;

        assert!(result.is_err());
        let (status, msg) = result.unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(msg.contains("Unknown key"));
    }
}
