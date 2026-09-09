use std::{
    net::SocketAddr,
    num::NonZeroU32,
    ops::{Range, RangeInclusive},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use eyre::Result;
use foyer::Source;
use futures::{Stream, StreamExt};
use parking_lot::Mutex;

mod metrics;
mod throughput;
pub use metrics::PageRequestType;
pub use throughput::SlidingThroughput;
mod routes;

use crate::{
    cache::{CacheConfig, CacheKey, CacheValue, build_cache},
    object_store::{DownloadError, DownloadLimits, Downloader, RequestConfig},
    types::{BucketName, BucketNameSet, ObjectKey, ObjectKind, PageId},
};

pub const PAGE_SIZE: u64 = 16 * 1024 * 1024;

pub const MAX_RANGE_END: u64 = PAGE_SIZE * PageId::MAX as u64;

fn page_id_for_byte_offset(byte_offset: u64) -> PageId {
    (byte_offset / PAGE_SIZE) as PageId
}

fn pagerange(byterange: &Range<u64>) -> RangeInclusive<PageId> {
    let first_page = page_id_for_byte_offset(byterange.start);
    let last_page = page_id_for_byte_offset(byterange.end - 1);
    first_page..=last_page
}

fn slice_page_data(
    page_id: PageId,
    byterange: &Range<u64>,
    value: &CacheValue,
) -> Result<(Bytes, Range<u64>), ServiceError> {
    let page_start = u64::from(page_id) * PAGE_SIZE;
    let range =
        byterange.start.max(page_start)..byterange.end.min(page_start + value.data.len() as u64);
    if range.is_empty() {
        return Err(ServiceError::Download(DownloadError::RangeNotSatisfied {
            requested: byterange.clone(),
            object_size: Some(value.object_size),
        }));
    }
    let start_offset = (range.start - page_start) as usize;
    let end_offset = (range.end - page_start) as usize;
    let data = value.data.slice(start_offset..end_offset);
    Ok((data, range))
}

#[derive(Debug)]
pub struct ServiceConfig {
    pub cache: CacheConfig,
    pub download_limits: DownloadLimits,
}

#[derive(Debug, Clone)]
pub struct Chunk {
    pub bucket: BucketName,
    pub mtime: u32,
    pub data: Bytes,
    pub range: Range<u64>,
    pub object_size: u64,
    pub cached_at: Option<NonZeroU32>,
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("Cache error: {0}")]
    Cache(#[from] foyer::Error),
    #[error("Object store: {0}")]
    Download(#[from] DownloadError),
    #[error("Object size was inconsistent across downloads: {new} != {prev}")]
    ObjectSizeInconsistency { prev: u64, new: u64 },
}

#[derive(Clone)]
pub struct CacheyService {
    cache: foyer::HybridCache<CacheKey, CacheValue>,
    downloader: Downloader,
    ingress_throughput: Arc<Mutex<SlidingThroughput>>,
    egress_throughput: Arc<Mutex<SlidingThroughput>>,
    server_handle: axum_server::Handle<SocketAddr>,
}

impl CacheyService {
    pub async fn new(
        config: ServiceConfig,
        s3: aws_sdk_s3::Client,
        server_handle: axum_server::Handle<SocketAddr>,
    ) -> Result<Self> {
        eyre::ensure!(
            config.download_limits.max_inflight_bytes >= PAGE_SIZE,
            "download memory must hold at least one cache page ({PAGE_SIZE} bytes)"
        );
        let ingress_throughput = Arc::new(Mutex::new(SlidingThroughput::default()));
        let egress_throughput = Arc::new(Mutex::new(SlidingThroughput::default()));
        let downloader = Downloader::new(s3, config.download_limits, ingress_throughput.clone())?;
        let cache = build_cache(config.cache).await?;
        Ok(Self {
            cache,
            downloader,
            ingress_throughput,
            egress_throughput,
            server_handle,
        })
    }

    pub fn observe_metrics(&self) {
        self.downloader
            .observe_bucket_metrics(metrics::set_bucket_stats);

        metrics::observe_jemalloc_metrics();

        let mut egress_throughput = self.egress_throughput.lock();
        metrics::observe_throughput(
            "egress",
            &[
                ("10s", egress_throughput.bps(Duration::from_secs(10))),
                ("30s", egress_throughput.bps(Duration::from_secs(30))),
                ("1m", egress_throughput.bps(Duration::from_mins(1))),
            ],
        );

        let mut ingress_throughput = self.ingress_throughput.lock();
        metrics::observe_throughput(
            "ingress",
            &[
                ("10s", ingress_throughput.bps(Duration::from_secs(10))),
                ("30s", ingress_throughput.bps(Duration::from_secs(30))),
                ("1m", ingress_throughput.bps(Duration::from_mins(1))),
            ],
        );

        metrics::set_connection_count(self.server_handle.connection_count());
    }

    #[must_use]
    pub fn ingress_throughput_bps(&self, lookback: Duration) -> f64 {
        self.ingress_throughput.lock().bps(lookback)
    }

    #[must_use]
    pub fn egress_throughput_bps(&self, lookback: Duration) -> f64 {
        self.egress_throughput.lock().bps(lookback)
    }

    /// # Panics
    ///
    /// if `byterange.start > byterange.end` or `byterange.end >= MAX_RANGE_END`.
    pub fn get(
        self,
        kind: ObjectKind,
        object: ObjectKey,
        buckets: BucketNameSet,
        byterange: Range<u64>,
        concurrency: usize,
        req_config: RequestConfig,
    ) -> impl Stream<Item = Result<Chunk, ServiceError>> {
        assert!(byterange.start < byterange.end);
        assert!(byterange.end <= MAX_RANGE_END);

        let pagerange = pagerange(&byterange);

        metrics::fetch_request_bytes(&kind, byterange.end - byterange.start);
        metrics::fetch_request_pages(&kind, pagerange.end() - pagerange.start() + 1);

        let executor = Arc::new(PageGetExecutor {
            downloader: self.downloader,
            kind,
            object,
            buckets,
            req_config,
        });
        let mut object_size = None;

        futures::stream::iter(pagerange)
            .map(move |page_id| executor.clone().execute(page_id, self.cache.clone()))
            .buffered(concurrency)
            .map(move |result| {
                let (page_id, value) = result?;
                let expected_size = *object_size.get_or_insert(value.object_size);
                if value.object_size != expected_size {
                    return Err(ServiceError::ObjectSizeInconsistency {
                        new: value.object_size,
                        prev: expected_size,
                    });
                }
                let (data, range) = slice_page_data(page_id, &byterange, &value)?;
                self.egress_throughput.lock().record(data.len());
                Ok(Chunk {
                    bucket: value.bucket,
                    mtime: value.mtime,
                    data,
                    range,
                    object_size: value.object_size,
                    cached_at: NonZeroU32::new(value.cached_at),
                })
            })
    }

    pub fn into_router(self) -> axum::Router {
        axum::Router::new()
            .route("/metrics", axum::routing::get(routes::metrics))
            .route("/stats", axum::routing::get(routes::stats))
            .route(
                "/debug/pprof/allocs",
                axum::routing::get(routes::heap_profile),
            )
            .route(
                "/debug/pprof/allocs/flamegraph",
                axum::routing::get(routes::heap_flamegraph),
            )
            .route(
                "/fetch/{kind}/{*object}",
                axum::routing::get(routes::fetch).head(routes::fetch),
            )
            .with_state(self)
    }
}

fn now() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32
}

#[derive(Debug)]
struct PageGetExecutor {
    downloader: Downloader,
    kind: ObjectKind,
    object: ObjectKey,
    buckets: BucketNameSet,
    req_config: RequestConfig,
}

impl PageGetExecutor {
    async fn execute(
        self: Arc<Self>,
        page_id: PageId,
        cache: foyer::HybridCache<CacheKey, CacheValue>,
    ) -> Result<(PageId, CacheValue), ServiceError> {
        metrics::page_request_count(&self.kind, metrics::PageRequestType::Access);

        let cache_key = CacheKey {
            kind: self.kind.clone(),
            object: self.object.clone(),
            page_id,
        };
        let fetched_by_current_request = Arc::new(AtomicBool::new(false));
        let entry = cache
            .get_or_fetch(&cache_key, {
                let fetched_by_current_request = Arc::clone(&fetched_by_current_request);
                move || async move {
                    fetched_by_current_request.store(true, Ordering::Relaxed);
                    metrics::page_request_count(&self.kind, metrics::PageRequestType::Download);

                    let start = u64::from(page_id) * PAGE_SIZE;
                    let end = start + PAGE_SIZE;
                    let out = self
                        .downloader
                        .download(
                            &self.buckets,
                            self.object.clone(),
                            &(start..end),
                            &self.req_config,
                        )
                        .await?;
                    metrics::page_download_latency(&self.kind, out.latency);
                    if out.hedged {
                        metrics::page_request_count(&self.kind, metrics::PageRequestType::Hedged);
                    }
                    if out.primary_bucket_idx == 0 {
                        metrics::page_request_count(
                            &self.kind,
                            metrics::PageRequestType::ClientPref,
                        );
                    }
                    if out.used_bucket_idx != out.primary_bucket_idx {
                        metrics::page_request_count(&self.kind, metrics::PageRequestType::Fallback);
                    }
                    Ok::<_, DownloadError>(CacheValue {
                        bucket: self.buckets[out.used_bucket_idx].clone(),
                        mtime: out.piece.mtime,
                        data: out.piece.data,
                        object_size: out.piece.object_size,
                        cached_at: now(),
                    })
                }
            })
            .await
            .map_err(|err| match err.downcast_ref::<DownloadError>() {
                Some(download_err) => ServiceError::Download(download_err.clone()),
                None => ServiceError::Cache(err),
            })?;
        let key = entry.key();
        metrics::page_request_count(&key.kind, metrics::PageRequestType::Success);

        let mut value = entry.value().clone();
        match entry.source() {
            Source::Memory => {
                metrics::page_request_count(&key.kind, metrics::PageRequestType::CacheHit);
                metrics::page_request_count(&key.kind, metrics::PageRequestType::CacheHitMemory);
            }
            Source::Disk => {
                metrics::page_request_count(&key.kind, metrics::PageRequestType::CacheHit);
                metrics::page_request_count(&key.kind, metrics::PageRequestType::CacheHitDisk);
            }
            Source::Outer => {
                value.cached_at = 0;
                if !fetched_by_current_request.load(Ordering::Relaxed) {
                    metrics::page_request_count(&key.kind, metrics::PageRequestType::Coalesced);
                }
            }
        }
        Ok((page_id, value))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering},
        },
        time::Duration,
    };

    use aws_config::BehaviorVersion;
    use aws_sdk_s3::config::{Credentials, Region};
    use axum::{
        Router,
        extract::{Path, State},
        http::{HeaderMap, StatusCode},
        routing::get,
    };
    use bytes::Bytes;
    use bytesize::ByteSize;
    use futures::TryStreamExt;
    use parking_lot::Mutex;

    use super::{
        CacheyService, PAGE_SIZE, PageGetExecutor, ServiceConfig, ServiceError, SlidingThroughput,
        page_id_for_byte_offset, pagerange, slice_page_data,
    };
    use crate::{
        cache::{CacheConfig, CacheKey, CacheValue, build_cache},
        object_store::{DownloadError, DownloadLimits, Downloader, RequestConfig},
        types::{BucketName, BucketNameSet, ObjectKey, ObjectKind},
    };

    #[derive(Debug, Clone)]
    struct MockS3State {
        expected_bucket: String,
        expected_key: String,
        object: Bytes,
        request_count: Arc<AtomicUsize>,
        response_delay: Duration,
    }

    fn parse_range_header(range_header: &str) -> Option<(u64, u64)> {
        let range = range_header.strip_prefix("bytes=")?;
        let (start, end) = range.split_once('-')?;
        Some((start.parse().ok()?, end.parse().ok()?))
    }

    async fn mock_get_object(
        State(state): State<Arc<MockS3State>>,
        Path((bucket, key)): Path<(String, String)>,
        headers: HeaderMap,
    ) -> (StatusCode, HeaderMap, Bytes) {
        if bucket != state.expected_bucket || key != state.expected_key {
            return (StatusCode::NOT_FOUND, HeaderMap::new(), Bytes::new());
        }

        let Some(range_header) = headers
            .get(http::header::RANGE)
            .and_then(|v| v.to_str().ok())
        else {
            return (StatusCode::BAD_REQUEST, HeaderMap::new(), Bytes::new());
        };
        let Some((requested_start, requested_end)) = parse_range_header(range_header) else {
            return (StatusCode::BAD_REQUEST, HeaderMap::new(), Bytes::new());
        };

        state.request_count.fetch_add(1, AtomicOrdering::Relaxed);
        tokio::time::sleep(state.response_delay).await;

        let object_size = state.object.len() as u64;
        if requested_start >= object_size {
            let mut headers = HeaderMap::new();
            headers.insert(
                http::header::CONTENT_RANGE,
                format!("bytes */{object_size}")
                    .parse()
                    .expect("content-range"),
            );
            return (StatusCode::RANGE_NOT_SATISFIABLE, headers, Bytes::new());
        }

        let response_end = requested_end.min(object_size.saturating_sub(1));
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_RANGE,
            format!("bytes {requested_start}-{response_end}/{object_size}")
                .parse()
                .expect("content-range"),
        );
        headers.insert(
            http::header::LAST_MODIFIED,
            "Tue, 15 Nov 1994 08:12:31 GMT"
                .parse()
                .expect("last-modified"),
        );
        let data = state
            .object
            .slice((requested_start as usize)..=(response_end as usize));
        (StatusCode::PARTIAL_CONTENT, headers, data)
    }

    async fn spawn_mock_s3_server(
        bucket: &BucketName,
        key: &ObjectKey,
        object: Bytes,
        response_delay: Duration,
    ) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        let request_count = Arc::new(AtomicUsize::new(0));
        let state = Arc::new(MockS3State {
            expected_bucket: bucket.to_string(),
            expected_key: key.to_string(),
            object,
            request_count: Arc::clone(&request_count),
            response_delay,
        });
        let app = Router::new()
            .route("/{bucket}/{*key}", get(mock_get_object))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock server");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve mock s3");
        });
        (endpoint, request_count, handle)
    }

    fn mock_s3_client(endpoint: &str) -> aws_sdk_s3::Client {
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(Credentials::new("test", "test", None, None, "test"))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .region(Region::new("us-east-1"))
            .build();
        aws_sdk_s3::Client::from_conf(config)
    }

    #[tokio::test]
    async fn service_requires_download_memory_for_a_full_page() {
        for max_inflight_bytes in [PAGE_SIZE - 1, PAGE_SIZE] {
            let result = CacheyService::new(
                ServiceConfig {
                    cache: CacheConfig {
                        memory_size: ByteSize::mib(16),
                        disk_cache: None,
                        metrics_registry: None,
                    },
                    download_limits: DownloadLimits {
                        max_inflight_bytes,
                        ..DownloadLimits::default()
                    },
                },
                mock_s3_client("http://unused.invalid"),
                axum_server::Handle::new(),
            )
            .await;
            if max_inflight_bytes < PAGE_SIZE {
                assert!(
                    result
                        .is_err_and(|error| error.to_string().contains("at least one cache page"))
                );
            } else {
                assert!(result.is_ok());
            }
        }
    }

    fn metric_page_request_total(kind: &ObjectKind, typ: &str) -> u64 {
        prometheus::gather()
            .into_iter()
            .find(|family| family.name() == "cachey_page_request_total")
            .and_then(|family| {
                family.metric.iter().find_map(|metric| {
                    let mut metric_kind = None;
                    let mut metric_type = None;
                    for label in &metric.label {
                        match label.name() {
                            "kind" => metric_kind = Some(label.value()),
                            "type" => metric_type = Some(label.value()),
                            _ => {}
                        }
                    }
                    if metric_kind == Some(&**kind) && metric_type == Some(typ) {
                        Some(metric.counter.value().round() as u64)
                    } else {
                        None
                    }
                })
            })
            .unwrap_or(0)
    }

    fn unique_name(prefix: &str) -> String {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        format!("{prefix}-{}", NEXT_ID.fetch_add(1, AtomicOrdering::Relaxed))
    }

    #[test]
    fn page_id_for_byte_offset_matches_page_boundaries() {
        assert_eq!(page_id_for_byte_offset(0), 0);
        assert_eq!(page_id_for_byte_offset(PAGE_SIZE - 1), 0);
        assert_eq!(page_id_for_byte_offset(PAGE_SIZE), 1);
        assert_eq!(page_id_for_byte_offset(PAGE_SIZE + 123), 1);
        assert_eq!(page_id_for_byte_offset(2 * PAGE_SIZE), 2);
    }

    #[test]
    fn page_bounds_for_range_end_on_page_boundary_does_not_advance_last_page() {
        let byterange = 0..PAGE_SIZE;
        assert_eq!(pagerange(&byterange), 0..=0);

        let byterange = 0..(2 * PAGE_SIZE);
        assert_eq!(pagerange(&byterange), 0..=1);
    }

    #[test]
    fn page_bounds_for_range_crosses_page_boundary() {
        let byterange = (PAGE_SIZE - 1)..(PAGE_SIZE + 1);
        assert_eq!(pagerange(&byterange), 0..=1);

        let byterange = PAGE_SIZE..(2 * PAGE_SIZE);
        assert_eq!(pagerange(&byterange), 1..=1);
    }

    #[test]
    fn slice_page_data_rejects_range_start_past_object_end() {
        let object_len = 1024 * 1024;
        let byterange = (2 * 1024 * 1024)..(3 * 1024 * 1024);
        let data = Bytes::from(vec![0_u8; object_len]);
        let value = CacheValue {
            bucket: BucketName::new("test-bucket").expect("bucket name"),
            mtime: 0,
            data: data.clone(),
            object_size: object_len as u64,
            cached_at: 0,
        };
        let err = slice_page_data(page_id_for_byte_offset(byterange.start), &byterange, &value)
            .expect_err("expected range error");
        match err {
            ServiceError::Download(DownloadError::RangeNotSatisfied {
                requested,
                object_size,
            }) => {
                assert_eq!(requested, byterange);
                assert_eq!(object_size, Some(object_len as u64));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn pages_must_agree_with_the_first_delivered_object_size() {
        let kind = ObjectKind::new("size-consistency").unwrap();
        let object = ObjectKey::new("object").unwrap();
        let bucket = BucketName::new("bucket").unwrap();
        let buckets = BucketNameSet::new(std::iter::once(bucket.clone())).unwrap();
        let data = Bytes::from(vec![7; 2 * PAGE_SIZE as usize]);
        let (endpoint, _, server_handle) =
            spawn_mock_s3_server(&bucket, &object, data.clone(), Duration::ZERO).await;
        let service = CacheyService::new(
            ServiceConfig {
                cache: CacheConfig {
                    memory_size: ByteSize::mib(64),
                    disk_cache: None,
                    metrics_registry: None,
                },
                download_limits: DownloadLimits::default(),
            },
            mock_s3_client(&endpoint),
            axum_server::Handle::new(),
        )
        .await
        .expect("service");
        service.cache.insert(
            CacheKey {
                kind: kind.clone(),
                object: object.clone(),
                page_id: 1,
            },
            CacheValue {
                bucket,
                mtime: 0,
                data: data.slice(PAGE_SIZE as usize..),
                object_size: 3 * PAGE_SIZE,
                cached_at: 1,
            },
        );
        let read = |range| {
            service.clone().get(
                kind.clone(),
                object.clone(),
                buckets.clone(),
                range,
                2,
                RequestConfig::default(),
            )
        };
        let mut chunks = std::pin::pin!(read(0..PAGE_SIZE + 1));
        let first = chunks
            .try_next()
            .await
            .expect("first page establishes the object size")
            .expect("first chunk");
        assert_eq!(first.object_size, 2 * PAGE_SIZE);
        let error = chunks
            .try_next()
            .await
            .expect_err("later cache hit disagrees with the first page");
        assert!(matches!(
            error,
            ServiceError::ObjectSizeInconsistency { prev, new }
                if prev == 2 * PAGE_SIZE && new == 3 * PAGE_SIZE
        ));
        let chunks = read(PAGE_SIZE..PAGE_SIZE + 1)
            .try_collect::<Vec<_>>()
            .await
            .expect("independent read of the newer page");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].object_size, 3 * PAGE_SIZE);
        server_handle.abort();
    }

    #[tokio::test]
    async fn page_get_executor_coalesced_miss_is_counted_end_to_end() {
        let kind = ObjectKind::new(unique_name("kind")).expect("kind");
        let object = ObjectKey::new(unique_name("object")).expect("object");
        let bucket = BucketName::new(unique_name("bucket")).expect("bucket");
        let buckets = BucketNameSet::new(std::iter::once(bucket.clone())).expect("buckets");
        let object_data = Bytes::from(vec![7_u8; 4096]);

        let (endpoint, request_count, server_handle) =
            spawn_mock_s3_server(&bucket, &object, object_data, Duration::from_millis(50)).await;
        let s3 = mock_s3_client(&endpoint);
        let downloader = Downloader::new(
            s3,
            DownloadLimits::default(),
            Arc::new(Mutex::new(SlidingThroughput::default())),
        )
        .unwrap();
        let cache = build_cache(CacheConfig {
            memory_size: ByteSize::mib(16),
            disk_cache: None,
            metrics_registry: None,
        })
        .await
        .expect("cache");

        let before_access = metric_page_request_total(&kind, "access");
        let before_success = metric_page_request_total(&kind, "success");
        let before_download = metric_page_request_total(&kind, "download");
        let before_coalesced = metric_page_request_total(&kind, "coalesced");
        let before_cache_hit = metric_page_request_total(&kind, "cache_hit");

        let executor = Arc::new(PageGetExecutor {
            downloader,
            kind: kind.clone(),
            object: object.clone(),
            buckets,
            req_config: RequestConfig::default(),
        });
        let (left, right) = tokio::join!(
            executor.clone().execute(0, cache.clone()),
            executor.execute(0, cache)
        );
        let left_value = left.expect("left request").1;
        let right_value = right.expect("right request").1;

        assert_eq!(request_count.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(left_value.cached_at, 0);
        assert_eq!(right_value.cached_at, 0);
        assert_eq!(
            metric_page_request_total(&kind, "access") - before_access,
            2
        );
        assert_eq!(
            metric_page_request_total(&kind, "success") - before_success,
            2
        );
        assert_eq!(
            metric_page_request_total(&kind, "download") - before_download,
            1
        );
        assert_eq!(
            metric_page_request_total(&kind, "coalesced") - before_coalesced,
            1
        );
        assert_eq!(
            metric_page_request_total(&kind, "cache_hit") - before_cache_hit,
            0
        );

        server_handle.abort();
    }
}
