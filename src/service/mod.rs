use std::{
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
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

pub const MAX_RANGE_END: u64 = PAGE_SIZE * (PageId::MAX as u64 + 1);

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

#[derive(Debug, Clone, thiserror::Error)]
pub enum ServiceError {
    #[error("Cache error: {0}")]
    Cache(#[from] foyer::Error),
    #[error("Object store: {0}")]
    Download(#[from] DownloadError),
    #[error("Object size {new} is inconsistent with {prev}")]
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

        for (direction, throughput) in [
            ("egress", &self.egress_throughput),
            ("ingress", &self.ingress_throughput),
        ] {
            let windowed_bps = {
                let mut throughput = throughput.lock();
                [
                    ("10s", throughput.bps(Duration::from_secs(10))),
                    ("30s", throughput.bps(Duration::from_secs(30))),
                    ("1m", throughput.bps(Duration::from_mins(1))),
                ]
            };
            metrics::observe_throughput(direction, &windowed_bps);
        }

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

    /// A known `object_size` keeps downloads within the object, so its pages are read
    /// exactly, and every page must report that size. A range starting at or past it fails
    /// without a download.
    ///
    /// # Panics
    ///
    /// If `byterange.start >= byterange.end` or `byterange.end > MAX_RANGE_END`.
    #[allow(clippy::too_many_arguments)]
    pub fn get(
        self,
        kind: ObjectKind,
        object: ObjectKey,
        buckets: BucketNameSet,
        byterange: Range<u64>,
        object_size: Option<u64>,
        concurrency: NonZeroUsize,
        req_config: RequestConfig,
    ) -> impl Stream<Item = Result<Chunk, ServiceError>> {
        assert!(byterange.start < byterange.end);
        assert!(byterange.end <= MAX_RANGE_END);

        let end = byterange.end.min(object_size.unwrap_or(u64::MAX));
        let pages = (byterange.start < end).then(|| pagerange(&(byterange.start..end)));
        let past_end = pages.is_none().then(|| {
            Err(ServiceError::Download(DownloadError::RangeNotSatisfied {
                requested: byterange.clone(),
                object_size,
            }))
        });

        metrics::fetch_request_bytes(&kind, byterange.end - byterange.start);
        metrics::fetch_request_pages(&kind, pages.as_ref().map_or(0, ExactSizeIterator::len));

        let executor = Arc::new(PageGetExecutor {
            downloader: self.downloader,
            kind,
            object,
            buckets,
            object_size,
            req_config,
        });
        let mut expected_size = object_size;

        futures::stream::iter(past_end).chain(
            futures::stream::iter(pages.into_iter().flatten())
                .map(move |page_id| executor.clone().execute(page_id, self.cache.clone()))
                .buffered(concurrency.get())
                .map(move |result| {
                    let (page_id, value) = result?;
                    let expected_size = *expected_size.get_or_insert(value.object_size);
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
                }),
        )
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
    object_size: Option<u64>,
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
                    let end = (start + PAGE_SIZE).min(self.object_size.unwrap_or(u64::MAX));
                    let out = self
                        .downloader
                        .download(
                            &self.buckets,
                            self.object.clone(),
                            &(start..end),
                            &self.req_config,
                        )
                        .await?;
                    // A page cut short by a wrong size must not be cached.
                    if let Some(size) = self.object_size
                        && out.piece.object_size != size
                    {
                        return Err(ServiceError::ObjectSizeInconsistency {
                            prev: size,
                            new: out.piece.object_size,
                        });
                    }
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
                    Ok::<_, ServiceError>(CacheValue {
                        bucket: self.buckets[out.used_bucket_idx].clone(),
                        mtime: out.piece.mtime,
                        data: out.piece.data,
                        object_size: out.piece.object_size,
                        cached_at: now(),
                    })
                }
            })
            .await
            .map_err(|err| {
                err.downcast_ref::<ServiceError>()
                    .cloned()
                    .unwrap_or(ServiceError::Cache(err))
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
        num::NonZeroUsize,
        sync::{
            Arc,
            atomic::{AtomicU64, AtomicUsize, Ordering},
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

    use super::{CacheyService, PAGE_SIZE, ServiceConfig, ServiceError, metrics, pagerange};
    use crate::{
        cache::{CacheConfig, CacheKey, CacheValue},
        object_store::{DownloadError, DownloadLimits, RequestConfig},
        types::{BucketName, BucketNameSet, ObjectKey, ObjectKind},
    };

    #[derive(Debug)]
    struct MockS3State {
        expected_bucket: String,
        expected_key: String,
        object: Bytes,
        request_count: AtomicUsize,
        requested_bytes: AtomicU64,
        response_delay: Duration,
    }

    async fn mock_get_object(
        State(state): State<Arc<MockS3State>>,
        Path((bucket, key)): Path<(String, String)>,
        headers: HeaderMap,
    ) -> impl axum::response::IntoResponse {
        assert_eq!(bucket, state.expected_bucket);
        assert_eq!(key, state.expected_key);
        let (start, end) = headers[http::header::RANGE]
            .to_str()
            .unwrap()
            .strip_prefix("bytes=")
            .unwrap()
            .split_once('-')
            .unwrap();
        let start = start.parse::<usize>().unwrap();
        let requested_end = end.parse::<usize>().unwrap() + 1;
        let end = requested_end.min(state.object.len());
        assert!(start < end);

        state.request_count.fetch_add(1, Ordering::Relaxed);
        state
            .requested_bytes
            .fetch_add((requested_end - start) as u64, Ordering::Relaxed);
        tokio::time::sleep(state.response_delay).await;
        (
            StatusCode::PARTIAL_CONTENT,
            [(
                "content-range",
                format!("bytes {start}-{}/{}", end - 1, state.object.len()),
            )],
            state.object.slice(start..end),
        )
    }

    async fn spawn_mock_s3_server(
        bucket: &BucketName,
        key: &ObjectKey,
        object: Bytes,
        response_delay: Duration,
    ) -> (String, Arc<MockS3State>, tokio::task::JoinHandle<()>) {
        let state = Arc::new(MockS3State {
            expected_bucket: bucket.to_string(),
            expected_key: key.to_string(),
            object,
            request_count: AtomicUsize::new(0),
            requested_bytes: AtomicU64::new(0),
            response_delay,
        });
        let app = Router::new()
            .route("/{bucket}/{*key}", get(mock_get_object))
            .with_state(Arc::clone(&state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock server");
        let endpoint = format!("http://{}", listener.local_addr().expect("local addr"));
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve mock s3");
        });
        (endpoint, state, handle)
    }

    async fn service(endpoint: &str, memory_size: ByteSize) -> CacheyService {
        CacheyService::new(
            ServiceConfig {
                cache: CacheConfig {
                    memory_size,
                    disk_cache: None,
                    metrics_registry: None,
                },
                download_limits: DownloadLimits::default(),
            },
            mock_s3_client(endpoint),
            axum_server::Handle::new(),
        )
        .await
        .expect("service")
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

    #[test]
    fn page_bounds_respect_exclusive_range_ends() {
        for (bytes, pages) in [
            (0..PAGE_SIZE, 0..=0),
            (0..2 * PAGE_SIZE, 0..=1),
            (PAGE_SIZE - 1..PAGE_SIZE + 1, 0..=1),
            (PAGE_SIZE..2 * PAGE_SIZE, 1..=1),
            ((1 << 40) - 1..1 << 40, 65535..=65535),
            (0..1 << 40, 0..=65535),
        ] {
            assert_eq!(pagerange(&bytes), pages, "{bytes:?}");
        }
    }

    #[tokio::test]
    async fn full_address_space_records_all_pages() {
        let service = service("http://unused.invalid", ByteSize::mib(16)).await;

        drop(service.get(
            ObjectKind::new("full-address-space").unwrap(),
            ObjectKey::new("object").unwrap(),
            BucketName::new("bucket").unwrap().into(),
            0..1 << 40,
            None,
            NonZeroUsize::MIN,
            RequestConfig::default(),
        ));
        let snapshot = metrics::gather();
        assert!(
            std::str::from_utf8(&snapshot)
                .unwrap()
                .contains("cachey_fetch_request_pages_sum{kind=\"full-address-space\"} 65536\n")
        );
    }

    #[tokio::test]
    async fn pages_must_agree_with_the_first_delivered_object_size() {
        let kind = ObjectKind::new("size-consistency").unwrap();
        let object = ObjectKey::new("object").unwrap();
        let bucket = BucketName::new("bucket").unwrap();
        let buckets = BucketNameSet::from(bucket.clone());
        let data = Bytes::from(vec![7; 2 * PAGE_SIZE as usize]);
        let (endpoint, _, server_handle) =
            spawn_mock_s3_server(&bucket, &object, data.clone(), Duration::ZERO).await;
        let service = service(&endpoint, ByteSize::mib(64)).await;
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
                None,
                const { NonZeroUsize::new(2).unwrap() },
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

    /// A known object size keeps page downloads within the object, and a size its bucket
    /// disagrees with fails the read without caching what was downloaded.
    #[tokio::test]
    async fn object_size_bounds_downloads_and_must_match_the_bucket() {
        let kind = ObjectKind::new("object-size").unwrap();
        let object = ObjectKey::new("object").unwrap();
        let bucket = BucketName::new("bucket").unwrap();
        let data = Bytes::from(vec![7; 4096]);
        let (endpoint, mock, server_handle) =
            spawn_mock_s3_server(&bucket, &object, data.clone(), Duration::ZERO).await;
        let service = service(&endpoint, ByteSize::mib(16)).await;
        let read = |range, object_size| {
            service
                .clone()
                .get(
                    kind.clone(),
                    object.clone(),
                    bucket.clone().into(),
                    range,
                    Some(object_size),
                    NonZeroUsize::MIN,
                    RequestConfig::default(),
                )
                .try_collect::<Vec<_>>()
        };
        let downloads = || {
            (
                mock.request_count.load(Ordering::Relaxed),
                mock.requested_bytes.load(Ordering::Relaxed),
            )
        };

        assert!(matches!(
            read(100..200, 1024).await,
            Err(ServiceError::ObjectSizeInconsistency {
                prev: 1024,
                new: 4096
            })
        ));
        assert_eq!(downloads(), (1, 1024));
        let chunks = read(100..200, 4096).await.unwrap();
        assert_eq!(chunks[0].data, data.slice(100..200));
        assert_eq!(downloads(), (2, 1024 + 4096));
        assert!(matches!(
            read(100..200, 8192).await,
            Err(ServiceError::ObjectSizeInconsistency {
                prev: 8192,
                new: 4096
            })
        ));
        assert!(matches!(
            read(4096..4196, 4096).await,
            Err(ServiceError::Download(DownloadError::RangeNotSatisfied {
                object_size: Some(4096),
                ..
            }))
        ));
        assert_eq!(downloads(), (2, 1024 + 4096));
        server_handle.abort();
    }

    #[tokio::test]
    async fn concurrent_service_reads_share_one_download() {
        let kind = ObjectKind::new("coalesced-reads").unwrap();
        let object = ObjectKey::new("object").unwrap();
        let bucket = BucketName::new("bucket").unwrap();
        let buckets = BucketNameSet::from(bucket.clone());
        let object_data = Bytes::from(
            (0..4096)
                .map(|index| (index % 251) as u8)
                .collect::<Vec<_>>(),
        );

        let (endpoint, mock, server_handle) = spawn_mock_s3_server(
            &bucket,
            &object,
            object_data.clone(),
            Duration::from_millis(50),
        )
        .await;
        let service = service(&endpoint, ByteSize::mib(16)).await;

        let read = |range| {
            service
                .clone()
                .get(
                    kind.clone(),
                    object.clone(),
                    buckets.clone(),
                    range,
                    None,
                    NonZeroUsize::MIN,
                    RequestConfig::default(),
                )
                .try_collect::<Vec<_>>()
        };
        let ranges = [10..100, 50..200];
        let (left, right) = tokio::join!(read(ranges[0].clone()), read(ranges[1].clone()));

        assert_eq!(mock.request_count.load(Ordering::Relaxed), 1);
        for (range, chunks) in ranges.into_iter().zip([left, right]) {
            let chunks = chunks.expect("read");
            assert_eq!(chunks.len(), 1);
            assert_eq!(
                chunks[0].data,
                object_data.slice(range.start as usize..range.end as usize)
            );
            assert_eq!(chunks[0].range, range);
            assert_eq!(chunks[0].cached_at, None);
        }
        let snapshot = super::metrics::gather();
        let text = std::str::from_utf8(&snapshot).unwrap();
        for (typ, expected) in [
            ("access", 2),
            ("success", 2),
            ("download", 1),
            ("coalesced", 1),
            ("cache_hit", 0),
        ] {
            let prefix = format!("cachey_page_requests_total{{kind=\"{kind}\",type=\"{typ}\"}} ");
            let value = text
                .lines()
                .find_map(|line| line.strip_prefix(&prefix))
                .unwrap_or("0");
            assert_eq!(value.parse::<u64>().unwrap(), expected, "{typ}");
        }

        server_handle.abort();
    }
}
