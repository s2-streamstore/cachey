use std::{
    net::SocketAddr,
    num::NonZeroU32,
    ops::{Range, RangeInclusive},
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use bytes::Bytes;
use crossbeam::atomic::AtomicCell;
use eyre::Result;
use futures::{Stream, StreamExt};
use parking_lot::Mutex;

mod metrics;
mod throughput;
pub use throughput::SlidingThroughput;
mod routes;

use crate::{
    cache::{CacheConfig, CacheKey, CacheValue, build_cache},
    object_store::{DownloadError, Downloader, RequestConfig},
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
    let page_start = page_id as u64 * PAGE_SIZE;
    let data_len = value.data.len();
    let mut range_start = page_start;
    let mut range_end = page_start + data_len as u64;
    let mut start_offset = 0;
    let mut end_offset = data_len;
    let pagerange = pagerange(byterange);
    if page_id == *pagerange.start() {
        start_offset = (byterange.start - page_start) as usize;
        if start_offset >= data_len {
            return Err(ServiceError::Download(DownloadError::RangeNotSatisfied {
                requested: byterange.clone(),
                object_size: Some(value.object_size),
            }));
        }
        range_start = byterange.start;
    }
    if page_id == *pagerange.end() {
        end_offset = ((byterange.end - page_start) as usize).min(end_offset);
        range_end = page_start + end_offset as u64;
    }
    let data = value.data.slice(start_offset..end_offset);
    Ok((data, range_start..range_end))
}

#[derive(Debug)]
pub struct ServiceConfig {
    pub cache: CacheConfig,
    pub hedge_quantile: f64,
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
    /// 500
    #[error("Cache error: {0}")]
    Cache(#[from] foyer::Error),
    /// NoSuchKey 404; RangeNotSatisfied 416; otherwise 500
    #[error("Object store: {0}")]
    Download(#[from] DownloadError),
    /// 409
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
        let cache = build_cache(config.cache).await?;
        let ingress_throughput = Arc::new(Mutex::new(SlidingThroughput::default()));
        let egress_throughput = Arc::new(Mutex::new(SlidingThroughput::default()));
        let downloader = Downloader::new(s3, config.hedge_quantile, ingress_throughput.clone());
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
                ("1m", egress_throughput.bps(Duration::from_secs(60))),
            ],
        );

        let mut ingress_throughput = self.ingress_throughput.lock();
        metrics::observe_throughput(
            "ingress",
            &[
                ("10s", ingress_throughput.bps(Duration::from_secs(10))),
                ("30s", ingress_throughput.bps(Duration::from_secs(30))),
                ("1m", ingress_throughput.bps(Duration::from_secs(60))),
            ],
        );

        metrics::set_connection_count(self.server_handle.connection_count());
    }

    pub fn ingress_throughput_bps(&self, lookback: Duration) -> f64 {
        self.ingress_throughput.lock().bps(lookback)
    }

    pub fn egress_throughput_bps(&self, lookback: Duration) -> f64 {
        self.egress_throughput.lock().bps(lookback)
    }

    /// Panics if range `start > end` or `end >= max_range_end`.
    pub async fn get(
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

        let executor = PageGetExecutor {
            cache: self.cache,
            downloader: self.downloader,
            kind,
            object,
            buckets,
            object_size: Default::default(),
            req_config,
        };

        futures::stream::iter(pagerange.map(move |page_id| executor.clone().execute(page_id)))
            .buffered(concurrency)
            .map(move |res| {
                res.and_then(|(page_id, value)| {
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

#[derive(Debug, Clone)]
struct PageGetExecutor {
    cache: foyer::HybridCache<CacheKey, CacheValue>,
    downloader: Downloader,
    kind: ObjectKind,
    object: ObjectKey,
    buckets: BucketNameSet,
    object_size: Arc<AtomicCell<Option<u64>>>,
    req_config: RequestConfig,
}

impl PageGetExecutor {
    async fn execute(self, page_id: PageId) -> Result<(PageId, CacheValue), ServiceError> {
        metrics::page_request_count(&self.kind, "access");

        let cache_key = CacheKey {
            kind: self.kind.clone(),
            object: self.object.clone(),
            page_id,
        };
        let hit_state = Arc::new(AtomicBool::new(true));
        match self
            .cache
            .get_or_fetch(&cache_key, {
                let hit_state = hit_state.clone();
                move || async move {
                    hit_state.store(false, std::sync::atomic::Ordering::Relaxed);
                    metrics::page_request_count(&self.kind, "download");

                    let start = page_id as u64 * PAGE_SIZE;
                    let end = start + PAGE_SIZE;
                    let out = self
                        .downloader
                        .download(&self.buckets, self.object, &(start..end), &self.req_config)
                        .await?;
                    metrics::page_download_latency(&self.kind, out.piece.latency);
                    if out.piece.hedged.is_some() {
                        metrics::page_request_count(&self.kind, "hedged");
                    }
                    if self.buckets.first() == Some(&self.buckets[out.primary_bucket_idx]) {
                        metrics::page_request_count(&self.kind, "client_pref");
                    }
                    if out.used_bucket_idx != out.primary_bucket_idx {
                        metrics::page_request_count(&self.kind, "fallback");
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
        {
            Ok(entry) => {
                let key = entry.key();
                metrics::page_request_count(&key.kind, "success");

                let mut value = entry.value().clone();
                match self
                    .object_size
                    .compare_exchange(None, Some(value.object_size))
                {
                    Ok(None) => {
                        // First piece
                    }
                    Err(Some(object_size)) => {
                        if value.object_size != object_size {
                            return Err(ServiceError::ObjectSizeInconsistency {
                                new: value.object_size,
                                prev: object_size,
                            });
                        }
                    }
                    Ok(Some(_)) | Err(None) => unreachable!("CAS"),
                }
                // It gets initialized as now() for insertion via fetch() in case of a miss,
                // but we need this to signal whether it was a hit or miss.
                // 0 => miss, non-zero => timestamp.
                if hit_state.load(std::sync::atomic::Ordering::Relaxed) {
                    metrics::page_request_count(&key.kind, "cache_hit");
                } else {
                    value.cached_at = 0;
                }
                Ok((page_id, value))
            }
            Err(err) => Err(match err.downcast_ref::<DownloadError>() {
                Some(download_err) => ServiceError::Download(download_err.clone()),
                None => ServiceError::Cache(err),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
