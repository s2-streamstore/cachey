use std::{
    num::NonZeroU32,
    ops::Range,
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use bytes::Bytes;
use crossbeam::atomic::AtomicCell;
use eyre::Result;
use futures::{Stream, StreamExt, TryStreamExt};
use parking_lot::Mutex;

mod metrics;
mod throughput;
pub use throughput::SlidingThroughput;
mod routes;

use crate::{
    cache::{CacheConfig, CacheKey, CacheValue, build_cache},
    object_store::{DownloadError, Downloader, S3RequestProfile},
    types::{BucketName, BucketNameSet, ObjectKey, ObjectKind, PageId},
};

pub const PAGE_SIZE: u64 = 16 * 1024 * 1024;

pub const MAX_RANGE_END: u64 = PAGE_SIZE * PageId::MAX as u64;

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
}

impl CacheyService {
    pub async fn new(config: ServiceConfig, s3: aws_sdk_s3::Client) -> Result<Self> {
        let cache = build_cache(config.cache).await?;
        let ingress_throughput = Arc::new(Mutex::new(SlidingThroughput::default()));
        let egress_throughput = Arc::new(Mutex::new(SlidingThroughput::default()));
        let downloader = Downloader::new(s3, config.hedge_quantile, ingress_throughput.clone());
        Ok(Self {
            cache,
            downloader,
            ingress_throughput,
            egress_throughput,
        })
    }

    pub fn observe_metrics(&self) {
        self.downloader
            .observe_bucket_metrics(metrics::set_bucket_stats);

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
        s3_profile: S3RequestProfile,
    ) -> impl Stream<Item = Result<Chunk, ServiceError>> {
        assert!(byterange.start < byterange.end);
        assert!(byterange.end <= MAX_RANGE_END);

        let first_page = (byterange.start / PAGE_SIZE) as PageId;
        let last_page = (byterange.end / PAGE_SIZE) as PageId;

        metrics::fetch_request_bytes(&kind, byterange.end - byterange.start);
        metrics::fetch_request_pages(&kind, last_page - first_page + 1);

        let executor = PageGetExecutor {
            cache: self.cache,
            downloader: self.downloader,
            kind,
            object,
            buckets,
            object_size: Default::default(),
            s3_profile,
        };

        futures::stream::iter(
            (first_page..=last_page).map(move |page_id| executor.clone().execute(page_id)),
        )
        .buffered(concurrency)
        .map_ok(move |(page_id, value)| {
            let mut range_start = page_id as u64 * PAGE_SIZE;
            let mut range_end = range_start + value.data.len() as u64;
            let mut start_offset = 0;
            let mut end_offset = value.data.len();
            if page_id == first_page {
                start_offset = (byterange.start % PAGE_SIZE) as usize;
                range_start = byterange.start;
            }
            if page_id == last_page {
                end_offset = (byterange.end % PAGE_SIZE) as usize;
                range_end = byterange.end;
            }
            let data = value.data.slice(start_offset..end_offset);
            self.egress_throughput.lock().record(data.len());
            Chunk {
                bucket: value.bucket,
                mtime: value.mtime,
                data,
                range: range_start..range_end,
                object_size: value.object_size,
                cached_at: NonZeroU32::new(value.cached_at),
            }
        })
    }

    pub fn into_router(self) -> axum::Router {
        axum::Router::new()
            .route("/metrics", axum::routing::get(routes::metrics))
            .route("/stats", axum::routing::get(routes::stats))
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
    s3_profile: S3RequestProfile,
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
            .fetch(cache_key, {
                let hit_state = hit_state.clone();
                move || async move {
                    hit_state.store(false, std::sync::atomic::Ordering::Relaxed);
                    metrics::page_request_count(&self.kind, "download");

                    let start = page_id as u64 * PAGE_SIZE;
                    let end = start + PAGE_SIZE;
                    let out = match self
                        .downloader
                        .download(&self.buckets, self.object, &(start..end), &self.s3_profile)
                        .await
                    {
                        Ok(out) => {
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
                            out
                        }
                        Err(e) => {
                            return Err(foyer::Error::Other(Box::new(e)));
                        }
                    };
                    Ok(CacheValue {
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
            Err(err @ foyer::Error::Memory(_) | err @ foyer::Error::Storage(_)) => {
                Err(ServiceError::Cache(err))
            }
            Err(foyer::Error::Other(other)) => Err(match other.downcast() {
                Ok(e) => ServiceError::Download(*e),
                Err(e) => ServiceError::Cache(foyer::Error::Other(e)),
            }),
        }
    }
}
