use std::{sync::LazyLock, time::Duration};

use bytes::{BufMut, Bytes, BytesMut};
use prometheus::{
    Encoder, GaugeVec, HistogramVec, IntCounterVec, IntGauge, TextEncoder, register_gauge_vec,
    register_histogram_vec, register_int_counter_vec, register_int_gauge,
};

use crate::{
    object_store::BucketMetrics,
    types::{BucketName, ObjectKind},
};

fn seconds(duration: Option<Duration>) -> Option<f64> {
    duration.map(|duration| duration.as_secs_f64())
}

pub fn set_bucket_stats(bucket: &BucketName, metrics: &BucketMetrics) {
    type BucketGauge = (GaugeVec, fn(&BucketMetrics) -> Option<f64>);

    static GAUGES: LazyLock<[BucketGauge; 9]> = LazyLock::new(|| {
        let gauge = |name: &str, help: &str| register_gauge_vec!(name, help, &["bucket"]).unwrap();
        [
            (
                gauge(
                    "cachey_bucket_error_ratio",
                    "Exponentially decayed fraction of completed bucket fetches that failed",
                ),
                |metrics| Some(metrics.error_rate),
            ),
            (
                gauge(
                    "cachey_bucket_latency_mean_seconds",
                    "Mean time from starting a request to its validated body, including SDK retries",
                ),
                |metrics| seconds(metrics.latency_mean),
            ),
            (
                gauge(
                    "cachey_bucket_latency_p99_seconds",
                    "p99 time from starting a request to its validated body, including SDK retries",
                ),
                |metrics| seconds(metrics.latency_p99),
            ),
            (
                gauge(
                    "cachey_bucket_first_byte_mean_seconds",
                    "Mean time from starting a request to successful response headers, including SDK retries",
                ),
                |metrics| seconds(metrics.first_byte_mean),
            ),
            (
                gauge(
                    "cachey_bucket_first_byte_p99_seconds",
                    "p99 time from starting a request to successful response headers, including SDK retries",
                ),
                |metrics| seconds(metrics.first_byte_p99),
            ),
            (
                gauge(
                    "cachey_bucket_body_model_bytes",
                    "Largest observed response-size band used for the exported body transfer estimate",
                ),
                |metrics| metrics.body_model_bytes.map(|bytes| bytes as f64),
            ),
            (
                gauge(
                    "cachey_bucket_body_seconds_per_byte",
                    "Uncapped p99 body time per byte beyond 1 ms, at cachey_bucket_body_model_bytes",
                ),
                |metrics| metrics.body_seconds_per_byte,
            ),
            (
                gauge(
                    "cachey_bucket_deprioritized",
                    "Whether the bucket is deprioritized while remaining eligible for fallback",
                ),
                |metrics| Some(f64::from(u8::from(metrics.deprioritized))),
            ),
            (
                gauge(
                    "cachey_bucket_consecutive_failures",
                    "Number of consecutive failures per bucket",
                ),
                |metrics| Some(f64::from(metrics.consecutive_failures)),
            ),
        ]
    });

    for (gauge, value) in &*GAUGES {
        match value(metrics) {
            Some(value) => gauge.with_label_values(&[bucket]).set(value),
            None => {
                let _ = gauge.remove_label_values(&[bucket]);
            }
        }
    }
}

pub fn fetch_request_count(kind: &ObjectKind, method: &axum::http::Method, typ: &str) {
    static COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec!(
            "cachey_fetch_requests_total",
            "Fetch requests",
            &["kind", "method", "status"]
        )
        .unwrap()
    });

    COUNTER
        .with_label_values(&[&**kind, method.as_str(), typ])
        .inc();
}

pub fn fetch_request_bytes(kind: &ObjectKind, bytes: u64) {
    static HISTOGRAM: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec!(
            "cachey_fetch_request_bytes",
            "Number of bytes requested",
            &["kind"],
            vec![
                f64::from(512 * 1024),       // 512 KiB
                f64::from(1024 * 1024),      // 1 MiB
                f64::from(2 * 1024 * 1024),  // 2 MiB
                f64::from(4 * 1024 * 1024),  // 4 MiB
                f64::from(8 * 1024 * 1024),  // 8 MiB
                f64::from(16 * 1024 * 1024), // 16 MiB
            ]
        )
        .unwrap()
    });

    HISTOGRAM
        .with_label_values(&[&**kind])
        .observe(bytes as f64);
}

pub fn fetch_request_pages(kind: &ObjectKind, pages: usize) {
    static HISTOGRAM: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec!(
            "cachey_fetch_request_pages",
            "Number of pages requested",
            &["kind"],
            vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0]
        )
        .unwrap()
    });

    HISTOGRAM
        .with_label_values(&[&**kind])
        .observe(pages as f64);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageRequestType {
    /// Total page requests observed by the service.
    Access,
    /// Page requests that fetched bytes from object storage.
    Download,
    /// Successful page downloads that started overlapping requests.
    Hedged,
    /// Fetches whose primary attempt used the client-preferred bucket.
    ClientPref,
    /// Fetches that succeeded using a copy other than the initially selected one.
    Fallback,
    /// Page requests that completed successfully, regardless of hit/miss path.
    Success,
    /// Aggregate cache-hit count (includes both memory and disk hits).
    CacheHit,
    /// Cache hits served from in-memory cache.
    CacheHitMemory,
    /// Cache hits served from disk cache.
    CacheHitDisk,
    /// Requests that waited on another in-flight miss for the same page instead of downloading.
    Coalesced,
}

impl PageRequestType {
    const fn as_label(self) -> &'static str {
        match self {
            Self::Access => "access",
            Self::Download => "download",
            Self::Hedged => "hedged",
            Self::ClientPref => "client_pref",
            Self::Fallback => "fallback",
            Self::Success => "success",
            Self::CacheHit => "cache_hit",
            Self::CacheHitMemory => "cache_hit_memory",
            Self::CacheHitDisk => "cache_hit_disk",
            Self::Coalesced => "coalesced",
        }
    }
}

pub fn page_request_count(kind: &ObjectKind, typ: PageRequestType) {
    static COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec!(
            "cachey_page_requests_total",
            "Page requests",
            &["kind", "type"]
        )
        .unwrap()
    });

    COUNTER.with_label_values(&[&**kind, typ.as_label()]).inc();
}

pub fn page_download_latency(kind: &ObjectKind, latency: std::time::Duration) {
    static HISTOGRAM: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec!(
            "cachey_page_download_latency_seconds",
            "Successful page download latency, including retries, hedging, and bucket fallback",
            &["kind"],
            vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0
            ]
        )
        .unwrap()
    });

    HISTOGRAM
        .with_label_values(&[&**kind])
        .observe(latency.as_secs_f64());
}

pub fn observe_throughput(direction: &str, windowed_bps: &[(&str, f64)]) {
    static GAUGE: LazyLock<GaugeVec> = LazyLock::new(|| {
        register_gauge_vec!(
            "cachey_throughput_bytes_per_second",
            "Throughput by direction and time window",
            &["direction", "window"]
        )
        .unwrap()
    });
    for (window, bps) in windowed_bps {
        GAUGE.with_label_values(&[direction, window]).set(*bps);
    }
}

pub fn set_connection_count(count: usize) {
    static CONNECTION_COUNT: LazyLock<IntGauge> = LazyLock::new(|| {
        register_int_gauge!("cachey_connections", "Current number of active connections").unwrap()
    });

    CONNECTION_COUNT.set(count as i64);
}

pub fn first_chunk_latency(kind: &ObjectKind, hit: bool, latency: Duration) {
    static HISTOGRAM: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec!(
            "cachey_first_chunk_latency_seconds",
            "Time to first chunk",
            &["kind", "hit"],
            vec![
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0
            ]
        )
        .unwrap()
    });

    let hit_str = if hit { "1" } else { "0" };
    HISTOGRAM
        .with_label_values(&[&**kind, hit_str])
        .observe(latency.as_secs_f64());
}

#[cfg(feature = "jemalloc")]
pub fn observe_jemalloc_metrics() {
    use tikv_jemalloc_ctl::{epoch, stats};

    static ALLOCATED: LazyLock<IntGauge> = LazyLock::new(|| {
        register_int_gauge!(
            "cachey_jemalloc_allocated_bytes",
            "jemalloc stats.allocated (live bytes requested by the application)"
        )
        .unwrap()
    });
    static ACTIVE: LazyLock<IntGauge> = LazyLock::new(|| {
        register_int_gauge!(
            "cachey_jemalloc_active_bytes",
            "jemalloc stats.active (bytes in active pages used for allocations)"
        )
        .unwrap()
    });
    static RESIDENT: LazyLock<IntGauge> = LazyLock::new(|| {
        register_int_gauge!(
            "cachey_jemalloc_resident_bytes",
            "jemalloc stats.resident (resident physical bytes for jemalloc-managed memory)"
        )
        .unwrap()
    });
    static RETAINED: LazyLock<IntGauge> = LazyLock::new(|| {
        register_int_gauge!(
            "cachey_jemalloc_retained_bytes",
            "jemalloc stats.retained (bytes retained in virtual memory mappings for reuse; not RSS: use resident/active/allocated for fragmentation/overhead ratios)"
        )
        .unwrap()
    });
    static MAPPED: LazyLock<IntGauge> = LazyLock::new(|| {
        register_int_gauge!(
            "cachey_jemalloc_mapped_bytes",
            "jemalloc stats.mapped (virtual memory mapped by jemalloc)"
        )
        .unwrap()
    });
    static METADATA: LazyLock<IntGauge> = LazyLock::new(|| {
        register_int_gauge!(
            "cachey_jemalloc_metadata_bytes",
            "jemalloc stats.metadata (jemalloc internal metadata bytes)"
        )
        .unwrap()
    });

    fn usize_to_i64(value: usize) -> i64 {
        i64::try_from(value).unwrap_or(i64::MAX)
    }

    if epoch::advance().is_err() {
        return;
    }

    let (Ok(allocated), Ok(active), Ok(resident), Ok(retained), Ok(mapped), Ok(metadata)) = (
        stats::allocated::read(),
        stats::active::read(),
        stats::resident::read(),
        stats::retained::read(),
        stats::mapped::read(),
        stats::metadata::read(),
    ) else {
        return;
    };

    ALLOCATED.set(usize_to_i64(allocated));
    ACTIVE.set(usize_to_i64(active));
    RESIDENT.set(usize_to_i64(resident));
    RETAINED.set(usize_to_i64(retained));
    MAPPED.set(usize_to_i64(mapped));
    METADATA.set(usize_to_i64(metadata));
}

#[cfg(not(feature = "jemalloc"))]
pub fn observe_jemalloc_metrics() {}

pub fn gather() -> Bytes {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = BytesMut::new().writer();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    buffer.into_inner().freeze()
}
