use std::{sync::LazyLock, time::Duration};

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use prometheus::{
    Encoder, GaugeVec, HistogramVec, IntCounterVec, IntGauge, TextEncoder, register_gauge_vec,
    register_histogram_vec, register_int_counter_vec, register_int_gauge,
};

use crate::{
    object_store::{BucketMetrics, Hedge, Predictions, QUANTILES, REPORTED_SIZES},
    types::{BucketName, ObjectKind},
};

static SIZE_LABELS: LazyLock<[String; 4]> =
    LazyLock::new(|| REPORTED_SIZES.map(|bytes| bytes.to_string()));

static QUANTILE_LABELS: LazyLock<[String; 3]> =
    LazyLock::new(|| QUANTILES.map(|quantile| quantile.to_string()));

/// Only the request model is reported; `phase` stays so existing queries keep matching.
const PHASE: &str = "request";

/// Median and p99, as indices into [`QUANTILES`].
const REPORTED_QUANTILES: [usize; 2] = [0, 2];

pub fn set_bucket_stats(bucket: &BucketName, metrics: &BucketMetrics) {
    type BucketGauge = (GaugeVec, fn(&BucketMetrics) -> f64);

    static GAUGES: LazyLock<[BucketGauge; 3]> = LazyLock::new(|| {
        let gauge = |name: &str, help: &str| register_gauge_vec!(name, help, &["bucket"]).unwrap();
        [
            (
                gauge(
                    "cachey_bucket_error_ratio",
                    "Exponentially decayed fraction of completed bucket fetches that failed",
                ),
                |metrics| metrics.error_rate,
            ),
            (
                gauge(
                    "cachey_bucket_deprioritized",
                    "Whether the bucket is deprioritized while remaining eligible for fallback",
                ),
                |metrics| f64::from(u8::from(metrics.deprioritized)),
            ),
            (
                gauge(
                    "cachey_bucket_consecutive_failures",
                    "Number of consecutive failures per bucket",
                ),
                |metrics| f64::from(metrics.consecutive_failures),
            ),
        ]
    });

    static LATENCY: LazyLock<GaugeVec> = LazyLock::new(|| {
        register_gauge_vec!(
            "cachey_bucket_latency_seconds",
            "Latency quantile at a read size in bytes",
            &["bucket", "phase", "bytes", "quantile"]
        )
        .unwrap()
    });

    static MODEL_BYTES: LazyLock<GaugeVec> = LazyLock::new(|| {
        register_gauge_vec!(
            "cachey_bucket_latency_model_bytes",
            "Largest size retained by the latency model",
            &["bucket", "phase"]
        )
        .unwrap()
    });

    for (gauge, value) in &*GAUGES {
        gauge.with_label_values(&[bucket]).set(value(metrics));
    }
    let sizes = metrics
        .request
        .as_ref()
        .map_or(&[][..], |estimate| &estimate.sizes);
    for (bytes, bytes_label) in REPORTED_SIZES.iter().zip(&*SIZE_LABELS) {
        let quantiles = sizes
            .iter()
            .find(|size| size.bytes == *bytes)
            .map(|size| size.quantiles);
        for index in REPORTED_QUANTILES {
            let labels = [&**bucket, PHASE, bytes_label, &QUANTILE_LABELS[index]];
            match quantiles {
                Some(quantiles) => LATENCY
                    .with_label_values(&labels)
                    .set(quantiles[index].as_secs_f64()),
                None => {
                    let _ = LATENCY.remove_label_values(&labels);
                }
            }
        }
    }
    match &metrics.request {
        Some(estimate) => {
            MODEL_BYTES
                .with_label_values(&[&**bucket, PHASE])
                .set(estimate.largest_bytes as f64);
            count_predictions(bucket, estimate.predictions);
        }
        None => {
            let _ = MODEL_BYTES.remove_label_values(&[bucket, PHASE]);
        }
    }
}

/// Advances counters to the cumulative totals of a bucket's statistics. Concurrent
/// scrapes take turns, or each would add the same difference.
fn count_predictions(bucket: &BucketName, predictions: Predictions) {
    static ADVANCING: Mutex<()> = Mutex::new(());

    static PREDICTIONS: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec!(
            "cachey_bucket_latency_predictions_total",
            "Latency observations at sizes the model predicted",
            &["bucket", "phase"]
        )
        .unwrap()
    });

    static PREDICTIONS_EXCEEDED: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec!(
            "cachey_bucket_latency_predictions_exceeded_total",
            "Latency observations slower than the model's predicted quantile",
            &["bucket", "phase", "quantile"]
        )
        .unwrap()
    });

    for (counter, total) in [
        (
            PREDICTIONS.with_label_values(&[&**bucket, PHASE]),
            predictions.observations,
        ),
        (
            PREDICTIONS_EXCEEDED.with_label_values(&[&**bucket, PHASE, &QUANTILE_LABELS[0]]),
            predictions.above_median,
        ),
        (
            PREDICTIONS_EXCEEDED.with_label_values(&[&**bucket, PHASE, &QUANTILE_LABELS[2]]),
            predictions.above_tail,
        ),
    ] {
        let _advancing = ADVANCING.lock();
        counter.inc_by(total.saturating_sub(counter.get()));
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
            vec![f64::from(64 << 10), f64::from(1 << 20), f64::from(16 << 20)]
        )
        .unwrap()
    });

    HISTOGRAM
        .with_label_values(&[&**kind])
        .observe(bytes as f64);
}

pub fn fetch_request_entries(kind: &ObjectKind, entries: usize) {
    static HISTOGRAM: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec!(
            "cachey_fetch_request_entries",
            "Number of cache entries a fetch spans: pages, or one exact range",
            &["kind"],
            vec![1.0, 2.0, 4.0]
        )
        .unwrap()
    });

    HISTOGRAM
        .with_label_values(&[&**kind])
        .observe(entries as f64);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryRequestType {
    /// Total cache entry requests observed by the service.
    Access,
    /// Entry requests that fetched bytes from object storage.
    Download,
    /// Successful entry downloads that started overlapping requests.
    Hedged,
    /// Downloads whose primary attempt used the client-preferred bucket.
    ClientPref,
    /// Downloads that succeeded using a copy other than the initially selected one.
    Fallback,
    /// Entry requests that completed successfully, regardless of hit/miss path.
    Success,
    /// Aggregate cache-hit count (includes both memory and disk hits).
    CacheHit,
    /// Cache hits served from in-memory cache.
    CacheHitMemory,
    /// Cache hits served from disk cache.
    CacheHitDisk,
    /// Requests that waited on another in-flight miss for the same entry instead of downloading.
    Coalesced,
}

impl EntryRequestType {
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

pub fn entry_request_count(kind: &ObjectKind, typ: EntryRequestType) {
    static COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec!(
            "cachey_entry_requests_total",
            "Cache entry requests: pages, or exact ranges",
            &["kind", "type"]
        )
        .unwrap()
    });

    COUNTER.with_label_values(&[&**kind, typ.as_label()]).inc();
}

pub fn hedge_count(kind: &ObjectKind, primary_bucket: &BucketName, hedge: Hedge) {
    static COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec!(
            "cachey_hedges_total",
            "Early hedges of successful downloads, by the phase they fired in, whether they went to the primary's bucket, and whether they served the response",
            &["kind", "bucket", "phase", "target", "outcome"]
        )
        .unwrap()
    });

    let [phase, target, outcome] = hedge.labels();
    COUNTER
        .with_label_values(&[&**kind, &**primary_bucket, phase, target, outcome])
        .inc();
}

pub fn download_latency(kind: &ObjectKind, latency: std::time::Duration) {
    static HISTOGRAM: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec!(
            "cachey_download_latency_seconds",
            "Successful download latency of a cache entry, including retries, hedging, and bucket fallback",
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
