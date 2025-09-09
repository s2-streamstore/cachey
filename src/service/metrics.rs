use std::sync::LazyLock;

use bytes::{BufMut, Bytes, BytesMut};
use prometheus::{
    Encoder, GaugeVec, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, TextEncoder,
    register_gauge_vec, register_histogram_vec, register_int_counter_vec, register_int_gauge,
    register_int_gauge_vec,
};

use crate::{
    object_store::BucketMetrics,
    types::{BucketName, ObjectKind},
};

pub fn set_bucket_stats(bucket: &BucketName, metrics: BucketMetrics) {
    static ERROR_RATE: LazyLock<GaugeVec> = LazyLock::new(|| {
        register_gauge_vec!(
            "cachey_bucket_error_rate",
            "Exponentially decayed error rate per bucket",
            &["bucket"]
        )
        .unwrap()
    });

    static LATENCY_MEAN: LazyLock<GaugeVec> = LazyLock::new(|| {
        register_gauge_vec!(
            "cachey_bucket_latency_mean_seconds",
            "Mean latency in seconds per bucket",
            &["bucket"]
        )
        .unwrap()
    });

    static LATENCY_HEDGE: LazyLock<GaugeVec> = LazyLock::new(|| {
        register_gauge_vec!(
            "cachey_bucket_latency_hedge_seconds",
            "Hedge latency in seconds per bucket",
            &["bucket"]
        )
        .unwrap()
    });

    static CIRCUIT_BREAKER_OPEN: LazyLock<IntGaugeVec> = LazyLock::new(|| {
        register_int_gauge_vec!(
            "cachey_bucket_circuit_breaker_open",
            "Whether circuit breaker is open (1) or closed (0) per bucket",
            &["bucket"]
        )
        .unwrap()
    });

    static CONSECUTIVE_FAILURES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
        register_int_gauge_vec!(
            "cachey_bucket_consecutive_failures",
            "Number of consecutive failures per bucket",
            &["bucket"]
        )
        .unwrap()
    });

    ERROR_RATE
        .with_label_values(&[bucket])
        .set(metrics.error_rate);
    LATENCY_MEAN
        .with_label_values(&[bucket])
        .set(metrics.latency_mean.as_secs_f64());
    LATENCY_HEDGE
        .with_label_values(&[bucket])
        .set(metrics.latency_hedge.as_secs_f64());
    CIRCUIT_BREAKER_OPEN
        .with_label_values(&[bucket])
        .set(if metrics.circuit_breaker_open { 1 } else { 0 });
    CONSECUTIVE_FAILURES
        .with_label_values(&[bucket])
        .set(metrics.consecutive_failures as i64);
}

pub fn fetch_request_count(kind: &ObjectKind, method: &axum::http::Method, typ: &str) {
    static COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec!(
            "cachey_fetch_request_total",
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
                (512 * 1024) as f64,       // 512 KiB
                (1024 * 1024) as f64,      // 1 MiB
                (2 * 1024 * 1024) as f64,  // 2 MiB
                (4 * 1024 * 1024) as f64,  // 4 MiB
                (8 * 1024 * 1024) as f64,  // 8 MiB
                (16 * 1024 * 1024) as f64, // 16 MiB
            ]
        )
        .unwrap()
    });

    HISTOGRAM
        .with_label_values(&[&**kind])
        .observe(bytes as f64);
}

pub fn fetch_request_pages(kind: &ObjectKind, pages: u16) {
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

pub fn page_request_count(kind: &ObjectKind, typ: &str) {
    static COUNTER: LazyLock<IntCounterVec> = LazyLock::new(|| {
        register_int_counter_vec!(
            "cachey_page_request_total",
            "Page requests",
            &["kind", "type"]
        )
        .unwrap()
    });

    COUNTER.with_label_values(&[&**kind, typ]).inc();
}

pub fn page_download_latency(kind: &ObjectKind, latency: std::time::Duration) {
    static HISTOGRAM: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec!(
            "cachey_page_download_latency_seconds",
            "Page download latency",
            &["kind"],
            vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
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
            "cachey_throughput_bytes_per_sec",
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
        register_int_gauge!(
            "cachey_connection_count",
            "Current number of active connections"
        )
        .unwrap()
    });

    CONNECTION_COUNT.set(count as i64);
}

pub fn gather() -> Bytes {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = BytesMut::new().writer();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    buffer.into_inner().freeze()
}
