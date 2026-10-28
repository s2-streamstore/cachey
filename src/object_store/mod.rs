mod admission;
mod budget;
mod config;
mod downloader;
mod stats;

#[cfg(test)]
mod simulation;

/// The SDK shares a retry token bucket among a process's clients with the same retry
/// partition, so test clients that share one draw down each other's retries.
#[cfg(test)]
pub(crate) fn isolated_retry_partition() -> aws_sdk_s3::config::retry::RetryPartition {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let partition = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    aws_sdk_s3::config::retry::RetryPartition::new(format!("cachey-test-{partition}"))
}

pub use config::{DownloadLimits, RequestConfig};
pub use downloader::{DownloadError, DownloadOutput, Downloader, Hedge, HedgePhase, ObjectPiece};
pub use stats::{BandLatency, BucketMetrics, LatencyEstimate, Predictions, QUANTILES, size_bands};
