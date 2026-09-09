mod config;
mod downloader;
mod hedging;
mod stats;

#[cfg(test)]
mod simulation;

pub use config::{DownloadLimits, RequestConfig};
pub use downloader::{DownloadError, DownloadOutput, Downloader, ObjectPiece};
pub use stats::BucketMetrics;
mod admission;
