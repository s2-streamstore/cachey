use std::{collections::BTreeMap, time::Duration};

use serde::Serialize;

use super::{
    backend::{State, Transport},
    model::Scenario,
};
use crate::object_store::{BucketMetrics, DownloadError};

#[derive(Debug, Clone, Serialize)]
pub struct Copy {
    pub read: u64,
    pub replica: usize,
    pub start_us: u64,
    pub end_us: u64,
}

/// A bucket's exported latency model, with each estimate absent until measured.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct Estimates {
    latency_p99_us: Option<u64>,
    first_byte_p99_us: Option<u64>,
    small_body_p99_us: Option<u64>,
    large_body_throughput_p1_bytes_per_second: Option<u64>,
}

impl From<&BucketMetrics> for Estimates {
    fn from(metrics: &BucketMetrics) -> Self {
        let micros =
            |duration: Option<Duration>| duration.map(|duration| duration.as_micros() as u64);
        Self {
            latency_p99_us: micros(metrics.latency_p99),
            first_byte_p99_us: micros(metrics.first_byte_p99),
            small_body_p99_us: micros(metrics.small_body_p99),
            large_body_throughput_p1_bytes_per_second: metrics
                .large_body_throughput_p1
                .map(|throughput| throughput as u64),
        }
    }
}

impl Estimates {
    fn slowest(self, other: Self) -> Self {
        Self {
            latency_p99_us: self.latency_p99_us.max(other.latency_p99_us),
            first_byte_p99_us: self.first_byte_p99_us.max(other.first_byte_p99_us),
            small_body_p99_us: self.small_body_p99_us.max(other.small_body_p99_us),
            large_body_throughput_p1_bytes_per_second: self
                .large_body_throughput_p1_bytes_per_second
                .into_iter()
                .chain(other.large_body_throughput_p1_bytes_per_second)
                .min(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Health {
    pub replica: usize,
    pub deprioritized: bool,
    pub recovery_successes: u32,
    pub estimates: Estimates,
}

#[derive(Debug, Clone, Serialize)]
pub struct Read {
    pub id: u64,
    pub body_bytes: usize,
    pub completion_order: usize,
    pub arrival_us: u64,
    pub end_us: u64,
    pub error: Option<String>,
    pub primary: Option<usize>,
    pub winner: Option<usize>,
    pub hedged: bool,
    pub health: Vec<Health>,
}

impl Read {
    pub fn latency_us(&self) -> u64 {
        self.end_us - self.arrival_us
    }
}

#[derive(Debug, Serialize)]
#[allow(clippy::struct_field_names)]
pub struct Latency {
    mean_us: u64,
    p50_us: u64,
    p99_us: u64,
    max_us: u64,
}

impl Latency {
    fn new(mut values: Vec<u64>) -> Self {
        values.sort_unstable();
        let quantile = |percent: usize| {
            values
                .get((values.len() * percent).div_ceil(100).saturating_sub(1))
                .copied()
                .unwrap_or(0)
        };
        Self {
            mean_us: values
                .iter()
                .sum::<u64>()
                .checked_div(values.len() as u64)
                .unwrap_or(0),
            p50_us: quantile(50),
            p99_us: quantile(99),
            max_us: values.last().copied().unwrap_or(0),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Recovery {
    pub replica: usize,
    pub observed_impaired_us: u64,
    pub restored_us: u64,
    pub successes: u32,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub scenario: Scenario,
    seed: u64,
    histogram_mode: &'static str,
    histogram_updates_retained: Vec<(u64, usize)>,
    pub arrivals: usize,
    pub completed: usize,
    completed_before_deadline: usize,
    pub errors: BTreeMap<String, usize>,
    fault_window_arrivals: usize,
    fault_window_failures: usize,
    success_latency: Latency,
    /// Successful latency of full and short responses, in fixtures with short bodies.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    success_latency_by_response: BTreeMap<&'static str, Latency>,
    all_arrival_latency: Latency,
    copy_operations: usize,
    pub transport_attempts: usize,
    sdk_invocations: usize,
    peak_copy_operations: usize,
    peak_copy_bytes: u64,
    peak_transport_concurrency: usize,
    peak_server_concurrency: usize,
    peak_server_queue: usize,
    capacity_rejections: usize,
    pub cancelled_transports: usize,
    pub service_us: u64,
    pub post_read_service_us: u64,
    server_work_after_last_read_us: u64,
    server_drain_after_last_read_us: u64,
    requested_bytes: u64,
    produced_bytes: u64,
    delivered_bytes: u64,
    useful_bytes: u64,
    pub overlapping_reads: usize,
    wins_by_replica: Vec<usize>,
    /// Each bucket's estimates after the last read completed.
    final_estimates: Vec<Estimates>,
    /// Each bucket's slowest estimates observed after any read: highest durations, lowest
    /// throughput.
    slowest_estimates: Vec<Estimates>,
    pub recoveries: Vec<Recovery>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reads: Option<Vec<Read>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    copies: Option<Vec<Copy>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transports: Option<Vec<Transport>>,
}

impl Report {
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub fn new(
        scenario: Scenario,
        seed: u64,
        native: bool,
        trace: bool,
        reads: Vec<Read>,
        copies: Vec<Copy>,
        peak_copies: usize,
        peak_bytes: u64,
        state: &State,
        last_read_us: u64,
        mut histograms: Vec<(u64, usize)>,
    ) -> Self {
        let mut errors = BTreeMap::new();
        let mut wins = vec![0; scenario.replicas.len()];
        for read in &reads {
            if let Some(error) = &read.error {
                *errors.entry(error.clone()).or_default() += 1;
            }
            if let Some(winner) = read.winner {
                wins[winner] += 1;
            }
        }
        let completed = reads.iter().filter(|read| read.error.is_none()).count();
        assert!(peak_copies <= scenario.max_inflight_requests as usize);
        assert!(peak_bytes <= scenario.max_inflight_bytes);
        let service_after = |transport: &Transport, time: u64| {
            transport.service_start_us.map_or(0, |start| {
                transport
                    .server_end_us
                    .unwrap()
                    .saturating_sub(start.max(time))
            })
        };
        let service_us = state
            .transports
            .iter()
            .map(|transport| service_after(transport, 0))
            .sum();
        let post_read_service_us = state
            .transports
            .iter()
            .map(|transport| service_after(transport, reads[transport.read as usize].end_us))
            .sum();
        let mut chronological: Vec<_> = reads.iter().collect();
        chronological.sort_by_key(|read| read.completion_order);
        let mut impaired = vec![None; scenario.replicas.len()];
        let mut recoveries = vec![];
        let mut final_estimates = vec![Estimates::default(); scenario.replicas.len()];
        let mut slowest_estimates = final_estimates.clone();
        for read in chronological {
            for health in &read.health {
                final_estimates[health.replica] = health.estimates;
                slowest_estimates[health.replica] =
                    slowest_estimates[health.replica].slowest(health.estimates);
                if health.deprioritized {
                    impaired[health.replica].get_or_insert(read.end_us);
                } else if let Some(since) = impaired[health.replica].take() {
                    recoveries.push(Recovery {
                        replica: health.replica,
                        observed_impaired_us: since,
                        restored_us: read.end_us,
                        successes: health.recovery_successes,
                    });
                }
            }
        }
        let fault_window = |read: &&Read| {
            scenario
                .faults
                .iter()
                .any(|fault| (fault.start_ms..fault.end_ms).contains(&(read.arrival_us / 1000)))
        };
        histograms.sort_unstable();
        let mut latency_by_response = BTreeMap::<&'static str, Vec<u64>>::new();
        if scenario.short_body.is_some() {
            for read in reads.iter().filter(|read| read.error.is_none()) {
                latency_by_response
                    .entry(if read.body_bytes == scenario.body_bytes {
                        "full"
                    } else {
                        "short"
                    })
                    .or_default()
                    .push(read.latency_us());
            }
        }
        Self {
            seed,
            histogram_mode: if native {
                "production_reservoir_unseeded"
            } else {
                "retain_all_samples"
            },
            histogram_updates_retained: histograms,
            arrivals: reads.len(),
            completed,
            completed_before_deadline: reads
                .iter()
                .filter(|read| {
                    read.error.is_none() && read.latency_us() < scenario.page_timeout_ms * 1000
                })
                .count(),
            errors,
            fault_window_arrivals: reads.iter().filter(fault_window).count(),
            fault_window_failures: reads
                .iter()
                .filter(fault_window)
                .filter(|read| read.error.is_some())
                .count(),
            success_latency: Latency::new(
                reads
                    .iter()
                    .filter(|read| read.error.is_none())
                    .map(Read::latency_us)
                    .collect(),
            ),
            success_latency_by_response: latency_by_response
                .into_iter()
                .map(|(response, latencies)| (response, Latency::new(latencies)))
                .collect(),
            all_arrival_latency: Latency::new(reads.iter().map(Read::latency_us).collect()),
            copy_operations: copies.len(),
            transport_attempts: state.transports.len(),
            sdk_invocations: state
                .transports
                .iter()
                .filter(|transport| transport.attempt == 1)
                .count(),
            peak_copy_operations: peak_copies,
            peak_copy_bytes: peak_bytes,
            peak_transport_concurrency: state.peak_client,
            peak_server_concurrency: state.peak_server,
            peak_server_queue: state.peak_queued,
            capacity_rejections: state
                .transports
                .iter()
                .filter(|transport| transport.capacity_rejected)
                .count(),
            cancelled_transports: state
                .transports
                .iter()
                .filter(|transport| transport.cancelled)
                .count(),
            service_us,
            post_read_service_us,
            server_work_after_last_read_us: state
                .transports
                .iter()
                .map(|transport| service_after(transport, last_read_us))
                .sum(),
            server_drain_after_last_read_us: state
                .transports
                .iter()
                .map(|transport| {
                    transport
                        .server_end_us
                        .unwrap()
                        .saturating_sub(last_read_us)
                })
                .max()
                .unwrap_or(0),
            requested_bytes: state
                .transports
                .iter()
                .map(|transport| scenario.read_request_bytes(seed, transport.read))
                .sum(),
            produced_bytes: state
                .transports
                .iter()
                .map(|transport| transport.produced_bytes)
                .sum(),
            delivered_bytes: state
                .transports
                .iter()
                .map(|transport| transport.delivered_bytes)
                .sum(),
            useful_bytes: reads
                .iter()
                .filter(|read| read.error.is_none())
                .map(|read| read.body_bytes as u64)
                .sum(),
            overlapping_reads: reads.iter().filter(|read| read.hedged).count(),
            wins_by_replica: wins,
            final_estimates,
            slowest_estimates,
            recoveries,
            scenario,
            reads: trace.then_some(reads),
            copies: trace.then_some(copies),
            transports: trace.then(|| state.transports.clone()),
        }
    }
}

pub fn error_name(error: &DownloadError) -> &'static str {
    match error {
        DownloadError::InvalidObjectState(_) => "invalid_object_state",
        DownloadError::NoSuchKey => "missing",
        DownloadError::RangeNotSatisfied { .. } => "range",
        DownloadError::BodyStreaming(_) => "body",
        DownloadError::InvalidResponse(_) => "invalid_response",
        DownloadError::Overloaded(_) => "overloaded",
        DownloadError::AdmissionTimeout => "admission_timeout",
        DownloadError::AdmissionExhausted { .. } => "admission_exhausted",
        DownloadError::Timeout { .. } => "timeout",
        DownloadError::Unknown(_) => "unknown",
    }
}
