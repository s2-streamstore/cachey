use std::collections::BTreeMap;

use serde::Serialize;

use super::{
    backend::{State, Transport},
    model::Scenario,
};
use crate::object_store::DownloadError;

#[derive(Debug, Clone, Serialize)]
pub struct Copy {
    pub read: u64,
    pub replica: usize,
    pub start_us: u64,
    pub end_us: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Health {
    pub replica: usize,
    pub deprioritized: bool,
    pub recovery_successes: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct Read {
    pub id: u64,
    pub arrival_us: u64,
    pub end_us: u64,
    pub error: Option<String>,
    pub primary: Option<usize>,
    pub winner: Option<usize>,
    pub hedged: bool,
    pub health: Vec<Health>,
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
    all_arrival_latency: Latency,
    copy_operations: usize,
    pub transport_attempts: usize,
    sdk_invocations: usize,
    peak_copy_operations: usize,
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
    overlapping_reads: usize,
    wins_by_replica: Vec<usize>,
    pub recoveries: Vec<Recovery>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reads: Option<Vec<Read>>,
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
        chronological.sort_by_key(|read| (read.end_us, read.id));
        let mut impaired = vec![None; scenario.replicas.len()];
        let mut recoveries = vec![];
        for read in chronological {
            for health in &read.health {
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
                    read.error.is_none()
                        && read.end_us - read.arrival_us < scenario.page_timeout_ms * 1000
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
                    .map(|read| read.end_us - read.arrival_us)
                    .collect(),
            ),
            all_arrival_latency: Latency::new(
                reads
                    .iter()
                    .map(|read| read.end_us - read.arrival_us)
                    .collect(),
            ),
            copy_operations: copies.len(),
            transport_attempts: state.transports.len(),
            sdk_invocations: state
                .transports
                .iter()
                .filter(|transport| transport.attempt == 1)
                .count(),
            peak_copy_operations: peak_copies,
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
            requested_bytes: state.transports.len() as u64 * scenario.body_bytes as u64,
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
            useful_bytes: completed as u64 * scenario.body_bytes as u64,
            overlapping_reads: reads.iter().filter(|read| read.hedged).count(),
            wins_by_replica: wins,
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
        DownloadError::Overloaded(_) => "overloaded",
        DownloadError::AdmissionTimeout => "admission_timeout",
        DownloadError::AdmissionExhausted { .. } => "admission_exhausted",
        DownloadError::Timeout { .. } => "timeout",
        DownloadError::Unknown(_) => "unknown",
    }
}
