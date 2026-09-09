use serde::{Deserialize, Serialize};

use crate::object_store::DownloadLimits;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Replica {
    pub headers_ms: u64,
    pub body_ms: u64,
    pub slots: u32,
    pub queue: u32,
}

impl Default for Replica {
    fn default() -> Self {
        Self {
            headers_ms: 1,
            body_ms: 2,
            slots: 1024,
            queue: 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FaultKind {
    ServiceError,
    BodyError,
    Missing,
    HeaderStall,
    BodyStall,
    Slow,
    Overload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Fault {
    pub start_ms: u64,
    pub end_ms: u64,
    pub replicas: Vec<usize>,
    pub kind: FaultKind,
    pub probability_ppm: u32,
    pub correlated: bool,
    pub first_sdk_attempt_only: bool,
    pub duration_ms: u64,
    pub clock_at_arrival: bool,
}

impl Default for Fault {
    fn default() -> Self {
        Self {
            start_ms: 2000,
            end_ms: 8000,
            replicas: vec![0],
            kind: FaultKind::BodyError,
            probability_ppm: 1_000_000,
            correlated: false,
            first_sdk_attempt_only: false,
            duration_ms: 20_000,
            clock_at_arrival: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Scenario {
    pub name: String,
    pub duration_ms: u64,
    pub interval_ms: u64,
    pub warmup_per_replica: u32,
    pub body_bytes: usize,
    pub request_bytes: Option<u64>,
    pub chunks: u32,
    pub jitter_percent: u32,
    pub replicas: Vec<Replica>,
    pub faults: Vec<Fault>,
    pub bucket_timeout_ms: u64,
    pub page_timeout_ms: u64,
    pub hedge_percent: u8,
    pub max_inflight_requests: u32,
    pub max_inflight_bytes: u64,
    pub sdk_attempts: u32,
    pub sdk_backoff_ms: u64,
    pub cancellation_lag_ms: u64,
    pub cancel_read_after_ms: Option<u64>,
}

impl Default for Scenario {
    fn default() -> Self {
        Self {
            name: String::new(),
            duration_ms: 12_000,
            interval_ms: 20,
            warmup_per_replica: 16,
            body_bytes: 1024,
            request_bytes: None,
            chunks: 4,
            jitter_percent: 0,
            replicas: vec![
                Replica::default(),
                Replica {
                    body_ms: 4,
                    ..Replica::default()
                },
                Replica {
                    body_ms: 5,
                    ..Replica::default()
                },
            ],
            faults: vec![],
            bucket_timeout_ms: 5000,
            page_timeout_ms: 10_000,
            hedge_percent: 0,
            max_inflight_requests: 1024,
            max_inflight_bytes: 1024 * 1024 * 1024,
            sdk_attempts: 1,
            sdk_backoff_ms: 10,
            cancellation_lag_ms: 0,
            cancel_read_after_ms: None,
        }
    }
}

impl Scenario {
    pub fn validate(&self) -> eyre::Result<()> {
        eyre::ensure!(
            !self.name.is_empty() && self.interval_ms > 0 && self.duration_ms > 0,
            "invalid arrival schedule"
        );
        eyre::ensure!(
            self.body_bytes > 0 && self.body_bytes <= 16 * 1024 * 1024 && self.chunks > 0,
            "invalid body shape"
        );
        eyre::ensure!(
            !self.replicas.is_empty() && self.replicas.iter().all(|replica| replica.slots > 0),
            "invalid replica capacity"
        );
        eyre::ensure!(
            self.sdk_attempts > 0 && self.jitter_percent <= 100,
            "invalid SDK or latency configuration"
        );
        for fault in &self.faults {
            eyre::ensure!(
                fault.start_ms < fault.end_ms
                    && fault.probability_ppm <= 1_000_000
                    && fault
                        .replicas
                        .iter()
                        .all(|index| *index < self.replicas.len()),
                "invalid fault"
            );
        }
        eyre::ensure!(
            self.request_bytes() >= self.body_bytes as u64
                && self.request_bytes() <= 16 * 1024 * 1024,
            "requested range must cover the response and fit a cache page"
        );
        self.limits().validate()
    }

    pub fn limits(&self) -> DownloadLimits {
        DownloadLimits {
            bucket_timeout: std::time::Duration::from_millis(self.bucket_timeout_ms),
            page_timeout: std::time::Duration::from_millis(self.page_timeout_ms),
            hedge_budget_percent: self.hedge_percent,
            max_inflight_requests: self.max_inflight_requests,
            max_inflight_bytes: self.max_inflight_bytes,
        }
    }

    pub fn request_bytes(&self) -> u64 {
        self.request_bytes.unwrap_or(self.body_bytes as u64)
    }

    pub fn reads(&self) -> usize {
        self.duration_ms.div_ceil(self.interval_ms) as usize
    }
}

pub fn scenarios() -> Vec<Scenario> {
    serde_json::from_str(include_str!("../../../tests/scenarios/replica_reads.json"))
        .expect("valid checked-in scenarios")
}

pub fn mix(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

pub fn draw(
    seed: u64,
    read: u64,
    replica: usize,
    invocation: u32,
    attempt: u32,
    domain: u64,
) -> u64 {
    mix(seed
        ^ mix(read)
        ^ mix(replica as u64 + 100)
        ^ mix(u64::from(invocation) + 200)
        ^ mix(u64::from(attempt) + 300)
        ^ mix(domain))
}
