mod backend;
mod model;
mod report;

use std::{sync::Arc, time::Duration};

use aws_sdk_s3::config::{Credentials, Region, retry::RetryConfig};
use aws_smithy_runtime_api::client::http::{SharedHttpConnector, http_client_fn};
use parking_lot::Mutex;
use tokio::time::{Instant, sleep_until};

use self::{
    backend::{Backend, WARM_READ},
    model::{Scenario, mix},
    report::{Copy, Health, Read, Report},
};
use super::{Downloader, RequestConfig};
use crate::{
    service::SlidingThroughput,
    types::{BucketName, BucketNameSet, ObjectKey},
};

struct Controls {
    capacity: Option<usize>,
    seed: u64,
    state: Mutex<ControlState>,
}

struct ControlState {
    epoch: Instant,
    recovery_draw: u64,
    copies: Vec<Copy>,
    active_copies: usize,
    peak_copies: usize,
    active_bytes: u64,
    peak_bytes: u64,
    completed_reads: usize,
}

tokio::task_local! { static CONTROLS: Arc<Controls>; }

pub(super) fn histogram_capacity() -> Option<usize> {
    CONTROLS
        .try_with(|controls| controls.capacity)
        .ok()
        .flatten()
}

pub(super) fn recovery_delay() -> Option<Duration> {
    CONTROLS
        .try_with(|controls| {
            let mut state = controls.state.lock();
            state.recovery_draw += 1;
            Duration::from_millis(24_000 + mix(controls.seed ^ state.recovery_draw) % 12_001)
        })
        .ok()
}

pub(super) struct CopyGuard {
    controls: Arc<Controls>,
    index: usize,
    bytes: u64,
}

impl Drop for CopyGuard {
    fn drop(&mut self) {
        let mut state = self.controls.state.lock();
        let now = state.epoch.elapsed().as_micros() as u64;
        state.copies[self.index].end_us = now;
        state.active_copies -= 1;
        state.active_bytes -= self.bytes;
    }
}

pub(super) fn record_copy(
    object: &ObjectKey,
    bucket: &BucketName,
    bytes: u64,
) -> Option<CopyGuard> {
    CONTROLS
        .try_with(|controls| {
            let read = object
                .strip_prefix("read-")
                .expect("simulation object")
                .parse()
                .unwrap();
            let replica = bucket
                .strip_prefix("replica-")
                .expect("simulation bucket")
                .parse()
                .unwrap();
            let mut state = controls.state.lock();
            let index = state.copies.len();
            let start_us = state.epoch.elapsed().as_micros() as u64;
            state.active_copies += 1;
            state.peak_copies = state.peak_copies.max(state.active_copies);
            state.active_bytes += bytes;
            state.peak_bytes = state.peak_bytes.max(state.active_bytes);
            state.copies.push(Copy {
                read,
                replica,
                start_us,
                end_us: 0,
            });
            CopyGuard {
                controls: controls.clone(),
                index,
                bytes,
            }
        })
        .ok()
}

#[allow(clippy::too_many_lines)]
async fn simulate(scenario: Scenario, seed: u64, native_reservoir: bool, trace: bool) -> Report {
    scenario.validate().expect("valid scenario");
    let controls = Arc::new(Controls {
        capacity: (!native_reservoir)
            .then_some(scenario.reads() * 2 + scenario.warmup_per_replica as usize + 16),
        seed,
        state: Mutex::new(ControlState {
            epoch: Instant::now(),
            recovery_draw: 0,
            copies: vec![],
            active_copies: 0,
            peak_copies: 0,
            active_bytes: 0,
            peak_bytes: 0,
            completed_reads: 0,
        }),
    });
    CONTROLS
        .scope(controls.clone(), async {
            let scenario = Arc::new(scenario);
            let backend = Backend::new(scenario.clone(), seed);
            let connector = SharedHttpConnector::new(backend.clone());
            let retry = RetryConfig::standard()
                .with_max_attempts(scenario.sdk_attempts)
                .with_initial_backoff(Duration::from_millis(scenario.sdk_backoff_ms))
                .with_max_backoff(Duration::from_millis(scenario.sdk_backoff_ms))
                .with_use_static_exponential_base(true);
            let config = aws_sdk_s3::Config::builder()
                .behavior_version(aws_config::BehaviorVersion::latest())
                .credentials_provider(Credentials::new(
                    "simulation",
                    "simulation",
                    None,
                    None,
                    "simulation",
                ))
                .region(Region::new("us-east-1"))
                .endpoint_url("http://simulation.invalid")
                .force_path_style(true)
                .retry_config(retry)
                .http_client(http_client_fn(move |_, _| connector.clone()))
                .build();
            let downloader = Downloader::new(
                aws_sdk_s3::Client::from_conf(config),
                scenario.limits(),
                Arc::new(Mutex::new(SlidingThroughput::default())),
            )
            .unwrap();
            let buckets = BucketNameSet::new(
                (0..scenario.replicas.len())
                    .map(|index| BucketName::new(format!("replica-{index}")).unwrap()),
            )
            .unwrap();
            for (replica, bucket) in buckets.iter().enumerate() {
                let singleton = BucketNameSet::from(bucket.clone());
                for warm in 0..scenario.warmup_per_replica {
                    let id = WARM_READ + replica as u64 * 1_000_000 + u64::from(warm);
                    let range = 0..scenario.read_request_bytes(seed, id);
                    // Calibration needs to measure even a replica that cannot meet the campaign
                    // deadline.
                    let calibration = downloader
                        .clone()
                        .with_test_limits(super::DownloadLimits {
                            page_timeout: Duration::from_secs(60),
                            bucket_timeout: Duration::from_secs(60),
                            hedge_budget_percent: 0,
                            ..scenario.limits()
                        })
                        .unwrap();
                    calibration
                        .download(
                            &singleton,
                            ObjectKey::new(format!("read-{id}")).unwrap(),
                            &range,
                            &RequestConfig::default(),
                        )
                        .await
                        .expect("healthy calibration read");
                }
            }
            backend.drain().await;
            backend.reset();
            let epoch = backend.state.lock().epoch;
            {
                let mut state = controls.state.lock();
                state.epoch = epoch;
                state.copies.clear();
                state.peak_copies = 0;
                state.peak_bytes = 0;
            }
            let reads = futures::future::join_all((0..scenario.reads()).map(|id| {
                let downloader = &downloader;
                let buckets = &buckets;
                let scenario = &scenario;
                async move {
                    let at_ms = id as u64 * scenario.interval_ms;
                    sleep_until(epoch + Duration::from_millis(at_ms)).await;
                    let object = ObjectKey::new(format!("read-{id}")).unwrap();
                    let config = RequestConfig::default();
                    let body_bytes = scenario.read_body_bytes(seed, id as u64);
                    let range = 0..scenario.read_request_bytes(seed, id as u64);
                    let operation = downloader.download(buckets, object, &range, &config);
                    tokio::pin!(operation);
                    let result = if let Some(cancel_ms) = scenario.cancel_read_after_ms {
                        tokio::select! {
                            biased;
                            () = tokio::time::sleep(Duration::from_millis(cancel_ms)) => None,
                            result = &mut operation => Some(result),
                        }
                    } else {
                        Some(operation.await)
                    };
                    let end_us = epoch.elapsed().as_micros() as u64;
                    let completion_order = CONTROLS.with(|controls| {
                        let mut state = controls.state.lock();
                        let order = state.completed_reads;
                        state.completed_reads += 1;
                        order
                    });
                    let (error, primary, winner, hedged) = match result {
                        Some(Ok(output)) => {
                            assert_eq!(output.piece.data.len(), body_bytes);
                            (
                                None,
                                Some(output.primary_bucket_idx),
                                Some(output.used_bucket_idx),
                                output.hedged,
                            )
                        }
                        Some(Err(error)) => (
                            Some(report::error_name(&error).to_owned()),
                            None,
                            None,
                            false,
                        ),
                        None => (Some("caller_cancelled".to_owned()), None, None, false),
                    };
                    let mut health = vec![];
                    downloader.observe_bucket_metrics(|bucket, metrics| {
                        health.push(Health {
                            replica: bucket.strip_prefix("replica-").unwrap().parse().unwrap(),
                            deprioritized: metrics.deprioritized,
                            recovery_successes: metrics.recovery_successes,
                            estimates: metrics.into(),
                        });
                    });
                    health.sort_by_key(|health| health.replica);
                    Read {
                        id: id as u64,
                        body_bytes,
                        completion_order,
                        arrival_us: at_ms * 1000,
                        end_us,
                        error,
                        primary,
                        winner,
                        hedged,
                        health,
                    }
                }
            }))
            .await;
            let client_finished_us = epoch.elapsed().as_micros() as u64;
            backend.drain().await;
            let histograms = downloader.simulation_histograms();
            if !native_reservoir {
                assert!(
                    histograms
                        .iter()
                        .all(|(updates, retained)| *updates == *retained as u64),
                    "exact-retention histogram evicted samples"
                );
            }
            let (copies, peak_copies, peak_bytes) = {
                let state = controls.state.lock();
                assert_eq!(state.active_copies, 0);
                (state.copies.clone(), state.peak_copies, state.peak_bytes)
            };
            Report::new(
                (*scenario).clone(),
                seed,
                native_reservoir,
                trace,
                reads,
                copies,
                peak_copies,
                peak_bytes,
                &backend.state.lock(),
                client_finished_us,
                histograms,
            )
        })
        .await
}

fn scenario_named(name: &str) -> Scenario {
    model::scenarios()
        .into_iter()
        .find(|scenario| scenario.name == name)
        .unwrap()
}

#[tokio::test(start_paused = true)]
async fn simulation_guarantees() {
    for name in [
        "s3_express_healthy",
        "s3_standard_cold_cross_region_failover",
        "s3_express_two_zones_fail",
        "s3_express_two_zones_header_stall",
        "s3_express_two_zones_body_stall",
        "s3_standard_cross_region_deadline",
        "s3_standard_replication_lag",
        "s3_standard_internal_errors",
        "s3_express_guarded_recovery",
        "s3_express_delayed_cancellation",
        "s3_express_single_zone_header_stalls",
        "s3_express_single_zone_slow_bodies",
        "s3_express_header_stalls_page_admission",
    ] {
        let mut scenario = scenario_named(name);
        if !matches!(
            name,
            "s3_standard_internal_errors"
                | "s3_express_guarded_recovery"
                | "s3_express_delayed_cancellation"
        ) {
            scenario.duration_ms = 250;
            scenario.faults.iter_mut().for_each(|fault| {
                fault.start_ms = 0;
                fault.end_ms = 250;
            });
        }
        let report = simulate(scenario, 7, false, true).await;
        assert_eq!(report.completed, report.arrivals, "{name}: {report:?}");
        if name == "s3_standard_internal_errors" {
            assert_eq!(report.transport_attempts, report.arrivals * 2);
        }
        if name == "s3_express_guarded_recovery" {
            assert!(
                report
                    .recoveries
                    .iter()
                    .any(|recovery| recovery.replica == 0 && recovery.successes >= 20)
            );
        }
        if name == "s3_express_delayed_cancellation" {
            assert!(report.post_read_service_us >= 40_000);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn simulation_reads_finishing_at_their_tail_are_not_hedged() {
    let scenario = scenario_named("s3_express_healthy_page_admission");
    let report = simulate(scenario, 7, false, false).await;
    assert_eq!(report.completed, report.arrivals, "{report:?}");
    assert_eq!(report.overlapping_reads, 0, "{report:?}");
}

/// Checks that at least `min_rescued` reads were hedged, that each finished within `bound_ms` of
/// the fixture and its length, and that at most 1% of reads took longer unhedged.
async fn assert_stalls_are_rescued(
    scenario: Scenario,
    min_rescued: usize,
    bound_ms: impl Fn(&model::Scenario, usize) -> u64,
) -> Vec<Report> {
    let mut reports = Vec::new();
    for seed in [7, 42, 2026] {
        let late = |read: &Read| read.latency_us() > bound_ms(&scenario, read.body_bytes) * 1000;
        let report = simulate(scenario.clone(), seed, false, true).await;
        assert_eq!(report.completed, report.arrivals, "seed {seed}");
        let (rescued, unhedged): (Vec<_>, Vec<_>) =
            report.reads.iter().flatten().partition(|read| read.hedged);
        assert!(
            rescued.len() >= min_rescued,
            "seed {seed}: {}",
            rescued.len()
        );
        let late_rescues = rescued
            .iter()
            .filter(|read| late(read))
            .map(|read| (read.body_bytes, read.latency_us()))
            .collect::<Vec<_>>();
        assert!(late_rescues.is_empty(), "seed {seed}: {late_rescues:?}");
        let late_unhedged = unhedged.iter().filter(|read| late(read)).count();
        assert!(
            late_unhedged * 100 <= report.arrivals,
            "seed {seed}: {late_unhedged}"
        );
        reports.push(report);
    }
    reports
}

/// As on S3, fixed latency comes before the first byte and bodies stream at a steady rate. A hedge
/// started at the first-byte p99, or at the bounded body prediction for its length, finishes within
/// about two and a half nominal reads of that length. Stalls outnumber 1% of reads, so they must
/// not set hedge delays; only reads the budget leaves unhedged may take longer.
#[tokio::test(start_paused = true)]
async fn simulation_s3_shaped_stalls_are_hedged_at_size_aware_tails() {
    assert_stalls_are_rescued(
        scenario_named("s3_standard_stalls"),
        10,
        |scenario, bytes| scenario.replicas[0].read_ms(bytes) * 5 / 2 + 30,
    )
    .await;
}

/// As on Capacitor, the server reads and verifies the whole chunk before sending headers, so time
/// to first byte grows with chunk length, and the body follows from memory. A stalled read of any
/// length waits for the first-byte p99 of full chunks, then a hedge to another zone, and finishes
/// within three nominal full-chunk reads from the local zone. Sparse startup history can send
/// reads to peers, but must not keep them there: after a minute, at least 90% use the local zone.
#[tokio::test(start_paused = true)]
async fn simulation_capacitor_shaped_stalls_are_hedged_to_another_zone() {
    let mut scenario = scenario_named("capacitor_stalls");
    let bound_ms = |scenario: &Scenario, _| scenario.replicas[0].read_ms(scenario.body_bytes) * 3;
    assert_stalls_are_rescued(scenario.clone(), 5, bound_ms).await;
    scenario.duration_ms = 120_000;
    for fault in &mut scenario.faults {
        fault.end_ms = scenario.duration_ms;
    }
    for report in assert_stalls_are_rescued(scenario, 5, bound_ms).await {
        let settled_reads: Vec<_> = report
            .reads
            .iter()
            .flatten()
            .filter(|read| read.arrival_us >= 60_000_000)
            .collect();
        let local_wins = settled_reads
            .iter()
            .filter(|read| read.winner == Some(0))
            .count();
        assert!(
            local_wins * 10 >= settled_reads.len() * 9,
            "{local_wins}/{}",
            settled_reads.len()
        );
    }
}

/// A connection reset after headers, which the SDK cannot retry, is retried against the same
/// bucket, so a single-bucket read survives it.
#[tokio::test(start_paused = true)]
async fn simulation_body_resets_on_one_bucket_are_retried() {
    let scenario = scenario_named("s3_standard_body_resets");
    let report = simulate(scenario, 7, false, false).await;
    assert_eq!(report.completed, report.arrivals, "{report:?}");
}

#[tokio::test(start_paused = true)]
async fn simulation_reservations_survive_reordering_and_admission_wait() {
    for name in [
        "s3_standard_cross_region_deadline",
        "s3_standard_queued_long_read",
    ] {
        let scenario = scenario_named(name);
        let report = simulate(scenario, 7, false, true).await;
        assert_eq!(report.completed, report.arrivals, "{name}: {report:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn simulation_repeatability_and_cancellation() {
    let mut scenario = scenario_named("s3_express_independent_errors");
    scenario.duration_ms = 500;
    scenario.faults[0].start_ms = 0;
    let first = simulate(scenario.clone(), 42, false, true).await;
    let second = simulate(scenario, 42, false, true).await;
    assert_eq!(
        serde_json::to_value(first).unwrap(),
        serde_json::to_value(second).unwrap()
    );
    let recovery = scenario_named("s3_express_recovery_during_peer_outage");
    let recovery_report = simulate(recovery, 7, false, true).await;
    assert_eq!(
        recovery_report
            .recoveries
            .iter()
            .filter(|recovery| recovery.replica == 0)
            .count(),
        1
    );
    let scenario = scenario_named("s3_express_caller_cancellation");
    let report = simulate(scenario, 7, false, true).await;
    assert_eq!(
        report.errors.get("caller_cancelled"),
        Some(&report.arrivals)
    );
    assert!(report.cancelled_transports >= report.arrivals);
}

#[tokio::test(start_paused = true)]
async fn simulation_rescue_preserves_availability_and_work_under_overload() {
    for (name, completed, attempts) in [
        ("capacitor_overload", 1807, 3100),
        ("s3_express_overload_sdk_retries", 1806, 3620),
    ] {
        let scenario = scenario_named(name);
        let report = simulate(scenario, 7, false, false).await;
        assert!(report.completed >= completed, "{name}: {report:?}");
        assert!(report.transport_attempts <= attempts, "{name}: {report:?}");
        assert!(report.service_us <= 24_000_000, "{name}: {report:?}");
    }
}

#[tokio::test(start_paused = true)]
#[ignore = "explicit simulation campaign; see docs/simulation/README.md"]
async fn campaign() {
    let seeds = std::env::var("CACHEY_SIM_SEEDS").unwrap_or_else(|_| "7,42,2026".to_owned());
    let filter = std::env::var("CACHEY_SIM_SCENARIOS").ok();
    let rates = std::env::var("CACHEY_SIM_RATE_MULTIPLIERS").unwrap_or_else(|_| "1".to_owned());
    let native = std::env::var("CACHEY_SIM_NATIVE_RESERVOIR").is_ok_and(|value| value == "1");
    let trace = std::env::var("CACHEY_SIM_TRACE").is_ok_and(|value| value == "1");
    let mut reports = vec![];
    for scenario in model::scenarios() {
        if filter
            .as_ref()
            .is_some_and(|filter| !filter.split(',').any(|name| name == scenario.name))
        {
            continue;
        }
        for rate in rates.split(',').map(|value| {
            value
                .parse::<u64>()
                .expect("integer arrival rate multiplier")
        }) {
            for seed in seeds
                .split(',')
                .map(|value| value.parse().expect("integer seed"))
            {
                let mut scenario = scenario.clone();
                scenario.interval_ms = scenario
                    .interval_ms
                    .checked_div(rate)
                    .filter(|value| *value > 0)
                    .expect("rate must preserve millisecond interval");
                if let Ok(limit) = std::env::var("CACHEY_SIM_REQUEST_LIMIT") {
                    scenario.max_inflight_requests = limit.parse().expect("request limit");
                }
                let report = simulate(scenario, seed, native, trace).await;
                eprintln!(
                    "{} seed={seed}: {}/{} complete, {} wire attempts, {} us work",
                    report.scenario.name,
                    report.completed,
                    report.arrivals,
                    report.transport_attempts,
                    report.service_us
                );
                reports.push(report);
            }
        }
    }
    assert!(!reports.is_empty(), "scenario filter matched nothing");
    let output =
        std::env::var("CACHEY_SIM_OUTPUT").expect("set CACHEY_SIM_OUTPUT to a report path");
    let revision = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("git revision");
    let dirty = std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .expect("git status");
    let envelope = serde_json::json!({ "schema": 1, "git_revision": String::from_utf8(revision.stdout).unwrap().trim(),
        "working_tree_dirty": !dirty.stdout.is_empty(), "sdk_backoff": "static exponential base, configured cap; actual SDK retries",
        "clock": "paused Tokio, millisecond timers, current-thread scheduler", "reports": reports });
    std::fs::write(output, serde_json::to_vec_pretty(&envelope).unwrap())
        .expect("write campaign report");
}
