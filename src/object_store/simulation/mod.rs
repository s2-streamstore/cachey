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
}

impl Drop for CopyGuard {
    fn drop(&mut self) {
        let mut state = self.controls.state.lock();
        let now = state.epoch.elapsed().as_micros() as u64;
        state.copies[self.index].end_us = now;
        state.active_copies -= 1;
    }
}

pub(super) fn record_copy(object: &ObjectKey, bucket: &BucketName) -> Option<CopyGuard> {
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
            state.copies.push(Copy {
                read,
                replica,
                start_us,
                end_us: 0,
            });
            CopyGuard {
                controls: controls.clone(),
                index,
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
            let range = 0..scenario.request_bytes();
            for (replica, bucket) in buckets.iter().enumerate() {
                let singleton = BucketNameSet::new(std::iter::once(bucket.clone())).unwrap();
                for warm in 0..scenario.warmup_per_replica {
                    let id = WARM_READ + replica as u64 * 1_000_000 + u64::from(warm);
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
            }
            let reads = futures::future::join_all((0..scenario.reads()).map(|id| {
                let downloader = &downloader;
                let buckets = &buckets;
                let scenario = &scenario;
                let range = &range;
                async move {
                    let at_ms = id as u64 * scenario.interval_ms;
                    sleep_until(epoch + Duration::from_millis(at_ms)).await;
                    let object = ObjectKey::new(format!("read-{id}")).unwrap();
                    let config = RequestConfig::default();
                    let operation = downloader.download(buckets, object, range, &config);
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
                            assert_eq!(output.piece.data.len(), scenario.body_bytes);
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
                        });
                    });
                    health.sort_by_key(|health| health.replica);
                    Read {
                        id: id as u64,
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
            let (copies, peak_copies) = {
                let state = controls.state.lock();
                assert_eq!(state.active_copies, 0);
                (state.copies.clone(), state.peak_copies)
            };
            Report::new(
                (*scenario).clone(),
                seed,
                native_reservoir,
                trace,
                reads,
                copies,
                peak_copies,
                &backend.state.lock(),
                client_finished_us,
                histograms,
            )
        })
        .await
}

#[tokio::test(start_paused = true)]
async fn simulation_guarantees() {
    for name in [
        "healthy_zonal",
        "cold_regional_failover",
        "third_after_errors",
        "third_after_header_stalls",
        "third_after_body_stalls",
        "regional_deadline",
        "missing_objects",
        "sdk_retry",
        "guarded_recovery",
        "delayed_cancellation",
    ] {
        let mut scenario = model::scenarios()
            .into_iter()
            .find(|scenario| scenario.name == name)
            .unwrap();
        if !matches!(
            name,
            "sdk_retry" | "guarded_recovery" | "delayed_cancellation"
        ) {
            scenario.duration_ms = 250;
            scenario.faults.iter_mut().for_each(|fault| {
                fault.start_ms = 0;
                fault.end_ms = 250;
            });
        }
        let report = simulate(scenario, 7, false, true).await;
        assert_eq!(report.completed, report.arrivals, "{name}: {report:?}");
        if name == "sdk_retry" {
            assert_eq!(report.transport_attempts, report.arrivals * 2);
        }
        if name == "guarded_recovery" {
            assert!(
                report
                    .recoveries
                    .iter()
                    .any(|recovery| recovery.replica == 0 && recovery.successes >= 20)
            );
        }
        if name == "delayed_cancellation" {
            assert!(report.post_read_service_us >= 40_000);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn simulation_reservations_survive_reordering_and_admission_wait() {
    for name in ["regional_deadline", "queued_long_read"] {
        let scenario = model::scenarios()
            .into_iter()
            .find(|scenario| scenario.name == name)
            .unwrap();
        let report = simulate(scenario, 7, false, true).await;
        assert_eq!(report.completed, report.arrivals, "{name}: {report:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn simulation_repeatability_and_cancellation() {
    let mut scenario = model::scenarios()
        .into_iter()
        .find(|scenario| scenario.name == "independent_errors")
        .unwrap();
    scenario.duration_ms = 500;
    scenario.faults[0].start_ms = 0;
    let first = simulate(scenario.clone(), 42, false, true).await;
    let second = simulate(scenario, 42, false, true).await;
    assert_eq!(
        serde_json::to_value(first).unwrap(),
        serde_json::to_value(second).unwrap()
    );
    let recovery = model::scenarios()
        .into_iter()
        .find(|scenario| scenario.name == "recovery_during_peer_outage")
        .unwrap();
    let recovery_report = simulate(recovery, 7, false, true).await;
    assert_eq!(
        recovery_report
            .recoveries
            .iter()
            .filter(|recovery| recovery.replica == 0)
            .count(),
        1
    );
    let scenario = model::scenarios()
        .into_iter()
        .find(|scenario| scenario.name == "caller_cancellation")
        .unwrap();
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
        ("finite_capacity", 1806, 3100),
        ("finite_capacity_sdk_retries", 1807, 3550),
    ] {
        let scenario = model::scenarios()
            .into_iter()
            .find(|scenario| scenario.name == name)
            .unwrap();
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
