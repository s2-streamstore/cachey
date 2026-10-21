# Replica read simulation

The harness runs the real downloader and AWS SDK against an in-process backend
model using paused Tokio time. It makes no external requests.

## Running

Regression checks run with the normal test suite:

```sh
cargo test --locked --lib object_store::simulation
```

Run the full campaign:

```sh
CACHEY_SIM_OUTPUT=/tmp/cachey-simulation.json \
  cargo test --locked --lib object_store::simulation::campaign -- --ignored --nocapture
```

| Environment variable | Value / default |
|----------------------|-----------------|
| `CACHEY_SIM_OUTPUT` | Required output JSON path |
| `CACHEY_SIM_SCENARIOS` | Comma-separated fixture names; default all |
| `CACHEY_SIM_SEEDS` | Comma-separated seeds; default `7,42,2026` |
| `CACHEY_SIM_RATE_MULTIPLIERS` | Comma-separated positive integer load multipliers; default `1` |
| `CACHEY_SIM_REQUEST_LIMIT` | Override global active-copy limit |
| `CACHEY_SIM_NATIVE_RESERVOIR` | `1` uses production sampling for all latency histograms |
| `CACHEY_SIM_TRACE` | `1` includes every read, admitted copy, and wire attempt |

[Fixtures](../../tests/scenarios/replica_reads.json) inherit omitted fields from
[model defaults](../../src/object_store/simulation/model.rs). Reports include the
resolved configuration, seed, sampling mode, Git revision, and dirty-tree flag.

## Model

Each replica has service slots and a finite queue. Queue overflow returns
`SlowDown`. Cancellation may leave backend work running for a configurable delay.
Every run checks that workers release their capacity.

Faults cover errors, missing objects, slow service, and stalls. Warmup reads
establish latency history before measurement; zero warmup exercises cold starts.
Admission reservations and response lengths are independent, so mixed-size reads
can still reserve a full page.

Seeds control faults, jitter, and recovery probes. SDK retries use the real
classifier and quota with deterministic backoff. All latency histograms retain
observations by default, including each body size band. Production sampling is
unseeded, so repeat those runs when comparing small differences.

## Fixtures

Fixtures share their backend family's latency profile. The S3 model streams bodies
after headers. The Capacitor model reads and verifies the range before headers,
then serves it from memory.

| Profile | Replicas | Before headers | After headers |
|---------|----------|----------------|---------------|
| S3 Standard | one bucket in the region | 25 ms | 11 ms per MiB |
| S3 Standard, cross-region | the local bucket and replicas in two other regions | 25, 85, 165 ms | 11 ms per MiB |
| S3 Express One Zone | a directory bucket per AZ, the first local | 3, 4, 5 ms | 4 ms per MiB |
| Capacitor | a deployment per AZ, the first local, chunks up to 4 MiB | 1, 2, 2 ms plus 1 ms per MiB | 1 ms per MiB |

These profiles are illustrative and should be calibrated against bucket latency
gauges.

| Scenario | S3 Standard | S3 Express One Zone | Capacitor |
|----------|-------------|---------------------|-----------|
| Healthy | `s3_standard_healthy`, `s3_standard_cross_region_healthy` | `s3_express_healthy`, `s3_express_healthy_jitter`, `s3_express_healthy_page_admission` | `capacitor_healthy` |
| Stalls | `s3_standard_stalls`, `s3_standard_cross_region_deadline`, `s3_standard_cross_region_deadline_page_admission` | `s3_express_two_zones_header_stall`, `s3_express_two_zones_header_stall_page_admission`, `s3_express_two_zones_body_stall`, `s3_express_header_stalls_page_admission`, `s3_express_local_slowdown` | `capacitor_stalls` |
| One bucket or zone | `s3_standard_stalls`, `s3_standard_body_resets` | `s3_express_single_zone_header_stalls`, `s3_express_single_zone_slow_bodies` | |
| Errors and outages | `s3_standard_internal_errors`, `s3_standard_distant_region_cannot_fit` | `s3_express_independent_errors`, `s3_express_independent_errors_large`, `s3_express_correlated_errors`, `s3_express_two_zones_fail`, `s3_express_sdk_retry_with_hedges` | `capacitor_zone_loss` |
| Missing objects | `s3_standard_replication_lag` | | `capacitor_missing_chunks` |
| Cold start and recovery | `s3_standard_cold_cross_region_failover` | `s3_express_guarded_recovery`, `s3_express_recovery_during_peer_outage` | |
| Throttling and overload | `s3_standard_slowdown` | `s3_express_overload_sdk_retries` | `capacitor_overload`, `capacitor_overload_delayed_cancel` |
| Admission | `s3_standard_admission_saturation`, `s3_standard_queued_long_read` | `s3_express_stalled_admission` | |
| Cancellation | | `s3_express_caller_cancellation`, `s3_express_delayed_cancellation` | `capacitor_cancel_storm`, `capacitor_cancel_immediate` |

The `synthetic_fixed_body_time_healthy` and
`synthetic_fixed_body_time_slow_bodies` fixtures also put a fixed cost after headers.
They check that mixed-size reads learn that cost and preserve hedge credits for stalls.

## Interpreting results

Compare revisions with the same harness, fixtures, and controls. Assess completion
first, then latency and extra backend work:

- Completion includes every scheduled arrival, including admission failures and
  caller cancellations.
- Latency is reported for successful reads and for all arrivals. Mixed-size
  fixtures also separate results by response size.
- Backend work includes occupied service time and work that continues after a read
  ends. Reports distinguish admitted copies, SDK invocations, and wire attempts.

The model omits connection setup, network contention, and interactions among cachey
instances. Millisecond timers distort fast-body latency and favor completed
responses over hedges on the same tick. Results describe these fixtures, not
production latency.

See the [reference results](REFERENCE.md) for pinned comparisons and measured limits.
