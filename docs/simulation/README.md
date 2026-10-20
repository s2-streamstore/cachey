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
| `CACHEY_SIM_RATE_MULTIPLIERS` | Comma-separated positive integers; default `1`. Divides arrival spacing, truncated to milliseconds; spacing must remain positive. |
| `CACHEY_SIM_REQUEST_LIMIT` | Override global active-copy limit |
| `CACHEY_SIM_NATIVE_RESERVOIR` | `1` uses the production 1,028-sample reservoir |
| `CACHEY_SIM_TRACE` | `1` includes every read, admitted copy, and wire attempt |

[Fixtures](../../tests/scenarios/replica_reads.json) inherit omitted fields from
[model defaults](../../src/object_store/simulation/model.rs). Reports include the
resolved configuration, seed, sampling mode, Git revision, and dirty-tree flag.

## Model

Each replica has service slots and a finite FIFO queue. Queue overflow returns
`SlowDown`; accepted work occupies a slot through headers and body generation.
Cancellation frees backend capacity after the configured delay or normal completion,
whichever comes first. Every run drains workers and checks that capacity is released.

Faults cover errors, missing objects, slow service, and stalled headers or bodies.
Fault timing uses read arrival by default; `clock_at_arrival=false` evaluates each
wire attempt instead. Healthy downloads calibrate replicas before measurement;
a run with zero warmup reads exercises cold starts. `request_bytes` controls admission
reservations independently of `body_bytes`; page-admission fixtures reserve 16 MiB.
`short_body` gives a seeded share of reads a shorter response: the last page of an
object, or an exact-range read when `request_bytes` is unset. With `max_bytes`, short
responses spread uniformly from `bytes` up to that length, as the last pages of objects
of varied sizes do. A replica's `body_ms_per_mib` adds body time in proportion to the
response length.

Seeds control faults, latency jitter, and recovery probes. SDK retries use the real
classifier and quota with deterministic backoff. The default histogram reservoir
retains all observations, but its priorities remain unseeded. Production-reservoir
comparisons are statistical; repeat runs when investigating small differences.

## Fixtures

Each fixture models one backend, named by its prefix, and uses that backend's latency
profile, so fixtures within a family differ only in faults, limits, and load. On every
backend a small body arrives with its headers. S3 then streams the body, so its time
grows with length after headers; Capacitor reads and verifies the whole range before
sending headers and sends the body from memory, so a replica's `headers_ms_per_mib`
adds its time before headers instead.

| Profile | Replicas | Before headers | After headers |
|---------|----------|----------------|---------------|
| S3 Standard | one bucket in the region | 25 ms | 11 ms per MiB |
| S3 Standard, cross-region | the local bucket and replicas in two other regions | 25, 85, 165 ms | 11 ms per MiB |
| S3 Express One Zone | a directory bucket per AZ, the first local | 3, 4, 5 ms | 4 ms per MiB |
| Capacitor | a deployment per AZ, the first local, chunks up to 4 MiB | 1, 2, 2 ms plus 1 ms per MiB | 1 ms per MiB |

AWS describes S3 Standard medians for small requests in the tens of milliseconds, and
earlier guidance planned on 85–90 MB/s per request. S3 Express One Zone promises
single-digit-millisecond first bytes; crossing an AZ adds about a millisecond, and its
time per MiB is an assumption. Capacitor's figures follow its p99 goals of 5 ms under
1 MiB and 10 ms up to 4 MiB. Calibrate profiles against the bucket latency gauges.

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

`synthetic_fixed_body_time_slow_bodies` alone puts fixed time after headers, which no
backend above does.

## Interpreting results

Completion includes **every scheduled arrival**, including admission failures and
caller cancellations. Reports separate successful latency from all-arrival latency,
and admitted copies from SDK invocations and wire attempts. Service work measures
occupied slot time; post-read work is the portion after the originating read finishes.
Recovery timestamps are sampled when reads complete. Fixtures with short bodies also
report successful latency for full and short responses.

The model omits connection setup, network contention, and interactions among cachey
instances. Millisecond timing and prescribed service distributions limit conclusions
about real S3 or Capacitor latency. Whole-millisecond timing makes responses land
exactly on a learned tail far more often than real ones do. Each server fixes its
response schedule when it starts serving, and the reading task delivers headers and
body chunks on its own timers, so a response landing on the tail reaches the
downloader before a hedge timer firing on the same tick, and such ties do not spend
the budget.

Compare revisions with the same harness, fixtures, and controls; retain patches for
dirty trees. Assess completion first, then latency and extra backend work. See the
[reference results](REFERENCE.md) for pinned revisions and measured limits.
