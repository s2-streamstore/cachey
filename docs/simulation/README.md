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

Seeds control faults, latency jitter, and recovery probes. SDK retries use the real
classifier and quota with deterministic backoff. The default histogram reservoir
retains all observations, but its priorities remain unseeded. Production-reservoir
comparisons are statistical; repeat runs when investigating small differences.

## Interpreting results

Completion includes **every scheduled arrival**, including admission failures and
caller cancellations. Reports separate successful latency from all-arrival latency,
and admitted copies from SDK invocations and wire attempts. Service work measures
occupied slot time; post-read work is the portion after the originating read finishes.
Recovery timestamps are sampled when reads complete.

The model omits connection setup, network contention, and interactions among cachey
instances. Millisecond timing and prescribed service distributions limit conclusions
about real S3 or Capacitor latency.

Compare revisions with the same harness, fixtures, and controls; retain patches for
dirty trees. Assess completion first, then latency and extra backend work. See the
[reference results](REFERENCE.md) for pinned revisions and measured limits.
