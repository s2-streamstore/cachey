# Replica read simulation

The harness runs the real `Downloader`, bucket statistics, admission, cancellation,
and AWS SDK against an in-process HTTP connector. Only backend service and faults
are modeled. It neither contacts S3 nor runs production experiments.

## Running

Fast regression checks run with the normal test suite:

```sh
cargo test --locked --lib object_store::simulation
```

Run the explicit campaign (virtual time, normally a few minutes of wall time):

```sh
CACHEY_SIM_OUTPUT=/tmp/cachey-simulation.json \
  cargo test --locked --lib object_store::simulation::campaign -- --ignored --nocapture
```

Controls:

- `CACHEY_SIM_SCENARIOS`: comma-separated fixture names; default all.
- `CACHEY_SIM_SEEDS`: comma-separated integer seeds; default `7,42,2026`.
- `CACHEY_SIM_RATE_MULTIPLIERS`: comma-separated positive integers; default `1`.
  Multiplies offered load by dividing arrival spacing, rounded to milliseconds.
- `CACHEY_SIM_REQUEST_LIMIT`: override the global active-copy limit.
- `CACHEY_SIM_NATIVE_RESERVOIR=1`: use the production 1,028-sample reservoir.
- `CACHEY_SIM_TRACE=1`: include every logical read, admitted copy, and wire attempt.
- `CACHEY_SIM_OUTPUT`: required output JSON path.

For example, a load sweep with production histogram sampling:

```sh
CACHEY_SIM_SCENARIOS=healthy_jitter,finite_capacity,finite_capacity_delayed_cancel \
CACHEY_SIM_RATE_MULTIPLIERS=1,2,4 CACHEY_SIM_SEEDS=7,42,2026 \
CACHEY_SIM_NATIVE_RESERVOIR=1 CACHEY_SIM_OUTPUT=/tmp/cachey-sweep.json \
  cargo test --locked --lib object_store::simulation::campaign -- --ignored --nocapture
```

Fixtures are in `tests/scenarios/replica_reads.json`; omitted fields use the defaults
in `src/object_store/simulation/model.rs`. Every result embeds the fully resolved
configuration, seed, sampling mode, Git revision, and dirty-tree flag. Retain the
patch as well as the report when running from a dirty tree.

## What is controlled

All arrivals are scheduled against paused Tokio time in a current-thread runtime.
Futures share a task so test-only task-local hooks reach the real statistics code.
Each destination is calibrated with real, healthy singleton downloads; calibration
uses a generous deadline and is excluded from arrival/work counters. No synthetic
latency or health is inserted. Calibration can be disabled with zero warmup reads.

Fault draws are keyed by seed, logical read, destination, invocation ordinal, SDK
attempt, and fault index. Fully correlated faults omit destination and attempt.
The SDK's random invocation UUID is used only to recognize retries, never as a
random seed or report identifier. By default, a fault belongs to the read's arrival
cohort. `clock_at_arrival=false` instead evaluates the fault interval when the wire
attempt starts; this captures actual outage/recovery transitions. Such changes in
exposure between revisions are causal and should not be mistaken for different RNG
inputs. Latency jitter uses separate keyed draws.

SDK backoff uses the SDK's `test-util` static exponential base, with the real retry
classifier, retry quota, configured attempt limit, and backoff cap. Recovery jitter
uses a seeded stream with the production 24–36 second distribution. Its consumption
can change when routing decisions change.

The default histogram mode enlarges the real reservoir enough to retain every
sample. A final audit verifies retained observations equal update count. This removes
random eviction without substituting a statistics implementation. The crate still
uses an internal unseeded RNG for priorities; floating-point ordering at quantile
boundaries is not a universal bitwise determinism guarantee. A repeated-run test
compares integer outcomes and complete traces for a fixed regression workload.
Production-reservoir runs retain the real sampling behavior and are statistical
comparisons, even with a fixed scenario seed. Record repeated runs when studying
small differences. The simulation does not control signing timestamps or SDK UUIDs;
neither is consulted by the backend model.

## Backend and measurements

Each replica has service slots and a finite FIFO waiting queue. A full queue returns
`SlowDown` after a modeled 1 ms rejection delay. Accepted work holds its service slot
through headers and body generation. Bodies arrive in bounded-channel chunks and
are validated by the real downloader. Faults cover service errors, missing objects,
body errors, slow service, and stalled headers or bodies.

Dropping a client future signals server cancellation. The worker continues until
its normal completion or the configured cancellation delay, whichever is earlier.
That includes queued work; canceled work can still acquire a service slot during
the delay. All workers are drained and client/server counters must return to zero.
No connection pool, DNS/TLS, retransmission, shared network bandwidth, fleet of cachey
processes, or adaptive upstream service is modeled. Millisecond scheduling cannot
resolve Capacitor's potential submillisecond differences. Input latencies are
prescribed full-request service distributions, not S3 benchmark measurements.

Completion uses **every scheduled arrival** as the denominator, including admission
failures, deadline expiration, missing data, overload rejection, and caller
cancellation. Reports separate successful latency from latency of all arrivals,
logical copy operations from SDK invocations and transport attempts, and requested,
produced, delivered, and useful body bytes. Service work is occupied service-slot time;
it excludes queued time and the modeled rejection delay, and is not CPU cost.
Post-read work sums service time after each originating read finished, including
canceled losers. Queue/concurrency peaks and observed health restorations are
reported. Completion sequence numbers preserve observation order when reads finish at
the same virtual timestamp. Recovery timestamps are sampled at logical read completion, not continuous
health-transition instrumentation. Zero-duration copies can occur at equal-time
completion/rescue boundaries; their transport attempt still counts.

## Comparing changes

Use the same harness and fixtures on both revisions, never a second implementation
of the routing policy. For a before/after fix, commit the harness first, record that
revision, apply the production fix, and rerun with identical environment variables.
Use separate worktrees for older revisions. If they predate the harness, port only
the connector, fixtures, test-only observation hooks, and dev dependency; review that
patch to ensure it does not include the policy under test. Record both the base
revision and harness patch. Do not overwrite newer fixtures with historical ones.

Compare all-arrival completion first, then latency, attempt amplification, occupied
service time, bytes, concurrency, and recovery. Separate deterministic regression
assertions from statistical rates. Independent 10% copy errors suggest a 0.1% floor
when three copies fit, while fully correlated 10% faults cannot be repaired by
replica choice. Those are model expectations, not an availability guarantee.

The retained counterexamples deliberately include a healthy replica beyond the
page deadline, insufficient admission capacity for legitimate long bodies, and
stalled primaries occupying every admission slot. A strict resource bound cannot
promise successful failover under arbitrary load. Consult `REFERENCE.md` for the
measured results and the specific implementation regressions found by this harness.
