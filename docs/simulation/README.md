# Replica read simulation

The harness tests the downloader and AWS SDK against a simulated backend using
paused Tokio time.

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

Replicas have limited service slots and queues. Queue overflow returns `SlowDown`;
cancellation can leave work running for a configurable delay.

A replica with `hot_mib_ms` models S3 Standard's cache: an object is cold once its age
in milliseconds times its size in MiB exceeds it, and a cold read adds
`cold_headers_ms` before headers. Every copy of a cold read waits for the first
copy's fetch, so a hedge gets headers no sooner. Fixtures with such replicas set
`object_ages`, sampled log-uniformly per read.

Fixtures cover errors, missing objects, stalls, overload, cancellation, and recovery.
Warmup reads establish latency history; zero warmup tests cold starts. Admission
reservations are independent of response size.

Seeds control faults, jitter, and recovery probes. SDK retries use deterministic
backoff. By default, histograms keep every observation until it ages out.
Production sampling is unseeded, so repeat runs when comparing small differences.

## Fixtures

The S3 model streams bodies after headers. The Capacitor model reads the range
before headers, then serves it from memory.

| Profile | Replicas | Before headers | After headers |
|---------|----------|----------------|---------------|
| S3 Standard | one bucket in the region | 25 ms | 11 ms per MiB |
| S3 Standard, cold objects | one bucket in the region, objects 1 s to 1 h old | 27 ms, plus 80 ms when cold | 6 ms per MiB |
| S3 Standard, cross-region | the local bucket and replicas in two other regions | 25, 85, 165 ms | 11 ms per MiB |
| S3 Express One Zone | a directory bucket per AZ, the first local | 3, 4, 5 ms | 4 ms per MiB |
| Capacitor | a deployment per AZ, the first local, chunks up to 4 MiB | 1, 2, 2 ms plus 1 ms per MiB | 1 ms per MiB |

Exact-range fixtures read sizes log-uniformly from 1 KiB to each profile's largest
response, starting cold. A sparse Express fixture reads once per second. Admission
burst fixtures send groups of simultaneous reads to one bucket through eight
admission slots. The cold-object fixtures read 16 KiB to 16 MiB log-uniformly, one
with 1% of reads stalling before headers and 1% slowed.

Measured in us-east-1 from EC2 in 2026, S3 Standard's first byte took about 30 ms
for a warm object and 100–120 ms for a cold one, at any size. An object stayed warm
while its age times its size was under about 50 MiB·s: 16 MiB cooled within 5 s and
256 KiB within 120 s, while 16 KiB stayed warm past 10 minutes. Reading it again
warmed it. A second copy sent within 10 ms of a cold read was just as cold, and later
copies finished no sooner than the first. S3 Express One Zone took 5–8 ms at every
size and age.

## Interpreting results

Compare revisions with the same fixtures and settings. Check completion first,
then latency and extra backend work:

- Count every arrival, including admission failures and cancellations.
- Compare successful-read latency and all-arrival latency, including size breakdowns.
- Check wire attempts and service time, including work after a read ends.

Millisecond timers favor responses over hedges on the same tick.

See the [reference results](REFERENCE.md) for pinned comparisons.
