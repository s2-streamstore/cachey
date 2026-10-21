# Replica reads

## Selection and recovery

Replica selection considers the remaining page deadline, backend health, and recent
request latency. The first client-supplied bucket gets a locality preference. Every
supplied bucket remains eligible for fallback.

Backend failures immediately reduce a bucket's preference. Missing objects, invalid
ranges, cancellation, and admission failures do not affect health. Successful reads
restore preference over time. Recovery probes let a sidelined bucket prove itself
while reserving admission for a working alternative.

Stalls and canceled reads can raise routing latency above the measured mean. The
excess decays with a 5-second half-life. A bucket without a measured baseline keeps
its penalty until it provides new evidence.

## Latency model

Every validated backend request contributes observations of total latency, time to
headers, and body time. These observations have a 15-second half-life, balancing
sample retention against adaptation to changing conditions. Failed and canceled
copies do not contribute completed samples.

Before headers arrive, early hedges use first-byte observations. After headers,
they use body observations for the announced response length. Both hedge delays
use p99 capped at 1.5 times the median, with a 1 ms median floor, so occasional
completed stalls do not set future hedge delays. Health classification and retry
feasibility use uncapped predictions; changing the hedge policy does not change
those decisions.

Latency history is shared across objects and callers within each bucket. Body time
uses two continuous lines, one for the median and one for p99:

```text
body time = fixed cost + response bytes × transfer cost per byte
```

Both coefficients are nonnegative. Quantile regression fits the lines directly to
recent body durations, so fixed overhead is learned rather than assumed to scale
with bytes. A small read's delay does not become a multiplier on every larger read.
Predictions have a 1 ms timing floor, and the tail never falls below the median.

Power-of-two sampling strata preserve rare transfers when tiny reads are frequent.
Their observations train the same two lines; there are no independent size-band
estimates or adaptive groups. Decayed observation counts restore each stratum's
weight after reservoir sampling. Snapshot refreshes run once per second and when
the observation count doubles, with at most 32 fitting iterations per line. Each
reader keeps a constant-size snapshot.

With only one observed size, fixed overhead and transfer cost cannot be separated;
the fit starts with a 1 ms allowance and learns time per byte. Diverse sizes let it
learn the fixed cost too. A flat or buffered-only median cannot establish throughput
beyond the largest retained size. Deadline rescue remains available when an early
hedge has no estimate. Sparse samples do not establish a precise p99, and the model
assumes body time is approximately affine in size within each bucket.

## Deadlines and hedging

The page deadline includes admission, SDK retries, and body validation. All copies
to one bucket share a deadline starting at its first admission. Up to three copies
can run per page. The first validated result wins and cancels the others.

Early hedges use one SDK attempt and skip overloaded buckets or unavailable
admission. They prefer an untried bucket but can also target the primary's bucket.
Single-bucket reads follow the same rules. A failed body can be retried once when
no other copy or untried bucket remains, subject to the deadline and retry budget.

Hedges and body retries have separate budgets funded by successful reads. The
default hedge budget allows one startup hedge and one per 20 successful pages.
Setting it to zero leaves fallback and deadline rescue enabled.

## Limits

| Server option | Default |
|---------------|---------|
| `--bucket-timeout-ms` | 5,000 ms per bucket, per page |
| `--page-timeout-ms` | 10,000 ms per page |
| `--max-download-memory` | 1 GiB of reserved body bytes |
| `--hedge-budget-percent` | 5% |

Limits are shared by downloader clones. Server reads reserve 16 MiB each, admitting
64 copies by default. This reservation is separate from cache capacity and does
not cap total process memory. Ordinary copies can wait for admission within the
page deadline.

Rust callers configure `DownloadLimits` through `Downloader::new` or
`ServiceConfig::download_limits`. The default active-copy limit is 1,024.

## Errors

Admission failure or overload returns HTTP 503; timeouts return 504. When several
buckets fail, backend errors take precedence over timeouts, admission failures, and
missing objects. This prevents a partial miss from hiding an unavailable replica.
Errors after response streaming starts terminate the body.

An unsatisfied range permits fallback because replicas may disagree about object
size. If that remains the final error, it carries the largest size observed in
unsatisfied-range responses. Empty or reversed request ranges are rejected before
contacting a backend.

## Metrics

| Measurement | Meaning |
|-------------|---------|
| `cachey_bucket_latency_{mean,p99}_seconds` | Request start through validated body, including SDK retries |
| `cachey_bucket_first_byte_{mean,p99}_seconds` | Request start through successful headers |
| `cachey_bucket_body_model_bytes` | Largest response size retained by the body model |
| `cachey_bucket_body_seconds_per_byte` | Transfer slope of the uncapped p99 body-time model, excluding its learned fixed cost |
| `cachey_bucket_error_ratio` / `cachey_bucket_consecutive_failures` | Backend health outcomes |
| `cachey_bucket_deprioritized` | Reduced bucket preference |
| `cachey_page_download_latency_seconds` | Successful page latency, including admission and fallback |
| `cachey_first_chunk_latency_seconds` | Handler time to first chunk, including cache lookup and coalescing |

Bucket latency gauges are absent until measured. Buffered-only body history reports
zero. Latencies exclude client body transmission.

`DownloadOutput` identifies the initial and winning buckets. `hedged` means copies
overlapped, including rescue and recovery probes. The page `fallback` metric counts
wins from another bucket.

`C0-Status` exposes the serving bucket and cache status. The first page uses a
response header. Subsequent pages use HTTP/2 trailers; HTTP/1.1 reports only the
first page.

See the [simulation harness](simulation/README.md) and
[reference results](simulation/REFERENCE.md).
