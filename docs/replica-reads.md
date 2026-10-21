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

Before headers arrive, early hedges use first-byte p99. After headers, the hedge
delay scales with the announced body length. Nearby sizes receive similar delays;
there is no fixed boundary between small and large bodies.

One body model shares evidence across sizes. Observations are weighted by byte
count so that useful transfers remain informative among many tiny reads. Bodies
too fast to time contribute an upper bound on transfer time.

The body hedge delay uses the p99 prediction, capped at 1.5 times the median
prediction. The cap limits the influence of occasional stalls on future hedge
delays. It is a hedge policy, not a guarantee about request p99. When the model
lacks evidence for an early hedge, deadline rescue still applies.

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
| `cachey_bucket_body_seconds_per_byte` | Byte-weighted p99 transfer slope, before the hedge cap |
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
