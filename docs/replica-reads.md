# Replica reads

## Selection and recovery

Replica selection uses backend health, recent latency, and the remaining deadline.
The first client-supplied bucket gets a locality preference; all buckets remain
eligible for fallback.

Backend failures reduce preference; successful reads and recovery probes restore
it. Missing objects, invalid ranges, cancellation, and admission failures do not
affect health.

Stalls and canceled reads can raise routing latency. The excess above the measured
mean decays with a 5-second half-life. Without a measured baseline, it stays until
new observations arrive.

## Latency model

Each validated response trains the bucket's latency model. Observations have a
15-second half-life. Failed and canceled requests do not train it.

Early hedges use time to headers, then body time for the announced response length.
Each delay is p99 capped at 1.5 times the median, with a 1 ms median floor. Health
and retry decisions use uncapped estimates.

Body time follows two fitted lines, one for the median and one for p99:

```text
body time = fixed cost + response bytes × transfer cost per byte
```

Both coefficients are nonnegative. Predictions have a 1 ms floor, and p99 stays at
or above the median. Samples from all objects and response sizes train the same
model. Sampling by size preserves rare large transfers.
Header, complete-request, and body measurements share one priority-sampling
implementation. Retained samples use inclusion-corrected weights so sampling does
not apply age decay a second time.

Models refresh at most once per second, or sooner when the sample count doubles.
Fits with more than 64 retained samples run on Tokio's blocking pool, with one
pending fit per bucket. Requests keep using the previous model while it runs;
recovery discards any pending result along with the old history. Collecting the
retained samples still briefly holds the bucket's statistics lock.

One observed size cannot separate fixed cost from transfer cost, so the model starts
with a 1 ms fixed allowance. Flat or buffered-only history leaves larger transfer
times unknown; deadline rescue remains available.

## Deadlines and hedging

The page deadline includes admission, SDK retries, and body validation. Copies to
one bucket share a deadline starting at its first admission. Up to three copies can
run per page; the first validated result wins and cancels the others.

Early hedges use one SDK attempt, skip overloaded buckets, and never wait for
admission. They prefer an untried bucket but can use the primary's bucket. A failed
body can be retried once when no other copy or untried bucket remains.
A missing object, unsatisfied range, or invalid object state cancels all remaining
copies to that bucket immediately, releasing their admission capacity.

Hedges and body retries have separate budgets funded by successful reads. The
default allows one startup hedge and one per 20 successful pages. A zero hedge
budget still permits fallback and deadline rescue.

## Limits

| Server option | Default |
|---------------|---------|
| `--bucket-timeout-ms` | 5,000 ms per bucket, per page |
| `--page-timeout-ms` | 10,000 ms per page |
| `--max-download-memory` | 1 GiB of reserved body bytes |
| `--hedge-budget-percent` | 5% |

Server reads reserve 16 MiB each, admitting 64 copies by default. This limit is
separate from cache capacity and total process memory. Copies other than early
hedges can wait for admission within the page deadline.

Rust callers use `DownloadLimits`; the default active-copy limit is 1,024. Downloader
clones share limits.

## Errors

Admission failure or overload returns HTTP 503; timeouts return 504. Backend errors
take precedence over timeouts, admission failures, and missing objects. Errors
after streaming starts terminate the body.

An unsatisfied range permits fallback. If it is the final error, it reports the
largest size from those responses. Empty or reversed ranges are rejected before
contacting a backend.

## Metrics

| Measurement | Meaning |
|-------------|---------|
| `cachey_bucket_latency_{mean,p99}_seconds` | Request start through validated body, including SDK retries |
| `cachey_bucket_first_byte_{mean,p99}_seconds` | Request start through successful headers |
| `cachey_bucket_body_model_bytes` | Largest response size retained by the body model |
| `cachey_bucket_body_seconds_per_byte` | p99 transfer cost per byte, excluding fixed cost |
| `cachey_bucket_error_ratio` / `cachey_bucket_consecutive_failures` | Backend health outcomes |
| `cachey_bucket_deprioritized` | Reduced bucket preference |
| `cachey_page_download_latency_seconds` | Successful page latency, including admission and fallback |
| `cachey_first_chunk_latency_seconds` | Handler time to first chunk, including cache lookup and coalescing |

Bucket latency gauges are absent until measured. Buffered-only history reports zero
transfer cost. Latencies exclude transmission to the client.

`DownloadOutput` identifies the initial and winning buckets. `hedged` means copies
overlapped, including rescue and recovery probes; `fallback` counts wins from
another bucket.

`C0-Status` exposes the serving bucket and cache status. The first page uses a
response header. Subsequent pages use HTTP/2 trailers; HTTP/1.1 reports only the
first page.

See the [simulation harness](simulation/README.md) and
[reference results](simulation/REFERENCE.md).
