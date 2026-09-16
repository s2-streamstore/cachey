# Replica reads

## Selection and recovery

Selection favors copies likely to finish within the remaining page deadline, then
health, then recent complete-read latency. The first client-supplied bucket has a
locality preference: its latency is compared with 1.5 times each alternative's.
Every supplied bucket remains eligible for fallback.

A single backend failure immediately deprioritizes a bucket, favoring prompt failover
at the cost of extra cross-zone reads after isolated errors. Missing objects, invalid
client ranges, cancellation, and admission failures do not affect health. Timeouts count
against health when the copy had enough time for its expected latency. The error
fraction decays, but restoring normal preference requires 20 successful operations
started after the latest failure. After 24–36 seconds, a previously competitive
bucket—or one that failed before its first successful read—can receive an exclusive
recovery probe, with admission reserved for a working alternative. Successful probes
can repeat immediately until recovery completes.

## Deadlines and hedging

The page deadline includes admission, SDK retries, and body validation. Each copy
also has a bucket deadline starting at admission. Recoverable errors and rescue
timers start untried replicas; timers reserve execution time for alternatives,
including unmeasured buckets. Multi-bucket reads try each bucket at most once at
the downloader layer, with up to three copies active per page. The first validated
result wins and cancels the others.

Early hedges start at successful-operation p99 latency and use one SDK attempt,
against another bucket when available or the same bucket otherwise. By default,
the shared budget permits one startup hedge and earns another per 20 successful
pages. Setting the budget to zero disables early hedges; fallback and deadline
rescue remain available. When every bucket has recently reported overload, early
hedges stop and extra attempts use a separate success-funded retry budget.

## Limits

| Server option | Default |
|---------------|---------|
| `--bucket-timeout-ms` | 5,000 ms per copy |
| `--page-timeout-ms` | 10,000 ms per page |
| `--max-download-memory` | 1 GiB of reserved body bytes |
| `--hedge-budget-percent` | 5 credits per successful page; a hedge costs 100 |

Limits are shared by downloader clones. Server reads reserve 16 MiB even for small
objects, so the default budget admits 64 copies. This budget is separate from cache
capacity and does not cap total process memory. Early hedges skip unavailable
admission; ordinary copies can wait within the page deadline.

Rust callers configure `DownloadLimits` through `Downloader::new` or
`ServiceConfig::download_limits`, with an additional default limit of 1,024 active
copies. The server requires memory for at least one page.

Admission failure or backend overload returns HTTP 503; timeouts return 504. When a
multi-bucket read fails, backend errors take precedence over timeouts, timeouts over
admission failures, and admission failures over missing objects. The latest error
within the same category is returned. This preserves backend diagnostics and
prevents a partial miss from hiding a timeout or admission failure. Error precedence
is independent of bucket-health scoring and historical latency. Exhausting the
retry budget produces HTTP 503 if the read would otherwise report only missing
objects. Failures after response streaming begins terminate the body.

## Metrics

| Measurement | Meaning |
|-------------|---------|
| `cachey_bucket_latency_mean_seconds` | Mean successful complete-operation duration |
| `cachey_bucket_latency_hedge_seconds` | Successful-operation p99 |
| `cachey_bucket_error_rate` / `cachey_bucket_consecutive_failures` | Backend health outcomes |
| `cachey_bucket_deprioritized` | Reduced bucket preference |
| `cachey_page_download_latency_seconds` | Successful page latency, including admission and fallback |
| `cachey_first_chunk_latency_seconds` | Handler time to first available chunk, including cache lookup and coalescing |

Stalls and canceled reads can raise routing latency estimates without adding
successful histogram samples. A single-bucket hedge win is timed from the original
primary start. Latency histograms exclude failures and client body transmission.

`DownloadOutput` reports the initial and winning buckets. `hedged` means copies
overlapped, including rescue and recovery probes; the page `fallback` metric counts
successes from a bucket other than the initial choice.

HTTP responses expose the serving bucket and cache status through `C0-Status`.
The first page's status is a response header. For multi-page GET responses,
subsequent pages' statuses require HTTP/2 and arrive as trailers after the body.
HTTP/1.1 provides only the first page's status.

See the [simulation harness](simulation/README.md) and
[reference results](simulation/REFERENCE.md).
