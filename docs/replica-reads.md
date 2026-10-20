# Replica reads

## Selection and recovery

Selection favors copies likely to finish within the remaining page deadline, then
health, then recent request latency. The first client-supplied bucket has a locality
preference: its latency is compared with 1.5 times each alternative's. Every supplied
bucket remains eligible for fallback.

## Latency model

Pages are requested whole, so a response's length is unknown until its headers arrive.
Each validated backend request is measured in two phases: time to first byte, from
starting the request to successful response headers including SDK retries, and body
time after headers. Decisions made before headers arrive—selection, rescue
reservations, and stall detection—use complete request latency across the lengths a
bucket actually served. Hedges and timeouts use the phases.

Body time is fitted as a fixed time plus a time per byte, so short last pages and full
pages inform one estimate instead of skewing each other. The fit counts errors relative
to body time and weighs a slow body no more than one that took as long as predicted, so
a stall barely moves it. While bodies share nearly one length, the two terms cannot be
told apart, and the fit uses duration alone under 1 MiB or time per byte alone from
1 MiB. The body tail for an announced length is its fitted time scaled by the p99 ratio
of body time to fitted time. Ratios are kept separately under and from 1 MiB, so stalls
that dominate small bodies do not scale up large transfers, and a bucket has no body
tail for a size class it has not served.

Tails and ratios are nearest-rank p99s of samples decayed with a 5-second half-life, so
a long-decayed sample cannot set them; the fit decays at the same rate. Two kinds of
copies are left out, so stalls keep being hedged instead of raising the hedge delay to
their own duration: copies a hedge superseded, and primaries whose hedge the budget
denied that outlasted the time such a hedge would have taken at the bucket's tails. The
budget denies hedges only while others run or recently ran, and those races keep
teaching the tails if the bucket itself slows. A hedge withheld for overload or
admission is no evidence that the primary was an outlier, so the primary is measured.
A successful recovery probe starts the bucket's latency model over from its
measurements, as for a bucket never measured.

A backend `InvalidRange` (HTTP 416) response also permits fallback: an empty or
shorter copy in one bucket does not establish the object size in another. If no
replica serves the range, the read fails using the error precedence described below.
Empty or reversed request ranges are rejected before contacting any bucket.

A single backend failure immediately deprioritizes a bucket, favoring prompt failover
at the cost of extra cross-zone reads after isolated errors. Missing objects, invalid
or unsatisfied ranges, cancellation, and admission failures do not affect health.
Timeouts count against health only when the copy had at least the bucket's complete
request tail and the tails of the phases it reached: response headers, then a body of
the announced length. The complete request tail keeps a caller timeout shorter than a
typical request from counting against the bucket. A phase the bucket has not measured,
including a body in a size class it has not served, is allowed half the copy's share of
the page deadline, capped at half the bucket deadline. The error fraction decays, but
restoring normal preference requires 20 successful operations started after the latest
failure. After 24–36 seconds, a previously competitive bucket—or one that failed before
its first successful read—can receive an exclusive recovery probe, with admission
reserved for a working alternative. Successful probes can repeat immediately until
recovery completes.

## Deadlines and hedging

The page deadline includes admission, SDK retries, and body validation. Each copy
also has a bucket deadline starting at admission. Recoverable errors and rescue
timers start untried replicas; timers reserve execution time for alternatives,
including unmeasured buckets. Multi-bucket reads try each bucket at most once at
the downloader layer, apart from an early hedge against the primary's bucket once
every other bucket has a copy, with up to three copies active per page. The first
validated result wins and cancels the others.

Early hedges use one SDK attempt, against another bucket when available or the
primary's bucket otherwise. They start only before the primary's bucket deadline, and
one against the same bucket shares that deadline, so a single-bucket read ends by its
bucket deadline. A hedge is timed in two phases. Before headers arrive, it starts at the
p99 time to first byte after the primary request starts, excluding admission wait. Once
headers announce the body length, it starts when the body has taken longer than both its
body tail and the p99 time to first byte. A phase whose tail is zero, or a body in a
size class the bucket has not served, is not hedged early. Each phase is one opportunity,
so a hedge that cannot start before headers may still start during the body.

A single-bucket read hedges against its own bucket under the same rules. Each copy
records its own health outcome, so throttling or a failure in one copy counts even when
its peer succeeds, and a copy that fails transiently waits for its peer. An error that
another copy from the same bucket would repeat, such as a missing object, ends the read.

By default, the shared budget permits one startup hedge and earns another per 20
successful pages. Setting the budget to zero disables early hedges; fallback and
deadline rescue remain available. When every bucket has recently reported overload,
early hedges stop and extra attempts use a separate success-funded retry budget.

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

If the selected error is an unsatisfied range, it carries the largest known object
size from all observed unsatisfied-range responses, even across intervening errors
or responses without a size. This is the largest observed size; it does not establish
an authoritative size or replica agreement.

## Metrics

| Measurement | Meaning |
|-------------|---------|
| `cachey_bucket_latency_{mean,p99}_seconds` | Time from starting a request to its validated body, including SDK retries; absent until measured |
| `cachey_bucket_first_byte_{mean,p99}_seconds` | Time from starting a request to successful response headers, including SDK retries; absent until measured |
| `cachey_bucket_body_overhead_seconds` | Fitted time to read a body after its headers, regardless of length; until bodies of different lengths separate it from transfer time, their whole time under 1 MiB and zero otherwise; absent until measured |
| `cachey_bucket_body_throughput_bytes_per_second` | Fitted body throughput beyond the overhead; absent until measured, while bodies arrive too fast to time, or while all share nearly one length under 1 MiB |
| `cachey_bucket_{small,large}_body_p99_ratio` | p99 of body time over its fitted time, for bodies under 1 MiB and of at least 1 MiB; absent until measured |
| `cachey_bucket_error_ratio` / `cachey_bucket_consecutive_failures` | Backend health outcomes |
| `cachey_bucket_deprioritized` | Reduced bucket preference |
| `cachey_page_download_latency_seconds` | Successful page latency, including admission and fallback |
| `cachey_first_chunk_latency_seconds` | Handler time to first available chunk, including cache lookup and coalescing |

Bucket latency gauges describe the [latency model](#latency-model): samples come from
each validated backend request, including hedges, except the copies it leaves out.
Those copies, in-flight stalls, and canceled reads can still raise routing latency.
The body overhead, throughput, and ratio gauges expose the fitted body model for
diagnosis, and may change with it. Page latency counts every successful page, and for a
hedge win is timed from the original primary start. Latency histograms exclude client
body transmission.

`DownloadOutput` reports the initial and winning buckets. `hedged` means copies
overlapped, including rescue and recovery probes; the page `fallback` metric counts
successes from a bucket other than the initial choice.

HTTP responses expose the serving bucket and cache status through `C0-Status`.
The first page's status is a response header. For multi-page GET responses,
subsequent pages' statuses require HTTP/2 and arrive as trailers after the body.
HTTP/1.1 provides only the first page's status.

See the [simulation harness](simulation/README.md) and
[reference results](simulation/REFERENCE.md).
