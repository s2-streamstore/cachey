# Replica reads

## Selection and recovery

Selection favors copies likely to finish within the remaining page deadline, then
health, then recent request latency. The first client-supplied bucket has a locality
preference: its latency is compared with 1.5 times each alternative's. Every supplied
bucket remains eligible for fallback.

A bucket's request latency for selection averages its measured requests, and copies
left unmeasured, such as stalls or canceled reads, can raise it. A rise above the
bucket's measured mean decays with a 5-second half-life, so a bucket that lost
preference to one outlier regains it without first receiving reads. A bucket never
measured keeps its rise.

## Latency model

A response's length is unknown until its headers arrive, since a requested range may
extend past the end of the object. Each validated backend request is measured in two
phases: time to first byte, from starting the request to successful response headers
including SDK retries, and body time after headers. Decisions made before headers
arrive—selection, rescue reservations, and stall detection—use complete request latency
across the lengths a bucket actually served. Hedges and timeouts use the phases.
Request, first-byte, and body observations decay with a 15-second half-life, retaining
evidence across bursts independently of the faster routing-penalty decay.

Body time uses one continuous model per bucket:

```
predicted_time(q, bytes) = 1 ms + bytes × transfer_slope(q)
body_tail(bytes) = min(predicted_time(p99, bytes), 1.5 × predicted_time(p50, bytes))
```

The 1 ms allowance covers timer granularity and scheduling after headers. Bodies
long enough to time teach `(body_time − 1 ms) / body_bytes`, weighted by their bytes
and decayed with a 15-second half-life. All sizes share the same histogram; there is no
size boundary. The median bound limits isolated stalls without discarding their
measurements. Sustained changes move both quantiles. First-byte timing stays at p99,
since it includes server work that can legitimately vary with response length.

Bodies completed within the allowance cannot establish a transfer rate. They contribute
`1 ms / body_bytes` as a conservative upper bound to the median, but do not enter the
p99 transfer-rate distribution. Thus many tiny buffered reads cannot drown a rare
measurable transfer or imply infinite throughput. Until any transfer is measured,
only lengths up to the largest buffered body observed get the 1 ms allowance; larger
bodies remain unmeasured. Empty bodies teach neither a rate nor a bound.

A sparse histogram combines nearby rates, preserving byte weights instead of sampling
requests. Each bin uses its slowest rate, so aggregation can raise a quantile by less
than 0.8%. Storage is bounded by rate range and precision, not request count. Estimates
refresh at most once per second after the first measurement. Bytes determine influence,
not a count of independent samples or a guarantee about request completion percentiles.

Every validated completion teaches the model, including a slow primary whose hedge
was denied by the budget, admission, or overload. Cancellation leaves no complete
sample. This lets a sustained slowdown change the estimates even without spare
capacity. A successful recovery probe starts the bucket's history over from its
measurements. Sparse histories remain conservative: a lone measurable slow transfer
can set the prediction until there is more evidence.

A backend `InvalidRange` (HTTP 416) response also permits fallback: an empty or
shorter copy in one bucket does not establish the object size in another. If no
replica serves the range, the read fails using the error precedence described below.
Empty or reversed request ranges are rejected before contacting any bucket.

A single backend failure immediately deprioritizes a bucket, favoring prompt failover
at the cost of extra cross-zone reads after isolated errors. Missing objects, invalid
or unsatisfied ranges, cancellation, and admission failures do not affect health.
Timeouts count against health only after the copy had time for a normal request.
Before headers, the complete request p99 is the floor, since the response length is
unknown. Once headers announce a measured body length, the allowance is the first-byte
p99 plus the bounded body prediction for that length. Large historical reads therefore
do not prevent a small stalled body from counting as a timeout. An unmeasured phase gets
half the copy's share of the page deadline, capped at half the bucket deadline, and
keeps the complete request p99 as a floor. The error fraction decays, but
restoring normal preference requires 20 successful operations started after the latest
failure. After 24–36 seconds, a previously competitive bucket—or one that failed before
its first successful read—can receive an exclusive recovery probe, with admission
reserved for a working alternative. Successful probes can repeat immediately until
recovery completes.

## Deadlines and hedging

The page deadline includes admission, SDK retries, and body validation. Each bucket
also has a deadline, starting when the page's first copy to it is admitted, that its
later copies share, so a single-bucket read ends by its bucket deadline. Recoverable
errors and rescue timers start untried replicas; timers reserve execution time for
alternatives, including unmeasured buckets. Reads try each bucket at most once at the
downloader layer, apart from an early hedge against the primary's bucket once every
other bucket has a copy and one retry of a failed body, with up to three copies active
per page. A later copy to a bucket needs enough time for a fresh request before its
shared deadline: first-byte p99 plus body prediction at that bucket's announced length,
or complete request p99 while that prediction is unknown. Each bucket keeps its own
length, since replicas can disagree. The first validated result wins and cancels the
others.

Early hedges use one SDK attempt, against another bucket when available or the
primary's bucket otherwise, and never against a bucket that recently reported overload.
They start only before the primary's bucket deadline. A hedge is timed in two phases.
Before headers arrive, it starts at the p99 time to first byte after the primary
request starts, excluding admission wait. Once headers announce the body length, it
starts at the bounded body prediction after headers. The first-byte delay is not paid
again. A zero first-byte tail or an unmeasured body is not hedged early. Each phase is one
opportunity, so a hedge that cannot start before headers may still start during the
body.

A single-bucket read hedges against its own bucket under the same rules. Each copy
records its own health outcome, so throttling or a failure in one copy counts even when
its peer succeeds, and a copy that fails transiently waits for its peer. An error that
another copy from the same bucket would repeat, such as a missing object, settles that
bucket: its other copies no longer hold the read open, so the read ends once no copy to
another bucket is in flight or can start.

A body whose transfer fails after its headers, which the SDK cannot retry, is retried
once per read against its bucket when no untried bucket remains and no copy to an
unsettled bucket is in flight. If bodies failed in several buckets, the retry goes to
the preferred one that still has time before its deadline, by the rule above for later
copies. A body whose length disagrees with its `Content-Range` is an invalid response,
which another request would likely repeat, so it is not retried against its bucket.

By default, the shared budget permits one startup hedge and earns another per 20
successful pages. Setting the budget to zero disables early hedges; fallback and
deadline rescue remain available. Body retries, and extra attempts while every bucket
has recently reported overload, draw on a separate retry budget that permits one at
startup and earns another per 10 successful pages.

## Limits

| Server option | Default |
|---------------|---------|
| `--bucket-timeout-ms` | 5,000 ms per bucket, per page |
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
read fails, each bucket contributes one error, and backend errors take precedence over
timeouts, timeouts over admission failures, and admission failures over missing
objects. The latest error within the same category is returned. This preserves backend
diagnostics and prevents a partial miss from hiding a timeout or admission failure.
Within a bucket, an error that its other copies would repeat, such as a missing object,
replaces the bucket's earlier errors and outlasts later ones. Error precedence
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
| `cachey_bucket_body_seconds_per_byte` | Uncapped byte-weighted p99 transfer slope of timed bodies after subtracting the 1 ms allowance; absent until a nonempty body is measured; zero does not establish throughput for larger bodies |
| `cachey_bucket_error_ratio` / `cachey_bucket_consecutive_failures` | Backend health outcomes |
| `cachey_bucket_deprioritized` | Reduced bucket preference |
| `cachey_page_download_latency_seconds` | Successful page latency, including admission and fallback |
| `cachey_first_chunk_latency_seconds` | Handler time to first available chunk, including cache lookup and coalescing |

Bucket latency gauges describe the [latency model](#latency-model): samples come from
each validated backend request, including hedges. The body gauge reports the uncapped
p99 rate of measurable transfers; buffered-only history reports zero. In-flight stalls
and canceled reads can also raise routing latency.
Page latency counts every successful page, and for a hedge win is timed from the
original primary start. Latency histograms exclude client
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
