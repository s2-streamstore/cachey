# Replica reads

## Selection and recovery

Selection favors copies likely to finish within the remaining page deadline, then
health, then recent request latency. The first client-supplied bucket has a locality
preference: its latency is compared with 1.5 times each alternative's. Every supplied
bucket remains eligible for fallback.

A bucket's request latency for selection averages its measured requests, and copies
left unmeasured, such as stalls or canceled reads, can raise it. A rise above the
bucket's measured mean decays with the latency model's 5-second half-life, so a bucket
that lost preference to one outlier regains it without first receiving reads. A bucket
never measured keeps its rise.

## Latency model

A response's length is unknown until its headers arrive, since a requested range may
extend past the end of the object. Each validated backend request is measured in two
phases: time to first byte, from starting the request to successful response headers
including SDK retries, and body time after headers. Decisions made before headers
arrive—selection, rescue reservations, and stall detection—use complete request latency
across the lengths a bucket actually served. Hedges and timeouts use the phases.

Body time is modeled in two size classes. Bodies under 1 MiB are modeled by their
duration, which per-request overhead and stalls can dominate, and longer bodies by their
time per MiB, since transfer time dominates them. The body tail for an announced length
is the p99 duration of small bodies, or the p99 time per MiB of large bodies scaled to
that length, so a stall in a small body never scales up a large transfer. A bucket has no
body tail for a size class it has not served. A fixed delay after headers raises the
time per MiB of the shortest large bodies most, so it makes the tail of longer ones
conservative.

Tails are nearest-rank p99s of samples decayed with a 5-second half-life, so a
long-decayed sample cannot set them. Copies a hedge superseded are left out, and so are
primaries whose hedge came due but did not start, for any reason, that outlasted the
time such a hedge would have taken at the bucket's tails. Stalls therefore keep being
hedged instead of raising the hedge delay to their own duration. A bucket that slows as
a whole still teaches its tails through primaries that finish within that time and
races they win against their hedges. A successful recovery probe starts the bucket's
latency model over from its measurements, as for a bucket never measured.

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

The page deadline includes admission, SDK retries, and body validation. Each bucket
also has a deadline, starting when the page's first copy to it is admitted, that its
later copies share, so a single-bucket read ends by its bucket deadline. Recoverable
errors and rescue timers start untried replicas; timers reserve execution time for
alternatives, including unmeasured buckets. Reads try each bucket at most once at the
downloader layer, apart from an early hedge against the primary's bucket once every
other bucket has a copy and one retry of a failed body, with up to three copies active
per page. Such a later copy to a bucket starts only while more than the bucket's p99
request latency remains before its deadline. The first validated result wins and
cancels the others.

Early hedges use one SDK attempt, against another bucket when available or the
primary's bucket otherwise, and never against a bucket that recently reported overload.
They start only before the primary's bucket deadline. A hedge is timed in two phases.
Before headers arrive, it starts at the p99 time to first byte after the primary
request starts, excluding admission wait. Once headers announce the body length, it
starts when the body has taken longer than both its body tail and the p99 time to first
byte. A phase whose tail is zero, or a body in a size class the bucket has not served,
is not hedged early. Each phase is one opportunity, so a hedge that cannot start before
headers may still start during the body.

A single-bucket read hedges against its own bucket under the same rules. Each copy
records its own health outcome, so throttling or a failure in one copy counts even when
its peer succeeds, and a copy that fails transiently waits for its peer. An error that
another copy from the same bucket would repeat, such as a missing object, settles that
bucket: its other copies no longer hold the read open, so the read ends once no copy to
another bucket is in flight or can start.

A body whose transfer fails after its headers, which the SDK cannot retry, is retried
once per read against its bucket when no untried bucket remains and no copy to an
unsettled bucket is in flight. A body whose length disagrees with its `Content-Range`
is an invalid response, which another request would likely repeat, so it is not
retried against its bucket.

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
| `cachey_bucket_small_body_p99_seconds` | Time to read a body under 1 MiB after its headers; absent until measured |
| `cachey_bucket_large_body_throughput_p1_bytes_per_second` | Throughput after headers that 99% of bodies of at least 1 MiB exceed; absent until measured or while those bodies arrive too fast to time |
| `cachey_bucket_error_ratio` / `cachey_bucket_consecutive_failures` | Backend health outcomes |
| `cachey_bucket_deprioritized` | Reduced bucket preference |
| `cachey_page_download_latency_seconds` | Successful page latency, including admission and fallback |
| `cachey_first_chunk_latency_seconds` | Handler time to first available chunk, including cache lookup and coalescing |

Bucket latency gauges describe the [latency model](#latency-model): samples come from
each validated backend request, including hedges, except the copies it leaves out.
Those copies, in-flight stalls, and canceled reads can still raise routing latency.
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
