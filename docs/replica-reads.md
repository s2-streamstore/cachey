# Replica reads

## Selection and recovery

Replica selection uses backend health, recent latency, and the time a new copy has:
the remaining download deadline, within the bucket timeout.
The first client-supplied bucket gets a locality preference; all buckets remain
eligible for fallback.

Backend failures reduce preference; successful reads and recovery probes restore
it. Missing objects, invalid ranges, cancellation, and admission failures do not
affect health. A deprioritized bucket, or one without an outcome for 30 seconds, is
probed if it might beat the current choice, and the first bucket regardless.

Buckets are ranked by routing latency at the read's size: the request model's
median, scaled by a moving average of the last 32 successful reads relative to it and
by an excess from canceled copies. The moving average follows a shift in latency
sooner than the model does, and since it is taken against the current median, a
shift the model absorbs is not counted twice. Outside the range of sizes its model has
observed, a bucket is ranked by the model's estimate: below it, its smallest band's
median; above it, its last segment extended where latency grows measurably with size,
or else its median at the largest size it has seen, since latency grows with size. An estimate can be far off, so there a peer never
ranks ahead of the first bucket, and reads of those sizes leave the moving average
alone.

Canceled copies raise the excess toward how far they outlasted their routing
latency, as a fraction, so a slow large read slows small ones only in proportion.
Copies of a size the model has not observed are not judged. The excess decays with a
5-second half-life. Before a bucket has a median, canceled copies raise its routing
latency directly, which stays until a read succeeds. A bucket's first measurement
resets the moving average, the excess, and that latency. A recovery probe clears the
excess and that latency, and lowers the moving average at once when it is faster, but
raises it only as any read does, since a bucket that looks slow gets no reads to
correct it.

Feasibility, rescue timing, and stall detection use the request model at each
read's size. A copy is overdue after four times its timeout allowance, which is
refined when headers announce the body length. Three overdue copies raise routing
latency to the elapsed time of the third.

## Latency model

Each bucket has three models:

| Model | Interval | Keyed by |
|-------|----------|----------|
| First byte | Request start through validated headers, including SDK retries | Requested bytes |
| Body | Headers through the end of the body | Announced response bytes |
| Request | Request start through body validation, including SDK retries | Requested bytes |

Requested bytes are known before headers arrive. Capacitor reads the range before
sending headers, so its time to first byte grows with size. Page reads request a
whole page, and exact reads (`C0-Exact`) only their range. With `C0-Object-Size`,
admission and the models count a read at the bytes the object holds within it, which
is what it receives. Without it, the models of page reads describe whatever response
sizes pages return.

Each model has a point per size band, bands being four times apart: the band's median
size and median latency, with medians made nondecreasing across bands by pooling
adjacent violators into their weighted mean. Medians are interpolated linearly between
points and held flat below the first. Beyond the last they follow the last segment,
where latency grows measurably with size, at most in proportion to size and no faster
than the last band's own halves, each twice the other's sizes, show. A lone band
follows its halves; without them, its body time is a transfer cost from 1 ms at zero
bytes. Predictions have a 1 ms floor.

A band's p95 and p99 are its median times those quantiles of each sample's latency
relative to the median at the sample's size, and at least the band's own, so neither
the band's largest sizes nor a step in latency below it set its tail. They come only
from the band's own samples, so a stall or slow mode in one band never stretches
another's. Between points they are interpolated like medians, so sizes between two
bands take tails from both, and beyond the last they keep their ratio to it.

Latency need not grow in proportion to size. S3 Standard serves objects from a cache
while their age times their size stays under about 50 MiB·s, with a first byte in
about 30 ms, and adds about 80 ms when they are cold, regardless of size. A read soon
after a write can find an object warm, while a later one, such as a reader catching
up, finds it cold. Small objects stay warm longer, so the share of cold reads grows
with size: reads of one size split between a warm and a cold mode, and the median
steps up where cold reads become the majority. A line through every size predicts
seconds for 16 MiB from that step; quantiles by band follow it.

Sizes read only a few times are estimated from few samples, and mostly err toward
hedging late. A lone slow read becomes its band's median and tail, and raises the
median of sizes between it and the next band, until more reads arrive or it ages out.
Sizes beyond the largest retained one follow the last segment, which overstates them
after a warm-to-cold step when the last band holds one size. The exception is a band
that has seen only a few healthy reads: its tails sit close to its median, so its
hedges can fire on healthy reads, within the hedge budget. At a cold start, when every
band is sparse, they can spend the budget before the first stall. Likewise, where cold
reads are 1% to 5% of a band, its p95 stays warm and its p99 cold, so those cold reads
are hedged, within the budget.

Observations have a 15-second half-life of model time. Model time advances at most
150 ms per observation, so a half-life spans at least 100 observations when traffic
is light, and idle buckets keep their history. Each model retains up to 1,024
samples, split evenly across size bands, so rare sizes survive.
Priority sampling keeps weights unbiased. Samples are dropped once they weigh under
0.4% of a new observation.

Validated responses train all three models. Copies that receive headers train the
first-byte model even if they later fail, time out, or are canceled. Otherwise,
unfinished copies train nothing, so tails describe completed requests: stalls
superseded by a hedge stay outside the p99 that detects them. Recovery probes keep
latency history.

Models refit at most once per second, or sooner while the observation count
doubles. A routing decision refits at most one bucket on that interval, the one
longest without a refit. A fit of 1,024 samples takes tens of microseconds, under the
bucket's statistics lock. Metrics report the latest fit.

## Deadlines and hedging

A download fetches one cache entry: a page, or an exact range. Its deadline includes
admission, SDK retries, and body validation. Copies to one bucket share a deadline
starting at its first admission, whose expiry counts against health once. Up to three
copies can run per download; the first validated result wins and cancels the others.

Early hedges use the first-byte model at the requested size, then the body model at
the announced size. Each delay is p99 capped at 1.25 times the p95 and five times the
median. Where stalls exceed 1% of reads they set the p99, and with ±20% jitter the p95
cap is about 1.5 times the median. The cap follows the p95 because reads of one size
can split between fast and slow objects, like S3 Standard's warm and cold ones. Every
copy of a cold read waits for the same fetch from storage, so hedging it only spends
the budget that stalls need. Cold reads take about four times as long as warm ones, so
a slow mode beyond five times the median is a stall, which a hedge can rescue even
when it exceeds 5% of reads. Health, rescue, and retry decisions use uncapped
estimates.

Early hedges use one SDK attempt, skip overloaded buckets, and never wait for
admission. They prefer an untried bucket expected to finish within its timeout and
the download deadline, or else use the primary's bucket. A failed body can be retried
once when no other copy or untried bucket remains, without waiting for admission.

A copy's timeout allowance is the p99 of each interval, or half the bucket's share
of the download timeout for each unmeasured interval. A later copy to a bucket needs
that much of the bucket's deadline left. A missing object, unsatisfied range, or
invalid object state cancels all remaining copies to that bucket immediately,
releasing their admission capacity.

Hedges and body retries have separate budgets funded by successful reads. The
default allows one startup hedge and one per 20 successful downloads. A zero hedge
budget still permits fallback and deadline rescue.

## Limits

| Server option | Default |
|---------------|---------|
| `--bucket-timeout-ms` | 5,000 ms per bucket, per download |
| `--download-timeout-ms` | 10,000 ms per download |
| `--max-download-memory` | 1 GiB of reserved body bytes |
| `--hedge-budget-percent` | 5% |

Server reads reserve the bytes they expect: 16 MiB per page or an exact range's
length, less any part past the end of an object of known size. Admission trusts that
size, so a read whose size is understated can hold up to the rest of its range beyond
its reservation. By default, 64 full pages fit. This limit is separate from cache
capacity and total process memory. Active copies are also capped at 1,024, so at
the default memory limit, copies under 1 MiB, such as small exact reads, reach that cap
first.

Copies other than early hedges and body retries wait for admission while they can
still finish by the deadline. A queue that formed within the last 100 ms is
absorbing a burst, which capacity will reach. Once it has stood longer, waiting only
delays reads without filling more entries, so a copy joining it waits no longer than
its timeout allowance or 100 ms, whichever is longer. A copy that fails admission
does not fall back to another bucket, whose copy would wait in the same queue. A
copy fails at once if it would not be admitted in time: each copy ahead of it, then
the copy itself, is expected to take the average time between departures, admitted
or abandoned, since the queue was last empty.

Rust callers set both limits through `DownloadLimits`. Downloader clones share limits.

## Errors

A backend response signals overload when its status is 429 or 503, or its error code
is `SlowDown`, `ServiceUnavailable`, or a throttling code.

Admission failure or overload returns HTTP 503; timeouts return 504. Backend errors
take precedence over timeouts, admission failures, and missing objects. Errors
after streaming starts terminate the body.

An unsatisfied range permits fallback. If it is the final error, it reports the
largest size from those responses. Empty or reversed ranges are rejected before
contacting a backend.

## Metrics

| Measurement | Meaning |
|-------------|---------|
| `cachey_bucket_latency_seconds` | Median, p95, or p99 (`quantile`) of a size band, labeled by its smallest size (`bytes`) |
| `cachey_bucket_latency_model_bytes` | Largest size retained by the model |
| `cachey_bucket_latency_predictions_total` | Observations at sizes the model predicted |
| `cachey_bucket_latency_predictions_exceeded_total` | Those slower than the predicted median (`quantile="0.5"`) or p99 (`quantile="0.99"`) |
| `cachey_bucket_error_ratio` / `cachey_bucket_consecutive_failures` | Backend health outcomes |
| `cachey_bucket_deprioritized` | Reduced bucket preference |
| `cachey_download_latency_seconds` | Successful download latency, including admission and fallback |
| `cachey_first_chunk_latency_seconds` | Handler time to first chunk, including cache lookup and coalescing |

Latency gauges have a `phase` label: `first_byte`, `body`, or `request`. They are
absent until measured, and a band's series only while it retains samples. Latencies
exclude transmission to the client.

Prediction counters compare each observation that trains a model with the fit in use
when it arrives. For an accurate model, half exceed the median and 1% exceed p99.
Sizes beyond the model's reach are not counted, nor are stalls a hedge superseded,
since unfinished copies train nothing past their headers.

`DownloadOutput` identifies the initial and winning buckets. `hedged` means copies
overlapped, including rescue and recovery probes; `fallback` counts wins from
another bucket.

`C0-Status` exposes the serving bucket and cache status. The first entry uses a
response header. Later pages use HTTP/2 trailers; HTTP/1.1 reports only the first
entry.

See the [simulation harness](simulation/README.md) and
[reference results](simulation/REFERENCE.md).
