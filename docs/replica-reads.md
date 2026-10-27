# Replica reads

## Selection and recovery

Replica selection uses backend health, recent latency, and the time a new copy has:
the remaining page deadline, within the bucket timeout.
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
shift the model absorbs is not counted twice. Beyond the sizes its model can predict, a bucket is
ranked by its median at the largest size it has seen, since latency grows with size.
It never ranks ahead of the first bucket there. Only reads of sizes the model has
observed move the moving average, since an extrapolated median can be far off.

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
whole page. With `C0-Object-Size`, admission and the models count them at the bytes
the object holds, which is what they receive. Without it, their models describe
whatever response sizes pages return.

Each model fits one line to the median:

```text
median = fixed cost + bytes × cost per byte
```

Both coefficients are nonnegative. A line minimizing absolute deviations sets the
trend; the fixed cost is then refit to residuals relative to the trend's prediction at
each size, and the cost per byte to absolute deviations at that fixed cost. Absolute
deviations at small sizes stay small even when they are many times the latency, so
where no line fits every size, as during a change in speed, the trend can drop the
fixed cost. Relative residuals let small sizes set it, while large sizes still decide
the cost per byte. Predictions have a 1 ms floor. A single tail
ratio scales the median to p99 for every size, so p99 needs as much data as one
quantile rather than a second line. Observations weigh into the ratio by their
predicted median, so constant stalls on small reads do not scale large ones. Sizes
beyond every retained observation are predicted only from a measurable cost per
byte. With one observed size, body time is treated as transfer cost and the other
intervals as fixed cost.

Observations have a 15-second half-life of model time. Model time advances at most
150 ms per observation, so a half-life spans at least 100 observations when traffic
is light, and idle buckets keep their history. Each model retains up to 1,024
samples, split evenly across size bands four times apart, so rare sizes survive.
Priority sampling keeps weights unbiased. Samples are dropped once they weigh under
0.4% of a new observation.

Validated responses train all three models. Copies that receive headers train the
first-byte model even if they later fail, time out, or are canceled. Otherwise,
unfinished copies train nothing, so tails describe completed requests: stalls
superseded by a hedge stay outside the p99 that detects them. Recovery probes keep
latency history.

Models refit at most once per second, or sooner while the observation count
doubles. A routing decision refits at most one bucket on that interval, the one
longest without a refit. Samples are copied under the bucket's statistics lock, then
fitted outside it. Metrics report the latest fit.

## Deadlines and hedging

The page deadline includes admission, SDK retries, and body validation. Copies to
one bucket share a deadline starting at its first admission, whose expiry counts
against health once. Up to three copies can run
per page; the first validated result wins and cancels the others.

Early hedges use the first-byte model at the requested size, then the body model at
the announced size. Each delay is p99 capped at 1.5 times the median. Health,
rescue, and retry decisions use uncapped estimates.

Early hedges use one SDK attempt, skip overloaded buckets, and never wait for
admission. They prefer an untried bucket expected to finish within its timeout and
the page deadline, or else use the primary's bucket. A failed body can be retried
once when no other copy or untried bucket remains, without waiting for admission.

A copy's timeout allowance is the p99 of each interval, or half the bucket's share
of the page timeout for each unmeasured interval. A later copy to a bucket needs
that much of the bucket's deadline left. A missing object, unsatisfied range, or
invalid object state cancels all remaining copies to that bucket immediately,
releasing their admission capacity.

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

Server reads reserve the bytes they expect: 16 MiB per page, or what remains of an
object of known size. Admission trusts that size, so a page whose size is understated
can hold up to the rest of the page beyond its reservation. By default, 64 full pages
fit. This limit is separate from cache capacity and total process memory.

Copies other than early hedges and body retries wait for admission while they can
still finish by the deadline. A queue that formed within the last 100 ms is
absorbing a burst, which capacity will reach. Once it has stood longer, waiting only
delays reads without filling more pages, so a copy joining it waits no longer than
its timeout allowance or 100 ms, whichever is longer. A copy that fails admission
does not fall back to another bucket, whose copy would wait in the same queue. A
copy fails at once if it would not be admitted in time: each copy ahead of it, then
the copy itself, is expected to take the average time between departures, admitted
or abandoned, since the queue was last empty.

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
| `cachey_bucket_latency_base_seconds` | Median latency of a zero-byte request |
| `cachey_bucket_latency_seconds_per_byte` | Median latency added per byte |
| `cachey_bucket_latency_tail_ratio` | p99 latency as a multiple of the median |
| `cachey_bucket_latency_model_bytes` | Largest size retained by the model |
| `cachey_bucket_error_ratio` / `cachey_bucket_consecutive_failures` | Backend health outcomes |
| `cachey_bucket_deprioritized` | Reduced bucket preference |
| `cachey_page_download_latency_seconds` | Successful page latency, including admission and fallback |
| `cachey_first_chunk_latency_seconds` | Handler time to first chunk, including cache lookup and coalescing |

Latency gauges have a `phase` label: `first_byte`, `body`, or `request`. They are
absent until measured. A hedge delay is the median at the read's size times the
smaller of the tail ratio and 1.5. Buffered-only history reports zero cost per byte.
Latencies exclude transmission to the client.

`DownloadOutput` identifies the initial and winning buckets. `hedged` means copies
overlapped, including rescue and recovery probes; `fallback` counts wins from
another bucket.

`C0-Status` exposes the serving bucket and cache status. The first page uses a
response header. Subsequent pages use HTTP/2 trailers; HTTP/1.1 reports only the
first page.

See the [simulation harness](simulation/README.md) and
[reference results](simulation/REFERENCE.md).
