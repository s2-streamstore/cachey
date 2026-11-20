# Replica reads

When a read can be served from multiple replicas, `cachey` routes across buckets using per-bucket health, size-aware latency models, shared deadlines, and bounded concurrency.

## Bucket selection and recovery

### Ranking order

For each read, buckets are ranked by:

1. **Feasibility:** whether the bucket's predicted request p99 at the read size fits within the remaining download deadline and `bucket_timeout` (unmeasured buckets count as feasible).
2. **Health:** healthy buckets rank ahead of deprioritized ones; among deprioritized buckets, lower `error_rate` wins.
3. **Routing latency:** lower locality-adjusted routing latency wins. The first client-supplied bucket gets a locality preference (peers are scaled by 1.5×).
4. **Client order:** earlier buckets break ties.

All supplied buckets remain eligible for fallback even if deprioritized or historically infeasible.

### Routing latency and stall detection

A bucket's routing latency at a read's size is:

- **Measured buckets:** the request model's median at that size, scaled by a **recent moving average** and a **cancellation excess**.
  - The moving average tracks the last 32 successful reads (at sizes within the model's observed range) relative to the model's *current* median at each read's size. It reacts to latency shifts before the next model refit without double-counting shifts the model has already absorbed.
  - Outside the model's observed size range, the bucket uses its estimate (flat below the smallest band; extended along the last segment above the largest where latency grows measurably with size, or else the largest observed band's median). Because extrapolations can be far off, reads outside the observed range do not update the moving average, and a peer never ranks ahead of the first bucket there.
- **Unmeasured buckets:** start at zero and rise directly from canceled copies until the first completed read succeeds.

Two live signals push traffic off a slowing bucket before timeouts accumulate:

- **Canceled copies:** when a copy at an observed size is canceled, it raises the bucket's cancellation excess toward how far it outlasted its scaled median (as a fraction, so a slow large read slows small reads only in proportion). The excess decays with a 5-second half-life and eases on each normal success. Copies outside observed sizes are not judged. The bucket's first completed measurement resets the moving average, excess, and unmeasured latency.
- **Overdue copies (stalls):** an active copy becomes overdue at 4× its [timeout allowance](#timeout-allowance), refined once headers announce the body length. When three or more copies to a bucket are overdue, its routing latency is raised to at least the elapsed time of the third.

### Health and recovery probes

- **What affects health:** backend errors, overload responses, and timeouts that outlast the copy's [timeout allowance](#timeout-allowance) immediately deprioritize the bucket and reset its recovery progress. Missing objects (`404`), unsatisfied ranges (`416`), invalid object state, cancellation, admission failures, and early timeouts do not affect health.
- **Recovery:** 20 successful reads started after the latest failure clear the deprioritized state.
- **Probing:** when the chosen primary is healthy and feasible, `cachey` may instead probe another bucket that is deprioritized or has had no outcome for 30 seconds, provided no probe or active copy is already running on it and its 24–36 second backoff has elapsed. The first bucket is always eligible for probing; other buckets are probed only if their locality-adjusted median could beat the current choice.
- **Probe safety:** a probe pre-acquires two admission permits (one for itself and one reserved for a backup copy) and disables early hedges; if both permits are not immediately available, the probe is skipped and the healthy winner runs instead. A successful probe clears cancellation excess and lowers the moving average immediately when faster, but raises it only like any normal read so a single slow probe cannot starve the bucket of traffic needed to correct it. Probes preserve existing latency history.

## Latency model

Each bucket maintains three size-aware models:

| Model | Measured interval | Keyed by |
|---|---|---|
| **First byte** | Request start through validated headers (including SDK retries) | Requested bytes |
| **Body** | Validated headers through end of body | Announced response bytes |
| **Request** | Request start through validated body (including SDK retries) | Requested bytes |

Requested bytes are known before headers arrive; backends such as Capacitor read the range before sending headers, so first-byte latency can grow with size. Page reads request a full page, while exact reads (`C0-Exact`) request only their range. When `C0-Object-Size` is provided, admission and the models count a read at the bytes the object holds within the range (what it will actually receive). Without it, page-read models reflect whatever response sizes pages return.

### Fitting and interpolation

- **Bands and medians:** observations are grouped into size bands 4× apart. Each band becomes a point at its weighted median size and weighted median latency, made nondecreasing across bands via pool-adjacent-violators (weighted mean). Predictions have a 1 ms floor.
- **Between and below bands:** medians interpolate linearly between adjacent band points and stay flat below the first band.
- **Above the last band:** medians extrapolate only where latency grows measurably with size, following the last segment's slope capped by both proportionality to size and the slope between the last band's lower and upper size halves. A lone band uses its halves, or—for the body model without split halves—a proportional transfer cost from 1 ms at zero bytes.
- **Tails (p95 and p99):** each band computes p95 and p99 from its own samples only, as quantiles of each sample's ratio to `max(interpolated_median(sample_size), band_median)`, multiplied by the band median. This prevents large sizes within a band, steps below it, or stalls in another band from inflating a band's tail. Tails interpolate linearly between bands and keep a constant ratio to the median beyond the last band.

Band quantiles are used instead of a single line because real object stores are not purely linear in size. For example, S3 Standard serves warm objects from cache (~30 ms first byte) while `age × size` stays under ~50 MiB·s and adds ~80 ms once cold regardless of size. Because small objects stay warm longer, the median steps up where cold reads become the majority; fitting one line across that step predicts seconds for 16 MiB reads, whereas band quantiles follow the step.

### Decay, sampling, training, and refits

- **Decay:** observations decay with a 15-second half-life in *model time*. Model time advances by at most 150 ms per observation, so a half-life spans at least 100 observations under light traffic and idle buckets retain their history.
- **Retention:** each model keeps up to 1,024 samples split evenly across active size bands using unbiased priority sampling, pruning samples whose weight falls below 0.4% of a new observation.
- **Training:** validated responses train all three models. Unfinished copies that received headers train only the first-byte model; otherwise unfinished copies train nothing so hedged-away stalls do not inflate the p99 used to detect them.
- **Refits:** models refit at most once per second, or sooner while observation counts double. Each routing decision refits at most one bucket on the 1-second cadence (the one longest without a refit). Fitting 1,024 samples takes tens of microseconds under the bucket's lock.

### Sparse-size behavior

Rarely read sizes have few samples and usually err toward hedging late: a lone slow read sets its band's median and tail until more reads arrive or it ages out, and sizes past the largest retained sample can be overstated if the last band sits just above a warm-to-cold step with a single size. Conversely, a band with only a few healthy reads has tails close to its median and can fire hedges on healthy reads (bounded by the hedge budget), including at cold start or in bands where 1%–5% of reads are cold (warm p95, cold p99).

## Deadlines, hedging, rescues, and retries

A download fetches one cache entry (a full page or an exact range). Its `download_timeout` covers admission, SDK retries, and body validation. Up to **3 copies** may run concurrently per download; the first validated response wins and cancels the rest.

Copies to the same bucket share a single **bucket deadline** starting at that bucket's first admission (`min(bucket_timeout, remaining_download_time)`), and its expiry counts against bucket health at most once. Conclusive per-bucket errors (`404` missing object, `416` unsatisfied range, or invalid object state) immediately cancel all remaining copies to that bucket, releasing their admission capacity.

### Copy types

- **Early hedges:** at most one early hedge is launched per download, timed from the primary's request start (first-byte model at requested size) and then from header arrival (body model at announced size). If the header hedge is blocked by admission or budget, the body phase can still fire.
  - Delay is `min(p99, 1.25 × p95, 5 × median)`. Following p95 avoids hedging a bimodal slow mode above 5% of reads (such as S3 Standard cold objects, where a second copy waits on the same backend fetch), while the 5× median cap still rescues stalls above 5% of reads (cold reads are ~4× warm). Health, rescue, and retry decisions use uncapped p99.
  - Early hedges use a single SDK attempt, skip overloaded buckets, never wait for admission, and prefer an untried feasible peer; otherwise they hedge against the primary's bucket if more than its [timeout allowance](#timeout-allowance) remains on the bucket deadline.
- **Deadline rescues:** at download start, untried feasible replicas are given staggered launch delays (consuming up to two-thirds of `download_timeout`, plus a grace-timed backup when probing) so a stalled primary does not exhaust the download deadline. Non-overloaded rescues are pulled earlier when needed to finish before the deadline; overloaded rescues keep their staggered time. When a rescue timer fires, it re-checks fresh feasibility against `remaining` (or `download_timeout` if the target is overloaded) before launching.
- **Fallbacks and body retries:** when a copy fails with a retryable error, the download falls back to the best untried bucket (even if historically infeasible). Once no other copy is active and no untried bucket remains, a `BodyStreaming` failure may be retried once against a failed bucket that still has more than its [timeout allowance](#timeout-allowance) left on its bucket deadline, without waiting for admission.

### Timeout allowance

A copy's timeout allowance governs timeout health verdicts, stall thresholds (`4×`), admission queueing patience, and whether a later copy to the same bucket has enough time left:

- Each reached phase allows its uncapped p99, or half of `min(bucket_share, bucket_timeout)` when unmeasured (where `bucket_share = download_timeout / bucket_count`).
- Before both phases are measured, the request model's p99 is a floor.
- Once both `first_byte` and `body` tails are measured, their sum is capped by `max(request.tail, first_byte.tail, body.tail)` (querying `request` at `max(requested_bytes, announced_body_bytes)`). This keeps anticorrelated or independent phase tails aligned with `request.tail` and `bucket_timeout` while still allowing small announced bodies on page reads to tighten below `request.tail` and rare or extrapolated large bodies to keep `body.tail`.

### Attempt budgets

Hedges and retries use separate success-funded budgets shared across `Downloader` clones:

- **Hedge budget:** starts with 1 hedge, earns `--hedge-budget-percent` credits per successful download (default 5%, or 1 hedge per 20 successes, up to 16 stored), and caps active hedges at 16 globally and 2 per bucket. Setting `--hedge-budget-percent 0` disables early hedges while keeping fallback and deadline rescue active.
- **Retry budget:** starts with 1 attempt and earns 1 per 10 successful downloads (up to 16 stored). It gates same-bucket body retries and any extra copy launched while all buckets are overloaded.

## Limits and admission

| Server option | Default |
|---|---|
| `--bucket-timeout-ms` | 5,000 ms per bucket, per download |
| `--download-timeout-ms` | 10,000 ms per download |
| `--max-download-memory` | 1 GiB of reserved body bytes |
| `--hedge-budget-percent` | 5% |

Rust callers configure limits through `DownloadLimits`; `Downloader` clones share limits and admission state.

### Capacity reservations

Each active copy reserves its expected bytes—16 MiB for a page read, or an exact range's length, clamped to `C0-Object-Size` when provided—separate from cache capacity and total process memory. By default, 64 full 16 MiB pages fit. Active copies are also capped at 1,024, so reads under 1 MiB reach the request cap before the memory cap. Admission trusts `C0-Object-Size`, so an understated size lets a read hold up to the rest of its range beyond its reservation.

### Queueing policy

- **Non-queueing copies:** early hedges, body retries, and copies launched during widespread overload (when all buckets are overloaded) never queue for admission—they require an immediate permit (or a recovery probe's reserved backup permit) so they cannot spend a budget credit and time out before starting.
- **Queueing copies:** all other copies wait while they can still start by `download_deadline - timeout_allowance`.
  - A queue younger than **100 ms** is treated as absorbing a burst and waits up to that deadline.
  - Once a queue has stood for **100 ms** or longer, waiting only adds latency without improving throughput, so newly arriving copies wait at most `max(timeout_allowance, 100 ms)`.
  - A copy fails immediately with `AdmissionTimeout` if its predicted wait—the moving average departure interval since the queue last emptied, multiplied by `(waiting + 1)` minus time since the last departure—reaches its latest start time.
- **No admission fallback:** because every bucket shares the same admission queue, a copy that fails admission does not fall back to another bucket. If an untried fallback during widespread overload is rejected for lacking immediate admission, `AdmissionTimeout` is recorded on that bucket so a prior `404` from another replica is not returned as a false miss.

## Errors

- **Classification:** HTTP `429`/`503` or `SlowDown`, `ServiceUnavailable`, and throttling codes signal overload (marking the bucket overloaded for 1 second). Bare `404` or `416` responses without a more specific error code (for example, empty or non-XML bodies from S3-compatible stores or proxies) are treated as missing objects or unsatisfied ranges.
- **HTTP status mapping:** admission failures and overload return HTTP `503`; timeouts return HTTP `504`.
- **Precedence:** backend errors take precedence over timeouts, admission failures, and missing objects. Once response streaming to the client has started, errors terminate the body.
- **Ranges:** empty or reversed ranges are rejected before contacting any backend. Unsatisfied ranges (`416`) permit fallback across replicas; if all replicas fail and `RangeNotSatisfied` is the final error, it reports the largest object size seen across responses.

## Metrics and headers

| Metric | Description |
|---|---|
| `cachey_bucket_latency_seconds` | Request-model median or p99 (`quantile`) at `bytes` of 4 KiB, 64 KiB, 1 MiB, or 16 MiB |
| `cachey_bucket_latency_model_bytes` | Largest read size retained by the request model |
| `cachey_bucket_latency_predictions_total` | Training observations at sizes the current fit predicted |
| `cachey_bucket_latency_predictions_exceeded_total` | Observations exceeding the predicted median (`quantile="0.5"`) or p99 (`quantile="0.99"`) |
| `cachey_bucket_error_ratio` / `cachey_bucket_consecutive_failures` | Backend health outcomes |
| `cachey_bucket_deprioritized` | Whether the bucket is currently deprioritized |
| `cachey_bucket_hedge_delay_seconds` | Early hedge delay by `phase` (`first_byte` or `body`) and `bytes` |
| `cachey_hedges_total` | Early hedges of successful downloads by primary `bucket`, `phase`, `target` (`primary` or `peer`), and `outcome` (`won` or `lost`) |
| `cachey_download_latency_seconds` | Successful download latency, including admission, retries, and fallback |
| `cachey_first_chunk_latency_seconds` | Handler time to first chunk, including cache lookup and coalescing |

- **Reporting scope:** latency metrics report the request model (`phase="request"`); first-byte and body models are exposed through `cachey_bucket_hedge_delay_seconds`. Gauges are omitted until measured, and reported only at sizes up to the model's largest retained sample so no extrapolated values are exported. Latencies exclude transmission to the client.
- **Prediction accuracy:** each training observation is compared against the fit active when it arrives (well-calibrated models sit near 50% above median and 1% above p99). Sizes beyond the model's reach and unfinished copies past headers are excluded.
- **`DownloadOutput` and `C0-Status`:** `DownloadOutput` records the primary and winning bucket indices, whether any copies overlapped (`hedged`, including rescues and probes), and any early `hedge` outcome. `C0-Status` reports the serving bucket and cache status in a response header for the first entry and HTTP/2 trailers for subsequent pages (HTTP/1.1 reports only the first entry).

See the [simulation harness](simulation/README.md) and [reference results](simulation/REFERENCE.md).
