# Replica reads

## Selection and recovery

Cachey prefers copies likely to finish within the remaining page deadline, then healthy copies, then recent complete-read latency. The first client-supplied bucket has a locality preference: its latency is compared with 1.5 times each alternative's latency. Other buckets have equal preference. Unknown alternatives initially use the preferred bucket's latency as an estimate. Every supplied bucket remains eligible for fallback.

A backend failure immediately deprioritizes that bucket. Missing objects, invalid client ranges, caller cancellation, and local admission failures do not count as backend health failures. Malformed backend responses and operations that expire with sufficient execution time do count as failures. The error fraction decays over time, but health recovers through evidence: 20 consecutive successful operations started after the latest failure restore normal preference. After 24–36 seconds, a previously competitive bucket can receive one recovery probe at a time. A working alternative protects that probe if it is slow. Probing requires reserving admission for both copies; otherwise the healthy route keeps the request and recovery remains eligible. Idle time permits a recheck; it does not declare recovery.

## Deadlines and hedging

Each distinct copy gets its own operation deadline after admission, including SDK retries, body transfer, and validation. Defaults are 5 seconds per copy (`--bucket-timeout-ms`) and 10 seconds for the entire page (`--page-timeout-ms`). Recoverable errors immediately advance to an untried copy. Timed rescue attempts reserve part of the page budget using the alternatives' recent successful p99 durations; unmeasured copies receive an equal share of the page budget. A normal primary's p99 grace cannot consume a fallback's useful execution time. Recent explicit overload preserves the primary grace to avoid increasing congestion. Tight deadlines can require overlapping copies even when the primary completes normally. Each reservation stays attached to its destination even if preferences change; rescue attempts do not cancel an otherwise viable earlier read. At most three copies are active per page, and each supplied copy is tried at most once at this layer. Backend SDK retries are contained within those operations. The first fully validated result wins and cancels the remaining work.

With multiple buckets, early hedges use another copy. With one bucket, the primary and hedge race against the same bucket. Early hedges use successful-operation p99 latency. They require a shared success-funded allowance: one startup hedge plus one per 20 successful page downloads by default (`--hedge-budget-percent 5`; zero disables early hedging). Credits and concurrent early hedges are capped globally at 16; each destination allows at most two concurrent early hedges. Early hedges use one SDK attempt. Ordinary fallback and deadline rescue do not require early-hedge credits. When every supplied bucket has recently reported explicit overload, early hedges stop and extra attempts share a separate bounded retry allowance, replenished by successful pages.

## Admission

Admission limits are shared by downloader clones and cover primaries, retries within their operations, and speculative copies: 1 GiB of requested body bytes (`--max-download-memory`) by default. Server reads reserve a full 16 MiB page, including when the object is smaller, giving 64 active copies at the default budget. The Rust downloader also has a 1,024-request guard for arbitrary small ranges. Waiting consumes the original page deadline and does not count as backend failure; the backend operation timeout starts when admission is granted. These limits cover active downloads, separately from cache capacity and total process memory. Body collection rejects excess data as soon as it exceeds the validated response range. Early hedges skip unavailable admission; ordinary copy operations can wait until their deadline. Admission exhaustion and explicit backend overload return HTTP 503 for the first chunk; a timeout returns HTTP 504. A missing copy cannot turn another copy's failure or a suppressed overload retry into HTTP 404. Later failures terminate the response body.

Rust callers pass `DownloadLimits` to `Downloader::new`, which validates the complete configuration before creating shared controls, or through `ServiceConfig::download_limits`. Deadlines and admission capacities must be positive, and the hedge budget percentage must be in `0..=100`. Adaptive concurrency control is not enabled.

## Latency and metrics

Successful full-operation durations populate the bucket latency histogram. A separate routing EWMA reacts to latency changes; several concurrent stalled operations also affect routing before their hard timeouts. Cancellation can raise a too-optimistic routing estimate, but cannot count as a successful latency sample or a health failure. Snapshots refresh at most once per second, with immediate initialization from the first success. For the single-bucket race, a success is measured from the original primary start, including the hedge delay. Multi-copy operations each use their own start time; page latency includes all elapsed selection, admission, and fallback time.

| Measurement | Boundary |
|-------------|----------|
| `cachey_bucket_latency_mean_seconds` | Mean successful complete bucket-operation duration. |
| `cachey_bucket_latency_hedge_seconds` | Successful bucket-operation p99 used for early hedging. |
| `cachey_bucket_error_rate` / `cachey_bucket_consecutive_failures` | Backend health outcomes; object-specific and local failures are excluded. |
| `cachey_bucket_deprioritized` | Soft health priority. |
| `cachey_page_download_latency_seconds` | Successful page download, including admission and every attempted copy. |
| `cachey_first_chunk_latency_seconds` | HTTP handler time to its first available chunk, including cache lookup or coalesced-fill waiting. |

`DownloadOutput::secondary_bucket_idx` identifies the first additional copy actually started; `used_bucket_idx` can identify any supplied copy. Its `hedged` flag reports overlapping requests, including timed rescue and protected recovery probes. The page `fallback` metric counts successes from a copy other than the initially selected one. Successful latency histograms exclude failed pages and client response-body transmission.

See the [simulation harness](simulation/README.md) for scenarios and run instructions, and the [reference report](simulation/REFERENCE.md) for measured behavior and limitations.
