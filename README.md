# Cachey

High-performance read-through cache for object storage.

- Simple HTTP API
- Hybrid memory + disk cache powered by [foyer](https://github.com/foyer-rs/foyer)
- Designed for caching immutable blobs
- Works with any S3-compatible backend, but has its own `/fetch` API requiring a precise `Range`
- Fixed page size (16 MiB) – maps requested byte range to page-aligned lookups
- Coalesces concurrent requests for the same page
- Makes hedged requests to manage tail latency of object storage
- Can attempt redundant buckets for a given object

[Motivating context](https://www.reddit.com/r/databasedevelopment/comments/1nh1goo/cachey_a_readthrough_cache_for_s3)

## API

### Fetching data

#### Request

```
HEAD|GET /fetch/{kind}/{object}
```
- `kind` + `object` form the cache key
- `kind` identifies the bucket set (up to 64 chars)
- `object` is the S3 object key

| Header | Required | Description |
|--------|----------|-------------|
| `Range` | yes | Byte range in format `bytes={first}-{last}` |
| `C0-Bucket` | no | Bucket(s) containing the object |
| `C0-Config` | no | Override S3 request config |

`C0-Bucket` behavior:
- Multiple headers indicate bucket preference order
- If omitted, `kind` is used as the singular bucket name
- Client preference may be overridden based on internal latency/error stats
- At most 2 buckets attempted per page miss

`C0-Config` overrides:
Space-separated key-value pairs to override S3 request configuration per page miss.
- `ct=<ms>` Connect timeout (in case an existing connection could not be reused)
- `rt=<ms>` Read timeout (time-to-first-byte)
- `ot=<ms>` Operation timeout (across retries)
- `oat=<ms>` Operation attempt timeout
- `ma=<num>` Maximum attempts for each bucket's primary SDK operation
- `ib=<ms>` Initial backoff duration
- `mb=<ms>` Maximum backoff duration
- `fps=<bool>` Force path-style addressing

SDK overrides apply within the server's bucket and page download deadlines. A hedge uses the same overrides with one SDK attempt, so retries cannot multiply speculative requests.

#### Example Request

```http
GET /fetch/prod-videos/movie-2024.mp4 HTTP/1.1
Range: bytes=1048576-18874367
C0-Bucket: us-west-videos
C0-Bucket: us-east-videos-backup
C0-Config: ct=1000 oat=1500 ma=5 ib=10 mb=100
```

#### Response

The service maps requests to 16 MiB page-aligned ranges and the response has standard HTTP semantics (`206 Partial Content`, `404 Not Found` etc.)

| Header | Description |
|--------|-------------|
| `Content-Range` | Actual byte range served |
| `Content-Length` | Number of bytes in response |
| `Last-Modified` | Timestamp from first page |
| `Content-Type` | Always `application/octet-stream` |
| `C0-Status` | Status for first page |

`C0-Status` format: `{first}-{last}; {bucket}; {cached_at}`
- Byte range and which bucket was used
- `cached_at` is Unix timestamp with 0 implying a cache miss
- Only first page status is sent as a header; status for subsequent pages follows the body as trailers

#### Example Response

```http
HTTP/1.1 206 Partial Content
Content-Range: bytes 1048576-18874367/52428800
Content-Length: 17825792
Content-Type: application/octet-stream
C0-Status: 1048576-16777215; us-west-videos; 1704067200

<data>

C0-Status: 16777216-18874367; us-west-videos; 0
```

### Monitoring

`GET /stats` returns throughput stats as JSON for load balancing and health checking.

`GET /metrics` returns a more comprehensive set of metrics in Prometheus text format.

### Replica selection, deadlines, and hedging

Cachey prefers copies likely to finish within the remaining page deadline, then healthy copies, then recent complete-read latency. The first client-supplied bucket has a locality preference: its latency is compared with 1.5 times each alternative's latency. Other buckets have equal preference. Unknown alternatives initially use the preferred bucket's latency as an estimate. Every supplied bucket remains eligible for fallback.

A backend failure immediately deprioritizes that bucket. Missing objects, invalid ranges, caller cancellation, and local admission failures do not count as backend health failures. The error fraction decays over time, but health recovers through evidence: 20 consecutive successful operations started after the latest failure restore normal preference. After 24–36 seconds, a previously competitive bucket can receive one recovery probe at a time. A working alternative protects that probe if it is slow. Probing requires reserving admission for both copies; otherwise the healthy route keeps the request. Idle time permits a recheck; it does not declare recovery.

Each distinct copy gets its own operation deadline after admission, including SDK retries, body transfer, and validation. Defaults are 5 seconds per copy (`--bucket-timeout-ms`) and 10 seconds for the entire page (`--page-timeout-ms`). Recoverable errors immediately advance to an untried copy. Timed rescue attempts reserve part of the page budget using the alternatives' recent successful p99 durations. Each reservation stays attached to its destination even if preferences change; rescue attempts do not cancel an otherwise viable earlier read. At most three copies are active per page, and each supplied copy is tried at most once at this layer. Backend SDK retries are contained within those operations. The first fully validated result wins and cancels the remaining work.

With multiple buckets, early hedges use another copy. With one bucket, the existing same-bucket primary/hedge race remains. Early hedges use the configured successful-latency quantile (`--hedge-quantile`, zero disables early hedging). They require a shared success-funded allowance: one startup hedge plus one per 20 successful page downloads by default (`--hedge-budget-percent 5`). Credits and concurrent early hedges are capped globally at 16 (`--max-concurrent-hedges`); each destination allows at most two concurrent early hedges. Early hedges use one SDK attempt. Ordinary fallback and deadline rescue do not require early-hedge credits. When every supplied bucket has recently reported explicit overload, early hedges stop and extra attempts share a separate bounded retry allowance, replenished by successful pages.

Admission limits are shared by downloader clones and cover primaries, retries within their operations, and speculative copies: 1,024 backend requests (`--max-inflight-requests`) and 1 GiB of requested body bytes (`--max-download-memory`) by default. Waiting consumes the original page deadline and does not count as backend failure; the backend operation timeout starts when admission is granted. These limits cover active downloads, separately from cache capacity and total process memory. Body collection rejects excess data as soon as it exceeds the validated response range. Early hedges skip unavailable admission; ordinary copy operations can wait until their deadline. Admission exhaustion and explicit backend overload return HTTP 503 for the first chunk; a timeout returns HTTP 504. Later failures terminate the response body.

Successful full-operation durations populate the bucket latency histogram. A separate routing EWMA reacts to latency changes; several concurrent stalled operations also affect routing before their hard timeouts. Cancellation can raise a too-optimistic routing estimate, but cannot count as a successful latency sample or a health failure. Snapshots refresh at most once per second, with immediate initialization from the first success. For the single-bucket race, a success is measured from the original primary start, including the hedge delay. Multi-copy operations each use their own start time; page latency includes all elapsed selection, admission, and fallback time.

| Measurement | Boundary |
|-------------|----------|
| `cachey_bucket_latency_mean_seconds` | Mean successful complete bucket-operation duration. |
| `cachey_bucket_latency_hedge_seconds` | Successful bucket-operation quantile used for early hedging. |
| `cachey_bucket_error_rate` / `cachey_bucket_consecutive_failures` | Backend health outcomes; object-specific and local failures are excluded. |
| `cachey_bucket_deprioritized` | Soft health priority; replaces `cachey_bucket_circuit_breaker_open`. |
| `cachey_page_download_latency_seconds` | Successful page download, including admission and every attempted copy. |
| `cachey_first_chunk_latency_seconds` | HTTP handler time to its first available chunk, including cache lookup or coalesced-fill waiting. |

`DownloadOutput::secondary_bucket_idx` identifies the first additional copy actually started; `used_bucket_idx` can identify any supplied copy. Its `hedged` flag reports overlapping requests, including timed rescue and protected recovery probes. The page `fallback` metric counts successes from a copy other than the initially selected one. Successful latency histograms exclude failed pages and client response-body transmission.

Rust callers configure `DownloadLimits` through `Downloader::with_limits` before sharing clones, or through `ServiceConfig::download_limits`. Deadlines and admission capacities must be positive, and the hedge budget percentage must be in `0..=100`. Adaptive concurrency control is not enabled.

## Command line

[Docker images](https://github.com/s2-streamstore/cachey/pkgs/container/cachey) are available.

```
Usage: server [OPTIONS]

Options:
      --memory <MEMORY>
          Maximum memory to use for cache (e.g., "512MiB", "2GB", "1.5GiB") [default: 4GiB]
      --disk-path <DISK_PATH>
          Path to disk cache storage, which may be a directory or block device
      --disk-kind <DISK_KIND>
          Kind of disk cache, which may be a file system or block device [default: fs] [possible values: block, fs]
      --disk-capacity <DISK_CAPACITY>
          Maximum disk cache capacity (e.g., "100GiB") If not specified, up to 80% of the available space will be used
      --iouring
          Use `io_uring` (if available) for disk IO
      --hedge-quantile <HEDGE_QUANTILE>
          Latency quantile for early hedges (0.0-1.0, use 0 to disable early hedging) [default: 0.99]
      --bucket-timeout-ms <BUCKET_TIMEOUT_MS>
          Maximum bucket download time through body validation, in milliseconds [default: 5000]
      --page-timeout-ms <PAGE_TIMEOUT_MS>
          Maximum page download time including fallback, in milliseconds [default: 10000]
      --max-concurrent-hedges <MAX_CONCURRENT_HEDGES>
          Maximum concurrent early hedges across all buckets (0 disables early hedging) [default: 16]
      --hedge-budget-percent <HEDGE_BUDGET_PERCENT>
          Hedge allowance earned per successful page fetch, as a percentage [default: 5]
      --max-inflight-requests <MAX_INFLIGHT_REQUESTS>
          Maximum active backend requests, including speculative copies [default: 1024]
      --max-download-memory <MAX_DOWNLOAD_MEMORY>
          Body memory reserved by active downloads, separate from the cache [default: 1GiB]
      --tls-self
          Use a self-signed certificate for TLS
      --tls-cert <TLS_CERT>
          Path to the TLS certificate file (e.g., cert.pem) Must be used together with --tls-key
      --tls-key <TLS_KEY>
          Path to the private key file (e.g., key.pem) Must be used together with --tls-cert
      --port <PORT>
          Port to listen on [default: 443 if HTTPS configured, otherwise 80 for HTTP]
  -h, --help
          Print help
  -V, --version
          Print version
```

## Development

- [justfile](./justfile) contains commands for [just](https://just.systems/man/en/) doing things
- [AGENTS.md](./AGENTS.md) and symlinks for your favorite coding buddies

Use the nightly Cargo dependency commands so that the seven-day publication cooldown applies:

```bash
cargo +nightly add <crate>
cargo +nightly update
cargo +nightly update -p <crate>
cargo +nightly remove <crate>
cargo +nightly generate-lockfile
```

Use `--locked` with normal build, check, test, run, document, fetch, and metadata commands. The pull request dependency gate verifies each proposed lockfile change before Rust build jobs start.

The [replica simulation harness](docs/simulation/README.md) runs the actual downloader and SDK against modeled faults, finite service queues, and delayed cancellation. It includes reproducible regression cases and explicit statistical campaigns; no production traffic is used.
