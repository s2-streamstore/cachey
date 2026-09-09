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

### Latency and hedging

A bucket fetch starts with its primary SDK operation and ends when the primary/hedge race produces validated page data or a terminal error. Its duration includes SDK retries and backoff, the wait before starting a hedge, body transfer, and validation. Each completed bucket fetch contributes one outcome to bucket stats: a successful fetch adds one latency sample; a failed fetch, including a deadline expiry, updates the error rate and consecutive-failure count. A failed peer rescued by its sibling is one successful bucket fetch. Canceled peers and caller-canceled bucket fetches add no observations.

The hedge timer uses the configured quantile of these successful bucket-fetch durations. A winning hedge is measured from the original primary start, so it cannot contribute a sample shorter than the time spent waiting to launch it. These samples describe latency delivered with the current hedging policy; they do not estimate how long canceled primary requests would have taken. There is no hedge until the bucket has a successful latency sample, and `--hedge-quantile 0` disables hedging. Latency snapshots refresh at most once per second.

Hedges share a success-funded budget across the downloader and all its clones. The default allowance is one startup hedge plus one credit per 20 successful bucket fetches (`--hedge-budget-percent 5`). Both the global budget and the target bucket's budget must allow a hedge. Stored credits and concurrent hedges are capped at 16 globally (`--max-concurrent-hedges`) and two per bucket, allowing bounded bursts. Failures and cancellations earn no credit; canceling a hedge releases its concurrency slot but does not refund its spent credit. A hedge denied by the budget is skipped. Setting either budget option to zero disables hedging.

| Measurement | Boundary |
|------------|----------|
| `cachey_bucket_latency_mean_seconds` | Mean successful bucket-fetch duration; used in bucket ranking. |
| `cachey_bucket_latency_hedge_seconds` | Successful bucket-fetch quantile used as the hedge delay; zero when hedging is disabled. |
| `cachey_bucket_error_rate` / `cachey_bucket_consecutive_failures` | Completed bucket-fetch outcomes after resolving any hedge. |
| `cachey_page_download_latency_seconds` | Successful page download across both buckets, including time spent failing the first bucket. |
| `cachey_first_chunk_latency_seconds` | Successful HTTP handler's time to its first available chunk, including cache lookup or waiting for a coalesced fill. |

Bucket fallback has its own clock and stats: the fallback bucket is not charged for the failed first bucket. The Rust `DownloadOutput` carries total `latency` and a `hedged` flag for a hedge started in either bucket; `ObjectPiece` carries the returned data and object metadata. The `hedged` page counter counts successful page downloads with that flag, including a primary winner or a hedge in a failed first bucket. Failed page downloads and client response-body transmission are not included in the success latency histograms.

Full body transfer increases the measured latency and therefore can increase hedge delays and change bucket rankings. Since [AWS SDK operation timeouts exclude response-body consumption](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/timeouts.html), Cachey also bounds the full bucket race, including retries, backoff, and body validation. The defaults are 5 seconds per bucket (`--bucket-timeout-ms 5000`) and 10 seconds per page download (`--page-timeout-ms 10000`).

When a fallback bucket is available, the first bucket receives at most half the page budget, capped by the bucket timeout. The fallback receives the remaining page time, also capped by the bucket timeout. A bucket deadline cancels both racing requests before fallback starts. The page clock starts before bucket selection and does not restart for fallback. An exhausted download that ends in a timeout returns HTTP 504 for the first chunk; a later timeout ends the response body with an error.

Rust callers can configure `DownloadLimits` through `Downloader::with_limits` before cloning the downloader, or through `ServiceConfig::download_limits`. `Downloader::new` uses the same defaults as the server. Timeouts must be positive, and the hedge budget percentage must be in `0..=100`.

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
          Latency quantile for making hedged requests (0.0-1.0, use 0 to disable hedging) [default: 0.99]
      --bucket-timeout-ms <BUCKET_TIMEOUT_MS>
          Maximum bucket download time through body validation, in milliseconds [default: 5000]
      --page-timeout-ms <PAGE_TIMEOUT_MS>
          Maximum page download time including fallback, in milliseconds [default: 10000]
      --max-concurrent-hedges <MAX_CONCURRENT_HEDGES>
          Maximum concurrent hedges across all buckets (0 disables hedging) [default: 16]
      --hedge-budget-percent <HEDGE_BUDGET_PERCENT>
          Hedge allowance earned per successful bucket fetch, as a percentage [default: 5]
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
