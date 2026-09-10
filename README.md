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
- Multiple headers specify redundant buckets, with the preferred bucket first
- If omitted, `kind` is used as the singular bucket name

See [replica reads](docs/replica-reads.md) for selection, fallback, hedging, and download limits.

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

SDK overrides are bounded by the server's download deadlines; early hedges use one SDK attempt.

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
| `Content-Length` | Number of bytes in response. Omitted for multi-page `GET` responses, which are streamed with chunked transfer encoding (HTTP/1.1) or DATA frames (HTTP/2); `HEAD` and single-page `GET` always include it |
| `Last-Modified` | Timestamp from first page |
| `Content-Type` | Always `application/octet-stream` |
| `C0-Status` | Status for first page |
| `Trailer` | For multi-page `GET`, advertises `C0-Status` as a trailing header field |

`C0-Status` format: `{first}-{last}; {bucket}; {cached_at}`
- Byte range and which bucket was used
- `cached_at` is Unix timestamp with 0 implying a cache miss
- Only first page status is sent as a header; status for subsequent pages follows the body as trailers

HTTP/1.1 clients that want to receive the per-page `C0-Status` trailers MUST send `TE: trailers` with the request. Without it, the server is prohibited by the HTTP/1.1 protocol from emitting the trailer block, so subsequent-page `C0-Status` is silently omitted (the body is unaffected). HTTP/2 clients receive the trailers unconditionally.

#### Example Response

```http
HTTP/1.1 206 Partial Content
Content-Range: bytes 1048576-18874367/52428800
Transfer-Encoding: chunked
Content-Type: application/octet-stream
C0-Status: 1048576-16777215; us-west-videos; 1704067200
Trailer: C0-Status

<chunked data>

C0-Status: 16777216-18874367; us-west-videos; 0
```

The response above spans two pages, so it is streamed without `Content-Length` and advertises `C0-Status` as a trailer field. To receive the trailing `C0-Status` over HTTP/1.1, the request must include `TE: trailers`.

### Monitoring

`GET /stats` returns throughput stats as JSON for load balancing and health checking.

`GET /metrics` returns a more comprehensive set of metrics in Prometheus text format.

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
      --bucket-timeout-ms <BUCKET_TIMEOUT_MS>
          Maximum bucket download time through body validation, in milliseconds [default: 5000]
      --page-timeout-ms <PAGE_TIMEOUT_MS>
          Maximum page download time including fallback, in milliseconds [default: 10000]
      --hedge-budget-percent <HEDGE_BUDGET_PERCENT>
          Hedge allowance per successful page fetch, as a percentage (0 disables early hedging) [default: 5]
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
- [Replica simulation harness](docs/simulation/README.md)

Use the nightly Cargo dependency commands so that the seven-day publication cooldown applies:

```bash
cargo +nightly add <crate>
cargo +nightly update
cargo +nightly update -p <crate>
cargo +nightly remove <crate>
cargo +nightly generate-lockfile
```

Use `--locked` with normal build, check, test, run, document, fetch, and metadata commands. The pull request dependency gate verifies each proposed lockfile change before Rust build jobs start.
