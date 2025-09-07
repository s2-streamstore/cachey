# Cachey

A high-performance read-through cache for S3-compatible object storage.

- Simple HTTP API.
- Hybrid memory + disk cache powered by [foyer](https://foyer.rs/).
- Fixed 16 MiB page size – maps requested range to page-aligned lookups.
- Coalesces concurrent requests for the same page.
- Makes hedged requests to manage tail latency of object storage.
- Can attempt redundant buckets for a given object.

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
| `Range` | ✅ | Byte range in format `bytes={first}-{last}` |
| `C0-Bucket` | ❌ | Bucket(s) containing the object |
| `C0-Upstream` | ❌ | Override S3 request config |

`C0-Bucket` behavior:
- Multiple headers indicate bucket preference order
- If omitted, `kind` is used as the singular bucket name
- Client preference may be overridden based on internal latency/error stats
- At most 2 buckets attempted per page miss

`C0-Upstream` overrides:
Space-separated key-value pairs to override S3 request configuration, per page miss.
- `ot=<ms>` Operation timeout
- `oat=<ms>` Operation attempt timeout
- `ma=<num>` Maximum attempts
- `ib=<ms>` Initial backoff duration
- `mb=<ms>` Maximum backoff duration

#### Example Request

```http
GET /fetch/prod-videos/movie-2024.mp4 HTTP/1.1
Range: bytes=1048576-18874367
C0-Bucket: us-west-videos
C0-Bucket: us-east-videos-backup
C0-Upstream: ot=1500 ma=3
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
- `cached_at` is Unix timestamp (0 = cache miss)
- Only first page status is sent as a header; status for subsequent pages follows the body as trailers.

#### Example Response

```http
HTTP/1.1 206 Partial Content
Content-Range: bytes 1048576-18874367/52428800
Content-Length: 17825792
Content-Type: application/octet-stream
C0-Status: 1048576-16777215; us-west-videos; 1704067200

[binary data]

C0-Status: 16777216-18874367; us-west-videos; 0
```

### Monitoring

`GET /stats` returns throughput stats as JSON for load balancing and health checking.

`GET /metrics` returns a more comprehensive set of metrics in Prometheus text format.

## Command line

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
      --hedge-quantile <HEDGE_QUANTILE>
          Latency quantile for making hedged requests (0.0-1.0, use 0 to disable hedging) [default: 0.99]
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
