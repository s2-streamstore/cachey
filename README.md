# Cachey

A high-performance read-through cache for S3-compatible object storage.

- Simple HTTP API.
- Hybrid memory + disk cache powered by [foyer](https://foyer.rs/).
- Fixed 16 MiB page size – maps requested range to page-aligned lookups.
- Coalesces concurrent requests for the same page.
- Limits intra-request concurrency to 2 page fetches.
- Makes hedged requests to manage high tail latency of object storage.
- Can attempt redundant buckets for a given object.

## API

### Fetching data

Non-standard HTTP headers are prefixed with `C0-`.

Request:
- `HEAD|GET /fetch/{kind}/{object}`
  - `kind` and `object` together form the cache key.
  - `kind` can be a string upto 64 characters that uniquely identifies the set of buckets the object is stored on.
  - `object` is the object key in the object store.

Headers:
- `Range` **required**
  - Only supported form is `bytes={first}-{last}`.
- `C0-Bucket` **optional**
  - Multiple occurrences indicate the set of buckets the object is stored on, in client's preference order.
  - If none provided, `kind` is interpreted as singular bucket for the object.
  - The client's preference may not be respected based on recent operational statistics i.e. latency and error rates.
  - At most 2 buckets will be attempted, including when an object was not found.

The service will map each request to page-aligned ranges per object.

Standard HTTP response semantics can be expected, and status code and headers are sent eagerly based on the first page read.
- Codes: 206, 404, ..
- Headers:
  - `Content-Range`
  - `Content-Length`
  - `Last-Modified` of first page
  - `Content-Type` is always `application/octet-stream`
  - `C0-Status: {first}-{last}; {bucket}; {cached_at}`
    - Byte range, source bucket, and whether it was a hit or miss
    - `cached_at` is Unix epoch timestamp in seconds that will be 0 in case of a miss
    - Sent as a header for the first page accessed only; status for all pages follows as trailers after the body

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
