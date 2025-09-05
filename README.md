# Cachey

A high-performance read-through cache for S3-compatible object storage.

- Simple HTTP API.
- Hybrid memory + disk cache powered by [foyer](https://foyer.rs/).
- Internally uses a fixed 16 MiB page size.
- Maps requested range to page-aligned object store fetches in case of miss.
- Concurrent requests for the same page are coalesced.
- Intra-request concurrency of 2 page fetches.
- Hedging to manage high tail latency of object storage.

## API

### Fetching data

Non-standard HTTP headers are prefixed with `C0-`.

Request:
- `HEAD|GET /fetch/{kind}/{object}`
  - `kind` and `object` together form the cache key.
  - `kind` can be a string upto 64 characters that uniquely identifies the set of buckets the object is stored on, and may be the bucket itself if the object is only stored on a single bucket.
  - `object` is the object key in the object store.
- `Range` **required**
  - Only supported form is `bytes={first}-{last}`.
- `C0-Bucket` **optional**
  - Multiple occurrences indicate the set of buckets the object is stored on, in client's preference order.
  - If none provided, `kind` used as singleton set of buckets.
  - The client's preference may not be respected based on recent operational statistics i.e. latency and error rates.
  - At most 2 buckets will be attempted, including when an object was not found.

The service maps each request to page-aligned ranges per object. The page size is currently hardcoded to 16 MiB.

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
    - As a header for first page accessed only, status for subsequent pages follows as trailers after the body

## Monitoring

`GET /stats` returns throughput stats as JSON for load balancing and health checking.

`GET /metrics` returns a more comprehensive set of metrics in Prometheus text format.

### Command line options

```
Usage: server [OPTIONS]

Options:
      --tls-self
          Use a self-signed certificate for TLS
      --tls-cert <TLS_CERT>
          Path to the TLS certificate file (e.g., cert.pem) Must be used together with --tls-key
      --tls-key <TLS_KEY>
          Path to the private key file (e.g., key.pem) Must be used together with --tls-cert
      --port <PORT>
          Port to listen on (default: 443 for HTTPS, 80 for HTTP)
      --cache-memory <CACHE_MEMORY>
          Maximum memory to use for cache (e.g., "512MiB", "2GB", "1.5GiB") Defaults to 4GiB if not specified [default: 4GiB]
      --disk-path <DISK_PATH>
          Path for disk cache storage (directory or block device)
      --disk-kind <DISK_KIND>
          Cache type: file system ("fs") or block device ("block") Defaults to file system if not specified [default: fs] [possible values: block, fs]
      --disk-capacity <DISK_CAPACITY>
          Maximum disk cache capacity (e.g., "100GiB") Defaults to 80% of available space if not specified
      --hedge-latency-quantile <HEDGE_LATENCY_QUANTILE>
          S3 hedge request latency quantile (0.0-1.0, use 0 to disable hedging) Default: 0.99 (99th percentile) [default: 0.99]
  -h, --help
          Print help
  -V, --version
          Print version
```

## Development

See [CLAUDE.md](CLAUDE.md) for development guidelines.

## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
