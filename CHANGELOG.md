# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.10.6](https://github.com/s2-streamstore/cachey/compare/0.10.5...0.10.6) - 2026-03-27

### Fixed

- refresh latency snapshot at threshold boundary ([#99](https://github.com/s2-streamstore/cachey/pull/99))

## [0.10.5](https://github.com/s2-streamstore/cachey/compare/0.10.4...0.10.5) - 2026-03-12

### Fixed

- record fetch success metrics for ranged GETs ([#93](https://github.com/s2-streamstore/cachey/pull/93))

### Other

- dep updates
- update license file

## [0.10.4](https://github.com/s2-streamstore/cachey/compare/0.10.3...0.10.4) - 2026-02-28

### Fixed

- *(object-store)* make bucket error-rate respond to successes ([#89](https://github.com/s2-streamstore/cachey/pull/89))

### Other

- dep updates + future size fix for clippy ([#91](https://github.com/s2-streamstore/cachey/pull/91))

## [0.10.3](https://github.com/s2-streamstore/cachey/compare/0.10.2...0.10.3) - 2026-02-20

### Fixed

- *(types)* reject control characters in bucket names ([#84](https://github.com/s2-streamstore/cachey/pull/84))
- *(object_store)* merge per-request timeout and retry overrides ([#85](https://github.com/s2-streamstore/cachey/pull/85))
- classify coalesced cache misses in PageGetExecutor ([#82](https://github.com/s2-streamstore/cachey/pull/82))

## [0.10.2](https://github.com/s2-streamstore/cachey/compare/0.10.1...0.10.2) - 2026-02-19

### Fixed

- *(ci)* use buildx imagetools for multi-arch manifest creation ([#79](https://github.com/s2-streamstore/cachey/pull/79))

### Other

- add conventional commit check for PR titles ([#81](https://github.com/s2-streamstore/cachey/pull/81))

## [0.10.1](https://github.com/s2-streamstore/cachey/compare/0.10.0...0.10.1) - 2026-02-19

### Fixed

- *(throughput)* use fractional lookback divisor ([#78](https://github.com/s2-streamstore/cachey/pull/78))
- *(object_store)* prioritize client-ordered buckets and correct decay ([#76](https://github.com/s2-streamstore/cachey/pull/76))

## [0.10.0](https://github.com/s2-streamstore/cachey/compare/0.9.3...0.10.0) - 2026-02-19

### Added

- [**breaking**] use default eviction policy ([#73](https://github.com/s2-streamstore/cachey/pull/73))

### Fixed

- *(downloader)* record bucket success after response validation ([#75](https://github.com/s2-streamstore/cachey/pull/75))

## [0.9.3](https://github.com/s2-streamstore/cachey/compare/0.9.2...0.9.3) - 2026-02-01

### Other

- enable pedantic clippy lints ([#50](https://github.com/s2-streamstore/cachey/pull/50))

## [0.9.2](https://github.com/s2-streamstore/cachey/compare/0.9.1...0.9.2) - 2026-02-01

### Fixed

- avoid panic when range starts past object end ([#61](https://github.com/s2-streamstore/cachey/pull/61))
- *(downloader)* keep hedge alive through body download ([#65](https://github.com/s2-streamstore/cachey/pull/65))
- make Prometheus metrics registry opt-in ([#64](https://github.com/s2-streamstore/cachey/pull/64))
- validate ObjectKind/ObjectKey during deserialization ([#62](https://github.com/s2-streamstore/cachey/pull/62))

### Other

- remove manual release commands from Justfile

## [0.9.1](https://github.com/s2-streamstore/cachey/compare/0.9.0...0.9.1) - 2026-01-31

### Fixed

- simplify sliding throughput bps ([#58](https://github.com/s2-streamstore/cachey/pull/58))

### Other

- setup release-plz for automated releases ([#59](https://github.com/s2-streamstore/cachey/pull/59))
