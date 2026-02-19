# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
