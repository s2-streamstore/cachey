# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
