# Changelog

All notable changes to `crabka-protocol` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2026-05-11

### Added

- Wire protocol codec for Apache Kafka 4.2.0.
- Owned + borrowed flavors for every active Kafka 4.2 message (189
  message types across 604 supported `(api_key, version)` pairs).
- Typed `RecordBatch` v2 decoder/encoder with `zerocopy` header
  reinterpretation and `crabka-compression` integration.
- Central `ApiKey` enum listing every Kafka 4.2 API.
- Differential testing against `kafka-clients` 4.2.0 for every active
  `(api_key, version)` pair — all byte-equal.

### Supported Kafka versions

- Wire protocol: 4.2.0.

### MSRV

- Rust 1.95.0.
