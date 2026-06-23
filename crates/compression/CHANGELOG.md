# Changelog

All notable changes to `crabka-compression` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.8] — 2026-06-23


## [0.3.7] — 2026-06-17


### <!-- 0 -->🚀 Features


- Scaffold rules_rust client build + lean client_minimal facade ([#570](https://github.com/robot-head/crabka/pull/570))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Add cargo-mutants incremental mutation testing ([#550](https://github.com/robot-head/crabka/pull/550))


## [0.3.6] — 2026-06-13


## [0.3.5] — 2026-06-12


## [0.1.1] — 2026-05-29


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Expand microbenchmark coverage across crates ([#138](https://github.com/robot-head/crabka/pull/138))


## [0.1.0] — 2026-05-11

### Added

- gzip via `flate2` rust_backend.
- snappy with xerial-snappy framing over `snap` raw blocks.
- lz4 frame format with independent blocks via `lz4_flex`.
- zstd via `zstd-sys`.
- Free-function API parameterised on a `CompressionType` enum matching
  Kafka's record-batch attribute bits.
- Per-codec Cargo features (default-enabled); disabled codecs return
  `CompressionError::FeatureDisabled` at runtime.
- Differential testing against Apache Kafka's compression codecs for
  every codec, both directions.

### MSRV

- Rust 1.95.0.
