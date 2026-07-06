# crabka-compression

[![Crates.io](https://img.shields.io/crates/v/crabka-compression.svg)](https://crates.io/crates/crabka-compression)
[![Docs.rs](https://docs.rs/crabka-compression/badge.svg)](https://docs.rs/crabka-compression)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Kafka wire-protocol compression codecs for Rust.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation
of Apache Kafka-compatible infrastructure and clients.

## Overview

`crabka-compression` implements the compression formats Apache Kafka uses in
record batches. It is a small, I/O-free crate used by `crabka-protocol`, the
producer, and storage code whenever bytes must match Kafka's wire framing.

The crate exposes a stable `CompressionType` enum that maps to Kafka's record
batch attribute bits and `compress`/`decompress` helpers for payload bytes.

## Codecs

- `gzip` - RFC 1952 gzip streams.
- `snappy` - Kafka's xerial-snappy framing, not Google's Snappy stream format.
- `lz4` - LZ4 frame format with independent blocks and 64 KiB block size.
- `zstd` - plain zstd frame at level 3.
- `none` - pass-through payloads.

## Install

```sh
cargo add crabka-compression
```

For workspace development, use the path dependency from this repository.

## Usage

Compress and decompress a Kafka payload:

```rust
use crabka_compression::{compress, decompress, CompressionType};

let compressed = compress(CompressionType::Snappy, b"hello kafka")?;
let plain = decompress(CompressionType::Snappy, &compressed, 1024)?;

assert_eq!(plain.as_ref(), b"hello kafka");
# Ok::<(), crabka_compression::CompressionError>(())
```

`decompress` requires a caller-provided `max_output` limit to guard against
decompression bombs.

## Cargo Features

All codecs are enabled by default. Disable defaults and opt into only the
codecs you need:

```toml
crabka-compression = { version = "0.3.8", default-features = false, features = ["gzip", "zstd"] }
```

Calling a disabled codec returns `CompressionError::FeatureDisabled`.

## Documentation

- [API documentation](https://docs.rs/crabka-compression)
- [Crabka repository](https://github.com/robot-head/crabka)
- [Kafka compatibility matrix](https://github.com/robot-head/crabka/blob/main/docs/KIP_MATRIX.md)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](https://github.com/robot-head/crabka/blob/main/NOTICE).
