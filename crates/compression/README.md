# crabka-compression

[![Crates.io](https://img.shields.io/crates/v/crabka-compression.svg)](https://crates.io/crates/crabka-compression)
[![Docs.rs](https://docs.rs/crabka-compression/badge.svg)](https://docs.rs/crabka-compression)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Kafka wire-protocol compression codecs for Rust. Implements the four
codecs Apache Kafka uses on the wire — gzip, snappy, lz4, zstd — with
byte-level wire compatibility verified against the JVM `kafka-clients`
implementation.

## Quick start

```rust
use crabka_compression::{compress, decompress, CompressionType};

let bytes = compress(CompressionType::Snappy, b"hello kafka").unwrap();
let back  = decompress(CompressionType::Snappy, &bytes).unwrap();
assert_eq!(back.as_ref(), b"hello kafka");
```

## Features

Default features enable all four codecs. Disable individually:

```toml
crabka-compression = { version = "0.1", default-features = false, features = ["gzip", "zstd"] }
```

Calling a codec whose feature is off returns
`CompressionError::FeatureDisabled`.

## Kafka-specific framing

- **Snappy** uses xerial-snappy framing (Kafka does not use Google's
  official Snappy stream format).
- **LZ4** uses the LZ4 frame format with independent blocks, 64 KiB
  block size, no checksums.
- **Gzip** is plain RFC-1952.
- **Zstd** is plain zstd frame at level 3.

## MSRV

Rust 1.95.0.

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see `NOTICE`.
