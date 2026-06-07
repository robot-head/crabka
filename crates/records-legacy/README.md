# crabka-records-legacy

[![Crates.io](https://img.shields.io/crates/v/crabka-records-legacy.svg)](https://crates.io/crates/crabka-records-legacy)
[![Docs.rs](https://docs.rs/crabka-records-legacy/badge.svg)](https://docs.rs/crabka-records-legacy)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Apache Kafka legacy (v0/v1) `MessageSet` codec, with bridges to and from the v2 `RecordBatch` types.

See the [Kafka protocol docs](https://kafka.apache.org/protocol.html#messageset) for the wire layout this crate implements. v0 carries no per-message timestamp; v1 adds an `i64` timestamp per message (KIP-32). Compression in both is signalled in the low 3 bits of the per-message `attributes` byte, with the compressed payload appearing as a single outer message whose `value` is a nested (uncompressed) MessageSet.

## Quick start

```rust
use bytes::{Bytes, BytesMut};
use crabka_records_legacy::{
    Magic, ParsedRecord, decode_message_set, encode_flat_message_set,
};

let records = vec![ParsedRecord {
    offset: 42,
    timestamp: Some(1_713_000_000_000),
    key: Some(Bytes::from_static(b"order-42")),
    value: Some(Bytes::from_static(b"created")),
}];

let mut buf = BytesMut::new();
encode_flat_message_set(records, Magic::V1, &mut buf);
let decoded = decode_message_set(&mut &buf[..], buf.len()).unwrap();
assert_eq!(decoded[0].offset, 42);
```

## Features

This crate supports the following compression features via `crabka-compression`:
- `gzip`
- `snappy`
- `lz4`
- `zstd`

All compression features are enabled by default.

## MSRV

Rust 1.95.0.

## License

Apache-2.0.
