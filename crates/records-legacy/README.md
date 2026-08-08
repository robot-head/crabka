# crabka-records-legacy

[![Crates.io](https://img.shields.io/crates/v/crabka-records-legacy.svg)](https://crates.io/crates/crabka-records-legacy)
[![Docs.rs](https://docs.rs/crabka-records-legacy/badge.svg)](https://docs.rs/crabka-records-legacy)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Apache Kafka legacy (v0/v1) `MessageSet` codec, with bridges to and from the v2 `RecordBatch` types.

See the [Kafka message format docs](https://kafka.apache.org/43/implementation/message-format/) for the wire layout that this crate implements. v0 carries no per-message timestamp. v1 adds an `i64` timestamp to each message (KIP-32). Both formats signal compression in the low 3 bits of the per-message `attributes` byte. The compressed payload is a single outer message. The `value` of that outer message is a nested, uncompressed MessageSet.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add crabka-records-legacy
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Encode and decode a Kafka v1 MessageSet:

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

This crate supports these compression features through `crabka-compression`:
- `gzip`
- `snappy`
- `lz4`
- `zstd`

All compression features are on by default.

## MSRV

Rust 1.95.0.

## Documentation

Read the API documentation at [docs.rs/crabka-records-legacy](https://docs.rs/crabka-records-legacy). The repository README contains the project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
