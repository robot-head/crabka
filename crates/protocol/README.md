# crabka-protocol

[![Crates.io](https://img.shields.io/crates/v/crabka-protocol.svg)](https://crates.io/crates/crabka-protocol)
[![Docs.rs](https://docs.rs/crabka-protocol/badge.svg)](https://docs.rs/crabka-protocol)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Apache Kafka wire-protocol codec for Rust. Implements every message
type Apache Kafka 4.3.0 defines (189 messages, 604 `(api_key, version)`
pairs), with byte-level wire compatibility verified against the JVM
`kafka-clients` implementation.

## Quick start

```rust
use bytes::BytesMut;
use crabka_protocol::{Decode, Encode};
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;

let req = ApiVersionsRequest::default();
let mut buf = BytesMut::with_capacity(req.encoded_len(3));
req.encode(&mut buf, 3).unwrap();

let mut cur: &[u8] = &buf;
let decoded = ApiVersionsRequest::decode(&mut cur, 3).unwrap();
assert_eq!(decoded, req);
```

## Features

- **Two flavors per message:** owned (`crate::owned::*`) and zero-copy
  borrowed (`crate::borrowed::*`).
- **Typed `RecordBatch` v2** via `crate::records::*`, with eager
  decompression through `crabka-compression`.
- **Central `ApiKey` enum** listing every Kafka 4.3 API.

## Cargo features

Default features enable all four compression codecs. Disable per-codec
via `--no-default-features` + selective `--features`:

```toml
crabka-protocol = { version = "0.3.1", default-features = false, features = ["snappy", "zstd"] }
```

## MSRV

Rust 1.95.0.

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see `NOTICE`.
