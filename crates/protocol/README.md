# crabka-protocol

[![Crates.io](https://img.shields.io/crates/v/crabka-protocol.svg)](https://crates.io/crates/crabka-protocol)
[![Docs.rs](https://docs.rs/crabka-protocol/badge.svg)](https://docs.rs/crabka-protocol)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Apache Kafka wire-protocol codec for Rust.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation
of Apache Kafka-compatible infrastructure and clients.

## Overview

`crabka-protocol` is the generated Kafka request/response and record-batch
codec used by Crabka clients, broker code, and protocol tests. It contains no
networking and no async runtime assumptions: callers provide bytes, choose the
Kafka API version, and use typed encode/decode traits.

The generated schemas are pinned to Apache Kafka 4.3.0. Both owned message
types and borrowed zero-copy decode types are generated for Kafka APIs.

## Capabilities

- `Encode`, `Decode`, and `DecodeBorrow` traits for Kafka wire structs.
- `ProtocolRequest` implementations that carry each request's response type and
  API key.
- Generated `owned::*` request/response modules for owned data.
- Generated `borrowed::*` modules for zero-copy decode from contiguous buffers.
- `ApiKey` registry for Kafka APIs through Kafka 4.3.0.
- Typed v2 `RecordBatch`, `Record`, and record-header support.
- Produce/read framing helpers for record-batch byte streams.
- Optional compression forwarding through `crabka-compression`.

## Kafka Scope

This crate is wire-protocol and record-codec infrastructure only. It does not
open sockets, maintain connections, implement broker state, or provide producer
or consumer behavior. Use `crabka-client-core` for typed RPC transport and the
higher-level client crates for application APIs.

Legacy v0/v1 MessageSet conversion lives in the private `crabka-records-legacy`
workspace crate; this published crate focuses on generated Kafka APIs and modern
v2 record batches.

## Install

```sh
cargo add crabka-protocol
cargo add bytes
```

For workspace development, use the path dependency from this repository.

## Usage

Encode and decode an `ApiVersionsRequest` for a specific Kafka API version:

```rust
use bytes::BytesMut;
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::{Decode, Encode};

let request = ApiVersionsRequest::default();
let version = 4;

let mut buf = BytesMut::with_capacity(request.encoded_len(version));
request.encode(&mut buf, version)?;

let mut input = &buf[..];
let decoded = ApiVersionsRequest::decode(&mut input, version)?;

assert_eq!(decoded, request);
# Ok::<(), crabka_protocol::ProtocolError>(())
```

## Cargo Features

Default features enable arbitrary generation support and all four compression
codecs. Disable defaults to opt into a smaller codec set:

```toml
crabka-protocol = { version = "0.3.8", default-features = false, features = ["snappy", "zstd"] }
```

- `arbitrary` - enables `arbitrary` implementations for generated/test data.
- `gzip`, `snappy`, `lz4`, `zstd` - forward to `crabka-compression` codecs.

## Documentation

- [API documentation](https://docs.rs/crabka-protocol)
- [Crabka repository](https://github.com/robot-head/crabka)
- [Kafka compatibility matrix](https://github.com/robot-head/crabka/blob/main/docs/KIP_MATRIX.md)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](https://github.com/robot-head/crabka/blob/main/NOTICE).
