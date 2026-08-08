# crabka-protocol

[![Crates.io](https://img.shields.io/crates/v/crabka-protocol.svg)](https://crates.io/crates/crabka-protocol)
[![Docs.rs](https://docs.rs/crabka-protocol/badge.svg)](https://docs.rs/crabka-protocol)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Apache Kafka wire-protocol codec for Rust.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation
of Apache Kafka-compatible infrastructure and clients.

## Overview

`crabka-protocol` is the generated Kafka request/response and record-batch
codec. Crabka clients, broker code, and protocol tests all use it. It contains
no networking and no async runtime assumptions. The caller supplies the bytes,
chooses the Kafka API version, and uses the typed encode/decode traits.

The generated schemas are pinned to Apache Kafka 4.3.0. The codegen emits both
owned message types and borrowed zero-copy decode types for Kafka APIs.

## Capabilities

- `Encode`, `Decode`, and `DecodeBorrow` traits for Kafka wire structs.
- `ProtocolRequest` implementations that carry each request's response type and
  API key.
- Generated `owned::*` request/response modules for owned data.
- Generated `borrowed::*` modules for zero-copy decode from contiguous buffers.
- `ApiKey` registry for Kafka APIs through Kafka 4.3.0.
- Typed v2 `RecordBatch`, `Record`, and record-header support.
- Produce/read framing helpers for record-batch byte streams.
- Optional compression, forwarded through `crabka-compression`.

## Kafka Scope

This crate is wire-protocol and record-codec infrastructure only. It does not
open sockets, maintain connections, implement broker state, or supply producer
or consumer behavior. Use `crabka-client-core` for typed RPC transport, and use
the higher-level client crates for application APIs.

The private `crabka-records-legacy` workspace crate holds the legacy v0/v1
MessageSet conversion. This published crate holds the generated Kafka APIs and
the modern v2 record batches.

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

The default features enable arbitrary generation support and all four
compression codecs. Turn off the default features to select a smaller codec
set:

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
