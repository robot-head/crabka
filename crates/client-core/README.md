# crabka-client-core

[![Crates.io](https://img.shields.io/crates/v/crabka-client-core.svg)](https://crates.io/crates/crabka-client-core)
[![Docs.rs](https://docs.rs/crabka-client-core/badge.svg)](https://docs.rs/crabka-client-core)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Connection management and typed request dispatch for Apache Kafka in Rust.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation
of Apache Kafka-compatible infrastructure and clients.

## Overview

Crabka's higher-level admin, producer, consumer, and streams clients all share
`crabka-client-core` as their transport layer. It opens broker connections,
negotiates API versions, and routes requests by broker id. It sends typed Kafka
requests that `crabka-protocol` generates.

Use this crate to build a specialized Kafka client or test harness. Such a
client needs raw typed RPCs without producer batching, consumer group
management, or admin convenience wrappers.

## Capabilities

- Bootstrap DNS resolution and broker connection management.
- One pooled connection per broker id, plus direct `Connection` use.
- Correlation-id request/response multiplexing.
- API-version negotiation through `ApiVersions`.
- Typed `send` for any `ProtocolRequest` from `crabka-protocol`.
- Broker-routed request dispatch through `Client::broker`.
- Low-level partition fetch and offset-for-leader-epoch helpers.
- Client TLS and SASL configuration through `crabka-security`.

## Boundaries

This crate intentionally does not implement producer batching, idempotence,
transactions, consumer group coordination, commits, admin retries, or topology
runtime behavior. Those behaviors live in `crabka-client-producer`,
`crabka-client-consumer`, `crabka-client-admin`, and `crabka-client-streams`.

Connections use plaintext by default. Configure client security to change this.

## Install

```sh
cargo add crabka-client-core
cargo add crabka-protocol
```

For workspace development, use the path dependency from this repository.

## Usage

Send a typed Kafka protocol request over a negotiated client connection:

```rust,no_run
use crabka_client_core::Client;
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = Client::builder()
    .bootstrap("127.0.0.1:9092")
    .client_id("metadata-prober")
    .build()
    .await?;

let response = client.send(ApiVersionsRequest::default()).await?;
println!("broker returned {} API keys", response.api_keys.len());

client.close();
# Ok(())
# }
```

## Cargo Features

- `mock` - exposes `MockBroker` support for downstream tests.

## Documentation

- [API documentation](https://docs.rs/crabka-client-core)
- [Crabka repository](https://github.com/robot-head/crabka)
- [Kafka compatibility matrix](https://github.com/robot-head/crabka/blob/main/docs/KIP_MATRIX.md)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](https://github.com/robot-head/crabka/blob/main/NOTICE).
