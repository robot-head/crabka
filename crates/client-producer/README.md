# crabka-client-producer

[![Crates.io](https://img.shields.io/crates/v/crabka-client-producer.svg)](https://crates.io/crates/crabka-client-producer)
[![Docs.rs](https://docs.rs/crabka-client-producer/badge.svg)](https://docs.rs/crabka-client-producer)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Idempotent Kafka producer client for Rust.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation
of Apache Kafka-compatible infrastructure and clients.

## Overview

`crabka-client-producer` provides the high-level producer behavior above
`crabka-client-core`: batching, compression, retries, idempotent sequence
numbers, and transactional production. It accepts raw `bytes::Bytes` keys and
values so applications can choose their own serialization layer, including
`crabka-schema-serde`.

Use this crate for application writes, exactly-once produce flows, and
consume-process-produce transactions.

## Capabilities

- Async producer builder with bootstrap, linger, compression, and ack settings.
- Per-topic/partition batching with sticky/hash partitioning.
- Idempotent producer identity and sequence stamping through `InitProducerId`.
- Retries that preserve producer identity and batch sequence numbers.
- Per-record partition override with `ProducerRecord::partition`.
- Transaction lifecycle: initialize, begin, commit, and abort.
- `send_offsets_to_transaction` for KIP-447 consume-process-produce flows.
- `flush` and graceful `close` APIs.

## Kafka Scope

Idempotence is enabled by default. `acks=One` is raised to `acks=All`, and
`acks=Zero` is rejected when idempotence is enabled. Transaction APIs cover
exactly-once production and the KIP-447 path for committing consumed offsets as
part of a producer transaction.

Serialization is caller-owned. Keys, values, and headers are byte payloads, not
typed schema values.

## Install

```sh
cargo add crabka-client-producer
cargo add bytes
```

For workspace development, use the path dependency from this repository.

## Usage

Produce an idempotent record and wait for delivery metadata:

```rust,no_run
use std::time::Duration;

use bytes::Bytes;
use crabka_client_producer::{Acks, Compression, Producer, ProducerRecord};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let producer = Producer::builder()
    .bootstrap("127.0.0.1:9092")
    .compression(Compression::Lz4)
    .acks(Acks::All)
    .linger(Duration::from_millis(5))
    .build()
    .await?;

let delivered = producer
    .send(ProducerRecord {
        topic: "orders".into(),
        key: Some(Bytes::from_static(b"order-1")),
        value: Some(Bytes::from_static(br#"{"status":"created"}"#)),
        ..Default::default()
    })
    .await
    .await??;

println!("wrote offset {}", delivered.offset);
producer.close().await?;
# Ok(())
# }
```

## Documentation

- [API documentation](https://docs.rs/crabka-client-producer)
- [Crabka repository](https://github.com/robot-head/crabka)
- [Kafka compatibility matrix](https://github.com/robot-head/crabka/blob/main/docs/KIP_MATRIX.md)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](https://github.com/robot-head/crabka/blob/main/NOTICE).
