# crabka-client-producer

[![Crates.io](https://img.shields.io/crates/v/crabka-client-producer.svg)](https://crates.io/crates/crabka-client-producer)
[![Docs.rs](https://docs.rs/crabka-client-producer/badge.svg)](https://docs.rs/crabka-client-producer)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Idempotent producer client for Apache Kafka in Rust.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```toml
crabka-client-producer = "0.3.1"
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Produce an idempotent record and wait for its delivery metadata:

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

let delivered = producer.send(ProducerRecord {
    topic: "orders".into(),
    key: Some(Bytes::from_static(b"order-1")),
    value: Some(Bytes::from_static(br#"{"status":"created"}"#)),
    ..Default::default()
}).await.await??;

println!("wrote offset {}", delivered.offset);
producer.close().await?;
# Ok(())
# }
```

## Documentation

API documentation is published on [docs.rs/crabka-client-producer](https://docs.rs/crabka-client-producer). The repository README contains project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
