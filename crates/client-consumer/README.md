# crabka-client-consumer

[![Crates.io](https://img.shields.io/crates/v/crabka-client-consumer.svg)](https://crates.io/crates/crabka-client-consumer)
[![Docs.rs](https://docs.rs/crabka-client-consumer/badge.svg)](https://docs.rs/crabka-client-consumer)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Subscribe-style consumer client for Apache Kafka in Rust.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add crabka-client-consumer
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Subscribe to a topic, poll records, then commit offsets:

```rust,no_run
use std::time::Duration;
use crabka_client_consumer::{AutoOffsetReset, Consumer};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let mut consumer = Consumer::builder()
    .bootstrap("127.0.0.1:9092")
    .group_id("orders-worker")
    .client_id("orders-worker-1")
    .subscribe(["orders".to_string()])
    .auto_offset_reset(AutoOffsetReset::Earliest)
    .build()
    .await?;

for record in consumer.poll(Duration::from_secs(1)).await? {
    println!("{}:{}@{}", record.topic, record.partition, record.offset);
}
consumer.commit_sync().await?;
consumer.close().await?;
# Ok(())
# }
```

## Documentation

API documentation is published on [docs.rs/crabka-client-consumer](https://docs.rs/crabka-client-consumer). The repository README contains project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
