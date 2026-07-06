# crabka-client-consumer

[![Crates.io](https://img.shields.io/crates/v/crabka-client-consumer.svg)](https://crates.io/crates/crabka-client-consumer)
[![Docs.rs](https://docs.rs/crabka-client-consumer/badge.svg)](https://docs.rs/crabka-client-consumer)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Subscribe-style Kafka consumer client for Rust.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation
of Apache Kafka-compatible infrastructure and clients.

## Overview

`crabka-client-consumer` owns the high-level consumer behavior above
`crabka-client-core`: subscription, group membership, polling, offset commits,
position validation, cooperative shutdown, and share-group consumption. It is
the crate to use for application-style consumers that join Kafka groups and
process records by topic subscription.

Manual single-partition fetches and raw request dispatch stay in
`crabka-client-core`; admin group inspection stays in `crabka-client-admin`.

## Capabilities

- Classic consumer-group lifecycle using `JoinGroup`, `SyncGroup`, `Heartbeat`,
  `Fetch`, `OffsetCommit`, and `LeaveGroup`.
- Range and cooperative-sticky partition assignors.
- `poll`, `commit_sync`, `commit_async`, `seek`, and graceful `close` APIs.
- Leader-epoch position validation for truncation handling.
- KIP-447 `ConsumerGroupMetadata` for transactional producers.
- Share-group consumer support with explicit acknowledgement and commit.

## Kafka Scope

The crate implements subscribe-style consumer flows, including KIP-429
cooperative sticky assignment, KIP-320 leader-epoch validation, KIP-516 offset
wire shapes, KIP-447 consumer group metadata, and KIP-932 share groups.

The public API does not currently expose a JVM-style manual `assign()` consumer.
Use `crabka-client-core` for lower-level partition fetches.

## Install

```sh
cargo add crabka-client-consumer
```

For workspace development, use the path dependency from this repository.

## Usage

Subscribe to a topic, poll records, and commit offsets:

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

- [API documentation](https://docs.rs/crabka-client-consumer)
- [Crabka repository](https://github.com/robot-head/crabka)
- [Kafka compatibility matrix](https://github.com/robot-head/crabka/blob/main/docs/KIP_MATRIX.md)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](https://github.com/robot-head/crabka/blob/main/NOTICE).
