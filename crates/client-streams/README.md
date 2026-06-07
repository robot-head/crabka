# crabka-client-streams

[![Crates.io](https://img.shields.io/crates/v/crabka-client-streams.svg)](https://crates.io/crates/crabka-client-streams)
[![Docs.rs](https://docs.rs/crabka-client-streams/badge.svg)](https://docs.rs/crabka-client-streams)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

KIP-1071 Kafka Streams rebalance-protocol client for Apache Kafka in Rust.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```toml
crabka-client-streams = "0.3.1"
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Build and run a simple source-to-sink topology using the KIP-1071 membership client:

```rust,no_run
use std::sync::Arc;
use crabka_client_streams::{Consumed, Produced, StreamsEvent, StreamsMembership, StringSerde, Topology};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let mut topo = Topology::new();
let src = topo.add_source("src", ["input-topic"], Consumed::with(StringSerde, StringSerde));
topo.add_sink("snk", "output-topic", [&src], Produced::with(StringSerde, StringSerde));
let built = topo.build("orders-stream")?;

let mut membership = StreamsMembership::builder()
    .bootstrap("127.0.0.1:9092")
    .group_id("orders-stream")
    .topology(Arc::new(built))
    .build()
    .await?;

if let StreamsEvent::Assigned(assignment) = membership.next_event().await? {
    println!("active tasks: {:?}", assignment.active);
}
# Ok(())
# }
```

## Documentation

API documentation is published on [docs.rs/crabka-client-streams](https://docs.rs/crabka-client-streams). The repository README contains project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
