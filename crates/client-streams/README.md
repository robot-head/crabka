# crabka-client-streams

[![Crates.io](https://img.shields.io/crates/v/crabka-client-streams.svg)](https://crates.io/crates/crabka-client-streams)
[![Docs.rs](https://docs.rs/crabka-client-streams/badge.svg)](https://docs.rs/crabka-client-streams)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Kafka Streams-style topology, state-store, and membership runtime for Rust.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation
of Apache Kafka-compatible infrastructure and clients.

## Overview

`crabka-client-streams` is a Kafka Streams-inspired API on top of Crabka's
producer, consumer, and protocol crates. It includes a typed topology DSL,
processor API, state stores, broker-free topology tests, KIP-1071 streams-group
membership, and a managed `KafkaStreams` runtime.

Use this crate when an application needs streaming joins, tables, windows,
stateful processors, or local interactive queries instead of plain producer and
consumer loops.

## Capabilities

- Typed `Topology` and `StreamsBuilder` APIs for source, processor, table, and
  sink graphs.
- `KStream` and `KTable` operators, including joins, cogrouping, windows,
  suppression, and fixed-key processors.
- State stores with changelog metadata and local read-only query handles.
- Broker-free `TopologyTestDriver` for deterministic topology tests.
- KIP-1071 streams-group membership through `StreamsMembership`.
- Managed `KafkaStreams` runtime with configurable processing guarantee and
  state backend.
- Schema-aware serdes through `crabka-schema-serde`.
- Optional dataframe and columnar serde/topology support.

## Kafka Scope

The crate tracks Kafka Streams semantics. It covers KIP-1071 streams-group
membership, KIP-447 EOS v2 integration, KIP-213 foreign-key joins, KIP-150
cogroup, and KIP-450 sliding windows. It also covers KIP-633 stream-stream
left/outer emission, KIP-820 fixed-key processors, KIP-825 suppression, KIP-889
versioned stores, KIP-914 versioned join behavior, and KIP-923 stream-table join
grace.

The default processing guarantee is at-least-once. You must select exactly-once
v2 explicitly. The default state backend is in-memory. Persistent stores need a
durable backend and a state directory. Interactive queries are local
active-store queries. They can fail during rebalance, or when a store is not
local to the process.

## Install

```sh
cargo add crabka-client-streams
```

For workspace development, use the path dependency from this repository.

## Usage

Build and join a streams group for a simple source-to-sink topology:

```rust,no_run
use std::sync::Arc;

use crabka_client_streams::{StreamsEvent, StreamsMembership, Topology};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let mut topology = Topology::new();
let source = topology.add_source::<String, String>("source", ["orders"]);
topology.add_sink("sink", "orders-copy", [&source]);
let built = topology.build("orders-stream")?;

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

## Testing Topologies

Use `TopologyTestDriver` for broker-free tests. It pipes typed input records
through a topology and reads typed output from sink topics. It opens no Kafka
connections.

```rust,no_run
use crabka_client_streams::{Topology, TopologyTestDriver};

let mut topology = Topology::new();
let source = topology.add_source::<String, String>("source", ["orders"]);
topology.add_sink("sink", "orders-copy", [&source]);

let built = topology.build("orders-test")?;
let mut driver = TopologyTestDriver::new(&built)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Schema-Aware Payloads

The `SchemaSerde<T, S>` bridge lets streams read and write Confluent-framed
Avro, Protobuf, or JSON Schema payloads through `crabka-schema-serde`. Serdes
are topic-aware and match Kafka's `serialize(topic, value)` shape. The key and
value roles derive `<topic>-key` or `<topic>-value` subjects from the topic that
the caller passes at runtime.

Runnable examples live under `examples/`:

```sh
cargo run -p crabka-client-streams --example avro_pipeline
cargo run -p crabka-client-streams --example protobuf_pipeline
cargo run -p crabka-client-streams --example json_pipeline
```

## Optional Columnar Support

Columnar and dataframe support is opt-in:

- `polars` - Polars `DataFrame` serde and native columnar topology support.
- `arrow` - Arrow `RecordBatch` serde.
- `columnar` - serde for `columnar::Columnar` values.

```sh
cargo add crabka-client-streams --features polars
```

Columnar topologies operate on batches within a consumed batch. Cross-batch
stateful operations such as accumulated joins and windows need the normal
streams state-store APIs.

## Cargo Features

- `polars` - enables Polars dataframe serde and topology helpers.
- `arrow` - enables Arrow IPC `RecordBatch` serde.
- `columnar` - enables native `columnar` crate serde support.

No optional feature is enabled by default.

## Documentation

- [API documentation](https://docs.rs/crabka-client-streams)
- [Schema serde crate](https://crates.io/crates/crabka-schema-serde)
- [Crabka repository](https://github.com/robot-head/crabka)
- [Kafka compatibility matrix](https://github.com/robot-head/crabka/blob/main/docs/KIP_MATRIX.md)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](https://github.com/robot-head/crabka/blob/main/NOTICE).
