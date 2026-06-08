# crabka-client-streams

[![Crates.io](https://img.shields.io/crates/v/crabka-client-streams.svg)](https://crates.io/crates/crabka-client-streams)
[![Docs.rs](https://docs.rs/crabka-client-streams/badge.svg)](https://docs.rs/crabka-client-streams)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

KIP-1071 Kafka Streams rebalance-protocol client for Apache Kafka in Rust.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```toml
crabka-client-streams = "0.3.2"
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Build and run a simple source-to-sink topology using the KIP-1071 membership client:

```rust,no_run
use std::sync::Arc;
use crabka_client_streams::{StreamsEvent, StreamsMembership, StringSerde, Topology};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let mut topo = Topology::new();
let src = topo.add_source("src", ["input-topic"], (StringSerde, StringSerde));
topo.add_sink("snk", "output-topic", [&src], (StringSerde, StringSerde));
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

## Schema-aware payloads (Avro / Protobuf / JSON)

Enable the `schema-serde` feature to read and write **Confluent-framed** payloads
whose schemas are registered/validated against a Confluent-compatible Schema
Registry (e.g. `crabka-schema-registry`). It wraps the typed serdes from
[`crabka-schema-serde`](../schema-serde) into the Streams `Serde<T>` boundary.

```toml
crabka-client-streams = { version = "0.3.2", features = ["schema-serde"] }
```

```rust,no_run
use std::sync::Arc;
use apache_avro::AvroSchema;
use serde::{Deserialize, Serialize};
use crabka_client_streams::{SchemaPrewarm, SchemaSerde, StreamsMembership, StringSerde, Topology};
use crabka_schema_serde::{RegistryClient, cache::{CacheConfig, SchemaCache}, format::avro::AvroSerde};

#[derive(Clone, Serialize, Deserialize, AvroSchema)]
struct Order { id: String, total: f64 }

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let cache = SchemaCache::new(RegistryClient::new("http://127.0.0.1:8081"), CacheConfig::default());
let value = SchemaSerde::new(AvroSerde::<Order>::new(&cache, "orders-value"));

let mut topo = Topology::new();
let src = topo.add_source("src", ["orders"], (StringSerde, value.clone()));
topo.add_sink("snk", "orders-copy", [&src], (StringSerde, value));
let built = topo.build("orders-avro")?;

// `schema_prewarm` resolves/registers schema ids once at membership start.
let mut membership = StreamsMembership::builder()
    .bootstrap("127.0.0.1:9092")
    .group_id("orders-avro")
    .topology(Arc::new(built))
    .maybe_schema_prewarm(Some(cache as Arc<dyn SchemaPrewarm>))
    .build()
    .await?;
# let _ = &mut membership;
# Ok(()) }
```

Runnable per-format pipelines live under [`examples/`](examples) — run them
against a live broker + registry:

```bash
cargo run -p crabka-client-streams --features schema-serde --example avro_pipeline
cargo run -p crabka-client-streams --features schema-serde --example protobuf_pipeline
cargo run -p crabka-client-streams --features schema-serde --example json_pipeline
```

## Documentation

API documentation is published on [docs.rs/crabka-client-streams](https://docs.rs/crabka-client-streams). The repository README contains project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
