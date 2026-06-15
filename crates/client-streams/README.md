# crabka-client-streams

[![Crates.io](https://img.shields.io/crates/v/crabka-client-streams.svg)](https://crates.io/crates/crabka-client-streams)
[![Docs.rs](https://docs.rs/crabka-client-streams/badge.svg)](https://docs.rs/crabka-client-streams)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

KIP-1071 Kafka Streams rebalance-protocol client for Apache Kafka in Rust.

This crate is part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add crabka-client-streams
```

For workspace development, use the path dependency from this repository instead.

## Usage example

Build and run a simple source-to-sink topology using the KIP-1071 membership client:

```rust,no_run
use std::sync::Arc;
use crabka_client_streams::{StreamsEvent, StreamsMembership, Topology};

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut topo = Topology::new();
    let src = topo.add_source::<String, String>("src", ["input-topic"]);
    topo.add_sink("snk", "output-topic", [&src]);
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
    Ok(())
}
```

## Schema-aware payloads (Avro / Protobuf / JSON)

Read and write **Confluent-framed** payloads whose schemas are
registered/validated against a Confluent-compatible Schema Registry (e.g.
`crabka-schema-registry`) — built in, no feature flag. The typed serdes from
[`crabka-schema-serde`](../schema-serde) plug straight into the Streams
`Serde<T>` boundary.

The serdes are **topic-aware** (like JVM Kafka's `serialize(topic, data)`): a serde
carries its key/value role and derives its subject (`<topic>-value` / `<topic>-key`)
from the topic. Declare a type's default serde once and use the plain
`add_source`/`add_sink`:

```rust,no_run
use std::sync::Arc;
use apache_avro::AvroSchema;
use serde::{Deserialize, Serialize};
use crabka_client_streams::{DefaultSerde, SchemaPrewarm, SchemaSerde, StreamsMembership, Topology};
use crabka_schema_serde::{RegistryClient, set_default_registry, cache::{CacheConfig, SchemaCache}, format::avro::AvroSerde};

#[derive(Clone, Serialize, Deserialize, AvroSchema)]
struct Order { id: String, total: f64 }

// Order's default serde: Avro values from the process default registry.
impl DefaultSerde for Order {
    type Serde = SchemaSerde<Order, AvroSerde<Order>>;
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cache = SchemaCache::new(RegistryClient::new("http://127.0.0.1:8081"), CacheConfig::default());
    set_default_registry(cache.clone());

    let mut topo = Topology::new();
    let src = topo.add_source::<String, Order>("src", ["orders"]);
    topo.add_sink("snk", "orders-copy", [&src]);
    let built = topo.build("orders-avro")?;

    // `schema_prewarm` resolves/registers schema ids once at membership start.
    let mut membership = StreamsMembership::builder()
        .bootstrap("127.0.0.1:9092")
        .group_id("orders-avro")
        .topology(Arc::new(built))
        .maybe_schema_prewarm(Some(cache as Arc<dyn SchemaPrewarm>))
        .build()
        .await?;

    Ok(())
}
```

For keys, per-topic subjects, or validation, construct the serde explicitly
(`AvroSerde::<T>::value(&cache)` / `::key(&cache)`) and use
`add_source_explicit`/`add_sink_explicit` with `(key_serde, value_serde)`.

Runnable per-format pipelines live under [`examples/`](examples) — run them
against a live broker + registry:

```bash
cargo run -p crabka-client-streams --example avro_pipeline
cargo run -p crabka-client-streams --example protobuf_pipeline
cargo run -p crabka-client-streams --example json_pipeline
```

## Columnar / DataFrame support

Process records as columns instead of one-at-a-time rows. This is **opt-in**
behind three cargo features, all **off by default**:

- `polars` — polars `DataFrame` serde and the native columnar topology.
- `arrow` — arrow-rs `RecordBatch` serde. (The original design called for
  `minarrow`, but it was substituted with arrow-rs because `minarrow` requires
  nightly Rust; this crate stays on stable.)
- `columnar` — native serde for [frankmcsherry's `columnar`](https://crates.io/crates/columnar)
  types.

```sh
cargo add crabka-client-streams --features polars
```

### Columnar serdes

Three serdes plug straight into the Streams `Serde<T>` boundary, each
**topic-aware** like the schema serdes (`serialize(topic, data)`):

- `PolarsIpcSerde` — `Serde<DataFrame>`, Arrow-IPC framed (feature `polars`).
- `ArrowIpcSerde` — `Serde<RecordBatch>`, Arrow-IPC framed (feature `arrow`).
- `ColumnarSerde<T>` — `Serde<T>` for any `columnar::Columnar` type (feature `columnar`).

```rust,no_run
use crabka_client_streams::Serde;
use crabka_client_streams::columnar::serde::polars::PolarsIpcSerde;
use polars::prelude::*;

let df = df!("id" => ["a", "b"], "total" => [1.0_f64, 2.5]).unwrap();
let bytes = PolarsIpcSerde.serialize("orders", &df);
let back = PolarsIpcSerde.deserialize("orders", &bytes).unwrap();
assert!(back.equals(&df));
```

### Native columnar topology (feature `polars`)

`ColumnarTopology` builds a source → operator → sink graph whose **edges carry
polars `DataFrame`s**. A `BatchCodec` bridges Kafka records ↔ `DataFrame` at
each source/sink; two are provided:

- `RowCodec` — rows stay standard Kafka records (key/value decoded via a
  `RowBridge`, e.g. `JsonRowBridge`); the codec assembles a column-per-field
  `DataFrame` for a batch.
- `BlobCodec` — each record's *value* is itself an Arrow-IPC `DataFrame`; the
  codec vstacks per-record frames into the batch frame and re-chunks output to
  stay under Kafka's record-size limit.

Every assembled `DataFrame` carries reserved metadata columns —
`__key`, `__timestamp`, `__partition`, `__offset` — so the sink codec can
faithfully reconstruct records and the runtime can commit offsets. Payload
columns may not use these names.

Operators are expressed with polars `Expr`s via `BuiltinOp`: `Filter`,
`Select`, `WithColumns`, and `GroupByAgg { keys, aggs }`. The topology runs on
the existing broker runtime through `run_partition_once`, or broker-free in
tests with `ColumnarTestDriver`:

```rust,no_run
use crabka_client_streams::Serde;
use crabka_client_streams::columnar::serde::polars::PolarsIpcSerde;
use crabka_client_streams::columnar::topology::codec::{BlobCodec, ConsumedRecord};
use crabka_client_streams::columnar::topology::operator::BuiltinOp;
use crabka_client_streams::columnar::topology::{ColumnarTestDriver, ColumnarTopology};
use polars::prelude::*;

let mut topo = ColumnarTopology::new();
let src = topo.add_source("src", ["txns"], BlobCodec::default());
let agg = topo.add_operator(
    "sum-by-user",
    BuiltinOp::GroupByAgg {
        keys: vec![col("user")],
        aggs: vec![col("amount").sum().alias("total")],
    },
    src,
);
topo.add_sink("out", "txn-totals", BlobCodec::default(), agg);

let mut driver = ColumnarTestDriver::new(&topo).unwrap();
let df = df!("user" => ["a", "a", "b"], "amount" => [5_i64, 3, 9]).unwrap();
driver
    .pipe_batch("txns", vec![ConsumedRecord {
        key: None,
        value: PolarsIpcSerde.serialize("txns", &df),
        timestamp: 0,
        partition: 0,
        offset: 0,
    }])
    .unwrap();
```

> **Within-batch only.** Operators apply to one consumed batch at a time.
> Cross-batch *stateful* operations — joins, windows, and aggregations that
> accumulate across batches — are **not yet implemented** and are a named
> follow-up. `GroupByAgg` aggregates only the rows present in the current batch.

### Examples

```bash
cargo run -p crabka-client-streams --features polars --example dataframe_serde
cargo run -p crabka-client-streams --features polars --example polars_pipeline
```

## Documentation

API documentation is published on [docs.rs/crabka-client-streams](https://docs.rs/crabka-client-streams). The repository README contains project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
