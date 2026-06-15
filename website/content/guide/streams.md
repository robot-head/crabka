+++
title = "Streams & Data Formats"
weight = 35
template = "docs/page.html"
+++

`crabka-client-streams` is the KIP-1071 Streams client: it joins a Streams
rebalance group, runs a processing topology, and reads/writes Kafka topics
through pluggable **serdes**. It offers two processing models:

- **Row model** — the Processor API and the high-level DSL (`StreamsApp` /
  `streams_builder`), one record at a time, with `TopologyTestDriver` for
  broker-free tests.
- **Columnar model** — a `ColumnarTopology` whose edges are Polars
  `DataFrame`s, for vectorized aggregation, with `ColumnarTestDriver` for
  broker-free tests.

## Data formats

| Serde / codec | Rust type | Cargo feature | Use it for |
|---|---|---|---|
| `StringSerde` / `I64Serde` / `BytesSerde` | `String` / `i64` / `Bytes` | (built-in) | primitive keys/values |
| `SchemaSerde<T, JsonSerde<T>>` | any `serde` + `schemars::JsonSchema` | (built-in) | Confluent JSON Schema |
| `SchemaSerde<T, ProtobufSerde<T>>` | a prost `Message` | (built-in) | Confluent Protobuf (dynamic via `prost-reflect`) |
| `SchemaSerde<T, AvroSerde<T>>` | `apache_avro::AvroSchema` | (built-in) | Confluent Avro |
| `PolarsIpcSerde` | `polars::DataFrame` | `polars` | columnar values (Arrow IPC) |
| `ArrowIpcSerde` | `arrow::RecordBatch` | `arrow` | arrow-rs interchange |
| `ColumnarSerde<T>` | `columnar::Columnar` | `columnar` | zero-copy native columnar |

Schema serdes resolve schema IDs against a Confluent-compatible registry
(`crabka-schema-registry`); the columnar serdes are self-describing Arrow IPC.

## Getting started

Add the client and pick the columnar features you need:

```toml
[dependencies]
crabka-client-streams = { version = "0.3.6", features = ["polars", "arrow"] }
```

Round-tripping a typed value through a schema serde:

<!-- snippet: client-streams/examples/format_json.rs#json-roundtrip -->
placeholder
<!-- /snippet -->

The idiomatic high-level DSL wires types in via `DefaultSerde`:

<!-- snippet: client-streams/examples/format_dsl.rs#dsl-defaultserde -->
placeholder
<!-- /snippet -->

<!-- snippet: client-streams/examples/format_dsl.rs#dsl-topology -->
placeholder
<!-- /snippet -->

## Worked pipeline: JSON → Protobuf → Arrow → Polars → summary Protobuf

This pipeline ingests order events as JSON, normalizes them to a Protobuf
canonical form, batches them into Arrow, aggregates per user with the Polars
columnar engine, and emits a Protobuf summary — one format at each topic hop:

```text
orders.json   --JSON Schema-->  Stage A  --Protobuf-->  orders.proto
orders.proto  --Protobuf----->  Stage B  --Arrow IPC--> orders.arrow
orders.arrow  --Arrow IPC----->  Stage C (Polars group-by)
(agg rows)    --------------->  Stage D  --Protobuf-->  orders.summary
```

The full source is `crates/client-streams/examples/format_pipeline.rs`; it boots
an in-process broker and Schema Registry and asserts the result, so it runs in
CI as a test.

The shared event type and the Arrow→Polars bridge codec:

<!-- snippet: client-streams/examples/format_pipeline.rs#types -->
placeholder
<!-- /snippet -->

<!-- snippet: client-streams/examples/format_pipeline.rs#arrow-codec -->
placeholder
<!-- /snippet -->

**Stage A — JSON → Protobuf**

<!-- snippet: client-streams/examples/format_pipeline.rs#stage-a-json-proto -->
placeholder
<!-- /snippet -->

**Stage B — Protobuf → Arrow**

<!-- snippet: client-streams/examples/format_pipeline.rs#stage-b-proto-arrow -->
placeholder
<!-- /snippet -->

**Stage C — Arrow → Polars (columnar group-by)**

<!-- snippet: client-streams/examples/format_pipeline.rs#stage-c-arrow-polars -->
placeholder
<!-- /snippet -->

**Stage D — Polars → summary Protobuf**

<!-- snippet: client-streams/examples/format_pipeline.rs#stage-d-polars-proto -->
placeholder
<!-- /snippet -->

**Verifying the rollup**

<!-- snippet: client-streams/examples/format_pipeline.rs#assert -->
placeholder
<!-- /snippet -->
