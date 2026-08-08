# crabka-schema-serde

[![Crates.io](https://img.shields.io/crates/v/crabka-schema-serde.svg)](https://crates.io/crates/crabka-schema-serde)
[![Docs.rs](https://docs.rs/crabka-schema-serde/badge.svg)](https://docs.rs/crabka-schema-serde)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Confluent-compatible schema serializers and deserializers for Crabka clients.

Part of [Crabka](https://github.com/robot-head/crabka), a Rust implementation
of Apache Kafka-compatible infrastructure and clients.

## Overview

`crabka-schema-serde` frames typed payloads with the Confluent Schema Registry
wire format. It registers or resolves schemas through a Confluent-compatible
REST API. It also gives clients synchronous hot-path serialize/deserialize
calls, backed by an async `SchemaCache`.

The crate is client-agnostic. It plugs into `crabka-client-streams` and
`crabka-connect`. Applications that produce or consume Confluent-framed Kafka
records directly can also use it.

## Capabilities

- Confluent framing: `magic(0x00) | schema_id(4 BE) | body`.
- Protobuf message-index encoding compatible with Confluent framing.
- Async `RegistryClient` for register, lookup, latest, and schema-by-id calls.
- Shared `SchemaCache` with prewarm for synchronous serialize/deserialize.
- Topic-aware subject names through `TopicNameStrategy`:
  `<topic>-key` and `<topic>-value`.
- Register modes for auto-register, lookup-only, and use-latest workflows.
- Optional typed serdes for Avro, Protobuf, and JSON Schema.

## Schema Formats

| Feature | Type | Schema source |
| --- | --- | --- |
| `avro` | `AvroSerde<T>` | `apache-avro::AvroSchema` plus serde |
| `protobuf` | `ProtobufSerde<T>` | `prost` plus `prost-reflect::ReflectMessage` |
| `json` | `JsonSerde<T>` | `schemars::JsonSchema` plus serde JSON |

No schema format is on by default.

## Install

```sh
cargo add crabka-schema-serde --features avro
cargo add apache-avro
cargo add serde --features derive
```

For workspace development, use the path dependency from this repository.

## Usage

Register an Avro value schema, prewarm the cache, then serialize and
deserialize records synchronously:

```rust,no_run
use apache_avro::AvroSchema;
use crabka_schema_serde::format::avro::AvroSerde;
use crabka_schema_serde::format::{SchemaDeserializer, SchemaSerializer, SchemaSubject};
use crabka_schema_serde::{CacheConfig, RegistryClient, SchemaCache};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, AvroSchema)]
struct Order {
    id: String,
    total: f64,
}

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let cache = SchemaCache::new(
    RegistryClient::new("http://localhost:8081"),
    CacheConfig::default(),
);
let serde = AvroSerde::<Order>::value(&cache);

serde.register_subject("orders");
cache.prewarm().await?;

let bytes = serde.serialize(
    "orders",
    &Order {
        id: "o-1".into(),
        total: 9.5,
    },
)?;
let decoded: Order = serde.deserialize("orders", &bytes)?;
# let _ = decoded;
# Ok(())
# }
```

If deserialization finds an unknown writer schema id, it starts a background
fetch. It returns a retriable `WriterSchemaPending` error until the cache fills.

## Cargo Features

- `avro` - enables Avro schema support.
- `protobuf` - enables Protobuf schema support.
- `json` - enables JSON Schema support.

## Documentation

- [API documentation](https://docs.rs/crabka-schema-serde)
- [Streams integration](https://crates.io/crates/crabka-client-streams)
- [Connector integration](https://crates.io/crates/crabka-connect)
- [Crabka repository](https://github.com/robot-head/crabka)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](https://github.com/robot-head/crabka/blob/main/NOTICE).
