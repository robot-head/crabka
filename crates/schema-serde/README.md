# crabka-schema-serde

[![Crates.io](https://img.shields.io/crates/v/crabka-schema-serde.svg)](https://crates.io/crates/crabka-schema-serde)
[![Docs.rs](https://docs.rs/crabka-schema-serde/badge.svg)](https://docs.rs/crabka-schema-serde)

Client-agnostic, Confluent-compatible schema serializers/deserializers for
[Crabka](https://github.com/robot-head/crabka).

It frames payloads with the Confluent wire format
(`magic(0x00) | schema_id(4 BE) | body`, plus the Protobuf message-index),
registers/resolves schemas against a Confluent-compatible Schema Registry, and
supports **Avro**, **Protobuf**, and **JSON Schema** via optional feature flags.

## Features

| Feature | Enables | Schema source |
|---|---|---|
| `avro` | `AvroSerde<T>` | `apache-avro` `AvroSchema` derive |
| `protobuf` | `ProtobufSerde<T>` | `prost` + `prost-reflect` descriptor → `.proto` |
| `json` | `JsonSerde<T>` | `schemars` `JsonSchema` derive |

```toml
crabka-schema-serde = { version = "0.3.2", features = ["avro"] }
```

## How it works

- A serde is constructed for a key/value **role** (`AvroSerde::<T>::value(&cache)`
  / `::key(&cache)`) and derives its **subject** (`<topic>-value` / `<topic>-key`
  via `TopicNameStrategy`) from the topic passed at (de)serialize time — so one
  serde instance works across topics (like JVM Kafka's `serialize(topic, data)`).
- `register_subject(topic)` interns the local schema into a shared
  [`SchemaCache`]; `SchemaCache::prewarm()` resolves every interned subject's id
  once at startup — auto-registering (Confluent default) or looking up.
- `serialize`/`deserialize` are synchronous: they read ids from the cache. An
  unknown writer id on the deserialize path triggers a non-blocking background
  fetch and returns a retriable error until the cache fills.

## Example (Avro)

```rust,no_run
use std::sync::Arc;
use apache_avro::AvroSchema;
use serde::{Deserialize, Serialize};
use crabka_schema_serde::{RegistryClient, SchemaCache, CacheConfig};
use crabka_schema_serde::format::{avro::AvroSerde, SchemaSerializer, SchemaDeserializer, SchemaSubject};

#[derive(Serialize, Deserialize, AvroSchema)]
struct Order { id: String, total: f64 }

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let cache = SchemaCache::new(RegistryClient::new("http://localhost:8081"), CacheConfig::default());
let serde = AvroSerde::<Order>::value(&cache);
serde.register_subject("orders"); // intern orders-value for pre-warm
cache.prewarm().await?;            // register/resolve the subject id

let bytes = serde.serialize("orders", &Order { id: "o-1".into(), total: 9.5 })?;
let back: Order = serde.deserialize("orders", &bytes)?;
# Ok(()) }
```

For a Kafka Streams integration (the `Serde<T>` bridge + runnable per-format
pipelines), see [`crabka-client-streams`](../client-streams) with its
`schema-serde` feature.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files.
