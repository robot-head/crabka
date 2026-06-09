//! JSON Schema schema-serde Streams pipeline using the default-serde DSL.
//! Requires a running broker (`127.0.0.1:9092`) and a Confluent-compatible
//! registry (`http://127.0.0.1:8081`).
//!
//! Reads JSON `Order` records from `orders-json`, doubles each total, and writes
//! them to `orders-json-doubled`. Mirrors the Avro/Protobuf examples — only the
//! serde format differs.
//!
//! Run: `cargo run -p crabka-client-streams --example json_pipeline`

use crabka_client_streams::{DefaultSerde, KafkaStreams, SchemaSerde, StreamsBuilder};
use crabka_schema_serde::cache::{CacheConfig, SchemaCache};
use crabka_schema_serde::format::json::JsonSerde;
use crabka_schema_serde::{RegistryClient, set_default_registry};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, JsonSchema)]
struct Order {
    id: String,
    total: f64,
}

// JSON values resolved against the process default registry. (Payload validation
// is off by default; build `JsonSerde::value(&cache, true)` and wire it with
// `add_source_explicit` to enable it.)
impl DefaultSerde for Order {
    type Serde = SchemaSerde<Order, JsonSerde<Order>>;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache = SchemaCache::new(
        RegistryClient::new("http://127.0.0.1:8081"),
        CacheConfig::default(),
    );
    // Install the registry BEFORE building: the default serdes read it when the
    // DSL constructs them during `build`.
    set_default_registry(cache.clone());

    let builder = StreamsBuilder::new();
    builder
        .stream::<String, Order>(["orders-json"])
        .map_values(|o: &Order| Order {
            id: o.id.clone(),
            total: o.total * 2.0,
        })
        .to("orders-json-doubled");
    let built = builder.build("orders-json")?;

    cache.prewarm().await?;

    let mut streams = KafkaStreams::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("orders-json")
        .topology(built)
        .build()
        .await?;
    streams.close().await?;
    Ok(())
}
