// `#[tracing::instrument]` on the DSL runtime deepens this example's `main`
// future past the default type-layout depth limit; raise it.
#![recursion_limit = "512"]
//! JSON Schema schema-serde Streams pipeline using `StreamsApp` + the
//! default-serde DSL. Requires a running broker (`127.0.0.1:9092`) and a
//! Confluent-compatible registry (`http://127.0.0.1:8081`).
//!
//! Reads JSON `Order` records from `orders-json`, doubles each total, and writes
//! them to `orders-json-doubled`. Mirrors the Avro/Protobuf examples — only the
//! serde format differs.
//!
//! Run: `cargo run -p crabka-client-streams --example json_pipeline`

use crabka_client_streams::{DefaultSerde, SchemaSerde, StreamsApp};
use crabka_schema_serde::format::json::JsonSerde;
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
    let app = StreamsApp::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("orders-json")
        .schema_registry("http://127.0.0.1:8081")
        .build();

    let topology = app.streams_builder();
    topology
        .stream::<String, Order>(["orders-json"])
        .map_values(|o: &Order| Order {
            id: o.id.clone(),
            total: o.total * 2.0,
        })
        .to("orders-json-doubled");

    let streams = app.run(topology).await?;
    streams.close().await?;
    Ok(())
}
