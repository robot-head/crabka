//! JSON Schema schema-serde Streams pipeline using the default-serde API.
//! Requires a running broker (`127.0.0.1:9092`) and a Confluent-compatible
//! registry (`http://127.0.0.1:8081`).
//!
//! Run: `cargo run -p crabka-client-streams --features schema-serde --example json_pipeline`

use std::sync::Arc;

use crabka_client_streams::{
    DefaultSerde, SchemaPrewarm, SchemaSerde, StreamsMembership, Topology,
};
use crabka_schema_serde::cache::{CacheConfig, SchemaCache};
use crabka_schema_serde::format::json::JsonSerde;
use crabka_schema_serde::{RegistryClient, set_default_registry};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct Order {
    id: String,
    total: f64,
}

// JSON values resolved against the process default registry (validation off by
// default; use `JsonSerde::value(&cache, true)` + add_source_explicit to enable).
impl DefaultSerde for Order {
    type Serde = SchemaSerde<Order, JsonSerde<Order>>;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache = SchemaCache::new(
        RegistryClient::new("http://127.0.0.1:8081"),
        CacheConfig::default(),
    );
    set_default_registry(cache.clone());

    let mut topo = Topology::new();
    let src = topo.add_source::<String, Order>("src", ["orders-json"]);
    topo.add_sink("snk", "orders-json-doubled", [&src]);
    let built = topo.build("orders-json")?;

    let mut membership = StreamsMembership::builder()
        .bootstrap("127.0.0.1:9092")
        .group_id("orders-json")
        .topology(Arc::new(built))
        .maybe_schema_prewarm(Some(cache as Arc<dyn SchemaPrewarm>))
        .build()
        .await?;

    while let Ok(event) = membership.next_event().await {
        println!("event: {event:?}");
    }
    Ok(())
}
