//! JSON Schema schema-serde Streams pipeline. Requires a running broker
//! (`127.0.0.1:9092`) and a Confluent-compatible registry
//! (`http://127.0.0.1:8081`).
//!
//! Run: `cargo run -p crabka-client-streams --features schema-serde --example json_pipeline`

use std::sync::Arc;

use crabka_client_streams::{SchemaPrewarm, SchemaSerde, StreamsMembership, StringSerde, Topology};
use crabka_schema_serde::RegistryClient;
use crabka_schema_serde::cache::{CacheConfig, SchemaCache};
use crabka_schema_serde::format::json::JsonSerde;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct Order {
    id: String,
    total: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache = SchemaCache::new(
        RegistryClient::new("http://127.0.0.1:8081"),
        CacheConfig::default(),
    );

    let in_value = SchemaSerde::new(JsonSerde::<Order>::new(&cache, "orders-json-value", true));
    let out_value = SchemaSerde::new(JsonSerde::<Order>::new(
        &cache,
        "orders-json-doubled-value",
        true,
    ));

    let mut topo = Topology::new();
    let src = topo.add_source("src", ["orders-json"], (StringSerde, in_value));
    topo.add_sink(
        "snk",
        "orders-json-doubled",
        [&src],
        (StringSerde, out_value),
    );
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
