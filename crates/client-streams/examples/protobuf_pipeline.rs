//! Protobuf schema-serde Streams pipeline using the default-serde API. Requires
//! a running broker (`127.0.0.1:9092`) and a Confluent-compatible registry
//! (`http://127.0.0.1:8081`).
//!
//! Run: `cargo run -p crabka-client-streams --example protobuf_pipeline`

use std::sync::Arc;

use crabka_client_streams::{
    DefaultSerde, SchemaPrewarm, SchemaSerde, StreamsMembership, Topology,
};
use crabka_schema_serde::cache::{CacheConfig, SchemaCache};
use crabka_schema_serde::format::protobuf::ProtobufSerde;
use crabka_schema_serde::{RegistryClient, set_default_registry};

/// Embedded descriptor set referenced by the generated `Order` (see examples/gen/regenerate.sh).
pub const FILE_DESCRIPTOR_SET_BYTES: &[u8] = include_bytes!("gen/file_descriptor_set.bin");

mod order {
    include!("gen/order.rs");
}
use order::Order;

// Protobuf values resolved against the process default registry.
impl DefaultSerde for Order {
    type Serde = SchemaSerde<Order, ProtobufSerde<Order>>;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache = SchemaCache::new(
        RegistryClient::new("http://127.0.0.1:8081"),
        CacheConfig::default(),
    );
    set_default_registry(cache.clone());

    let mut topo = Topology::new();
    let src = topo.add_source::<String, Order>("src", ["orders-pb"]);
    topo.add_sink("snk", "orders-pb-doubled", [&src]);
    let built = topo.build("orders-pb")?;

    let mut membership = StreamsMembership::builder()
        .bootstrap("127.0.0.1:9092")
        .group_id("orders-pb")
        .topology(Arc::new(built))
        .maybe_schema_prewarm(Some(cache as Arc<dyn SchemaPrewarm>))
        .build()
        .await?;

    while let Ok(event) = membership.next_event().await {
        println!("event: {event:?}");
    }
    Ok(())
}
