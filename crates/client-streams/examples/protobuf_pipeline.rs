//! Protobuf schema-serde Streams pipeline. Requires a running broker
//! (`127.0.0.1:9092`) and a Confluent-compatible registry
//! (`http://127.0.0.1:8081`).
//!
//! Run: `cargo run -p crabka-client-streams --features schema-serde --example protobuf_pipeline`

use std::sync::Arc;

use crabka_client_streams::{SchemaPrewarm, SchemaSerde, StreamsMembership, StringSerde, Topology};
use crabka_schema_serde::RegistryClient;
use crabka_schema_serde::cache::{CacheConfig, SchemaCache};
use crabka_schema_serde::format::protobuf::ProtobufSerde;

/// Embedded descriptor set referenced by the generated `Order` (see build.rs).
pub const FILE_DESCRIPTOR_SET_BYTES: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));

#[allow(clippy::all, clippy::pedantic, missing_docs)]
mod order {
    include!(concat!(env!("OUT_DIR"), "/demo.rs"));
}
use order::Order;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache = SchemaCache::new(
        RegistryClient::new("http://127.0.0.1:8081"),
        CacheConfig::default(),
    );

    let in_value = SchemaSerde::new(ProtobufSerde::<Order>::new(&cache, "orders-pb-value"));
    let out_value = SchemaSerde::new(ProtobufSerde::<Order>::new(
        &cache,
        "orders-pb-doubled-value",
    ));

    let mut topo = Topology::new();
    let src = topo.add_source("src", ["orders-pb"], (StringSerde, in_value));
    topo.add_sink("snk", "orders-pb-doubled", [&src], (StringSerde, out_value));
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
