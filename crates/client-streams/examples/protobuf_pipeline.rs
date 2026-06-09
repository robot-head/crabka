//! Protobuf schema-serde Streams pipeline using the default-serde DSL. Requires
//! a running broker (`127.0.0.1:9092`) and a Confluent-compatible registry
//! (`http://127.0.0.1:8081`).
//!
//! Reads Protobuf `Order` records from `orders-pb`, doubles each total, and
//! writes them to `orders-pb-doubled`. Mirrors the Avro/JSON examples — only the
//! serde format differs.
//!
//! Run: `cargo run -p crabka-client-streams --example protobuf_pipeline`

use crabka_client_streams::{DefaultSerde, KafkaStreams, SchemaSerde, StreamsBuilder};
use crabka_schema_serde::cache::{CacheConfig, SchemaCache};
use crabka_schema_serde::format::protobuf::ProtobufSerde;
use crabka_schema_serde::{RegistryClient, set_default_registry};

/// Embedded descriptor set referenced by the generated `Order` (regenerate via
/// examples/gen/regenerate.sh).
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

    let builder = StreamsBuilder::new();
    builder
        .stream::<String, Order>(["orders-pb"])
        .map_values(|o: &Order| Order {
            id: o.id.clone(),
            total: o.total * 2.0,
        })
        .to("orders-pb-doubled");
    let built = builder.build("orders-pb")?;

    cache.prewarm().await?;

    let mut streams = KafkaStreams::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("orders-pb")
        .topology(built)
        .build()
        .await?;
    streams.close().await?;
    Ok(())
}
