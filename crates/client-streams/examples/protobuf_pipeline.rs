// `#[tracing::instrument]` on the DSL runtime deepens this example's `main`
// future past the default type-layout depth limit; raise it.
#![recursion_limit = "512"]
//! Protobuf schema-serde Streams pipeline that uses `StreamsApp` and the
//! default-serde DSL. It needs a running broker at `127.0.0.1:9092` and a
//! Confluent-compatible registry at `http://127.0.0.1:8081`.
//!
//! The pipeline reads Protobuf `Order` records from `orders-pb`, doubles each
//! total, and writes them to `orders-pb-doubled`. It matches the Avro and JSON
//! examples, and only the serde format differs.
//!
//! Run: `cargo run -p crabka-client-streams --example protobuf_pipeline`

use crabka_client_streams::{DefaultSerde, SchemaSerde, StreamsApp};
use crabka_schema_serde::format::protobuf::ProtobufSerde;

/// Embedded descriptor set that the generated `Order` references. Regenerate it
/// with examples/gen/regenerate.sh.
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
    let app = StreamsApp::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("orders-pb")
        .schema_registry("http://127.0.0.1:8081")
        .build();

    let topology = app.streams_builder();
    topology
        .stream::<String, Order>(["orders-pb"])
        .map_values(|o: &Order| Order {
            id: o.id.clone(),
            total: o.total * 2.0,
        })
        .to("orders-pb-doubled");

    let streams = app.run(topology).await?;
    streams.close().await?;
    Ok(())
}
