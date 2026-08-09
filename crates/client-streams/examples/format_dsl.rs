//! Idiomatic high-level Streams DSL over compile-checked schema serdes.
//!
//! The example reads JSON `OrderEvent`s, normalizes them to a Protobuf
//! `OrderProto`, and writes them out. It needs an external broker and registry to
//! run. By default it only builds the topology.
//! Run: `cargo run -p crabka-client-streams --example format_dsl`
use crabka_client_streams::{DefaultSerde, SchemaSerde, StreamsApp};
use crabka_schema_serde::format::{json::JsonSerde, protobuf::ProtobufSerde};
use serde::{Deserialize, Serialize};

pub const FILE_DESCRIPTOR_SET_BYTES: &[u8] = include_bytes!("gen/file_descriptor_set.bin");
// The shared generated module defines both OrderProto and OrderSummary; this
// example only normalizes into OrderProto.
#[allow(dead_code)]
mod orders {
    include!("gen/orders.rs");
}
use orders::OrderProto;

#[derive(Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct OrderEvent {
    order_id: String,
    user: String,
    amount: f64,
    currency: String,
    ts_ms: i64,
}

fn amount_cents(amount: f64) -> i64 {
    let rounded = (amount * 100.0).round();
    rounded.to_string().parse().unwrap_or_else(|_| {
        if rounded.is_sign_negative() {
            i64::MIN
        } else {
            i64::MAX
        }
    })
}

// docs:begin dsl-defaultserde
impl DefaultSerde for OrderEvent {
    type Serde = SchemaSerde<OrderEvent, JsonSerde<OrderEvent>>;
}
impl DefaultSerde for OrderProto {
    type Serde = SchemaSerde<OrderProto, ProtobufSerde<OrderProto>>;
}
// docs:end dsl-defaultserde

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A process-wide default registry lets the DefaultSerde-backed serdes
    // construct offline; building the topology graph needs no network.
    crabka_schema_serde::set_default_registry(crabka_schema_serde::cache::SchemaCache::new(
        crabka_schema_serde::RegistryClient::new("http://127.0.0.1:8081"),
        crabka_schema_serde::cache::CacheConfig::default(),
    ));

    // docs:begin dsl-topology
    let app = StreamsApp::builder()
        .bootstrap("127.0.0.1:9092")
        .application_id("orders-formats")
        .schema_registry("http://127.0.0.1:8081")
        .build();

    let topology = app.streams_builder();
    topology
        .stream::<String, OrderEvent>(["orders.json"])
        .map_values(|e: &OrderEvent| OrderProto {
            order_id: e.order_id.clone(),
            user: e.user.clone(),
            amount_cents: amount_cents(e.amount),
            currency: e.currency.to_uppercase(),
            ts_ms: e.ts_ms,
        })
        .to("orders.proto");
    // docs:end dsl-topology

    let _ = topology.build("orders-formats")?;
    println!("format_dsl: built");
    Ok(())
}
