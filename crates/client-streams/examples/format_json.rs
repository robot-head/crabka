//! `JsonSerde` round-trip: a typed value <-> Confluent JSON-Schema wire bytes.
//! Run: `cargo run -p crabka-client-streams --example format_json`
use crabka_client_streams::{SchemaSerde, processor::serde::Serde};
use crabka_schema_serde::{
    RegistryClient,
    cache::{CacheConfig, SchemaCache},
    format::json::JsonSerde,
};
use serde::{Deserialize, Serialize};

// docs:begin json-type
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
struct OrderEvent {
    order_id: String,
    user: String,
    amount: f64,
    currency: String,
    ts_ms: i64,
}
// docs:end json-type

fn main() {
    // docs:begin json-roundtrip
    let cache = SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default());
    cache.seed_subject_id("orders.json-value", 1);
    let serde = SchemaSerde::new(JsonSerde::<OrderEvent>::value(&cache, false));

    let event = OrderEvent {
        order_id: "o-1".into(),
        user: "alice".into(),
        amount: 5.0,
        currency: "USD".into(),
        ts_ms: 1,
    };
    let bytes = serde.serialize("orders.json", &event);
    let back: OrderEvent = serde.deserialize("orders.json", &bytes).unwrap();
    // docs:end json-roundtrip
    assert_eq!(back, event);
    println!("format_json: OK ({} bytes)", bytes.len());
}
