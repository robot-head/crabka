#![cfg(feature = "schema-serde")]
//! Asserts our JSON framing/body matches bytes captured from Confluent's JVM
//! `KafkaJsonSchemaSerializer`. The golden in
//! `testdata/schema_serde/json/order.hex` is a PLACEHOLDER until captured from
//! a real Confluent run, so the test is `#[ignore]`. NOTE: cross-vendor JSON
//! byte-exactness also depends on field ordering (`serde_json` vs Jackson) — to
//! be reconciled when capturing the real golden.

use assert2::check;
use crabka_schema_serde::RegistryClient;
use crabka_schema_serde::cache::{CacheConfig, SchemaCache};
use crabka_schema_serde::format::SchemaSerializer;
use crabka_schema_serde::format::json::JsonSerde;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct Order {
    id: String,
    total: f64,
}

#[test]
#[ignore = "golden bytes must be captured from the Confluent JVM KafkaJsonSchemaSerializer"]
fn json_frame_matches_confluent_golden() {
    let golden = hex::decode(include_str!("testdata/schema_serde/json/order.hex").trim())
        .expect("valid hex");

    let cache = SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default());
    let serde = JsonSerde::<Order>::new(&cache, "orders-json-value", false);
    cache.seed_subject_id("orders-json-value", 1);

    let order = Order {
        id: "o-1".into(),
        total: 9.5,
    };
    let ours = serde.serialize(&order).unwrap();
    check!(ours.as_ref() == golden.as_slice());
}
