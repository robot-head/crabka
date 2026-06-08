#![cfg(feature = "schema-serde")]
//! Asserts our Avro framing/body matches bytes captured from Confluent's JVM
//! `KafkaAvroSerializer`. The golden in `testdata/schema_serde/avro/order.hex`
//! is a PLACEHOLDER until captured from a real Confluent run (no Docker/cp in
//! this environment), so the test is `#[ignore]`.

use apache_avro::AvroSchema;
use assert2::check;
use crabka_schema_serde::RegistryClient;
use crabka_schema_serde::cache::{CacheConfig, SchemaCache};
use crabka_schema_serde::format::SchemaSerializer;
use crabka_schema_serde::format::avro::AvroSerde;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, AvroSchema)]
struct Order {
    id: String,
    total: f64,
}

#[test]
#[ignore = "golden bytes must be captured from the Confluent JVM KafkaAvroSerializer"]
fn avro_frame_matches_confluent_golden() {
    let golden_hex = include_str!("testdata/schema_serde/avro/order.hex").trim();
    let golden = hex::decode(golden_hex).expect("valid hex");

    let cache = SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default());
    let serde = AvroSerde::<Order>::new(&cache, "orders-value");
    cache.seed_subject_id("orders-value", 1);

    let order = Order {
        id: "o-1".into(),
        total: 9.5,
    };
    let ours = serde.serialize(&order).unwrap();
    check!(ours.as_ref() == golden.as_slice());
}
