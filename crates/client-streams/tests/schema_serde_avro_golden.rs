//! Asserts that our Avro framing and body match the bytes captured from
//! Confluent's `AvroSerializer`.
//!
//! `tests/schema-serde-capture/run.sh` captured the golden in
//! `testdata/schema_serde/avro/order.hex` against
//! `mirror.gcr.io/confluentinc/cp-schema-registry`, with schema id 1.

use apache_avro::AvroSchema;
use assert2::check;
use crabka_schema_serde::{
    RegistryClient,
    cache::{CacheConfig, SchemaCache},
    format::{SchemaSerializer, avro::AvroSerde},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, AvroSchema)]
struct Order {
    id: String,
    total: f64,
}

#[test]
fn avro_frame_matches_confluent_golden() {
    let golden_hex = include_str!("testdata/schema_serde/avro/order.hex").trim();
    let golden = hex::decode(golden_hex).expect("valid hex");

    let cache = SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default());
    let serde = AvroSerde::<Order>::value(&cache);
    cache.seed_subject_id("orders-value", 1);

    let order = Order {
        id: "o-1".into(),
        total: 9.5,
    };
    let ours = serde.serialize("orders", &order).unwrap();
    check!(ours.as_ref() == golden.as_slice());
}
