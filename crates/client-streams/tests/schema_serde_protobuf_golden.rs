#![cfg(feature = "schema-serde")]
//! Asserts our Protobuf framing (magic+id+message-index+body) matches bytes
//! captured from Confluent's JVM `KafkaProtobufSerializer`. The golden in
//! `testdata/schema_serde/protobuf/order.hex` is a PLACEHOLDER until captured
//! from a real Confluent run, so the test is `#[ignore]`.

use assert2::check;
use crabka_schema_serde::RegistryClient;
use crabka_schema_serde::cache::{CacheConfig, SchemaCache};
use crabka_schema_serde::format::SchemaSerializer;
use crabka_schema_serde::format::protobuf::ProtobufSerde;

/// Embedded descriptor set referenced by the generated `Order` (see build.rs).
pub const FILE_DESCRIPTOR_SET_BYTES: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin"));

#[allow(clippy::all, clippy::pedantic, missing_docs)]
mod order {
    include!(concat!(env!("OUT_DIR"), "/demo.rs"));
}
use order::Order;

#[test]
#[ignore = "golden bytes must be captured from the Confluent JVM KafkaProtobufSerializer"]
fn protobuf_frame_matches_confluent_golden() {
    let golden = hex::decode(include_str!("testdata/schema_serde/protobuf/order.hex").trim())
        .expect("valid hex");

    let cache = SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default());
    let serde = ProtobufSerde::<Order>::new(&cache, "orders-pb-value");
    cache.seed_subject_id("orders-pb-value", 1);

    let order = Order {
        id: "o-1".into(),
        total: 9.5,
    };
    let ours = serde.serialize(&order).unwrap();
    check!(ours.as_ref() == golden.as_slice());
}
