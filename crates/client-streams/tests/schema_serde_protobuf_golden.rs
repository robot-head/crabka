//! Asserts our Protobuf framing (magic+id+message-index+body) matches bytes
//! captured from Confluent's `ProtobufSerializer`. The golden in
//! `testdata/schema_serde/protobuf/order.hex` was captured against
//! `mirror.gcr.io/confluentinc/cp-schema-registry` (schema id 2; top-level message-index is
//! the single `0x00` byte) via `tests/schema-serde-capture/run.sh`.

use assert2::check;
use crabka_schema_serde::{
    RegistryClient,
    cache::{CacheConfig, SchemaCache},
    format::{SchemaSerializer, protobuf::ProtobufSerde},
};

/// Embedded descriptor set referenced by the generated `Order` (see ../examples/gen/regenerate.sh).
pub const FILE_DESCRIPTOR_SET_BYTES: &[u8] =
    include_bytes!("../examples/gen/file_descriptor_set.bin");

mod order {
    include!("../examples/gen/order.rs");
}
use order::Order;

#[test]
fn protobuf_frame_matches_confluent_golden() {
    let golden = hex::decode(include_str!("testdata/schema_serde/protobuf/order.hex").trim())
        .expect("valid hex");

    let cache = SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default());
    let serde = ProtobufSerde::<Order>::value(&cache);
    cache.seed_subject_id("orders-pb-value", 2);

    let order = Order {
        id: "o-1".into(),
        total: 9.5,
    };
    let ours = serde.serialize("orders-pb", &order).unwrap();
    check!(ours.as_ref() == golden.as_slice());
}
