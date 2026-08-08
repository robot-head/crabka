//! Asserts that our Protobuf framing matches the bytes captured from Confluent's
//! `ProtobufSerializer`. That framing is magic, id, message-index, and body.
//!
//! `tests/schema-serde-capture/run.sh` captured the golden in
//! `testdata/schema_serde/protobuf/order.hex` against
//! `mirror.gcr.io/confluentinc/cp-schema-registry`. The schema id is 2, and the
//! top-level message-index is the single `0x00` byte.

use assert2::check;
use crabka_schema_serde::{
    RegistryClient,
    cache::{CacheConfig, SchemaCache},
    format::{SchemaSerializer, protobuf::ProtobufSerde},
};

/// Embedded descriptor set that the generated `Order` references. See
/// ../examples/gen/regenerate.sh.
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
