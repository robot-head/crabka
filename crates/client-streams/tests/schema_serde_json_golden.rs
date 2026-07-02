//! Asserts our JSON framing/body matches bytes captured from Confluent's
//! `JSONSerializer`. The golden in `testdata/schema_serde/json/order.hex` was
//! captured against `mirror.gcr.io/confluentinc/cp-schema-registry` (schema id 3) via
//! `tests/schema-serde-capture/run.sh`. Confluent emits compact JSON
//! (`{"id":"o-1","total":9.5}`, no spaces, declaration field order) — which
//! matches `serde_json`'s compact output, so the frames are byte-identical.

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
fn json_frame_matches_confluent_golden() {
    let golden = hex::decode(include_str!("testdata/schema_serde/json/order.hex").trim())
        .expect("valid hex");

    let cache = SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default());
    let serde = JsonSerde::<Order>::value(&cache, false);
    cache.seed_subject_id("orders-json-value", 3);

    let order = Order {
        id: "o-1".into(),
        total: 9.5,
    };
    let ours = serde.serialize("orders-json", &order).unwrap();
    check!(ours.as_ref() == golden.as_slice());
}
