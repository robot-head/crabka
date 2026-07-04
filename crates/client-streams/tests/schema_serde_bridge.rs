use apache_avro::AvroSchema;
use assert2::check;
use crabka_client_streams::{SchemaSerde, processor::serde::Serde};
use crabka_schema_serde::{
    RegistryClient,
    cache::{CacheConfig, SchemaCache},
    format::avro::AvroSerde,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, AvroSchema)]
struct Order {
    id: String,
    total: f64,
}

#[test]
fn avro_bridge_round_trips() {
    let cache = SchemaCache::new(RegistryClient::new("http://unused"), CacheConfig::default());
    let inner = AvroSerde::<Order>::value(&cache);
    cache.seed_subject_id("orders-value", 9);
    cache.seed_writer_schema(9, Order::get_schema().canonical_form());
    let serde = SchemaSerde::new(inner);

    let order = Order {
        id: "o-1".into(),
        total: 2.5,
    };
    let bytes = Serde::serialize(&serde, "orders", &order);
    let back: Order = Serde::deserialize(&serde, "orders", &bytes).unwrap();
    check!(back == order);
}
