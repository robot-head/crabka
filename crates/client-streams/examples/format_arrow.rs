//! `ArrowIpcSerde` round-trip for an arrow-rs `RecordBatch`.
//! Run: `cargo run -p crabka-client-streams --example format_arrow --features arrow`
use std::sync::Arc;

use ::arrow::{
    array::{Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use crabka_client_streams::{columnar::serde::arrow::ArrowIpcSerde, processor::serde::Serde};

fn main() {
    // docs:begin arrow-roundtrip
    let schema = Arc::new(Schema::new(vec![
        Field::new("user", DataType::Utf8, false),
        Field::new("amount_cents", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob"])),
            Arc::new(Int64Array::from(vec![850_i64, 900])),
        ],
    )
    .unwrap();
    let bytes = ArrowIpcSerde.serialize("orders.arrow", &batch);
    let back = ArrowIpcSerde.deserialize("orders.arrow", &bytes).unwrap();
    // docs:end arrow-roundtrip
    assert_eq!(back.num_rows(), 2);
    assert_eq!(back, batch);
    println!("format_arrow: OK ({} bytes)", bytes.len());
}
