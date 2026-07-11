//! Round-trip a polars `DataFrame` through the `Serde<DataFrame>` boundary.
//! Run: `cargo run -p crabka-client-streams --features polars --example dataframe_serde`

use crabka_client_streams::{Serde, columnar::serde::polars::PolarsIpcSerde};
use polars::prelude::*;

fn main() {
    let df = df!("id" => ["a", "b"], "total" => [1.0_f64, 2.5]).unwrap();
    let bytes = PolarsIpcSerde.serialize("orders", &df);
    println!("encoded {} bytes", bytes.len());
    let back = PolarsIpcSerde.deserialize("orders", &bytes).unwrap();
    println!("{back}");
    assert!(back.equals(&df));
}
