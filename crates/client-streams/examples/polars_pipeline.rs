//! Native columnar topology: sum `amount` per `user` within a batch and write the
//! result as an IPC `DataFrame`.
//!
//! `ColumnarTestDriver` drives it in process, and it needs no broker.
//! Run: `cargo run -p crabka-client-streams --features polars --example polars_pipeline`

use crabka_client_streams::{
    Serde,
    columnar::{
        serde::polars::PolarsIpcSerde,
        topology::{
            ColumnarTestDriver, ColumnarTopology,
            codec::{BlobCodec, ConsumedRecord},
            operator::BuiltinOp,
        },
    },
};
use polars::prelude::*;

fn main() {
    let mut topo = ColumnarTopology::new();
    let src = topo.add_source("src", ["txns"], BlobCodec::default());
    let agg = topo.add_operator(
        "sum-by-user",
        BuiltinOp::GroupByAgg {
            keys: vec![col("user")],
            aggs: vec![col("amount").sum().alias("total")],
        },
        src,
    );
    topo.add_sink("out", "txn-totals", BlobCodec::default(), agg);

    let mut driver = ColumnarTestDriver::new(&topo).unwrap();
    let df = df!("user" => ["a", "a", "b"], "amount" => [5_i64, 3, 9]).unwrap();
    let rec = ConsumedRecord {
        key: None,
        value: PolarsIpcSerde.serialize("txns", &df),
        timestamp: 0,
        partition: 0,
        offset: 0,
    };
    driver.pipe_batch("txns", vec![rec]).unwrap();

    for produced in driver.read_output("txn-totals") {
        let out = PolarsIpcSerde
            .deserialize("txn-totals", &produced.value)
            .unwrap()
            .sort(["user"], SortMultipleOptions::default())
            .unwrap();
        println!("{out}");
    }
}
