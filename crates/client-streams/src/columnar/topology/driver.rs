//! Broker-free driver for columnar topologies (analog of `TopologyTestDriver`).
//! Pipe a batch of input records for a source topic, read produced records per
//! sink topic.

use std::collections::HashMap;

use super::{
    codec::{ConsumedRecord, ProduceRecord},
    graph::{BuiltColumnarTopology, ColumnarTopology},
};

/// Drives a [`ColumnarTopology`] in-process: pipe a batch of input records for a
/// source topic, then read the records produced to each sink topic.
pub struct ColumnarTestDriver<'t> {
    built: BuiltColumnarTopology<'t>,
    output: HashMap<String, Vec<ProduceRecord>>,
}

impl<'t> ColumnarTestDriver<'t> {
    /// Build the topology for in-process execution.
    ///
    /// # Errors
    /// Returns the validation error message if the topology is structurally invalid.
    pub fn new(topo: &'t ColumnarTopology) -> Result<Self, String> {
        Ok(Self {
            built: topo.build()?,
            output: HashMap::new(),
        })
    }

    /// Run one batch of input records (arriving on `topic`) through the topology,
    /// buffering produced records by sink topic.
    ///
    /// # Errors
    /// Returns the codec/operator error string if processing the batch fails.
    // Takes the batch by value (the driver owns piped input, like `TopologyTestDriver`);
    // `run_batch` only needs a borrow, hence the lint suppression.
    #[allow(clippy::needless_pass_by_value)]
    pub fn pipe_batch(&mut self, topic: &str, records: Vec<ConsumedRecord>) -> Result<(), String> {
        let produced = self
            .built
            .run_batch(topic, &records)
            .map_err(|e| e.to_string())?;
        for (sink_topic, rec) in produced {
            self.output.entry(sink_topic).or_default().push(rec);
        }
        Ok(())
    }

    /// Drain produced records for `topic`.
    ///
    /// # Returns
    /// All records buffered for `topic` so far, removing them from the driver
    /// (an empty `Vec` if none).
    pub fn read_output(&mut self, topic: &str) -> Vec<ProduceRecord> {
        self.output.remove(topic).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use ::polars::prelude::*;
    use assert2::check;

    use super::*;
    use crate::{
        columnar::{
            serde::polars::PolarsIpcSerde,
            topology::{
                codec::{BlobCodec, ConsumedRecord},
                operator::BuiltinOp,
            },
        },
        processor::serde::Serde,
    };

    #[test]
    fn driver_pipes_batch_and_reads_output() {
        let mut t = ColumnarTopology::new();
        let src = t.add_source("src", ["in"], BlobCodec::default());
        let agg = t.add_operator(
            "agg",
            BuiltinOp::GroupByAgg {
                keys: vec![col("user")],
                aggs: vec![col("amount").sum().alias("total")],
            },
            src,
        );
        t.add_sink("out", "out", BlobCodec::default(), agg);

        let mut driver = ColumnarTestDriver::new(&t).unwrap();
        let df = df!("user" => ["a", "a", "b"], "amount" => [5_i64, 3, 9]).unwrap();
        let rec = ConsumedRecord {
            key: None,
            value: PolarsIpcSerde.serialize("", &df),
            timestamp: 0,
            partition: 0,
            offset: 0,
        };
        driver.pipe_batch("in", vec![rec]).unwrap();

        let out = driver.read_output("out");
        check!(out.len() == 1);
        let result = PolarsIpcSerde
            .deserialize("", &out[0].value)
            .unwrap()
            .sort(["user"], SortMultipleOptions::default())
            .unwrap();
        check!(result.height() == 2);
    }
}
