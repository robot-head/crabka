//! Runs a `ColumnarTopology` against the existing KIP-1071 runtime I/O traits.
//! One `FetchBatch` per (topic, partition) becomes one `DataFrame` batch (= the
//! commit/transaction boundary); produced records go through `RecordProducer`.

use super::{codec::ConsumedRecord, graph::ColumnarTopology};
use crate::{
    error::StreamsClientError,
    runtime::io::{FetchBatch, IsolationLevel, RecordFetcher, RecordProducer},
};

/// Convert a fetched per-partition batch into codec input records.
fn to_consumed(partition: i32, batch: &FetchBatch) -> Vec<ConsumedRecord> {
    batch
        .records
        .iter()
        .map(|r| ConsumedRecord {
            key: r.key.clone(),
            value: r.value.clone().unwrap_or_default(),
            timestamp: r.timestamp,
            partition,
            offset: r.offset,
        })
        .collect()
}

/// Drive one fetch→process→produce→flush cycle for `(topic, partition)` starting
/// at `offset`. Returns the next offset to fetch (unchanged if nothing new).
///
/// The whole fetched batch is assembled into one `DataFrame` and processed as a
/// unit, so the batch is the commit boundary.
///
/// # Errors
/// Returns a `StreamsClientError` if fetching, processing, producing, or flushing
/// fails.
#[tracing::instrument(
    name = "streams.columnar.run_partition_once",
    level = "debug",
    skip_all,
    fields(topic = %topic, partition, offset),
    err,
)]
pub async fn run_partition_once(
    topo: &ColumnarTopology,
    fetcher: &dyn RecordFetcher,
    producer: &dyn RecordProducer,
    topic: &str,
    partition: i32,
    offset: i64,
) -> Result<i64, StreamsClientError> {
    let batch = fetcher
        .fetch(topic, partition, offset, IsolationLevel::ReadUncommitted)
        .await?;
    if batch.records.is_empty() {
        return Ok(offset);
    }
    let next = batch.next_offset(offset);
    let consumed = to_consumed(partition, &batch);
    let built = topo.build().map_err(StreamsClientError::Runtime)?;
    let outputs = built
        .run_batch(topic, &consumed)
        .map_err(|e| StreamsClientError::Runtime(e.to_string()))?;
    for (sink_topic, rec) in outputs {
        producer
            .send_with_timestamp(
                &sink_topic,
                None,
                rec.key,
                Some(rec.value),
                Some(rec.timestamp),
            )
            .await?;
    }
    producer.flush().await?;
    Ok(next)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use ::polars::prelude::*;
    use assert2::check;
    use bytes::Bytes;

    use super::*;
    use crate::{
        columnar::{
            serde::polars::PolarsIpcSerde,
            topology::{codec::BlobCodec, operator::BuiltinOp},
        },
        processor::serde::Serde,
        runtime::io::FetchedRec,
    };

    struct OneShotFetcher(Mutex<Option<FetchBatch>>);
    #[async_trait::async_trait]
    impl RecordFetcher for OneShotFetcher {
        async fn fetch(
            &self,
            _t: &str,
            _p: i32,
            _o: i64,
            _i: IsolationLevel,
        ) -> Result<FetchBatch, StreamsClientError> {
            Ok(self.0.lock().unwrap().take().unwrap_or_default())
        }
    }

    /// One captured produced record: `(topic, key, value)`.
    type Sent = (String, Option<Bytes>, Option<Bytes>);

    #[derive(Default)]
    struct CollectProducer {
        sent: Mutex<Vec<Sent>>,
        flushed: Mutex<u32>,
    }
    #[async_trait::async_trait]
    impl RecordProducer for CollectProducer {
        async fn send(
            &self,
            topic: &str,
            _part: Option<i32>,
            key: Option<Bytes>,
            value: Option<Bytes>,
        ) -> Result<(), StreamsClientError> {
            self.sent
                .lock()
                .unwrap()
                .push((topic.to_string(), key, value));
            Ok(())
        }
        async fn flush(&self) -> Result<(), StreamsClientError> {
            *self.flushed.lock().unwrap() += 1;
            Ok(())
        }
    }

    fn topo() -> ColumnarTopology {
        let mut t = ColumnarTopology::new();
        let src = t.add_source("src", ["in"], BlobCodec::default());
        let op = t.add_operator("flt", BuiltinOp::Filter(col("amount").gt(lit(4))), src);
        t.add_sink("out", "out", BlobCodec::default(), op);
        t
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_partition_once_filters_and_produces() {
        let t = topo();
        let df = df!("amount" => [1_i64, 5, 9]).unwrap();
        let batch = FetchBatch {
            records: vec![FetchedRec {
                offset: 0,
                key: None,
                value: Some(PolarsIpcSerde.serialize("", &df)),
                timestamp: 7,
            }],
        };
        let fetcher = OneShotFetcher(Mutex::new(Some(batch)));
        let producer = CollectProducer::default();

        let next = run_partition_once(&t, &fetcher, &producer, "in", 0, 0)
            .await
            .unwrap();
        check!(next == 1);
        let sent = producer.sent.lock().unwrap();
        check!(sent.len() == 1);
        check!(sent[0].0 == "out");
        let back = PolarsIpcSerde
            .deserialize("", sent[0].2.as_ref().unwrap())
            .unwrap();
        check!(back.height() == 2); // amounts 5 and 9
        check!(*producer.flushed.lock().unwrap() == 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn empty_fetch_is_noop() {
        let t = topo();
        let fetcher = OneShotFetcher(Mutex::new(Some(FetchBatch::default())));
        let producer = CollectProducer::default();
        let next = run_partition_once(&t, &fetcher, &producer, "in", 42, 42)
            .await
            .unwrap();
        check!(next == 42);
        check!(producer.sent.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn offset_advances_past_last_record() {
        let t = topo();
        let df = df!("amount" => [9_i64]).unwrap();
        let batch = FetchBatch {
            records: vec![FetchedRec {
                offset: 100,
                key: None,
                value: Some(PolarsIpcSerde.serialize("", &df)),
                timestamp: 1,
            }],
        };
        let fetcher = OneShotFetcher(Mutex::new(Some(batch)));
        let producer = CollectProducer::default();
        let next = run_partition_once(&t, &fetcher, &producer, "in", 100, 100)
            .await
            .unwrap();
        check!(next == 101);
    }
}
