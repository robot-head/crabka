//! Single-key produce path for the state topic. Built directly on
//! `crabka_client_core::Client` to match the rebalancer's
//! `ingest::admin_client` pattern; we don't pull in the high-level
//! `crabka-client-producer` for a one-key-per-write workload.

use bytes::Bytes;

use crabka_client_core::Client;
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::records::{Record, RecordBatch};

use crate::state_topic::error::StateTopicError;

/// Produce a single record to `(topic, partition=0)`. `value=None` is
/// a tombstone (null value), matching Kafka compaction semantics.
/// `acks=all`, `timeout_ms=10_000`.
#[allow(dead_code)] // wired in Task 5
pub(crate) async fn produce_state(
    client: &Client,
    topic: &str,
    key: &str,
    value: Option<Bytes>,
) -> Result<(), StateTopicError> {
    let record = Record {
        key: Some(Bytes::copy_from_slice(key.as_bytes())),
        value,
        ..Default::default()
    };
    let batch = RecordBatch {
        records: vec![record],
        ..Default::default()
    };
    let req = ProduceRequest {
        transactional_id: None,
        acks: -1, // all
        timeout_ms: 10_000,
        topic_data: vec![TopicProduceData {
            name: topic.into(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(batch.into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = client.send(req).await?;
    for t in &resp.responses {
        for p in &t.partition_responses {
            if p.error_code != 0 {
                return Err(StateTopicError::ProduceErrorCode { code: p.error_code });
            }
        }
    }
    Ok(())
}
