//! Single-key produce path for the state topic. Built directly on
//! `crabka_client_core::Client` to match the rebalancer's
//! `ingest::admin_client` pattern; we don't pull in the high-level
//! `crabka-client-producer` for a one-key-per-write workload.

use std::time::Duration;

use bytes::Bytes;
use tracing::debug;

use crabka_client_core::Client;
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::records::{Record, RecordBatch};

use crate::state_topic::error::StateTopicError;

/// Transient produce error codes that mean "topic exists in metadata
/// but the partition isn't fully realized yet" — almost always a brief
/// window right after `ensure_topic` returns. Matches the loader's
/// equivalent softness in `loader::poll_once`.
///
/// - 3: `UNKNOWN_TOPIC_OR_PARTITION` — metadata not yet propagated
/// - 5: `LEADER_NOT_AVAILABLE` — leader election in progress
/// - 9: `REPLICA_NOT_AVAILABLE` — follower fetch lag
fn is_transient_produce_code(code: i16) -> bool {
    matches!(code, 3 | 5 | 9)
}

const PRODUCE_RETRY_ATTEMPTS: usize = 50;
const PRODUCE_RETRY_BACKOFF: Duration = Duration::from_millis(200);

/// Produce a single record to `(topic, partition=0)`. `value=None` is
/// a tombstone (null value), matching Kafka compaction semantics.
/// `acks=all`, `timeout_ms=10_000`. Transient error codes (see
/// [`is_transient_produce_code`]) retry with a short backoff for up
/// to `PRODUCE_RETRY_ATTEMPTS * PRODUCE_RETRY_BACKOFF` total wait.
pub(crate) async fn produce_state(
    client: &Client,
    topic: &str,
    key: &str,
    value: Option<Bytes>,
) -> Result<(), StateTopicError> {
    let key_bytes = Bytes::copy_from_slice(key.as_bytes());
    let mut last_transient: Option<i16> = None;
    for attempt in 0..PRODUCE_RETRY_ATTEMPTS {
        match send_once(client, topic, &key_bytes, value.clone()).await {
            Ok(()) => return Ok(()),
            Err(StateTopicError::ProduceErrorCode { code }) if is_transient_produce_code(code) => {
                last_transient = Some(code);
                debug!(
                    code,
                    attempt, "transient produce error; retrying after backoff"
                );
                tokio::time::sleep(PRODUCE_RETRY_BACKOFF).await;
            }
            Err(e) => return Err(e),
        }
    }
    Err(StateTopicError::ProduceErrorCode {
        code: last_transient.unwrap_or(0),
    })
}

async fn send_once(
    client: &Client,
    topic: &str,
    key: &Bytes,
    value: Option<Bytes>,
) -> Result<(), StateTopicError> {
    let record = Record {
        key: Some(key.clone()),
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
