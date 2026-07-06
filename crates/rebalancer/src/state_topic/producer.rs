//! Single-key produce path for the state topic. Built directly on
//! `crabka_client_core::Client` to match the rebalancer's
//! `ingest::admin_client` pattern; we don't pull in the high-level
//! `crabka-client-producer` for a one-key-per-write workload.

use std::time::Duration;

use bytes::Bytes;
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        metadata_response::MetadataResponse,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid,
    records::{Record, RecordBatch},
};
use tracing::debug;

use crate::state_topic::error::{StateTopicError, is_transient_topic_partition_code};

const PRODUCE_RETRY_ATTEMPTS: usize = 50;
const PRODUCE_RETRY_BACKOFF: Duration = Duration::from_millis(200);

/// Produce a single record to `(topic, partition=0)`. `value=None` is
/// a tombstone (null value), matching Kafka compaction semantics.
/// `acks=all`, `timeout_ms=10_000`. Transient error codes (see
/// [`is_transient_topic_partition_code`]) retry with a short backoff for up
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
        // KIP-516: Produce v13+ keys partition routing by `topic_id`.
        // Resolve it via Metadata on each attempt — also nudges the
        // broker to load the topic into its data plane if it hasn't
        // yet, which addresses the post-create transient window.
        let topic_id = match resolve_topic_id(client, topic).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                last_transient = Some(3);
                debug!(
                    attempt,
                    topic, "metadata returned no topic_id; retrying after backoff"
                );
                tokio::time::sleep(PRODUCE_RETRY_BACKOFF).await;
                continue;
            }
            Err(e) => return Err(e),
        };
        match classify_send_result(
            send_once(client, topic, topic_id, &key_bytes, value.clone()).await,
        ) {
            Ok(None) => return Ok(()),
            Ok(Some(code)) => {
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

/// Resolve a topic's UUID via Metadata. Returns `Ok(None)` when the
/// metadata response has no entry for the topic (the topic exists in
/// the controller's metadata image but hasn't propagated to the broker
/// we're talking to — treat as transient and retry).
async fn resolve_topic_id(client: &Client, topic: &str) -> Result<Option<Uuid>, StateTopicError> {
    let resp = client.send(metadata_request(topic)).await?;
    Ok(topic_id_from_metadata(&resp, topic))
}

fn metadata_request(topic: &str) -> MetadataRequest {
    MetadataRequest {
        topics: Some(vec![MetadataRequestTopic {
            name: Some(topic.into()),
            ..Default::default()
        }]),
        ..Default::default()
    }
}

fn topic_id_from_metadata(resp: &MetadataResponse, topic: &str) -> Option<Uuid> {
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic))
        .map(|t| t.topic_id)
        .filter(|id| *id != Uuid::default())
}

async fn send_once(
    client: &Client,
    topic: &str,
    topic_id: Uuid,
    key: &Bytes,
    value: Option<Bytes>,
) -> Result<(), StateTopicError> {
    let req = produce_request(topic, topic_id, key, value);
    let resp = client.send(req).await?;
    if let Some(code) = produce_response_error(&resp) {
        return Err(StateTopicError::ProduceErrorCode { code });
    }
    Ok(())
}

fn produce_request(
    topic: &str,
    topic_id: Uuid,
    key: &Bytes,
    value: Option<Bytes>,
) -> ProduceRequest {
    let record = Record {
        key: Some(key.clone()),
        value,
        ..Default::default()
    };
    let batch = RecordBatch {
        records: vec![record],
        ..Default::default()
    };
    ProduceRequest {
        acks: -1, // all
        timeout_ms: 10_000,
        topic_data: vec![TopicProduceData {
            name: topic.into(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(batch.into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn produce_response_error(resp: &ProduceResponse) -> Option<i16> {
    for t in &resp.responses {
        for p in &t.partition_responses {
            if p.error_code != 0 {
                return Some(p.error_code);
            }
        }
    }
    None
}

fn classify_send_result(
    result: Result<(), StateTopicError>,
) -> Result<Option<i16>, StateTopicError> {
    match result {
        Ok(()) => Ok(None),
        Err(StateTopicError::ProduceErrorCode { code })
            if is_transient_topic_partition_code(code) =>
        {
            Ok(Some(code))
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_protocol::{
        owned::{
            metadata_response::{MetadataResponse, MetadataResponseTopic},
            produce_response::{PartitionProduceResponse, ProduceResponse, TopicProduceResponse},
        },
        records::RecordsPayload,
    };

    use super::*;

    fn response_with_error(code: i16) -> ProduceResponse {
        ProduceResponse {
            responses: vec![TopicProduceResponse {
                partition_responses: vec![PartitionProduceResponse {
                    error_code: code,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn unreachable_client_id(suffix: &str) -> String {
        format!("state-topic-producer-test-{suffix}")
    }

    async fn unreachable_client(suffix: &str) -> Client {
        Client::builder()
            .bootstrap("127.0.0.1:1")
            .client_id(unreachable_client_id(suffix))
            .connect_timeout(Duration::from_millis(50))
            .request_timeout(Duration::from_millis(50))
            .build()
            .await
            .expect("client build does not connect")
    }

    #[test]
    fn produce_request_writes_single_key_record_to_partition_zero() {
        let topic_id = Uuid([7; 16]);
        let key = Bytes::from_static(b"in_flight");
        let value = Some(Bytes::from_static(b"{json}"));

        let req = produce_request("state-topic", topic_id, &key, value.clone());

        check!(req.transactional_id.is_none());
        check!(req.acks == -1);
        check!(req.timeout_ms == 10_000);
        assert!(req.topic_data.len() == 1);
        check!(req.topic_data[0].name == "state-topic");
        check!(req.topic_data[0].topic_id == topic_id);
        assert!(req.topic_data[0].partition_data.len() == 1);
        check!(req.topic_data[0].partition_data[0].index == 0);
        let records = req.topic_data[0].partition_data[0]
            .records
            .as_ref()
            .expect("records");
        let RecordsPayload::V2(batches) = records else {
            panic!("produce request should use v2 record batches");
        };
        assert!(batches.len() == 1);
        assert!(batches[0].records.len() == 1);
        check!(batches[0].records[0].key.as_ref() == Some(&key));
        check!(batches[0].records[0].value == value);
    }

    #[test]
    fn metadata_request_scopes_to_state_topic_name() {
        let req = metadata_request("state-topic");

        let topics = req.topics.expect("topics");
        assert!(topics.len() == 1);
        assert!(topics[0].name.as_deref() == Some("state-topic"));
    }

    #[test]
    fn topic_id_from_metadata_requires_matching_nonzero_topic_id() {
        let wanted = Uuid([7; 16]);
        let resp = MetadataResponse {
            topics: vec![
                MetadataResponseTopic {
                    name: Some("other-topic".into()),
                    topic_id: Uuid([9; 16]),
                    ..Default::default()
                },
                MetadataResponseTopic {
                    name: Some("state-topic".into()),
                    topic_id: wanted,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        for (topic, want) in [
            ("state-topic", Some(wanted)),
            ("other-topic", Some(Uuid([9; 16]))),
            ("missing", None),
        ] {
            assert!(topic_id_from_metadata(&resp, topic) == want);
        }
    }

    #[test]
    fn topic_id_from_metadata_treats_zero_uuid_as_missing() {
        let resp = MetadataResponse {
            topics: vec![MetadataResponseTopic {
                name: Some("state-topic".into()),
                topic_id: Uuid::default(),
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(topic_id_from_metadata(&resp, "state-topic").is_none());
    }

    #[test]
    fn produce_response_errors_are_classified_for_retry() {
        check!(classify_send_result(Ok(())).unwrap().is_none());
        check!(
            classify_send_result(Err(StateTopicError::ProduceErrorCode { code: 5 })).unwrap()
                == Some(5)
        );
        let err =
            classify_send_result(Err(StateTopicError::ProduceErrorCode { code: 42 })).unwrap_err();
        assert!(matches!(
            err,
            StateTopicError::ProduceErrorCode { code: 42 }
        ));
    }

    #[test]
    fn produce_response_error_scans_partition_responses() {
        assert!(produce_response_error(&response_with_error(0)).is_none());
        assert!(produce_response_error(&response_with_error(42)) == Some(42));
    }

    #[tokio::test]
    async fn produce_state_propagates_initial_metadata_send_errors() {
        let client = unreachable_client("produce-state").await;

        assert!(
            produce_state(
                &client,
                "__crabka_state",
                "in_flight",
                Some(Bytes::from_static(b"{}"))
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn resolve_topic_id_propagates_metadata_send_errors() {
        let client = unreachable_client("resolve-topic-id").await;

        assert!(resolve_topic_id(&client, "__crabka_state").await.is_err());
    }

    #[tokio::test]
    async fn send_once_propagates_produce_send_errors() {
        let client = unreachable_client("send-once").await;
        let key = Bytes::from_static(b"in_flight");

        assert!(
            send_once(
                &client,
                "__crabka_state",
                Uuid([7; 16]),
                &key,
                Some(Bytes::from_static(b"{}"))
            )
            .await
            .is_err()
        );
    }
}
