//! `ReadShareGroupStateSummary` (`api_key=87`). Returns the lightweight summary
//! (state epoch, leader epoch, start offset, delivery-complete count) for each
//! `(group, topic, partition)` without the full state-batch list. A partition
//! this broker does not lead returns per-partition `NOT_COORDINATOR`; an
//! unknown-but-led key returns the initial summary (`start_offset = -1`) with
//! `error_code = 0`.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::read_share_group_state_summary_request::ReadShareGroupStateSummaryRequest;
use crabka_protocol::owned::read_share_group_state_summary_response::{
    PartitionResult, ReadShareGroupStateSummaryResponse, ReadStateSummaryResult,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let coordinator = Arc::clone(&broker.share_coordinator);
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = ReadShareGroupStateSummaryRequest::decode(&mut cur, version)?;
        let group_id = req.group_id;

        let mut results: Vec<ReadStateSummaryResult> = Vec::with_capacity(req.topics.len());
        for topic in req.topics {
            let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
            let mut partitions: Vec<PartitionResult> = Vec::with_capacity(topic.partitions.len());
            for pd in topic.partitions {
                let state_partition =
                    coordinator.state_partition_for(&group_id, &topic_id, pd.partition);
                let result = if coordinator.is_leader(state_partition).await {
                    match coordinator
                        .read_summary(&group_id, topic_id, pd.partition)
                        .await
                    {
                        Some((
                            state_epoch,
                            leader_epoch,
                            start_offset,
                            delivery_complete_count,
                        )) => PartitionResult {
                            partition: pd.partition,
                            state_epoch,
                            leader_epoch,
                            start_offset,
                            delivery_complete_count,
                            ..Default::default()
                        },
                        None => PartitionResult {
                            partition: pd.partition,
                            start_offset: -1,
                            delivery_complete_count: 0,
                            ..Default::default()
                        },
                    }
                } else {
                    PartitionResult {
                        partition: pd.partition,
                        error_code: codes::NOT_COORDINATOR,
                        start_offset: -1,
                        delivery_complete_count: 0,
                        ..Default::default()
                    }
                };
                partitions.push(result);
            }
            results.push(ReadStateSummaryResult {
                topic_id: topic.topic_id,
                partitions,
                ..Default::default()
            });
        }

        let resp = ReadShareGroupStateSummaryResponse {
            results,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_protocol::UnknownTaggedFields;
    use crabka_protocol::owned::read_share_group_state_summary_request::{
        PartitionData, ReadShareGroupStateSummaryRequest, ReadStateSummaryData,
    };
    use crabka_protocol::owned::read_share_group_state_summary_response::ReadShareGroupStateSummaryResponse;
    use crabka_protocol::primitives::uuid::Uuid as ProtoUuid;

    const VERSION: i16 = 1;

    fn decode(bytes: &Bytes) -> ReadShareGroupStateSummaryResponse {
        let mut cur: &[u8] = bytes.as_ref();
        let resp =
            ReadShareGroupStateSummaryResponse::decode(&mut cur, VERSION).expect("decode response");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    fn encode_request(req: &ReadShareGroupStateSummaryRequest) -> Bytes {
        let mut buf = BytesMut::with_capacity(req.encoded_len(VERSION));
        req.encode(&mut buf, VERSION).expect("encode request");
        buf.freeze()
    }

    fn request(
        group_id: &str,
        topic_id: ProtoUuid,
        partition: i32,
    ) -> ReadShareGroupStateSummaryRequest {
        ReadShareGroupStateSummaryRequest {
            group_id: group_id.into(),
            topics: vec![ReadStateSummaryData {
                topic_id,
                partitions: vec![PartitionData {
                    partition,
                    leader_epoch: 3,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn returns_persisted_summary_for_led_partition() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let (broker_handle, broker) =
            super::super::test_support::broker_with_led_share_coordinator(dir.path()).await;
        let topic_id = uuid::Uuid::from_bytes([41; 16]);
        let wire_topic_id = ProtoUuid(*topic_id.as_bytes());
        broker
            .share_coordinator
            .initialize("share-group", topic_id, 4, 17, 90)
            .await
            .expect("initialize state");
        broker
            .share_coordinator
            .write(
                "share-group",
                topic_id,
                4,
                17,
                3,
                101,
                9,
                vec![super::super::test_support::batch(101, 105)],
            )
            .await
            .expect("write state");
        let req = request("share-group", wire_topic_id, 4);
        let req_bytes = encode_request(&req);

        broker
            .share_coordinator
            .lead_all_partitions_for_test()
            .await;
        let bytes = super::handle(&broker, VERSION, 123, &req_bytes)
            .await
            .expect("handle");
        let resp = decode(&bytes);

        let expected = ReadShareGroupStateSummaryResponse {
            results: vec![ReadStateSummaryResult {
                topic_id: wire_topic_id,
                partitions: vec![PartitionResult {
                    partition: 4,
                    error_code: codes::NONE,
                    error_message: None,
                    state_epoch: 17,
                    leader_epoch: 3,
                    start_offset: 101,
                    delivery_complete_count: 9,
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn returns_initial_summary_for_led_missing_partition() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let (broker_handle, broker) =
            super::super::test_support::broker_with_led_share_coordinator(dir.path()).await;
        let topic_id = ProtoUuid([42; 16]);
        let req = request("share-group", topic_id, 6);
        let req_bytes = encode_request(&req);

        broker
            .share_coordinator
            .lead_all_partitions_for_test()
            .await;
        let bytes = super::handle(&broker, VERSION, 123, &req_bytes)
            .await
            .expect("handle");
        let resp = decode(&bytes);

        let expected = ReadShareGroupStateSummaryResponse {
            results: vec![ReadStateSummaryResult {
                topic_id,
                partitions: vec![PartitionResult {
                    partition: 6,
                    error_code: codes::NONE,
                    error_message: None,
                    state_epoch: 0,
                    leader_epoch: 0,
                    start_offset: -1,
                    delivery_complete_count: 0,
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn returns_not_coordinator_for_unled_partition() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let (broker_handle, broker) = super::super::test_support::broker(dir.path()).await;
        let topic_id = ProtoUuid([43; 16]);
        let req = request("share-group", topic_id, 8);
        let req_bytes = encode_request(&req);

        let bytes = super::handle(&broker, VERSION, 123, &req_bytes)
            .await
            .expect("handle");
        let resp = decode(&bytes);

        let expected = ReadShareGroupStateSummaryResponse {
            results: vec![ReadStateSummaryResult {
                topic_id,
                partitions: vec![PartitionResult {
                    partition: 8,
                    error_code: codes::NOT_COORDINATOR,
                    error_message: None,
                    state_epoch: 0,
                    leader_epoch: 0,
                    start_offset: -1,
                    delivery_complete_count: 0,
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
