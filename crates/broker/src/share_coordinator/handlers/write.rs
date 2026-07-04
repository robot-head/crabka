//! `WriteShareGroupState` (`api_key=85`). Applies a delivery-state delta
//! (advance start offset, upsert state batches, bump leader/state epoch +
//! delivery-complete count) for each `(group, topic, partition)`. Gates on
//! local leadership and surfaces epoch fencing as the per-partition error code.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use crabka_log::Offset;
use crabka_protocol::{
    Decode, Encode,
    owned::{
        write_share_group_state_request::WriteShareGroupStateRequest,
        write_share_group_state_response::{
            PartitionResult, WriteShareGroupStateResponse, WriteStateResult,
        },
    },
};
use futures_util::future::BoxFuture;

use crate::{
    broker::Broker, codes, error::BrokerError, share_coordinator::persistence::StateBatch,
};

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
        let req = WriteShareGroupStateRequest::decode(&mut cur, version)?;
        let group_id = req.group_id;

        let mut results: Vec<WriteStateResult> = Vec::with_capacity(req.topics.len());
        for topic in req.topics {
            let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
            let mut partitions: Vec<PartitionResult> = Vec::with_capacity(topic.partitions.len());
            for pd in topic.partitions {
                let state_partition =
                    coordinator.state_partition_for(&group_id, &topic_id, pd.partition);
                let error_code = if coordinator.is_leader(state_partition).await {
                    let batches: Vec<StateBatch> = pd
                        .state_batches
                        .iter()
                        .map(|b| StateBatch {
                            first_offset: Offset(b.first_offset),
                            last_offset: Offset(b.last_offset),
                            delivery_state: b.delivery_state,
                            delivery_count: b.delivery_count,
                        })
                        .collect();
                    match coordinator
                        .write(
                            &group_id,
                            topic_id,
                            pd.partition,
                            pd.state_epoch,
                            pd.leader_epoch,
                            Offset(pd.start_offset),
                            pd.delivery_complete_count,
                            batches,
                        )
                        .await
                    {
                        Ok(()) => codes::NONE,
                        Err(code) => code,
                    }
                } else {
                    codes::NOT_COORDINATOR
                };
                partitions.push(PartitionResult {
                    partition: pd.partition,
                    error_code,
                    error_message: None,
                    ..Default::default()
                });
            }
            results.push(WriteStateResult {
                topic_id: topic.topic_id,
                partitions,
                ..Default::default()
            });
        }

        let resp = WriteShareGroupStateResponse {
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
    use assert2::assert;
    use crabka_protocol::{
        UnknownTaggedFields,
        owned::{
            write_share_group_state_request::{
                PartitionData, StateBatch, WriteShareGroupStateRequest, WriteStateData,
            },
            write_share_group_state_response::WriteShareGroupStateResponse,
        },
        primitives::uuid::Uuid as ProtoUuid,
    };

    use super::*;

    const VERSION: i16 = 1;

    fn decode(bytes: &Bytes) -> WriteShareGroupStateResponse {
        let mut cur: &[u8] = bytes.as_ref();
        let resp =
            WriteShareGroupStateResponse::decode(&mut cur, VERSION).expect("decode response");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    fn encode_request(req: &WriteShareGroupStateRequest) -> Bytes {
        let mut buf = BytesMut::with_capacity(req.encoded_len(VERSION));
        req.encode(&mut buf, VERSION).expect("encode request");
        buf.freeze()
    }

    fn request(group_id: &str, topic_id: ProtoUuid, partition: i32) -> WriteShareGroupStateRequest {
        WriteShareGroupStateRequest {
            group_id: group_id.into(),
            topics: vec![WriteStateData {
                topic_id,
                partitions: vec![PartitionData {
                    partition,
                    state_epoch: 17,
                    leader_epoch: 3,
                    start_offset: 101,
                    delivery_complete_count: 9,
                    state_batches: vec![StateBatch {
                        first_offset: 101,
                        last_offset: 105,
                        delivery_state: 2,
                        delivery_count: 3,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn returns_success_row_and_persists_state_for_led_partition() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let (broker_handle, broker) =
            super::super::test_support::broker_with_led_share_coordinator(dir.path()).await;
        let topic_id = uuid::Uuid::from_bytes([51; 16]);
        let wire_topic_id = ProtoUuid(*topic_id.as_bytes());
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

        let expected = WriteShareGroupStateResponse {
            results: vec![WriteStateResult {
                topic_id: wire_topic_id,
                partitions: vec![PartitionResult {
                    partition: 4,
                    error_code: codes::NONE,
                    error_message: None,
                    unknown_tagged_fields: UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);

        let summary = broker
            .share_coordinator
            .read_summary("share-group", topic_id, 4)
            .await
            .expect("written state is readable");
        assert!(summary == (17, 3, Offset(101), 9));
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn returns_not_coordinator_row_for_unled_partition() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let (broker_handle, broker) = super::super::test_support::broker(dir.path()).await;
        let topic_id = ProtoUuid([52; 16]);
        let req = request("share-group", topic_id, 8);
        let req_bytes = encode_request(&req);

        let bytes = super::handle(&broker, VERSION, 123, &req_bytes)
            .await
            .expect("handle");
        let resp = decode(&bytes);

        let expected = WriteShareGroupStateResponse {
            results: vec![WriteStateResult {
                topic_id,
                partitions: vec![PartitionResult {
                    partition: 8,
                    error_code: codes::NOT_COORDINATOR,
                    error_message: None,
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
