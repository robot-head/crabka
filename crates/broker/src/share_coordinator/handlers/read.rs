//! `ReadShareGroupState` (`api_key=84`). Returns the durable delivery state
//! (start offset + state batches) for each `(group, topic, partition)`. A
//! partition this broker does not lead returns per-partition `NOT_COORDINATOR`;
//! an unknown-but-led key returns the initial/empty state (`start_offset = -1`,
//! no batches) with `error_code = 0`.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        read_share_group_state_request::ReadShareGroupStateRequest,
        read_share_group_state_response::{
            PartitionResult, ReadShareGroupStateResponse, ReadStateResult, StateBatch,
        },
    },
};
use futures_util::future::BoxFuture;

use crate::{
    broker::Broker,
    codes,
    error::BrokerError,
    share_coordinator::coordinator::{ShareCoordinator, UNINITIALIZED_START_OFFSET},
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
        let req = ReadShareGroupStateRequest::decode(&mut cur, version)?;
        handle_request(coordinator, version, req).await
    })
}

async fn handle_request(
    coordinator: Arc<ShareCoordinator>,
    version: i16,
    req: ReadShareGroupStateRequest,
) -> Result<Bytes, BrokerError> {
    let group_id = req.group_id;

    let mut results: Vec<ReadStateResult> = Vec::with_capacity(req.topics.len());
    for topic in req.topics {
        let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
        let mut partitions: Vec<PartitionResult> = Vec::with_capacity(topic.partitions.len());
        for pd in topic.partitions {
            let state_partition =
                coordinator.state_partition_for(&group_id, &topic_id, pd.partition);
            let result = if coordinator.is_leader(state_partition).await {
                match coordinator.read(&group_id, topic_id, pd.partition).await {
                    Some(st) => PartitionResult {
                        partition: pd.partition,
                        state_epoch: st.state_epoch,
                        start_offset: st.start_offset.0,
                        state_batches: st
                            .state_batches
                            .iter()
                            .map(|b| StateBatch {
                                first_offset: b.first_offset.0,
                                last_offset: b.last_offset.0,
                                delivery_state: b.delivery_state,
                                delivery_count: b.delivery_count,
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    },
                    // Unknown key on a led partition: report the initial,
                    // empty state with no error so the share-partition
                    // leader starts from scratch.
                    None => PartitionResult {
                        partition: pd.partition,
                        start_offset: UNINITIALIZED_START_OFFSET,
                        ..Default::default()
                    },
                }
            } else {
                PartitionResult {
                    partition: pd.partition,
                    error_code: codes::NOT_COORDINATOR,
                    start_offset: UNINITIALIZED_START_OFFSET,
                    ..Default::default()
                }
            };
            partitions.push(result);
        }
        results.push(ReadStateResult {
            topic_id: topic.topic_id,
            partitions,
            ..Default::default()
        });
    }

    let resp = ReadShareGroupStateResponse {
        results,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_log::Offset;
    use crabka_protocol::{
        UnknownTaggedFields,
        owned::{
            read_share_group_state_request::{PartitionData, ReadStateData},
            read_share_group_state_response::ReadShareGroupStateResponse,
        },
        primitives::uuid::Uuid as ProtoUuid,
    };

    use super::*;

    fn decode(bytes: &Bytes) -> ReadShareGroupStateResponse {
        let mut cur: &[u8] = bytes.as_ref();
        let resp =
            ReadShareGroupStateResponse::decode(&mut cur, super::super::test_support::VERSION)
                .expect("decode response");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    fn encode_request(req: &ReadShareGroupStateRequest) -> Bytes {
        let mut buf = BytesMut::with_capacity(req.encoded_len(super::super::test_support::VERSION));
        req.encode(&mut buf, super::super::test_support::VERSION)
            .expect("encode request");
        buf.freeze()
    }

    fn request(group_id: &str, topic_id: ProtoUuid, partition: i32) -> ReadShareGroupStateRequest {
        ReadShareGroupStateRequest {
            group_id: group_id.into(),
            topics: vec![ReadStateData {
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
    async fn returns_persisted_state_batches_for_led_partition() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let (broker_handle, broker) =
            super::super::test_support::broker_with_led_share_coordinator(dir.path()).await;
        let topic_id = uuid::Uuid::from_bytes([33; 16]);
        let wire_topic_id = ProtoUuid(*topic_id.as_bytes());
        broker
            .share_coordinator
            .initialize("share-group", topic_id, 4, 17, Offset(90))
            .await
            .expect("initialize state");
        broker
            .share_coordinator
            .write(
                "share-group",
                topic_id,
                4,
                (17, 3),
                (Offset(101), 9),
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
        let bytes = super::handle(
            &broker,
            super::super::test_support::VERSION,
            123,
            &req_bytes,
        )
        .await
        .expect("handle");
        let resp = decode(&bytes);

        let expected = ReadShareGroupStateResponse {
            results: vec![ReadStateResult {
                topic_id: wire_topic_id,
                partitions: vec![PartitionResult {
                    partition: 4,
                    error_code: codes::NONE,
                    error_message: None,
                    state_epoch: 17,
                    start_offset: 101,
                    state_batches: vec![StateBatch {
                        first_offset: 101,
                        last_offset: 105,
                        delivery_state: 2,
                        delivery_count: 3,
                        unknown_tagged_fields: UnknownTaggedFields(vec![]),
                    }],
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
    async fn returns_initial_state_for_led_missing_partition() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let (broker_handle, broker) =
            super::super::test_support::broker_with_led_share_coordinator(dir.path()).await;
        let topic_id = ProtoUuid([34; 16]);
        let req = request("share-group", topic_id, 6);
        let req_bytes = encode_request(&req);

        broker
            .share_coordinator
            .lead_all_partitions_for_test()
            .await;
        let bytes = super::handle(
            &broker,
            super::super::test_support::VERSION,
            123,
            &req_bytes,
        )
        .await
        .expect("handle");
        let resp = decode(&bytes);

        let expected = ReadShareGroupStateResponse {
            results: vec![ReadStateResult {
                topic_id,
                partitions: vec![PartitionResult {
                    partition: 6,
                    error_code: codes::NONE,
                    error_message: None,
                    state_epoch: 0,
                    start_offset: -1,
                    state_batches: vec![],
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
        let topic_id = ProtoUuid([35; 16]);
        let req = request("share-group", topic_id, 8);
        let req_bytes = encode_request(&req);

        let bytes = super::handle(
            &broker,
            super::super::test_support::VERSION,
            123,
            &req_bytes,
        )
        .await
        .expect("handle");
        let resp = decode(&bytes);

        let expected = ReadShareGroupStateResponse {
            results: vec![ReadStateResult {
                topic_id,
                partitions: vec![PartitionResult {
                    partition: 8,
                    error_code: codes::NOT_COORDINATOR,
                    error_message: None,
                    state_epoch: 0,
                    start_offset: -1,
                    state_batches: vec![],
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
