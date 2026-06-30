//! `ReadShareGroupState` (`api_key=84`). Returns the durable delivery state
//! (start offset + state batches) for each `(group, topic, partition)`. A
//! partition this broker does not lead returns per-partition `NOT_COORDINATOR`;
//! an unknown-but-led key returns the initial/empty state (`start_offset = -1`,
//! no batches) with `error_code = 0`.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::read_share_group_state_request::ReadShareGroupStateRequest;
use crabka_protocol::owned::read_share_group_state_response::{
    PartitionResult, ReadShareGroupStateResponse, ReadStateResult, StateBatch,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::share_coordinator::coordinator::ShareCoordinator;

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
                        start_offset: st.start_offset,
                        state_batches: st
                            .state_batches
                            .iter()
                            .map(|b| StateBatch {
                                first_offset: b.first_offset,
                                last_offset: b.last_offset,
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
                        start_offset: -1,
                        ..Default::default()
                    },
                }
            } else {
                PartitionResult {
                    partition: pd.partition,
                    error_code: codes::NOT_COORDINATOR,
                    start_offset: -1,
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
    use super::*;
    use assert2::assert;
    use crabka_protocol::owned::read_share_group_state_request::{PartitionData, ReadStateData};
    use crabka_protocol::owned::read_share_group_state_response::ReadShareGroupStateResponse;
    use crabka_protocol::primitives::uuid::Uuid as ProtoUuid;

    fn decode(bytes: &Bytes) -> ReadShareGroupStateResponse {
        let mut cur: &[u8] = bytes.as_ref();
        let resp =
            ReadShareGroupStateResponse::decode(&mut cur, super::super::test_support::VERSION)
                .expect("decode response");
        assert!(cur.is_empty(), "response decoder consumed all bytes");
        resp
    }

    #[tokio::test]
    async fn returns_persisted_state_batches_for_led_partition() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let coordinator = super::super::test_support::led_coordinator(dir.path()).await;
        let topic_id = uuid::Uuid::from_bytes([33; 16]);
        let wire_topic_id = ProtoUuid(*topic_id.as_bytes());
        coordinator
            .initialize("share-group", topic_id, 4, 17, 90)
            .await
            .expect("initialize state");
        coordinator
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
        let req = ReadShareGroupStateRequest {
            group_id: "share-group".into(),
            topics: vec![ReadStateData {
                topic_id: wire_topic_id,
                partitions: vec![PartitionData {
                    partition: 4,
                    leader_epoch: 3,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let bytes = handle_request(coordinator, super::super::test_support::VERSION, req)
            .await
            .expect("handle request");
        let resp = decode(&bytes);

        assert!(resp.results.len() == 1);
        assert!(resp.results[0].topic_id == wire_topic_id);
        assert!(resp.results[0].partitions.len() == 1);
        let partition = &resp.results[0].partitions[0];
        assert!(partition.partition == 4);
        assert!(partition.error_code == codes::NONE);
        assert!(partition.error_message.is_none());
        assert!(partition.state_epoch == 17);
        assert!(partition.start_offset == 101);
        assert!(partition.state_batches.len() == 1);
        let batch = &partition.state_batches[0];
        assert!(batch.first_offset == 101);
        assert!(batch.last_offset == 105);
        assert!(batch.delivery_state == 2);
        assert!(batch.delivery_count == 3);
    }
}
