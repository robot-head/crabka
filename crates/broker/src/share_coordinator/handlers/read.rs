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
                            error_code: codes::NONE,
                            error_message: None,
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
                            error_code: codes::NONE,
                            error_message: None,
                            state_epoch: 0,
                            start_offset: -1,
                            state_batches: Vec::new(),
                            ..Default::default()
                        },
                    }
                } else {
                    PartitionResult {
                        partition: pd.partition,
                        error_code: codes::NOT_COORDINATOR,
                        error_message: None,
                        state_epoch: 0,
                        start_offset: -1,
                        state_batches: Vec::new(),
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
    })
}
