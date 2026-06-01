//! `DeleteShareGroupState` (`api_key=86`). Tombstones the durable share state
//! for each `(group, topic, partition)` and drops the in-memory entry. Gates on
//! local leadership of the target `__share_group_state` partition.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::delete_share_group_state_request::DeleteShareGroupStateRequest;
use crabka_protocol::owned::delete_share_group_state_response::{
    DeleteShareGroupStateResponse, DeleteStateResult, PartitionResult,
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
        let req = DeleteShareGroupStateRequest::decode(&mut cur, version)?;
        let group_id = req.group_id;

        let mut results: Vec<DeleteStateResult> = Vec::with_capacity(req.topics.len());
        for topic in req.topics {
            let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
            let mut partitions: Vec<PartitionResult> = Vec::with_capacity(topic.partitions.len());
            for pd in topic.partitions {
                let state_partition =
                    coordinator.state_partition_for(&group_id, &topic_id, pd.partition);
                let error_code = if coordinator.is_leader(state_partition).await {
                    match coordinator.delete(&group_id, topic_id, pd.partition).await {
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
            results.push(DeleteStateResult {
                topic_id: topic.topic_id,
                partitions,
                ..Default::default()
            });
        }

        let resp = DeleteShareGroupStateResponse {
            results,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
