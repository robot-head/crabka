//! `WriteShareGroupState` (`api_key=85`). Applies a delivery-state delta
//! (advance start offset, upsert state batches, bump leader/state epoch +
//! delivery-complete count) for each `(group, topic, partition)`. Gates on
//! local leadership and surfaces epoch fencing as the per-partition error code.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::write_share_group_state_request::WriteShareGroupStateRequest;
use crabka_protocol::owned::write_share_group_state_response::{
    PartitionResult, WriteShareGroupStateResponse, WriteStateResult,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::share_coordinator::persistence::StateBatch;

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
                            first_offset: b.first_offset,
                            last_offset: b.last_offset,
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
                            pd.start_offset,
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
