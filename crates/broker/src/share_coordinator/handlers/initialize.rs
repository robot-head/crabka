//! `InitializeShareGroupState` (`api_key=83`). Seeds the durable share state
//! for each `(group, topic, partition)` at the requested `state_epoch` /
//! `start_offset`. Gates every partition on local leadership of its
//! `__share_group_state` partition.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::initialize_share_group_state_request::InitializeShareGroupStateRequest;
use crabka_protocol::owned::initialize_share_group_state_response::{
    InitializeShareGroupStateResponse, InitializeStateResult, PartitionResult,
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
        let req = InitializeShareGroupStateRequest::decode(&mut cur, version)?;
        let group_id = req.group_id;

        let mut results: Vec<InitializeStateResult> = Vec::with_capacity(req.topics.len());
        for topic in req.topics {
            let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
            let mut partitions: Vec<PartitionResult> = Vec::with_capacity(topic.partitions.len());
            for pd in topic.partitions {
                let state_partition =
                    coordinator.state_partition_for(&group_id, &topic_id, pd.partition);
                let error_code = if coordinator.is_leader(state_partition).await {
                    match coordinator
                        .initialize(
                            &group_id,
                            topic_id,
                            pd.partition,
                            pd.state_epoch,
                            pd.start_offset,
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
            results.push(InitializeStateResult {
                topic_id: topic.topic_id,
                partitions,
                ..Default::default()
            });
        }

        let resp = InitializeShareGroupStateResponse {
            results,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
