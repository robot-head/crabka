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
                            error_code: codes::NONE,
                            error_message: None,
                            state_epoch,
                            leader_epoch,
                            start_offset,
                            delivery_complete_count,
                            ..Default::default()
                        },
                        None => PartitionResult {
                            partition: pd.partition,
                            error_code: codes::NONE,
                            error_message: None,
                            state_epoch: 0,
                            leader_epoch: 0,
                            start_offset: -1,
                            delivery_complete_count: 0,
                            ..Default::default()
                        },
                    }
                } else {
                    PartitionResult {
                        partition: pd.partition,
                        error_code: codes::NOT_COORDINATOR,
                        error_message: None,
                        state_epoch: 0,
                        leader_epoch: 0,
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
