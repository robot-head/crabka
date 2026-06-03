//! `AlterPartition` (`api_key=56`). Controller-side ISR update handler.
//!
//! Validates that this broker is the openraft leader (`NOT_CONTROLLER` if not),
//! checks leader-epoch fencing per partition, validates that the proposed ISR
//! is a non-empty subset of the partition's replicas, and submits the updated
//! `PartitionRecord` via `controller.submit_change`.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_metadata::{MetadataRecord, PartitionRecord};
use crabka_protocol::owned::alter_partition_request::AlterPartitionRequest;
use crabka_protocol::owned::alter_partition_response::{
    AlterPartitionResponse, PartitionData as RespPartitionData, TopicData as RespTopicData,
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
    let controller = broker.controller.clone();
    let node_id = broker.config.node_id;

    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = AlterPartitionRequest::decode(&mut cur, version)?;

        // Only the openraft leader handles AlterPartition.
        let is_leader = controller
            .watch_leader()
            .borrow()
            .is_some_and(|n| n == node_id);
        if !is_leader {
            return encode_resp(
                version,
                &AlterPartitionResponse {
                    throttle_time_ms: 0,
                    error_code: codes::NOT_CONTROLLER,
                    ..Default::default()
                },
            );
        }

        let image = controller.current_image();
        let mut changes: Vec<MetadataRecord> = Vec::new();
        let mut resp_topics: Vec<RespTopicData> = Vec::new();

        for req_topic in &req.topics {
            // Find the topic name via topic_id from the metadata image.
            let topic_name_opt = image
                .topics()
                .find(|t| t.topic_id.as_bytes() == &req_topic.topic_id.0)
                .map(|t| t.name.clone());

            let mut resp_partitions: Vec<RespPartitionData> = Vec::new();
            for req_part in &req_topic.partitions {
                let resp_part = handle_partition(
                    &image,
                    topic_name_opt.as_deref(),
                    req_part.partition_index,
                    req_part.leader_epoch,
                    &req_part.new_isr,
                    &req_part.new_isr_with_epochs,
                    &mut changes,
                );
                resp_partitions.push(resp_part);
            }

            resp_topics.push(RespTopicData {
                topic_id: req_topic.topic_id,
                partitions: resp_partitions,
                ..Default::default()
            });
        }

        if !changes.is_empty()
            && let Err(e) = controller.submit_change(changes).await
        {
            return Err(BrokerError::Replication(format!("submit_change: {e}")));
        }

        encode_resp(
            version,
            &AlterPartitionResponse {
                throttle_time_ms: 0,
                error_code: codes::NONE,
                topics: resp_topics,
                ..Default::default()
            },
        )
    })
}

/// Validate and apply a single partition's ISR proposal. Returns the
/// per-partition response data and appends to `changes` on success.
///
/// `new_isr_i32` carries the v2 `new_isr` field; `new_isr_with_epochs`
/// carries the v3 field. For v3 requests `new_isr` is empty and
/// `new_isr_with_epochs` is populated — this function falls back to
/// extracting broker IDs from `new_isr_with_epochs` when `new_isr`
/// is empty.
fn handle_partition(
    image: &crabka_metadata::MetadataImage,
    topic_name: Option<&str>,
    partition_index: i32,
    req_leader_epoch: i32,
    new_isr_i32: &[i32],
    new_isr_with_epochs: &[crabka_protocol::owned::alter_partition_request::BrokerState],
    changes: &mut Vec<MetadataRecord>,
) -> RespPartitionData {
    let Some(topic_name) = topic_name else {
        return error_part(
            partition_index,
            codes::UNKNOWN_TOPIC_OR_PARTITION,
            0,
            0,
            &[],
        );
    };
    let Some(part_rec) = image.partition(topic_name, partition_index) else {
        return error_part(
            partition_index,
            codes::UNKNOWN_TOPIC_OR_PARTITION,
            0,
            0,
            &[],
        );
    };

    let leader_i32 = i32::try_from(part_rec.leader).unwrap_or(0);
    let current_isr_i32: Vec<i32> = part_rec
        .isr
        .iter()
        .map(|n| i32::try_from(*n).unwrap_or(0))
        .collect();

    // Leader-epoch fencing.
    if req_leader_epoch != part_rec.leader_epoch {
        return error_part(
            partition_index,
            codes::FENCED_LEADER_EPOCH,
            leader_i32,
            part_rec.leader_epoch,
            &current_isr_i32,
        );
    }

    // Resolve the effective ISR from the request. Protocol v2 sends
    // `new_isr: Vec<i32>`; v3 sends `new_isr_with_epochs` instead and
    // leaves `new_isr` empty. Fall back to extracting broker_ids from
    // `new_isr_with_epochs` when the v2 field is absent.
    let effective_isr_i32: &[i32];
    let fallback_isr_i32: Vec<i32>;
    if new_isr_i32.is_empty() && !new_isr_with_epochs.is_empty() {
        fallback_isr_i32 = new_isr_with_epochs.iter().map(|bs| bs.broker_id).collect();
        effective_isr_i32 = &fallback_isr_i32;
    } else {
        effective_isr_i32 = new_isr_i32;
    }

    // Validate proposed ISR: non-empty + subset of replicas.
    let proposed_isr: Vec<u64> = effective_isr_i32
        .iter()
        .map(|&n| u64::try_from(n).unwrap_or(0))
        .collect();
    let replicas_set: std::collections::HashSet<u64> = part_rec.replicas.iter().copied().collect();
    let valid = !proposed_isr.is_empty() && proposed_isr.iter().all(|n| replicas_set.contains(n));
    if !valid {
        return error_part(
            partition_index,
            codes::INVALID_REQUEST,
            leader_i32,
            part_rec.leader_epoch,
            &current_isr_i32,
        );
    }

    // Success: submit the ISR change.
    changes.push(MetadataRecord::V1Partition(PartitionRecord {
        topic: topic_name.to_string(),
        partition: partition_index,
        leader: part_rec.leader,
        replicas: part_rec.replicas.clone(),
        isr: proposed_isr,
        leader_epoch: part_rec.leader_epoch,
        adding_replicas: part_rec.adding_replicas.clone(),
        removing_replicas: part_rec.removing_replicas.clone(),
        directories: part_rec.directories.clone(),
    }));

    RespPartitionData {
        partition_index,
        error_code: codes::NONE,
        leader_id: leader_i32,
        leader_epoch: part_rec.leader_epoch,
        isr: effective_isr_i32.to_vec(),
        leader_recovery_state: 0,
        partition_epoch: 0,
        ..Default::default()
    }
}

fn error_part(
    partition_index: i32,
    error_code: i16,
    leader_id: i32,
    leader_epoch: i32,
    isr: &[i32],
) -> RespPartitionData {
    RespPartitionData {
        partition_index,
        error_code,
        leader_id,
        leader_epoch,
        isr: isr.to_vec(),
        leader_recovery_state: 0,
        partition_epoch: 0,
        ..Default::default()
    }
}

fn encode_resp(version: i16, resp: &AlterPartitionResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
