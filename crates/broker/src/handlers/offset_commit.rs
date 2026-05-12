//! `OffsetCommit` (`api_key=8`). Encodes `OffsetCommitKey` +
//! `OffsetCommitValue` records, appends them to `__consumer_offsets-0`
//! via the partition writer, then updates `Group.committed_offsets`.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::offset_commit_request::OffsetCommitRequest;
use crabka_protocol::owned::offset_commit_response::{
    OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use crabka_protocol::records::{Record, RecordBatch};
use crabka_protocol::{Decode, Encode};
use dashmap::DashMap;
use tokio::sync::oneshot;

use crate::broker::Broker;
use crate::codes;
use crate::coordinator::GroupHandle;
use crate::coordinator::bootstrap::{OFFSETS_PARTITION, OFFSETS_TOPIC};
use crate::coordinator::group::OffsetEntry;
use crate::coordinator::persistence::OffsetCommitValue;
use crate::error::BrokerError;
use crate::partition::{Partition, ProduceJob};

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let group_manager = broker.group_manager.clone();
    let partitions = broker.partitions.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = OffsetCommitRequest::decode(&mut cur, version)?;

        let now_ms = now_ms();
        let handle = group_manager.get_or_create(&req.group_id);

        // 1. Validate (group, generation, member). Empty `member_id` means
        //    a "simple" consumer that doesn't participate in a group — skip
        //    the membership/generation check entirely.
        if let Some(code) = validate(&req, &handle).await {
            let resp = build_response_all(&req, code);
            return encode(version, &resp);
        }

        // 2. Append a RecordBatch into `__consumer_offsets-0`.
        if let Err(code) = append_batch(&req, &partitions, now_ms).await {
            let resp = build_response_all(&req, code);
            return encode(version, &resp);
        }

        // 3. Update in-memory state.
        update_committed(&req, &handle, now_ms).await;

        // 4. Uniform per-(topic, partition) success.
        let resp = build_response_all(&req, codes::NONE);
        encode(version, &resp)
    })
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(0)
}

/// Returns `Some(error_code)` if the request should be rejected.
async fn validate(req: &OffsetCommitRequest, handle: &Arc<GroupHandle>) -> Option<i16> {
    if req.member_id.is_empty() {
        return None;
    }
    let g = handle.state.lock().await;
    if !g.members.contains_key(&req.member_id) {
        return Some(codes::UNKNOWN_MEMBER_ID);
    }
    if g.generation_id != req.generation_id_or_member_epoch {
        return Some(codes::ILLEGAL_GENERATION);
    }
    None
}

/// Append a single `RecordBatch` covering every (topic, partition) in `req`
/// to the `__consumer_offsets-0` writer. Returns `Err(error_code)` on
/// failure to either find the partition or hear back from the writer.
async fn append_batch(
    req: &OffsetCommitRequest,
    partitions: &Arc<DashMap<(String, i32), Arc<Partition>>>,
    now_ms: i64,
) -> Result<(), i16> {
    let mut batch = RecordBatch {
        max_timestamp: now_ms,
        ..RecordBatch::default()
    };
    let mut delta: i32 = 0;
    for topic in &req.topics {
        for part in &topic.partitions {
            let value = OffsetCommitValue {
                offset: part.committed_offset,
                leader_epoch: part.committed_leader_epoch,
                metadata: part.committed_metadata.clone().unwrap_or_default(),
                commit_timestamp_ms: now_ms,
            };
            batch.records.push(Record {
                offset_delta: delta,
                timestamp_delta: 0,
                key: Some(OffsetCommitValue::encode_key(
                    &req.group_id,
                    &topic.name,
                    part.partition_index,
                )),
                value: Some(value.encode_value()),
                ..Default::default()
            });
            delta += 1;
        }
    }
    batch.last_offset_delta = (delta - 1).max(0);

    let Some(part_handle) = partitions
        .get(&(OFFSETS_TOPIC.to_string(), OFFSETS_PARTITION))
        .map(|e| e.value().clone())
    else {
        return Err(codes::UNKNOWN_SERVER_ERROR);
    };
    let (ack_tx, ack_rx) = oneshot::channel();
    if part_handle
        .writer_tx
        .send(ProduceJob { batch, ack: ack_tx })
        .await
        .is_err()
    {
        return Err(codes::UNKNOWN_SERVER_ERROR);
    }
    match ack_rx.await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "OffsetCommit writer returned error");
            Err(codes::from_broker_error(&e))
        }
        Err(e) => {
            tracing::error!(error = %e, "OffsetCommit writer ack dropped");
            Err(codes::UNKNOWN_SERVER_ERROR)
        }
    }
}

async fn update_committed(req: &OffsetCommitRequest, handle: &Arc<GroupHandle>, now_ms: i64) {
    let mut g = handle.state.lock().await;
    for topic in &req.topics {
        for part in &topic.partitions {
            g.committed_offsets.insert(
                (topic.name.clone(), part.partition_index),
                OffsetEntry {
                    offset: part.committed_offset,
                    leader_epoch: part.committed_leader_epoch,
                    metadata: part.committed_metadata.clone().unwrap_or_default(),
                    commit_timestamp_ms: now_ms,
                },
            );
        }
    }
}

fn build_response_all(req: &OffsetCommitRequest, code: i16) -> OffsetCommitResponse {
    let topics = req
        .topics
        .iter()
        .map(|t| OffsetCommitResponseTopic {
            name: t.name.clone(),
            partitions: t
                .partitions
                .iter()
                .map(|p| OffsetCommitResponsePartition {
                    partition_index: p.partition_index,
                    error_code: code,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    OffsetCommitResponse {
        topics,
        throttle_time_ms: 0,
        ..Default::default()
    }
}

fn encode(version: i16, resp: &OffsetCommitResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
