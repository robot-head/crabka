//! `TxnOffsetCommit` (`api_key=28`). The consumer side of the
//! consume-process-produce pattern. A transactional producer that also
//! reads commits its consumed offsets atomically with its transaction by
//! appending them to `__consumer_offsets` with `is_transactional=true` +
//! the producer's (pid, epoch). The offsets are held under the partition's
//! LSO until a `WriteTxnMarkers` commit or abort marker arrives.
//!
//! Versions 0–2: non-flexible (no `generation_id`/`member_id` fields).
//! Versions 3–5: flexible (tagged fields; adds `generation_id`, `member_id`,
//!               `group_instance_id`).

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::txn_offset_commit_request::TxnOffsetCommitRequest;
use crabka_protocol::owned::txn_offset_commit_response::{
    TxnOffsetCommitResponse, TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
};
use crabka_protocol::records::{Attributes, Record, RecordBatch};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::coordinator::bootstrap::{OFFSETS_PARTITION, OFFSETS_TOPIC};
use crate::coordinator::persistence::OffsetCommitValue;
use crate::error::BrokerError;
use crate::txn::util::now_millis;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let partitions = broker.partitions.clone();
    let group_manager = broker.group_manager.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = TxnOffsetCommitRequest::decode(&mut cur, version)?;

        // 1. Verify the group coordinator is this broker.  In the current
        //    single-broker MVP every group is local. We check that the group
        //    exists (or create it) — if the partition for __consumer_offsets
        //    is not present we'll detect that below and return NOT_COORDINATOR.
        //    For a multi-broker future this would route to the leader for
        //    hash(group_id) % __consumer_offsets.partition_count.
        let _handle = group_manager.get_or_create(&req.group_id);

        // 2. KIP-1319 stale-member-epoch check (api_version >= 3 adds
        //    generation_id/member_id). Slice-5's GroupManager does not yet
        //    expose a `member_epoch()` accessor, so we emit ILLEGAL_GENERATION
        //    only when the request carries a non-default generation_id that
        //    differs from the group's current generation_id (classic protocol).
        //    TODO(KIP-1319 v4+): implement per-member epoch tracking and
        //    surface STALE_MEMBER_EPOCH (82) when supplied epoch < current.
        if version >= 3 && req.generation_id >= 0 {
            let group_handle = group_manager.get_or_create(&req.group_id);
            let g = group_handle.state.lock().await;
            if g.generation_id >= 0 && req.generation_id != g.generation_id {
                drop(g);
                return encode_err_all(version, &req, codes::ILLEGAL_GENERATION);
            }
            drop(g);
        }

        // 3. Append a transactional RecordBatch to __consumer_offsets.
        //    We reuse the OffsetCommitKey/Value layout but stamp the batch with
        //    is_transactional=true + (producer_id, producer_epoch) so the log's
        //    LSO machinery holds the offsets until EndTxn commits/aborts.
        let now_ms = now_millis();
        if let Err(code) = append_txn_batch(&req, &partitions, now_ms).await {
            return encode_err_all(version, &req, code);
        }

        // 4. Success — per-(topic, partition) error_code = NONE.
        encode_ok_all(version, &req)
    })
}

// ── batch construction ────────────────────────────────────────────────────────

async fn append_txn_batch(
    req: &TxnOffsetCommitRequest,
    partitions: &std::sync::Arc<
        dashmap::DashMap<(String, i32), std::sync::Arc<crate::partition::Partition>>,
    >,
    now_ms: i64,
) -> Result<(), i16> {
    let mut batch = RecordBatch {
        attributes: Attributes::default().with_transactional(true),
        max_timestamp: now_ms,
        producer_id: req.producer_id,
        producer_epoch: req.producer_epoch,
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
        // __consumer_offsets not hosted here — report NOT_COORDINATOR.
        return Err(codes::NOT_COORDINATOR);
    };
    // `produce_batch` drives the single-writer task and returns the
    // assigned base_offset; we don't need it here.
    part_handle
        .produce_batch(batch)
        .await
        .map(|_| ())
        .map_err(|e| {
            tracing::error!(
                group = %req.group_id,
                tid   = %req.transactional_id,
                error = %e,
                "TxnOffsetCommit: produce_batch failed"
            );
            codes::UNKNOWN_SERVER_ERROR
        })
}

// ── response helpers ──────────────────────────────────────────────────────────

fn build_response(req: &TxnOffsetCommitRequest, code: i16) -> TxnOffsetCommitResponse {
    let topics = req
        .topics
        .iter()
        .map(|t| TxnOffsetCommitResponseTopic {
            name: t.name.clone(),
            partitions: t
                .partitions
                .iter()
                .map(|p| TxnOffsetCommitResponsePartition {
                    partition_index: p.partition_index,
                    error_code: code,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    TxnOffsetCommitResponse {
        throttle_time_ms: 0,
        topics,
        ..Default::default()
    }
}

fn encode_resp(version: i16, resp: &TxnOffsetCommitResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

fn encode_err_all(
    version: i16,
    req: &TxnOffsetCommitRequest,
    code: i16,
) -> Result<Bytes, BrokerError> {
    encode_resp(version, &build_response(req, code))
}

fn encode_ok_all(version: i16, req: &TxnOffsetCommitRequest) -> Result<Bytes, BrokerError> {
    encode_resp(version, &build_response(req, codes::NONE))
}
