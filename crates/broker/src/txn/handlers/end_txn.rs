//! `EndTxn` (`api_key=26`). Finalises a transaction — the producer calls
//! `commitTransaction()` or `abortTransaction()`, which drives two state
//! transitions with a `WriteTxnMarkers` fan-out in between.
//!
//! ## Flow
//!
//! 1. Verify coordinator-ness, pid, epoch.
//! 2. `Ongoing` → `PrepareCommit` (or `PrepareAbort`); persist.
//! 3. Fan out `WriteTxnMarkers` to every involved partition's leader:
//!    - **local** leader  → `Partition::produce_batch`.
//!    - **remote** leader → `WriteTxnMarkersRequest` via `crabka_client_core`.
//! 4. `PrepareCommit` → `CompleteCommit` (or `PrepareAbort` → `CompleteAbort`); persist.
//! 5. Return `NONE` to the producer.
//!
//! Wire format: v0-2 non-flexible, v3-5 flexible (tagged fields).
//! Request fields: `transactional_id`, `producer_id`, `producer_epoch`, `committed`.
//! Response fields: `throttle_time_ms`, `error_code`.

use std::collections::HashMap;

use bytes::{Bytes, BytesMut};

use crabka_client_core::Client;
use crabka_metadata::{AclOperation, MetadataImage, NodeId, ResourceType};
use crabka_protocol::Decode;
use crabka_protocol::Encode;
use crabka_protocol::owned::end_txn_request::EndTxnRequest;
use crabka_protocol::owned::end_txn_response::EndTxnResponse;
use crabka_protocol::owned::write_txn_markers_request::{
    WritableTxnMarker, WritableTxnMarkerTopic, WriteTxnMarkersRequest,
};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult, authorize};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::bootstrap::OFFSETS_TOPIC;
use crate::error::BrokerError;
use crate::txn::marker::{MarkerType, build_marker_batch};
use crate::txn::partitioner::partition_for_tid;
use crate::txn::state::{TopicPartition, TxnEntry, TxnState};
use crate::txn::util::now_millis;

/// Number of partitions in `__consumer_offsets`. Slice 5 bootstraps a
/// 1-partition topic (`OFFSETS_PARTITION = 0`), so all group-ids map to
/// partition 0 for this slice. Document here so it's easy to wire up the
/// 50-partition topology once we get there.
const OFFSETS_NUM_PARTITIONS: i32 = 1;

pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let coord = broker.txn_coordinator.clone();
    let controller = broker.controller.clone();
    let partitions = broker.partitions.clone();
    let node_id = broker.config.node_id;
    let super_users = &broker.config.super_users;
    let mut cur: &[u8] = req_bytes;
    let req = EndTxnRequest::decode(&mut cur, version)?;

    // Refresh leader-partition view from the current metadata image
    // before checking coordinator-ness (mirrors Task 12/13/14 pattern).
    let image = controller.current_image();
    coord.refresh_leader_partitions(&image).await;

    let tid = req.transactional_id.as_str();

    // ── slice-13 ACL preamble: Write on TransactionalId ─────────────
    let tid_req = AuthorizationRequest {
        principal: ctx.principal,
        host: ctx.peer,
        resource_type: ResourceType::TransactionalId,
        resource_name: tid,
        operation: AclOperation::Write,
    };
    if authorize(&image, super_users, &tid_req) == AuthorizationResult::Deny {
        return encode_err(version, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
    }

    if !coord.is_coordinator_for(tid).await {
        return encode_err(version, codes::NOT_COORDINATOR);
    }

    let Some(entry_mutex) = coord.get(tid) else {
        return encode_err(version, codes::INVALID_PRODUCER_ID_MAPPING);
    };

    {
        let entry = entry_mutex.lock().await;
        if entry.producer_id != req.producer_id || entry.producer_epoch != req.producer_epoch {
            return encode_err(version, codes::INVALID_PRODUCER_EPOCH);
        }
    }

    // ── Phase 1: Ongoing → Prepare{Commit,Abort} ──────────────────────

    let prepare = if req.committed {
        TxnState::PrepareCommit
    } else {
        TxnState::PrepareAbort
    };
    let complete = if req.committed {
        TxnState::CompleteCommit
    } else {
        TxnState::CompleteAbort
    };
    let marker_type = if req.committed {
        MarkerType::Commit
    } else {
        MarkerType::Abort
    };

    let prepare_snap: TxnEntry = {
        let mut entry = entry_mutex.lock().await;
        if !entry.state.can_transition_to(prepare) {
            return encode_err(version, codes::INVALID_TXN_STATE);
        }
        entry.state = prepare;
        entry.last_update_ms = now_millis();
        entry.clone()
        // Lock dropped here.
    };

    if let Err(e) = coord.put(prepare_snap.clone()).await {
        tracing::error!(
            tid,
            state = ?prepare,
            error = %e,
            "EndTxn: failed to persist PrepareCommit/PrepareAbort"
        );
        return encode_err(version, codes::UNKNOWN_SERVER_ERROR);
    }

    // ── Phase 2: Fan out WriteTxnMarkers ──────────────────────────────

    if let Err(e) = dispatch_markers(node_id, &partitions, &prepare_snap, marker_type, &image).await
    {
        tracing::error!(
            tid,
            error = %e,
            "EndTxn: WriteTxnMarkers fan-out failed; returning retriable error"
        );
        // Return a retriable error; the producer will retry EndTxn.
        return encode_err(version, codes::UNKNOWN_SERVER_ERROR);
    }

    // ── Phase 3: Prepare{Commit,Abort} → Complete{Commit,Abort} ───────

    let complete_snap: TxnEntry = {
        let mut entry = entry_mutex.lock().await;
        entry.state = complete;
        entry.last_update_ms = now_millis();
        entry.clone()
        // Lock dropped here.
    };

    if let Err(e) = coord.put(complete_snap).await {
        tracing::error!(
            tid,
            state = ?complete,
            error = %e,
            "EndTxn: failed to persist CompleteCommit/CompleteAbort"
        );
        return encode_err(version, codes::UNKNOWN_SERVER_ERROR);
    }

    encode_ok(version)
}

// ── marker fan-out ────────────────────────────────────────────────────────────

/// Dispatch `WriteTxnMarkers` to every partition leader involved in the
/// transaction. Groups partitions by leader node:
///
/// - **local** (leader == `node_id`): directly calls
///   [`Partition::produce_batch`] on the in-memory handle.
/// - **remote**: sends a [`WriteTxnMarkersRequest`] over a fresh
///   inter-broker `crabka_client_core` connection.
///
/// `__consumer_offsets` partitions are added for each group in
/// `entry.offset_commit_groups`.
async fn dispatch_markers(
    node_id: NodeId,
    partitions: &std::sync::Arc<
        dashmap::DashMap<(String, i32), std::sync::Arc<crate::partition::Partition>>,
    >,
    entry: &TxnEntry,
    marker_type: MarkerType,
    image: &MetadataImage,
) -> Result<(), BrokerError> {
    // Group every involved (topic, partition) by its current leader.
    let mut by_leader: HashMap<NodeId, Vec<TopicPartition>> = HashMap::new();

    for tp in &entry.partitions {
        let leader = image
            .partition(&tp.topic, tp.partition)
            .map_or(node_id, |p| p.leader);
        by_leader.entry(leader).or_default().push(tp.clone());
    }

    // Also add the `__consumer_offsets` partition for each transactional
    // offset-commit group. Slice 5 uses a 1-partition `__consumer_offsets`
    // topic, so `partition_for_tid(group_id, 1)` always returns 0.
    for group_id in &entry.offset_commit_groups {
        let part_idx = partition_for_tid(group_id, OFFSETS_NUM_PARTITIONS);
        let tp = TopicPartition {
            topic: OFFSETS_TOPIC.to_string(),
            partition: part_idx,
        };
        let leader = image
            .partition(OFFSETS_TOPIC, part_idx)
            .map_or(node_id, |p| p.leader);
        by_leader.entry(leader).or_default().push(tp);
    }

    for (leader, tps) in by_leader {
        if leader == node_id {
            // Local path: directly append a marker batch to each partition.
            for tp in &tps {
                let Some(part) = partitions
                    .get(&(tp.topic.clone(), tp.partition))
                    .map(|e| e.value().clone())
                else {
                    tracing::warn!(
                        topic = %tp.topic,
                        partition = tp.partition,
                        "EndTxn: local partition not found; skipping marker"
                    );
                    continue;
                };
                let base_offset = part.log_end_offset();
                let marker = build_marker_batch(
                    entry.producer_id,
                    entry.producer_epoch,
                    base_offset,
                    marker_type,
                );
                part.produce_batch(marker).await?;
            }
        } else {
            // Remote path: send WriteTxnMarkersRequest to the leader.
            send_write_txn_markers(node_id, leader, entry, marker_type, &tps, image).await?;
        }
    }

    Ok(())
}

/// Send a `WriteTxnMarkersRequest` to a remote broker that leads one or more
/// of the transaction's partitions.
///
/// Opens a fresh TCP connection per call — adequate for slice 9's correctness
/// goal. A connection pool can be added in slice 10+.
///
/// ## Coordinator epoch
///
/// Apache Kafka tracks a per-coordinator epoch that increments on each
/// leadership change. Slice 9 defers leader-election-on-failure, so we
/// hard-code `coordinator_epoch = 0` here. Once coordinator failover is
/// implemented the caller must supply the real epoch.
async fn send_write_txn_markers(
    my_node_id: NodeId,
    leader_node: NodeId,
    entry: &TxnEntry,
    marker_type: MarkerType,
    tps: &[TopicPartition],
    image: &MetadataImage,
) -> Result<(), BrokerError> {
    let Some(broker_info) = image.broker(leader_node) else {
        return Err(BrokerError::Txn(format!(
            "EndTxn: leader node {leader_node} not found in metadata image"
        )));
    };

    let addr = format!("{}:{}", broker_info.host, broker_info.port);
    let client_id = format!("crabka-broker-txn-{my_node_id}");

    let client = Client::builder()
        .bootstrap(addr.clone())
        .client_id(client_id)
        .build()
        .await
        .map_err(|e| BrokerError::Txn(format!("EndTxn: connect to {addr}: {e}")))?;

    // Group tps by topic for the nested WritableTxnMarkerTopic structure.
    let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
    for tp in tps {
        by_topic
            .entry(tp.topic.clone())
            .or_default()
            .push(tp.partition);
    }

    let topics: Vec<WritableTxnMarkerTopic> = by_topic
        .into_iter()
        .map(|(name, partition_indexes)| WritableTxnMarkerTopic {
            name,
            partition_indexes,
            ..Default::default()
        })
        .collect();

    let req = WriteTxnMarkersRequest {
        markers: vec![WritableTxnMarker {
            producer_id: entry.producer_id,
            producer_epoch: entry.producer_epoch,
            transaction_result: marker_type == MarkerType::Commit,
            topics,
            // Hard-coded to 0 for slice 9. Coordinator leader-change
            // tracking (real epoch increment on failover) is deferred to
            // slice 10+.
            coordinator_epoch: 0,
            ..Default::default()
        }],
        ..Default::default()
    };

    // Use the minimum supported version for WriteTxnMarkersRequest (v1).
    let _resp = client
        .send(req)
        .await
        .map_err(|e| BrokerError::Txn(format!("EndTxn: WriteTxnMarkers to {addr}: {e}")))?;

    client.close();
    Ok(())
}

// ── encoding helpers ──────────────────────────────────────────────────────────

fn encode_err(version: i16, error_code: i16) -> Result<Bytes, BrokerError> {
    encode_response(version, error_code)
}

fn encode_ok(version: i16) -> Result<Bytes, BrokerError> {
    encode_response(version, codes::NONE)
}

fn encode_response(version: i16, error_code: i16) -> Result<Bytes, BrokerError> {
    let resp = EndTxnResponse {
        throttle_time_ms: 0,
        error_code,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
