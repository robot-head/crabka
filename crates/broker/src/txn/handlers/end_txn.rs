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
//!    - **remote** leader → `WriteTxnMarkersRequest` over the shared
//!      `InterBrokerClient` (runs inter-broker TLS / SASL as the listener demands).
//! 4. `PrepareCommit` → `CompleteCommit` (or `PrepareAbort` → `CompleteAbort`); persist.
//! 5. Return `NONE` to the producer.
//!
//! Wire format: v0-2 non-flexible, v3-5 flexible (tagged fields).
//! Request fields: `transactional_id`, `producer_id`, `producer_epoch`, `committed`.
//! Response fields: `throttle_time_ms`, `error_code`.

use std::collections::HashMap;

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, MetadataImage, NodeId, ResourceType};
use crabka_protocol::Decode;
use crabka_protocol::Encode;
use crabka_protocol::owned::end_txn_request::EndTxnRequest;
use crabka_protocol::owned::end_txn_response::EndTxnResponse;
use crabka_protocol::owned::write_txn_markers_request::{
    WritableTxnMarker, WritableTxnMarkerTopic, WriteTxnMarkersRequest,
};
use crabka_security::ListenerProtocol;

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::coordinator::bootstrap::OFFSETS_TOPIC;
use crate::error::BrokerError;
use crate::network::client::InterBrokerClient;
use crate::txn::marker::{MarkerType, build_marker_batch};
use crate::txn::partitioner::partition_for_tid;
use crate::txn::state::{TopicPartition, TxnEntry, TxnState};
use crate::txn::util::now_millis;

/// Number of partitions in `__consumer_offsets`. Bootstrap creates a
/// 1-partition topic (`OFFSETS_PARTITION = 0`), so all group-ids map to
/// partition 0. Documented here so it's easy to wire up the 50-partition
/// topology once we get there.
const OFFSETS_NUM_PARTITIONS: i32 = 1;

#[allow(clippy::too_many_lines)]
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
    let authorizer = broker.config.authorizer.as_ref();
    let mut cur: &[u8] = req_bytes;
    let req = EndTxnRequest::decode(&mut cur, version)?;

    // Refresh leader-partition view from the current metadata image
    // before checking coordinator-ness.
    let image = controller.current_image();
    coord.refresh_leader_partitions(&image).await;

    let tid = req.transactional_id.as_str();

    // ── ACL preamble: Write on TransactionalId ─────────────
    let tid_req = AuthorizationRequest {
        principal: ctx.principal,
        host: ctx.peer,
        resource_type: ResourceType::TransactionalId,
        resource_name: tid,
        operation: AclOperation::Write,
    };
    if authorizer.authorize(&image, &tid_req) == AuthorizationResult::Deny {
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

    if let Err(e) = dispatch_markers(
        node_id,
        &partitions,
        &prepare_snap,
        marker_type,
        &image,
        &broker.inter_broker_client,
        broker.inter_broker_listener_protocol,
        &broker.config.inter_broker_listener_name,
    )
    .await
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
    //
    // The entry lock was *intentionally* dropped before the Phase-2 marker
    // fan-out (network I/O to remote brokers); holding it across the fan-out
    // would serialize/deadlock the coordinator. That window lets a concurrent
    // caller (another EndTxn, an AddPartitionsToTxn, or an InitProducerId that
    // bumps the epoch) interleave on this same transactional-id.
    //
    // We must NOT re-lock the original `entry_mutex` captured at the top of the
    // handler: `coord.put` replaces the coordinator's map slot with a *fresh*
    // `Arc<Mutex<TxnEntry>>` on every persist (see `TxnCoordinator::put`), so a
    // concurrent caller operates on a different Arc than the one we hold. The
    // only authoritative view is the entry currently registered under `tid`.
    //
    // Re-fetch it and re-validate that nothing advanced underneath us BEFORE
    // writing Complete. If the producer was fenced (epoch bumped) or the state
    // was advanced by another caller, abort this handler's Complete write and
    // return the matching Kafka error instead of blindly overwriting.
    let Some(current_mutex) = coord.get(tid) else {
        // The entry vanished (e.g. expired/deleted) while markers were in
        // flight. Treat as a producer-mapping loss.
        return encode_err(version, codes::INVALID_PRODUCER_ID_MAPPING);
    };

    let complete_snap: TxnEntry = {
        let mut entry = current_mutex.lock().await;
        match validate_complete_reacquire(
            &entry,
            req.producer_id,
            req.producer_epoch,
            prepare,
            complete,
        ) {
            ReacquireDecision::Proceed => {}
            ReacquireDecision::AlreadyComplete => {
                // Another caller already drove this exact transition to
                // completion (or we are an idempotent EndTxn retry that lost
                // the race). The desired post-state is already persisted, so
                // report success without re-writing.
                return encode_ok(version);
            }
            ReacquireDecision::Reject(code) => {
                tracing::warn!(
                    tid,
                    expected_epoch = req.producer_epoch,
                    found_epoch = entry.producer_epoch,
                    expected_state = ?prepare,
                    found_state = ?entry.state,
                    error_code = code,
                    "EndTxn: entry changed underneath the marker fan-out; \
                     aborting Complete write"
                );
                return encode_err(version, code);
            }
        }
        entry.state = complete;
        entry.last_update_ms = now_millis();
        entry.clone()
        // Lock dropped here.
    };

    // FINAL put: move `complete_snap` in (no use-after-move below) to avoid the
    // redundant full `TxnEntry` clone (incl. the partition / offset-commit-group
    // sets) that the intermediate phases pay.
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

/// Decision for the Phase-3 (Complete) re-acquire re-validation. See
/// [`validate_complete_reacquire`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReacquireDecision {
    /// State is exactly as this handler left it after Prepare; write Complete.
    Proceed,
    /// The entry already advanced to the Complete state this handler intended
    /// (idempotent retry / lost race). Report success without re-writing.
    AlreadyComplete,
    /// The entry changed in a way that means this handler must NOT write
    /// Complete. Return this Kafka error code to the producer.
    Reject(i16),
}

/// Re-validate, after re-acquiring the coordinator's *current* entry for a
/// transactional-id, that it is safe to finalise the transaction.
///
/// `expected_epoch` / `expected_pid` are the producer identity this `EndTxn`
/// handler validated and acted on. `prepare` is the state this handler wrote
/// in Phase 1; `complete` is the state it is about to write.
///
/// Returns:
/// - [`ReacquireDecision::Reject`] with `INVALID_PRODUCER_EPOCH` if the pid or
///   epoch no longer matches (a concurrent `InitProducerId` fenced us). Apache
///   Kafka maps a stale producer epoch on `EndTxn` to `INVALID_PRODUCER_EPOCH`
///   (a.k.a. `PRODUCER_FENCED` for the newer producer client).
/// - [`ReacquireDecision::AlreadyComplete`] if the entry is already in the
///   exact `complete` state we intended — another caller (or an `EndTxn` retry)
///   finished the transition; finalising again would be a redundant overwrite.
/// - [`ReacquireDecision::Reject`] with `INVALID_TXN_STATE` if the state is
///   anything other than the `prepare` we left it in (e.g. advanced to
///   `Ongoing` by a concurrent `AddPartitionsToTxn`, or into the *opposite*
///   prepare/complete kind), meaning our marker fan-out no longer reflects the
///   live transaction and we must not finalise.
/// - [`ReacquireDecision::Proceed`] only when the epoch matches and the state
///   is still exactly `prepare`.
fn validate_complete_reacquire(
    entry: &TxnEntry,
    expected_pid: i64,
    expected_epoch: i16,
    prepare: TxnState,
    complete: TxnState,
) -> ReacquireDecision {
    if entry.producer_id != expected_pid || entry.producer_epoch != expected_epoch {
        return ReacquireDecision::Reject(codes::INVALID_PRODUCER_EPOCH);
    }
    if entry.state == prepare {
        return ReacquireDecision::Proceed;
    }
    if entry.state == complete {
        return ReacquireDecision::AlreadyComplete;
    }
    ReacquireDecision::Reject(codes::INVALID_TXN_STATE)
}

// ── marker fan-out ────────────────────────────────────────────────────────────

/// Dispatch `WriteTxnMarkers` to every partition leader involved in the
/// transaction. Groups partitions by leader node:
///
/// - **local** (leader == `node_id`): directly calls
///   [`Partition::produce_batch`] on the in-memory handle.
/// - **remote**: sends a [`WriteTxnMarkersRequest`] over the shared
///   [`InterBrokerClient`], which runs TLS / SASL when the inter-broker
///   listener demands them.
///
/// `__consumer_offsets` partitions are added for each group in
/// `entry.offset_commit_groups`.
#[allow(clippy::too_many_arguments)]
async fn dispatch_markers(
    node_id: NodeId,
    partitions: &std::sync::Arc<crate::partition_registry::PartitionRegistry>,
    entry: &TxnEntry,
    marker_type: MarkerType,
    image: &MetadataImage,
    inter_broker_client: &InterBrokerClient,
    inter_broker_protocol: ListenerProtocol,
    inter_broker_listener_name: &str,
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
    // offset-commit group. `__consumer_offsets` is currently a 1-partition
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
                let Some(part) = partitions.get(&tp.topic, tp.partition) else {
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
            send_write_txn_markers(
                node_id,
                leader,
                entry,
                marker_type,
                &tps,
                image,
                inter_broker_client,
                inter_broker_protocol,
                inter_broker_listener_name,
            )
            .await?;
        }
    }

    Ok(())
}

/// Send a `WriteTxnMarkersRequest` to a remote broker that leads one or more
/// of the transaction's partitions.
///
/// Dials through the shared [`InterBrokerClient`] so the connection
/// terminates TLS and runs the SASL client handshake whenever the
/// inter-broker listener demands them. The previous implementation opened a
/// one-shot `crabka_client_core::Client` per call, which carried no TLS
/// connector and no inter-broker credentials — marker fan-out therefore only
/// succeeded against a PLAINTEXT inter-broker listener and silently broke
/// transactions spanning remote-led partitions on any secured cluster.
///
/// ## Coordinator epoch
///
/// Apache Kafka tracks a per-coordinator epoch that increments on each
/// leadership change. Leader-election-on-failure is not yet implemented, so we
/// hard-code `coordinator_epoch = 0` here. Once coordinator failover is
/// implemented the caller must supply the real epoch.
#[allow(clippy::too_many_arguments)]
async fn send_write_txn_markers(
    my_node_id: NodeId,
    leader_node: NodeId,
    entry: &TxnEntry,
    marker_type: MarkerType,
    tps: &[TopicPartition],
    image: &MetadataImage,
    inter_broker_client: &InterBrokerClient,
    inter_broker_protocol: ListenerProtocol,
    inter_broker_listener_name: &str,
) -> Result<(), BrokerError> {
    let Some(broker_info) = image.broker(leader_node) else {
        return Err(BrokerError::Txn(format!(
            "EndTxn: leader node {leader_node} not found in metadata image"
        )));
    };

    // Prefer the leader's inter-broker listener endpoint when it has projected
    // one onto its registration record; fall back to the legacy top-level
    // host/port. Mirrors the resolution in the replicator supervisor and
    // heartbeat client — the marker RPC must target the same listener whose
    // protocol we dial with.
    let (host, port) = broker_info
        .endpoints
        .iter()
        .find(|e| e.name == inter_broker_listener_name)
        .map_or_else(
            || (broker_info.host.clone(), broker_info.port),
            |e| (e.host.clone(), e.port),
        );

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
            // Hard-coded to 0. Coordinator leader-change tracking (real
            // epoch increment on failover) is not yet implemented.
            coordinator_epoch: 0,
            ..Default::default()
        }],
        ..Default::default()
    };

    // SNI server_name "localhost" matches the convention used by the
    // replicator / heartbeat / auto-join inter-broker dials.
    let opts = crabka_client_core::ConnectionOptions {
        client_id: format!("crabka-broker-txn-{my_node_id}"),
        ..crabka_client_core::ConnectionOptions::default()
    };
    let conn = inter_broker_client
        .connect_as_connection(&host, port, inter_broker_protocol, "localhost", opts)
        .await
        .map_err(|e| BrokerError::Txn(format!("EndTxn: connect to {host}:{port}: {e}")))?;

    // `Connection::send` negotiates the wire version from the broker-advertised
    // ApiVersions table established during connect.
    let _resp = conn
        .send(req)
        .await
        .map_err(|e| BrokerError::Txn(format!("EndTxn: WriteTxnMarkers to {host}:{port}: {e}")))?;

    conn.close();
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

#[cfg(test)]
mod tests {
    use super::{ReacquireDecision, validate_complete_reacquire};
    use crate::codes;
    use crate::txn::state::{TxnEntry, TxnState};

    /// Build a `TxnEntry` in a given (pid, epoch, state) for the re-validation
    /// tests. Partition sets are irrelevant to the decision, so leave empty.
    fn entry(pid: i64, epoch: i16, state: TxnState) -> TxnEntry {
        let mut e = TxnEntry::new_empty("tid-x".into(), pid, epoch, 60_000, 1);
        e.state = state;
        e
    }

    #[test]
    fn proceeds_when_unchanged() {
        // Entry is exactly as Phase 1 left it: same pid/epoch, still in Prepare.
        let e = entry(7, 3, TxnState::PrepareCommit);
        assert_eq!(
            validate_complete_reacquire(
                &e,
                7,
                3,
                TxnState::PrepareCommit,
                TxnState::CompleteCommit
            ),
            ReacquireDecision::Proceed
        );
    }

    #[test]
    fn fenced_when_epoch_bumped() {
        // A concurrent InitProducerId bumped the epoch during the marker
        // fan-out. We must NOT overwrite with the stale epoch / Complete state.
        let e = entry(7, 4, TxnState::PrepareCommit);
        assert_eq!(
            validate_complete_reacquire(
                &e,
                7,
                3,
                TxnState::PrepareCommit,
                TxnState::CompleteCommit
            ),
            ReacquireDecision::Reject(codes::INVALID_PRODUCER_EPOCH)
        );
    }

    #[test]
    fn fenced_when_pid_changed() {
        let e = entry(8, 3, TxnState::PrepareCommit);
        assert_eq!(
            validate_complete_reacquire(
                &e,
                7,
                3,
                TxnState::PrepareCommit,
                TxnState::CompleteCommit
            ),
            ReacquireDecision::Reject(codes::INVALID_PRODUCER_EPOCH)
        );
    }

    #[test]
    fn idempotent_when_already_complete() {
        // Another caller (or an EndTxn retry that lost the race) already drove
        // this exact transition. Report success, do not re-write.
        let e = entry(7, 3, TxnState::CompleteCommit);
        assert_eq!(
            validate_complete_reacquire(
                &e,
                7,
                3,
                TxnState::PrepareCommit,
                TxnState::CompleteCommit
            ),
            ReacquireDecision::AlreadyComplete
        );
    }

    #[test]
    fn rejects_when_advanced_to_ongoing() {
        // A concurrent AddPartitionsToTxn re-opened the txn (Complete→Ongoing
        // reuse, or some other interleave). Our marker fan-out no longer
        // reflects the live transaction; refuse to finalise.
        let e = entry(7, 3, TxnState::Ongoing);
        assert_eq!(
            validate_complete_reacquire(
                &e,
                7,
                3,
                TxnState::PrepareCommit,
                TxnState::CompleteCommit
            ),
            ReacquireDecision::Reject(codes::INVALID_TXN_STATE)
        );
    }

    #[test]
    fn rejects_when_opposite_prepare_kind() {
        // We prepared a Commit, but the entry is now in PrepareAbort — a
        // different finalisation kind raced us. Refuse to write CompleteCommit.
        let e = entry(7, 3, TxnState::PrepareAbort);
        assert_eq!(
            validate_complete_reacquire(
                &e,
                7,
                3,
                TxnState::PrepareCommit,
                TxnState::CompleteCommit
            ),
            ReacquireDecision::Reject(codes::INVALID_TXN_STATE)
        );
    }

    #[test]
    fn abort_path_proceeds_and_is_idempotent() {
        // Mirror the abort branch: prepare=PrepareAbort, complete=CompleteAbort.
        let prep = entry(7, 3, TxnState::PrepareAbort);
        assert_eq!(
            validate_complete_reacquire(
                &prep,
                7,
                3,
                TxnState::PrepareAbort,
                TxnState::CompleteAbort
            ),
            ReacquireDecision::Proceed
        );
        let done = entry(7, 3, TxnState::CompleteAbort);
        assert_eq!(
            validate_complete_reacquire(
                &done,
                7,
                3,
                TxnState::PrepareAbort,
                TxnState::CompleteAbort
            ),
            ReacquireDecision::AlreadyComplete
        );
    }
}
