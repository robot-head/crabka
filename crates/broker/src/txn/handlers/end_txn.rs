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
use crate::coordinator::unified::actor::{GroupActorMessage, GroupKindTag};
use crate::error::BrokerError;
use crate::network::client::InterBrokerClient;
use crate::txn::decision::{
    CompletionDecision, decide_end_txn_completion, decide_phase1_transition,
};
use crate::txn::marker::{MarkerType, build_marker_batch};
use crate::txn::state::{TopicPartition, TxnEntry, TxnState};
use crate::txn::util::now_millis;
use crate::txn::version::TxnVersion;

/// A producer's identity pair as carried on the wire:
/// (`producer_id`, `producer_epoch`).
pub(crate) type ProducerIdentity = (i64, i16);

/// Kafka wire sentinel: "no producer id" (`RecordBatch.NO_PRODUCER_ID`).
/// Returned on `EndTxn` error responses, where the identity is meaningless.
const NO_PRODUCER_ID: i64 = -1;

/// Kafka wire sentinel: "no producer epoch" (`RecordBatch.NO_PRODUCER_EPOCH`).
const NO_PRODUCER_EPOCH: i16 = -1;

/// Coordinator epoch stamped on outgoing `WriteTxnMarkers`. Apache Kafka
/// increments it on each coordinator leadership change; coordinator failover
/// tracking is not implemented yet, so every marker carries the initial epoch.
const INITIAL_COORDINATOR_EPOCH: i32 = 0;

/// TLS SNI server name used on inter-broker dials — matches the convention
/// of the replicator / heartbeat / auto-join inter-broker clients.
const INTER_BROKER_SNI: &str = "localhost";

#[allow(clippy::too_many_lines)]
#[tracing::instrument(
    name = "handle_end_txn",
    level = "info",
    skip_all,
    fields(api = "EndTxn", version, req_bytes = req_bytes.len()),
    err,
)]
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
    let txnv = crate::txn::version::resolve_txn_version(&image);
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
    if authorizer.authorize(&*image, &tid_req) == AuthorizationResult::Deny {
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

    let marker_type = if req.committed {
        MarkerType::Commit
    } else {
        MarkerType::Abort
    };

    let (prepare, complete, prepare_snap): (TxnState, TxnState, TxnEntry) = {
        let mut entry = entry_mutex.lock().await;
        match decide_phase1_transition(&mut entry, req.committed) {
            Ok((prepare, complete)) => {
                entry.last_update_ms = now_millis();
                (prepare, complete, entry.clone())
            }
            Err(code) => return encode_err(version, code),
        }
        // Lock dropped here.
    };

    if let Err(e) = coord.put(prepare_snap.clone(), txnv).await {
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

    // The (pid, epoch) returned to the producer (see `next_producer_identity`).
    // At TV_2 the epoch is bumped by one on completion; on epoch exhaustion the
    // producer rolls to a new producer_id at epoch 0. Below TV_2 both are the
    // producer's current values unchanged. Both are assigned on the Proceed
    // path below (the other re-acquire branches return early).
    let response_pid;
    let response_epoch;

    let complete_snap: TxnEntry = {
        let mut entry = current_mutex.lock().await;
        // KIP-890: on the Proceed path `decide_end_txn_completion` bumps the
        // producer epoch (at TV_2) so a zombie holding the old epoch is fenced
        // WITHOUT a fresh InitProducerId. The bump is applied AFTER the Phase-2
        // marker fan-out (markers were written with the old/current epoch); only
        // the persisted and returned identity reflects it. On epoch exhaustion
        // the producer rolls to a freshly-allocated producer_id at epoch 0.
        match decide_end_txn_completion(
            &entry,
            req.producer_id,
            req.producer_epoch,
            prepare,
            complete,
            txnv,
            &coord.producer_ids,
        ) {
            CompletionDecision::Proceed {
                next_state,
                response_pid: new_pid,
                response_epoch: new_epoch,
            } => {
                if new_pid != entry.producer_id {
                    // Epoch rolled over to a new producer_id: record the prior id
                    // so the transition is traceable (KIP-890 PreviousProducerId).
                    entry.prev_producer_id = entry.producer_id;
                }
                entry.state = next_state;
                entry.last_update_ms = now_millis();
                entry.producer_id = new_pid;
                entry.producer_epoch = new_epoch;
                response_pid = new_pid;
                response_epoch = new_epoch;
                entry.clone()
            }
            CompletionDecision::AlreadyComplete {
                response_pid: pid,
                response_epoch: epoch,
            } => {
                // Another caller already drove this exact transition to
                // completion (or we are an idempotent EndTxn retry that lost the
                // race). Report success without re-writing, returning the
                // persisted (possibly already-bumped) identity so a KIP-890
                // client that retried picks up the authoritative value.
                return encode_ok(version, pid, epoch);
            }
            CompletionDecision::Reject(code) => {
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
        // Lock dropped here.
    };

    // FINAL put: move `complete_snap` in (no use-after-move below) to avoid the
    // redundant full `TxnEntry` clone (incl. the partition / offset-commit-group
    // sets) that the intermediate phases pay.
    if let Err(e) = coord.put(complete_snap, txnv).await {
        tracing::error!(
            tid,
            state = ?complete,
            error = %e,
            "EndTxn: failed to persist CompleteCommit/CompleteAbort"
        );
        return encode_err(version, codes::UNKNOWN_SERVER_ERROR);
    }

    // ── KIP-447: materialize buffered transactional consumer offsets ──────
    //
    // A consume-process-produce producer folds its source offsets into the
    // transaction via `AddOffsetsToTxn` + `TxnOffsetCommit`, which appended
    // them to `__consumer_offsets` (held under the LSO) AND buffered them on
    // the txn coordinator keyed by `producer_id`. The Phase-2 marker fan-out
    // above just wrote the COMMIT/ABORT marker to those partitions; nothing
    // else surfaces the buffered offsets into the group coordinator's
    // in-memory `committed_offsets` (the map `OffsetFetch` reads). Do it here,
    // exactly at the commit-marker boundary (Kafka makes txn offsets visible
    // to `OffsetFetch` only AFTER the commit marker):
    //
    // - COMMIT (`req.committed`): drain the producer's buffer and apply each
    //   group's offsets via the same `UpdateCommitted` actor message a normal
    //   `OffsetCommit` uses, so a restarting EOS consumer resumes from them.
    // - ABORT: still drain (to free the buffer) but discard — aborted offsets
    //   must never become committed.
    //
    // Keyed by `req.producer_id` (the buffer key from `TxnOffsetCommit`), not
    // the post-completion `response_pid`, which may have been epoch-bumped or
    // rolled to a fresh id at TV_2.
    materialize_txn_offsets(broker, req.producer_id, req.committed).await;

    encode_ok(version, response_pid, response_epoch)
}

/// Apply (on COMMIT) or drop (on ABORT) the transactional consumer offsets
/// buffered for `producer_id` under each consumer `group_id`. On COMMIT the
/// offsets are written into the owning group's in-memory `committed_offsets`
/// via the group actor's `UpdateCommitted` message — the same path a normal
/// `OffsetCommit` uses and the same map `OffsetFetch` reads — so they become
/// visible to `OffsetFetch` only now, after the commit marker. The buffer is
/// always drained (even on abort) so a producer's pending offsets can't leak.
///
/// Single-broker MVP: every consumer group is local, so the owning group's
/// actor is found (or created) on this broker. A multi-broker future would
/// route each group's offsets to its `__consumer_offsets`-partition leader.
async fn materialize_txn_offsets(broker: &Broker, producer_id: i64, committed: bool) {
    let pending = broker.txn_coordinator.take_txn_offsets(producer_id);
    if !committed || pending.is_empty() {
        // Abort (or nothing buffered): the take above already dropped it.
        return;
    }
    for (group_id, entries) in pending {
        if entries.is_empty() {
            continue;
        }
        let handle = broker.group_coordinator.find(&group_id).unwrap_or_else(|| {
            broker
                .group_coordinator
                .get_or_create_group(&group_id, GroupKindTag::Classic)
        });
        let (tx, rx) = tokio::sync::oneshot::channel();
        if handle
            .tx
            .send(GroupActorMessage::UpdateCommitted { entries, reply: tx })
            .await
            .is_ok()
        {
            let _ = rx.await;
        } else {
            tracing::warn!(
                group = %group_id,
                producer_id,
                "EndTxn: group actor unavailable; could not materialize txn offsets"
            );
        }
    }
}

/// KIP-890: the `(producer_id, producer_epoch)` a producer continues with after
/// a transaction completes.
///
/// - Below `TV_2`: unchanged — the epoch only moves on `InitProducerId` reuse.
/// - `TV_2`, normal: same `producer_id`, `epoch + 1` — bumping on completion
///   fences a zombie holding the old epoch without a fresh `InitProducerId`.
/// - `TV_2`, epoch exhaustion (`epoch == i16::MAX`): the epoch can't bump, so a
///   *new* `producer_id` is allocated (`epoch` reset to 0). The caller records
///   the old id as the entry's `prev_producer_id` so the transition is
///   traceable. The `EndTxn` v5 response returns the new pair and the producer
///   adopts it for its next transaction.
pub(crate) fn next_producer_identity(
    txnv: TxnVersion,
    pid: i64,
    epoch: i16,
    ids: &crate::producer_id_manager::ProducerIdManager,
) -> ProducerIdentity {
    if !txnv.verified() {
        return (pid, epoch);
    }
    match epoch.checked_add(1) {
        Some(bumped) => (pid, bumped),
        // Epoch exhausted: roll to a fresh producer_id at epoch 0 (KIP-890).
        None => ids.allocate(),
    }
}

/// Decision for the Phase-3 (Complete) re-acquire re-validation. See
/// [`validate_complete_reacquire`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReacquireDecision {
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
pub(crate) fn validate_complete_reacquire(
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
/// Any `__consumer_offsets` partitions registered via `AddOffsetsToTxn` live
/// in `entry.partitions` (Kafka's model has no separate group list), so they
/// are fanned out by the same loop as data partitions.
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
            // Coordinator leader-change tracking (real epoch increment on
            // failover) is not yet implemented; see the constant's doc.
            coordinator_epoch: INITIAL_COORDINATOR_EPOCH,
            ..Default::default()
        }],
        ..Default::default()
    };

    let opts = crabka_client_core::ConnectionOptions {
        client_id: format!("crabka-broker-txn-{my_node_id}"),
        ..crabka_client_core::ConnectionOptions::default()
    };
    let conn = inter_broker_client
        .connect_as_connection(&host, port, inter_broker_protocol, INTER_BROKER_SNI, opts)
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
    // On the error path the producer_id/epoch fields are not meaningful;
    // leave them at the "no producer" wire sentinels.
    encode_response(version, error_code, NO_PRODUCER_ID, NO_PRODUCER_EPOCH)
}

/// Encode a successful `EndTxn` response. `producer_id` / `producer_epoch` are
/// the post-completion identity (the epoch is bumped at `TV_2`, or rolls to a
/// new `producer_id` on epoch exhaustion; see [`next_producer_identity`]). They
/// are only on the wire at v5 (KIP-890); at
/// lower versions the producer never observes them, and the persisted bump
/// fences a stale-epoch producer on its next coordinator call instead.
fn encode_ok(version: i16, producer_id: i64, producer_epoch: i16) -> Result<Bytes, BrokerError> {
    encode_response(version, codes::NONE, producer_id, producer_epoch)
}

fn encode_response(
    version: i16,
    error_code: i16,
    producer_id: i64,
    producer_epoch: i16,
) -> Result<Bytes, BrokerError> {
    let resp = EndTxnResponse {
        throttle_time_ms: 0,
        error_code,
        producer_id,
        producer_epoch,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_metadata::{BrokerEndpoint, BrokerRegistrationRecord, MetadataRecord};
    use crabka_protocol::UnknownTaggedFields;
    use crabka_protocol::owned::end_txn_response::EndTxnResponse;

    fn decode_response(bytes: &Bytes, version: i16) -> EndTxnResponse {
        crate::test_support::decode_response(bytes, version)
    }

    // ── KIP-890 TV_2 completion identity: next_producer_identity ────────────

    #[test]
    fn encode_err_leaves_producer_identity_at_error_sentinels() {
        let bytes = encode_err(5, codes::NOT_COORDINATOR).expect("encode error");
        assert!(!bytes.is_empty());
        let resp = decode_response(&bytes, 5);

        let expected = EndTxnResponse {
            throttle_time_ms: 0,
            error_code: codes::NOT_COORDINATOR,
            producer_id: -1,
            producer_epoch: -1,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }

    #[test]
    fn encode_ok_returns_v5_producer_identity() {
        let bytes = encode_ok(5, 42, 7).expect("encode ok");
        assert!(!bytes.is_empty());
        let resp = decode_response(&bytes, 5);

        let expected = EndTxnResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            producer_id: 42,
            producer_epoch: 7,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }

    #[test]
    fn epoch_bumps_only_at_tv2() {
        use crate::txn::version::TxnVersion;
        let ids = crate::producer_id_manager::ProducerIdManager::new();
        let cases = [
            // Below TV_2 (Classic, Flexible): pid + epoch unchanged.
            (TxnVersion::Classic, (7, 3)),
            (TxnVersion::Flexible, (7, 3)),
            // TV_2 (Verified) non-overflow: same pid, epoch + 1.
            (TxnVersion::Verified, (7, 4)),
        ];
        for (version, want) in cases {
            assert!(
                next_producer_identity(version, 7, 3, &ids) == want,
                "txn version {version:?}"
            );
        }
    }

    #[test]
    fn epoch_overflow_at_tv2_allocates_new_pid_at_epoch_zero() {
        use crate::txn::version::TxnVersion;
        let ids = crate::producer_id_manager::ProducerIdManager::new();
        // At i16::MAX the epoch can't bump: a fresh producer_id is allocated
        // (monotonic from PID_BASE) and the epoch resets to 0. No panic.
        let (new_pid, new_epoch) = next_producer_identity(TxnVersion::Verified, 7, i16::MAX, &ids);
        assert!(new_pid != 7);
        assert!(new_epoch == 0);
        // The allocator hands out a distinct pid on the next overflow too.
        let (next_pid, _) = next_producer_identity(TxnVersion::Verified, 7, i16::MAX, &ids);
        assert!(next_pid != new_pid);
        // Below TV_2 at i16::MAX: no roll, epoch stays (no bump path taken).
        assert!(next_producer_identity(TxnVersion::Classic, 7, i16::MAX, &ids) == (7, i16::MAX));
    }

    // ── Phase-3 re-validation: validate_complete_reacquire ──────────────────

    /// Build a `TxnEntry` in a given (pid, epoch, state) for the re-validation
    /// tests. Partition sets are irrelevant to the decision, so leave empty.
    fn entry(pid: i64, epoch: i16, state: TxnState) -> TxnEntry {
        let mut e = TxnEntry::new_empty("tid-x".into(), pid, epoch, 60_000, 1);
        e.state = state;
        e
    }

    #[test]
    fn commit_reacquire_decision_matrix() {
        // Phase 1 left (pid=7, epoch=3, PrepareCommit); the reacquire always
        // asks to drive PrepareCommit → CompleteCommit.
        // (observed_pid, observed_epoch, observed_state, expected)
        let cases = [
            // Entry is exactly as Phase 1 left it: same pid/epoch, still in
            // Prepare — proceed.
            (7, 3, TxnState::PrepareCommit, ReacquireDecision::Proceed),
            // A concurrent InitProducerId bumped the epoch during the marker
            // fan-out. We must NOT overwrite with the stale epoch / Complete
            // state.
            (
                7,
                4,
                TxnState::PrepareCommit,
                ReacquireDecision::Reject(codes::INVALID_PRODUCER_EPOCH),
            ),
            // Producer id changed underneath us — fenced.
            (
                8,
                3,
                TxnState::PrepareCommit,
                ReacquireDecision::Reject(codes::INVALID_PRODUCER_EPOCH),
            ),
            // Another caller (or an EndTxn retry that lost the race) already
            // drove this exact transition. Report success, do not re-write.
            (
                7,
                3,
                TxnState::CompleteCommit,
                ReacquireDecision::AlreadyComplete,
            ),
            // A concurrent AddPartitionsToTxn re-opened the txn
            // (Complete→Ongoing reuse, or some other interleave). Our marker
            // fan-out no longer reflects the live transaction; refuse to
            // finalise.
            (
                7,
                3,
                TxnState::Ongoing,
                ReacquireDecision::Reject(codes::INVALID_TXN_STATE),
            ),
            // We prepared a Commit, but the entry is now in PrepareAbort — a
            // different finalisation kind raced us. Refuse to write
            // CompleteCommit.
            (
                7,
                3,
                TxnState::PrepareAbort,
                ReacquireDecision::Reject(codes::INVALID_TXN_STATE),
            ),
        ];
        for (pid, epoch, state, expected) in cases {
            let e = entry(pid, epoch, state);
            let decision = validate_complete_reacquire(
                &e,
                7,
                3,
                TxnState::PrepareCommit,
                TxnState::CompleteCommit,
            );
            assert!(
                decision == expected,
                "observed pid {pid}, epoch {epoch}, state {state:?}"
            );
        }
    }

    #[test]
    fn abort_path_proceeds_and_is_idempotent() {
        // Mirror the abort branch: prepare=PrepareAbort, complete=CompleteAbort.
        let prep = entry(7, 3, TxnState::PrepareAbort);
        assert!(
            validate_complete_reacquire(
                &prep,
                7,
                3,
                TxnState::PrepareAbort,
                TxnState::CompleteAbort
            ) == ReacquireDecision::Proceed
        );
        let done = entry(7, 3, TxnState::CompleteAbort);
        assert!(
            validate_complete_reacquire(
                &done,
                7,
                3,
                TxnState::PrepareAbort,
                TxnState::CompleteAbort
            ) == ReacquireDecision::AlreadyComplete
        );
    }

    // ── Remote marker fan-out: send_write_txn_markers error / fallback
    //    branches the happy-path integration test does not reach. ───────────

    fn marker_entry() -> TxnEntry {
        TxnEntry::new_empty("tid".to_string(), 7, 0, 60_000, 0)
    }

    fn tps() -> Vec<TopicPartition> {
        vec![TopicPartition {
            topic: "t".to_string(),
            partition: 0,
        }]
    }

    /// A client with no TLS connector and no SASL creds — fine here, every
    /// case fails at the TCP connect (unreachable address) before any
    /// handshake would run.
    fn plaintext_client() -> InterBrokerClient {
        InterBrokerClient::new(None, None)
    }

    /// Leader node absent from the metadata image → descriptive `Txn` error,
    /// no dial attempted.
    #[tokio::test]
    async fn errors_when_leader_node_missing_from_image() {
        let image = MetadataImage::default();
        let err = send_write_txn_markers(
            1,
            99,
            &marker_entry(),
            MarkerType::Commit,
            &tps(),
            &image,
            &plaintext_client(),
            ListenerProtocol::Plaintext,
            "PLAINTEXT",
        )
        .await
        .expect_err("missing leader must error");
        assert!(
            matches!(&err, BrokerError::Txn(m) if m.contains("not found")),
            "unexpected error: {err:?}"
        );
    }

    /// Leader resolves to its inter-broker endpoint, but the address is
    /// unreachable → the dial fails and the error names the resolved
    /// `host:port` (the endpoint, not the top-level fallback).
    #[tokio::test]
    async fn errors_when_inter_broker_endpoint_unreachable() {
        let mut image = MetadataImage::default();
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: 2,
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::nil(),
                host: "127.0.0.1".to_string(),
                port: 9,
                rack: None,
                endpoints: vec![BrokerEndpoint {
                    name: "INTERNAL".to_string(),
                    host: "127.0.0.1".to_string(),
                    // Discard port: refuses connections immediately.
                    port: 9,
                    protocol: ListenerProtocol::Plaintext,
                }],
            },
        ));
        let err = send_write_txn_markers(
            1,
            2,
            &marker_entry(),
            MarkerType::Commit,
            &tps(),
            &image,
            &plaintext_client(),
            ListenerProtocol::Plaintext,
            "INTERNAL",
        )
        .await
        .expect_err("unreachable endpoint must error");
        assert!(
            matches!(&err, BrokerError::Txn(m) if m.contains("connect to 127.0.0.1:9")),
            "unexpected error: {err:?}"
        );
    }

    /// No endpoint matches the inter-broker listener name → fall back to the
    /// record's top-level `host`/`port`. Still unreachable, so the dial fails
    /// against the fallback address.
    #[tokio::test]
    async fn falls_back_to_top_level_host_port_when_no_matching_endpoint() {
        let mut image = MetadataImage::default();
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: 2,
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::nil(),
                host: "127.0.0.1".to_string(),
                port: 9,
                rack: None,
                // Endpoint exists but under a different listener name, so the
                // `find(name == inter_broker_listener_name)` misses.
                endpoints: vec![BrokerEndpoint {
                    name: "SOMETHING_ELSE".to_string(),
                    host: "127.0.0.1".to_string(),
                    port: 65000,
                    protocol: ListenerProtocol::Plaintext,
                }],
            },
        ));
        let err = send_write_txn_markers(
            1,
            2,
            &marker_entry(),
            MarkerType::Commit,
            &tps(),
            &image,
            &plaintext_client(),
            ListenerProtocol::Plaintext,
            "INTERNAL",
        )
        .await
        .expect_err("unreachable fallback must error");
        assert!(
            matches!(&err, BrokerError::Txn(m) if m.contains("connect to 127.0.0.1:9")),
            "expected fallback to top-level 127.0.0.1:9, got: {err:?}"
        );
    }
}
