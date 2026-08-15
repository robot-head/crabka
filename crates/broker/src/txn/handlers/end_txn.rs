//! `EndTxn` (`api_key=26`). Finalises a transaction. The producer calls
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
use crabka_log::ProducerId;
use crabka_metadata::{AclOperation, MetadataImage, NodeId, ResourceType};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        end_txn_request::EndTxnRequest,
        end_txn_response::EndTxnResponse,
        write_txn_markers_request::{
            WritableTxnMarker, WritableTxnMarkerTopic, WriteTxnMarkersRequest,
        },
        write_txn_markers_response::WriteTxnMarkersResponse,
    },
};
use crabka_security::ListenerProtocol;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
    network::client::InterBrokerClient,
    txn::{
        decision::{CompletionDecision, decide_end_txn_completion, decide_phase1_transition},
        handlers::write_txn_markers::{MarkerAppend, append_marker_and_materialize},
        marker::MarkerType,
        state::{TopicPartition, TxnEntry, TxnState},
        util::now_millis,
        version::TxnVersion,
    },
};

/// Kafka wire sentinel: "no producer id" (`RecordBatch.NO_PRODUCER_ID`).
/// Returned on `EndTxn` error responses, where the identity is meaningless.
const NO_PRODUCER_ID: i64 = -1;

/// Kafka wire sentinel: "no producer epoch" (`RecordBatch.NO_PRODUCER_EPOCH`).
const NO_PRODUCER_EPOCH: i16 = -1;

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
    let authorizer = broker.config.authorizer.as_ref();
    let mut cur: &[u8] = req_bytes;
    let req = EndTxnRequest::decode(&mut cur, version)?;

    // Refresh leader-partition view from the current metadata image
    // before checking coordinator-ness.
    let image = controller.current_image();
    let txnv = crate::txn::version::resolve_txn_version(&image);
    coord.refresh_leader_partitions(&image).await;

    let tid = req.transactional_id.as_str();
    let entry_mutex = match validate_end_txn(&coord, authorizer, &image, ctx, &req).await {
        Ok(EndTxnValidation::Proceed(entry)) => entry,
        Ok(EndTxnValidation::AlreadyComplete(pid, epoch)) => {
            return encode_ok(version, pid.get(), epoch);
        }
        Err(code) => return encode_err(version, code),
    };

    // ── Phase 1: Ongoing → Prepare{Commit,Abort} ──────────────────────

    let (marker_type, prepare, complete, prepare_snap) =
        match prepare_transaction(&coord, &entry_mutex, req.committed, txnv, tid).await {
            Ok(prepared) => prepared,
            Err(code) => return encode_err(version, code),
        };

    // ── Phase 2: Fan out WriteTxnMarkers ──────────────────────────────

    if let Err(code) = dispatch_transaction_markers(broker, &prepare_snap, marker_type, tid).await {
        return encode_err(version, code);
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

    // The completion identity was selected and persisted with the Prepare
    // state. Phase 3 adopts that identity after marker fan-out; it does not
    // allocate or increment it again.
    let response_pid;
    let response_epoch;
    let (prepared_completion_pid, prepared_completion_epoch) =
        completion_producer_identity(&prepare_snap);

    let complete_snap: TxnEntry = {
        let mut entry = current_mutex.lock().await;
        // The Prepare record already contains both identities: the marker uses
        // the incremented epoch of the producer that wrote the transaction,
        // while the staged completion identity is returned to the client. This
        // revalidation prevents Phase 3 from adopting a stale staged identity.
        match decide_end_txn_completion(
            &entry,
            prepare_snap.producer_id,
            prepare_snap.producer_epoch,
            prepared_completion_pid,
            prepared_completion_epoch,
            prepare,
            complete,
        ) {
            CompletionDecision::Proceed {
                next_state,
                response_pid: new_pid,
                response_epoch: new_epoch,
            } => {
                if new_pid != ProducerId(req.producer_id) {
                    // Epoch rolled over to a new producer_id: record the prior id
                    // so the transition is traceable (KIP-890 PreviousProducerId).
                    entry.prev_producer_id = ProducerId(req.producer_id);
                }
                entry.state = next_state;
                entry.last_update_ms = now_millis();
                entry.producer_id = new_pid;
                entry.producer_epoch = new_epoch;
                entry.next_producer_id = ProducerId(-1);
                entry.next_producer_epoch = -1;
                entry.partitions.clear();
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
                return encode_ok(version, pid.get(), epoch);
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

    // Unwrap the post-completion `ProducerId` into the raw-`i64` wire response.
    encode_ok(version, response_pid.get(), response_epoch)
}

enum EndTxnValidation {
    Proceed(std::sync::Arc<tokio::sync::Mutex<TxnEntry>>),
    AlreadyComplete(ProducerId, i16),
}

async fn validate_end_txn(
    coordinator: &crate::txn::coordinator::TxnCoordinator,
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    request: &EndTxnRequest,
) -> Result<EndTxnValidation, i16> {
    let transactional_id = request.transactional_id.as_str();
    let authorization = AuthorizationRequest {
        principal: context.principal,
        host: context.peer,
        resource_type: ResourceType::TransactionalId,
        resource_name: transactional_id,
        operation: AclOperation::Write,
    };
    if authorizer.authorize(image, &authorization) == AuthorizationResult::Deny {
        return Err(codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
    }
    if !coordinator.is_coordinator_for(transactional_id).await {
        return Err(codes::NOT_COORDINATOR);
    }
    let entry = coordinator
        .get(transactional_id)
        .ok_or(codes::INVALID_PRODUCER_ID_MAPPING)?;
    {
        let state = entry.lock().await;
        if matches!(
            state.state,
            TxnState::PrepareCommit | TxnState::PrepareAbort
        ) {
            return Err(codes::CONCURRENT_TRANSACTIONS);
        }
        let request_pid = ProducerId(request.producer_id);
        let request_epoch = request.producer_epoch;
        if matches!(
            state.state,
            TxnState::CompleteCommit | TxnState::CompleteAbort
        ) {
            let same_result = matches!(state.state, TxnState::CompleteCommit) == request.committed;
            if same_result && is_completed_end_txn_retry(&state, request_pid, request_epoch) {
                return Ok(EndTxnValidation::AlreadyComplete(
                    state.producer_id,
                    state.producer_epoch,
                ));
            }
        }
        if client_producer_identity(&state) != (request_pid, request_epoch) {
            return Err(codes::INVALID_PRODUCER_EPOCH);
        }
    }
    Ok(EndTxnValidation::Proceed(entry))
}

async fn prepare_transaction(
    coordinator: &crate::txn::coordinator::TxnCoordinator,
    entry: &std::sync::Arc<tokio::sync::Mutex<TxnEntry>>,
    committed: bool,
    version: crate::txn::version::TxnVersion,
    transactional_id: &str,
) -> Result<(MarkerType, TxnState, TxnState, TxnEntry), i16> {
    let marker_type = if committed {
        MarkerType::Commit
    } else {
        MarkerType::Abort
    };
    let (prepare, complete, snapshot) = {
        let mut state = entry.lock().await;
        let (prepare, complete) = decide_phase1_transition(&mut state, committed)?;
        prepare_completion_identities(&mut state, version, &coordinator.producer_ids)
            .await
            .map_err(|error| {
                tracing::error!(
                    tid = transactional_id,
                    %error,
                    "EndTxn: failed to allocate completion producer identity"
                );
                codes::UNKNOWN_SERVER_ERROR
            })?;
        state.last_update_ms = now_millis();
        (prepare, complete, state.clone())
    };
    if let Err(error) = coordinator.put(snapshot.clone(), version).await {
        tracing::error!(
            tid = transactional_id,
            state = ?prepare,
            error = %error,
            "EndTxn: failed to persist PrepareCommit/PrepareAbort"
        );
        return Err(codes::UNKNOWN_SERVER_ERROR);
    }
    Ok((marker_type, prepare, complete, snapshot))
}

async fn dispatch_transaction_markers(
    broker: &Broker,
    snapshot: &TxnEntry,
    marker_type: MarkerType,
    transactional_id: &str,
) -> Result<(), i16> {
    broker
        .txn_coordinator
        .dispatch_transaction_markers(snapshot, marker_type)
        .await
        .map_err(|error| {
            tracing::error!(
                tid = transactional_id,
                error = %error,
                "EndTxn: WriteTxnMarkers fan-out failed; returning retriable error"
            );
            codes::UNKNOWN_SERVER_ERROR
        })
}

/// KIP-890: the `(producer_id, producer_epoch)` a producer continues with after
/// a transaction completes.
///
/// - Below `TV_2`: unchanged — the epoch only moves on `InitProducerId` reuse.
/// - `TV >= 2`, normal: same `producer_id`, `epoch + 1` — bumping on completion
///   fences a zombie holding the old epoch without a fresh `InitProducerId`.
/// - `TV >= 2`, marker-epoch boundary (`epoch >= i16::MAX - 1`): `i16::MAX` is
///   reserved for the transaction marker, so a *new* `producer_id` is allocated
///   at epoch 0 before the client can receive the reserved epoch. The caller
///   records the old id as `prev_producer_id`; `EndTxn` v5 returns the new pair.
pub(crate) async fn next_producer_identity(
    txnv: TxnVersion,
    pid: ProducerId,
    epoch: i16,
    ids: &crate::producer_id_manager::ProducerIdManager,
) -> Result<(ProducerId, i16), BrokerError> {
    let fresh = if txnv.verified() && epoch >= i16::MAX - 1 {
        Some(ids.allocate().await?.0)
    } else {
        None
    };
    Ok(next_producer_identity_with_fresh(txnv, pid, epoch, fresh)
        .expect("fresh producer ID supplied at the rotation boundary"))
}

fn next_producer_identity_with_fresh(
    txnv: TxnVersion,
    pid: ProducerId,
    epoch: i16,
    fresh: Option<ProducerId>,
) -> Option<(ProducerId, i16)> {
    if !txnv.verified() {
        Some((pid, epoch))
    } else if epoch < i16::MAX - 1 {
        Some((pid, epoch + 1))
    } else {
        fresh.map(|producer_id| (producer_id, 0))
    }
}

/// KIP-939 recovery identities have already moved past the original producer
/// identity that must retain `i16::MAX` for its transaction marker. A staged
/// recovery identity can therefore advance through `i16::MAX`; only a later
/// recovery or completion rotates it to a fresh producer ID.
pub(crate) async fn next_recovery_producer_identity(
    pid: ProducerId,
    epoch: i16,
    ids: &crate::producer_id_manager::ProducerIdManager,
) -> Result<(ProducerId, i16), BrokerError> {
    let fresh = if epoch == i16::MAX {
        Some(ids.allocate().await?.0)
    } else {
        None
    };
    Ok(
        next_recovery_producer_identity_with_fresh(pid, epoch, fresh)
            .expect("fresh producer ID supplied at the recovery rotation boundary"),
    )
}

fn next_recovery_producer_identity_with_fresh(
    pid: ProducerId,
    epoch: i16,
    fresh: Option<ProducerId>,
) -> Option<(ProducerId, i16)> {
    if epoch < i16::MAX {
        Some((pid, epoch + 1))
    } else {
        fresh.map(|producer_id| (producer_id, 0))
    }
}

pub(crate) fn client_producer_identity(entry: &TxnEntry) -> (ProducerId, i16) {
    if entry.has_staged_producer_identity() {
        (entry.next_producer_id, entry.next_producer_epoch)
    } else {
        (entry.producer_id, entry.producer_epoch)
    }
}

pub(crate) fn completion_producer_identity(entry: &TxnEntry) -> (ProducerId, i16) {
    client_producer_identity(entry)
}

pub(crate) async fn prepare_completion_identities(
    entry: &mut TxnEntry,
    txnv: TxnVersion,
    ids: &crate::producer_id_manager::ProducerIdManager,
) -> Result<(), BrokerError> {
    let had_recovery_identity = entry.has_staged_producer_identity();
    let (_, client_epoch) = client_producer_identity(entry);
    let at_rotation_boundary = if had_recovery_identity {
        client_epoch == i16::MAX
    } else {
        client_epoch >= i16::MAX - 1
    };
    let fresh = if txnv.verified() && at_rotation_boundary {
        Some(ids.allocate().await?.0)
    } else {
        None
    };
    prepare_completion_identities_with_fresh(entry, txnv, fresh)
        .expect("fresh producer ID supplied at the rotation boundary");
    Ok(())
}

pub(crate) fn prepare_completion_identities_with_fresh(
    entry: &mut TxnEntry,
    txnv: TxnVersion,
    fresh: Option<ProducerId>,
) -> Option<()> {
    if !txnv.verified() {
        return Some(());
    }

    let had_recovery_identity = entry.has_staged_producer_identity();
    let (client_pid, client_epoch) = client_producer_identity(entry);
    let (completion_pid, completion_epoch) = if had_recovery_identity {
        next_recovery_producer_identity_with_fresh(client_pid, client_epoch, fresh)?
    } else {
        next_producer_identity_with_fresh(txnv, client_pid, client_epoch, fresh)?
    };

    // The transaction marker fences the identity that wrote the transaction.
    // i16::MAX is reserved for this final marker epoch.
    entry.producer_epoch = entry.producer_epoch.saturating_add(1);

    if had_recovery_identity || completion_pid != entry.producer_id {
        entry.next_producer_id = completion_pid;
        entry.next_producer_epoch = completion_epoch;
    } else {
        entry.next_producer_id = ProducerId(-1);
        entry.next_producer_epoch = -1;
    }
    Some(())
}

fn is_completed_end_txn_retry(
    entry: &TxnEntry,
    request_pid: ProducerId,
    request_epoch: i16,
) -> bool {
    (entry.producer_id == request_pid && request_epoch.checked_add(1) == Some(entry.producer_epoch))
        || (entry.prev_producer_id == request_pid
            && request_epoch == i16::MAX - 1
            && entry.producer_epoch == 0)
}

/// Decision for the Phase-3 (Complete) re-acquire re-validation. See
/// [`validate_complete_reacquire`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReacquireDecision {
    /// State is exactly as this handler left it after Prepare. Write Complete.
    Proceed,
    /// The entry already advanced to the Complete state this handler intended,
    /// after an idempotent retry or a lost race. Report success and do not
    /// write again.
    AlreadyComplete,
    /// The entry changed in a way that means this handler must NOT write
    /// Complete. Return this Kafka error code to the producer.
    Reject(i16),
}

/// Re-validate, after re-acquiring the coordinator's *current* entry for a
/// transactional-id, that it is safe to finalise the transaction.
///
/// `expected_epoch` and `expected_pid` are the producer identity this `EndTxn`
/// handler validated and acted on. `prepare` is the state this handler wrote
/// in Phase 1. `complete` is the state it is about to write.
///
/// Returns:
/// - [`ReacquireDecision::Reject`] with `INVALID_PRODUCER_EPOCH` if the pid or
///   epoch no longer matches, which means a concurrent `InitProducerId` fenced
///   this handler. Apache Kafka maps a stale producer epoch on `EndTxn` to
///   `INVALID_PRODUCER_EPOCH`, also known as `PRODUCER_FENCED` for the newer
///   producer client.
/// - [`ReacquireDecision::AlreadyComplete`] if the entry is already in the
///   exact `complete` state this handler intended. Another caller, or an
///   `EndTxn` retry, finished the transition, so a second finalise would be a
///   redundant overwrite.
/// - [`ReacquireDecision::Reject`] with `INVALID_TXN_STATE` if the state is
///   anything other than the `prepare` this handler left in place. For
///   example, a concurrent `AddPartitionsToTxn` advanced it to `Ongoing`, or
///   it moved into the *opposite* prepare/complete kind. The marker fan-out
///   then no longer reflects the live transaction, and this handler must not
///   finalise.
/// - [`ReacquireDecision::Proceed`] only when the epoch matches and the state
///   is still exactly `prepare`.
pub(crate) fn validate_complete_reacquire(
    entry: &TxnEntry,
    expected_pid: ProducerId,
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
/// transaction. The function groups partitions by leader node:
///
/// - **local** (leader == `node_id`): directly calls
///   [`Partition::produce_batch`] on the in-memory handle.
/// - **remote**: sends a [`WriteTxnMarkersRequest`] over the shared
///   [`InterBrokerClient`], which runs TLS / SASL when the inter-broker
///   listener demands them.
///
/// Any `__consumer_offsets` partitions registered through `AddOffsetsToTxn`
/// live in `entry.partitions`, because Kafka's model has no separate group
/// list. The same loop therefore fans them out with the data partitions.
#[derive(Clone, Copy)]
pub(crate) struct MarkerDispatchContext<'a> {
    pub(crate) node_id: NodeId,
    pub(crate) coordinator_epoch: i32,
    pub(crate) image: &'a MetadataImage,
    pub(crate) inter_broker_client: &'a InterBrokerClient,
    pub(crate) inter_broker_protocol: ListenerProtocol,
    pub(crate) inter_broker_listener_name: &'a str,
    pub(crate) inter_broker_server_name: &'a str,
    pub(crate) group_coordinator: Option<&'a std::sync::Arc<crate::coordinator::GroupCoordinator>>,
}

pub(crate) async fn dispatch_markers(
    context: MarkerDispatchContext<'_>,
    partitions: &std::sync::Arc<crate::partition_registry::PartitionRegistry>,
    entry: &TxnEntry,
    marker_type: MarkerType,
) -> Result<(), BrokerError> {
    let MarkerDispatchContext {
        node_id,
        coordinator_epoch,
        image,
        ..
    } = context;
    // Group every involved (topic, partition) by its current leader.
    let mut by_leader: HashMap<NodeId, Vec<TopicPartition>> = HashMap::new();

    for tp in &entry.partitions {
        let Some(partition) = image.partition(&tp.topic, tp.partition.get()) else {
            // The partition was deleted after it joined the transaction. There
            // is no log left to mark, so it must not block transaction completion.
            continue;
        };
        by_leader
            .entry(partition.leader)
            .or_default()
            .push(tp.clone());
    }

    for (leader, tps) in by_leader {
        if leader == node_id {
            // Local path: directly append a marker batch to each partition.
            for tp in &tps {
                let part = partitions.get(&tp.topic, tp.partition).ok_or_else(|| {
                    BrokerError::Txn(format!(
                        "transaction marker target {}-{} is led locally but is not materialized",
                        tp.topic,
                        tp.partition.get()
                    ))
                })?;
                append_marker_and_materialize(
                    &part,
                    context.group_coordinator,
                    &tp.topic,
                    MarkerAppend {
                        producer_id: entry.producer_id,
                        producer_epoch: entry.producer_epoch,
                        marker_type,
                        coordinator_epoch,
                        commit_stamp: None,
                    },
                )
                .await?;
            }
        } else {
            // Remote path: send WriteTxnMarkersRequest to the leader.
            send_write_txn_markers(context, leader, entry, marker_type, &tps).await?;
        }
    }

    Ok(())
}

/// Send a `WriteTxnMarkersRequest` to a remote broker that leads one or more
/// of the transaction's partitions.
///
/// Dials through the shared [`InterBrokerClient`] so the connection
/// terminates TLS and runs the SASL client handshake whenever the
/// inter-broker listener demands them. A one-shot
/// `crabka_client_core::Client` per call would carry no TLS
/// connector and no inter-broker credentials. Marker fan-out would then
/// succeed only against a PLAINTEXT inter-broker listener, and it would
/// silently break transactions that span remote-led partitions on any
/// secured cluster.
///
/// ## Coordinator epoch
///
/// The caller resolves the current `__transaction_state` partition leader epoch
/// from the metadata image and stamps it on every marker.
#[cfg_attr(test, mutants::skip)]
async fn send_write_txn_markers(
    context: MarkerDispatchContext<'_>,
    leader_node: NodeId,
    entry: &TxnEntry,
    marker_type: MarkerType,
    tps: &[TopicPartition],
) -> Result<(), BrokerError> {
    let MarkerDispatchContext {
        node_id: my_node_id,
        coordinator_epoch,
        image,
        inter_broker_client,
        inter_broker_protocol,
        inter_broker_listener_name,
        inter_broker_server_name,
        ..
    } = context;
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

    let req = build_write_txn_markers_request(entry, marker_type, tps, coordinator_epoch);

    let opts = crabka_client_core::ConnectionOptions {
        client_id: format!("crabka-broker-txn-{my_node_id}"),
        ..crabka_client_core::ConnectionOptions::default()
    };
    let conn = inter_broker_client
        .connect_as_connection(
            &host,
            port,
            inter_broker_protocol,
            inter_broker_server_name,
            opts,
        )
        .await
        .map_err(|e| BrokerError::Txn(format!("EndTxn: connect to {host}:{port}: {e}")))?;

    // `Connection::send` negotiates the wire version from the broker-advertised
    // ApiVersions table established during connect.
    let resp = conn
        .send(req)
        .await
        .map_err(|e| BrokerError::Txn(format!("EndTxn: WriteTxnMarkers to {host}:{port}: {e}")))?;

    conn.close();
    validate_marker_response(entry, tps, &resp)
}

fn build_write_txn_markers_request(
    entry: &TxnEntry,
    marker_type: MarkerType,
    tps: &[TopicPartition],
    coordinator_epoch: i32,
) -> WriteTxnMarkersRequest {
    // Group tps by topic for the nested WritableTxnMarkerTopic structure.
    let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
    for tp in tps {
        by_topic
            .entry(tp.topic.clone())
            .or_default()
            .push(tp.partition.get());
    }

    let topics: Vec<WritableTxnMarkerTopic> = by_topic
        .into_iter()
        .map(|(name, partition_indexes)| WritableTxnMarkerTopic {
            name,
            partition_indexes,
            ..Default::default()
        })
        .collect();

    WriteTxnMarkersRequest {
        markers: vec![WritableTxnMarker {
            // Unwrap into the raw-`i64` wire field.
            producer_id: entry.producer_id.get(),
            producer_epoch: entry.producer_epoch,
            transaction_result: marker_type == MarkerType::Commit,
            topics,
            coordinator_epoch,
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn validate_marker_response(
    entry: &TxnEntry,
    tps: &[TopicPartition],
    response: &WriteTxnMarkersResponse,
) -> Result<(), BrokerError> {
    let marker = response
        .markers
        .iter()
        .find(|marker| marker.producer_id == entry.producer_id.get())
        .ok_or_else(|| {
            BrokerError::Txn(format!(
                "WriteTxnMarkers response omitted producer {}",
                entry.producer_id.get()
            ))
        })?;
    for tp in tps {
        let result = marker
            .topics
            .iter()
            .find(|topic| topic.name == tp.topic)
            .and_then(|topic| {
                topic
                    .partitions
                    .iter()
                    .find(|partition| partition.partition_index == tp.partition.get())
            })
            .ok_or_else(|| {
                BrokerError::Txn(format!(
                    "WriteTxnMarkers response omitted {}-{}",
                    tp.topic,
                    tp.partition.get()
                ))
            })?;
        if result.error_code != codes::NONE {
            return Err(BrokerError::Txn(format!(
                "WriteTxnMarkers failed for {}-{} with error code {}",
                tp.topic,
                tp.partition.get(),
                result.error_code
            )));
        }
    }
    Ok(())
}

// ── encoding helpers ──────────────────────────────────────────────────────────

fn encode_err(version: i16, error_code: i16) -> Result<Bytes, BrokerError> {
    // On the error path the producer_id/epoch fields are not meaningful;
    // leave them at the "no producer" wire sentinels.
    encode_response(version, error_code, NO_PRODUCER_ID, NO_PRODUCER_EPOCH)
}

/// Encode a successful `EndTxn` response. `producer_id` and `producer_epoch`
/// are the post-completion identity. The epoch bumps at `TV >= 2`, or rolls to a
/// new `producer_id` on epoch exhaustion; see [`next_producer_identity`]. They
/// are only on the wire at v5 (KIP-890). At lower versions the producer never
/// observes them, and the persisted bump instead fences a stale-epoch producer
/// on its next coordinator call.
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
    use assert2::assert;
    use crabka_ids::PartitionIndex;
    use crabka_metadata::{BrokerEndpoint, BrokerRegistrationRecord, MetadataRecord};
    use crabka_protocol::{
        UnknownTaggedFields,
        owned::{
            end_txn_response::EndTxnResponse,
            write_txn_markers_response::{
                WritableTxnMarkerPartitionResult, WritableTxnMarkerResult,
                WritableTxnMarkerTopicResult,
            },
        },
    };

    fn decode_response(bytes: &Bytes, version: i16) -> EndTxnResponse {
        crate::test_support::decode_response(bytes, version)
    }

    use super::*;

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

    #[tokio::test]
    async fn epoch_bumps_only_at_tv2() {
        use crate::txn::version::TxnVersion;
        let ids = crate::producer_id_manager::ProducerIdManager::new();
        let cases = [
            // Below TV_2 (Classic, Flexible): pid + epoch unchanged.
            (TxnVersion::Classic, (ProducerId(7), 3)),
            (TxnVersion::Flexible, (ProducerId(7), 3)),
            // TV_2+ non-overflow: same pid, epoch + 1.
            (TxnVersion::Verified, (ProducerId(7), 4)),
            (TxnVersion::TwoPhase, (ProducerId(7), 4)),
        ];
        for (version, want) in cases {
            assert!(
                next_producer_identity(version, ProducerId(7), 3, &ids)
                    .await
                    .unwrap()
                    == want,
                "txn version {version:?}"
            );
        }
    }

    #[tokio::test]
    async fn epoch_overflow_at_tv2_allocates_new_pid_at_epoch_zero() {
        use crate::txn::version::TxnVersion;
        let ids = crate::producer_id_manager::ProducerIdManager::new();
        // MAX is reserved for the marker epoch, so the client rotates at
        // MAX-1 and receives a fresh producer_id at epoch 0.
        let (new_pid, new_epoch) =
            next_producer_identity(TxnVersion::Verified, ProducerId(7), i16::MAX - 1, &ids)
                .await
                .unwrap();
        assert!(new_pid != 7);
        assert!(new_epoch == 0);
        // The allocator hands out a distinct pid on the next overflow too.
        let (next_pid, _) =
            next_producer_identity(TxnVersion::Verified, ProducerId(7), i16::MAX, &ids)
                .await
                .unwrap();
        assert!(next_pid != new_pid);
        // Below TV_2 at i16::MAX: no roll, epoch stays (no bump path taken).
        assert!(
            next_producer_identity(TxnVersion::Classic, ProducerId(7), i16::MAX, &ids)
                .await
                .unwrap()
                == (ProducerId(7), i16::MAX)
        );
    }

    #[tokio::test]
    async fn normal_completion_rotates_before_the_reserved_marker_epoch() {
        let ids = crate::producer_id_manager::ProducerIdManager::new();
        let mut entry = entry(7, i16::MAX - 1, TxnState::PrepareCommit);

        prepare_completion_identities(&mut entry, TxnVersion::Verified, &ids)
            .await
            .unwrap();

        assert!(entry.producer_epoch == i16::MAX);
        let (completion_pid, completion_epoch) = completion_producer_identity(&entry);
        assert!(completion_pid != 7);
        assert!(completion_epoch == 0);
    }

    #[tokio::test]
    async fn legacy_completion_does_not_allocate_at_the_tv2_boundary() {
        let ids = crate::producer_id_manager::ProducerIdManager::new();
        let mut entry = entry(7, i16::MAX - 1, TxnState::PrepareCommit);

        prepare_completion_identities(&mut entry, TxnVersion::Classic, &ids)
            .await
            .unwrap();

        assert!(completion_producer_identity(&entry) == (ProducerId(7), i16::MAX - 1));
        assert!(
            ids.allocate().await.unwrap() == (ProducerId(0), 0),
            "legacy completion must not consume a fresh producer ID"
        );
    }

    #[tokio::test]
    async fn prepared_recovery_uses_marker_identity_and_fences_the_recovery_client() {
        let ids = crate::producer_id_manager::ProducerIdManager::new();
        let mut entry = entry(7, 3, TxnState::PrepareCommit);
        entry.next_producer_id = ProducerId(7);
        entry.next_producer_epoch = 4;

        prepare_completion_identities(&mut entry, TxnVersion::TwoPhase, &ids)
            .await
            .unwrap();

        assert!(entry.producer_id == 7);
        assert!(entry.producer_epoch == 4, "marker identity must advance");
        assert!(completion_producer_identity(&entry) == (ProducerId(7), 5));
    }

    #[tokio::test]
    async fn prepared_recovery_can_use_max_epoch_before_rotating() {
        let ids = crate::producer_id_manager::ProducerIdManager::new();
        let mut entry = entry(7, i16::MAX - 1, TxnState::PrepareCommit);
        entry.next_producer_id = ProducerId(11);
        entry.next_producer_epoch = i16::MAX - 1;

        prepare_completion_identities(&mut entry, TxnVersion::TwoPhase, &ids)
            .await
            .unwrap();

        assert!(entry.producer_epoch == i16::MAX);
        assert!(completion_producer_identity(&entry) == (ProducerId(11), i16::MAX));

        entry.next_producer_epoch = i16::MAX;
        prepare_completion_identities(&mut entry, TxnVersion::TwoPhase, &ids)
            .await
            .unwrap();
        let (rotated_pid, rotated_epoch) = completion_producer_identity(&entry);
        assert!(rotated_pid != 11);
        assert!(rotated_epoch == 0);
    }

    // ── Phase-3 re-validation: validate_complete_reacquire ──────────────────

    /// Build a `TxnEntry` in a given (pid, epoch, state) for the re-validation
    /// tests. Partition sets do not change the decision, so leave them empty.
    fn entry(pid: i64, epoch: i16, state: TxnState) -> TxnEntry {
        let mut e = TxnEntry::new_empty("tid-x".into(), ProducerId(pid), epoch, 60_000, 1);
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
                ProducerId(7),
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
                ProducerId(7),
                3,
                TxnState::PrepareAbort,
                TxnState::CompleteAbort
            ) == ReacquireDecision::Proceed
        );
        let done = entry(7, 3, TxnState::CompleteAbort);
        assert!(
            validate_complete_reacquire(
                &done,
                ProducerId(7),
                3,
                TxnState::PrepareAbort,
                TxnState::CompleteAbort
            ) == ReacquireDecision::AlreadyComplete
        );
    }

    // ── Remote marker fan-out: send_write_txn_markers error / fallback
    //    branches the happy-path integration test does not reach. ───────────

    fn marker_entry() -> TxnEntry {
        TxnEntry::new_empty("tid".to_string(), ProducerId(7), 0, 60_000, 0)
    }

    fn tps() -> Vec<TopicPartition> {
        vec![TopicPartition {
            topic: "t".to_string(),
            partition: PartitionIndex(0),
        }]
    }

    fn marker_response(error_code: i16) -> WriteTxnMarkersResponse {
        WriteTxnMarkersResponse {
            markers: vec![WritableTxnMarkerResult {
                producer_id: 7,
                topics: vec![WritableTxnMarkerTopicResult {
                    name: "t".to_string(),
                    partitions: vec![WritableTxnMarkerPartitionResult {
                        partition_index: 0,
                        error_code,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn marker_response_requires_every_partition_to_succeed() {
        let entry = marker_entry();
        let partitions = tps();
        assert!(
            validate_marker_response(&entry, &partitions, &marker_response(codes::NONE)).is_ok()
        );
        assert!(
            validate_marker_response(
                &entry,
                &partitions,
                &marker_response(codes::NOT_LEADER_OR_FOLLOWER)
            )
            .is_err()
        );
        assert!(
            validate_marker_response(&entry, &partitions, &WriteTxnMarkersResponse::default())
                .is_err()
        );
    }

    #[test]
    fn marker_request_uses_current_coordinator_epoch() {
        let request =
            build_write_txn_markers_request(&marker_entry(), MarkerType::Abort, &tps(), 42);

        assert!(request.markers.len() == 1);
        assert!(request.markers[0].coordinator_epoch == 42);
    }

    #[tokio::test]
    async fn marker_dispatch_skips_deleted_partition() {
        let image = MetadataImage::default();
        let client = plaintext_client();
        let partitions = std::sync::Arc::new(crate::partition_registry::PartitionRegistry::new());
        let mut entry = marker_entry();
        entry.partitions.insert(tps().remove(0));

        let result = dispatch_markers(
            MarkerDispatchContext {
                node_id: NodeId(1),
                coordinator_epoch: 0,
                image: &image,
                inter_broker_client: &client,
                inter_broker_protocol: ListenerProtocol::Plaintext,
                inter_broker_listener_name: "PLAINTEXT",
                inter_broker_server_name: "localhost",
                group_coordinator: None,
            },
            &partitions,
            &entry,
            MarkerType::Commit,
        )
        .await;

        assert!(result.is_ok());
    }

    /// A client with no TLS connector and no SASL creds — fine here, every
    /// case fails at the TCP connect (unreachable address) before any
    /// handshake would run.
    fn plaintext_client() -> InterBrokerClient {
        InterBrokerClient::new(None, None)
    }

    async fn send_test_markers(
        image: &MetadataImage,
        leader: NodeId,
        listener_name: &str,
    ) -> Result<(), BrokerError> {
        let client = plaintext_client();
        let entry = marker_entry();
        let partitions = tps();
        send_write_txn_markers(
            MarkerDispatchContext {
                node_id: NodeId(1),
                coordinator_epoch: 0,
                image,
                inter_broker_client: &client,
                inter_broker_protocol: ListenerProtocol::Plaintext,
                inter_broker_listener_name: listener_name,
                inter_broker_server_name: "localhost",
                group_coordinator: None,
            },
            leader,
            &entry,
            MarkerType::Commit,
            &partitions,
        )
        .await
    }

    /// Leader node absent from the metadata image → descriptive `Txn` error,
    /// and no dial.
    #[tokio::test]
    async fn errors_when_leader_node_missing_from_image() {
        let image = MetadataImage::default();
        let err = send_test_markers(&image, NodeId(99), "PLAINTEXT")
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
                node_id: NodeId(2),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::nil(),
                host: "127.0.0.1".to_string(),
                port: 9,
                rack: None,
                log_dirs: vec![],
                endpoints: vec![BrokerEndpoint {
                    name: "INTERNAL".to_string(),
                    host: "127.0.0.1".to_string(),
                    // Discard port: refuses connections immediately.
                    port: 9,
                    protocol: ListenerProtocol::Plaintext,
                }],
                features: std::collections::BTreeMap::new(),
            },
        ));
        let err = send_test_markers(&image, NodeId(2), "INTERNAL")
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
                node_id: NodeId(2),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::nil(),
                host: "127.0.0.1".to_string(),
                port: 9,
                rack: None,
                log_dirs: vec![],
                // Endpoint exists but under a different listener name, so the
                // `find(name == inter_broker_listener_name)` misses.
                endpoints: vec![BrokerEndpoint {
                    name: "SOMETHING_ELSE".to_string(),
                    host: "127.0.0.1".to_string(),
                    port: 65000,
                    protocol: ListenerProtocol::Plaintext,
                }],
                features: std::collections::BTreeMap::new(),
            },
        ));
        let err = send_test_markers(&image, NodeId(2), "INTERNAL")
            .await
            .expect_err("unreachable fallback must error");
        assert!(
            matches!(&err, BrokerError::Txn(m) if m.contains("connect to 127.0.0.1:9")),
            "expected fallback to top-level 127.0.0.1:9, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn remote_marker_dispatch_dials_with_configured_server_name() {
        use std::sync::Arc;

        use tokio::net::TcpListener;
        use tokio_rustls::{
            LazyConfigAcceptor,
            rustls::{ClientConfig, RootCertStore, server::Acceptor},
        };

        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();

        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind TLS ClientHello capture listener");
        let port = listener
            .local_addr()
            .expect("capture listener address")
            .port();
        let capture = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept marker dial");
            let handshake = LazyConfigAcceptor::new(Acceptor::default(), stream)
                .await
                .expect("parse marker dial ClientHello");
            handshake.client_hello().server_name().map(str::to_owned)
        });

        let mut image = MetadataImage::default();
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: NodeId(2),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::nil(),
                host: "127.0.0.1".to_string(),
                port,
                rack: None,
                log_dirs: vec![],
                endpoints: vec![BrokerEndpoint {
                    name: "INTERNAL".to_string(),
                    host: "127.0.0.1".to_string(),
                    port,
                    protocol: ListenerProtocol::Ssl,
                }],
                features: std::collections::BTreeMap::new(),
            },
        ));
        let tls = ClientConfig::builder()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth();
        let client =
            InterBrokerClient::new(Some(tokio_rustls::TlsConnector::from(Arc::new(tls))), None);
        let entry = marker_entry();
        let partitions = tps();
        let result = send_write_txn_markers(
            MarkerDispatchContext {
                node_id: NodeId(1),
                coordinator_epoch: 0,
                image: &image,
                inter_broker_client: &client,
                inter_broker_protocol: ListenerProtocol::Ssl,
                inter_broker_listener_name: "INTERNAL",
                inter_broker_server_name: "broker.internal",
                group_coordinator: None,
            },
            NodeId(2),
            &entry,
            MarkerType::Commit,
            &partitions,
        )
        .await;

        assert!(
            result.is_err(),
            "capture server intentionally stops after ClientHello"
        );
        assert!(
            capture.await.expect("join ClientHello capture").as_deref() == Some("broker.internal")
        );
    }
}
