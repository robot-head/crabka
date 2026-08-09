//! `InitProducerId` (`api_key=22`). This handler hands out
//! `(producer_id, producer_epoch)` to a producer, or it initialises /
//! re-initialises a transactional producer.
//!
//! Non-transactional path: idempotent-producer support.
//! Transactional path:     coordinator routing.
//!
//! ## ACL preamble
//!
//! Two distinct authorize gates branch off `req.transactional_id`:
//!
//! * `Some(non-empty)` → `Write` on
//!   `TransactionalId(transactional_id)`. Deny →
//!   `error_code = TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)`.
//! * `None | Some("")` (idempotent-only producer) →
//!   `IdempotentWrite` on `Cluster("kafka-cluster")`. Deny →
//!   `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.

use std::sync::Arc;

use bytes::Bytes;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode,
    owned::{
        init_producer_id_request::InitProducerIdRequest,
        init_producer_id_response::InitProducerIdResponse,
    },
};
use crabka_units::convert::TimeExt as _;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
    replicator_supervisor::materialize_partition,
    txn::{
        coordinator::TxnCoordinator,
        state::{TxnEntry, TxnState},
        util::now_millis,
    },
};

// cargo-mutants: the surviving mutant here deletes `error_code: codes::NONE`
// from the non-transactional `InitProducerIdResponse`; `codes::NONE == 0`, so
// the field's Default is identical and the mutation is a true equivalent. The
// rest of `handle` (ACL branches, 2PC gates, coordinator routing) is covered
// by the live-broker integration suite, not this in-file module.
#[cfg_attr(test, mutants::skip)]
#[tracing::instrument(
    name = "handle_init_producer_id",
    level = "info",
    skip_all,
    fields(api = "InitProducerId", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let producer_ids = broker.producer_ids.clone();
    let coord = broker.txn_coordinator.clone();
    let controller = broker.controller.clone();
    let log_dirs = broker.config.all_log_dirs();
    let log_config = broker.config.log_config.clone();
    let log_dir_status = broker.log_dir_status.clone();

    let mut cur: &[u8] = req_bytes;
    let req = InitProducerIdRequest::decode(&mut cur, version)?;

    // ── ACL preamble ────────────────────────────────────────
    // Branch on whether this is an idempotent-only or transactional
    // request and gate on the appropriate resource/operation.
    {
        let image = controller.current_image();
        let authorizer = broker.config.authorizer.as_ref();
        match req.transactional_id.as_deref() {
            Some(tid) if !tid.is_empty() => {
                let acl_req = AuthorizationRequest {
                    principal: ctx.principal,
                    host: ctx.peer,
                    resource_type: ResourceType::TransactionalId,
                    resource_name: tid,
                    operation: AclOperation::Write,
                };
                if authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
                    return encode_err(version, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
                }
            }
            _ => {
                let acl_req = AuthorizationRequest {
                    principal: ctx.principal,
                    host: ctx.peer,
                    resource_type: ResourceType::Cluster,
                    resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
                    operation: AclOperation::IdempotentWrite,
                };
                if authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
                    return encode_err(version, codes::CLUSTER_AUTHORIZATION_FAILED);
                }
            }
        }
    }

    let resp = match req.transactional_id.as_deref() {
        None | Some("") => {
            // Non-transactional path (idempotence).
            let (pid, epoch) = producer_ids.allocate().await?;
            InitProducerIdResponse {
                throttle_time_ms: 0,
                error_code: codes::NONE,
                // Unwrap the allocated `ProducerId` into the raw-`i64` wire field.
                producer_id: pid.get(),
                producer_epoch: epoch,
                ..Default::default()
            }
        }
        Some(tid) => {
            // Refresh the coordinator's leader-partition view from the
            // current metadata image. This is a cheap idempotent read,
            // and it ensures we don't race with the replicator-supervisor
            // loop when a `FindCoordinator(TRANSACTION)` call that
            // triggered `__transaction_state` bootstrap just happened.
            let image = controller.current_image();
            let txnv = crate::txn::version::resolve_txn_version(&image);

            // ── KIP-939 two-phase-commit gates ───────────────────────────
            // Validated up-front (like Kafka's `handleInitProducerId`), before
            // the coordinator-ness check, so a client learns its request is
            // unauthorized / unsupported regardless of which broker it hit.
            if req.enable2_pc || req.keep_prepared_txn {
                // (1) Cluster must have 2PC enabled. Kafka maps a disabled
                //     cluster to TRANSACTIONAL_ID_AUTHORIZATION_FAILED (not an
                //     UNSUPPORTED_*), so a client can't probe the feature flag.
                if !broker.config.features.transaction_two_phase_commit_enable {
                    return encode_err(version, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
                }
                // (2) Principal must hold the TWO_PHASE_COMMIT ACL on the tid,
                //     in addition to the Write checked in the preamble.
                let two_pc_req = AuthorizationRequest {
                    principal: ctx.principal,
                    host: ctx.peer,
                    resource_type: ResourceType::TransactionalId,
                    resource_name: tid,
                    operation: AclOperation::TwoPhaseCommit,
                };
                if broker.config.authorizer.authorize(&*image, &two_pc_req)
                    == AuthorizationResult::Deny
                {
                    return encode_err(version, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
                }
            }
            if req.keep_prepared_txn && (req.producer_id != -1 || req.producer_epoch != -1) {
                return encode_err(version, codes::INVALID_REQUEST);
            }
            if req.keep_prepared_txn && !txnv.verified() {
                return encode_err(version, codes::UNSUPPORTED_VERSION);
            }
            coord.refresh_leader_partitions(&image).await;

            // Verify we're the coordinator for this tid.
            if coord.is_coordinator_for(tid).await {
                // Ensure the __transaction_state partition for this tid
                // is materialized on disk. The replicator-supervisor
                // handles this asynchronously, but we may race with it
                // when FindCoordinator just bootstrapped the topic in
                // the same request round-trip. `materialize_partition`
                // uses `DashMap::entry()` to atomically check-and-insert,
                // so two concurrent InitProducerId calls for the same
                // partition cannot both spawn independent writer tasks.
                let txn_partition = coord.partition_for(tid);
                materialize_partition(crate::replicator_supervisor::MaterializePartitionConfig {
                    partitions: &coord.partitions,
                    topic: crate::txn::bootstrap::TOPIC,
                    topic_id: None,
                    partition: txn_partition.get(),
                    log_dirs: &log_dirs,
                    log_config: &log_config,
                    log_dir_status: &log_dir_status,
                    producer_state: &broker.producer_state,
                    producer_id_expiration: broker.config.producer_id_expiration,
                    max_produce_group: broker.config.max_produce_group,
                    partition_writer_queue_depth: broker.config.partition_writer_queue_depth,
                    diskless_wal_local_replica_count: broker
                        .config
                        .diskless_wal_local_replica_count,
                    diskless: false,
                    hot_tail: None,
                    wal_shards: None,
                    sequencer: None,
                })
                .map_err(BrokerError::Txn)?;
                let txn_timeout = crate::txn::two_pc::resolve_txn_timeout(
                    req.enable2_pc,
                    req.transaction_timeout_ms,
                    broker.config.transaction_min_timeout.millis_i32(),
                    broker.config.transaction_max_timeout.millis_i32(),
                );
                handle_transactional(
                    &coord,
                    tid,
                    txnv,
                    txn_timeout,
                    req.enable2_pc,
                    req.keep_prepared_txn,
                )
                .await?
            } else {
                InitProducerIdResponse {
                    error_code: codes::NOT_COORDINATOR,
                    producer_id: -1,
                    producer_epoch: -1,
                    ..Default::default()
                }
            }
        }
    };

    crate::handlers::encode_response(&resp, version)
}

fn encode_err(version: i16, error_code: i16) -> Result<Bytes, BrokerError> {
    let resp = InitProducerIdResponse {
        throttle_time_ms: 0,
        error_code,
        producer_id: -1,
        producer_epoch: -1,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

/// Transactional sub-path: allocate or bump-epoch for `tid`.
async fn handle_transactional(
    coord: &Arc<TxnCoordinator>,
    tid: &str,
    txnv: crate::txn::version::TxnVersion,
    txn_timeout: i32,
    enable_2pc: bool,
    keep_prepared_txn: bool,
) -> Result<InitProducerIdResponse, BrokerError> {
    let now_ms = now_millis();

    match coord.get(tid) {
        None => {
            // Fresh tid — allocate a new producer id.
            let (pid, epoch) = coord.producer_ids.allocate().await?;
            let entry = TxnEntry::new_empty(tid.to_string(), pid, epoch, txn_timeout, now_ms);
            coord.put(entry, txnv).await?;
            Ok(InitProducerIdResponse {
                error_code: codes::NONE,
                // Unwrap the allocated `ProducerId` into the raw-`i64` wire field.
                producer_id: pid.get(),
                producer_epoch: epoch,
                ..Default::default()
            })
        }
        Some(existing) => {
            if keep_prepared_txn {
                let recovery = {
                    let mut entry = existing.lock().await;
                    if entry.state == TxnState::Ongoing {
                        if entry.txn_timeout_ms != i32::MAX && !enable_2pc {
                            return Ok(InitProducerIdResponse {
                                error_code: codes::INVALID_TXN_STATE,
                                producer_id: -1,
                                producer_epoch: -1,
                                ..Default::default()
                            });
                        }
                        let ongoing_pid = entry.producer_id;
                        let ongoing_epoch = entry.producer_epoch;
                        if enable_2pc {
                            entry.txn_timeout_ms = i32::MAX;
                        }
                        let (next_pid, next_epoch) =
                            stage_recovery_identity(&mut entry, &coord.producer_ids).await?;
                        entry.last_update_ms = now_ms;
                        Some((
                            entry.clone(),
                            next_pid,
                            next_epoch,
                            ongoing_pid,
                            ongoing_epoch,
                        ))
                    } else {
                        None
                    }
                };
                if let Some((snapshot, next_pid, next_epoch, ongoing_pid, ongoing_epoch)) = recovery
                {
                    coord.put(snapshot, txnv).await?;
                    return Ok(InitProducerIdResponse {
                        error_code: codes::NONE,
                        producer_id: next_pid.get(),
                        producer_epoch: next_epoch,
                        ongoing_txn_producer_id: ongoing_pid.get(),
                        ongoing_txn_producer_epoch: ongoing_epoch,
                        ..Default::default()
                    });
                }
                let state = existing.lock().await.state;
                if matches!(state, TxnState::PrepareCommit | TxnState::PrepareAbort) {
                    return Ok(InitProducerIdResponse {
                        error_code: codes::CONCURRENT_TRANSACTIONS,
                        producer_id: -1,
                        producer_epoch: -1,
                        ..Default::default()
                    });
                }
            }

            // Reusing tid — bump epoch (KIP-1319 v2). If prior state was
            // Ongoing, write PrepareAbort + dispatch abort markers before
            // responding.
            let aborted_ongoing = {
                let mut e = existing.lock().await;
                if matches!(e.state, TxnState::Ongoing) {
                    // Transition to PrepareAbort; persist; dispatch markers.
                    let request_pid = crate::txn::handlers::end_txn::client_producer_identity(&e).0;
                    e.state = TxnState::PrepareAbort;
                    crate::txn::handlers::end_txn::prepare_completion_identities(
                        &mut e,
                        txnv,
                        &coord.producer_ids,
                    )
                    .await?;
                    e.last_update_ms = now_ms;
                    let entry_clone = e.clone();
                    drop(e); // release lock while we fan out markers
                    coord.put(entry_clone.clone(), txnv).await?;
                    dispatch_abort_markers(coord, &entry_clone).await?;
                    // Re-acquire + transition to CompleteAbort.
                    let mut e2 = existing.lock().await;
                    e2.state = TxnState::CompleteAbort;
                    e2.last_update_ms = now_millis();
                    let (completed_pid, completed_epoch) =
                        crate::txn::handlers::end_txn::completion_producer_identity(&e2);
                    if completed_pid != request_pid {
                        e2.prev_producer_id = request_pid;
                    }
                    e2.producer_id = completed_pid;
                    e2.producer_epoch = completed_epoch;
                    e2.next_producer_id = crabka_log::ProducerId(-1);
                    e2.next_producer_epoch = -1;
                    e2.partitions.clear();
                    let snap = e2.clone();
                    drop(e2);
                    coord.put(snap, txnv).await?;
                    true
                } else {
                    false
                }
            };

            // Bump epoch on the existing entry. Persist a new TxnEntry with
            // new epoch, Empty state, cleared partitions.
            let current = coord.get(tid).unwrap_or(existing);
            let mut e3 = current.lock().await;
            let (new_pid, new_epoch) = if aborted_ongoing && txnv.verified() {
                (e3.producer_id, e3.producer_epoch)
            } else {
                next_init_producer_identity(&e3, txnv, &coord.producer_ids).await?
            };
            *e3 = TxnEntry::new_empty(tid.to_string(), new_pid, new_epoch, txn_timeout, now_ms);
            let snap = e3.clone();
            drop(e3);
            coord.put(snap.clone(), txnv).await?;
            Ok(InitProducerIdResponse {
                error_code: codes::NONE,
                // Unwrap the entry's `ProducerId` into the raw-`i64` wire field.
                producer_id: snap.producer_id.get(),
                producer_epoch: snap.producer_epoch,
                ..Default::default()
            })
        }
    }
}

async fn next_init_producer_identity(
    entry: &TxnEntry,
    txnv: crate::txn::version::TxnVersion,
    producer_ids: &crate::producer_id_manager::ProducerIdManager,
) -> Result<(crabka_log::ProducerId, i16), BrokerError> {
    let (pid, epoch) = crate::txn::handlers::end_txn::client_producer_identity(entry);
    if txnv.verified() {
        crate::txn::handlers::end_txn::next_producer_identity(txnv, pid, epoch, producer_ids).await
    } else {
        match epoch.checked_add(1) {
            Some(next_epoch) => Ok((pid, next_epoch)),
            None => Ok(producer_ids.allocate().await?),
        }
    }
}

async fn stage_recovery_identity(
    entry: &mut TxnEntry,
    producer_ids: &crate::producer_id_manager::ProducerIdManager,
) -> Result<(crabka_log::ProducerId, i16), BrokerError> {
    let (client_pid, client_epoch) = crate::txn::handlers::end_txn::client_producer_identity(entry);
    let (next_pid, next_epoch) = crate::txn::handlers::end_txn::next_producer_identity(
        crate::txn::version::TxnVersion::Verified,
        client_pid,
        client_epoch,
        producer_ids,
    )
    .await?;
    entry.next_producer_id = next_pid;
    entry.next_producer_epoch = next_epoch;
    Ok((next_pid, next_epoch))
}

async fn dispatch_abort_markers(
    coord: &TxnCoordinator,
    entry: &TxnEntry,
) -> Result<(), BrokerError> {
    coord
        .dispatch_transaction_markers(entry, crate::txn::marker::MarkerType::Abort)
        .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_ids::PartitionIndex;
    use crabka_log::{Log, LogConfig, ProducerId};
    use crabka_units::secs;

    use super::*;
    use crate::{
        test_support::{peer, principal, start_broker_with},
        txn::state::{TopicPartition, TxnEntry},
    };

    /// `dispatch_abort_markers` appends an abort control-marker batch to each
    /// locally-led partition in the entry's partition set. Each append advances
    /// that partition's LEO by one. A whole-function `Ok(())` replacement would
    /// skip the dispatch entirely and leave the LEO at 0.
    #[tokio::test]
    async fn dispatch_abort_markers_appends_marker_to_local_partition() {
        let dir = tempfile::tempdir().unwrap();
        let partitions = Arc::new(crate::partition_registry::PartitionRegistry::new());
        let coord = TxnCoordinator::new(
            crabka_audit::NodeId(1),
            Arc::clone(&partitions),
            Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
            50,
            crabka_units::mebibytes(1),
        );

        // Materialize a local partition for `__transaction_state`-style data.
        let part_dir = crate::log_dir::partition_dir(dir.path(), "orders", 0);
        std::fs::create_dir_all(&part_dir).unwrap();
        let log = Log::open(&part_dir, LogConfig::default()).unwrap();
        let part = crate::broker::spawn_partition(
            "orders".to_string(),
            PartitionIndex(0),
            dir.path().to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
            false,
        );
        assert!(part.log_end_offset() == 0);
        partitions.insert("orders".to_string(), PartitionIndex(0), Arc::clone(&part));

        // Build a txn entry that names this partition.
        let mut entry = TxnEntry::new_empty("tx-1".to_string(), ProducerId(1000), 3, 60_000, 0);
        entry.partitions.insert(TopicPartition {
            topic: "orders".to_string(),
            partition: PartitionIndex(0),
        });

        dispatch_abort_markers(&coord, &entry)
            .await
            .expect("dispatch markers");

        // The abort marker is a single control record → LEO advances to 1.
        assert!(
            part.log_end_offset() == 1,
            "abort marker must be appended (LEO 1), got {:?}",
            part.log_end_offset()
        );
    }

    /// Without remote transport, a partition that is not hosted locally must
    /// fail the abort. Advancing the transaction without its marker would leave
    /// an open transaction in the data partition.
    #[tokio::test]
    async fn dispatch_abort_markers_rejects_missing_remote_transport() {
        let partitions = Arc::new(crate::partition_registry::PartitionRegistry::new());
        let coord = TxnCoordinator::new(
            crabka_audit::NodeId(1),
            partitions,
            Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
            50,
            crabka_units::mebibytes(1),
        );
        let mut entry = TxnEntry::new_empty("tx-2".to_string(), ProducerId(2000), 0, 60_000, 0);
        entry.partitions.insert(TopicPartition {
            topic: "ghost".to_string(),
            partition: PartitionIndex(0),
        });
        assert!(dispatch_abort_markers(&coord, &entry).await.is_err());
    }

    #[tokio::test]
    async fn recovery_identity_advances_on_every_call_and_rotates_before_marker_epoch() {
        let ids = crate::producer_id_manager::ProducerIdManager::new();
        let mut entry = TxnEntry::new_empty("tid-recover".into(), ProducerId(7), 3, i32::MAX, 0);
        entry.state = TxnState::Ongoing;

        assert!(stage_recovery_identity(&mut entry, &ids).await.unwrap() == (ProducerId(7), 4));
        assert!(stage_recovery_identity(&mut entry, &ids).await.unwrap() == (ProducerId(7), 5));
        assert!(entry.producer_id == 7);
        assert!(entry.producer_epoch == 3);

        entry.next_producer_id = ProducerId(7);
        entry.next_producer_epoch = i16::MAX - 1;
        let (rotated_pid, rotated_epoch) = stage_recovery_identity(&mut entry, &ids).await.unwrap();
        assert!(rotated_pid != 7);
        assert!(rotated_epoch == 0);
        assert!(entry.producer_id == 7);
        assert!(entry.producer_epoch == 3);
    }

    #[tokio::test]
    async fn handler_persists_configured_timeout_bounds_and_2pc_sentinel() {
        let (broker_handle, _dir) = start_broker_with(|config| {
            config.audit_enabled = false;
            config.transaction_state_num_partitions = 7;
            config.transaction_min_timeout = secs(2);
            config.transaction_max_timeout = secs(8);
            config.features.transaction_two_phase_commit_enable = true;
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal("admin");
        let peer = peer();
        let context = crate::test_support::request_context(&principal, &peer, "txn-client");
        let tids = ["txn-below-min", "txn-above-max", "txn-2pc"];

        let find_version = crabka_protocol::owned::find_coordinator_response::MAX_VERSION;
        let find_request =
            crabka_protocol::owned::find_coordinator_request::FindCoordinatorRequest {
                key_type: 1,
                coordinator_keys: tids.iter().map(ToString::to_string).collect(),
                ..Default::default()
            };
        let find_response = crate::handlers::find_coordinator::handle(
            &broker,
            find_version,
            1,
            &crate::test_support::encode_request(&find_request, find_version),
            &context,
        )
        .await
        .expect("find transaction coordinators");
        let find_response: crabka_protocol::owned::find_coordinator_response::FindCoordinatorResponse =
            crate::test_support::decode_response(&find_response, find_version);
        assert!(
            find_response
                .coordinators
                .iter()
                .all(|coordinator| coordinator.error_code == codes::NONE)
        );

        let version = crabka_protocol::owned::init_producer_id_response::MAX_VERSION;
        for (tid, requested_ms, enable_2pc, expected_ms) in [
            (tids[0], 500, false, 2_000),
            (tids[1], 10_000, false, 8_000),
            (tids[2], 500, true, i32::MAX),
        ] {
            let request = InitProducerIdRequest {
                transactional_id: Some(tid.to_string()),
                transaction_timeout_ms: requested_ms,
                enable2_pc: enable_2pc,
                ..Default::default()
            };
            let response = handle(
                &broker,
                version,
                2,
                &crate::test_support::encode_request(&request, version),
                &context,
            )
            .await
            .expect("initialize transactional producer");
            let response: InitProducerIdResponse =
                crate::test_support::decode_response(&response, version);
            assert!(response.error_code == codes::NONE, "{tid}: {response:?}");

            let entry = broker
                .txn_coordinator
                .get(tid)
                .expect("persisted transaction entry");
            assert!(entry.lock().await.txn_timeout_ms == expected_ms, "{tid}");
        }

        let ongoing = broker
            .txn_coordinator
            .get(tids[2])
            .expect("2PC transaction entry");
        let (ongoing_pid, ongoing_epoch, snapshot) = {
            let mut entry = ongoing.lock().await;
            entry.state = TxnState::Ongoing;
            (entry.producer_id, entry.producer_epoch, entry.clone())
        };
        broker
            .txn_coordinator
            .put(snapshot, crate::txn::version::TxnVersion::Verified)
            .await
            .expect("persist ongoing 2PC transaction");

        let recovery_request = InitProducerIdRequest {
            transactional_id: Some(tids[2].to_string()),
            transaction_timeout_ms: 500,
            enable2_pc: true,
            keep_prepared_txn: true,
            ..Default::default()
        };
        let recovery_response = handle(
            &broker,
            version,
            3,
            &crate::test_support::encode_request(&recovery_request, version),
            &context,
        )
        .await
        .expect("recover prepared transaction");
        let recovery_response: InitProducerIdResponse =
            crate::test_support::decode_response(&recovery_response, version);
        assert!(recovery_response.error_code == codes::NONE);
        assert!(recovery_response.ongoing_txn_producer_id == ongoing_pid.get());
        assert!(recovery_response.ongoing_txn_producer_epoch == ongoing_epoch);

        let second_recovery_response = handle(
            &broker,
            version,
            4,
            &crate::test_support::encode_request(&recovery_request, version),
            &context,
        )
        .await
        .expect("recover prepared transaction again");
        let second_recovery_response: InitProducerIdResponse =
            crate::test_support::decode_response(&second_recovery_response, version);
        assert!(second_recovery_response.error_code == codes::NONE);
        assert!(second_recovery_response.producer_id == recovery_response.producer_id);
        assert!(second_recovery_response.producer_epoch == recovery_response.producer_epoch + 1);
        assert!(second_recovery_response.ongoing_txn_producer_id == ongoing_pid.get());
        assert!(second_recovery_response.ongoing_txn_producer_epoch == ongoing_epoch);

        let end_version = crabka_protocol::owned::end_txn_response::MAX_VERSION;
        let fenced_end_request = crabka_protocol::owned::end_txn_request::EndTxnRequest {
            transactional_id: tids[2].to_string(),
            producer_id: recovery_response.producer_id,
            producer_epoch: recovery_response.producer_epoch,
            committed: true,
            ..Default::default()
        };
        let fenced_end_response = crate::txn::handlers::end_txn::handle(
            &broker,
            end_version,
            5,
            &crate::test_support::encode_request(&fenced_end_request, end_version),
            &context,
        )
        .await
        .expect("reject fenced recovery client");
        let fenced_end_response: crabka_protocol::owned::end_txn_response::EndTxnResponse =
            crate::test_support::decode_response(&fenced_end_response, end_version);
        assert!(fenced_end_response.error_code == codes::INVALID_PRODUCER_EPOCH);

        let end_request = crabka_protocol::owned::end_txn_request::EndTxnRequest {
            transactional_id: tids[2].to_string(),
            producer_id: second_recovery_response.producer_id,
            producer_epoch: second_recovery_response.producer_epoch,
            committed: true,
            ..Default::default()
        };
        let end_response = crate::txn::handlers::end_txn::handle(
            &broker,
            end_version,
            6,
            &crate::test_support::encode_request(&end_request, end_version),
            &context,
        )
        .await
        .expect("complete recovered transaction");
        let end_response: crabka_protocol::owned::end_txn_response::EndTxnResponse =
            crate::test_support::decode_response(&end_response, end_version);
        assert!(end_response.error_code == codes::NONE);
        assert!(end_response.producer_id == second_recovery_response.producer_id);
        assert!(end_response.producer_epoch == second_recovery_response.producer_epoch + 1);

        let retry_response = crate::txn::handlers::end_txn::handle(
            &broker,
            end_version,
            7,
            &crate::test_support::encode_request(&end_request, end_version),
            &context,
        )
        .await
        .expect("retry recovered transaction completion");
        let retry_response: crabka_protocol::owned::end_txn_response::EndTxnResponse =
            crate::test_support::decode_response(&retry_response, end_version);
        assert!(retry_response == end_response);
        let completed = broker
            .txn_coordinator
            .get(tids[2])
            .expect("completed 2PC transaction entry");
        let completed = completed.lock().await;
        assert!(completed.state == TxnState::CompleteCommit);
        assert!(completed.next_producer_id.is_none());
        assert!(completed.next_producer_epoch == -1);
        broker_handle.shutdown().await;
    }
}
