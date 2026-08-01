//! `InitProducerId` (`api_key=22`). Hands out `(producer_id, producer_epoch)`
//! to a producer, or initialises / re-initialises a transactional producer.
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
            let (pid, epoch) = producer_ids.allocate();
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
            if req.enable2_pc {
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
            // keepPreparedTxn (the prepared-txn recovery flow) is not yet a
            // stable feature in Apache Kafka — its coordinator returns
            // UNSUPPORTED_VERSION. Match that until the recovery path lands.
            if req.keep_prepared_txn {
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
                    producer_id_expiration_ms: broker.config.producer_id_expiration_ms,
                    max_produce_group: broker.config.max_produce_group,
                    partition_writer_queue_depth: broker.config.partition_writer_queue_depth,
                    diskless: false,
                    hot_tail: None,
                    wal_shards: None,
                    sequencer: None,
                })
                .map_err(BrokerError::Txn)?;
                let txn_timeout = crate::txn::two_pc::resolve_txn_timeout(
                    req.enable2_pc,
                    req.transaction_timeout_ms,
                    broker.config.transaction_min_timeout_ms,
                    broker.config.transaction_max_timeout_ms,
                );
                handle_transactional(&coord, tid, txnv, txn_timeout).await?
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
) -> Result<InitProducerIdResponse, BrokerError> {
    let now_ms = now_millis();

    match coord.get(tid) {
        None => {
            // Fresh tid — allocate a new producer id.
            let (pid, epoch) = coord.producer_ids.allocate();
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
            // Reusing tid — bump epoch (KIP-1319 v2). If prior state was
            // Ongoing, write PrepareAbort + dispatch abort markers before
            // responding.
            {
                let mut e = existing.lock().await;
                if matches!(e.state, TxnState::Ongoing) {
                    // Transition to PrepareAbort; persist; dispatch markers.
                    e.state = TxnState::PrepareAbort;
                    e.last_update_ms = now_ms;
                    let entry_clone = e.clone();
                    drop(e); // release lock while we fan out markers
                    coord.put(entry_clone.clone(), txnv).await?;
                    dispatch_abort_markers(coord, &entry_clone).await?;
                    // Re-acquire + transition to CompleteAbort.
                    let mut e2 = existing.lock().await;
                    e2.state = TxnState::CompleteAbort;
                    e2.last_update_ms = now_millis();
                    let snap = e2.clone();
                    drop(e2);
                    coord.put(snap, txnv).await?;
                }
            }

            // Bump epoch on the existing entry. Persist a new TxnEntry with
            // new epoch, Empty state, cleared partitions.
            let mut e3 = existing.lock().await;
            let new_epoch = e3.producer_epoch.checked_add(1).unwrap_or(0);
            *e3 = TxnEntry::new_empty(
                tid.to_string(),
                e3.producer_id,
                new_epoch,
                txn_timeout,
                now_ms,
            );
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

async fn dispatch_abort_markers(
    coord: &TxnCoordinator,
    entry: &TxnEntry,
) -> Result<(), BrokerError> {
    use crate::txn::marker::{MarkerType, build_marker_batch};
    for tp in &entry.partitions {
        let Some(part) = coord.partitions.get(&tp.topic, tp.partition) else {
            // Not locally-led; would require inter-broker WriteTxnMarkers
            // (Tasks 15-16). For abort-on-init-due-to-stale-Ongoing (rare),
            // log + skip; the data partition retains the dangling open txn
            // but the new epoch prevents the original producer from completing.
            tracing::warn!(
                topic = %tp.topic,
                partition = tp.partition.get(),
                "abort marker dispatch needs inter-broker WriteTxnMarkers (Tasks 15-16)"
            );
            continue;
        };
        let marker = build_marker_batch(
            entry.producer_id,
            entry.producer_epoch,
            part.log_end_offset(),
            MarkerType::Abort,
        );
        part.produce_batch(marker).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_ids::PartitionIndex;
    use crabka_log::{Log, LogConfig, ProducerId};

    use super::*;
    use crate::{
        test_support::{peer, principal, start_broker_with},
        txn::state::{TopicPartition, TxnEntry},
    };

    /// `dispatch_abort_markers` appends an abort control-marker batch to each
    /// locally-led partition in the entry's partition set — advancing that
    /// partition's LEO by one. A whole-function `Ok(())` replacement would
    /// skip the dispatch entirely, leaving the LEO at 0.
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

    /// A partition in the entry that isn't hosted locally is skipped without
    /// error (no marker to dispatch) — the loop's `else` branch.
    #[tokio::test]
    async fn dispatch_abort_markers_skips_non_local_partition() {
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
        // No local partition registered → nothing appended, no error.
        dispatch_abort_markers(&coord, &entry)
            .await
            .expect("skip non-local partition without error");
    }

    #[tokio::test]
    async fn handler_persists_configured_timeout_bounds_and_2pc_sentinel() {
        let (broker_handle, _dir) = start_broker_with(|config| {
            config.audit_enabled = false;
            config.transaction_state_num_partitions = 7;
            config.transaction_min_timeout_ms = 2_000;
            config.transaction_max_timeout_ms = 8_000;
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
        broker_handle.shutdown().await;
    }
}
