//! `InitProducerId` (`api_key=22`). Hands out `(producer_id, producer_epoch)`
//! to a producer, or initialises / re-initialises a transactional producer.
//!
//! Non-transactional path: slice-6 idempotent-producer support.
//! Transactional path:     slice-9 coordinator routing (Task 12).
//!
//! ## slice-13 ACL preamble
//!
//! Two distinct authorize gates branch off `req.transactional_id`:
//!
//! * `Some(non-empty)` → `Write` on
//!   `TransactionalId(transactional_id)`. Deny →
//!   `error_code = TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)`.
//! * `None | Some("")` (idempotent-only producer) →
//!   `IdempotentWrite` on `Cluster("kafka-cluster")`. Deny →
//!   `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::owned::init_producer_id_request::InitProducerIdRequest;
use crabka_protocol::owned::init_producer_id_response::InitProducerIdResponse;
use crabka_protocol::{Decode, Encode};
use crabka_security::Principal;

use crate::authorizer::{AuthorizationRequest, AuthorizationResult, authorize};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::replicator_supervisor::materialize_partition;
use crate::txn::coordinator::TxnCoordinator;
use crate::txn::state::{TxnEntry, TxnState};
use crate::txn::util::now_millis;

pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    principal: &Principal,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let producer_ids = broker.producer_ids.clone();
    let coord = broker.txn_coordinator.clone();
    let controller = broker.controller.clone();
    let log_dir = broker.config.log_dir.clone();
    let log_config = broker.config.log_config.clone();

    let mut cur: &[u8] = req_bytes;
    let req = InitProducerIdRequest::decode(&mut cur, version)?;

    // ── slice-13 ACL preamble ────────────────────────────────────────
    // Branch on whether this is an idempotent-only or transactional
    // request and gate on the appropriate resource/operation.
    {
        let image = controller.current_image();
        let super_users = &broker.config.super_users;
        match req.transactional_id.as_deref() {
            Some(tid) if !tid.is_empty() => {
                let acl_req = AuthorizationRequest {
                    principal,
                    host: peer,
                    resource_type: ResourceType::TransactionalId,
                    resource_name: tid,
                    operation: AclOperation::Write,
                };
                if authorize(&image, super_users, &acl_req) == AuthorizationResult::Deny {
                    return encode_err(version, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
                }
            }
            _ => {
                let acl_req = AuthorizationRequest {
                    principal,
                    host: peer,
                    resource_type: ResourceType::Cluster,
                    resource_name: "kafka-cluster",
                    operation: AclOperation::IdempotentWrite,
                };
                if authorize(&image, super_users, &acl_req) == AuthorizationResult::Deny {
                    return encode_err(version, codes::CLUSTER_AUTHORIZATION_FAILED);
                }
            }
        }
    }

    let resp = match req.transactional_id.as_deref() {
        None | Some("") => {
            // Non-transactional path (slice-6 idempotence).
            let (pid, epoch) = producer_ids.allocate();
            InitProducerIdResponse {
                throttle_time_ms: 0,
                error_code: codes::NONE,
                producer_id: pid,
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
            coord
                .refresh_leader_partitions(&controller.current_image())
                .await;

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
                materialize_partition(
                    &coord.partitions,
                    crate::txn::bootstrap::TOPIC,
                    txn_partition,
                    &log_dir,
                    &log_config,
                )
                .map_err(BrokerError::Txn)?;
                handle_transactional(&coord, tid, &req).await?
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

    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

fn encode_err(version: i16, error_code: i16) -> Result<Bytes, BrokerError> {
    let resp = InitProducerIdResponse {
        throttle_time_ms: 0,
        error_code,
        producer_id: -1,
        producer_epoch: -1,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

/// Transactional sub-path: allocate or bump-epoch for `tid`.
async fn handle_transactional(
    coord: &Arc<TxnCoordinator>,
    tid: &str,
    req: &InitProducerIdRequest,
) -> Result<InitProducerIdResponse, BrokerError> {
    let now_ms = now_millis();
    let txn_timeout = req.transaction_timeout_ms.clamp(1_000, 15 * 60 * 1_000);

    match coord.get(tid) {
        None => {
            // Fresh tid — allocate a new producer id.
            let (pid, epoch) = coord.producer_ids.allocate();
            let entry = TxnEntry::new_empty(tid.to_string(), pid, epoch, txn_timeout, now_ms);
            coord.put(entry).await?;
            Ok(InitProducerIdResponse {
                error_code: codes::NONE,
                producer_id: pid,
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
                    coord.put(entry_clone.clone()).await?;
                    dispatch_abort_markers(coord, &entry_clone).await?;
                    // Re-acquire + transition to CompleteAbort.
                    let mut e2 = existing.lock().await;
                    e2.state = TxnState::CompleteAbort;
                    e2.last_update_ms = now_millis();
                    let snap = e2.clone();
                    drop(e2);
                    coord.put(snap).await?;
                }
            }

            // Bump epoch on the existing entry. Persist a new TxnEntry with
            // new epoch, Empty state, cleared partitions + offset_commit_groups.
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
            coord.put(snap.clone()).await?;
            Ok(InitProducerIdResponse {
                error_code: codes::NONE,
                producer_id: snap.producer_id,
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
        let Some(part) = coord
            .partitions
            .get(&(tp.topic.clone(), tp.partition))
            .map(|e| e.value().clone())
        else {
            // Not locally-led; would require inter-broker WriteTxnMarkers
            // (Tasks 15-16). For abort-on-init-due-to-stale-Ongoing (rare),
            // log + skip; the data partition retains the dangling open txn
            // but the new epoch prevents the original producer from completing.
            tracing::warn!(
                topic = %tp.topic,
                partition = tp.partition,
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
