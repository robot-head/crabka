//! `AddPartitionsToTxn` (`api_key=24`). Registers one or more
//! (topic, partition) pairs with an ongoing transaction.
//!
//! Wire-format versions:
//!  - v0-3: single `(transactional_id, producer_id, producer_epoch, topics)`
//!    on the request; `results_by_topic_v3_and_below` on the response.
//!  - v4-5: batched `transactions` array on the request;
//!    `results_by_transaction` on the response.
//!
//! This broker only handles the single-tid case (the only shape a
//! producer client ever sends). If a v4+ request carries more than one
//! transaction entry we process them all sequentially.
//!
//! ## ACL preamble
//!
//! Per transaction in the request:
//! * `Write` on `TransactionalId(tid)`. Deny → every topic row in that
//!   transaction's results emits
//!   `TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)` on every partition.
//! * Per topic, `Write` on `Topic(name)`. Deny → that topic's partition
//!   rows emit `TOPIC_AUTHORIZATION_FAILED (29)`.

use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, MetadataImage, ResourceType};
use crabka_protocol::owned::add_partitions_to_txn_request::AddPartitionsToTxnRequest;
use crabka_protocol::owned::add_partitions_to_txn_response::{
    AddPartitionsToTxnResponse, AddPartitionsToTxnResult,
};
use crabka_protocol::owned::common::add_partitions_to_txn_response::add_partitions_to_txn_partition_result::AddPartitionsToTxnPartitionResult;
use crabka_protocol::owned::common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic;
use crabka_protocol::owned::common::add_partitions_to_txn_response::add_partitions_to_txn_topic_result::AddPartitionsToTxnTopicResult;
use crabka_protocol::{Decode, Encode};
use crabka_security::Principal;

use crate::authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer, authorize_topics};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::txn::state::{TopicPartition, TxnState};
use crate::txn::util::now_millis;

#[tracing::instrument(
    name = "handle_add_partitions_to_txn",
    level = "info",
    skip_all,
    fields(api = "AddPartitionsToTxn", version, req_bytes = req_bytes.len()),
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
    let req = AddPartitionsToTxnRequest::decode(&mut cur, version)?;

    // Refresh leader-partition view from the current metadata image
    // before checking coordinator-ness, to avoid a race.
    let image = controller.current_image();
    let txnv = crate::txn::version::resolve_txn_version(&image);
    coord.refresh_leader_partitions(&image).await;

    if version >= 4 {
        handle_v4(
            &coord,
            version,
            &req,
            &image,
            txnv,
            authorizer,
            ctx.principal,
            ctx.peer,
        )
        .await
    } else {
        handle_v3(
            &coord,
            version,
            &req,
            &image,
            txnv,
            authorizer,
            ctx.principal,
            ctx.peer,
        )
        .await
    }
}

// ── v4+ path ─────────────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn handle_v4(
    coord: &crate::txn::coordinator::TxnCoordinator,
    version: i16,
    req: &AddPartitionsToTxnRequest,
    image: &MetadataImage,
    txnv: crate::txn::version::TxnVersion,
    authorizer: &dyn Authorizer,
    principal: &Principal,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    let mut results_by_transaction: Vec<AddPartitionsToTxnResult> =
        Vec::with_capacity(req.transactions.len());

    for txn in &req.transactions {
        // ── ACL preamble: per-txn Write on TransactionalId ─────
        let tid_req = AuthorizationRequest {
            principal,
            host: peer,
            resource_type: ResourceType::TransactionalId,
            resource_name: txn.transactional_id.as_str(),
            operation: AclOperation::Write,
        };
        let topic_results = if authorizer.authorize(image, &tid_req) == AuthorizationResult::Deny {
            topic_error(&txn.topics, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
        } else {
            // Per-topic Write check.
            let denied = denied_topics(authorizer, image, principal, peer, &txn.topics);
            process_one_txn(
                coord,
                txn.transactional_id.as_str(),
                txn.producer_id,
                txn.producer_epoch,
                &txn.topics,
                &denied,
                txnv,
                txn.verify_only,
            )
            .await
        };
        results_by_transaction.push(AddPartitionsToTxnResult {
            transactional_id: txn.transactional_id.clone(),
            topic_results,
            ..Default::default()
        });
    }

    let resp = AddPartitionsToTxnResponse {
        results_by_transaction,
        ..Default::default()
    };
    encode_response(&resp, version)
}

// ── v0-3 path ─────────────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn handle_v3(
    coord: &crate::txn::coordinator::TxnCoordinator,
    version: i16,
    req: &AddPartitionsToTxnRequest,
    image: &MetadataImage,
    txnv: crate::txn::version::TxnVersion,
    authorizer: &dyn Authorizer,
    principal: &Principal,
    peer: &SocketAddr,
) -> Result<Bytes, BrokerError> {
    // ── ACL preamble: Write on TransactionalId ────────────────
    let tid_req = AuthorizationRequest {
        principal,
        host: peer,
        resource_type: ResourceType::TransactionalId,
        resource_name: req.v3_and_below_transactional_id.as_str(),
        operation: AclOperation::Write,
    };
    let topic_results = if authorizer.authorize(image, &tid_req) == AuthorizationResult::Deny {
        topic_error(
            &req.v3_and_below_topics,
            codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
        )
    } else {
        let denied = denied_topics(authorizer, image, principal, peer, &req.v3_and_below_topics);
        process_one_txn(
            coord,
            req.v3_and_below_transactional_id.as_str(),
            req.v3_and_below_producer_id,
            req.v3_and_below_producer_epoch,
            &req.v3_and_below_topics,
            &denied,
            txnv,
            // v0-3 has no `verify_only` field (predates KIP-890); always add.
            false,
        )
        .await
    };

    let resp = AddPartitionsToTxnResponse {
        results_by_topic_v3_and_below: topic_results,
        ..Default::default()
    };
    encode_response(&resp, version)
}

// ── shared per-transaction logic ──────────────────────────────────────────────

/// Build the set of topic names denied `Write` on `Topic(name)` for this
/// principal/host. Caller uses this to stamp `TOPIC_AUTHORIZATION_FAILED`
/// on every partition row of denied topics.
fn denied_topics(
    authorizer: &dyn Authorizer,
    image: &MetadataImage,
    principal: &Principal,
    peer: &SocketAddr,
    topics: &[AddPartitionsToTxnTopic],
) -> std::collections::HashSet<String> {
    let names: Vec<&str> = topics.iter().map(|t| t.name.as_str()).collect();
    let map = authorize_topics(
        authorizer,
        image,
        principal,
        peer,
        AclOperation::Write,
        names,
    );
    map.into_iter()
        .filter_map(|(name, r)| {
            if r == AuthorizationResult::Deny {
                Some(name.to_string())
            } else {
                None
            }
        })
        .collect()
}

/// Process a single `transactional_id` / `producer_id` / `producer_epoch`.
/// Returns per-topic, per-partition result entries. Topics named in
/// `denied` short-circuit with `TOPIC_AUTHORIZATION_FAILED`; the remaining
/// topics go through the state-machine check and partition registration.
#[allow(clippy::too_many_arguments)]
// cargo-mutants: I/O over live txn state + partition registration
#[cfg_attr(test, mutants::skip)]
async fn process_one_txn(
    coord: &crate::txn::coordinator::TxnCoordinator,
    tid: &str,
    producer_id: i64,
    producer_epoch: i16,
    topics: &[AddPartitionsToTxnTopic],
    denied: &std::collections::HashSet<String>,
    txnv: crate::txn::version::TxnVersion,
    verify_only: bool,
) -> Vec<AddPartitionsToTxnTopicResult> {
    // Topics allowed to proceed past the per-topic Write ACL gate.
    let allowed_topics: Vec<&AddPartitionsToTxnTopic> = topics
        .iter()
        .filter(|t| !denied.contains(&t.name))
        .collect();

    // 1. Coordinator check (applies only to non-denied topics — for
    //    denied topics we always emit TOPIC_AUTHORIZATION_FAILED).
    if !coord.is_coordinator_for(tid).await {
        return per_topic_with_denied(topics, denied, codes::NOT_COORDINATOR);
    }

    // 2. Look up entry; verify (pid, epoch).
    let Some(entry_mutex) = coord.get(tid) else {
        return per_topic_with_denied(topics, denied, codes::INVALID_PRODUCER_ID_MAPPING);
    };

    let mut entry = entry_mutex.lock().await;
    if entry.producer_id != producer_id || entry.producer_epoch != producer_epoch {
        return per_topic_with_denied(topics, denied, codes::INVALID_PRODUCER_EPOCH);
    }

    // KIP-890 TV_2 server-side verification: confirm each requested
    // partition is already part of the producer's ongoing txn; never add,
    // never touch state, never persist. Absent partitions get
    // TRANSACTION_ABORTABLE so the client aborts. Below TV_2, or with
    // verify_only=false, this is skipped and the classic add path runs
    // unchanged (verify_only is ignored, matching pre-KIP-890 behavior).
    if txnv.verified() && verify_only {
        return verify_partitions(&entry, topics, denied);
    }

    // 3. State machine: Empty/Ongoing → Ongoing.
    //    CompleteCommit/CompleteAbort → Ongoing is also allowed to support
    //    re-use of a transactional_id without an intervening InitProducerId.
    //    In that case we clear the stale partition set so EndTxn only fans out
    //    markers to the new transaction's partitions.
    if !entry.state.can_transition_to(TxnState::Ongoing) {
        return per_topic_with_denied(topics, denied, codes::INVALID_TXN_STATE);
    }
    let prior_state = entry.state;
    let was_complete = matches!(
        prior_state,
        TxnState::CompleteCommit | TxnState::CompleteAbort
    );
    entry.state = TxnState::Ongoing;
    if was_complete {
        // Starting a new transaction after a completed one: discard the stale
        // partition set so the new transaction starts clean.
        entry.partitions.clear();
    }
    // KIP-98/KIP-939: a transaction "starts" on the edge into Ongoing. Stamp
    // the start timestamp here (Kafka's `txnStartTimestamp`) so the idle-txn
    // reaper measures the timeout from the real start, not from InitProducerId.
    // A partition added to an already-Ongoing transaction keeps the original
    // start, so an active producer can't keep resetting its own timeout.
    if prior_state != TxnState::Ongoing {
        entry.start_ms = now_millis();
    }

    // 4. Register partitions for ALLOWED topics only.
    for t in &allowed_topics {
        for &p in &t.partitions {
            entry.partitions.insert(TopicPartition {
                topic: t.name.clone(),
                partition: p,
            });
        }
    }
    entry.last_update_ms = now_millis();
    let snap = entry.clone();
    // Drop lock before the async persist call.
    drop(entry);

    // 5. Persist.
    if let Err(e) = coord.put(snap, txnv).await {
        tracing::error!(tid, error = %e, "AddPartitionsToTxn: failed to persist TxnEntry");
        return per_topic_with_denied(topics, denied, codes::UNKNOWN_SERVER_ERROR);
    }

    // 6. Success — NONE for allowed topics, TOPIC_AUTHORIZATION_FAILED for denied.
    per_topic_with_denied(topics, denied, codes::NONE)
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// KIP-890 `TV_2` verify-only per-partition decision: `NONE (0)` if the
/// partition is already part of the ongoing transaction, else
/// `TRANSACTION_ABORTABLE (120)`. Matches cp-kafka 4.0's verify-only path:
/// `if txnMetadata.topicPartitions.contains(part) NONE else TRANSACTION_ABORTABLE`.
fn verify_partition_code(entry: &crate::txn::state::TxnEntry, tp: &TopicPartition) -> i16 {
    if entry.partitions.contains(tp) {
        codes::NONE
    } else {
        codes::TRANSACTION_ABORTABLE
    }
}

/// Build the verify-only response. Same shape as the add path's
/// `per_topic_with_denied`, but each partition carries the verify result
/// rather than a single shared code. Denied topics still short-circuit to
/// `TOPIC_AUTHORIZATION_FAILED` on every partition row.
fn verify_partitions(
    entry: &crate::txn::state::TxnEntry,
    topics: &[AddPartitionsToTxnTopic],
    denied: &std::collections::HashSet<String>,
) -> Vec<AddPartitionsToTxnTopicResult> {
    topics
        .iter()
        .map(|t| {
            let topic_denied = denied.contains(&t.name);
            AddPartitionsToTxnTopicResult {
                name: t.name.clone(),
                results_by_partition: t
                    .partitions
                    .iter()
                    .map(|&p| {
                        let row_code = if topic_denied {
                            codes::TOPIC_AUTHORIZATION_FAILED
                        } else {
                            verify_partition_code(
                                entry,
                                &TopicPartition {
                                    topic: t.name.clone(),
                                    partition: p,
                                },
                            )
                        };
                        AddPartitionsToTxnPartitionResult {
                            partition_index: p,
                            partition_error_code: row_code,
                            ..Default::default()
                        }
                    })
                    .collect(),
                ..Default::default()
            }
        })
        .collect()
}

/// Build a per-topic/per-partition result list. Topics named in `denied`
/// get `TOPIC_AUTHORIZATION_FAILED (29)` on every partition row; the rest
/// get `code`.
fn per_topic_with_denied(
    topics: &[AddPartitionsToTxnTopic],
    denied: &std::collections::HashSet<String>,
    code: i16,
) -> Vec<AddPartitionsToTxnTopicResult> {
    topics
        .iter()
        .map(|t| {
            let row_code = if denied.contains(&t.name) {
                codes::TOPIC_AUTHORIZATION_FAILED
            } else {
                code
            };
            AddPartitionsToTxnTopicResult {
                name: t.name.clone(),
                results_by_partition: t
                    .partitions
                    .iter()
                    .map(|&p| AddPartitionsToTxnPartitionResult {
                        partition_index: p,
                        partition_error_code: row_code,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }
        })
        .collect()
}

/// Build a per-topic/per-partition result list with every partition carrying
/// `error_code` (used by whole-txn errors like the txn-id ACL deny path).
fn topic_error(
    topics: &[AddPartitionsToTxnTopic],
    code: i16,
) -> Vec<AddPartitionsToTxnTopicResult> {
    topics
        .iter()
        .map(|t| AddPartitionsToTxnTopicResult {
            name: t.name.clone(),
            results_by_partition: t
                .partitions
                .iter()
                .map(|&p| AddPartitionsToTxnPartitionResult {
                    partition_index: p,
                    partition_error_code: code,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect()
}

fn encode_response(resp: &AddPartitionsToTxnResponse, version: i16) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use assert2::assert;
    use crabka_protocol::owned::add_partitions_to_txn_request::AddPartitionsToTxnTransaction;
    use crabka_security::Principal;

    use super::*;
    use crate::test_support::{DenyAll, peer};
    use crate::txn::state::TxnEntry;

    #[test]
    fn verify_only_codes_present_vs_absent() {
        let mut e = TxnEntry::new_empty("t".into(), 1, 0, 30_000, 0);
        let present = TopicPartition {
            topic: "a".into(),
            partition: 0,
        };
        e.partitions.insert(present.clone());
        let absent = TopicPartition {
            topic: "b".into(),
            partition: 0,
        };
        assert!(verify_partition_code(&e, &present) == codes::NONE);
        assert!(verify_partition_code(&e, &absent) == codes::TRANSACTION_ABORTABLE);
    }

    fn topic(name: &str, partitions: &[i32]) -> AddPartitionsToTxnTopic {
        AddPartitionsToTxnTopic {
            name: name.into(),
            partitions: partitions.to_vec(),
            ..Default::default()
        }
    }

    /// Build a fully-pinned expected topic-result row: every field spelled
    /// out explicitly so whole-value comparisons kill field-drop mutants.
    fn topic_result(name: &str, rows: &[(i32, i16)]) -> AddPartitionsToTxnTopicResult {
        AddPartitionsToTxnTopicResult {
            name: name.into(),
            results_by_partition: rows
                .iter()
                .map(
                    |&(partition_index, partition_error_code)| AddPartitionsToTxnPartitionResult {
                        partition_index,
                        partition_error_code,
                        unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
                    },
                )
                .collect(),
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        }
    }

    #[test]
    fn verify_partitions_preserves_topic_and_partition_rows() {
        let mut e = TxnEntry::new_empty("t".into(), 1, 0, 30_000, 0);
        e.partitions.insert(TopicPartition {
            topic: "alpha".into(),
            partition: 1,
        });
        let topics = vec![topic("alpha", &[1, 2]), topic("denied", &[3])];
        let denied = HashSet::from(["denied".to_string()]);

        let rows = verify_partitions(&e, &topics, &denied);

        let expected = vec![
            topic_result(
                "alpha",
                &[(1, codes::NONE), (2, codes::TRANSACTION_ABORTABLE)],
            ),
            topic_result("denied", &[(3, codes::TOPIC_AUTHORIZATION_FAILED)]),
        ];
        assert!(rows == expected);
    }

    #[test]
    fn per_topic_with_denied_preserves_rows_and_overrides_denied_topics() {
        let topics = vec![topic("alpha", &[1, 2]), topic("denied", &[3])];
        let denied = HashSet::from(["denied".to_string()]);

        let rows = per_topic_with_denied(&topics, &denied, codes::NOT_COORDINATOR);

        let expected = vec![
            topic_result(
                "alpha",
                &[(1, codes::NOT_COORDINATOR), (2, codes::NOT_COORDINATOR)],
            ),
            topic_result("denied", &[(3, codes::TOPIC_AUTHORIZATION_FAILED)]),
        ];
        assert!(rows == expected);
    }

    #[test]
    fn topic_error_preserves_each_requested_partition() {
        let topics = vec![topic("alpha", &[4, 5])];

        let rows = topic_error(&topics, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);

        let expected = vec![topic_result(
            "alpha",
            &[
                (4, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
                (5, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
            ],
        )];
        assert!(rows == expected);
    }

    crate::test_support::wire_helpers!(
        AddPartitionsToTxnRequest,
        AddPartitionsToTxnResponse,
        client_id = "producer-client"
    );

    #[test]
    fn encode_response_round_trips_v4_transaction_results() {
        let resp = AddPartitionsToTxnResponse {
            results_by_transaction: vec![AddPartitionsToTxnResult {
                transactional_id: "tid-4".into(),
                topic_results: topic_error(&[topic("alpha", &[1])], codes::INVALID_TXN_STATE),
                ..Default::default()
            }],
            ..Default::default()
        };

        let bytes = encode_response(&resp, 4).expect("encode response");
        assert!(!bytes.is_empty());
        let decoded = decode_response(&bytes, 4);

        let expected = AddPartitionsToTxnResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            results_by_transaction: vec![AddPartitionsToTxnResult {
                transactional_id: "tid-4".into(),
                topic_results: vec![topic_result("alpha", &[(1, codes::INVALID_TXN_STATE)])],
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
            }],
            results_by_topic_v3_and_below: vec![],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(decoded == expected);
    }

    #[test]
    fn encode_response_round_trips_v3_topic_results() {
        let resp = AddPartitionsToTxnResponse {
            results_by_topic_v3_and_below: topic_error(&[topic("alpha", &[7])], codes::NONE),
            ..Default::default()
        };

        let bytes = encode_response(&resp, 3).expect("encode response");
        assert!(!bytes.is_empty());
        let decoded = decode_response(&bytes, 3);

        let expected = AddPartitionsToTxnResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            results_by_transaction: vec![],
            results_by_topic_v3_and_below: vec![topic_result("alpha", &[(7, codes::NONE)])],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(decoded == expected);
    }

    fn principal() -> Principal {
        crate::test_support::principal("ANONYMOUS")
    }

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

    #[tokio::test]
    async fn handle_v4_transactional_id_deny_returns_transaction_rows() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let principal = principal();
        let peer = peer();
        let ctx = test_context(&principal, &peer);
        let req = AddPartitionsToTxnRequest {
            transactions: vec![AddPartitionsToTxnTransaction {
                transactional_id: "tid-4".into(),
                producer_id: 11,
                producer_epoch: 2,
                verify_only: false,
                topics: vec![topic("alpha", &[1, 2])],
                ..Default::default()
            }],
            ..Default::default()
        };
        let req_bytes = encode_request(&req, 4);

        let bytes = handle(
            &broker_handle.broker_arc_for_test(),
            4,
            123,
            &req_bytes,
            &ctx,
        )
        .await
        .expect("handle");
        let resp = decode_response(&bytes, 4);

        let expected = AddPartitionsToTxnResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            results_by_transaction: vec![AddPartitionsToTxnResult {
                transactional_id: "tid-4".into(),
                topic_results: vec![topic_result(
                    "alpha",
                    &[
                        (1, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
                        (2, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
                    ],
                )],
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
            }],
            results_by_topic_v3_and_below: vec![],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_v3_transactional_id_deny_returns_topic_rows() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let principal = principal();
        let peer = peer();
        let ctx = test_context(&principal, &peer);
        let req = AddPartitionsToTxnRequest {
            v3_and_below_transactional_id: "tid-3".into(),
            v3_and_below_producer_id: 11,
            v3_and_below_producer_epoch: 2,
            v3_and_below_topics: vec![topic("alpha", &[3, 4])],
            ..Default::default()
        };
        let req_bytes = encode_request(&req, 3);

        let bytes = handle(
            &broker_handle.broker_arc_for_test(),
            3,
            123,
            &req_bytes,
            &ctx,
        )
        .await
        .expect("handle");
        let resp = decode_response(&bytes, 3);

        let expected = AddPartitionsToTxnResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            results_by_transaction: vec![],
            results_by_topic_v3_and_below: vec![topic_result(
                "alpha",
                &[
                    (3, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
                    (4, codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
                ],
            )],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
