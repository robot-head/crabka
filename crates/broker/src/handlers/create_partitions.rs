//! `CreatePartitions` (`api_key=37`). `kafka-topics --alter --partitions
//! N`. Round-robin replica placement matches the `CreateTopics`
//! path when the caller omits `assignments`; an explicit, validated
//! `assignments` list (one entry per *new* partition) overrides round-robin
//! and gets used verbatim — matching the JVM `kafka-topics
//! --alter --partitions N --replica-assignment 0:1,1:2,...` flow.

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, MetadataRecord, PartitionRecord};
use crabka_protocol::owned::create_partitions_request::{
    CreatePartitionsAssignment, CreatePartitionsRequest,
};
use crabka_protocol::owned::create_partitions_response::{
    CreatePartitionsResponse, CreatePartitionsTopicResult,
};
use crabka_protocol::{Decode, Encode};
use crabka_raft::{NodeId, RaftError};

use crate::authorizer::{AuthorizationResult, authorize_topics};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;
use crate::handlers::create_topics::round_robin_replicas;
use crate::replicator_supervisor::materialize_partition;

/// Resolve the replica list for each newly-added partition.
///
/// `provided` is the caller's `assignments` field (`None` ≙ round-robin,
/// `Some(...)` ≙ honor verbatim after validating against `known_brokers` and
/// `rf`). `existing` is the current partition count; `new_partition_count`
/// is `new_count - existing` (always > 0 by the time this helper runs; the
/// `INVALID_PARTITIONS` check fires earlier). On the round-robin path the
/// helper computes the full `0..new_count` assignment and returns only the
/// tail so the new partitions keep rotating from where the existing ones
/// left off — matching JVM behavior on `kafka-topics --alter --partitions`.
///
/// Returns one replica list per new partition (in `existing..new_count`
/// order), or an `(error_code, error_message)` pair the caller stamps into
/// the per-topic result.
fn resolve_new_partition_assignments(
    provided: Option<&Vec<CreatePartitionsAssignment>>,
    known_brokers: &[NodeId],
    existing: i32,
    new_partition_count: usize,
    rf: i16,
) -> Result<Vec<Vec<NodeId>>, (i16, String)> {
    let rf_usize = usize::try_from(rf).unwrap_or(0);
    if let Some(provided) = provided {
        // Length must match new-partition count. Empty `Some(vec![])` with
        // any new partitions fails here too — matches JVM.
        if provided.len() != new_partition_count {
            return Err((
                codes::INVALID_REPLICA_ASSIGNMENT,
                format!(
                    "assignments.len()={} does not match new partition count={new_partition_count}",
                    provided.len()
                ),
            ));
        }
        let mut out: Vec<Vec<NodeId>> = Vec::with_capacity(new_partition_count);
        for (i, a) in provided.iter().enumerate() {
            if a.broker_ids.len() != rf_usize {
                return Err((
                    codes::INVALID_REPLICA_ASSIGNMENT,
                    format!(
                        "assignment[{i}].broker_ids.len()={} does not match replication_factor={rf}",
                        a.broker_ids.len()
                    ),
                ));
            }
            let mut seen: std::collections::HashSet<i32> = std::collections::HashSet::new();
            let mut replicas: Vec<NodeId> = Vec::with_capacity(rf_usize);
            for b in &a.broker_ids {
                if !seen.insert(*b) {
                    return Err((
                        codes::INVALID_REPLICA_ASSIGNMENT,
                        format!("assignment[{i}] contains duplicate broker id {b}"),
                    ));
                }
                let Ok(b_u64) = u64::try_from(*b) else {
                    return Err((
                        codes::INVALID_REPLICA_ASSIGNMENT,
                        format!("assignment[{i}] references negative broker id {b}"),
                    ));
                };
                if !known_brokers.contains(&b_u64) {
                    return Err((
                        codes::INVALID_REPLICA_ASSIGNMENT,
                        format!("assignment[{i}] references unknown broker id {b}"),
                    ));
                }
                replicas.push(b_u64);
            }
            out.push(replicas);
        }
        Ok(out)
    } else {
        let total = existing
            .checked_add(i32::try_from(new_partition_count).unwrap_or(i32::MAX))
            .unwrap_or(i32::MAX);
        let all = round_robin_replicas(known_brokers, total, rf);
        if all.is_empty() {
            return Err((
                codes::INVALID_REPLICATION_FACTOR,
                format!(
                    "replication_factor={rf} > broker_count={}",
                    known_brokers.len()
                ),
            ));
        }
        let start = usize::try_from(existing).unwrap_or(0);
        Ok(all.into_iter().skip(start).collect())
    }
}

fn create_partitions_response(
    results: Vec<CreatePartitionsTopicResult>,
    throttle_time_ms: i32,
) -> CreatePartitionsResponse {
    CreatePartitionsResponse {
        results,
        throttle_time_ms,
        ..Default::default()
    }
}

fn encode_response<R: Encode>(resp: &R, version: i16) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

fn should_materialize_locally(replicas: &[NodeId], node_id: NodeId) -> bool {
    replicas.contains(&node_id)
}

fn is_local_leader(leader: NodeId, node_id: NodeId) -> bool {
    leader == node_id
}

#[allow(clippy::too_many_lines)]
#[tracing::instrument(
    name = "handle_create_partitions",
    level = "info",
    skip_all,
    fields(api = "CreatePartitions", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = CreatePartitionsRequest::decode(&mut cur, version)?;

    let controller = &broker.controller;
    let node_id = broker.config.node_id;
    let partitions_map = broker.partitions.clone();
    let producer_state = broker.producer_state.clone();
    let log_dirs = broker.config.all_log_dirs();
    let log_config = broker.config.log_config.clone();
    let log_dir_status = broker.log_dir_status.clone();

    let image = controller.current_image();

    // KIP-599: count partition mutations before running handler logic so that
    // even invalid/rejected requests consume quota (bad-faith clients can't
    // escape throttling by sending malformed RPCs).
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    let mutation_count: u64 = req
        .topics
        .iter()
        .map(|t| {
            let current: i32 =
                i32::try_from(image.partitions_of(&t.name).count()).unwrap_or(i32::MAX);
            (t.count - current).max(0) as u64
        })
        .sum();

    // ── ACL preamble ────────────────────────────────────────
    // Batch-authorize every topic name for `Alter`. Topics that come
    // back `Deny` short-circuit the partition-change loop and emit
    // TOPIC_AUTHORIZATION_FAILED on that topic row.
    let topic_names: Vec<&str> = req.topics.iter().map(|t| t.name.as_str()).collect();
    let acl_results = authorize_topics(
        broker.config.authorizer.as_ref(),
        &*image,
        ctx.principal,
        ctx.peer,
        AclOperation::Alter,
        topic_names.iter().copied(),
    );
    let denied_topics: std::collections::HashSet<String> = acl_results
        .iter()
        .filter_map(|(name, r)| {
            if *r == AuthorizationResult::Deny {
                Some((*name).to_string())
            } else {
                None
            }
        })
        .collect();

    let mut results: Vec<CreatePartitionsTopicResult> = Vec::with_capacity(req.topics.len());

    for t in req.topics {
        let mut out = CreatePartitionsTopicResult {
            name: t.name.clone(),
            ..Default::default()
        };

        // Per-topic ACL check.
        if denied_topics.contains(&t.name) {
            out.error_code = codes::TOPIC_AUTHORIZATION_FAILED;
            results.push(out);
            continue;
        }

        let Some(topic_rec) = image.topic(&t.name).cloned() else {
            out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
            out.error_message = Some(format!("unknown topic `{}`", t.name));
            results.push(out);
            continue;
        };

        let existing = topic_rec.partitions;
        if t.count <= existing {
            out.error_code = codes::INVALID_PARTITIONS;
            out.error_message = Some(format!(
                "topic `{}` already has {} partitions; cannot decrease to {}",
                t.name, existing, t.count
            ));
            results.push(out);
            continue;
        }

        let mut sorted_brokers: Vec<crabka_raft::NodeId> =
            image.brokers().map(|b| b.node_id).collect();
        if sorted_brokers.is_empty() {
            sorted_brokers.push(node_id);
        }
        sorted_brokers.sort_unstable();
        let rf = topic_rec.replication_factor;
        let new_count = t.count;
        let new_partition_indices: Vec<i32> = (existing..new_count).collect();
        let new_partition_count = new_partition_indices.len();
        let new_assignments = match resolve_new_partition_assignments(
            t.assignments.as_ref(),
            &sorted_brokers,
            existing,
            new_partition_count,
            rf,
        ) {
            Ok(a) => a,
            Err((code, msg)) => {
                out.error_code = code;
                out.error_message = Some(msg);
                results.push(out);
                continue;
            }
        };

        if req.validate_only {
            results.push(out);
            continue;
        }

        // Build batch: one V1Partition per new index. Under KIP-631 framing the
        // topic's partition count IS the number of PartitionRecords (the
        // `TopicRecord` carries no count), so CreatePartitions appends only the
        // new partition records — no `V1Topic` rewrite. The image derives the
        // grown count from the partitions map as these apply. (Re-submitting a
        // `V1Topic` would round-trip back to the pre-grow count and be rejected
        // by the strict-expansion `validate` on the apply path.)
        let mut records: Vec<MetadataRecord> = Vec::with_capacity(new_partition_count);
        for (i, p) in new_partition_indices.iter().enumerate() {
            let replicas = new_assignments[i].clone();
            records.push(MetadataRecord::V1Partition(PartitionRecord {
                topic: t.name.clone(),
                partition: *p,
                leader: replicas[0],
                replicas: replicas.clone(),
                isr: replicas,
                leader_epoch: 0,
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }));
        }

        match controller.submit_change(records).await {
            Ok(()) => {
                // Materialize new partitions on local disk where self in replicas.
                for (i, p) in new_partition_indices.iter().enumerate() {
                    let replicas = &new_assignments[i];
                    if should_materialize_locally(replicas, node_id) {
                        if let Err(e) = materialize_partition(
                            &partitions_map,
                            &t.name,
                            *p,
                            &log_dirs,
                            &log_config,
                            &log_dir_status,
                            &producer_state,
                        ) {
                            tracing::error!(
                                topic = %t.name, partition = *p, error = %e,
                                "CreatePartitions: materialize after quorum commit failed"
                            );
                        } else if let Some(part) = partitions_map.get(&t.name, *p) {
                            let leader = replicas[0];
                            part.install_leader_change(leader, 0).await;
                            if is_local_leader(leader, node_id) {
                                // At creation the ISR equals the full replica set.
                                part.install_isr(replicas, replicas, leader).await;
                            }
                        }
                    }
                }
            }
            Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                out.error_code = codes::NOT_CONTROLLER;
            }
            Err(e) => {
                tracing::error!(topic = %t.name, error = %e,
                    "CreatePartitions submit_change failed");
                out.error_code = codes::UNKNOWN_SERVER_ERROR;
            }
        }

        results.push(out);
    }

    // KIP-599: apply controller_mutation_rate throttle after response assembly,
    // before encoding. Sets throttle_time_ms and sleeps so the client waits.
    let delay = crate::quota::consume_controller_mutation_quota(
        &image,
        &broker.quota_buckets,
        ctx.principal.name.as_str(),
        ctx.client_id,
        mutation_count,
    );
    let resp = create_partitions_response(
        results,
        i32::try_from(delay.as_millis()).unwrap_or(i32::MAX),
    );
    if delay > std::time::Duration::ZERO {
        tokio::time::sleep(delay).await;
    }
    encode_response(&resp, version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use assert2::check;
    use crabka_metadata::TopicRecord;
    use crabka_protocol::owned::create_partitions_request::{
        CreatePartitionsAssignment, CreatePartitionsTopic,
    };
    use crabka_security::Principal;
    use std::net::SocketAddr;
    use std::sync::Arc;

    use crate::authorizer::Authorizer;
    use crate::broker::{Broker, BrokerHandle};
    use crate::test_support::{DenyAll, peer, principal};

    const VERSION: i16 = 3;

    fn assn(broker_ids: &[i32]) -> CreatePartitionsAssignment {
        CreatePartitionsAssignment {
            broker_ids: broker_ids.to_vec(),
            ..Default::default()
        }
    }

    fn topic_req(
        name: &str,
        count: i32,
        assignments: Option<Vec<CreatePartitionsAssignment>>,
    ) -> CreatePartitionsTopic {
        CreatePartitionsTopic {
            name: name.into(),
            count,
            assignments,
            ..Default::default()
        }
    }

    fn request(topics: Vec<CreatePartitionsTopic>, validate_only: bool) -> CreatePartitionsRequest {
        CreatePartitionsRequest {
            topics,
            timeout_ms: 5_000,
            validate_only,
            ..Default::default()
        }
    }

    fn encode_request(req: &CreatePartitionsRequest) -> Bytes {
        crate::test_support::encode_request(req, VERSION)
    }

    fn decode_response(bytes: &Bytes) -> CreatePartitionsResponse {
        crate::test_support::decode_response(bytes, VERSION)
    }

    fn test_context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::test_support::request_context(principal, peer, "admin-client")
    }

    async fn start_broker(authorizer: Arc<dyn Authorizer>) -> (BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|cfg| {
            cfg.audit_enabled = false;
            cfg.authorizer = authorizer;
        })
        .await
    }

    async fn seed_topic(handle: &BrokerHandle, name: &str, partitions: i32, rf: i16) {
        let replicas = vec![handle.node_id()];
        let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
            name: name.into(),
            topic_id: uuid::Uuid::new_v4(),
            partitions,
            replication_factor: rf,
        })];
        for partition in 0..partitions {
            records.push(MetadataRecord::V1Partition(PartitionRecord {
                topic: name.into(),
                partition,
                leader: handle.node_id(),
                replicas: replicas.clone(),
                isr: replicas.clone(),
                leader_epoch: 0,
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }));
        }
        handle
            .broker_arc_for_test()
            .controller
            .submit_change(records)
            .await
            .expect("seed topic");
    }

    async fn seed_controller_quota(handle: &BrokerHandle, rate: f64) {
        handle
            .broker_arc_for_test()
            .controller
            .submit_change(vec![MetadataRecord::V1ClientQuota(
                crabka_metadata::ClientQuotaRecord {
                    entity: vec![
                        crabka_metadata::QuotaEntity {
                            entity_type: "user".into(),
                            entity_name: Some("admin".into()),
                        },
                        crabka_metadata::QuotaEntity {
                            entity_type: "client-id".into(),
                            entity_name: Some("admin-client".into()),
                        },
                    ],
                    config_key: "controller_mutation_rate".into(),
                    config_value: Some(rate),
                },
            )])
            .await
            .expect("seed quota");
    }

    async fn drive(
        broker: &Broker,
        req: &CreatePartitionsRequest,
        principal: &Principal,
        peer: &SocketAddr,
    ) -> CreatePartitionsResponse {
        let ctx = test_context(principal, peer);
        let req_bytes = encode_request(req);
        let bytes = handle(broker, VERSION, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        decode_response(&bytes)
    }

    #[test]
    fn round_robin_when_assignments_none() {
        let brokers: Vec<NodeId> = vec![0, 1, 2];
        let out = resolve_new_partition_assignments(None, &brokers, 0, 3, 2)
            .expect("round-robin should succeed");
        assert!(out.len() == 3);
        for r in &out {
            assert!(r.len() == 2, "each replica list must be rf=2");
            for b in r {
                assert!(brokers.contains(b));
            }
        }
    }

    #[test]
    fn round_robin_continues_rotation_from_existing() {
        let brokers: Vec<NodeId> = vec![0, 1, 2];
        // Topic already has 2 partitions; adding 2 more (so partitions 2..4).
        // Helper must return the *tail* of `round_robin_replicas(...,4,2)`,
        // i.e. the assignments for indices 2 and 3 — not start from rotation 0.
        let new_tail = resolve_new_partition_assignments(None, &brokers, 2, 2, 2)
            .expect("round-robin tail should succeed");
        let full = crate::handlers::create_topics::round_robin_replicas(&brokers, 4, 2);
        assert!(new_tail == full[2..]);
    }

    #[test]
    fn round_robin_rf_exceeds_broker_count_returns_invalid_rf() {
        let brokers: Vec<NodeId> = vec![0, 1];
        let err = resolve_new_partition_assignments(None, &brokers, 0, 1, 3)
            .expect_err("rf=3 against 2 brokers must fail");
        assert!(err.0 == codes::INVALID_REPLICATION_FACTOR);
    }

    #[test]
    fn honored_assignments_pass_through_verbatim() {
        let brokers: Vec<NodeId> = vec![0, 1, 2, 3];
        let provided = vec![assn(&[3, 1]), assn(&[2, 0]), assn(&[1, 3])];
        let out = resolve_new_partition_assignments(Some(&provided), &brokers, 0, 3, 2)
            .expect("explicit assignments should pass validation");
        assert!(out == vec![vec![3, 1], vec![2, 0], vec![1, 3]]);
    }

    #[test]
    fn explicit_length_mismatch_returns_invalid_replica_assignment() {
        let brokers: Vec<NodeId> = vec![0, 1, 2];
        let provided = vec![assn(&[0, 1]), assn(&[1, 2])];
        let err = resolve_new_partition_assignments(Some(&provided), &brokers, 0, 3, 2)
            .expect_err("2 assignments for 3 new partitions must fail");
        let expected = (
            codes::INVALID_REPLICA_ASSIGNMENT,
            "assignments.len()=2 does not match new partition count=3".to_string(),
        );
        assert!(err == expected);
    }

    #[test]
    fn explicit_wrong_rf_returns_invalid_replica_assignment() {
        let brokers: Vec<NodeId> = vec![0, 1, 2];
        let provided = vec![assn(&[0, 1, 2])]; // 3 replicas, but rf=2
        let err = resolve_new_partition_assignments(Some(&provided), &brokers, 0, 1, 2)
            .expect_err("rf mismatch must fail");
        assert!(err.0 == codes::INVALID_REPLICA_ASSIGNMENT);
        assert!(err.1.contains("does not match replication_factor=2"));
    }

    #[test]
    fn explicit_duplicate_broker_in_assignment_returns_invalid_replica_assignment() {
        let brokers: Vec<NodeId> = vec![0, 1, 2];
        let provided = vec![assn(&[1, 1])]; // duplicate
        let err = resolve_new_partition_assignments(Some(&provided), &brokers, 0, 1, 2)
            .expect_err("duplicate broker must fail");
        assert!(err.0 == codes::INVALID_REPLICA_ASSIGNMENT);
        assert!(err.1.contains("duplicate broker id 1"));
    }

    #[test]
    fn explicit_unknown_broker_returns_invalid_replica_assignment() {
        let brokers: Vec<NodeId> = vec![0, 1, 2];
        let provided = vec![assn(&[0, 9])]; // 9 unknown
        let err = resolve_new_partition_assignments(Some(&provided), &brokers, 0, 1, 2)
            .expect_err("unknown broker must fail");
        assert!(err.0 == codes::INVALID_REPLICA_ASSIGNMENT);
        assert!(err.1.contains("unknown broker id 9"));
    }

    #[test]
    fn explicit_negative_broker_id_returns_invalid_replica_assignment() {
        let brokers: Vec<NodeId> = vec![0, 1, 2];
        let provided = vec![assn(&[0, -1])];
        let err = resolve_new_partition_assignments(Some(&provided), &brokers, 0, 1, 2)
            .expect_err("negative broker id must fail");
        assert!(err.0 == codes::INVALID_REPLICA_ASSIGNMENT);
        assert!(err.1.contains("negative broker id -1"));
    }

    #[test]
    fn empty_assignments_some_with_new_partitions_fails() {
        let brokers: Vec<NodeId> = vec![0, 1];
        let provided: Vec<CreatePartitionsAssignment> = vec![];
        let err = resolve_new_partition_assignments(Some(&provided), &brokers, 0, 2, 1)
            .expect_err("Some(empty) for >0 new partitions must fail");
        assert!(err.0 == codes::INVALID_REPLICA_ASSIGNMENT);
        assert!(err.1.contains("assignments.len()=0"));
    }

    #[test]
    fn encode_response_writes_decodable_results_and_throttle() {
        let bytes = encode_response(
            &create_partitions_response(
                vec![CreatePartitionsTopicResult {
                    name: "orders".into(),
                    error_code: codes::INVALID_PARTITIONS,
                    error_message: Some("bad count".into()),
                    ..Default::default()
                }],
                321,
            ),
            VERSION,
        )
        .expect("encode");
        let resp = decode_response(&bytes);

        let expected = CreatePartitionsResponse {
            throttle_time_ms: 321,
            results: vec![CreatePartitionsTopicResult {
                name: "orders".into(),
                error_code: codes::INVALID_PARTITIONS,
                error_message: Some("bad count".into()),
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }

    #[test]
    fn local_materialization_predicates_track_replica_membership_and_leader() {
        check!(should_materialize_locally(&[1, 2], 1));
        check!(should_materialize_locally(&[1, 2], 2));
        check!(!should_materialize_locally(&[1, 2], 3));
        check!(is_local_leader(1, 1));
        check!(!is_local_leader(2, 1));
    }

    #[tokio::test]
    async fn handle_denies_topic_alter_for_each_topic() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let req = request(
            vec![topic_req("orders", 2, None), topic_req("payments", 2, None)],
            false,
        );

        let resp = drive(&broker, &req, &p, &peer).await;

        let expected = CreatePartitionsResponse {
            throttle_time_ms: 0,
            results: vec![
                CreatePartitionsTopicResult {
                    name: "orders".into(),
                    error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    error_message: None,
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
                },
                CreatePartitionsTopicResult {
                    name: "payments".into(),
                    error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    error_message: None,
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
                },
            ],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_reports_unknown_topic_and_rejects_same_partition_count() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_topic(&broker_handle, "stable", 2, 1).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let req = request(
            vec![topic_req("missing", 3, None), topic_req("stable", 2, None)],
            false,
        );

        let resp = drive(&broker, &req, &p, &peer).await;

        let expected = CreatePartitionsResponse {
            throttle_time_ms: 0,
            results: vec![
                CreatePartitionsTopicResult {
                    name: "missing".into(),
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    error_message: Some("unknown topic `missing`".into()),
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
                },
                CreatePartitionsTopicResult {
                    name: "stable".into(),
                    error_code: codes::INVALID_PARTITIONS,
                    error_message: Some(
                        "topic `stable` already has 2 partitions; cannot decrease to 2".into(),
                    ),
                    unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
                },
            ],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        assert!(
            broker_handle
                .controller_image_for_test()
                .partitions_of("stable")
                .count()
                == 2
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn validate_only_reports_success_without_adding_partitions() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_topic(&broker_handle, "dry-run", 1, 1).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let req = request(vec![topic_req("dry-run", 3, None)], true);

        let resp = drive(&broker, &req, &p, &peer).await;

        let expected = CreatePartitionsResponse {
            throttle_time_ms: 0,
            results: vec![CreatePartitionsTopicResult {
                name: "dry-run".into(),
                error_code: codes::NONE,
                error_message: None,
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        assert!(
            broker_handle
                .controller_image_for_test()
                .partitions_of("dry-run")
                .count()
                == 1
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_adds_new_partitions_and_preserves_response_identity() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_topic(&broker_handle, "grow", 1, 1).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let req = request(
            vec![topic_req("grow", 3, Some(vec![assn(&[1]), assn(&[1])]))],
            false,
        );

        let resp = drive(&broker, &req, &p, &peer).await;

        let expected = CreatePartitionsResponse {
            throttle_time_ms: 0,
            results: vec![CreatePartitionsTopicResult {
                name: "grow".into(),
                error_code: codes::NONE,
                error_message: None,
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        assert!(
            broker_handle
                .controller_image_for_test()
                .partitions_of("grow")
                .count()
                == 3
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn throttled_create_partitions_counts_only_new_partitions_and_waits() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_topic(&broker_handle, "metered", 2, 1).await;
        seed_controller_quota(&broker_handle, 2.0).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let req = request(vec![topic_req("metered", 5, None)], false);

        let started = tokio::time::Instant::now();
        let resp = drive(&broker, &req, &p, &peer).await;
        let elapsed = started.elapsed();

        let expected = CreatePartitionsResponse {
            throttle_time_ms: 500,
            results: vec![CreatePartitionsTopicResult {
                name: "metered".into(),
                error_code: codes::NONE,
                error_message: None,
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        check!(
            elapsed >= std::time::Duration::from_millis(450),
            "handler must wait for the advertised throttle, elapsed={elapsed:?}"
        );
        check!(
            broker_handle
                .controller_image_for_test()
                .partitions_of("metered")
                .count()
                == 5
        );
        broker_handle.shutdown().await;
    }
}
