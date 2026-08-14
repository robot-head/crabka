//! `CreatePartitions` (`api_key=37`), which serves
//! `kafka-topics --alter --partitions N`.
//!
//! When the caller omits `assignments`, the round-robin replica placement
//! matches the `CreateTopics` path. An explicit, validated `assignments` list,
//! with one entry per *new* partition, overrides round-robin, and the handler
//! uses it verbatim. That matches the JVM flow
//! `kafka-topics --alter --partitions N --replica-assignment 0:1,1:2,...`.

use bytes::Bytes;
use crabka_metadata::{AclOperation, MetadataRecord, PartitionRecord};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        create_partitions_request::{CreatePartitionsAssignment, CreatePartitionsRequest},
        create_partitions_response::{CreatePartitionsResponse, CreatePartitionsTopicResult},
    },
};
use crabka_raft::{NodeId, RaftError};
use crabka_units::{Time, convert::TimeExt};

use crate::{
    authorizer::{AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::create_topics::round_robin_replicas,
    replicator_supervisor::materialize_partition,
};

/// Resolve the replica list for each newly-added partition.
///
/// `provided` is the caller's `assignments` field. `None` selects
/// round-robin. `Some(...)` is used verbatim, after this function validates it
/// against `known_brokers` and `rf`.
///
/// `existing` is the current partition count. `new_partition_count` is
/// `new_count - existing`. It is always above 0 by the time this helper runs,
/// because the `INVALID_PARTITIONS` check runs earlier.
///
/// On the round-robin path the helper computes the full `0..new_count`
/// assignment and returns only the tail, so the new partitions keep rotating
/// from where the existing ones stopped. That matches the JVM behavior of
/// `kafka-topics --alter --partitions`.
///
/// It returns one replica list per new partition, in `existing..new_count`
/// order. It returns an `(error_code, error_message)` pair instead when the
/// request is invalid, and the caller stamps that pair into the per-topic
/// result.
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
                if !known_brokers.contains(&NodeId(b_u64)) {
                    return Err((
                        codes::INVALID_REPLICA_ASSIGNMENT,
                        format!("assignment[{i}] references unknown broker id {b}"),
                    ));
                }
                replicas.push(NodeId(b_u64));
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
    crate::handlers::encode_response(resp, version)
}

fn should_materialize_locally(replicas: &[NodeId], node_id: NodeId) -> bool {
    replicas.contains(&node_id)
}

fn is_local_leader(leader: NodeId, node_id: NodeId) -> bool {
    leader == node_id
}

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

    let node_id = broker.config.node_id;
    let partitions_map = broker.partitions.clone();
    let producer_state = broker.producer_state.clone();
    let log_dirs = broker.config.all_log_dirs();
    let log_config = broker.config.log_config.clone();
    let log_dir_status = broker.log_dir_status.clone();
    let hot_tail = broker.hot_tail.clone();
    let wal_shards = broker.wal_shards.clone();

    let image = broker.controller.current_image();

    // KIP-599: count partition mutations before running handler logic so that
    // even invalid/rejected requests consume quota (bad-faith clients can't
    // escape throttling by sending malformed RPCs).
    let mutation_count = partition_mutation_count(&req, &image);
    let quota = crate::quota::apply_controller_mutation_quota_mode(
        &image,
        &broker.quota_buckets,
        ctx.principal.name.as_str(),
        ctx.client_id,
        mutation_count,
        broker.config.controller_mutation_quota_window,
        broker.config.quota_throttle_max,
        version >= 3,
    );
    if quota.is_rejected() {
        let results = req
            .topics
            .iter()
            .map(|topic| CreatePartitionsTopicResult {
                name: topic.name.clone(),
                error_code: codes::THROTTLING_QUOTA_EXCEEDED,
                ..Default::default()
            })
            .collect();
        return encode_response(
            &create_partitions_response(results, crate::quota::throttle_time_ms(quota.delay())),
            version,
        );
    }

    // ── ACL preamble ────────────────────────────────────────
    // Batch-authorize every topic name for `Alter`. Topics that come
    // back `Deny` short-circuit the partition-change loop and emit
    // TOPIC_AUTHORIZATION_FAILED on that topic row.
    let denied_topics = denied_topics(
        broker.config.authorizer.as_ref(),
        &image,
        ctx.principal,
        ctx.peer,
        &req,
    );

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
        let diskless = crate::broker::diskless_topic_config(image.topic_config(&t.name));
        if t.count <= existing {
            out.error_code = codes::INVALID_PARTITIONS;
            out.error_message = Some(format!(
                "topic `{}` already has {} partitions; cannot decrease to {}",
                t.name, existing, t.count
            ));
            results.push(out);
            continue;
        }

        let sorted_brokers = sorted_brokers(&image, node_id);
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
        let records = partition_records(&t.name, &new_partition_indices, &new_assignments);

        match broker.controller.submit_change(records).await {
            Ok(_) => {
                materialize_new_partitions(
                    MaterializeContext {
                        partitions: &partitions_map,
                        log_dirs: &log_dirs,
                        log_config: &log_config,
                        log_dir_status: &log_dir_status,
                        producer_state: &producer_state,
                        producer_id_expiration: broker.config.producer_id_expiration,
                        max_produce_group: broker.config.max_produce_group,
                        partition_writer_queue_depth: broker.config.partition_writer_queue_depth,
                        diskless_wal_local_replica_count: broker
                            .config
                            .diskless_wal_local_replica_count,
                        node_id,
                        diskless,
                        topic_id: topic_rec.topic_id,
                        hot_tail: &hot_tail,
                        wal_shards: &wal_shards,
                        controller: &broker.controller,
                    },
                    &t.name,
                    &new_partition_indices,
                    &new_assignments,
                )
                .await;
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
    finish_response(quota.delay(), results, version).await
}

fn sorted_brokers(image: &crabka_metadata::MetadataImage, node_id: NodeId) -> Vec<NodeId> {
    let mut brokers: Vec<_> = image.brokers().map(|broker| broker.node_id).collect();
    if brokers.is_empty() {
        brokers.push(node_id);
    }
    brokers.sort_unstable();
    brokers
}

fn partition_records(
    topic: &str,
    indices: &[i32],
    assignments: &[Vec<NodeId>],
) -> Vec<MetadataRecord> {
    indices
        .iter()
        .zip(assignments)
        .map(|(index, replicas)| {
            MetadataRecord::V1Partition(PartitionRecord {
                topic: topic.to_string(),
                partition: *index,
                leader: replicas[0],
                replicas: replicas.clone(),
                isr: replicas.clone(),
                leader_epoch: crabka_metadata::LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            })
        })
        .collect()
}

#[derive(Clone, Copy)]
struct MaterializeContext<'a> {
    partitions: &'a std::sync::Arc<crate::partition_registry::PartitionRegistry>,
    log_dirs: &'a [std::path::PathBuf],
    log_config: &'a crabka_log::LogConfig,
    log_dir_status: &'a crate::log_dir_status::LogDirRegistry,
    producer_state: &'a std::sync::Arc<crate::producer_state::ProducerState>,
    producer_id_expiration: Time,
    max_produce_group: usize,
    partition_writer_queue_depth: usize,
    diskless_wal_local_replica_count: usize,
    node_id: NodeId,
    diskless: bool,
    topic_id: uuid::Uuid,
    hot_tail: &'a std::sync::Arc<crate::diskless::hot_tail::HotTailCache>,
    wal_shards: &'a std::sync::Arc<crate::wal::quorum::registry::WalShardRegistry>,
    controller: &'a std::sync::Arc<dyn crate::metadata_source::MetadataSource>,
}

async fn materialize_new_partitions(
    context: MaterializeContext<'_>,
    topic: &str,
    indices: &[i32],
    assignments: &[Vec<NodeId>],
) {
    for (index, replicas) in indices.iter().zip(assignments) {
        if !should_materialize_locally(replicas, context.node_id) {
            continue;
        }
        if let Err(error) =
            materialize_partition(crate::replicator_supervisor::MaterializePartitionConfig {
                partitions: context.partitions,
                topic,
                topic_id: Some(context.topic_id),
                partition: *index,
                log_dirs: context.log_dirs,
                log_config: context.log_config,
                log_dir_status: context.log_dir_status,
                producer_state: context.producer_state,
                producer_id_expiration: context.producer_id_expiration,
                max_produce_group: context.max_produce_group,
                partition_writer_queue_depth: context.partition_writer_queue_depth,
                diskless_wal_local_replica_count: context.diskless_wal_local_replica_count,
                diskless: context.diskless,
                hot_tail: Some(context.hot_tail.clone()),
                wal_shards: Some(context.wal_shards.clone()),
                sequencer: context.diskless.then(|| {
                    std::sync::Arc::new(crate::wal::ControllerSequencer::new(
                        context.controller.clone(),
                    )) as std::sync::Arc<dyn crate::wal::OffsetSequencer>
                }),
            })
        {
            tracing::error!(topic, partition = *index, error = %error,
                "CreatePartitions: materialize after quorum commit failed");
            continue;
        }
        let Some(partition) = context
            .partitions
            .get(topic, crabka_ids::PartitionIndex(*index))
        else {
            continue;
        };
        let leader = replicas[0];
        partition.install_leader_change(leader.0, 0).await;
        if is_local_leader(leader, context.node_id) {
            partition.install_isr(replicas, replicas, leader).await;
        }
    }
}

fn partition_mutation_count(
    request: &CreatePartitionsRequest,
    image: &crabka_metadata::MetadataImage,
) -> u64 {
    request
        .topics
        .iter()
        .map(|topic| {
            let current =
                i32::try_from(image.partitions_of(&topic.name).count()).unwrap_or(i32::MAX);
            u64::try_from((i64::from(topic.count) - i64::from(current)).max(0))
                .expect("mutation count is non-negative")
        })
        .sum()
}

fn denied_topics(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &crabka_metadata::MetadataImage,
    principal: &crabka_security::Principal,
    peer: &std::net::SocketAddr,
    request: &CreatePartitionsRequest,
) -> std::collections::HashSet<String> {
    authorize_topics(
        authorizer,
        image,
        principal,
        peer,
        AclOperation::Alter,
        request.topics.iter().map(|topic| topic.name.as_str()),
    )
    .into_iter()
    .filter(|(_, result)| *result == AuthorizationResult::Deny)
    .map(|(name, _)| name.to_string())
    .collect()
}

async fn finish_response(
    delay: Time,
    results: Vec<CreatePartitionsTopicResult>,
    version: i16,
) -> Result<Bytes, BrokerError> {
    let resp = create_partitions_response(results, crate::quota::throttle_time_ms(delay));
    if delay > <Time as TimeExt>::ZERO {
        tokio::time::sleep(delay.to_std()).await;
    }
    encode_response(&resp, version)
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::{assert, check};
    use crabka_metadata::TopicRecord;
    use crabka_protocol::owned::create_partitions_request::{
        CreatePartitionsAssignment, CreatePartitionsTopic,
    };
    use crabka_security::Principal;

    use crate::{
        broker::{Broker, BrokerHandle},
        test_support::{DenyAll, peer, principal},
    };

    const VERSION: i16 = 3;

    use super::*;

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

    crate::test_support::wire_helpers!(
        CreatePartitionsRequest,
        CreatePartitionsResponse,
        version = VERSION,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

    async fn seed_topic(handle: &BrokerHandle, name: &str, partitions: i32, rf: i16) {
        let replicas = vec![NodeId(handle.node_id())];
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
                leader: NodeId(handle.node_id()),
                replicas: replicas.clone(),
                isr: replicas.clone(),
                leader_epoch: crabka_metadata::LeaderEpoch(0),
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
        let brokers: Vec<NodeId> = vec![
            crabka_audit::NodeId(0),
            crabka_audit::NodeId(1),
            crabka_audit::NodeId(2),
        ];
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
        let brokers: Vec<NodeId> = vec![
            crabka_audit::NodeId(0),
            crabka_audit::NodeId(1),
            crabka_audit::NodeId(2),
        ];
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
        let brokers: Vec<NodeId> = vec![crabka_audit::NodeId(0), crabka_audit::NodeId(1)];
        let err = resolve_new_partition_assignments(None, &brokers, 0, 1, 3)
            .expect_err("rf=3 against 2 brokers must fail");
        assert!(err.0 == codes::INVALID_REPLICATION_FACTOR);
    }

    #[test]
    fn honored_assignments_pass_through_verbatim() {
        let brokers: Vec<NodeId> = vec![
            crabka_audit::NodeId(0),
            crabka_audit::NodeId(1),
            crabka_audit::NodeId(2),
            crabka_audit::NodeId(3),
        ];
        let provided = vec![assn(&[3, 1]), assn(&[2, 0]), assn(&[1, 3])];
        let out = resolve_new_partition_assignments(Some(&provided), &brokers, 0, 3, 2)
            .expect("explicit assignments should pass validation");
        assert!(
            out == vec![
                vec![NodeId(3), NodeId(1)],
                vec![NodeId(2), NodeId(0)],
                vec![NodeId(1), NodeId(3)],
            ]
        );
    }

    #[test]
    fn explicit_length_mismatch_returns_invalid_replica_assignment() {
        let brokers: Vec<NodeId> = vec![
            crabka_audit::NodeId(0),
            crabka_audit::NodeId(1),
            crabka_audit::NodeId(2),
        ];
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
        let brokers: Vec<NodeId> = vec![
            crabka_audit::NodeId(0),
            crabka_audit::NodeId(1),
            crabka_audit::NodeId(2),
        ];
        let provided = vec![assn(&[0, 1, 2])]; // 3 replicas, but rf=2
        let err = resolve_new_partition_assignments(Some(&provided), &brokers, 0, 1, 2)
            .expect_err("rf mismatch must fail");
        assert!(err.0 == codes::INVALID_REPLICA_ASSIGNMENT);
        assert!(err.1.contains("does not match replication_factor=2"));
    }

    #[test]
    fn explicit_duplicate_broker_in_assignment_returns_invalid_replica_assignment() {
        let brokers: Vec<NodeId> = vec![
            crabka_audit::NodeId(0),
            crabka_audit::NodeId(1),
            crabka_audit::NodeId(2),
        ];
        let provided = vec![assn(&[1, 1])]; // duplicate
        let err = resolve_new_partition_assignments(Some(&provided), &brokers, 0, 1, 2)
            .expect_err("duplicate broker must fail");
        assert!(err.0 == codes::INVALID_REPLICA_ASSIGNMENT);
        assert!(err.1.contains("duplicate broker id 1"));
    }

    #[test]
    fn explicit_unknown_broker_returns_invalid_replica_assignment() {
        let brokers: Vec<NodeId> = vec![
            crabka_audit::NodeId(0),
            crabka_audit::NodeId(1),
            crabka_audit::NodeId(2),
        ];
        let provided = vec![assn(&[0, 9])]; // 9 unknown
        let err = resolve_new_partition_assignments(Some(&provided), &brokers, 0, 1, 2)
            .expect_err("unknown broker must fail");
        assert!(err.0 == codes::INVALID_REPLICA_ASSIGNMENT);
        assert!(err.1.contains("unknown broker id 9"));
    }

    #[test]
    fn explicit_negative_broker_id_returns_invalid_replica_assignment() {
        let brokers: Vec<NodeId> = vec![
            crabka_audit::NodeId(0),
            crabka_audit::NodeId(1),
            crabka_audit::NodeId(2),
        ];
        let provided = vec![assn(&[0, -1])];
        let err = resolve_new_partition_assignments(Some(&provided), &brokers, 0, 1, 2)
            .expect_err("negative broker id must fail");
        assert!(err.0 == codes::INVALID_REPLICA_ASSIGNMENT);
        assert!(err.1.contains("negative broker id -1"));
    }

    #[test]
    fn empty_assignments_some_with_new_partitions_fails() {
        let brokers: Vec<NodeId> = vec![crabka_audit::NodeId(0), crabka_audit::NodeId(1)];
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
        check!(should_materialize_locally(
            &[NodeId(1), NodeId(2)],
            NodeId(1)
        ));
        check!(should_materialize_locally(
            &[NodeId(1), NodeId(2)],
            NodeId(2)
        ));
        check!(!should_materialize_locally(
            &[NodeId(1), NodeId(2)],
            NodeId(3)
        ));
        check!(is_local_leader(NodeId(1), NodeId(1)));
        check!(!is_local_leader(NodeId(2), NodeId(1)));
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
    async fn strict_create_partitions_rejects_after_quota_exhaustion() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_topic(&broker_handle, "metered", 2, 1).await;
        seed_controller_quota(&broker_handle, 2.0).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let req = request(vec![topic_req("metered", 5, None)], false);

        let resp = drive(&broker, &req, &p, &peer).await;

        let expected = CreatePartitionsResponse {
            throttle_time_ms: 0,
            results: vec![CreatePartitionsTopicResult {
                name: "metered".into(),
                error_code: codes::NONE,
                error_message: None,
                unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: crabka_protocol::UnknownTaggedFields::default(),
        };
        assert!(resp == expected);

        let rejected = drive(
            &broker,
            &request(vec![topic_req("metered", 6, None)], false),
            &p,
            &peer,
        )
        .await;
        let expected = CreatePartitionsResponse {
            throttle_time_ms: rejected.throttle_time_ms,
            results: vec![CreatePartitionsTopicResult {
                name: "metered".into(),
                error_code: codes::THROTTLING_QUOTA_EXCEEDED,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(rejected == expected);
        check!(rejected.throttle_time_ms > 0 && rejected.throttle_time_ms <= 500);
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
