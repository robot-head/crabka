//! `CreateTopics` (`api_key=19`). Routes through `Controller::submit_change`
//! so every topic/partition creation goes through the metadata quorum before
//! the partition directories are materialized on disk.

use bytes::Bytes;
use crabka_metadata::{
    AclOperation, MetadataRecord, PartitionRecord, TopicConfigRecord, TopicRecord,
};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::{CreatableTopicResult, CreateTopicsResponse},
    },
    primitives::uuid::Uuid as ProtoUuid,
};
use crabka_raft::RaftError;
use crabka_units::{Time, convert::TimeExt};
use uuid::Uuid;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
    replicator_supervisor::materialize_partition,
};

/// Leader epoch a freshly created partition starts at; the committed
/// `PartitionRecord` and the handler-side leader-cache install must agree.
const INITIAL_LEADER_EPOCH: i32 = 0;

/// Round-robin replica placement.
///
/// Given a sorted broker set `bs = [b0, b1, …, bk-1]` and a partition
/// count `P`, returns a `Vec<Vec<NodeId>>` of length `P`, where each
/// inner vec is `R = replication_factor` long. Partition `p`'s leader
/// is `bs[(p) % k]`; the remaining replicas are `bs[(p + i) % k]` for
/// `i in 1..R`. Caller must guarantee `R <= k` (else returns an empty
/// outer vec and the caller surfaces `INVALID_REPLICATION_FACTOR`).
pub(crate) fn round_robin_replicas(
    sorted_brokers: &[crabka_raft::NodeId],
    num_partitions: i32,
    replication_factor: i16,
) -> Vec<Vec<crabka_raft::NodeId>> {
    let k = sorted_brokers.len();
    let r = usize::try_from(replication_factor).unwrap_or(0);
    if r == 0 || r > k {
        return Vec::new();
    }
    let p_count = usize::try_from(num_partitions).unwrap_or(0);
    (0..p_count)
        .map(|p| {
            (0..r)
                .map(|i| sorted_brokers[(p + i) % k])
                .collect::<Vec<_>>()
        })
        .collect()
}

fn topic_error_result(
    name: String,
    error_code: i16,
    error_message: Option<String>,
) -> CreatableTopicResult {
    CreatableTopicResult {
        name,
        error_code,
        error_message,
        ..Default::default()
    }
}

fn create_topics_response(
    topics: Vec<CreatableTopicResult>,
    throttle_time_ms: i32,
) -> CreateTopicsResponse {
    CreateTopicsResponse {
        topics,
        throttle_time_ms,
        ..Default::default()
    }
}

fn created_topic_resources(results: &[CreatableTopicResult]) -> Vec<crabka_audit::AuditResource> {
    results
        .iter()
        .filter(|t| t.error_code == codes::NONE)
        .map(|t| crabka_audit::AuditResource {
            resource_type: "Topic".to_string(),
            name: t.name.clone(),
        })
        .collect()
}

fn audit_created_topics(
    audit_log: &crabka_audit::AuditLog,
    ctx: &crate::handlers::RequestContext<'_>,
    created: Vec<crabka_audit::AuditResource>,
) {
    if !created.is_empty() {
        crate::handlers::audit_admin(
            audit_log,
            ctx,
            "CreateTopics",
            crabka_audit::AuditOutcome::Success,
            created,
        );
    }
}

fn encode_response<R: Encode>(resp: &R, version: i16) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(resp, version)
}

fn should_materialize_locally(
    replicas: &[crabka_raft::NodeId],
    node_id: crabka_raft::NodeId,
) -> bool {
    replicas.contains(&node_id)
}

fn is_local_leader(leader: crabka_raft::NodeId, node_id: crabka_raft::NodeId) -> bool {
    leader == node_id
}

#[tracing::instrument(
    name = "handle_create_topics",
    level = "info",
    skip_all,
    fields(api = "CreateTopics", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    // ── ACL preamble ────────────────────────────────────────
    // Whole-request Cluster Create gate. On Deny, return
    // CLUSTER_AUTHORIZATION_FAILED on every topic row and short-circuit.
    let mut cursor = req_bytes;
    let req = CreateTopicsRequest::decode(&mut cursor, version)?;
    let image = broker.controller.current_image();
    if cluster_create_denied(broker, &image, ctx) {
        let results = req
            .topics
            .iter()
            .map(|topic| {
                topic_error_result(
                    topic.name.clone(),
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                    Some("create-topics denied".into()),
                )
            })
            .collect();
        return encode_response(&create_topics_response(results, 0), version);
    }

    let controller = broker.controller.clone();
    let node_id = broker.config.node_id;
    let log_dirs = broker.config.all_log_dirs();
    let log_config = broker.config.log_config.clone();
    let log_dir_status = broker.log_dir_status.clone();
    let partitions_map = broker.partitions.clone();
    let producer_state = broker.producer_state.clone();
    let hot_tail = broker.hot_tail.clone();
    let wal_shards = broker.wal_shards.clone();

    // KIP-599: count mutations before running handler logic so that even
    // invalid requests consume quota (bad-faith clients can't escape by
    // sending malformed RPCs). num_partitions == -1 means "use cluster
    // default"; count it as 1 for accounting.
    let mutation_count = mutation_count(&req);

    let mut results: Vec<CreatableTopicResult> = Vec::with_capacity(req.topics.len());

    for topic_req in req.topics {
        let name = topic_req.name.clone();
        let partition_count = topic_req.num_partitions;
        let replication_factor = topic_req.replication_factor;

        // Reject invalid partition counts before attempting placement.
        if partition_count <= 0 {
            results.push(topic_error_result(name, codes::INVALID_PARTITIONS, None));
            continue;
        }

        // Read the current broker set from the controller's image; sort by
        // node_id for determinism.
        //
        // Race-tolerance: on a freshly-started cluster, the self-registration
        // V1BrokerRegistration record may not have made it into the local
        // MetadataImage yet when this handler runs (the controller's apply is
        // mostly synchronous on the leader but observable timing has slipped
        // on slow runners). If `brokers()` is empty, fall back to "this broker
        // is the only known broker" so the single-broker case (which is by
        // far the most common) doesn't silently degrade to
        // INVALID_REPLICATION_FACTOR.
        let mut sorted_brokers: Vec<crabka_raft::NodeId> =
            image.brokers().map(|b| b.node_id).collect();
        if sorted_brokers.is_empty() {
            sorted_brokers.push(node_id);
        }
        sorted_brokers.sort_unstable();

        let assignments =
            round_robin_replicas(&sorted_brokers, partition_count, replication_factor);

        if assignments.is_empty() {
            // RF > broker count. Surface INVALID_REPLICATION_FACTOR per Apache
            // Kafka semantics.
            results.push(topic_error_result(
                name,
                codes::INVALID_REPLICATION_FACTOR,
                None,
            ));
            continue;
        }

        let topic_id = Uuid::new_v4();

        // Build the batch: one TopicRecord + N PartitionRecords.
        let records = topic_records(&topic_req, topic_id, &assignments);
        let topic_config_overrides = topic_req
            .configs
            .iter()
            .filter_map(|config| {
                config
                    .value
                    .as_ref()
                    .map(|value| (config.name.clone(), value.clone()))
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let diskless = crate::broker::diskless_topic_config(Some(&topic_config_overrides));

        let result = controller.submit_change(records).await;

        let error_code = match result {
            Ok(_) => {
                materialize_topic(
                    TopicMaterialization {
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
                        topic_id,
                        hot_tail: &hot_tail,
                        wal_shards: &wal_shards,
                        controller: &controller,
                    },
                    &name,
                    &assignments,
                )
                .await;
                codes::NONE
            }
            Err(RaftError::Metadata(crabka_metadata::MetadataError::TopicExists(_))) => {
                codes::TOPIC_ALREADY_EXISTS
            }
            Err(RaftError::Metadata(crabka_metadata::MetadataError::InvalidRecord(_))) => {
                // E.g., `partitions <= 0` rejected by image::validate.
                codes::INVALID_PARTITIONS
            }
            Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => codes::NOT_CONTROLLER,
            Err(e) => {
                tracing::error!(topic = %name, error = %e, "CreateTopics submit_change failed");
                codes::UNKNOWN_SERVER_ERROR
            }
        };

        // Convert uuid::Uuid → crabka_protocol::primitives::uuid::Uuid.
        let proto_uuid = ProtoUuid(topic_id.into_bytes());

        let mut result = CreatableTopicResult {
            name,
            topic_id: proto_uuid,
            error_code,
            ..Default::default()
        };

        if error_code == codes::NONE {
            result.num_partitions = partition_count;
            result.replication_factor = replication_factor;
            // KIP-525 (v5+): return an empty configs list to satisfy
            // clients that unconditionally call `configs().stream()`.
            result.configs = Some(Vec::new());
        }
        results.push(result);
    }

    finish_response(broker, &image, ctx, results, mutation_count, version).await
}

fn mutation_count(request: &CreateTopicsRequest) -> u64 {
    request
        .topics
        .iter()
        .map(|topic| {
            u64::try_from(topic.num_partitions.max(1)).expect("mutation count is positive")
        })
        .sum()
}

async fn finish_response(
    broker: &Broker,
    image: &crabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    results: Vec<CreatableTopicResult>,
    mutation_count: u64,
    version: i16,
) -> Result<Bytes, BrokerError> {
    audit_created_topics(
        broker.audit_log.as_ref(),
        context,
        created_topic_resources(&results),
    );
    let delay = crate::quota::consume_controller_mutation_quota(
        image,
        &broker.quota_buckets,
        &context.principal.name,
        context.client_id,
        mutation_count,
        broker.config.quota_throttle_max,
    );
    let response = create_topics_response(results, crate::quota::throttle_time_ms(delay));
    if delay > <Time as TimeExt>::ZERO {
        tokio::time::sleep(delay.to_std()).await;
    }
    encode_response(&response, version)
}

fn cluster_create_denied(
    broker: &Broker,
    image: &crabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
) -> bool {
    broker.config.authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal: context.principal,
            host: context.peer,
            resource_type: crabka_metadata::ResourceType::Cluster,
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            operation: AclOperation::Create,
        },
    ) == AuthorizationResult::Deny
}

#[derive(Clone, Copy)]
struct TopicMaterialization<'a> {
    partitions: &'a std::sync::Arc<crate::partition_registry::PartitionRegistry>,
    log_dirs: &'a [std::path::PathBuf],
    log_config: &'a crabka_log::LogConfig,
    log_dir_status: &'a crate::log_dir_status::LogDirRegistry,
    producer_state: &'a std::sync::Arc<crate::producer_state::ProducerState>,
    producer_id_expiration: Time,
    max_produce_group: usize,
    partition_writer_queue_depth: usize,
    diskless_wal_local_replica_count: usize,
    node_id: crabka_raft::NodeId,
    diskless: bool,
    topic_id: uuid::Uuid,
    hot_tail: &'a std::sync::Arc<crate::diskless::hot_tail::HotTailCache>,
    wal_shards: &'a std::sync::Arc<crate::wal::quorum::registry::WalShardRegistry>,
    controller: &'a std::sync::Arc<dyn crate::metadata_source::MetadataSource>,
}

async fn materialize_topic(
    context: TopicMaterialization<'_>,
    topic: &str,
    assignments: &[Vec<crabka_raft::NodeId>],
) {
    for (index, replicas) in assignments.iter().enumerate() {
        if !should_materialize_locally(replicas, context.node_id) {
            continue;
        }
        let index = i32::try_from(index).unwrap_or(0);
        if let Err(error) =
            materialize_partition(crate::replicator_supervisor::MaterializePartitionConfig {
                partitions: context.partitions,
                topic,
                topic_id: Some(context.topic_id),
                partition: index,
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
            tracing::error!(topic, partition = index, error = %error,
                "CreateTopics: materialize after quorum commit failed");
            continue;
        }
        let Some(partition) = context
            .partitions
            .get(topic, crabka_ids::PartitionIndex(index))
        else {
            continue;
        };
        let leader = replicas[0];
        partition
            .install_leader_change(leader.0, INITIAL_LEADER_EPOCH)
            .await;
        if is_local_leader(leader, context.node_id) {
            partition.install_isr(replicas, replicas, leader).await;
        }
    }
}

fn topic_records(
    request: &CreatableTopic,
    topic_id: Uuid,
    assignments: &[Vec<crabka_raft::NodeId>],
) -> Vec<MetadataRecord> {
    let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
        name: request.name.clone(),
        topic_id,
        partitions: request.num_partitions,
        replication_factor: request.replication_factor,
    })];
    records.extend(assignments.iter().enumerate().map(|(index, replicas)| {
        MetadataRecord::V1Partition(PartitionRecord {
            topic: request.name.clone(),
            partition: i32::try_from(index).unwrap_or(0),
            leader: replicas[0],
            replicas: replicas.clone(),
            isr: replicas.clone(),
            leader_epoch: crabka_metadata::LeaderEpoch(INITIAL_LEADER_EPOCH),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        })
    }));
    let overrides = request
        .configs
        .iter()
        .filter_map(|config| {
            config
                .value
                .as_ref()
                .map(|value| (config.name.clone(), value.clone()))
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    if !overrides.is_empty() {
        records.push(MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: request.name.clone(),
            overrides,
        }));
    }
    records
}

#[cfg(test)]
mod replica_assignment_tests {
    use assert2::assert;
    use crabka_raft::NodeId;
    use crabka_units::{Time, convert::TimeExt, secs};

    use super::round_robin_replicas;

    #[test]
    fn three_brokers_three_partitions_rf_three() {
        let bs = vec![NodeId(1), NodeId(2), NodeId(3)];
        let out = round_robin_replicas(&bs, 3, 3);
        // Every broker should lead exactly one partition.
        let leaders: Vec<_> = out.iter().map(|r| r[0]).collect();
        let mut sorted = leaders.clone();
        sorted.sort_unstable();
        assert!(sorted == vec![NodeId(1), NodeId(2), NodeId(3)]);
        // Each partition has all three brokers as replicas.
        for replicas in &out {
            let mut s = replicas.clone();
            s.sort_unstable();
            assert!(s == vec![NodeId(1), NodeId(2), NodeId(3)]);
        }
    }

    #[test]
    fn offset_per_partition_means_distinct_leaders() {
        let bs = vec![NodeId(1), NodeId(2), NodeId(3)];
        let out = round_robin_replicas(&bs, 3, 1);
        assert!(out == vec![vec![NodeId(1)], vec![NodeId(2)], vec![NodeId(3)]]);
    }

    #[test]
    fn rf_too_high_returns_empty() {
        let bs = vec![NodeId(1), NodeId(2), NodeId(3)];
        let out = round_robin_replicas(&bs, 1, 5);
        assert!(out.is_empty());
    }

    #[test]
    fn rf_one_single_broker_preserves_replica_shape() {
        let bs = vec![NodeId(1)];
        let out = round_robin_replicas(&bs, 2, 1);
        assert!(out == vec![vec![NodeId(1)], vec![NodeId(1)]]);
    }

    #[test]
    fn consume_controller_mutation_quota_tuple_match_overage_throttles() {
        use crabka_metadata::{ClientQuotaRecord, MetadataImage, MetadataRecord, QuotaEntity};
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![
                QuotaEntity {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                },
                QuotaEntity {
                    entity_type: "client-id".into(),
                    entity_name: Some("app-x".into()),
                },
            ],
            config_key: "controller_mutation_rate".into(),
            config_value: Some(1.0),
        }));
        let cases = [
            // Exact (user, client-id) tuple match should throttle on overage.
            ("app-x", true),
            // Non-matching client_id should not throttle.
            ("other", false),
        ];
        for (client_id, want_throttle) in cases {
            let buckets = crate::quota::QuotaBuckets::new();
            let delay = crate::quota::consume_controller_mutation_quota(
                &img,
                &buckets,
                "alice",
                client_id,
                10,
                secs(1),
            );
            assert!(
                (delay > <Time as TimeExt>::ZERO) == want_throttle,
                "client_id {client_id}, delay {delay:?}"
            );
        }
    }
}

#[cfg(test)]
mod handler_tests {
    use std::{net::SocketAddr, sync::Arc, time::Duration};

    use assert2::{assert, check};
    use crabka_protocol::{
        UnknownTaggedFields,
        owned::create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
    };
    use crabka_raft::NodeId;
    use crabka_security::Principal;

    use super::*;
    use crate::{
        broker::BrokerHandle,
        test_support::{DenyAll, peer, principal},
    };

    const VERSION: i16 = 7;

    fn topic(name: &str, partitions: i32, rf: i16) -> CreatableTopic {
        CreatableTopic {
            name: name.into(),
            num_partitions: partitions,
            replication_factor: rf,
            ..Default::default()
        }
    }

    fn topic_with_config(name: &str) -> CreatableTopic {
        CreatableTopic {
            configs: vec![CreatableTopicConfig {
                name: "retention.ms".into(),
                value: Some("60000".into()),
                ..Default::default()
            }],
            ..topic(name, 2, 1)
        }
    }

    fn request(topics: Vec<CreatableTopic>) -> CreateTopicsRequest {
        CreateTopicsRequest {
            topics,
            timeout_ms: 5_000,
            ..Default::default()
        }
    }

    crate::test_support::wire_helpers!(
        CreateTopicsRequest,
        CreateTopicsResponse,
        version = VERSION,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

    async fn drive(
        broker: &Broker,
        req: &CreateTopicsRequest,
        principal: &Principal,
        peer: &SocketAddr,
    ) -> CreateTopicsResponse {
        let ctx = test_context(principal, peer);
        let req_bytes = encode_request(req);
        let bytes = handle(broker, VERSION, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        decode_response(&bytes)
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

    #[test]
    fn created_topic_resources_include_only_successful_topics() {
        let results = vec![
            CreatableTopicResult {
                name: "ok".into(),
                error_code: codes::NONE,
                ..Default::default()
            },
            CreatableTopicResult {
                name: "bad".into(),
                error_code: codes::TOPIC_ALREADY_EXISTS,
                ..Default::default()
            },
        ];

        let resources = created_topic_resources(&results);

        let expected = vec![crabka_audit::AuditResource {
            resource_type: "Topic".into(),
            name: "ok".into(),
        }];
        assert!(resources == expected);
    }

    #[test]
    fn audit_created_topics_skips_empty_and_emits_non_empty_admin_event() {
        let (log, mut rx) = crabka_audit::AuditLog::new(8);
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        audit_created_topics(log.as_ref(), &ctx, Vec::new());
        assert!(
            rx.try_recv().is_err(),
            "empty audit resource list is a no-op"
        );

        audit_created_topics(
            log.as_ref(),
            &ctx,
            vec![crabka_audit::AuditResource {
                resource_type: "Topic".into(),
                name: "orders".into(),
            }],
        );

        let event = rx.try_recv().expect("admin audit event");
        let crabka_audit::AuditEvent::AdminOperation {
            outcome,
            principal,
            operation,
            resources,
            ..
        } = event
        else {
            panic!("expected AdminOperation");
        };
        check!(outcome == crabka_audit::AuditOutcome::Success);
        check!(principal.name.as_str() == "admin");
        check!(operation.as_str() == "CreateTopics");
        let expected_resources = vec![crabka_audit::AuditResource {
            resource_type: "Topic".into(),
            name: "orders".into(),
        }];
        assert!(resources == expected_resources);
    }

    #[test]
    fn local_materialization_predicates_track_replica_membership_and_leader() {
        let materialize_cases: [(&[crabka_raft::NodeId], crabka_raft::NodeId, bool); 3] = [
            (&[NodeId(1), NodeId(2)], NodeId(1), true),
            (&[NodeId(1), NodeId(2)], NodeId(2), true),
            (&[NodeId(1), NodeId(2)], NodeId(3), false),
        ];
        for (replicas, node_id, want) in materialize_cases {
            assert!(
                should_materialize_locally(replicas, node_id) == want,
                "replicas {replicas:?}, node {node_id}"
            );
        }

        let leader_cases: [(crabka_raft::NodeId, crabka_raft::NodeId, bool); 2] =
            [(NodeId(1), NodeId(1), true), (NodeId(2), NodeId(1), false)];
        for (leader, node_id, want) in leader_cases {
            assert!(
                is_local_leader(leader, node_id) == want,
                "leader {leader}, node {node_id}"
            );
        }
    }

    #[tokio::test]
    async fn handle_denies_cluster_create_for_each_topic() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let req = request(vec![topic("orders", 1, 1), topic("payments", 1, 1)]);

        let resp = drive(&broker, &req, &p, &peer).await;

        let expected = CreateTopicsResponse {
            throttle_time_ms: 0,
            topics: vec![
                CreatableTopicResult {
                    name: "orders".into(),
                    topic_id: ProtoUuid([0; 16]),
                    error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                    error_message: Some("create-topics denied".into()),
                    num_partitions: -1,
                    replication_factor: -1,
                    configs: None,
                    topic_config_error_code: 0,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
                CreatableTopicResult {
                    name: "payments".into(),
                    topic_id: ProtoUuid([0; 16]),
                    error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                    error_message: Some("create-topics denied".into()),
                    num_partitions: -1,
                    replication_factor: -1,
                    configs: None,
                    topic_config_error_code: 0,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            ],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        assert!(
            broker_handle
                .controller_image_for_test()
                .topic("orders")
                .is_none()
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_reports_invalid_partition_count_and_replication_factor() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let req = request(vec![topic("bad-count", 0, 1), topic("bad-rf", 1, 2)]);

        let resp = drive(&broker, &req, &p, &peer).await;

        let expected = CreateTopicsResponse {
            throttle_time_ms: 0,
            topics: vec![
                CreatableTopicResult {
                    name: "bad-count".into(),
                    topic_id: ProtoUuid([0; 16]),
                    error_code: codes::INVALID_PARTITIONS,
                    error_message: None,
                    num_partitions: -1,
                    replication_factor: -1,
                    configs: None,
                    topic_config_error_code: 0,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
                CreatableTopicResult {
                    name: "bad-rf".into(),
                    topic_id: ProtoUuid([0; 16]),
                    error_code: codes::INVALID_REPLICATION_FACTOR,
                    error_message: None,
                    num_partitions: -1,
                    replication_factor: -1,
                    configs: None,
                    topic_config_error_code: 0,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            ],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        for name in ["bad-count", "bad-rf"] {
            let image = broker_handle.controller_image_for_test();
            assert!(image.topic(name).is_none(), "topic {name} not committed");
        }
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_success_persists_topic_config_and_success_fields() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let req = request(vec![topic_with_config("configured")]);

        let resp = drive(&broker, &req, &p, &peer).await;

        assert!(resp.topics.len() == 1);
        assert!(resp.topics[0].topic_id != ProtoUuid([0; 16]));
        let expected = CreateTopicsResponse {
            throttle_time_ms: 0,
            topics: vec![CreatableTopicResult {
                name: "configured".into(),
                // Randomly generated per create; copied from the actual
                // response (the != nil assert above pins non-default).
                topic_id: resp.topics[0].topic_id,
                error_code: codes::NONE,
                error_message: None,
                num_partitions: 2,
                replication_factor: 1,
                configs: Some(Vec::new()),
                topic_config_error_code: 0,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);

        let image = broker_handle.controller_image_for_test();
        let topic = image.topic("configured").expect("topic in image");
        assert!(topic.partitions == 2);
        let configs = image.topic_config("configured").expect("topic configs");
        assert!(configs.get("retention.ms").map(String::as_str) == Some("60000"));
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn duplicate_topic_reports_error_without_success_fields() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let req = request(vec![topic("dupe", 1, 1)]);
        let first = drive(&broker, &req, &p, &peer).await;
        assert!(first.topics[0].error_code == codes::NONE);

        let second = drive(&broker, &req, &p, &peer).await;

        assert!(second.topics.len() == 1);
        let expected = CreateTopicsResponse {
            throttle_time_ms: 0,
            topics: vec![CreatableTopicResult {
                name: "dupe".into(),
                // A fresh topic_id is generated before submit_change even on
                // the error path; copied from the actual response.
                topic_id: second.topics[0].topic_id,
                error_code: codes::TOPIC_ALREADY_EXISTS,
                error_message: None,
                num_partitions: -1,
                replication_factor: -1,
                configs: None,
                topic_config_error_code: 0,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(second == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn throttled_create_topics_reports_delay_and_waits() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_controller_quota(&broker_handle, 2.0).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let req = request(vec![topic("throttled", 5, 1)]);

        let started = tokio::time::Instant::now();
        let resp = drive(&broker, &req, &p, &peer).await;
        let elapsed = started.elapsed();

        assert!(resp.topics.len() == 1);
        let expected = CreateTopicsResponse {
            throttle_time_ms: 1_000,
            topics: vec![CreatableTopicResult {
                name: "throttled".into(),
                // Randomly generated per create; copied from the actual response.
                topic_id: resp.topics[0].topic_id,
                error_code: codes::NONE,
                error_message: None,
                num_partitions: 5,
                replication_factor: 1,
                configs: Some(Vec::new()),
                topic_config_error_code: 0,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        assert!(
            elapsed >= Duration::from_millis(900),
            "handler must wait for the advertised throttle, elapsed={elapsed:?}"
        );
        broker_handle.shutdown().await;
    }
}
