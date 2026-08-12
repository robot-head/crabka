//! `AlterPartitionReassignments` (`api_key` 45, KIP-455).
//!
//! The wire handler lives here too. The pure-logic `process_one_partition`
//! helper turns one alter row into a `PartitionRecord` that is ready to
//! submit, or into a wire error code.

use crabka_metadata::{MetadataImage, PartitionRecord};
use crabka_raft::NodeId;

use crate::codes::{
    ELIGIBLE_LEADERS_NOT_AVAILABLE, INVALID_REPLICA_ASSIGNMENT, NO_REASSIGNMENT_IN_PROGRESS,
    UNKNOWN_TOPIC_OR_PARTITION,
};

/// Per-row rejection: a Kafka wire error code and a readable message.
type RowError = (i16, String);

/// Process one (topic, partition, `target_opt`) row from an
/// `AlterPartitionReassignments` request.
///
/// The return values are:
///   - `Ok(Some(PartitionRecord))`: submit this intermediate record
///   - `Ok(None)`: do nothing, because the row is already at target or the
///     alter is empty
///   - `Err((wire_code, message))`: reject this row
pub(crate) fn process_one_partition(
    image: &MetadataImage,
    topic: &str,
    partition: i32,
    target: Option<&[i32]>,
    allow_rf_change: bool,
) -> Result<Option<PartitionRecord>, RowError> {
    let pr = image
        .partition(topic, partition)
        .ok_or((UNKNOWN_TOPIC_OR_PARTITION, "unknown partition".into()))?;

    match target {
        None => cancel_path(pr),
        Some(target_slice) => {
            validate_target(target_slice, image, allow_rf_change, pr)?;
            Ok(start_path(pr, target_slice))
        }
    }
}

fn validate_target(
    target: &[i32],
    image: &MetadataImage,
    allow_rf_change: bool,
    pr: &PartitionRecord,
) -> Result<(), RowError> {
    if target.is_empty() {
        return Err((INVALID_REPLICA_ASSIGNMENT, "empty target".into()));
    }
    // Duplicates.
    let mut seen: std::collections::HashSet<i32> = std::collections::HashSet::new();
    for &n in target {
        if !seen.insert(n) {
            return Err((INVALID_REPLICA_ASSIGNMENT, format!("duplicate replica {n}")));
        }
    }
    // Every node id must be a registered broker.
    for &n in target {
        let Ok(node_id) = u64::try_from(n) else {
            return Err((INVALID_REPLICA_ASSIGNMENT, format!("negative broker {n}")));
        };
        if image.broker(NodeId(node_id)).is_none() {
            return Err((INVALID_REPLICA_ASSIGNMENT, format!("unknown broker {n}")));
        }
    }
    // RF-change check.
    if !allow_rf_change {
        let current_target_len = pr
            .replicas
            .iter()
            .filter(|n| !pr.removing_replicas.contains(n))
            .count();
        if target.len() != current_target_len {
            return Err((
                INVALID_REPLICA_ASSIGNMENT,
                format!(
                    "rf change disallowed: target len {} != current target len {}",
                    target.len(),
                    current_target_len,
                ),
            ));
        }
    }
    Ok(())
}

fn cancel_path(pr: &PartitionRecord) -> Result<Option<PartitionRecord>, RowError> {
    if pr.adding_replicas.is_empty() && pr.removing_replicas.is_empty() {
        return Err((NO_REASSIGNMENT_IN_PROGRESS, "nothing to cancel".into()));
    }
    let reverted_replicas: Vec<NodeId> = pr
        .replicas
        .iter()
        .filter(|n| !pr.adding_replicas.contains(n))
        .copied()
        .collect();
    let reverted_isr: Vec<NodeId> = pr
        .isr
        .iter()
        .filter(|n| !pr.adding_replicas.contains(n))
        .copied()
        .collect();
    let (leader, epoch_bump) = if pr.adding_replicas.contains(&pr.leader) {
        // Leader was an adding replica; revert leadership.
        match reverted_replicas.iter().find(|n| reverted_isr.contains(n)) {
            Some(&n) => (n, 1),
            None => {
                return Err((
                    ELIGIBLE_LEADERS_NOT_AVAILABLE,
                    "no eligible leader after cancel".into(),
                ));
            }
        }
    } else {
        (pr.leader, 0)
    };
    let new_directories =
        crate::reassignment::remap_directories(&pr.replicas, &pr.directories, &reverted_replicas);
    Ok(Some(PartitionRecord {
        topic: pr.topic.clone(),
        partition: pr.partition,
        leader,
        replicas: reverted_replicas,
        isr: reverted_isr,
        leader_epoch: crabka_metadata::LeaderEpoch(pr.leader_epoch.0 + epoch_bump),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: new_directories,
        partition_epoch: pr.partition_epoch + 1,
    }))
}

fn start_path(pr: &PartitionRecord, target: &[i32]) -> Option<PartitionRecord> {
    let target_set: Vec<NodeId> = target
        .iter()
        .map(|&id| NodeId(u64::try_from(id).expect("target validated as non-negative")))
        .collect();
    let current_target: Vec<NodeId> = pr
        .replicas
        .iter()
        .filter(|n| !pr.removing_replicas.contains(n))
        .copied()
        .collect();
    let old: Vec<NodeId> = current_target
        .iter()
        .filter(|n| !target_set.contains(n))
        .copied()
        .collect();
    let new: Vec<NodeId> = target_set
        .iter()
        .filter(|n| !current_target.contains(n))
        .copied()
        .collect();
    if old.is_empty() && new.is_empty() {
        return None; // already at target — no-op
    }
    // replicas = current_target ∪ target (current_target first, then new).
    let mut new_replicas = current_target;
    for n in &new {
        new_replicas.push(*n);
    }
    let new_directories =
        crate::reassignment::remap_directories(&pr.replicas, &pr.directories, &new_replicas);
    Some(PartitionRecord {
        topic: pr.topic.clone(),
        partition: pr.partition,
        leader: pr.leader,
        replicas: new_replicas,
        isr: pr.isr.clone(),
        leader_epoch: pr.leader_epoch,
        adding_replicas: new,
        removing_replicas: old,
        directories: new_directories,
        partition_epoch: pr.partition_epoch + 1,
    })
}

use std::collections::HashMap;

use bytes::Bytes;
use crabka_metadata::ResourceType;
use crabka_protocol::{
    Encode,
    owned::{
        alter_partition_reassignments_request::AlterPartitionReassignmentsRequest,
        alter_partition_reassignments_response::{
            AlterPartitionReassignmentsResponse, ReassignablePartitionResponse,
            ReassignableTopicResponse,
        },
    },
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes::{CLUSTER_AUTHORIZATION_FAILED, COORDINATOR_NOT_AVAILABLE},
};

#[tracing::instrument(
    name = "handle_alter_partition_reassignments",
    level = "info",
    skip_all,
    fields(api = "AlterPartitionReassignments"),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: AlterPartitionReassignmentsRequest,
    ctx: &crate::handlers::RequestContext<'_>,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let image = broker.controller.current_image();
    // Whole-request Cluster Alter authorize.
    let allow = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Cluster,
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            operation: crabka_metadata::AclOperation::Alter,
        },
    );
    if matches!(allow, AuthorizationResult::Deny) {
        return encode_whole_request_error(
            &req,
            CLUSTER_AUTHORIZATION_FAILED,
            "alter-reassignment denied",
            api_version,
        );
    }

    let mut by_topic: HashMap<String, Vec<ReassignablePartitionResponse>> = HashMap::new();
    let mut to_submit: Vec<crabka_metadata::MetadataRecord> = Vec::new();
    for topic in &req.topics {
        let mut rows = Vec::with_capacity(topic.partitions.len());
        for p in &topic.partitions {
            let target_slice: Option<&[i32]> = p.replicas.as_deref();
            match process_one_partition(
                &image,
                &topic.name,
                p.partition_index,
                target_slice,
                req.allow_replication_factor_change,
            ) {
                Ok(Some(record)) => {
                    to_submit.push(crabka_metadata::MetadataRecord::V1Partition(record));
                    rows.push(ok_row(p.partition_index));
                }
                Ok(None) => rows.push(ok_row(p.partition_index)),
                Err((code, msg)) => rows.push(err_row(p.partition_index, code, msg)),
            }
        }
        by_topic.insert(topic.name.clone(), rows);
    }

    if !to_submit.is_empty()
        && let Err(e) = broker.controller.submit_change(to_submit).await
    {
        tracing::warn!(error = %e, "alter-reassignment submit failed");
        mark_submit_failed(&mut by_topic, &format!("submit failed: {e}"));
    }

    let responses: Vec<ReassignableTopicResponse> = by_topic
        .into_iter()
        .map(|(name, partitions)| ReassignableTopicResponse {
            name,
            partitions,
            ..Default::default()
        })
        .collect();
    let resp = AlterPartitionReassignmentsResponse {
        allow_replication_factor_change: req.allow_replication_factor_change,
        responses,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

fn ok_row(partition_index: i32) -> ReassignablePartitionResponse {
    ReassignablePartitionResponse {
        partition_index,
        ..Default::default()
    }
}

fn err_row(partition_index: i32, code: i16, msg: String) -> ReassignablePartitionResponse {
    ReassignablePartitionResponse {
        partition_index,
        error_code: code,
        error_message: Some(msg),
        ..Default::default()
    }
}

fn mark_submit_failed(
    by_topic: &mut HashMap<String, Vec<ReassignablePartitionResponse>>,
    msg: &str,
) {
    for rows in by_topic.values_mut() {
        for r in rows.iter_mut() {
            if r.error_code == 0 {
                r.error_code = COORDINATOR_NOT_AVAILABLE;
                r.error_message = Some(msg.to_string());
            }
        }
    }
}

fn encode_whole_request_error(
    req: &AlterPartitionReassignmentsRequest,
    code: i16,
    msg: &str,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let responses: Vec<ReassignableTopicResponse> = req
        .topics
        .iter()
        .map(|t| ReassignableTopicResponse {
            name: t.name.clone(),
            partitions: t
                .partitions
                .iter()
                .map(|p| err_row(p.partition_index, code, msg.into()))
                .collect(),
            ..Default::default()
        })
        .collect();
    let resp = AlterPartitionReassignmentsResponse {
        allow_replication_factor_change: req.allow_replication_factor_change,
        responses,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    crate::handlers::encode_response_with_context(
        resp,
        api_version,
        "encode AlterPartitionReassignments",
    )
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc, time::Duration};

    use assert2::assert;
    use crabka_metadata::{
        BrokerRegistrationRecord, LeaderEpoch, MetadataRecord, PartitionRecord, TopicRecord,
    };
    use crabka_protocol::{
        UnknownTaggedFields,
        owned::alter_partition_reassignments_request::{
            AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
        },
    };
    use crabka_security::{AuthMethod, Principal};
    use uuid::Uuid;

    use super::*;
    use crate::test_support::DenyAll;

    fn img_with(
        replicas: &[u64],
        isr: &[u64],
        adding: &[u64],
        removing: &[u64],
        leader: u64,
    ) -> MetadataImage {
        img_with_epoch(replicas, isr, adding, removing, leader, 0)
    }

    fn img_with_epoch(
        replicas: &[u64],
        isr: &[u64],
        adding: &[u64],
        removing: &[u64],
        leader: u64,
        partition_epoch: i32,
    ) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        // Register brokers 1..=6 so validate_target accepts target lists.
        for n in 1u64..=6 {
            img.apply(&MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    node_id: NodeId(n),
                    broker_epoch: 0,
                    incarnation_id: uuid::Uuid::nil(),
                    host: "localhost".into(),
                    port: 9092,
                    rack: None,
                    log_dirs: vec![],
                    endpoints: vec![],
                    features: std::collections::BTreeMap::new(),
                },
            ));
        }
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "foo".into(),
            topic_id: Uuid::nil(),
            partitions: 1,
            replication_factor: i16::try_from(replicas.len()).expect("replication factor fits i16"),
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(leader),
            replicas: replicas.iter().copied().map(NodeId).collect(),
            isr: isr.iter().copied().map(NodeId).collect(),
            leader_epoch: crabka_metadata::LeaderEpoch(5),
            adding_replicas: adding.iter().copied().map(NodeId).collect(),
            removing_replicas: removing.iter().copied().map(NodeId).collect(),
            directories: vec![],
            partition_epoch,
        }));
        img
    }

    fn request(
        allow_replication_factor_change: bool,
        topic: &str,
        partition_index: i32,
        replicas: Option<Vec<i32>>,
    ) -> AlterPartitionReassignmentsRequest {
        AlterPartitionReassignmentsRequest {
            timeout_ms: 30_000,
            allow_replication_factor_change,
            topics: vec![ReassignableTopic {
                name: topic.into(),
                partitions: vec![ReassignablePartition {
                    partition_index,
                    replicas,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn validate_target_rejects_negative_broker_id() {
        let image = img_with(&[1], &[1], &[], &[], 1);
        let partition = image.partition("foo", 0).expect("seeded partition");
        let error = validate_target(&[-1], &image, true, partition).expect_err("negative broker");
        assert!(error.0 == INVALID_REPLICA_ASSIGNMENT);
        assert!(error.1.contains("negative broker"));
    }

    crate::test_support::response_helpers!(
        AlterPartitionReassignmentsResponse,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer as start_broker;

    async fn wait_for_leader(broker: &Broker) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if broker
                .controller
                .watch_leader()
                .borrow()
                .is_some_and(|n| n == broker.config.node_id)
            {
                return;
            }
            assert!(
                std::time::Instant::now() <= deadline,
                "broker did not become controller leader"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn seed_reassignable_partition(broker: &Broker) {
        broker
            .controller
            .submit_change(vec![
                MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                    node_id: NodeId(1),
                    broker_epoch: 1,
                    incarnation_id: uuid::Uuid::nil(),
                    host: "localhost".into(),
                    port: 9092,
                    rack: None,
                    log_dirs: vec![],
                    endpoints: vec![],
                    features: std::collections::BTreeMap::new(),
                }),
                MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                    node_id: NodeId(2),
                    broker_epoch: 1,
                    incarnation_id: uuid::Uuid::nil(),
                    host: "localhost".into(),
                    port: 9093,
                    rack: None,
                    log_dirs: vec![],
                    endpoints: vec![],
                    features: std::collections::BTreeMap::new(),
                }),
                MetadataRecord::V1Topic(TopicRecord {
                    name: "orders".into(),
                    topic_id: Uuid::nil(),
                    partitions: 1,
                    replication_factor: 1,
                }),
                MetadataRecord::V1Partition(PartitionRecord {
                    topic: "orders".into(),
                    partition: 7,
                    leader: NodeId(1),
                    replicas: vec![NodeId(1)],
                    isr: vec![NodeId(1)],
                    leader_epoch: LeaderEpoch(3),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![],
                    partition_epoch: 11,
                }),
            ])
            .await
            .expect("seed reassignment metadata");
    }

    #[test]
    fn noop_when_already_at_target() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 2, 3]), true).unwrap();
        assert!(res.is_none());
    }

    #[test]
    fn start_writes_union_replicas() {
        let img = img_with_epoch(&[1, 2, 3], &[1, 2, 3], &[], &[], 1, 11);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 4]), true)
            .expect("ok")
            .expect("Some");
        let expected = PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
            isr: vec![NodeId(1), NodeId(2), NodeId(3)],
            leader_epoch: LeaderEpoch(5), // unchanged on start
            adding_replicas: vec![NodeId(4)],
            removing_replicas: vec![NodeId(2), NodeId(3)],
            directories: vec![Uuid::nil(); 4],
            partition_epoch: 12,
        };
        assert!(res == expected);
    }

    #[test]
    fn row_builders_preserve_non_default_fields() {
        let ok = ok_row(7);
        let expected_ok = ReassignablePartitionResponse {
            partition_index: 7,
            error_code: 0,
            error_message: None,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(ok == expected_ok);

        let err = err_row(8, UNKNOWN_TOPIC_OR_PARTITION, "missing partition".into());
        let expected_err = ReassignablePartitionResponse {
            partition_index: 8,
            error_code: UNKNOWN_TOPIC_OR_PARTITION,
            error_message: Some("missing partition".into()),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(err == expected_err);
    }

    #[test]
    fn encode_whole_request_error_preserves_request_shape() {
        let version = 1;
        let req = request(false, "payments", 8, Some(vec![1, 2]));

        let bytes =
            encode_whole_request_error(&req, CLUSTER_AUTHORIZATION_FAILED, "denied", version)
                .expect("encode whole request error");
        let resp = decode_response(&bytes, version);

        let expected = AlterPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            allow_replication_factor_change: false,
            error_code: 0,
            error_message: None,
            responses: vec![ReassignableTopicResponse {
                name: "payments".into(),
                partitions: vec![ReassignablePartitionResponse {
                    partition_index: 8,
                    error_code: CLUSTER_AUTHORIZATION_FAILED,
                    error_message: Some("denied".into()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }

    #[test]
    fn mark_submit_failed_only_rewrites_successful_rows() {
        let mut by_topic = std::collections::HashMap::from([(
            "orders".to_string(),
            vec![
                ok_row(7),
                err_row(8, UNKNOWN_TOPIC_OR_PARTITION, "unknown partition".into()),
            ],
        )]);

        mark_submit_failed(&mut by_topic, "submit failed: not controller");
        let rows = by_topic.get("orders").expect("topic rows");

        let expected = vec![
            ReassignablePartitionResponse {
                partition_index: 7,
                error_code: COORDINATOR_NOT_AVAILABLE,
                error_message: Some("submit failed: not controller".into()),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            ReassignablePartitionResponse {
                partition_index: 8,
                error_code: UNKNOWN_TOPIC_OR_PARTITION,
                error_message: Some("unknown partition".into()),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
        ];
        assert!(*rows == expected);
    }

    #[tokio::test]
    async fn handle_preserves_unknown_partition_response_shape() {
        let version = 1;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);

        let bytes = handle(
            &broker,
            request(false, "payments", 8, Some(vec![1, 2])),
            &ctx,
            version,
        )
        .await
        .expect("handle");
        let resp = decode_response(&bytes, version);

        let expected = AlterPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            allow_replication_factor_change: false,
            error_code: 0,
            error_message: None,
            responses: vec![ReassignableTopicResponse {
                name: "payments".into(),
                partitions: vec![ReassignablePartitionResponse {
                    partition_index: 8,
                    error_code: UNKNOWN_TOPIC_OR_PARTITION,
                    error_message: Some("unknown partition".into()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_denies_cluster_alter_for_each_requested_partition() {
        let version = 1;
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);

        let bytes = handle(
            &broker,
            request(false, "payments", 8, Some(vec![1, 2])),
            &ctx,
            version,
        )
        .await
        .expect("handle");
        let resp = decode_response(&bytes, version);

        let expected = AlterPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            allow_replication_factor_change: false,
            error_code: 0,
            error_message: None,
            responses: vec![ReassignableTopicResponse {
                name: "payments".into(),
                partitions: vec![ReassignablePartitionResponse {
                    partition_index: 8,
                    error_code: CLUSTER_AUTHORIZATION_FAILED,
                    error_message: Some("alter-reassignment denied".into()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_submits_successful_reassignment_records() {
        let version = 1;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_leader(&broker).await;
        seed_reassignable_partition(&broker).await;
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);

        let bytes = handle(
            &broker,
            request(true, "orders", 7, Some(vec![1, 2])),
            &ctx,
            version,
        )
        .await
        .expect("handle");
        let resp = decode_response(&bytes, version);

        let expected = AlterPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            allow_replication_factor_change: true,
            error_code: 0,
            error_message: None,
            responses: vec![ReassignableTopicResponse {
                name: "orders".into(),
                partitions: vec![ReassignablePartitionResponse {
                    partition_index: 7,
                    error_code: 0,
                    error_message: None,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);

        let image = broker.controller.current_image();
        let partition = image.partition("orders", 7).expect("partition committed");
        assert!(partition.adding_replicas == vec![NodeId(2)]);
        assert!(partition.partition_epoch == 12);
        broker_handle.shutdown().await;
    }

    #[test]
    fn replaces_existing_in_flight_reassignment() {
        // Currently in flight: replicas=[1,2,3,4], adding=[4], removing=[2,3].
        // current_target = [1,4]. New alter target = [5,6].
        // Expected: replicas=[1,4,5,6], adding=[5,6], removing=[1,4].
        let img = img_with(&[1, 2, 3, 4], &[1, 2, 3], &[4], &[2, 3], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[5, 6]), true)
            .expect("ok")
            .expect("Some");
        let expected = PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1), NodeId(4), NodeId(5), NodeId(6)],
            isr: vec![NodeId(1), NodeId(2), NodeId(3)],
            leader_epoch: LeaderEpoch(5),
            adding_replicas: vec![NodeId(5), NodeId(6)],
            removing_replicas: vec![NodeId(1), NodeId(4)],
            directories: vec![Uuid::nil(); 4],
            partition_epoch: 1,
        };
        assert!(res == expected);
    }

    #[test]
    fn rf_change_rejected_when_disabled() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let err = process_one_partition(&img, "foo", 0, Some(&[1, 2]), false).unwrap_err();
        assert!(err.0 == INVALID_REPLICA_ASSIGNMENT);
    }

    #[test]
    fn rf_change_allowed_when_enabled() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 2]), true)
            .expect("ok")
            .expect("Some");
        assert!(res.removing_replicas == vec![NodeId(3)]);
    }

    #[test]
    fn rf_check_counts_current_target_without_removing_replicas() {
        let img = img_with(&[1, 2, 3, 4], &[1, 3, 4], &[4], &[2], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 3, 4]), false).expect("ok");

        assert!(res.is_none());
    }

    #[test]
    fn cancel_with_leader_in_adding_reverts_leader() {
        // After a successful leader handoff during reassignment, leader=4 (an adding replica).
        // Cancel: leader should revert to whoever in reverted replicas ∩ isr.
        // replicas=[1,2,3,4], adding=[4], removing=[2,3], leader=4, isr=[1,4].
        let img = img_with_epoch(&[1, 2, 3, 4], &[1, 4], &[4], &[2, 3], 4, 11);
        let res = process_one_partition(&img, "foo", 0, None, true)
            .expect("ok")
            .expect("Some");
        let expected = PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(1), // reverted replicas ∩ isr = [1]
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(1)],
            leader_epoch: LeaderEpoch(6), // bumped
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![Uuid::nil(); 3],
            partition_epoch: 12,
        };
        assert!(res == expected);
    }

    #[test]
    fn cancel_with_only_removing_replicas_is_valid() {
        let img = img_with_epoch(&[1, 2, 3], &[1, 2, 3], &[], &[3], 1, 11);
        let res = process_one_partition(&img, "foo", 0, None, true)
            .expect("ok")
            .expect("Some");

        let expected = PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(1), NodeId(2), NodeId(3)],
            leader_epoch: LeaderEpoch(5),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![Uuid::nil(); 3],
            partition_epoch: 12,
        };
        assert!(res == expected);
    }

    #[test]
    fn empty_target_rejected() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let err = process_one_partition(&img, "foo", 0, Some(&[]), true).unwrap_err();
        assert!(err.0 == INVALID_REPLICA_ASSIGNMENT);
    }
}
