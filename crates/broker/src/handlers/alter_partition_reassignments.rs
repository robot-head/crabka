//! `AlterPartitionReassignments` (`api_key` 45, KIP-455).
//!
//! The wire handler lives here too. The pure-logic
//! `process_one_partition` helper turns one alter row
//! into a `PartitionRecord` ready to submit, or a wire error code.

use crabka_metadata::{MetadataImage, PartitionRecord};
use crabka_raft::NodeId;

use crate::codes::{
    ELIGIBLE_LEADERS_NOT_AVAILABLE, INVALID_REPLICA_ASSIGNMENT, NO_REASSIGNMENT_IN_PROGRESS,
    UNKNOWN_TOPIC_OR_PARTITION,
};

/// Process one (topic, partition, `target_opt`) row from an
/// `AlterPartitionReassignments` request. Returns:
///   - `Ok(Some(PartitionRecord))` — submit this intermediate record
///   - `Ok(None)` — no-op (already at target, or empty alter)
///   - `Err((wire_code, message))` — reject this row
pub(crate) fn process_one_partition(
    image: &MetadataImage,
    topic: &str,
    partition: i32,
    target: Option<&[i32]>,
    allow_rf_change: bool,
) -> Result<Option<PartitionRecord>, (i16, String)> {
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
) -> Result<(), (i16, String)> {
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
        #[allow(clippy::cast_sign_loss)] // replica IDs are always non-negative
        if image.broker(n as NodeId).is_none() {
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

fn cancel_path(pr: &PartitionRecord) -> Result<Option<PartitionRecord>, (i16, String)> {
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
        leader_epoch: pr.leader_epoch + epoch_bump,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: new_directories,
        partition_epoch: pr.partition_epoch + 1,
    }))
}

fn start_path(pr: &PartitionRecord, target: &[i32]) -> Option<PartitionRecord> {
    #[allow(clippy::cast_sign_loss)] // replica IDs are always non-negative
    let target_set: Vec<NodeId> = target.iter().map(|&x| x as NodeId).collect();
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
    let mut new_replicas = current_target.clone();
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
use crabka_protocol::Encode;
use crabka_protocol::owned::alter_partition_reassignments_request::AlterPartitionReassignmentsRequest;
use crabka_protocol::owned::alter_partition_reassignments_response::{
    AlterPartitionReassignmentsResponse, ReassignablePartitionResponse, ReassignableTopicResponse,
};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes::{CLUSTER_AUTHORIZATION_FAILED, COORDINATOR_NOT_AVAILABLE};

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
            resource_name: "kafka-cluster",
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
        for rows in by_topic.values_mut() {
            for r in rows.iter_mut() {
                if r.error_code == 0 {
                    r.error_code = COORDINATOR_NOT_AVAILABLE;
                    r.error_message = Some(format!("submit failed: {e}"));
                }
            }
        }
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
        throttle_time_ms: 0,
        allow_replication_factor_change: req.allow_replication_factor_change,
        error_code: 0,
        error_message: None,
        responses,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

fn ok_row(partition_index: i32) -> ReassignablePartitionResponse {
    ReassignablePartitionResponse {
        partition_index,
        error_code: 0,
        error_message: None,
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
        throttle_time_ms: 0,
        allow_replication_factor_change: req.allow_replication_factor_change,
        error_code: 0,
        error_message: None,
        responses,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let mut body = Vec::new();
    resp.encode(&mut body, api_version).map_err(|e| {
        crate::error::BrokerError::Replication(format!("encode AlterPartitionReassignments: {e}"))
    })?;
    Ok(Bytes::from(body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_metadata::{BrokerRegistrationRecord, MetadataRecord, TopicRecord};
    use uuid::Uuid;

    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    fn img_with(
        replicas: &[NodeId],
        isr: &[NodeId],
        adding: &[NodeId],
        removing: &[NodeId],
        leader: NodeId,
    ) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        // Register brokers 1..=6 so validate_target accepts target lists.
        for n in 1u64..=6 {
            img.apply(&MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    node_id: n,
                    broker_epoch: 0,
                    incarnation_id: uuid::Uuid::nil(),
                    host: "localhost".into(),
                    port: 9092,
                    rack: None,
                    endpoints: vec![],
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
            leader,
            replicas: replicas.to_vec(),
            isr: isr.to_vec(),
            leader_epoch: 5,
            adding_replicas: adding.to_vec(),
            removing_replicas: removing.to_vec(),
            directories: vec![],
            partition_epoch: 0,
        }));
        img
    }

    #[test]
    fn noop_when_already_at_target() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 2, 3]), true).unwrap();
        assert!(res.is_none());
    }

    #[test]
    fn start_writes_union_replicas() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 4]), true)
            .expect("ok")
            .expect("Some");
        assert!(res.replicas == vec![1, 2, 3, 4]);
        assert!(res.adding_replicas == vec![4]);
        assert!(res.removing_replicas == vec![2, 3]);
        assert!(res.leader == 1);
        assert!(res.leader_epoch == 5); // unchanged on start
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
        assert!(res.replicas == vec![1, 4, 5, 6]);
        assert!(res.adding_replicas == vec![5, 6]);
        assert!(res.removing_replicas == vec![1, 4]);
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
        assert!(res.removing_replicas == vec![3]);
    }

    #[test]
    fn cancel_with_leader_in_adding_reverts_leader() {
        // After a successful leader handoff during reassignment, leader=4 (an adding replica).
        // Cancel: leader should revert to whoever in reverted replicas ∩ isr.
        // replicas=[1,2,3,4], adding=[4], removing=[2,3], leader=4, isr=[1,4].
        let img = img_with(&[1, 2, 3, 4], &[1, 4], &[4], &[2, 3], 4);
        let res = process_one_partition(&img, "foo", 0, None, true)
            .expect("ok")
            .expect("Some");
        assert!(res.replicas == vec![1, 2, 3]);
        assert!(res.adding_replicas == Vec::<NodeId>::new());
        assert!(res.removing_replicas == Vec::<NodeId>::new());
        assert!(res.leader == 1); // reverted replicas ∩ isr = [1]
        assert!(res.leader_epoch == 6); // bumped
    }

    #[test]
    fn empty_target_rejected() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let err = process_one_partition(&img, "foo", 0, Some(&[]), true).unwrap_err();
        assert!(err.0 == INVALID_REPLICA_ASSIGNMENT);
    }
}
