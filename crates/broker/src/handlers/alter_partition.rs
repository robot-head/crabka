//! `AlterPartition` (`api_key=56`). Controller-side ISR update handler.
//!
//! Validates that this broker is the openraft leader (`NOT_CONTROLLER` if not),
//! checks leader-epoch fencing per partition, validates that the proposed ISR
//! is a non-empty subset of the partition's replicas, and submits the updated
//! `PartitionRecord` via `controller.submit_change`.

use bytes::{Bytes, BytesMut};

use crabka_metadata::{AclOperation, MetadataRecord, PartitionRecord, ResourceType};
use crabka_protocol::owned::alter_partition_request::AlterPartitionRequest;
use crabka_protocol::owned::alter_partition_response::{
    AlterPartitionResponse, PartitionData as RespPartitionData, TopicData as RespTopicData,
};
use crabka_protocol::{Decode, Encode};

use crate::authorizer::{AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

#[tracing::instrument(
    name = "handle_alter_partition",
    level = "info",
    skip_all,
    fields(api = "AlterPartition", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let controller = broker.controller.clone();
    let node_id = broker.config.node_id;

    {
        let mut cur: &[u8] = req_bytes;
        let req = AlterPartitionRequest::decode(&mut cur, version)?;

        // ── ACL preamble ────────────────────────────────────────────
        // Inter-broker control-plane RPC: `ClusterAction` on
        // `Cluster("kafka-cluster")`. On Deny → whole-response
        // `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.
        {
            let image = controller.current_image();
            if cluster_action_denied(
                broker.config.authorizer.as_ref(),
                &image,
                ctx.principal,
                ctx.peer,
            ) {
                return denied_response(version);
            }
        }

        // Only the openraft leader handles AlterPartition.
        let is_leader = controller
            .watch_leader()
            .borrow()
            .is_some_and(|n| n == node_id);
        if !is_leader {
            return encode_resp(
                version,
                &AlterPartitionResponse {
                    throttle_time_ms: 0,
                    error_code: codes::NOT_CONTROLLER,
                    ..Default::default()
                },
            );
        }

        let image = controller.current_image();
        let mut changes: Vec<MetadataRecord> = Vec::new();
        let mut resp_topics: Vec<RespTopicData> = Vec::new();

        for req_topic in &req.topics {
            // Find the topic name via topic_id from the metadata image.
            let topic_name_opt = image
                .topics()
                .find(|t| t.topic_id.as_bytes() == &req_topic.topic_id.0)
                .map(|t| t.name.clone());

            let mut resp_partitions: Vec<RespPartitionData> = Vec::new();
            for req_part in &req_topic.partitions {
                let resp_part = handle_partition(
                    &image,
                    topic_name_opt.as_deref(),
                    req_part.partition_index,
                    req_part.leader_epoch,
                    &req_part.new_isr,
                    &req_part.new_isr_with_epochs,
                    &mut changes,
                );
                resp_partitions.push(resp_part);
            }

            resp_topics.push(RespTopicData {
                topic_id: req_topic.topic_id,
                partitions: resp_partitions,
                ..Default::default()
            });
        }

        if !changes.is_empty()
            && let Err(e) = controller.submit_change(changes).await
        {
            return Err(BrokerError::Replication(format!("submit_change: {e}")));
        }

        encode_resp(
            version,
            &AlterPartitionResponse {
                throttle_time_ms: 0,
                error_code: codes::NONE,
                topics: resp_topics,
                ..Default::default()
            },
        )
    }
}

/// Validate and apply a single partition's ISR proposal. Returns the
/// per-partition response data and appends to `changes` on success.
///
/// `new_isr_i32` carries the v2 `new_isr` field; `new_isr_with_epochs`
/// carries the v3 field. For v3 requests `new_isr` is empty and
/// `new_isr_with_epochs` is populated — this function falls back to
/// extracting broker IDs from `new_isr_with_epochs` when `new_isr`
/// is empty.
fn handle_partition(
    image: &crabka_metadata::MetadataImage,
    topic_name: Option<&str>,
    partition_index: i32,
    req_leader_epoch: i32,
    new_isr_i32: &[i32],
    new_isr_with_epochs: &[crabka_protocol::owned::alter_partition_request::BrokerState],
    changes: &mut Vec<MetadataRecord>,
) -> RespPartitionData {
    let Some(topic_name) = topic_name else {
        return error_part(
            partition_index,
            codes::UNKNOWN_TOPIC_OR_PARTITION,
            0,
            0,
            &[],
        );
    };
    let Some(part_rec) = image.partition(topic_name, partition_index) else {
        return error_part(
            partition_index,
            codes::UNKNOWN_TOPIC_OR_PARTITION,
            0,
            0,
            &[],
        );
    };

    let leader_i32 = i32::try_from(part_rec.leader).unwrap_or(0);
    let current_isr_i32: Vec<i32> = part_rec
        .isr
        .iter()
        .map(|n| i32::try_from(*n).unwrap_or(0))
        .collect();

    // Leader-epoch fencing.
    if req_leader_epoch != part_rec.leader_epoch {
        return error_part(
            partition_index,
            codes::FENCED_LEADER_EPOCH,
            leader_i32,
            part_rec.leader_epoch,
            &current_isr_i32,
        );
    }

    // Resolve the effective ISR from the request. Protocol v2 sends
    // `new_isr: Vec<i32>`; v3 sends `new_isr_with_epochs` instead and
    // leaves `new_isr` empty. Fall back to extracting broker_ids from
    // `new_isr_with_epochs` when the v2 field is absent.
    let effective_isr_i32: &[i32];
    let fallback_isr_i32: Vec<i32>;
    if new_isr_i32.is_empty() && !new_isr_with_epochs.is_empty() {
        fallback_isr_i32 = new_isr_with_epochs.iter().map(|bs| bs.broker_id).collect();
        effective_isr_i32 = &fallback_isr_i32;
    } else {
        effective_isr_i32 = new_isr_i32;
    }

    // Validate proposed ISR: non-empty + subset of replicas.
    let proposed_isr: Vec<u64> = effective_isr_i32
        .iter()
        .map(|&n| u64::try_from(n).unwrap_or(0))
        .collect();
    let replicas_set: std::collections::HashSet<u64> = part_rec.replicas.iter().copied().collect();
    let valid = !proposed_isr.is_empty() && proposed_isr.iter().all(|n| replicas_set.contains(n));
    if !valid {
        return error_part(
            partition_index,
            codes::INVALID_REQUEST,
            leader_i32,
            part_rec.leader_epoch,
            &current_isr_i32,
        );
    }

    // KIP-903: fence ineligible replicas. A broker in the proposed ISR is
    // ineligible if it is not currently registered, or if its stamped broker
    // epoch is non-sentinel (-1) and disagrees with the controller's
    // registration epoch. Any ineligible replica fails the whole partition.
    for bstate in new_isr_with_epochs {
        let node = u64::try_from(bstate.broker_id).unwrap_or(u64::MAX);
        let registered = image.broker_epoch(node);
        let ineligible = registered.is_none()
            || (bstate.broker_epoch != -1 && registered != Some(bstate.broker_epoch));
        if ineligible {
            return error_part(
                partition_index,
                codes::INELIGIBLE_REPLICA,
                leader_i32,
                part_rec.leader_epoch,
                &current_isr_i32,
            );
        }
    }

    // Success: submit the ISR change.
    let new_partition_epoch = part_rec.partition_epoch + 1;
    changes.push(MetadataRecord::V1Partition(PartitionRecord {
        topic: topic_name.to_string(),
        partition: partition_index,
        leader: part_rec.leader,
        replicas: part_rec.replicas.clone(),
        isr: proposed_isr,
        leader_epoch: part_rec.leader_epoch,
        adding_replicas: part_rec.adding_replicas.clone(),
        removing_replicas: part_rec.removing_replicas.clone(),
        directories: part_rec.directories.clone(),
        partition_epoch: new_partition_epoch,
    }));

    RespPartitionData {
        partition_index,
        error_code: codes::NONE,
        leader_id: leader_i32,
        leader_epoch: part_rec.leader_epoch,
        isr: effective_isr_i32.to_vec(),
        leader_recovery_state: 0,
        partition_epoch: new_partition_epoch,
        ..Default::default()
    }
}

fn error_part(
    partition_index: i32,
    error_code: i16,
    leader_id: i32,
    leader_epoch: i32,
    isr: &[i32],
) -> RespPartitionData {
    RespPartitionData {
        partition_index,
        error_code,
        leader_id,
        leader_epoch,
        isr: isr.to_vec(),
        leader_recovery_state: 0,
        partition_epoch: 0,
        ..Default::default()
    }
}

/// `ClusterAction` on `Cluster("kafka-cluster")` gate. Returns `true`
/// when the principal is denied (inter-broker control-plane RPC).
fn cluster_action_denied(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &crabka_metadata::MetadataImage,
    principal: &crabka_security::Principal,
    host: &std::net::SocketAddr,
) -> bool {
    authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal,
            host,
            resource_type: ResourceType::Cluster,
            resource_name: "kafka-cluster",
            operation: AclOperation::ClusterAction,
        },
    ) == AuthorizationResult::Deny
}

/// Whole-response `CLUSTER_AUTHORIZATION_FAILED (31)` response built on Deny.
fn denied_response(version: i16) -> Result<Bytes, BrokerError> {
    encode_resp(
        version,
        &AlterPartitionResponse {
            throttle_time_ms: 0,
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            ..Default::default()
        },
    )
}

fn encode_resp(version: i16, resp: &AlterPartitionResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_metadata::{
        BrokerRegistrationRecord, MetadataImage, MetadataRecord, PartitionRecord, TopicRecord,
    };
    use crabka_protocol::owned::alter_partition_request::BrokerState;

    fn reg(node_id: u64, epoch: i64) -> MetadataRecord {
        MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id,
            broker_epoch: epoch,
            incarnation_id: uuid::Uuid::nil(),
            host: "h".into(),
            port: 9092,
            rack: None,
            endpoints: vec![],
        })
    }

    /// Image with topic "t" / partition 0, replicas [1,2,3], isr [1,2],
    /// leader 1 @ `leader_epoch` 5. Brokers registered per `epochs`.
    fn image_with(epochs: &[(u64, i64)]) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: uuid::Uuid::nil(),
            partitions: 1,
            replication_factor: 3,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2],
            leader_epoch: 5,
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
        for &(id, ep) in epochs {
            image.apply(&reg(id, ep));
        }
        image
    }

    fn bs(broker_id: i32, broker_epoch: i64) -> BrokerState {
        BrokerState {
            broker_id,
            broker_epoch,
            ..Default::default()
        }
    }

    /// Empty ACLs + no super-users → every principal is denied
    /// `ClusterAction`, so the denied response carries
    /// `CLUSTER_AUTHORIZATION_FAILED`.
    #[test]
    fn cluster_action_denied_yields_cluster_authorization_failed() {
        use crabka_protocol::owned::alter_partition_response::{self, AlterPartitionResponse};

        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = MetadataImage::new(uuid::Uuid::nil());
        let principal = crabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        };
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));

        assert!(cluster_action_denied(
            &authorizer,
            &image,
            &principal,
            &peer
        ));

        let bytes = denied_response(alter_partition_response::MAX_VERSION).expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp = AlterPartitionResponse::decode(&mut cur, alter_partition_response::MAX_VERSION)
            .unwrap();
        assert!(resp.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
    }

    #[test]
    fn matching_epochs_succeed() {
        let image = image_with(&[(1, 10), (2, 20), (3, 30)]);
        let mut changes = Vec::new();
        let isr = vec![bs(1, 10), bs(2, 20), bs(3, 30)];
        let resp = handle_partition(&image, Some("t"), 0, 5, &[], &isr, &mut changes);
        assert!(resp.error_code == codes::NONE, "got {}", resp.error_code);
        assert!(changes.len() == 1);
    }

    #[test]
    fn stale_epoch_is_ineligible() {
        let image = image_with(&[(1, 10), (2, 20), (3, 30)]);
        let mut changes = Vec::new();
        let isr = vec![bs(1, 10), bs(2, 20), bs(3, 29)]; // 29 != image 30
        let resp = handle_partition(&image, Some("t"), 0, 5, &[], &isr, &mut changes);
        assert!(
            resp.error_code == codes::INELIGIBLE_REPLICA,
            "got {}",
            resp.error_code
        );
        assert!(changes.is_empty());
    }

    #[test]
    fn unregistered_replica_is_ineligible() {
        let image = image_with(&[(1, 10), (2, 20)]); // broker 3 never registered
        let mut changes = Vec::new();
        let isr = vec![bs(1, 10), bs(2, 20), bs(3, -1)];
        let resp = handle_partition(&image, Some("t"), 0, 5, &[], &isr, &mut changes);
        assert!(
            resp.error_code == codes::INELIGIBLE_REPLICA,
            "got {}",
            resp.error_code
        );
        assert!(changes.is_empty());
    }

    #[test]
    fn sentinel_epoch_skips_epoch_check() {
        let image = image_with(&[(1, 10), (2, 20), (3, 30)]);
        let mut changes = Vec::new();
        let isr = vec![bs(1, -1), bs(2, -1), bs(3, -1)]; // -1 = don't check
        let resp = handle_partition(&image, Some("t"), 0, 5, &[], &isr, &mut changes);
        assert!(resp.error_code == codes::NONE, "got {}", resp.error_code);
        assert!(changes.len() == 1);
    }

    #[test]
    fn v2_no_epochs_path_unaffected() {
        let image = image_with(&[(1, 10), (2, 20)]);
        let mut changes = Vec::new();
        // v2: new_isr populated, new_isr_with_epochs empty -> no epoch fencing.
        let resp = handle_partition(&image, Some("t"), 0, 5, &[1, 2, 3], &[], &mut changes);
        assert!(resp.error_code == codes::NONE, "got {}", resp.error_code);
        assert!(changes.len() == 1);
    }
}
