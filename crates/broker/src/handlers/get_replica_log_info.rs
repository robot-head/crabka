//! `GetReplicaLogInfo` (`api_key` 93, KIP-966). Inter-broker RPC: the
//! controller asks this broker for the log-end-offset and last-written
//! leader epoch of partitions it hosts, to drive offset-aware unclean
//! recovery. Served on the inter-broker listener via the handler table.
//!
//! For each requested partition the broker hosts locally we answer with
//! the local LEO + cached leader epoch. Partitions not hosted here get
//! `REPLICA_NOT_AVAILABLE (11)` with sentinel offsets (`-1`), matching
//! the JVM behaviour for a replica the broker isn't a member of.

use std::sync::atomic::Ordering;

use bytes::Bytes;
use crabka_metadata::{AclOperation, ResourceType};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        get_replica_log_info_request::GetReplicaLogInfoRequest,
        get_replica_log_info_response::{
            GetReplicaLogInfoResponse, PartitionLogInfo, TopicPartitionLogInfo,
        },
    },
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
};

#[allow(clippy::unused_async)] // async to match the inline-intercept handler shape.
#[tracing::instrument(
    name = "handle_get_replica_log_info",
    level = "info",
    skip_all,
    fields(api = "GetReplicaLogInfo", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let image = broker.controller.current_image();

    // ── ACL preamble ────────────────────────────────────────────
    // Inter-broker control-plane RPC: `ClusterAction` on
    // `Cluster("kafka-cluster")`. The response has no top-level error
    // field (it's a list of per-partition log-info rows), so on Deny we
    // stamp `CLUSTER_AUTHORIZATION_FAILED (31)` on every requested
    // partition — mirroring `alter_replica_log_dirs`' cluster-deny path.
    if cluster_action_denied(
        broker.config.authorizer.as_ref(),
        &image,
        ctx.principal,
        ctx.peer,
    ) {
        return denied_response(version, req_bytes);
    }

    let mut topic_results: Vec<TopicPartitionLogInfo> = Vec::new();

    let mut cur: &[u8] = req_bytes;
    if let Ok(req) = GetReplicaLogInfoRequest::decode(&mut cur, version) {
        for tp in &req.topic_partitions {
            // The protocol `topic_id` is the `[u8; 16]` wire newtype; the
            // metadata image stores the external `uuid::Uuid`. Match on the
            // raw bytes (mirrors the lookup in `handlers/metadata.rs`).
            let topic_name = image
                .topics()
                .find(|t| t.topic_id.into_bytes() == tp.topic_id.0)
                .map(|t| t.name.clone());

            let mut partition_log_info = Vec::with_capacity(tp.partitions.len());
            for &p in &tp.partitions {
                let hosted = topic_name
                    .as_deref()
                    .and_then(|name| broker.partitions.get(name, p));
                partition_log_info.push(match hosted {
                    Some(part) => {
                        let epoch = part.current_leader_epoch.load(Ordering::Acquire);
                        PartitionLogInfo {
                            partition: p,
                            last_written_leader_epoch: epoch,
                            current_leader_epoch: epoch,
                            log_end_offset: part.log_end_offset(),
                            error_code: codes::NONE,
                            error_message: None,
                            ..Default::default()
                        }
                    }
                    None => PartitionLogInfo {
                        partition: p,
                        last_written_leader_epoch: -1,
                        current_leader_epoch: -1,
                        log_end_offset: -1,
                        error_code: codes::REPLICA_NOT_AVAILABLE,
                        error_message: Some("partition not hosted locally".into()),
                        ..Default::default()
                    },
                });
            }

            topic_results.push(TopicPartitionLogInfo {
                topic_id: tp.topic_id,
                partition_log_info,
                ..Default::default()
            });
        }
    }

    let resp = GetReplicaLogInfoResponse {
        broker_epoch: 0,
        topic_partition_log_info_list: topic_results,
        ..Default::default()
    };

    let mut body = Vec::new();
    resp.encode(&mut body, version)
        .map_err(|e| BrokerError::Replication(format!("encode GetReplicaLogInfo: {e}")))?;
    Ok(Bytes::from(body))
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
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            operation: AclOperation::ClusterAction,
        },
    ) == AuthorizationResult::Deny
}

/// On Deny, echo every requested `(topic_id, partition)` with
/// `CLUSTER_AUTHORIZATION_FAILED (31)` and sentinel offsets. The response
/// carries no top-level error code, so the per-partition stamp is the
/// channel for the authorization failure (mirrors `alter_replica_log_dirs`).
fn denied_response(version: i16, req_bytes: &[u8]) -> Result<Bytes, BrokerError> {
    let mut topic_results: Vec<TopicPartitionLogInfo> = Vec::new();
    let mut cur: &[u8] = req_bytes;
    if let Ok(req) = GetReplicaLogInfoRequest::decode(&mut cur, version) {
        for tp in &req.topic_partitions {
            let partition_log_info = tp
                .partitions
                .iter()
                .map(|&p| PartitionLogInfo {
                    partition: p,
                    last_written_leader_epoch: -1,
                    current_leader_epoch: -1,
                    log_end_offset: -1,
                    error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                    error_message: Some("cluster authorization failed".into()),
                    ..Default::default()
                })
                .collect();
            topic_results.push(TopicPartitionLogInfo {
                topic_id: tp.topic_id,
                partition_log_info,
                ..Default::default()
            });
        }
    }
    let resp = GetReplicaLogInfoResponse {
        broker_epoch: 0,
        topic_partition_log_info_list: topic_results,
        ..Default::default()
    };
    let mut body = Vec::new();
    resp.encode(&mut body, version)
        .map_err(|e| BrokerError::Replication(format!("encode GetReplicaLogInfo: {e}")))?;
    Ok(Bytes::from(body))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    /// Empty ACLs + no super-users → every principal is denied
    /// `ClusterAction`, so the denied response carries
    /// `CLUSTER_AUTHORIZATION_FAILED`.
    #[test]
    fn cluster_action_denied_yields_cluster_authorization_failed() {
        use bytes::BytesMut;
        use crabka_protocol::owned::{
            get_replica_log_info_request::{self, GetReplicaLogInfoRequest, TopicPartitions},
            get_replica_log_info_response::GetReplicaLogInfoResponse,
        };

        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
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

        let version = get_replica_log_info_request::MAX_VERSION;
        let req = GetReplicaLogInfoRequest {
            topic_partitions: vec![TopicPartitions {
                topic_id: crabka_protocol::primitives::uuid::Uuid([7u8; 16]),
                partitions: vec![0, 1],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut req_buf = BytesMut::new();
        req.encode(&mut req_buf, version).expect("encode req");

        let bytes = denied_response(version, &req_buf).expect("encode resp");
        let mut cur: &[u8] = &bytes;
        let resp = GetReplicaLogInfoResponse::decode(&mut cur, version).unwrap();
        let codes_seen: Vec<i16> = resp
            .topic_partition_log_info_list
            .iter()
            .flat_map(|t| t.partition_log_info.iter().map(|p| p.error_code))
            .collect();
        assert!(!codes_seen.is_empty());
        assert!(
            codes_seen
                .iter()
                .all(|&c| c == codes::CLUSTER_AUTHORIZATION_FAILED)
        );
    }
}
