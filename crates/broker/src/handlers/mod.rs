//! Handler dispatch. One module per API key implements:
//!
//!   `pub async fn handle(broker: &Broker, version: i16, req_bytes: &[u8])
//!       -> Result<bytes::Bytes, BrokerError>`
//!
//! Handlers decode the request, do their work, encode the response, and
//! return the encoded bytes. The bytes are ready to send after
//! `network::dispatch` prepends the response header.

/// Raw wire `api_key` (i16) that selects the RPC.
///
/// This is the numeric form of a [`crabka_protocol::api_key::ApiKey`] variant.
/// It stays an `i16` because it arrives off the wire and may name an API that
/// this broker does not know.
pub type ApiKeyCode = i16;

/// Negotiated Kafka request/response schema version for a single RPC.
pub type ApiVersion = i16;

/// Kafka wire error code (`crate::codes::*`), `0` = NONE.
pub type ErrorCode = i16;

/// Client-chosen request correlation id. The response header echoes it exactly.
pub type CorrelationId = i32;

use bytes::{Bytes, BytesMut};
use crabka_protocol::Encode;

use crate::error::BrokerError;

pub(crate) mod context;
pub(crate) use context::{RequestContext, TelemetryContext};

pub(crate) mod registry;
pub(crate) use registry::{DispatchEntry, DispatchKind, DispatchRegistry, RequestQuotaPolicy};

pub(crate) fn encode_response<R: Encode>(
    resp: &R,
    version: ApiVersion,
) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

pub(crate) fn encode_response_with_context<R: Encode>(
    resp: &R,
    version: ApiVersion,
    context: &'static str,
) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)
        .map_err(|e| BrokerError::Replication(format!("{context}: {e}")))?;
    Ok(buf.freeze())
}

pub(crate) fn acl_denied(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &crabka_metadata::MetadataImage,
    ctx: &RequestContext<'_>,
    resource_type: crabka_metadata::ResourceType,
    resource_name: &str,
    operation: crabka_metadata::AclOperation,
) -> bool {
    authorizer.authorize(
        image,
        &crate::authorizer::AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type,
            resource_name,
            operation,
        },
    ) == crate::authorizer::AuthorizationResult::Deny
}

pub(crate) fn group_read_denied(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &crabka_metadata::MetadataImage,
    ctx: &RequestContext<'_>,
    group_id: &str,
) -> bool {
    acl_denied(
        authorizer,
        image,
        ctx,
        crabka_metadata::ResourceType::Group,
        group_id,
        crabka_metadata::AclOperation::Read,
    )
}

/// Return the Kafka routing error for a group RPC sent to the wrong broker,
/// or `None` when this broker leads the group's offsets partition.
pub(crate) fn group_coordinator_error(
    broker: &crate::broker::Broker,
    group_id: &str,
) -> Option<i16> {
    use crate::coordinator::partitioner::{GroupRoutingError, local_partition_for_group};

    match local_partition_for_group(
        &broker.controller.current_image(),
        broker.config.node_id,
        group_id,
    ) {
        Ok(_) => None,
        Err(GroupRoutingError::Unavailable) => Some(crate::codes::COORDINATOR_NOT_AVAILABLE),
        Err(GroupRoutingError::NotCoordinator) => Some(crate::codes::NOT_COORDINATOR),
    }
}

pub(crate) fn cluster_alter_denied(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &crabka_metadata::MetadataImage,
    ctx: &RequestContext<'_>,
) -> bool {
    acl_denied(
        authorizer,
        image,
        ctx,
        crabka_metadata::ResourceType::Cluster,
        acl_wire::CLUSTER_RESOURCE_NAME,
        crabka_metadata::AclOperation::Alter,
    )
}

pub(crate) fn parse_advertised_host_port(addr: &str) -> (String, u16) {
    if let Some(host_port) = crate::host_port::parse_host_port(addr) {
        return host_port;
    }
    tracing::warn!(
        addr,
        "advertised_listener not host:port; falling back to localhost:9092"
    );
    (
        crate::host_port::DEFAULT_KAFKA_HOST.into(),
        crate::host_port::DEFAULT_KAFKA_PORT,
    )
}

pub(crate) mod acl_wire;
// KIP-853 dynamic-quorum reconfiguration (api_keys 80/81/82).
pub(crate) mod add_raft_voter;
pub(crate) mod allocate_producer_ids;
pub(crate) mod alter_client_quotas;
pub(crate) mod alter_configs;
pub(crate) mod alter_partition;
pub(crate) mod alter_partition_reassignments;
pub(crate) mod alter_replica_log_dirs;
pub(crate) mod alter_user_scram_credentials;
pub(crate) mod api_versions;
pub(crate) mod assign_replicas_to_dirs;
// KIP-430: authorized-operations bitfield helper used by metadata,
// describe_cluster, describe_groups when the request opts in.
pub(crate) mod authorized_operations;
pub(crate) mod broker_heartbeat;
pub(crate) mod broker_registration;
pub(crate) mod consumer_group_describe;
pub(crate) mod consumer_group_heartbeat;
pub(crate) mod controller_registration;
pub(crate) mod create_acls;
pub(crate) mod create_delegation_token;
pub(crate) mod create_partitions;
pub(crate) mod create_topics;
pub(crate) mod delete_acls;
pub(crate) mod delete_groups;
pub(crate) mod delete_records;
pub(crate) mod delete_topics;
pub(crate) mod describe_acls;
pub(crate) mod describe_client_quotas;
pub(crate) mod describe_cluster;
pub(crate) mod describe_configs;
pub(crate) mod describe_delegation_token;
pub(crate) mod describe_groups;
pub(crate) mod describe_log_dirs;
// KIP-664 producer-state introspection (api_key 61).
pub(crate) mod describe_producers;
// KIP-595 raft-quorum introspection (api_key 55).
pub(crate) mod describe_quorum;
// KIP-664 transaction introspection (api_key 65).
pub(crate) mod describe_transactions;
// KIP-966 paginated topic listing (api_key 75).
pub(crate) mod describe_topic_partitions;
pub(crate) mod describe_user_scram_credentials;
pub(crate) mod elect_leaders;
pub(crate) mod expire_delegation_token;
pub(crate) mod fetch;
pub(crate) mod fetch_downconvert;
// KIP-630 controller-snapshot fetch (api_key 59).
pub(crate) mod fetch_snapshot;
pub(crate) mod find_coordinator;
pub(crate) mod get_replica_log_info;
// KIP-714 client telemetry. `get` assigns configured subscriptions and `push`
// validates, decodes, and exports OTLP metrics to the configured sinks.
pub(crate) mod get_telemetry_subscriptions;
pub(crate) mod heartbeat;
pub(crate) mod incremental_alter_configs;
pub(crate) mod init_producer_id;
pub(crate) mod join_group;
pub(crate) mod leave_group;
// KIP-1142 list-config-resources admin RPC (api_key 74). Generalises the
// v0 ListClientMetricsResources call (KIP-714) into a typed enumeration.
pub(crate) mod list_config_resources;
pub(crate) mod list_groups;
pub(crate) mod list_offsets;
// KIP-664 transaction-summary admin RPC (api_key 66).
pub(crate) mod list_partition_reassignments;
pub(crate) mod list_transactions;
pub(crate) mod metadata;
pub(crate) mod offset_commit;
pub(crate) mod offset_delete;
pub(crate) mod offset_fetch;
pub(crate) mod offset_for_leader_epoch;
pub(crate) mod produce;
// KIP-714 client-metrics push, paired with get_telemetry_subscriptions.
pub(crate) mod push_telemetry;
pub(crate) mod remove_raft_voter;
pub(crate) mod renew_delegation_token;
// KIP-932 ShareGroupDescribe (api_key 77). Intercepted inline in
// `network::dispatch` so the handler receives the per-connection principal +
// peer `SocketAddr` for the per-group Describe ACL gate.
pub(crate) mod share_group_describe;
// KIP-932 share-group membership (api_key 76).
pub(crate) mod share_group_heartbeat;
// KIP-932 admin offset RPCs (api_key 90/91/92). Intercepted inline in
// `network::dispatch` for the per-group Describe/Alter/Delete ACL gates.
pub(crate) mod alter_share_group_offsets;
pub(crate) mod delete_share_group_offsets;
pub(crate) mod describe_share_group_offsets;
// KIP-932 ShareAcknowledge (api_key 79). Intercepted inline in
// `network::dispatch` for the per-topic Read ACL gate.
pub(crate) mod share_acknowledge;
// KIP-932 ShareFetch (api_key 78). Intercepted inline in `network::dispatch`
// for the per-topic Read ACL gate.
pub(crate) mod share_fetch;
// KIP-1071 StreamsGroupDescribe (api_key 89). Plain 4-arg handler mirroring
// consumer_group_describe; it does not apply a per-group Describe ACL gate.
pub(crate) mod streams_group_describe;
// KIP-1071 streams-group membership / rebalance protocol (api_key 88).
pub(crate) mod streams_group_heartbeat;
pub(crate) mod sync_group;
// KIP-919 admin RPC to permanently drop a broker registration (api_key 64).
pub(crate) mod unregister_broker;
// KIP-584 feature finalization (api_key 57). Intercepted inline in
// `network::dispatch` so the handler receives the per-connection principal +
// peer `SocketAddr` for the Cluster:Alter ACL gate.
pub(crate) mod update_features;
pub(crate) mod update_raft_voter;

/// Emits an `AdminOperation` audit event for a completed admin request.
///
/// Call this on the SUCCESS path of each admin handler, after the broker
/// applies the operation and knows the set of resources that it changed
/// successfully. This function does nothing when `resources` is empty. The
/// caller guards with `if !resources.is_empty()`.
pub(crate) fn audit_admin(
    audit_log: &crabka_audit::AuditLog,
    ctx: &RequestContext<'_>,
    operation: &str,
    outcome: crabka_audit::AuditOutcome,
    resources: Vec<crabka_audit::AuditResource>,
) {
    audit_log.emit(crabka_audit::AuditEvent::AdminOperation {
        outcome,
        principal: crabka_audit::AuditPrincipal {
            name: ctx.principal.name.clone(),
            auth_method: format!("{:?}", ctx.principal.auth_method),
        },
        source: crabka_audit::AuditEndpoint {
            ip: ctx.peer.ip().to_string(),
            port: ctx.peer.port(),
        },
        operation: operation.to_string(),
        resources,
        time_ms: crate::time_util::now_ms(),
    });
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, net::SocketAddr};

    use assert2::assert;
    use crabka_metadata::{AclOperation, MetadataImage, ResourceType};
    use crabka_protocol::{
        Decode,
        owned::api_versions_response::{ApiVersion, ApiVersionsResponse},
    };
    use crabka_security::{AuthMethod, Principal};

    use super::*;

    fn principal() -> Principal {
        Principal {
            name: "alice".to_string(),
            auth_method: AuthMethod::SaslPlain,
            groups: vec!["operators".to_string()],
        }
    }

    #[test]
    fn audit_admin_emits_admin_operation_event() {
        let (log, mut rx) = crabka_audit::AuditLog::new(8);
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::SaslPlain,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "192.0.2.10:9092".parse().unwrap();
        let ctx = RequestContext {
            principal: &principal,
            peer: &peer,
            client_id: "admin-client",
            connection_id: "connection-a",
            sendfile_capable: false,
            connection_listener_name: "PLAINTEXT",
        };

        audit_admin(
            log.as_ref(),
            &ctx,
            "CreateTopics",
            crabka_audit::AuditOutcome::Success,
            vec![crabka_audit::AuditResource {
                resource_type: "Topic".into(),
                name: "orders".into(),
            }],
        );

        match rx.try_recv().expect("admin audit event") {
            crabka_audit::AuditEvent::AdminOperation {
                outcome,
                principal,
                source,
                operation,
                resources,
                ..
            } => {
                assert!(
                    (
                        outcome,
                        principal.name.as_str(),
                        principal.auth_method.as_str(),
                        source.ip.as_str(),
                        source.port,
                        operation.as_str(),
                        resources.len(),
                        resources[0].resource_type.as_str(),
                        resources[0].name.as_str()
                    ) == (
                        crabka_audit::AuditOutcome::Success,
                        "admin",
                        "SaslPlain",
                        "192.0.2.10",
                        9092,
                        "CreateTopics",
                        1,
                        "Topic",
                        "orders"
                    )
                );
            }
            other => panic!("expected admin operation event, got {other:?}"),
        }
    }

    #[test]
    fn encode_response_round_trips_protocol_body() {
        let resp = ApiVersionsResponse {
            error_code: crate::codes::NONE,
            api_keys: vec![ApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 4,
                ..Default::default()
            }],
            throttle_time_ms: 0,
            ..Default::default()
        };

        let bytes = encode_response(&resp, 3).expect("encode response");
        let mut cur: &[u8] = &bytes;
        let decoded = ApiVersionsResponse::decode(&mut cur, 3).expect("decode response");

        assert!(decoded.error_code == crate::codes::NONE);
        assert!(decoded.api_keys.len() == 1);
        assert!(decoded.api_keys[0].api_key == 18);
        assert!(decoded.api_keys[0].min_version == 0);
        assert!(decoded.api_keys[0].max_version == 4);
    }

    #[test]
    fn acl_denied_reports_simple_acl_denial() {
        let authorizer = crate::authorizer::SimpleAclAuthorizer::new(HashSet::new());
        let image = MetadataImage::new(uuid::Uuid::nil());
        let principal = principal();
        let peer = SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = RequestContext {
            principal: &principal,
            peer: &peer,
            client_id: "client-a",
            connection_id: "connection-a",
            sendfile_capable: false,
            connection_listener_name: "PLAINTEXT",
        };

        assert!(acl_denied(
            &authorizer,
            &image,
            &ctx,
            ResourceType::Topic,
            "orders",
            AclOperation::Describe,
        ));
    }
}
