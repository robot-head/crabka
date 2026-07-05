//! Handler dispatch. One module per API key implements:
//!
//!   `pub async fn handle(broker: &Broker, version: i16, req_bytes: &[u8])
//!       -> Result<bytes::Bytes, BrokerError>`
//!
//! Handlers decode the request, do their work, encode the response, and
//! return the encoded bytes ready to ship after the response header is
//! prepended in `network::dispatch`.

#![allow(dead_code)] // handler modules are registered as each API is enabled.

/// Raw wire `api_key` (i16) selecting the RPC — the numeric form of a
/// [`crabka_protocol::api_key::ApiKey`] variant, kept as `i16` because it
/// arrives off the wire and may name an API this broker doesn't know.
pub type ApiKeyCode = i16;

/// Negotiated Kafka request/response schema version for a single RPC.
pub type ApiVersion = i16;

/// Kafka wire error code (`crate::codes::*`), `0` = NONE.
pub type ErrorCode = i16;

/// Client-chosen request correlation id, echoed verbatim in the response header.
pub type CorrelationId = i32;

use bytes::{Bytes, BytesMut};
use crabka_protocol::Encode;

use crate::error::BrokerError;

pub(crate) mod context;
pub(crate) use context::{RequestContext, TelemetryContext};

pub(crate) mod registry;
pub(crate) use registry::DispatchRegistry;
#[allow(unused_imports)] // Staged for later dispatch-registry handler families.
pub(crate) use registry::{DispatchEntry, DispatchKind, PlainHandler, RequestQuotaPolicy};

pub(crate) fn encode_response<R: Encode>(
    resp: &R,
    version: ApiVersion,
) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
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

pub(crate) mod acl_wire;
// KIP-853 dynamic-quorum reconfiguration (api_keys 80/81/82).
pub(crate) mod add_raft_voter;
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
pub(crate) mod consumer_group_describe;
pub(crate) mod consumer_group_heartbeat;
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
// KIP-714 client telemetry. Pair of no-op handlers — `get` advertises
// "no metrics subscribed" so well-behaved clients skip `push` entirely;
// `push` is wired defensively in case a client races the subscription
// re-fetch.
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

/// Emit an `AdminOperation` audit event for a completed admin request.
///
/// Call this on the SUCCESS path of each admin handler after the operation
/// has been applied and the set of successfully-affected resources is known.
/// A no-op when `resources` is empty (caller guards with `if !resources.is_empty()`).
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
