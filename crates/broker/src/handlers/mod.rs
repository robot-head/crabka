//! Handler dispatch. One module per API key implements:
//!
//!   `pub async fn handle(broker: &Broker, version: i16, req_bytes: &[u8])
//!       -> Result<bytes::Bytes, BrokerError>`
//!
//! Handlers decode the request, do their work, encode the response, and
//! return the encoded bytes ready to ship after the response header is
//! prepended in `network::dispatch`.

#![allow(dead_code)] // handler modules are registered as each API is enabled.

use bytes::Bytes;
use crabka_protocol::api_key::ApiKey;

use crate::error::BrokerError;

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

/// Function signature every handler in this module exports.
pub type HandlerFn = fn(
    broker: &crate::broker::Broker,
    version: ApiVersion,
    correlation_id: CorrelationId,
    req_bytes: &[u8],
) -> futures_util::future::BoxFuture<'static, Result<Bytes, BrokerError>>;

/// API key → handler function. Built by `Broker::start` from the enabled
/// per-API modules.
#[derive(Default)]
pub struct HandlerTable {
    table: std::collections::HashMap<ApiKeyCode, HandlerFn>,
}

impl HandlerTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, api_key: ApiKeyCode, handler: HandlerFn) -> bool {
        self.table.insert(api_key, handler).is_none()
    }

    #[must_use]
    pub fn get(&self, api_key: ApiKeyCode) -> Option<HandlerFn> {
        self.table.get(&api_key).copied()
    }
}

pub(crate) mod context;
pub(crate) use context::{RequestContext, TelemetryContext};

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

/// Build the dispatch table for plain 4-arg handlers. Inline-intercepted
/// handlers are documented below and registered in `network::dispatch`.
#[must_use]
pub(crate) fn build_table() -> HandlerTable {
    let mut t = HandlerTable::new();
    // Produce (api_key 0) is intercepted inline in `network::dispatch`
    // so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-topic Write ACL enforcement.
    // Fetch (api_key 1) is intercepted inline in `network::dispatch`
    // so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-topic Read ACL enforcement.
    // Metadata (api_key 3) is intercepted inline in `network::dispatch`
    // so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-topic Describe ACL enforcement.
    // CreateTopics (api_key 19) is intercepted inline in `network::dispatch`
    // so the handler can receive the per-connection
    // principal + peer `SocketAddr` for Cluster Create ACL enforcement.
    // DeleteTopics (api_key 20) is intercepted inline in `network::dispatch`
    // so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-topic Delete ACL enforcement.
    // AlterConfigs (api_key 33) is intercepted inline in `network::dispatch`
    // so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-resource AlterConfigs ACL enforcement.
    // IncrementalAlterConfigs (api_key 44) is intercepted inline in `network::dispatch`
    // so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-resource AlterConfigs ACL enforcement.
    // DeleteRecords (api_key 21) is intercepted inline in `network::dispatch`
    // so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-topic Delete ACL enforcement.
    // CreatePartitions (api_key 37) is intercepted inline in `network::dispatch`
    // so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-topic Alter ACL enforcement.
    // DescribeGroups (api_key 15) is intercepted inline in `network::dispatch`
    // so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-group Describe ACL enforcement.
    // ListGroups (api_key 16) is intercepted inline in `network::dispatch`
    // so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-group Describe ACL enforcement
    // (silent filter — denied groups are omitted, not error-coded).
    // DeleteGroups (api_key 42) is intercepted inline in `network::dispatch`
    // so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-group Delete ACL enforcement.
    // JoinGroup (api_key 11) is intercepted inline in `network::dispatch`
    // so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-group Read ACL enforcement.
    // OffsetCommit (api_key 8) is intercepted inline in `network::dispatch`
    // so the handler can receive the per-connection
    // principal + peer `SocketAddr` for Group Read + per-topic Read ACL
    // enforcement.
    // OffsetFetch (api_key 9) is intercepted inline in `network::dispatch`
    // so the handler can receive the per-connection
    // principal + peer `SocketAddr` for Group Describe + per-topic Read ACL
    // enforcement (including the fetch-all `topics: None` sentinel).
    // DescribeCluster (api_key 60) is intercepted inline in `network::dispatch`
    // so the handler can receive the per-connection
    // principal + peer `SocketAddr` for Cluster Describe ACL enforcement.
    // AlterUserScramCredentials (api_key 51) is intercepted inline in
    // `network::dispatch` so the handler can receive the
    // per-connection principal + peer `SocketAddr` for Cluster Alter ACL
    // enforcement.
    // InitProducerId (api_key 22) is intercepted inline in `network::dispatch`
    // so the handler can receive the per-connection
    // principal + peer `SocketAddr` for either `Write` on
    // `TransactionalId` (transactional path) or `IdempotentWrite` on
    // `Cluster` (idempotent-only path).
    // AddPartitionsToTxn (api_key 24) is intercepted inline in `network::dispatch`
    // so the handler can receive the per-connection
    // principal + peer `SocketAddr` for `Write` on `TransactionalId` and
    // per-topic `Write` on `Topic` ACL enforcement.
    // EndTxn (api_key 26) is intercepted inline in `network::dispatch`
    // so the handler can receive the per-connection
    // principal + peer `SocketAddr` for `Write` on `TransactionalId` ACL
    // enforcement.
    // TxnOffsetCommit (api_key 28) is intercepted inline in `network::dispatch`
    // so the handler can receive the per-connection
    // principal + peer `SocketAddr` for `Write` on `TransactionalId` +
    // `Read` on `Group` + per-topic `Read` on `Topic` ACL enforcement.
    // 2 (ListOffsets) intercepted inline — per-topic Describe ACL.
    // 10 (FindCoordinator) intercepted inline — per-key Group/TransactionalId
    // Describe ACL.
    // 12 (Heartbeat) intercepted inline — Group Read ACL.
    // 13 (LeaveGroup) intercepted inline — Group Read ACL.
    // 14 (SyncGroup) intercepted inline — Group Read ACL.
    // 15 (DescribeGroups) intercepted inline — see comment above.
    // 16 (ListGroups) intercepted inline — see comment above.
    t.register(ApiKey::ApiVersions as i16, api_versions::handle);
    // 21 (DeleteRecords) intercepted inline — see comment above.
    // 22 (InitProducerId) intercepted inline — see comment above.
    // 23 (OffsetForLeaderEpoch) intercepted inline — per-topic Describe ACL.
    // 24 (AddPartitionsToTxn) intercepted inline — see comment above.
    t.register(
        ApiKey::AddOffsetsToTxn as i16,
        crate::txn::handlers::add_offset_commits_to_txn::handle,
    );
    // 26 (EndTxn) intercepted inline — see comment above.
    t.register(
        ApiKey::WriteTxnMarkers as i16,
        crate::txn::handlers::write_txn_markers::handle,
    );
    // 28 (TxnOffsetCommit) intercepted inline — see comment above.
    // 32 (DescribeConfigs) intercepted inline — per-resource DescribeConfigs ACL.
    // 33 (AlterConfigs) intercepted inline — see comment above.
    // 35 (DescribeLogDirs) intercepted inline — Cluster Describe ACL.
    // 37 (CreatePartitions) intercepted inline — see comment above.
    // 42 (DeleteGroups) intercepted inline — see comment above.
    // 44 (IncrementalAlterConfigs) intercepted inline — see comment above.
    // 56 (AlterPartition) intercepted inline — Cluster ClusterAction ACL.
    t.register(
        ApiKey::AssignReplicasToDirs as i16,
        assign_replicas_to_dirs::handle,
    );
    // FetchSnapshot (api_key 59, KIP-630) — controller-snapshot byte-range
    // fetch. Plain 4-arg signature: no per-connection ACL context needed.
    t.register(ApiKey::FetchSnapshot as i16, fetch_snapshot::handle);
    // 60 (DescribeCluster) intercepted inline — see comment above.
    // 63 (BrokerHeartbeat) intercepted inline — Cluster ClusterAction ACL.
    // 93 (GetReplicaLogInfo, KIP-966) intercepted inline — Cluster
    // ClusterAction ACL. Inter-broker RPC the controller's unclean recovery
    // manager uses to read each replica's LEO + leader epoch.
    // 68 (ConsumerGroupHeartbeat) intercepted inline — Group Read ACL.
    t.register(
        ApiKey::ConsumerGroupDescribe as i16,
        consumer_group_describe::handle,
    );
    // 76 (ShareGroupHeartbeat, KIP-932) intercepted inline — Group Read ACL.
    // 88 (StreamsGroupHeartbeat, KIP-1071) intercepted inline — Group Read ACL.
    // StreamsGroupDescribe (89) stays a plain 4-arg handler and does not apply
    // a per-group Describe ACL gate.
    t.register(
        ApiKey::StreamsGroupDescribe as i16,
        streams_group_describe::handle,
    );
    // KIP-932 share-state persister RPCs (api keys 83–87). Inter-broker
    // handlers, gated per-partition on local share-state leadership.
    t.register(
        ApiKey::InitializeShareGroupState as i16,
        crate::share_coordinator::handlers::initialize::handle,
    );
    t.register(
        ApiKey::ReadShareGroupState as i16,
        crate::share_coordinator::handlers::read::handle,
    );
    t.register(
        ApiKey::WriteShareGroupState as i16,
        crate::share_coordinator::handlers::write::handle,
    );
    t.register(
        ApiKey::DeleteShareGroupState as i16,
        crate::share_coordinator::handlers::delete::handle,
    );
    t.register(
        ApiKey::ReadShareGroupStateSummary as i16,
        crate::share_coordinator::handlers::read_summary::handle,
    );
    // 71 (GetTelemetrySubscriptions) intercepted inline in `network::dispatch`
    // so the handler receives the per-connection peer SocketAddr and software
    // name/version for KIP-714 subscription matching.
    // 72 (PushTelemetry) intercepted inline in `network::dispatch` for the
    // same reason — it needs the per-connection context to authorize pushes.
    t
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use assert2::assert;
    use crabka_security::{AuthMethod, Principal};

    use super::*;

    fn table_test_handler(
        _broker: &crate::broker::Broker,
        _version: i16,
        _correlation_id: i32,
        _req_bytes: &[u8],
    ) -> futures_util::future::BoxFuture<'static, Result<Bytes, BrokerError>> {
        Box::pin(async { Ok(Bytes::new()) })
    }

    #[test]
    fn handler_table_register_get_round_trips_handler() {
        let mut table = HandlerTable::new();

        assert!(table.get(1234).is_none());
        assert!(table.register(1234, table_test_handler));

        let registered = table.get(1234).expect("registered handler");
        assert!(std::ptr::fn_addr_eq(
            registered,
            table_test_handler as HandlerFn
        ));
        assert!(table.get(4321).is_none());
    }

    #[test]
    fn handler_table_register_reports_replaced_handler() {
        let mut table = HandlerTable::new();

        assert!(table.register(1234, table_test_handler));
        assert!(!table.register(1234, table_test_handler));

        let registered = table.get(1234).expect("registered handler");
        assert!(std::ptr::fn_addr_eq(
            registered,
            table_test_handler as HandlerFn
        ));
    }

    #[test]
    fn build_table_registers_required_plain_handlers() {
        let table = build_table();

        assert!(std::ptr::fn_addr_eq(
            table.get(18).expect("ApiVersions is registered"),
            api_versions::handle as HandlerFn
        ));
        for api_key in [59, 69, 89] {
            assert!(table.get(api_key).is_some(), "api_key {api_key}");
        }
    }

    #[test]
    fn build_table_registers_the_complete_plain_dispatch_set() {
        let table = build_table();

        let expected: &[(i16, HandlerFn)] = &[
            (18, api_versions::handle as HandlerFn),
            (
                25,
                crate::txn::handlers::add_offset_commits_to_txn::handle as HandlerFn,
            ),
            (
                27,
                crate::txn::handlers::write_txn_markers::handle as HandlerFn,
            ),
            (59, fetch_snapshot::handle as HandlerFn),
            (69, consumer_group_describe::handle as HandlerFn),
            (73, assign_replicas_to_dirs::handle as HandlerFn),
            (
                83,
                crate::share_coordinator::handlers::initialize::handle as HandlerFn,
            ),
            (
                84,
                crate::share_coordinator::handlers::read::handle as HandlerFn,
            ),
            (
                85,
                crate::share_coordinator::handlers::write::handle as HandlerFn,
            ),
            (
                86,
                crate::share_coordinator::handlers::delete::handle as HandlerFn,
            ),
            (
                87,
                crate::share_coordinator::handlers::read_summary::handle as HandlerFn,
            ),
            (89, streams_group_describe::handle as HandlerFn),
        ];

        assert!(table.table.len() == expected.len());
        for &(api_key, handler) in expected {
            assert!(std::ptr::fn_addr_eq(
                table.get(api_key).expect("plain handler is registered"),
                handler
            ));
        }
        for intercepted_api_key in [0, 1, 3, 19, 20, 33, 44, 56, 60, 63, 71, 72] {
            assert!(table.get(intercepted_api_key).is_none());
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
}
