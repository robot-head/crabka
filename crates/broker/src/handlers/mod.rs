//! Handler dispatch. One module per API key implements:
//!
//!   `pub async fn handle(broker: &Broker, version: i16, req_bytes: &[u8])
//!       -> Result<bytes::Bytes, BrokerError>`
//!
//! Handlers decode the request, do their work, encode the response, and
//! return the encoded bytes ready to ship after the response header is
//! prepended in `network::dispatch`.

#![allow(dead_code)] // handlers land per-API in Phase E.

use bytes::Bytes;

use crate::error::BrokerError;

/// Function signature every handler in this module exports.
pub type HandlerFn = fn(
    broker: &crate::broker::Broker,
    version: i16,
    correlation_id: i32,
    req_bytes: &[u8],
) -> futures_util::future::BoxFuture<'static, Result<Bytes, BrokerError>>;

/// API key → handler function. Built by `Broker::start` from the per-API
/// modules that exist after Phase E.
#[derive(Default)]
pub struct HandlerTable {
    table: std::collections::HashMap<i16, HandlerFn>,
}

impl HandlerTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, api_key: i16, handler: HandlerFn) {
        self.table.insert(api_key, handler);
    }

    #[must_use]
    pub fn get(&self, api_key: i16) -> Option<HandlerFn> {
        self.table.get(&api_key).copied()
    }
}

pub(crate) mod context;
pub(crate) use context::RequestContext;

pub(crate) mod acl_wire;
pub(crate) mod alter_client_quotas;
pub(crate) mod alter_configs;
pub(crate) mod alter_partition;
pub(crate) mod alter_partition_reassignments;
pub(crate) mod alter_replica_log_dirs;
pub(crate) mod alter_user_scram_credentials;
pub(crate) mod api_versions;
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
pub(crate) mod find_coordinator;
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
pub(crate) mod renew_delegation_token;
pub(crate) mod sync_group;
// KIP-185 admin RPC to permanently drop a broker registration (api_key 64).
pub(crate) mod unregister_broker;
// KIP-584 feature finalization (api_key 57). Intercepted inline in
// `network::dispatch` so the handler receives the per-connection principal +
// peer `SocketAddr` for the Cluster:Alter ACL gate.
pub(crate) mod update_features;

/// Build the dispatch table. Phase E registers concrete handlers; for
/// now this is an empty table so the dispatch loop can still look up.
#[must_use]
pub(crate) fn build_table() -> HandlerTable {
    let mut t = HandlerTable::new();
    // Produce (api_key 0) is intercepted inline in `network::dispatch`
    // (slice-13 T10) so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-topic Write ACL enforcement.
    // Fetch (api_key 1) is intercepted inline in `network::dispatch`
    // (slice-13 T11) so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-topic Read ACL enforcement.
    // Metadata (api_key 3) is intercepted inline in `network::dispatch`
    // (slice-13 T12) so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-topic Describe ACL enforcement.
    // CreateTopics (api_key 19) is intercepted inline in `network::dispatch`
    // (slice-13 T13) so the handler can receive the per-connection
    // principal + peer `SocketAddr` for Cluster Create ACL enforcement.
    // DeleteTopics (api_key 20) is intercepted inline in `network::dispatch`
    // (slice-13 T13) so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-topic Delete ACL enforcement.
    // AlterConfigs (api_key 33) is intercepted inline in `network::dispatch`
    // (slice-13 T14) so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-resource AlterConfigs ACL enforcement.
    // IncrementalAlterConfigs (api_key 44) is intercepted inline in `network::dispatch`
    // (slice-13 T14) so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-resource AlterConfigs ACL enforcement.
    // DeleteRecords (api_key 21) is intercepted inline in `network::dispatch`
    // (slice-13 T15) so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-topic Delete ACL enforcement.
    // CreatePartitions (api_key 37) is intercepted inline in `network::dispatch`
    // (slice-13 T15) so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-topic Alter ACL enforcement.
    // DescribeGroups (api_key 15) is intercepted inline in `network::dispatch`
    // (slice-13 T16) so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-group Describe ACL enforcement.
    // ListGroups (api_key 16) is intercepted inline in `network::dispatch`
    // (slice-13 T16) so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-group Describe ACL enforcement
    // (silent filter — denied groups are omitted, not error-coded).
    // DeleteGroups (api_key 42) is intercepted inline in `network::dispatch`
    // (slice-13 T16) so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-group Delete ACL enforcement.
    // JoinGroup (api_key 11) is intercepted inline in `network::dispatch`
    // (slice-13 T17) so the handler can receive the per-connection
    // principal + peer `SocketAddr` for per-group Read ACL enforcement.
    // OffsetCommit (api_key 8) is intercepted inline in `network::dispatch`
    // (slice-13 T18) so the handler can receive the per-connection
    // principal + peer `SocketAddr` for Group Read + per-topic Read ACL
    // enforcement.
    // OffsetFetch (api_key 9) is intercepted inline in `network::dispatch`
    // (slice-13 T18) so the handler can receive the per-connection
    // principal + peer `SocketAddr` for Group Describe + per-topic Read ACL
    // enforcement (including the fetch-all `topics: None` sentinel).
    // DescribeCluster (api_key 60) is intercepted inline in `network::dispatch`
    // (slice-13 T19) so the handler can receive the per-connection
    // principal + peer `SocketAddr` for Cluster Describe ACL enforcement.
    // AlterUserScramCredentials (api_key 51) is intercepted inline in
    // `network::dispatch` (slice-13 T19) so the handler can receive the
    // per-connection principal + peer `SocketAddr` for Cluster Alter ACL
    // enforcement (replacing the slice-12 super-user-name equality check).
    // InitProducerId (api_key 22) is intercepted inline in `network::dispatch`
    // (slice-13 T20) so the handler can receive the per-connection
    // principal + peer `SocketAddr` for either `Write` on
    // `TransactionalId` (transactional path) or `IdempotentWrite` on
    // `Cluster` (idempotent-only path).
    // AddPartitionsToTxn (api_key 24) is intercepted inline in `network::dispatch`
    // (slice-13 T20) so the handler can receive the per-connection
    // principal + peer `SocketAddr` for `Write` on `TransactionalId` and
    // per-topic `Write` on `Topic` ACL enforcement.
    // EndTxn (api_key 26) is intercepted inline in `network::dispatch`
    // (slice-13 T20) so the handler can receive the per-connection
    // principal + peer `SocketAddr` for `Write` on `TransactionalId` ACL
    // enforcement.
    // TxnOffsetCommit (api_key 28) is intercepted inline in `network::dispatch`
    // (slice-13 T20) so the handler can receive the per-connection
    // principal + peer `SocketAddr` for `Write` on `TransactionalId` +
    // `Read` on `Group` + per-topic `Read` on `Topic` ACL enforcement.
    t.register(2, list_offsets::handle);
    t.register(10, find_coordinator::handle);
    t.register(12, heartbeat::handle);
    t.register(13, leave_group::handle);
    t.register(14, sync_group::handle);
    // 15 (DescribeGroups) intercepted inline — see comment above.
    // 16 (ListGroups) intercepted inline — see comment above.
    t.register(18, api_versions::handle);
    // 21 (DeleteRecords) intercepted inline — see comment above.
    // 22 (InitProducerId) intercepted inline — see comment above.
    t.register(23, offset_for_leader_epoch::handle);
    // 24 (AddPartitionsToTxn) intercepted inline — see comment above.
    t.register(25, crate::txn::handlers::add_offset_commits_to_txn::handle);
    // 26 (EndTxn) intercepted inline — see comment above.
    t.register(27, crate::txn::handlers::write_txn_markers::handle);
    // 28 (TxnOffsetCommit) intercepted inline — see comment above.
    t.register(32, describe_configs::handle);
    // 33 (AlterConfigs) intercepted inline — see comment above.
    t.register(35, describe_log_dirs::handle);
    // 37 (CreatePartitions) intercepted inline — see comment above.
    // 42 (DeleteGroups) intercepted inline — see comment above.
    // 44 (IncrementalAlterConfigs) intercepted inline — see comment above.
    t.register(56, alter_partition::handle);
    // 60 (DescribeCluster) intercepted inline — see comment above.
    t.register(63, broker_heartbeat::handle);
    t.register(68, consumer_group_heartbeat::handle);
    t.register(69, consumer_group_describe::handle);
    // KIP-714 (client metrics push). Both handlers are no-ops: get returns
    // an empty subscription so JVM clients skip push; push silently
    // discards anything that races the subscription re-fetch.
    t.register(71, get_telemetry_subscriptions::handle);
    t.register(72, push_telemetry::handle);
    t
}
