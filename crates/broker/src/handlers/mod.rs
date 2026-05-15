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

pub(crate) mod acl_wire;
pub(crate) mod alter_configs;
pub(crate) mod alter_partition;
pub(crate) mod alter_user_scram_credentials;
pub(crate) mod api_versions;
pub(crate) mod broker_heartbeat;
pub(crate) mod create_acls;
pub(crate) mod create_partitions;
pub(crate) mod create_topics;
pub(crate) mod delete_acls;
pub(crate) mod delete_groups;
pub(crate) mod delete_records;
pub(crate) mod delete_topics;
pub(crate) mod describe_acls;
pub(crate) mod describe_cluster;
pub(crate) mod describe_configs;
pub(crate) mod describe_groups;
pub(crate) mod fetch;
pub(crate) mod find_coordinator;
pub(crate) mod heartbeat;
pub(crate) mod incremental_alter_configs;
pub(crate) mod init_producer_id;
pub(crate) mod join_group;
pub(crate) mod leave_group;
pub(crate) mod list_groups;
pub(crate) mod list_offsets;
pub(crate) mod metadata;
pub(crate) mod offset_commit;
pub(crate) mod offset_fetch;
pub(crate) mod offset_for_leader_epoch;
pub(crate) mod produce;
pub(crate) mod sync_group;

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
    t.register(2, list_offsets::handle);
    t.register(8, offset_commit::handle);
    t.register(9, offset_fetch::handle);
    t.register(10, find_coordinator::handle);
    t.register(11, join_group::handle);
    t.register(12, heartbeat::handle);
    t.register(13, leave_group::handle);
    t.register(14, sync_group::handle);
    t.register(15, describe_groups::handle);
    t.register(16, list_groups::handle);
    t.register(18, api_versions::handle);
    // 21 (DeleteRecords) intercepted inline — see comment above.
    t.register(22, init_producer_id::handle);
    t.register(23, offset_for_leader_epoch::handle);
    t.register(24, crate::txn::handlers::add_partitions_to_txn::handle);
    t.register(25, crate::txn::handlers::add_offset_commits_to_txn::handle);
    t.register(26, crate::txn::handlers::end_txn::handle);
    t.register(27, crate::txn::handlers::write_txn_markers::handle);
    t.register(28, crate::txn::handlers::txn_offset_commit::handle);
    t.register(32, describe_configs::handle);
    // 33 (AlterConfigs) intercepted inline — see comment above.
    // 37 (CreatePartitions) intercepted inline — see comment above.
    t.register(42, delete_groups::handle);
    // 44 (IncrementalAlterConfigs) intercepted inline — see comment above.
    t.register(56, alter_partition::handle);
    t.register(60, describe_cluster::handle);
    t.register(63, broker_heartbeat::handle);
    t
}
