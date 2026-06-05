//! Gateway configuration, parsed from CLI flags / env in `bin/gateway.rs`.

use std::net::SocketAddr;

/// Runtime configuration for the gateway process.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// `host:port,host:port,...` of brokers for bootstrap.
    pub bootstrap: String,
    /// Connect-RPC + HTTP listen address.
    pub listen_addr: SocketAddr,
    /// Base `client.id` for the native clients this gateway opens.
    pub client_id: String,
    /// Internal compacted topic that stores dedup claims.
    pub dedup_topic: String,
    /// Partition count of the dedup topic (also the ownership shard count in P3).
    pub dedup_partitions: u32,
    /// Dedup window: claim-topic `retention.ms` and the dedup guarantee horizon.
    pub dedup_window_ms: i64,
    /// `transactional.id` prefix; the per-partition id is `{prefix}-{p}`.
    pub dedup_txn_id_prefix: String,
    /// Address other replicas reach THIS gateway at (host:port of `listen_addr`,
    /// externally routable). Published to membership; used to forward.
    pub advertised_addr: String,
    /// Internal compacted topic carrying replica membership / owner routing.
    pub membership_topic: String,
}

impl GatewayConfig {
    /// Replication factor requested for the dedup topic at create time.
    /// Kept here so `bin` and tests agree; broker may downgrade.
    pub const DEDUP_TOPIC_REPLICATION: i16 = 3;
    /// Replication factor requested for the membership topic at create time.
    pub const MEMBERSHIP_TOPIC_REPLICATION: i16 = 3;
}
