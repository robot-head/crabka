//! Broker configuration. Built directly (library use) or from CLI flags
//! (binary entry point in `bin/broker.rs`).

use std::net::SocketAddr;
use std::path::PathBuf;

use crabka_log::LogConfig;
use crabka_raft::NodeId;

/// Construction-time configuration for [`crate::Broker::start`].
///
/// Build directly when embedding the broker as a library, or via the
/// `crabka-broker` binary's clap CLI in production.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    /// Broker id reported in `Metadata` responses. Default: 1.
    pub broker_id: i32,

    /// TCP address to listen on. Default: `127.0.0.1:9092`.
    pub listen_addr: SocketAddr,

    /// `host:port` returned in `Metadata` responses as this broker's
    /// advertised endpoint. Defaults to `listen_addr`'s string form.
    pub advertised_listener: String,

    /// Directory containing one `<topic>-<partition>/` per partition.
    /// Created on startup if missing. Default: `./crabka-data`.
    pub log_dir: PathBuf,

    /// Per-log configuration applied to every partition this broker hosts.
    pub log_config: LogConfig,

    /// Raft node id. Conventionally equal to `broker_id as NodeId`.
    pub node_id: NodeId,

    /// Address the controller listener binds on. `KRaft` convention: same
    /// host as `listen_addr`, port 9093. Test default: `127.0.0.1:0`.
    pub controller_listen_addr: SocketAddr,

    /// Static voter set: `[(node_id, controller_addr), …]`. Defaults to
    /// a single-voter cluster of just this broker, so existing
    /// slice-1..6 tests upgrade to quorum-of-1 without config changes.
    pub controller_quorum_voters: Vec<(NodeId, SocketAddr)>,

    /// How often each broker sends `BrokerHeartbeat` to the controller
    /// leader. Default 3,000ms.
    pub heartbeat_interval_ms: u64,
    /// Controller marks a broker dead after this many ms without a
    /// heartbeat. Default 9,000ms.
    pub heartbeat_timeout_ms: u64,
    /// Leader proposes ISR shrink when a follower lags more than this
    /// many ms. Default 30,000ms.
    pub replica_lag_time_max_ms: u64,
}

impl BrokerConfig {
    /// Helpful for tests: a config that listens on an OS-assigned port
    /// under a tempdir.
    #[must_use]
    pub fn for_tests(log_dir: PathBuf) -> Self {
        let listen_addr: SocketAddr = "127.0.0.1:0".parse().expect("static");
        let controller_addr: SocketAddr = "127.0.0.1:0".parse().expect("static");
        Self {
            broker_id: 1,
            listen_addr,
            advertised_listener: "127.0.0.1:0".into(),
            log_dir,
            log_config: LogConfig::default(),
            node_id: 1,
            controller_listen_addr: controller_addr,
            controller_quorum_voters: vec![(1, controller_addr)],
            heartbeat_interval_ms: 200,
            heartbeat_timeout_ms: 2_000,
            replica_lag_time_max_ms: 2_000,
        }
    }
}

impl Default for BrokerConfig {
    fn default() -> Self {
        let addr: SocketAddr = "127.0.0.1:9092".parse().expect("hard-coded valid addr");
        let controller_addr: SocketAddr = "127.0.0.1:9093".parse().expect("hard-coded valid addr");
        Self {
            broker_id: 1,
            listen_addr: addr,
            advertised_listener: addr.to_string(),
            log_dir: PathBuf::from("./crabka-data"),
            log_config: LogConfig::default(),
            node_id: 1,
            controller_listen_addr: controller_addr,
            controller_quorum_voters: vec![(1, controller_addr)],
            heartbeat_interval_ms: 3_000,
            heartbeat_timeout_ms: 9_000,
            replica_lag_time_max_ms: 30_000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_listen_on_localhost_9092() {
        let c = BrokerConfig::default();
        assert_eq!(c.listen_addr.port(), 9092);
        assert_eq!(c.broker_id, 1);
    }

    #[test]
    fn for_tests_uses_port_0() {
        let c = BrokerConfig::for_tests(PathBuf::from("/tmp"));
        assert_eq!(c.listen_addr.port(), 0);
    }
}
