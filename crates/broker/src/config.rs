//! Broker configuration. Built directly (library use) or from CLI flags
//! (binary entry point in `bin/broker.rs`).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use crabka_log::LogConfig;
use crabka_raft::NodeId;
use crabka_security::{ListenerProtocol, SaslMechanism, TlsConfig};

pub use crabka_raft::BootstrapMode;

/// A single named listener: the port the broker binds + what it tells clients.
#[derive(Debug, Clone)]
pub struct ListenerSpec {
    /// Listener name (e.g. `"PLAINTEXT"`, `"SSL"`, `"SASL_SSL"`).
    pub name: String,
    /// Local address to bind.
    pub bind_addr: SocketAddr,
    /// `host:port` advertised to clients in `Metadata` responses.
    pub advertised: String,
    /// Wire protocol (Plaintext / Ssl / `SaslPlaintext` / `SaslSsl`).
    pub protocol: ListenerProtocol,
}

/// Credentials the broker uses when connecting *to* other brokers.
#[derive(Debug, Clone)]
pub struct InterBrokerCredentials {
    pub mechanism: SaslMechanism,
    pub username: String,
    pub password: String,
}

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

    /// Openraft election timeout (sets `election_timeout_min`; max is 2×).
    /// Indirectly sets `leader_lease = election_timeout_max` inside
    /// openraft's engine — peers refuse to grant a new leader's vote
    /// until the lease expires, so this is also the lower bound on how
    /// fast a 3-broker cluster can recover from a dead controller leader.
    /// Default 5s (conservative; avoids split-vote on slow runners).
    pub controller_election_timeout: std::time::Duration,

    /// Openraft heartbeat interval. Default 500ms. Should be ≤
    /// `controller_election_timeout / 3` per raft consensus norms.
    pub controller_heartbeat_interval: std::time::Duration,

    /// How this broker participates in cluster formation. See
    /// [`crabka_raft::BootstrapMode`] for the trade-offs. The first broker
    /// of a fresh multi-broker cluster uses `Bootstrap`; subsequent brokers
    /// use `Join`; a restart of any previously-formatted broker uses
    /// `Rejoin`. Single-broker setups always use `Bootstrap`.
    pub bootstrap_mode: BootstrapMode,

    // ── Auth / listener registry (Task 7+) ──────────────────────────────
    /// Named listener definitions. When empty, `effective_listeners()` synthesizes
    /// a single PLAINTEXT listener from `listen_addr` + `advertised_listener`,
    /// preserving full backward compatibility.
    pub listeners: Vec<ListenerSpec>,

    /// Name of the listener used for inter-broker traffic (raft, replication,
    /// heartbeat). Must match a name in `listeners` when `listeners` is
    /// non-empty. Default: `"PLAINTEXT"`.
    pub inter_broker_listener_name: String,

    /// Credentials the broker uses for outbound inter-broker connections.
    /// `None` means no SASL — plaintext inter-broker traffic (slice 12 default).
    pub inter_broker_credentials: Option<InterBrokerCredentials>,

    /// Static PLAIN credentials: username → password.  Empty by default
    /// (PLAIN auth disabled until mechanisms are explicitly enabled).
    pub plain_credentials: HashMap<String, String>,

    /// When set, this username bypasses ACL checks (super-user).
    pub super_user_name: Option<String>,

    /// TLS configuration. `None` — no TLS (slice 12 default).
    pub tls_config: Option<TlsConfig>,

    /// Which SASL mechanisms are enabled. Empty → no SASL.
    pub enabled_sasl_mechanisms: Vec<SaslMechanism>,
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
            // Short timings: single-node tests don't need quorum so split-vote
            // isn't a risk; multi-broker tests use these (via the shared
            // `support::start_n_node_with_retry` helper) so failover from a
            // dead controller leader completes well under the producer's
            // 10s timeout. The factor of ~10× vs. production defaults
            // is what makes `acks_all_completes_via_isr_shrink_when_follower_dead`
            // pass within its 5s assertion window.
            controller_election_timeout: std::time::Duration::from_millis(500),
            controller_heartbeat_interval: std::time::Duration::from_millis(100),
            bootstrap_mode: BootstrapMode::Bootstrap,
            listeners: vec![],
            inter_broker_listener_name: "PLAINTEXT".to_string(),
            inter_broker_credentials: None,
            plain_credentials: HashMap::new(),
            super_user_name: None,
            tls_config: None,
            enabled_sasl_mechanisms: vec![],
        }
    }

    /// Returns the effective listener list.
    ///
    /// When [`listeners`][Self::listeners] is empty (the pre-Task-7 default),
    /// synthesizes a single `PLAINTEXT` listener from the legacy
    /// `listen_addr` + `advertised_listener` fields so all existing code
    /// continues to work without changes.
    #[must_use]
    pub fn effective_listeners(&self) -> Vec<ListenerSpec> {
        if !self.listeners.is_empty() {
            return self.listeners.clone();
        }
        vec![ListenerSpec {
            name: "PLAINTEXT".to_string(),
            bind_addr: self.listen_addr,
            advertised: self.advertised_listener.clone(),
            protocol: ListenerProtocol::Plaintext,
        }]
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
            controller_election_timeout: std::time::Duration::from_secs(5),
            controller_heartbeat_interval: std::time::Duration::from_millis(500),
            bootstrap_mode: BootstrapMode::Bootstrap,
            listeners: vec![],
            inter_broker_listener_name: "PLAINTEXT".to_string(),
            inter_broker_credentials: None,
            plain_credentials: HashMap::new(),
            super_user_name: None,
            tls_config: None,
            enabled_sasl_mechanisms: vec![],
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

    #[test]
    fn defaults_use_conservative_raft_timings() {
        let c = BrokerConfig::default();
        assert_eq!(
            c.controller_election_timeout,
            std::time::Duration::from_secs(5)
        );
        assert_eq!(
            c.controller_heartbeat_interval,
            std::time::Duration::from_millis(500)
        );
    }

    #[test]
    fn for_tests_uses_short_raft_timings_for_fast_failover() {
        let c = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        // Short enough that a 3-broker test can detect a dead leader and
        // re-elect within a few hundred ms — the deferred slice-10b tests
        // need failover well under their 10s producer timeout.
        assert!(c.controller_election_timeout <= std::time::Duration::from_millis(750));
        assert!(c.controller_heartbeat_interval <= std::time::Duration::from_millis(200));
    }

    #[test]
    fn defaults_use_bootstrap_mode() {
        let c = BrokerConfig::default();
        assert_eq!(c.bootstrap_mode, BootstrapMode::Bootstrap);
    }

    #[test]
    fn for_tests_uses_bootstrap_mode() {
        let c = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        assert_eq!(c.bootstrap_mode, BootstrapMode::Bootstrap);
    }
}
