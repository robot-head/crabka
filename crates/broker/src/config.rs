//! Broker configuration. Built directly (library use) or from CLI flags
//! (binary entry point in `bin/broker.rs`).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use crabka_log::LogConfig;
use crabka_raft::NodeId;
use crabka_security::{ListenerProtocol, SaslMechanism, TlsConfig};

use crate::BrokerError;

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

    /// Cluster UUID forwarded to `ControllerConfig::cluster_id`. Sourced
    /// from the operator (the `KafkaCluster` UID) via `--cluster-id`.
    /// `None` defaults to `Uuid::nil()` inside `Controller::start`.
    pub cluster_id: Option<uuid::Uuid>,

    // ── Auth / listener registry (Task 7+) ──────────────────────────────
    /// Named listener definitions. When empty, `effective_listeners()` synthesizes
    /// a single PLAINTEXT listener from `listen_addr` + `advertised_listener`,
    /// preserving full backward compatibility.
    pub listeners: Vec<ListenerSpec>,

    /// Protocol terminator for the controller listener. Default
    /// `Plaintext` preserves the legacy raw-TCP raft transport.
    /// Set to `SaslPlaintext` / `Ssl` / `SaslSsl` to require auth
    /// on inbound raft RPCs (and outbound, when paired with
    /// `inter_broker_credentials`).
    pub controller_listener_protocol: crabka_security::ListenerProtocol,

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

    /// Usernames that bypass ACL checks (super-users).
    pub super_users: std::collections::HashSet<String>,

    /// TLS configuration. `None` — no TLS (slice 12 default).
    pub tls_config: Option<TlsConfig>,

    /// Which SASL mechanisms are enabled. Empty → no SASL.
    pub enabled_sasl_mechanisms: Vec<SaslMechanism>,

    /// KIP-460 auto preferred-replica election. When true, a background
    /// task on the controller leader periodically scans partitions and
    /// re-elects the preferred replica as leader when it's alive + in
    /// ISR. Matches Kafka's `auto.leader.rebalance.enable`.
    pub auto_leader_rebalance_enable: bool,

    /// How often the auto-rebalance ticker fires, in seconds. Default
    /// 300 (5 minutes). Matches Kafka's
    /// `leader.imbalance.check.interval.seconds`.
    pub leader_imbalance_check_interval_secs: u64,

    /// Minimum percentage of imbalanced partitions before the
    /// auto-rebalance ticker submits any changes. Default 10. Matches
    /// Kafka's `leader.imbalance.per.broker.percentage`.
    pub leader_imbalance_per_broker_percentage: u32,

    /// Test-only: override the cleaner ticker interval.
    /// Production callers leave this as `None` (default 30s).
    #[cfg(any(test, feature = "test-helpers"))]
    pub cleaner_interval_override: Option<std::time::Duration>,

    /// Slice 33: how often the TLS reload watcher polls cert / key /
    /// client-CA file mtimes and rebuilds the `ServerConfig` if any
    /// changed. Defaults to 30s. Set lower in tests to keep watcher
    /// latency tight. `Duration::ZERO` disables the periodic watcher
    /// — callers can still trigger an immediate reload via
    /// [`crate::BrokerHandle::reload_tls`].
    pub tls_reload_interval: std::time::Duration,

    /// Slice 39: bind address for the Prometheus `/metrics` HTTP
    /// endpoint. `None` disables the server entirely (the broker still
    /// updates its internal counters, but nothing scrapes them).
    /// Defaults to `Some(0.0.0.0:9404)` in production (the same port
    /// the JMX exporter uses for vanilla Kafka), `None` in
    /// `for_tests` so unit tests don't fight over port allocation.
    pub metrics_listen_addr: Option<SocketAddr>,
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
            cluster_id: None,
            listeners: vec![],
            controller_listener_protocol: crabka_security::ListenerProtocol::Plaintext,
            inter_broker_listener_name: "PLAINTEXT".to_string(),
            inter_broker_credentials: None,
            plain_credentials: HashMap::new(),
            super_users: std::collections::HashSet::new(),
            tls_config: None,
            enabled_sasl_mechanisms: vec![],
            auto_leader_rebalance_enable: false, // tests opt in explicitly
            leader_imbalance_check_interval_secs: 300,
            leader_imbalance_per_broker_percentage: 10,
            #[cfg(any(test, feature = "test-helpers"))]
            cleaner_interval_override: None,
            // Short interval so hot-reload tests don't wait long for a
            // watcher tick. Tests that don't care can ignore it.
            tls_reload_interval: std::time::Duration::from_millis(200),
            // Tests opt into the metrics endpoint individually by
            // setting this to `Some(127.0.0.1:0)`; sharing a default
            // port would race in parallel test runs.
            metrics_listen_addr: None,
        }
    }

    /// Validate the listener and auth configuration.
    ///
    /// Called by [`crate::Broker::start`] before any side effects so
    /// mis-configurations surface immediately with a descriptive error rather
    /// than at first connection.
    ///
    /// # Errors
    ///
    /// Returns `Err` when:
    /// - Two listeners share the same `bind_addr`.
    /// - `inter_broker_listener_name` does not match any listener name.
    /// - A SASL listener is declared while `enabled_sasl_mechanisms` is empty.
    pub fn validate(&self) -> Result<(), BrokerError> {
        let listeners = self.effective_listeners();

        // Bind-address collisions.
        for i in 0..listeners.len() {
            for j in (i + 1)..listeners.len() {
                if listeners[i].bind_addr == listeners[j].bind_addr {
                    return Err(BrokerError::ListenerConflict {
                        a: listeners[i].name.clone(),
                        b: listeners[j].name.clone(),
                    });
                }
            }
        }

        // Inter-broker listener must exist.
        if !listeners
            .iter()
            .any(|l| l.name == self.inter_broker_listener_name)
        {
            return Err(BrokerError::InvalidInterBrokerListener {
                name: self.inter_broker_listener_name.clone(),
            });
        }

        // Every SASL listener requires at least one mechanism.
        for l in &listeners {
            if l.protocol.requires_sasl() && self.enabled_sasl_mechanisms.is_empty() {
                return Err(BrokerError::SaslListenerNoMechanisms {
                    name: l.name.clone(),
                });
            }
        }

        let cp = self.controller_listener_protocol;
        if cp.requires_tls() && self.tls_config.is_none() {
            return Err(BrokerError::Tls(
                "controller_listener_protocol requires TLS but tls_config is None".into(),
            ));
        }
        if cp.requires_sasl() && self.enabled_sasl_mechanisms.is_empty() {
            return Err(BrokerError::SaslListenerNoMechanisms {
                name: "controller".into(),
            });
        }

        if self.leader_imbalance_check_interval_secs == 0 {
            return Err(BrokerError::InvalidLeaderRebalanceInterval { value: 0 });
        }
        if self.leader_imbalance_per_broker_percentage > 100 {
            return Err(BrokerError::InvalidLeaderRebalanceThreshold {
                value: self.leader_imbalance_per_broker_percentage,
            });
        }

        Ok(())
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
            cluster_id: None,
            listeners: vec![],
            controller_listener_protocol: crabka_security::ListenerProtocol::Plaintext,
            inter_broker_listener_name: "PLAINTEXT".to_string(),
            inter_broker_credentials: None,
            plain_credentials: HashMap::new(),
            super_users: std::collections::HashSet::new(),
            tls_config: None,
            enabled_sasl_mechanisms: vec![],
            auto_leader_rebalance_enable: true,
            leader_imbalance_check_interval_secs: 300,
            leader_imbalance_per_broker_percentage: 10,
            #[cfg(any(test, feature = "test-helpers"))]
            cleaner_interval_override: None,
            tls_reload_interval: std::time::Duration::from_secs(30),
            metrics_listen_addr: Some("0.0.0.0:9404".parse().expect("static metrics addr parses")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BrokerError as BrokerStartError;

    /// A well-formed two-listener config used as the base for validation
    /// tests.
    fn base() -> BrokerConfig {
        BrokerConfig {
            listeners: vec![
                ListenerSpec {
                    name: "INTERNAL".to_string(),
                    bind_addr: "127.0.0.1:9093".parse().unwrap(),
                    advertised: "127.0.0.1:9093".to_string(),
                    protocol: ListenerProtocol::Plaintext,
                },
                ListenerSpec {
                    name: "EXTERNAL".to_string(),
                    bind_addr: "0.0.0.0:9092".parse().unwrap(),
                    advertised: "host.docker.internal:9092".to_string(),
                    protocol: ListenerProtocol::SaslSsl,
                },
            ],
            inter_broker_listener_name: "INTERNAL".to_string(),
            enabled_sasl_mechanisms: vec![SaslMechanism::Plain, SaslMechanism::ScramSha512],
            ..BrokerConfig::default()
        }
    }

    #[test]
    fn rejects_bind_collision() {
        let mut c = base();
        c.listeners[1].bind_addr = c.listeners[0].bind_addr;
        assert!(matches!(
            c.validate(),
            Err(BrokerStartError::ListenerConflict { .. })
        ));
    }

    #[test]
    fn rejects_missing_inter_broker_listener() {
        let mut c = base();
        c.inter_broker_listener_name = "NONESUCH".to_string();
        assert!(matches!(
            c.validate(),
            Err(BrokerStartError::InvalidInterBrokerListener { .. })
        ));
    }

    #[test]
    fn rejects_sasl_listener_without_mechanisms() {
        let mut c = base();
        c.enabled_sasl_mechanisms.clear();
        assert!(c.validate().is_err());
    }

    #[test]
    fn legacy_default_passes() {
        let c = BrokerConfig::default();
        c.validate().expect("legacy default must validate");
    }

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

    #[test]
    fn rejects_controller_tls_without_config() {
        let c = BrokerConfig {
            controller_listener_protocol: ListenerProtocol::Ssl,
            tls_config: None,
            ..BrokerConfig::default()
        };
        assert!(matches!(c.validate(), Err(BrokerError::Tls(_))));
    }

    #[test]
    fn rejects_controller_sasl_without_mechanisms() {
        let c = BrokerConfig {
            controller_listener_protocol: ListenerProtocol::SaslPlaintext,
            enabled_sasl_mechanisms: vec![],
            ..BrokerConfig::default()
        };
        assert!(matches!(
            c.validate(),
            Err(BrokerError::SaslListenerNoMechanisms { .. })
        ));
    }

    #[test]
    fn legacy_default_still_passes() {
        BrokerConfig::default()
            .validate()
            .expect("legacy default validates");
    }

    #[test]
    fn auto_leader_rebalance_defaults_to_true_in_default() {
        let c = BrokerConfig::default();
        assert!(c.auto_leader_rebalance_enable);
        assert_eq!(c.leader_imbalance_check_interval_secs, 300);
        assert_eq!(c.leader_imbalance_per_broker_percentage, 10);
    }

    #[test]
    fn auto_leader_rebalance_defaults_to_false_in_for_tests() {
        let c = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        assert!(!c.auto_leader_rebalance_enable);
    }

    #[test]
    fn rebalance_zero_interval_rejected_by_validate() {
        let c = BrokerConfig {
            leader_imbalance_check_interval_secs: 0,
            ..BrokerConfig::default()
        };
        assert!(matches!(
            c.validate(),
            Err(BrokerError::InvalidLeaderRebalanceInterval { value: 0 })
        ));
    }

    #[test]
    fn rebalance_threshold_over_100_rejected_by_validate() {
        let c = BrokerConfig {
            leader_imbalance_per_broker_percentage: 101,
            ..BrokerConfig::default()
        };
        assert!(matches!(
            c.validate(),
            Err(BrokerError::InvalidLeaderRebalanceThreshold { value: 101 })
        ));
    }
}
