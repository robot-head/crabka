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
    /// Per-listener TLS material. When `Some`, overrides the top-level
    /// `BrokerConfig::tls_config` for this listener's accept loop.
    pub tls_config: Option<TlsConfig>,
    /// SASL mechanisms enabled on this listener. When `Some`, overrides
    /// the top-level `BrokerConfig::enabled_sasl_mechanisms`.
    pub sasl_mechanisms: Option<Vec<SaslMechanism>>,
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

    /// Primary log directory. Holds the `__cluster_metadata` raft log and
    /// is used for bootstrap-mode detection. Also a data directory: when
    /// [`extra_log_dirs`][Self::extra_log_dirs] is empty this is the only
    /// place partition data lives. Created on startup if missing.
    /// Default: `./crabka-data`.
    pub log_dir: PathBuf,

    /// Additional JBOD data directories (KIP-113). When non-empty, new
    /// partitions are spread across `[log_dir] + extra_log_dirs` by
    /// least-loaded placement; `__cluster_metadata` always stays on
    /// [`log_dir`][Self::log_dir]. Maps to Kafka's `log.dirs` having more
    /// than one entry. Default: empty (single-directory broker).
    pub extra_log_dirs: Vec<PathBuf>,

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

    /// Usernames that bypass ACL checks (super-users). Slice 51's
    /// `create_delegation_token` act-as gate reads this directly; the
    /// active [`crate::authorizer::Authorizer`] impl also reads it
    /// (`SimpleAclAuthorizer` / `OpaAuthorizer`). Both are populated
    /// from the same `[authorization]` TOML stanza by `file_config`.
    pub super_users: std::collections::HashSet<String>,

    /// Slice 53: pluggable cluster authorizer. One boxed instance per
    /// broker; configured via `[authorization]` in `broker.toml`. The
    /// default is [`crate::authorizer::AllowAllAuthorizer`] — explicit
    /// "allow everything" — which replaces the slice-13
    /// "no super-users + no ACLs ⇒ Allow" compat shim that previously
    /// lived inside the ACL impl.
    pub authorizer: std::sync::Arc<dyn crate::authorizer::Authorizer>,

    /// TLS configuration. `None` — no TLS (slice 12 default).
    pub tls_config: Option<TlsConfig>,

    /// Which SASL mechanisms are enabled. Empty → no SASL.
    pub enabled_sasl_mechanisms: Vec<SaslMechanism>,

    /// Slice 49 / 49b: validator for SASL/OAUTHBEARER bearer tokens. Only
    /// consulted when `OAuthBearer` is in `enabled_sasl_mechanisms` (the
    /// handshake won't advertise it otherwise). Defaults to the unsecured-JWS
    /// validator with principal claim `sub`; configuring a JWKS endpoint
    /// (`[oauthbearer].jwks_endpoint_uri`) selects the signed-JWT validator.
    pub oauthbearer_validator: crabka_security::OAuthBearerValidator,

    /// Slice 49b: JWKS endpoint to fetch OAUTHBEARER signing keys from. `Some`
    /// only when `oauthbearer_validator` is the signed variant. When set,
    /// `Broker::start` spawns a background refresher that fetches this URL and
    /// rotates the validator's key set on `oauthbearer_jwks_refresh_interval`.
    pub oauthbearer_jwks_endpoint: Option<String>,

    /// Slice 49b: how often to re-fetch the JWKS. Default 5 minutes.
    pub oauthbearer_jwks_refresh_interval: std::time::Duration,

    /// Slice 49c (renamed in 49d): optional PEM path for outbound
    /// HTTPS to the `IdP`. Shared across JWKS, introspection, and
    /// userinfo. None → reqwest's default webpki-roots.
    pub oauthbearer_idp_tls_trust: Option<std::path::PathBuf>,

    /// Slice 50d: optional ceiling on OAUTHBEARER session lifetime, in
    /// seconds. When set, the broker reports
    /// `session_lifetime_ms = min(token_exp_ms - now_ms, cap * 1000)`
    /// and the dispatch-loop re-auth timer fires at the clamped time.
    /// When unset, sessions last until the token's natural `exp`
    /// (slice 49e default).
    pub oauthbearer_max_session_lifetime_seconds: Option<u32>,

    /// Slice 49i: receiver half of the JWKS refresher signal channel.
    /// `apply_to` creates the channel pair: the sender is wired into the
    /// signed validator's `JwksHandle`; the receiver is parked here for
    /// `Broker::start` to `take()` and pass to `JwksRefresher`. `None`
    /// when JWKS validation isn't configured. `Arc<Mutex<…>>` so the
    /// containing `BrokerConfig` can stay `Clone`; only `Broker::start`
    /// `.lock().take()`s the receiver, and there is only ever one
    /// `Broker::start` per validator construction.
    pub oauthbearer_jwks_signal_rx:
        std::sync::Arc<std::sync::Mutex<Option<tokio::sync::mpsc::Receiver<()>>>>,

    /// Slice 49i: shared timestamp of the last successful JWKS fetch.
    /// `apply_to` creates it (`AtomicI64::new(0)`); the validator's
    /// `JwksHandle` and the refresher both clone this `Arc` so the
    /// refresher's writes are visible to the validator's expiry check.
    pub oauthbearer_jwks_last_successful_fetch_ms: std::sync::Arc<std::sync::atomic::AtomicI64>,

    /// Slice 49i: shared on-demand-refresh timestamp for rate-limiting.
    /// `apply_to` creates it; `Broker::start` hands a clone to the
    /// refresher. The validator never reads this — it's refresher-only
    /// bookkeeping carried through `BrokerConfig` for symmetry.
    pub oauthbearer_jwks_last_on_demand_refresh_ms: std::sync::Arc<std::sync::atomic::AtomicI64>,

    /// Slice 49i: minimum pause between on-demand JWKS refreshes
    /// triggered by validator signals. `apply_to` sets this from
    /// `FileOAuthBearerConfig::jwks_min_refresh_pause_seconds`;
    /// `Broker::start` passes it into `JwksRefresher`. Strimzi default
    /// 1 second; we default to 1 second too.
    pub oauthbearer_jwks_min_on_demand_pause: std::time::Duration,

    /// Slice 49i: when true, the refresher's JWKS parser keeps keys
    /// regardless of `use` value (default behavior filters out `use=enc`).
    pub oauthbearer_jwks_ignore_key_use: bool,

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

    /// KIP-227: maximum number of incremental-fetch sessions kept in the
    /// per-broker cache. Each session tracks the (topic, partition) set a
    /// client is subscribed to so subsequent fetches can be deltas. When
    /// full, a non-privileged (consumer) session is evicted LRU; privileged
    /// (follower-fetch) sessions are evicted only by other privileged
    /// sessions. Matches Apache Kafka's `max.incremental.fetch.session.cache.slots`
    /// (default 1000).
    pub max_incremental_fetch_session_cache_slots: usize,

    /// Slice 43e: partition disk-usage scan cadence, in seconds. `0`
    /// disables the scanner entirely (no background task spawned).
    /// Production default: 60s. The scanner walks every known
    /// (topic, partition) under `log_dir` each tick, sums regular-file
    /// sizes, and updates the `partition_disk_bytes` gauge consumed by
    /// the rebalancer's usage scraper.
    pub partition_disk_scan_interval_secs: u64,

    /// Slice 51 (KIP-48): HMAC-SHA-256 master key used to mint + verify
    /// delegation tokens. When `None`, the broker rejects all four
    /// delegation-token RPCs with `DELEGATION_TOKEN_AUTH_DISABLED` and
    /// SCRAM cannot fall back to token lookup. Sourced from
    /// `CRABKA_DELEGATION_TOKEN_SECRET_KEY` (env wins) or
    /// `[delegation_token] secret_key` in `broker.toml`. Wrapped in
    /// `SecretBytes` so `Debug` redacts the bytes.
    pub delegation_token_secret_key: Option<crabka_security::SecretBytes>,

    /// Slice 51 (KIP-48): hard upper bound on delegation-token lifetime,
    /// in milliseconds. A token's `max_timestamp_ms` is set to
    /// `issue_timestamp_ms + delegation_token_max_lifetime_ms` and the
    /// renew handler clamps any caller-requested expiry to this. Default
    /// 7 days (`delegation.token.max.lifetime.ms` in Kafka).
    pub delegation_token_max_lifetime_ms: i64,

    /// Slice 51 (KIP-48): cadence of the background sweep task that
    /// proposes `V1DeleteDelegationToken` tombstones for tokens whose
    /// `expiry_timestamp_ms` or `max_timestamp_ms` is in the past. Default
    /// 1 hour (`delegation.token.expiry.check.interval.ms` in Kafka).
    pub delegation_token_expiry_check_interval_ms: i64,

    /// Slice 51 (KIP-48): default renew period used as the *initial*
    /// `expiry_timestamp_ms` offset at create time, and as the implicit
    /// renew period when `RenewDelegationToken.renew_period_ms == -1`.
    /// Distinct from `delegation_token_max_lifetime_ms` (the absolute
    /// ceiling that `expiry_timestamp_ms` can never be pushed past via
    /// `Renew`): a fresh token gets `expiry_timestamp_ms = now +
    /// min(default_renew_period, chosen_max_lifetime)` while
    /// `max_timestamp_ms = now + chosen_max_lifetime`. Default 24 hours
    /// (`delegation.token.expiry.time.ms` in Kafka).
    pub delegation_token_default_renew_period_ms: i64,

    /// Slice 48b (KIP-405): tiered-storage backend selection. `Some(_)`
    /// enables tiered storage broker-wide (collapsing Kafka's
    /// `remote.log.storage.system.enable` plus the RSM selection into one
    /// knob) and spawns the `RemoteLogManager` copy task; per-topic
    /// offload is still gated by `remote.storage.enable`. `None`
    /// (default) leaves tiered storage off.
    ///
    /// TOML:
    /// - Local: `[remote_storage] storage_dir = "..."`
    /// - S3:    `[remote_storage.s3] bucket = "..." region = "..."`
    pub remote_storage_backend: Option<RemoteStorageBackend>,

    /// Slice 48b (KIP-405): tick cadence of the `RemoteLogManager` copy /
    /// retention task. Defaults to 30s (Kafka's
    /// `remote.log.manager.task.interval.ms`). Acceptance tests lower this
    /// so segments are tiered and locally evicted in seconds rather than
    /// minutes; production deployments leave it at the default.
    pub remote_log_manager_interval: std::time::Duration,

    /// Slice 48f (KIP-405): which
    /// [`RemoteLogMetadataManager`](crabka_remote_storage::RemoteLogMetadataManager)
    /// implementation to construct when tiered storage is enabled.
    /// `None` (default) keeps the in-memory fixture used by every
    /// 48a-48e test path; `Some(KafkaRlmmConfig)` swaps in the
    /// production [`crabka_remote_storage_topic::TopicBasedRemoteLogMetadataManager`]
    /// backed by `__remote_log_metadata`.
    pub remote_log_metadata_kafka: Option<KafkaRlmmConfig>,
}

/// Slice 48f: parameters for the topic-backed
/// [`RemoteLogMetadataManager`](crabka_remote_storage::RemoteLogMetadataManager).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaRlmmConfig {
    /// `host:port` the manager dials to reach its own broker (loopback
    /// in a single-broker setup, the inter-broker listener in a
    /// multi-broker setup).
    pub bootstrap: String,
    /// Partition count to create `__remote_log_metadata` with on first
    /// startup. Ignored when the topic already exists.
    pub num_partitions: i32,
    /// Replication factor to create `__remote_log_metadata` with on
    /// first startup. Ignored when the topic already exists.
    pub replication: i32,
}

/// What backs the broker's `RemoteStorageManager` when tiered storage is on.
#[derive(Debug, Clone)]
pub enum RemoteStorageBackend {
    /// Filesystem-backed `LocalTieredStorage`. Useful for tests, single-
    /// node dev setups, and shared-filesystem multi-broker deployments.
    Local {
        /// Root directory for the segment store.
        dir: PathBuf,
    },
    /// S3-compatible `S3RemoteStorage`. Production backend; works with
    /// AWS S3, `MinIO`, Cloudflare R2, and GCS via S3 compatibility.
    S3(crabka_remote_storage::S3Config),
}

/// Slice 51 (KIP-48): default hard upper bound on delegation-token lifetime.
/// 7 days, matches Kafka's `delegation.token.max.lifetime.ms` default.
pub const DEFAULT_DELEGATION_TOKEN_MAX_LIFETIME_MS: i64 = 7 * 24 * 60 * 60 * 1_000;

/// Slice 51 (KIP-48): default cadence of the background expiry sweep task.
/// 1 hour, matches Kafka's `delegation.token.expiry.check.interval.ms`.
pub const DEFAULT_DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS: i64 = 60 * 60 * 1_000;

/// Slice 51 (KIP-48): default renew period used as the initial
/// `expiry_timestamp_ms` offset at create time, and as the implicit
/// renew period when `RenewDelegationToken.renew_period_ms == -1`.
/// 24 hours, matches Kafka's `delegation.token.expiry.time.ms` default.
pub const DEFAULT_DELEGATION_TOKEN_RENEW_PERIOD_MS: i64 = 24 * 60 * 60 * 1_000;

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
            extra_log_dirs: Vec::new(),
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
            authorizer: std::sync::Arc::new(crate::authorizer::AllowAllAuthorizer),
            tls_config: None,
            enabled_sasl_mechanisms: vec![],
            oauthbearer_validator: crabka_security::OAuthBearerValidator::default(),
            oauthbearer_jwks_endpoint: None,
            oauthbearer_jwks_refresh_interval: std::time::Duration::from_mins(5),
            oauthbearer_idp_tls_trust: None,
            oauthbearer_max_session_lifetime_seconds: None,
            oauthbearer_jwks_signal_rx: std::sync::Arc::new(std::sync::Mutex::new(None)),
            oauthbearer_jwks_last_successful_fetch_ms: std::sync::Arc::new(
                std::sync::atomic::AtomicI64::new(0),
            ),
            oauthbearer_jwks_last_on_demand_refresh_ms: std::sync::Arc::new(
                std::sync::atomic::AtomicI64::new(0),
            ),
            oauthbearer_jwks_min_on_demand_pause: std::time::Duration::from_secs(1),
            oauthbearer_jwks_ignore_key_use: false,
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
            // Disable the disk scanner by default in tests so the
            // background task doesn't tick during short-lived fixtures.
            // The dedicated 43e integration test enables it explicitly.
            partition_disk_scan_interval_secs: 0,
            max_incremental_fetch_session_cache_slots: 1000,
            // Slice 51: tests opt into delegation tokens by setting
            // `delegation_token_secret_key`; default off keeps the
            // four DT RPCs returning DELEGATION_TOKEN_AUTH_DISABLED.
            delegation_token_secret_key: None,
            delegation_token_max_lifetime_ms: DEFAULT_DELEGATION_TOKEN_MAX_LIFETIME_MS,
            delegation_token_expiry_check_interval_ms:
                DEFAULT_DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS,
            delegation_token_default_renew_period_ms: DEFAULT_DELEGATION_TOKEN_RENEW_PERIOD_MS,
            // Slice 48b: tiered storage off by default in tests.
            remote_storage_backend: None,
            // Tests that turn tiered storage on want quick offload, so the
            // for_tests default is well below the 30s production value.
            remote_log_manager_interval: std::time::Duration::from_secs(2),
            // Slice 48f: tests use the in-memory RLMM fixture.
            remote_log_metadata_kafka: None,
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

        // Every SASL listener requires at least one mechanism. Per-listener
        // sasl_mechanisms (slice 31) wins over the broker-wide default.
        for l in &listeners {
            if l.protocol.requires_sasl() {
                let mechanisms = l
                    .sasl_mechanisms
                    .as_deref()
                    .unwrap_or(&self.enabled_sasl_mechanisms);
                if mechanisms.is_empty() {
                    return Err(BrokerError::SaslListenerNoMechanisms {
                        name: l.name.clone(),
                    });
                }
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

    /// All log directories this broker stores partition data in, primary
    /// first, de-duplicated. This is the placement + `DescribeLogDirs`
    /// surface (KIP-113). `__cluster_metadata` is excluded — it lives on
    /// [`log_dir`][Self::log_dir] only.
    #[must_use]
    pub fn all_log_dirs(&self) -> Vec<PathBuf> {
        let mut out = vec![self.log_dir.clone()];
        for d in &self.extra_log_dirs {
            if !out.contains(d) {
                out.push(d.clone());
            }
        }
        out
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
            tls_config: None,
            sasl_mechanisms: None,
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
            extra_log_dirs: Vec::new(),
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
            authorizer: std::sync::Arc::new(crate::authorizer::AllowAllAuthorizer),
            tls_config: None,
            enabled_sasl_mechanisms: vec![],
            oauthbearer_validator: crabka_security::OAuthBearerValidator::default(),
            oauthbearer_jwks_endpoint: None,
            oauthbearer_jwks_refresh_interval: std::time::Duration::from_mins(5),
            oauthbearer_idp_tls_trust: None,
            oauthbearer_max_session_lifetime_seconds: None,
            oauthbearer_jwks_signal_rx: std::sync::Arc::new(std::sync::Mutex::new(None)),
            oauthbearer_jwks_last_successful_fetch_ms: std::sync::Arc::new(
                std::sync::atomic::AtomicI64::new(0),
            ),
            oauthbearer_jwks_last_on_demand_refresh_ms: std::sync::Arc::new(
                std::sync::atomic::AtomicI64::new(0),
            ),
            oauthbearer_jwks_min_on_demand_pause: std::time::Duration::from_secs(1),
            oauthbearer_jwks_ignore_key_use: false,
            auto_leader_rebalance_enable: true,
            leader_imbalance_check_interval_secs: 300,
            leader_imbalance_per_broker_percentage: 10,
            #[cfg(any(test, feature = "test-helpers"))]
            cleaner_interval_override: None,
            tls_reload_interval: std::time::Duration::from_secs(30),
            // Default to `None` so multi-broker library users (and
            // multi-broker tests) don't race on a fixed port. The
            // `crabka-broker` binary opts in to `Some(0.0.0.0:9404)`
            // via its `--metrics-listen-addr` CLI flag — the operator
            // sets that via env, so production deployments still get
            // metrics by default.
            metrics_listen_addr: None,
            partition_disk_scan_interval_secs: 60,
            max_incremental_fetch_session_cache_slots: 1000,
            // Slice 51: master key off by default. Operators flip this on
            // via `CRABKA_DELEGATION_TOKEN_SECRET_KEY` env var or the
            // `[delegation_token] secret_key` TOML stanza.
            delegation_token_secret_key: None,
            delegation_token_max_lifetime_ms: DEFAULT_DELEGATION_TOKEN_MAX_LIFETIME_MS,
            delegation_token_expiry_check_interval_ms:
                DEFAULT_DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS,
            delegation_token_default_renew_period_ms: DEFAULT_DELEGATION_TOKEN_RENEW_PERIOD_MS,
            // Slice 48b: tiered storage off by default. Operators enable it
            // via `[remote_storage] storage_dir` in `broker.toml`.
            remote_storage_backend: None,
            remote_log_manager_interval: std::time::Duration::from_secs(30),
            // Slice 48f: production default keeps the in-memory RLMM
            // until the operator opts into the topic-backed manager.
            remote_log_metadata_kafka: None,
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
                    tls_config: None,
                    sasl_mechanisms: None,
                },
                ListenerSpec {
                    name: "EXTERNAL".to_string(),
                    bind_addr: "0.0.0.0:9092".parse().unwrap(),
                    advertised: "host.docker.internal:9092".to_string(),
                    protocol: ListenerProtocol::SaslSsl,
                    tls_config: None,
                    sasl_mechanisms: None,
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
    fn per_listener_sasl_mechanisms_satisfy_validation_without_broker_default() {
        let tls = TlsConfig {
            cert_chain_path: std::path::PathBuf::from("/tls/c"),
            private_key_path: std::path::PathBuf::from("/tls/k"),
            trust_roots_path: None,
            client_ca_path: None,
            client_auth: crabka_security::ClientAuthMode::Disabled,
        };
        let listener = ListenerSpec {
            name: "scram".into(),
            bind_addr: "0.0.0.0:9094".parse().unwrap(),
            advertised: "broker-0:9094".into(),
            protocol: ListenerProtocol::SaslSsl,
            tls_config: Some(tls.clone()),
            sasl_mechanisms: Some(vec![SaslMechanism::ScramSha512]),
        };
        let c = BrokerConfig {
            listeners: vec![listener],
            inter_broker_listener_name: "scram".into(),
            enabled_sasl_mechanisms: vec![],
            tls_config: Some(tls),
            controller_listener_protocol: ListenerProtocol::Plaintext,
            ..BrokerConfig::default()
        };
        c.validate()
            .expect("per-listener mechanisms satisfy SASL validation");
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
