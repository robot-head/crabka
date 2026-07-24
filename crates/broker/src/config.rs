//! Broker configuration. Built directly (library use) or from CLI flags
//! (binary entry point in `bin/broker.rs`).

use std::{collections::HashMap, net::SocketAddr, path::PathBuf, time::Duration};

use crabka_log::LogConfig;
pub use crabka_raft::BootstrapMode;
use crabka_raft::NodeId;
use crabka_security::{ListenerProtocol, SaslMechanism, TlsConfig};

use crate::BrokerError;

/// `KRaft` `process.roles`. A node is a metadata-quorum `Controller`, a data
/// `Broker`, or both. Default is the combined set `[Controller, Broker]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeRole {
    Controller,
    Broker,
}

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

/// Credentials the broker uses when connecting *to* other brokers, one
/// variant per SASL mechanism the inter-broker client can speak.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InterBrokerCredentials {
    /// SASL/PLAIN: `\0username\0password`.
    Plain { username: String, password: String },
    /// SASL/SCRAM (SHA-256 or SHA-512).
    Scram {
        mechanism: SaslMechanism,
        username: String,
        password: String,
    },
    /// SASL/GSSAPI: authenticate as `client_principal` using the long-term
    /// key in `keytab_path` (no password). `service_name` is the target
    /// broker's SPN primary (combined with the dialed host into
    /// `service_name/host` at connect time); `kdc_url` is the KDC endpoint
    /// (e.g. `tcp://kdc:88`).
    Gssapi {
        keytab_path: PathBuf,
        client_principal: String,
        service_name: String,
        kdc_url: String,
    },
}

impl InterBrokerCredentials {
    /// The SASL mechanism this credential set authenticates with.
    #[must_use]
    pub fn mechanism(&self) -> SaslMechanism {
        match self {
            Self::Plain { .. } => SaslMechanism::Plain,
            Self::Scram { mechanism, .. } => *mechanism,
            Self::Gssapi { .. } => SaslMechanism::Gssapi,
        }
    }
}

/// Construction-time configuration for [`crate::Broker::start`].
///
/// Build directly when embedding the broker as a library, or via the
/// `crabka-broker` binary's clap CLI in production.
#[derive(Debug, Clone, Copy)]
pub struct BrokerFeatureFlags {
    pub oauthbearer_jwks_ignore_key_use: bool,
    pub auto_leader_rebalance_enable: bool,
    pub transaction_two_phase_commit_enable: bool,
}

/// Runtime policy used by follower replication tasks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationRuntimeConfig {
    /// Maximum bytes requested from a leader in one replication fetch.
    pub fetch_max_bytes: i32,
    /// Maximum leader wait for a replication fetch.
    pub fetch_max_wait_ms: i32,
    /// Minimum bytes that satisfy a replication fetch.
    pub fetch_min_bytes: i32,
    /// Delay after a replication throttle budget is exhausted.
    pub throttle_exhausted_backoff: Duration,
    /// Retry delay after sending a replication request fails.
    pub send_error_backoff: Duration,
    /// Retry delay when the leader does not yet know the topic.
    pub unknown_topic_retry_delay: Duration,
    /// Retry delay after a leader-epoch fence.
    pub epoch_fence_backoff: Duration,
    /// Retry delay after an unexpected replication error.
    pub unexpected_error_backoff: Duration,
    /// Initial delay before reconnecting to a leader.
    pub reconnect_initial_delay: Duration,
    /// Maximum delay between leader reconnection attempts.
    pub reconnect_delay_cap: Duration,
}

impl Default for ReplicationRuntimeConfig {
    fn default() -> Self {
        Self {
            fetch_max_bytes: 1_048_576,
            fetch_max_wait_ms: 500,
            fetch_min_bytes: 1,
            throttle_exhausted_backoff: Duration::from_millis(100),
            send_error_backoff: Duration::from_secs(1),
            unknown_topic_retry_delay: Duration::from_millis(100),
            epoch_fence_backoff: Duration::from_millis(200),
            unexpected_error_backoff: Duration::from_millis(500),
            reconnect_initial_delay: Duration::from_millis(100),
            reconnect_delay_cap: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Clone)]
// a broad config struct; flags are independent knobs
pub struct BrokerConfig {
    /// Maximum time to wait for a controller leader during startup.
    pub startup_leader_wait_timeout: Duration,
    /// Initial delay between self-registration attempts.
    pub self_registration_backoff_min: Duration,
    /// Maximum delay between self-registration attempts.
    pub self_registration_backoff_max: Duration,
    /// Observer promotion polling cadence.
    pub observer_poll_interval: Duration,
    /// Audit spool replay cadence.
    pub audit_spool_replay_interval: Duration,
    /// Audit statistics polling cadence.
    pub audit_stats_poll_interval: Duration,
    /// Maximum wait for the audit partition to become available.
    pub audit_partition_wait_timeout: Duration,
    /// Broker liveness maintenance cadence.
    pub liveness_tick_interval: Duration,
    /// Broker gauge refresh cadence.
    pub gauge_poll_interval: Duration,
    /// In-sync replica maintenance cadence.
    pub isr_scan_interval: Duration,
    /// Log cleaner maintenance cadence.
    pub cleaner_interval: Duration,
    /// Retry delay when moving a future log fails.
    pub future_log_move_retry_backoff: Duration,
    /// Client-metrics cache eviction cadence.
    pub client_metrics_eviction_tick: Duration,
    /// Minimum age at which client metrics are stale.
    pub client_metrics_stale_floor: Duration,
    /// Default client telemetry subscription interval.
    pub client_metrics_default_interval_ms: i32,
    /// Capacity of the client-metrics OTLP forwarding queue.
    pub client_metrics_otlp_queue_capacity: usize,
    /// Maximum accepted client telemetry payload size.
    pub client_metrics_telemetry_max_bytes: i32,
    /// Prometheus client-metrics snapshot lifetime.
    pub client_metrics_prom_snapshot_ttl: Duration,
    /// Remote-log metadata reconciliation cadence.
    pub rlmm_reconcile_tick: Duration,
    /// Initial remote-log metadata bootstrap retry delay.
    pub rlmm_bootstrap_backoff_initial: Duration,
    /// Maximum remote-log metadata bootstrap retry delay.
    pub rlmm_bootstrap_backoff_max: Duration,
    /// Maximum connection-creation quota delay.
    pub connection_creation_throttle_max: Duration,
    /// OPA authorization request timeout.
    pub opa_http_timeout: Duration,
    /// OAuth JWKS HTTP request timeout.
    pub oauth_jwks_http_timeout: Duration,
    /// Dynamic-quorum auto-join retry delay.
    pub auto_join_retry_backoff: Duration,
    /// Follower replication runtime policy.
    pub replication: ReplicationRuntimeConfig,
    /// Consumer-group session expiry scan cadence.
    pub coordinator_session_expiry_tick: Duration,
    /// Maximum wait for coordinator shutdown acknowledgements.
    pub coordinator_shutdown_ack_timeout: Duration,
    /// Initial delay before a classic group begins rebalancing.
    pub classic_group_initial_rebalance_delay: Duration,
    /// Maximum time a follower waits for a `SyncGroup` assignment.
    pub sync_group_follower_wait: Duration,
    /// Aggressive unclean-recovery collection deadline.
    pub unclean_recovery_aggressive_deadline: Duration,
    /// Balanced unclean-recovery collection deadline.
    pub unclean_recovery_balanced_deadline: Duration,
    /// Operator-triggered recovery deadline.
    pub operator_recovery_deadline: Duration,
    /// Maximum quota throttle delay.
    pub quota_throttle_max: Duration,
    /// Maximum self-registration attempts before startup fails.
    pub self_registration_max_attempts: u32,
    /// Maximum bytes fetched by a metadata observer request.
    pub observer_fetch_max_bytes: u32,
    /// Capacity of the asynchronous audit event queue.
    pub audit_event_queue_capacity: usize,
    /// Number of offsets included in an audit tail request.
    pub audit_tail_window_offsets: i64,
    /// Maximum bytes read by an audit tail request.
    pub audit_tail_read_max_bytes: usize,
    /// Maximum wait for offsets-topic metadata.
    pub offsets_topic_metadata_wait_timeout: Duration,
    /// Push intervals after which client metrics become stale.
    pub client_metrics_stale_push_intervals: u32,
    /// Capacity of each coordinator actor mailbox.
    pub coordinator_actor_mailbox_capacity: usize,
    /// Capacity of the unclean-recovery work queue.
    pub unclean_recovery_queue_capacity: usize,
    /// Maximum bytes read while recovering share state.
    pub share_recovery_read_max_bytes: usize,
    /// Share-session cache ceiling when group count is unlimited.
    pub share_session_cache_max_when_unlimited: usize,
    /// Maximum encoded request size accepted from a socket.
    pub socket_request_max_bytes: usize,
    /// Minimum response size eligible for `sendfile`.
    pub sendfile_min_bytes: usize,
    /// Broker socket send-buffer size.
    pub socket_send_buffer_bytes: usize,
    /// Broker socket receive-buffer size.
    pub socket_receive_buffer_bytes: usize,
    /// Maximum encoded ACL principal length.
    pub acl_max_principal_bytes: usize,
    /// Maximum encoded ACL resource-name length.
    pub acl_max_resource_name_bytes: usize,
    /// Maximum accepted telemetry decompression ratio.
    pub telemetry_max_decompression_ratio: usize,
    /// Minimum telemetry decompression output allowance.
    pub telemetry_decompressed_output_floor_bytes: usize,
    /// Maximum telemetry decompression output allowance.
    pub telemetry_decompressed_output_ceiling_bytes: usize,
    /// TLS server name used for outbound inter-broker connections.
    pub inter_broker_server_name: String,
    /// Producer-id inactivity period before state expires.
    pub producer_id_expiration_ms: i64,
    /// Producer-state expiry scan cadence.
    pub producer_id_expiration_scan_interval: Duration,
    /// Maximum produce requests combined into one append group.
    pub max_produce_group: usize,
    /// Capacity of each partition-writer request queue.
    pub partition_writer_queue_depth: usize,
    /// Default minimum in-sync replica count.
    pub default_min_insync_replicas: i32,
    /// Bytes copied per future-log move read.
    pub future_log_move_read_chunk_bytes: usize,
    /// Partition count for the transaction-state internal topic.
    pub transaction_state_num_partitions: i32,
    /// Minimum accepted transaction timeout.
    pub transaction_min_timeout_ms: i32,
    /// Maximum accepted transaction timeout.
    pub transaction_max_timeout_ms: i32,

    /// Broker id reported in `Metadata` responses. Default: 1.
    pub broker_id: i32,

    /// `KRaft` `process.roles`. Controls whether this node is a metadata
    /// quorum voter (`Controller`), hosts data partitions + registers as a
    /// broker (`Broker`), or both. Default: `[Controller, Broker]`.
    pub roles: Vec<NodeRole>,

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

    /// Static voter set: `[(node_id, "<host>:<port>"), …]`. The address is the
    /// peer controller listener's `<host>:<port>` carried verbatim (NOT
    /// pre-resolved): the dialer re-resolves the host on every (re)connect so a
    /// peer that restarts on a new pod IP stays reachable. Defaults to a
    /// single-voter cluster of just this broker, so single-broker setups
    /// upgrade to quorum-of-1 without config changes.
    pub controller_quorum_voters: Vec<(NodeId, String)>,

    /// TLS server name (SNI) presented when dialing a peer's controller
    /// listener for the KIP-595 quorum. Set to a SAN shared by every
    /// broker's serving cert (the headless-Service FQDN) so mTLS validates
    /// regardless of which peer (a pod IP) is dialed. `None` falls back to
    /// `"localhost"`.
    pub controller_server_name: Option<String>,

    /// KIP-853 dynamic quorum: controller endpoints used only to discover
    /// the leader at cold start (the joiner path). Empty for a standalone
    /// bootstrap node. Maps to Kafka's `controller.quorum.bootstrap.servers`.
    pub bootstrap_servers: Vec<SocketAddr>,

    /// KIP-853: this replica's stable directory id, recovered from
    /// `meta.properties.json` at boot. Identifies which voter this node *is*.
    pub directory_id: uuid::Uuid,

    /// UUID identifying this specific broker process invocation. Persisted in
    /// `{log_dir}/incarnation_id` and reloaded on restart. Populated before
    /// self-registration by the internal `load_or_generate` helper.
    /// Tests generate a random UUID per call via [`Self::for_tests`].
    pub incarnation_id: uuid::Uuid,

    /// KIP-853: when true, an observer issues `AddVoter` for itself once it
    /// has caught up to the leader, joining the quorum without operator
    /// action. Maps to Kafka's `controller.quorum.auto.join.enable`.
    pub auto_join: bool,

    /// KIP-853: maximum log-entry lag an observer may have and still be
    /// promotable to a voter. Forwarded to `ControllerConfig`.
    pub observer_lag_bound: u64,

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

    /// `metadata.log.max.record.bytes.between.snapshots` (default 20 MiB).
    pub metadata_max_bytes_between_snapshots: u64,

    /// `metadata.log.max.snapshot.interval.ms` (default 1 h; 0 = disabled).
    pub metadata_max_snapshot_interval: std::time::Duration,

    /// KIP-630: snapshot the metadata log once committed offset advances this
    /// many records past the last snapshot, then prune below it.
    pub metadata_snapshot_interval_records: u64,

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

    /// KIP-392: this broker's rack identifier (`broker.rack`). Reported in
    /// its `BrokerRegistrationRecord` and used by the leader's rack-aware
    /// replica selector. `None` (default) means no rack.
    pub rack: Option<String>,

    /// KIP-392: which replica selector the leader runs to populate
    /// `FetchResponse.preferred_read_replica` for rack-aware consumers.
    /// Default `Leader` (never redirect).
    pub replica_selector: crate::replica_selector::ReplicaSelectorKind,

    // ── Auth / listener registry ─────────────────────────────────────────
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
    /// `None` means no SASL — plaintext inter-broker traffic (the default).
    pub inter_broker_credentials: Option<InterBrokerCredentials>,

    /// Static PLAIN credentials: username → password.  Empty by default
    /// (PLAIN auth disabled until mechanisms are explicitly enabled).
    pub plain_credentials: HashMap<String, String>,

    /// Usernames that bypass ACL checks (super-users). The
    /// `create_delegation_token` act-as gate reads this directly; the
    /// active [`crate::authorizer::Authorizer`] impl also reads it
    /// (`SimpleAclAuthorizer` / `OpaAuthorizer`). Both are populated
    /// from the same `[authorization]` TOML stanza by `file_config`.
    pub super_users: std::collections::HashSet<String>,

    /// Pluggable cluster authorizer. One boxed instance per
    /// broker; configured via `[authorization]` in `broker.toml`. The
    /// default is [`crate::authorizer::AllowAllAuthorizer`] — explicit
    /// "allow everything" — which replaces the earlier
    /// "no super-users + no ACLs ⇒ Allow" compat shim that previously
    /// lived inside the ACL impl.
    pub authorizer: std::sync::Arc<dyn crate::authorizer::Authorizer>,

    /// TLS configuration. `None` — no TLS (the default).
    pub tls_config: Option<TlsConfig>,

    /// Which SASL mechanisms are enabled. Empty → no SASL.
    pub enabled_sasl_mechanisms: Vec<SaslMechanism>,

    /// Validator for SASL/OAUTHBEARER bearer tokens. Only
    /// consulted when `OAuthBearer` is in `enabled_sasl_mechanisms` (the
    /// handshake won't advertise it otherwise). Defaults to the unsecured-JWS
    /// validator with principal claim `sub`; configuring a JWKS endpoint
    /// (`[oauthbearer].jwks_endpoint_uri`) selects the signed-JWT validator.
    pub oauthbearer_validator: crabka_security::OAuthBearerValidator,

    /// SASL/GSSAPI (Kerberos) configuration. `Some` only when `Gssapi` is in
    /// `enabled_sasl_mechanisms`; carries the service keytab path,
    /// `auth_to_local` rules, and KDC/realm settings for the initiate path.
    pub gssapi: Option<crabka_security::gssapi::GssapiConfig>,

    /// JWKS endpoint to fetch OAUTHBEARER signing keys from. `Some`
    /// only when `oauthbearer_validator` is the signed variant. When set,
    /// `Broker::start` spawns a background refresher that fetches this URL and
    /// rotates the validator's key set on `oauthbearer_jwks_refresh_interval`.
    pub oauthbearer_jwks_endpoint: Option<String>,

    /// How often to re-fetch the JWKS. Default 5 minutes.
    pub oauthbearer_jwks_refresh_interval: std::time::Duration,

    /// Optional PEM path for outbound
    /// HTTPS to the `IdP`. Shared across JWKS, introspection, and
    /// userinfo. None → reqwest's default webpki-roots.
    pub oauthbearer_idp_tls_trust: Option<std::path::PathBuf>,

    /// Optional ceiling on OAUTHBEARER session lifetime, in
    /// seconds. When set, the broker reports
    /// `session_lifetime_ms = min(token_exp_ms - now_ms, cap * 1000)`
    /// and the dispatch-loop re-auth timer fires at the clamped time.
    /// When unset, sessions last until the token's natural `exp`
    /// (the default).
    pub oauthbearer_max_session_lifetime_seconds: Option<u32>,

    /// Receiver half of the JWKS refresher signal channel.
    /// `apply_to` creates the channel pair: the sender is wired into the
    /// signed validator's `JwksHandle`; the receiver is parked here for
    /// `Broker::start` to `take()` and pass to `JwksRefresher`. `None`
    /// when JWKS validation isn't configured. `Arc<Mutex<…>>` so the
    /// containing `BrokerConfig` can stay `Clone`; only `Broker::start`
    /// `.lock().take()`s the receiver, and there is only ever one
    /// `Broker::start` per validator construction.
    pub oauthbearer_jwks_signal_rx:
        std::sync::Arc<std::sync::Mutex<Option<tokio::sync::mpsc::Receiver<()>>>>,

    /// Shared timestamp of the last successful JWKS fetch.
    /// `apply_to` creates it (`AtomicI64::new(0)`); the validator's
    /// `JwksHandle` and the refresher both clone this `Arc` so the
    /// refresher's writes are visible to the validator's expiry check.
    pub oauthbearer_jwks_last_successful_fetch_ms: std::sync::Arc<std::sync::atomic::AtomicI64>,

    /// Shared on-demand-refresh timestamp for rate-limiting.
    /// `apply_to` creates it; `Broker::start` hands a clone to the
    /// refresher. The validator never reads this — it's refresher-only
    /// bookkeeping carried through `BrokerConfig` for symmetry.
    pub oauthbearer_jwks_last_on_demand_refresh_ms: std::sync::Arc<std::sync::atomic::AtomicI64>,

    /// Minimum pause between on-demand JWKS refreshes
    /// triggered by validator signals. `apply_to` sets this from
    /// `FileOAuthBearerConfig::jwks_min_refresh_pause_seconds`;
    /// `Broker::start` passes it into `JwksRefresher`. Strimzi default
    /// 1 second; we default to 1 second too.
    pub oauthbearer_jwks_min_on_demand_pause: std::time::Duration,

    /// Independent compatibility and protocol feature gates.
    pub features: BrokerFeatureFlags,

    /// KIP-98 / KIP-939: how often the idle-transaction reaper scans for
    /// `Ongoing` transactions whose timeout has elapsed and aborts them (2PC
    /// transactions are never reaped). Mirrors Kafka's
    /// `transaction.abort.timed.out.transaction.cleanup.interval.ms` (10s).
    /// `Duration::ZERO` disables the reaper entirely (no background task
    /// spawned) — the default in `for_tests` so unit/integration tests aren't
    /// disturbed by a background abort; tests that exercise the reaper set it
    /// explicitly low.
    pub txn_abort_cleanup_interval: std::time::Duration,

    /// KIP-848 next-gen consumer group protocol configuration. Controls
    /// which rebalance protocols are advertised, session/heartbeat
    /// timeout bounds, and the set of enabled server-side assignors.
    pub next_gen_consumer_group: Box<crate::coordinator::unified::config::NextGenConfig>,

    /// KIP-932 share-group configuration.
    pub share_group: Box<crate::coordinator::unified::share::config::ShareGroupConfig>,

    /// KIP-1071 streams-group (Streams rebalance protocol) configuration.
    pub streams_group: Box<crate::coordinator::unified::streams::config::StreamsGroupConfig>,

    /// KIP-932 share-coordinator (persister) configuration. Controls the
    /// `__share_group_state` internal topic geometry and snapshot folding.
    pub share_coordinator: Box<crate::share_coordinator::config::ShareCoordinatorConfig>,

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

    /// How often the TLS reload watcher polls cert / key /
    /// client-CA file mtimes and rebuilds the `ServerConfig` if any
    /// changed. Defaults to 30s. Set lower in tests to keep watcher
    /// latency tight. `Duration::ZERO` disables the periodic watcher
    /// — callers can still trigger an immediate reload via
    /// [`crate::BrokerHandle::reload_tls`].
    pub tls_reload_interval: std::time::Duration,

    /// Bind address for the Prometheus `/metrics` HTTP
    /// endpoint. `None` disables the server entirely (the broker still
    /// updates its internal counters, but nothing scrapes them).
    /// Defaults to `Some(0.0.0.0:9404)` in production (the same port
    /// the JMX exporter uses for vanilla Kafka), `None` in
    /// `for_tests` so unit tests don't fight over port allocation.
    pub metrics_listen_addr: Option<SocketAddr>,

    /// Optional OTLP endpoint for KIP-714 client metrics forwarding.
    /// Populated by binaries from their parsed runtime configuration rather
    /// than read from the environment while the broker starts.
    pub client_metrics_otlp_endpoint: Option<String>,

    /// KIP-227: maximum number of incremental-fetch sessions kept in the
    /// per-broker cache. Each session tracks the (topic, partition) set a
    /// client is subscribed to so subsequent fetches can be deltas. When
    /// full, a non-privileged (consumer) session is evicted LRU; privileged
    /// (follower-fetch) sessions are evicted only by other privileged
    /// sessions. Matches Apache Kafka's `max.incremental.fetch.session.cache.slots`
    /// (default 1000).
    pub max_incremental_fetch_session_cache_slots: usize,

    /// Maximum number of live broker connections across all listeners.
    /// New connections accepted past this ceiling are closed immediately
    /// (Kafka silently drops them). Matches Apache Kafka's
    /// `max.connections`; default `usize::MAX` (unlimited, mirroring
    /// Kafka's `Integer.MAX_VALUE`).
    pub max_connections: usize,

    /// Maximum number of live connections from any single client IP.
    /// Connections past this per-IP ceiling are closed immediately.
    /// Matches Apache Kafka's `max.connections.per.ip`; default
    /// `usize::MAX` (unlimited).
    pub max_connections_per_ip: usize,

    /// Partition disk-usage scan cadence, in seconds. `0`
    /// disables the scanner entirely (no background task spawned).
    /// Production default: 60s. The scanner walks every known
    /// (topic, partition) under `log_dir` each tick, sums regular-file
    /// sizes, and updates the `partition_disk_bytes` gauge consumed by
    /// the rebalancer's usage scraper.
    pub partition_disk_scan_interval_secs: u64,

    /// KIP-48: HMAC-SHA-256 master key used to mint + verify
    /// delegation tokens. When `None`, the broker rejects all four
    /// delegation-token RPCs with `DELEGATION_TOKEN_AUTH_DISABLED` and
    /// SCRAM cannot fall back to token lookup. Sourced from
    /// `CRABKA_DELEGATION_TOKEN_SECRET_KEY` (env wins) or
    /// `[delegation_token] secret_key` in `broker.toml`. Wrapped in
    /// `SecretBytes` so `Debug` redacts the bytes.
    pub delegation_token_secret_key: Option<crabka_security::SecretBytes>,

    /// KIP-48: hard upper bound on delegation-token lifetime,
    /// in milliseconds. A token's `max_timestamp_ms` is set to
    /// `issue_timestamp_ms + delegation_token_max_lifetime_ms` and the
    /// renew handler clamps any caller-requested expiry to this. Default
    /// 7 days (`delegation.token.max.lifetime.ms` in Kafka).
    pub delegation_token_max_lifetime_ms: i64,

    /// KIP-48: cadence of the background sweep task that
    /// proposes `V1DeleteDelegationToken` tombstones for tokens whose
    /// `expiry_timestamp_ms` or `max_timestamp_ms` is in the past. Default
    /// 1 hour (`delegation.token.expiry.check.interval.ms` in Kafka).
    pub delegation_token_expiry_check_interval_ms: i64,

    /// KIP-48: default renew period used as the *initial*
    /// `expiry_timestamp_ms` offset at create time, and as the implicit
    /// renew period when `RenewDelegationToken.renew_period_ms == -1`.
    /// Distinct from `delegation_token_max_lifetime_ms` (the absolute
    /// ceiling that `expiry_timestamp_ms` can never be pushed past via
    /// `Renew`): a fresh token gets `expiry_timestamp_ms = now +
    /// min(default_renew_period, chosen_max_lifetime)` while
    /// `max_timestamp_ms = now + chosen_max_lifetime`. Default 24 hours
    /// (`delegation.token.expiry.time.ms` in Kafka).
    pub delegation_token_default_renew_period_ms: i64,

    /// KIP-405: tiered-storage backend selection. `Some(_)`
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

    /// KIP-405: tick cadence of the `RemoteLogManager` copy /
    /// retention task. Defaults to 30s (Kafka's
    /// `remote.log.manager.task.interval.ms`). Acceptance tests lower this
    /// so segments are tiered and locally evicted in seconds rather than
    /// minutes; production deployments leave it at the default.
    pub remote_log_manager_interval: std::time::Duration,

    /// KIP-405: which RLMM the broker runs when tiered storage is enabled.
    /// Defaults to [`RlmmKind::TopicBacked`] in production; [`RlmmKind::InMemory`]
    /// for in-process tests. Ignored when `remote_storage_backend` is `None`.
    pub remote_log_metadata: RlmmKind,

    /// Whether the audit subsystem is active (`FedRAMP` MLA).
    pub audit_enabled: bool,
    /// Internal topic name for audit records.
    pub audit_topic: String,
    /// Path to the PKCS#8 Ed25519 audit checkpoint signing key (None = no checkpoints).
    pub audit_signing_key_path: Option<std::path::PathBuf>,
    /// Key id recorded on checkpoints (for rotation).
    pub audit_signing_key_id: Option<String>,
    /// Emit a checkpoint after this many audit records.
    pub audit_checkpoint_every_n: u64,
    /// Emit a checkpoint at least this often (seconds).
    pub audit_checkpoint_every_secs: u64,
    /// Directory for the durable audit spool (relative paths resolve under the broker's log dir).
    pub audit_spool_dir: std::path::PathBuf,
    /// Cap on the audit spool size in bytes.
    pub audit_spool_max_bytes: u64,
}

/// Parameters for the topic-backed
/// [`RemoteLogMetadataManager`](crabka_remote_storage::RemoteLogMetadataManager).
///
/// Does not derive `PartialEq`/`Eq`: the `security` field holds
/// rustls-adjacent types (a `ClientConfig` connector) that are not
/// comparable, and nothing compares this config by value.
#[derive(Debug, Clone)]
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
    /// How often the topic-backed manager flushes its RLMM cache
    /// snapshot to disk. Maps to Kafka's
    /// `remote.log.metadata.snapshot.interval`. Default
    /// [`DEFAULT_RLMM_SNAPSHOT_INTERVAL`].
    pub snapshot_interval: std::time::Duration,
    /// Directory the RLMM cache snapshot is written to (one
    /// `snapshot` file). Derived from the broker `log.dir`.
    pub snapshot_dir: std::path::PathBuf,
    /// Client TLS/SASL security for the metadata client. `None` =
    /// plaintext loopback (single-broker / fully-plaintext clusters).
    /// The broker overrides this at runtime in `bootstrap_topic_rlmm`
    /// from the inter-broker listener; the TOML path always supplies
    /// `None`.
    ///
    /// Boxed to keep `KafkaRlmmConfig` (and the enclosing `BrokerConfig`)
    /// small: `BrokerConfig` is moved by value into the large
    /// `Broker::start` future.
    pub security: Option<Box<crabka_client_core::security::ClientSecurity>>,
}

/// Default cadence of the topic-backed RLMM snapshot flush. 60s,
/// matching Kafka's `remote.log.metadata.snapshot.interval` default.
pub const DEFAULT_RLMM_SNAPSHOT_INTERVAL: std::time::Duration = std::time::Duration::from_mins(1);

/// Which `RemoteLogMetadataManager` the broker runs when tiered storage is enabled.
///
/// Topic-backed is the production default (matches Kafka's
/// `TopicBasedRemoteLogMetadataManager`, the only production RLMM). In-memory
/// is an explicit opt-out for in-process integration tests that have no real
/// listener to loop the metadata client back to. Ignored entirely when
/// [`BrokerConfig::remote_storage_backend`] is `None`.
#[derive(Debug, Clone)]
pub enum RlmmKind {
    /// Durable `__remote_log_metadata`-backed manager. `cfg.bootstrap` and
    /// `cfg.snapshot_dir` may be empty; the broker derives them at start from
    /// the inter-broker listener and `log.dir` respectively.
    TopicBacked(KafkaRlmmConfig),
    /// Non-durable in-process manager. Tests only.
    InMemory,
}

impl Default for KafkaRlmmConfig {
    fn default() -> Self {
        Self {
            bootstrap: String::new(),
            num_partitions: DEFAULT_RLMM_TOPIC_NUM_PARTITIONS,
            replication: DEFAULT_RLMM_TOPIC_REPLICATION_FACTOR,
            snapshot_interval: DEFAULT_RLMM_SNAPSHOT_INTERVAL,
            snapshot_dir: std::path::PathBuf::new(),
            security: None,
        }
    }
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
    /// Native Google Cloud Storage `S3RemoteStorage` engine. Production
    /// backend for GKE deployments; supports keyless Workload Identity /
    /// ADC auth (leave all credential fields unset).
    Gcs(crabka_remote_storage::GcsConfig),
}

/// Default broker→controller `BrokerHeartbeat` cadence, in milliseconds.
pub const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 3_000;

/// Default controller-side broker-session timeout, in milliseconds (3× the
/// heartbeat interval, so a broker survives two missed heartbeats).
pub const DEFAULT_HEARTBEAT_TIMEOUT_MS: u64 = 9_000;

/// Default maximum follower lag before the leader proposes ISR shrink, in
/// milliseconds. Matches Kafka's `replica.lag.time.max.ms` default.
pub const DEFAULT_REPLICA_LAG_TIME_MAX_MS: u64 = 30_000;

/// Default byte gap between metadata-log snapshots: 20 MiB, matching Kafka's
/// `metadata.log.max.record.bytes.between.snapshots`.
pub const DEFAULT_METADATA_MAX_BYTES_BETWEEN_SNAPSHOTS: u64 = 20 * 1024 * 1024;

/// Default time cap between metadata-log snapshots in milliseconds: 1 hour,
/// matching Kafka's `metadata.log.max.snapshot.interval.ms`.
pub const DEFAULT_METADATA_MAX_SNAPSHOT_INTERVAL_MS: u64 = 3_600_000;

/// Default time cap between metadata-log snapshots: 1 hour, matching Kafka's
/// `metadata.log.max.snapshot.interval.ms`.
pub const DEFAULT_METADATA_MAX_SNAPSHOT_INTERVAL: std::time::Duration =
    std::time::Duration::from_millis(DEFAULT_METADATA_MAX_SNAPSHOT_INTERVAL_MS);

/// KIP-630: default committed-record gap between metadata-log snapshots.
pub const DEFAULT_METADATA_SNAPSHOT_INTERVAL_RECORDS: u64 = 10_000;

/// KIP-853: default maximum log-entry lag at which an observer is still
/// promotable to a quorum voter.
pub const DEFAULT_OBSERVER_LAG_BOUND: u64 = 100;

/// Default controller election timeout, in milliseconds.
pub const DEFAULT_CONTROLLER_ELECTION_TIMEOUT_MS: u64 = 5_000;

/// Default controller heartbeat interval, in milliseconds.
pub const DEFAULT_CONTROLLER_HEARTBEAT_INTERVAL_MS: u64 = 500;

/// Default controlled-shutdown leadership drain timeout, in milliseconds.
pub const DEFAULT_CONTROLLED_SHUTDOWN_DRAIN_TIMEOUT_MS: u64 = 20_000;

/// Default idle-transaction abort cleanup interval, in milliseconds.
pub const DEFAULT_TXN_ABORT_CLEANUP_INTERVAL_MS: u64 = 10_000;

/// Default TLS material reload polling interval, in milliseconds.
pub const DEFAULT_TLS_RELOAD_INTERVAL_MS: u64 = 30_000;

/// Default `RemoteLogManager` copy / retention cadence, in milliseconds.
pub const DEFAULT_REMOTE_LOG_MANAGER_INTERVAL_MS: u64 = 30_000;

/// KIP-460: default auto-rebalance tick cadence, in seconds. Matches Kafka's
/// `leader.imbalance.check.interval.seconds`.
pub const DEFAULT_LEADER_IMBALANCE_CHECK_INTERVAL_SECS: u64 = 300;

/// KIP-460: default minimum percentage of imbalanced partitions before the
/// auto-rebalance ticker acts. Matches Kafka's
/// `leader.imbalance.per.broker.percentage`.
pub const DEFAULT_LEADER_IMBALANCE_PER_BROKER_PERCENTAGE: u32 = 10;

/// KIP-227: default incremental-fetch session cache capacity. Matches Kafka's
/// `max.incremental.fetch.session.cache.slots`.
pub const DEFAULT_MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS: usize = 1000;

/// Default cadence of the background JWKS re-fetch for the signed
/// OAUTHBEARER validator: 5 minutes.
pub const DEFAULT_JWKS_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_mins(5);

/// Default minimum pause between on-demand JWKS refreshes triggered by
/// validator signals: 1 second (Strimzi parity).
pub const DEFAULT_JWKS_MIN_ON_DEMAND_PAUSE: std::time::Duration = std::time::Duration::from_secs(1);

/// KIP-405: default partition count for `__remote_log_metadata` on first
/// creation. Matches Kafka's `remote.log.metadata.topic.num.partitions`.
pub const DEFAULT_RLMM_TOPIC_NUM_PARTITIONS: i32 = 50;

/// KIP-405: default replication factor for `__remote_log_metadata` on first
/// creation. Matches Kafka's `remote.log.metadata.topic.replication.factor`.
pub const DEFAULT_RLMM_TOPIC_REPLICATION_FACTOR: i32 = 3;

/// Default internal topic name for `FedRAMP` MLA audit records.
pub const DEFAULT_AUDIT_TOPIC: &str = "__crabka_audit";

/// Default number of audit records between signed checkpoints.
pub const DEFAULT_AUDIT_CHECKPOINT_EVERY_N: u64 = 1000;

/// Default maximum seconds between signed audit checkpoints.
pub const DEFAULT_AUDIT_CHECKPOINT_EVERY_SECS: u64 = 60;

/// Default durable audit-spool directory (relative paths resolve under the
/// broker's log dir).
pub const DEFAULT_AUDIT_SPOOL_DIR: &str = "audit-spool";

/// Default cap on the durable audit spool: 1 GiB.
pub const DEFAULT_AUDIT_SPOOL_MAX_BYTES: u64 = 1024 * 1024 * 1024;

/// KIP-48: default hard upper bound on delegation-token lifetime.
/// 7 days, matches Kafka's `delegation.token.max.lifetime.ms` default.
pub const DEFAULT_DELEGATION_TOKEN_MAX_LIFETIME_MS: i64 = 7 * 24 * 60 * 60 * 1_000;

/// KIP-48: default cadence of the background expiry sweep task.
/// 1 hour, matches Kafka's `delegation.token.expiry.check.interval.ms`.
pub const DEFAULT_DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS: i64 = 60 * 60 * 1_000;

/// KIP-48: default renew period used as the initial
/// `expiry_timestamp_ms` offset at create time, and as the implicit
/// renew period when `RenewDelegationToken.renew_period_ms == -1`.
/// 24 hours, matches Kafka's `delegation.token.expiry.time.ms` default.
pub const DEFAULT_DELEGATION_TOKEN_RENEW_PERIOD_MS: i64 = 24 * 60 * 60 * 1_000;

impl BrokerConfig {
    /// Helpful for tests: a config that listens on an OS-assigned port
    /// under a tempdir.
    #[must_use]
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn for_tests(log_dir: PathBuf) -> Self {
        let listen_addr: SocketAddr = "127.0.0.1:0".parse().expect("static");
        let controller_addr: SocketAddr = "127.0.0.1:0".parse().expect("static");
        Self {
            startup_leader_wait_timeout: Duration::from_mins(2),
            self_registration_backoff_min: Duration::from_millis(100),
            self_registration_backoff_max: Duration::from_secs(5),
            observer_poll_interval: Duration::from_millis(100),
            audit_spool_replay_interval: Duration::from_secs(2),
            audit_stats_poll_interval: Duration::from_secs(1),
            audit_partition_wait_timeout: Duration::from_secs(10),
            liveness_tick_interval: Duration::from_secs(1),
            gauge_poll_interval: Duration::from_secs(1),
            isr_scan_interval: Duration::from_secs(1),
            cleaner_interval: Duration::from_secs(30),
            future_log_move_retry_backoff: Duration::from_millis(50),
            client_metrics_eviction_tick: Duration::from_mins(1),
            client_metrics_stale_floor: Duration::from_mins(10),
            client_metrics_default_interval_ms: 300_000,
            client_metrics_otlp_queue_capacity: 256,
            client_metrics_telemetry_max_bytes: 1_048_576,
            client_metrics_prom_snapshot_ttl: Duration::from_mins(5),
            rlmm_reconcile_tick: Duration::from_secs(30),
            rlmm_bootstrap_backoff_initial: Duration::from_millis(250),
            rlmm_bootstrap_backoff_max: Duration::from_secs(10),
            connection_creation_throttle_max: Duration::from_secs(1),
            opa_http_timeout: Duration::from_secs(5),
            oauth_jwks_http_timeout: Duration::from_secs(10),
            auto_join_retry_backoff: Duration::from_millis(500),
            replication: ReplicationRuntimeConfig::default(),
            coordinator_session_expiry_tick: Duration::from_secs(1),
            coordinator_shutdown_ack_timeout: Duration::from_secs(5),
            classic_group_initial_rebalance_delay: Duration::from_secs(3),
            sync_group_follower_wait: Duration::from_secs(30),
            unclean_recovery_aggressive_deadline: Duration::from_secs(2),
            unclean_recovery_balanced_deadline: Duration::from_secs(30),
            operator_recovery_deadline: Duration::from_secs(25),
            quota_throttle_max: Duration::from_secs(1),
            self_registration_max_attempts: 8,
            observer_fetch_max_bytes: 1_048_576,
            audit_event_queue_capacity: 8_192,
            audit_tail_window_offsets: 4_096,
            audit_tail_read_max_bytes: 1_048_576,
            offsets_topic_metadata_wait_timeout: Duration::from_secs(30),
            client_metrics_stale_push_intervals: 3,
            coordinator_actor_mailbox_capacity: 64,
            unclean_recovery_queue_capacity: 256,
            share_recovery_read_max_bytes: 1_048_576,
            share_session_cache_max_when_unlimited: 10_000,
            socket_request_max_bytes: 104_857_600,
            sendfile_min_bytes: 32_768,
            socket_send_buffer_bytes: 1_048_576,
            socket_receive_buffer_bytes: 1_048_576,
            acl_max_principal_bytes: 256,
            acl_max_resource_name_bytes: 256,
            telemetry_max_decompression_ratio: 100,
            telemetry_decompressed_output_floor_bytes: 16_777_216,
            telemetry_decompressed_output_ceiling_bytes: 1_073_741_824,
            inter_broker_server_name: "localhost".to_string(),
            producer_id_expiration_ms: 86_400_000,
            producer_id_expiration_scan_interval: Duration::from_mins(10),
            max_produce_group: 1_024,
            partition_writer_queue_depth: 64,
            default_min_insync_replicas: 1,
            future_log_move_read_chunk_bytes: 1_048_576,
            transaction_state_num_partitions: 50,
            transaction_min_timeout_ms: 1_000,
            transaction_max_timeout_ms: 900_000,
            broker_id: 1,
            roles: vec![NodeRole::Controller, NodeRole::Broker],
            listen_addr,
            advertised_listener: "127.0.0.1:0".into(),
            log_dir,
            extra_log_dirs: Vec::new(),
            log_config: LogConfig::default(),
            node_id: NodeId(1),
            controller_listen_addr: controller_addr,
            controller_quorum_voters: vec![(NodeId(1), controller_addr.to_string())],
            controller_server_name: None,
            bootstrap_servers: vec![],
            directory_id: uuid::Uuid::from_u128(1),
            incarnation_id: uuid::Uuid::new_v4(),
            auto_join: false,
            observer_lag_bound: DEFAULT_OBSERVER_LAG_BOUND,
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
            metadata_max_bytes_between_snapshots: DEFAULT_METADATA_MAX_BYTES_BETWEEN_SNAPSHOTS,
            metadata_max_snapshot_interval: DEFAULT_METADATA_MAX_SNAPSHOT_INTERVAL,
            metadata_snapshot_interval_records: DEFAULT_METADATA_SNAPSHOT_INTERVAL_RECORDS,
            bootstrap_mode: BootstrapMode::Bootstrap,
            cluster_id: None,
            rack: None,
            replica_selector: crate::replica_selector::ReplicaSelectorKind::Leader,
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
            gssapi: None,
            oauthbearer_jwks_endpoint: None,
            oauthbearer_jwks_refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
            oauthbearer_idp_tls_trust: None,
            oauthbearer_max_session_lifetime_seconds: None,
            oauthbearer_jwks_signal_rx: std::sync::Arc::new(std::sync::Mutex::new(None)),
            oauthbearer_jwks_last_successful_fetch_ms: std::sync::Arc::new(
                std::sync::atomic::AtomicI64::new(0),
            ),
            oauthbearer_jwks_last_on_demand_refresh_ms: std::sync::Arc::new(
                std::sync::atomic::AtomicI64::new(0),
            ),
            oauthbearer_jwks_min_on_demand_pause: DEFAULT_JWKS_MIN_ON_DEMAND_PAUSE,
            features: test_feature_flags(),
            // Reaper disabled in tests; suites that exercise it set it low.
            txn_abort_cleanup_interval: std::time::Duration::ZERO,
            next_gen_consumer_group: Box::new(
                crate::coordinator::unified::config::NextGenConfig::default(),
            ),
            share_group: Box::new(
                crate::coordinator::unified::share::config::ShareGroupConfig::default(),
            ),
            streams_group: Box::new(
                crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
            ),
            share_coordinator: Box::new(
                crate::share_coordinator::config::ShareCoordinatorConfig::default(),
            ),
            leader_imbalance_check_interval_secs: DEFAULT_LEADER_IMBALANCE_CHECK_INTERVAL_SECS,
            leader_imbalance_per_broker_percentage: DEFAULT_LEADER_IMBALANCE_PER_BROKER_PERCENTAGE,
            #[cfg(any(test, feature = "test-helpers"))]
            cleaner_interval_override: None,
            // Short interval so hot-reload tests don't wait long for a
            // watcher tick. Tests that don't care can ignore it.
            tls_reload_interval: std::time::Duration::from_millis(200),
            // Tests opt into the metrics endpoint individually by
            // setting this to `Some(127.0.0.1:0)`; sharing a default
            // port would race in parallel test runs.
            metrics_listen_addr: None,
            client_metrics_otlp_endpoint: None,
            // Disable the disk scanner by default in tests so the
            // background task doesn't tick during short-lived fixtures.
            // Integration tests enable this explicitly when needed.
            partition_disk_scan_interval_secs: 0,
            max_incremental_fetch_session_cache_slots:
                DEFAULT_MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS,
            // Connection caps unlimited by default (Kafka's
            // Integer.MAX_VALUE); the enforcement path treats usize::MAX
            // as "no cap" and never increments the per-IP map.
            max_connections: usize::MAX,
            max_connections_per_ip: usize::MAX,
            // Tests opt into delegation tokens by setting
            // `delegation_token_secret_key`; default off keeps the
            // four DT RPCs returning DELEGATION_TOKEN_AUTH_DISABLED.
            delegation_token_secret_key: None,
            delegation_token_max_lifetime_ms: DEFAULT_DELEGATION_TOKEN_MAX_LIFETIME_MS,
            delegation_token_expiry_check_interval_ms:
                DEFAULT_DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS,
            delegation_token_default_renew_period_ms: DEFAULT_DELEGATION_TOKEN_RENEW_PERIOD_MS,
            // Tiered storage off by default in tests.
            remote_storage_backend: None,
            // Tests that turn tiered storage on want quick offload, so the
            // for_tests default is well below the 30s production value.
            remote_log_manager_interval: std::time::Duration::from_secs(2),
            // Tests use the in-memory RLMM fixture.
            remote_log_metadata: RlmmKind::InMemory,
            // Audit enabled by default (secure-by-default / `FedRAMP` MLA).
            audit_enabled: true,
            audit_topic: DEFAULT_AUDIT_TOPIC.to_string(),
            audit_signing_key_path: None,
            audit_signing_key_id: None,
            audit_checkpoint_every_n: DEFAULT_AUDIT_CHECKPOINT_EVERY_N,
            audit_checkpoint_every_secs: DEFAULT_AUDIT_CHECKPOINT_EVERY_SECS,
            audit_spool_dir: std::path::PathBuf::from(DEFAULT_AUDIT_SPOOL_DIR),
            audit_spool_max_bytes: DEFAULT_AUDIT_SPOOL_MAX_BYTES,
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
        if self.roles.is_empty() {
            return Err(BrokerError::EmptyRoles);
        }
        if !self.is_controller()
            && self
                .controller_quorum_voters
                .iter()
                .any(|(id, _)| *id == self.node_id)
        {
            return Err(BrokerError::NonControllerIsVoter {
                node_id: self.node_id,
            });
        }

        let listeners = self.effective_listeners();

        // Bind-address collisions.
        for (i, listener) in listeners.iter().enumerate() {
            for other in listeners.iter().skip(i + 1) {
                if listener.bind_addr == other.bind_addr {
                    return Err(BrokerError::ListenerConflict {
                        a: listener.name.clone(),
                        b: other.name.clone(),
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
        // sasl_mechanisms wins over the broker-wide default.
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

        // GSSAPI, wherever it is enabled (per-listener override or broker-wide
        // default), requires a `gssapi` config block. Without it the dispatch
        // path has nothing to authenticate against, so reject at startup rather
        // than panicking when the first GSSAPI client connects.
        let gssapi_enabled = listeners.iter().any(|l| {
            l.protocol.requires_sasl()
                && l.sasl_mechanisms
                    .as_deref()
                    .unwrap_or(&self.enabled_sasl_mechanisms)
                    .contains(&SaslMechanism::Gssapi)
        }) || self
            .enabled_sasl_mechanisms
            .contains(&SaslMechanism::Gssapi);
        if gssapi_enabled && self.gssapi.is_none() {
            return Err(BrokerError::GssapiConfigMissing);
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
        self.validate_positive_runtime_scalars()?;
        self.validate_additional_runtime_scalars()?;

        if self.self_registration_backoff_min > self.self_registration_backoff_max {
            return Err(BrokerError::InvalidRuntimeConfig(
                "self registration minimum backoff exceeds maximum".into(),
            ));
        }
        if self.rlmm_bootstrap_backoff_initial > self.rlmm_bootstrap_backoff_max {
            return Err(BrokerError::InvalidRuntimeConfig(
                "RLMM bootstrap initial backoff exceeds maximum".into(),
            ));
        }
        if self.replication.fetch_min_bytes > self.replication.fetch_max_bytes {
            return Err(BrokerError::InvalidRuntimeConfig(
                "replication fetch minimum bytes exceeds maximum".into(),
            ));
        }
        if self.replication.reconnect_initial_delay > self.replication.reconnect_delay_cap {
            return Err(BrokerError::InvalidRuntimeConfig(
                "replication reconnect initial delay exceeds cap".into(),
            ));
        }
        if self.heartbeat_interval_ms >= self.heartbeat_timeout_ms {
            return Err(BrokerError::InvalidRuntimeConfig(
                "broker heartbeat interval must be below timeout".into(),
            ));
        }
        if self.controller_heartbeat_interval >= self.controller_election_timeout {
            return Err(BrokerError::InvalidRuntimeConfig(
                "controller heartbeat interval must be below election timeout".into(),
            ));
        }
        if self.delegation_token_default_renew_period_ms > self.delegation_token_max_lifetime_ms {
            return Err(BrokerError::InvalidRuntimeConfig(
                "delegation token default renew period exceeds maximum lifetime".into(),
            ));
        }
        if self.client_metrics_stale_floor < self.client_metrics_eviction_tick {
            return Err(BrokerError::InvalidRuntimeConfig(
                "client metrics stale floor is below eviction tick".into(),
            ));
        }
        if self.unclean_recovery_aggressive_deadline > self.unclean_recovery_balanced_deadline {
            return Err(BrokerError::InvalidRuntimeConfig(
                "unclean recovery aggressive deadline exceeds balanced deadline".into(),
            ));
        }
        self.validate_additional_runtime_relations()?;

        let validate_group = |name: &str,
                              session_timeout: Duration,
                              heartbeat_interval: Duration,
                              min_session_timeout: Duration,
                              max_session_timeout: Duration,
                              min_heartbeat_interval: Duration,
                              max_heartbeat_interval: Duration,
                              max_size: Option<usize>| {
            if min_session_timeout.is_zero() {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} minimum session timeout must be positive"
                )));
            }
            if min_session_timeout > max_session_timeout {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} minimum session timeout exceeds maximum"
                )));
            }
            if !(min_session_timeout..=max_session_timeout).contains(&session_timeout) {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} session timeout is outside its bounds"
                )));
            }
            if min_heartbeat_interval.is_zero() {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} minimum heartbeat interval must be positive"
                )));
            }
            if min_heartbeat_interval > max_heartbeat_interval {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} minimum heartbeat interval exceeds maximum"
                )));
            }
            if !(min_heartbeat_interval..=max_heartbeat_interval).contains(&heartbeat_interval) {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} heartbeat interval is outside its bounds"
                )));
            }
            if max_size == Some(0) {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} maximum size must be positive"
                )));
            }
            Ok(())
        };

        let consumer = &self.next_gen_consumer_group;
        validate_group(
            "consumer group",
            consumer.session_timeout,
            consumer.heartbeat_interval,
            consumer.min_session_timeout,
            consumer.max_session_timeout,
            consumer.min_heartbeat_interval,
            consumer.max_heartbeat_interval,
            Some(consumer.max_size),
        )?;
        let share = &self.share_group;
        validate_group(
            "share group",
            share.session_timeout,
            share.heartbeat_interval,
            share.min_session_timeout,
            share.max_session_timeout,
            share.min_heartbeat_interval,
            share.max_heartbeat_interval,
            Some(share.max_size),
        )?;
        let streams = &self.streams_group;
        validate_group(
            "streams group",
            streams.session_timeout,
            streams.heartbeat_interval,
            streams.min_session_timeout,
            streams.max_session_timeout,
            streams.min_heartbeat_interval,
            streams.max_heartbeat_interval,
            None,
        )?;

        self.validate_leader_rebalance()
    }

    fn validate_leader_rebalance(&self) -> Result<(), BrokerError> {
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

    fn validate_additional_runtime_relations(&self) -> Result<(), BrokerError> {
        if self.socket_request_max_bytes > u32::MAX as usize {
            return Err(BrokerError::InvalidRuntimeConfig(
                "socket_request_max_bytes exceeds u32::MAX".into(),
            ));
        }
        if self.telemetry_decompressed_output_floor_bytes
            > self.telemetry_decompressed_output_ceiling_bytes
        {
            return Err(BrokerError::InvalidRuntimeConfig(
                "telemetry decompressed output floor exceeds ceiling".into(),
            ));
        }
        if self.inter_broker_server_name.is_empty() {
            return Err(BrokerError::InvalidRuntimeConfig(
                "inter_broker_server_name must be nonempty".into(),
            ));
        }
        if self.transaction_min_timeout_ms >= self.transaction_max_timeout_ms {
            return Err(BrokerError::InvalidRuntimeConfig(
                "transaction minimum timeout must be below maximum".into(),
            ));
        }
        if self.transaction_max_timeout_ms == i32::MAX {
            return Err(BrokerError::InvalidRuntimeConfig(
                "transaction maximum timeout must be below i32::MAX".into(),
            ));
        }
        Ok(())
    }

    fn validate_positive_runtime_scalars(&self) -> Result<(), BrokerError> {
        for (name, value) in [
            (
                "startup_leader_wait_timeout",
                self.startup_leader_wait_timeout,
            ),
            (
                "self_registration_backoff_min",
                self.self_registration_backoff_min,
            ),
            (
                "self_registration_backoff_max",
                self.self_registration_backoff_max,
            ),
            ("observer_poll_interval", self.observer_poll_interval),
            (
                "audit_spool_replay_interval",
                self.audit_spool_replay_interval,
            ),
            ("audit_stats_poll_interval", self.audit_stats_poll_interval),
            (
                "audit_partition_wait_timeout",
                self.audit_partition_wait_timeout,
            ),
            ("liveness_tick_interval", self.liveness_tick_interval),
            ("gauge_poll_interval", self.gauge_poll_interval),
            ("isr_scan_interval", self.isr_scan_interval),
            ("cleaner_interval", self.cleaner_interval),
            (
                "future_log_move_retry_backoff",
                self.future_log_move_retry_backoff,
            ),
            (
                "client_metrics_eviction_tick",
                self.client_metrics_eviction_tick,
            ),
            (
                "client_metrics_stale_floor",
                self.client_metrics_stale_floor,
            ),
            (
                "client_metrics_prom_snapshot_ttl",
                self.client_metrics_prom_snapshot_ttl,
            ),
            ("rlmm_reconcile_tick", self.rlmm_reconcile_tick),
            (
                "rlmm_bootstrap_backoff_initial",
                self.rlmm_bootstrap_backoff_initial,
            ),
            (
                "rlmm_bootstrap_backoff_max",
                self.rlmm_bootstrap_backoff_max,
            ),
            (
                "connection_creation_throttle_max",
                self.connection_creation_throttle_max,
            ),
            ("opa_http_timeout", self.opa_http_timeout),
            ("oauth_jwks_http_timeout", self.oauth_jwks_http_timeout),
            ("auto_join_retry_backoff", self.auto_join_retry_backoff),
            (
                "replication.throttle_exhausted_backoff",
                self.replication.throttle_exhausted_backoff,
            ),
            (
                "replication.send_error_backoff",
                self.replication.send_error_backoff,
            ),
            (
                "replication.unknown_topic_retry_delay",
                self.replication.unknown_topic_retry_delay,
            ),
            (
                "replication.epoch_fence_backoff",
                self.replication.epoch_fence_backoff,
            ),
            (
                "replication.unexpected_error_backoff",
                self.replication.unexpected_error_backoff,
            ),
            (
                "replication.reconnect_initial_delay",
                self.replication.reconnect_initial_delay,
            ),
            (
                "replication.reconnect_delay_cap",
                self.replication.reconnect_delay_cap,
            ),
            (
                "coordinator_session_expiry_tick",
                self.coordinator_session_expiry_tick,
            ),
            (
                "coordinator_shutdown_ack_timeout",
                self.coordinator_shutdown_ack_timeout,
            ),
            (
                "classic_group_initial_rebalance_delay",
                self.classic_group_initial_rebalance_delay,
            ),
            ("sync_group_follower_wait", self.sync_group_follower_wait),
            (
                "unclean_recovery_aggressive_deadline",
                self.unclean_recovery_aggressive_deadline,
            ),
            (
                "unclean_recovery_balanced_deadline",
                self.unclean_recovery_balanced_deadline,
            ),
            (
                "operator_recovery_deadline",
                self.operator_recovery_deadline,
            ),
            ("quota_throttle_max", self.quota_throttle_max),
            (
                "controller_heartbeat_interval",
                self.controller_heartbeat_interval,
            ),
            (
                "remote_log_manager_interval",
                self.remote_log_manager_interval,
            ),
            (
                "producer_id_expiration_scan_interval",
                self.producer_id_expiration_scan_interval,
            ),
        ] {
            if value.is_zero() {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} must be positive"
                )));
            }
        }
        for (name, value) in [
            (
                "client_metrics_default_interval_ms",
                self.client_metrics_default_interval_ms,
            ),
            (
                "client_metrics_telemetry_max_bytes",
                self.client_metrics_telemetry_max_bytes,
            ),
            (
                "replication.fetch_max_bytes",
                self.replication.fetch_max_bytes,
            ),
            (
                "replication.fetch_max_wait_ms",
                self.replication.fetch_max_wait_ms,
            ),
            (
                "replication.fetch_min_bytes",
                self.replication.fetch_min_bytes,
            ),
        ] {
            if value <= 0 {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} must be positive"
                )));
            }
        }
        for (name, value) in [
            ("heartbeat_interval_ms", self.heartbeat_interval_ms),
            ("replica_lag_time_max_ms", self.replica_lag_time_max_ms),
            (
                "metadata_max_bytes_between_snapshots",
                self.metadata_max_bytes_between_snapshots,
            ),
            (
                "metadata_snapshot_interval_records",
                self.metadata_snapshot_interval_records,
            ),
        ] {
            if value == 0 {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} must be positive"
                )));
            }
        }
        for (name, value) in [
            (
                "delegation_token_max_lifetime_ms",
                self.delegation_token_max_lifetime_ms,
            ),
            (
                "delegation_token_expiry_check_interval_ms",
                self.delegation_token_expiry_check_interval_ms,
            ),
            (
                "delegation_token_default_renew_period_ms",
                self.delegation_token_default_renew_period_ms,
            ),
        ] {
            if value <= 0 {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} must be positive"
                )));
            }
        }
        Ok(())
    }

    fn validate_additional_runtime_scalars(&self) -> Result<(), BrokerError> {
        if self.offsets_topic_metadata_wait_timeout.is_zero() {
            return Err(BrokerError::InvalidRuntimeConfig(
                "offsets_topic_metadata_wait_timeout must be positive".into(),
            ));
        }
        if self.offsets_topic_metadata_wait_timeout < Duration::from_millis(1) {
            return Err(BrokerError::InvalidRuntimeConfig(
                "offsets_topic_metadata_wait_timeout must be at least 1ms".into(),
            ));
        }
        for (name, value) in [
            (
                "self_registration_max_attempts",
                self.self_registration_max_attempts,
            ),
            ("observer_fetch_max_bytes", self.observer_fetch_max_bytes),
            (
                "client_metrics_stale_push_intervals",
                self.client_metrics_stale_push_intervals,
            ),
        ] {
            if value == 0 {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} must be positive"
                )));
            }
        }
        for (name, value) in [
            (
                "client_metrics_otlp_queue_capacity",
                self.client_metrics_otlp_queue_capacity,
            ),
            (
                "audit_event_queue_capacity",
                self.audit_event_queue_capacity,
            ),
            ("audit_tail_read_max_bytes", self.audit_tail_read_max_bytes),
            (
                "coordinator_actor_mailbox_capacity",
                self.coordinator_actor_mailbox_capacity,
            ),
            (
                "unclean_recovery_queue_capacity",
                self.unclean_recovery_queue_capacity,
            ),
            (
                "share_recovery_read_max_bytes",
                self.share_recovery_read_max_bytes,
            ),
            (
                "share_session_cache_max_when_unlimited",
                self.share_session_cache_max_when_unlimited,
            ),
            ("socket_request_max_bytes", self.socket_request_max_bytes),
            ("sendfile_min_bytes", self.sendfile_min_bytes),
            ("socket_send_buffer_bytes", self.socket_send_buffer_bytes),
            (
                "socket_receive_buffer_bytes",
                self.socket_receive_buffer_bytes,
            ),
            ("acl_max_principal_bytes", self.acl_max_principal_bytes),
            (
                "acl_max_resource_name_bytes",
                self.acl_max_resource_name_bytes,
            ),
            (
                "telemetry_max_decompression_ratio",
                self.telemetry_max_decompression_ratio,
            ),
            (
                "telemetry_decompressed_output_floor_bytes",
                self.telemetry_decompressed_output_floor_bytes,
            ),
            (
                "telemetry_decompressed_output_ceiling_bytes",
                self.telemetry_decompressed_output_ceiling_bytes,
            ),
            ("max_produce_group", self.max_produce_group),
            (
                "partition_writer_queue_depth",
                self.partition_writer_queue_depth,
            ),
            (
                "future_log_move_read_chunk_bytes",
                self.future_log_move_read_chunk_bytes,
            ),
        ] {
            if value == 0 {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} must be positive"
                )));
            }
        }
        for (name, value) in [
            ("audit_tail_window_offsets", self.audit_tail_window_offsets),
            ("producer_id_expiration_ms", self.producer_id_expiration_ms),
        ] {
            if value <= 0 {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} must be positive"
                )));
            }
        }
        for (name, value) in [
            (
                "default_min_insync_replicas",
                self.default_min_insync_replicas,
            ),
            (
                "share_state_num_partitions",
                self.share_coordinator.state_topic_num_partitions,
            ),
            (
                "transaction_state_num_partitions",
                self.transaction_state_num_partitions,
            ),
            (
                "transaction_min_timeout_ms",
                self.transaction_min_timeout_ms,
            ),
            (
                "transaction_max_timeout_ms",
                self.transaction_max_timeout_ms,
            ),
        ] {
            if value <= 0 {
                return Err(BrokerError::InvalidRuntimeConfig(format!(
                    "{name} must be positive"
                )));
            }
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
    /// When [`listeners`][Self::listeners] is empty (the default),
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

    /// True when this node hosts data partitions and registers as a broker.
    #[must_use]
    pub fn is_broker(&self) -> bool {
        self.roles.contains(&NodeRole::Broker)
    }

    /// True when this node participates in the `__cluster_metadata` quorum.
    #[must_use]
    pub fn is_controller(&self) -> bool {
        self.roles.contains(&NodeRole::Controller)
    }
}

impl Default for BrokerConfig {
    fn default() -> Self {
        let addr: SocketAddr = "127.0.0.1:9092".parse().expect("hard-coded valid addr");
        let controller_addr: SocketAddr = "127.0.0.1:9093".parse().expect("hard-coded valid addr");
        Self {
            startup_leader_wait_timeout: Duration::from_mins(2),
            self_registration_backoff_min: Duration::from_millis(100),
            self_registration_backoff_max: Duration::from_secs(5),
            observer_poll_interval: Duration::from_millis(100),
            audit_spool_replay_interval: Duration::from_secs(2),
            audit_stats_poll_interval: Duration::from_secs(1),
            audit_partition_wait_timeout: Duration::from_secs(10),
            liveness_tick_interval: Duration::from_secs(1),
            gauge_poll_interval: Duration::from_secs(1),
            isr_scan_interval: Duration::from_secs(1),
            cleaner_interval: Duration::from_secs(30),
            future_log_move_retry_backoff: Duration::from_millis(50),
            client_metrics_eviction_tick: Duration::from_mins(1),
            client_metrics_stale_floor: Duration::from_mins(10),
            client_metrics_default_interval_ms: 300_000,
            client_metrics_otlp_queue_capacity: 256,
            client_metrics_telemetry_max_bytes: 1_048_576,
            client_metrics_prom_snapshot_ttl: Duration::from_mins(5),
            rlmm_reconcile_tick: Duration::from_secs(30),
            rlmm_bootstrap_backoff_initial: Duration::from_millis(250),
            rlmm_bootstrap_backoff_max: Duration::from_secs(10),
            connection_creation_throttle_max: Duration::from_secs(1),
            opa_http_timeout: Duration::from_secs(5),
            oauth_jwks_http_timeout: Duration::from_secs(10),
            auto_join_retry_backoff: Duration::from_millis(500),
            replication: ReplicationRuntimeConfig::default(),
            coordinator_session_expiry_tick: Duration::from_secs(1),
            coordinator_shutdown_ack_timeout: Duration::from_secs(5),
            classic_group_initial_rebalance_delay: Duration::from_secs(3),
            sync_group_follower_wait: Duration::from_secs(30),
            unclean_recovery_aggressive_deadline: Duration::from_secs(2),
            unclean_recovery_balanced_deadline: Duration::from_secs(30),
            operator_recovery_deadline: Duration::from_secs(25),
            quota_throttle_max: Duration::from_secs(1),
            self_registration_max_attempts: 8,
            observer_fetch_max_bytes: 1_048_576,
            audit_event_queue_capacity: 8_192,
            audit_tail_window_offsets: 4_096,
            audit_tail_read_max_bytes: 1_048_576,
            offsets_topic_metadata_wait_timeout: Duration::from_secs(30),
            client_metrics_stale_push_intervals: 3,
            coordinator_actor_mailbox_capacity: 64,
            unclean_recovery_queue_capacity: 256,
            share_recovery_read_max_bytes: 1_048_576,
            share_session_cache_max_when_unlimited: 10_000,
            socket_request_max_bytes: 104_857_600,
            sendfile_min_bytes: 32_768,
            socket_send_buffer_bytes: 1_048_576,
            socket_receive_buffer_bytes: 1_048_576,
            acl_max_principal_bytes: 256,
            acl_max_resource_name_bytes: 256,
            telemetry_max_decompression_ratio: 100,
            telemetry_decompressed_output_floor_bytes: 16_777_216,
            telemetry_decompressed_output_ceiling_bytes: 1_073_741_824,
            inter_broker_server_name: "localhost".to_string(),
            producer_id_expiration_ms: 86_400_000,
            producer_id_expiration_scan_interval: Duration::from_mins(10),
            max_produce_group: 1_024,
            partition_writer_queue_depth: 64,
            default_min_insync_replicas: 1,
            future_log_move_read_chunk_bytes: 1_048_576,
            transaction_state_num_partitions: 50,
            transaction_min_timeout_ms: 1_000,
            transaction_max_timeout_ms: 900_000,
            broker_id: 1,
            roles: vec![NodeRole::Controller, NodeRole::Broker],
            listen_addr: addr,
            advertised_listener: addr.to_string(),
            log_dir: PathBuf::from("./crabka-data"),
            extra_log_dirs: Vec::new(),
            log_config: LogConfig::default(),
            node_id: NodeId(1),
            controller_listen_addr: controller_addr,
            controller_quorum_voters: vec![(NodeId(1), controller_addr.to_string())],
            controller_server_name: None,
            bootstrap_servers: vec![],
            directory_id: uuid::Uuid::from_u128(1),
            incarnation_id: uuid::Uuid::nil(),
            auto_join: false,
            observer_lag_bound: DEFAULT_OBSERVER_LAG_BOUND,
            heartbeat_interval_ms: DEFAULT_HEARTBEAT_INTERVAL_MS,
            heartbeat_timeout_ms: DEFAULT_HEARTBEAT_TIMEOUT_MS,
            replica_lag_time_max_ms: DEFAULT_REPLICA_LAG_TIME_MAX_MS,
            controller_election_timeout: duration_ms(DEFAULT_CONTROLLER_ELECTION_TIMEOUT_MS),
            controller_heartbeat_interval: duration_ms(DEFAULT_CONTROLLER_HEARTBEAT_INTERVAL_MS),
            metadata_max_bytes_between_snapshots: DEFAULT_METADATA_MAX_BYTES_BETWEEN_SNAPSHOTS,
            metadata_max_snapshot_interval: DEFAULT_METADATA_MAX_SNAPSHOT_INTERVAL,
            metadata_snapshot_interval_records: DEFAULT_METADATA_SNAPSHOT_INTERVAL_RECORDS,
            bootstrap_mode: BootstrapMode::Bootstrap,
            cluster_id: None,
            rack: None,
            replica_selector: crate::replica_selector::ReplicaSelectorKind::Leader,
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
            gssapi: None,
            oauthbearer_jwks_endpoint: None,
            oauthbearer_jwks_refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
            oauthbearer_idp_tls_trust: None,
            oauthbearer_max_session_lifetime_seconds: None,
            oauthbearer_jwks_signal_rx: std::sync::Arc::new(std::sync::Mutex::new(None)),
            oauthbearer_jwks_last_successful_fetch_ms: std::sync::Arc::new(
                std::sync::atomic::AtomicI64::new(0),
            ),
            oauthbearer_jwks_last_on_demand_refresh_ms: std::sync::Arc::new(
                std::sync::atomic::AtomicI64::new(0),
            ),
            oauthbearer_jwks_min_on_demand_pause: DEFAULT_JWKS_MIN_ON_DEMAND_PAUSE,
            features: default_feature_flags(),
            // KIP-98/KIP-939 idle-transaction reaper cadence (Kafka's
            // `transaction.abort.timed.out.transaction.cleanup.interval.ms`).
            txn_abort_cleanup_interval: duration_ms(DEFAULT_TXN_ABORT_CLEANUP_INTERVAL_MS),
            next_gen_consumer_group: Box::new(
                crate::coordinator::unified::config::NextGenConfig::default(),
            ),
            share_group: Box::new(
                crate::coordinator::unified::share::config::ShareGroupConfig::default(),
            ),
            streams_group: Box::new(
                crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
            ),
            share_coordinator: Box::new(
                crate::share_coordinator::config::ShareCoordinatorConfig::default(),
            ),
            leader_imbalance_check_interval_secs: DEFAULT_LEADER_IMBALANCE_CHECK_INTERVAL_SECS,
            leader_imbalance_per_broker_percentage: DEFAULT_LEADER_IMBALANCE_PER_BROKER_PERCENTAGE,
            #[cfg(any(test, feature = "test-helpers"))]
            cleaner_interval_override: None,
            tls_reload_interval: duration_ms(DEFAULT_TLS_RELOAD_INTERVAL_MS),
            // Default to `None` so multi-broker library users (and
            // multi-broker tests) don't race on a fixed port. The
            // `crabka-broker` binary opts in to `Some(0.0.0.0:9404)`
            // via its `--metrics-listen-addr` CLI flag — the operator
            // sets that via env, so production deployments still get
            // metrics by default.
            metrics_listen_addr: None,
            client_metrics_otlp_endpoint: None,
            partition_disk_scan_interval_secs: 60,
            max_incremental_fetch_session_cache_slots:
                DEFAULT_MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS,
            // Connection caps unlimited by default, matching Kafka's
            // max.connections / max.connections.per.ip (Integer.MAX_VALUE).
            max_connections: usize::MAX,
            max_connections_per_ip: usize::MAX,
            // Master key off by default. Operators flip this on
            // via `CRABKA_DELEGATION_TOKEN_SECRET_KEY` env var or the
            // `[delegation_token] secret_key` TOML stanza.
            delegation_token_secret_key: None,
            delegation_token_max_lifetime_ms: DEFAULT_DELEGATION_TOKEN_MAX_LIFETIME_MS,
            delegation_token_expiry_check_interval_ms:
                DEFAULT_DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS,
            delegation_token_default_renew_period_ms: DEFAULT_DELEGATION_TOKEN_RENEW_PERIOD_MS,
            // Tiered storage off by default. Operators enable it
            // via `[remote_storage] storage_dir` in `broker.toml`.
            remote_storage_backend: None,
            remote_log_manager_interval: duration_ms(DEFAULT_REMOTE_LOG_MANAGER_INTERVAL_MS),
            // Production default: topic-backed RLMM. `bootstrap` and
            // `snapshot_dir` are empty; the broker derives them at startup.
            remote_log_metadata: RlmmKind::TopicBacked(KafkaRlmmConfig::default()),
            // Audit enabled by default (secure-by-default / `FedRAMP` MLA).
            audit_enabled: true,
            audit_topic: DEFAULT_AUDIT_TOPIC.to_string(),
            audit_signing_key_path: None,
            audit_signing_key_id: None,
            audit_checkpoint_every_n: DEFAULT_AUDIT_CHECKPOINT_EVERY_N,
            audit_checkpoint_every_secs: DEFAULT_AUDIT_CHECKPOINT_EVERY_SECS,
            audit_spool_dir: std::path::PathBuf::from(DEFAULT_AUDIT_SPOOL_DIR),
            audit_spool_max_bytes: DEFAULT_AUDIT_SPOOL_MAX_BYTES,
        }
    }
}

const fn duration_ms(milliseconds: u64) -> std::time::Duration {
    std::time::Duration::from_millis(milliseconds)
}

const fn test_feature_flags() -> BrokerFeatureFlags {
    BrokerFeatureFlags {
        oauthbearer_jwks_ignore_key_use: false,
        auto_leader_rebalance_enable: false,
        transaction_two_phase_commit_enable: false,
    }
}

const fn default_feature_flags() -> BrokerFeatureFlags {
    BrokerFeatureFlags {
        oauthbearer_jwks_ignore_key_use: false,
        auto_leader_rebalance_enable: true,
        transaction_two_phase_commit_enable: false,
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::BrokerError as BrokerStartError;

    #[test]
    fn operational_policy_defaults_match_existing_behavior() {
        let config = BrokerConfig::default();

        assert!(
            (
                config.startup_leader_wait_timeout,
                config.self_registration_backoff_min,
                config.self_registration_backoff_max,
                config.observer_poll_interval,
                config.audit_spool_replay_interval,
                config.audit_stats_poll_interval,
                config.audit_partition_wait_timeout,
                config.liveness_tick_interval,
            ) == (
                std::time::Duration::from_mins(2),
                std::time::Duration::from_millis(100),
                std::time::Duration::from_secs(5),
                std::time::Duration::from_millis(100),
                std::time::Duration::from_secs(2),
                std::time::Duration::from_secs(1),
                std::time::Duration::from_secs(10),
                std::time::Duration::from_secs(1),
            )
        );
        assert!(
            (
                config.gauge_poll_interval,
                config.isr_scan_interval,
                config.cleaner_interval,
                config.future_log_move_retry_backoff,
                config.client_metrics_eviction_tick,
                config.client_metrics_stale_floor,
            ) == (
                std::time::Duration::from_secs(1),
                std::time::Duration::from_secs(1),
                std::time::Duration::from_secs(30),
                std::time::Duration::from_millis(50),
                std::time::Duration::from_mins(1),
                std::time::Duration::from_mins(10),
            )
        );
        assert!(
            (
                config.client_metrics_default_interval_ms,
                config.client_metrics_otlp_queue_capacity,
                config.client_metrics_telemetry_max_bytes,
                config.client_metrics_prom_snapshot_ttl,
                config.rlmm_reconcile_tick,
                config.rlmm_bootstrap_backoff_initial,
                config.rlmm_bootstrap_backoff_max,
                config.connection_creation_throttle_max,
                config.opa_http_timeout,
                config.oauth_jwks_http_timeout,
                config.auto_join_retry_backoff,
            ) == (
                300_000,
                256,
                1_048_576,
                std::time::Duration::from_mins(5),
                std::time::Duration::from_secs(30),
                std::time::Duration::from_millis(250),
                std::time::Duration::from_secs(10),
                std::time::Duration::from_secs(1),
                std::time::Duration::from_secs(5),
                std::time::Duration::from_secs(10),
                std::time::Duration::from_millis(500),
            )
        );
        assert!(
            config.replication
                == ReplicationRuntimeConfig {
                    fetch_max_bytes: 1_048_576,
                    fetch_max_wait_ms: 500,
                    fetch_min_bytes: 1,
                    throttle_exhausted_backoff: std::time::Duration::from_millis(100),
                    send_error_backoff: std::time::Duration::from_secs(1),
                    unknown_topic_retry_delay: std::time::Duration::from_millis(100),
                    epoch_fence_backoff: std::time::Duration::from_millis(200),
                    unexpected_error_backoff: std::time::Duration::from_millis(500),
                    reconnect_initial_delay: std::time::Duration::from_millis(100),
                    reconnect_delay_cap: std::time::Duration::from_secs(5),
                }
        );
        assert!(
            (
                config.coordinator_session_expiry_tick,
                config.coordinator_shutdown_ack_timeout,
                config.classic_group_initial_rebalance_delay,
                config.sync_group_follower_wait,
                config.unclean_recovery_aggressive_deadline,
                config.unclean_recovery_balanced_deadline,
                config.operator_recovery_deadline,
                config.quota_throttle_max,
            ) == (
                std::time::Duration::from_secs(1),
                std::time::Duration::from_secs(5),
                std::time::Duration::from_secs(3),
                std::time::Duration::from_secs(30),
                std::time::Duration::from_secs(2),
                std::time::Duration::from_secs(30),
                std::time::Duration::from_secs(25),
                std::time::Duration::from_secs(1),
            )
        );
    }

    fn additional_policy_snapshot(config: BrokerConfig) -> [String; 31] {
        [
            config.self_registration_max_attempts.to_string(),
            config.observer_fetch_max_bytes.to_string(),
            config.audit_event_queue_capacity.to_string(),
            config.audit_tail_window_offsets.to_string(),
            config.audit_tail_read_max_bytes.to_string(),
            config
                .offsets_topic_metadata_wait_timeout
                .as_millis()
                .to_string(),
            config.client_metrics_stale_push_intervals.to_string(),
            config.coordinator_actor_mailbox_capacity.to_string(),
            config.unclean_recovery_queue_capacity.to_string(),
            config.share_recovery_read_max_bytes.to_string(),
            config.share_session_cache_max_when_unlimited.to_string(),
            config.socket_request_max_bytes.to_string(),
            config.sendfile_min_bytes.to_string(),
            config.socket_send_buffer_bytes.to_string(),
            config.socket_receive_buffer_bytes.to_string(),
            config.acl_max_principal_bytes.to_string(),
            config.acl_max_resource_name_bytes.to_string(),
            config.telemetry_max_decompression_ratio.to_string(),
            config.telemetry_decompressed_output_floor_bytes.to_string(),
            config
                .telemetry_decompressed_output_ceiling_bytes
                .to_string(),
            config.inter_broker_server_name,
            config.producer_id_expiration_ms.to_string(),
            config
                .producer_id_expiration_scan_interval
                .as_millis()
                .to_string(),
            config.max_produce_group.to_string(),
            config.partition_writer_queue_depth.to_string(),
            config.default_min_insync_replicas.to_string(),
            config.future_log_move_read_chunk_bytes.to_string(),
            config
                .share_coordinator
                .state_topic_num_partitions
                .to_string(),
            config.transaction_state_num_partitions.to_string(),
            config.transaction_min_timeout_ms.to_string(),
            config.transaction_max_timeout_ms.to_string(),
        ]
    }

    #[test]
    fn additional_operational_policy_defaults_match_existing_behavior() {
        let actual = additional_policy_snapshot(BrokerConfig::default());
        assert!(
            actual
                == [
                    "8",
                    "1048576",
                    "8192",
                    "4096",
                    "1048576",
                    "30000",
                    "3",
                    "64",
                    "256",
                    "1048576",
                    "10000",
                    "104857600",
                    "32768",
                    "1048576",
                    "1048576",
                    "256",
                    "256",
                    "100",
                    "16777216",
                    "1073741824",
                    "localhost",
                    "86400000",
                    "600000",
                    "1024",
                    "64",
                    "1",
                    "1048576",
                    "50",
                    "50",
                    "1000",
                    "900000",
                ]
        );
        assert!(additional_policy_snapshot(BrokerConfig::for_tests(PathBuf::new())) == actual);
    }

    fn assert_invalid_runtime(config: BrokerConfig, expected: &str) {
        let Err(BrokerError::InvalidRuntimeConfig(actual)) = config.validate() else {
            panic!("expected invalid runtime config");
        };
        assert!(actual == expected);
    }

    #[test]
    fn rejects_invalid_runtime_relations() {
        let mut config = BrokerConfig::default();
        config.self_registration_backoff_min = config.self_registration_backoff_max * 2;
        assert_invalid_runtime(config, "self registration minimum backoff exceeds maximum");

        let mut config = BrokerConfig::default();
        config.rlmm_bootstrap_backoff_initial = config.rlmm_bootstrap_backoff_max * 2;
        assert_invalid_runtime(config, "RLMM bootstrap initial backoff exceeds maximum");

        let mut config = BrokerConfig::default();
        config.replication.fetch_min_bytes = config.replication.fetch_max_bytes + 1;
        assert_invalid_runtime(config, "replication fetch minimum bytes exceeds maximum");

        let mut config = BrokerConfig::default();
        config.replication.reconnect_initial_delay = config.replication.reconnect_delay_cap * 2;
        assert_invalid_runtime(config, "replication reconnect initial delay exceeds cap");

        let mut config = BrokerConfig::default();
        config.heartbeat_interval_ms = config.heartbeat_timeout_ms;
        assert_invalid_runtime(config, "broker heartbeat interval must be below timeout");

        let mut config = BrokerConfig::default();
        config.controller_heartbeat_interval = config.controller_election_timeout;
        assert_invalid_runtime(
            config,
            "controller heartbeat interval must be below election timeout",
        );

        let mut config = BrokerConfig::default();
        config.delegation_token_default_renew_period_ms =
            config.delegation_token_max_lifetime_ms + 1;
        assert_invalid_runtime(
            config,
            "delegation token default renew period exceeds maximum lifetime",
        );

        let mut config = BrokerConfig::default();
        config.client_metrics_stale_floor = config.client_metrics_eviction_tick / 2;
        assert_invalid_runtime(config, "client metrics stale floor is below eviction tick");

        let mut config = BrokerConfig::default();
        config.unclean_recovery_aggressive_deadline = config.unclean_recovery_balanced_deadline * 2;
        assert_invalid_runtime(
            config,
            "unclean recovery aggressive deadline exceeds balanced deadline",
        );
    }

    #[test]
    fn rejects_non_positive_runtime_scalars() {
        let cases: [(&str, fn(&mut BrokerConfig)); 19] = [
            ("startup_leader_wait_timeout", |c| {
                c.startup_leader_wait_timeout = Duration::ZERO;
            }),
            ("cleaner_interval", |c| {
                c.cleaner_interval = Duration::ZERO;
            }),
            ("client_metrics_default_interval_ms", |c| {
                c.client_metrics_default_interval_ms = 0;
            }),
            ("client_metrics_telemetry_max_bytes", |c| {
                c.client_metrics_telemetry_max_bytes = 0;
            }),
            ("client_metrics_otlp_queue_capacity", |c| {
                c.client_metrics_otlp_queue_capacity = 0;
            }),
            ("replication.fetch_max_bytes", |c| {
                c.replication.fetch_max_bytes = 0;
            }),
            ("replication.fetch_max_wait_ms", |c| {
                c.replication.fetch_max_wait_ms = 0;
            }),
            ("replication.fetch_min_bytes", |c| {
                c.replication.fetch_min_bytes = 0;
            }),
            ("replication.send_error_backoff", |c| {
                c.replication.send_error_backoff = Duration::ZERO;
            }),
            ("heartbeat_interval_ms", |c| c.heartbeat_interval_ms = 0),
            ("replica_lag_time_max_ms", |c| {
                c.replica_lag_time_max_ms = 0;
            }),
            ("controller_heartbeat_interval", |c| {
                c.controller_heartbeat_interval = Duration::ZERO;
            }),
            ("metadata_max_bytes_between_snapshots", |c| {
                c.metadata_max_bytes_between_snapshots = 0;
            }),
            ("metadata_snapshot_interval_records", |c| {
                c.metadata_snapshot_interval_records = 0;
            }),
            ("delegation_token_max_lifetime_ms", |c| {
                c.delegation_token_max_lifetime_ms = 0;
            }),
            ("delegation_token_expiry_check_interval_ms", |c| {
                c.delegation_token_expiry_check_interval_ms = 0;
            }),
            ("delegation_token_default_renew_period_ms", |c| {
                c.delegation_token_default_renew_period_ms = -1;
            }),
            ("remote_log_manager_interval", |c| {
                c.remote_log_manager_interval = Duration::ZERO;
            }),
            ("delegation_token_max_lifetime_ms", |c| {
                c.delegation_token_max_lifetime_ms = -1;
            }),
        ];

        for (name, invalidate) in cases {
            let mut config = BrokerConfig::default();
            invalidate(&mut config);
            assert_invalid_runtime(config, &format!("{name} must be positive"));
        }
    }

    #[test]
    fn rejects_invalid_additional_runtime_scalars() {
        let cases: &[(&str, fn(&mut BrokerConfig))] = &[
            ("self_registration_max_attempts must be positive", |c| {
                c.self_registration_max_attempts = 0;
            }),
            ("observer_fetch_max_bytes must be positive", |c| {
                c.observer_fetch_max_bytes = 0;
            }),
            ("audit_event_queue_capacity must be positive", |c| {
                c.audit_event_queue_capacity = 0;
            }),
            ("audit_tail_window_offsets must be positive", |c| {
                c.audit_tail_window_offsets = 0;
            }),
            ("audit_tail_read_max_bytes must be positive", |c| {
                c.audit_tail_read_max_bytes = 0;
            }),
            (
                "offsets_topic_metadata_wait_timeout must be positive",
                |c| c.offsets_topic_metadata_wait_timeout = Duration::ZERO,
            ),
            (
                "offsets_topic_metadata_wait_timeout must be at least 1ms",
                |c| c.offsets_topic_metadata_wait_timeout = Duration::from_nanos(1),
            ),
            (
                "client_metrics_stale_push_intervals must be positive",
                |c| c.client_metrics_stale_push_intervals = 0,
            ),
            ("coordinator_actor_mailbox_capacity must be positive", |c| {
                c.coordinator_actor_mailbox_capacity = 0
            }),
            ("unclean_recovery_queue_capacity must be positive", |c| {
                c.unclean_recovery_queue_capacity = 0
            }),
            ("share_recovery_read_max_bytes must be positive", |c| {
                c.share_recovery_read_max_bytes = 0;
            }),
            (
                "share_session_cache_max_when_unlimited must be positive",
                |c| c.share_session_cache_max_when_unlimited = 0,
            ),
            ("socket_request_max_bytes must be positive", |c| {
                c.socket_request_max_bytes = 0;
            }),
            ("sendfile_min_bytes must be positive", |c| {
                c.sendfile_min_bytes = 0;
            }),
            ("socket_send_buffer_bytes must be positive", |c| {
                c.socket_send_buffer_bytes = 0;
            }),
            ("socket_receive_buffer_bytes must be positive", |c| {
                c.socket_receive_buffer_bytes = 0;
            }),
            ("acl_max_principal_bytes must be positive", |c| {
                c.acl_max_principal_bytes = 0;
            }),
            ("acl_max_resource_name_bytes must be positive", |c| {
                c.acl_max_resource_name_bytes = 0;
            }),
            ("telemetry_max_decompression_ratio must be positive", |c| {
                c.telemetry_max_decompression_ratio = 0
            }),
            (
                "telemetry_decompressed_output_floor_bytes must be positive",
                |c| c.telemetry_decompressed_output_floor_bytes = 0,
            ),
            (
                "telemetry_decompressed_output_ceiling_bytes must be positive",
                |c| c.telemetry_decompressed_output_ceiling_bytes = 0,
            ),
            ("producer_id_expiration_ms must be positive", |c| {
                c.producer_id_expiration_ms = 0;
            }),
            (
                "producer_id_expiration_scan_interval must be positive",
                |c| c.producer_id_expiration_scan_interval = Duration::ZERO,
            ),
            ("max_produce_group must be positive", |c| {
                c.max_produce_group = 0;
            }),
            ("partition_writer_queue_depth must be positive", |c| {
                c.partition_writer_queue_depth = 0;
            }),
            ("default_min_insync_replicas must be positive", |c| {
                c.default_min_insync_replicas = 0;
            }),
            ("future_log_move_read_chunk_bytes must be positive", |c| {
                c.future_log_move_read_chunk_bytes = 0
            }),
            ("share_state_num_partitions must be positive", |c| {
                c.share_coordinator.state_topic_num_partitions = 0;
            }),
            ("transaction_state_num_partitions must be positive", |c| {
                c.transaction_state_num_partitions = 0;
            }),
            ("transaction_min_timeout_ms must be positive", |c| {
                c.transaction_min_timeout_ms = 0;
            }),
            ("transaction_max_timeout_ms must be positive", |c| {
                c.transaction_max_timeout_ms = 0;
            }),
        ];

        for (expected, invalidate) in cases {
            let mut config = BrokerConfig::default();
            invalidate(&mut config);
            assert_invalid_runtime(config, expected);
        }
    }

    #[test]
    fn rejects_invalid_additional_runtime_relations() {
        let cases: &[(&str, fn(&mut BrokerConfig))] = &[
            ("socket_request_max_bytes exceeds u32::MAX", |c| {
                c.socket_request_max_bytes = usize::try_from(u64::from(u32::MAX) + 1).unwrap()
            }),
            ("telemetry decompressed output floor exceeds ceiling", |c| {
                c.telemetry_decompressed_output_floor_bytes =
                    c.telemetry_decompressed_output_ceiling_bytes + 1;
            }),
            ("inter_broker_server_name must be nonempty", |c| {
                c.inter_broker_server_name.clear();
            }),
            ("transaction minimum timeout must be below maximum", |c| {
                c.transaction_min_timeout_ms = c.transaction_max_timeout_ms
            }),
            ("transaction maximum timeout must be below i32::MAX", |c| {
                c.transaction_max_timeout_ms = i32::MAX
            }),
        ];

        for (expected, invalidate) in cases {
            let mut config = BrokerConfig::default();
            invalidate(&mut config);
            assert_invalid_runtime(config, expected);
        }
    }

    #[test]
    fn rejects_invalid_group_bounds_and_defaults() {
        let mut config = BrokerConfig::default();
        config.next_gen_consumer_group.min_session_timeout =
            config.next_gen_consumer_group.max_session_timeout * 2;
        assert_invalid_runtime(
            config,
            "consumer group minimum session timeout exceeds maximum",
        );

        let mut config = BrokerConfig::default();
        config.next_gen_consumer_group.session_timeout =
            config.next_gen_consumer_group.max_session_timeout * 2;
        assert_invalid_runtime(
            config,
            "consumer group session timeout is outside its bounds",
        );

        let mut config = BrokerConfig::default();
        config.share_group.min_heartbeat_interval = config.share_group.max_heartbeat_interval * 2;
        assert_invalid_runtime(
            config,
            "share group minimum heartbeat interval exceeds maximum",
        );

        let mut config = BrokerConfig::default();
        config.share_group.heartbeat_interval = config.share_group.max_heartbeat_interval * 2;
        assert_invalid_runtime(
            config,
            "share group heartbeat interval is outside its bounds",
        );
    }

    #[test]
    fn production_default_selects_topic_backed_rlmm() {
        let c = BrokerConfig::default();
        assert!(matches!(c.remote_log_metadata, RlmmKind::TopicBacked(_)));
    }

    #[test]
    fn test_default_selects_in_memory_rlmm() {
        let c = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        assert!(matches!(c.remote_log_metadata, RlmmKind::InMemory));
    }

    #[test]
    fn kafka_rlmm_config_default_has_sane_topic_settings() {
        let c = KafkaRlmmConfig::default();
        check!(c.num_partitions == 50);
        check!(c.replication == 3);
        check!(c.bootstrap.is_empty());
        check!(c.snapshot_dir == std::path::PathBuf::new());
        check!(c.snapshot_interval == DEFAULT_RLMM_SNAPSHOT_INTERVAL);
        check!(c.security.is_none());
    }

    #[test]
    fn kafka_rlmm_config_carries_snapshot_settings() {
        let c = KafkaRlmmConfig {
            bootstrap: "127.0.0.1:9092".into(),
            num_partitions: 50,
            replication: 1,
            snapshot_interval: std::time::Duration::from_mins(1),
            snapshot_dir: std::path::PathBuf::from("/data/remote-log-metadata"),
            security: None,
        };
        assert!(c.snapshot_interval == std::time::Duration::from_mins(1));
        assert!(c.snapshot_dir == std::path::PathBuf::from("/data/remote-log-metadata"));
    }

    #[test]
    fn kafka_rlmm_config_carries_optional_security() {
        let c = KafkaRlmmConfig {
            bootstrap: "127.0.0.1:9092".into(),
            num_partitions: 1,
            replication: 1,
            snapshot_interval: std::time::Duration::from_mins(1),
            snapshot_dir: std::path::PathBuf::from("/data/remote-log-metadata"),
            security: None,
        };
        assert!(c.security.is_none());
    }

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
    fn accepts_distinct_listener_bind_addresses() {
        let c = base();
        assert!(c.validate().is_ok());
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
        assert!(c.listen_addr.port() == 9092);
        assert!(c.broker_id == 1);
    }

    #[test]
    fn for_tests_uses_port_0() {
        let c = BrokerConfig::for_tests(PathBuf::from("/tmp"));
        assert!(c.listen_addr.port() == 0);
    }

    #[test]
    fn defaults_use_conservative_raft_timings() {
        let c = BrokerConfig::default();
        assert!(
            c.controller_election_timeout
                == std::time::Duration::from_millis(DEFAULT_CONTROLLER_ELECTION_TIMEOUT_MS)
        );
        assert!(
            c.controller_heartbeat_interval
                == std::time::Duration::from_millis(DEFAULT_CONTROLLER_HEARTBEAT_INTERVAL_MS)
        );
    }

    #[test]
    fn default_metadata_snapshot_interval() {
        let cfg = BrokerConfig::default();
        assert!(cfg.metadata_snapshot_interval_records == 10_000);
    }

    #[test]
    fn for_tests_uses_20_mib_metadata_snapshot_threshold() {
        let cfg = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        let mib = 1024 * 1024;
        assert!(cfg.metadata_max_bytes_between_snapshots == 20 * mib);
        assert!(cfg.metadata_max_bytes_between_snapshots / mib == 20);
    }

    #[test]
    fn for_tests_uses_short_raft_timings_for_fast_failover() {
        let c = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        // Short enough that a 3-broker test can detect a dead leader and
        // re-elect within a few hundred ms — the failover tests
        // need failover well under their 10s producer timeout.
        assert!(c.controller_election_timeout <= std::time::Duration::from_millis(750));
        assert!(c.controller_heartbeat_interval <= std::time::Duration::from_millis(200));
    }

    #[test]
    fn defaults_use_bootstrap_mode() {
        let c = BrokerConfig::default();
        assert!(c.bootstrap_mode == BootstrapMode::Bootstrap);
    }

    #[test]
    fn for_tests_uses_bootstrap_mode() {
        let c = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        assert!(c.bootstrap_mode == BootstrapMode::Bootstrap);
    }

    #[test]
    fn all_log_dirs_keeps_primary_first_and_deduplicates_extras() {
        let primary = std::path::PathBuf::from("/data/primary");
        let extra = std::path::PathBuf::from("/data/extra");
        let mut c = BrokerConfig::for_tests(primary.clone());
        c.extra_log_dirs = vec![extra.clone(), primary.clone(), extra.clone()];

        assert!(c.all_log_dirs() == vec![primary, extra]);
    }

    #[test]
    fn defaults_to_combined_roles() {
        let d = BrokerConfig::default();
        assert!(
            (d.is_controller(), d.is_broker(), d.roles)
                == (true, true, vec![NodeRole::Controller, NodeRole::Broker]),
            "default node is a combined controller+broker with the combined role set"
        );

        let t = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        assert!(t.is_controller() && t.is_broker());
    }

    #[test]
    fn controller_only_is_not_a_broker() {
        let c = BrokerConfig {
            roles: vec![NodeRole::Controller],
            ..BrokerConfig::default()
        };
        assert!(c.is_controller());
        assert!(!c.is_broker());
    }

    #[test]
    fn broker_only_is_not_a_controller() {
        let c = BrokerConfig {
            roles: vec![NodeRole::Broker],
            ..BrokerConfig::default()
        };
        assert!(c.is_broker());
        assert!(!c.is_controller());
    }

    #[test]
    fn rejects_empty_roles() {
        let c = BrokerConfig {
            roles: vec![],
            ..BrokerConfig::default()
        };
        assert!(matches!(c.validate(), Err(BrokerError::EmptyRoles)));
    }

    #[test]
    fn rejects_broker_only_node_listed_as_its_own_voter() {
        // node_id 1 is in the default single-voter quorum; a broker-only
        // node must not be a voter of itself.
        let c = BrokerConfig {
            roles: vec![NodeRole::Broker],
            node_id: NodeId(1),
            controller_quorum_voters: vec![(NodeId(1), "127.0.0.1:9093".to_string())],
            ..BrokerConfig::default()
        };
        assert!(matches!(
            c.validate(),
            Err(BrokerError::NonControllerIsVoter {
                node_id: crabka_raft::NodeId(1)
            })
        ));
    }

    #[test]
    fn combined_default_passes_role_validation() {
        BrokerConfig::default()
            .validate()
            .expect("combined default validates");
    }

    #[test]
    fn controller_only_does_not_register() {
        let c = BrokerConfig {
            roles: vec![NodeRole::Controller],
            ..BrokerConfig::default()
        };
        // Registration is gated on is_broker(); a controller-only node skips it.
        assert!(!c.is_broker());
    }

    #[test]
    fn controller_only_hosts_no_partitions() {
        let c = BrokerConfig {
            roles: vec![NodeRole::Controller],
            ..BrokerConfig::default()
        };
        // Partition scan/recovery is gated on is_broker().
        assert!(!c.is_broker());
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
    fn rejects_gssapi_mechanism_without_gssapi_config() {
        let c = BrokerConfig {
            controller_listener_protocol: ListenerProtocol::Plaintext,
            enabled_sasl_mechanisms: vec![SaslMechanism::Gssapi],
            gssapi: None,
            ..BrokerConfig::default()
        };
        assert!(matches!(
            c.validate(),
            Err(BrokerError::GssapiConfigMissing)
        ));
    }

    #[test]
    fn auto_leader_rebalance_defaults_to_true_in_default() {
        let c = BrokerConfig::default();
        check!(c.features.auto_leader_rebalance_enable);
        check!(c.leader_imbalance_check_interval_secs == 300);
        check!(c.leader_imbalance_per_broker_percentage == 10);
    }

    #[test]
    fn auto_leader_rebalance_defaults_to_false_in_for_tests() {
        let c = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        assert!(!c.features.auto_leader_rebalance_enable);
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

    #[test]
    fn rebalance_threshold_100_is_allowed_by_validate() {
        let c = BrokerConfig {
            leader_imbalance_per_broker_percentage: 100,
            ..BrokerConfig::default()
        };

        c.validate()
            .expect("100% leader imbalance threshold is the maximum valid value");
    }

    #[test]
    fn rack_and_selector_default_off() {
        let c = BrokerConfig::default();
        assert!(c.rack == None);
        assert!(c.replica_selector == crate::replica_selector::ReplicaSelectorKind::Leader);
        let t = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        assert!(t.rack == None);
        assert!(t.replica_selector == crate::replica_selector::ReplicaSelectorKind::Leader);
    }
}
