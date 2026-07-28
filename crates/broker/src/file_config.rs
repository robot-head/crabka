//! TOML file-config surface for the `crabka-broker` binary.
//!
//! Deserialized by `--config-file PATH` in `bin/broker.rs` and merged
//! into [`crate::BrokerConfig`]. Only `[[listeners]]`,
//! `inter_broker_listener_name`, and (passively) `[server_properties]`
//! are consumed; other top-level keys are accepted but ignored.

use std::{net::SocketAddr, sync::Arc};

use crabka_security::ListenerProtocol;
use crabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
    percent, secs,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::config::ListenerSpec;

/// Failures surfaced by [`FileConfig::apply_to`]. Each variant
/// corresponds to a specific misconfiguration the broker can diagnose
/// at startup; the variants exist (rather than a single `String`
/// fallthrough) so the binary entry point can log structured context.
#[derive(Debug, thiserror::Error)]
pub enum FileConfigError {
    /// A `[section]` referenced by another field is missing — e.g.
    /// `[authorization] type = "opa"` without an `[authorization.opa]`
    /// table. The payload names the missing section.
    #[error("missing required TOML section: {0}")]
    MissingSection(String),
    /// `OpaAuthorizer::new` failed (see [`crate::authorizer::opa::OpaConfigError`]).
    /// The payload is the underlying error's `Debug` form — formatted
    /// here rather than at the call site so the binary entry point can
    /// log a single string.
    #[error("OPA authorizer configuration error: {0}")]
    OpaConfig(String),
    /// A TOML section's contents conflict in a way only the apply step
    /// can diagnose — e.g. `[remote_storage]` carrying both `storage_dir`
    /// (local backend) and `[remote_storage.s3]` (object-store backend).
    #[error("invalid config: {0}")]
    InvalidConfig(String),
    /// A `controller_quorum_voters` entry is malformed (no `@`, non-numeric
    /// node id) or its `<host>:<port>` could not be DNS-resolved within the
    /// startup retry budget. The payload is the offending entry plus the
    /// underlying reason.
    #[error("invalid controller_quorum_voters entry: {0}")]
    InvalidQuorumVoter(String),
}

/// Top-level shape of `broker.toml`. `serde(deny_unknown_fields)` is
/// off — new fields may be added and old binaries should warn rather
/// than refuse to start.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq)]
pub struct FileConfig {
    /// Operational runtime policy. Present values replace the current broker
    /// value; absent values retain it.
    #[serde(default)]
    pub runtime: Option<RuntimeFileConfig>,
    pub broker_id: Option<i32>,
    pub log_dir: Option<String>,
    /// Additional JBOD data directories (KIP-113). Maps to
    /// [`crate::BrokerConfig::extra_log_dirs`].
    #[serde(default)]
    pub extra_log_dirs: Vec<String>,
    /// KIP-392: this broker's rack id. Maps to `BrokerConfig::rack`.
    #[serde(default)]
    pub rack: Option<String>,

    /// KIP-392: replica selector name (`"leader"` | `"rack-aware"`).
    /// Maps to `BrokerConfig::replica_selector`.
    #[serde(default)]
    pub replica_selector: Option<String>,
    /// How often this broker sends `BrokerHeartbeat` to the controller leader.
    /// Absent leaves the `BrokerConfig` default intact.
    #[serde(default)]
    pub heartbeat_interval_ms: Option<u64>,
    /// Controller-side session timeout for broker heartbeats. Absent leaves the
    /// `BrokerConfig` default intact.
    #[serde(default)]
    pub heartbeat_timeout_ms: Option<u64>,
    /// Maximum follower lag before the leader proposes ISR shrink. Absent
    /// leaves the `BrokerConfig` default intact.
    #[serde(default)]
    pub replica_lag_time_max_ms: Option<u64>,
    /// Controller election timeout. Absent leaves the `BrokerConfig` default intact.
    #[serde(default)]
    pub controller_election_timeout_ms: Option<u64>,
    /// Controller heartbeat interval. Absent leaves the `BrokerConfig` default intact.
    #[serde(default)]
    pub controller_heartbeat_interval_ms: Option<u64>,
    pub inter_broker_listener_name: Option<String>,

    /// Maximum number of live broker connections across all listeners
    /// (Kafka `max.connections`). Connections accepted past this ceiling
    /// are closed immediately. Absent leaves the `BrokerConfig` default
    /// `usize::MAX` (unlimited), matching Kafka's `Integer.MAX_VALUE`.
    #[serde(default)]
    pub max_connections: Option<usize>,

    /// Maximum number of live connections from any single client IP
    /// (Kafka `max.connections.per.ip`). Absent leaves the `BrokerConfig`
    /// default `usize::MAX` (unlimited).
    #[serde(default)]
    pub max_connections_per_ip: Option<usize>,

    /// KIP-595 static controller quorum voter set. Each entry is
    /// `<node_id>@<host>:<port>` pointing at a broker's controller listener
    /// (port 9093). At apply time each entry is parsed (NOT DNS-resolved) and
    /// its `<host>:<port>` is carried verbatim into
    /// `BrokerConfig::controller_quorum_voters`. The inter-broker dialer
    /// re-resolves the host on every (re)connect (`TcpStream::connect`), so a
    /// peer that restarts on a new pod IP (a `StatefulSet` pod keeps its stable
    /// DNS name but gets a fresh A record) is reached again without restarting
    /// this broker — pre-resolving here would freeze the peer's boot-time IP
    /// and strand a rejoining voter. Empty leaves the single self-voter the
    /// binary seeds (standalone).
    #[serde(default)]
    pub controller_quorum_voters: Vec<String>,

    /// TLS server name (SNI) presented when dialing a PEER's controller
    /// listener for the KIP-595 quorum. The operator renders the shared
    /// headless-Service FQDN here — a SAN on every broker's serving cert —
    /// so mTLS validation succeeds no matter which peer (resolved to a pod
    /// IP) is dialed. Absent falls back to `"localhost"`. Maps to
    /// [`crate::BrokerConfig::controller_server_name`].
    #[serde(default)]
    pub controller_server_name: Option<String>,

    #[serde(default)]
    pub listeners: Vec<FileListener>,
    #[serde(default)]
    pub server_properties: std::collections::BTreeMap<String, String>,

    /// Controller listener security protocol. When `Some(Ssl)`
    /// the controller listener terminates TLS using `tls_config`.
    #[serde(default)]
    #[schemars(with = "Option<String>")]
    pub controller_listener_protocol: Option<ListenerProtocol>,

    /// TLS material for the controller listener (and any
    /// listener whose `protocol` is TLS-bearing).
    #[serde(default)]
    pub tls_config: Option<FileTlsConfig>,

    /// SASL/OAUTHBEARER validator tuning. Only relevant when a
    /// listener enables the `OAUTHBEARER` mechanism.
    #[serde(default)]
    pub oauthbearer: Option<FileOAuthBearerConfig>,

    /// KIP-48: delegation-token master key + lifetime knobs.
    /// Env var `CRABKA_DELEGATION_TOKEN_SECRET_KEY` wins over `secret_key`
    /// here. When neither source provides a key, the broker disables
    /// delegation-token auth.
    #[serde(default)]
    pub delegation_token: Option<FileDelegationTokenConfig>,

    /// Principals that are unconditionally authorized for
    /// all operations, including KIP-48 delegation-token `act-as`. The
    /// operator emits `super_users = ["ANONYMOUS"]` when
    /// `Kafka.spec.delegationToken` is set so its PLAINTEXT
    /// inter-broker reconcile loop can mint per-`KafkaUser` tokens.
    /// `None` and `Some(empty)` are equivalent — both leave
    /// `BrokerConfig.super_users` empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub super_users: Option<Vec<String>>,

    /// KIP-405: tiered-storage enablement. Setting
    /// `storage_dir` turns tiered storage on broker-wide and roots the
    /// local reference `RemoteStorageManager` there.
    #[serde(default)]
    pub remote_storage: Option<FileRemoteStorageConfig>,

    /// Pluggable cluster authorizer + super-user list.
    /// `None` ⇒ [`crate::authorizer::AllowAllAuthorizer`] with empty
    /// super-users (default-on-no-config behavior). When `Some`, the
    /// `type` field selects the authorizer implementation; for
    /// `type = "opa"`, the `[authorization.opa]` subtable is required.
    #[serde(default)]
    pub authorization: Option<FileAuthorizationConfig>,

    /// `[process]` section — `KRaft` `process.roles`. Absent / empty leaves
    /// the `BrokerConfig` default `[Controller, Broker]`.
    #[serde(default)]
    pub process: Option<FileProcessConfig>,

    /// SASL/GSSAPI (Kerberos) accept-path config. Broker-global —
    /// there is one `[gssapi]` block per broker. Relevant when a listener
    /// enables the `GSSAPI` mechanism.
    #[serde(default)]
    pub gssapi: Option<FileGssapiConfig>,

    /// Credentials this broker uses to authenticate *to* peer brokers
    /// (inter-broker initiate path). Only the `gssapi` variant is supported.
    #[serde(default)]
    pub inter_broker_credentials: Option<FileInterBrokerCredentials>,

    /// `FedRAMP` 20x MLA audit subsystem configuration.
    /// Absent → secure default (enabled, standard internal topic name).
    #[serde(default)]
    pub audit: Option<FileAuditConfig>,
}

/// Validated operational policy loaded from `[runtime]`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeFileConfig {
    pub startup_leader_wait_timeout_ms: Option<u64>,
    pub self_registration_backoff_min_ms: Option<u64>,
    pub self_registration_backoff_max_ms: Option<u64>,
    pub observer_poll_interval_ms: Option<u64>,
    pub audit_spool_replay_interval_ms: Option<u64>,
    pub audit_stats_poll_interval_ms: Option<u64>,
    pub audit_partition_wait_timeout_ms: Option<u64>,
    pub liveness_tick_interval_ms: Option<u64>,
    pub gauge_poll_interval_ms: Option<u64>,
    pub isr_scan_interval_ms: Option<u64>,
    pub cleaner_interval_ms: Option<u64>,
    pub future_log_move_retry_backoff_ms: Option<u64>,
    pub client_metrics_eviction_tick_ms: Option<u64>,
    pub client_metrics_stale_floor_ms: Option<u64>,
    pub client_metrics_default_interval_ms: Option<i32>,
    pub client_metrics_telemetry_max_bytes: Option<i32>,
    pub client_metrics_prom_snapshot_ttl_ms: Option<u64>,
    pub rlmm_reconcile_tick_ms: Option<u64>,
    pub rlmm_bootstrap_backoff_initial_ms: Option<u64>,
    pub rlmm_bootstrap_backoff_max_ms: Option<u64>,
    pub connection_creation_throttle_max_ms: Option<u64>,
    pub opa_http_timeout_ms: Option<u64>,
    pub oauth_jwks_http_timeout_ms: Option<u64>,
    pub auto_join_retry_backoff_ms: Option<u64>,
    pub auto_join_voter_request_timeout_ms: Option<u64>,
    pub replication_fetch_max_bytes: Option<i32>,
    pub replication_fetch_max_wait_ms: Option<i32>,
    pub replication_fetch_min_bytes: Option<i32>,
    pub replication_throttle_exhausted_backoff_ms: Option<u64>,
    pub replication_send_error_backoff_ms: Option<u64>,
    pub replication_unknown_topic_retry_delay_ms: Option<u64>,
    pub replication_epoch_fence_backoff_ms: Option<u64>,
    pub replication_unexpected_error_backoff_ms: Option<u64>,
    pub replication_reconnect_initial_delay_ms: Option<u64>,
    pub replication_reconnect_delay_cap_ms: Option<u64>,
    pub coordinator_session_expiry_tick_ms: Option<u64>,
    pub coordinator_shutdown_ack_timeout_ms: Option<u64>,
    pub consumer_group_session_timeout_ms: Option<u64>,
    pub consumer_group_heartbeat_interval_ms: Option<u64>,
    pub consumer_group_min_session_timeout_ms: Option<u64>,
    pub consumer_group_max_session_timeout_ms: Option<u64>,
    pub consumer_group_min_heartbeat_interval_ms: Option<u64>,
    pub consumer_group_max_heartbeat_interval_ms: Option<u64>,
    pub consumer_group_max_size: Option<usize>,
    pub classic_group_initial_rebalance_delay_ms: Option<u64>,
    pub sync_group_follower_wait_ms: Option<u64>,
    pub unclean_recovery_aggressive_deadline_ms: Option<u64>,
    pub unclean_recovery_balanced_deadline_ms: Option<u64>,
    pub operator_recovery_deadline_ms: Option<u64>,
    pub quota_throttle_max_ms: Option<u64>,
    pub self_registration_max_attempts: Option<u32>,
    pub observer_fetch_max_bytes: Option<u32>,
    pub audit_event_queue_capacity: Option<usize>,
    pub audit_tail_window_offsets: Option<i64>,
    pub audit_tail_read_max_bytes: Option<usize>,
    pub offsets_topic_metadata_wait_timeout_ms: Option<u64>,
    pub client_metrics_stale_push_intervals: Option<u32>,
    pub client_metrics_otlp_queue_capacity: Option<usize>,
    pub coordinator_actor_mailbox_capacity: Option<usize>,
    pub unclean_recovery_queue_capacity: Option<usize>,
    pub share_recovery_read_max_bytes: Option<usize>,
    pub share_session_cache_max_when_unlimited: Option<usize>,
    pub socket_request_max_bytes: Option<usize>,
    pub sendfile_min_bytes: Option<usize>,
    pub socket_send_buffer_bytes: Option<usize>,
    pub socket_receive_buffer_bytes: Option<usize>,
    pub acl_max_principal_bytes: Option<usize>,
    pub acl_max_resource_name_bytes: Option<usize>,
    pub telemetry_max_decompression_ratio: Option<usize>,
    pub telemetry_decompressed_output_floor_bytes: Option<usize>,
    pub telemetry_decompressed_output_ceiling_bytes: Option<usize>,
    pub inter_broker_server_name: Option<String>,
    pub producer_id_expiration_ms: Option<i64>,
    pub producer_id_expiration_scan_interval_ms: Option<u64>,
    pub max_produce_group: Option<usize>,
    pub partition_writer_queue_depth: Option<usize>,
    pub default_min_insync_replicas: Option<i32>,
    pub future_log_move_read_chunk_bytes: Option<usize>,
    pub share_state_num_partitions: Option<i32>,
    pub share_state_replication_factor: Option<i16>,
    pub transaction_state_num_partitions: Option<i32>,
    pub transaction_state_replication_factor: Option<i16>,
    pub transaction_min_timeout_ms: Option<i32>,
    pub transaction_max_timeout_ms: Option<i32>,
    pub partition_disk_scan_interval_secs: Option<u64>,
    pub observer_lag_bound: Option<u64>,
    pub heartbeat_interval_ms: Option<u64>,
    pub heartbeat_timeout_ms: Option<u64>,
    pub replica_lag_time_max_ms: Option<u64>,
    pub controller_election_timeout_ms: Option<u64>,
    pub controller_heartbeat_interval_ms: Option<u64>,
    pub controlled_shutdown_drain_timeout_ms: Option<u64>,
    pub metadata_max_bytes_between_snapshots: Option<u64>,
    pub metadata_max_snapshot_interval_ms: Option<u64>,
    pub metadata_snapshot_interval_records: Option<u64>,
    pub txn_abort_cleanup_interval_ms: Option<u64>,
    pub leader_imbalance_check_interval_secs: Option<u64>,
    pub leader_imbalance_per_broker_percentage: Option<u32>,
    pub tls_reload_interval_ms: Option<u64>,
    pub max_incremental_fetch_session_cache_slots: Option<usize>,
    pub max_connections: Option<usize>,
    pub max_connections_per_ip: Option<usize>,
    pub delegation_token_max_lifetime_ms: Option<i64>,
    pub delegation_token_expiry_check_interval_ms: Option<i64>,
    pub delegation_token_default_renew_period_ms: Option<i64>,
    pub remote_log_manager_interval_ms: Option<u64>,

    pub share_group_enable: Option<bool>,
    pub share_group_session_timeout_ms: Option<u64>,
    pub share_group_heartbeat_interval_ms: Option<u64>,
    pub share_group_record_lock_duration_ms: Option<u64>,
    pub share_group_max_delivery_attempts: Option<i16>,
    pub share_group_max_inflight_records: Option<i32>,
    pub share_group_isolation_level: Option<String>,
    pub streams_group_session_timeout_ms: Option<u64>,
    pub streams_group_heartbeat_interval_ms: Option<u64>,
    pub streams_internal_topic_replication_factor: Option<i16>,
    pub streams_group_num_standby_replicas: Option<i32>,
    pub streams_group_num_warmup_replicas: Option<i32>,
    pub streams_group_acceptable_recovery_lag: Option<i64>,
    pub streams_group_task_offset_interval_ms: Option<u64>,
    pub streams_group_assignor: Option<String>,
}

/// TOML shape of `[remote_storage]`. Maps to
/// [`crate::BrokerConfig::remote_storage_backend`].
///
/// Exactly one of `storage_dir` (local filesystem), `[remote_storage.s3]`
/// (S3-compatible object store), or `[remote_storage.gcs]` (native Google
/// Cloud Storage) should be set. Setting more than one errors at load time.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileRemoteStorageConfig {
    /// Root directory for the local `LocalTieredStorage` backend.
    pub storage_dir: Option<String>,
    /// S3-compatible backend parameters. Omit to use `storage_dir`.
    pub s3: Option<FileRemoteStorageS3Config>,
    /// Native Google Cloud Storage backend parameters. Omit to use
    /// `storage_dir` or `[remote_storage.s3]`.
    pub gcs: Option<FileRemoteStorageGcsConfig>,
    /// Opt-in to the topic-backed `RemoteLogMetadataManager`.
    /// When absent, the broker uses the in-memory fixture.
    pub kafka_metadata: Option<FileKafkaRlmmConfig>,
}

/// TOML shape of `[remote_storage.kafka_metadata]`. Maps to
/// [`crate::config::KafkaRlmmConfig`].
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileKafkaRlmmConfig {
    /// `host:port` the manager dials to reach its own broker.
    /// May be empty; the broker derives the address from the inter-broker
    /// listener at startup.
    #[serde(default)]
    pub bootstrap: String,
    /// Partition count for `__remote_log_metadata` on first creation.
    /// Defaults to 50 (Kafka's
    /// `remote.log.metadata.topic.num.partitions`).
    #[serde(default)]
    pub num_partitions: Option<i32>,
    /// Replication factor for `__remote_log_metadata` on first
    /// creation. Defaults to 3 (Kafka's
    /// `remote.log.metadata.topic.replication.factor`).
    #[serde(default)]
    pub replication: Option<i32>,
    /// Explicit opt-out: run the non-durable in-memory RLMM instead of the
    /// topic-backed default. Tests / single-node dev only.
    #[serde(default)]
    pub in_memory: bool,
}

/// TOML shape of `[remote_storage.s3]`. Maps to
/// [`crabka_remote_storage::S3Config`].
#[derive(Clone, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileRemoteStorageS3Config {
    /// S3 bucket name.
    pub bucket: String,
    /// AWS region. Required even for non-AWS endpoints (use any value).
    pub region: String,
    /// Optional key prefix inside the bucket (lets multiple clusters
    /// share a bucket).
    #[serde(default)]
    pub prefix: Option<String>,
    /// Optional custom endpoint URL (e.g. `MinIO` or Cloudflare R2).
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Explicit access key id. Falls back to the AWS credential chain
    /// (env vars, instance profile, …) when omitted.
    #[serde(default)]
    pub access_key_id: Option<String>,
    /// Explicit secret access key. Falls back to the AWS credential chain
    /// when omitted.
    #[serde(default)]
    pub secret_access_key: Option<String>,
    /// Allow plaintext HTTP (off-by-default; required by `MinIO` running
    /// without TLS).
    #[serde(default)]
    pub allow_http: bool,
    /// Optional override of the multipart-upload threshold (bytes). When
    /// `None`, [`crabka_remote_storage::DEFAULT_MULTIPART_THRESHOLD`]
    /// applies. Operators typically leave this alone; lower it to force
    /// multipart on smaller segments for testing.
    #[serde(default)]
    pub multipart_threshold: Option<u64>,
    /// Optional override of the per-part multipart chunk size (bytes).
    /// When `None`, [`crabka_remote_storage::DEFAULT_MULTIPART_CHUNK_SIZE`]
    /// applies. AWS requires parts ≥ 5 MiB except the last; `MinIO`
    /// tolerates smaller values.
    #[serde(default)]
    pub multipart_chunk_size: Option<usize>,
}

impl std::fmt::Debug for FileRemoteStorageS3Config {
    /// Redacts the credential fields so a stray `{:?}` / tracing call never
    /// leaks them. Mirrors the hand-written `Debug` on
    /// [`crabka_remote_storage::S3Config`].
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redact = |opt: &Option<String>| opt.as_ref().map(|_| "***");
        f.debug_struct("FileRemoteStorageS3Config")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("prefix", &self.prefix)
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &redact(&self.access_key_id))
            .field("secret_access_key", &redact(&self.secret_access_key))
            .field("allow_http", &self.allow_http)
            .field("multipart_threshold", &self.multipart_threshold)
            .field("multipart_chunk_size", &self.multipart_chunk_size)
            .finish()
    }
}

/// TOML shape of `[remote_storage.gcs]`. Maps to
/// [`crabka_remote_storage::GcsConfig`].
///
/// Omitting all credential fields (`service_account_path`,
/// `service_account_key`, `application_credentials_path`) selects GKE
/// Workload Identity / Application Default Credentials (keyless) — the
/// primary production path.
#[derive(Clone, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileRemoteStorageGcsConfig {
    /// GCS bucket name.
    pub bucket: String,
    /// Optional key prefix inside the bucket (lets multiple clusters
    /// share a bucket).
    #[serde(default)]
    pub prefix: Option<String>,
    /// Path to a service-account JSON key file. Omit (along with the
    /// other credential fields) to use Workload Identity / ADC.
    #[serde(default)]
    pub service_account_path: Option<String>,
    /// Inline service-account JSON key. Omit (along with the other
    /// credential fields) to use Workload Identity / ADC.
    #[serde(default)]
    pub service_account_key: Option<String>,
    /// Path to an Application Default Credentials JSON file. Omit (along
    /// with the other credential fields) to use Workload Identity / ADC.
    #[serde(default)]
    pub application_credentials_path: Option<String>,
    /// Optional custom GCS API base URL (for emulators / fakes).
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Allow plaintext HTTP (off-by-default; required by emulators
    /// running without TLS).
    #[serde(default)]
    pub allow_http: bool,
    /// Optional override of the multipart-upload threshold (bytes). When
    /// `None`, [`crabka_remote_storage::DEFAULT_MULTIPART_THRESHOLD`]
    /// applies. Operators typically leave this alone; lower it to force
    /// multipart on smaller segments for testing.
    #[serde(default)]
    pub multipart_threshold: Option<u64>,
    /// Optional override of the per-part multipart chunk size (bytes).
    /// When `None`, [`crabka_remote_storage::DEFAULT_MULTIPART_CHUNK_SIZE`]
    /// applies.
    #[serde(default)]
    pub multipart_chunk_size: Option<usize>,
}

impl std::fmt::Debug for FileRemoteStorageGcsConfig {
    /// Redacts the credential fields so a stray `{:?}` / tracing call never
    /// leaks them. Mirrors the hand-written `Debug` on
    /// [`crabka_remote_storage::GcsConfig`].
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redact = |opt: &Option<String>| opt.as_ref().map(|_| "***");
        f.debug_struct("FileRemoteStorageGcsConfig")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("service_account_path", &redact(&self.service_account_path))
            .field("service_account_key", &redact(&self.service_account_key))
            .field(
                "application_credentials_path",
                &redact(&self.application_credentials_path),
            )
            .field("endpoint", &self.endpoint)
            .field("allow_http", &self.allow_http)
            .field("multipart_threshold", &self.multipart_threshold)
            .field("multipart_chunk_size", &self.multipart_chunk_size)
            .finish()
    }
}

/// TOML shape of `[authorization]`. `type` (renamed to `authz_type` on
/// the Rust side to avoid shadowing the keyword) defaults to
/// `AllowAll`; `super_users` is the principal bypass list consulted by
/// every concrete authorizer impl.
///
/// `deny_unknown_fields` so a misspelled `super_user` typo at the top
/// of the `[authorization]` block is rejected at parse time rather
/// than silently producing the wrong authorizer.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileAuthorizationConfig {
    #[serde(rename = "type", default)]
    pub authz_type: AuthzType,
    #[serde(default)]
    pub super_users: Vec<String>,
    /// `Some` iff `authz_type == Opa`. Required in that case;
    /// `apply_to` returns [`FileConfigError::MissingSection`] when
    /// omitted.
    #[serde(default)]
    pub opa: Option<FileOpaConfig>,
}

/// Which [`crate::authorizer::Authorizer`] impl to instantiate.
/// `snake_case` to match the spec's `type = "allow_all" | "simple" |
/// "opa"` wire shape.
#[derive(Debug, Clone, Copy, Default, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthzType {
    #[default]
    AllowAll,
    Simple,
    Opa,
}

/// TOML shape of `[authorization.opa]`. Mirrors the constructor
/// arguments of [`crate::authorizer::opa::OpaAuthorizer::new`]. Defaults
/// are picked to match Strimzi's `KafkaAuthorizationOpa` (`50_000` LRU
/// entries, 1 h TTL, fail-closed on OPA error).
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileOpaConfig {
    /// OPA decision endpoint URL — must include the data-API path,
    /// e.g. `http://opa:8181/v1/data/kafka/authz/allow`.
    pub url: String,
    /// **Security-sensitive.** Permit the operation when the OPA call
    /// fails (timeout, 5xx, parse error). When `true`, an OPA outage
    /// authorizes *every* request (fail-open). Default `false`
    /// (fail-closed) — omitting this field denies on error, matching the
    /// upstream Open Policy Agent Kafka plugin's `allow.on.error = false`.
    #[serde(default)]
    pub allow_on_error: bool,
    /// LRU cache capacity, in entries. Default `50_000`.
    #[serde(default = "default_opa_maximum_cache_size")]
    pub maximum_cache_size: usize,
    /// Decision TTL, in milliseconds. Default `3_600_000` (1 h).
    #[serde(default = "default_opa_expire_after_ms")]
    pub expire_after_ms: i64,
}

/// Default OPA decision-cache capacity, in entries. Matches Strimzi's
/// `KafkaAuthorizationOpa` default.
const DEFAULT_OPA_MAXIMUM_CACHE_SIZE: usize = 50_000;

/// Default OPA decision TTL: 1 hour, in milliseconds. Matches Strimzi's
/// `KafkaAuthorizationOpa` default.
const DEFAULT_OPA_EXPIRE_AFTER_MS: i64 = 60 * 60 * 1_000;

fn default_opa_maximum_cache_size() -> usize {
    DEFAULT_OPA_MAXIMUM_CACHE_SIZE
}

fn default_opa_expire_after_ms() -> i64 {
    DEFAULT_OPA_EXPIRE_AFTER_MS
}

/// TOML shape of `[delegation_token]`. Maps to the three `delegation_token_*`
/// fields on [`crate::BrokerConfig`].
#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileDelegationTokenConfig {
    /// HMAC master key. Overridden by `CRABKA_DELEGATION_TOKEN_SECRET_KEY`
    /// when set. Bytes are wrapped in
    /// [`crabka_security::SecretBytes`] before reaching `BrokerConfig`.
    pub secret_key: Option<String>,
    /// Hard upper bound on token lifetime, ms. Default 7 days.
    pub max_lifetime_ms: Option<i64>,
    /// Background sweep cadence, ms. Default 1 hour.
    pub expiry_check_interval_ms: Option<i64>,
    /// Default renew period — the initial `expiry_timestamp_ms` offset
    /// at create time and the implicit renew period when
    /// `RenewDelegationToken.renew_period_ms == -1`. Distinct from
    /// `max_lifetime_ms` (the absolute ceiling). Default 24 hours.
    pub default_renew_period_ms: Option<i64>,
}

/// `[process]` TOML section — `KRaft` `process.roles`.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileProcessConfig {
    /// Role strings: `"controller"`, `"broker"` (case-insensitive). Empty
    /// or absent leaves the `BrokerConfig` default `[Controller, Broker]`.
    #[serde(default)]
    pub roles: Vec<String>,
}

/// TOML shape of `[oauthbearer]`. Maps to
/// [`crabka_security::OAuthBearerValidator`]. Setting `jwks_endpoint_uri`
/// selects the signed-JWT validator; setting
/// `introspection_endpoint_uri` selects the RFC 7662 introspection
/// validator; the two endpoint URIs are mutually
/// exclusive. With neither set, the unsecured-JWS validator
/// (development only) is used.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq)]
pub struct FileOAuthBearerConfig {
    /// Claim whose value becomes the principal name. Default `sub`.
    #[serde(default)]
    pub principal_claim_name: Option<String>,
    /// Optional `JsonPath` expression (RFC 9535, via
    /// jsonpath-rust) evaluated against the token claim set. Token is
    /// rejected when the expression yields empty/null/false. Compiled
    /// once at broker startup; malformed expressions panic with a
    /// descriptive error.
    #[serde(default)]
    pub custom_claim_check: Option<String>,
    /// Optional JWT `typ` header check. When set, JWT-mode
    /// validators (unsecured + signed JWS) require the JWT header's
    /// `typ` field to equal this string. Introspection-mode skips
    /// (no JWT header). Ignored when unset.
    #[serde(default)]
    pub valid_token_type: Option<String>,
    /// Clock-skew tolerance, in milliseconds, for `exp` / `iat` / `nbf`.
    /// Default 30000.
    #[serde(default)]
    pub allowable_clock_skew_ms: Option<i64>,

    /// JWKS endpoint URL. When set, tokens are validated as signed
    /// JWTs (RS256 / ES256) against the keys fetched from this URL, and the
    /// broker spawns a background refresher. When unset, the unsecured-JWS
    /// (`alg:none`) development validator is used.
    #[serde(default)]
    pub jwks_endpoint_uri: Option<String>,
    /// When set, the token `iss` claim must equal this. Signed
    /// validator only.
    #[serde(default)]
    pub valid_issuer_uri: Option<String>,
    /// When set, the token `aud` claim must contain this. Signed
    /// validator only.
    #[serde(default)]
    pub expected_audience: Option<String>,
    /// JWKS re-fetch interval, in milliseconds. Default 300000
    /// (5 minutes). Signed validator only.
    #[serde(default)]
    pub jwks_refresh_interval_ms: Option<u64>,

    /// PEM file containing the CA
    /// certificate(s) used to verify the `IdP`'s TLS certificate on ALL
    /// outbound HTTPS to the `IdP` — JWKS endpoint, introspection
    /// endpoint, and userinfo endpoint. When set, these are
    /// the *only* trust roots used for the outbound HTTPS (replaces the
    /// default webpki-roots — Strimzi-shaped). When unset, the broker
    /// uses reqwest's default rustls webpki-roots.
    #[serde(default)]
    pub idp_tls_trust: Option<std::path::PathBuf>,

    /// RFC 7662 introspection endpoint URL. When set,
    /// selects the introspection validator (mutually exclusive with
    /// `jwks_endpoint_uri`).
    #[serde(default)]
    pub introspection_endpoint_uri: Option<String>,

    /// Optional OIDC userinfo endpoint URL. When set, the
    /// introspection validator calls `GET userinfo` after a successful
    /// introspection and merges the profile claims over the
    /// introspection claims (introspection wins for `active`, `exp`,
    /// `iat`, `nbf`, `scope`, `client_id`, `sub`).
    #[serde(default)]
    pub userinfo_endpoint_uri: Option<String>,

    /// `client_id` the broker uses to authenticate (HTTP Basic
    /// Auth) against the introspection endpoint. Required when
    /// `introspection_endpoint_uri` is set.
    #[serde(default)]
    pub introspection_client_id: Option<String>,

    /// Filesystem path to a file containing the client
    /// secret the broker uses to authenticate against the introspection
    /// endpoint. Required when `introspection_endpoint_uri` is set.
    /// File-based (not literal) so secret material doesn't sit in the
    /// TOML; operator mounts a `Secret` and writes the mount path here.
    /// The file's trailing newline (if any) is stripped at config-load.
    #[serde(default)]
    pub introspection_client_secret_path: Option<std::path::PathBuf>,

    /// Timeout for the introspection (and userinfo) HTTP
    /// requests, in milliseconds. Default 10 000 (10 s).
    #[serde(default)]
    pub introspection_http_timeout_ms: Option<u64>,

    /// Optional ceiling on OAUTHBEARER session lifetime, in
    /// seconds. When set, the broker clamps `session_lifetime_ms` to
    /// `min(token_exp_ms - now_ms, cap * 1000)`. When unset, sessions
    /// last until the token's natural `exp`.
    #[serde(default)]
    pub max_session_lifetime_seconds: Option<u32>,

    /// Alternate claim name for principal-name fallback.
    #[serde(default)]
    pub fallback_user_name_claim: Option<String>,
    /// Prepended on fallback only.
    #[serde(default)]
    pub fallback_user_name_prefix: Option<String>,
    /// `JsonPath` expression (RFC 9535) extracting groups.
    /// Compiled once at broker startup; malformed expression panics
    /// with descriptive error.
    #[serde(default)]
    pub groups_claim: Option<String>,
    /// When `groups_claim` resolves to a string, split on
    /// this delimiter.
    #[serde(default)]
    pub groups_claim_delimiter: Option<String>,

    /// Minimum pause (seconds) between on-demand JWKS refreshes
    /// triggered by validator signals (unknown-kid / bad-signature tokens).
    /// Defaults to 1 (Strimzi parity). Signed validator only.
    #[serde(default)]
    pub jwks_min_refresh_pause_seconds: Option<u32>,

    /// Maximum age (seconds) of the cached JWKS before validators
    /// reject tokens until the next successful refresh. Strimzi default 360
    /// (6 minutes). Unset = no expiry check. Fails
    /// closed on prolonged `IdP` outage. Signed validator only.
    #[serde(default)]
    pub jwks_expiry_seconds: Option<u32>,

    /// When true, the JWKS parser keeps keys regardless of `use`
    /// field. Default false (filter out `use=enc`). Some identity providers
    /// publish signing keys with `use="enc"` by mistake; operators set this
    /// to true to accept them. Signed validator only.
    #[serde(default)]
    pub jwks_ignore_key_use: Option<bool>,
}

/// Kafka protocol default for `sasl.kerberos.service.name`.
const DEFAULT_KERBEROS_SERVICE_NAME: &str = "kafka";

/// Default timeout for outbound introspection / userinfo HTTP requests (10 s).
const DEFAULT_INTROSPECTION_HTTP_TIMEOUT: Time = secs(10);

/// Default clock-skew tolerance for `exp` / `iat` / `nbf` checks. Matches the
/// `crabka_security` validators' built-in default.
const DEFAULT_ALLOWABLE_CLOCK_SKEW: Time = secs(30);

/// TOML shape of `[gssapi]`. Maps to
/// [`crabka_security::gssapi::GssapiConfig`]. `principal_to_local_rules`
/// are parsed into `name::Rule` at `apply_to` time.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileGssapiConfig {
    pub keytab_path: std::path::PathBuf,
    /// `sasl.kerberos.service.name`. Defaults to `"kafka"` when omitted.
    #[serde(default)]
    pub service_name: Option<String>,
    /// `auth_to_local` rule specs, applied in order (first match wins).
    #[serde(default)]
    pub principal_to_local_rules: Vec<String>,
    /// Default Kerberos realm, used for principals that omit their realm.
    #[serde(default)]
    pub realm: Option<String>,
    /// KDC endpoint (e.g. `tcp://kdc:88`) that bypasses krb5.conf discovery;
    /// falls back to krb5.conf when omitted.
    #[serde(default)]
    pub kdc: Option<String>,
}

/// TOML shape of `[inter_broker_credentials]`. A `type` discriminator
/// selects the variant; only `gssapi` is implemented (PLAIN/SCRAM
/// inter-broker over TOML is intentionally not exposed).
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum FileInterBrokerCredentials {
    Gssapi {
        keytab_path: std::path::PathBuf,
        client_principal: String,
        #[serde(default)]
        service_name: Option<String>,
        kdc_url: String,
    },
}

/// `[audit]` section of `broker.toml` (`FedRAMP` 20x MLA).
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileAuditConfig {
    /// Whether the audit subsystem is active.
    #[serde(default = "default_audit_enabled")]
    pub enabled: bool,
    /// Internal topic name for audit records.
    #[serde(default = "default_audit_topic")]
    pub topic: String,
    /// Ed25519 checkpoint signing key. `None` → chaining only, no checkpoints.
    #[serde(default)]
    pub signing: Option<FileAuditSigningConfig>,
    /// Checkpoint emission cadence. `None` → use defaults.
    #[serde(default)]
    pub checkpoint: Option<FileAuditCheckpointConfig>,
    /// Durable spool for the AU-5 degraded path. `None` → use defaults.
    #[serde(default)]
    pub spool: Option<FileAuditSpoolConfig>,
}

impl Default for FileAuditConfig {
    fn default() -> Self {
        Self {
            enabled: default_audit_enabled(),
            topic: default_audit_topic(),
            signing: None,
            checkpoint: None,
            spool: None,
        }
    }
}

/// `[audit.spool]` — durable spool for the AU-5 degraded path.
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileAuditSpoolConfig {
    #[serde(default = "default_spool_dir")]
    pub dir: String,
    #[serde(default = "default_spool_max_bytes")]
    pub max_bytes: u64,
}

impl Default for FileAuditSpoolConfig {
    fn default() -> Self {
        Self {
            dir: default_spool_dir(),
            max_bytes: default_spool_max_bytes(),
        }
    }
}

fn default_spool_dir() -> String {
    crate::config::DEFAULT_AUDIT_SPOOL_DIR.to_string()
}

fn default_spool_max_bytes() -> u64 {
    crate::config::DEFAULT_AUDIT_SPOOL_MAX.bytes_u64()
}

/// `[audit.signing]` — Ed25519 checkpoint signing key.
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileAuditSigningConfig {
    pub key_path: String,
    pub key_id: String,
}

/// `[audit.checkpoint]` — checkpoint cadence.
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileAuditCheckpointConfig {
    #[serde(default = "default_checkpoint_every_n")]
    pub every_n: u64,
    #[serde(default = "default_checkpoint_every_secs")]
    pub every_secs: u64,
}

impl Default for FileAuditCheckpointConfig {
    fn default() -> Self {
        Self {
            every_n: default_checkpoint_every_n(),
            every_secs: default_checkpoint_every_secs(),
        }
    }
}

fn default_checkpoint_every_n() -> u64 {
    crate::config::DEFAULT_AUDIT_CHECKPOINT_EVERY_N
}

fn default_checkpoint_every_secs() -> u64 {
    crate::config::DEFAULT_AUDIT_CHECKPOINT_EVERY
        .secs_i64()
        .cast_unsigned()
}

fn default_audit_enabled() -> bool {
    true
}

fn default_audit_topic() -> String {
    crate::config::DEFAULT_AUDIT_TOPIC.to_string()
}

#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq)]
pub struct FileTlsConfig {
    pub cert_path: std::path::PathBuf,
    pub key_path: std::path::PathBuf,
    /// PEM file of CA(s) this broker trusts when validating a PEER's server
    /// cert as an outbound inter-broker / controller-quorum dialer. The
    /// operator renders the cluster CA here so KIP-595 controller peers can
    /// mutually authenticate over the controller listener. Maps to
    /// [`crabka_security::TlsConfig::trust_roots_path`].
    #[serde(default)]
    pub trust_roots_path: Option<std::path::PathBuf>,
    #[serde(default)]
    pub client_ca_path: Option<std::path::PathBuf>,
    #[serde(default)]
    pub client_auth: FileClientAuthMode,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, JsonSchema, PartialEq, Eq)]
pub enum FileClientAuthMode {
    #[default]
    Disabled,
    Optional,
    Required,
}

#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq)]
pub struct FileListenerSaslConfig {
    #[serde(default, deserialize_with = "deserialize_sasl_mechanisms")]
    #[schemars(with = "Vec<String>")]
    pub enabled_mechanisms: Vec<crabka_security::SaslMechanism>,
}

fn deserialize_sasl_mechanisms<'de, D>(
    deserializer: D,
) -> Result<Vec<crabka_security::SaslMechanism>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let names: Vec<String> = Vec::deserialize(deserializer)?;
    names
        .into_iter()
        .map(|s| {
            crabka_security::SaslMechanism::from_wire(&s)
                .ok_or_else(|| D::Error::custom(format!("unknown SASL mechanism: {s}")))
        })
        .collect()
}

#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq)]
pub struct FileListener {
    pub name: String,
    #[schemars(with = "String")]
    pub bind_addr: SocketAddr,
    pub advertised: String,
    #[schemars(with = "String")]
    pub protocol: ListenerProtocol,
    #[serde(default)]
    pub tls_config: Option<FileTlsConfig>,
    #[serde(default)]
    pub sasl_config: Option<FileListenerSaslConfig>,
}

fn configure_introspection_validator(
    oauth: &FileOAuthBearerConfig,
    endpoint: &str,
    custom_claim_check: Option<jsonpath_rust::parser::model::JpQuery>,
    groups_claim: Option<jsonpath_rust::parser::model::JpQuery>,
    cfg: &mut crate::config::BrokerConfig,
) {
    let client_id = oauth.introspection_client_id.clone().unwrap_or_else(|| {
        panic!("[oauthbearer]: introspection endpoint requires introspection_client_id")
    });
    let secret_path = oauth
        .introspection_client_secret_path
        .clone()
        .unwrap_or_else(|| {
            panic!(
                "[oauthbearer]: introspection endpoint requires introspection_client_secret_path"
            )
        });
    let client_secret = std::fs::read_to_string(&secret_path)
        .unwrap_or_else(|error| {
            panic!(
                "[oauthbearer]: failed to read client secret {}: {error}",
                secret_path.display()
            )
        })
        .trim_end_matches(['\n', '\r'])
        .to_owned();
    let timeout = oauth
        .introspection_http_timeout_ms
        .map_or(DEFAULT_INTROSPECTION_HTTP_TIMEOUT, |ms| {
            Time::from_millis(i64::try_from(ms).unwrap_or(i64::MAX))
        });
    let client = crate::oauth_introspection::ReqwestIntrospectionClient::build(
        endpoint.to_owned(),
        oauth.userinfo_endpoint_uri.clone(),
        client_id,
        client_secret,
        oauth.idp_tls_trust.as_deref(),
        timeout,
    )
    .unwrap_or_else(|error| panic!("failed to build OAuth introspection client: {error}"));
    cfg.oauthbearer_validator = crabka_security::OAuthBearerValidator::Introspection(
        crabka_security::IntrospectionValidator {
            client,
            principal_claim_name: oauth
                .principal_claim_name
                .clone()
                .unwrap_or_else(|| "sub".into()),
            custom_claim_check,
            call_userinfo: oauth.userinfo_endpoint_uri.is_some(),
            allowable_clock_skew: oauth
                .allowable_clock_skew_ms
                .map_or(DEFAULT_ALLOWABLE_CLOCK_SKEW, Time::from_millis),
            expected_audience: oauth.expected_audience.clone(),
            fallback_user_name_claim: oauth.fallback_user_name_claim.clone(),
            fallback_user_name_prefix: oauth.fallback_user_name_prefix.clone(),
            groups_claim,
            groups_claim_delimiter: oauth.groups_claim_delimiter.clone(),
        },
    );
}

fn apply_oauthbearer(oauth: Option<FileOAuthBearerConfig>, cfg: &mut crate::config::BrokerConfig) {
    let Some(oauth) = oauth else { return };
    // Thread the IdP trust-store path
    // unconditionally. Inert when no HTTPS-bound endpoint is set,
    // and harmlessly carried for the unsecured validator.
    cfg.oauthbearer_idp_tls_trust
        .clone_from(&oauth.idp_tls_trust);
    // Optional session-lifetime cap. Carried unconditionally;
    // the auth handler interprets None as "no cap".
    cfg.oauthbearer_max_session_lifetime = oauth
        .max_session_lifetime_seconds
        .map(|seconds| Time::from_secs(i64::from(seconds)));

    // Compile the JsonPath expression once at load time;
    // a malformed expression panics with a descriptive error.
    let custom_claim_check_compiled = oauth.custom_claim_check.as_deref().map(|expr| {
        jsonpath_rust::parser::parse_json_path(expr).unwrap_or_else(|e| {
            panic!("[oauthbearer]: invalid custom_claim_check JsonPath expression {expr:?}: {e}")
        })
    });

    // Compile groups_claim JsonPath at load time.
    let groups_claim_compiled = oauth.groups_claim.as_deref().map(|expr| {
        jsonpath_rust::parser::parse_json_path(expr).unwrap_or_else(|e| {
            panic!("[oauthbearer]: invalid groups_claim JsonPath expression {expr:?}: {e}")
        })
    });

    match (
        oauth.jwks_endpoint_uri.as_ref(),
        oauth.introspection_endpoint_uri.as_ref(),
    ) {
        (Some(_), Some(_)) => {
            panic!(
                "[oauthbearer]: jwks_endpoint_uri and introspection_endpoint_uri are mutually exclusive; configure exactly one"
            );
        }
        (Some(_), None) => {
            // Signed-JWT validation. The empty key handle is
            // populated by the refresher `Broker::start` spawns.
            let jwks_uri = oauth.jwks_endpoint_uri.clone().unwrap();

            // Create the signal channel + the shared
            // timestamps here so the validator's `JwksHandle` and
            // the refresher (constructed in `Broker::start`) point at
            // the same Arc-shared state. Channel capacity 1 +
            // `try_send` on the producer ⇒ signals coalesce.
            let (signal_tx, signal_rx) = tokio::sync::mpsc::channel::<()>(1);
            let last_successful = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
            let last_on_demand = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));

            let handle = crabka_security::JwksHandle::new_with_refresher_handles(
                crabka_security::Jwks::empty(),
                last_successful.clone(),
                signal_tx,
            );

            let mut v = crabka_security::SignedJwsValidator::new(handle);
            if let Some(name) = oauth.principal_claim_name {
                v.principal_claim_name = name;
            }
            if let Some(skew) = oauth.allowable_clock_skew_ms {
                v.allowable_clock_skew = Time::from_millis(skew);
            }
            v.valid_issuer = oauth.valid_issuer_uri;
            v.expected_audience = oauth.expected_audience;
            // JsonPath custom_claim_check + JWT typ check.
            v.custom_claim_check
                .clone_from(&custom_claim_check_compiled);
            v.valid_token_type.clone_from(&oauth.valid_token_type);
            // Claims mapping.
            v.fallback_user_name_claim
                .clone_from(&oauth.fallback_user_name_claim);
            v.fallback_user_name_prefix
                .clone_from(&oauth.fallback_user_name_prefix);
            v.groups_claim.clone_from(&groups_claim_compiled);
            v.groups_claim_delimiter
                .clone_from(&oauth.groups_claim_delimiter);
            // Hard cache-expiry threshold.
            v.cache_expiry = oauth
                .jwks_expiry_seconds
                .map(|s| Time::from_secs(i64::from(s)));
            cfg.oauthbearer_validator = crabka_security::OAuthBearerValidator::Signed(v);
            cfg.oauthbearer_jwks_endpoint = Some(jwks_uri);
            if let Some(ms) = oauth.jwks_refresh_interval_ms {
                cfg.oauthbearer_jwks_refresh_interval =
                    Time::from_millis(i64::try_from(ms).unwrap_or(i64::MAX));
            }

            // Park signal_rx + shared state for Broker::start.
            *cfg.oauthbearer_jwks_signal_rx.lock().unwrap() = Some(signal_rx);
            cfg.oauthbearer_jwks_last_successful_fetch_ms = last_successful;
            cfg.oauthbearer_jwks_last_on_demand_refresh_ms = last_on_demand;
            cfg.oauthbearer_jwks_min_on_demand_pause = oauth
                .jwks_min_refresh_pause_seconds
                .map_or(crate::config::DEFAULT_JWKS_MIN_ON_DEMAND_PAUSE, |s| {
                    Time::from_secs(i64::from(s))
                });
            cfg.features.oauthbearer_jwks_ignore_key_use =
                oauth.jwks_ignore_key_use.unwrap_or(false);
        }
        (None, Some(introspect_uri)) => {
            configure_introspection_validator(
                &oauth,
                introspect_uri,
                custom_claim_check_compiled.clone(),
                groups_claim_compiled.clone(),
                cfg,
            );
        }
        (None, None) => {
            // Unsecured-JWS validation (development only).
            let mut v = crabka_security::UnsecuredJwsValidator::default();
            if let Some(name) = oauth.principal_claim_name {
                v.principal_claim_name = name;
            }
            if let Some(skew) = oauth.allowable_clock_skew_ms {
                v.allowable_clock_skew = Time::from_millis(skew);
            }
            // JsonPath custom_claim_check + JWT typ check.
            v.custom_claim_check = custom_claim_check_compiled;
            v.valid_token_type.clone_from(&oauth.valid_token_type);
            // Claims mapping.
            v.fallback_user_name_claim = oauth.fallback_user_name_claim;
            v.fallback_user_name_prefix = oauth.fallback_user_name_prefix;
            v.groups_claim = groups_claim_compiled;
            v.groups_claim_delimiter = oauth.groups_claim_delimiter;
            cfg.oauthbearer_validator = crabka_security::OAuthBearerValidator::Unsecured(v);
        }
    }
}

fn apply_remote_storage(
    remote: Option<&FileRemoteStorageConfig>,
    cfg: &mut crate::config::BrokerConfig,
) -> Result<(), FileConfigError> {
    let Some(rs) = remote else { return Ok(()) };
    let set_count = usize::from(rs.storage_dir.is_some())
        + usize::from(rs.s3.is_some())
        + usize::from(rs.gcs.is_some());
    if set_count > 1 {
        return Err(FileConfigError::InvalidConfig(
            "[remote_storage] cannot set both/more than one of `storage_dir` \
                     (local), `[remote_storage.s3]` (object store), and \
                     `[remote_storage.gcs]` (Google Cloud Storage)"
                .into(),
        ));
    }
    if let Some(dir) = &rs.storage_dir {
        cfg.remote_storage_backend = Some(crate::config::RemoteStorageBackend::Local {
            dir: std::path::PathBuf::from(dir),
        });
    } else if let Some(s3) = &rs.s3 {
        cfg.remote_storage_backend = Some(crate::config::RemoteStorageBackend::S3(
            crabka_remote_storage::S3Config {
                bucket: s3.bucket.clone(),
                region: s3.region.clone(),
                prefix: s3.prefix.clone(),
                endpoint: s3.endpoint.clone(),
                access_key_id: s3.access_key_id.clone(),
                secret_access_key: s3.secret_access_key.clone(),
                allow_http: s3.allow_http,
                multipart_threshold: s3
                    .multipart_threshold
                    .unwrap_or(crabka_remote_storage::DEFAULT_MULTIPART_THRESHOLD),
                multipart_chunk_size: s3
                    .multipart_chunk_size
                    .unwrap_or(crabka_remote_storage::DEFAULT_MULTIPART_CHUNK_SIZE),
            },
        ));
    } else if let Some(gcs) = &rs.gcs {
        cfg.remote_storage_backend = Some(crate::config::RemoteStorageBackend::Gcs(
            crabka_remote_storage::GcsConfig {
                bucket: gcs.bucket.clone(),
                prefix: gcs.prefix.clone(),
                service_account_path: gcs.service_account_path.clone(),
                service_account_key: gcs.service_account_key.clone(),
                application_credentials_path: gcs.application_credentials_path.clone(),
                endpoint: gcs.endpoint.clone(),
                allow_http: gcs.allow_http,
                multipart_threshold: gcs
                    .multipart_threshold
                    .unwrap_or(crabka_remote_storage::DEFAULT_MULTIPART_THRESHOLD),
                multipart_chunk_size: gcs
                    .multipart_chunk_size
                    .unwrap_or(crabka_remote_storage::DEFAULT_MULTIPART_CHUNK_SIZE),
            },
        ));
    }

    // KIP-405: topic-backed RLMM is the default whenever tiered storage
    // is enabled. `[remote_storage.kafka_metadata]` only overrides the
    // topic knobs; `in_memory = true` is the explicit opt-out.
    if cfg.remote_storage_backend.is_some() {
        let km = rs.kafka_metadata.as_ref();
        if km.is_some_and(|k| k.in_memory) {
            cfg.remote_log_metadata = crate::config::RlmmKind::InMemory;
        } else {
            cfg.remote_log_metadata =
                crate::config::RlmmKind::TopicBacked(crate::config::KafkaRlmmConfig {
                    bootstrap: km.map(|k| k.bootstrap.clone()).unwrap_or_default(),
                    num_partitions: km
                        .and_then(|k| k.num_partitions)
                        .unwrap_or(crate::config::DEFAULT_RLMM_TOPIC_NUM_PARTITIONS),
                    replication: km
                        .and_then(|k| k.replication)
                        .unwrap_or(crate::config::DEFAULT_RLMM_TOPIC_REPLICATION_FACTOR),
                    snapshot_interval: crate::config::DEFAULT_RLMM_SNAPSHOT_INTERVAL,
                    snapshot_dir: cfg.log_dir.join("remote-log-metadata"),
                    security: None,
                });
        }
    }
    Ok(())
}

struct FileConfigTail {
    authorization: Option<FileAuthorizationConfig>,
    process: Option<FileProcessConfig>,
    gssapi: Option<FileGssapiConfig>,
    inter_broker_credentials: Option<FileInterBrokerCredentials>,
    controller_quorum_voters: Vec<String>,
    controller_server_name: Option<String>,
    audit: Option<FileAuditConfig>,
}

struct ListenerSettings {
    listeners: Vec<FileListener>,
    inter_broker_listener_name: Option<String>,
    max_connections: Option<usize>,
    max_connections_per_ip: Option<usize>,
    server_properties: std::collections::BTreeMap<String, String>,
    controller_listener_protocol: Option<ListenerProtocol>,
    tls_config: Option<FileTlsConfig>,
}

fn apply_listener_settings(
    settings: ListenerSettings,
    cfg: &mut crate::config::BrokerConfig,
    defaults: &crate::config::BrokerConfig,
) {
    let had_file_listeners = !settings.listeners.is_empty();
    if had_file_listeners {
        cfg.listeners = settings
            .listeners
            .into_iter()
            .map(FileListener::into_spec)
            .collect();
    }
    if let Some(name) = settings.inter_broker_listener_name {
        cfg.inter_broker_listener_name = name;
    }
    if had_file_listeners
        && let Some(advertised) = cfg
            .listeners
            .iter()
            .find(|listener| listener.name == cfg.inter_broker_listener_name)
            .or_else(|| cfg.listeners.first())
            .map(|listener| listener.advertised.clone())
    {
        cfg.advertised_listener = advertised;
    }
    if let Some(maximum) = settings.max_connections
        && cfg.max_connections == defaults.max_connections
    {
        cfg.max_connections = maximum;
    }
    if let Some(maximum) = settings.max_connections_per_ip
        && cfg.max_connections_per_ip == defaults.max_connections_per_ip
    {
        cfg.max_connections_per_ip = maximum;
    }
    if cfg.features.transaction_two_phase_commit_enable
        == defaults.features.transaction_two_phase_commit_enable
        && let Some(value) = settings
            .server_properties
            .get("transaction.two.phase.commit.enable")
    {
        cfg.features.transaction_two_phase_commit_enable =
            value.trim().eq_ignore_ascii_case("true");
    }
    if let Some(protocol) = settings.controller_listener_protocol
        && cfg.controller_listener_protocol == defaults.controller_listener_protocol
    {
        cfg.controller_listener_protocol = protocol;
    }
    if let Some(tls) = settings.tls_config
        && cfg.tls_config.is_none()
    {
        use crabka_security::{ClientAuthMode, TlsConfig};
        cfg.tls_config = Some(TlsConfig {
            cert_chain_path: tls.cert_path,
            private_key_path: tls.key_path,
            trust_roots_path: tls.trust_roots_path,
            client_ca_path: tls.client_ca_path,
            client_auth: match tls.client_auth {
                FileClientAuthMode::Disabled => ClientAuthMode::Disabled,
                FileClientAuthMode::Optional => ClientAuthMode::Optional,
                FileClientAuthMode::Required => ClientAuthMode::Required,
            },
        });
    }
}

fn apply_delegation_tokens(
    delegation: Option<&FileDelegationTokenConfig>,
    cfg: &mut crate::config::BrokerConfig,
) -> Result<(), FileConfigError> {
    let Some(delegation) = delegation else {
        return Ok(());
    };
    if cfg.delegation_token_secret_key.is_none()
        && let Some(key) = delegation.secret_key.clone()
    {
        cfg.delegation_token_secret_key = Some(crabka_security::SecretBytes::new(key.into_bytes()));
    }
    if let Some(milliseconds) = delegation.max_lifetime_ms {
        cfg.delegation_token_max_lifetime = Time::from_millis(positive_i64(
            "delegation_token.max_lifetime_ms",
            milliseconds,
        )?);
    }
    if let Some(milliseconds) = delegation.expiry_check_interval_ms {
        cfg.delegation_token_expiry_check_interval = Time::from_millis(positive_i64(
            "delegation_token.expiry_check_interval_ms",
            milliseconds,
        )?);
    }
    if let Some(milliseconds) = delegation.default_renew_period_ms {
        cfg.delegation_token_default_renew_period = Time::from_millis(positive_i64(
            "delegation_token.default_renew_period_ms",
            milliseconds,
        )?);
    }
    Ok(())
}

fn apply_config_tail(
    tail: FileConfigTail,
    cfg: &mut crate::config::BrokerConfig,
) -> Result<(), FileConfigError> {
    if let Some(authorization) = tail.authorization.as_ref() {
        let super_users = authorization.super_users.iter().cloned().collect();
        cfg.super_users.clone_from(&super_users);
        cfg.authorizer = match authorization.authz_type {
            AuthzType::AllowAll => Arc::new(crate::authorizer::AllowAllAuthorizer),
            AuthzType::Simple => Arc::new(crate::authorizer::SimpleAclAuthorizer::new(super_users)),
            AuthzType::Opa => {
                let opa = authorization
                    .opa
                    .as_ref()
                    .ok_or_else(|| FileConfigError::MissingSection("[authorization.opa]".into()))?;
                Arc::new(
                    crate::authorizer::opa::OpaAuthorizer::new(
                        super_users,
                        opa.url.clone(),
                        opa.allow_on_error,
                        opa.maximum_cache_size,
                        Time::from_millis(opa.expire_after_ms),
                        cfg.opa_http_timeout,
                    )
                    .map_err(|error| FileConfigError::OpaConfig(format!("{error:?}")))?,
                )
            }
        };
    }
    if let Some(process) = tail.process
        && !process.roles.is_empty()
    {
        cfg.roles = process
            .roles
            .iter()
            .map(|role| match role.to_ascii_lowercase().as_str() {
                "controller" => Ok(crate::config::NodeRole::Controller),
                "broker" => Ok(crate::config::NodeRole::Broker),
                other => Err(FileConfigError::InvalidConfig(format!(
                    "unknown process.role `{other}`"
                ))),
            })
            .collect::<Result<_, _>>()?;
    }
    if let Some(gssapi) = tail.gssapi {
        let rules = gssapi
            .principal_to_local_rules
            .iter()
            .map(|spec| {
                crabka_security::gssapi::name::Rule::parse(spec).map_err(|error| {
                    FileConfigError::InvalidConfig(format!(
                        "invalid GSSAPI principal rule {spec:?}: {error}"
                    ))
                })
            })
            .collect::<Result<_, _>>()?;
        cfg.gssapi = Some(crabka_security::gssapi::GssapiConfig {
            keytab_path: gssapi.keytab_path,
            service_name: gssapi
                .service_name
                .unwrap_or_else(|| DEFAULT_KERBEROS_SERVICE_NAME.to_owned()),
            principal_to_local_rules: rules,
            realm: gssapi.realm,
            kdc: gssapi.kdc,
        });
    }
    if let Some(FileInterBrokerCredentials::Gssapi {
        keytab_path,
        client_principal,
        service_name,
        kdc_url,
    }) = tail.inter_broker_credentials
    {
        cfg.inter_broker_credentials = Some(crate::config::InterBrokerCredentials::Gssapi {
            keytab_path,
            client_principal,
            service_name: service_name.unwrap_or_else(|| DEFAULT_KERBEROS_SERVICE_NAME.to_owned()),
            kdc_url,
        });
    }
    if !tail.controller_quorum_voters.is_empty() {
        cfg.controller_quorum_voters = tail
            .controller_quorum_voters
            .iter()
            .map(|entry| FileConfig::parse_quorum_voter(entry))
            .collect::<Result<_, _>>()?;
    }
    if tail.controller_server_name.is_some() {
        cfg.controller_server_name = tail.controller_server_name;
    }
    let audit = tail.audit.unwrap_or_default();
    cfg.audit_enabled = audit.enabled;
    cfg.audit_topic = audit.topic;
    if let Some(signing) = audit.signing {
        cfg.audit_signing_key_path = Some(signing.key_path.into());
        cfg.audit_signing_key_id = Some(signing.key_id);
    }
    let checkpoint = audit.checkpoint.unwrap_or_default();
    cfg.audit_checkpoint_every_n = checkpoint.every_n;
    cfg.audit_checkpoint_every =
        Time::from_secs(i64::try_from(checkpoint.every_secs).unwrap_or(i64::MAX));
    let spool = audit.spool.unwrap_or_default();
    cfg.audit_spool_dir = spool.dir.into();
    cfg.audit_spool_max = ByteSize::from_bytes(spool.max_bytes);
    Ok(())
}

fn invalid_runtime_value(name: &str, error: impl std::fmt::Display) -> FileConfigError {
    FileConfigError::InvalidConfig(format!("{name}: {error}"))
}

fn positive_u64(name: &str, value: u64) -> Result<u64, FileConfigError> {
    crate::config_value::PositiveMillis::new(value)
        .map(crate::config_value::PositiveMillis::into_value)
        .map_err(|error| invalid_runtime_value(name, error))
}

fn positive_i32(name: &str, value: i32) -> Result<i32, FileConfigError> {
    crate::config_value::PositiveI32::new(value)
        .map(crate::config_value::PositiveI32::into_value)
        .map_err(|error| invalid_runtime_value(name, error))
}

fn positive_i64(name: &str, value: i64) -> Result<i64, FileConfigError> {
    crate::config_value::PositiveI64::new(value)
        .map(crate::config_value::PositiveI64::into_value)
        .map_err(|error| invalid_runtime_value(name, error))
}

fn positive_usize(name: &str, value: usize) -> Result<usize, FileConfigError> {
    crate::config_value::PositiveCount::new(value)
        .map(crate::config_value::PositiveCount::into_value)
        .map_err(|error| invalid_runtime_value(name, error))
}

fn positive_u32(name: &str, value: u32) -> Result<u32, FileConfigError> {
    let count = usize::try_from(value).map_err(|error| invalid_runtime_value(name, error))?;
    positive_usize(name, count)?;
    Ok(value)
}

/// A validated-positive millisecond count from the operator's TOML, as a
/// [`Time`]. The single place a `*_ms` key becomes a dimensioned extent
/// outside the `set_runtime_*` macros.
fn millis_time(name: &str, value: u64) -> Result<Time, FileConfigError> {
    let value = positive_u64(name, value)?;
    Ok(Time::from_millis(i64::try_from(value).unwrap_or(i64::MAX)))
}

fn positive_i16(name: &str, value: i16) -> Result<i16, FileConfigError> {
    positive_i32(name, i32::from(value))?;
    Ok(value)
}

fn percentage(name: &str, value: u32) -> Result<u32, FileConfigError> {
    crate::config_value::Percentage::new(value)
        .map(crate::config_value::Percentage::into_value)
        .map_err(|error| invalid_runtime_value(name, error))
}

/// Assigns a `_ms` key from the operator's TOML into a [`Time`] field.
///
/// The raw integer stops here: this is the one place a broker runtime timeout
/// crosses from "a number an operator wrote" into a dimensioned quantity.
macro_rules! set_runtime_time_millis {
    ($runtime:ident, $field:ident, $target:expr) => {
        set_runtime_time_millis!($runtime, $field, $target, positive_u64);
    };
    ($runtime:ident, $field:ident, $target:expr, $validator:ident) => {
        if let Some(value) = $runtime.$field {
            let value = $validator(stringify!($field), value)?;
            $target = Time::from_millis(i64::try_from(value).unwrap_or(i64::MAX));
        }
    };
}

/// Assigns a `_ms` key into a [`std::time::Duration`] field.
///
/// For the group-coordinator configs, which are still `Duration`-typed: two of
/// the four (`StreamsGroupConfig`, `ShareCoordinatorConfig`) derive `Eq` and so
/// cannot hold an `f64`-backed quantity, and keeping all four in one
/// representation is what lets `BrokerConfig::validate` compare them uniformly.
macro_rules! set_runtime_duration {
    ($runtime:ident, $field:ident, $target:expr) => {
        if let Some(value) = $runtime.$field {
            let value = positive_u64(stringify!($field), value)?;
            $target = std::time::Duration::from_millis(value);
        }
    };
}

/// Assigns a `_secs` key from the operator's TOML into a [`Time`] field.
macro_rules! set_runtime_time_secs {
    ($runtime:ident, $field:ident, $target:expr) => {
        if let Some(value) = $runtime.$field {
            let value = positive_u64(stringify!($field), value)?;
            $target = Time::from_secs(i64::try_from(value).unwrap_or(i64::MAX));
        }
    };
}

/// Assigns a `_bytes` key from the operator's TOML into a [`ByteSize`] field.
macro_rules! set_runtime_size_bytes {
    ($runtime:ident, $field:ident, $target:expr, $validator:ident) => {
        if let Some(value) = $runtime.$field {
            let value = $validator(stringify!($field), value)?;
            $target = ByteSize::from_bytes(u64::try_from(value).unwrap_or(u64::MAX));
        }
    };
}

macro_rules! set_runtime_validated {
    ($runtime:ident, $field:ident, $target:expr, $validator:ident) => {
        if let Some(value) = $runtime.$field {
            $target = $validator(stringify!($field), value)?;
        }
    };
}

macro_rules! set_runtime_i32 {
    ($runtime:ident, $field:ident, $target:expr) => {
        set_runtime_validated!($runtime, $field, $target, positive_i32);
    };
}

macro_rules! set_runtime_i64 {
    ($runtime:ident, $field:ident, $target:expr) => {
        set_runtime_validated!($runtime, $field, $target, positive_i64);
    };
}

macro_rules! set_runtime_usize {
    ($runtime:ident, $field:ident, $target:expr) => {
        set_runtime_validated!($runtime, $field, $target, positive_usize);
    };
}

macro_rules! set_runtime_u32 {
    ($runtime:ident, $field:ident, $target:expr) => {
        set_runtime_validated!($runtime, $field, $target, positive_u32);
    };
}

macro_rules! set_runtime_positive_u64 {
    ($runtime:ident, $field:ident, $target:expr) => {
        set_runtime_validated!($runtime, $field, $target, positive_u64);
    };
}

macro_rules! set_runtime_plain {
    ($runtime:ident, $field:ident, $target:expr) => {
        if let Some(value) = $runtime.$field {
            $target = value;
        }
    };
}

impl RuntimeFileConfig {
    /// Apply every present runtime value, validating scalar boundaries.
    ///
    /// # Errors
    ///
    /// Returns [`FileConfigError::InvalidConfig`] for an invalid value.
    pub fn apply_to(
        mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        self.apply_core(cfg)?;
        self.apply_replication(cfg)?;
        self.apply_coordinators(cfg)?;
        self.apply_recovery_and_queues(cfg)?;
        self.apply_network_limits(cfg)?;
        self.apply_transactions(cfg)?;
        self.apply_broker_policy(cfg)?;
        self.apply_share_group(cfg)?;
        self.apply_streams_group(cfg)
    }

    fn apply_core(&mut self, cfg: &mut crate::config::BrokerConfig) -> Result<(), FileConfigError> {
        let runtime = self;

        set_runtime_time_millis!(
            runtime,
            startup_leader_wait_timeout_ms,
            cfg.startup_leader_wait_timeout
        );
        set_runtime_time_millis!(
            runtime,
            self_registration_backoff_min_ms,
            cfg.self_registration_backoff_min
        );
        set_runtime_time_millis!(
            runtime,
            self_registration_backoff_max_ms,
            cfg.self_registration_backoff_max
        );
        set_runtime_time_millis!(
            runtime,
            observer_poll_interval_ms,
            cfg.observer_poll_interval
        );
        set_runtime_time_millis!(
            runtime,
            audit_spool_replay_interval_ms,
            cfg.audit_spool_replay_interval
        );
        set_runtime_time_millis!(
            runtime,
            audit_stats_poll_interval_ms,
            cfg.audit_stats_poll_interval
        );
        set_runtime_time_millis!(
            runtime,
            audit_partition_wait_timeout_ms,
            cfg.audit_partition_wait_timeout
        );
        set_runtime_time_millis!(
            runtime,
            liveness_tick_interval_ms,
            cfg.liveness_tick_interval
        );
        set_runtime_time_millis!(runtime, gauge_poll_interval_ms, cfg.gauge_poll_interval);
        set_runtime_time_millis!(runtime, isr_scan_interval_ms, cfg.isr_scan_interval);
        set_runtime_time_millis!(runtime, cleaner_interval_ms, cfg.cleaner_interval);
        set_runtime_time_millis!(
            runtime,
            future_log_move_retry_backoff_ms,
            cfg.future_log_move_retry_backoff
        );
        set_runtime_time_millis!(
            runtime,
            client_metrics_eviction_tick_ms,
            cfg.client_metrics_eviction_tick
        );
        set_runtime_time_millis!(
            runtime,
            client_metrics_stale_floor_ms,
            cfg.client_metrics_stale_floor
        );
        set_runtime_time_millis!(
            runtime,
            client_metrics_default_interval_ms,
            cfg.client_metrics_default_interval,
            positive_i32
        );
        set_runtime_size_bytes!(
            runtime,
            client_metrics_telemetry_max_bytes,
            cfg.client_metrics_telemetry_max,
            positive_i32
        );
        set_runtime_time_millis!(
            runtime,
            client_metrics_prom_snapshot_ttl_ms,
            cfg.client_metrics_prom_snapshot_ttl
        );
        set_runtime_time_millis!(runtime, rlmm_reconcile_tick_ms, cfg.rlmm_reconcile_tick);
        set_runtime_time_millis!(
            runtime,
            rlmm_bootstrap_backoff_initial_ms,
            cfg.rlmm_bootstrap_backoff_initial
        );
        set_runtime_time_millis!(
            runtime,
            rlmm_bootstrap_backoff_max_ms,
            cfg.rlmm_bootstrap_backoff_max
        );
        set_runtime_time_millis!(
            runtime,
            connection_creation_throttle_max_ms,
            cfg.connection_creation_throttle_max
        );
        set_runtime_time_millis!(runtime, opa_http_timeout_ms, cfg.opa_http_timeout);
        set_runtime_time_millis!(
            runtime,
            oauth_jwks_http_timeout_ms,
            cfg.oauth_jwks_http_timeout
        );
        set_runtime_time_millis!(
            runtime,
            auto_join_retry_backoff_ms,
            cfg.auto_join_retry_backoff
        );
        if let Some(value) = runtime.auto_join_voter_request_timeout_ms {
            let value = crate::config_value::VoterRequestTimeoutMillis::new(value)
                .map_err(|error| {
                    invalid_runtime_value("auto_join_voter_request_timeout_ms", error)
                })?
                .into_value();
            cfg.auto_join_voter_request_timeout =
                Time::from_millis(i64::try_from(value).unwrap_or(i64::MAX));
        }
        Ok(())
    }

    fn apply_replication(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        set_runtime_i32!(
            runtime,
            replication_fetch_max_bytes,
            cfg.replication.fetch_max_bytes
        );
        set_runtime_i32!(
            runtime,
            replication_fetch_max_wait_ms,
            cfg.replication.fetch_max_wait_ms
        );
        set_runtime_i32!(
            runtime,
            replication_fetch_min_bytes,
            cfg.replication.fetch_min_bytes
        );
        set_runtime_duration!(
            runtime,
            replication_throttle_exhausted_backoff_ms,
            cfg.replication.throttle_exhausted_backoff
        );
        set_runtime_duration!(
            runtime,
            replication_send_error_backoff_ms,
            cfg.replication.send_error_backoff
        );
        set_runtime_duration!(
            runtime,
            replication_unknown_topic_retry_delay_ms,
            cfg.replication.unknown_topic_retry_delay
        );
        set_runtime_duration!(
            runtime,
            replication_epoch_fence_backoff_ms,
            cfg.replication.epoch_fence_backoff
        );
        set_runtime_duration!(
            runtime,
            replication_unexpected_error_backoff_ms,
            cfg.replication.unexpected_error_backoff
        );
        set_runtime_duration!(
            runtime,
            replication_reconnect_initial_delay_ms,
            cfg.replication.reconnect_initial_delay
        );
        set_runtime_duration!(
            runtime,
            replication_reconnect_delay_cap_ms,
            cfg.replication.reconnect_delay_cap
        );
        Ok(())
    }

    fn apply_coordinators(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        set_runtime_time_millis!(
            runtime,
            coordinator_session_expiry_tick_ms,
            cfg.coordinator_session_expiry_tick
        );
        set_runtime_time_millis!(
            runtime,
            coordinator_shutdown_ack_timeout_ms,
            cfg.coordinator_shutdown_ack_timeout
        );
        set_runtime_duration!(
            runtime,
            consumer_group_session_timeout_ms,
            cfg.next_gen_consumer_group.session_timeout
        );
        set_runtime_duration!(
            runtime,
            consumer_group_heartbeat_interval_ms,
            cfg.next_gen_consumer_group.heartbeat_interval
        );
        set_runtime_duration!(
            runtime,
            consumer_group_min_session_timeout_ms,
            cfg.next_gen_consumer_group.min_session_timeout
        );
        set_runtime_duration!(
            runtime,
            consumer_group_max_session_timeout_ms,
            cfg.next_gen_consumer_group.max_session_timeout
        );
        set_runtime_duration!(
            runtime,
            consumer_group_min_heartbeat_interval_ms,
            cfg.next_gen_consumer_group.min_heartbeat_interval
        );
        set_runtime_duration!(
            runtime,
            consumer_group_max_heartbeat_interval_ms,
            cfg.next_gen_consumer_group.max_heartbeat_interval
        );
        set_runtime_usize!(
            runtime,
            consumer_group_max_size,
            cfg.next_gen_consumer_group.max_size
        );
        set_runtime_time_millis!(
            runtime,
            classic_group_initial_rebalance_delay_ms,
            cfg.classic_group_initial_rebalance_delay
        );
        set_runtime_time_millis!(
            runtime,
            sync_group_follower_wait_ms,
            cfg.sync_group_follower_wait
        );
        Ok(())
    }

    fn apply_recovery_and_queues(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        set_runtime_time_millis!(
            runtime,
            unclean_recovery_aggressive_deadline_ms,
            cfg.unclean_recovery_aggressive_deadline
        );
        set_runtime_time_millis!(
            runtime,
            unclean_recovery_balanced_deadline_ms,
            cfg.unclean_recovery_balanced_deadline
        );
        set_runtime_time_millis!(
            runtime,
            operator_recovery_deadline_ms,
            cfg.operator_recovery_deadline
        );
        set_runtime_time_millis!(runtime, quota_throttle_max_ms, cfg.quota_throttle_max);
        set_runtime_u32!(
            runtime,
            self_registration_max_attempts,
            cfg.self_registration_max_attempts
        );
        set_runtime_size_bytes!(
            runtime,
            observer_fetch_max_bytes,
            cfg.observer_fetch_max,
            positive_u32
        );
        set_runtime_usize!(
            runtime,
            audit_event_queue_capacity,
            cfg.audit_event_queue_capacity
        );
        set_runtime_i64!(
            runtime,
            audit_tail_window_offsets,
            cfg.audit_tail_window_offsets
        );
        set_runtime_size_bytes!(
            runtime,
            audit_tail_read_max_bytes,
            cfg.audit_tail_read_max,
            positive_usize
        );
        set_runtime_time_millis!(
            runtime,
            offsets_topic_metadata_wait_timeout_ms,
            cfg.offsets_topic_metadata_wait_timeout
        );
        set_runtime_u32!(
            runtime,
            client_metrics_stale_push_intervals,
            cfg.client_metrics_stale_push_intervals
        );
        set_runtime_usize!(
            runtime,
            client_metrics_otlp_queue_capacity,
            cfg.client_metrics_otlp_queue_capacity
        );
        set_runtime_usize!(
            runtime,
            coordinator_actor_mailbox_capacity,
            cfg.coordinator_actor_mailbox_capacity
        );
        set_runtime_usize!(
            runtime,
            unclean_recovery_queue_capacity,
            cfg.unclean_recovery_queue_capacity
        );
        set_runtime_size_bytes!(
            runtime,
            share_recovery_read_max_bytes,
            cfg.share_recovery_read_max,
            positive_usize
        );
        set_runtime_usize!(
            runtime,
            share_session_cache_max_when_unlimited,
            cfg.share_session_cache_max_when_unlimited
        );
        Ok(())
    }

    fn apply_network_limits(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        set_runtime_size_bytes!(
            runtime,
            socket_request_max_bytes,
            cfg.socket_request_max,
            positive_usize
        );
        set_runtime_size_bytes!(
            runtime,
            sendfile_min_bytes,
            cfg.sendfile_min,
            positive_usize
        );
        set_runtime_size_bytes!(
            runtime,
            socket_send_buffer_bytes,
            cfg.socket_send_buffer,
            positive_usize
        );
        set_runtime_size_bytes!(
            runtime,
            socket_receive_buffer_bytes,
            cfg.socket_receive_buffer,
            positive_usize
        );
        set_runtime_size_bytes!(
            runtime,
            acl_max_principal_bytes,
            cfg.acl_max_principal,
            positive_usize
        );
        set_runtime_size_bytes!(
            runtime,
            acl_max_resource_name_bytes,
            cfg.acl_max_resource_name,
            positive_usize
        );
        if let Some(value) = runtime.telemetry_max_decompression_ratio {
            let value = positive_usize("telemetry_max_decompression_ratio", value)?;
            // A multiplier, not a fraction: `100` means "100× the compressed
            // size", so it lands as `fraction(100.0)` rather than `percent(100)`.
            cfg.telemetry_max_decompression_ratio =
                crabka_units::fraction(f64::from(u32::try_from(value).unwrap_or(u32::MAX)));
        }
        set_runtime_size_bytes!(
            runtime,
            telemetry_decompressed_output_floor_bytes,
            cfg.telemetry_decompressed_output_floor,
            positive_usize
        );
        set_runtime_size_bytes!(
            runtime,
            telemetry_decompressed_output_ceiling_bytes,
            cfg.telemetry_decompressed_output_ceiling,
            positive_usize
        );
        Ok(())
    }

    fn apply_transactions(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        set_runtime_time_millis!(
            runtime,
            producer_id_expiration_ms,
            cfg.producer_id_expiration,
            positive_i64
        );
        set_runtime_time_millis!(
            runtime,
            producer_id_expiration_scan_interval_ms,
            cfg.producer_id_expiration_scan_interval
        );
        set_runtime_usize!(runtime, max_produce_group, cfg.max_produce_group);
        set_runtime_usize!(
            runtime,
            partition_writer_queue_depth,
            cfg.partition_writer_queue_depth
        );
        set_runtime_i32!(
            runtime,
            default_min_insync_replicas,
            cfg.default_min_insync_replicas
        );
        set_runtime_size_bytes!(
            runtime,
            future_log_move_read_chunk_bytes,
            cfg.future_log_move_read_chunk,
            positive_usize
        );
        set_runtime_i32!(
            runtime,
            share_state_num_partitions,
            cfg.share_coordinator.state_topic_num_partitions
        );
        if let Some(value) = runtime.share_state_replication_factor {
            cfg.share_coordinator.state_topic_replication_factor =
                positive_i16("share_state_replication_factor", value)?;
        }
        set_runtime_i32!(
            runtime,
            transaction_state_num_partitions,
            cfg.transaction_state_num_partitions
        );
        if let Some(value) = runtime.transaction_state_replication_factor {
            cfg.transaction_state_replication_factor =
                positive_i16("transaction_state_replication_factor", value)?;
        }
        set_runtime_time_millis!(
            runtime,
            transaction_min_timeout_ms,
            cfg.transaction_min_timeout,
            positive_i32
        );
        set_runtime_time_millis!(
            runtime,
            transaction_max_timeout_ms,
            cfg.transaction_max_timeout,
            positive_i32
        );
        Ok(())
    }

    fn apply_broker_policy(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        // Zero is a valid `partition_disk_scan_interval_secs`: it disables the
        // scanner, so this one is not routed through the positive-only macro.
        if let Some(value) = runtime.partition_disk_scan_interval_secs {
            cfg.partition_disk_scan_interval =
                Time::from_secs(i64::try_from(value).unwrap_or(i64::MAX));
        }
        set_runtime_plain!(runtime, observer_lag_bound, cfg.observer_lag_bound);
        set_runtime_time_millis!(runtime, heartbeat_interval_ms, cfg.heartbeat_interval);
        set_runtime_time_millis!(runtime, heartbeat_timeout_ms, cfg.heartbeat_timeout);
        set_runtime_time_millis!(runtime, replica_lag_time_max_ms, cfg.replica_lag_time_max);
        set_runtime_time_millis!(
            runtime,
            controller_election_timeout_ms,
            cfg.controller_election_timeout
        );
        set_runtime_time_millis!(
            runtime,
            controller_heartbeat_interval_ms,
            cfg.controller_heartbeat_interval
        );
        if let Some(value) = runtime.controlled_shutdown_drain_timeout_ms {
            positive_u64("controlled_shutdown_drain_timeout_ms", value)?;
        }
        set_runtime_size_bytes!(
            runtime,
            metadata_max_bytes_between_snapshots,
            cfg.metadata_max_bytes_between_snapshots,
            positive_u64
        );
        // Zero disables the time-based snapshot cap, so it bypasses the
        // positive-only macro.
        if let Some(value) = runtime.metadata_max_snapshot_interval_ms {
            cfg.metadata_max_snapshot_interval =
                Time::from_millis(i64::try_from(value).unwrap_or(i64::MAX));
        }
        set_runtime_positive_u64!(
            runtime,
            metadata_snapshot_interval_records,
            cfg.metadata_snapshot_interval_records
        );
        // Zero disables the reaper, so it bypasses the positive-only macro.
        if let Some(value) = runtime.txn_abort_cleanup_interval_ms {
            cfg.txn_abort_cleanup_interval =
                Time::from_millis(i64::try_from(value).unwrap_or(i64::MAX));
        }
        set_runtime_time_secs!(
            runtime,
            leader_imbalance_check_interval_secs,
            cfg.leader_imbalance_check_interval
        );
        if let Some(value) = runtime.leader_imbalance_per_broker_percentage {
            let value = percentage("leader_imbalance_per_broker_percentage", value)?;
            cfg.leader_imbalance_per_broker = percent(value);
        }
        // Zero disables the periodic TLS watcher, so it bypasses the
        // positive-only macro.
        if let Some(value) = runtime.tls_reload_interval_ms {
            cfg.tls_reload_interval = Time::from_millis(i64::try_from(value).unwrap_or(i64::MAX));
        }
        set_runtime_plain!(
            runtime,
            max_incremental_fetch_session_cache_slots,
            cfg.max_incremental_fetch_session_cache_slots
        );
        set_runtime_plain!(runtime, max_connections, cfg.max_connections);
        set_runtime_plain!(runtime, max_connections_per_ip, cfg.max_connections_per_ip);
        set_runtime_time_millis!(
            runtime,
            delegation_token_max_lifetime_ms,
            cfg.delegation_token_max_lifetime,
            positive_i64
        );
        set_runtime_time_millis!(
            runtime,
            delegation_token_expiry_check_interval_ms,
            cfg.delegation_token_expiry_check_interval,
            positive_i64
        );
        set_runtime_time_millis!(
            runtime,
            delegation_token_default_renew_period_ms,
            cfg.delegation_token_default_renew_period,
            positive_i64
        );
        set_runtime_time_millis!(
            runtime,
            remote_log_manager_interval_ms,
            cfg.remote_log_manager_interval
        );
        Ok(())
    }

    fn apply_share_group(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        set_runtime_plain!(runtime, share_group_enable, cfg.share_group.enable);
        set_runtime_duration!(
            runtime,
            share_group_session_timeout_ms,
            cfg.share_group.session_timeout
        );
        set_runtime_duration!(
            runtime,
            share_group_heartbeat_interval_ms,
            cfg.share_group.heartbeat_interval
        );
        set_runtime_duration!(
            runtime,
            share_group_record_lock_duration_ms,
            cfg.share_group.record_lock_duration
        );
        if let Some(value) = runtime.share_group_max_delivery_attempts {
            let value = positive_i16("share_group_max_delivery_attempts", value)?;
            cfg.share_group.max_delivery_attempts = value;
        }
        set_runtime_i32!(
            runtime,
            share_group_max_inflight_records,
            cfg.share_group.max_inflight_records
        );
        if let Some(value) = runtime.share_group_isolation_level.take() {
            use crate::coordinator::unified::share::config::ShareIsolationLevel;
            let value = match value.as_str() {
                "read-uncommitted" => ShareIsolationLevel::ReadUncommitted,
                "read-committed" => ShareIsolationLevel::ReadCommitted,
                _ => {
                    return Err(invalid_runtime_value(
                        "share_group_isolation_level",
                        "expected `read-uncommitted` or `read-committed`",
                    ));
                }
            };
            cfg.share_group.isolation_level = value;
        }
        Ok(())
    }

    fn apply_streams_group(
        &mut self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        let runtime = self;
        set_runtime_duration!(
            runtime,
            streams_group_session_timeout_ms,
            cfg.streams_group.session_timeout
        );
        set_runtime_duration!(
            runtime,
            streams_group_heartbeat_interval_ms,
            cfg.streams_group.heartbeat_interval
        );
        if let Some(value) = runtime.streams_internal_topic_replication_factor {
            cfg.streams_group.internal_topic_replication_factor =
                positive_i16("streams_internal_topic_replication_factor", value)?;
        }
        if let Some(value) = runtime.streams_group_num_standby_replicas {
            if value < 0 {
                return Err(invalid_runtime_value(
                    "streams_group_num_standby_replicas",
                    "must be nonnegative",
                ));
            }
            cfg.streams_group.num_standby_replicas = value;
        }
        if let Some(value) = runtime.streams_group_num_warmup_replicas {
            if value < 0 {
                return Err(invalid_runtime_value(
                    "streams_group_num_warmup_replicas",
                    "must be nonnegative",
                ));
            }
            cfg.streams_group.num_warmup_replicas = value;
        }
        if let Some(value) = runtime.streams_group_acceptable_recovery_lag {
            if value < 0 {
                return Err(invalid_runtime_value(
                    "streams_group_acceptable_recovery_lag",
                    "must be nonnegative",
                ));
            }
            cfg.streams_group.acceptable_recovery_lag = value;
        }
        set_runtime_duration!(
            runtime,
            streams_group_task_offset_interval_ms,
            cfg.streams_group.task_offset_interval
        );
        if let Some(value) = runtime.streams_group_assignor.take() {
            use crate::coordinator::unified::streams::config::StreamsAssignorKind;
            let value = match value.as_str() {
                "auto" => StreamsAssignorKind::Auto,
                "sticky" => StreamsAssignorKind::Sticky,
                "highly-available" => StreamsAssignorKind::HighlyAvailable,
                _ => {
                    return Err(invalid_runtime_value(
                        "streams_group_assignor",
                        "expected `auto`, `sticky`, or `highly-available`",
                    ));
                }
            };
            cfg.streams_group.assignor = value;
        }

        if let Some(value) = runtime.inter_broker_server_name.take() {
            cfg.inter_broker_server_name = value;
        }
        Ok(())
    }
}

impl FileConfig {
    /// Apply this file-config to a `BrokerConfig`. Present `[runtime]` values
    /// replace current runtime values; other file sections retain their
    /// established fill-or-replace semantics.
    ///
    /// The broker binary uses [`Self::apply_before_runtime_overlay`] and then
    /// applies explicit CLI/environment values so those inputs win.
    ///
    /// **Caller contract:** when `--config-file` is used, the caller
    /// must NOT pass `--listen-addr` or `--advertised-listener`. The
    /// binary entrypoint enforces this (see `bin/broker.rs`); this
    /// method just merges what it's given.
    // Linear config-load pipeline; each arm is its own validator construction —
    // extraction obscures the dispatch shape.
    //
    // # Errors
    //
    // * [`FileConfigError::MissingSection`] when `[authorization] type = "opa"`
    //   is set without the required `[authorization.opa]` subtable.
    // * [`FileConfigError::OpaConfig`] when [`crate::authorizer::opa::OpaAuthorizer::new`]
    //   rejects the resolved knobs (zero cache size, no tokio runtime, etc.).
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn apply_to(self, cfg: &mut crate::config::BrokerConfig) -> Result<(), FileConfigError> {
        self.apply_to_inner(cfg, true)
    }

    /// Apply file values before a higher-precedence runtime overlay.
    ///
    /// Runtime relational validation is deferred until the caller applies the
    /// final overlay and validates the resolved [`crate::config::BrokerConfig`].
    ///
    /// # Errors
    ///
    /// Returns an error when any individual file value is invalid.
    pub fn apply_before_runtime_overlay(
        self,
        cfg: &mut crate::config::BrokerConfig,
    ) -> Result<(), FileConfigError> {
        self.apply_to_inner(cfg, false)
    }

    fn apply_to_inner(
        self,
        cfg: &mut crate::config::BrokerConfig,
        validate_runtime: bool,
    ) -> Result<(), FileConfigError> {
        let defaults = crate::config::BrokerConfig::default();
        let has_runtime = self.runtime.is_some();
        if let Some(runtime) = self.runtime {
            runtime.apply_to(cfg)?;
        }
        if let Some(id) = self.broker_id
            && cfg.broker_id == defaults.broker_id
        {
            cfg.broker_id = id;
        }
        if let Some(rack) = self.rack {
            cfg.rack = Some(rack);
        }
        if let Some(sel) = self.replica_selector {
            cfg.replica_selector = crate::replica_selector::ReplicaSelectorKind::from_config_str(
                &sel,
            )
            .map_err(|bad| {
                FileConfigError::InvalidConfig(format!("unknown replica_selector: {bad}"))
            })?;
        }
        if let Some(ms) = self.heartbeat_interval_ms
            && cfg.heartbeat_interval == defaults.heartbeat_interval
        {
            cfg.heartbeat_interval = millis_time("heartbeat_interval_ms", ms)?;
        }
        if let Some(ms) = self.heartbeat_timeout_ms
            && cfg.heartbeat_timeout == defaults.heartbeat_timeout
        {
            cfg.heartbeat_timeout = millis_time("heartbeat_timeout_ms", ms)?;
        }
        if let Some(ms) = self.replica_lag_time_max_ms
            && cfg.replica_lag_time_max == defaults.replica_lag_time_max
        {
            cfg.replica_lag_time_max = millis_time("replica_lag_time_max_ms", ms)?;
        }
        if let Some(ms) = self.controller_election_timeout_ms
            && cfg.controller_election_timeout == defaults.controller_election_timeout
        {
            cfg.controller_election_timeout = millis_time("controller_election_timeout_ms", ms)?;
        }
        if let Some(ms) = self.controller_heartbeat_interval_ms
            && cfg.controller_heartbeat_interval == defaults.controller_heartbeat_interval
        {
            cfg.controller_heartbeat_interval =
                millis_time("controller_heartbeat_interval_ms", ms)?;
        }
        if let Some(ld) = self.log_dir
            && cfg.log_dir == defaults.log_dir
        {
            cfg.log_dir = std::path::PathBuf::from(ld);
        }
        if !self.extra_log_dirs.is_empty() && cfg.extra_log_dirs.is_empty() {
            cfg.extra_log_dirs = self
                .extra_log_dirs
                .into_iter()
                .map(std::path::PathBuf::from)
                .collect();
        }
        apply_listener_settings(
            ListenerSettings {
                listeners: self.listeners,
                inter_broker_listener_name: self.inter_broker_listener_name,
                max_connections: self.max_connections,
                max_connections_per_ip: self.max_connections_per_ip,
                server_properties: self.server_properties,
                controller_listener_protocol: self.controller_listener_protocol,
                tls_config: self.tls_config,
            },
            cfg,
            &defaults,
        );
        apply_oauthbearer(self.oauthbearer, cfg);

        apply_delegation_tokens(self.delegation_token.as_ref(), cfg)?;

        // Merge the TOML super-user list into the broker's
        // set (initially empty). `extend` over `clone_from` because a
        // future CLI/programmatic source may pre-populate entries that
        // we should preserve. The `[authorization]` block
        // below may overwrite this with its own super-user list.
        if let Some(vec) = self.super_users {
            cfg.super_users.extend(vec.iter().cloned());
        }

        // `[remote_storage]` enables tiered storage broker-
        // wide. Exactly one of `storage_dir` (local filesystem),
        // `[remote_storage.s3]` (S3-compatible object store), or
        // `[remote_storage.gcs]` (native Google Cloud Storage) selects the
        // backend. More than one set → error.
        apply_remote_storage(self.remote_storage.as_ref(), cfg)?;

        // Pluggable cluster authorizer. When `[authorization]`
        // is present, its `super_users` list becomes the broker's
        // authoritative super-user set (overwriting whatever the
        // top-level list contributed above — operator O2
        // emits exactly one of the two sources). When absent, fall
        // through to the default [`AllowAllAuthorizer`] and leave
        // `cfg.super_users` as whatever the earlier extend produced.
        apply_config_tail(
            FileConfigTail {
                authorization: self.authorization,
                process: self.process,
                gssapi: self.gssapi,
                inter_broker_credentials: self.inter_broker_credentials,
                controller_quorum_voters: self.controller_quorum_voters,
                controller_server_name: self.controller_server_name,
                audit: self.audit,
            },
            cfg,
        )?;

        if has_runtime && validate_runtime {
            cfg.validate()
                .map_err(|error| FileConfigError::InvalidConfig(error.to_string()))?;
        }
        Ok(())
    }

    /// Parse a single `controller_quorum_voters` entry of the form
    /// `<node_id>@<host>:<port>` into `(NodeId, "<host>:<port>")`. The host is
    /// **not** DNS-resolved — it is carried verbatim so the dialer can
    /// re-resolve it on every (re)connect. Freezing a peer's boot-time IP here
    /// would strand a `StatefulSet` peer that restarts on a new pod IP (its
    /// stable DNS name still resolves, but to a different address). Only the
    /// shape is validated: a numeric node id and a `<host>:<port>` with a
    /// non-empty host and a numeric port.
    ///
    /// # Errors
    ///
    /// [`FileConfigError::InvalidQuorumVoter`] when the entry has no `@`, a
    /// non-numeric node id, or a malformed `<host>:<port>` (missing port,
    /// empty host, or non-numeric port).
    fn parse_quorum_voter(entry: &str) -> Result<(crabka_raft::NodeId, String), FileConfigError> {
        let (id_str, host_port) = entry.split_once('@').ok_or_else(|| {
            FileConfigError::InvalidQuorumVoter(format!(
                "{entry:?}: expected `<node_id>@<host>:<port>` (missing `@`)"
            ))
        })?;
        let node_id = crabka_raft::NodeId(id_str.parse::<u64>().map_err(|e| {
            FileConfigError::InvalidQuorumVoter(format!(
                "{entry:?}: invalid node id {id_str:?}: {e}"
            ))
        })?);
        // Validate the `<host>:<port>` shape without resolving. Split on the
        // LAST ':' so the port is taken from the end (the dialer splits the
        // same way), then carry `<host>:<port>` verbatim for per-dial lookup.
        let (host, port_str) = host_port.rsplit_once(':').ok_or_else(|| {
            FileConfigError::InvalidQuorumVoter(format!(
                "{entry:?}: expected `<host>:<port>` after `@` (missing `:port`)"
            ))
        })?;
        if host.is_empty() {
            return Err(FileConfigError::InvalidQuorumVoter(format!(
                "{entry:?}: empty host"
            )));
        }
        port_str.parse::<u16>().map_err(|e| {
            FileConfigError::InvalidQuorumVoter(format!(
                "{entry:?}: invalid port {port_str:?}: {e}"
            ))
        })?;
        Ok((node_id, host_port.to_string()))
    }
}

impl FileListener {
    #[must_use]
    pub fn into_spec(self) -> ListenerSpec {
        use crabka_security::{ClientAuthMode, TlsConfig as BrokerTlsConfig};
        ListenerSpec {
            name: self.name,
            bind_addr: self.bind_addr,
            advertised: self.advertised,
            protocol: self.protocol,
            tls_config: self.tls_config.map(|t| BrokerTlsConfig {
                cert_chain_path: t.cert_path,
                private_key_path: t.key_path,
                trust_roots_path: t.trust_roots_path,
                client_ca_path: t.client_ca_path,
                client_auth: match t.client_auth {
                    FileClientAuthMode::Disabled => ClientAuthMode::Disabled,
                    FileClientAuthMode::Optional => ClientAuthMode::Optional,
                    FileClientAuthMode::Required => ClientAuthMode::Required,
                },
            }),
            sasl_mechanisms: self.sasl_config.map(|s| s.enabled_mechanisms),
        }
    }
}

#[cfg(test)]
mod listener_auth_tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn file_listener_parses_per_listener_tls_config_inline() {
        let toml = r#"
broker_id = 0
log_dir = "/tmp"
inter_broker_listener_name = "internal"

[[listeners]]
name = "internal"
bind_addr = "0.0.0.0:9092"
advertised = "localhost:9092"
protocol = "Plaintext"

[[listeners]]
name = "data"
bind_addr = "0.0.0.0:9094"
advertised = "localhost:9094"
protocol = "Ssl"
tls_config = { cert_path = "/tls/broker.crt", key_path = "/tls/broker.key", client_ca_path = "/tls/clients-ca.crt", client_auth = "Required" }
"#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        assert!(cfg.listeners.len() == 2);
        assert!(cfg.listeners[0].tls_config.is_none());
        let data_tls = cfg.listeners[1].tls_config.as_ref().unwrap();
        let expected = FileTlsConfig {
            cert_path: std::path::PathBuf::from("/tls/broker.crt"),
            key_path: std::path::PathBuf::from("/tls/broker.key"),
            trust_roots_path: None,
            client_ca_path: Some(std::path::PathBuf::from("/tls/clients-ca.crt")),
            client_auth: FileClientAuthMode::Required,
        };
        assert!(*data_tls == expected);
    }

    #[test]
    fn file_listener_parses_per_listener_sasl_config_inline() {
        let toml = r#"
broker_id = 0
log_dir = "/tmp"
inter_broker_listener_name = "internal"

[[listeners]]
name = "scram"
bind_addr = "0.0.0.0:9094"
advertised = "localhost:9094"
protocol = "SaslSsl"
tls_config = { cert_path = "/tls/c", key_path = "/tls/k", client_auth = "Disabled" }
sasl_config = { enabled_mechanisms = ["SCRAM-SHA-512"] }
"#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        let sasl = cfg.listeners[0].sasl_config.as_ref().unwrap();
        assert!(sasl.enabled_mechanisms == vec![crabka_security::SaslMechanism::ScramSha512]);
    }

    #[test]
    fn top_level_tls_config_still_parses_back_compat() {
        let toml = r#"
broker_id = 0
log_dir = "/tmp"
inter_broker_listener_name = "internal"
controller_listener_protocol = "Ssl"

[[listeners]]
name = "internal"
bind_addr = "0.0.0.0:9092"
advertised = "localhost:9092"
protocol = "Plaintext"

[tls_config]
cert_path = "/tls/c"
key_path = "/tls/k"
client_ca_path = "/tls/clients-ca"
client_auth = "Required"
"#;
        let cfg: FileConfig = toml::from_str(toml).unwrap();
        assert!(cfg.tls_config.is_some());
        assert!(cfg.listeners[0].tls_config.is_none());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use assert2::{assert, check};
    use crabka_units::{
        convert::{ByteSizeExt as _, RatioExt as _, TimeExt as _},
        days, hours, mebibytes, millis, minutes, secs,
    };

    use super::*;

    /// Serializes any test that mutates process-wide env vars. Tests in
    /// the same `cargo test` process run on multiple threads by default,
    /// and `set_var`/`remove_var` are global side-effects.
    static ENV_LOCK_CELL: OnceLock<Mutex<()>> = OnceLock::new();
    fn env_lock() -> &'static Mutex<()> {
        ENV_LOCK_CELL.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn s3_config_debug_redacts_credentials() {
        let cfg = FileRemoteStorageS3Config {
            bucket: "logs".to_string(),
            region: "us-east-1".to_string(),
            prefix: None,
            endpoint: None,
            access_key_id: Some("AKIAEXAMPLEKEYID".to_string()),
            secret_access_key: Some("super-secret-key-value".to_string()),
            allow_http: false,
            multipart_threshold: None,
            multipart_chunk_size: None,
        };
        let dbg = format!("{cfg:?}");
        // Secrets are redacted; non-secret fields are still printed.
        let cases = [
            ("super-secret-key-value", false),
            ("AKIAEXAMPLEKEYID", false),
            ("***", true),
            ("logs", true),
            ("us-east-1", true),
        ];
        for (needle, want) in cases {
            assert!(dbg.contains(needle) == want, "needle {needle:?} in: {dbg}");
        }
    }

    #[test]
    fn empty_toml_round_trips() {
        let cfg: FileConfig = toml::from_str("").unwrap();
        assert!(cfg == FileConfig::default());
    }

    #[test]
    fn full_toml_round_trips() {
        let src = r#"
broker_id = 0
log_dir = "/var/lib/crabka/data"
inter_broker_listener_name = "PLAIN"

[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0:9092"
protocol = "Plaintext"

[[listeners]]
name = "EXTERNAL"
bind_addr = "0.0.0.0:9094"
advertised = "10.0.1.5:32100"
protocol = "Plaintext"

[server_properties]
"log.retention.hours" = "24"
"#;
        let cfg: FileConfig = toml::from_str(src).unwrap();
        let expected = FileConfig {
            runtime: None,
            broker_id: Some(0),
            log_dir: Some("/var/lib/crabka/data".to_string()),
            extra_log_dirs: vec![],
            rack: None,
            replica_selector: None,
            heartbeat_interval_ms: None,
            heartbeat_timeout_ms: None,
            replica_lag_time_max_ms: None,
            controller_election_timeout_ms: None,
            controller_heartbeat_interval_ms: None,
            inter_broker_listener_name: Some("PLAIN".to_string()),
            max_connections: None,
            max_connections_per_ip: None,
            controller_quorum_voters: vec![],
            controller_server_name: None,
            listeners: vec![
                FileListener {
                    name: "PLAIN".to_string(),
                    bind_addr: "0.0.0.0:9092".parse().unwrap(),
                    advertised: "demo-0:9092".to_string(),
                    protocol: ListenerProtocol::Plaintext,
                    tls_config: None,
                    sasl_config: None,
                },
                FileListener {
                    name: "EXTERNAL".to_string(),
                    bind_addr: "0.0.0.0:9094".parse().unwrap(),
                    advertised: "10.0.1.5:32100".to_string(),
                    protocol: ListenerProtocol::Plaintext,
                    tls_config: None,
                    sasl_config: None,
                },
            ],
            server_properties: std::collections::BTreeMap::from([(
                "log.retention.hours".to_string(),
                "24".to_string(),
            )]),
            controller_listener_protocol: None,
            tls_config: None,
            oauthbearer: None,
            delegation_token: None,
            super_users: None,
            remote_storage: None,
            authorization: None,
            process: None,
            gssapi: None,
            inter_broker_credentials: None,
            audit: None,
        };
        assert!(cfg == expected);
    }

    #[test]
    fn unknown_top_level_key_is_ignored() {
        // Forward-compat: a newer config file shouldn't break older brokers.
        let src = r#"
broker_id = 0
some_future_field = "from-a-later-slice"
"#;
        let cfg: FileConfig = toml::from_str(src).unwrap();
        assert!(cfg.broker_id == Some(0));
    }

    #[test]
    fn snake_case_protocol_names() {
        let src = r#"
[[listeners]]
name = "S"
bind_addr = "0.0.0.0:9094"
advertised = "h:9094"
protocol = "SaslSsl"
"#;
        let cfg: FileConfig = toml::from_str(src).unwrap();
        assert!(cfg.listeners[0].protocol == ListenerProtocol::SaslSsl);
    }

    #[test]
    fn invalid_bind_addr_is_an_error() {
        let src = r#"
[[listeners]]
name = "X"
bind_addr = "not-a-socket-address"
advertised = "h:9094"
protocol = "Plaintext"
"#;
        let err = toml::from_str::<FileConfig>(src).unwrap_err();
        assert!(
            err.to_string().contains("bind_addr") || err.to_string().contains("socket"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn file_listener_into_spec_preserves_fields() {
        let fl = FileListener {
            name: "X".into(),
            bind_addr: "0.0.0.0:9094".parse().unwrap(),
            advertised: "h:9094".into(),
            protocol: ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_config: None,
        };
        let spec = fl.into_spec();
        check!(spec.name == "X");
        check!(spec.bind_addr == "0.0.0.0:9094".parse::<SocketAddr>().unwrap());
        check!(spec.advertised == "h:9094");
        check!(spec.protocol == ListenerProtocol::Plaintext);
        check!(spec.tls_config.is_none());
        check!(spec.sasl_mechanisms.is_none());
    }

    #[test]
    fn apply_to_populates_listeners() {
        use crate::config::BrokerConfig;

        let src = r#"
inter_broker_listener_name = "PLAIN"

[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0:9092"
protocol = "Plaintext"
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        check!(cfg.listeners.len() == 1);
        check!(cfg.listeners[0].name.as_str() == "PLAIN");
        check!(cfg.listeners[0].advertised.as_str() == "demo-0:9092");
        check!(cfg.inter_broker_listener_name.as_str() == "PLAIN");
    }

    #[test]
    fn apply_to_log_dir_fills_default_but_preserves_existing() {
        use crate::config::BrokerConfig;

        let file: FileConfig = toml::from_str(r#"log_dir = "/var/lib/crabka/file""#).unwrap();

        let mut default_cfg = BrokerConfig::default();
        file.clone().apply_to(&mut default_cfg).unwrap();
        assert!(default_cfg.log_dir == std::path::PathBuf::from("/var/lib/crabka/file"));

        let mut existing_cfg = BrokerConfig {
            log_dir: std::path::PathBuf::from("/var/lib/crabka/cli"),
            ..BrokerConfig::default()
        };
        file.apply_to(&mut existing_cfg).unwrap();
        assert!(existing_cfg.log_dir == std::path::PathBuf::from("/var/lib/crabka/cli"));
    }

    #[test]
    fn apply_to_extra_log_dirs_fills_empty_but_preserves_existing() {
        use crate::config::BrokerConfig;

        let file: FileConfig = toml::from_str(r#"extra_log_dirs = ["/mnt/a", "/mnt/b"]"#).unwrap();

        let mut default_cfg = BrokerConfig::default();
        file.clone().apply_to(&mut default_cfg).unwrap();
        assert!(
            default_cfg.extra_log_dirs
                == vec![
                    std::path::PathBuf::from("/mnt/a"),
                    std::path::PathBuf::from("/mnt/b"),
                ]
        );

        let mut existing_cfg = BrokerConfig {
            extra_log_dirs: vec![std::path::PathBuf::from("/mnt/cli")],
            ..BrokerConfig::default()
        };
        file.apply_to(&mut existing_cfg).unwrap();
        assert!(existing_cfg.extra_log_dirs == vec![std::path::PathBuf::from("/mnt/cli")]);

        let mut empty_file_existing_cfg = BrokerConfig {
            extra_log_dirs: vec![std::path::PathBuf::from("/mnt/cli")],
            ..BrokerConfig::default()
        };
        FileConfig::default()
            .apply_to(&mut empty_file_existing_cfg)
            .unwrap();
        assert!(
            empty_file_existing_cfg.extra_log_dirs == vec![std::path::PathBuf::from("/mnt/cli")]
        );
    }

    #[test]
    fn apply_to_maps_connection_caps() {
        use crate::config::BrokerConfig;

        let src = r"
max_connections = 100
max_connections_per_ip = 8
";
        let file: FileConfig = toml::from_str(src).unwrap();
        assert!(file.max_connections == Some(100));
        assert!(file.max_connections_per_ip == Some(8));

        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(cfg.max_connections == 100);
        assert!(cfg.max_connections_per_ip == 8);
    }

    #[test]
    fn apply_to_omitted_connection_caps_keep_default_unlimited() {
        use crate::config::BrokerConfig;

        let file: FileConfig = toml::from_str("broker_id = 0").unwrap();
        assert!(file.max_connections == None);
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        // Omitted → unchanged from the (unlimited) BrokerConfig default.
        assert!(cfg.max_connections == usize::MAX);
        assert!(cfg.max_connections_per_ip == usize::MAX);
    }

    #[test]
    fn apply_to_reads_two_phase_commit_enable_from_server_properties() {
        use crate::config::BrokerConfig;

        // KIP-939: the `transaction.two.phase.commit.enable` server property
        // flips the cluster 2PC gate on; absent / "false" leaves it off.
        let on: FileConfig = toml::from_str(
            "[server_properties]\n\"transaction.two.phase.commit.enable\" = \"true\"\n",
        )
        .unwrap();
        let mut cfg = BrokerConfig::default();
        assert!(!cfg.features.transaction_two_phase_commit_enable); // default
        on.apply_to(&mut cfg).unwrap();
        assert!(cfg.features.transaction_two_phase_commit_enable);

        // Omitted → unchanged (stays at the default false).
        let absent: FileConfig = toml::from_str("broker_id = 0").unwrap();
        let mut cfg2 = BrokerConfig::default();
        absent.apply_to(&mut cfg2).unwrap();
        assert!(!cfg2.features.transaction_two_phase_commit_enable);
    }

    #[test]
    fn apply_to_parses_multi_voter_quorum_in_order() {
        use crate::config::BrokerConfig;

        let src = r#"
controller_quorum_voters = ["0@127.0.0.1:9093", "1@127.0.0.2:9093", "2@127.0.0.3:9093"]
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        // Host:port carried verbatim (parsed, NOT DNS-resolved) so the dialer
        // re-resolves each peer per connect.
        let expected: Vec<(crabka_raft::NodeId, String)> = vec![
            (crabka_audit::NodeId(0), "127.0.0.1:9093".to_string()),
            (crabka_audit::NodeId(1), "127.0.0.2:9093".to_string()),
            (crabka_audit::NodeId(2), "127.0.0.3:9093".to_string()),
        ];
        assert!(cfg.controller_quorum_voters == expected);
    }

    #[test]
    fn apply_to_keeps_unresolvable_hostname_without_dns() {
        use crate::config::BrokerConfig;

        // A peer FQDN that does not resolve right now (a `StatefulSet` peer
        // whose A record isn't published yet, or simply offline) MUST be
        // accepted and carried verbatim — the old resolve-at-startup path
        // would have failed the whole broker boot here. The dialer resolves it
        // later, per connect, so a peer coming up on a new pod IP is reachable.
        let src = r#"
controller_quorum_voters = ["0@demo-broker-0-0.demo-broker-headless.default.svc.cluster.local:9093"]
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        let expected: Vec<(crabka_raft::NodeId, String)> = vec![(
            crabka_audit::NodeId(0),
            "demo-broker-0-0.demo-broker-headless.default.svc.cluster.local:9093".to_string(),
        )];
        assert!(cfg.controller_quorum_voters == expected);
    }

    #[test]
    fn apply_to_rejects_malformed_quorum_voters() {
        use crate::config::BrokerConfig;

        let cases = [
            ("0@just-a-host", "missing port"),
            ("0@host:nine-thousand", "non-numeric port"),
            ("127.0.0.1:9093", "missing @"),
            ("foo@127.0.0.1:9093", "non-numeric id"),
        ];
        for (voter, label) in cases {
            let src = format!("controller_quorum_voters = [\"{voter}\"]\n");
            let file: FileConfig = toml::from_str(&src).unwrap();
            let mut cfg = BrokerConfig::default();
            let err = file.apply_to(&mut cfg).unwrap_err();
            assert!(
                matches!(err, FileConfigError::InvalidQuorumVoter(_)),
                "voter {voter:?} ({label}) must be rejected as InvalidQuorumVoter; got {err:?}"
            );
        }
    }

    #[test]
    fn apply_to_empty_quorum_voters_leaves_existing_unchanged() {
        use crate::config::BrokerConfig;

        // No `controller_quorum_voters` key at all → empty default.
        let file: FileConfig = toml::from_str("broker_id = 0").unwrap();
        assert!(file.controller_quorum_voters.is_empty());

        // Seed a pre-existing single self-voter as the binary would.
        let seeded: Vec<(crabka_raft::NodeId, String)> =
            vec![(crabka_audit::NodeId(7), "127.0.0.1:9093".to_string())];
        let mut cfg = BrokerConfig {
            controller_quorum_voters: seeded.clone(),
            ..BrokerConfig::default()
        };

        file.apply_to(&mut cfg).unwrap();

        // Empty list must NOT clear the seeded voter set.
        assert!(cfg.controller_quorum_voters == seeded);
    }

    #[test]
    fn apply_to_does_not_clobber_non_default_broker_id() {
        use crate::config::BrokerConfig;

        let src = r"broker_id = 42";
        let file: FileConfig = toml::from_str(src).unwrap();
        // simulate CLI --broker-id 7 already applied
        let mut cfg = BrokerConfig {
            broker_id: 7,
            ..BrokerConfig::default()
        };

        file.apply_to(&mut cfg).unwrap();

        // CLI value wins because it differs from default.
        assert!(cfg.broker_id == 7);
    }

    #[test]
    fn apply_to_fills_in_default_broker_id() {
        use crate::config::BrokerConfig;

        let src = r"broker_id = 42";
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = BrokerConfig::default(); // broker_id == default (1)

        file.apply_to(&mut cfg).unwrap();

        assert!(cfg.broker_id == 42);
    }

    #[test]
    fn apply_to_fills_heartbeat_and_lag_tunables() {
        use crate::config::BrokerConfig;

        let src = r"
heartbeat_interval_ms = 500
heartbeat_timeout_ms = 1500
replica_lag_time_max_ms = 2000
controller_election_timeout_ms = 500
controller_heartbeat_interval_ms = 100
";
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = BrokerConfig::default();

        file.apply_to(&mut cfg).unwrap();

        check!(cfg.heartbeat_interval == millis(500));
        check!(cfg.heartbeat_timeout == millis(1500));
        check!(cfg.replica_lag_time_max == secs(2));
        check!(cfg.controller_election_timeout == millis(500));
        check!(cfg.controller_heartbeat_interval == millis(100));
    }

    #[test]
    fn tls_keys_round_trip() {
        let src = r#"
controller_listener_protocol = "Ssl"

[tls_config]
cert_path = "/etc/crabka/broker-tls/0.crt"
key_path  = "/etc/crabka/broker-tls/0.key"
client_ca_path = "/etc/crabka/cluster-ca/ca.crt"
client_auth = "Required"
"#;
        let cfg: FileConfig = toml::from_str(src).expect("parse TLS config");
        assert!(cfg.controller_listener_protocol == Some(ListenerProtocol::Ssl));
        let tls = cfg.tls_config.expect("tls_config present");
        assert!(tls.cert_path == std::path::PathBuf::from("/etc/crabka/broker-tls/0.crt"));
        assert!(tls.client_auth == FileClientAuthMode::Required);
    }

    #[test]
    fn tls_keys_absent_round_trips() {
        let src = r#"
broker_id = 0
[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0:9092"
protocol = "Plaintext"
"#;
        let cfg: FileConfig = toml::from_str(src).expect("parse no-TLS");
        assert!(cfg.controller_listener_protocol == None);
        assert!(cfg.tls_config.is_none());
    }

    #[test]
    fn apply_to_propagates_tls_config() {
        let src = r#"
controller_listener_protocol = "Ssl"
[tls_config]
cert_path = "/c"
key_path = "/k"
client_ca_path = "/ca"
client_auth = "Required"
"#;
        let file: FileConfig = toml::from_str(src).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(cfg.controller_listener_protocol == crabka_security::ListenerProtocol::Ssl);
        let tls = cfg.tls_config.expect("tls_config propagated");
        assert!(tls.cert_chain_path == std::path::PathBuf::from("/c"));
    }

    #[test]
    fn apply_to_threads_trust_roots_and_controller_server_name() {
        // The operator renders the cluster CA as the dialer trust root and
        // the shared headless FQDN as the controller SNI so KIP-595 peers can
        // mTLS to each other.
        let src = r#"
controller_server_name = "demo-broker-headless.default.svc.cluster.local"
[tls_config]
cert_path = "/etc/crabka/broker-tls/0.crt"
key_path = "/etc/crabka/broker-tls/0.key"
trust_roots_path = "/etc/crabka/cluster-ca/ca.crt"
client_ca_path = "/etc/crabka/cluster-ca/ca.crt"
client_auth = "Required"
"#;
        let file: FileConfig = toml::from_str(src).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(
            cfg.controller_server_name.as_deref()
                == Some("demo-broker-headless.default.svc.cluster.local")
        );
        let tls = cfg.tls_config.expect("tls_config propagated");
        assert!(
            tls.trust_roots_path.as_deref()
                == Some(std::path::Path::new("/etc/crabka/cluster-ca/ca.crt"))
        );
    }

    #[test]
    fn apply_to_absent_controller_server_name_leaves_default() {
        let src = r#"
controller_listener_protocol = "Ssl"
[tls_config]
cert_path = "/c"
key_path = "/k"
client_auth = "Required"
"#;
        let file: FileConfig = toml::from_str(src).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(cfg.controller_server_name.is_none());
        assert!(cfg.tls_config.expect("tls").trust_roots_path.is_none());
    }

    #[test]
    fn apply_to_oauthbearer_jwks_selects_signed_validator() {
        let src = r#"
[oauthbearer]
jwks_endpoint_uri = "https://idp.example/jwks"
valid_issuer_uri = "https://idp.example"
expected_audience = "kafka"
principal_claim_name = "client_id"
jwks_refresh_interval_ms = 60000
jwks_expiry_seconds = 360
"#;
        let file: FileConfig = toml::from_str(src).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(cfg.oauthbearer_jwks_endpoint.as_deref() == Some("https://idp.example/jwks"));
        assert!(cfg.oauthbearer_jwks_refresh_interval == minutes(1));
        match cfg.oauthbearer_validator {
            crabka_security::OAuthBearerValidator::Signed(v) => {
                check!(v.valid_issuer.as_deref() == Some("https://idp.example"));
                check!(v.expected_audience.as_deref() == Some("kafka"));
                check!(v.principal_claim_name.as_str() == "client_id");
                check!(v.cache_expiry == Some(secs(360)));
            }
            other => panic!("jwks_endpoint_uri must select the Signed validator; got {other:?}"),
        }
    }

    #[test]
    fn apply_to_oauthbearer_without_jwks_stays_unsecured() {
        let src = r#"
[oauthbearer]
principal_claim_name = "sub"
allowable_clock_skew_ms = 5000
"#;
        let file: FileConfig = toml::from_str(src).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(cfg.oauthbearer_jwks_endpoint.is_none());
        match cfg.oauthbearer_validator {
            crabka_security::OAuthBearerValidator::Unsecured(v) => {
                assert!(v.allowable_clock_skew == secs(5));
            }
            other => {
                panic!("no jwks_endpoint_uri must keep the unsecured validator; got {other:?}")
            }
        }
    }

    #[test]
    fn apply_to_oauthbearer_threads_idp_tls_trust_to_broker_config() {
        let toml = r#"
[oauthbearer]
jwks_endpoint_uri = "https://idp.example/certs"
idp_tls_trust = "/etc/crabka/oauth/idp-ca.pem"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(
            cfg.oauthbearer_idp_tls_trust.as_deref()
                == Some(std::path::Path::new("/etc/crabka/oauth/idp-ca.pem"))
        );
    }

    #[test]
    fn apply_to_oauthbearer_without_idp_tls_trust_leaves_field_none() {
        let toml = r#"
[oauthbearer]
jwks_endpoint_uri = "https://idp.example/certs"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(cfg.oauthbearer_idp_tls_trust.is_none());
    }

    #[test]
    fn apply_to_oauthbearer_selects_introspection_validator_when_endpoint_set() {
        let dir = tempfile::tempdir().unwrap();
        let secret_path = dir.path().join("client-secret");
        std::fs::write(&secret_path, "the-secret").unwrap();
        let toml = format!(
            r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "kafka-broker"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let file: FileConfig = toml::from_str(&toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(matches!(
            cfg.oauthbearer_validator,
            crabka_security::OAuthBearerValidator::Introspection(_)
        ));
    }

    #[test]
    #[should_panic(expected = "mutually exclusive")]
    fn apply_to_oauthbearer_rejects_both_jwks_and_introspection_set() {
        let dir = tempfile::tempdir().unwrap();
        let secret_path = dir.path().join("client-secret");
        std::fs::write(&secret_path, "x").unwrap();
        let toml = format!(
            r#"
[oauthbearer]
jwks_endpoint_uri = "https://idp.example/jwks"
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "id"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let file: FileConfig = toml::from_str(&toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
    }

    #[test]
    #[should_panic(expected = "introspection_client_id")]
    fn apply_to_oauthbearer_introspection_requires_client_id() {
        let dir = tempfile::tempdir().unwrap();
        let secret_path = dir.path().join("client-secret");
        std::fs::write(&secret_path, "x").unwrap();
        let toml = format!(
            r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let file: FileConfig = toml::from_str(&toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
    }

    #[test]
    #[should_panic(expected = "introspection_client_secret_path")]
    fn apply_to_oauthbearer_introspection_requires_client_secret_path() {
        let toml = r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "kafka-broker"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
    }

    #[test]
    fn apply_to_oauthbearer_introspection_with_userinfo_sets_call_userinfo_true() {
        let dir = tempfile::tempdir().unwrap();
        let secret_path = dir.path().join("client-secret");
        std::fs::write(&secret_path, "x").unwrap();
        let toml = format!(
            r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
userinfo_endpoint_uri = "https://idp.example/userinfo"
introspection_client_id = "id"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let file: FileConfig = toml::from_str(&toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        match cfg.oauthbearer_validator {
            crabka_security::OAuthBearerValidator::Introspection(v) => assert!(v.call_userinfo),
            other => panic!("expected Introspection, got {other:?}"),
        }
    }

    #[test]
    fn apply_to_oauthbearer_introspection_without_userinfo_sets_call_userinfo_false() {
        let dir = tempfile::tempdir().unwrap();
        let secret_path = dir.path().join("client-secret");
        std::fs::write(&secret_path, "x").unwrap();
        let toml = format!(
            r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "id"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let file: FileConfig = toml::from_str(&toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        match cfg.oauthbearer_validator {
            crabka_security::OAuthBearerValidator::Introspection(v) => assert!(!v.call_userinfo),
            other => panic!("expected Introspection, got {other:?}"),
        }
    }

    #[test]
    fn apply_to_empty_listeners_does_not_clear_existing() {
        use crate::config::BrokerConfig;

        let file: FileConfig = toml::from_str("").unwrap();
        let mut cfg = BrokerConfig {
            listeners: vec![crate::config::ListenerSpec {
                name: "X".into(),
                bind_addr: "0.0.0.0:9094".parse().unwrap(),
                advertised: "h:9094".into(),
                protocol: crabka_security::ListenerProtocol::Plaintext,
                tls_config: None,
                sasl_mechanisms: None,
            }],
            ..BrokerConfig::default()
        };

        file.apply_to(&mut cfg).unwrap();

        assert!(cfg.listeners.len() == 1);
        assert!(cfg.listeners[0].name == "X");
    }

    #[test]
    fn apply_to_syncs_advertised_listener_from_inter_broker_listener() {
        use crate::config::BrokerConfig;

        // Two listeners; the inter-broker one ("PLAIN") is NOT declared first.
        // `advertised_listener` (used by FindCoordinator + broker
        // self-registration) must be taken from the inter-broker listener's
        // `advertised` (the pod FQDN), not left at the CLI default
        // 127.0.0.1:9092 and not taken from the first-declared listener.
        let toml = r#"
inter_broker_listener_name = "PLAIN"

[[listeners]]
name = "EXTERNAL"
bind_addr = "0.0.0.0:9094"
advertised = "ext.example.com:9094"
protocol = "Plaintext"

[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0.demo-broker-headless.default.svc.cluster.local:9092"
protocol = "Plaintext"
"#;
        let file: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        assert!(
            cfg.advertised_listener == "demo-0.demo-broker-headless.default.svc.cluster.local:9092"
        );
        // The inter-broker listener wins over the first-declared EXTERNAL one.
        assert!(cfg.advertised_listener != "ext.example.com:9094");
    }

    #[test]
    fn remote_storage_section_enables_and_sets_dir() {
        let toml = r#"
[remote_storage]
storage_dir = "/var/lib/crabka/tier"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        match cfg.remote_storage_backend {
            Some(crate::config::RemoteStorageBackend::Local { dir }) => {
                assert!(dir == std::path::PathBuf::from("/var/lib/crabka/tier"));
            }
            other => panic!("expected Local backend, got {other:?}"),
        }
    }

    #[test]
    fn no_remote_storage_section_leaves_backend_none() {
        let file: FileConfig = toml::from_str("broker_id = 1").unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(cfg.remote_storage_backend.is_none());
        // No remote_storage section: RLMM stays at the production default (TopicBacked).
        assert!(matches!(
            cfg.remote_log_metadata,
            crate::config::RlmmKind::TopicBacked(_)
        ));
    }

    #[test]
    fn kafka_metadata_section_parses_with_defaults() {
        let toml = r#"
[remote_storage]
storage_dir = "/tmp/tier"

[remote_storage.kafka_metadata]
bootstrap = "127.0.0.1:9092"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        let km = match &cfg.remote_log_metadata {
            crate::config::RlmmKind::TopicBacked(k) => k.clone(),
            crate::config::RlmmKind::InMemory => panic!("expected TopicBacked"),
        };
        check!(km.bootstrap.as_str() == "127.0.0.1:9092");
        check!(km.num_partitions == 50);
        check!(km.replication == 3);
    }

    #[test]
    fn kafka_metadata_section_honors_overrides() {
        let toml = r#"
[remote_storage]
storage_dir = "/tmp/tier"

[remote_storage.kafka_metadata]
bootstrap = "broker-0:9094"
num_partitions = 8
replication = 1
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        let km = match &cfg.remote_log_metadata {
            crate::config::RlmmKind::TopicBacked(k) => k.clone(),
            crate::config::RlmmKind::InMemory => panic!("expected TopicBacked"),
        };
        check!(km.bootstrap.as_str() == "broker-0:9094");
        check!(km.num_partitions == 8);
        check!(km.replication == 1);
    }

    #[test]
    fn remote_storage_s3_section_parses() {
        let toml = r#"
[remote_storage.s3]
bucket = "crabka-prod"
region = "us-east-1"
prefix = "cluster-a"
endpoint = "http://minio:9000"
allow_http = true
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        match cfg.remote_storage_backend {
            Some(crate::config::RemoteStorageBackend::S3(s3)) => {
                // Credentials default to None and the multipart knobs default
                // when the TOML omits them.
                check!(s3.bucket.as_str() == "crabka-prod");
                check!(s3.region.as_str() == "us-east-1");
                check!(s3.prefix.as_deref() == Some("cluster-a"));
                check!(s3.endpoint.as_deref() == Some("http://minio:9000"));
                check!(s3.allow_http);
                check!(s3.access_key_id.is_none());
                check!(s3.secret_access_key.is_none());
                check!(
                    s3.multipart_threshold == crabka_remote_storage::DEFAULT_MULTIPART_THRESHOLD
                );
                check!(
                    s3.multipart_chunk_size == crabka_remote_storage::DEFAULT_MULTIPART_CHUNK_SIZE
                );
            }
            other => panic!("expected S3 backend, got {other:?}"),
        }
    }

    #[test]
    fn remote_storage_s3_section_round_trips_multipart_overrides() {
        let toml = r#"
[remote_storage.s3]
bucket = "b"
region = "us-east-1"
multipart_threshold = 8192
multipart_chunk_size = 5242880
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        match cfg.remote_storage_backend {
            Some(crate::config::RemoteStorageBackend::S3(s3)) => {
                assert!(s3.multipart_threshold == 8192);
                assert!(s3.multipart_chunk_size == 5_242_880);
            }
            other => panic!("expected S3 backend, got {other:?}"),
        }
    }

    #[test]
    fn remote_storage_local_and_s3_together_rejected() {
        let toml = r#"
[remote_storage]
storage_dir = "/tmp/tier"

[remote_storage.s3]
bucket = "b"
region = "us-east-1"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        let err = file.apply_to(&mut cfg).unwrap_err();
        let rendered = err.to_string();
        assert!(
            rendered.contains("cannot set both"),
            "expected backend-conflict error, got: {rendered}"
        );
    }

    #[test]
    fn remote_storage_gcs_section_parses() {
        let toml = r#"
[remote_storage.gcs]
bucket = "crabka-prod"
prefix = "cluster-a"
endpoint = "http://fake-gcs:4443"
allow_http = true
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        match cfg.remote_storage_backend {
            Some(crate::config::RemoteStorageBackend::Gcs(g)) => {
                // Leaving all credential fields unset selects Workload
                // Identity / ADC; multipart knobs default when the TOML
                // omits them.
                check!(g.bucket.as_str() == "crabka-prod");
                check!(g.prefix.as_deref() == Some("cluster-a"));
                check!(g.endpoint.as_deref() == Some("http://fake-gcs:4443"));
                check!(g.allow_http);
                check!(g.service_account_path.is_none());
                check!(g.service_account_key.is_none());
                check!(g.application_credentials_path.is_none());
                check!(g.multipart_threshold == crabka_remote_storage::DEFAULT_MULTIPART_THRESHOLD);
                check!(
                    g.multipart_chunk_size == crabka_remote_storage::DEFAULT_MULTIPART_CHUNK_SIZE
                );
            }
            other => panic!("expected Gcs backend, got {other:?}"),
        }
    }

    #[test]
    fn remote_storage_gcs_credentials_parse() {
        let toml = r#"
[remote_storage.gcs]
bucket = "b"
service_account_path = "/etc/gcs/key.json"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        match cfg.remote_storage_backend {
            Some(crate::config::RemoteStorageBackend::Gcs(g)) => {
                assert!(g.bucket == "b");
                assert!(g.service_account_path.as_deref() == Some("/etc/gcs/key.json"));
            }
            other => panic!("expected Gcs backend, got {other:?}"),
        }
    }

    #[test]
    fn remote_storage_gcs_config_debug_redacts_credentials() {
        let gcs = FileRemoteStorageGcsConfig {
            bucket: "crabka-prod".into(),
            prefix: None,
            service_account_path: Some("/etc/gcs/sa-path.json".into()),
            service_account_key: Some("super-secret-inline-key".into()),
            application_credentials_path: Some("/etc/gcs/adc.json".into()),
            endpoint: None,
            allow_http: false,
            multipart_threshold: None,
            multipart_chunk_size: None,
        };
        let rendered = format!("{gcs:?}");
        // All three credential fields are redacted; non-secret fields are
        // still printed.
        let cases = [
            ("/etc/gcs/sa-path.json", false),
            ("super-secret-inline-key", false),
            ("/etc/gcs/adc.json", false),
            ("***", true),
            ("crabka-prod", true),
        ];
        for (needle, want) in cases {
            assert!(
                rendered.contains(needle) == want,
                "needle {needle:?} in: {rendered}"
            );
        }
    }

    #[test]
    fn remote_storage_local_and_gcs_together_rejected() {
        let toml = r#"
[remote_storage]
storage_dir = "/tmp/tier"

[remote_storage.gcs]
bucket = "b"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        let err = file.apply_to(&mut cfg).unwrap_err();
        let rendered = err.to_string();
        assert!(
            rendered.contains("cannot set"),
            "expected backend-conflict error, got: {rendered}"
        );
    }

    #[test]
    fn remote_storage_s3_and_gcs_together_rejected() {
        let toml = r#"
[remote_storage.s3]
bucket = "b"
region = "us-east-1"

[remote_storage.gcs]
bucket = "b"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        let err = file.apply_to(&mut cfg).unwrap_err();
        let rendered = err.to_string();
        assert!(
            rendered.contains("cannot set"),
            "expected backend-conflict error, got: {rendered}"
        );
    }

    #[test]
    fn delegation_token_section_parses_secret_key_and_defaults() {
        // Hold the lock so a concurrently-running env-var test can't
        // leak CRABKA_DELEGATION_TOKEN_SECRET_KEY into this assertion.
        // `temp_env::with_var_unset` removes the var for the duration
        // of the closure and restores the prior value on return —
        // safe against the workspace `forbid(unsafe_code)` lint.
        let _g = env_lock().lock().unwrap();
        temp_env::with_var_unset("CRABKA_DELEGATION_TOKEN_SECRET_KEY", || {
            let toml = r#"
[delegation_token]
secret_key = "abcdef"
"#;
            let file: FileConfig = toml::from_str(toml).unwrap();
            let mut cfg = crate::config::BrokerConfig::default();
            file.apply_to(&mut cfg).unwrap();

            // KIP-48 defaults: 7 days max lifetime, 1 hour sweep cadence,
            // 24 hour default renew period.
            check!(
                cfg.delegation_token_secret_key
                    .as_ref()
                    .map(|s| s.as_bytes().to_vec())
                    == Some(b"abcdef".to_vec())
            );
            check!(cfg.delegation_token_max_lifetime == days(7));
            check!(cfg.delegation_token_expiry_check_interval == hours(1));
            check!(cfg.delegation_token_default_renew_period == hours(24));
        });
    }

    #[test]
    fn delegation_token_default_renew_period_ms_default_and_override() {
        let _g = env_lock().lock().unwrap();
        temp_env::with_var_unset("CRABKA_DELEGATION_TOKEN_SECRET_KEY", || {
            // (1) When the TOML omits `default_renew_period_ms`, the config
            //     stays at the 24h KIP-48 default.
            let toml = r#"
[delegation_token]
secret_key = "abcdef"
"#;
            let file: FileConfig = toml::from_str(toml).unwrap();
            let mut cfg = crate::config::BrokerConfig::default();
            file.apply_to(&mut cfg).unwrap();
            assert!(
                cfg.delegation_token_default_renew_period == hours(24),
                "absent default_renew_period_ms should leave the 24h default in place"
            );

            // (2) When the TOML sets it, the override wins.
            let toml = r#"
[delegation_token]
secret_key = "abcdef"
default_renew_period_ms = 7200000
"#;
            let file: FileConfig = toml::from_str(toml).unwrap();
            let mut cfg = crate::config::BrokerConfig::default();
            file.apply_to(&mut cfg).unwrap();
            assert!(
                cfg.delegation_token_default_renew_period == hours(2),
                "TOML default_renew_period_ms must override the default"
            );
        });
    }

    #[test]
    fn delegation_token_runtime_key_overrides_toml() {
        let toml = r#"
[delegation_token]
secret_key = "toml-loses"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig {
            delegation_token_secret_key: Some(crabka_security::SecretBytes::new(
                b"runtime-wins".to_vec(),
            )),
            ..crate::config::BrokerConfig::default()
        };
        file.apply_to(&mut cfg).unwrap();

        assert!(
            cfg.delegation_token_secret_key
                .as_ref()
                .map(|s| s.as_bytes().to_vec())
                == Some(b"runtime-wins".to_vec())
        );
    }

    #[test]
    fn delegation_token_absent_when_unset_anywhere() {
        let _g = env_lock().lock().unwrap();
        temp_env::with_var_unset("CRABKA_DELEGATION_TOKEN_SECRET_KEY", || {
            let file: FileConfig = toml::from_str("").unwrap();
            let mut cfg = crate::config::BrokerConfig::default();
            file.apply_to(&mut cfg).unwrap();

            // No secret key anywhere; lifetime knobs stay at their defaults
            // when no section is present.
            check!(cfg.delegation_token_secret_key.is_none());
            check!(cfg.delegation_token_max_lifetime == days(7));
            check!(cfg.delegation_token_expiry_check_interval == hours(1));
            check!(cfg.delegation_token_default_renew_period == hours(24));
        });
    }

    #[test]
    fn super_users_toml_populates_broker_config_set() {
        let toml = r#"
super_users = ["ANONYMOUS", "admin"]
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        let expected: std::collections::HashSet<String> =
            ["ANONYMOUS".to_string(), "admin".to_string()].into();
        assert!(cfg.super_users == expected);
    }

    // `[authorization]` TOML section → `Arc<dyn Authorizer>`.

    fn test_principal(name: &str) -> crabka_security::Principal {
        crabka_security::Principal {
            name: name.into(),
            auth_method: crabka_security::AuthMethod::SaslPlain,
            groups: vec![],
        }
    }

    #[test]
    fn authorization_section_simple_builds_simple_acl_authorizer() {
        use crabka_metadata::{AclOperation, MetadataImage, ResourceType};

        use crate::authorizer::{AuthorizationRequest, AuthorizationResult};

        let toml = r#"
[authorization]
type = "simple"
super_users = ["admin"]
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        assert!(
            cfg.super_users.contains("admin"),
            "[authorization].super_users must populate BrokerConfig.super_users for act-as parity"
        );
        // `admin` is a super-user → bypass returns Allow even with an
        // empty MetadataImage (no ACLs). This is the SimpleAclAuthorizer
        // contract; AllowAllAuthorizer would also Allow, but the
        // default-deny SimpleAcl behavior is exercised by the
        // explicit `type = "simple"` branch's own unit tests.
        let img = MetadataImage::new(uuid::Uuid::nil());
        let admin = test_principal("admin");
        let host: std::net::SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let req = AuthorizationRequest {
            principal: &admin,
            host: &host,
            resource_type: ResourceType::Topic,
            resource_name: "t",
            operation: AclOperation::Read,
        };
        assert!(cfg.authorizer.authorize(&img, &req) == AuthorizationResult::Allow);

        // Non-super-user with no matching ACL → Deny (proves we got
        // SimpleAcl, not AllowAll).
        let alice = test_principal("alice");
        let req_alice = AuthorizationRequest {
            principal: &alice,
            host: &host,
            resource_type: ResourceType::Topic,
            resource_name: "t",
            operation: AclOperation::Read,
        };
        assert!(
            cfg.authorizer.authorize(&img, &req_alice) == AuthorizationResult::Deny,
            "type=simple must default-deny non-super-users with no matching ACL"
        );
    }

    #[test]
    fn authorization_section_opa_builds_opa_authorizer() {
        use crabka_metadata::{AclOperation, MetadataImage, ResourceType};

        use crate::authorizer::{AuthorizationRequest, AuthorizationResult};

        // `OpaAuthorizer::new` captures `Handle::try_current()` — needs
        // an active tokio runtime. `Runtime::new()` defaults to
        // multi-thread, which the OPA `block_in_place` bridge requires
        // for any actual HTTP call (super-user bypass below sidesteps
        // that).
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let toml = r#"
[authorization]
type = "opa"
super_users = ["ANONYMOUS"]

[authorization.opa]
url = "http://opa.invalid:8181/v1/data/k/a"
allow_on_error = false
maximum_cache_size = 100
expire_after_ms = 60000
"#;
            let file: FileConfig = toml::from_str(toml).unwrap();
            let mut cfg = crate::config::BrokerConfig::default();
            file.apply_to(&mut cfg).unwrap();

            assert!(cfg.super_users.contains("ANONYMOUS"));

            // Smoke-check via the super-user bypass — no HTTP call is
            // made (and `opa.invalid` deliberately doesn't resolve).
            let img = MetadataImage::new(uuid::Uuid::nil());
            let anon = test_principal("ANONYMOUS");
            let host: std::net::SocketAddr = "127.0.0.1:9092".parse().unwrap();
            let req = AuthorizationRequest {
                principal: &anon,
                host: &host,
                resource_type: ResourceType::Topic,
                resource_name: "t",
                operation: AclOperation::Read,
            };
            assert!(
                cfg.authorizer.authorize(&img, &req) == AuthorizationResult::Allow,
                "OPA super-user bypass must short-circuit before any HTTP call"
            );
        });
    }

    #[test]
    fn opa_allow_on_error_defaults_to_fail_closed_when_omitted() {
        // L-6: omitting `allow_on_error` must default to fail-closed
        // (false), matching the upstream OPA Kafka plugin.
        let toml = r#"
url = "http://opa.invalid:8181/v1/data/k/a"
maximum_cache_size = 100
expire_after_ms = 60000
"#;
        let opa: FileOpaConfig = toml::from_str(toml).unwrap();
        assert!(!opa.allow_on_error, "allow_on_error must default to false");

        // And the built authorizer must Deny on OPA error (fail-closed).
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            use crabka_metadata::{AclOperation, MetadataImage, ResourceType};

            use crate::authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer};

            let auth = crate::authorizer::opa::OpaAuthorizer::new(
                std::collections::HashSet::new(),
                // Unresolvable host → every call errors.
                "http://opa.invalid:8181/v1/data/k/a".to_string(),
                opa.allow_on_error,
                opa.maximum_cache_size,
                Time::from_millis(opa.expire_after_ms),
                crate::config::BrokerConfig::default().opa_http_timeout,
            )
            .unwrap();
            let img = MetadataImage::new(uuid::Uuid::nil());
            let p = test_principal("alice");
            let host: std::net::SocketAddr = "127.0.0.1:9092".parse().unwrap();
            let req = AuthorizationRequest {
                principal: &p,
                host: &host,
                resource_type: ResourceType::Topic,
                resource_name: "t",
                operation: AclOperation::Read,
            };
            assert!(
                auth.authorize(&img, &req) == AuthorizationResult::Deny,
                "OPA outage with default allow_on_error must fail closed (Deny)"
            );
        });
    }

    #[test]
    fn opa_cache_defaults_match_documented_capacity_and_ttl() {
        let toml = r#"
url = "http://opa.invalid:8181/v1/data/k/a"
"#;
        let opa: FileOpaConfig = toml::from_str(toml).unwrap();

        assert!(opa.maximum_cache_size == 50_000);
        assert!(opa.expire_after_ms == 3_600_000);
    }

    #[test]
    fn authorization_section_absent_defaults_to_allow_all() {
        use crabka_metadata::{AclOperation, MetadataImage, ResourceType};

        use crate::authorizer::{AuthorizationRequest, AuthorizationResult};

        let file: FileConfig = toml::from_str("").unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        // Default authorizer is AllowAll — anyone gets Allow, including
        // a principal who isn't in any super-user set.
        let img = MetadataImage::new(uuid::Uuid::nil());
        let anyone = test_principal("anyone");
        let host: std::net::SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let req = AuthorizationRequest {
            principal: &anyone,
            host: &host,
            resource_type: ResourceType::Topic,
            resource_name: "t",
            operation: AclOperation::Read,
        };
        assert!(cfg.authorizer.authorize(&img, &req) == AuthorizationResult::Allow);
    }

    #[test]
    fn process_roles_controller_only_from_toml() {
        let toml = r#"
            [process]
            roles = ["controller"]
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        fc.apply_to(&mut cfg).expect("apply");
        assert!(cfg.roles == vec![crate::config::NodeRole::Controller]);
    }

    #[test]
    fn process_roles_both_from_toml() {
        let toml = r#"
            [process]
            roles = ["broker", "controller"]
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        fc.apply_to(&mut cfg).expect("apply");
        assert!(
            cfg.roles
                == vec![
                    crate::config::NodeRole::Broker,
                    crate::config::NodeRole::Controller
                ]
        );
    }

    #[test]
    fn process_roles_rejects_unknown_role() {
        let toml = r#"
            [process]
            roles = ["wizard"]
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        let err = fc.apply_to(&mut cfg).expect_err("unknown role rejected");
        assert!(matches!(err, FileConfigError::InvalidConfig(_)));
    }

    #[test]
    fn process_section_absent_leaves_default_roles() {
        let fc: FileConfig = toml::from_str("").expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        fc.apply_to(&mut cfg).expect("apply");
        assert!(
            cfg.roles
                == vec![
                    crate::config::NodeRole::Controller,
                    crate::config::NodeRole::Broker
                ]
        );
    }

    #[test]
    fn apply_to_parses_rack_and_replica_selector() {
        use crate::replica_selector::ReplicaSelectorKind;
        let src = r#"
broker_id = 0
rack = "az-1"
replica_selector = "rack-aware"
"#;
        let cfg: FileConfig = toml::from_str(src).expect("parse");
        let mut broker = crate::config::BrokerConfig::default();
        cfg.apply_to(&mut broker).expect("apply");
        assert!(broker.rack.as_deref() == Some("az-1"));
        assert!(broker.replica_selector == ReplicaSelectorKind::RackAware);
    }

    #[test]
    fn apply_to_rejects_unknown_replica_selector() {
        let src = r#"
broker_id = 0
replica_selector = "nonsense"
"#;
        let cfg: FileConfig = toml::from_str(src).expect("parse");
        let mut broker = crate::config::BrokerConfig::default();
        assert!(cfg.apply_to(&mut broker).is_err());
    }

    #[test]
    fn apply_to_gssapi_maps_all_fields() {
        let src = r#"
broker_id = 1
[gssapi]
keytab_path = "/etc/crabka/gssapi-keytab/keytab"
service_name = "kafka"
principal_to_local_rules = ["RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//", "DEFAULT"]
realm = "EXAMPLE.COM"
kdc = "tcp://kdc:88"
"#;
        let file: FileConfig = toml::from_str(src).expect("parse [gssapi]");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).expect("apply [gssapi]");
        let g = cfg.gssapi.expect("gssapi config present");
        check!(g.keytab_path == std::path::PathBuf::from("/etc/crabka/gssapi-keytab/keytab"));
        check!(g.service_name.as_str() == "kafka");
        check!(g.principal_to_local_rules.len() == 2);
        // Second rule in the fixture is the bare DEFAULT rule.
        check!(matches!(
            g.principal_to_local_rules[1],
            crabka_security::gssapi::name::Rule::Default
        ));
        check!(g.realm.as_deref() == Some("EXAMPLE.COM"));
        check!(g.kdc.as_deref() == Some("tcp://kdc:88"));
    }

    #[test]
    fn apply_to_gssapi_defaults_service_name_to_kafka() {
        let src = r#"
[gssapi]
keytab_path = "/k/keytab"
principal_to_local_rules = ["DEFAULT"]
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(cfg.gssapi.unwrap().service_name == "kafka");
    }

    #[test]
    fn apply_to_gssapi_rejects_malformed_rule() {
        let src = r#"
[gssapi]
keytab_path = "/k/keytab"
principal_to_local_rules = ["NOT_A_RULE:::"]
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        let err = file.apply_to(&mut cfg).unwrap_err();
        assert!(matches!(err, FileConfigError::InvalidConfig(_)));
    }

    #[test]
    fn apply_to_inter_broker_credentials_gssapi() {
        let src = r#"
[inter_broker_credentials]
type = "gssapi"
keytab_path = "/etc/crabka/gssapi-keytab/keytab"
client_principal = "kafka@EXAMPLE.COM"
service_name = "kafka"
kdc_url = "tcp://kdc:88"
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        let expected = crate::config::InterBrokerCredentials::Gssapi {
            keytab_path: std::path::PathBuf::from("/etc/crabka/gssapi-keytab/keytab"),
            client_principal: "kafka@EXAMPLE.COM".to_string(),
            service_name: "kafka".to_string(),
            kdc_url: "tcp://kdc:88".to_string(),
        };
        assert!(cfg.inter_broker_credentials == Some(expected));
    }

    #[test]
    fn apply_to_inter_broker_credentials_rejects_unknown_type() {
        // Unknown `type` variants are rejected at TOML parse time because
        // `FileInterBrokerCredentials` is a tagged enum with `deny_unknown_fields`.
        let src = r#"
[inter_broker_credentials]
type = "carrier-pigeon"
"#;
        assert!(toml::from_str::<FileConfig>(src).is_err());
    }

    #[test]
    fn apply_to_inter_broker_credentials_defaults_service_name_to_kafka() {
        let src = r#"
[inter_broker_credentials]
type = "gssapi"
keytab_path = "/k/keytab"
client_principal = "kafka@EXAMPLE.COM"
kdc_url = "tcp://kdc:88"
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        match cfg.inter_broker_credentials.unwrap() {
            crate::config::InterBrokerCredentials::Gssapi { service_name, .. } => {
                assert!(service_name == "kafka");
            }
            other => panic!("expected Gssapi, got {other:?}"),
        }
    }

    #[test]
    fn file_config_schema_generates() {
        let schema = schemars::schema_for!(FileConfig);
        let value = serde_json::to_value(&schema).expect("schema serializes");
        assert!(
            value.get("properties").is_some(),
            "FileConfig schema has properties"
        );
    }

    #[test]
    fn kafka_metadata_in_memory_true_opts_out_to_in_memory_rlmm() {
        let toml = r#"
[remote_storage]
storage_dir = "/tmp/tier"

[remote_storage.kafka_metadata]
in_memory = true
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(
            matches!(cfg.remote_log_metadata, crate::config::RlmmKind::InMemory),
            "in_memory = true must opt out to RlmmKind::InMemory, got {:?}",
            cfg.remote_log_metadata
        );
    }

    #[test]
    fn audit_section_parses_and_applies() {
        let toml = r#"
            [audit]
            enabled = true
            topic = "__crabka_audit"
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse audit section");
        let audit = fc.audit.clone().expect("audit present");
        assert2::check!(audit.enabled);
        assert2::check!(audit.topic == "__crabka_audit");

        let mut cfg = crate::config::BrokerConfig::for_tests(std::path::PathBuf::from("/tmp/x"));
        fc.apply_to(&mut cfg).expect("apply");
        assert2::check!(cfg.audit_enabled);
        assert2::check!(cfg.audit_topic == "__crabka_audit");
    }

    #[test]
    fn audit_defaults_to_enabled_with_internal_topic() {
        // Absent [audit] section → secure default (enabled, standard topic name).
        let fc: FileConfig = toml::from_str("").expect("parse empty");
        let mut cfg = crate::config::BrokerConfig::for_tests(std::path::PathBuf::from("/tmp/x"));
        fc.apply_to(&mut cfg).expect("apply");
        assert2::check!(cfg.audit_enabled);
        assert2::check!(cfg.audit_topic == "__crabka_audit");
    }

    #[test]
    fn audit_signing_and_checkpoint_parse_and_apply() {
        let toml = r#"
            [audit]
            enabled = true

            [audit.signing]
            key_path = "/etc/crabka/audit.pk8"
            key_id = "audit-2026"

            [audit.checkpoint]
            every_n = 500
            every_secs = 30
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = crate::config::BrokerConfig::for_tests(std::path::PathBuf::from("/tmp/x"));
        fc.apply_to(&mut cfg).expect("apply");
        assert2::check!(
            cfg.audit_signing_key_path == Some(std::path::PathBuf::from("/etc/crabka/audit.pk8"))
        );
        assert2::check!(cfg.audit_signing_key_id.as_deref() == Some("audit-2026"));
        assert2::check!(cfg.audit_checkpoint_every_n == 500);
        assert2::check!(cfg.audit_checkpoint_every == secs(30));
    }

    #[test]
    fn audit_checkpoint_has_sane_defaults_when_absent() {
        let fc: FileConfig = toml::from_str("[audit]\nenabled = true\n").expect("parse");
        let mut cfg = crate::config::BrokerConfig::for_tests(std::path::PathBuf::from("/tmp/x"));
        fc.apply_to(&mut cfg).expect("apply");
        assert2::check!(cfg.audit_signing_key_path == None);
        assert2::check!(cfg.audit_signing_key_id == None);
        assert2::check!(cfg.audit_checkpoint_every_n == 1000);
        assert2::check!(cfg.audit_checkpoint_every == secs(60));
    }

    #[test]
    fn audit_spool_parses_and_defaults() {
        let toml = r#"
            [audit]
            enabled = true
            [audit.spool]
            dir = "/var/lib/crabka/audit-spool"
            max_bytes = 2048
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = crate::config::BrokerConfig::for_tests(std::path::PathBuf::from("/tmp/x"));
        fc.apply_to(&mut cfg).expect("apply");
        assert2::check!(
            cfg.audit_spool_dir == std::path::PathBuf::from("/var/lib/crabka/audit-spool")
        );
        assert2::check!(cfg.audit_spool_max == crabka_units::kibibytes(2));

        let fc2: FileConfig = toml::from_str("[audit]\nenabled = true\n").expect("parse");
        let mut cfg2 = crate::config::BrokerConfig::for_tests(std::path::PathBuf::from("/tmp/x"));
        fc2.apply_to(&mut cfg2).expect("apply");
        assert2::check!(cfg2.audit_spool_dir == std::path::PathBuf::from("audit-spool"));
        assert2::check!(cfg2.audit_spool_max == crabka_units::gibibytes(1));
    }

    #[test]
    fn runtime_file_config_applies_representative_values() {
        let file: FileConfig = toml::from_str(
            r"
[runtime]
cleaner_interval_ms = 7000
isr_scan_interval_ms = 800
opa_http_timeout_ms = 2500
replication_fetch_max_bytes = 2097152
replication_fetch_max_wait_ms = 750
replication_fetch_min_bytes = 2
",
        )
        .expect("parse runtime config");
        let mut cfg = crate::config::BrokerConfig::default();

        file.apply_to(&mut cfg).expect("apply runtime config");

        assert!(
            (
                cfg.cleaner_interval,
                cfg.isr_scan_interval,
                cfg.opa_http_timeout,
                cfg.replication.fetch_max_bytes,
                cfg.replication.fetch_max_wait_ms,
                cfg.replication.fetch_min_bytes,
            ) == (secs(7), millis(800), millis(2_500), 2_097_152, 750, 2)
        );
    }

    /// Every `*_ms` / `*_bytes` runtime key must survive the round trip
    /// TOML integer → quantity → wire integer unchanged. This is the
    /// regression the `crabka-units` adoption exists to prevent: a mapping
    /// that reads `30000` as 30 000 *seconds*, or writes a 30 s timeout back
    /// as `30`, changes a Kafka wire field by three orders of magnitude.
    #[test]
    fn runtime_millisecond_and_byte_keys_round_trip_through_quantities() {
        let file: FileConfig = toml::from_str(
            r"
[runtime]
heartbeat_interval_ms = 3000
heartbeat_timeout_ms = 9000
replica_lag_time_max_ms = 30000
transaction_min_timeout_ms = 1000
transaction_max_timeout_ms = 900000
producer_id_expiration_ms = 86400000
client_metrics_default_interval_ms = 300000
delegation_token_max_lifetime_ms = 604800000
socket_request_max_bytes = 104857600
client_metrics_telemetry_max_bytes = 1048576
observer_fetch_max_bytes = 1048576
",
        )
        .expect("parse runtime config");
        let mut cfg = crate::config::BrokerConfig::default();

        file.apply_to(&mut cfg).expect("apply runtime config");

        // Landed as dimensioned quantities, spelled in their natural units.
        assert!(cfg.heartbeat_interval == secs(3));
        assert!(cfg.heartbeat_timeout == secs(9));
        assert!(cfg.replica_lag_time_max == secs(30));
        assert!(cfg.transaction_min_timeout == secs(1));
        assert!(cfg.transaction_max_timeout == minutes(15));
        assert!(cfg.producer_id_expiration == hours(24));
        assert!(cfg.client_metrics_default_interval == minutes(5));
        assert!(cfg.delegation_token_max_lifetime == days(7));
        assert!(cfg.socket_request_max == mebibytes(100));
        assert!(cfg.client_metrics_telemetry_max == mebibytes(1));
        assert!(cfg.observer_fetch_max == mebibytes(1));

        // …and leave for the wire exactly the integers that came in.
        let millis: [(&str, i64); 8] = [
            ("heartbeat_interval_ms", cfg.heartbeat_interval.millis_i64()),
            ("heartbeat_timeout_ms", cfg.heartbeat_timeout.millis_i64()),
            (
                "replica_lag_time_max_ms",
                cfg.replica_lag_time_max.millis_i64(),
            ),
            (
                "transaction_min_timeout_ms",
                i64::from(cfg.transaction_min_timeout.millis_i32()),
            ),
            (
                "transaction_max_timeout_ms",
                i64::from(cfg.transaction_max_timeout.millis_i32()),
            ),
            (
                "producer_id_expiration_ms",
                cfg.producer_id_expiration.millis_i64(),
            ),
            (
                "client_metrics_default_interval_ms",
                i64::from(cfg.client_metrics_default_interval.millis_i32()),
            ),
            (
                "delegation_token_max_lifetime_ms",
                cfg.delegation_token_max_lifetime.millis_i64(),
            ),
        ];
        assert!(
            millis
                == [
                    ("heartbeat_interval_ms", 3_000),
                    ("heartbeat_timeout_ms", 9_000),
                    ("replica_lag_time_max_ms", 30_000),
                    ("transaction_min_timeout_ms", 1_000),
                    ("transaction_max_timeout_ms", 900_000),
                    ("producer_id_expiration_ms", 86_400_000),
                    ("client_metrics_default_interval_ms", 300_000),
                    ("delegation_token_max_lifetime_ms", 604_800_000),
                ]
        );
        let sizes: [(&str, i64); 3] = [
            (
                "socket_request_max_bytes",
                cfg.socket_request_max.bytes_i64(),
            ),
            (
                "client_metrics_telemetry_max_bytes",
                i64::from(cfg.client_metrics_telemetry_max.bytes_i32()),
            ),
            (
                "observer_fetch_max_bytes",
                cfg.observer_fetch_max.bytes_i64(),
            ),
        ];
        assert!(
            sizes
                == [
                    ("socket_request_max_bytes", 104_857_600),
                    ("client_metrics_telemetry_max_bytes", 1_048_576),
                    ("observer_fetch_max_bytes", 1_048_576),
                ]
        );
    }

    /// Kafka's `leader.imbalance.per.broker.percentage` is an integer
    /// percentage on the operator surface and a [`Ratio`] in the domain.
    #[test]
    fn leader_imbalance_percentage_lands_as_a_ratio() {
        for (raw, want) in [(0_u32, 0.0), (10, 0.10), (55, 0.55), (100, 1.0)] {
            let file: FileConfig = toml::from_str(&format!(
                "[runtime]\nleader_imbalance_per_broker_percentage = {raw}\n"
            ))
            .expect("parse runtime config");
            let mut cfg = crate::config::BrokerConfig::default();

            file.apply_to(&mut cfg).expect("apply runtime config");

            assert!(
                (cfg.leader_imbalance_per_broker.as_f64() - want).abs() < 1e-12,
                "{raw}% should be {want}"
            );
        }
    }

    #[test]
    fn runtime_file_config_rejects_zero_and_names_field() {
        let file: FileConfig =
            toml::from_str("[runtime]\ncleaner_interval_ms = 0\n").expect("parse runtime config");
        let mut cfg = crate::config::BrokerConfig::default();

        let error = file
            .apply_to(&mut cfg)
            .expect_err("zero cleaner interval must fail");

        assert!(error.to_string().contains("cleaner_interval_ms"));
    }

    #[test]
    fn runtime_file_config_rejects_voter_timeout_above_wire_limit() {
        let file: FileConfig =
            toml::from_str("[runtime]\nauto_join_voter_request_timeout_ms = 2147483648\n")
                .expect("parse runtime config");
        let mut cfg = crate::config::BrokerConfig::default();

        let error = file
            .apply_to(&mut cfg)
            .expect_err("timeout above i32 wire limit must fail");

        assert!(
            error
                .to_string()
                .contains("auto_join_voter_request_timeout_ms")
        );
    }

    #[test]
    fn existing_file_inputs_reject_invalid_refined_values() {
        let cases = [
            ("heartbeat_interval_ms = 0\n", "heartbeat_interval_ms"),
            (
                "[runtime]\nstreams_internal_topic_replication_factor = 0\n",
                "streams_internal_topic_replication_factor",
            ),
            (
                "[delegation_token]\nmax_lifetime_ms = 0\n",
                "max_lifetime_ms",
            ),
        ];

        for (source, field) in cases {
            let file: FileConfig = toml::from_str(source).expect("parse config");
            let mut cfg = crate::config::BrokerConfig::default();
            let error = file.apply_to(&mut cfg).expect_err("zero must fail");
            assert!(error.to_string().contains(field));
        }
    }

    #[test]
    fn runtime_file_config_rejects_relational_conflicts() {
        let cases = [
            (
                "[runtime]\nreplication_fetch_min_bytes = 3\nreplication_fetch_max_bytes = 2\n",
                "replication fetch minimum",
            ),
            (
                "[runtime]\ntransaction_min_timeout_ms = 2000\ntransaction_max_timeout_ms = 1000\n",
                "transaction minimum timeout",
            ),
        ];

        for (source, message) in cases {
            let file: FileConfig = toml::from_str(source).expect("parse runtime config");
            let mut cfg = crate::config::BrokerConfig::default();
            let error = file
                .apply_to(&mut cfg)
                .expect_err("relational conflict must fail");
            assert!(error.to_string().contains(message));
        }
    }
}
