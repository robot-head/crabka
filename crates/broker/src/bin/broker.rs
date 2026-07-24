//! `crabka-broker` — single-node Kafka-compatible broker daemon.

// Heap profiling: install jemalloc as the global allocator and enable the
// prof:true malloc_conf so jemalloc_pprof can dump heap profiles at runtime.
// Gated on both `unix` (tikv-jemallocator is Unix-only) and the
// `heap-profiling` feature so `cargo build` on Windows compiles cleanly.
#[cfg(all(unix, feature = "heap-profiling"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

use clap::Parser;
use crabka_broker::{
    BootstrapMode, Broker, BrokerConfig,
    config::{
        DEFAULT_CONTROLLED_SHUTDOWN_DRAIN_TIMEOUT_MS, DEFAULT_LEADER_IMBALANCE_CHECK_INTERVAL_SECS,
        DEFAULT_MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS,
        DEFAULT_METADATA_MAX_BYTES_BETWEEN_SNAPSHOTS, DEFAULT_METADATA_MAX_SNAPSHOT_INTERVAL_MS,
        DEFAULT_METADATA_SNAPSHOT_INTERVAL_RECORDS, DEFAULT_OBSERVER_LAG_BOUND,
        DEFAULT_TLS_RELOAD_INTERVAL_MS, DEFAULT_TXN_ABORT_CLEANUP_INTERVAL_MS,
    },
    config_value::{
        Percentage, PositiveCount, PositiveI32, PositiveI64, PositiveMillis, parse_percentage,
        parse_positive_count, parse_positive_i32, parse_positive_i64, parse_positive_millis,
    },
};
use crabka_log::LogConfig;

/// Parse `--process-roles` string values into `NodeRole`s.
fn parse_roles_arg(roles: &[String]) -> Result<Vec<crabka_broker::config::NodeRole>, String> {
    use crabka_broker::config::NodeRole;
    roles
        .iter()
        .map(|r| match r.to_ascii_lowercase().as_str() {
            "controller" => Ok(NodeRole::Controller),
            "broker" => Ok(NodeRole::Broker),
            other => Err(format!(
                "unknown --process-roles value `{other}` (expected `controller` or `broker`)"
            )),
        })
        .collect()
}

fn parse_share_isolation(
    value: &str,
) -> Result<crabka_broker::coordinator::unified::share::config::ShareIsolationLevel, String> {
    use crabka_broker::coordinator::unified::share::config::ShareIsolationLevel;
    match value {
        "read-uncommitted" => Ok(ShareIsolationLevel::ReadUncommitted),
        "read-committed" => Ok(ShareIsolationLevel::ReadCommitted),
        _ => Err("expected `read-uncommitted` or `read-committed`".into()),
    }
}

fn parse_streams_assignor(
    value: &str,
) -> Result<crabka_broker::coordinator::unified::streams::config::StreamsAssignorKind, String> {
    use crabka_broker::coordinator::unified::streams::config::StreamsAssignorKind;
    match value {
        "auto" => Ok(StreamsAssignorKind::Auto),
        "sticky" => Ok(StreamsAssignorKind::Sticky),
        "highly-available" => Ok(StreamsAssignorKind::HighlyAvailable),
        _ => Err("expected `auto`, `sticky`, or `highly-available`".into()),
    }
}

#[derive(Debug, clap::Args)]
struct RuntimeArgs {
    #[arg(long, env = "CRABKA_STARTUP_LEADER_WAIT_TIMEOUT_MS", default_value = "120000", value_parser = parse_positive_millis)]
    startup_leader_wait_timeout_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_SELF_REGISTRATION_BACKOFF_MIN_MS", default_value = "100", value_parser = parse_positive_millis)]
    self_registration_backoff_min_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_SELF_REGISTRATION_BACKOFF_MAX_MS", default_value = "5000", value_parser = parse_positive_millis)]
    self_registration_backoff_max_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_OBSERVER_POLL_INTERVAL_MS", default_value = "100", value_parser = parse_positive_millis)]
    observer_poll_interval_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_AUDIT_SPOOL_REPLAY_INTERVAL_MS", default_value = "2000", value_parser = parse_positive_millis)]
    audit_spool_replay_interval_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_AUDIT_STATS_POLL_INTERVAL_MS", default_value = "1000", value_parser = parse_positive_millis)]
    audit_stats_poll_interval_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_AUDIT_PARTITION_WAIT_TIMEOUT_MS", default_value = "10000", value_parser = parse_positive_millis)]
    audit_partition_wait_timeout_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_LIVENESS_TICK_INTERVAL_MS", default_value = "1000", value_parser = parse_positive_millis)]
    liveness_tick_interval_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_GAUGE_POLL_INTERVAL_MS", default_value = "1000", value_parser = parse_positive_millis)]
    gauge_poll_interval_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_ISR_SCAN_INTERVAL_MS", default_value = "1000", value_parser = parse_positive_millis)]
    isr_scan_interval_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_CLEANER_INTERVAL_MS", default_value = "30000", value_parser = parse_positive_millis)]
    cleaner_interval_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_FUTURE_LOG_MOVE_RETRY_BACKOFF_MS", default_value = "50", value_parser = parse_positive_millis)]
    future_log_move_retry_backoff_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_CLIENT_METRICS_EVICTION_TICK_MS", default_value = "60000", value_parser = parse_positive_millis)]
    client_metrics_eviction_tick_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_CLIENT_METRICS_STALE_FLOOR_MS", default_value = "600000", value_parser = parse_positive_millis)]
    client_metrics_stale_floor_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_CLIENT_METRICS_DEFAULT_INTERVAL_MS", default_value = "300000", value_parser = parse_positive_i32)]
    client_metrics_default_interval_ms: PositiveI32,
    #[arg(long, env = "CRABKA_CLIENT_METRICS_TELEMETRY_MAX_BYTES", default_value = "1048576", value_parser = parse_positive_i32)]
    client_metrics_telemetry_max_bytes: PositiveI32,
    #[arg(long, env = "CRABKA_CLIENT_METRICS_PROM_SNAPSHOT_TTL_MS", default_value = "300000", value_parser = parse_positive_millis)]
    client_metrics_prom_snapshot_ttl_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_RLMM_RECONCILE_TICK_MS", default_value = "30000", value_parser = parse_positive_millis)]
    rlmm_reconcile_tick_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_RLMM_BOOTSTRAP_BACKOFF_INITIAL_MS", default_value = "250", value_parser = parse_positive_millis)]
    rlmm_bootstrap_backoff_initial_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_RLMM_BOOTSTRAP_BACKOFF_MAX_MS", default_value = "10000", value_parser = parse_positive_millis)]
    rlmm_bootstrap_backoff_max_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_CONNECTION_CREATION_THROTTLE_MAX_MS", default_value = "1000", value_parser = parse_positive_millis)]
    connection_creation_throttle_max_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_OPA_HTTP_TIMEOUT_MS", default_value = "5000", value_parser = parse_positive_millis)]
    opa_http_timeout_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_OAUTH_JWKS_HTTP_TIMEOUT_MS", default_value = "10000", value_parser = parse_positive_millis)]
    oauth_jwks_http_timeout_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_AUTO_JOIN_RETRY_BACKOFF_MS", default_value = "500", value_parser = parse_positive_millis)]
    auto_join_retry_backoff_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_REPLICATION_FETCH_MAX_BYTES", default_value = "1048576", value_parser = parse_positive_i32)]
    replication_fetch_max_bytes: PositiveI32,
    #[arg(long, env = "CRABKA_REPLICATION_FETCH_MAX_WAIT_MS", default_value = "500", value_parser = parse_positive_i32)]
    replication_fetch_max_wait_ms: PositiveI32,
    #[arg(long, env = "CRABKA_REPLICATION_FETCH_MIN_BYTES", default_value = "1", value_parser = parse_positive_i32)]
    replication_fetch_min_bytes: PositiveI32,
    #[arg(long, env = "CRABKA_REPLICATION_THROTTLE_EXHAUSTED_BACKOFF_MS", default_value = "100", value_parser = parse_positive_millis)]
    replication_throttle_exhausted_backoff_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_REPLICATION_SEND_ERROR_BACKOFF_MS", default_value = "1000", value_parser = parse_positive_millis)]
    replication_send_error_backoff_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_REPLICATION_UNKNOWN_TOPIC_RETRY_DELAY_MS", default_value = "100", value_parser = parse_positive_millis)]
    replication_unknown_topic_retry_delay_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_REPLICATION_EPOCH_FENCE_BACKOFF_MS", default_value = "200", value_parser = parse_positive_millis)]
    replication_epoch_fence_backoff_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_REPLICATION_UNEXPECTED_ERROR_BACKOFF_MS", default_value = "500", value_parser = parse_positive_millis)]
    replication_unexpected_error_backoff_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_REPLICATION_RECONNECT_INITIAL_DELAY_MS", default_value = "100", value_parser = parse_positive_millis)]
    replication_reconnect_initial_delay_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_REPLICATION_RECONNECT_DELAY_CAP_MS", default_value = "5000", value_parser = parse_positive_millis)]
    replication_reconnect_delay_cap_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_COORDINATOR_SESSION_EXPIRY_TICK_MS", default_value = "1000", value_parser = parse_positive_millis)]
    coordinator_session_expiry_tick_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_COORDINATOR_SHUTDOWN_ACK_TIMEOUT_MS", default_value = "5000", value_parser = parse_positive_millis)]
    coordinator_shutdown_ack_timeout_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_CONSUMER_GROUP_SESSION_TIMEOUT_MS", default_value = "45000", value_parser = parse_positive_millis)]
    consumer_group_session_timeout_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS", default_value = "5000", value_parser = parse_positive_millis)]
    consumer_group_heartbeat_interval_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS", default_value = "45000", value_parser = parse_positive_millis)]
    consumer_group_min_session_timeout_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS", default_value = "60000", value_parser = parse_positive_millis)]
    consumer_group_max_session_timeout_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS", default_value = "5000", value_parser = parse_positive_millis)]
    consumer_group_min_heartbeat_interval_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS", default_value = "15000", value_parser = parse_positive_millis)]
    consumer_group_max_heartbeat_interval_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_CONSUMER_GROUP_MAX_SIZE", default_value = "200", value_parser = parse_positive_count)]
    consumer_group_max_size: PositiveCount,
    #[arg(long, env = "CRABKA_CLASSIC_GROUP_INITIAL_REBALANCE_DELAY_MS", default_value = "3000", value_parser = parse_positive_millis)]
    classic_group_initial_rebalance_delay_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_SYNC_GROUP_FOLLOWER_WAIT_MS", default_value = "30000", value_parser = parse_positive_millis)]
    sync_group_follower_wait_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_UNCLEAN_RECOVERY_AGGRESSIVE_DEADLINE_MS", default_value = "2000", value_parser = parse_positive_millis)]
    unclean_recovery_aggressive_deadline_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_UNCLEAN_RECOVERY_BALANCED_DEADLINE_MS", default_value = "30000", value_parser = parse_positive_millis)]
    unclean_recovery_balanced_deadline_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_OPERATOR_RECOVERY_DEADLINE_MS", default_value = "25000", value_parser = parse_positive_millis)]
    operator_recovery_deadline_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_QUOTA_THROTTLE_MAX_MS", default_value = "1000", value_parser = parse_positive_millis)]
    quota_throttle_max_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_SELF_REGISTRATION_MAX_ATTEMPTS", default_value_t = 8, value_parser = clap::value_parser!(u32).range(1..))]
    self_registration_max_attempts: u32,
    #[arg(long, env = "CRABKA_OBSERVER_FETCH_MAX_BYTES", default_value_t = 1_048_576, value_parser = clap::value_parser!(u32).range(1..))]
    observer_fetch_max_bytes: u32,
    #[arg(long, env = "CRABKA_AUDIT_EVENT_QUEUE_CAPACITY", default_value = "8192", value_parser = parse_positive_count)]
    audit_event_queue_capacity: PositiveCount,
    #[arg(long, env = "CRABKA_AUDIT_TAIL_WINDOW_OFFSETS", default_value = "4096", value_parser = parse_positive_i64)]
    audit_tail_window_offsets: PositiveI64,
    #[arg(long, env = "CRABKA_AUDIT_TAIL_READ_MAX_BYTES", default_value = "1048576", value_parser = parse_positive_count)]
    audit_tail_read_max_bytes: PositiveCount,
    #[arg(long, env = "CRABKA_OFFSETS_TOPIC_METADATA_WAIT_TIMEOUT_MS", default_value = "30000", value_parser = parse_positive_millis)]
    offsets_topic_metadata_wait_timeout_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_CLIENT_METRICS_STALE_PUSH_INTERVALS", default_value_t = 3, value_parser = clap::value_parser!(u32).range(1..))]
    client_metrics_stale_push_intervals: u32,
    #[arg(long, env = "CRABKA_CLIENT_METRICS_OTLP_QUEUE_CAPACITY", default_value = "256", value_parser = parse_positive_count)]
    client_metrics_otlp_queue_capacity: PositiveCount,
    #[arg(long, env = "CRABKA_COORDINATOR_ACTOR_MAILBOX_CAPACITY", default_value = "64", value_parser = parse_positive_count)]
    coordinator_actor_mailbox_capacity: PositiveCount,
    #[arg(long, env = "CRABKA_UNCLEAN_RECOVERY_QUEUE_CAPACITY", default_value = "256", value_parser = parse_positive_count)]
    unclean_recovery_queue_capacity: PositiveCount,
    #[arg(long, env = "CRABKA_SHARE_RECOVERY_READ_MAX_BYTES", default_value = "1048576", value_parser = parse_positive_count)]
    share_recovery_read_max_bytes: PositiveCount,
    #[arg(long, env = "CRABKA_SHARE_SESSION_CACHE_MAX_WHEN_UNLIMITED", default_value = "10000", value_parser = parse_positive_count)]
    share_session_cache_max_when_unlimited: PositiveCount,
    #[arg(long, env = "CRABKA_SOCKET_REQUEST_MAX_BYTES", default_value = "104857600", value_parser = parse_positive_count)]
    socket_request_max_bytes: PositiveCount,
    #[arg(long, env = "CRABKA_SENDFILE_MIN_BYTES", default_value = "32768", value_parser = parse_positive_count)]
    sendfile_min_bytes: PositiveCount,
    #[arg(long, env = "CRABKA_SOCKET_SEND_BUFFER_BYTES", default_value = "1048576", value_parser = parse_positive_count)]
    socket_send_buffer_bytes: PositiveCount,
    #[arg(long, env = "CRABKA_SOCKET_RECEIVE_BUFFER_BYTES", default_value = "1048576", value_parser = parse_positive_count)]
    socket_receive_buffer_bytes: PositiveCount,
    #[arg(long, env = "CRABKA_ACL_MAX_PRINCIPAL_BYTES", default_value = "256", value_parser = parse_positive_count)]
    acl_max_principal_bytes: PositiveCount,
    #[arg(long, env = "CRABKA_ACL_MAX_RESOURCE_NAME_BYTES", default_value = "256", value_parser = parse_positive_count)]
    acl_max_resource_name_bytes: PositiveCount,
    #[arg(long, env = "CRABKA_TELEMETRY_MAX_DECOMPRESSION_RATIO", default_value = "100", value_parser = parse_positive_count)]
    telemetry_max_decompression_ratio: PositiveCount,
    #[arg(long, env = "CRABKA_TELEMETRY_DECOMPRESSED_OUTPUT_FLOOR_BYTES", default_value = "16777216", value_parser = parse_positive_count)]
    telemetry_decompressed_output_floor_bytes: PositiveCount,
    #[arg(long, env = "CRABKA_TELEMETRY_DECOMPRESSED_OUTPUT_CEILING_BYTES", default_value = "1073741824", value_parser = parse_positive_count)]
    telemetry_decompressed_output_ceiling_bytes: PositiveCount,
    #[arg(
        long,
        env = "CRABKA_INTER_BROKER_SERVER_NAME",
        default_value = "localhost"
    )]
    inter_broker_server_name: String,
    #[arg(long, env = "CRABKA_PRODUCER_ID_EXPIRATION_MS", default_value = "86400000", value_parser = parse_positive_i64)]
    producer_id_expiration_ms: PositiveI64,
    #[arg(long, env = "CRABKA_PRODUCER_ID_EXPIRATION_SCAN_INTERVAL_MS", default_value = "600000", value_parser = parse_positive_millis)]
    producer_id_expiration_scan_interval_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_MAX_PRODUCE_GROUP", default_value = "1024", value_parser = parse_positive_count)]
    max_produce_group: PositiveCount,
    #[arg(long, env = "CRABKA_PARTITION_WRITER_QUEUE_DEPTH", default_value = "64", value_parser = parse_positive_count)]
    partition_writer_queue_depth: PositiveCount,
    #[arg(long, env = "CRABKA_DEFAULT_MIN_INSYNC_REPLICAS", default_value = "1", value_parser = parse_positive_i32)]
    default_min_insync_replicas: PositiveI32,
    #[arg(long, env = "CRABKA_FUTURE_LOG_MOVE_READ_CHUNK_BYTES", default_value = "1048576", value_parser = parse_positive_count)]
    future_log_move_read_chunk_bytes: PositiveCount,
    #[arg(long, env = "CRABKA_SHARE_STATE_NUM_PARTITIONS", default_value = "50", value_parser = parse_positive_i32)]
    share_state_num_partitions: PositiveI32,
    #[arg(long, env = "CRABKA_TRANSACTION_STATE_NUM_PARTITIONS", default_value = "50", value_parser = parse_positive_i32)]
    transaction_state_num_partitions: PositiveI32,
    #[arg(long, env = "CRABKA_TRANSACTION_MIN_TIMEOUT_MS", default_value = "1000", value_parser = parse_positive_i32)]
    transaction_min_timeout_ms: PositiveI32,
    #[arg(long, env = "CRABKA_TRANSACTION_MAX_TIMEOUT_MS", default_value = "900000", value_parser = parse_positive_i32)]
    transaction_max_timeout_ms: PositiveI32,

    #[arg(long, env = "CRABKA_SHARE_GROUP_ENABLE", default_value_t = true, action = clap::ArgAction::Set)]
    share_group_enable: bool,
    #[arg(long, env = "CRABKA_SHARE_GROUP_SESSION_TIMEOUT_MS", default_value = "45000", value_parser = parse_positive_millis)]
    share_group_session_timeout_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_SHARE_GROUP_HEARTBEAT_INTERVAL_MS", default_value = "5000", value_parser = parse_positive_millis)]
    share_group_heartbeat_interval_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_SHARE_GROUP_RECORD_LOCK_DURATION_MS", default_value = "30000", value_parser = parse_positive_millis)]
    share_group_record_lock_duration_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_SHARE_GROUP_MAX_DELIVERY_ATTEMPTS", default_value_t = 5, value_parser = clap::value_parser!(i16).range(1..))]
    share_group_max_delivery_attempts: i16,
    #[arg(long, env = "CRABKA_SHARE_GROUP_MAX_INFLIGHT_RECORDS", default_value = "200", value_parser = parse_positive_i32)]
    share_group_max_inflight_records: PositiveI32,
    #[arg(long, env = "CRABKA_SHARE_GROUP_ISOLATION_LEVEL", default_value = "read-uncommitted", value_parser = parse_share_isolation)]
    share_group_isolation_level:
        crabka_broker::coordinator::unified::share::config::ShareIsolationLevel,
    #[arg(long, env = "CRABKA_STREAMS_GROUP_SESSION_TIMEOUT_MS", default_value = "45000", value_parser = parse_positive_millis)]
    streams_group_session_timeout_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_STREAMS_GROUP_HEARTBEAT_INTERVAL_MS", default_value = "5000", value_parser = parse_positive_millis)]
    streams_group_heartbeat_interval_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_STREAMS_GROUP_NUM_STANDBY_REPLICAS", default_value_t = 0, value_parser = clap::value_parser!(i32).range(0..))]
    streams_group_num_standby_replicas: i32,
    #[arg(long, env = "CRABKA_STREAMS_GROUP_NUM_WARMUP_REPLICAS", default_value_t = 2, value_parser = clap::value_parser!(i32).range(0..))]
    streams_group_num_warmup_replicas: i32,
    #[arg(long, env = "CRABKA_STREAMS_GROUP_ACCEPTABLE_RECOVERY_LAG", default_value_t = 10_000, value_parser = clap::value_parser!(i64).range(0..))]
    streams_group_acceptable_recovery_lag: i64,
    #[arg(long, env = "CRABKA_STREAMS_GROUP_TASK_OFFSET_INTERVAL_MS", default_value = "30000", value_parser = parse_positive_millis)]
    streams_group_task_offset_interval_ms: PositiveMillis,
    #[arg(long, env = "CRABKA_STREAMS_GROUP_ASSIGNOR", default_value = "auto", value_parser = parse_streams_assignor)]
    streams_group_assignor:
        crabka_broker::coordinator::unified::streams::config::StreamsAssignorKind,
}

#[derive(Debug, Parser)]
#[command(
    name = "crabka-broker",
    version,
    about = "Single-node Kafka-compatible broker (MVP)"
)]
struct Args {
    #[command(flatten)]
    runtime: RuntimeArgs,

    /// TCP address to listen on. Mutually exclusive with `--config-file`.
    #[arg(long, default_value = "127.0.0.1:9092", conflicts_with = "config_file")]
    listen_addr: SocketAddr,

    /// `host:port` to advertise to clients (defaults to `listen_addr`).
    /// Set via env `CRABKA_ADVERTISED_LISTENER` from the operator.
    /// Mutually exclusive with `--config-file`.
    #[arg(
        long,
        env = "CRABKA_ADVERTISED_LISTENER",
        conflicts_with = "config_file"
    )]
    advertised_listener: Option<String>,

    /// Path to a TOML config file (operator-managed). When set,
    /// `--listen-addr` / `--advertised-listener` must NOT be set;
    /// listener configuration comes from the file's `[[listeners]]`
    /// table. See `crabka_broker::file_config::FileConfig`.
    #[arg(long)]
    config_file: Option<PathBuf>,

    /// Primary log directory. Holds the cluster-metadata raft log and is
    /// the default partition data directory.
    #[arg(long, default_value = "./crabka-data")]
    log_dir: PathBuf,

    /// Additional JBOD data directories (KIP-113), comma-separated. New
    /// partitions are spread across `--log-dir` plus these by least-loaded
    /// placement. The cluster-metadata log always stays on `--log-dir`.
    /// Maps to Kafka's `log.dirs` having more than one entry.
    #[arg(
        long,
        env = "CRABKA_EXTRA_LOG_DIRS",
        value_delimiter = ',',
        num_args = 0..
    )]
    extra_log_dirs: Vec<PathBuf>,

    /// Numeric broker id.
    #[arg(long, default_value_t = 1)]
    broker_id: i32,

    /// `KRaft` `process.roles`, comma-separated (`controller`, `broker`).
    /// Defaults to the combined set when unset. The operator normally sets
    /// this via the `[process]` section of `--config-file` instead.
    #[arg(
        long,
        env = "CRABKA_PROCESS_ROLES",
        value_delimiter = ',',
        num_args = 0..
    )]
    process_roles: Vec<String>,

    /// Cluster UUID. Every broker in the same cluster must share this
    /// value. Set via env `CRABKA_CLUSTER_ID` from the operator
    /// (the `KafkaCluster` UID).
    #[arg(long, env = "CRABKA_CLUSTER_ID")]
    cluster_id: Option<uuid::Uuid>,

    /// Bind address for the Prometheus `/metrics` HTTP endpoint.
    /// Empty string (or `none`) disables. Defaults to `0.0.0.0:9404`
    /// — the same port `jmx_prometheus_javaagent` uses for vanilla
    /// Kafka, so existing scrape configs apply unchanged.
    #[arg(
        long,
        env = "CRABKA_METRICS_LISTEN_ADDR",
        default_value = "0.0.0.0:9404"
    )]
    metrics_listen_addr: String,

    /// Partition disk-usage scan cadence, in seconds. `0`
    /// disables the scanner entirely. The rebalancer's usage scraper
    /// reads the `partition_disk_bytes` gauge this populates.
    #[arg(
        long,
        env = "CRABKA_PARTITION_DISK_SCAN_INTERVAL_SECS",
        default_value_t = 60
    )]
    partition_disk_scan_interval_secs: u64,

    /// KIP-853: controller endpoints to discover the quorum leader at cold
    /// start, comma-separated `host:port`. Used by joiner nodes (those
    /// formatted without `--standalone` / `--initial-controllers`). Maps to
    /// Kafka's `controller.quorum.bootstrap.servers`.
    #[arg(
        long,
        env = "CRABKA_CONTROLLER_BOOTSTRAP_SERVERS",
        value_delimiter = ',',
        num_args = 0..
    )]
    controller_bootstrap_servers: Vec<SocketAddr>,

    /// KIP-853: auto-join the quorum as a voter once caught up as an
    /// observer. Maps to Kafka's `controller.quorum.auto.join.enable`.
    #[arg(long, env = "CRABKA_CONTROLLER_AUTO_JOIN")]
    controller_auto_join: bool,

    /// KIP-853 observer promotion lag bound.
    #[arg(
        long,
        env = "CRABKA_OBSERVER_LAG_BOUND",
        default_value_t = DEFAULT_OBSERVER_LAG_BOUND
    )]
    observer_lag_bound: u64,

    /// Broker heartbeat interval in milliseconds.
    #[arg(
        long,
        env = "CRABKA_HEARTBEAT_INTERVAL_MS",
        default_value = "3000",
        value_parser = parse_positive_millis
    )]
    heartbeat_interval_ms: PositiveMillis,

    /// Broker heartbeat timeout in milliseconds.
    #[arg(
        long,
        env = "CRABKA_HEARTBEAT_TIMEOUT_MS",
        default_value = "9000",
        value_parser = parse_positive_millis
    )]
    heartbeat_timeout_ms: PositiveMillis,

    /// Follower lag timeout in milliseconds before ISR shrink.
    #[arg(
        long,
        env = "CRABKA_REPLICA_LAG_TIME_MAX_MS",
        default_value = "30000",
        value_parser = parse_positive_millis
    )]
    replica_lag_time_max_ms: PositiveMillis,

    /// Controller election timeout in milliseconds.
    #[arg(
        long,
        env = "CRABKA_CONTROLLER_ELECTION_TIMEOUT_MS",
        default_value = "5000",
        value_parser = parse_positive_millis
    )]
    controller_election_timeout_ms: PositiveMillis,

    /// Controller heartbeat interval in milliseconds.
    #[arg(
        long,
        env = "CRABKA_CONTROLLER_HEARTBEAT_INTERVAL_MS",
        default_value = "500",
        value_parser = parse_positive_millis
    )]
    controller_heartbeat_interval_ms: PositiveMillis,

    /// Controlled-shutdown leadership drain timeout in milliseconds.
    #[arg(
        long,
        env = "CRABKA_CONTROLLED_SHUTDOWN_DRAIN_TIMEOUT_MS",
        default_value = "20000",
        value_parser = parse_positive_millis
    )]
    controlled_shutdown_drain_timeout_ms: PositiveMillis,

    /// Maximum bytes between metadata-log snapshots.
    #[arg(
        long,
        env = "CRABKA_METADATA_MAX_BYTES_BETWEEN_SNAPSHOTS",
        default_value_t = DEFAULT_METADATA_MAX_BYTES_BETWEEN_SNAPSHOTS,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    metadata_max_bytes_between_snapshots: u64,

    /// Maximum milliseconds between metadata-log snapshots; `0` disables the interval cap.
    #[arg(
        long,
        env = "CRABKA_METADATA_MAX_SNAPSHOT_INTERVAL_MS",
        default_value_t = DEFAULT_METADATA_MAX_SNAPSHOT_INTERVAL_MS
    )]
    metadata_max_snapshot_interval_ms: u64,

    /// Committed-record gap between metadata-log snapshots.
    #[arg(
        long,
        env = "CRABKA_METADATA_SNAPSHOT_INTERVAL_RECORDS",
        default_value_t = DEFAULT_METADATA_SNAPSHOT_INTERVAL_RECORDS,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    metadata_snapshot_interval_records: u64,

    /// Idle-transaction abort cleanup interval in milliseconds; `0` disables the reaper.
    #[arg(
        long,
        env = "CRABKA_TXN_ABORT_CLEANUP_INTERVAL_MS",
        default_value_t = DEFAULT_TXN_ABORT_CLEANUP_INTERVAL_MS
    )]
    txn_abort_cleanup_interval_ms: u64,

    /// Auto preferred-replica election scan cadence, in seconds.
    #[arg(
        long,
        env = "CRABKA_LEADER_IMBALANCE_CHECK_INTERVAL_SECS",
        default_value_t = DEFAULT_LEADER_IMBALANCE_CHECK_INTERVAL_SECS,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    leader_imbalance_check_interval_secs: u64,

    /// Minimum per-broker leader imbalance percentage before auto-rebalance acts.
    #[arg(
        long,
        env = "CRABKA_LEADER_IMBALANCE_PER_BROKER_PERCENTAGE",
        default_value = "10",
        value_parser = parse_percentage
    )]
    leader_imbalance_per_broker_percentage: Percentage,

    /// TLS cert/key reload polling interval in milliseconds; `0` disables the watcher.
    #[arg(long, env = "CRABKA_TLS_RELOAD_INTERVAL_MS", default_value_t = DEFAULT_TLS_RELOAD_INTERVAL_MS)]
    tls_reload_interval_ms: u64,

    /// Maximum incremental fetch-session cache slots.
    #[arg(
        long,
        env = "CRABKA_MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS",
        default_value_t = DEFAULT_MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS
    )]
    max_incremental_fetch_session_cache_slots: usize,

    /// Maximum live broker connections across all listeners.
    #[arg(long, env = "CRABKA_MAX_CONNECTIONS", default_value_t = usize::MAX)]
    max_connections: usize,

    /// Maximum live broker connections from any single client IP.
    #[arg(long, env = "CRABKA_MAX_CONNECTIONS_PER_IP", default_value_t = usize::MAX)]
    max_connections_per_ip: usize,

    /// Delegation-token maximum lifetime in milliseconds.
    #[arg(
        long,
        env = "CRABKA_DELEGATION_TOKEN_MAX_LIFETIME_MS",
        default_value = "604800000",
        value_parser = parse_positive_i64
    )]
    delegation_token_max_lifetime_ms: PositiveI64,

    /// Delegation-token expiry sweep interval in milliseconds.
    #[arg(
        long,
        env = "CRABKA_DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL_MS",
        default_value = "3600000",
        value_parser = parse_positive_i64
    )]
    delegation_token_expiry_check_interval_ms: PositiveI64,

    /// Delegation-token default renew period in milliseconds.
    #[arg(
        long,
        env = "CRABKA_DELEGATION_TOKEN_RENEW_PERIOD_MS",
        default_value = "86400000",
        value_parser = parse_positive_i64
    )]
    delegation_token_default_renew_period_ms: PositiveI64,

    /// `RemoteLogManager` copy/retention cadence in milliseconds.
    #[arg(
        long,
        env = "CRABKA_REMOTE_LOG_MANAGER_INTERVAL_MS",
        default_value = "30000",
        value_parser = parse_positive_millis
    )]
    remote_log_manager_interval_ms: PositiveMillis,

    /// Delegation-token HMAC master key. Prefer secrets managers over shell history.
    #[arg(
        long,
        env = "CRABKA_DELEGATION_TOKEN_SECRET_KEY",
        hide_env_values = true
    )]
    delegation_token_secret_key: Option<String>,

    /// Disable OpenTelemetry SDK/exporters when truthy.
    #[arg(long, env = "OTEL_SDK_DISABLED")]
    otel_sdk_disabled: Option<String>,

    /// CRABKA-specific OTLP endpoint override.
    #[arg(long, env = "CRABKA_OTLP_ENDPOINT")]
    crabka_otlp_endpoint: Option<String>,

    /// OpenTelemetry traces endpoint override.
    #[arg(long, env = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")]
    otel_exporter_otlp_traces_endpoint: Option<String>,

    /// OpenTelemetry endpoint override shared by signals.
    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    otel_exporter_otlp_endpoint: Option<String>,

    /// Enable OTLP export without setting an endpoint.
    #[arg(long, env = "CRABKA_OTLP_ENABLED")]
    crabka_otlp_enabled: Option<String>,

    /// OTLP protocol (`grpc` or `http/protobuf`).
    #[arg(long, env = "CRABKA_OTLP_PROTOCOL")]
    crabka_otlp_protocol: Option<String>,

    /// OpenTelemetry exporter protocol (`grpc` or `http/protobuf`).
    #[arg(long, env = "OTEL_EXPORTER_OTLP_PROTOCOL")]
    otel_exporter_otlp_protocol: Option<String>,

    /// OTLP head sampling ratio in `[0.0, 1.0]`.
    #[arg(long, env = "CRABKA_OTLP_SAMPLE_RATIO")]
    crabka_otlp_sample_ratio: Option<String>,

    /// OpenTelemetry sampler argument used as the trace sample ratio.
    #[arg(long, env = "OTEL_TRACES_SAMPLER_ARG")]
    otel_traces_sampler_arg: Option<String>,

    /// OpenTelemetry service name.
    #[arg(long, env = "OTEL_SERVICE_NAME")]
    otel_service_name: Option<String>,

    /// CRABKA-specific OTLP timeout in seconds.
    #[arg(long, env = "CRABKA_OTLP_TIMEOUT_SECS")]
    crabka_otlp_timeout_secs: Option<String>,

    /// OpenTelemetry exporter timeout in seconds.
    #[arg(long, env = "OTEL_EXPORTER_OTLP_TIMEOUT_SECS")]
    otel_exporter_otlp_timeout_secs: Option<String>,

    /// OTLP heartbeat interval in seconds; `0` disables heartbeats.
    #[arg(long, env = "CRABKA_OTLP_HEARTBEAT_INTERVAL_SECS")]
    crabka_otlp_heartbeat_interval_secs: Option<String>,
}

impl Args {
    fn telemetry_value(&self, key: &str) -> Option<String> {
        match key {
            "OTEL_SDK_DISABLED" => self.otel_sdk_disabled.clone(),
            "CRABKA_OTLP_ENDPOINT" => self.crabka_otlp_endpoint.clone(),
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" => self.otel_exporter_otlp_traces_endpoint.clone(),
            "OTEL_EXPORTER_OTLP_ENDPOINT" => self.otel_exporter_otlp_endpoint.clone(),
            "CRABKA_OTLP_ENABLED" => self.crabka_otlp_enabled.clone(),
            "CRABKA_OTLP_PROTOCOL" => self.crabka_otlp_protocol.clone(),
            "OTEL_EXPORTER_OTLP_PROTOCOL" => self.otel_exporter_otlp_protocol.clone(),
            "CRABKA_OTLP_SAMPLE_RATIO" => self.crabka_otlp_sample_ratio.clone(),
            "OTEL_TRACES_SAMPLER_ARG" => self.otel_traces_sampler_arg.clone(),
            "OTEL_SERVICE_NAME" => self.otel_service_name.clone(),
            "CRABKA_OTLP_TIMEOUT_SECS" => self.crabka_otlp_timeout_secs.clone(),
            "OTEL_EXPORTER_OTLP_TIMEOUT_SECS" => self.otel_exporter_otlp_timeout_secs.clone(),
            "CRABKA_OTLP_HEARTBEAT_INTERVAL_SECS" => {
                self.crabka_otlp_heartbeat_interval_secs.clone()
            }
            _ => None,
        }
    }
}

macro_rules! build_broker_config {
    ($source_args:ident, $source_advertised:ident, $source_controller_addr:ident, $source_node_id:ident, $source_metrics_addr:ident, $source_client_metrics_endpoint:ident) => {{
        let args = $source_args;
        let advertised = $source_advertised;
        let controller_addr = $source_controller_addr;
        let node_id = $source_node_id;
        let metrics_listen_addr = $source_metrics_addr;
        let client_metrics_otlp_endpoint = $source_client_metrics_endpoint;
        BrokerConfig {
            startup_leader_wait_timeout: std::time::Duration::from_millis(
                args.runtime.startup_leader_wait_timeout_ms.into_value(),
            ),
            self_registration_backoff_min: std::time::Duration::from_millis(
                args.runtime.self_registration_backoff_min_ms.into_value(),
            ),
            self_registration_backoff_max: std::time::Duration::from_millis(
                args.runtime.self_registration_backoff_max_ms.into_value(),
            ),
            observer_poll_interval: std::time::Duration::from_millis(
                args.runtime.observer_poll_interval_ms.into_value(),
            ),
            audit_spool_replay_interval: std::time::Duration::from_millis(
                args.runtime.audit_spool_replay_interval_ms.into_value(),
            ),
            audit_stats_poll_interval: std::time::Duration::from_millis(
                args.runtime.audit_stats_poll_interval_ms.into_value(),
            ),
            audit_partition_wait_timeout: std::time::Duration::from_millis(
                args.runtime.audit_partition_wait_timeout_ms.into_value(),
            ),
            liveness_tick_interval: std::time::Duration::from_millis(
                args.runtime.liveness_tick_interval_ms.into_value(),
            ),
            gauge_poll_interval: std::time::Duration::from_millis(
                args.runtime.gauge_poll_interval_ms.into_value(),
            ),
            isr_scan_interval: std::time::Duration::from_millis(
                args.runtime.isr_scan_interval_ms.into_value(),
            ),
            cleaner_interval: std::time::Duration::from_millis(
                args.runtime.cleaner_interval_ms.into_value(),
            ),
            future_log_move_retry_backoff: std::time::Duration::from_millis(
                args.runtime.future_log_move_retry_backoff_ms.into_value(),
            ),
            client_metrics_eviction_tick: std::time::Duration::from_millis(
                args.runtime.client_metrics_eviction_tick_ms.into_value(),
            ),
            client_metrics_stale_floor: std::time::Duration::from_millis(
                args.runtime.client_metrics_stale_floor_ms.into_value(),
            ),
            client_metrics_default_interval_ms: args
                .runtime
                .client_metrics_default_interval_ms
                .into_value(),
            client_metrics_otlp_queue_capacity: args
                .runtime
                .client_metrics_otlp_queue_capacity
                .into_value(),
            client_metrics_telemetry_max_bytes: args
                .runtime
                .client_metrics_telemetry_max_bytes
                .into_value(),
            client_metrics_prom_snapshot_ttl: std::time::Duration::from_millis(
                args.runtime
                    .client_metrics_prom_snapshot_ttl_ms
                    .into_value(),
            ),
            rlmm_reconcile_tick: std::time::Duration::from_millis(
                args.runtime.rlmm_reconcile_tick_ms.into_value(),
            ),
            rlmm_bootstrap_backoff_initial: std::time::Duration::from_millis(
                args.runtime.rlmm_bootstrap_backoff_initial_ms.into_value(),
            ),
            rlmm_bootstrap_backoff_max: std::time::Duration::from_millis(
                args.runtime.rlmm_bootstrap_backoff_max_ms.into_value(),
            ),
            connection_creation_throttle_max: std::time::Duration::from_millis(
                args.runtime
                    .connection_creation_throttle_max_ms
                    .into_value(),
            ),
            opa_http_timeout: std::time::Duration::from_millis(
                args.runtime.opa_http_timeout_ms.into_value(),
            ),
            oauth_jwks_http_timeout: std::time::Duration::from_millis(
                args.runtime.oauth_jwks_http_timeout_ms.into_value(),
            ),
            auto_join_retry_backoff: std::time::Duration::from_millis(
                args.runtime.auto_join_retry_backoff_ms.into_value(),
            ),
            replication: crabka_broker::config::ReplicationRuntimeConfig {
                fetch_max_bytes: args.runtime.replication_fetch_max_bytes.into_value(),
                fetch_max_wait_ms: args.runtime.replication_fetch_max_wait_ms.into_value(),
                fetch_min_bytes: args.runtime.replication_fetch_min_bytes.into_value(),
                throttle_exhausted_backoff: std::time::Duration::from_millis(
                    args.runtime
                        .replication_throttle_exhausted_backoff_ms
                        .into_value(),
                ),
                send_error_backoff: std::time::Duration::from_millis(
                    args.runtime.replication_send_error_backoff_ms.into_value(),
                ),
                unknown_topic_retry_delay: std::time::Duration::from_millis(
                    args.runtime
                        .replication_unknown_topic_retry_delay_ms
                        .into_value(),
                ),
                epoch_fence_backoff: std::time::Duration::from_millis(
                    args.runtime.replication_epoch_fence_backoff_ms.into_value(),
                ),
                unexpected_error_backoff: std::time::Duration::from_millis(
                    args.runtime
                        .replication_unexpected_error_backoff_ms
                        .into_value(),
                ),
                reconnect_initial_delay: std::time::Duration::from_millis(
                    args.runtime
                        .replication_reconnect_initial_delay_ms
                        .into_value(),
                ),
                reconnect_delay_cap: std::time::Duration::from_millis(
                    args.runtime.replication_reconnect_delay_cap_ms.into_value(),
                ),
            },
            coordinator_session_expiry_tick: std::time::Duration::from_millis(
                args.runtime.coordinator_session_expiry_tick_ms.into_value(),
            ),
            coordinator_shutdown_ack_timeout: std::time::Duration::from_millis(
                args.runtime
                    .coordinator_shutdown_ack_timeout_ms
                    .into_value(),
            ),
            classic_group_initial_rebalance_delay: std::time::Duration::from_millis(
                args.runtime
                    .classic_group_initial_rebalance_delay_ms
                    .into_value(),
            ),
            sync_group_follower_wait: std::time::Duration::from_millis(
                args.runtime.sync_group_follower_wait_ms.into_value(),
            ),
            unclean_recovery_aggressive_deadline: std::time::Duration::from_millis(
                args.runtime
                    .unclean_recovery_aggressive_deadline_ms
                    .into_value(),
            ),
            unclean_recovery_balanced_deadline: std::time::Duration::from_millis(
                args.runtime
                    .unclean_recovery_balanced_deadline_ms
                    .into_value(),
            ),
            operator_recovery_deadline: std::time::Duration::from_millis(
                args.runtime.operator_recovery_deadline_ms.into_value(),
            ),
            quota_throttle_max: std::time::Duration::from_millis(
                args.runtime.quota_throttle_max_ms.into_value(),
            ),
            self_registration_max_attempts: args.runtime.self_registration_max_attempts,
            observer_fetch_max_bytes: args.runtime.observer_fetch_max_bytes,
            audit_event_queue_capacity: args.runtime.audit_event_queue_capacity.into_value(),
            audit_tail_window_offsets: args.runtime.audit_tail_window_offsets.into_value(),
            audit_tail_read_max_bytes: args.runtime.audit_tail_read_max_bytes.into_value(),
            offsets_topic_metadata_wait_timeout: std::time::Duration::from_millis(
                args.runtime
                    .offsets_topic_metadata_wait_timeout_ms
                    .into_value(),
            ),
            client_metrics_stale_push_intervals: args.runtime.client_metrics_stale_push_intervals,
            coordinator_actor_mailbox_capacity: args
                .runtime
                .coordinator_actor_mailbox_capacity
                .into_value(),
            unclean_recovery_queue_capacity: args
                .runtime
                .unclean_recovery_queue_capacity
                .into_value(),
            share_recovery_read_max_bytes: args.runtime.share_recovery_read_max_bytes.into_value(),
            share_session_cache_max_when_unlimited: args
                .runtime
                .share_session_cache_max_when_unlimited
                .into_value(),
            socket_request_max_bytes: args.runtime.socket_request_max_bytes.into_value(),
            sendfile_min_bytes: args.runtime.sendfile_min_bytes.into_value(),
            socket_send_buffer_bytes: args.runtime.socket_send_buffer_bytes.into_value(),
            socket_receive_buffer_bytes: args.runtime.socket_receive_buffer_bytes.into_value(),
            acl_max_principal_bytes: args.runtime.acl_max_principal_bytes.into_value(),
            acl_max_resource_name_bytes: args.runtime.acl_max_resource_name_bytes.into_value(),
            telemetry_max_decompression_ratio: args
                .runtime
                .telemetry_max_decompression_ratio
                .into_value(),
            telemetry_decompressed_output_floor_bytes: args
                .runtime
                .telemetry_decompressed_output_floor_bytes
                .into_value(),
            telemetry_decompressed_output_ceiling_bytes: args
                .runtime
                .telemetry_decompressed_output_ceiling_bytes
                .into_value(),
            inter_broker_server_name: args.runtime.inter_broker_server_name.clone(),
            producer_id_expiration_ms: args.runtime.producer_id_expiration_ms.into_value(),
            producer_id_expiration_scan_interval: std::time::Duration::from_millis(
                args.runtime
                    .producer_id_expiration_scan_interval_ms
                    .into_value(),
            ),
            max_produce_group: args.runtime.max_produce_group.into_value(),
            partition_writer_queue_depth: args.runtime.partition_writer_queue_depth.into_value(),
            default_min_insync_replicas: args.runtime.default_min_insync_replicas.into_value(),
            future_log_move_read_chunk_bytes: args
                .runtime
                .future_log_move_read_chunk_bytes
                .into_value(),
            transaction_state_num_partitions: args
                .runtime
                .transaction_state_num_partitions
                .into_value(),
            transaction_min_timeout_ms: args.runtime.transaction_min_timeout_ms.into_value(),
            transaction_max_timeout_ms: args.runtime.transaction_max_timeout_ms.into_value(),
            broker_id: args.broker_id,
            listen_addr: args.listen_addr,
            advertised_listener: advertised,
            log_dir: args.log_dir,
            extra_log_dirs: args.extra_log_dirs,
            log_config: LogConfig::default(),
            node_id: crabka_broker::NodeId(node_id),
            controller_listen_addr: controller_addr,
            controller_quorum_voters: vec![(
                crabka_broker::NodeId(node_id),
                controller_addr.to_string(),
            )],
            bootstrap_servers: args.controller_bootstrap_servers,
            // Placeholder — replaced from `meta.properties.json` (written by
            // `crabka format`) once `log_dir` is resolved against the TOML.
            directory_id: uuid::Uuid::nil(),
            auto_join: args.controller_auto_join,
            observer_lag_bound: args.observer_lag_bound,
            heartbeat_interval_ms: args.heartbeat_interval_ms.into_value(),
            heartbeat_timeout_ms: args.heartbeat_timeout_ms.into_value(),
            replica_lag_time_max_ms: args.replica_lag_time_max_ms.into_value(),
            controller_election_timeout: std::time::Duration::from_millis(
                args.controller_election_timeout_ms.into_value(),
            ),
            controller_heartbeat_interval: std::time::Duration::from_millis(
                args.controller_heartbeat_interval_ms.into_value(),
            ),
            metadata_max_bytes_between_snapshots: args.metadata_max_bytes_between_snapshots,
            metadata_max_snapshot_interval: std::time::Duration::from_millis(
                args.metadata_max_snapshot_interval_ms,
            ),
            metadata_snapshot_interval_records: args.metadata_snapshot_interval_records,
            txn_abort_cleanup_interval: std::time::Duration::from_millis(
                args.txn_abort_cleanup_interval_ms,
            ),
            leader_imbalance_check_interval_secs: args.leader_imbalance_check_interval_secs,
            leader_imbalance_per_broker_percentage: args
                .leader_imbalance_per_broker_percentage
                .into_value(),
            tls_reload_interval: std::time::Duration::from_millis(args.tls_reload_interval_ms),
            max_incremental_fetch_session_cache_slots: args
                .max_incremental_fetch_session_cache_slots,
            max_connections: args.max_connections,
            max_connections_per_ip: args.max_connections_per_ip,
            delegation_token_max_lifetime_ms: args.delegation_token_max_lifetime_ms.into_value(),
            delegation_token_expiry_check_interval_ms: args
                .delegation_token_expiry_check_interval_ms
                .into_value(),
            delegation_token_default_renew_period_ms: args
                .delegation_token_default_renew_period_ms
                .into_value(),
            remote_log_manager_interval: std::time::Duration::from_millis(
                args.remote_log_manager_interval_ms.into_value(),
            ),
            next_gen_consumer_group: Box::new(
                crabka_broker::coordinator::unified::config::NextGenConfig {
                    session_timeout: std::time::Duration::from_millis(
                        args.runtime.consumer_group_session_timeout_ms.into_value(),
                    ),
                    heartbeat_interval: std::time::Duration::from_millis(
                        args.runtime
                            .consumer_group_heartbeat_interval_ms
                            .into_value(),
                    ),
                    min_session_timeout: std::time::Duration::from_millis(
                        args.runtime
                            .consumer_group_min_session_timeout_ms
                            .into_value(),
                    ),
                    max_session_timeout: std::time::Duration::from_millis(
                        args.runtime
                            .consumer_group_max_session_timeout_ms
                            .into_value(),
                    ),
                    min_heartbeat_interval: std::time::Duration::from_millis(
                        args.runtime
                            .consumer_group_min_heartbeat_interval_ms
                            .into_value(),
                    ),
                    max_heartbeat_interval: std::time::Duration::from_millis(
                        args.runtime
                            .consumer_group_max_heartbeat_interval_ms
                            .into_value(),
                    ),
                    max_size: args.runtime.consumer_group_max_size.into_value(),
                    ..Default::default()
                },
            ),
            share_group: Box::new(
                crabka_broker::coordinator::unified::share::config::ShareGroupConfig {
                    enable: args.runtime.share_group_enable,
                    session_timeout: std::time::Duration::from_millis(
                        args.runtime.share_group_session_timeout_ms.into_value(),
                    ),
                    heartbeat_interval: std::time::Duration::from_millis(
                        args.runtime.share_group_heartbeat_interval_ms.into_value(),
                    ),
                    record_lock_duration: std::time::Duration::from_millis(
                        args.runtime
                            .share_group_record_lock_duration_ms
                            .into_value(),
                    ),
                    max_delivery_attempts: args.runtime.share_group_max_delivery_attempts,
                    max_inflight_records: args
                        .runtime
                        .share_group_max_inflight_records
                        .into_value(),
                    isolation_level: args.runtime.share_group_isolation_level,
                    ..Default::default()
                },
            ),
            streams_group: Box::new(
                crabka_broker::coordinator::unified::streams::config::StreamsGroupConfig {
                    session_timeout: std::time::Duration::from_millis(
                        args.runtime.streams_group_session_timeout_ms.into_value(),
                    ),
                    heartbeat_interval: std::time::Duration::from_millis(
                        args.runtime
                            .streams_group_heartbeat_interval_ms
                            .into_value(),
                    ),
                    num_standby_replicas: args.runtime.streams_group_num_standby_replicas,
                    num_warmup_replicas: args.runtime.streams_group_num_warmup_replicas,
                    acceptable_recovery_lag: args.runtime.streams_group_acceptable_recovery_lag,
                    task_offset_interval: std::time::Duration::from_millis(
                        args.runtime
                            .streams_group_task_offset_interval_ms
                            .into_value(),
                    ),
                    assignor: args.runtime.streams_group_assignor,
                    ..Default::default()
                },
            ),
            share_coordinator: Box::new(
                crabka_broker::share_coordinator::config::ShareCoordinatorConfig {
                    state_topic_num_partitions: args
                        .runtime
                        .share_state_num_partitions
                        .into_value(),
                    ..Default::default()
                },
            ),
            // Placeholder — overwritten after `apply_to` against the final `log_dir`.
            bootstrap_mode: BootstrapMode::Bootstrap,
            cluster_id: args.cluster_id,
            metrics_listen_addr,
            client_metrics_otlp_endpoint,
            partition_disk_scan_interval_secs: args.partition_disk_scan_interval_secs,
            delegation_token_secret_key: args
                .delegation_token_secret_key
                .map(|key| crabka_security::SecretBytes::new(key.into_bytes())),
            ..BrokerConfig::default()
        }
    }};
}

#[tokio::main]
// binary entrypoint: linear startup wiring
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = Args::parse();

    // Install the tracing subscriber — stdout `fmt` plus an
    // optional OTLP export layer. OTLP stays off unless the environment
    // opts in (see `crabka_broker::telemetry`). Built here, inside the
    // tokio runtime, so the gRPC exporter captures the runtime handle.
    let otlp = crabka_broker::telemetry::OtlpConfig::from_env(
        |k| args.telemetry_value(k),
        &args.broker_id.to_string(),
        env!("CARGO_PKG_VERSION"),
        "crabka-broker",
    );
    let client_metrics_otlp_endpoint = otlp.as_ref().map(|cfg| cfg.endpoint.clone());
    let telemetry = crabka_broker::telemetry::init(
        otlp,
        "crabka_broker=info,crabka_log=info,info",
        "info,crabka_broker::request=debug,crabka_log=info",
        "crabka-broker",
    )?;
    let file_config: Option<crabka_broker::file_config::FileConfig> =
        match args.config_file.as_ref() {
            Some(p) => {
                let contents = std::fs::read_to_string(p)
                    .map_err(|e| format!("failed to read {}: {e}", p.display()))?;
                Some(
                    toml::from_str(&contents)
                        .map_err(|e| format!("failed to parse {}: {e}", p.display()))?,
                )
            }
            None => None,
        };
    let cli_shutdown_timeout_ms = args.controlled_shutdown_drain_timeout_ms.into_value();
    let controlled_shutdown_drain_timeout_ms =
        if cli_shutdown_timeout_ms == DEFAULT_CONTROLLED_SHUTDOWN_DRAIN_TIMEOUT_MS {
            file_config
                .as_ref()
                .and_then(|file| file.runtime.as_ref())
                .and_then(|runtime| runtime.controlled_shutdown_drain_timeout_ms)
                .map(|value| {
                    PositiveMillis::new(value)
                        .map(PositiveMillis::into_value)
                        .map_err(|error| format!("controlled_shutdown_drain_timeout_ms: {error}"))
                })
                .transpose()?
                .unwrap_or(cli_shutdown_timeout_ms)
        } else {
            cli_shutdown_timeout_ms
        };
    let advertised = args
        .advertised_listener
        .take()
        .unwrap_or_else(|| args.listen_addr.to_string());
    let controller_addr: std::net::SocketAddr = {
        let mut a = args.listen_addr;
        a.set_port(9093);
        // Under `--config-file` (operator/StatefulSet mode), `--listen-addr`
        // conflicts_with the config file, so `args.listen_addr` keeps its
        // 127.0.0.1:9092 default. Peers dial this broker's controller via its
        // pod FQDN, so binding the controller listener to loopback would make
        // it unreachable across pods — bind all interfaces (0.0.0.0) instead.
        if args.config_file.is_some() {
            a.set_ip(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
        }
        a
    };
    let node_id = u64::try_from(args.broker_id).unwrap_or_else(|_| {
        eprintln!("broker_id must be non-negative");
        std::process::exit(1);
    });
    let metrics_listen_addr = parse_metrics_addr(&args.metrics_listen_addr)?;
    let roles = if args.process_roles.is_empty() {
        None
    } else {
        Some(parse_roles_arg(&args.process_roles)?)
    };
    let mut config = build_broker_config!(
        args,
        advertised,
        controller_addr,
        node_id,
        metrics_listen_addr,
        client_metrics_otlp_endpoint
    );
    if let Some(roles) = roles {
        config.roles = roles;
    }
    if let Some(fc) = file_config {
        fc.apply_to(&mut config)?;
    }
    // Detect against the *resolved* log_dir so a TOML override picks up
    // its on-disk state rather than the CLI-default empty path. This is
    // the difference between a fresh-pod Bootstrap and a rolled-pod
    // Rejoin against an existing PVC.
    config.bootstrap_mode = detect_bootstrap_mode(&config.log_dir);
    // KIP-853: recover this replica's stable directory id, written by
    // `crabka format`. Required for every formatted node; absence means the
    // dir was never formatted, which is an operator error.
    config.directory_id = crabka_broker::bootstrap::read_directory_id(&config.log_dir)?;
    tracing::info!(
        bootstrap_mode = ?config.bootstrap_mode,
        directory_id = %config.directory_id,
        log_dir = %config.log_dir.display(),
        "selected bootstrap mode"
    );

    let handle = Broker::start(config).await?;
    tracing::info!(addr = %handle.listen_addr(), "crabka-broker listening");

    let mut shutdown_rx = handle.should_shutdown_rx();
    tokio::select! {
        signal = wait_for_termination_signal() => {
            tracing::info!(signal, "shutdown signal received");
        }
        () = async {
            // Wait until the self-shutdown flag flips true.
            loop {
                // Check first in case the flag was already set before we subscribed.
                if *shutdown_rx.borrow_and_update() { break; }
                if shutdown_rx.changed().await.is_err() { break; }
            }
        } => {
            tracing::error!("self-shutdown triggered (all log dirs offline); stopping broker");
        }
    }
    // KIP-500 controlled shutdown: ask the controller to move leadership of
    // every partition this broker leads onto its other in-sync replicas
    // BEFORE we stop. This is the difference between a near-seamless failover
    // and stranding producers on a dead leader until their request timeout —
    // `kubectl delete pod` sends SIGTERM, and without this hand-off the
    // partition has no leader until the controller fences us (~tens of
    // seconds). Bounded well under the pod's terminationGracePeriod (30s); on
    // timeout `controlled_shutdown` falls back to a hard stop internally.
    match handle
        .controlled_shutdown(std::time::Duration::from_millis(
            controlled_shutdown_drain_timeout_ms,
        ))
        .await
    {
        Ok(()) => tracing::info!("controlled shutdown complete (leadership drained)"),
        Err(e) => tracing::warn!(error = %e, "controlled shutdown incomplete; hard-stopped"),
    }
    tracing::info!("crabka-broker stopped");
    telemetry.shutdown();
    Ok(())
}

/// Block until a process-termination signal arrives, returning its name for
/// logging. On Unix this is SIGINT (Ctrl-C) **or SIGTERM** — `kubectl delete
/// pod` (and the default container stop) sends SIGTERM, so catching only
/// SIGINT meant the broker was hard-killed by SIGKILL with no controlled
/// shutdown. On non-Unix targets, Ctrl-C only.
#[cfg(unix)]
async fn wait_for_termination_signal() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};
    match signal(SignalKind::terminate()) {
        Ok(mut sigterm) => tokio::select! {
            _ = tokio::signal::ctrl_c() => "SIGINT",
            _ = sigterm.recv() => "SIGTERM",
        },
        Err(e) => {
            // Couldn't install the SIGTERM handler; fall back to SIGINT only
            // rather than refusing to start.
            tracing::warn!(error = %e, "failed to install SIGTERM handler; SIGINT only");
            let _ = tokio::signal::ctrl_c().await;
            "SIGINT"
        }
    }
}

#[cfg(not(unix))]
async fn wait_for_termination_signal() -> &'static str {
    let _ = tokio::signal::ctrl_c().await;
    "SIGINT"
}

/// Map the `--metrics-listen-addr` CLI value onto an `Option<SocketAddr>`.
/// Empty string or `none` (case-insensitive) disables the endpoint;
/// anything else must parse as `SocketAddr`.
fn parse_metrics_addr(s: &str) -> Result<Option<SocketAddr>, Box<dyn std::error::Error>> {
    let trimmed = s.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("none") {
        return Ok(None);
    }
    Ok(Some(trimmed.parse()?))
}

/// Pick `Bootstrap` (fresh cluster) vs `Rejoin` (restart on existing
/// state) based on whether the raft log directory has been populated.
///
/// The broker hands `BrokerConfig.log_dir.join("__cluster_metadata")`
/// to `ControllerConfig.log_dir` (see `broker.rs:833`), and
/// `RaftLogStore::open` then puts its segment files under
/// `<that>/@metadata-0/`. So the absolute path of the raft segments is
/// `<log_dir>/__cluster_metadata/@metadata-0/`. On the first broker
/// boot the directory doesn't exist yet; on every subsequent boot it
/// has segment files from the prior run. Using directory presence +
/// non-empty as the signal matches `Controller::start`'s
/// `log_is_empty` check without having to open the log store from
/// here.
fn detect_bootstrap_mode(log_dir: &Path) -> BootstrapMode {
    // Use the controller's own emptiness check (durable raft state =
    // `__cluster_metadata/quorum-state`, written only after the node has
    // participated in an election/commit) so this Bootstrap/Rejoin choice can
    // never disagree with `Controller::start_with_listener`'s mode validation.
    //
    // The bare `__cluster_metadata/@metadata-0` segment dir is created by
    // `KraftController::open` *before* the first commit. Keying Rejoin on its
    // existence (as we used to) bricked any node killed mid-election on a
    // multi-node cold start: the next boot saw the segment dir, picked Rejoin,
    // and died with "Rejoin requires non-empty raft log" — a crashloop. A node
    // with no persisted quorum-state now correctly re-Bootstraps.
    let metadata_dir = log_dir.join("__cluster_metadata");
    if crabka_raft::metadata_log_nonempty(&metadata_dir) {
        BootstrapMode::Rejoin
    } else {
        BootstrapMode::Bootstrap
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use assert2::assert;
    use tempfile::tempdir;

    use super::*;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[test]
    fn detect_bootstrap_when_log_dir_is_empty() {
        let dir = tempdir().unwrap();
        assert!(detect_bootstrap_mode(dir.path()) == BootstrapMode::Bootstrap);
    }

    #[test]
    fn parse_roles_arg_maps_strings() {
        assert!(
            parse_roles_arg(&["controller".to_string(), "broker".to_string()]).unwrap()
                == vec![
                    crabka_broker::config::NodeRole::Controller,
                    crabka_broker::config::NodeRole::Broker
                ]
        );
    }

    #[test]
    fn parse_roles_arg_rejects_unknown() {
        assert!(parse_roles_arg(&["nope".to_string()]).is_err());
    }

    #[test]
    fn detect_bootstrap_when_metadata_dir_missing() {
        let dir = tempdir().unwrap();
        // log_dir exists with unrelated content (bootstrap.json from
        // `crabka format`) but no __cluster_metadata/@metadata-0 subdir.
        std::fs::write(dir.path().join("bootstrap.json"), "{}").unwrap();
        assert!(detect_bootstrap_mode(dir.path()) == BootstrapMode::Bootstrap);
    }

    #[test]
    fn detect_rejoin_when_quorum_state_persisted() {
        let dir = tempdir().unwrap();
        let meta = dir.path().join("__cluster_metadata");
        std::fs::create_dir_all(&meta).unwrap();
        // Durable raft state — `quorum-state` is written only after the node
        // has participated in an election/commit. This marks a true Rejoin.
        std::fs::write(meta.join("quorum-state"), b"{}").unwrap();
        assert!(detect_bootstrap_mode(dir.path()) == BootstrapMode::Rejoin);
    }

    #[test]
    fn detect_bootstrap_when_segment_dir_but_no_quorum_state() {
        // Regression: a node killed mid-election on a multi-node cold start has
        // an `@metadata-0` segment dir (created by `KraftController::open`)
        // but no `quorum-state`. It must re-Bootstrap, not die in a Rejoin
        // crashloop. Previously this returned Rejoin and bricked the node.
        let dir = tempdir().unwrap();
        let meta = dir.path().join("__cluster_metadata").join("@metadata-0");
        std::fs::create_dir_all(&meta).unwrap();
        std::fs::write(meta.join("00000000000000000000.log"), b"segment").unwrap();
        assert!(detect_bootstrap_mode(dir.path()) == BootstrapMode::Bootstrap);
    }

    #[test]
    fn detect_bootstrap_when_metadata_dir_empty() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("__cluster_metadata").join("@metadata-0")).unwrap();
        // empty @metadata-0 dir is treated as no state (corner case:
        // crashed first start before any segment was written).
        assert!(detect_bootstrap_mode(dir.path()) == BootstrapMode::Bootstrap);
    }

    #[test]
    fn detect_bootstrap_when_only_outer_cluster_metadata_dir_exists() {
        let dir = tempdir().unwrap();
        // The outer __cluster_metadata dir exists but the inner
        // @metadata-0 subdir doesn't — should still be Bootstrap.
        std::fs::create_dir_all(dir.path().join("__cluster_metadata")).unwrap();
        assert!(detect_bootstrap_mode(dir.path()) == BootstrapMode::Bootstrap);
    }

    #[test]
    fn config_file_mutually_exclusive_with_listen_addr() {
        use clap::Parser;

        let res = Args::try_parse_from([
            "crabka-broker",
            "--config-file=/tmp/a.toml",
            "--listen-addr=127.0.0.1:9092",
        ]);
        let err = res.expect_err("expected mutual-exclusion error");
        let s = err.to_string();
        assert!(
            s.contains("config-file") && s.contains("listen-addr"),
            "expected clap conflict mentioning both flags, got: {s}"
        );
    }

    #[test]
    fn config_file_mutually_exclusive_with_advertised_listener() {
        use clap::Parser;

        let res = Args::try_parse_from([
            "crabka-broker",
            "--config-file=/tmp/a.toml",
            "--advertised-listener=h:9092",
        ]);
        let err = res.expect_err("expected mutual-exclusion error");
        let s = err.to_string();
        assert!(
            s.contains("config-file") && s.contains("advertised-listener"),
            "expected clap conflict, got: {s}"
        );
    }

    #[test]
    fn config_file_alone_parses() {
        use clap::Parser;

        let args = Args::try_parse_from(["crabka-broker", "--config-file=/tmp/a.toml"]).unwrap();
        assert!(args.config_file.as_deref() == Some(std::path::Path::new("/tmp/a.toml")));
        assert!(args.advertised_listener.is_none());
    }

    #[test]
    fn runtime_policy_cli_rejects_invalid_and_accepts_valid_values() {
        let cases = [
            (vec!["crabka-broker", "--cleaner-interval-ms=0"], false),
            (vec!["crabka-broker", "--cleaner-interval-ms=1"], true),
            (
                vec!["crabka-broker", "--replication-fetch-min-bytes=0"],
                false,
            ),
            (
                vec!["crabka-broker", "--replication-fetch-min-bytes=1"],
                true,
            ),
            (
                vec![
                    "crabka-broker",
                    "--leader-imbalance-per-broker-percentage=101",
                ],
                false,
            ),
            (
                vec![
                    "crabka-broker",
                    "--leader-imbalance-per-broker-percentage=100",
                ],
                true,
            ),
        ];

        for (args, accepted) in cases {
            assert!(Args::try_parse_from(args).is_ok() == accepted);
        }
    }

    #[test]
    fn runtime_policy_cli_reads_crabka_environment() {
        let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
        let _guard = lock.lock().expect("environment lock");

        temp_env::with_var("CRABKA_CLEANER_INTERVAL_MS", Some("17"), || {
            let args = Args::try_parse_from(["crabka-broker"]).expect("parse environment");
            assert!(args.runtime.cleaner_interval_ms.into_value() == 17);
        });
    }
}
