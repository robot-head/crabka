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
    config::DEFAULT_CONTROLLED_SHUTDOWN_DRAIN_TIMEOUT,
    config_value::{
        PositiveCount, PositiveI16, PositiveI32, PositiveI64, parse_positive_count,
        parse_positive_i16, parse_positive_i32, parse_positive_i64,
    },
};
use crabka_client_core::{
    ClientFrameMax, ConnectionDispatchQueueCapacity, DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
};
use crabka_log::LogConfig;
use crabka_units::{ByteSize, Ratio, Time, convert::TimeExt as _, fmt::Human as _};

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
    #[arg(
        long,
        env = "CRABKA_BROKER_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_client_dispatch_queue_capacity
    )]
    client_dispatch_queue_capacity: usize,
    #[arg(
        long,
        env = "CRABKA_BROKER_CLIENT_FRAME_MAX",
        default_value = "100MiB",
        value_parser = parse_client_frame_max
    )]
    client_frame_max: ByteSize,
    #[arg(long, env = "CRABKA_STARTUP_LEADER_WAIT_TIMEOUT", value_parser = crabka_units::parse::positive_time)]
    startup_leader_wait_timeout: Option<Time>,
    #[arg(long, env = "CRABKA_SELF_REGISTRATION_BACKOFF_MIN", value_parser = crabka_units::parse::positive_time)]
    self_registration_backoff_min: Option<Time>,
    #[arg(long, env = "CRABKA_SELF_REGISTRATION_BACKOFF_MAX", value_parser = crabka_units::parse::positive_time)]
    self_registration_backoff_max: Option<Time>,
    #[arg(long, env = "CRABKA_OBSERVER_POLL_INTERVAL", value_parser = crabka_units::parse::positive_time)]
    observer_poll_interval: Option<Time>,
    #[arg(long, env = "CRABKA_AUDIT_SPOOL_REPLAY_INTERVAL", value_parser = crabka_units::parse::positive_time)]
    audit_spool_replay_interval: Option<Time>,
    #[arg(long, env = "CRABKA_AUDIT_STATS_POLL_INTERVAL", value_parser = crabka_units::parse::positive_time)]
    audit_stats_poll_interval: Option<Time>,
    #[arg(long, env = "CRABKA_AUDIT_PARTITION_WAIT_TIMEOUT", value_parser = crabka_units::parse::positive_time)]
    audit_partition_wait_timeout: Option<Time>,
    #[arg(long, env = "CRABKA_LIVENESS_TICK_INTERVAL", value_parser = crabka_units::parse::positive_time)]
    liveness_tick_interval: Option<Time>,
    #[arg(long, env = "CRABKA_GAUGE_POLL_INTERVAL", value_parser = crabka_units::parse::positive_time)]
    gauge_poll_interval: Option<Time>,
    #[arg(long, env = "CRABKA_ISR_SCAN_INTERVAL", value_parser = crabka_units::parse::positive_time)]
    isr_scan_interval: Option<Time>,
    #[arg(long, env = "CRABKA_CLEANER_INTERVAL", value_parser = crabka_units::parse::positive_time)]
    cleaner_interval: Option<Time>,
    #[arg(long, env = "CRABKA_FUTURE_LOG_MOVE_RETRY_BACKOFF", value_parser = crabka_units::parse::positive_time)]
    future_log_move_retry_backoff: Option<Time>,
    #[arg(long, env = "CRABKA_CLIENT_METRICS_EVICTION_TICK", value_parser = crabka_units::parse::positive_time)]
    client_metrics_eviction_tick: Option<Time>,
    #[arg(long, env = "CRABKA_CLIENT_METRICS_STALE_FLOOR", value_parser = crabka_units::parse::positive_time)]
    client_metrics_stale_floor: Option<Time>,
    #[arg(long, env = "CRABKA_CLIENT_METRICS_DEFAULT_INTERVAL", value_parser = crabka_units::parse::positive_time)]
    client_metrics_default_interval: Option<Time>,
    #[arg(long, env = "CRABKA_CLIENT_METRICS_TELEMETRY_MAX", value_parser = crabka_units::parse::positive_byte_size)]
    client_metrics_telemetry_max: Option<ByteSize>,
    #[arg(long, env = "CRABKA_CLIENT_METRICS_PROM_SNAPSHOT_TTL", value_parser = crabka_units::parse::positive_time)]
    client_metrics_prom_snapshot_ttl: Option<Time>,
    #[arg(long, env = "CRABKA_RLMM_RECONCILE_TICK", value_parser = crabka_units::parse::positive_time)]
    rlmm_reconcile_tick: Option<Time>,
    #[arg(long, env = "CRABKA_RLMM_BOOTSTRAP_BACKOFF_INITIAL", value_parser = crabka_units::parse::positive_time)]
    rlmm_bootstrap_backoff_initial: Option<Time>,
    #[arg(long, env = "CRABKA_RLMM_BOOTSTRAP_BACKOFF_MAX", value_parser = crabka_units::parse::positive_time)]
    rlmm_bootstrap_backoff_max: Option<Time>,
    #[arg(long, env = "CRABKA_CONNECTION_CREATION_THROTTLE_MAX", value_parser = crabka_units::parse::positive_time)]
    connection_creation_throttle_max: Option<Time>,
    #[arg(long, env = "CRABKA_OPA_HTTP_TIMEOUT", value_parser = crabka_units::parse::positive_time)]
    opa_http_timeout: Option<Time>,
    #[arg(long, env = "CRABKA_OAUTH_JWKS_HTTP_TIMEOUT", value_parser = crabka_units::parse::positive_time)]
    oauth_jwks_http_timeout: Option<Time>,
    #[arg(long, env = "CRABKA_AUTO_JOIN_RETRY_BACKOFF", value_parser = crabka_units::parse::positive_time)]
    auto_join_retry_backoff: Option<Time>,
    #[arg(long, env = "CRABKA_AUTO_JOIN_VOTER_REQUEST_TIMEOUT", value_parser = crabka_units::parse::positive_time)]
    auto_join_voter_request_timeout: Option<Time>,
    #[arg(long, env = "CRABKA_REPLICATION_FETCH_MAX", value_parser = crabka_units::parse::positive_byte_size)]
    replication_fetch_max: Option<ByteSize>,
    #[arg(long, env = "CRABKA_REPLICATION_FETCH_MAX_WAIT", value_parser = crabka_units::parse::positive_time)]
    replication_fetch_max_wait: Option<Time>,
    #[arg(long, env = "CRABKA_REPLICATION_FETCH_MIN", value_parser = crabka_units::parse::positive_byte_size)]
    replication_fetch_min: Option<ByteSize>,
    #[arg(long, env = "CRABKA_REPLICATION_THROTTLE_EXHAUSTED_BACKOFF", value_parser = crabka_units::parse::positive_time)]
    replication_throttle_exhausted_backoff: Option<Time>,
    #[arg(long, env = "CRABKA_REPLICATION_SEND_ERROR_BACKOFF", value_parser = crabka_units::parse::positive_time)]
    replication_send_error_backoff: Option<Time>,
    #[arg(long, env = "CRABKA_REPLICATION_UNKNOWN_TOPIC_RETRY_DELAY", value_parser = crabka_units::parse::positive_time)]
    replication_unknown_topic_retry_delay: Option<Time>,
    #[arg(long, env = "CRABKA_REPLICATION_EPOCH_FENCE_BACKOFF", value_parser = crabka_units::parse::positive_time)]
    replication_epoch_fence_backoff: Option<Time>,
    #[arg(long, env = "CRABKA_REPLICATION_UNEXPECTED_ERROR_BACKOFF", value_parser = crabka_units::parse::positive_time)]
    replication_unexpected_error_backoff: Option<Time>,
    #[arg(long, env = "CRABKA_REPLICATION_RECONNECT_INITIAL_DELAY", value_parser = crabka_units::parse::positive_time)]
    replication_reconnect_initial_delay: Option<Time>,
    #[arg(long, env = "CRABKA_REPLICATION_RECONNECT_DELAY_CAP", value_parser = crabka_units::parse::positive_time)]
    replication_reconnect_delay_cap: Option<Time>,
    #[arg(long, env = "CRABKA_COORDINATOR_SESSION_EXPIRY_TICK", value_parser = crabka_units::parse::positive_time)]
    coordinator_session_expiry_tick: Option<Time>,
    #[arg(long, env = "CRABKA_COORDINATOR_SHUTDOWN_ACK_TIMEOUT", value_parser = crabka_units::parse::positive_time)]
    coordinator_shutdown_ack_timeout: Option<Time>,
    #[arg(long, env = "CRABKA_CONSUMER_GROUP_SESSION_TIMEOUT", value_parser = crabka_units::parse::positive_time)]
    consumer_group_session_timeout: Option<Time>,
    #[arg(long, env = "CRABKA_CONSUMER_GROUP_HEARTBEAT_INTERVAL", value_parser = crabka_units::parse::positive_time)]
    consumer_group_heartbeat_interval: Option<Time>,
    #[arg(long, env = "CRABKA_CONSUMER_GROUP_MIN_SESSION_TIMEOUT", value_parser = crabka_units::parse::positive_time)]
    consumer_group_min_session_timeout: Option<Time>,
    #[arg(long, env = "CRABKA_CONSUMER_GROUP_MAX_SESSION_TIMEOUT", value_parser = crabka_units::parse::positive_time)]
    consumer_group_max_session_timeout: Option<Time>,
    #[arg(long, env = "CRABKA_CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL", value_parser = crabka_units::parse::positive_time)]
    consumer_group_min_heartbeat_interval: Option<Time>,
    #[arg(long, env = "CRABKA_CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL", value_parser = crabka_units::parse::positive_time)]
    consumer_group_max_heartbeat_interval: Option<Time>,
    #[arg(long, env = "CRABKA_CONSUMER_GROUP_MAX_SIZE", value_parser = parse_positive_count)]
    consumer_group_max_size: Option<PositiveCount>,
    #[arg(long, env = "CRABKA_CLASSIC_GROUP_INITIAL_REBALANCE_DELAY", value_parser = crabka_units::parse::positive_time)]
    classic_group_initial_rebalance_delay: Option<Time>,
    #[arg(long, env = "CRABKA_SYNC_GROUP_FOLLOWER_WAIT", value_parser = crabka_units::parse::positive_time)]
    sync_group_follower_wait: Option<Time>,
    #[arg(long, env = "CRABKA_UNCLEAN_RECOVERY_AGGRESSIVE_DEADLINE", value_parser = crabka_units::parse::positive_time)]
    unclean_recovery_aggressive_deadline: Option<Time>,
    #[arg(long, env = "CRABKA_UNCLEAN_RECOVERY_BALANCED_DEADLINE", value_parser = crabka_units::parse::positive_time)]
    unclean_recovery_balanced_deadline: Option<Time>,
    #[arg(long, env = "CRABKA_OPERATOR_RECOVERY_DEADLINE", value_parser = crabka_units::parse::positive_time)]
    operator_recovery_deadline: Option<Time>,
    #[arg(long, env = "CRABKA_QUOTA_THROTTLE_MAX", value_parser = crabka_units::parse::positive_time)]
    quota_throttle_max: Option<Time>,
    #[arg(long, env = "CRABKA_SELF_REGISTRATION_MAX_ATTEMPTS", value_parser = clap::value_parser!(u32).range(1..))]
    self_registration_max_attempts: Option<u32>,
    #[arg(long, env = "CRABKA_OBSERVER_FETCH_MAX", value_parser = crabka_units::parse::positive_byte_size)]
    observer_fetch_max: Option<ByteSize>,
    #[arg(long, env = "CRABKA_AUDIT_EVENT_QUEUE_CAPACITY", value_parser = parse_positive_count)]
    audit_event_queue_capacity: Option<PositiveCount>,
    #[arg(long, env = "CRABKA_AUDIT_TAIL_WINDOW_OFFSETS", value_parser = parse_positive_i64)]
    audit_tail_window_offsets: Option<PositiveI64>,
    #[arg(long, env = "CRABKA_AUDIT_TAIL_READ_MAX", value_parser = crabka_units::parse::positive_byte_size)]
    audit_tail_read_max: Option<ByteSize>,
    #[arg(long, env = "CRABKA_OFFSETS_TOPIC_METADATA_WAIT_TIMEOUT", value_parser = crabka_units::parse::positive_time)]
    offsets_topic_metadata_wait_timeout: Option<Time>,
    #[arg(long, env = "CRABKA_CLIENT_METRICS_STALE_PUSH_INTERVALS", value_parser = clap::value_parser!(u32).range(1..))]
    client_metrics_stale_push_intervals: Option<u32>,
    #[arg(long, env = "CRABKA_CLIENT_METRICS_OTLP_QUEUE_CAPACITY", value_parser = parse_positive_count)]
    client_metrics_otlp_queue_capacity: Option<PositiveCount>,
    #[arg(long, env = "CRABKA_COORDINATOR_ACTOR_MAILBOX_CAPACITY", value_parser = parse_positive_count)]
    coordinator_actor_mailbox_capacity: Option<PositiveCount>,
    #[arg(long, env = "CRABKA_UNCLEAN_RECOVERY_QUEUE_CAPACITY", value_parser = parse_positive_count)]
    unclean_recovery_queue_capacity: Option<PositiveCount>,
    #[arg(long, env = "CRABKA_SHARE_RECOVERY_READ_MAX", value_parser = crabka_units::parse::positive_byte_size)]
    share_recovery_read_max: Option<ByteSize>,
    #[arg(long, env = "CRABKA_SHARE_SESSION_CACHE_MAX_WHEN_UNLIMITED", value_parser = parse_positive_count)]
    share_session_cache_max_when_unlimited: Option<PositiveCount>,
    #[arg(long, env = "CRABKA_LOG_READ_BUFFER_CAP", value_parser = crabka_units::parse::positive_byte_size)]
    log_read_buffer_cap: Option<ByteSize>,
    #[arg(long, env = "CRABKA_LOG_TIMESTAMP_SCAN_WINDOW", value_parser = crabka_units::parse::positive_byte_size)]
    log_timestamp_scan_window: Option<ByteSize>,
    #[arg(long, env = "CRABKA_SOCKET_REQUEST_MAX", value_parser = crabka_units::parse::positive_byte_size)]
    socket_request_max: Option<ByteSize>,
    #[arg(long, env = "CRABKA_SENDFILE_MIN", value_parser = crabka_units::parse::positive_byte_size)]
    sendfile_min: Option<ByteSize>,
    #[arg(long, env = "CRABKA_SOCKET_SEND_BUFFER", value_parser = crabka_units::parse::positive_byte_size)]
    socket_send_buffer: Option<ByteSize>,
    #[arg(long, env = "CRABKA_SOCKET_RECEIVE_BUFFER", value_parser = crabka_units::parse::positive_byte_size)]
    socket_receive_buffer: Option<ByteSize>,
    #[arg(long, env = "CRABKA_ACL_MAX_PRINCIPAL", value_parser = crabka_units::parse::positive_byte_size)]
    acl_max_principal: Option<ByteSize>,
    #[arg(long, env = "CRABKA_ACL_MAX_RESOURCE_NAME", value_parser = crabka_units::parse::positive_byte_size)]
    acl_max_resource_name: Option<ByteSize>,
    #[arg(long, env = "CRABKA_TELEMETRY_MAX_DECOMPRESSION_RATIO", value_parser = crabka_units::parse::ratio)]
    telemetry_max_decompression_ratio: Option<Ratio>,
    #[arg(long, env = "CRABKA_TELEMETRY_DECOMPRESSED_OUTPUT_FLOOR", value_parser = crabka_units::parse::positive_byte_size)]
    telemetry_decompressed_output_floor: Option<ByteSize>,
    #[arg(long, env = "CRABKA_TELEMETRY_DECOMPRESSED_OUTPUT_CEILING", value_parser = crabka_units::parse::positive_byte_size)]
    telemetry_decompressed_output_ceiling: Option<ByteSize>,
    #[arg(long, env = "CRABKA_RECORD_DECOMPRESSION_MAX_RATIO", value_parser = crabka_units::parse::positive_ratio)]
    record_decompression_max_ratio: Option<Ratio>,
    #[arg(long, env = "CRABKA_RECORD_DECOMPRESSION_OUTPUT_FLOOR", value_parser = crabka_units::parse::positive_byte_size)]
    record_decompression_output_floor: Option<ByteSize>,
    #[arg(long, env = "CRABKA_RECORD_DECOMPRESSION_OUTPUT_CEILING", value_parser = crabka_units::parse::positive_byte_size)]
    record_decompression_output_ceiling: Option<ByteSize>,
    #[arg(long, env = "CRABKA_INTER_BROKER_SERVER_NAME")]
    inter_broker_server_name: Option<String>,
    #[arg(long, env = "CRABKA_PRODUCER_ID_EXPIRATION", value_parser = crabka_units::parse::positive_time)]
    producer_id_expiration: Option<Time>,
    #[arg(long, env = "CRABKA_PRODUCER_ID_EXPIRATION_SCAN_INTERVAL", value_parser = crabka_units::parse::positive_time)]
    producer_id_expiration_scan_interval: Option<Time>,
    #[arg(long, env = "CRABKA_MAX_PRODUCE_GROUP", value_parser = parse_positive_count)]
    max_produce_group: Option<PositiveCount>,
    #[arg(long, env = "CRABKA_PARTITION_WRITER_QUEUE_DEPTH", value_parser = parse_positive_count)]
    partition_writer_queue_depth: Option<PositiveCount>,
    #[arg(long, env = "CRABKA_DEFAULT_MIN_INSYNC_REPLICAS", value_parser = parse_positive_i32)]
    default_min_insync_replicas: Option<PositiveI32>,
    #[arg(long, env = "CRABKA_FUTURE_LOG_MOVE_READ_CHUNK", value_parser = crabka_units::parse::positive_byte_size)]
    future_log_move_read_chunk: Option<ByteSize>,
    #[arg(long, env = "CRABKA_SHARE_STATE_NUM_PARTITIONS", value_parser = parse_positive_i32)]
    share_state_num_partitions: Option<PositiveI32>,
    #[arg(long, env = "CRABKA_SHARE_STATE_REPLICATION_FACTOR", value_parser = parse_positive_i16)]
    share_state_replication_factor: Option<PositiveI16>,
    #[arg(long, env = "CRABKA_TRANSACTION_STATE_NUM_PARTITIONS", value_parser = parse_positive_i32)]
    transaction_state_num_partitions: Option<PositiveI32>,
    #[arg(long, env = "CRABKA_TRANSACTION_RECOVERY_READ_MAX", value_parser = crabka_units::parse::positive_byte_size)]
    transaction_recovery_read_max: Option<ByteSize>,
    #[arg(long, env = "CRABKA_TRANSACTION_STATE_REPLICATION_FACTOR", value_parser = parse_positive_i16)]
    transaction_state_replication_factor: Option<PositiveI16>,
    #[arg(long, env = "CRABKA_TRANSACTION_MIN_TIMEOUT", value_parser = crabka_units::parse::positive_time)]
    transaction_min_timeout: Option<Time>,
    #[arg(long, env = "CRABKA_TRANSACTION_MAX_TIMEOUT", value_parser = crabka_units::parse::positive_time)]
    transaction_max_timeout: Option<Time>,

    #[arg(long, env = "CRABKA_SHARE_GROUP_ENABLE", action = clap::ArgAction::Set)]
    share_group_enable: Option<bool>,
    #[arg(long, env = "CRABKA_SHARE_GROUP_SESSION_TIMEOUT", value_parser = crabka_units::parse::positive_time)]
    share_group_session_timeout: Option<Time>,
    #[arg(long, env = "CRABKA_SHARE_GROUP_HEARTBEAT_INTERVAL", value_parser = crabka_units::parse::positive_time)]
    share_group_heartbeat_interval: Option<Time>,
    #[arg(long, env = "CRABKA_SHARE_GROUP_RECORD_LOCK_DURATION", value_parser = crabka_units::parse::positive_time)]
    share_group_record_lock_duration: Option<Time>,
    #[arg(long, env = "CRABKA_SHARE_GROUP_MAX_DELIVERY_ATTEMPTS", value_parser = clap::value_parser!(i16).range(1..))]
    share_group_max_delivery_attempts: Option<i16>,
    #[arg(long, env = "CRABKA_SHARE_GROUP_MAX_INFLIGHT_RECORDS", value_parser = parse_positive_i32)]
    share_group_max_inflight_records: Option<PositiveI32>,
    #[arg(long, env = "CRABKA_SHARE_GROUP_ISOLATION_LEVEL", value_parser = parse_share_isolation)]
    share_group_isolation_level:
        Option<crabka_broker::coordinator::unified::share::config::ShareIsolationLevel>,
    #[arg(long, env = "CRABKA_STREAMS_GROUP_SESSION_TIMEOUT", value_parser = crabka_units::parse::positive_time)]
    streams_group_session_timeout: Option<Time>,
    #[arg(long, env = "CRABKA_STREAMS_GROUP_HEARTBEAT_INTERVAL", value_parser = crabka_units::parse::positive_time)]
    streams_group_heartbeat_interval: Option<Time>,
    #[arg(long, env = "CRABKA_STREAMS_INTERNAL_TOPIC_REPLICATION_FACTOR", value_parser = parse_positive_i16)]
    streams_internal_topic_replication_factor: Option<PositiveI16>,
    #[arg(long, env = "CRABKA_STREAMS_GROUP_NUM_STANDBY_REPLICAS", value_parser = clap::value_parser!(i32).range(0..))]
    streams_group_num_standby_replicas: Option<i32>,
    #[arg(long, env = "CRABKA_STREAMS_GROUP_NUM_WARMUP_REPLICAS", value_parser = clap::value_parser!(i32).range(0..))]
    streams_group_num_warmup_replicas: Option<i32>,
    #[arg(long, env = "CRABKA_STREAMS_GROUP_ACCEPTABLE_RECOVERY_LAG", value_parser = clap::value_parser!(i64).range(0..))]
    streams_group_acceptable_recovery_lag: Option<i64>,
    #[arg(long, env = "CRABKA_STREAMS_GROUP_TASK_OFFSET_INTERVAL", value_parser = crabka_units::parse::positive_time)]
    streams_group_task_offset_interval: Option<Time>,
    #[arg(long, env = "CRABKA_STREAMS_GROUP_ASSIGNOR", value_parser = parse_streams_assignor)]
    streams_group_assignor:
        Option<crabka_broker::coordinator::unified::streams::config::StreamsAssignorKind>,
}

macro_rules! copy_refined_runtime {
    ($source:ident, $target:ident, $($field:ident),+ $(,)?) => {
        $(
            $target.$field = $source.$field.map(|value| value.into_value());
        )+
    };
}

macro_rules! copy_plain_runtime {
    ($source:ident, $target:ident, $($field:ident),+ $(,)?) => {
        $(
            $target.$field = $source.$field;
        )+
    };
}

impl RuntimeArgs {
    fn copy_core(&self, runtime: &mut crabka_broker::file_config::RuntimeFileConfig) {
        copy_plain_runtime!(
            self,
            runtime,
            startup_leader_wait_timeout,
            self_registration_backoff_min,
            self_registration_backoff_max,
            observer_poll_interval,
            audit_spool_replay_interval,
            audit_stats_poll_interval,
            audit_partition_wait_timeout,
            liveness_tick_interval,
            gauge_poll_interval,
            isr_scan_interval,
            cleaner_interval,
            future_log_move_retry_backoff,
            rlmm_reconcile_tick,
            rlmm_bootstrap_backoff_initial,
            rlmm_bootstrap_backoff_max,
            connection_creation_throttle_max,
            opa_http_timeout,
            oauth_jwks_http_timeout,
            auto_join_retry_backoff,
            auto_join_voter_request_timeout,
            self_registration_max_attempts,
            observer_fetch_max,
        );
    }

    fn copy_client_metrics(&self, runtime: &mut crabka_broker::file_config::RuntimeFileConfig) {
        copy_plain_runtime!(
            self,
            runtime,
            client_metrics_eviction_tick,
            client_metrics_stale_floor,
            client_metrics_default_interval,
            client_metrics_telemetry_max,
            client_metrics_prom_snapshot_ttl,
            client_metrics_stale_push_intervals,
        );
        copy_refined_runtime!(self, runtime, client_metrics_otlp_queue_capacity,);
    }

    fn copy_replication(&self, runtime: &mut crabka_broker::file_config::RuntimeFileConfig) {
        copy_plain_runtime!(
            self,
            runtime,
            replication_fetch_max_wait,
            replication_fetch_max,
            replication_fetch_min,
            replication_throttle_exhausted_backoff,
            replication_send_error_backoff,
            replication_unknown_topic_retry_delay,
            replication_epoch_fence_backoff,
            replication_unexpected_error_backoff,
            replication_reconnect_initial_delay,
            replication_reconnect_delay_cap,
        );
    }

    fn copy_coordinators(&self, runtime: &mut crabka_broker::file_config::RuntimeFileConfig) {
        copy_plain_runtime!(
            self,
            runtime,
            coordinator_session_expiry_tick,
            coordinator_shutdown_ack_timeout,
            consumer_group_session_timeout,
            consumer_group_heartbeat_interval,
            consumer_group_min_session_timeout,
            consumer_group_max_session_timeout,
            consumer_group_min_heartbeat_interval,
            consumer_group_max_heartbeat_interval,
            classic_group_initial_rebalance_delay,
            sync_group_follower_wait,
            share_recovery_read_max,
        );
        copy_refined_runtime!(
            self,
            runtime,
            consumer_group_max_size,
            coordinator_actor_mailbox_capacity,
            share_session_cache_max_when_unlimited,
            share_state_num_partitions,
            share_state_replication_factor,
        );
    }

    fn copy_storage_and_queues(&self, runtime: &mut crabka_broker::file_config::RuntimeFileConfig) {
        copy_plain_runtime!(
            self,
            runtime,
            unclean_recovery_aggressive_deadline,
            unclean_recovery_balanced_deadline,
            operator_recovery_deadline,
            quota_throttle_max,
            offsets_topic_metadata_wait_timeout,
            producer_id_expiration,
            producer_id_expiration_scan_interval,
            transaction_min_timeout,
            transaction_max_timeout,
            audit_tail_read_max,
            future_log_move_read_chunk,
            transaction_recovery_read_max,
        );
        copy_refined_runtime!(
            self,
            runtime,
            audit_event_queue_capacity,
            audit_tail_window_offsets,
            unclean_recovery_queue_capacity,
            max_produce_group,
            partition_writer_queue_depth,
            default_min_insync_replicas,
            transaction_state_num_partitions,
            transaction_state_replication_factor,
        );
    }

    fn copy_network_and_limits(&self, runtime: &mut crabka_broker::file_config::RuntimeFileConfig) {
        copy_plain_runtime!(
            self,
            runtime,
            socket_request_max,
            sendfile_min,
            socket_send_buffer,
            socket_receive_buffer,
            log_read_buffer_cap,
            log_timestamp_scan_window,
            acl_max_principal,
            acl_max_resource_name,
            telemetry_max_decompression_ratio,
            telemetry_decompressed_output_floor,
            telemetry_decompressed_output_ceiling,
            record_decompression_max_ratio,
            record_decompression_output_floor,
            record_decompression_output_ceiling,
        );
        runtime
            .inter_broker_server_name
            .clone_from(&self.inter_broker_server_name);
    }

    fn copy_group_protocols(&self, runtime: &mut crabka_broker::file_config::RuntimeFileConfig) {
        copy_plain_runtime!(
            self,
            runtime,
            share_group_session_timeout,
            share_group_heartbeat_interval,
            share_group_record_lock_duration,
            streams_group_session_timeout,
            streams_group_heartbeat_interval,
            streams_group_task_offset_interval,
        );
        copy_refined_runtime!(
            self,
            runtime,
            share_group_max_inflight_records,
            streams_internal_topic_replication_factor,
        );
        copy_plain_runtime!(
            self,
            runtime,
            share_group_enable,
            share_group_max_delivery_attempts,
            streams_group_num_standby_replicas,
            streams_group_num_warmup_replicas,
            streams_group_acceptable_recovery_lag,
        );
        runtime.share_group_isolation_level = self.share_group_isolation_level.map(|value| {
            use crabka_broker::coordinator::unified::share::config::ShareIsolationLevel;
            match value {
                ShareIsolationLevel::ReadUncommitted => "read-uncommitted",
                ShareIsolationLevel::ReadCommitted => "read-committed",
            }
            .to_owned()
        });
        runtime.streams_group_assignor = self.streams_group_assignor.map(|value| {
            use crabka_broker::coordinator::unified::streams::config::StreamsAssignorKind;
            match value {
                StreamsAssignorKind::Auto => "auto",
                StreamsAssignorKind::Sticky => "sticky",
                StreamsAssignorKind::HighlyAvailable => "highly-available",
            }
            .to_owned()
        });
    }

    fn as_file_runtime(&self) -> crabka_broker::file_config::RuntimeFileConfig {
        let mut runtime = crabka_broker::file_config::RuntimeFileConfig::default();
        self.copy_core(&mut runtime);
        self.copy_client_metrics(&mut runtime);
        self.copy_replication(&mut runtime);
        self.copy_coordinators(&mut runtime);
        self.copy_storage_and_queues(&mut runtime);
        self.copy_network_and_limits(&mut runtime);
        self.copy_group_protocols(&mut runtime);
        runtime
    }
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

    #[command(flatten)]
    profiling: crabka_telemetry::profiling::ProfilingConfig,

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

    /// Partition disk-usage scan cadence. `0s` disables the scanner entirely.
    /// The rebalancer's usage scraper
    /// reads the `partition_disk_bytes` gauge this populates.
    #[arg(long, env = "CRABKA_PARTITION_DISK_SCAN_INTERVAL", value_parser = crabka_units::parse::non_negative_time)]
    partition_disk_scan_interval: Option<Time>,

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
    #[arg(long, env = "CRABKA_OBSERVER_LAG_BOUND")]
    observer_lag_bound: Option<u64>,

    /// Broker heartbeat interval in milliseconds.
    #[arg(
        long,
        env = "CRABKA_HEARTBEAT_INTERVAL",
        value_parser = crabka_units::parse::positive_time
    )]
    heartbeat_interval: Option<Time>,

    /// Broker heartbeat timeout in milliseconds.
    #[arg(
        long,
        env = "CRABKA_HEARTBEAT_TIMEOUT",
        value_parser = crabka_units::parse::positive_time
    )]
    heartbeat_timeout: Option<Time>,

    /// Follower lag timeout in milliseconds before ISR shrink.
    #[arg(
        long,
        env = "CRABKA_REPLICA_LAG_TIME_MAX",
        value_parser = crabka_units::parse::positive_time
    )]
    replica_lag_time_max: Option<Time>,

    /// Controller election timeout in milliseconds.
    #[arg(
        long,
        env = "CRABKA_CONTROLLER_ELECTION_TIMEOUT",
        value_parser = crabka_units::parse::positive_time
    )]
    controller_election_timeout: Option<Time>,

    /// Controller heartbeat interval in milliseconds.
    #[arg(
        long,
        env = "CRABKA_CONTROLLER_HEARTBEAT_INTERVAL",
        value_parser = crabka_units::parse::positive_time
    )]
    controller_heartbeat_interval: Option<Time>,

    /// Consecutive controller fetch misses tolerated before election.
    #[arg(
        long,
        env = "CRABKA_CONTROLLER_FETCH_MISS_LIMIT",
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    controller_fetch_miss_limit: Option<u32>,

    /// Capacity of the metadata Raft command queue.
    #[arg(
        long,
        env = "CRABKA_METADATA_RAFT_COMMAND_QUEUE_CAPACITY",
        value_parser = parse_metadata_raft_command_queue_capacity
    )]
    metadata_raft_command_queue_capacity: Option<usize>,

    /// Per-read and per-snapshot-request metadata Raft byte budget.
    #[arg(
        long,
        env = "CRABKA_METADATA_RAFT_FETCH_MAX",
        value_parser = crabka_units::parse::positive_byte_size
    )]
    metadata_raft_fetch_max: Option<ByteSize>,

    /// Controlled-shutdown leadership drain timeout in milliseconds.
    #[arg(
        long,
        env = "CRABKA_CONTROLLED_SHUTDOWN_DRAIN_TIMEOUT",
        value_parser = crabka_units::parse::positive_time
    )]
    controlled_shutdown_drain_timeout: Option<Time>,

    /// Maximum bytes between metadata-log snapshots.
    #[arg(
        long,
        env = "CRABKA_METADATA_MAX_BETWEEN_SNAPSHOTS",
        value_parser = crabka_units::parse::positive_byte_size
    )]
    metadata_max_between_snapshots: Option<ByteSize>,

    /// Maximum time between metadata-log snapshots; `0s` disables the interval cap.
    #[arg(long, env = "CRABKA_METADATA_MAX_SNAPSHOT_INTERVAL", value_parser = crabka_units::parse::non_negative_time)]
    metadata_max_snapshot_interval: Option<Time>,

    /// Committed-record gap between metadata-log snapshots.
    #[arg(
        long,
        env = "CRABKA_METADATA_SNAPSHOT_INTERVAL_RECORDS",
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    metadata_snapshot_interval_records: Option<u64>,

    /// Maximum metadata snapshot size a follower will fetch.
    #[arg(
        long,
        env = "CRABKA_METADATA_SNAPSHOT_FETCH_MAX",
        value_parser = crabka_units::parse::positive_byte_size
    )]
    metadata_snapshot_fetch_max: Option<ByteSize>,

    /// Idle-transaction abort cleanup interval; `0s` disables the reaper.
    #[arg(long, env = "CRABKA_TXN_ABORT_CLEANUP_INTERVAL", value_parser = crabka_units::parse::non_negative_time)]
    txn_abort_cleanup_interval: Option<Time>,

    /// Auto preferred-replica election scan cadence.
    #[arg(
        long,
        env = "CRABKA_LEADER_IMBALANCE_CHECK_INTERVAL",
        value_parser = crabka_units::parse::positive_time
    )]
    leader_imbalance_check_interval: Option<Time>,

    /// Minimum per-broker leader imbalance percentage before auto-rebalance acts.
    #[arg(
        long,
        env = "CRABKA_LEADER_IMBALANCE_PER_BROKER",
        value_parser = crabka_units::parse::ratio
    )]
    leader_imbalance_per_broker: Option<Ratio>,

    /// TLS cert/key reload polling interval; `0s` disables the watcher.
    #[arg(long, env = "CRABKA_TLS_RELOAD_INTERVAL", value_parser = crabka_units::parse::non_negative_time)]
    tls_reload_interval: Option<Time>,

    /// Maximum incremental fetch-session cache slots.
    #[arg(long, env = "CRABKA_MAX_INCREMENTAL_FETCH_SESSION_CACHE_SLOTS")]
    max_incremental_fetch_session_cache_slots: Option<usize>,

    /// Maximum live broker connections across all listeners.
    #[arg(long, env = "CRABKA_MAX_CONNECTIONS")]
    max_connections: Option<usize>,

    /// Maximum live broker connections from any single client IP.
    #[arg(long, env = "CRABKA_MAX_CONNECTIONS_PER_IP")]
    max_connections_per_ip: Option<usize>,

    /// Delegation-token maximum lifetime.
    #[arg(
        long,
        env = "CRABKA_DELEGATION_TOKEN_MAX_LIFETIME",
        value_parser = crabka_units::parse::positive_time
    )]
    delegation_token_max_lifetime: Option<Time>,

    /// Delegation-token expiry sweep interval.
    #[arg(
        long,
        env = "CRABKA_DELEGATION_TOKEN_EXPIRY_CHECK_INTERVAL",
        value_parser = crabka_units::parse::positive_time
    )]
    delegation_token_expiry_check_interval: Option<Time>,

    /// Delegation-token default renew period.
    #[arg(
        long,
        env = "CRABKA_DELEGATION_TOKEN_RENEW_PERIOD",
        value_parser = crabka_units::parse::positive_time
    )]
    delegation_token_default_renew_period: Option<Time>,

    /// `RemoteLogManager` copy/retention cadence in milliseconds.
    #[arg(
        long,
        env = "CRABKA_REMOTE_LOG_MANAGER_INTERVAL",
        value_parser = crabka_units::parse::positive_time
    )]
    remote_log_manager_interval: Option<Time>,

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

    /// CRABKA-specific OTLP timeout.
    #[arg(long, env = "CRABKA_OTLP_TIMEOUT", value_parser = crabka_units::parse::non_negative_time)]
    crabka_otlp_timeout: Option<Time>,

    /// OpenTelemetry exporter timeout in seconds.
    #[arg(long, env = "OTEL_EXPORTER_OTLP_TIMEOUT_SECS")]
    otel_exporter_otlp_timeout_secs: Option<String>,

    /// OTLP heartbeat interval; `0s` disables heartbeats.
    #[arg(long, env = "CRABKA_OTLP_HEARTBEAT_INTERVAL", value_parser = crabka_units::parse::non_negative_time)]
    crabka_otlp_heartbeat_interval: Option<Time>,
}

impl Args {
    fn runtime_overlay(&self) -> crabka_broker::file_config::RuntimeFileConfig {
        let mut runtime = self.runtime.as_file_runtime();
        copy_plain_runtime!(
            self,
            runtime,
            partition_disk_scan_interval,
            observer_lag_bound,
            metadata_max_between_snapshots,
            metadata_max_snapshot_interval,
            metadata_snapshot_interval_records,
            metadata_snapshot_fetch_max,
            txn_abort_cleanup_interval,
            leader_imbalance_check_interval,
            leader_imbalance_per_broker,
            tls_reload_interval,
            heartbeat_interval,
            heartbeat_timeout,
            replica_lag_time_max,
            controller_election_timeout,
            controller_heartbeat_interval,
            controller_fetch_miss_limit,
            metadata_raft_command_queue_capacity,
            metadata_raft_fetch_max,
            controlled_shutdown_drain_timeout,
            delegation_token_max_lifetime,
            delegation_token_expiry_check_interval,
            delegation_token_default_renew_period,
            remote_log_manager_interval,
            max_incremental_fetch_session_cache_slots,
            max_connections,
            max_connections_per_ip,
        );
        runtime
    }

    fn apply_runtime_to(
        &self,
        cfg: &mut BrokerConfig,
        file_shutdown: Option<Time>,
    ) -> Result<Time, String> {
        let runtime = self.runtime_overlay();
        let cli_shutdown = runtime.controlled_shutdown_drain_timeout;
        runtime.apply_to(cfg).map_err(|error| error.to_string())?;
        cfg.client_dispatch_queue_capacity =
            ConnectionDispatchQueueCapacity::new(self.runtime.client_dispatch_queue_capacity)
                .expect("validated by clap");
        cfg.client_frame_max =
            ClientFrameMax::try_from(self.runtime.client_frame_max).expect("validated by clap");
        cfg.validate().map_err(|error| error.to_string())?;
        Ok(cli_shutdown
            .or(file_shutdown)
            .unwrap_or(DEFAULT_CONTROLLED_SHUTDOWN_DRAIN_TIMEOUT))
    }

    fn base_broker_config(
        &mut self,
        advertised_listener: String,
        controller_listen_addr: SocketAddr,
        node_id: u64,
        metrics_listen_addr: Option<SocketAddr>,
        client_metrics_otlp_endpoint: Option<String>,
    ) -> BrokerConfig {
        BrokerConfig {
            broker_id: self.broker_id,
            listen_addr: self.listen_addr,
            advertised_listener,
            log_dir: std::mem::take(&mut self.log_dir),
            extra_log_dirs: std::mem::take(&mut self.extra_log_dirs),
            log_config: LogConfig::default(),
            node_id: crabka_broker::NodeId(node_id),
            controller_listen_addr,
            controller_quorum_voters: vec![(
                crabka_broker::NodeId(node_id),
                controller_listen_addr.to_string(),
            )],
            bootstrap_servers: std::mem::take(&mut self.controller_bootstrap_servers),
            directory_id: uuid::Uuid::nil(),
            auto_join: self.controller_auto_join,
            bootstrap_mode: BootstrapMode::Bootstrap,
            cluster_id: self.cluster_id.take(),
            metrics_listen_addr,
            profiling: self.profiling.clone(),
            client_metrics_otlp_endpoint,
            delegation_token_secret_key: self
                .delegation_token_secret_key
                .take()
                .map(|key| crabka_security::SecretBytes::new(key.into_bytes())),
            ..BrokerConfig::default()
        }
    }

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
            "CRABKA_OTLP_TIMEOUT" => self
                .crabka_otlp_timeout
                .map(|value| value.human().to_string()),
            "OTEL_EXPORTER_OTLP_TIMEOUT_SECS" => self.otel_exporter_otlp_timeout_secs.clone(),
            "CRABKA_OTLP_HEARTBEAT_INTERVAL" => self
                .crabka_otlp_heartbeat_interval
                .map(|value| value.human().to_string()),
            _ => None,
        }
    }
}

fn parse_client_dispatch_queue_capacity(value: &str) -> Result<usize, String> {
    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    ConnectionDispatchQueueCapacity::new(value).map(ConnectionDispatchQueueCapacity::get)
}

fn parse_metadata_raft_command_queue_capacity(value: &str) -> Result<usize, String> {
    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    crabka_raft::MetadataRaftCommandQueueCapacity::new(value)
        .map(crabka_raft::MetadataRaftCommandQueueCapacity::get)
}

fn parse_client_frame_max(value: &str) -> Result<ByteSize, String> {
    let value =
        crabka_units::parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    ClientFrameMax::try_from(value).map(ClientFrameMax::size)
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
    )?;
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
    let file_shutdown_timeout = file_config
        .as_ref()
        .and_then(|file| file.runtime.as_ref())
        .and_then(|runtime| runtime.controlled_shutdown_drain_timeout);
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
    let mut config = args.base_broker_config(
        advertised,
        controller_addr,
        node_id,
        metrics_listen_addr,
        client_metrics_otlp_endpoint,
    );
    if let Some(roles) = roles {
        config.roles = roles;
    }
    if let Some(fc) = file_config {
        fc.apply_before_runtime_overlay(&mut config)?;
    }
    let controlled_shutdown_drain_timeout =
        args.apply_runtime_to(&mut config, file_shutdown_timeout)?;
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
        .controlled_shutdown(controlled_shutdown_drain_timeout.to_std())
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
    use crabka_units::secs;
    use tempfile::tempdir;

    use super::*;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[test]
    fn profiling_policy_reads_environment_and_cli_wins() {
        let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
        let _guard = lock.lock().expect("environment lock");

        let defaults = Args::try_parse_from(["crabka-broker"]).expect("parse defaults");
        assert!(defaults.profiling == crabka_telemetry::profiling::ProfilingConfig::default());

        temp_env::with_vars(
            [
                ("CRABKA_PROFILING_CPU_DEFAULT_DURATION", Some("2s")),
                ("CRABKA_PROFILING_CPU_SAMPLE_FREQUENCY", Some("101Hz")),
            ],
            || {
                let args = Args::try_parse_from([
                    "crabka-broker",
                    "--profiling-cpu-default-duration=3s",
                    "--profiling-cpu-sample-frequency=103Hz",
                ])
                .expect("parse profiling overrides");
                assert!(args.profiling.profiling_cpu_default_duration == secs(3));
                assert!(
                    args.profiling.profiling_cpu_sample_frequency.frequency()
                        == crabka_units::per_sec(103)
                );
            },
        );
    }

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
            (vec!["crabka-broker", "--cleaner-interval=0ms"], false),
            (vec!["crabka-broker", "--cleaner-interval=1ms"], true),
            (
                vec![
                    "crabka-broker",
                    "--streams-internal-topic-replication-factor=0",
                ],
                false,
            ),
            (
                vec![
                    "crabka-broker",
                    "--streams-internal-topic-replication-factor=1",
                ],
                true,
            ),
            (vec!["crabka-broker", "--replication-fetch-min=0B"], false),
            (vec!["crabka-broker", "--replication-fetch-min=1B"], true),
            (
                vec!["crabka-broker", "--metadata-snapshot-fetch-max=0B"],
                false,
            ),
            (
                vec!["crabka-broker", "--metadata-snapshot-fetch-max=512MiB"],
                true,
            ),
            (
                vec!["crabka-broker", "--record-decompression-max-ratio=0"],
                false,
            ),
            (
                vec!["crabka-broker", "--record-decompression-max-ratio=50"],
                true,
            ),
            (
                vec!["crabka-broker", "--record-decompression-output-floor=0B"],
                false,
            ),
            (
                vec!["crabka-broker", "--client-dispatch-queue-capacity=0"],
                false,
            ),
            (vec!["crabka-broker", "--client-frame-max=101MiB"], false),
            (
                vec![
                    "crabka-broker",
                    "--client-dispatch-queue-capacity=7",
                    "--client-frame-max=32KiB",
                ],
                true,
            ),
            (
                vec![
                    "crabka-broker",
                    "--record-decompression-output-ceiling=512MiB",
                ],
                true,
            ),
        ];

        for (args, accepted) in cases {
            assert!(Args::try_parse_from(args).is_ok() == accepted);
        }

        let args = Args::try_parse_from(["crabka-broker", "--leader-imbalance-per-broker=101%"])
            .expect("parse ratio");
        assert!(
            args.apply_runtime_to(&mut BrokerConfig::default(), None)
                .is_err()
        );

        let args = Args::try_parse_from(["crabka-broker", "--record-decompression-max-ratio=101"])
            .expect("parse positive ratio");
        assert!(
            args.apply_runtime_to(&mut BrokerConfig::default(), None)
                .is_err()
        );

        let args = Args::try_parse_from(["crabka-broker", "--leader-imbalance-per-broker=100%"])
            .expect("parse ratio");
        assert!(
            args.apply_runtime_to(&mut BrokerConfig::default(), None)
                .is_ok()
        );

        let args =
            Args::try_parse_from(["crabka-broker", "--metadata-snapshot-fetch-max=1073741825B"])
                .expect("parse dimensioned over-ceiling size");
        assert!(
            args.apply_runtime_to(&mut BrokerConfig::default(), None)
                .is_err()
        );
    }

    #[test]
    fn runtime_policy_cli_reads_crabka_environment() {
        let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
        let _guard = lock.lock().expect("environment lock");

        temp_env::with_vars(
            [
                ("CRABKA_CLEANER_INTERVAL", Some("17ms")),
                ("CRABKA_SOCKET_REQUEST_MAX", Some("100MiB")),
                ("CRABKA_LEADER_IMBALANCE_PER_BROKER", Some("10%")),
                ("CRABKA_METADATA_SNAPSHOT_FETCH_MAX", Some("512MiB")),
                ("CRABKA_CONTROLLER_HEARTBEAT_INTERVAL", Some("500ms")),
                ("CRABKA_CONTROLLER_FETCH_MISS_LIMIT", Some("7")),
                ("CRABKA_METADATA_RAFT_COMMAND_QUEUE_CAPACITY", Some("512")),
                ("CRABKA_METADATA_RAFT_FETCH_MAX", Some("4MiB")),
                ("CRABKA_RECORD_DECOMPRESSION_MAX_RATIO", Some("50")),
                ("CRABKA_RECORD_DECOMPRESSION_OUTPUT_FLOOR", Some("8MiB")),
                ("CRABKA_RECORD_DECOMPRESSION_OUTPUT_CEILING", Some("512MiB")),
                ("CRABKA_LOG_READ_BUFFER_CAP", Some("2MiB")),
                ("CRABKA_LOG_TIMESTAMP_SCAN_WINDOW", Some("32KiB")),
                ("CRABKA_TRANSACTION_RECOVERY_READ_MAX", Some("3MiB")),
                ("CRABKA_BROKER_CLIENT_DISPATCH_QUEUE_CAPACITY", Some("7")),
                ("CRABKA_BROKER_CLIENT_FRAME_MAX", Some("32KiB")),
            ],
            || {
                let args = Args::try_parse_from(["crabka-broker"]).expect("parse environment");
                assert!(args.runtime.cleaner_interval == Some(Time::from_millis(17)));
                assert!(args.runtime.socket_request_max == Some(crabka_units::mebibytes(100)));
                assert!(args.leader_imbalance_per_broker == Some(crabka_units::fraction(0.1)));
                assert!(args.metadata_snapshot_fetch_max == Some(crabka_units::mebibytes(512)));
                assert!(args.controller_fetch_miss_limit == Some(7));
                assert!(args.metadata_raft_command_queue_capacity == Some(512));
                assert!(args.metadata_raft_fetch_max == Some(crabka_units::mebibytes(4)));
                assert!(
                    args.runtime.record_decompression_max_ratio
                        == Some(crabka_units::fraction(50.0))
                );
                let mut config = BrokerConfig::default();
                args.apply_runtime_to(&mut config, None)
                    .expect("apply environment runtime");
                assert!(config.metadata_snapshot_fetch_max == crabka_units::mebibytes(512));
                assert!(config.controller_heartbeat_interval_explicit);
                assert!(config.controller_heartbeat_interval == crabka_units::millis(500));
                assert!(config.controller_fetch_miss_limit.get() == 7);
                assert!(config.metadata_raft_command_queue_capacity.get() == 512);
                assert!(config.metadata_raft_fetch_max.bytes() == 4 * 1024 * 1024);
                assert!(
                    config.record_decompression_policy().unwrap().output_floor()
                        == crabka_units::mebibytes(8)
                );
                assert!(config.log_config.read_buffer_cap == crabka_units::mebibytes(2));
                assert!(config.log_config.timestamp_scan_window == crabka_units::kibibytes(32));
                assert!(config.transaction_recovery_read_max == crabka_units::mebibytes(3));
                assert!(config.client_dispatch_queue_capacity.get() == 7);
                assert!(config.client_frame_max.size() == crabka_units::kibibytes(32));
            },
        );
    }

    #[test]
    fn client_resource_policy_defaults_and_cli_precedence() {
        let defaults = Args::try_parse_from(["crabka-broker"]).expect("parse defaults");
        let mut config = BrokerConfig::default();
        defaults
            .apply_runtime_to(&mut config, None)
            .expect("apply defaults");
        assert!(config.client_dispatch_queue_capacity.get() == 64);
        assert!(config.client_frame_max.size() == crabka_units::mebibytes(100));

        let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
        let _guard = lock.lock().expect("environment lock");
        temp_env::with_vars(
            [
                ("CRABKA_BROKER_CLIENT_DISPATCH_QUEUE_CAPACITY", Some("7")),
                ("CRABKA_BROKER_CLIENT_FRAME_MAX", Some("32KiB")),
            ],
            || {
                let args = Args::try_parse_from([
                    "crabka-broker",
                    "--client-dispatch-queue-capacity=9",
                    "--client-frame-max=64KiB",
                ])
                .expect("parse CLI overrides");
                let mut config = BrokerConfig::default();
                args.apply_runtime_to(&mut config, None)
                    .expect("apply CLI overrides");
                assert!(config.client_dispatch_queue_capacity.get() == 9);
                assert!(config.client_frame_max.size() == crabka_units::kibibytes(64));
            },
        );
    }

    #[test]
    fn otlp_time_cli_values_override_environment() {
        let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
        let _guard = lock.lock().expect("environment lock");

        temp_env::with_vars(
            [
                ("CRABKA_OTLP_TIMEOUT", Some("17s")),
                ("CRABKA_OTLP_HEARTBEAT_INTERVAL", Some("19s")),
            ],
            || {
                let args = Args::try_parse_from([
                    "crabka-broker",
                    "--crabka-otlp-timeout=23s",
                    "--crabka-otlp-heartbeat-interval=29s",
                ])
                .expect("parse CLI OTLP overrides");
                assert!(
                    (
                        args.telemetry_value("CRABKA_OTLP_TIMEOUT"),
                        args.telemetry_value("CRABKA_OTLP_HEARTBEAT_INTERVAL"),
                    ) == (Some("23s".to_owned()), Some("29s".to_owned()))
                );
            },
        );
    }

    fn file_runtime_with_nondefault_values() -> crabka_broker::file_config::FileConfig {
        toml::from_str(
            r#"
            [runtime]
            cleaner_interval = "7s"
            controlled_shutdown_drain_timeout = "9s"
            auto_join_voter_request_timeout = "9s"
            share_state_replication_factor = 2
            transaction_state_replication_factor = 2
            streams_internal_topic_replication_factor = 2
            "#,
        )
        .expect("parse runtime file config")
    }

    #[test]
    fn explicit_cli_default_runtime_values_override_file() {
        let args = Args::try_parse_from([
            "crabka-broker",
            "--cleaner-interval=30s",
            "--controlled-shutdown-drain-timeout=20s",
            "--auto-join-voter-request-timeout=30s",
            "--share-state-replication-factor=3",
            "--transaction-state-replication-factor=3",
            "--streams-internal-topic-replication-factor=3",
        ])
        .expect("parse explicit CLI defaults");
        let mut config = BrokerConfig::default();
        let file = file_runtime_with_nondefault_values();
        let file_shutdown = file
            .runtime
            .as_ref()
            .and_then(|runtime| runtime.controlled_shutdown_drain_timeout);
        file.apply_to(&mut config).expect("apply file runtime");

        let shutdown = args
            .apply_runtime_to(&mut config, file_shutdown)
            .expect("overlay CLI runtime");

        assert!(
            (
                config.cleaner_interval,
                shutdown,
                config.auto_join_voter_request_timeout,
                config.share_coordinator.state_topic_replication_factor,
                config.transaction_state_replication_factor,
                config.streams_group.internal_topic_replication_factor,
            ) == (secs(30), secs(20), secs(30), 3, 3, 3)
        );
    }

    #[test]
    fn explicit_env_default_runtime_values_override_file() {
        let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
        let _guard = lock.lock().expect("environment lock");

        temp_env::with_vars(
            [
                ("CRABKA_CLEANER_INTERVAL", Some("30s")),
                ("CRABKA_CONTROLLED_SHUTDOWN_DRAIN_TIMEOUT", Some("20s")),
                ("CRABKA_AUTO_JOIN_VOTER_REQUEST_TIMEOUT", Some("30s")),
                ("CRABKA_SHARE_STATE_REPLICATION_FACTOR", Some("3")),
                ("CRABKA_TRANSACTION_STATE_REPLICATION_FACTOR", Some("3")),
                (
                    "CRABKA_STREAMS_INTERNAL_TOPIC_REPLICATION_FACTOR",
                    Some("3"),
                ),
            ],
            || {
                let args = Args::try_parse_from(["crabka-broker"]).expect("parse env defaults");
                let mut config = BrokerConfig::default();
                let file = file_runtime_with_nondefault_values();
                let file_shutdown = file
                    .runtime
                    .as_ref()
                    .and_then(|runtime| runtime.controlled_shutdown_drain_timeout);
                file.apply_to(&mut config).expect("apply file runtime");

                let shutdown = args
                    .apply_runtime_to(&mut config, file_shutdown)
                    .expect("overlay env runtime");

                assert!(
                    (
                        config.cleaner_interval,
                        shutdown,
                        config.auto_join_voter_request_timeout,
                        config.share_coordinator.state_topic_replication_factor,
                        config.transaction_state_replication_factor,
                        config.streams_group.internal_topic_replication_factor,
                    ) == (secs(30), secs(20), secs(30), 3, 3, 3)
                );
            },
        );
    }
}
