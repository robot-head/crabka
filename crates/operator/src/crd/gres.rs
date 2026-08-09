//! `Gres` CRD.
//!
//! One `Gres` is one PgDog-backed Gres front door for a Kafka cluster.
//! Tenant CRs point at a `Gres` fleet by name. A later batch adds the
//! controller that renders `PgDog`.

use std::time::Duration;

use crabka_client_producer::ProducerFlushTimeout;
use crabka_gres_control::{
    CheckpointPartBytes, DEFAULT_CHECKPOINT_DELETE_RECORDS_TIMEOUT,
    DEFAULT_CHECKPOINT_POLL_INTERVAL, DEFAULT_IDLE_SUSPEND_POLL_INTERVAL,
    DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL, DEFAULT_RANGE0_FOLLOWER_REBUILD_BACKOFF_CEILING,
    DEFAULT_RANGE0_FOLLOWER_REBUILD_BACKOFF_FLOOR, PgdogConnectAttempts, PgdogPoolerMode,
    PositiveI32, PositiveMillis, PositiveUsize,
};
use crabka_gres_substrate::{
    DEFAULT_CHECKPOINT_RETAIN, DEFAULT_DURABLE_INSPECTION_FOLD_MAX_RECORDS,
    DEFAULT_DURABLE_INSPECTION_FOLD_MAX_SIZE, DEFAULT_DURABLE_INSPECTION_TIMEOUT,
    DEFAULT_MAX_FRAME_SIZE, DEFAULT_PART_MAX_SIZE, DEFAULT_WAL_ADMIN_CONNECT_TIMEOUT,
    DEFAULT_WAL_ADMIN_REQUEST_TIMEOUT, DEFAULT_WAL_RECOVERY_CONNECT_TIMEOUT,
    DEFAULT_WAL_RECOVERY_DNS_TIMEOUT, DEFAULT_WAL_RECOVERY_EMPTY_FETCH_RETRIES,
    DEFAULT_WAL_RECOVERY_FETCH_MAX_WAIT, DEFAULT_WAL_RECOVERY_FETCH_PARTITION_MAX,
    DEFAULT_WAL_RECOVERY_FETCH_RESPONSE_MAX, DEFAULT_WAL_RECOVERY_REQUEST_TIMEOUT,
    DEFAULT_WAL_TOPIC_ENSURE_TIMEOUT, DEFAULT_WAL_TOPIC_REPLICATION_FACTOR,
};
use crabka_units::{
    ByteSize, Ratio, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
    gibibytes, mebibytes, percent,
};
use kube::CustomResource;
use refined_type::rule::{GreaterI32, GreaterU64, GreaterUsize};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[cfg(test)]
use crate::controller::common::millis_u64;
use crate::crd::kafka::Tracing;

const DEFAULT_LIFECYCLE_REQUEUE: Time = crabka_units::secs(5);

fn whole_millis(name: &str, value: Time) -> Result<u64, String> {
    let millis = value.millis_i64();
    if value.secs_f64().is_finite()
        && millis > 0
        && Time::from_millis(millis) == value
        && let Ok(millis) = u64::try_from(millis)
    {
        Ok(millis)
    } else {
        Err(format!(
            "{name}: must be finite, positive, and a whole number of milliseconds"
        ))
    }
}

fn whole_millis_i32(name: &str, value: Time) -> Result<i32, String> {
    let millis = whole_millis(name, value)?;
    i32::try_from(millis).map_err(|_| format!("{name}: must be within 1ms..=2147483647ms"))
}

fn nonnegative_whole_millis_i32(name: &str, value: Time) -> Result<i32, String> {
    let millis = value.millis_i64();
    if value.secs_f64().is_finite()
        && millis >= 0
        && millis <= i64::from(i32::MAX)
        && Time::from_millis(millis) == value
    {
        Ok(i32::try_from(millis).expect("range checked"))
    } else {
        Err(format!(
            "{name}: must be a whole number of milliseconds within 0ms..=2147483647ms"
        ))
    }
}

fn whole_bytes_u64(name: &str, value: ByteSize) -> Result<u64, String> {
    let bytes = value.bytes_u64();
    if bytes > 0 && ByteSize::from_bytes(bytes) == value {
        Ok(bytes)
    } else {
        Err(format!(
            "{name}: must be a finite, positive whole number of bytes"
        ))
    }
}

fn whole_bytes_i32(name: &str, value: ByteSize) -> Result<i32, String> {
    i32::try_from(whole_bytes_u64(name, value)?)
        .map_err(|_| format!("{name}: must not exceed i32::MAX bytes"))
}

fn whole_bytes_usize(name: &str, value: ByteSize) -> Result<usize, String> {
    usize::try_from(whole_bytes_u64(name, value)?)
        .map_err(|_| format!("{name}: must not exceed usize::MAX bytes"))
}

/// Gres fleet specification.
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[kube(
    group = "crabka.io",
    version = "v1alpha1",
    kind = "Gres",
    plural = "greses",
    singular = "gres",
    shortname = "gg",
    namespaced,
    status = "GresStatus",
    derive = "PartialEq"
)]
#[serde(rename_all = "camelCase")]
pub struct GresSpec {
    /// Kafka cluster name this Gres fleet targets.
    pub kafka_cluster: String,

    /// `PgDog` front-door deployment settings.
    pub pgdog: PgdogSpec,

    /// Wake activator deployment and runtime policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activator: Option<GresActivatorSpec>,

    /// Tenant compute workload policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compute: Option<GresComputeSpec>,

    /// Default tenant runtime settings. Each `GresTenant` inherits them
    /// unless it sets `spec.overrides`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defaults: Option<TenantDefaults>,

    /// Dry-run Gres balancer planning knobs. The operator does not yet do
    /// live execution, and this is on purpose.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub balancer: Option<GresBalancerSpec>,

    /// Distributed-tracing wiring for this fleet's tenant compute pods.
    ///
    /// When this field is set, the `GresTenant` reconciler renders the
    /// `CRABKA_OTLP_*` and `OTEL_SERVICE_NAME` env contract on every
    /// compute container, and `crabka-gres` installs the OTLP exporter at
    /// startup. When the field is absent, the reconciler writes no OTLP env
    /// var at all. That is what keeps tracing off. An empty endpoint would
    /// still switch the exporter on, and the exporter would then fail to
    /// reach a collector.
    ///
    /// This field uses the schema of `Kafka.spec.tracing`. The two fleets
    /// therefore have one shape, one validation path, and the field names
    /// that an operator already knows.
    ///
    /// The field is fleet-scoped and not per-`GresTenant`, and this is on
    /// purpose. The collector endpoint, the protocol, and the export
    /// timeout are cluster infrastructure. A tenant-writable copy would
    /// make telemetry routing a tenant decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracing: Option<Tracing>,
}

/// Wake activator deployment and runtime policy.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GresActivatorSpec {
    /// Container image override. When absent, the operator uses its global
    /// activator image override or its compiled default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1))]
    pub image: Option<String>,

    /// Number of activator replicas.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub replicas: Option<i32>,

    /// Registry readiness polling interval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub registry_poll: Option<Time>,

    /// Maximum duration to hold one cold-starting connection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub cold_start_timeout: Option<Time>,

    /// Activator readiness probe period in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub readiness_probe_period_seconds: Option<i32>,

    /// Kafka client request-dispatch queue capacity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub client_dispatch_queue_capacity: Option<usize>,

    /// Maximum accepted Kafka client frame size.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_byte_size"
    )]
    #[schemars(with = "Option<String>")]
    pub client_frame_max: Option<ByteSize>,
}

impl GresActivatorSpec {
    pub(crate) fn client_resource_policy(
        &self,
    ) -> Result<
        (
            Option<crabka_client_core::ConnectionDispatchQueueCapacity>,
            Option<crabka_client_core::ClientFrameMax>,
        ),
        String,
    > {
        let queue = self
            .client_dispatch_queue_capacity
            .map(crabka_client_core::ConnectionDispatchQueueCapacity::new)
            .transpose()
            .map_err(|error| format!("spec.activator.clientDispatchQueueCapacity: {error}"))?;
        let frame = self
            .client_frame_max
            .map(crabka_client_core::ClientFrameMax::try_from)
            .transpose()
            .map_err(|error| format!("spec.activator.clientFrameMax: {error}"))?;
        Ok((queue, frame))
    }
}

/// Compression codec for the Gres WAL producer.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum WalProducerCompression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl From<WalProducerCompression> for crabka_client_producer::Compression {
    fn from(value: WalProducerCompression) -> Self {
        match value {
            WalProducerCompression::None => Self::None,
            WalProducerCompression::Gzip => Self::Gzip,
            WalProducerCompression::Snappy => Self::Snappy,
            WalProducerCompression::Lz4 => Self::Lz4,
            WalProducerCompression::Zstd => Self::Zstd,
        }
    }
}

/// Tenant compute workload policy.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GresComputeSpec {
    /// Compute readiness probe period in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub readiness_probe_period_seconds: Option<i32>,

    /// Kafka client request-dispatch queue capacity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub client_dispatch_queue_capacity: Option<usize>,

    /// Maximum accepted Kafka client frame size.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_byte_size"
    )]
    #[schemars(with = "Option<String>")]
    pub client_frame_max: Option<ByteSize>,

    /// Maximum accepted `PostgreSQL` frontend message size.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_byte_size"
    )]
    #[schemars(with = "Option<String>")]
    pub pgwire_max_message_size: Option<ByteSize>,

    /// Memory retained by one blocking query operator.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<String>")]
    pub pgexec_blocking_query_memory: Option<ByteSize>,

    /// Maximum encoded size of one result page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<String>")]
    pub pgexec_result_page_max: Option<ByteSize>,

    /// Largest estimated join input eligible for broadcast.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<String>")]
    pub pgexec_join_broadcast_threshold: Option<ByteSize>,

    /// Per-session LISTEN/NOTIFY queue capacity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub pgexec_notify_queue_capacity: Option<usize>,

    /// Durable XID reservation size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub pgexec_xid_reservation: Option<u64>,

    /// Durable internal row-ID reservation size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub pgexec_rowid_reservation: Option<u64>,

    /// Maximum timestamp versions pruned per written row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub pgexec_ts_prune_versions_per_row: Option<usize>,

    /// Lag retained behind the timestamp GC floor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub pgexec_ts_gc_floor_lag: Option<Time>,

    /// Minimum response size for FDW Kafka fetches.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_byte_size"
    )]
    #[schemars(with = "Option<String>")]
    pub fdw_fetch_min: Option<ByteSize>,

    /// Maximum time a broker may hold one FDW fetch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub fdw_fetch_max_wait: Option<Time>,

    /// Maximum bytes returned for one FDW partition fetch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<String>")]
    pub fdw_fetch_partition_max: Option<ByteSize>,

    /// FDW broker TCP connection timeout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub fdw_connect_timeout: Option<Time>,

    /// FDW broker request timeout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub fdw_request_timeout: Option<Time>,

    /// Total deadline for resolving a cold FDW writer schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub fdw_schema_fetch_timeout: Option<Time>,

    /// Poll cadence while awaiting a cold FDW writer schema.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub fdw_schema_fetch_poll: Option<Time>,

    /// Minimum response size for committed-WAL recovery fetches.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_byte_size"
    )]
    #[schemars(with = "Option<String>")]
    pub wal_recovery_fetch_min: Option<ByteSize>,

    /// Maximum checkpoint object part size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<String>")]
    pub checkpoint_part_size: Option<ByteSize>,

    /// Number of checkpoint manifests to retain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub checkpoint_retain: Option<usize>,

    /// Kafka `DeleteRecords` timeout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub checkpoint_delete_records_timeout: Option<Time>,

    /// Checkpoint threshold polling interval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub checkpoint_poll_interval: Option<Time>,

    /// Idle-suspend polling interval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub idle_suspend_poll_interval: Option<Time>,

    /// Periodic range-0 follower refresh cadence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub range0_follower_poll_interval: Option<Time>,

    /// Initial delay before retrying consecutive range-0 follower rebuilds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub range0_follower_rebuild_backoff_floor: Option<Time>,

    /// Maximum delay between consecutive range-0 follower rebuilds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub range0_follower_rebuild_backoff_ceiling: Option<Time>,

    /// Maximum distributed join key columns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub range_join_key_columns: Option<usize>,

    /// Maximum distributed join projection columns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub range_join_projection_columns: Option<usize>,

    /// Maximum predicates per distributed join side.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub range_join_predicates: Option<usize>,

    /// Maximum active XIDs in each distributed join snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub range_join_snapshot_xids: Option<usize>,

    /// Maximum materialized broadcast rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub range_join_broadcast_rows: Option<usize>,

    /// Maximum encoded distributed join row size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<String>")]
    pub range_join_row_max: Option<ByteSize>,

    /// Maximum distributed join result rows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub range_join_result_rows: Option<usize>,

    /// Maximum encoded range RPC frame size.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_byte_size"
    )]
    #[schemars(with = "Option<String>")]
    pub range_rpc_frame_max: Option<ByteSize>,

    /// Deadline for one range RPC request.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub range_rpc_request_timeout: Option<Time>,

    /// Range RPC server connection idle timeout.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub range_rpc_server_idle_timeout: Option<Time>,

    /// Range RPC client-pool idle connection lifetime.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub range_rpc_pool_idle_ttl: Option<Time>,

    /// Maximum idle range RPC connections retained per endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub range_rpc_pool_max_idle_per_endpoint: Option<usize>,

    /// Hosted remote-session idle retention.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub range_remote_session_idle: Option<Time>,

    /// Maximum hosted remote sessions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub range_remote_session_max: Option<usize>,

    /// Range-0 catch-up wait timeout.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub range0_wait_timeout: Option<Time>,

    /// Whole-reply budget for range-0 barriers.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub range0_barrier_reply_budget: Option<Time>,

    /// Lock-wait cap for cross-range transactions.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub range_cross_range_lock_wait_cap: Option<Time>,

    /// Durable range-inspection record ceiling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub range_durable_inspect_max_records: Option<u32>,

    /// Durable range-inspection byte ceiling.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_byte_size"
    )]
    #[schemars(with = "Option<String>")]
    pub range_durable_inspect_max_size: Option<ByteSize>,

    /// Decision-release lag retry count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub range_decision_release_lag_retries: Option<u32>,

    /// Decision-release retry backoff.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub range_decision_release_retry_backoff: Option<Time>,

    /// Timestamp-oracle heartbeat cadence.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub range_tso_heartbeat_interval: Option<Time>,

    /// Minimum interval between logical horizon persists.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub range_logical_min_persist_interval: Option<Time>,

    /// Initial logical horizon persistence stride.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub range_logical_base_persist_stride: Option<u64>,

    /// Maximum adaptive logical horizon persistence stride.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub range_logical_max_persist_stride: Option<u64>,

    /// Wall-clock headroom persisted by the HLC oracle.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub range_hlc_horizon_headroom: Option<Time>,

    /// Deadline for one durable record inspection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub durable_inspection_timeout: Option<Time>,

    /// Maximum records materialized by one durable inspection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub durable_inspection_fold_max_records: Option<usize>,

    /// Maximum data materialized by one durable inspection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<String>")]
    pub durable_inspection_fold_max_size: Option<ByteSize>,

    /// Timeout for resolving Kafka broker hostnames used by the FDW.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub fdw_broker_dns_timeout: Option<Time>,

    /// Initial delay before retrying a transient Schema Registry fetch failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub schema_fetch_retry_initial_backoff: Option<Time>,

    /// Maximum delay between transient Schema Registry fetch retries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub schema_fetch_retry_max_backoff: Option<Time>,

    /// Kafka broker long-poll wait for committed-WAL recovery fetches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub wal_recovery_fetch_max_wait: Option<Time>,

    /// Per-partition size limit for committed-WAL recovery fetches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<String>")]
    pub wal_recovery_fetch_partition_max: Option<ByteSize>,

    /// Whole-response size limit for committed-WAL recovery fetches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<String>")]
    pub wal_recovery_fetch_response_max: Option<ByteSize>,

    /// Consecutive empty-fetch retries after the first empty recovery fetch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub wal_recovery_empty_fetch_retries: Option<usize>,

    /// Timeout for resolving committed-WAL recovery broker hostnames.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub wal_recovery_dns_timeout: Option<Time>,

    /// Timeout for opening committed-WAL recovery broker connections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub wal_recovery_connect_timeout: Option<Time>,

    /// Timeout for committed-WAL recovery broker requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub wal_recovery_request_timeout: Option<Time>,

    /// Deadline for flushing all buffered and in-flight WAL records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub wal_producer_flush_timeout: Option<Time>,

    /// Timeout for resolving WAL producer broker hostnames.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub wal_producer_dns_timeout: Option<Time>,

    /// Timeout for WAL producer broker requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub wal_producer_request_timeout: Option<Time>,

    /// WAL producer retries after the initial batch send.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub wal_producer_retries: Option<i32>,

    /// WAL producer retry and producer-ID initial backoff.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub wal_producer_retry_backoff: Option<Time>,

    /// Per-batch WAL producer routing retry budget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub wal_producer_routing_retry_budget: Option<Time>,

    /// WAL producer-ID initialization retry timeout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub wal_producer_init_retry_timeout: Option<Time>,

    /// WAL producer-ID initialization backoff cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub wal_producer_init_max_backoff: Option<Time>,

    /// WAL producer transaction timeout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub wal_producer_transaction_timeout: Option<Time>,

    /// WAL producer compression codec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wal_producer_compression: Option<WalProducerCompression>,

    /// Delay before sending a partial WAL producer batch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub wal_producer_linger: Option<Time>,

    /// Maximum WAL producer batch size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<String>")]
    pub wal_producer_batch: Option<ByteSize>,

    /// Target maximum size of one encoded logical WAL frame.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<String>")]
    pub wal_frame_max_size: Option<ByteSize>,

    /// Maximum active memtable size for each on-disk substrate cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<String>")]
    pub pgkv_max_memtable_size: Option<ByteSize>,

    /// Committed operations between requested substrate-cache memtable rotations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub pgkv_rotate_after_ops: Option<u64>,

    /// Replication factor requested when creating a range WAL topic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = 32_767))]
    pub wal_topic_replication_factor: Option<i32>,

    /// Timeout for ensuring a range WAL topic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub wal_topic_ensure_timeout: Option<Time>,

    /// Timeout for opening WAL admin connections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub wal_admin_connect_timeout: Option<Time>,

    /// Timeout for WAL admin requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub wal_admin_request_timeout: Option<Time>,

    /// Tenant lifecycle reconciliation interval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub lifecycle_requeue: Option<Time>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct EffectiveGresComputePolicy {
    pub(crate) readiness_probe_period_seconds: i32,
    pub(crate) client_dispatch_queue_capacity:
        Option<crabka_client_core::ConnectionDispatchQueueCapacity>,
    pub(crate) client_frame_max: Option<crabka_client_core::ClientFrameMax>,
    pub(crate) pgwire_max_message_size: ByteSize,
    pub(crate) pgexec_runtime_policy: crabka_pgexec::RuntimePolicy,
    pub(crate) registry_reader_fetch_min: Option<crabka_client_core::FetchMinBytes>,
    pub(crate) fdw_fetch_min: Option<crabka_client_core::FetchMinBytes>,
    pub(crate) fdw_fetch_max_wait: Time,
    pub(crate) fdw_fetch_partition_max: ByteSize,
    pub(crate) fdw_connect_timeout: Time,
    pub(crate) fdw_request_timeout: Time,
    pub(crate) fdw_schema_fetch_timeout: Time,
    pub(crate) fdw_schema_fetch_poll: Time,
    pub(crate) wal_recovery_fetch_min: Option<crabka_client_core::FetchMinBytes>,
    pub(crate) checkpoint_part_size: CheckpointPartBytes,
    pub(crate) checkpoint_retain: PositiveUsize,
    pub(crate) checkpoint_delete_records_timeout_ms: PositiveI32,
    pub(crate) checkpoint_poll_interval_ms: PositiveMillis,
    pub(crate) idle_suspend_poll_interval_ms: PositiveMillis,
    pub(crate) range0_follower_poll_interval_ms: PositiveMillis,
    pub(crate) range0_follower_rebuild_backoff_floor_ms: PositiveMillis,
    pub(crate) range0_follower_rebuild_backoff_ceiling_ms: PositiveMillis,
    pub(crate) range_runtime_policy: crabka_gres_ranges::RangeRuntimePolicy,
    pub(crate) durable_inspection_timeout_ms: PositiveMillis,
    pub(crate) durable_inspection_fold_max_records: PositiveUsize,
    pub(crate) durable_inspection_fold_max_size: ByteSize,
    pub(crate) fdw_broker_dns_timeout: crabka_client_core::ClientDnsTimeout,
    pub(crate) schema_fetch_retry_policy: crabka_schema_serde::SchemaFetchRetryPolicy,
    pub(crate) wal_recovery_fetch_max_wait_ms: PositiveI32,
    pub(crate) wal_recovery_fetch_partition_max: PositiveI32,
    pub(crate) wal_recovery_fetch_response_max: PositiveI32,
    pub(crate) wal_recovery_empty_fetch_retries: PositiveUsize,
    pub(crate) wal_recovery_dns_timeout_ms: PositiveMillis,
    pub(crate) wal_recovery_connect_timeout_ms: PositiveMillis,
    pub(crate) wal_recovery_request_timeout_ms: PositiveMillis,
    pub(crate) wal_producer_flush_timeout: ProducerFlushTimeout,
    pub(crate) wal_producer_dns_timeout: crabka_client_core::ClientDnsTimeout,
    pub(crate) wal_producer_retry_policy: crabka_client_producer::ProducerRetryPolicy,
    pub(crate) wal_producer_throughput_policy: crabka_client_producer::ProducerThroughputPolicy,
    pub(crate) wal_frame_max_size: ByteSize,
    pub(crate) pgkv_options: crabka_pgkv::FjallOptions,
    pub(crate) wal_topic_replication_factor: PositiveI32,
    pub(crate) wal_topic_ensure_timeout_ms: PositiveI32,
    pub(crate) wal_admin_connect_timeout_ms: PositiveMillis,
    pub(crate) wal_admin_request_timeout_ms: PositiveMillis,
    pub(crate) lifecycle_requeue_ms: PositiveMillis,
}

impl GresComputeSpec {
    pub(crate) fn effective_readiness_probe_period_seconds(&self) -> Result<i32, String> {
        GreaterI32::<0>::new(self.readiness_probe_period_seconds.unwrap_or(5))
            .map_err(|error| format!("spec.compute.readinessProbePeriodSeconds: {error}"))
            .map(refined_type::Refined::into_value)
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn effective_policy(&self) -> Result<EffectiveGresComputePolicy, String> {
        let range0_follower_rebuild_backoff_floor_ms = PositiveMillis::new(whole_millis(
            "spec.compute.range0FollowerRebuildBackoffFloor",
            self.range0_follower_rebuild_backoff_floor
                .unwrap_or(DEFAULT_RANGE0_FOLLOWER_REBUILD_BACKOFF_FLOOR),
        )?)
        .map_err(|error| format!("spec.compute.range0FollowerRebuildBackoffFloor: {error}"))?;
        let range0_follower_rebuild_backoff_ceiling_ms = PositiveMillis::new(whole_millis(
            "spec.compute.range0FollowerRebuildBackoffCeiling",
            self.range0_follower_rebuild_backoff_ceiling
                .unwrap_or(DEFAULT_RANGE0_FOLLOWER_REBUILD_BACKOFF_CEILING),
        )?)
        .map_err(|error| format!("spec.compute.range0FollowerRebuildBackoffCeiling: {error}"))?;
        if range0_follower_rebuild_backoff_floor_ms.into_value()
            > range0_follower_rebuild_backoff_ceiling_ms.into_value()
        {
            return Err(
                "spec.compute.range0FollowerRebuildBackoffFloor: must not exceed range0FollowerRebuildBackoffCeiling"
                    .to_owned(),
            );
        }
        let durable_inspection_fold_max_size = self
            .durable_inspection_fold_max_size
            .unwrap_or(DEFAULT_DURABLE_INSPECTION_FOLD_MAX_SIZE);
        whole_bytes_usize(
            "spec.compute.durableInspectionFoldMaxSize",
            durable_inspection_fold_max_size,
        )?;
        let pgwire_max_message_size = self
            .pgwire_max_message_size
            .unwrap_or_else(|| mebibytes(64));
        whole_bytes_usize("spec.compute.pgwireMaxMessageSize", pgwire_max_message_size)?;
        let pgexec_defaults = crabka_pgexec::RuntimePolicy::default();
        let pgexec_blocking_query_memory = self
            .pgexec_blocking_query_memory
            .unwrap_or(pgexec_defaults.blocking_query_memory);
        whole_bytes_usize(
            "spec.compute.pgexecBlockingQueryMemory",
            pgexec_blocking_query_memory,
        )?;
        let pgexec_result_page_max = self
            .pgexec_result_page_max
            .unwrap_or(pgexec_defaults.result_page_max);
        whole_bytes_usize("spec.compute.pgexecResultPageMax", pgexec_result_page_max)?;
        let pgexec_join_broadcast_threshold = self
            .pgexec_join_broadcast_threshold
            .unwrap_or(pgexec_defaults.join_broadcast_threshold);
        whole_bytes_usize(
            "spec.compute.pgexecJoinBroadcastThreshold",
            pgexec_join_broadcast_threshold,
        )?;
        let positive_usize = |field: &str, value: usize| {
            GreaterUsize::<0>::new(value)
                .map(refined_type::Refined::into_value)
                .map_err(|error| format!("spec.compute.{field}: {error}"))
        };
        let positive_u64 = |field: &str, value: u64| {
            GreaterU64::<0>::new(value)
                .map(refined_type::Refined::into_value)
                .map_err(|error| format!("spec.compute.{field}: {error}"))
        };
        let pgexec_ts_gc_floor_lag = self
            .pgexec_ts_gc_floor_lag
            .unwrap_or(pgexec_defaults.ts_gc_floor_lag);
        let pgexec_notify_queue_capacity = positive_usize(
            "pgexecNotifyQueueCapacity",
            self.pgexec_notify_queue_capacity
                .unwrap_or(pgexec_defaults.notify_queue_capacity),
        )?;
        let pgexec_xid_reservation = positive_u64(
            "pgexecXidReservation",
            self.pgexec_xid_reservation
                .unwrap_or(pgexec_defaults.xid_reservation),
        )?;
        let pgexec_rowid_reservation = positive_u64(
            "pgexecRowidReservation",
            self.pgexec_rowid_reservation
                .unwrap_or(pgexec_defaults.rowid_reservation),
        )?;
        let pgexec_ts_prune_versions_per_row = positive_usize(
            "pgexecTsPruneVersionsPerRow",
            self.pgexec_ts_prune_versions_per_row
                .unwrap_or(pgexec_defaults.ts_prune_versions_per_row),
        )?;
        let pgexec_runtime_policy = crabka_pgexec::RuntimePolicy {
            blocking_query_memory: pgexec_blocking_query_memory,
            result_page_max: pgexec_result_page_max,
            join_broadcast_threshold: pgexec_join_broadcast_threshold,
            notify_queue_capacity: pgexec_notify_queue_capacity,
            xid_reservation: pgexec_xid_reservation,
            rowid_reservation: pgexec_rowid_reservation,
            ts_prune_versions_per_row: pgexec_ts_prune_versions_per_row,
            ts_gc_floor_lag: pgexec_ts_gc_floor_lag,
        }
        .validate()
        .map_err(|error| format!("spec.compute.pgexecTsGcFloorLag: {error:?}"))?;
        let wal_frame_max_size = self.wal_frame_max_size.unwrap_or(DEFAULT_MAX_FRAME_SIZE);
        whole_bytes_usize("spec.compute.walFrameMaxSize", wal_frame_max_size)?;
        let pgkv_defaults = crabka_pgkv::FjallOptions::default();
        let pgkv_options = crabka_pgkv::FjallOptions::new(
            self.pgkv_max_memtable_size
                .unwrap_or(pgkv_defaults.max_memtable_size()),
            self.pgkv_rotate_after_ops
                .unwrap_or(pgkv_defaults.rotate_after_ops().get()),
        )
        .map_err(|error| format!("spec.compute.pgkv: {error}"))?;
        let schema_fetch_retry_defaults = crabka_schema_serde::SchemaFetchRetryPolicy::default();
        let range_defaults = crabka_gres_ranges::RangeRuntimePolicy::default();
        let range_join_row_max = self.range_join_row_max.unwrap_or_else(|| {
            crabka_units::ByteSize::from_bytes(
                u64::try_from(range_defaults.join.row_bytes).expect("compiled default fits u64"),
            )
        });
        let range_join_row_bytes =
            whole_bytes_usize("spec.compute.rangeJoinRowMax", range_join_row_max)?;
        let range_runtime_policy = crabka_gres_ranges::RangeRuntimePolicy {
            join: crabka_pgexec::scanner::JoinPolicy {
                key_columns: positive_usize(
                    "spec.compute.rangeJoinKeyColumns",
                    self.range_join_key_columns
                        .unwrap_or(range_defaults.join.key_columns),
                )?,
                projection_columns: positive_usize(
                    "spec.compute.rangeJoinProjectionColumns",
                    self.range_join_projection_columns
                        .unwrap_or(range_defaults.join.projection_columns),
                )?,
                predicates: positive_usize(
                    "spec.compute.rangeJoinPredicates",
                    self.range_join_predicates
                        .unwrap_or(range_defaults.join.predicates),
                )?,
                snapshot_xids: positive_usize(
                    "spec.compute.rangeJoinSnapshotXids",
                    self.range_join_snapshot_xids
                        .unwrap_or(range_defaults.join.snapshot_xids),
                )?,
                broadcast_rows: positive_usize(
                    "spec.compute.rangeJoinBroadcastRows",
                    self.range_join_broadcast_rows
                        .unwrap_or(range_defaults.join.broadcast_rows),
                )?,
                row_bytes: range_join_row_bytes,
                result_rows: positive_usize(
                    "spec.compute.rangeJoinResultRows",
                    self.range_join_result_rows
                        .unwrap_or(range_defaults.join.result_rows),
                )?,
            },
            rpc_frame_max: self
                .range_rpc_frame_max
                .unwrap_or(range_defaults.rpc_frame_max),
            rpc_request_timeout: self
                .range_rpc_request_timeout
                .unwrap_or(range_defaults.rpc_request_timeout),
            rpc_server_idle_timeout: self
                .range_rpc_server_idle_timeout
                .unwrap_or(range_defaults.rpc_server_idle_timeout),
            rpc_pool_idle_ttl: self
                .range_rpc_pool_idle_ttl
                .unwrap_or(range_defaults.rpc_pool_idle_ttl),
            rpc_pool_max_idle_per_endpoint: crabka_gres_ranges::PositiveUsize::new(
                self.range_rpc_pool_max_idle_per_endpoint
                    .unwrap_or(range_defaults.rpc_pool_max_idle_per_endpoint.get()),
            )
            .map_err(|error| format!("spec.compute.rangeRpcPoolMaxIdlePerEndpoint: {error}"))?,
            remote_session_idle: self
                .range_remote_session_idle
                .unwrap_or(range_defaults.remote_session_idle),
            remote_session_max: crabka_gres_ranges::PositiveUsize::new(
                self.range_remote_session_max
                    .unwrap_or(range_defaults.remote_session_max.get()),
            )
            .map_err(|error| format!("spec.compute.rangeRemoteSessionMax: {error}"))?,
            range0_wait_timeout: self
                .range0_wait_timeout
                .unwrap_or(range_defaults.range0_wait_timeout),
            range0_barrier_reply_budget: self
                .range0_barrier_reply_budget
                .unwrap_or(range_defaults.range0_barrier_reply_budget),
            cross_range_lock_wait_cap: self
                .range_cross_range_lock_wait_cap
                .unwrap_or(range_defaults.cross_range_lock_wait_cap),
            durable_inspect_max_records: crabka_gres_ranges::PositiveU32::new(
                self.range_durable_inspect_max_records
                    .unwrap_or(range_defaults.durable_inspect_max_records.get()),
            )
            .map_err(|error| format!("spec.compute.rangeDurableInspectMaxRecords: {error}"))?,
            durable_inspect_max_size: self
                .range_durable_inspect_max_size
                .unwrap_or(range_defaults.durable_inspect_max_size),
            decision_release_lag_retries: crabka_gres_ranges::PositiveU32::new(
                self.range_decision_release_lag_retries
                    .unwrap_or(range_defaults.decision_release_lag_retries.get()),
            )
            .map_err(|error| format!("spec.compute.rangeDecisionReleaseLagRetries: {error}"))?,
            decision_release_retry_backoff: self
                .range_decision_release_retry_backoff
                .unwrap_or(range_defaults.decision_release_retry_backoff),
            tso_heartbeat_interval: self
                .range_tso_heartbeat_interval
                .unwrap_or(range_defaults.tso_heartbeat_interval),
            logical_min_persist_interval: self
                .range_logical_min_persist_interval
                .unwrap_or(range_defaults.logical_min_persist_interval),
            logical_base_persist_stride: crabka_gres_ranges::PositiveU64::new(
                self.range_logical_base_persist_stride
                    .unwrap_or(range_defaults.logical_base_persist_stride.get()),
            )
            .map_err(|error| format!("spec.compute.rangeLogicalBasePersistStride: {error}"))?,
            logical_max_persist_stride: crabka_gres_ranges::PositiveU64::new(
                self.range_logical_max_persist_stride
                    .unwrap_or(range_defaults.logical_max_persist_stride.get()),
            )
            .map_err(|error| format!("spec.compute.rangeLogicalMaxPersistStride: {error}"))?,
            hlc_horizon_headroom: self
                .range_hlc_horizon_headroom
                .unwrap_or(range_defaults.hlc_horizon_headroom),
        };
        range_runtime_policy
            .validate()
            .map_err(|error| format!("spec.compute range runtime policy: {error}"))?;
        let schema_fetch_retry_policy = crabka_schema_serde::SchemaFetchRetryPolicy::new(
            self.schema_fetch_retry_initial_backoff
                .unwrap_or_else(|| schema_fetch_retry_defaults.initial_backoff()),
            self.schema_fetch_retry_max_backoff
                .unwrap_or_else(|| schema_fetch_retry_defaults.max_backoff()),
        )
        .map_err(|error| {
            let field = if error.starts_with("maximum") {
                "schemaFetchRetryMaxBackoff"
            } else {
                "schemaFetchRetryInitialBackoff"
            };
            format!("spec.compute.{field}: {error}")
        })?;
        let fdw_fetch_max_wait = self.fdw_fetch_max_wait.unwrap_or(crabka_units::secs(5));
        whole_millis_i32("spec.compute.fdwFetchMaxWait", fdw_fetch_max_wait)?;
        let fdw_fetch_partition_max = self
            .fdw_fetch_partition_max
            .unwrap_or_else(|| crabka_units::mebibytes(10));
        whole_bytes_i32("spec.compute.fdwFetchPartitionMax", fdw_fetch_partition_max)?;
        let fdw_connect_timeout = self.fdw_connect_timeout.unwrap_or(crabka_units::secs(10));
        whole_millis_i32("spec.compute.fdwConnectTimeout", fdw_connect_timeout)?;
        let fdw_request_timeout = self.fdw_request_timeout.unwrap_or(crabka_units::secs(30));
        whole_millis_i32("spec.compute.fdwRequestTimeout", fdw_request_timeout)?;
        let fdw_schema_fetch_timeout = self
            .fdw_schema_fetch_timeout
            .unwrap_or_else(|| crabka_units::secs(10));
        whole_millis(
            "spec.compute.fdwSchemaFetchTimeout",
            fdw_schema_fetch_timeout,
        )?;
        let fdw_schema_fetch_poll = self
            .fdw_schema_fetch_poll
            .unwrap_or_else(|| crabka_units::millis(20));
        whole_millis("spec.compute.fdwSchemaFetchPoll", fdw_schema_fetch_poll)?;
        if fdw_schema_fetch_poll > fdw_schema_fetch_timeout {
            return Err(
                "spec.compute.fdwSchemaFetchPoll must not exceed fdwSchemaFetchTimeout".to_owned(),
            );
        }

        Ok(EffectiveGresComputePolicy {
            readiness_probe_period_seconds: self.effective_readiness_probe_period_seconds()?,
            client_dispatch_queue_capacity: self
                .client_dispatch_queue_capacity
                .map(crabka_client_core::ConnectionDispatchQueueCapacity::new)
                .transpose()
                .map_err(|error| format!("spec.compute.clientDispatchQueueCapacity: {error}"))?,
            client_frame_max: self
                .client_frame_max
                .map(crabka_client_core::ClientFrameMax::try_from)
                .transpose()
                .map_err(|error| format!("spec.compute.clientFrameMax: {error}"))?,
            pgwire_max_message_size,
            pgexec_runtime_policy,
            registry_reader_fetch_min: None,
            fdw_fetch_min: self
                .fdw_fetch_min
                .map(crabka_client_core::FetchMinBytes::try_from)
                .transpose()
                .map_err(|error| format!("spec.compute.fdwFetchMin: {error}"))?,
            fdw_fetch_max_wait,
            fdw_fetch_partition_max,
            fdw_connect_timeout,
            fdw_request_timeout,
            fdw_schema_fetch_timeout,
            fdw_schema_fetch_poll,
            wal_recovery_fetch_min: self
                .wal_recovery_fetch_min
                .map(crabka_client_core::FetchMinBytes::try_from)
                .transpose()
                .map_err(|error| format!("spec.compute.walRecoveryFetchMin: {error}"))?,
            checkpoint_part_size: CheckpointPartBytes::new(whole_bytes_usize(
                "spec.compute.checkpointPartSize",
                self.checkpoint_part_size.unwrap_or(DEFAULT_PART_MAX_SIZE),
            )?)
            .map_err(|error| format!("spec.compute.checkpointPartSize: {error}"))?,
            checkpoint_retain: PositiveUsize::new(
                self.checkpoint_retain.unwrap_or(DEFAULT_CHECKPOINT_RETAIN),
            )
            .map_err(|error| format!("spec.compute.checkpointRetain: {error}"))?,
            checkpoint_delete_records_timeout_ms: PositiveI32::new(whole_millis_i32(
                "spec.compute.checkpointDeleteRecordsTimeout",
                self.checkpoint_delete_records_timeout
                    .unwrap_or(DEFAULT_CHECKPOINT_DELETE_RECORDS_TIMEOUT),
            )?)
            .map_err(|error| format!("spec.compute.checkpointDeleteRecordsTimeout: {error}"))?,
            checkpoint_poll_interval_ms: PositiveMillis::new(whole_millis(
                "spec.compute.checkpointPollInterval",
                self.checkpoint_poll_interval
                    .unwrap_or(DEFAULT_CHECKPOINT_POLL_INTERVAL),
            )?)
            .map_err(|error| format!("spec.compute.checkpointPollInterval: {error}"))?,
            idle_suspend_poll_interval_ms: PositiveMillis::new(whole_millis(
                "spec.compute.idleSuspendPollInterval",
                self.idle_suspend_poll_interval
                    .unwrap_or(DEFAULT_IDLE_SUSPEND_POLL_INTERVAL),
            )?)
            .map_err(|error| format!("spec.compute.idleSuspendPollInterval: {error}"))?,
            range0_follower_poll_interval_ms: PositiveMillis::new(whole_millis(
                "spec.compute.range0FollowerPollInterval",
                self.range0_follower_poll_interval
                    .unwrap_or(DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL),
            )?)
            .map_err(|error| format!("spec.compute.range0FollowerPollInterval: {error}"))?,
            range0_follower_rebuild_backoff_floor_ms,
            range0_follower_rebuild_backoff_ceiling_ms,
            range_runtime_policy,
            durable_inspection_timeout_ms: PositiveMillis::new(whole_millis(
                "spec.compute.durableInspectionTimeout",
                self.durable_inspection_timeout
                    .unwrap_or(DEFAULT_DURABLE_INSPECTION_TIMEOUT),
            )?)
            .map_err(|error| format!("spec.compute.durableInspectionTimeout: {error}"))?,
            durable_inspection_fold_max_records: PositiveUsize::new(
                self.durable_inspection_fold_max_records
                    .unwrap_or(DEFAULT_DURABLE_INSPECTION_FOLD_MAX_RECORDS),
            )
            .map_err(|error| format!("spec.compute.durableInspectionFoldMaxRecords: {error}"))?,
            durable_inspection_fold_max_size,
            fdw_broker_dns_timeout: crabka_client_core::ClientDnsTimeout::new(
                self.fdw_broker_dns_timeout
                    .unwrap_or_else(|| crabka_client_core::ClientDnsTimeout::default().time()),
            )
            .map_err(|error| format!("spec.compute.fdwBrokerDnsTimeout: {error}"))?,
            schema_fetch_retry_policy,
            wal_recovery_fetch_max_wait_ms: PositiveI32::new(whole_millis_i32(
                "spec.compute.walRecoveryFetchMaxWait",
                self.wal_recovery_fetch_max_wait
                    .unwrap_or(DEFAULT_WAL_RECOVERY_FETCH_MAX_WAIT),
            )?)
            .map_err(|error| format!("spec.compute.walRecoveryFetchMaxWait: {error}"))?,
            wal_recovery_fetch_partition_max: PositiveI32::new(whole_bytes_i32(
                "spec.compute.walRecoveryFetchPartitionMax",
                self.wal_recovery_fetch_partition_max
                    .unwrap_or(DEFAULT_WAL_RECOVERY_FETCH_PARTITION_MAX),
            )?)
            .map_err(|error| format!("spec.compute.walRecoveryFetchPartitionMax: {error}"))?,
            wal_recovery_fetch_response_max: PositiveI32::new(whole_bytes_i32(
                "spec.compute.walRecoveryFetchResponseMax",
                self.wal_recovery_fetch_response_max
                    .unwrap_or(DEFAULT_WAL_RECOVERY_FETCH_RESPONSE_MAX),
            )?)
            .map_err(|error| format!("spec.compute.walRecoveryFetchResponseMax: {error}"))?,
            wal_recovery_empty_fetch_retries: PositiveUsize::new(
                self.wal_recovery_empty_fetch_retries
                    .unwrap_or(DEFAULT_WAL_RECOVERY_EMPTY_FETCH_RETRIES),
            )
            .map_err(|error| format!("spec.compute.walRecoveryEmptyFetchRetries: {error}"))?,
            wal_recovery_dns_timeout_ms: PositiveMillis::new(whole_millis(
                "spec.compute.walRecoveryDnsTimeout",
                self.wal_recovery_dns_timeout
                    .unwrap_or(DEFAULT_WAL_RECOVERY_DNS_TIMEOUT),
            )?)
            .map_err(|error| format!("spec.compute.walRecoveryDnsTimeout: {error}"))?,
            wal_recovery_connect_timeout_ms: PositiveMillis::new(whole_millis(
                "spec.compute.walRecoveryConnectTimeout",
                self.wal_recovery_connect_timeout
                    .unwrap_or(DEFAULT_WAL_RECOVERY_CONNECT_TIMEOUT),
            )?)
            .map_err(|error| format!("spec.compute.walRecoveryConnectTimeout: {error}"))?,
            wal_recovery_request_timeout_ms: PositiveMillis::new(whole_millis(
                "spec.compute.walRecoveryRequestTimeout",
                self.wal_recovery_request_timeout
                    .unwrap_or(DEFAULT_WAL_RECOVERY_REQUEST_TIMEOUT),
            )?)
            .map_err(|error| format!("spec.compute.walRecoveryRequestTimeout: {error}"))?,
            wal_producer_flush_timeout: ProducerFlushTimeout::new(
                self.wal_producer_flush_timeout
                    .unwrap_or_else(|| Time::from_std(ProducerFlushTimeout::default().duration()))
                    .to_std(),
            )
            .map_err(|error| format!("spec.compute.walProducerFlushTimeout: {error}"))?,
            wal_producer_dns_timeout: crabka_client_core::ClientDnsTimeout::new(
                self.wal_producer_dns_timeout
                    .unwrap_or_else(|| crabka_client_core::ClientDnsTimeout::default().time()),
            )
            .map_err(|error| format!("spec.compute.walProducerDnsTimeout: {error}"))?,
            wal_producer_retry_policy: self.effective_wal_producer_retry_policy()?,
            wal_producer_throughput_policy: self.effective_wal_producer_throughput_policy()?,
            wal_frame_max_size,
            pgkv_options,
            wal_topic_replication_factor: PositiveI32::new(
                self.wal_topic_replication_factor
                    .unwrap_or(DEFAULT_WAL_TOPIC_REPLICATION_FACTOR),
            )
            .map_err(|error| format!("spec.compute.walTopicReplicationFactor: {error}"))?,
            wal_topic_ensure_timeout_ms: PositiveI32::new(whole_millis_i32(
                "spec.compute.walTopicEnsureTimeout",
                self.wal_topic_ensure_timeout
                    .unwrap_or(DEFAULT_WAL_TOPIC_ENSURE_TIMEOUT),
            )?)
            .map_err(|error| format!("spec.compute.walTopicEnsureTimeout: {error}"))?,
            wal_admin_connect_timeout_ms: PositiveMillis::new(whole_millis(
                "spec.compute.walAdminConnectTimeout",
                self.wal_admin_connect_timeout
                    .unwrap_or(DEFAULT_WAL_ADMIN_CONNECT_TIMEOUT),
            )?)
            .map_err(|error| format!("spec.compute.walAdminConnectTimeout: {error}"))?,
            wal_admin_request_timeout_ms: PositiveMillis::new(whole_millis(
                "spec.compute.walAdminRequestTimeout",
                self.wal_admin_request_timeout
                    .unwrap_or(DEFAULT_WAL_ADMIN_REQUEST_TIMEOUT),
            )?)
            .map_err(|error| format!("spec.compute.walAdminRequestTimeout: {error}"))?,
            lifecycle_requeue_ms: PositiveMillis::new(whole_millis(
                "spec.compute.lifecycleRequeue",
                self.lifecycle_requeue.unwrap_or(DEFAULT_LIFECYCLE_REQUEUE),
            )?)
            .map_err(|error| format!("spec.compute.lifecycleRequeue: {error}"))?,
        })
    }

    fn effective_wal_producer_retry_policy(
        &self,
    ) -> Result<crabka_client_producer::ProducerRetryPolicy, String> {
        let defaults = crabka_client_producer::ProducerRetryPolicy::default();
        let millis = |name, value| {
            whole_millis_i32(name, value)
                .map(|value| Duration::from_millis(u64::try_from(value).expect("positive")))
        };
        let retry_backoff = millis(
            "spec.compute.walProducerRetryBackoff",
            self.wal_producer_retry_backoff
                .unwrap_or_else(|| Time::from_std(defaults.retry_backoff())),
        )?;
        let init_max_backoff = millis(
            "spec.compute.walProducerInitMaxBackoff",
            self.wal_producer_init_max_backoff
                .unwrap_or_else(|| Time::from_std(defaults.init_max_backoff())),
        )?;
        crabka_client_producer::ProducerRetryPolicy::new(
            millis(
                "spec.compute.walProducerRequestTimeout",
                self.wal_producer_request_timeout
                    .unwrap_or_else(|| Time::from_std(defaults.request_timeout())),
            )?,
            self.wal_producer_retries.unwrap_or(defaults.retries()),
            retry_backoff,
            millis(
                "spec.compute.walProducerRoutingRetryBudget",
                self.wal_producer_routing_retry_budget
                    .unwrap_or_else(|| Time::from_std(defaults.routing_retry_budget())),
            )?,
            millis(
                "spec.compute.walProducerInitRetryTimeout",
                self.wal_producer_init_retry_timeout
                    .unwrap_or_else(|| Time::from_std(defaults.init_retry_timeout())),
            )?,
            init_max_backoff,
            millis(
                "spec.compute.walProducerTransactionTimeout",
                self.wal_producer_transaction_timeout
                    .unwrap_or_else(|| Time::from_std(defaults.transaction_timeout())),
            )?,
        )
        .map_err(|error| {
            let field = if error == "producer retry backoff exceeds producer-ID backoff cap" {
                "walProducerRetryBackoff/walProducerInitMaxBackoff"
            } else if error.starts_with("request timeout:") {
                "walProducerRequestTimeout"
            } else if self.wal_producer_retries.is_some_and(|value| value < 0) {
                "walProducerRetries"
            } else if error.starts_with("producer retry backoff:") {
                "walProducerRetryBackoff"
            } else if error.starts_with("routing retry budget:") {
                "walProducerRoutingRetryBudget"
            } else if error.starts_with("producer-ID initialization retry timeout:") {
                "walProducerInitRetryTimeout"
            } else if error.starts_with("producer-ID initialization maximum backoff:") {
                "walProducerInitMaxBackoff"
            } else if error.starts_with("transaction timeout:") {
                "walProducerTransactionTimeout"
            } else {
                "walProducerRetries"
            };
            format!("spec.compute.{field}: {error}")
        })
    }

    fn effective_wal_producer_throughput_policy(
        &self,
    ) -> Result<crabka_client_producer::ProducerThroughputPolicy, String> {
        let defaults = crabka_client_producer::ProducerThroughputPolicy::default();
        crabka_client_producer::ProducerThroughputPolicy::new(
            self.wal_producer_compression
                .map_or_else(|| defaults.compression(), Into::into),
            Duration::from_millis(
                u64::try_from(nonnegative_whole_millis_i32(
                    "spec.compute.walProducerLinger",
                    self.wal_producer_linger
                        .unwrap_or_else(|| Time::from_std(defaults.linger())),
                )?)
                .expect("nonnegative"),
            ),
            whole_bytes_usize(
                "spec.compute.walProducerBatch",
                self.wal_producer_batch.unwrap_or_else(|| {
                    ByteSize::from_bytes(
                        u64::try_from(defaults.batch_bytes()).expect("producer batch fits u64"),
                    )
                }),
            )?,
            defaults.max_in_flight(),
        )
        .map_err(|error| {
            let field = if error.starts_with("producer linger:") {
                "walProducerLinger"
            } else {
                "walProducerBatch"
            };
            format!("spec.compute.{field}: {error}")
        })
    }
}

/// Dry-run balancer integration settings for a Gres fleet.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GresBalancerSpec {
    /// Enable dry-run planning status for this fleet.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Per-goal enablement knobs.
    #[serde(default)]
    pub goals: GresBalancerGoals,

    /// Threshold and cooldown knobs used by future live planner input.
    #[serde(default)]
    pub thresholds: GresBalancerThresholds,

    /// Explicit capability declaration for Kafka's transactional registry
    /// protocol. This protocol does not make any physical Move, Split, or
    /// Merge operation executable. The operator reports plans only.
    #[serde(default)]
    pub registry_layout: GresBalancerRegistryLayout,

    /// Optional dry-run plan snapshot supplied to the operator for status
    /// reporting. The operator never executes these operations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_snapshot: Option<GresBalancerPlanSnapshot>,
}

/// Kafka registry-layout capability available to balancer status reporting.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GresBalancerRegistryLayout {
    /// Kafka transactional registry protocol is configured and available for
    /// metadata transactions only. Physical range operations remain unavailable.
    #[serde(default)]
    pub transactional_registry_protocol: bool,
}

/// A dry-run plan snapshot whose operation kinds are reported in status.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GresBalancerPlanSnapshot {
    /// Operations emitted by the planner for the observed registry snapshot.
    pub operations: Vec<GresBalancerOperationKind>,
}

/// Operation kinds emitted by the dry-run balancer planner.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GresBalancerOperationKind {
    /// Split an existing range.
    Split,
    /// Move an existing range between computes.
    Move,
    /// Merge adjacent ranges.
    Merge,
    /// Convert an unsharded table to sharded layout.
    ConvertToSharded,
}

impl GresBalancerOperationKind {
    /// Return the stable planner operation name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Split => "split",
            Self::Move => "move",
            Self::Merge => "merge",
            Self::ConvertToSharded => "convert_to_sharded",
        }
    }
}

/// Per-goal enablement knobs.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GresBalancerGoals {
    /// Goals disabled for dry-run planning. Omit a goal to leave it enabled.
    #[serde(default)]
    pub disabled_goals: Vec<GresBalancerGoal>,
}

/// Supported dry-run balancer goals.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GresBalancerGoal {
    /// Preserve co-located buckets on the same compute.
    CoLocationIntegrity,
    /// Keep per-compute range counts under the configured limit.
    RangeLimit,
    /// Split oversized ranges and merge tiny adjacent ranges.
    RangeSize,
    /// Move hot ranges away from overloaded computes.
    LoadSkew,
    /// Plan conversion of large or hot unsharded tables.
    AutoShardConversion,
}

/// Planner threshold and no-flapping knobs.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GresBalancerThresholds {
    /// Split ranges larger than this size.
    #[serde(with = "crabka_units::serde_units::human::byte_size")]
    #[schemars(with = "String")]
    pub size_ceiling: ByteSize,
    /// Merge adjacent ranges below this combined size.
    #[serde(with = "crabka_units::serde_units::human::byte_size")]
    #[schemars(with = "String")]
    pub merge_floor: ByteSize,
    /// Row stride used when a range has no upper bound.
    pub split_stride_rows: u64,
    /// Load skew tolerated before move planning.
    #[serde(with = "crabka_units::serde_units::human::ratio")]
    #[schemars(with = "String")]
    pub load_skew_hysteresis: Ratio,
    /// Optional maximum ranges per compute.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_ranges_per_compute: Option<usize>,
    /// Maximum operations in one dry-run plan.
    pub max_operations: usize,
    /// Epoch count that suppresses repeated operation kinds.
    pub cooldown_epochs: u64,
}

impl Default for GresBalancerThresholds {
    fn default() -> Self {
        Self {
            size_ceiling: gibibytes(1),
            merge_floor: mebibytes(64),
            split_stride_rows: 1_000_000,
            load_skew_hysteresis: percent(25),
            max_ranges_per_compute: None,
            max_operations: 32,
            cooldown_epochs: 2,
        }
    }
}

const fn default_true() -> bool {
    true
}

/// `PgDog` front-door deployment settings.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PgdogSpec {
    /// Container image override. When absent, the operator uses
    /// `--default-pgdog-image`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    /// Number of `PgDog` replicas.
    #[schemars(range(min = 0, max = 1_000))]
    pub replicas: i32,

    /// `PgDog` client listen port.
    #[schemars(range(min = 1, max = 65_535))]
    pub listen_port: i32,

    /// Optional TLS Secret mounted by the future `PgDog` controller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_secret_ref: Option<SecretRef>,

    /// Secret key reference for `PgDog` admin credentials.
    pub admin_secret_ref: SecretKeyRef,

    /// Fleet-wide pooling mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pooler_mode: Option<PgdogPoolerModeSpec>,

    /// Number of backend connection attempts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = 65_535))]
    pub connect_attempts: Option<u16>,

    /// Idle pooled-server disconnect window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub idle_timeout: Option<Time>,

    /// Idle timeout used while at least one tenant can suspend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub suspension_idle_timeout: Option<Time>,

    /// Maximum lifetime for pooled backend connections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub server_lifetime: Option<Time>,

    /// `PgDog` readiness probe period in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub readiness_probe_period_seconds: Option<i32>,

    /// Direct-route credential retention grace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub direct_bootstrap_grace: Option<Time>,
}

/// Pooling modes accepted by the Gres CRD.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PgdogPoolerModeSpec {
    /// Reuse backend connections across transaction boundaries.
    Transaction,
    /// Bind one backend connection to one client session.
    Session,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct EffectivePgdogPolicy {
    pub(crate) listen_port: u16,
    pub(crate) pooler_mode: PgdogPoolerMode,
    pub(crate) connect_attempts: PgdogConnectAttempts,
    pub(crate) idle_timeout: PositiveMillis,
    pub(crate) suspension_idle_timeout: PositiveMillis,
    pub(crate) server_lifetime: PositiveMillis,
    pub(crate) readiness_probe_period_seconds: i32,
    pub(crate) direct_bootstrap_grace: PositiveMillis,
}

impl PgdogSpec {
    pub(crate) fn effective_policy(&self) -> Result<EffectivePgdogPolicy, String> {
        let listen_port = u16::try_from(self.listen_port)
            .ok()
            .filter(|port| *port > 0)
            .ok_or_else(|| "spec.pgdog.listenPort: must be in 1..=65535".to_string())?;
        let pooler_mode = match self.pooler_mode.unwrap_or(PgdogPoolerModeSpec::Transaction) {
            PgdogPoolerModeSpec::Transaction => PgdogPoolerMode::Transaction,
            PgdogPoolerModeSpec::Session => PgdogPoolerMode::Session,
        };
        let connect_attempts = PgdogConnectAttempts::new(self.connect_attempts.unwrap_or(3))
            .map_err(|error| format!("spec.pgdog.connectAttempts: {error}"))?;
        let idle_timeout = PositiveMillis::new(whole_millis(
            "spec.pgdog.idleTimeout",
            self.idle_timeout.unwrap_or_else(|| Time::from_secs(60)),
        )?)
        .map_err(|error| format!("spec.pgdog.idleTimeout: {error}"))?;
        let suspension_idle_timeout = PositiveMillis::new(whole_millis(
            "spec.pgdog.suspensionIdleTimeout",
            self.suspension_idle_timeout
                .unwrap_or_else(|| Time::from_secs(1)),
        )?)
        .map_err(|error| format!("spec.pgdog.suspensionIdleTimeout: {error}"))?;
        let server_lifetime = PositiveMillis::new(whole_millis(
            "spec.pgdog.serverLifetime",
            self.server_lifetime.unwrap_or_else(|| Time::from_secs(300)),
        )?)
        .map_err(|error| format!("spec.pgdog.serverLifetime: {error}"))?;
        let readiness_probe_period_seconds =
            GreaterI32::<0>::new(self.readiness_probe_period_seconds.unwrap_or(5))
                .map_err(|error| format!("spec.pgdog.readinessProbePeriodSeconds: {error}"))?
                .into_value();
        let direct_bootstrap_grace = PositiveMillis::new(whole_millis(
            "spec.pgdog.directBootstrapGrace",
            self.direct_bootstrap_grace
                .unwrap_or_else(|| Time::from_secs(4)),
        )?)
        .map_err(|error| format!("spec.pgdog.directBootstrapGrace: {error}"))?;
        Ok(EffectivePgdogPolicy {
            listen_port,
            pooler_mode,
            connect_attempts,
            idle_timeout,
            suspension_idle_timeout,
            server_lifetime,
            readiness_probe_period_seconds,
            direct_bootstrap_grace,
        })
    }
}

/// Reference to a Kubernetes Secret in the same namespace.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SecretRef {
    /// Secret name.
    pub name: String,
}

/// Reference to one key within a Kubernetes Secret in the same namespace.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeyRef {
    /// Secret name.
    pub name: String,

    /// Secret data key.
    pub key: String,
}

/// Defaults shared by a Gres fleet's tenants.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TenantDefaults {
    /// Replication factor for tenant WAL topics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wal_replication: Option<i32>,

    /// PBKDF2 iteration count for tenant Kafka and `PostgreSQL` SCRAM credentials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 4096, max = 16384))]
    pub scram_iterations: Option<i32>,

    /// Checkpoint after this many frames when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_frames: Option<u64>,

    /// Checkpoint after this much WAL when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<String>")]
    pub checkpoint_size: Option<ByteSize>,

    /// Keep the tenant warm when its latest checkpoint exceeds this size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<String>")]
    pub suspend_max_checkpoint_size: Option<ByteSize>,

    /// Idle timeout in seconds. When unset, the tenant never suspends
    /// because it is idle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_seconds: Option<u64>,
}

/// Observed Gres fleet state.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GresStatus {
    /// Kubernetes-style condition list.
    #[serde(default)]
    pub conditions: Vec<crate::crd::KafkaCondition>,

    /// `metadata.generation` of the last successfully-reconciled spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Last `PgDog` config hash that was confirmed through the admin surface.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirmed_pgdog_config_hash: Option<String>,

    /// In-cluster service URL for the `PgDog` front door.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_url: Option<String>,

    /// Last balancer planning summary. The operator does not execute range
    /// changes from this status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub balancer: Option<GresBalancerStatus>,
}

/// Balancer status summary.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GresBalancerStatus {
    /// Whether planner status is enabled by spec.
    pub enabled: bool,
    /// True while the operator reports planner status and does not execute
    /// live changes.
    pub dry_run_only: bool,
    /// Whether Kafka's transactional registry protocol was explicitly
    /// configured as available for metadata transactions. It does not enable
    /// physical range operations.
    pub transactional_registry_protocol_available: bool,
    /// Goals active for dry-run planning.
    pub enabled_goals: Vec<String>,
    /// Goals disabled by spec or by the top-level balancer switch.
    pub disabled_goals: Vec<String>,
    /// Number of operations in the reported dry-run plan snapshot.
    pub planned_operations: usize,
    /// Distinct operation kinds in the reported dry-run plan snapshot.
    pub planned_operation_kinds: Vec<String>,
    /// Number of planned operations executable by the operator. Always zero:
    /// the transactional registry protocol cannot execute physical operations.
    pub executable_operations: usize,
    /// Distinct planned operation kinds executable by the operator. Empty
    /// until the operator implements physical orchestration.
    pub executable_operation_kinds: Vec<String>,
    /// Number of planned operations the operator intentionally will not execute.
    pub unsupported_operations: usize,
    /// Distinct planned operation kinds the operator will not classify as
    /// executable. `convert_to_sharded` remains unsupported.
    pub unsupported_operation_kinds: Vec<String>,
    /// Why mutations are disabled even when a dry-run plan is available.
    pub mutation_disabled_reason: String,
    /// Human-readable dry-run status reason.
    pub message: String,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::{assert, check};
    use crabka_units::convert::ByteSizeExt as _;
    use kube::CustomResourceExt as _;

    use super::*;

    #[test]
    fn crd_metadata_is_correct() {
        let crd = Gres::crd();
        check!(crd.spec.group == "crabka.io");
        check!(crd.spec.names.kind == "Gres");
        check!(crd.spec.names.plural == "greses");
        check!(
            crd.spec
                .names
                .short_names
                .as_ref()
                .is_some_and(|v| v.contains(&"gg".to_string())),
            "expected shortname `gg`",
        );
        check!(crd.spec.versions.len() == 1);
        check!(crd.spec.versions[0].name == "v1alpha1");
    }

    #[test]
    fn tenant_scram_iterations_schema_matches_broker_bounds() {
        let crd = serde_json::to_value(Gres::crd()).unwrap();
        let iterations = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["defaults"]["properties"]["scramIterations"];
        assert!(iterations["minimum"].as_f64() == Some(4_096.0));
        assert!(iterations["maximum"].as_f64() == Some(16_384.0));
    }

    #[test]
    fn spec_round_trips_through_json() {
        let spec = GresSpec {
            kafka_cluster: "demo".into(),
            pgdog: PgdogSpec {
                image: None,
                replicas: 2,
                listen_port: 6_432,
                tls_secret_ref: Some(SecretRef {
                    name: "gres-tls".into(),
                }),
                admin_secret_ref: SecretKeyRef {
                    name: "pgdog-admin".into(),
                    key: "password".into(),
                },
                pooler_mode: None,
                connect_attempts: None,
                idle_timeout: None,
                suspension_idle_timeout: None,
                server_lifetime: None,
                readiness_probe_period_seconds: None,
                direct_bootstrap_grace: None,
            },
            activator: Some(GresActivatorSpec {
                image: Some("example.test/activator:v2".into()),
                replicas: Some(3),
                registry_poll: Some(crabka_units::millis(500)),
                cold_start_timeout: Some(crabka_units::secs(45)),
                readiness_probe_period_seconds: Some(7),
                client_dispatch_queue_capacity: None,
                client_frame_max: None,
            }),
            compute: Some(GresComputeSpec {
                readiness_probe_period_seconds: Some(11),
                ..GresComputeSpec::default()
            }),
            defaults: Some(TenantDefaults {
                wal_replication: Some(3),
                scram_iterations: Some(12_288),
                checkpoint_frames: Some(10_000),
                checkpoint_size: None,
                suspend_max_checkpoint_size: None,
                idle_seconds: Some(3_600),
            }),
            balancer: Some(GresBalancerSpec {
                enabled: true,
                goals: GresBalancerGoals {
                    disabled_goals: vec![GresBalancerGoal::LoadSkew],
                },
                thresholds: GresBalancerThresholds::default(),
                registry_layout: GresBalancerRegistryLayout::default(),
                plan_snapshot: Some(GresBalancerPlanSnapshot {
                    operations: vec![GresBalancerOperationKind::Move],
                }),
            }),
            tracing: Some(Tracing {
                kind: crate::crd::kafka::TracingType::Otlp,
                otlp: Some(crate::crd::kafka::OtlpTracing {
                    endpoint: "http://otel:4317".into(),
                    protocol: Some(crate::crd::kafka::OtlpProtocol::HttpProtobuf),
                    sample_ratio: Some(0.25),
                    service_name: Some("gres-analytics".into()),
                    timeout: Some(crabka_units::secs(7)),
                }),
            }),
        };
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("\"kafkaCluster\":\"demo\""), "got: {json}");
        assert!(json.contains("\"listenPort\":6432"), "got: {json}");
        assert!(json.contains("\"scramIterations\":12288"), "got: {json}");
        assert!(
            json.contains(
                "\"activator\":{\"image\":\"example.test/activator:v2\",\"replicas\":3,\"registryPoll\":\"500ms\",\"coldStartTimeout\":\"45s\",\"readinessProbePeriodSeconds\":7}"
            ),
            "got: {json}"
        );
        assert!(
            json.contains("\"disabledGoals\":[\"load_skew\"]"),
            "got: {json}"
        );
        assert!(
            json.contains(
                "\"tracing\":{\"type\":\"Otlp\",\"otlp\":{\"endpoint\":\"http://otel:4317\",\"protocol\":\"http_protobuf\",\"sampleRatio\":0.25,\"serviceName\":\"gres-analytics\",\"timeout\":\"7s\"}}"
            ),
            "got: {json}"
        );
        let back: GresSpec = serde_json::from_str(&json).unwrap();
        assert!(back == spec);
    }

    /// `Gres.spec.tracing` uses the [`Tracing`] type of `Kafka` and does
    /// not declare a parallel set of types. The two CRDs must therefore
    /// show the same field shape, the same enum values, and the same
    /// required set. Only the top-level `description` may differ, because
    /// each field documents its own fleet.
    #[test]
    fn tracing_schema_is_shared_with_the_kafka_crd() {
        let gres = serde_json::to_value(Gres::crd()).expect("serialize Gres CRD");
        let kafka = serde_json::to_value(crate::crd::Kafka::crd()).expect("serialize Kafka CRD");
        let tracing = |crd: &serde_json::Value| {
            let mut schema = crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
                ["spec"]["properties"]["tracing"]
                .clone();
            schema
                .as_object_mut()
                .expect("tracing schema is an object")
                .remove("description");
            schema
        };
        let gres_tracing = tracing(&gres);
        assert!(gres_tracing["type"] == "object", "got: {gres_tracing}");
        assert!(
            gres_tracing["properties"]["otlp"]["properties"]["endpoint"]["type"] == "string",
            "got: {gres_tracing}"
        );
        assert!(
            gres_tracing == tracing(&kafka),
            "Gres and Kafka must render one shared tracing schema; got: {gres_tracing}"
        );
    }

    #[test]
    fn activator_schema_requires_positive_values() {
        let crd = serde_json::to_value(Gres::crd()).expect("serialize Gres CRD");
        let activator = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["activator"];

        assert!(activator["type"] == "object");
        assert!(activator["properties"]["image"]["minLength"].as_u64() == Some(1));
        for field in ["replicas", "readinessProbePeriodSeconds"] {
            assert!(
                activator["properties"][field]["minimum"].as_f64() == Some(1.0),
                "missing minimum for {field}: {activator}"
            );
        }
        for field in ["registryPoll", "coldStartTimeout"] {
            assert!(activator["properties"][field]["type"] == "string");
        }
        assert!(activator["properties"]["registryReplicationFactor"].is_null());
    }

    #[test]
    fn activator_client_policy_round_trips_and_validates() {
        let policy = GresActivatorSpec {
            client_dispatch_queue_capacity: Some(7),
            client_frame_max: Some(crabka_units::kibibytes(32)),
            ..GresActivatorSpec::default()
        };
        let json = serde_json::to_string(&policy).expect("serialize activator policy");
        assert!(serde_json::from_str::<GresActivatorSpec>(&json).unwrap() == policy);
        let (queue, frame) = policy
            .client_resource_policy()
            .expect("valid activator client policy");
        assert!(queue.expect("queue").get() == 7);
        assert!(frame.expect("frame").size() == crabka_units::kibibytes(32));

        let crd = serde_json::to_value(Gres::crd()).expect("serialize Gres CRD");
        let activator = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["activator"]["properties"];
        assert!(activator["clientDispatchQueueCapacity"]["minimum"].as_f64() == Some(1.0));
        assert!(activator["clientFrameMax"]["type"] == "string");

        for (policy, path) in [
            (
                GresActivatorSpec {
                    client_dispatch_queue_capacity: Some(0),
                    ..GresActivatorSpec::default()
                },
                "spec.activator.clientDispatchQueueCapacity",
            ),
            (
                GresActivatorSpec {
                    client_frame_max: Some(ByteSize::ZERO),
                    ..GresActivatorSpec::default()
                },
                "spec.activator.clientFrameMax",
            ),
            (
                GresActivatorSpec {
                    client_frame_max: Some(ByteSize::from_bytes_f64(1.5)),
                    ..GresActivatorSpec::default()
                },
                "spec.activator.clientFrameMax",
            ),
            (
                GresActivatorSpec {
                    client_frame_max: Some(crabka_units::mebibytes(101)),
                    ..GresActivatorSpec::default()
                },
                "spec.activator.clientFrameMax",
            ),
        ] {
            let error = policy
                .client_resource_policy()
                .expect_err("invalid client policy");
            assert!(error.contains(path), "got: {error}");
        }
    }

    #[test]
    fn compute_readiness_policy_round_trips_and_requires_positive_values() {
        let policy = GresComputeSpec {
            readiness_probe_period_seconds: Some(7),
            ..GresComputeSpec::default()
        };
        let json = serde_json::to_string(&policy).expect("serialize compute policy");
        assert!(
            json.contains("\"readinessProbePeriodSeconds\":7"),
            "got: {json}"
        );
        assert!(serde_json::from_str::<GresComputeSpec>(&json).unwrap() == policy);

        let crd = serde_json::to_value(Gres::crd()).expect("serialize Gres CRD");
        let compute = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["compute"];
        assert!(
            compute["properties"]["readinessProbePeriodSeconds"]["minimum"].as_f64() == Some(1.0)
        );
    }

    #[test]
    fn compute_client_policy_round_trips_and_validates() {
        let policy = GresComputeSpec {
            client_dispatch_queue_capacity: Some(7),
            client_frame_max: Some(crabka_units::kibibytes(32)),
            fdw_fetch_min: Some(crabka_units::bytes(2)),
            fdw_fetch_max_wait: Some(crabka_units::millis(41)),
            fdw_fetch_partition_max: Some(crabka_units::bytes(43)),
            fdw_connect_timeout: Some(crabka_units::millis(47)),
            fdw_request_timeout: Some(crabka_units::millis(53)),
            fdw_schema_fetch_timeout: Some(crabka_units::millis(59)),
            fdw_schema_fetch_poll: Some(crabka_units::millis(17)),
            wal_recovery_fetch_min: Some(crabka_units::bytes(3)),
            ..GresComputeSpec::default()
        };
        let json = serde_json::to_string(&policy).expect("serialize compute client policy");
        assert!(serde_json::from_str::<GresComputeSpec>(&json).unwrap() == policy);
        let effective = policy.effective_policy().expect("valid compute policy");
        assert!(
            effective
                .client_dispatch_queue_capacity
                .expect("queue")
                .get()
                == 7
        );
        assert!(effective.client_frame_max.expect("frame").size() == crabka_units::kibibytes(32));
        assert!(effective.fdw_fetch_min.expect("FDW fetch").size() == crabka_units::bytes(2));
        assert!(effective.fdw_fetch_max_wait == crabka_units::millis(41));
        assert!(effective.fdw_fetch_partition_max == crabka_units::bytes(43));
        assert!(effective.fdw_connect_timeout == crabka_units::millis(47));
        assert!(effective.fdw_request_timeout == crabka_units::millis(53));
        assert!(effective.fdw_schema_fetch_timeout == crabka_units::millis(59));
        assert!(effective.fdw_schema_fetch_poll == crabka_units::millis(17));
        assert!(
            effective
                .wal_recovery_fetch_min
                .expect("WAL recovery fetch")
                .size()
                == crabka_units::bytes(3)
        );

        let crd = serde_json::to_value(Gres::crd()).expect("serialize Gres CRD");
        let compute = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["compute"]["properties"];
        assert!(compute["clientDispatchQueueCapacity"]["minimum"].as_f64() == Some(1.0));
        for field in [
            "clientFrameMax",
            "fdwFetchMin",
            "fdwFetchMaxWait",
            "fdwFetchPartitionMax",
            "fdwConnectTimeout",
            "fdwRequestTimeout",
            "fdwSchemaFetchTimeout",
            "fdwSchemaFetchPoll",
            "walRecoveryFetchMin",
        ] {
            assert!(
                compute[field]["type"] == "string",
                "wrong schema for {field}"
            );
        }

        for (policy, path) in [
            (
                GresComputeSpec {
                    client_dispatch_queue_capacity: Some(0),
                    ..GresComputeSpec::default()
                },
                "spec.compute.clientDispatchQueueCapacity",
            ),
            (
                GresComputeSpec {
                    client_frame_max: Some(crabka_units::mebibytes(101)),
                    ..GresComputeSpec::default()
                },
                "spec.compute.clientFrameMax",
            ),
            (
                GresComputeSpec {
                    fdw_fetch_min: Some(ByteSize::ZERO),
                    ..GresComputeSpec::default()
                },
                "spec.compute.fdwFetchMin",
            ),
            (
                GresComputeSpec {
                    fdw_fetch_max_wait: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                "spec.compute.fdwFetchMaxWait",
            ),
            (
                GresComputeSpec {
                    fdw_schema_fetch_timeout: Some(crabka_units::millis(10)),
                    fdw_schema_fetch_poll: Some(crabka_units::millis(11)),
                    ..GresComputeSpec::default()
                },
                "spec.compute.fdwSchemaFetchPoll",
            ),
            (
                GresComputeSpec {
                    wal_recovery_fetch_min: Some(ByteSize::from_bytes_f64(1.5)),
                    ..GresComputeSpec::default()
                },
                "spec.compute.walRecoveryFetchMin",
            ),
        ] {
            let error = policy
                .effective_policy()
                .expect_err("invalid client policy");
            assert!(error.contains(path), "got: {error}");
        }
    }

    #[test]
    fn compute_readiness_policy_uses_validated_default_and_rejects_zero() {
        assert!(
            GresComputeSpec::default()
                .effective_readiness_probe_period_seconds()
                .expect("default readiness")
                == 5
        );
        let error = GresComputeSpec {
            readiness_probe_period_seconds: Some(0),
            ..GresComputeSpec::default()
        }
        .effective_readiness_probe_period_seconds()
        .expect_err("zero readiness must fail");
        assert!(
            error.contains("spec.compute.readinessProbePeriodSeconds"),
            "got: {error}"
        );
    }

    #[test]
    fn compute_checkpoint_lifecycle_policy_round_trips_and_has_exact_schema_bounds() {
        let policy = GresComputeSpec {
            checkpoint_part_size: Some(crabka_units::bytes(8)),
            checkpoint_retain: Some(1),
            checkpoint_delete_records_timeout: Some(Time::from_millis(i64::from(i32::MAX))),
            checkpoint_poll_interval: Some(crabka_units::millis(1)),
            idle_suspend_poll_interval: Some(crabka_units::millis(1)),
            range0_follower_poll_interval: Some(crabka_units::millis(1)),
            range0_follower_rebuild_backoff_floor: Some(crabka_units::millis(2)),
            range0_follower_rebuild_backoff_ceiling: Some(crabka_units::millis(3)),
            durable_inspection_timeout: Some(crabka_units::millis(4)),
            durable_inspection_fold_max_records: Some(5),
            durable_inspection_fold_max_size: Some(crabka_units::bytes(6)),
            lifecycle_requeue: Some(crabka_units::millis(1)),
            ..GresComputeSpec::default()
        };
        let json = serde_json::to_string(&policy).expect("serialize compute policy");
        assert!(serde_json::from_str::<GresComputeSpec>(&json).unwrap() == policy);

        let crd = serde_json::to_value(Gres::crd()).expect("serialize Gres CRD");
        let properties = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["compute"]["properties"];
        assert!(properties["checkpointPartSize"]["type"] == "string");
        assert!(properties["checkpointRetain"]["minimum"].as_f64() == Some(1.0));
        assert!(properties["durableInspectionFoldMaxRecords"]["minimum"].as_f64() == Some(1.0));
        for field in [
            "checkpointDeleteRecordsTimeout",
            "checkpointPollInterval",
            "idleSuspendPollInterval",
            "range0FollowerPollInterval",
            "range0FollowerRebuildBackoffFloor",
            "range0FollowerRebuildBackoffCeiling",
            "durableInspectionTimeout",
            "durableInspectionFoldMaxSize",
            "lifecycleRequeue",
        ] {
            assert!(
                properties[field]["type"] == "string",
                "wrong schema for {field}"
            );
        }
    }

    #[test]
    fn compute_checkpoint_lifecycle_policy_uses_exact_defaults_and_rejects_boundaries() {
        let defaults = GresComputeSpec::default()
            .effective_policy()
            .expect("default compute policy");
        // The CRD field stays a raw usize (the spec derives Eq); the policy
        // hands out a ByteSize, so compare at the seam.
        assert!(
            defaults.checkpoint_part_size.into_value().bytes_usize()
                == DEFAULT_PART_MAX_SIZE.bytes_usize()
        );
        assert!(defaults.checkpoint_retain.into_value() == DEFAULT_CHECKPOINT_RETAIN);
        assert!(
            defaults.checkpoint_delete_records_timeout_ms.into_value()
                == DEFAULT_CHECKPOINT_DELETE_RECORDS_TIMEOUT.millis_i32()
        );
        assert!(
            defaults.checkpoint_poll_interval_ms.into_value()
                == millis_u64(DEFAULT_CHECKPOINT_POLL_INTERVAL)
        );
        assert!(
            defaults.idle_suspend_poll_interval_ms.into_value()
                == millis_u64(DEFAULT_IDLE_SUSPEND_POLL_INTERVAL)
        );
        assert!(
            defaults.range0_follower_poll_interval_ms.into_value()
                == millis_u64(DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL)
        );
        assert!(
            defaults
                .range0_follower_rebuild_backoff_floor_ms
                .into_value()
                == millis_u64(DEFAULT_RANGE0_FOLLOWER_REBUILD_BACKOFF_FLOOR)
        );
        assert!(
            defaults
                .range0_follower_rebuild_backoff_ceiling_ms
                .into_value()
                == millis_u64(DEFAULT_RANGE0_FOLLOWER_REBUILD_BACKOFF_CEILING)
        );
        assert!(
            defaults.durable_inspection_timeout_ms.into_value()
                == millis_u64(DEFAULT_DURABLE_INSPECTION_TIMEOUT)
        );
        assert!(
            defaults.durable_inspection_fold_max_records.into_value()
                == DEFAULT_DURABLE_INSPECTION_FOLD_MAX_RECORDS
        );
        assert!(
            defaults.durable_inspection_fold_max_size == DEFAULT_DURABLE_INSPECTION_FOLD_MAX_SIZE
        );
        assert!(
            defaults.lifecycle_requeue_ms.into_value()
                == u64::try_from(DEFAULT_LIFECYCLE_REQUEUE.millis_i64()).expect("positive")
        );

        for (policy, path) in [
            (
                GresComputeSpec {
                    checkpoint_part_size: Some(crabka_units::bytes(7)),
                    ..GresComputeSpec::default()
                },
                "spec.compute.checkpointPartSize",
            ),
            (
                GresComputeSpec {
                    checkpoint_retain: Some(0),
                    ..GresComputeSpec::default()
                },
                "spec.compute.checkpointRetain",
            ),
            (
                GresComputeSpec {
                    checkpoint_delete_records_timeout: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                "spec.compute.checkpointDeleteRecordsTimeout",
            ),
            (
                GresComputeSpec {
                    checkpoint_poll_interval: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                "spec.compute.checkpointPollInterval",
            ),
            (
                GresComputeSpec {
                    idle_suspend_poll_interval: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                "spec.compute.idleSuspendPollInterval",
            ),
            (
                GresComputeSpec {
                    range0_follower_poll_interval: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                "spec.compute.range0FollowerPollInterval",
            ),
            (
                GresComputeSpec {
                    range0_follower_rebuild_backoff_floor: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                "spec.compute.range0FollowerRebuildBackoffFloor",
            ),
            (
                GresComputeSpec {
                    range0_follower_rebuild_backoff_ceiling: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                "spec.compute.range0FollowerRebuildBackoffCeiling",
            ),
            (
                GresComputeSpec {
                    durable_inspection_timeout: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                "spec.compute.durableInspectionTimeout",
            ),
            (
                GresComputeSpec {
                    durable_inspection_fold_max_records: Some(0),
                    ..GresComputeSpec::default()
                },
                "spec.compute.durableInspectionFoldMaxRecords",
            ),
            (
                GresComputeSpec {
                    durable_inspection_fold_max_size: Some(ByteSize::ZERO),
                    ..GresComputeSpec::default()
                },
                "spec.compute.durableInspectionFoldMaxSize",
            ),
            (
                GresComputeSpec {
                    lifecycle_requeue: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                "spec.compute.lifecycleRequeue",
            ),
        ] {
            let error = policy.effective_policy().expect_err("boundary must fail");
            assert!(error.contains(path), "got: {error}");
        }

        let error = GresComputeSpec {
            range0_follower_rebuild_backoff_floor: Some(crabka_units::millis(2)),
            range0_follower_rebuild_backoff_ceiling: Some(crabka_units::millis(1)),
            ..GresComputeSpec::default()
        }
        .effective_policy()
        .expect_err("inverted backoff must fail");
        assert!(
            error.contains("spec.compute.range0FollowerRebuildBackoffFloor"),
            "got: {error}"
        );
    }

    #[test]
    fn compute_wal_recovery_policy_round_trips_validates_and_uses_substrate_defaults() {
        let policy = GresComputeSpec {
            wal_recovery_fetch_max_wait: Some(crabka_units::millis(11)),
            wal_recovery_fetch_partition_max: Some(crabka_units::bytes(22)),
            wal_recovery_fetch_response_max: Some(crabka_units::bytes(33)),
            wal_recovery_empty_fetch_retries: Some(44),
            wal_recovery_dns_timeout: Some(crabka_units::millis(77)),
            wal_recovery_connect_timeout: Some(crabka_units::millis(55)),
            wal_recovery_request_timeout: Some(crabka_units::millis(66)),
            ..GresComputeSpec::default()
        };
        let json = serde_json::to_string(&policy).expect("serialize compute policy");
        let yaml = serde_yaml::to_string(&policy).expect("serialize compute policy");
        assert!(serde_json::from_str::<GresComputeSpec>(&json).unwrap() == policy);
        assert!(serde_yaml::from_str::<GresComputeSpec>(&yaml).unwrap() == policy);

        let crd = serde_json::to_value(Gres::crd()).expect("serialize Gres CRD");
        let properties = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["compute"]["properties"];
        for field in [
            "walRecoveryFetchPartitionMax",
            "walRecoveryFetchResponseMax",
        ] {
            assert!(
                properties[field]["type"] == "string",
                "wrong schema for {field}: {properties}"
            );
        }
        assert!(properties["walRecoveryEmptyFetchRetries"]["minimum"].as_f64() == Some(1.0));
        for field in [
            "walRecoveryFetchMaxWait",
            "walRecoveryDnsTimeout",
            "walRecoveryConnectTimeout",
            "walRecoveryRequestTimeout",
        ] {
            assert!(
                properties[field]["type"] == "string",
                "wrong schema for {field}"
            );
        }

        let defaults = GresComputeSpec::default()
            .effective_policy()
            .expect("default compute policy");
        assert!(
            defaults.wal_recovery_fetch_max_wait_ms.into_value()
                == DEFAULT_WAL_RECOVERY_FETCH_MAX_WAIT.millis_i32()
        );
        assert!(
            defaults.wal_recovery_fetch_partition_max.into_value()
                == DEFAULT_WAL_RECOVERY_FETCH_PARTITION_MAX.bytes_i32()
        );
        assert!(
            defaults.wal_recovery_fetch_response_max.into_value()
                == DEFAULT_WAL_RECOVERY_FETCH_RESPONSE_MAX.bytes_i32()
        );
        assert!(
            defaults.wal_recovery_empty_fetch_retries.into_value()
                == DEFAULT_WAL_RECOVERY_EMPTY_FETCH_RETRIES
        );
        assert!(
            defaults.wal_recovery_dns_timeout_ms.into_value()
                == millis_u64(DEFAULT_WAL_RECOVERY_DNS_TIMEOUT)
        );
        assert!(
            defaults.wal_recovery_connect_timeout_ms.into_value()
                == millis_u64(DEFAULT_WAL_RECOVERY_CONNECT_TIMEOUT)
        );
        assert!(
            defaults.wal_recovery_request_timeout_ms.into_value()
                == millis_u64(DEFAULT_WAL_RECOVERY_REQUEST_TIMEOUT)
        );

        for (policy, path) in [
            (
                GresComputeSpec {
                    wal_recovery_fetch_max_wait: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                "spec.compute.walRecoveryFetchMaxWait",
            ),
            (
                GresComputeSpec {
                    wal_recovery_fetch_partition_max: Some(crabka_units::bytes(0)),
                    ..GresComputeSpec::default()
                },
                "spec.compute.walRecoveryFetchPartitionMax",
            ),
            (
                GresComputeSpec {
                    wal_recovery_fetch_response_max: Some(crabka_units::bytes(0)),
                    ..GresComputeSpec::default()
                },
                "spec.compute.walRecoveryFetchResponseMax",
            ),
            (
                GresComputeSpec {
                    wal_recovery_empty_fetch_retries: Some(0),
                    ..GresComputeSpec::default()
                },
                "spec.compute.walRecoveryEmptyFetchRetries",
            ),
            (
                GresComputeSpec {
                    wal_recovery_dns_timeout: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                "spec.compute.walRecoveryDnsTimeout",
            ),
            (
                GresComputeSpec {
                    wal_recovery_connect_timeout: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                "spec.compute.walRecoveryConnectTimeout",
            ),
            (
                GresComputeSpec {
                    wal_recovery_request_timeout: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                "spec.compute.walRecoveryRequestTimeout",
            ),
        ] {
            let error = policy.effective_policy().expect_err("zero must fail");
            assert!(error.contains(path), "got: {error}");
        }
    }

    #[test]
    fn compute_wal_producer_policy_round_trips_and_has_exact_schema_bounds() {
        let policy = GresComputeSpec {
            wal_producer_request_timeout: Some(Time::from_millis(i64::from(i32::MAX))),
            wal_producer_retries: Some(0),
            wal_producer_retry_backoff: Some(crabka_units::millis(1)),
            wal_producer_routing_retry_budget: Some(Time::from_millis(i64::from(i32::MAX))),
            wal_producer_init_retry_timeout: Some(Time::from_millis(i64::from(i32::MAX))),
            wal_producer_init_max_backoff: Some(crabka_units::millis(1)),
            wal_producer_transaction_timeout: Some(Time::from_millis(i64::from(i32::MAX))),
            ..GresComputeSpec::default()
        };
        let json = serde_json::to_string(&policy).expect("serialize compute policy");
        let yaml = serde_yaml::to_string(&policy).expect("serialize compute policy");
        assert!(serde_json::from_str::<GresComputeSpec>(&json).unwrap() == policy);
        assert!(serde_yaml::from_str::<GresComputeSpec>(&yaml).unwrap() == policy);

        let crd = serde_json::to_value(Gres::crd()).expect("serialize Gres CRD");
        let properties = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["compute"]["properties"];
        for field in [
            "walProducerRequestTimeout",
            "walProducerRetryBackoff",
            "walProducerRoutingRetryBudget",
            "walProducerInitRetryTimeout",
            "walProducerInitMaxBackoff",
            "walProducerTransactionTimeout",
        ] {
            assert!(
                properties[field]["type"] == "string",
                "wrong schema for {field}"
            );
        }
        assert!(properties["walProducerRetries"]["minimum"].as_f64() == Some(0.0));
    }

    #[test]
    fn wal_frame_max_size_has_default_override_schema_and_validation() {
        assert!(
            GresComputeSpec::default()
                .effective_policy()
                .expect("defaults")
                .wal_frame_max_size
                == DEFAULT_MAX_FRAME_SIZE
        );
        let spec = GresComputeSpec {
            wal_frame_max_size: Some(crabka_units::bytes(37)),
            ..Default::default()
        };
        assert!(
            spec.effective_policy()
                .expect("override")
                .wal_frame_max_size
                == crabka_units::bytes(37)
        );
        let json = serde_json::to_value(&spec).expect("serialize");
        assert!(json["walFrameMaxSize"] == "37B");
        let crd = serde_json::to_value(Gres::crd()).expect("CRD");
        assert!(
            crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]["properties"]
                ["compute"]["properties"]["walFrameMaxSize"]["type"]
                == "string"
        );
        let error = GresComputeSpec {
            wal_frame_max_size: Some(ByteSize::ZERO),
            ..Default::default()
        }
        .effective_policy()
        .expect_err("zero");
        assert!(error.contains("spec.compute.walFrameMaxSize"));
    }

    #[test]
    fn pgwire_max_message_size_has_default_override_schema_and_validation() {
        assert!(
            GresComputeSpec::default()
                .effective_policy()
                .expect("defaults")
                .pgwire_max_message_size
                == mebibytes(64)
        );
        let spec = GresComputeSpec {
            pgwire_max_message_size: Some(crabka_units::bytes(37)),
            ..Default::default()
        };
        assert!(
            spec.effective_policy()
                .expect("override")
                .pgwire_max_message_size
                == crabka_units::bytes(37)
        );
        let json = serde_json::to_value(&spec).expect("serialize");
        assert!(json["pgwireMaxMessageSize"] == "37B");
        let crd = serde_json::to_value(Gres::crd()).expect("CRD");
        assert!(
            crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]["properties"]
                ["compute"]["properties"]["pgwireMaxMessageSize"]["type"]
                == "string"
        );
        let error = GresComputeSpec {
            pgwire_max_message_size: Some(ByteSize::ZERO),
            ..Default::default()
        }
        .effective_policy()
        .expect_err("zero");
        assert!(error.contains("spec.compute.pgwireMaxMessageSize"));
    }

    #[test]
    fn pgexec_runtime_policy_has_overrides_schema_and_validation() {
        let spec = GresComputeSpec {
            pgexec_notify_queue_capacity: Some(37),
            pgexec_blocking_query_memory: Some(crabka_units::bytes(34)),
            pgexec_result_page_max: Some(crabka_units::bytes(35)),
            pgexec_join_broadcast_threshold: Some(crabka_units::bytes(36)),
            pgexec_xid_reservation: Some(38),
            pgexec_rowid_reservation: Some(39),
            pgexec_ts_prune_versions_per_row: Some(40),
            pgexec_ts_gc_floor_lag: Some(crabka_units::millis(41)),
            ..Default::default()
        };
        let policy = spec.effective_policy().expect("overrides");
        assert_eq!(policy.pgexec_runtime_policy.notify_queue_capacity, 37);
        assert!(policy.pgexec_runtime_policy.blocking_query_memory == crabka_units::bytes(34));
        assert!(policy.pgexec_runtime_policy.result_page_max == crabka_units::bytes(35));
        assert!(policy.pgexec_runtime_policy.join_broadcast_threshold == crabka_units::bytes(36));
        assert_eq!(policy.pgexec_runtime_policy.xid_reservation, 38);
        assert_eq!(policy.pgexec_runtime_policy.rowid_reservation, 39);
        assert_eq!(policy.pgexec_runtime_policy.ts_prune_versions_per_row, 40);
        assert!(policy.pgexec_runtime_policy.ts_gc_floor_lag == crabka_units::millis(41));

        let crd = serde_json::to_value(Gres::crd()).expect("CRD");
        let fields = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["compute"]["properties"];
        for field in [
            "pgexecNotifyQueueCapacity",
            "pgexecXidReservation",
            "pgexecRowidReservation",
            "pgexecTsPruneVersionsPerRow",
        ] {
            assert!(fields[field]["minimum"].as_f64() == Some(1.0));
        }
        assert!(fields["pgexecTsGcFloorLag"]["type"] == "string");
        for field in [
            "pgexecBlockingQueryMemory",
            "pgexecResultPageMax",
            "pgexecJoinBroadcastThreshold",
        ] {
            assert!(fields[field]["type"] == "string");
        }

        let error = GresComputeSpec {
            pgexec_xid_reservation: Some(0),
            ..Default::default()
        }
        .effective_policy()
        .expect_err("zero");
        assert!(error.contains("spec.compute.pgexecXidReservation"));

        let error = GresComputeSpec {
            pgexec_ts_gc_floor_lag: Some(Time::from_micros(500)),
            ..Default::default()
        }
        .effective_policy()
        .expect_err("fractional millisecond");
        assert!(error.contains("spec.compute.pgexecTsGcFloorLag"));
    }

    #[test]
    fn pgkv_policy_has_defaults_overrides_schema_and_validation() {
        let defaults = GresComputeSpec::default()
            .effective_policy()
            .expect("defaults")
            .pgkv_options;
        assert_eq!(defaults, crabka_pgkv::FjallOptions::default());

        let spec = GresComputeSpec {
            pgkv_max_memtable_size: Some(crabka_units::bytes(37)),
            pgkv_rotate_after_ops: Some(41),
            ..Default::default()
        };
        let policy = spec.effective_policy().expect("override").pgkv_options;
        assert_eq!(policy.max_memtable_size(), crabka_units::bytes(37));
        assert_eq!(policy.rotate_after_ops().get(), 41);
        let json = serde_json::to_value(&spec).expect("serialize");
        assert!(json["pgkvMaxMemtableSize"] == "37B");
        assert!(json["pgkvRotateAfterOps"] == 41);

        let crd = serde_json::to_value(Gres::crd()).expect("CRD");
        let properties = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["compute"]["properties"];
        assert!(properties["pgkvMaxMemtableSize"]["type"] == "string");
        assert!(properties["pgkvRotateAfterOps"]["minimum"].as_f64() == Some(1.0));

        for invalid in [
            GresComputeSpec {
                pgkv_max_memtable_size: Some(ByteSize::ZERO),
                ..Default::default()
            },
            GresComputeSpec {
                pgkv_rotate_after_ops: Some(0),
                ..Default::default()
            },
        ] {
            assert!(invalid.effective_policy().is_err());
        }
    }

    #[test]
    fn compute_wal_producer_policy_uses_shared_defaults_and_rejects_exact_boundaries() {
        const ZERO: &str = "[the value must be equal to 1, but received 0 || the value must be greater than 1, but received 0]";
        const MILLIS_OVERFLOW: &str = "[the value must be equal to 2147483647, but received 2147483648 || the value must be less than 2147483647, but received 2147483648]";
        const NANOS_OVERFLOW: &str = "[the value must be equal to 2147483647000000, but received 2147483648000000 || the value must be less than 2147483647000000, but received 2147483648000000]";
        let exact = |prefix: &str, detail: &str| [prefix, detail].concat();
        let effective = GresComputeSpec::default()
            .effective_policy()
            .expect("default compute policy")
            .wal_producer_retry_policy;
        assert!(effective == crabka_client_producer::ProducerRetryPolicy::default());

        for (policy, expected) in vec![
            (
                GresComputeSpec {
                    wal_producer_request_timeout: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                exact(
                    "spec.compute.walProducerRequestTimeout: request timeout: ",
                    ZERO
                ),
            ),
            (
                GresComputeSpec {
                    wal_producer_retries: Some(-1),
                    ..GresComputeSpec::default()
                },
                "spec.compute.walProducerRetries: producer retries: the value must be greater than -1, but received -1"
                    .to_owned(),
            ),
            (
                GresComputeSpec {
                    wal_producer_retry_backoff: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                exact(
                    "spec.compute.walProducerRetryBackoff: producer retry backoff: ",
                    ZERO
                ),
            ),
            (
                GresComputeSpec {
                    wal_producer_routing_retry_budget: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                exact(
                    "spec.compute.walProducerRoutingRetryBudget: routing retry budget: ",
                    ZERO
                ),
            ),
            (
                GresComputeSpec {
                    wal_producer_init_retry_timeout: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                exact(
                    "spec.compute.walProducerInitRetryTimeout: producer-ID initialization retry timeout: ",
                    ZERO
                ),
            ),
            (
                GresComputeSpec {
                    wal_producer_init_max_backoff: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                exact(
                    "spec.compute.walProducerInitMaxBackoff: producer-ID initialization maximum backoff: ",
                    ZERO
                ),
            ),
            (
                GresComputeSpec {
                    wal_producer_transaction_timeout: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                exact(
                    "spec.compute.walProducerTransactionTimeout: transaction timeout: ",
                    ZERO
                ),
            ),
            (
                GresComputeSpec {
                    wal_producer_request_timeout: Some(Time::from_millis(i64::from(i32::MAX) + 1)),
                    ..GresComputeSpec::default()
                },
                exact(
                    "spec.compute.walProducerRequestTimeout: request timeout: ",
                    MILLIS_OVERFLOW
                ),
            ),
            (
                GresComputeSpec {
                    wal_producer_retry_backoff: Some(Time::from_millis(i64::from(i32::MAX) + 1)),
                    ..GresComputeSpec::default()
                },
                exact(
                    "spec.compute.walProducerRetryBackoff: producer retry backoff: ",
                    NANOS_OVERFLOW
                ),
            ),
            (
                GresComputeSpec {
                    wal_producer_routing_retry_budget: Some(Time::from_millis(i64::from(i32::MAX) + 1)),
                    ..GresComputeSpec::default()
                },
                exact(
                    "spec.compute.walProducerRoutingRetryBudget: routing retry budget: ",
                    NANOS_OVERFLOW
                ),
            ),
            (
                GresComputeSpec {
                    wal_producer_init_retry_timeout: Some(Time::from_millis(i64::from(i32::MAX) + 1)),
                    ..GresComputeSpec::default()
                },
                exact(
                    "spec.compute.walProducerInitRetryTimeout: producer-ID initialization retry timeout: ",
                    NANOS_OVERFLOW
                ),
            ),
            (
                GresComputeSpec {
                    wal_producer_init_max_backoff: Some(Time::from_millis(i64::from(i32::MAX) + 1)),
                    ..GresComputeSpec::default()
                },
                exact(
                    "spec.compute.walProducerInitMaxBackoff: producer-ID initialization maximum backoff: ",
                    NANOS_OVERFLOW
                ),
            ),
            (
                GresComputeSpec {
                    wal_producer_transaction_timeout: Some(Time::from_millis(i64::from(i32::MAX) + 1)),
                    ..GresComputeSpec::default()
                },
                exact(
                    "spec.compute.walProducerTransactionTimeout: transaction timeout: ",
                    MILLIS_OVERFLOW
                ),
            ),
        ] {
            let error = policy.effective_policy().expect_err("boundary must fail");
            let path = expected.split_once(':').map_or(expected.as_str(), |(path, _)| path);
            assert!(error.starts_with(path), "got: {error}");
        }

        let error = GresComputeSpec {
            wal_producer_retry_backoff: Some(crabka_units::millis(2)),
            wal_producer_init_max_backoff: Some(crabka_units::millis(1)),
            ..GresComputeSpec::default()
        }
        .effective_policy()
        .expect_err("cross-field violation must fail");
        assert!(
            error
                == "spec.compute.walProducerRetryBackoff/walProducerInitMaxBackoff: producer retry backoff exceeds producer-ID backoff cap"
        );

        let configured = GresComputeSpec {
            wal_producer_request_timeout: Some(crabka_units::millis(11)),
            wal_producer_retries: Some(12),
            wal_producer_retry_backoff: Some(crabka_units::millis(13)),
            wal_producer_routing_retry_budget: Some(crabka_units::millis(14)),
            wal_producer_init_retry_timeout: Some(crabka_units::millis(15)),
            wal_producer_init_max_backoff: Some(crabka_units::millis(16)),
            wal_producer_transaction_timeout: Some(crabka_units::millis(17)),
            ..GresComputeSpec::default()
        }
        .effective_policy()
        .expect("valid producer policy")
        .wal_producer_retry_policy;
        assert!(
            configured
                == crabka_client_producer::ProducerRetryPolicy::new(
                    Duration::from_millis(11),
                    12,
                    Duration::from_millis(13),
                    Duration::from_millis(14),
                    Duration::from_millis(15),
                    Duration::from_millis(16),
                    Duration::from_millis(17),
                )
                .unwrap()
        );
    }

    #[test]
    fn wal_producer_flush_timeout_has_exact_schema_default_override_and_errors() {
        let crd = serde_json::to_value(Gres::crd()).expect("serialize Gres CRD");
        let field = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["compute"]["properties"]["walProducerFlushTimeout"];
        assert!(field["type"] == "string");

        let default = GresComputeSpec::default()
            .effective_policy()
            .expect("default compute policy")
            .wal_producer_flush_timeout;
        assert!(default.milliseconds() == 50_000);

        let configured = GresComputeSpec {
            wal_producer_flush_timeout: Some(crabka_units::millis(12_345)),
            ..GresComputeSpec::default()
        }
        .effective_policy()
        .expect("configured compute policy")
        .wal_producer_flush_timeout;
        assert!(configured.milliseconds() == 12_345);

        for (value, expected) in [
            (
                0,
                "spec.compute.walProducerFlushTimeout: producer flush timeout: [the value must be equal to 1, but received 0 || the value must be greater than 1, but received 0]",
            ),
            (
                i32::MAX as u64 + 1,
                "spec.compute.walProducerFlushTimeout: producer flush timeout: [the value must be equal to 2147483647, but received 2147483648 || the value must be less than 2147483647, but received 2147483648]",
            ),
        ] {
            let error = GresComputeSpec {
                wal_producer_flush_timeout: Some(Time::from_millis(
                    i64::try_from(value).expect("test value fits i64"),
                )),
                ..GresComputeSpec::default()
            }
            .effective_policy()
            .expect_err("boundary must fail");
            assert!(error == expected, "got: {error}");
        }
    }

    #[test]
    fn wal_producer_dns_timeout_has_exact_schema_default_override_and_error() {
        let crd = serde_json::to_value(Gres::crd()).expect("serialize Gres CRD");
        let field = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["compute"]["properties"]["walProducerDnsTimeout"];
        assert!(field["type"] == "string");

        let default = GresComputeSpec::default()
            .effective_policy()
            .expect("default compute policy")
            .wal_producer_dns_timeout;
        assert!(default == crabka_client_core::ClientDnsTimeout::default());

        let configured = GresComputeSpec {
            wal_producer_dns_timeout: Some(crabka_units::millis(37)),
            ..GresComputeSpec::default()
        }
        .effective_policy()
        .expect("configured compute policy")
        .wal_producer_dns_timeout;
        assert!(configured.milliseconds() == 37);

        let error = GresComputeSpec {
            wal_producer_dns_timeout: Some(Time::ZERO),
            ..GresComputeSpec::default()
        }
        .effective_policy()
        .expect_err("zero DNS timeout");
        assert!(error.starts_with("spec.compute.walProducerDnsTimeout:"));
    }

    #[test]
    fn fdw_broker_dns_timeout_has_exact_schema_default_override_and_error() {
        let crd = serde_json::to_value(Gres::crd()).expect("serialize Gres CRD");
        let field = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["compute"]["properties"]["fdwBrokerDnsTimeout"];
        assert!(field["type"] == "string");

        let defaults = GresComputeSpec::default()
            .effective_policy()
            .expect("default policy");
        assert_eq!(
            defaults.fdw_broker_dns_timeout,
            crabka_client_core::ClientDnsTimeout::default()
        );

        let overridden = GresComputeSpec {
            fdw_broker_dns_timeout: Some(crabka_units::millis(37)),
            ..GresComputeSpec::default()
        }
        .effective_policy()
        .expect("override");
        assert_eq!(overridden.fdw_broker_dns_timeout.milliseconds(), 37);

        let error = GresComputeSpec {
            fdw_broker_dns_timeout: Some(Time::ZERO),
            ..GresComputeSpec::default()
        }
        .effective_policy()
        .expect_err("zero must fail");
        assert!(error.starts_with("spec.compute.fdwBrokerDnsTimeout:"));
    }

    #[test]
    fn schema_fetch_retry_has_exact_schema_defaults_overrides_and_errors() {
        let crd = serde_json::to_value(Gres::crd()).expect("serialize Gres CRD");
        let compute = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["compute"];
        for field in [
            "schemaFetchRetryInitialBackoff",
            "schemaFetchRetryMaxBackoff",
        ] {
            assert_eq!(compute["properties"][field]["type"], "string");
            assert!(
                !compute["required"]
                    .as_array()
                    .is_some_and(|required| required.iter().any(|value| value == field))
            );
        }

        let defaults = GresComputeSpec::default()
            .effective_policy()
            .expect("default policy")
            .schema_fetch_retry_policy;
        assert_eq!(defaults.initial_backoff(), crabka_units::millis(10));
        assert_eq!(defaults.max_backoff(), crabka_units::secs(1));

        let configured = GresComputeSpec {
            schema_fetch_retry_initial_backoff: Some(crabka_units::millis(37)),
            schema_fetch_retry_max_backoff: Some(crabka_units::millis(91)),
            ..GresComputeSpec::default()
        }
        .effective_policy()
        .expect("configured policy")
        .schema_fetch_retry_policy;
        assert_eq!(configured.initial_backoff(), crabka_units::millis(37));
        assert_eq!(configured.max_backoff(), crabka_units::millis(91));

        for (initial, max, path) in [
            (
                Some(Time::ZERO),
                None,
                "spec.compute.schemaFetchRetryInitialBackoff:",
            ),
            (
                None,
                Some(Time::ZERO),
                "spec.compute.schemaFetchRetryMaxBackoff:",
            ),
            (
                Some(Time::from_secs_f64(f64::INFINITY)),
                None,
                "spec.compute.schemaFetchRetryInitialBackoff:",
            ),
            (
                Some(crabka_units::millis(91)),
                Some(crabka_units::millis(37)),
                "spec.compute.schemaFetchRetryInitialBackoff:",
            ),
        ] {
            let error = GresComputeSpec {
                schema_fetch_retry_initial_backoff: initial,
                schema_fetch_retry_max_backoff: max,
                ..GresComputeSpec::default()
            }
            .effective_policy()
            .expect_err("invalid schema fetch retry policy");
            assert!(error.starts_with(path), "{error}");
        }
    }

    #[test]
    fn compute_wal_producer_throughput_round_trips_and_has_exact_schema() {
        for (compression, serialized) in [
            (WalProducerCompression::None, r#""none""#),
            (WalProducerCompression::Gzip, r#""gzip""#),
            (WalProducerCompression::Snappy, r#""snappy""#),
            (WalProducerCompression::Lz4, r#""lz4""#),
            (WalProducerCompression::Zstd, r#""zstd""#),
        ] {
            assert!(serde_json::to_string(&compression).unwrap() == serialized);
            assert!(
                serde_json::from_str::<WalProducerCompression>(serialized).unwrap() == compression
            );
        }

        let policy = GresComputeSpec {
            wal_producer_compression: Some(WalProducerCompression::Zstd),
            wal_producer_linger: Some(Time::from_millis(i64::from(i32::MAX))),
            wal_producer_batch: Some(ByteSize::from_bytes(i32::MAX as u64)),
            ..GresComputeSpec::default()
        };
        let json = serde_json::to_string(&policy).expect("serialize compute policy");
        let yaml = serde_yaml::to_string(&policy).expect("serialize compute policy");
        assert!(serde_json::from_str::<GresComputeSpec>(&json).unwrap() == policy);
        assert!(serde_yaml::from_str::<GresComputeSpec>(&yaml).unwrap() == policy);
        assert!(json.contains(r#""walProducerCompression":"zstd""#));
        assert!(
            serde_json::from_str::<GresComputeSpec>(r#"{"walProducerCompression":null}"#)
                .unwrap()
                .wal_producer_compression
                .is_none()
        );

        let crd = serde_json::to_value(Gres::crd()).expect("serialize Gres CRD");
        let properties = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["compute"]["properties"];
        assert!(
            properties["walProducerCompression"]["enum"]
                == serde_json::json!(["none", "gzip", "snappy", "lz4", "zstd", null])
        );
        assert!(properties["walProducerCompression"]["nullable"] == true);
        assert!(properties["walProducerLinger"]["type"] == "string");
        assert!(properties["walProducerBatch"]["type"] == "string");
    }

    #[test]
    fn compute_wal_producer_throughput_uses_shared_defaults_and_exact_errors() {
        let effective = GresComputeSpec::default()
            .effective_policy()
            .expect("default compute policy")
            .wal_producer_throughput_policy;
        assert!(effective == crabka_client_producer::ProducerThroughputPolicy::default());

        let configured = GresComputeSpec {
            wal_producer_compression: Some(WalProducerCompression::Lz4),
            wal_producer_linger: Some(crabka_units::millis(11)),
            wal_producer_batch: Some(crabka_units::bytes(12)),
            ..GresComputeSpec::default()
        }
        .effective_policy()
        .expect("valid producer throughput policy")
        .wal_producer_throughput_policy;
        assert!(
            configured
                == crabka_client_producer::ProducerThroughputPolicy::new(
                    crabka_client_producer::Compression::Lz4,
                    Duration::from_millis(11),
                    12,
                    crabka_client_producer::DEFAULT_PRODUCER_MAX_IN_FLIGHT,
                )
                .unwrap()
        );

        for (policy, expected) in [
            (
                GresComputeSpec {
                    wal_producer_linger: Some(Time::from_millis(i64::from(i32::MAX) + 1)),
                    ..GresComputeSpec::default()
                },
                "spec.compute.walProducerLinger: producer linger: [the value must be equal to 2147483647, but received 2147483648 || the value must be less than 2147483647, but received 2147483648]",
            ),
            (
                GresComputeSpec {
                    wal_producer_batch: Some(crabka_units::bytes(0)),
                    ..GresComputeSpec::default()
                },
                "spec.compute.walProducerBatch: must be a finite, positive whole number of bytes",
            ),
            (
                GresComputeSpec {
                    wal_producer_batch: Some(ByteSize::from_bytes(i32::MAX as u64 + 1)),
                    ..GresComputeSpec::default()
                },
                "spec.compute.walProducerBatch: producer batch bytes: [the value must be equal to 2147483647, but received 2147483648 || the value must be less than 2147483647, but received 2147483648]",
            ),
        ] {
            let error = policy.effective_policy().expect_err("invalid boundary");
            let path = expected.split_once(':').map_or(expected, |(path, _)| path);
            assert!(error.starts_with(path), "got: {error}");
        }
    }

    #[test]
    fn compute_wal_admin_policy_round_trips_validates_and_uses_substrate_defaults() {
        let policy = GresComputeSpec {
            wal_topic_replication_factor: Some(32_767),
            wal_topic_ensure_timeout: Some(Time::from_millis(i64::from(i32::MAX))),
            wal_admin_connect_timeout: Some(crabka_units::millis(33)),
            wal_admin_request_timeout: Some(crabka_units::millis(44)),
            ..GresComputeSpec::default()
        };
        let json = serde_json::to_string(&policy).expect("serialize compute policy");
        let yaml = serde_yaml::to_string(&policy).expect("serialize compute policy");
        assert!(serde_json::from_str::<GresComputeSpec>(&json).unwrap() == policy);
        assert!(serde_yaml::from_str::<GresComputeSpec>(&yaml).unwrap() == policy);

        let crd = serde_json::to_value(Gres::crd()).expect("serialize Gres CRD");
        let properties = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["compute"]["properties"];
        assert!(properties["walTopicReplicationFactor"]["minimum"].as_f64() == Some(1.0));
        for field in [
            "walTopicEnsureTimeout",
            "walAdminConnectTimeout",
            "walAdminRequestTimeout",
        ] {
            assert!(
                properties[field]["type"] == "string",
                "wrong schema for {field}"
            );
        }
        assert!(properties["walTopicReplicationFactor"]["maximum"].as_f64() == Some(32_767.0));

        let defaults = GresComputeSpec::default()
            .effective_policy()
            .expect("default compute policy");
        assert!(
            defaults.wal_topic_replication_factor.into_value()
                == DEFAULT_WAL_TOPIC_REPLICATION_FACTOR
        );
        assert!(
            defaults.wal_topic_ensure_timeout_ms.into_value()
                == DEFAULT_WAL_TOPIC_ENSURE_TIMEOUT.millis_i32()
        );
        assert!(
            defaults.wal_admin_connect_timeout_ms.into_value()
                == millis_u64(DEFAULT_WAL_ADMIN_CONNECT_TIMEOUT)
        );
        assert!(
            defaults.wal_admin_request_timeout_ms.into_value()
                == millis_u64(DEFAULT_WAL_ADMIN_REQUEST_TIMEOUT)
        );

        for (policy, expected) in [
            (
                GresComputeSpec {
                    wal_topic_replication_factor: Some(0),
                    ..GresComputeSpec::default()
                },
                "spec.compute.walTopicReplicationFactor: [the value must be equal to 1, but received 0 || the value must be greater than 1, but received 0]",
            ),
            (
                GresComputeSpec {
                    wal_topic_ensure_timeout: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                "spec.compute.walTopicEnsureTimeout: [the value must be equal to 1, but received 0 || the value must be greater than 1, but received 0]",
            ),
            (
                GresComputeSpec {
                    wal_admin_connect_timeout: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                "spec.compute.walAdminConnectTimeout: the value must be greater than 0, but received 0",
            ),
            (
                GresComputeSpec {
                    wal_admin_request_timeout: Some(Time::ZERO),
                    ..GresComputeSpec::default()
                },
                "spec.compute.walAdminRequestTimeout: the value must be greater than 0, but received 0",
            ),
        ] {
            let error = policy.effective_policy().expect_err("zero must fail");
            let path = expected.split_once(':').map_or(expected, |(path, _)| path);
            assert!(error.starts_with(path), "got: {error}");
        }
    }

    #[test]
    fn pgdog_runtime_policy_round_trips_through_json_and_yaml() {
        let policy = PgdogSpec {
            image: None,
            replicas: 2,
            listen_port: 6_432,
            tls_secret_ref: None,
            admin_secret_ref: SecretKeyRef {
                name: "pgdog-admin".into(),
                key: "password".into(),
            },
            pooler_mode: Some(PgdogPoolerModeSpec::Session),
            connect_attempts: Some(7),
            idle_timeout: Some(crabka_units::secs(61)),
            suspension_idle_timeout: Some(crabka_units::millis(1_500)),
            server_lifetime: Some(crabka_units::millis(301_000)),
            readiness_probe_period_seconds: Some(6),
            direct_bootstrap_grace: Some(crabka_units::millis(4_500)),
        };

        let json = serde_json::to_string(&policy).expect("serialize JSON");
        let yaml = serde_yaml::to_string(&policy).expect("serialize YAML");

        assert!(json.contains("\"poolerMode\":\"session\""), "got: {json}");
        assert!(json.contains("\"connectAttempts\":7"), "got: {json}");
        assert!(yaml.contains("directBootstrapGrace: 4.5s"), "got: {yaml}");
        assert!(serde_json::from_str::<PgdogSpec>(&json).expect("parse JSON") == policy);
        assert!(serde_yaml::from_str::<PgdogSpec>(&yaml).expect("parse YAML") == policy);
    }

    #[test]
    fn absent_pgdog_runtime_policy_uses_exact_defaults() {
        let policy = PgdogSpec {
            image: None,
            replicas: 1,
            listen_port: 6_432,
            tls_secret_ref: None,
            admin_secret_ref: SecretKeyRef {
                name: "pgdog-admin".into(),
                key: "password".into(),
            },
            pooler_mode: None,
            connect_attempts: None,
            idle_timeout: None,
            suspension_idle_timeout: None,
            server_lifetime: None,
            readiness_probe_period_seconds: None,
            direct_bootstrap_grace: None,
        }
        .effective_policy()
        .expect("default policy");

        assert!(policy.pooler_mode == crabka_gres_control::PgdogPoolerMode::Transaction);
        assert!(policy.connect_attempts.into_value() == 3);
        assert!(policy.idle_timeout.into_value() == 60_000);
        assert!(policy.suspension_idle_timeout.into_value() == 1_000);
        assert!(policy.server_lifetime.into_value() == 300_000);
        assert!(policy.readiness_probe_period_seconds == 5);
        assert!(policy.direct_bootstrap_grace.into_value() == 4_000);
    }

    #[test]
    fn pgdog_runtime_policy_schema_has_exact_bounds_and_enum() {
        let crd = serde_json::to_value(Gres::crd()).expect("serialize Gres CRD");
        let pgdog = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["pgdog"]["properties"];

        assert!(pgdog["poolerMode"]["enum"] == serde_json::json!(["transaction", "session"]));
        assert!(pgdog["connectAttempts"]["minimum"].as_f64() == Some(1.0));
        assert!(pgdog["connectAttempts"]["maximum"].as_f64() == Some(65_535.0));
        assert!(pgdog["readinessProbePeriodSeconds"]["minimum"].as_f64() == Some(1.0));
        for field in [
            "idleTimeout",
            "suspensionIdleTimeout",
            "serverLifetime",
            "directBootstrapGrace",
        ] {
            assert!(pgdog[field]["type"] == "string", "wrong schema for {field}");
        }
    }

    #[test]
    fn status_omits_optional_fields_when_unset() {
        let json = serde_json::to_string(&GresStatus::default()).unwrap();
        assert!(!json.contains("observedGeneration"), "got: {json}");
        assert!(!json.contains("confirmedPgdogConfigHash"), "got: {json}");
        assert!(!json.contains("serviceUrl"), "got: {json}");
        assert!(!json.contains("balancer"), "got: {json}");
    }

    #[test]
    fn compute_range_runtime_policy_round_trips_validates_and_has_schema_types() {
        let spec = GresComputeSpec {
            range_join_key_columns: Some(3),
            range_join_projection_columns: Some(4),
            range_join_predicates: Some(5),
            range_join_snapshot_xids: Some(6),
            range_join_broadcast_rows: Some(7),
            range_join_row_max: Some(crabka_units::kibibytes(8)),
            range_join_result_rows: Some(9),
            range_rpc_frame_max: Some(crabka_units::mebibytes(2)),
            range_rpc_request_timeout: Some(crabka_units::secs(8)),
            range_rpc_server_idle_timeout: Some(crabka_units::secs(30)),
            range_rpc_pool_idle_ttl: Some(crabka_units::secs(3)),
            range_remote_session_max: Some(17),
            range_logical_base_persist_stride: Some(2048),
            range_logical_max_persist_stride: Some(4096),
            ..GresComputeSpec::default()
        };
        let json = serde_json::to_string(&spec).expect("serialize range runtime policy");
        assert!(serde_json::from_str::<GresComputeSpec>(&json).unwrap() == spec);
        let policy = spec.effective_policy().unwrap().range_runtime_policy;
        assert!(policy.rpc_frame_max == crabka_units::mebibytes(2));
        assert!(policy.remote_session_max.get() == 17);
        assert!(policy.logical_max_persist_stride.get() == 4096);
        assert!(policy.join.key_columns == 3);
        assert!(policy.join.projection_columns == 4);
        assert!(policy.join.predicates == 5);
        assert!(policy.join.snapshot_xids == 6);
        assert!(policy.join.broadcast_rows == 7);
        assert!(policy.join.row_bytes == 8192);
        assert!(policy.join.result_rows == 9);

        let invalid = GresComputeSpec {
            range_rpc_request_timeout: Some(crabka_units::secs(2)),
            range0_barrier_reply_budget: Some(crabka_units::secs(2)),
            ..GresComputeSpec::default()
        };
        assert!(invalid.effective_policy().is_err());

        let crd = serde_json::to_value(Gres::crd()).unwrap();
        let properties = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["compute"]["properties"];
        assert!(properties["rangeRpcFrameMax"]["type"] == "string");
        assert!(properties["rangeRpcRequestTimeout"]["type"] == "string");
        assert!(properties["rangeRemoteSessionMax"]["minimum"].as_f64() == Some(1.0));
        assert!(properties["rangeJoinKeyColumns"]["minimum"].as_f64() == Some(1.0));
        assert!(properties["rangeJoinRowMax"]["type"] == "string");
    }
}
