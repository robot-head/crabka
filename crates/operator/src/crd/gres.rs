//! `Gres` CRD. Represents one PgDog-backed Gres front door for a Kafka
//! cluster. Tenant CRs point at a `Gres` fleet by name; the controller that
//! renders `PgDog` is added in a later batch.

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
    DEFAULT_PART_MAX_SIZE, DEFAULT_WAL_ADMIN_CONNECT_TIMEOUT, DEFAULT_WAL_ADMIN_REQUEST_TIMEOUT,
    DEFAULT_WAL_RECOVERY_CONNECT_TIMEOUT, DEFAULT_WAL_RECOVERY_DNS_TIMEOUT,
    DEFAULT_WAL_RECOVERY_EMPTY_FETCH_RETRIES, DEFAULT_WAL_RECOVERY_FETCH_MAX_WAIT,
    DEFAULT_WAL_RECOVERY_FETCH_PARTITION_MAX, DEFAULT_WAL_RECOVERY_FETCH_RESPONSE_MAX,
    DEFAULT_WAL_RECOVERY_REQUEST_TIMEOUT, DEFAULT_WAL_TOPIC_ENSURE_TIMEOUT,
    DEFAULT_WAL_TOPIC_REPLICATION_FACTOR,
};
use crabka_units::{
    ByteSize, Ratio, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
    gibibytes, mebibytes, percent,
};
use kube::CustomResource;
use refined_type::rule::GreaterI32;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[cfg(test)]
use crate::controller::common::millis_u64;

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

    /// Default tenant runtime settings inherited by `GresTenant`s unless
    /// they set `spec.overrides`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defaults: Option<TenantDefaults>,

    /// Dry-run Gres balancer planning knobs. Live execution is intentionally
    /// not performed by the operator yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub balancer: Option<GresBalancerSpec>,
}

/// Wake activator deployment and runtime policy.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GresActivatorSpec {
    /// Container image override. When absent the operator uses its global
    /// activator image override or compiled default.
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
    pub(crate) checkpoint_part_size: CheckpointPartBytes,
    pub(crate) checkpoint_retain: PositiveUsize,
    pub(crate) checkpoint_delete_records_timeout_ms: PositiveI32,
    pub(crate) checkpoint_poll_interval_ms: PositiveMillis,
    pub(crate) idle_suspend_poll_interval_ms: PositiveMillis,
    pub(crate) range0_follower_poll_interval_ms: PositiveMillis,
    pub(crate) range0_follower_rebuild_backoff_floor_ms: PositiveMillis,
    pub(crate) range0_follower_rebuild_backoff_ceiling_ms: PositiveMillis,
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
        let schema_fetch_retry_defaults =
            crabka_schema_serde::SchemaFetchRetryPolicy::default();
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

        Ok(EffectiveGresComputePolicy {
            readiness_probe_period_seconds: self.effective_readiness_probe_period_seconds()?,
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
                    .unwrap_or_else(|| {
                        Time::from_std(crabka_client_core::ClientDnsTimeout::default().duration())
                    })
                    .to_std(),
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
                    .unwrap_or_else(|| {
                        Time::from_std(crabka_client_core::ClientDnsTimeout::default().duration())
                    })
                    .to_std(),
            )
            .map_err(|error| format!("spec.compute.walProducerDnsTimeout: {error}"))?,
            wal_producer_retry_policy: self.effective_wal_producer_retry_policy()?,
            wal_producer_throughput_policy: self.effective_wal_producer_throughput_policy()?,
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
    /// Merge operation executable; the operator reports plans only.
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
    /// Container image override. When absent the operator uses
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

    /// Idle timeout in seconds; unset means never suspend by idleness.
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
    /// True while the operator reports planner status without executing live changes.
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
    /// until physical orchestration is implemented.
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
        let back: GresSpec = serde_json::from_str(&json).unwrap();
        assert!(back == spec);
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

        for (policy, expected) in [
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
}
