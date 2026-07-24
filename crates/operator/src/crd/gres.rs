//! `Gres` CRD. Represents one PgDog-backed Gres front door for a Kafka
//! cluster. Tenant CRs point at a `Gres` fleet by name; the controller that
//! renders `PgDog` is added in a later batch.

use crabka_gres_control::{
    CheckpointPartBytes, DEFAULT_CHECKPOINT_DELETE_RECORDS_TIMEOUT_MS,
    DEFAULT_CHECKPOINT_POLL_INTERVAL_MS, DEFAULT_IDLE_SUSPEND_POLL_INTERVAL_MS,
    DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL_MS, PgdogConnectAttempts, PgdogPoolerMode, PositiveI32,
    PositiveMillis, PositiveUsize,
};
use crabka_gres_substrate::{DEFAULT_CHECKPOINT_RETAIN, DEFAULT_PART_MAX_BYTES};
use kube::CustomResource;
use refined_type::rule::GreaterI32;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const DEFAULT_LIFECYCLE_REQUEUE_MS: u64 = 5_000;

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
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
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

    /// Registry readiness polling interval in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub registry_poll_ms: Option<u64>,

    /// Maximum duration to hold one cold-starting connection in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub cold_start_timeout_ms: Option<u64>,

    /// Activator readiness probe period in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub readiness_probe_period_seconds: Option<i32>,
}

/// Tenant compute workload policy.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GresComputeSpec {
    /// Compute readiness probe period in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub readiness_probe_period_seconds: Option<i32>,

    /// Maximum checkpoint object part size in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 8))]
    pub checkpoint_part_bytes: Option<usize>,

    /// Number of checkpoint manifests to retain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub checkpoint_retain: Option<usize>,

    /// Kafka `DeleteRecords` timeout in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = 2_147_483_647))]
    pub checkpoint_delete_records_timeout_ms: Option<i32>,

    /// Checkpoint threshold polling interval in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub checkpoint_poll_interval_ms: Option<u64>,

    /// Idle-suspend polling interval in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub idle_suspend_poll_interval_ms: Option<u64>,

    /// Periodic range-0 follower refresh cadence in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub range0_follower_poll_interval_ms: Option<u64>,

    /// Tenant lifecycle reconciliation interval in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub lifecycle_requeue_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct EffectiveGresComputePolicy {
    pub(crate) readiness_probe_period_seconds: i32,
    pub(crate) checkpoint_part_bytes: CheckpointPartBytes,
    pub(crate) checkpoint_retain: PositiveUsize,
    pub(crate) checkpoint_delete_records_timeout_ms: PositiveI32,
    pub(crate) checkpoint_poll_interval_ms: PositiveMillis,
    pub(crate) idle_suspend_poll_interval_ms: PositiveMillis,
    pub(crate) range0_follower_poll_interval_ms: PositiveMillis,
    pub(crate) lifecycle_requeue_ms: PositiveMillis,
}

impl GresComputeSpec {
    pub(crate) fn effective_readiness_probe_period_seconds(&self) -> Result<i32, String> {
        GreaterI32::<0>::new(self.readiness_probe_period_seconds.unwrap_or(5))
            .map_err(|error| format!("spec.compute.readinessProbePeriodSeconds: {error}"))
            .map(refined_type::Refined::into_value)
    }

    pub(crate) fn effective_policy(&self) -> Result<EffectiveGresComputePolicy, String> {
        Ok(EffectiveGresComputePolicy {
            readiness_probe_period_seconds: self.effective_readiness_probe_period_seconds()?,
            checkpoint_part_bytes: CheckpointPartBytes::new(
                self.checkpoint_part_bytes.unwrap_or(DEFAULT_PART_MAX_BYTES),
            )
            .map_err(|error| format!("spec.compute.checkpointPartBytes: {error}"))?,
            checkpoint_retain: PositiveUsize::new(
                self.checkpoint_retain.unwrap_or(DEFAULT_CHECKPOINT_RETAIN),
            )
            .map_err(|error| format!("spec.compute.checkpointRetain: {error}"))?,
            checkpoint_delete_records_timeout_ms: PositiveI32::new(
                self.checkpoint_delete_records_timeout_ms
                    .unwrap_or(DEFAULT_CHECKPOINT_DELETE_RECORDS_TIMEOUT_MS),
            )
            .map_err(|error| format!("spec.compute.checkpointDeleteRecordsTimeoutMs: {error}"))?,
            checkpoint_poll_interval_ms: PositiveMillis::new(
                self.checkpoint_poll_interval_ms
                    .unwrap_or(DEFAULT_CHECKPOINT_POLL_INTERVAL_MS),
            )
            .map_err(|error| format!("spec.compute.checkpointPollIntervalMs: {error}"))?,
            idle_suspend_poll_interval_ms: PositiveMillis::new(
                self.idle_suspend_poll_interval_ms
                    .unwrap_or(DEFAULT_IDLE_SUSPEND_POLL_INTERVAL_MS),
            )
            .map_err(|error| format!("spec.compute.idleSuspendPollIntervalMs: {error}"))?,
            range0_follower_poll_interval_ms: PositiveMillis::new(
                self.range0_follower_poll_interval_ms
                    .unwrap_or(DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL_MS),
            )
            .map_err(|error| format!("spec.compute.range0FollowerPollIntervalMs: {error}"))?,
            lifecycle_requeue_ms: PositiveMillis::new(
                self.lifecycle_requeue_ms
                    .unwrap_or(DEFAULT_LIFECYCLE_REQUEUE_MS),
            )
            .map_err(|error| format!("spec.compute.lifecycleRequeueMs: {error}"))?,
        })
    }
}

/// Dry-run balancer integration settings for a Gres fleet.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
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
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GresBalancerThresholds {
    /// Split ranges larger than this many bytes.
    pub size_ceiling_bytes: u64,
    /// Merge adjacent ranges below this combined byte size.
    pub merge_floor_bytes: u64,
    /// Row stride used when a range has no upper bound.
    pub split_stride_rows: u64,
    /// Load skew percentage tolerated before move planning.
    pub load_skew_hysteresis_pct: u32,
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
            size_ceiling_bytes: 1_073_741_824,
            merge_floor_bytes: 67_108_864,
            split_stride_rows: 1_000_000,
            load_skew_hysteresis_pct: 25,
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
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
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

    /// Idle pooled-server disconnect window in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub idle_timeout_ms: Option<u64>,

    /// Idle timeout used while at least one tenant can suspend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub suspension_idle_timeout_ms: Option<u64>,

    /// Maximum lifetime for pooled backend connections in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub server_lifetime_ms: Option<u64>,

    /// `PgDog` readiness probe period in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub readiness_probe_period_seconds: Option<i32>,

    /// Direct-route credential retention grace in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub direct_bootstrap_grace_ms: Option<u64>,
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
        let idle_timeout = PositiveMillis::new(self.idle_timeout_ms.unwrap_or(60_000))
            .map_err(|error| format!("spec.pgdog.idleTimeoutMs: {error}"))?;
        let suspension_idle_timeout =
            PositiveMillis::new(self.suspension_idle_timeout_ms.unwrap_or(1_000))
                .map_err(|error| format!("spec.pgdog.suspensionIdleTimeoutMs: {error}"))?;
        let server_lifetime = PositiveMillis::new(self.server_lifetime_ms.unwrap_or(300_000))
            .map_err(|error| format!("spec.pgdog.serverLifetimeMs: {error}"))?;
        let readiness_probe_period_seconds =
            GreaterI32::<0>::new(self.readiness_probe_period_seconds.unwrap_or(5))
                .map_err(|error| format!("spec.pgdog.readinessProbePeriodSeconds: {error}"))?
                .into_value();
        let direct_bootstrap_grace =
            PositiveMillis::new(self.direct_bootstrap_grace_ms.unwrap_or(4_000))
                .map_err(|error| format!("spec.pgdog.directBootstrapGraceMs: {error}"))?;
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
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
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
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TenantDefaults {
    /// Replication factor for tenant WAL topics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wal_replication: Option<i32>,

    /// Checkpoint after this many frames when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_frames: Option<u64>,

    /// Checkpoint after this many bytes when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_bytes: Option<u64>,

    /// Keep the tenant warm when its latest checkpoint exceeds this size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suspend_max_checkpoint_bytes: Option<u64>,

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
    use assert2::{assert, check};
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
                idle_timeout_ms: None,
                suspension_idle_timeout_ms: None,
                server_lifetime_ms: None,
                readiness_probe_period_seconds: None,
                direct_bootstrap_grace_ms: None,
            },
            activator: Some(GresActivatorSpec {
                image: Some("example.test/activator:v2".into()),
                replicas: Some(3),
                registry_poll_ms: Some(500),
                cold_start_timeout_ms: Some(45_000),
                readiness_probe_period_seconds: Some(7),
            }),
            compute: Some(GresComputeSpec {
                readiness_probe_period_seconds: Some(11),
                ..GresComputeSpec::default()
            }),
            defaults: Some(TenantDefaults {
                wal_replication: Some(3),
                checkpoint_frames: Some(10_000),
                checkpoint_bytes: None,
                suspend_max_checkpoint_bytes: None,
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
        assert!(
            json.contains(
                "\"activator\":{\"image\":\"example.test/activator:v2\",\"replicas\":3,\"registryPollMs\":500,\"coldStartTimeoutMs\":45000,\"readinessProbePeriodSeconds\":7}"
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
        for field in [
            "replicas",
            "registryPollMs",
            "coldStartTimeoutMs",
            "readinessProbePeriodSeconds",
        ] {
            assert!(
                activator["properties"][field]["minimum"].as_f64() == Some(1.0),
                "missing minimum for {field}: {activator}"
            );
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
            checkpoint_part_bytes: Some(8),
            checkpoint_retain: Some(1),
            checkpoint_delete_records_timeout_ms: Some(i32::MAX),
            checkpoint_poll_interval_ms: Some(1),
            idle_suspend_poll_interval_ms: Some(1),
            range0_follower_poll_interval_ms: Some(1),
            lifecycle_requeue_ms: Some(1),
            ..GresComputeSpec::default()
        };
        let json = serde_json::to_string(&policy).expect("serialize compute policy");
        assert!(serde_json::from_str::<GresComputeSpec>(&json).unwrap() == policy);

        let crd = serde_json::to_value(Gres::crd()).expect("serialize Gres CRD");
        let properties = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["compute"]["properties"];
        assert!(
            properties["checkpointPartBytes"]["minimum"].as_f64() == Some(8.0),
            "got: {properties}"
        );
        for field in [
            "checkpointRetain",
            "checkpointDeleteRecordsTimeoutMs",
            "checkpointPollIntervalMs",
            "idleSuspendPollIntervalMs",
            "range0FollowerPollIntervalMs",
            "lifecycleRequeueMs",
        ] {
            assert!(
                properties[field]["minimum"].as_f64() == Some(1.0),
                "missing minimum for {field}: {properties}"
            );
        }
        assert!(
            properties["checkpointDeleteRecordsTimeoutMs"]["maximum"].as_f64()
                == Some(f64::from(i32::MAX))
        );
    }

    #[test]
    fn compute_checkpoint_lifecycle_policy_uses_exact_defaults_and_rejects_boundaries() {
        let defaults = GresComputeSpec::default()
            .effective_policy()
            .expect("default compute policy");
        assert!(defaults.checkpoint_part_bytes.into_value() == DEFAULT_PART_MAX_BYTES);
        assert!(defaults.checkpoint_retain.into_value() == DEFAULT_CHECKPOINT_RETAIN);
        assert!(
            defaults.checkpoint_delete_records_timeout_ms.into_value()
                == DEFAULT_CHECKPOINT_DELETE_RECORDS_TIMEOUT_MS
        );
        assert!(
            defaults.checkpoint_poll_interval_ms.into_value()
                == DEFAULT_CHECKPOINT_POLL_INTERVAL_MS
        );
        assert!(
            defaults.idle_suspend_poll_interval_ms.into_value()
                == DEFAULT_IDLE_SUSPEND_POLL_INTERVAL_MS
        );
        assert!(
            defaults.range0_follower_poll_interval_ms.into_value()
                == DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL_MS
        );
        assert!(defaults.lifecycle_requeue_ms.into_value() == DEFAULT_LIFECYCLE_REQUEUE_MS);

        for (policy, path) in [
            (
                GresComputeSpec {
                    checkpoint_part_bytes: Some(7),
                    ..GresComputeSpec::default()
                },
                "spec.compute.checkpointPartBytes",
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
                    checkpoint_delete_records_timeout_ms: Some(0),
                    ..GresComputeSpec::default()
                },
                "spec.compute.checkpointDeleteRecordsTimeoutMs",
            ),
            (
                GresComputeSpec {
                    checkpoint_poll_interval_ms: Some(0),
                    ..GresComputeSpec::default()
                },
                "spec.compute.checkpointPollIntervalMs",
            ),
            (
                GresComputeSpec {
                    idle_suspend_poll_interval_ms: Some(0),
                    ..GresComputeSpec::default()
                },
                "spec.compute.idleSuspendPollIntervalMs",
            ),
            (
                GresComputeSpec {
                    range0_follower_poll_interval_ms: Some(0),
                    ..GresComputeSpec::default()
                },
                "spec.compute.range0FollowerPollIntervalMs",
            ),
            (
                GresComputeSpec {
                    lifecycle_requeue_ms: Some(0),
                    ..GresComputeSpec::default()
                },
                "spec.compute.lifecycleRequeueMs",
            ),
        ] {
            let error = policy.effective_policy().expect_err("boundary must fail");
            assert!(error.contains(path), "got: {error}");
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
            idle_timeout_ms: Some(61_000),
            suspension_idle_timeout_ms: Some(1_500),
            server_lifetime_ms: Some(301_000),
            readiness_probe_period_seconds: Some(6),
            direct_bootstrap_grace_ms: Some(4_500),
        };

        let json = serde_json::to_string(&policy).expect("serialize JSON");
        let yaml = serde_yaml::to_string(&policy).expect("serialize YAML");

        assert!(json.contains("\"poolerMode\":\"session\""), "got: {json}");
        assert!(json.contains("\"connectAttempts\":7"), "got: {json}");
        assert!(yaml.contains("directBootstrapGraceMs: 4500"), "got: {yaml}");
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
            idle_timeout_ms: None,
            suspension_idle_timeout_ms: None,
            server_lifetime_ms: None,
            readiness_probe_period_seconds: None,
            direct_bootstrap_grace_ms: None,
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
        for field in [
            "idleTimeoutMs",
            "suspensionIdleTimeoutMs",
            "serverLifetimeMs",
            "readinessProbePeriodSeconds",
            "directBootstrapGraceMs",
        ] {
            assert!(
                pgdog[field]["minimum"].as_f64() == Some(1.0),
                "missing minimum for {field}: {pgdog}"
            );
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
