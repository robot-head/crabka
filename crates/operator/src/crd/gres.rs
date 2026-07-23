//! `Gres` CRD. Represents one PgDog-backed Gres front door for a Kafka
//! cluster. Tenant CRs point at a `Gres` fleet by name; the controller that
//! renders `PgDog` is added in a later batch.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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

    /// Runtime tuning for the PgDog and Gres activator microservices. Every
    /// operational timeout, retry, probe, and activator port used by the
    /// controller is surfaced here so it can be tuned without rebuilding the
    /// operator.
    #[serde(default)]
    pub runtime: GresRuntimeSpec,

    /// Default tenant runtime settings inherited by `GresTenant`s unless
    /// they set `spec.overrides`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defaults: Option<TenantDefaults>,

    /// Dry-run Gres balancer planning knobs. Live execution is intentionally
    /// not performed by the operator yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub balancer: Option<GresBalancerSpec>,
}

/// Operational tuning values for a Gres fleet's managed microservices.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GresRuntimeSpec {
    /// Container image override for the Gres activator.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activator_image: Option<String>,
    /// TCP port used by the Gres activator service and container.
    #[schemars(range(min = 1, max = 65_535))]
    pub activator_port: i32,
    /// Number of PgDog admin reload attempts before reconciliation requeues.
    #[schemars(range(min = 1, max = 100))]
    pub reload_retry_limit: usize,
    /// Delay between PgDog admin reload attempts in milliseconds.
    pub reload_retry_delay_ms: u64,
    /// Requeue delay after PgDog reload verification fails, in seconds.
    pub reload_requeue_seconds: u64,
    /// Maximum duration of a PgDog admin operation, in seconds.
    pub admin_operation_timeout_seconds: u64,
    /// Credential grace period after a route transition, in milliseconds.
    pub direct_bootstrap_ms: u64,
    /// Maximum time PgDog waits for a cold tenant backend, in seconds.
    pub cold_start_ceiling_seconds: u64,
    /// Idle timeout used while tenant idleness is enabled, in seconds.
    pub idle_timeout_seconds: u64,
    /// Fallback reconcile interval when no credential grace deadline exists.
    pub transition_requeue_seconds: u64,
    /// Kubernetes readiness probe period for PgDog and the activator, in seconds.
    #[schemars(range(min = 1))]
    pub readiness_probe_period_seconds: i32,
}

impl Default for GresRuntimeSpec {
    fn default() -> Self {
        Self {
            activator_image: None,
            activator_port: 6_543,
            reload_retry_limit: 3,
            reload_retry_delay_ms: 100,
            reload_requeue_seconds: 15,
            admin_operation_timeout_seconds: 20,
            direct_bootstrap_ms: 4_000,
            cold_start_ceiling_seconds: 90,
            idle_timeout_seconds: 1,
            transition_requeue_seconds: 60,
            readiness_probe_period_seconds: 5,
        }
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
    fn crd_exposes_runtime_tuning_defaults() {
        let crd = Gres::crd();
        let schema = serde_json::to_value(&crd).expect("CRD serializes");
        let runtime = &schema["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]
            ["spec"]["properties"]["runtime"];
        check!(runtime["default"]["activatorPort"] == 6_543);
        check!(runtime["default"]["reloadRetryLimit"] == 3);
        check!(runtime["default"]["coldStartCeilingSeconds"] == 90);
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
            },
            runtime: GresRuntimeSpec::default(),
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
            json.contains("\"disabledGoals\":[\"load_skew\"]"),
            "got: {json}"
        );
        let back: GresSpec = serde_json::from_str(&json).unwrap();
        assert!(back == spec);
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
