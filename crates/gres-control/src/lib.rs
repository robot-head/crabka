//! Control-plane tenant registry for Chapter Gres.

mod checkpoint;
pub mod error;
pub mod pgdog;
pub mod record;
pub mod registry;

/// Default periodic range-0 follower refresh cadence.
pub const DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL: crabka_units::Time = crabka_units::millis(100);
/// Default delay before retrying consecutive range-0 follower rebuilds.
pub const DEFAULT_RANGE0_FOLLOWER_REBUILD_BACKOFF_FLOOR: crabka_units::Time =
    crabka_units::millis(250);
/// Default ceiling for consecutive range-0 follower rebuild backoff.
pub const DEFAULT_RANGE0_FOLLOWER_REBUILD_BACKOFF_CEILING: crabka_units::Time =
    crabka_units::secs(30);

pub use checkpoint::{
    CheckpointPartBytes, DEFAULT_CHECKPOINT_BYTES, DEFAULT_CHECKPOINT_DELETE_RECORDS_TIMEOUT,
    DEFAULT_CHECKPOINT_FRAMES, DEFAULT_CHECKPOINT_POLL_INTERVAL,
    DEFAULT_IDLE_SUSPEND_POLL_INTERVAL, PositiveUsize,
};
pub use error::ControlError;
pub use pgdog::{
    PgdogConnectAttempts, PgdogGeneral, PgdogPoolerMode, PgdogRenderInput, PgdogTimeouts,
    PgdogUser, TenantEndpoint, render_pgdog_toml, render_users_toml,
};
pub use record::{
    FinalCheckpoint, HashPlacement, MoveRangeState, RangeBoundary, RangeLayoutEntry,
    RangeLayoutMerge, RangeLayoutMutation, RangeLayoutSplit, RangeLifecycle, RangeMutationPlan,
    RangeRetirement, RangeRetirementCheckpoint, RangeRetirementPhase, RangeRetirementRecord,
    RegistryKey, SplitOperationEvidence, SplitOperationPhase, SplitOperationPlan,
    SplitOperationRecord, SplitState, SqlUser, TENANT_CONFIG_TOPIC_PREFIX, TENANT_REGISTRY_TOPIC,
    TenantId, TenantName, TenantRecord, TenantState, decode_registry_record,
    decode_tenant_config_record, encode_registry_record, encode_tenant_config_record,
    tenant_config_topic, tenant_registry_key,
};
pub use registry::{
    InMemoryRegistryStore, PositiveI32, PositiveMillis, Registry, RegistryPolicy,
    RegistryReplicationFactor, TenantRegistryStore, fold,
};
