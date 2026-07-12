//! Control-plane tenant registry for Chapter Gres.

pub mod error;
pub mod pgdog;
pub mod record;
pub mod registry;

pub use error::ControlError;
pub use pgdog::{
    PgdogGeneral, PgdogPoolerMode, PgdogRenderInput, PgdogTimeouts, PgdogUser, TenantEndpoint,
    render_pgdog_toml, render_users_toml,
};
pub use record::{
    FinalCheckpoint, HashPlacement, RangeBoundary, RangeLayoutEntry, RangeLayoutMerge,
    RangeLayoutMutation, RangeLayoutSplit, RangeLifecycle, RangeRetirement, RegistryKey,
    SplitOperationPhase, SplitOperationRecord, SplitState, SqlUser, TENANT_CONFIG_TOPIC_PREFIX,
    TENANT_REGISTRY_TOPIC, TenantId, TenantName, TenantRecord, TenantState, decode_registry_record,
    decode_tenant_config_record, encode_registry_record, encode_tenant_config_record,
    tenant_config_topic, tenant_registry_key,
};
pub use registry::{InMemoryRegistryStore, Registry, TenantRegistryStore, fold};
