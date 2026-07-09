//! Shared, cheaply-cloneable handles for Connect handlers.

use std::sync::Arc;

use crate::{
    codec::RecordCodec,
    config::GatewayConfig,
    produce::ProduceCore,
    queue::{QueueSessionConfig, QueueSessionTable},
};

#[derive(Clone)]
pub struct AppState {
    pub produce: Arc<ProduceCore>,
    pub config: Arc<GatewayConfig>,
    /// Trusted-proxy authorizer + ACL-cache holder. Produce/consume handlers
    /// gate each topic/group access through this. Defaults to an
    /// `AllowAllAuthorizer` when authz is unconfigured, so every decision is
    /// `Allow` and the gateway's pre-authz behavior is preserved exactly.
    pub authz: Arc<crate::authz::GatewayAuthz>,
    /// Shared codec for the consume path. Built once at startup from
    /// `GatewayConfig::schema_registry_url`: `SchemaRegistryCodec` when a URL
    /// is configured, `RawCodec` (identity pass-through) otherwise.
    pub codec: Arc<dyn RecordCodec>,
    /// Principal-bound explicit share-consumer sessions backing Queue RPCs.
    pub queue_sessions: Arc<QueueSessionTable>,
}

impl AppState {
    /// Build the queue-session table matching this state's gateway config.
    #[must_use]
    pub fn queue_sessions_from_config(config: &GatewayConfig) -> Arc<QueueSessionTable> {
        Arc::new(QueueSessionTable::new(
            QueueSessionConfig::from_gateway_config(config),
        ))
    }
}
