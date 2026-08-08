//! Shared, cheaply-cloneable handles for Connect handlers.

use std::sync::Arc;

use crate::{codec::RecordCodec, config::GatewayConfig, produce::ProduceCore};

#[derive(Clone)]
pub struct AppState {
    pub produce: Arc<ProduceCore>,
    pub config: Arc<GatewayConfig>,
    /// Trusted-proxy authorizer and ACL-cache holder. The produce and consume
    /// handlers gate each topic and group access through it. It defaults to an
    /// `AllowAllAuthorizer` when authz is not configured, so every decision is
    /// `Allow` and the gateway keeps its pre-authz behavior exactly.
    pub authz: Arc<crate::authz::GatewayAuthz>,
    /// Shared codec for the consume path. The gateway builds it once at startup
    /// from `GatewayConfig::schema_registry_url`. It is a
    /// `SchemaRegistryCodec` when a URL is configured, and the identity
    /// pass-through `RawCodec` if there is none.
    pub codec: Arc<dyn RecordCodec>,
    /// Process-local handles for unary share-group queue sessions.
    pub queue: Arc<crate::queue::QueueSessionTable>,
}
