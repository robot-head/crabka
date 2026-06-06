//! Shared, cheaply-cloneable handles for Connect handlers.

use std::sync::Arc;

use crate::config::GatewayConfig;
use crate::produce::ProduceCore;

#[derive(Clone)]
pub struct AppState {
    pub produce: Arc<ProduceCore>,
    pub config: Arc<GatewayConfig>,
    /// Trusted-proxy authorizer + ACL-cache holder. Produce/consume handlers
    /// gate each topic/group access through this. Defaults to an
    /// `AllowAllAuthorizer` when authz is unconfigured, so every decision is
    /// `Allow` and the gateway's pre-authz behavior is preserved exactly.
    pub authz: Arc<crate::authz::GatewayAuthz>,
}
