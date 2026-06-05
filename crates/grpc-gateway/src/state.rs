//! Shared, cheaply-cloneable handles for Connect handlers.

use std::sync::Arc;

use crate::config::GatewayConfig;
use crate::produce::ProduceCore;

#[derive(Clone)]
pub struct AppState {
    pub produce: Arc<ProduceCore>,
    pub config: Arc<GatewayConfig>,
}
