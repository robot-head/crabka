use std::sync::Arc;

use kube::Client;

use crate::config::OperatorConfig;
use crate::telemetry::SharedRegistry;

/// Shared per-reconciler context. Cheap to clone (all fields Arc).
#[derive(Clone)]
pub struct Context {
    pub client: Client,
    pub config: Arc<OperatorConfig>,
    pub registry: SharedRegistry,
}

impl Context {
    #[must_use]
    pub fn new(client: Client, config: OperatorConfig, registry: SharedRegistry) -> Self {
        Self {
            client,
            config: Arc::new(config),
            registry,
        }
    }
}
