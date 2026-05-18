use std::collections::HashMap;
use std::sync::Arc;

use crabka_client_admin::AdminClient;
use kube::Client;
use tokio::sync::Mutex;

use crate::config::OperatorConfig;
use crate::telemetry::SharedRegistry;

/// Shared per-reconciler context. Cheap to clone (all fields Arc /
/// shared via interior mutability).
#[derive(Clone)]
pub struct Context {
    pub client: Client,
    pub config: Arc<OperatorConfig>,
    pub registry: SharedRegistry,
    /// Per-cluster admin-client cache. Keyed by `Kafka` resource name.
    /// Broken connections are replaced lazily on next use.
    pub admin_clients: Arc<Mutex<HashMap<String, Arc<Mutex<AdminClient>>>>>,
}

impl Context {
    #[must_use]
    pub fn new(client: Client, config: OperatorConfig, registry: SharedRegistry) -> Self {
        Self {
            client,
            config: Arc::new(config),
            registry,
            admin_clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Look up or open an `AdminClient` for the named cluster.
    ///
    /// `bootstrap` is the inter-broker listener's `bootstrap_servers`
    /// string, e.g. `demo-broker-headless.default.svc.cluster.local:9092`.
    pub async fn admin_client_for(
        &self,
        cluster: &str,
        bootstrap: &str,
    ) -> Result<Arc<Mutex<AdminClient>>, crabka_client_admin::AdminError> {
        let mut map = self.admin_clients.lock().await;
        if let Some(client) = map.get(cluster) {
            return Ok(client.clone());
        }
        let admin = AdminClient::connect(&[bootstrap.to_string()]).await?;
        let entry = Arc::new(Mutex::new(admin));
        map.insert(cluster.to_string(), entry.clone());
        Ok(entry)
    }

    /// Drop the cached admin client for `cluster` (used by reconcile when
    /// a Transport error indicates the connection died — next call will
    /// reopen).
    pub async fn drop_admin_client(&self, cluster: &str) {
        self.admin_clients.lock().await.remove(cluster);
    }
}
