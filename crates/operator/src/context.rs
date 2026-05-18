use std::collections::HashMap;
use std::sync::Arc;

use crabka_client_admin::{AdminClient, AdminClientLike};
use kube::Client;
use tokio::sync::Mutex;

use crate::config::OperatorConfig;
use crate::telemetry::SharedRegistry;

/// Boxed-dyn admin client handle: tests substitute a fake here without
/// opening a TCP connection, while production code wraps a real
/// `AdminClient`.
pub type AdminClientHandle = Arc<Mutex<dyn AdminClientLike + Send>>;

/// Shared per-reconciler context. Cheap to clone (all fields Arc /
/// shared via interior mutability).
#[derive(Clone)]
pub struct Context {
    pub client: Client,
    pub config: Arc<OperatorConfig>,
    pub registry: SharedRegistry,
    /// Per-cluster admin-client cache. Keyed by `Kafka` resource name.
    /// Broken connections are replaced lazily on next use.
    pub admin_clients: Arc<Mutex<HashMap<String, AdminClientHandle>>>,
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
    ) -> Result<AdminClientHandle, crabka_client_admin::AdminError> {
        let mut map = self.admin_clients.lock().await;
        if let Some(client) = map.get(cluster) {
            return Ok(client.clone());
        }
        let admin = AdminClient::connect(&[bootstrap.to_string()]).await?;
        let entry: AdminClientHandle = Arc::new(Mutex::new(admin));
        map.insert(cluster.to_string(), entry.clone());
        Ok(entry)
    }

    /// Drop the cached admin client for `cluster` (used by reconcile when
    /// a Transport error indicates the connection died — next call will
    /// reopen).
    pub async fn drop_admin_client(&self, cluster: &str) {
        self.admin_clients.lock().await.remove(cluster);
    }

    /// Test-only: pre-populate the admin-client cache with a caller-supplied
    /// handle. The `AdminClientLike` trait abstracts over the real client
    /// and per-test fakes, so reconcile tests can drive the trait methods
    /// without opening a TCP connection.
    ///
    /// Not cfg-gated: the function exists in the public API but is harmless
    /// (and unused) in production — keeping it un-gated avoids a parallel
    /// test-only build profile.
    pub async fn insert_admin_client_for_test(&self, cluster: &str, admin: AdminClientHandle) {
        self.admin_clients
            .lock()
            .await
            .insert(cluster.to_string(), admin);
    }
}
