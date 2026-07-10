use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use crabka_client_admin::{AdminClient, AdminClientLike};
use crabka_gres_substrate::checkpoint::{Manifest, ManifestValidation};
use crabka_object_store::{
    GcsConfig, ObjectStoreConfig, S3Config, build_object_store, read_capped,
};
use kube::Client;
use object_store::path::Path;
use tokio::sync::Mutex;

use crate::{
    config::{GresCheckpointStoreKind, OperatorConfig},
    rebalancer_client::{ConnectRebalancerClient, RebalancerClientLike},
    telemetry::{ControllerMetrics, SharedRegistry},
};

/// Boxed-dyn admin client handle: tests substitute a fake here without
/// opening a TCP connection, while production code wraps a real
/// `AdminClient`.
pub type AdminClientHandle = Arc<Mutex<dyn AdminClientLike + Send>>;

/// Boxed-dyn rebalancer client handle. Production wraps a
/// [`ConnectRebalancerClient`]; reconcile tests substitute a fake. No
/// `Mutex` — the client's methods take `&self` and the inner HTTP client
/// is a shareable connection pool.
pub type RebalancerClientHandle = Arc<dyn RebalancerClientLike>;

/// Gres control-plane write seam. Production writes Kafka records with the
/// idempotent producer; tests install an in-memory recorder.
pub type GresControlHandle = Arc<dyn GresControlLike>;

/// Narrow durable-checkpoint verification seam. Implementations retrieve and
/// decode the referenced object before comparing its tenant and checkpoint
/// metadata with the registry record.
pub type CheckpointManifestVerifierHandle = Arc<dyn CheckpointManifestVerifier>;

/// Boxed `PgDog` admin seam. Production uses the `PgDog` admin `PostgreSQL`
/// endpoint; tests install a deterministic fake that can report stale views.
pub type PgdogAdminHandle = Arc<dyn PgdogAdminLike>;

#[async_trait::async_trait]
pub trait CheckpointManifestVerifier: Send + Sync {
    /// Verify that the durable manifest matches `record` exactly.
    async fn validate(
        &self,
        record: &crabka_gres_control::TenantRecord,
    ) -> Result<(), CheckpointManifestError>;
}

/// Why the durable checkpoint required for WAL parking could not be verified.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointManifestError {
    /// The operator was not configured with a durable checkpoint object store.
    #[error(
        "Gres checkpoint verifier is not configured: set GRES_CHECKPOINT_STORE, GRES_CHECKPOINT_BUCKET, and provider settings"
    )]
    Unconfigured,
    /// A provider-specific required setting was absent.
    #[error("Gres checkpoint verifier configuration is invalid: {0}")]
    InvalidConfiguration(String),
    /// The object store client could not be constructed.
    #[error("Gres checkpoint object store configuration: {0}")]
    ObjectStoreConfiguration(#[from] crabka_object_store::ObjectStoreError),
    /// The referenced checkpoint is absent, corrupt, or does not match its registry record.
    #[error("Gres checkpoint manifest verification failed: {0}")]
    Verification(String),
}

#[derive(Debug)]
struct UnavailableCheckpointManifestVerifier {
    reason: String,
    unconfigured: bool,
}

#[async_trait::async_trait]
impl CheckpointManifestVerifier for UnavailableCheckpointManifestVerifier {
    async fn validate(
        &self,
        _record: &crabka_gres_control::TenantRecord,
    ) -> Result<(), CheckpointManifestError> {
        if self.unconfigured {
            return Err(CheckpointManifestError::Unconfigured);
        }
        Err(CheckpointManifestError::InvalidConfiguration(
            self.reason.clone(),
        ))
    }
}

struct ObjectStoreCheckpointManifestVerifier {
    store: Arc<dyn object_store::ObjectStore>,
}

impl ObjectStoreCheckpointManifestVerifier {
    fn from_config(config: &OperatorConfig) -> Result<Self, CheckpointManifestError> {
        let Some(kind) = config.gres_checkpoint_store else {
            return Err(CheckpointManifestError::Unconfigured);
        };
        let bucket = required_config(
            config.gres_checkpoint_bucket.as_ref(),
            "GRES_CHECKPOINT_BUCKET",
        )?;
        let store_config = match kind {
            GresCheckpointStoreKind::S3 => {
                let (access_key_id, secret_access_key) = s3_credentials(config)?;
                ObjectStoreConfig::S3(S3Config {
                    bucket,
                    region: required_config(
                        config.gres_checkpoint_region.as_ref(),
                        "GRES_CHECKPOINT_REGION",
                    )?,
                    endpoint: config.gres_checkpoint_endpoint.clone(),
                    access_key_id,
                    secret_access_key,
                    allow_http: config.gres_checkpoint_allow_http,
                    ..Default::default()
                })
            }
            GresCheckpointStoreKind::Gcs => {
                let (service_account_path, application_credentials_path) = gcs_credentials(config)?;
                ObjectStoreConfig::Gcs(GcsConfig {
                    bucket,
                    service_account_path,
                    application_credentials_path,
                    endpoint: config.gres_checkpoint_endpoint.clone(),
                    allow_http: config.gres_checkpoint_allow_http,
                    ..Default::default()
                })
            }
        };
        Ok(Self {
            store: build_object_store(&store_config)?,
        })
    }
}

fn optional_config(value: Option<&String>) -> Option<String> {
    value
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn s3_credentials(
    config: &OperatorConfig,
) -> Result<(Option<String>, Option<String>), CheckpointManifestError> {
    let access_key_id = optional_config(config.gres_checkpoint_access_key_id.as_ref());
    let secret_access_key = optional_config(config.gres_checkpoint_secret_access_key.as_ref());
    if access_key_id.is_some() != secret_access_key.is_some() {
        return Err(CheckpointManifestError::InvalidConfiguration(
            "GRES_CHECKPOINT_ACCESS_KEY_ID and GRES_CHECKPOINT_SECRET_ACCESS_KEY must be set together"
                .into(),
        ));
    }
    Ok((access_key_id, secret_access_key))
}

fn gcs_credentials(
    config: &OperatorConfig,
) -> Result<(Option<String>, Option<String>), CheckpointManifestError> {
    let service_account_path =
        optional_config(config.gres_checkpoint_gcs_service_account_path.as_ref());
    let application_credentials_path = optional_config(
        config
            .gres_checkpoint_gcs_application_credentials_path
            .as_ref(),
    );
    if service_account_path.is_some() && application_credentials_path.is_some() {
        return Err(CheckpointManifestError::InvalidConfiguration(
            "GRES_CHECKPOINT_GCS_SERVICE_ACCOUNT_PATH conflicts with GRES_CHECKPOINT_GCS_APPLICATION_CREDENTIALS_PATH"
                .into(),
        ));
    }
    Ok((service_account_path, application_credentials_path))
}

fn required_config(value: Option<&String>, name: &str) -> Result<String, CheckpointManifestError> {
    let Some(value) = value
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Err(CheckpointManifestError::InvalidConfiguration(format!(
            "{name} is required"
        )));
    };
    Ok(value.to_owned())
}

#[async_trait::async_trait]
impl CheckpointManifestVerifier for ObjectStoreCheckpointManifestVerifier {
    async fn validate(
        &self,
        record: &crabka_gres_control::TenantRecord,
    ) -> Result<(), CheckpointManifestError> {
        let checkpoint = record.final_checkpoint.as_ref().ok_or_else(|| {
            CheckpointManifestError::Verification("registry record has no final checkpoint".into())
        })?;
        let manifest_path = Path::from(checkpoint.manifest_key.as_str());
        let manifest_bytes = read_capped(&self.store, &manifest_path, checkpoint.total_bytes)
            .await
            .map_err(|error| CheckpointManifestError::Verification(error.to_string()))?;
        let manifest = Manifest::decode(&manifest_bytes)
            .map_err(|error| CheckpointManifestError::Verification(error.to_string()))?;
        if manifest.tenant != record.name.as_str()
            || manifest.wal_generation != checkpoint.wal_generation
            || manifest.covered_offset != checkpoint.covered_offset
        {
            return Err(CheckpointManifestError::Verification(
                "manifest tenant, WAL generation, or covered offset does not match the registry checkpoint".into(),
            ));
        }

        let mut parts = BTreeMap::new();
        let mut actual_bytes = u64::try_from(manifest_bytes.len()).map_err(|_| {
            CheckpointManifestError::Verification("manifest byte length overflow".into())
        })?;
        for part in &manifest.parts {
            let part_bytes = read_capped(
                &self.store,
                &Path::from(part.name.as_str()),
                part.encoded_bytes,
            )
            .await
            .map_err(|error| CheckpointManifestError::Verification(error.to_string()))?;
            actual_bytes = actual_bytes
                .checked_add(u64::try_from(part_bytes.len()).map_err(|_| {
                    CheckpointManifestError::Verification(
                        "checkpoint part byte length overflow".into(),
                    )
                })?)
                .ok_or_else(|| {
                    CheckpointManifestError::Verification("checkpoint byte size overflow".into())
                })?;
            parts.insert(part.name.clone(), part_bytes.to_vec());
        }
        if actual_bytes != checkpoint.total_bytes {
            return Err(CheckpointManifestError::Verification(
                "checkpoint byte total does not match the registry checkpoint".into(),
            ));
        }
        manifest
            .validate(&ManifestValidation {
                tenant: record.name.as_str(),
                wal_generation: checkpoint.wal_generation,
                log_start: None,
                parts_by_name: &parts,
            })
            .map_err(|error| CheckpointManifestError::Verification(error.to_string()))?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgdogReloadRequest {
    pub host: String,
    pub port: u16,
    pub password: String,
    pub expected_databases: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum PgdogAdminError {
    #[error("pgdog admin connection: {0}")]
    Connect(#[from] tokio_postgres::Error),
}

#[async_trait::async_trait]
pub trait PgdogAdminLike: Send + Sync {
    async fn reload_and_database_view_matches(
        &self,
        request: &PgdogReloadRequest,
    ) -> Result<bool, PgdogAdminError>;
}

#[derive(Debug, Default)]
struct TokioPostgresPgdogAdmin;

#[async_trait::async_trait]
impl PgdogAdminLike for TokioPostgresPgdogAdmin {
    async fn reload_and_database_view_matches(
        &self,
        request: &PgdogReloadRequest,
    ) -> Result<bool, PgdogAdminError> {
        let mut config = tokio_postgres::Config::new();
        config
            .host(&request.host)
            .port(request.port)
            .user("admin")
            .password(&request.password)
            .dbname("admin");
        let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::debug!(error = %err, "pgdog admin connection task ended");
            }
        });
        client.simple_query("RELOAD").await?;
        let rows = client.query("SHOW DATABASES", &[]).await?;
        let observed_databases = rows
            .iter()
            .filter_map(|row| row.try_get::<usize, String>(0).ok())
            .collect::<std::collections::BTreeSet<_>>();
        Ok(request
            .expected_databases
            .iter()
            .all(|database| observed_databases.contains(database)))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GresControlWriteError {
    #[error("control record: {0}")]
    Control(#[from] crabka_gres_control::ControlError),
    #[error("producer: {0}")]
    Producer(#[from] crabka_client_producer::ProducerError),
    #[error("producer completion channel closed: {0}")]
    Completion(#[from] tokio::sync::oneshot::error::RecvError),
    #[error("durable checkpoint manifest: {0}")]
    CheckpointManifest(#[from] CheckpointManifestError),
}

#[async_trait::async_trait]
pub trait GresControlLike: Send + Sync {
    async fn get_tenant(
        &self,
        tenant: &crabka_gres_control::TenantName,
    ) -> Result<Option<crabka_gres_control::TenantRecord>, GresControlWriteError>;
    /// Create a record or replace the exact version that the reconciler read.
    async fn replace_tenant_if_version(
        &self,
        record: &crabka_gres_control::TenantRecord,
        expected_record_version: Option<u64>,
    ) -> Result<crabka_gres_control::TenantRecord, GresControlWriteError>;
    async fn delete_tenant(
        &self,
        tenant: &crabka_gres_control::TenantName,
    ) -> Result<(), GresControlWriteError>;
    /// Retrieve and validate the durable final checkpoint manifest referenced by
    /// `record`. Implementations must verify manifest identity and checkpoint
    /// metadata; registry metadata alone is not proof that it is durable.
    async fn validate_final_checkpoint_manifest(
        &self,
        record: &crabka_gres_control::TenantRecord,
    ) -> Result<(), GresControlWriteError>;
}

struct KafkaGresControl {
    registry: Mutex<crabka_gres_control::Registry>,
    checkpoint_manifest_verifier: CheckpointManifestVerifierHandle,
}

#[async_trait::async_trait]
impl GresControlLike for KafkaGresControl {
    async fn get_tenant(
        &self,
        tenant: &crabka_gres_control::TenantName,
    ) -> Result<Option<crabka_gres_control::TenantRecord>, GresControlWriteError> {
        Ok(self.registry.lock().await.get(tenant.as_str()).await?)
    }

    async fn replace_tenant_if_version(
        &self,
        record: &crabka_gres_control::TenantRecord,
        expected_record_version: Option<u64>,
    ) -> Result<crabka_gres_control::TenantRecord, GresControlWriteError> {
        let mut registry = self.registry.lock().await;
        registry.ensure_topic(record.wal_replication).await?;
        let stored_record = registry
            .replace_if_version(record, expected_record_version)
            .await?;
        registry
            .upsert_tenant_config(&stored_record, stored_record.wal_replication)
            .await?;
        Ok(stored_record)
    }

    async fn delete_tenant(
        &self,
        tenant: &crabka_gres_control::TenantName,
    ) -> Result<(), GresControlWriteError> {
        self.registry.lock().await.delete(tenant.as_str()).await?;
        Ok(())
    }

    async fn validate_final_checkpoint_manifest(
        &self,
        record: &crabka_gres_control::TenantRecord,
    ) -> Result<(), GresControlWriteError> {
        self.checkpoint_manifest_verifier
            .validate(record)
            .await
            .map_err(GresControlWriteError::CheckpointManifest)
    }
}

/// Shared per-reconciler context. Cheap to clone (all fields Arc /
/// shared via interior mutability).
#[derive(Clone)]
pub struct Context {
    pub client: Client,
    pub config: Arc<OperatorConfig>,
    pub registry: SharedRegistry,
    /// Operator-wide controller metrics (reconcile counters/histograms/gauges).
    /// Cheaply cloneable; the handles are registered against `registry`.
    pub metrics: ControllerMetrics,
    /// Per-cluster admin-client cache. Keyed by `Kafka` resource name.
    /// Broken connections are replaced lazily on next use.
    pub admin_clients: Arc<Mutex<HashMap<String, AdminClientHandle>>>,
    /// Per-endpoint rebalancer-client cache. Keyed by the
    /// resolved Connect base URL. Dropped + re-created lazily on
    /// transport failure.
    pub rebalancer_clients: Arc<Mutex<HashMap<String, RebalancerClientHandle>>>,
    pub gres_controls: Arc<Mutex<HashMap<String, GresControlHandle>>>,
    pub checkpoint_manifest_verifier: CheckpointManifestVerifierHandle,
    pub pgdog_admin: PgdogAdminHandle,
}

impl Context {
    #[must_use]
    pub fn new(
        client: Client,
        config: OperatorConfig,
        registry: SharedRegistry,
        metrics: ControllerMetrics,
    ) -> Self {
        let config = Arc::new(config);
        Self {
            client,
            checkpoint_manifest_verifier: checkpoint_manifest_verifier(&config),
            config,
            registry,
            metrics,
            admin_clients: Arc::new(Mutex::new(HashMap::new())),
            rebalancer_clients: Arc::new(Mutex::new(HashMap::new())),
            gres_controls: Arc::new(Mutex::new(HashMap::new())),
            pgdog_admin: Arc::new(TokioPostgresPgdogAdmin),
        }
    }

    #[must_use]
    pub fn with_pgdog_admin_for_test(mut self, pgdog_admin: PgdogAdminHandle) -> Self {
        self.pgdog_admin = pgdog_admin;
        self
    }

    #[must_use]
    pub fn with_checkpoint_manifest_verifier_for_test(
        mut self,
        checkpoint_manifest_verifier: CheckpointManifestVerifierHandle,
    ) -> Self {
        self.checkpoint_manifest_verifier = checkpoint_manifest_verifier;
        self
    }

    /// Look up or open an `AdminClient` for the named cluster.
    ///
    /// `bootstrap` is the inter-broker listener's `bootstrap_servers`
    /// string, e.g. `demo-broker-headless.default.svc.cluster.local:9092`.
    /// # Errors
    /// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
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

    /// Look up or build a rebalancer client for `endpoint` (a Connect base
    /// URL like `http://host:9300`). Construction is infallible (no
    /// connection is opened until the first RPC), so this returns the
    /// handle directly.
    pub async fn rebalancer_client_for(&self, endpoint: &str) -> RebalancerClientHandle {
        let mut map = self.rebalancer_clients.lock().await;
        if let Some(client) = map.get(endpoint) {
            return client.clone();
        }
        let client: RebalancerClientHandle = Arc::new(ConnectRebalancerClient::new(endpoint));
        map.insert(endpoint.to_string(), client.clone());
        client
    }

    /// Drop the cached rebalancer client for `endpoint` (used by reconcile
    /// after a transport error — the next call rebuilds it).
    pub async fn drop_rebalancer_client(&self, endpoint: &str) {
        self.rebalancer_clients.lock().await.remove(endpoint);
    }

    /// Test-only: pre-populate the rebalancer-client cache with a fake.
    /// Mirrors [`Self::insert_admin_client_for_test`].
    pub async fn insert_rebalancer_client_for_test(
        &self,
        endpoint: &str,
        client: RebalancerClientHandle,
    ) {
        self.rebalancer_clients
            .lock()
            .await
            .insert(endpoint.to_string(), client);
    }

    pub async fn gres_control_for(
        &self,
        cluster: &str,
        bootstrap: &str,
    ) -> Result<GresControlHandle, GresControlWriteError> {
        let mut map = self.gres_controls.lock().await;
        if let Some(control) = map.get(cluster) {
            return Ok(control.clone());
        }
        let mut registry = crabka_gres_control::Registry::connect(bootstrap).await?;
        registry.ensure_topic(1).await?;
        let control: GresControlHandle = Arc::new(KafkaGresControl {
            registry: Mutex::new(registry),
            checkpoint_manifest_verifier: Arc::clone(&self.checkpoint_manifest_verifier),
        });
        map.insert(cluster.to_string(), control.clone());
        Ok(control)
    }

    pub async fn insert_gres_control_for_test(&self, cluster: &str, control: GresControlHandle) {
        self.gres_controls
            .lock()
            .await
            .insert(cluster.to_string(), control);
    }
}

fn checkpoint_manifest_verifier(config: &OperatorConfig) -> CheckpointManifestVerifierHandle {
    match ObjectStoreCheckpointManifestVerifier::from_config(config) {
        Ok(verifier) => Arc::new(verifier),
        Err(error) => {
            tracing::error!(error = %error, "Gres tenant WAL parking is unavailable until durable checkpoint verification is configured");
            Arc::new(UnavailableCheckpointManifestVerifier {
                reason: error.to_string(),
                unconfigured: matches!(error, CheckpointManifestError::Unconfigured),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use clap::Parser;

    use super::*;

    #[derive(Parser)]
    struct ConfigArgs {
        #[command(flatten)]
        config: OperatorConfig,
    }

    fn checkpoint_config(kind: GresCheckpointStoreKind) -> OperatorConfig {
        let mut config = ConfigArgs::parse_from(["operator"]).config;
        config.gres_checkpoint_store = Some(kind);
        config.gres_checkpoint_bucket = Some("checkpoints".into());
        config.gres_checkpoint_region = Some("us-east-1".into());
        config
    }

    #[tokio::test]
    async fn checkpoint_verifier_preserves_unconfigured_error_category() {
        let config = ConfigArgs::parse_from(["operator"]).config;
        let verifier = checkpoint_manifest_verifier(&config);
        let record = crabka_gres_control::TenantRecord::new(
            1,
            crabka_gres_control::TenantId::try_from("tenant-a").unwrap(),
            crabka_gres_control::TenantName::try_from("tenant-a").unwrap(),
            crabka_gres_control::TenantState::Suspended,
            crabka_gres_control::SqlUser::try_from("alice").unwrap(),
            "SCRAM-SHA-256$4096:salt$stored:server".into(),
            1,
        )
        .unwrap();

        let result = verifier.validate(&record).await;
        assert!(matches!(result, Err(CheckpointManifestError::Unconfigured)));
    }

    #[test]
    fn checkpoint_provider_credentials_reject_ambiguous_or_partial_configuration() {
        let mut s3 = checkpoint_config(GresCheckpointStoreKind::S3);
        s3.gres_checkpoint_access_key_id = Some("access".into());
        assert!(matches!(
            ObjectStoreCheckpointManifestVerifier::from_config(&s3),
            Err(CheckpointManifestError::InvalidConfiguration(_))
        ));

        let mut gcs = checkpoint_config(GresCheckpointStoreKind::Gcs);
        gcs.gres_checkpoint_gcs_service_account_path = Some("service.json".into());
        gcs.gres_checkpoint_gcs_application_credentials_path = Some("adc.json".into());
        assert!(matches!(
            ObjectStoreCheckpointManifestVerifier::from_config(&gcs),
            Err(CheckpointManifestError::InvalidConfiguration(_))
        ));
    }
}
