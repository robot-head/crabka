use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
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
        let expected_tenant = checkpoint_manifest_tenant(record);
        if manifest.tenant != expected_tenant
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
                tenant: &expected_tenant,
                wal_generation: checkpoint.wal_generation,
                log_start: None,
                parts_by_name: &parts,
            })
            .map_err(|error| CheckpointManifestError::Verification(error.to_string()))?;
        Ok(())
    }
}

fn checkpoint_manifest_tenant(record: &crabka_gres_control::TenantRecord) -> String {
    match record.ranges.as_slice() {
        [range] => format!("{}/r{}", record.name, range.range_id),
        _ => record.name.to_string(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PgdogExpectedRoute {
    pub database: String,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgdogReloadRequest {
    /// DNS name used for `PostgreSQL` host identity and TLS verification.
    pub host: String,
    /// Optional per-replica TCP destination, while retaining `host` for SNI.
    pub connect_addr: Option<std::net::IpAddr>,
    pub port: u16,
    pub password: String,
    pub expected_routes: Vec<PgdogExpectedRoute>,
    pub maintenance_mode: bool,
    pub tls_ca_pem: Option<Vec<u8>>,
    pub tls_client_identity_pem: Option<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, thiserror::Error)]
pub enum PgdogAdminError {
    #[error("pgdog admin connection: {0}")]
    Connect(#[from] tokio_postgres::Error),
    #[error("pgdog admin TLS: {0}")]
    Tls(#[from] native_tls::Error),
    #[error("pgdog fleet admin: {0}")]
    Fleet(String),
}

#[async_trait::async_trait]
pub trait PgdogAdminLike: Send + Sync {
    async fn reload_and_database_views_match(
        &self,
        requests: &[PgdogReloadRequest],
    ) -> Result<bool, PgdogAdminError>;
}

#[derive(Debug, Default)]
struct TokioPostgresPgdogAdmin;

#[async_trait::async_trait]
impl PgdogAdminLike for TokioPostgresPgdogAdmin {
    async fn reload_and_database_views_match(
        &self,
        requests: &[PgdogReloadRequest],
    ) -> Result<bool, PgdogAdminError> {
        if requests.is_empty() {
            return Err(PgdogAdminError::Fleet(
                "reload request must address at least one PgDog replica".into(),
            ));
        }
        let mut clients = Vec::with_capacity(requests.len());
        for request in requests {
            let mut config = tokio_postgres::Config::new();
            config
                .host(&request.host)
                .port(request.port)
                .user("admin")
                .password(&request.password)
                .dbname("admin");
            if let Some(connect_addr) = request.connect_addr {
                config.hostaddr(connect_addr);
            }
            clients.push(
                connect_pgdog_admin(
                    config,
                    request.tls_ca_pem.as_deref(),
                    request
                        .tls_client_identity_pem
                        .as_ref()
                        .map(|(cert, key)| (cert.as_slice(), key.as_slice())),
                )
                .await?,
            );
        }
        let connections = clients
            .iter()
            .map(|client| client as &dyn PgdogAdminConnectionLike)
            .collect::<Vec<_>>();
        reload_and_match_connections(&connections, requests).await
    }
}

#[async_trait::async_trait]
trait PgdogAdminConnectionLike: Send + Sync {
    async fn execute(&self, command: &str) -> Result<(), PgdogAdminError>;
    async fn routes(
        &self,
    ) -> Result<std::collections::BTreeSet<PgdogExpectedRoute>, PgdogAdminError>;
}

#[async_trait::async_trait]
impl PgdogAdminConnectionLike for tokio_postgres::Client {
    async fn execute(&self, command: &str) -> Result<(), PgdogAdminError> {
        self.simple_query(command).await?;
        Ok(())
    }

    async fn routes(
        &self,
    ) -> Result<std::collections::BTreeSet<PgdogExpectedRoute>, PgdogAdminError> {
        // PgDog 0.1.47 exposes effective routes through SHOW POOLS. Columns
        // 1, 3, and 4 are database, configured address, and port. This view
        // contains configured database pools, not the admin pseudo-database.
        let messages = self.simple_query("SHOW POOLS").await?;
        let mut routes = std::collections::BTreeSet::new();
        for (index, row) in messages
            .iter()
            .filter_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .enumerate()
        {
            let field = |column: usize, name: &str| {
                row.get(column).ok_or_else(|| {
                    PgdogAdminError::Fleet(format!(
                        "SHOW POOLS row {index} is missing {name} column {column}"
                    ))
                })
            };
            let database = field(1, "database")?.to_string();
            let host = field(3, "address")?.to_string();
            let port_text = field(4, "port")?;
            let port = port_text.parse::<i32>().map_err(|error| {
                PgdogAdminError::Fleet(format!(
                    "SHOW POOLS row {index} has invalid port {port_text}: {error}"
                ))
            })?;
            routes.insert(pgdog_route_from_fields(index, database, host, port)?);
        }
        Ok(routes)
    }
}

fn pgdog_route_from_fields(
    row_index: usize,
    database: String,
    host: String,
    port: i32,
) -> Result<PgdogExpectedRoute, PgdogAdminError> {
    let port = u16::try_from(port).map_err(|error| {
        PgdogAdminError::Fleet(format!(
            "SHOW POOLS row {row_index} has invalid port {port}: {error}"
        ))
    })?;
    if database.is_empty() || host.is_empty() {
        return Err(PgdogAdminError::Fleet(format!(
            "SHOW POOLS row {row_index} has an empty database or address"
        )));
    }
    Ok(PgdogExpectedRoute {
        database,
        host,
        port,
    })
}

async fn reload_and_match_connections(
    clients: &[&dyn PgdogAdminConnectionLike],
    requests: &[PgdogReloadRequest],
) -> Result<bool, PgdogAdminError> {
    let maintenance = requests.iter().any(|request| request.maintenance_mode);
    let mut maintenance_clients = 0;
    if maintenance {
        for client in clients {
            if let Err(error) = client.execute("MAINTENANCE ON").await {
                let mut cleanup_errors = Vec::new();
                // The failing command may have reached PgDog before its
                // response failed. OFF is idempotent, so clean every
                // connected replica, including this and later clients.
                for entered in clients {
                    if let Err(cleanup_error) = entered.execute("MAINTENANCE OFF").await {
                        cleanup_errors.push(cleanup_error.to_string());
                    }
                }
                if !cleanup_errors.is_empty() {
                    return Err(PgdogAdminError::Fleet(format!(
                        "{error}; maintenance rollback failed: {}",
                        cleanup_errors.join("; ")
                    )));
                }
                return Err(error);
            }
            maintenance_clients += 1;
        }
    }
    let operation = reload_and_match_all(clients, requests).await;
    let mut cleanup_errors = Vec::new();
    if maintenance {
        for client in &clients[..maintenance_clients] {
            if let Err(error) = client.execute("MAINTENANCE OFF").await {
                cleanup_errors.push(error.to_string());
            }
        }
    }
    match (operation, cleanup_errors.is_empty()) {
        (Ok(matches), true) => Ok(matches),
        (Ok(_), false) => Err(PgdogAdminError::Fleet(format!(
            "maintenance cleanup failed: {}",
            cleanup_errors.join("; ")
        ))),
        (Err(operation), true) => Err(operation),
        (Err(operation), false) => Err(PgdogAdminError::Fleet(format!(
            "{operation}; maintenance cleanup failed: {}",
            cleanup_errors.join("; ")
        ))),
    }
}

async fn reload_and_match_all(
    clients: &[&dyn PgdogAdminConnectionLike],
    requests: &[PgdogReloadRequest],
) -> Result<bool, PgdogAdminError> {
    for (client, request) in clients.iter().zip(requests) {
        client.execute("RELOAD").await?;
        let observed = client.routes().await?;
        if !route_view_matches(&request.expected_routes, &observed) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn route_view_matches(
    expected: &[PgdogExpectedRoute],
    observed: &std::collections::BTreeSet<PgdogExpectedRoute>,
) -> bool {
    expected
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        == *observed
}

#[cfg(test)]
mod pgdog_reload_tests {
    use std::{collections::BTreeSet, sync::Mutex};

    use super::{
        PgdogAdminConnectionLike, PgdogAdminError, PgdogExpectedRoute, PgdogReloadRequest,
        pgdog_route_from_fields, reload_and_match_connections, route_view_matches,
    };

    struct FakeConnection {
        fail_execute: Option<&'static str>,
        fail_routes: bool,
        calls: Mutex<Vec<String>>,
        routes: BTreeSet<PgdogExpectedRoute>,
    }

    impl FakeConnection {
        fn new(fail_execute: Option<&'static str>, fail_routes: bool) -> Self {
            Self {
                fail_execute,
                fail_routes,
                calls: Mutex::new(Vec::new()),
                routes: BTreeSet::from([expected_route()]),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl PgdogAdminConnectionLike for FakeConnection {
        async fn execute(&self, command: &str) -> Result<(), PgdogAdminError> {
            self.calls.lock().unwrap().push(command.into());
            if self.fail_execute == Some(command) {
                return Err(PgdogAdminError::Fleet(format!("{command} failed")));
            }
            Ok(())
        }

        async fn routes(&self) -> Result<BTreeSet<PgdogExpectedRoute>, PgdogAdminError> {
            self.calls.lock().unwrap().push("SHOW POOLS".into());
            if self.fail_routes {
                return Err(PgdogAdminError::Fleet("SHOW POOLS failed".into()));
            }
            Ok(self.routes.clone())
        }
    }

    fn expected_route() -> PgdogExpectedRoute {
        PgdogExpectedRoute {
            database: "tenant-a".into(),
            host: "tenant-a-gres.ns.svc.cluster.local".into(),
            port: 5_432,
        }
    }

    #[test]
    fn malformed_show_pools_route_is_rejected_instead_of_discarded() {
        assert!(pgdog_route_from_fields(7, "tenant-a".into(), "host".into(), -1).is_err());
        assert!(pgdog_route_from_fields(8, String::new(), "host".into(), 5_432).is_err());
    }

    fn requests() -> Vec<PgdogReloadRequest> {
        ["10.0.0.10", "10.0.0.11"]
            .into_iter()
            .map(|ip| PgdogReloadRequest {
                host: "fleet-pgdog.ns.svc.cluster.local".into(),
                connect_addr: Some(ip.parse().unwrap()),
                port: 6_432,
                password: "pw".into(),
                expected_routes: vec![expected_route()],
                maintenance_mode: true,
                tls_ca_pem: Some(b"ca".to_vec()),
                tls_client_identity_pem: Some((b"cert".to_vec(), b"key".to_vec())),
            })
            .collect()
    }

    #[tokio::test]
    async fn maintenance_on_failure_rolls_back_every_connected_replica() {
        let first = FakeConnection::new(None, false);
        let second = FakeConnection::new(Some("MAINTENANCE ON"), false);

        let error = reload_and_match_connections(&[&first, &second], &requests())
            .await
            .unwrap_err();

        assert!(error.to_string().contains("MAINTENANCE ON failed"));
        assert!(first.calls().contains(&"MAINTENANCE OFF".into()));
        assert!(second.calls().contains(&"MAINTENANCE OFF".into()));
    }

    #[tokio::test]
    async fn reload_and_show_failures_still_exit_maintenance_on_every_replica() {
        for (fail_execute, fail_routes) in [(Some("RELOAD"), false), (None, true)] {
            let first = FakeConnection::new(fail_execute, fail_routes);
            let second = FakeConnection::new(None, false);

            assert!(
                reload_and_match_connections(&[&first, &second], &requests())
                    .await
                    .is_err()
            );
            assert!(first.calls().contains(&"MAINTENANCE OFF".into()));
            assert!(second.calls().contains(&"MAINTENANCE OFF".into()));
        }
    }

    #[tokio::test]
    async fn reload_never_reconnects_other_tenant_pools() {
        let first = FakeConnection::new(None, false);
        let second = FakeConnection::new(None, false);

        assert!(
            reload_and_match_connections(&[&first, &second], &requests())
                .await
                .unwrap()
        );
        for calls in [first.calls(), second.calls()] {
            assert_eq!(
                calls,
                ["MAINTENANCE ON", "RELOAD", "SHOW POOLS", "MAINTENANCE OFF"]
            );
            assert!(!calls.iter().any(|command| command == "RECONNECT"));
        }
    }

    #[tokio::test]
    async fn operation_and_maintenance_off_errors_are_both_preserved() {
        let first = FakeConnection::new(Some("RELOAD"), false);
        let second = FakeConnection::new(Some("MAINTENANCE OFF"), false);

        let error = reload_and_match_connections(&[&first, &second], &requests())
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("RELOAD failed"));
        assert!(error.contains("MAINTENANCE OFF failed"));
    }

    #[test]
    fn route_confirmation_rejects_same_database_on_wrong_endpoint() {
        let expected = vec![PgdogExpectedRoute {
            database: "tenant-a".into(),
            host: "tenant-a-gres.ns.svc.cluster.local".into(),
            port: 5_432,
        }];
        let observed = BTreeSet::from([PgdogExpectedRoute {
            database: "tenant-a".into(),
            host: "stale-tenant-a-gres.ns.svc.cluster.local".into(),
            port: 5_432,
        }]);

        assert!(!route_view_matches(&expected, &observed));
    }

    #[test]
    fn route_confirmation_rejects_stale_extra_database() {
        let expected_route = PgdogExpectedRoute {
            database: "tenant-a".into(),
            host: "tenant-a-gres.ns.svc.cluster.local".into(),
            port: 5_432,
        };
        let stale_route = PgdogExpectedRoute {
            database: "deleted-tenant".into(),
            host: "deleted-tenant-gres.ns.svc.cluster.local".into(),
            port: 5_432,
        };
        let observed = BTreeSet::from([expected_route.clone(), stale_route]);

        assert!(!route_view_matches(&[expected_route], &observed));
    }
}

async fn connect_pgdog_admin(
    config: tokio_postgres::Config,
    tls_ca_pem: Option<&[u8]>,
    tls_client_identity_pem: Option<(&[u8], &[u8])>,
) -> Result<tokio_postgres::Client, PgdogAdminError> {
    if let Some(tls_ca_pem) = tls_ca_pem {
        let certificate = native_tls::Certificate::from_pem(tls_ca_pem)?;
        let mut builder = native_tls::TlsConnector::builder();
        builder.add_root_certificate(certificate);
        if let Some((certificate, private_key)) = tls_client_identity_pem {
            builder.identity(native_tls::Identity::from_pkcs8(certificate, private_key)?);
        }
        let connector = postgres_native_tls::MakeTlsConnector::new(builder.build()?);
        let (client, connection) = config.connect(connector).await?;
        drop(tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%error, "pgdog TLS admin connection task ended");
            }
        }));
        return Ok(client);
    }
    let (client, connection) = config.connect(tokio_postgres::NoTls).await?;
    drop(tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::debug!(%error, "pgdog plaintext admin connection task ended");
        }
    }));
    Ok(client)
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
    async fn list_split_operations(
        &self,
        _tenant: &crabka_gres_control::TenantName,
    ) -> Result<Vec<crabka_gres_control::SplitOperationRecord>, GresControlWriteError> {
        Ok(Vec::new())
    }
    async fn compare_and_swap_split_operation(
        &self,
        _expected_revision: u64,
        _operation: &crabka_gres_control::SplitOperationRecord,
    ) -> Result<crabka_gres_control::SplitOperationRecord, GresControlWriteError> {
        Err(
            crabka_gres_control::ControlError::UnsupportedRegistryMutation {
                mutation: "compare_and_swap_split_operation",
                reason: "control backend does not expose the split journal",
            }
            .into(),
        )
    }
}

struct KafkaGresControl {
    registry: Mutex<crabka_gres_control::Registry>,
    checkpoint_manifest_verifier: CheckpointManifestVerifierHandle,
}

#[derive(Clone)]
struct CachedGresControl {
    bootstrap: String,
    policy: crabka_gres_control::RegistryPolicy,
    control: GresControlHandle,
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
        registry.ensure_topic().await?;
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

    async fn list_split_operations(
        &self,
        tenant: &crabka_gres_control::TenantName,
    ) -> Result<Vec<crabka_gres_control::SplitOperationRecord>, GresControlWriteError> {
        Ok(self
            .registry
            .lock()
            .await
            .list_split_operations(tenant.as_str())
            .await?)
    }

    async fn compare_and_swap_split_operation(
        &self,
        expected_revision: u64,
        operation: &crabka_gres_control::SplitOperationRecord,
    ) -> Result<crabka_gres_control::SplitOperationRecord, GresControlWriteError> {
        Ok(self
            .registry
            .lock()
            .await
            .compare_and_swap_split_operation(Some(expected_revision), operation)
            .await?)
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
    gres_controls: Arc<Mutex<HashMap<(String, String), CachedGresControl>>>,
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
        let admin = AdminClient::connect_with_options(
            &[bootstrap.to_string()],
            crabka_client_core::ConnectionOptions {
                dispatch_queue_capacity: crabka_client_core::ConnectionDispatchQueueCapacity::new(
                    self.config.client_dispatch_queue_capacity,
                )
                .map_err(crabka_client_admin::AdminError::Protocol)?,
                frame_max: crabka_client_core::ClientFrameMax::try_from(
                    self.config.client_frame_max,
                )
                .map_err(crabka_client_admin::AdminError::Protocol)?,
                ..crabka_client_core::ConnectionOptions::default()
            },
        )
        .await?;
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
        let client: RebalancerClientHandle = Arc::new(ConnectRebalancerClient::new(
            endpoint,
            self.config.rebalancer_request_timeout,
        ));
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

    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub async fn gres_control_for(
        &self,
        namespace: &str,
        kafka_name: &str,
        bootstrap: &str,
        policy: &crabka_gres_control::RegistryPolicy,
    ) -> Result<GresControlHandle, GresControlWriteError> {
        let bootstrap_owned = bootstrap.to_owned();
        let policy_owned = policy.clone();
        let checkpoint_manifest_verifier = Arc::clone(&self.checkpoint_manifest_verifier);
        self.gres_control_for_with(namespace, kafka_name, bootstrap, policy, async move {
            let mut registry =
                crabka_gres_control::Registry::connect_with_policy(&bootstrap_owned, policy_owned)
                    .await?;
            registry.ensure_topic().await?;
            Ok(Arc::new(KafkaGresControl {
                registry: Mutex::new(registry),
                checkpoint_manifest_verifier,
            }) as GresControlHandle)
        })
        .await
    }

    async fn gres_control_for_with<F>(
        &self,
        namespace: &str,
        kafka_name: &str,
        bootstrap: &str,
        policy: &crabka_gres_control::RegistryPolicy,
        build: F,
    ) -> Result<GresControlHandle, GresControlWriteError>
    where
        F: Future<Output = Result<GresControlHandle, GresControlWriteError>>,
    {
        let key = (namespace.to_owned(), kafka_name.to_owned());
        if let Some(entry) = self.gres_controls.lock().await.get(&key)
            && entry.bootstrap == bootstrap
            && entry.policy == *policy
        {
            return Ok(Arc::clone(&entry.control));
        }

        let control = build.await?;
        let mut map = self.gres_controls.lock().await;
        if let Some(entry) = map.get(&key)
            && entry.bootstrap == bootstrap
            && entry.policy == *policy
        {
            return Ok(Arc::clone(&entry.control));
        }
        map.insert(
            key,
            CachedGresControl {
                bootstrap: bootstrap.to_owned(),
                policy: policy.clone(),
                control: Arc::clone(&control),
            },
        );
        Ok(control)
    }

    pub async fn insert_gres_control_for_test(
        &self,
        namespace: &str,
        kafka_name: &str,
        control: GresControlHandle,
    ) {
        self.insert_gres_control_for_test_with_policy(
            namespace,
            kafka_name,
            &format!("{kafka_name}-broker-headless.{namespace}.svc.cluster.local:9092"),
            crabka_gres_control::RegistryPolicy::default(),
            control,
        )
        .await;
    }

    pub async fn insert_gres_control_for_test_with_policy(
        &self,
        namespace: &str,
        kafka_name: &str,
        bootstrap: &str,
        policy: crabka_gres_control::RegistryPolicy,
        control: GresControlHandle,
    ) {
        self.gres_controls.lock().await.insert(
            (namespace.to_owned(), kafka_name.to_owned()),
            CachedGresControl {
                bootstrap: bootstrap.to_owned(),
                policy,
                control,
            },
        );
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
    use tower::service_fn;

    use super::*;

    fn fixture_password() -> String {
        std::process::id().to_string()
    }

    #[derive(Parser)]
    struct ConfigArgs {
        #[command(flatten)]
        config: OperatorConfig,
    }

    struct TestGresControl;

    #[async_trait::async_trait]
    impl GresControlLike for TestGresControl {
        async fn get_tenant(
            &self,
            _tenant: &crabka_gres_control::TenantName,
        ) -> Result<Option<crabka_gres_control::TenantRecord>, GresControlWriteError> {
            unreachable!("cache test does not read tenants")
        }

        async fn replace_tenant_if_version(
            &self,
            _record: &crabka_gres_control::TenantRecord,
            _expected_record_version: Option<u64>,
        ) -> Result<crabka_gres_control::TenantRecord, GresControlWriteError> {
            unreachable!("cache test does not write tenants")
        }

        async fn delete_tenant(
            &self,
            _tenant: &crabka_gres_control::TenantName,
        ) -> Result<(), GresControlWriteError> {
            unreachable!("cache test does not delete tenants")
        }

        async fn validate_final_checkpoint_manifest(
            &self,
            _record: &crabka_gres_control::TenantRecord,
        ) -> Result<(), GresControlWriteError> {
            unreachable!("cache test does not verify checkpoints")
        }
    }

    fn test_context() -> Context {
        let client = Client::new(
            service_fn(|_| async {
                Ok::<_, std::convert::Infallible>(
                    http::Response::builder()
                        .status(500)
                        .body(kube::client::Body::from(Vec::new()))
                        .expect("response"),
                )
            }),
            "default",
        );
        let (registry, metrics) = crate::telemetry::new_registry_with_metrics();
        Context::new(
            client,
            ConfigArgs::parse_from(["operator"]).config,
            Arc::new(Mutex::new(registry)),
            metrics,
        )
    }

    #[tokio::test]
    async fn gres_control_cache_tracks_inputs_without_locking_during_build() {
        let ctx = test_context();
        let defaults = crabka_gres_control::RegistryPolicy::default();
        let first: GresControlHandle = Arc::new(TestGresControl);
        let observed = ctx
            .gres_control_for_with("ns-a", "demo", "a:9092", &defaults, async {
                assert!(ctx.gres_controls.try_lock().is_ok());
                Ok(Arc::clone(&first))
            })
            .await
            .expect("first control");
        assert!(Arc::ptr_eq(&observed, &first));

        let reused = ctx
            .gres_control_for_with("ns-a", "demo", "a:9092", &defaults, async {
                unreachable!("equal cache inputs must not rebuild")
            })
            .await
            .expect("reused control");
        assert!(Arc::ptr_eq(&reused, &first));

        let changed_reader_admin_dns = defaults
            .clone()
            .with_reader_admin_dns_timeout(crabka_units::millis(37))
            .expect("reader/admin DNS timeout");
        let changed_reader_admin_dns_control: GresControlHandle = Arc::new(TestGresControl);
        let replaced = ctx
            .gres_control_for_with("ns-a", "demo", "a:9092", &changed_reader_admin_dns, async {
                Ok(Arc::clone(&changed_reader_admin_dns_control))
            })
            .await
            .expect("reader/admin DNS policy replacement");
        assert!(Arc::ptr_eq(&replaced, &changed_reader_admin_dns_control));

        let changed_dns = changed_reader_admin_dns
            .clone()
            .with_producer_dns_timeout(crabka_units::millis(37))
            .expect("DNS timeout");
        let changed_dns_control: GresControlHandle = Arc::new(TestGresControl);
        let replaced = ctx
            .gres_control_for_with("ns-a", "demo", "a:9092", &changed_dns, async {
                Ok(Arc::clone(&changed_dns_control))
            })
            .await
            .expect("DNS policy replacement");
        assert!(Arc::ptr_eq(&replaced, &changed_dns_control));

        let custom = crabka_gres_control::RegistryPolicy::new(
            2,
            crabka_units::millis(15_001),
            crabka_units::millis(251),
            crabka_units::millis(501),
            crabka_units::bytes(1_048_577),
        )
        .expect("policy");
        let changed_policy: GresControlHandle = Arc::new(TestGresControl);
        let replaced = ctx
            .gres_control_for_with("ns-a", "demo", "a:9092", &custom, async {
                Ok(Arc::clone(&changed_policy))
            })
            .await
            .expect("policy replacement");
        assert!(Arc::ptr_eq(&replaced, &changed_policy));

        let changed_bootstrap: GresControlHandle = Arc::new(TestGresControl);
        let replaced = ctx
            .gres_control_for_with("ns-a", "demo", "b:9092", &custom, async {
                Ok(Arc::clone(&changed_bootstrap))
            })
            .await
            .expect("bootstrap replacement");
        assert!(Arc::ptr_eq(&replaced, &changed_bootstrap));

        let other_namespace: GresControlHandle = Arc::new(TestGresControl);
        let isolated = ctx
            .gres_control_for_with("ns-b", "demo", "b:9092", &custom, async {
                Ok(Arc::clone(&other_namespace))
            })
            .await
            .expect("namespace-isolated control");
        assert!(Arc::ptr_eq(&isolated, &other_namespace));
        assert!(ctx.gres_controls.lock().await.len() == 2);
    }

    fn checkpoint_config(kind: GresCheckpointStoreKind) -> OperatorConfig {
        let mut config = ConfigArgs::parse_from(["operator"]).config;
        config.gres_checkpoint_store = Some(kind);
        config.gres_checkpoint_bucket = Some("checkpoints".into());
        config.gres_checkpoint_region = Some("us-east-1".into());
        config
    }

    #[test]
    fn single_range_checkpoint_manifest_identity_is_generation_namespace() {
        let record = crabka_gres_control::TenantRecord::new(
            1,
            crabka_gres_control::TenantId::try_from("tenant-a").unwrap(),
            crabka_gres_control::TenantName::try_from("tenant-a").unwrap(),
            crabka_gres_control::TenantState::Active,
            crabka_gres_control::SqlUser::try_from("alice").unwrap(),
            crabka_security::scram::PgScramVerifier::generate_with_salt(
                &fixture_password(),
                4096,
                vec![1; 16],
            )
            .unwrap()
            .to_string(),
            1,
        )
        .unwrap()
        .with_range_layout(vec![crabka_gres_control::RangeLayoutEntry {
            range_id: 0,
            end_key: None,
            endpoint: "tenant-a-gres.default.svc:5432".into(),
            wal_generation: 0,
            lifecycle: crabka_gres_control::RangeLifecycle::default(),
            retirement: None,
        }])
        .unwrap();

        assert!(checkpoint_manifest_tenant(&record) == "tenant-a/r0");
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
