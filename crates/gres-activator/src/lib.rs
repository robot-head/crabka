//! Wake activator foundation for suspended Gres tenants.

pub mod hold;
pub mod peek;
pub mod pipe;

use std::{collections::BTreeSet, net::SocketAddr, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::BytesMut;
use crabka_gres_control::{Registry, TenantName, TenantState};
use crabka_pgwire::{error::PgError, messages::backend};
pub use hold::{BackendEndpoint, Readiness, WaitForReadyConfig, wait_for_ready};
pub use peek::{Prelude, peek_prelude, peek_prelude_from};
pub use pipe::pipe_startup_and_session;
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::Mutex};

/// Activator runtime configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivatorConfig {
    /// Address to accept frontend connections on.
    pub listen: SocketAddr,
    /// Kafka bootstrap address for the tenant registry.
    pub bootstrap: String,
    /// How often readiness waits poll the registry.
    pub registry_poll: Duration,
    /// Maximum time to hold a cold-starting connection.
    pub cold_start_timeout: Duration,
    /// Backend endpoint template used until registry records grow an endpoint field.
    pub backend_endpoint_template: String,
}

impl ActivatorConfig {
    /// Render the deterministic compute endpoint for a tenant.
    #[must_use]
    pub fn endpoint_for_tenant(&self, tenant: &TenantName) -> BackendEndpoint {
        BackendEndpoint(
            self.backend_endpoint_template
                .replace("{tenant}", tenant.as_str()),
        )
    }
}

/// A validated wake request derived from a frontend startup prelude or explicit event.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct WakeRequest {
    tenant: TenantName,
}

impl WakeRequest {
    /// Parse a wake request from a tenant database name.
    pub fn for_database(database: &str) -> Result<Self, ActivatorError> {
        let tenant = TenantName::try_from(database).map_err(ActivatorError::Control)?;
        Ok(Self { tenant })
    }

    /// Return the requested tenant.
    #[must_use]
    pub fn tenant(&self) -> &TenantName {
        &self.tenant
    }
}

/// Registry operations required by the wake path.
#[async_trait]
pub trait WakeRegistry: Send + Sync {
    /// Idempotently record that a suspended tenant should resume.
    async fn request_resume(&self, request: &WakeRequest) -> Result<(), ActivatorError>;

    /// Return current readiness for the requested tenant.
    async fn readiness(&self, tenant: &TenantName) -> Result<Readiness, ActivatorError>;
}

/// Coordinates duplicate wake requests within one activator process.
#[derive(Debug)]
pub struct WakeCoordinator<R> {
    registry: R,
    requested_tenants: Mutex<BTreeSet<TenantName>>,
}

impl<R> WakeCoordinator<R> {
    /// Build a wake coordinator over a registry implementation.
    #[must_use]
    pub fn new(registry: R) -> Self {
        Self {
            registry,
            requested_tenants: Mutex::new(BTreeSet::new()),
        }
    }
}

impl<R> WakeCoordinator<R>
where
    R: WakeRegistry,
{
    /// Request resume once per tenant and wait for an active backend endpoint.
    pub async fn wake_and_wait(
        &self,
        request: &WakeRequest,
        wait: WaitForReadyConfig,
    ) -> Result<BackendEndpoint, ActivatorError> {
        self.request_resume_once(request).await?;
        let result = wait_for_ready(&self.registry, request.tenant(), wait).await;
        if result.is_err() {
            self.forget_request(request.tenant()).await;
        }
        result
    }

    async fn request_resume_once(&self, request: &WakeRequest) -> Result<(), ActivatorError> {
        let mut requested_tenants = self.requested_tenants.lock().await;
        if !requested_tenants.insert(request.tenant().clone()) {
            return Ok(());
        }
        drop(requested_tenants);
        self.registry.request_resume(request).await
    }

    async fn forget_request(&self, tenant: &TenantName) {
        self.requested_tenants.lock().await.remove(tenant);
    }
}

/// Serve one frontend connection through the activator.
pub async fn serve_conn<R>(
    mut stream: TcpStream,
    coordinator: &WakeCoordinator<R>,
    cfg: &ActivatorConfig,
) -> Result<(), ActivatorError>
where
    R: WakeRegistry,
{
    let prelude = peek_prelude(&mut stream).await?;
    let request = WakeRequest::for_database(&prelude.database)?;
    let wait = WaitForReadyConfig {
        timeout: cfg.cold_start_timeout,
        poll_interval: cfg.registry_poll,
    };
    let endpoint = match coordinator.wake_and_wait(&request, wait).await {
        Ok(endpoint) => endpoint,
        Err(ActivatorError::ReadyTimeout { .. }) => {
            write_cannot_connect_now(&mut stream).await?;
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    pipe_startup_and_session(stream, endpoint.as_str(), &prelude.raw_startup).await
}

async fn write_cannot_connect_now(stream: &mut TcpStream) -> Result<(), ActivatorError> {
    let error = PgError::fatal("57P03", "tenant is still resuming");
    let mut out = BytesMut::new();
    backend::error_response(&mut out, &error);
    stream.write_all(&out).await?;
    Ok(())
}

/// Adapter for the current Kafka-backed Gres control registry.
#[derive(Clone)]
pub struct ControlRegistryWakeRegistry {
    registry: Arc<Mutex<Registry>>,
    cfg: ActivatorConfig,
}

impl ControlRegistryWakeRegistry {
    /// Build an adapter around the existing control registry.
    #[must_use]
    pub fn new(registry: Registry, cfg: ActivatorConfig) -> Self {
        Self {
            registry: Arc::new(Mutex::new(registry)),
            cfg,
        }
    }
}

#[async_trait]
impl WakeRegistry for ControlRegistryWakeRegistry {
    async fn request_resume(&self, request: &WakeRequest) -> Result<(), ActivatorError> {
        let mut registry = self.registry.lock().await;
        let Some(record) = registry.get(request.tenant().as_str()).await? else {
            return Err(ActivatorError::TenantMissing(request.tenant().clone()));
        };
        if record.state == TenantState::Active {
            return Ok(());
        }
        registry.request_resume(request.tenant().as_str()).await?;
        Ok(())
    }

    async fn readiness(&self, tenant: &TenantName) -> Result<Readiness, ActivatorError> {
        let mut registry = self.registry.lock().await;
        let Some(record) = registry.get(tenant.as_str()).await? else {
            return Ok(Readiness::Missing);
        };
        if record.state != TenantState::Active {
            return Ok(Readiness::NotReady);
        }
        Ok(Readiness::Ready(self.cfg.endpoint_for_tenant(tenant)))
    }
}

/// Activator errors.
#[derive(Debug, thiserror::Error)]
pub enum ActivatorError {
    /// Control-plane registry operation failed.
    #[error("gres control registry error: {0}")]
    Control(#[from] crabka_gres_control::ControlError),
    /// I/O operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// Frontend prelude was invalid.
    #[error("frontend prelude error: {0}")]
    Prelude(String),
    /// Startup message omitted the database/tenant name.
    #[error("startup message does not name a database")]
    MissingDatabase,
    /// Tenant was not found in the registry.
    #[error("tenant {0} is missing from the registry")]
    TenantMissing(TenantName),
    /// Readiness wait exceeded the configured timeout.
    #[error("tenant {tenant} did not become ready within {timeout:?}")]
    ReadyTimeout {
        /// Tenant being awaited.
        tenant: TenantName,
        /// Timeout that elapsed.
        timeout: Duration,
    },
    /// Current control record schema cannot represent `ResumeRequested` or endpoint.
    #[error("gres-control registry lacks ResumeRequested/endpoint lifecycle fields")]
    RegistryLifecycleMissing,
}

#[cfg(test)]
pub mod test_doubles {
    use std::{collections::BTreeMap, sync::Arc};

    use async_trait::async_trait;
    use tokio::sync::{Mutex, Notify};

    use super::*;

    /// In-memory wake registry for activator unit tests.
    #[derive(Debug, Default, Clone)]
    pub struct FakeWakeRegistry {
        state: Arc<Mutex<FakeState>>,
        changed: Arc<Notify>,
    }

    #[derive(Debug, Default)]
    struct FakeState {
        tenants: BTreeMap<TenantName, Readiness>,
        resume_requests: BTreeMap<TenantName, u64>,
    }

    impl FakeWakeRegistry {
        /// Set the fake readiness state for a tenant.
        pub async fn set_readiness(&self, tenant: TenantName, readiness: Readiness) {
            self.state.lock().await.tenants.insert(tenant, readiness);
            self.changed.notify_waiters();
        }

        /// Return how many resume requests were recorded for a tenant.
        pub async fn resume_request_count(&self, tenant: &TenantName) -> u64 {
            self.state
                .lock()
                .await
                .resume_requests
                .get(tenant)
                .copied()
                .unwrap_or_default()
        }
    }

    #[async_trait]
    impl WakeRegistry for FakeWakeRegistry {
        async fn request_resume(&self, request: &WakeRequest) -> Result<(), ActivatorError> {
            let mut state = self.state.lock().await;
            let count = state
                .resume_requests
                .entry(request.tenant().clone())
                .or_default();
            *count += 1;
            state
                .tenants
                .entry(request.tenant().clone())
                .or_insert(Readiness::NotReady);
            self.changed.notify_waiters();
            Ok(())
        }

        async fn readiness(&self, tenant: &TenantName) -> Result<Readiness, ActivatorError> {
            Ok(self
                .state
                .lock()
                .await
                .tenants
                .get(tenant)
                .cloned()
                .unwrap_or(Readiness::Missing))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use assert2::assert;
    use bytes::{BufMut, BytesMut};
    use crabka_gres_control::TenantName;
    use tokio::{io::AsyncWriteExt, join};

    use super::{test_doubles::FakeWakeRegistry, *};

    fn tenant_name() -> TenantName {
        TenantName::try_from("tenant-a").unwrap()
    }

    fn startup_bytes(params: &[(&str, &str)]) -> Vec<u8> {
        let mut body = BytesMut::new();
        body.put_i32(0x0003_0000);
        for (key, value) in params {
            body.put_slice(key.as_bytes());
            body.put_u8(0);
            body.put_slice(value.as_bytes());
            body.put_u8(0);
        }
        body.put_u8(0);
        let len = i32::try_from(body.len() + 4).unwrap();
        let mut out = BytesMut::new();
        out.put_i32(len);
        out.extend_from_slice(&body);
        out.to_vec()
    }

    fn ssl_request_bytes() -> Vec<u8> {
        let mut out = BytesMut::new();
        out.put_i32(8);
        out.put_i32(80_877_103);
        out.to_vec()
    }

    #[tokio::test]
    async fn peek_answers_ssl_and_keeps_only_database_plus_raw_startup() {
        let startup = startup_bytes(&[
            ("user", "alice"),
            ("database", "tenant-a"),
            ("password", "plain-secret"),
        ]);
        let (mut client, mut server) = tokio::io::duplex(1024);
        let writer = tokio::spawn(async move {
            client.write_all(&ssl_request_bytes()).await.unwrap();
            let mut ssl_answer = [0_u8; 1];
            tokio::io::AsyncReadExt::read_exact(&mut client, &mut ssl_answer)
                .await
                .unwrap();
            assert!(ssl_answer == *b"N");
            client.write_all(&startup).await.unwrap();
        });

        let prelude = peek_prelude_from(&mut server).await.unwrap();
        writer.await.unwrap();

        assert!(prelude.database == "tenant-a");
        assert!(!format!("{prelude:?}").contains("plain-secret"));
    }

    #[tokio::test]
    async fn bounded_wait_times_out_when_readiness_never_arrives() {
        let registry = FakeWakeRegistry::default();
        let tenant = tenant_name();
        let result = wait_for_ready(
            &registry,
            &tenant,
            WaitForReadyConfig {
                timeout: Duration::from_millis(15),
                poll_interval: Duration::from_millis(5),
            },
        )
        .await;

        assert!(matches!(result, Err(ActivatorError::ReadyTimeout { .. })));
    }

    #[tokio::test]
    async fn bounded_wait_returns_ready_endpoint() {
        let registry = FakeWakeRegistry::default();
        let tenant = tenant_name();
        let setter = registry.clone();
        let tenant_for_setter = tenant.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            setter
                .set_readiness(
                    tenant_for_setter,
                    Readiness::Ready(BackendEndpoint("127.0.0.1:5432".to_string())),
                )
                .await;
        });

        let endpoint = wait_for_ready(
            &registry,
            &tenant,
            WaitForReadyConfig {
                timeout: Duration::from_secs(1),
                poll_interval: Duration::from_millis(5),
            },
        )
        .await
        .unwrap();

        assert!(endpoint.as_str() == "127.0.0.1:5432");
    }

    #[tokio::test]
    async fn duplicate_wakes_are_coalesced_until_ready() {
        let registry = FakeWakeRegistry::default();
        let coordinator = Arc::new(WakeCoordinator::new(registry.clone()));
        let request = WakeRequest::for_database("tenant-a").unwrap();
        let wait = WaitForReadyConfig {
            timeout: Duration::from_secs(1),
            poll_interval: Duration::from_millis(5),
        };
        let first = Arc::clone(&coordinator);
        let first_request = request.clone();
        let second = Arc::clone(&coordinator);
        let second_request = request.clone();
        let registry_for_setter = registry.clone();
        let tenant = tenant_name();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            registry_for_setter
                .set_readiness(
                    tenant,
                    Readiness::Ready(BackendEndpoint("127.0.0.1:15432".to_string())),
                )
                .await;
        });

        let (left, right) = join!(
            first.wake_and_wait(&first_request, wait),
            second.wake_and_wait(&second_request, wait),
        );

        assert!(left.unwrap().as_str() == "127.0.0.1:15432");
        assert!(right.unwrap().as_str() == "127.0.0.1:15432");
        assert!(registry.resume_request_count(&tenant_name()).await == 1);
    }

    #[tokio::test]
    async fn wake_request_is_idempotent_after_tenant_is_ready() {
        let registry = FakeWakeRegistry::default();
        let coordinator = WakeCoordinator::new(registry.clone());
        let request = WakeRequest::for_database("tenant-a").unwrap();
        let tenant = tenant_name();
        registry
            .set_readiness(
                tenant.clone(),
                Readiness::Ready(BackendEndpoint("127.0.0.1:25432".to_string())),
            )
            .await;
        let wait = WaitForReadyConfig {
            timeout: Duration::from_millis(50),
            poll_interval: Duration::from_millis(5),
        };

        let first = coordinator.wake_and_wait(&request, wait).await.unwrap();
        let second = coordinator.wake_and_wait(&request, wait).await.unwrap();

        assert!(first == second);
        assert!(registry.resume_request_count(&tenant).await == 1);
    }
}
