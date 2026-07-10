use std::time::{Duration, Instant};

use crabka_gres_control::TenantName;
use tokio::time::sleep;

use crate::{ActivatorError, WakeRegistry};

/// Backend endpoint for an active tenant compute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendEndpoint(pub String);

impl BackendEndpoint {
    /// Borrow the endpoint as `host:port` or another `TcpStream::connect` target.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Current readiness state for a tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Readiness {
    /// Tenant does not exist.
    Missing,
    /// Tenant exists but is not yet accepting backend connections.
    NotReady,
    /// Tenant is accepting backend connections at this endpoint.
    Ready(BackendEndpoint),
}

/// Bounds for cold-start readiness waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WaitForReadyConfig {
    /// Maximum time to wait.
    pub timeout: Duration,
    /// Poll interval while the registry watch surface is still pending.
    pub poll_interval: Duration,
}

/// Wait until a tenant is active, or fail when the configured timeout elapses.
pub async fn wait_for_ready<R>(
    registry: &R,
    tenant: &TenantName,
    cfg: WaitForReadyConfig,
) -> Result<BackendEndpoint, ActivatorError>
where
    R: WakeRegistry,
{
    let deadline = Instant::now() + cfg.timeout;
    loop {
        if let Readiness::Ready(endpoint) = registry.readiness(tenant).await? {
            return Ok(endpoint);
        }
        let now = Instant::now();
        if now >= deadline {
            return Err(ActivatorError::ReadyTimeout {
                tenant: tenant.clone(),
                timeout: cfg.timeout,
            });
        }
        sleep(cfg.poll_interval.min(deadline - now)).await;
    }
}
