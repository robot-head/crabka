use std::time::Instant;

use crabka_gres_control::TenantName;
use crabka_units::{
    Time,
    convert::{StdDurationExt as _, TimeExt as _},
};
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
///
/// Not `Eq`: both bounds are `f64`-backed quantities.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WaitForReadyConfig {
    /// Maximum time to wait.
    pub timeout: Time,
    /// Poll interval while the registry watch surface is still pending.
    pub poll_interval: Time,
}

/// Wait until a tenant is active, or fail when the configured timeout elapses.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn wait_for_ready<R>(
    registry: &R,
    tenant: &TenantName,
    cfg: WaitForReadyConfig,
) -> Result<BackendEndpoint, ActivatorError>
where
    R: WakeRegistry,
{
    // The deadline is an instant, so it stays raw; only the extent left to wait
    // is a quantity.
    let deadline = Instant::now() + cfg.timeout.to_std();
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
        let remaining = deadline.duration_since(now).as_time();
        sleep(cfg.poll_interval.min(remaining).to_std()).await;
    }
}
