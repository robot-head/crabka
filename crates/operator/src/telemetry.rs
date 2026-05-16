use std::sync::Arc;

use prometheus_client::registry::Registry;
use tokio::sync::Mutex;

/// Shared Prometheus registry. Wrap in `Arc<Mutex<…>>` because
/// `prometheus-client` requires `&mut Registry` to register metrics
/// and we want controllers to register lazily on first reconcile.
pub type SharedRegistry = Arc<Mutex<Registry>>;

/// Initialise the global `tracing` subscriber. Idempotent in tests
/// (returns Ok if already set).
pub fn init_tracing(filter: &str) {
    let env = tracing_subscriber::EnvFilter::try_new(filter)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env)
        .with_target(true)
        .json()
        .try_init();
}

/// Build a fresh registry. Callers wrap in `Arc<Mutex<…>>`.
#[must_use]
pub fn new_registry() -> Registry {
    Registry::with_prefix("crabka_operator")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_has_prefix() {
        let mut r = new_registry();
        let g = prometheus_client::metrics::gauge::Gauge::<i64>::default();
        r.register("up", "operator liveness", g);
        let mut s = String::new();
        prometheus_client::encoding::text::encode(&mut s, &r).unwrap();
        assert!(s.contains("crabka_operator_up"));
    }

    #[test]
    fn init_tracing_idempotent() {
        init_tracing("info");
        init_tracing("debug");
    }
}
