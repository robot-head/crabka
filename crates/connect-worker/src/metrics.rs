//! Prometheus metrics shared by the worker and health server.

use std::sync::Arc;

use prometheus_client::{
    metrics::{counter::Counter, gauge::Gauge},
    registry::Registry,
};
use tokio::sync::Mutex;

/// Shared worker metrics and their owning registry.
#[derive(Clone)]
pub(crate) struct WorkerMetrics {
    pub(crate) registry: Arc<Mutex<Registry>>,
    live: Gauge,
    ready: Gauge,
    records_produced: Counter,
    checkpoints_saved: Counter,
    errors: Counter,
}

impl WorkerMetrics {
    pub(crate) fn new() -> Self {
        let live = Gauge::default();
        let ready = Gauge::default();
        let records_produced = Counter::default();
        let checkpoints_saved = Counter::default();
        let errors = Counter::default();
        let mut registry = Registry::with_prefix("crabka_connect_worker");
        registry.register("live", "1 while the worker process is live.", live.clone());
        registry.register(
            "ready",
            "1 while the connector runtime can process records.",
            ready.clone(),
        );
        registry.register(
            "records_produced_total",
            "Records durably acknowledged by Kafka.",
            records_produced.clone(),
        );
        registry.register(
            "checkpoints_saved_total",
            "Source checkpoints durably acknowledged by Kafka.",
            checkpoints_saved.clone(),
        );
        registry.register(
            "errors_total",
            "Connector, Kafka, and health-server errors.",
            errors.clone(),
        );
        Self {
            registry: Arc::new(Mutex::new(registry)),
            live,
            ready,
            records_produced,
            checkpoints_saved,
            errors,
        }
    }

    pub(crate) fn set_live(&self, live: bool) {
        self.live.set(i64::from(live));
    }

    pub(crate) fn set_ready(&self, ready: bool) {
        self.ready.set(i64::from(ready));
    }

    pub(crate) fn is_live(&self) -> bool {
        self.live.get() == 1
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.ready.get() == 1
    }

    pub(crate) fn record_produced(&self) {
        self.records_produced.inc();
    }

    pub(crate) fn record_checkpoint(&self) {
        self.checkpoints_saved.inc();
    }

    pub(crate) fn record_error(&self) {
        self.errors.inc();
    }
}

impl Default for WorkerMetrics {
    fn default() -> Self {
        Self::new()
    }
}
