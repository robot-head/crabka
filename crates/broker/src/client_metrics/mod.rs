//! KIP-714 client metrics receiver: subscription config, client-instance
//! registry, OTLP decode, and the Prometheus + OTLP sinks.

pub(crate) mod config;
pub(crate) mod manager;
pub(crate) mod otlp;
pub(crate) mod otlp_sink;
pub(crate) mod prometheus_sink;
use std::{sync::Arc, time::Duration};

pub(crate) use manager::ClientMetricsManager;

use self::{otlp_sink::OtlpForwarder, prometheus_sink::ClientMetricsCollector};

/// Broker-held bundle: the manager (instance state + matching) plus the two
/// sinks. The Prometheus collector is shared with the metrics registry.
pub(crate) struct ClientMetrics {
    pub manager: ClientMetricsManager,
    pub prometheus: Arc<ClientMetricsCollector>,
    pub otlp: OtlpForwarder,
}

impl ClientMetrics {
    /// `otlp_endpoint` is `None` when OTLP forwarding is disabled.
    pub(crate) fn new(
        telemetry_max_bytes: i32,
        default_interval_ms: i32,
        otlp_endpoint: Option<String>,
        otlp_queue_capacity: usize,
        prometheus_snapshot_ttl: Duration,
    ) -> Self {
        let otlp = match otlp_endpoint {
            Some(ep) => OtlpForwarder::spawn(ep, otlp_queue_capacity),
            None => OtlpForwarder::disabled(),
        };
        Self {
            manager: ClientMetricsManager::new(telemetry_max_bytes, default_interval_ms),
            prometheus: Arc::new(ClientMetricsCollector::new(prometheus_snapshot_ttl)),
            otlp,
        }
    }
}
