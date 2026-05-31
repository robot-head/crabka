//! KIP-714 client metrics receiver: subscription config, client-instance
//! registry, OTLP decode, and the Prometheus + OTLP sinks.

pub(crate) mod config;
pub(crate) mod manager;
pub(crate) mod otlp;
pub(crate) mod otlp_sink;
pub(crate) mod prometheus_sink;
pub(crate) use manager::ClientMetricsManager;
