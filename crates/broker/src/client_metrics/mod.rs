//! KIP-714 client metrics receiver: subscription config, client-instance
//! registry, OTLP decode, and the Prometheus + OTLP sinks.

pub(crate) mod config;
pub(crate) mod manager;
pub(crate) use manager::ClientMetricsManager;
