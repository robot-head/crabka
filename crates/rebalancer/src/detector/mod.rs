//! Anomaly detector — slice 43g.
//!
//! The detector watches `SharedSnapshot` + `UsageStore` for trouble
//! (broker death, sustained under-replicated partitions, disk pressure,
//! slow broker) and auto-triggers self-healing proposals via the
//! existing optimizer path. Anomaly history is persisted to
//! `{data_dir}/anomalies.json` and surfaced via `GetAnomalies`.

pub mod anomaly;
pub mod store;

pub use anomaly::{Anomaly, AnomalyKey, AnomalyKind, AnomalySeverity};
pub use store::{AnomalyStore, StoreError};
