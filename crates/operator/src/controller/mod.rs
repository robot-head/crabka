//! Controllers (reconcilers) for Crabka CRDs. Each kind lives in its own
//! submodule and shares helpers via `common` (cluster-level rendering,
//! SSA helpers, label / owner-ref builders, status derivation).

pub mod common;
pub mod kafka;
pub mod kafka_node_pool;
pub(crate) mod listeners;
