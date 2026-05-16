//! Controllers (reconcilers) for Crabka CRDs. Each kind lives in its own
//! submodule. Slice 17 only ships a placeholder Kafka controller that
//! flips `Ready=True`; real workload reconciliation lands in slice 18.

pub mod kafka;
