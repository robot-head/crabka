//! `crabka-replicator` — cross-cluster geo-replication for Crabka.

pub mod admin_util;
pub mod checkpoint_store;
pub mod config;
pub mod error;
pub mod mm2;
pub mod naming;
pub mod offset_sync_store;
pub mod record;
pub mod residency;
pub mod selector;
pub mod sink;
pub mod source;
pub mod supervisor;
pub mod tasks;
pub mod worker;

pub use error::{ReplicatorError, Result};

#[cfg(test)]
mod test_util;
