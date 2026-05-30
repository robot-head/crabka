//! Unified group-coordinator subsystem (KIP-848 64d-B). Shared infra and
//! persistence for both the classic and next-gen group protocols.
pub mod assignor;
pub mod config;
pub mod offsets_log;
pub(crate) mod persistence;
pub mod persistence_next_gen;
pub mod reconciler;
