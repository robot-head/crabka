//! Deterministic, sans-IO `KRaft` consensus core (KIP-595 + KIP-996).
//!
//! This crate is Crabka's metadata-quorum engine: a synchronous
//! `on_event(event, log, now) -> Vec<Action>` state machine
//! ([`core::QuorumStateMachine`]) over the `QuorumState`/[`Role`] model. It
//! never reads the clock, touches a socket, or writes a byte to disk. The
//! caller injects the time as a [`SimInstant`], the state machine reads the log
//! through the [`LogView`] seam, and it returns every effect as an [`Action`]
//! for the caller to execute.
//!
//! `crabka-raft` wraps this core with the async engine, the real KIP-595 wire,
//! and the on-disk `crabka-log`. It re-exports these modules under
//! `crabka_raft::kraft`. The core is a leaf crate with no tokio, no filesystem,
//! and no crypto, so it compiles for `wasm32-unknown-unknown`. The [`sim`]
//! module, behind the `sim` feature, uses this to drive an interactive
//! consensus simulation in the browser playground.

#![doc(html_root_url = "https://docs.rs/crabka-kraft-core/0.4.0")]
#![allow(dead_code, unused_imports)]

pub mod action;
pub mod core;
pub mod event;
pub mod role;
pub mod snapshot_fetch;
pub mod types;

#[cfg(feature = "sim")]
pub mod sim;

pub use core::{QuorumStateMachine, election_jitter_ms};

pub use action::{Action, TimerKind};
pub use event::{Event, LogEnd};
pub use role::{ReplicaProgress, Role};
pub use types::{Epoch, LogOffsetMetadata, LogView, NodeId, QuorumState, ReplicaKey, SimInstant};
