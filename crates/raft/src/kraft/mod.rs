//! Hand-rolled `KRaft` consensus engine (KIP-595 and KIP-996).
//!
//! The engine is a pure, deterministic, sans-IO `on_event` state machine
//! ([`core`]) over the `QuorumState` and `Role` model. The async
//! [`controller::KraftController`] drives that state machine over the
//! [`log::KraftLog`] and the real KIP-595 [`transport`] wire. This is Crabka's
//! live metadata consensus engine.
#![allow(dead_code, unused_imports)]

// The pure, deterministic, sans-IO consensus core (the `on_event` state
// machine, its event/action/role/type model, and the snapshot reassembler)
// lives in the wasm-friendly leaf crate `crabka-kraft-core`. Re-export its
// modules here so the async engine, real wire, and on-disk log below keep
// referencing `crate::kraft::{core, types, event, ...}` unchanged.
pub use crabka_kraft_core::{action, core, event, role, snapshot_fetch, types};

pub mod controller;
pub mod log;
pub mod transport;

pub use core::QuorumStateMachine;

pub use action::Action;
pub use controller::{KraftConfig, KraftController, checkpoint_dir};
pub use event::Event;
pub use log::KraftLog;
pub use role::Role;
pub use transport::{
    Command, Inbound, MetadataFetchSlice, NullPeerSender, PeerSender, QuorumStateSnapshot,
    TimerTick,
};
pub use types::{Epoch, LogOffsetMetadata, LogView, NodeId, QuorumState, ReplicaKey, SimInstant};
