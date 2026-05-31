//! Hand-rolled `KRaft` consensus core (KIP-595 + KIP-996). Pure, deterministic,
//! sans-IO: an `on_event` state machine over `QuorumState` + `Role`. Not wired
//! to the controller/wire/log in slice 3a (openraft remains the live engine);
//! 3b/3c integrate it and ultimately replace openraft.
//!
//! Slice 3a leaves this module unwired (openraft is still the live engine), so
//! its public surface is dead code from the crate's perspective until 3b; the
//! `allow` keeps the staged scaffold warning-free.
#![allow(dead_code, unused_imports)]

pub mod action;
pub mod controller;
pub mod core;
pub mod event;
pub mod log;
pub mod role;
pub mod transport;
pub mod types;

pub use action::Action;
pub use controller::{KraftConfig, KraftController};
pub use core::QuorumStateMachine;
pub use event::Event;
pub use log::KraftLog;
pub use role::Role;
pub use transport::{Command, Inbound, NullPeerSender, PeerSender};
pub use types::{
    LeaderEpoch, LogOffsetMetadata, LogView, NodeId, QuorumState, ReplicaKey, SimInstant,
};
