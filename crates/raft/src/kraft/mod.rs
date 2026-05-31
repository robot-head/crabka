//! Hand-rolled `KRaft` consensus engine (KIP-595 + KIP-996): a pure,
//! deterministic, sans-IO `on_event` state machine ([`core`]) over the
//! `QuorumState`/`Role` model, driven by the async
//! [`controller::KraftController`] over the [`log::KraftLog`] and the real
//! KIP-595 [`transport`] wire. This is Crabka's live metadata consensus engine.
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
pub use controller::{KraftConfig, KraftController, checkpoint_dir};
pub use core::QuorumStateMachine;
pub use event::Event;
pub use log::KraftLog;
pub use role::Role;
pub use transport::{
    Command, Inbound, MetadataFetchSlice, NullPeerSender, PeerSender, QuorumStateSnapshot,
    TimerTick,
};
pub use types::{
    LeaderEpoch, LogOffsetMetadata, LogView, NodeId, QuorumState, ReplicaKey, SimInstant,
};
