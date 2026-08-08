//! KIP-932 share-partition leader: the in-memory acquisition state machine.
//!
//! Distinct from `share_coordinator::state::SharePartitionState` (the
//! *persisted* type). [`state::AcquisitionState`] is the live, per-partition
//! offset-range state machine the share-partition leader drives during
//! `ShareFetch`/`ShareAcknowledge`.
pub(crate) mod backlog_poller;
pub mod manager;
pub mod session;
pub mod state;
