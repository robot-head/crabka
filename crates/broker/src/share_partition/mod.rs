//! KIP-932 share-partition leader: the in-memory acquisition state machine.
//!
//! This is not `share_coordinator::state::SharePartitionState`, which is the
//! *persisted* type. [`state::AcquisitionState`] is the live, per-partition
//! offset-range state machine that the share-partition leader drives during
//! `ShareFetch` and `ShareAcknowledge`.
pub(crate) mod backlog_poller;
pub mod manager;
pub mod session;
pub mod state;
