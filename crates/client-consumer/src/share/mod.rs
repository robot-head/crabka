//! KIP-932 share-group consumer client.
//!
//! A [`ShareConsumer`] joins a *share group* via `ShareGroupHeartbeat`
//! (API key 76) and — unlike the classic [`Consumer`](crate::Consumer) — does
//! not own partitions exclusively. The broker assigns the *same* partitions to
//! multiple members of the group; records are acquired (not assigned) per
//! `ShareFetch` and acknowledged individually (KIP-932 queues).
//!
//! This module currently provides the membership skeleton: the
//! `ShareGroupHeartbeat` join + a background heartbeat loop that tracks the
//! member epoch and live assignment. `poll()` / `acknowledge()` (`ShareFetch` +
//! `ShareAcknowledge`) land in a follow-up task.

mod consumer;
mod coordinator;
mod types;

pub use consumer::ShareConsumer;
pub use types::{ShareAckMode, ShareAckType, ShareConsumerRecord};
