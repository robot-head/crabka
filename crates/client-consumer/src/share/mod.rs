//! KIP-932 share-group consumer client.
//!
//! A [`ShareConsumer`] joins a *share group* via `ShareGroupHeartbeat`
//! (API key 76) and — unlike the classic [`Consumer`](crate::Consumer) — does
//! not own partitions exclusively. The broker assigns the *same* partitions to
//! multiple members of the group; records are acquired (not assigned) per
//! `ShareFetch` and acknowledged individually (KIP-932 queues).
//!
//! Membership runs over a `ShareGroupHeartbeat` join + background heartbeat loop
//! that tracks the member epoch and live assignment; `poll()` issues
//! `ShareFetch` over the live assignment and acknowledgement (implicit/explicit)
//! is carried back via `ShareFetch` piggyback or a standalone
//! `ShareAcknowledge`.

mod consumer;
mod coordinator;
mod poll;
mod types;

pub use consumer::{
    DEFAULT_SHARE_CONSUMER_FETCH_MAX_BYTES, DEFAULT_SHARE_CONSUMER_FETCH_MAX_RECORDS,
    DEFAULT_SHARE_CONSUMER_FETCH_MIN_BYTES, DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT,
    ShareConsumer, ShareConsumerFetchMaxBytes, ShareConsumerFetchMaxRecords,
    ShareConsumerFetchMinBytes, ShareConsumerLeaveHeartbeatTimeout,
};
pub use types::{ShareAckMode, ShareAckType, ShareConsumerRecord};
