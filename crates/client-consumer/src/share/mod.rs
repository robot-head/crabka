//! KIP-932 share-group consumer client.
//!
//! A [`ShareConsumer`] joins a *share group* with `ShareGroupHeartbeat`
//! (API key 76). Unlike the classic [`Consumer`](crate::Consumer), it does not
//! own partitions exclusively. The broker assigns the *same* partitions to
//! several members of the group. A member acquires records per `ShareFetch`
//! rather than receiving an assignment, and acknowledges them individually
//! through the KIP-932 queues.
//!
//! Membership runs over a `ShareGroupHeartbeat` join plus a background
//! heartbeat loop that tracks the member epoch and the live assignment.
//! `poll()` issues `ShareFetch` over the live assignment. Acknowledgement,
//! implicit or explicit, travels back on a `ShareFetch` piggyback or on a
//! standalone `ShareAcknowledge`.

mod consumer;
mod coordinator;
mod poll;
mod types;

pub use consumer::{
    DEFAULT_SHARE_CONSUMER_FETCH_MAX, DEFAULT_SHARE_CONSUMER_FETCH_MAX_RECORDS,
    DEFAULT_SHARE_CONSUMER_FETCH_MIN, DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT,
    ShareConsumer, ShareConsumerFetchMaxBytes, ShareConsumerFetchMaxRecords,
    ShareConsumerFetchMinBytes, ShareConsumerLeaveHeartbeatTimeout,
};
pub use types::{ShareAckMode, ShareAckType, ShareAcquireMode, ShareConsumerRecord};
