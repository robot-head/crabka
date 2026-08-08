//! KIP-932 share coordinator, also called the persister.
//!
//! The coordinator holds durable per-`(group, topicId, partition)` delivery
//! state in the `__share_group_state` internal topic. It mirrors the
//! transaction coordinator, `crate::txn`.

pub mod bootstrap;
pub mod config;
pub(crate) mod coordinator;
pub(crate) mod handlers;
pub mod partitioner;
pub mod persistence;
pub(crate) mod persister_client;
pub mod pruning;
pub mod state;
