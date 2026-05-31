//! KIP-932 share coordinator (persister): durable per-`(group, topicId,
//! partition)` delivery state stored in the `__share_group_state` internal
//! topic. Mirrors the transaction coordinator (`crate::txn`).

pub mod bootstrap;
pub mod config;
pub(crate) mod coordinator;
pub mod partitioner;
pub mod persistence;
pub mod pruning;
pub mod state;
