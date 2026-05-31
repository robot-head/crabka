//! KIP-932 share coordinator (persister): durable per-`(group, topicId,
//! partition)` delivery state stored in the `__share_group_state` internal
//! topic. Mirrors the transaction coordinator (`crate::txn`).

pub mod config;
