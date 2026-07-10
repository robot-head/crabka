//! The linearizable-read seam. Mirrors the durable-write `Committer` seam: a read
//! confirms it may observe local state before taking its MVCC snapshot. The local
//! impl is a no-op (single-node applied state is authoritative); the replicated
//! impl (`cluster::RaftLinearizer`) performs an openraft ReadIndex check.

use crate::error::ExecError;

#[async_trait::async_trait]
pub trait Linearizer: Send + Sync {
    /// Confirm this node may serve a linearizable read now. Replicated: confirm
    /// leadership via a quorum heartbeat and block until the local state machine
    /// has applied through the read log id. `Err(NotLeader)` (or `Unavailable`)
    /// if leadership can't be confirmed (deposed/partitioned), so the caller
    /// rejects the read rather than serving stale state.
    async fn ensure_readable(&self) -> Result<(), ExecError>;
}

/// Single-node / non-replicated: local applied state is authoritative, so a read
/// is always immediately serveable.
pub struct LocalLinearizer;

#[async_trait::async_trait]
impl Linearizer for LocalLinearizer {
    async fn ensure_readable(&self) -> Result<(), ExecError> {
        Ok(())
    }
}
