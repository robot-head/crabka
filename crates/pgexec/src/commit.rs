//! The durable-write seam. SP6 wrote one batch via `Kv::write_batch`; SP7 routes
//! those batches through a `Committer` so a replicated engine can propose them
//! through Raft instead. The local impl is byte-for-byte the SP6 write.

use std::sync::Arc;

use crabka_pgkv::{Kv, WriteOp};

use crate::error::ExecError;

#[async_trait::async_trait]
pub trait Committer: Send + Sync {
    /// Durably apply one atomic batch. Returns only once the batch is durable
    /// (local: written; replicated: committed to a majority AND applied).
    async fn commit(&self, ops: Vec<WriteOp>) -> Result<(), ExecError>;
}

/// Single-node committer: writes straight to the local KV (SP6 behavior).
pub struct LocalCommitter {
    pub(crate) kv: Arc<dyn Kv>,
}

#[async_trait::async_trait]
impl Committer for LocalCommitter {
    async fn commit(&self, ops: Vec<WriteOp>) -> Result<(), ExecError> {
        self.kv.write_batch(&ops)?;
        Ok(())
    }
}
