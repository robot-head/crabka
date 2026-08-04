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
        // Named `pg.commit`, the same as the substrate committer's span, so one
        // Grafana query answers "how long did the durable write take" in either
        // engine mode. The two never nest: a `LocalCommitter` writes the KV
        // directly and delegates to no other committer, so exactly one of them
        // is on any commit path.
        let span = tracing::debug_span!(
            target: crate::telemetry::EXEC_TARGET,
            "pg.commit",
            otel.kind = "internal",
            otel.status_code = tracing::field::Empty,
            otel.status_description = tracing::field::Empty,
            db.response.status_code = tracing::field::Empty,
            "error.type" = tracing::field::Empty,
            pg.commit.ops = crate::telemetry::integer(ops.len()),
            pg.commit.mode = "local",
        );
        let _guard = span.enter();
        if let Err(error) = self.kv.write_batch(&ops) {
            let error = ExecError::from(error);
            let rendered = error.clone().into_pg();
            crate::telemetry::record_error(&span, &rendered.code, &rendered.message);
            return Err(error);
        }
        Ok(())
    }
}
