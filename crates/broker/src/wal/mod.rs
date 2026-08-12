//! The WAL durability seam for diskless broker partitions. The partition
//! writer assigns offsets and appends to its source log, then asks this store
//! to replicate and durably commit that log prefix. Production uses the replicated
//! [`quorum::QuorumWalStore`]; tests can use the local-fsync implementation.

#[cfg(test)]
mod local_fsync;
mod offset_sequencer;
pub(crate) mod quorum;

use std::sync::Arc;

use async_trait::async_trait;
use crabka_ids::Offset;
#[cfg(test)]
pub(crate) use local_fsync::LocalFsyncWal;
pub(crate) use offset_sequencer::{ControllerSequencer, OffsetSequencer};

use crate::error::BrokerError;

/// A durability medium behind the partition writer.
#[async_trait]
pub trait WalStore: Send + Sync {
    /// Make all records up to `leo` durable; return the durable LEO. Never
    /// regresses the durable watermark.
    async fn sync_durable(&self, leo: Offset) -> Result<Offset, BrokerError>;

    /// Discard the durable prefix below `new_start` after the object index has
    /// committed it. Returns the resulting local log start offset.
    async fn trim_to_offset(&self, new_start: Offset) -> Result<Offset, BrokerError>;
}

/// Convenience alias for an injected WAL medium (present only for diskless topics).
pub type SharedWal = Arc<dyn WalStore>;
