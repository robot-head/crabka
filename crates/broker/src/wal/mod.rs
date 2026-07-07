//! The WAL durability seam. Slice 1 of the diskless broker: a two-phase
//! "append (assign offsets) then make durable" contract that the partition
//! writer drives for diskless-mode topics. The Slice-1 implementation is
//! [`local_fsync::LocalFsyncWal`]; later slices swap it for a replicated /
//! object-store-backed WAL without changing the writer or the ack gate.

#![allow(dead_code)]

mod local_fsync;

use std::sync::Arc;

use async_trait::async_trait;
use crabka_ids::Offset;
#[allow(unused_imports)]
pub(crate) use local_fsync::LocalFsyncWal;

use crate::{error::BrokerError, partition::ProduceData};

/// A durability medium behind the partition writer.
///
/// Two-phase so the writer can resolve the produce offset (for `acks=0/1`)
/// before durability completes, then gate `acks=all` on `sync_durable`:
/// 1. [`WalStore::append`] assigns offsets and returns the post-append LEO,
///    WITHOUT waiting for durability.
/// 2. [`WalStore::sync_durable`] makes everything up to `leo` durable and
///    returns the (monotonic) durable LEO.
#[async_trait]
pub trait WalStore: Send + Sync {
    /// Append a group of batches, assigning offsets. Not yet durable.
    async fn append(
        &self,
        datas: Vec<ProduceData>,
    ) -> Result<(Vec<Result<Offset, BrokerError>>, Offset), BrokerError>;

    /// Make all records up to `leo` durable; return the durable LEO. Never
    /// regresses the durable watermark.
    async fn sync_durable(&self, leo: Offset) -> Result<Offset, BrokerError>;
}

/// Convenience alias for an injected WAL medium (present only for diskless topics).
pub type SharedWal = Arc<dyn WalStore>;
