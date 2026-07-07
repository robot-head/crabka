//! Slice-1 WAL medium: a single-node, `fsync`-durable WAL that reuses the
//! partition's existing local `Log`. Offsets are assigned locally (Slice 2
//! moves them to KRaft); durability is a local `fsync` (Slice 6 upgrades to a
//! cross-AZ quorum). Survives crash-restart, NOT node/disk loss.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use crabka_ids::Offset;
use crabka_log::Log;
use tokio::runtime::RuntimeFlavor;

use super::WalStore;
use crate::{error::BrokerError, partition::ProduceData};

/// A [`WalStore`] backed by the partition's local `Log` plus an explicit
/// `fsync` (`Log::sync`).
pub struct LocalFsyncWal {
    log: Arc<Mutex<Log>>,
}

impl LocalFsyncWal {
    #[must_use]
    pub fn new(log: Arc<Mutex<Log>>) -> Self {
        Self { log }
    }
}

#[async_trait]
impl WalStore for LocalFsyncWal {
    async fn append(
        &self,
        datas: Vec<ProduceData>,
    ) -> Result<(Vec<Result<Offset, BrokerError>>, Offset), BrokerError> {
        // Reuse the exact offset-assigning append the classic path uses, so
        // offsets stay locally assigned and identical to a classic topic.
        crate::partition_writer::run_produce_append_batch(self.log.clone(), datas).await
    }

    async fn sync_durable(&self, leo: Offset) -> Result<Offset, BrokerError> {
        let log = self.log.clone();
        // fsync off the async poller (mirrors run_produce_append_batch's
        // block_in_place / spawn_blocking discipline).
        let res = match tokio::runtime::Handle::current().runtime_flavor() {
            RuntimeFlavor::MultiThread => tokio::task::block_in_place(|| {
                log.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .sync()
            }),
            _ => tokio::task::spawn_blocking(move || {
                log.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .sync()
            })
            .await
            .map_err(|e| {
                crate::partition_writer::storage_failure_error("wal fsync task panicked", &e)
            })?,
        };
        res.map_err(BrokerError::from)?;
        Ok(leo)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use assert2::assert;
    use crabka_log::{Log, LogConfig};
    use crabka_protocol::records::{Record, RecordBatch};

    use super::*;
    use crate::partition::ProduceData;

    fn wal(dir: &std::path::Path) -> LocalFsyncWal {
        let log = Arc::new(Mutex::new(Log::open(dir, LogConfig::default()).unwrap()));
        LocalFsyncWal::new(log)
    }

    #[tokio::test]
    async fn append_assigns_sequential_offsets_then_sync_advances_durable() {
        let dir = tempfile::tempdir().unwrap();
        let w = wal(dir.path());
        let (results, leo) = w
            .append(vec![sample_owned(2), sample_owned(3)])
            .await
            .unwrap();
        assert!(results.iter().all(Result::is_ok));
        assert!(leo == crabka_ids::Offset(5));
        // Durable watermark only advances after sync_durable.
        let durable = w.sync_durable(leo).await.unwrap();
        assert!(durable == leo);
    }

    fn sample_owned(n: i32) -> ProduceData {
        ProduceData::Owned(sample_batch(n))
    }

    fn sample_batch(n: i32) -> RecordBatch {
        let mut b = RecordBatch {
            last_offset_delta: n - 1,
            ..RecordBatch::default()
        };
        for i in 0..n {
            b.records.push(Record {
                offset_delta: i,
                ..Default::default()
            });
        }
        b
    }
}
