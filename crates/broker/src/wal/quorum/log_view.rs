//! `crabka-log` adapter for the sans-IO quorum core.

use std::sync::{Arc, Mutex, MutexGuard};

use crabka_ids::LeaderEpoch;
use crabka_kraft_core::{Epoch, LogView};
use crabka_log::Log;

/// A durable WAL-replica log exposed through [`LogView`].
#[derive(Debug, Clone)]
pub(crate) struct ShardLog {
    log: Arc<Mutex<Log>>,
}

impl ShardLog {
    #[must_use]
    pub(crate) fn new(log: Arc<Mutex<Log>>) -> Self {
        Self { log }
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, Log> {
        self.log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[must_use]
    pub(crate) fn shares_log(&self, other: &Arc<Mutex<Log>>) -> bool {
        Arc::ptr_eq(&self.log, other)
    }
}

impl LogView for ShardLog {
    fn end_offset(&self) -> i64 {
        self.lock().log_end_offset().0
    }

    fn last_epoch(&self) -> Epoch {
        let latest = self
            .lock()
            .epoch_checkpoint()
            .latest_epoch()
            .unwrap_or(LeaderEpoch(0));
        u32::try_from(latest.0).unwrap_or(0)
    }

    fn end_offset_for_epoch(&self, epoch: Epoch) -> Option<i64> {
        let epoch = LeaderEpoch(i32::try_from(epoch).ok()?);
        let log = self.lock();
        match log
            .epoch_checkpoint()
            .end_offset_for_epoch(epoch, log.log_end_offset())
            .0
        {
            -1 => None,
            offset => Some(offset),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use assert2::assert;
    use crabka_log::{Log, LogConfig};
    use crabka_protocol::records::{Record, RecordBatch};

    use super::*;

    #[test]
    fn shard_log_view_reports_offset_and_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&mut batch(3, 2)).unwrap();

        let view = ShardLog::new(Arc::new(Mutex::new(log)));

        assert!(view.end_offset() == 3);
        assert!(view.last_epoch() == 2);
        assert!(view.end_offset_for_epoch(2) == Some(3));
        assert!(view.end_offset_for_epoch(1).is_none());
    }

    fn batch(records: i32, epoch: i32) -> RecordBatch {
        let mut batch = RecordBatch {
            last_offset_delta: records - 1,
            partition_leader_epoch: epoch,
            ..RecordBatch::default()
        };
        for offset_delta in 0..records {
            batch.records.push(Record {
                offset_delta,
                ..Record::default()
            });
        }
        batch
    }
}
