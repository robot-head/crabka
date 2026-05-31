//! `KraftLog`: the real replicated metadata log behind the 3a `LogView` seam.
//! A thin facade over `crabka_log::Log` that adds high-watermark tracking,
//! committed-read filtering for KIP-595 `Fetch`, and divergence lookup. Wired
//! into the controller (replacing openraft's `log_store`) in slice 3c.

use std::path::Path;

use crabka_log::{Log, LogConfig, RawRead};
use crabka_protocol::records::RecordBatch;

use crate::error::RaftError;
use crate::kraft::types::{LeaderEpoch, LogView};

pub struct KraftLog {
    log: Log,
    /// Highest committed offset (consensus state; crabka-log does not track it).
    hwm: i64,
}

impl KraftLog {
    /// Open or create the metadata log under `dir/@metadata-0`.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the log directory cannot be created or the
    /// underlying `crabka_log::Log` fails to open.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, RaftError> {
        let log_dir = dir.as_ref().join("@metadata-0");
        std::fs::create_dir_all(&log_dir).map_err(crabka_log::LogError::Io)?;
        let log = Log::open(&log_dir, LogConfig::default())?;
        let hwm = log.log_start_offset();
        Ok(Self { log, hwm })
    }

    #[must_use]
    pub fn log_start_offset(&self) -> i64 {
        self.log.log_start_offset()
    }
    #[must_use]
    pub fn log_end_offset(&self) -> i64 {
        self.log.log_end_offset()
    }
    #[must_use]
    pub fn hwm(&self) -> i64 {
        self.hwm
    }

    /// Leader path: append a batch; crabka-log assigns the offset and records the
    /// batch's `partition_leader_epoch`. Returns the assigned base offset.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying append fails.
    pub fn append(&mut self, batch: &mut RecordBatch) -> Result<i64, RaftError> {
        Ok(self.log.append(batch)?)
    }

    /// Follower path: append a batch at the leader-assigned `offset`.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying append fails (e.g. `offset` does
    /// not equal the current log end offset).
    pub fn append_at(&mut self, batch: &mut RecordBatch, offset: i64) -> Result<(), RaftError> {
        self.log.append_at(batch, offset)?;
        Ok(())
    }

    /// Decoded read (used by tests + replication apply). Reads from `offset`.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying read fails.
    pub fn read_decoded(
        &self,
        offset: i64,
        max_bytes: usize,
    ) -> Result<Vec<RecordBatch>, RaftError> {
        Ok(self.log.read(offset, max_bytes)?.batches)
    }
}

impl LogView for KraftLog {
    fn end_offset(&self) -> i64 {
        self.log.log_end_offset()
    }
    fn last_epoch(&self) -> LeaderEpoch {
        // crabka-log epochs are i32 and non-negative; 0 for an empty log.
        u32::try_from(self.log.epoch_checkpoint().latest_epoch().unwrap_or(0)).unwrap_or(0)
    }
    fn end_offset_for_epoch(&self, epoch: LeaderEpoch) -> Option<i64> {
        let log_end = self.log.log_end_offset();
        let epoch_i32 = i32::try_from(epoch).ok()?;
        match self
            .log
            .epoch_checkpoint()
            .end_offset_for_epoch(epoch_i32, log_end)
        {
            -1 => None,
            off => Some(off),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    fn open_tmp() -> (KraftLog, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = KraftLog::open(dir.path()).expect("open");
        (log, dir)
    }

    // test helper
    fn batch(base: i64, epoch: i32, value: &[u8]) -> RecordBatch {
        use crabka_protocol::records::{Attributes, Record};
        RecordBatch {
            base_offset: base,
            partition_leader_epoch: epoch,
            attributes: Attributes::default(),
            last_offset_delta: 0,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![Record {
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(bytes::Bytes::copy_from_slice(value)),
                headers: Vec::new(),
            }],
        }
    }

    #[test]
    fn opens_empty_at_offset_zero() {
        let (log, _dir) = open_tmp();
        assert!(log.log_start_offset() == 0);
        assert!(log.log_end_offset() == 0);
        assert!(log.hwm() == 0);
    }

    #[test]
    fn append_assigns_sequential_offsets_and_reads_back() {
        let (mut log, _dir) = open_tmp();
        let off0 = log.append(&mut batch(0, 1, b"a")).unwrap();
        let off1 = log.append(&mut batch(0, 1, b"b")).unwrap();
        assert!(off0 == 0 && off1 == 1);
        assert!(log.log_end_offset() == 2);
        // read back decoded
        let out = log.read_decoded(0, 1 << 20).unwrap();
        assert!(out.len() == 2);
        assert!(out[0].partition_leader_epoch == 1);
    }

    #[test]
    fn append_at_preserves_leader_offset() {
        let (mut log, _dir) = open_tmp();
        // follower applies a leader-assigned batch at offset 0
        log.append_at(&mut batch(0, 2, b"x"), 0).unwrap();
        assert!(log.log_end_offset() == 1);
        assert!(log.read_decoded(0, 1 << 20).unwrap()[0].partition_leader_epoch == 2);
    }

    #[test]
    fn logview_reports_end_offset_and_last_epoch() {
        let (mut log, _dir) = open_tmp();
        log.append(&mut batch(0, 1, b"a")).unwrap();
        log.append(&mut batch(0, 3, b"b")).unwrap(); // epoch jumps to 3
        assert!(LogView::end_offset(&log) == 2);
        assert!(LogView::last_epoch(&log) == 3);
    }

    #[test]
    fn logview_end_offset_for_epoch_maps_unknown_to_none() {
        let (mut log, _dir) = open_tmp();
        log.append(&mut batch(0, 1, b"a")).unwrap(); // epoch 1 @ [0,1)
        log.append(&mut batch(0, 2, b"b")).unwrap(); // epoch 2 @ [1,2)
        // epoch 1 ends where epoch 2 starts (offset 1); epoch 2 is current → end 2.
        assert!(LogView::end_offset_for_epoch(&log, 1) == Some(1));
        assert!(LogView::end_offset_for_epoch(&log, 2) == Some(2));
        // unknown future epoch → None
        assert!(LogView::end_offset_for_epoch(&log, 9).is_none());
    }

    #[test]
    fn empty_log_last_epoch_is_zero() {
        let (log, _dir) = open_tmp();
        assert!(LogView::last_epoch(&log) == 0);
    }
}
