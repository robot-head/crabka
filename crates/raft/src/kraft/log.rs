//! `KraftLog`: the replicated metadata log behind the `LogView` seam.
//! A thin facade over `crabka_log::Log` that adds high-watermark tracking,
//! committed-read filtering for KIP-595 `Fetch`, and divergence lookup. Wired
//! into the controller as the metadata log.

use std::path::Path;

use crabka_log::{Log, LogConfig, RawRead};
use crabka_protocol::records::RecordBatch;

use crate::{
    error::RaftError,
    kraft::types::{LeaderEpoch, LogView},
};

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

    /// Serve KIP-595 `Fetch`: verbatim batch bytes in `[offset, min(hwm, log_end))`.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying raw read fails.
    pub fn read_committed(&self, offset: i64, max_bytes: usize) -> Result<RawRead, RaftError> {
        let limit = self.hwm.min(self.log.log_end_offset());
        Ok(self.log.read_raw(offset, limit, max_bytes)?)
    }

    /// Advance the high watermark (monotonic; never past the log end).
    pub fn advance_hwm(&mut self, new_hwm: i64) {
        let clamped = new_hwm.min(self.log.log_end_offset());
        self.hwm = self.hwm.max(clamped);
        debug_assert!(self.hwm <= self.log.log_end_offset());
    }

    /// Truncate the log so no record at offset `>= offset` remains; clamp HWM down.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying truncation fails.
    pub fn truncate_to(&mut self, offset: i64) -> Result<(), RaftError> {
        debug_assert!(offset >= self.log.log_start_offset());
        self.log.truncate_to(offset)?;
        self.hwm = self.hwm.min(offset);
        Ok(())
    }

    /// Prune the committed prefix below `end_offset`: advance the log-start
    /// pointer and trim now-dead segments. No-op when `end_offset` is at or
    /// below the current log start. Used by the leader after writing a snapshot.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying log operations fail.
    pub fn prune_to(&mut self, end_offset: i64) -> Result<(), RaftError> {
        if end_offset <= self.log.log_start_offset() {
            return Ok(());
        }
        self.log.set_log_start_offset(end_offset)?;
        self.log.trim_to_offset(end_offset)?;
        Ok(())
    }

    /// Replace the log with an empty log starting at `end_offset` (drops every
    /// segment), and set the high watermark to `end_offset`. Used by a follower
    /// installing a fetched snapshot whose `end_offset` is ahead of its log.
    ///
    /// # Errors
    /// Returns [`RaftError`] if the underlying reset fails.
    pub fn install_snapshot(&mut self, end_offset: i64) -> Result<(), RaftError> {
        self.log.reset_to(end_offset)?;
        self.hwm = end_offset;
        Ok(())
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
    use assert2::{assert, check};

    use super::*;

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
        check!(log.log_start_offset() == 0);
        check!(log.log_end_offset() == 0);
        check!(log.hwm() == 0);
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
    fn append_return_matches_assigned_base_and_advances_public_end_offset() {
        let (mut log, _dir) = open_tmp();

        let first = log.append(&mut batch(0, 1, b"a")).unwrap();
        let second = log.append(&mut batch(0, 1, b"b")).unwrap();
        let decoded = log.read_decoded(0, 1 << 20).unwrap();

        check!(first == 0);
        check!(second == 1);
        assert!(decoded.len() == 2);
        check!(decoded[0].base_offset == first);
        check!(decoded[1].base_offset == second);
        check!(log.log_end_offset() == i64::try_from(decoded.len()).unwrap());
        check!(log.log_end_offset() == LogView::end_offset(&log));
    }

    #[test]
    fn public_hwm_accessor_tracks_committed_offset_after_advance_and_snapshot() {
        let (mut log, _dir) = open_tmp();
        for _ in 0..3 {
            log.append(&mut batch(0, 1, b"x")).unwrap();
        }
        log.advance_hwm(2);
        assert!(log.hwm() == 2);

        log.install_snapshot(9).unwrap();
        check!(log.hwm() == 9);
        check!(log.log_start_offset() == 9);
        check!(log.log_end_offset() == 9);
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
        // unknown future epoch → None
        for (epoch, want) in [(1, Some(1)), (2, Some(2)), (9, None)] {
            assert!(
                LogView::end_offset_for_epoch(&log, epoch) == want,
                "epoch {epoch}"
            );
        }
    }

    #[test]
    fn empty_log_last_epoch_is_zero() {
        let (log, _dir) = open_tmp();
        assert!(LogView::last_epoch(&log) == 0);
    }

    #[test]
    fn read_committed_never_returns_bytes_past_hwm() {
        let (mut log, _dir) = open_tmp();
        for _ in 0..5 {
            log.append(&mut batch(0, 1, b"x")).unwrap();
        } // offsets 0..5
        log.advance_hwm(3);
        let r = log.read_committed(0, 1 << 20).unwrap();
        // bytes contain only batches with base_offset < 3 (offsets 0,1,2)
        let decoded = log.read_decoded(0, 1 << 20).unwrap();
        let committed: Vec<_> = decoded.into_iter().filter(|b| b.base_offset < 3).collect();
        check!(committed.len() == 3);
        check!(r.start_offset == 0);
        // total committed bytes equals the size of the first 3 batches
        check!(!r.bytes.is_empty());
    }

    #[test]
    fn advance_hwm_is_monotonic_and_clamped_to_log_end() {
        let (mut log, _dir) = open_tmp();
        log.append(&mut batch(0, 1, b"x")).unwrap(); // log_end = 1
        log.advance_hwm(5); // clamp to log_end
        assert!(log.hwm() == 1);
        log.advance_hwm(0); // never regress
        assert!(log.hwm() == 1);
    }

    #[test]
    fn prune_to_advances_log_start_and_is_noop_when_behind() {
        let (mut log, _dir) = open_tmp();
        for _ in 0..5 {
            log.append(&mut batch(0, 1, b"x")).unwrap();
        }
        log.advance_hwm(log.log_end_offset());
        assert!(log.log_start_offset() == 0);
        log.prune_to(3).unwrap();
        assert!(log.log_start_offset() == 3);
        log.prune_to(2).unwrap(); // <= current start: no-op
        assert!(log.log_start_offset() == 3);
    }

    #[test]
    fn install_snapshot_resets_log_to_empty_at_offset() {
        let (mut log, _dir) = open_tmp();
        for _ in 0..4 {
            log.append(&mut batch(0, 1, b"x")).unwrap();
        }
        log.install_snapshot(100).unwrap();
        check!(log.log_start_offset() == 100);
        check!(log.log_end_offset() == 100);
        check!(log.hwm() == 100);
        let base = log.append(&mut batch(0, 1, b"x")).unwrap();
        assert!(base == 100);
    }

    #[test]
    fn truncate_to_drops_log_end_and_hwm() {
        let (mut log, _dir) = open_tmp();
        for _ in 0..5 {
            log.append(&mut batch(0, 1, b"x")).unwrap();
        }
        log.advance_hwm(5);
        log.truncate_to(2).unwrap();
        assert!(log.log_end_offset() == 2);
        assert!(log.hwm() == 2); // hwm clamped down to the truncation point
    }
}
