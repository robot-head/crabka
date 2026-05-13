//! Per-partition `.leader-epoch-checkpoint` file. Two-column text
//! format matching Apache Kafka exactly:
//!
//! ```text
//!   0          <-- header version
//!   <n>        <-- row count
//!   <epoch_0> <start_offset_0>
//!   <epoch_1> <start_offset_1>
//!   ...
//! ```
//!
//! Byte layout is preserved so `kafka-dump-log` can read our files.

#![allow(dead_code)]

use std::fmt::Write as _;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

use crate::error::LogError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochEntry {
    pub epoch: i32,
    pub start_offset: i64,
}

#[derive(Debug)]
pub struct LeaderEpochCheckpoint {
    path: PathBuf,
    entries: Vec<EpochEntry>,
}

impl LeaderEpochCheckpoint {
    /// Open (or recover) the checkpoint at `path`. Missing file → empty.
    pub fn open(path: PathBuf) -> Result<Self, LogError> {
        let entries = match fs::read_to_string(&path) {
            Ok(s) => Self::parse(&s)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(LogError::Io(e)),
        };
        Ok(Self { path, entries })
    }

    fn parse(s: &str) -> Result<Vec<EpochEntry>, LogError> {
        let mut lines = s.lines();
        let _version = lines.next();
        let count: usize = lines
            .next()
            .and_then(|l| l.trim().parse().ok())
            .unwrap_or(0);
        let mut out = Vec::with_capacity(count);
        for line in lines.take(count) {
            let mut parts = line.split_whitespace();
            let epoch = parts
                .next()
                .and_then(|t| t.parse().ok())
                .ok_or_else(|| LogError::Corrupt(format!("bad checkpoint row: {line:?}")))?;
            let start_offset = parts
                .next()
                .and_then(|t| t.parse().ok())
                .ok_or_else(|| LogError::Corrupt(format!("bad checkpoint row: {line:?}")))?;
            out.push(EpochEntry {
                epoch,
                start_offset,
            });
        }
        Ok(out)
    }

    /// Append `(epoch, start_offset)`. Idempotent: re-appending an entry
    /// with the same epoch is a no-op (keeps the earliest recorded
    /// `start_offset`). Rewrites the file atomically.
    pub fn append(&mut self, epoch: i32, start_offset: i64) -> Result<(), LogError> {
        if self.entries.iter().any(|e| e.epoch == epoch) {
            return Ok(());
        }
        self.entries.push(EpochEntry {
            epoch,
            start_offset,
        });
        self.flush()
    }

    fn flush(&self) -> Result<(), LogError> {
        let mut s = String::new();
        s.push_str("0\n");
        let _ = writeln!(s, "{}", self.entries.len());
        for e in &self.entries {
            let _ = writeln!(s, "{} {}", e.epoch, e.start_offset);
        }
        let tmp = self.path.with_extension("tmp");
        {
            let mut f = fs::File::create(&tmp).map_err(LogError::Io)?;
            f.write_all(s.as_bytes()).map_err(LogError::Io)?;
            f.sync_data().map_err(LogError::Io)?;
        }
        fs::rename(&tmp, &self.path).map_err(LogError::Io)?;
        Ok(())
    }

    /// End offset of `epoch` = `start_offset` of the next-larger recorded
    /// epoch, or `log_end_offset` if `epoch` is the current epoch.
    /// Returns -1 (`UNDEFINED_OFFSET`) if `epoch` is unknown.
    #[must_use]
    pub fn end_offset_for_epoch(&self, epoch: i32, log_end_offset: i64) -> i64 {
        let mut sorted: Vec<EpochEntry> = self.entries.clone();
        sorted.sort_by_key(|e| e.epoch);
        let mut iter = sorted.iter().peekable();
        while let Some(e) = iter.next() {
            if e.epoch == epoch {
                return iter.peek().map_or(log_end_offset, |next| next.start_offset);
            }
        }
        -1
    }

    #[must_use]
    pub fn latest_epoch(&self) -> Option<i32> {
        self.entries.iter().map(|e| e.epoch).max()
    }

    #[must_use]
    pub fn entries(&self) -> &[EpochEntry] {
        &self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn fresh() -> (TempDir, PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("leader-epoch-checkpoint");
        (dir, path)
    }

    #[test]
    fn round_trip_byte_compat_format() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path.clone()).unwrap();
        c.append(0, 0).unwrap();
        c.append(1, 50).unwrap();
        c.append(2, 100).unwrap();

        let s = std::fs::read_to_string(&path).unwrap();
        assert_eq!(s, "0\n3\n0 0\n1 50\n2 100\n");
    }

    #[test]
    fn append_preserves_existing_rows() {
        let (_d, path) = fresh();
        {
            let mut c = LeaderEpochCheckpoint::open(path.clone()).unwrap();
            c.append(0, 0).unwrap();
        }
        let mut c2 = LeaderEpochCheckpoint::open(path).unwrap();
        c2.append(1, 50).unwrap();
        assert_eq!(c2.entries().len(), 2);
    }

    #[test]
    fn append_idempotent_for_same_epoch() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        c.append(0, 999).unwrap(); // ignored; epoch 0 already recorded
        assert_eq!(
            c.entries(),
            &[EpochEntry {
                epoch: 0,
                start_offset: 0
            }]
        );
    }

    #[test]
    fn end_offset_for_current_epoch_returns_log_end_offset() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        c.append(1, 50).unwrap();
        assert_eq!(c.end_offset_for_epoch(1, 100), 100);
    }

    #[test]
    fn end_offset_for_older_epoch_returns_next_start() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        c.append(1, 50).unwrap();
        c.append(2, 100).unwrap();
        assert_eq!(c.end_offset_for_epoch(0, 200), 50);
        assert_eq!(c.end_offset_for_epoch(1, 200), 100);
    }

    #[test]
    fn end_offset_for_unknown_epoch_returns_undefined() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        assert_eq!(c.end_offset_for_epoch(7, 200), -1);
    }

    #[test]
    fn missing_file_yields_empty() {
        let (_d, path) = fresh();
        let c = LeaderEpochCheckpoint::open(path).unwrap();
        assert!(c.entries().is_empty());
        assert_eq!(c.latest_epoch(), None);
    }
}
