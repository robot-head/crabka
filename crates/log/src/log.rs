//! `Log` — a sorted collection of `Segment`s with append/read/truncate.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crabka_protocol::records::RecordBatch;

use crate::config::LogConfig;
use crate::error::LogError;
use crate::name;
use crate::segment::Segment;

#[derive(Debug)]
pub struct Log {
    // `dir` and `config` are read by `append`/`truncate_to` in subsequent
    // tasks of this batch; the discovery-only `Log::open` doesn't yet
    // re-read them.
    #[allow(dead_code)]
    dir: PathBuf,
    #[allow(dead_code)]
    config: LogConfig,
    segments: Vec<Arc<Segment>>,
    active: Option<Segment>,
}

#[derive(Debug)]
pub struct ReadOutput {
    pub start_offset: i64,
    pub batches: Vec<RecordBatch>,
}

impl Log {
    /// Open or create a `Log` at `dir`. Discovers existing segments by
    /// `.log` filename, marks all but the latest as sealed, and (if the
    /// directory is empty) creates a fresh active segment at offset 0.
    pub fn open(dir: impl AsRef<Path>, config: LogConfig) -> Result<Self, LogError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut base_offsets: Vec<i64> = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let Ok(file_name) = entry.file_name().into_string() else {
                continue; // non-UTF-8 names: ignore (unlikely)
            };
            if let Ok(base) = name::parse_log_filename(&file_name) {
                base_offsets.push(base);
            }
        }
        base_offsets.sort_unstable();
        base_offsets.dedup();

        let mut segments: Vec<Arc<Segment>> = Vec::with_capacity(base_offsets.len());
        let mut active: Option<Segment> = None;
        for (i, base) in base_offsets.iter().enumerate() {
            let mut seg = Segment::open(&dir, *base)?;
            if i + 1 < base_offsets.len() {
                seg.seal();
                segments.push(Arc::new(seg));
            } else {
                active = Some(seg);
            }
        }

        let active = match active {
            Some(s) => s,
            None => Segment::create(&dir, 0)?,
        };

        Ok(Self {
            dir,
            config,
            segments,
            active: Some(active),
        })
    }

    /// First absolute offset still in the log.
    #[must_use]
    pub fn log_start_offset(&self) -> i64 {
        if let Some(first) = self.segments.first() {
            return first.base_offset();
        }
        if let Some(active) = &self.active {
            return active.base_offset();
        }
        0
    }

    /// Next offset that `append` will assign.
    #[must_use]
    pub fn log_end_offset(&self) -> i64 {
        if let Some(active) = &self.active {
            return active.last_offset() + 1;
        }
        0
    }

    /// Close all segments. Drop runs automatically when `self` moves;
    /// this method just names the operation explicitly.
    pub fn close(self) {
        drop(self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn open_empty_dir_creates_first_segment() {
        let dir = tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(log.log_start_offset(), 0);
        assert_eq!(log.log_end_offset(), 0);
        log.close();
    }

    #[test]
    fn open_creates_log_file() {
        let dir = tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        drop(log);
        let log_path = dir.path().join("00000000000000000000.log");
        assert!(log_path.exists());
    }
}
