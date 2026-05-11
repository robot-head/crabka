//! `Log` — a sorted collection of `Segment`s with append/read/truncate.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use crabka_protocol::records::RecordBatch;

use crate::config::LogConfig;
use crate::error::LogError;
use crate::name;
use crate::retention;
use crate::segment::Segment;

#[derive(Debug)]
pub struct Log {
    dir: PathBuf,
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
            if i + 1 < base_offsets.len() {
                let mut seg = Segment::open(&dir, *base)?;
                seg.seal();
                segments.push(Arc::new(seg));
            } else {
                active = Some(Segment::open_active(&dir, *base, config.validate_on_open)?);
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

    /// Append a `RecordBatch`. The batch's `base_offset` is overwritten
    /// by the log to be the next assigned offset; `last_offset_delta`
    /// determines how many absolute offsets this batch consumes.
    /// Returns the assigned `base_offset`.
    pub fn append(&mut self, batch: &mut RecordBatch) -> Result<i64, LogError> {
        let should_roll = match &self.active {
            Some(seg) => seg.size_bytes() >= self.config.segment_bytes,
            None => false,
        };
        if should_roll {
            self.roll_active_segment()?;
        }

        let assigned_base = self.log_end_offset();
        batch.base_offset = assigned_base;

        let active = self
            .active
            .as_mut()
            .expect("active segment must exist after Log::open");
        active.append(batch, self.config.index_interval_bytes)?;

        if self.config.flush_on_append {
            active.flush()?;
        }
        Ok(assigned_base)
    }

    fn roll_active_segment(&mut self) -> Result<(), LogError> {
        let new_base = self.log_end_offset();
        let mut old = self
            .active
            .take()
            .expect("active segment must exist before rolling");
        old.seal();
        self.segments.push(Arc::new(old));
        self.active = Some(Segment::create(&self.dir, new_base)?);
        Ok(())
    }

    /// Read batches starting at `offset`, returning up to roughly
    /// `max_bytes` of `.log` data. Walks sealed segments first, then the
    /// active segment, so reads can span segment boundaries.
    pub fn read(&self, offset: i64, max_bytes: usize) -> Result<ReadOutput, LogError> {
        let log_start = self.log_start_offset();
        let log_end = self.log_end_offset();
        if offset < log_start {
            return Err(LogError::OffsetTooLow {
                requested: offset,
                log_start,
            });
        }
        if offset >= log_end {
            return Ok(ReadOutput {
                start_offset: log_end,
                batches: Vec::new(),
            });
        }

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut current_offset = offset;
        let mut remaining = max_bytes;

        for seg in &self.segments {
            if seg.last_offset() < current_offset {
                continue;
            }
            let bs = seg.read(current_offset, remaining)?;
            if !bs.is_empty() {
                let consumed: usize = bs.iter().map(RecordBatch::encoded_len).sum();
                remaining = remaining.saturating_sub(consumed);
                let last = bs.last().expect("non-empty by branch");
                current_offset = last.base_offset + i64::from(last.last_offset_delta) + 1;
                batches.extend(bs);
                if remaining == 0 {
                    break;
                }
            }
        }

        if (remaining > 0 || batches.is_empty())
            && let Some(active) = &self.active
            && current_offset <= active.last_offset()
        {
            let bs = active.read(current_offset, remaining.max(1))?;
            batches.extend(bs);
        }

        let start_offset = batches.first().map_or(offset, |b| b.base_offset);
        Ok(ReadOutput {
            start_offset,
            batches,
        })
    }

    /// Truncate the log so no records at offset `>= offset` remain. Used
    /// by replication / leader election.
    pub fn truncate_to(&mut self, offset: i64) -> Result<(), LogError> {
        let log_start = self.log_start_offset();
        let log_end = self.log_end_offset();
        if offset >= log_end {
            return Ok(()); // nothing to truncate
        }
        if offset < log_start {
            return Err(LogError::OffsetTooLow {
                requested: offset,
                log_start,
            });
        }

        // Drop sealed segments whose base_offset >= offset.
        while let Some(last_sealed) = self.segments.last() {
            if last_sealed.base_offset() >= offset {
                let popped = self.segments.pop().expect("non-empty by while-let");
                let base = popped.base_offset();
                drop(popped);
                let _ = fs::remove_file(name::log_path(&self.dir, base));
                let _ = fs::remove_file(name::index_path(&self.dir, base));
                let _ = fs::remove_file(name::timeindex_path(&self.dir, base));
            } else {
                break;
            }
        }

        // Drop the active segment if its base_offset >= offset.
        if let Some(active) = &self.active
            && active.base_offset() >= offset
        {
            let base = active.base_offset();
            self.active = None;
            let _ = fs::remove_file(name::log_path(&self.dir, base));
            let _ = fs::remove_file(name::index_path(&self.dir, base));
            let _ = fs::remove_file(name::timeindex_path(&self.dir, base));
        }

        // If no active segment, promote the last sealed one (if any) and
        // truncate it in place. Otherwise, create a fresh one at `offset`.
        if self.active.is_none() {
            if let Some(last) = self.segments.pop() {
                let mut seg =
                    Arc::try_unwrap(last).expect("no outstanding readers during truncate");
                let rel = u32::try_from(offset - seg.base_offset())
                    .map_err(|_| LogError::BadSegmentName("offset overflow".into()))?;
                seg.truncate_to_relative(rel)?;
                self.active = Some(seg);
            } else {
                self.active = Some(Segment::create(&self.dir, offset)?);
            }
        } else if let Some(active) = self.active.as_mut()
            && active.last_offset() >= offset
        {
            // The surviving active segment contains records at or past
            // `offset`; truncate them in place.
            let rel = u32::try_from(offset - active.base_offset())
                .map_err(|_| LogError::BadSegmentName("offset overflow".into()))?;
            active.truncate_to_relative(rel)?;
        }
        Ok(())
    }

    /// Periodic maintenance: apply time- and size-based retention to the
    /// sealed segments. The active segment is never deleted, and if every
    /// segment would otherwise be evicted we retain at least one.
    /// (Active-roll-on-age is a placeholder per the plan; skip it.)
    pub fn tick(&mut self, now: SystemTime) -> Result<(), LogError> {
        let sealed_refs: Vec<&Segment> = self.segments.iter().map(AsRef::as_ref).collect();
        let active_size = self.active.as_ref().map_or(0, Segment::size_bytes);

        let time_evict = retention::time_based_evict(&sealed_refs, &self.config, now);
        let size_evict = retention::size_based_evict(&sealed_refs, active_size, &self.config);

        // Union preserving order: time first (oldest first), then size.
        let mut to_evict: Vec<i64> = time_evict;
        for base in size_evict {
            if !to_evict.contains(&base) {
                to_evict.push(base);
            }
        }

        // Guard: never drop the only remaining segment. `total_segments`
        // includes the active one.
        let total_segments = self.segments.len() + usize::from(self.active.is_some());
        if to_evict.len() >= total_segments {
            to_evict.truncate(total_segments.saturating_sub(1));
        }

        for base in to_evict {
            self.segments.retain(|s| s.base_offset() != base);
            let _ = retention::delete_segment_files(&self.dir, base);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crabka_protocol::records::Record;
    use tempfile::tempdir;

    fn sample_batch(n: i32) -> RecordBatch {
        let mut b = RecordBatch {
            base_offset: 0, // overwritten by Log::append
            max_timestamp: 0,
            last_offset_delta: n - 1,
            ..RecordBatch::default()
        };
        for i in 0..n {
            b.records.push(Record {
                offset_delta: i,
                key: Some(Bytes::from(format!("k{i}"))),
                value: Some(Bytes::from(format!("v{i}"))),
                ..Default::default()
            });
        }
        b
    }

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

    #[test]
    fn append_assigns_monotonic_offsets() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b1 = sample_batch(3);
        let mut b2 = sample_batch(2);
        assert_eq!(log.append(&mut b1).unwrap(), 0);
        assert_eq!(log.append(&mut b2).unwrap(), 3);
        assert_eq!(log.log_end_offset(), 5);
    }

    #[test]
    fn append_then_read_back_in_order() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        for _ in 0..3 {
            let mut b = sample_batch(2);
            log.append(&mut b).unwrap();
        }
        let out = log.read(0, usize::MAX).unwrap();
        assert_eq!(out.batches.len(), 3);
        assert_eq!(out.start_offset, 0);
    }

    #[test]
    fn read_offset_too_low_errors() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
        assert!(matches!(
            log.read(-1, 1024),
            Err(LogError::OffsetTooLow { .. })
        ));
    }

    #[test]
    fn read_at_log_end_returns_empty() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
        let out = log.read(log.log_end_offset(), 1024).unwrap();
        assert!(out.batches.is_empty());
    }

    #[test]
    fn truncate_to_drops_later_records() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b1 = sample_batch(3);
        let mut b2 = sample_batch(2);
        log.append(&mut b1).unwrap();
        log.append(&mut b2).unwrap();
        assert_eq!(log.log_end_offset(), 5);
        log.truncate_to(3).unwrap();
        // First batch (offsets 0..=2) survives; last_offset == 2, end == 3.
        assert_eq!(log.log_end_offset(), 3);
    }

    #[test]
    fn truncate_to_log_end_is_noop() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
        let before = log.log_end_offset();
        log.truncate_to(before + 100).unwrap();
        assert_eq!(log.log_end_offset(), before);
    }

    #[test]
    fn open_recovers_partial_trailing_batch() {
        let dir = tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            let mut b1 = sample_batch(3);
            let mut b2 = sample_batch(2);
            log.append(&mut b1).unwrap();
            log.append(&mut b2).unwrap();
        }
        // Append 10 bytes of garbage to the .log file.
        let log_path = dir.path().join("00000000000000000000.log");
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .unwrap();
        std::io::Write::write_all(&mut f, &[0xAB; 10]).unwrap();
        f.sync_data().unwrap();
        drop(f);
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(log.log_end_offset(), 5);
    }

    #[test]
    fn tick_with_no_retention_is_noop() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b1 = sample_batch(2);
        let mut b2 = sample_batch(3);
        log.append(&mut b1).unwrap();
        log.append(&mut b2).unwrap();
        let before = log.log_end_offset();
        log.tick(SystemTime::now()).unwrap();
        assert_eq!(log.log_end_offset(), before);
    }

    #[test]
    fn tick_never_deletes_only_segment() {
        use std::time::Duration;
        let dir = tempdir().unwrap();
        let config = LogConfig {
            retention_ms: Some(Duration::from_secs(1)),
            retention_bytes: Some(0),
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        let mut b1 = sample_batch(2);
        log.append(&mut b1).unwrap();
        // Advance "now" 30 days into the future.
        let now = SystemTime::now() + Duration::from_hours(30 * 24);
        log.tick(now).unwrap();
        assert_eq!(log.log_end_offset(), 2);
    }

    #[test]
    fn segment_rolls_when_bytes_exceeded() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_bytes: 200, // tiny so we roll fast
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        for _ in 0..5 {
            let mut b = sample_batch(2);
            log.append(&mut b).unwrap();
        }
        // Multiple .log files should exist now.
        let log_files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("log"))
            .collect();
        assert!(
            log_files.len() >= 2,
            "expected segment roll; got {} .log files",
            log_files.len()
        );
    }
}
