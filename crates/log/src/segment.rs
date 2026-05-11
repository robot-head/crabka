//! A single segment: `.log` + `.index` + `.timeindex` files sharing a
//! base offset.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crabka_protocol::records::RecordBatch;

use crate::error::LogError;
use crate::index::{OffsetIndex, TimeIndex};
use crate::name;

#[derive(Debug)]
pub struct Segment {
    #[allow(dead_code)] // used by later phases (Log retention, recovery).
    dir: PathBuf,
    base_offset: i64,
    log_file: File,
    log_size: u64,
    offset_index: OffsetIndex,
    #[allow(dead_code)] // written by `append` (Task 8); not read until Task 9+.
    time_index: TimeIndex,
    /// `true` once a new segment has been started after this one. Sealed
    /// segments don't accept appends.
    sealed: bool,
    /// Highest timestamp observed across all batches written here.
    max_timestamp: i64,
    /// Last absolute offset (inclusive) of any batch in this segment.
    last_offset: i64,
}

impl Segment {
    /// Open an existing segment for reading. Lightweight — no full scan.
    pub fn open(dir: &Path, base_offset: i64) -> Result<Self, LogError> {
        let log_path = name::log_path(dir, base_offset);
        let log_file = OpenOptions::new().read(true).write(true).open(&log_path)?;
        let log_size = log_file.metadata()?.len();
        let offset_index = OffsetIndex::open(&name::index_path(dir, base_offset))?;
        let time_index = TimeIndex::open(&name::timeindex_path(dir, base_offset))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            base_offset,
            log_file,
            log_size,
            offset_index,
            time_index,
            sealed: false,
            max_timestamp: i64::MIN,
            last_offset: base_offset - 1,
        })
    }

    #[must_use]
    pub fn base_offset(&self) -> i64 {
        self.base_offset
    }

    #[must_use]
    pub fn last_offset(&self) -> i64 {
        self.last_offset
    }

    #[must_use]
    pub fn size_bytes(&self) -> u64 {
        self.log_size
    }

    #[must_use]
    pub fn max_timestamp(&self) -> i64 {
        self.max_timestamp
    }

    #[must_use]
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    /// Read batches starting at or just before `offset`, up to roughly
    /// `max_bytes` of `.log` data. Returns an empty `Vec` when `offset`
    /// is past `last_offset`.
    pub fn read(&self, offset: i64, max_bytes: usize) -> Result<Vec<RecordBatch>, LogError> {
        if offset > self.last_offset {
            return Ok(vec![]);
        }
        let target_rel = u32::try_from((offset - self.base_offset).max(0))
            .map_err(|_| LogError::BadSegmentName("target offset out of range".into()))?;
        let start_pos = u64::from(self.offset_index.lookup(target_rel));

        let initial_cap = max_bytes.min(4 * 1024 * 1024);
        let mut buf: Vec<u8> = Vec::with_capacity(initial_cap);
        self.read_log_range(start_pos, &mut buf, max_bytes)?;

        let mut out: Vec<RecordBatch> = Vec::new();
        let mut total: usize = 0;
        let mut cursor: &[u8] = &buf;
        while !cursor.is_empty() {
            let before = cursor.len();
            let Ok(batch) = RecordBatch::decode(&mut cursor) else {
                break; // partial trailing batch — stop.
            };
            let consumed = before - cursor.len();
            let batch_last = batch.base_offset + i64::from(batch.last_offset_delta);
            if batch_last >= offset {
                out.push(batch);
                total += consumed;
                if total >= max_bytes {
                    break;
                }
            }
        }
        Ok(out)
    }

    fn read_log_range(
        &self,
        start_pos: u64,
        buf: &mut Vec<u8>,
        max_bytes: usize,
    ) -> Result<(), LogError> {
        let available = self.log_size.saturating_sub(start_pos);
        let to_read = available.min(u64::try_from(max_bytes).unwrap_or(u64::MAX));
        let mut f = self.log_file.try_clone()?;
        f.seek(SeekFrom::Start(start_pos))?;
        let mut bounded = f.take(to_read);
        bounded.read_to_end(buf)?;
        Ok(())
    }
}
