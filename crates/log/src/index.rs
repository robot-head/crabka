//! Sparse offset index. 8 bytes per entry: `relative_offset` (u32 BE)
//! + position (u32 BE). Entries are monotonically increasing.

// Methods are consumed by later phases (Segment, Log); kept module-internal
// until those wire up. Suppress dead-code while batches land incrementally.
#![allow(dead_code)]

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::LogError;

/// 8 bytes per entry.
pub const OFFSET_ENTRY_SIZE: usize = 8;

#[derive(Debug)]
pub struct OffsetIndex {
    file: File,
    /// Entries currently in the file. Lazily loaded into memory on construction.
    entries: Vec<(u32, u32)>,
}

impl OffsetIndex {
    /// Open or create an offset-index file. If the file exists, load its
    /// entries into memory. If it doesn't, create an empty file.
    pub fn open(path: &Path) -> Result<Self, LogError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut entries = Vec::with_capacity(buf.len() / OFFSET_ENTRY_SIZE);
        for chunk in buf.chunks_exact(OFFSET_ENTRY_SIZE) {
            let rel = u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
            let pos = u32::from_be_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
            entries.push((rel, pos));
        }
        Ok(Self { file, entries })
    }

    /// Append a new entry. Caller ensures monotonicity.
    pub fn append(&mut self, relative_offset: u32, position: u32) -> Result<(), LogError> {
        let mut buf = [0u8; OFFSET_ENTRY_SIZE];
        buf[0..4].copy_from_slice(&relative_offset.to_be_bytes());
        buf[4..8].copy_from_slice(&position.to_be_bytes());
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&buf)?;
        self.entries.push((relative_offset, position));
        Ok(())
    }

    /// Find the byte position to start reading at for a given relative offset.
    /// Returns the position of the largest entry with `relative_offset <= target`,
    /// or 0 if no entries are present.
    #[must_use]
    pub fn lookup(&self, target: u32) -> u32 {
        // Binary search for the largest entry <= target.
        match self.entries.binary_search_by_key(&target, |&(rel, _)| rel) {
            Ok(i) => self.entries[i].1,
            Err(0) => 0,
            Err(i) => self.entries[i - 1].1,
        }
    }

    /// Truncate entries (and the on-disk file) so that all entries with
    /// `position >= max_position_exclusive` are removed.
    pub fn truncate_by_position(&mut self, max_position_exclusive: u32) -> Result<(), LogError> {
        let new_len = self
            .entries
            .iter()
            .take_while(|(_, pos)| *pos < max_position_exclusive)
            .count();
        self.entries.truncate(new_len);
        let new_file_len = (new_len * OFFSET_ENTRY_SIZE) as u64;
        self.file.set_len(new_file_len)?;
        self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }

    #[must_use]
    pub fn last_entry(&self) -> Option<(u32, u32)> {
        self.entries.last().copied()
    }

    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub fn flush(&mut self) -> Result<(), LogError> {
        self.file.sync_data().map_err(LogError::Io)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn append_and_lookup() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        let mut idx = OffsetIndex::open(&path).unwrap();
        idx.append(0, 0).unwrap();
        idx.append(100, 4096).unwrap();
        idx.append(200, 8192).unwrap();
        assert_eq!(idx.lookup(50), 0);
        assert_eq!(idx.lookup(100), 4096);
        assert_eq!(idx.lookup(150), 4096);
        assert_eq!(idx.lookup(200), 8192);
        assert_eq!(idx.lookup(9999), 8192);
    }

    #[test]
    fn empty_index_returns_zero() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        let idx = OffsetIndex::open(&path).unwrap();
        assert_eq!(idx.lookup(0), 0);
        assert_eq!(idx.lookup(1000), 0);
    }

    #[test]
    fn persists_across_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        {
            let mut idx = OffsetIndex::open(&path).unwrap();
            idx.append(0, 0).unwrap();
            idx.append(100, 4096).unwrap();
            idx.flush().unwrap();
        }
        let idx = OffsetIndex::open(&path).unwrap();
        assert_eq!(idx.entry_count(), 2);
        assert_eq!(idx.lookup(100), 4096);
    }

    #[test]
    fn truncate_by_position() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        let mut idx = OffsetIndex::open(&path).unwrap();
        idx.append(0, 0).unwrap();
        idx.append(100, 4096).unwrap();
        idx.append(200, 8192).unwrap();
        idx.truncate_by_position(8192).unwrap();
        assert_eq!(idx.entry_count(), 2);
        assert_eq!(idx.last_entry(), Some((100, 4096)));
    }
}
