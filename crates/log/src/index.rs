//! Sparse offset index. Each entry is 8 bytes: `relative_offset` as u32 BE
//! and position as u32 BE. Entries increase monotonically.

// `truncate_by_position`, `truncate_by_relative_offset`, `entry_count`,
// and `TimeIndex::{lookup, last_entry}` are consumed by log truncation,
// recovery, and lookup-by-time paths.
#![allow(dead_code)]

use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use tracing::instrument;
use zerocopy::{
    BigEndian, FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
    byteorder::{I64, U32},
};

use crate::error::LogError;

/// 8 bytes per entry.
pub const OFFSET_ENTRY_SIZE: usize = 8;

/// On-disk byte layout of one offset-index entry.
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct OffsetEntryRaw {
    relative_offset: U32<BigEndian>,
    position: U32<BigEndian>,
}

const _: [(); OFFSET_ENTRY_SIZE] = [(); std::mem::size_of::<OffsetEntryRaw>()];

#[derive(Debug)]
pub struct OffsetIndex {
    file: File,
    /// Entries currently in the file. The constructor loads them into memory
    /// lazily.
    entries: Vec<(u32, u32)>,
}

impl OffsetIndex {
    /// Open or create an offset-index file. If the file exists, this method
    /// loads its entries into memory. If it does not exist, this method
    /// creates an empty file.
    #[instrument(
        level = "debug",
        skip_all,
        fields(path = %path.display(), entries = tracing::field::Empty),
        err,
    )]
    pub fn open(path: &Path) -> Result<Self, LogError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let truncated_len = (buf.len() / OFFSET_ENTRY_SIZE) * OFFSET_ENTRY_SIZE;
        let raws = <[OffsetEntryRaw]>::ref_from_bytes(&buf[..truncated_len])
            .expect("length is a multiple of OFFSET_ENTRY_SIZE and OffsetEntryRaw is Unaligned");
        // Byte positions strictly increase across real entries. A Kafka
        // index file is preallocated to `segment.index.bytes` and only
        // truncated on clean roll/shutdown, so an unclean copy carries
        // trailing zero-padding that decodes to `(0, 0)`. Stop at the
        // first non-increasing position to keep `lookup`'s binary search
        // operating over a monotonic slice.
        let mut entries: Vec<(u32, u32)> = Vec::with_capacity(raws.len());
        for r in raws {
            let (rel, pos) = (r.relative_offset.get(), r.position.get());
            if let Some(&(_, prev_pos)) = entries.last()
                && pos <= prev_pos
            {
                break;
            }
            entries.push((rel, pos));
        }
        tracing::Span::current().record("entries", entries.len());
        Ok(Self { file, entries })
    }

    /// Append a new entry. The caller must keep the entries monotonic.
    pub fn append(&mut self, relative_offset: u32, position: u32) -> Result<(), LogError> {
        let raw = OffsetEntryRaw {
            relative_offset: U32::new(relative_offset),
            position: U32::new(position),
        };
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(raw.as_bytes())?;
        self.entries.push((relative_offset, position));
        Ok(())
    }

    /// Find the byte position where a read for a given relative offset must
    /// start. This method returns the position of the largest entry with
    /// `relative_offset <= target`, or 0 when there are no entries.
    #[must_use]
    pub fn lookup(&self, target: u32) -> u32 {
        crabka_verified::offset_index_lookup(&self.entries, target)
    }

    /// Truncate the entries, and the on-disk file, so that no entry with
    /// `position >= max_position_exclusive` remains.
    #[instrument(level = "debug", skip(self), fields(entries = tracing::field::Empty), err)]
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
        tracing::Span::current().record("entries", new_len);
        Ok(())
    }

    /// Byte position of the first entry whose `relative_offset >= target`, or
    /// `None` when every entry is below `target`. Every batch that covers an
    /// offset `< target` lies strictly below this position, so the position
    /// bounds a scan from the start that must stop at `target`.
    #[must_use]
    pub fn position_at_or_after(&self, target: u32) -> Option<u32> {
        match self.entries.binary_search_by_key(&target, |&(rel, _)| rel) {
            Ok(i) => Some(self.entries[i].1),
            Err(i) => self.entries.get(i).map(|&(_, pos)| pos),
        }
    }

    #[must_use]
    pub fn last_entry(&self) -> Option<(u32, u32)> {
        self.entries.last().copied()
    }

    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    #[instrument(level = "debug", skip_all, err)]
    pub fn flush(&mut self) -> Result<(), LogError> {
        self.file.sync_data().map_err(LogError::Io)
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn append_and_lookup() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        let mut idx = OffsetIndex::open(&path).unwrap();
        idx.append(0, 0).unwrap();
        idx.append(100, 4096).unwrap();
        idx.append(200, 8192).unwrap();
        for (name, offset, want) in [
            ("floor first", 50, 0),
            ("exact middle", 100, 4096),
            ("floor middle", 150, 4096),
            ("exact last", 200, 8192),
            ("past last", 9999, 8192),
        ] {
            check!(idx.lookup(offset) == want, "case {name}: offset={offset}");
        }
    }

    #[test]
    fn empty_index_returns_zero() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        let idx = OffsetIndex::open(&path).unwrap();
        for (_name, offset) in [("zero", 0), ("positive", 1000)] {
            assert2::assert!(idx.lookup(offset) == 0);
        }
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
        assert2::assert!(idx.entry_count() == 2);
        assert2::assert!(idx.lookup(100) == 4096);
    }

    #[test]
    fn ignores_trailing_zero_padding() {
        // Kafka preallocates `.index` to `segment.index.bytes` and only
        // truncates on clean shutdown; an unclean copy carries trailing
        // zero entries. Loading must stop at the real data so the binary
        // search stays monotonic.
        use std::io::Write;
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        {
            let mut idx = OffsetIndex::open(&path).unwrap();
            idx.append(0, 0).unwrap();
            idx.append(100, 4096).unwrap();
            idx.flush().unwrap();
        }
        // Append two zero-filled entries (preallocation padding).
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[0u8; OFFSET_ENTRY_SIZE * 2]).unwrap();
        f.sync_data().unwrap();
        drop(f);

        let idx = OffsetIndex::open(&path).unwrap();
        assert2::assert!(idx.entry_count() == 2);
        assert2::assert!(idx.last_entry() == Some((100, 4096)));
        assert2::assert!(idx.lookup(150) == 4096);
    }

    #[test]
    fn position_at_or_after_finds_ceiling() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        let mut idx = OffsetIndex::open(&path).unwrap();
        idx.append(0, 0).unwrap();
        idx.append(100, 4096).unwrap();
        idx.append(200, 8192).unwrap();
        for (name, offset, want) in [
            ("exact", 100, Some(4096)),
            ("ceiling", 150, Some(8192)),
            ("first", 0, Some(0)),
            ("past last", 201, None),
        ] {
            check!(
                idx.position_at_or_after(offset) == want,
                "case {name}: offset={offset}"
            );
        }
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
        assert2::assert!(idx.entry_count() == 2);
        assert2::assert!(idx.last_entry() == Some((100, 4096)));
    }
}

// ===== TimeIndex =====

/// 12 bytes per entry: timestamp as i64 BE and `relative_offset` as u32 BE.
pub const TIME_ENTRY_SIZE: usize = 12;

/// On-disk byte layout of one time-index entry.
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct TimeEntryRaw {
    timestamp: I64<BigEndian>,
    relative_offset: U32<BigEndian>,
}

const _: [(); TIME_ENTRY_SIZE] = [(); std::mem::size_of::<TimeEntryRaw>()];

#[derive(Debug)]
pub struct TimeIndex {
    file: File,
    entries: Vec<(i64, u32)>,
}

impl TimeIndex {
    #[instrument(
        level = "debug",
        skip_all,
        fields(path = %path.display(), entries = tracing::field::Empty),
        err,
    )]
    pub fn open(path: &Path) -> Result<Self, LogError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let truncated_len = (buf.len() / TIME_ENTRY_SIZE) * TIME_ENTRY_SIZE;
        let raws = <[TimeEntryRaw]>::ref_from_bytes(&buf[..truncated_len])
            .expect("length is a multiple of TIME_ENTRY_SIZE and TimeEntryRaw is Unaligned");
        // Relative offsets strictly increase across real entries; trailing
        // `(0, 0)` padding from a preallocated Kafka index decodes as a
        // non-increasing offset. Stop there. (Timestamps may repeat when
        // `max_timestamp` is unchanged between index points, so the offset
        // column — not the timestamp — is the monotonic discriminator.)
        let mut entries: Vec<(i64, u32)> = Vec::with_capacity(raws.len());
        for r in raws {
            let (ts, rel) = (r.timestamp.get(), r.relative_offset.get());
            if let Some(&(_, prev_rel)) = entries.last()
                && rel <= prev_rel
            {
                break;
            }
            entries.push((ts, rel));
        }
        tracing::Span::current().record("entries", entries.len());
        Ok(Self { file, entries })
    }

    /// Append an entry. The caller must keep the entries monotonic.
    pub fn append(&mut self, timestamp: i64, relative_offset: u32) -> Result<(), LogError> {
        let raw = TimeEntryRaw {
            timestamp: I64::new(timestamp),
            relative_offset: U32::new(relative_offset),
        };
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(raw.as_bytes())?;
        self.entries.push((timestamp, relative_offset));
        Ok(())
    }

    /// Find the relative offset at or after the given timestamp. This method
    /// returns the relative offset of the largest entry with
    /// `timestamp <= target`, or 0 when there are no entries.
    #[must_use]
    pub fn lookup(&self, target_timestamp: i64) -> u32 {
        match self
            .entries
            .binary_search_by_key(&target_timestamp, |&(ts, _)| ts)
        {
            Ok(i) => self.entries[i].1,
            Err(0) => 0,
            Err(i) => self.entries[i - 1].1,
        }
    }

    #[instrument(level = "debug", skip(self), fields(entries = tracing::field::Empty), err)]
    pub fn truncate_by_relative_offset(&mut self, max_rel_exclusive: u32) -> Result<(), LogError> {
        let new_len = self
            .entries
            .iter()
            .take_while(|(_, rel)| *rel < max_rel_exclusive)
            .count();
        self.entries.truncate(new_len);
        self.file.set_len((new_len * TIME_ENTRY_SIZE) as u64)?;
        self.file.seek(SeekFrom::End(0))?;
        tracing::Span::current().record("entries", new_len);
        Ok(())
    }

    #[must_use]
    pub fn last_entry(&self) -> Option<(i64, u32)> {
        self.entries.last().copied()
    }

    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    #[instrument(level = "debug", skip_all, err)]
    pub fn flush(&mut self) -> Result<(), LogError> {
        self.file.sync_data().map_err(LogError::Io)
    }
}

#[cfg(test)]
mod time_tests {
    use assert2::check;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn append_and_lookup_time() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.timeindex");
        let mut idx = TimeIndex::open(&path).unwrap();
        idx.append(1_000_000, 0).unwrap();
        idx.append(2_000_000, 100).unwrap();
        idx.append(3_000_000, 200).unwrap();
        for (name, ts, want) in [
            ("before first", 0, 0),
            ("floor first", 1_500_000, 0),
            ("exact middle", 2_000_000, 100),
            ("floor middle", 2_500_000, 100),
            ("past last", 5_000_000, 200),
        ] {
            check!(idx.lookup(ts) == want, "case {name}: ts={ts}");
        }
    }

    #[test]
    fn persists_across_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.timeindex");
        {
            let mut idx = TimeIndex::open(&path).unwrap();
            idx.append(1, 0).unwrap();
            idx.append(2, 50).unwrap();
            idx.flush().unwrap();
        }
        let idx = TimeIndex::open(&path).unwrap();
        assert2::assert!(idx.entry_count() == 2);
    }

    #[test]
    fn ignores_trailing_zero_padding() {
        use std::io::Write;
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.timeindex");
        {
            let mut idx = TimeIndex::open(&path).unwrap();
            idx.append(1_000, 0).unwrap();
            idx.append(2_000, 100).unwrap();
            idx.flush().unwrap();
        }
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[0u8; TIME_ENTRY_SIZE * 2]).unwrap();
        f.sync_data().unwrap();
        drop(f);

        let idx = TimeIndex::open(&path).unwrap();
        assert2::assert!(idx.entry_count() == 2);
        assert2::assert!(idx.last_entry() == Some((2_000, 100)));
        assert2::assert!(idx.lookup(2_500) == 100);
    }
}
