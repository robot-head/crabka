//! Per-partition `.stampindex` sidecar. One fixed-width record per
//! stamped offset range in the segment:
//!
//!   `base_offset`: i64 (big-endian)
//!   `last_offset`: i64 (big-endian)
//!   `stamp`:       u64 (big-endian)
//!
//! The stamp is an additional internal coordinate — a packed
//! `TimestampSource` reading — stored beside the wire-exact `.log`. The
//! `.log` bytes are never touched; the stampindex is derived state,
//! rebuildable by rescanning the log, and never leaves the broker on any
//! client-facing API. This mirrors the `.txnindex` sidecar pattern.

use std::{fs::OpenOptions, io::Write, path::PathBuf};

use crabka_ids::Offset;
use tracing::instrument;
use zerocopy::{
    BigEndian, FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
    byteorder::{I64, U64},
};

use crate::error::LogError;

const ENTRY_BYTES: usize = 24;

/// One stamped offset range: the inclusive offsets `[base_offset,
/// last_offset]` all carry `stamp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StampEntry {
    pub base_offset: Offset,
    pub last_offset: Offset,
    pub stamp: u64,
}

/// On-disk byte layout of one `StampEntry`. Reinterpreted in place from
/// the file bytes via `zerocopy`.
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct StampEntryRaw {
    base_offset: I64<BigEndian>,
    last_offset: I64<BigEndian>,
    stamp: U64<BigEndian>,
}

const _: [(); ENTRY_BYTES] = [(); std::mem::size_of::<StampEntryRaw>()];

#[derive(Debug)]
pub struct StampIndex {
    path: PathBuf,
    entries: Vec<StampEntry>,
}

impl StampIndex {
    /// Open (or recover) a `.stampindex` file at the given path. Reads
    /// the entire file into memory at startup. An empty / missing file
    /// is fine — we treat that as zero stamped ranges.
    #[instrument(
        level = "debug",
        skip_all,
        fields(path = %path.display(), entries = tracing::field::Empty),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails or the file's length is not a
    /// whole number of fixed-width entries.
    /// # Panics
    /// Panics if the in-place reinterpretation of a length-validated,
    /// `Unaligned` byte buffer fails — an invariant that cannot hold false.
    pub fn open(path: PathBuf) -> Result<Self, LogError> {
        let mut entries = Vec::new();
        match std::fs::read(&path) {
            Ok(bytes) => {
                if !bytes.len().is_multiple_of(ENTRY_BYTES) {
                    return Err(LogError::Corrupt(format!(
                        "stampindex {} has length {} not divisible by {}",
                        path.display(),
                        bytes.len(),
                        ENTRY_BYTES,
                    )));
                }
                let raws = <[StampEntryRaw]>::ref_from_bytes(&bytes)
                    .expect("length is a multiple of ENTRY_BYTES and StampEntryRaw is Unaligned");
                entries.reserve(raws.len());
                for raw in raws {
                    entries.push(StampEntry {
                        base_offset: Offset(raw.base_offset.get()),
                        last_offset: Offset(raw.last_offset.get()),
                        stamp: raw.stamp.get(),
                    });
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(LogError::Io(e)),
        }
        tracing::Span::current().record("entries", entries.len());
        Ok(Self { path, entries })
    }

    /// Append one stamped-range entry.
    ///
    /// # Precondition
    /// Entries are appended in nondecreasing offset order: each new
    /// entry's `base_offset` is at least the previous entry's
    /// `base_offset`. Stamps are folded to observe partition offset order
    /// before they reach this method, so within a partition stamp order
    /// never contradicts offset order.
    #[instrument(
        level = "debug",
        skip(self),
        fields(stamp = entry.stamp),
        err,
    )]
    /// # Errors
    /// Returns an error when appending to or syncing the file fails.
    pub fn append(&mut self, entry: StampEntry) -> Result<(), LogError> {
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(LogError::Io)?;
        let raw = StampEntryRaw {
            base_offset: I64::new(entry.base_offset.0),
            last_offset: I64::new(entry.last_offset.0),
            stamp: U64::new(entry.stamp),
        };
        f.write_all(raw.as_bytes()).map_err(LogError::Io)?;
        f.sync_data().map_err(LogError::Io)?;
        self.entries.push(entry);
        Ok(())
    }

    #[must_use]
    pub fn entries(&self) -> &[StampEntry] {
        &self.entries
    }

    /// The stamp of the entry whose inclusive `[base_offset, last_offset]`
    /// range contains `offset`, or `None` when no entry covers it.
    #[must_use]
    pub fn stamp_for_offset(&self, offset: Offset) -> Option<u64> {
        self.entries
            .iter()
            .find(|e| e.base_offset <= offset && offset <= e.last_offset)
            .map(|e| e.stamp)
    }
}

#[cfg(test)]
mod tests {

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn stamp_empty_file_yields_empty_entries() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.stampindex");
        let idx = StampIndex::open(path).unwrap();
        assert2::assert!(idx.entries() == &[]);
    }

    #[test]
    fn stamp_append_round_trips_through_disk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.stampindex");
        let mut idx = StampIndex::open(path.clone()).unwrap();
        idx.append(StampEntry {
            base_offset: Offset(5),
            last_offset: Offset(7),
            stamp: 1_000,
        })
        .unwrap();
        idx.append(StampEntry {
            base_offset: Offset(10),
            last_offset: Offset(12),
            stamp: 2_000,
        })
        .unwrap();

        let idx2 = StampIndex::open(path).unwrap();
        assert2::assert!(
            idx2.entries()
                == &[
                    StampEntry {
                        base_offset: Offset(5),
                        last_offset: Offset(7),
                        stamp: 1_000,
                    },
                    StampEntry {
                        base_offset: Offset(10),
                        last_offset: Offset(12),
                        stamp: 2_000,
                    },
                ]
        );
    }

    #[test]
    fn stamp_corrupt_length_is_rejected() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.stampindex");
        std::fs::write(&path, [0_u8; ENTRY_BYTES + 1]).unwrap();
        let err = StampIndex::open(path).unwrap_err();
        assert2::assert!(let LogError::Corrupt(_) = err);
    }

    #[test]
    fn stamp_for_offset_finds_covering_range() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.stampindex");
        let mut idx = StampIndex::open(path).unwrap();
        idx.append(StampEntry {
            base_offset: Offset(0),
            last_offset: Offset(4),
            stamp: 100,
        })
        .unwrap();
        idx.append(StampEntry {
            base_offset: Offset(10),
            last_offset: Offset(14),
            stamp: 200,
        })
        .unwrap();

        // Inclusive endpoints and interior offsets resolve to their range.
        assert2::assert!(idx.stamp_for_offset(Offset(0)) == Some(100));
        assert2::assert!(idx.stamp_for_offset(Offset(4)) == Some(100));
        assert2::assert!(idx.stamp_for_offset(Offset(10)) == Some(200));
        assert2::assert!(idx.stamp_for_offset(Offset(14)) == Some(200));
        // Offsets in the gap and past the end are uncovered.
        assert2::assert!(idx.stamp_for_offset(Offset(5)) == None);
        assert2::assert!(idx.stamp_for_offset(Offset(15)) == None);
    }
}
