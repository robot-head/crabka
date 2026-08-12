//! Per-partition `.stampindex` sidecar. One fixed-width record per
//! stamped offset range in the segment:
//!
//!   `base_offset`: i64 (big-endian)
//!   `last_offset`: i64 (big-endian)
//!   `stamp`:       u64 (big-endian)
//!
//! The stamp is an additional internal coordinate, a packed
//! `TimestampSource` reading, stored beside the wire-exact `.log`. Nothing
//! ever touches the `.log` bytes. The stampindex is internal metadata that is
//! retained and truncated with its segment. It never leaves the broker on any
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

/// One stamped offset range. The inclusive offsets
/// `[base_offset, last_offset]` all carry `stamp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StampEntry {
    pub base_offset: Offset,
    pub last_offset: Offset,
    pub stamp: u64,
}

/// On-disk byte layout of one `StampEntry`. `zerocopy` reinterprets it in
/// place from the file bytes.
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
    /// Open or recover a `.stampindex` file at the given path. This method
    /// reads the entire file into memory at startup. An empty file or a
    /// missing file is acceptable and means zero stamped ranges.
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
    /// `Unaligned` byte buffer fails. That invariant cannot be false.
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
    /// Entries need not arrive in offset order. Transactional ranges are
    /// added when their commit marker lands, and two interleaved transactions
    /// can commit in either order. Ranges themselves must not overlap.
    #[instrument(
        level = "debug",
        skip(self),
        fields(stamp = entry.stamp),
        err,
    )]
    /// # Errors
    /// Returns an error when appending to or syncing the file fails.
    pub fn append(&mut self, entry: StampEntry) -> Result<(), LogError> {
        if entry.last_offset < entry.base_offset {
            return Err(LogError::InvalidArgument(format!(
                "stamp range {}..={} is inverted",
                entry.base_offset, entry.last_offset
            )));
        }
        if let Some(existing) = self.entries.iter().find(|existing| {
            existing.base_offset <= entry.last_offset && entry.base_offset <= existing.last_offset
        }) {
            if *existing == entry {
                return Ok(());
            }
            return Err(LogError::Corrupt(format!(
                "stamp range {}..={} overlaps existing range {}..={} in {}",
                entry.base_offset,
                entry.last_offset,
                existing.base_offset,
                existing.last_offset,
                self.path.display()
            )));
        }
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

    /// Insert a committed transactional range or replace the same exact
    /// range. Replacement upgrades append-time entries written by older
    /// brokers to the marker-time commit stamp.
    ///
    /// # Errors
    /// Returns an error for a partial overlap or when rewriting the sidecar
    /// fails.
    pub fn upsert(&mut self, entry: StampEntry) -> Result<(), LogError> {
        if let Some(position) = self.entries.iter().position(|existing| {
            existing.base_offset == entry.base_offset && existing.last_offset == entry.last_offset
        }) {
            if self.entries[position] != entry {
                let mut entries = self.entries.clone();
                entries[position] = entry;
                self.rewrite(&entries)?;
                self.entries = entries;
            }
            return Ok(());
        }
        self.append(entry)
    }

    /// Remove entries that cover offsets at or after `offset` and rewrite the
    /// sidecar. Log truncation calls this after truncating the segment bytes.
    ///
    /// # Errors
    /// Returns an error when rewriting or syncing the sidecar fails.
    pub fn truncate_from(&mut self, offset: Offset) -> Result<(), LogError> {
        let entries: Vec<_> = self
            .entries
            .iter()
            .copied()
            .filter(|entry| entry.last_offset < offset)
            .collect();
        if entries.len() == self.entries.len() {
            return Ok(());
        }
        self.rewrite(&entries)?;
        self.entries = entries;
        Ok(())
    }

    /// Remove exact ranges, if present. Startup uses this to hide
    /// append-time transactional entries created by older brokers while the
    /// transaction is still open.
    ///
    /// # Errors
    /// Returns an error when rewriting or syncing the sidecar fails.
    pub fn remove_ranges(&mut self, ranges: &[(Offset, Offset)]) -> Result<(), LogError> {
        let entries: Vec<_> = self
            .entries
            .iter()
            .copied()
            .filter(|entry| {
                !ranges
                    .iter()
                    .any(|range| range.0 == entry.base_offset && range.1 == entry.last_offset)
            })
            .collect();
        if entries.len() == self.entries.len() {
            return Ok(());
        }
        self.rewrite(&entries)?;
        self.entries = entries;
        Ok(())
    }

    fn rewrite(&self, entries: &[StampEntry]) -> Result<(), LogError> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .map_err(LogError::Io)?;
        for entry in entries {
            let raw = StampEntryRaw {
                base_offset: I64::new(entry.base_offset.0),
                last_offset: I64::new(entry.last_offset.0),
                stamp: U64::new(entry.stamp),
            };
            file.write_all(raw.as_bytes()).map_err(LogError::Io)?;
        }
        file.sync_data().map_err(LogError::Io)
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

    /// Only a real `NotFound` means "no index yet". Every other I/O error
    /// must surface as `LogError::Io`. Here the path is a directory, so the
    /// read fails with a kind other than `NotFound`. To swallow that error
    /// would return an empty index over a real failure.
    #[test]
    fn open_surfaces_non_notfound_io_error() {
        let dir = TempDir::new().unwrap();
        let err = StampIndex::open(dir.path().to_path_buf()).unwrap_err();
        assert2::assert!(let LogError::Io(_) = err);
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

    #[test]
    fn append_rejects_overlapping_ranges_but_accepts_exact_retry() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.stampindex");
        let mut idx = StampIndex::open(path).unwrap();
        let entry = StampEntry {
            base_offset: Offset(5),
            last_offset: Offset(7),
            stamp: 100,
        };
        idx.append(entry).unwrap();
        idx.append(entry).unwrap();
        assert2::assert!(idx.entries() == [entry]);

        let error = idx
            .append(StampEntry {
                base_offset: Offset(7),
                last_offset: Offset(9),
                stamp: 200,
            })
            .unwrap_err();
        assert2::assert!(let LogError::Corrupt(_) = error);

        let error = idx
            .append(StampEntry {
                base_offset: Offset(10),
                last_offset: Offset(9),
                stamp: 300,
            })
            .unwrap_err();
        assert2::assert!(let LogError::InvalidArgument(_) = error);
    }

    #[test]
    fn upsert_replaces_only_an_exact_range() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.stampindex");
        let mut idx = StampIndex::open(path.clone()).unwrap();
        idx.append(StampEntry {
            base_offset: Offset(5),
            last_offset: Offset(7),
            stamp: 100,
        })
        .unwrap();

        idx.upsert(StampEntry {
            base_offset: Offset(5),
            last_offset: Offset(7),
            stamp: 200,
        })
        .unwrap();
        assert2::assert!(idx.stamp_for_offset(Offset(6)) == Some(200));
        assert2::assert!(StampIndex::open(path).unwrap().entries() == idx.entries());

        for (base, last) in [(5, 8), (4, 7)] {
            let error = idx
                .upsert(StampEntry {
                    base_offset: Offset(base),
                    last_offset: Offset(last),
                    stamp: 300,
                })
                .unwrap_err();
            assert2::assert!(let LogError::Corrupt(_) = error);
        }
    }

    #[test]
    fn truncate_from_removes_tail_entries_on_disk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.stampindex");
        let mut idx = StampIndex::open(path.clone()).unwrap();
        for entry in [
            StampEntry {
                base_offset: Offset(0),
                last_offset: Offset(2),
                stamp: 100,
            },
            StampEntry {
                base_offset: Offset(3),
                last_offset: Offset(6),
                stamp: 103,
            },
            StampEntry {
                base_offset: Offset(10),
                last_offset: Offset(12),
                stamp: 110,
            },
        ] {
            idx.append(entry).unwrap();
        }

        idx.truncate_from(Offset(6)).unwrap();
        assert2::assert!(
            StampIndex::open(path).unwrap().entries()
                == [StampEntry {
                    base_offset: Offset(0),
                    last_offset: Offset(2),
                    stamp: 100,
                }]
        );
    }

    #[test]
    fn remove_ranges_requires_both_exact_boundaries() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.stampindex");
        let mut idx = StampIndex::open(path.clone()).unwrap();
        for entry in [
            StampEntry {
                base_offset: Offset(0),
                last_offset: Offset(1),
                stamp: 10,
            },
            StampEntry {
                base_offset: Offset(2),
                last_offset: Offset(3),
                stamp: 20,
            },
        ] {
            idx.append(entry).unwrap();
        }

        idx.remove_ranges(&[(Offset(0), Offset(9)), (Offset(9), Offset(3))])
            .unwrap();
        assert2::assert!(idx.entries().len() == 2);
        idx.remove_ranges(&[(Offset(2), Offset(3))]).unwrap();

        assert2::assert!(
            StampIndex::open(path).unwrap().entries()
                == [StampEntry {
                    base_offset: Offset(0),
                    last_offset: Offset(1),
                    stamp: 10,
                }]
        );
    }
}
