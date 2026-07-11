//! Per-segment `.txnindex` file. One fixed-width record per aborted
//! transaction in the segment:
//!
//!   `start_offset`: i64 (big-endian)
//!   `last_offset`:  i64 (big-endian)
//!   `producer_id`:  i64 (big-endian)
//!
//! Byte layout matches Apache Kafka's `TransactionIndex`, so
//! `kafka-dump-log --offsets-decoder` can dump it.

use std::{fs::OpenOptions, io::Write, path::PathBuf};

use crabka_ids::{Offset, ProducerId};
use tracing::instrument;
use zerocopy::{
    BigEndian, FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned, byteorder::I64,
};

use crate::error::LogError;

const ENTRY_BYTES: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbortedTxn {
    pub start_offset: Offset,
    pub last_offset: Offset,
    pub producer_id: ProducerId,
}

/// On-disk byte layout of one `AbortedTxn` entry. Reinterpreted in place
/// from the file bytes via `zerocopy`.
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct AbortedTxnRaw {
    start_offset: I64<BigEndian>,
    last_offset: I64<BigEndian>,
    producer_id: I64<BigEndian>,
}

const _: [(); ENTRY_BYTES] = [(); std::mem::size_of::<AbortedTxnRaw>()];

#[derive(Debug)]
pub struct TxnIndex {
    path: PathBuf,
    entries: Vec<AbortedTxn>,
}

impl TxnIndex {
    /// Open (or recover) a `.txnindex` file at the given path. Reads
    /// the entire file into memory at startup. An empty / missing file
    /// is fine — we treat that as zero aborted transactions.
    #[instrument(
        level = "debug",
        skip_all,
        fields(path = %path.display(), entries = tracing::field::Empty),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn open(path: PathBuf) -> Result<Self, LogError> {
        let mut entries = Vec::new();
        match std::fs::read(&path) {
            Ok(bytes) => {
                if !bytes.len().is_multiple_of(ENTRY_BYTES) {
                    return Err(LogError::Corrupt(format!(
                        "txnindex {} has length {} not divisible by {}",
                        path.display(),
                        bytes.len(),
                        ENTRY_BYTES,
                    )));
                }
                let raws = <[AbortedTxnRaw]>::ref_from_bytes(&bytes)
                    .expect("length is a multiple of ENTRY_BYTES and AbortedTxnRaw is Unaligned");
                entries.reserve(raws.len());
                for raw in raws {
                    entries.push(AbortedTxn {
                        start_offset: Offset(raw.start_offset.get()),
                        last_offset: Offset(raw.last_offset.get()),
                        producer_id: ProducerId(raw.producer_id.get()),
                    });
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(LogError::Io(e)),
        }
        tracing::Span::current().record("entries", entries.len());
        Ok(Self { path, entries })
    }

    /// Append one aborted-txn entry.
    #[instrument(
        level = "debug",
        skip(self),
        fields(producer_id = entry.producer_id.0),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn append(&mut self, entry: AbortedTxn) -> Result<(), LogError> {
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(LogError::Io)?;
        let raw = AbortedTxnRaw {
            start_offset: I64::new(entry.start_offset.0),
            last_offset: I64::new(entry.last_offset.0),
            producer_id: I64::new(entry.producer_id.0),
        };
        f.write_all(raw.as_bytes()).map_err(LogError::Io)?;
        f.sync_data().map_err(LogError::Io)?;
        self.entries.push(entry);
        Ok(())
    }

    #[must_use]
    pub fn entries(&self) -> &[AbortedTxn] {
        &self.entries
    }

    /// Aborted transactions whose offset range overlaps `[start, end)`.
    pub fn aborted_in_range(
        &self,
        start: Offset,
        end: Offset,
    ) -> impl Iterator<Item = &AbortedTxn> {
        self.entries.iter().filter(move |e| {
            // Overlap test: [e.start, e.last] intersects [start, end-1]?
            e.start_offset < end && e.last_offset >= start
        })
    }
}

#[cfg(test)]
mod tests {

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn empty_file_yields_empty_entries() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.txnindex");
        let idx = TxnIndex::open(path).unwrap();
        assert2::assert!(idx.entries() == &[]);
    }

    #[test]
    fn append_round_trips_through_disk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.txnindex");
        let mut idx = TxnIndex::open(path.clone()).unwrap();
        idx.append(AbortedTxn {
            start_offset: Offset(5),
            last_offset: Offset(7),
            producer_id: ProducerId(1000),
        })
        .unwrap();
        idx.append(AbortedTxn {
            start_offset: Offset(10),
            last_offset: Offset(12),
            producer_id: ProducerId(1000),
        })
        .unwrap();

        let idx2 = TxnIndex::open(path).unwrap();
        assert2::assert!(
            idx2.entries()
                == &[
                    AbortedTxn {
                        start_offset: Offset(5),
                        last_offset: Offset(7),
                        producer_id: ProducerId(1000)
                    },
                    AbortedTxn {
                        start_offset: Offset(10),
                        last_offset: Offset(12),
                        producer_id: ProducerId(1000)
                    },
                ]
        );
    }

    #[test]
    fn aborted_in_range_overlaps() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.txnindex");
        let mut idx = TxnIndex::open(path).unwrap();
        idx.append(AbortedTxn {
            start_offset: Offset(0),
            last_offset: Offset(4),
            producer_id: ProducerId(1),
        })
        .unwrap();
        idx.append(AbortedTxn {
            start_offset: Offset(10),
            last_offset: Offset(14),
            producer_id: ProducerId(2),
        })
        .unwrap();

        let in_3_to_12 = idx
            .aborted_in_range(Offset(3), Offset(12))
            .copied()
            .collect::<Vec<_>>();
        let in_5_to_9 = idx
            .aborted_in_range(Offset(5), Offset(9))
            .copied()
            .collect::<Vec<_>>();
        assert2::assert!(
            in_3_to_12
                == vec![
                    AbortedTxn {
                        start_offset: Offset(0),
                        last_offset: Offset(4),
                        producer_id: ProducerId(1),
                    },
                    AbortedTxn {
                        start_offset: Offset(10),
                        last_offset: Offset(14),
                        producer_id: ProducerId(2),
                    },
                ]
        );
        assert2::assert!(in_5_to_9 == Vec::new());
    }
}
