//! Per-segment `.txnindex` file. One fixed-width record per aborted
//! transaction in the segment:
//!
//!   `start_offset`: i64 (big-endian)
//!   `last_offset`:  i64 (big-endian)
//!   `producer_id`:  i64 (big-endian)
//!
//! Byte layout matches Apache Kafka's `TransactionIndex`, so
//! `kafka-dump-log --offsets-decoder` can dump it.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

use crate::error::LogError;

const ENTRY_BYTES: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbortedTxn {
    pub start_offset: i64,
    pub last_offset: i64,
    pub producer_id: i64,
}

#[derive(Debug)]
pub struct TxnIndex {
    path: PathBuf,
    entries: Vec<AbortedTxn>,
}

impl TxnIndex {
    /// Open (or recover) a `.txnindex` file at the given path. Reads
    /// the entire file into memory at startup. An empty / missing file
    /// is fine — we treat that as zero aborted transactions.
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
                for chunk in bytes.chunks_exact(ENTRY_BYTES) {
                    entries.push(AbortedTxn {
                        start_offset: i64::from_be_bytes(chunk[0..8].try_into().unwrap()),
                        last_offset: i64::from_be_bytes(chunk[8..16].try_into().unwrap()),
                        producer_id: i64::from_be_bytes(chunk[16..24].try_into().unwrap()),
                    });
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(LogError::Io(e)),
        }
        Ok(Self { path, entries })
    }

    /// Append one aborted-txn entry.
    pub fn append(&mut self, entry: AbortedTxn) -> Result<(), LogError> {
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(LogError::Io)?;
        let mut buf = [0u8; ENTRY_BYTES];
        buf[0..8].copy_from_slice(&entry.start_offset.to_be_bytes());
        buf[8..16].copy_from_slice(&entry.last_offset.to_be_bytes());
        buf[16..24].copy_from_slice(&entry.producer_id.to_be_bytes());
        f.write_all(&buf).map_err(LogError::Io)?;
        f.sync_data().map_err(LogError::Io)?;
        self.entries.push(entry);
        Ok(())
    }

    #[must_use]
    pub fn entries(&self) -> &[AbortedTxn] {
        &self.entries
    }

    /// Aborted transactions whose offset range overlaps `[start, end)`.
    pub fn aborted_in_range(&self, start: i64, end: i64) -> impl Iterator<Item = &AbortedTxn> {
        self.entries.iter().filter(move |e| {
            // Overlap test: [e.start, e.last] intersects [start, end-1]?
            e.start_offset < end && e.last_offset >= start
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn empty_file_yields_empty_entries() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.txnindex");
        let idx = TxnIndex::open(path).unwrap();
        assert_eq!(idx.entries(), &[]);
    }

    #[test]
    fn append_round_trips_through_disk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.txnindex");
        let mut idx = TxnIndex::open(path.clone()).unwrap();
        idx.append(AbortedTxn { start_offset: 5, last_offset: 7, producer_id: 1000 }).unwrap();
        idx.append(AbortedTxn { start_offset: 10, last_offset: 12, producer_id: 1000 }).unwrap();

        let idx2 = TxnIndex::open(path).unwrap();
        assert_eq!(idx2.entries(), &[
            AbortedTxn { start_offset: 5, last_offset: 7, producer_id: 1000 },
            AbortedTxn { start_offset: 10, last_offset: 12, producer_id: 1000 },
        ]);
    }

    #[test]
    fn aborted_in_range_overlaps() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.txnindex");
        let mut idx = TxnIndex::open(path).unwrap();
        idx.append(AbortedTxn { start_offset: 0, last_offset: 4, producer_id: 1 }).unwrap();
        idx.append(AbortedTxn { start_offset: 10, last_offset: 14, producer_id: 2 }).unwrap();

        let in_3_to_12: Vec<_> = idx.aborted_in_range(3, 12).collect();
        assert_eq!(in_3_to_12.len(), 2);

        let in_5_to_9: Vec<_> = idx.aborted_in_range(5, 9).collect();
        assert_eq!(in_5_to_9.len(), 0);
    }
}
