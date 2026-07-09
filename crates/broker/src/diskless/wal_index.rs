//! Diskless WAL offset-to-object index records and in-memory projection.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Diskless WAL index schema version carried in committed flush records.
pub const WAL_INDEX_FORMAT_VERSION: u16 = 2;

/// One aborted transaction whose data overlaps a flushed diskless WAL run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalAbortedTxn {
    pub producer_id: i64,
    pub first_offset: i64,
    pub last_offset: i64,
}

impl WalAbortedTxn {
    /// Whether this aborted transaction overlaps the half-open offset range
    /// `[first_offset, last_offset_exclusive)`.
    #[must_use]
    pub fn overlaps(self, first_offset: i64, last_offset_exclusive: i64) -> bool {
        if last_offset_exclusive <= first_offset {
            return false;
        }
        self.first_offset < last_offset_exclusive && self.last_offset >= first_offset
    }
}

/// One partition's byte range within a flushed WAL object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalIndexEntry {
    pub topic_id: Uuid,
    pub partition: i32,
    pub first_offset: i64,
    pub last_offset: i64,
    pub byte_start: u64,
    pub byte_len: u32,
    pub aborted_transactions: Vec<WalAbortedTxn>,
}

/// Durable index event for one flushed diskless WAL object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalFlushRecord {
    pub object_key: String,
    pub format_version: u16,
    pub entries: Vec<WalIndexEntry>,
}

impl WalFlushRecord {
    /// Serialize this record with the workspace `serde-wincode` codec.
    ///
    /// # Errors
    ///
    /// Returns an error if wincode cannot encode the record.
    pub fn to_bytes(&self) -> Result<Bytes, String> {
        <serde_wincode::SerdeCompat<Self> as wincode::Serialize>::serialize(self)
            .map(Bytes::from)
            .map_err(|error| error.to_string())
    }

    /// Deserialize a record written by [`Self::to_bytes`].
    ///
    /// # Errors
    ///
    /// Returns an error if `bytes` is not a valid encoded `WalFlushRecord`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        <serde_wincode::SerdeCompat<Self> as wincode::Deserialize>::deserialize(bytes)
            .map_err(|error| error.to_string())
    }
}

/// In-memory projection of committed `WalFlushRecord`s.
#[derive(Default)]
pub struct WalIndexCache {
    by_topic_partition: HashMap<(Uuid, i32), BTreeMap<i64, (String, WalIndexEntry)>>,
}

impl WalIndexCache {
    /// Apply one committed flush record to the projection.
    pub fn apply(&mut self, record: &WalFlushRecord) {
        for entry in &record.entries {
            self.by_topic_partition
                .entry((entry.topic_id, entry.partition))
                .or_default()
                .insert(
                    entry.first_offset,
                    (record.object_key.clone(), entry.clone()),
                );
        }
    }

    /// Return the object and index entry covering `offset`, if one exists.
    #[must_use]
    pub fn lookup(
        &self,
        topic_id: Uuid,
        partition: i32,
        offset: i64,
    ) -> Option<(String, WalIndexEntry)> {
        let entries = self.by_topic_partition.get(&(topic_id, partition))?;
        let (_, (object_key, entry)) = entries.range(..=offset).next_back()?;
        (offset <= entry.last_offset).then(|| (object_key.clone(), entry.clone()))
    }

    /// Return the highest flushed offset plus one for the partition.
    #[must_use]
    pub fn flushed_frontier(&self, topic_id: Uuid, partition: i32) -> Option<i64> {
        let entries = self.by_topic_partition.get(&(topic_id, partition))?;
        entries
            .values()
            .next_back()
            .map(|(_, entry)| entry.last_offset + 1)
    }

    /// Return the smallest first offset covered by object storage for the partition.
    #[must_use]
    pub fn earliest_covered(&self, topic_id: Uuid, partition: i32) -> Option<i64> {
        self.by_topic_partition
            .get(&(topic_id, partition))?
            .values()
            .next()
            .map(|(_, entry)| entry.first_offset)
    }

    /// Remove all projected entries that point at `object_key`.
    pub fn tombstone_object(&mut self, object_key: &str) {
        for entries in self.by_topic_partition.values_mut() {
            entries.retain(|_, (key, _)| key != object_key);
        }
        self.by_topic_partition
            .retain(|_, entries| !entries.is_empty());
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use uuid::Uuid;

    use super::*;

    fn entry(p: i32, f: i64, l: i64) -> WalIndexEntry {
        WalIndexEntry {
            topic_id: Uuid::from_u128(1),
            partition: p,
            first_offset: f,
            last_offset: l,
            byte_start: 0,
            byte_len: 1,
            aborted_transactions: Vec::new(),
        }
    }

    #[test]
    fn floor_lookup_returns_covering_object() {
        let mut c = WalIndexCache::default();
        c.apply(&WalFlushRecord {
            object_key: "o1".into(),
            format_version: WAL_INDEX_FORMAT_VERSION,
            entries: vec![entry(0, 0, 4)],
        });
        c.apply(&WalFlushRecord {
            object_key: "o2".into(),
            format_version: WAL_INDEX_FORMAT_VERSION,
            entries: vec![entry(0, 5, 9)],
        });
        let t = Uuid::from_u128(1);
        assert!(c.lookup(t, 0, 3).unwrap().0 == "o1");
        assert!(c.lookup(t, 0, 7).unwrap().0 == "o2");
        assert!(c.lookup(t, 0, 20).is_none());
        assert!(c.flushed_frontier(t, 0) == Some(10));
    }

    #[test]
    fn apply_is_idempotent() {
        let mut c = WalIndexCache::default();
        let rec = WalFlushRecord {
            object_key: "o1".into(),
            format_version: WAL_INDEX_FORMAT_VERSION,
            entries: vec![entry(0, 0, 4)],
        };
        c.apply(&rec);
        c.apply(&rec);
        let t = Uuid::from_u128(1);
        assert!(c.flushed_frontier(t, 0) == Some(5));
    }

    #[test]
    fn wincode_round_trips() {
        let rec = WalFlushRecord {
            object_key: "o".into(),
            format_version: WAL_INDEX_FORMAT_VERSION,
            entries: vec![WalIndexEntry {
                aborted_transactions: vec![WalAbortedTxn {
                    producer_id: 42,
                    first_offset: 1,
                    last_offset: 2,
                }],
                ..entry(3, 1, 2)
            }],
        };
        let bytes = rec.to_bytes().unwrap();
        assert!(WalFlushRecord::from_bytes(&bytes).unwrap() == rec);
    }

    #[test]
    fn earliest_covered_is_smallest_first_offset() {
        let mut c = WalIndexCache::default();
        c.apply(&WalFlushRecord {
            object_key: "o2".into(),
            format_version: WAL_INDEX_FORMAT_VERSION,
            entries: vec![entry(0, 5, 9)],
        });
        c.apply(&WalFlushRecord {
            object_key: "o1".into(),
            format_version: WAL_INDEX_FORMAT_VERSION,
            entries: vec![entry(0, 0, 4)],
        });

        assert!(c.earliest_covered(Uuid::from_u128(1), 0) == Some(0));
        assert!(c.earliest_covered(Uuid::from_u128(1), 1).is_none());
    }

    #[test]
    fn aborted_txn_overlap_uses_half_open_query_range() {
        let txn = WalAbortedTxn {
            producer_id: 7,
            first_offset: 5,
            last_offset: 8,
        };

        assert!(txn.overlaps(0, 6));
        assert!(txn.overlaps(8, 9));
        assert!(!txn.overlaps(0, 5));
        assert!(!txn.overlaps(9, 10));
        assert!(!txn.overlaps(5, 5));
    }
}
