//! In-memory cache for quorum-committed diskless WAL tail batches.

use std::{collections::BTreeMap, sync::Mutex};

use bytes::Bytes;
use crabka_ids::PartitionIndex;
use crabka_protocol::records::RecordBatch;
use dashmap::DashMap;
use uuid::Uuid;

/// Advisory cache of recently quorum-committed diskless WAL batches.
#[derive(Debug)]
pub(crate) struct HotTailCache {
    max_batches_per_partition: usize,
    entries: DashMap<(Uuid, i32), Mutex<BTreeMap<i64, HotTailEntry>>>,
}

impl Default for HotTailCache {
    fn default() -> Self {
        Self::new(256)
    }
}

impl HotTailCache {
    #[must_use]
    pub(crate) fn new(max_batches_per_partition: usize) -> Self {
        Self {
            max_batches_per_partition,
            entries: DashMap::new(),
        }
    }

    pub(crate) fn insert_run(&self, topic_id: Uuid, partition: PartitionIndex, bytes: &Bytes) {
        let mut offset = 0usize;
        while offset < bytes.len() {
            let mut cur = bytes.slice(offset..);
            let Ok(batch) = RecordBatch::decode(&mut cur) else {
                return;
            };
            let len = batch.encoded_len();
            if len == 0 || offset + len > bytes.len() {
                return;
            }
            self.insert_batch(
                topic_id,
                partition,
                bytes.slice(offset..offset + len),
                &batch,
            );
            offset += len;
        }
    }

    pub(crate) fn get(
        &self,
        topic_id: Uuid,
        partition: PartitionIndex,
        fetch_offset: i64,
        max_bytes: usize,
    ) -> Option<Bytes> {
        let map = self.entries.get(&(topic_id, partition.0))?;
        let map = map
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_base, entry) = map.range(..=fetch_offset).next_back()?;
        if fetch_offset <= entry.last_offset && entry.bytes.len() <= max_bytes {
            Some(entry.bytes.clone())
        } else {
            None
        }
    }

    fn insert_batch(
        &self,
        topic_id: Uuid,
        partition: PartitionIndex,
        bytes: Bytes,
        batch: &RecordBatch,
    ) {
        let base_offset = batch.base_offset;
        let last_offset = base_offset + i64::from(batch.last_offset_delta);
        let entry = self
            .entries
            .entry((topic_id, partition.0))
            .or_insert_with(|| Mutex::new(BTreeMap::new()));
        let mut batches = entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        batches.insert(base_offset, HotTailEntry { last_offset, bytes });
        while batches.len() > self.max_batches_per_partition {
            let Some(first) = batches.keys().next().copied() else {
                break;
            };
            batches.remove(&first);
        }
    }
}

#[derive(Debug, Clone)]
struct HotTailEntry {
    last_offset: i64,
    bytes: Bytes,
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use crabka_protocol::records::Record;

    use super::*;

    #[test]
    fn hot_tail_cache_floor_lookup_and_bound() {
        let cache = HotTailCache::new(1);
        let topic_id = Uuid::from_u128(7);
        let first = batch_bytes(0, 1);
        let second = batch_bytes(2, 2);

        cache.insert_run(topic_id, PartitionIndex(0), &first);
        cache.insert_run(topic_id, PartitionIndex(0), &second);

        assert!(
            cache
                .get(topic_id, PartitionIndex(0), 0, usize::MAX)
                .is_none()
        );
        assert!(cache.get(topic_id, PartitionIndex(0), 2, usize::MAX) == Some(second));
        assert!(
            cache
                .get(topic_id, PartitionIndex(0), 3, usize::MAX)
                .is_some()
        );
        assert!(cache.get(topic_id, PartitionIndex(0), 3, 1).is_none());
    }

    fn batch_bytes(base_offset: i64, records: i32) -> Bytes {
        let mut batch = RecordBatch {
            base_offset,
            last_offset_delta: records - 1,
            ..RecordBatch::default()
        };
        for offset_delta in 0..records {
            batch.records.push(Record {
                offset_delta,
                ..Record::default()
            });
        }
        let mut buf = BytesMut::new();
        batch.encode(&mut buf).unwrap();
        buf.freeze()
    }
}
