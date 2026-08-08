//! Diskless WAL cold-read path from flushed object-store runs.

use std::sync::Arc;

use bytes::Bytes;
use crabka_protocol::records::{RecordBatch, RecordsPayload};
use object_store::{GetOptions, GetRange, ObjectStore, path::Path};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use super::wal_index::WalIndexCache;
use crate::{broker::Broker, codes, handlers::fetch::PendingRead, partition::Partition};

/// Shared state that serves diskless offsets. The broker trimmed these offsets
/// locally, but the committed WAL object index still covers them.
pub(crate) struct DisklessReadHandle {
    pub(crate) index: Arc<AsyncMutex<WalIndexCache>>,
    store: Arc<dyn ObjectStore>,
}

impl DisklessReadHandle {
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn new(index: Arc<AsyncMutex<WalIndexCache>>, store: Arc<dyn ObjectStore>) -> Self {
        Self { index, store }
    }

    async fn read_run(&self, topic_id: Uuid, partition: i32, offset: i64) -> Option<Bytes> {
        let (object_key, byte_start, byte_len) = self
            .index
            .lock()
            .await
            .lookup(topic_id, partition, offset)?;
        let range_end = byte_start.checked_add(u64::from(byte_len))?;
        self.store
            .get_opts(
                &Path::from(object_key),
                GetOptions {
                    range: Some(GetRange::Bounded(byte_start..range_end)),
                    ..Default::default()
                },
            )
            .await
            .ok()?
            .bytes()
            .await
            .ok()
    }

    async fn read_records(&self, topic_id: Uuid, partition: i32, offset: i64) -> Option<Bytes> {
        let run = self.read_run(topic_id, partition, offset).await?;
        first_batch_bytes_at_or_after(&run, offset)
    }
}

/// Tries to satisfy a local `OFFSET_OUT_OF_RANGE` fetch from the diskless WAL
/// objects.
pub(crate) async fn try_diskless_read(
    broker: &Broker,
    p: &mut PendingRead,
    part: &Partition,
) -> Option<usize> {
    if !part.diskless || p.topic_id == crabka_protocol::primitives::uuid::Uuid::ZERO {
        return None;
    }
    let remote_storage_enable = {
        let log = part.log.lock().expect("log mutex poisoned");
        log.config_snapshot().remote_storage_enable
    };
    if remote_storage_enable {
        return None;
    }

    let handle = broker.diskless_read.clone()?;
    let topic_id = Uuid::from_bytes(p.topic_id.0);
    let records = handle
        .read_records(topic_id, p.partition_index, p.fetch_offset)
        .await?;
    let bytes_est = records.len();
    p.out.error_code = codes::NONE;
    if p.read_committed && !p.is_follower_fetch {
        p.out.aborted_transactions = Some(Vec::new());
    }
    p.out.records = Some(RecordsPayload::Raw(records));
    Some(bytes_est)
}

fn first_batch_bytes_at_or_after(run: &Bytes, floor: i64) -> Option<Bytes> {
    let mut offset = 0;
    while offset < run.len() {
        let slice = run.slice(offset..);
        let mut cur: &[u8] = &slice;
        let Ok(batch) = RecordBatch::decode(&mut cur) else {
            return None;
        };
        let encoded_len = batch.encoded_len();
        let last_offset = batch.base_offset + i64::from(batch.last_offset_delta);
        if last_offset >= floor {
            return Some(run.slice(offset..));
        }
        offset = offset.checked_add(encoded_len)?;
    }
    None
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::BytesMut;
    use crabka_protocol::records::{Attributes, Record, RecordBatch};
    use object_store::{ObjectStoreExt, PutPayload, path::Path};

    use super::*;

    fn batch(base_offset: i64, value: &'static [u8]) -> RecordBatch {
        RecordBatch {
            base_offset,
            partition_leader_epoch: 0,
            attributes: Attributes::default(),
            last_offset_delta: 0,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: vec![Record {
                attributes: 0,
                offset_delta: 0,
                timestamp_delta: 0,
                key: None,
                value: Some(Bytes::from_static(value)),
                headers: vec![],
            }],
        }
    }

    fn encode_batches(batches: &[RecordBatch]) -> Bytes {
        let mut bytes = BytesMut::new();
        for batch in batches {
            batch.encode(&mut bytes).unwrap();
        }
        bytes.freeze()
    }

    #[test]
    fn cold_read_returns_byte_exact_covering_batch() {
        let first = batch(0, b"a");
        let second = batch(1, b"b");
        let run = encode_batches(&[first.clone(), second.clone()]);
        let mut expected = BytesMut::new();
        second.encode(&mut expected).unwrap();

        let got = first_batch_bytes_at_or_after(&run, 1).unwrap();

        assert!(got == expected.freeze());
    }

    #[test]
    fn cold_read_miss_leaves_out_of_range() {
        let run = encode_batches(&[batch(0, b"a")]);

        assert!(first_batch_bytes_at_or_after(&run, 5).is_none());
    }

    #[test]
    fn mid_batch_positioning_returns_covering_batch_boundary() {
        let run = encode_batches(&[RecordBatch {
            base_offset: 10,
            last_offset_delta: 2,
            records: (0..3)
                .map(|offset_delta| Record {
                    offset_delta,
                    value: Some(Bytes::from_static(b"v")),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }]);

        let got = first_batch_bytes_at_or_after(&run, 11).unwrap();

        assert!(got == run);
    }

    #[tokio::test]
    async fn indexed_object_range_read_returns_byte_exact_covering_batch() {
        let topic_id = Uuid::from_u128(7);
        let first = encode_batches(&[batch(0, b"a")]);
        let second = encode_batches(&[batch(1, b"b")]);
        let mut object = BytesMut::new();
        object.extend_from_slice(&first);
        let byte_start = u64::try_from(object.len()).unwrap();
        object.extend_from_slice(&second);

        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        store
            .put(
                &Path::from("diskless-wal/o"),
                PutPayload::from(object.freeze()),
            )
            .await
            .unwrap();

        let mut cache = WalIndexCache::default();
        cache.apply(&super::super::wal_index::WalFlushRecord {
            object_key: "diskless-wal/o".into(),
            format_version: 1,
            entries: vec![super::super::wal_index::WalIndexEntry {
                topic_id,
                partition: 0,
                first_offset: 1,
                last_offset: 1,
                byte_start,
                byte_len: u32::try_from(second.len()).unwrap(),
            }],
        });
        let handle = DisklessReadHandle::new(Arc::new(AsyncMutex::new(cache)), store);

        let got = handle.read_records(topic_id, 0, 1).await.unwrap();

        assert!(got == second);
    }
}
