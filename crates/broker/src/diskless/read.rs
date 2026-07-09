//! Diskless WAL cold-read path from flushed object-store runs.

use std::sync::Arc;

use bytes::Bytes;
use crabka_protocol::{
    owned::fetch_response::AbortedTransaction,
    records::{RecordBatch, RecordsPayload},
};
use object_store::{GetOptions, GetRange, ObjectStore, path::Path};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use super::wal_index::{WalAbortedTxn, WalIndexCache};
use crate::{broker::Broker, codes, handlers::fetch::PendingRead, partition::Partition};

#[derive(Debug, Clone)]
struct IndexedRun {
    bytes: Bytes,
    aborted_transactions: Vec<WalAbortedTxn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BatchWindow {
    bytes: Bytes,
    last_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DisklessReadRecords {
    records: Bytes,
    aborted_transactions: Vec<AbortedTransaction>,
}

/// Shared state for serving diskless offsets that were trimmed locally but
/// covered by the committed WAL object index.
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

    #[must_use]
    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    async fn read_run(&self, topic_id: Uuid, partition: i32, offset: i64) -> Option<IndexedRun> {
        let (object_key, entry) = self
            .index
            .lock()
            .await
            .lookup(topic_id, partition, offset)?;
        let range_end = entry.byte_start.checked_add(u64::from(entry.byte_len))?;
        let bytes = self
            .store
            .get_opts(
                &Path::from(object_key),
                GetOptions {
                    range: Some(GetRange::Bounded(entry.byte_start..range_end)),
                    ..Default::default()
                },
            )
            .await
            .ok()?
            .bytes()
            .await
            .ok()?;
        Some(IndexedRun {
            bytes,
            aborted_transactions: entry.aborted_transactions,
        })
    }

    async fn read_records(
        &self,
        topic_id: Uuid,
        partition: i32,
        offset: i64,
        max_bytes: usize,
        limit_offset: i64,
    ) -> Option<DisklessReadRecords> {
        let run = self.read_run(topic_id, partition, offset).await?;
        let window = bounded_batch_window_at_or_after(&run.bytes, offset, limit_offset, max_bytes)?;
        let aborted_transactions = aborted_transactions_for_range(
            &run.aborted_transactions,
            offset,
            limit_offset.min(window.last_offset.saturating_add(1)),
        );
        Some(DisklessReadRecords {
            records: window.bytes,
            aborted_transactions,
        })
    }
}

/// Try to satisfy a local `OFFSET_OUT_OF_RANGE` fetch from diskless WAL objects.
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
    let read = handle
        .read_records(
            topic_id,
            p.partition_index,
            p.fetch_offset,
            usize::try_from(p.max_bytes.max(0)).unwrap_or(0),
            diskless_limit_offset(p),
        )
        .await?;
    let bytes_est = read.records.len();
    p.out.error_code = codes::NONE;
    if p.read_committed && !p.is_follower_fetch {
        p.out.aborted_transactions = Some(read.aborted_transactions);
    }
    p.out.records = Some(RecordsPayload::Raw(read.records));
    Some(bytes_est)
}

fn diskless_limit_offset(p: &PendingRead) -> i64 {
    if p.read_committed || p.is_follower_fetch {
        return p.out.last_stable_offset;
    }
    p.out.high_watermark
}

#[cfg(test)]
fn bounded_batch_bytes_at_or_after(run: &Bytes, floor: i64, max_bytes: usize) -> Option<Bytes> {
    bounded_batch_window_at_or_after(run, floor, i64::MAX, max_bytes).map(|window| window.bytes)
}

fn bounded_batch_window_at_or_after(
    run: &Bytes,
    floor: i64,
    limit_offset: i64,
    max_bytes: usize,
) -> Option<BatchWindow> {
    if max_bytes == 0 {
        return None;
    }
    if floor >= limit_offset {
        return None;
    }

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
            return batch_window_at(
                run,
                offset,
                batch.base_offset,
                last_offset,
                encoded_len,
                limit_offset,
                max_bytes,
            );
        }
        offset = offset.checked_add(encoded_len)?;
    }
    None
}

fn batch_window_at(
    run: &Bytes,
    start: usize,
    first_base_offset: i64,
    first_last_offset: i64,
    first_batch_len: usize,
    limit_offset: i64,
    max_bytes: usize,
) -> Option<BatchWindow> {
    if first_batch_len == 0 {
        return None;
    }
    if first_base_offset >= limit_offset {
        return None;
    }

    let mut end = start.checked_add(first_batch_len)?;
    let mut last_offset = first_last_offset;
    while end < run.len() {
        let slice = run.slice(end..);
        let mut cur: &[u8] = &slice;
        let Ok(batch) = RecordBatch::decode(&mut cur) else {
            return None;
        };
        let encoded_len = batch.encoded_len();
        if encoded_len == 0 {
            return None;
        }
        if batch.base_offset >= limit_offset {
            break;
        }
        let next_end = end.checked_add(encoded_len)?;
        if next_end.checked_sub(start)? > max_bytes {
            break;
        }
        last_offset = batch.base_offset + i64::from(batch.last_offset_delta);
        end = next_end;
    }
    Some(BatchWindow {
        bytes: run.slice(start..end),
        last_offset,
    })
}

fn aborted_transactions_for_range(
    aborted_transactions: &[WalAbortedTxn],
    first_offset: i64,
    last_offset_exclusive: i64,
) -> Vec<AbortedTransaction> {
    aborted_transactions
        .iter()
        .copied()
        .filter(|txn| txn.overlaps(first_offset, last_offset_exclusive))
        .map(|txn| AbortedTransaction {
            producer_id: txn.producer_id,
            first_offset: txn.first_offset,
            ..Default::default()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::BytesMut;
    use crabka_protocol::records::{Attributes, Record, RecordBatch};
    use object_store::{ObjectStoreExt, PutPayload, path::Path};

    use super::{
        super::wal_index::{WAL_INDEX_FORMAT_VERSION, WalFlushRecord, WalIndexEntry},
        *,
    };

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

    fn index_entry(
        topic_id: Uuid,
        first_offset: i64,
        last_offset: i64,
        byte_start: u64,
        byte_len: usize,
        aborted_transactions: Vec<WalAbortedTxn>,
    ) -> WalIndexEntry {
        WalIndexEntry {
            topic_id,
            partition: 0,
            first_offset,
            last_offset,
            byte_start,
            byte_len: u32::try_from(byte_len).unwrap(),
            aborted_transactions,
        }
    }

    #[test]
    fn cold_read_returns_byte_exact_covering_batch() {
        let first = batch(0, b"a");
        let second = batch(1, b"b");
        let run = encode_batches(&[first.clone(), second.clone()]);
        let mut expected = BytesMut::new();
        second.encode(&mut expected).unwrap();

        let got = bounded_batch_bytes_at_or_after(&run, 1, usize::MAX).unwrap();

        assert!(got == expected.freeze());
    }

    #[test]
    fn cold_read_miss_leaves_out_of_range() {
        let run = encode_batches(&[batch(0, b"a")]);

        assert!(bounded_batch_bytes_at_or_after(&run, 5, usize::MAX).is_none());
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

        let got = bounded_batch_bytes_at_or_after(&run, 11, usize::MAX).unwrap();

        assert!(got == run);
    }

    #[test]
    fn read_window_stops_before_read_committed_limit() {
        let first = batch(0, b"a");
        let second = batch(1, b"b");
        let run = encode_batches(&[first.clone(), second]);
        let mut expected = BytesMut::new();
        first.encode(&mut expected).unwrap();

        let got = bounded_batch_window_at_or_after(&run, 0, 1, usize::MAX).unwrap();

        assert!(got.bytes == expected.freeze());
        assert!(got.last_offset == 0);
    }

    #[test]
    fn abort_metadata_filters_to_returned_read_committed_window() {
        let aborted_transactions = vec![
            WalAbortedTxn {
                producer_id: 1,
                first_offset: 0,
                last_offset: 1,
            },
            WalAbortedTxn {
                producer_id: 2,
                first_offset: 5,
                last_offset: 6,
            },
        ];

        let got = aborted_transactions_for_range(&aborted_transactions, 4, 7);

        assert!(
            got == vec![AbortedTransaction {
                producer_id: 2,
                first_offset: 5,
                ..Default::default()
            }]
        );
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
        cache.apply(&WalFlushRecord {
            object_key: "diskless-wal/o".into(),
            format_version: WAL_INDEX_FORMAT_VERSION,
            entries: vec![index_entry(
                topic_id,
                1,
                1,
                byte_start,
                second.len(),
                Vec::new(),
            )],
        });
        let handle = DisklessReadHandle::new(Arc::new(AsyncMutex::new(cache)), store);

        let got = handle
            .read_records(topic_id, 0, 1, usize::MAX, i64::MAX)
            .await
            .unwrap();

        assert!(got.records == second);
        assert!(got.aborted_transactions.is_empty());
    }

    #[tokio::test]
    async fn indexed_object_read_preserves_aborted_transaction_metadata() {
        let topic_id = Uuid::from_u128(8);
        let run = encode_batches(&[RecordBatch {
            base_offset: 0,
            attributes: Attributes::default().with_transactional(true),
            producer_id: 42,
            producer_epoch: 0,
            base_sequence: 0,
            records: vec![Record {
                value: Some(Bytes::from_static(b"aborted")),
                ..Default::default()
            }],
            ..Default::default()
        }]);

        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        store
            .put(
                &Path::from("diskless-wal/txn"),
                PutPayload::from(run.clone()),
            )
            .await
            .unwrap();

        let mut cache = WalIndexCache::default();
        cache.apply(&WalFlushRecord {
            object_key: "diskless-wal/txn".into(),
            format_version: WAL_INDEX_FORMAT_VERSION,
            entries: vec![index_entry(
                topic_id,
                0,
                0,
                0,
                run.len(),
                vec![WalAbortedTxn {
                    producer_id: 42,
                    first_offset: 0,
                    last_offset: 1,
                }],
            )],
        });
        let handle = DisklessReadHandle::new(Arc::new(AsyncMutex::new(cache)), store);

        let got = handle
            .read_records(topic_id, 0, 0, usize::MAX, 1)
            .await
            .unwrap();

        assert!(got.records == run);
        assert!(
            got.aborted_transactions
                == vec![AbortedTransaction {
                    producer_id: 42,
                    first_offset: 0,
                    ..Default::default()
                }]
        );
    }
}
