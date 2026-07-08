//! Projection of committed diskless WAL index events.

#![allow(dead_code)]

use std::sync::Arc;

use crabka_remote_storage_topic::{MetadataEventLog, PartitionStart};
use futures_util::StreamExt;
use tokio::sync::Mutex;

use super::wal_index::{WalFlushRecord, WalIndexCache};

pub(crate) const DISKLESS_WAL_INDEX_TOPIC: &str = "__diskless_wal_index";

#[derive(Clone)]
pub(crate) struct DisklessIndexLog {
    log: Arc<dyn MetadataEventLog>,
    cache: Arc<Mutex<WalIndexCache>>,
}

impl DisklessIndexLog {
    #[must_use]
    pub(crate) fn start(log: Arc<dyn MetadataEventLog>) -> Self {
        let cache = Arc::new(Mutex::new(WalIndexCache::default()));
        Self::start_with_cache(log, cache)
    }

    #[must_use]
    pub(crate) fn start_with_cache(
        log: Arc<dyn MetadataEventLog>,
        cache: Arc<Mutex<WalIndexCache>>,
    ) -> Self {
        let starts = (0..log.partition_count())
            .map(|partition| PartitionStart {
                partition,
                start_offset: 0,
            })
            .collect();
        let (mut stream, _assignment) = log.subscribe(starts);
        let pump_cache = cache.clone();
        tokio::spawn(async move {
            while let Some(event) = stream.next().await {
                if let Ok(record) = WalFlushRecord::from_bytes(&event.payload) {
                    pump_cache.lock().await.apply(&record);
                }
            }
        });
        Self { log, cache }
    }

    #[must_use]
    pub(crate) fn cache(&self) -> Arc<Mutex<WalIndexCache>> {
        self.cache.clone()
    }

    pub(crate) async fn publish_flush(
        &self,
        record: &WalFlushRecord,
    ) -> Result<i64, crate::error::BrokerError> {
        let bytes = record.to_bytes().map_err(crate::error::BrokerError::Txn)?;
        self.log
            .publish(
                index_partition(&record.object_key, self.log.partition_count()),
                bytes,
            )
            .await
            .map_err(|error| {
                crate::error::BrokerError::Txn(format!("diskless index publish: {error}"))
            })
    }
}

fn index_partition(key: &str, partitions: i32) -> i32 {
    let hash = key.bytes().fold(0u32, |acc, byte| {
        acc.wrapping_mul(31).wrapping_add(u32::from(byte))
    });
    i32::try_from(hash % u32::try_from(partitions).expect("positive partition count"))
        .expect("index partition fits i32")
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_remote_storage_topic::InProcessMetadataEventLog;
    use tokio::time::{Duration, timeout};
    use uuid::Uuid;

    use super::*;
    use crate::diskless::wal_index::WalIndexEntry;

    #[tokio::test]
    async fn index_log_projects_published_flush_records() {
        let event_log = InProcessMetadataEventLog::new(1);
        let index = DisklessIndexLog::start(event_log);
        let topic_id = Uuid::from_u128(7);
        let record = WalFlushRecord {
            object_key: "object-a".into(),
            format_version: 1,
            entries: vec![WalIndexEntry {
                topic_id,
                partition: 0,
                first_offset: 0,
                last_offset: 3,
                byte_start: 6,
                byte_len: 10,
            }],
        };

        index.publish_flush(&record).await.unwrap();
        timeout(Duration::from_secs(1), async {
            loop {
                if index.cache().lock().await.flushed_frontier(topic_id, 0) == Some(4) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(index.cache().lock().await.lookup(topic_id, 0, 2).is_some());
    }
}
