//! Diskless WAL object-store flusher.

#![allow(dead_code)]

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use crabka_log::{Log, Offset};
use crabka_units::{ByteSize, mebibytes};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use super::{
    index_log::DisklessIndexLog,
    wal_index::{WalFlushRecord, WalIndexCache, WalIndexEntry},
    wal_object::WalObjectBuilder,
};

pub(crate) const FLUSH_INTERVAL: Duration = Duration::from_millis(250);
pub(crate) const FLUSH_MAX_SIZE: ByteSize = mebibytes(8);
pub(crate) const DEFAULT_TRIM_SAFETY_LAG: i64 = 1;

#[derive(Debug, Clone)]
pub(crate) struct FlushConfig {
    pub(crate) interval: Duration,
    pub(crate) max_size: ByteSize,
    pub(crate) trim_safety_lag: Option<i64>,
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            interval: FLUSH_INTERVAL,
            max_size: FLUSH_MAX_SIZE,
            trim_safety_lag: Some(DEFAULT_TRIM_SAFETY_LAG),
        }
    }
}

pub(crate) struct FlushPartition {
    pub(crate) topic_id: Uuid,
    pub(crate) partition: i32,
    pub(crate) log: Arc<Mutex<Log>>,
    pub(crate) high_watermark: Offset,
}

pub(crate) async fn flush_once(
    object_store: Arc<dyn ObjectStore>,
    index_log: &DisklessIndexLog,
    cache: Arc<AsyncMutex<WalIndexCache>>,
    partitions: &[FlushPartition],
    config: &FlushConfig,
) -> Result<Option<WalFlushRecord>, crate::error::BrokerError> {
    let mut builder = WalObjectBuilder::new();
    for partition in partitions {
        let start = cache
            .lock()
            .await
            .flushed_frontier(partition.topic_id, partition.partition)
            .unwrap_or(0);
        let raw = partition
            .log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .read_raw(Offset(start), partition.high_watermark, config.max_size)
            .map_err(crate::error::BrokerError::from)?;
        let Some(last_offset) = raw.last_offset else {
            continue;
        };
        builder.append_run(
            partition.topic_id,
            partition.partition,
            raw.start_offset.0,
            last_offset.0,
            &raw.bytes,
        );
    }

    if builder.is_empty() {
        return Ok(None);
    }
    let object_key = format!("diskless-wal/{}.ckwl", Uuid::new_v4());
    let object = builder.finish();
    object_store
        .put(
            &Path::from(object_key.clone()),
            PutPayload::from(object.clone()),
        )
        .await
        .map_err(|error| crate::error::BrokerError::Txn(format!("diskless wal put: {error}")))?;

    let entries = super::wal_object::parse_wal_object(&object)
        .map_err(|error| crate::error::BrokerError::Txn(error.to_string()))?
        .into_iter()
        .map(|entry| WalIndexEntry {
            topic_id: entry.topic_id,
            partition: entry.partition,
            first_offset: entry.first_offset,
            last_offset: entry.last_offset,
            byte_start: entry.byte_start,
            byte_len: entry.byte_len,
        })
        .collect();
    let record = WalFlushRecord {
        object_key,
        format_version: 1,
        entries,
    };
    index_log.publish_flush(&record).await?;
    wait_for_committed_projection(cache.clone(), &record).await?;

    if let Some(lag) = config.trim_safety_lag {
        for partition in partitions {
            if let Some(frontier) = cache
                .lock()
                .await
                .flushed_frontier(partition.topic_id, partition.partition)
            {
                let hw_trim_floor = partition.high_watermark.0.saturating_sub(lag);
                let trim_to = frontier.min(hw_trim_floor);
                if trim_to > 0 {
                    partition
                        .log
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .trim_to_offset(Offset(trim_to))
                        .map_err(crate::error::BrokerError::from)?;
                }
            }
        }
    }

    Ok(Some(record))
}

async fn wait_for_committed_projection(
    cache: Arc<AsyncMutex<WalIndexCache>>,
    record: &WalFlushRecord,
) -> Result<(), crate::error::BrokerError> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            {
                let cache = cache.lock().await;
                if record.entries.iter().all(|entry| {
                    cache
                        .flushed_frontier(entry.topic_id, entry.partition)
                        .is_some_and(|frontier| frontier > entry.last_offset)
                }) {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| crate::error::BrokerError::Txn("diskless wal index projection timed out".into()))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_log::LogConfig;
    use crabka_protocol::records::{Attributes, Record, RecordBatch};
    use object_store::memory::InMemory;
    use tempfile::tempdir;

    use super::*;
    use crate::diskless::index_log::DisklessIndexLog;

    fn batch(count: i32) -> RecordBatch {
        RecordBatch {
            base_offset: 0,
            partition_leader_epoch: 0,
            attributes: Attributes::default(),
            last_offset_delta: count - 1,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: (0..count)
                .map(|i| Record {
                    attributes: 0,
                    offset_delta: i,
                    timestamp_delta: 0,
                    key: None,
                    value: Some(bytes::Bytes::from_static(b"v")),
                    headers: vec![],
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn flusher_writes_object_and_publishes_index() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&mut batch(3)).unwrap();
        let log = Arc::new(Mutex::new(log));
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let event_log = crabka_remote_storage_topic::InProcessMetadataEventLog::new(1);
        let index = DisklessIndexLog::start(event_log);
        let topic_id = Uuid::from_u128(11);
        let cache = index.cache();
        let record = flush_once(
            store.clone(),
            &index,
            cache.clone(),
            &[FlushPartition {
                topic_id,
                partition: 0,
                log: log.clone(),
                high_watermark: Offset(3),
            }],
            &FlushConfig::default(),
        )
        .await
        .unwrap()
        .unwrap();

        assert!(record.entries[0].first_offset == 0);
        assert!(record.entries[0].last_offset == 2);
        assert!(cache.lock().await.flushed_frontier(topic_id, 0) == Some(3));
        assert!(store.head(&Path::from(record.object_key)).await.is_ok());
        assert!(log.lock().unwrap().log_start_offset() == Offset(2));
    }

    #[tokio::test]
    async fn default_config_enables_safe_trim_lag() {
        assert!(FlushConfig::default().trim_safety_lag == Some(DEFAULT_TRIM_SAFETY_LAG));
    }
}
