//! Diskless WAL object-store flusher.

#[cfg(any(test, feature = "test-helpers"))]
use std::collections::HashMap;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use crabka_log::Offset;
use crabka_metadata::{MetadataImage, NodeId};
use crabka_units::{
    ByteSize,
    convert::{ByteSizeExt as _, TimeExt as _},
};
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    index_log::DisklessIndexLog,
    wal_index::{WalFlushRecord, WalIndexCache, WalIndexEntry},
    wal_object::WalObjectBuilder,
};
use crate::{
    config::{
        DEFAULT_DISKLESS_WAL_FLUSH_INTERVAL, DEFAULT_DISKLESS_WAL_FLUSH_MAX_SIZE,
        DEFAULT_DISKLESS_WAL_INDEX_PROJECTION_TIMEOUT, DEFAULT_DISKLESS_WAL_TRIM_SAFETY_LAG,
    },
    partition::Partition,
    partition_registry::PartitionRegistry,
};

#[cfg(any(test, feature = "test-helpers"))]
static PUT_FAILURES: std::sync::LazyLock<std::sync::Mutex<HashMap<i32, u64>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

#[cfg(any(test, feature = "test-helpers"))]
#[must_use]
pub(crate) fn put_failure_count(broker_id: i32) -> u64 {
    *PUT_FAILURES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&broker_id)
        .unwrap_or(&0)
}

#[derive(Debug, Clone)]
pub(crate) struct FlushConfig {
    pub(crate) interval: Duration,
    pub(crate) max_size: ByteSize,
    pub(crate) trim_safety_lag: Option<i64>,
    pub(crate) index_projection_timeout: Duration,
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_DISKLESS_WAL_FLUSH_INTERVAL.to_std(),
            max_size: DEFAULT_DISKLESS_WAL_FLUSH_MAX_SIZE,
            trim_safety_lag: Some(DEFAULT_DISKLESS_WAL_TRIM_SAFETY_LAG),
            index_projection_timeout: DEFAULT_DISKLESS_WAL_INDEX_PROJECTION_TIMEOUT.to_std(),
        }
    }
}

impl FlushConfig {
    pub(crate) fn from_broker(config: &crate::config::BrokerConfig) -> Self {
        Self {
            interval: config.diskless_wal_flush_interval.to_std(),
            max_size: config.diskless_wal_flush_max_size,
            trim_safety_lag: Some(config.diskless_wal_trim_safety_lag),
            index_projection_timeout: config.diskless_wal_index_projection_timeout.to_std(),
        }
    }
}

pub(crate) struct FlushPartition {
    pub(crate) topic_id: Uuid,
    pub(crate) handle: Arc<Partition>,
    pub(crate) high_watermark: Offset,
}

/// Dependencies owned by the broker's diskless flush task.
pub(crate) struct FlusherContext {
    pub(crate) partitions: Arc<PartitionRegistry>,
    pub(crate) image_rx: tokio::sync::watch::Receiver<Arc<MetadataImage>>,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) index_log: DisklessIndexLog,
    pub(crate) node_id: NodeId,
    pub(crate) broker_id: i32,
}

/// Flush committed tails until broker shutdown. A failed tick does not move
/// the durable index frontier, so the next tick safely retries the same tail.
pub(crate) async fn run(context: FlusherContext, config: FlushConfig, shutdown: CancellationToken) {
    let mut ticker = tokio::time::interval(config.interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut rotation = 0usize;
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            () = shutdown.cancelled() => {
                tracing::debug!("diskless WAL flusher shutting down");
                return;
            }
        }
        if let Err(error) = flush_tick(&context, &config, rotation).await {
            tracing::warn!(%error, "diskless WAL flush failed; retrying");
        }
        rotation = rotation.wrapping_add(1);
    }
}

async fn flush_tick(
    context: &FlusherContext,
    config: &FlushConfig,
    rotation: usize,
) -> Result<Option<WalFlushRecord>, crate::error::BrokerError> {
    let image = context.image_rx.borrow().clone();
    let mut partitions = flushable_partitions(&context.partitions, &image, context.node_id).await;
    if !partitions.is_empty() {
        let start = rotation % partitions.len();
        partitions.rotate_left(start);
    }
    flush_once(
        Arc::clone(&context.object_store),
        context.broker_id,
        &context.index_log,
        context.index_log.cache(),
        &partitions,
        config,
    )
    .await
}

async fn flushable_partitions(
    registry: &PartitionRegistry,
    image: &MetadataImage,
    node_id: NodeId,
) -> Vec<FlushPartition> {
    // Snapshot registry handles before awaiting any partition state.
    let mut out = Vec::new();
    for handle in registry.arcs() {
        if !handle.diskless || handle.current_leader.load(Ordering::Relaxed) != node_id {
            continue;
        }
        let Some(topic_id) = image.topic(&handle.topic).map(|topic| topic.topic_id) else {
            continue;
        };
        let high_watermark = handle.high_watermark().await;
        out.push(FlushPartition {
            topic_id,
            handle,
            high_watermark,
        });
    }
    out.sort_unstable_by(|left, right| {
        left.handle
            .topic
            .cmp(&right.handle.topic)
            .then_with(|| left.handle.index.cmp(&right.handle.index))
    });
    out
}

pub(crate) async fn flush_once(
    object_store: Arc<dyn ObjectStore>,
    broker_id: i32,
    index_log: &DisklessIndexLog,
    cache: Arc<AsyncMutex<WalIndexCache>>,
    partitions: &[FlushPartition],
    config: &FlushConfig,
) -> Result<Option<WalFlushRecord>, crate::error::BrokerError> {
    let mut builder = WalObjectBuilder::new();
    for partition in partitions {
        let remaining = config
            .max_size
            .bytes_usize()
            .saturating_sub(builder.body_len());
        if remaining == 0 {
            break;
        }
        let start = cache
            .lock()
            .await
            .flushed_frontier(partition.topic_id, partition.handle.index.get());
        let raw = {
            let log = partition
                .handle
                .log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let start = start.unwrap_or_else(|| log.log_start_offset().0);
            log.read_raw(
                Offset(start),
                partition.high_watermark,
                ByteSize::from_bytes(u64::try_from(remaining).unwrap_or(u64::MAX)),
            )
            .map_err(crate::error::BrokerError::from)?
        };
        let Some(last_offset) = raw.last_offset else {
            continue;
        };
        builder.append_run(
            partition.topic_id,
            partition.handle.index.get(),
            raw.start_offset.0,
            last_offset.0,
            &raw.bytes,
        );
    }

    if builder.is_empty() {
        return Ok(None);
    }
    let object_key = format!("diskless-wal/{broker_id}/{}.ckwl", Uuid::new_v4());
    let object = builder.finish();
    if let Err(error) = object_store
        .put(
            &Path::from(object_key.clone()),
            PutPayload::from(object.clone()),
        )
        .await
    {
        #[cfg(any(test, feature = "test-helpers"))]
        {
            let mut failures = PUT_FAILURES
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *failures.entry(broker_id).or_default() += 1;
        }
        return Err(crate::error::BrokerError::Txn(format!(
            "diskless wal put: {error}"
        )));
    }

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
    wait_for_committed_projection(cache.clone(), &record, config.index_projection_timeout).await?;

    if let Some(lag) = config.trim_safety_lag {
        for partition in partitions {
            if let Some(frontier) = cache
                .lock()
                .await
                .flushed_frontier(partition.topic_id, partition.handle.index.get())
            {
                let hw_trim_floor = partition.high_watermark.0.saturating_sub(lag.max(0));
                let trim_to = frontier.min(hw_trim_floor);
                let current_start = partition
                    .handle
                    .log
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .log_start_offset()
                    .0;
                if trim_to > current_start {
                    partition.handle.trim_to_offset(Offset(trim_to)).await?;
                }
            }
        }
    }

    Ok(Some(record))
}

async fn wait_for_committed_projection(
    cache: Arc<AsyncMutex<WalIndexCache>>,
    record: &WalFlushRecord,
    timeout: Duration,
) -> Result<(), crate::error::BrokerError> {
    tokio::time::timeout(timeout, async {
        loop {
            {
                let cache = cache.lock().await;
                if cache.contains_record(record) {
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
    use std::path::Path as FsPath;

    use assert2::assert;
    use crabka_log::{Log, LogConfig};
    use crabka_metadata::{MetadataImage, MetadataRecord, TopicRecord};
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

    fn test_partition(
        root: &FsPath,
        topic: &str,
        partition: i32,
        diskless: bool,
        leader: NodeId,
    ) -> Arc<Partition> {
        let partition_dir = root.join(format!("{topic}-{partition}"));
        std::fs::create_dir_all(&partition_dir).unwrap();
        let mut log = Log::open(&partition_dir, LogConfig::default()).unwrap();
        log.append(&mut batch(3)).unwrap();
        let handle = crate::broker::spawn_partition(
            topic.to_owned(),
            crabka_ids::PartitionIndex(partition),
            root.to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
            diskless,
        );
        handle.current_leader.store(leader.0, Ordering::Relaxed);
        handle
    }

    #[tokio::test]
    async fn flusher_writes_object_and_publishes_index() {
        let dir = tempdir().unwrap();
        let handle = test_partition(dir.path(), "orders", 0, true, NodeId(1));
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let event_log = crabka_remote_storage_topic::InProcessMetadataEventLog::new(1);
        let index = DisklessIndexLog::start(event_log);
        let topic_id = Uuid::from_u128(11);
        let cache = index.cache();
        let record = flush_once(
            store.clone(),
            7,
            &index,
            cache.clone(),
            &[FlushPartition {
                topic_id,
                handle: Arc::clone(&handle),
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
        assert!(handle.log.lock().unwrap().log_start_offset() == Offset(2));
    }

    #[tokio::test]
    async fn flusher_skips_noop_trim_when_writer_is_stopped() {
        let dir = tempdir().unwrap();
        let handle = test_partition(dir.path(), "orders", 0, true, NodeId(1));
        let writer = handle
            .writer_handle
            .lock()
            .unwrap()
            .take()
            .expect("partition writer");
        writer.abort();
        assert!(writer.await.unwrap_err().is_cancelled());

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let index = DisklessIndexLog::start(
            crabka_remote_storage_topic::InProcessMetadataEventLog::new(1),
        );
        let cache = index.cache();
        let topic_id = Uuid::from_u128(11);
        let record = flush_once(
            store,
            7,
            &index,
            cache,
            &[FlushPartition {
                topic_id,
                handle: Arc::clone(&handle),
                high_watermark: Offset(3),
            }],
            &FlushConfig {
                trim_safety_lag: Some(3),
                ..FlushConfig::default()
            },
        )
        .await
        .expect("a no-op trim must not depend on the partition writer")
        .expect("the durable prefix is flushed");

        assert!(record.entries[0].last_offset == 2);
        assert!(handle.log.lock().unwrap().log_start_offset() == Offset(0));
    }

    #[tokio::test]
    async fn committed_projection_wait_requires_the_record_to_be_applied() {
        let cache = Arc::new(AsyncMutex::new(WalIndexCache::default()));
        let record = WalFlushRecord {
            object_key: "diskless-wal/test.ckwl".into(),
            format_version: 1,
            entries: vec![WalIndexEntry {
                topic_id: Uuid::from_u128(11),
                partition: 0,
                first_offset: 0,
                last_offset: 2,
                byte_start: 0,
                byte_len: 1,
            }],
        };

        let error =
            wait_for_committed_projection(Arc::clone(&cache), &record, Duration::from_millis(10))
                .await
                .expect_err("an unapplied record must time out");
        assert!(error.to_string().contains("projection timed out"));

        cache.lock().await.apply(&record);
        wait_for_committed_projection(cache, &record, Duration::from_secs(1))
            .await
            .expect("the exact applied record is visible");
    }

    #[tokio::test]
    async fn combined_object_stops_after_size_budget() {
        let dir = tempdir().unwrap();
        let first = test_partition(dir.path(), "orders", 0, true, NodeId(1));
        let second = test_partition(dir.path(), "orders", 1, true, NodeId(1));
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let index = DisklessIndexLog::start(
            crabka_remote_storage_topic::InProcessMetadataEventLog::new(1),
        );
        let cache = index.cache();
        let config = FlushConfig {
            max_size: ByteSize::from_bytes(1),
            trim_safety_lag: None,
            ..FlushConfig::default()
        };

        let record = flush_once(
            store,
            7,
            &index,
            cache,
            &[
                FlushPartition {
                    topic_id: Uuid::from_u128(11),
                    handle: first,
                    high_watermark: Offset(3),
                },
                FlushPartition {
                    topic_id: Uuid::from_u128(11),
                    handle: second,
                    high_watermark: Offset(3),
                },
            ],
            &config,
        )
        .await
        .unwrap()
        .unwrap();

        assert!(record.entries.len() == 1);
        assert!(record.entries[0].partition == 0);
    }

    #[tokio::test]
    async fn tick_rotates_size_limited_flush_start() {
        let dir = tempdir().unwrap();
        let first = test_partition(dir.path(), "orders", 0, true, NodeId(1));
        let second = test_partition(dir.path(), "orders", 1, true, NodeId(1));
        let partitions = Arc::new(PartitionRegistry::new());
        partitions.insert("orders".into(), crabka_ids::PartitionIndex(0), first);
        partitions.insert("orders".into(), crabka_ids::PartitionIndex(1), second);

        let topic_id = Uuid::from_u128(11);
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id,
            partitions: 2,
            replication_factor: 1,
        }));
        let (_, image_rx) = tokio::sync::watch::channel(Arc::new(image));
        let index = DisklessIndexLog::start(
            crabka_remote_storage_topic::InProcessMetadataEventLog::new(1),
        );
        let cache = index.cache();
        let context = FlusherContext {
            partitions,
            image_rx,
            object_store: Arc::new(InMemory::new()),
            index_log: index,
            node_id: NodeId(1),
            broker_id: 7,
        };

        flush_tick(
            &context,
            &FlushConfig {
                max_size: ByteSize::from_bytes(1),
                trim_safety_lag: None,
                ..FlushConfig::default()
            },
            1,
        )
        .await
        .unwrap();

        let cache = cache.lock().await;
        assert!(cache.flushed_frontier(topic_id, 0).is_none());
        assert!(cache.flushed_frontier(topic_id, 1) == Some(3));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_rotates_size_limited_flushes_without_starvation() {
        let dir = tempdir().unwrap();
        let first = test_partition(dir.path(), "orders", 0, true, NodeId(1));
        let second = test_partition(dir.path(), "orders", 1, true, NodeId(1));
        let partitions = Arc::new(PartitionRegistry::new());
        partitions.insert("orders".into(), crabka_ids::PartitionIndex(0), first);
        partitions.insert("orders".into(), crabka_ids::PartitionIndex(1), second);

        let topic_id = Uuid::from_u128(11);
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id,
            partitions: 2,
            replication_factor: 1,
        }));
        let (_, image_rx) = tokio::sync::watch::channel(Arc::new(image));
        let index = DisklessIndexLog::start(
            crabka_remote_storage_topic::InProcessMetadataEventLog::new(1),
        );
        let cache = index.cache();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            FlusherContext {
                partitions,
                image_rx,
                object_store: Arc::new(InMemory::new()),
                index_log: index,
                node_id: NodeId(1),
                broker_id: 7,
            },
            FlushConfig {
                interval: Duration::from_millis(1),
                max_size: ByteSize::from_bytes(1),
                trim_safety_lag: None,
                ..FlushConfig::default()
            },
            shutdown.clone(),
        ));

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let cache = cache.lock().await;
                if cache.flushed_frontier(topic_id, 0) == Some(3)
                    && cache.flushed_frontier(topic_id, 1) == Some(3)
                {
                    break;
                }
                drop(cache);
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        shutdown.cancel();
        task.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_flushes_only_led_diskless_partitions_and_stops() {
        let dir = tempdir().unwrap();
        let led = test_partition(dir.path(), "orders", 0, true, NodeId(1));
        let follower = test_partition(dir.path(), "orders", 1, true, NodeId(2));
        let local = test_partition(dir.path(), "orders", 2, false, NodeId(1));
        let partitions = Arc::new(PartitionRegistry::new());
        partitions.insert("orders".into(), crabka_ids::PartitionIndex(0), led);
        partitions.insert("orders".into(), crabka_ids::PartitionIndex(1), follower);
        partitions.insert("orders".into(), crabka_ids::PartitionIndex(2), local);

        let topic_id = Uuid::from_u128(11);
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id,
            partitions: 3,
            replication_factor: 1,
        }));
        let (_, image_rx) = tokio::sync::watch::channel(Arc::new(image));
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let index = DisklessIndexLog::start(
            crabka_remote_storage_topic::InProcessMetadataEventLog::new(1),
        );
        let cache = index.cache();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            FlusherContext {
                partitions,
                image_rx,
                object_store: Arc::clone(&store),
                index_log: index,
                node_id: NodeId(1),
                broker_id: 7,
            },
            FlushConfig {
                interval: Duration::from_millis(1),
                trim_safety_lag: None,
                ..FlushConfig::default()
            },
            shutdown.clone(),
        ));

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if cache.lock().await.flushed_frontier(topic_id, 0) == Some(3) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        shutdown.cancel();
        task.await.unwrap();

        assert!(cache.lock().await.flushed_frontier(topic_id, 1).is_none());
        assert!(cache.lock().await.flushed_frontier(topic_id, 2).is_none());
        let object_key = cache.lock().await.lookup(topic_id, 0, 0).unwrap().0;
        assert!(object_key.starts_with("diskless-wal/7/"));
        assert!(store.head(&Path::from(object_key)).await.is_ok());
    }

    #[tokio::test]
    async fn default_config_enables_safe_trim_lag() {
        let config = FlushConfig::default();
        assert!(config.trim_safety_lag == Some(DEFAULT_DISKLESS_WAL_TRIM_SAFETY_LAG));
        assert!(
            config.index_projection_timeout
                == DEFAULT_DISKLESS_WAL_INDEX_PROJECTION_TIMEOUT.to_std()
        );
    }

    #[test]
    fn broker_config_controls_every_flusher_policy() {
        let broker = crate::config::BrokerConfig {
            diskless_wal_flush_interval: crabka_units::millis(125),
            diskless_wal_flush_max_size: crabka_units::mebibytes(4),
            diskless_wal_trim_safety_lag: 0,
            diskless_wal_index_projection_timeout: crabka_units::secs(3),
            ..crate::config::BrokerConfig::default()
        };

        let config = FlushConfig::from_broker(&broker);

        assert!(config.interval == Duration::from_millis(125));
        assert!(config.max_size == crabka_units::mebibytes(4));
        assert!(config.trim_safety_lag == Some(0));
        assert!(config.index_projection_timeout == Duration::from_secs(3));
    }
}
