//! Per-broker log-compaction ticker. Every `interval`, walks the
//! partitions registry and dispatches [`Partition::compact_log`] for
//! every partition where:
//!
//!   - the topic's `cleanup.policy` is `compact`, and
//!   - this broker is currently the leader.
//!
//! The actual compaction runs on the partition's writer actor, so
//! appends and compaction are serialized.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crabka_metadata::NodeId;

use crate::partition::Partition;
use crate::partition_registry::PartitionRegistry;

/// Tunables for [`run`].
#[derive(Debug, Clone)]
pub(crate) struct CleanerConfig {
    pub interval: Duration,
}

impl Default for CleanerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
        }
    }
}

/// Spawned task entry point.
pub(crate) async fn run(
    partitions: Arc<PartitionRegistry>,
    node_id: NodeId,
    cfg: CleanerConfig,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(cfg.interval);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            () = shutdown.cancelled() => {
                debug!("cleaner task shutting down");
                return;
            }
        }
        tick_all(&partitions, node_id).await;
    }
}

pub(crate) async fn tick_all(partitions: &PartitionRegistry, node_id: NodeId) {
    // Snapshot first to avoid holding any registry guard across await.
    let snapshot: Vec<Arc<Partition>> = partitions.arcs();
    for partition in snapshot {
        let leader = partition.current_leader.load(Ordering::Relaxed);
        if leader != node_id {
            continue;
        }
        let policy = {
            // Recover the guard if the mutex was poisoned by a panic
            // elsewhere rather than killing the (discarded-JoinHandle)
            // cleaner task. The config snapshot stays readable.
            let log = partition
                .log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            log.config_snapshot().cleanup_policy
        };
        if policy != crabka_log::CleanupPolicy::Compact {
            continue;
        }
        if let Err(e) = partition.compact_log().await {
            warn!(
                topic = %partition.topic,
                partition_id = partition.partition_id,
                error = %e,
                "compaction failed for partition",
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use bytes::Bytes;
    use crabka_protocol::records::{Record, RecordBatch};
    use std::sync::atomic::Ordering;
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;

    fn keyed_batch(base: i64, key: &[u8], value: &[u8]) -> RecordBatch {
        RecordBatch {
            base_offset: base,
            records: vec![Record {
                offset_delta: 0,
                key: Some(Bytes::copy_from_slice(key)),
                value: Some(Bytes::copy_from_slice(value)),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn compactable_partition(
        root: &TempDir,
        topic: &str,
        partition_id: i32,
        leader: NodeId,
        cleanup_policy: crabka_log::CleanupPolicy,
    ) -> Arc<Partition> {
        let part_dir = crate::log_dir::partition_dir(root.path(), topic, partition_id);
        std::fs::create_dir_all(&part_dir).expect("create partition dir");
        let cfg = crabka_log::LogConfig {
            cleanup_policy,
            segment_bytes: 256,
            ..Default::default()
        };
        let mut log = crabka_log::Log::open(&part_dir, cfg).expect("open compactable log");
        for idx in 0..12 {
            let mut batch = keyed_batch(idx, b"duplicate-key", format!("v{idx}").as_bytes());
            log.append(&mut batch).expect("append duplicate-key batch");
        }
        let mut active = keyed_batch(12, b"active-key", b"active");
        log.append(&mut active).expect("append active batch");

        let part = crate::broker::spawn_partition(
            topic.to_string(),
            partition_id,
            root.path().to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
        );
        part.current_leader.store(leader, Ordering::Relaxed);
        part
    }

    fn record_count(partition: &Partition) -> usize {
        let read = partition
            .log
            .lock()
            .expect("partition log lock")
            .read(0, 1 << 20)
            .expect("read partition log");
        read.batches.iter().map(|batch| batch.records.len()).sum()
    }

    #[tokio::test]
    async fn tick_all_compacts_only_local_leader_compact_topics() {
        let dir = tempfile::tempdir().expect("log root");
        let registry = PartitionRegistry::new();
        let local_compact = compactable_partition(
            &dir,
            "local-compact",
            0,
            7,
            crabka_log::CleanupPolicy::Compact,
        );
        let follower_compact = compactable_partition(
            &dir,
            "follower-compact",
            0,
            8,
            crabka_log::CleanupPolicy::Compact,
        );
        let local_delete = compactable_partition(
            &dir,
            "local-delete",
            0,
            7,
            crabka_log::CleanupPolicy::Delete,
        );

        let local_compact_before = record_count(&local_compact);
        let follower_compact_before = record_count(&follower_compact);
        let local_delete_before = record_count(&local_delete);
        registry.insert("local-compact".to_string(), 0, Arc::clone(&local_compact));
        registry.insert(
            "follower-compact".to_string(),
            0,
            Arc::clone(&follower_compact),
        );
        registry.insert("local-delete".to_string(), 0, Arc::clone(&local_delete));

        tick_all(&registry, 7).await;

        assert!(record_count(&local_compact) < local_compact_before);
        assert!(record_count(&follower_compact) == follower_compact_before);
        assert!(record_count(&local_delete) == local_delete_before);
    }

    #[tokio::test]
    async fn run_ticks_until_shutdown() {
        let dir = tempfile::tempdir().expect("log root");
        let registry = Arc::new(PartitionRegistry::new());
        let partition = compactable_partition(
            &dir,
            "run-compact",
            0,
            7,
            crabka_log::CleanupPolicy::Compact,
        );
        let before = record_count(&partition);
        registry.insert("run-compact".to_string(), 0, Arc::clone(&partition));
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            Arc::clone(&registry),
            7,
            CleanerConfig {
                interval: Duration::from_millis(20),
            },
            shutdown.clone(),
        ));

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            if record_count(&partition) < before {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "cleaner run loop did not compact eligible partition"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        shutdown.cancel();
        task.await.expect("cleaner task exits");
    }
}
