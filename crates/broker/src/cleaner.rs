//! Per-broker log-compaction ticker. Every `interval`, walks the
//! partitions registry and dispatches [`Partition::compact_log`] for
//! every partition where:
//!
//!   - the topic's `cleanup.policy` is `compact`, and
//!   - this broker is currently the leader.
//!
//! The actual compaction runs on the partition's writer actor, so
//! appends and compaction are serialized.

use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use crabka_metadata::NodeId;
use qubit_clock::sleep::{AsyncSleeper, SystemSleeper};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{metrics::BrokerMetrics, partition::Partition, partition_registry::PartitionRegistry};

/// Default cadence of the broker-wide compaction sweep.
const DEFAULT_COMPACTION_INTERVAL: Duration = Duration::from_secs(30);

/// Tunables for [`run`].
#[derive(Clone)]
pub(crate) struct CleanerConfig {
    pub interval: Duration,
    /// Relative sleeper driving the compaction-sweep cadence. Production uses
    /// [`qubit_clock::sleep::SystemSleeper`] (real time); tests inject a
    /// [`qubit_clock::sleep::MockSleeper`] so the sweep interval fires on a
    /// controlled mock timeline instead of wall-clock time.
    pub sleeper: Arc<dyn AsyncSleeper>,
}

impl Default for CleanerConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_COMPACTION_INTERVAL,
            sleeper: Arc::new(SystemSleeper::new()),
        }
    }
}

/// Spawned task entry point.
pub(crate) async fn run(
    partitions: Arc<PartitionRegistry>,
    node_id: NodeId,
    cfg: CleanerConfig,
    shutdown: CancellationToken,
    metrics: BrokerMetrics,
) {
    // Drive the sweep cadence through the injected `AsyncSleeper` (production:
    // real time; tests: a controlled mock timeline). A zero-duration first sleep
    // reproduces `tokio::time::interval`'s immediate t=0 tick, so the first sweep
    // fires at startup; each subsequent sleep is re-armed to `cfg.interval` only
    // after the sweep completes (`MissedTickBehavior::Delay` semantics — a slow
    // sweep never triggers a catch-up burst). The sleeper is cloned into a local
    // so the tick future borrows it rather than `cfg`, leaving `cfg` free.
    let sleeper = cfg.sleeper.clone();
    let mut tick = sleeper.sleep_for_async(Duration::ZERO);
    loop {
        tokio::select! {
            () = &mut tick => {
                tick_all(&partitions, node_id, &metrics).await;
                tick = sleeper.sleep_for_async(cfg.interval);
            }
            () = shutdown.cancelled() => {
                debug!("cleaner task shutting down");
                return;
            }
        }
    }
}

pub(crate) async fn tick_all(
    partitions: &PartitionRegistry,
    node_id: NodeId,
    metrics: &BrokerMetrics,
) {
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
        match partition.compact_log().await {
            Ok(()) => {
                metrics.record_compaction(&partition.topic, partition.partition_id.get());
            }
            Err(e) => {
                warn!(
                    topic = %partition.topic,
                    partition_id = partition.partition_id.get(),
                    error = %e,
                    "compaction failed for partition",
                );
            }
        }
    }
    // One increment per completed sweep, whether or not any partition was
    // eligible, so a test that seals a segment can poll this counter to
    // confirm a full pass ran after the seal (see `wait_for_metrics`).
    metrics.record_cleaner_run();
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use assert2::assert;
    use bytes::Bytes;
    use crabka_ids::PartitionIndex;
    use crabka_protocol::records::{Record, RecordBatch};
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;

    use super::*;

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
            PartitionIndex(partition_id),
            root.path().to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
        );
        part.current_leader.store(leader.0, Ordering::Relaxed);
        part
    }

    fn record_count(partition: &Partition) -> usize {
        let read = partition
            .log
            .lock()
            .expect("partition log lock")
            .read(crabka_log::Offset(0), 1 << 20)
            .expect("read partition log");
        read.batches.iter().map(|batch| batch.records.len()).sum()
    }

    #[tokio::test]
    async fn tick_all_compacts_only_local_leader_compact_topics() {
        let dir = tempfile::tempdir().expect("log root");
        let registry = PartitionRegistry::new();
        // (topic, leader, cleanup_policy, expect_compacted): only partitions
        // led locally (leader 7) with the Compact policy should shrink.
        let specs = [
            ("local-compact", 7, crabka_log::CleanupPolicy::Compact, true),
            (
                "follower-compact",
                8,
                crabka_log::CleanupPolicy::Compact,
                false,
            ),
            ("local-delete", 7, crabka_log::CleanupPolicy::Delete, false),
        ];
        let cases: Vec<_> = specs
            .into_iter()
            .map(|(topic, leader, policy, expect_compacted)| {
                let partition = compactable_partition(&dir, topic, 0, NodeId(leader), policy);
                let before = record_count(&partition);
                registry.insert(topic.to_string(), PartitionIndex(0), Arc::clone(&partition));
                (topic, partition, before, expect_compacted)
            })
            .collect();

        let metrics = BrokerMetrics::new();
        tick_all(&registry, NodeId(7), &metrics).await;

        // A single `tick_all` is exactly one cleaner sweep, so the run counter
        // must advance by one. This pins `record_cleaner_run` against a no-op
        // mutation (nothing else asserts on `log_cleaner_runs_total`).
        assert_eq!(metrics.log_cleaner_runs_total.get(), 1);

        for (topic, partition, before, expect_compacted) in cases {
            let after = record_count(&partition);
            let count_ok = if expect_compacted {
                after < before
            } else {
                after == before
            };
            assert!(
                count_ok,
                "case: {topic} (before={before}, after={after}, expect_compacted={expect_compacted})"
            );
        }
    }

    #[tokio::test]
    async fn run_ticks_until_shutdown() {
        use qubit_clock::{MockWaiterKind, sleep::MockSleeper};

        let dir = tempfile::tempdir().expect("log root");
        let registry = Arc::new(PartitionRegistry::new());
        let partition = compactable_partition(
            &dir,
            "run-compact",
            0,
            NodeId(7),
            crabka_log::CleanupPolicy::Compact,
        );
        let before = record_count(&partition);
        registry.insert(
            "run-compact".to_string(),
            PartitionIndex(0),
            Arc::clone(&partition),
        );

        // Drive the sweep cadence on a mock timeline instead of wall-clock time.
        let interval = Duration::from_secs(30);
        let sleeper = MockSleeper::new();
        let timeline = sleeper.timeline();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            Arc::clone(&registry),
            NodeId(7),
            CleanerConfig {
                interval,
                sleeper: Arc::new(sleeper),
            },
            shutdown.clone(),
            BrokerMetrics::new(),
        ));

        // The immediate t=0 tick runs a compaction sweep, then the loop re-arms
        // on `sleep_for_async(interval)`. Block (bounded real time, hang-guard
        // only) until that interval-sleep waiter is parked — it registers
        // strictly after the first sweep's `tick_all` returns, so the compaction
        // is fully applied by then. `wait_for_blocked_waiters` runs on a blocking
        // thread so it never stalls the current-thread runtime that must drive
        // the cleaner task and the partition writer actor to completion.
        let tl = timeline.clone();
        let parked = tokio::task::spawn_blocking(move || {
            tl.wait_for_blocked_waiters(MockWaiterKind::Sleep, 1, Duration::from_secs(5))
        })
        .await
        .unwrap();
        assert!(
            parked,
            "cleaner should park on the interval sleep after the first sweep"
        );
        assert!(
            record_count(&partition) < before,
            "immediate first sweep should compact the eligible partition"
        );

        // Advance one interval to fire a second sweep, then confirm the loop
        // re-parks — proving it keeps ticking on the injected cadence with no
        // wall-clock time (the second sweep is idempotent, so the log stays
        // compacted rather than shrinking further).
        timeline.advance(interval);
        let tl = timeline.clone();
        let parked_again = tokio::task::spawn_blocking(move || {
            tl.wait_for_blocked_waiters(MockWaiterKind::Sleep, 1, Duration::from_secs(5))
        })
        .await
        .unwrap();
        assert!(
            parked_again,
            "cleaner should re-park on the interval sleep after the second sweep"
        );
        assert!(record_count(&partition) < before, "log stays compacted");

        shutdown.cancel();
        task.await.expect("cleaner task exits");
    }
}
