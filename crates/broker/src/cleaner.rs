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

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crabka_metadata::NodeId;

use crate::partition::Partition;

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
    partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
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

pub(crate) async fn tick_all(partitions: &DashMap<(String, i32), Arc<Partition>>, node_id: NodeId) {
    // Snapshot first to avoid holding the DashMap iter across await.
    let snapshot: Vec<Arc<Partition>> = partitions.iter().map(|kv| kv.value().clone()).collect();
    for partition in snapshot {
        let leader = partition.current_leader.load(Ordering::Relaxed);
        if leader != node_id {
            continue;
        }
        let policy = {
            let log = partition.log.lock().expect("log mutex poisoned");
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
