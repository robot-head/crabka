//! Anomaly detector — slice 43g.
//!
//! The detector watches `SharedSnapshot` + `UsageStore` for trouble
//! (broker death, sustained under-replicated partitions, disk pressure,
//! slow broker) and auto-triggers self-healing proposals via the
//! existing optimizer path. Anomaly history is persisted to
//! `{data_dir}/anomalies.json` and surfaced via `GetAnomalies`.

pub mod anomaly;
pub mod metrics;
pub mod rules;
pub mod store;

pub use anomaly::{Anomaly, AnomalyKey, AnomalyKind, AnomalySeverity};
pub use metrics::DetectorMetrics;
pub use store::{AnomalyStore, StoreError};

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::capacity::BrokerCapacities;
use crate::executor::ExecutorState;
use crate::ingest::SharedSnapshot;
use crate::model::{ClusterState, ProposalStore};
use crate::scraper::UsageStore;

#[derive(Debug, Clone)]
pub struct DetectorConfig {
    pub tick_interval: Duration,
    pub broker_death_threshold: Duration,
    pub under_replicated_threshold: Duration,
    pub disk_pressure_pct: f64,
    pub disk_critical_pct: f64,
    pub slow_broker_multiplier: f64,
    pub slow_broker_min_cores: f64,
    pub default_mute_window: Duration,
    pub auto_trigger_enabled: bool,
    pub history_capacity: usize,
}

impl Default for DetectorConfig {
    fn default() -> Self {
        Self {
            tick_interval: Duration::from_secs(30),
            broker_death_threshold: Duration::from_mins(1),
            under_replicated_threshold: Duration::from_mins(2),
            disk_pressure_pct: 0.85,
            disk_critical_pct: 0.95,
            slow_broker_multiplier: 2.0,
            slow_broker_min_cores: 0.5,
            default_mute_window: Duration::from_mins(15),
            auto_trigger_enabled: false,
            history_capacity: 10,
        }
    }
}

/// Compact snapshot fingerprint used by rules that need to verify a
/// condition is sustained across multiple ticks. NOT persisted —
/// restart resets sustained-condition timers, briefly delaying
/// re-detection. Acceptable trade-off; anomalies are derived signals.
#[derive(Debug, Clone)]
pub struct SnapshotMemo {
    pub snapshot_at_ms: i64,
    pub broker_ids: Vec<i32>,
    /// (topic, partition) → (`replicas.len()`, `isr.len()`)
    pub partition_isr: std::collections::HashMap<(String, i32), (usize, usize)>,
}

#[derive(Debug, Default)]
pub struct SnapshotHistory {
    inner: VecDeque<SnapshotMemo>,
    capacity: usize,
}

impl SnapshotHistory {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: VecDeque::with_capacity(capacity.max(1)),
            capacity: capacity.max(1),
        }
    }

    pub fn push(&mut self, state: &ClusterState) {
        let memo = SnapshotMemo {
            snapshot_at_ms: state.snapshot_at_ms,
            broker_ids: state.brokers.iter().map(|b| b.id).collect(),
            partition_isr: state
                .partitions
                .iter()
                .map(|p| {
                    (
                        (p.topic.clone(), p.partition),
                        (p.replicas.len(), p.isr.len()),
                    )
                })
                .collect(),
        };
        if self.inner.len() == self.capacity {
            self.inner.pop_front();
        }
        self.inner.push_back(memo);
    }

    /// Returns the oldest memo with `snapshot_at_ms >= cutoff_ms`, or
    /// the oldest memo we have. Rules use this to ask "has condition X
    /// held since `cutoff_ms`?".
    #[must_use]
    pub fn oldest_since(&self, cutoff_ms: i64) -> Option<&SnapshotMemo> {
        self.inner.iter().find(|m| m.snapshot_at_ms >= cutoff_ms)
    }

    #[must_use]
    pub fn iter_recent(&self) -> impl DoubleEndedIterator<Item = &SnapshotMemo> {
        self.inner.iter()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[allow(dead_code)] // Fields wired in T9.
pub struct Detector {
    cfg: DetectorConfig,
    snapshot: SharedSnapshot,
    usage_store: Arc<UsageStore>,
    capacities: Arc<BrokerCapacities>,
    anomaly_store: Arc<AnomalyStore>,
    proposal_store: Arc<ProposalStore>,
    executor_state: ExecutorState,
    goal_registry: Arc<crate::api::GoalRegistry>,
    goal_ctx: crate::goals::GoalContext,
    shutdown: CancellationToken,
    history: AsyncMutex<SnapshotHistory>,
}

impl Detector {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        cfg: DetectorConfig,
        snapshot: SharedSnapshot,
        usage_store: Arc<UsageStore>,
        capacities: Arc<BrokerCapacities>,
        anomaly_store: Arc<AnomalyStore>,
        proposal_store: Arc<ProposalStore>,
        executor_state: ExecutorState,
        goal_registry: Arc<crate::api::GoalRegistry>,
        goal_ctx: crate::goals::GoalContext,
        shutdown: CancellationToken,
    ) -> Self {
        let history_capacity = cfg.history_capacity;
        Self {
            cfg,
            snapshot,
            usage_store,
            capacities,
            anomaly_store,
            proposal_store,
            executor_state,
            goal_registry,
            goal_ctx,
            shutdown,
            history: AsyncMutex::new(SnapshotHistory::new(history_capacity)),
        }
    }

    pub async fn run(self) {
        info!(
            tick_secs = self.cfg.tick_interval.as_secs(),
            auto_trigger = self.cfg.auto_trigger_enabled,
            "detector starting"
        );
        let mut ticker = tokio::time::interval(self.cfg.tick_interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                () = self.shutdown.cancelled() => {
                    info!("detector shutting down");
                    return;
                }
            }
            self.tick_once(now_ms()).await;
        }
    }

    /// One detector pass over the current snapshot. T9 wires this to the
    /// real rule evaluators + `auto_trigger`; this stub records history
    /// only and is intentionally inert.
    pub async fn tick_once(&self, now_ms: i64) {
        let g = self.snapshot.load();
        let Some(state) = (*g).as_ref() else {
            debug!("detector tick: no snapshot yet");
            return;
        };
        {
            let mut hist = self.history.lock().await;
            hist.push(state);
        }
        let _ = (state, now_ms);
        let _ = evaluate_all();
    }
}

fn evaluate_all() -> Vec<()> {
    Vec::new()
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{BrokerView, PartitionView};

    fn make_state(snapshot_at_ms: i64, broker_ids: &[i32]) -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms,
            brokers: broker_ids
                .iter()
                .map(|&id| BrokerView {
                    id,
                    host: format!("h{id}"),
                    port: 9092,
                    rack: None,
                })
                .collect(),
            partitions: vec![PartitionView {
                topic: "t".into(),
                partition: 0,
                replicas: vec![1, 2],
                leader: 1,
                isr: vec![1, 2],
            }],
            in_flight_reassignments: vec![],
        }
    }

    #[test]
    fn snapshot_history_evicts_when_full() {
        let mut h = SnapshotHistory::new(2);
        h.push(&make_state(10, &[1, 2]));
        h.push(&make_state(20, &[1, 2]));
        h.push(&make_state(30, &[1, 2, 3]));
        assert_eq!(h.len(), 2);
        let oldest = h.iter_recent().next().expect("at least one memo");
        assert_eq!(oldest.snapshot_at_ms, 20);
    }

    #[test]
    fn snapshot_history_oldest_since_returns_oldest_within_cutoff() {
        let mut h = SnapshotHistory::new(4);
        h.push(&make_state(10, &[1]));
        h.push(&make_state(20, &[1]));
        h.push(&make_state(30, &[1]));
        let got = h.oldest_since(15).expect("memo at 20 satisfies cutoff");
        assert_eq!(got.snapshot_at_ms, 20);
        assert!(h.oldest_since(1000).is_none());
    }
}
