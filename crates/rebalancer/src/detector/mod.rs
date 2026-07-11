//! Anomaly detector.
//!
//! The detector watches `SharedSnapshot` + `UsageStore` for trouble
//! (broker death, sustained under-replicated partitions, disk pressure,
//! slow broker) and auto-triggers self-healing proposals via the
//! existing optimizer path. Anomaly history is persisted to
//! `{data_dir}/anomalies.json` and surfaced via `GetAnomalies`.

pub mod anomaly;
pub mod auto_trigger;
pub mod metrics;
pub mod rules;
pub mod store;

use std::{collections::VecDeque, sync::Arc, time::Duration};

pub use anomaly::{Anomaly, AnomalyKey, AnomalyKind, AnomalySeverity};
pub use auto_trigger::{AutoTriggerError, goals_for_kind, maybe_trigger};
pub use metrics::DetectorMetrics;
pub use store::{AnomalyStore, StoreError};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    capacity::BrokerCapacities,
    executor::ExecutorState,
    ingest::SharedSnapshot,
    model::{ClusterState, ProposalStore},
    scraper::UsageStore,
    time::now_ms,
};

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
    metrics: DetectorMetrics,
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
        metrics: DetectorMetrics,
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
            metrics,
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

    /// One detector pass over the current snapshot. Pushes the current
    /// snapshot to history, runs all four rules, reconciles open
    /// anomalies (mark resolved + upsert new), updates per-kind open
    /// gauges, and fires auto-trigger on freshly-detected anomalies.
    pub async fn tick_once(&self, now_ms: i64) {
        use std::collections::HashMap;

        use crate::detector::rules::{
            BrokerDeath, DiskPressure, Rule, RuleCtx, RuleHit, SlowBroker,
            UnderReplicatedPartitions,
        };

        let g = self.snapshot.load();
        let Some(state) = (*g).as_ref() else {
            debug!("detector tick: no snapshot yet");
            return;
        };
        {
            let mut hist = self.history.lock().await;
            hist.push(state);
        }

        let rules: Vec<(AnomalyKind, Box<dyn Rule>)> = vec![
            (AnomalyKind::BrokerDeath, Box::new(BrokerDeath)),
            (
                AnomalyKind::UnderReplicatedPartitions,
                Box::new(UnderReplicatedPartitions),
            ),
            (AnomalyKind::DiskPressure, Box::new(DiskPressure)),
            (AnomalyKind::SlowBroker, Box::new(SlowBroker)),
        ];

        // Evaluate all rules under one lock; release before any awaits.
        let mut all_hits: HashMap<AnomalyKind, Vec<RuleHit>> = HashMap::new();
        {
            let history_guard = self.history.lock().await;
            for (kind, rule) in &rules {
                let ctx = RuleCtx {
                    snapshot: state,
                    history: &history_guard,
                    usages: &self.usage_store,
                    capacities: &self.capacities,
                    now_ms,
                    cfg: &self.cfg,
                };
                all_hits.insert(*kind, rule.evaluate(&ctx));
            }
        }

        // Reconcile each kind's hits against open anomalies; collect
        // newly-detected anomalies for auto-trigger.
        let mut to_trigger: Vec<Anomaly> = Vec::new();
        for (kind, hits) in &all_hits {
            let active_keys: std::collections::HashSet<_> =
                hits.iter().map(|h| h.key.clone()).collect();

            // 1. Mark as resolved any open (kind, key) not in this tick's hits.
            for open in self.anomaly_store.list(0, false) {
                if open.kind == *kind
                    && !active_keys.contains(&open.key)
                    && self.anomaly_store.mark_resolved(*kind, &open.key, now_ms)
                {
                    self.metrics.record_resolved(*kind);
                }
            }

            // 2. Upsert each active hit; new records are candidates for auto-trigger.
            for hit in hits {
                let (id, is_new) = self.anomaly_store.upsert_open(
                    *kind,
                    hit.key.clone(),
                    hit.severity,
                    hit.details.clone(),
                    now_ms,
                );
                if is_new {
                    self.metrics.record_detected(*kind);
                    if let Some(a) = self.anomaly_store.get(&id) {
                        to_trigger.push(a);
                    }
                }
            }
        }

        // 3. Update open-count gauges (per kind).
        let all_open = self.anomaly_store.list(0, false);
        for kind in [
            AnomalyKind::BrokerDeath,
            AnomalyKind::UnderReplicatedPartitions,
            AnomalyKind::DiskPressure,
            AnomalyKind::SlowBroker,
        ] {
            let n = all_open.iter().filter(|a| a.kind == kind).count();
            self.metrics
                .set_open_count(kind, i64::try_from(n).unwrap_or(i64::MAX));
        }

        // 4. Auto-trigger on each newly-detected anomaly. Errors are logged
        //    and swallowed — the tick loop continues.
        for a in to_trigger {
            let ctx = crate::detector::auto_trigger::AutoTriggerCtx {
                snapshot: self.snapshot.clone(),
                goal_registry: &self.goal_registry,
                goal_ctx: &self.goal_ctx,
                proposal_store: &self.proposal_store,
                anomaly_store: &self.anomaly_store,
                executor_state: &self.executor_state,
                config: &self.cfg,
                metrics: &self.metrics,
                now_ms,
            };
            if let Err(e) = crate::detector::auto_trigger::maybe_trigger(&a, &ctx).await {
                warn!(anomaly_id = %a.id, error = %e, "auto_trigger errored");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::check;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{
        capacity::BrokerCapacities,
        executor::{ExecutorConfig, ExecutorState},
        goals::GoalContext,
        model::{BrokerView, PartitionView, ProposalStore},
        scraper::UsageStore,
    };

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

    fn make_under_replicated_state(snapshot_at_ms: i64, is_under: bool) -> ClusterState {
        let isr = if is_under { vec![1] } else { vec![1, 2] };
        ClusterState {
            cluster_id: None,
            snapshot_at_ms,
            brokers: vec![
                BrokerView {
                    id: 1,
                    host: "h1".into(),
                    port: 9092,
                    rack: None,
                },
                BrokerView {
                    id: 2,
                    host: "h2".into(),
                    port: 9092,
                    rack: None,
                },
            ],
            partitions: vec![PartitionView {
                topic: "t".into(),
                partition: 0,
                replicas: vec![1, 2],
                leader: 1,
                isr,
            }],
            in_flight_reassignments: vec![],
        }
    }

    fn detector_with(
        cfg: DetectorConfig,
        snapshot: SharedSnapshot,
        anomaly_store: Arc<AnomalyStore>,
        proposal_store: Arc<ProposalStore>,
        metrics: DetectorMetrics,
        shutdown: CancellationToken,
    ) -> Detector {
        let usage_store = Arc::new(UsageStore::default());
        let capacities = Arc::new(BrokerCapacities::default());
        let goal_ctx = GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
            broker_capacities: capacities.clone(),
            broker_usages: usage_store.clone(),
        };
        let executor_state = ExecutorState {
            store: proposal_store.clone(),
            config: ExecutorConfig {
                data_dir: std::path::PathBuf::new(),
                default_throttle_bytes_per_sec: 50_000_000,
                poll_interval: Duration::from_millis(10),
                execute_deadline: Duration::from_secs(1),
                batch_size: 200,
            },
            metrics: crate::metrics::RebalancerMetrics::default(),
            in_flight: Arc::new(AsyncMutex::new(None)),
            state_topic: Arc::new(crate::state_topic::fake::InMemoryBackend::new_loaded()),
        };
        Detector::new(
            cfg,
            snapshot,
            usage_store,
            capacities,
            anomaly_store,
            proposal_store,
            executor_state,
            Arc::new(crate::api::GoalRegistry::default_registry()),
            goal_ctx,
            metrics,
            shutdown,
        )
    }

    #[test]
    fn snapshot_history_evicts_when_full() {
        let mut h = SnapshotHistory::new(2);
        h.push(&make_state(10, &[1, 2]));
        h.push(&make_state(20, &[1, 2]));
        h.push(&make_state(30, &[1, 2, 3]));
        let oldest = h.iter_recent().next().expect("at least one memo");
        assert2::assert!(h.len() == 2);
        assert2::assert!(oldest.snapshot_at_ms == 20);
    }

    #[test]
    fn snapshot_history_oldest_since_returns_oldest_within_cutoff() {
        let mut h = SnapshotHistory::new(4);
        h.push(&make_state(10, &[1]));
        h.push(&make_state(20, &[1]));
        h.push(&make_state(30, &[1]));
        let got = h.oldest_since(15).expect("memo at 20 satisfies cutoff");
        assert2::assert!(got.snapshot_at_ms == 20);
        assert2::assert!(h.oldest_since(1000).is_none());
    }

    #[test]
    fn snapshot_history_is_empty_tracks_pushes() {
        let mut h = SnapshotHistory::new(2);
        assert2::assert!(h.is_empty());
        h.push(&make_state(10, &[1]));
        assert2::assert!(!h.is_empty());
    }

    #[tokio::test]
    async fn detector_run_waits_until_shutdown() {
        let snapshot = crate::ingest::new_shared_snapshot();
        let anomaly_store = Arc::new(AnomalyStore::new(10));
        let proposal_store = Arc::new(ProposalStore::new(10));
        let shutdown = CancellationToken::new();
        let detector = detector_with(
            DetectorConfig {
                tick_interval: Duration::from_mins(1),
                ..DetectorConfig::default()
            },
            snapshot,
            anomaly_store,
            proposal_store,
            DetectorMetrics::default(),
            shutdown.clone(),
        );

        let handle = tokio::spawn(detector.run());
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert2::assert!(!handle.is_finished());
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("detector should stop after cancellation")
            .expect("detector task should join");
    }

    #[tokio::test]
    async fn tick_once_detects_then_resolves_under_replicated_partition() {
        let snapshot = crate::ingest::new_shared_snapshot();
        snapshot.store(Arc::new(Some(make_under_replicated_state(200_000, true))));
        let anomaly_store = Arc::new(AnomalyStore::new(10));
        let proposal_store = Arc::new(ProposalStore::new(10));
        let metrics = DetectorMetrics::default();
        let detector = detector_with(
            DetectorConfig {
                under_replicated_threshold: Duration::from_mins(2),
                auto_trigger_enabled: false,
                ..DetectorConfig::default()
            },
            snapshot.clone(),
            anomaly_store.clone(),
            proposal_store,
            metrics.clone(),
            CancellationToken::new(),
        );
        {
            let mut history = detector.history.lock().await;
            history.push(&make_under_replicated_state(80_000, true));
        }

        detector.tick_once(200_000).await;

        let key = AnomalyKey::Partition {
            topic: "t".into(),
            partition: 0,
        };
        let open = anomaly_store
            .find_open(AnomalyKind::UnderReplicatedPartitions, &key)
            .expect("under-replicated anomaly should be open");
        check!(
            (
                open.severity,
                metrics.anomalies_detected_under_replicated.get(),
                metrics.anomalies_open_under_replicated.get(),
            ) == (AnomalySeverity::Critical, 1, 1)
        );

        snapshot.store(Arc::new(Some(make_under_replicated_state(210_000, false))));
        detector.tick_once(210_000).await;

        assert2::assert!(
            anomaly_store
                .find_open(AnomalyKind::UnderReplicatedPartitions, &key)
                .is_none()
        );
        let resolved = anomaly_store
            .list(0, true)
            .into_iter()
            .find(|a| a.id == open.id)
            .expect("resolved anomaly remains in history");
        check!(
            (
                resolved.resolved_at_ms,
                metrics.anomalies_resolved_under_replicated.get(),
                metrics.anomalies_open_under_replicated.get(),
            ) == (Some(210_000), 1, 0)
        );
    }
}
