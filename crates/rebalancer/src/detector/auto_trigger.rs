//! Auto-trigger: map a freshly-detected anomaly to a goal set, run the
//! optimizer, persist the resulting proposal, tag the anomaly. Guarded
//! by config and by in-flight-execution / in-flight-reassignment gates.

use tracing::{debug, info, warn};

use super::DetectorConfig;
use crate::{
    api::GoalRegistry,
    detector::{Anomaly, AnomalyKind, AnomalyStore, DetectorMetrics},
    executor::ExecutorState,
    goals::GoalContext,
    ingest::SharedSnapshot,
    model::ProposalStore,
    optimizer,
};

#[derive(Debug, thiserror::Error)]
pub enum AutoTriggerError {
    #[error("unknown goal in goals_for_kind(): {0}")]
    UnknownGoal(String),
    #[error("optimizer error: {0}")]
    Optimizer(#[from] optimizer::OptimizeError),
}

/// Per-kind goal list. Each list is intentionally minimal — auto-trigger
/// runs the smallest goal set that will heal the specific anomaly,
/// rather than the full registry.
#[must_use]
pub fn goals_for_kind(kind: AnomalyKind) -> Vec<String> {
    match kind {
        AnomalyKind::BrokerDeath => vec![
            "PreferredLeaderIdempotency".into(),
            "RackAware".into(),
            "ReplicaDistribution".into(),
            "TopicReplicaDistribution".into(),
        ],
        AnomalyKind::UnderReplicatedPartitions => vec![
            "PreferredLeaderIdempotency".into(),
            "ReplicaDistribution".into(),
        ],
        AnomalyKind::DiskPressure => vec!["DiskCapacity".into(), "DiskUsage".into()],
        AnomalyKind::SlowBroker => vec!["CpuUsage".into(), "LeaderBytesIn".into()],
    }
}

pub struct AutoTriggerCtx<'a> {
    pub snapshot: SharedSnapshot,
    pub goal_registry: &'a GoalRegistry,
    pub goal_ctx: &'a GoalContext,
    pub proposal_store: &'a ProposalStore,
    pub anomaly_store: &'a AnomalyStore,
    pub executor_state: &'a ExecutorState,
    pub config: &'a DetectorConfig,
    pub metrics: &'a DetectorMetrics,
    pub now_ms: i64,
}

/// Evaluate gates, run the optimizer if all pass, persist the proposal
/// and tag the anomaly. Returns Ok on every non-error path (the gates
/// are not errors); returns Err only on optimizer failure or an unknown
/// goal name (which would be a bug — the registry should contain every
/// goal in `goals_for_kind`).
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(anomaly_id = %anomaly.id, kind = anomaly.kind.as_str()),
    err,
)]
pub async fn maybe_trigger(
    anomaly: &Anomaly,
    ctx: &AutoTriggerCtx<'_>,
) -> Result<(), AutoTriggerError> {
    if !ctx.config.auto_trigger_enabled {
        ctx.metrics.auto_trigger_skipped_disabled.inc();
        debug!(anomaly_id = %anomaly.id, "auto-trigger disabled");
        return Ok(());
    }
    if ctx.executor_state.in_flight.lock().await.is_some() {
        ctx.metrics.auto_trigger_skipped_executing.inc();
        debug!(anomaly_id = %anomaly.id, "auto-trigger skipped: execution in flight");
        return Ok(());
    }
    let snapshot_guard = ctx.snapshot.load();
    let Some(state) = (*snapshot_guard).as_ref() else {
        // No snapshot yet — the rule shouldn't have produced an anomaly
        // without a snapshot, but be defensive.
        ctx.metrics.auto_trigger_skipped_no_movements.inc();
        return Ok(());
    };
    if !state.in_flight_reassignments.is_empty() {
        ctx.metrics.auto_trigger_skipped_reassignments.inc();
        debug!(anomaly_id = %anomaly.id, "auto-trigger skipped: reassignments in flight");
        return Ok(());
    }
    if let Some(mute_until) = anomaly.mute_until_ms
        && ctx.now_ms < mute_until
    {
        ctx.metrics.auto_trigger_skipped_muted.inc();
        debug!(anomaly_id = %anomaly.id, mute_until, "auto-trigger skipped: muted");
        return Ok(());
    }

    let names = goals_for_kind(anomaly.kind);
    let goals = ctx.goal_registry.select(&names).map_err(|e| {
        warn!(error = %e, "goals_for_kind references unknown goal");
        AutoTriggerError::UnknownGoal(e.to_string())
    })?;

    let rebal_metrics = &ctx.executor_state.metrics;
    let started = std::time::Instant::now();
    let out = match optimizer::optimize(state, &goals, ctx.goal_ctx) {
        Ok(out) => out,
        Err(e) => {
            rebal_metrics.record_rebalance("error");
            ctx.metrics.auto_trigger_skipped_optimizer_error.inc();
            warn!(error = %e, anomaly_id = %anomaly.id, "auto-trigger optimizer error");
            return Err(AutoTriggerError::Optimizer(e));
        }
    };
    rebal_metrics.observe_rebalance_duration(started.elapsed().as_secs_f64());
    if out.proposal.movements.is_empty() {
        rebal_metrics.record_rebalance("no_movements");
        ctx.metrics.auto_trigger_skipped_no_movements.inc();
        debug!(anomaly_id = %anomaly.id, "auto-trigger skipped: optimizer produced no movements");
        return Ok(());
    }
    rebal_metrics.record_rebalance("ok");

    let proposal_id = out.proposal.id.clone();
    ctx.proposal_store.insert(out.proposal);

    let mute_until_ms = ctx.now_ms.saturating_add(
        i64::try_from(ctx.config.default_mute_window.as_millis()).unwrap_or(i64::MAX),
    );
    ctx.anomaly_store
        .set_triggered_proposal(&anomaly.id, proposal_id.clone(), mute_until_ms);

    ctx.metrics.record_auto_trigger_fired(anomaly.kind);
    info!(
        anomaly_id = %anomaly.id,
        kind = anomaly.kind.as_str(),
        proposal_id = %proposal_id,
        mute_until_ms,
        "auto-trigger fired",
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use assert2::assert;
    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{
        api::GoalRegistry,
        capacity::BrokerCapacities,
        detector::{
            Anomaly, AnomalyKey, AnomalyKind, AnomalySeverity, AnomalyStore, DetectorConfig,
            DetectorMetrics,
        },
        executor::{ExecutionHandle, ExecutorConfig, ExecutorState},
        goals::GoalContext,
        health::new_registry,
        ingest::new_shared_snapshot,
        model::{BrokerView, ClusterState, PartitionView, ProposalStore},
        scraper::UsageStore,
    };

    fn anomaly(kind: AnomalyKind) -> Anomaly {
        Anomaly {
            id: "a1".into(),
            kind,
            key: AnomalyKey::Broker(1),
            severity: AnomalySeverity::Critical,
            detected_at_ms: 0,
            last_seen_at_ms: 0,
            resolved_at_ms: None,
            triggered_proposal_id: None,
            mute_until_ms: None,
            details: String::new(),
        }
    }

    struct Harness {
        proposal_store: Arc<ProposalStore>,
        anomaly_store: Arc<AnomalyStore>,
        executor_state: ExecutorState,
        snapshot: crate::ingest::SharedSnapshot,
        goal_registry: GoalRegistry,
        goal_ctx: GoalContext,
        metrics: DetectorMetrics,
        config: DetectorConfig,
        _dir: tempfile::TempDir,
    }

    fn build_harness(auto_trigger_enabled: bool) -> Harness {
        let dir = tempdir().unwrap();
        let proposal_store = Arc::new(ProposalStore::new(20));
        let anomaly_store = Arc::new(AnomalyStore::new(200));

        // Pre-populate snapshot with a multi-broker state so the
        // optimizer has somewhere to move data.
        let snap = new_shared_snapshot();
        let state = ClusterState {
            cluster_id: None,
            snapshot_at_ms: 1,
            brokers: (1..=3)
                .map(|id| BrokerView {
                    id,
                    host: format!("h{id}"),
                    port: 9092,
                    rack: None,
                })
                .collect(),
            partitions: (0..6)
                .map(|p| PartitionView {
                    topic: "t".into(),
                    partition: p,
                    replicas: vec![1, 2],
                    leader: 1,
                    isr: vec![1, 2],
                })
                .collect(),
            in_flight_reassignments: vec![],
        };
        snap.store(Arc::new(Some(state)));

        let mut registry = new_registry();
        let rebal_metrics = crate::metrics::RebalancerMetrics::register(&mut registry);
        let metrics = DetectorMetrics::register(&mut registry);

        let executor_state = ExecutorState {
            store: proposal_store.clone(),
            config: ExecutorConfig {
                data_dir: dir.path().to_path_buf(),
                default_throttle_bytes_per_sec: 50_000_000,
                poll_interval: Duration::from_millis(50),
                execute_deadline: Duration::from_secs(30),
                batch_size: 200,
            },
            metrics: rebal_metrics,
            in_flight: Arc::new(tokio::sync::Mutex::new(None)),
            state_topic: std::sync::Arc::new(
                crate::state_topic::fake::InMemoryBackend::new_loaded(),
            ),
        };

        let goal_registry = GoalRegistry::default_registry();
        let goal_ctx = GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
            broker_capacities: Arc::new(BrokerCapacities::default()),
            broker_usages: Arc::new(UsageStore::default()),
        };

        Harness {
            proposal_store,
            anomaly_store,
            executor_state,
            snapshot: snap,
            goal_registry,
            goal_ctx,
            metrics,
            config: DetectorConfig {
                auto_trigger_enabled,
                default_mute_window: Duration::from_mins(15),
                ..DetectorConfig::default()
            },
            _dir: dir,
        }
    }

    fn make_ctx(h: &Harness, now: i64) -> AutoTriggerCtx<'_> {
        AutoTriggerCtx {
            snapshot: h.snapshot.clone(),
            goal_registry: &h.goal_registry,
            goal_ctx: &h.goal_ctx,
            proposal_store: &h.proposal_store,
            anomaly_store: &h.anomaly_store,
            executor_state: &h.executor_state,
            config: &h.config,
            metrics: &h.metrics,
            now_ms: now,
        }
    }

    #[tokio::test]
    async fn disabled_skips_trigger() {
        let h = build_harness(false);
        let a = anomaly(AnomalyKind::BrokerDeath);
        maybe_trigger(&a, &make_ctx(&h, 1000)).await.unwrap();
        assert!(h.proposal_store.list(0).len() == 0);
        assert!(h.metrics.auto_trigger_skipped_disabled.get() == 1);
    }

    #[tokio::test]
    async fn in_flight_execution_skips_trigger() {
        let h = build_harness(true);
        let handle = ExecutionHandle {
            proposal_id: "p1".into(),
            task: tokio::spawn(async {}),
            cancel: CancellationToken::new(),
            started_at: std::time::Instant::now(),
        };
        *h.executor_state.in_flight.lock().await = Some(handle);
        let a = anomaly(AnomalyKind::BrokerDeath);
        maybe_trigger(&a, &make_ctx(&h, 1000)).await.unwrap();
        assert!(h.proposal_store.list(0).len() == 0);
        assert!(h.metrics.auto_trigger_skipped_executing.get() == 1);
    }

    #[tokio::test]
    async fn in_flight_reassignments_skip_trigger() {
        let h = build_harness(true);
        // Mutate the snapshot to add an in-flight reassignment.
        {
            let g = h.snapshot.load();
            let Some(state) = (*g).as_ref() else { panic!() };
            let mut new_state = state.clone();
            new_state.in_flight_reassignments = vec![crate::model::InFlightReassignment {
                topic: "t".into(),
                partition: 0,
                adding: vec![3],
                removing: vec![1],
            }];
            h.snapshot.store(Arc::new(Some(new_state)));
        }
        let a = anomaly(AnomalyKind::BrokerDeath);
        maybe_trigger(&a, &make_ctx(&h, 1000)).await.unwrap();
        assert!(h.proposal_store.list(0).len() == 0);
        assert!(h.metrics.auto_trigger_skipped_reassignments.get() == 1);
    }

    #[test]
    fn goals_for_kind_lists() {
        assert!(
            goals_for_kind(AnomalyKind::BrokerDeath)
                == vec![
                    "PreferredLeaderIdempotency".to_string(),
                    "RackAware".into(),
                    "ReplicaDistribution".into(),
                    "TopicReplicaDistribution".into(),
                ]
        );
        assert!(
            goals_for_kind(AnomalyKind::DiskPressure)
                == vec!["DiskCapacity".to_string(), "DiskUsage".into()]
        );
    }

    #[tokio::test]
    async fn muted_anomaly_skips_trigger() {
        let h = build_harness(true);
        let mut a = anomaly(AnomalyKind::BrokerDeath);
        a.mute_until_ms = Some(5000); // muted until 5000ms
        maybe_trigger(&a, &make_ctx(&h, 1000)).await.unwrap();
        assert!(h.proposal_store.list(0).len() == 0);
        assert!(h.metrics.auto_trigger_skipped_muted.get() == 1);
    }

    #[tokio::test]
    async fn mute_window_expires_at_exact_boundary() {
        let h = build_harness(true);
        let mut a = anomaly(AnomalyKind::BrokerDeath);
        a.mute_until_ms = Some(1000);

        let _ = maybe_trigger(&a, &make_ctx(&h, 1000)).await;

        assert!(
            h.metrics.auto_trigger_skipped_muted.get() == 0,
            "anomaly should not be muted at the exact mute_until_ms boundary"
        );
    }
}
