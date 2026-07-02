//! Process-level `OpenMetrics` surface for the rebalancer.
//!
//! Usage and latency counters are registered off the same `Registry`.
//!
//! The `Counter` / `Gauge` handles from `prometheus-client` are cheaply
//! clonable (internally `Arc`-backed), so [`RebalancerMetrics`] itself
//! is `Clone` and can be shared between the ingester tick and the RPC
//! handlers without further wrapping.

use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};

/// Label for a rebalance-compute outcome (`"ok"`, `"no_movements"`,
/// `"error"`).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ResultLabel {
    pub result: String,
}

/// Bucket boundaries (seconds) for the rebalance-compute latency histogram.
/// The optimizer is CPU-bound over the in-memory cluster snapshot, so the
/// buckets skew sub-second (1 ms – 5 s) with headroom for very large
/// clusters.
fn rebalance_duration_buckets() -> [f64; 11] {
    [
        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
    ]
}

/// Bundle of metric handles emitted by the rebalancer process.
///
/// Metric names are intentionally bare (no `_total` / no prefix); the
/// `crabka_rebalancer` prefix is applied by the shared `Registry`
/// (see [`crate::health::new_registry`]) and `prometheus-client`
/// appends `_total` to `Counter` names automatically at encode time.
#[derive(Clone)]
pub struct RebalancerMetrics {
    /// Unix epoch millis of the most recent successful cluster-state snapshot.
    pub snapshot_at_ms: Gauge,
    /// Total successful cluster-state snapshots.
    pub snapshots_total: Counter,
    /// Total proposals computed via `CreateProposal`.
    pub proposals_created_total: Counter,
    /// Total `ExecuteProposal` invocations that successfully entered `Executing`.
    pub executions_started_total: Counter,
    /// Total executions that reached `Completed`.
    pub executions_completed_total: Counter,
    /// Total executions that reached `Failed`.
    pub executions_failed_total: Counter,
    /// Total executions that reached `Cancelled` via `CancelExecution`.
    pub executions_cancelled_total: Counter,
    /// Rebalance (optimizer) computations, labelled by result
    /// (`ok`, `no_movements`, `error`).
    pub rebalances: Family<ResultLabel, Counter>,
    /// Wall-clock latency of a single `optimizer::optimize` invocation, seconds.
    pub rebalance_duration_seconds: Histogram,
    /// Current number of in-flight partition reassignments observed in the
    /// most recent cluster snapshot.
    pub pending_reassignments: Gauge,
}

impl Default for RebalancerMetrics {
    fn default() -> Self {
        Self {
            snapshot_at_ms: Gauge::default(),
            snapshots_total: Counter::default(),
            proposals_created_total: Counter::default(),
            executions_started_total: Counter::default(),
            executions_completed_total: Counter::default(),
            executions_failed_total: Counter::default(),
            executions_cancelled_total: Counter::default(),
            rebalances: Family::<ResultLabel, Counter>::default(),
            // Histogram is not `Default`; seed it with the shared buckets so the
            // family/standalone handle is populated identically on every clone.
            rebalance_duration_seconds: Histogram::new(rebalance_duration_buckets()),
            pending_reassignments: Gauge::default(),
        }
    }
}

impl RebalancerMetrics {
    /// Register all metrics against `registry` and return a bundle of
    /// the handles. Caller-supplied registry so the binary entry can
    /// share one `Registry` between this and the axum `/metrics`
    /// handler.
    #[must_use]
    pub fn register(registry: &mut Registry) -> Self {
        let m = Self::default();
        registry.register(
            "snapshot_at_ms",
            "Unix epoch millis of the most recent successful cluster-state snapshot",
            m.snapshot_at_ms.clone(),
        );
        // Counter names omit the `_total` suffix; `prometheus-client`
        // appends it automatically at encode time (so registering
        // `snapshots` here renders as `crabka_rebalancer_snapshots_total`).
        registry.register(
            "snapshots",
            "Total successful cluster-state snapshots",
            m.snapshots_total.clone(),
        );
        registry.register(
            "proposals_created",
            "Total proposals computed via CreateProposal",
            m.proposals_created_total.clone(),
        );
        registry.register(
            "executions_started",
            "Total ExecuteProposal invocations that successfully entered Executing",
            m.executions_started_total.clone(),
        );
        registry.register(
            "executions_completed",
            "Total executions that reached Completed",
            m.executions_completed_total.clone(),
        );
        registry.register(
            "executions_failed",
            "Total executions that reached Failed",
            m.executions_failed_total.clone(),
        );
        registry.register(
            "executions_cancelled",
            "Total executions that reached Cancelled via CancelExecution",
            m.executions_cancelled_total.clone(),
        );
        // `rebalances` is a Counter family; `prometheus-client` appends `_total`
        // and renders the `result` label, so this becomes
        // `crabka_rebalancer_rebalances_total{result="..."}`.
        registry.register(
            "rebalances",
            "Total optimizer rebalance computations, labelled by result (ok, no_movements, error)",
            m.rebalances.clone(),
        );
        registry.register(
            "rebalance_duration_seconds",
            "Wall-clock latency of a single optimizer::optimize invocation (histogram), seconds",
            m.rebalance_duration_seconds.clone(),
        );
        registry.register(
            "pending_reassignments",
            "In-flight partition reassignments observed in the most recent cluster snapshot",
            m.pending_reassignments.clone(),
        );
        m
    }

    /// Record a rebalance-compute outcome with the given `result` label
    /// (`"ok"`, `"no_movements"`, `"error"`).
    pub fn record_rebalance(&self, result: &str) {
        self.rebalances
            .get_or_create(&ResultLabel {
                result: result.into(),
            })
            .inc();
    }

    /// Observe an `optimizer::optimize` latency (seconds).
    pub fn observe_rebalance_duration(&self, secs: f64) {
        self.rebalance_duration_seconds.observe(secs);
    }

    /// Set the pending-reassignments gauge to `n`.
    pub fn set_pending_reassignments(&self, n: i64) {
        self.pending_reassignments.set(n);
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::health::new_registry;

    #[test]
    fn register_emits_three_metric_names_with_crate_prefix() {
        let mut registry = new_registry();
        let m = RebalancerMetrics::register(&mut registry);
        m.snapshots_total.inc();
        m.proposals_created_total.inc();
        m.snapshot_at_ms.set(42);

        let mut buf = String::new();
        prometheus_client::encoding::text::encode(&mut buf, &registry).unwrap();
        for needle in [
            "crabka_rebalancer_snapshot_at_ms",
            "crabka_rebalancer_snapshots_total",
            "crabka_rebalancer_proposals_created_total",
        ] {
            assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
        assert!(buf.contains("# EOF"), "OpenMetrics terminator missing");
    }

    #[test]
    fn handles_clone_share_state() {
        let mut registry = new_registry();
        let m = RebalancerMetrics::register(&mut registry);
        // Clone is `Arc`-backed: increments on the clone are visible
        // through the original handle.
        let cloned = m.clone();
        cloned.snapshots_total.inc();
        cloned.snapshots_total.inc();
        assert!(m.snapshots_total.get() == 2);
    }

    #[test]
    fn rebalance_metrics_register_and_record() {
        let mut registry = new_registry();
        let m = RebalancerMetrics::register(&mut registry);
        m.record_rebalance("ok");
        m.record_rebalance("ok");
        m.record_rebalance("no_movements");
        m.observe_rebalance_duration(0.012);
        m.set_pending_reassignments(4);

        // Counter family accumulates per-label.
        assert!(
            m.rebalances
                .get_or_create(&ResultLabel {
                    result: "ok".into()
                })
                .get()
                == 2
        );
        assert!(m.pending_reassignments.get() == 4);

        let mut buf = String::new();
        prometheus_client::encoding::text::encode(&mut buf, &registry).unwrap();
        for needle in [
            "crabka_rebalancer_rebalances_total",
            "crabka_rebalancer_rebalance_duration_seconds_bucket",
            "crabka_rebalancer_rebalance_duration_seconds_count",
            "crabka_rebalancer_pending_reassignments",
        ] {
            assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
    }
}
