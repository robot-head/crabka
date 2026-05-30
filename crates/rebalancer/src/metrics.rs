//! Process-level `OpenMetrics` surface for the rebalancer.
//!
//! Usage and latency counters are registered off the same `Registry`.
//!
//! The `Counter` / `Gauge` handles from `prometheus-client` are cheaply
//! clonable (internally `Arc`-backed), so [`RebalancerMetrics`] itself
//! is `Clone` and can be shared between the ingester tick and the RPC
//! handlers without further wrapping.

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;

/// Bundle of metric handles emitted by the rebalancer process.
///
/// Metric names are intentionally bare (no `_total` / no prefix); the
/// `crabka_rebalancer` prefix is applied by the shared `Registry`
/// (see [`crate::health::new_registry`]) and `prometheus-client`
/// appends `_total` to `Counter` names automatically at encode time.
#[derive(Clone, Default)]
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
        m
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::new_registry;
    use assert2::assert;

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
}
