use std::sync::Arc;

use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};
use tokio::sync::Mutex;

/// Shared Prometheus registry. Wrap in `Arc<Mutex<…>>` because
/// `prometheus-client` requires `&mut Registry` to register metrics
/// and we want controllers to register lazily on first reconcile.
pub type SharedRegistry = Arc<Mutex<Registry>>;

/// `{kind, result}` label for the reconcile-outcome counter. `kind` is the
/// CRD kind (`Kafka`, `KafkaNodePool`, `KafkaTopic`, …); `result` is one of
/// `ok` (reconcile returned an `Action` without error), `error` (reconcile
/// returned `Err`), or `requeue` (a caller explicitly classified a
/// surfaced-not-ready / invalid-spec short-circuit — see [`ReconcileResult`]).
/// The per-kind `reconcile` wrappers auto-classify `ok`/`error`; the `requeue`
/// value is reserved for call sites that record it deliberately.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ReconcileOutcomeLabels {
    pub kind: String,
    pub result: String,
}

/// `{kind}` label, used for the per-kind reconcile-duration histogram and the
/// managed-resources gauge.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct KindLabel {
    pub kind: String,
}

/// Reconcile result variants for [`ControllerMetrics::record_reconcile`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconcileResult {
    /// Reconcile completed a full pass (resources applied, `Ready` derived).
    Ok,
    /// Reconcile returned `Err` — surfaced through the controller's error
    /// policy (requeue-with-backoff).
    Error,
    /// Reconcile returned a requeue `Action` after writing a not-ready /
    /// invalid-spec condition, rather than erroring.
    Requeue,
}

impl ReconcileResult {
    const fn as_str(self) -> &'static str {
        match self {
            ReconcileResult::Ok => "ok",
            ReconcileResult::Error => "error",
            ReconcileResult::Requeue => "requeue",
        }
    }
}

/// Cheaply-clonable bundle of operator-wide controller metrics. Handles are
/// `Arc`-backed inside `prometheus-client`, so cloning the bundle into every
/// per-reconciler [`crate::context::Context`] is a handful of `Arc::clone`s.
///
/// Metric names are registered WITHOUT the `_total` suffix — `prometheus-client`
/// appends it to `Counter`s at encode time — and WITHOUT a crate prefix (the
/// shared [`Registry`] carries the `crabka_operator` prefix).
#[derive(Clone)]
pub struct ControllerMetrics {
    /// `reconciliations_total{kind,result}` — reconcile passes by CRD kind and
    /// outcome (`ok` / `error` / `requeue`).
    pub reconciliations: Family<ReconcileOutcomeLabels, Counter>,
    /// `reconcile_duration_seconds{kind}` — wall-clock duration of one full
    /// reconcile pass, per CRD kind.
    pub reconcile_duration: Family<KindLabel, Histogram>,
    /// `managed_resources{kind}` — last-observed count of primary CRs the
    /// operator reconciled in the most recent pass of each kind (a coarse
    /// liveness/ownership signal; set, not accumulated).
    pub managed_resources: Family<KindLabel, Gauge>,
}

impl ControllerMetrics {
    /// Register every controller metric against `registry` and return the
    /// handle bundle. Call once against the process registry before wrapping it
    /// in the shared `Mutex`.
    #[must_use]
    pub fn register(registry: &mut Registry) -> Self {
        let reconciliations = Family::<ReconcileOutcomeLabels, Counter>::default();
        // Histogram buckets span sub-millisecond (fully-cached no-op passes)
        // through ~30 s (a pass that issues several apiserver round-trips plus
        // an admin RPC).
        let reconcile_duration = Family::<KindLabel, Histogram>::new_with_constructor(|| {
            Histogram::new([
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
            ])
        });
        let managed_resources = Family::<KindLabel, Gauge>::default();

        // Counter registered WITHOUT `_total`; encoder appends it →
        // `crabka_operator_reconciliations_total`.
        registry.register(
            "reconciliations",
            "Reconcile passes by CRD kind and outcome (result=ok|error|requeue)",
            reconciliations.clone(),
        );
        registry.register(
            "reconcile_duration_seconds",
            "Duration of one full reconcile pass in seconds, by CRD kind",
            reconcile_duration.clone(),
        );
        registry.register(
            "managed_resources",
            "Count of primary CRs observed in the most recent reconcile of each kind",
            managed_resources.clone(),
        );

        Self {
            reconciliations,
            reconcile_duration,
            managed_resources,
        }
    }

    /// Record one reconcile pass: bump the `{kind,result}` outcome counter and
    /// observe the pass duration against the `{kind}` histogram.
    pub fn record_reconcile(&self, kind: &str, result: ReconcileResult, duration_secs: f64) {
        self.reconciliations
            .get_or_create(&ReconcileOutcomeLabels {
                kind: kind.to_string(),
                result: result.as_str().to_string(),
            })
            .inc();
        self.reconcile_duration
            .get_or_create(&KindLabel {
                kind: kind.to_string(),
            })
            .observe(duration_secs);
    }

    /// Set the `managed_resources{kind}` gauge to `count`.
    pub fn set_managed_resources(&self, kind: &str, count: i64) {
        self.managed_resources
            .get_or_create(&KindLabel {
                kind: kind.to_string(),
            })
            .set(count);
    }
}

/// Initialise the global `tracing` subscriber. Idempotent: silently
/// no-ops if a global subscriber is already installed (e.g., in tests
/// that call this more than once across a process).
// cargo-mutants: installs a process-global subscriber via idempotent try_init;
// no return value or per-call observable effect to assert.
#[cfg_attr(test, mutants::skip)]
pub fn init_tracing(filter: &str) {
    use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

    let env = tracing_subscriber::EnvFilter::try_new(filter)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::registry()
        .with(crabka_logfmt::layer(env, std::io::stdout))
        .try_init();
}

/// Build a fresh registry. Callers wrap in `Arc<Mutex<…>>`.
#[must_use]
pub fn new_registry() -> Registry {
    Registry::with_prefix("crabka_operator")
}

/// Build a fresh registry with the controller metrics already registered.
/// Returns the registry (to be wrapped in `Arc<Mutex<…>>` for the `/metrics`
/// exporter) alongside the cloneable [`ControllerMetrics`] handle bundle (to be
/// threaded into every reconciler's [`crate::context::Context`]).
#[must_use]
pub fn new_registry_with_metrics() -> (Registry, ControllerMetrics) {
    let mut registry = new_registry();
    let metrics = ControllerMetrics::register(&mut registry);
    (registry, metrics)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn registry_has_prefix() {
        let mut r = new_registry();
        let g = prometheus_client::metrics::gauge::Gauge::<i64>::default();
        r.register("up", "operator liveness", g);
        let mut s = String::new();
        prometheus_client::encoding::text::encode(&mut s, &r).unwrap();
        assert!(s.contains("crabka_operator_up"));
    }

    #[test]
    fn init_tracing_idempotent() {
        init_tracing("info");
        // Second call's filter is intentionally ignored — global subscriber is already installed.
        init_tracing("debug");
    }

    #[test]
    fn controller_metrics_register_and_encode() {
        let (registry, metrics) = new_registry_with_metrics();
        metrics.record_reconcile("Kafka", ReconcileResult::Ok, 0.42);
        metrics.record_reconcile("Kafka", ReconcileResult::Error, 0.1);
        metrics.record_reconcile("KafkaTopic", ReconcileResult::Requeue, 0.01);
        metrics.set_managed_resources("Kafka", 3);

        let mut s = String::new();
        prometheus_client::encoding::text::encode(&mut s, &registry).unwrap();

        // Counter is registered without `_total`; the encoder appends it.
        for (name, expected) in [
            (
                "reconciliation counter",
                "crabka_operator_reconciliations_total",
            ),
            (
                "duration histogram",
                "crabka_operator_reconcile_duration_seconds",
            ),
            (
                "managed resource gauge",
                "crabka_operator_managed_resources",
            ),
            ("kind label", "kind=\"Kafka\""),
            ("ok label", "result=\"ok\""),
            ("error label", "result=\"error\""),
            ("requeue label", "result=\"requeue\""),
            ("OpenMetrics terminator", "# EOF"),
        ] {
            assert!(s.contains(expected), "case {name}");
        }

        // Re-encoding still works and the gauge reflects the last set value.
        metrics.set_managed_resources("Kafka", 5);
        let mut s2 = String::new();
        prometheus_client::encoding::text::encode(&mut s2, &registry).unwrap();
        assert!(s2.contains("crabka_operator_managed_resources"));
    }

    #[test]
    fn controller_metrics_handles_clone_share_state() {
        let (_registry, metrics) = new_registry_with_metrics();
        let cloned = metrics.clone();
        cloned.record_reconcile("Kafka", ReconcileResult::Ok, 0.01);
        cloned.record_reconcile("Kafka", ReconcileResult::Ok, 0.02);
        // Increments on the clone are visible through the original handle
        // (Family entries are Arc-backed).
        let n = metrics
            .reconciliations
            .get_or_create(&ReconcileOutcomeLabels {
                kind: "Kafka".into(),
                result: "ok".into(),
            })
            .get();
        assert!(n == 2, "expected 2 ok reconciles, got {n}");
    }
}
