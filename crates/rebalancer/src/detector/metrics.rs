//! Detector-specific metrics surface.
//!
//! Mirrors the shape of [`crate::metrics::RebalancerMetrics`]: a flat
//! struct of `Counter` / `Gauge` handles, all registered against the
//! shared `crabka_rebalancer_` registry so they share the existing
//! `/metrics` endpoint.
//!
//! Explicit per-variant fields (rather than a `Family<AnomalyKind, _>`)
//! keep the surface simple — four anomaly kinds, four counters per
//! event, no extra `EncodeLabelSet` derive needed. Convenience helpers
//! dispatch on `AnomalyKind` so callers don't need to know the field
//! names.

use prometheus_client::{
    metrics::{counter::Counter, gauge::Gauge},
    registry::Registry,
};

use crate::detector::AnomalyKind;

/// Bundle of detector-side metric handles. Handles are `Arc`-backed so
/// `Clone` is cheap; the detector tick and any future RPC handlers can
/// share one instance.
#[derive(Clone, Default)]
pub struct DetectorMetrics {
    pub anomalies_detected_broker_death: Counter,
    pub anomalies_detected_under_replicated: Counter,
    pub anomalies_detected_disk_pressure: Counter,
    pub anomalies_detected_slow_broker: Counter,

    pub anomalies_resolved_broker_death: Counter,
    pub anomalies_resolved_under_replicated: Counter,
    pub anomalies_resolved_disk_pressure: Counter,
    pub anomalies_resolved_slow_broker: Counter,

    pub auto_trigger_fired_broker_death: Counter,
    pub auto_trigger_fired_under_replicated: Counter,
    pub auto_trigger_fired_disk_pressure: Counter,
    pub auto_trigger_fired_slow_broker: Counter,

    pub auto_trigger_skipped_disabled: Counter,
    pub auto_trigger_skipped_executing: Counter,
    pub auto_trigger_skipped_reassignments: Counter,
    pub auto_trigger_skipped_muted: Counter,
    pub auto_trigger_skipped_no_movements: Counter,
    pub auto_trigger_skipped_optimizer_error: Counter,

    pub anomalies_open_broker_death: Gauge,
    pub anomalies_open_under_replicated: Gauge,
    pub anomalies_open_disk_pressure: Gauge,
    pub anomalies_open_slow_broker: Gauge,
}

impl DetectorMetrics {
    /// Register all detector metrics against `registry` and return the
    /// bundle of handles. Counter names omit `_total`; `prometheus-client`
    /// appends it at encode time.
    #[must_use]
    // Flat per-variant registration is intentional.
    pub fn register(registry: &mut Registry) -> Self {
        let m = Self::default();

        // Detected per kind.
        registry.register(
            "anomalies_detected_broker_death",
            "Total BrokerDeath anomalies detected",
            m.anomalies_detected_broker_death.clone(),
        );
        registry.register(
            "anomalies_detected_under_replicated",
            "Total UnderReplicatedPartitions anomalies detected",
            m.anomalies_detected_under_replicated.clone(),
        );
        registry.register(
            "anomalies_detected_disk_pressure",
            "Total DiskPressure anomalies detected",
            m.anomalies_detected_disk_pressure.clone(),
        );
        registry.register(
            "anomalies_detected_slow_broker",
            "Total SlowBroker anomalies detected",
            m.anomalies_detected_slow_broker.clone(),
        );

        // Resolved per kind.
        registry.register(
            "anomalies_resolved_broker_death",
            "Total BrokerDeath anomalies resolved",
            m.anomalies_resolved_broker_death.clone(),
        );
        registry.register(
            "anomalies_resolved_under_replicated",
            "Total UnderReplicatedPartitions anomalies resolved",
            m.anomalies_resolved_under_replicated.clone(),
        );
        registry.register(
            "anomalies_resolved_disk_pressure",
            "Total DiskPressure anomalies resolved",
            m.anomalies_resolved_disk_pressure.clone(),
        );
        registry.register(
            "anomalies_resolved_slow_broker",
            "Total SlowBroker anomalies resolved",
            m.anomalies_resolved_slow_broker.clone(),
        );

        // Auto-trigger fired per kind.
        registry.register(
            "auto_trigger_fired_broker_death",
            "Total BrokerDeath anomalies that fired an auto-trigger",
            m.auto_trigger_fired_broker_death.clone(),
        );
        registry.register(
            "auto_trigger_fired_under_replicated",
            "Total UnderReplicatedPartitions anomalies that fired an auto-trigger",
            m.auto_trigger_fired_under_replicated.clone(),
        );
        registry.register(
            "auto_trigger_fired_disk_pressure",
            "Total DiskPressure anomalies that fired an auto-trigger",
            m.auto_trigger_fired_disk_pressure.clone(),
        );
        registry.register(
            "auto_trigger_fired_slow_broker",
            "Total SlowBroker anomalies that fired an auto-trigger",
            m.auto_trigger_fired_slow_broker.clone(),
        );

        // Skipped-reason counters.
        registry.register(
            "auto_trigger_skipped_disabled",
            "Total auto-triggers skipped because auto_trigger_enabled is false",
            m.auto_trigger_skipped_disabled.clone(),
        );
        registry.register(
            "auto_trigger_skipped_executing",
            "Total auto-triggers skipped because an execution is already in flight",
            m.auto_trigger_skipped_executing.clone(),
        );
        registry.register(
            "auto_trigger_skipped_reassignments",
            "Total auto-triggers skipped because in-flight reassignments exist",
            m.auto_trigger_skipped_reassignments.clone(),
        );
        registry.register(
            "auto_trigger_skipped_muted",
            "Total auto-triggers skipped because the anomaly is muted",
            m.auto_trigger_skipped_muted.clone(),
        );
        registry.register(
            "auto_trigger_skipped_no_movements",
            "Total auto-triggers skipped because the optimizer produced no movements",
            m.auto_trigger_skipped_no_movements.clone(),
        );
        registry.register(
            "auto_trigger_skipped_optimizer_error",
            "Total auto-triggers skipped because the optimizer returned an error",
            m.auto_trigger_skipped_optimizer_error.clone(),
        );

        // Open-count gauges per kind.
        registry.register(
            "anomalies_open_broker_death",
            "Currently-open BrokerDeath anomalies",
            m.anomalies_open_broker_death.clone(),
        );
        registry.register(
            "anomalies_open_under_replicated",
            "Currently-open UnderReplicatedPartitions anomalies",
            m.anomalies_open_under_replicated.clone(),
        );
        registry.register(
            "anomalies_open_disk_pressure",
            "Currently-open DiskPressure anomalies",
            m.anomalies_open_disk_pressure.clone(),
        );
        registry.register(
            "anomalies_open_slow_broker",
            "Currently-open SlowBroker anomalies",
            m.anomalies_open_slow_broker.clone(),
        );

        m
    }

    /// Increment the `detected` counter for `kind`.
    pub fn record_detected(&self, kind: AnomalyKind) {
        let _ = match kind {
            AnomalyKind::BrokerDeath => self.anomalies_detected_broker_death.inc(),
            AnomalyKind::UnderReplicatedPartitions => {
                self.anomalies_detected_under_replicated.inc()
            }
            AnomalyKind::DiskPressure => self.anomalies_detected_disk_pressure.inc(),
            AnomalyKind::SlowBroker => self.anomalies_detected_slow_broker.inc(),
        };
    }

    /// Increment the `resolved` counter for `kind`.
    pub fn record_resolved(&self, kind: AnomalyKind) {
        let _ = match kind {
            AnomalyKind::BrokerDeath => self.anomalies_resolved_broker_death.inc(),
            AnomalyKind::UnderReplicatedPartitions => {
                self.anomalies_resolved_under_replicated.inc()
            }
            AnomalyKind::DiskPressure => self.anomalies_resolved_disk_pressure.inc(),
            AnomalyKind::SlowBroker => self.anomalies_resolved_slow_broker.inc(),
        };
    }

    /// Increment the `auto_trigger_fired` counter for `kind`.
    pub fn record_auto_trigger_fired(&self, kind: AnomalyKind) {
        let _ = match kind {
            AnomalyKind::BrokerDeath => self.auto_trigger_fired_broker_death.inc(),
            AnomalyKind::UnderReplicatedPartitions => {
                self.auto_trigger_fired_under_replicated.inc()
            }
            AnomalyKind::DiskPressure => self.auto_trigger_fired_disk_pressure.inc(),
            AnomalyKind::SlowBroker => self.auto_trigger_fired_slow_broker.inc(),
        };
    }

    /// Set the open-count gauge for `kind`.
    pub fn set_open_count(&self, kind: AnomalyKind, count: i64) {
        let _ = match kind {
            AnomalyKind::BrokerDeath => self.anomalies_open_broker_death.set(count),
            AnomalyKind::UnderReplicatedPartitions => {
                self.anomalies_open_under_replicated.set(count)
            }
            AnomalyKind::DiskPressure => self.anomalies_open_disk_pressure.set(count),
            AnomalyKind::SlowBroker => self.anomalies_open_slow_broker.set(count),
        };
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::health::new_registry;

    #[test]
    fn register_emits_all_metric_names() {
        let mut registry = new_registry();
        let m = DetectorMetrics::register(&mut registry);
        m.record_detected(AnomalyKind::BrokerDeath);
        m.record_resolved(AnomalyKind::SlowBroker);
        m.record_auto_trigger_fired(AnomalyKind::DiskPressure);
        m.auto_trigger_skipped_executing.inc();
        m.set_open_count(AnomalyKind::UnderReplicatedPartitions, 3);

        let mut buf = String::new();
        prometheus_client::encoding::text::encode(&mut buf, &registry).unwrap();
        for needle in [
            "crabka_rebalancer_anomalies_detected_broker_death_total",
            "crabka_rebalancer_anomalies_resolved_slow_broker_total",
            "crabka_rebalancer_auto_trigger_fired_disk_pressure_total",
            "crabka_rebalancer_auto_trigger_skipped_executing_total",
            "crabka_rebalancer_anomalies_open_under_replicated",
        ] {
            assert2::assert!(buf.contains(needle));
        }
    }

    #[test]
    fn helper_methods_update_the_expected_variant_handles() {
        let m = DetectorMetrics::default();

        m.record_detected(AnomalyKind::UnderReplicatedPartitions);
        assert2::assert!(
            (
                m.anomalies_detected_broker_death.get(),
                m.anomalies_detected_under_replicated.get()
            ) == (0, 1)
        );

        m.record_resolved(AnomalyKind::DiskPressure);
        assert2::assert!(
            (
                m.anomalies_resolved_disk_pressure.get(),
                m.anomalies_resolved_slow_broker.get()
            ) == (1, 0)
        );

        m.record_auto_trigger_fired(AnomalyKind::SlowBroker);
        assert2::assert!(
            (
                m.auto_trigger_fired_slow_broker.get(),
                m.auto_trigger_fired_broker_death.get()
            ) == (1, 0)
        );

        m.set_open_count(AnomalyKind::BrokerDeath, 7);
        assert2::assert!(
            (
                m.anomalies_open_broker_death.get(),
                m.anomalies_open_under_replicated.get()
            ) == (7, 0)
        );
    }
}
