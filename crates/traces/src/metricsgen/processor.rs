//! Metrics-generator processor orchestration.

use std::{collections::HashMap, sync::Arc};

use crate::metricsgen::{
    checkpoint::CheckpointCodecError, clock::Clock, config::MetricsGenConfig, contract::SpanRecord,
    series::SeriesPayload, servicegraph::EdgeStore, spanmetrics::SpanMetricsRegistry,
};

#[derive(Debug)]
struct TenantState {
    span_metrics: SpanMetricsRegistry,
    edges: EdgeStore,
}

pub type EdgeCheckpointEntry = (Vec<u8>, Vec<u8>);
pub type TenantEdgeCheckpoints = (String, Vec<EdgeCheckpointEntry>);

/// Composes span-metrics and service-graph processors per tenant.
pub struct MetricsGenerator {
    cfg: MetricsGenConfig,
    clock: Arc<dyn Clock>,
    per_tenant: HashMap<String, TenantState>,
}

impl MetricsGenerator {
    #[must_use]
    pub fn new(cfg: MetricsGenConfig, clock: Arc<dyn Clock>) -> Self {
        Self {
            cfg,
            clock,
            per_tenant: HashMap::new(),
        }
    }

    pub fn process(&mut self, span: &SpanRecord) {
        let cfg = &self.cfg;
        let state = self
            .per_tenant
            .entry(span.tenant.clone())
            .or_insert_with(|| TenantState {
                span_metrics: SpanMetricsRegistry::new(cfg),
                edges: EdgeStore::new(cfg),
            });

        state.span_metrics.record_span(span);
        state.edges.record_span(span, self.clock.now_ns());
    }

    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub fn restore_edge_checkpoint(
        &mut self,
        tenant: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), CheckpointCodecError> {
        let cfg = &self.cfg;
        let state = self
            .per_tenant
            .entry(tenant.to_string())
            .or_insert_with(|| TenantState {
                span_metrics: SpanMetricsRegistry::new(cfg),
                edges: EdgeStore::new(cfg),
            });
        state.edges.restore_checkpoint_entry(tenant, key, value)
    }

    #[must_use]
    pub fn collect(&mut self, timestamp_ms: i64) -> Vec<SeriesPayload> {
        let now_ns = self.clock.now_ns();
        let mut payloads = Vec::new();

        for (tenant, state) in &mut self.per_tenant {
            state.edges.expire(now_ns);
            let mut series = state.span_metrics.drain(timestamp_ms);
            series.extend(state.edges.drain(timestamp_ms));
            if !series.is_empty() {
                payloads.push(SeriesPayload {
                    tenant: tenant.clone(),
                    series,
                });
            }
        }

        payloads
    }

    #[must_use]
    pub fn edge_checkpoints(&self) -> Vec<TenantEdgeCheckpoints> {
        let mut checkpoints: Vec<_> = self
            .per_tenant
            .iter()
            .map(|(tenant, state)| (tenant.clone(), state.edges.checkpoint_entries(tenant)))
            .collect();
        checkpoints.sort_by(|a, b| a.0.cmp(&b.0));
        checkpoints
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crabka_units::{ByteSize, convert::ByteSizeExt as _};

    use super::*;
    use crate::metricsgen::{
        clock::MockClock,
        config::MetricsGenConfig,
        contract::{SpanKind, SpanRecord, StatusCode},
    };

    fn span(
        tenant: &str,
        service: &str,
        kind: SpanKind,
        span_id: [u8; 8],
        parent: [u8; 8],
    ) -> SpanRecord {
        SpanRecord {
            tenant: tenant.into(),
            trace_id: [0x33; 16],
            span_id,
            parent_span_id: parent,
            name: "op".into(),
            kind,
            start_ns: 0,
            duration_ns: 5_000_000,
            status: StatusCode::Ok,
            status_message: String::new(),
            service_name: service.into(),
            attributes: vec![],
            size: ByteSize::from_bytes(10),
        }
    }

    #[tokio::test]
    async fn process_then_collect_emits_both_processors_per_tenant() {
        let clock = MockClock::new(0);
        let mut generator = MetricsGenerator::new(MetricsGenConfig::default(), Arc::new(clock));

        generator.process(&span("A", "frontend", SpanKind::Client, [0xA; 8], [0; 8]));
        generator.process(&span("A", "backend", SpanKind::Server, [0xB; 8], [0xA; 8]));
        generator.process(&span("B", "svc", SpanKind::Server, [0xC; 8], [0; 8]));

        let payloads = generator.collect(1_000);
        assert2::assert!(payloads.len() == 2);

        let a = payloads.iter().find(|p| p.tenant == "A").unwrap();
        assert2::assert!(
            a.series
                .iter()
                .any(|s| s.name == "traces_service_graph_request_total")
        );
        assert2::assert!(
            a.series
                .iter()
                .any(|s| s.name == "traces_spanmetrics_calls_total")
        );

        let b = payloads.iter().find(|p| p.tenant == "B").unwrap();
        assert2::assert!(
            b.series
                .iter()
                .any(|s| s.name == "traces_spanmetrics_calls_total")
        );
        assert2::assert!(
            !b.series
                .iter()
                .any(|s| s.name == "traces_service_graph_request_total")
        );
    }

    #[tokio::test]
    async fn collect_expires_stale_edges_via_clock() {
        let clock = MockClock::new(0);
        let mut generator =
            MetricsGenerator::new(MetricsGenConfig::default(), Arc::new(clock.clone()));
        generator.process(&span("A", "frontend", SpanKind::Client, [0xA; 8], [0; 8]));

        clock.set(11_000_000_000);
        let payloads = generator.collect(2_000);
        let a = payloads.iter().find(|p| p.tenant == "A").unwrap();
        assert2::assert!(
            a.series
                .iter()
                .any(|s| s.name == "traces_service_graph_unpaired_spans_total")
        );
    }
}
