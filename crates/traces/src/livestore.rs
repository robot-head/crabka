//! Recent trace hot tier for the traces backend.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use crabka_client_consumer::Consumer;
use datafusion::catalog::MemTable;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::error::TracesError;
use crate::querier::live::{LiveSource, Result as LiveResult};
use crate::span::{AttrValue, KeyValue, Span, batch::span_batch};
use crate::wal::SpanRecord;

/// In-memory recent span store keyed by tenant and trace id.
#[derive(Debug)]
pub struct LiveStore {
    retention_ns: i64,
    max_start_ns: i64,
    by_tenant: BTreeMap<String, BTreeMap<[u8; 16], Vec<Span>>>,
}

impl LiveStore {
    /// Create a live store that retains spans within `retention_ns` of the
    /// newest ingested span timestamp.
    #[must_use]
    pub fn new(retention_ns: i64) -> Self {
        Self {
            retention_ns,
            max_start_ns: i64::MIN,
            by_tenant: BTreeMap::new(),
        }
    }

    /// Append a WAL span record and evict spans older than the retention window.
    pub fn ingest(&mut self, rec: SpanRecord) {
        self.max_start_ns = self.max_start_ns.max(rec.span.start_ns);
        self.by_tenant
            .entry(rec.tenant)
            .or_default()
            .entry(rec.span.trace_id)
            .or_default()
            .push(rec.span);
        self.evict_old();
    }

    /// Return recent spans for one trace ordered by start time and span id.
    #[must_use]
    pub fn trace_by_id(&self, tenant: &str, trace_id: &[u8; 16]) -> Vec<Span> {
        let mut spans = self
            .by_tenant
            .get(tenant)
            .and_then(|traces| traces.get(trace_id))
            .cloned()
            .unwrap_or_default();
        order_spans(&mut spans);
        spans
    }

    /// Expose a tenant's recent spans as a `DataFusion` `MemTable`.
    pub fn mem_table(&self, tenant: &str) -> Result<MemTable, TracesError> {
        let schema = crabka_blockstore::span_block_schema();
        let mut batches = Vec::new();
        if let Some(traces) = self.by_tenant.get(tenant) {
            for spans in traces.values() {
                let mut ordered = spans.clone();
                order_spans(&mut ordered);
                batches.push(span_batch(&ordered)?);
            }
        }
        MemTable::try_new(schema, vec![batches]).map_err(|err| TracesError::Block(err.to_string()))
    }

    fn evict_old(&mut self) {
        if self.retention_ns == i64::MAX || self.max_start_ns == i64::MIN {
            return;
        }
        let cutoff = self.max_start_ns.saturating_sub(self.retention_ns);
        self.by_tenant.retain(|_, traces| {
            traces.retain(|_, spans| {
                spans.retain(|span| span.start_ns >= cutoff);
                !spans.is_empty()
            });
            !traces.is_empty()
        });
    }
}

/// Decode WAL payloads and ingest them into the live store.
pub fn ingest_wal_payloads<'a, I>(store: &mut LiveStore, payloads: I) -> Result<usize, TracesError>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    let mut count = 0;
    for payload in payloads {
        store.ingest(SpanRecord::decode(payload)?);
        count += 1;
    }
    Ok(count)
}

/// Consume traces WAL records and rebuild the in-memory hot tier.
pub async fn run(
    mut consumer: Consumer,
    store: Arc<RwLock<LiveStore>>,
    shutdown: CancellationToken,
) -> Result<(), TracesError> {
    while !shutdown.is_cancelled() {
        let records = consumer
            .poll(Duration::from_millis(500))
            .await
            .map_err(|err| TracesError::Wal(err.to_string()))?;
        if records.is_empty() {
            continue;
        }

        {
            let payloads = records
                .iter()
                .filter_map(|record| record.value.as_deref())
                .collect::<Vec<_>>();
            let mut guard = store.write().await;
            ingest_wal_payloads(&mut guard, payloads)?;
        }

        let _ = consumer.commit_sync().await;
    }
    Ok(())
}

#[async_trait::async_trait]
impl LiveSource for LiveStore {
    async fn span_batches(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> LiveResult<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        if let Some(traces) = self.by_tenant.get(tenant) {
            for spans in traces.values() {
                let mut in_range = spans
                    .iter()
                    .filter(|span| in_time_range(span, start_ns, end_ns))
                    .cloned()
                    .collect::<Vec<_>>();
                if !in_range.is_empty() {
                    order_spans(&mut in_range);
                    batches.push(
                        span_batch(&in_range)
                            .map_err(|err| crabka_traceql::TraceqlError::Store(err.to_string()))?,
                    );
                }
            }
        }
        Ok(batches)
    }

    async fn trace_spans(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
    ) -> LiveResult<Option<crabka_traceql::TraceSpans>> {
        let spans = self.trace_by_id(tenant, trace_id);
        Ok((!spans.is_empty()).then(|| trace_spans(trace_id, &spans)))
    }

    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<crabka_traceql::TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> LiveResult<Vec<crabka_traceql::ScopedTag>> {
        let mut resource = BTreeSet::new();
        let mut span = BTreeSet::new();
        if let Some(traces) = self.by_tenant.get(tenant) {
            for item in traces
                .values()
                .flatten()
                .filter(|item| in_time_range(item, start_ns, end_ns))
            {
                resource.extend(item.resource_attrs.iter().map(|attr| attr.key.clone()));
                span.extend(item.span_attrs.iter().map(|attr| attr.key.clone()));
            }
        }

        let mut out = Vec::new();
        if matches!(scope, None | Some(crabka_traceql::TagScope::Resource)) && !resource.is_empty()
        {
            out.push(crabka_traceql::ScopedTag {
                scope: crabka_traceql::TagScope::Resource,
                tags: resource.into_iter().collect(),
            });
        }
        if matches!(scope, None | Some(crabka_traceql::TagScope::Span)) && !span.is_empty() {
            out.push(crabka_traceql::ScopedTag {
                scope: crabka_traceql::TagScope::Span,
                tags: span.into_iter().collect(),
            });
        }
        Ok(out)
    }

    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> LiveResult<Vec<crabka_traceql::TypedValue>> {
        let tag = tag.strip_prefix('.').unwrap_or(tag);
        let mut values = BTreeSet::new();
        if let Some(traces) = self.by_tenant.get(tenant) {
            for span in traces
                .values()
                .flatten()
                .filter(|item| in_time_range(item, start_ns, end_ns))
            {
                values.extend(
                    span.resource_attrs
                        .iter()
                        .chain(&span.span_attrs)
                        .filter(|attr| attr.key == tag)
                        .map(|attr| typed_value_parts(&attr.value)),
                );
            }
        }
        Ok(values
            .into_iter()
            .map(|(type_, value)| crabka_traceql::TypedValue { type_, value })
            .collect())
    }

    fn block_builder_frontier_ns(&self, _tenant: &str) -> i64 {
        if self.max_start_ns == i64::MIN {
            0
        } else {
            self.max_start_ns
        }
    }
}

fn order_spans(spans: &mut [Span]) {
    spans.sort_by_key(|span| (span.start_ns, span.span_id));
}

fn in_time_range(span: &Span, start_ns: i64, end_ns: i64) -> bool {
    start_ns <= span.start_ns && span.start_ns <= end_ns
}

fn trace_spans(trace_id: &[u8; 16], spans: &[Span]) -> crabka_traceql::TraceSpans {
    let root = spans
        .iter()
        .find(|span| span.is_root())
        .or_else(|| spans.iter().min_by_key(|span| span.start_ns));
    crabka_traceql::TraceSpans {
        trace_id: *trace_id,
        root_service_name: root
            .and_then(|span| attr_string(&span.resource_attrs, "service.name"))
            .unwrap_or_default(),
        root_trace_name: root.map(|span| span.name.clone()).unwrap_or_default(),
        spans: spans.iter().map(span_ref).collect(),
    }
}

fn span_ref(span: &Span) -> crabka_traceql::SpanRef {
    crabka_traceql::SpanRef {
        span_id: span.span_id,
        parent_span_id: span.parent_span_id,
        name: span.name.clone(),
        kind: span.kind.as_i32(),
        start_time_unix_nano: non_negative_u64(span.start_ns),
        duration_nanos: non_negative_u64(span.duration_ns),
        status_code: span.status.as_i32(),
        status_message: span.status_message.clone(),
        attributes: span
            .resource_attrs
            .iter()
            .chain(&span.span_attrs)
            .filter_map(|attr| traceql_attr(attr).map(|value| (attr.key.clone(), value)))
            .collect(),
    }
}

fn attr_string(attrs: &[KeyValue], key: &str) -> Option<String> {
    attrs.iter().find_map(|attr| {
        (attr.key == key).then(|| match &attr.value {
            AttrValue::Str(value) => Some(value.clone()),
            _ => None,
        })?
    })
}

fn traceql_attr(attr: &KeyValue) -> Option<crabka_traceql::AttrValue> {
    Some(match &attr.value {
        AttrValue::Str(value) => crabka_traceql::AttrValue::Str(value.clone()),
        AttrValue::Int(value) => crabka_traceql::AttrValue::Int(*value),
        AttrValue::Double(value) => crabka_traceql::AttrValue::Float(*value),
        AttrValue::Bool(value) => crabka_traceql::AttrValue::Bool(*value),
        AttrValue::Bytes(_) => return None,
    })
}

fn typed_value_parts(value: &AttrValue) -> (String, String) {
    match value {
        AttrValue::Str(value) => ("string".into(), value.clone()),
        AttrValue::Int(value) => ("int".into(), value.to_string()),
        AttrValue::Double(value) => ("float".into(), value.to_string()),
        AttrValue::Bool(value) => ("bool".into(), value.to_string()),
        AttrValue::Bytes(value) => ("string".into(), hex::encode(value)),
    }
}

fn non_negative_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}
