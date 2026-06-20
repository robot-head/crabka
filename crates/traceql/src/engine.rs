//! Public `TraceQL` engine.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::arrow::array::AsArray;

use crate::error::{Result, TraceqlError};
use crate::parser::parse;
use crate::planner::{PlannerContext, plan_query};
use crate::result::{
    AttrValue, ScopedTag, SearchResponse, SpanRef, SpanSet, TagScope, TraceMetricsResponse,
    TraceResult, TraceSpans, TypedValue,
};
use crate::span_columns::{
    ATTR_PREFIX, COL_DURATION, COL_NAME, COL_PARENT_SPAN_ID, COL_ROOT_SERVICE_NAME,
    COL_ROOT_SPAN_NAME, COL_SPAN_ID, COL_START, COL_TRACE_DURATION, COL_TRACE_ID, COL_TRACE_START,
};
use crate::store::SpanStore;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineOpts {
    pub default_limit: usize,
    pub default_spss: usize,
    pub max_traces: usize,
}

impl Default for EngineOpts {
    fn default() -> Self {
        Self {
            default_limit: 20,
            default_spss: 3,
            max_traces: 1000,
        }
    }
}

pub struct TraceqlEngine<S: SpanStore> {
    store: Arc<S>,
    opts: EngineOpts,
}

impl<S: SpanStore> TraceqlEngine<S> {
    #[must_use]
    pub fn new(store: Arc<S>, opts: EngineOpts) -> Self {
        Self { store, opts }
    }

    #[must_use]
    pub fn store(&self) -> &Arc<S> {
        &self.store
    }

    pub async fn search(
        &self,
        tenant: &str,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
    ) -> Result<SearchResponse> {
        let q = parse(query)?;
        let planned = plan_query(
            self.store.as_ref(),
            &PlannerContext {
                tenant: tenant.to_string(),
                start_ns,
                end_ns,
            },
            &q,
        )
        .await?;
        let batches = planned
            .ctx
            .execute_logical_plan(planned.plan)
            .await?
            .collect()
            .await?;
        let effective_limit = if limit == 0 {
            self.opts.default_limit
        } else {
            limit
        }
        .min(self.opts.max_traces);
        assemble_search_response(&batches, effective_limit, self.opts.default_spss)
    }

    pub async fn query_range(
        &self,
        _tenant: &str,
        _query: &str,
        _start_ns: i64,
        _end_ns: i64,
        _step_ns: i64,
    ) -> Result<TraceMetricsResponse> {
        std::future::ready(()).await;
        Err(TraceqlError::Unsupported("traceql metrics: slice 3".into()))
    }

    pub async fn trace_by_id(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
    ) -> Result<Option<TraceSpans>> {
        self.store.trace_by_id(tenant, trace_id).await
    }

    pub async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>> {
        self.store.tag_names(tenant, scope, start_ns, end_ns).await
    }

    pub async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>> {
        self.store.tag_values(tenant, tag, start_ns, end_ns).await
    }
}

struct TraceAcc {
    root_service_name: String,
    root_trace_name: String,
    start_time_unix_nano: u64,
    duration_ms: u64,
    spans: Vec<SpanRef>,
}

pub(crate) fn assemble_search_response(
    batches: &[RecordBatch],
    limit: usize,
    spss: usize,
) -> Result<SearchResponse> {
    let mut traces: BTreeMap<[u8; 16], TraceAcc> = BTreeMap::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let trace_id = fixed_16(batch, COL_TRACE_ID, row)?;
            let span = SpanRef {
                span_id: fixed_8(batch, COL_SPAN_ID, row)?,
                parent_span_id: optional_fixed_8(batch, COL_PARENT_SPAN_ID, row)?,
                name: string_value(batch, COL_NAME, row).unwrap_or_default(),
                start_time_unix_nano: u64_from_i64(i64_value(batch, COL_START, row)?)?,
                duration_nanos: u64_from_i64(i64_value(batch, COL_DURATION, row)?)?,
                attributes: row_attrs(batch, row)?,
            };
            traces
                .entry(trace_id)
                .or_insert_with(|| TraceAcc {
                    root_service_name: string_value(batch, COL_ROOT_SERVICE_NAME, row)
                        .unwrap_or_default(),
                    root_trace_name: string_value(batch, COL_ROOT_SPAN_NAME, row)
                        .unwrap_or_default(),
                    start_time_unix_nano: u64_from_i64(
                        i64_value(batch, COL_TRACE_START, row).unwrap_or_default(),
                    )
                    .unwrap_or_default(),
                    duration_ms: u64_from_i64(
                        i64_value(batch, COL_TRACE_DURATION, row).unwrap_or_default(),
                    )
                    .unwrap_or_default()
                        / 1_000_000,
                    spans: Vec::new(),
                })
                .spans
                .push(span);
        }
    }

    let mut out: Vec<TraceResult> = traces
        .into_iter()
        .map(|(trace_id, mut acc)| {
            acc.spans
                .sort_by_key(|s| (s.start_time_unix_nano, s.span_id));
            let matched = u32::try_from(acc.spans.len()).unwrap_or(u32::MAX);
            let spans = acc.spans.into_iter().take(spss).collect();
            TraceResult {
                trace_id,
                root_service_name: acc.root_service_name,
                root_trace_name: acc.root_trace_name,
                start_time_unix_nano: acc.start_time_unix_nano,
                duration_ms: acc.duration_ms,
                span_sets: vec![SpanSet { spans, matched }],
            }
        })
        .collect();
    out.sort_by_key(|t| (t.start_time_unix_nano, t.trace_id));
    out.truncate(limit);
    Ok(SearchResponse { traces: out })
}

fn fixed_16(batch: &RecordBatch, col: &str, row: usize) -> Result<[u8; 16]> {
    batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?
        .as_fixed_size_binary()
        .value(row)
        .try_into()
        .map_err(|_| TraceqlError::Exec(format!("column {col} is not 16 bytes")))
}

fn fixed_8(batch: &RecordBatch, col: &str, row: usize) -> Result<[u8; 8]> {
    batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?
        .as_fixed_size_binary()
        .value(row)
        .try_into()
        .map_err(|_| TraceqlError::Exec(format!("column {col} is not 8 bytes")))
}

fn optional_fixed_8(batch: &RecordBatch, col: &str, row: usize) -> Result<Option<[u8; 8]>> {
    let arr = batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?;
    if arr.is_null(row) {
        return Ok(None);
    }
    arr.as_fixed_size_binary()
        .value(row)
        .try_into()
        .map(Some)
        .map_err(|_| TraceqlError::Exec(format!("column {col} is not 8 bytes")))
}

fn i64_value(batch: &RecordBatch, col: &str, row: usize) -> Result<i64> {
    Ok(batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?
        .as_primitive::<arrow::datatypes::Int64Type>()
        .value(row))
}

fn string_value(batch: &RecordBatch, col: &str, row: usize) -> Option<String> {
    let arr = batch.column_by_name(col)?.as_string::<i32>();
    (!arr.is_null(row)).then(|| arr.value(row).to_string())
}

fn row_attrs(batch: &RecordBatch, row: usize) -> Result<Vec<(String, AttrValue)>> {
    let schema = batch.schema();
    let mut attrs = Vec::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        let Some(name) = field.name().strip_prefix(ATTR_PREFIX) else {
            continue;
        };
        let array = batch.column(idx);
        if array.is_null(row) {
            continue;
        }
        let value = match field.data_type() {
            DataType::Utf8 => AttrValue::Str(array.as_string::<i32>().value(row).to_string()),
            DataType::Int64 => AttrValue::Int(
                array
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .value(row),
            ),
            DataType::Float64 => AttrValue::Float(
                array
                    .as_primitive::<arrow::datatypes::Float64Type>()
                    .value(row),
            ),
            DataType::Boolean => AttrValue::Bool(array.as_boolean().value(row)),
            other => {
                return Err(TraceqlError::Exec(format!(
                    "unsupported attribute column type {other:?}"
                )));
            }
        };
        attrs.push((name.to_string(), value));
    }
    attrs.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(attrs)
}

fn u64_from_i64(v: i64) -> Result<u64> {
    u64::try_from(v).map_err(|e| TraceqlError::Exec(e.to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;

    use super::*;
    use crate::TraceqlError;
    use crate::in_memory::InMemorySpanStore;
    use crate::result::AttrValue;
    use crate::span_columns::InputSpan;

    fn sp(tid: u8, id: u8, parent: Option<u8>, svc: &str) -> InputSpan {
        InputSpan {
            trace_id: [tid; 16],
            span_id: [id; 8],
            parent_span_id: parent.map(|p| [p; 8]),
            name: format!("op-{id}"),
            kind: 0,
            start_unix_nano: 1000 + i64::from(id),
            duration_nanos: 200,
            status_code: 0,
            status_message: String::new(),
            attrs: vec![("svc".into(), AttrValue::Str(svc.into()))],
        }
    }

    fn engine() -> TraceqlEngine<InMemorySpanStore> {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![sp(9, 1, None, "a"), sp(9, 2, Some(1), "b")],
        );
        s.push_trace("t", "x", "root", vec![sp(8, 1, None, "x")]);
        TraceqlEngine::new(Arc::new(s), EngineOpts::default())
    }

    #[tokio::test]
    async fn search_selector_returns_matching_trace() {
        let e = engine();
        let r = e
            .search("t", "{ .svc = \"b\" }", 0, 100_000, 20)
            .await
            .unwrap();
        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [9; 16]);
        assert!(r.traces[0].root_service_name == "a");
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [2; 8]);
    }

    #[tokio::test]
    async fn search_inter_brace_and_matches_different_spans() {
        let e = engine();
        let r = e
            .search("t", "{ .svc = \"a\" } && { .svc = \"b\" }", 0, 100_000, 20)
            .await
            .unwrap();
        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [9; 16]);
        assert!(r.traces[0].span_sets[0].matched == 2);
    }

    #[tokio::test]
    async fn search_descendant_structural() {
        let e = engine();
        let r = e
            .search("t", "{ .svc = \"b\" } >> { .svc = \"a\" }", 0, 100_000, 20)
            .await
            .unwrap();
        assert!(r.traces.len() == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [2; 8]);
    }

    #[tokio::test]
    async fn search_limit_uses_default_for_zero_and_caps_result_count() {
        let e = engine();
        let r = e
            .search("t", "{ .svc != nil }", 0, 100_000, 1)
            .await
            .unwrap();
        assert!(r.traces.len() == 1);
    }

    #[tokio::test]
    async fn trace_by_id_path() {
        let e = engine();
        let got = e.trace_by_id("t", &[9; 16]).await.unwrap().unwrap();
        assert!(got.spans.len() == 2);
        assert!(e.trace_by_id("t", &[1; 16]).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn query_range_is_unsupported_in_slice2() {
        let e = engine();
        let err = e.query_range("t", "{ } | rate()", 0, 100_000, 10_000).await;
        assert!(matches!(err, Err(TraceqlError::Unsupported(_))));
    }
}
