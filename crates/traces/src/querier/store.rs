//! `SpanStore` implementation over cold span blocks plus the live tier.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array,
    LargeStringArray, StringArray, StringViewArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use crabka_blockstore::{BlockIndex, BlockStore, TraceIndex, span_block_schema};
use crabka_traceql::{
    ATTR_PREFIX, AttrValue, COL_DURATION, COL_KIND, COL_NAME, COL_NS_LEFT, COL_NS_RIGHT,
    COL_PARENT_ID, COL_PARENT_SPAN_ID, COL_ROOT_SERVICE_NAME, COL_ROOT_SPAN_NAME, COL_SPAN_ID,
    COL_START, COL_STATUS_CODE, COL_STATUS_MESSAGE, COL_TRACE_DURATION, COL_TRACE_ID, ScanResult,
    ScopedTag, SpanMatcher, SpanRef, SpanStore, TagScope, TraceSpans, TraceqlError, TypedValue,
    span_schema,
};
use datafusion::catalog::MemTable;
use datafusion::prelude::SessionContext;

use crate::querier::live::LiveTier;

/// Query-side span store that merges sealed blocks with an optional live tier.
pub struct CrabkaSpanStore {
    blocks: Arc<BlockStore>,
    trace_index: Arc<TraceIndex>,
    live: Option<LiveTier>,
}

impl CrabkaSpanStore {
    #[must_use]
    pub fn new(
        blocks: Arc<BlockStore>,
        trace_index: Arc<TraceIndex>,
        live: Option<LiveTier>,
    ) -> Self {
        Self {
            blocks,
            trace_index,
            live,
        }
    }

    async fn cold_batches(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<RecordBatch>, TraceqlError> {
        if end_ns < start_ns {
            return Ok(Vec::new());
        }
        let keys = self.trace_index.candidate_blocks(tenant, start_ns, end_ns);
        let (ctx, table) = self
            .blocks
            .scan_block_keys(&keys, span_block_schema())
            .await
            .map_err(|err| block_err(&err))?;
        collect_table(&ctx, &table).await
    }
}

#[async_trait::async_trait]
impl SpanStore for CrabkaSpanStore {
    async fn scan(
        &self,
        tenant: &str,
        _matchers: &[SpanMatcher],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<ScanResult, TraceqlError> {
        let (cold_end, live_start) = self.live.as_ref().map_or((end_ns, end_ns + 1), |live| {
            let frontier = live.block_builder_frontier_ns(tenant);
            (
                end_ns.min(frontier.saturating_sub(1)),
                start_ns.max(frontier),
            )
        });

        let mut batches = self.cold_batches(tenant, start_ns, cold_end).await?;
        if let Some(live) = &self.live
            && live_start <= end_ns
        {
            batches.extend(live.span_batches(tenant, live_start, end_ns).await?);
        }

        let schema = batches
            .first()
            .map_or_else(span_schema, RecordBatch::schema);
        let partitions = if batches.is_empty() {
            vec![vec![]]
        } else {
            vec![batches]
        };
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, partitions)?;
        ctx.register_table("spans", Arc::new(table))?;
        Ok(ScanResult {
            ctx,
            span_table: "spans".into(),
        })
    }

    async fn trace_by_id(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
    ) -> Result<Option<TraceSpans>, TraceqlError> {
        let keys = self
            .trace_index
            .candidate_blocks_for_trace(tenant, trace_id, 0, i64::MAX);
        let (ctx, table) = self
            .blocks
            .scan_block_keys(&keys, span_block_schema())
            .await
            .map_err(|err| block_err(&err))?;
        let mut spans = trace_from_batches(trace_id, collect_table(&ctx, &table).await?)?;

        if let Some(live_trace) = match &self.live {
            Some(live) => live.trace_spans(tenant, trace_id).await?,
            None => None,
        } {
            if spans.is_none() {
                spans = Some(TraceSpans {
                    trace_id: live_trace.trace_id,
                    root_service_name: live_trace.root_service_name.clone(),
                    root_trace_name: live_trace.root_trace_name.clone(),
                    spans: Vec::new(),
                });
            }
            if let Some(out) = &mut spans {
                out.spans.extend(live_trace.spans);
                out.spans.sort_by_key(|span| span.start_time_unix_nano);
            }
        }

        Ok(spans)
    }

    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>, TraceqlError> {
        let mut by_scope: BTreeMap<&'static str, (TagScope, BTreeSet<String>)> = BTreeMap::new();
        if matches!(scope, None | Some(TagScope::Span)) {
            by_scope.insert("span", (TagScope::Span, BTreeSet::new()));
            for tag in self.trace_index.tag_names(tenant, start_ns, end_ns) {
                by_scope.get_mut("span").expect("span scope").1.insert(tag);
            }
        }
        if let Some(live) = &self.live {
            for scoped in live.tag_names(tenant, scope, start_ns, end_ns).await? {
                let key = tag_scope_key(scoped.scope);
                let (_, tags) = by_scope
                    .entry(key)
                    .or_insert((scoped.scope, BTreeSet::new()));
                tags.extend(scoped.tags);
            }
        }
        Ok(by_scope
            .into_values()
            .filter_map(|(scope, tags)| {
                (!tags.is_empty()).then_some(ScopedTag {
                    scope,
                    tags: tags.into_iter().collect(),
                })
            })
            .collect())
    }

    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>, TraceqlError> {
        let tag = tag.strip_prefix('.').unwrap_or(tag);
        if is_intrinsic_tag(tag) {
            let scan = self.scan(tenant, &[], start_ns, end_ns).await?;
            let batches = collect_table(&scan.ctx, &scan.span_table).await?;
            return intrinsic_values_from_batches(tag, &batches);
        }
        let mut values: BTreeSet<(String, String)> = self
            .trace_index
            .tag_values(tenant, tag, start_ns, end_ns)
            .into_iter()
            .map(|value| ("string".to_string(), value))
            .collect();
        if let Some(live) = &self.live {
            values.extend(
                live.tag_values(tenant, tag, start_ns, end_ns)
                    .await?
                    .into_iter()
                    .map(|value| (value.type_, value.value)),
            );
        }
        Ok(values
            .into_iter()
            .map(|(type_, value)| TypedValue { type_, value })
            .collect())
    }
}

async fn collect_table(
    ctx: &SessionContext,
    table: &str,
) -> Result<Vec<RecordBatch>, TraceqlError> {
    Ok(ctx.table(table).await?.collect().await?)
}

fn trace_from_batches(
    trace_id: &[u8; 16],
    batches: Vec<RecordBatch>,
) -> Result<Option<TraceSpans>, TraceqlError> {
    let mut root_service_name = String::new();
    let mut root_trace_name = String::new();
    let mut spans = Vec::new();

    for batch in batches {
        let trace_ids = fixed(&batch, COL_TRACE_ID)?;
        for row in 0..batch.num_rows() {
            if trace_ids.value(row) != trace_id {
                continue;
            }
            if root_service_name.is_empty() {
                root_service_name = string_value(&batch, COL_ROOT_SERVICE_NAME, row)?;
            }
            if root_trace_name.is_empty() {
                root_trace_name = string_value(&batch, COL_ROOT_SPAN_NAME, row)?;
            }
            spans.push(SpanRef {
                span_id: fixed_value::<8>(&batch, COL_SPAN_ID, row)?,
                parent_span_id: nullable_fixed_value::<8>(&batch, COL_PARENT_SPAN_ID, row)?,
                name: string_value(&batch, COL_NAME, row)?,
                kind: int32_value(&batch, COL_KIND, row)?,
                nested_set_left: int32_value(&batch, COL_NS_LEFT, row)?,
                nested_set_right: int32_value(&batch, COL_NS_RIGHT, row)?,
                nested_set_parent: int32_value(&batch, COL_PARENT_ID, row)?,
                start_time_unix_nano: u64::try_from(int64_value(&batch, COL_START, row)?)
                    .unwrap_or(0),
                duration_nanos: u64::try_from(int64_value(&batch, COL_DURATION, row)?).unwrap_or(0),
                status_code: int32_value(&batch, COL_STATUS_CODE, row)?,
                status_message: string_value(&batch, COL_STATUS_MESSAGE, row)?,
                attributes: attr_values(&batch, row)?,
            });
        }
    }

    spans.sort_by_key(|span| span.start_time_unix_nano);
    Ok((!spans.is_empty()).then_some(TraceSpans {
        trace_id: *trace_id,
        root_service_name,
        root_trace_name,
        spans,
    }))
}

fn is_intrinsic_tag(tag: &str) -> bool {
    tag.contains(':')
}

fn intrinsic_values_from_batches(
    tag: &str,
    batches: &[RecordBatch],
) -> Result<Vec<TypedValue>, TraceqlError> {
    let mut values = BTreeSet::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            collect_intrinsic_value(batch, row, tag, &mut values)?;
        }
    }
    Ok(values
        .into_iter()
        .map(|(type_, value)| TypedValue { type_, value })
        .collect())
}

fn collect_intrinsic_value(
    batch: &RecordBatch,
    row: usize,
    tag: &str,
    values: &mut BTreeSet<(String, String)>,
) -> Result<(), TraceqlError> {
    match tag {
        "span:duration" => {
            values.insert((
                "duration".to_string(),
                int64_value(batch, COL_DURATION, row)?.to_string(),
            ));
        }
        "span:id" => {
            values.insert((
                "string".to_string(),
                bytes_to_hex(fixed(batch, COL_SPAN_ID)?.value(row)),
            ));
        }
        "span:kind" => {
            values.insert((
                "int".to_string(),
                int32_value(batch, COL_KIND, row)?.to_string(),
            ));
        }
        "span:name" => {
            values.insert(("string".to_string(), string_value(batch, COL_NAME, row)?));
        }
        "span:parentID" => {
            if let Some(parent_id) = nullable_fixed_value::<8>(batch, COL_PARENT_SPAN_ID, row)? {
                values.insert(("string".to_string(), bytes_to_hex(&parent_id)));
            }
        }
        "span:status" => {
            values.insert((
                "int".to_string(),
                int32_value(batch, COL_STATUS_CODE, row)?.to_string(),
            ));
        }
        "span:statusMessage" => {
            let message = string_value(batch, COL_STATUS_MESSAGE, row)?;
            if !message.is_empty() {
                values.insert(("string".to_string(), message));
            }
        }
        "span:nestedSetLeft" => {
            values.insert((
                "int".to_string(),
                int32_value(batch, COL_NS_LEFT, row)?.to_string(),
            ));
        }
        "span:nestedSetParent" => {
            values.insert((
                "int".to_string(),
                int32_value(batch, COL_PARENT_ID, row)?.to_string(),
            ));
        }
        "span:nestedSetRight" => {
            values.insert((
                "int".to_string(),
                int32_value(batch, COL_NS_RIGHT, row)?.to_string(),
            ));
        }
        "trace:duration" => {
            values.insert((
                "duration".to_string(),
                int64_value(batch, COL_TRACE_DURATION, row)?.to_string(),
            ));
        }
        "trace:id" => {
            values.insert((
                "string".to_string(),
                bytes_to_hex(fixed(batch, COL_TRACE_ID)?.value(row)),
            ));
        }
        "trace:rootName" => {
            values.insert((
                "string".to_string(),
                string_value(batch, COL_ROOT_SPAN_NAME, row)?,
            ));
        }
        "trace:rootService" => {
            values.insert((
                "string".to_string(),
                string_value(batch, COL_ROOT_SERVICE_NAME, row)?,
            ));
        }
        _ => {}
    }
    Ok(())
}

fn attr_values(batch: &RecordBatch, row: usize) -> Result<Vec<(String, AttrValue)>, TraceqlError> {
    let mut out = Vec::new();
    for (idx, field) in batch.schema().fields().iter().enumerate() {
        let Some(key) = field.name().strip_prefix(ATTR_PREFIX) else {
            continue;
        };
        let col = batch.column(idx);
        if col.is_null(row) {
            continue;
        }
        let value = match field.data_type() {
            DataType::Utf8 => AttrValue::Str(string_array_value(col.as_ref(), row)?),
            DataType::Int64 => AttrValue::Int(int64_array_value(col.as_ref(), row)?),
            DataType::Float64 => AttrValue::Float(float64_array_value(col.as_ref(), row)?),
            DataType::Boolean => AttrValue::Bool(bool_array_value(col.as_ref(), row)?),
            _ => continue,
        };
        out.push((key.to_string(), value));
    }
    Ok(out)
}

fn fixed<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a FixedSizeBinaryArray, TraceqlError> {
    batch
        .column_by_name(name)
        .and_then(|col| col.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| TraceqlError::Store(format!("missing fixed binary column `{name}`")))
}

fn fixed_value<const N: usize>(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<[u8; N], TraceqlError> {
    fixed(batch, name)?
        .value(row)
        .try_into()
        .map_err(|_| TraceqlError::Store(format!("bad fixed binary width for `{name}`")))
}

fn nullable_fixed_value<const N: usize>(
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<Option<[u8; N]>, TraceqlError> {
    let arr = fixed(batch, name)?;
    if arr.is_null(row) {
        Ok(None)
    } else {
        arr.value(row)
            .try_into()
            .map(Some)
            .map_err(|_| TraceqlError::Store(format!("bad fixed binary width for `{name}`")))
    }
}

fn string_value(batch: &RecordBatch, name: &str, row: usize) -> Result<String, TraceqlError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| TraceqlError::Store(format!("missing string column `{name}`")))?;
    if col.is_null(row) {
        return Ok(String::new());
    }
    string_array_value(col.as_ref(), row)
}

fn string_array_value(col: &dyn Array, row: usize) -> Result<String, TraceqlError> {
    col.as_any()
        .downcast_ref::<StringArray>()
        .map(|a| a.value(row).to_string())
        .or_else(|| {
            col.as_any()
                .downcast_ref::<LargeStringArray>()
                .map(|a| a.value(row).to_string())
        })
        .or_else(|| {
            col.as_any()
                .downcast_ref::<StringViewArray>()
                .map(|a| a.value(row).to_string())
        })
        .ok_or_else(|| TraceqlError::Store("unsupported string column type".into()))
}

fn int64_value(batch: &RecordBatch, name: &str, row: usize) -> Result<i64, TraceqlError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| TraceqlError::Store(format!("missing int64 column `{name}`")))?;
    int64_array_value(col.as_ref(), row)
}

fn int64_array_value(col: &dyn Array, row: usize) -> Result<i64, TraceqlError> {
    col.as_any()
        .downcast_ref::<Int64Array>()
        .map(|a| a.value(row))
        .ok_or_else(|| TraceqlError::Store("unsupported int64 column type".into()))
}

fn int32_value(batch: &RecordBatch, name: &str, row: usize) -> Result<i32, TraceqlError> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| TraceqlError::Store(format!("missing int32 column `{name}`")))?;
    col.as_any()
        .downcast_ref::<Int32Array>()
        .map(|a| a.value(row))
        .ok_or_else(|| TraceqlError::Store("unsupported int32 column type".into()))
}

fn float64_array_value(col: &dyn Array, row: usize) -> Result<f64, TraceqlError> {
    col.as_any()
        .downcast_ref::<Float64Array>()
        .map(|a| a.value(row))
        .ok_or_else(|| TraceqlError::Store("unsupported float64 column type".into()))
}

fn bool_array_value(col: &dyn Array, row: usize) -> Result<bool, TraceqlError> {
    col.as_any()
        .downcast_ref::<BooleanArray>()
        .map(|a| a.value(row))
        .ok_or_else(|| TraceqlError::Store("unsupported bool column type".into()))
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

fn tag_scope_key(scope: TagScope) -> &'static str {
    match scope {
        TagScope::Resource => "resource",
        TagScope::Span => "span",
        TagScope::Intrinsic => "intrinsic",
        TagScope::Event => "event",
        TagScope::Link => "link",
        TagScope::Instrumentation => "instrumentation",
    }
}

fn block_err(err: &crabka_blockstore::BlockStoreError) -> TraceqlError {
    TraceqlError::Store(err.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use assert2::assert;
    use crabka_blockstore::{ShardedTraceBloom, TraceBlockStats};
    use crabka_traceql::{
        COL_CHILD_COUNT, COL_INSTRUMENTATION_NAME, COL_INSTRUMENTATION_VERSION, EngineOpts,
        TraceqlEngine,
    };
    use object_store::memory::InMemory;
    use url::Url;

    use super::*;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(COL_TRACE_ID, DataType::FixedSizeBinary(16), false),
            Field::new(COL_SPAN_ID, DataType::FixedSizeBinary(8), false),
            Field::new(COL_PARENT_SPAN_ID, DataType::FixedSizeBinary(8), true),
            Field::new("nested_set_left", DataType::Int32, false),
            Field::new("nested_set_right", DataType::Int32, false),
            Field::new("parent_id", DataType::Int32, false),
            Field::new(COL_CHILD_COUNT, DataType::Int32, false),
            Field::new(COL_ROOT_SERVICE_NAME, DataType::Utf8, true),
            Field::new(COL_ROOT_SPAN_NAME, DataType::Utf8, true),
            Field::new("trace_start_unix_nano", DataType::Int64, false),
            Field::new("trace_duration_nanos", DataType::Int64, false),
            Field::new(COL_NAME, DataType::Utf8, true),
            Field::new("kind", DataType::Int32, false),
            Field::new(COL_START, DataType::Int64, false),
            Field::new(COL_DURATION, DataType::Int64, false),
            Field::new("status_code", DataType::Int32, false),
            Field::new("status_message", DataType::Utf8, true),
            Field::new(COL_INSTRUMENTATION_NAME, DataType::Utf8, true),
            Field::new(COL_INSTRUMENTATION_VERSION, DataType::Utf8, true),
            Field::new(format!("{ATTR_PREFIX}svc"), DataType::Utf8, true),
        ]))
    }

    fn batch() -> RecordBatch {
        let schema = test_schema();
        let mut trace_id = FixedSizeBinaryBuilder::with_capacity(2, 16);
        trace_id.append_value([7; 16]).unwrap();
        trace_id.append_value([9; 16]).unwrap();
        let mut span_id = FixedSizeBinaryBuilder::with_capacity(2, 8);
        span_id.append_value([1; 8]).unwrap();
        span_id.append_value([2; 8]).unwrap();
        let mut parent_id = FixedSizeBinaryBuilder::with_capacity(2, 8);
        parent_id.append_null();
        parent_id.append_null();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(trace_id.finish()) as ArrayRef,
                Arc::new(span_id.finish()),
                Arc::new(parent_id.finish()),
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(Int32Array::from(vec![2, 2])),
                Arc::new(Int32Array::from(vec![0, 0])),
                Arc::new(Int32Array::from(vec![0, 0])),
                Arc::new(StringArray::from(vec!["api", "web"])),
                Arc::new(StringArray::from(vec!["GET /", "GET /x"])),
                Arc::new(Int64Array::from(vec![100, 200])),
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec!["root", "other"])),
                Arc::new(Int32Array::from(vec![0, 0])),
                Arc::new(Int64Array::from(vec![100, 200])),
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int32Array::from(vec![0, 0])),
                Arc::new(StringArray::from(vec!["", ""])),
                Arc::new(StringArray::from(vec!["tracer", "tracer"])),
                Arc::new(StringArray::from(vec!["", ""])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn reconstructs_trace_from_candidate_batches() {
        let got = trace_from_batches(&[7; 16], vec![batch()])
            .unwrap()
            .unwrap();
        assert!(got.root_service_name == "api");
        assert!(got.spans.len() == 1);
        assert!(got.spans[0].attributes == vec![("svc".into(), AttrValue::Str("a".into()))]);
    }

    #[tokio::test]
    async fn empty_store_scans_as_empty_span_table() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let store = CrabkaSpanStore::new(blocks, Arc::new(TraceIndex::new()), None);
        let scan = store.scan("tenant", &[], 0, 10).await.unwrap();
        let rows: usize = scan
            .ctx
            .table(&scan.span_table)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert!(rows == 0);
    }

    #[tokio::test]
    async fn tag_discovery_unions_cold_index_values() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let mut index = TraceIndex::new();
        let mut tags = BTreeSet::new();
        tags.insert("service.name".to_string());
        let mut values = BTreeMap::new();
        values.insert(
            "service.name".to_string(),
            BTreeSet::from(["api".to_string()]),
        );
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: "blocks/none.parquet".into(),
                min_ts: 0,
                max_ts: 10,
                bloom: ShardedTraceBloom::new(1, 8, 0.01),
                tag_names: tags,
                tag_values: values,
            },
        );

        let store = CrabkaSpanStore::new(blocks, Arc::new(index), None);
        assert!(
            store.tag_names("tenant", None, 0, 10).await.unwrap()[0].tags == vec!["service.name"]
        );
        assert!(
            store
                .tag_values("tenant", "service.name", 0, 10)
                .await
                .unwrap()[0]
                .value
                == "api"
        );
    }

    #[tokio::test]
    async fn can_back_traceql_engine() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let store = Arc::new(CrabkaSpanStore::new(
            blocks,
            Arc::new(TraceIndex::new()),
            None,
        ));
        let engine = TraceqlEngine::new(store, EngineOpts::default());
        let resp = engine
            .search("tenant", "{ span:name = \"missing\" }", 0, 10, 10)
            .await
            .unwrap();
        assert!(resp.traces.is_empty());
    }
}
