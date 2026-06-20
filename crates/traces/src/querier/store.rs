//! `SpanStore` implementation over cold span blocks plus the live tier.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array,
    LargeStringArray, ListArray, StringArray, StringViewArray, StructArray,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use crabka_blockstore::{
    BlockIndex, BlockStore, SCOL_EVENTS, SCOL_LINKS, TraceIndex, span_block_schema,
};
use crabka_traceql::{
    ATTR_PREFIX, AttrValue, COL_CHILD_COUNT, COL_DURATION, COL_INSTRUMENTATION_NAME,
    COL_INSTRUMENTATION_VERSION, COL_KIND, COL_NAME, COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID,
    COL_PARENT_SPAN_ID, COL_ROOT_SERVICE_NAME, COL_ROOT_SPAN_NAME, COL_SPAN_ID, COL_START,
    COL_STATUS_CODE, COL_STATUS_MESSAGE, COL_TRACE_DURATION, COL_TRACE_ID, EventRef, LinkRef,
    ScanResult, ScopedTag, SpanMatcher, SpanRef, SpanStore, TagScope, TraceSpans, TraceqlError,
    TypedValue, span_schema,
};
use datafusion::catalog::MemTable;
use datafusion::prelude::SessionContext;

use crate::querier::live::LiveTier;

const INTRINSIC_TAGS: &[&str] = &[
    "event:name",
    "event:timeSinceStart",
    "instrumentation:name",
    "instrumentation:version",
    "link:spanID",
    "link:traceID",
    "span:childCount",
    "span:duration",
    "span:id",
    "span:kind",
    "span:name",
    "span:Parent",
    "span:nestedSetLeft",
    "span:nestedSetParent",
    "span:nestedSetRight",
    "span:parentID",
    "span:status",
    "span:statusMessage",
    "trace:duration",
    "trace:id",
    "trace:rootName",
    "trace:rootService",
];
const EVENT_TAGS: &[&str] = &["event:name", "event:timeSinceStart"];
const LINK_TAGS: &[&str] = &["link:spanID", "link:traceID"];
const INSTRUMENTATION_TAGS: &[&str] = &["instrumentation:name", "instrumentation:version"];
const SCOPE_ORDER: &[TagScope] = &[
    TagScope::Resource,
    TagScope::Span,
    TagScope::Intrinsic,
    TagScope::Event,
    TagScope::Link,
    TagScope::Instrumentation,
];

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
        let has_cold_blocks = !self
            .trace_index
            .candidate_blocks(tenant, start_ns, end_ns)
            .is_empty();
        if matches!(scope, None | Some(TagScope::Span)) {
            by_scope.insert("span", (TagScope::Span, BTreeSet::new()));
            for tag in self.trace_index.tag_names(tenant, start_ns, end_ns) {
                if !is_intrinsic_tag(&tag) {
                    by_scope.get_mut("span").expect("span scope").1.insert(tag);
                }
            }
        }
        if has_cold_blocks {
            merge_static_scope(&mut by_scope, scope, TagScope::Intrinsic, INTRINSIC_TAGS);
            merge_static_scope(&mut by_scope, scope, TagScope::Event, EVENT_TAGS);
            merge_static_scope(&mut by_scope, scope, TagScope::Link, LINK_TAGS);
            merge_static_scope(
                &mut by_scope,
                scope,
                TagScope::Instrumentation,
                INSTRUMENTATION_TAGS,
            );
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
        Ok(SCOPE_ORDER
            .iter()
            .filter_map(|scope| by_scope.remove(tag_scope_key(*scope)))
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
        if is_nested_intrinsic_tag(tag) {
            return self
                .nested_intrinsic_tag_values(tenant, tag, start_ns, end_ns)
                .await;
        }
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

impl CrabkaSpanStore {
    async fn nested_intrinsic_tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>, TraceqlError> {
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

fn merge_static_scope(
    by_scope: &mut BTreeMap<&'static str, (TagScope, BTreeSet<String>)>,
    requested: Option<TagScope>,
    scope: TagScope,
    tags: &[&str],
) {
    if requested.is_some_and(|requested| requested != scope) {
        return;
    }
    let (_, out) = by_scope
        .entry(tag_scope_key(scope))
        .or_insert((scope, BTreeSet::new()));
    out.extend(tags.iter().map(|tag| (*tag).to_string()));
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
                instrumentation_name: string_value(&batch, COL_INSTRUMENTATION_NAME, row)?,
                instrumentation_version: string_value(&batch, COL_INSTRUMENTATION_VERSION, row)?,
                attributes: attr_values(&batch, row)?,
                events: event_values(&batch, row)?,
                links: link_values(&batch, row)?,
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

fn is_nested_intrinsic_tag(tag: &str) -> bool {
    matches!(
        tag,
        "event:name" | "event:timeSinceStart" | "link:traceID" | "link:spanID"
    )
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
            insert_i64_value(batch, row, values, "duration", COL_DURATION)?;
        }
        "span:id" => {
            values.insert((
                "string".to_string(),
                bytes_to_hex(fixed(batch, COL_SPAN_ID)?.value(row)),
            ));
        }
        "span:kind" => {
            insert_i32_value(batch, row, values, COL_KIND)?;
        }
        "span:name" => {
            insert_string_value(batch, row, values, COL_NAME)?;
        }
        "span:childCount" => {
            insert_i32_value(batch, row, values, COL_CHILD_COUNT)?;
        }
        "span:parentID" => {
            if let Some(parent_id) = nullable_fixed_value::<8>(batch, COL_PARENT_SPAN_ID, row)? {
                values.insert(("string".to_string(), bytes_to_hex(&parent_id)));
            }
        }
        "span:status" => {
            insert_i32_value(batch, row, values, COL_STATUS_CODE)?;
        }
        "span:statusMessage" => {
            let message = string_value(batch, COL_STATUS_MESSAGE, row)?;
            if !message.is_empty() {
                values.insert(("string".to_string(), message));
            }
        }
        "span:nestedSetLeft" => {
            insert_i32_value(batch, row, values, COL_NS_LEFT)?;
        }
        "span:nestedSetParent" | "span:Parent" => {
            insert_i32_value(batch, row, values, COL_PARENT_ID)?;
        }
        "span:nestedSetRight" => {
            insert_i32_value(batch, row, values, COL_NS_RIGHT)?;
        }
        "trace:duration" => {
            insert_i64_value(batch, row, values, "duration", COL_TRACE_DURATION)?;
        }
        "trace:id" => {
            values.insert((
                "string".to_string(),
                bytes_to_hex(fixed(batch, COL_TRACE_ID)?.value(row)),
            ));
        }
        "trace:rootName" => {
            insert_string_value(batch, row, values, COL_ROOT_SPAN_NAME)?;
        }
        "trace:rootService" => {
            insert_string_value(batch, row, values, COL_ROOT_SERVICE_NAME)?;
        }
        "instrumentation:name" => {
            insert_string_value(batch, row, values, COL_INSTRUMENTATION_NAME)?;
        }
        "instrumentation:version" => {
            insert_string_value(batch, row, values, COL_INSTRUMENTATION_VERSION)?;
        }
        _ => {}
    }
    Ok(())
}

fn insert_string_value(
    batch: &RecordBatch,
    row: usize,
    values: &mut BTreeSet<(String, String)>,
    column: &str,
) -> Result<(), TraceqlError> {
    values.insert(("string".to_string(), string_value(batch, column, row)?));
    Ok(())
}

fn insert_i32_value(
    batch: &RecordBatch,
    row: usize,
    values: &mut BTreeSet<(String, String)>,
    column: &str,
) -> Result<(), TraceqlError> {
    values.insert((
        "int".to_string(),
        int32_value(batch, column, row)?.to_string(),
    ));
    Ok(())
}

fn insert_i64_value(
    batch: &RecordBatch,
    row: usize,
    values: &mut BTreeSet<(String, String)>,
    type_: &str,
    column: &str,
) -> Result<(), TraceqlError> {
    values.insert((
        type_.to_string(),
        int64_value(batch, column, row)?.to_string(),
    ));
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

fn event_values(batch: &RecordBatch, row: usize) -> Result<Vec<EventRef>, TraceqlError> {
    let Some(events) = optional_list_column(batch, SCOL_EVENTS)? else {
        return Ok(Vec::new());
    };
    if events.is_null(row) {
        return Ok(Vec::new());
    }
    let row_events = events.value(row);
    let row_events = row_events
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            TraceqlError::Store(format!("nested column `{SCOL_EVENTS}` row is not a struct"))
        })?;
    let names = struct_string_field(row_events, 0, SCOL_EVENTS)?;
    let times = struct_int64_field(row_events, 1, SCOL_EVENTS)?;
    let attr_keys = struct_list_field(row_events, 2, SCOL_EVENTS)?;
    let attr_values = struct_list_field(row_events, 3, SCOL_EVENTS)?;

    let mut out = Vec::new();
    for idx in 0..row_events.len() {
        if row_events.is_null(idx) {
            continue;
        }
        let name = if names.is_null(idx) {
            String::new()
        } else {
            string_array_value(names, idx)?
        };
        let time_since_start_nano = if times.is_null(idx) {
            0
        } else {
            u64::try_from(times.value(idx)).unwrap_or(0)
        };
        out.push(EventRef {
            time_since_start_nano,
            name,
            attributes: nested_string_attrs(attr_keys, attr_values, idx)?,
        });
    }
    Ok(out)
}

fn link_values(batch: &RecordBatch, row: usize) -> Result<Vec<LinkRef>, TraceqlError> {
    let Some(links) = optional_list_column(batch, SCOL_LINKS)? else {
        return Ok(Vec::new());
    };
    if links.is_null(row) {
        return Ok(Vec::new());
    }
    let row_links = links.value(row);
    let row_links = row_links
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            TraceqlError::Store(format!("nested column `{SCOL_LINKS}` row is not a struct"))
        })?;
    let trace_ids = struct_fixed_field(row_links, 0, SCOL_LINKS)?;
    let span_ids = struct_fixed_field(row_links, 1, SCOL_LINKS)?;
    let attr_keys = struct_list_field(row_links, 2, SCOL_LINKS)?;
    let attr_values = struct_list_field(row_links, 3, SCOL_LINKS)?;

    let mut out = Vec::new();
    for idx in 0..row_links.len() {
        if row_links.is_null(idx) {
            continue;
        }
        out.push(LinkRef {
            trace_id: fixed_array_value::<16>(trace_ids, idx, SCOL_LINKS)?,
            span_id: fixed_array_value::<8>(span_ids, idx, SCOL_LINKS)?,
            attributes: nested_string_attrs(attr_keys, attr_values, idx)?,
        });
    }
    Ok(out)
}

fn optional_list_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<Option<&'a ListArray>, TraceqlError> {
    batch
        .column_by_name(name)
        .map(|col| {
            col.as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| TraceqlError::Store(format!("nested column `{name}` is not a list")))
        })
        .transpose()
}

fn struct_string_field<'a>(
    values: &'a StructArray,
    field: usize,
    name: &str,
) -> Result<&'a dyn Array, TraceqlError> {
    values
        .columns()
        .get(field)
        .map(std::convert::AsRef::as_ref)
        .ok_or_else(|| TraceqlError::Store(format!("nested column `{name}` missing string field")))
}

fn struct_int64_field<'a>(
    values: &'a StructArray,
    field: usize,
    name: &str,
) -> Result<&'a Int64Array, TraceqlError> {
    values
        .columns()
        .get(field)
        .and_then(|col| col.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| TraceqlError::Store(format!("nested column `{name}` missing int64 field")))
}

fn struct_fixed_field<'a>(
    values: &'a StructArray,
    field: usize,
    name: &str,
) -> Result<&'a FixedSizeBinaryArray, TraceqlError> {
    values
        .columns()
        .get(field)
        .and_then(|col| col.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| {
            TraceqlError::Store(format!("nested column `{name}` missing fixed binary field"))
        })
}

fn struct_list_field<'a>(
    values: &'a StructArray,
    field: usize,
    name: &str,
) -> Result<&'a ListArray, TraceqlError> {
    values
        .columns()
        .get(field)
        .and_then(|col| col.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| TraceqlError::Store(format!("nested column `{name}` missing list field")))
}

fn nested_string_attrs(
    keys: &ListArray,
    values: &ListArray,
    row: usize,
) -> Result<Vec<(String, AttrValue)>, TraceqlError> {
    if keys.is_null(row) || values.is_null(row) {
        return Ok(Vec::new());
    }
    let key_values = keys.value(row);
    let key_values = key_values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TraceqlError::Store("nested attribute keys are not strings".into()))?;
    let value_lists = values.value(row);
    let value_lists = value_lists
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            TraceqlError::Store("nested attribute values are not string lists".into())
        })?;

    let mut out = Vec::new();
    for idx in 0..key_values.len().min(value_lists.len()) {
        if key_values.is_null(idx) || value_lists.is_null(idx) {
            continue;
        }
        let scalar_values = value_lists.value(idx);
        let scalar_values = scalar_values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                TraceqlError::Store("nested attribute scalar values are not strings".into())
            })?;
        if scalar_values.is_empty() || scalar_values.is_null(0) {
            continue;
        }
        out.push((
            key_values.value(idx).to_string(),
            AttrValue::Str(scalar_values.value(0).to_string()),
        ));
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

fn fixed_array_value<const N: usize>(
    values: &FixedSizeBinaryArray,
    row: usize,
    name: &str,
) -> Result<[u8; N], TraceqlError> {
    if values.is_null(row) {
        return Ok([0; N]);
    }
    values
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
    use crabka_blockstore::{
        BlockWriter, SCOL_START_NANO, SCOL_TRACE_ID, ShardedTraceBloom, SummaryColumns,
        TraceBlockStats, span_block_decl,
    };
    use crabka_traceql::{
        COL_CHILD_COUNT, COL_INSTRUMENTATION_NAME, COL_INSTRUMENTATION_VERSION, EngineOpts,
        EventRef, LinkRef, TraceqlEngine,
    };
    use object_store::memory::InMemory;
    use url::Url;

    use crate::querier::live::LiveSource;
    use crate::span::{
        AttrValue as SpanAttrValue, EventRecord, KeyValue, LinkRecord, Span, SpanKind, StatusCode,
        batch::span_batch,
    };

    use super::*;

    #[derive(Default)]
    struct FakeLiveSource {
        values: Vec<TypedValue>,
        frontier_ns: i64,
    }

    #[async_trait::async_trait]
    impl LiveSource for FakeLiveSource {
        async fn span_batches(
            &self,
            _tenant: &str,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<RecordBatch>, TraceqlError> {
            Ok(Vec::new())
        }

        async fn trace_spans(
            &self,
            _tenant: &str,
            _trace_id: &[u8; 16],
        ) -> Result<Option<TraceSpans>, TraceqlError> {
            Ok(None)
        }

        async fn tag_names(
            &self,
            _tenant: &str,
            _scope: Option<TagScope>,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<ScopedTag>, TraceqlError> {
            Ok(Vec::new())
        }

        async fn tag_values(
            &self,
            _tenant: &str,
            _tag: &str,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<TypedValue>, TraceqlError> {
            Ok(self.values.clone())
        }

        fn block_builder_frontier_ns(&self, _tenant: &str) -> i64 {
            self.frontier_ns
        }
    }

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

    #[test]
    fn cold_intrinsic_values_include_child_count_and_instrumentation() {
        let batches = vec![batch()];
        assert!(
            intrinsic_values_from_batches("span:childCount", &batches)
                .unwrap()
                .iter()
                .any(|value| value.type_ == "int" && value.value == "0")
        );
        assert!(
            intrinsic_values_from_batches("instrumentation:name", &batches)
                .unwrap()
                .iter()
                .any(|value| value.type_ == "string" && value.value == "tracer")
        );
        assert!(
            intrinsic_values_from_batches("instrumentation:version", &batches)
                .unwrap()
                .iter()
                .any(|value| value.type_ == "string")
        );
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
    async fn cold_tag_discovery_exposes_static_traceql_scopes() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: "blocks/none.parquet".into(),
                min_ts: 0,
                max_ts: 10,
                bloom: ShardedTraceBloom::new(1, 8, 0.01),
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );

        let store = CrabkaSpanStore::new(blocks, Arc::new(index), None);

        let intrinsic = store
            .tag_names("tenant", Some(TagScope::Intrinsic), 0, 10)
            .await
            .unwrap();
        assert!(intrinsic.len() == 1);
        assert!(intrinsic[0].scope == TagScope::Intrinsic);
        assert!(intrinsic[0].tags.contains(&"span:duration".to_string()));
        assert!(intrinsic[0].tags.contains(&"trace:id".to_string()));

        let event = store
            .tag_names("tenant", Some(TagScope::Event), 0, 10)
            .await
            .unwrap();
        assert!(event.len() == 1);
        assert!(event[0].tags == vec!["event:name", "event:timeSinceStart"]);

        let link = store
            .tag_names("tenant", Some(TagScope::Link), 0, 10)
            .await
            .unwrap();
        assert!(link.len() == 1);
        assert!(link[0].tags == vec!["link:spanID", "link:traceID"]);

        let instrumentation = store
            .tag_names("tenant", Some(TagScope::Instrumentation), 0, 10)
            .await
            .unwrap();
        assert!(instrumentation.len() == 1);
        assert!(instrumentation[0].tags == vec!["instrumentation:name", "instrumentation:version"]);
    }

    #[tokio::test]
    async fn cold_span_tag_discovery_excludes_intrinsic_index_names() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: "blocks/none.parquet".into(),
                min_ts: 0,
                max_ts: 10,
                bloom: ShardedTraceBloom::new(1, 8, 0.01),
                tag_names: BTreeSet::from([
                    "http.method".to_string(),
                    "event:name".to_string(),
                    "instrumentation:name".to_string(),
                ]),
                tag_values: BTreeMap::new(),
            },
        );
        let store = CrabkaSpanStore::new(blocks, Arc::new(index), None);

        let tags = store
            .tag_names("tenant", Some(TagScope::Span), 0, 10)
            .await
            .unwrap();

        assert!(tags.len() == 1);
        assert!(tags[0].scope == TagScope::Span);
        assert!(tags[0].tags == vec!["http.method"]);
    }

    #[tokio::test]
    async fn live_nested_intrinsic_values_are_returned_by_store() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let live = LiveTier::new(Arc::new(FakeLiveSource {
            values: vec![TypedValue {
                type_: "string".into(),
                value: "cache.miss".into(),
            }],
            frontier_ns: 0,
        }));
        let store = CrabkaSpanStore::new(blocks, Arc::new(TraceIndex::new()), Some(live));

        let values = store
            .tag_values("tenant", "event:name", 0, 10)
            .await
            .unwrap();

        assert!(
            values
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "cache.miss".into(),
                }]
        );
    }

    #[tokio::test]
    async fn cold_nested_intrinsic_values_are_returned_from_trace_index() {
        let blocks = Arc::new(BlockStore::new(
            Arc::new(InMemory::new()),
            Url::parse("memory:///").unwrap(),
        ));
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: "blocks/none.parquet".into(),
                min_ts: 0,
                max_ts: 10,
                bloom: ShardedTraceBloom::new(1, 8, 0.01),
                tag_names: BTreeSet::from(["event:name".to_string()]),
                tag_values: BTreeMap::from([(
                    "event:name".to_string(),
                    BTreeSet::from(["exception".to_string()]),
                )]),
            },
        );
        let store = CrabkaSpanStore::new(blocks, Arc::new(index), None);

        let values = store
            .tag_values("tenant", "event:name", 0, 10)
            .await
            .unwrap();

        assert!(
            values
                == vec![TypedValue {
                    type_: "string".into(),
                    value: "exception".into(),
                }]
        );
    }

    fn span_with_nested_refs() -> Span {
        Span {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: None,
            name: "GET /users".into(),
            kind: SpanKind::Server,
            start_ns: 1_000,
            duration_ns: 500,
            status: StatusCode::Ok,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: SpanAttrValue::Str("api".into()),
            }],
            span_attrs: Vec::new(),
            events: vec![EventRecord {
                time_unix_nano: 1_050,
                name: "exception".into(),
                attrs: vec![KeyValue {
                    key: "exception.type".into(),
                    value: SpanAttrValue::Str("timeout".into()),
                }],
            }],
            links: vec![LinkRecord {
                trace_id: [9; 16],
                span_id: [8; 8],
                attrs: vec![KeyValue {
                    key: "link.kind".into(),
                    value: SpanAttrValue::Str("retry".into()),
                }],
            }],
            instrumentation_scope: "otel-rust".into(),
            instrumentation_version: "1.2.3".into(),
        }
    }

    #[tokio::test]
    async fn cold_trace_by_id_projects_events_and_links_from_span_blocks() {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let writer = BlockWriter::new(object_store);
        let span = span_with_nested_refs();
        let batch = span_batch(std::slice::from_ref(&span)).unwrap();
        let meta = writer
            .write_block_with_decl(
                "tenant",
                "blocks/spans.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&span.trace_id);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let store = CrabkaSpanStore::new(blocks, Arc::new(index), None);

        let trace = store
            .trace_by_id("tenant", &span.trace_id)
            .await
            .unwrap()
            .unwrap();

        assert!(trace.spans.len() == 1);
        assert!(
            trace.spans[0].events
                == vec![EventRef {
                    time_since_start_nano: 50,
                    name: "exception".into(),
                    attributes: vec![("exception.type".into(), AttrValue::Str("timeout".into()))],
                }]
        );
        assert!(
            trace.spans[0].links
                == vec![LinkRef {
                    trace_id: [9; 16],
                    span_id: [8; 8],
                    attributes: vec![("link.kind".into(), AttrValue::Str("retry".into()))],
                }]
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
