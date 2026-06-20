//! Compactor helpers for merging late-span blocks into replacement span blocks.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, FixedSizeBinaryArray, Float64Array, Int64Array, ListArray, StringArray,
    StructArray,
};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use crabka_blockstore::{
    BlockMeta, BlockWriter, SCOL_ATTR_KEYS, SCOL_ATTR_VALUE, SCOL_ATTR_VALUE_BOOL,
    SCOL_ATTR_VALUE_DOUBLE, SCOL_ATTR_VALUE_INT, SCOL_EVENTS, SCOL_INSTRUMENTATION_NAME,
    SCOL_INSTRUMENTATION_VERSION, SCOL_LINKS, SCOL_START_NANO, SCOL_TRACE_ID, ShardedTraceBloom,
    SummaryColumns, TraceBlockStats, TraceIndex, read_block, span_block_decl, span_block_schema,
};
use object_store::ObjectStore;

use crate::error::TracesError;

type TagMetadata = (BTreeSet<String>, BTreeMap<String, BTreeSet<String>>);

/// Deterministic object key for a compacted span block.
#[must_use]
pub fn compacted_object_key(
    tenant: &str,
    partition: i32,
    min_offset: i64,
    max_offset: i64,
    window_start_ns: i64,
) -> String {
    format!(
        "traces/{tenant}/{partition:05}/compacted-{min_offset:020}-{max_offset:020}-{window_start_ns}.parquet"
    )
}

/// Merge existing span block object keys into one replacement block and index entry.
pub async fn compact_block_keys(
    store: Arc<dyn ObjectStore>,
    writer: &BlockWriter,
    index: &mut TraceIndex,
    tenant: &str,
    input_keys: &[String],
    output_key: &str,
) -> Result<BlockMeta, TracesError> {
    let mut batches = Vec::new();
    for key in input_keys {
        batches.extend(
            read_block(store.clone(), key)
                .await
                .map_err(|err| TracesError::Block(err.to_string()))?,
        );
    }

    if batches.is_empty() {
        return Err(TracesError::Block("cannot compact empty block set".into()));
    }

    let schema = span_block_schema();
    let concatenated =
        concat_batches(&schema, &batches).map_err(|err| TracesError::Block(err.to_string()))?;
    let meta = writer
        .write_block_with_decl(
            tenant,
            output_key,
            schema,
            std::slice::from_ref(&concatenated),
            &span_block_decl(),
            SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
        )
        .await
        .map_err(|err| TracesError::Block(err.to_string()))?;

    let compacted_batches = std::slice::from_ref(&concatenated);
    let (tag_names, tag_values) = tag_metadata(compacted_batches)?;
    index.replace_trace_blocks(
        tenant,
        input_keys,
        TraceBlockStats {
            object_key: meta.object_key.clone(),
            min_ts: meta.min_ts,
            max_ts: meta.max_ts,
            bloom: trace_bloom(compacted_batches)?,
            tag_names,
            tag_values,
        },
    );

    Ok(meta)
}

fn trace_bloom(batches: &[RecordBatch]) -> Result<ShardedTraceBloom, TracesError> {
    let mut traces = BTreeSet::new();
    for batch in batches {
        let trace_ids = batch
            .column_by_name(SCOL_TRACE_ID)
            .ok_or_else(|| TracesError::Block("compacted block missing trace_id".into()))?
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| TracesError::Block("trace_id is not FixedSizeBinary".into()))?;
        for row in 0..batch.num_rows() {
            if trace_ids.is_null(row) {
                continue;
            }
            let mut trace_id = [0_u8; 16];
            trace_id.copy_from_slice(trace_ids.value(row));
            traces.insert(trace_id);
        }
    }

    let mut bloom = ShardedTraceBloom::with_tempo_defaults(traces.len());
    for trace_id in traces {
        bloom.insert(&trace_id);
    }
    Ok(bloom)
}

fn tag_metadata(batches: &[RecordBatch]) -> Result<TagMetadata, TracesError> {
    let mut tag_names = BTreeSet::new();
    let mut tag_values = BTreeMap::new();
    for batch in batches {
        collect_attr_metadata(batch, &mut tag_names, &mut tag_values)?;
        collect_event_metadata(batch, &mut tag_names, &mut tag_values)?;
        collect_link_metadata(batch, &mut tag_names, &mut tag_values)?;
        collect_string_column_metadata(
            batch,
            SCOL_INSTRUMENTATION_NAME,
            "instrumentation:name",
            &mut tag_names,
            &mut tag_values,
        )?;
        collect_string_column_metadata(
            batch,
            SCOL_INSTRUMENTATION_VERSION,
            "instrumentation:version",
            &mut tag_names,
            &mut tag_values,
        )?;
    }
    Ok((tag_names, tag_values))
}

fn collect_attr_metadata(
    batch: &RecordBatch,
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), TracesError> {
    let keys = list_column(batch, SCOL_ATTR_KEYS)?;
    for row in 0..batch.num_rows() {
        if keys.is_null(row) {
            continue;
        }
        let row_keys = keys.value(row);
        let row_keys = row_keys
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| TracesError::Block("attr_keys row is not Utf8".into()))?;
        for idx in 0..row_keys.len() {
            if row_keys.is_null(idx) {
                continue;
            }
            let key = row_keys.value(idx);
            if let Some(value) = attr_value(batch, row, idx)? {
                insert_tag_value(tag_names, tag_values, key, value);
            } else {
                tag_names.insert(key.to_string());
            }
        }
    }
    Ok(())
}

fn collect_event_metadata(
    batch: &RecordBatch,
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), TracesError> {
    let Some(events) = optional_list_column(batch, SCOL_EVENTS)? else {
        return Ok(());
    };
    collect_nested_metadata(events, |event| {
        let names = struct_string_field(event, 0)?;
        let times = struct_i64_field(event, 1)?;
        let keys = struct_list_field(event, 2)?;
        let values = struct_list_field(event, 3)?;
        for idx in 0..event.len() {
            if event.is_null(idx) {
                continue;
            }
            if !names.is_null(idx) {
                insert_tag_value(
                    tag_names,
                    tag_values,
                    "event:name",
                    names.value(idx).to_string(),
                );
            }
            if !times.is_null(idx) {
                insert_tag_value(
                    tag_names,
                    tag_values,
                    "event:timeSinceStart",
                    times.value(idx).to_string(),
                );
            }
            collect_nested_attrs(keys, values, idx, tag_names, tag_values)?;
        }
        Ok(())
    })
}

fn collect_link_metadata(
    batch: &RecordBatch,
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), TracesError> {
    let Some(links) = optional_list_column(batch, SCOL_LINKS)? else {
        return Ok(());
    };
    collect_nested_metadata(links, |link| {
        let trace_ids = struct_fixed_field(link, 0)?;
        let span_ids = struct_fixed_field(link, 1)?;
        let keys = struct_list_field(link, 2)?;
        let values = struct_list_field(link, 3)?;
        for idx in 0..link.len() {
            if link.is_null(idx) {
                continue;
            }
            if !trace_ids.is_null(idx) {
                insert_tag_value(
                    tag_names,
                    tag_values,
                    "link:traceID",
                    hex::encode(trace_ids.value(idx)),
                );
            }
            if !span_ids.is_null(idx) {
                insert_tag_value(
                    tag_names,
                    tag_values,
                    "link:spanID",
                    hex::encode(span_ids.value(idx)),
                );
            }
            collect_nested_attrs(keys, values, idx, tag_names, tag_values)?;
        }
        Ok(())
    })
}

fn collect_nested_metadata(
    values: &ListArray,
    mut collect: impl FnMut(&StructArray) -> Result<(), TracesError>,
) -> Result<(), TracesError> {
    for row in 0..values.len() {
        if values.is_null(row) {
            continue;
        }
        let nested = values.value(row);
        let nested = nested
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| TracesError::Block("nested metadata row is not a struct".into()))?;
        collect(nested)?;
    }
    Ok(())
}

fn collect_nested_attrs(
    keys: &ListArray,
    values: &ListArray,
    idx: usize,
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), TracesError> {
    if keys.is_null(idx) {
        return Ok(());
    }
    let attr_keys = keys.value(idx);
    let attr_keys = attr_keys
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TracesError::Block("nested attr keys are not Utf8".into()))?;
    let attr_values = if values.is_null(idx) {
        None
    } else {
        Some(values.value(idx))
    };
    let attr_values = attr_values
        .as_ref()
        .and_then(|array| array.as_any().downcast_ref::<ListArray>());

    for attr_idx in 0..attr_keys.len() {
        if attr_keys.is_null(attr_idx) {
            continue;
        }
        let key = attr_keys.value(attr_idx);
        if let Some(value) = attr_values.and_then(|values| string_list_value(values, attr_idx)) {
            insert_tag_value(tag_names, tag_values, key, value);
        } else {
            tag_names.insert(key.to_string());
        }
    }
    Ok(())
}

fn string_list_value(values: &ListArray, idx: usize) -> Option<String> {
    if idx >= values.len() || values.is_null(idx) {
        return None;
    }
    let values = values.value(idx);
    let values = values.as_any().downcast_ref::<StringArray>()?;
    (0..values.len())
        .find(|value_idx| !values.is_null(*value_idx))
        .map(|value_idx| values.value(value_idx).to_string())
}

fn collect_string_column_metadata(
    batch: &RecordBatch,
    column: &str,
    tag: &str,
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(), TracesError> {
    let Some(col) = batch.column_by_name(column) else {
        return Ok(());
    };
    let strings = col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TracesError::Block(format!("{column} is not Utf8")))?;
    for row in 0..strings.len() {
        if strings.is_null(row) || strings.value(row).is_empty() {
            continue;
        }
        insert_tag_value(tag_names, tag_values, tag, strings.value(row).to_string());
    }
    Ok(())
}

fn attr_value(
    batch: &RecordBatch,
    row: usize,
    attr_idx: usize,
) -> Result<Option<String>, TracesError> {
    if let Some(value) =
        first_string_list_value::<StringArray>(batch, SCOL_ATTR_VALUE, row, attr_idx)?
    {
        return Ok(Some(value));
    }
    if let Some(value) =
        first_string_list_value::<Int64Array>(batch, SCOL_ATTR_VALUE_INT, row, attr_idx)?
    {
        return Ok(Some(value));
    }
    if let Some(value) =
        first_string_list_value::<Float64Array>(batch, SCOL_ATTR_VALUE_DOUBLE, row, attr_idx)?
    {
        return Ok(Some(value));
    }
    first_string_list_value::<BooleanArray>(batch, SCOL_ATTR_VALUE_BOOL, row, attr_idx)
}

trait MetadataValueArray: Array {
    fn string_value(&self, idx: usize) -> String;
}

impl MetadataValueArray for StringArray {
    fn string_value(&self, idx: usize) -> String {
        self.value(idx).to_string()
    }
}

impl MetadataValueArray for Int64Array {
    fn string_value(&self, idx: usize) -> String {
        self.value(idx).to_string()
    }
}

impl MetadataValueArray for Float64Array {
    fn string_value(&self, idx: usize) -> String {
        self.value(idx).to_string()
    }
}

impl MetadataValueArray for BooleanArray {
    fn string_value(&self, idx: usize) -> String {
        self.value(idx).to_string()
    }
}

fn first_string_list_value<A: MetadataValueArray + 'static>(
    batch: &RecordBatch,
    column: &str,
    row: usize,
    attr_idx: usize,
) -> Result<Option<String>, TracesError> {
    let values = list_column(batch, column)?;
    if values.is_null(row) {
        return Ok(None);
    }
    let row_values = values.value(row);
    let row_values = row_values
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| TracesError::Block(format!("{column} row is not a list")))?;
    if attr_idx >= row_values.len() || row_values.is_null(attr_idx) {
        return Ok(None);
    }
    let attr_values = row_values.value(attr_idx);
    let attr_values = attr_values
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| TracesError::Block(format!("{column} values have wrong type")))?;
    for value_idx in 0..attr_values.len() {
        if !attr_values.is_null(value_idx) {
            return Ok(Some(attr_values.string_value(value_idx)));
        }
    }
    Ok(None)
}

fn list_column<'a>(batch: &'a RecordBatch, column: &str) -> Result<&'a ListArray, TracesError> {
    batch
        .column_by_name(column)
        .and_then(|col| col.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| TracesError::Block(format!("{column} is not a list")))
}

fn optional_list_column<'a>(
    batch: &'a RecordBatch,
    column: &str,
) -> Result<Option<&'a ListArray>, TracesError> {
    let Some(col) = batch.column_by_name(column) else {
        return Ok(None);
    };
    col.as_any()
        .downcast_ref::<ListArray>()
        .map(Some)
        .ok_or_else(|| TracesError::Block(format!("{column} is not a list")))
}

fn struct_string_field(array: &StructArray, idx: usize) -> Result<&StringArray, TracesError> {
    array
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TracesError::Block(format!("struct field {idx} is not Utf8")))
}

fn struct_i64_field(array: &StructArray, idx: usize) -> Result<&Int64Array, TracesError> {
    array
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| TracesError::Block(format!("struct field {idx} is not Int64")))
}

fn struct_fixed_field(
    array: &StructArray,
    idx: usize,
) -> Result<&FixedSizeBinaryArray, TracesError> {
    array
        .column(idx)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| TracesError::Block(format!("struct field {idx} is not FixedSizeBinary")))
}

fn struct_list_field(array: &StructArray, idx: usize) -> Result<&ListArray, TracesError> {
    array
        .column(idx)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| TracesError::Block(format!("struct field {idx} is not a list")))
}

fn insert_tag_value(
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
    tag: &str,
    value: String,
) {
    tag_names.insert(tag.to_string());
    tag_values.entry(tag.to_string()).or_default().insert(value);
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;
    use crate::span::{
        AttrValue, EventRecord, KeyValue, LinkRecord, Span, SpanKind, StatusCode, batch::span_batch,
    };

    fn span() -> Span {
        Span {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: None,
            name: "GET /".into(),
            kind: SpanKind::Server,
            start_ns: 1_000,
            duration_ns: 100,
            status: StatusCode::Ok,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str("api".into()),
            }],
            span_attrs: vec![KeyValue {
                key: "env".into(),
                value: AttrValue::Str("prod".into()),
            }],
            events: vec![EventRecord {
                time_unix_nano: 1_050,
                name: "exception".into(),
                attrs: vec![KeyValue {
                    key: "cache.key".into(),
                    value: AttrValue::Str("users".into()),
                }],
            }],
            links: vec![LinkRecord {
                trace_id: [9; 16],
                span_id: [8; 8],
                attrs: vec![KeyValue {
                    key: "link.kind".into(),
                    value: AttrValue::Str("retry".into()),
                }],
            }],
            instrumentation_scope: "otel-rust".into(),
            instrumentation_version: "1.2.3".into(),
        }
    }

    #[tokio::test]
    async fn compacted_block_recomputes_tag_metadata_from_rows() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());
        let batch = span_batch(&[span()]).unwrap();
        let input = writer
            .write_block_with_decl(
                "tenant",
                "input.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();

        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&[1; 16]);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: input.object_key.clone(),
                min_ts: input.min_ts,
                max_ts: input.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );

        compact_block_keys(
            store,
            &writer,
            &mut index,
            "tenant",
            std::slice::from_ref(&input.object_key),
            "compacted.parquet",
        )
        .await
        .unwrap();

        let names = index.tag_names("tenant", 0, 2_000);
        assert!(names.contains(&"service.name".to_string()));
        assert!(names.contains(&"env".to_string()));
        assert!(names.contains(&"instrumentation:name".to_string()));
        assert!(names.contains(&"event:name".to_string()));
        assert!(names.contains(&"event:timeSinceStart".to_string()));
        assert!(names.contains(&"cache.key".to_string()));
        assert!(names.contains(&"link:traceID".to_string()));
        assert!(names.contains(&"link:spanID".to_string()));
        assert!(names.contains(&"link.kind".to_string()));
        assert!(index.tag_values("tenant", "service.name", 0, 2_000) == vec!["api".to_string()]);
        assert!(index.tag_values("tenant", "env", 0, 2_000) == vec!["prod".to_string()]);
        assert!(
            index.tag_values("tenant", "instrumentation:name", 0, 2_000)
                == vec!["otel-rust".to_string()]
        );
        assert!(
            index.tag_values("tenant", "event:name", 0, 2_000) == vec!["exception".to_string()]
        );
        assert!(
            index.tag_values("tenant", "event:timeSinceStart", 0, 2_000) == vec!["50".to_string()]
        );
        assert!(index.tag_values("tenant", "cache.key", 0, 2_000) == vec!["users".to_string()]);
        assert!(
            index.tag_values("tenant", "link:traceID", 0, 2_000)
                == vec!["09090909090909090909090909090909".to_string()]
        );
        assert!(
            index.tag_values("tenant", "link:spanID", 0, 2_000)
                == vec!["0808080808080808".to_string()]
        );
        assert!(index.tag_values("tenant", "link.kind", 0, 2_000) == vec!["retry".to_string()]);
    }
}
