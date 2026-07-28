//! Compactor helpers for merging late-span blocks into replacement span blocks.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array,
        ListArray, StringArray, StructArray,
    },
    compute::concat_batches,
    record_batch::RecordBatch,
};
#[cfg(test)]
use crabka_blockstore::read_block;
use crabka_blockstore::{
    BlockIndex, BlockMeta, BlockReadMaxBytes, BlockWriter, SCOL_ATTR_KEYS, SCOL_ATTR_VALUE,
    SCOL_ATTR_VALUE_BOOL, SCOL_ATTR_VALUE_DOUBLE, SCOL_ATTR_VALUE_INT, SCOL_CHILD_COUNT,
    SCOL_DURATION_NANOS, SCOL_EVENTS, SCOL_INSTRUMENTATION_NAME, SCOL_INSTRUMENTATION_VERSION,
    SCOL_LINKS, SCOL_NAME, SCOL_NESTED_SET_LEFT, SCOL_NESTED_SET_RIGHT, SCOL_PARENT_ID,
    SCOL_PARENT_SPAN_ID, SCOL_ROOT_SERVICE_NAME, SCOL_ROOT_SPAN_NAME, SCOL_SPAN_ID,
    SCOL_START_NANO, SCOL_TRACE_DURATION_NANOS, SCOL_TRACE_ID, SCOL_TRACE_START_NANO,
    ShardedTraceBloom, SummaryColumns, TraceBlockStats, TraceIndex, read_block_with_max_bytes,
    span_block_decl, span_block_schema,
};
use object_store::ObjectStore;

use crate::{
    blockbuilder::prefixed_object_key,
    error::TracesError,
    ids::{MaxOffset, MinOffset, WindowStartNs},
    span::batch::RESOURCE_ATTR_PREFIX,
};

type TagMetadata = (BTreeSet<String>, BTreeMap<String, BTreeSet<String>>);

/// Deterministic object key for a compacted span block.
#[must_use]
pub fn compacted_object_key(
    tenant: &str,
    partition: i32,
    min_offset: MinOffset,
    max_offset: MaxOffset,
    window_start_ns: WindowStartNs,
) -> String {
    let (min_offset, max_offset, window_start_ns) = (min_offset.0, max_offset.0, window_start_ns.0);
    format!(
        "traces/{tenant}/{partition:05}/compacted-{min_offset:020}-{max_offset:020}-{window_start_ns}.parquet"
    )
}

/// Merge existing span block object keys into one replacement block and index entry.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn compact_block_keys(
    store: Arc<dyn ObjectStore>,
    writer: &BlockWriter,
    index: &mut TraceIndex,
    tenant: &str,
    input_keys: &[String],
    output_key: &str,
) -> Result<BlockMeta, TracesError> {
    compact_block_keys_with_max_bytes(
        store,
        writer,
        index,
        tenant,
        input_keys,
        output_key,
        BlockReadMaxBytes::default(),
    )
    .await
}

/// Merge existing span blocks with a caller-supplied on-disk read limit.
///
/// # Errors
/// Returns an error when an input exceeds the configured cap, the query is
/// malformed, an expression has incompatible operand types, or the backing
/// span store fails.
pub async fn compact_block_keys_with_max_bytes(
    store: Arc<dyn ObjectStore>,
    writer: &BlockWriter,
    index: &mut TraceIndex,
    tenant: &str,
    input_keys: &[String],
    output_key: &str,
    block_read_max_bytes: BlockReadMaxBytes,
) -> Result<BlockMeta, TracesError> {
    let mut batches = Vec::new();
    for key in input_keys {
        batches.extend(
            read_block_with_max_bytes(store.clone(), key, block_read_max_bytes)
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
    let concatenated = recompute_nested_sets(&concatenated)?;
    let concatenated = recompute_trace_level_columns(&concatenated)?;
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

/// Compact every tenant in the selected time window independently.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn compact_index_window(
    store: Arc<dyn ObjectStore>,
    writer: &BlockWriter,
    index: &mut TraceIndex,
    object_key_prefix: &str,
    start_ns: i64,
    end_ns: i64,
) -> Result<Vec<BlockMeta>, TracesError> {
    compact_index_window_with_max_bytes(
        store,
        writer,
        index,
        object_key_prefix,
        start_ns,
        end_ns,
        BlockReadMaxBytes::default(),
    )
    .await
}

/// Compact every tenant using a caller-supplied on-disk block-read limit.
///
/// # Errors
/// Returns an error when an input exceeds the configured cap, the query is
/// malformed, an expression has incompatible operand types, or the backing
/// span store fails.
pub async fn compact_index_window_with_max_bytes(
    store: Arc<dyn ObjectStore>,
    writer: &BlockWriter,
    index: &mut TraceIndex,
    object_key_prefix: &str,
    start_ns: i64,
    end_ns: i64,
    block_read_max_bytes: BlockReadMaxBytes,
) -> Result<Vec<BlockMeta>, TracesError> {
    let mut metas = Vec::new();
    for tenant in index.tenants() {
        let candidate_keys = index.candidate_blocks(&tenant, start_ns, end_ns);
        if candidate_keys.len() < 2 {
            continue;
        }
        let output_key = prefixed_object_key(
            object_key_prefix,
            &compacted_object_key(
                &tenant,
                0,
                MinOffset(0),
                MaxOffset(i64::try_from(candidate_keys.len()).unwrap_or(i64::MAX)),
                WindowStartNs(start_ns),
            ),
        );
        let meta = compact_block_keys_with_max_bytes(
            store.clone(),
            writer,
            index,
            &tenant,
            &candidate_keys,
            &output_key,
            block_read_max_bytes,
        )
        .await?;
        metas.push(meta);
    }
    Ok(metas)
}

fn recompute_nested_sets(batch: &RecordBatch) -> Result<RecordBatch, TracesError> {
    enum Frame {
        Enter { row: usize, parent_left: i32 },
        Exit { row: usize },
    }

    let trace_ids = fixed_column(batch, SCOL_TRACE_ID, 16)?;
    let span_ids = fixed_column(batch, SCOL_SPAN_ID, 8)?;
    let parent_span_ids = fixed_column(batch, SCOL_PARENT_SPAN_ID, 8)?;
    let mut by_trace: BTreeMap<[u8; 16], Vec<usize>> = BTreeMap::new();
    for row in 0..batch.num_rows() {
        if trace_ids.is_null(row) {
            continue;
        }
        let mut trace_id = [0_u8; 16];
        trace_id.copy_from_slice(trace_ids.value(row));
        by_trace.entry(trace_id).or_default().push(row);
    }

    let mut left = vec![0_i32; batch.num_rows()];
    let mut right = vec![0_i32; batch.num_rows()];
    // Default to the root sentinel (-1, Tempo's no-parent value): a row not
    // reached by the per-trace DFS has no parent. 0 is an invalid parent (left
    // values start at 1).
    let mut parent_id = vec![-1_i32; batch.num_rows()];

    for rows in by_trace.values() {
        let mut pos = HashMap::new();
        for row in rows {
            if span_ids.is_null(*row) {
                continue;
            }
            let mut span_id = [0_u8; 8];
            span_id.copy_from_slice(span_ids.value(*row));
            pos.insert(span_id, *row);
        }

        let mut children: HashMap<usize, Vec<usize>> = HashMap::new();
        let mut roots = Vec::new();
        for row in rows {
            let parent = (!parent_span_ids.is_null(*row)).then(|| {
                let mut parent = [0_u8; 8];
                parent.copy_from_slice(parent_span_ids.value(*row));
                parent
            });
            match parent.and_then(|parent| pos.get(&parent).copied()) {
                Some(parent_row) if parent_row != *row => {
                    children.entry(parent_row).or_default().push(*row);
                }
                _ => roots.push(*row),
            }
        }

        let mut counter = 1_i32;
        let mut stack = Vec::new();
        for row in roots.iter().rev() {
            stack.push(Frame::Enter {
                row: *row,
                // Root span: nestedSetParent = -1 (Tempo no-parent sentinel).
                parent_left: -1,
            });
        }
        while let Some(frame) = stack.pop() {
            match frame {
                Frame::Enter {
                    row,
                    parent_left: parent,
                } => {
                    left[row] = counter;
                    parent_id[row] = parent;
                    counter += 1;
                    stack.push(Frame::Exit { row });
                    if let Some(children) = children.get(&row) {
                        for child in children.iter().rev() {
                            stack.push(Frame::Enter {
                                row: *child,
                                parent_left: left[row],
                            });
                        }
                    }
                }
                Frame::Exit { row } => {
                    right[row] = counter;
                    counter += 1;
                }
            }
        }
    }

    let child_count = left
        .iter()
        .map(|node_left| {
            i32::try_from(
                parent_id
                    .iter()
                    .filter(|parent| *parent == node_left)
                    .count(),
            )
            .unwrap_or(i32::MAX)
        })
        .collect::<Vec<_>>();
    replace_int32_columns(
        batch,
        &[
            (SCOL_NESTED_SET_LEFT, left),
            (SCOL_NESTED_SET_RIGHT, right),
            (SCOL_PARENT_ID, parent_id),
            (SCOL_CHILD_COUNT, child_count),
        ],
    )
}

/// Recompute the trace-level denormalized columns over the FULL merged trace.
///
/// The write path (`span/batch.rs::root_info`) sets `trace_start_unix_nano` /
/// `trace_duration_nanos` / `root_service_name` / `root_span_name` from only the
/// spans in one flush-window block. After compacting several blocks of the same
/// trace, each origin block's rows still carry that block's (partial, stale)
/// values, so trace-level `TraceQL` matchers (`trace:duration`, `trace:rootName`,
/// `trace:rootService`) read wrong data. Regroup by `trace_id` and recompute the
/// four columns across all merged rows.
fn recompute_trace_level_columns(batch: &RecordBatch) -> Result<RecordBatch, TracesError> {
    let trace_ids = fixed_column(batch, SCOL_TRACE_ID, 16)?;
    let parent_span_ids = fixed_column(batch, SCOL_PARENT_SPAN_ID, 8)?;
    let start = int64_column(batch, SCOL_START_NANO)?;
    let duration = int64_column(batch, SCOL_DURATION_NANOS)?;
    let name = string_column(batch, SCOL_NAME)?;
    let root_service = string_column(batch, SCOL_ROOT_SERVICE_NAME)?;

    let mut by_trace: BTreeMap<[u8; 16], Vec<usize>> = BTreeMap::new();
    for row in 0..batch.num_rows() {
        if trace_ids.is_null(row) {
            continue;
        }
        let mut trace_id = [0_u8; 16];
        trace_id.copy_from_slice(trace_ids.value(row));
        by_trace.entry(trace_id).or_default().push(row);
    }

    let rows_n = batch.num_rows();
    let mut trace_start = vec![0_i64; rows_n];
    let mut trace_duration = vec![0_i64; rows_n];
    let mut root_service_out: Vec<Option<String>> = vec![None; rows_n];
    let mut root_name_out: Vec<Option<String>> = vec![None; rows_n];

    for rows in by_trace.values() {
        let mut min_start = i64::MAX;
        let mut max_end = i64::MIN;
        for &row in rows {
            let s = start.value(row);
            min_start = min_start.min(s);
            let d = if duration.is_null(row) {
                0
            } else {
                duration.value(row)
            };
            max_end = max_end.max(s.saturating_add(d));
        }
        let dur = max_end.saturating_sub(min_start).max(0);

        // Root = the first span with no in-trace parent, else the earliest span
        // (matching the write-path `root_info`). `root_service_name` of the root
        // row is its trace's root service; `name` is the root span's own name.
        let root_row = rows
            .iter()
            .copied()
            .find(|&row| parent_span_ids.is_null(row))
            .or_else(|| rows.iter().copied().min_by_key(|&row| start.value(row)));
        let (service, span_name) = root_row.map_or((None, None), |row| {
            (
                (!root_service.is_null(row)).then(|| root_service.value(row).to_string()),
                (!name.is_null(row)).then(|| name.value(row).to_string()),
            )
        });

        for &row in rows {
            trace_start[row] = min_start;
            trace_duration[row] = dur;
            root_service_out[row].clone_from(&service);
            root_name_out[row].clone_from(&span_name);
        }
    }

    let schema = batch.schema();
    let mut columns = batch.columns().to_vec();
    set_column(
        &schema,
        &mut columns,
        SCOL_TRACE_START_NANO,
        Arc::new(Int64Array::from(trace_start)),
    )?;
    set_column(
        &schema,
        &mut columns,
        SCOL_TRACE_DURATION_NANOS,
        Arc::new(Int64Array::from(trace_duration)),
    )?;
    set_column(
        &schema,
        &mut columns,
        SCOL_ROOT_SERVICE_NAME,
        Arc::new(root_service_out.into_iter().collect::<StringArray>()),
    )?;
    set_column(
        &schema,
        &mut columns,
        SCOL_ROOT_SPAN_NAME,
        Arc::new(root_name_out.into_iter().collect::<StringArray>()),
    )?;
    RecordBatch::try_new(schema, columns).map_err(|err| TracesError::Block(err.to_string()))
}

fn set_column(
    schema: &arrow::datatypes::SchemaRef,
    columns: &mut [ArrayRef],
    name: &str,
    array: ArrayRef,
) -> Result<(), TracesError> {
    let idx = schema
        .column_with_name(name)
        .ok_or_else(|| TracesError::Block(format!("missing column {name}")))?
        .0;
    columns[idx] = array;
    Ok(())
}

fn int64_column<'a>(batch: &'a RecordBatch, column: &str) -> Result<&'a Int64Array, TracesError> {
    batch
        .column_by_name(column)
        .and_then(|col| col.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| TracesError::Block(format!("{column} is not Int64")))
}

fn string_column<'a>(batch: &'a RecordBatch, column: &str) -> Result<&'a StringArray, TracesError> {
    batch
        .column_by_name(column)
        .and_then(|col| col.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| TracesError::Block(format!("{column} is not Utf8")))
}

fn replace_int32_columns(
    batch: &RecordBatch,
    replacements: &[(&str, Vec<i32>)],
) -> Result<RecordBatch, TracesError> {
    let schema = batch.schema();
    let mut columns = batch.columns().to_vec();
    for (name, values) in replacements {
        let idx = schema
            .column_with_name(name)
            .ok_or_else(|| TracesError::Block(format!("missing column {name}")))?
            .0;
        columns[idx] = Arc::new(Int32Array::from(values.clone()));
    }
    RecordBatch::try_new(schema, columns).map_err(|err| TracesError::Block(err.to_string()))
}

fn fixed_column<'a>(
    batch: &'a RecordBatch,
    column: &str,
    width: i32,
) -> Result<&'a FixedSizeBinaryArray, TracesError> {
    let array = batch
        .column_by_name(column)
        .ok_or_else(|| TracesError::Block(format!("missing column {column}")))?
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| TracesError::Block(format!("{column} is not FixedSizeBinary")))?;
    if array.value_length() != width {
        return Err(TracesError::Block(format!(
            "{column} is FixedSizeBinary({}), expected {width}",
            array.value_length()
        )));
    }
    Ok(array)
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
            let key = row_keys
                .value(idx)
                .strip_prefix(RESOURCE_ATTR_PREFIX)
                .unwrap_or_else(|| row_keys.value(idx));
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
    use assert2::check;
    use crabka_blockstore::BlockIndex;
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

    fn mk_span(
        span_id: [u8; 8],
        parent: Option<[u8; 8]>,
        start_ns: i64,
        duration_ns: i64,
        name: &str,
        service: &str,
    ) -> Span {
        Span {
            trace_id: [1; 16],
            span_id,
            parent_span_id: parent,
            name: name.into(),
            kind: SpanKind::Server,
            start_ns,
            duration_ns,
            status: StatusCode::Ok,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str(service.into()),
            }],
            span_attrs: vec![],
            events: vec![],
            links: vec![],
            instrumentation_scope: "otel-rust".into(),
            instrumentation_version: "1".into(),
        }
    }

    #[tokio::test]
    async fn compaction_recomputes_trace_level_columns_across_blocks() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());

        // Block A holds the root span (later start); block B holds an earlier
        // late child whose parent is NOT in B (so B's per-block root_info wrongly
        // treats the child as the root → stale `root_span_name`/`trace_start`).
        let batch_a = span_batch(&[mk_span([2; 8], None, 1_000, 100, "GET /", "api")]).unwrap();
        writer
            .write_block_with_decl(
                "tenant",
                "a.parquet",
                span_block_schema(),
                &[batch_a],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let batch_b =
            span_batch(&[mk_span([3; 8], Some([2; 8]), 800, 50, "child", "api")]).unwrap();
        writer
            .write_block_with_decl(
                "tenant",
                "b.parquet",
                span_block_schema(),
                &[batch_b],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();

        let mut index = TraceIndex::new();
        let rejected = compact_block_keys_with_max_bytes(
            store.clone(),
            &writer,
            &mut index,
            "tenant",
            &["a.parquet".to_string(), "b.parquet".to_string()],
            "rejected.parquet",
            crabka_blockstore::BlockReadMaxBytes::new(1).unwrap(),
        )
        .await;
        assert2::assert!(rejected.is_err());

        compact_block_keys(
            store.clone(),
            &writer,
            &mut index,
            "tenant",
            &["a.parquet".to_string(), "b.parquet".to_string()],
            "compacted.parquet",
        )
        .await
        .unwrap();

        let batches = read_block(store, "compacted.parquet").await.unwrap();
        let batch = &batches[0];
        check!(batch.num_rows() == 2);
        let trace_start = int64_column(batch, SCOL_TRACE_START_NANO).unwrap();
        let trace_duration = int64_column(batch, SCOL_TRACE_DURATION_NANOS).unwrap();
        let service = string_column(batch, SCOL_ROOT_SERVICE_NAME).unwrap();
        let root_name = string_column(batch, SCOL_ROOT_SPAN_NAME).unwrap();
        for row in 0..batch.num_rows() {
            // min start across both blocks, and span to the latest end.
            check!(trace_start.value(row) == 800);
            check!(trace_duration.value(row) == 300); // max(1100, 850) - 800
            // root is the true (no-parent) span, consistent across every row.
            check!(service.value(row) == "api");
            check!(root_name.value(row) == "GET /");
        }
    }

    #[tokio::test]
    async fn compact_index_window_compacts_each_tenant_independently() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());
        let mut index = TraceIndex::new();
        write_indexed_block(&writer, &mut index, "tenant-a", "tenant-a/input-1.parquet").await;
        write_indexed_block(&writer, &mut index, "tenant-a", "tenant-a/input-2.parquet").await;
        write_indexed_block(&writer, &mut index, "tenant-b", "tenant-b/input-1.parquet").await;
        write_indexed_block(&writer, &mut index, "tenant-b", "tenant-b/input-2.parquet").await;

        compact_index_window(store, &writer, &mut index, "", 0, 2_000)
            .await
            .unwrap();

        let tenant_a = index.candidate_blocks("tenant-a", 0, 2_000);
        let tenant_b = index.candidate_blocks("tenant-b", 0, 2_000);
        assert2::assert!(tenant_a.len() == 1);
        assert2::assert!(tenant_b.len() == 1);
        check!(tenant_a[0].contains("traces/tenant-a/"));
        check!(tenant_b[0].contains("traces/tenant-b/"));
    }

    async fn write_indexed_block(
        writer: &BlockWriter,
        index: &mut TraceIndex,
        tenant: &str,
        object_key: &str,
    ) {
        let batch = span_batch(&[span()]).unwrap();
        let input = writer
            .write_block_with_decl(
                tenant,
                object_key,
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&[1; 16]);
        index.add_trace_block(
            tenant,
            TraceBlockStats {
                object_key: input.object_key,
                min_ts: input.min_ts,
                max_ts: input.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
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
        for name in [
            "service.name",
            "env",
            "instrumentation:name",
            "event:name",
            "event:timeSinceStart",
            "cache.key",
            "link:traceID",
            "link:spanID",
            "link.kind",
        ] {
            check!(names.contains(&name.to_string()));
        }
        for (tag, want) in [
            ("service.name", "api"),
            ("env", "prod"),
            ("instrumentation:name", "otel-rust"),
            ("event:name", "exception"),
            ("event:timeSinceStart", "50"),
            ("cache.key", "users"),
            ("link:traceID", "09090909090909090909090909090909"),
            ("link:spanID", "0808080808080808"),
            ("link.kind", "retry"),
        ] {
            check!(index.tag_values("tenant", tag, 0, 2_000) == vec![want.to_string()]);
        }
    }
}
