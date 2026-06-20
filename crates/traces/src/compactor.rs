//! Compactor helpers for merging late-span blocks into replacement span blocks.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{Array, FixedSizeBinaryArray};
use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use crabka_blockstore::{
    BlockMeta, BlockWriter, SCOL_START_NANO, SCOL_TRACE_ID, ShardedTraceBloom, SummaryColumns,
    TraceBlockStats, TraceIndex, read_block, span_block_decl, span_block_schema,
};
use object_store::ObjectStore;

use crate::error::TracesError;

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

    index.replace_trace_blocks(
        tenant,
        input_keys,
        TraceBlockStats {
            object_key: meta.object_key.clone(),
            min_ts: meta.min_ts,
            max_ts: meta.max_ts,
            bloom: trace_bloom(&[concatenated])?,
            tag_names: BTreeSet::new(),
            tag_values: BTreeMap::default(),
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
