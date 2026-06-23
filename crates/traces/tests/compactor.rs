use std::sync::Arc;

use arrow::array::{FixedSizeBinaryArray, Int32Array};
use assert2::assert;
use crabka_blockstore::{BlockWriter, TraceIndex, read_block};
use crabka_traces::{
    AttrValue, KeyValue, Span, SpanKind, SpanRecord, StatusCode,
    blockbuilder::build_blocks,
    compactor::{compact_block_keys, compacted_object_key},
};
use object_store::ObjectStore;
use object_store::memory::InMemory;

fn span(trace_id: [u8; 16], span_id: u8, parent: Option<u8>, start_ns: i64) -> Span {
    Span {
        trace_id,
        span_id: [span_id; 8],
        parent_span_id: parent.map(|id| [id; 8]),
        name: format!("span-{span_id}"),
        kind: SpanKind::Server,
        start_ns,
        duration_ns: 5,
        status: StatusCode::Ok,
        status_message: String::new(),
        resource_attrs: vec![KeyValue {
            key: "service.name".into(),
            value: AttrValue::Str("api".into()),
        }],
        span_attrs: vec![KeyValue {
            key: "http.method".into(),
            value: AttrValue::Str("GET".into()),
        }],
        events: Vec::new(),
        links: Vec::new(),
        instrumentation_scope: "test".into(),
        instrumentation_version: String::new(),
    }
}

fn rec(trace_id: [u8; 16], span_id: u8, parent: Option<u8>, start_ns: i64) -> SpanRecord {
    SpanRecord {
        tenant: "tenant-a".into(),
        span: span(trace_id, span_id, parent, start_ns),
    }
}

#[tokio::test]
async fn compact_block_keys_merges_late_spans_and_replaces_index_entries() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();

    let first = build_blocks(
        &writer,
        &mut index,
        "tenant-a",
        7,
        &[rec([1; 16], 1, None, 100)],
        (10, 10),
    )
    .await
    .unwrap();
    let late = build_blocks(
        &writer,
        &mut index,
        "tenant-a",
        7,
        &[rec([1; 16], 2, Some(1), 200)],
        (20, 20),
    )
    .await
    .unwrap();
    let input_keys = vec![first[0].object_key.clone(), late[0].object_key.clone()];
    let output_key = compacted_object_key("tenant-a", 7, 10, 20, 100);

    let meta = compact_block_keys(
        store.clone(),
        &writer,
        &mut index,
        "tenant-a",
        &input_keys,
        &output_key,
    )
    .await
    .unwrap();

    assert!(meta.object_key == output_key);
    assert!(meta.row_count == 2);
    assert!(meta.min_ts == 100);
    assert!(meta.max_ts == 200);
    assert!(
        index.candidate_blocks_for_trace("tenant-a", &[1; 16], 0, 1_000)
            == vec![output_key.clone()]
    );
    assert!(
        index.prune_blocks_by_tag("tenant-a", "service.name", Some("api"), 0, 1_000)
            == vec![output_key.clone()]
    );

    let batches = read_block(store, &output_key).await.unwrap();
    assert!(
        batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>()
            == 2
    );
}

#[tokio::test]
async fn compact_block_keys_recomputes_nested_sets_for_late_children() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();

    let first = build_blocks(
        &writer,
        &mut index,
        "tenant-a",
        7,
        &[rec([1; 16], 1, None, 100)],
        (10, 10),
    )
    .await
    .unwrap();
    let late = build_blocks(
        &writer,
        &mut index,
        "tenant-a",
        7,
        &[rec([1; 16], 2, Some(1), 200)],
        (20, 20),
    )
    .await
    .unwrap();
    let input_keys = vec![first[0].object_key.clone(), late[0].object_key.clone()];
    let output_key = compacted_object_key("tenant-a", 7, 10, 20, 100);

    compact_block_keys(
        store.clone(),
        &writer,
        &mut index,
        "tenant-a",
        &input_keys,
        &output_key,
    )
    .await
    .unwrap();

    let batches = read_block(store, &output_key).await.unwrap();
    let batch = &batches[0];
    let span_ids = batch
        .column_by_name(crabka_blockstore::SCOL_SPAN_ID)
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let left = batch
        .column_by_name(crabka_blockstore::SCOL_NESTED_SET_LEFT)
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let right = batch
        .column_by_name(crabka_blockstore::SCOL_NESTED_SET_RIGHT)
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let parent_id = batch
        .column_by_name(crabka_blockstore::SCOL_PARENT_ID)
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    let root = (0..batch.num_rows())
        .find(|row| span_ids.value(*row) == [1; 8])
        .unwrap();
    let child = (0..batch.num_rows())
        .find(|row| span_ids.value(*row) == [2; 8])
        .unwrap();

    assert!(parent_id.value(child) == left.value(root));
    assert!(left.value(root) < left.value(child));
    assert!(right.value(child) < right.value(root));
}
