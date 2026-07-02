//! End-to-end (slice 1): DFS nested-set -> span block -> `BlockWriter` ->
//! `TraceIndex` bloom locate -> read back. Proves the by-id path is index-less
//! (bloom only),
//! across multiple traces, and that the nested-set columns survive the round-trip
//! with ancestor-contains-descendant interval containment intact.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow::array::{FixedSizeBinaryArray, Int32Array};
use assert2::{assert, check};
use crabka_blockstore::{
    AttrValue, BlockWriter, ShardedTraceBloom, SpanAttr, SpanKind, SpanNode, SpanRow, StatusCode,
    SummaryColumns, TraceBlockStats, TraceIndex, assign_nested_set, encode_span_rows, read_block,
    span_block_decl, span_block_schema,
};
use object_store::{ObjectStore, memory::InMemory};

fn sid(n: u8) -> [u8; 8] {
    [n, 0, 0, 0, 0, 0, 0, 0]
}

/// Build the span rows for one trace from its forest of `SpanNode`s, computing
/// the nested-set intervals via the slice-1 DFS builder. `service` is denormalized
/// onto each row as both the root service name and a `service.name` attribute.
fn build_trace(trace_id: [u8; 16], nodes: &[SpanNode], service: &str) -> Vec<SpanRow> {
    let ns = assign_nested_set(nodes);
    nodes
        .iter()
        .zip(&ns)
        .enumerate()
        .map(|(i, (node, nset))| SpanRow {
            trace_id,
            span_id: node.span_id,
            parent_span_id: node.parent_span_id,
            nested_set: *nset,
            child_count: 0,
            root_service_name: Some(service.to_string()),
            root_span_name: Some("POST /pay".into()),
            trace_start_unix_nano: 1_000,
            trace_duration_nanos: 300,
            name: Some(format!("span-{i}")),
            kind: SpanKind::Server,
            start_unix_nano: 1_000 + i64::try_from(i).unwrap(),
            duration_nanos: 10,
            status_code: StatusCode::Ok,
            status_message: None,
            instrumentation_name: Some("tracer".into()),
            instrumentation_version: None,
            attrs: vec![SpanAttr {
                key: "service.name".into(),
                is_array: false,
                value: AttrValue::Str(vec![service.to_string()]),
            }],
            events: vec![],
            links: vec![],
        })
        .collect()
}

/// The per-block trace footprint a block-builder (slice 4) will register: a bloom
/// seeded with every `trace_id` in the block plus the union of tag name/values.
fn block_stats(
    object_key: &str,
    min_ts: i64,
    max_ts: i64,
    trace_ids: &[[u8; 16]],
    service_names: &[&str],
) -> TraceBlockStats {
    // Sized as the trace-index unit tests do (8 shards, 64 items/shard, 1% FP):
    // `with_tempo_defaults` for a handful of items collapses to a 64-bit, high-k
    // shard that saturates and false-positives almost everything, which would
    // make the negative by-id assertions meaningless.
    let mut bloom = ShardedTraceBloom::new(8, 64, 0.01);
    for tid in trace_ids {
        bloom.insert(tid);
    }
    let mut tag_values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let entry = tag_values.entry("service.name".into()).or_default();
    for svc in service_names {
        entry.insert((*svc).to_string());
    }
    TraceBlockStats {
        object_key: object_key.to_string(),
        min_ts,
        max_ts,
        bloom,
        tag_names: BTreeSet::from(["service.name".to_string()]),
        tag_values,
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "single cohesive end-to-end pipeline test"
)]
async fn trace_block_built_indexed_and_located_by_id() {
    // Two distinct traces in the same span block.
    let trace_a = [9u8; 16];
    let trace_b = [17u8; 16];

    // Trace A: root(1) -> child(2) -> grandchild(3).
    let nodes_a = vec![
        SpanNode {
            span_id: sid(1),
            parent_span_id: None,
        },
        SpanNode {
            span_id: sid(2),
            parent_span_id: Some(sid(1)),
        },
        SpanNode {
            span_id: sid(3),
            parent_span_id: Some(sid(2)),
        },
    ];
    // Trace B: root(10) with two children (11, 12).
    let nodes_b = vec![
        SpanNode {
            span_id: sid(10),
            parent_span_id: None,
        },
        SpanNode {
            span_id: sid(11),
            parent_span_id: Some(sid(10)),
        },
        SpanNode {
            span_id: sid(12),
            parent_span_id: Some(sid(10)),
        },
    ];

    let mut rows = build_trace(trace_a, &nodes_a, "checkout");
    rows.extend(build_trace(trace_b, &nodes_b, "payments"));

    let batch = encode_span_rows(&rows).unwrap();

    // Span blocks validate against the span declaration, not the series one, so
    // they must be written via the declaration-aware path with span summary cols.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    BlockWriter::new(store.clone())
        .write_block_with_decl(
            "tenant",
            "blocks/t.parquet",
            span_block_schema(),
            &[batch],
            &span_block_decl(),
            SummaryColumns::new("trace_id", "start_unix_nano"),
        )
        .await
        .unwrap();

    // Build the TraceIndex footprint (what slice 4's block-builder will do).
    let mut idx = TraceIndex::new();
    idx.add_trace_block(
        "tenant",
        block_stats(
            "blocks/t.parquet",
            1_000,
            1_300,
            &[trace_a, trace_b],
            &["checkout", "payments"],
        ),
    );
    // A second, unrelated block holding only a third trace; it must never be a
    // by-id candidate for trace A or B.
    let trace_c = [200u8; 16];
    idx.add_trace_block(
        "tenant",
        block_stats(
            "blocks/other.parquet",
            5_000,
            5_300,
            &[trace_c],
            &["billing"],
        ),
    );

    // INDEX-LESS by-id locate: the bloom (not a global map) finds each trace's
    // block, and only that block.
    let cand_a = idx.candidate_blocks_for_trace("tenant", &trace_a, 0, 10_000);
    assert!(cand_a == vec!["blocks/t.parquet".to_string()]);
    let cand_b = idx.candidate_blocks_for_trace("tenant", &trace_b, 0, 10_000);
    assert!(cand_b == vec!["blocks/t.parquet".to_string()]);
    let cand_c = idx.candidate_blocks_for_trace("tenant", &trace_c, 0, 10_000);
    assert!(cand_c == vec!["blocks/other.parquet".to_string()]);

    // A trace_id the blooms never saw must not (almost surely) match any block.
    let never = [0u8; 16];
    assert!(
        idx.candidate_blocks_for_trace("tenant", &never, 0, 10_000)
            .is_empty()
    );

    // Read the located block back and confirm both traces survived the round-trip.
    let back = read_block(store, &cand_a[0]).await.unwrap();
    let total: usize = back
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum();
    assert!(total == 6);

    let b = &back[0];
    let trace_ids = b
        .column_by_name("trace_id")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let left = b
        .column_by_name("nested_set_left")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let right = b
        .column_by_name("nested_set_right")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    // Rows are grouped/sorted by trace_id: trace A occupies rows 0..3, the
    // grandchild (deepest) is row 2. The root interval must strictly contain it.
    assert!(trace_ids.value(0) == &trace_a);
    assert!(trace_ids.value(2) == &trace_a);
    check!(left.value(0) < left.value(2));
    check!(right.value(2) < right.value(0));

    // Sanity: every node's own interval is well-formed.
    for i in 0..b.num_rows() {
        check!(left.value(i) < right.value(i));
    }
}
