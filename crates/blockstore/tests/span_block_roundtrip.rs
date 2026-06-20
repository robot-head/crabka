//! Span blocks validate against the span declaration and round-trip through
//! object-store Parquet.

use std::sync::Arc;

use arrow::array::FixedSizeBinaryArray;
use arrow::record_batch::RecordBatch;
use crabka_blockstore::{
    AttrValue, BlockWriter, NestedSet, SpanAttr, SpanKind, SpanRow, StatusCode, SummaryColumns,
    encode_span_rows, read_block, span_block_decl, span_block_schema, validate_against,
};
use object_store::ObjectStore;
use object_store::memory::InMemory;

fn row(trace: u8, span: u8, left: i32) -> SpanRow {
    SpanRow {
        trace_id: [trace; 16],
        span_id: [span; 8],
        parent_span_id: None,
        nested_set: NestedSet {
            nested_set_left: left,
            nested_set_right: left + 1,
            parent_id: 0,
        },
        root_service_name: Some("svc".into()),
        root_span_name: Some("root".into()),
        trace_start_unix_nano: 100,
        trace_duration_nanos: 10,
        name: Some("op".into()),
        kind: SpanKind::Server,
        start_unix_nano: 100,
        duration_nanos: 5,
        status_code: StatusCode::Ok,
        status_message: None,
        attrs: vec![SpanAttr {
            key: "k".into(),
            is_array: false,
            value: AttrValue::Int(vec![7]),
        }],
        events: vec![],
        links: vec![],
    }
}

#[tokio::test]
async fn span_block_validates_and_round_trips() {
    let batch = encode_span_rows(&[row(1, 1, 1), row(1, 2, 2)]).unwrap();
    validate_against(&batch.schema(), &span_block_decl()).unwrap();

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    BlockWriter::new(store.clone())
        .write_block_with_decl(
            "tenant",
            "blocks/spans.parquet",
            span_block_schema(),
            &[batch],
            &span_block_decl(),
            SummaryColumns::new("trace_id", "start_unix_nano"),
        )
        .await
        .unwrap();

    let back = read_block(store, "blocks/spans.parquet").await.unwrap();
    let total: usize = back.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 2);
    let tids = back[0]
        .column_by_name("trace_id")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(tids.value(0), &[1_u8; 16]);
}
