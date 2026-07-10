//! Span blocks validate against the span declaration and round-trip through
//! object-store Parquet.

use std::sync::Arc;

use arrow::{
    array::{DictionaryArray, FixedSizeBinaryArray, Int64Array, StringArray},
    datatypes::Int32Type,
    record_batch::RecordBatch,
};
use crabka_blockstore::{
    AttrValue, BlockWriter, NestedSet, PromotedSpanAttr, SpanAttr, SpanKind, SpanRow, StatusCode,
    SummaryColumns, encode_span_rows, encode_span_rows_with_promoted_attrs, read_block,
    span_block_decl, span_block_schema, span_block_schema_with_promoted_attrs, validate_against,
};
use object_store::{ObjectStore, memory::InMemory};

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
        child_count: 0,
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
        instrumentation_name: Some("tracer".into()),
        instrumentation_version: None,
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
async fn span_block_promotes_configured_attribute_columns() {
    let schema = span_block_schema_with_promoted_attrs(&[
        PromotedSpanAttr::string("http.method"),
        PromotedSpanAttr::int("http.status_code"),
    ]);
    let batch = encode_span_rows_with_promoted_attrs(
        &[SpanRow {
            attrs: vec![
                SpanAttr {
                    key: "http.method".into(),
                    is_array: false,
                    value: AttrValue::Str(vec!["GET".into()]),
                },
                SpanAttr {
                    key: "http.status_code".into(),
                    is_array: false,
                    value: AttrValue::Int(vec![200]),
                },
            ],
            ..row(1, 1, 1)
        }],
        &[
            PromotedSpanAttr::string("http.method"),
            PromotedSpanAttr::int("http.status_code"),
        ],
    )
    .unwrap();

    let methods = batch
        .column_by_name("attr.http.method")
        .unwrap()
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .unwrap();
    let method_values = methods
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let method_key = usize::try_from(methods.keys().value(0)).unwrap();
    let statuses = batch
        .column_by_name("attr.http.status_code")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert2::assert!(batch.schema() == schema);
    assert2::assert!(method_values.value(method_key) == "GET");
    assert2::assert!(statuses.value(0) == 200);

    validate_against(&batch.schema(), &span_block_decl()).unwrap();
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
    let tids = back[0]
        .column_by_name("trace_id")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert2::assert!(total == 2);
    assert2::assert!(tids.value(0) == [1_u8; 16].as_slice());
}
