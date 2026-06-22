use arrow::array::{Array, Int64Array, StringArray};
use assert2::assert;
use crabka_blockstore::{
    SCOL_ROOT_SPAN_NAME, SCOL_TRACE_ID, SCOL_TRACE_START_NANO, span_block_schema,
};
use crabka_traceql::{AttrValue as TraceqlAttrValue, ScopedTag, TagScope, TypedValue};
use crabka_traces::{
    AttrValue, EventRecord, KeyValue, LinkRecord, LiveStore, Span, SpanKind, SpanRecord,
    StatusCode, livestore::ingest_wal_payloads, querier::live::LiveSource,
};
use datafusion::catalog::TableProvider;

fn span(trace_id: [u8; 16], span_id: u8, start_ns: i64) -> Span {
    Span {
        trace_id,
        span_id: [span_id; 8],
        parent_span_id: None,
        name: format!("span-{span_id}"),
        kind: SpanKind::Server,
        start_ns,
        duration_ns: 10,
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

fn record(tenant: &str, span: Span) -> SpanRecord {
    SpanRecord {
        tenant: tenant.into(),
        span,
    }
}

#[test]
fn assembles_recent_trace_by_id() {
    let mut store = LiveStore::new(i64::MAX);
    store.ingest(record("tenant-a", span([1; 16], 2, 20)));
    store.ingest(record("tenant-a", span([2; 16], 1, 10)));
    store.ingest(record("tenant-a", span([1; 16], 1, 10)));
    store.ingest(record("tenant-b", span([1; 16], 9, 5)));

    let trace = store.trace_by_id("tenant-a", &[1; 16]);
    assert!(trace.iter().map(|span| span.span_id).collect::<Vec<_>>() == vec![[1; 8], [2; 8]]);
    assert!(store.trace_by_id("tenant-a", &[2; 16]).len() == 1);
    assert!(store.trace_by_id("tenant-b", &[1; 16]).len() == 1);
    assert!(store.trace_by_id("missing", &[1; 16]).is_empty());
}

#[test]
fn evicts_spans_older_than_retention_window() {
    let mut store = LiveStore::new(50);
    store.ingest(record("tenant-a", span([1; 16], 1, 100)));
    store.ingest(record("tenant-a", span([1; 16], 2, 149)));
    store.ingest(record("tenant-a", span([1; 16], 3, 151)));

    let trace = store.trace_by_id("tenant-a", &[1; 16]);
    assert!(trace.iter().map(|span| span.span_id).collect::<Vec<_>>() == vec![[2; 8], [3; 8]]);
}

#[test]
fn exposes_recent_spans_as_mem_table_over_span_schema() {
    let mut store = LiveStore::new(i64::MAX);
    store.ingest(record("tenant-a", span([1; 16], 1, 10)));
    store.ingest(record("tenant-a", span([2; 16], 1, 20)));

    let table = store.mem_table("tenant-a").unwrap();
    assert!(table.schema() == span_block_schema());
    assert!(table.schema().index_of(SCOL_TRACE_ID).is_ok());
}

#[tokio::test]
async fn live_source_exposes_trace_spans_and_tags() {
    let mut store = LiveStore::new(i64::MAX);
    store.ingest(record("tenant-a", span([1; 16], 1, 10)));
    let mut child = span([1; 16], 2, 20);
    child.parent_span_id = Some([1; 8]);
    child.status_message = "retryable".into();
    child.instrumentation_scope = "otel-rust".into();
    child.instrumentation_version = "1.2.3".into();
    store.ingest(record("tenant-a", child));

    let trace = store
        .trace_spans("tenant-a", &[1; 16])
        .await
        .unwrap()
        .unwrap();
    assert!(trace.root_service_name == "api");
    assert!(trace.root_trace_name == "span-1");
    assert!(trace.spans.len() == 2);
    assert!(
        trace.spans[0].attributes
            == vec![("http.method".into(), TraceqlAttrValue::Str("GET".into()))]
    );

    let names = store.tag_names("tenant-a", None, 0, 100).await.unwrap();
    assert!(
        names
            .iter()
            .any(|tags| tags.scope == TagScope::Resource && tags.tags == vec!["service.name"])
    );
    assert!(
        names
            .iter()
            .any(|tags| tags.scope == TagScope::Span && tags.tags == vec!["http.method"])
    );
    assert_tag_scope_contains(
        &names,
        TagScope::Intrinsic,
        &["span:parentID", "span:statusMessage", "trace:duration"],
    );
    assert_tag_scope_contains(
        &names,
        TagScope::Instrumentation,
        &["instrumentation:name", "instrumentation:version"],
    );

    let values = store
        .tag_values("tenant-a", ".http.method", 0, 100)
        .await
        .unwrap();
    assert_typed_value(&values, "string", "GET");
    let parent_ids = store
        .tag_values("tenant-a", "span:parentID", 0, 100)
        .await
        .unwrap();
    assert_typed_value(&parent_ids, "string", "0101010101010101");
    let status_messages = store
        .tag_values("tenant-a", "span:statusMessage", 0, 100)
        .await
        .unwrap();
    assert_typed_value(&status_messages, "string", "retryable");
    let instrumentation_names = store
        .tag_values("tenant-a", "instrumentation:name", 0, 100)
        .await
        .unwrap();
    assert_typed_value(&instrumentation_names, "string", "otel-rust");
    let instrumentation_versions = store
        .tag_values("tenant-a", "instrumentation:version", 0, 100)
        .await
        .unwrap();
    assert_typed_value(&instrumentation_versions, "string", "1.2.3");
    let trace_root_names = store
        .tag_values("tenant-a", "trace:rootName", 0, 100)
        .await
        .unwrap();
    assert_typed_value(&trace_root_names, "string", "span-1");
    let trace_root_services = store
        .tag_values("tenant-a", "trace:rootService", 0, 100)
        .await
        .unwrap();
    assert_typed_value(&trace_root_services, "string", "api");
    let trace_durations = store
        .tag_values("tenant-a", "trace:duration", 0, 100)
        .await
        .unwrap();
    assert_typed_value(&trace_durations, "duration", "20");
}

#[tokio::test]
async fn live_trace_spans_keep_resource_attrs_out_of_span_attrs() {
    let mut store = LiveStore::new(i64::MAX);
    let mut item = span([1; 16], 1, 10);
    item.resource_attrs.push(KeyValue {
        key: "cloud.region".into(),
        value: AttrValue::Str("us-east-1".into()),
    });
    store.ingest(record("tenant-a", item));

    let trace = store
        .trace_spans("tenant-a", &[1; 16])
        .await
        .unwrap()
        .unwrap();

    assert!(
        trace.spans[0].attributes
            == vec![("http.method".into(), TraceqlAttrValue::Str("GET".into()))]
    );
}

fn assert_tag_scope_contains(tags: &[ScopedTag], scope: TagScope, expected: &[&str]) {
    assert!(tags.iter().any(|tags| {
        tags.scope == scope
            && expected
                .iter()
                .all(|expected| tags.tags.contains(&(*expected).to_string()))
    }));
}

fn assert_typed_value(values: &[TypedValue], type_: &str, value: &str) {
    assert!(
        values
            .iter()
            .any(|got| got.type_ == type_ && got.value == value)
    );
}

#[tokio::test]
async fn live_source_exposes_event_and_link_tags() {
    let mut store = LiveStore::new(i64::MAX);
    let mut span = span([1; 16], 1, 10);
    span.events.push(EventRecord {
        time_unix_nano: 17,
        name: "cache.miss".into(),
        attrs: vec![KeyValue {
            key: "cache.key".into(),
            value: AttrValue::Str("users/7".into()),
        }],
    });
    span.links.push(LinkRecord {
        trace_id: [2; 16],
        span_id: [3; 8],
        attrs: vec![KeyValue {
            key: "link.kind".into(),
            value: AttrValue::Str("follows-from".into()),
        }],
    });
    store.ingest(record("tenant-a", span));

    let event_tags = store
        .tag_names("tenant-a", Some(TagScope::Event), 0, 100)
        .await
        .unwrap();
    assert_tag_scope_contains(
        &event_tags,
        TagScope::Event,
        &["event:name", "event:timeSinceStart", "cache.key"],
    );
    let link_tags = store
        .tag_names("tenant-a", Some(TagScope::Link), 0, 100)
        .await
        .unwrap();
    assert_tag_scope_contains(
        &link_tags,
        TagScope::Link,
        &["link:traceID", "link:spanID", "link.kind"],
    );

    assert_typed_value(
        &store
            .tag_values("tenant-a", "event:name", 0, 100)
            .await
            .unwrap(),
        "string",
        "cache.miss",
    );
    assert_typed_value(
        &store
            .tag_values("tenant-a", "event:timeSinceStart", 0, 100)
            .await
            .unwrap(),
        "duration",
        "7",
    );
    assert_typed_value(
        &store
            .tag_values("tenant-a", "link:traceID", 0, 100)
            .await
            .unwrap(),
        "string",
        "02020202020202020202020202020202",
    );
    assert_typed_value(
        &store
            .tag_values("tenant-a", "link:spanID", 0, 100)
            .await
            .unwrap(),
        "string",
        "0303030303030303",
    );
}

#[tokio::test]
async fn live_source_batches_filter_by_time_range() {
    let mut store = LiveStore::new(i64::MAX);
    store.ingest(record("tenant-a", span([1; 16], 1, 10)));
    store.ingest(record("tenant-a", span([1; 16], 2, 200)));

    let batches = store.span_batches("tenant-a", 0, 100).await.unwrap();
    assert!(batches.len() == 1);
    assert!(batches[0].num_rows() == 1);
    assert!(store.block_builder_frontier_ns("tenant-a") == 200);
}

#[tokio::test]
async fn live_source_window_keeps_trace_level_columns_global() {
    // Root span starts at t=10 (outside the query window); a later child span
    // at t=200 falls inside the window. A window that clips the trace must not
    // make the trace-level columns reflect only the in-window subset.
    let mut store = LiveStore::new(i64::MAX);
    store.ingest(record("tenant-a", span([1; 16], 1, 10)));
    let mut child = span([1; 16], 2, 200);
    child.parent_span_id = Some([1; 8]);
    store.ingest(record("tenant-a", child));

    // Window [150, 300] includes only the child span.
    let batches = store.span_batches("tenant-a", 150, 300).await.unwrap();
    assert!(batches.len() == 1);
    assert!(batches[0].num_rows() == 1);

    let trace_start = batches[0]
        .column_by_name(SCOL_TRACE_START_NANO)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let root_name = batches[0]
        .column_by_name(SCOL_ROOT_SPAN_NAME)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Trace-global start is the root span's t=10, NOT the in-window child's t=200.
    assert!(trace_start.value(0) == 10);
    // Root span name is the actual root ("span-1"), NOT the in-window child.
    assert!(root_name.value(0) == "span-1");
}

#[test]
fn ingests_encoded_wal_payloads() {
    let mut store = LiveStore::new(i64::MAX);
    let first = record("tenant-a", span([1; 16], 1, 10)).encode().unwrap();
    let second = record("tenant-a", span([1; 16], 2, 20)).encode().unwrap();

    let count = ingest_wal_payloads(&mut store, [&first[..], &second[..]]).unwrap();

    assert!(count == 2);
    assert!(store.trace_by_id("tenant-a", &[1; 16]).len() == 2);
}
