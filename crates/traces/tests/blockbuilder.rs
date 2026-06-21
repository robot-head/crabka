use arrow::array::{DictionaryArray, StringArray};
use arrow::datatypes::Int32Type;
use assert2::assert;
use bytes::Bytes;
use crabka_blockstore::{BlockWriter, PromotedSpanAttr, TraceIndex, read_block};
use crabka_client_consumer::ConsumerRecord;
use crabka_traces::{
    AttrValue, KeyValue, Span, SpanKind, SpanRecord, StatusCode,
    blockbuilder::{
        build_blocks, build_blocks_with_prefix, build_blocks_with_promoted_attrs,
        decode_consumer_records, flush_partition_windows, group_by_trace, object_key,
    },
};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use std::sync::Arc;

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

fn rec(
    tenant: &str,
    trace_id: [u8; 16],
    span_id: u8,
    parent: Option<u8>,
    start_ns: i64,
) -> SpanRecord {
    SpanRecord {
        tenant: tenant.into(),
        span: span(trace_id, span_id, parent, start_ns),
    }
}

fn consumer_record(partition: i32, offset: i64, record: &SpanRecord) -> ConsumerRecord {
    ConsumerRecord {
        topic: "__crabka_traces_wal".into(),
        partition,
        offset,
        leader_epoch: 0,
        timestamp: 0,
        key: None,
        value: Some(Bytes::from(record.encode().unwrap())),
        headers: Vec::new(),
    }
}

#[test]
fn object_key_is_deterministic_and_offset_scoped() {
    let a = object_key("tenant-a", 3, 10, 20, 1_000);
    let b = object_key("tenant-a", 3, 10, 20, 1_000);
    let c = object_key("tenant-a", 3, 10, 21, 1_000);

    assert!(a == b);
    assert!(a != c);
    assert!(a == "traces/tenant-a/00003/00000000000000000010-00000000000000000020-1000.parquet");
}

#[test]
fn group_by_trace_orders_spans_per_tenant_trace() {
    let records = vec![
        rec("tenant-a", [1; 16], 2, Some(1), 200),
        rec("tenant-b", [1; 16], 9, None, 50),
        rec("tenant-a", [1; 16], 1, None, 100),
    ];

    let grouped = group_by_trace(&records);
    let group = &grouped[&("tenant-a".to_string(), [1; 16])];

    assert!(group.iter().map(|span| span.span_id).collect::<Vec<_>>() == vec![[1; 8], [2; 8]]);
    assert!(grouped[&("tenant-b".to_string(), [1; 16])][0].span_id == [9; 8]);
}

#[test]
fn decode_consumer_records_groups_by_partition_and_tracks_offsets() {
    let windows = decode_consumer_records(&[
        consumer_record(1, 11, &rec("tenant-a", [1; 16], 1, None, 100)),
        consumer_record(1, 12, &rec("tenant-a", [1; 16], 2, Some(1), 200)),
        consumer_record(2, 7, &rec("tenant-b", [2; 16], 1, None, 50)),
        ConsumerRecord {
            topic: "__crabka_traces_wal".into(),
            partition: 1,
            offset: 13,
            leader_epoch: 0,
            timestamp: 0,
            key: None,
            value: None,
            headers: Vec::new(),
        },
    ])
    .unwrap();

    assert!(windows.len() == 2);
    assert!(windows[&1].offset_range == (11, 12));
    assert!(windows[&1].records.len() == 2);
    assert!(windows[&2].offset_range == (7, 7));
    assert!(windows[&2].records[0].tenant == "tenant-b");
}

#[tokio::test]
async fn build_blocks_writes_span_block_and_updates_trace_index() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();
    let records = vec![
        rec("tenant-a", [1; 16], 2, Some(1), 200),
        rec("tenant-a", [1; 16], 1, None, 100),
        rec("tenant-b", [2; 16], 1, None, 50),
    ];

    let metas = build_blocks(&writer, &mut index, "tenant-a", 7, &records, (10, 20))
        .await
        .unwrap();

    assert!(metas.len() == 1);
    assert!(metas[0].tenant == "tenant-a");
    assert!(metas[0].row_count == 2);
    assert!(metas[0].min_ts == 100);
    assert!(metas[0].max_ts == 200);

    let batches = read_block(store, &metas[0].object_key).await.unwrap();
    assert!(
        batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>()
            == 2
    );
    assert!(
        index.candidate_blocks_for_trace("tenant-a", &[1; 16], 0, 1_000)
            == vec![metas[0].object_key.clone()]
    );
    assert!(
        index.prune_blocks_by_tag("tenant-a", "service.name", Some("api"), 0, 1_000)
            == vec![metas[0].object_key.clone()]
    );
    assert!(
        index
            .candidate_blocks_for_trace("tenant-b", &[2; 16], 0, 1_000)
            .is_empty()
    );
}

#[tokio::test]
async fn replaying_same_offset_window_is_idempotent_in_trace_index() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();
    let records = vec![
        rec("tenant-a", [1; 16], 2, Some(1), 200),
        rec("tenant-a", [1; 16], 1, None, 100),
    ];

    let first = build_blocks(&writer, &mut index, "tenant-a", 7, &records, (10, 20))
        .await
        .unwrap();
    let replay = build_blocks(&writer, &mut index, "tenant-a", 7, &records, (10, 20))
        .await
        .unwrap();

    assert!(first[0].object_key == replay[0].object_key);
    assert!(
        index.candidate_blocks_for_trace("tenant-a", &[1; 16], 0, 1_000)
            == vec![first[0].object_key.clone()]
    );
    assert!(
        index.prune_blocks_by_tag("tenant-a", "service.name", Some("api"), 0, 1_000)
            == vec![first[0].object_key.clone()]
    );
    let batches = read_block(store, &first[0].object_key).await.unwrap();
    assert!(
        batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>()
            == 2
    );
}

#[tokio::test]
async fn replaying_saved_partition_window_after_restart_is_idempotent() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let config = crabka_traces::blockbuilder::BlockBuilderConfig {
        object_key_prefix: String::new(),
        index_key: "index/traces.json".into(),
        window: std::time::Duration::from_millis(1),
        promoted_attrs: Vec::new(),
    };
    let records = [
        consumer_record(7, 10, &rec("tenant-a", [1; 16], 2, Some(1), 200)),
        consumer_record(7, 11, &rec("tenant-a", [1; 16], 1, None, 100)),
    ];
    let windows = decode_consumer_records(&records).unwrap();
    let mut index = TraceIndex::new();

    flush_partition_windows(&writer, &mut index, store.clone(), &config, windows.clone())
        .await
        .unwrap();
    let mut restarted = TraceIndex::load(&store, "index/traces.json").await.unwrap();

    flush_partition_windows(&writer, &mut restarted, store.clone(), &config, windows)
        .await
        .unwrap();
    let reloaded = TraceIndex::load(&store, "index/traces.json").await.unwrap();

    assert!(
        reloaded.candidate_blocks_for_trace("tenant-a", &[1; 16], 0, 1_000)
            == vec![
                "traces/tenant-a/00007/00000000000000000010-00000000000000000011-100.parquet"
                    .to_string()
            ]
    );
}

#[tokio::test]
async fn build_blocks_with_prefix_scopes_block_keys() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();
    let records = vec![rec("tenant-a", [1; 16], 1, None, 100)];

    let metas = build_blocks_with_prefix(
        &writer,
        &mut index,
        "tempo/traces",
        "tenant-a",
        7,
        &records,
        (10, 20),
    )
    .await
    .unwrap();

    assert!(metas.len() == 1);
    assert!(
        metas[0].object_key
            == "tempo/traces/traces/tenant-a/00007/00000000000000000010-00000000000000000020-100.parquet"
    );
    assert!(read_block(store, &metas[0].object_key).await.is_ok());
    assert!(
        index.candidate_blocks_for_trace("tenant-a", &[1; 16], 0, 1_000)
            == vec![metas[0].object_key.clone()]
    );
}

#[tokio::test]
async fn build_blocks_promotes_configured_attribute_columns() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();
    let records = vec![rec("tenant-a", [1; 16], 1, None, 100)];

    let metas = build_blocks_with_promoted_attrs(
        &writer,
        &mut index,
        "tenant-a",
        7,
        &records,
        (10, 20),
        &[PromotedSpanAttr::string("http.method")],
    )
    .await
    .unwrap();

    let batches = read_block(store, &metas[0].object_key).await.unwrap();
    let methods = batches[0]
        .column_by_name("attr.http.method")
        .unwrap()
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .unwrap();
    let values = methods
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let key = usize::try_from(methods.keys().value(0)).unwrap();
    assert!(values.value(key) == "GET");
}
