use arrow::array::{DictionaryArray, StringArray};
use arrow::datatypes::Int32Type;
use assert2::assert;
use bytes::Bytes;
use crabka_blockstore::{BlockWriter, PromotedSpanAttr, TraceIndex, read_block};
use crabka_client_consumer::ConsumerRecord;
use crabka_traces::{
    AttrValue, KeyValue, Span, SpanKind, SpanRecord, StatusCode, TracesError,
    blockbuilder::{
        BlockBuilderConfig, WalConsumerCommit, WalConsumerPoll, build_blocks,
        build_blocks_with_prefix, build_blocks_with_promoted_attrs, decode_consumer_records,
        flush_partition_windows, group_by_trace, object_key, run,
    },
};
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use object_store::{ObjectStore, ObjectStoreExt};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

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
        flush_max_records: crabka_traces::blockbuilder::DEFAULT_FLUSH_MAX_RECORDS,
        flush_max_age: crabka_traces::blockbuilder::DEFAULT_FLUSH_MAX_AGE,
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
    let mut restarted = TraceIndex::load_latest_snapshot(&store, "index/traces.json")
        .await
        .unwrap();

    flush_partition_windows(&writer, &mut restarted, store.clone(), &config, windows)
        .await
        .unwrap();
    let reloaded = TraceIndex::load_latest_snapshot(&store, "index/traces.json")
        .await
        .unwrap();

    assert!(
        store
            .head(&object_store::path::Path::from("index/traces.json"))
            .await
            .is_err()
    );

    assert!(
        reloaded.candidate_blocks_for_trace("tenant-a", &[1; 16], 0, 1_000)
            == vec![
                "traces/tenant-a/00007/00000000000000000010-00000000000000000011-100.parquet"
                    .to_string()
            ]
    );
}

#[tokio::test]
async fn multiple_polls_below_threshold_flush_one_block_per_partition() {
    use crabka_traces::blockbuilder::{BlockBuilderConfig, FlushAccumulator};
    use tokio::time::Instant;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let config = BlockBuilderConfig {
        object_key_prefix: String::new(),
        index_key: "index/traces.json".into(),
        window: std::time::Duration::from_millis(1),
        promoted_attrs: Vec::new(),
        flush_max_records: 50_000,
        flush_max_age: std::time::Duration::from_mins(1),
    };

    // Three polls, each well under the flush threshold, all for the same trace
    // across two polls plus a second trace in the third poll.
    let poll1 = decode_consumer_records(&[consumer_record(
        7,
        10,
        &rec("tenant-a", [1; 16], 1, None, 100),
    )])
    .unwrap();
    let poll2 = decode_consumer_records(&[consumer_record(
        7,
        11,
        &rec("tenant-a", [1; 16], 2, Some(1), 200),
    )])
    .unwrap();
    let poll3 = decode_consumer_records(&[consumer_record(
        7,
        12,
        &rec("tenant-a", [2; 16], 3, None, 300),
    )])
    .unwrap();

    let mut accumulator = FlushAccumulator::new();
    accumulator.merge(poll1, Instant::now());
    assert!(!accumulator.should_flush(&config, Instant::now()));
    accumulator.merge(poll2, Instant::now());
    assert!(!accumulator.should_flush(&config, Instant::now()));
    accumulator.merge(poll3, Instant::now());
    assert!(!accumulator.should_flush(&config, Instant::now()));
    assert!(accumulator.record_count() == 3);

    // A single flush of the merged buffer writes ONE block for the partition
    // (not one block per poll), covering the full offset range 10..=12.
    let windows = accumulator.take();
    assert!(accumulator.is_empty());
    let mut index = TraceIndex::new();
    flush_partition_windows(&writer, &mut index, store.clone(), &config, windows)
        .await
        .unwrap();

    let key = "traces/tenant-a/00007/00000000000000000010-00000000000000000012-100.parquet";
    let batches = read_block(store, key).await.unwrap();
    // Both spans of trace [1;16] grouped across polls + the lone span of [2;16].
    assert!(
        batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>()
            == 3
    );
    assert!(
        index.candidate_blocks_for_trace("tenant-a", &[1; 16], 0, 1_000) == vec![key.to_string()]
    );
    assert!(
        index.candidate_blocks_for_trace("tenant-a", &[2; 16], 0, 1_000) == vec![key.to_string()]
    );
}

#[tokio::test]
async fn accumulator_flushes_on_record_count_threshold() {
    use crabka_traces::blockbuilder::{BlockBuilderConfig, FlushAccumulator};
    use tokio::time::Instant;

    let config = BlockBuilderConfig {
        object_key_prefix: String::new(),
        index_key: "index/traces.json".into(),
        window: std::time::Duration::from_millis(1),
        promoted_attrs: Vec::new(),
        flush_max_records: 2,
        flush_max_age: std::time::Duration::from_mins(1),
    };

    let mut accumulator = FlushAccumulator::new();
    accumulator.merge(
        decode_consumer_records(&[consumer_record(0, 1, &rec("t", [1; 16], 1, None, 1))]).unwrap(),
        Instant::now(),
    );
    // One record buffered, below the threshold of 2.
    assert!(!accumulator.should_flush(&config, Instant::now()));

    accumulator.merge(
        decode_consumer_records(&[consumer_record(0, 2, &rec("t", [1; 16], 2, None, 2))]).unwrap(),
        Instant::now(),
    );
    // Two records buffered -> threshold reached.
    assert!(accumulator.should_flush(&config, Instant::now()));
}

#[tokio::test]
async fn accumulator_flushes_on_age_for_low_traffic_stream() {
    use crabka_traces::blockbuilder::{BlockBuilderConfig, FlushAccumulator};
    use tokio::time::Instant;

    let config = BlockBuilderConfig {
        object_key_prefix: String::new(),
        index_key: "index/traces.json".into(),
        window: std::time::Duration::from_millis(1),
        promoted_attrs: Vec::new(),
        flush_max_records: 50_000,
        flush_max_age: std::time::Duration::from_mins(1),
    };

    let mut accumulator = FlushAccumulator::new();
    let start = Instant::now();
    accumulator.merge(
        decode_consumer_records(&[consumer_record(0, 1, &rec("t", [1; 16], 1, None, 1))]).unwrap(),
        start,
    );

    // Far below the record threshold, and the oldest record is young.
    assert!(!accumulator.should_flush(&config, start + std::time::Duration::from_secs(59)));
    // Once the oldest buffered record ages past flush_max_age, flush anyway so a
    // low-traffic stream stays queryable.
    assert!(accumulator.should_flush(&config, start + std::time::Duration::from_mins(1)));
}

#[tokio::test]
async fn shutdown_drain_flushes_remaining_buffer_without_losing_spans() {
    use crabka_traces::blockbuilder::{BlockBuilderConfig, FlushAccumulator};
    use tokio::time::Instant;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let config = BlockBuilderConfig {
        object_key_prefix: String::new(),
        index_key: "index/traces.json".into(),
        window: std::time::Duration::from_millis(1),
        promoted_attrs: Vec::new(),
        flush_max_records: 50_000,
        flush_max_age: std::time::Duration::from_mins(1),
    };

    // Two polls buffered, never reaching the flush threshold (mirrors a pending
    // buffer at shutdown).
    let mut accumulator = FlushAccumulator::new();
    accumulator.merge(
        decode_consumer_records(&[consumer_record(
            3,
            5,
            &rec("tenant-a", [4; 16], 1, None, 100),
        )])
        .unwrap(),
        Instant::now(),
    );
    accumulator.merge(
        decode_consumer_records(&[consumer_record(
            3,
            6,
            &rec("tenant-a", [4; 16], 2, Some(1), 200),
        )])
        .unwrap(),
        Instant::now(),
    );
    assert!(!accumulator.should_flush(&config, Instant::now()));

    // The shutdown drain path: a non-empty buffer is flushed before exit.
    assert!(!accumulator.is_empty());
    let windows = accumulator.take();
    let mut index = TraceIndex::new();
    flush_partition_windows(&writer, &mut index, store.clone(), &config, windows)
        .await
        .unwrap();

    // No spans lost: both buffered spans land in the durable block.
    let key = "traces/tenant-a/00003/00000000000000000005-00000000000000000006-100.parquet";
    let batches = read_block(store, key).await.unwrap();
    assert!(
        batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>()
            == 2
    );
}

#[tokio::test]
async fn merged_buffer_offset_range_is_stable_for_idempotent_keying() {
    use crabka_traces::blockbuilder::FlushAccumulator;
    use tokio::time::Instant;

    // Polls arriving out of order still produce a buffer whose offset range spans
    // every buffered record, so the derived block key is stable across re-runs.
    let mut accumulator = FlushAccumulator::new();
    accumulator.merge(
        decode_consumer_records(&[consumer_record(7, 12, &rec("t", [1; 16], 2, None, 200))])
            .unwrap(),
        Instant::now(),
    );
    accumulator.merge(
        decode_consumer_records(&[consumer_record(7, 10, &rec("t", [1; 16], 1, None, 100))])
            .unwrap(),
        Instant::now(),
    );
    accumulator.merge(
        decode_consumer_records(&[consumer_record(7, 11, &rec("t", [1; 16], 3, None, 150))])
            .unwrap(),
        Instant::now(),
    );

    let windows = accumulator.take();
    let window = &windows[&7];
    assert!(window.offset_range == (10, 12));
    assert!(window.records.len() == 3);
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

/// Shared, ordered event log: object-store `put`s and consumer `commit`s push
/// markers here so a test can assert the durable write strictly precedes the
/// offset commit (the at-least-once invariant of `flush_and_commit`).
type EventLog = Arc<StdMutex<Vec<String>>>;

/// Object store that records every `put` (block / index write) into a shared
/// event log, optionally failing `put` once a flush is reached. Everything else
/// delegates to an inner [`InMemory`] store.
struct RecordingObjectStore {
    inner: Arc<InMemory>,
    events: EventLog,
    fail_puts: bool,
}

impl RecordingObjectStore {
    fn recording(events: EventLog) -> Self {
        Self {
            inner: Arc::new(InMemory::new()),
            events,
            fail_puts: false,
        }
    }

    fn failing(events: EventLog) -> Self {
        Self {
            inner: Arc::new(InMemory::new()),
            events,
            fail_puts: true,
        }
    }
}

impl std::fmt::Debug for RecordingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecordingObjectStore")
    }
}

impl std::fmt::Display for RecordingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecordingObjectStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for RecordingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if self.fail_puts {
            return Err(object_store::Error::Generic {
                store: "RecordingObjectStore",
                source: "injected put failure".into(),
            });
        }
        self.events
            .lock()
            .expect("events lock")
            .push(format!("put:{location}"));
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Scripted WAL consumer: returns each queued batch in turn, then cancels the
/// shutdown token and returns empty so `run` exits its loop and reaches the
/// drain path. Every `commit_sync` is recorded both as a count and as an
/// ordered marker in the shared event log.
struct ScriptedConsumer {
    batches: std::collections::VecDeque<Vec<ConsumerRecord>>,
    shutdown: CancellationToken,
    commit_calls: Arc<AtomicUsize>,
    events: EventLog,
}

impl ScriptedConsumer {
    fn new(
        batches: Vec<Vec<ConsumerRecord>>,
        shutdown: CancellationToken,
        commit_calls: Arc<AtomicUsize>,
        events: EventLog,
    ) -> Self {
        Self {
            batches: batches.into(),
            shutdown,
            commit_calls,
            events,
        }
    }
}

#[async_trait::async_trait]
impl WalConsumerPoll for ScriptedConsumer {
    async fn poll(
        &mut self,
        _window: std::time::Duration,
    ) -> Result<Vec<ConsumerRecord>, TracesError> {
        if let Some(batch) = self.batches.pop_front() {
            Ok(batch)
        } else {
            // Scripted input exhausted: stop the loop so `run` proceeds to its
            // shutdown drain on whatever is still buffered.
            self.shutdown.cancel();
            Ok(Vec::new())
        }
    }
}

#[async_trait::async_trait]
impl WalConsumerCommit for ScriptedConsumer {
    async fn commit_sync(&mut self) -> Result<(), TracesError> {
        self.commit_calls.fetch_add(1, Ordering::SeqCst);
        self.events
            .lock()
            .expect("events lock")
            .push("commit".to_string());
        Ok(())
    }
}

fn block_builder_config() -> BlockBuilderConfig {
    BlockBuilderConfig {
        object_key_prefix: String::new(),
        index_key: "index/traces.json".into(),
        window: std::time::Duration::from_millis(1),
        promoted_attrs: Vec::new(),
        // Below-threshold counts never trip the count flush; the loop drains on
        // shutdown instead, exercising the drain path the tests target.
        flush_max_records: 50_000,
        flush_max_age: std::time::Duration::from_hours(1),
    }
}

#[tokio::test]
async fn run_commits_offsets_only_after_a_durable_block_write() {
    let events: EventLog = Arc::new(StdMutex::new(Vec::new()));
    let store = Arc::new(RecordingObjectStore::recording(Arc::clone(&events)));
    let object_store: Arc<dyn ObjectStore> = store.clone();
    let writer = BlockWriter::new(object_store.clone());
    let index = Arc::new(Mutex::new(TraceIndex::new()));
    let shutdown = CancellationToken::new();
    let commit_calls = Arc::new(AtomicUsize::new(0));

    // One poll of two spans for the same trace, well below the flush threshold,
    // so the only flush+commit happens on the shutdown drain.
    let batch = vec![
        consumer_record(3, 10, &rec("tenant-a", [1; 16], 1, None, 100)),
        consumer_record(3, 11, &rec("tenant-a", [1; 16], 2, Some(1), 200)),
    ];
    let consumer = ScriptedConsumer::new(
        vec![batch],
        shutdown.clone(),
        Arc::clone(&commit_calls),
        Arc::clone(&events),
    );

    run(
        consumer,
        writer,
        Arc::clone(&index),
        object_store.clone(),
        block_builder_config(),
        shutdown,
    )
    .await
    .unwrap();

    // Commit happened exactly once.
    assert!(commit_calls.load(Ordering::SeqCst) == 1);

    // ...and strictly AFTER the durable writes: every `put` marker precedes the
    // single `commit` marker. Reordering `flush_and_commit` to commit-before-flush
    // would put "commit" first and fail this.
    let recorded = events.lock().expect("events lock").clone();
    let commit_index = recorded.iter().position(|e| e == "commit").unwrap();
    assert!(commit_index == recorded.len() - 1);
    assert!(
        recorded[..commit_index]
            .iter()
            .all(|e| e.starts_with("put:"))
    );
    assert!(commit_index >= 1);

    // The block is durable and the index references it: no data lost.
    let key = "traces/tenant-a/00003/00000000000000000010-00000000000000000011-100.parquet";
    let batches = read_block(object_store, key).await.unwrap();
    assert!(
        batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>()
            == 2
    );
    assert!(
        index
            .lock()
            .await
            .candidate_blocks_for_trace("tenant-a", &[1; 16], 0, 1_000)
            == vec![key.to_string()]
    );
}

#[tokio::test]
async fn run_does_not_commit_when_the_flush_write_fails() {
    let events: EventLog = Arc::new(StdMutex::new(Vec::new()));
    // Every `put` errors, so `flush_partition_windows` fails on the first block
    // (or index) write and `flush_and_commit` returns Err before commit_sync.
    let store = Arc::new(RecordingObjectStore::failing(Arc::clone(&events)));
    let object_store: Arc<dyn ObjectStore> = store.clone();
    let writer = BlockWriter::new(object_store.clone());
    let index = Arc::new(Mutex::new(TraceIndex::new()));
    let shutdown = CancellationToken::new();
    let commit_calls = Arc::new(AtomicUsize::new(0));

    let batch = vec![consumer_record(
        3,
        10,
        &rec("tenant-a", [1; 16], 1, None, 100),
    )];
    let consumer = ScriptedConsumer::new(
        vec![batch],
        shutdown.clone(),
        Arc::clone(&commit_calls),
        Arc::clone(&events),
    );

    let result = run(
        consumer,
        writer,
        Arc::clone(&index),
        object_store,
        block_builder_config(),
        shutdown,
    )
    .await;

    // The drain flush failed, so `run` propagates the error...
    assert!(result.is_err());
    // ...and offsets were NOT committed: they stay behind the undurable data so a
    // restart re-reads them (at-least-once). Committing on the error path would
    // make this counter non-zero.
    assert!(commit_calls.load(Ordering::SeqCst) == 0);
    let recorded = events.lock().expect("events lock").clone();
    assert!(recorded.iter().all(|e| e != "commit"));
}

#[tokio::test]
async fn run_drains_remaining_buffer_exactly_once_on_shutdown() {
    let events: EventLog = Arc::new(StdMutex::new(Vec::new()));
    let store = Arc::new(RecordingObjectStore::recording(Arc::clone(&events)));
    let object_store: Arc<dyn ObjectStore> = store.clone();
    let writer = BlockWriter::new(object_store.clone());
    let index = Arc::new(Mutex::new(TraceIndex::new()));
    let shutdown = CancellationToken::new();
    let commit_calls = Arc::new(AtomicUsize::new(0));

    // Two below-threshold polls buffer four spans for one trace; the count never
    // trips `flush_max_records`, so nothing flushes inside the loop. Only the
    // shutdown drain should flush — exactly one block and exactly one commit.
    let consumer = ScriptedConsumer::new(
        vec![
            vec![
                consumer_record(3, 5, &rec("tenant-a", [4; 16], 1, None, 100)),
                consumer_record(3, 6, &rec("tenant-a", [4; 16], 2, Some(1), 200)),
            ],
            vec![
                consumer_record(3, 7, &rec("tenant-a", [4; 16], 3, Some(2), 300)),
                consumer_record(3, 8, &rec("tenant-a", [4; 16], 4, Some(3), 400)),
            ],
        ],
        shutdown.clone(),
        Arc::clone(&commit_calls),
        Arc::clone(&events),
    );

    run(
        consumer,
        writer,
        Arc::clone(&index),
        object_store.clone(),
        block_builder_config(),
        shutdown,
    )
    .await
    .unwrap();

    // Exactly one drain commit. Deleting the `if !accumulator.is_empty()` drain
    // block leaves the buffer unflushed -> zero commits -> this fails.
    assert!(commit_calls.load(Ordering::SeqCst) == 1);
    let recorded = events.lock().expect("events lock").clone();
    assert!(recorded.iter().filter(|e| *e == "commit").count() == 1);

    // No spans dropped: all four buffered spans land in the single drained block,
    // whose key spans the merged offset range 5..=8.
    let key = "traces/tenant-a/00003/00000000000000000005-00000000000000000008-100.parquet";
    let batches = read_block(object_store, key).await.unwrap();
    assert!(
        batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>()
            == 4
    );
    assert!(
        index
            .lock()
            .await
            .candidate_blocks_for_trace("tenant-a", &[4; 16], 0, 1_000)
            == vec![key.to_string()]
    );
}
