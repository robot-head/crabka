use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    ops::Range,
    sync::Arc,
    time::Duration,
};

use assert2::check;
use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use crabka_blockstore::{
    BlockKey, LabelIndex, LogBlockIndex as BlockIndex, LogBlockStoreError, LogRow, TimeRange,
    labels, list_tenant_log_index_shard_ranges_from_object_store, log_block_object_path,
    read_log_block, read_log_block_from_object_store, read_log_index_manifest,
    read_tenant_log_index_manifest_from_object_store,
    read_tenant_log_index_shard_from_object_store, read_tenant_log_index_shards_from_object_store,
    series_fingerprint, write_log_block, write_log_block_to_object_store, write_log_index_manifest,
    write_tenant_log_index_manifest_to_object_store, write_tenant_log_index_shards_to_object_store,
};
use crabka_client_consumer::ConsumerError;
use crabka_observability::{
    CompactionFrontier, CompactionOffsetCommitter, KafkaWalHeader, KafkaWalRecord, LogWalConsumer,
    Offset, PartitionIndex, QuerierIndexSource, Role, ServiceConfig, ServiceDependencies,
    SharedCompactionFrontier, WalConsumerError, WalLogRecord, WalPosition, build_kafka_wal_record,
    build_service_router, compact_kafka_wal_records_to_object_store,
    compact_log_block_to_object_store, compact_next_kafka_wal_batch_to_object_store,
    compact_wal_records_to_object_store, read_compaction_frontier_from_object_store,
    run_compactor_once, run_compactor_until_idle, run_compactor_until_shutdown, serve_service,
    serve_service_listener, write_compaction_frontier_to_object_store,
};
use futures_util::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, local::LocalFileSystem,
    path::Path as ObjectPath,
};
use prost::bytes::Bytes;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
};
use tower::ServiceExt as _;

#[derive(Clone)]
struct RecordingObjectStore {
    inner: Arc<object_store::memory::InMemory>,
    get_paths: Arc<std::sync::Mutex<Vec<String>>>,
    put_paths: Arc<std::sync::Mutex<Vec<(String, usize)>>>,
}

impl RecordingObjectStore {
    fn new() -> Self {
        Self {
            inner: Arc::new(object_store::memory::InMemory::new()),
            get_paths: Arc::new(std::sync::Mutex::new(Vec::new())),
            put_paths: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn get_paths(&self) -> Vec<String> {
        self.get_paths.lock().unwrap().clone()
    }

    fn put_paths(&self) -> Vec<(String, usize)> {
        self.put_paths.lock().unwrap().clone()
    }
}

impl fmt::Debug for RecordingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RecordingObjectStore")
    }
}

impl fmt::Display for RecordingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RecordingObjectStore")
    }
}

async fn read_all_tenant_shard_indexes(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
) -> Result<(LabelIndex, BlockIndex), LogBlockStoreError> {
    read_tenant_log_index_shards_from_object_store(
        store,
        prefix,
        tenant,
        TimeRange::new(i64::MIN, i64::MAX)?,
    )
    .await
}

#[async_trait]
impl ObjectStore for RecordingObjectStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.put_paths
            .lock()
            .unwrap()
            .push((location.to_string(), payload.content_length()));
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.get_paths.lock().unwrap().push(location.to_string());
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn compactor_writes_block_then_tenant_index_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("observability/logs");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let mut block_index = BlockIndex::default();
    let key = BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap());

    let descriptor = compact_log_block_to_object_store(
        &store,
        &prefix,
        &key,
        &label_index,
        &mut block_index,
        vec![
            LogRow::new(api, 19, "api error", BTreeMap::new()),
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
        ],
    )
    .await
    .unwrap();

    assert2::assert!(&descriptor.key == &key);
    assert2::assert!(&descriptor.fingerprints == &BTreeSet::from([api]));
    check!(
        block_index.match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[api])
            == vec![descriptor.clone()]
    );

    let rows = read_log_block_from_object_store(&store, &prefix, &key)
        .await
        .unwrap();
    assert2::assert!(
        rows.iter().map(|row| row.line.as_str()).collect::<Vec<_>>() == vec!["api ok", "api error"]
    );

    let (loaded_labels, loaded_blocks) = read_all_tenant_shard_indexes(&store, &prefix, "tenant-a")
        .await
        .unwrap();

    assert2::assert!(
        loaded_labels.label_values("tenant-a", "app") == BTreeSet::from(["api".into()])
    );
    assert2::assert!(
        loaded_blocks.match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[api])
            == vec![descriptor]
    );
}

#[tokio::test]
async fn compactor_commits_partition_offset_after_writing_block_and_index() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("observability/logs");
    let mut label_index = LabelIndex::default();
    let mut block_index = BlockIndex::default();
    let mut committer = RecordingCommitter::default();

    let descriptor = compact_wal_records_to_object_store(
        &store,
        &prefix,
        &mut label_index,
        &mut block_index,
        &mut committer,
        vec![
            wal_record(10, 42, "api ok"),
            wal_record(19, 43, "api error"),
        ],
    )
    .await
    .unwrap();

    let key = BlockKey::new("tenant-a", 0, 42, 43, TimeRange::new(10, 19).unwrap());
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    check!(descriptor.key == key);
    check!(
        committer.committed
            == vec![WalPosition {
                partition: PartitionIndex(0),
                offset: Offset(43)
            }]
    );
    check!(
        block_index.match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[api])
            == vec![descriptor.clone()]
    );

    let rows = read_log_block_from_object_store(&store, &prefix, &key)
        .await
        .unwrap();
    assert2::assert!(
        rows.iter().map(|row| row.line.as_str()).collect::<Vec<_>>() == vec!["api ok", "api error"]
    );

    let (_, loaded_blocks) =
        read_tenant_log_index_manifest_from_object_store(&store, &prefix, "tenant-a")
            .await
            .unwrap();
    assert2::assert!(
        loaded_blocks.match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[api])
            == vec![descriptor]
    );
}

#[tokio::test]
async fn compactor_decodes_kafka_wal_records_before_writing_block() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("observability/logs");
    let mut label_index = LabelIndex::default();
    let mut block_index = BlockIndex::default();
    let mut committer = RecordingCommitter::default();

    let descriptor = compact_kafka_wal_records_to_object_store(
        &store,
        &prefix,
        &mut label_index,
        &mut block_index,
        &mut committer,
        vec![
            kafka_wal_record(&wal_record_without_position(10, "api ok"), 2, 42),
            kafka_wal_record(&wal_record_without_position(19, "api error"), 2, 43),
        ],
    )
    .await
    .unwrap();

    let key = BlockKey::new("tenant-a", 2, 42, 43, TimeRange::new(10, 19).unwrap());
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    check!(descriptor.key == key);
    check!(
        committer.committed
            == vec![WalPosition {
                partition: PartitionIndex(2),
                offset: Offset(43)
            }]
    );
    check!(
        block_index.match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[api])
            == vec![descriptor.clone()]
    );

    let rows = read_log_block_from_object_store(&store, &prefix, &key)
        .await
        .unwrap();
    assert2::assert!(
        rows.iter().map(|row| row.line.as_str()).collect::<Vec<_>>() == vec!["api ok", "api error"]
    );
}

#[tokio::test]
async fn compactor_decodes_native_kafka_log_records_from_headers() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("observability/logs");
    let mut label_index = LabelIndex::default();
    let mut block_index = BlockIndex::default();
    let mut committer = RecordingCommitter::default();

    let descriptor = compact_kafka_wal_records_to_object_store(
        &store,
        &prefix,
        &mut label_index,
        &mut block_index,
        &mut committer,
        vec![KafkaWalRecord {
            value: b"api error".to_vec(),
            partition: PartitionIndex(2),
            offset: Offset(44),
            timestamp_ms: Some(1),
            headers: vec![
                kafka_header("crabka-wal-record-type", "log-line"),
                kafka_header("crabka-tenant", "tenant-a"),
                kafka_header("crabka-log-timestamp-ns", "1900000"),
                kafka_header("crabka-log-label-app", "api"),
                kafka_header("crabka-log-label-env", "prod"),
                kafka_header("crabka-log-metadata-trace_id", "abc"),
            ],
        }],
    )
    .await
    .unwrap();

    let key = BlockKey::new(
        "tenant-a",
        2,
        44,
        44,
        TimeRange::new(1_900_000, 1_900_000).unwrap(),
    );

    assert2::assert!(descriptor.key == key);
    assert2::assert!(
        committer.committed
            == vec![WalPosition {
                partition: PartitionIndex(2),
                offset: Offset(44)
            }]
    );
    let rows = read_log_block_from_object_store(&store, &prefix, &key)
        .await
        .unwrap();
    let labels = labels([("app", "api"), ("env", "prod")]);
    let fingerprint = series_fingerprint(&labels);
    assert2::assert!(
        rows == vec![LogRow::new(
            fingerprint,
            1_900_000,
            "api error",
            BTreeMap::from([("trace_id".into(), "abc".into())]),
        )]
    );

    let (loaded_labels, loaded_blocks) = read_all_tenant_shard_indexes(&store, &prefix, "tenant-a")
        .await
        .unwrap();
    assert2::assert!(
        loaded_labels.label_values("tenant-a", "app") == BTreeSet::from(["api".into()])
    );
    assert2::assert!(
        loaded_blocks.match_blocks(
            "tenant-a",
            TimeRange::new(0, 2_000_000).unwrap(),
            &[fingerprint]
        ) == vec![descriptor]
    );
}

#[tokio::test]
async fn compactor_does_not_commit_offset_for_invalid_wal_batch() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("observability/logs");
    let mut label_index = LabelIndex::default();
    let mut block_index = BlockIndex::default();
    let mut committer = RecordingCommitter::default();
    let mut record = wal_record(10, 42, "api ok");
    record.position = None;

    let error = compact_wal_records_to_object_store(
        &store,
        &prefix,
        &mut label_index,
        &mut block_index,
        &mut committer,
        vec![record],
    )
    .await
    .unwrap_err();

    check!(error.to_string().contains("missing WAL position"));
    check!(committer.committed.is_empty());
    check!(label_index.label_names("tenant-a").is_empty());
    check!(
        block_index
            .match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[])
            .is_empty()
    );
}

#[tokio::test]
async fn compactor_does_not_commit_offset_for_invalid_kafka_wal_payload() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("observability/logs");
    let mut label_index = LabelIndex::default();
    let mut block_index = BlockIndex::default();
    let mut committer = RecordingCommitter::default();

    let error = compact_kafka_wal_records_to_object_store(
        &store,
        &prefix,
        &mut label_index,
        &mut block_index,
        &mut committer,
        vec![KafkaWalRecord {
            value: b"not json".to_vec(),
            partition: PartitionIndex(2),
            offset: Offset(42),
            timestamp_ms: None,
            headers: Vec::new(),
        }],
    )
    .await
    .unwrap_err();

    check!(
        error
            .to_string()
            .contains("wal record deserialization failed")
    );
    check!(committer.committed.is_empty());
    check!(label_index.label_names("tenant-a").is_empty());
    check!(
        block_index
            .match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[])
            .is_empty()
    );
}

#[tokio::test]
async fn compactor_polls_kafka_wal_batch_then_commits_after_object_store_write() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("observability/logs");
    let mut label_index = LabelIndex::default();
    let mut block_index = BlockIndex::default();
    let mut consumer = RecordingWalConsumer::new(vec![vec![
        kafka_wal_record(&wal_record_without_position(10, "api ok"), 3, 42),
        kafka_wal_record(&wal_record_without_position(19, "api error"), 3, 43),
    ]]);

    let descriptor = compact_next_kafka_wal_batch_to_object_store(
        &store,
        &prefix,
        &mut label_index,
        &mut block_index,
        &mut consumer,
        Duration::from_millis(1),
    )
    .await
    .unwrap()
    .expect("compacted descriptor");

    let key = BlockKey::new("tenant-a", 3, 42, 43, TimeRange::new(10, 19).unwrap());

    assert2::assert!(descriptor.key == key);
    assert2::assert!(
        consumer.committed
            == vec![WalPosition {
                partition: PartitionIndex(3),
                offset: Offset(43)
            }]
    );
    let rows = read_log_block_from_object_store(&store, &prefix, &key)
        .await
        .unwrap();
    assert2::assert!(
        rows.iter().map(|row| row.line.as_str()).collect::<Vec<_>>() == vec!["api ok", "api error"]
    );
}

#[tokio::test]
async fn compactor_does_not_commit_polled_batch_when_decode_fails() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("observability/logs");
    let mut label_index = LabelIndex::default();
    let mut block_index = BlockIndex::default();
    let mut consumer = RecordingWalConsumer::new(vec![vec![KafkaWalRecord {
        value: b"not json".to_vec(),
        partition: PartitionIndex(3),
        offset: Offset(42),
        timestamp_ms: None,
        headers: Vec::new(),
    }]]);

    let error = compact_next_kafka_wal_batch_to_object_store(
        &store,
        &prefix,
        &mut label_index,
        &mut block_index,
        &mut consumer,
        Duration::from_millis(1),
    )
    .await
    .unwrap_err();

    check!(
        error
            .to_string()
            .contains("wal record deserialization failed")
    );
    check!(consumer.committed.is_empty());
    check!(label_index.label_names("tenant-a").is_empty());
}

#[tokio::test]
async fn compactor_runtime_compacts_one_polled_batch_from_service_config() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let dependencies =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![vec![
            kafka_wal_record(&wal_record_without_position(10, "api ok"), 4, 42),
            kafka_wal_record(&wal_record_without_position(19, "api error"), 4, 43),
        ]]));

    let descriptor = run_compactor_once(&config, dependencies, Some(&store))
        .await
        .unwrap()
        .expect("compacted descriptor");

    let key = BlockKey::new("tenant-a", 4, 42, 43, TimeRange::new(10, 19).unwrap());

    assert2::assert!(descriptor.key == key);
    let rows =
        read_log_block_from_object_store(&store, &ObjectPath::from("observability/logs"), &key)
            .await
            .unwrap();
    assert2::assert!(
        rows.iter().map(|row| row.line.as_str()).collect::<Vec<_>>() == vec!["api ok", "api error"]
    );
}

#[tokio::test]
async fn compactor_runtime_materializes_active_delete_requests_in_written_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let mut config = compactor_config("observability/logs");
    config.data_root = dir.path().to_path_buf();
    let app = build_service_router(&config, ServiceDependencies::default(), Some(&store))
        .await
        .unwrap();
    let delete_response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22secret%22&start=14&end=16")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert2::assert!(delete_response.status() == StatusCode::NO_CONTENT);
    let dependencies =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![vec![
            kafka_wal_record(
                &wal_record_without_position(14_000_000_000, "api ok"),
                0,
                42,
            ),
            kafka_wal_record(
                &wal_record_without_position(15_000_000_000, "api secret"),
                0,
                43,
            ),
            kafka_wal_record(
                &wal_record_without_position(17_000_000_000, "api later secret"),
                0,
                44,
            ),
        ]]));

    let descriptor = run_compactor_once(&config, dependencies, Some(&store))
        .await
        .unwrap()
        .expect("compacted descriptor");

    let key = BlockKey::new(
        "tenant-a",
        0,
        42,
        44,
        TimeRange::new(14_000_000_000, 17_000_000_000).unwrap(),
    );
    assert2::assert!(descriptor.key == key);
    let rows =
        read_log_block_from_object_store(&store, &ObjectPath::from("observability/logs"), &key)
            .await
            .unwrap();
    assert2::assert!(
        rows.iter().map(|row| row.line.as_str()).collect::<Vec<_>>()
            == vec!["api ok", "api later secret"]
    );
}

#[tokio::test]
async fn compactor_runtime_materializes_active_delete_requests_in_existing_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let mut config = compactor_config("observability/logs");
    config.data_root = dir.path().to_path_buf();
    let prefix = ObjectPath::from("observability/logs");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let key = BlockKey::new(
        "tenant-a",
        0,
        42,
        44,
        TimeRange::new(14_000_000_000, 17_000_000_000).unwrap(),
    );
    let descriptor = write_log_block_to_object_store(
        &store,
        &prefix,
        &key,
        vec![
            LogRow::new(api, 14_000_000_000, "api ok", BTreeMap::new()),
            LogRow::new(api, 15_000_000_000, "api secret", BTreeMap::new()),
            LogRow::new(api, 17_000_000_000, "api later secret", BTreeMap::new()),
        ],
    )
    .await
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(descriptor);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();

    let app = build_service_router(&config, ServiceDependencies::default(), Some(&store))
        .await
        .unwrap();
    let delete_response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22secret%22&start=14&end=16")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert2::assert!(delete_response.status() == StatusCode::NO_CONTENT);

    let dependencies = ServiceDependencies::default()
        .with_wal_consumer(RecordingWalConsumer::new(vec![Vec::new()]));
    let descriptor = run_compactor_once(&config, dependencies, Some(&store))
        .await
        .unwrap();
    assert2::assert!(descriptor.is_none());

    let rows = read_log_block_from_object_store(&store, &prefix, &key)
        .await
        .unwrap();
    assert2::assert!(
        rows.iter().map(|row| row.line.as_str()).collect::<Vec<_>>()
            == vec!["api ok", "api later secret"]
    );
    let (_, loaded_blocks) =
        read_tenant_log_index_manifest_from_object_store(&store, &prefix, "tenant-a")
            .await
            .unwrap();
    assert2::assert!(
        loaded_blocks
            .match_blocks("tenant-a", key.time_range, &[api])
            .len()
            == 1
    );
}

#[tokio::test]
async fn compactor_runtime_materializes_active_delete_requests_in_existing_local_manifest_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let mut config = compactor_config("observability/logs");
    config.data_root = dir.path().to_path_buf();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let key = BlockKey::new(
        "tenant-a",
        0,
        42,
        44,
        TimeRange::new(14_000_000_000, 17_000_000_000).unwrap(),
    );
    let descriptor = write_log_block(
        dir.path(),
        &key,
        vec![
            LogRow::new(api, 14_000_000_000, "api ok", BTreeMap::new()),
            LogRow::new(api, 15_000_000_000, "api secret", BTreeMap::new()),
            LogRow::new(api, 17_000_000_000, "api later secret", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(descriptor);
    write_log_index_manifest(dir.path(), &label_index, &block_index).unwrap();

    let app = build_service_router(&config, ServiceDependencies::default(), Some(&store))
        .await
        .unwrap();
    let delete_response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22secret%22&start=14&end=16")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert2::assert!(delete_response.status() == StatusCode::NO_CONTENT);

    let dependencies = ServiceDependencies::default()
        .with_wal_consumer(RecordingWalConsumer::new(vec![Vec::new()]));
    let descriptor = run_compactor_once(&config, dependencies, Some(&store))
        .await
        .unwrap();
    assert2::assert!(descriptor.is_none());

    let rows = read_log_block(dir.path(), &key).unwrap();
    assert2::assert!(
        rows.iter().map(|row| row.line.as_str()).collect::<Vec<_>>()
            == vec!["api ok", "api later secret"]
    );
    let (_, loaded_blocks) = read_log_index_manifest(dir.path()).unwrap();
    assert2::assert!(
        loaded_blocks
            .match_blocks("tenant-a", key.time_range, &[api])
            .len()
            == 1
    );
}

#[tokio::test]
async fn compactor_runtime_materializes_active_delete_requests_in_existing_shard_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let mut config = compactor_config("observability/logs");
    config.data_root = dir.path().to_path_buf();
    let prefix = ObjectPath::from("observability/logs");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let key = BlockKey::new(
        "tenant-a",
        0,
        52,
        54,
        TimeRange::new(24_000_000_000, 27_000_000_000).unwrap(),
    );
    let descriptor = write_log_block_to_object_store(
        &store,
        &prefix,
        &key,
        vec![
            LogRow::new(api, 24_000_000_000, "api ok", BTreeMap::new()),
            LogRow::new(api, 25_000_000_000, "api secret", BTreeMap::new()),
            LogRow::new(api, 27_000_000_000, "api later secret", BTreeMap::new()),
        ],
    )
    .await
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(descriptor);
    let shard_range = TimeRange::new(24_000_000_000, 27_000_000_000).unwrap();
    write_tenant_log_index_shards_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &[shard_range],
        &label_index,
        &block_index,
    )
    .await
    .unwrap();

    let app = build_service_router(&config, ServiceDependencies::default(), Some(&store))
        .await
        .unwrap();
    let delete_response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22secret%22&start=24&end=26")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert2::assert!(delete_response.status() == StatusCode::NO_CONTENT);

    let dependencies = ServiceDependencies::default()
        .with_wal_consumer(RecordingWalConsumer::new(vec![Vec::new()]));
    let descriptor = run_compactor_once(&config, dependencies, Some(&store))
        .await
        .unwrap();
    assert2::assert!(descriptor.is_none());

    let rows = read_log_block_from_object_store(&store, &prefix, &key)
        .await
        .unwrap();
    assert2::assert!(
        rows.iter().map(|row| row.line.as_str()).collect::<Vec<_>>()
            == vec!["api ok", "api later secret"]
    );
    let (_, loaded_blocks) =
        read_tenant_log_index_shard_from_object_store(&store, &prefix, "tenant-a", shard_range)
            .await
            .unwrap();
    assert2::assert!(
        loaded_blocks
            .match_blocks("tenant-a", key.time_range, &[api])
            .len()
            == 1
    );
}

#[tokio::test]
async fn compactor_once_loads_existing_manifest_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let first_run =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![vec![
            kafka_wal_record(&wal_record_without_position(10, "api ok"), 4, 42),
        ]]));

    let first_descriptor = run_compactor_once(&config, first_run, Some(&store))
        .await
        .unwrap()
        .expect("first compacted descriptor");

    let second_run =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![vec![
            kafka_wal_record(&wal_record_without_position(19, "api error"), 4, 43),
        ]]));
    let second_descriptor = run_compactor_once(&config, second_run, Some(&store))
        .await
        .unwrap()
        .expect("second compacted descriptor");

    let prefix = ObjectPath::from("observability/logs");
    let (loaded_labels, loaded_blocks) = read_all_tenant_shard_indexes(&store, &prefix, "tenant-a")
        .await
        .unwrap();
    let api_labels = labels([("app", "api"), ("env", "prod")]);
    let api = series_fingerprint(&api_labels);

    assert2::assert!(
        loaded_labels.label_values("tenant-a", "app") == BTreeSet::from(["api".into()])
    );
    assert2::assert!(
        loaded_blocks.match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[api])
            == vec![first_descriptor, second_descriptor]
    );
}

#[tokio::test]
async fn compactor_runtime_updates_object_store_shard_catalog_incrementally() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let first_run =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![vec![
            kafka_wal_record(&wal_record_without_position(10, "api ok"), 4, 42),
        ]]));
    let first_descriptor = run_compactor_once(&config, first_run, Some(&store))
        .await
        .unwrap()
        .expect("first compacted descriptor");

    let second_run =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![vec![
            kafka_wal_record(&wal_record_without_position(19, "api error"), 4, 43),
        ]]));
    let second_descriptor = run_compactor_once(&config, second_run, Some(&store))
        .await
        .unwrap()
        .expect("second compacted descriptor");

    let prefix = ObjectPath::from("observability/logs");
    let shard_ranges =
        list_tenant_log_index_shard_ranges_from_object_store(&store, &prefix, "tenant-a")
            .await
            .unwrap();
    assert2::assert!(
        shard_ranges
            == vec![
                first_descriptor.key.time_range,
                second_descriptor.key.time_range
            ]
    );

    let api_labels = labels([("app", "api"), ("env", "prod")]);
    let api = series_fingerprint(&api_labels);
    let (loaded_labels, loaded_blocks) = read_tenant_log_index_shards_from_object_store(
        &store,
        &prefix,
        "tenant-a",
        TimeRange::new(0, 30).unwrap(),
    )
    .await
    .unwrap();

    assert2::assert!(
        loaded_labels.label_values("tenant-a", "app") == BTreeSet::from(["api".into()])
    );
    assert2::assert!(
        loaded_blocks.match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[api])
            == vec![first_descriptor, second_descriptor]
    );
}

#[tokio::test]
async fn compactor_runtime_rejects_missing_wal_consumer_dependency() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");

    let error = run_compactor_once(&config, ServiceDependencies::default(), Some(&store))
        .await
        .unwrap_err();

    assert2::assert!(error.to_string().contains("WAL consumer is required"));
}

#[tokio::test]
async fn compactor_runtime_rejects_missing_object_store() {
    let config = compactor_config("observability/logs");
    let dependencies =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::default());

    let error = run_compactor_once(&config, dependencies, None)
        .await
        .unwrap_err();

    assert2::assert!(error.to_string().contains("object store is required"));
}

#[tokio::test]
async fn compactor_runtime_preserves_indexes_across_polled_batches_until_idle() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let dependencies =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![
            vec![kafka_wal_record(
                &wal_record_without_position(10, "api ok"),
                5,
                42,
            )],
            vec![kafka_wal_record(
                &wal_record_without_position(19, "api error"),
                5,
                43,
            )],
            Vec::new(),
        ]));

    let descriptors = run_compactor_until_idle(&config, dependencies, Some(&store))
        .await
        .unwrap();

    assert2::assert!(descriptors.len() == 2);
    let prefix = ObjectPath::from("observability/logs");
    let (loaded_labels, loaded_blocks) = read_all_tenant_shard_indexes(&store, &prefix, "tenant-a")
        .await
        .unwrap();
    let api_labels = labels([("app", "api"), ("env", "prod")]);
    let api = series_fingerprint(&api_labels);

    assert2::assert!(
        loaded_labels.label_values("tenant-a", "app") == BTreeSet::from(["api".into()])
    );
    assert2::assert!(
        loaded_blocks.match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[api])
            == descriptors
    );
}

#[tokio::test]
async fn compactor_runtime_writes_shard_indexes_without_index_metadata_rewrites() {
    let store = RecordingObjectStore::new();
    let config = compactor_config("observability/logs");
    let dependencies =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![
            vec![kafka_wal_record(
                &wal_record_without_position(10, "api ok"),
                5,
                42,
            )],
            vec![kafka_wal_record(
                &wal_record_without_position(19, "api error"),
                5,
                43,
            )],
            Vec::new(),
        ]));

    let descriptors = run_compactor_until_idle(&config, dependencies, Some(&store))
        .await
        .unwrap();

    let prefix = ObjectPath::from("observability/logs");
    let manifest_path =
        crabka_blockstore::log_tenant_index_manifest_object_path(&prefix, "tenant-a").to_string();
    let shard_catalog_path =
        crabka_blockstore::log_tenant_index_shard_catalog_object_path(&prefix, "tenant-a")
            .to_string();
    let manifest_puts = store
        .put_paths()
        .into_iter()
        .filter(|(path, _)| path == &manifest_path)
        .count();
    let shard_catalog_puts = store
        .put_paths()
        .into_iter()
        .filter(|(path, _)| path == &shard_catalog_path)
        .count();
    let (_, loaded_blocks) = read_tenant_log_index_shards_from_object_store(
        &store,
        &prefix,
        "tenant-a",
        TimeRange::new(i64::MIN, i64::MAX).unwrap(),
    )
    .await
    .unwrap();
    let api = series_fingerprint(&labels([("app", "api"), ("env", "prod")]));

    check!(
        manifest_puts == 0,
        "service compactor should not rewrite the full tenant manifest"
    );
    check!(
        shard_catalog_puts == 0,
        "service compactor should not rewrite the shard catalog"
    );
    check!(
        loaded_blocks.match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[api])
            == descriptors
    );
}

#[tokio::test]
async fn compactor_runtime_writes_shards_with_only_the_new_block() {
    let store = RecordingObjectStore::new();
    let config = compactor_config("observability/logs");
    let dependencies =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![
            vec![
                kafka_wal_record(&wal_record_without_position(10, "api first"), 5, 42),
                kafka_wal_record(&wal_record_without_position(30, "api first later"), 5, 43),
            ],
            vec![
                kafka_wal_record(&wal_record_without_position(20, "api second"), 5, 44),
                kafka_wal_record(&wal_record_without_position(40, "api second later"), 5, 45),
            ],
            Vec::new(),
        ]));

    let descriptors = run_compactor_until_idle(&config, dependencies, Some(&store))
        .await
        .unwrap();

    let prefix = ObjectPath::from("observability/logs");
    let api = series_fingerprint(&labels([("app", "api"), ("env", "prod")]));
    let (_, first_blocks) = read_tenant_log_index_shard_from_object_store(
        &store,
        &prefix,
        "tenant-a",
        descriptors[0].key.time_range,
    )
    .await
    .unwrap();
    let (_, second_blocks) = read_tenant_log_index_shard_from_object_store(
        &store,
        &prefix,
        "tenant-a",
        descriptors[1].key.time_range,
    )
    .await
    .unwrap();

    assert2::assert!(
        first_blocks.match_blocks("tenant-a", TimeRange::new(0, 50).unwrap(), &[api])
            == vec![descriptors[0].clone()]
    );
    assert2::assert!(
        second_blocks.match_blocks("tenant-a", TimeRange::new(0, 50).unwrap(), &[api])
            == vec![descriptors[1].clone()]
    );
}

#[tokio::test]
async fn compactor_runtime_appends_batches_without_loading_tenant_manifest() {
    let store = RecordingObjectStore::new();
    let config = compactor_config("observability/logs");
    let dependencies =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![
            vec![kafka_wal_record(
                &wal_record_without_position(10, "api ok"),
                5,
                42,
            )],
            vec![kafka_wal_record(
                &wal_record_without_position(19, "api error"),
                5,
                43,
            )],
            Vec::new(),
        ]));

    let descriptors = run_compactor_until_idle(&config, dependencies, Some(&store))
        .await
        .unwrap();

    let manifest_path = crabka_blockstore::log_tenant_index_manifest_object_path(
        &ObjectPath::from("observability/logs"),
        "tenant-a",
    )
    .to_string();
    let manifest_gets = store
        .get_paths()
        .into_iter()
        .filter(|path| path == &manifest_path)
        .count();

    assert2::assert!(descriptors.len() == 2);
    assert2::assert!(manifest_gets == 0);
}

#[tokio::test]
async fn compactor_runtime_appends_shard_without_loading_historical_shards() {
    let store = RecordingObjectStore::new();
    let prefix = ObjectPath::from("observability/logs");
    let tenant = "tenant-a";
    let old_range = TimeRange::new(1, 1).unwrap();
    let old_key = BlockKey::new(tenant, 5, 40, 40, old_range);
    let mut old_labels = LabelIndex::default();
    let old_fingerprint = old_labels.insert_series(tenant, labels([("app", "api")]));
    let mut old_blocks = BlockIndex::default();
    old_blocks.insert(crabka_blockstore::BlockDescriptor::new_with_size(
        old_key,
        BTreeSet::from([old_fingerprint]),
        1,
    ));
    write_tenant_log_index_shards_to_object_store(
        &store,
        &prefix,
        tenant,
        &[old_range],
        &old_labels,
        &old_blocks,
    )
    .await
    .unwrap();
    store.get_paths.lock().unwrap().clear();

    let config = compactor_config("observability/logs");
    let dependencies =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![
            vec![kafka_wal_record(
                &wal_record_without_position(10, "api ok"),
                5,
                42,
            )],
            Vec::new(),
        ]));

    let descriptors = run_compactor_until_idle(&config, dependencies, Some(&store))
        .await
        .unwrap();

    let old_shard_manifest =
        crabka_blockstore::log_tenant_index_shard_manifest_object_path(&prefix, tenant, old_range)
            .to_string();
    assert2::assert!(descriptors.len() == 1);
    assert2::assert!(
        !store
            .get_paths()
            .into_iter()
            .any(|path| path == old_shard_manifest)
    );
}

#[tokio::test]
#[allow(clippy::similar_names)]
async fn compactor_runtime_splits_mixed_tenant_wal_batch_into_tenant_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let dependencies =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![
            vec![
                kafka_wal_record(
                    &wal_record_for_tenant("tenant-a", 10, "tenant a error"),
                    5,
                    42,
                ),
                kafka_wal_record(
                    &wal_record_for_tenant("tenant-b", 11, "tenant b error"),
                    5,
                    43,
                ),
            ],
            Vec::new(),
        ]));

    let descriptors = run_compactor_until_idle(&config, dependencies, Some(&store))
        .await
        .unwrap();

    assert2::assert!(
        descriptors
            .iter()
            .map(|descriptor| descriptor.key.tenant.as_str())
            .collect::<Vec<_>>()
            == vec!["tenant-a", "tenant-b"]
    );

    let prefix = ObjectPath::from("observability/logs");
    let (tenant_a_labels, tenant_a_blocks) =
        read_all_tenant_shard_indexes(&store, &prefix, "tenant-a")
            .await
            .unwrap();
    let (tenant_b_labels, tenant_b_blocks) =
        read_all_tenant_shard_indexes(&store, &prefix, "tenant-b")
            .await
            .unwrap();
    let api_labels = labels([("app", "api"), ("env", "prod")]);
    let api = series_fingerprint(&api_labels);

    for (tenant_labels, tenant_blocks, tenant, descriptor) in [
        (
            &tenant_a_labels,
            &tenant_a_blocks,
            "tenant-a",
            &descriptors[0],
        ),
        (
            &tenant_b_labels,
            &tenant_b_blocks,
            "tenant-b",
            &descriptors[1],
        ),
    ] {
        assert2::assert!(
            tenant_labels.label_values(tenant, "app") == BTreeSet::from(["api".into()])
        );
        assert2::assert!(
            tenant_blocks.match_blocks(tenant, TimeRange::new(0, 30).unwrap(), &[api])
                == vec![descriptor.clone()]
        );
    }
}

#[tokio::test]
async fn compactor_runtime_keeps_polling_after_idle_until_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let prefix = ObjectPath::from("observability/logs");
    let key = BlockKey::new("tenant-a", 7, 43, 43, TimeRange::new(19, 19).unwrap());
    let dependencies =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![
            Vec::new(),
            vec![kafka_wal_record(
                &wal_record_without_position(19, "api error"),
                7,
                43,
            )],
            Vec::new(),
        ]));

    let descriptors = tokio::time::timeout(
        Duration::from_secs(1),
        run_compactor_until_shutdown(&config, dependencies, Some(&store), async {
            let _ = wait_for_log_block(&store, &prefix, &key).await;
        }),
    )
    .await
    .unwrap()
    .unwrap();

    assert2::assert!(descriptors.len() == 1);
    let rows = read_log_block_from_object_store(&store, &prefix, &key)
        .await
        .unwrap();
    assert2::assert!(
        rows.iter().map(|row| row.line.as_str()).collect::<Vec<_>>() == vec!["api error"]
    );
}

#[tokio::test]
async fn compactor_runtime_retries_object_store_errors_before_committing_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let store = FailingPutObjectStore::fail_first_put(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    );
    let config = compactor_config("observability/logs");
    let dependencies =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![
            vec![kafka_wal_record(
                &wal_record_without_position(10, "api ok"),
                6,
                42,
            )],
            Vec::new(),
        ]));

    let descriptors = tokio::time::timeout(
        Duration::from_secs(1),
        run_compactor_until_shutdown(&config, dependencies, Some(&store), async {
            // real-time wait (not a progress poll): shutdown future — this sleep is the
            // compactor's run-duration/retry budget, not a poll cadence for a condition.
            tokio::time::sleep(Duration::from_millis(250)).await;
        }),
    )
    .await
    .unwrap()
    .unwrap();

    assert2::assert!(descriptors.len() == 1);
    assert2::assert!(store.failed_put_count() == 1);
    let key = BlockKey::new("tenant-a", 6, 42, 42, TimeRange::new(10, 10).unwrap());
    let rows =
        read_log_block_from_object_store(&store, &ObjectPath::from("observability/logs"), &key)
            .await
            .unwrap();
    assert2::assert!(
        rows.iter().map(|row| row.line.as_str()).collect::<Vec<_>>() == vec!["api ok"]
    );
}

#[tokio::test]
async fn compactor_runtime_retries_shard_manifest_write_errors_before_committing_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let store = FailingPutObjectStore::fail_first_matching_put(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
        "shards/time=10-10/manifest.json",
    );
    let config = compactor_config("observability/logs");
    let dependencies =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![
            vec![kafka_wal_record(
                &wal_record_without_position(10, "api ok"),
                6,
                42,
            )],
            Vec::new(),
        ]));

    let descriptors = tokio::time::timeout(
        Duration::from_secs(1),
        run_compactor_until_shutdown(&config, dependencies, Some(&store), async {
            // real-time wait (not a progress poll): shutdown future — this sleep is the
            // compactor's run-duration/retry budget, not a poll cadence for a condition.
            tokio::time::sleep(Duration::from_millis(250)).await;
        }),
    )
    .await
    .unwrap()
    .unwrap();

    assert2::assert!(descriptors.len() == 1);
    assert2::assert!(store.failed_put_count() == 1);

    let prefix = ObjectPath::from("observability/logs");
    let key = BlockKey::new("tenant-a", 6, 42, 42, TimeRange::new(10, 10).unwrap());
    let rows = read_log_block_from_object_store(&store, &prefix, &key)
        .await
        .unwrap();
    assert2::assert!(
        rows.iter().map(|row| row.line.as_str()).collect::<Vec<_>>() == vec!["api ok"]
    );

    let shard_ranges =
        list_tenant_log_index_shard_ranges_from_object_store(&store, &prefix, "tenant-a")
            .await
            .unwrap();
    assert2::assert!(shard_ranges == vec![key.time_range]);
}

#[tokio::test]
async fn compactor_runtime_retries_compaction_frontier_write_errors_after_committing_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let store = FailingPutObjectStore::fail_first_matching_put(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
        "compaction-frontier.json",
    );
    let config = compactor_config("observability/logs");
    let dependencies =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![
            vec![kafka_wal_record(
                &wal_record_without_position(10, "api ok"),
                6,
                42,
            )],
            Vec::new(),
        ]));

    let descriptors = tokio::time::timeout(
        Duration::from_secs(1),
        run_compactor_until_shutdown(&config, dependencies, Some(&store), async {
            // real-time wait (not a progress poll): shutdown future — this sleep is the
            // compactor's run-duration/retry budget, not a poll cadence for a condition.
            tokio::time::sleep(Duration::from_millis(250)).await;
        }),
    )
    .await
    .unwrap()
    .unwrap();

    assert2::assert!(descriptors.len() == 1);
    assert2::assert!(store.failed_put_count() == 1);

    let persisted_frontier =
        read_compaction_frontier_from_object_store(&store, &ObjectPath::from("observability/logs"))
            .await
            .unwrap();
    assert2::assert!(
        persisted_frontier
            == CompactionFrontier::new(i64::MIN)
                .with_partition_offset(PartitionIndex(6), Offset(42))
    );
}

#[tokio::test]
async fn compactor_runtime_advances_shared_compaction_frontier_after_commit() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let frontier = SharedCompactionFrontier::default();
    let dependencies = ServiceDependencies::default()
        .with_wal_consumer(RecordingWalConsumer::new(vec![
            vec![kafka_wal_record(
                &wal_record_without_position(19, "api error"),
                8,
                43,
            )],
            Vec::new(),
        ]))
        .with_compaction_frontier(frontier.clone());

    let descriptors = run_compactor_until_idle(&config, dependencies, Some(&store))
        .await
        .unwrap();

    assert2::assert!(descriptors.len() == 1);
    assert2::assert!(
        frontier.snapshot()
            == crabka_observability::CompactionFrontier::new(i64::MIN)
                .with_partition_offset(PartitionIndex(8), Offset(43))
    );
}

#[tokio::test]
async fn compaction_frontier_round_trips_through_object_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("observability/logs");
    let frontier = CompactionFrontier::new(0)
        .with_partition_offset(PartitionIndex(8), Offset(43))
        .with_partition_offset(PartitionIndex(9), Offset(55));

    write_compaction_frontier_to_object_store(&store, &prefix, &frontier)
        .await
        .unwrap();
    let loaded = read_compaction_frontier_from_object_store(&store, &prefix)
        .await
        .unwrap();

    assert2::assert!(loaded == frontier);
}

#[tokio::test]
async fn compactor_runtime_reloads_shared_frontier_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let first_frontier = SharedCompactionFrontier::default();
    let first_run = ServiceDependencies::default()
        .with_wal_consumer(RecordingWalConsumer::new(vec![
            vec![kafka_wal_record(
                &wal_record_without_position(19, "api error"),
                8,
                43,
            )],
            Vec::new(),
        ]))
        .with_compaction_frontier(first_frontier);
    run_compactor_until_idle(&config, first_run, Some(&store))
        .await
        .unwrap();

    let restarted_frontier = SharedCompactionFrontier::default();
    let second_run = ServiceDependencies::default()
        .with_wal_consumer(RecordingWalConsumer::new(vec![Vec::new()]))
        .with_compaction_frontier(restarted_frontier.clone());
    run_compactor_until_idle(&config, second_run, Some(&store))
        .await
        .unwrap();

    assert2::assert!(
        restarted_frontier.snapshot()
            == CompactionFrontier::new(i64::MIN)
                .with_partition_offset(PartitionIndex(8), Offset(43))
    );
}

#[tokio::test]
async fn compactor_runtime_loads_existing_manifest_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let first_run =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![
            vec![kafka_wal_record(
                &wal_record_without_position(10, "api ok"),
                6,
                42,
            )],
            Vec::new(),
        ]));

    let mut descriptors = run_compactor_until_idle(&config, first_run, Some(&store))
        .await
        .unwrap();

    let second_run =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![
            vec![kafka_wal_record(
                &wal_record_without_position(19, "api error"),
                6,
                43,
            )],
            Vec::new(),
        ]));
    descriptors.extend(
        run_compactor_until_idle(&config, second_run, Some(&store))
            .await
            .unwrap(),
    );

    let prefix = ObjectPath::from("observability/logs");
    let (loaded_labels, loaded_blocks) = read_all_tenant_shard_indexes(&store, &prefix, "tenant-a")
        .await
        .unwrap();
    let api_labels = labels([("app", "api"), ("env", "prod")]);
    let api = series_fingerprint(&api_labels);

    assert2::assert!(
        loaded_labels.label_values("tenant-a", "app") == BTreeSet::from(["api".into()])
    );
    assert2::assert!(
        loaded_blocks.match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[api])
            == descriptors
    );
}

#[tokio::test]
async fn compactor_runtime_reprocesses_uncommitted_wal_without_duplicate_manifest_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let first_run = ServiceDependencies::default().with_wal_consumer(
        RecordingWalConsumer::failing_first_commit(vec![vec![kafka_wal_record(
            &wal_record_without_position(10, "api ok"),
            6,
            42,
        )]]),
    );

    let err = run_compactor_until_idle(&config, first_run, Some(&store))
        .await
        .unwrap_err();
    assert2::assert!(err.to_string().contains("coordinator unavailable"));

    let prefix = ObjectPath::from("observability/logs");
    let key = BlockKey::new("tenant-a", 6, 42, 42, TimeRange::new(10, 10).unwrap());
    let first_bytes = store
        .get(&log_block_object_path(&prefix, &key))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();

    let second_run =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![
            vec![kafka_wal_record(
                &wal_record_without_position(10, "api ok"),
                6,
                42,
            )],
            Vec::new(),
        ]));
    let descriptors = run_compactor_until_idle(&config, second_run, Some(&store))
        .await
        .unwrap();

    let rewritten_bytes = store
        .get(&log_block_object_path(&prefix, &key))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let rows = read_log_block_from_object_store(&store, &prefix, &key)
        .await
        .unwrap();
    assert2::assert!(
        rows.iter().map(|row| row.line.as_str()).collect::<Vec<_>>() == vec!["api ok"]
    );
    assert2::assert!(rewritten_bytes == first_bytes);

    let (_, loaded_blocks) = read_all_tenant_shard_indexes(&store, &prefix, "tenant-a")
        .await
        .unwrap();
    let api_labels = labels([("app", "api"), ("env", "prod")]);
    let api = series_fingerprint(&api_labels);

    assert2::assert!(descriptors.len() == 1);
    assert2::assert!(
        loaded_blocks.match_blocks("tenant-a", TimeRange::new(0, 30).unwrap(), &[api])
            == descriptors
    );
}

#[tokio::test]
async fn compactor_service_target_keeps_running_after_idle() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let config = compactor_config("observability/logs");
    let dependencies =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![
            vec![kafka_wal_record(
                &wal_record_without_position(10, "api ok"),
                6,
                42,
            )],
            Vec::new(),
        ]));
    let server_store = Arc::clone(&store);
    let server = tokio::spawn(async move {
        serve_service(config, dependencies, Some(server_store.as_ref())).await
    });

    let key = BlockKey::new("tenant-a", 6, 42, 42, TimeRange::new(10, 10).unwrap());
    let rows = wait_for_log_block(
        store.as_ref(),
        &ObjectPath::from("observability/logs"),
        &key,
    )
    .await;

    assert2::assert!(!server.is_finished());
    server.abort();

    assert2::assert!(
        rows.iter().map(|row| row.line.as_str()).collect::<Vec<_>>() == vec!["api ok"]
    );
}

#[tokio::test]
async fn compactor_service_accumulates_adjacent_small_wal_polls_into_one_block() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let config = compactor_config("observability/logs");
    let dependencies =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![
            vec![kafka_wal_record(
                &wal_record_without_position(10, "first"),
                6,
                42,
            )],
            vec![kafka_wal_record(
                &wal_record_without_position(20, "second"),
                6,
                43,
            )],
            Vec::new(),
        ]));

    let descriptors = run_compactor_until_shutdown(
        &config,
        dependencies,
        Some(&store),
        // real-time wait (not a progress poll): shutdown future — this sleep is the
        // compactor's run-duration budget, not a poll cadence for a condition.
        tokio::time::sleep(Duration::from_millis(50)),
    )
    .await
    .unwrap();

    assert2::assert!(descriptors.len() == 1);
    assert2::assert!(
        descriptors[0].key.clone()
            == BlockKey::new("tenant-a", 6, 42, 43, TimeRange::new(10, 20).unwrap())
    );

    let prefix = ObjectPath::from("observability/logs");
    let rows = read_log_block_from_object_store(&store, &prefix, &descriptors[0].key)
        .await
        .unwrap();
    assert2::assert!(
        rows.iter().map(|row| row.line.as_str()).collect::<Vec<_>>() == vec!["first", "second"]
    );
}

#[tokio::test]
async fn compactor_service_listener_serves_http_while_polling_wal() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let config = compactor_config("observability/logs");
    let dependencies =
        ServiceDependencies::default().with_wal_consumer(RecordingWalConsumer::new(vec![
            vec![kafka_wal_record(
                &wal_record_without_position(10, "api ok"),
                6,
                42,
            )],
            Vec::new(),
        ]));
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_store = Arc::clone(&store);
    let server = tokio::spawn(async move {
        serve_service_listener(listener, config, dependencies, Some(server_store.as_ref()))
            .await
            .unwrap();
    });

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            format!("GET /ready HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();

    assert2::assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert2::assert!(response.ends_with("ready\n"));

    let key = BlockKey::new("tenant-a", 6, 42, 42, TimeRange::new(10, 10).unwrap());
    let mut rows = None;
    for _ in 0..20 {
        match read_log_block_from_object_store(
            store.as_ref(),
            &ObjectPath::from("observability/logs"),
            &key,
        )
        .await
        {
            Ok(block_rows) => {
                rows = Some(block_rows);
                break;
            }
            // real-time wait (not a progress poll): iteration-count-bounded retry
            // (`for _ in 0..20`); the sleep is the fixed time budget between reads.
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
    }
    server.abort();
    let rows = rows.expect("compactor writes block while HTTP server is running");
    assert2::assert!(
        rows.iter().map(|row| row.line.as_str()).collect::<Vec<_>>() == vec!["api ok"]
    );
}

#[derive(Debug)]
struct FailingPutObjectStore<S> {
    inner: Arc<S>,
    failed_puts_remaining: std::sync::Mutex<usize>,
    failed_puts: std::sync::atomic::AtomicUsize,
    matching_path: Option<String>,
}

impl<S> FailingPutObjectStore<S> {
    fn fail_first_put(inner: S) -> Self {
        Self {
            inner: Arc::new(inner),
            failed_puts_remaining: std::sync::Mutex::new(1),
            failed_puts: std::sync::atomic::AtomicUsize::new(0),
            matching_path: None,
        }
    }

    fn fail_first_matching_put(inner: S, matching_path: &str) -> Self {
        Self {
            inner: Arc::new(inner),
            failed_puts_remaining: std::sync::Mutex::new(1),
            failed_puts: std::sync::atomic::AtomicUsize::new(0),
            matching_path: Some(matching_path.to_string()),
        }
    }

    fn failed_put_count(&self) -> usize {
        self.failed_puts.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl<S> fmt::Display for FailingPutObjectStore<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailingPutObjectStore")
    }
}

#[async_trait]
impl<S> ObjectStore for FailingPutObjectStore<S>
where
    S: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let should_fail = {
            let mut remaining = self.failed_puts_remaining.lock().unwrap();
            let matches_path = self
                .matching_path
                .as_ref()
                .is_none_or(|matching_path| location.as_ref().contains(matching_path));
            if *remaining > 0 && matches_path {
                *remaining -= 1;
                true
            } else {
                false
            }
        };
        if should_fail {
            self.failed_puts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return Err(object_store::Error::Generic {
                store: "failing-put",
                source: "transient put failure".into(),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &ObjectPath,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectPath>,
        offset: &ObjectPath,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[derive(Default)]
struct RecordingCommitter {
    committed: Vec<WalPosition>,
}

impl CompactionOffsetCommitter for RecordingCommitter {
    fn commit_compacted(
        &mut self,
        position: WalPosition,
    ) -> Result<(), crabka_observability::CompactionCommitError> {
        self.committed.push(position);
        Ok(())
    }
}

#[derive(Default)]
struct RecordingWalConsumer {
    batches: Vec<Vec<KafkaWalRecord>>,
    committed: Vec<WalPosition>,
    failed_commits_remaining: usize,
}

impl RecordingWalConsumer {
    fn new(batches: Vec<Vec<KafkaWalRecord>>) -> Self {
        Self {
            batches,
            committed: Vec::new(),
            failed_commits_remaining: 0,
        }
    }

    fn failing_first_commit(batches: Vec<Vec<KafkaWalRecord>>) -> Self {
        Self {
            failed_commits_remaining: 1,
            ..Self::new(batches)
        }
    }
}

#[async_trait]
impl LogWalConsumer for RecordingWalConsumer {
    async fn poll(&mut self, _timeout: Duration) -> Result<Vec<KafkaWalRecord>, WalConsumerError> {
        if self.batches.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(self.batches.remove(0))
        }
    }

    async fn commit_compacted(&mut self, position: WalPosition) -> Result<(), WalConsumerError> {
        if self.failed_commits_remaining > 0 {
            self.failed_commits_remaining -= 1;
            return Err(WalConsumerError::Consumer(
                ConsumerError::CoordinatorUnavailable,
            ));
        }
        self.committed.push(position);
        Ok(())
    }
}

fn wal_record(timestamp_ns: i64, offset: i64, line: &str) -> WalLogRecord {
    WalLogRecord {
        tenant: "tenant-a".to_string(),
        labels: labels([("app", "api"), ("env", "prod")]),
        timestamp_ns,
        line: line.to_string(),
        structured_metadata: BTreeMap::new(),
        position: Some(WalPosition {
            partition: PartitionIndex(0),
            offset: Offset(offset),
        }),
    }
}

async fn wait_for_log_block(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    key: &BlockKey,
) -> Vec<LogRow> {
    for _ in 0..50 {
        if let Ok(rows) = read_log_block_from_object_store(store, prefix, key).await {
            return rows;
        }
        // real-time wait (not a progress poll): iteration-count-bounded retry
        // (`for _ in 0..50`); the sleep is the fixed time budget between reads.
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    read_log_block_from_object_store(store, prefix, key)
        .await
        .expect("compactor writes expected block")
}

fn wal_record_without_position(timestamp_ns: i64, line: &str) -> WalLogRecord {
    wal_record_for_tenant("tenant-a", timestamp_ns, line)
}

fn wal_record_for_tenant(tenant: &str, timestamp_ns: i64, line: &str) -> WalLogRecord {
    WalLogRecord {
        tenant: tenant.to_string(),
        labels: labels([("app", "api"), ("env", "prod")]),
        timestamp_ns,
        line: line.to_string(),
        structured_metadata: BTreeMap::new(),
        position: None,
    }
}

fn kafka_wal_record(record: &WalLogRecord, partition: i32, offset: i64) -> KafkaWalRecord {
    let producer_record =
        build_kafka_wal_record("__crabka_observability_logs_wal", record).expect("producer record");
    KafkaWalRecord {
        value: producer_record.value.expect("producer value").to_vec(),
        partition: PartitionIndex(partition),
        offset: Offset(offset),
        timestamp_ms: producer_record.timestamp_ms,
        headers: producer_record
            .headers
            .into_iter()
            .map(|header| KafkaWalHeader {
                key: header.key,
                value: header.value.map(|value| value.to_vec()),
            })
            .collect(),
    }
}

fn kafka_header(key: &str, value: &str) -> KafkaWalHeader {
    KafkaWalHeader {
        key: key.to_string(),
        value: Some(value.as_bytes().to_vec()),
    }
}

fn compactor_config(index_prefix: &str) -> ServiceConfig {
    ServiceConfig {
        target: Role::Compactor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: Some(index_prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    }
}
