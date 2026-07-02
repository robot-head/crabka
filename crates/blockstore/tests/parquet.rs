use std::collections::{BTreeMap, BTreeSet};

use assert2::{assert, check};
use crabka_blockstore::{
    BlockKey, LogRow, TimeRange, labels, log_block_object_path, read_log_block,
    read_log_block_from_object_store, series_fingerprint, write_log_block,
    write_log_block_to_object_store,
};
use datafusion::arrow::datatypes::{DataType, Fields};
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;

#[test]
fn parquet_log_block_round_trips_rows_sorted_by_series_and_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let api_labels = labels([("app", "api"), ("env", "prod")]);
    let worker_labels = labels([("app", "worker"), ("env", "prod")]);
    let api = series_fingerprint(&api_labels);
    let worker = series_fingerprint(&worker_labels);
    let key = BlockKey::new("tenant-a", 3, 50, 55, TimeRange::new(10, 30).unwrap());

    let descriptor = write_log_block(
        dir.path(),
        &key,
        vec![
            LogRow::new(worker, 20, "worker started", BTreeMap::new()),
            LogRow::new(
                api,
                30,
                "api error",
                BTreeMap::from([("level".into(), "error".into())]),
            ),
            LogRow::new(
                api,
                10,
                "api ok",
                BTreeMap::from([("level".into(), "info".into())]),
            ),
        ],
    )
    .unwrap();

    check!(descriptor.key == key);
    check!(descriptor.fingerprints == BTreeSet::from([api, worker]));
    check!(descriptor.size_bytes > 0);

    let rows = read_log_block(dir.path(), &key).unwrap();
    let mut expected = vec![
        LogRow::new(
            api,
            10,
            "api ok",
            BTreeMap::from([("level".into(), "info".into())]),
        ),
        LogRow::new(
            api,
            30,
            "api error",
            BTreeMap::from([("level".into(), "error".into())]),
        ),
        LogRow::new(worker, 20, "worker started", BTreeMap::new()),
    ];
    expected.sort_by_key(|row| (row.series_fingerprint, row.timestamp_ns));

    assert!(rows == expected);
}

#[test]
fn parquet_log_block_rejects_rows_outside_key_time_range() {
    let dir = tempfile::tempdir().unwrap();
    let key = BlockKey::new("tenant-a", 0, 1, 2, TimeRange::new(10, 20).unwrap());

    let error = write_log_block(
        dir.path(),
        &key,
        vec![LogRow::new(7, 21, "late", BTreeMap::new())],
    )
    .unwrap_err();

    assert!(error.to_string().contains("outside block time range"));
}

#[test]
fn parquet_log_block_writes_structured_metadata_as_arrow_map() {
    let dir = tempfile::tempdir().unwrap();
    let api = series_fingerprint(&labels([("app", "api"), ("env", "prod")]));
    let key = BlockKey::new("tenant-a", 0, 1, 1, TimeRange::new(10, 10).unwrap());

    write_log_block(
        dir.path(),
        &key,
        vec![LogRow::new(
            api,
            10,
            "api error",
            BTreeMap::from([("level".into(), "error".into())]),
        )],
    )
    .unwrap();

    let file = std::fs::File::open(
        dir.path()
            .join(log_block_object_path(&ObjectPath::from(""), &key).as_ref()),
    )
    .unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let field = builder
        .schema()
        .field_with_name("structured_metadata")
        .unwrap();

    let DataType::Map(entries, false) = field.data_type() else {
        panic!(
            "structured_metadata must be an Arrow map, got {:?}",
            field.data_type()
        );
    };
    let DataType::Struct(fields) = entries.data_type() else {
        panic!(
            "map entries must be a struct, got {:?}",
            entries.data_type()
        );
    };
    assert!(
        fields
            == &Fields::from(vec![
                datafusion::arrow::datatypes::Field::new("key", DataType::Utf8, false),
                datafusion::arrow::datatypes::Field::new("value", DataType::Utf8, false),
            ])
    );
}

#[test]
fn parquet_log_block_object_path_is_prefix_and_block_key() {
    let prefix = ObjectPath::from("observability/logs");
    let key = BlockKey::new("tenant-a", 2, 42, 99, TimeRange::new(1_000, 2_000).unwrap());

    assert!(
        log_block_object_path(&prefix, &key).to_string()
            == "observability/logs/tenant=tenant-a/partition=2/offsets=42-99/time=1000-2000.parquet"
    );
}

#[tokio::test]
async fn parquet_log_block_round_trips_through_object_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let prefix = ObjectPath::from("blocks");
    let api = series_fingerprint(&labels([("app", "api"), ("env", "prod")]));
    let worker = series_fingerprint(&labels([("app", "worker"), ("env", "prod")]));
    let key = BlockKey::new("tenant-a", 3, 50, 55, TimeRange::new(10, 30).unwrap());

    let descriptor = write_log_block_to_object_store(
        &store,
        &prefix,
        &key,
        vec![
            LogRow::new(worker, 20, "worker started", BTreeMap::new()),
            LogRow::new(
                api,
                30,
                "api error",
                BTreeMap::from([("level".into(), "error".into())]),
            ),
            LogRow::new(
                api,
                10,
                "api ok",
                BTreeMap::from([("level".into(), "info".into())]),
            ),
        ],
    )
    .await
    .unwrap();

    check!(descriptor.key == key);
    check!(descriptor.fingerprints == BTreeSet::from([api, worker]));
    check!(descriptor.size_bytes > 0);

    let rows = read_log_block_from_object_store(&store, &prefix, &key)
        .await
        .unwrap();
    let mut expected = vec![
        LogRow::new(
            api,
            10,
            "api ok",
            BTreeMap::from([("level".into(), "info".into())]),
        ),
        LogRow::new(
            api,
            30,
            "api error",
            BTreeMap::from([("level".into(), "error".into())]),
        ),
        LogRow::new(worker, 20, "worker started", BTreeMap::new()),
    ];
    expected.sort_by_key(|row| (row.series_fingerprint, row.timestamp_ns));

    assert!(rows == expected);
}
