use std::collections::BTreeMap;

use assert2::assert;
use crabka_blockstore::{
    BlockKey, LogBlockTableProvider, LogRow, TimeRange, labels, register_log_blocks,
    register_log_blocks_from_object_store, series_fingerprint, write_log_block,
    write_log_block_to_object_store,
};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::datasource::TableProvider;
use datafusion::datasource::provider::TableProviderFilterPushDown;
use datafusion::prelude::SessionContext;
use datafusion::prelude::{col, lit};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use std::sync::Arc;

#[tokio::test]
async fn datafusion_table_scans_only_planned_log_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let api = series_fingerprint(&labels([("app", "api"), ("env", "prod")]));
    let worker = series_fingerprint(&labels([("app", "worker"), ("env", "prod")]));

    let planned = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();
    write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(worker, 25, "worker error", BTreeMap::new())],
    )
    .unwrap();

    let ctx = SessionContext::new();
    register_log_blocks(&ctx, "logs", dir.path(), &[planned]).unwrap();

    let batches = ctx
        .sql(
            "select timestamp_ns, line \
             from logs \
             where line like '%error%' \
             order by timestamp_ns",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert!(batches.len() == 1);
    let batch = &batches[0];
    let timestamps = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let lines = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert!(batch.num_rows() == 1);
    assert!(timestamps.value(0) == 19);
    assert!(lines.value(0) == "api error");
}

#[tokio::test]
async fn log_block_table_provider_exposes_planned_filter_pushdown() {
    let dir = tempfile::tempdir().unwrap();
    let api = series_fingerprint(&labels([("app", "api"), ("env", "prod")]));
    let worker = series_fingerprint(&labels([("app", "worker"), ("env", "prod")]));

    let planned = write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();
    write_log_block(
        dir.path(),
        &BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(worker, 25, "worker error", BTreeMap::new())],
    )
    .unwrap();

    let provider = LogBlockTableProvider::try_new(dir.path(), &[planned]).unwrap();
    let timestamp_filter = col("timestamp_ns").gt_eq(lit(19_i64));
    let fingerprint_filter = col("series_fingerprint").eq(lit(api));
    let line_filter = col("line").eq(lit("api error"));
    let unsupported_filter = col("structured_metadata").eq(lit("api error"));

    assert!(
        provider
            .supports_filters_pushdown(&[
                &timestamp_filter,
                &fingerprint_filter,
                &line_filter,
                &unsupported_filter
            ])
            .unwrap()
            == vec![
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Unsupported,
            ]
    );

    let ctx = SessionContext::new();
    ctx.register_table("logs", std::sync::Arc::new(provider))
        .unwrap();
    let batches = ctx
        .sql(&format!(
            "select timestamp_ns, line \
             from logs \
             where timestamp_ns >= 19 and series_fingerprint = {api} \
             order by timestamp_ns"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert!(batches.len() == 1);
    let batch = &batches[0];
    let timestamps = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let lines = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert!(batch.num_rows() == 1);
    assert!(timestamps.value(0) == 19);
    assert!(lines.value(0) == "api error");
}

#[tokio::test]
async fn log_block_table_provider_scans_planned_object_store_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let prefix = ObjectPath::from("logs");
    let api = series_fingerprint(&labels([("app", "api"), ("env", "prod")]));
    let worker = series_fingerprint(&labels([("app", "worker"), ("env", "prod")]));

    let planned = write_log_block_to_object_store(
        store.as_ref(),
        &prefix,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .await
    .unwrap();
    write_log_block_to_object_store(
        store.as_ref(),
        &prefix,
        &BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(worker, 25, "worker error", BTreeMap::new())],
    )
    .await
    .unwrap();

    let provider = LogBlockTableProvider::try_new_object_store(store, prefix, &[planned]).unwrap();
    assert!(provider.planned_blocks().len() == 1);

    let ctx = SessionContext::new();
    ctx.register_table("logs", Arc::new(provider)).unwrap();
    let batches = ctx
        .sql(&format!(
            "select timestamp_ns, line \
             from logs \
             where line like '%error%' and series_fingerprint = {api} \
             order by timestamp_ns"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert!(batches.len() == 1);
    let batch = &batches[0];
    let timestamps = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let lines = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert!(batch.num_rows() == 1);
    assert!(timestamps.value(0) == 19);
    assert!(lines.value(0) == "api error");
}

#[tokio::test]
async fn registers_planned_object_store_blocks_as_datafusion_table() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let prefix = ObjectPath::from("logs");
    let api = series_fingerprint(&labels([("app", "api"), ("env", "prod")]));

    let planned = write_log_block_to_object_store(
        store.as_ref(),
        &prefix,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .await
    .unwrap();

    let ctx = SessionContext::new();
    register_log_blocks_from_object_store(&ctx, "logs", store, prefix, &[planned]).unwrap();
    let batches = ctx
        .sql(
            "select timestamp_ns, line \
             from logs \
             where line like '%error%' \
             order by timestamp_ns",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert!(batches.len() == 1);
    let batch = &batches[0];
    let timestamps = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let lines = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert!(batch.num_rows() == 1);
    assert!(timestamps.value(0) == 19);
    assert!(lines.value(0) == "api error");
}

#[tokio::test]
async fn datafusion_table_rejects_empty_block_list() {
    let ctx = SessionContext::new();

    let error = register_log_blocks(&ctx, "logs", "/", &[]).unwrap_err();

    assert!(error.to_string().contains("no log blocks"));
}
