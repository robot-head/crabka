use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Float64Builder, Int64Builder, MapBuilder, StringBuilder, UInt64Builder},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use assert2::{assert, check};
use crabka_blockstore::{BlockStore, Labels};
use crabka_metrics::{
    CompactionIndexManifest, CompactionObjectPlan, CompactionSeriesLabels, MetricBlockKind,
    encode_float_samples, exemplar_schema, float_sample_schema, metadata_schema,
};
use object_store::{ObjectStore, memory::InMemory};

use super::MetricBlockStore;
use crate::{
    EngineOpts, InstantSample, MetadataRecord, MetricStore, NamedTsdbStat, PromqlEngine,
    QueryResult, SampleValue, TsdbBlock, TsdbHeadStats, TsdbStats,
};

fn labels(pairs: &[(&str, &str)]) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in pairs {
        labels.insert(*name, *value);
    }
    labels
}

#[tokio::test]
async fn prometheus_query_reads_float_samples_from_blockstore() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let mut block_store = BlockStore::new(object_store, base);

    let series_labels = labels(&[("__name__", "up"), ("job", "api")]);
    let fp = series_labels.fingerprint();
    let batch = encode_float_samples(&[(fp, 1_000, 1.0)]).unwrap();
    let block_meta = block_store
        .writer()
        .write_block(
            "tenant-a",
            "metrics/float/0001.parquet",
            float_sample_schema(),
            &[batch],
        )
        .await
        .unwrap();
    block_store
        .index_mut()
        .add_series("tenant-a", fp, &series_labels);
    block_store.index_mut().add_block(&block_meta);

    let store = MetricBlockStore::new(block_store);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine.query_instant("tenant-a", "up", 1_000).await.unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected instant vector");
    };
    assert!(
        samples
            == vec![InstantSample {
                labels: series_labels,
                ts_ms: 1_000,
                value: SampleValue::Float(1.0),
            }]
    );
}

#[tokio::test]
async fn prometheus_query_rebuilds_float_index_from_compaction_manifest() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let writer_store = BlockStore::new(object_store.clone(), base.clone());

    let series_labels = labels(&[("__name__", "up"), ("job", "api")]);
    let fp = series_labels.fingerprint();
    let batch = encode_float_samples(&[(fp, 1_000, 1.0)]).unwrap();
    let block_meta = writer_store
        .writer()
        .write_block(
            "tenant-a",
            "metrics/float/0001.parquet",
            float_sample_schema(),
            &[batch],
        )
        .await
        .unwrap();
    let manifest = CompactionIndexManifest::from_block_meta(
        MetricBlockKind::Float,
        &CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: "metrics/float/0001.index".to_string(),
            first_offset: 0,
            last_offset: 0,
            row_count: block_meta.row_count,
        },
        &block_meta,
        vec![CompactionSeriesLabels {
            fingerprint: fp,
            labels: series_labels.clone(),
        }],
    );

    let fresh_store = BlockStore::new(object_store, base);
    let store = MetricBlockStore::from_compaction_manifests(fresh_store, None, &[manifest]);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine.query_instant("tenant-a", "up", 1_000).await.unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected instant vector");
    };
    assert!(
        samples
            == vec![InstantSample {
                labels: series_labels,
                ts_ms: 1_000,
                value: SampleValue::Float(1.0),
            }]
    );
}

#[tokio::test]
async fn tsdb_blocks_reports_compaction_manifest_blocks() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let writer_store = BlockStore::new(object_store.clone(), base.clone());

    let series_labels = labels(&[("__name__", "up"), ("job", "api")]);
    let fp = series_labels.fingerprint();
    let batch = encode_float_samples(&[(fp, 1_000, 1.0), (fp, 2_000, 0.0)]).unwrap();
    let block_meta = writer_store
        .writer()
        .write_block(
            "tenant-a",
            "metrics/float/0002.parquet",
            float_sample_schema(),
            &[batch],
        )
        .await
        .unwrap();
    let manifest = CompactionIndexManifest::from_block_meta(
        MetricBlockKind::Float,
        &CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: "metrics/float/0002.index".to_string(),
            first_offset: 0,
            last_offset: 1,
            row_count: block_meta.row_count,
        },
        &block_meta,
        vec![CompactionSeriesLabels {
            fingerprint: fp,
            labels: series_labels,
        }],
    );

    let fresh_store = BlockStore::new(object_store, base);
    let store = MetricBlockStore::from_compaction_manifests(fresh_store, None, &[manifest]);
    let blocks = store.tsdb_blocks("tenant-a").await.unwrap();

    assert!(
        blocks
            == vec![TsdbBlock {
                id: "metrics/float/0002.parquet".to_string(),
                min_time: 1_000,
                max_time: 2_000,
                num_samples: 2,
                num_series: 1,
            }]
    );
}

#[tokio::test]
async fn index_metadata_methods_report_float_and_histogram_series() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let mut floats = BlockStore::new(object_store.clone(), base.clone());
    let mut histograms = BlockStore::new(object_store, base);

    let up = labels(&[("__name__", "up"), ("instance", "a"), ("job", "api")]);
    let latency = labels(&[
        ("__name__", "http_request_duration_seconds"),
        ("instance", "b"),
        ("job", "api"),
        ("le", "0.5"),
    ]);
    floats
        .index_mut()
        .add_series("tenant-a", up.fingerprint(), &up);
    histograms
        .index_mut()
        .add_series("tenant-a", latency.fingerprint(), &latency);
    let store = MetricBlockStore::with_histograms(floats, histograms);

    let names = store.label_names("tenant-a", &[], 0, 10_000).await.unwrap();
    assert!(names == vec!["__name__", "instance", "job", "le"]);

    let job_values = store
        .label_values("tenant-a", "job", &[], 0, 10_000)
        .await
        .unwrap();
    assert!(job_values == vec!["api"]);
    let instance_values = store
        .label_values("tenant-a", "instance", &[], 0, 10_000)
        .await
        .unwrap();
    assert!(instance_values == vec!["a", "b"]);

    let mut active_series = store.cardinality_active_series("tenant-a").await.unwrap();
    active_series.sort_by_key(|labels| labels.get("instance").unwrap_or("").to_string());
    assert!(active_series == vec![up, latency]);

    let label_names = store.cardinality_label_names("tenant-a").await.unwrap();
    let name_counts = label_names
        .iter()
        .map(|stat| (stat.name.as_str(), stat.series_count))
        .collect::<Vec<_>>();
    assert!(name_counts == vec![("__name__", 2), ("instance", 2), ("job", 2), ("le", 1)]);

    let label_values = store.cardinality_label_values("tenant-a").await.unwrap();
    let value_counts = label_values
        .iter()
        .map(|stat| {
            (
                stat.label_name.as_str(),
                stat.label_value.as_str(),
                stat.series_count,
            )
        })
        .collect::<Vec<_>>();
    assert!(
        value_counts
            == vec![
                ("job", "api", 2),
                ("__name__", "http_request_duration_seconds", 1),
                ("__name__", "up", 1),
                ("instance", "a", 1),
                ("instance", "b", 1),
                ("le", "0.5", 1),
            ]
    );

    let stats = store.tsdb_stats("tenant-a").await.unwrap();
    assert!(
        stats
            == TsdbStats {
                head_stats: TsdbHeadStats {
                    num_series: 2,
                    num_samples: 0,
                    num_chunks: 2,
                    min_time: 0,
                    max_time: 0,
                },
                series_count_by_metric_name: expected_stats(&[
                    ("http_request_duration_seconds", 1),
                    ("up", 1),
                ]),
                label_value_count_by_label_name: expected_stats(&[
                    ("__name__", 2),
                    ("instance", 2),
                    ("job", 1),
                    ("le", 1),
                ]),
                memory_in_bytes_by_label_name: expected_stats(&[
                    ("__name__", 47),
                    ("instance", 18),
                    ("job", 12),
                    ("le", 5),
                ]),
                series_count_by_label_value_pair: expected_stats(&[
                    ("job=api", 2),
                    ("__name__=http_request_duration_seconds", 1),
                    ("__name__=up", 1),
                    ("instance=a", 1),
                    ("instance=b", 1),
                    ("le=0.5", 1),
                ]),
            }
    );
}

#[tokio::test]
async fn exemplars_reads_compacted_exemplar_sidecar_blocks() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let writer_store = BlockStore::new(object_store.clone(), base.clone());

    let series_labels = labels(&[("__name__", "http_requests_total"), ("job", "api")]);
    let fp = series_labels.fingerprint();
    let batch = exemplar_batch(fp, 10_500, 7.0, "abc", "def", "kind", "slow");
    let block_meta = writer_store
        .writer()
        .write_block(
            "tenant-a",
            "metrics/exemplars/0003.parquet",
            exemplar_schema(),
            &[batch],
        )
        .await
        .unwrap();
    let manifest = CompactionIndexManifest::from_block_meta(
        MetricBlockKind::Exemplars,
        &CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: "metrics/exemplars/0003.index".to_string(),
            first_offset: 0,
            last_offset: 0,
            row_count: block_meta.row_count,
        },
        &block_meta,
        vec![CompactionSeriesLabels {
            fingerprint: fp,
            labels: series_labels.clone(),
        }],
    );

    let fresh_store = BlockStore::new(object_store, base);
    let store = MetricBlockStore::from_compaction_manifests(fresh_store, None, &[manifest]);
    let exemplars = store
        .exemplars(
            "tenant-a",
            &[crabka_blockstore::LabelMatcher {
                name: "job".to_string(),
                op: crabka_blockstore::MatchOp::Eq,
                value: "api".to_string(),
            }],
            10_000,
            11_000,
        )
        .await
        .unwrap();

    check!(exemplars.len() == 1);
    check!(exemplars[0].series_labels == series_labels);
    check!(exemplars[0].labels.get("trace_id") == Some("abc"));
    check!(exemplars[0].labels.get("span_id") == Some("def"));
    check!(exemplars[0].labels.get("kind") == Some("slow"));
    check!(exemplars[0].ts_ms == 10_500);
    check!((exemplars[0].value - 7.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn exemplars_include_closed_range_boundaries_and_filter_outside_rows() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let writer_store = BlockStore::new(object_store.clone(), base.clone());

    let series_labels = labels(&[("__name__", "http_requests_total"), ("job", "api")]);
    let fp = series_labels.fingerprint();
    let batch = exemplar_batch_from_rows(&[
        (fp, 9_999, 1.0, "too-low", "s1", "kind", "outside"),
        (fp, 10_000, 2.0, "start", "s2", "kind", "inside"),
        (fp, 11_000, 3.0, "end", "s3", "kind", "inside"),
        (fp, 11_001, 4.0, "too-high", "s4", "kind", "outside"),
    ]);
    let block_meta = writer_store
        .writer()
        .write_block(
            "tenant-a",
            "metrics/exemplars/0005.parquet",
            exemplar_schema(),
            &[batch],
        )
        .await
        .unwrap();
    let manifest = CompactionIndexManifest::from_block_meta(
        MetricBlockKind::Exemplars,
        &CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: "metrics/exemplars/0005.index".to_string(),
            first_offset: 0,
            last_offset: 3,
            row_count: block_meta.row_count,
        },
        &block_meta,
        vec![CompactionSeriesLabels {
            fingerprint: fp,
            labels: series_labels.clone(),
        }],
    );

    let fresh_store = BlockStore::new(object_store, base);
    let store = MetricBlockStore::from_compaction_manifests(fresh_store, None, &[manifest]);
    let exemplars = store
        .exemplars(
            "tenant-a",
            &[crabka_blockstore::LabelMatcher {
                name: "job".to_string(),
                op: crabka_blockstore::MatchOp::Eq,
                value: "api".to_string(),
            }],
            10_000,
            11_000,
        )
        .await
        .unwrap();

    check!(exemplars.len() == 2);
    for (row, trace_id, span_id, ts_ms, value) in [
        (0_usize, "start", "s2", 10_000_i64, 2.0_f64),
        (1, "end", "s3", 11_000, 3.0),
    ] {
        check!(exemplars[row].series_labels == series_labels, "row {row}");
        check!(
            exemplars[row].labels.get("trace_id") == Some(trace_id),
            "row {row}"
        );
        check!(
            exemplars[row].labels.get("span_id") == Some(span_id),
            "row {row}"
        );
        check!(
            exemplars[row].labels.get("kind") == Some("inside"),
            "row {row}"
        );
        check!(exemplars[row].ts_ms == ts_ms, "row {row}");
        check!(
            exemplars[row].value.to_bits() == value.to_bits(),
            "row {row}"
        );
    }
}

#[tokio::test]
async fn metadata_reads_compacted_metadata_sidecar_blocks() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let writer_store = BlockStore::new(object_store.clone(), base.clone());

    let series_labels = labels(&[("__name__", "http_requests_total")]);
    let fp = series_labels.fingerprint();
    let batch = metadata_batch(
        fp,
        "http_requests_total",
        "counter",
        "Total HTTP requests.",
        "requests",
    );
    let block_meta = writer_store
        .writer()
        .write_block(
            "tenant-a",
            "metrics/metadata/0004.parquet",
            metadata_schema(),
            &[batch],
        )
        .await
        .unwrap();
    let manifest = CompactionIndexManifest::from_block_meta(
        MetricBlockKind::Metadata,
        &CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: "metrics/metadata/0004.index".to_string(),
            first_offset: 0,
            last_offset: 0,
            row_count: block_meta.row_count,
        },
        &block_meta,
        vec![CompactionSeriesLabels {
            fingerprint: fp,
            labels: series_labels,
        }],
    );

    let fresh_store = BlockStore::new(object_store, base);
    let store = MetricBlockStore::from_compaction_manifests(fresh_store, None, &[manifest]);
    let metadata = store
        .metadata("tenant-a", Some("http_requests_total"))
        .await
        .unwrap();

    assert!(
        metadata
            == vec![MetadataRecord {
                metric_family_name: "http_requests_total".to_string(),
                metric_type: "counter".to_string(),
                help: "Total HTTP requests.".to_string(),
                unit: "requests".to_string(),
            }]
    );
}

fn exemplar_batch(
    fingerprint: u64,
    timestamp_ms: i64,
    value: f64,
    trace_id: &str,
    span_id: &str,
    label_name: &str,
    label_value: &str,
) -> RecordBatch {
    exemplar_batch_from_rows(&[(
        fingerprint,
        timestamp_ms,
        value,
        trace_id,
        span_id,
        label_name,
        label_value,
    )])
}

fn exemplar_batch_from_rows(rows: &[(u64, i64, f64, &str, &str, &str, &str)]) -> RecordBatch {
    let mut fingerprints = UInt64Builder::new();
    let mut timestamps = Int64Builder::new();
    let mut values = Float64Builder::new();
    let mut trace_ids = StringBuilder::new();
    let mut span_ids = StringBuilder::new();
    let mut labels = MapBuilder::new(
        Some(arrow::array::builder::MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
    )
    .with_values_field(Field::new("value", DataType::Utf8, false));

    for (fingerprint, timestamp_ms, value, trace_id, span_id, label_name, label_value) in rows {
        fingerprints.append_value(*fingerprint);
        timestamps.append_value(*timestamp_ms);
        values.append_value(*value);
        trace_ids.append_value(*trace_id);
        span_ids.append_value(*span_id);
        labels.keys().append_value(*label_name);
        labels.values().append_value(*label_value);
        labels.append(true).unwrap();
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(fingerprints.finish()),
        Arc::new(timestamps.finish()),
        Arc::new(values.finish()),
        Arc::new(trace_ids.finish()),
        Arc::new(span_ids.finish()),
        Arc::new(labels.finish()),
    ];
    RecordBatch::try_new(exemplar_schema(), columns).unwrap()
}

fn metadata_batch(
    fingerprint: u64,
    metric_family_name: &str,
    metric_type: &str,
    help: &str,
    unit: &str,
) -> RecordBatch {
    let mut fingerprints = UInt64Builder::new();
    let mut timestamps = Int64Builder::new();
    let mut names = StringBuilder::new();
    let mut types = StringBuilder::new();
    let mut helps = StringBuilder::new();
    let mut units = StringBuilder::new();

    fingerprints.append_value(fingerprint);
    timestamps.append_value(0);
    names.append_value(metric_family_name);
    types.append_value(metric_type);
    helps.append_value(help);
    units.append_value(unit);

    let columns: Vec<ArrayRef> = vec![
        Arc::new(fingerprints.finish()),
        Arc::new(timestamps.finish()),
        Arc::new(names.finish()),
        Arc::new(types.finish()),
        Arc::new(helps.finish()),
        Arc::new(units.finish()),
    ];
    RecordBatch::try_new(metadata_schema(), columns).unwrap()
}

fn expected_stats(pairs: &[(&str, usize)]) -> Vec<NamedTsdbStat> {
    pairs
        .iter()
        .map(|(name, value)| NamedTsdbStat {
            name: (*name).to_string(),
            value: *value,
        })
        .collect()
}
