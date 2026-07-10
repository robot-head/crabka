use arrow::{array::AsArray, datatypes::Int64Type};
use assert2::{assert, check};
use crabka_blockstore::{LabelMatcher, Labels, MatchOp};
use crabka_metrics::{
    BucketSpan, NativeHistogram, ResetHint, SamplePayload, WalExemplar, WalRecord,
};

use super::*;
use crate::{
    EngineOpts, PromqlEngine, PromqlError, QueryResult, SampleValue, WalHead,
    store::{
        LabelNameCardinality, LabelValueCardinality, MetricStore, NamedTsdbStat, ScanResult,
        TsdbHeadStats,
    },
};

fn lbls(pairs: &[(&str, &str)]) -> Labels {
    let mut labels = Labels::new();
    for (key, value) in pairs {
        labels.insert(*key, *value);
    }
    labels
}

fn native_histogram() -> NativeHistogram {
    NativeHistogram {
        schema: 0,
        is_float: false,
        reset_hint: ResetHint::No,
        zero_threshold: 1e-128,
        zero_count: 0.0,
        count: 2.0,
        sum: 3.0,
        positive_spans: vec![BucketSpan {
            offset: 0,
            length: 1,
        }],
        positive_counts: vec![2.0],
        negative_spans: Vec::new(),
        negative_counts: Vec::new(),
        custom_values: None,
        start_timestamp_ms: None,
    }
}

async fn count_rows(result: &ScanResult, table: &str) -> i64 {
    let df = result
        .ctx
        .sql(&format!("SELECT count(*) AS c FROM {table}"))
        .await
        .unwrap();
    let output = df.collect().await.unwrap();
    output[0].column(0).as_primitive::<Int64Type>().value(0)
}

fn float_record(tenant: &str, labels: &Labels, timestamp_ms: i64, value: f64) -> WalRecord {
    WalRecord {
        tenant: tenant.to_string(),
        labels: labels
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
        payload: SamplePayload::Float {
            timestamp_ms,
            value,
            start_timestamp_ms: None,
        },
        exemplars: Vec::new(),
    }
}

fn store_with_float_and_hist_series() -> (InMemoryMetricStore, Labels) {
    let mut store = InMemoryMetricStore::new();
    let up_api = lbls(&[("__name__", "up"), ("job", "api")]);
    let up_worker = lbls(&[("__name__", "up"), ("job", "worker")]);
    let latency = lbls(&[("__name__", "latency_seconds"), ("job", "api")]);
    store.push_float("tenant-a", up_api.clone(), 1_000, 1.0);
    store.push_float("tenant-a", up_worker, 2_000, 2.0);
    store.push_histogram("tenant-a", latency, 3_000, native_histogram());
    (store, up_api)
}

fn expected_label_name_cardinality() -> Vec<LabelNameCardinality> {
    vec![
        LabelNameCardinality {
            name: "__name__".to_string(),
            series_count: 3,
        },
        LabelNameCardinality {
            name: "job".to_string(),
            series_count: 3,
        },
    ]
}

fn expected_label_value_cardinality() -> Vec<LabelValueCardinality> {
    vec![
        LabelValueCardinality {
            label_name: "__name__".to_string(),
            label_value: "up".to_string(),
            series_count: 2,
        },
        LabelValueCardinality {
            label_name: "job".to_string(),
            label_value: "api".to_string(),
            series_count: 2,
        },
        LabelValueCardinality {
            label_name: "__name__".to_string(),
            label_value: "latency_seconds".to_string(),
            series_count: 1,
        },
        LabelValueCardinality {
            label_name: "job".to_string(),
            label_value: "worker".to_string(),
            series_count: 1,
        },
    ]
}

fn expected_metric_name_stats() -> Vec<NamedTsdbStat> {
    vec![
        NamedTsdbStat {
            name: "up".to_string(),
            value: 2,
        },
        NamedTsdbStat {
            name: "latency_seconds".to_string(),
            value: 1,
        },
    ]
}

fn expected_label_value_count_stats() -> Vec<NamedTsdbStat> {
    vec![
        NamedTsdbStat {
            name: "__name__".to_string(),
            value: 2,
        },
        NamedTsdbStat {
            name: "job".to_string(),
            value: 2,
        },
    ]
}

fn expected_label_memory_stats() -> Vec<NamedTsdbStat> {
    vec![
        NamedTsdbStat {
            name: "__name__".to_string(),
            value: 43,
        },
        NamedTsdbStat {
            name: "job".to_string(),
            value: 21,
        },
    ]
}

fn expected_label_pair_stats() -> Vec<NamedTsdbStat> {
    vec![
        NamedTsdbStat {
            name: "__name__=up".to_string(),
            value: 2,
        },
        NamedTsdbStat {
            name: "job=api".to_string(),
            value: 2,
        },
        NamedTsdbStat {
            name: "__name__=latency_seconds".to_string(),
            value: 1,
        },
        NamedTsdbStat {
            name: "job=worker".to_string(),
            value: 1,
        },
    ]
}

#[test]
fn row_matches_rejects_outside_bounds_before_matching_labels() {
    let labels = lbls(&[("__name__", "up"), ("job", "api")]);
    let matchers =
        prepare_matchers(&[LabelMatcher::new("__name__", MatchOp::Eq, "up")]).expect("matchers");
    let fp = labels.fingerprint();

    for (ts_ms, want) in [(999, false), (1_000, true), (2_000, true), (2_001, false)] {
        assert!(
            row_matches(fp, &labels, ts_ms, &matchers, 1_000, 2_000) == want,
            "case {ts_ms}"
        );
    }

    let mismatch =
        prepare_matchers(&[LabelMatcher::new("job", MatchOp::Eq, "worker")]).expect("matchers");
    assert!(!row_matches(fp, &labels, 1_500, &mismatch, 1_000, 2_000));
}

#[tokio::test]
async fn replay_wal_records_populates_queryable_head() {
    let mut store = InMemoryMetricStore::new();
    let series_labels = vec![
        ("__name__".to_string(), "up".to_string()),
        ("job".to_string(), "api".to_string()),
    ];
    store.apply_wal_record(&WalRecord {
        tenant: "tenant-a".to_string(),
        labels: series_labels.clone(),
        payload: SamplePayload::Float {
            timestamp_ms: 10_000,
            value: 1.0,
            start_timestamp_ms: None,
        },
        exemplars: Vec::new(),
    });
    store.apply_wal_record(&WalRecord {
        tenant: "tenant-a".to_string(),
        labels: vec![
            (
                "__name__".to_string(),
                "request_duration_seconds".to_string(),
            ),
            ("job".to_string(), "api".to_string()),
        ],
        payload: SamplePayload::Hist {
            timestamp_ms: 10_000,
            hist: native_histogram(),
        },
        exemplars: Vec::new(),
    });
    store.apply_wal_record(&WalRecord {
        tenant: "tenant-a".to_string(),
        labels: series_labels.clone(),
        payload: SamplePayload::Metadata {
            metric_family_name: "up".to_string(),
            metric_type: "gauge".to_string(),
            help: "Target health.".to_string(),
            unit: String::new(),
        },
        exemplars: Vec::new(),
    });
    store.apply_wal_record(&WalRecord {
        tenant: "tenant-a".to_string(),
        labels: series_labels.clone(),
        payload: SamplePayload::Exemplars,
        exemplars: vec![WalExemplar {
            labels: vec![("trace_id".to_string(), "abc".to_string())],
            value: 1.0,
            timestamp_ms: 10_000,
        }],
    });

    let engine = PromqlEngine::new(std::sync::Arc::new(store.clone()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "up", 10_000)
        .await
        .expect("query");
    let QueryResult::InstantVector(vector) = result else {
        panic!("expected vector");
    };
    check!(vector[0].value == SampleValue::Float(1.0));
    check!(store.metadata("tenant-a", Some("up")).await.unwrap()[0].help == "Target health.");
    check!(
        store.exemplars("tenant-a", &[], 0, 10_000).await.unwrap()[0].labels
            == lbls(&[("trace_id", "abc")])
    );
    check!(
        store
            .scan("tenant-a", &[], 0, 10_000)
            .await
            .unwrap()
            .histogram_table
            .is_some()
    );
}

#[tokio::test]
async fn bulk_wal_replay_and_retention_are_observable() {
    assert!(DEFAULT_RETENTION_MS == 21_600_000);

    let records = [
        float_record(
            "tenant-a",
            &lbls(&[("__name__", "up"), ("job", "api")]),
            10_000,
            1.0,
        ),
        float_record(
            "tenant-a",
            &lbls(&[("__name__", "up"), ("job", "worker")]),
            20_000,
            2.0,
        ),
    ];

    let mut store = InMemoryMetricStore::with_retention_ms(5_000);
    assert!(store.retention_ms() == 5_000);
    store.set_retention_ms(7_000);
    assert!(store.retention_ms() == 7_000);
    store.apply_wal_records(&records);

    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let series = store
        .series("tenant-a", &matchers, 0, 30_000)
        .await
        .unwrap();
    assert!(series.len() == 2);

    let head = WalHead::with_retention_ms(9_000);
    assert!(head.retention_ms() == 9_000);
    head.apply_wal_records(&records);
    let jobs = head
        .label_values("tenant-a", "job", &matchers, 0, 30_000)
        .await
        .unwrap();
    assert!(jobs == vec!["api".to_string(), "worker".to_string()]);

    let stats = head.prune(20_000);
    assert!(
        stats
            == PruneStats {
                samples_dropped: 1,
                series_dropped: 1,
            }
    );
    let jobs = head
        .label_values("tenant-a", "job", &matchers, 0, 30_000)
        .await
        .unwrap();
    assert!(jobs == vec!["worker".to_string()]);
}

#[tokio::test]
async fn wal_head_delegates_metadata_cardinality_stats_and_blocks() {
    let (mut store, up_api) = store_with_float_and_hist_series();
    store.set_retention_ms(12_345);
    store.push_exemplar(
        "tenant-a",
        up_api.clone(),
        lbls(&[("trace_id", "abc")]),
        1_500,
        1.5,
    );
    store.push_metadata("tenant-a", "up", "gauge", "Target health.", "");
    store.push_tsdb_block("tenant-a", "block-a", 0, 5_000, 3, 3);
    store.record_offset(PartitionIndex(0), Offset(7));
    store.record_offset(PartitionIndex(0), Offset(9));

    let head = WalHead::from_store(store);
    check!(head.retention_ms() == 12_345);
    check!(
        head.watermarks().get(&PartitionIndex(0))
            == Some(&PartitionWatermark {
                low_water_offset: Offset(7),
                high_water_offset: Offset(9),
            })
    );

    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let names = head
        .label_names("tenant-a", &matchers, 0, 5_000)
        .await
        .unwrap();
    check!(names == vec!["__name__".to_string(), "job".to_string()]);
    let jobs = head
        .label_values("tenant-a", "job", &matchers, 0, 5_000)
        .await
        .unwrap();
    check!(jobs == vec!["api".to_string(), "worker".to_string()]);
    check!(
        head.exemplars("tenant-a", &matchers, 0, 5_000)
            .await
            .unwrap()[0]
            .labels
            == lbls(&[("trace_id", "abc")])
    );
    check!(head.metadata("tenant-a", Some("up")).await.unwrap()[0].help == "Target health.");
    check!(
        head.cardinality_active_series("tenant-a")
            .await
            .unwrap()
            .len()
            == 3
    );
    check!(
        head.cardinality_label_names("tenant-a").await.unwrap()
            == expected_label_name_cardinality()
    );
    check!(
        head.cardinality_label_values("tenant-a").await.unwrap()
            == expected_label_value_cardinality()
    );
    let stats = head.tsdb_stats("tenant-a").await.unwrap();
    assert_eq!(
        (
            stats.head_stats.num_series,
            stats.head_stats.num_samples,
            stats.series_count_by_metric_name,
        ),
        (3, 3, expected_metric_name_stats())
    );
    check!(head.tsdb_blocks("tenant-a").await.unwrap()[0].id == "block-a");
}

#[tokio::test]
async fn cloned_wal_head_sees_records_replayed_through_original_handle() {
    let head = WalHead::new();
    let query_handle = head.clone();
    head.apply_wal_record(&WalRecord {
        tenant: "tenant-a".to_string(),
        labels: vec![
            ("__name__".to_string(), "up".to_string()),
            ("job".to_string(), "api".to_string()),
        ],
        payload: SamplePayload::Float {
            timestamp_ms: 10_000,
            value: 1.0,
            start_timestamp_ms: None,
        },
        exemplars: Vec::new(),
    });

    let engine = PromqlEngine::new(std::sync::Arc::new(query_handle), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "up", 10_000)
        .await
        .expect("query");
    let QueryResult::InstantVector(vector) = result else {
        panic!("expected vector");
    };

    assert!(vector[0].value == SampleValue::Float(1.0));
}

#[tokio::test]
async fn scan_filters_by_matcher_and_time_and_registers_float_table() {
    let mut store = InMemoryMetricStore::new();
    store.push_float("t", lbls(&[("__name__", "up"), ("job", "a")]), 1000, 1.0);
    store.push_float("t", lbls(&[("__name__", "up"), ("job", "a")]), 2000, 1.0);
    store.push_float("t", lbls(&[("__name__", "up"), ("job", "b")]), 1000, 0.0);
    store.push_float("t", lbls(&[("__name__", "down")]), 1000, 9.0);

    let matchers = [
        LabelMatcher::new("__name__", MatchOp::Eq, "up"),
        LabelMatcher::new("job", MatchOp::Eq, "a"),
    ];
    let result = store.scan("t", &matchers, 0, 1500).await.unwrap();
    let table = result.float_table.clone().unwrap();
    assert!(result.histogram_table.is_none());
    assert!(count_rows(&result, &table).await == 1);
}

#[tokio::test]
async fn scan_filters_histograms_by_matcher_tenant_and_time() {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "t",
        lbls(&[("__name__", "latency_seconds"), ("job", "api")]),
        1_000,
        native_histogram(),
    );
    store.push_histogram(
        "t",
        lbls(&[("__name__", "latency_seconds"), ("job", "api")]),
        5_000,
        native_histogram(),
    );
    store.push_histogram(
        "t",
        lbls(&[("__name__", "latency_seconds"), ("job", "worker")]),
        1_000,
        native_histogram(),
    );
    store.push_histogram(
        "other",
        lbls(&[("__name__", "latency_seconds"), ("job", "api")]),
        1_000,
        native_histogram(),
    );

    let matchers = [
        LabelMatcher::new("__name__", MatchOp::Eq, "latency_seconds"),
        LabelMatcher::new("job", MatchOp::Eq, "api"),
    ];
    let result = store.scan("t", &matchers, 0, 1_500).await.unwrap();
    assert!(result.float_table.is_none());
    let table = result.histogram_table.clone().unwrap();
    assert!(count_rows(&result, &table).await == 1);
}

#[tokio::test]
async fn scan_with_no_match_returns_none_tables() {
    let mut store = InMemoryMetricStore::new();
    store.push_float("t", lbls(&[("__name__", "up")]), 1000, 1.0);
    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "absent")];
    let result = store.scan("t", &matchers, 0, 5000).await.unwrap();
    assert_eq!(
        (
            result.float_table.is_none(),
            result.histogram_table.is_none()
        ),
        (true, true)
    );
}

#[tokio::test]
async fn scan_validates_regex_matchers_before_row_iteration() {
    let store = InMemoryMetricStore::new();
    let matchers = [LabelMatcher::new("__name__", MatchOp::Re, "[")];

    let Err(error) = store.scan("missing", &matchers, 0, 5000).await else {
        panic!("expected invalid regex to fail before scanning rows");
    };

    assert!(matches!(error, PromqlError::Plan(_)));
    assert!(error.to_string().contains("bad regex"));
}

#[tokio::test]
async fn label_values_returns_distinct_for_name() {
    let mut store = InMemoryMetricStore::new();
    store.push_float("t", lbls(&[("__name__", "up"), ("job", "a")]), 1, 1.0);
    store.push_float("t", lbls(&[("__name__", "up"), ("job", "b")]), 1, 1.0);
    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let values = store
        .label_values("t", "job", &matchers, 0, 10)
        .await
        .unwrap();
    assert!(values == vec!["a".to_string(), "b".to_string()]);
}

#[tokio::test]
async fn series_filters_histograms_by_matcher_and_time() {
    let mut store = InMemoryMetricStore::new();
    let api = lbls(&[("__name__", "latency_seconds"), ("job", "api")]);
    let worker = lbls(&[("__name__", "latency_seconds"), ("job", "worker")]);
    store.push_histogram("t", api.clone(), 1_000, native_histogram());
    store.push_histogram("t", api.clone(), 5_000, native_histogram());
    store.push_histogram("t", worker, 1_000, native_histogram());

    let matchers = [
        LabelMatcher::new("__name__", MatchOp::Eq, "latency_seconds"),
        LabelMatcher::new("job", MatchOp::Eq, "api"),
    ];
    let series = store.series("t", &matchers, 0, 1_500).await.unwrap();
    assert!(series == vec![api]);
}

#[tokio::test]
async fn regex_matchers_are_anchored_and_absent_labels_match_empty() {
    let mut store = InMemoryMetricStore::new();
    store.push_float("t", lbls(&[("__name__", "up"), ("job", "api")]), 1, 1.0);
    store.push_float("t", lbls(&[("__name__", "up")]), 1, 2.0);

    let anchored = [LabelMatcher::new("job", MatchOp::Re, "a")];
    assert!(
        store
            .scan("t", &anchored, 0, 10)
            .await
            .unwrap()
            .float_table
            .is_none()
    );

    let empty = [LabelMatcher::new("missing", MatchOp::Eq, "")];
    assert!(
        store
            .scan("t", &empty, 0, 10)
            .await
            .unwrap()
            .float_table
            .is_some()
    );
}

#[tokio::test]
async fn query_shard_matcher_filters_by_series_fingerprint_modulo() {
    let mut store = InMemoryMetricStore::new();
    let series = (0..12)
        .map(|id| lbls(&[("__name__", "up"), ("series", &id.to_string())]))
        .collect::<Vec<_>>();
    for labels in &series {
        store.push_float("t", labels.clone(), 1, 1.0);
    }

    let expected = series
        .iter()
        .filter(|labels| labels.fingerprint() % 2 == 0)
        .map(|labels| (labels.fingerprint(), labels.clone()))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();
    assert!(!expected.is_empty());
    assert!(expected.len() < series.len());

    let matchers = [
        LabelMatcher::new("__name__", MatchOp::Eq, "up"),
        LabelMatcher::new("__query_shard__", MatchOp::Eq, "1_of_2"),
    ];
    let got = store.series("t", &matchers, 0, 10).await.unwrap();

    assert!(got == expected);
}

#[tokio::test]
async fn query_shard_neq_matcher_excludes_matching_fingerprint_modulo() {
    let mut store = InMemoryMetricStore::new();
    let series = (0..12)
        .map(|id| lbls(&[("__name__", "up"), ("series", &id.to_string())]))
        .collect::<Vec<_>>();
    for labels in &series {
        store.push_float("t", labels.clone(), 1, 1.0);
    }

    let expected = series
        .iter()
        .filter(|labels| labels.fingerprint() % 2 != 0)
        .map(|labels| (labels.fingerprint(), labels.clone()))
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();
    assert!(!expected.is_empty());
    assert!(expected.len() < series.len());

    let matchers = [
        LabelMatcher::new("__name__", MatchOp::Eq, "up"),
        LabelMatcher::new("__query_shard__", MatchOp::Neq, "1_of_2"),
    ];
    let got = store.series("t", &matchers, 0, 10).await.unwrap();

    assert!(got == expected);
}

#[tokio::test]
async fn prune_drops_old_samples() {
    let mut store = InMemoryMetricStore::with_retention_ms(1_000);
    let series = lbls(&[("__name__", "up"), ("job", "api")]);
    // ts 100 and 500 are old; ts 9_500 and 9_900 are within the window.
    store.push_float("t", series.clone(), 100, 1.0);
    store.push_float("t", series.clone(), 500, 2.0);
    store.push_float("t", series.clone(), 9_500, 3.0);
    store.push_float("t", series.clone(), 9_900, 4.0);
    // A histogram sample that is also old.
    store.push_histogram("t", series.clone(), 200, native_histogram());

    // now = 10_000, retention = 1_000 -> cutoff = 9_000; ts < 9_000 dropped.
    let stats = store.prune(10_000);
    assert_eq!((stats.samples_dropped, stats.series_dropped), (3, 0));

    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let remaining = store
        .scan("t", &matchers, i64::MIN, i64::MAX)
        .await
        .unwrap();
    let table = remaining.float_table.clone().unwrap();
    let df = remaining
        .ctx
        .sql(&format!("SELECT count(*) AS c FROM {table}"))
        .await
        .unwrap();
    let output = df.collect().await.unwrap();
    let count = output[0].column(0).as_primitive::<Int64Type>().value(0);
    assert!(count == 2);
    // The old histogram sample is gone.
    assert!(remaining.histogram_table.is_none());
}

#[tokio::test]
async fn prune_counts_partial_histogram_and_exemplar_retention() {
    let mut store = InMemoryMetricStore::with_retention_ms(1_000);
    let live = lbls(&[("__name__", "latency_seconds"), ("job", "api")]);
    let stale = lbls(&[("__name__", "latency_seconds"), ("job", "old")]);
    store.push_float("t", live.clone(), 8_999, 1.0);
    store.push_float("t", live.clone(), 9_000, 2.0);
    store.push_float("t", stale.clone(), 1_000, 3.0);
    store.push_histogram("t", live.clone(), 8_999, native_histogram());
    store.push_histogram("t", live.clone(), 9_000, native_histogram());
    store.push_exemplar("t", live.clone(), lbls(&[("trace_id", "old")]), 8_999, 1.0);
    store.push_exemplar("t", live.clone(), lbls(&[("trace_id", "new")]), 9_000, 2.0);

    let stats = store.prune(10_000);
    assert!(
        stats
            == PruneStats {
                samples_dropped: 4,
                series_dropped: 1,
            }
    );

    let matchers = [LabelMatcher::new("job", MatchOp::Eq, "api")];
    let result = store
        .scan("t", &matchers, i64::MIN, i64::MAX)
        .await
        .unwrap();
    check!(count_rows(&result, result.float_table.as_ref().unwrap()).await == 1);
    check!(count_rows(&result, result.histogram_table.as_ref().unwrap()).await == 1);
    let exemplars = store
        .exemplars("t", &matchers, i64::MIN, i64::MAX)
        .await
        .unwrap();
    check!(exemplars.len() == 1);
    check!(exemplars[0].labels == lbls(&[("trace_id", "new")]));
    let stale_matchers = [LabelMatcher::new("job", MatchOp::Eq, "old")];
    check!(
        store
            .series("t", &stale_matchers, i64::MIN, i64::MAX)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn prune_removes_emptied_series_from_index() {
    let mut store = InMemoryMetricStore::with_retention_ms(1_000);
    let stale = lbls(&[("__name__", "up"), ("job", "old")]);
    let fresh = lbls(&[("__name__", "up"), ("job", "new")]);
    store.push_float("t", stale.clone(), 100, 1.0);
    store.push_float("t", fresh.clone(), 9_900, 2.0);

    let stats = store.prune(10_000);
    assert_eq!((stats.samples_dropped, stats.series_dropped), (1, 1));

    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let series = store
        .series("t", &matchers, i64::MIN, i64::MAX)
        .await
        .unwrap();
    assert!(series == vec![fresh.clone()]);

    // The emptied series' label value no longer appears on the label surface.
    let jobs = store
        .label_values("t", "job", &matchers, i64::MIN, i64::MAX)
        .await
        .unwrap();
    assert!(jobs == vec!["new".to_string()]);
}

#[tokio::test]
async fn store_cardinality_and_tsdb_stats_include_float_and_hist_series() {
    let (store, _) = store_with_float_and_hist_series();

    check!(
        store.cardinality_label_names("tenant-a").await.unwrap()
            == expected_label_name_cardinality()
    );
    check!(
        store.cardinality_label_values("tenant-a").await.unwrap()
            == expected_label_value_cardinality()
    );

    let stats = store.tsdb_stats("tenant-a").await.unwrap();
    assert_eq!(
        (
            stats.head_stats,
            stats.label_value_count_by_label_name,
            stats.memory_in_bytes_by_label_name,
            stats.series_count_by_label_value_pair,
        ),
        (
            TsdbHeadStats {
                num_series: 3,
                num_samples: 3,
                num_chunks: 3,
                min_time: 1_000,
                max_time: 3_000,
            },
            expected_label_value_count_stats(),
            expected_label_memory_stats(),
            expected_label_pair_stats(),
        )
    );
}

#[test]
fn offsets_track_low_and_high_water() {
    let head = WalHead::new();
    let record = |ts: i64| WalRecord {
        tenant: "t".to_string(),
        labels: vec![("__name__".to_string(), "up".to_string())],
        payload: SamplePayload::Float {
            timestamp_ms: ts,
            value: 1.0,
            start_timestamp_ms: None,
        },
        exemplars: Vec::new(),
    };

    // No offsets ingested yet.
    assert_eq!(
        (
            head.high_water_offset(PartitionIndex(0)),
            head.low_water_offset(PartitionIndex(0)),
        ),
        (None, None)
    );

    head.apply_wal_record_at(&record(10), PartitionIndex(0), Offset(5));
    head.apply_wal_record_at(&record(20), PartitionIndex(0), Offset(6));
    head.apply_wal_record_at(&record(30), PartitionIndex(1), Offset(100));

    // High water is the latest applied offset per partition, low water the
    // first; untracked partitions stay empty.
    for (partition, want_high, want_low) in [
        (0, Some(6), Some(5)),
        (1, Some(100), Some(100)),
        (2, None, None),
    ] {
        assert_eq!(
            (
                head.high_water_offset(PartitionIndex(partition)),
                head.low_water_offset(PartitionIndex(partition)),
            ),
            (want_high.map(Offset), want_low.map(Offset)),
            "case partition {partition}"
        );
    }

    // Pruning does not move offsets (they track ingestion, not retention).
    head.prune(i64::MAX);
    assert_eq!(
        (
            head.high_water_offset(PartitionIndex(0)),
            head.low_water_offset(PartitionIndex(0)),
        ),
        (Some(Offset(6)), Some(Offset(5)))
    );
}
