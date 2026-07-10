use std::sync::Arc;

use crabka_blockstore::Labels;

use crate::{
    EngineOpts, ExemplarRecord, InMemoryMetricStore, InstantSample, MergedMetricStore, MetricStore,
    NamedTsdbStat, PromqlEngine, QueryResult, SampleValue, TsdbHeadStats, TsdbStats,
};

fn labels(pairs: &[(&str, &str)]) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in pairs {
        labels.insert(*name, *value);
    }
    labels
}

#[tokio::test]
async fn instant_query_uses_hot_sample_newer_than_compacted_sample() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    let labels = labels(&[("__name__", "up"), ("job", "api")]);
    cold.push_float("tenant-a", labels.clone(), 10_000, 1.0);
    hot.push_float("tenant-a", labels.clone(), 20_000, 2.0);

    let store = MergedMetricStore::new(cold, hot);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "up", 20_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected instant vector");
    };
    assert2::assert!(
        samples
            == vec![InstantSample {
                labels,
                ts_ms: 20_000,
                value: SampleValue::Float(2.0),
            }]
    );
}

#[tokio::test]
async fn label_names_merges_cold_and_hot_series_metadata() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    cold.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("instance", "a"), ("job", "api")]),
        10_000,
        1.0,
    );
    hot.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("cluster", "prod"), ("job", "api")]),
        20_000,
        2.0,
    );

    let store = MergedMetricStore::new(cold, hot);
    let names = store.label_names("tenant-a", &[], 0, 30_000).await.unwrap();

    assert2::assert!(names == vec!["__name__", "cluster", "instance", "job"]);
}

#[tokio::test]
async fn label_values_merges_cold_and_hot_series_metadata() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    cold.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );
    hot.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "worker")]),
        20_000,
        2.0,
    );
    hot.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        30_000,
        3.0,
    );

    let store = MergedMetricStore::new(cold, hot);
    let values = store
        .label_values("tenant-a", "job", &[], 0, 30_000)
        .await
        .unwrap();

    assert2::assert!(values == vec!["api", "worker"]);
}

#[tokio::test]
async fn exemplars_merges_cold_and_hot_records() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    let series = labels(&[("__name__", "request_latency"), ("job", "api")]);
    cold.push_exemplar(
        "tenant-a",
        series.clone(),
        labels(&[("trace_id", "cold")]),
        10_000,
        1.0,
    );
    hot.push_exemplar(
        "tenant-a",
        series.clone(),
        labels(&[("trace_id", "hot")]),
        20_000,
        2.0,
    );

    let store = MergedMetricStore::new(cold, hot);
    let exemplars = store.exemplars("tenant-a", &[], 0, 30_000).await.unwrap();

    assert2::assert!(
        exemplars
            == vec![
                ExemplarRecord {
                    series_labels: series.clone(),
                    labels: labels(&[("trace_id", "cold")]),
                    ts_ms: 10_000,
                    value: 1.0,
                },
                ExemplarRecord {
                    series_labels: series,
                    labels: labels(&[("trace_id", "hot")]),
                    ts_ms: 20_000,
                    value: 2.0,
                },
            ]
    );
}

#[tokio::test]
async fn metadata_merges_cold_and_hot_records() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    cold.push_metadata(
        "tenant-a",
        "requests_total",
        "counter",
        "requests",
        "requests",
    );
    cold.push_metadata("tenant-a", "up", "gauge", "availability", "");
    hot.push_metadata(
        "tenant-a",
        "requests_total",
        "counter",
        "requests",
        "requests",
    );
    hot.push_metadata(
        "tenant-a",
        "latency_seconds",
        "histogram",
        "latency",
        "seconds",
    );

    let store = MergedMetricStore::new(cold, hot);
    let metadata = store.metadata("tenant-a", None).await.unwrap();
    let fields = metadata
        .iter()
        .map(|record| {
            (
                record.metric_family_name.as_str(),
                record.metric_type.as_str(),
                record.help.as_str(),
                record.unit.as_str(),
            )
        })
        .collect::<Vec<_>>();

    assert2::assert!(
        fields
            == vec![
                ("latency_seconds", "histogram", "latency", "seconds"),
                ("requests_total", "counter", "requests", "requests"),
                ("up", "gauge", "availability", ""),
            ]
    );
}

#[tokio::test]
async fn cardinality_methods_merge_cold_and_hot_series() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    let api = labels(&[("__name__", "up"), ("instance", "a"), ("job", "api")]);
    let worker = labels(&[("__name__", "up"), ("instance", "b"), ("job", "worker")]);
    cold.push_float("tenant-a", api.clone(), 10_000, 1.0);
    hot.push_float("tenant-a", worker.clone(), 20_000, 2.0);

    let store = MergedMetricStore::new(cold, hot);
    let mut active_series = store.cardinality_active_series("tenant-a").await.unwrap();
    active_series.sort_by_key(|labels| labels.get("instance").unwrap_or("").to_string());
    assert2::assert!(active_series == vec![api, worker]);

    let label_names = store.cardinality_label_names("tenant-a").await.unwrap();
    let name_counts = label_names
        .iter()
        .map(|stat| (stat.name.as_str(), stat.series_count))
        .collect::<Vec<_>>();
    assert2::assert!(name_counts == vec![("__name__", 2), ("instance", 2), ("job", 2)]);

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
    assert2::assert!(
        value_counts
            == vec![
                ("__name__", "up", 2),
                ("instance", "a", 1),
                ("instance", "b", 1),
                ("job", "api", 1),
                ("job", "worker", 1),
            ]
    );
}

#[tokio::test]
async fn tsdb_stats_merge_cold_and_hot_counts() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    cold.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );
    cold.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        20_000,
        2.0,
    );
    hot.push_float(
        "tenant-a",
        labels(&[("__name__", "errors_total"), ("job", "worker")]),
        30_000,
        3.0,
    );

    let store = MergedMetricStore::new(cold, hot);
    let stats = store.tsdb_stats("tenant-a").await.unwrap();

    let stat = |name: &str, value: usize| NamedTsdbStat {
        name: name.to_string(),
        value,
    };
    assert2::assert!(
        stats
            == TsdbStats {
                head_stats: TsdbHeadStats {
                    num_series: 2,
                    num_samples: 3,
                    num_chunks: 2,
                    min_time: 10_000,
                    max_time: 30_000,
                },
                series_count_by_metric_name: vec![stat("errors_total", 1), stat("up", 1)],
                label_value_count_by_label_name: vec![stat("__name__", 2), stat("job", 2)],
                // Byte counts sum name.len() + value.len() per series:
                // "__name__"+"up" (10) + "__name__"+"errors_total" (20) = 30;
                // "job"+"api" (6) + "job"+"worker" (9) = 15.
                memory_in_bytes_by_label_name: vec![stat("__name__", 30), stat("job", 15)],
                series_count_by_label_value_pair: vec![
                    stat("__name__=errors_total", 1),
                    stat("__name__=up", 1),
                    stat("job=api", 1),
                    stat("job=worker", 1),
                ],
            }
    );
}

#[tokio::test]
async fn tsdb_stats_ignore_empty_side_min_time() {
    let mut hot_only = InMemoryMetricStore::new();
    hot_only.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        40_000,
        1.0,
    );
    let store = MergedMetricStore::new(InMemoryMetricStore::new(), hot_only);
    let stats = store.tsdb_stats("tenant-a").await.unwrap();
    assert2::assert!(stats.head_stats.min_time == 40_000);

    let mut cold_only = InMemoryMetricStore::new();
    cold_only.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        50_000,
        1.0,
    );
    let store = MergedMetricStore::new(cold_only, InMemoryMetricStore::new());
    let stats = store.tsdb_stats("tenant-a").await.unwrap();
    assert2::assert!(stats.head_stats.min_time == 50_000);
}

#[tokio::test]
async fn tsdb_blocks_merges_cold_and_hot_blocks() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    cold.push_tsdb_block("tenant-a", "cold-b", 30_000, 40_000, 3, 1);
    hot.push_tsdb_block("tenant-a", "hot-a", 10_000, 20_000, 2, 1);
    cold.push_tsdb_block("tenant-a", "cold-a", 10_000, 15_000, 1, 1);

    let store = MergedMetricStore::new(cold, hot);
    let blocks = store.tsdb_blocks("tenant-a").await.unwrap();
    let ids = blocks
        .iter()
        .map(|block| block.id.as_str())
        .collect::<Vec<_>>();

    assert2::assert!(ids == vec!["cold-a", "hot-a", "cold-b"]);
}

#[test]
fn min_present_time_preserves_legitimate_zero_min_time() {
    // A store that holds samples whose earliest is epoch 0 must report 0,
    // not be treated as empty and discarded in favor of the other store.
    // Absent stores fall back to the present one; both-empty is 0.
    for (left, right, want) in [
        (Some(0), Some(50), 0),
        (Some(0), None, 0),
        (None, Some(0), 0),
        (None, Some(50), 50),
        (Some(50), None, 50),
        (None, None, 0),
        (Some(20), Some(50), 20),
    ] {
        assert2::assert!(super::min_present_time(left, right) == want);
    }
}

#[tokio::test]
async fn range_query_counts_sample_present_in_both_stores_once() {
    // The same (fingerprint, timestamp) sample lives in both cold and hot —
    // the steady state, since hot retention is time-based and independent of
    // compaction. Without (fp, ts) dedup the merged scan double-counts it.
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    let labels = labels(&[("__name__", "up"), ("job", "api")]);
    cold.push_float("tenant-a", labels.clone(), 10_000, 1.0);
    cold.push_float("tenant-a", labels.clone(), 20_000, 1.0);
    // Hot re-reports the 20s sample (still within hot retention) and adds 30s.
    hot.push_float("tenant-a", labels.clone(), 20_000, 1.0);
    hot.push_float("tenant-a", labels.clone(), 30_000, 1.0);

    let store = MergedMetricStore::new(cold, hot);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let result = engine
        .query_instant("tenant-a", "count_over_time(up[1m])", 30_000)
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected instant vector");
    };
    assert2::assert!(samples.len() == 1);
    // Three distinct timestamps (10s, 20s, 30s); the duplicated 20s sample
    // must be counted once, not twice.
    assert2::assert!(samples[0].value == SampleValue::Float(3.0));

    // A windowed sum must likewise see each timestamp once.
    let result = engine
        .query_instant("tenant-a", "sum_over_time(up[1m])", 30_000)
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected instant vector");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].value == SampleValue::Float(3.0));
}
