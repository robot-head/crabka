use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use crabka_blockstore::{Labels, MatchOp};
use crabka_metrics::{BucketSpan, NativeHistogram, ResetHint};

use super::*;
use crate::{EngineOpts, InMemoryMetricStore, PromqlEngine, QueryResult, RangeSeries, SampleValue};

fn labels(pairs: &[(&str, &str)]) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in pairs {
        labels.insert(*name, *value);
    }
    labels
}

/// Deterministic clock whose epoch-millis reading the test can advance, so
/// TTL expiry can be exercised without sleeping.
#[derive(Default)]
struct ManualClock {
    now_ms: std::sync::atomic::AtomicI64,
}

impl ManualClock {
    fn new(now_ms: i64) -> Self {
        Self {
            now_ms: std::sync::atomic::AtomicI64::new(now_ms),
        }
    }

    fn advance(&self, delta_ms: i64) {
        self.now_ms
            .fetch_add(delta_ms, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_epoch_millis(&self) -> i64 {
        self.now_ms.load(std::sync::atomic::Ordering::SeqCst)
    }
}

fn native_histogram_with_positive_buckets(
    count: f64,
    sum: f64,
    positive_spans: Vec<BucketSpan>,
    positive_counts: Vec<f64>,
) -> NativeHistogram {
    NativeHistogram {
        schema: 0,
        is_float: true,
        reset_hint: ResetHint::No,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count,
        sum,
        positive_spans,
        positive_counts,
        negative_spans: Vec::new(),
        negative_counts: Vec::new(),
        custom_values: None,
        start_timestamp_ms: None,
    }
}

#[test]
fn range_query_plan_splits_on_step_grid_without_duplicate_steps() {
    let plan = plan_range_query(
        "rate(http_requests_total[5m])",
        0,
        250_000,
        60_000,
        QueryFrontendOptions {
            split_interval_ms: 120_000,
            shard_count: 1,
        },
    )
    .unwrap();

    let ranges = plan
        .iter()
        .map(|subquery| (subquery.start_ms, subquery.end_ms, subquery.shard))
        .collect::<Vec<_>>();

    // Eval points 0, 60k, 120k, 180k, 240k bucket into absolute
    // split-interval windows [0,120k), [120k,240k), [240k,360k); each
    // sub-range spans the eval points landing in its absolute window with
    // no duplicate step across sub-ranges.
    assert_eq!(
        ranges,
        vec![
            (0, 60_000, None),
            (120_000, 180_000, None),
            (240_000, 240_000, None),
        ]
    );
}

#[test]
fn range_query_plan_rejects_resolution_over_point_cap() {
    // (end-start)/step = 20_000 / 1 = 20_000 > 11_000: the frontend planner
    // must reject before expanding into ~20k per-step sub-queries, matching
    // Prometheus's unconditional resolution front-gate.
    let error = plan_range_query(
        "up",
        0,
        20_000,
        1,
        QueryFrontendOptions {
            split_interval_ms: 120_000,
            shard_count: 1,
        },
    )
    .unwrap_err();

    match error {
        PromqlError::Plan(message) => assert_eq!(
            message,
            "exceeded maximum resolution of 11,000 points per timeseries. \
             Try decreasing the query resolution (?step=XX)"
        ),
        other => panic!("expected Plan error, got {other:?}"),
    }
}

#[test]
fn range_query_plan_allows_resolution_at_point_cap_boundary() {
    // (end-start)/step = 11_000 / 1 = 11_000 is allowed (cap is exclusive,
    // matching Prometheus's `> 11000`).
    let plan = plan_range_query(
        "up",
        0,
        11_000,
        1,
        QueryFrontendOptions {
            split_interval_ms: 120_000,
            shard_count: 1,
        },
    )
    .unwrap();
    assert!(!plan.is_empty());
}

#[test]
fn range_query_plan_aligns_subranges_to_absolute_split_grid() {
    // A window that does not start on a split-interval multiple still
    // produces sub-ranges whose interior boundaries sit on the absolute
    // grid (multiples of split_interval), not relative to start_ms.
    let plan = plan_range_query(
        "up",
        60_000,
        300_000,
        60_000,
        QueryFrontendOptions {
            split_interval_ms: 120_000,
            shard_count: 1,
        },
    )
    .unwrap();

    let ranges = plan
        .iter()
        .map(|subquery| (subquery.start_ms, subquery.end_ms))
        .collect::<Vec<_>>();

    // Eval points 60k | 120k,180k | 240k,300k bucket into [0,120k),
    // [120k,240k), [240k,360k).
    assert_eq!(
        ranges,
        vec![(60_000, 60_000), (120_000, 180_000), (240_000, 300_000)]
    );
}

#[tokio::test]
async fn moving_window_reuses_cached_subranges() {
    let opts = QueryFrontendOptions {
        split_interval_ms: 120_000,
        shard_count: 1,
    };
    let cache = QueryFrontendCache::default();
    let executor = RecordingExecutor::default();

    // First query window [0, 360_000].
    execute_range_query_frontend(
        &executor,
        &cache,
        &FrontendRangeRequest {
            tenant: "tenant-a".into(),
            query: "up".into(),
            start_ms: 0,
            end_ms: 360_000,
            step_ms: 60_000,
            opts,
        },
    )
    .await
    .unwrap();

    let first_fresh = executor
        .calls
        .lock()
        .expect("recording executor calls poisoned")
        .len();
    // Absolute buckets: [0,120k)->[0,60k], [120k,240k)->[120k,180k],
    // [240k,360k)->[240k,300k], [360k,480k)->[360k,360k] => 4 sub-queries.
    assert_eq!(first_fresh, 4);

    // Second window shifted by one step (60_000 < split 120_000) and the
    // same step phase, so the absolute-aligned interior buckets
    // [120k,240k) and [240k,360k) reproduce identical sub-ranges that are
    // already cached.
    execute_range_query_frontend(
        &executor,
        &cache,
        &FrontendRangeRequest {
            tenant: "tenant-a".into(),
            query: "up".into(),
            start_ms: 60_000,
            end_ms: 420_000,
            step_ms: 60_000,
            opts,
        },
    )
    .await
    .unwrap();

    let all_calls = executor
        .calls
        .lock()
        .expect("recording executor calls poisoned")
        .clone();
    let second_fresh = all_calls.len() - first_fresh;

    // Second window sub-ranges: [60k,60k] | [120k,180k]* | [240k,300k]* |
    // [360k,420k]. The two starred interior sub-ranges hit the cache, so
    // only the two non-cached sub-ranges execute fresh.
    assert_eq!(second_fresh, 2);
    let second_starts = all_calls[first_fresh..]
        .iter()
        .map(|query| (query.start_ms, query.end_ms))
        .collect::<Vec<_>>();
    assert_eq!(second_starts, vec![(60_000, 60_000), (360_000, 420_000)]);
}

#[test]
fn range_query_plan_expands_each_split_across_mimir_query_shards() {
    let plan = plan_range_query(
        "sum(rate(http_requests_total[5m]))",
        0,
        60_000,
        60_000,
        QueryFrontendOptions {
            split_interval_ms: 120_000,
            shard_count: 3,
        },
    )
    .unwrap();

    let shard_values = plan
        .iter()
        .map(|subquery| subquery.shard_matcher().expect("sharded subquery").value)
        .collect::<Vec<_>>();

    assert_eq!(shard_values, vec!["1_of_3", "2_of_3", "3_of_3"]);
    assert!(
        plan.iter()
            .all(|subquery| subquery.start_ms == 0 && subquery.end_ms == 60_000)
    );

    let matcher = plan[0].shard_matcher().expect("first shard matcher");
    assert_eq!(
        (matcher.name.as_str(), matcher.op),
        ("__query_shard__", MatchOp::Eq)
    );
}

#[test]
fn range_query_plan_shards_avg_for_partial_sum_count_reduction() {
    let plan = plan_range_query(
        "avg(up)",
        0,
        60_000,
        60_000,
        QueryFrontendOptions {
            split_interval_ms: 120_000,
            shard_count: 3,
        },
    )
    .unwrap();

    assert_eq!(
        (
            plan.len(),
            plan.iter().all(|subquery| subquery.shard.is_some())
        ),
        (3, true)
    );
}

#[test]
fn range_query_plan_shards_stddev_and_stdvar_for_moment_reduction() {
    for query in ["stddev(up)", "stdvar(up)"] {
        let plan = plan_range_query(
            query,
            0,
            60_000,
            60_000,
            QueryFrontendOptions {
                split_interval_ms: 120_000,
                shard_count: 3,
            },
        )
        .unwrap();

        assert_eq!(
            (
                plan.len(),
                plan.iter().all(|subquery| subquery.shard.is_some())
            ),
            (3, true),
            "{query}"
        );
    }
}

#[test]
fn range_query_plan_skips_shards_for_unsupported_aggregate_reducers() {
    let plan = plan_range_query(
        "quantile(0.9, up)",
        0,
        60_000,
        60_000,
        QueryFrontendOptions {
            split_interval_ms: 120_000,
            shard_count: 3,
        },
    )
    .unwrap();

    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].shard, None);
}

#[test]
fn range_query_plan_skips_nested_avg_until_rewrite_is_aggregate_aware() {
    let plan = plan_range_query(
        "avg(sum by (job)(up))",
        0,
        60_000,
        60_000,
        QueryFrontendOptions {
            split_interval_ms: 120_000,
            shard_count: 3,
        },
    )
    .unwrap();

    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].shard, None);
}

#[test]
fn range_query_plan_shards_min_and_max_aggregate_reducers() {
    for query in ["min(up)", "max(up)"] {
        let plan = plan_range_query(
            query,
            0,
            60_000,
            60_000,
            QueryFrontendOptions {
                split_interval_ms: 120_000,
                shard_count: 3,
            },
        )
        .unwrap();

        assert_eq!(
            (
                plan.len(),
                plan.iter().all(|subquery| subquery.shard.is_some())
            ),
            (3, true),
            "{query}"
        );
    }
}

#[test]
fn range_query_plan_shards_group_aggregate_reducer() {
    let plan = plan_range_query(
        "group(up)",
        0,
        60_000,
        60_000,
        QueryFrontendOptions {
            split_interval_ms: 120_000,
            shard_count: 3,
        },
    )
    .unwrap();

    assert_eq!(
        (
            plan.len(),
            plan.iter().all(|subquery| subquery.shard.is_some())
        ),
        (3, true)
    );
}

#[test]
fn range_query_plan_shards_topk_and_bottomk_for_final_rank_reduction() {
    for query in ["topk(2, up)", "bottomk(2, up)"] {
        let plan = plan_range_query(
            query,
            0,
            60_000,
            60_000,
            QueryFrontendOptions {
                split_interval_ms: 120_000,
                shard_count: 3,
            },
        )
        .unwrap();

        assert_eq!(
            (
                plan.len(),
                plan.iter().all(|subquery| subquery.shard.is_some())
            ),
            (3, true),
            "{query}"
        );
    }
}

#[test]
fn shard_query_injection_adds_mimir_selector_to_vector_and_matrix_selectors() {
    let rewritten = query_with_shard_selector(
        r#"sum(rate(http_requests_total{job="api"}[5m])) + up"#,
        QueryShard { index: 1, total: 2 },
    )
    .unwrap();

    for needle in [
        r#"__query_shard__="1_of_2""#,
        r#"job="api""#,
        "http_requests_total",
        r#"up{__query_shard__="1_of_2"}"#,
    ] {
        assert!(
            rewritten.contains(needle),
            "missing {needle} in {rewritten}"
        );
    }
}

#[test]
fn range_query_merge_combines_time_split_samples_for_same_series() {
    let api_labels = labels(&[("__name__", "up"), ("job", "api")]);
    let worker_labels = labels(&[("__name__", "up"), ("job", "worker")]);
    let result = merge_range_query_results(vec![
        QueryResult::RangeMatrix(vec![
            RangeSeries {
                labels: api_labels.clone(),
                samples: vec![(60_000, SampleValue::Float(2.0))],
            },
            RangeSeries {
                labels: worker_labels.clone(),
                samples: vec![(0, SampleValue::Float(3.0))],
            },
        ]),
        QueryResult::RangeMatrix(vec![RangeSeries {
            labels: api_labels.clone(),
            samples: vec![
                (0, SampleValue::Float(1.0)),
                (120_000, SampleValue::Float(4.0)),
            ],
        }]),
    ])
    .unwrap();

    assert_eq!(
        result,
        QueryResult::RangeMatrix(vec![
            RangeSeries {
                labels: api_labels,
                samples: vec![
                    (0, SampleValue::Float(1.0)),
                    (60_000, SampleValue::Float(2.0)),
                    (120_000, SampleValue::Float(4.0)),
                ],
            },
            RangeSeries {
                labels: worker_labels,
                samples: vec![(0, SampleValue::Float(3.0))],
            },
        ])
    );
}

#[test]
fn range_query_merge_sums_sharded_partial_float_samples_for_same_series() {
    let labels = labels(&[]);
    let result = merge_range_query_results(vec![
        QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels.clone(),
            samples: vec![
                (0, SampleValue::Float(1.0)),
                (60_000, SampleValue::Float(2.0)),
            ],
        }]),
        QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels.clone(),
            samples: vec![
                (0, SampleValue::Float(10.0)),
                (60_000, SampleValue::Float(20.0)),
            ],
        }]),
    ])
    .unwrap();

    assert_eq!(
        result,
        QueryResult::RangeMatrix(vec![RangeSeries {
            labels,
            samples: vec![
                (0, SampleValue::Float(11.0)),
                (60_000, SampleValue::Float(22.0)),
            ],
        }])
    );
}

#[test]
#[allow(clippy::float_cmp)]
fn range_query_merge_sums_native_histograms_with_different_span_layouts() {
    let labels = labels(&[]);
    let result = merge_range_query_results(vec![
        QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels.clone(),
            samples: vec![(
                0,
                SampleValue::Histogram(native_histogram_with_positive_buckets(
                    3.0,
                    9.0,
                    vec![BucketSpan {
                        offset: 0,
                        length: 2,
                    }],
                    vec![1.0, 2.0],
                )),
            )],
        }]),
        QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels.clone(),
            samples: vec![(
                0,
                SampleValue::Histogram(native_histogram_with_positive_buckets(
                    7.0,
                    21.0,
                    vec![BucketSpan {
                        offset: 1,
                        length: 2,
                    }],
                    vec![3.0, 4.0],
                )),
            )],
        }]),
    ])
    .unwrap();

    let QueryResult::RangeMatrix(series) = result else {
        panic!("expected range matrix");
    };
    assert_eq!((series.len(), &series[0].labels), (1, &labels));
    let SampleValue::Histogram(histogram) = &series[0].samples[0].1 else {
        panic!("expected histogram sample");
    };
    assert_eq!(
        (
            histogram.count,
            histogram.sum,
            &histogram.positive_spans,
            &histogram.positive_counts,
        ),
        (
            10.0,
            30.0,
            &vec![BucketSpan {
                offset: 0,
                length: 3,
            }],
            &vec![1.0, 5.0, 4.0],
        )
    );
}

#[test]
fn range_query_merge_rejects_non_matrix_subquery_results() {
    let err = merge_range_query_results(vec![QueryResult::Scalar {
        ts_ms: 0,
        value: 1.0,
    }])
    .unwrap_err();

    assert!(format!("{err}").contains("range matrix"));
}

#[test]
fn range_result_cache_is_scoped_by_tenant_query_range_step_and_shard() {
    let cache = QueryFrontendCache::default();
    let query = FrontendRangeQuery {
        query: "up".into(),
        start_ms: 0,
        end_ms: 60_000,
        step_ms: 60_000,
        shard: Some(QueryShard { index: 1, total: 2 }),
    };
    let result = QueryResult::RangeMatrix(vec![RangeSeries {
        labels: labels(&[("__name__", "up"), ("job", "api")]),
        samples: vec![(0, SampleValue::Float(1.0))],
    }]);

    cache.insert("tenant-a", &query, result.clone());

    assert_eq!(
        (cache.get("tenant-a", &query), cache.get("tenant-b", &query),),
        (Some(result), None)
    );

    let other_shard = FrontendRangeQuery {
        shard: Some(QueryShard { index: 2, total: 2 }),
        ..query
    };
    assert_eq!(cache.get("tenant-a", &other_shard), None);
}

#[test]
fn range_result_cache_returns_owned_results() {
    let cache = QueryFrontendCache::default();
    let query = FrontendRangeQuery {
        query: "up".into(),
        start_ms: 0,
        end_ms: 0,
        step_ms: 60_000,
        shard: None,
    };
    let result = QueryResult::RangeMatrix(vec![RangeSeries {
        labels: labels(&[("__name__", "up")]),
        samples: vec![(0, SampleValue::Float(1.0))],
    }]);

    cache.insert("tenant-a", &query, result);
    let Some(QueryResult::RangeMatrix(mut first_hit)) = cache.get("tenant-a", &query) else {
        panic!("cached range matrix");
    };
    first_hit[0].samples.clear();

    let Some(QueryResult::RangeMatrix(second_hit)) = cache.get("tenant-a", &query) else {
        panic!("cached range matrix");
    };
    assert_eq!(second_hit[0].samples, vec![(0, SampleValue::Float(1.0))]);
}

#[test]
fn in_memory_round_trips_then_expires() {
    let clock = Arc::new(ManualClock::new(1_000_000));
    let cache =
        QueryFrontendCache::with_ttl(std::time::Duration::from_secs(90)).with_clock(clock.clone());
    let query = FrontendRangeQuery {
        query: "up".into(),
        start_ms: 0,
        end_ms: 0,
        step_ms: 60_000,
        shard: None,
    };
    let result = QueryResult::RangeMatrix(vec![RangeSeries {
        labels: labels(&[("__name__", "up")]),
        samples: vec![(0, SampleValue::Float(1.0))],
    }]);

    cache.insert("tenant-a", &query, result.clone());

    // Within the TTL window: hit.
    clock.advance(89_000);
    assert_eq!(cache.get("tenant-a", &query), Some(result));

    // One step past the TTL: miss, and the entry is evicted.
    clock.advance(2_000);
    assert_eq!(
        (
            cache.get("tenant-a", &query),
            cache
                .range_results
                .lock()
                .expect("query frontend cache poisoned")
                .len(),
        ),
        (None, 0),
        "expired entry must be evicted on miss"
    );
}

#[test]
fn in_memory_without_ttl_never_expires() {
    let clock = Arc::new(ManualClock::new(0));
    let cache = QueryFrontendCache::default().with_clock(clock.clone());
    let query = FrontendRangeQuery {
        query: "up".into(),
        start_ms: 0,
        end_ms: 0,
        step_ms: 60_000,
        shard: None,
    };
    let result = QueryResult::RangeMatrix(vec![RangeSeries {
        labels: labels(&[("__name__", "up")]),
        samples: vec![(0, SampleValue::Float(1.0))],
    }]);

    cache.insert("tenant-a", &query, result.clone());
    clock.advance(i64::from(u32::MAX));
    assert_eq!(cache.get("tenant-a", &query), Some(result));
}

#[tokio::test]
async fn object_store_range_result_cache_expires_stale_objects() {
    let object_store = std::sync::Arc::new(object_store::memory::InMemory::new());
    let clock = Arc::new(ManualClock::new(5_000_000));
    let cache = ObjectStoreQueryFrontendCache::new(object_store, "query-cache".to_string())
        .with_ttl(std::time::Duration::from_secs(30))
        .with_clock(clock.clone());
    let query = FrontendRangeQuery {
        query: "up".into(),
        start_ms: 0,
        end_ms: 0,
        step_ms: 60_000,
        shard: None,
    };
    let result = QueryResult::RangeMatrix(vec![RangeSeries {
        labels: labels(&[("__name__", "up")]),
        samples: vec![(0, SampleValue::Float(1.0))],
    }]);

    cache
        .insert("tenant-a", &query, result.clone())
        .await
        .unwrap();

    // Within TTL: hit.
    clock.advance(29_000);
    assert_eq!(cache.get("tenant-a", &query).await.unwrap(), Some(result));

    // Past TTL: miss.
    clock.advance(2_000);
    assert_eq!(cache.get("tenant-a", &query).await.unwrap(), None);
}

#[tokio::test]
async fn object_store_range_result_cache_persists_across_instances() {
    let object_store = std::sync::Arc::new(object_store::memory::InMemory::new());
    let first = ObjectStoreQueryFrontendCache::new(object_store.clone(), "query-cache".to_string());
    let second = ObjectStoreQueryFrontendCache::new(object_store, "query-cache".to_string());
    let query = FrontendRangeQuery {
        query: "up".into(),
        start_ms: 0,
        end_ms: 0,
        step_ms: 60_000,
        shard: Some(QueryShard { index: 1, total: 2 }),
    };
    let result = QueryResult::RangeMatrix(vec![RangeSeries {
        labels: labels(&[("__name__", "up"), ("job", "api")]),
        samples: vec![(0, SampleValue::Float(1.0))],
    }]);

    first
        .insert("tenant-a", &query, result.clone())
        .await
        .unwrap();

    assert_eq!(second.get("tenant-a", &query).await.unwrap(), Some(result));
}

#[derive(Default)]
struct RecordingExecutor {
    calls: Mutex<Vec<FrontendRangeQuery>>,
}

#[async_trait]
impl RangeQueryExecutor for RecordingExecutor {
    async fn execute_range_query(
        &self,
        _tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<QueryResult, PromqlError> {
        self.calls
            .lock()
            .expect("recording executor calls poisoned")
            .push(query.clone());
        Ok(QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[("__name__", "up"), ("job", "api")]),
            samples: vec![(query.start_ms, SampleValue::Float(120_000.0))],
        }]))
    }
}

#[derive(Default)]
struct AvgPartialRecordingExecutor {
    calls: Mutex<Vec<FrontendRangeQuery>>,
}

#[async_trait]
impl RangeQueryExecutor for AvgPartialRecordingExecutor {
    #[allow(clippy::match_same_arms)]
    async fn execute_range_query(
        &self,
        _tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<QueryResult, PromqlError> {
        self.calls
            .lock()
            .expect("avg partial executor calls poisoned")
            .push(query.clone());

        let shard = query.shard.expect("avg partial query shard");
        let value = match (query.query.as_str(), shard.index) {
            ("sum(up)", 1) => 2.0,
            ("sum(up)", 2) => 10.0,
            ("count(up)", 1) => 1.0,
            ("count(up)", 2) => 2.0,
            _ => {
                return Err(PromqlError::Plan(format!(
                    "unexpected avg partial query: {query:?}"
                )));
            }
        };
        Ok(QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[]),
            samples: vec![(query.start_ms, SampleValue::Float(value))],
        }]))
    }
}

#[derive(Default)]
struct MomentPartialRecordingExecutor {
    calls: Mutex<Vec<FrontendRangeQuery>>,
}

#[async_trait]
impl RangeQueryExecutor for MomentPartialRecordingExecutor {
    async fn execute_range_query(
        &self,
        _tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<QueryResult, PromqlError> {
        self.calls
            .lock()
            .expect("moment partial executor calls poisoned")
            .push(query.clone());

        let shard = query.shard.expect("moment partial query shard");
        let value = match (query.query.as_str(), shard.index) {
            ("sum(up)", 1) => 12.0,
            ("sum(up)", 2) => 3.0,
            ("count(up)", 1) => 2.0,
            ("count(up)", 2) => 1.0,
            ("sum((up) * (up))", 1) => 104.0,
            ("sum((up) * (up))", 2) => 9.0,
            _ => {
                return Err(PromqlError::Plan(format!(
                    "unexpected moment partial query: {query:?}"
                )));
            }
        };
        Ok(QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[]),
            samples: vec![(query.start_ms, SampleValue::Float(value))],
        }]))
    }
}

#[derive(Default)]
struct RankRecordingExecutor {
    calls: Mutex<Vec<FrontendRangeQuery>>,
}

#[async_trait]
impl RangeQueryExecutor for RankRecordingExecutor {
    async fn execute_range_query(
        &self,
        _tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<QueryResult, PromqlError> {
        self.calls
            .lock()
            .expect("rank executor calls poisoned")
            .push(query.clone());

        let shard = query.shard.expect("rank query shard");
        let series = match shard.index {
            1 => vec![
                RangeSeries {
                    labels: labels(&[("__name__", "up"), ("series", "a")]),
                    samples: vec![(0, SampleValue::Float(10.0))],
                },
                RangeSeries {
                    labels: labels(&[("__name__", "up"), ("series", "b")]),
                    samples: vec![(0, SampleValue::Float(2.0))],
                },
            ],
            2 => vec![
                RangeSeries {
                    labels: labels(&[("__name__", "up"), ("series", "c")]),
                    samples: vec![(0, SampleValue::Float(9.0))],
                },
                RangeSeries {
                    labels: labels(&[("__name__", "up"), ("series", "d")]),
                    samples: vec![(0, SampleValue::Float(8.0))],
                },
            ],
            _ => Vec::new(),
        };
        Ok(QueryResult::RangeMatrix(series))
    }
}

#[tokio::test]
async fn frontend_range_execution_reduces_sharded_topk_from_rank_candidates() {
    let cache = QueryFrontendCache::default();
    let executor = RankRecordingExecutor::default();

    let result = execute_range_query_frontend(
        &executor,
        &cache,
        &FrontendRangeRequest {
            tenant: "tenant-a".into(),
            query: "topk(2, up)".into(),
            start_ms: 0,
            end_ms: 0,
            step_ms: 60_000,
            opts: QueryFrontendOptions {
                split_interval_ms: 60_000,
                shard_count: 2,
            },
        },
    )
    .await
    .unwrap();

    let calls = executor
        .calls
        .lock()
        .expect("rank executor calls poisoned")
        .clone();
    assert_eq!(
        calls
            .iter()
            .map(|query| (query.query.as_str(), query.shard))
            .collect::<Vec<_>>(),
        vec![
            ("topk(2, up)", Some(QueryShard { index: 1, total: 2 })),
            ("topk(2, up)", Some(QueryShard { index: 2, total: 2 })),
        ]
    );
    let QueryResult::RangeMatrix(series) = result else {
        panic!("topk range matrix");
    };
    let selected = series
        .iter()
        .map(|series| {
            let SampleValue::Float(value) = series.samples[0].1 else {
                panic!("topk float sample");
            };
            (series.labels.get("series").unwrap().to_string(), value)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        selected,
        vec![("a".to_string(), 10.0), ("c".to_string(), 9.0)]
    );
}

#[tokio::test]
async fn frontend_range_execution_reduces_sharded_stdvar_from_moment_partials() {
    let cache = QueryFrontendCache::default();
    let executor = MomentPartialRecordingExecutor::default();

    let result = execute_range_query_frontend(
        &executor,
        &cache,
        &FrontendRangeRequest {
            tenant: "tenant-a".into(),
            query: "stdvar(up)".into(),
            start_ms: 0,
            end_ms: 0,
            step_ms: 60_000,
            opts: QueryFrontendOptions {
                split_interval_ms: 60_000,
                shard_count: 2,
            },
        },
    )
    .await
    .unwrap();

    let calls = executor
        .calls
        .lock()
        .expect("moment partial executor calls poisoned")
        .clone();
    assert_eq!(
        calls
            .iter()
            .map(|query| (query.query.as_str(), query.shard))
            .collect::<Vec<_>>(),
        vec![
            ("sum(up)", Some(QueryShard { index: 1, total: 2 })),
            ("sum(up)", Some(QueryShard { index: 2, total: 2 })),
            ("count(up)", Some(QueryShard { index: 1, total: 2 })),
            ("count(up)", Some(QueryShard { index: 2, total: 2 })),
            ("sum((up) * (up))", Some(QueryShard { index: 1, total: 2 }),),
            ("sum((up) * (up))", Some(QueryShard { index: 2, total: 2 }),),
        ]
    );
    let QueryResult::RangeMatrix(series) = result else {
        panic!("stdvar range matrix");
    };
    let SampleValue::Float(value) = series[0].samples[0].1 else {
        panic!("stdvar float sample");
    };
    assert!((value - (38.0 / 3.0)).abs() < 1e-9);
}

#[tokio::test]
async fn frontend_range_execution_reduces_sharded_stddev_from_moment_partials() {
    let cache = QueryFrontendCache::default();
    let executor = MomentPartialRecordingExecutor::default();

    let result = execute_range_query_frontend(
        &executor,
        &cache,
        &FrontendRangeRequest {
            tenant: "tenant-a".into(),
            query: "stddev(up)".into(),
            start_ms: 0,
            end_ms: 0,
            step_ms: 60_000,
            opts: QueryFrontendOptions {
                split_interval_ms: 60_000,
                shard_count: 2,
            },
        },
    )
    .await
    .unwrap();

    let QueryResult::RangeMatrix(series) = result else {
        panic!("stddev range matrix");
    };
    let SampleValue::Float(value) = series[0].samples[0].1 else {
        panic!("stddev float sample");
    };
    assert!((value - (38.0_f64 / 3.0).sqrt()).abs() < 1e-9);
}

#[tokio::test]
async fn frontend_range_execution_reduces_sharded_avg_from_sum_and_count_partials() {
    let cache = QueryFrontendCache::default();
    let executor = AvgPartialRecordingExecutor::default();

    let result = execute_range_query_frontend(
        &executor,
        &cache,
        &FrontendRangeRequest {
            tenant: "tenant-a".into(),
            query: "avg(up)".into(),
            start_ms: 0,
            end_ms: 0,
            step_ms: 60_000,
            opts: QueryFrontendOptions {
                split_interval_ms: 60_000,
                shard_count: 2,
            },
        },
    )
    .await
    .unwrap();

    let calls = executor
        .calls
        .lock()
        .expect("avg partial executor calls poisoned")
        .clone();
    assert_eq!(
        calls
            .iter()
            .map(|query| (query.query.as_str(), query.shard))
            .collect::<Vec<_>>(),
        vec![
            ("sum(up)", Some(QueryShard { index: 1, total: 2 })),
            ("sum(up)", Some(QueryShard { index: 2, total: 2 })),
            ("count(up)", Some(QueryShard { index: 1, total: 2 })),
            ("count(up)", Some(QueryShard { index: 2, total: 2 })),
        ]
    );
    assert_eq!(
        result,
        QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[]),
            samples: vec![(0, SampleValue::Float(4.0))],
        }])
    );
}

#[tokio::test]
async fn frontend_range_execution_uses_cache_and_merges_subquery_results() {
    let cache = QueryFrontendCache::default();
    let executor = RecordingExecutor::default();
    let cached_query = FrontendRangeQuery {
        query: "up".into(),
        start_ms: 0,
        end_ms: 60_000,
        step_ms: 60_000,
        shard: None,
    };
    cache.insert(
        "tenant-a",
        &cached_query,
        QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[("__name__", "up"), ("job", "api")]),
            samples: vec![(0, SampleValue::Float(1.0))],
        }]),
    );

    let result = execute_range_query_frontend(
        &executor,
        &cache,
        &FrontendRangeRequest {
            tenant: "tenant-a".into(),
            query: "up".into(),
            start_ms: 0,
            end_ms: 180_000,
            step_ms: 60_000,
            opts: QueryFrontendOptions {
                split_interval_ms: 120_000,
                shard_count: 1,
            },
        },
    )
    .await
    .unwrap();

    // Absolute windows [0,120k)->[0,60k] (pre-cached) and
    // [120k,240k)->[120k,180k] (executed fresh).
    let calls = executor
        .calls
        .lock()
        .expect("recording executor calls poisoned")
        .clone();
    assert_eq!(
        calls
            .iter()
            .map(|query| (query.start_ms, query.end_ms))
            .collect::<Vec<_>>(),
        vec![(120_000, 180_000)]
    );
    assert_eq!(
        cache
            .get("tenant-a", &calls[0])
            .expect("fresh subquery cached"),
        QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[("__name__", "up"), ("job", "api")]),
            samples: vec![(120_000, SampleValue::Float(120_000.0))],
        }])
    );
    assert_eq!(
        result,
        QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[("__name__", "up"), ("job", "api")]),
            samples: vec![
                (0, SampleValue::Float(1.0)),
                (120_000, SampleValue::Float(120_000.0)),
            ],
        }])
    );
}

/// Executor that blocks every sub-query on a shared barrier sized to the
/// expected fan-out width. A sequential dispatcher can never satisfy the
/// barrier (only one sub-query is ever in flight), so the surrounding
/// `tokio::time::timeout` trips; a concurrent dispatcher releases all N at
/// once. The executor also records the wall-clock order in which sub-queries
/// were admitted to prove every planned sub-query was dispatched.
struct ConcurrencyProbeExecutor {
    barrier: tokio::sync::Barrier,
    calls: Mutex<Vec<FrontendRangeQuery>>,
}

impl ConcurrencyProbeExecutor {
    fn new(width: usize) -> Self {
        Self {
            barrier: tokio::sync::Barrier::new(width),
            calls: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl RangeQueryExecutor for ConcurrencyProbeExecutor {
    async fn execute_range_query(
        &self,
        _tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<QueryResult, PromqlError> {
        self.calls
            .lock()
            .expect("probe executor calls poisoned")
            .push(query.clone());
        // All concurrently-dispatched sub-queries must reach here before any
        // can proceed. Under sequential dispatch this never completes.
        self.barrier.wait().await;
        // Each sub-query contributes a sample at a distinct timestamp
        // (its split start), so the stitched matrix is order-independent.
        Ok(QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[("__name__", "up"), ("job", "api")]),
            samples: vec![(query.start_ms, SampleValue::Float(1.0))],
        }]))
    }
}

#[tokio::test]
async fn frontend_range_execution_dispatches_subqueries_concurrently() {
    // 4 splits over [0, 720_000] with a 180_000 split interval and 60_000
    // step, times 1 shard => 4 independent sub-queries.
    let planned = plan_range_query(
        "up",
        0,
        720_000,
        60_000,
        QueryFrontendOptions {
            split_interval_ms: 180_000,
            shard_count: 1,
        },
    )
    .unwrap();
    let width = planned.len();
    assert!(width >= 2, "test needs multiple sub-queries, got {width}");

    let executor = ConcurrencyProbeExecutor::new(width);
    let cache = QueryFrontendCache::default();

    let results = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        execute_planned_range_queries(&executor, &cache, "tenant-a", planned.clone()),
    )
    .await
    .expect("parallel fan-out must not block on the per-subquery barrier")
    .unwrap();

    // Every planned sub-query was dispatched exactly once.
    let mut dispatched = executor
        .calls
        .lock()
        .expect("probe executor calls poisoned")
        .clone();
    dispatched.sort_by_key(|query| query.start_ms);
    let mut expected = planned.clone();
    expected.sort_by_key(|query| query.start_ms);
    assert_eq!(dispatched, expected);

    // Stitched result is identical to a deterministic sequential merge,
    // independent of completion order.
    let stitched =
        merge_range_query_results_with_reducer(results.clone(), QueryShardReducer::First).unwrap();
    let mut sequential = Vec::new();
    for subquery in &planned {
        sequential.push(QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[("__name__", "up"), ("job", "api")]),
            samples: vec![(subquery.start_ms, SampleValue::Float(1.0))],
        }]));
    }
    let sequential_merge =
        merge_range_query_results_with_reducer(sequential, QueryShardReducer::First).unwrap();
    assert_eq!(stitched, sequential_merge);
}

#[tokio::test]
async fn frontend_range_execution_runs_against_promql_engine() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        0,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        60_000,
        2.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        120_000,
        3.0,
    );
    let engine = PromqlEngine::new(std::sync::Arc::new(store), EngineOpts::default());
    let cache = QueryFrontendCache::default();

    let result = execute_range_query_frontend(
        &engine,
        &cache,
        &FrontendRangeRequest {
            tenant: "tenant-a".into(),
            query: "up".into(),
            start_ms: 0,
            end_ms: 120_000,
            step_ms: 60_000,
            opts: QueryFrontendOptions {
                split_interval_ms: 60_000,
                shard_count: 1,
            },
        },
    )
    .await
    .unwrap();

    assert_eq!(
        result,
        QueryResult::RangeMatrix(vec![RangeSeries {
            labels: labels(&[("__name__", "up"), ("job", "api")]),
            samples: vec![
                (0, SampleValue::Float(1.0)),
                (60_000, SampleValue::Float(2.0)),
                (120_000, SampleValue::Float(3.0)),
            ],
        }])
    );
}
