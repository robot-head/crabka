use std::{collections::BTreeMap, sync::Arc};

use assert2::{assert, check};
use crabka_blockstore::Labels;
use crabka_metrics::{BucketSpan, NativeHistogram, ResetHint};

use super::{MAX_RESOLUTION_POINTS, check_resolution_points, match_rate_range_call};
use crate::{EngineOpts, InMemoryMetricStore, PromqlEngine, PromqlError, QueryResult, SampleValue};

fn labels(pairs: &[(&str, &str)]) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in pairs {
        labels.insert(*name, *value);
    }
    labels
}

fn float_value(value: &SampleValue) -> f64 {
    match value {
        SampleValue::Float(value) => *value,
        SampleValue::Histogram(_) => panic!("expected float sample"),
    }
}

fn assert_single_float_sample(result: &QueryResult, job: &str, expected: f64, context: &str) {
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector for {context}");
    };
    assert_eq!(samples.len(), 1, "{context}");
    assert_eq!(samples[0].labels.get("__name__"), None, "{context}");
    assert_eq!(samples[0].labels.get("job"), Some(job), "{context}");
    assert!(
        approx_eq(float_value(&samples[0].value), expected),
        "{context}"
    );
}

fn assert_single_on_x_float_sample(result: &QueryResult, expected: f64, context: &str) {
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector for {context}");
    };
    assert_eq!(samples.len(), 1, "{context}");
    assert_eq!(samples[0].labels.get("__name__"), None, "{context}");
    assert_eq!(samples[0].labels.get("job"), None, "{context}");
    assert_eq!(samples[0].labels.get("x"), Some("1"), "{context}");
    assert!(
        approx_eq(float_value(&samples[0].value), expected),
        "{context}"
    );
}

#[cfg(feature = "experimental-functions")]
fn sample_instances(samples: &[crate::InstantSample]) -> Vec<&str> {
    let mut instances = samples
        .iter()
        .map(|sample| sample.labels.get("instance").expect("instance label"))
        .collect::<Vec<_>>();
    instances.sort_unstable();
    instances
}

fn approx_eq(left: f64, right: f64) -> bool {
    if !left.is_finite() || !right.is_finite() {
        return left == right;
    }
    // Relative tolerance: an absolute `f64::EPSILON` bound is too tight for
    // magnitudes above ~1 — a Kahan/Welford-compensated fold (matching
    // Prometheus) rounds in the last ULP, e.g. a population variance of 4.0
    // lands at 3.999999999999_9996. Scale the bound by operand magnitude.
    let scale = left.abs().max(right.abs()).max(1.0);
    (left - right).abs() <= f64::EPSILON * 4.0 * scale
}

fn stale_nan() -> f64 {
    f64::from_bits(0x7ff0_0000_0000_0002)
}

/// Compare two sorted instant-sample vectors for the parity tests, treating
/// floats bit-exactly (so a genuine NaN equals a genuine NaN). `PartialEq`
/// on `SampleValue::Float` uses IEEE `==`, under which `NaN != NaN`; a plain
/// `assert_eq!` would therefore spuriously fail whenever a path correctly
/// preserves a genuine NaN value. Stale-NaN markers are not expected to
/// survive selection on either path, so they never reach this comparison.
fn instant_samples_match(left: &[crate::InstantSample], right: &[crate::InstantSample]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter().zip(right.iter()).all(|(l, r)| {
        l.labels == r.labels
            && l.ts_ms == r.ts_ms
            && match (&l.value, &r.value) {
                (SampleValue::Float(a), SampleValue::Float(b)) => a.to_bits() == b.to_bits(),
                (other_l, other_r) => other_l == other_r,
            }
    })
}

/// Assert the post-fix staleness semantics for the aggregate parity test's
/// `nan_metric` queries: `sum` keeps the genuine NaN (NaN value, not a stale
/// marker), and `count` drops the stale-NaN marker before counting (2, not 3).
fn assert_aggregate_nan_staleness(query: &str, via_operators: &[crate::InstantSample]) {
    if query == "sum(nan_metric)" {
        assert_eq!(via_operators.len(), 1, "sum(nan_metric) row missing");
        let value = float_value(&via_operators[0].value);
        assert!(value.is_nan(), "genuine NaN not kept through sum: {value}");
        assert!(
            !super::is_stale_nan(value),
            "aggregate value is a stale marker"
        );
    }
    if query == "count(nan_metric)" {
        assert_eq!(via_operators.len(), 1, "count(nan_metric) row missing");
        let value = float_value(&via_operators[0].value);
        assert!(
            approx_eq(value, 2.0),
            "stale marker not dropped before count: got {value}, want 2"
        );
    }
}

/// Assert the NaN-ignoring `min`/`max` aggregation rule for the parity
/// test's `minmax_nan` queries on absolute values: the mixed group's
/// extremum is taken over its non-NaN samples (min=1, max=4), and the
/// all-NaN group is kept with a NaN result (the series is not dropped).
fn assert_minmax_nan_ignoring(query: &str, via_operators: &[crate::InstantSample]) {
    // Look up a group's value by its `g` label.
    let by_group = |g: &str| -> f64 {
        let sample = via_operators
            .iter()
            .find(|sample| sample.labels.get("g") == Some(g))
            .unwrap_or_else(|| panic!("`{query}`: group g={g} missing"));
        float_value(&sample.value)
    };
    match query {
        "min by (g) (minmax_nan)" => {
            assert_eq!(via_operators.len(), 2, "`{query}`: expected mixed+allnan");
            let mixed = by_group("mixed");
            assert!(
                approx_eq(mixed, 1.0),
                "`{query}`: mixed min over non-NaN: {mixed}"
            );
            let allnan = by_group("allnan");
            assert!(allnan.is_nan(), "`{query}`: all-NaN min not NaN: {allnan}");
        }
        "max by (g) (minmax_nan)" => {
            assert_eq!(via_operators.len(), 2, "`{query}`: expected mixed+allnan");
            let mixed = by_group("mixed");
            assert!(
                approx_eq(mixed, 4.0),
                "`{query}`: mixed max over non-NaN: {mixed}"
            );
            let allnan = by_group("allnan");
            assert!(allnan.is_nan(), "`{query}`: all-NaN max not NaN: {allnan}");
        }
        // `min`/`max` with no grouping fold both groups together: the global
        // extremum is over the only non-NaN samples (the mixed group's
        // {4, 1}), so min=1 and max=4 (the all-NaN group is ignored, but its
        // presence does not force a NaN because the mixed group has finite
        // values).
        "min(minmax_nan)" => {
            assert_eq!(via_operators.len(), 1, "`{query}`: expected one group");
            let value = float_value(&via_operators[0].value);
            assert!(
                approx_eq(value, 1.0),
                "`{query}`: global min over non-NaN: {value}"
            );
        }
        "max(minmax_nan)" => {
            assert_eq!(via_operators.len(), 1, "`{query}`: expected one group");
            let value = float_value(&via_operators[0].value);
            assert!(
                approx_eq(value, 4.0),
                "`{query}`: global max over non-NaN: {value}"
            );
        }
        _ => {}
    }
}

/// Assert the SPARSE aggregate-over-rate rule for the parity test's
/// `sparse_total` queries on absolute values: the no-value (sparse) series is
/// excluded from its group, the `g="mix"` group survives with only its dense
/// member's contribution, and the all-no-value `g="allsparse"` group produces
/// no result row at all (the series is absent, not present-with-NaN).
fn assert_sparse_aggregate_excludes_no_value(query: &str, via_operators: &[crate::InstantSample]) {
    let group_value = |g: &str| -> Option<f64> {
        via_operators
            .iter()
            .find(|sample| sample.labels.get("g") == Some(g))
            .map(|sample| float_value(&sample.value))
    };
    match query {
        // g="mix" survives (its dense member has a rate); g="allsparse" has
        // no value-bearing member, so it is absent. Only one row total.
        "sum by (g) (rate(sparse_total[2m]))"
        | "avg by (g) (rate(sparse_total[2m]))"
        | "min by (g) (rate(sparse_total[2m]))"
        | "max by (g) (rate(sparse_total[2m]))"
        | "count by (g) (rate(sparse_total[2m]))"
        | "group by (g) (rate(sparse_total[2m]))" => {
            assert_eq!(
                via_operators.len(),
                1,
                "`{query}`: only g=mix survives (g=allsparse is absent)"
            );
            assert!(group_value("mix").is_some(), "`{query}`: g=mix row missing");
            assert!(
                group_value("allsparse").is_none(),
                "`{query}`: g=allsparse must be absent (all members no-value)"
            );
            // `count`/`group` over g=mix see exactly the one dense member.
            if query == "count by (g) (rate(sparse_total[2m]))" {
                assert!(
                    approx_eq(group_value("mix").unwrap(), 1.0),
                    "`{query}`: count over g=mix must be 1 (sparse member excluded)"
                );
            }
            if query == "group by (g) (rate(sparse_total[2m]))" {
                assert!(
                    approx_eq(group_value("mix").unwrap(), 1.0),
                    "`{query}`: group=1"
                );
            }
        }
        // No grouping: the global aggregate is over the single dense rate.
        "count (rate(sparse_total[2m]))" => {
            assert_eq!(via_operators.len(), 1, "`{query}`: one global row");
            assert!(
                approx_eq(float_value(&via_operators[0].value), 1.0),
                "`{query}`: global count must be 1 (only the dense rate)"
            );
        }
        "sum (rate(sparse_total[2m]))" => {
            assert_eq!(via_operators.len(), 1, "`{query}`: one global row");
        }
        // The `*_over_time` window strands every sparse member, leaving only
        // the dense member in g=mix; g=allsparse is absent.
        "count by (g) (avg_over_time(sparse_total[30s]))" => {
            assert_eq!(via_operators.len(), 1, "`{query}`: only g=mix survives");
            assert!(
                approx_eq(group_value("mix").unwrap(), 1.0),
                "`{query}`: count over g=mix must be 1"
            );
            assert!(
                group_value("allsparse").is_none(),
                "`{query}`: g=allsparse must be absent"
            );
        }
        _ => {}
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "test defines an inline CountingStore mock with a full MetricStore impl"
)]
async fn range_query_scans_store_once_per_matcher_set_not_per_step() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crabka_blockstore::LabelMatcher;

    use crate::{
        error::Result,
        store::{
            ExemplarRecord, LabelNameCardinality, LabelValueCardinality, MetadataRecord,
            MetricStore, ScanResult, TsdbBlock, TsdbStats,
        },
    };

    // Wraps the in-memory store and counts store-level scans / series
    // resolutions, to prove the range driver no longer re-scans per step.
    struct CountingStore {
        inner: InMemoryMetricStore,
        scans: Arc<AtomicUsize>,
        series_calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl MetricStore for CountingStore {
        async fn scan(&self, t: &str, m: &[LabelMatcher], s: i64, e: i64) -> Result<ScanResult> {
            self.scans.fetch_add(1, Ordering::SeqCst);
            self.inner.scan(t, m, s, e).await
        }
        async fn series(&self, t: &str, m: &[LabelMatcher], s: i64, e: i64) -> Result<Vec<Labels>> {
            self.series_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.series(t, m, s, e).await
        }
        async fn label_names(
            &self,
            t: &str,
            m: &[LabelMatcher],
            s: i64,
            e: i64,
        ) -> Result<Vec<String>> {
            self.inner.label_names(t, m, s, e).await
        }
        async fn label_values(
            &self,
            t: &str,
            name: &str,
            m: &[LabelMatcher],
            s: i64,
            e: i64,
        ) -> Result<Vec<String>> {
            self.inner.label_values(t, name, m, s, e).await
        }
        async fn exemplars(
            &self,
            t: &str,
            m: &[LabelMatcher],
            s: i64,
            e: i64,
        ) -> Result<Vec<ExemplarRecord>> {
            self.inner.exemplars(t, m, s, e).await
        }
        async fn metadata(&self, t: &str, metric: Option<&str>) -> Result<Vec<MetadataRecord>> {
            self.inner.metadata(t, metric).await
        }
        async fn cardinality_label_names(&self, t: &str) -> Result<Vec<LabelNameCardinality>> {
            self.inner.cardinality_label_names(t).await
        }
        async fn cardinality_label_values(&self, t: &str) -> Result<Vec<LabelValueCardinality>> {
            self.inner.cardinality_label_values(t).await
        }
        async fn cardinality_active_series(&self, t: &str) -> Result<Vec<Labels>> {
            self.inner.cardinality_active_series(t).await
        }
        async fn tsdb_stats(&self, t: &str) -> Result<TsdbStats> {
            self.inner.tsdb_stats(t).await
        }
        async fn tsdb_blocks(&self, t: &str) -> Result<Vec<TsdbBlock>> {
            self.inner.tsdb_blocks(t).await
        }
    }

    let mut inner = InMemoryMetricStore::new();
    for i in 0..20 {
        inner.push_float(
            "t",
            labels(&[("__name__", "up"), ("job", "broker")]),
            i * 15_000,
            1.0,
        );
    }
    let scans = Arc::new(AtomicUsize::new(0));
    let series_calls = Arc::new(AtomicUsize::new(0));
    let engine = PromqlEngine::new(
        Arc::new(CountingStore {
            inner,
            scans: Arc::clone(&scans),
            series_calls: Arc::clone(&series_calls),
        }),
        EngineOpts::default(),
    );

    // 20 steps at 15s. Pre-fix this scanned the store ~2× per step (float +
    // histogram probe) plus a per-step series resolution. With the union-window
    // cache it is one float scan + one histogram scan + one series resolution
    // total, reused across every step.
    let result = engine
        .eval_range_via_planner_forced("t", "count({job=\"broker\"})", 0, 19 * 15_000, 15_000)
        .await
        .unwrap();
    assert!(matches!(result, QueryResult::RangeMatrix(_)));
    check!(
        scans.load(Ordering::SeqCst) == 2,
        "store scans should collapse to one float + one histogram union scan, got {}",
        scans.load(Ordering::SeqCst)
    );
    check!(
        series_calls.load(Ordering::SeqCst) == 1,
        "series resolution should be cached across steps, got {}",
        series_calls.load(Ordering::SeqCst)
    );
}

fn set_op_store() -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 1.0), ("b", 2.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("instance", instance), ("job", "api")]),
            10_000,
            value,
        );
    }
    for (instance, value) in [("b", 20.0), ("c", 30.0)] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "target_info"),
                ("instance", instance),
                ("region", "east"),
            ]),
            10_000,
            value,
        );
    }
    store
}

fn native_histogram(count: f64, sum: f64) -> NativeHistogram {
    NativeHistogram {
        schema: 0,
        is_float: true,
        reset_hint: ResetHint::No,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count,
        sum,
        positive_spans: vec![],
        positive_counts: vec![],
        negative_spans: vec![],
        negative_counts: vec![],
        custom_values: None,
        start_timestamp_ms: None,
    }
}

fn native_histogram_store() -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
        10_000,
        native_histogram(4.0, 10.0),
    );
    store
}

fn mixed_histogram_store() -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "series"), ("host", "a")]),
        0,
        native_histogram(4.0, 5.0),
    );
    for (le, value) in [("0.1", 2.0), ("1", 3.0), ("+Inf", 9.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "series"), ("host", "a"), ("le", le)]),
            0,
            value,
        );
    }
    store
}

#[tokio::test]
async fn histogram_quantile_mixed_emits_exact_warning_and_no_info() {
    let engine = PromqlEngine::new(Arc::new(mixed_histogram_store()), EngineOpts::default());
    let (_, annotations) = engine
        .query_instant_with_annotations("tenant-a", "histogram_quantile(0.8, series)", 0)
        .await
        .expect("query");
    assert_eq!(
        annotations,
        crate::Annotations {
            warnings: vec![
                "PromQL warning: vector contains a mix of classic and native histograms for metric name \"series\""
                    .to_string()
            ],
            infos: vec![],
        }
    );
}

#[tokio::test]
async fn histogram_fraction_mixed_emits_exact_warning() {
    let engine = PromqlEngine::new(Arc::new(mixed_histogram_store()), EngineOpts::default());
    let (_, annotations) = engine
        .query_instant_with_annotations("tenant-a", "histogram_fraction(-Inf, 1, series)", 0)
        .await
        .expect("query");
    assert!(annotations.warnings.iter().any(|w| w
            == "PromQL warning: vector contains a mix of classic and native histograms for metric name \"series\""));
}

#[tokio::test]
async fn histogram_float_comparison_emits_incompatible_types_info() {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "h"), ("job", "app")]),
        0,
        native_histogram(4.0, 5.0),
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let (result, annotations) = engine
        .query_instant_with_annotations("tenant-a", "h > 80", 0)
        .await
        .expect("query");
    assert!(matches!(result, QueryResult::InstantVector(ref v) if v.is_empty()));
    assert_eq!(
        annotations,
        crate::Annotations {
            infos: vec![
                "PromQL info: incompatible sample types encountered for binary operator \">\": histogram > float"
                    .to_string()
            ],
            warnings: vec![],
        }
    );
}

#[tokio::test]
async fn clean_query_raises_no_annotations() {
    let engine = PromqlEngine::new(Arc::new(set_op_store()), EngineOpts::default());
    let (_, annotations) = engine
        .query_instant_with_annotations("tenant-a", "up", 10_000)
        .await
        .expect("query");
    assert!(annotations.is_empty());
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
async fn limit_ratio_over_bound_emits_capping_warning() {
    let mut store = InMemoryMetricStore::new();
    for instance in ["0", "1"] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "http_requests"), ("instance", instance)]),
            0,
            1.0,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let (_, annotations) = engine
        .query_instant_with_annotations("tenant-a", "count(limit_ratio(1.1, http_requests))", 0)
        .await
        .expect("query");
    assert_eq!(
        annotations.warnings,
        vec![
            "PromQL warning: ratio value should be between -1 and 1, got 1.1, capping to 1"
                .to_string()
        ]
    );
}

#[tokio::test]
async fn instant_label_join_combines_source_labels() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "up"),
            ("job", "api"),
            ("instance", "a"),
            ("zone", "us-east-1a"),
        ]),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            r#"label_join(up, "target", "/", "job", "instance")"#,
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].labels.get("target"), Some("api/a"));
    assert_eq!(samples[0].labels.get("zone"), Some("us-east-1a"));
    assert!(approx_eq(float_value(&samples[0].value), 1.0));
}

#[tokio::test]
async fn instant_label_replace_uses_regex_capture_groups() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("instance", "api-1:9100")]),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            r#"label_replace(up, "host", "$1", "instance", "([^:]+):.*")"#,
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].labels.get("host"), Some("api-1"));
    assert_eq!(samples[0].labels.get("instance"), Some("api-1:9100"));
    assert!(approx_eq(float_value(&samples[0].value), 1.0));
}

#[tokio::test]
async fn instant_clamp_bounds_vector_values() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("low", -5.0), ("mid", 7.0), ("high", 20.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "temperature_celsius"), ("instance", instance)]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "clamp(temperature_celsius, 0, 10)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert!(samples.len() == 3);
    let values = samples
        .iter()
        .map(|sample| {
            (
                sample.labels.get("instance").unwrap().to_string(),
                float_value(&sample.value),
            )
        })
        .collect::<Vec<_>>();
    check!(values.contains(&("low".to_string(), 0.0)));
    check!(values.contains(&("mid".to_string(), 7.0)));
    check!(values.contains(&("high".to_string(), 10.0)));
}

#[tokio::test]
async fn instant_clamp_min_and_max_apply_single_bound() {
    let mut store = InMemoryMetricStore::new();
    for (metric, value) in [("below", -5.0), ("inside", 7.0), ("above", 20.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "temperature_celsius"), ("case", metric)]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let min_result = engine
        .query_instant("tenant-a", "clamp_min(temperature_celsius, 0)", 10_000)
        .await
        .unwrap();
    let max_result = engine
        .query_instant("tenant-a", "clamp_max(temperature_celsius, 10)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(min_samples) = min_result else {
        panic!("expected vector");
    };
    let QueryResult::InstantVector(max_samples) = max_result else {
        panic!("expected vector");
    };
    check!(min_samples.len() == 3);
    check!(max_samples.len() == 3);
    check!(min_samples.iter().any(|sample| {
        sample.labels.get("case") == Some("below") && approx_eq(float_value(&sample.value), 0.0)
    }));
    check!(min_samples.iter().any(|sample| {
        sample.labels.get("case") == Some("above") && approx_eq(float_value(&sample.value), 20.0)
    }));
    check!(max_samples.iter().any(|sample| {
        sample.labels.get("case") == Some("below") && approx_eq(float_value(&sample.value), -5.0)
    }));
    check!(max_samples.iter().any(|sample| {
        sample.labels.get("case") == Some("above") && approx_eq(float_value(&sample.value), 10.0)
    }));
}

#[tokio::test]
async fn instant_unary_numeric_functions_transform_vector_values() {
    let mut store = InMemoryMetricStore::new();
    for (case, value) in [("neg", -1.2), ("zero", 0.0), ("pos", 1.2)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "temperature_celsius"), ("case", case)]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        (
            "ceil(temperature_celsius)",
            [("neg", -1.0), ("zero", 0.0), ("pos", 2.0)],
        ),
        (
            "floor(temperature_celsius)",
            [("neg", -2.0), ("zero", 0.0), ("pos", 1.0)],
        ),
        (
            "sgn(temperature_celsius)",
            [("neg", -1.0), ("zero", 0.0), ("pos", 1.0)],
        ),
        (
            "abs(temperature_celsius)",
            [("neg", 1.2), ("zero", 0.0), ("pos", 1.2)],
        ),
        (
            "sqrt(abs(temperature_celsius))",
            [
                ("neg", 1.2_f64.sqrt()),
                ("zero", 0.0),
                ("pos", 1.2_f64.sqrt()),
            ],
        ),
        (
            "exp(abs(temperature_celsius))",
            [
                ("neg", 1.2_f64.exp()),
                ("zero", 1.0),
                ("pos", 1.2_f64.exp()),
            ],
        ),
        (
            "ln(abs(temperature_celsius))",
            [
                ("neg", 1.2_f64.ln()),
                ("zero", f64::NEG_INFINITY),
                ("pos", 1.2_f64.ln()),
            ],
        ),
        (
            "log2(abs(temperature_celsius))",
            [
                ("neg", 1.2_f64.log2()),
                ("zero", f64::NEG_INFINITY),
                ("pos", 1.2_f64.log2()),
            ],
        ),
        (
            "log10(abs(temperature_celsius))",
            [
                ("neg", 1.2_f64.log10()),
                ("zero", f64::NEG_INFINITY),
                ("pos", 1.2_f64.log10()),
            ],
        ),
        (
            "round(temperature_celsius)",
            [("neg", -1.0), ("zero", 0.0), ("pos", 1.0)],
        ),
        (
            "round(temperature_celsius, 0.5)",
            [("neg", -1.0), ("zero", 0.0), ("pos", 1.0)],
        ),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 3);
        for (case, value) in expected {
            let sample = samples
                .iter()
                .find(|sample| sample.labels.get("case") == Some(case))
                .expect("sample for case");
            assert_eq!(sample.labels.get("__name__"), None, "case {case}");
            assert!(approx_eq(float_value(&sample.value), value), "case {case}");
        }
    }
}

#[tokio::test]
async fn instant_hyperbolic_functions_transform_vector_values() {
    let mut store = InMemoryMetricStore::new();
    for (case, value) in [("neg", -1.2), ("zero", 0.0), ("pos", 1.2)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "temperature_celsius"), ("case", case)]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        (
            "sinh(temperature_celsius)",
            [
                ("neg", (-1.2_f64).sinh()),
                ("zero", 0.0_f64.sinh()),
                ("pos", 1.2_f64.sinh()),
            ],
        ),
        (
            "cosh(temperature_celsius)",
            [
                ("neg", (-1.2_f64).cosh()),
                ("zero", 0.0_f64.cosh()),
                ("pos", 1.2_f64.cosh()),
            ],
        ),
        (
            "tanh(temperature_celsius)",
            [
                ("neg", (-1.2_f64).tanh()),
                ("zero", 0.0_f64.tanh()),
                ("pos", 1.2_f64.tanh()),
            ],
        ),
        (
            "asinh(temperature_celsius)",
            [
                ("neg", (-1.2_f64).asinh()),
                ("zero", 0.0_f64.asinh()),
                ("pos", 1.2_f64.asinh()),
            ],
        ),
        (
            "acosh(abs(temperature_celsius) + 1)",
            [
                ("neg", 2.2_f64.acosh()),
                ("zero", 1.0_f64.acosh()),
                ("pos", 2.2_f64.acosh()),
            ],
        ),
        (
            "atanh(temperature_celsius / 2)",
            [
                ("neg", (-0.6_f64).atanh()),
                ("zero", 0.0_f64.atanh()),
                ("pos", 0.6_f64.atanh()),
            ],
        ),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 3);
        for (case, value) in expected {
            let sample = samples
                .iter()
                .find(|sample| sample.labels.get("case") == Some(case))
                .expect("sample for case");
            assert_eq!(sample.labels.get("__name__"), None, "case {case}");
            assert!(approx_eq(float_value(&sample.value), value), "case {case}");
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "table-driven coverage keeps related PromQL trig functions readable"
)]
#[tokio::test]
async fn instant_trigonometric_functions_transform_vector_values() {
    let mut store = InMemoryMetricStore::new();
    for (case, value) in [
        ("zero", 0.0),
        ("half_pi", std::f64::consts::FRAC_PI_2),
        ("pi", std::f64::consts::PI),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "angle_radians"), ("case", case)]),
            10_000,
            value,
        );
    }
    for (case, value) in [("neg", -0.5), ("zero", 0.0), ("pos", 0.5)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "unit_value"), ("case", case)]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        (
            "sin(angle_radians)",
            [
                ("zero", 0.0_f64.sin()),
                ("half_pi", std::f64::consts::FRAC_PI_2.sin()),
                ("pi", std::f64::consts::PI.sin()),
            ],
        ),
        (
            "cos(angle_radians)",
            [
                ("zero", 0.0_f64.cos()),
                ("half_pi", std::f64::consts::FRAC_PI_2.cos()),
                ("pi", std::f64::consts::PI.cos()),
            ],
        ),
        (
            "tan(angle_radians)",
            [
                ("zero", 0.0_f64.tan()),
                ("half_pi", std::f64::consts::FRAC_PI_2.tan()),
                ("pi", std::f64::consts::PI.tan()),
            ],
        ),
        (
            "deg(angle_radians)",
            [("zero", 0.0), ("half_pi", 90.0), ("pi", 180.0)],
        ),
        (
            "rad(deg(angle_radians))",
            [
                ("zero", 0.0),
                ("half_pi", std::f64::consts::FRAC_PI_2),
                ("pi", std::f64::consts::PI),
            ],
        ),
        (
            "asin(unit_value)",
            [
                ("neg", (-0.5_f64).asin()),
                ("zero", 0.0_f64.asin()),
                ("pos", 0.5_f64.asin()),
            ],
        ),
        (
            "acos(unit_value)",
            [
                ("neg", (-0.5_f64).acos()),
                ("zero", 0.0_f64.acos()),
                ("pos", 0.5_f64.acos()),
            ],
        ),
        (
            "atan(unit_value)",
            [
                ("neg", (-0.5_f64).atan()),
                ("zero", 0.0_f64.atan()),
                ("pos", 0.5_f64.atan()),
            ],
        ),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 3);
        for (case, value) in expected {
            let sample = samples
                .iter()
                .find(|sample| sample.labels.get("case") == Some(case))
                .expect("sample for case");
            assert_eq!(sample.labels.get("__name__"), None, "case {case}");
            assert!(approx_eq(float_value(&sample.value), value), "case {case}");
        }
    }
}

#[tokio::test]
async fn scalar_pi_function_returns_pi_constant() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "pi()", 10_000)
        .await
        .unwrap();
    assert!(
        result
            == QueryResult::Scalar {
                ts_ms: 10_000,
                value: std::f64::consts::PI,
            }
    );
}

#[tokio::test]
async fn instant_sort_functions_order_vector_by_sample_value() {
    let mut store = InMemoryMetricStore::new();
    for (instance, zone, value) in [
        ("api-b", "us-west-2b", 3.0),
        ("api-a", "us-east-1a", 1.0),
        ("api-c", "us-east-1a", 2.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "queue_depth"),
                ("instance", instance),
                ("zone", zone),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected_instances) in [
        ("sort(queue_depth)", ["api-a", "api-c", "api-b"]),
        ("sort_desc(queue_depth)", ["api-b", "api-c", "api-a"]),
        (
            r#"sort_by_label(queue_depth, "zone", "instance")"#,
            ["api-a", "api-c", "api-b"],
        ),
        (
            r#"sort_by_label_desc(queue_depth, "zone", "instance")"#,
            ["api-b", "api-c", "api-a"],
        ),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 3);
        let instances = samples
            .iter()
            .map(|sample| sample.labels.get("instance").unwrap().to_string())
            .collect::<Vec<_>>();
        assert!(instances == expected_instances);
        assert!(
            samples
                .iter()
                .all(|sample| sample.labels.get("__name__") == Some("queue_depth"))
        );
    }
}

#[tokio::test]
async fn instant_calendar_functions_extract_utc_fields_from_sample_values() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "event_timestamp_seconds"), ("case", "leap")]),
        10_000,
        1_709_178_060.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        ("year(event_timestamp_seconds)", 2024.0),
        ("month(event_timestamp_seconds)", 2.0),
        ("day_of_month(event_timestamp_seconds)", 29.0),
        ("day_of_week(event_timestamp_seconds)", 4.0),
        ("day_of_year(event_timestamp_seconds)", 60.0),
        ("days_in_month(event_timestamp_seconds)", 29.0),
        ("hour(event_timestamp_seconds)", 3.0),
        ("minute(event_timestamp_seconds)", 41.0),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert_eq!(samples.len(), 1, "query {query}");
        assert_eq!(samples[0].labels.get("__name__"), None, "query {query}");
        assert_eq!(samples[0].labels.get("case"), Some("leap"), "query {query}");
        assert!(
            approx_eq(float_value(&samples[0].value), expected),
            "query {query}"
        );
    }
}

#[tokio::test]
async fn instant_calendar_functions_without_args_use_eval_time() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "minute()", 3_660_000)
        .await
        .unwrap();

    assert!(
        result
            == QueryResult::Scalar {
                ts_ms: 3_660_000,
                value: 1.0,
            }
    );
}

#[tokio::test]
async fn instant_clamp_with_reversed_bounds_returns_empty_vector() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "temperature_celsius"), ("instance", "api")]),
        10_000,
        7.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "clamp(temperature_celsius, 10, 0)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert!(samples.is_empty());
}

#[tokio::test]
async fn instant_selector_returns_latest_sample_within_lookback() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        20_000,
        2.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        40_000,
        4.0,
    );

    let engine = PromqlEngine::new(
        Arc::new(store),
        EngineOpts {
            lookback_delta_ms: 15_000,
            max_samples: 100,
            ..EngineOpts::default()
        },
    );

    let result = engine
        .query_instant("tenant-a", "up", 30_000)
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(
        (
            samples.len(),
            &samples[0].labels,
            samples[0].ts_ms,
            approx_eq(float_value(&samples[0].value), 2.0),
        ) == (
            1,
            &labels(&[("__name__", "up"), ("job", "api")]),
            20_000,
            true,
        )
    );
}

#[tokio::test]
async fn instant_selector_offset_shifts_evaluation_time_backwards() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        60_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        120_000,
        2.0,
    );

    let engine = PromqlEngine::new(
        Arc::new(store),
        EngineOpts {
            lookback_delta_ms: 30_000,
            max_samples: 100,
            ..EngineOpts::default()
        },
    );
    let result = engine
        .query_instant("tenant-a", "up offset 1m", 120_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].ts_ms == 60_000);
    check!(approx_eq(float_value(&samples[0].value), 1.0));
}

#[tokio::test]
async fn instant_selector_at_uses_absolute_evaluation_time() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        60_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        120_000,
        2.0,
    );

    let engine = PromqlEngine::new(
        Arc::new(store),
        EngineOpts {
            lookback_delta_ms: 30_000,
            max_samples: 100,
            ..EngineOpts::default()
        },
    );
    let result = engine
        .query_instant("tenant-a", "up @ 60", 120_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].ts_ms == 60_000);
    check!(approx_eq(float_value(&samples[0].value), 1.0));
}

#[tokio::test]
async fn instant_selector_at_and_offset_combine_order_independently() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        60_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        120_000,
        2.0,
    );

    let engine = PromqlEngine::new(
        Arc::new(store),
        EngineOpts {
            lookback_delta_ms: 30_000,
            max_samples: 100,
            ..EngineOpts::default()
        },
    );
    for query in ["up @ 120 offset 1m", "up offset 1m @ 120"] {
        let result = engine
            .query_instant("tenant-a", query, 999_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        check!(samples.len() == 1);
        check!(samples[0].ts_ms == 60_000);
        check!(approx_eq(float_value(&samples[0].value), 1.0));
    }
}

#[tokio::test]
async fn instant_selector_honors_label_matchers_and_tenant() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web")]),
        10_000,
        0.0,
    );
    store.push_float(
        "tenant-b",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        9.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", r#"up{job=~"a.*"}"#, 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 1.0));
}

#[tokio::test]
async fn instant_selector_or_matchers_union_matching_series() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
        10_000,
        2.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "db"), ("instance", "c")]),
        10_000,
        3.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", r#"up{job="api" or job="web"}"#, 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert!(samples.len() == 2);
    let values_by_job = samples
        .iter()
        .map(|sample| {
            (
                sample.labels.get("job").expect("job label").to_string(),
                float_value(&sample.value),
            )
        })
        .collect::<BTreeMap<_, _>>();
    check!(approx_eq(values_by_job["api"], 1.0));
    check!(approx_eq(values_by_job["web"], 2.0));
    check!(!values_by_job.contains_key("db"));
}

#[tokio::test]
async fn instant_selector_stale_marker_terminates_series_before_lookback_expiry() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        20_000,
        stale_nan(),
    );

    let engine = PromqlEngine::new(
        Arc::new(store),
        EngineOpts {
            lookback_delta_ms: 60_000,
            max_samples: 100,
            ..EngineOpts::default()
        },
    );
    let result = engine
        .query_instant("tenant-a", "up", 30_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert!(samples.is_empty());
}

#[tokio::test]
async fn instant_sum_aggregates_all_series() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web")]),
        10_000,
        2.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "sum(up)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.is_empty());
    check!(approx_eq(float_value(&samples[0].value), 3.0));
}

#[tokio::test]
async fn instant_sum_by_groups_by_exact_labels_and_drops_metric_name() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "b")]),
        10_000,
        2.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web"), ("instance", "c")]),
        10_000,
        4.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "sum by (job) (up)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert!(samples.len() == 2);
    let api = samples
        .iter()
        .find(|sample| sample.labels.get("job") == Some("api"))
        .expect("api group");
    assert_eq!(api.labels.get("__name__"), None);
    assert_eq!(api.labels.get("instance"), None);
    assert!(approx_eq(float_value(&api.value), 3.0));
    let web = samples
        .iter()
        .find(|sample| sample.labels.get("job") == Some("web"))
        .expect("web group");
    assert!(approx_eq(float_value(&web.value), 4.0));
}

#[tokio::test]
async fn instant_count_counts_series() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web")]),
        10_000,
        0.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "count(up)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert_eq!(samples.len(), 1);
    assert!(approx_eq(float_value(&samples[0].value), 2.0));
}

#[tokio::test]
async fn instant_group_returns_one_for_each_group() {
    let mut store = InMemoryMetricStore::new();
    for (job, value) in [("api", 10.0), ("api", 30.0), ("web", 99.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", job)]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "group by (job) (up)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert!(samples.len() == 2);
    for sample in samples {
        assert_eq!(sample.labels.get("__name__"), None);
        assert!(approx_eq(float_value(&sample.value), 1.0));
    }
}

#[tokio::test]
async fn instant_stddev_and_stdvar_aggregate_population_variance() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
        .into_iter()
        .enumerate()
    {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "latency_seconds"),
                ("job", "api"),
                ("instance", &instance.to_string()),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let stdvar = engine
        .query_instant("tenant-a", "stdvar(latency_seconds)", 10_000)
        .await
        .unwrap();
    let stddev = engine
        .query_instant("tenant-a", "stddev(latency_seconds)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(stdvar_samples) = stdvar else {
        panic!("expected vector");
    };
    let QueryResult::InstantVector(stddev_samples) = stddev else {
        panic!("expected vector");
    };
    check!(stdvar_samples.len() == 1);
    check!(stddev_samples.len() == 1);
    check!(approx_eq(float_value(&stdvar_samples[0].value), 4.0));
    check!(approx_eq(float_value(&stddev_samples[0].value), 2.0));
}

/// M16: `stdvar`/`stddev` over a large-offset close-valued group must not
/// catastrophically cancel into a negative variance (whose `sqrt` is NaN).
/// Welford yields the small positive population variance `{0,1,2}` -> 2/3.
#[tokio::test]
async fn instant_stdvar_aggregate_is_stable_for_large_offset_group() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [1e8, 1e8 + 1.0, 1e8 + 2.0].into_iter().enumerate() {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "big"),
                ("job", "api"),
                ("instance", &instance.to_string()),
            ]),
            10_000,
            value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let QueryResult::InstantVector(stdvar) = engine
        .query_instant("tenant-a", "stdvar(big)", 10_000)
        .await
        .unwrap()
    else {
        panic!("expected vector");
    };
    assert!(stdvar.len() == 1);
    let value = float_value(&stdvar[0].value);
    check!(!value.is_nan(), "stdvar must be finite, got NaN");
    check!(value > 0.0, "stdvar must be positive, got {value}");
    check!(approx_eq(value, 2.0 / 3.0), "stdvar == 2/3, got {value}");
}

/// M17: `avg` of very-large-magnitude samples must not overflow the running
/// sum to +/-Inf; the incremental Kahan mean stays finite and equals the
/// common value for two equal maxima.
#[tokio::test]
async fn instant_avg_aggregate_does_not_overflow() {
    let mut store = InMemoryMetricStore::new();
    for instance in 0..2 {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "huge"),
                ("job", "api"),
                ("instance", &instance.to_string()),
            ]),
            10_000,
            f64::MAX,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let QueryResult::InstantVector(avg) = engine
        .query_instant("tenant-a", "avg(huge)", 10_000)
        .await
        .unwrap()
    else {
        panic!("expected vector");
    };
    assert!(avg.len() == 1);
    let value = float_value(&avg[0].value);
    assert!(value.is_finite(), "avg must stay finite, got {value}");
    assert!(approx_eq(value, f64::MAX));
}

/// M19: `count_values` renders a non-finite sample value through the canonical
/// Prometheus float formatter, so `+Inf` (not `f64::to_string`'s `inf`)
/// becomes the label value.
#[tokio::test]
async fn instant_count_values_formats_infinity_as_prometheus() {
    let mut store = InMemoryMetricStore::new();
    for instance in 0..2 {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "ratio"),
                ("job", "api"),
                ("instance", &instance.to_string()),
            ]),
            10_000,
            f64::INFINITY,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let QueryResult::InstantVector(samples) = engine
        .query_instant("tenant-a", r#"count_values("v", ratio)"#, 10_000)
        .await
        .unwrap()
    else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(
        samples[0].labels.get("v") == Some("+Inf"),
        "count_values must render +Inf, got {:?}",
        samples[0].labels.get("v")
    );
    check!(approx_eq(float_value(&samples[0].value), 2.0));
}

#[tokio::test]
async fn instant_count_values_counts_by_sample_value() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [200.0, 200.0, 500.0].into_iter().enumerate() {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "http_responses_total"),
                ("job", "api"),
                ("instance", &instance.to_string()),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            r#"count_values("code", http_responses_total)"#,
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert!(samples.len() == 2);
    let ok = samples
        .iter()
        .find(|sample| sample.labels.get("code") == Some("200"))
        .expect("200 bucket");
    assert_eq!(ok.labels.get("__name__"), None);
    assert!(approx_eq(float_value(&ok.value), 2.0));
    let err = samples
        .iter()
        .find(|sample| sample.labels.get("code") == Some("500"))
        .expect("500 bucket");
    assert!(approx_eq(float_value(&err.value), 1.0));
}

#[tokio::test]
async fn instant_count_values_counts_native_histogram_sample_values() {
    let mut repeated = native_histogram(4.0, 10.0);
    repeated.zero_count = 1.0;
    repeated.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 1,
    }];
    repeated.positive_counts = vec![3.0];
    let mut distinct = repeated.clone();
    distinct.sum = 12.0;

    let mut store = InMemoryMetricStore::new();
    for (instance, histogram) in [
        ("a", repeated.clone()),
        ("b", repeated.clone()),
        ("c", distinct),
    ] {
        store.push_histogram(
            "tenant-a",
            labels(&[
                ("__name__", "request_duration_seconds"),
                ("job", "api"),
                ("instance", instance),
            ]),
            10_000,
            histogram,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            r#"count_values by (job) ("histogram", request_duration_seconds)"#,
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    let mut values = samples
        .iter()
        .map(|sample| float_value(&sample.value))
        .collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    check!(
        (
            samples.len(),
            samples.iter().all(|sample| {
                sample.labels.get("__name__").is_none()
                    && sample.labels.get("job") == Some("api")
                    && sample.labels.get("histogram").is_some()
            }),
            values,
        ) == (2, true, vec![1.0, 2.0])
    );
}

#[tokio::test]
async fn instant_topk_keeps_largest_samples_with_original_labels() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 1.0), ("b", 3.0), ("c", 2.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "memory_bytes"), ("instance", instance)]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "topk(2, memory_bytes)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    let mut projection = samples
        .iter()
        .map(|sample| {
            (
                sample.labels.get("__name__"),
                sample.labels.get("instance"),
                float_value(&sample.value),
            )
        })
        .collect::<Vec<_>>();
    projection.sort_by_key(|(_, instance, _)| *instance);
    check!(
        projection
            == vec![
                (Some("memory_bytes"), Some("b"), 3.0),
                (Some("memory_bytes"), Some("c"), 2.0),
            ]
    );
}

#[tokio::test]
async fn instant_bottomk_keeps_smallest_samples_with_original_labels() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 1.0), ("b", 3.0), ("c", 2.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "memory_bytes"), ("instance", instance)]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "bottomk(2, memory_bytes)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 2);
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("memory_bytes")
            && sample.labels.get("instance") == Some("a")
            && approx_eq(float_value(&sample.value), 1.0)
    }));
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("memory_bytes")
            && sample.labels.get("instance") == Some("c")
            && approx_eq(float_value(&sample.value), 2.0)
    }));
}

#[tokio::test]
async fn instant_topk_by_selects_largest_sample_per_group_with_original_labels() {
    let mut store = InMemoryMetricStore::new();
    for (job, instance, value) in [
        ("api", "a", 1.0),
        ("api", "b", 3.0),
        ("worker", "c", 5.0),
        ("worker", "d", 2.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "memory_bytes"),
                ("job", job),
                ("instance", instance),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "topk by (job) (1, memory_bytes)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 2);
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("memory_bytes")
            && sample.labels.get("job") == Some("api")
            && sample.labels.get("instance") == Some("b")
            && approx_eq(float_value(&sample.value), 3.0)
    }));
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("memory_bytes")
            && sample.labels.get("job") == Some("worker")
            && sample.labels.get("instance") == Some("c")
            && approx_eq(float_value(&sample.value), 5.0)
    }));
}

#[tokio::test]
async fn instant_bottomk_without_selects_smallest_sample_per_group_with_original_labels() {
    let mut store = InMemoryMetricStore::new();
    for (job, instance, value) in [
        ("api", "a", 4.0),
        ("api", "b", 1.0),
        ("worker", "c", 5.0),
        ("worker", "d", 2.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "memory_bytes"),
                ("job", job),
                ("instance", instance),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "bottomk without (instance) (1, memory_bytes)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 2);
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("memory_bytes")
            && sample.labels.get("job") == Some("api")
            && sample.labels.get("instance") == Some("b")
            && approx_eq(float_value(&sample.value), 1.0)
    }));
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("memory_bytes")
            && sample.labels.get("job") == Some("worker")
            && sample.labels.get("instance") == Some("d")
            && approx_eq(float_value(&sample.value), 2.0)
    }));
}

#[tokio::test]
async fn instant_topk_and_bottomk_ignore_histograms() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 1.0), ("b", 3.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "memory_bytes"), ("instance", instance)]),
            10_000,
            value,
        );
    }
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "memory_bytes"), ("instance", "hist")]),
        10_000,
        native_histogram(4.0, 10.0),
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected_instance, expected_value) in [
        ("topk(1, memory_bytes)", "b", 3.0),
        ("bottomk(1, memory_bytes)", "a", 1.0),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert_eq!(samples.len(), 1, "{query}");
        assert_eq!(
            samples[0].labels.get("__name__"),
            Some("memory_bytes"),
            "{query}"
        );
        assert_eq!(
            samples[0].labels.get("instance"),
            Some(expected_instance),
            "{query}"
        );
        assert!(
            approx_eq(float_value(&samples[0].value), expected_value),
            "{query}"
        );
    }
}

#[cfg(not(feature = "experimental-functions"))]
#[tokio::test]
async fn instant_limit_ratio_requires_experimental_feature() {
    let store = InMemoryMetricStore::new();
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let error = engine
        .query_instant("tenant-a", "limit_ratio(0.5, memory_bytes)", 10_000)
        .await
        .unwrap_err();

    assert!(matches!(error, PromqlError::Unsupported(_)));
    assert!(format!("{error}").contains("experimental-functions"));
}

#[cfg(not(feature = "experimental-functions"))]
#[tokio::test]
async fn instant_limitk_requires_experimental_feature() {
    let store = InMemoryMetricStore::new();
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let error = engine
        .query_instant("tenant-a", "limitk(2, memory_bytes)", 10_000)
        .await
        .unwrap_err();

    assert!(matches!(error, PromqlError::Unsupported(_)));
    assert!(format!("{error}").contains("experimental-functions"));
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
async fn instant_limitk_selects_deterministic_hash_subset() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0), ("e", 5.0)] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "memory_bytes"),
                ("job", "api"),
                ("instance", instance),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "limitk(2, memory_bytes)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    let selected = sample_instances(&samples);
    assert!(selected == vec!["c", "e"]);
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
async fn instant_limitk_by_selects_deterministic_hash_subset_per_group() {
    let mut store = InMemoryMetricStore::new();
    for (job, instance, value) in [
        ("api", "a", 1.0),
        ("api", "b", 2.0),
        ("api", "c", 3.0),
        ("worker", "d", 4.0),
        ("worker", "e", 5.0),
        ("worker", "f", 6.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "memory_bytes"),
                ("job", job),
                ("instance", instance),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "limitk by (job) (1, memory_bytes)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 2);
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("memory_bytes")
            && sample.labels.get("job") == Some("api")
            && sample.labels.get("instance") == Some("c")
            && approx_eq(float_value(&sample.value), 3.0)
    }));
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("memory_bytes")
            && sample.labels.get("job") == Some("worker")
            && sample.labels.get("instance") == Some("d")
            && approx_eq(float_value(&sample.value), 4.0)
    }));
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
async fn instant_limit_ratio_selects_deterministic_hash_subset() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0), ("e", 5.0)] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "memory_bytes"),
                ("job", "api"),
                ("instance", instance),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "limit_ratio(0.75, memory_bytes)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    let selected = sample_instances(&samples);
    assert!(selected == vec!["b", "c", "e"]);
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
async fn instant_limit_ratio_negative_selects_complement_subset() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0), ("e", 5.0)] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "memory_bytes"),
                ("job", "api"),
                ("instance", instance),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "limit_ratio(-0.25, memory_bytes)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    let selected = sample_instances(&samples);
    assert!(selected == vec!["a", "d"]);
}

#[tokio::test]
async fn instant_quantile_interpolates_per_group() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [1.0, 2.0, 4.0, 8.0].into_iter().enumerate() {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "latency_seconds"),
                ("job", "api"),
                ("instance", &instance.to_string()),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "quantile by (job) (0.5, latency_seconds)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 3.0));
}

#[tokio::test]
async fn instant_quantile_aggregation_ignores_histograms() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 2.0), ("b", 6.0)] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "latency_seconds"),
                ("job", "api"),
                ("instance", instance),
            ]),
            10_000,
            value,
        );
    }
    store.push_histogram(
        "tenant-a",
        labels(&[
            ("__name__", "latency_seconds"),
            ("job", "api"),
            ("instance", "hist"),
        ]),
        10_000,
        native_histogram(4.0, 10.0),
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "quantile by (job) (0.5, latency_seconds)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].labels.get("__name__"), None);
    assert_eq!(samples[0].labels.get("job"), Some("api"));
    assert!(approx_eq(float_value(&samples[0].value), 4.0));
}

#[tokio::test]
async fn instant_min_max_and_std_aggregations_ignore_histograms() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "mixed_metric"),
            ("job", "api"),
            ("instance", "a"),
        ]),
        10_000,
        4.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "mixed_metric"),
            ("job", "api"),
            ("instance", "b"),
        ]),
        10_000,
        8.0,
    );
    store.push_histogram(
        "tenant-a",
        labels(&[
            ("__name__", "mixed_metric"),
            ("job", "api"),
            ("instance", "hist"),
        ]),
        10_000,
        native_histogram(4.0, 10.0),
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        ("min by (job) (mixed_metric)", 4.0),
        ("max by (job) (mixed_metric)", 8.0),
        ("stddev by (job) (mixed_metric)", 2.0),
        ("stdvar by (job) (mixed_metric)", 4.0),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert_eq!(samples.len(), 1, "{query}");
        assert_eq!(samples[0].labels.get("__name__"), None, "{query}");
        assert_eq!(samples[0].labels.get("job"), Some("api"), "{query}");
        assert!(
            approx_eq(float_value(&samples[0].value), expected),
            "{query}"
        );
    }
}

#[tokio::test]
async fn instant_count_and_group_aggregations_include_histograms() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "mixed_metric"),
            ("job", "api"),
            ("instance", "float"),
        ]),
        10_000,
        4.0,
    );
    store.push_histogram(
        "tenant-a",
        labels(&[
            ("__name__", "mixed_metric"),
            ("job", "api"),
            ("instance", "hist"),
        ]),
        10_000,
        native_histogram(4.0, 10.0),
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        ("count by (job) (mixed_metric)", 2.0),
        ("group by (job) (mixed_metric)", 1.0),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert_eq!(samples.len(), 1, "{query}");
        assert_eq!(samples[0].labels.get("__name__"), None, "{query}");
        assert_eq!(samples[0].labels.get("job"), Some("api"), "{query}");
        assert!(
            approx_eq(float_value(&samples[0].value), expected),
            "{query}"
        );
    }
}

#[tokio::test]
async fn instant_sum_and_avg_aggregations_combine_compatible_native_histograms() {
    let mut left = native_histogram(4.0, 10.0);
    left.zero_count = 1.0;
    left.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 2,
    }];
    left.positive_counts = vec![1.0, 2.0];
    let mut right = native_histogram(6.0, 20.0);
    right.zero_count = 2.0;
    right.positive_spans = left.positive_spans.clone();
    right.positive_counts = vec![2.0, 2.0];

    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[
            ("__name__", "request_duration_seconds"),
            ("job", "api"),
            ("instance", "a"),
        ]),
        10_000,
        left,
    );
    store.push_histogram(
        "tenant-a",
        labels(&[
            ("__name__", "request_duration_seconds"),
            ("job", "api"),
            ("instance", "b"),
        ]),
        10_000,
        right,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected_count, expected_sum, expected_avg) in [
        ("sum by (job) (request_duration_seconds)", 10.0, 30.0, 3.0),
        ("avg by (job) (request_duration_seconds)", 5.0, 15.0, 3.0),
    ] {
        let count = engine
            .query_instant("tenant-a", &format!("histogram_count({query})"), 10_000)
            .await
            .unwrap();
        let sum = engine
            .query_instant("tenant-a", &format!("histogram_sum({query})"), 10_000)
            .await
            .unwrap();
        let avg = engine
            .query_instant("tenant-a", &format!("histogram_avg({query})"), 10_000)
            .await
            .unwrap();

        assert_single_float_sample(&count, "api", expected_count, query);
        assert_single_float_sample(&sum, "api", expected_sum, query);
        assert_single_float_sample(&avg, "api", expected_avg, query);
    }
}

#[tokio::test]
async fn instant_sum_aggregation_combines_native_histograms_with_different_span_layouts() {
    let mut left = native_histogram(4.0, 10.0);
    left.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 1,
    }];
    left.positive_counts = vec![1.0];
    let mut right = native_histogram(6.0, 20.0);
    right.positive_spans = vec![BucketSpan {
        offset: 1,
        length: 1,
    }];
    right.positive_counts = vec![2.0];

    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[
            ("__name__", "request_duration_seconds"),
            ("job", "api"),
            ("instance", "a"),
        ]),
        10_000,
        left,
    );
    store.push_histogram(
        "tenant-a",
        labels(&[
            ("__name__", "request_duration_seconds"),
            ("job", "api"),
            ("instance", "b"),
        ]),
        10_000,
        right,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "sum by (job) (request_duration_seconds)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert_eq!(samples.len(), 1);
    let SampleValue::Histogram(histogram) = &samples[0].value else {
        panic!("expected histogram");
    };
    assert!(approx_eq(histogram.count, 10.0));
    assert!(approx_eq(histogram.sum, 30.0));
    assert_eq!(
        &histogram.positive_spans,
        &vec![BucketSpan {
            offset: 0,
            length: 2,
        }]
    );
    assert_eq!(&histogram.positive_counts, &vec![1.0, 2.0]);
}

#[tokio::test]
async fn instant_sum_and_avg_aggregations_omit_mixed_float_and_histogram_groups() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "mixed_metric"),
            ("job", "api"),
            ("instance", "float"),
        ]),
        10_000,
        4.0,
    );
    store.push_histogram(
        "tenant-a",
        labels(&[
            ("__name__", "mixed_metric"),
            ("job", "api"),
            ("instance", "hist"),
        ]),
        10_000,
        native_histogram(4.0, 10.0),
    );
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "mixed_metric"),
            ("job", "web"),
            ("instance", "float"),
        ]),
        10_000,
        6.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for query in ["sum by (job) (mixed_metric)", "avg by (job) (mixed_metric)"] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert_eq!(samples.len(), 1, "{query}");
        assert_eq!(samples[0].labels.get("job"), Some("web"), "{query}");
        assert!(approx_eq(float_value(&samples[0].value), 6.0), "{query}");
    }
}

#[tokio::test]
async fn instant_sum_aggregation_rejects_incompatible_native_histograms() {
    let mut left = native_histogram(4.0, 10.0);
    left.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 1,
    }];
    left.positive_counts = vec![1.0];
    let mut right = native_histogram(6.0, 20.0);
    right.schema = 1;
    right.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 1,
    }];
    right.positive_counts = vec![2.0];

    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[
            ("__name__", "request_duration_seconds"),
            ("job", "api"),
            ("instance", "a"),
        ]),
        10_000,
        left,
    );
    store.push_histogram(
        "tenant-a",
        labels(&[
            ("__name__", "request_duration_seconds"),
            ("job", "api"),
            ("instance", "b"),
        ]),
        10_000,
        right,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let error = engine
        .query_instant(
            "tenant-a",
            "sum by (job) (request_duration_seconds)",
            10_000,
        )
        .await
        .unwrap_err();

    assert!(matches!(error, PromqlError::Unsupported(_)));
    assert!(format!("{error}").contains("incompatible native histogram"));
}

#[tokio::test]
async fn instant_histogram_quantile_interpolates_classic_buckets() {
    let mut store = InMemoryMetricStore::new();
    for (le, value) in [("0.1", 0.0), ("0.2", 1.0), ("0.4", 3.0), ("+Inf", 3.0)] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "http_request_duration_seconds_bucket"),
                ("job", "api"),
                ("le", le),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "histogram_quantile(0.5, http_request_duration_seconds_bucket)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("le").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 0.25));
}

#[cfg(not(feature = "experimental-functions"))]
#[tokio::test]
async fn histogram_quantiles_requires_experimental_feature() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let error = engine
        .query_instant(
            "tenant-a",
            r#"histogram_quantiles(vector(1), "quantile", 0.5)"#,
            10_000,
        )
        .await
        .unwrap_err();

    assert!(format!("{error}").contains("experimental-functions"));
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
async fn histogram_quantiles_emits_one_sample_per_requested_quantile() {
    let mut store = InMemoryMetricStore::new();
    for (le, value) in [("0.1", 0.0), ("0.2", 1.0), ("0.4", 3.0), ("+Inf", 3.0)] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "http_request_duration_seconds_bucket"),
                ("job", "api"),
                ("le", le),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            r#"histogram_quantiles(http_request_duration_seconds_bucket, "quantile", 0.5, 0.9)"#,
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert!(samples.len() == 2);
    let values = samples
        .iter()
        .map(|sample| {
            (
                sample.labels.get("quantile").expect("quantile label"),
                float_value(&sample.value),
            )
        })
        .collect::<BTreeMap<_, _>>();
    check!(approx_eq(*values.get("0.5").expect("p50 sample"), 0.25));
    check!(approx_eq(*values.get("0.9").expect("p90 sample"), 0.37));
    check!(samples.iter().all(|sample| {
        sample.labels.get("__name__").is_none()
            && sample.labels.get("job") == Some("api")
            && sample.labels.get("le").is_none()
    }));
}

#[tokio::test]
async fn instant_histogram_quantile_interpolates_native_histogram_buckets() {
    let mut histogram = native_histogram(4.0, 6.5);
    histogram.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 2,
    }];
    histogram.positive_counts = vec![1.0, 3.0];
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
        10_000,
        histogram,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "histogram_quantile(0.5, request_duration_seconds)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(
        float_value(&samples[0].value),
        2_f64.powf(1.0 / 3.0)
    ));
}

#[tokio::test]
async fn instant_histogram_fraction_estimates_native_histogram_bucket_overlap() {
    let mut histogram = native_histogram(4.0, 6.5);
    histogram.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 2,
    }];
    histogram.positive_counts = vec![1.0, 3.0];
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
        10_000,
        histogram,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "histogram_fraction(1, 2, request_duration_seconds)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 0.75));
}

#[tokio::test]
async fn instant_histogram_count_returns_native_histogram_count() {
    let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "histogram_count(request_duration_seconds)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 4.0));
}

#[tokio::test]
async fn instant_histogram_sum_returns_native_histogram_sum() {
    let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "histogram_sum(request_duration_seconds)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 10.0));
}

#[tokio::test]
async fn instant_histogram_avg_returns_native_histogram_average() {
    let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "histogram_avg(request_duration_seconds)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 2.5));
}

#[tokio::test]
async fn native_histogram_scalar_arithmetic_scales_histograms() {
    let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
    for (query, expected) in [
        ("histogram_count(request_duration_seconds * 2)", 8.0),
        ("histogram_sum(2 * request_duration_seconds)", 20.0),
        ("histogram_count(request_duration_seconds / 2)", 2.0),
        ("histogram_sum(request_duration_seconds / 2)", 5.0),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert_eq!(samples.len(), 1, "{query}");
        assert_eq!(samples[0].labels.get("__name__"), None, "{query}");
        assert_eq!(samples[0].labels.get("job"), Some("api"), "{query}");
        assert!(
            approx_eq(float_value(&samples[0].value), expected),
            "{query}"
        );
    }
}

#[tokio::test]
async fn native_histogram_scalar_arithmetic_drops_invalid_operator_orders() {
    let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
    for query in [
        "histogram_count(2 / request_duration_seconds)",
        "histogram_count(request_duration_seconds + 2)",
    ] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.is_empty(), "{query}");
    }
}

#[tokio::test]
async fn instant_histogram_stdvar_estimates_native_histogram_population_variance() {
    let mut histogram = native_histogram(4.0, 5.25);
    histogram.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 2,
    }];
    histogram.positive_counts = vec![1.0, 3.0];
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
        10_000,
        histogram,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "histogram_stdvar(request_duration_seconds)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(
        float_value(&samples[0].value),
        0.099_384_473_924_297_3
    ));
}

#[tokio::test]
async fn instant_histogram_stddev_returns_native_histogram_population_stddev() {
    let mut histogram = native_histogram(4.0, 5.25);
    histogram.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 2,
    }];
    histogram.positive_counts = vec![1.0, 3.0];
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
        10_000,
        histogram,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "histogram_stddev(request_duration_seconds)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(
        float_value(&samples[0].value),
        0.099_384_473_924_297_3_f64.sqrt()
    ));
}

#[tokio::test]
async fn scalar_binary_arithmetic_returns_scalar() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "2 * 3", 10_000)
        .await
        .unwrap();
    assert!(
        result
            == QueryResult::Scalar {
                ts_ms: 10_000,
                value: 6.0
            }
    );
}

#[tokio::test]
async fn scalar_binary_atan2_returns_angle_radians() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "1 atan2 1", 10_000)
        .await
        .unwrap();
    assert!(
        result
            == QueryResult::Scalar {
                ts_ms: 10_000,
                value: std::f64::consts::FRAC_PI_4,
            }
    );
}

#[cfg(not(feature = "experimental-functions"))]
#[tokio::test]
async fn scalar_max_of_min_of_require_experimental_feature() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    for query in ["max_of(1, 2)", "min_of(1, 2)"] {
        let error = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap_err();

        assert!(matches!(error, PromqlError::Unsupported(_)));
        assert!(format!("{error}").contains("experimental-functions"));
    }
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
async fn scalar_max_of_min_of_return_larger_and_smaller_scalar() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    for (query, expected) in [("max_of(1, 2)", 2.0), ("min_of(1, 2)", 1.0)] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();
        assert!(
            result
                == QueryResult::Scalar {
                    ts_ms: 10_000,
                    value: expected,
                }
        );
    }
}

#[tokio::test]
async fn scalar_function_converts_single_sample_vector_and_nan_otherwise() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "single_value"), ("instance", "a")]),
        10_000,
        7.0,
    );
    for (instance, value) in [("a", 1.0), ("b", 2.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "multi_value"), ("instance", instance)]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let single = engine
        .query_instant("tenant-a", "scalar(single_value)", 10_000)
        .await
        .unwrap();
    assert!(
        single
            == QueryResult::Scalar {
                ts_ms: 10_000,
                value: 7.0,
            }
    );

    for query in ["scalar(missing_metric)", "scalar(multi_value)"] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();
        let QueryResult::Scalar { ts_ms, value } = result else {
            panic!("expected scalar");
        };
        assert!(ts_ms == 10_000);
        assert!(value.is_nan());
    }
}

#[tokio::test]
async fn vector_function_converts_scalar_to_unlabeled_instant_vector() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "vector(2 * 3)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.is_empty());
    check!(samples[0].ts_ms == 10_000);
    check!(approx_eq(float_value(&samples[0].value), 6.0));
}

#[tokio::test]
async fn vector_scalar_arithmetic_preserves_labels_and_drops_metric_name() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "up * 2", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 2.0));
}

#[tokio::test]
async fn vector_scalar_atan2_preserves_labels_and_drops_metric_name() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "y"), ("job", "api")]),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "y atan2 0", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(
        float_value(&samples[0].value),
        std::f64::consts::FRAC_PI_2
    ));
}

#[tokio::test]
async fn vector_vector_arithmetic_matches_on_labels() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "a"), ("x", "1")]),
        10_000,
        10.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "b"), ("x", "1")]),
        10_000,
        5.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "b"), ("x", "2")]),
        10_000,
        99.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "a + on (x) b", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("x") == Some("1"));
    check!(approx_eq(float_value(&samples[0].value), 15.0));
}

#[tokio::test]
async fn vector_vector_arithmetic_drops_metadata_labels() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "requests_total"),
            ("__type__", "counter"),
            ("__unit__", "requests"),
            ("instance", "a"),
        ]),
        10_000,
        10.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "requests_total"),
            ("__type__", "counter"),
            ("__unit__", "requests"),
            ("instance", "b"),
        ]),
        10_000,
        5.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "requests_total + 1", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 2);
    for name in ["__name__", "__type__", "__unit__"] {
        check!(
            samples
                .iter()
                .all(|sample| sample.labels.get(name).is_none()),
            "{name} must be dropped"
        );
    }
}

#[tokio::test]
async fn vector_vector_arithmetic_fill_uses_missing_side_values() {
    let mut store = InMemoryMetricStore::new();
    for (metric, instance, value) in [
        ("a", "matched", 10.0),
        ("a", "left-only", 7.0),
        ("b", "matched", 3.0),
        ("b", "right-only", 5.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", metric), ("instance", instance)]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "a + on (instance) fill(0) b", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    let values = samples
        .iter()
        .map(|sample| {
            (
                sample.labels.get("instance").expect("instance label"),
                float_value(&sample.value),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(values.len(), 3);
    check!(approx_eq(values["matched"], 13.0));
    check!(approx_eq(values["left-only"], 7.0));
    check!(approx_eq(values["right-only"], 5.0));
    check!(samples.iter().all(|sample| {
        let label_names = sample
            .labels
            .iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>();
        sample.labels.get("__name__").is_none() && label_names == vec!["instance"]
    }));
}

#[tokio::test]
async fn vector_vector_arithmetic_combines_compatible_native_histograms() {
    let mut left = native_histogram(4.0, 10.0);
    left.zero_count = 1.0;
    left.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 1,
    }];
    left.positive_counts = vec![3.0];
    let mut right = native_histogram(2.0, 4.0);
    right.zero_count = 0.5;
    right.positive_spans = left.positive_spans.clone();
    right.positive_counts = vec![1.5];

    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "a"), ("job", "api"), ("x", "1")]),
        10_000,
        left,
    );
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "b"), ("job", "api"), ("x", "1")]),
        10_000,
        right,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected_count, expected_sum) in
        [("a + on (x) b", 6.0, 14.0), ("a - on (x) b", 2.0, 6.0)]
    {
        let count = engine
            .query_instant("tenant-a", &format!("histogram_count({query})"), 10_000)
            .await
            .unwrap();
        let sum = engine
            .query_instant("tenant-a", &format!("histogram_sum({query})"), 10_000)
            .await
            .unwrap();

        assert_single_on_x_float_sample(&count, expected_count, query);
        assert_single_on_x_float_sample(&sum, expected_sum, query);
    }
}

#[tokio::test]
async fn vector_vector_comparison_matches_native_histogram_equality() {
    let mut left = native_histogram(4.0, 10.0);
    left.zero_count = 1.0;
    left.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 1,
    }];
    left.positive_counts = vec![3.0];
    let equal = left.clone();
    let mut different = left.clone();
    different.sum = 11.0;

    let mut store = InMemoryMetricStore::new();
    for (name, histogram) in [("a", left), ("b", equal), ("c", different)] {
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", name), ("job", "api"), ("x", "1")]),
            10_000,
            histogram,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let equal = engine
        .query_instant("tenant-a", "histogram_count(a == on (x) b)", 10_000)
        .await
        .unwrap();
    assert_single_float_sample(&equal, "api", 4.0, "a == b");

    let not_equal = engine
        .query_instant("tenant-a", "histogram_count(a != on (x) c)", 10_000)
        .await
        .unwrap();
    assert_single_float_sample(&not_equal, "api", 4.0, "a != c");

    let false_filter = engine
        .query_instant("tenant-a", "a == on (x) c", 10_000)
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = false_filter else {
        panic!("expected vector");
    };
    assert!(samples.is_empty());

    let bool_result = engine
        .query_instant("tenant-a", "a == bool on (x) c", 10_000)
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = bool_result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job").is_none());
    check!(samples[0].labels.get("x") == Some("1"));
    check!(approx_eq(float_value(&samples[0].value), 0.0));

    let invalid = engine
        .query_instant("tenant-a", "a > bool on (x) b", 10_000)
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = invalid else {
        panic!("expected vector");
    };
    assert!(samples.is_empty());
}

#[tokio::test]
async fn vector_vector_arithmetic_scales_native_histograms_with_matched_floats() {
    let mut histogram = native_histogram(4.0, 10.0);
    histogram.zero_count = 1.0;
    histogram.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 1,
    }];
    histogram.positive_counts = vec![3.0];

    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "duration"), ("job", "api"), ("x", "1")]),
        10_000,
        histogram,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "factor"), ("job", "api"), ("x", "1")]),
        10_000,
        2.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected_count, expected_sum) in [
        ("duration * on (x) factor", 8.0, 20.0),
        ("factor * on (x) duration", 8.0, 20.0),
        ("duration / on (x) factor", 2.0, 5.0),
    ] {
        let count = engine
            .query_instant("tenant-a", &format!("histogram_count({query})"), 10_000)
            .await
            .unwrap();
        let sum = engine
            .query_instant("tenant-a", &format!("histogram_sum({query})"), 10_000)
            .await
            .unwrap();

        assert_single_on_x_float_sample(&count, expected_count, query);
        assert_single_on_x_float_sample(&sum, expected_sum, query);
    }

    let invalid = engine
        .query_instant(
            "tenant-a",
            "histogram_count(factor / on (x) duration)",
            10_000,
        )
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = invalid else {
        panic!("expected vector");
    };
    assert!(samples.is_empty());
}

#[tokio::test]
async fn vector_vector_group_left_carries_labels_from_one_side() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 100.0), ("b", 50.0)] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "http_requests_total"),
                ("job", "api"),
                ("instance", instance),
            ]),
            10_000,
            value,
        );
    }
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "target_info"),
            ("job", "api"),
            ("region", "east"),
        ]),
        10_000,
        10.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "http_requests_total / on (job) group_left(region) target_info",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert!(samples.len() == 2);
    for sample in samples {
        check!(sample.labels.get("__name__").is_none());
        check!(sample.labels.get("job") == Some("api"));
        check!(sample.labels.get("region") == Some("east"));
    }
}

#[tokio::test]
async fn vector_vector_group_left_fill_right_preserves_unmatched_many_side() {
    let mut store = InMemoryMetricStore::new();
    for (job, instance, value) in [
        ("api", "a", 100.0),
        ("api", "b", 50.0),
        ("worker", "c", 7.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "http_requests_total"),
                ("job", job),
                ("instance", instance),
            ]),
            10_000,
            value,
        );
    }
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "target_info"),
            ("job", "api"),
            ("region", "east"),
        ]),
        10_000,
        10.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "http_requests_total + on (job) group_left(region) fill_right(0) target_info",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    let values = samples
        .iter()
        .map(|sample| {
            (
                sample.labels.get("instance").expect("instance label"),
                (sample.labels.get("region"), float_value(&sample.value)),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(values.len(), 3);
    assert_eq!(values["a"].0, Some("east"));
    assert!(approx_eq(values["a"].1, 110.0));
    assert_eq!(values["b"].0, Some("east"));
    assert!(approx_eq(values["b"].1, 60.0));
    assert_eq!(values["c"].0, None);
    assert!(approx_eq(values["c"].1, 7.0));
}

#[tokio::test]
async fn vector_vector_group_right_fill_left_preserves_unmatched_many_side() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "job_quota"),
            ("job", "api"),
            ("region", "east"),
        ]),
        10_000,
        10.0,
    );
    for (job, instance, value) in [
        ("api", "a", 100.0),
        ("api", "b", 50.0),
        ("worker", "c", 7.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "http_requests_total"),
                ("job", job),
                ("instance", instance),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "job_quota + on (job) group_right(region) fill_left(0) http_requests_total",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    let values = samples
        .iter()
        .map(|sample| {
            (
                sample.labels.get("instance").expect("instance label"),
                (sample.labels.get("region"), float_value(&sample.value)),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(values.len(), 3);
    assert_eq!(values["a"].0, Some("east"));
    assert!(approx_eq(values["a"].1, 110.0));
    assert_eq!(values["b"].0, Some("east"));
    assert!(approx_eq(values["b"].1, 60.0));
    assert_eq!(values["c"].0, None);
    assert!(approx_eq(values["c"].1, 7.0));
}

#[tokio::test]
async fn info_function_adds_target_info_data_labels_by_job_and_instance() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "http_requests_total"),
            ("job", "api"),
            ("instance", "a"),
        ]),
        10_000,
        7.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "target_info"),
            ("job", "api"),
            ("instance", "a"),
            ("region", "east"),
            ("cluster", "prod"),
        ]),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "info(http_requests_total)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__") == Some("http_requests_total"));
    check!(samples[0].labels.get("job") == Some("api"));
    check!(samples[0].labels.get("instance") == Some("a"));
    check!(samples[0].labels.get("region") == Some("east"));
    check!(samples[0].labels.get("cluster") == Some("prod"));
    check!(approx_eq(float_value(&samples[0].value), 7.0));
}

#[tokio::test]
async fn info_function_uses_data_label_selector_to_filter_and_copy_labels() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "http_requests_total"),
            ("job", "api"),
            ("instance", "a"),
        ]),
        10_000,
        7.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "target_info"),
            ("job", "api"),
            ("instance", "a"),
            ("region", "east"),
            ("cluster", "prod"),
        ]),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            r#"info(http_requests_total, {region="east"})"#,
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert_eq!(samples.len(), 1);
    assert_eq!(
        samples[0].labels.clone(),
        labels(&[
            ("__name__", "http_requests_total"),
            ("job", "api"),
            ("instance", "a"),
            ("region", "east"),
        ])
    );
    assert!(approx_eq(float_value(&samples[0].value), 7.0));
}

#[tokio::test]
async fn info_function_drops_series_when_required_data_label_selector_does_not_match() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "http_requests_total"),
            ("job", "api"),
            ("instance", "a"),
        ]),
        10_000,
        7.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "target_info"),
            ("job", "api"),
            ("instance", "a"),
            ("region", "east"),
        ]),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            r#"info(http_requests_total, {region="west"})"#,
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert!(samples.is_empty());
}

#[tokio::test]
async fn info_function_keeps_base_label_when_info_label_overlaps() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "http_requests_total"),
            ("job", "api"),
            ("instance", "a"),
            ("region", "base"),
        ]),
        10_000,
        7.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "target_info"),
            ("job", "api"),
            ("instance", "a"),
            ("region", "info"),
            ("cluster", "prod"),
        ]),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "info(http_requests_total)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].labels.get("region"), Some("base"));
    assert_eq!(samples[0].labels.get("cluster"), Some("prod"));
}

#[tokio::test]
async fn info_function_uses_named_info_metric_selector() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "http_requests_total"),
            ("job", "api"),
            ("instance", "a"),
        ]),
        10_000,
        7.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "build_info"),
            ("job", "api"),
            ("instance", "a"),
            ("version", "1.2.3"),
        ]),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            r#"info(http_requests_total, {__name__="build_info"})"#,
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].labels.get("version"), Some("1.2.3"));
}

#[tokio::test]
async fn info_function_merges_data_labels_from_multiple_info_metrics() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "http_requests_total"),
            ("job", "api"),
            ("instance", "a"),
        ]),
        10_000,
        7.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "target_info"),
            ("job", "api"),
            ("instance", "a"),
            ("cluster", "prod"),
        ]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "build_info"),
            ("job", "api"),
            ("instance", "a"),
            ("version", "1.2.3"),
        ]),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            r#"info(http_requests_total, {__name__=~".+_info"})"#,
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].labels.get("cluster"), Some("prod"));
    assert_eq!(samples[0].labels.get("version"), Some("1.2.3"));
}

#[tokio::test]
async fn vector_vector_group_right_carries_labels_from_one_side() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "target_limit"),
            ("job", "api"),
            ("region", "east"),
        ]),
        10_000,
        100.0,
    );
    for (instance, value) in [("a", 10.0), ("b", 25.0)] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "http_requests_total"),
                ("job", "api"),
                ("instance", instance),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "target_limit / on (job) group_right(region) http_requests_total",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 2);
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__").is_none()
            && sample.labels.get("job") == Some("api")
            && sample.labels.get("region") == Some("east")
            && sample.labels.get("instance") == Some("a")
            && approx_eq(float_value(&sample.value), 10.0)
    }));
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__").is_none()
            && sample.labels.get("job") == Some("api")
            && sample.labels.get("region") == Some("east")
            && sample.labels.get("instance") == Some("b")
            && approx_eq(float_value(&sample.value), 4.0)
    }));
}

#[tokio::test]
async fn comparison_bool_returns_one_or_zero() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "a"), ("x", "1")]),
        10_000,
        10.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "a > bool 0", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(approx_eq(float_value(&samples[0].value), 1.0));
}

#[tokio::test]
async fn comparison_without_bool_filters_false_samples() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "a"), ("x", "1")]),
        10_000,
        10.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "a > 100", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert!(samples.is_empty());
}

#[tokio::test]
async fn vector_and_keeps_left_samples_with_matching_right_key() {
    let engine = PromqlEngine::new(Arc::new(set_op_store()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "up and on (instance) target_info", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__") == Some("up"));
    check!(samples[0].labels.get("instance") == Some("b"));
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 2.0));
}

#[tokio::test]
async fn vector_and_default_matching_ignores_metadata_labels() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "requests_total"),
            ("__type__", "counter"),
            ("__unit__", "requests"),
            ("instance", "a"),
        ]),
        10_000,
        10.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "(requests_total + 1) and requests_total",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("__type__").is_none());
    check!(samples[0].labels.get("__unit__").is_none());
    check!(samples[0].labels.get("instance") == Some("a"));
    check!(approx_eq(float_value(&samples[0].value), 11.0));
}

#[tokio::test]
async fn vector_unless_keeps_left_samples_without_matching_right_key() {
    let engine = PromqlEngine::new(Arc::new(set_op_store()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "up unless on (instance) target_info", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__") == Some("up"));
    check!(samples[0].labels.get("instance") == Some("a"));
    check!(approx_eq(float_value(&samples[0].value), 1.0));
}

#[tokio::test]
async fn vector_or_returns_left_union_unmatched_right_samples() {
    let engine = PromqlEngine::new(Arc::new(set_op_store()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "up or on (instance) target_info", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 3);
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("up") && sample.labels.get("instance") == Some("a")
    }));
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("up") && sample.labels.get("instance") == Some("b")
    }));
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("target_info")
            && sample.labels.get("instance") == Some("c")
            && sample.labels.get("region") == Some("east")
            && approx_eq(float_value(&sample.value), 30.0)
    }));
}

#[tokio::test]
async fn instant_rate_extrapolates_counter_window() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [
        (0_i64, 0.0),
        (60_000, 1.0),
        (120_000, 2.0),
        (180_000, 3.0),
        (240_000, 4.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "http_requests_total"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "rate(http_requests_total[5m])", 300_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 5.0 / 300.0));
}

#[tokio::test]
async fn range_rate_uses_each_step_as_window_end() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [
        (0_i64, 0.0),
        (60_000, 1.0),
        (120_000, 2.0),
        (180_000, 3.0),
        (240_000, 4.0),
        (300_000, 5.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "http_requests_total"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_range(
            "tenant-a",
            "rate(http_requests_total[5m])",
            240_000,
            300_000,
            60_000,
        )
        .await
        .unwrap();

    let QueryResult::RangeMatrix(series) = result else {
        panic!("expected matrix");
    };
    check!(series.len() == 1);
    check!(series[0].samples.len() == 2);
    for (sample, (want_ts, want)) in series[0]
        .samples
        .iter()
        .zip([(240_000, 4.0 / 300.0), (300_000, 5.0 / 300.0)])
    {
        check!(sample.0 == want_ts);
        check!(approx_eq(float_value(&sample.1), want), "at ts {want_ts}");
    }
}

#[tokio::test]
async fn range_selector_at_start_and_end_use_query_bounds() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(60_000_i64, 1.0), (120_000, 2.0), (180_000, 3.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [("up @ start()", 1.0), ("up @ end()", 3.0)] {
        let result = engine
            .query_range("tenant-a", query, 60_000, 180_000, 60_000)
            .await
            .unwrap();

        let QueryResult::RangeMatrix(series) = result else {
            panic!("expected matrix");
        };
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].samples.len(), 3);
        for (ts_ms, value) in &series[0].samples {
            assert!([60_000, 120_000, 180_000].contains(ts_ms));
            assert!(approx_eq(float_value(value), expected));
        }
    }
}

#[tokio::test]
async fn instant_increase_corrects_counter_resets() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(0_i64, 1.0), (60_000, 2.0), (120_000, 1.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "http_requests_total"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "increase(http_requests_total[2m])", 120_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert_eq!(samples.len(), 1);
    assert!(approx_eq(float_value(&samples[0].value), 2.0));
}

#[tokio::test]
async fn instant_delta_is_gauge_delta_without_reset_correction() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(30_000_i64, 4.0), (60_000, 3.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "temperature_celsius"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "delta(temperature_celsius[1m])", 60_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert_eq!(samples.len(), 1);
    assert!(approx_eq(float_value(&samples[0].value), -2.0));
}

#[tokio::test]
async fn instant_changes_counts_value_transitions_in_range() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [
        (0_i64, 1.0),
        (60_000, 1.0),
        (120_000, 2.0),
        (180_000, 2.0),
        (240_000, 5.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "queue_depth"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "changes(queue_depth[4m])", 240_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 2.0));
}

#[tokio::test]
async fn instant_resets_counts_counter_decreases_in_range() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [
        (0_i64, 0.0),
        (60_000, 5.0),
        (120_000, 1.0),
        (180_000, 4.0),
        (240_000, 2.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "http_requests_total"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "resets(http_requests_total[4m])", 240_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 2.0));
}

#[tokio::test]
async fn instant_irate_uses_last_two_samples_per_second() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(0_i64, 0.0), (60_000, 1.0), (90_000, 3.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "http_requests_total"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "irate(http_requests_total[2m])", 90_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert_eq!(samples.len(), 1);
    assert!(approx_eq(float_value(&samples[0].value), 2.0 / 30.0));
}

#[tokio::test]
async fn instant_idelta_uses_last_two_samples_without_per_second_division() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(0_i64, 0.0), (60_000, 1.0), (90_000, 3.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "temperature_celsius"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "idelta(temperature_celsius[2m])", 90_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert_eq!(samples.len(), 1);
    assert!(approx_eq(float_value(&samples[0].value), 2.0));
}

#[tokio::test]
async fn instant_deriv_returns_gauge_slope_per_second() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(0_i64, 1.0), (60_000, 3.0), (120_000, 5.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "temperature_celsius"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "deriv(temperature_celsius[2m])", 120_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 2.0 / 60.0));
}

#[tokio::test]
async fn instant_predict_linear_extrapolates_gauge_series() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(0_i64, 1.0), (60_000, 3.0), (120_000, 5.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "disk_free_bytes"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "predict_linear(disk_free_bytes[2m], 60)",
            120_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 7.0));
}

#[cfg(not(feature = "experimental-functions"))]
#[tokio::test]
async fn instant_double_exponential_smoothing_requires_experimental_feature() {
    let store = InMemoryMetricStore::new();
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let error = engine
        .query_instant(
            "tenant-a",
            "double_exponential_smoothing(gauge[5m], 0.5, 0.5)",
            120_000,
        )
        .await
        .unwrap_err();

    assert!(matches!(error, PromqlError::Unsupported(_)));
    assert!(format!("{error}").contains("experimental-functions"));
}

#[cfg(not(feature = "experimental-functions"))]
#[tokio::test]
async fn instant_duration_expression_helpers_require_experimental_feature() {
    let store = InMemoryMetricStore::new();
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for query in ["range()", "step()", "start()", "end()"] {
        let error = engine
            .query_instant("tenant-a", query, 120_000)
            .await
            .unwrap_err();

        assert!(matches!(error, PromqlError::Unsupported(_)), "{query}");
        assert!(
            format!("{error}").contains("experimental-functions"),
            "{query}: {error}"
        );
    }
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
async fn instant_duration_expression_helpers_return_zero() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());

    for query in ["range()", "step()", "start()", "end()"] {
        let result = engine
            .query_instant("tenant-a", query, 120_000)
            .await
            .unwrap();

        assert!(
            result
                == QueryResult::Scalar {
                    ts_ms: 120_000,
                    value: 0.0,
                },
            "{query}"
        );
    }
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
async fn range_duration_expression_helpers_return_query_range_and_step_seconds() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());

    for (query, expected) in [
        ("range()", 120.0),
        ("step()", 30.0),
        ("start()", 60.0),
        ("end()", 180.0),
    ] {
        let result = engine
            .query_range("tenant-a", query, 60_000, 180_000, 30_000)
            .await
            .unwrap();

        let QueryResult::RangeMatrix(series) = result else {
            panic!("expected matrix");
        };
        assert_eq!(series.len(), 1, "{query}");
        assert_eq!(series[0].labels.len(), 0, "{query}");
        assert_eq!(
            series[0]
                .samples
                .iter()
                .map(|(_, value)| float_value(value))
                .collect::<Vec<_>>(),
            vec![expected; 5],
            "{query}"
        );
    }
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
async fn instant_double_exponential_smoothing_smooths_gauge_series() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [
        (0_i64, 3.0),
        (60_000, 6.0),
        (120_000, 12.0),
        (180_000, 21.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "queue_depth"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "double_exponential_smoothing(queue_depth[4m], 0.5, 0.5)",
            180_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 17.625));
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
async fn instant_double_exponential_smoothing_validates_factors() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(0_i64, 3.0), (60_000, 6.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "queue_depth"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let error = engine
        .query_instant(
            "tenant-a",
            "double_exponential_smoothing(queue_depth[2m], 1, 0.5)",
            60_000,
        )
        .await
        .unwrap_err();

    assert!(matches!(error, PromqlError::Plan(_)));
    assert!(format!("{error}").contains("smoothing factor"));
}

#[tokio::test]
async fn instant_basic_over_time_functions_reduce_range_samples() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(0_i64, 1.0), (60_000, 3.0), (120_000, 5.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "queue_depth"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected, preserves_name) in [
        ("sum_over_time(queue_depth[2m])", 8.0, false),
        ("avg_over_time(queue_depth[2m])", 4.0, false),
        ("count_over_time(queue_depth[2m])", 2.0, false),
        ("min_over_time(queue_depth[2m])", 3.0, false),
        ("max_over_time(queue_depth[2m])", 5.0, false),
        ("first_over_time(queue_depth[2m])", 3.0, true),
        ("last_over_time(queue_depth[2m])", 5.0, true),
        ("present_over_time(queue_depth[2m])", 1.0, false),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 120_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        if preserves_name {
            assert!(samples[0].labels.get("__name__") == Some("queue_depth"));
        } else {
            assert!(samples[0].labels.get("__name__").is_none());
        }
        assert_eq!(samples[0].labels.get("job"), Some("api"));
        assert!(approx_eq(float_value(&samples[0].value), expected));
    }
}

#[tokio::test]
async fn instant_count_and_present_over_time_include_native_histograms() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, sum) in [(60_000, 10.0), (120_000, 20.0)] {
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
            ts_ms,
            native_histogram(4.0, sum),
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        ("count_over_time(request_duration_seconds[2m])", 2.0),
        ("present_over_time(request_duration_seconds[2m])", 1.0),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 120_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        check!(samples.len() == 1, "{query}");
        check!(samples[0].labels.get("__name__").is_none(), "{query}");
        check!(samples[0].labels.get("job") == Some("api"), "{query}");
        check!(
            approx_eq(float_value(&samples[0].value), expected),
            "{query}"
        );
    }
}

#[tokio::test]
async fn instant_first_and_last_over_time_return_native_histograms() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, sum) in [(60_000, 10.0), (120_000, 20.0)] {
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
            ts_ms,
            native_histogram(4.0, sum),
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        (
            "histogram_sum(first_over_time(request_duration_seconds[2m]))",
            10.0,
        ),
        (
            "histogram_sum(last_over_time(request_duration_seconds[2m]))",
            20.0,
        ),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 120_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        check!(samples.len() == 1, "{query}");
        check!(samples[0].labels.get("__name__").is_none(), "{query}");
        check!(samples[0].labels.get("job") == Some("api"), "{query}");
        check!(
            approx_eq(float_value(&samples[0].value), expected),
            "{query}"
        );
    }
}

#[tokio::test]
async fn instant_ts_of_over_time_functions_return_sample_timestamps_seconds() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [
        (0_i64, 10.0),
        (60_000, 3.0),
        (120_000, 7.0),
        (180_000, 3.0),
        (240_000, 11.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "queue_depth"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        ("ts_of_first_over_time(queue_depth[4m])", 60.0),
        ("ts_of_last_over_time(queue_depth[4m])", 240.0),
        ("ts_of_min_over_time(queue_depth[4m])", 180.0),
        ("ts_of_max_over_time(queue_depth[4m])", 240.0),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 240_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        check!(samples.len() == 1);
        check!(samples[0].labels.get("__name__").is_none());
        check!(samples[0].labels.get("job") == Some("api"));
        check!(approx_eq(float_value(&samples[0].value), expected));
    }
}

#[tokio::test]
async fn instant_absent_returns_one_with_equality_matcher_labels_when_vector_is_empty() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            r#"absent(up{job="worker",instance=~".*"})"#,
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("worker"));
    check!(samples[0].labels.get("instance").is_none());
    check!(approx_eq(float_value(&samples[0].value), 1.0));
}

#[tokio::test]
async fn instant_absent_with_or_matchers_returns_unlabeled_absence_sample() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", r#"absent(up{job="api" or job="web"})"#, 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.is_empty());
    check!(approx_eq(float_value(&samples[0].value), 1.0));
}

#[tokio::test]
async fn instant_absent_over_time_returns_one_when_range_is_empty() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            r#"absent_over_time(up{job="api"}[1m])"#,
            120_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 1.0));
}

#[tokio::test]
async fn instant_absent_over_time_with_or_matchers_returns_unlabeled_absence_sample() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            r#"absent_over_time(up{job="api" or job="web"}[1m])"#,
            120_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.is_empty());
    check!(approx_eq(float_value(&samples[0].value), 1.0));
}

#[tokio::test]
async fn instant_absent_over_time_treats_native_histograms_as_present() {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
        90_000,
        native_histogram(4.0, 10.0),
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            r#"absent_over_time(request_duration_seconds{job="api"}[1m])"#,
            120_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert!(samples.is_empty());
}

#[tokio::test]
async fn instant_time_returns_evaluation_timestamp_seconds() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "time()", 123_456)
        .await
        .unwrap();

    assert!(
        result
            == QueryResult::Scalar {
                ts_ms: 123_456,
                value: 123.456
            }
    );
}

#[tokio::test]
async fn instant_timestamp_returns_sample_timestamp_seconds() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        60_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "timestamp(up)", 120_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(samples[0].ts_ms == 120_000);
    check!(approx_eq(float_value(&samples[0].value), 60.0));
}

#[tokio::test]
async fn instant_statistical_over_time_functions_reduce_range_samples() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [
        (0_i64, 2.0),
        (60_000, 4.0),
        (120_000, 4.0),
        (180_000, 4.0),
        (240_000, 5.0),
        (300_000, 5.0),
        (360_000, 7.0),
        (420_000, 9.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "latency_seconds"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        ("stdvar_over_time(latency_seconds[8m])", 4.0),
        ("stddev_over_time(latency_seconds[8m])", 2.0),
        ("quantile_over_time(0.5, latency_seconds[8m])", 4.5),
        ("mad_over_time(latency_seconds[8m])", 0.5),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 420_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        check!(samples.len() == 1);
        check!(samples[0].labels.get("__name__").is_none());
        check!(samples[0].labels.get("job") == Some("api"));
        check!(approx_eq(float_value(&samples[0].value), expected));
    }
}

#[tokio::test]
async fn unary_minus_negates_scalar_expression() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "-(2 * 3)", 10_000)
        .await
        .unwrap();
    assert!(
        result
            == QueryResult::Scalar {
                ts_ms: 10_000,
                value: -6.0
            }
    );
}

#[tokio::test]
async fn unary_minus_negates_vector_values_and_drops_metric_name() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "temperature_celsius"), ("job", "api")]),
        10_000,
        3.5,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "-temperature_celsius", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), -3.5));
}

#[tokio::test]
async fn unary_minus_negates_native_histogram_values_and_marks_gauge() {
    let mut histogram = native_histogram(4.0, 10.0);
    histogram.reset_hint = ResetHint::No;
    histogram.zero_count = 1.0;
    histogram.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 1,
    }];
    histogram.positive_counts = vec![3.0];

    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
        10_000,
        histogram,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "-request_duration_seconds", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    let SampleValue::Histogram(histogram) = &samples[0].value else {
        panic!("expected histogram");
    };
    check!(histogram.reset_hint == ResetHint::Gauge);
    check!(approx_eq(histogram.count, -4.0));
    check!(approx_eq(histogram.sum, -10.0));
    check!(approx_eq(histogram.zero_count, -1.0));
    check!(
        histogram
            .positive_counts
            .iter()
            .any(|count| approx_eq(*count, -3.0))
    );
}

#[tokio::test]
async fn range_selector_returns_samples_in_each_step_window() {
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels(&[("__name__", "up")]), 0, 0.0);
    store.push_float("tenant-a", labels(&[("__name__", "up")]), 60_000, 1.0);
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up")]),
        90_000,
        stale_nan(),
    );
    store.push_float("tenant-a", labels(&[("__name__", "up")]), 120_000, 2.0);
    store.push_float("tenant-a", labels(&[("__name__", "up")]), 180_000, 3.0);

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_range("tenant-a", "up[2m]", 120_000, 180_000, 60_000)
        .await
        .unwrap();

    let QueryResult::RangeMatrix(series) = result else {
        panic!("expected matrix");
    };
    check!(series.len() == 1);
    check!(series[0].samples.len() == 3);
    check!(series[0].samples[0].0 == 60_000);
    check!(series[0].samples[2].0 == 180_000);
    check!(series[0].samples.iter().all(|(_, value)| {
        let SampleValue::Float(value) = value else {
            return false;
        };
        value.to_bits() != stale_nan().to_bits()
    }));
}

#[tokio::test]
async fn range_query_accepts_parenthesized_expression() {
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels(&[("__name__", "up")]), 0, 0.0);
    store.push_float("tenant-a", labels(&[("__name__", "up")]), 60_000, 1.0);
    store.push_float("tenant-a", labels(&[("__name__", "up")]), 120_000, 2.0);

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_range("tenant-a", "(up)", 0, 120_000, 60_000)
        .await
        .unwrap();

    let QueryResult::RangeMatrix(series) = result else {
        panic!("expected matrix");
    };
    check!(series.len() == 1);
    check!(series[0].samples.len() == 3);
    for (sample, (want_ts, want)) in
        series[0]
            .samples
            .iter()
            .zip([(0, 0.0), (60_000, 1.0), (120_000, 2.0)])
    {
        check!(sample.0 == want_ts);
        check!(approx_eq(float_value(&sample.1), want), "at ts {want_ts}");
    }
}

#[tokio::test]
async fn range_selector_offset_shifts_matrix_window_backwards() {
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels(&[("__name__", "up")]), 0, 0.0);
    store.push_float("tenant-a", labels(&[("__name__", "up")]), 60_000, 1.0);
    store.push_float("tenant-a", labels(&[("__name__", "up")]), 120_000, 2.0);
    store.push_float("tenant-a", labels(&[("__name__", "up")]), 180_000, 3.0);

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "up[2m] offset 1m", 180_000)
        .await
        .unwrap();

    let QueryResult::RangeMatrix(series) = result else {
        panic!("expected matrix");
    };
    check!(series.len() == 1);
    check!(series[0].samples.len() == 2);
    for (sample, (want_ts, want)) in series[0]
        .samples
        .iter()
        .zip([(60_000, 1.0), (120_000, 2.0)])
    {
        check!(sample.0 == want_ts);
        check!(approx_eq(float_value(&sample.1), want), "at ts {want_ts}");
    }
}

#[tokio::test]
async fn instant_subquery_evaluates_expression_at_explicit_steps() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(0_i64, 1.0), (60_000, 2.0), (120_000, 3.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "queue_depth"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "(queue_depth * 2)[2m:1m]", 120_000)
        .await
        .unwrap();

    let QueryResult::RangeMatrix(series) = result else {
        panic!("expected matrix");
    };
    check!(series.len() == 1);
    check!(series[0].labels.get("__name__").is_none());
    check!(series[0].labels.get("job") == Some("api"));
    check!(series[0].samples.len() == 3);
    for (sample, (want_ts, want)) in
        series[0]
            .samples
            .iter()
            .zip([(0, 2.0), (60_000, 4.0), (120_000, 6.0)])
    {
        check!(sample.0 == want_ts);
        check!(approx_eq(float_value(&sample.1), want), "at ts {want_ts}");
    }
}

#[tokio::test]
async fn instant_subquery_uses_global_eval_interval_when_step_is_omitted() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(0_i64, 1.0), (30_000, 2.0), (60_000, 3.0), (90_000, 4.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "queue_depth"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(
        Arc::new(store),
        EngineOpts {
            eval_interval_ms: 30_000,
            ..EngineOpts::default()
        },
    );
    let result = engine
        .query_instant("tenant-a", "queue_depth[90s:]", 90_000)
        .await
        .unwrap();

    let QueryResult::RangeMatrix(series) = result else {
        panic!("expected matrix");
    };
    check!(series.len() == 1);
    let timestamps = series[0]
        .samples
        .iter()
        .map(|(ts_ms, _)| *ts_ms)
        .collect::<Vec<_>>();
    check!(timestamps == [0, 30_000, 60_000, 90_000]);
}

#[tokio::test]
async fn instant_subquery_aligns_start_to_step_grid() {
    let mut store = InMemoryMetricStore::new();
    for (index, value) in [
        1.0, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, 34.0, 55.0, 89.0, 144.0,
    ]
    .into_iter()
    .enumerate()
    {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "metric_total")]),
            i64::try_from(index).unwrap() * 7_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "rate(metric_total[1m500ms:10s])", 80_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert_eq!(samples.len(), 1);
    assert!(approx_eq(
        float_value(&samples[0].value),
        2.366_666_666_666_666_7,
    ));
}

#[tokio::test]
async fn instant_over_time_accepts_subquery_range_argument() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(0_i64, 1.0), (60_000, 2.0), (120_000, 3.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "queue_depth"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "avg_over_time((queue_depth * 2)[2m:1m])",
            120_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 5.0));
}

/// Compare two `RangeMatrix` results for the range parity test, bit-exact on
/// float sample values so a genuine NaN equals a genuine NaN (plain
/// `PartialEq` would spuriously fail `NaN == NaN`). Series order, labelsets,
/// per-step timestamps, and gaps must all match.
fn range_matrices_match(left: &QueryResult, right: &QueryResult) -> bool {
    let (QueryResult::RangeMatrix(left), QueryResult::RangeMatrix(right)) = (left, right) else {
        return false;
    };
    if left.len() != right.len() {
        return false;
    }
    left.iter().zip(right.iter()).all(|(l, r)| {
        l.labels == r.labels
            && l.samples.len() == r.samples.len()
            && l.samples.iter().zip(r.samples.iter()).all(|(lp, rp)| {
                lp.0 == rp.0
                    && match (&lp.1, &rp.1) {
                        (SampleValue::Float(a), SampleValue::Float(b)) => {
                            a.to_bits() == b.to_bits()
                        }
                        (a, b) => a == b,
                    }
            })
    })
}

/// Differential parity: a range query routed through the per-step operator
/// planner must produce the byte-exact `RangeMatrix` the interpreter range
/// path produces — same series (which appear, in what order), labelsets,
/// per-step `(t, value)` points, gaps, and a scalar-over-range shape.
/// Lock in which corpus-shaped range expressions route through the operator
/// planner vs fall back to the interpreter, so the gate's coverage is
/// explicit and regressions are caught.
#[test]
fn range_planner_gate_routes_expected_shapes() {
    use promql_parser::parser::Expr;

    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let routes = |query: &str| -> bool {
        let expr = parse_promql_with_duration_context(
            query,
            DurationExprContext::range(0, 120_000, 60_000),
        )
        .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let mut probe = &expr;
        while let Expr::Paren(paren) = probe {
            probe = &paren.expr;
        }
        super::range_expr_routes_through_planner(probe)
    };

    // Plannable range shapes that now flow through the per-step operators.
    for query in [
        "rate(bar[30s])",
        "sum_over_time(bar[30s])",
        "requests * 2",
        "foo > 2 or bar",
        "abs(metric)",
        "sum by(job)(metric)",
        "label_replace(metric, \"l\", \"v\", \"\", \"\")",
        // Aggregations over a rate / `*_over_time` range call now route
        // through the planner: the UDF emits NULL (not a NaN sentinel) for a
        // no-value window, the aggregate planner drops those NULL rows before
        // grouping, and the aggregates skip NULL — matching the interpreter,
        // which omits no-value series before aggregating.
        "sum(rate(bar[30s]))",
        "avg by(job)(rate(bar[30s]))",
        "max without(path)(increase(bar[2m]))",
        "count(avg_over_time(bar[1m]))",
        // Parameterized aggregations over a plannable float inner now recurse
        // the inner vector and apply the shared interpreter routine per step
        // (a `Precomputed` result), so they route through the planner too.
        "topk(1, metric)",
        "bottomk(2, metric) by(job)",
        "quantile(0.9, metric)",
        "count_values(\"v\", metric)",
        "stddev by(job)(metric)",
        "stdvar(metric)",
        // A range/`*_over_time` call whose argument is a SUBQUERY now routes
        // through the planner: the subquery's sub-grid is evaluated per-step
        // through the recursive planner and the shared outer fold is applied.
        "avg_over_time(bar[5m:30s])",
        "rate(sum_over_time(bar[30s:10s])[2m:30s])",
        // A subquery whose inner is a unary negation now routes too:
        // `Expr::Unary` is planner-supported, so the subquery's structural
        // gate accepts it.
        "avg_over_time((-bar)[5m:30s])",
        // A param aggregation over a plannable subquery-range inner routes
        // through the planner too (the inner subquery is plannable).
        "topk(1, max_over_time(metric[5m:1m]))",
        // `sort_by_label` / `sort_by_label_desc` now route through the planner.
        "sort_by_label(metric, \"job\")",
        "sort_by_label_desc(metric, \"job\")",
        // The experimental `*_over_time` members route through the shared kernel.
        "mad_over_time(metric[5m])",
        "first_over_time(metric[5m])",
        "ts_of_max_over_time(metric[5m])",
        // `info(v [, selector])` routes through the planner (the input vector is
        // plannable; the join is the shared kernel).
        "info(metric)",
        "info(metric, {__name__=\"target_info\"})",
        // A bare top-level instant-vector selector now routes through the
        // planner: the interpreter range path is fixed to the left-OPEN
        // lookback, agreeing with the operator selector chain (and Prometheus).
        "metric",
        // A top-level scalar-typed expression now routes too: both range paths
        // fold an identical no-label scalar series per step, so the operator
        // driver matches the interpreter byte-for-byte.
        "42",
        "1 + 2",
        "time()",
        // A bare selector with `@ start()`/`@ end()` now routes too: the
        // per-step planner driver scopes the query's `[start, end]` bounds in
        // `AT_MODIFIER_BOUNDS`, and `plan_instant_selector` resolves those
        // modifiers to the range bounds, matching the interpreter's dedicated
        // `eval_vector_selector_over_steps`.
        "metric @ start()",
        "metric @ end()",
    ] {
        assert!(
            routes(query),
            "expected `{query}` to route through the planner"
        );
    }

    // A raw matrix selector is a range-vector shape owned by the interpreter's
    // dedicated matrix range path (not per-step plannable), so the gate keeps
    // it on the interpreter.
    assert!(
        !routes("bar[30s]"),
        "expected `bar[30s]` to stay on the interpreter"
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn range_planner_path_matches_interpreter() {
    use promql_parser::parser::Expr;

    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();
    let stale_bits = stale_nan();
    // Two counters (for rate and sum-by-rate), grouped by `group`.
    for (lbls, samples) in [
        (
            labels(&[
                ("__name__", "http_requests_total"),
                ("job", "api"),
                ("group", "a"),
            ]),
            vec![
                (0_i64, 0.0),
                (60_000, 1.0),
                (120_000, 3.0),
                (180_000, 6.0),
                (240_000, 10.0),
                (300_000, 15.0),
            ],
        ),
        (
            labels(&[
                ("__name__", "http_requests_total"),
                ("job", "db"),
                ("group", "a"),
            ]),
            vec![
                (0, 0.0),
                (60_000, 2.0),
                (120_000, 4.0),
                (180_000, 8.0),
                (240_000, 16.0),
                (300_000, 32.0),
            ],
        ),
        (
            // group b: a full history so `rate` has a value at every step
            // (no no-value NaN sentinel), keeping the operator aggregate
            // over this rate parity-exact in the forced comparison below.
            labels(&[
                ("__name__", "http_requests_total"),
                ("job", "cache"),
                ("group", "b"),
            ]),
            vec![
                (0, 1.0),
                (60_000, 5.0),
                (120_000, 7.0),
                (180_000, 9.0),
                (240_000, 11.0),
                (300_000, 20.0),
            ],
        ),
    ] {
        for (ts, value) in samples {
            store.push_float("t", lbls.clone(), ts, value);
        }
    }
    // A second counter family with a single-sample (no-value rate) series, to
    // exercise the SPARSE aggregate-over-rate parity (group b's only member is
    // no-value, so it is excluded from the group on both paths).
    for (lbls, samples) in [
        (
            labels(&[("__name__", "spotty_total"), ("job", "api"), ("group", "a")]),
            vec![
                (0_i64, 0.0),
                (60_000, 1.0),
                (120_000, 2.0),
                (180_000, 3.0),
                (240_000, 4.0),
                (300_000, 5.0),
            ],
        ),
        (
            // Only one in-window sample at each step's 2m window: rate has no
            // value -> the operator rate emits NULL (not a NaN sentinel), the
            // aggregate planner drops it before grouping, and group b collapses
            // to no row at those steps — matching the interpreter, which omits
            // the no-value series. This drives the SPARSE aggregate-over-rate
            // parity proof below.
            labels(&[("__name__", "spotty_total"), ("job", "db"), ("group", "b")]),
            vec![(180_000, 100.0)],
        ),
    ] {
        for (ts, value) in samples {
            store.push_float("t", lbls.clone(), ts, value);
        }
    }
    // A plain gauge for a bare-selector range and a binary op.
    for (ts, value) in [
        (0_i64, 2.0),
        (60_000, 4.0),
        (120_000, 8.0),
        (180_000, 16.0),
        (240_000, 32.0),
        (300_000, 64.0),
    ] {
        store.push_float(
            "t",
            labels(&[("__name__", "gauge"), ("job", "api")]),
            ts,
            value,
        );
    }
    // A series whose mid-range latest in-window sample is a stale-NaN marker
    // (the series must vanish for the steps that select it) and whose later
    // sample is a genuine NaN (kept as a NaN value).
    for (ts, value) in [
        (0_i64, 1.0),
        (60_000, stale_bits),
        (120_000, 3.0),
        (180_000, f64::NAN),
        (240_000, 5.0),
    ] {
        store.push_float(
            "t",
            labels(&[("__name__", "spotty"), ("job", "api")]),
            ts,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let (start, end, step) = (0_i64, 300_000_i64, 60_000_i64);

    // Queries the production gate routes through the per-step operator
    // planner. For these, the gate must accept them and the planner-routed
    // public `query_range` must evaluate successfully; the byte-exact value
    // checks are pinned below (and across the conformance corpus).
    let planner_routed = [
        // A rate over a counter (per-step rate projection).
        "rate(http_requests_total[2m])",
        // A vector-scalar binary op.
        "gauge * 2",
        // A vector-vector binary op (one-to-one on job).
        "gauge + on(job) http_requests_total{job=\"api\"}",
        // A scalar-math call over a selector (preserves genuine NaN, gaps
        // the stale-marker steps).
        "abs(spotty - 10)",
        // A simple aggregate over a bare selector (no rate sentinel).
        "sum by(job)(gauge)",
        // Aggregation over a rate: the marquee fix. Every series is dense, so
        // no no-value NULL arises — pure parity with the interpreter.
        "sum by(group)(rate(http_requests_total[2m]))",
        // Aggregation over a rate where one group member is SPARSE (the
        // single-sample `spotty_total` series at job=db,group=b yields no rate
        // value across the early steps). The UDF emits NULL for those steps,
        // the aggregate planner drops them before grouping, and group b
        // collapses to no result row at the steps where its only member is
        // no-value — exactly as the interpreter omits the no-value series.
        // group a (dense) is unaffected. This is the headline divergence the
        // fix closes, proven byte-exact through the public range path.
        "sum by(group)(rate(spotty_total[2m]))",
        // Parameterized aggregations over a plannable inner now route through
        // the planner per step. `topk` selects original series each step (a
        // series can appear/disappear between steps, stitched by fingerprint);
        // `quantile`/`stddev` reduce per group per step. All must equal the
        // interpreter byte-for-byte across the step grid.
        "topk(2, rate(http_requests_total[2m]))",
        "quantile(0.5, gauge)",
        "stddev by(group)(rate(http_requests_total[2m]))",
        // A BARE top-level instant-vector selector now routes through the
        // planner: the interpreter range path is fixed to the left-OPEN
        // lookback, so the operator selector chain matches it (and Prometheus)
        // byte-for-byte, including the stale-marker gaps and genuine-NaN keep.
        "gauge",
        "spotty",
        // A top-level SCALAR-typed expression now routes too: both range paths
        // fold an identical no-label scalar series per step.
        "42",
        "time()",
        "1 + 2",
    ];
    for query in planner_routed {
        let expr =
            parse_promql_with_duration_context(query, DurationExprContext::range(start, end, step))
                .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let mut probe = &expr;
        while let Expr::Paren(paren) = probe {
            probe = &paren.expr;
        }
        assert!(
            super::range_expr_routes_through_planner(probe),
            "gate unexpectedly excludes `{query}` from the planner path"
        );
        // The public range path now routes these through the planner (the
        // only evaluation engine); it must evaluate without falling back.
        let planner = engine
            .query_range("t", query, start, end, step)
            .await
            .unwrap_or_else(|error| panic!("planner `{query}`: {error}"));
        assert!(
            matches!(planner, QueryResult::RangeMatrix(_)),
            "range query `{query}` did not yield a matrix: {planner:?}"
        );
    }

    // The only top-level range shape the gate keeps on the interpreter is a
    // raw matrix selector / subquery (a range-vector shape owned by the
    // dedicated matrix/subquery range path, not the per-step instant
    // planner). Assert the gate excludes it.
    for query in ["http_requests_total[2m]"] {
        let expr =
            parse_promql_with_duration_context(query, DurationExprContext::range(start, end, step))
                .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let mut probe = &expr;
        while let Expr::Paren(paren) = probe {
            probe = &paren.expr;
        }
        assert!(
            !super::range_expr_routes_through_planner(probe),
            "gate unexpectedly routes `{query}` through the planner"
        );
    }

    // The headline fix, proven directly on the SPARSE aggregate-over-rate.
    // `sum by(group)(rate(spotty_total[2m]))` over the full `[0, 300000]`
    // grid: group b's only member is a single-sample series, so its rate is
    // no-value (NULL) at every step. The rate UDF emits NULL for those steps,
    // the aggregate planner drops them before grouping, and group b collapses
    // to NO result row at all — only group a (dense) survives. (Before the
    // fix the operator path leaked a spurious NaN group-b row here.)
    let QueryResult::RangeMatrix(sparse) = engine
        .eval_range_via_planner_forced(
            "t",
            "sum by(group)(rate(spotty_total[2m]))",
            start,
            end,
            step,
        )
        .await
        .unwrap()
    else {
        panic!("expected matrix for the sparse aggregate-over-rate");
    };
    let sparse_groups: Vec<Option<&str>> = sparse
        .iter()
        .map(|series| series.labels.get("group"))
        .collect();
    assert_eq!(
        sparse_groups,
        vec![Some("a")],
        "no-value group b must be excluded, leaving only group a: {sparse:?}"
    );

    // Pin the stale-vs-genuine-NaN semantics the scalar-math `spotty` parity
    // relies on, via the forced planner path on `abs(spotty - 10)`.
    let QueryResult::RangeMatrix(series) = engine
        .eval_range_via_planner_forced("t", "spotty", start, end, step)
        .await
        .unwrap()
    else {
        panic!("expected matrix for `spotty`");
    };
    assert_eq!(series.len(), 1, "spotty series missing");
    // Steps (ms) -> selected latest-in-window value, lookback 5m:
    //   0 -> 1.0; 60k -> stale (DROPPED, no point); 120k -> 3.0;
    //   180k -> NaN (kept); 240k -> 5.0; 300k -> 5.0 (240k still in window).
    let points = &series[0].samples;
    let times: Vec<i64> = points.iter().map(|(t, _)| *t).collect();
    assert_eq!(
        times,
        vec![0, 120_000, 180_000, 240_000, 300_000],
        "stale-marker step not gapped: {times:?}"
    );
    let nan_point = points
        .iter()
        .find(|(t, _)| *t == 180_000)
        .expect("180k point");
    let SampleValue::Float(nan_value) = nan_point.1 else {
        panic!("expected float at 180k");
    };
    assert!(nan_value.is_nan(), "genuine NaN not kept at 180k");
    assert!(
        !super::is_stale_nan(nan_value),
        "genuine NaN reported as stale at 180k"
    );
}

#[tokio::test]
async fn instant_selector_planner_path_matches_interpreter() {
    use promql_parser::parser::Expr;

    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A small float-only store with multiple series, an empty-string-ish
    // label set, an offset-relevant history, a stale marker (job=db: its
    // latest in-window sample is a stale-NaN marker, so it must be DROPPED
    // on both paths), and a genuine-NaN series (job=nan: its latest
    // in-window sample is a genuine NaN, so it must be KEPT as a NaN value
    // on both paths).
    let mut store = InMemoryMetricStore::new();
    let stale_bits = stale_nan();
    for (lbls, ts, value) in [
        (labels(&[("__name__", "up"), ("job", "api")]), 0_i64, 1.0),
        (labels(&[("__name__", "up"), ("job", "api")]), 60_000, 2.0),
        (labels(&[("__name__", "up"), ("job", "api")]), 120_000, 3.0),
        (labels(&[("__name__", "up"), ("job", "db")]), 60_000, 9.0),
        (
            labels(&[("__name__", "up"), ("job", "db")]),
            120_000,
            stale_bits,
        ),
        // Genuine NaN as the latest in-window sample: kept as a NaN value.
        (labels(&[("__name__", "up"), ("job", "nan")]), 60_000, 5.0),
        (
            labels(&[("__name__", "up"), ("job", "nan")]),
            120_000,
            f64::NAN,
        ),
        (
            labels(&[("__name__", "down"), ("job", "api")]),
            120_000,
            7.0,
        ),
        (labels(&[("__name__", "lonely")]), 120_000, 42.0),
    ] {
        store.push_float("t", lbls, ts, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let selectors = [
        ("up", 120_000_i64),
        ("up{job=\"api\"}", 120_000),
        ("up{job=~\"a.*\"}", 120_000),
        ("up{job!=\"api\"}", 120_000),
        ("{__name__=~\".+\"}", 120_000),
        ("up offset 1m", 120_000),
        ("up @ 60", 120_000),
        ("up{job=\"missing\"}", 120_000),
        ("lonely", 120_000),
        // Genuine NaN must be kept (NaN value) on both paths.
        ("up{job=\"nan\"}", 120_000),
        // Stale-NaN marker must be dropped (empty result) on both paths.
        ("up{job=\"db\"}", 120_000),
    ];

    for (query, time_ms) in selectors {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let Expr::VectorSelector(selector) = expr else {
            panic!("`{query}` did not parse to a bare vector selector");
        };

        let interpreter = engine
            .eval_instant_selector("t", &selector, time_ms)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
        let planner = engine
            .eval_instant_selector_via_planner("t", &selector, time_ms)
            .await
            .unwrap_or_else(|error| panic!("planner `{query}`: {error}"));

        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };

        let interpreter = normalize(interpreter);
        let planner = normalize(planner);
        assert!(
            instant_samples_match(&interpreter, &planner),
            "planner/interpreter divergence for `{query}`: {interpreter:?} vs {planner:?}"
        );

        // Pin the staleness semantics the parity above relies on.
        if query == "up{job=\"nan\"}" {
            // Genuine NaN is kept as a NaN value (and is not a stale marker).
            assert_eq!(planner.len(), 1, "genuine NaN dropped for `{query}`");
            let value = float_value(&planner[0].value);
            assert!(value.is_nan(), "genuine NaN not kept for `{query}`");
            assert!(
                !super::is_stale_nan(value),
                "genuine NaN reported as stale for `{query}`"
            );
        }
        if query == "up{job=\"db\"}" {
            // Stale-NaN marker terminates the series: empty result.
            assert!(
                planner.is_empty(),
                "stale-NaN marker not dropped for `{query}`: {planner:?}"
            );
        }
    }
}

/// Differential parity for the present-but-empty-valued-label fix. A series
/// carrying `__unit__=""` (label PRESENT, value empty) must stay DISTINCT
/// from a series of the same name with `__unit__` ABSENT, all the way through
/// the operator leaf — which now encodes absent as NULL and present-empty as
/// `""`. The planner instant-selector and rate-range paths must therefore
/// produce the byte-exact result the interpreter does (same series set, same
/// labelsets, same per-series values), where they previously fell back.
#[tokio::test]
async fn empty_valued_label_planner_path_matches_interpreter() {
    use promql_parser::parser::Expr;

    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();
    // Three series sharing `__name__=m`, distinguished only by the presence
    // and value of `__unit__`:
    //   - job=a: `__unit__=""`  (PRESENT, empty value)
    //   - job=b: `__unit__="s"` (PRESENT, non-empty)
    //   - job=c: `__unit__` ABSENT
    // The fingerprints of a (present-empty) and c (absent) differ, so both
    // must survive selection as distinct series.
    for (lbls, samples) in [
        (
            labels(&[("__name__", "m"), ("job", "a"), ("__unit__", "")]),
            vec![(0_i64, 1.0), (60_000, 2.0), (120_000, 3.0)],
        ),
        (
            labels(&[("__name__", "m"), ("job", "b"), ("__unit__", "s")]),
            vec![(0, 10.0), (60_000, 20.0), (120_000, 30.0)],
        ),
        (
            labels(&[("__name__", "m"), ("job", "c")]),
            vec![(0, 100.0), (60_000, 200.0), (120_000, 300.0)],
        ),
    ] {
        for (ts, value) in samples {
            store.push_float("t", lbls.clone(), ts, value);
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
        let QueryResult::InstantVector(mut samples) = result else {
            panic!("expected instant vector");
        };
        samples.sort_by_key(|sample| sample.labels.fingerprint());
        samples
    };

    // (a) INSTANT selector path: the bare selector `m` matches all three
    // series. Planner (operator leaf) must equal the interpreter, preserving
    // the present-empty-vs-absent distinction.
    let time_ms = 120_000_i64;
    for query in ["m", "m{__unit__=\"\"}", "m{__unit__!=\"\"}"] {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let Expr::VectorSelector(selector) = expr else {
            panic!("`{query}` did not parse to a bare vector selector");
        };
        let interpreter = normalize(
            engine
                .eval_instant_selector("t", &selector, time_ms)
                .await
                .unwrap(),
        );
        let planner = normalize(
            engine
                .eval_instant_selector_via_planner("t", &selector, time_ms)
                .await
                .unwrap(),
        );
        assert!(
            instant_samples_match(&interpreter, &planner),
            "instant planner/interpreter divergence for `{query}`: {interpreter:?} vs {planner:?}"
        );
        // The plannable gate must route the empty-valued selector through the
        // operator path now (no `Ok(None)` fallback).
        let routed = engine
            .plan_instant_expr("t", &Expr::VectorSelector(selector.clone()), time_ms)
            .await
            .unwrap();
        assert!(
            routed.is_some(),
            "selector `{query}` unexpectedly fell back to the interpreter"
        );
    }

    // The bare selector `m` must yield exactly three rows (a, b, c) — proving
    // the present-empty (a) and absent (c) series were not collapsed.
    let bare = normalize(engine.query_instant("t", "m", time_ms).await.unwrap());
    assert_eq!(bare.len(), 3, "present-empty and absent series collapsed");

    // (b) RANGE/matrix path: a rate over the empty-valued-label series must
    // also route through the operator leaf and keep the present-empty (a),
    // non-empty (b), and absent (c) series DISTINCT — three separate result
    // series, the present-empty/absent pair not collapsed.
    let (start, end, step) = (0_i64, 120_000_i64, 60_000_i64);
    let query = "rate(m[2m])";
    let QueryResult::RangeMatrix(mut series) = engine
        .query_range("t", query, start, end, step)
        .await
        .unwrap()
    else {
        panic!("expected matrix for `{query}`");
    };
    series.sort_by_key(|s| s.labels.fingerprint());
    assert_eq!(
        series.len(),
        3,
        "rate over present-empty/absent-label series collapsed: {series:?}"
    );
    // All three result series carry DISTINCT labelsets (the present-empty and
    // absent `__unit__` were not merged): distinct fingerprints.
    let fps: std::collections::BTreeSet<_> =
        series.iter().map(|s| s.labels.fingerprint()).collect();
    assert_eq!(
        fps.len(),
        3,
        "present-empty and absent series collapsed to the same labelset: {series:?}"
    );
}

/// Pin the corrected RANGE-path lookback boundary against Prometheus
/// semantics: the instant-vector lookback window is `(eval - lookbackDelta,
/// eval]` — left-OPEN, right-closed. A sample landing EXACTLY on the lower
/// boundary (`ts == eval - lookbackDelta`) is EXCLUDED. Before the fix the
/// interpreter range path used a left-CLOSED `>=`, spuriously including it;
/// the operator path (and the interpreter's instant path) were already
/// left-open and correct. This test proves:
///   1. the bare-selector RANGE query now routes through the planner,
///   2. planner == interpreter byte-for-byte across the grid, and
///   3. the boundary sample is excluded (the Prometheus-correct behaviour),
///      so a step whose only in-window candidate is the boundary sample has
///      NO point.
#[tokio::test]
async fn range_bare_selector_lookback_boundary_matches_prometheus() {
    let lookback = EngineOpts::default().lookback_delta_ms; // 300_000 (5m)

    let mut store = InMemoryMetricStore::new();
    // A single sample at t=0. With a 5m lookback:
    //   - step t=0:        window (−300000, 0], sample at 0 is in-window (right-closed) -> value.
    //   - step t=300000:   window (0, 300000], sample at 0 is EXACTLY on the
    //                      left boundary -> EXCLUDED (left-open) -> NO point.
    //   - step t=240000:   window (−60000, 240000], sample at 0 in-window -> value.
    store.push_float(
        "t",
        labels(&[("__name__", "m"), ("job", "boundary")]),
        0,
        7.0,
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let (start, end, step) = (0_i64, lookback, 60_000_i64);

    // (1) the gate routes the bare selector through the planner.
    {
        use promql_parser::parser::Expr;

        use crate::{DurationExprContext, parse_promql_with_duration_context};
        let expr =
            parse_promql_with_duration_context("m", DurationExprContext::range(start, end, step))
                .unwrap();
        let mut probe = &expr;
        while let Expr::Paren(paren) = probe {
            probe = &paren.expr;
        }
        assert!(
            super::range_expr_routes_through_planner(probe),
            "bare selector range query should route through the planner"
        );
    }

    // (2) planner (public range path) yields the boundary-correct grid.
    let planner = engine
        .query_range("t", "m", start, end, step)
        .await
        .unwrap();

    // (3) the boundary step (t == eval - lookback) is excluded; the last
    // point is at t=240000, NOT at t=300000.
    let QueryResult::RangeMatrix(series) = &planner else {
        panic!("expected range matrix");
    };
    assert_eq!(series.len(), 1, "boundary series missing");
    let times: Vec<i64> = series[0].samples.iter().map(|(t, _)| *t).collect();
    assert_eq!(
        times,
        vec![0, 60_000, 120_000, 180_000, 240_000],
        "lookback boundary sample (t=300000 step) not excluded: {times:?}"
    );

    // (4) cross-check the interpreter's INSTANT path and the operator both
    // exclude the boundary sample directly, proving all three paths agree.
    let instant_at_boundary = engine.query_instant("t", "m", lookback).await.unwrap();
    let QueryResult::InstantVector(samples) = instant_at_boundary else {
        panic!("expected instant vector");
    };
    assert!(
        samples.is_empty(),
        "instant query at the lookback boundary must exclude the boundary sample: {samples:?}"
    );
}

/// Differential parity for a bare top-level selector carrying `@ start()` /
/// `@ end()` in a RANGE query. The per-step planner range driver now scopes the
/// query's `[start, end]` bounds, and `plan_instant_selector` resolves
/// `@ start()`/`@ end()` to those bounds (a fixed eval instant repeated across
/// every step) — exactly as the interpreter's dedicated
/// `eval_vector_selector_over_steps`. This proves:
///   1. the gate routes `m @ start()` / `m @ end()` through the planner, and
///   2. planner (public range path) == interpreter byte-for-byte.
#[tokio::test]
async fn range_at_start_end_selector_planner_matches_interpreter() {
    use promql_parser::parser::Expr;

    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();
    for (job, samples) in [
        ("a", vec![(0_i64, 1.0_f64), (120_000, 2.0), (300_000, 3.0)]),
        ("b", vec![(0, 10.0), (180_000, 20.0), (300_000, 30.0)]),
    ] {
        for (ts, value) in samples {
            store.push_float("t", labels(&[("__name__", "m"), ("job", job)]), ts, value);
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let (start, end, step) = (0_i64, 300_000_i64, 60_000_i64);

    // `m @ start()` pins eval to t=0 and `m @ end()` to t=300000: both have
    // an in-window sample, so they yield series. `m @ start() offset 1m`
    // shifts the pinned eval back 60s to t=-60000, whose 5m window
    // (-360000, -60000] holds NO sample (the earliest is at t=0), so it
    // yields an EMPTY matrix — the Prometheus-correct result.
    for (query, expect_series) in [
        ("m @ start()", true),
        ("m @ end()", true),
        ("m @ start() offset 1m", false),
    ] {
        // (1) the gate routes the `@ start()/end()` selector through the planner.
        let expr =
            parse_promql_with_duration_context(query, DurationExprContext::range(start, end, step))
                .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let mut probe = &expr;
        while let Expr::Paren(paren) = probe {
            probe = &paren.expr;
        }
        assert!(
            super::range_expr_routes_through_planner(probe),
            "`{query}` should route through the planner"
        );

        // (2) the planner resolves `@ start()`/`@ end()` to a FIXED eval
        // instant repeated across every grid step, so each surviving series
        // carries the SAME value at every one of the 6 steps (the value it
        // had at the pinned eval instant), matching Prometheus.
        let QueryResult::RangeMatrix(series) = engine
            .query_range("t", query, start, end, step)
            .await
            .unwrap_or_else(|error| panic!("planner `{query}`: {error}"))
        else {
            panic!("expected matrix for `{query}`");
        };
        assert_eq!(
            !series.is_empty(),
            expect_series,
            "`{query}` series presence mismatch: {series:?}"
        );
        for s in &series {
            let times: Vec<i64> = s.samples.iter().map(|(t, _)| *t).collect();
            assert_eq!(
                times,
                vec![0, 60_000, 120_000, 180_000, 240_000, 300_000],
                "`{query}` series {:?} must have a point at every step (fixed @ eval): {times:?}",
                s.labels
            );
            let values: Vec<u64> = s
                .samples
                .iter()
                .map(|(_, v)| float_value(v).to_bits())
                .collect();
            assert!(
                values.windows(2).all(|w| w[0] == w[1]),
                "`{query}` series {:?} value must be constant across steps (fixed @ eval): {:?}",
                s.labels,
                s.samples
            );
        }
    }

    // A bare `@ start()` selector in an INSTANT query has no range bounds, so it
    // must raise the SAME hard error on the planner path as the interpreter —
    // never silently produce a result or fall back.
    let instant_err = engine.query_instant("t", "m @ start()", 120_000).await;
    assert!(
        matches!(instant_err, Err(PromqlError::Unsupported(_))),
        "instant `m @ start()` must be a hard Unsupported error, got {instant_err:?}"
    );
}

/// Differential parity for the RESIDUAL range-vector folds the planner now
/// routes through the shared interpreter dispatch (`plan_extended_range_fold_call`):
/// `changes`/`resets`/`deriv` over a plain matrix selector (no operator-leaf
/// UDF), and the `anchored`/`smoothed` extended-selector forms of
/// `rate`/`increase`/`delta`/`changes`/`resets`. Each must plan to `Some` and
/// match the interpreter's `eval_instant_expr` byte-for-byte.
#[tokio::test]
async fn extended_range_fold_planner_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();
    // A monotonic-ish counter with a reset, sampled every 30s through t=300000.
    for (job, samples) in [
        (
            "a",
            vec![
                (0_i64, 0.0_f64),
                (30_000, 5.0),
                (60_000, 10.0),
                (90_000, 4.0), // reset
                (120_000, 9.0),
                (150_000, 15.0),
                (180_000, 21.0),
                (210_000, 25.0),
                (240_000, 30.0),
                (270_000, 33.0),
                (300_000, 40.0),
            ],
        ),
        (
            "b",
            vec![
                (0, 100.0),
                (60_000, 90.0),
                (120_000, 80.0),
                (180_000, 70.0),
                (240_000, 60.0),
                (300_000, 50.0),
            ],
        ),
    ] {
        for (ts, value) in samples {
            store.push_float("t", labels(&[("__name__", "ctr"), ("job", job)]), ts, value);
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let time_ms = 300_000_i64;

    let queries = [
        // changes/resets/deriv over a plain matrix (no operator-leaf UDF).
        "changes(ctr[5m])",
        "resets(ctr[5m])",
        "deriv(ctr[5m])",
        "changes(ctr[2m])",
        "resets(ctr[2m])",
        // anchored/smoothed extended-selector folds.
        "rate(anchored(ctr[5m]))",
        "increase(anchored(ctr[5m]))",
        "delta(anchored(ctr[5m]))",
        "changes(anchored(ctr[5m]))",
        "resets(anchored(ctr[5m]))",
        "rate(smoothed(ctr[5m]))",
        "increase(smoothed(ctr[5m]))",
        "delta(smoothed(ctr[5m]))",
        // predict_linear over a plain matrix.
        "predict_linear(ctr[5m], 60)",
    ];

    for query in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // The planner must claim this query (Some, never None).
        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(plan, time_ms)
            .await
            .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));
        let via_interpreter = engine
            .eval_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };
        let via_operators = normalize(via_operators);
        let via_interpreter = normalize(via_interpreter);
        assert!(
            instant_samples_match(&via_operators, &via_interpreter),
            "extended-range-fold planner/interpreter divergence for `{query}`:\n  operator={via_operators:?}\n  interpreter={via_interpreter:?}"
        );
    }
}

/// Differential parity for a top-level SCALAR-typed RANGE query. A scalar
/// expression (`time()`, `1 + 2`, an argless calendar form) now routes through
/// the per-step planner driver, which folds an identical no-label scalar
/// series per step. The result must be byte-exact with the interpreter's
/// `eval_instant_expr_over_steps` scalar stitching.
#[tokio::test]
async fn range_scalar_expr_planner_path_matches_interpreter() {
    use promql_parser::parser::Expr;

    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A store with one series so calendar functions over `time()` have a
    // defined eval timeline; scalars ignore the series entirely.
    let mut store = InMemoryMetricStore::new();
    store.push_float("t", labels(&[("__name__", "m"), ("job", "a")]), 0, 1.0);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let (start, end, step) = (0_i64, 300_000_i64, 60_000_i64);

    for query in ["42", "1 + 2", "time()", "2 * (3 + 4)"] {
        let expr =
            parse_promql_with_duration_context(query, DurationExprContext::range(start, end, step))
                .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let mut probe = &expr;
        while let Expr::Paren(paren) = probe {
            probe = &paren.expr;
        }
        assert!(
            super::range_expr_routes_through_planner(probe),
            "gate unexpectedly excludes scalar `{query}` from the planner path"
        );
        let planner = engine
            .query_range("t", query, start, end, step)
            .await
            .unwrap_or_else(|error| panic!("planner `{query}`: {error}"));
        // A scalar range query stitches a single no-label series, one float
        // point per step across the whole grid.
        let QueryResult::RangeMatrix(series) = &planner else {
            panic!("expected range matrix for `{query}`");
        };
        assert_eq!(
            series.len(),
            1,
            "scalar `{query}` must yield one unlabeled series with one point per grid step"
        );
        assert!(
            series[0].labels.is_empty(),
            "scalar `{query}` must yield one unlabeled series with one point per grid step"
        );
        assert_eq!(
            series[0].samples.len(),
            6,
            "scalar `{query}` must yield one unlabeled series with one point per grid step"
        );
        // The constant scalars fold to their exact value at every step.
        if let Some(expected) = match query {
            "42" => Some(42.0_f64),
            "1 + 2" => Some(3.0),
            "2 * (3 + 4)" => Some(14.0),
            _ => None,
        } {
            for (_, value) in &series[0].samples {
                assert_eq!(
                    float_value(value).to_bits(),
                    expected.to_bits(),
                    "scalar `{query}` step value diverged"
                );
            }
        }
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn rate_range_planner_path_matches_interpreter() {
    use promql_parser::parser::Expr;

    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // Float-only counters with a reset, a gauge for delta, an offset
    // history, and a single-sample series (no rate value).
    let mut store = InMemoryMetricStore::new();
    for (lbls, samples) in [
        (
            labels(&[("__name__", "http_requests_total"), ("job", "api")]),
            vec![
                (0_i64, 0.0),
                (60_000, 1.0),
                (120_000, 2.0),
                (180_000, 3.0),
                (240_000, 4.0),
                (300_000, 5.0),
            ],
        ),
        (
            // A counter reset mid-window (5 -> 1) exercises reset correction.
            labels(&[("__name__", "http_requests_total"), ("job", "db")]),
            vec![
                (0, 0.0),
                (60_000, 3.0),
                (120_000, 5.0),
                (180_000, 1.0),
                (240_000, 4.0),
                (300_000, 8.0),
            ],
        ),
        (
            // A gauge with ups and downs for delta/idelta.
            labels(&[("__name__", "temperature"), ("job", "api")]),
            vec![(180_000, 10.0), (240_000, 7.0), (300_000, 9.0)],
        ),
        (
            // Single sample in-window: rate-family yields no value. Both paths
            // must DROP this series identically (NULL-drop on the operator
            // path, no-value omission on the interpreter).
            labels(&[("__name__", "http_requests_total"), ("job", "lonely")]),
            vec![(295_000, 100.0)],
        ),
        (
            // A gauge whose window holds a GENUINE NaN sample: `delta` computes
            // a value (the window is non-empty with >=2 samples), and the
            // arithmetic yields NaN. That NaN is a real value (non-null), so it
            // must be KEPT and propagated on both paths — not dropped.
            labels(&[("__name__", "nan_gauge"), ("job", "api")]),
            vec![(240_000, f64::NAN), (300_000, 5.0)],
        ),
    ] {
        for (ts, value) in samples {
            store.push_float("t", lbls.clone(), ts, value);
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries = [
        ("rate(http_requests_total[5m])", 300_000_i64),
        ("increase(http_requests_total[5m])", 300_000),
        ("delta(temperature[2m])", 300_000),
        ("irate(http_requests_total[5m])", 300_000),
        ("idelta(http_requests_total[5m])", 300_000),
        // @ and offset on the matrix selector exercise the time modifier.
        ("rate(http_requests_total[3m] @ 300)", 999_999),
        ("increase(http_requests_total[4m] offset 1m)", 360_000),
        // Tighter window that strands the single-sample series.
        ("rate(http_requests_total{job=\"api\"}[90s])", 300_000),
        // A genuine-NaN delta: the computed value is NaN but non-null, so the
        // series is KEPT (not dropped). Both paths must agree, NaN-aware.
        ("delta(nan_gauge[2m])", 300_000),
    ];

    for (query, time_ms) in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let Expr::Call(_) = &expr else {
            panic!("`{query}` did not parse to a call");
        };
        let (selector, kind) = match_rate_range_call(&expr)
            .unwrap_or_else(|| panic!("`{query}` is not an operator-path rate call"));

        let interpreter = engine
            .eval_instant_call(
                "t",
                match &expr {
                    Expr::Call(call) => call,
                    _ => unreachable!(),
                },
                time_ms,
            )
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
        let planner = engine
            .eval_rate_range_via_planner("t", selector, time_ms, kind)
            .await
            .unwrap_or_else(|error| panic!("planner `{query}`: {error}"));

        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };

        let interpreter = normalize(interpreter);
        let planner = normalize(planner);
        // NaN-aware comparison so a genuine NaN value (e.g. `delta(nan_gauge)`)
        // is treated as equal to itself across both paths rather than spuriously
        // failing under IEEE `NaN != NaN`.
        assert!(
            instant_samples_match(&interpreter, &planner),
            "planner/interpreter divergence for `{query}`: {interpreter:?} vs {planner:?}"
        );

        // Pin that the genuine-NaN delta is KEPT (non-null NaN value), not
        // dropped as if it were a no-value series.
        if query == "delta(nan_gauge[2m])" {
            assert_eq!(planner.len(), 1, "genuine-NaN delta series dropped");
            let value = float_value(&planner[0].value);
            assert!(
                value.is_nan(),
                "genuine NaN not kept through delta: {value}"
            );
        }
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn over_time_range_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A float-only store: a multi-sample gauge for the reductions, a second
    // labelset, a single-sample window edge case, and a stale marker that the
    // matrix path drops.
    let mut store = InMemoryMetricStore::new();
    let stale_bits = f64::from_bits(0x7ff0_0000_0000_0002);
    for (lbls, samples) in [
        (
            labels(&[("__name__", "queue_depth"), ("job", "api")]),
            vec![
                (60_000_i64, 2.0),
                (120_000, 4.0),
                (180_000, 4.0),
                (240_000, 5.0),
                (300_000, 9.0),
            ],
        ),
        (
            labels(&[("__name__", "queue_depth"), ("job", "db")]),
            vec![(120_000, 3.0), (240_000, 7.0), (300_000, 1.0)],
        ),
        (
            // A stale marker mid-window: dropped by both paths.
            labels(&[("__name__", "queue_depth"), ("job", "stale")]),
            vec![(120_000, 5.0), (180_000, stale_bits), (300_000, 6.0)],
        ),
        (
            // Single in-window sample: rate yields no value, but over_time
            // reductions (avg/min/max/last/...) do.
            labels(&[("__name__", "queue_depth"), ("job", "lonely")]),
            vec![(295_000, 100.0)],
        ),
        // A `g`-grouped family for the SPARSE aggregate-over-over_time case at a
        // TIGHT `[30s]` window closing on t=300000 (window (270k, 300k]):
        //   g="mix": a member WITH an in-window sample (300k) -> has a value,
        //     plus a member whose only sample (120k) is outside the window ->
        //     no value (NULL). The no-value member is excluded, so the group
        //     survives with only the in-window member.
        //   g="allsparse": every member's only sample is outside the window,
        //     so the whole group is no-value and produces NO result row.
        (
            labels(&[("__name__", "depth_g"), ("g", "mix"), ("instance", "0")]),
            vec![(300_000, 5.0)],
        ),
        (
            labels(&[("__name__", "depth_g"), ("g", "mix"), ("instance", "1")]),
            vec![(120_000, 9.0)],
        ),
        (
            labels(&[
                ("__name__", "depth_g"),
                ("g", "allsparse"),
                ("instance", "0"),
            ]),
            vec![(120_000, 1.0)],
        ),
        (
            labels(&[
                ("__name__", "depth_g"),
                ("g", "allsparse"),
                ("instance", "1"),
            ]),
            vec![(120_000, 2.0)],
        ),
    ] {
        for (ts, value) in samples {
            store.push_float("t", lbls.clone(), ts, value);
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries = [
        ("avg_over_time(queue_depth[5m])", 300_000_i64),
        ("sum_over_time(queue_depth[5m])", 300_000),
        ("count_over_time(queue_depth[5m])", 300_000),
        ("min_over_time(queue_depth[5m])", 300_000),
        ("max_over_time(queue_depth[5m])", 300_000),
        ("stddev_over_time(queue_depth[5m])", 300_000),
        ("stdvar_over_time(queue_depth[5m])", 300_000),
        // `last_over_time` preserves the metric name; every other family drops it.
        ("last_over_time(queue_depth[5m])", 300_000),
        ("present_over_time(queue_depth[5m])", 300_000),
        ("quantile_over_time(0.5, queue_depth[5m])", 300_000),
        ("quantile_over_time(0.9, queue_depth[5m])", 300_000),
        // @ and offset on the matrix selector exercise the time modifier.
        ("avg_over_time(queue_depth[3m] @ 300)", 999_999),
        ("sum_over_time(queue_depth[4m] offset 1m)", 360_000),
        // Tighter window that strands the single-sample series for some fns.
        ("min_over_time(queue_depth[90s])", 300_000),
        // EXPERIMENTAL over_time members now route through the shared-kernel
        // operator path. `first_over_time` preserves `__name__`; the `ts_of_*`
        // family returns the matching sample's timestamp in seconds.
        ("mad_over_time(queue_depth[5m])", 300_000),
        ("first_over_time(queue_depth[5m])", 300_000),
        ("ts_of_min_over_time(queue_depth[5m])", 300_000),
        ("ts_of_max_over_time(queue_depth[5m])", 300_000),
        ("ts_of_first_over_time(queue_depth[5m])", 300_000),
        ("ts_of_last_over_time(queue_depth[5m])", 300_000),
        // Experimental members composed under an aggregation also route.
        ("sum by (job) (mad_over_time(queue_depth[5m]))", 300_000),
        (
            "count by (job) (ts_of_max_over_time(queue_depth[5m]))",
            300_000,
        ),
        // @ / offset on an experimental member.
        ("first_over_time(queue_depth[3m] @ 300)", 999_999),
        // Aggregation over over_time: the compositional operator-path case.
        ("sum by (job) (avg_over_time(queue_depth[5m]))", 300_000),
        (
            "max without (job) (last_over_time(queue_depth[5m]))",
            300_000,
        ),
        // SPARSE aggregate-over-over_time: a group mixing an in-window member
        // with a no-value (stranded) member excludes the no-value member, and
        // an all-no-value group produces no result row. Every op must agree
        // with the interpreter.
        ("sum by (g) (avg_over_time(depth_g[30s]))", 300_000),
        ("count by (g) (avg_over_time(depth_g[30s]))", 300_000),
        ("min by (g) (max_over_time(depth_g[30s]))", 300_000),
    ];

    for (query, time_ms) in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // Operator path: the recursive planner must claim this query.
        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(plan, time_ms)
            .await
            .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

        // Interpreter path: evaluate the same expression directly.
        let via_interpreter = engine
            .eval_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };

        let via_interpreter = normalize(via_interpreter);
        let via_operators = normalize(via_operators);
        // NaN-aware comparison (a genuine NaN reduction equals itself).
        assert!(
            instant_samples_match(&via_interpreter, &via_operators),
            "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
        );

        // Pin the SPARSE aggregate-over-over_time rule: the no-value (stranded)
        // member is excluded from its group, and the all-no-value group is
        // absent.
        if matches!(
            query,
            "sum by (g) (avg_over_time(depth_g[30s]))"
                | "count by (g) (avg_over_time(depth_g[30s]))"
                | "min by (g) (max_over_time(depth_g[30s]))"
        ) {
            assert_eq!(
                via_operators.len(),
                1,
                "`{query}`: only g=mix survives (g=allsparse absent)"
            );
            let mix = via_operators
                .iter()
                .find(|sample| sample.labels.get("g") == Some("mix"));
            assert!(mix.is_some(), "`{query}`: g=mix row missing");
            assert!(
                via_operators
                    .iter()
                    .all(|sample| sample.labels.get("g") != Some("allsparse")),
                "`{query}`: g=allsparse must be absent"
            );
            if query == "count by (g) (avg_over_time(depth_g[30s]))" {
                assert!(
                    approx_eq(float_value(&mix.unwrap().value), 1.0),
                    "`{query}`: count over g=mix must be 1 (stranded member excluded)"
                );
            }
        }
    }

    // The experimental over_time members (`mad`/`first`/`ts_of_*`) now route
    // through the shared-kernel operator path and are differentially checked in
    // the `queries` list above; pin that they are in fact claimed by the planner.
    for query in [
        "mad_over_time(queue_depth[5m])",
        "first_over_time(queue_depth[5m])",
        "ts_of_min_over_time(queue_depth[5m])",
        "ts_of_max_over_time(queue_depth[5m])",
        "ts_of_first_over_time(queue_depth[5m])",
        "ts_of_last_over_time(queue_depth[5m])",
    ] {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(300_000))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let planned = engine
            .plan_instant_expr("t", &expr, 300_000)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"));
        assert!(
            planned.is_some(),
            "`{query}` must now route through the operator path"
        );
    }
}

/// Differential parity for **subqueries** routed through the recursive
/// planner: a range/`*_over_time` call whose argument is `inner[range:res]`.
/// The subquery's range vector is built per aligned sub-step through the
/// planner and the shared outer fold is applied; the result must equal the
/// interpreter's `eval_subquery` + outer fold byte-for-byte (NaN-aware).
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn subquery_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A float-only store exercising the subquery sub-grid:
    //  - `reqs_total{l}`: two counters (l=a, l=b) for rate/over_time-of-rate.
    //  - `gauge{l}`: a plain gauge in two label groups for the aggregating
    //    inner (`sum by(l)`).
    //  - `sparse{l}`: a series with a single early sample so a tight subquery
    //    window strands it (no-value sub-grid -> dropped series), plus a dense
    //    member so the surviving series is observable.
    let mut store = InMemoryMetricStore::new();
    // Counters: slope = factor over 60s, sampled every 30s out to 20m.
    for (l, factor) in [("a", 1.0), ("b", 2.0)] {
        let lbls = labels(&[("__name__", "reqs_total"), ("l", l)]);
        for step in 0..=40_i32 {
            store.push_float(
                "t",
                lbls.clone(),
                i64::from(step) * 30_000,
                f64::from(step) * factor,
            );
        }
    }
    // Gauges in two groups (`sum by(l)` collapses the `g` member dimension).
    for (l, g, base) in [
        ("a", "0", 3.0),
        ("a", "1", 5.0),
        ("b", "0", 7.0),
        ("b", "1", 11.0),
    ] {
        let lbls = labels(&[("__name__", "gauge"), ("l", l), ("g", g)]);
        for step in 0..=40_i32 {
            store.push_float(
                "t",
                lbls.clone(),
                i64::from(step) * 30_000,
                base + f64::from(step),
            );
        }
    }
    // Sparse: l=dense has a full history; l=stranded has only one early
    // sample, so a tight late subquery window yields it no sub-grid points.
    {
        let dense = labels(&[("__name__", "sparse"), ("l", "dense")]);
        for step in 0..=40_i32 {
            store.push_float(
                "t",
                dense.clone(),
                i64::from(step) * 30_000,
                f64::from(step),
            );
        }
        let stranded = labels(&[("__name__", "sparse"), ("l", "stranded")]);
        store.push_float("t", stranded, 0, 1.0);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Each query must route through the operator path and match the
    // interpreter byte-for-byte. `EngineOpts::default().eval_interval_ms` is
    // 60s, so a subquery written `[range:]` (no resolution) uses a 60s stride
    // on BOTH paths.
    let queries = [
        // Selector inner, explicit resolution.
        ("rate(reqs_total[5m:1m])", 1_200_000_i64),
        // Nested: `*_over_time` over a `rate(...)` subquery — the inner rate is
        // itself planned per sub-step.
        ("max_over_time(rate(reqs_total[1m])[10m:2m])", 1_200_000),
        // Aggregating inner with DEFAULT resolution (`[5m:]` -> 60s stride).
        ("sum_over_time((sum by(l)(gauge))[5m:])", 1_200_000),
        ("avg_over_time((sum by(l)(gauge))[5m:])", 1_200_000),
        // `@` and offset on the subquery shift the evaluated end (and the
        // step-aligned start) identically on both paths.
        ("sum_over_time(gauge[5m:1m] @ 600)", 1_200_000),
        ("sum_over_time(gauge[5m:1m] offset 5m)", 1_200_000),
        // Sparse: the stranded member yields an empty sub-grid window and is
        // dropped from the result; the dense member survives. A tight window
        // at a late time.
        ("sum_over_time(sparse[1m:30s])", 1_200_000),
        ("last_over_time(sparse[1m:30s])", 1_200_000),
        // Binary inner.
        ("rate((reqs_total + reqs_total)[5m:1m])", 1_200_000),
        // Unary-negation inner: `Expr::Unary` now routes through the planner,
        // so the subquery's structural gate accepts it and the inner negation
        // is planned per sub-step.
        ("sum_over_time((-gauge)[5m:1m])", 1_200_000),
        ("max_over_time((-gauge)[5m:1m])", 1_200_000),
    ];

    for (query, time_ms) in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // Operator path: the recursive planner must claim this query.
        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(plan, time_ms)
            .await
            .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

        // Interpreter path: evaluate the same expression directly.
        let via_interpreter = engine
            .eval_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };

        let via_interpreter = normalize(via_interpreter);
        let via_operators = normalize(via_operators);
        assert!(
            instant_samples_match(&via_interpreter, &via_operators),
            "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
        );

        // Pin the sparse-window rule: the stranded member is dropped (no
        // sub-grid points), so only the dense series survives.
        if query == "sum_over_time(sparse[1m:30s])" {
            assert_eq!(
                via_operators.len(),
                1,
                "`{query}`: only l=dense survives (l=stranded dropped)"
            );
            assert_eq!(
                via_operators[0].labels.get("l"),
                Some("dense"),
                "`{query}`: surviving series must be l=dense"
            );
        }
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scalar_math_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // NaN-aware sample comparison: labels and ts must match exactly; values
    // match when bit-equal or both NaN (Prometheus treats all NaNs alike).
    fn samples_match(left: &[crate::InstantSample], right: &[crate::InstantSample]) -> bool {
        if left.len() != right.len() {
            return false;
        }
        left.iter().zip(right).all(|(a, b)| {
            a.labels == b.labels
                && a.ts_ms == b.ts_ms
                && match (&a.value, &b.value) {
                    (SampleValue::Float(x), SampleValue::Float(y)) => {
                        x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan())
                    }
                    _ => false,
                }
        })
    }

    // A float-only store: a multi-label gauge with negatives (for
    // `sqrt`/`ln` NaN/-inf edges), a genuine-NaN series (must survive the
    // operator path), an up-like series for the nested aggregate case, and a
    // counter for the nested rate case.
    let mut store = InMemoryMetricStore::new();
    for (lbls, ts, value) in [
        (labels(&[("__name__", "g"), ("l", "x")]), 60_000_i64, -3.0),
        (labels(&[("__name__", "g"), ("l", "y")]), 60_000, 20.0),
        (labels(&[("__name__", "g"), ("l", "z")]), 60_000, f64::NAN),
        (labels(&[("__name__", "up"), ("job", "api")]), 60_000, 1.0),
        (labels(&[("__name__", "up"), ("job", "db")]), 60_000, 1.0),
    ] {
        store.push_float("t", lbls, ts, value);
    }
    // A counter with a few samples for `abs(rate(...))`.
    let ctr = labels(&[("__name__", "c"), ("job", "api")]);
    for (ts, value) in [(0_i64, 0.0), (60_000, 30.0), (120_000, 90.0)] {
        store.push_float("t", ctr.clone(), ts, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Representative scalar-math queries over a plannable inner vector. Each
    // must route through the operator path and match the interpreter exactly,
    // including the genuine-NaN row and `sqrt(neg)`/`ln(neg)` -> NaN.
    let queries = [
        ("abs(g)", 60_000_i64),
        ("sqrt(g)", 60_000),
        ("ln(g)", 60_000),
        ("log2(g)", 60_000),
        ("sgn(g)", 60_000),
        ("ceil(g)", 60_000),
        ("floor(g)", 60_000),
        ("exp(g)", 60_000),
        ("sin(g)", 60_000),
        ("cos(g)", 60_000),
        ("atan(g)", 60_000),
        ("deg(g)", 60_000),
        ("rad(g)", 60_000),
        ("round(g)", 60_000),
        ("round(g, 5)", 60_000),
        ("clamp_min(g, 0)", 60_000),
        ("clamp_max(g, 10)", 60_000),
        ("clamp(g, 0, 10)", 60_000),
        // `min > max` yields the empty vector.
        ("clamp(g, 10, 0)", 60_000),
        // Nested compositional cases: scalar math over rate and over an
        // aggregate, both already on the operator path.
        ("abs(rate(c[5m]))", 120_000),
        ("ceil(sum by (job) (up))", 60_000),
        // Binary operands are now planner-supported, so scalar math over a
        // binary inner expression also routes through operators and must
        // match the interpreter (incl. the genuine-NaN row in `g`).
        ("abs(g + 1)", 60_000),
        // `atan2` is a binary operator returning a vector; it routes through
        // the binary planner path and must match the interpreter.
        ("g atan2 g", 60_000),
    ];

    for (query, time_ms) in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // Operator path: the recursive planner must claim this query.
        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(plan, time_ms)
            .await
            .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

        // Interpreter path: evaluate the same expression directly.
        let via_interpreter = engine
            .eval_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };

        let interpreter = normalize(via_interpreter);
        let operators = normalize(via_operators);
        assert!(
            samples_match(&interpreter, &operators),
            "planner/interpreter divergence for `{query}`: interpreter={interpreter:?}, operators={operators:?}"
        );
    }

    // A bare matrix selector now routes through the planner as a
    // `RangeMatrix` result (covered by `matrix_subquery_planner_path_matches_
    // interpreter`), so it is no longer asserted as a scalar-math fall-back
    // here.
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn label_ops_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // NaN-aware sample comparison: labels and ts must match exactly; values
    // match when bit-equal or both NaN.
    fn samples_match(left: &[crate::InstantSample], right: &[crate::InstantSample]) -> bool {
        if left.len() != right.len() {
            return false;
        }
        left.iter().zip(right).all(|(a, b)| {
            a.labels == b.labels
                && a.ts_ms == b.ts_ms
                && match (&a.value, &b.value) {
                    (SampleValue::Float(x), SampleValue::Float(y)) => {
                        x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan())
                    }
                    _ => false,
                }
        })
    }

    // A float-only store: a multi-label gauge (with a `src` label for
    // capture-group expansion), a genuine-NaN series (must survive the
    // operator path and sort last), and an up-like metric for the nested
    // aggregate case.
    let mut store = InMemoryMetricStore::new();
    for (lbls, value) in [
        (
            labels(&[("__name__", "g"), ("l", "x"), ("src", "a-1")]),
            3.0,
        ),
        (
            labels(&[("__name__", "g"), ("l", "y"), ("src", "b-2")]),
            1.0,
        ),
        (
            labels(&[("__name__", "g"), ("l", "z"), ("src", "c-3")]),
            f64::NAN,
        ),
    ] {
        store.push_float("t", lbls, 60_000, value);
    }
    for (job, value) in [("api", 1.0), ("db", 1.0)] {
        store.push_float(
            "t",
            labels(&[("__name__", "up"), ("job", job)]),
            60_000,
            value,
        );
    }
    // Two `h` series differing only in label `a`; overwriting `a` to a
    // constant collapses them onto the same labelset (the collision case).
    for (a, value) in [("1", 10.0), ("2", 20.0)] {
        store.push_float(
            "t",
            labels(&[("__name__", "h"), ("a", a), ("b", "p")]),
            60_000,
            value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Representative label-ops queries over a plannable inner vector. Each
    // must route through the operator path and match the interpreter exactly,
    // covering: capture-group `$1` expansion, no-match passthrough,
    // delete-via-empty-replacement, multi-source label_join + separator,
    // sort/sort_desc (incl. the genuine-NaN row), and a nested aggregate.
    let queries = [
        // Capture group: `src="a-1"` -> `dst="a"`.
        (
            r#"label_replace(g, "dst", "$1", "src", "(.*)-.*")"#,
            60_000_i64,
        ),
        // No match (`src` has no digit-only-prefix form here): unchanged.
        (r#"label_replace(g, "dst", "$1", "src", "(\\d+)")"#, 60_000),
        // Empty replacement writes `dst=""` (the interpreter keeps it).
        (r#"label_replace(g, "dst", "", "src", ".*")"#, 60_000),
        // Replace the metric name itself (label_replace does not drop it).
        (
            r#"label_replace(g, "__name__", "renamed", "l", "(.+)")"#,
            60_000,
        ),
        // label_join: multi-source with a separator.
        (r#"label_join(g, "dst", "/", "l", "src")"#, 60_000),
        // label_join with a single source and empty separator.
        (r#"label_join(g, "dst", "", "l")"#, 60_000),
        // sort / sort_desc over a bare selector, including the NaN row
        // (which the NaN-preserving inner sourcing must keep and place last).
        ("sort(g)", 60_000),
        ("sort_desc(g)", 60_000),
        // Nested compositional case: sort over an aggregate (NaN-free `up`,
        // so the aggregate operator path matches the interpreter exactly).
        ("sort(sum by (job) (up))", 60_000),
        // label_replace over a nested aggregate (operator inner).
        (
            r#"label_replace(sum by (job) (up), "tag", "$1", "job", "(.+)")"#,
            60_000,
        ),
        // Binary operands are now planner-supported, so label-ops over a
        // binary inner expression route through operators and must match the
        // interpreter (note: `g + 1` drops `__name__`, so `l`/`src` survive).
        (r#"label_join(g + 1, "dst", "/", "l")"#, 60_000),
        ("sort(g + 1)", 60_000),
        // sort_by_label / sort_by_label_desc over a bare selector: order by the
        // `l` label values, then by remaining labels (the canonical key
        // tiebreak). Order-sensitive (the comparator below treats `sort*`
        // queries as ordered).
        (r#"sort_by_label(g, "l")"#, 60_000),
        (r#"sort_by_label_desc(g, "l")"#, 60_000),
        // Multi-label sort_by_label: tie on `l` would fall through to `src`.
        (r#"sort_by_label(g, "l", "src")"#, 60_000),
        // sort_by_label over a nested aggregate (operator inner).
        (r#"sort_by_label(sum by (job) (up), "job")"#, 60_000),
    ];

    for (query, time_ms) in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // Operator path: the recursive planner must claim this query.
        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(plan, time_ms)
            .await
            .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

        // Interpreter path: evaluate the same expression directly.
        let via_interpreter = engine
            .eval_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

        // `sort`/`sort_desc` assert ordering, so compare order-sensitively for
        // them and fingerprint-normalize the unordered label-rewrites.
        let ordered = query.starts_with("sort");
        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            if !ordered {
                samples.sort_by(|left, right| {
                    left.labels.fingerprint().cmp(&right.labels.fingerprint())
                });
            }
            samples
        };

        let interpreter = normalize(via_interpreter);
        let operators = normalize(via_operators);
        assert!(
            samples_match(&interpreter, &operators),
            "planner/interpreter divergence for `{query}`: interpreter={interpreter:?}, operators={operators:?}"
        );
    }

    // A `label_replace` that collapses two series onto the same labelset must
    // error identically through the operator path and the interpreter. The
    // top-level uniqueness check enforces this for both (`query_instant`).
    let collision = r#"label_replace(h, "a", "same", "a", ".*")"#;
    let operator_err = engine
        .query_instant("t", collision, 60_000)
        .await
        .expect_err("collision must error through the operator path");
    assert!(matches!(operator_err, PromqlError::Exec(_)));
    // Confirm the operator path actually claimed the collision query (so the
    // error came from the operator path, not an interpreter fallback).
    let collision_expr =
        parse_promql_with_duration_context(collision, DurationExprContext::instant(60_000))
            .unwrap();
    assert!(
        engine
            .plan_instant_expr("t", &collision_expr, 60_000)
            .await
            .unwrap()
            .is_some(),
        "collision query must route through the planner"
    );

    // `sort_by_label` / `sort_by_label_desc` now route through the operator
    // path (differentially checked in the `queries` list above); pin that the
    // planner claims them and falls back on a missing label-name argument.
    for query in [r#"sort_by_label(g, "l")"#, r#"sort_by_label_desc(g, "l")"#] {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(60_000))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let planned = engine
            .plan_instant_expr("t", &expr, 60_000)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"));
        assert!(
            planned.is_some(),
            "`{query}` must now route through the operator path"
        );
    }
    // `sort_by_label(g)` with no label-name argument falls back so the
    // interpreter raises the canonical arity error.
    let no_label = parse_promql_with_duration_context(
        "sort_by_label(g)",
        DurationExprContext::instant(60_000),
    )
    .unwrap();
    assert!(
        engine
            .plan_instant_expr("t", &no_label, 60_000)
            .await
            .unwrap()
            .is_none(),
        "`sort_by_label(g)` (no label arg) must fall back to the interpreter"
    );
}

/// Differential parity for `info(v [, data_label_selector])` routed through the
/// recursive planner: the input vector is recursed, the `target_info` /
/// custom-selector series are selected through the shared interpreter helper,
/// and the shared `apply_info` join is applied. The result must equal the
/// interpreter's `eval_info_call` byte-for-byte.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn info_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A store mirroring the conformance corpus: a base metric, a metric whose
    // identifying labels don't match any target_info, a metric with an
    // overlapping data label, plus `target_info` and `build_info` series.
    let mut store = InMemoryMetricStore::new();
    for (lbls, value) in [
        (
            labels(&[
                ("__name__", "metric"),
                ("instance", "a"),
                ("job", "1"),
                ("label", "value"),
            ]),
            2.0,
        ),
        (
            labels(&[
                ("__name__", "metric_not_matching_target_info"),
                ("instance", "a"),
                ("job", "2"),
                ("label", "value"),
            ]),
            2.0,
        ),
        (
            labels(&[
                ("__name__", "metric_with_overlapping_label"),
                ("instance", "a"),
                ("job", "1"),
                ("label", "value"),
                ("data", "base"),
            ]),
            2.0,
        ),
        (
            labels(&[
                ("__name__", "target_info"),
                ("instance", "a"),
                ("job", "1"),
                ("data", "info"),
                ("another_data", "another info"),
            ]),
            1.0,
        ),
        (
            labels(&[
                ("__name__", "build_info"),
                ("instance", "a"),
                ("job", "1"),
                ("build_data", "build"),
            ]),
            1.0,
        ),
    ] {
        store.push_float("t", lbls, 600_000, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Each query must route through the operator path and match the
    // interpreter exactly: default target_info enrichment, single/all-label
    // restriction, non-matching identifying labels (passthrough), a required
    // matcher not matching empty (drop), overlapping-label passthrough, and
    // explicit `__name__` selectors (target_info / build_info / both /
    // non-existent), plus the input-as-bare-selector form.
    let queries = [
        "info(metric)",
        r#"info(metric, {data=~".+"})"#,
        "info(metric_not_matching_target_info)",
        r#"info(metric, {non_existent=~".+"})"#,
        r#"info(metric, {data=~".+", non_existent=~".*"})"#,
        "info(metric_with_overlapping_label)",
        r#"info(metric, {__name__="target_info"})"#,
        r#"info(metric, {__name__="non_existent"})"#,
        r#"info(metric, {__name__="build_info"})"#,
        r#"info(metric, {__name__=~".+_info"})"#,
        r#"info(build_info, {__name__=~".+_info", build_data=~".+"})"#,
        // Input as a bare brace-only selector.
        r#"info({job="1"}, {__name__="target_info"})"#,
    ];

    for query in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(600_000))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // Operator path: the recursive planner must claim this query.
        let plan = engine
            .plan_instant_expr("t", &expr, 600_000)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(plan, 600_000)
            .await
            .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

        // Interpreter path: evaluate the same expression directly.
        let via_interpreter = engine
            .eval_instant_expr("t", &expr, 600_000)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };

        let interpreter = normalize(via_interpreter);
        let operators = normalize(via_operators);
        assert!(
            instant_samples_match(&interpreter, &operators),
            "planner/interpreter divergence for `{query}`: interpreter={interpreter:?}, operators={operators:?}"
        );
    }

    // A histogram info-series match errors identically (the info series must be
    // float-typed). Pin that the planner surfaces the same error class.
    let mut hist_store = InMemoryMetricStore::new();
    hist_store.push_float(
        "t",
        labels(&[
            ("__name__", "metric"),
            ("instance", "a"),
            ("job", "1"),
            ("label", "value"),
        ]),
        600_000,
        2.0,
    );
    hist_store.push_histogram(
        "t",
        labels(&[("__name__", "hist"), ("instance", "a"), ("job", "1")]),
        600_000,
        native_histogram(4.0, 10.0),
    );
    let hist_engine = PromqlEngine::new(Arc::new(hist_store), EngineOpts::default());
    let hist_query = r#"info(metric, {__name__="hist"})"#;
    let hist_expr =
        parse_promql_with_duration_context(hist_query, DurationExprContext::instant(600_000))
            .unwrap();
    let operator_result = hist_engine
        .plan_instant_expr("t", &hist_expr, 600_000)
        .await;
    assert!(
        matches!(operator_result, Err(PromqlError::Plan(_))),
        "histogram info series must error through the operator path"
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn simple_aggregate_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A float-only multi-label store: two jobs across two groups, an
    // instance dimension for `without`, plus counters for the rate case.
    let mut store = InMemoryMetricStore::new();
    for (job, group, instance, value) in [
        ("api", "prod", "0", 1.0),
        ("api", "prod", "1", 2.0),
        ("api", "canary", "0", 4.0),
        ("db", "prod", "0", 8.0),
    ] {
        let lbls = labels(&[
            ("__name__", "http_requests"),
            ("job", job),
            ("group", group),
            ("instance", instance),
        ]);
        store.push_float("t", lbls, 120_000, value);
    }
    // A dedicated NaN metric exercising the post-fix selection semantics
    // through `sum`/`count`. instance=0 is a finite value, instance=1's
    // latest in-window sample is a GENUINE NaN (must be KEPT and flow into
    // the aggregate, so `sum(nan_metric)` is NaN and `count(nan_metric)` is
    // 2), and instance=2's latest in-window sample is a STALE-NaN marker
    // (must be DROPPED before aggregation, so it does not contribute to
    // `count`). Both paths must agree.
    for (instance, ts, value) in [
        ("0", 120_000_i64, 3.0),
        ("1", 60_000, 5.0),
        ("1", 120_000, f64::NAN),
        ("2", 60_000, 9.0),
        ("2", 120_000, stale_nan()),
    ] {
        let lbls = labels(&[
            ("__name__", "nan_metric"),
            ("job", "api"),
            ("instance", instance),
        ]);
        store.push_float("t", lbls, ts, value);
    }
    // A dedicated metric pinning the `min`/`max` NaN-ignoring rule. Group
    // g="mixed" holds genuine NaN alongside finite samples: Prometheus (and
    // the interpreter) take the extremum over the non-NaN values (NaN
    // ignored), so min=1, max=4. Group g="allnan" is entirely NaN:
    // Prometheus keeps the series with a NaN result (it is not dropped).
    // Arrow's built-in min/max instead order floats with total_cmp and
    // PROPAGATE NaN, so the operator path must use the NaN-ignoring UDAFs to
    // match the interpreter here.
    for (group, instance, value) in [
        ("mixed", "0", f64::NAN),
        ("mixed", "1", 4.0),
        ("mixed", "2", 1.0),
        ("mixed", "3", f64::NAN),
        ("allnan", "0", f64::NAN),
        ("allnan", "1", f64::NAN),
    ] {
        let lbls = labels(&[
            ("__name__", "minmax_nan"),
            ("g", group),
            ("instance", instance),
        ]);
        store.push_float("t", lbls, 120_000, value);
    }
    // Counters for `sum by (...) (rate(...))` (slope = step factor / 60s).
    for (job, path, factor) in [("api", "a", 1.0), ("api", "b", 2.0), ("db", "a", 5.0)] {
        let lbls = labels(&[("__name__", "reqs_total"), ("job", job), ("path", path)]);
        for step in 0..=3_i32 {
            store.push_float(
                "t",
                lbls.clone(),
                i64::from(step) * 60_000,
                f64::from(step) * factor,
            );
        }
    }
    // Counters for the SPARSE aggregate-over-rate parity. The `g` label groups
    // members; at the 2m rate window closing on t=180_000:
    //   g="mix": one DENSE member (full history -> rate has a value) plus one
    //     SPARSE member (a single in-window sample -> rate is no-value). The
    //     no-value series must be excluded, so `sum by(g)(rate)` over g="mix"
    //     equals just the dense member's rate and `count by(g)(rate)` is 1.
    //   g="allsparse": every member is a single-sample (no-value) series, so
    //     the whole group collapses to NO result row (series absent), matching
    //     the interpreter, which forms no group when no sample reaches it.
    for (g, instance) in [
        ("mix", "dense"),
        ("mix", "sparse"),
        ("allsparse", "0"),
        ("allsparse", "1"),
    ] {
        let lbls = labels(&[
            ("__name__", "sparse_total"),
            ("g", g),
            ("instance", instance),
        ]);
        if instance == "dense" {
            // A full counter history: rate has a value at t=180_000.
            for step in 0..=3_i32 {
                store.push_float(
                    "t",
                    lbls.clone(),
                    i64::from(step) * 60_000,
                    f64::from(step) * 7.0,
                );
            }
        } else {
            // A single in-window sample: rate yields no value (NULL).
            store.push_float("t", lbls.clone(), 120_000, 100.0);
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries = [
        ("sum by (group) (http_requests)", 120_000_i64),
        ("avg by (group) (http_requests)", 120_000),
        ("min by (group) (http_requests)", 120_000),
        ("max by (group) (http_requests)", 120_000),
        ("count by (group) (http_requests)", 120_000),
        ("group by (group) (http_requests)", 120_000),
        ("sum without (instance) (http_requests)", 120_000),
        ("sum without () (http_requests)", 120_000),
        ("sum by () (http_requests)", 120_000),
        ("sum(http_requests)", 120_000),
        ("sum by (job, nonexistent) (http_requests)", 120_000),
        ("sum(((http_requests)))", 120_000),
        // Empty-input aggregations must yield an empty vector (no global
        // group row), matching Prometheus and the interpreter.
        ("sum by () (does_not_exist)", 120_000),
        ("sum(does_not_exist)", 120_000),
        ("count by (group) (does_not_exist)", 120_000),
        // Aggregation over a rate call: the marquee operator-path case.
        // `sum by (l) (rate(x[range]))` mirrors the diff-corpus query
        // `sum by (method) (rate(http_requests_total[30s]))`.
        ("sum by (job) (rate(reqs_total[3m]))", 180_000),
        ("sum by (path) (rate(reqs_total[90s]))", 180_000),
        ("max without (path) (rate(reqs_total[3m]))", 180_000),
        // SPARSE aggregate-over-rate (the headline divergence the fix closes):
        // a group mixing a dense rate with a no-value (sparse) rate must
        // exclude the no-value series, and an all-no-value group must produce
        // no result row. Every simple op must agree with the interpreter.
        ("sum by (g) (rate(sparse_total[2m]))", 180_000),
        ("avg by (g) (rate(sparse_total[2m]))", 180_000),
        ("min by (g) (rate(sparse_total[2m]))", 180_000),
        ("max by (g) (rate(sparse_total[2m]))", 180_000),
        ("count by (g) (rate(sparse_total[2m]))", 180_000),
        ("group by (g) (rate(sparse_total[2m]))", 180_000),
        // No grouping: the global aggregate is over the single dense rate
        // (every sparse series is no-value and excluded). One result row.
        ("sum (rate(sparse_total[2m]))", 180_000),
        ("count (rate(sparse_total[2m]))", 180_000),
        // The same fix on `*_over_time`: avg_over_time has a value for the
        // single-sample sparse members too, but a TIGHT window can strand
        // them. Use a window narrow enough that the sparse members fall
        // outside it at t=180000 while the dense member still reduces.
        ("count by (g) (avg_over_time(sparse_total[30s]))", 180_000),
        // Genuine NaN flows into the aggregate (sum -> NaN), and the
        // stale-NaN marker is dropped before counting (count -> 2).
        ("sum(nan_metric)", 120_000),
        ("count(nan_metric)", 120_000),
        // NaN-ignoring min/max: the "mixed" group's extremum is over its
        // non-NaN samples (min=1, max=4); the "allnan" group keeps the
        // series with a NaN result. The operator path (NaN-ignoring UDAFs)
        // must match the interpreter bit-for-bit on every group, including
        // the all-NaN -> NaN case (a plain `value != value` filter would
        // instead drop the all-NaN series).
        ("min by (g) (minmax_nan)", 120_000),
        ("max by (g) (minmax_nan)", 120_000),
        ("min(minmax_nan)", 120_000),
        ("max(minmax_nan)", 120_000),
    ];

    for (query, time_ms) in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // Operator path: the recursive planner must claim this query.
        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(plan, time_ms)
            .await
            .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

        // Interpreter path: evaluate the same expression directly.
        let via_interpreter = engine
            .eval_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };

        let via_interpreter = normalize(via_interpreter);
        let via_operators = normalize(via_operators);
        assert!(
            instant_samples_match(&via_interpreter, &via_operators),
            "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
        );

        // Pin the staleness semantics through the aggregate: the genuine NaN
        // in `nan_metric` is kept (so `sum(nan_metric)` is NaN), and the
        // stale-NaN marker is dropped before counting (so `count(nan_metric)`
        // is 2, not 3).
        assert_aggregate_nan_staleness(query, &via_operators);
        // Pin the NaN-ignoring min/max rule on absolute values (not just
        // operator==interpreter): the mixed group's extremum is over its
        // non-NaN samples, and the all-NaN group is kept with a NaN result.
        assert_minmax_nan_ignoring(query, &via_operators);
        // Pin the SPARSE aggregate-over-rate rule on absolute values: the
        // no-value (sparse) series is excluded from its group, and an
        // all-no-value group produces no result row at all.
        assert_sparse_aggregate_excludes_no_value(query, &via_operators);
    }
}

/// Differential parity for a simple aggregation whose inner bare selector has
/// a genuine (non-stale) NaN series alone in its own `by` group — the exact
/// shape the operator path must NOT drop. `sum(nan_metric)` (collapsed) already
/// pins genuine-NaN propagation, but a genuine NaN ALONE in a distinct group is
/// the case where a NaN-dropping selector would silently omit a whole group row
/// rather than emit it with value NaN; this test pins that across all six simple
/// ops, `by`/`without`, and a stale group (dropped) and a mixed NaN+finite group
/// (NaN ignored by min/max), comparing the operator path against the interpreter
/// and asserting the absolute Prometheus outcomes.
#[tokio::test]
#[allow(clippy::too_many_lines, clippy::type_complexity)]
async fn aggregate_genuine_nan_group_parity() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();
    // `g` exercises every NaN/stale shape across DISTINCT `by (l)` groups:
    //   l=a: normal finite            -> group value 1.0
    //   l=b: a LONE genuine NaN       -> group KEPT with value NaN
    //   l=c: normal finite            -> group value 3.0
    //   l=d: latest is a STALE marker -> series dropped -> group ABSENT
    //   l=e: a group MIXING genuine NaN with finite {NaN, 2.0, 5.0}
    //        -> sum/avg NaN; min=2/max=5 (NaN ignored); count=3; group=1
    for (l, instance, ts, value) in [
        ("a", "0", 120_000_i64, 1.0_f64),
        ("b", "0", 120_000, f64::NAN),
        ("c", "0", 120_000, 3.0),
        ("d", "0", 60_000, 7.0),
        ("d", "0", 120_000, stale_nan()),
        ("e", "0", 120_000, f64::NAN),
        ("e", "1", 120_000, 2.0),
        ("e", "2", 120_000, 5.0),
    ] {
        let lbls = labels(&[("__name__", "g"), ("l", l), ("instance", instance)]);
        store.push_float("t", lbls, ts, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Each op must (a) route through the planner, (b) match the interpreter
    // byte-for-byte (NaN-aware), and (c) hit the documented absolute outcome.
    // `expect` maps l -> Some(value) for present groups; l=d is always absent.
    let cases: &[(&str, &[(&str, Option<f64>)])] = &[
        (
            "sum by (l) (g)",
            &[
                ("a", Some(1.0)),
                ("b", Some(f64::NAN)),
                ("c", Some(3.0)),
                ("e", Some(f64::NAN)),
            ],
        ),
        (
            "avg by (l) (g)",
            &[
                ("a", Some(1.0)),
                ("b", Some(f64::NAN)),
                ("c", Some(3.0)),
                ("e", Some(f64::NAN)),
            ],
        ),
        (
            "min by (l) (g)",
            &[
                ("a", Some(1.0)),
                ("b", Some(f64::NAN)),
                ("c", Some(3.0)),
                ("e", Some(2.0)),
            ],
        ),
        (
            "max by (l) (g)",
            &[
                ("a", Some(1.0)),
                ("b", Some(f64::NAN)),
                ("c", Some(3.0)),
                ("e", Some(5.0)),
            ],
        ),
        (
            "count by (l) (g)",
            &[
                ("a", Some(1.0)),
                ("b", Some(1.0)),
                ("c", Some(1.0)),
                ("e", Some(3.0)),
            ],
        ),
        (
            "group by (l) (g)",
            &[
                ("a", Some(1.0)),
                ("b", Some(1.0)),
                ("c", Some(1.0)),
                ("e", Some(1.0)),
            ],
        ),
        // `without (instance)` groups by `l` (and drops `__name__`): same shape.
        (
            "sum without (instance) (g)",
            &[
                ("a", Some(1.0)),
                ("b", Some(f64::NAN)),
                ("c", Some(3.0)),
                ("e", Some(f64::NAN)),
            ],
        ),
    ];

    for (query, expect) in cases {
        let time_ms = 120_000_i64;
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(plan, time_ms)
            .await
            .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));
        let via_interpreter = engine
            .eval_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
        let norm = |r: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut s) = r else {
                panic!("expected vector for `{query}`");
            };
            s.sort_by_key(|item| item.labels.fingerprint());
            s
        };
        let oper = norm(via_operators);
        let interp = norm(via_interpreter);
        assert!(
            instant_samples_match(&interp, &oper),
            "planner/interpreter divergence for `{query}`: {interp:?} vs {oper:?}"
        );
        // The stale group `l=d` is always absent on both paths.
        assert!(
            !oper.iter().any(|s| s.labels.get("l") == Some("d")),
            "`{query}`: stale group l=d must be absent, got {oper:?}"
        );
        // Absolute Prometheus outcome per group.
        for (l, want) in *expect {
            let got = oper.iter().find(|s| s.labels.get("l") == Some(*l));
            match want {
                Some(value) => {
                    let sample =
                        got.unwrap_or_else(|| panic!("`{query}`: group l={l} missing in {oper:?}"));
                    let got_value = float_value(&sample.value);
                    if value.is_nan() {
                        assert!(
                            got_value.is_nan() && !super::is_stale_nan(got_value),
                            "`{query}`: l={l} want genuine NaN, got {got_value}"
                        );
                    } else {
                        assert!(
                            approx_eq(got_value, *value),
                            "`{query}`: l={l} want {value}, got {got_value}"
                        );
                    }
                }
                None => assert!(got.is_none(), "`{query}`: l={l} must be absent"),
            }
        }
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn param_aggregate_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A float-only store exercising the parameterized aggregations:
    //  - `m{job,instance}`: a multi-instance gauge per job, with a TIE between
    //    two instances (api/0 and api/1 both 5.0) so topk/bottomk tie-breaks
    //    (by `labels_key`) are observable, plus a genuine NaN member to pin
    //    NaN ordering (`total_cmp`) and quantile/stddev NaN handling.
    //  - `single{instance}`: a one-member group (single-element quantile and
    //    stddev/stdvar -> 0).
    //  - `cv{instance}`: a metric with repeated values for `count_values`
    //    (two members share value 1, one is 2).
    //  - `reqs_total`: counters for the nested `topk(.., rate(...))` case.
    let mut store = InMemoryMetricStore::new();
    for (job, instance, value) in [
        ("api", "0", 5.0),
        ("api", "1", 5.0), // ties with api/0 under topk/bottomk
        ("api", "2", 2.0),
        ("api", "3", 8.0),
        ("api", "4", f64::NAN), // genuine NaN member
        ("db", "0", 1.0),
        ("db", "1", 9.0),
    ] {
        let lbls = labels(&[("__name__", "m"), ("job", job), ("instance", instance)]);
        store.push_float("t", lbls, 120_000, value);
    }
    // A single-member group per job (single-element quantile/stddev/stdvar).
    for (job, value) in [("api", 4.0), ("db", 7.0)] {
        let lbls = labels(&[("__name__", "single"), ("job", job)]);
        store.push_float("t", lbls, 120_000, value);
    }
    // Repeated values for count_values: 1, 1, 2 within job=api.
    for (instance, value) in [("0", 1.0), ("1", 1.0), ("2", 2.0)] {
        let lbls = labels(&[("__name__", "cv"), ("job", "api"), ("instance", instance)]);
        store.push_float("t", lbls, 120_000, value);
    }
    // Counters for `topk(.., rate(...))` (slope = factor / 60s).
    for (path, factor) in [("a", 1.0), ("b", 2.0), ("c", 5.0)] {
        let lbls = labels(&[("__name__", "reqs_total"), ("job", "api"), ("path", path)]);
        for step in 0..=3_i32 {
            store.push_float(
                "t",
                lbls.clone(),
                i64::from(step) * 60_000,
                f64::from(step) * factor,
            );
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Each query must route through the operator path and match the
    // interpreter byte-for-byte (NaN-aware, bit-exact).
    let queries = [
        // topk/bottomk: original series kept (labels incl. __name__), tie-break
        // by labels_key, k clamping (k > group size, k = 0). by and without.
        ("topk(2, m)", 120_000_i64),
        ("bottomk(2, m)", 120_000),
        ("topk(2, m) by (job)", 120_000),
        ("bottomk(2, m) by (job)", 120_000),
        ("topk(2, m) without (instance)", 120_000),
        // k larger than a group's size clamps to the whole group.
        ("topk(10, m) by (job)", 120_000),
        // k = 0 yields the empty vector.
        ("topk(0, m)", 120_000),
        ("bottomk(0, m) by (job)", 120_000),
        // Ties across the whole vector: api/0 and api/1 both 5.0.
        ("topk(3, m)", 120_000),
        // quantile: phi = 0 / 0.5 / 0.9 / 1, by and without.
        ("quantile(0, m) by (job)", 120_000),
        ("quantile(0.5, m) by (job)", 120_000),
        ("quantile(0.9, m) by (job)", 120_000),
        ("quantile(1, m) by (job)", 120_000),
        ("quantile(0.5, m) without (instance)", 120_000),
        // Single-element group: quantile equals the lone value.
        ("quantile(0.5, single) by (job)", 120_000),
        // count_values: one series per distinct value, value -> label, count.
        (r#"count_values("v", cv)"#, 120_000),
        (r#"count_values("v", cv) by (job)"#, 120_000),
        (r#"count_values("v", cv) without (instance)"#, 120_000),
        // stddev/stdvar: population std-dev / variance, by and without.
        ("stddev(m) by (job)", 120_000),
        ("stdvar(m) by (job)", 120_000),
        ("stddev(m) without (instance)", 120_000),
        ("stdvar without (instance) (m)", 120_000),
        // Single-element group -> stddev/stdvar = 0.
        ("stddev(single) by (job)", 120_000),
        ("stdvar(single) by (job)", 120_000),
        // No modifier (collapse all).
        ("stddev(m)", 120_000),
        ("quantile(0.5, m)", 120_000),
        // Nested: a parameterized aggregation over a rate inner already on the
        // operator path.
        ("topk(1, rate(reqs_total[3m]))", 180_000),
        ("quantile(0.5, rate(reqs_total[3m]))", 180_000),
        ("stddev by (job) (rate(reqs_total[3m]))", 180_000),
        (r#"count_values("v", rate(reqs_total[3m]))"#, 180_000),
        // Nested: a parameterized aggregation over a SUBQUERY-range inner,
        // which now routes through the planner (subquery sub-grid evaluated
        // per-step through the recursive planner, shared outer fold).
        ("quantile(0.5, max_over_time((m)[5m:1m]))", 120_000),
        // Unary-negation subquery inner: `Expr::Unary` now routes through the
        // planner, so the subquery's structural gate accepts it.
        ("quantile(0.5, max_over_time((-m)[5m:1m]))", 120_000),
    ];

    for (query, time_ms) in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // Operator path: the recursive planner must claim this query.
        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(plan, time_ms)
            .await
            .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

        // Interpreter path: evaluate the same expression directly.
        let via_interpreter = engine
            .eval_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };

        let via_interpreter = normalize(via_interpreter);
        let via_operators = normalize(via_operators);
        assert!(
            instant_samples_match(&via_interpreter, &via_operators),
            "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
        );
    }

    // The experimental `limitk`/`limit_ratio` param aggregations now route
    // through the planner via the shared interpreter kernels (incl.
    // `limit_ratio`'s `InvalidRatioWarning`); their parity is checked in
    // `experimental_param_aggregate_planner_path_matches_interpreter`.
}

/// M18: an out-of-range / NaN `quantile` phi does NOT error. Matching
/// Prometheus (and the `histogram_quantile` family already in this file), the
/// aggregate returns signed `+Inf` (phi > 1), `-Inf` (phi < 0), `NaN` (phi
/// NaN) and raises an `InvalidQuantileWarning` — never aborting. (This
/// reverses the earlier deliberate "canonical quantile-phi error" commit to
/// realign with Prometheus.)
#[tokio::test]
async fn quantile_out_of_range_phi_returns_signed_inf_with_warning() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("0", 1.0), ("1", 2.0), ("2", 3.0)] {
        let lbls = labels(&[("__name__", "m"), ("instance", instance)]);
        store.push_float("t", lbls, 120_000, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let time_ms = 120_000_i64;

    for (query, phi_text, predicate) in [
        (
            "quantile(1.1, m)",
            "1.1",
            f64::is_infinite as fn(f64) -> bool,
        ),
        ("quantile(-0.1, m)", "-0.1", f64::is_infinite),
        ("quantile(NaN, m)", "NaN", f64::is_nan),
    ] {
        let (result, annotations) = engine
            .query_instant_with_annotations("t", query, time_ms)
            .await
            .unwrap_or_else(|error| panic!("`{query}` must NOT error: {error}"));

        let QueryResult::InstantVector(samples) = result else {
            panic!("`{query}` must yield an instant vector");
        };
        assert_eq!(samples.len(), 1, "`{query}`: collapsed to one group");
        let value = float_value(&samples[0].value);
        assert!(
            predicate(value),
            "`{query}`: expected signed Inf / NaN, got {value}"
        );
        // For the +/-Inf cases, also pin the sign.
        if query.contains("1.1") {
            assert!(value > 0.0, "phi > 1 -> +Inf");
        } else if query.contains("-0.1") {
            assert!(value < 0.0, "phi < 0 -> -Inf");
        }

        assert_eq!(
            annotations.warnings,
            vec![format!(
                "PromQL warning: quantile value should be between 0 and 1, got {phi_text}"
            )],
            "`{query}` must raise exactly one InvalidQuantileWarning"
        );
    }
}

/// C2: `check_resolution_points` rejects a non-positive step, an abusive
/// point count above `MAX_RESOLUTION_POINTS`, and accepts a count at the cap.
#[test]
fn check_resolution_points_enforces_cap() {
    // A non-positive step is rejected outright.
    assert!(check_resolution_points(0, 1_000, 0).is_err());
    assert!(check_resolution_points(0, 1_000, -1).is_err());

    // `(end-start)/step == MAX_RESOLUTION_POINTS` intervals is accepted — the
    // same boundary the HTTP gate and Prometheus' `(end-start)/step > 11000`
    // rule admit (no off-by-one re-rejection of a gate-admitted query).
    let at_cap = i64::try_from(MAX_RESOLUTION_POINTS).unwrap(); // step = 1ms => intervals == MAX.
    assert!(check_resolution_points(0, at_cap, 1).is_ok());

    // One interval over the cap errors.
    assert!(check_resolution_points(0, at_cap + 1, 1).is_err());

    // The abusive `[1000d:1ms]`-style resolution is rejected before looping.
    let thousand_days_ms = 1_000_i64 * 24 * 60 * 60 * 1_000;
    let err = check_resolution_points(0, thousand_days_ms, 1).expect_err("must reject");
    assert!(err.to_string().contains("exceeded maximum resolution"));
}

/// C2 (engine backstop): an abusive subquery resolution errors via the range
/// driver's `check_resolution_points` guard rather than looping ~1e11 times.
#[tokio::test]
async fn abusive_subquery_resolution_errors_before_looping() {
    let mut store = InMemoryMetricStore::new();
    store.push_float("t", labels(&[("__name__", "up")]), 0, 1.0);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // `last_over_time(up[1000d:1ms])` would walk ~8.6e10 sub-steps; the
    // backstop rejects it with the resolution error instead.
    let err = engine
        .query_instant("t", "last_over_time(up[1000d:1ms])", 0)
        .await
        .expect_err("abusive subquery resolution must error");
    assert!(
        err.to_string().contains("exceeded maximum resolution"),
        "unexpected error: {err}"
    );
}

/// Divergences A + B: a collapsed/global `sum`/`avg` over a multi-series
/// group must be (a) deterministic run-to-run (bit-exact via `to_bits`) and
/// (b) bit-for-bit identical to the interpreter oracle — including the
/// NaN-SIGN-bit case where a `{+Inf,-Inf}` group's sign-flipped NaN
/// (`0xfff8…`) folds alongside genuine NaNs (`0x7ff8…`). A non-deterministic
/// `DataFusion` hash-aggregate fold would flicker by 1 ULP or flip the NaN
/// sign bit; routing through the shared `apply_simple_aggregate` kernel must
/// not.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn sum_avg_collapsed_is_deterministic_and_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();

    // A multi-series group whose float values sum with float-rounding
    // sensitivity: many members of widely different magnitudes, so the
    // accumulation order changes the low bits of the running sum. The
    // operator fold must pick a single canonical order so the result never
    // flickers and equals the interpreter's stable fold.
    let bz_values = [
        1.0,
        1e16,
        -1e16,
        3.0,
        1e-16,
        7.0,
        -2.5,
        1e8,
        -1e8,
        0.1,
        0.2,
        0.3,
        123_456.789,
        -987_654.321,
        2.617_281_828,
        3.041_592_653,
        -1.314_213_562,
        1e10,
        -1e10,
        42.0,
    ];
    for (idx, value) in bz_values.iter().enumerate() {
        let lbls = labels(&[
            ("__name__", "bz_total"),
            ("g", "all"),
            ("instance", &idx.to_string()),
        ]);
        store.push_float("t", lbls, 120_000, *value);
    }

    // Counters for the rate-then-sum/avg case, mirroring the audit's 2m-window
    // rates: a multi-series group whose per-series rates sum with
    // float-rounding sensitivity.
    for (instance, factor) in [
        ("0", 1.0),
        ("1", 1e8),
        ("2", 1e-8),
        ("3", 7.0),
        ("4", 1234.567),
        ("5", -3.5),
    ] {
        let lbls = labels(&[
            ("__name__", "bz_reqs_total"),
            ("g", "all"),
            ("instance", instance),
        ]);
        for step in 0..=2_i32 {
            store.push_float(
                "t",
                lbls.clone(),
                i64::from(step) * 60_000,
                f64::from(step) * factor,
            );
        }
    }

    // A global-fold case mixing a sign-flipped NaN (from `+Inf + -Inf`) with
    // genuine NaNs. `+Inf` and `-Inf` in the same group sum to a NaN whose
    // sign bit is SET (`0xfff8…`) on most platforms, distinct from a genuine
    // payload NaN (`0x7ff8…`). The fold order determines which NaN's bits
    // survive, so the operator path must agree with the interpreter bit-for-
    // bit on the sign bit.
    for (instance, value) in [
        ("a", f64::INFINITY),
        ("b", f64::NEG_INFINITY),
        ("c", f64::NAN),
        ("d", 5.0),
    ] {
        let lbls = labels(&[("__name__", "naninf"), ("instance", instance)]);
        store.push_float("t", lbls, 120_000, value);
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // (query, time_ms). Each is a collapsed/global or `by (g)` sum/avg over a
    // multi-series group, plus the rate-wrapped and naninf cases.
    let cases = [
        ("sum(bz_total)", 120_000_i64),
        ("avg(bz_total)", 120_000),
        ("sum by (g) (bz_total)", 120_000),
        ("avg by (g) (bz_total)", 120_000),
        ("sum(rate(bz_reqs_total[2m]))", 120_000),
        ("avg(rate(bz_reqs_total[2m]))", 120_000),
        ("sum by (g) (rate(bz_reqs_total[2m]))", 120_000),
        ("avg by (g) (rate(bz_reqs_total[2m]))", 120_000),
        ("sum(naninf)", 120_000),
        ("avg(naninf)", 120_000),
    ];

    for (query, time_ms) in cases {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // The interpreter oracle: the reference result.
        let interpreter = engine
            .eval_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
        let QueryResult::InstantVector(mut interpreter) = interpreter else {
            panic!("expected vector for interpreter `{query}`");
        };
        interpreter.sort_by_key(|sample| sample.labels.fingerprint());

        // Run the operator path MANY times: every run must be bit-identical
        // (deterministic) and equal the interpreter bit-for-bit.
        let mut first_bits: Option<Vec<u64>> = None;
        for run in 0..50 {
            let plan = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}` (run {run}): {error}"))
                .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
            let operator = engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap_or_else(|error| panic!("operator `{query}` (run {run}): {error}"));
            let QueryResult::InstantVector(mut operator) = operator else {
                panic!("expected vector for operator `{query}`");
            };
            operator.sort_by_key(|sample| sample.labels.fingerprint());

            // Bit-for-bit parity with the interpreter (NaN sign included).
            assert!(
                instant_samples_match(&interpreter, &operator),
                "operator/interpreter divergence for `{query}` (run {run}): \
                     {interpreter:?} vs {operator:?}"
            );

            // Determinism: capture the exact float bits and require every run
            // to reproduce them.
            let bits: Vec<u64> = operator
                .iter()
                .map(|sample| float_value(&sample.value).to_bits())
                .collect();
            match &first_bits {
                None => first_bits = Some(bits),
                Some(expected) => assert_eq!(
                    &bits, expected,
                    "operator path flickered for `{query}` on run {run}"
                ),
            }
        }
    }
}

/// Compare two whole [`QueryResult`]s for the parity tests below, NaN-aware
/// across scalar / vector / matrix / string shapes (so a genuine NaN equals a
/// genuine NaN). Vectors are pre-sorted by fingerprint by the caller.
fn query_results_match(left: &QueryResult, right: &QueryResult) -> bool {
    match (left, right) {
        (
            QueryResult::Scalar {
                ts_ms: lt,
                value: lv,
            },
            QueryResult::Scalar {
                ts_ms: rt,
                value: rv,
            },
        ) => lt == rt && lv.to_bits() == rv.to_bits(),
        (QueryResult::InstantVector(left), QueryResult::InstantVector(right)) => {
            instant_samples_match(left, right)
        }
        (QueryResult::RangeMatrix(_), QueryResult::RangeMatrix(_)) => {
            range_matrices_match(left, right)
        }
        (
            QueryResult::Str {
                ts_ms: lt,
                value: lv,
            },
            QueryResult::Str {
                ts_ms: rt,
                value: rv,
            },
        ) => lt == rt && lv == rv,
        _ => false,
    }
}

/// Sort an instant-vector result by fingerprint in place (a no-op for the
/// other result shapes), so `query_results_match` can compare vectors order-
/// independently.
fn sort_instant_result(result: QueryResult) -> QueryResult {
    match result {
        QueryResult::InstantVector(mut samples) => {
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            QueryResult::InstantVector(samples)
        }
        other => other,
    }
}

/// Differential parity for the newly-planned top-level structural node kinds:
/// unary negation, bare numeric / string literals, a raw matrix selector and a
/// subquery (both `RangeMatrix` results from `query_instant`), and the
/// `smoothed` extended selector. Each must produce — through the operator
/// planner — the byte-exact result the interpreter's `eval_instant_expr`
/// produces.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn structural_node_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();
    let stale_bits = stale_nan();
    for (lbls, ts, value) in [
        (
            labels(&[("__name__", "m"), ("job", "api")]),
            60_000_i64,
            2.0,
        ),
        (labels(&[("__name__", "m"), ("job", "api")]), 120_000, 3.0),
        (labels(&[("__name__", "m"), ("job", "db")]), 120_000, 7.0),
        // A genuine NaN latest-in-window sample (kept, negated to NaN).
        (
            labels(&[("__name__", "m"), ("job", "nan")]),
            120_000,
            f64::NAN,
        ),
        // A stale marker (dropped on both paths).
        (
            labels(&[("__name__", "m"), ("job", "stale")]),
            120_000,
            stale_bits,
        ),
        // A series with a short history for the matrix / subquery / smoothed
        // shapes.
        (labels(&[("__name__", "g")]), 0, 1.0),
        (labels(&[("__name__", "g")]), 60_000, 2.0),
        (labels(&[("__name__", "g")]), 120_000, 4.0),
        (labels(&[("__name__", "g")]), 180_000, 8.0),
        (labels(&[("__name__", "g")]), 240_000, 16.0),
    ] {
        store.push_float("t", lbls, ts, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries: &[(&str, i64)] = &[
        // Unary over a vector (drops `__name__`, negates each value, keeps a
        // genuine NaN, drops the stale marker).
        ("-m", 120_000),
        // Unary over an aggregate result (vector).
        ("-sum(m)", 120_000),
        // Unary over a scalar.
        ("-(1 + 2)", 120_000),
        // Double negation.
        ("- -m", 120_000),
        // Bare numeric / string literals.
        ("42", 120_000),
        ("-7.5", 120_000),
        (r#""hello""#, 120_000),
        // Raw matrix selector / subquery (RangeMatrix from query_instant).
        ("g[3m]", 240_000),
        ("m[2m]", 120_000),
        ("g[4m:1m]", 240_000),
        // `smoothed` extended selector (vector). The extension parser is not
        // feature-gated, so this routes in both build configs.
        ("smoothed(g)", 90_000),
    ];

    for &(query, time_ms) in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // The recursive planner must claim every one of these.
        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = sort_instant_result(
            engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap_or_else(|error| panic!("operator `{query}`: {error}")),
        );
        let via_interpreter = sort_instant_result(
            engine
                .eval_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}")),
        );

        assert!(
            query_results_match(&via_interpreter, &via_operators),
            "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
        );
    }

    // Pin the result types the parity above relies on.
    for (query, time_ms, want) in [
        ("42", 120_000_i64, "scalar"),
        (r#""hello""#, 120_000, "string"),
        ("g[3m]", 240_000, "matrix"),
        ("g[4m:1m]", 240_000, "matrix"),
        ("-m", 120_000, "vector"),
        ("-(1 + 2)", 120_000, "scalar"),
    ] {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap();
        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let result = engine
            .assemble_planned_instant(plan, time_ms)
            .await
            .unwrap();
        assert_eq!(result.result_type(), want, "`{query}` result type");
    }

    // The `anchored` modifier on an instant-vector selector is the same hard
    // error on both paths.
    {
        let expr = parse_promql_with_duration_context(
            "anchored(m)",
            DurationExprContext::instant(120_000),
        )
        .unwrap();
        let planner_err = engine.plan_instant_expr("t", &expr, 120_000).await;
        let interp_err = engine.eval_instant_expr("t", &expr, 120_000).await;
        assert!(
            planner_err.is_err(),
            "anchored(m) must error on the planner"
        );
        assert!(
            interp_err.is_err(),
            "anchored(m) must error on the interpreter"
        );
    }
}

/// Differential parity for the experimental scalar / range functions:
/// `max_of`/`min_of`, `double_exponential_smoothing` over a bare matrix
/// selector, and the duration helpers. Each delegates to the same interpreter
/// method, so the result is parity-exact by construction.
#[cfg(feature = "experimental-functions")]
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn experimental_call_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();
    for (ts, value) in [
        (0_i64, 1.0),
        (60_000, 2.0),
        (120_000, 4.0),
        (180_000, 8.0),
        (240_000, 16.0),
    ] {
        store.push_float("t", labels(&[("__name__", "g")]), ts, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries: &[(&str, i64)] = &[
        ("max_of(1, 2)", 120_000),
        ("min_of(1, 2)", 120_000),
        ("max_of(scalar(g), 3)", 240_000),
        ("double_exponential_smoothing(g[4m], 0.5, 0.5)", 240_000),
        // Duration helpers (instant query: no range context -> 0 on both
        // paths).
        ("step()", 120_000),
        ("start()", 120_000),
        ("end()", 120_000),
        ("range()", 120_000),
    ];

    for &(query, time_ms) in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = sort_instant_result(
            engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap_or_else(|error| panic!("operator `{query}`: {error}")),
        );
        let via_interpreter = sort_instant_result(
            engine
                .eval_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}")),
        );
        assert!(
            query_results_match(&via_interpreter, &via_operators),
            "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
        );
    }
}

/// Differential parity for the experimental `limitk`/`limit_ratio` param
/// aggregations, including `limit_ratio`'s `InvalidRatioWarning` annotation.
/// The planner reuses the same parameter-resolution helpers and selection
/// kernels as the interpreter, so both the result AND the emitted annotations
/// match.
#[cfg(feature = "experimental-functions")]
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn experimental_param_aggregate_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)] {
        store.push_float(
            "t",
            labels(&[("__name__", "m"), ("job", "api"), ("instance", instance)]),
            120_000,
            value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries: &[&str] = &[
        "limitk(2, m)",
        "limitk(10, m)",
        "limitk(0, m)",
        "limitk(2, m) by (job)",
        "limit_ratio(0.5, m)",
        "limit_ratio(-0.5, m)",
        "limit_ratio(1, m)",
        "limit_ratio(0, m)",
        // Out-of-range ratios: must emit the InvalidRatioWarning on BOTH paths.
        "limit_ratio(1.5, m)",
        "limit_ratio(-2, m)",
    ];
    let time_ms = 120_000_i64;

    for &query in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // Operator path, scoped so the InvalidRatioWarning is captured.
        let (via_operators, operator_annotations) = super::ANNOTATIONS
            .scope(std::cell::RefCell::new(crate::Annotations::new()), async {
                let plan = engine
                    .plan_instant_expr("t", &expr, time_ms)
                    .await
                    .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                    .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
                let result = engine
                    .assemble_planned_instant(plan, time_ms)
                    .await
                    .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));
                let annotations = super::ANNOTATIONS.with(|sink| sink.borrow().clone());
                (sort_instant_result(result), annotations)
            })
            .await;

        // Interpreter path, scoped identically.
        let (via_interpreter, interpreter_annotations) = super::ANNOTATIONS
            .scope(std::cell::RefCell::new(crate::Annotations::new()), async {
                let result = engine
                    .eval_instant_expr("t", &expr, time_ms)
                    .await
                    .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
                let annotations = super::ANNOTATIONS.with(|sink| sink.borrow().clone());
                (sort_instant_result(result), annotations)
            })
            .await;

        assert!(
            query_results_match(&via_interpreter, &via_operators),
            "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
        );
        assert_eq!(
            operator_annotations, interpreter_annotations,
            "annotation divergence for `{query}`"
        );
    }

    // Pin that an out-of-range ratio actually emits the warning (so the
    // equality above is not vacuously comparing two empty sets).
    let expr = parse_promql_with_duration_context(
        "limit_ratio(1.5, m)",
        DurationExprContext::instant(time_ms),
    )
    .unwrap();
    let annotations = super::ANNOTATIONS
        .scope(std::cell::RefCell::new(crate::Annotations::new()), async {
            let plan = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap()
                .unwrap();
            engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap();
            super::ANNOTATIONS.with(|sink| sink.borrow().clone())
        })
        .await;
    assert_eq!(
        annotations.warnings.len(),
        1,
        "unexpected warning text: {:?}",
        annotations.warnings
    );
    assert_eq!(
        annotations.warnings[0].contains("ratio value should be between -1 and 1"),
        true,
        "unexpected warning text: {:?}",
        annotations.warnings
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn classic_histogram_quantile_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A float-only store of classic `<metric>_bucket{le}` series exercising the
    // classic histogram_quantile fold:
    //  - `lat_bucket{job}`: a well-formed monotonic histogram with a real `+Inf`
    //    overflow bucket, in two groups (job=api / job=db) so the multi-group
    //    case and the `__name__` + `le` drop are both observable.
    //  - `nonmono_bucket`: a NON-monotonic cumulative bucket set (the le=2 count
    //    dips below le=1) so the monotonicity-forcing path is taken.
    //  - `inf_only_bucket`: a single `+Inf` bucket (<2 buckets -> NaN).
    //  - `reqs_bucket{le}`: counters for the NESTED
    //    `histogram_quantile(0.9, sum by (le) (rate(reqs_bucket[5m])))` case,
    //    whose fully-float inner plans through the rate + aggregate operators.
    let mut store = InMemoryMetricStore::new();
    for (job, le, value) in [
        ("api", "0.1", 1.0),
        ("api", "0.2", 2.0),
        ("api", "0.4", 4.0),
        ("api", "+Inf", 5.0),
        ("db", "0.1", 0.0),
        ("db", "0.2", 1.0),
        ("db", "0.4", 3.0),
        ("db", "+Inf", 3.0),
    ] {
        let lbls = labels(&[("__name__", "lat_bucket"), ("job", job), ("le", le)]);
        store.push_float("t", lbls, 300_000, value);
    }
    // A non-monotonic cumulative bucket set: le=2 (count 3) dips below le=1
    // (count 5); the fold must force monotonicity before interpolating.
    for (le, value) in [("1", 5.0), ("2", 3.0), ("+Inf", 8.0)] {
        let lbls = labels(&[("__name__", "nonmono_bucket"), ("le", le)]);
        store.push_float("t", lbls, 300_000, value);
    }
    // A single `+Inf` bucket: fewer than two buckets -> NaN.
    store.push_float(
        "t",
        labels(&[("__name__", "inf_only_bucket"), ("le", "+Inf")]),
        300_000,
        7.0,
    );
    // Counters for the nested `histogram_quantile(.., sum by (le) (rate(...)))`
    // case (slope = factor / 60s within the 5m window).
    for (le, factor) in [("0.1", 1.0), ("0.2", 2.0), ("0.4", 4.0), ("+Inf", 5.0)] {
        let lbls = labels(&[("__name__", "reqs_bucket"), ("le", le)]);
        for step in 0..=5_i32 {
            store.push_float(
                "t",
                lbls.clone(),
                i64::from(step) * 60_000,
                f64::from(step) * factor,
            );
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Each query must route through the operator path and match the
    // interpreter byte-for-byte (NaN-aware, bit-exact).
    let queries = [
        // Normal linear interpolation, multi-group (job=api / job=db), with the
        // `__name__` and `le` labels dropped from the output.
        ("histogram_quantile(0.5, lat_bucket)", 300_000_i64),
        ("histogram_quantile(0.9, lat_bucket)", 300_000),
        // phi at the boundaries 0 and 1.
        ("histogram_quantile(0, lat_bucket)", 300_000),
        ("histogram_quantile(1, lat_bucket)", 300_000),
        // phi out of [0, 1]: -Inf below, +Inf above.
        ("histogram_quantile(-0.5, lat_bucket)", 300_000),
        ("histogram_quantile(1.5, lat_bucket)", 300_000),
        // A non-monotonic cumulative bucket set is forced monotonic first.
        ("histogram_quantile(0.5, nonmono_bucket)", 300_000),
        // A single `+Inf` bucket (<2 buckets) yields NaN.
        ("histogram_quantile(0.5, inf_only_bucket)", 300_000),
        // NESTED: a fully-float inner that plans through the rate + aggregate
        // operators, then the classic fold over the assembled bucket vector.
        (
            "histogram_quantile(0.9, sum by (le) (rate(reqs_bucket[5m])))",
            300_000,
        ),
    ];

    for (query, time_ms) in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // Operator path: the recursive planner must claim this query.
        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(plan, time_ms)
            .await
            .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

        // Interpreter path: evaluate the same expression directly.
        let via_interpreter = engine
            .eval_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };

        let via_interpreter = normalize(via_interpreter);
        let via_operators = normalize(via_operators);
        assert!(
            instant_samples_match(&via_interpreter, &via_operators),
            "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
        );

        // Pin the `__name__` + `le` drop on the operator-path output.
        assert!(
            via_operators.iter().all(|sample| {
                sample.labels.get("__name__").is_none() && sample.labels.get("le").is_none()
            }),
            "`{query}`: operator path leaked __name__ / le: {via_operators:?}"
        );
    }
    // The native-histogram flavor of these folds (bare selector, native
    // `histogram_quantile`, the native accessors) now routes through the
    // planner too — see `native_histogram_planner_path_matches_interpreter`.
}

/// Differential parity for the **native-histogram** constructs that now route
/// through the recursive planner: a bare native-histogram selector, native
/// `histogram_quantile`, and every native accessor (`histogram_count`/`sum`/
/// `avg`/`stddev`/`stdvar`/`fraction`). Each query MUST claim the operator
/// (`Precomputed`) path and match the interpreter byte-for-byte, with the
/// histogram payloads compared by value (not float `==`).
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn native_histogram_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // Build a non-trivial native histogram (schema 0, two positive buckets
    // [1,2] and [2,4] carrying counts 1 and 3) with a real count/sum so the
    // quantile, fraction, and stddev/stdvar folds all produce finite values.
    fn seed_histogram(count: f64, sum: f64) -> NativeHistogram {
        let mut histogram = native_histogram(count, sum);
        histogram.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 2,
        }];
        histogram.positive_counts = vec![1.0, 3.0];
        histogram
    }

    // Two native-histogram groups (job=api / job=db) so multi-series output and
    // the `__name__` drop are both observable, plus a classic `cls_bucket{le}`
    // float histogram to exercise the classic+native co-routing inside the
    // shared `histogram_quantile` / `histogram_fraction` folds.
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "t",
        labels(&[("__name__", "nh"), ("job", "api")]),
        300_000,
        seed_histogram(4.0, 6.5),
    );
    store.push_histogram(
        "t",
        labels(&[("__name__", "nh"), ("job", "db")]),
        300_000,
        seed_histogram(8.0, 20.0),
    );
    for (le, value) in [("1", 1.0), ("2", 3.0), ("+Inf", 4.0)] {
        let lbls = labels(&[("__name__", "cls_bucket"), ("le", le)]);
        store.push_float("t", lbls, 300_000, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries = [
        // The bare native-histogram selector itself (carries the histogram
        // payload + full labelset, including `__name__`).
        ("nh", 300_000_i64),
        // Native histogram_quantile at two phis.
        ("histogram_quantile(0.5, nh)", 300_000),
        ("histogram_quantile(0.9, nh)", 300_000),
        // Every native accessor.
        ("histogram_count(nh)", 300_000),
        ("histogram_sum(nh)", 300_000),
        ("histogram_avg(nh)", 300_000),
        ("histogram_stddev(nh)", 300_000),
        ("histogram_stdvar(nh)", 300_000),
        // histogram_fraction carries two scalar bounds.
        ("histogram_fraction(1, 2, nh)", 300_000),
        ("histogram_fraction(-Inf, +Inf, nh)", 300_000),
        // The shared folds also work over the classic float buckets.
        ("histogram_quantile(0.5, cls_bucket)", 300_000),
        ("histogram_fraction(1, 2, cls_bucket)", 300_000),
    ];

    for (query, time_ms) in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // The recursive planner must claim this query (the `Precomputed` path).
        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(plan, time_ms)
            .await
            .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

        let via_interpreter = engine
            .eval_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };

        let via_interpreter = normalize(via_interpreter);
        let via_operators = normalize(via_operators);
        // `instant_samples_match` compares histogram payloads structurally
        // (via `SampleValue` `PartialEq`) and floats bit-exactly.
        assert!(
            instant_samples_match(&via_interpreter, &via_operators),
            "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
        );
    }

    // The bare-selector case must surface the native histogram payload (proving
    // the histogram-aware selection actually carried it, not a dropped/empty
    // vector).
    let bare = parse_promql_with_duration_context("nh", DurationExprContext::instant(300_000))
        .expect("parse nh");
    let plan = engine
        .plan_instant_expr("t", &bare, 300_000)
        .await
        .expect("plan nh")
        .expect("nh routes through planner");
    let QueryResult::InstantVector(samples) = engine
        .assemble_planned_instant(plan, 300_000)
        .await
        .expect("assemble nh")
    else {
        panic!("expected vector for nh");
    };
    assert_eq!(
        samples.len(),
        2,
        "bare native selector must carry histogram payloads, got: {samples:?}"
    );
    assert!(
        samples
            .iter()
            .all(|sample| matches!(sample.value, SampleValue::Histogram(_))),
        "bare native selector must carry histogram payloads, got: {samples:?}"
    );

    // `histogram_quantiles` (experimental) now routes through the shared
    // `apply_histogram_quantiles` fold and must match the interpreter for both
    // native-histogram and classic bucket inputs, across multiple phis.
    #[cfg(feature = "experimental-functions")]
    for query in [
        "histogram_quantiles(nh, \"q\", 0.5, 0.9)",
        "histogram_quantiles(cls_bucket, \"q\", 0.5)",
    ] {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(300_000))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let plan = engine
            .plan_instant_expr("t", &expr, 300_000)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(plan, 300_000)
            .await
            .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));
        let via_interpreter = engine
            .eval_instant_expr("t", &expr, 300_000)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };
        assert!(
            instant_samples_match(&normalize(via_interpreter), &normalize(via_operators)),
            "histogram_quantiles planner/interpreter divergence for `{query}`"
        );
    }
}

/// Differential parity for **histogram-bearing aggregations** that now route
/// through the recursive planner via the shared `apply_simple_aggregate` /
/// `apply_*` kernels (the `Precomputed` path). Each query MUST claim the
/// operator path and match the interpreter byte-for-byte — including the
/// native-histogram payloads (compared structurally, not by float `==`) and
/// any warning/info annotations.
///
/// The store exercises every native-histogram aggregation rule:
/// - `sum`/`avg` MERGE compatible histograms (and `avg` scales by `1/count`);
/// - a group that MIXES a float and a histogram is DROPPED (the mixed-sample
///   rule) under `sum`/`avg`;
/// - `count`/`group` count every sample regardless of type;
/// - `min`/`max`/`stddev`/`stdvar`/`topk`/`bottomk`/`quantile` IGNORE
///   histogram samples (drop them), reducing only the floats;
/// - `count_values` formats a histogram value as its JSON label value.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn histogram_aggregation_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A native histogram with two positive buckets so the merge / quantile
    // folds produce non-trivial structure.
    fn seed_histogram(count: f64, sum: f64) -> NativeHistogram {
        let mut histogram = native_histogram(count, sum);
        histogram.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 2,
        }];
        histogram.positive_counts = vec![1.0, 3.0];
        histogram
    }

    let mut store = InMemoryMetricStore::new();
    // Group g="hist": TWO compatible native histograms (so sum/avg actually
    // merge), across an `instance` dimension so `without (instance)` collapses
    // them into one group.
    store.push_histogram(
        "t",
        labels(&[("__name__", "m"), ("g", "hist"), ("instance", "0")]),
        300_000,
        seed_histogram(4.0, 6.0),
    );
    store.push_histogram(
        "t",
        labels(&[("__name__", "m"), ("g", "hist"), ("instance", "1")]),
        300_000,
        seed_histogram(8.0, 20.0),
    );
    // Group g="float": TWO float members (so the float aggregations reduce a
    // real group and `count`/`group` see floats).
    for (instance, value) in [("0", 2.0), ("1", 6.0)] {
        store.push_float(
            "t",
            labels(&[("__name__", "m"), ("g", "float"), ("instance", instance)]),
            300_000,
            value,
        );
    }
    // Group g="mixed": ONE float + ONE histogram in the same group. Under
    // `sum`/`avg` this group is dropped (mixed-sample rule); under
    // `count`/`group` it counts 2; under the histogram-ignoring ops only the
    // float survives.
    store.push_float(
        "t",
        labels(&[("__name__", "m"), ("g", "mixed"), ("instance", "0")]),
        300_000,
        10.0,
    );
    store.push_histogram(
        "t",
        labels(&[("__name__", "m"), ("g", "mixed"), ("instance", "1")]),
        300_000,
        seed_histogram(2.0, 3.0),
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries = [
        // sum/avg MERGE histograms per group; the mixed group is dropped.
        ("sum by (g) (m)", 300_000_i64),
        ("avg by (g) (m)", 300_000),
        ("sum without (instance) (m)", 300_000),
        ("avg without (instance) (m)", 300_000),
        // Global sum over everything: the lone global group mixes floats and
        // histograms, so it is dropped entirely (empty result).
        ("sum(m)", 300_000),
        // count/group count every sample regardless of type.
        ("count by (g) (m)", 300_000),
        ("group by (g) (m)", 300_000),
        ("count without (instance) (m)", 300_000),
        ("count(m)", 300_000),
        // min/max/stddev/stdvar IGNORE histograms (reduce only floats); the
        // all-histogram g="hist" group produces no row, g="float" reduces its
        // two floats, g="mixed" reduces just its one float.
        ("min by (g) (m)", 300_000),
        ("max by (g) (m)", 300_000),
        ("stddev by (g) (m)", 300_000),
        ("stdvar by (g) (m)", 300_000),
        // topk/bottomk/quantile also IGNORE histograms.
        ("topk by (g) (1, m)", 300_000),
        ("bottomk by (g) (1, m)", 300_000),
        ("quantile by (g) (0.5, m)", 300_000),
        // count_values formats histogram values as JSON label values.
        ("count_values by (g) (\"v\", m)", 300_000),
    ];

    for (query, time_ms) in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // The recursive planner must claim this query (the `Precomputed` path),
        // and its annotations must match the interpreter's. Scope an annotation
        // sink around each path so emitted warnings/infos are captured.
        let (via_operators, operator_annotations) = super::ANNOTATIONS
            .scope(std::cell::RefCell::new(crate::Annotations::new()), async {
                let plan = engine
                    .plan_instant_expr("t", &expr, time_ms)
                    .await
                    .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                    .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
                let result = engine
                    .assemble_planned_instant(plan, time_ms)
                    .await
                    .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));
                let annotations = super::ANNOTATIONS.with(|sink| sink.borrow().clone());
                (result, annotations)
            })
            .await;

        let (via_interpreter, interpreter_annotations) = super::ANNOTATIONS
            .scope(std::cell::RefCell::new(crate::Annotations::new()), async {
                let result = engine
                    .eval_instant_expr("t", &expr, time_ms)
                    .await
                    .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
                let annotations = super::ANNOTATIONS.with(|sink| sink.borrow().clone());
                (result, annotations)
            })
            .await;

        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };

        let via_interpreter = normalize(via_interpreter);
        let via_operators = normalize(via_operators);
        // `instant_samples_match` compares histogram payloads structurally
        // (via `SampleValue` `PartialEq`) and floats bit-exactly.
        assert!(
            instant_samples_match(&via_interpreter, &via_operators),
            "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
        );
        // Annotation parity: the shared kernel emits identical (here: no)
        // annotations on both paths.
        assert_eq!(
            operator_annotations, interpreter_annotations,
            "`{query}`: annotations diverge"
        );
    }

    // Pin the absolute histogram-aware rules (not just operator==interpreter).
    let sample_by_group =
        |samples: &[crate::InstantSample], g: &str| -> Option<crate::InstantSample> {
            samples
                .iter()
                .find(|sample| sample.labels.get("g") == Some(g))
                .cloned()
        };

    // `sum by (g) (m)`: g="hist" is the MERGED histogram (count 4+8=12,
    // sum 6+20=26), g="float" sums its two floats (2+6=8), g="mixed" is
    // DROPPED (float+histogram).
    let sum_expr =
        parse_promql_with_duration_context("sum by (g) (m)", DurationExprContext::instant(300_000))
            .expect("parse sum");
    let plan = engine
        .plan_instant_expr("t", &sum_expr, 300_000)
        .await
        .expect("plan sum")
        .expect("sum routes through planner");
    let QueryResult::InstantVector(sum_samples) = engine
        .assemble_planned_instant(plan, 300_000)
        .await
        .expect("assemble sum")
    else {
        panic!("expected vector for sum");
    };
    assert!(
        sample_by_group(&sum_samples, "mixed").is_none(),
        "sum: mixed float+histogram group must be dropped, got: {sum_samples:?}"
    );
    let hist_row = sample_by_group(&sum_samples, "hist").expect("sum: g=hist row");
    let SampleValue::Histogram(merged) = hist_row.value else {
        panic!("sum: g=hist must be a merged histogram, got: {hist_row:?}");
    };
    assert!(
        approx_eq(merged.count, 12.0) && approx_eq(merged.sum, 26.0),
        "sum: merged histogram count/sum wrong: {merged:?}"
    );
    let float_row = sample_by_group(&sum_samples, "float").expect("sum: g=float row");
    assert!(
        approx_eq(float_value(&float_row.value), 8.0),
        "sum: g=float must sum its floats to 8, got: {float_row:?}"
    );

    // `min by (g) (m)`: g="hist" (all histograms) yields NO row; g="float"
    // reduces to 2; g="mixed" reduces to just its one float (10).
    let min_expr =
        parse_promql_with_duration_context("min by (g) (m)", DurationExprContext::instant(300_000))
            .expect("parse min");
    let plan = engine
        .plan_instant_expr("t", &min_expr, 300_000)
        .await
        .expect("plan min")
        .expect("min routes through planner");
    let QueryResult::InstantVector(min_samples) = engine
        .assemble_planned_instant(plan, 300_000)
        .await
        .expect("assemble min")
    else {
        panic!("expected vector for min");
    };
    check!(
        sample_by_group(&min_samples, "hist").is_none(),
        "min: all-histogram group must be absent (histograms ignored), got: {min_samples:?}"
    );
    check!(
        approx_eq(
            float_value(
                &sample_by_group(&min_samples, "float")
                    .expect("min g=float")
                    .value
            ),
            2.0
        ),
        "min: g=float must be 2"
    );
    check!(
        approx_eq(
            float_value(
                &sample_by_group(&min_samples, "mixed")
                    .expect("min g=mixed")
                    .value
            ),
            10.0
        ),
        "min: g=mixed must reduce just its float (10)"
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn histogram_range_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A native-histogram counter sample with two positive buckets, so the
    // rate/increase/delta extrapolation produces non-trivial per-bucket
    // structure and the over_time merge folds real buckets.
    fn counter_histogram(count: f64, sum: f64, b0: f64, b1: f64) -> NativeHistogram {
        let mut histogram = native_histogram(count, sum);
        histogram.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 2,
        }];
        histogram.positive_counts = vec![b0, b1];
        histogram
    }

    // A single histogram-counter series sampled monotonically over a 10m
    // window, then a COUNTER RESET (all components drop) so the planner must
    // exercise the shared counter-reset + extrapolation rules. Timestamps are
    // 1m apart so the window `(eval-10m, eval]` captures the full series.
    let mut store = InMemoryMetricStore::new();
    let series = labels(&[("__name__", "h"), ("job", "api")]);
    for (ts, count, sum, b0, b1) in [
        (60_000_i64, 4.0, 6.0, 1.0, 3.0),
        (120_000, 6.0, 10.0, 2.0, 4.0),
        (180_000, 9.0, 15.0, 3.0, 6.0),
        // COUNTER RESET: every component decreases below the prior sample.
        (240_000, 2.0, 3.0, 1.0, 1.0),
        (300_000, 5.0, 8.0, 2.0, 3.0),
    ] {
        store.push_histogram(
            "t",
            series.clone(),
            ts,
            counter_histogram(count, sum, b0, b1),
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let time_ms = 300_000_i64;

    // Each query is a histogram-bearing rate-family / `_over_time` call (or a
    // composition over one). It must route through the recursive planner (the
    // `Precomputed` path), produce a result byte-for-byte identical to the
    // interpreter (histogram payloads compared structurally, floats
    // bit-exactly), and emit identical annotations.
    let queries = [
        // rate-family over histogram counters (counter-reset + extrapolation).
        "rate(h[10m])",
        "increase(h[10m])",
        "delta(h[10m])",
        "irate(h[10m])",
        "idelta(h[10m])",
        // `_over_time` members that MERGE histograms.
        "sum_over_time(h[10m])",
        "avg_over_time(h[10m])",
        // `_over_time` members that are histogram-SAFE (count the samples /
        // pick the latest, regardless of type).
        "count_over_time(h[10m])",
        "last_over_time(h[10m])",
        "present_over_time(h[10m])",
        // `_over_time` members that IGNORE histograms: an all-histogram window
        // yields no float sample, so the series is dropped (empty result).
        "min_over_time(h[10m])",
        "max_over_time(h[10m])",
        "stddev_over_time(h[10m])",
        "stdvar_over_time(h[10m])",
        "quantile_over_time(0.5, h[10m])",
        // Nested: `histogram_quantile` over `rate(h[range])` composes through
        // the operator path (rate produces a histogram, the quantile folds it).
        "histogram_quantile(0.5, rate(h[10m]))",
        // Aggregation over a histogram rate composes through operators.
        "sum(rate(h[10m]))",
        "sum by (job) (increase(h[10m]))",
    ];

    for query in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        let (via_operators, operator_annotations) = super::ANNOTATIONS
            .scope(std::cell::RefCell::new(crate::Annotations::new()), async {
                let plan = engine
                    .plan_instant_expr("t", &expr, time_ms)
                    .await
                    .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                    .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
                let result = engine
                    .assemble_planned_instant(plan, time_ms)
                    .await
                    .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));
                let annotations = super::ANNOTATIONS.with(|sink| sink.borrow().clone());
                (result, annotations)
            })
            .await;

        let (via_interpreter, interpreter_annotations) = super::ANNOTATIONS
            .scope(std::cell::RefCell::new(crate::Annotations::new()), async {
                let result = engine
                    .eval_instant_expr("t", &expr, time_ms)
                    .await
                    .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
                let annotations = super::ANNOTATIONS.with(|sink| sink.borrow().clone());
                (result, annotations)
            })
            .await;

        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };

        let via_interpreter = normalize(via_interpreter);
        let via_operators = normalize(via_operators);
        assert!(
            instant_samples_match(&via_interpreter, &via_operators),
            "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
        );
        assert_eq!(
            operator_annotations, interpreter_annotations,
            "`{query}`: annotations diverge"
        );
    }

    // Pin the absolute rules the parity above relies on (not just
    // operator==interpreter).

    // `rate(h[10m])` yields ONE histogram sample (name dropped), built by the
    // shared counter-reset + extrapolation rules.
    let rate_expr =
        parse_promql_with_duration_context("rate(h[10m])", DurationExprContext::instant(time_ms))
            .expect("parse rate");
    let plan = engine
        .plan_instant_expr("t", &rate_expr, time_ms)
        .await
        .expect("plan rate")
        .expect("rate routes through planner");
    let QueryResult::InstantVector(rate_samples) = engine
        .assemble_planned_instant(plan, time_ms)
        .await
        .expect("assemble rate")
    else {
        panic!("expected vector for rate");
    };
    assert_eq!(
        rate_samples.len(),
        1,
        "rate yields one nameless histogram sample"
    );
    assert_eq!(
        rate_samples[0].labels.get("__name__"),
        None,
        "rate yields one nameless histogram sample"
    );
    assert!(
        matches!(rate_samples[0].value, SampleValue::Histogram(_)),
        "rate yields one nameless histogram sample"
    );

    // `min_over_time(h[10m])` over an all-histogram window yields NO row
    // (histograms ignored).
    let min_expr = parse_promql_with_duration_context(
        "min_over_time(h[10m])",
        DurationExprContext::instant(time_ms),
    )
    .expect("parse min_over_time");
    let plan = engine
        .plan_instant_expr("t", &min_expr, time_ms)
        .await
        .expect("plan min_over_time")
        .expect("min_over_time routes through planner");
    let QueryResult::InstantVector(min_samples) = engine
        .assemble_planned_instant(plan, time_ms)
        .await
        .expect("assemble min_over_time")
    else {
        panic!("expected vector for min_over_time");
    };
    assert!(
        min_samples.is_empty(),
        "min_over_time ignores histograms: all-histogram window yields no row, got: {min_samples:?}"
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn binary_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // NaN-aware sample comparison: labels and ts must match exactly; values
    // match when bit-equal or both NaN (Prometheus treats all NaNs alike).
    fn samples_match(left: &[crate::InstantSample], right: &[crate::InstantSample]) -> bool {
        if left.len() != right.len() {
            return false;
        }
        left.iter().zip(right).all(|(a, b)| {
            a.labels == b.labels
                && a.ts_ms == b.ts_ms
                && match (&a.value, &b.value) {
                    (SampleValue::Float(x), SampleValue::Float(y)) => {
                        x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan())
                    }
                    _ => false,
                }
        })
    }

    // A float-only store with overlapping label dimensions for vector
    // matching. `left`/`right` share `{job}` for one-to-one and group_x
    // matching; `code` differentiates the many side. A NaN row and a series
    // present only on one side exercise NaN preservation and no-match drops.
    let mut store = InMemoryMetricStore::new();
    for (name, job, code, instance, value) in [
        // `left`: one per job (the "one" side for group_left).
        ("left", "api", "", "", 10.0),
        ("left", "db", "", "", 20.0),
        // `right`: many per job (`code` dimension), the "many" side.
        ("right", "api", "200", "", 1.0),
        ("right", "api", "500", "", 2.0),
        ("right", "db", "200", "", 4.0),
        // A `right` series whose job has no `left` match (no-match drop).
        ("right", "web", "200", "", 8.0),
        // `m1`/`m2` for one-to-one on/ignoring matching.
        ("m1", "api", "", "0", 3.0),
        ("m1", "api", "", "1", 5.0),
        ("m2", "api", "", "0", 7.0),
        ("m2", "api", "", "1", 11.0),
        // A genuine-NaN row that must survive vector∘scalar arithmetic.
        ("nanm", "api", "", "0", f64::NAN),
        ("nanm", "api", "", "1", 13.0),
    ] {
        let mut pairs = vec![("__name__", name), ("job", job)];
        if !code.is_empty() {
            pairs.push(("code", code));
        }
        if !instance.is_empty() {
            pairs.push(("instance", instance));
        }
        store.push_float("t", labels(&pairs), 60_000, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let time_ms = 60_000_i64;

    // Each query must route through the operator path and match the
    // interpreter byte-for-byte (NaN-aware).
    let queries = [
        // vector ∘ scalar (arithmetic, drops __name__).
        "m1 + 100",
        "m1 * 2",
        // scalar ∘ vector.
        "100 - m1",
        "2 ^ m1",
        // vector ∘ scalar comparison without bool (filters, keeps labelset).
        "m1 > 4",
        // vector ∘ scalar comparison with bool (keeps all, drops __name__).
        "m1 > bool 4",
        // genuine NaN must survive vector∘scalar arithmetic.
        "nanm + 1",
        // vector ∘ vector one-to-one, default matching (drops __name__).
        "m1 + m2",
        "m2 - m1",
        "m1 / m2",
        "m1 % m2",
        "m1 ^ m2",
        "m1 atan2 m2",
        // one-to-one with on / ignoring.
        "m1 + on(job, instance) m2",
        "m1 + ignoring(__name__) m2",
        // one-to-one comparison without bool (keeps LHS labelset incl. name).
        "m1 > m2",
        "m2 >= m1",
        // one-to-one comparison with bool (drops __name__).
        "m1 == bool m2",
        "m1 != bool m2",
        // group_left (many-to-one): the `right` many side copies a label
        // from the `left` one side.
        "right * on(job) group_left left",
        "right + on(job) group_left() left",
        // group_right (one-to-many): the `left` one side, many `right`.
        "left * on(job) group_right right",
        // set ops: and / or / unless, with and without on/ignoring.
        "m1 and m2",
        "m1 or m2",
        "m1 unless m2",
        "right and on(job) left",
        "right unless on(job) left",
        "left or on(job) right",
        // a no-match set op (web has no left): or keeps it, and/unless drop.
        "right and on(job) left",
    ];

    for query in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        // Operator path: the recursive planner must claim this query.
        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(plan, time_ms)
            .await
            .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

        // Interpreter path: evaluate the same expression directly.
        let via_interpreter = engine
            .eval_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };

        let interpreter = normalize(via_interpreter);
        let operators = normalize(via_operators);
        assert!(
            samples_match(&interpreter, &operators),
            "planner/interpreter divergence for `{query}`: interpreter={interpreter:?}, operators={operators:?}"
        );
    }

    // Pin specific behaviors the parity above relies on.
    // 1. `__name__` is dropped for arithmetic.
    let arith = engine.query_instant("t", "m1 + m2", time_ms).await.unwrap();
    let QueryResult::InstantVector(arith) = arith else {
        panic!("expected vector");
    };
    assert!(
        arith.iter().all(|s| s.labels.get("__name__").is_none()),
        "arithmetic must drop __name__"
    );
    // 2. A comparison without `bool` keeps the LHS labelset (incl. __name__).
    let cmp = engine.query_instant("t", "m1 > m2", time_ms).await.unwrap();
    let QueryResult::InstantVector(cmp) = cmp else {
        panic!("expected vector");
    };
    assert!(
        cmp.iter().all(|s| s.labels.get("__name__") == Some("m1")),
        "comparison without bool keeps the LHS metric name"
    );
    // 3. A no-match set op: `right and on(job) left` drops `web` (no left).
    let setop = engine
        .query_instant("t", "right and on(job) left", time_ms)
        .await
        .unwrap();
    let QueryResult::InstantVector(setop) = setop else {
        panic!("expected vector");
    };
    assert!(
        setop.iter().all(|s| s.labels.get("job") != Some("web")),
        "`and` must drop the unmatched `web` series"
    );

    // Scalar ∘ scalar now folds through the planner into a scalar planned
    // result; it must route AND match the interpreter's scalar value+ts.
    for (query, expected) in [
        ("1 + 2", 3.0_f64),
        ("3 * 4 - 1", 11.0_f64),
        ("2 > bool 1", 1.0_f64),
    ] {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap();
        let planned = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("scalar∘scalar `{query}` must route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(planned, time_ms)
            .await
            .unwrap();
        let QueryResult::Scalar { ts_ms, value } = via_operators else {
            panic!("expected scalar for `{query}`");
        };
        assert_eq!(ts_ms, time_ms, "scalar∘scalar `{query}` ts");
        assert!(
            value.to_bits() == expected.to_bits(),
            "scalar∘scalar `{query}` value: got {value}, want {expected}"
        );
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn util_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // NaN-aware vector comparison: labels + ts must match exactly; values
    // match when bit-equal or both NaN (Prometheus treats all NaNs alike).
    fn samples_match(left: &[crate::InstantSample], right: &[crate::InstantSample]) -> bool {
        if left.len() != right.len() {
            return false;
        }
        left.iter().zip(right).all(|(a, b)| {
            a.labels == b.labels
                && a.ts_ms == b.ts_ms
                && match (&a.value, &b.value) {
                    (SampleValue::Float(x), SampleValue::Float(y)) => {
                        x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan())
                    }
                    _ => false,
                }
        })
    }

    // NaN-aware whole-result comparison covering both scalar and vector
    // results, sorting vector samples by fingerprint first.
    fn results_match(left: QueryResult, right: QueryResult) -> bool {
        match (left, right) {
            (
                QueryResult::Scalar {
                    ts_ms: lt,
                    value: lv,
                },
                QueryResult::Scalar {
                    ts_ms: rt,
                    value: rv,
                },
            ) => lt == rt && (lv.to_bits() == rv.to_bits() || (lv.is_nan() && rv.is_nan())),
            (QueryResult::InstantVector(mut l), QueryResult::InstantVector(mut r)) => {
                l.sort_by_key(|sample| sample.labels.fingerprint());
                r.sort_by_key(|sample| sample.labels.fingerprint());
                samples_match(&l, &r)
            }
            _ => false,
        }
    }

    // A float-only store. `m{job}` carries distinct timestamps per series so
    // `timestamp(m)` differs per row; a single-series metric `solo` exercises
    // `scalar(single)`; `dup` (two series) exercises `scalar(multi)->NaN`. A
    // genuine-NaN row survives `timestamp`/calendar drops. `present` exists,
    // `gone` does not (for absent / absent_over_time).
    let mut store = InMemoryMetricStore::new();
    for (name, job, ts, value) in [
        // Two `m` series at different timestamps within the lookback window.
        ("m", "api", 30_000_i64, 100.0),
        ("m", "db", 60_000, 1_700_000_000.0),
        // A genuine-NaN `m` row (must survive timestamp/calendar, value-> ts).
        ("m", "nan", 45_000, f64::NAN),
        // A single-series metric for scalar(single).
        ("solo", "x", 60_000, 42.5),
        // Two series sharing a name for scalar(multi)->NaN.
        ("dup", "a", 60_000, 1.0),
        ("dup", "b", 60_000, 2.0),
        // A present series for absent(present)->empty / absent_over_time.
        ("present", "p", 55_000, 7.0),
    ] {
        store.push_float("t", labels(&[("__name__", name), ("job", job)]), ts, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let time_ms = 60_000_i64;

    // Each query must route through the operator path AND match the
    // interpreter (NaN-aware), covering both vector and scalar results.
    let queries = [
        // Vector-returning utilities over a plannable inner.
        "timestamp(m)",
        "timestamp(solo)",
        "day_of_week(m)",
        "day_of_month(m)",
        "day_of_year(m)",
        "days_in_month(m)",
        "hour(m)",
        "minute(m)",
        "month(m)",
        "year(m)",
        // vector(scalar) yields a single no-label series.
        "vector(42)",
        "vector(time())",
        // absent / absent_over_time, present and missing.
        "absent(present)",
        "absent(gone)",
        "absent(gone{job=\"z\"})",
        "absent_over_time(present[5m])",
        "absent_over_time(gone[5m])",
        "absent_over_time(gone{job=\"z\"}[5m])",
        // Scalar-returning utilities.
        "time()",
        "pi()",
        "scalar(solo)",
        "scalar(dup)",
        // Argless calendar forms operate on time().
        "hour()",
        "year()",
        // scalar∘scalar arithmetic folds to a scalar.
        "2 + 3 * 4",
        // calendar over a scalar-arg utility (vector arg).
        "timestamp(vector(1700000000))",
    ];

    for query in queries {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

        let plan = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
            .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
        let via_operators = engine
            .assemble_planned_instant(plan, time_ms)
            .await
            .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));
        let via_interpreter = engine
            .eval_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

        assert!(
            results_match(via_interpreter.clone(), via_operators.clone()),
            "planner/interpreter divergence for `{query}`: interpreter={via_interpreter:?}, operators={via_operators:?}"
        );
    }

    // Pin specific behaviors the parity above relies on.
    // 1. scalar(single) returns the lone value; scalar(multi) returns NaN.
    let QueryResult::Scalar { value: single, .. } = engine
        .query_instant("t", "scalar(solo)", time_ms)
        .await
        .unwrap()
    else {
        panic!("expected scalar");
    };
    assert!(single.to_bits() == 42.5_f64.to_bits(), "scalar(single)");
    let QueryResult::Scalar { value: multi, .. } = engine
        .query_instant("t", "scalar(dup)", time_ms)
        .await
        .unwrap()
    else {
        panic!("expected scalar");
    };
    assert!(multi.is_nan(), "scalar(multi) must be NaN");

    // 2. time() and pi() are the eval-time seconds and π.
    let QueryResult::Scalar {
        ts_ms: returned_ts,
        value: eval_seconds,
    } = engine.query_instant("t", "time()", time_ms).await.unwrap()
    else {
        panic!("expected scalar");
    };
    assert_eq!(returned_ts, time_ms);
    assert!(
        eval_seconds.to_bits() == 60.0_f64.to_bits(),
        "time() seconds"
    );
    let QueryResult::Scalar { value: pi_v, .. } =
        engine.query_instant("t", "pi()", time_ms).await.unwrap()
    else {
        panic!("expected scalar");
    };
    assert!(
        pi_v.to_bits() == std::f64::consts::PI.to_bits(),
        "pi() value"
    );

    // 3. absent(present) is empty; absent(gone{job="z"}) carries the matcher
    //    label and value 1.
    let QueryResult::InstantVector(present) = engine
        .query_instant("t", "absent(present)", time_ms)
        .await
        .unwrap()
    else {
        panic!("expected vector");
    };
    assert!(present.is_empty(), "absent(present) must be empty");
    let QueryResult::InstantVector(gone) = engine
        .query_instant("t", "absent(gone{job=\"z\"})", time_ms)
        .await
        .unwrap()
    else {
        panic!("expected vector");
    };
    assert_eq!(gone.len(), 1, "absent(missing) result");
    assert_eq!(
        gone[0].labels.get("job"),
        Some("z"),
        "absent(missing) result"
    );
    assert_eq!(
        gone[0].labels.get("__name__"),
        None,
        "absent(missing) result"
    );
    assert!(
        float_value(&gone[0].value).to_bits() == 1.0_f64.to_bits(),
        "absent value is 1"
    );

    // 4. timestamp(m) reports each sample's own timestamp in seconds, not the
    //    eval time, and drops __name__.
    let QueryResult::InstantVector(ts_samples) = engine
        .query_instant("t", "timestamp(m)", time_ms)
        .await
        .unwrap()
    else {
        panic!("expected vector");
    };
    assert!(
        ts_samples
            .iter()
            .all(|s| s.labels.get("__name__").is_none()),
        "timestamp drops __name__"
    );
    let by_job: std::collections::BTreeMap<&str, f64> = ts_samples
        .iter()
        .map(|s| (s.labels.get("job").unwrap(), float_value(&s.value)))
        .collect();
    for (job, want) in [("api", 30.0_f64), ("db", 60.0), ("nan", 45.0)] {
        check!(
            by_job[&job].to_bits() == want.to_bits(),
            "timestamp {job} row"
        );
    }
}

/// Corpus green-through-the-public-entry-points guard.
///
/// Runs the FULL conformance corpus through the public `query_instant` /
/// `query_range` entry points (exactly as the conformance harness does) and
/// asserts every file passes. With the tree-walking interpreter deleted, the
/// operator planner is the SOLE evaluation engine reached from these entry
/// points, so a green corpus here is a green corpus through the planner. The
/// direct totality proof (every valid query plans to `Ok(Some)`, every invalid
/// one to `Err`, never `Ok(None)`) lives in
/// [`plan_instant_expr_is_total_over_construct_sweep`].
#[tokio::test]
async fn conformance_corpus_runs_green_through_planner() {
    use crate::conformance::testkit::run_corpus_dir;

    let report = run_corpus_dir("tests/testdata").await;
    // Sanity: the corpus actually ran (no path/setup error swallowed the run).
    assert!(!report.files.is_empty(), "corpus produced no files");
    assert!(
        report.files.iter().all(|file| file.passed),
        "corpus regressed: {:?}",
        report
            .files
            .iter()
            .filter(|file| !file.passed)
            .collect::<Vec<_>>()
    );
}

/// Direct totality assertion over a representative construct sweep: for every
/// VALID query family the corpus can produce, `plan_instant_expr` must return
/// `Ok(Some(..))` (it routes through the planner) — never `Ok(None)`. For
/// every INVALID query, it must return `Err(..)` — never `Ok(None)`. This is
/// the per-construct complement to the corpus-wide counter proof.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn plan_instant_expr_is_total_over_construct_sweep() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();
    let time_ms = 300_000_i64;
    for (lbls, samples) in [
        (
            labels(&[("__name__", "m"), ("job", "a")]),
            vec![(120_000_i64, 1.0_f64), (240_000, 2.0), (300_000, 3.0)],
        ),
        (
            labels(&[("__name__", "m"), ("job", "b")]),
            vec![(120_000, 4.0), (240_000, 5.0), (300_000, 6.0)],
        ),
        (
            labels(&[("__name__", "n"), ("job", "a")]),
            vec![(120_000, 7.0), (240_000, 8.0), (300_000, 9.0)],
        ),
    ] {
        for (ts, value) in samples {
            store.push_float("t", lbls.clone(), ts, value);
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // VALID families: each MUST plan to Some (never None, never Err).
    let valid: &[&str] = &[
        // Leaves / literals.
        "m",
        "42",
        "\"hello\"",
        "m offset 1m",
        "m @ 100",
        // Parenthesized.
        "(m)",
        "((m + 1))",
        // Unary.
        "-m",
        "- (m + 1)",
        // Binary: vector∘vector, vector∘scalar, scalar∘scalar, set ops.
        "m + n",
        "m * 2",
        "2 + 3",
        "m and n",
        "m or n",
        "m unless n",
        "m > 5",
        "m == bool 5",
        "sum(m) / sum(n)",
        // Simple aggregations.
        "sum(m)",
        "sum by (job) (m)",
        "avg without (job) (m)",
        "count(m)",
        "min(m)",
        "max(m)",
        "group(m)",
        // Param aggregations.
        "topk(1, m)",
        "bottomk(1, m)",
        "quantile(0.5, m)",
        "count_values(\"v\", m)",
        "stddev(m)",
        "stdvar(m)",
        "stddev by (job) (m)",
        // Rate-family + over_time range calls.
        "rate(m[5m])",
        "increase(m[5m])",
        "delta(m[5m])",
        "irate(m[5m])",
        "idelta(m[5m])",
        "avg_over_time(m[5m])",
        "sum_over_time(m[5m])",
        "count_over_time(m[5m])",
        "min_over_time(m[5m])",
        "max_over_time(m[5m])",
        "stddev_over_time(m[5m])",
        "stdvar_over_time(m[5m])",
        "last_over_time(m[5m])",
        "present_over_time(m[5m])",
        "quantile_over_time(0.5, m[5m])",
        // Aggregation over a range call (compositional).
        "sum by (job) (rate(m[5m]))",
        "max without (job) (avg_over_time(m[5m]))",
        // Scalar-math per-row calls.
        "abs(m)",
        "ceil(m)",
        "floor(m)",
        "round(m)",
        "round(m, 2)",
        "clamp(m, 1, 5)",
        "clamp_min(m, 2)",
        "clamp_max(m, 4)",
        "sqrt(m)",
        "exp(m)",
        "ln(m)",
        "log2(m)",
        "log10(m)",
        "sgn(m)",
        // Trig.
        "sin(m)",
        "cos(m)",
        "tan(m)",
        // Label ops.
        "label_replace(m, \"x\", \"y\", \"job\", \"(.*)\")",
        "label_join(m, \"x\", \"-\", \"job\")",
        "sort(m)",
        "sort_desc(m)",
        // Utilities.
        "time()",
        "pi()",
        "scalar(sum(m))",
        "vector(1)",
        "timestamp(m)",
        "absent(m)",
        "absent(nonexistent_metric)",
        "absent_over_time(m[5m])",
        "day_of_week()",
        "day_of_month(m)",
        "minute()",
        "hour()",
        // Histogram-quantile over a classic bucket vector (float series).
        "histogram_quantile(0.5, m)",
        // Top-level raw matrix selector / subquery (instant query).
        "m[5m]",
        "m[5m:1m]",
        "rate(m[5m:1m])",
        "sum_over_time(m[5m:1m])",
    ];
    for query in valid {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse valid `{query}`: {error}"));
        let planned = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap_or_else(|error| panic!("plan valid `{query}` errored: {error}"));
        assert!(
            planned.is_some(),
            "VALID `{query}` returned Ok(None) — plan_instant_expr is not total"
        );
    }

    // INVALID families: each MUST surface as Err (never Ok(None), never
    // Ok(Some)). These mirror the corpus `expect fail` cases that previously
    // deferred to the interpreter purely to raise the canonical error.
    let invalid: &[&str] = &[
        // Non-scalar / out-of-range / NaN scalar params.
        "quantile_over_time(m, m[5m])",
        "topk(m, m)",
        "quantile(m, m)",
        "clamp(m, m, 5)",
        "round(m, m)",
        "histogram_quantile(m, m)",
        // Non-string-literal label args.
        "label_replace(m, m, \"y\", \"job\", \"(.*)\")",
        "label_join(m, m, \"-\", \"job\")",
        "count_values(m, m)",
        "sort_by_label(m, m)",
        // Wrong arity.
        "time(m)",
        "pi(m)",
        "scalar(m, m)",
        "vector(m, m)",
        "timestamp(m, m)",
        "label_replace(m, \"x\")",
        "histogram_quantile(0.5)",
        // Type mismatch in a binary op (vector op range).
        "m + m[5m]",
    ];
    for query in invalid {
        let Ok(expr) =
            parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
        else {
            // A parse-time rejection is also a total outcome (never reaches
            // the planner), so a query the parser rejects is acceptable here.
            continue;
        };
        let outcome = engine.plan_instant_expr("t", &expr, time_ms).await;
        let kind = match &outcome {
            Ok(Some(_)) => "Ok(Some)",
            Ok(None) => "Ok(None)",
            Err(_) => "Err",
        };
        assert!(
            outcome.is_err(),
            "INVALID `{query}` did not raise a planner-side Err (got {kind}) — \
                 the planner still defers this error to the interpreter via Ok(None)"
        );
    }
}
