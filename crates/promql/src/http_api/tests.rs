use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use crabka_blockstore::{LabelMatcher, Labels};
use crabka_metrics::{Limits, OverridesProvider};
use tower::ServiceExt;

use super::*;
use crate::{
    ExemplarRecord, InMemoryMetricStore, LabelNameCardinality, LabelValueCardinality,
    MetadataRecord, ScanResult, TsdbBlock, TsdbHeadStats, TsdbStats,
};

struct SlowEmptyStore {
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
}

impl SlowEmptyStore {
    fn new(active: Arc<AtomicUsize>, max_active: Arc<AtomicUsize>) -> Self {
        Self { active, max_active }
    }

    async fn enter(&self) {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        let mut current = self.max_active.load(Ordering::SeqCst);
        while active > current {
            match self.max_active.compare_exchange(
                current,
                active,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl MetricStore for SlowEmptyStore {
    async fn scan(
        &self,
        _tenant: &str,
        _matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<ScanResult, PromqlError> {
        self.enter().await;
        Ok(ScanResult {
            ctx: datafusion::prelude::SessionContext::new(),
            float_table: None,
            histogram_table: None,
        })
    }

    async fn label_names(
        &self,
        _tenant: &str,
        _matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<String>, PromqlError> {
        Ok(Vec::new())
    }

    async fn label_values(
        &self,
        _tenant: &str,
        _name: &str,
        _matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<String>, PromqlError> {
        Ok(Vec::new())
    }

    async fn series(
        &self,
        _tenant: &str,
        _matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<Labels>, PromqlError> {
        Ok(Vec::new())
    }

    async fn exemplars(
        &self,
        _tenant: &str,
        _matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<ExemplarRecord>, PromqlError> {
        Ok(Vec::new())
    }

    async fn metadata(
        &self,
        _tenant: &str,
        _metric: Option<&str>,
    ) -> Result<Vec<MetadataRecord>, PromqlError> {
        Ok(Vec::new())
    }

    async fn cardinality_label_names(
        &self,
        _tenant: &str,
    ) -> Result<Vec<LabelNameCardinality>, PromqlError> {
        Ok(Vec::new())
    }

    async fn cardinality_label_values(
        &self,
        _tenant: &str,
    ) -> Result<Vec<LabelValueCardinality>, PromqlError> {
        Ok(Vec::new())
    }

    async fn cardinality_active_series(&self, _tenant: &str) -> Result<Vec<Labels>, PromqlError> {
        Ok(Vec::new())
    }

    async fn tsdb_stats(&self, _tenant: &str) -> Result<TsdbStats, PromqlError> {
        Ok(TsdbStats {
            head_stats: TsdbHeadStats {
                num_series: 0,
                num_samples: 0,
                num_chunks: 0,
                min_time: 0,
                max_time: 0,
            },
            series_count_by_metric_name: Vec::new(),
            label_value_count_by_label_name: Vec::new(),
            memory_in_bytes_by_label_name: Vec::new(),
            series_count_by_label_value_pair: Vec::new(),
        })
    }

    async fn tsdb_blocks(&self, _tenant: &str) -> Result<Vec<TsdbBlock>, PromqlError> {
        Ok(Vec::new())
    }
}

#[test]
fn float_formatting_matches_go() {
    // Matches Go's strconv.AppendFloat(f, fmt, -1, 64) selection used by
    // Prometheus jsonutil.MarshalFloat.
    assert_eq!(format_sample_value(1.0), "1");
    assert_eq!(format_sample_value(1.5), "1.5");
    assert_eq!(format_sample_value(0.0), "0");
    assert_eq!(format_sample_value(-0.0), "-0");
    assert_eq!(format_sample_value(3.0), "3");
    assert_eq!(format_sample_value(0.5), "0.5");
    // 1e20 stays in 'f' form (abs < 1e21).
    assert_eq!(format_sample_value(1e20), "100000000000000000000");
    // 1e21 is the boundary where 'e' form kicks in.
    assert_eq!(format_sample_value(1e21), "1e+21");
    // 1e-6 is NOT < 1e-6, so it stays in 'f' form.
    assert_eq!(format_sample_value(1e-6), "0.000001");
    // Just below 1e-6 switches to 'e' form.
    assert_eq!(format_sample_value(9.999e-7), "9.999e-07");
    assert_eq!(format_sample_value(1.5e-7), "1.5e-07");
    assert_eq!(format_sample_value(f64::NAN), "NaN");
    assert_eq!(format_sample_value(f64::INFINITY), "+Inf");
    assert_eq!(format_sample_value(f64::NEG_INFINITY), "-Inf");
    // Very long decimal: shortest round-trip representation.
    assert_eq!(format_sample_value(0.1 + 0.2), "0.30000000000000004");
    assert_eq!(format_sample_value(-1234.5678), "-1234.5678");
    // Negative exponent boundary and large magnitudes.
    assert_eq!(format_sample_value(-1e21), "-1e+21");
    assert_eq!(format_sample_value(1.234e30), "1.234e+30");
}

#[test]
fn expand_alert_template_substitutions() {
    let mut labels = Labels::new();
    labels.insert("job", "api");
    labels.insert("instance", "host-1");

    assert_eq!(
        expand_alert_template("value is {{ $value }}", 42.5, &labels),
        "value is 42.5"
    );
    assert_eq!(
        expand_alert_template("job={{ $labels.job }}", 1.0, &labels),
        "job=api"
    );
    assert_eq!(
        expand_alert_template("job={{ $labels.\"job\" }}", 1.0, &labels),
        "job=api"
    );
    // Absent label expands to empty string.
    assert_eq!(
        expand_alert_template("x={{ $labels.missing }}", 1.0, &labels),
        "x="
    );
    // Unknown actions pass through verbatim.
    assert_eq!(
        expand_alert_template("{{ humanize $value }}", 1.0, &labels),
        "{{ humanize $value }}"
    );
    // No-whitespace variants still expand.
    assert_eq!(
        expand_alert_template("{{$value}} {{$labels.job}}", 7.0, &labels),
        "7 api"
    );
}

#[tokio::test]
async fn query_range_rejects_ranges_over_tenant_limit() {
    let limits = Limits {
        max_query_length_secs: 60,
        ..Limits::default()
    };
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default())
            .with_query_limits(OverridesProvider::new(limits)),
    );

    let response = prometheus_router(state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=up&start=0&end=120&step=60")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "execution");
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("query range too long")
    );
}

#[tokio::test]
async fn query_range_rejects_resolution_over_point_cap_without_limits() {
    // No per-tenant query_limits configured: Prometheus enforces the
    // 11000-point resolution cap unconditionally. start=0 end=20000 step=1s
    // => 20000 intervals > 11000.
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));

    let response = prometheus_router(state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=up&start=0&end=20000&step=1")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "bad_data");
    assert_eq!(
        body["error"],
        "exceeded maximum resolution of 11,000 points per timeseries. \
         Try decreasing the query resolution (?step=XX)"
    );
}

#[tokio::test]
async fn instant_query_without_time_defaults_to_current_time() {
    let mut store = InMemoryMetricStore::new();
    let mut labels = Labels::new();
    labels.insert("__name__", "up");
    labels.insert("job", "api");
    store.push_float("tenant-a", labels, unix_now_ms().unwrap(), 1.0);

    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));

    let response = prometheus_router(state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=up")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["result"][0]["metric"]["job"], "api");
    assert_eq!(body["data"]["result"][0]["value"][1], "1");
}

#[tokio::test]
async fn query_handlers_respect_configured_concurrency_limit() {
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let state = Arc::new(
        PrometheusApiState::new(
            Arc::new(SlowEmptyStore::new(
                Arc::clone(&active),
                Arc::clone(&max_active),
            )),
            EngineOpts::default(),
        )
        .with_max_concurrent_queries(1),
    );
    let router = prometheus_router(state);

    let one = router.clone().oneshot(
        Request::builder()
            .uri("/api/v1/query?query=up")
            .header("x-scope-orgid", "tenant-a")
            .body(Body::empty())
            .unwrap(),
    );
    let two = router.oneshot(
        Request::builder()
            .uri("/api/v1/query?query=up")
            .header("x-scope-orgid", "tenant-a")
            .body(Body::empty())
            .unwrap(),
    );

    let (one, two) = tokio::join!(one, two);
    assert_eq!(one.unwrap().status(), StatusCode::OK);
    assert_eq!(two.unwrap().status(), StatusCode::OK);
    assert_eq!(max_active.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn rejects_tenant_id_with_unsupported_character() {
    // dskit ValidTenantID rejects characters outside [a-zA-Z0-9] and the
    // allowed punctuation set; '/' is forbidden.
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));

    let response = prometheus_router(state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=up")
                .header("x-scope-orgid", "tenant/a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "bad_data");
    // The reason comes from the shared `crabka_metrics::validate_tenant`.
    assert_eq!(
        body["error"],
        "tenant ID contains unsupported character `/`"
    );
}

#[tokio::test]
async fn series_rejects_selected_series_over_tenant_limit() {
    let mut store = InMemoryMetricStore::new();
    let mut api_labels = Labels::new();
    api_labels.insert("__name__", "up");
    api_labels.insert("job", "api");
    store.push_float("tenant-a", api_labels, 0, 1.0);
    let mut worker_labels = Labels::new();
    worker_labels.insert("__name__", "up");
    worker_labels.insert("job", "worker");
    store.push_float("tenant-a", worker_labels, 0, 1.0);

    let limits = Limits {
        max_fetched_series_per_query: 1,
        ..Limits::default()
    };
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default())
            .with_query_limits(OverridesProvider::new(limits)),
    );

    let response = prometheus_router(state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/series?match[]=up&start=0&end=1")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "execution");
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("series per query exceeded")
    );
}

#[tokio::test]
async fn cardinality_active_series_rejects_over_tenant_limit() {
    let mut store = InMemoryMetricStore::new();
    let mut api_labels = Labels::new();
    api_labels.insert("__name__", "up");
    api_labels.insert("job", "api");
    store.push_float("tenant-a", api_labels, 0, 1.0);
    let mut worker_labels = Labels::new();
    worker_labels.insert("__name__", "up");
    worker_labels.insert("job", "worker");
    store.push_float("tenant-a", worker_labels, 0, 1.0);

    let limits = Limits {
        max_fetched_series_per_query: 1,
        ..Limits::default()
    };
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default())
            .with_query_limits(OverridesProvider::new(limits)),
    );

    let response = prometheus_router(state)
        .oneshot(
            Request::builder()
                .uri("/api/v1/cardinality/active_series")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "execution");
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("series per query exceeded")
    );
}
