//! Docker-backed differential probes against real Grafana Tempo.
//!
//! These tests are ignored by default because they pull and run upstream Docker
//! images. Run explicitly with:
//!
//! `cargo test -p crabka-traces --test tempo_differential -- --ignored --nocapture`

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use assert2::assert;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use crabka_traceql::{
    AttrValue as TraceqlAttrValue, EngineOpts, InMemorySpanStore, InputSpan, TraceqlEngine,
};
use crabka_traces::distributor::{self, DistributorState, WalSink};
use crabka_traces::metricsgen::{
    EdgeStore, MetricsGenConfig, RecordOutcome, Series, SeriesSample, SpanKind as MetricsSpanKind,
    SpanRecord as MetricsSpanRecord, StatusCode as MetricsStatusCode,
};
use crabka_traces::{AttrValue, Span, SpanRecord, TracesError};
use http_body_util::BodyExt as _;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, InstrumentationScope, KeyValue as OtlpKeyValue, any_value::Value,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span as OtlpSpan, Status as OtlpStatus, TracesData,
};
use prost::Message as _;
use reqwest::StatusCode as ReqwestStatusCode;
use serde_json::{Value as JsonValue, json};
use testcontainers::core::{Host, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{CopyDataSource, CopyTargetOptions, GenericImage, ImageExt};
use tower::ServiceExt as _;

const TENANT: &str = "tenant-a";
const TRACE_ID_HEX: &str = "01010101010101010101010101010101";
const CHILD_SPAN_ID_HEX: &str = "0303030303030303";
const ERROR_SPAN_ID_HEX: &str = "0404040404040404";
/// OTLP `STATUS_CODE_ERROR`.
const OTLP_STATUS_CODE_ERROR: i32 = 2;
const DOCKER_HOST_ALIAS: &str = "host.testcontainers.internal";
const GRAFANA_TEMPO_DATASOURCE_UID: &str = "crabka-traces";
const TEMPO_CONFIG: &str = r"
multitenancy_enabled: false
server:
  http_listen_port: 3200
distributor:
  receivers:
    otlp:
      protocols:
        http:
          endpoint: 0.0.0.0:4318
storage:
  trace:
    backend: local
    wal:
      path: /tmp/tempo/wal
    local:
      path: /tmp/tempo/blocks
";

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Default)]
struct CapturingSink {
    records: Arc<Mutex<Vec<SpanRecord>>>,
}

#[async_trait::async_trait]
impl WalSink for CapturingSink {
    async fn append(&self, rec: SpanRecord) -> Result<(), TracesError> {
        self.records
            .lock()
            .map_err(|_| TracesError::Wal("capturing sink lock poisoned".into()))?
            .push(rec);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryCaseKind {
    Selector,
    Structural,
    Pipeline,
}

#[derive(Clone, Copy, Debug)]
struct QueryCase {
    kind: QueryCaseKind,
    encoded_query: &'static str,
    expected_span_id: Option<&'static str>,
}

fn differential_search_corpus() -> Vec<QueryCase> {
    vec![
        QueryCase {
            kind: QueryCaseKind::Selector,
            encoded_query: "%7B%20resource.service.name%20%3D%20%22checkout%22%20%7D",
            expected_span_id: None,
        },
        QueryCase {
            kind: QueryCaseKind::Structural,
            encoded_query: "%7B%20.http.method%20%3D%20%22GET%22%20%7D%20%3E%3E%20%7B%20.db.system%20%3D%20%22postgresql%22%20%7D",
            expected_span_id: Some(CHILD_SPAN_ID_HEX),
        },
        QueryCase {
            // `| count() > 0` is a spanset count FILTER, valid in Tempo's search
            // API across versions. (`| by(...)` is a metrics-only stage that
            // real Tempo's /api/search rejects with a parse error, even though
            // Crabka accepts it as a superset.)
            kind: QueryCaseKind::Pipeline,
            encoded_query: "%7B%20resource.service.name%20%3D%20%22checkout%22%20%7D%20%7C%20count()%20%3E%200",
            expected_span_id: None,
        },
    ]
}

#[test]
fn differential_search_corpus_covers_selector_structural_and_pipeline_queries() {
    let corpus = differential_search_corpus();

    let kinds: Vec<QueryCaseKind> = corpus.iter().map(|case| case.kind).collect();
    let has_child_span_expectation = corpus
        .iter()
        .any(|case| case.expected_span_id == Some(CHILD_SPAN_ID_HEX));
    assert!(
        (kinds, has_child_span_expectation)
            == (
                vec![
                    QueryCaseKind::Selector,
                    QueryCaseKind::Structural,
                    QueryCaseKind::Pipeline,
                ],
                true,
            )
    );
}

#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/tempo image"]
async fn real_tempo_and_crabka_match_basic_by_id_and_search() -> TestResult {
    let client = reqwest::Client::new();
    let tempo = start_tempo().await?;
    let tempo_query = mapped_base_url(&tempo, 3200).await?;
    let tempo_otlp = mapped_base_url(&tempo, 4318).await?;
    wait_for_http_ok(&client, &tempo_query, &["/ready", "/status"]).await?;

    let query_range = "start=0&end=1";
    let otlp_body = sample_otlp_body();
    let crabka = start_crabka_pair(&otlp_body).await?;

    post_otlp(
        &client,
        &format!("{tempo_otlp}/v1/traces"),
        None,
        &otlp_body,
    )
    .await?;

    let tempo_trace = get_trace_by_id_until_found(&client, &tempo_query, None, query_range).await?;
    let crabka_trace =
        get_trace_by_id(&client, &crabka.base_url, Some(TENANT), query_range).await?;
    assert_trace_shape_matches(&tempo_trace, &crabka_trace);

    for case in differential_search_corpus() {
        let tempo_search = get_json_until_non_empty_traces(
            &client,
            &format!(
                "{tempo_query}/api/search?q={}&{query_range}",
                case.encoded_query
            ),
            None,
        )
        .await?;
        let crabka_search = get_json(
            &client,
            &format!(
                "{}/api/search?q={}&{query_range}",
                crabka.base_url, case.encoded_query
            ),
            Some(TENANT),
        )
        .await?;
        assert_search_shape_matches(&tempo_search, &crabka_search);
        if let Some(span_id) = case.expected_span_id {
            assert_search_contains_span_id(&tempo_search, span_id);
            assert_search_contains_span_id(&crabka_search, span_id);
        }
    }

    let tempo_tags = get_json(
        &client,
        &format!("{tempo_query}/api/v2/search/tags?{query_range}"),
        None,
    )
    .await?;
    let crabka_tags = get_json(
        &client,
        &format!("{}/api/v2/search/tags?{query_range}", crabka.base_url),
        Some(TENANT),
    )
    .await?;
    assert_required_tag_names_match(&tempo_tags, &crabka_tags);

    let tempo_service_values = get_json(
        &client,
        &format!("{tempo_query}/api/v2/search/tag/resource.service.name/values?{query_range}"),
        None,
    )
    .await?;
    let crabka_service_values = get_json(
        &client,
        &format!(
            "{}/api/v2/search/tag/resource.service.name/values?{query_range}",
            crabka.base_url
        ),
        Some(TENANT),
    )
    .await?;
    assert_required_tag_values_match(&tempo_service_values, &crabka_service_values, "checkout");

    crabka.shutdown();
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/tempo image"]
async fn real_tempo_and_crabka_match_traceql_metrics_query_range() -> TestResult {
    let client = reqwest::Client::new();
    let tempo = start_tempo().await?;
    let tempo_query = mapped_base_url(&tempo, 3200).await?;
    let tempo_otlp = mapped_base_url(&tempo, 4318).await?;
    wait_for_http_ok(&client, &tempo_query, &["/ready", "/status"]).await?;

    let trace_start_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .saturating_sub(60);
    let query_start = trace_start_secs.saturating_sub(60);
    let query_end = trace_start_secs + 120;
    let query_range = format!("start={query_start}&end={query_end}");
    let otlp_body = sample_otlp_body_at(trace_start_secs * 1_000_000_000);
    let crabka = start_crabka_pair(&otlp_body).await?;

    post_otlp(
        &client,
        &format!("{tempo_otlp}/v1/traces"),
        None,
        &otlp_body,
    )
    .await?;

    let metrics_query =
        "%7B%20resource.service.name%20%3D%20%22checkout%22%20%7D%20%7C%20count_over_time()";
    let tempo_metrics = get_json_until_positive_metric_total(
        &client,
        &format!("{tempo_query}/api/metrics/query_range?q={metrics_query}&{query_range}&step=30s"),
        None,
    )
    .await?;
    let crabka_metrics = get_json(
        &client,
        &format!(
            "{}/api/metrics/query_range?q={metrics_query}&{query_range}&step=30s",
            crabka.base_url
        ),
        Some(TENANT),
    )
    .await?;
    assert_metric_totals_match(&tempo_metrics, &crabka_metrics);

    crabka.shutdown();
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/tempo image"]
async fn real_tempo_and_crabka_accept_query_param_alias() -> TestResult {
    // The Grafana Tempo datasource — and therefore the Traces Drilldown
    // breakdown — sends the TraceQL metrics query under `query=`, not `q=`.
    // Real Tempo accepts both spellings; crabka must too, or every breakdown
    // panel 400s with "missing query parameter q" and renders blank. The
    // existing tests all send `q=`, so they could not catch this — this leg
    // mirrors the live datasource exactly.
    let client = reqwest::Client::new();
    let tempo = start_tempo().await?;
    let tempo_query = mapped_base_url(&tempo, 3200).await?;
    let tempo_otlp = mapped_base_url(&tempo, 4318).await?;
    wait_for_http_ok(&client, &tempo_query, &["/ready", "/status"]).await?;

    let trace_start_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .saturating_sub(60);
    let query_start = trace_start_secs.saturating_sub(60);
    let query_end = trace_start_secs + 120;
    let query_range = format!("start={query_start}&end={query_end}");
    let otlp_body = sample_otlp_body_at(trace_start_secs * 1_000_000_000);
    let crabka = start_crabka_pair(&otlp_body).await?;

    post_otlp(
        &client,
        &format!("{tempo_otlp}/v1/traces"),
        None,
        &otlp_body,
    )
    .await?;

    // Identical query to the `q=` test, sent under the `query=` alias.
    let metrics_query =
        "%7B%20resource.service.name%20%3D%20%22checkout%22%20%7D%20%7C%20count_over_time()";
    let tempo_metrics = get_json_until_positive_metric_total(
        &client,
        &format!(
            "{tempo_query}/api/metrics/query_range?query={metrics_query}&{query_range}&step=30s"
        ),
        None,
    )
    .await?;
    let crabka_metrics = get_json(
        &client,
        &format!(
            "{}/api/metrics/query_range?query={metrics_query}&{query_range}&step=30s",
            crabka.base_url
        ),
        Some(TENANT),
    )
    .await?;
    assert_metric_totals_match(&tempo_metrics, &crabka_metrics);

    crabka.shutdown();
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/tempo image"]
async fn real_tempo_and_crabka_match_traceql_metrics_by_labels() -> TestResult {
    // Regression for the Grafana Traces Drilldown breakdown: its per-attribute
    // panels key on the FULL scoped attribute (e.g. `resource.service.name`), so
    // the grouped-series label key must match real Tempo exactly. Crabka
    // previously emitted the scope-stripped key (`service.name`), which left the
    // breakdown blank even though the data and totals were correct.
    let client = reqwest::Client::new();
    let tempo = start_tempo().await?;
    let tempo_query = mapped_base_url(&tempo, 3200).await?;
    let tempo_otlp = mapped_base_url(&tempo, 4318).await?;
    wait_for_http_ok(&client, &tempo_query, &["/ready", "/status"]).await?;

    let trace_start_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .saturating_sub(60);
    let query_start = trace_start_secs.saturating_sub(60);
    let query_end = trace_start_secs + 120;
    let query_range = format!("start={query_start}&end={query_end}");
    let otlp_body = sample_otlp_body_at(trace_start_secs * 1_000_000_000);
    let crabka = start_crabka_pair(&otlp_body).await?;

    post_otlp(
        &client,
        &format!("{tempo_otlp}/v1/traces"),
        None,
        &otlp_body,
    )
    .await?;

    // `{ resource.service.name = "checkout" }` matches every span in the sample
    // trace; group by a resource- and a span-scoped attribute.
    let base = "%7B%20resource.service.name%20%3D%20%22checkout%22%20%7D";
    for by in ["resource.service.name", "span.http.method"] {
        let metrics_query = format!("{base}%20%7C%20rate()%20by%20({by})");
        let tempo_metrics = get_json_until_positive_metric_total(
            &client,
            &format!(
                "{tempo_query}/api/metrics/query_range?q={metrics_query}&{query_range}&step=30s"
            ),
            None,
        )
        .await?;
        let crabka_metrics = get_json(
            &client,
            &format!(
                "{}/api/metrics/query_range?q={metrics_query}&{query_range}&step=30s",
                crabka.base_url
            ),
            Some(TENANT),
        )
        .await?;
        let tempo_keys = metric_series_label_keys(&tempo_metrics);
        let crabka_keys = metric_series_label_keys(&crabka_metrics);
        eprintln!(
            "by({by}): Tempo keys={tempo_keys:?} promLabels={:?}; Crabka keys={crabka_keys:?} promLabels={:?}",
            metric_prom_labels_list(&tempo_metrics),
            metric_prom_labels_list(&crabka_metrics),
        );
        assert!(
            crabka_keys == tempo_keys,
            "by({by}) series label keys differ: Tempo={tempo_keys:?}, Crabka={crabka_keys:?}"
        );
    }
    crabka.shutdown();
    Ok(())
}

#[tokio::test]
async fn crabka_tenant_b_cannot_see_tenant_a_traces_tags_or_values() -> TestResult {
    let client = reqwest::Client::new();
    let query_range = "start=0&end=1";
    let query = "%7B%20resource.service.name%20%3D%20%22checkout%22%20%7D";
    let crabka = start_crabka_pair(&sample_otlp_body()).await?;

    let tenant_b_trace_status = get_status(
        &client,
        &format!(
            "{}/api/v2/traces/{TRACE_ID_HEX}?{query_range}",
            crabka.base_url
        ),
        Some("tenant-b"),
    )
    .await?;
    assert!(tenant_b_trace_status == ReqwestStatusCode::NOT_FOUND);

    let tenant_b_search = get_json(
        &client,
        &format!("{}/api/search?q={query}&{query_range}", crabka.base_url),
        Some("tenant-b"),
    )
    .await?;
    assert_search_empty(&tenant_b_search);

    let tenant_b_tags = get_json(
        &client,
        &format!("{}/api/v2/search/tags?{query_range}", crabka.base_url),
        Some("tenant-b"),
    )
    .await?;
    assert_tag_names_do_not_contain(&tenant_b_tags, "service.name");

    let tenant_b_values = get_json(
        &client,
        &format!(
            "{}/api/v2/search/tag/resource.service.name/values?{query_range}",
            crabka.base_url
        ),
        Some("tenant-b"),
    )
    .await?;
    assert_tag_values_do_not_contain(&tenant_b_values, "checkout");

    crabka.shutdown();
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/grafana image"]
#[allow(
    clippy::too_many_lines,
    reason = "integration test drives all Grafana datasource legs end-to-end"
)]
async fn grafana_accepts_tempo_datasource_pointing_at_crabka() -> TestResult {
    let client = reqwest::Client::new();
    let otlp_body = sample_otlp_body();
    let crabka = start_crabka_pair_reachable_from_container(&otlp_body).await?;

    let grafana = start_grafana().await?;
    let grafana_base = mapped_base_url(&grafana, 3000).await?;
    wait_for_http_ok(&client, &grafana_base, &["/api/health"]).await?;

    let payload = json!({
        "name": "Crabka Traces",
        "uid": GRAFANA_TEMPO_DATASOURCE_UID,
        "type": "tempo",
        "access": "proxy",
        "url": crabka.container_base_url,
        "isDefault": true,
        "jsonData": {
            "httpMethod": "GET",
            "httpHeaderName1": "X-Scope-OrgID"
        },
        "secureJsonData": {
            "httpHeaderValue1": TENANT
        }
    });
    let _created: JsonValue = client
        .post(format!("{grafana_base}/api/datasources"))
        .basic_auth("admin", Some("admin"))
        .json(&payload)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let fetched: JsonValue = client
        .get(format!(
            "{grafana_base}/api/datasources/uid/{GRAFANA_TEMPO_DATASOURCE_UID}"
        ))
        .basic_auth("admin", Some("admin"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    assert_eq!(
        fetched.get("type").and_then(JsonValue::as_str),
        Some("tempo")
    );
    assert_eq!(
        fetched.get("url").and_then(JsonValue::as_str),
        Some(crabka.container_base_url.as_str())
    );

    let echo = client
        .get(format!(
            "{grafana_base}/api/datasources/proxy/uid/{GRAFANA_TEMPO_DATASOURCE_UID}/api/echo"
        ))
        .basic_auth("admin", Some("admin"))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    assert_eq!(echo, "echo");

    let trace: JsonValue = client
        .get(format!(
            "{grafana_base}/api/datasources/proxy/uid/{GRAFANA_TEMPO_DATASOURCE_UID}/api/v2/traces/{TRACE_ID_HEX}?start=0&end=1"
        ))
        .basic_auth("admin", Some("admin"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(
        trace["trace"]["resourceSpans"]
            .as_array()
            .is_some_and(|spans| !spans.is_empty())
    );

    let search_query = "%7B%20resource.service.name%20%3D%20%22checkout%22%20%7D";
    let search: JsonValue = client
        .get(format!(
            "{grafana_base}/api/datasources/proxy/uid/{GRAFANA_TEMPO_DATASOURCE_UID}/api/search?q={search_query}&start=0&end=1"
        ))
        .basic_auth("admin", Some("admin"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(
        search["traces"]
            .as_array()
            .is_some_and(|traces| !traces.is_empty()),
        "Grafana-proxied TraceQL search response was empty: {search}"
    );

    // LEG 4 (error-span TraceQL): drive a status=error selector through the same
    // Grafana → Tempo-datasource → Crabka proxy and assert it returns the seeded
    // error trace. The seed body carries one `STATUS_CODE_ERROR` span (span id
    // 0404…), so `{ span:status = error }` must match its trace.
    let error_query = "%7B%20span%3Astatus%20%3D%20error%20%7D";
    let error_search: JsonValue = client
        .get(format!(
            "{grafana_base}/api/datasources/proxy/uid/{GRAFANA_TEMPO_DATASOURCE_UID}/api/search?q={error_query}&start=0&end=1"
        ))
        .basic_auth("admin", Some("admin"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(
        error_search["traces"]
            .as_array()
            .is_some_and(|traces| traces
                .iter()
                .any(|trace| { trace["traceID"].as_str() == Some(TRACE_ID_HEX) })),
        "Grafana-proxied error-status TraceQL search did not return the error trace: {error_search}"
    );
    assert_search_contains_span_id(&error_search, ERROR_SPAN_ID_HEX);

    crabka.shutdown();
    Ok(())
}

/// LEG 5 (Service Graph) — the loop-closing leg.
///
/// In Grafana, the Tempo datasource's **Service Graph** tab is NOT served by a
/// Tempo endpoint: Grafana renders it from `traces_service_graph_*` series in a
/// **Prometheus** datasource (spec §7.2/§8). The full production loop is
///   traces → metrics-generator → Prometheus `remote_write` → Prometheus → Grafana.
///
/// What this test FAITHFULLY covers (no fabrication):
///   1. The **Grafana-side wiring**: Grafana accepts and round-trips a
///      `prometheus`-type datasource (the Service-Graph backend), proving the
///      datasource half of the loop is configured exactly as the Tempo
///      datasource's `serviceMap.datasourceUid` would point at.
///   2. The **Crabka-side production**: the real in-process metrics-generator
///      `EdgeStore` (Slice 7) pairs the seed's client↔server span pair into the
///      exact `traces_service_graph_request_total` series — with the `client` /
///      `server` / `connection_type` edge labels — that Grafana's Service Graph
///      queries. This is the series the loop depends on.
///
/// What this test does NOT stand up (documented gap, deliberately not faked):
///   The metrics-generator → Prometheus `remote_write` ingestion path and a live
///   Prometheus container. There is no in-process Prometheus `/api/v1/query`
///   endpoint in `crabka-traces` (the metrics-generator emits via a
///   `RemoteWriteSink`, not a query API), so a live `POST /api/ds/query` against
///   the Prometheus datasource cannot return real data within this harness. We
///   therefore prove the two ends of the loop separately rather than asserting a
///   passing query against data the harness never produces.
#[tokio::test]
#[ignore = "requires Docker and the mirror.gcr.io/grafana/grafana image"]
async fn grafana_service_graph_prometheus_datasource_and_series() -> TestResult {
    let client = reqwest::Client::new();

    let grafana = start_grafana().await?;
    let grafana_base = mapped_base_url(&grafana, 3000).await?;
    wait_for_http_ok(&client, &grafana_base, &["/api/health"]).await?;

    // (1) Grafana-side wiring: provision the Prometheus datasource that backs the
    // Tempo datasource's Service Graph and assert Grafana round-trips it. In a
    // full deployment its URL points at Crabka's metrics querier / a Prometheus
    // scraping Crabka's metrics-generator `remote_write` output.
    let prom_uid = "crabka-service-graph";
    let payload = json!({
        "name": "Crabka Service Graph",
        "uid": prom_uid,
        "type": "prometheus",
        "access": "proxy",
        // Placeholder: the metrics-generator → Prometheus ingestion path is not
        // stood up in this harness (see the doc comment). The assertion below is
        // strictly the datasource round-trip, not a query against this URL.
        "url": format!("http://{DOCKER_HOST_ALIAS}:9090"),
        "isDefault": false,
        "jsonData": {
            "httpMethod": "GET",
            "httpHeaderName1": "X-Scope-OrgID"
        },
        "secureJsonData": {
            "httpHeaderValue1": TENANT
        }
    });
    let _created: JsonValue = client
        .post(format!("{grafana_base}/api/datasources"))
        .basic_auth("admin", Some("admin"))
        .json(&payload)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let fetched: JsonValue = client
        .get(format!("{grafana_base}/api/datasources/uid/{prom_uid}"))
        .basic_auth("admin", Some("admin"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        fetched.get("type").and_then(JsonValue::as_str),
        Some("prometheus")
    );

    // (2) Crabka-side production: the real metrics-generator EdgeStore pairs the
    // seed's client↔server pair into the Service-Graph series Grafana queries.
    let series = service_graph_series_for_seed_edge();
    let request_total = series
        .iter()
        .find(|s| s.name == "traces_service_graph_request_total")
        .expect("metrics-generator must emit traces_service_graph_request_total for a paired edge");
    let SeriesSample::Counter(count) = request_total.sample else {
        panic!("traces_service_graph_request_total must be a counter")
    };
    let has_edge_label = |key: &str, value: &str| {
        request_total
            .labels
            .iter()
            .any(|(k, v)| k == key && v == value)
    };
    assert!(
        (
            (count - 1.0).abs() < 1e-9,
            has_edge_label("client", "checkout-frontend"),
            has_edge_label("server", "cart-backend"),
        ) == (true, true, true),
        "expected one paired request for the seed edge with client=checkout-frontend and server=cart-backend labels, got count={count}, labels={:?}",
        request_total.labels
    );

    Ok(())
}

/// Drive the real Slice 7 `EdgeStore` over a client↔server span pair (the same
/// caller/callee shape the seed's `GET /checkout` → `SELECT cart` models) and
/// return the emitted service-graph series.
fn service_graph_series_for_seed_edge() -> Vec<Series> {
    let mut store = EdgeStore::new(&MetricsGenConfig::default());
    let client = metrics_span(
        "checkout-frontend",
        [0xA; 8],
        [0; 8],
        MetricsSpanKind::Client,
        MetricsStatusCode::Ok,
        10_000_000,
    );
    let server = metrics_span(
        "cart-backend",
        [0xB; 8],
        [0xA; 8],
        MetricsSpanKind::Server,
        MetricsStatusCode::Ok,
        8_000_000,
    );
    assert!(store.record_span(&client, 0) == RecordOutcome::Recorded);
    assert!(store.record_span(&server, 1) == RecordOutcome::Completed);
    store.drain(1_000)
}

fn metrics_span(
    service: &str,
    span_id: [u8; 8],
    parent: [u8; 8],
    kind: MetricsSpanKind,
    status: MetricsStatusCode,
    duration_ns: i64,
) -> MetricsSpanRecord {
    MetricsSpanRecord {
        tenant: TENANT.into(),
        trace_id: [0x11; 16],
        span_id,
        parent_span_id: parent,
        name: "op".into(),
        kind,
        start_ns: 0,
        duration_ns,
        status,
        status_message: String::new(),
        service_name: service.into(),
        attributes: vec![],
        size_bytes: 0,
    }
}

struct CrabkaPair {
    base_url: String,
    container_base_url: String,
    shutdown: tokio::sync::oneshot::Sender<()>,
}

impl CrabkaPair {
    fn shutdown(self) {
        let _ = self.shutdown.send(());
    }
}

async fn start_crabka_pair(otlp_body: &[u8]) -> TestResult<CrabkaPair> {
    start_crabka_pair_on(otlp_body, "127.0.0.1", "127.0.0.1").await
}

async fn start_crabka_pair_reachable_from_container(otlp_body: &[u8]) -> TestResult<CrabkaPair> {
    start_crabka_pair_on(otlp_body, "0.0.0.0", DOCKER_HOST_ALIAS).await
}

async fn start_crabka_pair_on(
    otlp_body: &[u8],
    bind_host: &str,
    container_host: &str,
) -> TestResult<CrabkaPair> {
    let sink = CapturingSink::default();
    let distributor_state = Arc::new(DistributorState::new(Arc::new(sink.clone())));
    let resp = distributor::router(distributor_state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header("content-type", "application/x-protobuf")
                .header("x-scope-orgid", TENANT)
                .body(Body::from(otlp_body.to_vec()))?,
        )
        .await?;
    assert!(resp.status() == StatusCode::OK);
    let _ = resp.into_body().collect().await?;

    let records = sink
        .records
        .lock()
        .map_err(|_| "capturing sink lock poisoned")?
        .clone();
    let store = Arc::new(TraceqlEngine::new(
        Arc::new(span_store_from_records(&records)),
        EngineOpts::default(),
    ));
    let app = crabka_traces::querier::http::router(store);
    let listener = tokio::net::TcpListener::bind(format!("{bind_host}:0")).await?;
    let addr = listener.local_addr()?;
    let port = addr.port();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .await;
    });

    Ok(CrabkaPair {
        base_url: format!("http://127.0.0.1:{port}"),
        container_base_url: format!("http://{container_host}:{port}"),
        shutdown: tx,
    })
}

fn span_store_from_records(records: &[SpanRecord]) -> InMemorySpanStore {
    let mut grouped: BTreeMap<(String, [u8; 16]), Vec<Span>> = BTreeMap::new();
    for record in records {
        grouped
            .entry((record.tenant.clone(), record.span.trace_id))
            .or_default()
            .push(record.span.clone());
    }

    let mut store = InMemorySpanStore::new();
    for ((tenant, _), spans) in grouped {
        let root = spans
            .iter()
            .find(|span| span.parent_span_id.is_none())
            .unwrap_or(&spans[0]);
        let root_service = resource_attr(root, "service.name")
            .unwrap_or("unknown")
            .to_string();
        let root_name = root.name.clone();
        store.push_trace(
            &tenant,
            &root_service,
            &root_name,
            spans.into_iter().map(input_span).collect(),
        );
    }
    store
}

fn input_span(span: Span) -> InputSpan {
    let mut attrs = span.resource_attrs;
    attrs.extend(span.span_attrs);
    InputSpan {
        trace_id: span.trace_id,
        span_id: span.span_id,
        parent_span_id: span.parent_span_id,
        name: span.name,
        kind: span.kind.as_i32(),
        start_unix_nano: span.start_ns,
        duration_nanos: span.duration_ns,
        status_code: span.status.as_i32(),
        status_message: span.status_message,
        instrumentation_name: span.instrumentation_scope,
        instrumentation_version: span.instrumentation_version,
        attrs: attrs
            .into_iter()
            .filter_map(|attr| Some((attr.key, traceql_attr(attr.value)?)))
            .collect(),
        events: Vec::new(),
        links: Vec::new(),
    }
}

fn traceql_attr(value: AttrValue) -> Option<TraceqlAttrValue> {
    match value {
        AttrValue::Str(value) => Some(TraceqlAttrValue::Str(value)),
        AttrValue::Int(value) => Some(TraceqlAttrValue::Int(value)),
        AttrValue::Double(value) => Some(TraceqlAttrValue::Float(value)),
        AttrValue::Bool(value) => Some(TraceqlAttrValue::Bool(value)),
        AttrValue::Bytes(_) => None,
    }
}

fn resource_attr<'a>(span: &'a Span, key: &str) -> Option<&'a str> {
    span.resource_attrs
        .iter()
        .find_map(|attr| match &attr.value {
            AttrValue::Str(value) if attr.key == key => Some(value.as_str()),
            _ => None,
        })
}

async fn start_tempo() -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    let tag = std::env::var("CRABKA_TEMPO_IMAGE_TAG").unwrap_or_else(|_| "latest".into());
    Ok(
        GenericImage::new("mirror.gcr.io/grafana/tempo".to_string(), tag)
            .with_exposed_port(3200.tcp())
            .with_exposed_port(4318.tcp())
            .with_wait_for(WaitFor::message_on_stderr("Tempo started"))
            .with_copy_to(
                CopyTargetOptions::new("/tmp/tempo.yaml").with_mode(0o644),
                CopyDataSource::Data(TEMPO_CONFIG.as_bytes().to_vec()),
            )
            .with_cmd(["-target=all", "-config.file=/tmp/tempo.yaml"])
            .with_user("root")
            .start()
            .await?,
    )
}

async fn start_grafana() -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    let tag = std::env::var("CRABKA_GRAFANA_IMAGE_TAG").unwrap_or_else(|_| "latest".into());
    Ok(
        GenericImage::new("mirror.gcr.io/grafana/grafana".to_string(), tag)
            .with_exposed_port(3000.tcp())
            .with_wait_for(WaitFor::seconds(5))
            .with_env_var("GF_SECURITY_ADMIN_PASSWORD", "admin")
            .with_host(DOCKER_HOST_ALIAS, Host::HostGateway)
            .start()
            .await?,
    )
}

async fn mapped_base_url(
    container: &testcontainers::ContainerAsync<GenericImage>,
    port: u16,
) -> TestResult<String> {
    let mapped = container.get_host_port_ipv4(port).await?;
    Ok(format!("http://127.0.0.1:{mapped}"))
}

async fn wait_for_http_ok(client: &reqwest::Client, base: &str, paths: &[&str]) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline {
        for path in paths {
            if client
                .get(format!("{base}{path}"))
                .send()
                .await
                .is_ok_and(|resp| resp.status().is_success())
            {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!("timed out waiting for {base}").into())
}

async fn post_otlp(
    client: &reqwest::Client,
    url: &str,
    tenant: Option<&str>,
    body: &[u8],
) -> TestResult {
    let mut req = client
        .post(url)
        .header("content-type", "application/x-protobuf")
        .body(body.to_vec());
    if let Some(tenant) = tenant {
        req = req.header("x-scope-orgid", tenant);
    }
    let status = req.send().await?.status();
    assert!(status.is_success(), "OTLP push failed: {status}");
    Ok(())
}

async fn get_trace_by_id(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    query_range: &str,
) -> TestResult<JsonValue> {
    get_json(
        client,
        &format!("{base}/api/v2/traces/{TRACE_ID_HEX}?{query_range}"),
        tenant,
    )
    .await
}

async fn get_json(
    client: &reqwest::Client,
    url: &str,
    tenant: Option<&str>,
) -> TestResult<JsonValue> {
    let mut req = client.get(url);
    if let Some(tenant) = tenant {
        req = req.header("x-scope-orgid", tenant);
    }
    let resp = req.send().await?;
    let status = resp.status();
    let body = resp.bytes().await?;
    assert!(
        status == ReqwestStatusCode::OK,
        "GET {url} failed: {status} {}",
        String::from_utf8_lossy(&body)
    );
    Ok(serde_json::from_slice(&body)?)
}

async fn get_status(
    client: &reqwest::Client,
    url: &str,
    tenant: Option<&str>,
) -> TestResult<ReqwestStatusCode> {
    let mut req = client.get(url);
    if let Some(tenant) = tenant {
        req = req.header("x-scope-orgid", tenant);
    }
    Ok(req.send().await?.status())
}

async fn get_json_until_non_empty_traces(
    client: &reqwest::Client,
    url: &str,
    tenant: Option<&str>,
) -> TestResult<JsonValue> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = JsonValue::Null;
    while Instant::now() < deadline {
        let json = get_json(client, url, tenant).await?;
        if json["traces"]
            .as_array()
            .is_some_and(|traces| !traces.is_empty())
        {
            return Ok(json);
        }
        last = json;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!("timed out waiting for non-empty traces from {url}: {last}").into())
}

async fn get_json_until_positive_metric_total(
    client: &reqwest::Client,
    url: &str,
    tenant: Option<&str>,
) -> TestResult<JsonValue> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = JsonValue::Null;
    while Instant::now() < deadline {
        let json = get_json(client, url, tenant).await?;
        if metric_points_total(&json) > 0.0 {
            return Ok(json);
        }
        last = json;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!("timed out waiting for positive metric total from {url}: {last}").into())
}

/// Real Tempo has ingestion latency: a freshly pushed trace is not immediately
/// queryable by id — `/api/v2/traces/{id}` returns 404 until the span is flushed
/// out of the ingester. Poll until the trace materialises, mirroring how the
/// search legs poll for non-empty results. (Crabka serves its in-process store
/// synchronously, so only the real-Tempo side needs this.)
async fn get_trace_by_id_until_found(
    client: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    query_range: &str,
) -> TestResult<JsonValue> {
    let url = format!("{base}/api/v2/traces/{TRACE_ID_HEX}?{query_range}");
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = String::new();
    while Instant::now() < deadline {
        let mut req = client.get(&url);
        if let Some(tenant) = tenant {
            req = req.header("x-scope-orgid", tenant);
        }
        let resp = req.send().await?;
        let status = resp.status();
        let body = resp.bytes().await?;
        if status == ReqwestStatusCode::OK {
            let json: JsonValue = serde_json::from_slice(&body)?;
            if json["trace"]["resourceSpans"]
                .as_array()
                .is_some_and(|spans| !spans.is_empty())
            {
                return Ok(json);
            }
            last = json.to_string();
        } else {
            last = format!("{status} {}", String::from_utf8_lossy(&body));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!("timed out waiting for trace by id from {url}: {last}").into())
}

fn assert_trace_shape_matches(tempo: &JsonValue, crabka: &JsonValue) {
    if !tempo["status"].is_null() {
        assert!(tempo["status"] == crabka["status"]);
    }
    assert!(
        tempo["trace"]["resourceSpans"]
            .as_array()
            .is_some_and(|spans| !spans.is_empty())
    );
    assert!(
        crabka["trace"]["resourceSpans"]
            .as_array()
            .is_some_and(|spans| !spans.is_empty())
    );
}

fn assert_search_shape_matches(tempo: &JsonValue, crabka: &JsonValue) {
    assert!(
        (
            tempo["traces"]
                .as_array()
                .is_some_and(|traces| !traces.is_empty()),
            crabka["traces"]
                .as_array()
                .is_some_and(|traces| !traces.is_empty()),
            crabka["traces"][0]["traceID"].as_str(),
        ) == (true, true, Some(TRACE_ID_HEX)),
        "search shape mismatch; Tempo search response: {tempo}; Crabka search response: {crabka}"
    );
}

fn assert_search_contains_span_id(search: &JsonValue, span_id: &str) {
    let found = search["traces"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|trace| trace["spanSets"].as_array().into_iter().flatten())
        .flat_map(|span_set| span_set["spans"].as_array().into_iter().flatten())
        .any(|span| span["spanID"].as_str() == Some(span_id));
    assert!(
        found,
        "search response did not contain span {span_id}: {search}"
    );
}

fn assert_search_empty(search: &JsonValue) {
    assert!(
        search["traces"].as_array().is_some_and(Vec::is_empty),
        "expected empty search response: {search}"
    );
}

fn assert_metric_totals_match(tempo: &JsonValue, crabka: &JsonValue) {
    let tempo_total = metric_points_total(tempo);
    let crabka_total = metric_points_total(crabka);
    assert!(
        tempo_total > 0.0,
        "Tempo metrics response had no positive points: {tempo}"
    );
    assert!(
        (tempo_total - crabka_total).abs() < f64::EPSILON,
        "metric totals differed; Tempo={tempo_total}, Crabka={crabka_total}, Tempo response={tempo}, Crabka response={crabka}"
    );
}

/// The set of `series[].labels[].key` strings across a TraceQL-metrics response
/// (the grouped-attribute names Grafana renders panels by).
fn metric_series_label_keys(resp: &JsonValue) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    if let Some(series) = resp.get("series").and_then(JsonValue::as_array) {
        for s in series {
            if let Some(labels) = s.get("labels").and_then(JsonValue::as_array) {
                for kv in labels {
                    if let Some(k) = kv.get("key").and_then(JsonValue::as_str) {
                        keys.insert(k.to_string());
                    }
                }
            }
        }
    }
    keys
}

/// The `series[].promLabels` strings (Grafana's legend form), for diagnostics.
fn metric_prom_labels_list(resp: &JsonValue) -> Vec<String> {
    resp.get("series")
        .and_then(JsonValue::as_array)
        .map(|series| {
            series
                .iter()
                .filter_map(|s| {
                    s.get("promLabels")
                        .and_then(JsonValue::as_str)
                        .map(String::from)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn metric_points_total(value: &JsonValue) -> f64 {
    let points_total: f64 = value["series"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|series| series["points"].as_array().into_iter().flatten())
        .filter_map(|point| point.as_array().and_then(|items| items.get(1)))
        .filter_map(JsonValue::as_f64)
        .sum();
    let samples_total: f64 = value["series"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|series| series["samples"].as_array().into_iter().flatten())
        .filter_map(|sample| sample["value"].as_f64())
        .sum();
    points_total + samples_total
}

fn assert_required_tag_names_match(tempo: &JsonValue, crabka: &JsonValue) {
    let tempo_tags = tag_names(tempo);
    let crabka_tags = tag_names(crabka);
    for required in ["service.name", "http.method", "db.system"] {
        assert!(tempo_tags.contains(required));
        assert!(crabka_tags.contains(required));
    }
}

fn assert_required_tag_values_match(tempo: &JsonValue, crabka: &JsonValue, required: &str) {
    let tempo_values = tag_values(tempo);
    let crabka_values = tag_values(crabka);
    assert!(tempo_values.contains(required));
    assert!(crabka_values.contains(required));
}

fn assert_tag_names_do_not_contain(value: &JsonValue, forbidden: &str) {
    let names = tag_names(value);
    assert!(
        !names.contains(forbidden),
        "tag names unexpectedly contained {forbidden}: {value}"
    );
}

fn assert_tag_values_do_not_contain(value: &JsonValue, forbidden: &str) {
    let values = tag_values(value);
    assert!(
        !values.contains(forbidden),
        "tag values unexpectedly contained {forbidden}: {value}"
    );
}

fn tag_names(value: &JsonValue) -> BTreeSet<String> {
    value["scopes"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|scope| {
            scope["tags"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(JsonValue::as_str)
                .map(str::to_string)
        })
        .collect()
}

fn tag_values(value: &JsonValue) -> BTreeSet<String> {
    value["tagValues"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|tag_value| tag_value["value"].as_str())
        .map(str::to_string)
        .collect()
}

fn sample_otlp_body() -> Vec<u8> {
    sample_otlp_body_at(1_000)
}

fn sample_otlp_body_at(start_ns: u64) -> Vec<u8> {
    TracesData {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "checkout")],
                ..Resource::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "crabka-differential".into(),
                    version: "1.0.0".into(),
                    ..InstrumentationScope::default()
                }),
                spans: vec![
                    OtlpSpan {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        name: "GET /checkout".into(),
                        start_time_unix_nano: start_ns,
                        end_time_unix_nano: start_ns + 500_000_000,
                        attributes: vec![string_kv("http.method", "GET")],
                        ..OtlpSpan::default()
                    },
                    OtlpSpan {
                        trace_id: vec![1; 16],
                        span_id: vec![3; 8],
                        parent_span_id: vec![2; 8],
                        name: "SELECT cart".into(),
                        start_time_unix_nano: start_ns + 100_000_000,
                        end_time_unix_nano: start_ns + 250_000_000,
                        attributes: vec![string_kv("db.system", "postgresql")],
                        ..OtlpSpan::default()
                    },
                    // An error-status span so the Grafana TraceQL `{ span:status = error }`
                    // leg (LEG 4) has a real error trace to find. Pushed identically to
                    // both Tempo and Crabka, so the differential corpus stays equal.
                    OtlpSpan {
                        trace_id: vec![1; 16],
                        span_id: vec![4; 8],
                        parent_span_id: vec![2; 8],
                        name: "charge card".into(),
                        start_time_unix_nano: start_ns + 260_000_000,
                        end_time_unix_nano: start_ns + 400_000_000,
                        attributes: vec![string_kv("http.method", "POST")],
                        status: Some(OtlpStatus {
                            code: OTLP_STATUS_CODE_ERROR,
                            message: "payment declined".into(),
                        }),
                        ..OtlpSpan::default()
                    },
                ],
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    }
    .encode_to_vec()
}

fn string_kv(key: &str, value: &str) -> OtlpKeyValue {
    OtlpKeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
        ..OtlpKeyValue::default()
    }
}
