//! Docker-backed differential probes against real Grafana Tempo.
//!
//! These tests are ignored by default because they pull and run upstream Docker
//! images. Run explicitly with:
//!
//! `cargo test -p crabka-traces --test tempo_differential -- --ignored --nocapture`

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use assert2::assert;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use crabka_traceql::{
    AttrValue as TraceqlAttrValue, EngineOpts, InMemorySpanStore, InputSpan, TraceqlEngine,
};
use crabka_traces::distributor::{self, DistributorState, WalSink};
use crabka_traces::{AttrValue, Span, SpanRecord, TracesError};
use http_body_util::BodyExt as _;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, InstrumentationScope, KeyValue as OtlpKeyValue, any_value::Value,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span as OtlpSpan, TracesData,
};
use prost::Message as _;
use reqwest::StatusCode as ReqwestStatusCode;
use serde_json::{Value as JsonValue, json};
use testcontainers::core::{Host, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tower::ServiceExt as _;

const TENANT: &str = "tenant-a";
const TRACE_ID_HEX: &str = "01010101010101010101010101010101";
const DOCKER_HOST_ALIAS: &str = "host.testcontainers.internal";
const GRAFANA_TEMPO_DATASOURCE_UID: &str = "crabka-traces";

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

#[tokio::test]
#[ignore = "requires Docker and the grafana/tempo image"]
async fn real_tempo_and_crabka_match_basic_by_id_and_search() -> TestResult {
    let client = reqwest::Client::new();
    let tempo = start_tempo().await?;
    let tempo_query = mapped_base_url(&tempo, 3200).await?;
    let tempo_otlp = mapped_base_url(&tempo, 4318).await?;
    wait_for_http_ok(&client, &tempo_query, &["/ready", "/status"]).await?;

    let otlp_body = sample_otlp_body();
    let crabka = start_crabka_pair(&otlp_body).await?;

    post_otlp(
        &client,
        &format!("{tempo_otlp}/v1/traces"),
        None,
        &otlp_body,
    )
    .await?;

    let tempo_trace = get_trace_by_id(&client, &tempo_query, None).await?;
    let crabka_trace = get_trace_by_id(&client, &crabka.base_url, Some(TENANT)).await?;
    assert_trace_shape_matches(&tempo_trace, &crabka_trace);

    let query = "%7B%20.service.name%20%3D%20%22checkout%22%20%7D";
    let tempo_search = get_json(
        &client,
        &format!("{tempo_query}/api/search?q={query}&start=0&end=1"),
        None,
    )
    .await?;
    let crabka_search = get_json(
        &client,
        &format!("{}/api/search?q={query}&start=0&end=1", crabka.base_url),
        Some(TENANT),
    )
    .await?;
    assert_search_shape_matches(&tempo_search, &crabka_search);

    crabka.shutdown();
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker and the grafana/grafana image"]
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

    crabka.shutdown();
    Ok(())
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
    Ok(GenericImage::new("grafana/tempo".to_string(), tag)
        .with_exposed_port(3200.tcp())
        .with_exposed_port(4318.tcp())
        .with_wait_for(WaitFor::seconds(5))
        .start()
        .await?)
}

async fn start_grafana() -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    let tag = std::env::var("CRABKA_GRAFANA_IMAGE_TAG").unwrap_or_else(|_| "latest".into());
    Ok(GenericImage::new("grafana/grafana".to_string(), tag)
        .with_exposed_port(3000.tcp())
        .with_wait_for(WaitFor::seconds(5))
        .with_env_var("GF_SECURITY_ADMIN_PASSWORD", "admin")
        .with_host(DOCKER_HOST_ALIAS, Host::HostGateway)
        .start()
        .await?)
}

async fn mapped_base_url(
    container: &testcontainers::ContainerAsync<GenericImage>,
    port: u16,
) -> TestResult<String> {
    let mapped = container.get_host_port_ipv4(port).await?;
    Ok(format!("http://127.0.0.1:{mapped}"))
}

async fn wait_for_http_ok(client: &reqwest::Client, base: &str, paths: &[&str]) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(30);
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
) -> TestResult<JsonValue> {
    get_json(
        client,
        &format!("{base}/api/v2/traces/{TRACE_ID_HEX}?start=0&end=1"),
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

fn assert_trace_shape_matches(tempo: &JsonValue, crabka: &JsonValue) {
    assert!(tempo["status"] == crabka["status"]);
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
        tempo["traces"]
            .as_array()
            .is_some_and(|traces| !traces.is_empty())
    );
    assert!(
        crabka["traces"]
            .as_array()
            .is_some_and(|traces| !traces.is_empty())
    );
    assert!(crabka["traces"][0]["traceID"] == TRACE_ID_HEX);
}

fn sample_otlp_body() -> Vec<u8> {
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
                        start_time_unix_nano: 1_000,
                        end_time_unix_nano: 1_500,
                        attributes: vec![string_kv("http.method", "GET")],
                        ..OtlpSpan::default()
                    },
                    OtlpSpan {
                        trace_id: vec![1; 16],
                        span_id: vec![3; 8],
                        parent_span_id: vec![2; 8],
                        name: "SELECT cart".into(),
                        start_time_unix_nano: 1_100,
                        end_time_unix_nano: 1_250,
                        attributes: vec![string_kv("db.system", "postgresql")],
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
