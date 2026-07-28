//! Headline multi-tenant isolation across every Tempo HTTP read surface.
//!
//! This is an in-process test (no Docker, never `#[ignore]`). It boots the real
//! Slice 5 querier router over a real TCP socket and drives it with `reqwest`,
//! so every request flows through the genuine `X-Scope-OrgID` tenant extractor.
//!
//! The sharpest probe here is a *colliding `trace_id`*: both tenants ingest a
//! trace whose `trace_id` bytes are byte-for-byte identical. The assertions can
//! only pass if isolation happens at the tenant key — before the by-id /
//! row-group lookup — rather than after it. A leak would surface as cross-tenant
//! span bleed even though neither tenant ever sent the other's spans.
//!
//! Ingest goes through the real distributor OTLP-protobuf door (so the tenant is
//! resolved from `X-Scope-OrgID` exactly as in production); the captured
//! `SpanRecord`s are loaded into the tenant-keyed `InMemorySpanStore` that backs
//! the querier, matching how the real store namespaces data by tenant.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use assert2::check;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use crabka_traceql::{
    AttrValue as TraceqlAttrValue, EngineOpts, InMemorySpanStore, InputSpan, TraceqlEngine,
};
use crabka_traces::{
    AttrValue, Limits, Span, SpanRecord, TracesError,
    distributor::{self, DistributorState, WalSink},
    limits::OverridesProvider,
    querier::http::HttpConfig,
};
use crabka_units::{Time, convert::TimeExt as _};
use http_body_util::BodyExt as _;
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, InstrumentationScope, KeyValue as OtlpKeyValue, any_value::Value},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan, TracesData},
};
use prost::Message as _;
use reqwest::StatusCode as ReqwestStatusCode;
use serde_json::Value as JsonValue;
use tower::ServiceExt as _;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The colliding trace identity both tenants ingest under the *same* bytes.
const COLLIDING_TRACE_ID: [u8; 16] = [0xAB; 16];
const COLLIDING_TRACE_ID_HEX: &str = "abababababababababababababababab";

/// Attribute key present only in tenant-a's spans.
const TENANT_A_ONLY_KEY: &str = "tenant_only";
const TENANT_A_ONLY_VALUE: &str = "A";

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

/// A running querier bound to a real ephemeral socket.
struct TestServer {
    base_url: String,
    shutdown: tokio::sync::oneshot::Sender<()>,
}

impl TestServer {
    fn shutdown(self) {
        let _ = self.shutdown.send(());
    }
}

/// Push one tenant's OTLP payload through the real distributor door and return
/// the captured `SpanRecord`s for that tenant.
async fn ingest(tenant: &str, otlp_body: &[u8]) -> TestResult<Vec<SpanRecord>> {
    let sink = CapturingSink::default();
    let state = Arc::new(DistributorState::new(Arc::new(sink.clone())));
    let resp = distributor::router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header("content-type", "application/x-protobuf")
                .header("x-scope-orgid", tenant)
                .body(Body::from(otlp_body.to_vec()))?,
        )
        .await?;
    assert2::assert!(resp.status() == StatusCode::OK);
    let _ = resp.into_body().collect().await?;
    let records = sink
        .records
        .lock()
        .map_err(|_| "capturing sink lock poisoned")?
        .clone();
    Ok(records)
}

/// Boot the querier over a real socket atop a tenant-keyed store seeded from
/// `records`. The optional overrides drive per-tenant limit enforcement.
async fn start_querier(
    records: Vec<SpanRecord>,
    overrides: Option<OverridesProvider>,
) -> TestResult<TestServer> {
    let store = Arc::new(TraceqlEngine::new(
        Arc::new(span_store_from_records(&records)),
        EngineOpts::default(),
    ));
    let cfg = HttpConfig {
        limits: Limits::default(),
        overrides,
        ..HttpConfig::default()
    };
    let app = crabka_traces::querier::http::router_with_config(store, cfg);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .await;
    });
    Ok(TestServer {
        base_url: format!("http://127.0.0.1:{port}"),
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
        duration: Time::from_nanos(span.duration_ns),
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

/// Build a one-span OTLP trace under the colliding `trace_id`.
///
/// `extra_attrs` lets the caller attach a tenant-unique span attribute so the
/// two tenants' traces are distinguishable in content while sharing identity.
fn colliding_trace(service: &str, root_name: &str, extra_attrs: &[(&str, &str)]) -> Vec<u8> {
    let mut span_attrs = vec![string_kv("http.method", "POST")];
    for (key, value) in extra_attrs {
        span_attrs.push(string_kv(key, value));
    }
    TracesData {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", service)],
                ..Resource::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "crabka-isolation".into(),
                    version: "1.0.0".into(),
                    ..InstrumentationScope::default()
                }),
                spans: vec![OtlpSpan {
                    trace_id: COLLIDING_TRACE_ID.to_vec(),
                    span_id: vec![2; 8],
                    name: root_name.into(),
                    start_time_unix_nano: 1_000,
                    end_time_unix_nano: 1_000 + 500_000_000,
                    attributes: span_attrs,
                    ..OtlpSpan::default()
                }],
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    }
    .encode_to_vec()
}

/// An OTLP trace with N spans sharing one fresh `trace_id`, used to drive ingest
/// volume for the per-tenant quota probe.
fn trace_with_n_spans(trace_seed: u8, n: usize) -> Vec<u8> {
    let spans = (0..n)
        .map(|i| OtlpSpan {
            trace_id: [trace_seed; 16].to_vec(),
            span_id: [i.to_le_bytes()[0].wrapping_add(1); 8].to_vec(),
            name: format!("span-{i}"),
            start_time_unix_nano: 1_000,
            end_time_unix_nano: 1_500,
            ..OtlpSpan::default()
        })
        .collect();
    TracesData {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "loadgen")],
                ..Resource::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans,
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

async fn get_json(
    client: &reqwest::Client,
    url: &str,
    tenant: &str,
) -> TestResult<(ReqwestStatusCode, JsonValue)> {
    let resp = client
        .get(url)
        .header("x-scope-orgid", tenant)
        .send()
        .await?;
    let status = resp.status();
    let body = resp.bytes().await?;
    let json = if body.is_empty() {
        JsonValue::Null
    } else {
        serde_json::from_slice(&body).unwrap_or(JsonValue::Null)
    };
    Ok((status, json))
}

/// Collect the span names contained in a `/api/v2/traces/{id}` response.
fn trace_span_names(trace: &JsonValue) -> Vec<String> {
    trace["trace"]["resourceSpans"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|rs| rs["scopeSpans"].as_array().into_iter().flatten())
        .flat_map(|ss| ss["spans"].as_array().into_iter().flatten())
        .filter_map(|span| span["name"].as_str().map(str::to_string))
        .collect()
}

/// Collect the `rootTraceName`s from a `/api/search` response.
fn root_trace_names(search: &JsonValue) -> Vec<String> {
    search["traces"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|trace| trace["rootTraceName"].as_str().map(str::to_string))
        .collect()
}

/// Collect every tag name across all scopes in a `/api/v2/search/tags` response.
fn tag_names(tags: &JsonValue) -> Vec<String> {
    tags["scopes"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|scope| scope["tags"].as_array().into_iter().flatten())
        .filter_map(|tag| tag.as_str().map(str::to_string))
        .collect()
}

/// Collect the tag values from a `/api/search/tag/{tag}/values` (v1) response.
fn tag_values(values: &JsonValue) -> Vec<String> {
    values["tagValues"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|value| value.as_str().map(str::to_string))
        .collect()
}

#[tokio::test]
async fn tenants_are_fully_isolated_across_all_read_surfaces() -> TestResult {
    // SAME trace_id bytes in both tenants, same service, different root name,
    // plus an attribute unique to tenant-a.
    let a_records = ingest(
        "tenant-a",
        &colliding_trace(
            "checkout",
            "POST /a",
            &[(TENANT_A_ONLY_KEY, TENANT_A_ONLY_VALUE)],
        ),
    )
    .await?;
    let b_records = ingest(
        "tenant-b",
        &colliding_trace("checkout", "POST /b", &[("plain", "x")]),
    )
    .await?;
    let mut records = a_records;
    records.extend(b_records);
    let server = start_querier(records, None).await?;
    let client = reqwest::Client::new();

    // start/end are epoch seconds; the handler multiplies by 1e9, so keep `end`
    // small enough that `end * 1e9` stays within i64. The seeded spans sit at
    // ns=1_000 (≈ epoch 0), so a wide-but-finite window covers them.
    let full_range = "start=0&end=2000000000";

    // 1) trace_by_id: tenant-a sees "POST /a", tenant-b sees "POST /b" — neither
    //    sees the other's span, even though the trace_id bytes are identical.
    let (sa, ta) = get_json(
        &client,
        &format!(
            "{}/api/v2/traces/{COLLIDING_TRACE_ID_HEX}?{full_range}",
            server.base_url
        ),
        "tenant-a",
    )
    .await?;
    let (sb, tb) = get_json(
        &client,
        &format!(
            "{}/api/v2/traces/{COLLIDING_TRACE_ID_HEX}?{full_range}",
            server.base_url
        ),
        "tenant-b",
    )
    .await?;
    check!(sa == ReqwestStatusCode::OK);
    check!(sb == ReqwestStatusCode::OK);
    let a_names = trace_span_names(&ta);
    let b_names = trace_span_names(&tb);
    check!(a_names == vec!["POST /a".to_string()]);
    check!(b_names == vec!["POST /b".to_string()]);
    check!(!a_names.contains(&"POST /b".to_string()));
    check!(!b_names.contains(&"POST /a".to_string()));

    // 2) search: each tenant's result set contains only its own root name.
    let select_all = "%7B%20.http.method%20%3D%20%22POST%22%20%7D";
    let (_, search_a) = get_json(
        &client,
        &format!("{}/api/search?q={select_all}&{full_range}", server.base_url),
        "tenant-a",
    )
    .await?;
    let (_, search_b) = get_json(
        &client,
        &format!("{}/api/search?q={select_all}&{full_range}", server.base_url),
        "tenant-b",
    )
    .await?;
    check!(root_trace_names(&search_a) == vec!["POST /a".to_string()]);
    check!(root_trace_names(&search_b) == vec!["POST /b".to_string()]);

    // 3) /api/v2/search/tags: tenant-a has the `tenant_only` tag, tenant-b does not.
    let (_, tags_a) = get_json(
        &client,
        &format!(
            "{}/api/v2/search/tags?scope=span&{full_range}",
            server.base_url
        ),
        "tenant-a",
    )
    .await?;
    let (_, tags_b) = get_json(
        &client,
        &format!(
            "{}/api/v2/search/tags?scope=span&{full_range}",
            server.base_url
        ),
        "tenant-b",
    )
    .await?;
    check!(tag_names(&tags_a).contains(&TENANT_A_ONLY_KEY.to_string()));
    check!(!tag_names(&tags_b).contains(&TENANT_A_ONLY_KEY.to_string()));

    // 4) tag/{tag}/values: tenant-a sees `tenant_only=A`; tenant-b sees nothing.
    let (_, values_a) = get_json(
        &client,
        &format!(
            "{}/api/search/tag/{TENANT_A_ONLY_KEY}/values?{full_range}",
            server.base_url
        ),
        "tenant-a",
    )
    .await?;
    let (_, values_b) = get_json(
        &client,
        &format!(
            "{}/api/search/tag/{TENANT_A_ONLY_KEY}/values?{full_range}",
            server.base_url
        ),
        "tenant-b",
    )
    .await?;
    check!(tag_values(&values_a).contains(&TENANT_A_ONLY_VALUE.to_string()));
    check!(tag_values(&values_b).is_empty());

    // 5) TraceQL select on the tenant-a-only attribute returns nothing for tenant-b.
    let select_a_only = "%7B%20.tenant_only%20%3D%20%22A%22%20%7D";
    let (_, q_a) = get_json(
        &client,
        &format!(
            "{}/api/search?q={select_a_only}&{full_range}",
            server.base_url
        ),
        "tenant-a",
    )
    .await?;
    let (_, q_b) = get_json(
        &client,
        &format!(
            "{}/api/search?q={select_a_only}&{full_range}",
            server.base_url
        ),
        "tenant-b",
    )
    .await?;
    check!(root_trace_names(&q_a) == vec!["POST /a".to_string()]);
    assert2::assert!(root_trace_names(&q_b).is_empty());

    server.shutdown();
    Ok(())
}

#[tokio::test]
async fn per_tenant_quota_throttles_only_the_capped_tenant() -> TestResult {
    // tenant-a has a tiny ingest-rate cap; tenant-b is unbounded. Pushing the
    // same burst to both proves quota buckets are keyed per tenant: the capped
    // tenant 429s while the other tenant stays 200 at the same instant.
    let overrides = OverridesProvider::from_yaml(
        r"
overrides:
  tenant-a:
    ingestion_rate_spans_per_sec: 1
    ingestion_burst_spans: 1
",
    )?;

    // Ingest enforcement lives in the distributor door. Build a distributor with
    // the same overrides and drive both tenants through it.
    let sink = CapturingSink::default();
    let mut state = DistributorState::new(Arc::new(sink.clone()));
    state.overrides = Some(overrides);
    let router = distributor::router(Arc::new(state));
    let client = reqwest::Client::new();

    // Bind a real socket for the distributor so X-Scope-OrgID flows through HTTP.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .await;
    });
    let base_url = format!("http://127.0.0.1:{port}");

    // tenant-a: first single-span push consumes the burst; second is over-rate.
    let a_first = client
        .post(format!("{base_url}/v1/traces"))
        .header("content-type", "application/x-protobuf")
        .header("x-scope-orgid", "tenant-a")
        .body(trace_with_n_spans(1, 1))
        .send()
        .await?
        .status();
    let a_second = client
        .post(format!("{base_url}/v1/traces"))
        .header("content-type", "application/x-protobuf")
        .header("x-scope-orgid", "tenant-a")
        .body(trace_with_n_spans(2, 1))
        .send()
        .await?
        .status();
    check!(a_first == ReqwestStatusCode::OK);
    check!(a_second == ReqwestStatusCode::TOO_MANY_REQUESTS);

    // tenant-b is unaffected at the same instant: a multi-span burst succeeds.
    let b_status = client
        .post(format!("{base_url}/v1/traces"))
        .header("content-type", "application/x-protobuf")
        .header("x-scope-orgid", "tenant-b")
        .body(trace_with_n_spans(3, 50))
        .send()
        .await?
        .status();
    check!(b_status == ReqwestStatusCode::OK);

    let _ = tx.send(());
    Ok(())
}
