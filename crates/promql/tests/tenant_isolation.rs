#![recursion_limit = "512"]

//! Slice 8 hardening: multi-tenant read isolation over a real TCP socket.
//!
//! Boots the Prometheus query HTTP API on an ephemeral `127.0.0.1:0` socket and
//! drives every read surface with a real HTTP client. Two tenants (`org-a` and
//! `org-b`) are seeded with disjoint series. Each assertion proves that a tenant
//! NEVER observes the other tenant's label values / series identifiers, and that
//! a request without `X-Scope-OrgID` is rejected as `bad_data`.

use std::{net::SocketAddr, sync::Arc};

use crabka_blockstore::Labels;
use crabka_promql::{EngineOpts, InMemoryMetricStore, PrometheusApiState, prometheus_router};
use serde_json::Value;

const ORG_A: &str = "org-a";
const ORG_B: &str = "org-b";

/// Sentinel label values unique to each tenant. If isolation leaks, these
/// strings show up in the foreign tenant's response body.
const A_JOB: &str = "alpha-job-a";
const A_INSTANCE: &str = "alpha-instance-a";
const A_ZONE: &str = "alpha-zone-a";
const B_JOB: &str = "bravo-job-b";
const B_INSTANCE: &str = "bravo-instance-b";
const B_ZONE: &str = "bravo-zone-b";
const A_TRACE: &str = "alpha-trace-a";
const B_TRACE: &str = "bravo-trace-b";

fn labels(pairs: &[(&str, &str)]) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in pairs {
        labels.insert(*name, *value);
    }
    labels
}

/// Seed disjoint series for the two tenants and boot the API on a real socket.
async fn boot_isolated_api() -> SocketAddr {
    let mut store = InMemoryMetricStore::new();

    // org-a: `up{job=alpha-job-a, instance=alpha-instance-a, zone=alpha-zone-a}`
    store.push_float(
        ORG_A,
        labels(&[
            ("__name__", "up"),
            ("job", A_JOB),
            ("instance", A_INSTANCE),
            ("zone", A_ZONE),
        ]),
        10_000,
        1.0,
    );
    store.push_float(
        ORG_A,
        labels(&[
            ("__name__", "up"),
            ("job", A_JOB),
            ("instance", A_INSTANCE),
            ("zone", A_ZONE),
        ]),
        20_000,
        2.0,
    );
    store.push_exemplar(
        ORG_A,
        labels(&[("__name__", "up"), ("job", A_JOB)]),
        labels(&[("trace_id", A_TRACE)]),
        10_500,
        1.0,
    );

    // org-b: `up{job=bravo-job-b, instance=bravo-instance-b, zone=bravo-zone-b}`
    store.push_float(
        ORG_B,
        labels(&[
            ("__name__", "up"),
            ("job", B_JOB),
            ("instance", B_INSTANCE),
            ("zone", B_ZONE),
        ]),
        10_000,
        3.0,
    );
    store.push_float(
        ORG_B,
        labels(&[
            ("__name__", "up"),
            ("job", B_JOB),
            ("instance", B_INSTANCE),
            ("zone", B_ZONE),
        ]),
        20_000,
        4.0,
    );
    store.push_exemplar(
        ORG_B,
        labels(&[("__name__", "up"), ("job", B_JOB)]),
        labels(&[("trace_id", B_TRACE)]),
        10_500,
        3.0,
    );

    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral socket");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, prometheus_router(state))
            .await
            .expect("serve prometheus api");
    });
    addr
}

/// GET `path` with the given tenant header; returns (status, raw body text).
async fn get_with_tenant(
    client: &reqwest::Client,
    addr: SocketAddr,
    path: &str,
    tenant: &str,
) -> (reqwest::StatusCode, String) {
    let response = client
        .get(format!("http://{addr}{path}"))
        .header("X-Scope-OrgID", tenant)
        .send()
        .await
        .expect("send request");
    let status = response.status();
    let text = response.text().await.expect("response text");
    (status, text)
}

/// Foreign-tenant sentinel substrings that must be ABSENT from `tenant`'s body.
fn foreign_sentinels(tenant: &str) -> &'static [&'static str] {
    if tenant == ORG_A {
        &[B_JOB, B_INSTANCE, B_ZONE, B_TRACE]
    } else {
        &[A_JOB, A_INSTANCE, A_ZONE, A_TRACE]
    }
}

/// Own-tenant sentinel substrings that should be PRESENT in `tenant`'s body.
fn own_sentinels(tenant: &str) -> &'static [&'static str] {
    if tenant == ORG_A {
        &[A_JOB, A_INSTANCE, A_ZONE]
    } else {
        &[B_JOB, B_INSTANCE, B_ZONE]
    }
}

/// Assert a successful read for `tenant` contains its own data and none of the
/// foreign tenant's sentinel values.
fn assert_isolated(tenant: &str, surface: &str, status: reqwest::StatusCode, body: &str) {
    assert!(
        status.is_success(),
        "{surface}: expected success for {tenant}, got {status}: {body}"
    );
    let parsed: Value = serde_json::from_str(body).expect("json body");
    assert_eq!(
        parsed["status"], "success",
        "{surface}: status field for {tenant}: {body}"
    );
    for foreign in foreign_sentinels(tenant) {
        assert!(
            !body.contains(foreign),
            "{surface}: {tenant} response leaked foreign value `{foreign}`: {body}"
        );
    }
}

/// Like [`assert_isolated`] but for Grafana Mimir cardinality surfaces, which
/// return a bare cardinality object without the Prometheus `{status,data}`
/// envelope. Verifies HTTP success and that no foreign sentinel leaks through.
fn assert_cardinality_isolated(
    tenant: &str,
    surface: &str,
    status: reqwest::StatusCode,
    body: &str,
) {
    assert!(
        status.is_success(),
        "{surface}: expected success for {tenant}, got {status}: {body}"
    );
    // Must be valid JSON, but Mimir cardinality responses have no status field.
    let _: Value = serde_json::from_str(body).expect("json body");
    for foreign in foreign_sentinels(tenant) {
        assert!(
            !body.contains(foreign),
            "{surface}: {tenant} response leaked foreign value `{foreign}`: {body}"
        );
    }
}

#[tokio::test]
async fn instant_query_is_tenant_isolated() {
    let addr = boot_isolated_api().await;
    let client = reqwest::Client::new();
    let path = "/api/v1/query?query=up&time=20";

    let (status, body) = get_with_tenant(&client, addr, path, ORG_A).await;
    assert_isolated(ORG_A, "query", status, &body);
    assert!(
        own_sentinels(ORG_A).iter().any(|own| body.contains(own)),
        "query: org-a should see its own data: {body}"
    );

    let (status, body) = get_with_tenant(&client, addr, path, ORG_B).await;
    assert_isolated(ORG_B, "query", status, &body);
    assert!(
        own_sentinels(ORG_B).iter().any(|own| body.contains(own)),
        "query: org-b should see its own data: {body}"
    );
}

#[tokio::test]
async fn range_query_is_tenant_isolated() {
    let addr = boot_isolated_api().await;
    let client = reqwest::Client::new();
    let path = "/api/v1/query_range?query=up&start=10&end=20&step=10";

    let (status, body) = get_with_tenant(&client, addr, path, ORG_A).await;
    assert_isolated(ORG_A, "query_range", status, &body);

    let (status, body) = get_with_tenant(&client, addr, path, ORG_B).await;
    assert_isolated(ORG_B, "query_range", status, &body);
}

#[tokio::test]
async fn series_is_tenant_isolated() {
    let addr = boot_isolated_api().await;
    let client = reqwest::Client::new();
    let path = "/api/v1/series?match%5B%5D=up&start=10&end=20";

    let (status, body) = get_with_tenant(&client, addr, path, ORG_A).await;
    assert_isolated(ORG_A, "series", status, &body);
    assert!(
        body.contains(A_JOB),
        "series: org-a should see its own series: {body}"
    );

    let (status, body) = get_with_tenant(&client, addr, path, ORG_B).await;
    assert_isolated(ORG_B, "series", status, &body);
    assert!(
        body.contains(B_JOB),
        "series: org-b should see its own series: {body}"
    );
}

#[tokio::test]
async fn labels_is_tenant_isolated() {
    let addr = boot_isolated_api().await;
    let client = reqwest::Client::new();
    let path = "/api/v1/labels?start=10&end=20";

    let (status, body) = get_with_tenant(&client, addr, path, ORG_A).await;
    assert_isolated(ORG_A, "labels", status, &body);

    let (status, body) = get_with_tenant(&client, addr, path, ORG_B).await;
    assert_isolated(ORG_B, "labels", status, &body);
}

#[tokio::test]
async fn label_values_is_tenant_isolated() {
    let addr = boot_isolated_api().await;
    let client = reqwest::Client::new();

    // `job` values must be partitioned by tenant.
    let path = "/api/v1/label/job/values?start=10&end=20";
    let (status, body) = get_with_tenant(&client, addr, path, ORG_A).await;
    assert_isolated(ORG_A, "label/job/values", status, &body);
    assert!(
        body.contains(A_JOB),
        "label_values: org-a should see its own job value: {body}"
    );
    let (status, body) = get_with_tenant(&client, addr, path, ORG_B).await;
    assert_isolated(ORG_B, "label/job/values", status, &body);
    assert!(
        body.contains(B_JOB),
        "label_values: org-b should see its own job value: {body}"
    );

    // `instance` and `zone` values must also be partitioned.
    for label in ["instance", "zone"] {
        let path = format!("/api/v1/label/{label}/values?start=10&end=20");
        let (status, body) = get_with_tenant(&client, addr, &path, ORG_A).await;
        assert_isolated(ORG_A, &format!("label/{label}/values"), status, &body);
        let (status, body) = get_with_tenant(&client, addr, &path, ORG_B).await;
        assert_isolated(ORG_B, &format!("label/{label}/values"), status, &body);
    }
}

#[tokio::test]
async fn query_exemplars_is_tenant_isolated() {
    let addr = boot_isolated_api().await;
    let client = reqwest::Client::new();
    let path = "/api/v1/query_exemplars?query=up&start=10&end=11";

    let (status, body) = get_with_tenant(&client, addr, path, ORG_A).await;
    assert_isolated(ORG_A, "query_exemplars", status, &body);
    assert!(
        body.contains(A_TRACE),
        "query_exemplars: org-a should see its own exemplar: {body}"
    );

    let (status, body) = get_with_tenant(&client, addr, path, ORG_B).await;
    assert_isolated(ORG_B, "query_exemplars", status, &body);
    assert!(
        body.contains(B_TRACE),
        "query_exemplars: org-b should see its own exemplar: {body}"
    );
}

#[tokio::test]
async fn cardinality_endpoints_are_tenant_isolated() {
    let addr = boot_isolated_api().await;
    let client = reqwest::Client::new();

    // Loosely-parsed cardinality surfaces: assert only presence/absence of the
    // foreign tenant's label VALUES, not the exact JSON schema. These endpoints
    // follow Grafana Mimir and return a bare cardinality object (no
    // {status,data} envelope), so isolation is checked without the status field.
    for path in [
        "/api/v1/cardinality/label_names",
        "/api/v1/cardinality/label_values",
        "/prometheus/api/v1/cardinality/active_series",
    ] {
        let (status, body) = get_with_tenant(&client, addr, path, ORG_A).await;
        assert_cardinality_isolated(ORG_A, path, status, &body);
        let (status, body) = get_with_tenant(&client, addr, path, ORG_B).await;
        assert_cardinality_isolated(ORG_B, path, status, &body);
    }

    // The label_values cardinality surface should additionally reflect each
    // tenant's own job value when it lists values.
    let path = "/api/v1/cardinality/label_values?label_names%5B%5D=job";
    let (_, body) = get_with_tenant(&client, addr, path, ORG_A).await;
    assert!(
        !body.contains(B_JOB),
        "cardinality/label_values: org-a leaked org-b job: {body}"
    );
    let (_, body) = get_with_tenant(&client, addr, path, ORG_B).await;
    assert!(
        !body.contains(A_JOB),
        "cardinality/label_values: org-b leaked org-a job: {body}"
    );
}

#[tokio::test]
async fn read_surfaces_reject_missing_tenant_header() {
    let addr = boot_isolated_api().await;
    let client = reqwest::Client::new();

    for path in [
        "/api/v1/query?query=up&time=20",
        "/api/v1/query_range?query=up&start=10&end=20&step=10",
        "/api/v1/series?match%5B%5D=up&start=10&end=20",
        "/api/v1/labels?start=10&end=20",
        "/api/v1/label/job/values?start=10&end=20",
        "/api/v1/query_exemplars?query=up&start=10&end=11",
        "/api/v1/cardinality/label_names",
        "/api/v1/cardinality/label_values",
        "/prometheus/api/v1/cardinality/active_series",
    ] {
        let response = client
            .get(format!("http://{addr}{path}"))
            .send()
            .await
            .expect("send request");
        let status = response.status();
        let body = response.text().await.expect("response text");
        assert_eq!(
            status,
            reqwest::StatusCode::BAD_REQUEST,
            "{path}: missing tenant should be rejected, got {status}: {body}"
        );
        let parsed: Value = serde_json::from_str(&body).expect("json body");
        assert_eq!(parsed["status"], "error", "{path}: error status: {body}");
        assert_eq!(
            parsed["errorType"], "bad_data",
            "{path}: bad_data errorType: {body}"
        );
    }
}
