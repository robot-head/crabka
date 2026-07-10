//! Docker-backed Grafana end-to-end coverage: a real Grafana with a provisioned
//! Prometheus-type datasource pointed at the in-process Crabka query API, driven
//! across (a) the full `PromQL` query-shape matrix via Grafana's dashboard query
//! path (`POST /api/ds/query`, instant + range) and (b) the metadata / label /
//! series / exemplar / build-info surfaces via Grafana's datasource resource
//! proxy (`/api/datasources/uid/<uid>/resources/...`). Every assertion exercises
//! the full Grafana -> Prometheus-datasource -> Crabka path.
//!
//! Ignored by default because it pulls and runs `mirror.gcr.io/grafana/grafana` under Docker.
//! Run with:
//!
//! `cargo test -p crabka-metrics-service --test grafana_integration -- --ignored --nocapture`
//!
//! Host reachability (platform-specific knob): Grafana runs in a container and
//! must reach the Crabka server running on the host. We bind Crabka to
//! `0.0.0.0:0`, hand the mapped port to Grafana via a provisioned datasource URL
//! of `http://host.docker.internal:<port>`, and add `host.docker.internal ->
//! host-gateway` to the container (`with_host(.., Host::HostGateway)`), which is
//! how Docker exposes the host from inside a container on Linux/macOS/Windows.

use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use crabka_metrics::{
    WalRecord,
    distributor::{DistributorState, ProduceError, WalSink},
    wire::pb,
};
use crabka_promql::WalHead;
use diff_corpus::seed_dataset;
use prost::Message;
use reqwest::StatusCode;
use serde_json::{Value, json};
use testcontainers::{
    GenericImage, ImageExt,
    core::{Host, IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::sync::oneshot;

// The shared corpus/differ module is path-included exactly as `diff_prometheus.rs`
// does it, so all metrics differential suites share one corpus definition. This
// integration only needs `seed_dataset`; the differ/corpus helpers are unused
// here, so allow dead code on the included module.
#[allow(dead_code)]
#[path = "../../metrics/tests/support/diff_corpus.rs"]
mod diff_corpus;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Tenant header Grafana forwards to Crabka (provisioned as a static
/// `X-Scope-OrgID` header on the datasource).
const TENANT: &str = "grafana";

/// Grafana's default HTTP port.
const GRAFANA_PORT: u16 = 3000;

/// Stable datasource UID referenced by the `/api/ds/query` payload.
const DATASOURCE_UID: &str = "crabka-prom";

/// Pinned Grafana image tag (no `:latest`). Override via `CRABKA_GRAFANA_IMAGE_TAG`.
const GRAFANA_IMAGE_TAG: &str = "11.6.1";

/// The seed data spans t=0..45s; every instant query evaluates at this instant.
const EVAL_MS: i64 = 45_000;

/// Provisioned datasource pointing Grafana at Crabka via the host gateway. The
/// `{PORT}` placeholder is substituted with the mapped Crabka port at runtime.
/// `X-Scope-OrgID` is sent on every datasource request (queries AND resource
/// calls) so Crabka keys storage by the test tenant.
const DATASOURCE_YAML_TEMPLATE: &str = r"apiVersion: 1
datasources:
  - name: Crabka
    type: prometheus
    access: proxy
    uid: crabka-prom
    url: http://host.docker.internal:{PORT}
    isDefault: true
    jsonData:
      httpHeaderName1: X-Scope-OrgID
    secureJsonData:
      httpHeaderValue1: grafana
";

#[tokio::test]
#[ignore = "requires Docker"]
async fn grafana_e2e_covers_all_api_surfaces_and_query_shapes() -> TestResult {
    let client = reqwest::Client::new();

    // In-process Crabka write+query path, bound to a host-reachable address so the
    // Grafana container can dial back via host.docker.internal.
    let crabka = start_crabka_query_server().await?;
    post_remote_write(
        &client,
        &crabka.base_url,
        "/api/v1/write",
        TENANT,
        &remote_write_body(),
    )
    .await?;
    wait_for_query_ready(&client, &crabka.base_url, TENANT, "up").await?;

    // Real Grafana with the provisioned datasource.
    let datasource_yaml = DATASOURCE_YAML_TEMPLATE.replace("{PORT}", &crabka.host_port.to_string());
    let grafana = start_grafana(&datasource_yaml).await?;
    let base = mapped_base_url(&grafana, GRAFANA_PORT).await?;
    wait_for_http_ok(&client, &base, "/api/health").await?;
    // /api/health ("database: ok") can race ahead of datasource provisioning; a
    // query before the datasource UID resolves returns 404. Poll until present.
    wait_for_datasource(&client, &base, DATASOURCE_UID).await?;

    // Collect every mismatch so one run reports all problems rather than aborting
    // on the first.
    let mut fails: Vec<String> = Vec::new();

    check_instant_query_shapes(&client, &base, &mut fails).await?;
    check_range_query_shapes(&client, &base, &mut fails).await?;
    check_resource_surfaces(&client, &base, &mut fails).await?;

    crabka.shutdown();

    if fails.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{} Grafana e2e check(s) failed:\n  - {}",
            fails.len(),
            fails.join("\n  - ")
        )
        .into())
    }
}

// ---------------------------------------------------------------------------
// Instant query-shape matrix (Grafana `/api/ds/query`, queryType=instant)
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
async fn check_instant_query_shapes(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
) -> TestResult {
    // (name, expr, expectation) — driven through Grafana's dashboard query path.
    // Values are computed against `seed_dataset()` evaluated at t=45s:
    //   up{job=api,instance=a}=1
    //   http_requests_total{method=GET,code=200}=120  {method=POST,code=500}=8
    //   cpu_temperature_celsius{job=node,instance=a}=43
    //   http_request_duration_seconds_bucket{le=0.5}=40 {le=1}=70 {le=+Inf}=90
    //   http_request_duration_seconds_sum=60  _count=90
    //   native_histogram_marker=1
    let exact = Expect::exact;
    let approx = Expect::approx;

    // -- selectors & label matchers --------------------------------------------
    instant(
        client,
        base,
        fails,
        "selector gauge",
        "up",
        &[(&[("job", "api")], exact(1.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "selector eq matcher",
        "http_requests_total{method=\"GET\"}",
        &[(&[("method", "GET")], exact(120.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "selector regex matcher",
        "http_requests_total{code=~\"5..\"}",
        &[(&[("method", "POST")], exact(8.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "selector negative matcher",
        "http_requests_total{method!=\"GET\"}",
        &[(&[("code", "500")], exact(8.0))],
    )
    .await;
    instant_empty(
        client,
        base,
        fails,
        "selector matches nothing",
        "http_requests_total{job=\"nope\"}",
    )
    .await;

    // -- aggregations ----------------------------------------------------------
    instant(
        client,
        base,
        fails,
        "sum",
        "sum(http_requests_total)",
        &[(&[], exact(128.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "sum by",
        "sum by (method) (http_requests_total)",
        &[
            (&[("method", "GET")], exact(120.0)),
            (&[("method", "POST")], exact(8.0)),
        ],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "sum without",
        "sum without (method, code, instance) (http_requests_total)",
        &[(&[("job", "api")], exact(128.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "avg",
        "avg(http_requests_total)",
        &[(&[], exact(64.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "min",
        "min(http_requests_total)",
        &[(&[], exact(8.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "max",
        "max(http_requests_total)",
        &[(&[], exact(120.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "count",
        "count(http_requests_total)",
        &[(&[], exact(2.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "group",
        "group(http_requests_total)",
        &[(&[], exact(1.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "topk",
        "topk(1, http_requests_total)",
        &[(&[("method", "GET")], exact(120.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "bottomk",
        "bottomk(1, http_requests_total)",
        &[(&[("method", "POST")], exact(8.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "quantile",
        "quantile(0.5, http_requests_total)",
        &[(&[], exact(64.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "stddev",
        "stddev(http_requests_total)",
        &[(&[], exact(56.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "stdvar",
        "stdvar(http_requests_total)",
        &[(&[], exact(3136.0))],
    )
    .await;
    instant_count(
        client,
        base,
        fails,
        "count_values",
        "count_values(\"v\", http_requests_total)",
        2,
    )
    .await;

    // -- binary operators ------------------------------------------------------
    instant(
        client,
        base,
        fails,
        "scalar arithmetic",
        "http_requests_total * 2",
        &[
            (&[("method", "GET")], exact(240.0)),
            (&[("method", "POST")], exact(16.0)),
        ],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "comparison filter",
        "http_requests_total > 100",
        &[(&[("method", "GET")], exact(120.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "comparison bool",
        "http_requests_total >= bool 100",
        &[
            (&[("method", "GET")], exact(1.0)),
            (&[("method", "POST")], exact(0.0)),
        ],
    )
    .await;
    instant_count(
        client,
        base,
        fails,
        "or set op",
        "up or http_requests_total",
        3,
    )
    .await;
    instant(
        client,
        base,
        fails,
        "and set op on label",
        "http_requests_total and on(job) up",
        &[
            (&[("method", "GET")], exact(120.0)),
            (&[("method", "POST")], exact(8.0)),
        ],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "unless set op",
        "http_requests_total unless on(method) http_requests_total{method=\"GET\"}",
        &[(&[("method", "POST")], exact(8.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "group_left vector match",
        "http_requests_total / on(job) group_left sum by (job) (http_requests_total)",
        &[
            (&[("method", "GET")], approx(120.0 / 128.0)),
            (&[("method", "POST")], approx(8.0 / 128.0)),
        ],
    )
    .await;

    // -- counter / range functions ---------------------------------------------
    // rate over [30s] at 45s: GET samples 30s=75,45s=120 -> ~3/s; POST 30s=5,45s=8 -> ~0.2/s.
    instant(
        client,
        base,
        fails,
        "rate",
        "rate(http_requests_total[30s])",
        &[
            (&[("method", "GET")], approx(3.0)),
            (&[("method", "POST")], approx(0.2)),
        ],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "increase",
        "increase(http_requests_total[30s])",
        &[
            (&[("method", "GET")], approx(90.0)),
            (&[("method", "POST")], approx(6.0)),
        ],
    )
    .await;
    instant_present(
        client,
        base,
        fails,
        "irate",
        "irate(http_requests_total[1m])",
    )
    .await;
    instant_present(
        client,
        base,
        fails,
        "delta",
        "delta(cpu_temperature_celsius[1m])",
    )
    .await;
    instant_present(
        client,
        base,
        fails,
        "idelta",
        "idelta(cpu_temperature_celsius[1m])",
    )
    .await;
    instant(
        client,
        base,
        fails,
        "changes",
        "changes(cpu_temperature_celsius[1m])",
        &[(&[("instance", "a")], approx(3.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "resets",
        "resets(cpu_temperature_celsius[1m])",
        &[(&[("instance", "a")], approx(1.0))],
    )
    .await;

    // -- *_over_time family ----------------------------------------------------
    let temps = "cpu_temperature_celsius[1m]";
    instant(
        client,
        base,
        fails,
        "max_over_time",
        &format!("max_over_time({temps})"),
        &[(&[("instance", "a")], exact(43.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "min_over_time",
        &format!("min_over_time({temps})"),
        &[(&[("instance", "a")], exact(40.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "avg_over_time",
        &format!("avg_over_time({temps})"),
        &[(&[("instance", "a")], approx(41.625))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "sum_over_time",
        &format!("sum_over_time({temps})"),
        &[(&[("instance", "a")], exact(166.5))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "count_over_time",
        &format!("count_over_time({temps})"),
        &[(&[("instance", "a")], exact(4.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "last_over_time",
        &format!("last_over_time({temps})"),
        &[(&[("instance", "a")], exact(43.0))],
    )
    .await;
    instant_present(
        client,
        base,
        fails,
        "stddev_over_time",
        &format!("stddev_over_time({temps})"),
    )
    .await;
    instant(
        client,
        base,
        fails,
        "quantile_over_time",
        &format!("quantile_over_time(0.5, {temps})"),
        &[(&[("instance", "a")], approx(41.75))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "present_over_time",
        &format!("present_over_time({temps})"),
        &[(&[("instance", "a")], exact(1.0))],
    )
    .await;

    // -- histograms (classic) --------------------------------------------------
    // bucket counts at 45s: le0.5=40, le1=70, le+Inf=90; p50 -> ~0.583, p90 -> in (1,+Inf].
    instant(
        client,
        base,
        fails,
        "histogram_quantile p50",
        "histogram_quantile(0.5, http_request_duration_seconds_bucket)",
        &[(&[("job", "api")], approx(0.5833))],
    )
    .await;
    instant_present(
        client,
        base,
        fails,
        "histogram_quantile p90",
        "histogram_quantile(0.9, http_request_duration_seconds_bucket)",
    )
    .await;

    // -- label manipulation ----------------------------------------------------
    instant(
        client,
        base,
        fails,
        "label_replace adds label",
        "label_replace(up, \"datacenter\", \"east\", \"job\", \"api\")",
        &[(&[("datacenter", "east"), ("job", "api")], exact(1.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "label_join concatenates",
        "label_join(up, \"id\", \"-\", \"job\", \"instance\")",
        &[(&[("id", "api-a")], exact(1.0))],
    )
    .await;

    // -- scalar / math / trig --------------------------------------------------
    instant(
        client,
        base,
        fails,
        "scalar",
        "scalar(up)",
        &[(&[], exact(1.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "vector",
        "vector(42)",
        &[(&[], exact(42.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "abs",
        "abs(cpu_temperature_celsius - 100)",
        &[(&[("instance", "a")], exact(57.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "ceil",
        "ceil(http_request_duration_seconds_sum + 0.4)",
        &[(&[("job", "api")], exact(61.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "floor",
        "floor(http_request_duration_seconds_sum + 0.9)",
        &[(&[("job", "api")], exact(60.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "round",
        "round(cpu_temperature_celsius)",
        &[(&[("instance", "a")], exact(43.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "clamp_max",
        "clamp_max(http_requests_total, 100)",
        &[
            (&[("method", "GET")], exact(100.0)),
            (&[("method", "POST")], exact(8.0)),
        ],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "clamp_min",
        "clamp_min(http_requests_total, 50)",
        &[
            (&[("method", "GET")], exact(120.0)),
            (&[("method", "POST")], exact(50.0)),
        ],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "sqrt",
        "sqrt(http_request_duration_seconds_count)",
        &[(&[("job", "api")], approx(9.4868))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "ln",
        "ln(native_histogram_marker)",
        &[(&[("job", "api")], exact(0.0))],
    )
    .await;
    instant_present(client, base, fails, "exp", "exp(native_histogram_marker)").await;
    instant_present(client, base, fails, "trig", "sin(native_histogram_marker)").await;

    // -- time / absence --------------------------------------------------------
    instant(
        client,
        base,
        fails,
        "timestamp",
        "timestamp(up)",
        &[(&[("job", "api")], approx(45.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "absent present-metric",
        "absent(http_requests_total)",
        &[],
    )
    .await; // present -> empty
    instant(
        client,
        base,
        fails,
        "absent missing-metric",
        "absent(nonexistent_metric)",
        &[(&[], exact(1.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "absent_over_time missing",
        "absent_over_time(nonexistent_metric[1m])",
        &[(&[], exact(1.0))],
    )
    .await;

    // -- sort / subquery / modifiers -------------------------------------------
    instant_count(client, base, fails, "sort", "sort(http_requests_total)", 2).await;
    instant_count(
        client,
        base,
        fails,
        "sort_desc",
        "sort_desc(http_requests_total)",
        2,
    )
    .await;
    instant_present(
        client,
        base,
        fails,
        "subquery",
        "max_over_time(rate(http_requests_total[30s])[1m:15s])",
    )
    .await;
    instant(
        client,
        base,
        fails,
        "at modifier",
        "up @ 30.000",
        &[(&[("job", "api")], exact(1.0))],
    )
    .await;
    instant(
        client,
        base,
        fails,
        "offset modifier",
        "http_requests_total offset 15s",
        &[
            (&[("method", "GET")], exact(75.0)),
            (&[("method", "POST")], exact(5.0)),
        ],
    )
    .await;

    Ok(())
}

// ---------------------------------------------------------------------------
// Range query-shape coverage (Grafana `/api/ds/query`, queryType=range)
// ---------------------------------------------------------------------------

async fn check_range_query_shapes(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
) -> TestResult {
    // A representative subset over [0, 45s] step 15s -> >= 2 points per series.
    for (name, expr, min_series) in [
        ("range gauge", "up", 1usize),
        ("range counter rate", "rate(http_requests_total[30s])", 2),
        (
            "range aggregation",
            "sum by (method) (http_requests_total)",
            2,
        ),
        (
            "range histogram_quantile",
            "histogram_quantile(0.5, http_request_duration_seconds_bucket)",
            1,
        ),
        ("range scalar math", "cpu_temperature_celsius * 2", 1),
        (
            "range subquery",
            "max_over_time(rate(http_requests_total[30s])[1m:15s])",
            2,
        ),
    ] {
        let raw = ds_query(client, base, expr, Some((0, EVAL_MS, 15))).await?;
        let series = parse_range_series(&raw);
        if series.len() < min_series {
            fails.push(format!(
                "range `{name}` ({expr}): expected >= {min_series} series, got {} ({raw})",
                series.len()
            ));
            continue;
        }
        let has_multi_point = series.iter().any(|points| points.len() >= 2);
        if !has_multi_point {
            fails.push(format!(
                "range `{name}` ({expr}): no series carried >= 2 data points"
            ));
        }
        // The render path must surface at least one real value. NaN at some steps
        // is legitimate (e.g. histogram_quantile over the all-zero start buckets),
        // so we require some-finite rather than all-finite.
        if !series.iter().flatten().any(|v| v.is_finite()) {
            fails.push(format!(
                "range `{name}` ({expr}): rendered no finite values"
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Resource-proxy surfaces (labels / values / series / metadata / exemplars /
// build-info) via Grafana's datasource resource API.
// ---------------------------------------------------------------------------

async fn check_resource_surfaces(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
) -> TestResult {
    // /api/v1/labels -> includes the seeded label names.
    let labels = resource_json(client, base, "api/v1/labels").await?;
    let names = string_array(&labels["data"]);
    for want in ["__name__", "job", "method", "code", "instance", "le"] {
        if !names.iter().any(|n| n == want) {
            fails.push(format!("resource labels: missing `{want}` in {names:?}"));
        }
    }

    // /api/v1/label/job/values -> {api, node}.
    let job_values = resource_json(client, base, "api/v1/label/job/values").await?;
    let jobs = string_array(&job_values["data"]);
    for want in ["api", "node"] {
        if !jobs.iter().any(|j| j == want) {
            fails.push(format!(
                "resource label values(job): missing `{want}` in {jobs:?}"
            ));
        }
    }

    // /api/v1/label/__name__/values -> includes the seeded metric names.
    let metric_values = resource_json(client, base, "api/v1/label/__name__/values").await?;
    let metrics = string_array(&metric_values["data"]);
    for want in ["up", "http_requests_total", "cpu_temperature_celsius"] {
        if !metrics.iter().any(|m| m == want) {
            fails.push(format!(
                "resource __name__ values: missing `{want}` in {metrics:?}"
            ));
        }
    }

    // /api/v1/series?match[]=http_requests_total -> 2 series.
    let series = resource_json(
        client,
        base,
        "api/v1/series?match%5B%5D=http_requests_total",
    )
    .await?;
    let series_count = series["data"].as_array().map_or(0, Vec::len);
    if series_count != 2 {
        fails.push(format!(
            "resource series(http_requests_total): expected 2, got {series_count} ({series})"
        ));
    }

    // /api/v1/metadata -> reachable, success status (seed carries no metadata, so
    // the payload may be empty; we assert the surface responds correctly).
    let metadata = resource_json(client, base, "api/v1/metadata").await?;
    if metadata["status"] != "success" {
        fails.push(format!(
            "resource metadata: status not success ({metadata})"
        ));
    }

    // /api/v1/query_exemplars -> reachable (seed carries no exemplars).
    let exemplars = resource_json(
        client,
        base,
        "api/v1/query_exemplars?query=http_requests_total&start=0&end=45",
    )
    .await
    .ok();
    if let Some(exemplars) = exemplars
        && exemplars["status"] != "success"
    {
        fails.push(format!(
            "resource query_exemplars: status not success ({exemplars})"
        ));
    }

    // /api/v1/status/buildinfo -> Grafana feature detection; must report success.
    let buildinfo = resource_json(client, base, "api/v1/status/buildinfo").await?;
    if buildinfo["status"] != "success" {
        fails.push(format!(
            "resource buildinfo: status not success ({buildinfo})"
        ));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Query helpers + frame parsing
// ---------------------------------------------------------------------------

/// Expected value for a single series.
#[derive(Clone, Copy)]
enum Expect {
    Exact(f64),
    Approx(f64),
}

impl Expect {
    fn exact(value: f64) -> Self {
        Self::Exact(value)
    }
    fn approx(value: f64) -> Self {
        Self::Approx(value)
    }
    fn matches(self, got: f64) -> bool {
        match self {
            Self::Exact(want) => (got - want).abs() <= 1e-6 * want.abs().max(1.0) + 1e-9,
            Self::Approx(want) => (got - want).abs() <= 0.05 * want.abs().max(1.0) + 1e-6,
        }
    }
    fn want(self) -> f64 {
        match self {
            Self::Exact(v) | Self::Approx(v) => v,
        }
    }
}

/// Run an instant query through Grafana and assert the parsed series exactly
/// match the expected `(label-subset, value)` set (order-independent).
async fn instant(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
    name: &str,
    expr: &str,
    expected: &[(&[(&str, &str)], Expect)],
) {
    let raw = match ds_query(client, base, expr, None).await {
        Ok(raw) => raw,
        Err(error) => {
            fails.push(format!(
                "instant `{name}` ({expr}): request failed: {error}"
            ));
            return;
        }
    };
    let series = parse_instant_series(&raw);
    if series.len() != expected.len() {
        fails.push(format!(
            "instant `{name}` ({expr}): expected {} series, got {} ({})",
            expected.len(),
            series.len(),
            serde_json::to_string(&raw).unwrap_or_default()
        ));
        return;
    }
    for (want_labels, want_value) in expected {
        match series_value(&series, want_labels) {
            Some(got) if want_value.matches(got) => {}
            Some(got) => fails.push(format!(
                "instant `{name}` ({expr}): series {want_labels:?} = {got}, expected ~{}",
                want_value.want()
            )),
            None => fails.push(format!(
                "instant `{name}` ({expr}): no series matched {want_labels:?} in {series:?}"
            )),
        }
    }
}

/// Assert an instant query renders exactly `count` series (values unchecked,
/// only that the path rendered the right cardinality).
async fn instant_count(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
    name: &str,
    expr: &str,
    count: usize,
) {
    match ds_query(client, base, expr, None).await {
        Ok(raw) => {
            let series = parse_instant_series(&raw);
            if series.len() != count {
                fails.push(format!(
                    "instant `{name}` ({expr}): expected {count} series, got {} ({series:?})",
                    series.len()
                ));
            }
        }
        Err(error) => fails.push(format!(
            "instant `{name}` ({expr}): request failed: {error}"
        )),
    }
}

/// Assert an instant query renders at least one finite-valued series.
async fn instant_present(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
    name: &str,
    expr: &str,
) {
    match ds_query(client, base, expr, None).await {
        Ok(raw) => {
            let series = parse_instant_series(&raw);
            if series.is_empty() || series.iter().any(|(_, v)| !v.is_finite()) {
                fails.push(format!(
                    "instant `{name}` ({expr}): expected >= 1 finite series, got {series:?}"
                ));
            }
        }
        Err(error) => fails.push(format!(
            "instant `{name}` ({expr}): request failed: {error}"
        )),
    }
}

/// Assert an instant query renders no series (empty vector).
async fn instant_empty(
    client: &reqwest::Client,
    base: &str,
    fails: &mut Vec<String>,
    name: &str,
    expr: &str,
) {
    match ds_query(client, base, expr, None).await {
        Ok(raw) => {
            let series = parse_instant_series(&raw);
            if !series.is_empty() {
                fails.push(format!(
                    "instant `{name}` ({expr}): expected empty, got {series:?}"
                ));
            }
        }
        Err(error) => fails.push(format!(
            "instant `{name}` ({expr}): request failed: {error}"
        )),
    }
}

/// Find the value of the parsed series whose labels include every `(k, v)` in
/// `want` (label-subset match, ignoring `__name__`).
fn series_value(series: &[(BTreeMap<String, String>, f64)], want: &[(&str, &str)]) -> Option<f64> {
    series
        .iter()
        .find(|(labels, _)| {
            want.iter()
                .all(|(k, v)| labels.get(*k).map(String::as_str) == Some(*v))
        })
        .map(|(_, value)| *value)
}

/// Parse a Grafana `/api/ds/query` instant response into `(labels, value)` per
/// series. Grafana renders each series as a numeric field carrying the series
/// labels; the instant value is the field's last (only) datum.
fn parse_instant_series(resp: &Value) -> Vec<(BTreeMap<String, String>, f64)> {
    let mut out = Vec::new();
    let Some(frames) = resp["results"]["A"]["frames"].as_array() else {
        return out;
    };
    for frame in frames {
        let (Some(fields), Some(columns)) = (
            frame["schema"]["fields"].as_array(),
            frame["data"]["values"].as_array(),
        ) else {
            continue;
        };
        for (index, field) in fields.iter().enumerate() {
            if field["type"].as_str() != Some("number") {
                continue; // skip the Time field
            }
            let labels = field["labels"]
                .as_object()
                .map(|map| {
                    map.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect::<BTreeMap<_, _>>()
                })
                .unwrap_or_default();
            if let Some(value) = columns.get(index).and_then(Value::as_array).and_then(|c| {
                c.last()
                    .and_then(Value::as_f64)
                    .or((!c.is_empty()).then_some(f64::NAN))
            }) {
                out.push((labels, value));
            }
        }
    }
    out
}

/// Parse a Grafana `/api/ds/query` range response into one value-column per
/// series.
fn parse_range_series(resp: &Value) -> Vec<Vec<f64>> {
    let mut out = Vec::new();
    let Some(frames) = resp["results"]["A"]["frames"].as_array() else {
        return out;
    };
    for frame in frames {
        let (Some(fields), Some(columns)) = (
            frame["schema"]["fields"].as_array(),
            frame["data"]["values"].as_array(),
        ) else {
            continue;
        };
        for (index, field) in fields.iter().enumerate() {
            if field["type"].as_str() != Some("number") {
                continue;
            }
            if let Some(column) = columns.get(index).and_then(Value::as_array) {
                out.push(
                    column
                        .iter()
                        .map(|v| v.as_f64().unwrap_or(f64::NAN))
                        .collect(),
                );
            }
        }
    }
    out
}

/// Issue a Grafana datasource query (`POST /api/ds/query`). `range` is
/// `(start_ms, end_ms, step_secs)`; `None` is an instant query at `EVAL_MS`.
async fn ds_query(
    client: &reqwest::Client,
    base: &str,
    expr: &str,
    range: Option<(i64, i64, i64)>,
) -> TestResult<Value> {
    let mut target = json!({
        "refId": "A",
        "datasource": { "type": "prometheus", "uid": DATASOURCE_UID },
        "expr": expr,
    });
    let (from, to) = if let Some((start, end, step)) = range {
        target["queryType"] = json!("range");
        target["range"] = json!(true);
        target["intervalMs"] = json!(step * 1000);
        target["maxDataPoints"] = json!(1000);
        (start, end)
    } else {
        target["queryType"] = json!("instant");
        target["instant"] = json!(true);
        (EVAL_MS, EVAL_MS)
    };
    let body = json!({ "from": from.to_string(), "to": to.to_string(), "queries": [target] });

    let response = client
        .post(format!("{base}/api/ds/query"))
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    Ok(response.json().await?)
}

/// GET a datasource resource path through Grafana's resource proxy, returning the
/// raw Prometheus JSON the datasource produced. `path` is the datasource-relative
/// path (e.g. `api/v1/labels`), already URL-encoded where needed.
async fn resource_json(client: &reqwest::Client, base: &str, path: &str) -> TestResult<Value> {
    let url = format!("{base}/api/datasources/uid/{DATASOURCE_UID}/resources/{path}");
    let response = client.get(url).send().await?.error_for_status()?;
    Ok(response.json().await?)
}

/// Extract a JSON string array (Prometheus `data` payloads for labels/values).
fn string_array(value: &Value) -> Vec<String> {
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Container + Crabka harness
// ---------------------------------------------------------------------------

async fn start_grafana(
    datasource_yaml: &str,
) -> TestResult<testcontainers::ContainerAsync<GenericImage>> {
    let tag =
        std::env::var("CRABKA_GRAFANA_IMAGE_TAG").unwrap_or_else(|_| GRAFANA_IMAGE_TAG.to_string());
    Ok(
        GenericImage::new("mirror.gcr.io/grafana/grafana".to_string(), tag)
            .with_exposed_port(GRAFANA_PORT.tcp())
            // Grafana writes its go logger to STDOUT (verified: the "HTTP Server
            // Listen" line appears on stdout, not stderr). /api/health is polled for
            // real readiness below.
            .with_wait_for(WaitFor::message_on_stdout("HTTP Server Listen"))
            // Provision the datasource so no UI/API setup is needed.
            .with_copy_to(
                "/etc/grafana/provisioning/datasources/crabka.yaml",
                datasource_yaml.as_bytes().to_vec(),
            )
            // host.docker.internal -> host gateway lets the container reach the Crabka
            // server running on the host.
            .with_host("host.docker.internal", Host::HostGateway)
            // Anonymous admin so the test drives the API without a login.
            .with_env_var("GF_AUTH_ANONYMOUS_ENABLED", "true")
            .with_env_var("GF_AUTH_ANONYMOUS_ORG_ROLE", "Admin")
            .with_env_var("GF_AUTH_BASIC_ENABLED", "false")
            .start()
            .await?,
    )
}

async fn mapped_base_url(
    container: &testcontainers::ContainerAsync<GenericImage>,
    port: u16,
) -> TestResult<String> {
    let mapped = container.get_host_port_ipv4(port.tcp()).await?;
    Ok(format!("http://127.0.0.1:{mapped}"))
}

struct CrabkaServer {
    base_url: String,
    /// Host-reachable port the container dials via host.docker.internal.
    host_port: u16,
    shutdown: Option<oneshot::Sender<()>>,
}

impl CrabkaServer {
    fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

async fn start_crabka_query_server() -> TestResult<CrabkaServer> {
    let head = WalHead::new();
    let query_router = crabka_metrics_service::prometheus_router_for_store(head.clone());
    let sink: Arc<dyn WalSink> = Arc::new(WalHeadSink { head });
    let distributor = Arc::new(DistributorState::new(sink));
    let router = query_router.merge(crabka_metrics::distributor::router(distributor));
    let (tx, rx) = oneshot::channel();
    // Bind 0.0.0.0 so the Grafana container can reach the server through the
    // Docker host gateway; the OS picks the port.
    let addr: SocketAddr = "0.0.0.0:0".parse()?;
    let bound = crabka_metrics_service::serve_prometheus_router(addr, router, async move {
        let _ = rx.await;
    })
    .await?;

    Ok(CrabkaServer {
        // Local queries dial loopback against the bound port.
        base_url: format!("http://127.0.0.1:{}", bound.port()),
        host_port: bound.port(),
        shutdown: Some(tx),
    })
}

struct WalHeadSink {
    head: WalHead,
}

#[async_trait::async_trait]
impl WalSink for WalHeadSink {
    async fn append(&self, _key: Bytes, record: WalRecord) -> Result<(), ProduceError> {
        self.head.apply_wal_record(&record);
        Ok(())
    }
}

fn remote_write_body() -> Vec<u8> {
    let req = pb::v1::WriteRequest {
        timeseries: seed_dataset()
            .into_iter()
            .map(|point| pb::v1::TimeSeries {
                labels: remote_write_labels(point.metric, point.labels),
                samples: point
                    .samples
                    .iter()
                    .map(|(timestamp, value)| pb::v1::Sample {
                        value: *value,
                        timestamp: *timestamp,
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    snap::raw::Encoder::new()
        .compress_vec(&req.encode_to_vec())
        .expect("snappy remote_write")
}

fn remote_write_labels(metric: &str, labels: &[(&str, &str)]) -> Vec<pb::v1::Label> {
    std::iter::once(pb::v1::Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    })
    .chain(labels.iter().map(|(name, value)| pb::v1::Label {
        name: (*name).to_string(),
        value: (*value).to_string(),
    }))
    .collect()
}

async fn post_remote_write(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    tenant: &str,
    body: &[u8],
) -> TestResult {
    let status = client
        .post(format!("{base}{path}"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .header("X-Scope-OrgID", tenant)
        .body(body.to_vec())
        .send()
        .await?
        .status();
    if !(status == StatusCode::OK || status == StatusCode::NO_CONTENT) {
        return Err(format!("remote_write to {base}{path} returned {status}").into());
    }
    Ok(())
}

async fn wait_for_http_ok(client: &reqwest::Client, base: &str, path: &str) -> TestResult {
    let deadline = std::time::Instant::now() + std::time::Duration::from_mins(1);
    while std::time::Instant::now() < deadline {
        if client
            .get(format!("{base}{path}"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    Err(format!("{base}{path} did not become ready").into())
}

async fn wait_for_datasource(client: &reqwest::Client, base: &str, uid: &str) -> TestResult {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(90);
    while std::time::Instant::now() < deadline {
        if client
            .get(format!("{base}/api/datasources/uid/{uid}"))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    Err(format!("datasource {uid} was not provisioned on {base}").into())
}

async fn wait_for_query_ready(
    client: &reqwest::Client,
    base: &str,
    tenant: &str,
    query: &str,
) -> TestResult {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while std::time::Instant::now() < deadline {
        let url = format!(
            "{base}/api/v1/query?query={}&time=45.000",
            url::form_urlencoded::byte_serialize(query.as_bytes()).collect::<String>()
        );
        let json: Value = client
            .get(url)
            .header("X-Scope-OrgID", tenant)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if json["data"]["result"]
            .as_array()
            .is_some_and(|result| !result.is_empty())
        {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    Err(format!("query `{query}` did not become non-empty on {base}").into())
}
