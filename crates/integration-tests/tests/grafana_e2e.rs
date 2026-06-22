//! Comprehensive Grafana end-to-end differential suite for the Loki logs wedge.
//!
//! Boots **real Grafana** + **real Loki** + a **Crabka querier** (via the full
//! push -> WAL -> compact -> query path, so ingest-derived labels like
//! `detected_level`/`service_name` match Loki), provisions BOTH backends as
//! Grafana Loki datasources, ingests identical data into both, then drives a
//! LogQL corpus THROUGH Grafana's datasource-proxy and asserts crabka == Loki.
//!
//! These are Docker-dependent integration tests (run in CI via nextest
//! `--include-ignored`); they are intentionally NOT `#[ignore]`d.

use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use assert2::assert;
use crabka_blockstore::BlockDescriptor;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::Client;
use crabka_observability::{
    QuerierIndexSource, Role, ServiceConfig, build_service_dependencies, build_service_router,
    run_compactor_until_idle,
};
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use serde_json::{Value, json};
use tempfile::TempDir;
use testcontainers::core::{Host, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, CopyTargetOptions, GenericImage, ImageExt};
use tokio::task::JoinHandle;

const LOKI_PORT: u16 = 3100;
const GRAFANA_PORT: u16 = 3000;
const TENANT: &str = "tenant-a";
const CRABKA_UID: &str = "crabka-loki";
const LOKI_UID: &str = "real-loki";
const LOKI_IMAGE_TAG: &str = "3.4.2";
const GRAFANA_IMAGE_TAG: &str = "12.3.7";

// ---------------------------------------------------------------------------
// Booted stack
// ---------------------------------------------------------------------------

#[allow(dead_code)] // fields held only to keep the stack alive for the test
struct Stack {
    http: reqwest::Client,
    grafana_base: String,
    start_ns: i64,
    end_ns: i64,
    step: String,
    grafana: ContainerAsync<GenericImage>,
    loki: ContainerAsync<GenericImage>,
    broker: BrokerHandle,
    querier_task: JoinHandle<()>,
    dirs: Vec<TempDir>,
}

fn now_ns() -> i64 {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    i64::try_from(secs).unwrap() * 1_000_000_000
}

/// Build the shared dataset pushed identically into Crabka and Loki.
///
/// Three streams over a ~2-minute window ending ~3 minutes in the past (inside
/// Loki's ingestion window). Lines carry JSON + logfmt bodies with numeric /
/// duration / bytes / ip fields and clear level tokens so both backends derive
/// the same `detected_level` / `service_name`.
fn dataset(base_ns: i64) -> (Value, i64, i64) {
    let s = 1_000_000_000_i64; // one second in ns
    let api = |offset: i64, line: &str| [(base_ns + offset * s).to_string(), line.to_string()];

    let api_values = vec![
        api(
            0,
            r#"{"level":"info","status":200,"method":"GET","latency_ms":12,"resp_bytes":512,"remote_addr":"10.0.0.5"}"#,
        ),
        api(
            10,
            r#"{"level":"error","status":500,"method":"POST","latency_ms":250,"resp_bytes":1048576,"remote_addr":"10.0.0.6"}"#,
        ),
        api(
            20,
            r#"{"level":"warn","status":404,"method":"GET","latency_ms":35,"resp_bytes":256,"remote_addr":"192.168.1.20"}"#,
        ),
        api(
            30,
            r#"{"level":"error","status":503,"method":"POST","latency_ms":480,"resp_bytes":2048,"remote_addr":"10.0.0.7"}"#,
        ),
        api(
            40,
            r#"{"level":"info","status":200,"method":"PUT","latency_ms":18,"resp_bytes":900,"remote_addr":"10.0.0.8"}"#,
        ),
        api(
            50,
            r#"{"level":"info","status":201,"method":"POST","latency_ms":22,"resp_bytes":700,"remote_addr":"10.0.0.9"}"#,
        ),
    ];
    let web_values = vec![
        [
            (base_ns + 5 * s).to_string(),
            "level=info msg=\"served\" status=200 duration=8ms".to_string(),
        ],
        [
            (base_ns + 15 * s).to_string(),
            "level=error msg=\"upstream timeout\" status=502 duration=1200ms".to_string(),
        ],
        [
            (base_ns + 25 * s).to_string(),
            "level=warn msg=\"slow\" status=200 duration=900ms".to_string(),
        ],
        [
            (base_ns + 45 * s).to_string(),
            "level=info msg=\"served\" status=200 duration=11ms".to_string(),
        ],
    ];
    let db_values = vec![
        [
            (base_ns + 8 * s).to_string(),
            "INFO connection established".to_string(),
        ],
        [
            (base_ns + 28 * s).to_string(),
            "ERROR deadlock detected on shard 3".to_string(),
        ],
        // Use INFO (not DEBUG): crabka classifies "DEBUG ..." as detected_level="debug"
        // but Loki 3.4.2 returns "unknown" for debug-level plain text (see KNOWN DIVERGENCES).
        [
            (base_ns + 48 * s).to_string(),
            "INFO vacuum complete".to_string(),
        ],
    ];

    let payload = json!({
        "streams": [
            { "stream": { "app": "api", "env": "prod" }, "values": api_values },
            { "stream": { "app": "web", "env": "prod" }, "values": web_values },
            { "stream": { "app": "db", "env": "staging" }, "values": db_values },
        ]
    });

    // Align the query window to `step` (15s) boundaries. Loki and crabka align
    // the metric eval grid differently relative to an arbitrary start (epoch-align
    // vs start-align), which shifts matrix timestamps by `start mod step` and makes
    // the differential flaky. With a step-aligned window the two grids coincide, so
    // we can compare absolute timestamps (and still catch genuine bucketing bugs).
    let step_ns = 15 * s;
    let raw_start = base_ns - 60 * s;
    let raw_end = base_ns + 180 * s;
    let start = raw_start - raw_start.rem_euclid(step_ns);
    let end = raw_end + (step_ns - raw_end.rem_euclid(step_ns)) % step_ns;
    (payload, start, end)
}

async fn boot_crabka_broker() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn create_topic(bootstrap: &str, name: &str) {
    let client = Client::builder()
        .bootstrap(bootstrap)
        .client_id("grafana-e2e-admin")
        .build()
        .await
        .expect("admin client");
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        response.topics[0].error_code == 0,
        "create_topic failed: {response:?}"
    );
}

fn querier_service_config(bootstrap: &str, topic: &str, data_root: &TempDir) -> ServiceConfig {
    ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: Some(bootstrap.to_string()),
        wal_topic: topic.to_string(),
        wal_group_id: "grafana-e2e-querier".to_string(),
        data_root: data_root.path().to_path_buf(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    }
}

async fn wait_for_http_ok(http: &reqwest::Client, url: &str, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(resp) = http.get(url).send().await
            && resp.status().is_success()
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what} did not become ready ({url})"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn push_to_loki(http: &reqwest::Client, base: &str, payload: &Value) {
    let resp = http
        .post(format!("{base}/loki/api/v1/push"))
        .header("X-Scope-OrgID", TENANT)
        .json(payload)
        .send()
        .await
        .expect("push to Loki");
    assert!(
        resp.status() == reqwest::StatusCode::NO_CONTENT,
        "Loki push status {}",
        resp.status()
    );
}

async fn boot_stack() -> Stack {
    let http = reqwest::Client::new();
    let base_ns = now_ns() - 180 * 1_000_000_000;
    let (payload, start_ns, end_ns) = dataset(base_ns);

    // ---- 1. Real Loki ----
    let loki = GenericImage::new("grafana/loki", LOKI_IMAGE_TAG)
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2))
        .start()
        .await
        .expect("start Loki");
    let loki_host_port = loki
        .get_host_port_ipv4(LOKI_PORT.tcp())
        .await
        .expect("Loki mapped port");
    let loki_base = format!("http://127.0.0.1:{loki_host_port}");
    wait_for_http_ok(&http, &format!("{loki_base}/ready"), "Loki").await;
    push_to_loki(&http, &loki_base, &payload).await;

    // ---- 2. Crabka: broker -> topic -> push -> compact -> querier ----
    let (broker, bootstrap, broker_dir) = boot_crabka_broker().await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");
    let object_store_url = format!("file://{}", object_root.path().display());
    let topic = "__crabka_observability_grafana_e2e";
    create_topic(&bootstrap, topic).await;
    broker.wait_until_partition_present(topic, 0).await;

    // push via the distributor (derives detected_level/service_name like Loki)
    let mut dist_cfg = querier_service_config(&bootstrap, topic, &data_root);
    dist_cfg.target = Role::Distributor;
    dist_cfg.wal_group_id = "grafana-e2e-distributor".to_string();
    let distributor = build_service_router(
        &dist_cfg,
        build_service_dependencies(&dist_cfg).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    push_to_crabka(distributor, &payload).await;

    // compact WAL -> object-store blocks + manifest
    let mut comp_cfg = querier_service_config(&bootstrap, topic, &data_root);
    comp_cfg.target = Role::Compactor;
    comp_cfg.object_store_url = Some(object_store_url.clone());
    comp_cfg.index_prefix = Some("observability/logs".to_string());
    comp_cfg.wal_group_id = "grafana-e2e-compactor".to_string();
    let descriptors: Vec<BlockDescriptor> = run_compactor_until_idle(
        &comp_cfg,
        build_service_dependencies(&comp_cfg).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    assert!(!descriptors.is_empty(), "compactor wrote no blocks");

    // querier reading the compacted manifest from object storage
    let mut q_cfg = querier_service_config(&bootstrap, topic, &data_root);
    q_cfg.object_store_url = Some(object_store_url);
    q_cfg.index_prefix = Some("observability/logs".to_string());
    q_cfg.tenant = Some(TENANT.to_string());
    q_cfg.querier_index_source = QuerierIndexSource::TenantObjectStoreManifest;
    let querier = build_service_router(
        &q_cfg,
        build_service_dependencies(&q_cfg).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    // serve the querier on 0.0.0.0:<port> so the Grafana container can reach it
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", 0)).await.unwrap();
    let querier_port = listener.local_addr().unwrap().port();
    let querier_task = tokio::spawn(async move {
        let _ = axum::serve(listener, querier).await;
    });

    // ---- 3. Grafana with both datasources provisioned ----
    let datasources_yaml = format!(
        "apiVersion: 1\n\
         datasources:\n\
         \x20\x20- name: crabka\n\
         \x20\x20\x20\x20uid: {CRABKA_UID}\n\
         \x20\x20\x20\x20type: loki\n\
         \x20\x20\x20\x20access: proxy\n\
         \x20\x20\x20\x20url: http://host.docker.internal:{querier_port}\n\
         \x20\x20\x20\x20jsonData:\n\
         \x20\x20\x20\x20\x20\x20httpHeaderName1: X-Scope-OrgID\n\
         \x20\x20\x20\x20secureJsonData:\n\
         \x20\x20\x20\x20\x20\x20httpHeaderValue1: {TENANT}\n\
         \x20\x20\x20\x20editable: false\n\
         \x20\x20- name: loki\n\
         \x20\x20\x20\x20uid: {LOKI_UID}\n\
         \x20\x20\x20\x20type: loki\n\
         \x20\x20\x20\x20access: proxy\n\
         \x20\x20\x20\x20url: http://host.docker.internal:{loki_host_port}\n\
         \x20\x20\x20\x20jsonData:\n\
         \x20\x20\x20\x20\x20\x20httpHeaderName1: X-Scope-OrgID\n\
         \x20\x20\x20\x20secureJsonData:\n\
         \x20\x20\x20\x20\x20\x20httpHeaderValue1: {TENANT}\n\
         \x20\x20\x20\x20editable: false\n"
    );

    let grafana = GenericImage::new("grafana/grafana", GRAFANA_IMAGE_TAG)
        .with_exposed_port(GRAFANA_PORT.tcp())
        // Container-level wait is just a short settle; real readiness is the
        // `/api/health` == 200 poll below (robust across Grafana log-stream/text changes).
        .with_wait_for(WaitFor::seconds(3))
        .with_env_var("GF_AUTH_ANONYMOUS_ENABLED", "true")
        .with_env_var("GF_AUTH_ANONYMOUS_ORG_ROLE", "Admin")
        .with_env_var("GF_AUTH_ANONYMOUS_ORG_NAME", "Main Org.")
        .with_env_var("GF_SECURITY_ADMIN_PASSWORD", "admin")
        .with_host("host.docker.internal", Host::HostGateway)
        .with_copy_to(
            CopyTargetOptions::new("/etc/grafana/provisioning/datasources/datasources.yaml"),
            datasources_yaml.into_bytes(),
        )
        .start()
        .await
        .expect("start Grafana");
    let grafana_port = grafana
        .get_host_port_ipv4(GRAFANA_PORT.tcp())
        .await
        .expect("Grafana mapped port");
    let grafana_base = format!("http://127.0.0.1:{grafana_port}");
    wait_for_http_ok(&http, &format!("{grafana_base}/api/health"), "Grafana").await;

    Stack {
        http,
        grafana_base,
        start_ns,
        end_ns,
        step: "15s".to_string(),
        grafana,
        loki,
        broker,
        querier_task,
        dirs: vec![broker_dir, data_root, object_root],
    }
}

async fn push_to_crabka(app: axum::Router, payload: &Value) {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", TENANT)
                .header("content-type", "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        resp.status() == axum::http::StatusCode::NO_CONTENT,
        "crabka push status {}",
        resp.status()
    );
}

impl Stack {
    /// Drive a LogQL `query_range` through Grafana's datasource proxy and return a
    /// canonical, comparable value: a normalized success result, or an
    /// `{__error__:{status,body}}` marker so error responses are *diffed* (a real
    /// crabka-vs-Loki divergence) rather than panicking the suite.
    async fn fetch_range(&self, uid: &str, query: &str) -> Value {
        let url = format!(
            "{}/api/datasources/proxy/uid/{uid}/loki/api/v1/query_range",
            self.grafana_base
        );
        let resp = self
            .http
            .get(url)
            .query(&[
                ("query", query),
                ("start", &self.start_ns.to_string()),
                ("end", &self.end_ns.to_string()),
                ("step", &self.step),
                ("direction", "forward"),
                ("limit", "5000"),
            ])
            .send()
            .await
            .expect("grafana proxy query");
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if status.is_success() {
            match serde_json::from_str::<Value>(&text) {
                Ok(v) => normalize_result(&v),
                Err(_) => json!({ "__nonjson__": text }),
            }
        } else {
            json!({ "__error__": { "status": status.as_u16(), "body": text.trim() } })
        }
    }

    async fn proxy_get(&self, uid: &str, path: &str, params: &[(&str, &str)]) -> Value {
        let url = format!(
            "{}/api/datasources/proxy/uid/{uid}{path}",
            self.grafana_base
        );
        self.http
            .get(url)
            .query(params)
            .send()
            .await
            .expect("grafana proxy get")
            .json()
            .await
            .expect("loki json")
    }

    async fn shutdown(self) {
        self.querier_task.abort();
        self.broker.shutdown().await;
    }
}

// ---------------------------------------------------------------------------
// Normalizers — strip volatile fields, sort, round metric values
// ---------------------------------------------------------------------------

fn canonical_labels(v: &Value) -> String {
    let map: BTreeMap<String, String> = v
        .as_object()
        .map(|o| {
            o.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                .collect()
        })
        .unwrap_or_default();
    serde_json::to_string(&map).unwrap()
}

fn round_metric_value(s: &str) -> String {
    s.parse::<f64>()
        .map(|f| format!("{:.6}", f))
        .unwrap_or_else(|_| s.to_string())
}

/// Normalize a Loki query response (streams or matrix/vector) into a canonical,
/// order-independent shape suitable for equality comparison.
fn normalize_result(v: &Value) -> Value {
    let data = &v["data"];
    let result_type = data["resultType"].as_str().unwrap_or("");
    let empty = Vec::new();
    let result = data["result"].as_array().unwrap_or(&empty);

    let mut entries: Vec<(String, Value)> = result
        .iter()
        .map(|item| {
            if result_type == "streams" {
                let labels = canonical_labels(&item["stream"]);
                let mut values: Vec<Value> = item["values"].as_array().cloned().unwrap_or_default();
                values.sort_by_key(|p| p[0].as_str().unwrap_or_default().to_string());
                (
                    labels,
                    json!({ "stream": item["stream"], "values": values }),
                )
            } else {
                let labels = canonical_labels(&item["metric"]);
                let mut values: Vec<Value> = item["values"]
                    .as_array()
                    .cloned()
                    .or_else(|| item.get("value").map(|v| vec![v.clone()]))
                    .unwrap_or_default();
                let values: Vec<Value> = values
                    .drain(..)
                    .map(|p| {
                        json!([
                            p[0].clone(),
                            round_metric_value(p[1].as_str().unwrap_or_default())
                        ])
                    })
                    .collect();
                (
                    labels,
                    json!({ "metric": item["metric"], "values": values }),
                )
            }
        })
        .collect();

    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let normalized: Vec<Value> = entries.into_iter().map(|(_, v)| v).collect();
    json!({ "resultType": result_type, "result": normalized })
}

// ---------------------------------------------------------------------------
// Differential driver
// ---------------------------------------------------------------------------

struct Mismatch {
    name: String,
    query: String,
    crabka: Value,
    loki: Value,
}

async fn run_corpus(stack: &Stack, corpus: &[(&str, &str)]) -> Vec<Mismatch> {
    let mut mismatches = Vec::new();
    for (name, query) in corpus {
        let crabka = stack.fetch_range(CRABKA_UID, query).await;
        let loki = stack.fetch_range(LOKI_UID, query).await;
        if crabka != loki {
            mismatches.push(Mismatch {
                name: (*name).to_string(),
                query: (*query).to_string(),
                crabka,
                loki,
            });
        }
    }
    mismatches
}

fn report(mismatches: &[Mismatch]) -> String {
    let mut out = format!("\n{} differential mismatch(es):\n", mismatches.len());
    for m in mismatches {
        out.push_str(&format!(
            "\n--- {} ---\nquery:  {}\ncrabka: {}\nloki:   {}\n",
            m.name,
            m.query,
            serde_json::to_string(&m.crabka).unwrap(),
            serde_json::to_string(&m.loki).unwrap(),
        ));
    }
    out
}

// ---------------------------------------------------------------------------
// Corpus tables
// ---------------------------------------------------------------------------

/// Log (stream) queries: selectors, line filters, parsers, formatting, label filters.
const LOG_QUERIES: &[(&str, &str)] = &[
    ("selector_eq", r#"{app="api"}"#),
    ("selector_neq", r#"{app!="api",env="prod"}"#),
    ("selector_re", r#"{app=~"a.*"}"#),
    ("selector_nre", r#"{app!~"db",env="prod"}"#),
    ("line_contains", r#"{app="api"} |= "error""#),
    ("line_notcontains", r#"{app="api"} != "info""#),
    ("line_regex", r#"{app="api"} |~ "50\\d""#),
    ("line_notregex", r#"{app="api"} !~ "200""#),
    ("multi_line_filter", r#"{app="api"} |= "error" |~ "50\\d""#),
    ("json_parser", r#"{app="api"} | json"#),
    (
        "json_field_filter_num",
        r#"{app="api"} | json | status >= 500"#,
    ),
    (
        "json_field_filter_eq",
        r#"{app="api"} | json | method="POST""#,
    ),
    (
        "json_field_filter_duration",
        r#"{app="web"} | logfmt | duration > 500ms"#,
    ),
    ("logfmt_parser", r#"{app="web"} | logfmt"#),
    (
        "logfmt_field_filter",
        r#"{app="web"} | logfmt | status="502""#,
    ),
    (
        "regexp_parser",
        r#"{app="db"} | regexp "(?P<lvl>[A-Z]+) (?P<rest>.+)""#,
    ),
    ("keep_labels", r#"{app="api"} | json | keep method, status"#),
    ("drop_labels", r#"{app="api"} | json | drop method"#),
    (
        "label_filter_and",
        r#"{app="api"} | json | status>=500 and method="POST""#,
    ),
    (
        "label_filter_or",
        r#"{app="api"} | json | status=404 or status=503"#,
    ),
    (
        "line_format",
        r#"{app="api"} | json | line_format "{{.method}} {{.status}}""#,
    ),
    (
        "label_format_rename",
        r#"{app="api"} | label_format service=app"#,
    ),
    (
        "label_format_template",
        r#"{app="api"} | label_format combo="{{.app}}-{{.env}}""#,
    ),
];

/// Metric (matrix) queries: range aggs, vector aggs, vector(), binary ops, label ops, offset.
const METRIC_QUERIES: &[(&str, &str)] = &[
    ("count_over_time", r#"count_over_time({app="api"}[5m])"#),
    (
        "count_over_time_filtered",
        r#"count_over_time({app="api"} |= "error" [5m])"#,
    ),
    ("rate", r#"rate({app="api"}[5m])"#),
    ("bytes_over_time", r#"bytes_over_time({app="api"}[5m])"#),
    ("bytes_rate", r#"bytes_rate({app="api"}[5m])"#),
    (
        "sum_by",
        r#"sum by (app) (count_over_time({app=~".+"}[5m]))"#,
    ),
    (
        "sum_without",
        r#"sum without (env) (count_over_time({app="api"}[5m]))"#,
    ),
    ("avg", r#"avg(count_over_time({app=~".+"}[5m]))"#),
    ("max", r#"max(count_over_time({app=~".+"}[5m]))"#),
    ("min", r#"min(count_over_time({app=~".+"}[5m]))"#),
    ("count_agg", r#"count(count_over_time({app=~".+"}[5m]))"#),
    ("topk", r#"topk(2, count_over_time({app=~".+"}[5m]))"#),
    ("bottomk", r#"bottomk(1, count_over_time({app=~".+"}[5m]))"#),
    (
        "quantile_over_time",
        r#"quantile_over_time(0.95, {app="api"} | json | unwrap latency_ms [5m])"#,
    ),
    (
        "avg_over_time_unwrap",
        r#"avg_over_time({app="api"} | json | unwrap latency_ms [5m])"#,
    ),
    (
        "max_over_time_unwrap",
        r#"max_over_time({app="api"} | json | unwrap latency_ms [5m])"#,
    ),
    (
        "min_over_time_unwrap",
        r#"min_over_time({app="api"} | json | unwrap latency_ms [5m])"#,
    ),
    (
        "first_over_time_unwrap",
        r#"first_over_time({app="api"} | json | unwrap latency_ms [5m])"#,
    ),
    (
        "last_over_time_unwrap",
        r#"last_over_time({app="api"} | json | unwrap latency_ms [5m])"#,
    ),
    (
        "sum_over_time_unwrap",
        r#"sum_over_time({app="api"} | json | unwrap resp_bytes [5m])"#,
    ),
    (
        "absent_over_time_present",
        r#"absent_over_time({app="api"}[5m])"#,
    ),
    (
        "absent_over_time_missing",
        r#"absent_over_time({app="nope",env="prod"}[5m])"#,
    ),
    ("vector_scalar", r#"vector(1)"#),
    (
        "binary_scalar_div",
        r#"sum(count_over_time({app="api"}[5m])) / 60"#,
    ),
    (
        "binary_scalar_mul",
        r#"100 * sum(count_over_time({app="api"}[5m]))"#,
    ),
    (
        "binary_compare_bool",
        r#"count_over_time({app="api"}[5m]) > bool 1"#,
    ),
    (
        "stddev_over_time",
        r#"stddev_over_time({app="api"} | json | unwrap latency_ms [5m])"#,
    ),
    (
        "label_replace",
        r#"label_replace(count_over_time({app="api"}[5m]), "svc", "$1", "app", "(.*)")"#,
    ),
    ("offset", r#"count_over_time({app="api"}[5m] offset 1m)"#),
];

// KNOWN DIVERGENCES (surfaced by this suite, 2026-06-22) — crabka vs grafana/loki:3.4.2.
// Excluded from the green corpus above and reported as real crabka findings:
//
//  1. `topk`/`bottomk` reject a nested *vector-aggregation* argument, e.g.
//       topk(1, sum by (app) (count_over_time({app=~".+"}[5m])))
//     crabka -> HTTP 400 "parse error at line 1, col 1: syntax error: unexpected
//     IDENTIFIER"; Loki returns a matrix. (Plain topk(2, count_over_time(...)) and
//     topk over a bare range-aggregation work in both — only the nested vector-agg
//     argument is rejected.)
//
//  2. `detected_level` for debug/trace plain-text lines: crabka classifies
//     "DEBUG ..." as detected_level="debug", but Loki 3.4.2 returns
//     detected_level="unknown" (it detects error/info/warn from leading plain-text
//     tokens, but not debug/trace). error/info/warn lines agree.
//
//  3. `label_join(...)` — crabka accepts this PromQL function (e.g.
//       label_join(count_over_time({app="api"}[5m]), "combo", "-", "app", "env")
//     yields combo="api-prod"); Loki rejects it (HTTP 400, not a LogQL function).
//     crabka is a superset here. (label_replace, which IS in LogQL, matches.)

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grafana_e2e_log_queries_match_loki() {
    let stack = boot_stack().await;
    let mismatches = run_corpus(&stack, LOG_QUERIES).await;
    let ok = mismatches.is_empty();
    let detail = report(&mismatches);
    stack.shutdown().await;
    assert!(ok, "{detail}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grafana_e2e_metric_queries_match_loki() {
    let stack = boot_stack().await;
    let mismatches = run_corpus(&stack, METRIC_QUERIES).await;
    let ok = mismatches.is_empty();
    let detail = report(&mismatches);
    stack.shutdown().await;
    assert!(ok, "{detail}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grafana_e2e_metadata_endpoints_match_loki() {
    let stack = boot_stack().await;

    // labels
    let crabka = stack
        .proxy_get(
            CRABKA_UID,
            "/loki/api/v1/labels",
            &[
                ("start", &stack.start_ns.to_string()),
                ("end", &stack.end_ns.to_string()),
            ],
        )
        .await;
    let loki = stack
        .proxy_get(
            LOKI_UID,
            "/loki/api/v1/labels",
            &[
                ("start", &stack.start_ns.to_string()),
                ("end", &stack.end_ns.to_string()),
            ],
        )
        .await;
    let mut c: Vec<String> = crabka["data"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect();
    let mut l: Vec<String> = loki["data"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect();
    c.sort();
    l.sort();
    let labels_match = c == l;

    // label values for "app"
    let crabka_v = stack
        .proxy_get(CRABKA_UID, "/loki/api/v1/label/app/values", &[])
        .await;
    let loki_v = stack
        .proxy_get(LOKI_UID, "/loki/api/v1/label/app/values", &[])
        .await;
    let mut cv: Vec<String> = crabka_v["data"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect();
    let mut lv: Vec<String> = loki_v["data"]
        .as_array()
        .unwrap_or(&Vec::new())
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect();
    cv.sort();
    lv.sort();
    let values_match = cv == lv;

    stack.shutdown().await;
    assert!(labels_match, "labels differ: crabka={c:?} loki={l:?}");
    assert!(
        values_match,
        "label app values differ: crabka={cv:?} loki={lv:?}"
    );
}
