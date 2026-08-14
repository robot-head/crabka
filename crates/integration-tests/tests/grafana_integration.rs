//! Grafana integration coverage for Crabka's Loki-compatible logs API.
//!
//! The test provisions Grafana's built-in Loki datasource against a real Crabka
//! querier listener. It then queries through Grafana's datasource proxy and
//! through the backend datasource execution path.

use std::{
    net::SocketAddr,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::Client;
use crabka_observability::{
    QuerierIndexSource, Role, ServiceConfig, ServiceDependencies, build_service_dependencies,
    build_service_router, run_compactor_until_idle, serve_service_listener,
};
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use serde_json::{Value, json};
use tempfile::TempDir;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{Host, IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::net::TcpListener;
use tower::ServiceExt;

const GRAFANA_PORT: u16 = 3000;
const GRAFANA_USER: &str = "admin";
const GRAFANA_PASSWORD: &str = "admin";
const HOST_ALIAS: &str = "host.testcontainers.internal";
/// The deadline for a container to start, which includes the image pull.
///
/// `AsyncRunner::start` waits for the pull with no bound of its own. A stalled
/// pull thus holds the test process open until the CI job wall stops it, and
/// the job log then names no test as the cause.
const CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(2);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grafana_loki_datasource_queries_crabka_querier_proxy() {
    let (broker, bootstrap, _broker_dir) = boot_crabka().await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");
    let object_store_url = format!("file://{}", object_root.path().display());
    let topic = "__crabka_observability_grafana_loki";
    let index_prefix = "observability/logs";
    create_topic(&bootstrap, topic).await;
    broker.wait_until_partition_present(topic, 0).await;

    let base_ns = current_unix_second_ns() - 60_000_000_000;
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api",
                    "env": "prod"
                },
                "values": [
                    [base_ns.to_string(), "api grafana datasource ok"],
                    [(base_ns + 1_000_000_000).to_string(), "api grafana datasource error"]
                ]
            }
        ]
    });

    let distributor_config = service_config(Role::Distributor, &bootstrap, topic, &data_root);
    let distributor = build_service_router(
        &distributor_config,
        build_service_dependencies(&distributor_config)
            .await
            .unwrap(),
        None,
    )
    .await
    .unwrap();
    push_crabka_payload(distributor, &payload).await;

    let mut compactor_config = service_config(Role::Compactor, &bootstrap, topic, &data_root);
    compactor_config.object_store_url = Some(object_store_url.clone());
    compactor_config.index_prefix = Some(index_prefix.to_string());
    compactor_config.wal_group_id = "grafana-loki-compactor".to_string();
    let descriptors = run_compactor_until_idle(
        &compactor_config,
        build_service_dependencies(&compactor_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    assert2::assert!(descriptors.len() == 1);

    let querier_listener = TcpListener::bind(("0.0.0.0", 0)).await.unwrap();
    let querier_port = querier_listener.local_addr().unwrap().port();
    let querier_addr = SocketAddr::from(([127, 0, 0, 1], querier_port));
    let querier = spawn_querier(
        querier_listener,
        &bootstrap,
        topic,
        &data_root,
        object_store_url,
        index_prefix,
    );
    let http = reqwest::Client::new();
    wait_for_crabka_ready(&http, querier_addr).await;

    let grafana = tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        GenericImage::new("mirror.gcr.io/grafana/grafana", "11.5.2")
            .with_exposed_port(GRAFANA_PORT.tcp())
            .with_wait_for(WaitFor::seconds(5))
            .with_env_var("GF_SECURITY_ADMIN_USER", GRAFANA_USER)
            .with_env_var("GF_SECURITY_ADMIN_PASSWORD", GRAFANA_PASSWORD)
            .with_env_var("GF_AUTH_ANONYMOUS_ENABLED", "false")
            .with_host(HOST_ALIAS, Host::HostGateway)
            .start(),
    )
    .await
    .expect("Grafana container start timed out")
    .expect("start Grafana container");
    let grafana_base = format!(
        "http://127.0.0.1:{}",
        grafana
            .get_host_port_ipv4(GRAFANA_PORT)
            .await
            .expect("Grafana mapped port")
    );
    wait_for_grafana_ready(&http, &grafana_base).await;

    let datasource_uid = create_loki_datasource(
        &http,
        &grafana_base,
        &format!("http://{HOST_ALIAS}:{}", querier_addr.port()),
    )
    .await;
    assert_grafana_datasource_health(&http, &grafana_base, &datasource_uid, &grafana).await;

    let labels = grafana_proxy_loki_result(
        &http,
        &grafana_base,
        &datasource_uid,
        "labels",
        &[
            ("start", base_ns.to_string()),
            ("end", (base_ns + 2_000_000_000).to_string()),
        ],
    )
    .await;
    assert2::assert!(labels["data"] == json!(["app", "env", "service_name"]));

    let app_values = grafana_proxy_loki_result(
        &http,
        &grafana_base,
        &datasource_uid,
        "label/app/values",
        &[
            ("start", base_ns.to_string()),
            ("end", (base_ns + 2_000_000_000).to_string()),
        ],
    )
    .await;
    assert2::assert!(app_values["data"] == json!(["api"]));

    let series = grafana_proxy_loki_result(
        &http,
        &grafana_base,
        &datasource_uid,
        "series",
        &[
            ("match[]", r#"{app="api"}"#.to_string()),
            ("start", base_ns.to_string()),
            ("end", (base_ns + 2_000_000_000).to_string()),
        ],
    )
    .await;
    assert2::assert!(
        series["data"]
            == json!([
                {
                    "app": "api",
                    "env": "prod",
                    "service_name": "api"
                }
            ])
    );

    let result = grafana_proxy_query_range_result(
        &http,
        &grafana_base,
        &datasource_uid,
        r#"{app="api",env="prod"} |= "error""#,
        base_ns,
        base_ns + 2_000_000_000,
    )
    .await;

    assert2::assert!(
        result["data"]["result"]
            == json!([
                {
                    "stream": {
                        "app": "api",
                        "detected_level": "error",
                        "env": "prod",
                        "service_name": "api"
                    },
                    "values": [[(base_ns + 1_000_000_000).to_string(), "api grafana datasource error"]]
                }
            ])
    );

    let datasource_query = grafana_backend_loki_query_result(
        &http,
        &grafana_base,
        &datasource_uid,
        r#"{app="api",env="prod"} |= "error""#,
        base_ns,
        base_ns + 2_000_000_000,
    )
    .await;
    assert2::assert!(json_contains_string(
        &datasource_query,
        "api grafana datasource error"
    ));

    querier.abort();
    broker.shutdown().await;
}

async fn boot_crabka() -> (BrokerHandle, String, TempDir) {
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
        .client_id("grafana-integration-admin")
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
    assert2::assert!(response.topics[0].error_code == 0);
}

fn service_config(role: Role, bootstrap: &str, topic: &str, data_root: &TempDir) -> ServiceConfig {
    ServiceConfig {
        target: role,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: Some(bootstrap.to_string()),
        wal_topic: topic.to_string(),
        wal_group_id: format!("grafana-loki-{topic}-{role:?}"),
        data_root: data_root.path().to_path_buf(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_length: None,
        max_ingest_body: None,
        wal_append_timeout: None,
        ..ServiceConfig::default()
    }
}

fn spawn_querier(
    listener: TcpListener,
    bootstrap: &str,
    topic: &str,
    data_root: &TempDir,
    object_store_url: String,
    index_prefix: &str,
) -> tokio::task::JoinHandle<()> {
    let mut config = service_config(Role::Querier, bootstrap, topic, data_root);
    config.object_store_url = Some(object_store_url);
    config.index_prefix = Some(index_prefix.to_string());
    config.querier_index_source = QuerierIndexSource::TenantObjectStoreShards;
    config.wal_group_id = "grafana-loki-querier".to_string();

    tokio::spawn(async move {
        serve_service_listener(listener, config, ServiceDependencies::default(), None)
            .await
            .unwrap();
    })
}

async fn push_crabka_payload(app: axum::Router, payload: &Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert2::assert!(response.status() == StatusCode::NO_CONTENT);
}

async fn wait_for_grafana_ready(http: &reqwest::Client, base: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let health_ok = http
            .get(format!("{base}/api/health"))
            .send()
            .await
            .is_ok_and(|response| response.status() == reqwest::StatusCode::OK);
        let api_ok = http
            .get(format!("{base}/api/org"))
            .basic_auth(GRAFANA_USER, Some(GRAFANA_PASSWORD))
            .send()
            .await
            .is_ok_and(|response| response.status() == reqwest::StatusCode::OK);
        if health_ok && api_ok {
            return;
        }
        assert2::assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_crabka_ready(http: &reqwest::Client, addr: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(response) = http.get(format!("http://{addr}/ready")).send().await
            && response.status() == reqwest::StatusCode::OK
        {
            return;
        }
        assert2::assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn create_loki_datasource(
    http: &reqwest::Client,
    grafana_base: &str,
    loki_url: &str,
) -> String {
    let response = http
        .post(format!("{grafana_base}/api/datasources"))
        .basic_auth(GRAFANA_USER, Some(GRAFANA_PASSWORD))
        .json(&json!({
            "name": "Crabka Loki",
            "type": "loki",
            "access": "proxy",
            "url": loki_url,
            "jsonData": {
                "httpHeaderName1": "X-Scope-OrgID"
            },
            "secureJsonData": {
                "httpHeaderValue1": "tenant-a"
            }
        }))
        .send()
        .await
        .expect("create Grafana datasource");
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert2::assert!(status.is_success());
    let body: Value = serde_json::from_str(&body).expect("datasource JSON");
    body.pointer("/datasource/uid")
        .or_else(|| body.get("uid"))
        .and_then(Value::as_str)
        .expect("datasource uid")
        .to_string()
}

async fn assert_grafana_datasource_health(
    http: &reqwest::Client,
    grafana_base: &str,
    datasource_uid: &str,
    grafana: &ContainerAsync<GenericImage>,
) {
    let response = http
        .get(format!(
            "{grafana_base}/api/datasources/uid/{datasource_uid}/health"
        ))
        .basic_auth(GRAFANA_USER, Some(GRAFANA_PASSWORD))
        .send()
        .await
        .expect("Grafana datasource health");
    let status = response.status();
    let _body = response.text().await.unwrap_or_default();
    let _stdout =
        String::from_utf8_lossy(&grafana.stdout_to_vec().await.unwrap_or_default()).into_owned();
    let _stderr =
        String::from_utf8_lossy(&grafana.stderr_to_vec().await.unwrap_or_default()).into_owned();
    assert2::assert!(status.is_success());
}

async fn grafana_proxy_query_range_result(
    http: &reqwest::Client,
    grafana_base: &str,
    datasource_uid: &str,
    query: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let response = http
        .get(format!(
            "{grafana_base}/api/datasources/proxy/uid/{datasource_uid}/loki/api/v1/query_range"
        ))
        .basic_auth(GRAFANA_USER, Some(GRAFANA_PASSWORD))
        .query(&[
            ("query", query.to_string()),
            ("start", start_ns.to_string()),
            ("end", end_ns.to_string()),
            ("direction", "forward".to_string()),
        ])
        .send()
        .await
        .expect("Grafana proxy query_range");
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert2::assert!(status.is_success());
    serde_json::from_str(&body).expect("Grafana proxy query JSON")
}

async fn grafana_proxy_loki_result(
    http: &reqwest::Client,
    grafana_base: &str,
    datasource_uid: &str,
    path: &str,
    query: &[(&str, String)],
) -> Value {
    let response = http
        .get(format!(
            "{grafana_base}/api/datasources/proxy/uid/{datasource_uid}/loki/api/v1/{path}"
        ))
        .basic_auth(GRAFANA_USER, Some(GRAFANA_PASSWORD))
        .query(query)
        .send()
        .await
        .expect("Grafana proxy Loki metadata query");
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert2::assert!(status.is_success());
    serde_json::from_str(&body).expect("Grafana proxy Loki metadata JSON")
}

async fn grafana_backend_loki_query_result(
    http: &reqwest::Client,
    grafana_base: &str,
    datasource_uid: &str,
    query: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let response = http
        .post(format!("{grafana_base}/api/ds/query"))
        .basic_auth(GRAFANA_USER, Some(GRAFANA_PASSWORD))
        .json(&json!({
            "from": unix_nanos_to_grafana_millis(start_ns).to_string(),
            "to": unix_nanos_to_grafana_millis(end_ns).to_string(),
            "queries": [
                {
                    "refId": "A",
                    "datasource": {
                        "type": "loki",
                        "uid": datasource_uid
                    },
                    "expr": query,
                    "queryType": "range",
                    "direction": "forward",
                    "maxLines": 1000,
                    "intervalMs": 1000,
                    "maxDataPoints": 1000
                }
            ]
        }))
        .send()
        .await
        .expect("Grafana backend datasource query");
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert2::assert!(status.is_success());
    serde_json::from_str(&body).expect("Grafana backend query JSON")
}

fn unix_nanos_to_grafana_millis(timestamp_ns: i64) -> i64 {
    timestamp_ns / 1_000_000
}

fn json_contains_string(value: &Value, needle: &str) -> bool {
    match value {
        Value::String(value) => value.contains(needle),
        Value::Array(values) => values
            .iter()
            .any(|value| json_contains_string(value, needle)),
        Value::Object(values) => values
            .values()
            .any(|value| json_contains_string(value, needle)),
        _ => false,
    }
}

fn current_unix_second_ns() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs();
    i64::try_from(now).expect("unix seconds fit in i64") * 1_000_000_000
}
