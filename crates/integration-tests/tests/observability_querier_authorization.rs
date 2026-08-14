//! Querier query-path availability once broker-backed authorization connects.
//!
//! The querier serves queries through an authorizer slot that starts out
//! unavailable and is swapped for a broker-backed authorizer by a background
//! connect task. Every query takes a read lock on that slot, so the swap must
//! not leave a writer parked on it: doing so wedges the whole query path for
//! the life of the service, and a wedged query never returns rather than
//! failing, which turns one bad service into an unbounded CI stall.
//!
//! These tests need no Docker; they run an in-process broker plus the
//! distributor -> compactor -> querier path.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::Client;
use crabka_observability::{
    QuerierIndexSource, Role, ServiceConfig, build_service_dependencies, build_service_router,
    run_compactor_until_idle,
};
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use serde_json::{Value, json};
use tempfile::TempDir;
use tower::ServiceExt;

const TENANT: &str = "tenant-a";
/// Per-request bound. A wedged query path never returns, so an unbounded
/// request here would hang the test run instead of failing it.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// How long to keep retrying while the background authorizer connect lands.
const CONNECT_DEADLINE: Duration = Duration::from_secs(60);

async fn boot_broker() -> (BrokerHandle, String, TempDir) {
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
        .client_id("querier-authorization-admin")
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
        wal_group_id: format!("querier-authorization-{topic}-{role:?}"),
        data_root: data_root.path().to_path_buf(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        ..ServiceConfig::default()
    }
}

fn current_unix_second_ns() -> i64 {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock");
    i64::try_from(now.as_secs()).expect("seconds fit") * 1_000_000_000
}

fn percent_encode_component(value: &str) -> String {
    let mut out = String::new();
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(char::from(*byte));
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Issues `query`, returning its status, or `None` when the request did not
/// return within [`REQUEST_TIMEOUT`] (i.e. the query path is wedged).
async fn query_status(
    app: axum::Router,
    query: &str,
    start_ns: i64,
    end_ns: i64,
) -> Option<StatusCode> {
    let uri = format!(
        "/loki/api/v1/query_range?query={}&start={start_ns}&end={end_ns}&step={}",
        percent_encode_component(query),
        percent_encode_component("1s")
    );
    let request = Request::builder()
        .uri(uri)
        .header("X-Scope-OrgID", TENANT)
        .body(Body::empty())
        .unwrap();
    tokio::time::timeout(REQUEST_TIMEOUT, app.oneshot(request))
        .await
        .ok()
        .map(|response| response.unwrap().status())
}

/// Boots broker -> distributor -> compactor -> querier and returns the querier
/// router alongside the broker handle and the seeded time range.
async fn boot_querier(topic: &str) -> (BrokerHandle, axum::Router, i64, i64, Vec<TempDir>) {
    let (broker, bootstrap, broker_dir) = boot_broker().await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");
    let object_store_url = format!("file://{}", object_root.path().display());
    let index_prefix = "observability/logs";
    create_topic(&bootstrap, topic).await;
    broker.wait_until_partition_present(topic, 0).await;

    let base_ns = current_unix_second_ns() - 60_000_000_000;
    let payload: Value = json!({
        "streams": [
            {
                "stream": { "app": "api", "env": "prod" },
                "values": [
                    [base_ns.to_string(), "aa"],
                    [(base_ns + 1_000_000_000).to_string(), "bbb"]
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
    let response = distributor
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("content-type", "application/json")
                .header("X-Scope-OrgID", TENANT)
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert2::assert!(response.status() == StatusCode::NO_CONTENT);

    let mut compactor_config = service_config(Role::Compactor, &bootstrap, topic, &data_root);
    compactor_config.object_store_url = Some(object_store_url.clone());
    compactor_config.index_prefix = Some(index_prefix.to_string());
    compactor_config.wal_group_id = format!("{topic}-compactor");
    run_compactor_until_idle(
        &compactor_config,
        build_service_dependencies(&compactor_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    let mut querier_config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    querier_config.object_store_url = Some(object_store_url);
    querier_config.index_prefix = Some(index_prefix.to_string());
    querier_config.querier_index_source = QuerierIndexSource::TenantObjectStoreShards;
    querier_config.wal_group_id = format!("{topic}-querier");
    let querier = build_service_router(
        &querier_config,
        build_service_dependencies(&querier_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    let end_ns = base_ns + 2_000_000_000;
    (
        broker,
        querier,
        base_ns,
        end_ns,
        vec![broker_dir, data_root, object_root],
    )
}

/// Regression: the querier kept serving after the background connect task
/// swapped in the broker-backed authorizer. Holding that slot's write guard
/// across the task's shutdown await wedged every subsequent query forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn querier_serves_queries_after_authorization_connects() {
    let (broker, querier, start_ns, end_ns, _dirs) =
        boot_querier("__crabka_observability_authorization_serves").await;

    // Retry while the background connect lands: it reports 503/500 until the
    // broker-backed authorizer is installed, and 200 once it is. A `None` here
    // is the regression — the request never came back at all.
    let deadline = Instant::now() + CONNECT_DEADLINE;
    let mut last = None;
    let mut served = false;
    while Instant::now() < deadline {
        let status = query_status(
            querier.clone(),
            r#"{app="api",env="prod"}"#,
            start_ns,
            end_ns,
        )
        .await;
        assert2::assert!(
            status.is_some(),
            "query never returned within {REQUEST_TIMEOUT:?}: the querier's authorizer slot is wedged"
        );
        last = status;
        if status == Some(StatusCode::OK) {
            served = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert2::assert!(served, "querier never served a query; last status {last:?}");

    broker.shutdown().await;
}

/// The same guarantee for metric queries, which take the identical authorizer
/// read path before any range-vector evaluation runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn querier_serves_metric_queries_after_authorization_connects() {
    let (broker, querier, start_ns, end_ns, _dirs) =
        boot_querier("__crabka_observability_authorization_metric").await;

    let deadline = Instant::now() + CONNECT_DEADLINE;
    let mut served = false;
    while Instant::now() < deadline {
        let status = query_status(
            querier.clone(),
            r#"count_over_time({app="api",env="prod"} [2s])"#,
            start_ns,
            end_ns,
        )
        .await;
        assert2::assert!(
            status.is_some(),
            "metric query never returned within {REQUEST_TIMEOUT:?}: the querier's authorizer slot is wedged"
        );
        if status == Some(StatusCode::OK) {
            served = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert2::assert!(served, "querier never served a metric query");

    broker.shutdown().await;
}
