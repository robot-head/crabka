//! Differential Loki compatibility coverage.
//!
//! These tests run a real Loki container beside in-process Crabka services,
//! ingest the same Loki push payload into both, and compare the stable query
//! result shape that Grafana's built-in Loki datasource consumes.

use std::collections::BTreeMap;
use std::io::Write;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use assert2::assert;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use crabka_blockstore::{BlockIndex, BlockKey, LabelIndex, LogRow, TimeRange, write_log_block};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::Client;
use crabka_observability::{
    InMemoryWalSink, QuerierIndexSource, QuerierState, Role, ServiceConfig, ServiceDependencies,
    build_service_dependencies, build_service_router, distributor_router, loki_router,
    run_compactor_until_idle,
};
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use flate2::Compression;
use flate2::write::DeflateEncoder;
use serde_json::{Value, json};
use tempfile::TempDir;
use testcontainers::GenericImage;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use tokio::net::TcpListener;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tower::ServiceExt;

const LOKI_PORT: u16 = 3100;

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
        .client_id("loki-differential-admin")
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

fn service_config(role: Role, bootstrap: &str, topic: &str, data_root: &TempDir) -> ServiceConfig {
    ServiceConfig {
        target: role,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: Some(bootstrap.to_string()),
        wal_topic: topic.to_string(),
        wal_group_id: format!("loki-differential-{topic}-{role:?}"),
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

fn distributor_router_for_status() -> axum::Router {
    distributor_router(InMemoryWalSink::default())
}

async fn compactor_router_for_status() -> axum::Router {
    let config = ServiceConfig {
        target: Role::Compactor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "loki-differential-status-compactor".to_string(),
        data_root: TempDir::new().expect("compactor status root").keep(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: Some("observability/logs".to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap()
}

fn labels<const N: usize>(pairs: [(&str, &str); N]) -> BTreeMap<String, String> {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_buildinfo_shape() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    let loki_result = loki_buildinfo_result(&http, &loki_base).await;
    let crabka_result = crabka_buildinfo_result(querier).await;

    assert!(crabka_result == loki_result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_basic_status_probe_shapes() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    for path in ["/ready", "/log_level", "/memberlist"] {
        let loki_result = loki_status_probe_result(&http, &loki_base, path).await;
        let crabka_result = crabka_status_probe_result(querier.clone(), path).await;

        assert!(crabka_result == loki_result);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_services_status_shape() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    let loki_result = loki_status_probe_result(&http, &loki_base, "/services").await;
    let crabka_result = crabka_status_probe_result(querier, "/services").await;

    assert!(crabka_result == loki_result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_stable_config_status_lines() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    let loki_result = loki_config_result(&http, &loki_base).await;
    let crabka_result = crabka_config_result(querier.clone()).await;

    assert!(crabka_result == loki_result);

    let loki_diff_result = loki_config_result_with_query(&http, &loki_base, "mode=diff").await;
    let crabka_diff_result = crabka_config_result_with_query(querier.clone(), "mode=diff").await;

    assert!(crabka_diff_result == loki_diff_result);

    let loki_defaults_result =
        loki_config_result_with_query(&http, &loki_base, "mode=defaults").await;
    let crabka_defaults_result = crabka_config_result_with_query(querier, "mode=defaults").await;

    assert!(crabka_defaults_result == loki_defaults_result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_expose_same_stable_metrics_families() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    let loki_result = loki_metrics_result(&http, &loki_base).await;
    let crabka_result = crabka_metrics_result(querier).await;

    assert!(crabka_result == loki_result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_log_level_post_shapes() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    let query_param_loki_result =
        loki_log_level_post_result(&http, &loki_base, Some("log_level=debug"), None).await;
    let query_param_crabka_result =
        crabka_log_level_post_result(querier.clone(), Some("log_level=debug"), None).await;
    assert!(query_param_crabka_result == query_param_loki_result);

    let form_loki_result =
        loki_log_level_post_result(&http, &loki_base, None, Some("log_level=warn")).await;
    let form_crabka_result =
        crabka_log_level_post_result(querier.clone(), None, Some("log_level=warn")).await;
    assert!(form_crabka_result == form_loki_result);

    let mixed_loki_result = loki_log_level_post_result(
        &http,
        &loki_base,
        Some("log_level=debug"),
        Some("log_level=warn"),
    )
    .await;
    let mixed_crabka_result =
        crabka_log_level_post_result(querier, Some("log_level=debug"), Some("log_level=warn"))
            .await;
    assert!(mixed_crabka_result == mixed_loki_result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_log_level_post_error_shapes() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    for (raw_query, form_body) in [
        (Some("log_level=trace"), None),
        (Some("log_level="), None),
        (None, Some("")),
    ] {
        let loki_result = loki_log_level_post_result(&http, &loki_base, raw_query, form_body).await;
        let crabka_result =
            crabka_log_level_post_result(querier.clone(), raw_query, form_body).await;
        assert!(crabka_result == loki_result);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_ingester_control_shapes() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let crabka = distributor_router_for_status();

    for (method, path) in [
        ("POST", "/flush"),
        ("GET", "/ingester/prepare_shutdown"),
        ("POST", "/ingester/prepare_shutdown"),
        ("GET", "/ingester/prepare_shutdown"),
        ("DELETE", "/ingester/prepare_shutdown"),
        ("GET", "/ingester/prepare_shutdown"),
        (
            "GET",
            "/ingester/shutdown?flush=false&delete_ring_tokens=false&terminate=false",
        ),
    ] {
        let loki_result = loki_ingester_control_result(&http, &loki_base, method, path).await;
        let crabka_result = crabka_ingester_control_result(crabka.clone(), method, path).await;

        assert!(crabka_result == loki_result);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_empty_ruler_inventory_shape() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    for path in [
        "/loki/api/v1/rules",
        "/loki/api/v1/rules/default",
        "/loki/api/v1/rules/default/api-errors",
        "/prometheus/api/v1/rules",
        "/prometheus/api/v1/alerts",
        "/api/prom/rules",
        "/api/prom/alerts",
        "/api/prom/rules/default",
        "/api/prom/rules/default/api-errors",
    ] {
        let loki_result = loki_ruler_inventory_result(&http, &loki_base, path).await;
        let crabka_result = crabka_ruler_inventory_result(querier.clone(), path).await;

        assert!(crabka_result == loki_result);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_ring_status_page_shapes() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let compactor = compactor_router_for_status().await;
    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    for (path, app) in [
        ("/ring", querier.clone()),
        ("/distributor/ring", distributor),
        ("/compactor/ring", compactor),
        ("/scheduler/ring", querier.clone()),
        ("/ruler/ring", querier),
    ] {
        let loki_result = loki_ring_status_result(&http, &loki_base, path).await;
        let crabka_result = crabka_ring_status_result(app, path).await;

        assert!(crabka_result == loki_result);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_default_delete_api_is_absent_while_crabka_serves_lifecycle() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let crabka = compactor_router_for_status().await;
    let query = r#"{app="api"} |= "secret""#;
    let start = 1_591_616_227;
    let end = 1_591_619_692;

    let loki_result = loki_delete_lifecycle_result(&http, &loki_base, query, start, end).await;
    assert!(
        loki_result
            == json!({
                "create": delete_not_found_response(),
                "listAfterCreate": delete_not_found_response(),
                "cancel": json!({
                    "httpStatus": 0,
                    "contentType": "",
                    "body": "<missing-request-id>",
                }),
                "listAfterCancel": delete_not_found_response(),
            })
    );

    let crabka_result = crabka_delete_lifecycle_result(crabka, query, start, end).await;
    assert!(
        crabka_result
            == json!({
                "create": {
                    "httpStatus": 204,
                    "contentType": "",
                    "body": "",
                },
                "listAfterCreate": {
                    "httpStatus": 200,
                    "contentType": "application/json",
                    "body": [
                        {
                            "request_id": "<request-id>",
                            "start_time": start,
                            "end_time": end,
                            "query": query,
                            "status": "received",
                            "created_at": "<created-at>",
                        }
                    ],
                },
                "cancel": {
                    "httpStatus": 204,
                    "contentType": "",
                    "body": "",
                },
                "listAfterCancel": {
                    "httpStatus": 200,
                    "contentType": "application/json",
                    "body": [],
                },
            })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_stream_query_range_result() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _broker_dir) = boot_crabka().await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");
    let object_store_url = format!("file://{}", object_root.path().display());
    let topic = "__crabka_observability_loki_differential";
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
                    [base_ns.to_string(), "api loki differential ok"],
                    [(base_ns + 1_000_000_000).to_string(), "api loki differential error"]
                ]
            }
        ]
    });

    push_loki_payload(&http, &loki_base, &payload).await;

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
    compactor_config.wal_group_id = "loki-differential-compactor".to_string();
    let descriptors = run_compactor_until_idle(
        &compactor_config,
        build_service_dependencies(&compactor_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    assert!(descriptors.len() == 1);

    let mut querier_config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    querier_config.object_store_url = Some(object_store_url);
    querier_config.index_prefix = Some(index_prefix.to_string());
    querier_config.tenant = Some("tenant-a".to_string());
    querier_config.querier_index_source = QuerierIndexSource::TenantObjectStoreManifest;
    querier_config.wal_group_id = "loki-differential-querier".to_string();
    let querier = build_service_router(
        &querier_config,
        build_service_dependencies(&querier_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    let query = r#"{app="api",env="prod"} |= "error""#;
    let loki_result =
        loki_query_range_result(&http, &loki_base, query, base_ns, base_ns + 2_000_000_000).await;
    let crabka_result =
        crabka_query_range_result(querier.clone(), query, base_ns, base_ns + 2_000_000_000).await;

    assert!(crabka_result == loki_result);

    let loki_alias_result = loki_api_prom_query_range_result(
        &http,
        &loki_base,
        query,
        base_ns,
        base_ns + 2_000_000_000,
    )
    .await;
    let crabka_alias_result = crabka_api_prom_query_range_result(
        querier.clone(),
        query,
        base_ns,
        base_ns + 2_000_000_000,
    )
    .await;

    assert!(
        loki_alias_result
            == json!({
                "httpStatus": 404,
                "body": "404 page not found\n",
            })
    );
    assert!(crabka_alias_result == loki_result);

    let query = r#"{app="api",env="prod"}"#;
    let loki_result = loki_query_range_result_with_default_direction_and_limit(
        &http,
        &loki_base,
        query,
        base_ns,
        base_ns + 2_000_000_000,
        1,
    )
    .await;
    let crabka_result = crabka_query_range_result_with_default_direction_and_limit(
        querier,
        query,
        base_ns,
        base_ns + 2_000_000_000,
        1,
    )
    .await;

    assert!(crabka_result == loki_result);
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_matcher_and_line_filter_results() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _broker_dir) = boot_crabka().await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");
    let object_store_url = format!("file://{}", object_root.path().display());
    let topic = "__crabka_observability_loki_filter_differential";
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
                    [base_ns.to_string(), "api differential info"],
                    [(base_ns + 1_000_000_000).to_string(), "api differential error"],
                    [(base_ns + 2_000_000_000).to_string(), "api differential warn"],
                    [(base_ns + 3_000_000_000).to_string(), "api differential debug error"]
                ]
            },
            {
                "stream": {
                    "app": "worker",
                    "env": "dev"
                },
                "values": [
                    [(base_ns + 4_000_000_000).to_string(), "worker differential error"]
                ]
            }
        ]
    });

    push_loki_payload(&http, &loki_base, &payload).await;

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
    compactor_config.wal_group_id = "loki-filter-differential-compactor".to_string();
    let descriptors = run_compactor_until_idle(
        &compactor_config,
        build_service_dependencies(&compactor_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    assert!(descriptors.len() == 1);

    let mut querier_config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    querier_config.object_store_url = Some(object_store_url);
    querier_config.index_prefix = Some(index_prefix.to_string());
    querier_config.tenant = Some("tenant-a".to_string());
    querier_config.querier_index_source = QuerierIndexSource::TenantObjectStoreManifest;
    querier_config.wal_group_id = "loki-filter-differential-querier".to_string();
    let querier = build_service_router(
        &querier_config,
        build_service_dependencies(&querier_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    let query =
        r#"{app=~"api|worker",env!="dev"} |= "differential" != "debug" |~ "error|warn" !~ "warn""#;
    let end_ns = base_ns + 5_000_000_000;
    let loki_result = loki_query_range_result(&http, &loki_base, query, base_ns, end_ns).await;
    let crabka_result = crabka_query_range_result(querier.clone(), query, base_ns, end_ns).await;

    assert!(crabka_result == loki_result);

    let query = r#"{app=~"api|worker"} | env = "prod" |= "differential error""#;
    let loki_result = loki_query_range_result(&http, &loki_base, query, base_ns, end_ns).await;
    let crabka_result = crabka_query_range_result(querier, query, base_ns, end_ns).await;

    assert!(crabka_result == loki_result);
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_metric_query_range_result() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _broker_dir) = boot_crabka().await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");
    let object_store_url = format!("file://{}", object_root.path().display());
    let topic = "__crabka_observability_loki_metric_differential";
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
                    [base_ns.to_string(), "api loki differential ok"],
                    [(base_ns + 1_000_000_000).to_string(), "api loki differential error one"],
                    [(base_ns + 2_000_000_000).to_string(), "api loki differential error two"]
                ]
            }
        ]
    });

    push_loki_payload(&http, &loki_base, &payload).await;

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
    compactor_config.wal_group_id = "loki-metric-differential-compactor".to_string();
    let descriptors = run_compactor_until_idle(
        &compactor_config,
        build_service_dependencies(&compactor_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    assert!(descriptors.len() == 1);

    let mut querier_config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    querier_config.object_store_url = Some(object_store_url);
    querier_config.index_prefix = Some(index_prefix.to_string());
    querier_config.tenant = Some("tenant-a".to_string());
    querier_config.querier_index_source = QuerierIndexSource::TenantObjectStoreManifest;
    querier_config.wal_group_id = "loki-metric-differential-querier".to_string();
    let querier = build_service_router(
        &querier_config,
        build_service_dependencies(&querier_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    let query = r#"count_over_time({app="api",env="prod"} |= "error" [2s])"#;
    let end_ns = base_ns + 3_000_000_000;
    let loki_result =
        loki_query_range_result_with_step(&http, &loki_base, query, base_ns, end_ns, "1s").await;
    let crabka_result =
        crabka_query_range_result_with_step(querier.clone(), query, base_ns, end_ns, "1s").await;

    assert!(crabka_result == loki_result);

    let loki_alias_result =
        loki_api_prom_query_range_result(&http, &loki_base, query, base_ns, end_ns).await;
    let crabka_alias_result =
        crabka_api_prom_query_range_result(querier.clone(), query, base_ns, end_ns).await;

    assert!(
        loki_alias_result
            == json!({
                "httpStatus": 404,
                "body": "404 page not found\n",
            })
    );
    assert!(
        crabka_alias_result
            == json!({
                "httpStatus": 400,
                "body": "rpc error: code = Code(400) desc = legacy endpoints only support streams result type",
            })
    );

    let query = r#"rate({app="api",env="prod"} |= "error" [2s])"#;
    let loki_result =
        loki_query_range_result_with_step(&http, &loki_base, query, base_ns, end_ns, "1s").await;
    let crabka_result =
        crabka_query_range_result_with_step(querier.clone(), query, base_ns, end_ns, "1s").await;

    assert!(crabka_result == loki_result);

    let query = r#"count_over_time({app="api",env="prod"} |= "error" [2s]) + on() vector(1)"#;
    let loki_result =
        loki_query_range_result_with_step(&http, &loki_base, query, base_ns, end_ns, "1s").await;
    let crabka_result =
        crabka_query_range_result_with_step(querier.clone(), query, base_ns, end_ns, "1s").await;

    assert!(crabka_result == loki_result);

    let query = r#"count_over_time({app="api",env="prod"} |= "error" [2s]) > bool on() vector(0)"#;
    let loki_result =
        loki_query_range_result_with_step(&http, &loki_base, query, base_ns, end_ns, "1s").await;
    let crabka_result =
        crabka_query_range_result_with_step(querier.clone(), query, base_ns, end_ns, "1s").await;

    assert!(crabka_result == loki_result);

    let query = r#"count_over_time({app="api",env="prod"} |= "error" [2s]) and on() vector(1)"#;
    let loki_result =
        loki_query_range_result_with_step(&http, &loki_base, query, base_ns, end_ns, "1s").await;
    let crabka_result =
        crabka_query_range_result_with_step(querier.clone(), query, base_ns, end_ns, "1s").await;

    assert!(crabka_result == loki_result);

    let query = r#"vector(1) + on() group_right(app, env) count_over_time({app="api",env="prod"} |= "error" [2s])"#;
    let loki_result =
        loki_query_range_result_with_step(&http, &loki_base, query, base_ns, end_ns, "1s").await;
    let crabka_result =
        crabka_query_range_result_with_step(querier.clone(), query, base_ns, end_ns, "1s").await;

    assert!(crabka_result == loki_result);

    let query = r#"vector(1) or on() count_over_time({app="api",env="prod"} |= "error" [2s])"#;
    let loki_result =
        loki_query_range_result_with_step(&http, &loki_base, query, base_ns, end_ns, "1s").await;
    let crabka_result =
        crabka_query_range_result_with_step(querier.clone(), query, base_ns, end_ns, "1s").await;

    assert!(crabka_result == loki_result);

    let query = r#"absent_over_time({app="missing",env="prod"} [2s])"#;
    let loki_result =
        loki_query_range_result_with_step(&http, &loki_base, query, base_ns, end_ns, "1s").await;
    let crabka_result =
        crabka_query_range_result_with_step(querier, query, base_ns, end_ns, "1s").await;

    assert!(crabka_result == loki_result);

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_vector_aggregation_result() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _broker_dir) = boot_crabka().await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");
    let object_store_url = format!("file://{}", object_root.path().display());
    let topic = "__crabka_observability_loki_vector_aggregation_differential";
    let index_prefix = "observability/logs";
    create_topic(&bootstrap, topic).await;
    broker.wait_until_partition_present(topic, 0).await;

    let base_ns = current_unix_second_ns() - 60_000_000_000;
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api",
                    "env": "prod",
                    "pod": "a"
                },
                "values": [
                    [base_ns.to_string(), "api vector differential error one"],
                    [(base_ns + 2_000_000_000).to_string(), "api vector differential error three"]
                ]
            },
            {
                "stream": {
                    "app": "api",
                    "env": "prod",
                    "pod": "b"
                },
                "values": [
                    [(base_ns + 1_000_000_000).to_string(), "api vector differential error two"]
                ]
            },
            {
                "stream": {
                    "app": "api",
                    "env": "dev",
                    "pod": "c"
                },
                "values": [
                    [(base_ns + 1_000_000_000).to_string(), "api vector differential error dev"]
                ]
            }
        ]
    });

    push_loki_payload(&http, &loki_base, &payload).await;

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
    compactor_config.wal_group_id = "loki-vector-aggregation-differential-compactor".to_string();
    let descriptors = run_compactor_until_idle(
        &compactor_config,
        build_service_dependencies(&compactor_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    assert!(descriptors.len() == 1);

    let mut querier_config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    querier_config.object_store_url = Some(object_store_url);
    querier_config.index_prefix = Some(index_prefix.to_string());
    querier_config.tenant = Some("tenant-a".to_string());
    querier_config.querier_index_source = QuerierIndexSource::TenantObjectStoreManifest;
    querier_config.wal_group_id = "loki-vector-aggregation-differential-querier".to_string();
    let querier = build_service_router(
        &querier_config,
        build_service_dependencies(&querier_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    let end_ns = base_ns + 4_000_000_000;
    for query in [
        r#"sum by (env) (count_over_time({app="api"} |= "vector differential error" [3s]))"#,
        r#"count without (pod) (count_over_time({app="api"} |= "vector differential error" [3s]))"#,
        r#"min by (env) (bytes_over_time({app="api"} |= "vector differential" [3s]))"#,
        r#"max by (env) (bytes_over_time({app="api"} |= "vector differential" [3s]))"#,
        r#"avg without (pod) (bytes_over_time({app="api",env="prod"} |= "vector differential" [3s]))"#,
        r#"stddev by (env) (count_over_time({app="api"} |= "vector differential error" [3s]))"#,
        r#"stdvar without (pod) (count_over_time({app="api"} |= "vector differential error" [3s]))"#,
        r#"topk by (env) (2, count_over_time({app="api"} |= "vector differential error" [3s]))"#,
        r#"bottomk(2, count_over_time({app="api"} |= "vector differential error" [3s])) without (pod)"#,
        r#"sort(count_over_time({app="api"} |= "vector differential error" [3s]))"#,
        r#"sort_desc(count_over_time({app="api"} |= "vector differential error" [3s]))"#,
    ] {
        let loki_result =
            loki_query_range_result_with_step(&http, &loki_base, query, base_ns, end_ns, "1s")
                .await;
        let crabka_result =
            crabka_query_range_result_with_step(querier.clone(), query, base_ns, end_ns, "1s")
                .await;

        assert!(crabka_result == loki_result);
    }
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_byte_metric_results() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _broker_dir) = boot_crabka().await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");
    let object_store_url = format!("file://{}", object_root.path().display());
    let topic = "__crabka_observability_loki_byte_metric_differential";
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
                    [base_ns.to_string(), "aa"],
                    [(base_ns + 1_000_000_000).to_string(), "bbb"],
                    [(base_ns + 2_000_000_000).to_string(), "cccc"]
                ]
            }
        ]
    });

    push_loki_payload(&http, &loki_base, &payload).await;

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
    compactor_config.wal_group_id = "loki-byte-metric-differential-compactor".to_string();
    let descriptors = run_compactor_until_idle(
        &compactor_config,
        build_service_dependencies(&compactor_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    assert!(descriptors.len() == 1);

    let mut querier_config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    querier_config.object_store_url = Some(object_store_url);
    querier_config.index_prefix = Some(index_prefix.to_string());
    querier_config.tenant = Some("tenant-a".to_string());
    querier_config.querier_index_source = QuerierIndexSource::TenantObjectStoreManifest;
    querier_config.wal_group_id = "loki-byte-metric-differential-querier".to_string();
    let querier = build_service_router(
        &querier_config,
        build_service_dependencies(&querier_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    let end_ns = base_ns + 3_000_000_000;
    let query = r#"bytes_over_time({app="api",env="prod"} [2s])"#;
    let loki_result =
        loki_query_range_result_with_step(&http, &loki_base, query, base_ns, end_ns, "1s").await;
    let crabka_result =
        crabka_query_range_result_with_step(querier.clone(), query, base_ns, end_ns, "1s").await;
    assert!(crabka_result == loki_result);

    let query = r#"bytes_rate({app="api",env="prod"} [2s])"#;
    let loki_result =
        loki_query_range_result_with_step(&http, &loki_base, query, base_ns, end_ns, "1s").await;
    let crabka_result =
        crabka_query_range_result_with_step(querier, query, base_ns, end_ns, "1s").await;
    assert!(crabka_result == loki_result);

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_metadata_results() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _broker_dir) = boot_crabka().await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");
    let object_store_url = format!("file://{}", object_root.path().display());
    let topic = "__crabka_observability_loki_metadata_differential";
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
                    [base_ns.to_string(), "api loki metadata ok"],
                    [(base_ns + 1_000_000_000).to_string(), "api loki metadata error"]
                ]
            },
            {
                "stream": {
                    "app": "worker",
                    "env": "prod"
                },
                "values": [
                    [(base_ns + 1_500_000_000).to_string(), "worker loki metadata ok"]
                ]
            }
        ]
    });

    push_loki_payload(&http, &loki_base, &payload).await;

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
    compactor_config.wal_group_id = "loki-metadata-differential-compactor".to_string();
    let descriptors = run_compactor_until_idle(
        &compactor_config,
        build_service_dependencies(&compactor_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    assert!(descriptors.len() == 1);

    let mut querier_config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    querier_config.object_store_url = Some(object_store_url);
    querier_config.index_prefix = Some(index_prefix.to_string());
    querier_config.tenant = Some("tenant-a".to_string());
    querier_config.querier_index_source = QuerierIndexSource::TenantObjectStoreManifest;
    querier_config.wal_group_id = "loki-metadata-differential-querier".to_string();
    let querier = build_service_router(
        &querier_config,
        build_service_dependencies(&querier_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    let end_ns = base_ns + 2_000_000_000;
    let loki_labels = loki_metadata_result(&http, &loki_base, "labels", base_ns, end_ns).await;
    let crabka_labels = crabka_metadata_result(querier.clone(), "labels", base_ns, end_ns).await;
    assert!(crabka_labels == loki_labels);

    let loki_singular_labels =
        loki_metadata_result(&http, &loki_base, "label", base_ns, end_ns).await;
    let crabka_singular_labels =
        crabka_metadata_result(querier.clone(), "label", base_ns, end_ns).await;
    assert!(crabka_singular_labels == loki_singular_labels);

    let loki_alias_labels =
        loki_api_prom_metadata_result(&http, &loki_base, "label", base_ns, end_ns).await;
    let crabka_alias_labels =
        crabka_api_prom_metadata_result(querier.clone(), "label", base_ns, end_ns).await;
    assert!(crabka_alias_labels == loki_alias_labels);

    let loki_app_values =
        loki_metadata_result(&http, &loki_base, "label/app/values", base_ns, end_ns).await;
    let crabka_app_values =
        crabka_metadata_result(querier.clone(), "label/app/values", base_ns, end_ns).await;
    assert!(crabka_app_values == loki_app_values);

    let loki_alias_app_values =
        loki_api_prom_metadata_result(&http, &loki_base, "label/app/values", base_ns, end_ns).await;
    let crabka_alias_app_values =
        crabka_api_prom_metadata_result(querier.clone(), "label/app/values", base_ns, end_ns).await;
    assert!(loki_alias_app_values == loki_alias_labels);
    assert!(crabka_alias_app_values == loki_alias_app_values);

    let detected_labels_path = "detected_labels?query=%7Bapp%3D%22api%22%7D&limit=10";
    let loki_detected_labels =
        loki_detected_labels_result(&http, &loki_base, detected_labels_path, base_ns, end_ns).await;
    let crabka_detected_labels =
        crabka_detected_labels_result(querier.clone(), detected_labels_path, base_ns, end_ns).await;
    assert!(crabka_detected_labels == loki_detected_labels);

    let all_detected_labels_path = "detected_labels?limit=10";
    let loki_all_detected_labels =
        loki_detected_labels_result(&http, &loki_base, all_detected_labels_path, base_ns, end_ns)
            .await;
    let crabka_all_detected_labels =
        crabka_detected_labels_result(querier.clone(), all_detected_labels_path, base_ns, end_ns)
            .await;
    assert!(crabka_all_detected_labels == loki_all_detected_labels);

    let lenient_detected_labels_path =
        "detected_labels?query=%7Bapp%3D%22api%22%7D&step=not-a-number&limit=not-a-limit";
    let loki_lenient_detected_labels = loki_detected_labels_result(
        &http,
        &loki_base,
        lenient_detected_labels_path,
        base_ns,
        end_ns,
    )
    .await;
    let crabka_lenient_detected_labels = crabka_detected_labels_result(
        querier.clone(),
        lenient_detected_labels_path,
        base_ns,
        end_ns,
    )
    .await;
    assert!(crabka_lenient_detected_labels == loki_lenient_detected_labels);

    let series_path = "series?match%5B%5D=%7Bapp%3D%22api%22%7D";
    let loki_series = loki_metadata_result(&http, &loki_base, series_path, base_ns, end_ns).await;
    let crabka_series = crabka_metadata_result(querier.clone(), series_path, base_ns, end_ns).await;
    assert!(crabka_series == loki_series);

    let loki_alias_series =
        loki_api_prom_metadata_result(&http, &loki_base, series_path, base_ns, end_ns).await;
    let crabka_alias_series =
        crabka_api_prom_metadata_result(querier.clone(), series_path, base_ns, end_ns).await;
    assert!(crabka_alias_series == loki_alias_series);

    let worker_series_path = "series?match%5B%5D=%7Bapp%3D%22worker%22%7D";
    let loki_post_series =
        loki_metadata_post_result(&http, &loki_base, worker_series_path, None, base_ns, end_ns)
            .await;
    let crabka_post_series =
        crabka_metadata_post_result(querier.clone(), worker_series_path, None, base_ns, end_ns)
            .await;
    assert!(crabka_post_series == loki_post_series);

    let form_series_path = "series";
    let form_series_body = "match%5B%5D=%7Benv%3D%22prod%22%7D";
    let form_series_start = base_ns + 1_000_000_001;
    let loki_form_post_series = loki_metadata_post_result(
        &http,
        &loki_base,
        form_series_path,
        Some(form_series_body),
        form_series_start,
        end_ns,
    )
    .await;
    let crabka_form_post_series = crabka_metadata_post_result(
        querier,
        form_series_path,
        Some(form_series_body),
        form_series_start,
        end_ns,
    )
    .await;
    assert!(crabka_form_post_series == loki_form_post_series);

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_empty_metadata_shapes() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    for path in [
        "/loki/api/v1/labels",
        "/loki/api/v1/label",
        "/loki/api/v1/label/app/values",
        "/api/prom/label",
        "/api/prom/label/app/values",
        "/loki/api/v1/detected_labels?limit=10",
        "/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&limit=10",
        "/loki/api/v1/detected_field/status/values?query=%7Bapp%3D%22api%22%7D&limit=10",
    ] {
        let loki_result = loki_json_path_result(&http, &loki_base, path).await;
        let crabka_result = crabka_json_path_result(querier.clone(), path).await;

        assert!(crabka_result == loki_result, "{path}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_detected_fields_results() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let base_ns = current_unix_second_ns() - 60_000_000_000;
    let json_line = r#"{"status":500,"ok":false,"path":"/checkout"}"#;
    let logfmt_line = "level=warn duration=12ms bytes=1.5MiB status=503";
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api",
                    "env": "prod"
                },
                "values": [
                    [base_ns.to_string(), json_line],
                    [(base_ns + 1_000_000_000).to_string(), logfmt_line]
                ]
            }
        ]
    });
    push_loki_payload(&http, &loki_base, &payload).await;

    let block = write_log_block(
        dir.path(),
        &BlockKey::new(
            "tenant-a",
            0,
            base_ns,
            base_ns + 1_000_000_000,
            TimeRange::new(base_ns, base_ns + 1_000_000_000).unwrap(),
        ),
        vec![
            LogRow::new(api, base_ns, json_line, BTreeMap::new()),
            LogRow::new(api, base_ns + 1_000_000_000, logfmt_line, BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(block);
    let querier = loki_router(QuerierState::new(dir.path(), label_index, block_index));

    let end_ns = base_ns + 3_000_000_000;
    let fields_path = "detected_fields?query=%7Bapp%3D%22api%22%7D&limit=10";
    let loki_fields =
        loki_detected_fields_result(&http, &loki_base, fields_path, base_ns, end_ns).await;
    let crabka_fields =
        crabka_detected_fields_result(querier.clone(), fields_path, base_ns, end_ns).await;
    assert!(crabka_fields == loki_fields);

    let values_path = "detected_field/status/values?query=%7Bapp%3D%22api%22%7D&limit=10";
    let loki_values =
        loki_detected_fields_result(&http, &loki_base, values_path, base_ns, end_ns).await;
    let crabka_values = crabka_detected_fields_result(querier, values_path, base_ns, end_ns).await;
    assert!(crabka_values == loki_values);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_default_patterns_endpoint_is_unavailable_while_crabka_serves_patterns() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    let base_ns = current_unix_second_ns() - 60_000_000_000;
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api",
                    "env": "prod"
                },
                "values": [
                    [base_ns.to_string(), "status=500 user=100 route=/checkout"],
                    [(base_ns + 1_000_000_000).to_string(), "status=200 user=200 route=/checkout"]
                ]
            },
            {
                "stream": {
                    "app": "worker",
                    "env": "prod"
                },
                "values": [
                    [(base_ns + 2_000_000_000).to_string(), "status=503 user=300 route=/ignored"]
                ]
            }
        ]
    });
    push_loki_payload(&http, &loki_base, &payload).await;

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new(
            "tenant-a",
            0,
            base_ns,
            base_ns + 1_000_000_000,
            TimeRange::new(base_ns, base_ns + 1_000_000_000).unwrap(),
        ),
        vec![
            LogRow::new(
                api,
                base_ns,
                "status=500 user=100 route=/checkout",
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                base_ns + 1_000_000_000,
                "status=200 user=200 route=/checkout",
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();
    let worker_block = write_log_block(
        dir.path(),
        &BlockKey::new(
            "tenant-a",
            1,
            base_ns + 2_000_000_000,
            base_ns + 2_000_000_000,
            TimeRange::new(base_ns + 2_000_000_000, base_ns + 2_000_000_000).unwrap(),
        ),
        vec![LogRow::new(
            worker,
            base_ns + 2_000_000_000,
            "status=503 user=300 route=/ignored",
            BTreeMap::new(),
        )],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    block_index.insert(worker_block);
    let querier = loki_router(QuerierState::new(dir.path(), label_index, block_index));

    let end_ns = base_ns + 4_000_000_000;
    let patterns_path = "patterns?query=%7Bapp%3D%22api%22%7D&step=1s";
    let loki_patterns =
        loki_patterns_default_response(&http, &loki_base, patterns_path, base_ns, end_ns).await;
    assert!(
        loki_patterns
            == json!({
                "httpStatus": 404,
                "body": "",
            })
    );

    let crabka_patterns = crabka_patterns_result(querier, patterns_path, base_ns, end_ns).await;
    assert!(
        crabka_patterns
            == json!({
                "httpStatus": 200,
                "body": {
                    "status": "success",
                    "data": [
                        {
                            "pattern": "status=<_> user=<_> route=/checkout",
                            "samples": [
                                ["<timestamp>", 1],
                                ["<timestamp>", 1]
                            ]
                        }
                    ]
                }
            })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_index_volume_shape() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    let base_ns = current_unix_second_ns() - 60_000_000_000;
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api",
                    "env": "prod"
                },
                "values": [
                    [base_ns.to_string(), "api volume one"],
                    [(base_ns + 1_000_000_000).to_string(), "api volume two"]
                ]
            },
            {
                "stream": {
                    "app": "worker",
                    "env": "prod"
                },
                "values": [
                    [(base_ns + 2_000_000_000).to_string(), "worker volume one"]
                ]
            }
        ]
    });
    push_loki_payload(&http, &loki_base, &payload).await;

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new(
            "tenant-a",
            0,
            base_ns,
            base_ns + 1_000_000_000,
            TimeRange::new(base_ns, base_ns + 1_000_000_000).unwrap(),
        ),
        vec![
            LogRow::new(api, base_ns, "api volume one", BTreeMap::new()),
            LogRow::new(
                api,
                base_ns + 1_000_000_000,
                "api volume two",
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();
    let worker_block = write_log_block(
        dir.path(),
        &BlockKey::new(
            "tenant-a",
            1,
            base_ns + 2_000_000_000,
            base_ns + 2_000_000_000,
            TimeRange::new(base_ns + 2_000_000_000, base_ns + 2_000_000_000).unwrap(),
        ),
        vec![LogRow::new(
            worker,
            base_ns + 2_000_000_000,
            "worker volume one",
            BTreeMap::new(),
        )],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    block_index.insert(worker_block);
    let querier = loki_router(QuerierState::new(dir.path(), label_index, block_index));

    let end_ns = base_ns + 4_000_000_000;
    let volume_path = "index/volume?query=%7Benv%3D%22prod%22%7D&targetLabels=app,env";
    let loki_volume =
        loki_index_volume_result(&http, &loki_base, volume_path, base_ns, end_ns).await;
    let crabka_volume = crabka_index_volume_result(querier, volume_path, base_ns, end_ns).await;
    assert!(crabka_volume == loki_volume);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_index_stats_shape() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    let base_ns = current_unix_second_ns() - 60_000_000_000;
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api",
                    "env": "prod"
                },
                "values": [
                    [base_ns.to_string(), "api stats one"],
                    [(base_ns + 1_000_000_000).to_string(), "api stats two"]
                ]
            },
            {
                "stream": {
                    "app": "worker",
                    "env": "prod"
                },
                "values": [
                    [(base_ns + 2_000_000_000).to_string(), "worker stats one"]
                ]
            }
        ]
    });
    push_loki_payload(&http, &loki_base, &payload).await;

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new(
            "tenant-a",
            0,
            base_ns,
            base_ns + 1_000_000_000,
            TimeRange::new(base_ns, base_ns + 1_000_000_000).unwrap(),
        ),
        vec![
            LogRow::new(api, base_ns, "api stats one", BTreeMap::new()),
            LogRow::new(
                api,
                base_ns + 1_000_000_000,
                "api stats two",
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();
    let worker_block = write_log_block(
        dir.path(),
        &BlockKey::new(
            "tenant-a",
            1,
            base_ns + 2_000_000_000,
            base_ns + 2_000_000_000,
            TimeRange::new(base_ns + 2_000_000_000, base_ns + 2_000_000_000).unwrap(),
        ),
        vec![LogRow::new(
            worker,
            base_ns + 2_000_000_000,
            "worker stats one",
            BTreeMap::new(),
        )],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    block_index.insert(worker_block);
    let querier = loki_router(QuerierState::new(dir.path(), label_index, block_index));

    let end_ns = base_ns + 4_000_000_000;
    let stats_path = "index/stats?query=%7Benv%3D%22prod%22%7D";
    let loki_stats = loki_index_stats_result(&http, &loki_base, stats_path, base_ns, end_ns).await;
    let crabka_stats = crabka_index_stats_result(querier, stats_path, base_ns, end_ns).await;
    assert!(crabka_stats == loki_stats);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_index_volume_range_shape() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    let base_ns = current_unix_second_ns() - 60_000_000_000;
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api",
                    "env": "prod"
                },
                "values": [
                    [base_ns.to_string(), "api range volume one"],
                    [(base_ns + 1_000_000_000).to_string(), "api range volume two"]
                ]
            },
            {
                "stream": {
                    "app": "worker",
                    "env": "prod"
                },
                "values": [
                    [(base_ns + 2_000_000_000).to_string(), "worker range volume one"],
                    [(base_ns + 3_000_000_000).to_string(), "worker range volume two"]
                ]
            }
        ]
    });
    push_loki_payload(&http, &loki_base, &payload).await;

    let api_block = write_log_block(
        dir.path(),
        &BlockKey::new(
            "tenant-a",
            0,
            base_ns,
            base_ns + 1_000_000_000,
            TimeRange::new(base_ns, base_ns + 1_000_000_000).unwrap(),
        ),
        vec![
            LogRow::new(api, base_ns, "api range volume one", BTreeMap::new()),
            LogRow::new(
                api,
                base_ns + 1_000_000_000,
                "api range volume two",
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();
    let worker_block = write_log_block(
        dir.path(),
        &BlockKey::new(
            "tenant-a",
            1,
            base_ns + 2_000_000_000,
            base_ns + 3_000_000_000,
            TimeRange::new(base_ns + 2_000_000_000, base_ns + 3_000_000_000).unwrap(),
        ),
        vec![
            LogRow::new(
                worker,
                base_ns + 2_000_000_000,
                "worker range volume one",
                BTreeMap::new(),
            ),
            LogRow::new(
                worker,
                base_ns + 3_000_000_000,
                "worker range volume two",
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    block_index.insert(worker_block);
    let querier = loki_router(QuerierState::new(dir.path(), label_index, block_index));

    let end_ns = base_ns + 4_000_000_000;
    let volume_path =
        "index/volume_range?query=%7Benv%3D%22prod%22%7D&targetLabels=app,env&step=1s";
    let loki_volume =
        loki_index_volume_result(&http, &loki_base, volume_path, base_ns, end_ns).await;
    let crabka_volume = crabka_index_volume_result(querier, volume_path, base_ns, end_ns).await;
    assert!(crabka_volume == loki_volume);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_parser_filter_results() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _broker_dir) = boot_crabka().await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");
    let object_store_url = format!("file://{}", object_root.path().display());
    let topic = "__crabka_observability_loki_parser_differential";
    let index_prefix = "observability/logs";
    create_topic(&bootstrap, topic).await;
    broker.wait_until_partition_present(topic, 0).await;

    let base_ns = current_unix_second_ns() - 60_000_000_000;
    let json_error_line = r#"{"request":{"method":"GET"},"response":{"status":500}}"#;
    let logfmt_error_line = r#"status=500 msg="api parser error""#;
    let logfmt_template_line = r#"raw=" /checkout/ " path=/api/items msg="template helper""#;
    let logfmt_spacing_template_line = r#"short=hi long=hello-world mark=x msg="spacing helper""#;
    let logfmt_typed_filter_line = r#"duration=25ms bytes_consumed=21MB msg="api typed parser ok""#;
    let logfmt_ip_line = r#"client=10.2.3.4 msg="api ip filter ok""#;
    let logfmt_ip_miss_line = r#"client=192.168.2.3 msg="api ip filter miss""#;
    let colored_logfmt_error_line = "\u{1b}[31mstatus=503 msg=\"colored parser error\"\u{1b}[0m";
    let decolored_logfmt_error_line = r#"status=503 msg="colored parser error""#;
    let packed_error_line =
        r#"{"container":"myapp","pod":"pod-3223f","_entry":"original log message"}"#;
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api",
                    "env": "prod",
                    "format": "json"
                },
                "values": [
                    [base_ns.to_string(), r#"{"request":{"method":"GET"},"response":{"status":200}}"#],
                    [(base_ns + 1_000_000_000).to_string(), json_error_line]
                ]
            },
            {
                "stream": {
                    "app": "api",
                    "env": "prod",
                    "format": "logfmt"
                },
                "values": [
                    [(base_ns + 2_000_000_000).to_string(), r#"status=200 msg="api parser ok""#],
                    [(base_ns + 3_000_000_000).to_string(), logfmt_error_line],
                    [(base_ns + 6_000_000_000).to_string(), r#"duration=10ms bytes_consumed=21MB msg="api typed parser too fast""#],
                    [(base_ns + 7_000_000_000).to_string(), r#"duration=25ms bytes_consumed=19MB msg="api typed parser too small""#],
                    [(base_ns + 8_000_000_000).to_string(), logfmt_typed_filter_line],
                    [(base_ns + 9_000_000_000).to_string(), colored_logfmt_error_line],
                    [(base_ns + 10_000_000_000).to_string(), logfmt_template_line],
                    [(base_ns + 11_000_000_000).to_string(), logfmt_spacing_template_line],
                    [(base_ns + 12_000_000_000).to_string(), logfmt_ip_line],
                    [(base_ns + 13_000_000_000).to_string(), logfmt_ip_miss_line]
                ]
            },
            {
                "stream": {
                    "app": "api",
                    "env": "prod",
                    "format": "metadata"
                },
                "values": [
                    [(base_ns + 4_000_000_000).to_string(), "api metadata ok", {"trace_id": "abc", "status": "200"}],
                    [(base_ns + 5_000_000_000).to_string(), "api metadata miss", {"trace_id": "def", "status": "500"}]
                ]
            },
            {
                "stream": {
                    "app": "api",
                    "env": "prod",
                    "format": "packed"
                },
                "values": [
                    [(base_ns + 9_000_000_000).to_string(), packed_error_line],
                    [(base_ns + 10_000_000_000).to_string(), r#"{"container":"myapp","pod":"pod-3223f","_entry":"container original log message"}"#]
                ]
            }
        ]
    });

    push_loki_payload(&http, &loki_base, &payload).await;

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
    compactor_config.wal_group_id = "loki-parser-differential-compactor".to_string();
    let descriptors = run_compactor_until_idle(
        &compactor_config,
        build_service_dependencies(&compactor_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    assert!(descriptors.len() == 1);

    let mut querier_config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    querier_config.object_store_url = Some(object_store_url);
    querier_config.index_prefix = Some(index_prefix.to_string());
    querier_config.tenant = Some("tenant-a".to_string());
    querier_config.querier_index_source = QuerierIndexSource::TenantObjectStoreManifest;
    querier_config.wal_group_id = "loki-parser-differential-querier".to_string();
    let querier = build_service_router(
        &querier_config,
        build_service_dependencies(&querier_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    let end_ns = base_ns + 14_000_000_000;
    let json_query =
        r#"{app="api",format="json"} | json | request_method = "GET" | response_status >= 500"#;
    let loki_json_result =
        loki_query_range_result(&http, &loki_base, json_query, base_ns, end_ns).await;
    let crabka_json_result =
        crabka_query_range_result(querier.clone(), json_query, base_ns, end_ns).await;
    assert!(crabka_json_result == loki_json_result);

    let selected_json_query = r#"{app="api",format="json"} | json method="request.method", status_code="response.status" | status_code >= 500"#;
    let loki_selected_json_result =
        loki_query_range_result(&http, &loki_base, selected_json_query, base_ns, end_ns).await;
    let crabka_selected_json_result =
        crabka_query_range_result(querier.clone(), selected_json_query, base_ns, end_ns).await;
    assert!(crabka_selected_json_result == loki_selected_json_result);

    let logfmt_query = r#"{app="api",format="logfmt"} | logfmt | status >= 500"#;
    let loki_logfmt_result =
        loki_query_range_result(&http, &loki_base, logfmt_query, base_ns, end_ns).await;
    let crabka_logfmt_result =
        crabka_query_range_result(querier.clone(), logfmt_query, base_ns, end_ns).await;
    assert!(crabka_logfmt_result == loki_logfmt_result);

    let parameterized_logfmt_query =
        r#"{app="api",format="logfmt"} | logfmt status, message="msg" | status >= 500"#;
    let loki_parameterized_logfmt_result = loki_query_range_result(
        &http,
        &loki_base,
        parameterized_logfmt_query,
        base_ns,
        end_ns,
    )
    .await;
    let crabka_parameterized_logfmt_result =
        crabka_query_range_result(querier.clone(), parameterized_logfmt_query, base_ns, end_ns)
            .await;
    assert!(crabka_parameterized_logfmt_result == loki_parameterized_logfmt_result);

    let logfmt_or_query =
        r#"{app="api",format="logfmt"} | logfmt | status >= 500 or msg = "api parser ok""#;
    let loki_logfmt_or_result =
        loki_query_range_result(&http, &loki_base, logfmt_or_query, base_ns, end_ns).await;
    let crabka_logfmt_or_result =
        crabka_query_range_result(querier.clone(), logfmt_or_query, base_ns, end_ns).await;
    assert!(crabka_logfmt_or_result == loki_logfmt_or_result);

    let logfmt_comma_and_query =
        r#"{app="api",format="logfmt"} | logfmt | status >= 500, msg = "api parser error""#;
    let loki_logfmt_comma_and_result =
        loki_query_range_result(&http, &loki_base, logfmt_comma_and_query, base_ns, end_ns).await;
    let crabka_logfmt_comma_and_result =
        crabka_query_range_result(querier.clone(), logfmt_comma_and_query, base_ns, end_ns).await;
    assert!(crabka_logfmt_comma_and_result == loki_logfmt_comma_and_result);

    let logfmt_adjacent_and_query =
        r#"{app="api",format="logfmt"} | logfmt | status >= 500 msg = "api parser error""#;
    let loki_logfmt_adjacent_and_result = loki_query_range_result(
        &http,
        &loki_base,
        logfmt_adjacent_and_query,
        base_ns,
        end_ns,
    )
    .await;
    let crabka_logfmt_adjacent_and_result =
        crabka_query_range_result(querier.clone(), logfmt_adjacent_and_query, base_ns, end_ns)
            .await;
    assert!(crabka_logfmt_adjacent_and_result == loki_logfmt_adjacent_and_result);

    let backtick_field_filter_query =
        r#"{app="api",format="logfmt"} | logfmt | msg = `api parser error`"#;
    let loki_backtick_field_filter_result = loki_query_range_result(
        &http,
        &loki_base,
        backtick_field_filter_query,
        base_ns,
        end_ns,
    )
    .await;
    let crabka_backtick_field_filter_result = crabka_query_range_result(
        querier.clone(),
        backtick_field_filter_query,
        base_ns,
        end_ns,
    )
    .await;
    assert!(crabka_backtick_field_filter_result == loki_backtick_field_filter_result);

    let line_format_query = r#"{app="api",format="logfmt"} | logfmt | line_format `{{.msg}} {{.status}}` |= "api parser error 500""#;
    let loki_line_format_result =
        loki_query_range_result(&http, &loki_base, line_format_query, base_ns, end_ns).await;
    let crabka_line_format_result =
        crabka_query_range_result(querier.clone(), line_format_query, base_ns, end_ns).await;
    assert!(crabka_line_format_result == loki_line_format_result);

    let line_format_pipeline_query = r#"{app="api",format="logfmt"} | logfmt | line_format `{{ .msg | replace " " "_" | upper }} {{.status}}` |= "API_PARSER_ERROR 500""#;
    let loki_line_format_pipeline_result = loki_query_range_result(
        &http,
        &loki_base,
        line_format_pipeline_query,
        base_ns,
        end_ns,
    )
    .await;
    let crabka_line_format_pipeline_result =
        crabka_query_range_result(querier.clone(), line_format_pipeline_query, base_ns, end_ns)
            .await;
    assert!(crabka_line_format_pipeline_result == loki_line_format_pipeline_result);

    let line_format_with_present_query = r#"{app="api",format="logfmt"} | logfmt | line_format `{{ with .raw }}raw={{ . }}{{ else }}missing{{ end }}` |= "raw= /checkout/ ""#;
    let loki_line_format_with_present_result = loki_query_range_result(
        &http,
        &loki_base,
        line_format_with_present_query,
        base_ns,
        end_ns,
    )
    .await;
    let crabka_line_format_with_present_result = crabka_query_range_result(
        querier.clone(),
        line_format_with_present_query,
        base_ns,
        end_ns,
    )
    .await;
    assert!(crabka_line_format_with_present_result == loki_line_format_with_present_result);

    let line_format_trim_marker_query = r#"{app="api",format="logfmt"} | logfmt | line_format `left {{- .msg -}} right` |= "leftapi parser errorright""#;
    let loki_line_format_trim_marker_result = loki_query_range_result(
        &http,
        &loki_base,
        line_format_trim_marker_query,
        base_ns,
        end_ns,
    )
    .await;
    let crabka_line_format_trim_marker_result = crabka_query_range_result(
        querier.clone(),
        line_format_trim_marker_query,
        base_ns,
        end_ns,
    )
    .await;
    assert!(crabka_line_format_trim_marker_result == loki_line_format_trim_marker_result);

    let line_format_comment_query = r#"{app="api",format="logfmt"} | logfmt | line_format `before{{/* hidden */}}after {{ .msg }}` |= "beforeafter api parser error""#;
    let loki_line_format_comment_result = loki_query_range_result(
        &http,
        &loki_base,
        line_format_comment_query,
        base_ns,
        end_ns,
    )
    .await;
    let crabka_line_format_comment_result =
        crabka_query_range_result(querier.clone(), line_format_comment_query, base_ns, end_ns)
            .await;
    assert!(crabka_line_format_comment_result == loki_line_format_comment_result);

    let label_format_query = r#"{app="api",format="logfmt"} | logfmt | label_format namespace=env, summary="{{.msg}} {{.status}}" | namespace = "prod" | summary = "api parser error 500""#;
    let loki_label_format_result =
        loki_query_range_result(&http, &loki_base, label_format_query, base_ns, end_ns).await;
    let crabka_label_format_result =
        crabka_query_range_result(querier.clone(), label_format_query, base_ns, end_ns).await;
    assert!(crabka_label_format_result == loki_label_format_result);

    let label_format_pipeline_query = r#"{app="api",format="logfmt"} | logfmt | label_format summary=`{{ .msg | replace " " "_" | upper }}` | summary = "API_PARSER_ERROR""#;
    let loki_label_format_pipeline_result = loki_query_range_result(
        &http,
        &loki_base,
        label_format_pipeline_query,
        base_ns,
        end_ns,
    )
    .await;
    let crabka_label_format_pipeline_result = crabka_query_range_result(
        querier.clone(),
        label_format_pipeline_query,
        base_ns,
        end_ns,
    )
    .await;
    assert!(crabka_label_format_pipeline_result == loki_label_format_pipeline_result);

    let line_format_string_helper_query = r#"{app="api",format="logfmt"} | logfmt | line_format `{{ .raw | trim | trimPrefix "/" | trimSuffix "/" | title }} {{ .raw | trimAll " /" }} {{ .path | substr 1 10 }} {{ .path | substr 5 -1 }} {{ .path | substr -1 4 }}` |= "Checkout checkout api/items items /api""#;
    let loki_line_format_string_helper_result = loki_query_range_result(
        &http,
        &loki_base,
        line_format_string_helper_query,
        base_ns,
        end_ns,
    )
    .await;
    let crabka_line_format_string_helper_result = crabka_query_range_result(
        querier.clone(),
        line_format_string_helper_query,
        base_ns,
        end_ns,
    )
    .await;
    assert!(crabka_line_format_string_helper_result == loki_line_format_string_helper_result);

    let line_format_logical_helper_query = r#"{app="api",format="logfmt"} | logfmt | line_format `{{ contains "helper" .msg }} {{ .path | hasPrefix "/api" }} {{ .path | hasSuffix "items" }} {{ .msg | eq "template helper" }}` |= "true true true true""#;
    let loki_line_format_logical_helper_result = loki_query_range_result(
        &http,
        &loki_base,
        line_format_logical_helper_query,
        base_ns,
        end_ns,
    )
    .await;
    let crabka_line_format_logical_helper_result = crabka_query_range_result(
        querier.clone(),
        line_format_logical_helper_query,
        base_ns,
        end_ns,
    )
    .await;
    assert!(crabka_line_format_logical_helper_result == loki_line_format_logical_helper_result);

    let line_format_ne_helper_query = r#"{app="api",format="logfmt"} | logfmt | line_format `{{ ne .msg "api parser error" }} {{ .path | ne "/health" }}` |= "true true""#;
    let loki_line_format_ne_helper_result = loki_query_range_result(
        &http,
        &loki_base,
        line_format_ne_helper_query,
        base_ns,
        end_ns,
    )
    .await;
    let crabka_line_format_ne_helper_result = crabka_query_range_result(
        querier.clone(),
        line_format_ne_helper_query,
        base_ns,
        end_ns,
    )
    .await;
    assert!(crabka_line_format_ne_helper_result == loki_line_format_ne_helper_result);

    let line_format_len_helper_query =
        r#"{app="api",format="logfmt"} | logfmt | line_format `len={{ len .msg }}` |= "len=15""#;
    let loki_line_format_len_helper_result = loki_query_range_result(
        &http,
        &loki_base,
        line_format_len_helper_query,
        base_ns,
        end_ns,
    )
    .await;
    let crabka_line_format_len_helper_result = crabka_query_range_result(
        querier.clone(),
        line_format_len_helper_query,
        base_ns,
        end_ns,
    )
    .await;
    assert!(crabka_line_format_len_helper_result == loki_line_format_len_helper_result);

    let line_format_spacing_helper_query = r#"{app="api",format="logfmt"} | logfmt | line_format `{{ alignLeft 5 .short }}|{{ alignLeft 5 .long }}|{{ alignRight 5 .short }}|{{ alignRight 5 .long }}|{{ repeat 3 .mark }}` |= "hi   |hello|   hi|world|xxx""#;
    let loki_line_format_spacing_helper_result = loki_query_range_result(
        &http,
        &loki_base,
        line_format_spacing_helper_query,
        base_ns,
        end_ns,
    )
    .await;
    let crabka_line_format_spacing_helper_result = crabka_query_range_result(
        querier.clone(),
        line_format_spacing_helper_query,
        base_ns,
        end_ns,
    )
    .await;
    assert!(crabka_line_format_spacing_helper_result == loki_line_format_spacing_helper_result);

    let line_format_regex_helper_query = r#"{app="api",format="logfmt"} | logfmt | line_format `{{ count "e" .msg }}|{{ regexReplaceAll "(template) (helper)" .msg "${2}-${1}" }}|{{ .msg | regexReplaceAllLiteral "(template) (helper)" "${2}-${1}" }}` |= "4|helper-template|${2}-${1}""#;
    let loki_line_format_regex_helper_result = loki_query_range_result(
        &http,
        &loki_base,
        line_format_regex_helper_query,
        base_ns,
        end_ns,
    )
    .await;
    let crabka_line_format_regex_helper_result = crabka_query_range_result(
        querier.clone(),
        line_format_regex_helper_query,
        base_ns,
        end_ns,
    )
    .await;
    assert!(crabka_line_format_regex_helper_result == loki_line_format_regex_helper_result);

    let drop_keep_query = r#"{app="api",format="logfmt"} | logfmt | drop env, msg="api parser error" | keep app, format, status="500" | status = "500""#;
    let loki_drop_keep_result =
        loki_query_range_result(&http, &loki_base, drop_keep_query, base_ns, end_ns).await;
    let crabka_drop_keep_result =
        crabka_query_range_result(querier.clone(), drop_keep_query, base_ns, end_ns).await;
    assert!(crabka_drop_keep_result == loki_drop_keep_result);

    let decolorize_query =
        r#"{app="api",format="logfmt"} | decolorize | logfmt | msg = "colored parser error""#;
    let loki_decolorize_result =
        loki_query_range_result(&http, &loki_base, decolorize_query, base_ns, end_ns).await;
    let crabka_decolorize_result =
        crabka_query_range_result(querier.clone(), decolorize_query, base_ns, end_ns).await;
    assert!(crabka_decolorize_result == loki_decolorize_result);

    let pattern_query = r#"{app="api",format="logfmt"} |> `status=500 msg="api parser error"`"#;
    let loki_pattern_result =
        loki_query_range_result(&http, &loki_base, pattern_query, base_ns, end_ns).await;
    let crabka_pattern_result =
        crabka_query_range_result(querier.clone(), pattern_query, base_ns, end_ns).await;
    assert!(crabka_pattern_result == loki_pattern_result);

    let pattern_parser_query =
        r#"{app="api",format="logfmt"} | pattern `status=<status> msg="<msg>"` | status >= 500"#;
    let loki_pattern_parser_result =
        loki_query_range_result(&http, &loki_base, pattern_parser_query, base_ns, end_ns).await;
    let crabka_pattern_parser_result =
        crabka_query_range_result(querier.clone(), pattern_parser_query, base_ns, end_ns).await;
    assert!(crabka_pattern_parser_result == loki_pattern_parser_result);

    let regexp_parser_query = r#"{app="api",format="logfmt"} | regexp `status=(?P<status>\d+) msg="(?P<msg>.*)"` | status >= 500"#;
    let loki_regexp_parser_result =
        loki_query_range_result(&http, &loki_base, regexp_parser_query, base_ns, end_ns).await;
    let crabka_regexp_parser_result =
        crabka_query_range_result(querier.clone(), regexp_parser_query, base_ns, end_ns).await;
    assert!(crabka_regexp_parser_result == loki_regexp_parser_result);

    let unpack_parser_query =
        r#"{app="api",format="packed"} | unpack != "container" | pod = "pod-3223f""#;
    let loki_unpack_parser_result =
        loki_query_range_result(&http, &loki_base, unpack_parser_query, base_ns, end_ns).await;
    let crabka_unpack_parser_result =
        crabka_query_range_result(querier.clone(), unpack_parser_query, base_ns, end_ns).await;
    assert!(crabka_unpack_parser_result == loki_unpack_parser_result);

    let commented_query = r#"
        {app="api",format="logfmt"} # selector comment
        |= "parser error" # line filter comment
        | logfmt
        # disabled stage: | status = 200
        | status >= 500 # field filter comment
    "#;
    let loki_commented_result =
        loki_query_range_result(&http, &loki_base, commented_query, base_ns, end_ns).await;
    let crabka_commented_result =
        crabka_query_range_result(querier.clone(), commented_query, base_ns, end_ns).await;
    assert!(crabka_commented_result == loki_commented_result);

    let logfmt_typed_query =
        r#"{app="api",format="logfmt"} | logfmt | duration >= 20ms | bytes_consumed > 20MB"#;
    let loki_logfmt_typed_result =
        loki_query_range_result(&http, &loki_base, logfmt_typed_query, base_ns, end_ns).await;
    let crabka_logfmt_typed_result =
        crabka_query_range_result(querier.clone(), logfmt_typed_query, base_ns, end_ns).await;
    assert!(crabka_logfmt_typed_result == loki_logfmt_typed_result);

    let ip_filter_query = r#"{app="api",format="logfmt"} |= ip("10.0.0.0/8")"#;
    let loki_ip_filter_result =
        loki_query_range_result(&http, &loki_base, ip_filter_query, base_ns, end_ns).await;
    let crabka_ip_filter_result =
        crabka_query_range_result(querier.clone(), ip_filter_query, base_ns, end_ns).await;
    assert!(crabka_ip_filter_result == loki_ip_filter_result);

    let ip_single_filter_query = r#"{app="api",format="logfmt"} |= ip("10.2.3.4")"#;
    let loki_ip_single_filter_result =
        loki_query_range_result(&http, &loki_base, ip_single_filter_query, base_ns, end_ns).await;
    let crabka_ip_single_filter_result =
        crabka_query_range_result(querier.clone(), ip_single_filter_query, base_ns, end_ns).await;
    assert!(crabka_ip_single_filter_result == loki_ip_single_filter_result);

    let ip_range_filter_query = r#"{app="api",format="logfmt"} |= ip("10.2.3.0-10.2.3.10")"#;
    let loki_ip_range_filter_result =
        loki_query_range_result(&http, &loki_base, ip_range_filter_query, base_ns, end_ns).await;
    let crabka_ip_range_filter_result =
        crabka_query_range_result(querier.clone(), ip_range_filter_query, base_ns, end_ns).await;
    assert!(crabka_ip_range_filter_result == loki_ip_range_filter_result);

    let not_ip_filter_query = r#"{app="api",format="logfmt"} != ip("192.168.0.0/16")"#;
    let loki_not_ip_filter_result =
        loki_query_range_result(&http, &loki_base, not_ip_filter_query, base_ns, end_ns).await;
    let crabka_not_ip_filter_result =
        crabka_query_range_result(querier.clone(), not_ip_filter_query, base_ns, end_ns).await;
    assert!(crabka_not_ip_filter_result == loki_not_ip_filter_result);

    let metadata_query = r#"{app="api",format="metadata"} | trace_id = "abc""#;
    let loki_metadata_result =
        loki_query_range_result(&http, &loki_base, metadata_query, base_ns, end_ns).await;
    let crabka_metadata_result =
        crabka_query_range_result(querier, metadata_query, base_ns, end_ns).await;
    assert!(crabka_metadata_result == loki_metadata_result);

    assert!(json_contains_string(&loki_json_result, json_error_line));
    assert!(json_contains_string(
        &loki_selected_json_result,
        json_error_line
    ));
    assert!(json_contains_string(
        &loki_selected_json_result,
        r#""method":"GET""#
    ));
    assert!(!json_contains_string(
        &loki_selected_json_result,
        r#""request_method":"GET""#
    ));
    assert!(json_contains_string(&loki_logfmt_result, logfmt_error_line));
    assert!(json_contains_string(
        &loki_parameterized_logfmt_result,
        logfmt_error_line
    ));
    assert!(json_contains_string(
        &loki_logfmt_or_result,
        logfmt_error_line
    ));
    assert!(json_contains_string(
        &loki_logfmt_or_result,
        r#"status=200 msg="api parser ok""#
    ));
    assert!(json_contains_string(
        &loki_logfmt_comma_and_result,
        logfmt_error_line
    ));
    assert!(!json_contains_string(
        &loki_logfmt_comma_and_result,
        r#"status=200 msg="api parser ok""#
    ));
    assert!(json_contains_string(
        &loki_logfmt_adjacent_and_result,
        logfmt_error_line
    ));
    assert!(!json_contains_string(
        &loki_logfmt_adjacent_and_result,
        r#"status=200 msg="api parser ok""#
    ));
    assert!(json_contains_string(
        &loki_backtick_field_filter_result,
        logfmt_error_line
    ));
    assert!(json_contains_string(
        &loki_line_format_result,
        "api parser error 500"
    ));
    assert!(!json_contains_string(
        &loki_line_format_result,
        logfmt_error_line
    ));
    assert!(json_contains_string(
        &loki_line_format_pipeline_result,
        "API_PARSER_ERROR 500"
    ));
    assert!(!json_contains_string(
        &loki_line_format_pipeline_result,
        logfmt_error_line
    ));
    assert!(json_contains_string(
        &loki_label_format_result,
        logfmt_error_line
    ));
    assert!(json_contains_string(
        &loki_label_format_pipeline_result,
        logfmt_error_line
    ));
    assert!(json_contains_string(
        &loki_line_format_string_helper_result,
        "Checkout checkout api/items items /api"
    ));
    assert!(!json_contains_string(
        &loki_line_format_string_helper_result,
        logfmt_template_line
    ));
    assert!(json_contains_string(
        &loki_line_format_logical_helper_result,
        "true true true true"
    ));
    assert!(!json_contains_string(
        &loki_line_format_logical_helper_result,
        logfmt_template_line
    ));
    assert!(json_contains_string(
        &loki_line_format_ne_helper_result,
        "true true"
    ));
    assert!(!json_contains_string(
        &loki_line_format_ne_helper_result,
        logfmt_template_line
    ));
    assert!(json_contains_string(
        &loki_line_format_len_helper_result,
        "len=15"
    ));
    assert!(!json_contains_string(
        &loki_line_format_len_helper_result,
        logfmt_template_line
    ));
    assert!(json_contains_string(
        &loki_line_format_spacing_helper_result,
        "hi   |hello|   hi|world|xxx"
    ));
    assert!(!json_contains_string(
        &loki_line_format_spacing_helper_result,
        logfmt_spacing_template_line
    ));
    assert!(json_contains_string(
        &loki_line_format_regex_helper_result,
        "4|helper-template|${2}-${1}"
    ));
    assert!(!json_contains_string(
        &loki_line_format_regex_helper_result,
        logfmt_template_line
    ));
    assert!(json_contains_string(
        &loki_drop_keep_result,
        logfmt_error_line
    ));
    assert!(json_contains_string(
        &loki_decolorize_result,
        decolored_logfmt_error_line
    ));
    assert!(!json_contains_string(
        &loki_decolorize_result,
        colored_logfmt_error_line
    ));
    assert!(json_contains_string(
        &loki_pattern_result,
        logfmt_error_line
    ));
    assert!(json_contains_string(
        &loki_pattern_parser_result,
        logfmt_error_line
    ));
    assert!(json_contains_string(
        &loki_regexp_parser_result,
        logfmt_error_line
    ));
    assert!(json_contains_string(
        &loki_unpack_parser_result,
        "original log message"
    ));
    assert!(!json_contains_string(
        &loki_unpack_parser_result,
        packed_error_line
    ));
    assert!(json_contains_string(
        &loki_commented_result,
        logfmt_error_line
    ));
    assert!(json_contains_string(
        &loki_logfmt_typed_result,
        logfmt_typed_filter_line
    ));
    assert!(json_contains_string(&loki_ip_filter_result, logfmt_ip_line));
    assert!(!json_contains_string(
        &loki_ip_filter_result,
        logfmt_ip_miss_line
    ));
    assert!(json_contains_string(
        &loki_ip_single_filter_result,
        logfmt_ip_line
    ));
    assert!(!json_contains_string(
        &loki_ip_single_filter_result,
        logfmt_ip_miss_line
    ));
    assert!(json_contains_string(
        &loki_ip_range_filter_result,
        logfmt_ip_line
    ));
    assert!(!json_contains_string(
        &loki_ip_range_filter_result,
        logfmt_ip_miss_line
    ));
    assert!(json_contains_string(
        &loki_not_ip_filter_result,
        logfmt_ip_line
    ));
    assert!(!json_contains_string(
        &loki_not_ip_filter_result,
        logfmt_ip_miss_line
    ));
    assert!(json_contains_string(
        &loki_metadata_result,
        "api metadata ok"
    ));
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_parser_metric_results() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _broker_dir) = boot_crabka().await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");
    let object_store_url = format!("file://{}", object_root.path().display());
    let topic = "__crabka_observability_loki_parser_metric_differential";
    let index_prefix = "observability/logs";
    create_topic(&bootstrap, topic).await;
    broker.wait_until_partition_present(topic, 0).await;

    let base_ns = current_unix_second_ns() - 60_000_000_000;
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api",
                    "env": "prod",
                    "format": "json"
                },
                "values": [
                    [base_ns.to_string(), r#"{"request":{"method":"GET"},"response":{"status":200}}"#],
                    [(base_ns + 1_000_000_000).to_string(), r#"{"request":{"method":"GET"},"response":{"status":500}}"#]
                ]
            },
            {
                "stream": {
                    "app": "api",
                    "env": "prod",
                    "format": "logfmt"
                },
                "values": [
                    [base_ns.to_string(), "cost=1.5 requests=1 size=1KiB latency=100ms api unwrap metric one"],
                    [(base_ns + 1_000_000_000).to_string(), "cost=2.5 requests=3 size=2KiB latency=200ms api unwrap metric two"],
                    [(base_ns + 2_000_000_000).to_string(), "cost=0.5 requests=2 size=512B latency=1s api unwrap metric reset"]
                ]
            }
        ]
    });

    push_loki_payload(&http, &loki_base, &payload).await;

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
    compactor_config.wal_group_id = "loki-parser-metric-differential-compactor".to_string();
    let descriptors = run_compactor_until_idle(
        &compactor_config,
        build_service_dependencies(&compactor_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    assert!(descriptors.len() == 1);

    let mut querier_config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    querier_config.object_store_url = Some(object_store_url);
    querier_config.index_prefix = Some(index_prefix.to_string());
    querier_config.tenant = Some("tenant-a".to_string());
    querier_config.querier_index_source = QuerierIndexSource::TenantObjectStoreManifest;
    querier_config.wal_group_id = "loki-parser-metric-differential-querier".to_string();
    let querier = build_service_router(
        &querier_config,
        build_service_dependencies(&querier_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    let query =
        r#"count_over_time({app="api",format="json"} | json | response_status >= 500 [5s])"#;
    let end_ns = base_ns + 4_000_000_000;
    let loki_result =
        loki_query_range_result_with_step(&http, &loki_base, query, base_ns, end_ns, "1s").await;
    let crabka_result =
        crabka_query_range_result_with_step(querier.clone(), query, base_ns, end_ns, "1s").await;

    assert!(crabka_result == loki_result);

    for query in [
        r#"sum_over_time({app="api",format="logfmt"} | logfmt | unwrap cost | __error__ = "" [3s])"#,
        r#"avg_over_time({app="api",format="logfmt"} | logfmt | unwrap cost | __error__ = "" [3s])"#,
        r#"stdvar_over_time({app="api",format="logfmt"} | logfmt | unwrap cost | __error__ = "" [3s])"#,
        r#"stddev_over_time({app="api",format="logfmt"} | logfmt | unwrap cost | __error__ = "" [3s])"#,
        r#"quantile_over_time(0.75, {app="api",format="logfmt"} | logfmt | unwrap cost | __error__ = "" [3s])"#,
        r#"min_over_time({app="api",format="logfmt"} | logfmt | unwrap cost | __error__ = "" [3s])"#,
        r#"max_over_time({app="api",format="logfmt"} | logfmt | unwrap cost | __error__ = "" [3s])"#,
        r#"first_over_time({app="api",format="logfmt"} | logfmt | unwrap cost | __error__ = "" [3s])"#,
        r#"last_over_time({app="api",format="logfmt"} | logfmt | unwrap cost | __error__ = "" [3s])"#,
        r#"rate_counter({app="api",format="logfmt"} | logfmt | unwrap requests | __error__ = "" [3s])"#,
        r#"sum_over_time({app="api",format="logfmt"} | logfmt | unwrap bytes(size) | __error__ = "" [3s])"#,
        r#"sum_over_time({app="api",format="logfmt"} | logfmt | unwrap duration(latency) | __error__ = "" [3s])"#,
        r#"sum_over_time({app="api",format="logfmt"} | logfmt | unwrap duration_seconds(latency) | __error__ = "" [3s])"#,
    ] {
        let loki_result =
            loki_query_range_result_with_step(&http, &loki_base, query, base_ns, end_ns, "1s")
                .await;
        let crabka_result =
            crabka_query_range_result_with_step(querier.clone(), query, base_ns, end_ns, "1s")
                .await;

        assert!(
            crabka_result == loki_result,
            "unwrapped range metric mismatch for query {query}"
        );
    }
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_instant_metric_query_result() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _broker_dir) = boot_crabka().await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");
    let object_store_url = format!("file://{}", object_root.path().display());
    let topic = "__crabka_observability_loki_instant_metric_differential";
    let index_prefix = "observability/logs";
    create_topic(&bootstrap, topic).await;
    broker.wait_until_partition_present(topic, 0).await;

    let base_ns = current_unix_second_ns() - 60_000_000_000;
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api",
                    "env": "prod",
                    "format": "json"
                },
                "values": [
                    [base_ns.to_string(), r#"{"request":{"method":"GET"},"response":{"status":200}}"#],
                    [(base_ns + 1_000_000_000).to_string(), r#"{"request":{"method":"GET"},"response":{"status":500}}"#]
                ]
            }
        ]
    });

    push_loki_payload(&http, &loki_base, &payload).await;

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
    compactor_config.wal_group_id = "loki-instant-metric-differential-compactor".to_string();
    let descriptors = run_compactor_until_idle(
        &compactor_config,
        build_service_dependencies(&compactor_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    assert!(descriptors.len() == 1);

    let mut querier_config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    querier_config.object_store_url = Some(object_store_url);
    querier_config.index_prefix = Some(index_prefix.to_string());
    querier_config.tenant = Some("tenant-a".to_string());
    querier_config.querier_index_source = QuerierIndexSource::TenantObjectStoreManifest;
    querier_config.wal_group_id = "loki-instant-metric-differential-querier".to_string();
    let querier = build_service_router(
        &querier_config,
        build_service_dependencies(&querier_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    let query =
        r#"count_over_time({app="api",format="json"} | json | response_status >= 500 [5s])"#;
    let time_ns = base_ns + 4_000_000_000;
    let loki_result = loki_query_result(&http, &loki_base, query, time_ns).await;
    let crabka_result = crabka_query_result(querier.clone(), query, time_ns).await;

    assert!(crabka_result == loki_result);

    let parenthesized_scalar_query =
        r#"(count_over_time({app="api",format="json"} | json | response_status >= 500 [5s]) * 2)"#;
    let loki_parenthesized_scalar_result =
        loki_query_result(&http, &loki_base, parenthesized_scalar_query, time_ns).await;
    let crabka_parenthesized_scalar_result =
        crabka_query_result(querier.clone(), parenthesized_scalar_query, time_ns).await;

    assert!(crabka_parenthesized_scalar_result == loki_parenthesized_scalar_result);

    let parenthesized_operand_scalar_query =
        r#"(count_over_time({app="api",format="json"} | json | response_status >= 500 [5s])) * 2"#;
    let loki_parenthesized_operand_scalar_result = loki_query_result(
        &http,
        &loki_base,
        parenthesized_operand_scalar_query,
        time_ns,
    )
    .await;
    let crabka_parenthesized_operand_scalar_result =
        crabka_query_result(querier.clone(), parenthesized_operand_scalar_query, time_ns).await;

    assert!(crabka_parenthesized_operand_scalar_result == loki_parenthesized_operand_scalar_result);

    let metric_vector_query = r#"count_over_time({app="api",format="json"} | json | response_status >= 500 [5s]) + on() vector(1)"#;
    let loki_metric_vector_result =
        loki_query_result(&http, &loki_base, metric_vector_query, time_ns).await;
    let crabka_metric_vector_result =
        crabka_query_result(querier.clone(), metric_vector_query, time_ns).await;

    assert!(crabka_metric_vector_result == loki_metric_vector_result);

    let vector_metric_group_right_query = r#"vector(1) + on() group_right(app, env) count_over_time({app="api",format="json"} | json | response_status >= 500 [5s])"#;
    let loki_vector_metric_group_right_result =
        loki_query_result(&http, &loki_base, vector_metric_group_right_query, time_ns).await;
    let crabka_vector_metric_group_right_result =
        crabka_query_result(querier.clone(), vector_metric_group_right_query, time_ns).await;

    assert!(crabka_vector_metric_group_right_result == loki_vector_metric_group_right_result);

    let metric_vector_comparison_query = r#"count_over_time({app="api",format="json"} | json | response_status >= 500 [5s]) > bool on() vector(0)"#;
    let loki_metric_vector_comparison_result =
        loki_query_result(&http, &loki_base, metric_vector_comparison_query, time_ns).await;
    let crabka_metric_vector_comparison_result =
        crabka_query_result(querier.clone(), metric_vector_comparison_query, time_ns).await;

    assert!(crabka_metric_vector_comparison_result == loki_metric_vector_comparison_result);

    let metric_vector_set_query = r#"count_over_time({app="api",format="json"} | json | response_status >= 500 [5s]) and on() vector(1)"#;
    let loki_metric_vector_set_result =
        loki_query_result(&http, &loki_base, metric_vector_set_query, time_ns).await;
    let crabka_metric_vector_set_result =
        crabka_query_result(querier.clone(), metric_vector_set_query, time_ns).await;

    assert!(crabka_metric_vector_set_result == loki_metric_vector_set_result);

    let vector_metric_set_query = r#"vector(1) or on() count_over_time({app="api",format="json"} | json | response_status >= 500 [5s])"#;
    let loki_vector_metric_set_result =
        loki_query_result(&http, &loki_base, vector_metric_set_query, time_ns).await;
    let crabka_vector_metric_set_result =
        crabka_query_result(querier.clone(), vector_metric_set_query, time_ns).await;

    assert!(crabka_vector_metric_set_result == loki_vector_metric_set_result);

    let vector_metric_set_and_query = r#"vector(1) and on() count_over_time({app="api",format="json"} | json | response_status >= 500 [5s])"#;
    let loki_vector_metric_set_and_result =
        loki_query_result(&http, &loki_base, vector_metric_set_and_query, time_ns).await;
    let crabka_vector_metric_set_and_result =
        crabka_query_result(querier.clone(), vector_metric_set_and_query, time_ns).await;

    assert!(crabka_vector_metric_set_and_result == loki_vector_metric_set_and_result);

    let vector_metric_set_unless_query = r#"vector(1) unless on(app) count_over_time({app="api",format="json"} | json | response_status >= 500 [5s])"#;
    let loki_vector_metric_set_unless_result =
        loki_query_result(&http, &loki_base, vector_metric_set_unless_query, time_ns).await;
    let crabka_vector_metric_set_unless_result =
        crabka_query_result(querier.clone(), vector_metric_set_unless_query, time_ns).await;

    assert!(crabka_vector_metric_set_unless_result == loki_vector_metric_set_unless_result);

    let vector_metric_group_right_comparison_query = r#"vector(2) > bool on() group_right(app, env) count_over_time({app="api",format="json"} | json | response_status >= 500 [5s])"#;
    let loki_vector_metric_group_right_comparison_result = loki_query_result(
        &http,
        &loki_base,
        vector_metric_group_right_comparison_query,
        time_ns,
    )
    .await;
    let crabka_vector_metric_group_right_comparison_result = crabka_query_result(
        querier.clone(),
        vector_metric_group_right_comparison_query,
        time_ns,
    )
    .await;

    assert!(
        crabka_vector_metric_group_right_comparison_result
            == loki_vector_metric_group_right_comparison_result
    );

    let label_replace_query = r#"label_replace(count_over_time({app="api",format="json"}[5s]) / count_over_time({app="api",format="json"}[5s]), "service", "$1-api", "app", "(.*)")"#;
    let loki_label_replace_result =
        loki_query_result(&http, &loki_base, label_replace_query, time_ns).await;
    let crabka_label_replace_result =
        crabka_query_result(querier.clone(), label_replace_query, time_ns).await;

    assert!(crabka_label_replace_result == loki_label_replace_result);

    let parenthesized_label_replace_query = r#"(label_replace(count_over_time({app="api",format="json"}[5s]), "service", "$1-api", "app", "(.*)"))"#;
    let loki_parenthesized_label_replace_result = loki_query_result(
        &http,
        &loki_base,
        parenthesized_label_replace_query,
        time_ns,
    )
    .await;
    let crabka_parenthesized_label_replace_result =
        crabka_query_result(querier.clone(), parenthesized_label_replace_query, time_ns).await;

    assert!(crabka_parenthesized_label_replace_result == loki_parenthesized_label_replace_result);

    let label_replace_operand_query = r#"label_replace(count_over_time({app="api",format="json"}[5s]), "service", "$1-api", "app", "(.*)") / label_replace(count_over_time({app="api",format="json"}[5s]), "service", "$1-api", "app", "(.*)")"#;
    let loki_label_replace_operand_result =
        loki_query_result(&http, &loki_base, label_replace_operand_query, time_ns).await;
    let crabka_label_replace_operand_result =
        crabka_query_result(querier.clone(), label_replace_operand_query, time_ns).await;

    assert!(crabka_label_replace_operand_result == loki_label_replace_operand_result);

    let label_replace_grouped_operand_query = r#"label_replace(sum by(app, env)(count_over_time({app="api",format="json"}[5s])), "service", "$1-api", "app", "(.*)") / on(env) group_left label_replace(sum by(env)(count_over_time({app="api",format="json"}[5s])), "service", "$1-api", "app", "(.*)")"#;
    let loki_label_replace_grouped_operand_result = loki_query_result(
        &http,
        &loki_base,
        label_replace_grouped_operand_query,
        time_ns,
    )
    .await;
    let crabka_label_replace_grouped_operand_result = crabka_query_result(
        querier.clone(),
        label_replace_grouped_operand_query,
        time_ns,
    )
    .await;

    assert!(
        crabka_label_replace_grouped_operand_result == loki_label_replace_grouped_operand_result
    );

    let label_replace_scalar_operand_query = r#"label_replace(count_over_time({app="api",format="json"}[5s]) + 1, "service", "$1-api", "app", "(.*)") / label_replace(count_over_time({app="api",format="json"}[5s]) + 1, "service", "$1-api", "app", "(.*)")"#;
    let loki_label_replace_scalar_operand_result = loki_query_result(
        &http,
        &loki_base,
        label_replace_scalar_operand_query,
        time_ns,
    )
    .await;
    let crabka_label_replace_scalar_operand_result =
        crabka_query_result(querier.clone(), label_replace_scalar_operand_query, time_ns).await;

    assert!(crabka_label_replace_scalar_operand_result == loki_label_replace_scalar_operand_result);

    let loki_alias_result = loki_api_prom_query_result(&http, &loki_base, query, time_ns).await;
    let crabka_alias_result = crabka_api_prom_query_result(querier, query, time_ns).await;

    assert!(
        loki_alias_result
            == json!({
                "httpStatus": 400,
                "body": "rpc error: code = Code(400) desc = legacy endpoints only support streams result type",
            })
    );
    assert!(crabka_alias_result == loki_alias_result);
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_scalar_query_range_result() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    let query = "1+2";
    let loki_result =
        loki_query_range_result_with_step(&http, &loki_base, query, 0, 20_000_000_000, "10s").await;
    let crabka_result =
        crabka_query_range_result_with_step(querier.clone(), query, 0, 20_000_000_000, "10s").await;

    assert!(crabka_result == loki_result);

    let query = r#"label_replace(vector(1), "service", "api-$1", "missing", "(.*)")"#;
    let loki_result =
        loki_query_range_result_with_step(&http, &loki_base, query, 0, 20_000_000_000, "10s").await;
    let crabka_result =
        crabka_query_range_result_with_step(querier.clone(), query, 0, 20_000_000_000, "10s").await;

    assert!(crabka_result == loki_result);

    let query = r#"label_replace(vector(1), "service", "api-$1", "missing", "(.*)") or vector(2)"#;
    let loki_result =
        loki_query_range_result_with_step(&http, &loki_base, query, 0, 20_000_000_000, "10s").await;
    let crabka_result =
        crabka_query_range_result_with_step(querier, query, 0, 20_000_000_000, "10s").await;

    assert!(crabka_result == loki_result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_label_replace_vector_function_result() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    let query = r#"label_replace(vector(1), "service", "api-$1", "missing", "(.*)")"#;
    let loki_result = loki_query_result(&http, &loki_base, query, 4_000_000_000).await;
    let crabka_result = crabka_query_result(querier.clone(), query, 4_000_000_000).await;

    assert!(crabka_result == loki_result);

    let arithmetic_query =
        r#"label_replace(vector(1), "service", "api-$1", "missing", "(.*)") + on() vector(2)"#;
    let loki_arithmetic_result =
        loki_query_result(&http, &loki_base, arithmetic_query, 4_000_000_000).await;
    let crabka_arithmetic_result =
        crabka_query_result(querier.clone(), arithmetic_query, 4_000_000_000).await;

    assert!(crabka_arithmetic_result == loki_arithmetic_result);

    let set_query =
        r#"label_replace(vector(1), "service", "api-$1", "missing", "(.*)") or vector(2)"#;
    let loki_set_result = loki_query_result(&http, &loki_base, set_query, 4_000_000_000).await;
    let crabka_set_result = crabka_query_result(querier.clone(), set_query, 4_000_000_000).await;

    assert!(crabka_set_result == loki_set_result);

    let sort_query = r#"sort(label_replace(vector(1), "service", "api-$1", "missing", "(.*)"))"#;
    let loki_sort_result = loki_query_result(&http, &loki_base, sort_query, 4_000_000_000).await;
    let crabka_sort_result = crabka_query_result(querier.clone(), sort_query, 4_000_000_000).await;

    assert!(crabka_sort_result == loki_sort_result);

    let sort_desc_query =
        r#"sort_desc(label_replace(vector(1), "service", "api-$1", "missing", "(.*)"))"#;
    let loki_sort_desc_result =
        loki_query_result(&http, &loki_base, sort_desc_query, 4_000_000_000).await;
    let crabka_sort_desc_result =
        crabka_query_result(querier, sort_desc_query, 4_000_000_000).await;

    assert!(crabka_sort_desc_result == loki_sort_desc_result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_use_same_duplicate_query_param_precedence() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    let raw_query = "query=vector%281%29&query=vector%282%29";
    let loki_result = loki_raw_query_result(&http, &loki_base, raw_query).await;
    let crabka_result = crabka_raw_query_result(querier.clone(), raw_query).await;
    assert!(crabka_result == loki_result);

    let raw_query = "query=vector%281%29&time=1&time=2";
    let loki_result = loki_raw_query_result(&http, &loki_base, raw_query).await;
    let crabka_result = crabka_raw_query_result(querier, raw_query).await;
    assert!(crabka_result == loki_result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_parser_error_labels() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _broker_dir) = boot_crabka().await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");
    let object_store_url = format!("file://{}", object_root.path().display());
    let topic = "__crabka_observability_loki_parser_error_differential";
    let index_prefix = "observability/logs";
    create_topic(&bootstrap, topic).await;
    broker.wait_until_partition_present(topic, 0).await;

    let base_ns = current_unix_second_ns() - 60_000_000_000;
    let invalid_line = "not json";
    let valid_line = r#"{"status":500}"#;
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api",
                    "env": "prod",
                    "format": "json"
                },
                "values": [
                    [base_ns.to_string(), invalid_line],
                    [(base_ns + 1_000_000_000).to_string(), valid_line]
                ]
            }
        ]
    });

    push_loki_payload(&http, &loki_base, &payload).await;

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
    compactor_config.wal_group_id = "loki-parser-error-differential-compactor".to_string();
    let descriptors = run_compactor_until_idle(
        &compactor_config,
        build_service_dependencies(&compactor_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    assert!(descriptors.len() == 1);

    let mut querier_config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    querier_config.object_store_url = Some(object_store_url);
    querier_config.index_prefix = Some(index_prefix.to_string());
    querier_config.tenant = Some("tenant-a".to_string());
    querier_config.querier_index_source = QuerierIndexSource::TenantObjectStoreManifest;
    querier_config.wal_group_id = "loki-parser-error-differential-querier".to_string();
    let querier = build_service_router(
        &querier_config,
        build_service_dependencies(&querier_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    let query = r#"{app="api",format="json"} | json"#;
    let end_ns = base_ns + 2_000_000_000;
    let loki_result = loki_query_range_result(&http, &loki_base, query, base_ns, end_ns).await;
    let crabka_result = crabka_query_range_result(querier.clone(), query, base_ns, end_ns).await;

    assert!(crabka_result == loki_result);
    assert!(json_contains_string(&loki_result, invalid_line));
    assert!(json_contains_string(&loki_result, valid_line));

    let query = r#"{app="api",format="json"} | json | __error__ = """#;
    let loki_result = loki_query_range_result(&http, &loki_base, query, base_ns, end_ns).await;
    let crabka_result = crabka_query_range_result(querier, query, base_ns, end_ns).await;

    assert!(crabka_result == loki_result);
    assert!(!json_contains_string(&loki_result, invalid_line));
    assert!(json_contains_string(&loki_result, valid_line));
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_logfmt_malformed_field_results() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _broker_dir) = boot_crabka().await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");
    let object_store_url = format!("file://{}", object_root.path().display());
    let topic = "__crabka_observability_loki_logfmt_parser_error_differential";
    let index_prefix = "observability/logs";
    create_topic(&bootstrap, topic).await;
    broker.wait_until_partition_present(topic, 0).await;

    let base_ns = current_unix_second_ns() - 60_000_000_000;
    let invalid_line = r#"status=500 msg="unterminated"#;
    let valid_line = r#"status=200 msg="ok""#;
    let standalone_key_line = r#"status=204 empty msg="keep empty""#;
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api",
                    "env": "prod",
                    "format": "logfmt"
                },
                "values": [
                    [base_ns.to_string(), invalid_line],
                    [(base_ns + 1_000_000_000).to_string(), valid_line],
                    [(base_ns + 2_000_000_000).to_string(), standalone_key_line]
                ]
            }
        ]
    });

    push_loki_payload(&http, &loki_base, &payload).await;

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
    compactor_config.wal_group_id = "loki-logfmt-parser-error-differential-compactor".to_string();
    let descriptors = run_compactor_until_idle(
        &compactor_config,
        build_service_dependencies(&compactor_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    assert!(descriptors.len() == 1);

    let mut querier_config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    querier_config.object_store_url = Some(object_store_url);
    querier_config.index_prefix = Some(index_prefix.to_string());
    querier_config.tenant = Some("tenant-a".to_string());
    querier_config.querier_index_source = QuerierIndexSource::TenantObjectStoreManifest;
    querier_config.wal_group_id = "loki-logfmt-parser-error-differential-querier".to_string();
    let querier = build_service_router(
        &querier_config,
        build_service_dependencies(&querier_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    let query = r#"{app="api",format="logfmt"} | logfmt"#;
    let end_ns = base_ns + 3_000_000_000;
    let loki_result = loki_query_range_result(&http, &loki_base, query, base_ns, end_ns).await;
    let crabka_result = crabka_query_range_result(querier.clone(), query, base_ns, end_ns).await;

    assert!(crabka_result == loki_result);
    assert!(json_contains_string(&loki_result, invalid_line));
    assert!(json_contains_string(&loki_result, valid_line));
    assert!(json_contains_string(&loki_result, standalone_key_line));

    let keep_empty_query = r#"{app="api",format="logfmt"} | logfmt --keep-empty | empty = """#;
    let loki_keep_empty_result =
        loki_query_range_result(&http, &loki_base, keep_empty_query, base_ns, end_ns).await;
    let crabka_keep_empty_result =
        crabka_query_range_result(querier.clone(), keep_empty_query, base_ns, end_ns).await;
    assert!(crabka_keep_empty_result == loki_keep_empty_result);
    assert!(json_contains_string(
        &loki_keep_empty_result,
        standalone_key_line
    ));

    let strict_query =
        r#"{app="api",format="logfmt"} | logfmt --strict | __error__ = "LogfmtParserErr""#;
    let loki_strict_result =
        loki_query_range_result(&http, &loki_base, strict_query, base_ns, end_ns).await;
    let crabka_strict_result =
        crabka_query_range_result(querier.clone(), strict_query, base_ns, end_ns).await;
    assert!(crabka_strict_result == loki_strict_result);
    assert!(json_contains_string(&loki_strict_result, invalid_line));
    assert!(!json_contains_string(&loki_strict_result, valid_line));

    let strict_clean_query = r#"{app="api",format="logfmt"} | logfmt --strict | __error__ = """#;
    let loki_strict_clean_result =
        loki_query_range_result(&http, &loki_base, strict_clean_query, base_ns, end_ns).await;
    let crabka_strict_clean_result =
        crabka_query_range_result(querier, strict_clean_query, base_ns, end_ns).await;
    assert!(crabka_strict_clean_result == loki_strict_clean_result);
    assert!(!json_contains_string(
        &loki_strict_clean_result,
        invalid_line
    ));
    assert!(json_contains_string(&loki_strict_clean_result, valid_line));
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_invalid_query_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    for query in [
        r#"{app="#,
        "vector(-2.5e-1)",
        "vector(1)orvector(2)",
        "abs(vector(-1.2))",
        r#"count_values by (env) ("hits", count_over_time({app="api"}[1s]))"#,
        r#"count_values("hits", count_over_time({app="api"}[1s])) by (env)"#,
        r#"approx_topk(1, count_over_time({app="api"}[1s]))"#,
    ] {
        let loki_error = loki_query_error(&http, &loki_base, query).await;
        let crabka_error = crabka_query_error(querier.clone(), query).await;

        assert!(crabka_error == loki_error);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_use_same_query_post_body_precedence() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    for (path, raw_query) in [
        (
            "/loki/api/v1/query",
            "query=%7Bapp%3D%22api%22%7D&time=1000000000",
        ),
        (
            "/loki/api/v1/query_range",
            "query=%7Bapp%3D%22api%22%7D&start=0&end=1000000000",
        ),
        (
            "/api/prom/query",
            "query=%7Bapp%3D%22api%22%7D&time=1000000000",
        ),
    ] {
        let body = "query=%7Bapp%3D";
        let loki_response =
            loki_post_query_precedence_response(&http, &loki_base, path, raw_query, body).await;
        let crabka_response =
            crabka_post_query_precedence_response(querier.clone(), path, raw_query, body).await;
        assert!(crabka_response == loki_response, "{path}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_invalid_tail_query_errors() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    let loki_error = loki_tail_ws_error(&loki_base, Some("{app=")).await;
    let crabka_error = crabka_tail_ws_error(querier.clone(), Some("{app=")).await;

    assert!(crabka_error == loki_error);

    let loki_error = loki_tail_ws_error(&loki_base, None).await;
    let crabka_error = crabka_tail_ws_error(querier, None).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_invalid_tail_delay_for_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    let raw_query = "query=%7Bapp%3D%22api%22%7D&delay_for=6";
    let loki_error = loki_tail_ws_raw_error(&loki_base, raw_query).await;
    let crabka_error = crabka_tail_ws_raw_error(querier, raw_query).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_invalid_query_range_direction_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = r#"{app="api"}"#;

    let loki_error = loki_query_range_direction_error(&http, &loki_base, query, "sideways").await;
    let crabka_error = crabka_query_range_direction_error(querier, query, "sideways").await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_invalid_query_range_step_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = r#"count_over_time({app="api"}[30s])"#;

    let loki_error = loki_query_range_step_error(&http, &loki_base, query, "0").await;
    let crabka_error = crabka_query_range_step_error(querier, query, "0").await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_invalid_query_range_step_parse_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = r#"count_over_time({app="api"}[30s])"#;

    let loki_error = loki_query_range_step_error(&http, &loki_base, query, "not-a-number").await;
    let crabka_error = crabka_query_range_step_error(querier, query, "not-a-number").await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_excessive_query_range_resolution_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = "vector(1)";

    let loki_error = loki_query_range_resolution_error(&http, &loki_base, query).await;
    let crabka_error = crabka_query_range_resolution_error(querier, query).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_oversized_query_range_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = "vector(1)";

    let loki_error = loki_query_range_range_error(&http, &loki_base, query).await;
    let crabka_error = crabka_query_range_range_error(querier, query).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_invalid_index_volume_range_step_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = r#"{app="api"}"#;

    let loki_error = loki_index_volume_range_step_error(&http, &loki_base, query, "0").await;
    let crabka_error = crabka_index_volume_range_step_error(querier.clone(), query, "0").await;
    assert!(crabka_error == loki_error);

    let loki_error =
        loki_index_volume_range_step_error(&http, &loki_base, query, "not-a-number").await;
    let crabka_error = crabka_index_volume_range_step_error(querier, query, "not-a-number").await;
    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_invalid_index_volume_aggregate_by_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = r#"{app="api"}"#;

    for endpoint in ["index/volume", "index/volume_range"] {
        let loki_error =
            loki_index_volume_aggregate_by_error(&http, &loki_base, endpoint, query, "bogus").await;
        let crabka_error =
            crabka_index_volume_aggregate_by_error(querier.clone(), endpoint, query, "bogus").await;
        assert!(crabka_error == loki_error);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_missing_index_volume_bounds_errors() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = r#"{app="api"}"#;

    for (path, params) in [
        (
            "index/volume",
            vec![
                ("query", query.to_string()),
                ("end", "1000000000".to_string()),
            ],
        ),
        (
            "index/volume",
            vec![("query", query.to_string()), ("start", "0".to_string())],
        ),
        (
            "index/volume_range",
            vec![
                ("query", query.to_string()),
                ("end", "1000000000".to_string()),
                ("step", "1000000000".to_string()),
            ],
        ),
        (
            "index/volume_range",
            vec![
                ("query", query.to_string()),
                ("start", "0".to_string()),
                ("step", "1000000000".to_string()),
            ],
        ),
    ] {
        let loki_response =
            loki_index_volume_params_response(&http, &loki_base, path, &params).await;
        let crabka_response =
            crabka_index_volume_params_response(querier.clone(), path, &params).await;
        assert_eq!(crabka_response, loki_response, "{path} params {params:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_invalid_index_query_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = "{app=";
    let start_ns = current_unix_second_ns() - 60_000_000_000;
    let end_ns = start_ns + 1_000_000_000;

    for endpoint in ["index/stats", "index/volume", "index/volume_range"] {
        let loki_error =
            loki_index_query_error(&http, &loki_base, endpoint, query, start_ns, end_ns).await;
        let crabka_error =
            crabka_index_query_error(querier.clone(), endpoint, query, start_ns, end_ns).await;
        assert!(crabka_error == loki_error);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_use_same_index_volume_duplicate_query_precedence() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let valid_query = r#"{app="api"}"#;
    let invalid_query = "{app=";

    let params = [
        ("query", valid_query.to_string()),
        ("query", invalid_query.to_string()),
        ("start", "0".to_string()),
        ("end", "1".to_string()),
    ];
    let loki_response =
        loki_index_volume_params_response(&http, &loki_base, "index/volume", &params).await;
    let crabka_response =
        crabka_index_volume_params_response(querier, "index/volume", &params).await;
    assert_eq!(crabka_response, loki_response);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_oversized_index_stats_range_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = r#"{app="api"}"#;

    let loki_error = loki_index_query_error(
        &http,
        &loki_base,
        "index/stats",
        query,
        0,
        2_595_601_000_000_000,
    )
    .await;
    let crabka_error =
        crabka_index_query_error(querier, "index/stats", query, 0, 2_595_601_000_000_000).await;
    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_use_same_index_stats_post_body_precedence() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = r#"{app="api"}"#;
    let body = format!(
        "query={}&start=0&end=2595601000000000",
        percent_encode_component(query)
    );

    let loki_error = loki_index_stats_post_body_precedence_error(&http, &loki_base, &body).await;
    let crabka_error = crabka_index_stats_post_body_precedence_error(querier, &body).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_missing_query_errors() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let start_ns = current_unix_second_ns() - 60_000_000_000;
    let end_ns = start_ns + 1_000_000_000;

    for path in [
        "query",
        "query_range",
        "index/stats",
        "index/volume",
        "index/volume_range",
        "detected_fields",
        "detected_field/status/values",
    ] {
        let params = if path == "query" {
            Vec::new()
        } else if path == "index/volume_range" {
            vec![
                ("start", start_ns.to_string()),
                ("end", end_ns.to_string()),
                ("step", "1000000000".to_string()),
            ]
        } else {
            vec![("start", start_ns.to_string()), ("end", end_ns.to_string())]
        };
        let loki_error = loki_missing_query_error(&http, &loki_base, path, &params).await;
        let crabka_error = crabka_missing_query_error(querier.clone(), path, &params).await;
        assert!(crabka_error == loki_error);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_missing_series_matcher_errors() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    for path in ["/loki/api/v1/series", "/api/prom/series"] {
        let loki_error = loki_raw_path_error(&http, &loki_base, path).await;
        let crabka_error = crabka_raw_path_error(querier.clone(), path).await;
        assert!(crabka_error == loki_error, "{path}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_empty_series_post_errors() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    for path in ["/loki/api/v1/series", "/api/prom/series"] {
        let loki_error = loki_raw_post_path_error(&http, &loki_base, path).await;
        let crabka_error = crabka_raw_post_path_error(querier.clone(), path).await;
        assert!(crabka_error == loki_error, "{path}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_invalid_detected_fields_step_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = r#"{app="api"}"#;

    for endpoint in ["detected_fields", "detected_field/status/values"] {
        let loki_error =
            loki_detected_fields_step_error(&http, &loki_base, endpoint, query, "0").await;
        let crabka_error =
            crabka_detected_fields_step_error(querier.clone(), endpoint, query, "0").await;
        assert!(crabka_error == loki_error);

        let loki_error =
            loki_detected_fields_step_error(&http, &loki_base, endpoint, query, "not-a-number")
                .await;
        let crabka_error =
            crabka_detected_fields_step_error(querier.clone(), endpoint, query, "not-a-number")
                .await;
        assert!(crabka_error == loki_error);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_invalid_detected_fields_query_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = "{app=";

    for endpoint in ["detected_fields", "detected_field/status/values"] {
        let loki_error = loki_detected_fields_query_error(&http, &loki_base, endpoint, query).await;
        let crabka_error =
            crabka_detected_fields_query_error(querier.clone(), endpoint, query).await;
        assert!(crabka_error == loki_error);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_oversized_detected_endpoint_range_errors() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = r#"{app="api"}"#;

    for (endpoint, query) in [
        ("detected_labels", None),
        ("detected_fields", Some(query)),
        ("detected_field/status/values", Some(query)),
    ] {
        let loki_error =
            loki_detected_endpoint_range_error(&http, &loki_base, endpoint, query).await;
        let crabka_error =
            crabka_detected_endpoint_range_error(querier.clone(), endpoint, query).await;
        assert!(crabka_error == loki_error);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_use_same_detected_endpoint_duplicate_start_precedence() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = r#"{app="api"}"#;

    for (endpoint, query) in [
        ("detected_labels", None),
        ("detected_fields", Some(query)),
        ("detected_field/status/values", Some(query)),
    ] {
        let loki_error =
            loki_detected_endpoint_duplicate_start_error(&http, &loki_base, endpoint, query).await;
        let crabka_error =
            crabka_detected_endpoint_duplicate_start_error(querier.clone(), endpoint, query).await;
        assert!(crabka_error == loki_error);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_use_same_detected_endpoint_post_body_precedence() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = r#"{app="api"}"#;

    for endpoint in [
        "detected_labels",
        "detected_fields",
        "detected_field/status/values",
    ] {
        let loki_error =
            loki_detected_endpoint_post_body_precedence_error(&http, &loki_base, endpoint, query)
                .await;
        let crabka_error =
            crabka_detected_endpoint_post_body_precedence_error(querier.clone(), endpoint, query)
                .await;
        assert!(crabka_error == loki_error);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_invalid_query_range_start_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = r#"{app="api"}"#;

    let loki_error = loki_query_range_start_error(&http, &loki_base, query, "not-a-number").await;
    let crabka_error = crabka_query_range_start_error(querier, query, "not-a-number").await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_invalid_query_range_since_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = r#"{app="api"}"#;

    let loki_error = loki_query_range_since_error(&http, &loki_base, query, "-1").await;
    let crabka_error = crabka_query_range_since_error(querier, query, "-1").await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_zero_query_range_interval_result() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = r#"{app="api"}"#;
    let start_ns = current_unix_second_ns() - 1_000_000_000;
    let end_ns = start_ns + 1_000_000_000;

    let loki_result =
        loki_query_range_interval_result(&http, &loki_base, query, start_ns, end_ns, "0").await;
    let crabka_result =
        crabka_query_range_interval_result(querier, query, start_ns, end_ns, "0").await;

    assert!(crabka_result == loki_result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_negative_query_range_interval_result() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = r#"{app="api"}"#;
    let start_ns = current_unix_second_ns() - 1_000_000_000;
    let end_ns = start_ns + 1_000_000_000;

    let loki_result =
        loki_query_range_interval_result(&http, &loki_base, query, start_ns, end_ns, "-1").await;
    let crabka_result =
        crabka_query_range_interval_result(querier, query, start_ns, end_ns, "-1").await;

    assert!(crabka_result == loki_result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_invalid_query_limit_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = r#"{app="api"}"#;

    let loki_error = loki_query_limit_error(&http, &loki_base, query, "not-a-number").await;
    let crabka_error = crabka_query_limit_error(querier, query, "not-a-number").await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_negative_query_limit_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = r#"{app="api"}"#;

    let loki_error = loki_query_limit_error(&http, &loki_base, query, "-1").await;
    let crabka_error = crabka_query_limit_error(querier, query, "-1").await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_format_query_result() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let query = r#"{app="api"} | logfmt | method=~"GET|POST" | path!~"/health.*""#;

    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = "(1+2)*3";
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = "vector(1) or vector(2)";
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = "vector(1)+vector(2)";
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"count_over_time({app="api"}[30s])+vector(1)"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"count_over_time({app="api"}[30s])+1.25e-1"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"count_over_time({app="api"}[10s] offset 1500ms)"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"1>bool count_over_time({app="api"}[30s])"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"quantile_over_time(0.75,{app="api"} | logfmt | unwrap cost [30s])+vector(1)"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"count_over_time({app="api"}[30s]) or on(app) vector(1)"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"vector(1)+count_over_time({app="api"}[30s])"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"count_over_time({app="api"}[30s])+on(app)vector(1)"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"vector(1)+on(app)group_left count_over_time({app="api"}[30s])"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = "vector(1)+on(app,env)vector(2)";
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = "vector(1)+on(app)group_left(env)vector(2)";
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = "vector(1)+on(app)group_left vector(2)";
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"count_over_time({app="api"}[30s])>bool vector(1)"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"vector(1)>on(app)group_left count_over_time({app="api"}[30s])"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = "vector(1)>bool vector(2)";
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = "vector(2.5e-1)";
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"sum(rate({app="api"}|="error"[5m])) by (env,status)"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query =
        r#"count_over_time({app="api"}[30s]) / count_over_time({app="api"} |= "error" [30s])"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"count_over_time({app="api"}[30s]) > bool count_over_time({app="worker"}[30s])"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"count_over_time({app="api"}[30s]) or count_over_time({app="worker"}[30s])"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query =
        r#"count_over_time({app="api"}[30s]) / ignoring(app) count_over_time({app="worker"}[30s])"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"sum by(app, env)(count_over_time({env="prod"}[30s])) / on(env) group_left sum by(env)(count_over_time({env="prod"}[30s]))"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"sort(count_over_time({app="api"}[30s]) + vector(1))"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"sort(label_replace(vector(1), "service", "api-$1", "missing", "(.*)"))"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"sort_desc(label_replace(count_over_time({app="api"}[30s]) + vector(1), "service", "$1-api", "app", "(.*)"))"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"quantile_over_time(.75,{app="api"}|logfmt|unwrap cost[30s]) by(app)"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"label_replace(count_over_time({app="api"}|="error"[30s]), "service", "$1-api", "app", "(.*)")"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"label_replace(vector(1), "service", "api-$1", "missing", "(.*)")"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"label_replace(vector(1)+vector(2), "service", "api-$1", "missing", "(.*)")"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"label_replace(vector(1), "service", "api-$1", "missing", "(.*)") + vector(2)"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"label_replace(vector(1), "service", "api-$1", "missing", "(.*)") or vector(2)"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"label_replace(count_over_time({app="api"}[30s]) + vector(1), "service", "$1-api", "app", "(.*)")"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"label_replace(count_over_time({app="api"}[30s]), "service", "$1-api", "app", "(.*)") + vector(2)"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"label_replace(count_over_time({app="api"}[30s]) + vector(1), "service", "$1-api", "app", "(.*)") + vector(2)"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"label_replace(count_over_time({app="api"}[30s]) + 1.25e-1, "service", "$1-api", "app", "(.*)") + vector(2)"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"label_replace(count_over_time({app="api"}[30s]), "service", "$1-api", "app", "(.*)") or vector(2)"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"label_replace(count_over_time({app="api"}[30s]) + vector(1), "service", "$1-api", "app", "(.*)") or vector(2)"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"label_replace(count_over_time({app="api"}[30s]), "service", "$1-api", "app", "(.*)") > bool vector(2)"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let query = r#"label_replace(count_over_time({app="api"}[30s]) + 1.25e-1, "service", "$1-api", "app", "(.*)")"#;
    let loki_result = loki_format_query_result(&http, &loki_base, query).await;
    let crabka_result = crabka_format_query_result(querier.clone(), query).await;

    assert!(crabka_result == loki_result);

    let loki_post_result = loki_format_query_post_result(
        &http,
        &loki_base,
        r#"{app="api"}"#,
        r#"query=%7Bapp%3D%22worker%22%7D"#,
    )
    .await;
    let crabka_post_result = crabka_format_query_post_result(
        querier,
        r#"{app="api"}"#,
        r#"query=%7Bapp%3D%22worker%22%7D"#,
    )
    .await;
    assert!(crabka_post_result == loki_post_result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_format_query_errors() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    let loki_error = loki_format_query_error(&http, &loki_base, Some("{foo=")).await;
    let crabka_error = crabka_format_query_error(querier.clone(), Some("{foo=")).await;
    assert!(crabka_error == loki_error);

    let loki_error = loki_format_query_error(&http, &loki_base, Some("vector(-2.5e-1)")).await;
    let crabka_error = crabka_format_query_error(querier.clone(), Some("vector(-2.5e-1)")).await;
    assert!(crabka_error == loki_error);

    let loki_error = loki_format_query_error(&http, &loki_base, Some("vector(1)orvector(2)")).await;
    let crabka_error =
        crabka_format_query_error(querier.clone(), Some("vector(1)orvector(2)")).await;
    assert!(crabka_error == loki_error);

    let loki_error = loki_format_query_error(
        &http,
        &loki_base,
        Some(r#"label_join(count_over_time({app="api"}[30s]), "joined", "/", "app")"#),
    )
    .await;
    let crabka_error = crabka_format_query_error(
        querier.clone(),
        Some(r#"label_join(count_over_time({app="api"}[30s]), "joined", "/", "app")"#),
    )
    .await;
    assert!(crabka_error == loki_error);

    let loki_error = loki_format_query_error(
        &http,
        &loki_base,
        Some(r#"label_join(vector(1), "joined", "/", "app")"#),
    )
    .await;
    let crabka_error = crabka_format_query_error(
        querier.clone(),
        Some(r#"label_join(vector(1), "joined", "/", "app")"#),
    )
    .await;
    assert!(crabka_error == loki_error);

    let loki_error = loki_format_query_error(&http, &loki_base, None).await;
    let crabka_error = crabka_format_query_error(querier, None).await;
    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_invalid_metadata_query_errors() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let paths = [
        "labels?query=%7Bapp%3D",
        "label/app/values?query=%7Bapp%3D",
        "series?match[]=%7Bapp%3D",
        "series?start=not-a-number",
        "labels?start=0&start=not-a-number",
    ];

    for path in paths {
        let loki_error = loki_metadata_error(&http, &loki_base, path).await;
        let crabka_error = crabka_metadata_error(querier.clone(), path).await;

        assert!(crabka_error == loki_error);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_oversized_metadata_range_errors() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let paths = [
        "labels?start=0&end=2595601000000000",
        "label/app/values?start=0&end=2595601000000000",
        "series?match[]=%7Bapp%3D%22api%22%7D&start=0&end=2595601000000000",
    ];

    for path in paths {
        let loki_error = loki_metadata_error(&http, &loki_base, path).await;
        let crabka_error = crabka_metadata_error(querier.clone(), path).await;

        assert!(crabka_error == loki_error);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_use_same_metadata_post_body_precedence() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let dir = TempDir::new().expect("querier root");
    let querier = loki_router(QuerierState::new(
        dir.path(),
        LabelIndex::default(),
        BlockIndex::default(),
    ));
    let body = "start=0&end=2595601000000000&match%5B%5D=%7Bapp%3D%22api%22%7D";

    for path in [
        "/loki/api/v1/labels",
        "/loki/api/v1/label/app/values",
        "/loki/api/v1/series",
        "/api/prom/label",
        "/api/prom/label/app/values",
        "/api/prom/series",
    ] {
        let loki_error =
            loki_metadata_post_body_precedence_error(&http, &loki_base, path, body).await;
        let crabka_error =
            crabka_metadata_post_body_precedence_error(querier.clone(), path, body).await;

        assert!(crabka_error == loki_error, "{path}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_invalid_push_label_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _) = boot_crabka().await;
    let topic = "__crabka_observability_loki_invalid_push_differential";
    create_topic(&bootstrap, topic).await;
    let data_root = TempDir::new().expect("data root");
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
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "bad-label": "api"
                },
                "values": [["1000000000", "invalid push label"]]
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_stale_push_timestamp_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _) = boot_crabka().await;
    let topic = "__crabka_observability_loki_stale_timestamp_differential";
    create_topic(&bootstrap, topic).await;
    let data_root = TempDir::new().expect("data root");
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
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api"
                },
                "values": [["1000000000", "stale push timestamp"]]
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_invalid_push_timestamp_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _) = boot_crabka().await;
    let topic = "__crabka_observability_loki_invalid_timestamp_differential";
    create_topic(&bootstrap, topic).await;
    let data_root = TempDir::new().expect("data root");
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
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api"
                },
                "values": [["not-a-timestamp", "invalid push timestamp"]]
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_non_string_push_timestamp_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api"
                },
                "values": [[1000000000, "non-string push timestamp"]]
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_deflated_json_push_response() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api"
                },
                "values": [[current_unix_second_ns().to_string(), "deflated json push"]]
            }
        ]
    })
    .to_string();
    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(payload.as_bytes()).unwrap();
    let payload = encoder.finish().unwrap();
    let headers = [
        ("content-type", "application/json"),
        ("content-encoding", "deflate"),
    ];

    let loki_result = loki_push_body_result(&http, &loki_base, payload.clone(), &headers).await;
    let crabka_result = crabka_push_body_result(distributor, payload, &headers).await;

    assert!(crabka_result == loki_result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_malformed_gzip_push_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let payload = b"not gzip".to_vec();
    let headers = [
        ("content-type", "application/json"),
        ("content-encoding", "gzip"),
    ];

    let loki_result = loki_push_body_result(&http, &loki_base, payload.clone(), &headers).await;
    let crabka_result = crabka_push_body_result(distributor, payload, &headers).await;

    assert!(crabka_result == loki_result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_malformed_deflate_push_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let payload = b"not deflate".to_vec();
    let headers = [
        ("content-type", "application/json"),
        ("content-encoding", "deflate"),
    ];

    let loki_result = loki_push_body_result(&http, &loki_base, payload.clone(), &headers).await;
    let crabka_result = crabka_push_body_result(distributor, payload, &headers).await;

    assert!(crabka_result == loki_result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_unsupported_content_encoding_push_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api"
                },
                "values": [[current_unix_second_ns().to_string(), "unsupported encoding"]]
            }
        ]
    })
    .to_string()
    .into_bytes();
    let headers = [
        ("content-type", "application/json"),
        ("content-encoding", "br"),
    ];

    let loki_result = loki_push_body_result(&http, &loki_base, payload.clone(), &headers).await;
    let crabka_result = crabka_push_body_result(distributor, payload, &headers).await;

    assert!(crabka_result == loki_result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_invalid_snappy_protobuf_push_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let payload = vec![0xff, 0xff, 0xff];
    let headers = [("content-type", "application/x-protobuf")];

    let loki_result = loki_push_body_result(&http, &loki_base, payload.clone(), &headers).await;
    let crabka_result = crabka_push_body_result(distributor, payload, &headers).await;

    assert!(crabka_result == loki_result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_object_push_timestamp_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api"
                },
                "values": [[{"ts": "1000000000"}, "object push timestamp"]]
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_array_push_timestamp_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api"
                },
                "values": [[["1000000000"], "array push timestamp"]]
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_invalid_push_line_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _) = boot_crabka().await;
    let topic = "__crabka_observability_loki_invalid_line_differential";
    create_topic(&bootstrap, topic).await;
    let data_root = TempDir::new().expect("data root");
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
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api"
                },
                "values": [["1000000000", 500]]
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_incomplete_push_value_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let timestamp = current_unix_second_ns().to_string();
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api"
                },
                "values": [[timestamp]]
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_empty_push_value_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _) = boot_crabka().await;
    let topic = "__crabka_observability_loki_empty_value_differential";
    create_topic(&bootstrap, topic).await;
    let data_root = TempDir::new().expect("data root");
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
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api"
                },
                "values": [[]]
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_non_object_metadata_push_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let timestamp = current_unix_second_ns().to_string();
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api"
                },
                "values": [[timestamp, "invalid metadata shape", null]]
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_extra_push_value_field_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let timestamp = current_unix_second_ns().to_string();
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api"
                },
                "values": [[timestamp, "extra push value field", {"trace_id": "abc"}, "extra"]]
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_non_array_push_value_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let timestamp = current_unix_second_ns().to_string();
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api"
                },
                "values": [
                    [timestamp, "valid line"],
                    "not-a-push-value"
                ]
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_non_object_push_stream_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let payload = json!({
        "streams": [
            "not-a-stream"
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_non_array_push_streams_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let payload = json!({
        "streams": "not-streams"
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_array_push_payload_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let payload = r#"[{"streams": []}]"#;

    let loki_error = loki_push_raw_error(&http, &loki_base, payload).await;
    let crabka_error = crabka_push_raw_error(distributor, payload).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_null_push_payload_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let payload = "null";

    let loki_error = loki_push_raw_error(&http, &loki_base, payload).await;
    let crabka_error = crabka_push_raw_error(distributor, payload).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_missing_push_streams_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let payload = json!({});

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_empty_push_streams_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let payload = json!({
        "streams": []
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_missing_push_values_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api"
                }
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_non_array_push_values_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api"
                },
                "values": "not-values"
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_non_object_push_labels_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let timestamp = current_unix_second_ns().to_string();
    let payload = json!({
        "streams": [
            {
                "stream": "not-labels",
                "values": [[timestamp, "labels field is not an object"]]
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_missing_push_labels_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let timestamp = current_unix_second_ns().to_string();
    let payload = json!({
        "streams": [
            {
                "values": [[timestamp, "missing labels field"]]
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_null_push_labels_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let timestamp = current_unix_second_ns().to_string();
    let payload = json!({
        "streams": [
            {
                "stream": null,
                "values": [[timestamp, "null labels field"]]
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_null_push_values_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let distributor = distributor_router_for_status();
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api"
                },
                "values": null
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_future_push_timestamp_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _) = boot_crabka().await;
    let topic = "__crabka_observability_loki_future_timestamp_differential";
    create_topic(&bootstrap, topic).await;
    let data_root = TempDir::new().expect("data root");
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
    let timestamp = (current_unix_second_ns() + 15 * 60 * 1_000_000_000).to_string();
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api"
                },
                "values": [[timestamp, "future push timestamp"]]
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_future_otlp_timestamp_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _broker_dir) = boot_crabka().await;
    let data_root = TempDir::new().expect("data root");
    let topic = "__crabka_observability_otlp_future_timestamp_differential";
    create_topic(&bootstrap, topic).await;
    broker.wait_until_partition_present(topic, 0).await;

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

    let timestamp = (current_unix_second_ns() + 15 * 60 * 1_000_000_000).to_string();
    let payload = json!({
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "checkout"}}
                    ]
                },
                "scopeLogs": [
                    {
                        "logRecords": [
                            {
                                "timeUnixNano": timestamp,
                                "body": {"stringValue": "future otlp timestamp"}
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let loki_error = loki_otlp_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_otlp_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_empty_push_label_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _broker_dir) = boot_crabka().await;
    let topic = "__crabka_observability_loki_empty_push_label_differential";
    create_topic(&bootstrap, topic).await;
    broker.wait_until_partition_present(topic, 0).await;
    let data_root = TempDir::new().expect("data root");
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
    let timestamp = current_unix_second_ns().to_string();
    let payload = json!({
        "streams": [
            {
                "stream": {},
                "values": [[timestamp, "empty push label"]]
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_non_string_structured_metadata_push_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _) = boot_crabka().await;
    let topic = "__crabka_observability_loki_invalid_metadata_differential";
    create_topic(&bootstrap, topic).await;
    let data_root = TempDir::new().expect("data root");
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
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api"
                },
                "values": [["1000000000", "invalid metadata value", {"status": 500}]]
            }
        ]
    });

    let loki_error = loki_push_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_loki_and_crabka_return_same_duplicate_push_label_error() {
    let image = GenericImage::new("grafana/loki", "3.4.2")
        .with_exposed_port(LOKI_PORT.tcp())
        .with_wait_for(WaitFor::seconds(2));
    let loki = image.start().await.expect("start Loki container");
    let loki_base = format!(
        "http://127.0.0.1:{}",
        loki.get_host_port_ipv4(LOKI_PORT)
            .await
            .expect("Loki mapped port")
    );
    let http = reqwest::Client::new();
    wait_for_loki_ready(&http, &loki_base).await;

    let (broker, bootstrap, _broker_dir) = boot_crabka().await;
    let topic = "__crabka_observability_loki_duplicate_push_label_differential";
    create_topic(&bootstrap, topic).await;
    broker.wait_until_partition_present(topic, 0).await;
    let data_root = TempDir::new().expect("data root");
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
    let timestamp = current_unix_second_ns().to_string();
    let payload = format!(
        r#"{{
        "streams": [
            {{
                "stream": {{
                    "app": "api",
                    "app": "worker"
                }},
                "values": [["{timestamp}", "duplicate push label"]]
            }}
        ]
    }}"#
    );

    let loki_error = loki_push_raw_error(&http, &loki_base, &payload).await;
    let crabka_error = crabka_push_raw_error(distributor, &payload).await;

    assert!(crabka_error == loki_error);
    broker.shutdown().await;
}

fn current_unix_second_ns() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs();
    i64::try_from(now).expect("unix seconds fit in i64") * 1_000_000_000
}

async fn wait_for_loki_ready(http: &reqwest::Client, base: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(response) = http.get(format!("{base}/ready")).send().await
            && response.status() == reqwest::StatusCode::OK
        {
            return;
        }
        assert!(Instant::now() < deadline, "Loki did not become ready");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn push_loki_payload(http: &reqwest::Client, base: &str, payload: &Value) {
    let response = http
        .post(format!("{base}/loki/api/v1/push"))
        .header("X-Scope-OrgID", "tenant-a")
        .json(payload)
        .send()
        .await
        .expect("push to Loki");
    assert!(
        response.status() == reqwest::StatusCode::NO_CONTENT,
        "Loki push failed: {}",
        response.text().await.unwrap_or_default()
    );
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
    assert!(response.status() == StatusCode::NO_CONTENT);
}

async fn loki_push_error(http: &reqwest::Client, base: &str, payload: &Value) -> Value {
    loki_push_raw_error(http, base, &payload.to_string()).await
}

async fn loki_push_raw_error(http: &reqwest::Client, base: &str, payload: &str) -> Value {
    loki_push_body_result(
        http,
        base,
        payload.as_bytes().to_vec(),
        &[("content-type", "application/json")],
    )
    .await
}

async fn loki_push_body_result(
    http: &reqwest::Client,
    base: &str,
    body: Vec<u8>,
    headers: &[(&str, &str)],
) -> Value {
    let mut request = http
        .post(format!("{base}/loki/api/v1/push"))
        .header("X-Scope-OrgID", "tenant-a");
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = http
        .execute(request.body(body).build().expect("build Loki push request"))
        .await
        .expect("push invalid payload to Loki");
    let status = response.status().as_u16();
    let body = response.text().await.expect("Loki push error body");
    stable_loki_error(status, &body)
}

async fn crabka_push_error(app: axum::Router, payload: &Value) -> Value {
    crabka_push_raw_error(app, &payload.to_string()).await
}

async fn crabka_push_raw_error(app: axum::Router, payload: &str) -> Value {
    crabka_push_body_result(
        app,
        payload.as_bytes().to_vec(),
        &[("content-type", "application/json")],
    )
    .await
}

async fn crabka_push_body_result(
    app: axum::Router,
    body: Vec<u8>,
    headers: &[(&str, &str)],
) -> Value {
    let mut request = Request::builder()
        .method("POST")
        .uri("/loki/api/v1/push")
        .header("X-Scope-OrgID", "tenant-a");
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = app
        .oneshot(request.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_otlp_error(http: &reqwest::Client, base: &str, payload: &Value) -> Value {
    let response = http
        .post(format!("{base}/otlp/v1/logs"))
        .header("X-Scope-OrgID", "tenant-a")
        .header("content-type", "application/json")
        .body(payload.to_string())
        .send()
        .await
        .expect("push invalid OTLP payload to Loki");
    let status = response.status().as_u16();
    let body = response.text().await.expect("Loki OTLP error body");
    stable_loki_error(status, &body)
}

async fn crabka_otlp_error(app: axum::Router, payload: &Value) -> Value {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/otlp/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_query_result(http: &reqwest::Client, base: &str, query: &str, time_ns: i64) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let response = http
            .get(format!("{base}/loki/api/v1/query"))
            .query(&[("query", query.to_string()), ("time", time_ns.to_string())])
            .send()
            .await
            .expect("query Loki");
        let status = response.status();
        let body_text = response.text().await.expect("Loki response body");
        let body: Value = serde_json::from_str(&body_text).unwrap_or_else(|error| {
            panic!("Loki JSON response: status={status}, body={body_text:?}, error={error}")
        });
        if !body["data"]["result"]
            .as_array()
            .unwrap_or(&Vec::new())
            .is_empty()
        {
            return stable_loki_result(&body);
        }
        assert!(
            Instant::now() < deadline,
            "Loki never returned the instant differential row: {body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn loki_api_prom_query_result(
    http: &reqwest::Client,
    base: &str,
    query: &str,
    time_ns: i64,
) -> Value {
    let response = http
        .get(format!("{base}/api/prom/query"))
        .query(&[("query", query.to_string()), ("time", time_ns.to_string())])
        .send()
        .await
        .expect("query Loki deprecated instant query alias");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki deprecated instant query alias response body");
    if status != 200 {
        return json!({
            "httpStatus": status,
            "body": body,
        });
    }
    let body: Value =
        serde_json::from_str(&body).expect("Loki deprecated instant query alias JSON response");
    stable_loki_result(&body)
}

async fn loki_query_range_result(
    http: &reqwest::Client,
    base: &str,
    query: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let body: Value = http
            .get(format!("{base}/loki/api/v1/query_range"))
            .query(&[
                ("query", query.to_string()),
                ("start", start_ns.to_string()),
                ("end", end_ns.to_string()),
                ("direction", "forward".to_string()),
            ])
            .send()
            .await
            .expect("query Loki")
            .json()
            .await
            .expect("Loki JSON response");
        if !body["data"]["result"]
            .as_array()
            .unwrap_or(&Vec::new())
            .is_empty()
        {
            return stable_loki_result(&body);
        }
        assert!(
            Instant::now() < deadline,
            "Loki never returned the differential row for query {query:?}: {body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn loki_api_prom_query_range_result(
    http: &reqwest::Client,
    base: &str,
    query: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let response = http
            .get(format!("{base}/api/prom/query_range"))
            .query(&[
                ("query", query.to_string()),
                ("start", start_ns.to_string()),
                ("end", end_ns.to_string()),
                ("direction", "forward".to_string()),
            ])
            .send()
            .await
            .expect("query Loki deprecated query_range alias");
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .expect("Loki deprecated query_range alias response body");
        if status != StatusCode::OK.as_u16() {
            return stable_loki_error(status, &body);
        }
        let body: Value =
            serde_json::from_str(&body).expect("Loki deprecated query_range alias JSON response");
        if !body["data"]["result"]
            .as_array()
            .unwrap_or(&Vec::new())
            .is_empty()
        {
            return stable_loki_result(&body);
        }
        assert!(
            Instant::now() < deadline,
            "Loki never returned the deprecated query_range alias row: {body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn loki_query_range_result_with_default_direction_and_limit(
    http: &reqwest::Client,
    base: &str,
    query: &str,
    start_ns: i64,
    end_ns: i64,
    limit: usize,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let body: Value = http
            .get(format!("{base}/loki/api/v1/query_range"))
            .query(&[
                ("query", query.to_string()),
                ("start", start_ns.to_string()),
                ("end", end_ns.to_string()),
                ("limit", limit.to_string()),
            ])
            .send()
            .await
            .expect("query Loki")
            .json()
            .await
            .expect("Loki JSON response");
        if !body["data"]["result"]
            .as_array()
            .unwrap_or(&Vec::new())
            .is_empty()
        {
            return stable_loki_result(&body);
        }
        assert!(
            Instant::now() < deadline,
            "Loki never returned the limited differential row: {body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn loki_query_range_result_with_step(
    http: &reqwest::Client,
    base: &str,
    query: &str,
    start_ns: i64,
    end_ns: i64,
    step: &str,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let response = http
            .get(format!("{base}/loki/api/v1/query_range"))
            .query(&[
                ("query", query.to_string()),
                ("start", start_ns.to_string()),
                ("end", end_ns.to_string()),
                ("step", step.to_string()),
            ])
            .send()
            .await
            .expect("query Loki");
        let status = response.status();
        let body_text = response.text().await.expect("Loki response body");
        let body: Value = serde_json::from_str(&body_text).unwrap_or_else(|error| {
            panic!("Loki JSON response: status={status}, body={body_text:?}, error={error}")
        });
        if !body["data"]["result"]
            .as_array()
            .unwrap_or(&Vec::new())
            .is_empty()
        {
            return stable_loki_result(&body);
        }
        assert!(
            Instant::now() < deadline,
            "Loki never returned the differential metric row: {body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn crabka_query_result(app: axum::Router, query: &str, time_ns: i64) -> Value {
    let uri = format!(
        "/loki/api/v1/query?query={}&time={time_ns}",
        percent_encode_component(query)
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_result(&serde_json::from_slice(&body).unwrap())
}

async fn loki_raw_query_result(http: &reqwest::Client, base: &str, raw_query: &str) -> Value {
    let body: Value = http
        .get(format!("{base}/loki/api/v1/query?{raw_query}"))
        .send()
        .await
        .expect("query Loki raw query")
        .json()
        .await
        .expect("Loki raw query JSON response");
    stable_scalar_or_vector_result(&body)
}

async fn crabka_raw_query_result(app: axum::Router, raw_query: &str) -> Value {
    let uri = format!("/loki/api/v1/query?{raw_query}");
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_scalar_or_vector_result(&serde_json::from_slice(&body).unwrap())
}

async fn crabka_api_prom_query_result(app: axum::Router, query: &str, time_ns: i64) -> Value {
    let uri = format!(
        "/api/prom/query?query={}&time={time_ns}",
        percent_encode_component(query)
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    if status != 200 {
        return json!({
            "httpStatus": status,
            "body": std::str::from_utf8(&body).unwrap(),
        });
    }
    stable_loki_result(&serde_json::from_slice(&body).unwrap())
}

async fn crabka_query_range_result(
    app: axum::Router,
    query: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let uri = format!(
        "/loki/api/v1/query_range?query={}&start={start_ns}&end={end_ns}&direction=forward",
        percent_encode_component(query)
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_result(&serde_json::from_slice(&body).unwrap())
}

async fn crabka_api_prom_query_range_result(
    app: axum::Router,
    query: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let uri = format!(
        "/api/prom/query_range?query={}&start={start_ns}&end={end_ns}&direction=forward",
        percent_encode_component(query)
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    if status != StatusCode::OK.as_u16() {
        return stable_loki_error(status, &String::from_utf8(body.to_vec()).unwrap());
    }
    stable_loki_result(&serde_json::from_slice(&body).unwrap())
}

async fn crabka_query_range_result_with_default_direction_and_limit(
    app: axum::Router,
    query: &str,
    start_ns: i64,
    end_ns: i64,
    limit: usize,
) -> Value {
    let uri = format!(
        "/loki/api/v1/query_range?query={}&start={start_ns}&end={end_ns}&limit={limit}",
        percent_encode_component(query)
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_result(&serde_json::from_slice(&body).unwrap())
}

async fn crabka_query_range_result_with_step(
    app: axum::Router,
    query: &str,
    start_ns: i64,
    end_ns: i64,
    step: &str,
) -> Value {
    let uri = format!(
        "/loki/api/v1/query_range?query={}&start={start_ns}&end={end_ns}&step={}",
        percent_encode_component(query),
        percent_encode_component(step)
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_result(&serde_json::from_slice(&body).unwrap())
}

async fn loki_query_error(http: &reqwest::Client, base: &str, query: &str) -> Value {
    let response = http
        .get(format!("{base}/loki/api/v1/query"))
        .query(&[("query", query.to_string())])
        .send()
        .await
        .expect("query Loki");
    let status = response.status().as_u16();
    let body = response.text().await.expect("Loki error response body");
    stable_loki_error(status, &body)
}

async fn crabka_query_error(app: axum::Router, query: &str) -> Value {
    let uri = format!(
        "/loki/api/v1/query?query={}",
        percent_encode_component(query)
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_post_query_precedence_response(
    http: &reqwest::Client,
    base: &str,
    path: &str,
    raw_query: &str,
    body: &str,
) -> Value {
    let response = http
        .post(format!("{base}{path}?{raw_query}"))
        .header("X-Scope-OrgID", "tenant-a")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(body.to_string())
        .send()
        .await
        .expect("post Loki query precedence request");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki query precedence response body");
    stable_query_or_error_response(status, &body)
}

async fn crabka_post_query_precedence_response(
    app: axum::Router,
    path: &str,
    raw_query: &str,
    body: &str,
) -> Value {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("{path}?{raw_query}"))
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_query_or_error_response(status, std::str::from_utf8(&body).unwrap())
}

fn stable_query_or_error_response(status: u16, body: &str) -> Value {
    if status == StatusCode::OK.as_u16() {
        let body: Value = serde_json::from_str(body).expect("Loki query success JSON response");
        json!({
            "httpStatus": status,
            "body": stable_loki_result(&body),
        })
    } else {
        stable_loki_error(status, body)
    }
}

async fn loki_tail_ws_error(base: &str, query: Option<&str>) -> Value {
    let raw_query = query.map(|query| format!("query={}", percent_encode_component(query)));
    loki_tail_ws_raw_error(base, raw_query.as_deref().unwrap_or_default()).await
}

async fn loki_tail_ws_raw_error(base: &str, raw_query: &str) -> Value {
    let mut uri = base.replacen("http://", "ws://", 1);
    uri.push_str("/loki/api/v1/tail");
    if !raw_query.is_empty() {
        uri.push('?');
        uri.push_str(raw_query);
    }
    let mut request = uri.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("X-Scope-OrgID", "tenant-a".parse().unwrap());
    websocket_error_response(connect_async(request).await)
}

async fn crabka_tail_ws_error(app: axum::Router, query: Option<&str>) -> Value {
    let raw_query = query.map(|query| format!("query={}", percent_encode_component(query)));
    crabka_tail_ws_raw_error(app, raw_query.as_deref().unwrap_or_default()).await
}

async fn crabka_tail_ws_raw_error(app: axum::Router, raw_query: &str) -> Value {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut uri = format!("ws://{addr}/loki/api/v1/tail");
    if !raw_query.is_empty() {
        uri.push('?');
        uri.push_str(raw_query);
    }
    let mut request = uri.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("X-Scope-OrgID", "tenant-a".parse().unwrap());
    let response = websocket_error_response(connect_async(request).await);
    server.abort();
    response
}

fn websocket_error_response(
    result: Result<
        (
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
            axum::http::Response<Option<Vec<u8>>>,
        ),
        TungsteniteError,
    >,
) -> Value {
    match result {
        Ok((_socket, response)) => json!({
            "httpStatus": response.status().as_u16(),
            "contentType": response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .split(';')
                .next()
                .unwrap_or_default(),
            "body": "",
        }),
        Err(TungsteniteError::Http(response)) => {
            let body = response
                .body()
                .as_ref()
                .map(|body| String::from_utf8_lossy(body).to_string())
                .unwrap_or_default();
            json!({
                "httpStatus": response.status().as_u16(),
                "contentType": response
                    .headers()
                    .get("content-type")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .split(';')
                    .next()
                    .unwrap_or_default(),
                "body": body,
            })
        }
        Err(error) => json!({
            "transportError": error.to_string(),
        }),
    }
}

async fn loki_query_limit_error(
    http: &reqwest::Client,
    base: &str,
    query: &str,
    limit: &str,
) -> Value {
    let response = http
        .get(format!("{base}/loki/api/v1/query"))
        .query(&[("query", query.to_string()), ("limit", limit.to_string())])
        .send()
        .await
        .expect("query Loki");
    let status = response.status().as_u16();
    let body = response.text().await.expect("Loki error response body");
    stable_loki_error(status, &body)
}

async fn crabka_query_limit_error(app: axum::Router, query: &str, limit: &str) -> Value {
    let uri = format!(
        "/loki/api/v1/query?query={}&limit={}",
        percent_encode_component(query),
        percent_encode_component(limit)
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_query_range_direction_error(
    http: &reqwest::Client,
    base: &str,
    query: &str,
    direction: &str,
) -> Value {
    let response = http
        .get(format!("{base}/loki/api/v1/query_range"))
        .query(&[
            ("query", query.to_string()),
            ("start", "0".to_string()),
            ("end", "1000000000".to_string()),
            ("direction", direction.to_string()),
        ])
        .send()
        .await
        .expect("query_range Loki");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki query_range error response body");
    if status == 200 {
        return json!({
            "httpStatus": status,
            "result": stable_loki_result(&serde_json::from_str(&body).unwrap()),
        });
    }
    stable_loki_error(status, &body)
}

async fn crabka_query_range_direction_error(
    app: axum::Router,
    query: &str,
    direction: &str,
) -> Value {
    let uri = format!(
        "/loki/api/v1/query_range?query={}&start=0&end=1000000000&direction={}",
        percent_encode_component(query),
        percent_encode_component(direction)
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    if status == 200 {
        return json!({
            "httpStatus": status,
            "result": stable_loki_result(&serde_json::from_slice(&body).unwrap()),
        });
    }
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_query_range_step_error(
    http: &reqwest::Client,
    base: &str,
    query: &str,
    step: &str,
) -> Value {
    let response = http
        .get(format!("{base}/loki/api/v1/query_range"))
        .query(&[
            ("query", query.to_string()),
            ("start", "0".to_string()),
            ("end", "1000000000".to_string()),
            ("step", step.to_string()),
        ])
        .send()
        .await
        .expect("query_range Loki");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki query_range error response body");
    stable_loki_error(status, &body)
}

async fn crabka_query_range_step_error(app: axum::Router, query: &str, step: &str) -> Value {
    let uri = format!(
        "/loki/api/v1/query_range?query={}&start=0&end=1000000000&step={}",
        percent_encode_component(query),
        percent_encode_component(step)
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_query_range_resolution_error(
    http: &reqwest::Client,
    base: &str,
    query: &str,
) -> Value {
    let response = http
        .get(format!("{base}/loki/api/v1/query_range"))
        .query(&[
            ("query", query.to_string()),
            ("start", "0".to_string()),
            ("end", "11001000000000".to_string()),
            ("step", "1s".to_string()),
        ])
        .send()
        .await
        .expect("query_range Loki");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki query_range error response body");
    stable_loki_error(status, &body)
}

async fn crabka_query_range_resolution_error(app: axum::Router, query: &str) -> Value {
    let uri = format!(
        "/loki/api/v1/query_range?query={}&start=0&end=11001000000000&step=1s",
        percent_encode_component(query)
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    if response.status().is_success() {
        return stable_loki_error(status, "<success>");
    }
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_query_range_range_error(http: &reqwest::Client, base: &str, query: &str) -> Value {
    let response = http
        .get(format!("{base}/loki/api/v1/query_range"))
        .query(&[
            ("query", query.to_string()),
            ("start", "0".to_string()),
            ("end", "2595601000000000".to_string()),
            ("step", "1h".to_string()),
        ])
        .send()
        .await
        .expect("query_range Loki");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki query_range range error response body");
    stable_loki_error(status, &body)
}

async fn crabka_query_range_range_error(app: axum::Router, query: &str) -> Value {
    let uri = format!(
        "/loki/api/v1/query_range?query={}&start=0&end=2595601000000000&step=1h",
        percent_encode_component(query)
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    if response.status().is_success() {
        return stable_loki_error(status, "<success>");
    }
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_index_volume_range_step_error(
    http: &reqwest::Client,
    base: &str,
    query: &str,
    step: &str,
) -> Value {
    let response = http
        .get(format!("{base}/loki/api/v1/index/volume_range"))
        .query(&[
            ("query", query.to_string()),
            ("start", "0".to_string()),
            ("end", "1000000000".to_string()),
            ("step", step.to_string()),
        ])
        .header("X-Scope-OrgID", "tenant-a")
        .send()
        .await
        .expect("query Loki index volume range");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki index volume range error response body");
    stable_loki_error(status, &body)
}

async fn crabka_index_volume_range_step_error(app: axum::Router, query: &str, step: &str) -> Value {
    let uri = format!(
        "/loki/api/v1/index/volume_range?query={}&start=0&end=1000000000&step={}",
        percent_encode_component(query),
        percent_encode_component(step)
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_index_volume_aggregate_by_error(
    http: &reqwest::Client,
    base: &str,
    endpoint: &str,
    query: &str,
    aggregate_by: &str,
) -> Value {
    let response = http
        .get(format!("{base}/loki/api/v1/{endpoint}"))
        .query(&[
            ("query", query.to_string()),
            ("start", "0".to_string()),
            ("end", "1000000000".to_string()),
            ("aggregateBy", aggregate_by.to_string()),
        ])
        .header("X-Scope-OrgID", "tenant-a")
        .send()
        .await
        .expect("query Loki index volume");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki index volume error response body");
    stable_loki_error(status, &body)
}

async fn crabka_index_volume_aggregate_by_error(
    app: axum::Router,
    endpoint: &str,
    query: &str,
    aggregate_by: &str,
) -> Value {
    let uri = format!(
        "/loki/api/v1/{endpoint}?query={}&start=0&end=1000000000&aggregateBy={}",
        percent_encode_component(query),
        percent_encode_component(aggregate_by)
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_index_query_error(
    http: &reqwest::Client,
    base: &str,
    endpoint: &str,
    query: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let response = http
        .get(format!("{base}/loki/api/v1/{endpoint}"))
        .query(&[
            ("query", query.to_string()),
            ("start", start_ns.to_string()),
            ("end", end_ns.to_string()),
        ])
        .header("X-Scope-OrgID", "tenant-a")
        .send()
        .await
        .expect("query Loki index endpoint");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki index endpoint error response body");
    stable_loki_error(status, &body)
}

async fn crabka_index_query_error(
    app: axum::Router,
    endpoint: &str,
    query: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let uri = format!(
        "/loki/api/v1/{endpoint}?query={}&start={start_ns}&end={end_ns}",
        percent_encode_component(query),
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_index_stats_post_body_precedence_error(
    http: &reqwest::Client,
    base: &str,
    body: &str,
) -> Value {
    let response = http
        .post(format!("{base}/loki/api/v1/index/stats"))
        .query(&[("start", "not-a-number")])
        .header("X-Scope-OrgID", "tenant-a")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(body.to_owned())
        .send()
        .await
        .expect("post Loki index stats with conflicting query/body params");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki index stats POST precedence error response body");
    stable_loki_error(status, &body)
}

async fn crabka_index_stats_post_body_precedence_error(app: axum::Router, body: &str) -> Value {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/index/stats?start=not-a-number")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body.to_owned()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_index_volume_params_response(
    http: &reqwest::Client,
    base: &str,
    path: &str,
    params: &[(&str, String)],
) -> Value {
    let response = http
        .get(format!("{base}/loki/api/v1/{path}"))
        .query(params)
        .header("X-Scope-OrgID", "tenant-a")
        .send()
        .await
        .expect("query Loki index volume");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki index volume response body");
    stable_index_volume_response(status, &body)
}

async fn crabka_index_volume_params_response(
    app: axum::Router,
    path: &str,
    params: &[(&str, String)],
) -> Value {
    let query = params
        .iter()
        .map(|(name, value)| format!("{name}={}", percent_encode_component(value)))
        .collect::<Vec<_>>()
        .join("&");
    let uri = if query.is_empty() {
        format!("/loki/api/v1/{path}")
    } else {
        format!("/loki/api/v1/{path}?{query}")
    };
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_index_volume_response(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_missing_query_error(
    http: &reqwest::Client,
    base: &str,
    path: &str,
    params: &[(&str, String)],
) -> Value {
    let response = http
        .get(format!("{base}/loki/api/v1/{path}"))
        .query(params)
        .header("X-Scope-OrgID", "tenant-a")
        .send()
        .await
        .expect("query Loki missing query endpoint");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki missing query error response body");
    stable_loki_error(status, &body)
}

async fn crabka_missing_query_error(
    app: axum::Router,
    path: &str,
    params: &[(&str, String)],
) -> Value {
    let query = params
        .iter()
        .map(|(name, value)| format!("{name}={}", percent_encode_component(value)))
        .collect::<Vec<_>>()
        .join("&");
    let uri = if query.is_empty() {
        format!("/loki/api/v1/{path}")
    } else {
        format!("/loki/api/v1/{path}?{query}")
    };
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_detected_fields_step_error(
    http: &reqwest::Client,
    base: &str,
    endpoint: &str,
    query: &str,
    step: &str,
) -> Value {
    let response = http
        .get(format!("{base}/loki/api/v1/{endpoint}"))
        .query(&[
            ("query", query.to_string()),
            ("start", "0".to_string()),
            ("end", "1000000000".to_string()),
            ("step", step.to_string()),
        ])
        .header("X-Scope-OrgID", "tenant-a")
        .send()
        .await
        .expect("query Loki detected fields");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki detected fields error response body");
    stable_loki_error(status, &body)
}

async fn crabka_detected_fields_step_error(
    app: axum::Router,
    endpoint: &str,
    query: &str,
    step: &str,
) -> Value {
    let uri = format!(
        "/loki/api/v1/{endpoint}?query={}&start=0&end=1000000000&step={}",
        percent_encode_component(query),
        percent_encode_component(step)
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_detected_fields_query_error(
    http: &reqwest::Client,
    base: &str,
    endpoint: &str,
    query: &str,
) -> Value {
    let response = http
        .get(format!("{base}/loki/api/v1/{endpoint}"))
        .query(&[
            ("query", query.to_string()),
            ("start", "0".to_string()),
            ("end", "1000000000".to_string()),
        ])
        .header("X-Scope-OrgID", "tenant-a")
        .send()
        .await
        .expect("query Loki detected fields");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki detected fields error response body");
    stable_loki_error(status, &body)
}

async fn crabka_detected_fields_query_error(
    app: axum::Router,
    endpoint: &str,
    query: &str,
) -> Value {
    let uri = format!(
        "/loki/api/v1/{endpoint}?query={}&start=0&end=1000000000",
        percent_encode_component(query)
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_detected_endpoint_range_error(
    http: &reqwest::Client,
    base: &str,
    endpoint: &str,
    query: Option<&str>,
) -> Value {
    let mut params = vec![
        ("start", "0".to_string()),
        ("end", "2595601000000000".to_string()),
    ];
    if let Some(query) = query {
        params.push(("query", query.to_string()));
    }
    let response = http
        .get(format!("{base}/loki/api/v1/{endpoint}"))
        .query(&params)
        .header("X-Scope-OrgID", "tenant-a")
        .send()
        .await
        .expect("query Loki detected endpoint");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki detected endpoint range error response body");
    stable_loki_error(status, &body)
}

async fn crabka_detected_endpoint_range_error(
    app: axum::Router,
    endpoint: &str,
    query: Option<&str>,
) -> Value {
    let query_param = query
        .map(|query| format!("&query={}", percent_encode_component(query)))
        .unwrap_or_default();
    let uri = format!("/loki/api/v1/{endpoint}?start=0&end=2595601000000000{query_param}");
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_detected_endpoint_duplicate_start_error(
    http: &reqwest::Client,
    base: &str,
    endpoint: &str,
    query: Option<&str>,
) -> Value {
    let mut params = vec![
        ("start", "0".to_string()),
        ("start", "not-a-number".to_string()),
        ("end", "2595601000000000".to_string()),
    ];
    if let Some(query) = query {
        params.push(("query", query.to_string()));
    }
    let response = http
        .get(format!("{base}/loki/api/v1/{endpoint}"))
        .query(&params)
        .header("X-Scope-OrgID", "tenant-a")
        .send()
        .await
        .expect("query Loki detected endpoint with duplicate start");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki detected endpoint duplicate start error response body");
    stable_loki_error(status, &body)
}

async fn crabka_detected_endpoint_duplicate_start_error(
    app: axum::Router,
    endpoint: &str,
    query: Option<&str>,
) -> Value {
    let query_param = query
        .map(|query| format!("&query={}", percent_encode_component(query)))
        .unwrap_or_default();
    let uri = format!(
        "/loki/api/v1/{endpoint}?start=0&start=not-a-number&end=2595601000000000{query_param}"
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_detected_endpoint_post_body_precedence_error(
    http: &reqwest::Client,
    base: &str,
    endpoint: &str,
    query: &str,
) -> Value {
    let body = format!(
        "start=0&end=2595601000000000&query={}",
        percent_encode_component(query)
    );
    let response = http
        .post(format!("{base}/loki/api/v1/{endpoint}"))
        .query(&[("start", "not-a-number")])
        .header("X-Scope-OrgID", "tenant-a")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .expect("post Loki detected endpoint with conflicting query/body params");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki detected endpoint POST precedence error response body");
    stable_loki_error(status, &body)
}

async fn crabka_detected_endpoint_post_body_precedence_error(
    app: axum::Router,
    endpoint: &str,
    query: &str,
) -> Value {
    let body = format!(
        "start=0&end=2595601000000000&query={}",
        percent_encode_component(query)
    );
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/loki/api/v1/{endpoint}?start=not-a-number"))
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_query_range_start_error(
    http: &reqwest::Client,
    base: &str,
    query: &str,
    start: &str,
) -> Value {
    let response = http
        .get(format!("{base}/loki/api/v1/query_range"))
        .query(&[
            ("query", query.to_string()),
            ("start", start.to_string()),
            ("end", "1000000000".to_string()),
        ])
        .send()
        .await
        .expect("query_range Loki");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki query_range error response body");
    stable_loki_error(status, &body)
}

async fn crabka_query_range_start_error(app: axum::Router, query: &str, start: &str) -> Value {
    let uri = format!(
        "/loki/api/v1/query_range?query={}&start={}&end=1000000000",
        percent_encode_component(query),
        percent_encode_component(start)
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_query_range_since_error(
    http: &reqwest::Client,
    base: &str,
    query: &str,
    since: &str,
) -> Value {
    let response = http
        .get(format!("{base}/loki/api/v1/query_range"))
        .query(&[
            ("query", query.to_string()),
            ("end", "1000000000".to_string()),
            ("since", since.to_string()),
        ])
        .send()
        .await
        .expect("query_range Loki");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki query_range error response body");
    stable_loki_error(status, &body)
}

async fn crabka_query_range_since_error(app: axum::Router, query: &str, since: &str) -> Value {
    let uri = format!(
        "/loki/api/v1/query_range?query={}&end=1000000000&since={}",
        percent_encode_component(query),
        percent_encode_component(since)
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_query_range_interval_result(
    http: &reqwest::Client,
    base: &str,
    query: &str,
    start_ns: i64,
    end_ns: i64,
    interval: &str,
) -> Value {
    let response = http
        .get(format!("{base}/loki/api/v1/query_range"))
        .query(&[
            ("query", query.to_string()),
            ("start", start_ns.to_string()),
            ("end", end_ns.to_string()),
            ("interval", interval.to_string()),
        ])
        .send()
        .await
        .expect("query_range Loki");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki query_range error response body");
    if status == 200 {
        return json!({
            "httpStatus": status,
            "result": stable_loki_result(&serde_json::from_str(&body).unwrap()),
        });
    }
    stable_loki_error(status, &body)
}

async fn crabka_query_range_interval_result(
    app: axum::Router,
    query: &str,
    start_ns: i64,
    end_ns: i64,
    interval: &str,
) -> Value {
    let uri = format!(
        "/loki/api/v1/query_range?query={}&start={start_ns}&end={end_ns}&interval={}",
        percent_encode_component(query),
        percent_encode_component(interval)
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    if status == 200 {
        return json!({
            "httpStatus": status,
            "result": stable_loki_result(&serde_json::from_slice(&body).unwrap()),
        });
    }
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_buildinfo_result(http: &reqwest::Client, base: &str) -> Value {
    let body = http
        .get(format!("{base}/loki/api/v1/status/buildinfo"))
        .send()
        .await
        .expect("query Loki buildinfo")
        .json()
        .await
        .expect("Loki buildinfo JSON response");
    stable_buildinfo_result(&body)
}

async fn crabka_buildinfo_result(app: axum::Router) -> Value {
    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/status/buildinfo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_buildinfo_result(&serde_json::from_slice(&body).unwrap())
}

async fn loki_status_probe_result(http: &reqwest::Client, base: &str, path: &str) -> Value {
    let response = http
        .get(format!("{base}{path}"))
        .send()
        .await
        .expect("query Loki status probe");
    let status = response.status().as_u16();
    let body = response.text().await.expect("Loki status probe body");
    stable_status_probe_response(status, &body)
}

async fn crabka_status_probe_result(app: axum::Router, path: &str) -> Value {
    let response = app
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_status_probe_response(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_config_result(http: &reqwest::Client, base: &str) -> Value {
    loki_config_result_with_query(http, base, "").await
}

async fn loki_config_result_with_query(
    http: &reqwest::Client,
    base: &str,
    raw_query: &str,
) -> Value {
    let suffix = if raw_query.is_empty() {
        String::new()
    } else {
        format!("?{raw_query}")
    };
    let response = http
        .get(format!("{base}/config{suffix}"))
        .send()
        .await
        .expect("query Loki config");
    let status = response.status().as_u16();
    let body = response.text().await.expect("Loki config body");
    stable_config_response(status, &body)
}

async fn crabka_config_result(app: axum::Router) -> Value {
    crabka_config_result_with_query(app, "").await
}

async fn crabka_config_result_with_query(app: axum::Router, raw_query: &str) -> Value {
    let uri = if raw_query.is_empty() {
        "/config".to_string()
    } else {
        format!("/config?{raw_query}")
    };
    let response = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_config_response(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_metrics_result(http: &reqwest::Client, base: &str) -> Value {
    let response = http
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("query Loki metrics");
    let status = response.status().as_u16();
    let body = response.text().await.expect("Loki metrics body");
    stable_metrics_response(status, &body)
}

async fn crabka_metrics_result(app: axum::Router) -> Value {
    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_metrics_response(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_log_level_post_result(
    http: &reqwest::Client,
    base: &str,
    raw_query: Option<&str>,
    form_body: Option<&str>,
) -> Value {
    let mut url = format!("{base}/log_level");
    if let Some(raw_query) = raw_query {
        url.push('?');
        url.push_str(raw_query);
    }
    let mut request = http.post(url);
    if let Some(form_body) = form_body {
        request = request
            .header("content-type", "application/x-www-form-urlencoded")
            .body(form_body.to_owned());
    }
    let response = request.send().await.expect("post Loki log level");
    let status = response.status().as_u16();
    let body = response.text().await.expect("Loki log level body");
    stable_status_probe_response(status, &body)
}

async fn crabka_log_level_post_result(
    app: axum::Router,
    raw_query: Option<&str>,
    form_body: Option<&str>,
) -> Value {
    let mut uri = "/log_level".to_owned();
    if let Some(raw_query) = raw_query {
        uri.push('?');
        uri.push_str(raw_query);
    }
    let mut builder = Request::builder().method("POST").uri(uri);
    if form_body.is_some() {
        builder = builder.header("content-type", "application/x-www-form-urlencoded");
    }
    let response = app
        .oneshot(
            builder
                .body(Body::from(form_body.unwrap_or_default().to_owned()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_status_probe_response(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_ingester_control_result(
    http: &reqwest::Client,
    base: &str,
    method: &str,
    path: &str,
) -> Value {
    let url = format!("{base}{path}");
    let response = match method {
        "GET" => http.get(url).send().await,
        "POST" => http.post(url).send().await,
        "DELETE" => http.delete(url).send().await,
        _ => panic!("unsupported method {method}"),
    }
    .expect("query Loki ingester control endpoint");
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .unwrap_or_default();
    let body = response.text().await.expect("Loki ingester control body");
    stable_lifecycle_control_response(status, &content_type, &body)
}

async fn crabka_ingester_control_result(app: axum::Router, method: &str, path: &str) -> Value {
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .unwrap_or_default();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_lifecycle_control_response(status, &content_type, std::str::from_utf8(&body).unwrap())
}

async fn loki_ruler_inventory_result(http: &reqwest::Client, base: &str, path: &str) -> Value {
    let response = http
        .get(format!("{base}{path}"))
        .send()
        .await
        .expect("query Loki ruler inventory");
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = response
        .text()
        .await
        .expect("Loki ruler inventory response body");
    stable_ruler_inventory_response(status, &content_type, &body)
}

async fn crabka_ruler_inventory_result(app: axum::Router, path: &str) -> Value {
    let response = app
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_ruler_inventory_response(status, &content_type, std::str::from_utf8(&body).unwrap())
}

async fn loki_ring_status_result(http: &reqwest::Client, base: &str, path: &str) -> Value {
    let response = http
        .get(format!("{base}{path}"))
        .send()
        .await
        .expect("query Loki ring status");
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = response.text().await.expect("Loki ring response body");
    stable_ring_status_response(status, &content_type, &body)
}

async fn crabka_ring_status_result(app: axum::Router, path: &str) -> Value {
    let response = app
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_ring_status_response(status, &content_type, std::str::from_utf8(&body).unwrap())
}

async fn loki_delete_lifecycle_result(
    http: &reqwest::Client,
    base: &str,
    query: &str,
    start: i64,
    end: i64,
) -> Value {
    let encoded_query = percent_encode_component(query);
    let create_path = format!("/loki/api/v1/delete?query={encoded_query}&start={start}&end={end}");
    let create = loki_delete_request(http, base, reqwest::Method::POST, &create_path).await;
    let (list_after_create, request_id) =
        loki_delete_list_request(http, base, "/loki/api/v1/delete").await;
    let cancel = if let Some(request_id) = request_id {
        let request_id = percent_encode_component(&request_id);
        let cancel_path = format!("/loki/api/v1/delete?request_id={request_id}");
        loki_delete_request(http, base, reqwest::Method::DELETE, &cancel_path).await
    } else {
        json!({
            "httpStatus": 0,
            "contentType": "",
            "body": "<missing-request-id>",
        })
    };
    let (list_after_cancel, _) = loki_delete_list_request(http, base, "/loki/api/v1/delete").await;

    json!({
        "create": create,
        "listAfterCreate": list_after_create,
        "cancel": cancel,
        "listAfterCancel": list_after_cancel,
    })
}

async fn loki_delete_request(
    http: &reqwest::Client,
    base: &str,
    method: reqwest::Method,
    path: &str,
) -> Value {
    let response = http
        .request(method, format!("{base}{path}"))
        .header("X-Scope-OrgID", "tenant-a")
        .send()
        .await
        .expect("query Loki delete API");
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = response.text().await.expect("Loki delete response body");
    stable_delete_response(status, &content_type, &body)
}

async fn loki_delete_list_request(
    http: &reqwest::Client,
    base: &str,
    path: &str,
) -> (Value, Option<String>) {
    let response = http
        .request(reqwest::Method::GET, format!("{base}{path}"))
        .header("X-Scope-OrgID", "tenant-a")
        .send()
        .await
        .expect("query Loki delete API");
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = response.text().await.expect("Loki delete response body");
    let request_id = raw_delete_request_id(&body);
    (
        stable_delete_response(status, &content_type, &body),
        request_id,
    )
}

async fn crabka_delete_lifecycle_result(
    app: axum::Router,
    query: &str,
    start: i64,
    end: i64,
) -> Value {
    let encoded_query = percent_encode_component(query);
    let create_path = format!("/loki/api/v1/delete?query={encoded_query}&start={start}&end={end}");
    let create = crabka_delete_request(app.clone(), "POST", &create_path).await;
    let (list_after_create, request_id) =
        crabka_delete_list_request(app.clone(), "/loki/api/v1/delete").await;
    let cancel = if let Some(request_id) = request_id {
        let request_id = percent_encode_component(&request_id);
        let cancel_path = format!("/loki/api/v1/delete?request_id={request_id}");
        crabka_delete_request(app.clone(), "DELETE", &cancel_path).await
    } else {
        json!({
            "httpStatus": 0,
            "contentType": "",
            "body": "<missing-request-id>",
        })
    };
    let (list_after_cancel, _) = crabka_delete_list_request(app, "/loki/api/v1/delete").await;

    json!({
        "create": create,
        "listAfterCreate": list_after_create,
        "cancel": cancel,
        "listAfterCancel": list_after_cancel,
    })
}

async fn crabka_delete_request(app: axum::Router, method: &str, path: &str) -> Value {
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_delete_response(status, &content_type, std::str::from_utf8(&body).unwrap())
}

async fn crabka_delete_list_request(app: axum::Router, path: &str) -> (Value, Option<String>) {
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(path)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    let request_id = raw_delete_request_id(body);
    (
        stable_delete_response(status, &content_type, body),
        request_id,
    )
}

async fn loki_format_query_result(http: &reqwest::Client, base: &str, query: &str) -> Value {
    http.get(format!("{base}/loki/api/v1/format_query"))
        .query(&[("query", query.to_string())])
        .send()
        .await
        .expect("format Loki query")
        .json()
        .await
        .expect("Loki format_query JSON response")
}

async fn crabka_format_query_result(app: axum::Router, query: &str) -> Value {
    let uri = format!(
        "/loki/api/v1/format_query?query={}",
        percent_encode_component(query)
    );
    let response = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn loki_format_query_post_result(
    http: &reqwest::Client,
    base: &str,
    query: &str,
    form_body: &str,
) -> Value {
    http.post(format!("{base}/loki/api/v1/format_query"))
        .query(&[("query", query.to_string())])
        .header("content-type", "application/x-www-form-urlencoded")
        .body(form_body.to_owned())
        .send()
        .await
        .expect("post format Loki query")
        .json()
        .await
        .expect("Loki format_query POST JSON response")
}

async fn crabka_format_query_post_result(app: axum::Router, query: &str, form_body: &str) -> Value {
    let uri = format!(
        "/loki/api/v1/format_query?query={}",
        percent_encode_component(query)
    );
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(form_body.to_owned()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn loki_format_query_error(http: &reqwest::Client, base: &str, query: Option<&str>) -> Value {
    let mut request = http.get(format!("{base}/loki/api/v1/format_query"));
    if let Some(query) = query {
        request = request.query(&[("query", query.to_string())]);
    }
    let response = request.send().await.expect("format Loki query");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki format_query error response body");
    stable_loki_error(status, &body)
}

async fn crabka_format_query_error(app: axum::Router, query: Option<&str>) -> Value {
    let uri = if let Some(query) = query {
        format!(
            "/loki/api/v1/format_query?query={}",
            percent_encode_component(query)
        )
    } else {
        "/loki/api/v1/format_query".to_string()
    };
    let response = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_metadata_error(http: &reqwest::Client, base: &str, path: &str) -> Value {
    let response = http
        .get(format!("{base}/loki/api/v1/{path}"))
        .header("X-Scope-OrgID", "tenant-a")
        .send()
        .await
        .expect("query Loki metadata");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki metadata error response body");
    stable_loki_error(status, &body)
}

async fn crabka_metadata_error(app: axum::Router, path: &str) -> Value {
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/loki/api/v1/{path}"))
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_metadata_post_body_precedence_error(
    http: &reqwest::Client,
    base: &str,
    path: &str,
    body: &str,
) -> Value {
    let response = http
        .post(format!("{base}{path}"))
        .query(&[("start", "not-a-number")])
        .header("X-Scope-OrgID", "tenant-a")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(body.to_owned())
        .send()
        .await
        .expect("post Loki metadata with conflicting query/body params");
    let status = response.status().as_u16();
    let body = response
        .text()
        .await
        .expect("Loki metadata POST precedence error response body");
    stable_loki_error(status, &body)
}

async fn crabka_metadata_post_body_precedence_error(
    app: axum::Router,
    path: &str,
    body: &str,
) -> Value {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("{path}?start=not-a-number"))
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body.to_owned()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_raw_path_error(http: &reqwest::Client, base: &str, path: &str) -> Value {
    let response = http
        .get(format!("{base}{path}"))
        .header("X-Scope-OrgID", "tenant-a")
        .send()
        .await
        .expect("query Loki raw path");
    let status = response.status().as_u16();
    let body = response.text().await.expect("Loki raw path body");
    stable_loki_error(status, &body)
}

async fn crabka_raw_path_error(app: axum::Router, path: &str) -> Value {
    let response = app
        .oneshot(
            Request::builder()
                .uri(path)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_raw_post_path_error(http: &reqwest::Client, base: &str, path: &str) -> Value {
    let response = http
        .post(format!("{base}{path}"))
        .header("X-Scope-OrgID", "tenant-a")
        .send()
        .await
        .expect("post Loki raw path");
    let status = response.status().as_u16();
    let body = response.text().await.expect("Loki raw post path body");
    stable_loki_error(status, &body)
}

async fn crabka_raw_post_path_error(app: axum::Router, path: &str) -> Value {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(path)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_loki_error(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_metadata_result(
    http: &reqwest::Client,
    base: &str,
    path: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let separator = if path.contains('?') { '&' } else { '?' };
        let body: Value = http
            .get(format!(
                "{base}/loki/api/v1/{path}{separator}start={start_ns}&end={end_ns}"
            ))
            .header("X-Scope-OrgID", "tenant-a")
            .send()
            .await
            .expect("query Loki metadata")
            .json()
            .await
            .expect("Loki metadata JSON response");
        if metadata_result_is_populated(&body) {
            return stable_metadata_result(&body);
        }
        assert!(
            Instant::now() < deadline,
            "Loki never returned the differential metadata row: {body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn crabka_metadata_result(
    app: axum::Router,
    path: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let separator = if path.contains('?') { '&' } else { '?' };
    let uri = format!("/loki/api/v1/{path}{separator}start={start_ns}&end={end_ns}");
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_metadata_result(&serde_json::from_slice(&body).unwrap())
}

async fn loki_metadata_post_result(
    http: &reqwest::Client,
    base: &str,
    path: &str,
    form_body: Option<&str>,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let separator = if path.contains('?') { '&' } else { '?' };
        let mut request = http
            .post(format!(
                "{base}/loki/api/v1/{path}{separator}start={start_ns}&end={end_ns}"
            ))
            .header("X-Scope-OrgID", "tenant-a");
        if let Some(form_body) = form_body {
            request = request
                .header("content-type", "application/x-www-form-urlencoded")
                .body(form_body.to_owned());
        }
        let body: Value = request
            .send()
            .await
            .expect("post Loki metadata")
            .json()
            .await
            .expect("Loki metadata POST JSON response");
        if metadata_result_is_populated(&body) {
            return stable_metadata_result(&body);
        }
        assert!(
            Instant::now() < deadline,
            "Loki never returned the differential metadata POST row: {body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn crabka_metadata_post_result(
    app: axum::Router,
    path: &str,
    form_body: Option<&str>,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let separator = if path.contains('?') { '&' } else { '?' };
    let uri = format!("/loki/api/v1/{path}{separator}start={start_ns}&end={end_ns}");
    let mut builder = Request::builder()
        .method("POST")
        .uri(uri)
        .header("X-Scope-OrgID", "tenant-a");
    if form_body.is_some() {
        builder = builder.header("content-type", "application/x-www-form-urlencoded");
    }
    let response = app
        .oneshot(
            builder
                .body(Body::from(form_body.unwrap_or_default().to_owned()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_metadata_result(&serde_json::from_slice(&body).unwrap())
}

async fn loki_json_path_result(http: &reqwest::Client, base: &str, path: &str) -> Value {
    http.get(format!("{base}{path}"))
        .header("X-Scope-OrgID", "tenant-a")
        .send()
        .await
        .expect("query Loki JSON path")
        .json()
        .await
        .expect("Loki JSON path response")
}

async fn crabka_json_path_result(app: axum::Router, path: &str) -> Value {
    let response = app
        .oneshot(
            Request::builder()
                .uri(path)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::OK, "{path}");
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn loki_api_prom_metadata_result(
    http: &reqwest::Client,
    base: &str,
    path: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let separator = if path.contains('?') { '&' } else { '?' };
        let response = http
            .get(format!(
                "{base}/api/prom/{path}{separator}start={start_ns}&end={end_ns}"
            ))
            .header("X-Scope-OrgID", "tenant-a")
            .send()
            .await
            .expect("query Loki deprecated metadata alias");
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .expect("Loki deprecated metadata alias response body");
        if status != StatusCode::OK.as_u16() {
            return stable_loki_error(status, &body);
        }
        let body: Value =
            serde_json::from_str(&body).expect("Loki deprecated metadata alias JSON response");
        let stable = stable_api_prom_metadata_result(&body);
        if api_prom_metadata_result_is_populated(&stable) {
            return stable;
        }
        assert!(
            Instant::now() < deadline,
            "Loki never returned the deprecated metadata alias row: {body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn crabka_api_prom_metadata_result(
    app: axum::Router,
    path: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let separator = if path.contains('?') { '&' } else { '?' };
    let uri = format!("/api/prom/{path}{separator}start={start_ns}&end={end_ns}");
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_api_prom_metadata_result(&serde_json::from_slice(&body).unwrap())
}

async fn loki_detected_fields_result(
    http: &reqwest::Client,
    base: &str,
    path: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let separator = if path.contains('?') { '&' } else { '?' };
        let body: Value = http
            .get(format!(
                "{base}/loki/api/v1/{path}{separator}start={start_ns}&end={end_ns}",
            ))
            .header("X-Scope-OrgID", "tenant-a")
            .send()
            .await
            .expect("query Loki detected fields")
            .json()
            .await
            .expect("Loki detected fields JSON response");
        let stable = stable_detected_fields_result(&body);
        if detected_fields_result_is_populated(&stable) {
            return stable;
        }
        assert!(
            Instant::now() < deadline,
            "Loki never returned the differential detected-fields row: {body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn crabka_detected_fields_result(
    app: axum::Router,
    path: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let separator = if path.contains('?') { '&' } else { '?' };
    let uri = format!("/loki/api/v1/{path}{separator}start={start_ns}&end={end_ns}");
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_detected_fields_result(&serde_json::from_slice(&body).unwrap())
}

async fn loki_detected_labels_result(
    http: &reqwest::Client,
    base: &str,
    path: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let separator = if path.contains('?') { '&' } else { '?' };
        let body: Value = http
            .get(format!(
                "{base}/loki/api/v1/{path}{separator}start={start_ns}&end={end_ns}",
            ))
            .header("X-Scope-OrgID", "tenant-a")
            .send()
            .await
            .expect("query Loki detected labels")
            .json()
            .await
            .expect("Loki detected labels JSON response");
        let stable = stable_detected_labels_result(&body);
        if detected_labels_result_is_populated(&stable) {
            return stable;
        }
        assert!(
            Instant::now() < deadline,
            "Loki never returned the differential detected-labels row: {body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn crabka_detected_labels_result(
    app: axum::Router,
    path: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let separator = if path.contains('?') { '&' } else { '?' };
    let uri = format!("/loki/api/v1/{path}{separator}start={start_ns}&end={end_ns}");
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    assert!(
        status == StatusCode::OK,
        "Crabka detected_labels failed: {}",
        std::str::from_utf8(&body).unwrap()
    );
    stable_detected_labels_result(&serde_json::from_slice(&body).unwrap())
}

async fn loki_patterns_default_response(
    http: &reqwest::Client,
    base: &str,
    path: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let separator = if path.contains('?') { '&' } else { '?' };
    let response = http
        .get(format!(
            "{base}/loki/api/v1/{path}{separator}start={start_ns}&end={end_ns}",
        ))
        .header("X-Scope-OrgID", "tenant-a")
        .send()
        .await
        .expect("query Loki patterns");
    let status = response.status().as_u16();
    let body = response.text().await.expect("Loki patterns response body");
    stable_patterns_response(status, &body)
}

async fn crabka_patterns_result(
    app: axum::Router,
    path: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let separator = if path.contains('?') { '&' } else { '?' };
    let uri = format!("/loki/api/v1/{path}{separator}start={start_ns}&end={end_ns}");
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_patterns_response(status, std::str::from_utf8(&body).unwrap())
}

async fn loki_index_stats_result(
    http: &reqwest::Client,
    base: &str,
    path: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let separator = if path.contains('?') { '&' } else { '?' };
    let body: Value = http
        .get(format!(
            "{base}/loki/api/v1/{path}{separator}start={start_ns}&end={end_ns}",
        ))
        .header("X-Scope-OrgID", "tenant-a")
        .send()
        .await
        .expect("query Loki index stats")
        .json()
        .await
        .expect("Loki index stats JSON response");
    stable_index_stats_result(&body)
}

async fn crabka_index_stats_result(
    app: axum::Router,
    path: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let separator = if path.contains('?') { '&' } else { '?' };
    let uri = format!("/loki/api/v1/{path}{separator}start={start_ns}&end={end_ns}");
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_index_stats_result(&serde_json::from_slice(&body).unwrap())
}

async fn loki_index_volume_result(
    http: &reqwest::Client,
    base: &str,
    path: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let separator = if path.contains('?') { '&' } else { '?' };
        let body: Value = http
            .get(format!(
                "{base}/loki/api/v1/{path}{separator}start={start_ns}&end={end_ns}",
            ))
            .header("X-Scope-OrgID", "tenant-a")
            .send()
            .await
            .expect("query Loki index volume")
            .json()
            .await
            .expect("Loki index volume JSON response");
        let stable = stable_index_volume_result(&body);
        if volume_result_is_populated(&stable) {
            return stable;
        }
        assert!(
            Instant::now() < deadline,
            "Loki never returned the differential index-volume row: {body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn crabka_index_volume_result(
    app: axum::Router,
    path: &str,
    start_ns: i64,
    end_ns: i64,
) -> Value {
    let separator = if path.contains('?') { '&' } else { '?' };
    let uri = format!("/loki/api/v1/{path}{separator}start={start_ns}&end={end_ns}");
    let response = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    stable_index_volume_result(&serde_json::from_slice(&body).unwrap())
}

fn stable_loki_result(body: &Value) -> Value {
    json!({
        "resultType": body["data"]["resultType"].clone(),
        "result": body["data"]["result"].clone()
    })
}

fn stable_scalar_or_vector_result(body: &Value) -> Value {
    let mut result = body["data"]["result"].clone();
    if let Some(series) = result.as_array_mut() {
        for series in series {
            if let Some(value) = series.get_mut("value").and_then(Value::as_array_mut)
                && value.len() == 2
            {
                value[0] = json!("<timestamp>");
            }
            if let Some(values) = series.get_mut("values").and_then(Value::as_array_mut) {
                for value in values {
                    if let Some(value) = value.as_array_mut()
                        && value.len() == 2
                    {
                        value[0] = json!("<timestamp>");
                    }
                }
            }
        }
    }
    json!({
        "resultType": body["data"]["resultType"].clone(),
        "result": result
    })
}

fn stable_loki_error(status: u16, body: &str) -> Value {
    json!({
        "httpStatus": status,
        "body": stable_loki_error_body(body),
    })
}

fn stable_loki_error_body(body: &str) -> String {
    if let Ok(value) = serde_json::from_str::<Value>(body) {
        return serde_json::to_string(&value).expect("serialize stable Loki error body");
    }
    let oldest_marker = "oldest acceptable timestamp is: ";
    if let Some(marker_start) = body.find(oldest_marker) {
        let stable_prefix = marker_start + oldest_marker.len();
        return format!("{}<oldest>\n", &body[..stable_prefix]);
    }
    let query_range_marker = "the query time range exceeds the limit (query length: ";
    if let Some(marker_start) = body.find(query_range_marker)
        && let Some(limit_start) = body[marker_start..].find(", limit: ")
    {
        let limit_start = marker_start + limit_start;
        return format!(
            "{}<query-length>{}",
            &body[..marker_start + query_range_marker.len()],
            &body[limit_start..]
        );
    }
    body.to_string()
}

fn stable_buildinfo_result(body: &Value) -> Value {
    let Some(fields) = body.as_object() else {
        return body.clone();
    };
    Value::Object(
        fields
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    if value.as_str().is_some() {
                        json!("<string>")
                    } else {
                        value.clone()
                    },
                )
            })
            .collect(),
    )
}

fn stable_status_probe_response(status: u16, body: &str) -> Value {
    let body = serde_json::from_str::<Value>(body).unwrap_or_else(|_| json!(body));
    json!({
        "httpStatus": status,
        "body": stable_status_probe_body(body),
    })
}

fn stable_status_probe_body(body: Value) -> Value {
    let Value::String(body) = body else {
        return body;
    };
    if body.lines().all(|line| line.contains("=> Running")) {
        let mut lines = body.lines().map(str::to_owned).collect::<Vec<String>>();
        lines.sort();
        return json!(lines);
    }
    json!(body)
}

fn stable_config_response(status: u16, body: &str) -> Value {
    let mut lines = body
        .lines()
        .filter_map(|line| {
            (line.starts_with("target:") || line.starts_with("auth_enabled:"))
                .then(|| line.trim().to_owned())
        })
        .collect::<Vec<String>>();
    lines.sort();
    json!({
        "httpStatus": status,
        "body": if lines.is_empty() { body } else { "" },
        "stableLines": lines,
    })
}

fn stable_metrics_response(status: u16, body: &str) -> Value {
    let selected = ["loki_boltdb_shipper_compactor_running", "loki_build_info"];
    let mut families = body
        .lines()
        .filter_map(metric_family_name)
        .filter(|name| selected.contains(name))
        .map(str::to_owned)
        .collect::<Vec<String>>();
    families.sort();
    families.dedup();
    json!({
        "httpStatus": status,
        "metricFamilies": families,
    })
}

fn metric_family_name(line: &str) -> Option<&str> {
    if let Some(rest) = line
        .strip_prefix("# HELP ")
        .or_else(|| line.strip_prefix("# TYPE "))
    {
        return rest.split_whitespace().next();
    }
    if line.starts_with('#') || line.is_empty() {
        return None;
    }
    Some(
        line.split_once('{')
            .map_or(line, |(name, _)| name)
            .split_whitespace()
            .next()
            .unwrap_or_default(),
    )
}

fn stable_ruler_inventory_response(status: u16, content_type: &str, body: &str) -> Value {
    let body = serde_json::from_str::<Value>(body)
        .map(|value| stable_ruler_inventory_body(&value))
        .unwrap_or_else(|_| json!(body));
    json!({
        "httpStatus": status,
        "contentType": content_type.split(';').next().unwrap_or_default(),
        "body": body,
    })
}

fn stable_ruler_inventory_body(body: &Value) -> Value {
    body.clone()
}

fn stable_delete_response(status: u16, content_type: &str, body: &str) -> Value {
    let body = serde_json::from_str::<Value>(body)
        .map(|value| stable_delete_body(&value))
        .unwrap_or_else(|_| json!(body));
    json!({
        "httpStatus": status,
        "contentType": content_type.split(';').next().unwrap_or_default(),
        "body": body,
    })
}

fn stable_lifecycle_control_response(status: u16, content_type: &str, body: &str) -> Value {
    json!({
        "httpStatus": status,
        "contentType": content_type.split(';').next().unwrap_or_default(),
        "body": body,
    })
}

fn stable_delete_body(body: &Value) -> Value {
    if let Some(requests) = body.as_array() {
        let mut requests = requests
            .iter()
            .map(stable_delete_request)
            .collect::<Vec<Value>>();
        requests.sort_by_key(|request| request.to_string());
        return json!(requests);
    }
    body.clone()
}

fn stable_delete_request(request: &Value) -> Value {
    let Some(fields) = request.as_object() else {
        return request.clone();
    };
    Value::Object(
        fields
            .iter()
            .map(|(key, value)| {
                let value = match key.as_str() {
                    "request_id" => json!("<request-id>"),
                    "created_at" => json!("<created-at>"),
                    _ => value.clone(),
                };
                (key.clone(), value)
            })
            .collect(),
    )
}

fn raw_delete_request_id(body: &str) -> Option<String> {
    serde_json::from_str::<Value>(body)
        .ok()?
        .as_array()
        .and_then(|requests| requests.first())
        .and_then(|request| request["request_id"].as_str())
        .map(str::to_owned)
}

fn delete_not_found_response() -> Value {
    json!({
        "httpStatus": 404,
        "contentType": "text/plain",
        "body": "404 page not found\n",
    })
}

fn stable_ring_status_response(status: u16, content_type: &str, body: &str) -> Value {
    let mut tokens = [
        "ACTIVE",
        "Healthy",
        "JOINING",
        "LEAVING",
        "PENDING",
        "Running",
        "UNHEALTHY",
        "Unhealthy",
    ]
    .into_iter()
    .filter(|token| body.contains(token))
    .collect::<Vec<&str>>();
    tokens.sort();
    let headings = html_headings(body);
    json!({
        "httpStatus": status,
        "contentType": content_type.split(';').next().unwrap_or_default(),
        "headings": headings,
        "tokens": tokens,
    })
}

fn html_headings(body: &str) -> Vec<String> {
    let mut headings = Vec::new();
    for tag in ["h1", "h2"] {
        let mut rest = body;
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        while let Some((_, after_open)) = rest.split_once(&open) {
            let Some((heading, after_close)) = after_open.split_once(&close) else {
                break;
            };
            headings.push(heading.trim().to_owned());
            rest = after_close;
        }
    }
    headings.sort();
    headings
}

fn metadata_result_is_populated(body: &Value) -> bool {
    body["data"].as_array().is_some_and(|data| !data.is_empty())
}

fn detected_fields_result_is_populated(body: &Value) -> bool {
    body["fields"]
        .as_array()
        .is_some_and(|fields| !fields.is_empty())
        || body["values"]
            .as_array()
            .is_some_and(|values| !values.is_empty())
}

fn detected_labels_result_is_populated(body: &Value) -> bool {
    body.as_array().is_some_and(|labels| !labels.is_empty())
}

fn volume_result_is_populated(body: &Value) -> bool {
    body["data"]["result"]
        .as_array()
        .is_some_and(|result| !result.is_empty())
}

fn stable_metadata_result(body: &Value) -> Value {
    let mut data = body["data"].as_array().cloned().unwrap_or_default();
    data.sort_by_key(|value| value.to_string());
    json!(data)
}

fn api_prom_metadata_result_is_populated(body: &Value) -> bool {
    body["values"]
        .as_array()
        .is_some_and(|values| !values.is_empty())
        || body["series"]
            .as_array()
            .is_some_and(|series| !series.is_empty())
        || body["data"].as_array().is_some_and(|data| !data.is_empty())
}

fn stable_api_prom_metadata_result(body: &Value) -> Value {
    if let Some(values) = body["values"].as_array() {
        let mut values = values.clone();
        values.sort_by_key(|value| value.to_string());
        return json!({
            "values": values,
        });
    }
    if let Some(series) = body.as_array() {
        let mut series = series.clone();
        series.sort_by_key(|value| value.to_string());
        return json!({
            "series": series,
        });
    }
    if let Some(data) = body["data"].as_array() {
        let mut data = data.clone();
        data.sort_by_key(|value| value.to_string());
        return json!({
            "data": data,
            "status": body["status"].clone(),
        });
    }
    body.clone()
}

fn stable_detected_fields_result(body: &Value) -> Value {
    if let Some(fields) = body["fields"].as_array() {
        let mut fields = fields.clone();
        for field in &mut fields {
            if let Some(parsers) = field.get_mut("parsers").and_then(Value::as_array_mut) {
                parsers.sort_by_key(|value| value.to_string());
            }
        }
        fields.sort_by_key(|value| value["label"].as_str().unwrap_or_default().to_string());
        json!({
            "fields": fields,
            "limit": body["limit"].clone()
        })
    } else {
        let mut values = body["values"].as_array().cloned().unwrap_or_default();
        values.sort_by_key(|value| value.to_string());
        json!({
            "values": values,
            "limit": body["limit"].clone()
        })
    }
}

fn stable_detected_labels_result(body: &Value) -> Value {
    let mut labels = body["detectedLabels"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    labels.sort_by_key(|value| value["label"].as_str().unwrap_or_default().to_string());
    json!(labels)
}

fn stable_patterns_result(body: &Value) -> Value {
    let mut patterns = body["data"].as_array().cloned().unwrap_or_default();
    for pattern in &mut patterns {
        if let Some(samples) = pattern.get_mut("samples").and_then(Value::as_array_mut) {
            for sample in samples.iter_mut() {
                if let Some(sample) = sample.as_array_mut()
                    && sample.len() == 2
                {
                    sample[0] = json!("<timestamp>");
                }
            }
            samples.sort_by_key(|value| value.to_string());
        }
    }
    patterns.sort_by_key(|value| value["pattern"].as_str().unwrap_or_default().to_string());
    json!({
        "status": body["status"].clone(),
        "data": patterns,
    })
}

fn stable_patterns_response(status: u16, body: &str) -> Value {
    if status == StatusCode::OK.as_u16() {
        let body = serde_json::from_str(body).expect("Loki patterns success response JSON");
        return json!({
            "httpStatus": status,
            "body": stable_patterns_result(&body),
        });
    }
    json!({
        "httpStatus": status,
        "body": body,
    })
}

fn stable_index_stats_result(body: &Value) -> Value {
    json!({
        "streams": stable_index_stats_counter(&body["streams"]),
        "chunks": stable_index_stats_counter(&body["chunks"]),
        "entries": stable_index_stats_counter(&body["entries"]),
        "bytes": stable_index_stats_counter(&body["bytes"]),
    })
}

fn stable_index_stats_counter(value: &Value) -> Value {
    if value.as_u64().is_some() {
        json!("<number>")
    } else {
        value.clone()
    }
}

fn stable_index_volume_result(body: &Value) -> Value {
    let mut result = body["data"]["result"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    for series in &mut result {
        if let Some(value) = series.get_mut("value").and_then(Value::as_array_mut)
            && value.len() == 2
        {
            value[0] = json!("<timestamp>");
            value[1] = json!("<bytes>");
        }
        if let Some(values) = series.get_mut("values").and_then(Value::as_array_mut) {
            for sample in values {
                if let Some(sample) = sample.as_array_mut()
                    && sample.len() == 2
                {
                    sample[0] = json!("<timestamp>");
                    sample[1] = json!("<bytes>");
                }
            }
        }
    }
    result.sort_by_key(|value| value["metric"].to_string());
    json!({
        "status": body["status"].clone(),
        "data": {
            "resultType": body["data"]["resultType"].clone(),
            "result": result,
            "stats": body["data"].get("stats").is_some()
        }
    })
}

fn stable_index_volume_response(status: u16, body: &str) -> Value {
    if status == StatusCode::OK.as_u16() {
        let body = serde_json::from_str(body).expect("Loki index volume success response JSON");
        return json!({
            "httpStatus": status,
            "body": stable_index_volume_result(&body),
        });
    }
    stable_loki_error(status, body)
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

fn percent_encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}
