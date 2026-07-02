//! Live broker coverage for the observability logs WAL.
//!
//! This drives the real Kafka producer and consumer clients against an
//! in-process Crabka broker, proving the distributor-facing WAL sink and the
//! querier/compactor-facing WAL consumer agree on the durable record boundary.

use std::{
    collections::BTreeMap,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use assert2::{assert, check};
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use bytes::Bytes;
use crabka_blockstore::{
    BlockDescriptor, LabelIndex, LogBlockIndex as BlockIndex, labels, write_log_index_manifest,
};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{
    AclEntry, AclOperation, AdminClient, PatternType, PermissionType, QuotaOp, ResourceType,
};
use crabka_client_core::Client;
use crabka_client_producer::{Acks, Header, Producer, ProducerRecord};
use crabka_observability::{
    BufferedLogHotTail, KafkaLogWalConsumer, KafkaLogWalSink, LogWalConsumer, LogWalSink,
    QuerierIndexSource, Role, ServiceConfig, WalLogRecord, WalPosition, build_service_dependencies,
    build_service_router, poll_log_hot_tail_once, run_compactor_until_idle,
    run_compactor_until_shutdown, serve_service_listener,
};
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use futures_util::StreamExt;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest as _};
use tower::ServiceExt;

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn create_topic(bootstrap: &str, name: &str, partitions: i32) {
    let client = Client::builder()
        .bootstrap(bootstrap)
        .client_id("observability-wal-test-admin")
        .build()
        .await
        .expect("admin client");
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: partitions,
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

fn current_unix_second_ns() -> i64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch");
    i64::try_from(duration.as_secs()).expect("seconds fit i64") * 1_000_000_000
}

fn expected_loki_stats_with(bytes: u64, lines: u64, chunks: u64) -> Value {
    json!({
        "ingester": {
            "compressedBytes": 0,
            "decompressedBytes": 0,
            "decompressedLines": 0,
            "headChunkBytes": 0,
            "headChunkLines": 0,
            "totalBatches": 0,
            "totalChunksMatched": 0,
            "totalDuplicates": 0,
            "totalLinesSent": 0,
            "totalReached": 0
        },
        "store": {
            "chunksDownloadTime": 0.0,
            "compressedBytes": bytes,
            "decompressedBytes": bytes,
            "decompressedLines": lines,
            "totalChunksDownloaded": chunks,
            "totalChunksRef": chunks,
            "totalDuplicates": 0
        },
        "summary": {
            "bytesProcessedPerSecond": 0,
            "execTime": 0.0,
            "linesProcessedPerSecond": 0,
            "queueTime": 0.0,
            "totalBytesProcessed": bytes,
            "totalLinesProcessed": lines
        }
    })
}

fn expected_loki_ingester_stats_with(lines: u64) -> Value {
    json!({
        "ingester": {
            "compressedBytes": 0,
            "decompressedBytes": 0,
            "decompressedLines": lines,
            "headChunkBytes": 0,
            "headChunkLines": 0,
            "totalBatches": 0,
            "totalChunksMatched": 0,
            "totalDuplicates": 0,
            "totalLinesSent": lines,
            "totalReached": 0
        },
        "store": {
            "chunksDownloadTime": 0.0,
            "compressedBytes": 0,
            "decompressedBytes": 0,
            "decompressedLines": 0,
            "totalChunksDownloaded": 0,
            "totalChunksRef": 0,
            "totalDuplicates": 0
        },
        "summary": {
            "bytesProcessedPerSecond": 0,
            "execTime": 0.0,
            "linesProcessedPerSecond": 0,
            "queueTime": 0.0,
            "totalBytesProcessed": 0,
            "totalLinesProcessed": lines
        }
    })
}

fn service_config(role: Role, bootstrap: &str, topic: &str, data_root: &TempDir) -> ServiceConfig {
    ServiceConfig {
        target: role,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: Some(bootstrap.to_string()),
        wal_topic: topic.to_string(),
        wal_group_id: format!("observability-wal-live-test-{topic}-{role:?}"),
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

fn current_fixture_timestamp_ns(offset_ns: i64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs();
    let base_ns = i64::try_from(now).expect("unix seconds fit in i64") * 1_000_000_000;
    (base_ns + offset_ns).to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observability_kafka_wal_sink_feeds_live_consumer_hot_tail() {
    let (broker, bootstrap, _dir) = boot().await;
    let topic = "__crabka_observability_logs_wal_live";
    create_topic(&bootstrap, topic, 1).await;
    broker.wait_until_partition_present(topic, 0).await;

    let record = WalLogRecord {
        tenant: "tenant-a".to_string(),
        labels: BTreeMap::from([
            ("app".to_string(), "api".to_string()),
            ("env".to_string(), "prod".to_string()),
        ]),
        timestamp_ns: 20_000_000,
        line: "api live wal error".to_string(),
        structured_metadata: BTreeMap::from([("trace_id".to_string(), "abc123".to_string())]),
        position: None,
    };
    let sink = KafkaLogWalSink::connect(&bootstrap, topic)
        .await
        .expect("wal sink");
    sink.append(record.clone())
        .await
        .expect("append wal record");

    let mut consumer =
        KafkaLogWalConsumer::connect(&bootstrap, "observability-wal-live-test", topic)
            .await
            .expect("wal consumer");
    let hot_tail = BufferedLogHotTail::default();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut decoded = 0;
    while decoded == 0 && Instant::now() < deadline {
        decoded = poll_log_hot_tail_once(&mut consumer, &hot_tail, Duration::from_millis(250))
            .await
            .expect("poll live wal");
    }

    assert!(decoded == 1, "expected one live WAL record");
    assert!(
        hot_tail.records()
            == vec![WalLogRecord {
                position: Some(WalPosition {
                    partition: 0,
                    offset: 0,
                }),
                ..record
            }]
    );
    consumer
        .commit_compacted(WalPosition {
            partition: 0,
            offset: 0,
        })
        .await
        .expect("commit compacted");
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_built_distributor_writes_loki_push_to_live_wal() {
    let (broker, bootstrap, _broker_dir) = boot().await;
    let data_root = TempDir::new().expect("data root");
    let topic = "__crabka_observability_logs_wal_config_distributor";
    create_topic(&bootstrap, topic, 1).await;
    broker.wait_until_partition_present(topic, 0).await;

    let config = service_config(Role::Distributor, &bootstrap, topic, &data_root);
    let app = build_service_router(
        &config,
        build_service_dependencies(&config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    let timestamp = current_fixture_timestamp_ns(20_000_000);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api",
                                    "env": "prod"
                                },
                                "values": [[timestamp, "api config wal error", {"trace_id": "abc123"}]]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);

    let mut consumer =
        KafkaLogWalConsumer::connect(&bootstrap, "observability-wal-config-distributor", topic)
            .await
            .expect("wal consumer");
    let hot_tail = BufferedLogHotTail::default();
    let decoded = poll_until_decoded(&mut consumer, &hot_tail).await;

    assert!(decoded == 1, "expected one config-produced WAL record");
    assert!(
        hot_tail.records()
            == vec![WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([
                    ("app", "api"),
                    ("detected_level", "error"),
                    ("env", "prod"),
                    ("service_name", "api"),
                ]),
                timestamp_ns: timestamp.parse().unwrap(),
                line: "api config wal error".to_string(),
                structured_metadata: BTreeMap::from([(
                    "trace_id".to_string(),
                    "abc123".to_string()
                )]),
                position: Some(WalPosition {
                    partition: 0,
                    offset: 0,
                }),
            }]
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_built_distributor_enforces_broker_user_producer_byte_rate_quota_before_wal_append()
{
    let (broker, bootstrap, _broker_dir) = boot().await;
    let data_root = TempDir::new().expect("data root");
    let topic = "__crabka_observability_logs_wal_quota_distributor";
    create_topic(&bootstrap, topic, 1).await;
    broker.wait_until_partition_present(topic, 0).await;

    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .expect("admin client");
    let outcome = admin
        .alter_user_quotas(
            "tenant-a",
            &[QuotaOp::Set {
                key: "producer_byte_rate".to_string(),
                value: 32.0,
            }],
            false,
        )
        .await
        .expect("set tenant quota");
    assert!(outcome.is_none(), "quota alter failed: {outcome:?}");

    let config = service_config(Role::Distributor, &bootstrap, topic, &data_root);
    let app = build_service_router(
        &config,
        build_service_dependencies(&config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    let timestamp = current_fixture_timestamp_ns(20_000_000);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api",
                                    "env": "prod"
                                },
                                "values": [[timestamp, "api quota blocked because this line is larger than the configured tenant byte rate"]]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::TOO_MANY_REQUESTS);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(body["errorType"] == "rate_limited");
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("producer_byte_rate")
    );

    let mut consumer =
        KafkaLogWalConsumer::connect(&bootstrap, "observability-wal-quota-distributor", topic)
            .await
            .expect("wal consumer");
    let hot_tail = BufferedLogHotTail::default();
    let decoded = poll_log_hot_tail_once(&mut consumer, &hot_tail, Duration::from_millis(250))
        .await
        .expect("poll live wal");
    assert!(
        decoded == 0,
        "quota rejection must happen before WAL append"
    );
    assert!(hot_tail.records().is_empty());
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_built_distributor_enforces_tenant_write_acl_before_wal_append() {
    let (broker, bootstrap, _broker_dir) = boot().await;
    let data_root = TempDir::new().expect("data root");
    let topic = "__crabka_observability_logs_wal_acl_distributor";
    create_topic(&bootstrap, topic, 1).await;
    broker.wait_until_partition_present(topic, 0).await;

    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .expect("admin client");
    let outcomes = admin
        .create_acls(&[AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "__observability_acl_policy_enabled__".to_string(),
            pattern_type: PatternType::Literal,
            principal: "User:admin".to_string(),
            host: "*".to_string(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        }])
        .await
        .expect("seed acl policy");
    assert!(
        outcomes.iter().all(|outcome| outcome.error.is_none()),
        "seed acl failed: {outcomes:?}"
    );

    let config = service_config(Role::Distributor, &bootstrap, topic, &data_root);
    let app = build_service_router(
        &config,
        build_service_dependencies(&config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    let timestamp = current_fixture_timestamp_ns(20_000_000);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api",
                                    "env": "prod"
                                },
                                "values": [[timestamp, "api acl blocked"]]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::FORBIDDEN);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(body["errorType"] == "forbidden");
    assert!(body["error"].as_str().unwrap().contains("tenant write ACL"));

    let mut consumer =
        KafkaLogWalConsumer::connect(&bootstrap, "observability-wal-acl-distributor", topic)
            .await
            .expect("wal consumer");
    let hot_tail = BufferedLogHotTail::default();
    let decoded = poll_log_hot_tail_once(&mut consumer, &hot_tail, Duration::from_millis(250))
        .await
        .expect("poll live wal");
    assert!(decoded == 0, "ACL rejection must happen before WAL append");
    assert!(hot_tail.records().is_empty());
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_built_distributor_allows_tenant_with_write_acl_to_append_wal() {
    let (broker, bootstrap, _broker_dir) = boot().await;
    let data_root = TempDir::new().expect("data root");
    let topic = "__crabka_observability_logs_wal_acl_allow_distributor";
    create_topic(&bootstrap, topic, 1).await;
    broker.wait_until_partition_present(topic, 0).await;

    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .expect("admin client");
    let outcomes = admin
        .create_acls(&[
            AclEntry {
                resource_type: ResourceType::Topic,
                resource_name: "__observability_acl_policy_enabled__".to_string(),
                pattern_type: PatternType::Literal,
                principal: "User:admin".to_string(),
                host: "*".to_string(),
                operation: AclOperation::Read,
                permission_type: PermissionType::Allow,
            },
            AclEntry {
                resource_type: ResourceType::Topic,
                resource_name: topic.to_string(),
                pattern_type: PatternType::Literal,
                principal: "User:tenant-a".to_string(),
                host: "*".to_string(),
                operation: AclOperation::Write,
                permission_type: PermissionType::Allow,
            },
        ])
        .await
        .expect("seed tenant write acl");
    assert!(
        outcomes.iter().all(|outcome| outcome.error.is_none()),
        "seed acl failed: {outcomes:?}"
    );

    let config = service_config(Role::Distributor, &bootstrap, topic, &data_root);
    let app = build_service_router(
        &config,
        build_service_dependencies(&config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    let timestamp = current_fixture_timestamp_ns(20_000_000);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api",
                                    "env": "prod"
                                },
                                "values": [[timestamp, "api acl allowed"]]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);

    let mut consumer =
        KafkaLogWalConsumer::connect(&bootstrap, "observability-wal-acl-allow-distributor", topic)
            .await
            .expect("wal consumer");
    let hot_tail = BufferedLogHotTail::default();
    let decoded = poll_until_decoded(&mut consumer, &hot_tail).await;
    assert!(decoded == 1, "expected one ACL-authorized WAL record");
    assert!(
        hot_tail.records()
            == vec![WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([("app", "api"), ("env", "prod"), ("service_name", "api"),]),
                timestamp_ns: timestamp.parse().unwrap(),
                line: "api acl allowed".to_string(),
                structured_metadata: BTreeMap::new(),
                position: Some(WalPosition {
                    partition: 0,
                    offset: 0,
                }),
            }]
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_built_querier_enforces_tenant_read_acl_before_query() {
    let (broker, bootstrap, _broker_dir) = boot().await;
    let data_root = TempDir::new().expect("data root");
    let topic = "__crabka_observability_logs_wal_acl_querier";
    create_topic(&bootstrap, topic, 1).await;
    broker.wait_until_partition_present(topic, 0).await;

    let mut label_index = LabelIndex::default();
    label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    write_log_index_manifest(data_root.path(), &label_index, &BlockIndex::default()).unwrap();

    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .expect("admin client");
    let outcomes = admin
        .create_acls(&[AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "__observability_acl_policy_enabled__".to_string(),
            pattern_type: PatternType::Literal,
            principal: "User:admin".to_string(),
            host: "*".to_string(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        }])
        .await
        .expect("seed acl policy");
    assert!(
        outcomes.iter().all(|outcome| outcome.error.is_none()),
        "seed acl failed: {outcomes:?}"
    );

    let config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    let app = build_service_router(
        &config,
        build_service_dependencies(&config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    // The querier serves queries through a `SwappableQueryAuthorizer` that begins
    // permissive (allow-all) and swaps in the real broker-backed authorizer once
    // it connects in the background ("FIX B2"). A query issued before that swap
    // completes is allowed, so poll until enforcement is active rather than
    // racing the swap (the race is marginal and only surfaces under CI's slower
    // coverage build).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let response = loop {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        if response.status() == StatusCode::FORBIDDEN || std::time::Instant::now() >= deadline {
            break response;
        }
        tokio::task::yield_now().await;
    };

    assert!(response.status() == StatusCode::FORBIDDEN);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(body["errorType"] == "forbidden");
    assert!(body["error"].as_str().unwrap().contains("tenant read ACL"));
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_built_querier_tails_live_wal_into_query_results() {
    let (broker, bootstrap, _broker_dir) = boot().await;
    let data_root = TempDir::new().expect("data root");
    let topic = "__crabka_observability_logs_wal_config_querier";
    create_topic(&bootstrap, topic, 1).await;
    broker.wait_until_partition_present(topic, 0).await;

    let mut label_index = LabelIndex::default();
    label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    write_log_index_manifest(data_root.path(), &label_index, &BlockIndex::default()).unwrap();

    let record = WalLogRecord {
        tenant: "tenant-a".to_string(),
        labels: labels([("app", "api"), ("env", "prod")]),
        timestamp_ns: 20_000_000,
        line: "api config querier error".to_string(),
        structured_metadata: BTreeMap::new(),
        position: None,
    };
    KafkaLogWalSink::connect(&bootstrap, topic)
        .await
        .expect("wal sink")
        .append(record)
        .await
        .expect("append wal record");

    let config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    let app = build_service_router(
        &config,
        build_service_dependencies(&config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    let body = query_until_api_error(app).await;
    assert!(
        body == json!({
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {
                            "app": "api",
                            "detected_level": "unknown",
                            "env": "prod"
                        },
                        "values": [
                            ["20000000", "api config querier error"]
                        ]
                    }
                ],
                "stats": expected_loki_ingester_stats_with(1)
            }
        })
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_listener_tails_live_wal_over_websocket() {
    let (broker, bootstrap, _broker_dir) = boot().await;
    let data_root = TempDir::new().expect("data root");
    let topic = "__crabka_observability_logs_wal_tail_listener";
    create_topic(&bootstrap, topic, 1).await;
    broker.wait_until_partition_present(topic, 0).await;

    write_log_index_manifest(
        data_root.path(),
        &LabelIndex::default(),
        &BlockIndex::default(),
    )
    .unwrap();
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

    let mut querier_config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    querier_config.wal_group_id = "observability-wal-tail-listener-querier".to_string();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let dependencies = build_service_dependencies(&querier_config).await.unwrap();
        serve_service_listener(listener, querier_config, dependencies, None)
            .await
            .unwrap();
    });

    let timestamp = current_fixture_timestamp_ns(20_000_000);
    let end = timestamp.parse::<i64>().unwrap() + 10_000_000;
    let mut request = format!(
        "ws://{addr}/loki/api/v1/tail?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22tail%22&start=0&end={end}"
    )
    .into_client_request()
    .unwrap();
    request
        .headers_mut()
        .insert("X-Scope-OrgID", "tenant-a".parse().unwrap());
    let (mut socket, response) = connect_async(request).await.unwrap();
    assert!(response.status() == StatusCode::SWITCHING_PROTOCOLS);

    push_api_log(distributor, &timestamp, "api live websocket tail error").await;

    let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let frame: serde_json::Value = serde_json::from_str(message.to_text().unwrap()).unwrap();

    assert!(
        frame
            == json!({
                "streams": [
                    {
                        "stream": {
                            "app": "api",
                            "detected_level": "error",
                            "env": "prod",
                            "service_name": "api"
                        },
                        "values": [
                            [timestamp, "api live websocket tail error"]
                        ]
                    }
                ]
            })
    );

    server.abort();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_built_distributor_compactor_querier_loop_serves_compacted_logs() {
    let (broker, bootstrap, _broker_dir) = boot().await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");
    let object_store_url = format!("file://{}", object_root.path().display());
    let topic = "__crabka_observability_logs_wal_config_loop";
    let index_prefix = "observability/logs";
    create_topic(&bootstrap, topic, 1).await;
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
    let ok_timestamp = current_fixture_timestamp_ns(10_000_000);
    let error_timestamp = current_fixture_timestamp_ns(20_000_000);

    let response = distributor
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api",
                                    "env": "prod"
                                },
                                "values": [
                                    [ok_timestamp, "api full loop ok"],
                                    [error_timestamp, "api full loop error"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::NO_CONTENT);

    let mut compactor_config = service_config(Role::Compactor, &bootstrap, topic, &data_root);
    compactor_config.object_store_url = Some(object_store_url.clone());
    compactor_config.index_prefix = Some(index_prefix.to_string());
    compactor_config.wal_group_id = "observability-wal-config-loop-compactor".to_string();
    let descriptors = run_compactor_until_idle(
        &compactor_config,
        build_service_dependencies(&compactor_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    assert!(descriptors.len() == 1);
    check!(descriptors[0].key.partition == 0);
    check!(descriptors[0].key.first_offset == 0);
    check!(descriptors[0].key.last_offset == 1);

    let mut querier_config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    querier_config.object_store_url = Some(object_store_url);
    querier_config.index_prefix = Some(index_prefix.to_string());
    querier_config.tenant = Some("tenant-a".to_string());
    querier_config.query_start_ns = Some(0);
    querier_config.query_end_ns = Some(error_timestamp.parse::<i64>().unwrap() + 10_000_000);
    querier_config.querier_index_source = QuerierIndexSource::TenantObjectStoreShards;
    querier_config.wal_group_id = "observability-wal-config-loop-querier".to_string();
    let querier = build_service_router(
        &querier_config,
        build_service_dependencies(&querier_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    let body = query_until_loop_error(querier, &error_timestamp).await;
    assert!(
        body == json!({
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {
                            "app": "api",
                            "detected_level": "error",
                            "env": "prod",
                            "service_name": "api"
                        },
                        "values": [
                            [error_timestamp, "api full loop error"]
                        ]
                    }
                ],
                "stats": expected_loki_stats_with(descriptors[0].size_bytes, 1, 1)
            }
        })
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn otlp_http_log_flows_through_configured_distributor_compactor_and_querier() {
    let (broker, bootstrap, _broker_dir) = boot().await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");
    let object_store_url = format!("file://{}", object_root.path().display());
    let topic = "__crabka_observability_logs_wal_otlp_loop";
    let index_prefix = "observability/logs";
    create_topic(&bootstrap, topic, 1).await;
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
    let timestamp = current_unix_second_ns().to_string();

    let response = distributor
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "resource": {
                                    "attributes": [
                                        {"key": "service.name", "value": {"stringValue": "checkout"}},
                                        {"key": "deployment.environment", "value": {"stringValue": "prod"}}
                                    ]
                                },
                                "scopeLogs": [
                                    {
                                        "scope": {
                                            "attributes": [
                                                {"key": "instrumentation.scope", "value": {"stringValue": "api"}}
                                            ]
                                        },
                                        "logRecords": [
                                            {
                                                "timeUnixNano": timestamp.clone(),
                                                "body": {"stringValue": "checkout otlp loop error"},
                                                "attributes": [
                                                    {"key": "status", "value": {"intValue": "500"}},
                                                    {"key": "trace_id", "value": {"stringValue": "abc123"}}
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::NO_CONTENT);

    let mut compactor_config = service_config(Role::Compactor, &bootstrap, topic, &data_root);
    compactor_config.object_store_url = Some(object_store_url.clone());
    compactor_config.index_prefix = Some(index_prefix.to_string());
    compactor_config.wal_group_id = "observability-wal-otlp-loop-compactor".to_string();
    let descriptors = run_compactor_until_idle(
        &compactor_config,
        build_service_dependencies(&compactor_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    assert!(descriptors.len() == 1);
    check!(descriptors[0].key.first_offset == 0);
    check!(descriptors[0].key.last_offset == 0);

    let mut querier_config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    querier_config.object_store_url = Some(object_store_url);
    querier_config.index_prefix = Some(index_prefix.to_string());
    querier_config.querier_index_source = QuerierIndexSource::TenantObjectStoreShards;
    querier_config.wal_group_id = "observability-wal-otlp-loop-querier".to_string();
    let querier = build_service_router(
        &querier_config,
        build_service_dependencies(&querier_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    let body = query_until_otlp_loop_error(querier, &timestamp).await;
    assert!(
        body.pointer("/data/result/0/stream")
            == Some(&json!({
                "deployment_environment": "prod",
                "detected_level": "unknown",
                "instrumentation_scope": "api",
                "service_name": "checkout",
                "status": "500",
                "trace_id": "abc123"
            }))
    );
    assert!(
        body.pointer("/data/result/0/values")
            == Some(&json!([[timestamp, "checkout otlp loop error"]]))
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_loop_isolates_tenants_sharing_one_wal_topic() {
    let (broker, bootstrap, _broker_dir) = boot().await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");
    let object_store_url = format!("file://{}", object_root.path().display());
    let topic = "__crabka_observability_logs_wal_tenant_loop";
    let index_prefix = "observability/logs";
    create_topic(&bootstrap, topic, 1).await;
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

    let tenant_a_timestamp = current_fixture_timestamp_ns(20_000_000);
    let tenant_b_timestamp = current_fixture_timestamp_ns(21_000_000);
    push_tenant_api_log(
        distributor.clone(),
        "tenant-a",
        &tenant_a_timestamp,
        "tenant a shared wal error",
    )
    .await;
    push_tenant_api_log(
        distributor,
        "tenant-b",
        &tenant_b_timestamp,
        "tenant b shared wal error",
    )
    .await;

    let mut compactor_config = service_config(Role::Compactor, &bootstrap, topic, &data_root);
    compactor_config.object_store_url = Some(object_store_url.clone());
    compactor_config.index_prefix = Some(index_prefix.to_string());
    compactor_config.wal_group_id = "observability-wal-tenant-loop-compactor".to_string();
    let descriptors = run_compactor_until_idle(
        &compactor_config,
        build_service_dependencies(&compactor_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    assert!(descriptors.len() == 2);

    let mut querier_config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    querier_config.object_store_url = Some(object_store_url);
    querier_config.index_prefix = Some(index_prefix.to_string());
    querier_config.querier_index_source = QuerierIndexSource::TenantObjectStoreShards;
    querier_config.wal_group_id = "observability-wal-tenant-loop-querier".to_string();
    let querier = build_service_router(
        &querier_config,
        build_service_dependencies(&querier_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    let end = tenant_b_timestamp.parse::<i64>().unwrap() + 10_000_000;
    let tenant_a = query_until_tenant_shared_error(querier.clone(), "tenant-a", end).await;
    assert!(
        tenant_a.pointer("/data/result/0/values")
            == Some(&json!([[tenant_a_timestamp, "tenant a shared wal error"]]))
    );
    assert!(
        !tenant_a.to_string().contains("tenant b shared wal error"),
        "tenant-a query leaked tenant-b row: {tenant_a}"
    );

    let tenant_b = query_until_tenant_shared_error(querier, "tenant-b", end).await;
    assert!(
        tenant_b.pointer("/data/result/0/values")
            == Some(&json!([[tenant_b_timestamp, "tenant b shared wal error"]]))
    );
    assert!(
        !tenant_b.to_string().contains("tenant a shared wal error"),
        "tenant-b query leaked tenant-a row: {tenant_b}"
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_built_querier_merges_compacted_blocks_with_uncompacted_live_tail() {
    let (broker, bootstrap, _broker_dir) = boot().await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");
    let object_store_url = format!("file://{}", object_root.path().display());
    let topic = "__crabka_observability_logs_wal_hot_cold_loop";
    let index_prefix = "observability/logs";
    create_topic(&bootstrap, topic, 1).await;
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

    let compacted_timestamp = current_fixture_timestamp_ns(10_000_000);
    let live_timestamp = current_fixture_timestamp_ns(20_000_000);
    push_api_log(
        distributor.clone(),
        &compacted_timestamp,
        "api compacted error",
    )
    .await;

    let mut compactor_config = service_config(Role::Compactor, &bootstrap, topic, &data_root);
    compactor_config.object_store_url = Some(object_store_url.clone());
    compactor_config.index_prefix = Some(index_prefix.to_string());
    compactor_config.wal_group_id = "observability-wal-hot-cold-compactor".to_string();
    let descriptors = run_compactor_until_idle(
        &compactor_config,
        build_service_dependencies(&compactor_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    assert!(descriptors.len() == 1);
    check!(descriptors[0].key.first_offset == 0);
    check!(descriptors[0].key.last_offset == 0);

    push_api_log(distributor, &live_timestamp, "api live tail error").await;

    let mut querier_config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    querier_config.object_store_url = Some(object_store_url);
    querier_config.index_prefix = Some(index_prefix.to_string());
    querier_config.querier_index_source = QuerierIndexSource::TenantObjectStoreShards;
    querier_config.wal_group_id = "observability-wal-hot-cold-querier".to_string();
    let querier = build_service_router(
        &querier_config,
        build_service_dependencies(&querier_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    let body = query_until_hot_cold_errors(querier, &compacted_timestamp, &live_timestamp).await;
    assert!(
        body.pointer("/data/result/0/values")
            == Some(&json!([
                [compacted_timestamp, "api compacted error"],
                [live_timestamp, "api live tail error"]
            ]))
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_built_compactor_restart_resumes_from_committed_live_wal_offset() {
    let (broker, bootstrap, _broker_dir) = boot().await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");
    let object_store_url = format!("file://{}", object_root.path().display());
    let topic = "__crabka_observability_logs_wal_compactor_restart";
    let index_prefix = "observability/logs";
    let compactor_group = "observability-wal-compactor-restart";
    create_topic(&bootstrap, topic, 1).await;
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

    let first_timestamp = current_fixture_timestamp_ns(10_000_000);
    let second_timestamp = current_fixture_timestamp_ns(20_000_000);
    push_api_log(
        distributor.clone(),
        &first_timestamp,
        "api first restart error",
    )
    .await;

    let mut first_compactor = service_config(Role::Compactor, &bootstrap, topic, &data_root);
    first_compactor.object_store_url = Some(object_store_url.clone());
    first_compactor.index_prefix = Some(index_prefix.to_string());
    first_compactor.wal_group_id = compactor_group.to_string();
    let first_descriptors =
        run_configured_compactor_for(&first_compactor, Duration::from_millis(750)).await;
    assert!(first_descriptors.len() == 1);
    check!(first_descriptors[0].key.first_offset == 0);
    check!(first_descriptors[0].key.last_offset == 0);

    push_api_log(distributor, &second_timestamp, "api second restart error").await;

    let mut restarted_compactor = service_config(Role::Compactor, &bootstrap, topic, &data_root);
    restarted_compactor.object_store_url = Some(object_store_url.clone());
    restarted_compactor.index_prefix = Some(index_prefix.to_string());
    restarted_compactor.wal_group_id = compactor_group.to_string();
    let second_descriptors =
        run_configured_compactor_for(&restarted_compactor, Duration::from_millis(750)).await;
    assert!(second_descriptors.len() == 1);
    check!(second_descriptors[0].key.first_offset == 1);
    check!(second_descriptors[0].key.last_offset == 1);

    let mut querier_config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    querier_config.object_store_url = Some(object_store_url);
    querier_config.index_prefix = Some(index_prefix.to_string());
    querier_config.querier_index_source = QuerierIndexSource::TenantObjectStoreShards;
    querier_config.wal_group_id = "observability-wal-compactor-restart-querier".to_string();
    let querier = build_service_router(
        &querier_config,
        build_service_dependencies(&querier_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    let body = query_until_restart_errors(querier, &first_timestamp, &second_timestamp).await;
    assert!(
        body.pointer("/data/result/0/values")
            == Some(&json!([
                [first_timestamp, "api first restart error"],
                [second_timestamp, "api second restart error"]
            ]))
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_kafka_produced_log_flows_through_configured_compactor_and_querier() {
    let (broker, bootstrap, _broker_dir) = boot().await;
    let data_root = TempDir::new().expect("data root");
    let object_root = TempDir::new().expect("object root");
    let object_store_url = format!("file://{}", object_root.path().display());
    let topic = "__crabka_observability_logs_wal_native_loop";
    let index_prefix = "observability/logs";
    create_topic(&bootstrap, topic, 1).await;
    broker.wait_until_partition_present(topic, 0).await;

    produce_native_kafka_log(
        &bootstrap,
        topic,
        "tenant-a",
        "20000000",
        "api native kafka error",
    )
    .await;

    let mut compactor_config = service_config(Role::Compactor, &bootstrap, topic, &data_root);
    compactor_config.object_store_url = Some(object_store_url.clone());
    compactor_config.index_prefix = Some(index_prefix.to_string());
    compactor_config.wal_group_id = "observability-wal-native-loop-compactor".to_string();
    let descriptors = run_compactor_until_idle(
        &compactor_config,
        build_service_dependencies(&compactor_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();
    assert!(descriptors.len() == 1);
    check!(descriptors[0].key.first_offset == 0);
    check!(descriptors[0].key.last_offset == 0);

    let mut querier_config = service_config(Role::Querier, &bootstrap, topic, &data_root);
    querier_config.object_store_url = Some(object_store_url);
    querier_config.index_prefix = Some(index_prefix.to_string());
    querier_config.querier_index_source = QuerierIndexSource::TenantObjectStoreShards;
    querier_config.wal_group_id = "observability-wal-native-loop-querier".to_string();
    let querier = build_service_router(
        &querier_config,
        build_service_dependencies(&querier_config).await.unwrap(),
        None,
    )
    .await
    .unwrap();

    let body = query_until_native_kafka_error(querier).await;
    assert!(
        body.pointer("/data/result/0/stream")
            == Some(&json!({
                "app": "api",
                "detected_level": "unknown",
                "env": "prod",
                "trace_id": "abc123"
            }))
    );
    assert!(
        body.pointer("/data/result/0/values")
            == Some(&json!([["20000000", "api native kafka error"]]))
    );
    broker.shutdown().await;
}

async fn poll_until_decoded(
    consumer: &mut KafkaLogWalConsumer,
    hot_tail: &BufferedLogHotTail,
) -> usize {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut decoded = 0;
    while decoded == 0 && Instant::now() < deadline {
        decoded = poll_log_hot_tail_once(consumer, hot_tail, Duration::from_millis(250))
            .await
            .expect("poll live wal");
    }
    decoded
}

async fn run_configured_compactor_for(
    config: &ServiceConfig,
    duration: Duration,
) -> Vec<BlockDescriptor> {
    run_compactor_until_shutdown(
        config,
        build_service_dependencies(config).await.unwrap(),
        None,
        // real-time wait (not a progress poll): fixed run-duration shutdown signal for the compactor
        tokio::time::sleep(duration),
    )
    .await
    .unwrap()
}

async fn produce_native_kafka_log(
    bootstrap: &str,
    topic: &str,
    tenant: &str,
    timestamp_ns: &str,
    line: &str,
) {
    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .client_id("observability-wal-native-test-producer")
        .acks(Acks::All)
        .build()
        .await
        .expect("native Kafka producer");
    producer
        .send(ProducerRecord {
            topic: topic.to_string(),
            partition: Some(0),
            key: Some(Bytes::from(format!("{tenant}:api"))),
            value: Some(Bytes::from(line.to_string())),
            headers: vec![
                kafka_header("crabka-tenant", tenant),
                kafka_header("crabka-log-timestamp-ns", timestamp_ns),
                kafka_header("crabka-log-label-app", "api"),
                kafka_header("crabka-log-label-env", "prod"),
                kafka_header("crabka-log-metadata-trace_id", "abc123"),
            ],
            timestamp_ms: None,
        })
        .await
        .await
        .expect("native Kafka producer delivery channel")
        .expect("native Kafka produce");
    producer.flush().await.expect("flush native Kafka producer");
    producer.close().await.expect("close native Kafka producer");
}

fn kafka_header(key: &str, value: &str) -> Header {
    Header {
        key: key.to_string(),
        value: Some(Bytes::from(value.to_string())),
    }
}

async fn push_api_log(app: axum::Router, timestamp_ns: &str, line: &str) {
    push_tenant_api_log(app, "tenant-a", timestamp_ns, line).await;
}

async fn push_tenant_api_log(app: axum::Router, tenant: &str, timestamp_ns: &str, line: &str) {
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", tenant)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api",
                                    "env": "prod"
                                },
                                "values": [[timestamp_ns, line]]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::NO_CONTENT);
}

async fn query_until_api_error(app: axum::Router) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(
                        "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=20000000",
                    )
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        if body
            != json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [],
                    "stats": {
                        "summary": {
                            "bytesProcessedPerSecond": 0,
                            "execTime": 0.0,
                            "linesProcessedPerSecond": 0,
                            "queueTime": 0.0,
                            "totalBytesProcessed": 0,
                            "totalLinesProcessed": 0
                        },
                        "ingester": {
                            "compressedBytes": 0,
                            "decompressedBytes": 0,
                            "decompressedLines": 0,
                            "headChunkBytes": 0,
                            "headChunkLines": 0,
                            "totalBatches": 0,
                            "totalChunksMatched": 0,
                            "totalDuplicates": 0,
                            "totalLinesSent": 0,
                            "totalReached": 0
                        },
                        "store": {
                            "chunksDownloadTime": 0.0,
                            "compressedBytes": 0,
                            "decompressedBytes": 0,
                            "decompressedLines": 0,
                            "totalChunksDownloaded": 0,
                            "totalChunksRef": 0,
                            "totalDuplicates": 0
                        }
                    }
                }
            })
        {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "query never observed hot WAL row"
        );
        tokio::task::yield_now().await;
    }
}

async fn query_until_hot_cold_errors(
    app: axum::Router,
    compacted_timestamp: &str,
    live_timestamp: &str,
) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    let end = live_timestamp.parse::<i64>().unwrap() + 10_000_000;
    loop {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end={end}&direction=forward"
                    ))
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        if body.pointer("/data/result/0/values")
            == Some(&json!([
                [compacted_timestamp, "api compacted error"],
                [live_timestamp, "api live tail error"]
            ]))
        {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "query never observed exact hot/cold merge result: {body}"
        );
        tokio::task::yield_now().await;
    }
}

async fn query_until_native_kafka_error(app: axum::Router) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(
                        "/loki/api/v1/query?query=%7Bapp%3D%22api%22%2Cenv%3D%22prod%22%7D%20%7C%3D%20%22native%22&time=20000000",
                    )
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        if body.pointer("/data/result/0/values")
            == Some(&json!([["20000000", "api native kafka error"]]))
        {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "query never observed native Kafka produced log: {body}"
        );
        tokio::task::yield_now().await;
    }
}

async fn query_until_otlp_loop_error(app: axum::Router, timestamp_ns: &str) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let uri = format!(
            "/loki/api/v1/query?query=%7Bservice_name%3D%22checkout%22%2Cdeployment_environment%3D%22prod%22%7D%20%7C%3D%20%22otlp%22&time={timestamp_ns}"
        );
        let response = app
            .clone()
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
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        if body.pointer("/data/result/0/values")
            == Some(&json!([[timestamp_ns, "checkout otlp loop error"]]))
        {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "query never observed OTLP produced log: {body}"
        );
        tokio::task::yield_now().await;
    }
}

async fn query_until_tenant_shared_error(
    app: axum::Router,
    tenant: &str,
    end: i64,
) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22shared%22&start=0&end={end}&direction=forward"
                    ))
                    .header("X-Scope-OrgID", tenant)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        if !body["data"]["result"].as_array().unwrap().is_empty() {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "query never observed shared WAL row for {tenant}: {body}"
        );
        tokio::task::yield_now().await;
    }
}

async fn query_until_restart_errors(
    app: axum::Router,
    first_timestamp: &str,
    second_timestamp: &str,
) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    let end = second_timestamp.parse::<i64>().unwrap() + 10_000_000;
    loop {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22restart%22&start=0&end={end}&direction=forward"
                    ))
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        if body.pointer("/data/result/0/values")
            == Some(&json!([
                [first_timestamp, "api first restart error"],
                [second_timestamp, "api second restart error"]
            ]))
        {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "query never observed exact restart result: {body}"
        );
        tokio::task::yield_now().await;
    }
}

async fn query_until_loop_error(app: axum::Router, timestamp: &str) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time={timestamp}"
                    ))
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let body = serde_json::from_slice(&body).unwrap();
        if body
            != json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [],
                    "stats": {
                        "summary": {
                            "bytesProcessedPerSecond": 0,
                            "execTime": 0.0,
                            "linesProcessedPerSecond": 0,
                            "queueTime": 0.0,
                            "totalBytesProcessed": 0,
                            "totalLinesProcessed": 0
                        },
                        "ingester": {
                            "compressedBytes": 0,
                            "decompressedBytes": 0,
                            "decompressedLines": 0,
                            "headChunkBytes": 0,
                            "headChunkLines": 0,
                            "totalBatches": 0,
                            "totalChunksMatched": 0,
                            "totalDuplicates": 0,
                            "totalLinesSent": 0,
                            "totalReached": 0
                        },
                        "store": {
                            "chunksDownloadTime": 0.0,
                            "compressedBytes": 0,
                            "decompressedBytes": 0,
                            "decompressedLines": 0,
                            "totalChunksDownloaded": 0,
                            "totalChunksRef": 0,
                            "totalDuplicates": 0
                        }
                    }
                }
            })
        {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "query never observed compacted loop row"
        );
        tokio::task::yield_now().await;
    }
}
