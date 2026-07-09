use std::{collections::BTreeMap, sync::Arc, time::Duration};

use axum::{
    Router,
    body::{Body, Bytes as AxumBytes},
    extract::State,
    http::{HeaderMap, Request, StatusCode},
    routing::post,
};
use bytes::Bytes;
use connectrpc_axum::message::Streaming;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerRecord, IsolationLevel};
use crabka_client_producer::{Acks, Header as ProducerHeader, Producer, ProducerRecord};
use crabka_grpc_gateway::{
    authz::GatewayAuthz,
    codec::RawCodec,
    config::GatewayConfig,
    outbound,
    outbound_config::{CompiledSubscription, OutboundContentMode},
    pb,
    produce::ProduceCore,
    state::AppState,
    streaming,
    webhook::webhook_router,
};
use futures_util::StreamExt;
use serde_json::Value;
use tempfile::TempDir;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

const CE_ID: &str = "roundtrip-1";
const CE_SOURCE: &str = "/roundtrip";
const CE_TYPE: &str = "com.example.roundtrip";
const CE_SPECVERSION: &str = "1.0";

#[derive(Debug, Clone)]
struct CapturedRequest {
    body: Vec<u8>,
    content_type: Option<String>,
    ce_headers: BTreeMap<String, String>,
}

#[derive(Default)]
struct CaptureState {
    requests: Mutex<Vec<CapturedRequest>>,
}

async fn capture_handler(
    State(state): State<Arc<CaptureState>>,
    headers: HeaderMap,
    body: AxumBytes,
) -> StatusCode {
    let ce_headers = headers
        .iter()
        .filter_map(|(name, value)| {
            let header_name = name.as_str();
            if !header_name.starts_with("ce-") {
                return None;
            }
            Some((
                header_name.to_owned(),
                value.to_str().ok().unwrap_or_default().to_owned(),
            ))
        })
        .collect();
    let content_type = headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    state.requests.lock().await.push(CapturedRequest {
        body: body.to_vec(),
        content_type,
        ce_headers,
    });
    StatusCode::OK
}

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().expect("temp dir");
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .expect("broker starts");
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn create_topic(bootstrap: &str, name: &str) {
    let mut admin = AdminClient::connect(&[bootstrap.to_owned()])
        .await
        .expect("admin connects");
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: name.into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .expect("topic is created");
}

async fn gateway_state(bootstrap: &str, client_id: &str) -> Arc<AppState> {
    let produce = ProduceCore::new(bootstrap, client_id, Arc::new(RawCodec), None)
        .await
        .expect("produce core starts");
    Arc::new(AppState {
        produce: Arc::new(produce),
        config: Arc::new(gateway_config(bootstrap, client_id)),
        authz: Arc::new(GatewayAuthz::new(Arc::new(
            crabka_authz::AllowAllAuthorizer,
        ))),
        codec: Arc::new(RawCodec),
        queue_sessions: AppState::queue_sessions_from_config(&gateway_config(bootstrap, client_id)),
    })
}

fn gateway_config(bootstrap: &str, client_id: &str) -> GatewayConfig {
    GatewayConfig {
        bootstrap: bootstrap.to_owned(),
        listen_addr: "127.0.0.1:0".parse().expect("listen address parses"),
        client_id: client_id.to_owned(),
        dedup_topic: "__crabka_ce_roundtrip_dedup".into(),
        dedup_partitions: 4,
        dedup_window_ms: 3_600_000,
        dedup_txn_id_prefix: format!("{client_id}-dedup"),
        advertised_addr: "127.0.0.1:0".into(),
        membership_topic: "__crabka_ce_roundtrip_membership".into(),
        tls: None,
        broker_security: None,
        authz: None,
        webhooks: std::collections::HashMap::new(),
        outbound: Vec::new(),
        schema_registry_url: None,
        queue_max_messages: GatewayConfig::DEFAULT_QUEUE_MAX_MESSAGES,
        queue_wait_ms_cap: GatewayConfig::DEFAULT_QUEUE_WAIT_MS_CAP,
        queue_session_idle_secs: GatewayConfig::DEFAULT_QUEUE_SESSION_IDLE_SECS,
        queue_max_sessions: GatewayConfig::DEFAULT_QUEUE_MAX_SESSIONS,
    }
}

async fn read_records(bootstrap: &str, topic: &str, group: &str) -> Vec<ConsumerRecord> {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.to_owned())
        .client_id(format!("{group}-client"))
        .group_id(group.to_owned())
        .subscribe(vec![topic.to_owned()])
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("consumer builds");
    let mut records = Vec::new();
    for _ in 0..20 {
        records.extend(
            consumer
                .poll(Duration::from_millis(250))
                .await
                .expect("poll succeeds"),
        );
        if !records.is_empty() {
            break;
        }
    }
    consumer.close().await.expect("consumer closes");
    records
}

fn header_value<'a>(record: &'a ConsumerRecord, key: &str) -> Option<&'a [u8]> {
    record
        .headers
        .iter()
        .find(|header| header.key == key)
        .and_then(|header| header.value.as_deref())
}

async fn spawn_capture_server(state: Arc<CaptureState>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("capture listener binds");
    let addr = listener.local_addr().expect("local address").to_string();
    let app = Router::new()
        .route("/hook", post(capture_handler))
        .with_state(state);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    addr
}

fn subscription(
    name: &str,
    topic: &str,
    addr: &str,
    content_mode: OutboundContentMode,
) -> CompiledSubscription {
    CompiledSubscription {
        name: name.into(),
        source_topics: vec![topic.into()],
        target_url: format!("http://{addr}/hook"),
        signing_secret: None,
        dead_letter_topic: None,
        max_attempts: 3,
        base_backoff_ms: 50,
        max_backoff_ms: 200,
        request_timeout_ms: 2_000,
        filter: None,
        headers: vec![],
        content_mode,
        decode_to_json: false,
    }
}

async fn run_outbound_until_captured(
    bootstrap: &str,
    topic: &str,
    subscription_name: &str,
    content_mode: OutboundContentMode,
) -> CapturedRequest {
    let state = Arc::new(CaptureState::default());
    let addr = spawn_capture_server(state.clone()).await;
    let token = CancellationToken::new();
    let handle = tokio::spawn(outbound::run_subscription(
        subscription(subscription_name, topic, &addr, content_mode),
        bootstrap.to_owned(),
        format!("{subscription_name}-client"),
        Arc::new(make_producer(bootstrap).await),
        token.clone(),
        None,
        Arc::new(RawCodec),
    ));

    let captured = wait_for_captured_request(&state).await;
    token.cancel();
    handle
        .await
        .expect("outbound task joins")
        .expect("outbound exits");
    captured
}

async fn wait_for_captured_request(state: &Arc<CaptureState>) -> CapturedRequest {
    for _ in 0..80 {
        if let Some(request) = state.requests.lock().await.first().cloned() {
            return request;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("expected captured webhook request");
}

async fn make_producer(bootstrap: &str) -> Producer {
    Producer::builder()
        .bootstrap(bootstrap.to_owned())
        .client_id("ce-roundtrip-producer")
        .enable_idempotence(true)
        .acks(Acks::All)
        .build()
        .await
        .expect("producer builds")
}

async fn post_binary_cloudevent(bootstrap: &str, topic: &str) {
    let state = gateway_state(bootstrap, "ce-binary-ingress").await;
    let app = webhook_router(state);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/produce/{topic}"))
                .header("ce-id", CE_ID)
                .header("ce-source", CE_SOURCE)
                .header("ce-type", CE_TYPE)
                .header("ce-specversion", CE_SPECVERSION)
                .header("content-type", "application/avro")
                .body(Body::from(Bytes::from_static(b"avro-bytes")))
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::OK);
}

async fn post_structured_cloudevent(bootstrap: &str, topic: &str) {
    let state = gateway_state(bootstrap, "ce-structured-ingress").await;
    let app = webhook_router(state);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/produce/{topic}"))
                .header("content-type", "application/cloudevents+json; charset=UTF-8")
                .body(Body::from(Bytes::from_static(
                    br#"{"specversion":"1.0","id":"roundtrip-1","source":"/roundtrip","type":"com.example.roundtrip","datacontenttype":"application/json","traceid":"abc123","data":{"n":7}}"#,
                )))
                .expect("request builds"),
        )
        .await
        .expect("request succeeds");
    assert_eq!(response.status(), StatusCode::OK);
}

fn assert_kafka_cloudevent_headers(record: &ConsumerRecord, content_type: &[u8]) {
    assert_eq!(header_value(record, "ce_id"), Some(CE_ID.as_bytes()));
    assert_eq!(
        header_value(record, "ce_source"),
        Some(CE_SOURCE.as_bytes())
    );
    assert_eq!(header_value(record, "ce_type"), Some(CE_TYPE.as_bytes()));
    assert_eq!(
        header_value(record, "ce_specversion"),
        Some(CE_SPECVERSION.as_bytes())
    );
    assert_eq!(header_value(record, "content-type"), Some(content_type));
    assert_eq!(header_value(record, "ce_datacontenttype"), None);
}

fn assert_http_cloudevent_headers(request: &CapturedRequest, content_type: &str) {
    assert_eq!(
        request.ce_headers.get("ce-id").map(String::as_str),
        Some(CE_ID)
    );
    assert_eq!(
        request.ce_headers.get("ce-source").map(String::as_str),
        Some(CE_SOURCE)
    );
    assert_eq!(
        request.ce_headers.get("ce-type").map(String::as_str),
        Some(CE_TYPE)
    );
    assert_eq!(
        request.ce_headers.get("ce-specversion").map(String::as_str),
        Some(CE_SPECVERSION)
    );
    assert_eq!(request.ce_headers.get("ce-datacontenttype"), None);
    assert_eq!(request.content_type.as_deref(), Some(content_type));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binary_http_cloudevent_roundtrips_through_kafka_to_webhook_modes() {
    let (broker, bootstrap, _dir) = boot().await;
    let topic = "ce-roundtrip-binary";
    create_topic(&bootstrap, topic).await;

    post_binary_cloudevent(&bootstrap, topic).await;

    let records = read_records(&bootstrap, topic, "ce-binary-verify").await;
    let record = records.first().expect("record should be produced");
    assert_eq!(record.value.as_deref(), Some(&b"avro-bytes"[..]));
    assert_kafka_cloudevent_headers(record, b"application/avro");

    let binary_request = run_outbound_until_captured(
        &bootstrap,
        topic,
        "ce-roundtrip-binary-egress",
        OutboundContentMode::CloudEventsBinary,
    )
    .await;
    assert_eq!(binary_request.body, b"avro-bytes");
    assert_http_cloudevent_headers(&binary_request, "application/avro");

    let structured_request = run_outbound_until_captured(
        &bootstrap,
        topic,
        "ce-roundtrip-structured-egress",
        OutboundContentMode::CloudEventsStructured,
    )
    .await;
    assert_eq!(
        structured_request.content_type.as_deref(),
        Some("application/cloudevents+json")
    );
    let structured_body: Value =
        serde_json::from_slice(&structured_request.body).expect("structured CloudEvent JSON");
    assert_eq!(structured_body["id"], CE_ID);
    assert_eq!(structured_body["source"], CE_SOURCE);
    assert_eq!(structured_body["type"], CE_TYPE);
    assert_eq!(structured_body["specversion"], CE_SPECVERSION);
    assert_eq!(structured_body["datacontenttype"], "application/avro");
    assert!(structured_body["data_base64"].is_string());
    assert!(structured_body.get("data").is_none());

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn structured_http_cloudevent_becomes_binary_kafka_record_and_webhook_request() {
    let (broker, bootstrap, _dir) = boot().await;
    let topic = "ce-roundtrip-structured";
    create_topic(&bootstrap, topic).await;

    post_structured_cloudevent(&bootstrap, topic).await;

    let records = read_records(&bootstrap, topic, "ce-structured-verify").await;
    let record = records.first().expect("record should be produced");
    assert_eq!(record.value.as_deref(), Some(&br#"{"n":7}"#[..]));
    assert_kafka_cloudevent_headers(record, b"application/json");
    assert_eq!(header_value(record, "ce_traceid"), Some(&b"abc123"[..]));

    let binary_request = run_outbound_until_captured(
        &bootstrap,
        topic,
        "ce-structured-to-binary-egress",
        OutboundContentMode::CloudEventsBinary,
    )
    .await;
    assert_eq!(binary_request.body, br#"{"n":7}"#);
    assert_http_cloudevent_headers(&binary_request, "application/json");
    assert_eq!(
        binary_request
            .ce_headers
            .get("ce-traceid")
            .map(String::as_str),
        Some("abc123")
    );

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_subscribe_preserves_cloudevent_kafka_header_names() {
    let (broker, bootstrap, _dir) = boot().await;
    let topic = "ce-roundtrip-grpc";
    create_topic(&bootstrap, topic).await;
    let producer = make_producer(&bootstrap).await;
    producer
        .send(ProducerRecord {
            topic: topic.into(),
            value: Some(Bytes::from_static(b"hello")),
            headers: vec![
                ProducerHeader {
                    key: "ce_id".into(),
                    value: Some(Bytes::from_static(CE_ID.as_bytes())),
                },
                ProducerHeader {
                    key: "ce_source".into(),
                    value: Some(Bytes::from_static(CE_SOURCE.as_bytes())),
                },
                ProducerHeader {
                    key: "ce_type".into(),
                    value: Some(Bytes::from_static(CE_TYPE.as_bytes())),
                },
                ProducerHeader {
                    key: "ce_specversion".into(),
                    value: Some(Bytes::from_static(CE_SPECVERSION.as_bytes())),
                },
                ProducerHeader {
                    key: "content-type".into(),
                    value: Some(Bytes::from_static(b"text/plain")),
                },
            ],
            ..ProducerRecord::default()
        })
        .await
        .await
        .expect("send joins")
        .expect("send succeeds");

    let state = gateway_state(&bootstrap, "ce-grpc-subscribe").await;
    let start = pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Start(pb::SubscribeStart {
            group_id: "ce-roundtrip-grpc-group".into(),
            topics: vec![topic.into()],
            auto_commit: true,
            filter: String::new(),
        })),
    };
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<
        Result<pb::SubscribeFrame, connectrpc_axum::message::ConnectError>,
    >();
    tx.send(Ok(start)).expect("subscribe start sends");
    let inbound = Streaming::new(Box::pin(
        tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
    ));
    let mut out = Box::pin(streaming::subscribe_inner(
        inbound,
        state,
        anonymous_principal(),
        "0.0.0.0:0".parse().expect("peer address parses"),
    ));
    let message = tokio::time::timeout(Duration::from_secs(10), out.next())
        .await
        .expect("record arrives")
        .expect("stream item")
        .expect("inbound ok");
    drop(tx);

    assert_eq!(message.value, b"hello");
    assert_eq!(
        message.headers,
        vec![
            pb::Header {
                key: "ce_id".into(),
                value: Some(CE_ID.as_bytes().to_vec()),
            },
            pb::Header {
                key: "ce_source".into(),
                value: Some(CE_SOURCE.as_bytes().to_vec()),
            },
            pb::Header {
                key: "ce_type".into(),
                value: Some(CE_TYPE.as_bytes().to_vec()),
            },
            pb::Header {
                key: "ce_specversion".into(),
                value: Some(CE_SPECVERSION.as_bytes().to_vec()),
            },
            pb::Header {
                key: "content-type".into(),
                value: Some(b"text/plain".to_vec()),
            },
        ]
    );

    broker.shutdown().await;
}

fn anonymous_principal() -> crabka_security::Principal {
    crabka_security::Principal {
        name: "ANONYMOUS".into(),
        auth_method: crabka_security::AuthMethod::Anonymous,
        groups: vec![],
    }
}
