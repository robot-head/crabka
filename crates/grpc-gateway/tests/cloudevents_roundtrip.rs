use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use assert2::check;
use axum::{
    Router,
    body::{Body, Bytes as AxumBytes},
    extract::State,
    http::{HeaderMap, Request, StatusCode},
    routing::post,
};
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerRecord, IsolationLevel};
use crabka_client_producer::{Acks, Producer};
use crabka_grpc_gateway::{
    authz::GatewayAuthz,
    codec::RawCodec,
    config::{GatewayConfig, GatewayRuntimeConfig},
    outbound,
    outbound_config::{CompiledSubscription, OutboundContentMode},
    produce::ProduceCore,
    state::AppState,
    webhook::webhook_router,
};
use crabka_units::prelude::*;
use hmac::{Hmac, KeyInit, Mac};
use serde_json::Value;
use sha2::Sha256;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt as _;

const CE_ID: &str = "roundtrip-1";
const CE_SOURCE: &str = "/roundtrip";
const CE_TYPE: &str = "com.example.roundtrip";
const STRUCTURED_CONTENT_TYPE: &str = "application/cloudevents+json; charset=UTF-8";

#[derive(Clone, Debug)]
struct CapturedRequest {
    body: Vec<u8>,
    headers: BTreeMap<String, String>,
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
    let headers = headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_owned(), value.to_owned()))
        })
        .collect();
    state
        .requests
        .lock()
        .expect("capture lock")
        .push(CapturedRequest {
            body: body.to_vec(),
            headers,
        });
    StatusCode::OK
}

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().expect("temporary broker directory");
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .expect("broker starts");
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn create_topic(bootstrap: &str, topic: &str) {
    let mut admin = AdminClient::connect(&[bootstrap.to_owned()])
        .await
        .expect("admin connects");
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: topic.to_owned(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            secs(10),
        )
        .await
        .expect("topic is created");
}

fn gateway_config(bootstrap: &str, client_id: &str) -> GatewayConfig {
    GatewayConfig {
        bootstrap: bootstrap.to_owned(),
        listen_addr: "127.0.0.1:0".parse().expect("listen address"),
        client_id: client_id.to_owned(),
        dedup_topic: "__crabka_ce_roundtrip_dedup".into(),
        dedup_partitions: 4,
        dedup_window: hours(1),
        dedup_ownership_group: "__crabka_ce_roundtrip_owners".into(),
        dedup_txn_id_prefix: format!("{client_id}-dedup"),
        advertised_addr: "127.0.0.1:0".into(),
        membership_topic: "__crabka_ce_roundtrip_membership".into(),
        tls: None,
        broker_security: None,
        authz: None,
        webhooks: std::collections::HashMap::new(),
        outbound: Vec::new(),
        schema_registry_url: None,
        runtime: GatewayRuntimeConfig::default(),
    }
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
        queue: Arc::default(),
    })
}

async fn read_one(bootstrap: &str, topic: &str, group: &str) -> ConsumerRecord {
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
    for _ in 0..40 {
        let records = consumer.poll(millis(250)).await.expect("poll succeeds");
        if let Some(record) = records.into_iter().next() {
            consumer.close().await.expect("consumer closes");
            return record;
        }
    }
    consumer.close().await.expect("consumer closes");
    panic!("record was not consumed");
}

fn record_header<'a>(record: &'a ConsumerRecord, key: &str) -> Option<&'a [u8]> {
    record
        .headers
        .iter()
        .find(|header| header.key == key)
        .and_then(|header| header.value.as_deref())
}

async fn make_producer(bootstrap: &str) -> Producer {
    Producer::builder()
        .bootstrap(bootstrap.to_owned())
        .client_id("ce-roundtrip-dlq-producer")
        .enable_idempotence(true)
        .acks(Acks::All)
        .build()
        .await
        .expect("producer builds")
}

async fn spawn_capture_server(state: Arc<CaptureState>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("capture listener binds");
    let address = listener.local_addr().expect("capture address").to_string();
    let router = Router::new()
        .route("/hook", post(capture_handler))
        .with_state(state);
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    address
}

fn subscription(
    name: &str,
    topic: &str,
    address: &str,
    content_mode: OutboundContentMode,
    signing_secret: Option<Vec<u8>>,
) -> CompiledSubscription {
    CompiledSubscription {
        name: name.into(),
        group_id: format!("__crabka_ce_{name}"),
        source_topics: vec![topic.into()],
        target_url: format!("http://{address}/hook"),
        signing_secret,
        dead_letter_topic: None,
        max_attempts: 3,
        base_backoff: millis(50),
        max_backoff: millis(200),
        request_timeout: secs(2),
        filter: None,
        headers: Vec::new(),
        content_mode,
        decode_to_json: false,
    }
}

async fn deliver_one(
    bootstrap: &str,
    topic: &str,
    name: &str,
    mode: OutboundContentMode,
    signing_secret: Option<Vec<u8>>,
) -> CapturedRequest {
    let state = Arc::new(CaptureState::default());
    let address = spawn_capture_server(state.clone()).await;
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(outbound::run_subscription(
        subscription(name, topic, &address, mode, signing_secret),
        bootstrap.to_owned(),
        format!("{name}-client"),
        Arc::new(make_producer(bootstrap).await),
        shutdown.clone(),
        (None, millis(250)),
        Arc::new(RawCodec),
    ));

    let mut captured = None;
    for _ in 0..400 {
        captured = state
            .requests
            .lock()
            .expect("capture lock")
            .first()
            .cloned();
        if captured.is_some() {
            break;
        }
        tokio::time::sleep(millis(50).to_std()).await;
    }
    shutdown.cancel();
    task.await
        .expect("outbound task joins")
        .expect("outbound exits");
    captured.expect("outbound request is captured")
}

async fn post_to_gateway(bootstrap: &str, topic: &str, request: Request<Body>) {
    let response = webhook_router(gateway_state(bootstrap, topic).await)
        .oneshot(request)
        .await
        .expect("HTTP produce request completes");
    check!(response.status() == StatusCode::OK);
}

fn verify_signature(secret: &[u8], body: &[u8], signature: &str) -> bool {
    let mut mac = <Hmac<Sha256>>::new_from_slice(secret).expect("HMAC key");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes()) == signature
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binary_http_kafka_http_round_trip_preserves_binding() {
    let (broker, bootstrap, _dir) = boot().await;
    let topic = "ce-roundtrip-binary";
    let opaque_body = br#"{ "looks": "json" }"#;
    create_topic(&bootstrap, topic).await;

    post_to_gateway(
        &bootstrap,
        topic,
        Request::post(format!("/v1/produce/{topic}"))
            .header("ce-id", CE_ID)
            .header("ce-source", CE_SOURCE)
            .header("ce-type", CE_TYPE)
            .header("ce-specversion", "1.0")
            .header("content-type", "application/avro")
            .body(Body::from(Bytes::from_static(opaque_body)))
            .expect("request"),
    )
    .await;

    let record = read_one(&bootstrap, topic, "ce-binary-inspect").await;
    check!(record.value.as_deref() == Some(opaque_body.as_slice()));
    check!(record_header(&record, "ce_id") == Some(CE_ID.as_bytes()));
    check!(record_header(&record, "ce_source") == Some(CE_SOURCE.as_bytes()));
    check!(record_header(&record, "ce_type") == Some(CE_TYPE.as_bytes()));
    check!(record_header(&record, "ce_specversion") == Some(b"1.0".as_slice()));
    check!(record_header(&record, "content-type") == Some(b"application/avro".as_slice()));
    check!(record_header(&record, "ce_datacontenttype").is_none());

    let secret = b"roundtrip-secret".to_vec();
    let binary = deliver_one(
        &bootstrap,
        topic,
        "ce-binary-out",
        OutboundContentMode::CloudEventsBinary,
        Some(secret.clone()),
    )
    .await;
    check!(binary.body == opaque_body);
    check!(binary.headers.get("ce-id").map(String::as_str) == Some(CE_ID));
    check!(binary.headers.get("ce-source").map(String::as_str) == Some(CE_SOURCE));
    check!(binary.headers.get("ce-type").map(String::as_str) == Some(CE_TYPE));
    check!(binary.headers.get("ce-specversion").map(String::as_str) == Some("1.0"));
    check!(binary.headers.get("content-type").map(String::as_str) == Some("application/avro"));
    check!(!binary.headers.contains_key("ce-datacontenttype"));
    let signature = binary
        .headers
        .get("x-crabka-signature")
        .expect("signature header");
    check!(verify_signature(&secret, &binary.body, signature));

    let structured = deliver_one(
        &bootstrap,
        topic,
        "ce-structured-out-from-binary",
        OutboundContentMode::CloudEventsStructured,
        None,
    )
    .await;
    check!(
        structured.headers.get("content-type").map(String::as_str) == Some(STRUCTURED_CONTENT_TYPE)
    );
    let event: Value = serde_json::from_slice(&structured.body).expect("structured CloudEvent");
    check!(event["id"] == CE_ID);
    check!(event["datacontenttype"] == "application/avro");
    check!(event["data_base64"] == "eyAibG9va3MiOiAianNvbiIgfQ==");

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn structured_http_kafka_http_round_trip_is_verbatim() {
    let (broker, bootstrap, _dir) = boot().await;
    let topic = "ce-roundtrip-structured";
    create_topic(&bootstrap, topic).await;
    let body = Bytes::from_static(
        br#"{ "specversion":"1.0", "id":"roundtrip-1", "source":"/roundtrip", "type":"com.example.roundtrip", "datacontenttype":"application/json", "traceid":"abc123", "data":{"n":7} }"#,
    );

    post_to_gateway(
        &bootstrap,
        topic,
        Request::post(format!("/v1/produce/{topic}"))
            .header("content-type", STRUCTURED_CONTENT_TYPE)
            .body(Body::from(body.clone()))
            .expect("request"),
    )
    .await;

    let record = read_one(&bootstrap, topic, "ce-structured-inspect").await;
    check!(record.value.as_deref() == Some(body.as_ref()));
    check!(record.headers.len() == 1);
    check!(record_header(&record, "content-type") == Some(STRUCTURED_CONTENT_TYPE.as_bytes()));

    let structured = deliver_one(
        &bootstrap,
        topic,
        "ce-structured-out",
        OutboundContentMode::CloudEventsStructured,
        None,
    )
    .await;
    check!(structured.body == body);
    check!(
        structured.headers.get("content-type").map(String::as_str) == Some(STRUCTURED_CONTENT_TYPE)
    );

    let binary = deliver_one(
        &bootstrap,
        topic,
        "ce-binary-out-from-structured",
        OutboundContentMode::CloudEventsBinary,
        None,
    )
    .await;
    check!(binary.body == br#"{"n":7}"#);
    check!(binary.headers.get("ce-id").map(String::as_str) == Some(CE_ID));
    check!(binary.headers.get("ce-traceid").map(String::as_str) == Some("abc123"));
    check!(binary.headers.get("content-type").map(String::as_str) == Some("application/json"));

    broker.shutdown().await;
}
