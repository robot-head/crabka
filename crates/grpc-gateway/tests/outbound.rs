//! End-to-end coverage for the outbound webhook delivery engine
//! (`outbound::run_subscription`). A real in-process broker holds the source
//! topic; the subscription tails it and HTTP-posts each record to a mock
//! receiver as a signed JSON envelope.
//!
//! What the tests prove:
//! - **2xx delivery**: each source record reaches the receiver exactly once
//!   per attempt, with `X-Crabka-Event-Id = topic-partition-offset`, a verifying
//!   `X-Crabka-Signature`, and a well-formed envelope body.
//! - **at-least-once / retry**: a transient 5xx run is retried with backoff and
//!   eventually delivered (the same event is sent ≥ 3 times) without
//!   dead-lettering.
//! - **DLQ on exhaustion**: a permanently-failing target dead-letters the
//!   record (value + `x-crabka-dlq-source` header) after `max_attempts`, and the
//!   loop keeps polling (a later record also reaches the DLQ — no wedge).
//! - **ordering**: within one partition, records are delivered in
//!   ascending-offset order.
//! - **filter**: a `json:` filter skips non-matching records (only the matching
//!   one is delivered).
//! - **SSRF guard**: a target host outside `allowed_targets` fails to compile.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU16, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::Bytes as AxumBytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_core::Client;
use crabka_client_producer::{Acks, Header, Producer, ProducerRecord};
use crabka_grpc_gateway::{
    codec::RawCodec,
    outbound,
    outbound_config::{CompiledSubscription, OutboundContentMode, OutboundFile},
};
use crabka_protocol::owned::{
    fetch_request::{FetchPartition, FetchRequest, FetchTopic},
    metadata_request::MetadataRequest,
};
use hmac::{Hmac, KeyInit, Mac};
use serde_json::Value;
use sha2::Sha256;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn create_topic(bootstrap: &str, name: &str, partitions: i32) {
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap.to_string()))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: name.into(),
                partitions,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();
}

async fn make_producer(bootstrap: &str) -> Producer {
    Producer::builder()
        .bootstrap(bootstrap.to_string())
        .client_id("outbound-test-producer")
        .enable_idempotence(true)
        .acks(Acks::All)
        .build()
        .await
        .unwrap()
}

/// One captured POST to the mock receiver.
#[derive(Debug, Clone)]
struct Received {
    body: Vec<u8>,
    event_id: Option<String>,
    signature: Option<String>,
    timestamp: Option<String>,
    content_type: Option<String>,
    ce_headers: BTreeMap<String, String>,
}

/// Shared mock-receiver state: the captured request log plus the status to
/// return. `queue` (when non-empty) pops one status per request (for the retry
/// test); otherwise `default` is returned for every request.
#[derive(Default)]
struct MockState {
    received: Mutex<Vec<Received>>,
    queue: Mutex<VecDeque<StatusCode>>,
    default: AtomicU16,
}

async fn mock_handler(
    State(state): State<Arc<MockState>>,
    headers: HeaderMap,
    body: AxumBytes,
) -> StatusCode {
    let get = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    let ce_headers = headers
        .iter()
        .filter_map(|(name, value)| {
            let header_name = name.as_str();
            if !header_name.starts_with("ce-") {
                return None;
            }
            Some((
                header_name.to_string(),
                value.to_str().ok().unwrap_or_default().to_string(),
            ))
        })
        .collect();
    state.received.lock().unwrap().push(Received {
        body: body.to_vec(),
        event_id: get("X-Crabka-Event-Id"),
        signature: get("X-Crabka-Signature"),
        timestamp: get("X-Crabka-Timestamp"),
        content_type: get("content-type"),
        ce_headers,
    });
    // A queued status wins (per-request); else the shared default (200 if unset).
    if let Some(code) = state.queue.lock().unwrap().pop_front() {
        return code;
    }
    let raw = state.default.load(Ordering::SeqCst);
    if raw == 0 {
        StatusCode::OK
    } else {
        StatusCode::from_u16(raw).unwrap_or(StatusCode::OK)
    }
}

/// Spawn a mock HTTP receiver on `127.0.0.1:0` with a single `POST /hook` route.
/// Returns the bound address and the shared state handle.
async fn spawn_mock(state: Arc<MockState>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let app = Router::new()
        .route("/hook", post(mock_handler))
        .with_state(state);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    // real-time wait (not a progress poll): readiness settle for the spawned mock HTTP receiver, not in-process gateway state.
    // Small readiness pause so the first request doesn't race serve startup.
    tokio::time::sleep(Duration::from_millis(150)).await;
    addr
}

/// Build a `CompiledSubscription` with test-fast backoff. All fields are pub, so
/// we construct directly rather than round-tripping through TOML.
fn sub(
    name: &str,
    source_topic: &str,
    addr: &str,
    signing_secret: Option<Vec<u8>>,
    dead_letter_topic: Option<String>,
    max_attempts: u32,
    filter: Option<jsonpath_rust::parser::model::JpQuery>,
) -> CompiledSubscription {
    CompiledSubscription {
        name: name.into(),
        source_topics: vec![source_topic.into()],
        target_url: format!("http://{addr}/hook"),
        signing_secret,
        dead_letter_topic,
        max_attempts,
        base_backoff_ms: 50,
        max_backoff_ms: 200,
        request_timeout_ms: 2_000,
        filter,
        headers: vec![],
        content_mode: OutboundContentMode::Envelope,
        decode_to_json: false,
    }
}

/// Poll `cond` up to ~20s (80 × 250ms). Returns whether it became true.
async fn wait_until<F: FnMut() -> bool>(mut cond: F) -> bool {
    for _ in 0..80 {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    cond()
}

fn received_len(state: &Arc<MockState>) -> usize {
    state.received.lock().unwrap().len()
}

/// Verify a lowercase-hex HMAC-SHA256 signature over `body` with `secret`. This
/// mirrors `webhook_config::verify_signature` (which is `pub(crate)`, so not
/// reachable from this integration test) for the engine-produced
/// `X-Crabka-Signature`.
fn verify_sig_hex(secret: &[u8], body: &[u8], provided: &str) -> bool {
    let mut mac = <Hmac<Sha256>>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(body);
    let expected = hex::encode(mac.finalize().into_bytes());
    // The header value the engine emits is plain lowercase hex (no prefix).
    expected == provided
}

async fn produce_value(producer: &Producer, topic: &str, value: &[u8]) {
    produce_value_with_headers(producer, topic, value, vec![]).await;
}

async fn produce_value_with_headers(
    producer: &Producer,
    topic: &str,
    value: &[u8],
    headers: Vec<Header>,
) {
    let rec = ProducerRecord {
        topic: topic.into(),
        partition: None,
        key: None,
        value: Some(Bytes::from(value.to_vec())),
        headers,
        timestamp_ms: None,
    };
    producer.send(rec).await.await.unwrap().unwrap();
}

fn ce_headers() -> Vec<Header> {
    vec![
        Header {
            key: "ce_id".into(),
            value: Some(Bytes::from_static(b"event-1")),
        },
        Header {
            key: "ce_source".into(),
            value: Some(Bytes::from_static(b"/tests")),
        },
        Header {
            key: "ce_type".into(),
            value: Some(Bytes::from_static(b"com.example.test")),
        },
        Header {
            key: "ce_specversion".into(),
            value: Some(Bytes::from_static(b"1.0")),
        },
        Header {
            key: "content-type".into(),
            value: Some(Bytes::from_static(b"application/json")),
        },
    ]
}

/// Decoded DLQ record: value plus the `x-crabka-dlq-source` header (the group
/// `Consumer` drops headers, so we issue a raw single-partition `Fetch` and
/// decode the v2 `RecordBatch` ourselves).
#[derive(Debug, Clone)]
struct DlqRecord {
    value: Option<Vec<u8>>,
    dlq_source: Option<String>,
}

/// Raw-fetch partition 0 of `topic` from offset 0 and decode every user record
/// (offset, value, and the `x-crabka-dlq-source` header). One in-process broker
/// ⇒ the bootstrap connection is the leader, so a direct `Fetch` suffices.
async fn fetch_dlq(client: &Client, topic: &str) -> Vec<DlqRecord> {
    // Resolve the topic_id (Fetch v13 keys partitions by topic_id).
    let md = client.send(MetadataRequest::default()).await.unwrap();
    let topic_id = md
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic))
        .map(|t| t.topic_id)
        .unwrap_or_default();

    let resp = client
        .send(FetchRequest {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 50 * 1024 * 1024,
            topics: vec![FetchTopic {
                topic: topic.to_string(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 10 * 1024 * 1024,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();

    let mut out = Vec::new();
    for t in &resp.responses {
        for p in &t.partitions {
            if p.partition_index != 0 {
                continue;
            }
            let Some(payload) = &p.records else { continue };
            let Some(batches) = payload.as_v2() else {
                continue;
            };
            for batch in batches {
                if batch.attributes.is_control_batch() {
                    continue;
                }
                for r in &batch.records {
                    let dlq_source = r
                        .headers
                        .iter()
                        .find(|h| h.key == "x-crabka-dlq-source")
                        .and_then(|h| h.value.as_ref())
                        .map(|v| String::from_utf8_lossy(v).into_owned());
                    out.push(DlqRecord {
                        value: r.value.as_ref().map(|v| v.to_vec()),
                        dlq_source,
                    });
                }
            }
        }
    }
    out
}

/// Poll the DLQ topic via raw fetch until it holds ≥ `n` records (~20s budget).
async fn wait_for_dlq(client: &Client, topic: &str, n: usize) -> Vec<DlqRecord> {
    let mut last = Vec::new();
    for _ in 0..80 {
        last = fetch_dlq(client, topic).await;
        if last.len() >= n {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    last
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// 3 records, mock always 200: each is delivered once with the right event id,
/// a verifying signature, and a well-formed envelope body.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_2xx() {
    let topic = "outbound-2xx";
    let secret = b"out-2xx-secret".to_vec();
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, topic, 1).await;

    let producer = make_producer(&bootstrap).await;
    // Produce BEFORE the subscription joins; it's Earliest, so it reads from 0.
    for i in 0..3u8 {
        produce_value(&producer, topic, format!(r#"{{"n":{i}}}"#).as_bytes()).await;
    }

    let state = Arc::new(MockState::default());
    let addr = spawn_mock(state.clone()).await;
    let s = sub("twoxx", topic, &addr, Some(secret.clone()), None, 5, None);
    let token = CancellationToken::new();
    // The DLQ producer is unused here (no DLQ configured) but the signature
    // requires one; reuse the same producer behind an Arc.
    let dlq_producer = Arc::new(make_producer(&bootstrap).await);
    let handle = tokio::spawn(outbound::run_subscription(
        s,
        bootstrap.clone(),
        "test-out".into(),
        dlq_producer,
        token.clone(),
        None,
        Arc::new(RawCodec),
    ));

    assert!(
        wait_until(|| received_len(&state) >= 3).await,
        "expected 3 deliveries, got {}",
        received_len(&state)
    );

    let recv = state.received.lock().unwrap().clone();
    // Collect the offsets seen so we can assert event ids match topic-part-offset.
    let mut offsets_seen = Vec::new();
    for r in &recv {
        let event_id = r.event_id.as_deref().expect("X-Crabka-Event-Id present");
        // event_id == "{topic}-{partition}-{offset}"
        let prefix = format!("{topic}-0-");
        let off: i64 = event_id
            .strip_prefix(&prefix)
            .unwrap_or_else(|| panic!("event id {event_id:?} should start with {prefix:?}"))
            .parse()
            .expect("offset suffix is an integer");
        offsets_seen.push(off);

        // Signature verifies over the exact body bytes.
        let sig = r.signature.as_deref().expect("X-Crabka-Signature present");
        assert!(
            verify_sig_hex(&secret, &r.body, sig),
            "signature must verify for event {event_id}"
        );
        assert!(r.timestamp.is_some(), "X-Crabka-Timestamp present");

        // Envelope is well-formed: topic + offset + embedded JSON value.
        let v: Value = serde_json::from_slice(&r.body).expect("body is a JSON envelope");
        assert_eq!(v["topic"], topic);
        assert_eq!(v["partition"], 0);
        assert_eq!(v["offset"], off);
        assert_eq!(v["event_id"], event_id);
        // The produced value was raw JSON {"n":k}, embedded as an object.
        assert!(v["value"]["n"].is_number(), "value embedded as raw JSON");
    }
    offsets_seen.sort_unstable();
    assert_eq!(
        offsets_seen,
        vec![0, 1, 2],
        "the three offsets 0,1,2 delivered"
    );

    token.cancel();
    let _ = handle.await;
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_cloudevents_binary() {
    let topic = "outbound-ce-binary";
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, topic, 1).await;

    let producer = make_producer(&bootstrap).await;
    produce_value_with_headers(&producer, topic, br#"{"n":1}"#, ce_headers()).await;

    let state = Arc::new(MockState::default());
    let addr = spawn_mock(state.clone()).await;
    let mut s = sub("ce-binary", topic, &addr, None, None, 5, None);
    s.content_mode = OutboundContentMode::CloudEventsBinary;
    let token = CancellationToken::new();
    let handle = tokio::spawn(outbound::run_subscription(
        s,
        bootstrap.clone(),
        "test-out".into(),
        Arc::new(make_producer(&bootstrap).await),
        token.clone(),
        None,
        Arc::new(RawCodec),
    ));

    assert!(wait_until(|| received_len(&state) >= 1).await);
    let recv = state.received.lock().unwrap().clone();
    let request = &recv[0];
    assert_eq!(request.body, br#"{"n":1}"#);
    assert_eq!(request.content_type.as_deref(), Some("application/json"));
    assert_eq!(
        request.ce_headers.get("ce-id").map(String::as_str),
        Some("event-1")
    );
    assert_eq!(
        request.ce_headers.get("ce-source").map(String::as_str),
        Some("/tests")
    );

    token.cancel();
    let _ = handle.await;
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_cloudevents_structured() {
    let topic = "outbound-ce-structured";
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, topic, 1).await;

    let producer = make_producer(&bootstrap).await;
    produce_value_with_headers(&producer, topic, br#"{"n":1}"#, ce_headers()).await;

    let state = Arc::new(MockState::default());
    let addr = spawn_mock(state.clone()).await;
    let mut s = sub("ce-structured", topic, &addr, None, None, 5, None);
    s.content_mode = OutboundContentMode::CloudEventsStructured;
    let token = CancellationToken::new();
    let handle = tokio::spawn(outbound::run_subscription(
        s,
        bootstrap.clone(),
        "test-out".into(),
        Arc::new(make_producer(&bootstrap).await),
        token.clone(),
        None,
        Arc::new(RawCodec),
    ));

    assert!(wait_until(|| received_len(&state) >= 1).await);
    let recv = state.received.lock().unwrap().clone();
    let request = &recv[0];
    let body: Value = serde_json::from_slice(&request.body).expect("structured CloudEvent JSON");
    assert_eq!(
        request.content_type.as_deref(),
        Some("application/cloudevents+json")
    );
    assert_eq!(body["id"], "event-1");
    assert_eq!(body["source"], "/tests");
    assert_eq!(body["type"], "com.example.test");
    assert_eq!(body["data"]["n"], 1);

    token.cancel();
    let _ = handle.await;
    broker.shutdown().await;
}

/// Mock 500 for the first 2 requests then 200: the record is retried and
/// eventually delivered (≥ 3 requests for the one event), and the configured DLQ
/// stays empty.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retries_then_succeeds() {
    let topic = "outbound-retry";
    let dlq = "outbound-retry-dlq";
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, topic, 1).await;
    create_topic(&bootstrap, dlq, 1).await;

    let producer = make_producer(&bootstrap).await;
    produce_value(&producer, topic, br#"{"hello":"world"}"#).await;

    // Per-request status queue: 500, 500, then fall through to default 200.
    let state = Arc::new(MockState::default());
    {
        let mut q = state.queue.lock().unwrap();
        q.push_back(StatusCode::INTERNAL_SERVER_ERROR);
        q.push_back(StatusCode::INTERNAL_SERVER_ERROR);
    }
    let addr = spawn_mock(state.clone()).await;

    let s = sub(
        "retry",
        topic,
        &addr,
        Some(b"retry-secret".to_vec()),
        Some(dlq.into()),
        5,
        None,
    );
    let token = CancellationToken::new();
    let dlq_producer = Arc::new(make_producer(&bootstrap).await);
    let handle = tokio::spawn(outbound::run_subscription(
        s,
        bootstrap.clone(),
        "test-out".into(),
        dlq_producer,
        token.clone(),
        None,
        Arc::new(RawCodec),
    ));

    // Wait until the event has been received ≥ 3 times (2 failures + 1 success).
    assert!(
        wait_until(|| received_len(&state) >= 3).await,
        "expected ≥3 attempts (2 failed + 1 ok), got {}",
        received_len(&state)
    );

    // The last attempt got a 200, so the record is committed and NOT dead-lettered.
    let client = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    // real-time wait (not a progress poll): settle-then-assert-absence — a record that eventually delivers must NOT be dead-lettered; the observable (fetch_dlq) is async, so there is no cheap synchronous positive to poll.
    // Give any (incorrect) DLQ produce a brief chance, then assert emptiness.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let dlq_recs = fetch_dlq(&client, dlq).await;
    assert!(
        dlq_recs.is_empty(),
        "a record that eventually delivers must not be dead-lettered, got {dlq_recs:?}"
    );

    token.cancel();
    let _ = handle.await;
    broker.shutdown().await;
}

/// Mock always 500, `max_attempts = 2`: the record is dead-lettered after
/// exhaustion (value + `x-crabka-dlq-source` header), and the loop keeps polling
/// — a SECOND record produced afterwards also reaches the DLQ (no wedge).
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dead_letters_on_exhaustion() {
    let topic = "outbound-dlq";
    let dlq = "outbound-dlq-sink";
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, topic, 1).await;
    create_topic(&bootstrap, dlq, 1).await;

    let producer = make_producer(&bootstrap).await;
    produce_value(&producer, topic, br#"{"first":1}"#).await;

    let state = Arc::new(MockState::default());
    state
        .default
        .store(StatusCode::INTERNAL_SERVER_ERROR.as_u16(), Ordering::SeqCst);
    let addr = spawn_mock(state.clone()).await;

    let s = sub(
        "dlq",
        topic,
        &addr,
        Some(b"dlq-secret".to_vec()),
        Some(dlq.into()),
        2, // max_attempts: 2 tries then DLQ
        None,
    );
    let token = CancellationToken::new();
    let dlq_producer = Arc::new(make_producer(&bootstrap).await);
    let handle = tokio::spawn(outbound::run_subscription(
        s,
        bootstrap.clone(),
        "test-out".into(),
        dlq_producer,
        token.clone(),
        None,
        Arc::new(RawCodec),
    ));

    let client = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();

    // The first record exhausts its 2 attempts and lands in the DLQ.
    let after_first = wait_for_dlq(&client, dlq, 1).await;
    assert!(
        !after_first.is_empty(),
        "first record must be dead-lettered after exhaustion"
    );
    let first = &after_first[0];
    assert_eq!(
        first.value.as_deref(),
        Some(br#"{"first":1}"#.as_ref()),
        "DLQ record value matches the original"
    );
    assert_eq!(
        first.dlq_source.as_deref(),
        Some(format!("{topic}-0-0").as_str()),
        "x-crabka-dlq-source header carries the event id"
    );

    // The loop did NOT wedge: produce a 2nd record (mock still 500) and assert it
    // too reaches the DLQ.
    produce_value(&producer, topic, br#"{"second":2}"#).await;
    let after_second = wait_for_dlq(&client, dlq, 2).await;
    assert!(
        after_second.len() >= 2,
        "second record must also be dead-lettered (loop keeps polling), got {} records",
        after_second.len()
    );
    assert!(
        after_second
            .iter()
            .any(|r| r.value.as_deref() == Some(br#"{"second":2}"#.as_ref())),
        "the second produced record must appear in the DLQ"
    );

    token.cancel();
    let _ = handle.await;
    broker.shutdown().await;
}

/// One partition, values 0..5 produced in order, mock 200: the receiver sees the
/// envelopes in ascending-offset order (offset order == produced order).
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordering_within_partition() {
    let topic = "outbound-order";
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, topic, 1).await;

    let producer = make_producer(&bootstrap).await;
    for i in 0..5u8 {
        produce_value(&producer, topic, format!(r#"{{"i":{i}}}"#).as_bytes()).await;
    }

    let state = Arc::new(MockState::default());
    let addr = spawn_mock(state.clone()).await;
    let s = sub("order", topic, &addr, None, None, 5, None);
    let token = CancellationToken::new();
    let dlq_producer = Arc::new(make_producer(&bootstrap).await);
    let handle = tokio::spawn(outbound::run_subscription(
        s,
        bootstrap.clone(),
        "test-out".into(),
        dlq_producer,
        token.clone(),
        None,
        Arc::new(RawCodec),
    ));

    assert!(
        wait_until(|| received_len(&state) >= 5).await,
        "expected 5 deliveries, got {}",
        received_len(&state)
    );

    // Extract the envelope offset for each delivery in arrival order.
    let recv = state.received.lock().unwrap().clone();
    let offsets: Vec<i64> = recv
        .iter()
        .map(|r| {
            let v: Value = serde_json::from_slice(&r.body).expect("envelope JSON");
            v["offset"].as_i64().expect("offset field")
        })
        .collect();
    assert_eq!(
        offsets,
        vec![0, 1, 2, 3, 4],
        "deliveries must arrive in ascending-offset (== produced) order, got {offsets:?}"
    );

    token.cancel();
    let _ = handle.await;
    broker.shutdown().await;
}

/// `filter = $.deliver`: of two records `{"deliver":true}` and
/// `{"deliver":false}`, only the truthy one is delivered.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filter_skips_nonmatching() {
    let topic = "outbound-filter";
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, topic, 1).await;

    let producer = make_producer(&bootstrap).await;
    produce_value(&producer, topic, br#"{"deliver":true}"#).await;
    produce_value(&producer, topic, br#"{"deliver":false}"#).await;

    let filter = jsonpath_rust::parser::parse_json_path("$.deliver").unwrap();
    let state = Arc::new(MockState::default());
    let addr = spawn_mock(state.clone()).await;
    let s = sub("filter", topic, &addr, None, None, 5, Some(filter));
    let token = CancellationToken::new();
    let dlq_producer = Arc::new(make_producer(&bootstrap).await);
    let handle = tokio::spawn(outbound::run_subscription(
        s,
        bootstrap.clone(),
        "test-out".into(),
        dlq_producer,
        token.clone(),
        None,
        Arc::new(RawCodec),
    ));

    // Exactly one record (the truthy one) is delivered.
    assert!(
        wait_until(|| received_len(&state) >= 1).await,
        "the matching record must be delivered"
    );
    // real-time wait (not a progress poll): settle-then-assert-absence — proving the filtered (false) record never slips through; polling to len==1 could pass before the false record would have arrived.
    // Give the filtered (false) record a chance to (wrongly) slip through.
    tokio::time::sleep(Duration::from_millis(750)).await;
    let recv = state.received.lock().unwrap().clone();
    assert_eq!(
        recv.len(),
        1,
        "only the matching record should be POSTed, got {} deliveries",
        recv.len()
    );
    let v: Value = serde_json::from_slice(&recv[0].body).expect("envelope JSON");
    assert_eq!(
        v["value"]["deliver"],
        Value::Bool(true),
        "the delivered record must be the deliver:true one"
    );

    token.cancel();
    let _ = handle.await;
    broker.shutdown().await;
}

/// Unit-level (no broker): an `OutboundFile` whose `target_url` host is not in
/// `allowed_targets` fails to `compile()` (SSRF guard).
#[test]
fn ssrf_rejected_at_compile() {
    let toml = r#"
[[allowed_targets]]
scheme = "http"
host   = "trusted.internal"

[[subscriptions]]
name          = "evil"
source_topics = ["t"]
target_url    = "http://attacker.example.com/exfil"
"#;
    let file: OutboundFile = toml::from_str(toml).expect("parse TOML");
    let err = file
        .compile()
        .expect_err("target outside allow-list must fail to compile");
    assert!(
        err.contains("SSRF guard"),
        "compile error must cite the SSRF guard, got: {err}"
    );
}

#[test]
fn cloudevents_static_header_override_rejected_at_compile() {
    let toml = r#"
[[allowed_targets]]
scheme = "http"
host   = "trusted.internal"

[[subscriptions]]
name          = "ce"
source_topics = ["t"]
target_url    = "http://trusted.internal/hook"
content_mode  = "cloud_events_binary"

[subscriptions.headers]
"content-type" = "application/json"
"#;
    let file: OutboundFile = toml::from_str(toml).expect("parse TOML");
    let err = file
        .compile()
        .expect_err("CloudEvents modes reserve content-type overrides");

    assert!(
        err.contains("reserves static header"),
        "compile error must cite the reserved header, got: {err}"
    );
}
