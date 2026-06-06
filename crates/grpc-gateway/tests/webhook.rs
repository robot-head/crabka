//! Integration tests for the webhook inbound routes.
//!
//! Covers:
//!  - `POST /v1/webhooks/{name}`: HMAC verification, `JSONPath` idempotency,
//!    header idempotency, body-size guard, unknown endpoint 404.
//!  - `POST /v1/produce/{topic}`: plain produce + `Idempotency-Key` dedup.
//!
//! Dedup tests spin up a real single-owner store and wait for `has_warmed_once`
//! before the first produce — exactly the pattern in `tests/integration_dedup.rs`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_grpc_gateway::authz::GatewayAuthz;
use crabka_grpc_gateway::codec::RawCodec;
use crabka_grpc_gateway::config::GatewayConfig;
use crabka_grpc_gateway::dedup::DedupEngine;
use crabka_grpc_gateway::dedup::store::DedupStore;
use crabka_grpc_gateway::dedup::topic::ensure_dedup_topic;
use crabka_grpc_gateway::produce::ProduceCore;
use crabka_grpc_gateway::state::AppState;
use crabka_grpc_gateway::webhook::webhook_router;
use crabka_grpc_gateway::webhook_config::WebhooksFile;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const N: u32 = 4;
const DEDUP_TOPIC: &str = "__crabka_wh_dedup";
const OWNERS_GROUP: &str = "__crabka_wh_dedup_owners";

// ---------------------------------------------------------------------------
// Boot helper
// ---------------------------------------------------------------------------

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

// ---------------------------------------------------------------------------
// HMAC helper
// ---------------------------------------------------------------------------

fn hmac_hex(secret: &[u8], body: &[u8]) -> String {
    let mut mac = <Hmac<Sha256>>::new_from_slice(secret).unwrap();
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

// ---------------------------------------------------------------------------
// State / harness builders
// ---------------------------------------------------------------------------

/// Build an `AppState` (and optionally a `DedupStore`) for webhook tests.
///
/// * `webhooks_toml` is parsed + compiled via `WebhooksFile::compile()` and
///   stored in `GatewayConfig.webhooks`.
/// * When `with_dedup = true` the function creates the dedup topic, builds a
///   `DedupStore`, spawns `run_ownership`, wraps the produce core with the
///   `DedupEngine`, and returns the store so the caller can wait for
///   `has_warmed_once()`. When `false` no dedup wiring is done.
///
/// Returns `(Arc<AppState>, token, Option<Arc<DedupStore>>)`.
async fn webhook_state(
    bootstrap: &str,
    client_prefix: &str,
    webhooks_toml: &str,
    with_dedup: bool,
) -> (Arc<AppState>, CancellationToken, Option<Arc<DedupStore>>) {
    let webhooks = toml::from_str::<WebhooksFile>(webhooks_toml)
        .expect("parse toml")
        .compile()
        .expect("compile webhooks");

    let token = CancellationToken::new();

    let (produce, store) = if with_dedup {
        ensure_dedup_topic(bootstrap, DEDUP_TOPIC, N, 3_600_000, 1)
            .await
            .unwrap();

        let store = Arc::new(DedupStore::new(N));
        {
            let store = store.clone();
            let bootstrap = bootstrap.to_string();
            let token = token.clone();
            let client_id = format!("{client_prefix}-owner");
            tokio::spawn(store.run_ownership(
                bootstrap,
                client_id,
                DEDUP_TOPIC.into(),
                OWNERS_GROUP.into(),
                token,
            ));
        }

        let engine = Arc::new(DedupEngine::new(
            bootstrap,
            client_prefix,
            &format!("crabka-wh-dedup-{client_prefix}"),
            DEDUP_TOPIC.into(),
            N,
            store.clone(),
        ));

        let core = ProduceCore::new(
            bootstrap,
            &format!("{client_prefix}-prod"),
            Arc::new(RawCodec),
        )
        .await
        .unwrap()
        .with_dedup(engine);

        (core, Some(store))
    } else {
        let core = ProduceCore::new(
            bootstrap,
            &format!("{client_prefix}-prod"),
            Arc::new(RawCodec),
        )
        .await
        .unwrap();
        (core, None)
    };

    let state = Arc::new(AppState {
        produce: Arc::new(produce),
        config: Arc::new(GatewayConfig {
            bootstrap: bootstrap.to_string(),
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            client_id: client_prefix.into(),
            dedup_topic: DEDUP_TOPIC.into(),
            dedup_partitions: N,
            dedup_window_ms: 3_600_000,
            dedup_txn_id_prefix: format!("crabka-wh-dedup-{client_prefix}"),
            advertised_addr: "127.0.0.1:0".into(),
            membership_topic: "__crabka_wh_membership".into(),
            tls: None,
            authz: None,
            webhooks,
        }),
        authz: Arc::new(GatewayAuthz::new(Arc::new(
            crabka_authz::AllowAllAuthorizer,
        ))),
    });

    (state, token, store)
}

/// Poll `has_warmed_once` up to 80 × 250 ms (20 s).
async fn wait_warm(store: &Arc<DedupStore>) {
    for _ in 0..80 {
        if store.has_warmed_once() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("DedupStore never warmed");
}

/// Consume all committed records from `topic` and count them (`ReadCommitted`,
/// `AutoOffsetReset::Earliest`, up to 10 poll rounds × 500 ms).
async fn count_topic(bootstrap: &str, topic: &str, group: &str) -> usize {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.to_string())
        .client_id("wh-verify")
        .group_id(group.to_string())
        .subscribe(vec![topic.to_string()])
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    let mut n = 0;
    for _ in 0..10 {
        let batch = consumer.poll(Duration::from_millis(500)).await.unwrap();
        n += batch.len();
    }
    let _ = consumer.close().await;
    n
}

/// A trimmed view of the `WebhookResponse` JSON returned by webhook routes.
#[derive(Debug)]
struct WR {
    #[allow(dead_code)]
    partition: i32,
    offset: i64,
    deduplicated: bool,
}

/// Parse the response body as a `WR` (from `{"partition":…,"offset":…,"deduplicated":…}`).
async fn parse_response(resp: axum::response::Response) -> WR {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).expect("WebhookResponse JSON");
    WR {
        partition: i32::try_from(v["partition"].as_i64().unwrap()).unwrap_or(0),
        offset: v["offset"].as_i64().unwrap(),
        deduplicated: v["deduplicated"].as_bool().unwrap(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A valid HMAC produces into the target topic and returns 200.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_hmac_produces() {
    let (broker, bootstrap, _dir) = boot().await;

    let topic = "wh-orders";
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: topic.into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();

    let toml = format!(
        r#"
[[endpoints]]
name = "orders"
target_topic = "{topic}"
secret = "wh-secret"
signature_header = "X-Sig"
signature_encoding = "hex"
"#
    );

    let (state, token, _store) = webhook_state(&bootstrap, "vhp", &toml, false).await;
    let app = webhook_router(state);

    let body = b"{\"event\":\"order.created\",\"id\":\"o-1\"}";
    let sig = hmac_hex(b"wh-secret", body);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/webhooks/orders")
                .header("X-Sig", sig)
                .body(Body::from(Bytes::from_static(body)))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let wr = parse_response(resp).await;
    assert!(!wr.deduplicated);
    assert!(wr.offset >= 0);

    // Verify the record landed in the topic.
    assert_eq!(count_topic(&bootstrap, topic, "vhp-verify").await, 1);

    token.cancel();
    broker.shutdown().await;
}

/// Wrong HMAC signature → 401; nothing produced.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_hmac_rejected() {
    let (broker, bootstrap, _dir) = boot().await;

    let topic = "wh-bad-sig";
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: topic.into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();

    let toml = format!(
        r#"
[[endpoints]]
name = "badsig"
target_topic = "{topic}"
secret = "wh-secret"
signature_header = "X-Sig"
signature_encoding = "hex"
"#
    );

    let (state, token, _store) = webhook_state(&bootstrap, "ihr", &toml, false).await;
    let app = webhook_router(state);

    let body = b"some body";
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/webhooks/badsig")
                .header(
                    "X-Sig",
                    "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
                )
                .body(Body::from(Bytes::from_static(body)))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Nothing should have been produced.
    assert_eq!(count_topic(&bootstrap, topic, "ihr-verify").await, 0);

    token.cancel();
    broker.shutdown().await;
}

/// `JSONPath` idempotency source: two POSTs with the same `$.id` value (provider
/// redelivery) dedup — the second returns `deduplicated=true` with the same
/// offset, and exactly one record lands in the topic.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jsonpath_idempotency_redelivery_dedups() {
    let (broker, bootstrap, _dir) = boot().await;

    let topic = "wh-jp-dedup";
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: topic.into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();

    let toml = format!(
        r#"
[[endpoints]]
name = "jpdedup"
target_topic = "{topic}"
secret = "jp-secret"
signature_header = "X-Sig"
signature_encoding = "hex"
idempotency_source = "json:$.id"
"#
    );

    let (state, token, store) = webhook_state(&bootstrap, "jpd", &toml, true).await;
    let store = store.expect("dedup store must be present");

    // Wait for the single-owner to warm up.
    wait_warm(&store).await;

    let app = webhook_router(state);

    let body = b"{\"id\":\"evt-1\",\"type\":\"payment.succeeded\"}";
    let sig = hmac_hex(b"jp-secret", body);

    // First delivery → produced, not deduplicated.
    let first_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/webhooks/jpdedup")
                .header("X-Sig", &sig)
                .header("content-type", "application/json")
                .body(Body::from(Bytes::from_static(body)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first_resp.status(), StatusCode::OK);
    let first = parse_response(first_resp).await;
    assert!(
        !first.deduplicated,
        "first delivery must not be deduplicated"
    );

    // Provider redelivery (same id) → deduplicated, same offset.
    let second_resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/webhooks/jpdedup")
                .header("X-Sig", &sig)
                .header("content-type", "application/json")
                .body(Body::from(Bytes::from_static(body)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second_resp.status(), StatusCode::OK);
    let second = parse_response(second_resp).await;
    assert!(second.deduplicated, "redelivery must be deduplicated");
    assert_eq!(
        first.offset, second.offset,
        "deduplicated response must return original offset"
    );

    // Exactly one record in the topic (EOS guarantee).
    assert_eq!(
        count_topic(&bootstrap, topic, "jpd-verify").await,
        1,
        "exactly one record must be in the topic after dedup"
    );

    token.cancel();
    broker.shutdown().await;
}

/// Header idempotency source (`header:X-Delivery`): two POSTs with the same
/// header value dedup on the second request.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn header_idempotency_works() {
    let (broker, bootstrap, _dir) = boot().await;

    let topic = "wh-hdr-dedup";
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: topic.into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();

    let toml = format!(
        r#"
[[endpoints]]
name = "hdrdedup"
target_topic = "{topic}"
secret = "hdr-secret"
signature_header = "X-Sig"
signature_encoding = "hex"
idempotency_source = "header:X-Delivery"
"#
    );

    let (state, token, store) = webhook_state(&bootstrap, "hdr", &toml, true).await;
    let store = store.expect("dedup store must be present");

    wait_warm(&store).await;

    let app = webhook_router(state);

    let body = b"some event payload";
    let sig = hmac_hex(b"hdr-secret", body);
    let delivery_id = "del-abc-123";

    // First POST.
    let first_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/webhooks/hdrdedup")
                .header("X-Sig", &sig)
                .header("X-Delivery", delivery_id)
                .body(Body::from(Bytes::from_static(body)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first_resp.status(), StatusCode::OK);
    let first = parse_response(first_resp).await;
    assert!(!first.deduplicated);

    // Second POST with same delivery id → dedup.
    let second_resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/webhooks/hdrdedup")
                .header("X-Sig", &sig)
                .header("X-Delivery", delivery_id)
                .body(Body::from(Bytes::from_static(body)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(second_resp.status(), StatusCode::OK);
    let second = parse_response(second_resp).await;
    assert!(
        second.deduplicated,
        "second with same X-Delivery must dedup"
    );
    assert_eq!(first.offset, second.offset);

    token.cancel();
    broker.shutdown().await;
}

/// `POST /v1/produce/{topic}`: plain produce returns 200; a repeat with the
/// same `Idempotency-Key` header is deduplicated.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generic_produce_route() {
    let (broker, bootstrap, _dir) = boot().await;

    let topic = "wh-generic";
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: topic.into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();

    let (state, token, store) = webhook_state(&bootstrap, "gpr", "", true).await;
    let store = store.expect("dedup store must be present");

    wait_warm(&store).await;

    let app = webhook_router(state);

    let body = b"hello generic";

    // Plain produce (no idempotency key) → 200.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/produce/{topic}"))
                .body(Body::from(Bytes::from_static(body)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let first = parse_response(resp).await;
    assert!(!first.deduplicated);

    // Same idempotency key twice → second is deduplicated.
    let idem_key = "gpr-key-1";

    let resp1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/produce/{topic}"))
                .header("Idempotency-Key", idem_key)
                .body(Body::from(Bytes::from_static(body)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    let keyed_first = parse_response(resp1).await;
    assert!(!keyed_first.deduplicated);

    let resp2 = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/produce/{topic}"))
                .header("Idempotency-Key", idem_key)
                .body(Body::from(Bytes::from_static(body)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    let keyed_second = parse_response(resp2).await;
    assert!(
        keyed_second.deduplicated,
        "second with same Idempotency-Key must dedup"
    );
    assert_eq!(keyed_first.offset, keyed_second.offset);

    token.cancel();
    broker.shutdown().await;
}

/// Body larger than `max_body_bytes` → 413 Payload Too Large.
#[tokio::test]
async fn body_too_large_413() {
    let (broker, bootstrap, _dir) = boot().await;

    let toml = r#"
[[endpoints]]
name = "tiny"
target_topic = "irrelevant"
max_body_bytes = 16
"#;

    let (state, token, _store) = webhook_state(&bootstrap, "btl", toml, false).await;
    let app = webhook_router(state);

    // 17 bytes > limit of 16.
    let body = vec![b'x'; 17];
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/webhooks/tiny")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);

    token.cancel();
    broker.shutdown().await;
}

/// Unknown webhook name → 404.
#[tokio::test]
async fn unknown_endpoint_404() {
    let (broker, bootstrap, _dir) = boot().await;

    let (state, token, _store) = webhook_state(&bootstrap, "u404", "", false).await;
    let app = webhook_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/webhooks/nope")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    token.cancel();
    broker.shutdown().await;
}
