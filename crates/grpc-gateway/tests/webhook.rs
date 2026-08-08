//! Integration tests for the webhook inbound routes.
//!
//! Covers:
//!  - `POST /v1/webhooks/{name}`: HMAC verification, `JSONPath` idempotency,
//!    header idempotency, body-size guard, unknown endpoint 404.
//!  - `POST /v1/produce/{topic}`: plain produce + `Idempotency-Key` dedup.
//!
//! Dedup tests spin up a real single-owner store and wait for `has_warmed_once`
//! before the first produce — exactly the pattern in `tests/integration_dedup.rs`.

use std::{
    collections::{BTreeMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use bytes::Bytes;
use crabka_authz::{AuthorizationRequest, AuthorizationResult, SimpleAclAuthorizer};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_grpc_gateway::{
    authz::GatewayAuthz,
    codec::RawCodec,
    config::GatewayConfig,
    dedup::{DedupEngine, store::DedupStore, topic::ensure_dedup_topic},
    produce::ProduceCore,
    state::AppState,
    webhook::webhook_router,
    webhook_config::WebhooksFile,
};
use crabka_metadata::{AclOperation, ResourceType};
use crabka_security::{AuthMethod, Principal};
use crabka_units::prelude::*;
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
        ensure_dedup_topic(
            bootstrap,
            DEDUP_TOPIC,
            N,
            hours(1),
            &crabka_grpc_gateway::dedup::topic::InternalTopicPolicy {
                replication_factor: 1,
                ..Default::default()
            },
            None,
        )
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
                None,
            ));
        }

        let engine = Arc::new(DedupEngine::new(
            bootstrap,
            client_prefix,
            &format!("crabka-wh-dedup-{client_prefix}"),
            DEDUP_TOPIC.into(),
            N,
            store.clone(),
            None,
        ));

        let core = ProduceCore::new(
            bootstrap,
            &format!("{client_prefix}-prod"),
            Arc::new(RawCodec),
            None,
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
            None,
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
            dedup_window: hours(1),
            dedup_ownership_group: OWNERS_GROUP.into(),
            dedup_txn_id_prefix: format!("crabka-wh-dedup-{client_prefix}"),
            advertised_addr: "127.0.0.1:0".into(),
            membership_topic: "__crabka_wh_membership".into(),
            tls: None,
            broker_security: None,
            authz: None,
            webhooks,
            outbound: Vec::new(),
            schema_registry_url: None,
            runtime: crabka_grpc_gateway::config::GatewayRuntimeConfig::default(),
        }),
        authz: Arc::new(GatewayAuthz::new(Arc::new(
            crabka_authz::AllowAllAuthorizer,
        ))),
        codec: Arc::new(RawCodec),
        queue: Arc::default(),
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
        let batch = consumer.poll(crabka_units::millis(500)).await.unwrap();
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
            crabka_units::secs(10),
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

    let status = resp.status();
    let wr = parse_response(resp).await;
    assert2::assert!(status == StatusCode::OK);
    assert2::assert!(!wr.deduplicated);
    assert2::assert!(wr.offset >= 0);

    // Verify the record landed in the topic.
    assert2::assert!(count_topic(&bootstrap, topic, "vhp-verify").await == 1);

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
            crabka_units::secs(10),
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

    assert2::assert!(resp.status() == StatusCode::UNAUTHORIZED);

    // Nothing should have been produced.
    assert2::assert!(count_topic(&bootstrap, topic, "ihr-verify").await == 0);

    token.cancel();
    broker.shutdown().await;
}

/// `JSONPath` idempotency source: two POSTs with the same `$.id` value (provider
/// redelivery) dedup — the second returns `deduplicated=true` with the same
/// offset, and exactly one record lands in the topic.
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
            crabka_units::secs(10),
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
    let first_status = first_resp.status();
    let first = parse_response(first_resp).await;

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
    let second_status = second_resp.status();
    let second = parse_response(second_resp).await;
    assert2::assert!(first_status == StatusCode::OK);
    assert2::assert!(!first.deduplicated);
    assert2::assert!(second_status == StatusCode::OK);
    assert2::assert!(second.deduplicated);
    assert2::assert!(first.offset == first.offset);
    assert2::assert!(second.offset == first.offset);

    // Exactly one record in the topic (EOS guarantee).
    assert2::assert!(count_topic(&bootstrap, topic, "jpd-verify").await == 1);

    token.cancel();
    broker.shutdown().await;
}

/// Header idempotency source (`header:X-Delivery`): two POSTs with the same
/// header value dedup on the second request.
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
            crabka_units::secs(10),
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
    let first_status = first_resp.status();
    let first = parse_response(first_resp).await;

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
    let second_status = second_resp.status();
    let second = parse_response(second_resp).await;
    assert2::assert!(first_status == StatusCode::OK);
    assert2::assert!(!first.deduplicated);
    assert2::assert!(second_status == StatusCode::OK);
    assert2::assert!(second.deduplicated);
    assert2::assert!(first.offset == first.offset);
    assert2::assert!(second.offset == first.offset);

    token.cancel();
    broker.shutdown().await;
}

/// `POST /v1/produce/{topic}`: plain produce returns 200; a repeat with the
/// same `Idempotency-Key` header is deduplicated.
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
            crabka_units::secs(10),
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
    let status = resp.status();
    let first = parse_response(resp).await;
    assert2::assert!(status == StatusCode::OK);
    assert2::assert!(!first.deduplicated);

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
    let first_status = resp1.status();
    let keyed_first = parse_response(resp1).await;

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
    let second_status = resp2.status();
    let keyed_second = parse_response(resp2).await;
    assert2::assert!(first_status == StatusCode::OK);
    assert2::assert!(!keyed_first.deduplicated);
    assert2::assert!(second_status == StatusCode::OK);
    assert2::assert!(keyed_second.deduplicated);
    assert2::assert!(keyed_first.offset == keyed_first.offset);
    assert2::assert!(keyed_second.offset == keyed_first.offset);

    token.cancel();
    broker.shutdown().await;
}

/// Body larger than `max_body` → 413 Payload Too Large.
#[tokio::test]
async fn body_too_large_413() {
    let (broker, bootstrap, _dir) = boot().await;

    let toml = r#"
[[endpoints]]
name = "tiny"
target_topic = "irrelevant"
max_body = "16B"
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

    assert2::assert!(resp.status() == StatusCode::PAYLOAD_TOO_LARGE);

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

    assert2::assert!(resp.status() == StatusCode::NOT_FOUND);

    token.cancel();
    broker.shutdown().await;
}

// ---------------------------------------------------------------------------
// Authz tests (SimpleAclAuthorizer)
// ---------------------------------------------------------------------------

/// Helper: build an `AppState` with a `SimpleAclAuthorizer` (empty super-users)
/// backed by the provided `GatewayAuthz`. No dedup wiring.
async fn webhook_state_with_authz(
    bootstrap: &str,
    client_prefix: &str,
    webhooks_toml: &str,
    authz: Arc<GatewayAuthz>,
) -> Arc<AppState> {
    let webhooks = toml::from_str::<WebhooksFile>(webhooks_toml)
        .expect("parse toml")
        .compile()
        .expect("compile webhooks");

    let produce = ProduceCore::new(
        bootstrap,
        &format!("{client_prefix}-prod"),
        Arc::new(RawCodec),
        None,
    )
    .await
    .unwrap();

    Arc::new(AppState {
        produce: Arc::new(produce),
        config: Arc::new(GatewayConfig {
            bootstrap: bootstrap.to_string(),
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            client_id: client_prefix.into(),
            dedup_topic: DEDUP_TOPIC.into(),
            dedup_partitions: N,
            dedup_window: hours(1),
            dedup_ownership_group: OWNERS_GROUP.into(),
            dedup_txn_id_prefix: format!("crabka-wh-dedup-{client_prefix}"),
            advertised_addr: "127.0.0.1:0".into(),
            membership_topic: "__crabka_wh_membership".into(),
            tls: None,
            broker_security: None,
            authz: None,
            webhooks,
            outbound: Vec::new(),
            schema_registry_url: None,
            runtime: crabka_grpc_gateway::config::GatewayRuntimeConfig::default(),
        }),
        authz,
        codec: Arc::new(RawCodec),
        queue: Arc::default(),
    })
}

/// Helper: poll the ACL cache until the authorization result for the given
/// probe matches `expect`. Times out after 20 s (80 × 250 ms).
async fn wait_authz(
    authz: &Arc<GatewayAuthz>,
    probe_principal: &Principal,
    rt: ResourceType,
    name: &str,
    op: AclOperation,
    expect: AuthorizationResult,
) {
    let host: SocketAddr = "0.0.0.0:0".parse().unwrap();
    for _ in 0..80 {
        let cache = authz.cache();
        let req = AuthorizationRequest {
            principal: probe_principal,
            host: &host,
            resource_type: rt,
            resource_name: name,
            operation: op,
        };
        if authz.authorizer().authorize(&**cache, &req) == expect {
            return;
        }
        drop(cache);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("ACL cache never converged to the expected decision for {name}");
}

/// Build an `AclEntry` granting `User:{user}` Allow `op` on `Topic:{topic}`.
fn topic_acl(
    user: &str,
    op: crabka_client_admin::AclOperation,
    topic: &str,
) -> crabka_client_admin::AclEntry {
    crabka_client_admin::AclEntry {
        resource_type: crabka_client_admin::ResourceType::Topic,
        resource_name: topic.to_string(),
        pattern_type: crabka_client_admin::PatternType::Literal,
        principal: format!("User:{user}"),
        host: "*".to_string(),
        operation: op,
        permission_type: crabka_client_admin::PermissionType::Allow,
    }
}

/// `SimpleAclAuthorizer` with an EMPTY cache (default-deny) rejects a webhook
/// produce even when the HMAC is valid, because the endpoint's principal
/// (`webhook:acl-denied`) has no Write ACL on the target topic.
///
/// After granting the ACL and refreshing the cache the same request succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn simpleacl_denies_webhook_principal() {
    let (broker, bootstrap, _dir) = boot().await;

    let topic = "wh-acl-topic";
    let endpoint_name = "acl-denied";
    let principal_name = format!("webhook:{endpoint_name}");

    // Create the target topic.
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
            crabka_units::secs(10),
        )
        .await
        .unwrap();

    let toml = format!(
        r#"
[[endpoints]]
name = "{endpoint_name}"
target_topic = "{topic}"
secret = "deny-secret"
signature_header = "X-Sig"
signature_encoding = "hex"
"#
    );

    // SimpleAcl with EMPTY cache — default-deny for everyone.
    let authz = Arc::new(GatewayAuthz::new(Arc::new(SimpleAclAuthorizer::new(
        HashSet::new(),
    ))));
    let state = webhook_state_with_authz(&bootstrap, "acld", &toml, authz.clone()).await;
    let app = webhook_router(state);

    let body = b"some webhook payload";
    let sig = hmac_hex(b"deny-secret", body);

    // Valid HMAC but principal has no ACL → 403 FORBIDDEN, nothing produced.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/webhooks/{endpoint_name}"))
                .header("X-Sig", &sig)
                .body(Body::from(Bytes::from_static(body)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert2::assert!(resp.status() == StatusCode::FORBIDDEN);
    assert2::assert!(count_topic(&bootstrap, topic, "acld-deny-verify").await == 0);

    // Now grant `User:webhook:acl-denied` Allow Write Topic:wh-acl-topic and
    // start the ACL refresh loop. Once the cache converges, the same request
    // must succeed.
    let outcomes = admin
        .create_acls(&[topic_acl(
            &principal_name,
            crabka_client_admin::AclOperation::Write,
            topic,
        )])
        .await
        .unwrap();
    assert2::assert!(outcomes.iter().all(|o| o.error.is_none()));

    let shutdown = CancellationToken::new();
    {
        let authz = authz.clone();
        let bootstrap = bootstrap.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(authz.run_acl_refresh(bootstrap, millis(200), shutdown, None));
    }

    // Build a Principal matching what the authorizer sees for this endpoint.
    let probe = Principal {
        name: principal_name.clone(),
        auth_method: AuthMethod::MTls,
        groups: vec![],
    };
    wait_authz(
        &authz,
        &probe,
        ResourceType::Topic,
        topic,
        AclOperation::Write,
        AuthorizationResult::Allow,
    )
    .await;

    // Need a fresh state sharing the now-populated authz.
    let toml2 = format!(
        r#"
[[endpoints]]
name = "{endpoint_name}"
target_topic = "{topic}"
secret = "deny-secret"
signature_header = "X-Sig"
signature_encoding = "hex"
"#
    );
    let state2 = webhook_state_with_authz(&bootstrap, "acla", &toml2, authz.clone()).await;
    let app2 = webhook_router(state2);

    let resp2 = app2
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/webhooks/{endpoint_name}"))
                .header("X-Sig", &sig)
                .body(Body::from(Bytes::from_static(body)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert2::assert!(resp2.status() == StatusCode::OK);
    assert2::assert!(count_topic(&bootstrap, topic, "acla-allow-verify").await == 1);

    shutdown.cancel();
    broker.shutdown().await;
}

/// Sending `X-Timestamp: i64::MIN` (the most negative timestamp) must return
/// 401 (stale) and NOT panic — this exercises the i128 overflow fix. Under the
/// test profile's overflow-checks the old i64 subtraction would panic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_timestamp_overflow_rejected() {
    let (broker, bootstrap, _dir) = boot().await;

    // Endpoint with a timestamp header and a realistic tolerance.
    let toml = r#"
[[endpoints]]
name = "ts-overflow"
target_topic = "irrelevant"
secret = "secret"
signature_header = "X-Sig"
signature_encoding = "hex"
timestamp_header = "X-Timestamp"
timestamp_tolerance = "300s"
"#;

    let (state, token, _store) = webhook_state(&bootstrap, "tsof", toml, false).await;
    let app = webhook_router(state);

    let body = b"payload";
    let sig = hmac_hex(b"secret", body);
    // i64::MIN is maximally stale and triggers the overflow in the old code.
    let extreme_ts = i64::MIN.to_string();

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/webhooks/ts-overflow")
                .header("X-Sig", sig)
                .header("X-Timestamp", extreme_ts)
                .body(Body::from(Bytes::from_static(body)))
                .unwrap(),
        )
        .await
        .unwrap(); // must complete, not panic

    // Stale timestamp → 401.
    assert2::assert!(resp.status() == StatusCode::UNAUTHORIZED);

    token.cancel();
    broker.shutdown().await;
}
