//! Coverage for the Forwarder's response/error mapping and the internal
//! forward endpoint's error arm — without the full multi-replica path.

use std::{sync::Arc, time::Duration};

use axum::{
    Json, Router,
    body::Body,
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig};
use crabka_grpc_gateway::{
    codec::RawCodec,
    config::{ClientAuthMode, GatewayConfig, TlsSettings},
    dedup::{DedupEngine, store::DedupStore},
    error::GatewayError,
    forward::{ForwardError, ForwardRecord, ForwardResult, Forwarder, forward_router},
    ids::{Offset, PartitionIndex},
    produce::ProduceCore,
    state::AppState,
    types::GatewayRecord,
};
use tempfile::TempDir;
use tower::ServiceExt;

fn rec(topic: &str) -> GatewayRecord {
    GatewayRecord {
        topic: topic.into(),
        key: None,
        value: Bytes::from_static(b"v"),
        body_structured: None,
        headers: vec![],
        partition: None,
        timestamp_ms: None,
        idempotency_key: Some("k".into()),
    }
}

/// The relayed caller for forward tests. With the mock owner / `AllowAll` the
/// value is immaterial to the response — it only satisfies `forward`'s signature.
fn anon() -> crabka_security::Principal {
    crabka_security::Principal {
        name: "ANONYMOUS".into(),
        auth_method: crabka_security::AuthMethod::Anonymous,
        groups: vec![],
    }
}

// Mock owner endpoint: the response is chosen by the forwarded record's `topic`.
async fn mock_forward(Json(req): Json<ForwardRecord>) -> Response {
    match req.topic.as_str() {
        "ok" => Json(ForwardResult {
            partition: PartitionIndex(7),
            offset: Offset(11),
            deduplicated: true,
            error: None,
        })
        .into_response(),
        "retriable" => Json(ForwardResult {
            partition: PartitionIndex(-1),
            offset: Offset(-1),
            deduplicated: false,
            error: Some(ForwardError {
                message: "warming".into(),
                retriable: true,
            }),
        })
        .into_response(),
        "fatal" => Json(ForwardResult {
            partition: PartitionIndex(-1),
            offset: Offset(-1),
            deduplicated: false,
            error: Some(ForwardError {
                message: "boom".into(),
                retriable: false,
            }),
        })
        .into_response(),
        "http500" => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        // "badjson" and everything else: 200 OK but non-JSON body
        _ => (StatusCode::OK, "not json").into_response(),
    }
}

async fn spawn_mock() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let app = Router::new().route("/internal/v1/forward", post(mock_forward));
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    // real-time wait (not a progress poll): readiness settle for the spawned mock forward endpoint, not in-process gateway state.
    // small readiness pause so the first request doesn't race serve startup
    tokio::time::sleep(Duration::from_millis(150)).await;
    addr
}

#[tokio::test]
async fn forward_transport_error_is_unavailable() {
    // Nothing listening on :1 => connection refused => Unavailable.
    let err = Forwarder::new()
        .forward("127.0.0.1:1", &rec("ok"), &anon())
        .await
        .unwrap_err();
    assert!(matches!(err, GatewayError::Unavailable));
}

#[tokio::test]
async fn forward_maps_owner_responses() {
    let addr = spawn_mock().await;
    let fwd = Forwarder::new();

    // Happy path: error: None => Ok with the forwarded outcome.
    let ok = fwd.forward(&addr, &rec("ok"), &anon()).await.unwrap();
    assert_eq!(
        (ok.partition, ok.offset, ok.deduplicated),
        (PartitionIndex(7), Offset(11), true)
    );

    for (name, key, expected) in [
        ("retriable_owner_error", "retriable", "unavailable"),
        ("fatal_owner_error", "fatal", "forward_boom"),
        ("http_500", "http500", "unavailable"),
        ("malformed_success_body", "badjson", "forward"),
    ] {
        let error = fwd.forward(&addr, &rec(key), &anon()).await.unwrap_err();
        let actual = match error {
            GatewayError::Unavailable => "unavailable",
            GatewayError::Forward(message) if message == "boom" => "forward_boom",
            GatewayError::Forward(_) => "forward",
            other => panic!("unexpected error for {name}: {other:?}"),
        };
        assert_eq!(actual, expected, "case {name}");
    }
}

#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forward_handler_error_arm_returns_retriable() {
    // Consts before any statements (clippy::items_after_statements).
    const N: u32 = 4;
    const DEDUP: &str = "__crabka_grpc_dedup_fh";

    // Boot a real broker so ProduceCore::new can connect, even though
    // produce_local will short-circuit before any data is written (the
    // DedupStore owns nothing => dedup_produce returns Unavailable).
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let store = Arc::new(DedupStore::new(N));
    let engine = Arc::new(DedupEngine::new(
        &bootstrap,
        "fh",
        "fh-dedup",
        DEDUP.into(),
        N,
        store,
        None,
    ));
    let produce = ProduceCore::new(&bootstrap, "fh", Arc::new(RawCodec), None)
        .await
        .unwrap()
        .with_dedup(engine);

    let state = Arc::new(AppState {
        produce: Arc::new(produce),
        config: Arc::new(GatewayConfig {
            bootstrap: bootstrap.clone(),
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            client_id: "fh".into(),
            dedup_topic: DEDUP.into(),
            dedup_partitions: N,
            dedup_window_ms: 3_600_000,
            dedup_txn_id_prefix: "fh-dedup".into(),
            advertised_addr: "127.0.0.1:0".into(),
            membership_topic: "__crabka_grpc_gateway_membership_fh".into(),
            tls: None,
            broker_security: None,
            authz: None,
            webhooks: std::collections::HashMap::new(),
            outbound: Vec::new(),
            schema_registry_url: None,
        }),
        authz: Arc::new(crabka_grpc_gateway::authz::GatewayAuthz::new(Arc::new(
            crabka_authz::AllowAllAuthorizer,
        ))),
        codec: Arc::new(RawCodec),
    });

    // Drive the REAL forward_router in-process via tower::ServiceExt::oneshot —
    // no listen socket needed, so no race between bind and serve.
    let app = forward_router(state);

    let fr = ForwardRecord {
        topic: "t".into(),
        key: None,
        value: vec![1],
        headers: vec![],
        partition: None,
        timestamp_ms: None,
        idempotency_key: Some("k".into()),
        principal: None,
    };

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/v1/forward")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&fr).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let result: ForwardResult = serde_json::from_slice(&bytes).unwrap();

    // produce_local => dedup_produce => DedupStore owns nothing => Unavailable
    // => forward_handler wraps it with retriable: true.
    assert_eq!(
        (
            status,
            result.partition,
            result.offset,
            result.deduplicated,
            result.error.map(|error| error.retriable),
        ),
        (
            StatusCode::OK,
            PartitionIndex(-1),
            Offset(-1),
            false,
            Some(true)
        ),
        "complete forward error result"
    );

    broker.shutdown().await;
}

/// The `/internal/v1/forward` gate: when `config.tls` is `Some` and NO
/// principal extension is present (anonymous caller), the handler returns
/// `403 FORBIDDEN` with `retriable: false` — before any broker interaction.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forward_handler_rejects_anonymous_when_tls_enabled() {
    // Consts before any statements (clippy::items_after_statements).
    const N: u32 = 4;
    const DEDUP: &str = "__crabka_grpc_dedup_fh_tls";

    // Boot a real broker so ProduceCore::new can connect (gate fires before
    // any broker round-trip, but ProduceCore::new requires the connection).
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let store = Arc::new(DedupStore::new(N));
    let engine = Arc::new(DedupEngine::new(
        &bootstrap,
        "fh-tls",
        "fh-tls-dedup",
        DEDUP.into(),
        N,
        store,
        None,
    ));
    let produce = ProduceCore::new(&bootstrap, "fh-tls", Arc::new(RawCodec), None)
        .await
        .unwrap()
        .with_dedup(engine);

    // Dummy TlsSettings — the cert files don't exist, but the gate fires
    // before any crypto work, so the paths are never read.
    let tls = Some(TlsSettings {
        cert_chain_path: "/nonexistent/cert.pem".into(),
        private_key_path: "/nonexistent/key.pem".into(),
        trust_roots_path: None,
        client_ca_path: None,
        client_auth: ClientAuthMode::Disabled,
        reload_interval_secs: 30,
    });

    let state = Arc::new(AppState {
        produce: Arc::new(produce),
        config: Arc::new(GatewayConfig {
            bootstrap: bootstrap.clone(),
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            client_id: "fh-tls".into(),
            dedup_topic: DEDUP.into(),
            dedup_partitions: N,
            dedup_window_ms: 3_600_000,
            dedup_txn_id_prefix: "fh-tls-dedup".into(),
            advertised_addr: "127.0.0.1:0".into(),
            membership_topic: "__crabka_grpc_gateway_membership_fh_tls".into(),
            tls,
            broker_security: None,
            authz: None,
            webhooks: std::collections::HashMap::new(),
            outbound: Vec::new(),
            schema_registry_url: None,
        }),
        authz: Arc::new(crabka_grpc_gateway::authz::GatewayAuthz::new(Arc::new(
            crabka_authz::AllowAllAuthorizer,
        ))),
        codec: Arc::new(RawCodec),
    });

    let app = forward_router(state);

    let fr = ForwardRecord {
        topic: "t".into(),
        key: None,
        value: vec![1],
        headers: vec![],
        partition: None,
        timestamp_ms: None,
        idempotency_key: Some("k".into()),
        principal: None,
    };

    // No principal extension on the request — anonymous caller.
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/v1/forward")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&fr).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let result: ForwardResult = serde_json::from_slice(&bytes).unwrap();

    // The gate returns an error with retriable: false — no broker round-trip.
    assert_eq!(
        (
            status,
            result.partition,
            result.offset,
            result.deduplicated,
            result.error.map(|error| error.retriable),
        ),
        (
            StatusCode::FORBIDDEN,
            PartitionIndex(-1),
            Offset(-1),
            false,
            Some(false),
        ),
        "complete anonymous-rejection result"
    );

    broker.shutdown().await;
}
