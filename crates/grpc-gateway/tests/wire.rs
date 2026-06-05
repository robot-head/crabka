//! Wire-surface tests: the unary `Send` Connect handler (Ok + error arms),
//! the gateway router construction, and the health/readiness endpoints.
//!
//! Handlers are called directly with a constructed `ConnectRequest` — the
//! same level a real Connect call reaches, minus HTTP serialization —
//! mirroring `crates/rebalancer/tests/end_to_end.rs`. The health router is
//! driven through `tower`'s `oneshot` so the `/healthz` and `/readyz` route
//! closures are exercised.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::Extension;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use connectrpc_axum::message::{ConnectRequest, ConnectResponse};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_grpc_gateway::codec::RawCodec;
use crabka_grpc_gateway::config::GatewayConfig;
use crabka_grpc_gateway::dedup::DedupEngine;
use crabka_grpc_gateway::dedup::store::DedupStore;
use crabka_grpc_gateway::health::{self, Readiness};
use crabka_grpc_gateway::produce::ProduceCore;
use crabka_grpc_gateway::state::AppState;
use crabka_grpc_gateway::{handlers, pb};
use tempfile::TempDir;
use tower::ServiceExt;

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

fn req<T>(msg: T) -> ConnectRequest<T> {
    ConnectRequest(msg)
}

#[tokio::test]
async fn health_endpoints_reflect_readiness() {
    let readiness = Readiness::new();

    // `/healthz` is always 200 once serving.
    let resp = health::router(readiness.clone())
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // `/readyz` is 503 until the readiness flag is set...
    let resp = health::router(readiness.clone())
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

    // ...and 200 afterwards.
    readiness.set_ready();
    let resp = health::router(readiness)
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// Drive the `Send` handler for both result arms: an unkeyed record takes the
/// plain path and succeeds; a keyed record routes to a dedup engine whose
/// store has never run `run_ownership`, so it returns `Unavailable` and the
/// handler maps it to a per-record `ErrorInfo`. Also constructs the Connect router.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_handler_ok_and_error_arms() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "wire-topic".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();

    // A dedup engine over a store that never ran run_ownership → keyed records fail
    // with `Unavailable`, exercising the handler's error arm deterministically.
    let store = Arc::new(DedupStore::new(4));
    let engine = Arc::new(DedupEngine::new(
        &bootstrap,
        "wire",
        "wire-dedup",
        "__crabka_grpc_dedup".to_string(),
        4,
        store,
    ));
    let produce = ProduceCore::new(&bootstrap, "wire", Arc::new(RawCodec))
        .await
        .unwrap()
        .with_dedup(engine);
    let state = Arc::new(AppState {
        produce: Arc::new(produce),
        config: Arc::new(GatewayConfig {
            bootstrap: bootstrap.clone(),
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            client_id: "wire".into(),
            dedup_topic: "__crabka_grpc_dedup".into(),
            dedup_partitions: 4,
            dedup_window_ms: 3_600_000,
            dedup_txn_id_prefix: "wire-dedup".into(),
        }),
    });

    // Constructing the Connect router covers `lib::router`.
    let _router = crabka_grpc_gateway::router(state.clone());

    let send = pb::SendRequest {
        records: vec![
            pb::Record {
                topic: "wire-topic".into(),
                key: None,
                value: b"ok".to_vec(),
                headers: BTreeMap::new().into_iter().collect(),
                partition: None,
                timestamp_ms: None,
                idempotency_key: None,
            },
            pb::Record {
                topic: "wire-topic".into(),
                key: None,
                value: b"dup".to_vec(),
                headers: BTreeMap::new().into_iter().collect(),
                partition: None,
                timestamp_ms: None,
                idempotency_key: Some("k1".into()),
            },
        ],
        acks: pb::Acks::All as i32,
    };

    let resp: ConnectResponse<pb::SendResponse> = handlers::send(Extension(state), req(send))
        .await
        .expect("handler returned Err");
    let body = resp.0;
    assert_eq!(body.results.len(), 2);
    // Unkeyed record produced successfully.
    assert!(body.results[0].error.is_none());
    assert_eq!(body.results[0].partition, 0);
    // Keyed record hit the unowned dedup store → per-record error, no panic.
    assert!(body.results[1].error.is_some());

    broker.shutdown().await;
}
