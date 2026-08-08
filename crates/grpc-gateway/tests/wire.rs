//! Wire-surface tests: the unary `Send` Connect handler (Ok + error arms),
//! the gateway router construction, and the health/readiness endpoints.
//!
//! Handlers are called directly with a constructed `ConnectRequest` — the
//! same level a real Connect call reaches, minus HTTP serialization —
//! mirroring `crates/rebalancer/tests/end_to_end.rs`. The health router is
//! driven through `tower`'s `oneshot` so the `/healthz` and `/readyz` route
//! closures are exercised.

use std::{collections::BTreeMap, sync::Arc};

use axum::{
    Extension,
    body::Body,
    http::{Request, StatusCode},
};
use connectrpc_axum::message::{ConnectRequest, ConnectResponse};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_grpc_gateway::{
    codec::RawCodec,
    config::GatewayConfig,
    dedup::{DedupEngine, store::DedupStore},
    handlers,
    health::{self, Readiness},
    pb,
    produce::ProduceCore,
    state::AppState,
};
use crabka_units::prelude::*;
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
    assert2::assert!(resp.status() == StatusCode::OK);

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
    assert2::assert!(resp.status() == StatusCode::SERVICE_UNAVAILABLE);

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
    assert2::assert!(resp.status() == StatusCode::OK);
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
            crabka_units::secs(10),
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
        None,
    ));
    let produce = ProduceCore::new(&bootstrap, "wire", Arc::new(RawCodec), None)
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
            dedup_window: hours(1),
            dedup_ownership_group: "__crabka_grpc_gateway_dedup_owners".into(),
            dedup_txn_id_prefix: "wire-dedup".into(),
            advertised_addr: "127.0.0.1:0".into(),
            membership_topic: "__crabka_grpc_gateway_membership".into(),
            tls: None,
            broker_security: None,
            authz: None,
            webhooks: std::collections::HashMap::new(),
            outbound: Vec::new(),
            schema_registry_url: None,
            runtime: crabka_grpc_gateway::config::GatewayRuntimeConfig::default(),
        }),
        authz: Arc::new(crabka_grpc_gateway::authz::GatewayAuthz::new(Arc::new(
            crabka_authz::AllowAllAuthorizer,
        ))),
        codec: Arc::new(RawCodec),
        queue: Arc::default(),
    });

    // Constructing the Connect router covers `lib::router`.
    let _router = crabka_grpc_gateway::router(state.clone());

    let send = pb::SendRequest {
        records: vec![
            pb::Record {
                topic: "wire-topic".into(),
                key: None,
                body: Some(pb::record::Body::Raw(b"ok".to_vec())),
                headers: BTreeMap::new().into_iter().collect(),
                partition: None,
                timestamp_ms: None,
                idempotency_key: None,
                schema: None,
            },
            pb::Record {
                topic: "wire-topic".into(),
                key: None,
                body: Some(pb::record::Body::Raw(b"dup".to_vec())),
                headers: BTreeMap::new().into_iter().collect(),
                partition: None,
                timestamp_ms: None,
                idempotency_key: Some("k1".into()),
                schema: None,
            },
        ],
        acks: pb::Acks::All as i32,
    };

    let resp: ConnectResponse<pb::SendResponse> =
        handlers::send(Extension(state), None, None, req(send))
            .await
            .expect("handler returned Err");
    let body = resp.0;
    assert2::assert!(
        body.results
            .iter()
            .map(|result| (result.partition, result.error.is_some()))
            .collect::<Vec<_>>()
            == vec![(0, false), (-1, true)]
    );

    broker.shutdown().await;
}
