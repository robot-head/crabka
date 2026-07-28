//! Streaming Connect handlers: `SendStream` (produce) and `Subscribe` (consume).

use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};

use assert2::check;
use bytes::Bytes;
use connectrpc_axum::message::Streaming;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_grpc_gateway::{
    codec::RawCodec, config::GatewayConfig, pb, produce::ProduceCore, state::AppState, streaming,
};
use crabka_units::prelude::*;
use futures_util::StreamExt;
use tempfile::TempDir;

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn state_for(bootstrap: &str) -> Arc<AppState> {
    let produce = ProduceCore::new(bootstrap, "stream", Arc::new(RawCodec), None)
        .await
        .unwrap();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    Arc::new(AppState {
        produce: Arc::new(produce),
        config: Arc::new(GatewayConfig {
            bootstrap: bootstrap.to_string(),
            listen_addr: addr,
            client_id: "stream".into(),
            dedup_topic: "__crabka_grpc_dedup".into(),
            dedup_partitions: 4,
            dedup_window: hours(1),
            dedup_ownership_group: "__crabka_grpc_gateway_dedup_owners".into(),
            dedup_txn_id_prefix: "stream-dedup".into(),
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
    })
}

/// On-behalf-of identity for the `*_inner` helpers: ANONYMOUS over the unknown
/// host. State carries an `AllowAllAuthorizer`, so the value is immaterial to
/// the decision (every record is allowed) — it just satisfies the signature.
fn anon() -> (crabka_security::Principal, SocketAddr) {
    (
        crabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: crabka_security::AuthMethod::Anonymous,
            groups: vec![],
        },
        "0.0.0.0:0".parse().unwrap(),
    )
}

fn rec(topic: &str, value: &'static [u8]) -> pb::Record {
    pb::Record {
        topic: topic.into(),
        key: None,
        body: Some(pb::record::Body::Raw(value.to_vec())),
        headers: std::collections::HashMap::default(),
        partition: None,
        timestamp_ms: None,
        idempotency_key: None,
        schema: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_stream_produces_all_records() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "ss-topic".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();
    let state = state_for(&bootstrap).await;

    let input = futures_util::stream::iter(vec![
        Ok(pb::SendRequest {
            records: vec![rec("ss-topic", b"a")],
            acks: 0,
        }),
        Ok(pb::SendRequest {
            records: vec![rec("ss-topic", b"b")],
            acks: 0,
        }),
    ]);
    let inbound = Streaming::new(Box::pin(input));

    let (p, h) = anon();
    let acks: Vec<_> = streaming::send_stream_inner(inbound, state, p, h)
        .collect()
        .await;
    check!(acks.len() == 2);
    for a in &acks {
        let ack = a.as_ref().expect("ack ok");
        check!(
            ack.results
                .iter()
                .map(|result| result.error.is_none())
                .collect::<Vec<_>>()
                == vec![true]
        );
    }

    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.clone())
        .group_id("ss-reader")
        .subscribe(vec!["ss-topic".to_string()])
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    let mut seen = 0;
    for _ in 0..10 {
        seen += consumer
            .poll(std::time::Duration::from_millis(500))
            .await
            .unwrap()
            .len();
        if seen >= 2 {
            break;
        }
    }
    check!(seen == 2);

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_streams_records_then_commits() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "sub-topic".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();
    let state = state_for(&bootstrap).await;

    // Produce one record up front.
    let (prod_principal, _) = anon();
    crabka_grpc_gateway::produce::ProduceCore::new(
        &bootstrap,
        "sub-prod",
        Arc::new(RawCodec),
        None,
    )
    .await
    .unwrap()
    .produce(
        crabka_grpc_gateway::types::GatewayRecord {
            topic: "sub-topic".into(),
            key: None,
            value: Bytes::from_static(b"hello"),
            body_structured: None,
            headers: vec![],
            partition: None,
            timestamp_ms: None,
            idempotency_key: None,
        },
        &prod_principal,
    )
    .await
    .unwrap();

    // Control stream: a Start frame (auto_commit), then stays open until dropped.
    let start = pb::SubscribeFrame {
        frame: Some(pb::subscribe_frame::Frame::Start(pb::SubscribeStart {
            group_id: "sub-group".into(),
            topics: vec!["sub-topic".into()],
            auto_commit: true,
            predicates: vec![],
        })),
    };
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<
        Result<pb::SubscribeFrame, connectrpc_axum::message::ConnectError>,
    >();
    tx.send(Ok(start)).unwrap();
    let inbound = Streaming::new(Box::pin(
        tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
    ));

    let (p, h) = anon();
    let mut out = Box::pin(streaming::subscribe_inner(inbound, state, p, h));
    let mut got = None;
    for _ in 0..20 {
        match tokio::time::timeout(std::time::Duration::from_millis(600), out.next()).await {
            Ok(Some(Ok(msg))) => {
                got = Some(msg);
                break;
            }
            Ok(Some(Err(e))) => panic!("subscribe error: {e:?}"),
            Ok(None) => break,
            Err(_) => {} // timed out this round; retry the poll
        }
    }
    // The loop above already captured (and broke on) the first record, so this
    // asserts on the already-received Inbound. Dropping the control stream just
    // releases the session's resources — the test does not wait to observe the
    // subscription closing.
    drop(tx);
    let msg = got.expect("received an Inbound record");
    check!((msg.topic.as_str(), msg.value.as_slice()) == ("sub-topic", b"hello".as_slice()));

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_wrappers_and_router_build() {
    use connectrpc_axum::message::{ConnectError as CErr, ConnectRequest};

    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "wrap-topic".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();
    let state = state_for(&bootstrap).await;

    // Router builds with both streaming methods registered (covers lib::router).
    let _router = crabka_grpc_gateway::router(state.clone());

    // send_stream wrapper → Ok with a StreamBody (covers the wrapper).
    let send_input = futures_util::stream::iter(vec![Ok::<_, CErr>(pb::SendRequest {
        records: vec![rec("wrap-topic", b"x")],
        acks: 0,
    })]);
    let send_req = ConnectRequest(Streaming::new(Box::pin(send_input)));
    let send_resp =
        streaming::send_stream(axum::Extension(state.clone()), None, None, send_req).await;
    check!(send_resp.is_ok());

    // subscribe wrapper → Ok (inner stream is lazy; not driven here).
    let sub_input = futures_util::stream::iter(Vec::<Result<pb::SubscribeFrame, CErr>>::new());
    let sub_req = ConnectRequest(Streaming::new(Box::pin(sub_input)));
    let sub_resp = streaming::subscribe(axum::Extension(state.clone()), None, None, sub_req).await;
    check!(sub_resp.is_ok());

    broker.shutdown().await;
}

/// Connect proto content-type regression: a connect-go client posts a unary
/// `application/proto` request and requires the 200 response to echo it. An
/// all-default `SendRequest` (no records) encodes to an empty body and the
/// `Send` handler returns 200 without producing. Before the `.build_connect()`
/// fix the router replied `application/json`, which proto clients reject with
/// `invalid content-type: "application/json"; expecting "application/proto"`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_echoes_proto_content_type() {
    use axum::{
        body::Body,
        http::{Method, Request, header::CONTENT_TYPE},
    };
    use tower::ServiceExt as _;

    let (broker, bootstrap, _dir) = boot().await;
    let state = state_for(&bootstrap).await;
    let app = crabka_grpc_gateway::router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/crabka.gateway.v1.Gateway/Send")
                .header(CONTENT_TYPE, "application/proto")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("router responds");

    let status = response.status();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    check!(status.is_success());
    check!(content_type.starts_with("application/proto"));

    broker.shutdown().await;
}
