//! Streaming Connect handlers: `SendStream` (produce) and `Subscribe` (consume).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use assert2::check;
use connectrpc_axum::message::Streaming;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_grpc_gateway::codec::RawCodec;
use crabka_grpc_gateway::config::GatewayConfig;
use crabka_grpc_gateway::produce::ProduceCore;
use crabka_grpc_gateway::state::AppState;
use crabka_grpc_gateway::{pb, streaming};
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
    let produce = ProduceCore::new(bootstrap, "stream", Arc::new(RawCodec))
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
            dedup_window_ms: 3_600_000,
            dedup_txn_id_prefix: "stream-dedup".into(),
        }),
    })
}

fn rec(topic: &str, value: &'static [u8]) -> pb::Record {
    pb::Record {
        topic: topic.into(),
        key: None,
        value: value.to_vec(),
        headers: std::collections::HashMap::default(),
        partition: None,
        timestamp_ms: None,
        idempotency_key: None,
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

    let acks: Vec<_> = streaming::send_stream_inner(inbound, state).collect().await;
    check!(acks.len() == 2);
    for a in &acks {
        let ack = a.as_ref().expect("ack ok");
        check!(ack.results.len() == 1);
        check!(ack.results[0].error.is_none());
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
