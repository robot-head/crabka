use std::{collections::BTreeMap, sync::Arc, time::Duration};

use assert2::check;
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_grpc_gateway::{
    codec::RawCodec, consume::ConsumeSession, produce::ProduceCore, types::GatewayRecord,
};
use tempfile::TempDir;

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_receives_then_commits() {
    let (broker, bootstrap, _dir) = boot().await;
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "consume-itest".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();

    let core = ProduceCore::new(&bootstrap, "gw-c", Arc::new(RawCodec), None)
        .await
        .unwrap();
    let anon = crabka_security::Principal {
        name: "ANONYMOUS".into(),
        auth_method: crabka_security::AuthMethod::Anonymous,
        groups: vec![],
    };
    core.produce(
        GatewayRecord {
            topic: "consume-itest".into(),
            key: None,
            value: Bytes::from_static(b"c1"),
            body_structured: None,
            headers: vec![],
            partition: None,
            timestamp_ms: None,
            idempotency_key: None,
        },
        &anon,
    )
    .await
    .unwrap();

    let mut session = ConsumeSession::new(
        &bootstrap,
        "gw-consume-group",
        "gw-c",
        vec!["consume-itest".to_string()],
        None,
        Arc::new(RawCodec),
    )
    .await
    .unwrap();

    let mut got = vec![];
    for _ in 0..20 {
        let batch = session.poll(Duration::from_millis(500)).await.unwrap();
        for r in batch {
            got.push(r.value.clone());
        }
        if !got.is_empty() {
            break;
        }
    }
    check!(got.iter().any(|v| v.as_ref() == b"c1".as_ref()));
    session.commit().await.unwrap();

    broker.shutdown().await;
}
