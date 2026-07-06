//! Produce via `ProduceCore` against an in-process broker; read the record
//! back with a native consumer to prove it landed.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use assert2::check;
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_grpc_gateway::{codec::RawCodec, produce::ProduceCore, types::GatewayRecord};
use tempfile::TempDir;

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn create_topic(bootstrap: &str, name: &str, partitions: i32) {
    let mut admin = AdminClient::connect(&[bootstrap.to_string()])
        .await
        .expect("admin");
    let spec = CreateTopicSpec {
        name: name.to_string(),
        partitions,
        replicas: 1,
        configs: BTreeMap::new(),
    };
    admin
        .create_topics(&[spec], 10_000)
        .await
        .expect("create_topics");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_plain_then_read_back() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, "send-itest", 1).await;

    let core = ProduceCore::new(&bootstrap, "gw-itest", Arc::new(RawCodec), None)
        .await
        .expect("core");

    let anon = crabka_security::Principal {
        name: "ANONYMOUS".into(),
        auth_method: crabka_security::AuthMethod::Anonymous,
        groups: vec![],
    };
    let outcome = core
        .produce(
            GatewayRecord {
                topic: "send-itest".into(),
                key: None,
                value: Bytes::from_static(b"payload-1"),
                body_structured: None,
                headers: vec![],
                partition: None,
                timestamp_ms: None,
                idempotency_key: None,
            },
            &anon,
        )
        .await
        .expect("produce");
    check!(outcome.partition == 0);
    check!(outcome.deduplicated == false);

    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.clone())
        .group_id("send-itest-reader")
        .subscribe(vec!["send-itest".to_string()])
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("consumer");

    let mut seen = vec![];
    for _ in 0..20 {
        let recs = consumer
            .poll(Duration::from_millis(500))
            .await
            .expect("poll");
        for r in recs {
            seen.push(r.value.unwrap_or_default());
        }
        if !seen.is_empty() {
            break;
        }
    }
    check!(seen.iter().any(|v| v.as_ref() == b"payload-1"));

    broker.shutdown().await;
}
