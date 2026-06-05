//! Dedup: warm-up reconstructs the claim map from the compacted topic, so a
//! post-restart duplicate is recognized.

use std::sync::Arc;

use assert2::check;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_grpc_gateway::dedup::store::{ClaimValue, DedupStore};
use crabka_grpc_gateway::dedup::topic::ensure_dedup_topic;
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
async fn warmup_reads_existing_claims() {
    let (broker, bootstrap, _dir) = boot().await;
    let topic = "__crabka_grpc_dedup";
    ensure_dedup_topic(&bootstrap, topic, 4, 3_600_000, 1)
        .await
        .unwrap();

    let store = Arc::new(DedupStore::new(4));
    store
        .write_claim(
            &bootstrap,
            "gw-dedup-writer",
            topic,
            "key-A",
            &ClaimValue {
                topic: "user".into(),
                partition: 0,
                offset: 7,
            },
        )
        .await
        .unwrap();

    let store2 = Arc::new(DedupStore::new(4));
    store2
        .warm_up(&bootstrap, "gw-dedup-warm", topic)
        .await
        .unwrap();
    check!(store2.is_ready());
    let got = store2.get("key-A");
    check!(got.is_some());
    check!(got.unwrap().offset == 7);
    check!(store2.get("absent").is_none());

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_idempotency_key_produces_once() {
    use bytes::Bytes;
    use crabka_client_admin::{AdminClient, CreateTopicSpec};
    use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
    use crabka_grpc_gateway::codec::RawCodec;
    use crabka_grpc_gateway::dedup::DedupEngine;
    use crabka_grpc_gateway::dedup::store::DedupStore;
    use crabka_grpc_gateway::dedup::topic::ensure_dedup_topic;
    use crabka_grpc_gateway::produce::ProduceCore;
    use crabka_grpc_gateway::types::GatewayRecord;
    use std::collections::BTreeMap;

    let (broker, bootstrap, _dir) = boot().await;
    let dedup_topic = "__crabka_grpc_dedup";
    ensure_dedup_topic(&bootstrap, dedup_topic, 4, 3_600_000, 1)
        .await
        .unwrap();
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "dedup-user".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();

    let store = Arc::new(DedupStore::new(4));
    store
        .warm_up(&bootstrap, "gw-warm", dedup_topic)
        .await
        .unwrap();
    let engine = Arc::new(DedupEngine::new(
        &bootstrap,
        "gw-dedup",
        "crabka-grpc-dedup",
        dedup_topic.to_string(),
        4,
        store.clone(),
    ));
    let core = ProduceCore::new(&bootstrap, "gw-prod", Arc::new(RawCodec))
        .await
        .unwrap()
        .with_dedup(engine);

    let mk = || GatewayRecord {
        topic: "dedup-user".into(),
        key: None,
        value: Bytes::from_static(b"once"),
        headers: vec![],
        partition: None,
        timestamp_ms: None,
        idempotency_key: Some("idem-1".into()),
    };

    let first = core.produce(mk()).await.unwrap();
    let second = core.produce(mk()).await.unwrap();
    assert!(!first.deduplicated);
    assert!(second.deduplicated);
    assert_eq!(first.partition, second.partition);
    assert_eq!(first.offset, second.offset);

    // Exactly one record landed in the user topic.
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.clone())
        .group_id("dedup-count")
        .subscribe(vec!["dedup-user".to_string()])
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    let mut count = 0;
    for _ in 0..10 {
        count += consumer
            .poll(std::time::Duration::from_millis(500))
            .await
            .unwrap()
            .len();
    }
    assert_eq!(count, 1);

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_ownership_rebuilds_map_and_owns_all_as_sole_member() {
    use crabka_grpc_gateway::dedup::store::{ClaimValue, DedupStore};
    use crabka_grpc_gateway::dedup::topic::ensure_dedup_topic;
    use tokio_util::sync::CancellationToken;

    let (broker, bootstrap, _dir) = boot().await;
    let topic = "__crabka_grpc_dedup";
    ensure_dedup_topic(&bootstrap, topic, 4, 3_600_000, 1)
        .await
        .unwrap();

    let writer = Arc::new(DedupStore::new(4));
    writer
        .write_claim(
            &bootstrap,
            "gw-own-writer",
            topic,
            "key-A",
            &ClaimValue {
                topic: "u".into(),
                partition: 0,
                offset: 9,
            },
        )
        .await
        .unwrap();

    let store = Arc::new(DedupStore::new(4));
    let token = CancellationToken::new();
    let handle = tokio::spawn(store.clone().run_ownership(
        bootstrap.clone(),
        "gw-own".into(),
        topic.to_string(),
        "__crabka_grpc_gateway_dedup_owners".into(),
        token.clone(),
    ));

    let mut warm = false;
    for _ in 0..80 {
        if store.has_warmed_once() {
            warm = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert!(warm, "store never warmed");
    // Sole member owns all 4 partitions.
    for p in 0..4u32 {
        assert!(store.owns(p), "should own partition {p}");
    }
    // Map rebuilt from the topic.
    assert_eq!(store.get("key-A").map(|c| c.offset), Some(9));

    token.cancel();
    let _ = handle.await;
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_duplicates_produce_once() {
    use bytes::Bytes;
    use crabka_client_admin::{AdminClient, CreateTopicSpec};
    use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
    use crabka_grpc_gateway::codec::RawCodec;
    use crabka_grpc_gateway::dedup::DedupEngine;
    use crabka_grpc_gateway::dedup::store::DedupStore;
    use crabka_grpc_gateway::dedup::topic::ensure_dedup_topic;
    use crabka_grpc_gateway::produce::ProduceCore;
    use crabka_grpc_gateway::types::GatewayRecord;
    use std::collections::BTreeMap;

    let (broker, bootstrap, _dir) = boot().await;
    let dedup_topic = "__crabka_grpc_dedup";
    ensure_dedup_topic(&bootstrap, dedup_topic, 4, 3_600_000, 1)
        .await
        .unwrap();
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "dedup-conc".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            10_000,
        )
        .await
        .unwrap();

    let store = Arc::new(DedupStore::new(4));
    store
        .warm_up(&bootstrap, "gw-warm2", dedup_topic)
        .await
        .unwrap();
    let engine = Arc::new(DedupEngine::new(
        &bootstrap,
        "gw-dedup2",
        "crabka-grpc-dedup",
        dedup_topic.to_string(),
        4,
        store.clone(),
    ));
    let core = Arc::new(
        ProduceCore::new(&bootstrap, "gw-prod2", Arc::new(RawCodec))
            .await
            .unwrap()
            .with_dedup(engine),
    );

    let mut handles = vec![];
    for _ in 0..8 {
        let core = core.clone();
        handles.push(tokio::spawn(async move {
            core.produce(GatewayRecord {
                topic: "dedup-conc".into(),
                key: None,
                value: Bytes::from_static(b"x"),
                headers: vec![],
                partition: None,
                timestamp_ms: None,
                idempotency_key: Some("same".into()),
            })
            .await
            .unwrap()
        }));
    }
    let mut deduped = 0;
    for h in handles {
        if h.await.unwrap().deduplicated {
            deduped += 1;
        }
    }
    assert_eq!(
        deduped, 7,
        "exactly one of 8 should be the original producer"
    );

    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.clone())
        .group_id("dedup-conc-count")
        .subscribe(vec!["dedup-conc".to_string()])
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    let mut count = 0;
    for _ in 0..10 {
        count += consumer
            .poll(std::time::Duration::from_millis(500))
            .await
            .unwrap()
            .len();
    }
    assert_eq!(count, 1);

    broker.shutdown().await;
}
