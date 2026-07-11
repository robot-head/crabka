//! Dedup: ownership consumer reconstructs the claim map from the compacted
//! topic, so a post-restart duplicate is recognized.

use std::{collections::BTreeMap, sync::Arc};

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use tempfile::TempDir;

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

/// The resolved caller relayed on a forward. With `AllowAll` the value is
/// immaterial — it only satisfies `produce`'s signature for these local tests.
fn anon() -> crabka_security::Principal {
    crabka_security::Principal {
        name: "ANONYMOUS".into(),
        auth_method: crabka_security::AuthMethod::Anonymous,
        groups: vec![],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_idempotency_key_produces_once() {
    use bytes::Bytes;
    use crabka_client_admin::{AdminClient, CreateTopicSpec};
    use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
    use crabka_grpc_gateway::{
        codec::RawCodec,
        dedup::{DedupEngine, store::DedupStore, topic::ensure_dedup_topic},
        produce::ProduceCore,
        types::GatewayRecord,
    };
    use tokio_util::sync::CancellationToken;
    let (broker, bootstrap, _dir) = boot().await;
    let dedup_topic = "__crabka_grpc_dedup";
    ensure_dedup_topic(&bootstrap, dedup_topic, 4, 3_600_000, 1, None)
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
    let token = CancellationToken::new();
    let own = tokio::spawn(store.clone().run_ownership(
        bootstrap.clone(),
        "gw-warm".into(),
        dedup_topic.to_string(),
        "__crabka_grpc_gateway_dedup_owners".into(),
        token.clone(),
        None,
    ));
    let mut warmed = false;
    for _ in 0..80 {
        if store.has_warmed_once() {
            warmed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert2::assert!(warmed);
    let engine = Arc::new(DedupEngine::new(
        &bootstrap,
        "gw-dedup",
        "crabka-grpc-dedup",
        dedup_topic.to_string(),
        4,
        store.clone(),
        None,
    ));
    let core = ProduceCore::new(&bootstrap, "gw-prod", Arc::new(RawCodec), None)
        .await
        .unwrap()
        .with_dedup(engine);
    let mk = || GatewayRecord {
        topic: "dedup-user".into(),
        key: None,
        value: Bytes::from_static(b"once"),
        body_structured: None,
        headers: vec![],
        partition: None,
        timestamp_ms: None,
        idempotency_key: Some("idem-1".into()),
    };
    let anon = anon();
    let first = core.produce(mk(), &anon).await.unwrap();
    let second = core.produce(mk(), &anon).await.unwrap();
    assert2::assert!(!first.deduplicated);
    assert2::assert!(second.deduplicated);
    assert2::assert!(second.partition == first.partition);
    assert2::assert!(second.offset == first.offset);
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
    assert2::assert!(count == 1);
    token.cancel();
    let _ = own.await;
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_ownership_rebuilds_map_and_owns_all_as_sole_member() {
    use crabka_grpc_gateway::{
        dedup::{
            store::{ClaimValue, DedupStore},
            topic::ensure_dedup_topic,
        },
        ids::{Offset, PartitionIndex},
    };
    use tokio_util::sync::CancellationToken;

    let (broker, bootstrap, _dir) = boot().await;
    let topic = "__crabka_grpc_dedup";
    ensure_dedup_topic(&bootstrap, topic, 4, 3_600_000, 1, None)
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
                partition: PartitionIndex(0),
                offset: Offset(9),
            },
            None,
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
        None,
    ));

    let mut warm = false;
    for _ in 0..80 {
        if store.has_warmed_once() {
            warm = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert2::assert!(warm);
    // Sole member owns all 4 partitions.
    for p in 0..4u32 {
        assert2::assert!(store.owns(p));
    }
    // Map rebuilt from the topic.
    assert2::assert!(store.get("key-A").map(|c| c.offset) == Some(Offset(9)));

    token.cancel();
    let _ = handle.await;
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_duplicates_produce_once() {
    use std::collections::BTreeMap;

    use bytes::Bytes;
    use crabka_client_admin::{AdminClient, CreateTopicSpec};
    use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
    use crabka_grpc_gateway::{
        codec::RawCodec,
        dedup::{DedupEngine, store::DedupStore, topic::ensure_dedup_topic},
        produce::ProduceCore,
        types::GatewayRecord,
    };
    use tokio_util::sync::CancellationToken;

    let (broker, bootstrap, _dir) = boot().await;
    let dedup_topic = "__crabka_grpc_dedup";
    ensure_dedup_topic(&bootstrap, dedup_topic, 4, 3_600_000, 1, None)
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
    let token = CancellationToken::new();
    let own = tokio::spawn(store.clone().run_ownership(
        bootstrap.clone(),
        "gw-warm2".into(),
        dedup_topic.to_string(),
        "__crabka_grpc_gateway_dedup_owners".into(),
        token.clone(),
        None,
    ));
    let mut warmed = false;
    for _ in 0..80 {
        if store.has_warmed_once() {
            warmed = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert2::assert!(warmed);
    let engine = Arc::new(DedupEngine::new(
        &bootstrap,
        "gw-dedup2",
        "crabka-grpc-dedup",
        dedup_topic.to_string(),
        4,
        store.clone(),
        None,
    ));
    let core = Arc::new(
        ProduceCore::new(&bootstrap, "gw-prod2", Arc::new(RawCodec), None)
            .await
            .unwrap()
            .with_dedup(engine),
    );

    let mut handles = vec![];
    for _ in 0..8 {
        let core = core.clone();
        handles.push(tokio::spawn(async move {
            core.produce(
                GatewayRecord {
                    topic: "dedup-conc".into(),
                    key: None,
                    value: Bytes::from_static(b"x"),
                    body_structured: None,
                    headers: vec![],
                    partition: None,
                    timestamp_ms: None,
                    idempotency_key: Some("same".into()),
                },
                &anon(),
            )
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
    assert2::assert!(deduped == 7);

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
    assert2::assert!(count == 1);

    token.cancel();
    let _ = own.await;
    broker.shutdown().await;
}
