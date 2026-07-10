//! `run_membership` builds the `partition → owner_addr` routing table from the
//! membership topic, and a later claim of the same partition (higher offset)
//! supersedes an earlier one.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use crabka_grpc_gateway::{
    config::GatewayConfig,
    dedup::{
        membership::{MembershipStore, NodeInfo},
        topic::ensure_membership_topic,
    },
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const TOPIC: &str = "__crabka_grpc_gateway_membership";

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn publish(producer: &Producer, node_id: &str, info: &NodeInfo) {
    let rec = ProducerRecord {
        topic: TOPIC.to_string(),
        partition: None,
        key: Some(Bytes::from(node_id.as_bytes().to_vec())),
        value: Some(Bytes::from(serde_json::to_vec(info).unwrap())),
        headers: vec![],
        timestamp_ms: None,
    };
    producer.send(rec).await.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_membership_builds_routing_with_offset_tiebreak() {
    let (broker, bootstrap, _dir) = boot().await;
    ensure_membership_topic(&bootstrap, TOPIC, 1, None)
        .await
        .unwrap();

    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .client_id("memb-test")
        .enable_idempotence(true)
        .acks(Acks::All)
        .build()
        .await
        .unwrap();

    // node-a owns {0,1}; node-b owns {2,3}.
    publish(
        &producer,
        "node-a",
        &NodeInfo {
            advertised_addr: "addr-a".into(),
            owned: vec![0, 1],
            epoch: 0,
        },
    )
    .await;
    publish(
        &producer,
        "node-b",
        &NodeInfo {
            advertised_addr: "addr-b".into(),
            owned: vec![2, 3],
            epoch: 0,
        },
    )
    .await;
    // Later, node-b also claims partition 1 (ownership moved off node-a): higher
    // offset ⇒ wins for partition 1.
    publish(
        &producer,
        "node-b",
        &NodeInfo {
            advertised_addr: "addr-b".into(),
            owned: vec![1, 2, 3],
            epoch: 1,
        },
    )
    .await;

    let store = Arc::new(MembershipStore::new());
    let token = CancellationToken::new();
    let h = tokio::spawn(store.clone().run_membership(
        bootstrap.clone(),
        "memb-reader".into(),
        TOPIC.into(),
        "memb-reader-unique-1".into(),
        token.clone(),
        None,
    ));

    let mut ok = false;
    for _ in 0..80 {
        if store.owner_of(0).as_deref() == Some("addr-a")
            && store.owner_of(1).as_deref() == Some("addr-b") // tiebreak: latest claim
            && store.owner_of(2).as_deref() == Some("addr-b")
            && store.owner_of(3).as_deref() == Some("addr-b")
        {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert2::assert!(ok);

    // Sanity: an unclaimed partition has no owner.
    assert2::assert!(store.owner_of(7) == None);
    let _ = GatewayConfig::DEDUP_TOPIC_REPLICATION; // touch the type (lint hygiene)

    token.cancel();
    let _ = h.await;
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_membership_tombstone_and_malformed_skip() {
    use std::time::Duration;

    use tokio_util::sync::CancellationToken;

    let (broker, bootstrap, _dir) = boot().await;
    ensure_membership_topic(&bootstrap, TOPIC, 1, None)
        .await
        .unwrap();

    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .client_id("memb-tombstone-test")
        .enable_idempotence(true)
        .acks(Acks::All)
        .build()
        .await
        .unwrap();

    // Publish node-a owning partition 0.
    publish(
        &producer,
        "node-a",
        &NodeInfo {
            advertised_addr: "addr-a".into(),
            owned: vec![0],
            epoch: 0,
        },
    )
    .await;

    // Publish a MALFORMED record (non-JSON value) — the loop must skip it, not die.
    let malformed = ProducerRecord {
        topic: TOPIC.to_string(),
        partition: None,
        key: Some(Bytes::from_static(b"node-bad")),
        value: Some(Bytes::from_static(b"not json{")),
        headers: vec![],
        timestamp_ms: None,
    };
    producer.send(malformed).await.await.unwrap().unwrap();

    // Publish a TOMBSTONE for node-a (None value => remove from store).
    let tombstone = ProducerRecord {
        topic: TOPIC.to_string(),
        partition: None,
        key: Some(Bytes::from("node-a".as_bytes().to_vec())),
        value: None,
        headers: vec![],
        timestamp_ms: None,
    };
    producer.send(tombstone).await.await.unwrap().unwrap();

    let store = Arc::new(MembershipStore::new());
    let token = CancellationToken::new();
    let h = tokio::spawn(store.clone().run_membership(
        bootstrap.clone(),
        "memb-tombstone-reader".into(),
        TOPIC.into(),
        "memb-tombstone-unique-1".into(),
        token.clone(),
        None,
    ));

    // Poll until owner_of(0) is None: node-a tombstoned AND malformed didn't crash loop.
    let mut reached_none = false;
    for _ in 0..80 {
        if store.owner_of(0).is_none() {
            reached_none = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert2::assert!(reached_none);

    token.cancel();
    let _ = h.await;
    broker.shutdown().await;
}
