//! Two ownership consumers in one group split the dedup-topic partitions, so
//! only the OWNING replica serves a keyed produce. The non-owner returns a
//! retriable Unavailable.
//!
//! These tests are timing-sensitive, because of the group join and rebalance,
//! so the waits are generous. The repo has a history of consumer-group test
//! flakes.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_grpc_gateway::{
    codec::RawCodec,
    dedup::{DedupEngine, partition_for, store::DedupStore, topic::ensure_dedup_topic},
    error::GatewayError,
    produce::ProduceCore,
    types::GatewayRecord,
};
use crabka_units::prelude::*;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const N: u32 = 4;
const DEDUP: &str = "__crabka_grpc_dedup";
const GROUP: &str = "__crabka_grpc_gateway_dedup_owners";

async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ownership_split_non_owner_is_unavailable() {
    let (broker, bootstrap, _dir) = boot().await;
    ensure_dedup_topic(
        &bootstrap,
        DEDUP,
        N,
        hours(1),
        &crabka_grpc_gateway::dedup::topic::InternalTopicPolicy {
            replication_factor: 1,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: "own-user".into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::new(),
            }],
            crabka_units::secs(10),
        )
        .await
        .unwrap();

    let store_a = Arc::new(DedupStore::new(N));
    let store_b = Arc::new(DedupStore::new(N));
    let token = CancellationToken::new();
    let ha = tokio::spawn(store_a.clone().run_ownership(
        bootstrap.clone(),
        "gw-a".into(),
        DEDUP.into(),
        GROUP.into(),
        token.clone(),
        None,
    ));
    let hb = tokio::spawn(store_b.clone().run_ownership(
        bootstrap.clone(),
        "gw-b".into(),
        DEDUP.into(),
        GROUP.into(),
        token.clone(),
        None,
    ));

    // Wait for a stable, disjoint split covering all N partitions, both warm.
    let mut split_ok = false;
    for _ in 0..120 {
        let a: Vec<u32> = (0..N).filter(|p| store_a.owns(*p)).collect();
        let b: Vec<u32> = (0..N).filter(|p| store_b.owns(*p)).collect();
        let disjoint = a.iter().all(|p| !b.contains(p));
        let covers = u32::try_from(a.len() + b.len()) == Ok(N);
        if !a.is_empty()
            && !b.is_empty()
            && disjoint
            && covers
            && store_a.has_warmed_once()
            && store_b.has_warmed_once()
        {
            split_ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert2::assert!(split_ok);

    // Find a key owned by A (not B).
    let key = (0..1000)
        .map(|i| format!("k{i}"))
        .find(|k| store_a.owns(partition_for(k, N)))
        .expect("a key owned by A");
    let p = partition_for(&key, N);
    assert2::assert!(store_a.owns(p) && !store_b.owns(p));

    let engine_a = Arc::new(DedupEngine::new(
        &bootstrap,
        "gw-a",
        "crabka-grpc-dedup-a",
        DEDUP.into(),
        N,
        store_a.clone(),
        None,
    ));
    let engine_b = Arc::new(DedupEngine::new(
        &bootstrap,
        "gw-b",
        "crabka-grpc-dedup-b",
        DEDUP.into(),
        N,
        store_b.clone(),
        None,
    ));
    let core_a = ProduceCore::new(&bootstrap, "gw-a", Arc::new(RawCodec), None)
        .await
        .unwrap()
        .with_dedup(engine_a);
    let core_b = ProduceCore::new(&bootstrap, "gw-b", Arc::new(RawCodec), None)
        .await
        .unwrap()
        .with_dedup(engine_b);

    let mk = || GatewayRecord {
        topic: "own-user".into(),
        key: None,
        value: Bytes::from_static(b"v"),
        body_structured: None,
        headers: vec![],
        partition: None,
        timestamp_ms: None,
        idempotency_key: Some(key.clone()),
    };

    let anon = crabka_security::Principal {
        name: "ANONYMOUS".into(),
        auth_method: crabka_security::AuthMethod::Anonymous,
        groups: vec![],
    };

    // Non-owner B refuses with a retriable Unavailable.
    let err = core_b.produce(mk(), &anon).await.unwrap_err();
    assert2::assert!(matches!(err, GatewayError::Unavailable));

    // Owner A produces it (deduplicated=false the first time).
    let ok = core_a.produce(mk(), &anon).await.unwrap();
    assert2::assert!(!ok.deduplicated);

    token.cancel();
    let _ = ha.await;
    let _ = hb.await;
    broker.shutdown().await;
}
