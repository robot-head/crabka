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
