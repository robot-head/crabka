//! End-to-end round-trip against a real broker.
//!
//! Requires Docker; gated `#[ignore]` and CI runs with `--include-ignored`.

#![allow(clippy::pedantic)]

use std::{sync::Arc, time::Duration};

use crabka_broker::{Broker, BrokerConfig};
use crabka_client_admin::AdminClient;
use crabka_client_core::Client;
use crabka_rebalancer::{
    executor::state::{InFlightFile, Phase},
    state_topic::{LoadedState, StateBackend, StateTopic, StateTopicLoader, topic_admin},
};
use tokio_util::sync::CancellationToken;

/// Boot a single-broker in-process Crabka, return its bootstrap address.
/// The `BrokerHandle` and `TempDir` must be kept alive for the duration of
/// the test.
async fn boot_broker() -> (crabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn drive_loader_until_loaded(state: Arc<LoadedState>, timeout: Duration) {
    let start = std::time::Instant::now();
    while !state.is_loaded() {
        if start.elapsed() > timeout {
            panic!("loader did not converge within {timeout:?}");
        }
        tokio::task::yield_now().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker or in-process broker; run with --include-ignored in CI"]
async fn write_load_round_trip_via_real_broker() {
    let (_broker, bootstrap, _dir) = boot_broker().await;

    let client = Arc::new(
        Client::builder()
            .bootstrap(bootstrap.as_str())
            .client_id("state-topic-test")
            .build()
            .await
            .expect("connect"),
    );
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .expect("admin connect");

    let topic = format!("__test_state_topic_{}", uuid::Uuid::new_v4());
    topic_admin::ensure_topic(&mut admin, &topic, 1)
        .await
        .expect("create topic");

    // Round 1: write a record, then start a fresh loader, expect to see it.
    //
    // Production startup ordering: ensure_topic → spawn loader →
    // (executor writes later, gated on /readyz). The loader's first
    // Fetch RPCs nudge the broker into fully loading the topic's
    // partition into its data plane, so by the time the executor
    // writes, UNKNOWN_TOPIC_OR_PARTITION isn't a concern. We mirror
    // that ordering here: start a "warmup" loader (separate state)
    // before the first write, wait for it to settle, then write.
    let warmup_state = LoadedState::new();
    let warmup_shutdown = CancellationToken::new();
    let warmup_loader = StateTopicLoader {
        client: client.clone(),
        topic: topic.clone(),
        state: warmup_state.clone(),
        shutdown: warmup_shutdown.clone(),
    };
    let warmup_handle = tokio::spawn(warmup_loader.run());
    drive_loader_until_loaded(warmup_state.clone(), Duration::from_secs(10)).await;
    warmup_shutdown.cancel();
    warmup_handle.await.unwrap();

    let state = LoadedState::new();
    let st = StateTopic::new(client.clone(), topic.clone(), state.clone());
    let f = InFlightFile::new("p-1".into(), Phase::Wait, 1_111, 50_000_000);
    st.write(&f).await.expect("write");

    let shutdown = CancellationToken::new();
    let loader = StateTopicLoader {
        client: client.clone(),
        topic: topic.clone(),
        state: state.clone(),
        shutdown: shutdown.clone(),
    };
    let handle = tokio::spawn(loader.run());

    drive_loader_until_loaded(state.clone(), Duration::from_secs(10)).await;
    let loaded = state.current().expect("non-tombstone");
    assert2::assert!((loaded.proposal_id.as_str(), loaded.phase) == ("p-1", Phase::Wait));
    shutdown.cancel();
    handle.await.unwrap();

    // Round 2: tombstone, restart loader, expect None.
    let state2 = LoadedState::new();
    let st2 = StateTopic::new(client.clone(), topic.clone(), state2.clone());
    st2.delete().await.expect("delete");

    let shutdown2 = CancellationToken::new();
    let loader2 = StateTopicLoader {
        client: client.clone(),
        topic: topic.clone(),
        state: state2.clone(),
        shutdown: shutdown2.clone(),
    };
    let handle2 = tokio::spawn(loader2.run());

    drive_loader_until_loaded(state2.clone(), Duration::from_secs(10)).await;
    assert2::assert!(state2.current().is_none());
    shutdown2.cancel();
    handle2.await.unwrap();
}
