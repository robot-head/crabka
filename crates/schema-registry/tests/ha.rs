#![cfg(not(target_os = "windows"))]

use std::time::Duration;

use crabka_broker::{Broker, BrokerConfig};
use crabka_schema_registry::config::RegistryConfig;
use crabka_schema_registry::election::{Election, PrimaryState};
use tokio_util::sync::CancellationToken;

fn cfg(bootstrap: &str, port: i32) -> RegistryConfig {
    RegistryConfig {
        bootstrap: bootstrap.into(),
        schemas_topic: "_schemas".into(),
        schemas_topic_rf: 1,
        client_id: format!("sr-{port}"),
        advertised_url: format!("http://127.0.0.1:{port}"),
        group_id: "schema-registry".into(),
        leader_eligibility: true,
    }
}

/// Wait until `pred(state)` holds or `secs` elapses; returns the matching state.
async fn await_state(
    rx: &mut tokio::sync::watch::Receiver<PrimaryState>,
    secs: u64,
    pred: impl Fn(&PrimaryState) -> bool,
) -> PrimaryState {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        if pred(&rx.borrow()) {
            return rx.borrow().clone();
        }
        tokio::select! {
            () = tokio::time::sleep_until(deadline) => panic!("state never matched: {:?}", *rx.borrow()),
            r = rx.changed() => { r.expect("election task alive"); }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_node_becomes_primary() {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let cancel = CancellationToken::new();
    let c = cfg(&broker.listen_addr().to_string(), 8081);
    let mut rx = Election::start(&c, cancel.clone()).await.unwrap();
    let st = await_state(&mut rx, 20, |s| s.is_primary).await;
    assert!(st.is_primary);
    assert_eq!(st.primary_url.as_deref(), Some("http://127.0.0.1:8081"));
    cancel.cancel();
    broker.shutdown().await;
}
