//! Integration test: replicator restart resumes with no data gap (at-least-once).
//!
//! Proves that when a `FlowSupervisor` is shut down and a new one is started
//! with the same flow configuration (and therefore the same consumer group +
//! checkpoint key), it picks up from the committed offset and delivers every
//! record produced during both runs to the target — no gap across the restart.

mod common;

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use crabka_replicator::config::{
    ClusterConfig, Delivery, FlowConfig, NamingPolicy, ReplicatorConfig, Selectors,
};
use crabka_replicator::supervisor::FlowSupervisor;

/// Build a `ReplicatorConfig` for the us-east → eu-west flow that replicates
/// the `orders` topic.  Both brokers must already be running when this is
/// called; the config is a cheap value type so we just construct it twice.
fn make_config(source_bootstrap: &str, target_bootstrap: &str) -> ReplicatorConfig {
    let mut clusters = BTreeMap::new();
    clusters.insert(
        "us-east".to_string(),
        ClusterConfig {
            bootstrap: source_bootstrap.to_string(),
            region: "us".into(),
            zones: vec!["us".into()],
        },
    );
    clusters.insert(
        "eu-west".to_string(),
        ClusterConfig {
            bootstrap: target_bootstrap.to_string(),
            region: "eu".into(),
            zones: vec!["eu".into()],
        },
    );
    ReplicatorConfig {
        clusters,
        flows: vec![FlowConfig {
            from: "us-east".into(),
            to: "eu-west".into(),
            topics: Selectors {
                include: vec!["orders".into()],
                exclude: vec![],
            },
            groups: Selectors::default(),
            naming: NamingPolicy::Default,
            delivery: Delivery::AtLeastOnce,
        }],
        policies: vec![],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_resumes_with_no_gap() {
    // ── Step 1: start both brokers (keep handles alive for the whole test) ────
    let source = common::start_broker().await;
    let target = common::start_broker().await;

    // Create `orders` on the source cluster.
    common::create_topic(&source.bootstrap, "orders", 1).await;

    // Produce first batch: k0 .. k9
    for i in 0..10u32 {
        let key = format!("k{i}");
        common::produce(&source.bootstrap, "orders", key.as_bytes(), b"value").await;
    }

    // ── Step 2 & 3: first supervisor run ──────────────────────────────────────
    let config_1 = make_config(&source.bootstrap, &target.bootstrap);
    let sup = FlowSupervisor::run(config_1)
        .await
        .expect("first supervisor run");

    // Wait until all 10 initial records arrive on the target.
    common::await_count(
        &target.bootstrap,
        "us-east.orders",
        10,
        Duration::from_secs(30),
    )
    .await;

    // Gracefully stop the replicator.
    sup.shutdown().await;

    // ── Step 4: produce second batch while supervisor is down ─────────────────
    for i in 10..20u32 {
        let key = format!("k{i}");
        common::produce(&source.bootstrap, "orders", key.as_bytes(), b"value").await;
    }

    // ── Step 5: second supervisor run (same flow name → same consumer group) ──
    let config_2 = make_config(&source.bootstrap, &target.bootstrap);
    let sup2 = FlowSupervisor::run(config_2)
        .await
        .expect("second supervisor run");

    // Wait until at least 20 records have arrived (dups are fine; at-least-once).
    common::await_count(
        &target.bootstrap,
        "us-east.orders",
        20,
        Duration::from_secs(30),
    )
    .await;

    // ── Step 6: collect the full key set from the target ─────────────────────
    let recs = crabka_replicator::admin_util::read_all(&target.bootstrap, "us-east.orders", None)
        .await
        .expect("read_all");

    let keys: HashSet<Vec<u8>> = recs
        .into_iter()
        .filter_map(|(k, _)| k.map(|b| b.to_vec()))
        .collect();

    let total = common::count(&target.bootstrap, "us-east.orders").await;
    println!("target record count after restart: {total}  (>= 20 required)");
    println!("distinct keys on target: {}", keys.len());

    // ── Step 7: assertions ────────────────────────────────────────────────────

    // Every record must have made it across the restart (dups allowed).
    assert!(
        total >= 20,
        "expected at least 20 records on target after restart, got {total}"
    );

    // All 20 distinct keys must be present — no gap.
    for i in 0..20u32 {
        let key = format!("k{i}").into_bytes();
        assert!(
            keys.contains(&key),
            "key k{i} is MISSING from target after restart — data gap detected"
        );
    }

    if total > 20 {
        println!(
            "note: {extra} duplicate record(s) observed — acceptable under at-least-once",
            extra = total - 20
        );
    }

    // ── Step 8: clean shutdown ────────────────────────────────────────────────
    sup2.shutdown().await;
}
