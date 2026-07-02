//! Integration test: replicator restart *resumes* from the durable checkpoint
//! (at-least-once), rather than re-reading the source topic from offset 0.
//!
//! Proves that when a `FlowSupervisor` is shut down and a new one is started
//! with the same flow configuration (and therefore the same consumer group +
//! checkpoint key), it seeks to the committed position and delivers every record
//! produced during both runs to the target — no gap across the restart — while
//! NOT re-delivering the whole pre-crash batch. The position is restored via
//! `SourceConsumer::seek` (the loaded `SourceOffset` → `Consumer::seek`), so the
//! target count after a 10-then-restart-then-10 sequence is close to 20, not ~30.

mod common;

use assert2::check;
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
    let duplicates = total.saturating_sub(20);
    println!("target record count after restart: {total}  (resume target: ~20, ceiling: <30)");
    println!("distinct keys on target: {}", keys.len());
    println!("duplicate (re-delivered) records: {duplicates}");

    // ── Step 7: assertions ────────────────────────────────────────────────────

    // (a) No gap: every one of the 20 distinct keys made it across the restart.
    assert!(
        keys.len() == 20,
        "expected 20 distinct keys on target after restart, got {} — data gap",
        keys.len()
    );
    for i in 0..20u32 {
        let key = format!("k{i}").into_bytes();
        assert!(
            keys.contains(&key),
            "key k{i} is MISSING from target after restart — data gap detected"
        );
    }

    // (b) RESUMED, not re-read from 0. A full re-read would deliver the first
    //     batch (k0..k9) twice → total ~30. A true resume re-reads at most the
    //     in-flight batch at shutdown, so the count stays close to 20 and well
    //     under 30. We allow a small at-least-once boundary re-delivery (a few
    //     records), but not a wholesale reprocess of the 10 pre-crash records.
    check!(
        total >= 20,
        "expected at least 20 records on target after restart, got {total}"
    );
    check!(
        total < 30,
        "target re-read the whole pre-crash batch (total {total} >= 30) — \
         restart did NOT resume from the checkpoint"
    );
    // Bound the duplicates tightly: at-least-once permits the in-flight batch to
    // re-deliver, not ~10 records. The runtime commits in 500ms intervals over a
    // 10-record source, so at most a handful straddle the shutdown boundary.
    check!(
        duplicates <= 5,
        "too many duplicates after restart ({duplicates}) — expected a small \
         boundary re-delivery, not a full re-read of the first batch"
    );

    // ── Step 8: clean shutdown ────────────────────────────────────────────────
    sup2.shutdown().await;
}
