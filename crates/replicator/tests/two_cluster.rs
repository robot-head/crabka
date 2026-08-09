//! Two-cluster integration tests for `crabka-replicator`.
//!
//! Test 1: selective replication, remote-topic naming, residency zero-bytes.
//! Test 2: active/active loop prevention.

mod common;

use std::collections::BTreeMap;

use crabka_replicator::{
    config::{
        ClusterConfig, Delivery, FlowConfig, NamingPolicy, PolicyConfig, ReplicatorConfig,
        Residency, Selectors,
    },
    supervisor::FlowSupervisor,
};
use crabka_units::prelude::{TimeExt as _, secs};

// ---------------------------------------------------------------------------
// Test 1 — selective replication, remote-topic naming, residency
// ---------------------------------------------------------------------------

/// Verifies that:
/// - Topics matching the selector are replicated to the target with the
///   `<source-alias>.<topic>` naming convention.
/// - Topics excluded by a glob pattern (`*.internal`) are not replicated.
/// - Topics blocked by a residency policy produce zero bytes on the target. The
///   policy allows `customers` only in `gdpr` zones, and the target has no
///   `gdpr` zone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn selective_replication_naming_and_residency() {
    // --- infrastructure ---------------------------------------------------
    let source = common::start_broker().await;
    let target = common::start_broker().await;

    // Create topics on the source cluster.
    common::create_topic(&source.bootstrap, "orders", 1).await;
    common::create_topic(&source.bootstrap, "payments", 1).await;
    common::create_topic(&source.bootstrap, "audit.internal", 1).await;
    common::create_topic(&source.bootstrap, "customers", 1).await;

    // Produce records so every topic has content to replicate.
    common::produce(&source.bootstrap, "orders", b"o1", b"v1").await;
    common::produce(&source.bootstrap, "orders", b"o2", b"v2").await;
    common::produce(&source.bootstrap, "orders", b"o3", b"v3").await;

    common::produce(&source.bootstrap, "payments", b"p1", b"v1").await;
    common::produce(&source.bootstrap, "payments", b"p2", b"v2").await;

    common::produce(&source.bootstrap, "audit.internal", b"a1", b"v1").await;
    common::produce(&source.bootstrap, "audit.internal", b"a2", b"v2").await;

    common::produce(&source.bootstrap, "customers", b"c1", b"v1").await;
    common::produce(&source.bootstrap, "customers", b"c2", b"v2").await;

    // --- config -----------------------------------------------------------
    // `eu-west` intentionally has zones ["eu"] (no "gdpr") to demonstrate
    // that the residency policy blocks `customers` from flowing there.
    let mut clusters = BTreeMap::new();
    clusters.insert(
        "us-east".to_string(),
        ClusterConfig {
            bootstrap: source.bootstrap.clone(),
            region: "us".to_string(),
            zones: vec!["us".to_string()],
        },
    );
    clusters.insert(
        "eu-west".to_string(),
        ClusterConfig {
            bootstrap: target.bootstrap.clone(),
            region: "eu".to_string(),
            zones: vec!["eu".to_string()],
        },
    );

    let config = ReplicatorConfig {
        clusters,
        flows: vec![FlowConfig {
            from: "us-east".to_string(),
            to: "eu-west".to_string(),
            topics: Selectors {
                // Include the three user topics; exclude anything matching *.internal.
                include: vec![
                    "orders".to_string(),
                    "payments".to_string(),
                    "customers".to_string(),
                ],
                exclude: vec!["*.internal".to_string()],
            },
            groups: Selectors::default(),
            naming: NamingPolicy::Default,
            delivery: Delivery::AtLeastOnce,
        }],
        policies: vec![PolicyConfig {
            name: "keep-pii-in-eu".to_string(),
            topics: vec!["customers".to_string()],
            residency: Some(Residency {
                // `customers` may only land on clusters that have the `gdpr` zone.
                // `eu-west` has zones ["eu"] — no gdpr — so the gate blocks it.
                allow_zones: vec!["gdpr".to_string()],
                deny_zones: vec![],
            }),
        }],
    };

    // --- run supervisor ---------------------------------------------------
    let sup = FlowSupervisor::run(config).await.unwrap();

    // Wait for replicated topics to appear on the target.
    common::await_count(&target.bootstrap, "us-east.orders", 3, secs(20)).await;
    common::await_count(&target.bootstrap, "us-east.payments", 2, secs(20)).await;

    // Give time for any wrongly replicated records to arrive.
    // real-time wait (not a progress poll): settle-then-assert-absence; proves excluded and
    // residency-blocked topics received zero records, so the wait can't be a progress poll.
    tokio::time::sleep(secs(2).to_std()).await;

    // --- assertions -------------------------------------------------------

    // Selective replication + naming: expected counts on target.
    assert2::assert!(common::count(&target.bootstrap, "us-east.orders").await == 3);
    assert2::assert!(common::count(&target.bootstrap, "us-east.payments").await == 2);

    // Excluded by selector glob `*.internal`.
    // Note: `audit.internal` contains a `.` so the supervisor's `is_remote`
    // heuristic also treats it as a remote topic — either guard suffices to
    // prevent it from appearing in the topic list sent to the worker.
    assert2::assert!(common::count(&target.bootstrap, "us-east.audit.internal").await == 0);

    // Blocked by residency policy: `customers` requires `gdpr` zone; target has none.
    assert2::assert!(common::count(&target.bootstrap, "us-east.customers").await == 0);

    sup.shutdown().await;
}

// ---------------------------------------------------------------------------
// Test 2 — active/active loop prevention
// ---------------------------------------------------------------------------

/// Verifies that two symmetric flows between clusters `ca` and `cb` never
/// replicate a topic back to its origin.
///
/// A topic that replicates from `ca` to `cb` appears there as `ca.orders`. The
/// test verifies that it does NOT replicate back to `ca`, which would create
/// `cb.ca.orders`.
///
/// The topic discovery of the supervisor prevents the loop. Under the `Default`
/// naming policy, `Renamer::is_remote` excludes any topic whose name contains
/// `.`. `ca.orders` on broker `b` contains a `.`, so the supervisor never
/// subscribes to it for the `cb → ca` flow.
///
/// Setup: `cb` needs at least one non-remote topic so the `cb → ca` flow can
/// build a valid consumer. The test uses a `pings` topic on `b` for this
/// purpose. After replication starts, `ca.orders` appears on `b`, but the
/// `cb → ca` topic list excludes it, so it never propagates back to `a`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn active_active_loop_prevention() {
    // --- infrastructure ---------------------------------------------------
    let a = common::start_broker().await;
    let b = common::start_broker().await;

    // `ca` (broker a): the topic that should replicate one-way to `cb`.
    common::create_topic(&a.bootstrap, "orders", 1).await;
    common::produce(&a.bootstrap, "orders", b"k1", b"v1").await;
    common::produce(&a.bootstrap, "orders", b"k2", b"v2").await;
    common::produce(&a.bootstrap, "orders", b"k3", b"v3").await;

    // `cb` (broker b): a non-remote (no `.`) topic so the `cb → ca` flow has
    // something to subscribe to at supervisor-start time. This is not related
    // to the loop-prevention assertion — it just keeps the consumer non-empty.
    common::create_topic(&b.bootstrap, "pings", 1).await;
    common::produce(&b.bootstrap, "pings", b"p1", b"v1").await;

    // --- config -----------------------------------------------------------
    let mut clusters = BTreeMap::new();
    clusters.insert(
        "ca".to_string(),
        ClusterConfig {
            bootstrap: a.bootstrap.clone(),
            region: "us".to_string(),
            zones: vec!["us".to_string()],
        },
    );
    clusters.insert(
        "cb".to_string(),
        ClusterConfig {
            bootstrap: b.bootstrap.clone(),
            region: "eu".to_string(),
            zones: vec!["eu".to_string()],
        },
    );

    let wildcard_sel = Selectors {
        include: vec!["*".to_string()],
        exclude: vec![],
    };

    let config = ReplicatorConfig {
        clusters,
        flows: vec![
            // Flow 1: ca → cb  (replicates `orders` to b as `ca.orders`).
            FlowConfig {
                from: "ca".to_string(),
                to: "cb".to_string(),
                topics: wildcard_sel.clone(),
                groups: Selectors::default(),
                naming: NamingPolicy::Default,
                delivery: Delivery::AtLeastOnce,
            },
            // Flow 2: cb → ca  (would replicate `ca.orders` back to a as
            // `cb.ca.orders` if loop prevention were absent — but the
            // supervisor excludes `ca.orders` because it contains a `.`).
            FlowConfig {
                from: "cb".to_string(),
                to: "ca".to_string(),
                topics: wildcard_sel,
                groups: Selectors::default(),
                naming: NamingPolicy::Default,
                delivery: Delivery::AtLeastOnce,
            },
        ],
        policies: vec![],
    };

    // --- run supervisor ---------------------------------------------------
    let sup = FlowSupervisor::run(config).await.unwrap();

    // Wait until `orders` from `ca` is visible on `cb` as `ca.orders`.
    common::await_count(&b.bootstrap, "ca.orders", 3, secs(20)).await;

    // Also wait for `pings` from `cb` to appear on `ca` as `cb.pings` — this
    // proves the cb → ca flow is actively running, so any loop would have shown
    // up by now.
    common::await_count(&a.bootstrap, "cb.pings", 1, secs(20)).await;

    // Allow several supervision cycles for any additional loop bounce.
    // real-time wait (not a progress poll): settle-then-assert-absence; proves the looped
    // `cb.ca.orders` never appears, so the wait can't be a progress poll.
    tokio::time::sleep(secs(3).to_std()).await;

    // --- assertions -------------------------------------------------------

    // The looped topic must NOT exist on `a`.
    assert2::assert!(common::count(&a.bootstrap, "cb.ca.orders").await == 0);

    // The original topic on `a` should be unchanged.
    assert2::assert!(common::count(&a.bootstrap, "orders").await == 3);

    sup.shutdown().await;
}
