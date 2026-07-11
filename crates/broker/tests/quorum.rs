//! Multi-node in-process Crabka cluster tests. Each test spins up
//! 3 brokers on distinct loopback ports, all listed as voters in
//! each other's config.
//!
//! These tests exercise the metadata quorum end-to-end: leader
//! election, log replication, follower-forwarding via `submit_change`,
//! and openraft's per-leader `client_write` serialization.
//!
//! Deadlines are 2 minutes throughout: a 3-broker cluster spinning up
//! cold on a hosted GitHub Actions runner can take tens of seconds for
//! openraft to converge on a leader, and `cluster_lock` serializes the
//! tests so three slow startups accumulate.

// Test-file pragmatism: broker ids are formed with plain
// `u64::try_from(i+1).unwrap()`-style casts when turning 1-based `i` into broker
// ids, and topics are built with `..Default::default()`. Hoisting these into
// named helpers would obscure the per-test narrative.
#![allow(clippy::cast_possible_truncation, clippy::default_trait_access)]

use std::{sync::OnceLock, time::Duration};

use crabka_broker::{BrokerConfig, BrokerHandle};
use tokio::sync::Mutex;

mod support;

/// Test-binary-wide serialization. Each test in this file spins up a
/// 3-broker cluster on loopback; running them concurrently exhausts
/// loopback ephemeral ports and starves the openraft election timing.
/// Acquire this lock at the top of every `#[tokio::test]` so the binary
/// is effectively single-threaded for these scenarios, regardless of
/// whether the caller serializes execution through nextest test groups.
///
/// `tokio::sync::Mutex` rather than `std::sync::Mutex` so the lock can
/// be held across the `.await` calls that fill each test body without
/// tripping clippy's `await_holding_lock`.
fn cluster_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}
use crabka_client_core::Client;
use crabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    metadata_request::MetadataRequest,
};
use tempfile::TempDir;

/// Await every broker reporting an elected (non-zero) controller leader.
/// Event-driven (each handle awaits its leader watch channel); stricter than the
/// old "any one node" poll and exactly the precondition the callers need before
/// issuing client requests to a specific node.
async fn wait_for_leader(cluster: &[(BrokerHandle, BrokerConfig, TempDir)]) {
    for (h, _, _) in cluster {
        h.wait_until_controller_leader().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_elects_leader() {
    let _g = cluster_lock().lock().await;
    let cluster = support::start_n_node_with_retry(3).await;
    // Each node's controller leader channel converges to the same elected id.
    for (h, _, _) in &cluster {
        h.wait_until_controller_leader().await;
    }
    let mut leaders = std::collections::HashSet::new();
    for (h, _, _) in &cluster {
        if let Some(l) = h.controller_leader_id().await {
            leaders.insert(l);
        }
    }
    assert2::assert!(leaders.len() == 1 && !leaders.contains(&crabka_broker::NodeId(0)));
    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_topic_on_any_node_propagates() {
    let _g = cluster_lock().lock().await;
    let cluster = support::start_n_node_with_retry(3).await;
    wait_for_leader(&cluster).await;

    // CreateTopics against node 0.
    let c = Client::builder()
        .bootstrap(cluster[0].1.listen_addr.to_string())
        .build()
        .await
        .unwrap();
    let resp = c
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "prop".into(),
                num_partitions: 3,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert2::assert!(resp.topics[0].error_code == 0);

    // Metadata against node 2 should see it within 1s.
    let c2 = Client::builder()
        .bootstrap(cluster[2].1.listen_addr.to_string())
        .build()
        .await
        .unwrap();
    // Await the topic in node 2's controller image (deterministic), then the
    // client metadata reflects it immediately.
    cluster[2].0.wait_until_partition_present("prop", 0).await;
    let m = c2.send(MetadataRequest::default()).await.unwrap();
    assert2::assert!(m.topics.iter().any(|t| t.name.as_deref() == Some("prop")));

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_kill_recovers() {
    let _g = cluster_lock().lock().await;
    let mut cluster = support::start_n_node_with_retry(3).await;
    wait_for_leader(&cluster).await;

    // Find a broker that thinks it is the leader.
    let mut leader_idx = None;
    for (i, (h, cfg, _)) in cluster.iter().enumerate() {
        if h.controller_leader_id().await == Some(cfg.node_id) {
            leader_idx = Some(i);
            break;
        }
    }
    let leader_idx = leader_idx.expect("at least one broker self-identifies as leader");

    // Kill the leader.
    let (leader, leader_cfg, _dir) = cluster.remove(leader_idx);
    let killed_node_id = leader_cfg.node_id;
    leader.shutdown().await;

    // Survivors elect a new leader (id != killed). Await each survivor's leader
    // channel, then assert convergence to a single new leader.
    for (h, _, _) in &cluster {
        let mut rx = h.watch_leader_for_test();
        tokio::time::timeout(
            Duration::from_secs(30),
            rx.wait_for(|l| matches!(l, Some(id) if *id != crabka_broker::NodeId(0) && *id != killed_node_id)),
        )
        .await
        .expect("no new leader within 30s after kill")
        .expect("leader channel closed");
    }
    let mut leaders = std::collections::HashSet::new();
    for (h, _, _) in &cluster {
        if let Some(l) = h.controller_leader_id().await {
            leaders.insert(l);
        }
    }
    assert2::assert!(
        leaders.len() == 1
            && !leaders.contains(&crabka_broker::NodeId(0))
            && !leaders.contains(&killed_node_id)
    );

    // CreateTopics against a survivor succeeds.
    let c = Client::builder()
        .bootstrap(cluster[0].1.listen_addr.to_string())
        .build()
        .await
        .unwrap();
    let resp = c
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "post-kill".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert2::assert!(resp.topics[0].error_code == 0);

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_forwards_create_topic() {
    let _g = cluster_lock().lock().await;
    let cluster = support::start_n_node_with_retry(3).await;
    wait_for_leader(&cluster).await;

    // Identify a follower (any broker whose self-view of the leader != its own node_id).
    let mut follower_idx = None;
    for (i, (h, cfg, _)) in cluster.iter().enumerate() {
        if h.controller_leader_id().await != Some(cfg.node_id) {
            follower_idx = Some(i);
            break;
        }
    }
    let follower_idx = follower_idx.expect("at least one follower");

    let c = Client::builder()
        .bootstrap(cluster[follower_idx].1.listen_addr.to_string())
        .build()
        .await
        .unwrap();
    let resp = c
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "via-follower".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert2::assert!(resp.topics[0].error_code == 0);

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}

// macOS-gated for the same reason quorum.rs is windows-gated at the
// module level: the hosted macos-latest runner's task scheduler
// reorders openraft's internal callbacks under the short raft timings,
// tripping a
// `debug_assert!(Some(log_id) <= self.committed())` in
// `openraft::raft_state::log_state_reader`. Linux never hits it; the
// other 4 tests in this file also never hit it on macOS. Gating only
// this test keeps macOS coverage on leader election, follower
// forwarding, and kill/recover.
#[cfg(not(target_os = "macos"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_topic_creates_one_wins() {
    let _g = cluster_lock().lock().await;
    let cluster = support::start_n_node_with_retry(3).await;
    wait_for_leader(&cluster).await;
    for (h, _, _) in &cluster {
        h.wait_until_brokers_registered(3).await;
    }

    let clients = {
        let mut v = Vec::new();
        for (_, cfg, _) in &cluster {
            v.push(
                Client::builder()
                    .bootstrap(cfg.listen_addr.to_string())
                    .build()
                    .await
                    .unwrap(),
            );
        }
        v
    };

    let mut joins = Vec::new();
    for c in clients {
        joins.push(tokio::spawn(async move {
            c.send(CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: "race".into(),
                    num_partitions: 1,
                    replication_factor: 1,
                    ..Default::default()
                }],
                timeout_ms: 5_000,
                ..Default::default()
            })
            .await
            .unwrap()
        }));
    }
    let mut zero = 0;
    let mut already = 0;
    for j in joins {
        let resp = j.await.unwrap();
        match resp.topics[0].error_code {
            0 => zero += 1,
            36 /* TOPIC_ALREADY_EXISTS */ => already += 1,
            other => panic!("unexpected error_code {other}"),
        }
    }
    for (h, _, _) in &cluster {
        h.wait_until_partition_present("race", 0).await;
    }
    assert2::assert!(zero == 1);
    assert2::assert!(already == 2);

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}
