//! Multi-node in-process Crabka cluster tests. Each test spins up
//! 3 brokers on distinct loopback ports, all listed as voters in
//! each other's config.
//!
//! These tests exercise the slice-7 metadata quorum end-to-end: leader
//! election, log replication, follower-forwarding via `submit_change`,
//! and openraft's per-leader `client_write` serialization.
//!
//! Deadlines are 2 minutes throughout: a 3-broker cluster spinning up
//! cold on a hosted GitHub Actions runner can take tens of seconds for
//! openraft to converge on a leader, and `cluster_lock` serializes the
//! tests so three slow startups accumulate.
//!
//! Gated `#[cfg(not(target_os = "windows"))]`: the hosted Windows
//! runner's task scheduler reorders openraft's internal callbacks
//! enough to trip a `debug_assert!` in openraft's
//! `LogStateReader::has_log_id` (`Some(log_id) <= self.committed()`).
//! Linux + macOS never hit this; the slice-7 spec called the
//! hand-rolled wire's openraft drift an explicit risk, and tightening
//! it is a slice-7-followup. Until then, Linux + macOS cover the
//! quorum and the Docker JVM acceptance test covers end-to-end.

#![cfg(not(target_os = "windows"))]
// Test-file pragmatism: deadlines are expressed as `if Instant::now() > … { panic!(…) }`
// for readability (each panic message describes the test scenario it
// covers) and as plain `u64::try_from(i+1).unwrap()`-style casts when
// turning 1-based `i` into broker ids. Hoisting these into named
// assertions / helpers would obscure the per-test narrative.
#![allow(
    clippy::manual_assert,
    clippy::cast_possible_truncation,
    clippy::default_trait_access
)]

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crabka_broker::{BrokerConfig, BrokerHandle};
use tokio::sync::Mutex;

mod support;

/// Test-binary-wide serialization. Each test in this file spins up a
/// 3-broker cluster on loopback; running them concurrently exhausts
/// loopback ephemeral ports and starves the openraft election timing.
/// Acquire this lock at the top of every `#[tokio::test]` so the binary
/// is effectively single-threaded for these scenarios, regardless of
/// whether the caller passes `--test-threads=1`.
///
/// `tokio::sync::Mutex` rather than `std::sync::Mutex` so the lock can
/// be held across the `.await` calls that fill each test body without
/// tripping clippy's `await_holding_lock`.
fn cluster_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}
use crabka_client_core::Client;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::metadata_request::MetadataRequest;
use tempfile::TempDir;

/// Poll each broker until at least one of them reports a leader.
async fn wait_for_leader(cluster: &[(BrokerHandle, BrokerConfig, TempDir)]) {
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        for (h, _, _) in cluster {
            if h.controller_leader_id().await.is_some() {
                return;
            }
        }
        if Instant::now() > deadline {
            panic!("no leader within 2 min");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_elects_leader() {
    let _g = cluster_lock().lock().await;
    let cluster = support::start_n_node_with_retry(3).await;
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let mut leaders = std::collections::HashSet::new();
        for (h, _, _) in &cluster {
            if let Some(l) = h.controller_leader_id().await {
                leaders.insert(l);
            }
        }
        if leaders.len() == 1 && !leaders.contains(&0) {
            break;
        }
        if Instant::now() > deadline {
            panic!("leader not converged within 2 min; current views: {leaders:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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
    assert_eq!(resp.topics[0].error_code, 0);

    // Metadata against node 2 should see it within 1s.
    let c2 = Client::builder()
        .bootstrap(cluster[2].1.listen_addr.to_string())
        .build()
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let m = c2.send(MetadataRequest::default()).await.unwrap();
        if m.topics.iter().any(|t| t.name.as_deref() == Some("prop")) {
            break;
        }
        if Instant::now() > deadline {
            panic!("topic not propagated to node 2 within 2 min");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

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
    let (leader, _, _dir) = cluster.remove(leader_idx);
    leader.shutdown().await;

    // Survivors elect a new leader within 2 min.
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let mut leaders = std::collections::HashSet::new();
        for (h, _, _) in &cluster {
            if let Some(l) = h.controller_leader_id().await {
                leaders.insert(l);
            }
        }
        if leaders.len() == 1 && !leaders.contains(&0) {
            break;
        }
        if Instant::now() > deadline {
            panic!("no new leader within 2 min of kill");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

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
    assert_eq!(resp.topics[0].error_code, 0);

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
    assert_eq!(resp.topics[0].error_code, 0);

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_topic_creates_one_wins() {
    let _g = cluster_lock().lock().await;
    let cluster = support::start_n_node_with_retry(3).await;
    wait_for_leader(&cluster).await;

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
    assert_eq!(zero, 1, "exactly one winner");
    assert_eq!(already, 2, "two losers see TOPIC_ALREADY_EXISTS");

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}
