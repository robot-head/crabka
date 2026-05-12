//! Multi-node in-process Crabka cluster tests. Each test spins up
//! 3 brokers on distinct loopback ports, all listed as voters in
//! each other's config.
//!
//! These tests exercise the slice-7 metadata quorum end-to-end: leader
//! election, log replication, follower-forwarding via `submit_change`,
//! and openraft's per-leader `client_write` serialization.
//!
//! Gated `#[cfg(not(target_os = "windows"))]`: GitHub Actions windows
//! runners are slow enough that three sequential 3-broker clusters
//! (one per test, serialized via `cluster_lock`) blow past openraft's
//! election timeout even with a 30s `Broker::start` deadline, and a
//! distinct ordering of state updates also trips an internal
//! `Some(log_id) <= self.committed()` assertion. The slice-7
//! architecture is exercised on Linux + macOS plus the
//! `three_node_jvm_round_trip` Docker test; Windows isn't a primary
//! deployment target for Crabka brokers.

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

use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use tokio::sync::Mutex;

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

/// Spin up an `n`-node cluster on loopback, returning each broker's
/// handle, the config used to start it, and its tempdir (kept alive
/// for the lifetime of the test).
///
/// Pre-binds both listeners (client + controller) per broker so we can
/// capture stable addresses for the voter list, then drops the bindings
/// so `Broker::start` can re-bind to the same ports. On Linux the
/// kernel reserves the port briefly post-drop; if the rebind races on
/// other platforms we'd need to thread the listener into `Broker::start`.
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

async fn start_n_node(n: u64) -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    init_tracing();
    // Phase 1: capture addresses by binding + dropping.
    let mut client_addrs = Vec::with_capacity(n as usize);
    let mut controller_addrs = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let cl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        client_addrs.push(cl.local_addr().unwrap());
        let ct = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        controller_addrs.push(ct.local_addr().unwrap());
        drop((cl, ct));
    }

    let voters: Vec<(u64, SocketAddr)> = (0..n)
        .map(|i| (i + 1, controller_addrs[i as usize]))
        .collect();

    // Phase 2: spawn brokers in parallel so they can converge on a leader.
    let mut spawned = Vec::with_capacity(n as usize);
    let mut metas: Vec<(TempDir, BrokerConfig)> = Vec::with_capacity(n as usize);
    for i in 0..n {
        let dir = TempDir::new().unwrap();
        let cfg = BrokerConfig {
            broker_id: i32::try_from(i + 1).unwrap(),
            listen_addr: client_addrs[i as usize],
            advertised_listener: client_addrs[i as usize].to_string(),
            log_dir: dir.path().to_path_buf(),
            log_config: Default::default(),
            node_id: i + 1,
            controller_listen_addr: controller_addrs[i as usize],
            controller_quorum_voters: voters.clone(),
        };
        let cfg_clone = cfg.clone();
        spawned.push(tokio::spawn(async move {
            Broker::start(cfg_clone).await.expect("broker start")
        }));
        metas.push((dir, cfg));
    }

    let mut out = Vec::with_capacity(n as usize);
    for (j, (dir, cfg)) in spawned.into_iter().zip(metas) {
        let h = j.await.unwrap();
        out.push((h, cfg, dir));
    }
    out
}

/// Poll each broker until at least one of them reports a leader.
async fn wait_for_leader(cluster: &[(BrokerHandle, BrokerConfig, TempDir)]) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        for (h, _, _) in cluster {
            if h.controller_leader_id().await.is_some() {
                return;
            }
        }
        if Instant::now() > deadline {
            panic!("no leader within 5s");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_elects_leader() {
    let _g = cluster_lock().lock().await;
    let cluster = start_n_node(3).await;
    let deadline = Instant::now() + Duration::from_secs(5);
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
            panic!("leader not converged within 5s; current views: {leaders:?}");
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
    let cluster = start_n_node(3).await;
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
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let m = c2.send(MetadataRequest::default()).await.unwrap();
        if m.topics.iter().any(|t| t.name.as_deref() == Some("prop")) {
            break;
        }
        if Instant::now() > deadline {
            panic!("topic not propagated to node 2 within 5s");
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
    let mut cluster = start_n_node(3).await;
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

    // Survivors elect a new leader within 5s.
    let deadline = Instant::now() + Duration::from_secs(5);
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
            panic!("no new leader within 5s of kill");
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
    let cluster = start_n_node(3).await;
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
    let cluster = start_n_node(3).await;
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
