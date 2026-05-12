//! Multi-node in-process tests for slice-8 basic replication. Gated
//! `#[cfg(not(target_os = "windows"))]` to mirror `quorum.rs` (openraft
//! `debug_assert!` race on the hosted Windows runner).
//!
//! Spins up a 3-broker cluster on loopback, creates a topic with
//! replication-factor 3, produces records to the leader, and asserts
//! every follower's local log converges to the leader's
//! `log_end_offset`. Exercises the full slice-8 replication path:
//! supervisor reconcile, follower Fetch loop, and
//! `Partition::replicate_batch`.
//!
//! Deadlines are 2 minutes throughout — same reasoning as `quorum.rs`:
//! a cold 3-broker cluster on a hosted CI runner can take tens of
//! seconds for openraft to converge, and `cluster_lock` serializes the
//! tests so slow startups accumulate.

#![cfg(not(target_os = "windows"))]
// Test-file pragmatism: deadlines are expressed as
// `if Instant::now() > … { panic!(…) }` for readability, and casts
// turn 1-based `i` into broker ids. Hoisting these into named helpers
// would obscure the per-test narrative.
#![allow(
    clippy::manual_assert,
    clippy::cast_possible_truncation,
    clippy::default_trait_access,
    // The full propagation test reads top-to-bottom as one scenario
    // (bring up cluster → wait for brokers → CreateTopics → wait for
    // propagation → produce → wait for convergence). Splitting it into
    // helpers obscures the per-stage `deadline`/poll pattern without
    // making any individual piece reusable.
    clippy::too_many_lines
)]

use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::Client;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::records::{Record, RecordBatch};
use tempfile::TempDir;
use tokio::sync::Mutex;

/// Test-binary-wide serialization. Each test in this file spins up a
/// 3-broker cluster on loopback; running them concurrently exhausts
/// loopback ephemeral ports and starves the openraft election timing.
/// Same rationale as `quorum.rs::cluster_lock`.
fn cluster_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

/// Copied verbatim from `crates/broker/tests/quorum.rs`. See the
/// docstring there for the full rationale; the short version is
/// "bind-and-drop to capture stable loopback ports, then spawn brokers
/// in parallel". A future refactor can hoist this and
/// [`start_n_node_with_retry`] into a shared test-support module.
async fn start_n_node(
    n: u64,
) -> Result<Vec<(BrokerHandle, BrokerConfig, TempDir)>, crabka_broker::BrokerError> {
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
        spawned.push(tokio::spawn(async move { Broker::start(cfg_clone).await }));
        metas.push((dir, cfg));
    }

    let mut out = Vec::with_capacity(n as usize);
    for (j, (dir, cfg)) in spawned.into_iter().zip(metas) {
        let h = j.await.expect("broker spawn join")?;
        out.push((h, cfg, dir));
    }
    Ok(out)
}

/// Retry `start_n_node` up to 3 times. Copied verbatim from
/// `crates/broker/tests/quorum.rs`; the hand-rolled wire occasionally
/// split-votes on slow CI runners, and a fresh tempdir / port set on
/// retry clears the openraft state.
async fn start_n_node_with_retry(n: u64) -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    let mut last_err = None;
    for attempt in 1..=3 {
        match start_n_node(n).await {
            Ok(cluster) => return cluster,
            Err(e) => {
                tracing::warn!(attempt, error = %e, "cluster start failed; retrying");
                last_err = Some(e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    panic!("cluster start failed after 3 attempts; last error: {last_err:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_factor_three_propagates_to_all_followers() {
    let _g = cluster_lock().lock().await;
    let cluster = start_n_node_with_retry(3).await;

    // Wait for all 3 brokers to register in each other's MetadataImage.
    // Iterate sequentially with `.await` (no `block_on`).
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let mut all_see_three = true;
        for (h, _, _) in &cluster {
            if h.broker_count().await < 3 {
                all_see_three = false;
                break;
            }
        }
        if all_see_three {
            break;
        }
        if Instant::now() > deadline {
            panic!("brokers didn't converge on 3-broker view within 2 min");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // `start_n_node_with_retry` binds brokers in order, so cluster[0]
    // is node 1; with rf=3 / partition_index=0 the round-robin placement
    // chooses node 1 as the partition leader. We use it as the
    // CreateTopics + Produce target.
    let leader_addr = cluster[0].1.listen_addr.to_string();

    // CreateTopics("repl", num_partitions=1, replication_factor=3).
    let admin = Client::builder()
        .bootstrap(leader_addr.clone())
        .build()
        .await
        .unwrap();
    let resp = admin
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "repl".into(),
                num_partitions: 1,
                replication_factor: 3,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(resp.topics[0].error_code, 0);
    // ProduceRequest v13 wire format drops `topic.name` in favour of
    // `topic.topic_id` (KIP-516). The client negotiates the broker's
    // max supported version (v13), so we must echo the CreateTopics-
    // assigned topic_id on the produce path, otherwise the broker's
    // image lookup returns an empty topic name and the partition lookup
    // fails with UNKNOWN_TOPIC_OR_PARTITION.
    let topic_id = resp.topics[0].topic_id;

    // Wait for the topic to propagate to every broker's MetadataImage.
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let mut all_have = true;
        for (h, _, _) in &cluster {
            if !h.has_partition("repl", 0).await {
                all_have = false;
                break;
            }
        }
        if all_have {
            break;
        }
        if Instant::now() > deadline {
            panic!("topic 'repl' didn't propagate to all 3 brokers within 2 min");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Produce 20 records to the leader.
    let producer = Client::builder()
        .bootstrap(leader_addr)
        .build()
        .await
        .unwrap();
    let batch = RecordBatch {
        base_offset: 0,
        last_offset_delta: 19,
        records: (0..20)
            .map(|i| Record {
                offset_delta: i,
                value: Some(bytes::Bytes::from(format!("v{i}"))),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let prod = producer
        .send(ProduceRequest {
            acks: -1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "repl".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(batch),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(prod.responses[0].partition_responses[0].error_code, 0);

    // Wait until every broker's local log shows log_end_offset >= 20.
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let mut offsets = Vec::with_capacity(cluster.len());
        for (h, _, _) in &cluster {
            offsets.push(h.local_log_end_offset("repl", 0).await.unwrap_or(0));
        }
        if offsets.iter().all(|&n| n >= 20) {
            break;
        }
        if Instant::now() > deadline {
            panic!("not all 3 brokers caught up to 20 records within 2 min; saw: {offsets:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}
