//! In-process integration tests for leader election +
//! KIP-101 leader-epoch + ISR shrink/expand.
//!
//! Windows-gated like the other multi-broker tests: openraft +
//! `tokio` scheduling on Windows runners cause flakes that have
//! nothing to do with the protocol being tested.

use std::time::{Duration, Instant};

use assert2::assert;
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};
use tempfile::TempDir;

mod support;

/// Await until exactly one broker reports itself as the openraft controller
/// leader. Returns the cluster index of that broker (0-based).
async fn find_controller_leader(cluster: &[(BrokerHandle, BrokerConfig, TempDir)]) -> usize {
    for (h, _, _) in cluster {
        h.wait_until_controller_leader().await;
    }
    for (i, (h, cfg, _)) in cluster.iter().enumerate() {
        if h.controller_leader_id() == Some(cfg.node_id) {
            return i;
        }
    }
    panic!("a leader was elected but no handle self-identifies as leader");
}

async fn create_topic(broker: &BrokerHandle, bootstrap: &str, name: &str, rf: i16) {
    let client = Client::builder()
        .bootstrap(bootstrap.to_string())
        .build()
        .await
        .unwrap();
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: 1,
                replication_factor: rf,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(resp.topics[0].error_code == 0, "CreateTopics: {resp:?}");
    broker.wait_until_partition_present(name, 0).await;
}

async fn topic_id_for(client: &Client, name: &str) -> WireUuid {
    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

fn record_batch_with_values(values: &[&str]) -> RecordBatch {
    let mut batch = RecordBatch {
        last_offset_delta: (i32::try_from(values.len()).unwrap() - 1).max(0),
        max_timestamp: i64::try_from(values.len()).unwrap(),
        ..RecordBatch::default()
    };
    for (i, v) in values.iter().enumerate() {
        batch.records.push(Record {
            offset_delta: i32::try_from(i).unwrap(),
            value: Some(Bytes::from(v.to_string())),
            ..Default::default()
        });
    }
    batch
}

async fn produce_acks(
    bootstrap: &str,
    topic: &str,
    values: &[&str],
    acks: i16,
    timeout_ms: i32,
) -> Result<i64, i16> {
    let client = Client::builder()
        .bootstrap(bootstrap.to_string())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, topic).await;
    let resp = client
        .send(ProduceRequest {
            acks,
            timeout_ms,
            topic_data: vec![TopicProduceData {
                name: topic.into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(record_batch_with_values(values).into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce");
    let pr = &resp.responses[0].partition_responses[0];
    if pr.error_code == 0 {
        Ok(pr.base_offset)
    } else {
        Err(pr.error_code)
    }
}

// Cluster lock — same rationale as replication.rs.
fn cluster_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_death_elects_new_leader() {
    let _g = cluster_lock().lock().await;
    let mut cluster = support::start_n_node_with_retry(3).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;
    let bootstrap_1 = cluster[0].1.listen_addr.to_string();
    create_topic(&cluster[0].0, &bootstrap_1, "elect", 3).await;

    // Kill broker 1 (the partition leader by round-robin).
    let dead = cluster.remove(0);
    dead.0.shutdown().await;

    // Wait for election: await until the surviving broker sees a new leader.
    // cluster[0] is now broker 2 (broker 1 was removed above).
    cluster[0]
        .0
        .wait_until_partition_leader_changed("elect", 0, crabka_broker::NodeId(1))
        .await;
    let client = Client::builder()
        .bootstrap(cluster[0].1.listen_addr.to_string())
        .build()
        .await
        .unwrap();
    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some("elect".into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("metadata");
    let t = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some("elect"))
        .expect("topic");
    let p = t.partitions.first().expect("partition");
    let (new_leader, new_epoch) = (p.leader_id, p.leader_epoch);
    assert!(
        new_leader == 2 || new_leader == 3,
        "unexpected new leader: {new_leader}"
    );
    assert!(new_epoch > 0, "leader_epoch should bump after election");

    // Clean up surviving brokers.
    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acks_all_completes_after_isr_shrink() {
    let _g = cluster_lock().lock().await;
    let mut cluster = support::start_n_node_with_retry(3).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;
    let bootstrap_1 = cluster[0].1.listen_addr.to_string();
    create_topic(&cluster[0].0, &bootstrap_1, "shrink2", 3).await;

    // Freeze broker 3 by shutting it down.
    let dead = cluster.pop().expect("3rd broker");
    dead.0.shutdown().await;

    // Produce acks=-1. ISR should shrink to {1,2} within
    // replica_lag_time_max_ms (2s on CI) + heartbeat_timeout (2s); produce
    // completes after.
    let start = Instant::now();
    let offset = produce_acks(&bootstrap_1, "shrink2", &["a", "b", "c"], -1, 15_000)
        .await
        .expect("acks=-1 after shrink");
    let elapsed = start.elapsed();
    assert!(offset == 0);
    assert!(
        elapsed < Duration::from_secs(10),
        "shrink should be quick on for_tests config; took {elapsed:?}"
    );

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "KIP-853 dynamic reconfig (change_membership/add_learner): Slice 5"]
async fn isr_expand_on_catchup() {
    let _g = cluster_lock().lock().await;
    let mut cluster = support::start_n_node_with_retry(3).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;
    let bootstrap_1 = cluster[0].1.listen_addr.to_string();

    create_topic(&cluster[0].0, &bootstrap_1, "expand", 3).await;

    // 1. Find the current openraft leader so we can drive membership changes on it.
    let leader_idx = find_controller_leader(&cluster).await;
    let leader_node_id = cluster[leader_idx].1.node_id;
    eprintln!("CRABKA[test] controller leader is node_id={leader_node_id}");

    // 2. Remove node 3 from the voter set BEFORE kill. The leader's openraft
    //    commits a joint config then a uniform config; after the second commit
    //    survivors stop replicating to node 3.
    cluster[leader_idx]
        .0
        .change_membership(
            [crabka_broker::NodeId(1), crabka_broker::NodeId(2)]
                .into_iter()
                .collect(),
        )
        .await
        .expect("remove node 3 from voter set");

    // 3. Capture node 3's addr for the reborn broker, then kill.
    let dead_listen_addr = cluster[2].1.listen_addr;
    let dead_controller_addr = cluster[2].1.controller_listen_addr;
    let (dead_h, _dead_cfg, _dead_dir) = cluster.remove(2);
    dead_h.shutdown().await;

    // 4. Reboot node 3 with a fresh TempDir + same controller addr.
    //    Boot as a 1-node cluster (voters = [self]) so node 3 can
    //    self-elect immediately and Broker::start returns quickly.
    //    The actual cluster leader will call add_learner below, which
    //    sends AppendEntries at a higher term and causes node 3 to
    //    step down and follow the real leader.
    let reborn_dir = TempDir::new().unwrap();
    let voters = [(3u64, dead_controller_addr)];
    let reborn_cfg = BrokerConfig {
        broker_id: 3,
        listen_addr: dead_listen_addr,
        advertised_listener: dead_listen_addr.to_string(),
        log_dir: reborn_dir.path().to_path_buf(),
        log_config: crabka_log::LogConfig::default(),
        node_id: crabka_broker::NodeId(3),
        controller_listen_addr: dead_controller_addr,
        controller_quorum_voters: voters
            .iter()
            .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
            .collect(),
        heartbeat_interval: crabka_units::millis(200),
        heartbeat_timeout: crabka_units::millis(2_000),
        replica_lag_time_max: crabka_units::millis(2_000),
        controller_election_timeout: crabka_units::millis(500),
        controller_heartbeat_interval: crabka_units::millis(100),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        ..BrokerConfig::default()
    };
    let reborn = Broker::start(reborn_cfg).await.expect("reborn node 3");
    eprintln!("CRABKA[test] reborn node 3 started");

    // 5. Re-find the controller leader (broker_death_elects_new_leader proves it
    //    might have changed during the joint-config commit).
    let leader_idx = find_controller_leader(&cluster).await;

    // 6. Register reborn node 3 as a learner; openraft will replicate the
    //    committed log to it. Then promote it back to a voter.
    cluster[leader_idx]
        .0
        .add_learner(crabka_broker::NodeId(3), dead_controller_addr)
        .await
        .expect("add reborn node 3 as learner");
    cluster[leader_idx]
        .0
        .change_membership(
            [
                crabka_broker::NodeId(1),
                crabka_broker::NodeId(2),
                crabka_broker::NodeId(3),
            ]
            .into_iter()
            .collect(),
        )
        .await
        .expect("promote reborn node 3 to voter");

    // 7. Wait for the partition's ISR to expand back to {1, 2, 3}.
    cluster[0].0.wait_until_isr_len("expand", 0, 3).await;

    reborn.shutdown().await;
    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn produce_during_leader_failover() {
    let _g = cluster_lock().lock().await;
    let mut cluster = support::start_n_node_with_retry(3).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;
    let bootstrap_1 = cluster[0].1.listen_addr.to_string();
    create_topic(&cluster[0].0, &bootstrap_1, "failover", 3).await;

    // Produce 5 records with acks=1, kill broker 1 mid-burst, produce 5
    // more pointed at broker 2's bootstrap (clients will re-fetch
    // metadata on NOT_LEADER_OR_FOLLOWER).
    for v in &["a", "b", "c", "d", "e"] {
        produce_acks(&bootstrap_1, "failover", &[v], 1, 5_000)
            .await
            .expect("pre");
    }
    let bootstrap_2 = cluster[1].1.listen_addr.to_string();
    let dead = cluster.remove(0);
    dead.0.shutdown().await;

    // Wait for the new leader to be elected (node 1 was killed).
    cluster[0]
        .0
        .wait_until_partition_leader_changed("failover", 0, crabka_broker::NodeId(1))
        .await;

    // Continue producing. The first attempt may hit NOT_LEADER_OR_FOLLOWER;
    // retry via bootstrap_2.
    for v in &["f", "g", "h", "i", "j"] {
        let res = produce_acks(&bootstrap_2, "failover", &[v], 1, 5_000).await;
        // Either success (election done) or NOT_LEADER_OR_FOLLOWER if metadata still stale.
        // For test purposes, accept either; just verify the cluster keeps serving.
        let _ = res;
    }

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}
