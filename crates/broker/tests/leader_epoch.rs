//! In-process integration tests for KIP-101 leader-epoch
//! fencing + .leader-epoch-checkpoint byte format.
//!
//! Windows-gated like the other multi-broker tests.

use assert2::{assert, check};
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};
use tempfile::TempDir;

mod support;

use std::sync::OnceLock;

use tokio::sync::Mutex;

/// Serialize the multi-broker tests in this binary. Each spins up a
/// 3-broker loopback cluster; running them concurrently exhausts
/// ephemeral ports and starves openraft election timing. Same rationale
/// as `replication.rs::cluster_lock` / `quorum.rs::cluster_lock`.
fn cluster_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn boot_single() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
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
        .expect("metadata");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

async fn create_topic(broker: &BrokerHandle, bootstrap: &str, name: &str) {
    let client = Client::builder()
        .bootstrap(bootstrap.to_string())
        .build()
        .await
        .unwrap();
    let _ = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    broker.wait_until_partition_present(name, 0).await;
}

fn record(value: &str) -> RecordBatch {
    let mut b = RecordBatch::default();
    b.records.push(Record {
        offset_delta: 0,
        value: Some(Bytes::from(value.to_string())),
        ..Default::default()
    });
    b.last_offset_delta = 0;
    b
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fenced_leader_epoch_truncates_zombie_writes() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&broker, &bootstrap, "fence").await;

    // Produce a record at epoch 0.
    let client = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, "fence").await;
    client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "fence".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(record("v0").into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("produce");

    // Force the partition's epoch up to 5 (simulate "split brain").
    broker.test_set_leader_epoch("fence", 0, 5);

    // Fetch with current_leader_epoch=2 → FENCED_LEADER_EPOCH (code 74).
    let resp = client
        .send(FetchRequest {
            replica_id: 99,
            max_wait_ms: 100,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: "fence".into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    current_leader_epoch: 2,
                    partition_max_bytes: 1 << 20,
                    ..FetchPartition::default()
                }],
                ..FetchTopic::default()
            }],
            ..FetchRequest::default()
        })
        .await
        .expect("fetch");
    let pd = &resp.responses[0].partitions[0];
    // FENCED_LEADER_EPOCH = 74
    assert!(pd.error_code == 74, "expected FENCED_LEADER_EPOCH");

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_leader_epoch_on_metadata_lag() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&broker, &bootstrap, "unknown").await;
    let client = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, "unknown").await;

    // Fetch with current_leader_epoch=5 — broker has epoch=0; UNKNOWN_LEADER_EPOCH (code 75).
    let resp = client
        .send(FetchRequest {
            replica_id: 99,
            max_wait_ms: 100,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: "unknown".into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    current_leader_epoch: 5,
                    partition_max_bytes: 1 << 20,
                    ..FetchPartition::default()
                }],
                ..FetchTopic::default()
            }],
            ..FetchRequest::default()
        })
        .await
        .expect("fetch");
    let pd = &resp.responses[0].partitions[0];
    // UNKNOWN_LEADER_EPOCH = 75
    assert!(pd.error_code == 75, "expected UNKNOWN_LEADER_EPOCH");

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn epoch_checkpoint_byte_compat() {
    let (broker, bootstrap, dir) = boot_single().await;
    create_topic(&broker, &bootstrap, "ckpt").await;

    // Produce at epoch 0.
    let client = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, "ckpt").await;
    client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "ckpt".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(record("v0").into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("produce");

    // Bump epoch to 1 + produce another.
    broker.test_set_leader_epoch("ckpt", 0, 1);
    client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "ckpt".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(record("v1").into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("produce");

    // Read the checkpoint file from disk.
    let path = dir.path().join("ckpt-0").join("leader-epoch-checkpoint");
    let s = std::fs::read_to_string(&path).expect("checkpoint file");
    // Format: header "0\n", count "2\n", rows "0 0\n1 1\n".
    check!(s.starts_with("0\n"), "header should be '0\\n', got: {s:?}");
    check!(s.contains("\n2\n"), "count should be 2, got: {s:?}");
    check!(s.contains("0 0\n"), "epoch 0 row missing: {s:?}");
    check!(s.contains("1 1\n"), "epoch 1 row missing: {s:?}");

    broker.shutdown().await;
}

/// KIP-320 leader side. A follower-style Fetch (`replica_id >= 0`) that
/// advertises a stale `last_fetched_epoch` whose epoch ends *before* the
/// requested `fetch_offset` must get a `diverging_epoch` pointing at the
/// epoch boundary, and NO records.
///
/// Build the leader's epoch history deterministically:
///   * produce `k = 2` records at epoch 0  → checkpoint `0 -> 0`,
///   * bump the leader epoch to 1 (split-brain shim used by the fence
///     test), produce 2 more                → checkpoint `1 -> 2`,
/// so the cache is `e0 -> [0, 2)`, `e1 -> [2, 4)` and the log end is 4.
///
/// A follower fetch at `fetch_offset = 4` with `last_fetched_epoch = 0`
/// (i.e. "my last record was in epoch 0, give me offset 4") is divergent:
/// epoch 0 on this leader ends at offset 2, not 4. The leader's
/// `epoch_and_offset_for(0, 4)` returns `(0, 2)` and, because the
/// recorded end (2) is below the fetch offset (4), the handler answers
/// with `diverging_epoch { epoch: 0, end_offset: 2 }` and serves nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diverging_epoch_returned_on_stale_last_fetched_epoch() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&broker, &bootstrap, "diverge").await;

    let client = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, "diverge").await;

    // Helper: produce one single-record batch, stamped with whatever leader
    // epoch the partition currently holds.
    let produce_one = |value: &'static str| {
        let client = &client;
        async move {
            client
                .send(ProduceRequest {
                    acks: 1,
                    timeout_ms: 5_000,
                    topic_data: vec![TopicProduceData {
                        name: "diverge".into(),
                        topic_id,
                        partition_data: vec![PartitionProduceData {
                            index: 0,
                            records: Some(record(value).into()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .await
                .expect("produce");
        }
    };

    // Epoch 0: produce k = 2 records → checkpoint row `0 0`, LEO = 2.
    let e0: i32 = 0;
    let k: i64 = 2;
    produce_one("e0-a").await;
    produce_one("e0-b").await;

    // Bump leader epoch to 1, produce 2 more → checkpoint row `1 2`, LEO = 4.
    broker.test_set_leader_epoch("diverge", 0, 1);
    produce_one("e1-a").await;
    produce_one("e1-b").await;
    let n: i64 = 4;

    // Sanity: the leader really advanced to LEO == n.
    let leo = broker
        .local_log_end_offset("diverge", 0)
        .expect("local leo");
    assert!(leo == n, "expected leader LEO == {n}, got {leo}");

    // Follower Fetch at offset n claiming last_fetched_epoch == e0. Leave
    // `current_leader_epoch` at its -1 default so we don't trip the KIP-101
    // fence and actually reach the KIP-320 divergence check.
    let resp = client
        .send(FetchRequest {
            replica_id: 7,
            max_wait_ms: 100,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: "diverge".into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: n,
                    last_fetched_epoch: e0,
                    partition_max_bytes: 1 << 20,
                    ..FetchPartition::default()
                }],
                ..FetchTopic::default()
            }],
            ..FetchRequest::default()
        })
        .await
        .expect("fetch");

    let part = &resp.responses[0].partitions[0];
    // NONE error code: divergence is reported in-band, not as an error.
    check!(
        part.error_code == 0,
        "expected NONE, got {}",
        part.error_code
    );
    check!(
        part.diverging_epoch.end_offset == k,
        "diverging_epoch.end_offset should be the epoch-0 boundary {k}, got {}",
        part.diverging_epoch.end_offset
    );
    check!(
        part.diverging_epoch.epoch == e0,
        "diverging_epoch.epoch should be {e0}, got {}",
        part.diverging_epoch.epoch
    );
    // No records are served alongside a divergence signal.
    check!(
        part.records.is_none()
            || part
                .records
                .as_ref()
                .and_then(|r| r.as_v2())
                .is_none_or(<[_]>::is_empty),
        "diverging fetch must serve no records"
    );

    broker.shutdown().await;
}

/// KIP-320 follower side, end-to-end. A follower whose local log has a
/// divergent suffix beyond the leader's epoch boundary must truncate it
/// *in band* on the leader's `diverging_epoch` Fetch response — without
/// ever issuing an `OffsetForLeaderEpoch` RPC (that path is reserved for
/// the `FENCED`/`UNKNOWN_LEADER_EPOCH` error codes, which this scenario
/// never produces).
///
/// Determinism without racing a real unclean election:
///   1. 3-broker cluster, topic rf=3, partition leader = broker 1.
///   2. Produce `k = 8` records through the leader (acks=-1); every
///      replica converges to LEO 8 with checkpoint `0 -> 0` (the produce
///      handler stamps leader epoch 0 onto each batch).
///   3. On a *follower*, append a divergent suffix straight to its local
///      log via `produce_records_for_test` (5 extra records). Those
///      batches carry epoch -1, so they add no checkpoint entry — the
///      follower's latest recorded epoch stays 0 while its LEO jumps to
///      13, i.e. records the leader does not have.
///   4. The follower's already-running replicator fetches at offset 13
///      advertising `last_fetched_epoch = 0`. The leader (LEO 8, latest
///      epoch 0) computes `epoch_and_offset_for(0, 8) = (0, 8)`; since
///      the epoch-0 end (8) is below the fetch offset (13) it answers
///      `diverging_epoch { end_offset: 8 }`. The replicator truncates to
///      8 and converges back to the leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_truncates_in_band_on_diverging_epoch() {
    let _g = cluster_lock().lock().await;
    let cluster = support::start_n_node_with_retry(3).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    // cluster[0] is node 1; rf=3 round-robin makes it the leader for
    // partition 0 (same placement the replication tests rely on).
    let leader_addr = cluster[0].1.listen_addr.to_string();
    let admin = Client::builder()
        .bootstrap(leader_addr.clone())
        .build()
        .await
        .unwrap();
    let resp = admin
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "divtrunc".into(),
                num_partitions: 1,
                replication_factor: 3,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(resp.topics[0].error_code == 0);
    let topic_id = resp.topics[0].topic_id;

    // Wait for the partition to materialize on every broker.
    for (h, _, _) in &cluster {
        h.wait_until_partition_present("divtrunc", 0).await;
    }

    // Produce k = 8 records to the leader at epoch 0 (acks=-1 so it lands
    // on the followers too). One record per batch keeps offsets dense.
    let k: i64 = 8;
    let producer = Client::builder()
        .bootstrap(leader_addr)
        .build()
        .await
        .unwrap();
    for i in 0..k {
        let prod = producer
            .send(ProduceRequest {
                acks: -1,
                timeout_ms: 5_000,
                topic_data: vec![TopicProduceData {
                    name: "divtrunc".into(),
                    topic_id,
                    partition_data: vec![PartitionProduceData {
                        index: 0,
                        records: Some(record(&format!("v{i}")).into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(prod.responses[0].partition_responses[0].error_code == 0);
    }

    // Wait for all three brokers to converge to LEO k.
    for (h, _, _) in &cluster {
        h.wait_until_local_log_end_offset("divtrunc", 0, k).await;
    }

    // Pick a follower: a broker that is not the partition leader.
    let leader_id = cluster[0]
        .0
        .partition_leader_for_test("divtrunc", 0)
        .expect("partition leader known");
    let follower_idx = cluster
        .iter()
        .position(|(h, _, _)| h.node_id() != leader_id)
        .expect("a non-leader replica exists");
    let follower = &cluster[follower_idx].0;

    // Append a divergent suffix to the follower's local log. These batches
    // carry epoch -1 (no checkpoint row), so the follower's latest recorded
    // epoch stays 0 while its LEO jumps past the leader's epoch-0 boundary.
    let suffix: i64 = 5;
    follower
        .produce_records_for_test("divtrunc", 0, usize::try_from(suffix).unwrap())
        .await
        .expect("inject divergent suffix");
    let diverged_leo = follower
        .local_log_end_offset("divtrunc", 0)
        .expect("follower leo after suffix");
    assert!(
        diverged_leo == k + suffix,
        "follower should hold a divergent suffix (expected {}, got {diverged_leo})",
        k + suffix
    );

    // The leader stays at LEO k, so its epoch-0 boundary (8) is below the
    // follower's fetch offset (13): the next follower Fetch gets a
    // `diverging_epoch` and the replicator truncates in band back to k.
    // Follower truncates its divergent suffix and re-replicates to match the
    // leader exactly. Wait for the follower to settle at exactly k (it may
    // transiently sit above k with divergent data before truncating).
    follower
        .wait_until_local_log_end_offset_eq("divtrunc", 0, k)
        .await;
    let f_leo = follower.local_log_end_offset("divtrunc", 0).unwrap_or(-1);
    let l_leo = cluster[0]
        .0
        .local_log_end_offset("divtrunc", 0)
        .unwrap_or(-1);
    assert!(
        f_leo == l_leo && f_leo == k,
        "follower did not converge to leader (follower={f_leo}, leader={l_leo}, k={k})"
    );

    // Final cross-check: leader LEO and follower LEO agree.
    assert!(
        follower.local_log_end_offset("divtrunc", 0)
            == cluster[0].0.local_log_end_offset("divtrunc", 0)
    );

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}
