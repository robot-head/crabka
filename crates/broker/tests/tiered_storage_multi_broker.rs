//! In-process multi-broker tiered-storage metadata-sharing test.
//!
//! Proves that a broker which NEVER ran the RLM copy task itself can serve a
//! remote-tier read using segment metadata it consumed from the replicated
//! `__remote_log_metadata` topic.
//!
//! ## Design
//!
//! Three in-process Crabka brokers boot with:
//! - advertised listeners on `127.0.0.1:<port>` (no Docker/host.docker.internal)
//! - a **shared** `Local` remote-storage backend (same temp dir) on all three
//! - topic-backed RLMM, all clients bootstrap to broker 1's loopback port
//!   (`num_partitions=1, replication=1` — all metadata on broker 1's partition 0)
//!
//! A **3-broker quorum** is required so that after killing the partition leader
//! (broker 1), the surviving quorum (2/3 = majority) can still commit the
//! partition-leader-election record for broker 2.  A 2-voter cluster (1/2 < majority)
//! would break the raft quorum and the partition leader could never be moved.
//!
//! The metadata-sharing claim: broker 2's RLMM consumer reads `CopySegment*`
//! events from broker 1's `__remote_log_metadata` partition 0 over loopback and
//! caches segment metadata locally.  When broker 1 is shut down, broker 2 uses
//! the already-consumed cached metadata to serve remote reads.  The leader-epoch
//! fallback in `remote_reader.rs` (`list_remote_log_segments` scan) handles the
//! epoch change from broker 1's epoch to broker 2's new leader epoch.
//!
//! Scenario:
//! 1. Three brokers boot concurrently (3-voter static bootstrap).
//! 2. Wait for all 3 to see each other + topic-backed RLMM active on all.
//! 3. Create a rf=2, tiered-storage topic with tiny `segment.bytes` and
//!    `local.retention.bytes=1` so every sealed segment is evicted locally.
//!    With 3 registered brokers and rf=2, the round-robin assignment places
//!    the partition on broker 1 (leader) + broker 2 (follower).
//! 4. Produce 160 records via broker 1; wait until several segments land
//!    in the shared remote dir (leader ran the copy task and published
//!    `CopySegment*` events to `__remote_log_metadata`).
//! 5. Wait 8s for broker 2's RLMM consumer to consume the `CopySegment` events.
//! 6. Shut down broker 1 (the partition leader).  The surviving quorum (2/3)
//!    commits a new partition leader record; broker 2 wins the election.
//! 7. Consume ALL records from broker 2 at offset 0.  These can only come
//!    from the shared remote tier — broker 2's local log is evicted and it
//!    never ran the copy task.  Broker 2 serves them using cached metadata.
//!
//! ## Discriminating property
//!
//! The survivor never ran the copy task for these segments and its local copy is
//! evicted; it can only serve via the shared Local tier + shared RLMM metadata.
//! With a per-broker in-memory RLMM the survivor would have no metadata and the
//! consume would fail.  Do NOT weaken the assertion (must require all records back).

use assert2::assert;
mod support;

use std::time::{Duration, Instant};

use crabka_broker::{
    BootstrapMode, Broker, BrokerConfig, BrokerHandle, KafkaRlmmConfig, RemoteStorageBackend,
    RlmmKind,
};
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};
use tempfile::TempDir;

const TOPIC: &str = "tiered-multi-broker-itest";
const RECORDS: usize = 160;

/// Boot three in-process brokers with a shared Local remote tier and topic-backed
/// RLMM.  A 3-voter quorum is required so the surviving 2/3 can commit the
/// partition-leader-election record after broker 1 is shut down.
///
/// Returns `(broker1, broker2, broker3, dirs[], shared_remote_dir)`.
/// The remote dir is shared so all brokers write/read the same object store.
async fn start_three_tiered_brokers() -> (
    BrokerHandle,
    BrokerHandle,
    BrokerHandle,
    Vec<TempDir>,
    TempDir,
) {
    support::init_tracing();

    // Pre-bind concrete client + controller ports for all 3 brokers.
    // Concrete ports are required: the advertised_listener is registered into
    // the controller image before the listener binds (a `:0` would register
    // port 0 and break inter-broker replication); controller ports go into the
    // static voter set so peers can dial each other.
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(3).await;

    let log_dirs: Vec<TempDir> = (0..3).map(|_| TempDir::new().expect("log dir")).collect();
    // **Shared** remote dir: all brokers point at the same Local object store.
    let remote_dir = TempDir::new().expect("shared remote dir");

    // 3-voter static voter set.
    let voters: Vec<(u64, std::net::SocketAddr)> = (0..3)
        .map(|i| (u64::try_from(i + 1).unwrap(), controller_addrs[i]))
        .collect();

    // Build a config for broker `i` (1-indexed broker_id/node_id).
    let mut broker_configs: Vec<BrokerConfig> = (0..3)
        .map(|i| {
            let mut cfg = BrokerConfig::for_tests(log_dirs[i].path().to_path_buf());
            cfg.broker_id = i32::try_from(i + 1).unwrap();
            cfg.node_id = crabka_broker::NodeId(u64::try_from(i + 1).unwrap());
            cfg.directory_id = uuid::Uuid::from_u128(u128::try_from(i + 1).unwrap());
            cfg.listen_addr = client_addrs[i];
            cfg.advertised_listener = format!("127.0.0.1:{}", client_addrs[i].port());
            cfg.controller_listen_addr = controller_addrs[i];
            cfg.controller_quorum_voters = voters
                .iter()
                .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
                .collect();
            cfg.bootstrap_mode = BootstrapMode::Bootstrap;
            cfg.auto_join = false;
            cfg.bootstrap_servers = vec![];
            cfg.remote_storage_backend = Some(RemoteStorageBackend::Local {
                dir: remote_dir.path().to_path_buf(),
            });
            cfg.remote_log_manager_interval = crabka_units::secs(1);
            // RLMM: all 3 brokers bootstrap into broker 1's loopback.
            // num_partitions=1 keeps all metadata on a single partition.
            // replication=1: partition 0 lives exclusively on broker 1.
            // Broker 2's RLMM consumer reads CopySegment events from broker 1
            // before broker 1 dies; the cached metadata is then used for remote
            // reads from the survivor.
            cfg.remote_log_metadata = RlmmKind::TopicBacked(KafkaRlmmConfig {
                bootstrap: format!("127.0.0.1:{}", client_addrs[0].port()),
                num_partitions: 1,
                replication: 1,
                snapshot_interval: crabka_units::hours(1),
                snapshot_dir: std::path::PathBuf::new(), // derived from log_dir
                security: None,
            });
            cfg
        })
        .collect();

    // Static cold-boot: all 3 start concurrently (sequential would deadlock —
    // a leader needs a majority of the static voter set up).
    let (config0, config1, config2) = (
        broker_configs.remove(0),
        broker_configs.remove(0),
        broker_configs.remove(0),
    );
    let mut client_ls = client_listeners.into_iter();
    let mut ctrl_ls = controller_listeners.into_iter();
    let (client0, controller0) = (client_ls.next().unwrap(), ctrl_ls.next().unwrap());
    let (client1, controller1) = (client_ls.next().unwrap(), ctrl_ls.next().unwrap());
    let (client2, controller2) = (client_ls.next().unwrap(), ctrl_ls.next().unwrap());
    let j0 = tokio::spawn(async move {
        Broker::start_with_listeners(config0, Some(controller0), Some(client0)).await
    });
    let j1 = tokio::spawn(async move {
        Broker::start_with_listeners(config1, Some(controller1), Some(client1)).await
    });
    let j2 = tokio::spawn(async move {
        Broker::start_with_listeners(config2, Some(controller2), Some(client2)).await
    });
    let b1 = j0.await.expect("b1 spawn join").expect("b1 start");
    let b2 = j1.await.expect("b2 spawn join").expect("b2 start");
    let b3 = j2.await.expect("b3 spawn join").expect("b3 start");

    (b1, b2, b3, log_dirs, remote_dir)
}

/// Wait until all three brokers see each other registered (`broker_count` >= 3).
async fn await_all_brokers_registered(b1: &BrokerHandle, b2: &BrokerHandle, b3: &BrokerHandle) {
    // Each broker's own metadata image must show all 3 brokers registered.
    b1.wait_until_brokers_registered(3).await;
    b2.wait_until_brokers_registered(3).await;
    b3.wait_until_brokers_registered(3).await;
}

/// Wait until the topic-backed RLMM is active on all three brokers.
async fn await_all_rlmm_active(b1: &BrokerHandle, b2: &BrokerHandle, b3: &BrokerHandle) {
    // Topic-backed RLMM going live flips the tiered_storage_rlmm_topic_backed
    // gauge to 1 on each broker (the same signal rlmm_topic_backed_active_for_test
    // reads directly).
    b1.wait_for_metrics("b1 topic-backed RLMM active", |m| {
        m.tiered_storage_rlmm_topic_backed.get() == 1
    })
    .await;
    b2.wait_for_metrics("b2 topic-backed RLMM active", |m| {
        m.tiered_storage_rlmm_topic_backed.get() == 1
    })
    .await;
    b3.wait_for_metrics("b3 topic-backed RLMM active", |m| {
        m.tiered_storage_rlmm_topic_backed.get() == 1
    })
    .await;
}

/// Fetch the topic-id for `name` from the given client (Metadata request).
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

/// Count files named `log` anywhere under `root` — the `LocalTieredStorage`
/// segment-bytes object for each copied segment (same helper as
/// `tiered_storage_topic_rlmm.rs`).
fn count_remote_log_files(root: &std::path::Path) -> usize {
    fn walk(dir: &std::path::Path, count: &mut usize) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, count);
            } else if path.file_name().and_then(|n| n.to_str()) == Some("log") {
                *count += 1;
            }
        }
    }
    let mut count = 0;
    walk(root, &mut count);
    count
}

/// Fetch all records from `(topic, partition)` starting at `start_offset`
/// from the broker at `bootstrap`, retrying until `expected_count` records
/// arrive or the deadline expires.  Returns the total record count.
async fn fetch_all_records(
    bootstrap: &str,
    topic: &str,
    partition: i32,
    start_offset: i64,
    expected_count: usize,
    deadline: Instant,
) -> usize {
    let client = Client::builder()
        .bootstrap(bootstrap)
        .client_id("tiered-multi-fetch-test")
        .build()
        .await
        .expect("fetch client build");

    // Resolve the topic id first (retry until the metadata is available from
    // the survivor — the leader election may still be settling).
    let topic_id = loop {
        let id = topic_id_for(&client, topic).await;
        if id != WireUuid::default() {
            break id;
        }
        assert!(
            Instant::now() <= deadline,
            "survivor never returned a valid topic id for {topic} within deadline"
        );
        // intentional: topic-id visibility is polled over the wire client (this
        // helper has no BrokerHandle); retry until the survivor's metadata settles.
        tokio::time::sleep(Duration::from_millis(300)).await;
    };

    let mut total_records = 0usize;
    let mut fetch_offset = start_offset;

    loop {
        let resp = client
            .send(FetchRequest {
                max_wait_ms: 1_000,
                min_bytes: 1,
                topics: vec![FetchTopic {
                    topic: topic.into(),
                    topic_id,
                    partitions: vec![FetchPartition {
                        partition,
                        fetch_offset,
                        partition_max_bytes: 2_097_152,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("Fetch");

        let part = resp.responses.first().and_then(|t| t.partitions.first());

        if let Some(p) = part {
            if p.error_code == 1 {
                // OFFSET_OUT_OF_RANGE — local-log eviction moved past fetch_offset;
                // advance to log_start_offset.
                if p.log_start_offset > fetch_offset {
                    fetch_offset = p.log_start_offset;
                }
            } else if let Some(recs) = p.records.as_ref().and_then(|r| r.as_v2()) {
                for batch in recs {
                    for rec in &batch.records {
                        total_records += 1;
                        fetch_offset = batch.base_offset + i64::from(rec.offset_delta) + 1;
                    }
                }
            }
        }

        if total_records >= expected_count {
            break;
        }

        assert!(
            Instant::now() <= deadline,
            "survivor only served {total_records}/{expected_count} records before deadline; \
             fetch_offset={fetch_offset}"
        );
        // intentional: records are fetched over the wire client (no BrokerHandle
        // here); retry the bounded Fetch poll until the survivor serves them all.
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    total_records
}

async fn create_tiered_topic(admin: &Client, b1: &BrokerHandle, b2: &BrokerHandle) {
    let response = admin
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: TOPIC.into(),
                num_partitions: 1,
                replication_factor: 2,
                configs: vec![
                    CreatableTopicConfig {
                        name: "remote.storage.enable".into(),
                        value: Some("true".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "segment.bytes".into(),
                        value: Some("1024".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "local.retention.bytes".into(),
                        value: Some("1".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "retention.bytes".into(),
                        value: Some("-1".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "retention.ms".into(),
                        value: Some("-1".into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            timeout_ms: 10_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        response.topics[0].error_code == 0,
        "CreateTopics failed: {response:?}"
    );

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let ready = |broker: &BrokerHandle| {
            broker
                .partition_log_config_for_test(TOPIC, 0)
                .is_some_and(|config| {
                    config.remote_storage_enable
                        && config.segment_size == crabka_units::kibibytes(1)
                        && config.local_retention_size == Some(crabka_units::bytes(1))
                })
        };
        if ready(b1) || ready(b2) {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "tiered config did not propagate"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn produce_and_await_remote_segments(admin: &Client, remote_dir: &std::path::Path) {
    let topic_id = topic_id_for(admin, TOPIC).await;
    for index in 0..RECORDS {
        let batch = RecordBatch {
            records: vec![Record {
                value: Some(bytes::Bytes::from(format!("test-record-{index}"))),
                ..Default::default()
            }],
            ..Default::default()
        };
        let response = admin
            .send(ProduceRequest {
                acks: -1,
                timeout_ms: 10_000,
                topic_data: vec![TopicProduceData {
                    name: TOPIC.into(),
                    topic_id,
                    partition_data: vec![PartitionProduceData {
                        index: 0,
                        records: Some(batch.into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("Produce");
        assert!(
            response.responses[0].partition_responses[0].error_code == 0,
            "Produce failed: {response:?}"
        );
    }

    let deadline = Instant::now() + Duration::from_mins(1);
    while count_remote_log_files(remote_dir) < 2 {
        assert!(
            Instant::now() <= deadline,
            "fewer than two segments were tiered"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// In-process multi-broker tiered metadata-sharing proof.
///
/// Three brokers share a `Local` remote tier and a topic-backed RLMM with
/// rf=2 metadata replication.  Broker 1 leads the rf=2 user partition and
/// runs the RLM copy task; broker 2 only consumes `__remote_log_metadata`
/// to learn segment locations.  After broker 1 is shut down, the surviving
/// 2/3 quorum commits a new partition-leader record and broker 2 serves all
/// records from the remote tier — proving the RLMM metadata sharing claim.
///
/// Runs under plain `cargo test` (no Docker, no `MinIO`, no
/// host.docker.internal).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tiered_storage_metadata_sharing_via_survivor() {
    let (b1, b2, b3, _dirs, remote_dir) = start_three_tiered_brokers().await;

    // Wait for all brokers to see each other registered.
    await_all_brokers_registered(&b1, &b2, &b3).await;
    eprintln!("ITEST: all 3 brokers registered; waiting for topic-backed RLMM to activate");

    // Wait for all RLMMs to activate (metadata topic created + bootstrap done).
    await_all_rlmm_active(&b1, &b2, &b3).await;
    eprintln!("ITEST: RLMM active on all 3 brokers; creating tiered topic");

    // Build an admin client against broker 1 for CreateTopics + Produce.
    let b1_bootstrap = format!("127.0.0.1:{}", b1.listen_addr().port());
    let admin = Client::builder()
        .bootstrap(&b1_bootstrap)
        .client_id("tiered-multi-admin")
        .build()
        .await
        .expect("admin client");

    create_tiered_topic(&admin, &b1, &b2).await;
    eprintln!("ITEST: tiered config propagated; discovering partition leader");

    // Discover which of broker 1 / broker 2 is the partition leader.
    // With rf=2 and 3 registered brokers, round-robin assigns [1, 2];
    // broker 1 is the preferred leader. Wait until b1's metadata image names
    // the partition leader as one of the two replicas, then read which.
    let b1_id = b1.node_id();
    let b2_id = b2.node_id();
    b1.wait_for_image(|img| {
        img.partition(TOPIC, 0)
            .is_some_and(|p| p.leader == b1_id || p.leader == b2_id)
    })
    .await;
    let (leader_node_id, follower_node_id, follower_addr) =
        if b1.partition_leader_for_test(TOPIC, 0) == Some(b1_id) {
            let f_addr = format!("127.0.0.1:{}", b2.listen_addr().port());
            (b1_id, b2_id, f_addr)
        } else {
            let f_addr = format!("127.0.0.1:{}", b1.listen_addr().port());
            (b2_id, b1_id, f_addr)
        };
    eprintln!(
        "ITEST: partition leader=broker{leader_node_id} follower=broker{follower_node_id}; \
         producing {RECORDS} records"
    );

    produce_and_await_remote_segments(&admin, remote_dir.path()).await;

    // Give the RLMM time to propagate CopySegment metadata to the follower via
    // __remote_log_metadata (rf=2).  Interval=1s → 8 ticks plus consume latency.
    // intentional: the follower's RLMM consumer catching up on
    // __remote_log_metadata has no metadata-image/metric signal to await;
    // wait a fixed propagation window before killing the leader.
    eprintln!("ITEST: waiting 8s for RLMM metadata propagation to follower");
    // real-time wait (not a progress poll): RLMM propagates CopySegment metadata to the follower over the broker's own 1s interval ticks; no in-process observable to poll.
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Shut down the partition leader.  The surviving 2/3 quorum (broker 2 + 3)
    // can still commit the partition-leader-election record.
    eprintln!("ITEST: shutting down leader (broker{leader_node_id}); waiting for failover");

    // Drop the admin client before shutdown so its connection doesn't block.
    drop(admin);

    // Move all three handles into Options so we can selectively shut the leader
    // down and retain the survivor.
    let mut opt_b1: Option<BrokerHandle> = Some(b1);
    let mut opt_b2: Option<BrokerHandle> = Some(b2);
    let mut opt_b3: Option<BrokerHandle> = Some(b3);

    if leader_node_id == opt_b1.as_ref().unwrap().node_id() {
        opt_b1.take().unwrap().shutdown().await;
        eprintln!("ITEST: broker1 (leader) shut down");
    } else {
        opt_b2.take().unwrap().shutdown().await;
        eprintln!("ITEST: broker2 (leader) shut down");
    }

    // The surviving replica is whichever of b1/b2 is still alive (the follower).
    let survivor = if follower_node_id
        == opt_b1
            .as_ref()
            .map_or(0, crabka_broker::BrokerHandle::node_id)
    {
        opt_b1.as_ref().unwrap()
    } else {
        opt_b2.as_ref().unwrap()
    };

    // Wait for the survivor to become the user-partition leader.
    // The surviving quorum (broker2 + broker3) commits the new leader record.
    eprintln!("ITEST: waiting for survivor (broker{follower_node_id}) to become partition leader");
    // Failover moves the partition leader off the (killed) old leader; with
    // rf=2 the only surviving replica is the follower, so the new leader can
    // only be `follower_node_id`.
    survivor
        .wait_until_partition_leader_changed(TOPIC, 0, crabka_broker::NodeId(leader_node_id))
        .await;
    eprintln!("ITEST: survivor (broker{follower_node_id}) is now partition leader");

    // Give the survivor's RLMM 3 more reconcile ticks to settle on the
    // now-led partition's metadata (RLMM interval=1s → 3 extra ticks).
    // intentional: RLMM reconcile settling has no image/metric signal to await.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Consume ALL produced records from the survivor at offset 0.
    // These can only come from the shared remote tier (local log evicted by
    // local.retention.bytes=1) using metadata the survivor consumed from
    // __remote_log_metadata (it never ran the copy task itself).
    eprintln!(
        "ITEST: consuming {RECORDS} records from survivor (broker{follower_node_id}) \
         at {follower_addr}"
    );
    let consume_deadline = Instant::now() + Duration::from_mins(1);
    let served = fetch_all_records(&follower_addr, TOPIC, 0, 0, RECORDS, consume_deadline).await;

    eprintln!("ITEST: survivor served {served} records (expected >= {RECORDS})");
    assert!(
        served >= RECORDS,
        "expected >= {RECORDS} records served by the surviving broker via the remote tier; \
         got {served}. The survivor (broker{follower_node_id}) should have learned segment \
         locations from __remote_log_metadata (rf=2) without having run the copy task itself."
    );

    // Shut down surviving brokers.
    if let Some(h) = opt_b1.take() {
        h.shutdown().await;
    }
    if let Some(h) = opt_b2.take() {
        h.shutdown().await;
    }
    if let Some(h) = opt_b3.take() {
        h.shutdown().await;
    }
}
