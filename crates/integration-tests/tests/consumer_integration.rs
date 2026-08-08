//! End-to-end consumer tests against a live broker.
//!
//! A Rust producer writes records through `crabka-client-core`. A Rust
//! [`crabka_client_consumer::Consumer`] subscribes through a group and reads
//! the records back. The commits survive a broker restart.
//!
//! `flavor = "multi_thread", worker_threads = 2` is mandatory here for the
//! same reason as the JVM acceptance tests. A single-threaded runtime cannot
//! drive both the broker's accept loop and the test body when the test makes
//! synchronous-style blocking calls into the broker.

use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use assert2::check;
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    records::{Record, RecordBatch},
};
use tempfile::TempDir;

/// Builds a `RecordBatch` with one entry per value.
///
/// This helper is a copy of the helper in `crates/broker/tests/integration.rs`.
fn record_batch_with_values(values: &[&str]) -> RecordBatch {
    let len_i32 = i32::try_from(values.len()).expect("test fixture small enough for i32");
    let len_i64 = i64::try_from(values.len()).expect("test fixture small enough for i64");
    let mut batch = RecordBatch {
        last_offset_delta: (len_i32 - 1).max(0),
        max_timestamp: len_i64,
        ..RecordBatch::default()
    };
    for (i, v) in values.iter().enumerate() {
        batch.records.push(Record {
            offset_delta: i32::try_from(i).expect("test fixture small enough for i32"),
            value: Some(Bytes::from(v.to_string())),
            ..Default::default()
        });
    }
    batch
}

/// Resolves the topic UUID through a Metadata request.
///
/// Produce and Fetch at v ≥ 13 carry only `topic_id` on the wire.
async fn topic_id_for(client: &Client, name: &str) -> crabka_protocol::primitives::uuid::Uuid {
    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata for topic_id");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

async fn produce(broker: &BrokerHandle, client: &Client, topic: &str, values: &[&str]) {
    // Wait until partition 0 is materialized in the metadata image before
    // producing, so the common case doesn't race CreateTopics propagation.
    // The bounded retry loop below stays as a backstop for any residual
    // openraft state-machine apply lag on slow CI runners (especially Windows).
    broker.wait_until_partition_present(topic, 0).await;
    // Retry on UNKNOWN_TOPIC_OR_PARTITION (3) up to 5 times: the
    // openraft state-machine apply has occasionally-visible-late timing
    // on slow CI runners (especially Windows), and producers immediately
    // following CreateTopics can race the metadata propagation.
    let topic_id = topic_id_for(client, topic).await;
    for attempt in 1..=5 {
        let resp = client
            .send(ProduceRequest {
                acks: 1,
                timeout_ms: 5_000,
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
            .expect("produce");
        let err = resp.responses[0].partition_responses[0].error_code;
        if err == 0 {
            return;
        }
        if err == 3 && attempt < 5 {
            // UNKNOWN_TOPIC_OR_PARTITION — metadata-apply race; retry.
            // real-time wait (not a progress poll): bounded retry backoff between full Produce RPC round-trips
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        panic!("produce failed after {attempt} attempt(s): {resp:?}");
    }
}

async fn create_topic_with_partitions(client: &Client, name: &str, num_partitions: i32) {
    let cr = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert2::assert!(cr.topics[0].error_code == 0);
}

/// Produces records to a specific partition index.
///
/// The plain `produce` helper always writes to partition 0.
async fn produce_to_partition(
    broker: &BrokerHandle,
    client: &Client,
    topic: &str,
    partition: i32,
    values: &[&str],
) {
    // Wait until the target partition is materialized before producing; the
    // bounded retry loop below remains as a backstop for residual apply lag.
    broker.wait_until_partition_present(topic, partition).await;
    let topic_id = topic_id_for(client, topic).await;
    for attempt in 1..=5 {
        let resp = client
            .send(ProduceRequest {
                acks: 1,
                timeout_ms: 5_000,
                topic_data: vec![TopicProduceData {
                    name: topic.into(),
                    topic_id,
                    partition_data: vec![PartitionProduceData {
                        index: partition,
                        records: Some(record_batch_with_values(values).into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("produce");
        let err = resp.responses[0].partition_responses[0].error_code;
        if err == 0 {
            return;
        }
        if err == 3 && attempt < 5 {
            // UNKNOWN_TOPIC_OR_PARTITION — metadata-apply race; retry.
            // real-time wait (not a progress poll): bounded retry backoff between full Produce RPC round-trips
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        panic!("produce failed after {attempt} attempt(s): {resp:?}");
    }
}

async fn create_topic(client: &Client, name: &str) {
    let cr = client
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
    assert2::assert!(cr.topics[0].error_code == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rust_producer_to_rust_consumer_through_group() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let producer = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("rust-producer")
        .build()
        .await
        .unwrap();
    create_topic(&producer, "rrtopic").await;
    produce(&broker, &producer, "rrtopic", &["a", "b", "c"]).await;

    let mut consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .client_id("rust-consumer")
        .group_id("g1")
        .session_timeout(crabka_units::secs(30))
        .rebalance_timeout(crabka_units::secs(2))
        .heartbeat_interval(crabka_units::secs(1))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe(["rrtopic".to_string()])
        .build()
        .await
        .unwrap();

    let mut seen: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while std::time::Instant::now() < deadline && seen.len() < 3 {
        let records = consumer.poll(crabka_units::millis(500)).await.unwrap();
        for r in records {
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned());
        }
    }
    assert2::assert!(seen == vec!["a", "b", "c"]);

    consumer.commit_sync().await.unwrap();
    consumer.close().await.unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offsets_survive_broker_restart() {
    let dir = TempDir::new().unwrap();
    let log_path = dir.path().to_path_buf();

    // First boot: create + produce + consume + commit.
    {
        let broker = Broker::start(BrokerConfig::for_tests(log_path.clone()))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let producer = Client::builder()
            .bootstrap(&bootstrap)
            .client_id("p")
            .build()
            .await
            .unwrap();
        create_topic(&producer, "persist").await;
        produce(&broker, &producer, "persist", &["x", "y", "z"]).await;

        let mut consumer = Consumer::builder()
            .bootstrap(&bootstrap)
            .client_id("c")
            .group_id("persist-grp")
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .rebalance_timeout(crabka_units::secs(2))
            .heartbeat_interval(crabka_units::secs(1))
            .subscribe(["persist".to_string()])
            .build()
            .await
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut seen = 0;
        while std::time::Instant::now() < deadline && seen < 3 {
            seen += consumer
                .poll(crabka_units::millis(500))
                .await
                .unwrap()
                .len();
        }
        assert2::assert!(seen == 3);
        consumer.commit_sync().await.unwrap();
        consumer.close().await.unwrap();
        broker.shutdown().await;
    }

    // Second boot: same group reads from the committed offset (= end).
    {
        let mut cfg = BrokerConfig::for_tests(log_path);
        // The raft log already exists from the first boot; Bootstrap would be
        // rejected with "requires empty raft log". Rejoin replays on-disk state.
        cfg.bootstrap_mode = crabka_broker::BootstrapMode::Rejoin;
        let broker = Broker::start(cfg).await.unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let mut consumer = Consumer::builder()
            .bootstrap(&bootstrap)
            .client_id("c2")
            .group_id("persist-grp")
            .rebalance_timeout(crabka_units::secs(2))
            .heartbeat_interval(crabka_units::secs(1))
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .subscribe(["persist".to_string()])
            .build()
            .await
            .unwrap();
        // Quick poll: should NOT receive the same x/y/z again.
        let r = consumer.poll(crabka_units::millis(500)).await.unwrap();
        assert2::assert!(r.is_empty());
        consumer.close().await.unwrap();
        broker.shutdown().await;
    }
}

/// Two Range (eager) consumers share a 2-partition topic.
///
/// When the second consumer joins, the survivor gives up a partition through
/// the coordinator's *eager* rejoin path. When the second consumer leaves, the
/// survivor takes the freed partition back through that same path. That path
/// primes the fetch offset of the re-acquired partition *before* it publishes
/// the assignment again.
///
/// This test covers the prime-before-publish order in the eager branch of
/// `coordinator.rs`. `cooperative_rebalance.rs` covers the cooperative
/// branches. A poll that races the rejoin must not see a re-acquired partition
/// with no primed offset and fetch it from 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eager_rebalance_reacquires_and_primes() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let producer = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("p")
        .build()
        .await
        .unwrap();
    create_topic_with_partitions(&producer, "eagerrebal", 2).await;

    let build = |client_id: &'static str| {
        let bootstrap = bootstrap.clone();
        async move {
            Consumer::builder()
                .bootstrap(&bootstrap)
                .client_id(client_id)
                .group_id("eager-grp")
                .session_timeout(crabka_units::secs(30))
                .rebalance_timeout(crabka_units::secs(2))
                .heartbeat_interval(crabka_units::millis(500))
                .auto_offset_reset(AutoOffsetReset::Earliest)
                .subscribe(["eagerrebal".to_string()])
                .build()
                .await
                .expect("build consumer")
        }
    };

    // Build both members concurrently so they batch into the *first*
    // rebalance round (the broker holds it open for INITIAL_REBALANCE_DELAY
    // and only completes once both have joined). That avoids the
    // follower-waits-on-a-late-leader deadlock you'd get by adding a second
    // member to an already-stable group on a tight rebalance window. They
    // split the two partitions 1/1.
    let (mut m1, m2) = tokio::join!(build("m1"), build("m2"));
    tokio::time::timeout(Duration::from_secs(30), async {
        let settle = Instant::now() + Duration::from_secs(30);
        loop {
            if m1.assignment().await.len() == 1 && m2.assignment().await.len() == 1 {
                break;
            }
            assert2::assert!(Instant::now() < settle);
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("1/1 eager split within 30s");

    // m2 leaves → m1 becomes the sole member and re-acquires the freed
    // partition via the coordinator's *eager* rejoin path, priming its fetch
    // offset before the assignment is republished. m1 is its own leader here,
    // so there is no follower-wait to race.
    m2.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        let regain = Instant::now() + Duration::from_secs(30);
        loop {
            let _ = m1.poll(crabka_units::millis(200)).await;
            if m1.assignment().await.len() == 2 {
                break;
            }
            assert2::assert!(Instant::now() < regain);
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("m1 reacquired both partitions within 30s");

    // Produce a fresh record to each partition; m1 (sole owner again) must
    // deliver both — proving the re-acquired partition primed correctly and
    // poll() didn't get stuck on a missing next-offset entry.
    produce_to_partition(&broker, &producer, "eagerrebal", 0, &["b0"]).await;
    produce_to_partition(&broker, &producer, "eagerrebal", 1, &["b1"]).await;
    let mut second: HashSet<String> = HashSet::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    while second.len() < 2 && Instant::now() < deadline {
        for r in m1.poll(crabka_units::millis(200)).await.unwrap() {
            let v = String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned();
            if v.starts_with('b') {
                second.insert(v);
            }
        }
    }
    assert2::assert!(second.len() == 2);

    m1.close().await.unwrap();
    broker.shutdown().await;
}

/// Regression: a commit after a rebalance stamps the CURRENT generation.
///
/// A rebalance bumps the group generation. A commit issued after that rebalance
/// must use the current generation, not the start-up snapshot. The commit path
/// read a `generation_id` captured at build time and never kept it in sync as
/// the coordinator rejoined. So the first commit after any rebalance hit
/// `ILLEGAL_GENERATION (22)`, and a long-running block-builder/compactor commit
/// loop crashed. The demo metrics-compactor crash-loop showed this failure.
///
/// The coordinator now shares the generation in an `Arc<AtomicI32>` and
/// publishes it on every rejoin. A rebalance code on commit now defers the
/// commit instead of failing it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_succeeds_after_rebalance_bumps_generation() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let producer = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("p")
        .build()
        .await
        .unwrap();
    create_topic_with_partitions(&producer, "genbump", 2).await;

    let build = |client_id: &'static str| {
        let bootstrap = bootstrap.clone();
        async move {
            Consumer::builder()
                .bootstrap(&bootstrap)
                .client_id(client_id)
                .group_id("genbump-grp")
                .session_timeout(crabka_units::secs(30))
                .rebalance_timeout(crabka_units::secs(2))
                .heartbeat_interval(crabka_units::millis(500))
                .auto_offset_reset(AutoOffsetReset::Earliest)
                .subscribe(["genbump".to_string()])
                .build()
                .await
                .expect("build consumer")
        }
    };

    // Build both members in the first rebalance round → they split 1/1.
    let (mut m1, m2) = tokio::join!(build("m1"), build("m2"));
    tokio::time::timeout(Duration::from_secs(30), async {
        let settle = Instant::now() + Duration::from_secs(30);
        loop {
            if m1.assignment().await.len() == 1 && m2.assignment().await.len() == 1 {
                break;
            }
            assert2::assert!(Instant::now() < settle);
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("1/1 split within 30s");
    let gen_after_split = m1.generation_id();

    // m2 leaves → m1 re-acquires both partitions via an eager rejoin, which
    // advances the group generation. m1's commit must follow that bump.
    m2.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        let regain = Instant::now() + Duration::from_secs(30);
        loop {
            let _ = m1.poll(crabka_units::millis(200)).await;
            if m1.assignment().await.len() == 2 {
                break;
            }
            assert2::assert!(Instant::now() < regain);
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("m1 reacquired both partitions within 30s");

    // The rejoin bumped the generation, and the accessor reads it live (shared
    // atomic) — proving the generation the commit path stamps is current.
    let gen_after_rejoin = m1.generation_id();
    assert2::assert!(gen_after_rejoin > gen_after_split);

    // Consume a record on each partition so there are offsets to commit, then
    // commit. Pre-fix this stamped the stale start-up generation and the broker
    // returned ILLEGAL_GENERATION (22) → commit_sync errored → panic here.
    produce_to_partition(&broker, &producer, "genbump", 0, &["g0"]).await;
    produce_to_partition(&broker, &producer, "genbump", 1, &["g1"]).await;
    let mut seen: HashSet<String> = HashSet::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    while seen.len() < 2 && Instant::now() < deadline {
        for r in m1.poll(crabka_units::millis(200)).await.unwrap() {
            let v = String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned();
            if v.starts_with('g') {
                seen.insert(v);
            }
        }
    }
    assert2::assert!(seen.len() == 2);

    m1.commit_sync()
        .await
        .expect("commit after a generation-bumping rebalance must succeed (current generation)");

    m1.close().await.unwrap();
    broker.shutdown().await;
}

// ── KIP-320 truncation detection (Task 10) ───────────────────────────────────

use crabka_client_consumer::ConsumerError;
use crabka_protocol::owned::{
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic},
    offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopics},
};

/// Moves a partition's `log_start_offset` forward with `DeleteRecords`.
///
/// `DeleteRecords` is `api_key=21`. It drops every record below `offset`. This
/// helper returns the resulting `low_watermark`. A consumer positioned below
/// the new log start then sees `OFFSET_OUT_OF_RANGE` on its next `Fetch`. This
/// is the deterministic way to cause the truncation and divergence that
/// KIP-320 handles.
async fn delete_records_before(client: &Client, topic: &str, partition: i32, offset: i64) -> i64 {
    let resp = client
        .send(DeleteRecordsRequest {
            topics: vec![DeleteRecordsTopic {
                name: topic.into(),
                partitions: vec![DeleteRecordsPartition {
                    partition_index: partition,
                    offset,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("DeleteRecords");
    let pr = &resp.topics[0].partitions[0];
    assert2::assert!(pr.error_code == 0);
    pr.low_watermark
}

/// `OFFSET_OUT_OF_RANGE` recovery under `auto.offset.reset=latest`.
///
/// A consumer sits at offset 0 while `DeleteRecords` moves `log_start` to 5.
/// The next `Fetch` from 0 is below the log start, so the broker returns
/// `OFFSET_OUT_OF_RANGE` (code 1). The error-first poll loop then resets by
/// policy: `Latest` writes the `i64::MAX` sentinel. The next poll resolves that
/// sentinel to the live log-end with `ListOffsets(-1)`. The consumer then reads
/// the records produced after that point. This shows real recovery, with no
/// error and a resumed fetch, and not a silent stall.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_resets_on_offset_out_of_range_latest() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let producer = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("p")
        .build()
        .await
        .unwrap();
    create_topic(&producer, "oor-latest").await;
    produce(
        &broker,
        &producer,
        "oor-latest",
        &["a", "b", "c", "d", "e", "f", "g", "h"],
    )
    .await;

    // Trim log_start to 5; the consumer (starting at 0) is now below the log.
    let low = delete_records_before(&producer, "oor-latest", 0, 5).await;
    assert2::assert!(low == 5);
    assert2::assert!(broker.partition_log_start_for_test("oor-latest", 0) == Some(5));

    let mut consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .client_id("c")
        .group_id("oor-latest-grp")
        .session_timeout(crabka_units::secs(30))
        .rebalance_timeout(crabka_units::secs(2))
        .heartbeat_interval(crabka_units::secs(1))
        .auto_offset_reset(AutoOffsetReset::Latest)
        .subscribe(["oor-latest".to_string()])
        .build()
        .await
        .unwrap();

    // First poll: fetch from 0 → OFFSET_OUT_OF_RANGE → reset to the Latest
    // sentinel. It must NOT surface an error.
    let first = consumer
        .poll(crabka_units::millis(400))
        .await
        .expect("OOR under Latest must reset, not error");
    assert2::assert!(first.is_empty());

    // Produce fresh records past the (resolved) log-end; the recovered
    // consumer must deliver exactly these.
    produce(&broker, &producer, "oor-latest", &["NEW1", "NEW2", "NEW3"]).await;
    let mut seen: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline && seen.len() < 3 {
        for r in consumer
            .poll(crabka_units::millis(300))
            .await
            .expect("post-reset poll must not error")
        {
            // Recovery landed at the live log-end (>= 8), never below the trim.
            assert2::assert!(r.offset >= 5);
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned());
        }
    }
    assert2::assert!(seen == vec!["NEW1", "NEW2", "NEW3"]);

    consumer.close().await.unwrap();
    broker.shutdown().await;
}

/// `OFFSET_OUT_OF_RANGE` recovery under `auto.offset.reset=earliest`.
///
/// A consumer sits at offset 0 while `DeleteRecords` moves `log_start` to 5.
/// The next `Fetch` from 0 is below the log start, so the broker returns
/// `OFFSET_OUT_OF_RANGE` (code 1). The error-first poll loop then resets by
/// policy: `Earliest` must reset to the `log_start_offset` in the response,
/// which is 5 in this test, and NOT to the literal 0. A reset to 0 would cause
/// `OFFSET_OUT_OF_RANGE` again without end, and that is the root cause this
/// test catches.
///
/// After recovery the consumer starts again from the new log start. It delivers
/// the records that survived the trim, and also the records produced after the
/// trim. This shows real recovery.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_resets_on_offset_out_of_range_earliest() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let producer = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("p")
        .build()
        .await
        .unwrap();
    create_topic(&producer, "oor-earliest").await;
    // Produce 8 records at offsets 0–7.
    produce(
        &broker,
        &producer,
        "oor-earliest",
        &["a", "b", "c", "d", "e", "f", "g", "h"],
    )
    .await;

    // Trim log_start to 5; a consumer starting at 0 is now below the log.
    let low = delete_records_before(&producer, "oor-earliest", 0, 5).await;
    assert2::assert!(low == 5);
    assert2::assert!(broker.partition_log_start_for_test("oor-earliest", 0) == Some(5));

    let mut consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .client_id("c")
        .group_id("oor-earliest-grp")
        .session_timeout(crabka_units::secs(30))
        .rebalance_timeout(crabka_units::secs(2))
        .heartbeat_interval(crabka_units::secs(1))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe(["oor-earliest".to_string()])
        .build()
        .await
        .unwrap();

    // Drive polls until we have consumed at least the 3 records that survived
    // the trim (offsets 5, 6, 7 → "f", "g", "h"). The consumer must NOT loop
    // forever on OFFSET_OUT_OF_RANGE; it must recover by jumping to
    // log_start_offset (5) and delivering from there.
    let mut seen: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline && seen.len() < 3 {
        for r in consumer
            .poll(crabka_units::millis(300))
            .await
            .expect("Earliest OOR reset must not error")
        {
            // Every delivered record must be at or after the new log start.
            // If we ever see an offset < 5, the consumer re-fetched from 0
            // instead of from log_start — the original bug.
            assert2::assert!(r.offset >= 5);
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned());
        }
    }
    assert2::assert!(seen == vec!["f", "g", "h"]);

    consumer.close().await.unwrap();
    broker.shutdown().await;
}

/// `auto.offset.reset=none` reports `OFFSET_OUT_OF_RANGE` as an error.
///
/// The consumer returns `ConsumerError::LogTruncation` and does not reset
/// silently.
///
/// This test causes the same divergence as the `Latest` test: `DeleteRecords`
/// trims `log_start` past the consumer's offset. But the `None` policy makes
/// the OOR arm return an error instead of a safe offset. `poll()` must return
/// `Err(ConsumerError::LogTruncation { .. })`. That error carries the
/// out-of-range fetch offset and, as the `safe_offset`, the `log_start_offset`
/// from the response.
///
/// A new `None` consumer starts at the `i64::MAX` sentinel, which resolves to
/// the live log-end, so it is never out of range. To seat a below-trim position
/// deterministically, an `Earliest` seed consumer first commits offset 0 for
/// the group BEFORE the trim. `DeleteRecords` then moves `log_start` forward
/// past that committed 0. The `None` consumer inherits the below-trim committed
/// offset and gets `OFFSET_OUT_OF_RANGE`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_none_policy_surfaces_log_truncation() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let producer = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("p")
        .build()
        .await
        .unwrap();
    create_topic(&producer, "oor-none").await;
    produce(
        &broker,
        &producer,
        "oor-none",
        &["a", "b", "c", "d", "e", "f"],
    )
    .await;

    // Seed: commit offset 0 for the group BEFORE the trim. The Earliest
    // consumer primes its next_offset to 0 during assignment; we wait for
    // the coordinator to settle (without polling/consuming records) and then
    // commit the initial 0 position. DeleteRecords then advances log_start
    // to 4, stranding the group's committed 0 below the new log start.
    {
        let seed = Consumer::builder()
            .bootstrap(&bootstrap)
            .client_id("seed")
            .group_id("oor-none-grp")
            .session_timeout(crabka_units::secs(30))
            .rebalance_timeout(crabka_units::secs(2))
            .heartbeat_interval(crabka_units::secs(1))
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .subscribe(["oor-none".to_string()])
            .build()
            .await
            .unwrap();
        // The coordinator runs as a background task; wait for it to complete
        // prime_offsets (which seats next_offset = 0 for Earliest with no
        // existing commit). We check assignment() rather than poll()-ing so
        // that we don't consume records and advance the position.
        tokio::time::timeout(Duration::from_secs(30), async {
            let settle = Instant::now() + Duration::from_secs(10);
            while seed.assignment().await.is_empty() {
                assert2::assert!(Instant::now() < settle);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("seed consumer assigned within 30s");
        // next_offsets is 0 (Earliest, primed during assignment, no records
        // fetched yet that would advance it). Committing 0 seats the group.
        seed.commit_sync().await.unwrap();
        seed.close().await.unwrap();
    }

    // Now trim past offset 0; the group's committed 0 is now below log_start.
    let low = delete_records_before(&producer, "oor-none", 0, 4).await;
    assert2::assert!(low == 4);

    let mut consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .client_id("c")
        .group_id("oor-none-grp")
        .session_timeout(crabka_units::secs(30))
        .rebalance_timeout(crabka_units::secs(2))
        .heartbeat_interval(crabka_units::secs(1))
        .auto_offset_reset(AutoOffsetReset::None)
        .subscribe(["oor-none".to_string()])
        .build()
        .await
        .unwrap();

    // The committed offset (0) is below log_start (4) → OFFSET_OUT_OF_RANGE →
    // None surfaces LogTruncation. Empty/timeout polls are tolerated while the
    // assignment settles, but the first non-empty outcome must be the error.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut got_truncation = false;
    while Instant::now() < deadline {
        match consumer.poll(crabka_units::millis(300)).await {
            Ok(recs) => {
                assert2::assert!(recs.is_empty());
            }
            Err(ConsumerError::LogTruncation {
                topic,
                partition,
                fetch_offset,
                ..
            }) => {
                assert2::assert!(topic == "oor-none");
                assert2::assert!(partition == 0);
                check!(
                    fetch_offset == 0,
                    "fetch_offset should be the out-of-range offset 0, got {fetch_offset}"
                );
                got_truncation = true;
                break;
            }
            Err(other) => panic!("expected LogTruncation, got {other:?}"),
        }
    }
    assert2::assert!(got_truncation);

    consumer.close().await.unwrap();
    broker.shutdown().await;
}

/// The `committed_leader_epoch` survives a broker restart.
///
/// The consumer commits the epoch it consumed, and `OffsetFetch` reads that
/// epoch back. The consumer needs this round-trip to seed
/// `positions[..].offset_epoch`, so that a later leader-epoch bump can start
/// KIP-320 validation.
///
/// The test produces the records at the partition's natural leader epoch, which
/// is 0. The consumer's `Fetch` carries `current_leader_epoch = 0` and matches
/// the broker, so the broker does not epoch-fence the fetch and the consumer
/// sees `ConsumerRecord.leader_epoch == 0`. After the consume and the commit,
/// the committed epoch must read back as exactly `0` across a broker restart.
/// It must NOT read back as the `-1` "no epoch committed" sentinel that an
/// uncommitted partition gives.
///
/// The difference between `0` and `-1` proves that the consumer sent the
/// consumed epoch through `OffsetCommit` and that the epoch came back through
/// `OffsetFetch`. The test checks an unrelated never-committed group as the
/// `-1` control.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_leader_epoch_survives_restart() {
    let dir = TempDir::new().unwrap();
    let log_path = dir.path().to_path_buf();
    let topic_uuid;

    // First boot: produce, consume, assert the records carry epoch 0, commit.
    {
        let broker = Broker::start(BrokerConfig::for_tests(log_path.clone()))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let producer = Client::builder()
            .bootstrap(&bootstrap)
            .client_id("p")
            .build()
            .await
            .unwrap();
        create_topic(&producer, "epoch-persist").await;
        topic_uuid = topic_id_for(&producer, "epoch-persist").await;
        produce(&broker, &producer, "epoch-persist", &["e0", "e1", "e2"]).await;

        let mut consumer = Consumer::builder()
            .bootstrap(&bootstrap)
            .client_id("c")
            .group_id("epoch-grp")
            .session_timeout(crabka_units::secs(30))
            .rebalance_timeout(crabka_units::secs(2))
            .heartbeat_interval(crabka_units::secs(1))
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .subscribe(["epoch-persist".to_string()])
            .build()
            .await
            .unwrap();

        let mut epochs: Vec<i32> = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline && epochs.len() < 3 {
            for r in consumer.poll(crabka_units::millis(300)).await.unwrap() {
                epochs.push(r.leader_epoch);
            }
        }
        assert2::assert!(epochs == vec![0, 0, 0]);

        consumer.commit_sync().await.unwrap();
        consumer.close().await.unwrap();
        broker.shutdown().await;
    }

    // Second boot: re-read the committed offset via OffsetFetch and assert the
    // committed_leader_epoch survived as 0 (not the -1 "absent" sentinel).
    {
        let mut cfg = BrokerConfig::for_tests(log_path);
        cfg.bootstrap_mode = crabka_broker::BootstrapMode::Rejoin;
        let broker = Broker::start(cfg).await.unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let client = Client::builder()
            .bootstrap(&bootstrap)
            .client_id("verify")
            .build()
            .await
            .unwrap();

        let fetch_committed = |group: &str| {
            let client = &client;
            let req = OffsetFetchRequest {
                group_id: group.into(),
                groups: vec![OffsetFetchRequestGroup {
                    group_id: group.into(),
                    topics: Some(vec![OffsetFetchRequestTopics {
                        name: "epoch-persist".into(),
                        topic_id: topic_uuid,
                        partition_indexes: vec![0],
                        ..Default::default()
                    }]),
                    ..Default::default()
                }],
                ..Default::default()
            };
            async move {
                let of = client.send(req).await.expect("OffsetFetch");
                // v8+ response shape: the per-group `groups[]` array.
                of.groups
                    .iter()
                    .flat_map(|g| &g.topics)
                    .flat_map(|t| &t.partitions)
                    .find(|p| p.partition_index == 0)
                    .map(|p| (p.committed_offset, p.committed_leader_epoch))
                    .expect("partition row present in OffsetFetch response")
            }
        };

        let (offset, epoch) = fetch_committed("epoch-grp").await;
        assert2::assert!(offset == 3);
        assert2::assert!(epoch == 0);

        // Control: a never-committed group yields the -1 "absent" sentinel, so
        // the `epoch == 0` above is a real committed value, not a default.
        let (ctrl_offset, ctrl_epoch) = fetch_committed("never-committed-grp").await;
        assert2::assert!(ctrl_offset == -1 && ctrl_epoch == -1);

        broker.shutdown().await;
    }
}

/// Regression for the WAL-consumer cold-start hang.
///
/// A single-member group that JOINS before its subscribed topic exists gets a
/// 0-partition assignment. The broker never sends a rebalance to a Stable
/// one-member group. Recovery depends *solely* on the coordinator: its metadata
/// refresh must see that the topic appeared, and the coordinator must rejoin.
/// Without that loop the empty assignment stays empty for ever, which is the
/// `logs-compactor` and `profiles-block-builder` hang.
///
/// No second member ever joins, so this test separates the metadata-driven
/// rejoin from the heartbeat-driven `REBALANCE_IN_PROGRESS` path. It also
/// guards the TOCTOU fix: the coordinator seeds its rejoin baseline from the
/// snapshot that it used for the *initial* empty assignment. So a topic created
/// at any time after the join, and also during start-up, is still seen as
/// growth.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_start_rejoins_when_subscribed_topic_appears() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let producer = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("p")
        .build()
        .await
        .unwrap();

    // Subscribe to a topic that does NOT exist yet. The join still succeeds and
    // the assignment is empty — exactly the cold start the WAL consumers hit.
    let mut consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .client_id("c")
        .group_id("cold-start-grp")
        .session_timeout(crabka_units::secs(30))
        .heartbeat_interval(crabka_units::millis(500))
        .subscription_metadata_refresh_interval(crabka_units::millis(750))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe(["wal-late".to_string()])
        .build()
        .await
        .expect("consumer builds even though its subscribed topic does not exist yet");

    // Let the initial join settle, then assert it really is the empty-assignment
    // cold start (a single Stable member with nothing to consume).
    let _ = consumer.poll(crabka_units::millis(200)).await;
    assert2::assert!(consumer.assignment().await.is_empty());

    // The distributor creates the WAL topic AFTER the consumer has joined.
    create_topic_with_partitions(&producer, "wal-late", 2).await;

    // Recovery is driven by the configured metadata refresh interval and
    // bounded by heartbeat wakeups — there is no membership change — so keep a
    // generous outer deadline for slow CI.
    tokio::time::timeout(Duration::from_secs(30), async {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let _ = consumer.poll(crabka_units::millis(200)).await;
            if consumer.assignment().await.len() == 2 {
                break;
            }
            assert2::assert!(Instant::now() < deadline);
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("metadata-driven rejoin within 30s");

    // The recovered assignment must be functional: produce to both partitions
    // and confirm every record is delivered (proves the re-acquired partitions
    // primed their fetch offsets).
    produce_to_partition(&broker, &producer, "wal-late", 0, &["p0a", "p0b"]).await;
    produce_to_partition(&broker, &producer, "wal-late", 1, &["p1a", "p1b"]).await;

    let mut seen: HashSet<String> = HashSet::new();
    tokio::time::timeout(Duration::from_secs(30), async {
        let deadline = Instant::now() + Duration::from_secs(30);
        while seen.len() < 4 {
            for r in consumer.poll(crabka_units::millis(200)).await.unwrap() {
                seen.insert(
                    String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned(),
                );
            }
            assert2::assert!(Instant::now() < deadline);
        }
    })
    .await
    .expect("records delivered from the recovered assignment within 30s");

    let expected: HashSet<String> = ["p0a", "p0b", "p1a", "p1b"]
        .into_iter()
        .map(String::from)
        .collect();
    assert2::assert!(seen == expected);

    consumer.close().await.unwrap();
    broker.shutdown().await;
}
