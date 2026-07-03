//! End-to-end: a Rust producer (via `crabka-client-core`) writes records;
//! a Rust [`crabka_client_consumer::Consumer`] subscribes through a group
//! and reads them back; commits survive a broker restart.
//!
//! `flavor = "multi_thread", worker_threads = 2` is mandatory here for the
//! same reason as the JVM acceptance tests: a single-threaded
//! runtime can't drive both the broker's accept loop and the test body
//! when the test makes synchronous-style blocking calls into the broker.

use assert2::{assert, check};
use std::collections::HashSet;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tempfile::TempDir;

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_core::Client;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::records::{Record, RecordBatch};

/// Build a `RecordBatch` with one entry per value. Mirrors the helper in
/// `crates/broker/tests/integration.rs`.
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

/// Resolve the topic UUID via Metadata. Produce / Fetch at v ≥ 13 carry
/// only `topic_id` on the wire.
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
    assert!(cr.topics[0].error_code == 0, "create_topic failed: {cr:?}");
}

/// Produce records to a specific partition index (the plain `produce` helper
/// hardcodes partition 0).
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
    assert!(cr.topics[0].error_code == 0, "create_topic failed: {cr:?}");
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
        .session_timeout(Duration::from_secs(30))
        .rebalance_timeout(Duration::from_secs(2))
        .heartbeat_interval(Duration::from_secs(1))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe(["rrtopic".to_string()])
        .build()
        .await
        .unwrap();

    let mut seen: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while std::time::Instant::now() < deadline && seen.len() < 3 {
        let records = consumer.poll(Duration::from_millis(500)).await.unwrap();
        for r in records {
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned());
        }
    }
    assert!(seen == vec!["a", "b", "c"]);

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
            .rebalance_timeout(Duration::from_secs(2))
            .heartbeat_interval(Duration::from_secs(1))
            .subscribe(["persist".to_string()])
            .build()
            .await
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut seen = 0;
        while std::time::Instant::now() < deadline && seen < 3 {
            seen += consumer
                .poll(Duration::from_millis(500))
                .await
                .unwrap()
                .len();
        }
        assert!(seen == 3);
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
            .rebalance_timeout(Duration::from_secs(2))
            .heartbeat_interval(Duration::from_secs(1))
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .subscribe(["persist".to_string()])
            .build()
            .await
            .unwrap();
        // Quick poll: should NOT receive the same x/y/z again.
        let r = consumer.poll(Duration::from_millis(500)).await.unwrap();
        assert!(r.is_empty(), "expected empty poll after restart, got {r:?}");
        consumer.close().await.unwrap();
        broker.shutdown().await;
    }
}

/// Two Range (eager) consumers share a 2-partition topic. When the second
/// joins, the survivor sheds a partition through the coordinator's *eager*
/// rejoin path; when it leaves, the survivor re-acquires the freed partition
/// through that same path, which primes the re-acquired partition's fetch
/// offset *before* republishing the assignment. Regression coverage for the
/// prime-before-publish ordering in `coordinator.rs`'s eager branch (the
/// cooperative branches are covered by `cooperative_rebalance.rs`). A poll
/// racing the rejoin must not observe a re-acquired partition with no primed
/// offset and fetch it from 0.
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
                .session_timeout(Duration::from_secs(30))
                .rebalance_timeout(Duration::from_secs(2))
                .heartbeat_interval(Duration::from_millis(500))
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
            assert!(
                Instant::now() < settle,
                "group did not split 1+1 (m1={} m2={})",
                m1.assignment().await.len(),
                m2.assignment().await.len()
            );
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
            let _ = m1.poll(Duration::from_millis(200)).await;
            if m1.assignment().await.len() == 2 {
                break;
            }
            assert!(
                Instant::now() < regain,
                "m1 did not re-acquire both partitions, last={}",
                m1.assignment().await.len()
            );
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
        for r in m1.poll(Duration::from_millis(200)).await.unwrap() {
            let v = String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned();
            if v.starts_with('b') {
                second.insert(v);
            }
        }
    }
    assert!(
        second.len() == 2,
        "m1 delivered both second-wave records after re-acquiring: {second:?}"
    );

    m1.close().await.unwrap();
    broker.shutdown().await;
}

/// Regression: a commit issued AFTER a rebalance bumped the group generation
/// must stamp the CURRENT generation, not the start-up snapshot. The commit
/// path used to read a `generation_id` captured at build time and never kept in
/// sync as the coordinator rejoined, so the first commit after any rebalance hit
/// `ILLEGAL_GENERATION (22)` and a long-running block-builder/compactor commit
/// loop crashed (observed as the demo metrics-compactor crash-loop). Now the
/// generation is shared (`Arc<AtomicI32>`) and published by the coordinator on
/// every rejoin, and a rebalance code on commit defers rather than failing.
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
                .session_timeout(Duration::from_secs(30))
                .rebalance_timeout(Duration::from_secs(2))
                .heartbeat_interval(Duration::from_millis(500))
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
            assert!(Instant::now() < settle, "group did not split 1+1");
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
            let _ = m1.poll(Duration::from_millis(200)).await;
            if m1.assignment().await.len() == 2 {
                break;
            }
            assert!(
                Instant::now() < regain,
                "m1 did not re-acquire both partitions"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("m1 reacquired both partitions within 30s");

    // The rejoin bumped the generation, and the accessor reads it live (shared
    // atomic) — proving the generation the commit path stamps is current.
    let gen_after_rejoin = m1.generation_id();
    assert!(
        gen_after_rejoin > gen_after_split,
        "rejoin should advance the generation: split={gen_after_split} rejoin={gen_after_rejoin}",
    );

    // Consume a record on each partition so there are offsets to commit, then
    // commit. Pre-fix this stamped the stale start-up generation and the broker
    // returned ILLEGAL_GENERATION (22) → commit_sync errored → panic here.
    produce_to_partition(&broker, &producer, "genbump", 0, &["g0"]).await;
    produce_to_partition(&broker, &producer, "genbump", 1, &["g1"]).await;
    let mut seen: HashSet<String> = HashSet::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    while seen.len() < 2 && Instant::now() < deadline {
        for r in m1.poll(Duration::from_millis(200)).await.unwrap() {
            let v = String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned();
            if v.starts_with('g') {
                seen.insert(v);
            }
        }
    }
    assert!(seen.len() == 2, "m1 delivered both records: {seen:?}");

    m1.commit_sync()
        .await
        .expect("commit after a generation-bumping rebalance must succeed (current generation)");

    m1.close().await.unwrap();
    broker.shutdown().await;
}

// ── KIP-320 truncation detection (Task 10) ───────────────────────────────────

use crabka_client_consumer::ConsumerError;
use crabka_protocol::owned::delete_records_request::{
    DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
};
use crabka_protocol::owned::offset_fetch_request::{
    OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopics,
};

/// Move a partition's `log_start_offset` forward via `DeleteRecords`
/// (`api_key=21`), dropping every record below `offset`. Returns the
/// resulting `low_watermark`. A consumer positioned below the new log start
/// then sees `OFFSET_OUT_OF_RANGE` on its next `Fetch` — the deterministic
/// way to induce the truncation/divergence KIP-320 handles.
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
    assert!(
        pr.error_code == 0,
        "DeleteRecords error: {:?}",
        pr.error_code
    );
    pr.low_watermark
}

/// `OFFSET_OUT_OF_RANGE` recovery under `auto.offset.reset=latest`.
///
/// A consumer parked at offset 0 has its log trimmed out from under it
/// (`DeleteRecords` moves `log_start` to 5). The next `Fetch` from 0 is below
/// the log start, so the broker returns `OFFSET_OUT_OF_RANGE` (code 1). The
/// error-first poll loop resets per policy: `Latest` plants the `i64::MAX`
/// sentinel, which the following poll resolves to the live log-end via
/// `ListOffsets(-1)`. Records produced after that point are then consumed
/// cleanly — proving genuine recovery (no error, fetch resumes), not a silent
/// stall.
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
    assert!(low == 5, "expected low_watermark 5, got {low}");
    assert!(broker.partition_log_start_for_test("oor-latest", 0) == Some(5));

    let mut consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .client_id("c")
        .group_id("oor-latest-grp")
        .session_timeout(Duration::from_secs(30))
        .rebalance_timeout(Duration::from_secs(2))
        .heartbeat_interval(Duration::from_secs(1))
        .auto_offset_reset(AutoOffsetReset::Latest)
        .subscribe(["oor-latest".to_string()])
        .build()
        .await
        .unwrap();

    // First poll: fetch from 0 → OFFSET_OUT_OF_RANGE → reset to the Latest
    // sentinel. It must NOT surface an error.
    let first = consumer
        .poll(Duration::from_millis(400))
        .await
        .expect("OOR under Latest must reset, not error");
    assert!(
        first.is_empty(),
        "Latest reset jumps to log-end; the pre-trim records must not reappear, got {first:?}"
    );

    // Produce fresh records past the (resolved) log-end; the recovered
    // consumer must deliver exactly these.
    produce(&broker, &producer, "oor-latest", &["NEW1", "NEW2", "NEW3"]).await;
    let mut seen: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline && seen.len() < 3 {
        for r in consumer
            .poll(Duration::from_millis(300))
            .await
            .expect("post-reset poll must not error")
        {
            // Recovery landed at the live log-end (>= 8), never below the trim.
            assert!(
                r.offset >= 5,
                "recovered fetch must be at/after the new log start, got offset {}",
                r.offset
            );
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned());
        }
    }
    assert!(
        seen == vec!["NEW1", "NEW2", "NEW3"],
        "consumer recovered and consumed the post-reset records, got {seen:?}"
    );

    consumer.close().await.unwrap();
    broker.shutdown().await;
}

/// `OFFSET_OUT_OF_RANGE` recovery under `auto.offset.reset=earliest`.
///
/// A consumer parked at offset 0 has its log trimmed out from under it
/// (`DeleteRecords` moves `log_start` to 5). The next `Fetch` from 0 is below
/// the log start, so the broker returns `OFFSET_OUT_OF_RANGE` (code 1). The
/// error-first poll loop resets per policy: `Earliest` must reset to the
/// response's `log_start_offset` (5 in this test), NOT to the literal 0,
/// because resetting to 0 would re-trigger `OFFSET_OUT_OF_RANGE` endlessly
/// (the real root cause this test catches). After recovery the consumer
/// resumes from the new log start and delivers the records that survived the
/// trim, plus any produced afterwards — proving genuine recovery.
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
    assert!(low == 5, "expected low_watermark 5, got {low}");
    assert!(broker.partition_log_start_for_test("oor-earliest", 0) == Some(5));

    let mut consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .client_id("c")
        .group_id("oor-earliest-grp")
        .session_timeout(Duration::from_secs(30))
        .rebalance_timeout(Duration::from_secs(2))
        .heartbeat_interval(Duration::from_secs(1))
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
            .poll(Duration::from_millis(300))
            .await
            .expect("Earliest OOR reset must not error")
        {
            // Every delivered record must be at or after the new log start.
            // If we ever see an offset < 5, the consumer re-fetched from 0
            // instead of from log_start — the original bug.
            assert!(
                r.offset >= 5,
                "recovered Earliest fetch must be at/after log_start (5), got offset {}",
                r.offset
            );
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned());
        }
    }
    assert!(
        seen == vec!["f", "g", "h"],
        "consumer recovered and consumed the surviving records from log_start, got {seen:?}"
    );

    consumer.close().await.unwrap();
    broker.shutdown().await;
}

/// `auto.offset.reset=none` surfaces `OFFSET_OUT_OF_RANGE` as a
/// `ConsumerError::LogTruncation` instead of silently resetting.
///
/// Same induced divergence as the `Latest` test (`DeleteRecords` trims
/// `log_start` past the consumer's offset), but the `None` policy means
/// the OOR arm returns an error rather than a safe offset. `poll()` must
/// propagate `Err(ConsumerError::LogTruncation { .. })` carrying the
/// out-of-range fetch offset and the response's `log_start_offset` as
/// the `safe_offset`.
///
/// A fresh `None` consumer starts at the `i64::MAX` sentinel (resolved to the
/// live log-end), so it would never be out of range. To seat a below-trim
/// position deterministically, an `Earliest` seed consumer first commits
/// offset 0 for the group (BEFORE the trim), and then `DeleteRecords` moves
/// `log_start` forward past that committed 0. The `None` consumer then inherits
/// the below-trim committed offset and hits `OFFSET_OUT_OF_RANGE`.
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
            .session_timeout(Duration::from_secs(30))
            .rebalance_timeout(Duration::from_secs(2))
            .heartbeat_interval(Duration::from_secs(1))
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
                assert!(Instant::now() < settle, "seed assignment did not settle");
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
    assert!(low == 4, "expected low_watermark 4, got {low}");

    let mut consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .client_id("c")
        .group_id("oor-none-grp")
        .session_timeout(Duration::from_secs(30))
        .rebalance_timeout(Duration::from_secs(2))
        .heartbeat_interval(Duration::from_secs(1))
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
        match consumer.poll(Duration::from_millis(300)).await {
            Ok(recs) => {
                assert!(
                    recs.is_empty(),
                    "None policy must not deliver records from a truncated position; got {recs:?}"
                );
            }
            Err(ConsumerError::LogTruncation {
                topic,
                partition,
                fetch_offset,
                ..
            }) => {
                check!(topic == "oor-none");
                check!(partition == 0);
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
    assert!(
        got_truncation,
        "None policy must surface ConsumerError::LogTruncation for the trimmed position"
    );

    consumer.close().await.unwrap();
    broker.shutdown().await;
}

/// The `committed_leader_epoch` consumed at offset-commit survives a broker
/// restart and is readable back through `OffsetFetch` — the round-trip the
/// consumer relies on to seed `positions[..].offset_epoch` so a later
/// leader-epoch bump can trigger KIP-320 validation.
///
/// The records are produced at the partition's natural leader epoch (0), so the
/// consumer's `Fetch` (`current_leader_epoch = 0`, matching the broker) is not
/// epoch-fenced and the consumer observes `ConsumerRecord.leader_epoch == 0`.
/// After consume + commit, the committed epoch must read back as exactly `0`
/// across a broker restart — crucially NOT the `-1` "no epoch committed"
/// sentinel an uncommitted partition yields. That `0 != -1` distinction is what
/// proves the consumer transmitted the consumed epoch through
/// `OffsetCommit` and it round-tripped through `OffsetFetch`; an unrelated
/// never-committed group is checked as the `-1` control.
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
            .session_timeout(Duration::from_secs(30))
            .rebalance_timeout(Duration::from_secs(2))
            .heartbeat_interval(Duration::from_secs(1))
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .subscribe(["epoch-persist".to_string()])
            .build()
            .await
            .unwrap();

        let mut epochs: Vec<i32> = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline && epochs.len() < 3 {
            for r in consumer.poll(Duration::from_millis(300)).await.unwrap() {
                epochs.push(r.leader_epoch);
            }
        }
        assert!(
            epochs == vec![0, 0, 0],
            "consumed records must carry the partition leader epoch 0, got {epochs:?}"
        );

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
        assert!(
            offset == 3,
            "committed offset should be the log end (3), got {offset}"
        );
        assert!(
            epoch == 0,
            "committed_leader_epoch must survive restart as 0 (the consumed epoch), got {epoch}"
        );

        // Control: a never-committed group yields the -1 "absent" sentinel, so
        // the `epoch == 0` above is a real committed value, not a default.
        let (ctrl_offset, ctrl_epoch) = fetch_committed("never-committed-grp").await;
        assert!(
            ctrl_offset == -1 && ctrl_epoch == -1,
            "uncommitted group must read back (-1, -1), got ({ctrl_offset}, {ctrl_epoch})"
        );

        broker.shutdown().await;
    }
}

/// Regression for the WAL-consumer cold-start hang: a single-member group that
/// JOINS before its subscribed topic exists gets a 0-partition assignment, and
/// a Stable one-member group is never sent a broker-driven rebalance — so
/// recovery depends *solely* on the coordinator noticing, via its metadata
/// refresh, that the topic appeared, and rejoining. Without that loop the empty
/// assignment strands forever (the `logs-compactor` / `profiles-block-builder`
/// hang). No second member ever joins, so this isolates the metadata-driven
/// rejoin from the heartbeat-driven `REBALANCE_IN_PROGRESS` path. It also guards
/// the TOCTOU fix: the coordinator seeds its rejoin baseline from the snapshot
/// the *initial* (empty) assignment was computed against, so a topic created any
/// time after the join — including during start-up — is still seen as growth.
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
        .session_timeout(Duration::from_secs(30))
        .heartbeat_interval(Duration::from_millis(500))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe(["wal-late".to_string()])
        .build()
        .await
        .expect("consumer builds even though its subscribed topic does not exist yet");

    // Let the initial join settle, then assert it really is the empty-assignment
    // cold start (a single Stable member with nothing to consume).
    let _ = consumer.poll(Duration::from_millis(200)).await;
    assert!(
        consumer.assignment().await.is_empty(),
        "expected an empty assignment before the topic exists, got len {}",
        consumer.assignment().await.len()
    );

    // The distributor creates the WAL topic AFTER the consumer has joined.
    create_topic_with_partitions(&producer, "wal-late", 2).await;

    // Recovery is driven only by the coordinator's metadata refresh
    // (SUBSCRIPTION_METADATA_REFRESH = 5s) — there is no membership change — so
    // allow a few multiples of that interval on slow CI.
    tokio::time::timeout(Duration::from_secs(30), async {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let _ = consumer.poll(Duration::from_millis(200)).await;
            if consumer.assignment().await.len() == 2 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "consumer never rejoined after its topic appeared (last assignment len {})",
                consumer.assignment().await.len()
            );
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
            for r in consumer.poll(Duration::from_millis(200)).await.unwrap() {
                seen.insert(
                    String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned(),
                );
            }
            assert!(
                Instant::now() < deadline,
                "did not deliver all records from the recovered assignment, seen {seen:?}"
            );
        }
    })
    .await
    .expect("records delivered from the recovered assignment within 30s");

    let expected: HashSet<String> = ["p0a", "p0b", "p1a", "p1b"]
        .into_iter()
        .map(String::from)
        .collect();
    assert!(seen == expected, "expected {expected:?}, got {seen:?}");

    consumer.close().await.unwrap();
    broker.shutdown().await;
}
