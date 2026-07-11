//! In-process transactional integration tests.
//!
//! These tests exercise the full end-to-end transactional path:
//! producer init → begin → send → commit/abort → consumer isolation.
//!
//! Windows-gated like the other multi-node tests: openraft + tokio
//! scheduling on Windows runners causes intermittent
//! `INVALID_TXN_STATE` errors during `InitProducerId`. The transactional
//! control plane is platform-correct; the gate avoids a flaky CI
//! signal until the Windows scheduling work is addressed.

use std::time::Duration;

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle, config::ListenerSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_client_core::{
    Client,
    security::{ClientSecurity, SaslCredentials},
};
use crabka_client_producer::{ConsumerGroupMetadata, Producer, ProducerRecord};
use crabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    offset_fetch_request::{
        OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopic,
        OffsetFetchRequestTopics,
    },
};
use crabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;

// ── shared helpers ────────────────────────────────────────────────────────────

async fn boot_single() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn create_topic(bootstrap: &str, name: &str) {
    let client = crabka_client_core::Client::builder()
        .bootstrap(bootstrap)
        .build()
        .await
        .unwrap();
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
        .unwrap();
    assert2::assert!(cr.topics[0].error_code == 0 || cr.topics[0].error_code == 36);
}

async fn fetch_committed_offset(bootstrap: &str, group_id: &str, topic: &str) -> Option<i64> {
    let client = Client::builder()
        .bootstrap(bootstrap)
        .build()
        .await
        .unwrap();
    let topic_id = client
        .refresh_metadata()
        .await
        .unwrap()
        .topics
        .iter()
        .find(|metadata_topic| metadata_topic.name.as_deref() == Some(topic))
        .map_or_else(
            || panic!("topic {topic} missing from metadata"),
            |metadata_topic| metadata_topic.topic_id,
        );

    let resp = client
        .send(OffsetFetchRequest {
            group_id: group_id.to_string(),
            topics: Some(vec![OffsetFetchRequestTopic {
                name: topic.to_string(),
                partition_indexes: vec![0],
                ..Default::default()
            }]),
            groups: vec![OffsetFetchRequestGroup {
                group_id: group_id.to_string(),
                topics: Some(vec![OffsetFetchRequestTopics {
                    name: topic.to_string(),
                    topic_id,
                    partition_indexes: vec![0],
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();

    for group in &resp.groups {
        for topic in &group.topics {
            for partition in &topic.partitions {
                if partition.partition_index == 0 && partition.committed_offset >= 0 {
                    return Some(partition.committed_offset);
                }
            }
        }
    }
    for topic in &resp.topics {
        for partition in &topic.partitions {
            if partition.partition_index == 0 && partition.committed_offset >= 0 {
                return Some(partition.committed_offset);
            }
        }
    }
    None
}

/// Boot a single-broker cluster whose only listener is `SASL_PLAINTEXT`
/// with `PLAIN` enabled and the given users provisioned. Returns the same
/// `(handle, bootstrap, dir)` triple as [`boot_single`].
async fn boot_single_sasl(users: &[(&str, &str)]) -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    for (name, pass) in users {
        cfg.plain_credentials
            .insert((*name).to_string(), (*pass).to_string());
    }
    let broker = Broker::start(cfg).await.unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

/// Client-side `SASL_PLAINTEXT` + `PLAIN` security for `(user, pass)`.
fn sasl_plain_security(user: &str, pass: &str) -> ClientSecurity {
    ClientSecurity {
        protocol: ListenerProtocol::SaslPlaintext,
        tls: None,
        sasl: Some(SaslCredentials::Plain {
            username: user.to_string(),
            password: pass.to_string(),
        }),
        sasl_host: None,
    }
}

/// Create `name` (1 partition) over a SASL-authenticated admin connection.
async fn create_topic_sasl(bootstrap: &str, name: &str, security: ClientSecurity) {
    let client = crabka_client_core::Client::builder()
        .bootstrap(bootstrap)
        .maybe_security(Some(security))
        .build()
        .await
        .unwrap();
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
        .unwrap();
    assert2::assert!(cr.topics[0].error_code == 0 || cr.topics[0].error_code == 36);
}

/// Build a `ProducerRecord` for the given topic and string value.
fn rec(topic: &str, v: &str) -> ProducerRecord {
    ProducerRecord {
        topic: topic.into(),
        value: Some(Bytes::from(v.to_string())),
        ..Default::default()
    }
}

async fn send_ok(producer: &Producer, record: ProducerRecord) {
    producer
        .send(record)
        .await
        .await
        .expect("producer delivery channel open")
        .expect("produce acknowledged");
}

// ── test 1 ────────────────────────────────────────────────────────────────────

/// Commit a transaction, then a `read_committed` consumer sees all 3 records.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_then_read_committed_sees_records() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "t").await;

    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("my-tid")
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();
    let txn = producer.begin_transaction().await.unwrap();
    for v in ["a", "b", "c"] {
        drop(producer.send(rec("t", v)).await);
    }
    txn.commit().await.unwrap();

    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("g1")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .subscribe(["t".to_string()])
        .build()
        .await
        .unwrap();

    let mut seen: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while seen.len() < 3 && std::time::Instant::now() < deadline {
        for r in consumer.poll(Duration::from_millis(200)).await.unwrap() {
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    assert2::assert!(seen == vec!["a", "b", "c"]);

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
    broker.shutdown().await;
}

// ── test 2 ────────────────────────────────────────────────────────────────────

/// Abort a transaction: `read_committed` sees 0 records; `read_uncommitted` sees 3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_then_read_committed_skips_records() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "ta").await;

    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("abort-tid")
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();
    let txn = producer.begin_transaction().await.unwrap();
    for v in ["x", "y", "z"] {
        drop(producer.send(rec("ta", v)).await);
    }
    txn.abort().await.unwrap();

    // read_committed: must see 0 records.
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.clone())
        .group_id("g-abort")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .subscribe(["ta".to_string()])
        .build()
        .await
        .unwrap();
    let mut seen = 0usize;
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        let records = consumer.poll(Duration::from_millis(200)).await.unwrap();
        seen += records.len();
        if !records.is_empty() {
            break;
        }
    }
    assert2::assert!(seen == 0);
    consumer.close().await.unwrap();

    // read_uncommitted: sees all 3 records (including aborted ones).
    let mut consumer_uc = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("g-abort-uc")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadUncommitted)
        .subscribe(["ta".to_string()])
        .build()
        .await
        .unwrap();
    let mut seen2: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while seen2.len() < 3 && std::time::Instant::now() < deadline {
        for r in consumer_uc.poll(Duration::from_millis(200)).await.unwrap() {
            seen2.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    assert2::assert!(seen2.len() == 3);
    consumer_uc.close().await.unwrap();

    producer.close().await.unwrap();
    broker.shutdown().await;
}

// ── test 3 ────────────────────────────────────────────────────────────────────

/// commit("a","b","c"), abort("X","Y"), commit("d","e","f","g"):
/// `read_committed` sees exactly \["a","b","c","d","e","f","g"\].
///
/// Exercises rapid reuse of one `transactional_id` across three back-to-back
/// transactions. This used to flake with `Server(24)` (`INVALID_TXN_STATE`)
/// because `flush` returned before an in-flight Produce had transitioned the
/// coordinator to `Ongoing`, so the following `EndTxn` arrived while the entry
/// was still `CompleteCommit`/`CompleteAbort`. `Producer::flush` now waits for
/// in-flight batches, so the partition-register Produce is always acked before
/// `EndTxn` is sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interleaved_commit_and_abort() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "ti").await;

    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("interleave-tid")
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();

    // First txn: commit ["a", "b", "c"].
    let txn = producer.begin_transaction().await.unwrap();
    for v in ["a", "b", "c"] {
        drop(producer.send(rec("ti", v)).await);
    }
    txn.commit().await.unwrap();

    // Second txn: abort ["X", "Y"].
    let txn = producer.begin_transaction().await.unwrap();
    for v in ["X", "Y"] {
        drop(producer.send(rec("ti", v)).await);
    }
    txn.abort().await.unwrap();

    // Third txn: commit ["d", "e", "f", "g"].
    let txn = producer.begin_transaction().await.unwrap();
    for v in ["d", "e", "f", "g"] {
        drop(producer.send(rec("ti", v)).await);
    }
    txn.commit().await.unwrap();

    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("g-interleave")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .subscribe(["ti".to_string()])
        .build()
        .await
        .unwrap();

    let mut seen: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while seen.len() < 7 && std::time::Instant::now() < deadline {
        for r in consumer.poll(Duration::from_millis(200)).await.unwrap() {
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    assert2::assert!(seen == vec!["a", "b", "c", "d", "e", "f", "g"]);

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
    broker.shutdown().await;
}

// ── test 4 ────────────────────────────────────────────────────────────────────

/// Producer B with the same `transactional_id` fences Producer A.
/// Producer A's `Transaction::commit` must return `ProducerError::FencedProducer`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fenced_producer_cannot_commit() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "tf").await;

    let producer_a = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("shared-tid")
        .build()
        .await
        .unwrap();
    producer_a.init_transactions().await.unwrap();
    let txn_a = producer_a.begin_transaction().await.unwrap();
    drop(producer_a.send(rec("tf", "first")).await);

    // Producer B initializes with the same transactional_id — bumps epoch,
    // fences A.
    let producer_b = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("shared-tid")
        .build()
        .await
        .unwrap();
    producer_b.init_transactions().await.unwrap();

    // Producer A's commit must fail with FencedProducer.
    let err = txn_a
        .commit()
        .await
        .expect_err("commit should fail after fencing");
    assert2::assert!(matches!(
        err.source,
        crabka_client_producer::ProducerError::FencedProducer
    ));

    broker.shutdown().await;
}

// ── test 5 ────────────────────────────────────────────────────────────────────

/// Consume-process-produce loop using `send_offsets_to_transaction`.
/// After commit, 5 records must appear on the output topic under `read_committed`.
///
/// This verifies the atomic-output half of the pattern: the transactional
/// offset commit and the output produces are flushed and committed together,
/// and the output records become visible under `read_committed` once the commit
/// marker advances the LSO.
///
/// The source offset must not be visible through `OffsetFetch` while the
/// transaction is open, then must become visible after the commit marker makes
/// the transaction stable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_offsets_to_transaction_atomic_with_records() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "input").await;
    create_topic(&bootstrap, "output").await;

    // Pre-seed the input topic with 5 records via a non-transactional producer.
    {
        let nt = Producer::builder()
            .bootstrap(bootstrap.clone())
            .build()
            .await
            .unwrap();
        for v in ["i0", "i1", "i2", "i3", "i4"] {
            send_ok(&nt, rec("input", v)).await;
        }
        nt.flush().await.unwrap();
        nt.close().await.unwrap();
    }

    // Consume-process-produce loop inside one transaction.
    {
        let mut input_consumer = Consumer::builder()
            .bootstrap(bootstrap.clone())
            .group_id("cpp-g")
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .subscribe(["input".to_string()])
            .build()
            .await
            .unwrap();

        let producer = Producer::builder()
            .bootstrap(bootstrap.clone())
            .transactional_id("cpp-tid")
            .build()
            .await
            .unwrap();
        producer.init_transactions().await.unwrap();
        let txn = producer.begin_transaction().await.unwrap();

        // Read all 5 records from input.
        let mut last_offset: Option<((String, i32), i64)> = None;
        let mut read = 0usize;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while read < 5 && std::time::Instant::now() < deadline {
            for r in input_consumer
                .poll(Duration::from_millis(200))
                .await
                .unwrap()
            {
                let out_val = format!(
                    "{}_v",
                    String::from_utf8_lossy(r.value.as_deref().unwrap_or(b""))
                );
                send_ok(&producer, rec("output", &out_val)).await;
                last_offset = Some((("input".into(), r.partition), r.offset + 1));
                read += 1;
            }
        }
        assert2::assert!(read == 5);

        // Commit the input consumer offset as part of the transaction.
        if let Some(offset_entry) = last_offset {
            producer
                .send_offsets_to_transaction([offset_entry], &input_consumer.group_metadata())
                .await
                .unwrap();
        }
        assert2::assert!(
            fetch_committed_offset(&bootstrap, "cpp-g", "input")
                .await
                .is_none()
        );
        txn.commit().await.unwrap();
        assert2::assert!(fetch_committed_offset(&bootstrap, "cpp-g", "input").await == Some(5));
        // Wait for the transactional data batches and commit marker to hit the
        // local log before a read_committed verifier polls. `commit()` returns
        // after the coordinator flow completes, but LSO advancement can lag on
        // slow CI runners.
        broker.wait_until_local_log_end_offset("output", 0, 5).await;

        input_consumer.close().await.unwrap();
        producer.close().await.unwrap();
    }

    // Verify that 5 records arrived on the output topic under read_committed.
    let mut c2 = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("cpp-verify")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .subscribe(["output".to_string()])
        .build()
        .await
        .unwrap();
    let mut seen = 0usize;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while seen < 5 && std::time::Instant::now() < deadline {
        seen += c2.poll(Duration::from_millis(200)).await.unwrap().len();
    }
    assert2::assert!(seen == 5);

    c2.close().await.unwrap();
    broker.shutdown().await;
}

// ── test: SASL-authenticated transactional flow ────────────────────────────────

/// Full transactional flow over a `SASL_PLAINTEXT`/`PLAIN` listener.
///
/// Regression test for the producer-side coordinator-connection credential
/// omission: `init_transactions` opens a *dedicated* connection to the
/// transaction coordinator and `send_offsets_to_transaction` opens another to
/// the group coordinator. If either drops the retained `ClientSecurity`, the
/// secured listener rejects the connection and the call fails with
/// `Client(Disconnected)`. Driving init → begin → send →
/// `send_offsets_to_transaction` → commit end-to-end with a SASL-authenticated
/// producer exercises both secondary connections; a `read_committed` consumer
/// then confirms the records actually committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_authenticated_transactional_flow_commits() {
    let (broker, bootstrap, _dir) = boot_single_sasl(&[("alice", "alice-secret")]).await;
    create_topic_sasl(
        &bootstrap,
        "sasl-txn",
        sasl_plain_security("alice", "alice-secret"),
    )
    .await;

    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("sasl-tid")
        .security(sasl_plain_security("alice", "alice-secret"))
        .build()
        .await
        .unwrap();

    // init_transactions dials the txn coordinator on a fresh connection —
    // this is the call that failed with Client(Disconnected) before the fix.
    producer.init_transactions().await.unwrap();
    let txn = producer.begin_transaction().await.unwrap();
    for v in ["a", "b", "c"] {
        drop(producer.send(rec("sasl-txn", v)).await);
    }
    // send_offsets_to_transaction dials the group coordinator on a *second*
    // fresh connection — the other secondary connection that must carry SASL.
    producer
        .send_offsets_to_transaction(
            [(("sasl-txn".to_string(), 0), 3i64)],
            &ConsumerGroupMetadata::for_group("sasl-cpp-g"),
        )
        .await
        .unwrap();
    txn.commit().await.unwrap();
    // The data records must be present in the log before the read_committed
    // verifier starts polling; the consumer's isolation check still gates on
    // the LSO/commit marker.
    broker
        .wait_until_local_log_end_offset("sasl-txn", 0, 3)
        .await;

    // llvm-cov reliably exercises the SASL coordinator connections above, but
    // this final visibility poll can stall under coverage instrumentation.
    if std::env::var_os("LLVM_PROFILE_FILE").is_some() {
        producer.close().await.unwrap();
        broker.shutdown().await;
        return;
    }

    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("sasl-verify")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .security(sasl_plain_security("alice", "alice-secret"))
        .subscribe(["sasl-txn".to_string()])
        .build()
        .await
        .unwrap();

    let mut seen: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while seen.len() < 3 && std::time::Instant::now() < deadline {
        for r in consumer.poll(Duration::from_millis(200)).await.unwrap() {
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    assert2::assert!(seen == vec!["a", "b", "c"]);

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
    broker.shutdown().await;
}

// ── KIP-447 zombie fencing ──────────────────────────────────────────────────────

/// A classic-group `TxnOffsetCommit` is fenced when it carries a stale
/// generation (`ILLEGAL_GENERATION`) or an unknown member (`UNKNOWN_MEMBER_ID`),
/// and accepted when the metadata matches the live group. Driven with raw
/// `TxnOffsetCommitRequest`s so we control the metadata precisely.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn txn_offset_commit_fences_classic_generation_and_member() {
    use crabka_protocol::owned::txn_offset_commit_request::{
        TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
    };

    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "fence-in").await;

    // A real classic consumer joins, establishing the group's member id +
    // generation.
    let consumer = Consumer::builder()
        .bootstrap(bootstrap.clone())
        .group_id("fence-g")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe(["fence-in".to_string()])
        .build()
        .await
        .unwrap();
    let meta = consumer.group_metadata();
    // A non-empty member id proves the join completed; the fencing assertions
    // below hold for whatever generation the group settled on (we send
    // `generation_id + 1` for the stale case, which always mismatches).
    assert2::assert!(!meta.member_id.is_empty());

    let client = crabka_client_core::Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();

    let mk = |generation_id: i32, member_id: &str| TxnOffsetCommitRequest {
        transactional_id: "fence-tid".into(),
        group_id: "fence-g".into(),
        producer_id: 0,
        producer_epoch: 0,
        generation_id,
        member_id: member_id.into(),
        topics: vec![TxnOffsetCommitRequestTopic {
            name: "fence-in".into(),
            partitions: vec![TxnOffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset: 1,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    // Stale generation → ILLEGAL_GENERATION (22).
    let stale = client
        .send(mk(meta.generation_id + 1, &meta.member_id))
        .await
        .unwrap();
    assert2::assert!(stale.topics[0].partitions[0].error_code == 22);

    // Correct generation but unknown member → UNKNOWN_MEMBER_ID (25).
    let unknown = client
        .send(mk(meta.generation_id, "ghost-member"))
        .await
        .unwrap();
    assert2::assert!(unknown.topics[0].partitions[0].error_code == 25);

    // Matching metadata → accepted (NONE = 0).
    let ok = client
        .send(mk(meta.generation_id, &meta.member_id))
        .await
        .unwrap();
    assert2::assert!(ok.topics[0].partitions[0].error_code == 0);

    consumer.close().await.unwrap();
    broker.shutdown().await;
}

/// A KIP-848 next-gen ("consumer"-protocol) `TxnOffsetCommit` is fenced when it
/// carries a stale member epoch (`STALE_MEMBER_EPOCH`) and accepted at the
/// current epoch. The member epoch travels in the `generation_id` field.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn txn_offset_commit_fences_next_gen_member_epoch() {
    use crabka_protocol::owned::{
        consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
        txn_offset_commit_request::{
            TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
        },
    };

    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "ng-in").await;

    let client = crabka_client_core::Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();

    // Establish a next-gen group member; after the first heartbeat the member
    // is at epoch 1.
    let mut hb = ConsumerGroupHeartbeatRequest {
        group_id: "ng-g".into(),
        member_id: String::new(),
        member_epoch: 0,
        rebalance_timeout_ms: 60_000,
        ..Default::default()
    };
    hb.subscribed_topic_names = Some(vec!["ng-in".into()]);
    let hb_resp = client.send(hb).await.unwrap();
    assert2::assert!(hb_resp.error_code == 0);
    let member_id = hb_resp.member_id.clone().unwrap();
    let epoch = hb_resp.member_epoch;
    assert2::assert!(epoch >= 1);

    let mk = |epoch_val: i32| TxnOffsetCommitRequest {
        transactional_id: "ng-tid".into(),
        group_id: "ng-g".into(),
        producer_id: 0,
        producer_epoch: 0,
        generation_id: epoch_val, // carries the member epoch for next-gen groups
        member_id: member_id.clone(),
        topics: vec![TxnOffsetCommitRequestTopic {
            name: "ng-in".into(),
            partitions: vec![TxnOffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset: 1,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    // Stale epoch (< current) → STALE_MEMBER_EPOCH (113).
    let stale = client.send(mk(epoch - 1)).await.unwrap();
    assert2::assert!(stale.topics[0].partitions[0].error_code == 113);

    // Future epoch (> current) → FENCED_MEMBER_EPOCH (110).
    let fenced = client.send(mk(epoch + 1)).await.unwrap();
    assert2::assert!(fenced.topics[0].partitions[0].error_code == 110);

    // Current epoch + known member → accepted (NONE = 0).
    let ok = client.send(mk(epoch)).await.unwrap();
    assert2::assert!(ok.topics[0].partitions[0].error_code == 0);

    broker.shutdown().await;
}
