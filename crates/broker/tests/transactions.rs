//! In-process transactional integration tests.
//!
//! These tests exercise the full end-to-end transactional path: producer
//! init, begin, send, commit or abort, then consumer isolation.
//!
//! They are gated off Windows, like the other multi-node tests. openraft and
//! tokio scheduling on Windows runners cause intermittent
//! `INVALID_TXN_STATE` errors during `InitProducerId`. The transactional
//! control plane is correct on every platform. The gate avoids a flaky CI
//! signal until the Windows scheduling work is done.

use std::time::Duration;

use assert2::assert;
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle, config::ListenerSpec};
use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_client_core::security::{ClientSecurity, SaslCredentials};
use crabka_client_producer::{ConsumerGroupMetadata, Producer, ProducerRecord};
use crabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    find_coordinator_request::FindCoordinatorRequest,
    init_producer_id_request::InitProducerIdRequest,
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
    assert!(
        cr.topics[0].error_code == 0 || cr.topics[0].error_code == 36,
        "create_topic {name}: error_code={}",
        cr.topics[0].error_code
    );
}

async fn init_transaction(
    client: &crabka_client_core::Client,
    transactional_id: &str,
) -> (i64, i16) {
    let coordinator = client
        .send(FindCoordinatorRequest {
            key: transactional_id.into(),
            key_type: 1,
            coordinator_keys: vec![transactional_id.into()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        coordinator.error_code == 0
            || coordinator
                .coordinators
                .iter()
                .all(|entry| entry.error_code == 0),
        "FindCoordinator: {coordinator:?}"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let response = client
            .send(InitProducerIdRequest {
                transactional_id: Some(transactional_id.into()),
                transaction_timeout_ms: 60_000,
                ..Default::default()
            })
            .await
            .unwrap();
        if response.error_code == 0 {
            return (response.producer_id, response.producer_epoch);
        }
        assert!(
            response.error_code == 15 || response.error_code == 16,
            "InitProducerId: {response:?}"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "InitProducerId coordinator did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Boots a single-broker cluster whose only listener is `SASL_PLAINTEXT`, with
/// `PLAIN` enabled and the given users provisioned. Returns the same
/// `(handle, bootstrap, dir)` triple as [`boot_single`].
fn boot_single_sasl(
    users: &[(&str, &str)],
) -> impl std::future::Future<Output = (BrokerHandle, String, TempDir)> {
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
    Box::pin(async move {
        let broker = Broker::start(cfg).await.unwrap();
        let bootstrap = broker.listen_addr().to_string();
        (broker, bootstrap, dir)
    })
}

/// Client-side `SASL_PLAINTEXT` and `PLAIN` security for `(user, pass)`.
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

/// Creates the topic `name` with 1 partition over a SASL-authenticated admin
/// connection.
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
    assert!(
        cr.topics[0].error_code == 0 || cr.topics[0].error_code == 36,
        "create_topic_sasl {name}: error_code={}",
        cr.topics[0].error_code
    );
}

/// Builds a `ProducerRecord` for the given topic and string value.
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

/// Commits a transaction, after which a `read_committed` consumer sees all 3
/// records.
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
        for r in consumer.poll(crabka_units::millis(200)).await.unwrap() {
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    assert!(seen == vec!["a", "b", "c"]);

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
    broker.shutdown().await;
}

// ── test 2 ────────────────────────────────────────────────────────────────────

/// Aborts a transaction. `read_committed` then sees 0 records, and
/// `read_uncommitted` sees 3.
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
        let records = consumer.poll(crabka_units::millis(200)).await.unwrap();
        seen += records.len();
        if !records.is_empty() {
            break;
        }
    }
    assert!(seen == 0, "read_committed must skip aborted records");
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
        for r in consumer_uc.poll(crabka_units::millis(200)).await.unwrap() {
            seen2.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    assert!(
        seen2.len() == 3,
        "read_uncommitted must see aborted records"
    );
    consumer_uc.close().await.unwrap();

    producer.close().await.unwrap();
    broker.shutdown().await;
}

// ── test 3 ────────────────────────────────────────────────────────────────────

/// commit("a","b","c"), abort("X","Y"), commit("d","e","f","g"):
/// `read_committed` sees exactly \["a","b","c","d","e","f","g"\].
///
/// Exercises rapid reuse of one `transactional_id` across three back-to-back
/// transactions. This used to flake with `Server(48)` (`INVALID_TXN_STATE`)
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
        for r in consumer.poll(crabka_units::millis(200)).await.unwrap() {
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    assert!(seen == vec!["a", "b", "c", "d", "e", "f", "g"]);

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
    broker.shutdown().await;
}

// ── test 4 ────────────────────────────────────────────────────────────────────

/// Producer B with the same `transactional_id` fences Producer A. Producer A's
/// `Transaction::commit` must return `ProducerError::FencedProducer`.
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
    assert!(
        matches!(
            err.source,
            crabka_client_producer::ProducerError::FencedProducer
        ),
        "expected FencedProducer, got: {err:?}"
    );

    broker.shutdown().await;
}

// ── test 5 ────────────────────────────────────────────────────────────────────

/// Consume-process-produce loop with `send_offsets_to_transaction`. After the
/// commit, 5 records must appear on the output topic under `read_committed`.
///
/// This verifies the atomic-output half of the pattern: the transactional
/// offset commit and the output produces are flushed and committed together,
/// and the output records become visible under `read_committed` once the commit
/// marker advances the LSO. `txn_offset_commit_materialize.rs` separately
/// verifies that the same marker makes committed offsets visible through
/// `OffsetFetch` and drops them on abort.
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
                .poll(crabka_units::millis(200))
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
        assert!(read == 5, "expected to read 5 input records");

        // Commit the input consumer offset as part of the transaction.
        if let Some(offset_entry) = last_offset {
            producer
                .send_offsets_to_transaction([offset_entry], &input_consumer.group_metadata())
                .await
                .unwrap();
        }
        txn.commit().await.unwrap();
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
        seen += c2.poll(crabka_units::millis(200)).await.unwrap().len();
    }
    assert!(seen == 5, "expected 5 records on output topic");

    c2.close().await.unwrap();
    broker.shutdown().await;
}

// ── test: SASL-authenticated transactional flow ────────────────────────────────

/// Full transactional flow over a `SASL_PLAINTEXT`/`PLAIN` listener.
///
/// Regression test for a producer-side coordinator-connection credential
/// omission. `init_transactions` opens a *dedicated* connection to the
/// transaction coordinator, and `send_offsets_to_transaction` opens another one
/// to the group coordinator. If either drops the retained `ClientSecurity`, the
/// secured listener rejects the connection and the call fails with
/// `Client(Disconnected)`.
///
/// The test drives init, begin, send, `send_offsets_to_transaction`, and commit
/// end to end with a SASL-authenticated producer, which exercises both
/// secondary connections. A `read_committed` consumer then confirms that the
/// records committed.
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
        for r in consumer.poll(crabka_units::millis(200)).await.unwrap() {
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    assert!(seen == vec!["a", "b", "c"], "seen={seen:?}");

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
    broker.shutdown().await;
}

// ── KIP-447 zombie fencing ──────────────────────────────────────────────────────

/// The broker fences a classic-group `TxnOffsetCommit` when it carries a stale
/// generation (`ILLEGAL_GENERATION`) or an unknown member
/// (`UNKNOWN_MEMBER_ID`), and accepts it when the metadata matches the live
/// group. The test uses raw `TxnOffsetCommitRequest` values, which give it
/// precise control over the metadata.
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
    assert!(
        !meta.member_id.is_empty(),
        "consumer should have a member id: {meta:?}"
    );

    let client = crabka_client_core::Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let (producer_id, producer_epoch) = init_transaction(&client, "fence-tid").await;

    let mk = |generation_id: i32, member_id: &str| TxnOffsetCommitRequest {
        transactional_id: "fence-tid".into(),
        group_id: "fence-g".into(),
        producer_id,
        producer_epoch,
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
    assert!(
        stale.topics[0].partitions[0].error_code == 22,
        "stale generation should be ILLEGAL_GENERATION: {stale:?}"
    );

    // Correct generation but unknown member → UNKNOWN_MEMBER_ID (25).
    let unknown = client
        .send(mk(meta.generation_id, "ghost-member"))
        .await
        .unwrap();
    assert!(
        unknown.topics[0].partitions[0].error_code == 25,
        "unknown member should be UNKNOWN_MEMBER_ID: {unknown:?}"
    );

    // Matching metadata → accepted (NONE = 0).
    let ok = client
        .send(mk(meta.generation_id, &meta.member_id))
        .await
        .unwrap();
    assert!(
        ok.topics[0].partitions[0].error_code == 0,
        "valid metadata should commit: {ok:?}"
    );

    consumer.close().await.unwrap();
    broker.shutdown().await;
}

/// The broker fences a KIP-848 next-gen "consumer"-protocol `TxnOffsetCommit`
/// when it carries a stale member epoch (`STALE_MEMBER_EPOCH`), and accepts it
/// at the current epoch. The member epoch travels in the `generation_id`
/// field.
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
    let (producer_id, producer_epoch) = init_transaction(&client, "ng-tid").await;

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
    assert!(hb_resp.error_code == 0, "heartbeat failed: {hb_resp:?}");
    let member_id = hb_resp.member_id.clone().unwrap();
    let epoch = hb_resp.member_epoch;
    assert!(
        epoch >= 1,
        "member should have a positive epoch: {hb_resp:?}"
    );

    let mk = |epoch_val: i32| TxnOffsetCommitRequest {
        transactional_id: "ng-tid".into(),
        group_id: "ng-g".into(),
        producer_id,
        producer_epoch,
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
    assert!(
        stale.topics[0].partitions[0].error_code == 113,
        "stale epoch should be STALE_MEMBER_EPOCH: {stale:?}"
    );

    // Future epoch (> current) → FENCED_MEMBER_EPOCH (110).
    let fenced = client.send(mk(epoch + 1)).await.unwrap();
    assert!(
        fenced.topics[0].partitions[0].error_code == 110,
        "future epoch should be FENCED_MEMBER_EPOCH: {fenced:?}"
    );

    // Current epoch + known member → accepted (NONE = 0).
    let ok = client.send(mk(epoch)).await.unwrap();
    assert!(
        ok.topics[0].partitions[0].error_code == 0,
        "current epoch should commit: {ok:?}"
    );

    broker.shutdown().await;
}
