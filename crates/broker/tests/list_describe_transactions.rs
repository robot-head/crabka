// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files.
#![allow(clippy::pedantic)]

//! KIP-664 `ListTransactions` (api_key 66) + `DescribeTransactions`
//! (api_key 65). Both surface the broker's local `TxnCoordinator`
//! state — `(transactional_id, producer_id, state)` summary for List,
//! full per-tid detail (timeout, start time, partitions) for Describe.

use std::time::Duration;

use assert2::check;
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_producer::{Producer, ProducerRecord};
use crabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    describe_transactions_request::DescribeTransactionsRequest,
    list_transactions_request::ListTransactionsRequest,
};
use tempfile::TempDir;

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

fn rec(topic: &str, v: &str) -> ProducerRecord {
    ProducerRecord {
        topic: topic.into(),
        value: Some(Bytes::from(v.to_string())),
        ..Default::default()
    }
}

async fn admin_client(bootstrap: &str) -> crabka_client_core::Client {
    crabka_client_core::Client::builder()
        .bootstrap(bootstrap)
        .build()
        .await
        .unwrap()
}

/// Boot a broker, init a transactional producer with the given tid,
/// begin a txn, and produce one record to `topic`. Leaves the txn
/// in `Ongoing` so the admin APIs can see it. Returns the broker +
/// producer (caller closes both).
async fn boot_with_ongoing_txn(
    tid: &str,
    topic: &str,
) -> (BrokerHandle, String, TempDir, Producer) {
    let (broker, bootstrap, dir) = boot_single().await;
    create_topic(&bootstrap, topic).await;

    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id(tid)
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();
    let _ = producer.begin_transaction().await.unwrap();
    // `send` enqueues into the producer's local batch; without
    // `flush()` the AddPartitionsToTxn round-trip that registers
    // `(topic, partition)` on the coordinator's TxnEntry may not run
    // before the admin call. The drop pattern around `send` is
    // intentional — it's a future-of-record-metadata handle we don't
    // need (commits will never fire on this test path).
    drop(producer.send(rec(topic, "v")).await);
    producer.flush().await.unwrap();
    // Don't commit/abort — we want the txn to stay Ongoing.

    (broker, bootstrap, dir, producer)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_transactions_returns_ongoing_txn() {
    let (broker, bootstrap, _dir, producer) = boot_with_ongoing_txn("my-tid", "t-list").await;

    let client = admin_client(&bootstrap).await;
    // Retry briefly — AddPartitionsToTxn races the visibility of the
    // partitions set in the coordinator's TxnEntry. macOS runners are
    // measurably slower than ubuntu/linux on the in-process
    // transactional path, so the deadline is generous.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let resp = loop {
        let r = client
            .send(ListTransactionsRequest::default())
            .await
            .expect("ListTransactions");
        if !r.transaction_states.is_empty() {
            break r;
        }
        if std::time::Instant::now() > deadline {
            panic!("ListTransactions never saw the ongoing txn: {r:?}");
        }
        // intentional: polls RPC response until the txn is visible as Ongoing
        // (coordinator TxnEntry state after AddPartitionsToTxn); no
        // metadata-image signal for this
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    let row = resp
        .transaction_states
        .iter()
        .find(|r| r.transactional_id == "my-tid")
        .expect("my-tid not present");
    check!(
        (
            resp.error_code,
            row.transaction_state.as_str(),
            row.producer_id > 0,
            resp.unknown_state_filters.is_empty(),
        ) == (0, "Ongoing", true, true),
        "no filters sent → no unknowns: {:?}",
        resp.unknown_state_filters,
    );

    producer.close().await.unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_transactions_state_filter_excludes_non_matching() {
    let (broker, bootstrap, _dir, producer) =
        boot_with_ongoing_txn("my-tid", "t-state-filter").await;

    let client = admin_client(&bootstrap).await;
    // Filter to "Empty" only — our txn is Ongoing, so the row should
    // be excluded.
    let r = client
        .send(ListTransactionsRequest {
            state_filters: vec!["Empty".into()],
            ..Default::default()
        })
        .await
        .expect("ListTransactions(state=Empty)");
    assert2::assert!(
        (
            r.error_code,
            r.transaction_states
                .iter()
                .all(|t| t.transactional_id != "my-tid"),
        ) == (0, true)
    );

    producer.close().await.unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_transactions_reports_unknown_state_filters() {
    let (broker, bootstrap, _dir) = boot_single().await;

    let client = admin_client(&bootstrap).await;
    let r = client
        .send(ListTransactionsRequest {
            state_filters: vec!["Ongoing".into(), "BogusState".into(), "Empty".into()],
            ..Default::default()
        })
        .await
        .expect("ListTransactions");
    // The known names round-trip silently; the bogus one rides on the
    // unknown_state_filters echo per KIP-664.
    assert2::assert!(
        (
            r.error_code,
            r.unknown_state_filters.len(),
            r.unknown_state_filters[0].as_str(),
        ) == (0, 1, "BogusState")
    );

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_transactions_returns_full_state_for_known_tid() {
    let (broker, bootstrap, _dir, producer) =
        boot_with_ongoing_txn("describe-tid", "t-describe").await;

    let client = admin_client(&bootstrap).await;
    // 30 s deadline matches `list_transactions_returns_ongoing_txn` —
    // the txn's `Ongoing` state + partition set both ride on the same
    // AddPartitionsToTxn round-trip the producer's `send()` triggers,
    // and macOS scheduling can be slow.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let row = loop {
        let r = client
            .send(DescribeTransactionsRequest {
                transactional_ids: vec!["describe-tid".into()],
                ..Default::default()
            })
            .await
            .expect("DescribeTransactions");
        assert2::assert!(r.transaction_states.len() == 1);
        let row = &r.transaction_states[0];
        if row.error_code == 0 && !row.topics.is_empty() {
            break row.clone();
        }
        if std::time::Instant::now() > deadline {
            panic!("Ongoing txn never showed its partitions: {row:?}");
        }
        // intentional: polls RPC response until the txn shows its registered
        // partitions (coordinator TxnEntry state after AddPartitionsToTxn); no
        // metadata-image signal for this
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    // Exactly one topic + partition, since we produced one record to
    // a 1-partition topic.
    check!(
        (
            row.transactional_id.as_str(),
            row.transaction_state.as_str(),
            row.producer_id > 0,
            row.transaction_timeout_ms,
            row.transaction_start_time_ms > 0,
            row.topics.len(),
            row.topics[0].topic.as_str(),
            row.topics[0].partitions.as_slice(),
        ) == (
            "describe-tid",
            "Ongoing",
            true,
            60_000,
            true,
            1,
            "t-describe",
            &[0i32][..],
        )
    );

    producer.close().await.unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_transactions_returns_not_found_for_unknown_tid() {
    let (broker, bootstrap, _dir) = boot_single().await;

    let client = admin_client(&bootstrap).await;
    let r = client
        .send(DescribeTransactionsRequest {
            transactional_ids: vec!["ghost-tid".into()],
            ..Default::default()
        })
        .await
        .expect("DescribeTransactions");
    check!(
        (
            r.transaction_states.len(),
            r.transaction_states[0].error_code,
            r.transaction_states[0].transactional_id.as_str(),
        ) == (1, 75, "ghost-tid"),
        "expected TRANSACTIONAL_ID_NOT_FOUND (75), got {:?}",
        r.transaction_states[0]
    );

    broker.shutdown().await;
}
