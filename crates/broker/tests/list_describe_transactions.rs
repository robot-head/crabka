// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files.

//! KIP-664 `ListTransactions` (`api_key` 66) and `DescribeTransactions`
//! (`api_key` 65). Both surface the broker's local `TxnCoordinator`
//! state. List returns a `(transactional_id, producer_id, state)` summary.
//! Describe returns the full per-tid detail: timeout, start time, and
//! partitions.

use std::{sync::Arc, time::Duration};

use assert2::{assert, check};
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_producer::{OwnedTransaction, Producer, ProducerRecord};
use crabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    describe_transactions_request::DescribeTransactionsRequest,
    describe_transactions_response::{TopicData, TransactionState as DescribedTransactionState},
    list_transactions_request::ListTransactionsRequest,
    list_transactions_response::{
        ListTransactionsResponse, TransactionState as ListedTransactionState,
    },
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
    assert!(
        cr.topics[0].error_code == 0 || cr.topics[0].error_code == 36,
        "create_topic {name}: error_code={}",
        cr.topics[0].error_code
    );
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
/// begin a txn, and produce one record to `topic`. It leaves the txn
/// in `Ongoing` so the admin APIs can see it. It returns the broker and the
/// producer, and the caller closes both.
async fn boot_with_ongoing_txn(
    tid: &str,
    topic: &str,
) -> (
    BrokerHandle,
    String,
    TempDir,
    Arc<Producer>,
    OwnedTransaction,
) {
    let (broker, bootstrap, dir) = boot_single().await;
    create_topic(&bootstrap, topic).await;

    let producer = Arc::new(
        Producer::builder()
            .bootstrap(bootstrap.clone())
            .transactional_id(tid)
            .build()
            .await
            .unwrap(),
    );
    producer.init_transactions().await.unwrap();
    let transaction = Arc::clone(&producer)
        .begin_transaction_owned()
        .await
        .unwrap();
    // `send` enqueues into the producer's local batch; without
    // `flush()` the AddPartitionsToTxn round-trip that registers
    // `(topic, partition)` on the coordinator's TxnEntry may not run
    // before the admin call. The drop pattern around `send` is
    // intentional — it's a future-of-record-metadata handle we don't
    // need (commits will never fire on this test path).
    drop(producer.send(rec(topic, "v")).await);
    producer.flush().await.unwrap();
    // Don't commit/abort — we want the txn to stay Ongoing.

    (broker, bootstrap, dir, producer, transaction)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_transactions_returns_ongoing_txn() {
    let (broker, bootstrap, _dir, producer, transaction) =
        boot_with_ongoing_txn("my-tid", "t-list").await;

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
        let ongoing = match r.transaction_states.as_slice() {
            [row] => {
                *row == ListedTransactionState {
                    transactional_id: "my-tid".into(),
                    producer_id: row.producer_id,
                    transaction_state: "Ongoing".into(),
                    ..Default::default()
                }
            }
            _ => false,
        };
        if ongoing {
            break r;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "ListTransactions never saw the ongoing txn: {r:?}"
        );
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
    check!(row.producer_id >= 0);
    assert!(
        resp == ListTransactionsResponse {
            transaction_states: vec![ListedTransactionState {
                transactional_id: "my-tid".into(),
                producer_id: row.producer_id,
                transaction_state: "Ongoing".into(),
                ..Default::default()
            }],
            ..Default::default()
        }
    );

    transaction.abort().await.unwrap();
    Arc::into_inner(producer)
        .expect("transaction guard released its producer reference")
        .close()
        .await
        .unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_transactions_state_filter_excludes_non_matching() {
    let (broker, bootstrap, _dir, producer, transaction) =
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
    assert!(
        r == ListTransactionsResponse::default(),
        "Ongoing txn must not match an Empty state filter: {r:?}",
    );

    transaction.abort().await.unwrap();
    Arc::into_inner(producer)
        .expect("transaction guard released its producer reference")
        .close()
        .await
        .unwrap();
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
    assert!(
        r == ListTransactionsResponse {
            unknown_state_filters: vec!["BogusState".to_string()],
            ..Default::default()
        }
    );

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_transactions_returns_full_state_for_known_tid() {
    let (broker, bootstrap, _dir, producer, transaction) =
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
        assert!(r.transaction_states.len() == 1);
        let row = &r.transaction_states[0];
        if row.error_code == 0 && !row.topics.is_empty() {
            break row.clone();
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "Ongoing txn never showed its partitions: {row:?}"
        );
        // intentional: polls RPC response until the txn shows its registered
        // partitions (coordinator TxnEntry state after AddPartitionsToTxn); no
        // metadata-image signal for this
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    check!(row.producer_id >= 0);
    check!(row.transaction_start_time_ms > 0);
    assert!(
        row == DescribedTransactionState {
            transactional_id: "describe-tid".into(),
            transaction_state: "Ongoing".into(),
            transaction_timeout_ms: 60_000,
            transaction_start_time_ms: row.transaction_start_time_ms,
            producer_id: row.producer_id,
            topics: vec![TopicData {
                topic: "t-describe".into(),
                partitions: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        }
    );

    transaction.abort().await.unwrap();
    Arc::into_inner(producer)
        .expect("transaction guard released its producer reference")
        .close()
        .await
        .unwrap();
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
    assert!(
        r.transaction_states
            == [DescribedTransactionState {
                error_code: 75,
                transactional_id: "ghost-tid".into(),
                ..Default::default()
            }]
    );

    broker.shutdown().await;
}
