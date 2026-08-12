//! Native client coverage for KIP-939 prepare/recovery and forced termination.

use std::time::Duration;

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::AdminClient;
use crabka_client_core::Client;
use crabka_client_producer::{PreparedTransactionState, Producer, ProducerRecord};
use crabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    describe_transactions_request::DescribeTransactionsRequest,
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
};
use tempfile::TempDir;

async fn boot() -> (BrokerHandle, String, TempDir) {
    let directory = TempDir::new().expect("temporary broker directory");
    let mut config = BrokerConfig::for_tests(directory.path().to_path_buf());
    config.features.transaction_two_phase_commit_enable = true;
    let broker = Broker::start(config).await.expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    let client = client(&bootstrap).await;
    let response = client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![FeatureUpdateKey {
                feature: "transaction.version".to_owned(),
                max_version_level: 3,
                upgrade_type: 1,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("enable transaction.version 3");
    assert2::assert!(response.error_code == 0, "{response:?}");
    (broker, bootstrap, directory)
}

async fn client(bootstrap: &str) -> Client {
    Client::builder()
        .bootstrap(bootstrap)
        .build()
        .await
        .expect("client connects")
}

async fn create_topic(bootstrap: &str, topic: &str) {
    let response = client(bootstrap)
        .await
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.to_owned(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("create topic");
    assert2::assert!(response.topics[0].error_code == 0, "{response:?}");
}

async fn producer(bootstrap: &str, transactional_id: &str) -> Producer {
    Producer::builder()
        .bootstrap(bootstrap)
        .transactional_id(transactional_id)
        .transaction_two_phase_commit_enable(true)
        .linger(Duration::ZERO)
        .build()
        .await
        .expect("2PC producer connects")
}

async fn prepare_record(
    bootstrap: &str,
    transactional_id: &str,
    topic: &str,
    value: &'static [u8],
) -> PreparedTransactionState {
    let producer = producer(bootstrap, transactional_id).await;
    producer
        .init_transactions()
        .await
        .expect("initialize 2PC producer");
    let transaction = producer
        .begin_transaction()
        .await
        .expect("begin transaction");
    producer
        .send(ProducerRecord {
            topic: topic.to_owned(),
            partition: Some(0),
            value: Some(Bytes::from_static(value)),
            ..Default::default()
        })
        .await
        .await
        .expect("producer acknowledgement channel")
        .expect("transactional produce");
    let prepared = transaction.prepare().await.expect("prepare transaction");
    drop(transaction);
    producer.close().await.expect("close prepared producer");
    prepared
}

async fn transaction_state(client: &Client, transactional_id: &str) -> String {
    let response = client
        .send(DescribeTransactionsRequest {
            transactional_ids: vec![transactional_id.to_owned()],
            ..Default::default()
        })
        .await
        .expect("describe transaction");
    let state = &response.transaction_states[0];
    assert2::assert!(state.error_code == 0, "{state:?}");
    state.transaction_state.clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepare_recovery_commit_abort_and_admin_termination() {
    let (broker, bootstrap, _directory) = boot().await;
    let topic = "two-pc-client";
    create_topic(&bootstrap, topic).await;
    let observer = client(&bootstrap).await;

    let commit_id = "two-pc-recover-commit";
    let commit_state = prepare_record(&bootstrap, commit_id, topic, b"commit").await;
    let persisted_commit_state = commit_state
        .to_string()
        .parse::<PreparedTransactionState>()
        .expect("persisted prepared state round-trips");
    let commit_recovery = producer(&bootstrap, commit_id).await;
    commit_recovery
        .init_transactions_with_keep_prepared(true)
        .await
        .expect("recover prepared transaction for commit");
    commit_recovery
        .complete_transaction(persisted_commit_state)
        .await
        .expect("matching state commits");
    assert2::assert!(transaction_state(&observer, commit_id).await == "CompleteCommit");
    commit_recovery
        .close()
        .await
        .expect("close commit recovery");

    let abort_id = "two-pc-recover-abort";
    let _abort_state = prepare_record(&bootstrap, abort_id, topic, b"abort").await;
    let abort_recovery = producer(&bootstrap, abort_id).await;
    abort_recovery
        .init_transactions_with_keep_prepared(true)
        .await
        .expect("recover prepared transaction for abort");
    abort_recovery
        .complete_transaction(PreparedTransactionState::default())
        .await
        .expect("mismatched state aborts");
    assert2::assert!(transaction_state(&observer, abort_id).await == "CompleteAbort");
    abort_recovery.close().await.expect("close abort recovery");

    let terminated_id = "two-pc-admin-terminate";
    let _terminated_state = prepare_record(&bootstrap, terminated_id, topic, b"terminate").await;
    assert2::assert!(transaction_state(&observer, terminated_id).await == "Ongoing");
    let admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .expect("admin connects");
    admin
        .force_terminate_transaction(terminated_id)
        .await
        .expect("force terminate transaction");
    // InitProducerId first aborts the ongoing generation, then installs a new
    // fenced generation in Empty state.
    assert2::assert!(transaction_state(&observer, terminated_id).await == "Empty");

    broker.shutdown().await;
}
