// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files.

//! KIP-939 two-phase-commit (2PC) participation — `InitProducerId` v6
//! coordinator semantics:
//!  - `enable2Pc` is rejected with `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` when
//!    the cluster has `transaction.two.phase.commit.enable=false`;
//!  - `keepPreparedTxn` returns `UNSUPPORTED_VERSION` (matches Kafka, where the
//!    prepared-txn recovery flow is still unstable);
//!  - with 2PC enabled, an `enable2Pc` transaction is persisted with the
//!    no-timeout sentinel (`i32::MAX`), so it is exempt from the idle reaper.
//!
//! The reaper's *decision* (never abort a 2PC txn) is proven exhaustively in
//! `txn::two_pc_model`; these tests pin the wire/handler behaviour end-to-end.

use std::time::Duration;

use assert2::assert;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_producer::Producer;
use crabka_protocol::owned::{
    describe_transactions_request::DescribeTransactionsRequest,
    init_producer_id_request::InitProducerIdRequest,
};
use tempfile::TempDir;

// Kafka error codes (see crates/broker/src/codes.rs).
const NONE: i16 = 0;
const UNSUPPORTED_VERSION: i16 = 35;
const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;

async fn boot(two_pc_enabled: bool) -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    cfg.features.transaction_two_phase_commit_enable = two_pc_enabled;
    let broker = Broker::start(cfg).await.unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn client(bootstrap: &str) -> crabka_client_core::Client {
    crabka_client_core::Client::builder()
        .bootstrap(bootstrap)
        .build()
        .await
        .unwrap()
}

/// `enable2Pc=true` against a cluster with 2PC disabled is rejected with
/// `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` — before any coordinator lookup, so
/// it does not depend on `__transaction_state` being bootstrapped, and a client
/// cannot probe the cluster flag with an `UNSUPPORTED_*`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_2pc_rejected_when_cluster_disabled() {
    let (broker, bootstrap, _dir) = boot(false).await;
    let client = client(&bootstrap).await;

    let resp = client
        .send(InitProducerIdRequest {
            transactional_id: Some("tid-2pc".into()),
            transaction_timeout_ms: 30_000,
            producer_id: -1,
            producer_epoch: -1,
            enable2_pc: true,
            keep_prepared_txn: false,
            ..Default::default()
        })
        .await
        .expect("InitProducerId");

    assert!(
        resp.error_code == TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
        "expected 53 (TRANSACTIONAL_ID_AUTHORIZATION_FAILED), got {}",
        resp.error_code
    );
    broker.shutdown().await;
}

/// `keepPreparedTxn=true` returns `UNSUPPORTED_VERSION` regardless of the 2PC
/// flag — the prepared-txn recovery flow is not yet a stable Kafka feature.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keep_prepared_txn_is_unsupported() {
    let (broker, bootstrap, _dir) = boot(true).await;
    let client = client(&bootstrap).await;

    let resp = client
        .send(InitProducerIdRequest {
            transactional_id: Some("tid-keep".into()),
            transaction_timeout_ms: 30_000,
            producer_id: -1,
            producer_epoch: -1,
            enable2_pc: false,
            keep_prepared_txn: true,
            ..Default::default()
        })
        .await
        .expect("InitProducerId");

    assert!(
        resp.error_code == UNSUPPORTED_VERSION,
        "expected 35 (UNSUPPORTED_VERSION), got {}",
        resp.error_code
    );
    broker.shutdown().await;
}

/// With 2PC enabled, an `enable2Pc` `InitProducerId` succeeds and the
/// transaction is persisted with the no-timeout sentinel (`i32::MAX`), which is
/// exactly how the coordinator marks a transaction exempt from the timeout
/// reaper. We bootstrap the coordinator with a normal transactional producer
/// (which creates `__transaction_state` + the tid's entry), then re-init the
/// same tid with `enable2Pc=true`, and read the persisted timeout back via
/// `DescribeTransactions`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_2pc_persists_no_timeout_sentinel() {
    let (broker, bootstrap, _dir) = boot(true).await;

    // Bootstrap the txn coordinator + this tid's entry via a normal producer.
    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("tid-2pc-ok")
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();

    // Re-init the SAME tid with enable2Pc → flips it to a no-timeout 2PC txn.
    let client = client(&bootstrap).await;
    let resp = client
        .send(InitProducerIdRequest {
            transactional_id: Some("tid-2pc-ok".into()),
            transaction_timeout_ms: 30_000,
            producer_id: -1,
            producer_epoch: -1,
            enable2_pc: true,
            keep_prepared_txn: false,
            ..Default::default()
        })
        .await
        .expect("InitProducerId(enable2Pc)");
    assert!(
        resp.error_code == NONE,
        "enable2Pc init should succeed once the cluster enables 2PC, got {}",
        resp.error_code
    );
    assert!(resp.producer_id >= 0);

    // The persisted transaction timeout must be the 2PC no-timeout sentinel.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let timeout_ms = loop {
        let r = client
            .send(DescribeTransactionsRequest {
                transactional_ids: vec!["tid-2pc-ok".into()],
                ..Default::default()
            })
            .await
            .expect("DescribeTransactions");
        let row = &r.transaction_states[0];
        if row.error_code == NONE {
            break row.transaction_timeout_ms;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "DescribeTransactions never returned the tid: {row:?}"
        );
        // intentional: transaction-coordinator state (persisted txn timeout) is
        // read via a DescribeTransactions RPC and is not in the metadata image
        // nor exposed as a metric — bounded RPC-response poll, no awaiter exists.
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        timeout_ms == i32::MAX,
        "2PC transaction must persist the no-timeout sentinel i32::MAX, got {timeout_ms}"
    );

    producer.close().await.ok();
    broker.shutdown().await;
}
