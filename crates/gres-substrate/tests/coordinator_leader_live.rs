//! Real multi-broker fencing when the transaction coordinator is not the WAL leader.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use assert2::assert;
use crabka_client_admin::AdminClient;
use crabka_client_core::Client;
use crabka_client_producer::{Acks, Producer};
use crabka_gres_ranges::{RangeId, TenantName};
use crabka_gres_substrate::{
    GroupCommitRequest, ProducerWalWriter, TransactionalWalWriter, WalFrame,
    ensure_wal_topic_for_range, recover_live,
};
use crabka_pgkv::{Kv, MemKv, WriteOp};
use crabka_protocol::owned::find_coordinator_request::FindCoordinatorRequest;

#[path = "../../broker/tests/support/mod.rs"]
mod broker_support;

fn request(seq: u64, key: &[u8]) -> GroupCommitRequest {
    GroupCommitRequest {
        generation: crabka_gres_substrate::WriterGeneration(0),
        frames: vec![WalFrame {
            journal_seq: seq,
            ops: vec![WriteOp::Put {
                key: key.to_vec(),
                value: b"yes".to_vec(),
            }],
        }],
    }
}

async fn wait_for_transaction_coordinator(bootstrap: &str, transactional_id: &str) {
    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .acks(Acks::All)
        .transactional_id(transactional_id)
        .build()
        .await
        .expect("coordinator readiness producer");
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match producer.init_transactions().await {
            Ok(()) => break,
            Err(error) if Instant::now() < deadline => {
                tracing::debug!(%error, "transaction coordinator not ready yet");
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => panic!("transaction coordinator readiness deadline: {error}"),
        }
    }
    producer.close().await.expect("close readiness producer");
}

fn raise_fd_limit_for_cluster() {
    let limits = rustix::process::getrlimit(rustix::process::Resource::Nofile);
    rustix::process::setrlimit(
        rustix::process::Resource::Nofile,
        rustix::process::Rlimit {
            current: Some(8192),
            maximum: limits.maximum,
        },
    )
    .expect("raise soft file descriptor limit for three in-process brokers");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fencing_falls_back_to_end_txn_when_coordinator_differs_from_partition_leader() {
    raise_fd_limit_for_cluster();
    let cluster = broker_support::start_n_node_with_retry(3).await;
    for (broker, _, _) in &cluster {
        broker.wait_until_brokers_registered(3).await;
    }
    let bootstrap = cluster[0].0.listen_addr().to_string();
    let client = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .expect("client");
    let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .expect("admin");

    let mut selected = None;
    for suffix in 0..64 {
        let tenant = format!("coord-split-{suffix}");
        let tenant_name = TenantName::parse(&tenant).expect("tenant name");
        let topic = ensure_wal_topic_for_range(&mut admin, &tenant_name, RangeId::COORDINATOR)
            .await
            .expect("ensure candidate WAL topic");
        let leader = cluster
            .iter()
            .find_map(|(broker, _, _)| broker.partition_leader_for_test(&topic, 0))
            .expect("WAL leader");
        let transactional_id = format!("__gres.{tenant}.r0");
        let deadline = Instant::now() + Duration::from_secs(20);
        let coordinator = loop {
            let response = client
                .send(FindCoordinatorRequest {
                    key: transactional_id.clone(),
                    key_type: 1,
                    coordinator_keys: vec![transactional_id.clone()],
                    ..Default::default()
                })
                .await
                .expect("find transaction coordinator");
            let coordinator = response
                .coordinators
                .first()
                .map_or(response.node_id, |entry| entry.node_id);
            if coordinator >= 0 {
                break u64::try_from(coordinator).expect("coordinator fits u64");
            }
            assert!(
                Instant::now() < deadline,
                "transaction coordinator discovery deadline"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        if coordinator != leader {
            selected = Some(tenant);
            break;
        }
    }
    let tenant = selected.expect("find a tenant whose coordinator differs from WAL leader");
    wait_for_transaction_coordinator(&bootstrap, &format!("__gres.{tenant}.r0.readiness")).await;
    let first_cache = MemKv::default();
    let first = recover_live(&bootstrap, &tenant, None, &first_cache)
        .await
        .expect("initial recovery on split topology");
    let topic = format!("__gres_wal.{tenant}.r0");
    let stale = Arc::new(ProducerWalWriter::new(first.producer, topic.clone()));
    stale
        .commit_group(request(0, b"row/first"))
        .await
        .expect("first commit");

    let successor_cache = MemKv::default();
    let successor_recovery = recover_live(&bootstrap, &tenant, None, &successor_cache)
        .await
        .expect("successor recovery");
    let successor = ProducerWalWriter::new(successor_recovery.producer, topic);
    let stale_error = stale
        .commit_group(request(1, b"row/stale"))
        .await
        .expect_err("stale fenced");
    assert!(matches!(
        stale_error,
        crabka_gres_substrate::SubstrateError::Fenced
    ));
    successor
        .commit_group(request(1, b"row/successor"))
        .await
        .expect("successor commit");

    let final_cache = MemKv::default();
    recover_live(&bootstrap, &tenant, None, &final_cache)
        .await
        .expect("third recovery");
    assert!(final_cache.get(b"row/first").expect("first") == Some(b"yes".to_vec()));
    assert!(final_cache.get(b"row/stale").expect("stale").is_none());
    assert!(final_cache.get(b"row/successor").expect("successor") == Some(b"yes".to_vec()));

    for (broker, _, _) in cluster {
        broker.shutdown().await;
    }
}
