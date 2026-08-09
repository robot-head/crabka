//! A compute with no tracing subscriber must not stamp anything onto the WAL.
//!
//! This test lives in its own test binary on purpose. The assertion is about a
//! process where *no* subscriber is installed. A sibling test that calls
//! `set_global_default` would silently invalidate it.

use std::sync::Arc;

use assert2::check;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::AdminClient;
use crabka_client_core::Client;
use crabka_gres_substrate::{ProducerWalWriter, SubstrateCommitter, recover_live};
use crabka_pgexec::Committer as _;
use crabka_pgkv::{Kv, MemKv, WriteOp};
use crabka_protocol::{
    owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic},
    primitives::uuid::Uuid as WireUuid,
    records::Record,
};
use tempfile::TempDir;

async fn boot() -> (BrokerHandle, String, TempDir) {
    raise_fd_limit_for_broker();
    let dir = TempDir::new().expect("broker tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

#[cfg(unix)]
fn raise_fd_limit_for_broker() {
    let limits = rustix::process::getrlimit(rustix::process::Resource::Nofile);
    if limits.current.unwrap_or(0) < 8192 {
        rustix::process::setrlimit(
            rustix::process::Resource::Nofile,
            rustix::process::Rlimit {
                current: Some(8192),
                maximum: limits.maximum,
            },
        )
        .expect("raise soft file descriptor limit for live broker tests");
    }
}

#[cfg(not(unix))]
fn raise_fd_limit_for_broker() {}

async fn wal_records(bootstrap: &str, topic: &str) -> Vec<Record> {
    let mut admin = AdminClient::connect(&[bootstrap.to_owned()])
        .await
        .expect("admin");
    let metadata = admin.metadata(&[topic]).await.expect("metadata");
    let topic_id = metadata
        .topics
        .into_iter()
        .find(|entry| entry.name == topic)
        .and_then(|entry| entry.topic_id)
        .map_or(WireUuid::ZERO, |id| WireUuid(id.into_bytes()));
    let response = Client::builder()
        .bootstrap(bootstrap)
        .build()
        .await
        .expect("raw fetch client")
        .send(FetchRequest {
            isolation_level: 0,
            max_wait_ms: 100,
            min_bytes: 1,
            max_bytes: 50 * 1024 * 1024,
            topics: vec![FetchTopic {
                topic: topic.to_owned(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 50 * 1024 * 1024,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("raw fetch");
    response.responses[0].partitions[0]
        .records
        .as_ref()
        .and_then(|records| records.as_v2())
        .unwrap_or(&[])
        .iter()
        .filter(|batch| !batch.attributes.is_control_batch())
        .flat_map(|batch| batch.records.iter().cloned())
        .collect()
}

/// With every callsite disabled the carrier is empty, and an empty carrier must
/// yield *no* headers. It must not yield an empty-valued `traceparent`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_without_a_subscriber_writes_records_with_no_headers() {
    check!(
        tracing::Span::current().is_none(),
        "this binary must run with no subscriber installed"
    );

    let (broker, bootstrap, _dir) = boot().await;
    let store = Arc::new(MemKv::default());
    let recovered = recover_live(&bootstrap, "trace-off", None, store.as_ref())
        .await
        .expect("recovery");
    let topic = "__gres_wal.trace-off.r0".to_owned();
    let kv: Arc<dyn Kv> = store;
    let writer = Arc::new(ProducerWalWriter::new(recovered.producer, topic.clone()));
    let committer =
        SubstrateCommitter::new(kv, writer, recovered.generation, recovered.next_journal_seq);

    committer
        .commit(vec![WriteOp::Put {
            key: b"row/1".to_vec(),
            value: b"a".to_vec(),
        }])
        .await
        .expect("commit");

    let records = wal_records(&bootstrap, &topic).await;
    check!(!records.is_empty());
    for record in &records {
        check!(record.headers.is_empty());
    }

    broker.shutdown().await;
}
