//! Live read-only fold over a real WAL topic.
//!
//! `committed_fold_snapshot_live` is the durable-inspection entry point the
//! gres binary calls, and it is the only production path that reaches
//! `CommittedWalReader::committed_from` on the Kafka reader — recovery and the
//! apply loop both use the `_traced` variant. Nothing exercised it, so a reader
//! that returned no records at all still passed every test.

use std::sync::Arc;

use assert2::{assert, check};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_gres_ranges::{RangeId, TenantName};
use crabka_gres_substrate::{
    GroupCommitRequest, LiveRecoveryConfig, ProducerWalWriter, SubstrateError,
    TransactionalWalWriter, WalFrame,
    readonly_fold::{FoldLimits, FoldProjection, GenerationWitness, committed_fold_snapshot_live},
    recover_live,
};
use crabka_pgkv::{MemKv, WriteOp};
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

/// Reports whatever generation the writer was handed, so the fold's fencing
/// check passes and the test is about the WAL read rather than about fencing.
struct FixedGeneration(u64);

#[async_trait::async_trait]
impl GenerationWitness for FixedGeneration {
    async fn current_generation(&self) -> Result<u64, SubstrateError> {
        Ok(self.0)
    }
}

/// A fold over a WAL with committed frames must return those frames' rows.
///
/// With the reader's `committed_from` short-circuited to an empty vector the
/// fold still succeeds — it just reports a snapshot with no records — so the
/// assertion has to be on the rows, not on the call.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_live_fold_returns_the_rows_the_wal_committed() {
    let (_broker, bootstrap, _dir) = boot().await;
    let tenant = "fold-live";
    let store = Arc::new(MemKv::default());
    let recovered = recover_live(&bootstrap, tenant, None, store.as_ref())
        .await
        .expect("recover");
    let generation = recovered.generation;
    let writer = ProducerWalWriter::new(recovered.producer, format!("__gres_wal.{tenant}.r0"));
    writer
        .commit_group(GroupCommitRequest {
            generation,
            frames: vec![WalFrame {
                journal_seq: recovered.next_journal_seq,
                ops: vec![WriteOp::Put {
                    key: b"catalog/folded".to_vec(),
                    value: b"from the wal".to_vec(),
                }],
            }],
        })
        .await
        .expect("commit one frame");

    let config = LiveRecoveryConfig::new(
        bootstrap,
        TenantName::parse(tenant).expect("tenant"),
        RangeId::COORDINATOR,
        None,
    );
    let snapshot = committed_fold_snapshot_live(
        &config,
        &FixedGeneration(generation.0),
        FoldProjection::All,
        FoldLimits::default(),
    )
    .await
    .expect("live fold");

    assert!(
        snapshot
            .records
            .iter()
            .any(|(key, value)| key == b"catalog/folded" && value == b"from the wal"),
        "the fold dropped the committed row: {:?}",
        snapshot.records
    );
    check!(snapshot.sample_offset >= 0);
}
