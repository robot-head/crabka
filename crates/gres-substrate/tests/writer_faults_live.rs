//! Deterministic faults through the real producer-backed Gres WAL writer.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use assert2::assert;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::AdminClient;
use crabka_client_core::Client;
use crabka_client_producer::ProducerError;
use crabka_gres_substrate::{
    GroupCommitRequest, ProducerWalWriter, SubstrateCommitter, TransactionalWalWriter, WalFrame,
    WalWriterFaultInjector, WalWriterFaultStage, WriterGeneration, recover_live,
};
use crabka_pgexec::Committer;
use crabka_pgkv::{Kv, MemKv, WriteOp};
use crabka_protocol::{
    owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic},
    primitives::uuid::Uuid as WireUuid,
};
use tempfile::TempDir;
use tokio::sync::oneshot;

struct OneShotFault {
    stage: WalWriterFaultStage,
    fired: AtomicBool,
}

impl OneShotFault {
    fn new(stage: WalWriterFaultStage) -> Self {
        Self {
            stage,
            fired: AtomicBool::new(false),
        }
    }
}

impl WalWriterFaultInjector for OneShotFault {
    fn inject(&self, stage: WalWriterFaultStage) -> Option<ProducerError> {
        (stage == self.stage && !self.fired.swap(true, Ordering::SeqCst))
            .then_some(ProducerError::BufferFull)
    }
}

struct SendThenAbortFault {
    abort: OneShotFault,
    send: OneShotFault,
}

impl WalWriterFaultInjector for SendThenAbortFault {
    fn inject(&self, stage: WalWriterFaultStage) -> Option<ProducerError> {
        self.send.inject(stage).or_else(|| self.abort.inject(stage))
    }
}

fn request(seq: u64, key: &[u8], value: &[u8]) -> GroupCommitRequest {
    GroupCommitRequest {
        generation: WriterGeneration(0),
        frames: vec![WalFrame {
            journal_seq: seq,
            ops: vec![WriteOp::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            }],
        }],
    }
}

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

async fn raw_batch_identities(bootstrap: &str, topic: &str) -> Vec<(i64, i64, i16, bool)> {
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
        .map(|batch| {
            (
                batch.base_offset,
                batch.producer_id,
                batch.producer_epoch,
                batch.attributes.is_control_batch(),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transient_pre_end_txn_failure_aborts_and_next_group_commits() {
    let (broker, bootstrap, _dir) = boot().await;
    let cache = MemKv::default();
    let recovered = recover_live(&bootstrap, "fault-transient", None, &cache)
        .await
        .expect("initial recovery");
    let writer = ProducerWalWriter::new(recovered.producer, "__gres_wal.fault-transient.r0".into())
        .with_fault_injector(Arc::new(OneShotFault::new(
            WalWriterFaultStage::PendingSendResult,
        )));

    let first = writer
        .commit_group(request(0, b"row/failed", b"must-not-appear"))
        .await
        .expect_err("faulted group must fail after a completed abort");
    assert!(first.to_string().contains("send buffer full"));
    writer
        .commit_group(request(0, b"row/successor", b"committed"))
        .await
        .expect("producer remains usable after abort");

    let rebuilt = MemKv::default();
    recover_live(&bootstrap, "fault-transient", None, &rebuilt)
        .await
        .expect("successor recovery");
    assert!(
        rebuilt
            .get(b"row/failed")
            .expect("read failed row")
            .is_none()
    );
    assert!(rebuilt.get(b"row/successor").expect("read successor") == Some(b"committed".to_vec()));
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_advances_over_aborted_only_fetch_page_to_its_barrier() {
    let (broker, bootstrap, _dir) = boot().await;
    let cache = MemKv::default();
    let recovered = recover_live(&bootstrap, "fault-aborted-page", None, &cache)
        .await
        .expect("initial recovery");
    let writer = ProducerWalWriter::new(
        recovered.producer,
        "__gres_wal.fault-aborted-page.r0".into(),
    )
    .with_fault_injector(Arc::new(OneShotFault::new(
        WalWriterFaultStage::PendingSendResult,
    )));
    writer
        .commit_group(GroupCommitRequest {
            generation: WriterGeneration(0),
            frames: (0_u64..3)
                .map(|journal_seq| WalFrame {
                    journal_seq,
                    ops: vec![WriteOp::Put {
                        key: format!("aborted-only/{journal_seq}").into_bytes(),
                        value: vec![b'x'; 600_000],
                    }],
                })
                .collect(),
        })
        .await
        .expect_err("group aborts");

    let rebuilt = MemKv::default();
    let successor = recover_live(&bootstrap, "fault-aborted-page", None, &rebuilt)
        .await
        .expect("recovery advances through aborted page");
    assert!(successor.next_journal_seq == 0);
    assert!(rebuilt.get(b"aborted-only/0").expect("get").is_none());
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ambiguous_end_txn_never_returns_a_false_failure_and_successor_replays_atomically() {
    let (broker, bootstrap, _dir) = boot().await;
    let cache = MemKv::default();
    let recovered = recover_live(&bootstrap, "fault-ambiguous", None, &cache)
        .await
        .expect("initial recovery");
    let (fatal_tx, fatal_rx) = oneshot::channel();
    let fatal_tx = Arc::new(Mutex::new(Some(fatal_tx)));
    let writer = Arc::new(
        ProducerWalWriter::new(recovered.producer, "__gres_wal.fault-ambiguous.r0".into())
            .with_fault_injector(Arc::new(OneShotFault::new(
                WalWriterFaultStage::AfterCommit,
            )))
            .with_indeterminate_handler(Arc::new(move |_| {
                if let Some(sender) = fatal_tx.lock().expect("fatal sender lock").take() {
                    let _ = sender.send(());
                }
            })),
    );
    let task = tokio::spawn({
        let writer = Arc::clone(&writer);
        async move { writer.commit_group(request(0, b"atomic/a", b"yes")).await }
    });

    tokio::time::timeout(Duration::from_secs(5), fatal_rx)
        .await
        .expect("fatal outcome deadline")
        .expect("fatal signal");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), task)
            .await
            .is_err(),
        "an indeterminate commit must not answer its caller"
    );

    let rebuilt = MemKv::default();
    recover_live(&bootstrap, "fault-ambiguous", None, &rebuilt)
        .await
        .expect("fenced successor recovery");
    assert!(rebuilt.get(b"atomic/a").expect("read atom") == Some(b"yes".to_vec()));
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abort_failure_is_indeterminate_and_never_reported_as_cleanly_aborted() {
    let (broker, bootstrap, _dir) = boot().await;
    let cache = MemKv::default();
    let recovered = recover_live(&bootstrap, "fault-abort", None, &cache)
        .await
        .expect("initial recovery");
    let (fatal_tx, fatal_rx) = oneshot::channel();
    let fatal_tx = Arc::new(Mutex::new(Some(fatal_tx)));
    // A second injector is needed to route the transaction onto the abort path.
    let writer = Arc::new(
        ProducerWalWriter::new(recovered.producer, "__gres_wal.fault-abort.r0".into())
            .with_fault_injector(Arc::new(SendThenAbortFault {
                send: OneShotFault::new(WalWriterFaultStage::AfterSendAcks),
                abort: OneShotFault::new(WalWriterFaultStage::BeforeAbort),
            }))
            .with_indeterminate_handler(Arc::new(move |_| {
                if let Some(sender) = fatal_tx.lock().expect("fatal sender lock").take() {
                    let _ = sender.send(());
                }
            })),
    );
    let task = tokio::spawn({
        let writer = Arc::clone(&writer);
        async move {
            writer
                .commit_group(request(0, b"abort/unknown", b"value"))
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), fatal_rx)
        .await
        .expect("fatal abort deadline")
        .expect("fatal abort signal");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), task)
            .await
            .is_err()
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_read_uncommitted_batches_have_no_stale_epoch_after_successor_fence() {
    let (broker, bootstrap, _dir) = boot().await;
    let topic = "__gres_wal.fault-epoch.r0";
    let first_cache = MemKv::default();
    let first = recover_live(&bootstrap, "fault-epoch", None, &first_cache)
        .await
        .expect("first recovery");
    let stale = Arc::new(ProducerWalWriter::new(first.producer, topic.into()));
    let first_ack = stale
        .commit_group(request(0, b"epoch/first", b"yes"))
        .await
        .expect("first commit");

    let second_cache = MemKv::default();
    let second = recover_live(&bootstrap, "fault-epoch", None, &second_cache)
        .await
        .expect("second recovery");
    let fence_offset = second.barrier_offset;
    let successor = ProducerWalWriter::new(second.producer, topic.into());
    stale
        .commit_group(request(1, b"epoch/stale", b"no"))
        .await
        .expect_err("stale fenced");
    let successor_ack = successor
        .commit_group(request(1, b"epoch/successor", b"yes"))
        .await
        .expect("successor commit");

    let batches = raw_batch_identities(&bootstrap, topic).await;
    let first_offset = first_ack.frames[0].offset;
    let successor_offset = successor_ack.frames[0].offset;
    let stale_identity = batches
        .iter()
        .find(|batch| batch.0 == first_offset)
        .map(|batch| (batch.1, batch.2))
        .expect("stale generation batch");
    let successor_identity = batches
        .iter()
        .find(|batch| batch.0 == successor_offset)
        .map(|batch| (batch.1, batch.2))
        .expect("successor generation batch");
    assert!(
        stale_identity != successor_identity,
        "successor must use a fenced producer identity"
    );
    assert!(
        batches
            .iter()
            .filter(|batch| batch.0 > fence_offset && !batch.3)
            .all(|batch| (batch.1, batch.2) != stale_identity)
    );

    let final_cache = MemKv::default();
    recover_live(&bootstrap, "fault-epoch", None, &final_cache)
        .await
        .expect("third recovery");
    assert!(final_cache.get(b"epoch/first").expect("first") == Some(b"yes".to_vec()));
    assert!(final_cache.get(b"epoch/stale").expect("stale").is_none());
    assert!(final_cache.get(b"epoch/successor").expect("successor") == Some(b"yes".to_vec()));
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_real_broker_group_chunks_and_recovers_as_one_transaction() {
    let (broker, bootstrap, _dir) = boot().await;
    let first_cache: Arc<dyn Kv> = Arc::new(MemKv::default());
    let recovered = recover_live(&bootstrap, "fault-oversized", None, first_cache.as_ref())
        .await
        .expect("first recovery");
    let writer = Arc::new(ProducerWalWriter::new(
        recovered.producer,
        "__gres_wal.fault-oversized.r0".into(),
    ));
    let committer = SubstrateCommitter::new(
        Arc::clone(&first_cache),
        writer,
        recovered.generation,
        recovered.next_journal_seq,
    );
    let left = vec![b'l'; 700_000];
    let right = vec![b'r'; 700_000];
    committer
        .commit(vec![
            WriteOp::Put {
                key: b"oversized/left".to_vec(),
                value: left.clone(),
            },
            WriteOp::Put {
                key: b"oversized/right".to_vec(),
                value: right.clone(),
            },
        ])
        .await
        .expect("atomic chunked commit");
    drop(committer);
    drop(first_cache);

    let rebuilt = MemKv::default();
    let successor = recover_live(&bootstrap, "fault-oversized", None, &rebuilt)
        .await
        .expect("recover oversized group");
    assert!(
        successor.next_journal_seq == 2,
        "batch must have crossed the frame cap"
    );
    assert!(rebuilt.get(b"oversized/left").expect("left") == Some(left));
    assert!(rebuilt.get(b"oversized/right").expect("right") == Some(right));
    broker.shutdown().await;
}
