//! Distributed-tracing behaviour of the substrate WAL path.
//!
//! Assertions run against exported [`SpanData`], never against live
//! [`tracing::Span`] handles: `tracing-opentelemetry` resolves a span's trace
//! id and parent when the span *closes*, so a live handle reports neither.
//! The subscriber is installed with `set_global_default` for the same reason a
//! thread-local one would not do — the broker, the producer, and the commit all
//! hop runtime worker threads.

use std::{
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use assert2::{assert, check};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_admin::AdminClient;
use crabka_client_core::Client;
use crabka_client_producer::ProducerError;
use crabka_gres_substrate::{
    GroupCommitAck, GroupCommitRequest, ProducerWalWriter, SubstrateCommitter, SubstrateError,
    TransactionalWalWriter, WalAppendAck, WalFrame, WalWriterFaultInjector, WalWriterFaultStage,
    WriterGeneration, recover_live,
};
use crabka_pgexec::Committer as _;
use crabka_pgkv::{Kv, MemKv, WriteOp};
use crabka_protocol::{
    owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic},
    primitives::uuid::Uuid as WireUuid,
    records::Record,
};
use opentelemetry::{
    Value,
    trace::{SpanKind, Status, TracerProvider as _},
};
use opentelemetry_sdk::trace::{InMemorySpanExporter, Sampler, SdkTracerProvider, SpanData};
use tempfile::TempDir;
use tokio::sync::oneshot;
use tracing::Instrument as _;
use tracing_subscriber::layer::SubscriberExt as _;

/// Serialises the tests: they share one process-wide subscriber and one
/// in-memory exporter, and each drains everything the previous test left.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

static TRACING: OnceLock<(SdkTracerProvider, InMemorySpanExporter)> = OnceLock::new();

fn install_tracing() -> &'static (SdkTracerProvider, InMemorySpanExporter) {
    TRACING.get_or_init(|| {
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry().with(
            tracing_opentelemetry::layer().with_tracer(provider.tracer("gres-substrate-wal-test")),
        );
        tracing::subscriber::set_global_default(subscriber).expect("global tracing subscriber");
        (provider, exporter)
    })
}

/// Take everything closed so far, so a test only sees its own spans.
fn drain_spans() -> Vec<SpanData> {
    let (provider, exporter) = install_tracing();
    provider.force_flush().expect("flush tracer provider");
    let spans = exporter.get_finished_spans().expect("finished spans");
    exporter.reset();
    spans
}

fn find_span<'a>(spans: &'a [SpanData], name: &str) -> &'a SpanData {
    spans
        .iter()
        .find(|span| span.name == name)
        .unwrap_or_else(|| panic!("no exported span named {name}"))
}

fn attribute<'a>(span: &'a SpanData, key: &str) -> Option<&'a Value> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| &kv.value)
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

/// Every non-control record on the WAL topic, in offset order.
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

fn header<'a>(record: &'a Record, key: &str) -> Option<&'a [u8]> {
    record
        .headers
        .iter()
        .find(|header| header.key == key)
        .and_then(|header| header.value.as_deref())
}

fn put(key: &[u8], value: &[u8]) -> WriteOp {
    WriteOp::Put {
        key: key.to_vec(),
        value: value.to_vec(),
    }
}

/// How long [`SlowWalWriter`] holds the group-commit permit.
const APPEND_DELAY: Duration = Duration::from_millis(150);

/// A WAL writer that acknowledges everything, slowly, so a second commit has to
/// queue on the group-commit gate.
struct SlowWalWriter;

#[async_trait::async_trait]
impl TransactionalWalWriter for SlowWalWriter {
    async fn commit_group(
        &self,
        request: GroupCommitRequest,
    ) -> Result<GroupCommitAck, SubstrateError> {
        tokio::time::sleep(APPEND_DELAY).await;
        Ok(GroupCommitAck {
            frames: request
                .frames
                .iter()
                .map(|frame| WalAppendAck {
                    offset: i64::try_from(frame.journal_seq).expect("offset"),
                    journal_seq: frame.journal_seq,
                })
                .collect(),
        })
    }
}

/// Fails the very first `EndTxn` acknowledgement, and only that one, with an
/// error the writer must classify as indeterminate.
struct OnceAfterCommit(AtomicBool);

impl WalWriterFaultInjector for OnceAfterCommit {
    fn inject(&self, stage: WalWriterFaultStage) -> Option<ProducerError> {
        (stage == WalWriterFaultStage::AfterCommit && !self.0.swap(true, Ordering::SeqCst))
            .then_some(ProducerError::FlushTimeout)
    }
}

/// One commit under a caller's span must produce the whole WAL span tree and
/// stamp the durable record with the append span's own trace context.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_emits_the_wal_span_tree_and_stamps_the_record_with_the_append_context() {
    let _serial = SERIAL.lock().await;
    drain_spans();

    let (broker, bootstrap, _dir) = boot().await;
    let store = Arc::new(MemKv::default());
    let recovered = recover_live(&bootstrap, "trace-commit", None, store.as_ref())
        .await
        .expect("recovery");
    let topic = "__gres_wal.trace-commit.r0".to_owned();
    let first_seq = recovered.next_journal_seq;
    let kv: Arc<dyn Kv> = store;
    let writer = Arc::new(ProducerWalWriter::new(recovered.producer, topic.clone()));
    let committer = SubstrateCommitter::new(kv, writer, recovered.generation, first_seq);

    let caller = tracing::info_span!("test.caller");
    committer
        .commit(vec![put(b"row/1", b"a"), put(b"row/2", b"b")])
        .instrument(caller.clone())
        .await
        .expect("commit");
    drop(caller);

    let spans = drain_spans();
    let caller = find_span(&spans, "test.caller");
    let commit = find_span(&spans, "pg.commit");
    // Recovery fences with its own barrier append, so select by parent rather
    // than by name alone.
    let child = |name: &str| {
        spans
            .iter()
            .find(|span| span.name == name && span.parent_span_id == commit.span_context.span_id())
            .unwrap_or_else(|| panic!("no {name} span under pg.commit"))
    };
    let append = child("gres.wal_append");
    let apply = child("kv.apply");

    check!(commit.parent_span_id == caller.span_context.span_id());
    check!(append.span_context.trace_id() == caller.span_context.trace_id());
    check!(apply.span_context.trace_id() == caller.span_context.trace_id());

    let expected_seq = i64::try_from(first_seq).expect("journal sequence");
    check!(attribute(commit, "pg.commit.ops") == Some(&Value::I64(2)));
    check!(attribute(commit, "pg.commit.frames") == Some(&Value::I64(1)));
    check!(attribute(commit, "pg.commit.bytes") == attribute(append, "pg.wal.bytes"));
    check!(attribute(commit, "pg.journal_seq.first") == Some(&Value::I64(expected_seq)));
    check!(attribute(commit, "pg.journal_seq.next") == Some(&Value::I64(expected_seq + 1)));
    check!(attribute(apply, "pg.frame.ops") == Some(&Value::I64(2)));
    check!(
        attribute(append, "otel.kind").is_none(),
        "otel.kind is lifted onto SpanKind"
    );
    check!(append.span_kind == SpanKind::Producer);
    check!(
        attribute(append, "messaging.destination.name")
            == Some(&Value::String(topic.clone().into()))
    );
    check!(attribute(append, "pg.wal.frames") == Some(&Value::I64(1)));
    // Pin the offsets themselves, not just their equality: when the recorder
    // is stubbed out both attributes are absent and `None == None` passes.
    assert!(let Some(&Value::I64(first_offset)) = attribute(append, "pg.wal.first_offset"));
    assert!(let Some(&Value::I64(last_offset)) = attribute(append, "pg.wal.last_offset"));
    check!(first_offset >= 0);
    check!(
        first_offset == last_offset,
        "one frame was appended, so its first and last offset coincide"
    );
    // Success leaves the status untouched — never `"OK"`.
    check!(append.status == Status::Unset);
    check!(commit.status == Status::Unset);

    let records = wal_records(&bootstrap, &topic).await;
    assert!(let Some(record) = records.last());
    assert!(let Some(traceparent) = header(record, "traceparent"));
    let expected = format!(
        "00-{}-{}-01",
        append.span_context.trace_id(),
        append.span_context.span_id()
    );
    check!(std::str::from_utf8(traceparent) == Ok(expected.as_str()));

    broker.shutdown().await;
}

/// The gate wait must measure the permit alone, so a commit queued behind a
/// slow one reports the queueing delay and the leading commit does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gate_wait_reflects_time_queued_behind_another_commit() {
    let _serial = SERIAL.lock().await;
    drain_spans();

    let kv: Arc<dyn Kv> = Arc::new(MemKv::default());
    let committer = SubstrateCommitter::new(kv, Arc::new(SlowWalWriter), WriterGeneration(0), 1);
    let (first, second) = tokio::join!(
        committer.commit(vec![put(b"row/1", b"a")]),
        committer.commit(vec![put(b"row/2", b"b")]),
    );
    first.expect("first commit");
    second.expect("second commit");

    let spans = drain_spans();
    let mut waits: Vec<f64> = spans
        .iter()
        .filter(|span| span.name == "pg.commit")
        .map(|span| match attribute(span, "pg.gate_wait_ms") {
            Some(Value::F64(ms)) => *ms,
            other => panic!("pg.gate_wait_ms missing or not an f64: {other:?}"),
        })
        .collect();
    waits.sort_by(f64::total_cmp);

    check!(waits.len() == 2);
    // The leading commit takes the permit immediately; the queued one waits out
    // the whole append. A generous lower bound keeps this off the CI knife edge.
    check!(waits[0] < 50.0);
    check!(waits[1] > 100.0);
}

/// An unknown commit outcome is the single most important span in the feature:
/// the compute is about to be terminated, so the span has to carry `ERROR` and
/// be closed *before* the handler runs, or it never leaves the process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_indeterminate_append_exports_an_error_span_before_the_compute_dies() {
    let _serial = SERIAL.lock().await;
    drain_spans();

    let (broker, bootstrap, _dir) = boot().await;
    let kv = MemKv::default();
    let recovered = recover_live(&bootstrap, "trace-indeterminate", None, &kv)
        .await
        .expect("recovery");
    let generation = recovered.generation;
    let journal_seq = recovered.next_journal_seq;
    let (fatal_tx, fatal_rx) = oneshot::channel();
    let fatal_tx = Arc::new(Mutex::new(Some(fatal_tx)));
    let writer = Arc::new(
        ProducerWalWriter::new(
            recovered.producer,
            "__gres_wal.trace-indeterminate.r0".into(),
        )
        .with_fault_injector(Arc::new(OnceAfterCommit(AtomicBool::new(false))))
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
                .commit_group(GroupCommitRequest {
                    generation,
                    frames: vec![WalFrame {
                        journal_seq,
                        ops: vec![put(b"row/ambiguous", b"?")],
                    }],
                })
                .await
        }
    });

    tokio::time::timeout(Duration::from_secs(5), fatal_rx)
        .await
        .expect("fatal outcome deadline")
        .expect("fatal signal");
    check!(
        tokio::time::timeout(Duration::from_millis(100), task)
            .await
            .is_err(),
        "an indeterminate commit must not answer its caller"
    );

    // The handler has already fired, so the span must already be exported —
    // that ordering is the whole point.
    let spans = drain_spans();
    // Recovery's fencing barrier also appends, so pick the failed one.
    assert!(
        let Some(append) = spans
            .iter()
            .find(|span| span.name == "gres.wal_append" && span.status != Status::Unset)
    );
    check!(attribute(append, "error.type") == Some(&Value::String("indeterminate".into())));
    assert!(let Status::Error { description } = &append.status);
    check!(description.contains("flush"));

    broker.shutdown().await;
}
