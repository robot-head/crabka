//! How a WAL apply joins the trace of the commit that produced it.
//!
//! The relationship is an OpenTelemetry **link**, never a parent. The
//! assertions here make an accidental change back to `set_remote_parent` fail
//! loudly. Every test that checks for a link also checks that the apply span is
//! *not* a child of the remote span and does not share its trace. Without that
//! second check, a parented span would satisfy a links-only assertion just as
//! well.
//!
//! There are three reasons for links. A replay at recovery can run hours after
//! the commit. One commit goes to every follower, every checkpoint service, and
//! every future replay. Under `ParentBased` sampling, a sampled remote parent
//! would force export of every apply of every sampled write, forever.
//!
//! Harness rules, both load-bearing:
//!
//! - Assert on exported [`SpanData`], never on a live `tracing::Span`. Parent,
//!   status, trace id, and links are all resolved at close.
//! - Install the propagator as well as the subscriber. Without
//!   `set_text_map_propagator`, header extraction silently yields nothing and
//!   every propagation assertion passes vacuously.

use std::sync::{Arc, OnceLock};

use assert2::{assert, check};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_gres_substrate::{
    ProducerWalWriter, SubstrateCommitter, recover_live,
    recovery::{MAX_WAL_APPLY_LINKS, WalTraceLinks, wal_apply_span},
};
use crabka_pgexec::Committer as _;
use crabka_pgkv::{Kv, MemKv, WriteOp};
use opentelemetry::{
    Value,
    trace::{SpanKind, TraceId, TracerProvider as _},
};
use opentelemetry_sdk::trace::{InMemorySpanExporter, Sampler, SdkTracerProvider, SpanData};
use tempfile::TempDir;
use tracing::Instrument as _;
use tracing_subscriber::layer::SubscriberExt as _;

/// Serialises the tests. They share one process-wide subscriber and one
/// in-memory exporter, and each test drains everything the previous test left.
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
            tracing_opentelemetry::layer().with_tracer(provider.tracer("gres-substrate-wal-apply")),
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

fn attribute<'a>(span: &'a SpanData, key: &str) -> Option<&'a Value> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .map(|attribute| &attribute.value)
}

fn only<'a>(spans: &'a [SpanData], name: &str) -> &'a SpanData {
    let matching: Vec<&SpanData> = spans.iter().filter(|span| span.name == name).collect();
    assert!(matching.len() == 1, "expected exactly one {name} span");
    matching[0]
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

fn put(key: &[u8], value: &[u8]) -> WriteOp {
    WriteOp::Put {
        key: key.to_vec(),
        value: value.to_vec(),
    }
}

/// A synthetic `traceparent` for trace `index`, in the canonical form the
/// producer stamps on a WAL record.
fn traceparent(index: u128) -> String {
    format!("00-{index:032x}-{:016x}-01", index + 1)
}

/// Close `span` and return the one exported `gres.wal_apply` span.
fn exported_apply(span: tracing::Span) -> SpanData {
    drop(span);
    let spans = drain_spans();
    only(&spans, "gres.wal_apply").clone()
}

/// The batch is the unit of linking. A batch of a thousand records from one
/// commit must produce one link. A batch that spans more traces than the cap
/// must stop at the cap and must not export a link per record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn links_are_deduplicated_by_trace_and_capped() {
    let _serial = SERIAL.lock().await;
    install_tracing();
    drain_spans();

    let mut links = WalTraceLinks::collecting();
    // Twelve distinct traces, each offered three times: dedup must collapse the
    // repeats, and the cap must stop the distinct count at eight.
    for index in 1..=12_u128 {
        let header = traceparent(index);
        for _ in 0..3 {
            links.record([("traceparent", header.as_bytes())]);
        }
    }
    check!(links.len() == 8);
    check!(MAX_WAL_APPLY_LINKS == 8);

    let apply = exported_apply(wal_apply_span("recovery", 36, &links));

    check!(apply.links.len() == 8);
    check!(attribute(&apply, "pg.wal.links") == Some(&Value::I64(8)));
    check!(attribute(&apply, "pg.wal.records") == Some(&Value::I64(36)));
    // The first eight distinct traces, in arrival order.
    let linked: Vec<TraceId> = apply
        .links
        .iter()
        .map(|link| link.span_context.trace_id())
        .collect();
    let expected: Vec<TraceId> = (1..=8_u128)
        .map(|index| TraceId::from_bytes(index.to_be_bytes()))
        .collect();
    check!(linked == expected);
}

/// Records with no trace headers must yield a span with no links at all. A link
/// built from an empty or invalid context would point a backend at a trace that
/// does not exist.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_untraced_batch_gets_a_span_with_no_links() {
    let _serial = SERIAL.lock().await;
    install_tracing();
    drain_spans();

    let mut links = WalTraceLinks::collecting();
    links.record(std::iter::empty::<(&str, &[u8])>());
    links.record([("content-type", &b"application/octet-stream"[..])]);
    // A malformed value is discarded rather than turned into a garbage link.
    links.record([("traceparent", &b"not-a-traceparent"[..])]);
    links.record([(
        "traceparent",
        b"00-00000000000000000000000000000000-0000000000000001-01".as_slice(),
    )]);
    check!(links.is_empty());

    let apply = exported_apply(wal_apply_span("follower_bootstrap", 4, &links));

    check!(apply.links.is_empty());
    check!(attribute(&apply, "pg.wal.links") == Some(&Value::I64(0)));
    check!(attribute(&apply, "pg.wal.source") == Some(&Value::String("follower_bootstrap".into())));
}

/// The decisive test. A commit made under a caller's trace, then replayed by a
/// second recovery, must appear on the apply span as a **link**. The apply must
/// be in its own trace and must not be a child of the commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replayed_commit_is_linked_from_the_apply_span_and_never_parented() {
    let _serial = SERIAL.lock().await;
    install_tracing();
    drain_spans();

    let (broker, bootstrap, _dir) = boot().await;
    let tenant = "wal-apply-links";
    let store = Arc::new(MemKv::default());
    let recovered = recover_live(&bootstrap, tenant, None, store.as_ref())
        .await
        .expect("first recovery");
    let topic = format!("__gres_wal.{tenant}.r0");
    let kv: Arc<dyn Kv> = store;
    let committer = SubstrateCommitter::new(
        kv,
        Arc::new(ProducerWalWriter::new(recovered.producer, topic)),
        recovered.generation,
        recovered.next_journal_seq,
    );

    let caller = tracing::info_span!("test.caller");
    committer
        .commit(vec![put(b"row/1", b"a")])
        .instrument(caller.clone())
        .await
        .expect("commit");
    drop(caller);

    // The commit's own spans. Recovery fences with a barrier append of its
    // own, so the append that stamped the record is the one under `pg.commit`.
    let commit_spans = drain_spans();
    let commit = only(&commit_spans, "pg.commit");
    let append = commit_spans
        .iter()
        .find(|span| {
            span.name == "gres.wal_append" && span.parent_span_id == commit.span_context.span_id()
        })
        .expect("the commit's own WAL append");
    let committed_trace = append.span_context.trace_id();
    let committed_span = append.span_context.span_id();

    // A second recovery replays the committed WAL into a fresh store; that
    // replay is what opens `gres.wal_apply`.
    let replayed = MemKv::default();
    recover_live(&bootstrap, tenant, None, &replayed)
        .await
        .expect("second recovery replays the committed WAL");
    assert!(replayed.get(b"row/1").expect("get") == Some(b"a".to_vec()));

    let replay_spans = drain_spans();
    let apply = only(&replay_spans, "gres.wal_apply");

    check!(apply.span_kind == SpanKind::Consumer);
    check!(attribute(apply, "pg.wal.source") == Some(&Value::String("recovery".into())));
    assert!(let Some(Value::I64(records)) = attribute(apply, "pg.wal.records"));
    check!(*records >= 1);
    // Recovery's own fencing barriers are appended under spans of their own, so
    // the batch legitimately carries more than the one commit's trace. The count
    // attribute must agree with the links actually exported.
    check!(
        attribute(apply, "pg.wal.links")
            == Some(&Value::I64(
                i64::try_from(apply.links.len()).expect("link count")
            ))
    );

    // The link: the append that stamped the record, by trace *and* by span.
    check!(apply.links.iter().any(|link| {
        link.span_context.trace_id() == committed_trace
            && link.span_context.span_id() == committed_span
    }));

    // And the half that stops someone "fixing" the link into a parent: the
    // apply is a root of its own trace, unrelated to any trace it links to.
    check!(apply.parent_span_id != committed_span);
    check!(apply.span_context.trace_id() != committed_trace);
    check!(
        !apply
            .links
            .iter()
            .any(|link| link.span_context.span_id() == apply.parent_span_id
                || link.span_context.trace_id() == apply.span_context.trace_id())
    );

    broker.shutdown().await;
}
