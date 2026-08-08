//! Trace assertions for the two unbounded blocking waits on a gres read path:
//! the batched timestamp grant and the range-0 read barrier.
//!
//! At both places a statement stops for as long as another node takes, with
//! nothing in a waterfall to show for it. The assertions below pin the fields
//! that make the wait interpretable: whether a grant queued behind another
//! grant, and how far behind the local range-0 tail was. They do not only assert
//! that a span exists.
//!
//! Two harness rules apply, and each one was learned the expensive way:
//!
//! - Assert on exported [`SpanData`], never on a live `tracing::Span`.
//!   `tracing-opentelemetry` resolves a span's parent, status and trace id when
//!   the span *closes*.
//! - Install with `set_global_default`, not `with_default`. The conveyor
//!   flusher and the barrier's sampler both run on spawned tasks, which a
//!   thread-local subscriber never reaches.
//!
//! Each test installs its own global subscriber, which relies on the repository
//! convention of running tests under `cargo nextest` (one process per test).

use std::{
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use assert2::{assert, check};
use crabka_gres_ranges::{
    BarrierError, BatchedTsoClient, GrantLease, Range0Barrier, Range0EndSampler, Range0Frame,
    Range0Tail, TsoError, TsoTimestamp,
};
use crabka_pgkv::MemKv;
use crabka_units::millis;
use opentelemetry::{
    Value,
    trace::{SpanKind, Status, TracerProvider as _},
};
use opentelemetry_sdk::trace::{InMemorySpanExporter, Sampler, SdkTracerProvider, SpanData};
use tokio::sync::Notify;
use tracing_subscriber::{EnvFilter, Layer as _, layer::SubscriberExt as _};

struct Traces {
    provider: SdkTracerProvider,
    exporter: InMemorySpanExporter,
}

impl Traces {
    fn install() -> Self {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_simple_exporter(exporter.clone())
            .build();
        let layer = tracing_opentelemetry::layer()
            .with_tracer(provider.tracer("blocking-wait-tracing"))
            .with_filter(EnvFilter::new("crabka_gres_ranges::route=trace"));
        tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer))
            .expect("install global subscriber; run these tests under cargo nextest");
        Self { provider, exporter }
    }

    fn named(&self, name: &str) -> Vec<SpanData> {
        self.provider.force_flush().expect("flush exporter");
        self.exporter
            .get_finished_spans()
            .expect("finished spans")
            .into_iter()
            .filter(|span| span.name == name)
            .collect()
    }

    fn only(&self, name: &str) -> SpanData {
        let mut spans = self.named(name);
        assert!(spans.len() == 1, "expected exactly one {name} span");
        spans.pop().expect("checked above")
    }
}

fn attribute<'a>(span: &'a SpanData, key: &str) -> Option<&'a Value> {
    span.attributes
        .iter()
        .find(|attribute| attribute.key.as_str() == key)
        .map(|attribute| &attribute.value)
}

/// A timestamp oracle that hands out contiguous leases and never fails.
struct SequentialOracle {
    next_ts: AtomicU64,
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl crabka_gres_ranges::TsoRpc for SequentialOracle {
    async fn grant(&self, count: NonZeroU64) -> Result<GrantLease, TsoError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let first = self.next_ts.fetch_add(count.get(), Ordering::SeqCst);
        Ok(GrantLease::new(
            TsoTimestamp::new(NonZeroU64::new(first).expect("non-zero first timestamp")),
            count,
        ))
    }
}

/// An oracle that always fails, to pin the error status on a failed grant.
struct BrokenOracle;

#[async_trait::async_trait]
impl crabka_gres_ranges::TsoRpc for BrokenOracle {
    async fn grant(&self, _count: NonZeroU64) -> Result<GrantLease, TsoError> {
        Err(TsoError::FencedEpoch { epoch: 7 })
    }
}

/// A range-0 end sampler that blocks until the test releases an offset.
#[derive(Default)]
struct ReleasableSampler {
    offset: tokio::sync::Mutex<Option<i64>>,
    notify: Notify,
}

impl ReleasableSampler {
    async fn release(&self, offset: i64) {
        *self.offset.lock().await = Some(offset);
        self.notify.notify_waiters();
    }
}

#[async_trait::async_trait]
impl Range0EndSampler for ReleasableSampler {
    async fn sample_end_after_call_begins(&self) -> Result<i64, BarrierError> {
        loop {
            let notified = self.notify.notified();
            if let Some(offset) = *self.offset.lock().await {
                return Ok(offset);
            }
            notified.await;
        }
    }
}

/// The sampler as the trait object [`Range0Barrier`] takes. The function keeps
/// the concrete handle, so a test can still release it.
fn as_sampler(sampler: &Arc<ReleasableSampler>) -> Arc<dyn Range0EndSampler> {
    Arc::clone(sampler) as Arc<dyn Range0EndSampler>
}

/// A fresh range-0 tail that has already applied through `applied_through`. A
/// negative value gives an empty tail.
fn tail_applied_through(applied_through: i64) -> Range0Tail {
    let tail = Range0Tail::new(Arc::new(MemKv::default()));
    if applied_through >= 0 {
        tail.apply_committed(&Range0Frame::new(applied_through, Vec::new()))
            .expect("seed the applied offset");
    }
    tail
}

/// The one field that makes a `tso.grant` span worth reading. A caller that
/// queued behind an in-flight batch measures queueing time, not oracle latency,
/// and the duration alone cannot separate the two.
#[tokio::test]
async fn a_coalesced_timestamp_grant_is_recorded_as_batched() {
    let traces = Traces::install();
    let oracle = Arc::new(SequentialOracle {
        next_ts: AtomicU64::new(1),
        calls: AtomicUsize::new(0),
    });
    let client = BatchedTsoClient::new(Arc::clone(&oracle));

    // Both callers are polled before the conveyor's flusher runs, so the
    // second necessarily joins the first's batch.
    let (first, second) = tokio::join!(
        client.grant(NonZeroU64::new(2).expect("non-zero")),
        client.grant(NonZeroU64::new(3).expect("non-zero")),
    );
    first.expect("first grant");
    second.expect("second grant");
    // One upstream RPC for two callers is what "coalesced" means.
    check!(oracle.calls.load(Ordering::SeqCst) == 1);

    let spans = traces.named("tso.grant");
    check!(spans.len() == 2);
    let starter = spans
        .iter()
        .find(|span| attribute(span, "pg.tso.batched") == Some(&Value::Bool(false)))
        .expect("exactly one grant starts the batch");
    let joiner = spans
        .iter()
        .find(|span| attribute(span, "pg.tso.batched") == Some(&Value::Bool(true)))
        .expect("exactly one grant joins the batch");

    // The lease split is contiguous and in queue order: 2 timestamps then 3.
    check!(attribute(starter, "pg.tso.count") == Some(&Value::I64(2)));
    check!(attribute(starter, "pg.tso.first") == Some(&Value::I64(1)));
    check!(attribute(starter, "pg.tso.last") == Some(&Value::I64(2)));
    check!(attribute(joiner, "pg.tso.count") == Some(&Value::I64(3)));
    check!(attribute(joiner, "pg.tso.first") == Some(&Value::I64(3)));
    check!(attribute(joiner, "pg.tso.last") == Some(&Value::I64(5)));

    // `otel.kind` is lifted onto the span kind rather than kept as an attribute.
    check!(starter.span_kind == SpanKind::Client);
    check!(attribute(starter, "otel.kind").is_none());
    // A grant that succeeded stays `Unset` — `"OK"` is never recorded.
    check!(starter.status == Status::Unset);
}

/// A fenced oracle is the failure an operator must be able to find. It must
/// survive the batching as its own discriminator, and not as generic RPC
/// noise.
#[tokio::test]
async fn a_failed_grant_records_the_error_variant_and_message() {
    let traces = Traces::install();
    let client = BatchedTsoClient::new(Arc::new(BrokenOracle));

    let error = client
        .grant(NonZeroU64::new(1).expect("non-zero"))
        .await
        .expect_err("the oracle always fails");
    assert!(matches!(error, TsoError::FencedEpoch { epoch: 7 }));

    let span = traces.only("tso.grant");
    check!(attribute(&span, "error.type") == Some(&Value::String("fenced_epoch".into())));
    assert!(let Status::Error { description } = &span.status);
    check!(description.as_ref() == "timestamp oracle epoch 7 was fenced");
    // Nothing was granted, so neither timestamp bound was recorded.
    check!(attribute(&span, "pg.tso.first").is_none());
    check!(attribute(&span, "pg.tso.last").is_none());
}

/// The only job of the write-side barrier is to wait out a catch-up, so the
/// span must report the distance it waited. That is the applied offset when the
/// barrier opened, and the offset the barrier waited for.
#[tokio::test]
async fn the_fresh_end_barrier_reports_the_catch_up_distance_it_waited_out() {
    let traces = Traces::install();
    let sampler = Arc::new(ReleasableSampler::default());
    let tail = tail_applied_through(2);
    let barrier = Range0Barrier::with_timeout(tail.clone(), as_sampler(&sampler), millis(5_000));

    let waiter = tokio::spawn({
        let barrier = barrier.clone();
        async move { barrier.wait_for_fresh_end().await }
    });
    sampler.release(5).await;
    // The tail is at 2 and the sample says 5, so the wait cannot finish until
    // the frame at 5 lands.
    tokio::task::yield_now().await;
    tail.apply_committed(&Range0Frame::new(5, Vec::new()))
        .expect("apply the sampled end");
    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("the barrier must complete once the tail catches up")
        .expect("join")
        .expect("barrier");

    let span = traces.only("range.barrier");
    check!(attribute(&span, "pg.barrier.mode") == Some(&Value::String("fresh_end".into())));
    check!(attribute(&span, "pg.barrier.applied_offset") == Some(&Value::I64(2)));
    check!(attribute(&span, "pg.barrier.target_offset") == Some(&Value::I64(5)));
    // This path never adopts an in-flight sample, so it always takes exactly one.
    check!(attribute(&span, "pg.barrier.polls") == Some(&Value::I64(1)));
    check!(attribute(&span, "pg.range_id") == Some(&Value::I64(0)));
    check!(span.span_kind == SpanKind::Client);
    check!(span.status == Status::Unset);
}

/// The read gate resolves through the same barrier and must be distinguishable
/// from the write-side wait in a waterfall.
#[tokio::test]
async fn the_read_gate_barrier_reports_its_own_mode() {
    let traces = Traces::install();
    let sampler = Arc::new(ReleasableSampler::default());
    sampler.release(4).await;
    let tail = tail_applied_through(4);
    let barrier = Range0Barrier::with_timeout(tail, as_sampler(&sampler), millis(5_000));

    crabka_pgexec::Linearizer::ensure_readable(&barrier)
        .await
        .expect("the tail already covers the sampled end");

    let span = traces.only("range.barrier");
    check!(attribute(&span, "pg.barrier.mode") == Some(&Value::String("read".into())));
    check!(attribute(&span, "pg.barrier.applied_offset") == Some(&Value::I64(4)));
    check!(attribute(&span, "pg.barrier.target_offset") == Some(&Value::I64(4)));
    check!(attribute(&span, "pg.barrier.polls") == Some(&Value::I64(1)));
    check!(span.status == Status::Unset);
}

/// A tail that never catches up is the failure the barrier exists to surface.
/// The span must name it, and must not collapse it into the client-facing
/// "unavailable".
#[tokio::test]
async fn a_barrier_that_times_out_records_the_catch_up_failure() {
    let traces = Traces::install();
    let sampler = Arc::new(ReleasableSampler::default());
    sampler.release(9).await;
    let tail = tail_applied_through(-1);
    let barrier = Range0Barrier::with_timeout(tail, as_sampler(&sampler), millis(50));

    let error = barrier
        .wait_for_fresh_end()
        .await
        .expect_err("the tail never reaches offset 9");
    assert!(matches!(error, BarrierError::CatchUpTimeout(timeout) if timeout == millis(50)));

    let span = traces.only("range.barrier");
    check!(attribute(&span, "error.type") == Some(&Value::String("catch_up_timeout".into())));
    check!(attribute(&span, "pg.barrier.target_offset") == Some(&Value::I64(9)));
    assert!(let Status::Error { description } = &span.status);
    check!(description.as_ref() == "range-0 tail did not catch up within 50ms");
}
