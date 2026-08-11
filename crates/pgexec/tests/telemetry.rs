//! Executor tracing: what a statement's spans carry, and that they survive the
//! hop onto the blocking pool.
//!
//! Every assertion runs against exported [`SpanData`], and never against a live
//! `tracing::Span` handle. `tracing-opentelemetry` resolves a re-parented
//! span's trace id when the span closes, and not when its parent is set, so a
//! live handle reports the pre-parenting trace.
//!
//! The subscriber is installed with `set_global_default`, and not with
//! `with_default`. A thread-local subscriber is invisible on
//! `tokio::task::spawn_blocking` threads, so a test that crosses one would
//! silently observe zero spans and pass while it proved nothing. `nextest` runs
//! each test in its own process, which is what makes one global default per
//! test sound.

use std::sync::Arc;

use assert2::{assert, check};
use crabka_pgexec::SqlEngine;
use crabka_pgwire::engine::{CollectingResultSink, Engine as _, Session as _};
use crabka_trace_context::TraceCarrier;
use opentelemetry::{
    Value,
    trace::{Status, TraceId, TracerProvider as _},
};
use opentelemetry_sdk::trace::{InMemorySpanExporter, Sampler, SdkTracerProvider, SpanData};
use tracing::Instrument as _;
use tracing_subscriber::layer::SubscriberExt as _;

const REMOTE: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
const REMOTE_TRACE_ID: &str = "0af7651916cd43dd8448eb211c80319c";

/// A second, distinct client trace, so a test can tell which ingress channel a
/// statement actually followed.
const OTHER: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
const OTHER_TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";

/// Install a real `OTel` tracer as the process-wide subscriber, run `f` against
/// a fresh in-memory engine, and return the spans that closed.
fn traced<F, Fut>(f: F) -> Vec<SpanData>
where
    F: FnOnce(<SqlEngine as crabka_pgwire::engine::Engine>::Session) -> Fut,
    Fut: Future<Output = ()>,
{
    // `TraceCarrier::apply_to` extracts through the global text-map
    // propagator; without one the default is a no-op and every ingress test
    // would silently observe no parent. `crabka_telemetry::init` installs the
    // same one in a real process.
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("pgexec-telemetry-test")));
    tracing::subscriber::set_global_default(subscriber).expect(
        "a global subscriber is already installed -- run these tests under \
         `cargo nextest`, which gives each one its own process. Under \
         `cargo test` the whole target shares a process, so only the first \
         test can install one and the rest fail here. See the module docs for \
         why this cannot be `with_default`.",
    );

    let engine = Arc::new(SqlEngine::new());
    let session = engine.connect();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(f(session));

    provider.force_flush().expect("flush");
    exporter.get_finished_spans().expect("finished spans")
}

/// The statement spans among `spans`.
///
/// `db.statement` is the `tracing` span name, but `tracing-opentelemetry` maps
/// the `otel.name` field onto the exported name, and OpenTelemetry's database
/// conventions ask for the query summary there. An exported statement span is
/// therefore called `SELECT orders`, and not `db.statement`. `db.system.name`
/// is what identifies one, whatever statement it ran.
fn statements<'a>(spans: &'a [SpanData], operation: &str) -> Vec<&'a SpanData> {
    spans
        .iter()
        .filter(|span| {
            text(span, "db.system.name").as_deref() == Some("postgresql")
                && text(span, "db.operation.name").as_deref() == Some(operation)
        })
        .collect()
}

fn find<'a>(spans: &'a [SpanData], name: &str) -> &'a SpanData {
    spans
        .iter()
        .find(|span| span.name == name)
        .unwrap_or_else(|| panic!("no exported span named {name} in {:?}", names(spans)))
}

fn names(spans: &[SpanData]) -> Vec<&str> {
    spans.iter().map(|span| span.name.as_ref()).collect()
}

fn attribute<'a>(span: &'a SpanData, key: &str) -> Option<&'a Value> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| &kv.value)
}

fn text(span: &SpanData, key: &str) -> Option<String> {
    attribute(span, key).map(std::string::ToString::to_string)
}

fn trace_id(hex: &str) -> TraceId {
    TraceId::from_hex(hex).expect("trace id")
}

#[test]
fn a_select_statement_span_carries_its_summary_and_target() {
    let spans = traced(|mut session| async move {
        session
            .simple_query("CREATE TABLE orders (id int4)")
            .await
            .expect("create");
        session
            .simple_query("SELECT id FROM orders")
            .await
            .expect("select");
    });

    let selects = statements(&spans, "SELECT");
    assert!(let [statement] = selects.as_slice());

    // `otel.name` becomes the exported span name, which is what the database
    // semantic conventions ask for.
    check!(statement.name == "SELECT orders");

    check!(text(statement, "db.query.summary").as_deref() == Some("SELECT orders"));
    check!(text(statement, "db.operation.name").as_deref() == Some("SELECT"));
    check!(text(statement, "db.collection.name").as_deref() == Some("orders"));
    check!(text(statement, "db.namespace").as_deref() == Some("public"));
    check!(text(statement, "db.system.name").as_deref() == Some("postgresql"));
    check!(attribute(statement, "pg.table_id").is_some());
    check!(statement.status == Status::Unset);

    // Tier 3 is off unless `CRABKA_OTLP_SQL_TEXT` says otherwise.
    check!(attribute(statement, "db.query.text").is_none());

    // The read path's spans hang off the statement.
    let select = find(&spans, "pg.select");
    check!(select.parent_span_id == statement.span_context.span_id());
    check!(attribute(select, "pg.fast_path") == Some(&Value::Bool(false)));
    check!(attribute(select, "pg.read_ts").is_some());
    check!(attribute(select, "pg.snapshot.xmin").is_some());
}

#[test]
fn an_insert_statement_span_reports_rows_affected() {
    let spans = traced(|mut session| async move {
        session
            .simple_query("CREATE TABLE orders (id int4)")
            .await
            .expect("create");
        session
            .simple_query("INSERT INTO orders VALUES (1), (2), (3)")
            .await
            .expect("insert");
    });

    let inserts = statements(&spans, "INSERT");
    assert!(let [statement] = inserts.as_slice());

    check!(text(statement, "db.query.summary").as_deref() == Some("INSERT orders"));
    check!(text(statement, "pg.rows_affected").as_deref() == Some("3"));
    check!(attribute(statement, "pg.txn.implicit") == Some(&Value::Bool(true)));

    let write = find(&spans, "pg.write");
    check!(write.parent_span_id == statement.span_context.span_id());
    check!(attribute(write, "pg.txn.xid").is_some());
}

#[test]
fn a_failing_statement_records_error_and_its_sqlstate() {
    let spans = traced(|mut session| async move {
        session
            .simple_query("SELECT id FROM missing_table")
            .await
            .expect_err("undefined table");
    });

    let selects = statements(&spans, "SELECT");
    assert!(let [statement] = selects.as_slice());
    check!(statement.status == Status::error("relation \"missing_table\" does not exist"));
    // 42P01 is `undefined_table`; it is both the response status and the
    // low-cardinality discriminator an operator groups failures by.
    check!(text(statement, "db.response.status_code").as_deref() == Some("42P01"));
    check!(text(statement, "error.type").as_deref() == Some("42P01"));
}

#[test]
fn a_statement_in_an_aborted_block_records_its_own_error() {
    let spans = traced(|mut session| async move {
        session.simple_query("BEGIN").await.expect("begin");
        session
            .simple_query("SELECT id FROM missing_table")
            .await
            .expect_err("undefined table");
        session
            .simple_query("SELECT 1")
            .await
            .expect_err("aborted block");
        session.simple_query("ROLLBACK").await.expect("rollback");
    });

    // 25P02 — `in_failed_sql_transaction`. The statement genuinely failed, so
    // its span is an error even though nothing it names is wrong.
    let aborted: Vec<_> = spans
        .iter()
        .filter(|span| text(span, "error.type").as_deref() == Some("25P02"))
        .collect();
    assert!(let [statement] = aborted.as_slice());
    check!(text(statement, "db.system.name").as_deref() == Some("postgresql"));
    check!(matches!(statement.status, Status::Error { .. }));
}

#[test]
fn a_successful_statement_leaves_its_span_unset() {
    let spans = traced(|mut session| async move {
        session.simple_query("SELECT 1").await.expect("select");
    });

    let selects = statements(&spans, "SELECT");
    assert!(let [statement] = selects.as_slice());
    check!(statement.status == Status::Unset);
    check!(attribute(statement, "error.type").is_none());
    check!(attribute(statement, "db.collection.name").is_none());
    check!(text(statement, "db.query.summary").as_deref() == Some("SELECT"));
}

#[test]
fn the_traceparent_guc_parents_the_statement_span() {
    let spans = traced(|mut session| async move {
        session
            .simple_query(&format!("SET crabka.traceparent = '{REMOTE}'"))
            .await
            .expect("set guc");
        session.simple_query("SELECT 1").await.expect("select");
    });

    let remote_trace = trace_id(REMOTE_TRACE_ID);
    let joined: Vec<_> = spans
        .iter()
        .filter(|span| span.span_context.trace_id() == remote_trace)
        .collect();
    check!(!joined.is_empty(), "no span joined the client's trace");

    let statement = joined
        .iter()
        .find(|span| text(span, "db.system.name").as_deref() == Some("postgresql"))
        .expect("statement span joined the client trace");
    check!(statement.parent_span_is_remote);
    check!(text(statement, "db.query.summary").as_deref() == Some("SELECT"));

    // The whole subtree follows, not just the statement span itself.
    let select = find(&spans, "pg.select");
    check!(select.span_context.trace_id() == remote_trace);
}

#[test]
fn a_malformed_traceparent_guc_is_ignored_without_failing_the_query() {
    let spans = traced(|mut session| async move {
        session
            .simple_query("SET crabka.traceparent = 'definitely-not-a-traceparent'")
            .await
            .expect("a bad trace header must never fail the statement that set it");
        let rows = session.simple_query("SELECT 1").await.expect("select");
        check!(rows.len() == 1);
    });

    let selects = statements(&spans, "SELECT");
    assert!(let [statement] = selects.as_slice());
    check!(!statement.parent_span_is_remote);
    check!(statement.status == Status::Unset);
}

#[test]
fn work_on_the_blocking_pool_stays_inside_the_statement_span() {
    // The assertion that protects the `spawn_blocking` brackets: the executor
    // runs on a pool thread with no ambient span of its own, so without them
    // every span the executor opens would be a root.
    let spans = traced(|mut session| async move {
        session
            .simple_query("CREATE TABLE orders (id int4)")
            .await
            .expect("create");
        session
            .simple_query("INSERT INTO orders VALUES (1)")
            .await
            .expect("insert");
        session
            .simple_query("SELECT id FROM orders")
            .await
            .expect("select");
    });

    let select = find(&spans, "pg.select");
    let read_workers: Vec<_> = spans
        .iter()
        .filter(|span| {
            span.name == "pg.blocking_worker" && text(span, "pg.worker").as_deref() == Some("read")
        })
        .collect();
    assert!(let [probe] = read_workers.as_slice());
    check!(probe.parent_span_id == select.span_context.span_id());
    check!(probe.span_context.trace_id() == select.span_context.trace_id());

    // The write path crosses its own bracket, onto a different pool thread.
    let write = find(&spans, "pg.write");
    let write_workers: Vec<_> = spans
        .iter()
        .filter(|span| {
            span.name == "pg.blocking_worker" && text(span, "pg.worker").as_deref() == Some("write")
        })
        .collect();
    assert!(let [probe] = write_workers.as_slice());
    check!(probe.parent_span_id == write.span_context.span_id());
}

#[test]
fn the_streaming_cursor_opens_its_own_statement_span_and_marks_the_fast_path() {
    let spans = traced(|mut session| async move {
        session
            .simple_query("CREATE TABLE orders (id int4)")
            .await
            .expect("create");
        session
            .simple_query("INSERT INTO orders VALUES (1), (2)")
            .await
            .expect("insert");
        let mut sink = CollectingResultSink::default();
        session
            .simple_query_into("SELECT id FROM orders", 16, &mut sink)
            .await
            .expect("streaming select");
    });

    let selects = statements(&spans, "SELECT");
    assert!(let [statement] = selects.as_slice());
    check!(statement.name == "SELECT orders");

    let select = find(&spans, "pg.select");
    check!(attribute(select, "pg.fast_path") == Some(&Value::Bool(true)));
    check!(select.parent_span_id == statement.span_context.span_id());
    check!(attribute(select, "pg.read_ts").is_some());
}

#[test]
fn a_statement_sqlcommenter_outranks_the_session_traceparent_guc() {
    // The precedence the design fixes: statement sqlcommenter > GUC > none.
    // Both channels are set here, to *different* traces, so the assertion can
    // only pass if the GUC stood down.
    let spans = traced(|mut session| async move {
        session
            .simple_query(&format!("SET crabka.traceparent = '{OTHER}'"))
            .await
            .expect("set guc");

        // Stand in for pgwire's sqlcommenter ingress, which is upstream of the
        // engine: it parents the wire-level statement span into the trace the
        // comment names, and the engine's `db.statement` inherits from there.
        let wire = tracing::info_span!("gres.statement");
        TraceCarrier::from_w3c(REMOTE, None)
            .expect("valid traceparent")
            .apply_to(&wire);
        async {
            session
                .simple_query(&format!("SELECT 1 /*traceparent='{REMOTE}'*/"))
                .await
                .expect("select");
        }
        .instrument(wire)
        .await;
    });

    let selects = statements(&spans, "SELECT");
    assert!(let [statement] = selects.as_slice());
    check!(statement.span_context.trace_id() == trace_id(REMOTE_TRACE_ID));
    check!(statement.span_context.trace_id() != trace_id(OTHER_TRACE_ID));
    // Inherited from the wire span rather than re-parented by the GUC, so the
    // engine span is a local child, not a remote root.
    check!(!statement.parent_span_is_remote);

    // The whole subtree follows the sqlcommenter, not the GUC.
    let select = find(&spans, "pg.select");
    check!(select.span_context.trace_id() == trace_id(REMOTE_TRACE_ID));
}

#[test]
fn the_guc_still_applies_to_a_statement_without_a_sqlcommenter() {
    // The other half of the precedence rule: the GUC is the fallback, not dead
    // weight. Same shape as the test above, minus the comment on the statement.
    let spans = traced(|mut session| async move {
        session
            .simple_query(&format!("SET crabka.traceparent = '{OTHER}'"))
            .await
            .expect("set guc");
        session.simple_query("SELECT 1").await.expect("select");
    });

    let selects = statements(&spans, "SELECT");
    assert!(let [statement] = selects.as_slice());
    check!(statement.span_context.trace_id() == trace_id(OTHER_TRACE_ID));
    check!(statement.parent_span_is_remote);
}

#[test]
fn integer_attributes_export_as_numbers_not_strings() {
    // OTLP has no unsigned integer type, so a `u64` or `usize` span field is
    // stringified by `tracing-opentelemetry` — and a string attribute cannot be
    // compared or range-filtered, so `pg.rows_affected > 1000` would silently
    // match nothing. Asserting the attribute merely *exists* does not catch it;
    // only asserting its OTLP type does.
    let spans = traced(|mut session| async move {
        session
            .simple_query("CREATE TABLE orders (id int4)")
            .await
            .expect("create");
        session
            .simple_query("INSERT INTO orders VALUES (1), (2), (3)")
            .await
            .expect("insert");
        session
            .simple_query("SELECT id FROM orders")
            .await
            .expect("select");
    });

    // `CREATE TABLE` is deliberately absent: its span is built before the DDL
    // runs, so the relation does not exist yet and `pg.table_id` is correctly
    // unset rather than wrongly typed.
    let numeric: [(&str, &str); 4] = [
        ("SELECT", "pg.table_id"),
        ("SELECT", "db.response.returned_rows"),
        ("SELECT", "pg.rows_affected"),
        ("INSERT", "pg.rows_affected"),
    ];
    for (operation, key) in numeric {
        let spans = statements(&spans, operation);
        assert!(let [statement] = spans.as_slice(), "{operation}");
        assert!(
            let Some(Value::I64(_)) = attribute(statement, key),
            "{operation} / {key} is not an OTLP integer"
        );
    }

    // The same rule holds on the executor's own spans, whose fields are all
    // timestamps and snapshot bounds.
    let select = find(&spans, "pg.select");
    for key in ["pg.read_ts", "pg.snapshot.xmin", "pg.snapshot.xmax"] {
        assert!(let Some(Value::I64(_)) = attribute(select, key), "pg.select / {key}");
    }
    let parse = find(&spans, "pg.parse.sql");
    for key in ["pg.sql.bytes", "pg.statements"] {
        assert!(let Some(Value::I64(_)) = attribute(parse, key), "pg.parse.sql / {key}");
    }
    let timestamp = find(&spans, "pg.timestamp.read");
    assert!(let Some(Value::I64(_)) = attribute(timestamp, "pg.read_ts"));
    let read_context = find(&spans, "pg.read_context");
    for key in ["pg.snapshot.xmin", "pg.snapshot.xmax"] {
        assert!(let Some(Value::I64(_)) = attribute(read_context, key), "pg.read_context / {key}");
    }
    // An autocommit write's xid lives on `pg.write`: its implicit transaction
    // is already torn down by the time the statement span folds its outcome.
    let write = find(&spans, "pg.write");
    assert!(let Some(Value::I64(_)) = attribute(write, "pg.txn.xid"));
}

#[test]
fn a_statement_inside_a_block_is_stamped_with_the_block_xid() {
    // The counterpart to the note above: inside an explicit block the
    // transaction outlives the statement, so `db.statement` carries the xid.
    let spans = traced(|mut session| async move {
        session
            .simple_query("CREATE TABLE orders (id int4)")
            .await
            .expect("create");
        session.simple_query("BEGIN").await.expect("begin");
        session
            .simple_query("INSERT INTO orders VALUES (1)")
            .await
            .expect("insert");
        session.simple_query("COMMIT").await.expect("commit");
    });

    let inserts = statements(&spans, "INSERT");
    assert!(let [statement] = inserts.as_slice());
    assert!(let Some(Value::I64(_)) = attribute(statement, "pg.txn.xid"));
    check!(attribute(statement, "pg.txn.implicit") == Some(&Value::Bool(false)));
}
