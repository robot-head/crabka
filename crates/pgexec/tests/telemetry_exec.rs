//! The executor's own spans: reads, scans, writes, row-lock waits, the
//! referential drain, and the local commit.
//!
//! Every assertion runs against exported [`SpanData`] and not against a live
//! `tracing::Span`. The subscriber is installed with `set_global_default` and
//! not with `with_default`. Both points matter here more than anywhere else in
//! the crate. These spans are opened on `spawn_blocking` pool threads and
//! inside `std::thread::scope`, where a thread-local subscriber is invisible. A
//! test that used one would observe zero spans and pass while it proved
//! nothing. `nextest` runs each test in its own process, which is what makes
//! one global default per test sound.

use std::{sync::Arc, time::Duration};

use assert2::{assert, check};
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Engine as _, QueryResult, Session as _};
use opentelemetry::{Value, trace::TracerProvider as _};
use opentelemetry_sdk::trace::{InMemorySpanExporter, Sampler, SdkTracerProvider, SpanData};
use tracing_subscriber::layer::SubscriberExt as _;

/// Install a real `OTel` tracer as the process-wide subscriber, run `f` against
/// a fresh in-memory engine, and return the spans that closed.
///
/// This helper returns the engine and not a single session, because the
/// row-lock tests need two sessions that contend over one lock table.
fn traced<F, Fut>(f: F) -> Vec<SpanData>
where
    F: FnOnce(Arc<SqlEngine>) -> Fut,
    Fut: Future<Output = ()>,
{
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("pgexec-exec-test")));
    tracing::subscriber::set_global_default(subscriber).expect(
        "a global subscriber is already installed -- run these tests under \
         `cargo nextest`, which gives each one its own process. Under \
         `cargo test` the whole target shares a process, so only the first \
         test can install one and the rest fail here. See the module docs for \
         why this cannot be `with_default`.",
    );

    let engine = Arc::new(SqlEngine::new());
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(f(engine));

    provider.force_flush().expect("flush");
    exporter.get_finished_spans().expect("finished spans")
}

async fn run(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
}

fn all<'a>(spans: &'a [SpanData], name: &str) -> Vec<&'a SpanData> {
    spans.iter().filter(|span| span.name == name).collect()
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

/// The integer an attribute carries, or `None` when the attribute is absent.
/// The helper fails when the attribute is present but is not an OTLP integer.
///
/// OTLP has no unsigned type, so a `u64` or `usize` field is exported as a
/// *string*, which Grafana cannot sort or range-filter. A read of the counts
/// through this helper is what stops a regression to the stringified form from
/// passing unnoticed.
fn number(span: &SpanData, key: &str) -> Option<i64> {
    match attribute(span, key) {
        None => None,
        Some(Value::I64(value)) => Some(*value),
        Some(other) => panic!(
            "{}/{key} is {other:?}, not an OTLP integer",
            span.name.as_ref()
        ),
    }
}

async fn seed_orders(engine: &Arc<SqlEngine>) -> SqlSession {
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE orders (id int4, note text)").await;
    run(
        &mut session,
        "INSERT INTO orders VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    session
}

#[test]
fn a_select_scans_beneath_the_read_span_on_the_blocking_pool() {
    let spans = traced(|engine| async move {
        let mut session = seed_orders(&engine).await;
        run(&mut session, "SELECT id FROM orders").await;
    });

    // The chain the design promises, one link at a time: the statement hands
    // off to a pool thread, the executor's read runs there, and its scans hang
    // off that. Without the `spawn_blocking` bracket the read would be a root
    // in a trace of its own.
    let select = find(&spans, "pg.select");
    let workers: Vec<_> = all(&spans, "pg.blocking_worker")
        .into_iter()
        .filter(|span| text(span, "pg.worker").as_deref() == Some("read"))
        .collect();
    assert!(let [worker] = workers.as_slice());
    check!(worker.parent_span_id == select.span_context.span_id());

    let read = find(&spans, "gres.exec_read");
    check!(read.parent_span_id == worker.span_context.span_id());
    check!(read.span_context.trace_id() == select.span_context.trace_id());
    check!(number(read, "pg.rows_out") == Some(3));
    check!(number(read, "pg.blocking_query_memory_bytes").is_some());
    // Nothing planned a distributed join, so the field stays unset rather than
    // reporting a strategy no planner chose.
    check!(attribute(read, "pg.join_strategy").is_none());

    let table_scans = all(&spans, "pg.scan");
    let scan = table_scans
        .iter()
        .find(|span| text(span, "db.collection.name").as_deref() == Some("orders"))
        .expect("a scan of orders");
    check!(scan.parent_span_id == read.span_context.span_id());
    check!(scan.span_context.trace_id() == select.span_context.trace_id());
    check!(attribute(scan, "pg.sharded") == Some(&Value::Bool(false)));
    check!(attribute(scan, "pg.pushdown.predicate") == Some(&Value::Bool(false)));
    check!(number(scan, "pg.table_id").is_some());
    check!(number(scan, "pg.rows_scanned") == Some(3));
    check!(number(scan, "pg.rows_visible") == Some(3));
}

#[test]
fn a_pushed_down_predicate_is_marked_and_narrows_the_visible_rows() {
    let spans = traced(|engine| async move {
        let mut session = seed_orders(&engine).await;
        run(&mut session, "SELECT id FROM orders WHERE id = 2").await;
    });

    let pushed: Vec<_> = all(&spans, "pg.scan")
        .into_iter()
        .filter(|span| {
            text(span, "db.collection.name").as_deref() == Some("orders")
                && attribute(span, "pg.pushdown.predicate") == Some(&Value::Bool(true))
        })
        .collect();
    assert!(!pushed.is_empty(), "no scan reported a pushed predicate");
    // Every row of the table is read; one survives the pushed-down filter.
    // That ratio is the whole reason both counts are recorded.
    let total: i64 = pushed
        .iter()
        .filter_map(|span| number(span, "pg.rows_visible"))
        .sum();
    check!(total == 1);
}

#[test]
fn a_write_reports_its_ops_rows_and_absent_returning() {
    let spans = traced(|engine| async move {
        let mut session = engine.connect();
        run(&mut session, "CREATE TABLE orders (id int4)").await;
        run(&mut session, "INSERT INTO orders VALUES (1), (2), (3)").await;
    });

    let writes: Vec<_> = all(&spans, "pg.execute_write")
        .into_iter()
        .filter(|span| text(span, "db.collection.name").as_deref() == Some("orders"))
        .collect();
    assert!(let [write] = writes.as_slice());

    check!(number(write, "pg.rows_affected") == Some(3));
    check!(number(write, "pg.table_id").is_some());
    // One heap version per row, and no secondary index to maintain.
    check!(number(write, "pg.write_ops").is_some_and(|ops| ops >= 3));
    check!(number(write, "pg.index_ops") == Some(0));
    check!(number(write, "pg.triggers_fired") == Some(0));
    check!(attribute(write, "pg.returning") == Some(&Value::Bool(false)));
    // No foreign key on the table, so the drain never ran and left no field.
    check!(attribute(write, "pg.fk_checks").is_none());
    check!(all(&spans, "pg.fk.drain").is_empty());

    // The durable batch the write produced is committed under its own span.
    let commits = all(&spans, "pg.commit");
    assert!(!commits.is_empty());
    let commit = find(&spans, "pg.commit");
    check!(text(commit, "pg.commit.mode").as_deref() == Some("local"));
    check!(number(commit, "pg.commit.ops").is_some_and(|ops| ops > 0));
}

#[test]
fn an_indexed_write_with_returning_reports_both() {
    let spans = traced(|engine| async move {
        let mut session = engine.connect();
        run(&mut session, "CREATE TABLE orders (id int4, note text)").await;
        run(&mut session, "CREATE INDEX orders_note ON orders (note)").await;
        run(
            &mut session,
            "INSERT INTO orders VALUES (1, 'a'), (2, 'b') RETURNING id",
        )
        .await;
    });

    let writes: Vec<_> = all(&spans, "pg.execute_write")
        .into_iter()
        .filter(|span| {
            text(span, "db.collection.name").as_deref() == Some("orders")
                && number(span, "pg.rows_affected") == Some(2)
        })
        .collect();
    assert!(let [write] = writes.as_slice());
    check!(attribute(write, "pg.returning") == Some(&Value::Bool(true)));
    // Equality and ordered B-tree entries are both physical writes per row.
    check!(number(write, "pg.index_ops") == Some(4));
}

#[test]
fn an_uncontended_row_lock_opens_no_span() {
    // The rule that keeps write traces readable: a lock span per row touched
    // would outnumber every other span in the trace by orders of magnitude.
    let spans = traced(|engine| async move {
        let mut session = seed_orders(&engine).await;
        run(&mut session, "BEGIN").await;
        run(
            &mut session,
            "SELECT id FROM orders WHERE id = 1 FOR UPDATE",
        )
        .await;
        run(&mut session, "UPDATE orders SET note = 'z' WHERE id = 2").await;
        run(&mut session, "COMMIT").await;
    });

    // Locks were taken — the UPDATE cannot have run without one — and not one
    // of them waited.
    check!(!all(&spans, "pg.execute_write").is_empty());
    check!(
        all(&spans, "pg.lock.row").is_empty(),
        "uncontended acquires emitted lock spans: {:?}",
        names(&spans)
    );
}

#[test]
fn a_contended_row_lock_opens_exactly_one_span_naming_its_holder() {
    let spans = traced(|engine| async move {
        let mut seed = seed_orders(&engine).await;
        drop(seed.simple_query("SELECT 1").await);

        let mut holder = engine.connect();
        run(&mut holder, "BEGIN").await;
        run(&mut holder, "SELECT id FROM orders WHERE id = 1 FOR UPDATE").await;

        let waiter_engine = Arc::clone(&engine);
        let waiter = tokio::spawn(async move {
            let mut session = waiter_engine.connect();
            run(&mut session, "UPDATE orders SET note = 'x' WHERE id = 1").await;
        });

        // Long enough for the waiter to reach the blocking acquire; the join
        // below is what actually proves it got through.
        tokio::time::sleep(Duration::from_millis(200)).await;
        run(&mut holder, "COMMIT").await;
        tokio::time::timeout(Duration::from_secs(10), waiter)
            .await
            .expect("the waiter must not hang")
            .expect("waiter join");
    });

    let locks = all(&spans, "pg.lock.row");
    assert!(let [lock] = locks.as_slice(), "{:?}", names(&spans));
    check!(text(lock, "pg.lock.key_kind").as_deref() == Some("row"));
    check!(text(lock, "pg.lock.mode").as_deref() == Some("exclusive"));
    check!(text(lock, "pg.lock.outcome").as_deref() == Some("granted"));
    check!(attribute(lock, "pg.lock.waited") == Some(&Value::Bool(true)));
    check!(number(lock, "pg.table_id").is_some());
    check!(number(lock, "pg.rowid").is_some());
    check!(number(lock, "pg.txn.xid").is_some());
    // The transaction that was in the way, which is the field an operator
    // needs to find the other side of a lock-wait incident.
    let holder_xid = number(lock, "pg.lock.holder_xid").expect("a holder xid");
    check!(holder_xid != number(lock, "pg.txn.xid").expect("own xid"));

    // The wait hangs off the write that blocked, not off a root of its own.
    let writes: Vec<_> = all(&spans, "pg.execute_write")
        .into_iter()
        .map(|span| span.span_context.span_id())
        .collect();
    check!(writes.contains(&lock.parent_span_id));
}

#[test]
fn a_referential_check_drains_in_one_span_and_folds_its_count_onto_the_write() {
    let spans = traced(|engine| async move {
        let mut session = engine.connect();
        run(&mut session, "CREATE TABLE parent (id int4 PRIMARY KEY)").await;
        run(
            &mut session,
            "CREATE TABLE child (id int4 PRIMARY KEY, parent_id int4 REFERENCES parent (id))",
        )
        .await;
        run(&mut session, "INSERT INTO parent VALUES (1)").await;
        run(&mut session, "INSERT INTO child VALUES (10, 1), (11, 1)").await;
    });

    // One drain for the whole statement, never one span per referencing row.
    let drains = all(&spans, "pg.fk.drain");
    assert!(let [drain] = drains.as_slice(), "{:?}", names(&spans));
    check!(number(drain, "pg.fk_queued") == Some(2));
    check!(number(drain, "pg.fk_checks") == Some(2));
    check!(number(drain, "pg.fk_action_ops") == Some(0));

    let write = spans
        .iter()
        .find(|span| span.span_context.span_id() == drain.parent_span_id)
        .expect("the drain's parent");
    check!(write.name == "pg.execute_write");
    check!(text(write, "db.collection.name").as_deref() == Some("child"));
    check!(number(write, "pg.fk_checks") == Some(2));
}

#[test]
fn a_statement_trigger_fires_in_one_batch_span_and_is_counted_on_the_write() {
    let spans = traced(|engine| async move {
        let mut session = engine.connect();
        run(&mut session, "CREATE TABLE orders (id int4)").await;
        run(&mut session, "CREATE TABLE audit (note text)").await;
        run(
            &mut session,
            "CREATE FUNCTION note_write() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN INSERT INTO audit VALUES ('fired'); RETURN NULL; END $$",
        )
        .await;
        run(
            &mut session,
            "CREATE TRIGGER orders_audit AFTER INSERT ON orders \
             FOR EACH STATEMENT EXECUTE FUNCTION note_write()",
        )
        .await;
        run(&mut session, "INSERT INTO orders VALUES (1), (2)").await;
    });

    let batches = all(&spans, "pg.triggers");
    assert!(let [batch] = batches.as_slice(), "{:?}", names(&spans));
    check!(text(batch, "db.collection.name").as_deref() == Some("orders"));
    check!(text(batch, "pg.trigger.event").as_deref() == Some("INSERT"));
    check!(text(batch, "pg.trigger.timing").as_deref() == Some("AFTER"));
    check!(text(batch, "pg.trigger.level").as_deref() == Some("STATEMENT"));
    check!(number(batch, "pg.triggers_fired") == Some(1));

    let writes: Vec<_> = all(&spans, "pg.execute_write")
        .into_iter()
        .filter(|span| text(span, "db.collection.name").as_deref() == Some("orders"))
        .collect();
    assert!(let [write] = writes.as_slice());
    check!(number(write, "pg.triggers_fired") == Some(1));
}

#[test]
fn every_executor_count_exports_as_an_integer() {
    // The one failure mode a presence check cannot catch: `tracing` stringifies
    // `u64` and `usize` fields, and a string attribute silently matches nothing
    // in a Grafana range filter. `number` fails on a non-integer, so this table
    // is an assertion even where it only reads.
    let spans = traced(|engine| async move {
        let mut session = seed_orders(&engine).await;
        run(&mut session, "SELECT id FROM orders").await;
    });

    let cases: [(&str, &[&str]); 4] = [
        (
            "gres.exec_read",
            &["pg.rows_out", "pg.blocking_query_memory_bytes"],
        ),
        (
            "pg.scan",
            &[
                "pg.table_id",
                "pg.rows_scanned",
                "pg.rows_visible",
                "pg.rowid.start",
                "pg.rowid.end",
            ],
        ),
        (
            "pg.execute_write",
            &[
                "pg.table_id",
                "pg.rows_affected",
                "pg.write_ops",
                "pg.index_ops",
                "pg.triggers_fired",
            ],
        ),
        ("pg.commit", &["pg.commit.ops"]),
    ];

    for (name, keys) in cases {
        let span = find(&spans, name);
        for key in keys {
            check!(number(span, key).is_some(), "{name} / {key}");
        }
    }
}
