//! The wire path's streaming cursor as a read of a stored relation.
//!
//! `simple_query_into` serves a plain single-table `SELECT` from a streaming
//! cursor that scans exactly one relation. It reached that scan without the
//! three gates every other stored-relation read passes: it took no read
//! permit, ran no row-security gate, and expanded no inheritance parent. A
//! role holding no grant read the table, a policy that hid a row hid it from
//! every other path and not from this one, and `SELECT * FROM parent`
//! returned the parent's own rows while the same query with `ORDER BY`
//! returned the tree. A fourth hole came out of the same reading: an
//! unpopulated materialized view streamed as empty rather than as 55000.
//!
//! Anything that makes a query ineligible to stream — `ORDER BY`, `DISTINCT`,
//! an aggregate, a subquery — routes to the materializing path, which has
//! always been right. That is why they survived: every test in this crate that
//! asks about grants, policies or inheritance sorts its output, and
//! `Session::simple_query` never reaches the cursor at all. **Every
//! measurement here therefore goes through `simple_query_into`**, and the
//! recorded cursor page sizes prove the shapes really are the ones the cursor
//! serves.
//!
//! Every expectation was measured against `PostgreSQL` 18.4.

use std::sync::{Arc, Mutex};

use assert2::assert;
use crabka_pgexec::{
    ExecError, RangeCursor, RangeScanner, ScanPage, ScanRequest, ScannedRow, SqlEngine, SqlSession,
    scanner::LocalRangeScanner,
};
use crabka_pgwire::engine::{Cell, CollectingResultSink, Engine as _, QueryResult, Session as _};

/// The wire page size every measurement here asks for. Distinctive on purpose:
/// it is what tells the streaming cursor apart from the materializing path.
const WIRE_PAGE: usize = 7;

/// A real local scanner that also records the page size each read asked its
/// cursor for.
///
/// Counting `scan_cursor` calls would not do: the materializing path opens a
/// cursor too, through `scanner::collect_cursor_bounded`. The page size is what
/// separates them — the bounded collector always asks for 1024 rows, while the
/// streaming cursor asks for the size the wire handed `simple_query_into`. That
/// is what keeps this file honest, because every gate below is closed by
/// *declining* to stream: without a witness, a change that made these shapes
/// ineligible for any other reason would leave the tests passing while they
/// measured nothing.
struct PageSizeScanner {
    inner: LocalRangeScanner,
    sizes: Arc<Mutex<Vec<usize>>>,
}

impl PageSizeScanner {
    /// Page sizes seen since the last reading, so setup statements are not
    /// charged to the statement under test.
    fn take(&self) -> Vec<usize> {
        std::mem::take(&mut *self.sizes.lock().expect("page-size mutex"))
    }

    /// Whether the last statement was served by the streaming cursor.
    fn streamed(&self) -> bool {
        self.take().contains(&WIRE_PAGE)
    }
}

impl RangeScanner for PageSizeScanner {
    fn scan(&self, request: ScanRequest<'_>) -> Result<Vec<ScannedRow>, ExecError> {
        self.inner.scan(request)
    }

    fn scan_cursor<'a>(
        &'a self,
        request: ScanRequest<'a>,
    ) -> Result<Box<dyn RangeCursor + 'a>, ExecError> {
        Ok(Box::new(RecordingCursor {
            inner: self.inner.scan_cursor(request)?,
            sizes: Arc::clone(&self.sizes),
        }))
    }
}

struct RecordingCursor<'a> {
    inner: Box<dyn RangeCursor + 'a>,
    sizes: Arc<Mutex<Vec<usize>>>,
}

#[async_trait::async_trait]
impl RangeCursor for RecordingCursor<'_> {
    async fn next_page(&mut self, max_rows: usize) -> Result<ScanPage, ExecError> {
        self.sizes.lock().expect("page-size mutex").push(max_rows);
        self.inner.next_page(max_rows).await
    }
}

/// An engine whose cursor page sizes are recorded, plus the recorder.
fn watched_engine() -> (SqlEngine, Arc<PageSizeScanner>) {
    let scanner = Arc::new(PageSizeScanner {
        inner: LocalRangeScanner,
        sizes: Arc::new(Mutex::new(Vec::new())),
    });
    let mut engine = SqlEngine::new();
    engine.set_range_scanner(Arc::clone(&scanner) as Arc<dyn RangeScanner>);
    (engine, scanner)
}

async fn run(session: &mut SqlSession, sql: &str) {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"));
}

fn cell_text(cell: Option<&Cell>) -> String {
    cell.map_or_else(
        || "NULL".to_string(),
        |cell| String::from_utf8(cell.text.to_vec()).expect("utf8"),
    )
}

/// Run `sql` through the wire entry point — the only one that reaches the
/// streaming cursor — and return each row as a comma-joined string.
async fn streamed(session: &mut SqlSession, sql: &str) -> Vec<String> {
    let mut sink = CollectingResultSink::default();
    session
        .simple_query_into(sql, WIRE_PAGE, &mut sink)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"));
    let results = sink.finish().expect("well-formed result pages");
    match &results[0] {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| cell_text(cell.as_ref()))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

/// The SQLSTATE and message `sql` fails with on the wire path.
async fn streamed_error(session: &mut SqlSession, sql: &str) -> (String, String) {
    let mut sink = CollectingResultSink::default();
    let error = session
        .simple_query_into(sql, WIRE_PAGE, &mut sink)
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} should have failed"));
    (error.code.clone(), error.message)
}

fn rows(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

/// A parent, a plain child, and a child carrying a column of its own — the
/// third makes the column mapping observable, since its stored rows are a
/// different shape from the ones the parent reports.
///
/// The siblings are named `c1` and `c2` rather than anything descriptive, and
/// that is not taste. `inheritance::children_of` reads them back in KV key
/// order, and `pgkv::key::push_key_part` writes a four-byte length before the
/// name — so siblings come back ordered by name *length* first, while
/// `PostgreSQL` orders them by OID, which is creation order. Two same-length
/// names created in alphabetical order agree under both rules, which keeps
/// this file measuring the gates rather than that separate divergence.
const TREE: &str = r"
CREATE TABLE parent (a text, b int4);
CREATE TABLE c1 () INHERITS (parent);
CREATE TABLE c2 (extra numeric) INHERITS (parent);
INSERT INTO parent VALUES ('P', 1);
INSERT INTO c1     VALUES ('C', 2);
INSERT INTO c2     VALUES ('W', 3, 9.5);
";

/// The control. A granted, unpolicied, childless relation still streams, so
/// every "no longer streams" assertion below is a decision about the gates and
/// not about the shape.
#[tokio::test]
async fn an_ordinary_select_still_streams() {
    let (engine, watcher) = watched_engine();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE plain (a int4); INSERT INTO plain VALUES (1), (2)",
    )
    .await;
    let _setup = watcher.take();

    assert!(streamed(&mut session, "SELECT a FROM plain").await == rows(&["1", "2"]));
    assert!(watcher.streamed());
}

/// **The wrong answer.** A streamed read of an inheritance parent covers the
/// tree, exactly as the same query with an `ORDER BY` always has.
#[tokio::test]
async fn a_streamed_select_of_a_parent_covers_the_tree() {
    let (engine, watcher) = watched_engine();
    let mut session = engine.connect();
    run(&mut session, TREE).await;
    let _setup = watcher.take();

    // Unordered on purpose: the append order is part of the answer, and adding
    // `ORDER BY` would route the query away from the cursor under test.
    assert!(
        streamed(&mut session, "SELECT a, b FROM parent").await == rows(&["P,1", "C,2", "W,3"])
    );
    assert!(
        !watcher.streamed(),
        "a parent with children must not stream"
    );
}

/// `ONLY` asks for the parent's own rows, which is what one scan of one
/// relation already computes — so this shape keeps streaming.
#[tokio::test]
async fn only_keeps_a_streamed_select_at_the_parent() {
    let (engine, watcher) = watched_engine();
    let mut session = engine.connect();
    run(&mut session, TREE).await;
    let _setup = watcher.take();

    assert!(streamed(&mut session, "SELECT a FROM ONLY parent").await == rows(&["P"]));
    assert!(watcher.streamed());
}

/// A childless leaf of a tree is one relation, and still streams.
#[tokio::test]
async fn a_leaf_of_a_tree_still_streams() {
    let (engine, watcher) = watched_engine();
    let mut session = engine.connect();
    run(&mut session, TREE).await;
    let _setup = watcher.take();

    assert!(streamed(&mut session, "SELECT a, extra FROM c2").await == rows(&["W,9.5"]));
    assert!(watcher.streamed());
}

/// A grandchild's rows arrive too, and a level at a time.
#[tokio::test]
async fn a_streamed_select_reaches_a_grandchild() {
    let mut session = SqlEngine::new().connect();
    run(
        &mut session,
        r"
CREATE TABLE top (a int4);
CREATE TABLE b1 () INHERITS (top);
CREATE TABLE b2 () INHERITS (top);
CREATE TABLE g1 () INHERITS (b1);
INSERT INTO top VALUES (1);
INSERT INTO b1  VALUES (2);
INSERT INTO b2  VALUES (3);
INSERT INTO g1  VALUES (4);
",
    )
    .await;
    assert!(streamed(&mut session, "SELECT a FROM top").await == rows(&["1", "2", "3", "4"]));
}

/// **The first bypass.** A policy that hides a row hides it from the streamed
/// read too.
#[tokio::test]
async fn a_streamed_select_applies_row_security() {
    let (engine, watcher) = watched_engine();
    let mut owner = engine.connect();
    run(
        &mut owner,
        r"
CREATE TABLE document (id int4);
INSERT INTO document VALUES (1), (2), (3);
ALTER TABLE document ENABLE ROW LEVEL SECURITY;
CREATE POLICY low ON document USING (id = 1);
CREATE ROLE reader;
GRANT SELECT ON document TO reader;
",
    )
    .await;
    let mut reader = engine.connect();
    run(&mut reader, "SET ROLE reader").await;
    let _setup = watcher.take();

    assert!(streamed(&mut reader, "SELECT id FROM document").await == rows(&["1"]));
    assert!(!watcher.streamed(), "a policied relation must not stream");

    // The owner is not subject to its own policies, so its read is still the
    // shape the cursor serves.
    assert!(streamed(&mut owner, "SELECT id FROM document").await == rows(&["1", "2", "3"]));
    assert!(watcher.streamed());
}

/// A restrictive policy narrows the streamed read as well.
#[tokio::test]
async fn a_streamed_select_applies_a_restrictive_policy() {
    let engine = SqlEngine::new();
    let mut owner = engine.connect();
    run(
        &mut owner,
        r"
CREATE TABLE narrowed (id int4);
INSERT INTO narrowed VALUES (1), (2), (3), (4);
ALTER TABLE narrowed ENABLE ROW LEVEL SECURITY;
CREATE POLICY wide ON narrowed USING (id < 4);
CREATE POLICY narrow ON narrowed AS RESTRICTIVE USING (id > 1);
CREATE ROLE reader;
GRANT SELECT ON narrowed TO reader;
",
    )
    .await;
    let mut reader = engine.connect();
    run(&mut reader, "SET ROLE reader").await;
    assert!(streamed(&mut reader, "SELECT id FROM narrowed").await == rows(&["2", "3"]));
}

/// **The second bypass.** A role holding no grant cannot read the relation by
/// asking for it unsorted.
#[tokio::test]
async fn a_streamed_select_needs_a_grant() {
    let (engine, watcher) = watched_engine();
    let mut owner = engine.connect();
    run(
        &mut owner,
        "CREATE TABLE secret (id int4); INSERT INTO secret VALUES (1);
         CREATE ROLE stranger; CREATE ROLE reader; GRANT SELECT ON secret TO reader",
    )
    .await;

    let mut stranger = engine.connect();
    run(&mut stranger, "SET ROLE stranger").await;
    let _setup = watcher.take();
    let (code, message) = streamed_error(&mut stranger, "SELECT id FROM secret").await;
    assert!(code == "42501", "{code}: {message}");
    assert!(message == "permission denied for table secret");
    assert!(!watcher.streamed(), "an ungranted relation must not stream");

    // A granted role reads it, and by the streaming path.
    let mut reader = engine.connect();
    run(&mut reader, "SET ROLE reader").await;
    let _setup = watcher.take();
    assert!(streamed(&mut reader, "SELECT id FROM secret").await == rows(&["1"]));
    assert!(watcher.streamed());
}

/// The three gates compose: a policied tree read by a granted role returns the
/// admitted rows of the whole tree, judged by the parent's policies.
#[tokio::test]
async fn the_gates_compose_over_a_tree() {
    let engine = SqlEngine::new();
    let mut owner = engine.connect();
    run(
        &mut owner,
        r"
CREATE TABLE base (id int4);
CREATE TABLE leaf () INHERITS (base);
INSERT INTO base VALUES (1), (2);
INSERT INTO leaf VALUES (3), (4);
ALTER TABLE base ENABLE ROW LEVEL SECURITY;
CREATE POLICY odd ON base USING (id % 2 = 1);
CREATE ROLE reader;
GRANT SELECT ON base TO reader;
",
    )
    .await;
    let mut reader = engine.connect();
    run(&mut reader, "SET ROLE reader").await;
    assert!(streamed(&mut reader, "SELECT id FROM base").await == rows(&["1", "3"]));
}

/// **The fourth bypass, found while closing the other three.** A materialized
/// view whose contents have never been computed is an error to read, not an
/// empty relation. The cursor streamed its empty row space and answered zero
/// rows and no error at all, while every other read path refused it — the same
/// shape of hole as the three above, in the same function.
#[tokio::test]
async fn a_streamed_select_refuses_an_unpopulated_materialized_view() {
    let (engine, watcher) = watched_engine();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE t (a int4); INSERT INTO t VALUES (1), (2);
         CREATE MATERIALIZED VIEW empty AS SELECT a FROM t WITH NO DATA;
         CREATE MATERIALIZED VIEW filled AS SELECT a FROM t",
    )
    .await;
    let _setup = watcher.take();

    let (code, message) = streamed_error(&mut session, "SELECT a FROM empty").await;
    assert!(code == "55000", "{code}: {message}");
    assert!(message == "materialized view \"empty\" has not been populated");
    assert!(!watcher.streamed());

    // A populated one is an ordinary stored relation, and still streams.
    assert!(streamed(&mut session, "SELECT a FROM filled").await == rows(&["1", "2"]));
    assert!(watcher.streamed());
}

/// A view is not a stored relation, and the cursor never mistook one for a
/// table — its rows come from the view body, through the materializing path.
#[tokio::test]
async fn a_streamed_select_of_a_view_reads_the_body() {
    let (engine, watcher) = watched_engine();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE t (a int4); INSERT INTO t VALUES (1), (2);
         CREATE VIEW one AS SELECT a FROM t WHERE a = 1",
    )
    .await;
    let _setup = watcher.take();

    assert!(streamed(&mut session, "SELECT a FROM one").await == rows(&["1"]));
    assert!(!watcher.streamed());
}

/// **The fifth bypass.** A `VIRTUAL` generated column is a NULL placeholder in
/// storage; its value is produced by evaluating the catalog's expression at
/// read time, and only the materializing path does that. The cursor handed the
/// placeholder to the wire, so `SELECT * FROM t` reported a blank where the
/// same query with an `ORDER BY` reported the value — a wrong answer rather
/// than a refusal, which is why it outlived the other four.
#[tokio::test]
async fn a_streamed_select_materializes_a_virtual_generated_column() {
    let (engine, watcher) = watched_engine();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE gen (a int4, b int4 GENERATED ALWAYS AS (a * 2) VIRTUAL);
         CREATE TABLE plain (a int4);
         INSERT INTO gen (a) VALUES (1), (2);
         INSERT INTO plain VALUES (1), (2)",
    )
    .await;
    let _setup = watcher.take();

    assert!(streamed(&mut session, "SELECT * FROM gen").await == rows(&["1,2", "2,4"]));
    assert!(!watcher.streamed());

    // The same shape over a relation with no generated column still streams,
    // so the decline is about the column and not about the shape.
    assert!(streamed(&mut session, "SELECT * FROM plain").await == rows(&["1", "2"]));
    assert!(watcher.streamed());
}
