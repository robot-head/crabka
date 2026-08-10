//! The correlated scalar/`EXISTS` lookup as a read of a stored relation.
//!
//! `(SELECT s.c FROM s WHERE s.k = outer.k LIMIT 1)` and the `EXISTS` that
//! compiles to the same plan are answered by hashing the **whole** inner
//! relation once and probing that hash per outer row. That scan reached
//! storage without the gates every other stored-relation read passes: it took
//! no read permit, made no row-security decision, and never asked whether a
//! materialized view had been populated. So:
//!
//! - a policy that hid every row hid it from every path but this one;
//! - `row_security = off`, which must refuse rather than filter, answered;
//! - a role holding no grant at all read the relation, and with
//!   `generate_series` as the outer relation it could enumerate the key space
//!   of a table it was never granted;
//! - an unpopulated materialized view answered `NULL` instead of 55000.
//!
//! Drop the `LIMIT 1` and every one of them came out right, because that shape
//! is not eligible and falls back to re-running the subquery per outer row
//! through the gated read path. The same SQL therefore answered differently
//! depending on which plan it got, which is the shape of the bug.
//!
//! # The eligibility witness
//!
//! Every gate below is closed by *declining* to push down, so a test that only
//! checked the answer would still pass against an engine where the fast path
//! had simply been deleted. What tells the two apart here is how many times the
//! inner relation is scanned: the hash scans it **once** for the whole outer
//! relation, while the fallback scans it **once per outer row**. Each fixture
//! has five outer rows on purpose, so the two paths read 1 and 5, and
//! [`ScanLog::scans_of`] is what every assertion about "still fires" is made
//! of.
//!
//! Every expectation was measured against `PostgreSQL` 18.4.

use std::sync::{Arc, Mutex};

use assert2::assert;
use crabka_pgexec::{
    ExecError, RangeCursor, RangeScanner, ScanRequest, ScannedRow, SqlEngine, SqlSession,
    scanner::LocalRangeScanner,
};
use crabka_pgwire::engine::{Cell, Engine as _, QueryResult, Session as _};

/// A real local scanner that also records which relation each scan opened.
///
/// Counting is the whole point: both paths reach the same scanner, and only
/// the number of times they open the inner relation separates a single hash of
/// it from one re-read per outer row.
struct ScanLog {
    inner: LocalRangeScanner,
    relations: Arc<Mutex<Vec<String>>>,
}

impl ScanLog {
    /// How many times `relation` was scanned since the last reading, and reset.
    ///
    /// Reading clears the log, so setup statements are never charged to the
    /// statement under test — call it exactly once after each statement.
    fn scans_of(&self, relation: &str) -> usize {
        std::mem::take(&mut *self.relations.lock().expect("scan-log mutex"))
            .iter()
            .filter(|scanned| *scanned == relation)
            .count()
    }

    fn record(&self, request: &ScanRequest<'_>) {
        self.relations
            .lock()
            .expect("scan-log mutex")
            .push(request.table.name.name.clone());
    }
}

impl RangeScanner for ScanLog {
    fn scan(&self, request: ScanRequest<'_>) -> Result<Vec<ScannedRow>, ExecError> {
        self.record(&request);
        self.inner.scan(request)
    }

    fn scan_cursor<'a>(
        &'a self,
        request: ScanRequest<'a>,
    ) -> Result<Box<dyn RangeCursor + 'a>, ExecError> {
        self.record(&request);
        self.inner.scan_cursor(request)
    }
}

/// An engine whose scans are logged by relation, plus the log.
fn watched_engine() -> (SqlEngine, Arc<ScanLog>) {
    let scanner = Arc::new(ScanLog {
        inner: LocalRangeScanner,
        relations: Arc::new(Mutex::new(Vec::new())),
    });
    let mut engine = SqlEngine::new();
    engine.set_range_scanner(Arc::clone(&scanner) as Arc<dyn RangeScanner>);
    (engine, scanner)
}

/// Five outer rows over two inner keys: enough that "scanned once" and
/// "scanned per outer row" are 1 and 5 rather than 1 and 2, and the repeated
/// keys keep the hash doing the work a hash is for.
const FIXTURE: &str = r"
CREATE TABLE secret (id int4, payload text);
INSERT INTO secret VALUES (1, 'alpha'), (2, 'beta');
CREATE TABLE keys_t (id int4);
INSERT INTO keys_t VALUES (1), (2), (1), (2), (1);
";

/// The eligible shape: one relation, an equality against an outer column, and
/// the `LIMIT 1` that makes the subquery scalar by construction.
const LOOKUP: &str =
    "SELECT id, (SELECT s.payload FROM secret s WHERE s.id = keys_t.id LIMIT 1) FROM keys_t";

/// The same question written so it is *not* eligible. It is here as the
/// control on the witness: it must answer identically and scan five times.
const FALLBACK: &str =
    "SELECT id, (SELECT s.payload FROM secret s WHERE s.id = keys_t.id) FROM keys_t";

const EXISTS_LOOKUP: &str =
    "SELECT id, EXISTS (SELECT 1 FROM secret s WHERE s.id = keys_t.id) FROM keys_t";

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

/// Run `sql` and return each row as a comma-joined string.
async fn query(session: &mut SqlSession, sql: &str) -> Vec<String> {
    match &session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"))[0]
    {
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

/// The SQLSTATE and message `sql` fails with.
async fn query_error(session: &mut SqlSession, sql: &str) -> (String, String) {
    let error = session
        .simple_query(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} should have failed"));
    (error.code.clone(), error.message)
}

fn rows(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

/// What the fixture's five outer rows look up.
fn payloads() -> Vec<String> {
    rows(&["1,alpha", "2,beta", "1,alpha", "2,beta", "1,alpha"])
}

/// **The control, and the witness.** A granted, unpolicied, populated relation
/// is still hashed once for the whole outer relation — so every "no longer
/// pushes down" assertion below is a decision about the gates and not about
/// the shape, and a fast path that had been deleted rather than gated would
/// fail here.
#[tokio::test]
async fn an_ordinary_lookup_hashes_the_inner_relation_once() {
    let (engine, log) = watched_engine();
    let mut session = engine.connect();
    run(&mut session, FIXTURE).await;
    let _setup = log.scans_of("secret");

    assert!(query(&mut session, LOOKUP).await == payloads());
    assert!(log.scans_of("secret") == 1);

    assert!(query(&mut session, EXISTS_LOOKUP).await == rows(&["1,t", "2,t", "1,t", "2,t", "1,t"]));
    assert!(log.scans_of("secret") == 1);
}

/// The other half of the witness: the ineligible shape answers the same and
/// reads the inner relation once per outer row. Without this, "1 scan" would
/// not be evidence of anything.
#[tokio::test]
async fn the_ungated_shape_rereads_the_inner_relation_per_outer_row() {
    let (engine, log) = watched_engine();
    let mut session = engine.connect();
    run(&mut session, FIXTURE).await;
    let _setup = log.scans_of("secret");

    assert!(query(&mut session, FALLBACK).await == payloads());
    assert!(log.scans_of("secret") == 5);
}

/// A role that holds the grant but is not the owner keeps the pushdown. The
/// gate admits a permitted read; it does not narrow the fast path to owners.
#[tokio::test]
async fn a_granted_role_still_gets_the_pushdown() {
    let (engine, log) = watched_engine();
    let mut owner = engine.connect();
    run(&mut owner, FIXTURE).await;
    run(
        &mut owner,
        "CREATE ROLE reader;
         GRANT SELECT ON secret TO reader;
         GRANT SELECT ON keys_t TO reader",
    )
    .await;
    let mut reader = engine.connect();
    run(&mut reader, "SET ROLE reader").await;
    let _setup = log.scans_of("secret");

    assert!(query(&mut reader, LOOKUP).await == payloads());
    assert!(log.scans_of("secret") == 1);
}

/// **The first bypass.** A policy that admits no row hides the payload from the
/// lookup too, exactly as it always did from the shape without the `LIMIT 1`.
#[tokio::test]
async fn a_correlated_lookup_applies_row_security() {
    let (engine, log) = watched_engine();
    let mut owner = engine.connect();
    run(&mut owner, FIXTURE).await;
    run(
        &mut owner,
        "ALTER TABLE secret ENABLE ROW LEVEL SECURITY;
         CREATE POLICY nothing ON secret FOR SELECT USING (false);
         CREATE ROLE reader;
         GRANT SELECT ON secret TO reader;
         GRANT SELECT ON keys_t TO reader",
    )
    .await;
    let mut reader = engine.connect();
    run(&mut reader, "SET ROLE reader").await;
    let _setup = log.scans_of("secret");

    let hidden = rows(&["1,NULL", "2,NULL", "1,NULL", "2,NULL", "1,NULL"]);
    assert!(query(&mut reader, LOOKUP).await == hidden);
    assert!(
        log.scans_of("secret") == 5,
        "a policied relation must not be hashed"
    );
    assert!(query(&mut reader, FALLBACK).await == hidden);
    assert!(log.scans_of("secret") == 5);

    // The owner is not subject to its own policies, so its read is still the
    // shape the hash serves.
    assert!(query(&mut owner, LOOKUP).await == payloads());
    assert!(log.scans_of("secret") == 1);
}

/// The `EXISTS` form compiles to the same plan and leaked the same fact — that
/// a key exists — one boolean at a time.
#[tokio::test]
async fn a_correlated_exists_applies_row_security() {
    let (engine, log) = watched_engine();
    let mut owner = engine.connect();
    run(&mut owner, FIXTURE).await;
    run(
        &mut owner,
        "ALTER TABLE secret ENABLE ROW LEVEL SECURITY;
         CREATE POLICY nothing ON secret FOR SELECT USING (false);
         CREATE ROLE reader;
         GRANT SELECT ON secret TO reader;
         GRANT SELECT ON keys_t TO reader",
    )
    .await;
    let mut reader = engine.connect();
    run(&mut reader, "SET ROLE reader").await;
    let _setup = log.scans_of("secret");

    assert!(query(&mut reader, EXISTS_LOOKUP).await == rows(&["1,f", "2,f", "1,f", "2,f", "1,f"]));
    assert!(log.scans_of("secret") == 5);
}

/// A policy that admits part of the relation narrows the lookup rather than
/// emptying it, which is what proves the fallback answers from the policy and
/// not from nothing.
#[tokio::test]
async fn a_partial_policy_narrows_the_lookup() {
    let engine = SqlEngine::new();
    let mut owner = engine.connect();
    run(&mut owner, FIXTURE).await;
    run(
        &mut owner,
        "ALTER TABLE secret ENABLE ROW LEVEL SECURITY;
         CREATE POLICY low ON secret FOR SELECT USING (id = 1);
         CREATE ROLE reader;
         GRANT SELECT ON secret TO reader;
         GRANT SELECT ON keys_t TO reader",
    )
    .await;
    let mut reader = engine.connect();
    run(&mut reader, "SET ROLE reader").await;

    let narrowed = rows(&["1,alpha", "2,NULL", "1,alpha", "2,NULL", "1,alpha"]);
    assert!(query(&mut reader, LOOKUP).await == narrowed);
    assert!(query(&mut reader, EXISTS_LOOKUP).await == rows(&["1,t", "2,f", "1,t", "2,f", "1,t"]));
}

/// **The second bypass, and the sharpest.** The outer relation need not be a
/// table: `generate_series` is a legal one, so a role holding no grant at all
/// read a relation it was never granted by enumerating its key space. The
/// `ORDER BY` is deliberate — it puts the outer query on the materializing
/// path, so this is not the separately fixed streaming-cursor bypass.
#[tokio::test]
async fn a_correlated_lookup_needs_a_grant() {
    let (engine, log) = watched_engine();
    let mut owner = engine.connect();
    run(&mut owner, FIXTURE).await;
    run(&mut owner, "CREATE ROLE stranger; CREATE ROLE reader").await;
    run(&mut owner, "GRANT SELECT ON secret TO reader").await;

    let enumerate = "SELECT i, (SELECT s.payload FROM secret s WHERE s.id = i LIMIT 1)
                     FROM generate_series(1, 2) AS g(i) ORDER BY i";
    let mut stranger = engine.connect();
    run(&mut stranger, "SET ROLE stranger").await;
    let _setup = log.scans_of("secret");

    let (code, message) = query_error(&mut stranger, enumerate).await;
    assert!(code == "42501", "{code}: {message}");
    assert!(message == "permission denied for table secret");

    let (code, message) = query_error(
        &mut stranger,
        "SELECT i, EXISTS (SELECT 1 FROM secret s WHERE s.id = i)
         FROM generate_series(1, 2) AS g(i) ORDER BY i",
    )
    .await;
    assert!(code == "42501", "{code}: {message}");
    assert!(message == "permission denied for table secret");

    // A granted role reads it, and by the hashed path.
    let mut reader = engine.connect();
    run(&mut reader, "SET ROLE reader").await;
    let _setup = log.scans_of("secret");
    assert!(query(&mut reader, enumerate).await == rows(&["1,alpha", "2,beta"]));
    assert!(log.scans_of("secret") == 1);
}

/// **The third bypass.** `row_security = off` asks `PostgreSQL` to fail rather
/// than silently filter, and a non-owner reading a policied relation under it
/// gets 42501. The hash answered instead, which is worse than the leak above:
/// the setting exists precisely so a dump does not quietly lose rows.
#[tokio::test]
async fn row_security_off_refuses_the_lookup() {
    let engine = SqlEngine::new();
    let mut owner = engine.connect();
    run(&mut owner, FIXTURE).await;
    run(
        &mut owner,
        "ALTER TABLE secret ENABLE ROW LEVEL SECURITY;
         CREATE POLICY nothing ON secret FOR SELECT USING (false);
         CREATE ROLE reader;
         GRANT SELECT ON secret TO reader;
         GRANT SELECT ON keys_t TO reader",
    )
    .await;
    let mut reader = engine.connect();
    run(&mut reader, "SET ROLE reader").await;
    run(&mut reader, "SET row_security = off").await;

    let (code, message) = query_error(&mut reader, LOOKUP).await;
    assert!(code == "42501", "{code}: {message}");
    assert!(message == "query would be affected by row-level security policy for table \"secret\"");

    let (code, message) = query_error(&mut reader, EXISTS_LOOKUP).await;
    assert!(code == "42501", "{code}: {message}");
    assert!(message == "query would be affected by row-level security policy for table \"secret\"");
}

/// **The fourth bypass, found while closing the other three.** A materialized
/// view whose contents have never been computed is an error to read, not an
/// empty relation. The hash read its empty row space and answered `NULL` for
/// every outer key, while the same subquery without the `LIMIT 1` raised
/// 55000 — the same relation, the same session, two answers.
#[tokio::test]
async fn a_correlated_lookup_refuses_an_unpopulated_materialized_view() {
    let (engine, log) = watched_engine();
    let mut session = engine.connect();
    run(&mut session, FIXTURE).await;
    run(
        &mut session,
        "CREATE MATERIALIZED VIEW empty_mv AS SELECT id, payload FROM secret WITH NO DATA;
         CREATE MATERIALIZED VIEW filled_mv AS SELECT id, payload FROM secret",
    )
    .await;
    let _setup = log.scans_of("filled_mv");

    let (code, message) = query_error(
        &mut session,
        "SELECT id, (SELECT m.payload FROM empty_mv m WHERE m.id = keys_t.id LIMIT 1) FROM keys_t",
    )
    .await;
    assert!(code == "55000", "{code}: {message}");
    assert!(message == "materialized view \"empty_mv\" has not been populated");

    let (code, message) = query_error(
        &mut session,
        "SELECT id, EXISTS (SELECT 1 FROM empty_mv m WHERE m.id = keys_t.id) FROM keys_t",
    )
    .await;
    assert!(code == "55000", "{code}: {message}");
    assert!(message == "materialized view \"empty_mv\" has not been populated");

    // A populated one is an ordinary stored relation, and is still hashed once.
    assert!(
        query(
            &mut session,
            "SELECT id, (SELECT m.payload FROM filled_mv m WHERE m.id = keys_t.id LIMIT 1)
             FROM keys_t",
        )
        .await
            == payloads()
    );
    assert!(log.scans_of("filled_mv") == 1);
}

/// The gates compose: a granted role reading a partly-admitting relation with
/// `row_security` on gets the admitted values, and the relation it may not read
/// at all still refuses first.
#[tokio::test]
async fn the_gates_compose() {
    let engine = SqlEngine::new();
    let mut owner = engine.connect();
    run(&mut owner, FIXTURE).await;
    run(
        &mut owner,
        "CREATE TABLE other (id int4, payload text);
         INSERT INTO other VALUES (1, 'x'), (2, 'y');
         ALTER TABLE secret ENABLE ROW LEVEL SECURITY;
         CREATE POLICY low ON secret FOR SELECT USING (id = 1);
         CREATE ROLE reader;
         GRANT SELECT ON secret TO reader;
         GRANT SELECT ON keys_t TO reader",
    )
    .await;
    let mut reader = engine.connect();
    run(&mut reader, "SET ROLE reader").await;

    assert!(
        query(&mut reader, LOOKUP).await
            == rows(&["1,alpha", "2,NULL", "1,alpha", "2,NULL", "1,alpha"])
    );
    let (code, message) = query_error(
        &mut reader,
        "SELECT id, (SELECT o.payload FROM other o WHERE o.id = keys_t.id LIMIT 1) FROM keys_t",
    )
    .await;
    assert!(code == "42501", "{code}: {message}");
    assert!(message == "permission denied for table other");
}

/// **The one that was safe only because the gate was skipped.** A policy qual
/// that reads its own relation would re-enter the read path forever, so
/// `apply_row_security` keeps a recursion guard and reports 42P17 instead of
/// overflowing the stack — the qual is attacker-supplied SQL, so the overflow
/// would be remotely triggerable.
///
/// The hash never entered that guard, because it never entered the gate. So a
/// self-referencing policy did not report recursion here; it read the relation
/// its own policy protects and answered from it, while the same session's
/// direct read of the same relation raised 42P17. Writing the leak *into the
/// policy* is a shape no grant can restrain, since the qual runs whoever asks.
#[tokio::test]
async fn a_self_referencing_policy_reports_recursion_rather_than_answering() {
    let engine = SqlEngine::new();
    let mut owner = engine.connect();
    run(
        &mut owner,
        "CREATE TABLE t (id int4, payload text, ok bool);
         INSERT INTO t VALUES (1, 'alpha', true), (2, 'beta', false);
         CREATE TABLE keys_t (id int4);
         INSERT INTO keys_t VALUES (1), (2);
         CREATE ROLE reader;
         GRANT SELECT ON t TO reader;
         GRANT SELECT ON keys_t TO reader;
         ALTER TABLE t ENABLE ROW LEVEL SECURITY;
         CREATE POLICY selfref ON t FOR SELECT
           USING ((SELECT s.ok FROM t s WHERE s.id = t.id LIMIT 1))",
    )
    .await;
    let mut reader = engine.connect();
    run(&mut reader, "SET ROLE reader").await;

    let recursion = "infinite recursion detected in policy for relation \"t\"";
    let (code, message) = query_error(&mut reader, "SELECT id FROM t ORDER BY id").await;
    assert!(code == "42P17", "{code}: {message}");
    assert!(message == recursion);

    let (code, message) = query_error(
        &mut reader,
        "SELECT id, (SELECT s.payload FROM t s WHERE s.id = keys_t.id LIMIT 1) FROM keys_t",
    )
    .await;
    assert!(code == "42P17", "{code}: {message}");
    assert!(message == recursion);
}
