//! `TRUNCATE` against foreign keys: the 0A000 that refuses a truncate leaving a
//! referencing table behind, the two ways of satisfying it, and the fact that
//! `CASCADE` widens the *set of relations truncated* rather than firing
//! `ON DELETE` actions. Every expectation is the behaviour of a live
//! `PostgreSQL` 18.4.
//!
//! **Known divergence, asserted as current behaviour rather than fixed:**
//! `PostgreSQL` emits `NOTICE: truncate cascades to table "c"` for each relation
//! `CASCADE` pulls in. This engine has no `NoticeResponse` path at all, so no
//! notice is produced and none is asserted here; the truncation itself is what
//! these tests check.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::{
    engine::{Cell, Engine, QueryResult, Session},
    error::PgError,
};

async fn run(s: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    s.simple_query(sql).await.expect("statement should succeed")
}

fn cell_text(cell: Option<&Cell>) -> Option<String> {
    cell.map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
}

fn rows_text(r: &QueryResult) -> Vec<Vec<Option<String>>> {
    match r {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| row.iter().map(|c| cell_text(c.as_ref())).collect())
            .collect(),
        o => panic!("expected Rows, got {o:?}"),
    }
}

async fn query(s: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    rows_text(&run(s, sql).await[0])
}

async fn error(s: &mut SqlSession, sql: &str) -> PgError {
    s.simple_query(sql).await.expect_err("expected error")
}

async fn engine_with(setup: &[&str]) -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    for sql in setup {
        run(&mut s, sql).await;
    }
    (engine, s)
}

fn text_row(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_string())).collect()
}

fn no_rows() -> Vec<Vec<Option<String>>> {
    Vec::new()
}

/// The whole refusal: a feature-not-supported SQLSTATE with both a `DETAIL`
/// naming the pair and a `HINT` spelling out the two ways forward.
fn blocked_by(referencing: &str, referenced: &str) -> PgError {
    PgError::error(
        "0A000",
        "cannot truncate a table referenced in a foreign key constraint",
    )
    .with_detail(format!(
        "Table \"{referencing}\" references \"{referenced}\"."
    ))
    .with_hint(format!(
        "Truncate table \"{referencing}\" at the same time, or use TRUNCATE ... CASCADE."
    ))
}

/// A parent, a child that references it, and a row in each. The child's action
/// is whatever the caller names, so the same fixture drives both the refusal and
/// the "`CASCADE` is not `ON DELETE CASCADE`" cases.
async fn pair_with(action: &str) -> (SqlEngine, SqlSession) {
    engine_with(&[
        "CREATE TABLE p (id int4 PRIMARY KEY)",
        &format!("CREATE TABLE c (id int4 PRIMARY KEY, a int4 REFERENCES p (id) {action})"),
        "INSERT INTO p VALUES (1)",
        "INSERT INTO c VALUES (100, 1)",
    ])
    .await
}

// ---------------------------------------------------------------------------
// The refusal and the two ways past it

/// Truncating a referenced table without its referencing table is refused
/// whatever the referential action says — `TRUNCATE` does not run actions, so
/// even `ON DELETE CASCADE` earns the same 0A000.
#[tokio::test]
async fn truncate_refuses_a_child_outside_the_set() {
    for action in ["", "ON DELETE CASCADE", "ON DELETE SET NULL"] {
        let (_engine, mut s) = pair_with(action).await;
        assert!(
            error(&mut s, "TRUNCATE p").await == blocked_by("c", "p"),
            "{action}"
        );
        // The refusal is total: neither relation is touched.
        assert!(query(&mut s, "SELECT id FROM p").await == vec![text_row(&["1"])]);
        assert!(query(&mut s, "SELECT id, a FROM c").await == vec![text_row(&["100", "1"])]);
    }
}

/// Naming both relations in one `TRUNCATE` satisfies the check: the constraint's
/// referencing side is inside the set being emptied, so nothing can be left
/// dangling.
#[tokio::test]
async fn truncate_naming_both_relations_succeeds() {
    let (_engine, mut s) = pair_with("").await;
    run(&mut s, "TRUNCATE p, c").await;
    assert!(query(&mut s, "SELECT id FROM p").await == no_rows());
    assert!(query(&mut s, "SELECT id FROM c").await == no_rows());
}

/// `CASCADE` widens the *set of relations truncated*; it does not fire
/// `ON DELETE CASCADE`.
///
/// The two are told apart by rows a referential action would never touch. A
/// child row whose key is NULL references no parent row at all, and a child
/// under `ON DELETE SET NULL` would survive a cascaded delete with its column
/// nulled. `TRUNCATE p CASCADE` removes both, because it empties the relation
/// rather than walking keys.
#[tokio::test]
async fn truncate_cascade_widens_the_set_rather_than_firing_on_delete_actions() {
    for action in ["ON DELETE CASCADE", "ON DELETE SET NULL", ""] {
        let (_engine, mut s) = pair_with(action).await;
        // A second child row references nothing, so no key-driven action could
        // reach it.
        run(&mut s, "INSERT INTO c VALUES (200, NULL)").await;
        run(&mut s, "TRUNCATE p CASCADE").await;
        assert!(
            query(&mut s, "SELECT id FROM p").await == no_rows(),
            "{action}"
        );
        assert!(
            query(&mut s, "SELECT id, a FROM c").await == no_rows(),
            "{action}"
        );
    }
}

/// `CASCADE` follows the references transitively: a grandchild that references
/// the child is pulled into the set too, and a relation outside the closure is
/// left alone.
///
/// The oracle capture only exercises a two-relation `CASCADE`, so the grandchild
/// hop here follows `TRUNCATE`'s documented rule — "any tables added to the
/// group due to CASCADE" are themselves expanded — rather than a captured
/// transcript.
#[tokio::test]
async fn truncate_cascade_expands_transitively_through_a_chain() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE p (id int4 PRIMARY KEY)",
        "CREATE TABLE c (id int4 PRIMARY KEY, a int4 REFERENCES p (id))",
        "CREATE TABLE g (id int4 PRIMARY KEY, b int4 REFERENCES c (id))",
        "CREATE TABLE unrelated (id int4 PRIMARY KEY)",
        "INSERT INTO p VALUES (1)",
        "INSERT INTO c VALUES (100, 1)",
        "INSERT INTO g VALUES (1000, 100)",
        "INSERT INTO unrelated VALUES (7)",
    ])
    .await;
    // Naming the parent and the child but not the grandchild is still short of
    // the closure, and the refusal names the pair that is still split.
    assert!(error(&mut s, "TRUNCATE p, c").await == blocked_by("g", "c"));
    run(&mut s, "TRUNCATE p CASCADE").await;
    assert!(query(&mut s, "SELECT id FROM p").await == no_rows());
    assert!(query(&mut s, "SELECT id FROM c").await == no_rows());
    assert!(query(&mut s, "SELECT id FROM g").await == no_rows());
    assert!(query(&mut s, "SELECT id FROM unrelated").await == vec![text_row(&["7"])]);
}

/// A self-referencing table truncates on its own: the only relation referencing
/// it is already in the set.
#[tokio::test]
async fn truncate_of_a_self_referencing_table_succeeds() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE tree (id int4 PRIMARY KEY, parent int4 REFERENCES tree (id))",
        "INSERT INTO tree VALUES (1, NULL), (2, 1), (3, 2)",
    ])
    .await;
    run(&mut s, "TRUNCATE tree").await;
    assert!(query(&mut s, "SELECT id FROM tree").await == no_rows());
    // And it is truncatable again once refilled, this time through CASCADE.
    run(&mut s, "INSERT INTO tree VALUES (1, NULL), (2, 1)").await;
    run(&mut s, "TRUNCATE tree CASCADE").await;
    assert!(query(&mut s, "SELECT id FROM tree").await == no_rows());
}

/// A mutually referencing pair behaves the same way: each is the other's
/// referencing relation, so either both are named or `CASCADE` pulls the other
/// in.
#[tokio::test]
async fn truncate_of_a_mutually_referencing_pair_needs_both() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE m1 (id int4 PRIMARY KEY, other int4)",
        "CREATE TABLE m2 (id int4 PRIMARY KEY, a int4 REFERENCES m1 (id))",
        "ALTER TABLE m1 ADD CONSTRAINT m1_other_fkey FOREIGN KEY (other) REFERENCES m2 (id)",
        "INSERT INTO m1 VALUES (1, NULL)",
        "INSERT INTO m2 VALUES (10, 1)",
        "UPDATE m1 SET other = 10 WHERE id = 1",
    ])
    .await;
    assert!(error(&mut s, "TRUNCATE m1").await == blocked_by("m2", "m1"));
    assert!(error(&mut s, "TRUNCATE m2").await == blocked_by("m1", "m2"));
    run(&mut s, "TRUNCATE m1, m2").await;
    assert!(query(&mut s, "SELECT id FROM m1").await == no_rows());
    assert!(query(&mut s, "SELECT id FROM m2").await == no_rows());
}
