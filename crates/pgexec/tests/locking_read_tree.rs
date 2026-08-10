//! `SELECT … FOR UPDATE/SHARE` as a read of a stored relation.
//!
//! The locking read is the executor's seventh way to read a stored relation,
//! and it was the one that went around every check the other six share. It
//! scanned the named relation with a `ScanRequest` of its own, so it took no
//! [`crabka_pgexec`] read permit, ran no row-security gate, and never expanded
//! an inheritance parent to its children. All three were silent: a role holding
//! no grant read the table by asking to lock it, a policy that hid a row hid it
//! from every other path and not from this one, and `SELECT * FROM parent FOR
//! UPDATE` returned the parent's rows alone while `SELECT * FROM parent`
//! returned the tree.
//!
//! Every expectation here was measured against `PostgreSQL` 18.4. The one worth
//! naming is the policy rule: a locking read is judged by the `UPDATE` policies
//! as well as the `SELECT` ones, at every lock strength, so a relation with a
//! `FOR SELECT` policy and no `UPDATE` policy returns nothing at all to a
//! `FOR UPDATE`.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"))
}

fn cell_text(cell: Option<&Cell>) -> String {
    cell.map_or_else(
        || "NULL".to_string(),
        |cell| String::from_utf8(cell.text.to_vec()).expect("utf8"),
    )
}

/// Every row of the first result, each rendered as a comma-joined string so a
/// whole expectation is one literal.
async fn query(session: &mut SqlSession, sql: &str) -> Vec<String> {
    match &run(session, sql).await[0] {
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

async fn error_of(session: &mut SqlSession, sql: &str) -> (String, String) {
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

/// A parent, a plain child, and a child carrying a column of its own — the
/// third is what makes the column mapping observable, since its stored rows are
/// a different shape from the ones the parent reports.
const TREE: &str = r"
CREATE TABLE parent (a text, b int4);
CREATE TABLE child () INHERITS (parent);
CREATE TABLE wide (extra numeric) INHERITS (parent);
INSERT INTO parent VALUES ('P', 1);
INSERT INTO child  VALUES ('C', 2);
INSERT INTO wide   VALUES ('W', 3, 9.5);
";

async fn session_with(setup: &str) -> SqlSession {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, setup).await;
    session
}

/// The lock strengths, which differ in the lock they take and in nothing this
/// file measures.
const STRENGTHS: [&str; 4] = [
    "FOR UPDATE",
    "FOR NO KEY UPDATE",
    "FOR SHARE",
    "FOR KEY SHARE",
];

/// **The wrong answer this file exists to invert.** A locking read of a parent
/// covers the whole tree, exactly as the ordinary read of the same `FROM` does.
#[tokio::test]
async fn a_locking_read_of_a_parent_covers_the_tree() {
    for strength in STRENGTHS {
        let mut session = session_with(TREE).await;
        let sql = format!("SELECT a, b FROM parent ORDER BY a {strength}");
        assert!(
            query(&mut session, &sql).await == rows(&["C,2", "P,1", "W,3"]),
            "{sql}"
        );
    }
}

/// `ONLY` asks for the parent's own rows, and the locking read honours it.
#[tokio::test]
async fn only_stops_a_locking_read_at_the_parent() {
    let mut session = session_with(TREE).await;
    assert!(query(&mut session, "SELECT a FROM ONLY parent FOR UPDATE").await == rows(&["P"]));
}

/// A `WHERE` that only a child's row satisfies still finds it.
#[tokio::test]
async fn a_locking_read_filters_the_children_it_reached() {
    let mut session = session_with(TREE).await;
    assert!(
        query(&mut session, "SELECT a FROM parent WHERE b = 3 FOR UPDATE").await == rows(&["W"])
    );
}

/// The tree arrives in the order `PostgreSQL` appends it: the parent, then a
/// whole level before the next one.
#[tokio::test]
async fn the_tree_arrives_a_level_at_a_time() {
    let mut session = session_with(
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
    // Unordered on purpose: the append order is the answer being measured.
    assert!(
        query(&mut session, "SELECT a FROM top FOR UPDATE").await == rows(&["1", "2", "3", "4"])
    );
    assert!(query(&mut session, "SELECT a FROM top").await == rows(&["1", "2", "3", "4"]));
}

/// A relation reachable by two routes is read once, not once per route.
#[tokio::test]
async fn a_diamond_is_locked_once() {
    let mut session = session_with(
        r"
CREATE TABLE top (a int4);
CREATE TABLE b1 () INHERITS (top);
CREATE TABLE b2 () INHERITS (top);
CREATE TABLE d () INHERITS (b1, b2);
INSERT INTO top VALUES (1);
INSERT INTO d   VALUES (2);
",
    )
    .await;
    assert!(
        query(&mut session, "SELECT a FROM top ORDER BY a FOR UPDATE").await == rows(&["1", "2"])
    );
}

/// A grant on the parent covers the children, and a grant on nothing covers
/// nothing — the same rule the ordinary read follows.
#[tokio::test]
async fn a_locking_read_needs_a_grant_on_the_relation_it_names() {
    let engine = SqlEngine::new();
    let mut owner = engine.connect();
    run(&mut owner, TREE).await;
    run(
        &mut owner,
        "CREATE ROLE reader; CREATE ROLE stranger; CREATE ROLE onlooker;
         GRANT SELECT, UPDATE ON parent TO reader;
         GRANT SELECT ON parent TO onlooker",
    )
    .await;

    let mut stranger = engine.connect();
    run(&mut stranger, "SET ROLE stranger").await;
    let (code, message) = error_of(&mut stranger, "SELECT a FROM parent FOR UPDATE").await;
    assert!(code == "42501", "{code}: {message}");
    assert!(message == "permission denied for table parent");

    // A locking read reserves the rows, so `PostgreSQL` charges `UPDATE` for it
    // besides `SELECT`, at every lock strength.
    let mut onlooker = engine.connect();
    run(&mut onlooker, "SET ROLE onlooker").await;
    assert!(query(&mut onlooker, "SELECT a FROM ONLY parent").await == rows(&["P"]));
    for strength in STRENGTHS {
        let sql = format!("SELECT a FROM parent {strength}");
        let (code, _) = error_of(&mut onlooker, &sql).await;
        assert!(code == "42501", "{sql}");
    }

    // The permit is taken on the relation the query named, so the children come
    // with it and are never checked on their own.
    let mut reader = engine.connect();
    run(&mut reader, "SET ROLE reader").await;
    assert!(
        query(&mut reader, "SELECT a FROM parent ORDER BY a FOR UPDATE").await
            == rows(&["C", "P", "W"])
    );
    let (code, _) = error_of(&mut reader, "SELECT a FROM child FOR UPDATE").await;
    assert!(code == "42501");
}

/// A policy that hides a row hides it from the locking read too.
#[tokio::test]
async fn a_locking_read_applies_row_security() {
    let engine = SqlEngine::new();
    let mut owner = engine.connect();
    run(
        &mut owner,
        r"
CREATE TABLE document (id int4);
INSERT INTO document VALUES (1), (2), (3);
ALTER TABLE document ENABLE ROW LEVEL SECURITY;
CREATE POLICY low ON document USING (id = 1);
CREATE ROLE reader;
GRANT SELECT, UPDATE ON document TO reader;
",
    )
    .await;
    let mut reader = engine.connect();
    run(&mut reader, "SET ROLE reader").await;
    for strength in STRENGTHS {
        let sql = format!("SELECT id FROM document ORDER BY id {strength}");
        assert!(query(&mut reader, &sql).await == rows(&["1"]), "{sql}");
    }
    // The ordinary read has always agreed; the point is that the two now do.
    assert!(query(&mut reader, "SELECT id FROM document ORDER BY id").await == rows(&["1"]));
}

/// `PostgreSQL` judges a locking read by the `UPDATE` policies as well as the
/// `SELECT` ones. A relation whose `SELECT` policy admits one row and whose
/// `UPDATE` policy admits a different one returns neither, and a relation with
/// no `UPDATE` policy at all returns nothing — the empty permissive fold is
/// default-deny.
#[tokio::test]
async fn a_locking_read_is_judged_by_the_update_policies_too() {
    let engine = SqlEngine::new();
    let mut owner = engine.connect();
    run(
        &mut owner,
        r"
CREATE TABLE split (id int4);
INSERT INTO split VALUES (1), (2);
ALTER TABLE split ENABLE ROW LEVEL SECURITY;
CREATE POLICY readable ON split FOR SELECT USING (id = 1);
CREATE POLICY writable ON split FOR UPDATE USING (id = 2);
CREATE TABLE readonly (id int4);
INSERT INTO readonly VALUES (1), (2);
ALTER TABLE readonly ENABLE ROW LEVEL SECURITY;
CREATE POLICY readable ON readonly FOR SELECT USING (id = 1);
CREATE ROLE reader;
GRANT SELECT, UPDATE ON split TO reader;
GRANT SELECT, UPDATE ON readonly TO reader;
",
    )
    .await;
    let mut reader = engine.connect();
    run(&mut reader, "SET ROLE reader").await;
    assert!(query(&mut reader, "SELECT id FROM split ORDER BY id").await == rows(&["1"]));
    assert!(query(&mut reader, "SELECT id FROM split ORDER BY id FOR UPDATE").await == rows(&[]));
    assert!(query(&mut reader, "SELECT id FROM readonly ORDER BY id").await == rows(&["1"]));
    assert!(
        query(
            &mut reader,
            "SELECT id FROM readonly ORDER BY id FOR UPDATE"
        )
        .await
            == rows(&[])
    );
}
