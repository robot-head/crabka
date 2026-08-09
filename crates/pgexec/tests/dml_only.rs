//! `UPDATE ONLY`, `DELETE FROM ONLY` and `TRUNCATE ONLY`.
//!
//! `ONLY` restricts a statement to the named relation instead of its whole
//! inheritance tree, and the two spellings genuinely differ: without it all
//! three commands descend into every relation below the target.
//!
//! `ONLY` also has to *parse*, and it used to not: `only` was taken as the table
//! name and the real table became its alias, which surfaced as `relation "only"
//! does not exist`.

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

fn rows(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

/// A parent with one inheriting child, each holding one row.
const SETUP: &str = r"
CREATE TABLE parent (id int4, tag text);
CREATE TABLE child () INHERITS (parent);
INSERT INTO parent VALUES (1, 'p');
INSERT INTO child VALUES (2, 'c');
";

async fn tree() -> SqlSession {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, SETUP).await;
    session
}

/// `ONLY` names the relation it precedes, on every command that takes it, in
/// both the bare and the schema-qualified spelling.
#[tokio::test]
async fn only_names_the_relation_it_precedes() {
    let cases = [
        "UPDATE ONLY parent SET tag = 'x'",
        "UPDATE ONLY public.parent SET tag = 'x'",
        "UPDATE ONLY parent AS p SET tag = 'x' WHERE p.id = 1",
        "DELETE FROM ONLY parent",
        "DELETE FROM ONLY public.parent WHERE id = 1",
        "TRUNCATE ONLY parent",
        "TRUNCATE TABLE ONLY public.parent",
    ];
    for sql in cases {
        let mut session = tree().await;
        run(&mut session, sql).await;
    }
}

/// `UPDATE ONLY` writes the parent's own rows and leaves the child's alone.
#[tokio::test]
async fn update_only_touches_the_named_relation() {
    let mut session = tree().await;
    run(&mut session, "UPDATE ONLY parent SET tag = 'x'").await;
    assert!(
        query(&mut session, "SELECT id, tag FROM ONLY parent ORDER BY id").await == rows(&["1,x"])
    );
    assert!(query(&mut session, "SELECT id, tag FROM child").await == rows(&["2,c"]));
}

/// `DELETE FROM ONLY` likewise.
#[tokio::test]
async fn delete_only_touches_the_named_relation() {
    let mut session = tree().await;
    run(&mut session, "DELETE FROM ONLY parent").await;
    assert!(query(&mut session, "SELECT id FROM ONLY parent").await == rows(&[]));
    assert!(query(&mut session, "SELECT id FROM child").await == rows(&["2"]));
}

/// `TRUNCATE ONLY` likewise, and `ONLY` binds to one name in a list rather than
/// to the whole list.
#[tokio::test]
async fn truncate_only_binds_per_name() {
    let mut session = tree().await;
    run(&mut session, "CREATE TABLE other (id int4)").await;
    run(&mut session, "INSERT INTO other VALUES (9)").await;
    run(&mut session, "TRUNCATE ONLY parent, other").await;
    assert!(query(&mut session, "SELECT id FROM ONLY parent").await == rows(&[]));
    assert!(query(&mut session, "SELECT id FROM other").await == rows(&[]));
    assert!(query(&mut session, "SELECT id FROM child").await == rows(&["2"]));
}

/// Omitting `ONLY` reaches the children, on all three commands.
///
/// This is the case that made the flag worth honouring: `SELECT count(*) FROM
/// parent` has always counted the child's row, so a `DELETE FROM parent` that
/// walked past it left the hierarchy holding rows the same statement claimed to
/// have removed.
#[tokio::test]
async fn omitting_only_reaches_the_children() {
    let mut session = tree().await;
    run(&mut session, "UPDATE parent SET tag = 'x'").await;
    assert!(query(&mut session, "SELECT tag FROM child").await == rows(&["x"]));

    run(&mut session, "DELETE FROM parent").await;
    assert!(query(&mut session, "SELECT id FROM child").await == rows(&[]));

    run(&mut session, "INSERT INTO parent VALUES (1, 'p')").await;
    run(&mut session, "INSERT INTO child VALUES (2, 'c')").await;
    run(&mut session, "TRUNCATE parent").await;
    assert!(query(&mut session, "SELECT id FROM child").await == rows(&[]));
}

/// The command tag counts every row the statement touched, across the tree.
#[tokio::test]
async fn the_command_tag_counts_the_whole_tree() {
    let cases = [
        ("UPDATE parent SET tag = 'x'", "UPDATE 2"),
        ("UPDATE ONLY parent SET tag = 'x'", "UPDATE 1"),
        ("DELETE FROM parent", "DELETE 2"),
        ("DELETE FROM ONLY parent", "DELETE 1"),
    ];
    for (sql, expected) in cases {
        let mut session = tree().await;
        let tag = match &run(&mut session, sql).await[0] {
            QueryResult::Command { tag } | QueryResult::Rows { tag, .. } => tag.clone(),
            other @ QueryResult::Empty => panic!("expected a tag from {sql}, got {other:?}"),
        };
        assert!(tag == expected, "{sql} reported {tag}, expected {expected}");
    }
}

/// `only` is still an ordinary identifier when no name follows it, so a table
/// actually called `only` remains reachable.
#[tokio::test]
async fn a_table_called_only_is_still_reachable() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE only (id int4)").await;
    run(&mut session, "INSERT INTO only VALUES (1)").await;
    run(&mut session, "UPDATE only SET id = 2").await;
    assert!(query(&mut session, "SELECT id FROM only").await == rows(&["2"]));
    run(&mut session, "DELETE FROM only").await;
    assert!(query(&mut session, "SELECT id FROM only").await == rows(&[]));
    run(&mut session, "TRUNCATE only").await;
}
