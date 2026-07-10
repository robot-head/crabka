//! Data-mutation (UPDATE / DELETE) semantics over MVCC: autocommit and
//! in-transaction, read-your-writes, tombstone hiding, command tags.
//!
//! NOTE: this file is named `mutation_semantics.rs` (not `update_delete.rs`) so
//! its compiled test binary does not contain the substring `update`, which
//! Windows UAC installer-detection rejects with os error 740
//! (`ERROR_ELEVATION_REQUIRED`). See the "UAC-safe target names" policy in
//! CLAUDE.md.

use crabka_pgexec::SqlEngine;
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(s: &mut impl Session, sql: &str) -> Vec<QueryResult> {
    s.simple_query(sql).await.expect("ok")
}

fn tag_of(r: &QueryResult) -> &str {
    match r {
        QueryResult::Command { tag } | QueryResult::Rows { tag, .. } => tag,
        other @ QueryResult::Empty => panic!("expected a tagged result, got {other:?}"),
    }
}

fn col0(r: &QueryResult) -> Vec<Option<String>> {
    match r {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row[0]
                    .as_ref()
                    .map(|c: &Cell| String::from_utf8(c.text.to_vec()).expect("utf8"))
            })
            .collect(),
        other => panic!("expected Rows, got {other:?}"),
    }
}

fn rows_text(r: &QueryResult) -> Vec<Vec<Option<String>>> {
    match r {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| {
                        cell.as_ref()
                            .map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
                    })
                    .collect()
            })
            .collect(),
        other => panic!("expected Rows, got {other:?}"),
    }
}

#[tokio::test]
async fn update_changes_value_and_tags_count() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4, name text)").await;
    run(&mut s, "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')").await;
    let r = run(&mut s, "UPDATE t SET name = 'z' WHERE id > 1").await;
    assert_eq!(tag_of(&r[0]), "UPDATE 2");
    let r = run(&mut s, "SELECT name FROM t ORDER BY id").await;
    assert_eq!(
        col0(&r[0]),
        vec![Some("a".into()), Some("z".into()), Some("z".into())]
    );
}

#[tokio::test]
async fn update_expression_references_current_row() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4)").await;
    run(&mut s, "INSERT INTO t VALUES (1),(2),(3)").await;
    let r = run(&mut s, "UPDATE t SET id = id + 10").await;
    assert_eq!(tag_of(&r[0]), "UPDATE 3");
    let r = run(&mut s, "SELECT id FROM t ORDER BY id").await;
    assert_eq!(
        col0(&r[0]),
        vec![Some("11".into()), Some("12".into()), Some("13".into())]
    );
}

#[tokio::test]
async fn delete_hides_rows_and_tags_count() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4)").await;
    run(&mut s, "INSERT INTO t VALUES (1),(2),(3)").await;
    let r = run(&mut s, "DELETE FROM t WHERE id = 2").await;
    assert_eq!(tag_of(&r[0]), "DELETE 1");
    let r = run(&mut s, "SELECT id FROM t ORDER BY id").await;
    assert_eq!(col0(&r[0]), vec![Some("1".into()), Some("3".into())]);
}

#[tokio::test]
async fn delete_all_then_select_is_empty() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4)").await;
    run(&mut s, "INSERT INTO t VALUES (1),(2)").await;
    assert_eq!(tag_of(&run(&mut s, "DELETE FROM t").await[0]), "DELETE 2");
    assert!(col0(&run(&mut s, "SELECT id FROM t").await[0]).is_empty());
}

#[tokio::test]
async fn update_then_delete_read_your_writes_in_txn() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4, name text)").await;
    run(&mut s, "INSERT INTO t VALUES (1,'a'),(2,'b')").await;
    run(&mut s, "BEGIN").await;
    run(&mut s, "UPDATE t SET name = 'x' WHERE id = 1").await;
    run(&mut s, "DELETE FROM t WHERE id = 2").await;
    let r = run(&mut s, "SELECT name FROM t ORDER BY id").await;
    assert_eq!(col0(&r[0]), vec![Some("x".into())]);
    run(&mut s, "ROLLBACK").await;
    let r = run(&mut s, "SELECT name FROM t ORDER BY id").await;
    assert_eq!(col0(&r[0]), vec![Some("a".into()), Some("b".into())]);
}

#[tokio::test]
#[allow(non_snake_case)]
async fn update_missing_table_is_42P01() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    let err = s
        .simple_query("UPDATE nope SET a = 1")
        .await
        .expect_err("no table");
    assert_eq!(err.code, "42P01");
}

#[tokio::test]
async fn update_unknown_column_is_42703() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4)").await;
    let err = s
        .simple_query("UPDATE t SET nope = 1")
        .await
        .expect_err("no column");
    assert_eq!(err.code, "42703");
}

#[tokio::test]
async fn fromless_select_where_false_returns_no_rows() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    // WHERE without FROM must still be honored.
    let r = run(&mut s, "SELECT 1 WHERE false").await;
    assert!(
        col0(&r[0]).is_empty(),
        "WHERE false must filter the single row out"
    );
    let r = run(&mut s, "SELECT 1 WHERE true").await;
    assert_eq!(col0(&r[0]), vec![Some("1".into())]);
}

#[tokio::test]
async fn update_and_delete_zero_matches_tag_zero() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4)").await;
    run(&mut s, "INSERT INTO t VALUES (1),(2),(3)").await;
    assert_eq!(
        tag_of(&run(&mut s, "UPDATE t SET id = 9 WHERE id = 9999").await[0]),
        "UPDATE 0"
    );
    assert_eq!(
        tag_of(&run(&mut s, "DELETE FROM t WHERE id = 9999").await[0]),
        "DELETE 0"
    );
    // a NULL-producing WHERE matches nothing
    assert_eq!(
        tag_of(&run(&mut s, "DELETE FROM t WHERE null").await[0]),
        "DELETE 0"
    );
    // table still intact
    assert_eq!(
        col0(&run(&mut s, "SELECT id FROM t ORDER BY id").await[0]).len(),
        3
    );
}

#[tokio::test]
async fn insert_returning_reports_inserted_rows_after_defaults() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE TABLE t (id serial, name text DEFAULT 'anon', n int4 NOT NULL)",
    )
    .await;

    let r = run(
        &mut s,
        "INSERT INTO t (n) VALUES (7), (8) RETURNING id, name, n",
    )
    .await;

    assert_eq!(tag_of(&r[0]), "INSERT 0 2");
    assert_eq!(
        rows_text(&r[0]),
        vec![
            vec![Some("1".into()), Some("anon".into()), Some("7".into())],
            vec![Some("2".into()), Some("anon".into()), Some("8".into())],
        ]
    );
}

#[tokio::test]
async fn update_returning_reports_rows_after_update_with_aliases_and_expressions() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4, n int4)").await;
    run(&mut s, "INSERT INTO t VALUES (1, 10), (2, 20)").await;

    let r = run(
        &mut s,
        "UPDATE t SET n = n + 1 WHERE id = 2 RETURNING id AS row_id, n, n + 10 AS bumped",
    )
    .await;

    assert_eq!(tag_of(&r[0]), "UPDATE 1");
    match &r[0] {
        QueryResult::Rows { fields, .. } => {
            assert_eq!(fields[0].name, "row_id");
            assert_eq!(fields[2].name, "bumped");
        }
        other => panic!("expected Rows, got {other:?}"),
    }
    assert_eq!(
        rows_text(&r[0]),
        vec![vec![Some("2".into()), Some("21".into()), Some("31".into())]]
    );
}

#[tokio::test]
async fn delete_returning_reports_deleted_rows_and_wildcard_shape() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4, name text)").await;
    run(&mut s, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").await;

    let r = run(&mut s, "DELETE FROM t WHERE id > 1 RETURNING *").await;

    assert_eq!(tag_of(&r[0]), "DELETE 2");
    assert_eq!(
        rows_text(&r[0]),
        vec![
            vec![Some("2".into()), Some("b".into())],
            vec![Some("3".into()), Some("c".into())],
        ]
    );
    assert_eq!(
        col0(&run(&mut s, "SELECT id FROM t").await[0]),
        vec![Some("1".into())]
    );
}

#[tokio::test]
async fn non_returning_dml_still_uses_command_results() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4)").await;

    assert!(matches!(
        &run(&mut s, "INSERT INTO t VALUES (1)").await[0],
        QueryResult::Command { tag } if tag == "INSERT 0 1"
    ));
    assert!(matches!(
        &run(&mut s, "UPDATE t SET id = 2").await[0],
        QueryResult::Command { tag } if tag == "UPDATE 1"
    ));
    assert!(matches!(
        &run(&mut s, "DELETE FROM t").await[0],
        QueryResult::Command { tag } if tag == "DELETE 1"
    ));
}
