//! Writes over a partition whose columns are declared in a different order
//! than its parent's.
//!
//! The per-leaf body resolves `RETURNING` against the leaf's own column order,
//! so a reordered leaf would contribute rows in a different shape. That is
//! refused. Without `RETURNING` no row shape escapes the statement, so the
//! write proceeds -- which is what lets `TRUNCATE` reach such a partition,
//! since it runs as an unqualified `DELETE`.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Engine, Session};

async fn run(session: &mut SqlSession, sql: &str) {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"));
}

async fn error_of(session: &mut SqlSession, sql: &str) -> String {
    session
        .simple_query(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} should have failed"))
        .message
}

/// `p_leaf` declares `(b, a)` where its parent declares `(a, b)`.
const SETUP: &str = "CREATE TABLE p (a int, b int) PARTITION BY LIST (a); \
     CREATE TABLE p_leaf (b int, a int); \
     ALTER TABLE p ATTACH PARTITION p_leaf FOR VALUES IN (1)";

async fn seeded() -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, SETUP).await;
    (engine, session)
}

async fn row_count(session: &mut SqlSession, sql: &str) -> String {
    let result = session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"));
    result
        .iter()
        .find_map(|outcome| match outcome {
            crabka_pgwire::engine::QueryResult::Rows { rows, .. } => rows.first(),
            _ => None,
        })
        .and_then(|row| row.first())
        .and_then(Option::as_ref)
        .map_or_else(
            || panic!("{sql} returned no value"),
            |cell| String::from_utf8_lossy(&cell.text).into_owned(),
        )
}

#[tokio::test]
async fn truncate_empties_a_partition_declared_out_of_order() {
    let (_engine, mut session) = seeded().await;
    run(&mut session, "INSERT INTO p VALUES (1, 10), (1, 20)").await;
    assert!(row_count(&mut session, "SELECT count(*) FROM p").await == "2");

    run(&mut session, "TRUNCATE p").await;

    assert!(row_count(&mut session, "SELECT count(*) FROM p").await == "0");
}

#[tokio::test]
async fn an_unqualified_delete_reaches_a_partition_declared_out_of_order() {
    let (_engine, mut session) = seeded().await;
    run(&mut session, "INSERT INTO p VALUES (1, 10), (1, 20)").await;

    run(&mut session, "DELETE FROM p").await;

    assert!(row_count(&mut session, "SELECT count(*) FROM p").await == "0");
}

#[tokio::test]
async fn an_update_reaches_a_partition_declared_out_of_order() {
    let (_engine, mut session) = seeded().await;
    run(&mut session, "INSERT INTO p VALUES (1, 10)").await;

    run(&mut session, "UPDATE p SET b = 99").await;

    assert!(row_count(&mut session, "SELECT b FROM p").await == "99");
}

#[tokio::test]
async fn returning_over_a_partition_declared_out_of_order_is_refused() {
    let (_engine, mut session) = seeded().await;
    run(&mut session, "INSERT INTO p VALUES (1, 10)").await;

    for (sql, verb) in [
        ("DELETE FROM p RETURNING a, b", "DELETE"),
        ("UPDATE p SET b = 1 RETURNING a, b", "UPDATE"),
    ] {
        let message = error_of(&mut session, sql).await;
        assert!(message.contains(verb), "{sql}: {message}");
        assert!(message.contains("different order"), "{sql}: {message}");
        assert!(message.contains("p_leaf"), "{sql}: {message}");
    }
}
