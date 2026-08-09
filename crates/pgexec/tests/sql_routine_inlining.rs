//! The two ways Gres can differ when it inlines a `LANGUAGE sql` routine.
//!
//! Gres reaches a SQL routine only when it inlines the final query of the
//! routine into the calling query. That is faithful for the shapes
//! `PostgreSQL` itself inlines. It is wrong in two specific ways that Gres has
//! to refuse rather than answer:
//!
//! - a body with several statements would run only its last statement, and
//!   would silently drop the writes that the earlier ones do;
//! - an argument substituted at several parameter references would be evaluated
//!   once per reference rather than once per call.

use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(s: &mut SqlSession, sql: &str) {
    s.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}

async fn scalar(s: &mut SqlSession, sql: &str) -> Option<String> {
    match &s
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))[0]
    {
        QueryResult::Rows { rows, .. } => rows[0][0]
            .as_ref()
            .map(|c: &Cell| String::from_utf8(c.text.to_vec()).expect("utf8")),
        other => panic!("expected rows, got {other:?}"),
    }
}

/// Gres refuses a multi-statement body.
///
/// If it ran only the last statement, it would return the right answer but
/// drop the earlier writes.
#[tokio::test]
async fn a_sql_body_with_several_statements_is_refused_rather_than_partly_run() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE audit (v int)").await;
    run(
        &mut s,
        "CREATE FUNCTION f(int) RETURNS int LANGUAGE sql \
         AS 'INSERT INTO audit VALUES ($1); SELECT $1;'",
    )
    .await;

    let error = s.simple_query("SELECT f(7)").await.expect_err("refused");
    assert!(error.code == "0A000", "{}", error.code);
    assert!(
        error.message.contains("several statements"),
        "{}",
        error.message
    );
    // Nothing ran: the refusal happens before the final query is inlined, so the
    // write the body would have performed did not happen either way.
    assert!(scalar(&mut s, "SELECT count(*) FROM audit").await == Some("0".to_string()));
}

/// Gres refuses an argument that may not be constant when the body uses it
/// more than once.
///
/// The cases where a duplicate use cannot change the answer still run.
#[tokio::test]
async fn an_argument_used_twice_is_refused_only_when_duplicating_it_could_matter() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE SEQUENCE s").await;
    run(&mut s, "CREATE TABLE t (a int)").await;
    run(&mut s, "INSERT INTO t VALUES (4)").await;
    run(
        &mut s,
        "CREATE FUNCTION twice(int) RETURNS int LANGUAGE sql AS 'SELECT $1 + $1'",
    )
    .await;
    run(
        &mut s,
        "CREATE FUNCTION once(int) RETURNS int LANGUAGE sql AS 'SELECT $1 + 1'",
    )
    .await;

    // A call that would consume two sequence values is refused.
    let error = s
        .simple_query("SELECT twice(nextval('s')::int)")
        .await
        .expect_err("a volatile argument used twice is refused");
    assert!(error.code == "0A000", "{}", error.code);
    assert!(
        error.message.contains("more than once"),
        "{}",
        error.message
    );

    // A literal and a column reference are free to duplicate.
    assert!(scalar(&mut s, "SELECT twice(3)").await == Some("6".to_string()));
    assert!(scalar(&mut s, "SELECT twice(a) FROM t").await == Some("8".to_string()));
    // And one reference is fine however volatile the argument is.
    assert!(scalar(&mut s, "SELECT once(nextval('s')::int)").await == Some("2".to_string()));
}
