//! `LANGUAGE sql` routines execute their body through the owning session.

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

/// Every statement runs, and the final one supplies the scalar result.
#[tokio::test]
async fn a_sql_body_runs_every_statement_and_returns_its_final_result() {
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

    assert!(scalar(&mut s, "SELECT f(7)").await == Some("7".to_string()));
    assert!(scalar(&mut s, "SELECT count(*) FROM audit").await == Some("1".to_string()));
}

/// A routine argument evaluates exactly once, even when the body reads it twice.
#[tokio::test]
async fn a_sql_routine_binds_a_volatile_argument_once() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE SEQUENCE s").await;
    run(
        &mut s,
        "CREATE FUNCTION twice(int) RETURNS int LANGUAGE sql AS 'SELECT $1 + $1'",
    )
    .await;
    assert!(scalar(&mut s, "SELECT twice(nextval('s')::int)").await == Some("2".to_string()));
    assert!(scalar(&mut s, "SELECT nextval('s')::int").await == Some("2".to_string()));
}

#[tokio::test]
async fn a_sql_routine_binds_named_and_unused_arguments() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION named(left_arg int, ignored int, right_arg int) RETURNS int \
         LANGUAGE sql AS 'SELECT left_arg + right_arg'",
    )
    .await;

    assert!(scalar(&mut s, "SELECT named(3, 0, 4)").await == Some("7".to_string()));
}

#[tokio::test]
async fn a_sql_routine_reads_relations_with_a_row_varying_argument() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE TABLE lookup (k int PRIMARY KEY, v int); \
         CREATE TABLE input_rows (k int); \
         INSERT INTO lookup VALUES (1, 10), (2, 20); \
         INSERT INTO input_rows VALUES (1), (2)",
    )
    .await;
    run(
        &mut s,
        "CREATE FUNCTION lookup_value(input int) RETURNS int LANGUAGE sql \
         AS 'SELECT v FROM lookup WHERE k = input'",
    )
    .await;

    assert!(scalar(&mut s, "SELECT sum(lookup_value(k)) FROM input_rows").await == Some("30".to_string()));
}

#[tokio::test]
async fn a_sql_routine_returns_final_dml_returning() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE audit (v int)").await;
    run(
        &mut s,
        "CREATE FUNCTION record(int) RETURNS int LANGUAGE sql \
         AS 'INSERT INTO audit VALUES ($1) RETURNING v'",
    )
    .await;

    assert!(scalar(&mut s, "SELECT record(8)").await == Some("8".to_string()));
}

#[tokio::test]
async fn a_sql_scalar_routine_rejects_multiple_final_rows() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION too_many() RETURNS int LANGUAGE sql \
         AS 'SELECT 1 UNION ALL SELECT 2'",
    )
    .await;

    let error = s
        .simple_query("SELECT too_many()")
        .await
        .expect_err("multiple result rows must fail");
    assert!(error.code == "21000", "{}", error.code);
}

#[tokio::test]
async fn a_sql_set_function_expands_in_a_select_list() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION numbers(int) RETURNS SETOF int LANGUAGE sql \
         AS 'SELECT n FROM generate_series(1, $1) AS n'",
    )
    .await;

    let QueryResult::Rows { rows, .. } = s
        .simple_query("SELECT numbers(3)")
        .await
        .expect("set function")
        .remove(0)
    else {
        panic!("expected rows");
    };
    assert!(rows.len() == 3);
    assert!(
        rows[0][0].as_ref().is_some_and(|cell| cell.text.as_ref() == b"1"),
        "{rows:?}"
    );
    assert!(
        rows[2][0].as_ref().is_some_and(|cell| cell.text.as_ref() == b"3"),
        "{rows:?}"
    );
}

#[tokio::test]
async fn a_sql_routine_returns_a_declared_rowtype() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE TABLE pair (a int, b text); \
         CREATE FUNCTION make_pair(int) RETURNS pair LANGUAGE sql \
         AS 'SELECT $1 AS a, ''value''::text AS b'",
    )
    .await;

    assert!(scalar(&mut s, "SELECT (make_pair(4)).a").await == Some("4".to_string()));
    assert!(scalar(&mut s, "SELECT (make_pair(4)).b").await == Some("value".to_string()));
}

#[tokio::test]
async fn a_sql_procedure_binds_its_arguments_once() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE SEQUENCE s; \
         CREATE TABLE audit (v int); \
         CREATE PROCEDURE record_value(value int) LANGUAGE sql \
         AS 'INSERT INTO audit VALUES (value); INSERT INTO audit VALUES (value + 1)'",
    )
    .await;

    run(&mut s, "CALL record_value(nextval('s')::int)").await;
    assert!(scalar(&mut s, "SELECT sum(v) FROM audit").await == Some("3".to_string()));
    assert!(scalar(&mut s, "SELECT nextval('s')::int").await == Some("2".to_string()));
}

#[tokio::test]
async fn a_sql_procedure_dispatches_nested_calls() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE TABLE audit (v int); \
         CREATE PROCEDURE record_child(value int) LANGUAGE sql \
         AS 'INSERT INTO audit VALUES (value)'; \
         CREATE PROCEDURE record_parent(value int) LANGUAGE sql \
         AS 'CALL record_child(value)'",
    )
    .await;

    run(&mut s, "CALL record_parent(7)").await;
    assert!(scalar(&mut s, "SELECT v FROM audit").await == Some("7".to_string()));
}

#[tokio::test]
async fn sql_procedure_transaction_control_obeys_routine_restrictions() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE PROCEDURE sql_commit() LANGUAGE sql AS 'COMMIT'; \
         CREATE PROCEDURE sql_child_commit() LANGUAGE sql AS 'COMMIT'; \
         CREATE PROCEDURE sql_security_commit() LANGUAGE sql SECURITY DEFINER AS 'COMMIT'; \
         CREATE PROCEDURE sql_config_commit() LANGUAGE sql SET work_mem = '64MB' AS 'COMMIT'; \
         CREATE PROCEDURE sql_security_child_commit() LANGUAGE sql SECURITY DEFINER \
         AS 'CALL sql_child_commit()'",
    )
    .await;

    run(&mut s, "CALL sql_commit()").await;
    for name in [
        "sql_security_commit",
        "sql_config_commit",
        "sql_security_child_commit",
    ] {
        let error = s
            .simple_query(&format!("CALL {name}()"))
            .await
            .expect_err("restricted procedure must reject COMMIT");
        assert!(error.code == "25001", "{name}: {error:?}");
    }
}

/// Parameters inside a FROM-clause function are substituted before its query
/// is evaluated.
#[tokio::test]
async fn a_sql_body_substitutes_parameters_into_from_functions() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION singleton(int) RETURNS int LANGUAGE sql \
         AS 'SELECT n FROM generate_series($1, $1) AS n'",
    )
    .await;

    assert!(scalar(&mut s, "SELECT singleton(7)").await == Some("7".to_string()));
}
