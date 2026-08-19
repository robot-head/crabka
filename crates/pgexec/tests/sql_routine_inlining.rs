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

async fn error_context(s: &mut SqlSession, sql: &str) -> String {
    let error = s.simple_query(sql).await.expect_err("statement must fail");
    error
        .diagnostics
        .as_ref()
        .and_then(|fields| fields.context.clone())
        .unwrap_or_else(|| panic!("missing context: {error:?}"))
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
async fn a_sql_routine_accepts_named_and_default_arguments() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION named_defaults(a int, b int DEFAULT 5, c int DEFAULT 9) RETURNS int \
         LANGUAGE sql AS 'SELECT a + b + c'",
    )
    .await;

    assert!(scalar(&mut s, "SELECT named_defaults(c => 3, a := 1)").await == Some("9".to_string()));
}

#[tokio::test]
async fn a_named_argument_selects_the_matching_overload() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION overloaded(value int) RETURNS text LANGUAGE sql AS 'SELECT ''int'''; \
         CREATE FUNCTION overloaded(value text) RETURNS text LANGUAGE sql AS 'SELECT ''text'''",
    )
    .await;

    assert!(scalar(&mut s, "SELECT overloaded(7)").await == Some("int".to_string()));
    assert!(scalar(&mut s, "SELECT overloaded(value => 7)").await == Some("int".to_string()));
    assert!(scalar(&mut s, "SELECT overloaded(value => 'x')").await == Some("text".to_string()));
}

/// A scalar built-in in FROM is a one-row `FunctionScan`, not an undefined SRF.
#[tokio::test]
async fn a_scalar_builtin_scans_as_one_row_from_item() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE tid_source (value int); INSERT INTO tid_source VALUES (1)").await;
    assert!(
        scalar(
            &mut s,
            "SELECT answer::text || ':' || position::text \
             FROM abs(-3::int) WITH ORDINALITY AS t(answer, position)",
        )
        .await
            == Some("3:1".to_string())
    );
    for (sql, expected) in [
        (
            "SELECT value::text FROM to_regtype('int4') AS t(value)",
            "int4",
        ),
        (
            "SELECT value::text FROM currtid2('tid_source', '(0,1)'::tid) AS t(value)",
            "(0,1)",
        ),
        (
            "SELECT value::text FROM make_date(2024, 1, 2) AS t(value)",
            "2024-01-02",
        ),
        (
            "SELECT value::text FROM date_trunc('day', timestamp '2024-01-02 03:04:05') AS t(value)",
            "2024-01-02 00:00:00",
        ),
        ("SELECT value FROM to_char(7, 'FM9') AS t(value)", "7"),
        (
            "SELECT value FROM jsonb_typeof('[1]'::jsonb) AS t(value)",
            "array",
        ),
        (
            "SELECT value::text FROM array_length(ARRAY[1,2], 1) AS t(value)",
            "2",
        ),
    ] {
        assert!(scalar(&mut s, sql).await == Some(expected.to_string()));
    }
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
async fn a_sql_routine_error_reports_its_statement_context() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION failing_context() RETURNS int VOLATILE LANGUAGE sql \
         AS 'SELECT 1; SELECT 1 / 0'",
    )
    .await;

    let error = s
        .simple_query("SELECT failing_context()")
        .await
        .expect_err("division by zero");
    assert!(error.code == "22012", "{error:?}");
    assert!(
        error
            .diagnostics
            .as_ref()
            .and_then(|fields| fields.context.as_deref())
            == Some("SQL function \"failing_context\" statement 2"),
        "{error:?}"
    );
}

#[tokio::test]
async fn every_sql_routine_executor_counts_context_statements() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION scalar_bind_context(int) RETURNS int LANGUAGE sql \
         AS 'SELECT 1; SELECT * FROM missing_scalar_bind_context'; \
         CREATE PROCEDURE procedure_bind_context(int) LANGUAGE sql \
         AS 'SELECT 1; SELECT * FROM missing_procedure_bind_context'; \
         CREATE PROCEDURE procedure_run_context() LANGUAGE sql \
         AS 'SELECT 1; SELECT 1 / 0'; \
         CREATE FUNCTION set_bind_context(int) RETURNS SETOF int LANGUAGE sql \
         AS 'SELECT 1; SELECT * FROM missing_set_bind_context'; \
         CREATE FUNCTION set_run_context() RETURNS SETOF int LANGUAGE sql \
         AS 'SELECT 1; SELECT 1 / 0'",
    )
    .await;

    for (sql, name) in [
        ("SELECT scalar_bind_context(1)", "scalar_bind_context"),
        ("CALL procedure_bind_context(1)", "procedure_bind_context"),
        ("CALL procedure_run_context()", "procedure_run_context"),
        ("SELECT set_bind_context(1)", "set_bind_context"),
        ("SELECT set_run_context()", "set_run_context"),
    ] {
        assert!(
            error_context(&mut s, sql).await == format!("SQL function \"{name}\" statement 2"),
            "{sql}"
        );
    }
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
async fn a_sql_set_function_scans_in_from_with_ordinality() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION numbers_from(int) RETURNS SETOF int LANGUAGE sql \
         AS 'SELECT n FROM generate_series(1, $1) AS n'",
    )
    .await;
    assert!(
        scalar(
            &mut s,
            "SELECT string_agg(n::text || ':' || o::text, ',' ORDER BY n) \
             FROM numbers_from(2) WITH ORDINALITY AS t(n, o)",
        )
        .await
            == Some("1:1,2:2".to_string())
    );
}

#[tokio::test]
async fn a_sql_table_function_participates_in_rows_from() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION rows_from_sql(int) RETURNS SETOF int LANGUAGE sql \
         AS 'SELECT n FROM generate_series(1, $1) AS n'",
    )
    .await;
    run(
        &mut s,
        "CREATE FUNCTION rows_from_plpgsql(limit_value int) RETURNS SETOF int LANGUAGE plpgsql \
         AS $$ BEGIN RETURN QUERY SELECT n FROM generate_series(1, limit_value) AS n; END $$",
    )
    .await;
    run(
        &mut s,
        "CREATE FUNCTION rows_from_record(int) RETURNS SETOF record LANGUAGE sql \
         AS 'SELECT n, ''value''::text FROM generate_series(1, $1) AS n'",
    )
    .await;
    run(
        &mut s,
        "CREATE VIEW rows_from_view AS \
         SELECT n FROM rows_from_plpgsql(2) AS emitted(n)",
    )
    .await;

    assert!(
        scalar(
            &mut s,
            "EXPLAIN (COSTS OFF) SELECT * \
             FROM ROWS FROM (rows_from_sql(2), generate_series(10, 12)) AS t(a, b)",
        )
        .await
            == Some("Function Scan".to_string())
    );
    assert!(
        scalar(
            &mut s,
            "EXPLAIN (COSTS OFF) SELECT * FROM (VALUES (1)) AS input(value), \
             ROWS FROM (rows_from_sql(2), generate_series(10, 12)) AS t(a, b)",
        )
        .await
            == Some("Nested Loop".to_string())
    );
    assert!(
        scalar(
            &mut s,
            "SELECT string_agg(coalesce(a::text, 'null') || ':' || b::text, ',' ORDER BY b) \
             FROM ROWS FROM (rows_from_sql(2), generate_series(10, 12)) AS t(a, b)",
        )
        .await
            == Some("1:10,2:11,null:12".to_string())
    );
    assert!(
        scalar(
            &mut s,
            "SELECT string_agg(coalesce(a::text, 'null') || ':' || b::text, ',' ORDER BY b) \
             FROM ROWS FROM (rows_from_plpgsql(1), generate_series(20, 21)) AS t(a, b)",
        )
        .await
            == Some("1:20,null:21".to_string())
    );
    assert!(
        scalar(
            &mut s,
            "SELECT string_agg(coalesce(a::text, 'null') || ':' || b::text || ':' || o::text, ',' ORDER BY o) \
             FROM ROWS FROM (rows_from_sql(2), generate_series(10, 12)) WITH ORDINALITY AS t(a, b, o)",
        )
        .await
            == Some("1:10:1,2:11:2,null:12:3".to_string())
    );
    assert!(
        scalar(
            &mut s,
            "SELECT string_agg(coalesce(a::text, 'null') || ':' || coalesce(label, 'null') || ':' || b::text || ':' || o::text, ',' ORDER BY o) \
             FROM ROWS FROM (rows_from_record(1) AS (a int, label text), generate_series(30, 32)) \
             WITH ORDINALITY AS t(a, label, b, o)",
        )
        .await
            == Some("1:value:30:1,null:null:31:2,null:null:32:3".to_string())
    );
    assert!(
        scalar(
            &mut s,
            "SELECT string_agg((left_side.n * 10 + right_side.n)::text, ',' ORDER BY left_side.n) \
             FROM (SELECT n FROM rows_from_sql(2) AS emitted(n)) AS left_side \
             JOIN rows_from_view AS right_side ON left_side.n = right_side.n",
        )
        .await
            == Some("11,22".to_string())
    );
    assert!(
        scalar(
            &mut s,
            "SELECT string_agg(emitted::text, ',' ORDER BY n) \
             FROM rows_from_sql(2) AS emitted(n)",
        )
        .await
            == Some("(1),(2)".to_string())
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
async fn a_sql_routine_returns_an_anonymous_record_in_a_select_list() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION make_record(int) RETURNS record LANGUAGE sql \
         AS 'SELECT $1 AS a, ''value''::text AS b'",
    )
    .await;

    assert!(scalar(&mut s, "SELECT make_record(4)").await == Some("(4,value)".to_string()));
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

#[tokio::test]
async fn replacing_a_routine_reports_the_drop_hint_in_diagnostics() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION replace_me(value int) RETURNS int LANGUAGE sql AS 'SELECT value'",
    )
    .await;

    for (sql, message) in [
        (
            "CREATE OR REPLACE FUNCTION replace_me(value int) RETURNS text LANGUAGE sql AS 'SELECT value::text'",
            "cannot change return type of existing function",
        ),
        (
            "CREATE OR REPLACE FUNCTION replace_me(renamed int) RETURNS int LANGUAGE sql AS 'SELECT renamed'",
            "cannot change name of input parameter \"value\"",
        ),
        (
            "CREATE OR REPLACE PROCEDURE replace_me(value int) LANGUAGE sql AS 'SELECT value'",
            "cannot change routine kind",
        ),
    ] {
        let error = s.simple_query(sql).await.expect_err("replacement must fail");
        assert!(error.message == message, "{error:?}");
        assert!(
            error
                .diagnostics
                .as_ref()
                .and_then(|fields| fields.hint.as_deref())
                == Some("Use DROP FUNCTION replace_me(integer) first."),
            "{error:?}"
        );
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
