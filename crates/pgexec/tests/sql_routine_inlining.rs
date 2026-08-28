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

#[tokio::test]
async fn an_atomic_sql_body_ignores_redundant_semicolons() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION atomic_semicolons() RETURNS boolean LANGUAGE sql \
         BEGIN ATOMIC ;;RETURN false;; END",
    )
    .await;
    assert!(scalar(&mut s, "SELECT atomic_semicolons()").await == Some("f".to_string()));
}

#[tokio::test]
async fn an_atomic_sql_body_defaults_to_sql_and_is_visible_to_regproc_catalog_functions() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION atomic_catalog_default() RETURNS boolean BEGIN ATOMIC RETURN false; END",
    )
    .await;
    assert!(scalar(&mut s, "SELECT atomic_catalog_default()").await == Some("f".to_string()));
    assert!(
        scalar(
            &mut s,
            "SELECT pg_get_functiondef('atomic_catalog_default'::regproc)",
        )
        .await
        .is_some_and(
            |definition| definition.contains("LANGUAGE sql\nBEGIN ATOMIC\n RETURN false;\nEND")
        )
    );
}

#[tokio::test]
async fn a_sql_setof_void_function_runs_without_exposing_rows() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION void_set(int) RETURNS SETOF void LANGUAGE sql \
         AS 'SELECT generate_series(1, $1)'",
    )
    .await;
    let result = s
        .simple_query("SELECT * FROM void_set(3)")
        .await
        .expect("SETOF void runs");
    assert!(matches!(&result[0], QueryResult::Rows { rows, .. } if rows.is_empty()));
}

#[tokio::test]
async fn a_sql_table_function_supports_with_ordinality() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION ordinal_rows(int) RETURNS SETOF int LANGUAGE sql \
         AS 'SELECT generate_series(1, $1)'",
    )
    .await;
    assert!(
        scalar(
            &mut s,
            "SELECT string_agg(value::text || ':' || ordinality::text, ',' ORDER BY value) \
             FROM ordinal_rows(2) WITH ORDINALITY AS t(value, ordinality)",
        )
        .await
            == Some("1:1,2:2".to_string())
    );
    run(
        &mut s,
        "CREATE VIEW ordinal_rows_view AS \
         SELECT * FROM ordinal_rows(2) WITH ORDINALITY AS t(value, ordinality)",
    )
    .await;
    assert!(
        scalar(
            &mut s,
            "SELECT string_agg(value::text || ':' || ordinality::text, ',' ORDER BY value) \
             FROM ordinal_rows_view",
        )
        .await
            == Some("1:1,2:2".to_string())
    );
}

#[tokio::test]
async fn an_unquoted_sql_body_is_type_checked_at_definition_time() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    let error = s
        .simple_query(
            "CREATE FUNCTION invalid_date_comparison(x date) RETURNS boolean LANGUAGE sql RETURN x > 1",
        )
        .await
        .expect_err("the body has no date > integer operator");
    assert!(error.code == "42883");
    assert!(error.message == "operator does not exist: date > integer");
    run(
        &mut s,
        "CREATE FUNCTION valid_date_comparison(x date) RETURNS boolean LANGUAGE sql RETURN x > date '2000-01-01'",
    )
    .await;
    assert!(
        scalar(&mut s, "SELECT valid_date_comparison(date '2000-01-02')",).await
            == Some("t".to_string())
    );
}

#[tokio::test]
async fn an_unquoted_sql_body_with_a_scalar_subquery_is_checked_at_execution() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE counted_rows (value int)").await;
    run(&mut s, "INSERT INTO counted_rows VALUES (1), (2)").await;
    run(
        &mut s,
        "CREATE FUNCTION count_rows() RETURNS bigint RETURN (SELECT count(*) FROM counted_rows)",
    )
    .await;
    assert!(scalar(&mut s, "SELECT count_rows()").await == Some("2".to_string()));
}

#[tokio::test]
async fn quoted_sql_body_validation_respects_check_function_bodies() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    for (sql, code, message) in [
        (
            "CREATE FUNCTION bad_return_type() RETURNS int LANGUAGE sql AS 'SELECT ''text'''",
            "42P13",
            "return type mismatch in function declared to return integer",
        ),
        (
            "CREATE FUNCTION too_many_return_columns() RETURNS int LANGUAGE sql AS 'SELECT 1, 2'",
            "42P13",
            "return type mismatch in function declared to return integer",
        ),
        (
            "CREATE FUNCTION missing_body_parameter(int) RETURNS int LANGUAGE sql AS 'SELECT $2'",
            "42P02",
            "there is no parameter $2",
        ),
    ] {
        let error = s.simple_query(sql).await.expect_err("body must be checked");
        assert!(error.code == code, "{error:?}");
        assert!(error.message == message, "{error:?}");
    }
    run(
        &mut s,
        "CREATE FUNCTION valid_source_body() RETURNS int LANGUAGE sql AS 'SELECT 1'",
    )
    .await;
    assert!(scalar(&mut s, "SELECT valid_source_body()").await == Some("1".to_string()));
    run(
        &mut s,
        "CREATE FUNCTION coercible_source_body() RETURNS bigint LANGUAGE sql AS 'SELECT 1'",
    )
    .await;
    assert!(scalar(&mut s, "SELECT coercible_source_body()").await == Some("1".to_string()));
    run(&mut s, "SET check_function_bodies = off").await;
    run(
        &mut s,
        "CREATE FUNCTION deferred_bad_syntax() RETURNS int LANGUAGE sql AS 'not even SQL'",
    )
    .await;
    run(
        &mut s,
        "CREATE FUNCTION deferred_empty_body(anyelement) RETURNS anyarray LANGUAGE sql AS ''",
    )
    .await;
    let error = s
        .simple_query("SELECT deferred_empty_body(0)")
        .await
        .expect_err("an empty body must fail at execution");
    assert!(error.code == "42P13", "{error:?}");
    assert!(
        error.message == "return type mismatch in function declared to return integer[]",
        "{error:?}"
    );
    let fields = error.diagnostics.expect("error diagnostics");
    assert!(
        fields.detail.as_deref()
            == Some(
                "Function's final statement must be SELECT or INSERT/UPDATE/DELETE/MERGE RETURNING."
            ),
        "{fields:?}"
    );
    assert!(
        fields.context.as_deref() == Some("SQL function \"deferred_empty_body\" during startup"),
        "{fields:?}"
    );
}

#[tokio::test]
async fn directly_recursive_sql_functions_create_then_hit_the_stack_guard() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION infinite_recurse() RETURNS int LANGUAGE sql AS 'SELECT infinite_recurse()'",
    )
    .await;
    let error = s
        .simple_query("SELECT infinite_recurse()")
        .await
        .expect_err("recursive function must hit the stack guard");
    assert!(error.code == "54001", "{error:?}");
    assert!(error.message == "stack depth limit exceeded", "{error:?}");
}

#[tokio::test]
async fn only_superusers_can_mark_routines_leakproof() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE ROLE unpriv_leakproof").await;
    run(&mut s, "SET SESSION AUTHORIZATION unpriv_leakproof").await;
    for sql in [
        "CREATE FUNCTION unpriv_leakproof_fn(int) RETURNS boolean LANGUAGE sql LEAKPROOF RETURN $1 > 0",
        "CREATE FUNCTION unpriv_leakproof_fn(int) RETURNS boolean LANGUAGE sql AS 'SELECT $1 > 0' LEAKPROOF",
    ] {
        let error = s
            .simple_query(sql)
            .await
            .expect_err("LEAKPROOF requires superuser");
        assert!(error.code == "42501");
        assert!(error.message == "only superuser can define a leakproof function");
    }
    run(&mut s, "RESET SESSION AUTHORIZATION").await;
    run(
        &mut s,
        "CREATE FUNCTION superuser_leakproof_fn(int) RETURNS boolean LANGUAGE sql LEAKPROOF RETURN $1 > 0",
    )
    .await;
    run(&mut s, "SET SESSION AUTHORIZATION unpriv_leakproof").await;
    let error = s
        .simple_query("ALTER FUNCTION superuser_leakproof_fn(int) LEAKPROOF")
        .await
        .expect_err("LEAKPROOF requires superuser");
    assert!(error.code == "42501");
    assert!(error.message == "only superuser can define a leakproof function");
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

#[tokio::test]
async fn a_sql_routine_accepts_a_variadic_array_argument() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION array_count(VARIADIC values int[]) RETURNS int LANGUAGE sql \
         AS 'SELECT array_length(values, 1)'",
    )
    .await;

    assert!(
        scalar(&mut s, "SELECT array_count(VARIADIC ARRAY[1, 2, 3])").await
            == Some("3".to_string())
    );
    run(
        &mut s,
        "CREATE VIEW array_count_view AS \
         SELECT array_count(VARIADIC ARRAY[1, 2, 3]) AS result",
    )
    .await;
    assert!(scalar(&mut s, "SELECT result FROM array_count_view").await == Some("3".to_string()));
    assert!(
        scalar(
            &mut s,
            "SELECT pg_get_viewdef('array_count_view'::regclass)"
        )
        .await
        .expect("view definition")
        .contains("VARIADIC ARRAY[1, 2, 3]")
    );
}

#[tokio::test]
async fn a_sql_routine_accepts_name_before_parameter_mode() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION named_out(result OUT int) LANGUAGE sql AS 'SELECT 7'; \
         CREATE FUNCTION named_variadic(values VARIADIC int[]) RETURNS int LANGUAGE sql \
         AS 'SELECT array_length(values, 1) + values[1]'; \
         CREATE FUNCTION positional_default(value int, increment int DEFAULT 5) RETURNS int \
         LANGUAGE sql AS 'SELECT value + increment'",
    )
    .await;

    assert!(scalar(&mut s, "SELECT named_out()").await == Some("7".to_string()));
    assert!(scalar(&mut s, "SELECT named_variadic(1, 2, 3)").await == Some("4".to_string()));
    assert!(scalar(&mut s, "SELECT named_variadic('1', '2', '3')").await == Some("4".to_string()));
    assert!(scalar(&mut s, "SELECT positional_default(3)").await == Some("8".to_string()));
}

#[tokio::test]
async fn from_functions_bind_named_default_and_variadic_arguments() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION from_named(a int, b int DEFAULT 5) RETURNS TABLE(total int) \
         LANGUAGE sql AS 'SELECT a + b'",
    )
    .await;
    run(
        &mut s,
        "CREATE FUNCTION from_variadic(VARIADIC values int[]) RETURNS TABLE(total int) \
         LANGUAGE sql AS 'SELECT array_length(values, 1)'",
    )
    .await;

    assert!(
        scalar(&mut s, "SELECT total FROM from_named(b => 4, a := 3)").await
            == Some("7".to_string())
    );
    assert!(scalar(&mut s, "SELECT total FROM from_named(a => 3)").await == Some("8".to_string()));
    assert!(
        scalar(
            &mut s,
            "SELECT total FROM (VALUES (3)) AS input(a), from_named(a => input.a)",
        )
        .await
            == Some("8".to_string())
    );
    assert!(
        scalar(
            &mut s,
            "SELECT total FROM from_variadic(VARIADIC ARRAY[1, 2, 3])",
        )
        .await
            == Some("3".to_string())
    );
    run(
        &mut s,
        "CREATE VIEW named_from_view AS \
         SELECT total FROM from_named(b => 4, a => 3)",
    )
    .await;
    assert!(scalar(&mut s, "SELECT total FROM named_from_view").await == Some("7".to_string()));
    assert!(
        scalar(&mut s, "SELECT pg_get_viewdef('named_from_view'::regclass)")
            .await
            .expect("view definition")
            .contains("from_named(b => 4, a => 3)")
    );
}

#[tokio::test]
async fn a_sql_table_result_does_not_shadow_its_body_column() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION table_range(upper_bound int) RETURNS TABLE(a int) LANGUAGE sql \
         AS 'SELECT a FROM generate_series(1, upper_bound) AS a(a)'",
    )
    .await;

    assert!(
        scalar(
            &mut s,
            "SELECT string_agg(a::text, ',' ORDER BY a) FROM table_range(3)",
        )
        .await
            == Some("1,2,3".to_string())
    );
}

/// PostgreSQL installs these labels/defaults during initdb, so they must come
/// from the initialized pg_proc fixture rather than a hand-maintained table.
#[tokio::test]
async fn a_builtin_accepts_catalog_named_default_arguments() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    assert!(
        scalar(
            &mut s,
            "SELECT jsonb_path_exists(target => '{\"a\": 1}'::jsonb, path => '$.a'::jsonpath)",
        )
        .await
            == Some("t".to_string())
    );
}

/// A scalar built-in in FROM is a one-row `FunctionScan`, not an undefined SRF.
#[tokio::test]
async fn a_scalar_builtin_scans_as_one_row_from_item() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE TABLE tid_source (value int); INSERT INTO tid_source VALUES (1)",
    )
    .await;
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
            "integer",
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

    assert!(
        scalar(&mut s, "SELECT sum(lookup_value(k)) FROM input_rows").await
            == Some("30".to_string())
    );
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
async fn a_sql_scalar_routine_returns_its_first_final_row() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION too_many() RETURNS int LANGUAGE sql \
         AS 'SELECT 1 UNION ALL SELECT 2'",
    )
    .await;

    assert!(scalar(&mut s, "SELECT too_many()").await == Some("1".to_string()));
    run(&mut s, "CREATE TABLE audit (v int)").await;
    run(
        &mut s,
        "CREATE FUNCTION insert_many() RETURNS int LANGUAGE sql \
         AS 'INSERT INTO audit VALUES (1), (2) RETURNING v'",
    )
    .await;
    assert!(scalar(&mut s, "SELECT insert_many()").await == Some("1".to_string()));
    assert!(scalar(&mut s, "SELECT count(*) FROM audit").await == Some("2".to_string()));
}

#[tokio::test]
async fn a_sql_setof_record_in_a_select_list_returns_composite_rows() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION set_record() RETURNS SETOF record LANGUAGE sql \
         AS 'SELECT 1 AS a, ''x'' AS b UNION ALL SELECT 2, ''y'''",
    )
    .await;
    let results = s.simple_query("SELECT set_record()").await.expect("call");
    let QueryResult::Rows { rows, .. } = &results[0] else {
        panic!("expected rows");
    };
    let values = rows
        .iter()
        .map(|row| String::from_utf8(row[0].as_ref().expect("value").text.to_vec()).expect("utf8"))
        .collect::<Vec<_>>();
    assert!(values == ["(1,x)", "(2,y)"]);
}

#[tokio::test]
async fn a_sql_record_function_in_from_requires_a_column_definition_list() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION bare_record() RETURNS record LANGUAGE sql AS 'SELECT 1, ''x'''",
    )
    .await;
    let error = s
        .simple_query("SELECT * FROM bare_record()")
        .await
        .expect_err("record function needs a definition list");
    assert!(error.code == "42601");
    assert!(
        error.message == "a column definition list is required for functions returning \"record\""
    );
}

#[tokio::test]
async fn a_sql_named_composite_function_rejects_a_column_definition_list() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TYPE pair AS (a int, b text)").await;
    run(
        &mut s,
        "CREATE FUNCTION named_pair() RETURNS pair LANGUAGE sql AS 'SELECT 1, ''x'''",
    )
    .await;
    let error = s
        .simple_query("SELECT * FROM named_pair() AS t(a int, b text)")
        .await
        .expect_err("named composite has its own row definition");
    assert!(error.code == "42601");
    assert!(
        error.message
            == "a column definition list is redundant for a function returning a named composite type"
    );
}

#[tokio::test]
async fn sql_function_column_definition_lists_only_describe_bare_records() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION bare_record_with_definition() RETURNS record LANGUAGE sql \
         AS 'SELECT 1, ''x'''; \
         CREATE FUNCTION scalar_result() RETURNS int LANGUAGE sql AS 'SELECT 1'; \
         CREATE FUNCTION out_record(OUT a int, OUT b text) RETURNS record LANGUAGE sql \
         AS 'SELECT 1, ''x'''",
    )
    .await;
    run(
        &mut s,
        "SELECT * FROM bare_record_with_definition() AS t(a int, b text)",
    )
    .await;

    let scalar = s
        .simple_query("SELECT * FROM scalar_result() AS t(value int)")
        .await
        .expect_err("scalar functions cannot use a column definition list");
    assert!(scalar.code == "42601");
    assert!(
        scalar.message
            == "a column definition list is only allowed for functions returning \"record\""
    );

    let out = s
        .simple_query("SELECT * FROM out_record() AS t(a int, b text)")
        .await
        .expect_err("OUT parameters already provide a row definition");
    assert!(out.code == "42601");
    assert!(
        out.message == "a column definition list is redundant for a function with OUT parameters"
    );
}

#[tokio::test]
async fn a_sql_record_function_uses_column_definition_types_in_from() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION record_number() RETURNS record LANGUAGE sql AS 'SELECT 1'",
    )
    .await;

    assert!(
        scalar(
            &mut s,
            "SELECT value FROM record_number() AS t(value numeric(4,2))",
        )
        .await
            == Some("1.00".to_string())
    );
}

#[tokio::test]
async fn sql_record_functions_in_from_apply_relation_body_cardinality() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE TABLE record_source (value int); INSERT INTO record_source VALUES (1), (2)",
    )
    .await;
    run(
        &mut s,
        "CREATE FUNCTION record_row() RETURNS record LANGUAGE sql \
         AS 'SELECT value FROM record_source ORDER BY value'; \
         CREATE FUNCTION record_rows() RETURNS SETOF record LANGUAGE sql \
         AS 'SELECT value FROM record_source ORDER BY value'",
    )
    .await;

    assert!(
        scalar(&mut s, "SELECT count(*) FROM record_row() AS t(value int)",).await
            == Some("1".to_string())
    );
    assert!(
        scalar(&mut s, "SELECT count(*) FROM record_rows() AS t(value int)",).await
            == Some("2".to_string())
    );
}

#[tokio::test]
async fn a_non_strict_polymorphic_record_function_inlines_a_literal_array() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION inline_array_to_set(anyarray) RETURNS SETOF record LANGUAGE sql \
         AS 'SELECT i, $1[i] FROM generate_subscripts($1, 1) AS i'",
    )
    .await;

    let error = s
        .simple_query(
            "SELECT * FROM inline_array_to_set(array['one', 'two']) AS t(value point, label text)",
        )
        .await
        .expect_err("the literal array body is inlined");
    let diagnostics = error.diagnostics.expect("return mismatch diagnostics");
    assert!(
        diagnostics.context == Some("SQL function \"inline_array_to_set\" during inlining".into())
    );
}

#[tokio::test]
async fn a_non_strict_polymorphic_record_function_evaluates_a_volatile_array_once() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE SEQUENCE record_array_sequence").await;
    run(
        &mut s,
        "CREATE FUNCTION echo_record_array(anyarray) RETURNS record LANGUAGE sql \
         AS 'SELECT $1[1], $1[1]'",
    )
    .await;

    assert!(
        scalar(
            &mut s,
            "SELECT a FROM echo_record_array(ARRAY[nextval('record_array_sequence')::int]) \
             AS t(a int, b int)",
        )
        .await
            == Some("1".to_string())
    );
    assert!(
        scalar(&mut s, "SELECT nextval('record_array_sequence')::int",).await
            == Some("2".to_string())
    );
}

#[tokio::test]
async fn a_sql_record_function_rejects_an_incompatible_column_definition() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION record_number() RETURNS record LANGUAGE sql AS 'SELECT 1'",
    )
    .await;

    let error = s
        .simple_query("SELECT * FROM record_number() AS t(value point)")
        .await
        .expect_err("record descriptor must match the final SQL result");
    assert!(error.code == "42P13");
    assert!(error.message == "return type mismatch in function declared to return record");
    let diagnostics = error.diagnostics.expect("return mismatch diagnostics");
    assert!(
        diagnostics.detail
            == Some("Final statement returns integer instead of point at column 1.".into())
    );
    assert!(diagnostics.context == Some("SQL function \"record_number\" during inlining".into()));
}

#[tokio::test]
async fn a_strict_sql_record_function_uses_the_executor_not_the_inliner() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION strict_record_number() RETURNS record STRICT LANGUAGE sql AS 'SELECT 1'",
    )
    .await;

    let error = s
        .simple_query("SELECT * FROM strict_record_number() AS t(value point)")
        .await
        .expect_err("strict record function must not inline");
    let diagnostics = error.diagnostics.expect("return mismatch diagnostics");
    assert!(
        diagnostics.context == Some("SQL function \"strict_record_number\" statement 1".into())
    );
}

#[tokio::test]
async fn set_returning_routines_in_a_select_list_preserve_result_shapes() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION strict_set(value int) RETURNS SETOF int STRICT LANGUAGE sql \
         AS 'SELECT $1'",
    )
    .await;
    run(
        &mut s,
        "CREATE FUNCTION plpgsql_set(value int) RETURNS SETOF int LANGUAGE plpgsql \
         AS $$ BEGIN RETURN NEXT value; END $$",
    )
    .await;
    run(
        &mut s,
        "CREATE FUNCTION table_pair() RETURNS TABLE(a int, b text) LANGUAGE sql \
         AS 'SELECT 1, ''x'''",
    )
    .await;
    run(
        &mut s,
        "CREATE FUNCTION table_single() RETURNS TABLE(value int) LANGUAGE sql \
         AS 'SELECT 2'",
    )
    .await;
    run(
        &mut s,
        "CREATE FUNCTION out_pair(IN value int, OUT a int, OUT b text) RETURNS SETOF record \
         LANGUAGE sql AS 'SELECT $1, $1::text'",
    )
    .await;
    run(
        &mut s,
        "CREATE FUNCTION text_from_varchar() RETURNS SETOF text LANGUAGE sql \
         AS 'SELECT ''foo''::varchar UNION ALL SELECT ''bar''::varchar'",
    )
    .await;

    assert!(matches!(
        &s.simple_query("SELECT strict_set(NULL::int)")
            .await
            .expect("strict call")[0],
        QueryResult::Rows { rows, .. } if rows.is_empty()
    ));
    assert!(scalar(&mut s, "SELECT plpgsql_set(7)").await == Some("7".into()));
    assert!(scalar(&mut s, "SELECT table_pair()").await == Some("(1,x)".into()));
    assert!(scalar(&mut s, "SELECT table_single()").await == Some("2".into()));
    assert!(scalar(&mut s, "SELECT out_pair(4)").await == Some("(4,4)".into()));
    assert!(scalar(&mut s, "SELECT text_from_varchar()").await == Some("foo".into()));
}

#[tokio::test]
async fn a_sql_setof_named_composite_in_a_select_list_packs_body_columns() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE TYPE named_pair_for_srf AS (left_value int, right_value text)",
    )
    .await;
    run(
        &mut s,
        "CREATE FUNCTION named_pairs() RETURNS SETOF named_pair_for_srf LANGUAGE sql \
         AS 'SELECT 1, ''x'' UNION ALL SELECT 2, ''y'''",
    )
    .await;

    let QueryResult::Rows { rows, .. } = s
        .simple_query("SELECT named_pairs()")
        .await
        .expect("named composite call")
        .remove(0)
    else {
        panic!("expected rows");
    };
    let values = rows
        .iter()
        .map(|row| String::from_utf8(row[0].as_ref().expect("value").text.to_vec()).expect("utf8"))
        .collect::<Vec<_>>();
    assert!(values == ["(1,x)", "(2,y)"]);
}

#[tokio::test]
async fn a_sql_multi_out_function_unpacks_an_anonymous_record() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION packed_out(OUT value int, OUT fraction numeric) LANGUAGE sql \
         AS 'SELECT (1, 2.1)'",
    )
    .await;

    let QueryResult::Rows { rows, .. } = s
        .simple_query("SELECT * FROM packed_out()")
        .await
        .expect("packed OUT call")
        .remove(0)
    else {
        panic!("expected rows");
    };
    assert!(rows.len() == 1);
    assert!(
        rows[0][0]
            .as_ref()
            .is_some_and(|cell| cell.text.as_ref() == b"1")
    );
    assert!(
        rows[0][1]
            .as_ref()
            .is_some_and(|cell| cell.text.as_ref() == b"2.1")
    );

    run(
        &mut s,
        "CREATE OR REPLACE FUNCTION packed_out(OUT value int, OUT fraction numeric) LANGUAGE sql \
         AS 'SELECT (1, 2)'",
    )
    .await;
    let error = s
        .simple_query("SELECT * FROM packed_out()")
        .await
        .expect_err("mismatched OUT field type");
    assert!(error.code == "42P13");
    assert!(
        error
            .diagnostics
            .as_ref()
            .and_then(|fields| fields.detail.as_deref())
            == Some("Returned type integer at ordinal position 2, but query expects numeric.")
    );

    run(
        &mut s,
        "CREATE OR REPLACE FUNCTION packed_out(OUT value int, OUT fraction numeric) LANGUAGE sql \
         AS 'SELECT (1, 2.1, 3)'",
    )
    .await;
    let error = s
        .simple_query("SELECT * FROM packed_out()")
        .await
        .expect_err("wrong OUT field count");
    assert!(error.code == "42P13");
    assert!(
        error
            .diagnostics
            .as_ref()
            .and_then(|fields| fields.detail.as_deref())
            == Some("Returned row contains 3 attributes, but query expects 2.")
    );
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
        rows[0][0]
            .as_ref()
            .is_some_and(|cell| cell.text.as_ref() == b"1"),
        "{rows:?}"
    );
    assert!(
        rows[2][0]
            .as_ref()
            .is_some_and(|cell| cell.text.as_ref() == b"3"),
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
        "CREATE FUNCTION scalar_from_plpgsql(value int) RETURNS int LANGUAGE plpgsql \
         AS $$ BEGIN RETURN value; END $$",
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
    run(
        &mut s,
        "CREATE VIEW rows_from_sql_view AS \
         SELECT n FROM rows_from_sql(2) AS emitted(n)",
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
            "SELECT string_agg(coalesce(a::text, 'null') || ':' || coalesce(b::text, 'null'), ',' ORDER BY a NULLS LAST) \
             FROM ROWS FROM (rows_from_sql(2), rows_from_sql(3)) AS t(a, b)",
        )
        .await
            == Some("1:1,2:2,null:3".to_string())
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
            "SELECT value::text || ':' || ordinality::text \
             FROM scalar_from_plpgsql(9) WITH ORDINALITY AS t(value, ordinality)",
        )
        .await
            == Some("9:1".to_string())
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
            "SELECT string_agg(n::text, ',' ORDER BY n) FROM rows_from_sql_view",
        )
        .await
            == Some("1,2".to_string())
    );
    assert!(
        scalar(
            &mut s,
            "SELECT string_agg(value::text, ',' ORDER BY value) \
             FROM (VALUES (1), (2)) AS input(value) \
             WHERE value IN (SELECT n FROM rows_from_sql(value) AS emitted(n))",
        )
        .await
            == Some("1,2".to_string())
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
async fn rows_from_caches_only_its_uncorrelated_function_calls() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE SEQUENCE rows_from_sequence").await;
    run(
        &mut s,
        "CREATE FUNCTION rows_from_once() RETURNS SETOF record LANGUAGE sql \
         AS 'SELECT nextval(''rows_from_sequence'')'",
    )
    .await;
    run(
        &mut s,
        "SELECT * FROM (VALUES (1), (2), (3)) AS input(value), \
         ROWS FROM (rows_from_once() AS (sequence_value bigint), generate_series(value, value))",
    )
    .await;

    assert!(
        scalar(&mut s, "SELECT nextval('rows_from_sequence')::int").await == Some("2".to_string())
    );
}

#[tokio::test]
async fn a_scalar_sql_table_function_emits_one_row() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION scalar_rows() RETURNS int LANGUAGE sql \
         AS 'SELECT value FROM (VALUES (1), (2)) AS rows(value)'",
    )
    .await;
    assert!(
        scalar(
            &mut s,
            "SELECT string_agg(value::text, ',' ORDER BY value) \
             FROM scalar_rows() AS t(value)",
        )
        .await
            == Some("1".to_string())
    );
    assert!(
        scalar(
            &mut s,
            "SELECT string_agg(coalesce(value::text, 'null') || ':' || series::text, ',' ORDER BY series) \
             FROM ROWS FROM (scalar_rows(), generate_series(10, 11)) AS t(value, series)",
        )
        .await
            == Some("1:10,null:11".to_string())
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
    run(
        &mut s,
        "INSERT INTO pair VALUES (4, 'value'); \
         CREATE FUNCTION packed_pair(int) RETURNS pair LANGUAGE sql \
         AS 'SELECT p FROM pair AS p WHERE p.a = $1'",
    )
    .await;

    assert!(scalar(&mut s, "SELECT (make_pair(4)).a").await == Some("4".to_string()));
    assert!(scalar(&mut s, "SELECT (make_pair(4)).b").await == Some("value".to_string()));
    assert!(
        scalar(&mut s, "SELECT a::text || ':' || b FROM packed_pair(4)",).await
            == Some("4:value".to_string())
    );
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
async fn a_sql_routine_with_multiple_out_parameters_returns_a_scalar_record() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION out_record(int, OUT a int, OUT b text) LANGUAGE sql \
         AS 'SELECT $1, ''value'''; \
         CREATE FUNCTION out_scalar(int, OUT a int) LANGUAGE sql AS 'SELECT $1'",
    )
    .await;

    assert!(scalar(&mut s, "SELECT out_record(4)").await == Some("(4,value)".to_string()));
    assert!(scalar(&mut s, "SELECT out_scalar(5)").await == Some("5".to_string()));
}

#[tokio::test]
async fn a_polymorphic_sql_routine_resolves_out_columns_in_from() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE FUNCTION out_poly(anyelement, OUT value anyelement, OUT values anyarray) \
         LANGUAGE sql AS 'SELECT $1, ARRAY[$1, $1]'",
    )
    .await;

    assert!(
        scalar(
            &mut s,
            "SELECT value::text || ':' || values::text FROM out_poly('x'::text)",
        )
        .await
            == Some("x:{x,x}".to_string())
    );
    let error = s
        .simple_query("SELECT out_poly('x')")
        .await
        .expect_err("unknown polymorphic input must fail");
    assert!(error.code == "42804", "{error:?}");
    assert!(
        error.message == "could not determine polymorphic type because input has type unknown",
        "{error:?}"
    );
}

#[tokio::test]
async fn a_plpgsql_rowtype_variable_returns_a_relation_row() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE TABLE rowtype_source (id int, label text); \
         INSERT INTO rowtype_source VALUES (7, 'seven'); \
         CREATE FUNCTION rowtype_value(int) RETURNS rowtype_source LANGUAGE plpgsql AS $$ \
         DECLARE output_value rowtype_source%ROWTYPE; \
         BEGIN \
           SELECT * INTO output_value FROM rowtype_source WHERE id = $1; \
           RETURN output_value; \
         END $$",
    )
    .await;
    assert!(
        scalar(
            &mut s,
            "SELECT id::text || ':' || label FROM rowtype_value(7)",
        )
        .await
            == Some("7:seven".to_string())
    );
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

    for (sql, message, detail, hint) in [
        (
            "CREATE OR REPLACE FUNCTION replace_me(value int) RETURNS text LANGUAGE sql AS 'SELECT value::text'",
            "cannot change return type of existing function",
            None,
            Some("Use DROP FUNCTION replace_me(integer) first."),
        ),
        (
            "CREATE OR REPLACE FUNCTION replace_me(renamed int) RETURNS int LANGUAGE sql AS 'SELECT renamed'",
            "cannot change name of input parameter \"value\"",
            None,
            Some("Use DROP FUNCTION replace_me(integer) first."),
        ),
        (
            "CREATE OR REPLACE FUNCTION replace_me(value int) RETURNS int LANGUAGE sql WINDOW AS 'SELECT value'",
            "cannot change routine kind",
            Some("\"replace_me\" is a function."),
            None,
        ),
        (
            "CREATE OR REPLACE PROCEDURE replace_me(value int) LANGUAGE sql AS 'SELECT value'",
            "cannot change routine kind",
            Some("\"replace_me\" is a function."),
            None,
        ),
    ] {
        let error = s
            .simple_query(sql)
            .await
            .expect_err("replacement must fail");
        assert!(error.message == message, "{error:?}");
        assert!(
            error
                .diagnostics
                .as_ref()
                .and_then(|fields| fields.detail.as_deref())
                == detail,
            "{error:?}"
        );
        assert!(
            error
                .diagnostics
                .as_ref()
                .and_then(|fields| fields.hint.as_deref())
                == hint,
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

#[tokio::test]
async fn scalar_record_function_scans_validate_column_definition_lists() {
    use assert2::assert;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    let prefix = "WITH a(b) AS (VALUES (ROW(1, 2, 3))) ";

    assert!(
        scalar(
            &mut s,
            &(prefix.to_string()
                + "SELECT d::text || ',' || e::text || ',' || f::text \
                   FROM a, coalesce(b) AS c(d int, e int, f int)"),
        )
        .await
            == Some("1,2,3".to_string())
    );
    for (defs, detail) in [
        (
            "d int, e int",
            "Returned row contains 3 attributes, but query expects 2.",
        ),
        (
            "d int, e int, f float",
            "Returned type integer at ordinal position 3, but query expects double precision.",
        ),
    ] {
        let error = s
            .simple_query(&format!(
                "{prefix}SELECT * FROM a, coalesce(b) AS c({defs})"
            ))
            .await
            .expect_err("record descriptor mismatch must fail");
        assert!(error.code == "42804", "{error:?}");
        assert!(error.message == "function return row and query-specified return row do not match");
        assert!(
            error
                .diagnostics
                .as_ref()
                .and_then(|fields| fields.detail.as_deref())
                == Some(detail),
            "{error:?}"
        );
    }
}
