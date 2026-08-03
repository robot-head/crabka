//! Planner gates for operations whose `PostgreSQL` implementation needs an
//! equality, ordering, or default index operator class.

use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Engine, QueryResult, Session};

async fn run(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("query {sql:?} failed: {error:?}"))
}

async fn error(session: &mut SqlSession, sql: &str, code: &str, message: &str) {
    let error = match session.simple_query(sql).await {
        Err(error) => error,
        Ok(result) => panic!("query {sql:?} unexpectedly succeeded: {result:?}"),
    };
    assert_eq!(error.code, code, "{sql}: {error:?}");
    assert_eq!(error.message, message, "{sql}: {error:?}");
}

fn text_rows(result: &QueryResult) -> Vec<Vec<Option<String>>> {
    let QueryResult::Rows { rows, .. } = result else {
        panic!("expected rows, got {result:?}");
    };
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|cell| {
                    cell.as_ref().map(|cell| {
                        std::str::from_utf8(&cell.text)
                            .expect("UTF-8 result")
                            .to_string()
                    })
                })
                .collect()
        })
        .collect()
}

#[tokio::test]
async fn scalar_jsonpath_has_no_default_btree_or_hash_operator_class() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE paths (j jsonpath)").await;

    for (sql, method) in [
        ("CREATE INDEX paths_j_btree ON paths (j)", "btree"),
        ("CREATE INDEX paths_j_hash ON paths USING hash (j)", "hash"),
        ("CREATE INDEX paths_expr ON paths ((j))", "btree"),
        ("CREATE UNIQUE INDEX paths_unique ON paths (j)", "btree"),
        ("ALTER TABLE paths ADD UNIQUE (j)", "btree"),
    ] {
        error(
            &mut session,
            sql,
            "42704",
            &format!(
                "data type jsonpath has no default operator class for access method \"{method}\""
            ),
        )
        .await;
    }
    for sql in [
        "CREATE TABLE inline_unique (j jsonpath UNIQUE)",
        "CREATE TABLE inline_primary (j jsonpath PRIMARY KEY)",
    ] {
        error(
            &mut session,
            sql,
            "42704",
            "data type jsonpath has no default operator class for access method \"btree\"",
        )
        .await;
    }

    // PostgreSQL's generic array_ops classes are defaults for jsonpath[] even
    // though an operation that reaches a non-empty element later fails.
    run(&mut session, "CREATE TABLE path_arrays (a jsonpath[])").await;
    run(
        &mut session,
        "CREATE INDEX path_arrays_btree ON path_arrays (a)",
    )
    .await;
    run(
        &mut session,
        "CREATE INDEX path_arrays_hash ON path_arrays USING hash (a)",
    )
    .await;
}

#[tokio::test]
async fn recursive_union_requires_equality_and_uses_jsonpath_input_coercion() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();

    error(
        &mut session,
        "WITH RECURSIVE r(j) AS (SELECT '$'::jsonpath UNION \
         SELECT j FROM r WHERE false) SELECT * FROM r",
        "42883",
        "could not identify an equality operator for type jsonpath",
    )
    .await;
    error(
        &mut session,
        "WITH RECURSIVE r(a) AS (SELECT ARRAY['$'::jsonpath] UNION \
         SELECT a FROM r WHERE false) SELECT * FROM r",
        "42883",
        "could not identify an equality operator for type jsonpath[]",
    )
    .await;

    let result = run(
        &mut session,
        "WITH RECURSIVE r(j, n) AS (VALUES ('lax $.a'::jsonpath, 1) UNION ALL \
         SELECT 'lax $.b', n + 1 FROM r WHERE n < 2) SELECT j FROM r ORDER BY n",
    )
    .await;
    assert_eq!(
        text_rows(&result[0]),
        vec![vec![Some("$.\"a\"".into())], vec![Some("$.\"b\"".into())]]
    );
}

#[tokio::test]
async fn grouping_and_window_keys_validate_before_reading_rows() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE paths (j jsonpath)").await;

    for sql in [
        "SELECT count(*) FROM paths GROUP BY GROUPING SETS ((j), ())",
        "SELECT count(*) OVER (PARTITION BY j) FROM paths",
    ] {
        error(
            &mut session,
            sql,
            "42883",
            "could not identify an equality operator for type jsonpath",
        )
        .await;
    }
    error(
        &mut session,
        "SELECT count(*) OVER (ORDER BY j) FROM paths",
        "42883",
        "could not identify an ordering operator for type jsonpath",
    )
    .await;

    error(
        &mut session,
        "SELECT DISTINCT '$'::jsonpath FROM paths GROUP BY GROUPING SETS ((), ())",
        "42883",
        "could not identify an equality operator for type jsonpath",
    )
    .await;
    error(
        &mut session,
        "SELECT '$'::jsonpath FROM paths GROUP BY GROUPING SETS ((), ()) ORDER BY 1",
        "42883",
        "could not identify an ordering operator for type jsonpath",
    )
    .await;
}

#[tokio::test]
async fn row_comparisons_validate_scalar_fields_but_keep_array_ops_runtime_bound() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE paths (j jsonpath, a jsonpath[])",
    )
    .await;

    for (op, spelled) in [("=", "="), ("<", "<"), ("IS DISTINCT FROM", "=")] {
        error(
            &mut session,
            &format!("SELECT ROW(j) {op} ROW(j) FROM paths"),
            "42883",
            &format!("operator does not exist: jsonpath {spelled} jsonpath"),
        )
        .await;
    }

    let empty = run(&mut session, "SELECT ROW(a) = ROW(a) FROM paths").await;
    assert!(text_rows(&empty[0]).is_empty());
    run(
        &mut session,
        "INSERT INTO paths VALUES ('$'::jsonpath, ARRAY['$'::jsonpath])",
    )
    .await;
    let runtime = session
        .simple_query("SELECT ROW(a) = ROW(a) FROM paths")
        .await
        .expect_err("array comparison reaches the missing jsonpath element operator at runtime");
    assert_eq!(runtime.code, "42883");
}
