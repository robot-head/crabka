//! Streaming local aggregates: plain COUNT/SUM/MIN/MAX/AVG over one local base
//! table fold per cursor page, so they succeed over tables larger than the
//! blocking-query memory budget while unsupported shapes (whole-row reads,
//! DISTINCT, unbounded group keys) still fail closed on that budget.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Engine, QueryResult, Session};

async fn exec(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("query {sql:?} failed: {error:?}"))
}

/// Result rows as text cells (`None` for SQL NULL).
async fn query_rows(session: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    match exec(session, sql).await.remove(0) {
        QueryResult::Rows { rows, .. } => rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|cell| {
                        cell.map(|cell| String::from_utf8(cell.text.to_vec()).expect("utf8 cell"))
                    })
                    .collect()
            })
            .collect(),
        other => panic!("expected rows for {sql:?}, got {other:?}"),
    }
}

/// The single cell of a one-row one-column result.
async fn one_cell(session: &mut SqlSession, sql: &str) -> Option<String> {
    let mut rows = query_rows(session, sql).await;
    assert!(rows.len() == 1, "expected one row for {sql:?}");
    let mut row = rows.remove(0);
    assert!(row.len() == 1, "expected one column for {sql:?}");
    row.remove(0)
}

/// SQLSTATE of a query expected to fail.
async fn query_error_code(session: &mut SqlSession, sql: &str) -> String {
    match session.simple_query(sql).await {
        Ok(result) => panic!("query {sql:?} unexpectedly succeeded: {result:?}"),
        Err(error) => error.code,
    }
}

const BIG_ROWS: i64 = 2500;
const PAYLOAD_CHARS: usize = 8192;

/// Build a table whose visible rows exceed the 16 MiB blocking-query budget:
/// 2250 non-null 8 KiB payloads, each unique via its id prefix (v is NULL when
/// id is a multiple of ten).
async fn seed_big_table(session: &mut SqlSession) {
    exec(session, "CREATE TABLE big (id BIGINT, grp BIGINT, v TEXT)").await;
    let padding = "x".repeat(PAYLOAD_CHARS - 8);
    for batch in 0..(BIG_ROWS / 125) {
        let values = (batch * 125 + 1..=batch * 125 + 125)
            .map(|id| {
                if id % 10 == 0 {
                    format!("({id}, {}, NULL)", id % 7)
                } else {
                    format!("({id}, {}, '{id:08}{padding}')", id % 7)
                }
            })
            .collect::<Vec<_>>()
            .join(",");
        exec(session, &format!("INSERT INTO big VALUES {values}")).await;
    }
}

#[tokio::test]
async fn scalar_aggregates_stream_over_a_table_larger_than_the_memory_budget() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    seed_big_table(&mut session).await;

    // The guard first: whole-row materialization over the same table must still
    // exceed the budget, proving the aggregates below cannot be materializing.
    let code = query_error_code(&mut session, "SELECT * FROM big").await;
    assert!(code == "53200");

    let count = one_cell(&mut session, "SELECT count(*) FROM big").await;
    assert!(count.as_deref() == Some("2500"));
    let sum = one_cell(&mut session, "SELECT sum(id) FROM big").await;
    assert!(sum.as_deref() == Some("3126250"));
    let bounds = query_rows(&mut session, "SELECT min(id), max(id) FROM big").await;
    assert!(bounds == vec![vec![Some("1".to_string()), Some("2500".to_string())]]);
    let avg = one_cell(&mut session, "SELECT avg(id) FROM big").await;
    let avg = avg
        .expect("avg over non-empty table is not null")
        .parse::<f64>()
        .expect("avg is numeric text");
    assert!((avg - 1250.5).abs() < 1e-9);
}

#[tokio::test]
async fn filtered_aggregates_stream_with_a_pushdown_where() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    seed_big_table(&mut session).await;

    let count = one_cell(&mut session, "SELECT count(*) FROM big WHERE id <= 100").await;
    assert!(count.as_deref() == Some("100"));
    let sum = one_cell(
        &mut session,
        "SELECT sum(id) FROM big WHERE id > 2400 AND id <= 2500",
    )
    .await;
    // 2401 + … + 2500
    assert!(sum.as_deref() == Some("245050"));
    // A filtered grouped aggregate: ids 1..=700 hold exactly 100 of each residue.
    let grouped = query_rows(
        &mut session,
        "SELECT grp, count(*) FROM big WHERE id <= 700 GROUP BY grp ORDER BY grp",
    )
    .await;
    let expected = (0..7)
        .map(|grp| vec![Some(grp.to_string()), Some("100".to_string())])
        .collect::<Vec<_>>();
    assert!(grouped == expected);
}

#[tokio::test]
async fn count_of_a_nullable_column_skips_nulls_while_streaming() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    seed_big_table(&mut session).await;

    // v is NULL for every tenth id: count(v) < count(*).
    let count_star = one_cell(&mut session, "SELECT count(*) FROM big").await;
    let count_v = one_cell(&mut session, "SELECT count(v) FROM big").await;
    assert!(count_star.as_deref() == Some("2500"));
    assert!(count_v.as_deref() == Some("2250"));
}

#[tokio::test]
async fn unsupported_shapes_keep_the_memory_budget_guard() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    seed_big_table(&mut session).await;

    // Whole-row materialization.
    assert!(query_error_code(&mut session, "SELECT * FROM big").await == "53200");
    // DISTINCT aggregates are outside the partial-aggregate pushdown model.
    assert!(query_error_code(&mut session, "SELECT count(DISTINCT v) FROM big").await == "53200");
    // Grouping by a high-cardinality wide key: the accumulated group state
    // itself exceeds the budget even though the fold streams pages.
    assert!(
        query_error_code(&mut session, "SELECT v, count(*) FROM big GROUP BY v").await == "53200"
    );
}

#[tokio::test]
async fn streaming_aggregates_keep_small_table_and_empty_table_semantics() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    exec(&mut session, "CREATE TABLE t (id BIGINT, v TEXT)").await;

    // Empty table: count is zero, the others are NULL.
    assert!(
        one_cell(&mut session, "SELECT count(*) FROM t")
            .await
            .as_deref()
            == Some("0")
    );
    assert!(
        one_cell(&mut session, "SELECT sum(id) FROM t")
            .await
            .is_none()
    );
    assert!(
        one_cell(&mut session, "SELECT min(v) FROM t")
            .await
            .is_none()
    );
    assert!(
        one_cell(&mut session, "SELECT avg(id) FROM t")
            .await
            .is_none()
    );

    exec(
        &mut session,
        "INSERT INTO t VALUES (1, 'a'), (2, NULL), (3, 'c')",
    )
    .await;
    assert!(
        one_cell(&mut session, "SELECT count(*) FROM t")
            .await
            .as_deref()
            == Some("3")
    );
    assert!(
        one_cell(&mut session, "SELECT count(v) FROM t")
            .await
            .as_deref()
            == Some("2")
    );
    assert!(
        one_cell(&mut session, "SELECT sum(id) FROM t")
            .await
            .as_deref()
            == Some("6")
    );
    assert!(
        one_cell(&mut session, "SELECT min(v) FROM t")
            .await
            .as_deref()
            == Some("a")
    );
    assert!(
        one_cell(&mut session, "SELECT max(v) FROM t")
            .await
            .as_deref()
            == Some("c")
    );
}

#[tokio::test]
async fn streaming_aggregates_respect_mvcc_snapshots() {
    let engine = SqlEngine::new();
    let mut writer = engine.connect();
    let mut reader = engine.connect();
    exec(&mut writer, "CREATE TABLE t (id BIGINT)").await;
    exec(&mut writer, "INSERT INTO t VALUES (1), (2)").await;

    exec(&mut writer, "BEGIN").await;
    exec(&mut writer, "INSERT INTO t VALUES (3)").await;
    // The writer counts its own uncommitted insert; the reader does not.
    assert!(
        one_cell(&mut writer, "SELECT count(*) FROM t")
            .await
            .as_deref()
            == Some("3")
    );
    assert!(
        one_cell(&mut reader, "SELECT count(*) FROM t")
            .await
            .as_deref()
            == Some("2")
    );
    exec(&mut writer, "COMMIT").await;
    assert!(
        one_cell(&mut reader, "SELECT count(*) FROM t")
            .await
            .as_deref()
            == Some("3")
    );

    exec(&mut writer, "BEGIN").await;
    exec(&mut writer, "INSERT INTO t VALUES (4)").await;
    exec(&mut writer, "ROLLBACK").await;
    assert!(
        one_cell(&mut reader, "SELECT count(*) FROM t")
            .await
            .as_deref()
            == Some("3")
    );
}

#[tokio::test]
async fn grouped_streaming_aggregate_orders_groups_with_nulls_last() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    exec(&mut session, "CREATE TABLE t (g TEXT, id BIGINT)").await;
    exec(
        &mut session,
        "INSERT INTO t VALUES ('b', 1), ('a', 2), (NULL, 3), ('b', 4)",
    )
    .await;

    let rows = query_rows(
        &mut session,
        "SELECT g, count(*) FROM t GROUP BY g ORDER BY g",
    )
    .await;

    assert!(
        rows == vec![
            vec![Some("a".to_string()), Some("1".to_string())],
            vec![Some("b".to_string()), Some("2".to_string())],
            vec![None, Some("1".to_string())],
        ]
    );
}
