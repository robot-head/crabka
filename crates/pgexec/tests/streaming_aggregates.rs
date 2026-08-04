//! Streaming local aggregates: COUNT/SUM/MIN/MAX/AVG over one local base
//! table — bare or wrapped in scalar expressions (`CAST(count(*) AS BIGINT)`,
//! `COALESCE(sum(x), 0)`, `sum(a) / count(*)`) — fold per cursor page, so they
//! succeed over tables larger than the blocking-query memory budget while
//! unsupported shapes (whole-row reads, DISTINCT, non-column aggregate
//! arguments, unbounded group keys) still fail closed on that budget.

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

/// Field `(name, type oid)` pairs of a query's `RowDescription`.
async fn query_fields(session: &mut SqlSession, sql: &str) -> Vec<(String, u32)> {
    match exec(session, sql).await.remove(0) {
        QueryResult::Rows { fields, .. } => fields
            .into_iter()
            .map(|field| (field.name, field.type_oid))
            .collect(),
        other => panic!("expected rows for {sql:?}, got {other:?}"),
    }
}

/// Assert each query yields exactly the expected single row of text cells.
async fn assert_single_rows(session: &mut SqlSession, cases: &[(&str, &[Option<&str>])]) {
    for (sql, expected) in cases {
        let rows = query_rows(session, sql).await;
        let expected: Vec<Vec<Option<String>>> = vec![
            expected
                .iter()
                .map(|cell| cell.map(str::to_string))
                .collect(),
        ];
        assert!(rows == expected, "wrong rows for {sql:?}");
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
async fn aggregates_wrapped_in_scalar_expressions_stream_over_a_big_table() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    seed_big_table(&mut session).await;

    // The guard first: whole-row materialization over the same table must still
    // exceed the budget, proving the wrapped forms below cannot be materializing.
    assert!(query_error_code(&mut session, "SELECT * FROM big").await == "53200");

    assert_single_rows(
        &mut session,
        &[
            ("SELECT CAST(count(*) AS BIGINT) FROM big", &[Some("2500")]),
            ("SELECT COALESCE(sum(id), 0) FROM big", &[Some("3126250")]),
            ("SELECT count(*) + 1 FROM big", &[Some("2501")]),
            // Two aggregates inside one expression: 3126250 / 2500.
            ("SELECT sum(id) / count(*) FROM big", &[Some("1250")]),
            // Mixed projection: a wrapped aggregate beside a bare one.
            (
                "SELECT COALESCE(sum(id), 0), count(*) FROM big",
                &[Some("3126250"), Some("2500")],
            ),
            // A constant item beside an aggregate item.
            ("SELECT 1, count(*) FROM big", &[Some("1"), Some("2500")]),
            // The same aggregate bare and wrapped streams one shared spec.
            (
                "SELECT sum(id), COALESCE(sum(id), 0) FROM big",
                &[Some("3126250"), Some("3126250")],
            ),
            // Wrapped forms keep the pushdown WHERE: 1 + … + 100.
            (
                "SELECT COALESCE(sum(id), 0) FROM big WHERE id <= 100",
                &[Some("5050")],
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn wrapped_aggregates_match_materializing_semantics_on_small_and_empty_tables() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    exec(&mut session, "CREATE TABLE s (a BIGINT, x BIGINT)").await;
    exec(
        &mut session,
        "INSERT INTO s VALUES (1, 100), (2, NULL), (3, 300)",
    )
    .await;
    exec(&mut session, "CREATE TABLE e (a BIGINT, x BIGINT)").await;

    // Expectations recorded from the materializing path before this streamed.
    assert_single_rows(
        &mut session,
        &[
            ("SELECT CAST(count(*) AS BIGINT) FROM s", &[Some("3")]),
            ("SELECT COALESCE(sum(x), 0) FROM s", &[Some("400")]),
            ("SELECT count(*) + 1 FROM s", &[Some("4")]),
            ("SELECT sum(a) / count(*) FROM s", &[Some("2")]),
            ("SELECT CAST(avg(a) AS BIGINT) FROM s", &[Some("2")]),
            ("SELECT abs(sum(a)) FROM s", &[Some("6")]),
            (
                "SELECT COALESCE(sum(a), 0), count(*) FROM s",
                &[Some("6"), Some("3")],
            ),
            ("SELECT 1, count(*) FROM s", &[Some("1"), Some("3")]),
            // The empty table is the point of the COALESCE idiom: 0, not NULL.
            ("SELECT COALESCE(sum(x), 0) FROM e", &[Some("0")]),
            ("SELECT sum(x) FROM e", &[None]),
            ("SELECT CAST(count(*) AS BIGINT) FROM e", &[Some("0")]),
            ("SELECT count(*) + 1 FROM e", &[Some("1")]),
            ("SELECT sum(a) / count(*) FROM e", &[None]),
            (
                "SELECT COALESCE(sum(a), 0), count(*) FROM e",
                &[Some("0"), Some("0")],
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn wrapped_aggregate_row_descriptions_match_the_materializing_path() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    exec(&mut session, "CREATE TABLE s (a BIGINT, x BIGINT)").await;
    exec(&mut session, "INSERT INTO s VALUES (1, 100)").await;

    // (name, oid) pairs recorded from the materializing path before this
    // streamed: int8 = 20, int4 = 23.
    let cases: [(&str, &[(&str, u32)]); 5] = [
        // A CAST is labelled by what is inside it, as PostgreSQL's FigureColname
        // is: the aggregate's own name here, the type's name when the inner
        // expression supplies none.
        ("SELECT CAST(count(*) AS BIGINT) FROM s", &[("count", 20)]),
        ("SELECT CAST(1 AS BIGINT) FROM s", &[("bigint", 20)]),
        ("SELECT COALESCE(sum(x), 0) FROM s", &[("coalesce", 20)]),
        // An arithmetic expression has no name of its own, and unlike a CAST it
        // has no type-name fallback either.
        ("SELECT count(*) + 1 FROM s", &[("?column?", 20)]),
        (
            "SELECT 1, count(*) FROM s",
            &[("?column?", 23), ("count", 20)],
        ),
    ];
    for (sql, expected) in cases {
        let fields = query_fields(&mut session, sql).await;
        let expected: Vec<(String, u32)> = expected
            .iter()
            .map(|(name, oid)| ((*name).to_string(), *oid))
            .collect();
        assert!(fields == expected, "wrong fields for {sql:?}");
    }
}

#[tokio::test]
async fn count_of_an_unknown_column_is_undefined_column() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    exec(&mut session, "CREATE TABLE t (id BIGINT)").await;
    exec(&mut session, "INSERT INTO t VALUES (1), (2)").await;

    // Before the per-call spec fix this silently streamed count(*) over all rows.
    assert!(query_error_code(&mut session, "SELECT count(nope) FROM t").await == "42703");
    assert!(query_error_code(&mut session, "SELECT count(nope) + 1 FROM t").await == "42703");
}

#[tokio::test]
async fn unsupported_shapes_keep_the_memory_budget_guard() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    seed_big_table(&mut session).await;

    // Whole-row materialization.
    assert!(query_error_code(&mut session, "SELECT * FROM big").await == "53200");
    // DISTINCT aggregates are outside the partial-aggregate pushdown model —
    // bare or wrapped in a scalar expression.
    assert!(query_error_code(&mut session, "SELECT count(DISTINCT v) FROM big").await == "53200");
    assert!(
        query_error_code(&mut session, "SELECT count(DISTINCT v) + 1 FROM big").await == "53200"
    );
    // An aggregate over a non-column argument stays on the materializing scan.
    assert!(query_error_code(&mut session, "SELECT sum(id + 1) FROM big").await == "53200");
    // A bare column beside an aggregate is not a streamable projection.
    assert!(query_error_code(&mut session, "SELECT id, count(*) FROM big").await == "53200");
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

/// `GROUPING()` outside a grouped query must keep `PostgreSQL`'s 42803, naming
/// the missing GROUP BY, on the streaming path as well as the materializing one.
///
/// The streaming cursor resolves projection expressions row-by-row and has no
/// notion of `GROUPING()`, so if it accepts the shape at all it reports 42883
/// "function grouping(...) does not exist" — a different error for the same SQL
/// depending only on which path served it. Its eligibility gate therefore has
/// to decline on `grouping::is_grouping_query`, not just on
/// `agg::is_aggregate_query`: `GROUPING()` is not an aggregate.
///
/// This regressed the differential-conformance parity gate the moment
/// `RuntimeSession` began forwarding `simple_query_into`, which is what made
/// the streaming path reachable in the first place.
#[tokio::test]
async fn bare_grouping_call_keeps_its_sqlstate_on_the_streaming_path() {
    use crabka_pgwire::engine::CollectingResultSink;

    let engine = SqlEngine::new();
    let mut setup = engine.connect();
    exec(&mut setup, "CREATE TABLE q3gs (a int4, b int4)").await;
    exec(&mut setup, "INSERT INTO q3gs VALUES (1, 2)").await;

    // Both entry points must agree, and both must agree with PostgreSQL.
    let mut session = engine.connect();
    let buffered = session.simple_query("SELECT grouping(a) FROM q3gs").await;
    let error = buffered.expect_err("bare GROUPING() is an error");
    assert!(error.code == "42803", "buffered path: {error:?}");

    let mut sink = CollectingResultSink::default();
    let streamed = session
        .simple_query_into("SELECT grouping(a) FROM q3gs", 16, &mut sink)
        .await;
    let error = streamed.expect_err("bare GROUPING() is an error");
    assert!(error.code == "42803", "streaming path: {error:?}");
}
