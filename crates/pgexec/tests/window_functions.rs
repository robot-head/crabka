//! Q2: window functions: `OVER`, frames, named windows, and the window-function
//! set, end-to-end over the wire against `PostgreSQL` 18.4's observed behavior.

use std::sync::Arc;

use assert2::assert;
use crabka_pgexec::SqlEngine;
use crabka_pgwire::session::SessionConfig;
use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls, types::Type};

async fn spawn() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(SqlEngine::new()),
        Arc::new(SessionConfig::trust()),
    ));
    port
}

async fn connect(port: u16) -> Client {
    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

/// Every row of the result, rendered as one tab-joined text line.
///
/// A NULL becomes an empty field, so a whole expected result compares as one
/// value.
async fn rows(client: &Client, sql: &str) -> Vec<String> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("query `{sql}` failed: {e}"))
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).unwrap_or("").to_string())
                    .collect::<Vec<_>>()
                    .join("\t"),
            ),
            _ => None,
        })
        .collect()
}

async fn sqlstate(client: &Client, sql: &str) -> String {
    match client.simple_query(sql).await {
        Ok(_) => panic!("`{sql}` unexpectedly succeeded"),
        Err(error) => error
            .as_db_error()
            .unwrap_or_else(|| panic!("`{sql}` produced no db error: {error}"))
            .code()
            .code()
            .to_string(),
    }
}

async fn fixture() -> Client {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE w (g int4, v int4, s text); \
             INSERT INTO w VALUES (1, 10, 'a'), (1, 20, 'b'), (2, 30, 'c'), \
                                  (2, 30, 'd'), (3, NULL, 'e')",
        )
        .await
        .expect("fixture");
    client
}

#[tokio::test]
async fn ranking_family_matches_postgres() {
    let client = fixture().await;
    let cases: Vec<(&str, Vec<&str>)> = vec![
        (
            "SELECT row_number() OVER (ORDER BY g, v) FROM w ORDER BY 1",
            vec!["1", "2", "3", "4", "5"],
        ),
        (
            "SELECT rank() OVER (ORDER BY g) FROM w ORDER BY 1",
            vec!["1", "1", "3", "3", "5"],
        ),
        (
            "SELECT dense_rank() OVER (ORDER BY g) FROM w ORDER BY 1",
            vec!["1", "1", "2", "2", "3"],
        ),
        (
            "SELECT percent_rank() OVER (ORDER BY g) FROM w ORDER BY 1",
            vec!["0", "0", "0.5", "0.5", "1"],
        ),
        (
            "SELECT cume_dist() OVER (ORDER BY g) FROM w ORDER BY 1",
            vec!["0.4", "0.4", "0.8", "0.8", "1"],
        ),
        (
            "SELECT ntile(2) OVER (ORDER BY g, v) FROM w ORDER BY 1",
            vec!["1", "1", "1", "2", "2"],
        ),
        (
            "SELECT ntile(3) OVER (ORDER BY g, v) FROM w ORDER BY 1",
            vec!["1", "1", "2", "2", "3"],
        ),
        // More buckets than rows gives every row its own bucket.
        (
            "SELECT ntile(9) OVER (ORDER BY g, v) FROM w ORDER BY 1",
            vec!["1", "2", "3", "4", "5"],
        ),
    ];
    for (sql, expected) in cases {
        assert!(rows(&client, sql).await == expected, "{sql}");
    }
}

#[tokio::test]
async fn value_functions_read_the_partition_and_the_frame() {
    let client = fixture().await;
    let cases: Vec<(&str, Vec<&str>)> = vec![
        (
            "SELECT g, lag(v) OVER w1, lead(v) OVER w1 FROM w \
             WINDOW w1 AS (ORDER BY g, v) ORDER BY g, v",
            vec!["1\t\t20", "1\t10\t30", "2\t20\t30", "2\t30\t", "3\t30\t"],
        ),
        (
            "SELECT lag(v, 2, -1) OVER (ORDER BY g, v) FROM w ORDER BY g, v",
            vec!["-1", "-1", "10", "20", "30"],
        ),
        // A NULL offset yields NULL, not an error.
        (
            "SELECT lag(v, NULL) OVER (ORDER BY g, v) FROM w ORDER BY g, v",
            vec!["", "", "", "", ""],
        ),
        // The default frame ends at the current row's last peer.
        (
            "SELECT first_value(v) OVER w1, last_value(v) OVER w1 FROM w \
             WINDOW w1 AS (ORDER BY g) ORDER BY g, v",
            vec!["10\t20", "10\t20", "10\t30", "10\t30", "10\t"],
        ),
        (
            "SELECT nth_value(v, 2) OVER (ORDER BY g, v) FROM w ORDER BY g, v",
            vec!["", "20", "20", "20", "20"],
        ),
        // lag and lead ignore the frame.
        (
            "SELECT lag(v) OVER (ORDER BY g, v ROWS BETWEEN CURRENT ROW AND CURRENT ROW) \
             FROM w ORDER BY g, v",
            vec!["", "10", "20", "30", "30"],
        ),
    ];
    for (sql, expected) in cases {
        assert!(rows(&client, sql).await == expected, "{sql}");
    }
}

#[tokio::test]
async fn frames_select_the_rows_an_aggregate_folds() {
    let client = fixture().await;
    let cases: Vec<(&str, Vec<&str>)> = vec![
        // Default frame: peers share the running total.
        (
            "SELECT sum(v) OVER (ORDER BY g) FROM w ORDER BY g, v",
            vec!["30", "30", "90", "90", "90"],
        ),
        (
            "SELECT sum(v) OVER (ORDER BY g, v ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
             FROM w ORDER BY g, v",
            vec!["30", "60", "80", "60", "30"],
        ),
        (
            "SELECT sum(v) OVER (ORDER BY g, v ROWS UNBOUNDED PRECEDING) FROM w ORDER BY g, v",
            vec!["10", "30", "60", "90", "90"],
        ),
        // An out-of-range frame is empty: count 0, sum NULL.
        (
            "SELECT count(*) OVER (ORDER BY g ROWS BETWEEN 9 FOLLOWING AND 10 FOLLOWING) \
             FROM w ORDER BY g, v",
            vec!["0", "0", "0", "0", "0"],
        ),
        (
            "SELECT sum(v) OVER (ORDER BY g ROWS BETWEEN 9 FOLLOWING AND 10 FOLLOWING) \
             FROM w ORDER BY g, v",
            vec!["", "", "", "", ""],
        ),
        // GROUPS counts whole peer groups.
        (
            "SELECT count(*) OVER (ORDER BY g GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
             FROM w ORDER BY g, v",
            vec!["4", "4", "5", "5", "3"],
        ),
        (
            "SELECT count(*) OVER (ORDER BY g GROUPS 1 PRECEDING) FROM w ORDER BY g, v",
            vec!["2", "2", "4", "4", "3"],
        ),
        // RANGE with an offset uses the ordering column's arithmetic.
        (
            "SELECT sum(v) OVER (ORDER BY v RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING) \
             FROM w ORDER BY v NULLS LAST",
            vec!["30", "90", "80", "80", ""],
        ),
        // A NULL ordering value's frame is exactly the run of NULLs.
        (
            "SELECT count(*) OVER (ORDER BY v RANGE BETWEEN 5 PRECEDING AND 5 FOLLOWING) \
             FROM w ORDER BY v NULLS LAST",
            vec!["1", "1", "2", "2", "1"],
        ),
        // A DESC ordering mirrors the offset directions.
        (
            "SELECT sum(v) OVER (ORDER BY v DESC RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) \
             FROM w ORDER BY v NULLS LAST",
            vec!["30", "80", "60", "60", ""],
        ),
    ];
    for (sql, expected) in cases {
        assert!(rows(&client, sql).await == expected, "{sql}");
    }
}

#[tokio::test]
async fn frame_offsets_past_the_partition_saturate_instead_of_overflowing() {
    let client = fixture().await;
    // PostgreSQL's in_range support functions clamp rather than erroring, so a
    // bigint-wide offset is a whole-partition frame — and must not overflow.
    let cases: Vec<(&str, Vec<&str>)> = vec![
        (
            "SELECT count(*) OVER (ORDER BY g ROWS BETWEEN 9223372036854775807 PRECEDING \
             AND 9223372036854775807 FOLLOWING) FROM w ORDER BY g, v",
            vec!["5", "5", "5", "5", "5"],
        ),
        (
            "SELECT count(*) OVER (ORDER BY g GROUPS BETWEEN 9223372036854775807 PRECEDING \
             AND 9223372036854775807 FOLLOWING) FROM w ORDER BY g, v",
            vec!["5", "5", "5", "5", "5"],
        ),
        (
            "SELECT count(*) OVER (ORDER BY g ROWS BETWEEN 9223372036854775807 FOLLOWING \
             AND 9223372036854775807 FOLLOWING) FROM w ORDER BY g, v",
            vec!["0", "0", "0", "0", "0"],
        ),
        (
            "SELECT sum(v) OVER (ORDER BY v RANGE BETWEEN 2147483647 PRECEDING \
             AND 2147483647 FOLLOWING) FROM w ORDER BY v NULLS LAST",
            vec!["90", "90", "90", "90", ""],
        ),
    ];
    for (sql, expected) in cases {
        assert!(rows(&client, sql).await == expected, "{sql}");
    }
}

#[tokio::test]
async fn exclusion_drops_the_current_row_its_group_or_its_ties() {
    let client = fixture().await;
    let whole = "ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING";
    let cases: Vec<(String, Vec<&str>)> = vec![
        (
            format!("SELECT sum(v) OVER ({whole} EXCLUDE NO OTHERS) FROM w ORDER BY g, v"),
            vec!["90", "90", "90", "90", "90"],
        ),
        (
            format!("SELECT sum(v) OVER ({whole} EXCLUDE CURRENT ROW) FROM w ORDER BY g, v"),
            vec!["80", "70", "60", "60", "90"],
        ),
        (
            format!("SELECT sum(v) OVER ({whole} EXCLUDE GROUP) FROM w ORDER BY g, v"),
            vec!["60", "60", "30", "30", "90"],
        ),
        (
            format!("SELECT sum(v) OVER ({whole} EXCLUDE TIES) FROM w ORDER BY g, v"),
            vec!["70", "80", "60", "60", "90"],
        ),
    ];
    for (sql, expected) in cases {
        assert!(rows(&client, &sql).await == expected, "{sql}");
    }
}

#[tokio::test]
async fn named_windows_are_reusable_and_composable() {
    let client = fixture().await;
    let cases: Vec<(&str, Vec<&str>)> = vec![
        (
            "SELECT rank() OVER x, count(*) OVER x FROM w \
             WINDOW x AS (PARTITION BY g ORDER BY v) ORDER BY g, v",
            vec!["1\t1", "2\t2", "1\t2", "1\t2", "1\t1"],
        ),
        // A window may be built from an earlier one, adding only an ordering.
        (
            "SELECT count(*) OVER a, count(*) OVER b FROM w \
             WINDOW a AS (PARTITION BY g), b AS (a ORDER BY v) ORDER BY g, v",
            vec!["2\t1", "2\t2", "2\t2", "2\t2", "1\t1"],
        ),
        (
            "SELECT count(*) OVER (a ORDER BY v) FROM w \
             WINDOW a AS (PARTITION BY g) ORDER BY g, v",
            vec!["1", "2", "2", "2", "1"],
        ),
        // An unused definition is legal.
        (
            "SELECT g FROM w WINDOW unused AS (ORDER BY v) ORDER BY g, v",
            vec!["1", "1", "2", "2", "3"],
        ),
    ];
    for (sql, expected) in cases {
        assert!(rows(&client, sql).await == expected, "{sql}");
    }
}

#[tokio::test]
async fn ordinary_aggregates_run_as_window_functions() {
    let client = fixture().await;
    let cases: Vec<(&str, Vec<&str>)> = vec![
        (
            "SELECT count(*) OVER (), count(v) OVER () FROM w ORDER BY g, v LIMIT 1",
            vec!["5\t4"],
        ),
        (
            "SELECT string_agg(s, ',') OVER (ORDER BY g, v, s) FROM w ORDER BY g, v, s",
            vec!["a", "a,b", "a,b,c", "a,b,c,d", "a,b,c,d,e"],
        ),
        (
            "SELECT array_agg(g) OVER (PARTITION BY g) FROM w ORDER BY g, v",
            vec!["{1,1}", "{1,1}", "{2,2}", "{2,2}", "{3}"],
        ),
        (
            "SELECT min(v) OVER (PARTITION BY g), max(v) OVER (PARTITION BY g) \
             FROM w ORDER BY g, v",
            vec!["10\t20", "10\t20", "30\t30", "30\t30", "\t"],
        ),
        // FILTER restricts which frame rows the aggregate folds.
        (
            "SELECT count(*) FILTER (WHERE v > 10) OVER (ORDER BY g) FROM w ORDER BY g, v",
            vec!["1", "1", "3", "3", "3"],
        ),
    ];
    for (sql, expected) in cases {
        assert!(rows(&client, sql).await == expected, "{sql}");
    }
}

#[tokio::test]
async fn windows_run_after_where_and_group_by_and_before_distinct_and_limit() {
    let client = fixture().await;
    let cases: Vec<(&str, Vec<&str>)> = vec![
        // WHERE first: the filtered-out row never reaches the ranking.
        (
            "SELECT rank() OVER (ORDER BY g) FROM w WHERE v IS NOT NULL ORDER BY 1",
            vec!["1", "1", "3", "3"],
        ),
        // DISTINCT after: duplicate ranks collapse.
        (
            "SELECT DISTINCT rank() OVER (ORDER BY g) FROM w ORDER BY 1",
            vec!["1", "3", "5"],
        ),
        // LIMIT after: the window still saw every row.
        (
            "SELECT sum(v) OVER () FROM w ORDER BY g, v LIMIT 2",
            vec!["90", "90"],
        ),
        // GROUP BY below: the window reads the grouped rows.
        (
            "SELECT g, sum(v), rank() OVER (ORDER BY sum(v) DESC NULLS LAST) \
             FROM w GROUP BY g ORDER BY g",
            vec!["1\t30\t2", "2\t60\t1", "3\t\t3"],
        ),
        (
            "SELECT g, sum(sum(v)) OVER (ORDER BY g) FROM w GROUP BY g ORDER BY g",
            vec!["1\t30", "2\t90", "3\t90"],
        ),
        // An arithmetic combination of a window result and a grouped value.
        (
            "SELECT g, rank() OVER (ORDER BY g) + count(*) FROM w GROUP BY g ORDER BY g",
            vec!["1\t3", "2\t4", "3\t4"],
        ),
    ];
    for (sql, expected) in cases {
        assert!(rows(&client, sql).await == expected, "{sql}");
    }
}

#[tokio::test]
async fn unaliased_window_call_is_labelled_after_its_function() {
    let client = fixture().await;
    let described = client
        .prepare("SELECT row_number() OVER (), sum(v) OVER () FROM w")
        .await
        .expect("prepare");
    let labels: Vec<&str> = described
        .columns()
        .iter()
        .map(tokio_postgres::Column::name)
        .collect();
    assert!(labels == vec!["row_number", "sum"]);

    // `*` beside a window call expands only the relation's own columns.
    assert!(
        rows(
            &client,
            "SELECT *, row_number() OVER (ORDER BY g, v) FROM w ORDER BY g, v"
        )
        .await
            == vec![
                "1\t10\ta\t1",
                "1\t20\tb\t2",
                "2\t30\tc\t3",
                "2\t30\td\t4",
                "3\t\te\t5",
            ]
    );
}

#[tokio::test]
async fn describe_reports_each_window_function_result_type() {
    let client = fixture().await;
    let described = client
        .prepare(
            "SELECT row_number() OVER (), rank() OVER (ORDER BY g), \
                    dense_rank() OVER (ORDER BY g), percent_rank() OVER (ORDER BY g), \
                    cume_dist() OVER (ORDER BY g), ntile(2) OVER (), \
                    lag(g) OVER (), lag(s) OVER (), first_value(v) OVER (), \
                    nth_value(s, 1) OVER (), sum(g) OVER (), avg(g) OVER (), \
                    count(*) OVER () \
             FROM w",
        )
        .await
        .expect("prepare");
    let types: Vec<Type> = described
        .columns()
        .iter()
        .map(|c| c.type_().clone())
        .collect();
    assert!(
        types
            == vec![
                Type::INT8,
                Type::INT8,
                Type::INT8,
                Type::FLOAT8,
                Type::FLOAT8,
                Type::INT4,
                Type::INT4,
                Type::TEXT,
                Type::INT4,
                Type::TEXT,
                Type::INT8,
                Type::NUMERIC,
                Type::INT8,
            ]
    );
}

#[tokio::test]
async fn misplaced_and_malformed_window_calls_report_postgres_sqlstates() {
    let client = fixture().await;
    let cases: Vec<(&str, &str)> = vec![
        ("SELECT row_number() FROM w", "42809"),
        ("SELECT * FROM w WHERE row_number() OVER () > 1", "42P20"),
        ("SELECT g FROM w GROUP BY row_number() OVER ()", "42P20"),
        ("SELECT g FROM w HAVING row_number() OVER () > 1", "42P20"),
        ("SELECT sum(DISTINCT v) OVER () FROM w", "0A000"),
        (
            "SELECT row_number() FILTER (WHERE v > 1) OVER () FROM w",
            "0A000",
        ),
        ("SELECT count(*) OVER nope FROM w", "42704"),
        (
            "SELECT count(*) OVER (x PARTITION BY v) FROM w WINDOW x AS (PARTITION BY g)",
            "42P20",
        ),
        (
            "SELECT count(*) OVER (x ORDER BY v) FROM w WINDOW x AS (PARTITION BY g ORDER BY v)",
            "42P20",
        ),
        (
            "SELECT count(*) OVER (x) FROM w \
             WINDOW x AS (ORDER BY g ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)",
            "42P20",
        ),
        (
            "SELECT count(*) OVER () FROM w WINDOW x AS (), x AS ()",
            "42P20",
        ),
        ("SELECT ntile(0) OVER () FROM w", "22014"),
        ("SELECT ntile(-1) OVER () FROM w", "22014"),
        ("SELECT nth_value(v, 0) OVER () FROM w", "22016"),
        ("SELECT lag() OVER () FROM w", "42883"),
        ("SELECT first_value(v, 2) OVER () FROM w", "42883"),
        ("SELECT nosuchwinfunc() OVER () FROM w", "42883"),
    ];
    for (sql, expected) in cases {
        assert!(sqlstate(&client, sql).await == expected, "{sql}");
    }
}

#[tokio::test]
async fn malformed_frames_report_postgres_sqlstates() {
    let client = fixture().await;
    let cases: Vec<(&str, &str)> = vec![
        (
            "SELECT sum(v) OVER (ORDER BY g ROWS BETWEEN 1 FOLLOWING AND 1 PRECEDING) FROM w",
            "42P20",
        ),
        (
            "SELECT sum(v) OVER (ORDER BY g ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) \
             FROM w",
            "42P20",
        ),
        (
            "SELECT sum(v) OVER (ORDER BY g ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) \
             FROM w",
            "42P20",
        ),
        (
            "SELECT sum(v) OVER (ORDER BY g ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM w",
            "42P20",
        ),
        (
            "SELECT sum(v) OVER (ORDER BY g ROWS -1 PRECEDING) FROM w",
            "22013",
        ),
        (
            "SELECT sum(v) OVER (ORDER BY g GROUPS -1 PRECEDING) FROM w",
            "22013",
        ),
        (
            "SELECT sum(v) OVER (ORDER BY g ROWS NULL PRECEDING) FROM w",
            "22004",
        ),
        (
            "SELECT sum(v) OVER (RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM w",
            "42P20",
        ),
        (
            "SELECT sum(v) OVER (ORDER BY g, v RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM w",
            "42P20",
        ),
        (
            "SELECT sum(v) OVER (ORDER BY v RANGE BETWEEN -1 PRECEDING AND 1 FOLLOWING) FROM w",
            "22013",
        ),
        (
            "SELECT sum(v) OVER (ORDER BY v RANGE BETWEEN 1.5 PRECEDING AND 1 FOLLOWING) FROM w",
            "0A000",
        ),
        (
            "SELECT sum(v) OVER (ORDER BY s RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM w",
            "0A000",
        ),
    ];
    for (sql, expected) in cases {
        assert!(sqlstate(&client, sql).await == expected, "{sql}");
    }
}

#[tokio::test]
async fn range_offsets_use_the_ordering_column_type_arithmetic() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE ts (t timestamp, v int4); \
             INSERT INTO ts VALUES ('2020-01-01 00:00:00', 1), ('2020-01-01 00:30:00', 2), \
                                   ('2020-01-01 02:00:00', 4), ('2020-01-02 00:00:00', 8)",
        )
        .await
        .expect("fixture");
    let cases: Vec<(&str, Vec<&str>)> = vec![
        (
            "SELECT sum(v) OVER (ORDER BY t RANGE BETWEEN INTERVAL '1 hour' PRECEDING \
             AND CURRENT ROW) FROM ts ORDER BY t",
            vec!["1", "3", "4", "8"],
        ),
        (
            "SELECT sum(v) OVER (ORDER BY t RANGE BETWEEN INTERVAL '1 hour' PRECEDING \
             AND INTERVAL '1 hour' FOLLOWING) FROM ts ORDER BY t",
            vec!["3", "3", "4", "8"],
        ),
        (
            "SELECT sum(v) OVER (ORDER BY t DESC RANGE BETWEEN INTERVAL '1 hour' PRECEDING \
             AND INTERVAL '1 hour' FOLLOWING) FROM ts ORDER BY t",
            vec!["3", "3", "4", "8"],
        ),
    ];
    for (sql, expected) in cases {
        assert!(rows(&client, sql).await == expected, "{sql}");
    }
}

#[tokio::test]
async fn window_over_an_empty_relation_produces_no_rows() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE e (v int4)")
        .await
        .expect("fixture");
    assert!(
        rows(&client, "SELECT rank() OVER (ORDER BY v) FROM e")
            .await
            .is_empty()
    );
    assert!(
        rows(&client, "SELECT count(*) OVER () FROM e")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn window_call_inside_a_derived_table_is_owned_by_that_subquery() {
    let client = fixture().await;
    assert!(
        rows(
            &client,
            "SELECT d.g, d.r FROM (SELECT g, rank() OVER (ORDER BY g) AS r FROM w) d \
             WHERE d.r > 1 ORDER BY d.g, d.r",
        )
        .await
            == vec!["2\t3", "2\t3", "3\t5"]
    );
    // The outer query's own ORDER BY window call is independent of the inner one.
    assert!(
        rows(
            &client,
            "SELECT g FROM w ORDER BY row_number() OVER (ORDER BY g DESC, v DESC)",
        )
        .await
            == vec!["3", "2", "2", "1", "1"]
    );
}

/// The fixture the remediation tests below share.
///
/// `id` is dense and unique, so a window ORDER BY is total. `g` has a peer run
/// and a NULL. `v` carries the values that `ntile` and `lag` read.
async fn ordered_fixture() -> Client {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE o (id int4, g text, v int4, s text); \
             INSERT INTO o VALUES (1,'a',10,'x'), (2,'a',20,'y'), (3,'a',NULL,'z'), \
                                  (4,'b',20,'p'), (5,'b',5,'q'), (6,NULL,7,'r')",
        )
        .await
        .expect("fixture");
    client
}

#[tokio::test]
async fn ntile_reads_its_argument_once_per_partition() {
    let client = ordered_fixture().await;
    let cases: Vec<(&str, Vec<&str>)> = vec![
        // The partition's FIRST row in window order decides the bucket count;
        // later rows' values are never looked at.
        (
            "SELECT ntile(CASE WHEN id=1 THEN 2 ELSE 6 END) OVER (ORDER BY id) FROM o ORDER BY id",
            vec!["1", "1", "1", "2", "2", "2"],
        ),
        // Including a zero, which would be 22014 had it been read.
        (
            "SELECT ntile(CASE WHEN id=2 THEN 0 ELSE 2 END) OVER (ORDER BY id) FROM o ORDER BY id",
            vec!["1", "1", "1", "2", "2", "2"],
        ),
        // DESC ordering reads the last row by id, because that is the first in
        // window order.
        (
            "SELECT ntile(CASE WHEN id=6 THEN 2 ELSE 3 END) OVER (ORDER BY id DESC) \
             FROM o ORDER BY id",
            vec!["2", "2", "2", "1", "1", "1"],
        ),
        // Each partition reads its own first row.
        (
            "SELECT ntile(CASE WHEN id=1 THEN 1 WHEN id=4 THEN 2 ELSE 9 END) \
             OVER (PARTITION BY g ORDER BY id) FROM o ORDER BY id",
            vec!["1", "1", "1", "1", "2", "1"],
        ),
        // A NULL there is that row's result alone and leaves the run unarmed, so
        // the NEXT row arms it — over the whole partition's row count.
        (
            "SELECT ntile(CASE WHEN id=1 THEN NULL ELSE 2 END) OVER (ORDER BY id) \
             FROM o ORDER BY id",
            vec!["", "1", "1", "1", "2", "2"],
        ),
        // The plain bucket split, for every divisibility case.
        (
            "SELECT ntile(4) OVER (ORDER BY id) FROM o ORDER BY id",
            vec!["1", "1", "2", "2", "3", "4"],
        ),
        (
            "SELECT ntile(7) OVER (ORDER BY id) FROM o ORDER BY id",
            vec!["1", "2", "3", "4", "5", "6"],
        ),
    ];
    for (sql, expected) in cases {
        assert!(rows(&client, sql).await == expected, "{sql}");
    }
    // A zero on the row that IS read is still 22014.
    assert!(
        sqlstate(
            &client,
            "SELECT ntile(CASE WHEN id=1 THEN 0 ELSE 2 END) OVER (ORDER BY id) FROM o",
        )
        .await
            == "22014"
    );
}

#[tokio::test]
async fn a_window_over_grouping_sets_sees_every_set_row() {
    let client = ordered_fixture().await;
    let cases: Vec<(&str, Vec<&str>)> = vec![
        // ROLLUP emits a grand-total row, and the window counts it.
        (
            "SELECT g, count(*), count(*) OVER () FROM o GROUP BY ROLLUP(g) \
             ORDER BY g NULLS LAST, 2",
            vec!["a\t3\t4", "b\t2\t4", "\t1\t4", "\t6\t4"],
        ),
        (
            "SELECT g, count(*), count(*) OVER () FROM o GROUP BY CUBE(g) \
             ORDER BY g NULLS LAST, 2",
            vec!["a\t3\t4", "b\t2\t4", "\t1\t4", "\t6\t4"],
        ),
        // A repeated grouping set emits its rows twice.
        (
            "SELECT g, count(*), count(*) OVER () FROM o GROUP BY GROUPING SETS ((g),(g)) \
             ORDER BY g NULLS LAST",
            vec![
                "a\t3\t6", "a\t3\t6", "b\t2\t6", "b\t2\t6", "\t1\t6", "\t1\t6",
            ],
        ),
        (
            "SELECT sum(count(*)) OVER () FROM o GROUP BY GROUPING SETS ((g),())",
            vec!["12", "12", "12", "12"],
        ),
        // GROUPING() still tells the sets apart under a window.
        (
            "SELECT g, grouping(g), count(*) OVER () FROM o GROUP BY ROLLUP(g) \
             ORDER BY g NULLS LAST, 2",
            vec!["a\t0\t4", "b\t0\t4", "\t0\t4", "\t1\t4"],
        ),
        // A SQL92 output reference in GROUP BY names the ORIGINAL select list,
        // which the window node has already resolved.
        (
            "SELECT g, count(*), count(*) OVER () FROM o GROUP BY 1 ORDER BY g NULLS LAST",
            vec!["a\t3\t3", "b\t2\t3", "\t1\t3"],
        ),
        (
            "SELECT g AS gg, count(*), count(*) OVER () FROM o GROUP BY gg \
             ORDER BY gg NULLS LAST",
            vec!["a\t3\t3", "b\t2\t3", "\t1\t3"],
        ),
    ];
    for (sql, expected) in cases {
        assert!(rows(&client, sql).await == expected, "{sql}");
    }
}

#[tokio::test]
async fn distinct_on_and_order_by_match_on_the_same_window_call() {
    let client = ordered_fixture().await;
    let cases: Vec<(&str, Vec<&str>)> = vec![
        (
            "SELECT DISTINCT ON (row_number() OVER ()) id FROM o \
             ORDER BY row_number() OVER (), id",
            vec!["1", "2", "3", "4", "5", "6"],
        ),
        (
            "SELECT DISTINCT ON (rank() OVER (ORDER BY g NULLS LAST)) id FROM o \
             ORDER BY rank() OVER (ORDER BY g NULLS LAST), id",
            vec!["1", "4", "6"],
        ),
    ];
    for (sql, expected) in cases {
        assert!(rows(&client, sql).await == expected, "{sql}");
    }
    // A DISTINCT ON key the ORDER BY does not lead with is still 42P10.
    assert!(
        sqlstate(
            &client,
            "SELECT DISTINCT ON (rank() OVER (ORDER BY g NULLS LAST)) id FROM o ORDER BY id",
        )
        .await
            == "42P10"
    );
}

#[tokio::test]
async fn a_window_call_in_a_subquery_tail_belongs_to_that_subquery() {
    let client = ordered_fixture().await;
    let cases: Vec<(&str, Vec<&str>)> = vec![
        (
            "SELECT id FROM (SELECT id FROM o ORDER BY rank() OVER (ORDER BY id DESC) LIMIT 2) q \
             ORDER BY id",
            vec!["5", "6"],
        ),
        (
            "SELECT id FROM o WHERE id IN \
             (SELECT id FROM o ORDER BY rank() OVER (ORDER BY id) LIMIT 2) ORDER BY id",
            vec!["1", "2"],
        ),
    ];
    for (sql, expected) in cases {
        assert!(rows(&client, sql).await == expected, "{sql}");
    }
}

#[tokio::test]
async fn lag_and_lead_deliver_one_type_for_the_value_and_the_default() {
    let client = ordered_fixture().await;
    // `anycompatible` unifies the value and the default, so the column carries
    // that one type — a `RowDescription` of `integer` is never followed by text.
    let described = client
        .prepare("SELECT lag(v, 1, 5.5) OVER (ORDER BY id) FROM o")
        .await
        .expect("prepare");
    assert!(described.columns()[0].type_() == &Type::NUMERIC);
    let cases: Vec<(&str, Vec<&str>)> = vec![
        (
            "SELECT lag(v, 1, 5.5) OVER (ORDER BY id) FROM o ORDER BY id",
            vec!["5.5", "10", "20", "", "20", "5"],
        ),
        // An `unknown` literal default adopts the value's type.
        (
            "SELECT lag(v, 1, '7') OVER (ORDER BY id) FROM o ORDER BY id",
            vec!["7", "10", "20", "", "20", "5"],
        ),
    ];
    for (sql, expected) in cases {
        assert!(rows(&client, sql).await == expected, "{sql}");
    }
    let errors: Vec<(&str, &str)> = vec![
        // A default that cannot be read as the value's type is that type's input
        // error, not a text column emitted under an integer description.
        ("SELECT lag(v, 1, 'zzz') OVER (ORDER BY id) FROM o", "22P02"),
        (
            "SELECT lead(v, 1, 'zzz') OVER (ORDER BY id) FROM o",
            "22P02",
        ),
        // No common type at all is 42883, as an unresolvable `anycompatible` is.
        ("SELECT lag(s, 1, 5) OVER (ORDER BY id) FROM o", "42883"),
        // The offset parameter is `integer`; there is no bigint or numeric one.
        (
            "SELECT lag(v, 9223372036854775807) OVER (ORDER BY id) FROM o",
            "42883",
        ),
        (
            "SELECT nth_value(v, 9223372036854775807) OVER () FROM o",
            "42883",
        ),
        ("SELECT nth_value(v, 2.0) OVER () FROM o", "42883"),
        ("SELECT ntile(3::bigint) OVER (ORDER BY id) FROM o", "42883"),
        ("SELECT ntile(2.5) OVER (ORDER BY id) FROM o", "42883"),
        // `OVER` on a real function that is neither a window function nor an
        // aggregate is 42809; on a name nothing matches it stays 42883.
        ("SELECT upper(s) OVER () FROM o", "42809"),
        ("SELECT nosuchfn(s) OVER () FROM o", "42883"),
        ("SELECT sum(s) OVER () FROM o", "42883"),
    ];
    for (sql, expected) in errors {
        assert!(sqlstate(&client, sql).await == expected, "{sql}");
    }
    // smallint widens into the integer offset parameter.
    assert!(
        rows(
            &client,
            "SELECT lag(v, 2::smallint) OVER (ORDER BY id) FROM o ORDER BY id",
        )
        .await
            == vec!["", "", "10", "20", "", "20"]
    );
}

#[tokio::test]
async fn groups_mode_requires_an_order_by() {
    let client = ordered_fixture().await;
    for sql in [
        "SELECT count(*) OVER (PARTITION BY g GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM o",
        "SELECT count(*) OVER (GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM o",
        "SELECT count(*) OVER (GROUPS CURRENT ROW) FROM o",
        "SELECT count(*) OVER (PARTITION BY g GROUPS UNBOUNDED PRECEDING) FROM o",
        "SELECT count(*) OVER (w1 GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM o \
         WINDOW w1 AS (PARTITION BY g)",
    ] {
        assert!(sqlstate(&client, sql).await == "42P20", "{sql}");
    }
    // RANGE, by contrast, needs an ORDER BY only for an offset bound.
    assert!(
        rows(
            &client,
            "SELECT count(*) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) \
             FROM o ORDER BY 1",
        )
        .await
            == vec!["6", "6", "6", "6", "6", "6"]
    );
}

#[tokio::test]
async fn a_locking_read_cannot_carry_a_window_function() {
    let client = ordered_fixture().await;
    for (sql, expected) in [
        ("SELECT id, row_number() OVER () FROM o FOR UPDATE", "0A000"),
        ("SELECT id, row_number() OVER () FROM o FOR SHARE", "0A000"),
    ] {
        assert!(sqlstate(&client, sql).await == expected, "{sql}");
    }
}

#[tokio::test]
async fn range_offsets_follow_postgres_in_range() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE fl (v float8); \
             INSERT INTO fl VALUES (1), (2), ('NaN'), ('Infinity'), ('-Infinity'), (NULL); \
             CREATE TABLE nm (v numeric); \
             INSERT INTO nm VALUES (1.0), (2.0), ('NaN'), (NULL); \
             CREATE TABLE e (v float8)",
        )
        .await
        .expect("fixture");
    let cases: Vec<(&str, Vec<&str>)> = vec![
        // A NaN ordering value has no arithmetic neighbourhood: its frame is
        // exactly its own peer run, and it is in nobody else's frame.
        (
            "SELECT v, count(*) OVER (ORDER BY v RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
             FROM fl ORDER BY v NULLS LAST",
            vec![
                "-Infinity\t1",
                "1\t2",
                "2\t2",
                "Infinity\t1",
                "NaN\t1",
                "\t1",
            ],
        ),
        // +inf infinitely precedes +inf, so an infinite offset against it admits
        // every finite and infinite value — but not NaN.
        (
            "SELECT v, count(*) OVER (ORDER BY v \
             RANGE BETWEEN 'Infinity'::float8 PRECEDING AND 1 FOLLOWING) \
             FROM fl ORDER BY v NULLS LAST",
            vec![
                "-Infinity\t1",
                "1\t3",
                "2\t3",
                "Infinity\t4",
                "NaN\t1",
                "\t1",
            ],
        ),
        // -inf infinitely follows -inf, symmetrically.
        (
            "SELECT v, count(*) OVER (ORDER BY v \
             RANGE BETWEEN CURRENT ROW AND 'Infinity'::float8 FOLLOWING) \
             FROM fl ORDER BY v NULLS LAST",
            vec![
                "-Infinity\t4",
                "1\t3",
                "2\t2",
                "Infinity\t1",
                "NaN\t1",
                "\t1",
            ],
        ),
        (
            "SELECT v, count(*) OVER (ORDER BY v RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
             FROM nm ORDER BY v NULLS LAST",
            vec!["1.0\t2", "2.0\t2", "NaN\t1", "\t1"],
        ),
        // An `unknown` literal offset adopts the ordering column's in_range type.
        (
            "SELECT v, count(*) OVER (ORDER BY v RANGE BETWEEN '1' PRECEDING AND 1 FOLLOWING) \
             FROM nm ORDER BY v NULLS LAST",
            vec!["1.0\t2", "2.0\t2", "NaN\t1", "\t1"],
        ),
    ];
    for (sql, expected) in cases {
        assert!(rows(&client, sql).await == expected, "{sql}");
    }
    let errors: Vec<(&str, &str)> = vec![
        // A NaN or negative RANGE offset is rejected where in_range sees it.
        (
            "SELECT count(*) OVER (ORDER BY v \
             RANGE BETWEEN 'NaN'::float8 PRECEDING AND 1 FOLLOWING) FROM fl",
            "22013",
        ),
        (
            "SELECT count(*) OVER (ORDER BY v \
             RANGE BETWEEN CURRENT ROW AND 'NaN'::float8 FOLLOWING) FROM fl",
            "22013",
        ),
        (
            "SELECT count(*) OVER (ORDER BY v \
             RANGE BETWEEN 'NaN'::numeric PRECEDING AND 1 FOLLOWING) FROM nm",
            "22013",
        ),
        // An `unknown` literal that does not read as that type is its input error.
        (
            "SELECT count(*) OVER (ORDER BY v RANGE BETWEEN 'x' PRECEDING AND 1 FOLLOWING) \
             FROM nm",
            "22P02",
        ),
        (
            "SELECT count(*) OVER (ORDER BY v RANGE BETWEEN NULL PRECEDING AND 1 FOLLOWING) \
             FROM nm",
            "22004",
        ),
        // A ROWS/GROUPS offset is a bigint count; a non-count type is 42804 and a
        // row reference is 42P10.
        (
            "SELECT count(*) OVER (ORDER BY v ROWS INTERVAL '1 day' PRECEDING) FROM fl",
            "42804",
        ),
        (
            "SELECT count(*) OVER (ORDER BY v GROUPS INTERVAL '1 day' PRECEDING) FROM fl",
            "42804",
        ),
        (
            "SELECT count(*) OVER (ORDER BY v ROWS v PRECEDING) FROM fl",
            "42P10",
        ),
    ];
    for (sql, expected) in errors {
        assert!(sqlstate(&client, sql).await == expected, "{sql}");
    }
    // No row consults the offset over an empty partition, so no row rejects it.
    for sql in [
        "SELECT count(*) OVER (ORDER BY v RANGE BETWEEN -1 PRECEDING AND 1 FOLLOWING) FROM e",
        "SELECT count(*) OVER (ORDER BY v \
         RANGE BETWEEN 'NaN'::float8 PRECEDING AND 1 FOLLOWING) FROM e",
    ] {
        assert!(rows(&client, sql).await.is_empty(), "{sql}");
    }
    // Nor does a NULL ordering value, which never reaches in_range.
    assert!(
        rows(
            &client,
            "SELECT count(*) OVER (ORDER BY v RANGE BETWEEN -1 PRECEDING AND 1 FOLLOWING) \
             FROM (SELECT NULL::float8 v) q",
        )
        .await
            == vec!["1"]
    );
}

/// **A frame offset may be a subquery.**
///
/// `ROWS <offset> PRECEDING` takes an arbitrary expression, evaluated once for
/// the whole window rather than per row. The subquery pre-pass walked a window
/// specification's `PARTITION BY` and `ORDER BY` but not its frame, so a
/// subquery there survived as a raw node into the scalar evaluator, which runs
/// none — the statement was refused rather than answered. Both ends of a
/// `BETWEEN` and the named-`WINDOW` form reach the specification by different
/// routes, so each is checked.
#[tokio::test]
async fn a_frame_offset_may_be_a_subquery() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE f (k int4); \
             INSERT INTO f VALUES (1), (2), (3), (4), (5); \
             CREATE TABLE width (n int4); \
             INSERT INTO width VALUES (2)",
        )
        .await
        .expect("fixture");

    // Two rows back plus the current one.
    assert!(
        rows(
            &client,
            "SELECT sum(k) OVER (ORDER BY k ROWS (SELECT n FROM width) PRECEDING) FROM f ORDER BY k",
        )
        .await
            == vec!["1", "3", "6", "9", "12"]
    );

    // Both ends of a BETWEEN, and an offset that is a subquery inside a larger
    // expression rather than the whole of it.
    assert!(
        rows(
            &client,
            "SELECT sum(k) OVER (ORDER BY k
                 ROWS BETWEEN (SELECT n FROM width) PRECEDING
                          AND (SELECT n - 1 FROM width) FOLLOWING) FROM f ORDER BY k",
        )
        .await
            == vec!["3", "6", "10", "14", "12"]
    );
    assert!(
        rows(
            &client,
            "SELECT sum(k) OVER (ORDER BY k
                 ROWS (SELECT n FROM width ORDER BY n LIMIT 1) - 1 PRECEDING) FROM f ORDER BY k",
        )
        .await
            == vec!["1", "3", "5", "7", "9"]
    );

    // The named-WINDOW form reaches the same specification by another route.
    assert!(
        rows(
            &client,
            "SELECT sum(k) OVER win FROM f
               WINDOW win AS (ORDER BY k ROWS (SELECT n FROM width) PRECEDING) ORDER BY k",
        )
        .await
            == vec!["1", "3", "6", "9", "12"]
    );
}
