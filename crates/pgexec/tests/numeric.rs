//! SP32: arbitrary-precision `numeric` / `decimal` — end-to-end over the wire.
//! Bare decimal literals are numeric (scale-faithful), exact arithmetic with
//! `PostgreSQL` scale rules (incl. division/AVG display scale), `numeric(p,s)`
//! rounding + overflow, the cast matrix, comparison/grouping, and the result type
//! OID 1700. Values are read in TEXT mode (simple query) — the engine's own
//! `numeric_out` — so the assertions are exact decimal strings.

use std::sync::Arc;

use crabka_pgexec::SqlEngine;
use crabka_pgwire::session::SessionConfig;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, types::Type};

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

async fn connect(port: u16) -> tokio_postgres::Client {
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

/// All first-column values (text format) for a query, in row order.
async fn col0(client: &tokio_postgres::Client, sql: &str) -> Vec<Option<String>> {
    use tokio_postgres::SimpleQueryMessage;
    let mut out = Vec::new();
    for m in client.simple_query(sql).await.expect("query") {
        if let SimpleQueryMessage::Row(row) = m {
            out.push(row.get(0).map(std::string::ToString::to_string));
        }
    }
    out
}

/// First column of the first row as text.
async fn one(client: &tokio_postgres::Client, sql: &str) -> Option<String> {
    col0(client, sql).await.into_iter().next().expect("a row")
}

async fn err_code(client: &tokio_postgres::Client, sql: &str) -> String {
    client
        .simple_query(sql)
        .await
        .expect_err("expected error")
        .as_db_error()
        .expect("db error")
        .code()
        .code()
        .to_string()
}

#[tokio::test]
async fn numeric_literal_scale_and_result_type() {
    let client = connect(spawn().await).await;
    // Bare decimal literals are numeric and scale-faithful.
    assert_eq!(one(&client, "SELECT 1.50").await, Some("1.50".into()));
    assert_eq!(one(&client, "SELECT 1e3").await, Some("1000".into()));
    assert_eq!(one(&client, "SELECT 0.0015").await, Some("0.0015".into()));
    // The result type is numeric (OID 1700). (Read the RowDescription only — the
    // binary value decoder for numeric is not wired in this test client.)
    let rows = client.query("SELECT 1.5", &[]).await.expect("q");
    assert_eq!(*rows[0].columns()[0].type_(), Type::NUMERIC);
}

#[tokio::test]
async fn exact_arithmetic_and_division_scale() {
    let client = connect(spawn().await).await;
    // +, -, * carry PostgreSQL's scale rules exactly.
    assert_eq!(one(&client, "SELECT 1.50 + 1.5").await, Some("3.00".into()));
    assert_eq!(one(&client, "SELECT 1.5 * 1.5").await, Some("2.25".into()));
    assert_eq!(one(&client, "SELECT 2.5 - 1.25").await, Some("1.25".into()));
    // int ⊕ numeric → numeric.
    assert_eq!(one(&client, "SELECT 2 * 1.5").await, Some("3.0".into()));
    // division uses select_div_scale (16 significant digits here).
    assert_eq!(
        one(&client, "SELECT 10 / 3.0").await,
        Some("3.3333333333333333".into())
    );
    assert_eq!(
        one(&client, "SELECT 1.0 / 3").await,
        Some("0.33333333333333333333".into())
    );
    // numeric division by zero is 22012.
    assert_eq!(err_code(&client, "SELECT 1.5 / 0").await, "22012");
}

#[tokio::test]
async fn numeric_column_roundtrip_typmod_and_overflow() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE acct (id int4, bal numeric(10,2), ratio numeric)")
        .await
        .expect("create");
    // numeric(10,2) rounds to scale 2 on store; unconstrained numeric keeps its scale.
    client
        .batch_execute("INSERT INTO acct VALUES (1, 10, 1.5), (2, 2.5, 0.333), (3, 9.999, 2)")
        .await
        .expect("insert");
    assert_eq!(
        col0(&client, "SELECT bal FROM acct ORDER BY id").await,
        vec![
            Some("10.00".into()),
            Some("2.50".into()),
            Some("10.00".into())
        ] // 9.999 → 10.00
    );
    assert_eq!(
        col0(&client, "SELECT ratio FROM acct ORDER BY id").await,
        vec![Some("1.5".into()), Some("0.333".into()), Some("2".into())]
    );
    // The numeric column reports OID 1700.
    let rows = client.query("SELECT bal FROM acct", &[]).await.expect("q");
    assert_eq!(*rows[0].columns()[0].type_(), Type::NUMERIC);
    // A value exceeding numeric(10,2)'s precision is 22003 on store.
    assert_eq!(
        err_code(&client, "INSERT INTO acct VALUES (4, 99999999.999, 1)").await,
        "22003"
    );
}

#[tokio::test]
async fn aggregates_over_numeric() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (g int4, v numeric)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 1.5), (1, 2.25), (2, 3.0), (2, NULL)")
        .await
        .expect("insert");
    // sum keeps the max input scale; min/max preserve value.
    assert_eq!(
        one(&client, "SELECT sum(v) FROM t").await,
        Some("6.75".into())
    );
    assert_eq!(
        one(&client, "SELECT min(v) FROM t").await,
        Some("1.5".into())
    );
    assert_eq!(
        one(&client, "SELECT max(v) FROM t").await,
        Some("3.0".into())
    );
    // avg uses the division display scale; an all-null group's avg/sum is NULL.
    assert_eq!(
        col0(&client, "SELECT avg(v) FROM t GROUP BY g ORDER BY g").await,
        vec![
            Some("1.8750000000000000".into()),
            Some("3.0000000000000000".into())
        ]
    );
    // avg over integers is now numeric (exact), not float8.
    assert_eq!(
        one(&client, "SELECT avg(g) FROM t").await,
        Some("1.5000000000000000".into())
    );
}

#[tokio::test]
async fn cast_matrix_and_grouping() {
    let client = connect(spawn().await).await;
    // text ↔ numeric, int → numeric, numeric → int (half-away-from-zero).
    assert_eq!(
        one(&client, "SELECT '12.34'::numeric").await,
        Some("12.34".into())
    );
    assert_eq!(one(&client, "SELECT 5::numeric").await, Some("5".into()));
    assert_eq!(one(&client, "SELECT 2.5::int4").await, Some("3".into())); // not 2
    assert_eq!(one(&client, "SELECT (-2.5)::int4").await, Some("-3".into()));
    // numeric ↔ float8, and a typmod-bearing cast target.
    assert_eq!(
        one(&client, "SELECT (0.1::float8)::numeric").await,
        Some("0.1".into())
    );
    assert_eq!(
        one(&client, "SELECT 1.236::numeric(5,2)").await,
        Some("1.24".into())
    );
    // numeric → bool has no cast (42846).
    assert_eq!(err_code(&client, "SELECT 1.5::bool").await, "42846");
    // bad text → numeric is 22P02.
    assert_eq!(err_code(&client, "SELECT 'abc'::numeric").await, "22P02");
    // value-equality grouping: 1.0 and 1.00 are one group.
    client
        .batch_execute("CREATE TABLE g (v numeric)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO g VALUES (1.0), (1.00), (2.5)")
        .await
        .expect("insert");
    assert_eq!(
        one(&client, "SELECT count(DISTINCT v) FROM g").await,
        Some("2".into())
    );
}
