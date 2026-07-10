//! SP30: `double precision` (float8) + `AVG` — end-to-end over the wire.
//! Float8 column round-trip and result type (OID 701), float arithmetic and
//! comparison/ordering, `avg`/`sum`/`min`/`max`/`abs` over floats, `GROUP BY` /
//! `DISTINCT` over float keys, text rendering, and the error SQLSTATEs.

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

async fn seed(client: &tokio_postgres::Client) {
    client
        .batch_execute("CREATE TABLE m (g int4, x double precision)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO m VALUES (1, 1.5), (1, 2.5), (2, 2.0), (2, 4.0), (3, NULL)")
        .await
        .expect("insert");
}

/// The first column of the first row as text (simple query protocol → exercises the
/// engine's own float8 text encoding).
async fn text(client: &tokio_postgres::Client, sql: &str) -> Option<String> {
    use tokio_postgres::SimpleQueryMessage;
    for m in client.simple_query(sql).await.expect("query") {
        if let SimpleQueryMessage::Row(row) = m {
            return row.get(0).map(std::string::ToString::to_string);
        }
    }
    panic!("no row for `{sql}`");
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

fn assert_float_eq(actual: f64, expected: f64) {
    assert!((actual - expected).abs() < f64::EPSILON);
}

#[tokio::test]
async fn float8_column_roundtrip_and_result_type() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    let rows = client
        .query("SELECT x FROM m WHERE g = 1 ORDER BY x", &[])
        .await
        .expect("select");
    // The DataRow column is reported as float8 (OID 701).
    assert_eq!(*rows[0].columns()[0].type_(), Type::FLOAT8);
    let got: Vec<f64> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(got, vec![1.5, 2.5]);
    // A whole-number float renders without a trailing `.0`, and NULL stays NULL.
    assert_eq!(
        text(&client, "SELECT x FROM m WHERE g = 2 ORDER BY x").await,
        Some("2".into())
    );
    assert_eq!(text(&client, "SELECT x FROM m WHERE g = 3").await, None);
}

#[tokio::test]
async fn float_arithmetic_promotes_and_divides() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    // float8 + int → float8; float8 / int → real (non-truncating) division.
    let r = client
        .query_one("SELECT x + 1, x / 2 FROM m WHERE g = 2 AND x = 4.0", &[])
        .await
        .expect("arith");
    let (plus, div): (f64, f64) = (r.get(0), r.get(1));
    assert_float_eq(plus, 5.0);
    assert_float_eq(div, 2.0);
    // int / float is real division too (SP32: a bare `2.0` is now numeric, so
    // float8 division is exercised with an explicit `::float8`).
    let r = client
        .query_one("SELECT 3 / 2.0::float8", &[])
        .await
        .expect("div");
    assert_float_eq(r.get::<_, f64>(0), 1.5);
    assert_eq!(*r.columns()[0].type_(), Type::FLOAT8);
}

#[tokio::test]
async fn aggregates_over_float8() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    let r = client
        .query_one(
            "SELECT sum(x), avg(x), min(x), max(x), count(x), count(*) FROM m",
            &[],
        )
        .await
        .expect("agg");
    let (sum, avg, min, max): (f64, f64, f64, f64) = (r.get(0), r.get(1), r.get(2), r.get(3));
    let (cx, cstar): (i64, i64) = (r.get(4), r.get(5));
    assert_float_eq(sum, 10.0);
    assert_float_eq(avg, 2.5);
    assert_float_eq(min, 1.5);
    assert_float_eq(max, 4.0);
    assert_eq!((cx, cstar), (4, 5)); // NULL skipped by count(x), counted by count(*)
    // sum/avg/min are reported as float8.
    assert_eq!(*r.columns()[0].type_(), Type::FLOAT8);
    assert_eq!(*r.columns()[1].type_(), Type::FLOAT8);

    // SP32: avg over a float8 expression stays float8 (avg of int is now numeric —
    // covered by the casts/aggregate numeric tests).
    let r = client
        .query_one("SELECT avg(g::float8) FROM m", &[])
        .await
        .expect("avg float");
    assert_eq!(*r.columns()[0].type_(), Type::FLOAT8);
    assert_float_eq(r.get::<_, f64>(0), 1.8); // (1+1+2+2+3)/5
    // abs over a float8 value.
    assert_eq!(
        text(&client, "SELECT abs(-2.5::float8)").await,
        Some("2.5".into())
    );
}

#[tokio::test]
async fn group_by_and_distinct_over_floats() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    // Per-group sum/avg over a float column; NULL group's sum/avg are NULL, count 0.
    let rows = client
        .query(
            "SELECT g, sum(x), avg(x), count(x) FROM m GROUP BY g ORDER BY g",
            &[],
        )
        .await
        .expect("group by");
    let got: Vec<(i32, Option<f64>, Option<f64>, i64)> = rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect();
    assert_eq!(
        got,
        vec![
            (1, Some(4.0), Some(2.0), 2),
            (2, Some(6.0), Some(3.0), 2),
            (3, None, None, 0),
        ]
    );
    // count(DISTINCT float) over the four distinct non-null values.
    let r = client
        .query_one("SELECT count(DISTINCT x) FROM m", &[])
        .await
        .expect("distinct");
    assert_eq!(r.get::<_, i64>(0), 4);
}

#[tokio::test]
async fn comparison_and_ordering() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    // float comparison in WHERE, then DESC ordering (NULLs filtered out).
    let rows = client
        .query("SELECT x FROM m WHERE x > 2 ORDER BY x DESC", &[])
        .await
        .expect("cmp");
    let got: Vec<f64> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(got, vec![4.0, 2.5]);
    // equality against a float literal.
    let r = client
        .query_one("SELECT g FROM m WHERE x = 2.5", &[])
        .await
        .expect("eq");
    assert_eq!(r.get::<_, i32>(0), 1);
}

#[tokio::test]
async fn error_surface() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    // float division by zero is 22012.
    assert_eq!(
        err_code(&client, "SELECT x / 0 FROM m WHERE g = 1").await,
        "22012"
    );
    // SP32: a bare `1e400` is now an (exact, unbounded) numeric — no overflow.
    // float8 overflow is reached via an explicit cast.
    assert_eq!(err_code(&client, "SELECT '1e400'::float8").await, "22003");
    // an unknown function is 42883.
    assert_eq!(
        err_code(&client, "SELECT frobnicate(x) FROM m").await,
        "42883"
    );
}
