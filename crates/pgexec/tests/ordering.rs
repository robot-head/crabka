//! SP39: plain SELECT ORDER BY `PostgreSQL` parity, end-to-end over the wire.

use std::sync::Arc;

use crabka_pgexec::SqlEngine;
use crabka_pgwire::session::SessionConfig;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

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

async fn col0(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    client
        .query(sql, &[])
        .await
        .expect("query")
        .iter()
        .map(|row| row.get(0))
        .collect()
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
async fn plain_select_order_by_position_alias_and_source_fallback() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (a int4, b int4, name text)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1,20,'a'),(2,10,'b'),(3,30,'c')")
        .await
        .expect("insert");

    assert_eq!(
        col0(&client, "SELECT name FROM t ORDER BY 1 DESC").await,
        vec!["c".to_string(), "b".to_string(), "a".to_string()]
    );

    let rows = client
        .query("SELECT a AS b FROM t ORDER BY b", &[])
        .await
        .expect("alias order");
    let got: Vec<i32> = rows.iter().map(|row| row.get(0)).collect();
    assert_eq!(got, vec![1, 2, 3]);

    let rows = client
        .query("SELECT a AS b FROM t ORDER BY t.b", &[])
        .await
        .expect("source order");
    let got: Vec<i32> = rows.iter().map(|row| row.get(0)).collect();
    assert_eq!(got, vec![2, 1, 3]);

    let rows = client
        .query("SELECT a AS x, a AS x FROM t ORDER BY x", &[])
        .await
        .expect("identical duplicate output labels");
    let got: Vec<(i32, i32)> = rows.iter().map(|row| (row.get(0), row.get(1))).collect();
    assert_eq!(got, vec![(1, 1), (2, 2), (3, 3)]);
}

#[tokio::test]
async fn distinct_and_aggregate_order_by_parity() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (a int4, b int4)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1,20),(1,10),(2,30),(3,5)")
        .await
        .expect("insert");

    let rows = client
        .query("SELECT DISTINCT a AS x FROM t ORDER BY x DESC", &[])
        .await
        .expect("distinct order");
    let got: Vec<i32> = rows.iter().map(|row| row.get(0)).collect();
    assert_eq!(got, vec![3, 2, 1]);

    let rows = client
        .query("SELECT DISTINCT a FROM t ORDER BY t.a DESC", &[])
        .await
        .expect("distinct qualified output order");
    let got: Vec<i32> = rows.iter().map(|row| row.get(0)).collect();
    assert_eq!(got, vec![3, 2, 1]);

    let rows = client
        .query(
            "SELECT a, count(*) AS c FROM t GROUP BY a ORDER BY c DESC, a",
            &[],
        )
        .await
        .expect("aggregate alias order");
    let got: Vec<(i32, i64)> = rows.iter().map(|row| (row.get(0), row.get(1))).collect();
    assert_eq!(got, vec![(1, 2), (2, 1), (3, 1)]);

    let rows = client
        .query(
            "SELECT a, count(*) AS c FROM t GROUP BY a ORDER BY 2 DESC, 1",
            &[],
        )
        .await
        .expect("aggregate positional order");
    let got: Vec<(i32, i64)> = rows.iter().map(|row| (row.get(0), row.get(1))).collect();
    assert_eq!(got, vec![(1, 2), (2, 1), (3, 1)]);
}

#[tokio::test]
async fn order_by_pg_error_surface() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (a int4, b int4)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1,20),(2,10)")
        .await
        .expect("insert");

    assert_eq!(
        err_code(&client, "SELECT a FROM t ORDER BY 0").await,
        "42P10"
    );
    assert_eq!(
        err_code(
            &client,
            "SELECT a FROM t ORDER BY 999999999999999999999999999"
        )
        .await,
        "42601"
    );
    assert_eq!(
        err_code(&client, "SELECT a FROM t ORDER BY 2147483648").await,
        "42601"
    );
    assert_eq!(
        err_code(&client, "SELECT a AS x, b AS x FROM t ORDER BY x").await,
        "42702"
    );
    assert_eq!(
        err_code(&client, "SELECT DISTINCT a FROM t ORDER BY b").await,
        "42P10"
    );
}
