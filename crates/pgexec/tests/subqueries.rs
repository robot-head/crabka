//! SP34: uncorrelated subquery expressions — scalar `(SELECT …)`, `x [NOT] IN
//! (SELECT …)`, `[NOT] EXISTS (…)`, and `x op ANY|SOME|ALL (…)` — end-to-end over
//! the wire (simple query protocol → exercises the engine's own execution + text
//! encoding), plus the 21000 / 42601 error surface.

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

/// All first-column text values of a simple query's row results.
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

async fn seed(client: &tokio_postgres::Client) {
    client
        .simple_query("CREATE TABLE t (id int4, v int4)")
        .await
        .expect("create t");
    client
        .simple_query("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .expect("seed t");
    client
        .simple_query("CREATE TABLE u (k int4)")
        .await
        .expect("create u");
    client
        .simple_query("INSERT INTO u VALUES (1), (3)")
        .await
        .expect("seed u");
}

#[tokio::test]
async fn scalar_subquery_projection_and_where() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    assert_eq!(
        col0(&client, "SELECT (SELECT max(v) FROM t)").await,
        vec![Some("30".into())]
    );
    assert_eq!(
        col0(
            &client,
            "SELECT id FROM t WHERE v > (SELECT avg(v) FROM t) ORDER BY id"
        )
        .await,
        vec![Some("3".into())]
    );
    // Zero rows → NULL.
    assert_eq!(
        col0(&client, "SELECT (SELECT v FROM t WHERE id = 99)").await,
        vec![None]
    );
}

#[tokio::test]
async fn in_not_in_exists_quantified() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    assert_eq!(
        col0(
            &client,
            "SELECT id FROM t WHERE id IN (SELECT k FROM u) ORDER BY id"
        )
        .await,
        vec![Some("1".into()), Some("3".into())]
    );
    assert_eq!(
        col0(
            &client,
            "SELECT id FROM t WHERE id NOT IN (SELECT k FROM u) ORDER BY id"
        )
        .await,
        vec![Some("2".into())]
    );
    assert_eq!(
        col0(&client, "SELECT EXISTS (SELECT 1 FROM u WHERE k = 3)").await,
        vec![Some("t".into())]
    );
    assert_eq!(
        col0(
            &client,
            "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE k = 99) ORDER BY id"
        )
        .await,
        vec![Some("1".into()), Some("2".into()), Some("3".into())]
    );
    // v > ALL (1,3) → all rows (10,20,30 each > 3); SOME synonym for ANY.
    assert_eq!(
        col0(
            &client,
            "SELECT id FROM t WHERE v > ALL (SELECT k FROM u) ORDER BY id"
        )
        .await,
        vec![Some("1".into()), Some("2".into()), Some("3".into())]
    );
    assert_eq!(
        col0(
            &client,
            "SELECT id FROM t WHERE id = SOME (SELECT k FROM u) ORDER BY id"
        )
        .await,
        vec![Some("1".into()), Some("3".into())]
    );
}

#[tokio::test]
async fn error_surface() {
    let client = connect(spawn().await).await;
    seed(&client).await;
    // scalar subquery > 1 row → 21000.
    assert_eq!(err_code(&client, "SELECT (SELECT v FROM t)").await, "21000");
    // scalar subquery > 1 column → 42601.
    assert_eq!(
        err_code(&client, "SELECT (SELECT id, v FROM t WHERE id = 1)").await,
        "42601"
    );
    // IN-subquery > 1 column → 42601.
    assert_eq!(
        err_code(
            &client,
            "SELECT id FROM t WHERE id IN (SELECT id, v FROM t)"
        )
        .await,
        "42601"
    );
}

/// A subquery in FROM may omit its alias, as it has since `PostgreSQL` 16. Several
/// unnamed subqueries can appear in one FROM without colliding, and the columns
/// they expose are usable unqualified.
#[tokio::test]
async fn a_from_subquery_may_omit_its_alias() {
    use assert2::assert;

    let client = connect(spawn().await).await;
    assert!(col0(&client, "SELECT * FROM (SELECT 1 AS x)").await == vec![Some("1".to_string())]);
    assert!(col0(&client, "SELECT count(*) FROM (SELECT 1)").await == vec![Some("1".to_string())]);
    // The exposed column is in scope unqualified.
    assert!(
        col0(&client, "SELECT x FROM (SELECT 1 AS x) WHERE x = 1").await
            == vec![Some("1".to_string())]
    );
    // Two of them in one FROM each get their own name rather than clashing.
    assert!(
        col0(&client, "SELECT x FROM (SELECT 1 AS x), (SELECT 2 AS y)").await
            == vec![Some("1".to_string())]
    );
    // An alias, when given, still wins.
    assert!(
        col0(&client, "SELECT q.x FROM (SELECT 1 AS x) q").await == vec![Some("1".to_string())]
    );
}
