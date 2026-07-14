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

async fn connect_new() -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(spawn().await)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

async fn rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<Option<String>>> {
    use tokio_postgres::SimpleQueryMessage;
    let mut out = Vec::new();
    for m in client.simple_query(sql).await.expect("query") {
        if let SimpleQueryMessage::Row(row) = m {
            out.push(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_string))
                    .collect(),
            );
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

#[tokio::test]
async fn top_level_queries_keep_existing_behavior() {
    let c = connect_new().await;
    assert_eq!(rows(&c, "SELECT 1").await, vec![vec![Some("1".into())]]);
    assert_eq!(
        rows(&c, "VALUES (2), (1) ORDER BY 1").await,
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
    assert_eq!(
        rows(&c, "SELECT 1 UNION SELECT 2 ORDER BY 1").await,
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
}

#[tokio::test]
async fn describe_top_level_query_exprs() {
    let c = connect_new().await;

    let stmt = c.prepare("SELECT 1 AS one").await.expect("describe select");
    assert_eq!(stmt.columns()[0].name(), "one");

    let stmt = c.prepare("VALUES (1, 'a')").await.expect("describe values");
    assert_eq!(stmt.columns()[0].name(), "column1");
    assert_eq!(stmt.columns()[1].name(), "column2");

    let stmt = c
        .prepare("SELECT 1 AS x UNION SELECT 2")
        .await
        .expect("describe set op");
    assert_eq!(stmt.columns()[0].name(), "x");
}

#[tokio::test]
async fn locking_select_still_uses_locking_path() {
    let c = connect_new().await;
    c.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    c.simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    assert_eq!(
        rows(&c, "SELECT id FROM t FOR UPDATE").await,
        vec![vec![Some("1".into())]]
    );
    assert_eq!(err_code(&c, "VALUES (1) FOR UPDATE").await, "42601");
}

#[tokio::test]
async fn expression_subqueries_accept_values_and_setops() {
    let c = connect_new().await;
    c.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    c.simple_query("INSERT INTO t VALUES (1), (2), (3)")
        .await
        .expect("insert");

    assert_eq!(
        rows(&c, "SELECT (VALUES (2) UNION SELECT 1 ORDER BY 1 LIMIT 1)").await,
        vec![vec![Some("1".into())]]
    );
    assert_eq!(
        rows(&c, "SELECT (VALUES (2))").await,
        vec![vec![Some("2".into())]]
    );
    assert_eq!(
        rows(
            &c,
            "SELECT id FROM t WHERE id IN (VALUES (1), (3)) ORDER BY id",
        )
        .await,
        vec![vec![Some("1".into())], vec![Some("3".into())]]
    );
    assert_eq!(
        rows(&c, "SELECT EXISTS (SELECT 1 EXCEPT SELECT 1)").await,
        vec![vec![Some("f".into())]]
    );
    assert_eq!(
        rows(&c, "SELECT 3 > ALL (VALUES (1), (2))").await,
        vec![vec![Some("t".into())]]
    );
    assert_eq!(
        rows(&c, "SELECT 2 = ANY (SELECT 1 UNION SELECT 2)").await,
        vec![vec![Some("t".into())]]
    );
}

#[tokio::test]
async fn derived_tables_accept_setops_and_tailed_values() {
    let c = connect_new().await;

    assert_eq!(
        rows(
            &c,
            "SELECT d.x FROM (VALUES (2), (3) UNION SELECT 1) AS d(x) ORDER BY d.x",
        )
        .await,
        vec![
            vec![Some("1".into())],
            vec![Some("2".into())],
            vec![Some("3".into())],
        ]
    );

    assert_eq!(
        rows(
            &c,
            "SELECT v.x FROM (VALUES (3), (1), (2) ORDER BY 1 DESC LIMIT 2) AS v(x) ORDER BY v.x",
        )
        .await,
        vec![vec![Some("2".into())], vec![Some("3".into())]]
    );
}

#[tokio::test]
async fn derived_query_expr_describe_uses_alias_columns() {
    let c = connect_new().await;

    let stmt = c
        .prepare(
            "SELECT d.id, d.label \
             FROM (VALUES (2, 'b') UNION SELECT 1, 'a') AS d(id, label) \
             ORDER BY d.id",
        )
        .await
        .expect("describe derived query expr");

    let names: Vec<_> = stmt
        .columns()
        .iter()
        .map(tokio_postgres::Column::name)
        .collect();
    assert_eq!(names, vec!["id", "label"]);

    assert_eq!(
        rows(
            &c,
            "SELECT d.id, d.label \
             FROM (VALUES (2, 'b') UNION SELECT 1, 'a') AS d(id, label) \
             ORDER BY d.id",
        )
        .await,
        vec![
            vec![Some("1".into()), Some("a".into())],
            vec![Some("2".into()), Some("b".into())],
        ]
    );
}

#[tokio::test]
async fn non_select_query_tails_resolve_subqueries() {
    let c = connect_new().await;

    assert_eq!(
        rows(&c, "VALUES (2), (1) ORDER BY (SELECT 1), 1").await,
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
    assert_eq!(
        rows(
            &c,
            "SELECT v.x FROM (VALUES (2), (1) ORDER BY (SELECT 1), 1) AS v(x)",
        )
        .await,
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
    assert_eq!(
        rows(&c, "SELECT 2 UNION SELECT 1 ORDER BY (SELECT 1), 1").await,
        vec![vec![Some("1".into())], vec![Some("2".into())]]
    );
}

#[tokio::test]
async fn expression_subquery_error_surface_is_preserved() {
    let c = connect_new().await;
    assert_eq!(err_code(&c, "SELECT (VALUES (1), (2))").await, "21000");
    assert_eq!(err_code(&c, "SELECT (VALUES (1, 2))").await, "42601");
    assert_eq!(err_code(&c, "SELECT 1 IN (VALUES (1, 2))").await, "42601");
}
