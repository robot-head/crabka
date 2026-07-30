//! SP39: standalone VALUES, VALUES-derived tables, and VALUES set-op leaves over
//! the `PostgreSQL` wire path.

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

async fn connect_new() -> tokio_postgres::Client {
    connect(spawn().await).await
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
async fn standalone_values_orders_limits_and_names_columns() {
    let c = connect_new().await;
    let stmt = c
        .prepare("VALUES (3, 'c'), (1, 'a'), (2, 'b') ORDER BY 1 DESC LIMIT 2 OFFSET 1")
        .await
        .expect("describe values");

    let names: Vec<_> = stmt
        .columns()
        .iter()
        .map(tokio_postgres::Column::name)
        .collect();
    assert_eq!(names, vec!["column1", "column2"]);

    assert_eq!(
        rows(
            &c,
            "VALUES (3, 'c'), (1, 'a'), (2, 'b') ORDER BY 1 DESC LIMIT 2 OFFSET 1"
        )
        .await,
        vec![
            vec![Some("2".into()), Some("b".into())],
            vec![Some("1".into()), Some("a".into())],
        ]
    );
    assert_eq!(
        err_code(&c, "VALUES (1) ORDER BY 2").await,
        "42P10",
        "positional ORDER BY outside the VALUES output is invalid_column_reference"
    );
}

#[tokio::test]
async fn values_row_arity_error_is_42601_and_session_survives() {
    let c = connect_new().await;
    assert_eq!(err_code(&c, "VALUES (1), (2, 3)").await, "42601");
    assert_eq!(
        rows(&c, "VALUES (42)").await,
        vec![vec![Some("42".into())]],
        "autocommit VALUES error must not poison the session"
    );
}

#[tokio::test]
async fn values_derived_table_uses_alias_column_names() {
    let c = connect_new().await;
    let stmt = c
        .prepare(
            "SELECT v.id, v.name FROM (VALUES (2, 'b'), (1, 'a')) AS v(id, name) ORDER BY v.id",
        )
        .await
        .expect("describe derived values");

    let names: Vec<_> = stmt
        .columns()
        .iter()
        .map(tokio_postgres::Column::name)
        .collect();
    assert_eq!(names, vec!["id", "name"]);

    assert_eq!(
        rows(
            &c,
            "SELECT v.id, v.name FROM (VALUES (2, 'b'), (1, 'a')) AS v(id, name) ORDER BY v.id"
        )
        .await,
        vec![
            vec![Some("1".into()), Some("a".into())],
            vec![Some("2".into()), Some("b".into())],
        ]
    );
}

/// `PostgreSQL` renames a *prefix* when the alias list is shorter than the
/// item's column list, and rejects only a list that names more columns than
/// exist.
#[tokio::test]
async fn values_derived_column_alias_list_may_be_shorter_but_not_longer() {
    let c = connect_new().await;
    assert_eq!(
        rows(&c, "SELECT * FROM (VALUES (1, 2)) AS v(one)").await,
        vec![vec![Some("1".into()), Some("2".into())]]
    );
    assert_eq!(
        err_code(&c, "SELECT * FROM (VALUES (1, 2)) AS v(a, b, c)").await,
        "42P10"
    );
}

#[tokio::test]
async fn derived_select_optional_column_aliases_still_work() {
    let c = connect_new().await;
    assert_eq!(
        rows(&c, "SELECT d.n FROM (SELECT 1 AS x) AS d(n)").await,
        vec![vec![Some("1".into())]]
    );
}

#[tokio::test]
async fn derived_select_column_alias_list_may_be_shorter_but_not_longer() {
    let c = connect_new().await;
    assert_eq!(
        rows(&c, "SELECT * FROM (SELECT 1, 2) AS d(one)").await,
        vec![vec![Some("1".into()), Some("2".into())]]
    );
    assert_eq!(
        err_code(&c, "SELECT * FROM (SELECT 1, 2) AS d(a, b, c)").await,
        "42P10"
    );
}

#[tokio::test]
async fn values_derived_table_is_not_correlated() {
    let c = connect_new().await;
    c.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    c.simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");

    assert_eq!(
        err_code(&c, "SELECT * FROM t, (VALUES (t.id)) AS v(x)").await,
        "42P01"
    );
}

#[tokio::test]
async fn values_can_participate_in_set_operations() {
    let c = connect_new().await;
    c.simple_query("CREATE TABLE t (a int4)")
        .await
        .expect("create");
    c.simple_query("INSERT INTO t VALUES (2), (3)")
        .await
        .expect("insert");

    assert_eq!(
        rows(&c, "VALUES (1), (2) UNION SELECT a FROM t ORDER BY 1").await,
        vec![
            vec![Some("1".into())],
            vec![Some("2".into())],
            vec![Some("3".into())],
        ]
    );
}

#[tokio::test]
async fn values_set_ops_share_unknown_resolution() {
    let c = connect_new().await;

    assert_eq!(
        rows(&c, "VALUES ('1'), (2) UNION SELECT 3 ORDER BY 1").await,
        vec![
            vec![Some("1".into())],
            vec![Some("2".into())],
            vec![Some("3".into())],
        ]
    );
    assert_eq!(
        rows(&c, "VALUES (NULL), (1) UNION SELECT 2 ORDER BY 1").await,
        vec![vec![Some("1".into())], vec![Some("2".into())], vec![None]]
    );
    assert_eq!(
        rows(&c, "VALUES ('a') UNION SELECT 'b' ORDER BY 1").await,
        vec![vec![Some("a".into())], vec![Some("b".into())]]
    );
    assert_eq!(
        err_code(&c, "VALUES (NULL), ('5') UNION SELECT 2 ORDER BY 1").await,
        "42804"
    );
    assert_eq!(
        rows(&c, "VALUES (NULL), ('5') UNION SELECT 2::text ORDER BY 1").await,
        vec![vec![Some("2".into())], vec![Some("5".into())], vec![None]]
    );
    assert_eq!(
        err_code(&c, "VALUES ('1') UNION SELECT 2 ORDER BY 1").await,
        "42804"
    );
    assert_eq!(
        err_code(&c, "VALUES ('x') UNION SELECT 3").await,
        "42804",
        "an all-unknown VALUES body resolves to text before set-op matching"
    );
}
