use std::sync::Arc;

use crabka_pgexec::SqlEngine;
use crabka_pgwire::session::SessionConfig;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

async fn connect() -> tokio_postgres::Client {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(SqlEngine::new()),
        Arc::new(SessionConfig::trust()),
    ));
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("crab")
        .dbname("crab")
        .connect(NoTls)
        .await
        .expect("connect");
    tokio::spawn(connection);
    client
}

async fn assert_sqlstate(client: &tokio_postgres::Client, sql: &str, expected: &str) {
    let error = client.simple_query(sql).await.expect_err("expected error");
    assert_eq!(
        error.as_db_error().expect("database error").code().code(),
        expected,
        "{sql}"
    );
}

#[tokio::test]
async fn boolean_comparison_builtins_validate_before_strict_null() {
    let client = connect().await;

    assert_sqlstate(&client, "SELECT 1 WHERE booleq(NULL::bool, 1)", "42883").await;
    assert_sqlstate(&client, "SELECT 1 WHERE boolne(NULL::bool)", "42883").await;

    client
        .batch_execute("CREATE DOMAIN truth_value AS boolean")
        .await
        .expect("create boolean domain");
    let rows = client
        .simple_query("SELECT booleq(TRUE::truth_value, TRUE)")
        .await
        .expect("compare boolean domain");
    let value = rows.iter().find_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0),
        _ => None,
    });
    assert_eq!(value, Some("t"));
}
