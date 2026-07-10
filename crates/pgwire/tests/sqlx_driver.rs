use std::sync::Arc;

use crabka_pgwire::{session::SessionConfig, stub::StubEngine};
use sqlx::Connection;
use tokio::net::TcpListener;

async fn connect_sqlx() -> sqlx::postgres::PgConnection {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(SessionConfig::trust()),
    ));

    let url = format!("postgres://crab@127.0.0.1:{port}/crab");
    sqlx::postgres::PgConnection::connect(&url)
        .await
        .expect("sqlx connect")
}

#[tokio::test]
async fn sqlx_connects_and_queries() {
    let mut conn = connect_sqlx().await;
    let row: (i32,) = sqlx::query_as("SELECT 1")
        .fetch_one(&mut conn)
        .await
        .expect("query");
    assert_eq!(row.0, 1);
}

#[tokio::test]
async fn sqlx_binds_text_parameter() {
    let mut conn = connect_sqlx().await;
    let row: (String,) = sqlx::query_as("SELECT $1")
        .bind("crab")
        .fetch_one(&mut conn)
        .await
        .expect("query");
    assert_eq!(row.0, "crab");
}

#[tokio::test]
async fn sqlx_binds_int4_parameter() {
    let mut conn = connect_sqlx().await;
    let row: (i32,) = sqlx::query_as("SELECT $1")
        .bind(42_i32)
        .fetch_one(&mut conn)
        .await
        .expect("query");
    assert_eq!(row.0, 42);
}

#[tokio::test]
async fn sqlx_preserves_null_text_parameter() {
    let mut conn = connect_sqlx().await;
    let value: Option<&str> = None;
    let row: (Option<String>,) = sqlx::query_as("SELECT $1")
        .bind(value)
        .fetch_one(&mut conn)
        .await
        .expect("query");
    assert_eq!(row.0, None);
}
