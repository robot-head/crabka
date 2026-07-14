//! Recursion-depth `DoS` guard, end-to-end over the wire.
//!
//! A deeply-nested query used to overflow the parser/evaluator stack and ABORT
//! the whole server process (dropping every connection). With the depth guard it
//! must instead return SQLSTATE `54001` (`statement_too_complex` / "stack depth
//! limit exceeded"), and — crucially — the SERVER MUST STAY ALIVE: a follow-up
//! query on the SAME connection still succeeds. That follow-up is the key
//! DoS-fixed assertion (a crashed server would have dropped the connection).

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

/// The SQLSTATE of a failed simple query (panics if the query unexpectedly
/// succeeded or the error was not a DB error).
fn sqlstate(err: &tokio_postgres::Error) -> String {
    err.as_db_error()
        .expect("a DB error (not a transport/crash error)")
        .code()
        .code()
        .to_string()
}

/// After a too-deep query is rejected, the connection must still work — proving
/// the server did NOT crash (a stack overflow would have aborted the process and
/// dropped the connection, so this follow-up would fail at the transport level).
async fn assert_connection_alive(client: &tokio_postgres::Client) {
    let row = client
        .query_one("SELECT 1 + 1", &[])
        .await
        .expect("connection must still be alive after a 54001 (server did not crash)");
    assert_eq!(row.get::<_, i32>(0), 2);
}

/// Mode 1 — deep PARSE recursion (nested parens). Must be a clean 54001, and the
/// server must survive (the follow-up query succeeds on the same connection).
#[tokio::test]
async fn deeply_nested_parens_return_54001_and_server_survives() {
    let client = connect(spawn().await).await;

    let n = 5000;
    let sql = format!("SELECT {}1{}", "(".repeat(n), ")".repeat(n));
    let err = client
        .simple_query(&sql)
        .await
        .expect_err("a 5000-deep paren nest must be rejected, not crash the server");
    assert_eq!(sqlstate(&err), "54001");

    assert_connection_alive(&client).await;
}

/// Mode 2 — a deep AST tree from a flat left-associative chain (`1+1+1+…`). The
/// parser caps the Pratt loop so the over-deep tree is never built (which would
/// otherwise overflow eval AND recursive `Box` `Drop`). Clean 54001, server alive.
#[tokio::test]
async fn long_left_assoc_chain_returns_54001_and_server_survives() {
    let client = connect(spawn().await).await;

    let n = 5000;
    let sql = format!("SELECT {}1", "1+".repeat(n));
    let err = client
        .simple_query(&sql)
        .await
        .expect_err("a 5000-long additive chain must be rejected, not crash the server");
    assert_eq!(sqlstate(&err), "54001");

    assert_connection_alive(&client).await;
}

/// Mode 1 — deeply nested scalar subqueries. Clean 54001, server alive.
#[tokio::test]
async fn deeply_nested_subqueries_return_54001_and_server_survives() {
    let client = connect(spawn().await).await;

    let n = 3000;
    let sql = format!("SELECT {}1{}", "(SELECT ".repeat(n), ")".repeat(n));
    let err = client
        .simple_query(&sql)
        .await
        .expect_err("deeply nested subqueries must be rejected, not crash the server");
    assert_eq!(sqlstate(&err), "54001");

    assert_connection_alive(&client).await;
}

/// Mode 2 (set ops) — a flat `… UNION ALL …` chain builds a deep left-nested
/// `SetExpr`; the `set_expr` loop cap rejects it (54001) before the over-deep tree
/// is built, and the server survives.
#[tokio::test]
async fn long_union_chain_returns_54001_and_server_survives() {
    let client = connect(spawn().await).await;

    let n = 5000;
    let sql = format!("SELECT 1{}", " UNION ALL SELECT 1".repeat(n));
    let err = client
        .simple_query(&sql)
        .await
        .expect_err("a 5000-long UNION chain must be rejected, not crash the server");
    assert_eq!(sqlstate(&err), "54001");

    assert_connection_alive(&client).await;
}

/// A modest, realistic nesting depth still works — the guard does not reject
/// ordinary queries.
#[tokio::test]
async fn modest_nesting_still_works() {
    let client = connect(spawn().await).await;
    let sql = format!("SELECT {}7{}", "(".repeat(20), ")".repeat(20));
    let row = client
        .query_one(&sql, &[])
        .await
        .expect("modest nesting works");
    assert_eq!(row.get::<_, i32>(0), 7);
}
