//! SP29: scalar (row) functions + the `||` operator — end-to-end over the wire.
//! String (length/upper/lower/trim/substr/replace/concat), math (abs/mod),
//! null/conditional (coalesce/nullif/greatest/least), `||`, the aggregate
//! interaction, and the error SQLSTATEs (42883 / 42809 / 42804).

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

/// First column of the first row, as text (via the simple query protocol so the
/// engine's own text encoding is exercised). Panics if there is no row.
async fn scalar(client: &tokio_postgres::Client, sql: &str) -> Option<String> {
    use tokio_postgres::SimpleQueryMessage;
    for m in client.simple_query(sql).await.expect("query") {
        if let SimpleQueryMessage::Row(row) = m {
            return row.get(0).map(std::string::ToString::to_string);
        }
    }
    panic!("no row for `{sql}`");
}

/// The SQLSTATE of a statement expected to error.
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
async fn string_functions() {
    let client = connect(spawn().await).await;
    assert_eq!(
        scalar(&client, "SELECT length('hello')").await.as_deref(),
        Some("5")
    );
    assert_eq!(
        scalar(&client, "SELECT upper('aBc')").await.as_deref(),
        Some("ABC")
    );
    assert_eq!(
        scalar(&client, "SELECT lower('aBc')").await.as_deref(),
        Some("abc")
    );
    assert_eq!(
        scalar(&client, "SELECT btrim('  hi  ')").await.as_deref(),
        Some("hi")
    );
    assert_eq!(
        scalar(&client, "SELECT ltrim('xxhi', 'x')")
            .await
            .as_deref(),
        Some("hi")
    );
    assert_eq!(
        scalar(&client, "SELECT substr('abcdef', 2, 3)")
            .await
            .as_deref(),
        Some("bcd")
    );
    assert_eq!(
        scalar(&client, "SELECT replace('a.b.c', '.', '-')")
            .await
            .as_deref(),
        Some("a-b-c")
    );
    assert_eq!(
        scalar(&client, "SELECT concat('x', NULL, 'y', 1)")
            .await
            .as_deref(),
        Some("xy1")
    );
}

#[tokio::test]
async fn concat_operator_and_math() {
    let client = connect(spawn().await).await;
    assert_eq!(
        scalar(&client, "SELECT 'id=' || 5 || '!'").await.as_deref(),
        Some("id=5!")
    );
    // a NULL operand makes the whole `||` NULL.
    assert_eq!(scalar(&client, "SELECT 'x' || NULL").await, None);
    assert_eq!(
        scalar(&client, "SELECT abs(-7)").await.as_deref(),
        Some("7")
    );
    assert_eq!(
        scalar(&client, "SELECT mod(11, 3)").await.as_deref(),
        Some("2")
    );
}

#[tokio::test]
async fn null_and_conditional_functions() {
    let client = connect(spawn().await).await;
    assert_eq!(
        scalar(&client, "SELECT coalesce(NULL, NULL, 'third')")
            .await
            .as_deref(),
        Some("third")
    );
    assert_eq!(scalar(&client, "SELECT nullif(5, 5)").await, None);
    assert_eq!(
        scalar(&client, "SELECT nullif(5, 6)").await.as_deref(),
        Some("5")
    );
    assert_eq!(
        scalar(&client, "SELECT greatest(3, 7, NULL, 2)")
            .await
            .as_deref(),
        Some("7")
    );
    assert_eq!(
        scalar(&client, "SELECT least('b', 'a', 'c')")
            .await
            .as_deref(),
        Some("a")
    );
}

#[tokio::test]
async fn over_a_table_and_in_where_order() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (id int4, name text)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 'Alice'), (2, 'bob'), (3, 'Carol')")
        .await
        .expect("insert");
    // scalar function in the projection + ORDER BY over a function value.
    let names: Vec<String> = client
        .query("SELECT upper(name) FROM t ORDER BY lower(name)", &[])
        .await
        .expect("query")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    assert_eq!(names, ["ALICE", "BOB", "CAROL"]);
    // scalar function in WHERE.
    let ids: Vec<i32> = client
        .query("SELECT id FROM t WHERE length(name) = 5 ORDER BY id", &[])
        .await
        .expect("query")
        .iter()
        .map(|r| r.get::<_, i32>(0))
        .collect();
    assert_eq!(ids, [1, 3]); // 'Alice', 'Carol'
}

#[tokio::test]
async fn scalar_function_wrapping_an_aggregate() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (g int4, v int4)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1, 10), (1, 30), (2, 5)")
        .await
        .expect("insert");
    // upper over a grouped text, and a scalar function over an aggregate result.
    let rows: Vec<(i32, i32)> = client
        .query(
            "SELECT g, abs(0 - sum(v)) FROM t GROUP BY g ORDER BY g",
            &[],
        )
        .await
        .expect("query")
        .iter()
        .map(|r| {
            (
                r.get::<_, i32>(0),
                i32::try_from(r.get::<_, i64>(1)).expect("sum(v) fits i32"),
            )
        })
        .collect();
    assert_eq!(rows, [(1, 40), (2, 5)]);
    // concat of a grouped column with an aggregate, all on one range.
    let labels: Vec<String> = client
        .query(
            "SELECT g || ':' || count(*) FROM t GROUP BY g ORDER BY g",
            &[],
        )
        .await
        .expect("query")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    assert_eq!(labels, ["1:2", "2:1"]);
}

#[tokio::test]
async fn error_sqlstates() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (n int4, s text)")
        .await
        .expect("create");
    // unknown function / wrong arity / bad argument type -> 42883.
    assert_eq!(err_code(&client, "SELECT frobnicate(1)").await, "42883");
    assert_eq!(err_code(&client, "SELECT length('a', 'b')").await, "42883");
    assert_eq!(err_code(&client, "SELECT upper(1)").await, "42883");
    // `int || int` (neither operand text) -> 42883.
    assert_eq!(err_code(&client, "SELECT 1 || 2").await, "42883");
    // incompatible coalesce types -> 42804.
    assert_eq!(err_code(&client, "SELECT coalesce(1, 'x')").await, "42804");
    // DISTINCT on a scalar function -> 42809.
    assert_eq!(
        err_code(&client, "SELECT upper(DISTINCT s) FROM t").await,
        "42809"
    );
}
