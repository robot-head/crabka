//! An unadorned string literal is `unknown`, not `text`: in an assignment
//! context it adopts the target's type and is parsed by that type's input
//! function. Covers the three contexts that assign one — `UPDATE … SET`,
//! `INSERT … VALUES`, and a partition bound — and the boundary that makes the
//! rule about *literals*: a genuine `text` expression still needs a cast.

use std::sync::Arc;

use assert2::assert;
use crabka_pgexec::SqlEngine;
use crabka_pgwire::session::SessionConfig;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage};

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

async fn run(client: &tokio_postgres::Client, sql: &str) {
    client.simple_query(sql).await.expect(sql);
}

/// The first row's columns as text, over the simple query protocol.
async fn row(client: &tokio_postgres::Client, sql: &str) -> Vec<Option<String>> {
    for message in client.simple_query(sql).await.expect(sql) {
        if let SimpleQueryMessage::Row(row) = message {
            return (0..row.len())
                .map(|i| row.get(i).map(str::to_owned))
                .collect();
        }
    }
    panic!("no row for `{sql}`");
}

async fn err_code(client: &tokio_postgres::Client, sql: &str) -> String {
    client
        .simple_query(sql)
        .await
        .expect_err(sql)
        .as_db_error()
        .expect("db error")
        .code()
        .code()
        .to_string()
}

/// `UPDATE … SET col = '…'` resolves the literal through the column's input
/// function, for every column type — the `text` default would have made each of
/// these 42804.
#[tokio::test]
async fn update_set_resolves_an_unknown_literal_to_the_column_type() {
    let client = connect(spawn().await).await;
    run(
        &client,
        "CREATE TABLE g (r int4range, n int4, b bool, ts timestamp, d date, \
         j jsonb, a int4[], u uuid, iv interval)",
    )
    .await;
    run(&client, "INSERT INTO g (n) VALUES (0)").await;
    run(
        &client,
        "UPDATE g SET r = '[11,12)', n = '5', b = 'yes', ts = '2020-01-02 03:04:05', \
         d = '2020-01-02', j = '{\"a\":1}', a = '{1,2}', \
         u = 'A1B2C3D4-0000-0000-0000-000000000000', iv = '1 day'",
    )
    .await;
    assert!(
        row(&client, "SELECT * FROM g").await
            == vec![
                Some("[11,12)".to_owned()),
                Some("5".to_owned()),
                Some("t".to_owned()),
                Some("2020-01-02 03:04:05".to_owned()),
                Some("2020-01-02".to_owned()),
                Some("{\"a\": 1}".to_owned()),
                Some("{1,2}".to_owned()),
                Some("a1b2c3d4-0000-0000-0000-000000000000".to_owned()),
                Some("1 day".to_owned()),
            ]
    );
}

/// Resolving the literal is an *assignment*, so the target's own rules apply:
/// an over-long `varchar(n)` is 22001 (an explicit cast would have truncated),
/// and an unparseable literal is the input function's 22P02.
#[tokio::test]
async fn resolution_reports_the_target_type_s_own_errors() {
    let client = connect(spawn().await).await;
    run(
        &client,
        "CREATE TABLE g (r int4range, n int4, v varchar(3), s text)",
    )
    .await;
    run(&client, "INSERT INTO g (n, s) VALUES (0, '5')").await;
    for (sql, code) in [
        ("UPDATE g SET n = 'abc'", "22P02"),
        ("UPDATE g SET r = 'nope'", "22P02"),
        ("UPDATE g SET v = 'abcd'", "22001"),
        // The rule is about literals: a `text` *expression* keeps its type and
        // still needs an explicit cast into a non-string column.
        ("UPDATE g SET n = s", "42804"),
        ("UPDATE g SET r = s", "42804"),
        // …including a literal that has been given the `text` type outright.
        ("UPDATE g SET n = '5'::text", "42804"),
    ] {
        assert!(err_code(&client, sql).await == code, "{sql}");
    }
}

/// A list or range partition bound is an assignment to the key column, so the
/// bound literal resolves the same way — and the resulting bound is the one row
/// routing then matches against.
#[tokio::test]
async fn partition_bounds_resolve_an_unknown_literal_to_the_key_type() {
    let client = connect(spawn().await).await;
    run(
        &client,
        "CREATE TABLE pt (id int4range, name text) PARTITION BY LIST (id)",
    )
    .await;
    run(
        &client,
        "CREATE TABLE ptp1 PARTITION OF pt FOR VALUES IN ('[1,2)')",
    )
    .await;
    run(&client, "INSERT INTO pt VALUES ('[1,2)', 'a')").await;
    assert!(
        row(&client, "SELECT * FROM ptp1").await
            == vec![Some("[1,2)".to_owned()), Some("a".to_owned())]
    );

    run(
        &client,
        "CREATE TABLE rp (id int4range) PARTITION BY RANGE (id)",
    )
    .await;
    run(
        &client,
        "CREATE TABLE rp1 PARTITION OF rp FOR VALUES FROM ('[1,2)') TO ('[9,10)')",
    )
    .await;
    run(&client, "INSERT INTO rp VALUES ('[3,4)')").await;
    assert!(row(&client, "SELECT * FROM rp1").await == vec![Some("[3,4)".to_owned())]);
}

/// `INSERT … VALUES` and a column `DEFAULT` resolve their literals through the
/// same seam, so the three contexts agree on what `'…'` means. (A `DEFAULT`
/// carrying a range is a separate, still-open gap in the catalog serializer, so
/// the default here is an `int4`/`bool` pair.)
#[tokio::test]
async fn insert_and_default_resolve_an_unknown_literal_to_the_column_type() {
    let client = connect(spawn().await).await;
    run(
        &client,
        "CREATE TABLE g (r int4range, n int4 DEFAULT '9', b bool DEFAULT 'yes')",
    )
    .await;
    run(&client, "INSERT INTO g (r) VALUES ('[7,8)')").await;
    run(&client, "INSERT INTO g VALUES ('[11,12)', '5', 'no')").await;
    assert!(
        row(&client, "SELECT * FROM g ORDER BY n").await
            == vec![
                Some("[11,12)".to_owned()),
                Some("5".to_owned()),
                Some("f".to_owned()),
            ]
    );
    assert!(
        row(&client, "SELECT * FROM g ORDER BY n DESC").await
            == vec![
                Some("[7,8)".to_owned()),
                Some("9".to_owned()),
                Some("t".to_owned()),
            ]
    );
}
