//! SP38: `to_char`, `to_timestamp`, `to_date`, make_* and justify_*, end to end
//! over the wire.
//!
//! These tests exercise the SP38 formatting and construction functions through
//! the full pgwire to executor stack. Values are read in TEXT mode, through the
//! simple query protocol, so the assertions exercise the engine's own text
//! encodings directly. Result OIDs are read from the binary-query
//! `RowDescription`.

use std::sync::Arc;

use crabka_pgexec::{SqlEngine, clock::FixedClock};
use crabka_pgwire::session::SessionConfig;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

/// The fixed "now" that all clock-function tests use.
const FIXED_NOW: &str = "2024-01-15T12:00:00Z";

fn fixed_clock() -> Arc<FixedClock> {
    let ts: jiff::Timestamp = FIXED_NOW.parse().expect("fixed now");
    Arc::new(FixedClock(ts))
}

async fn spawn() -> u16 {
    spawn_with_clock(fixed_clock()).await
}

async fn spawn_with_clock(clock: Arc<FixedClock>) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(crabka_pgwire::server::serve(
        listener,
        Arc::new(SqlEngine::new().with_clock(clock)),
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

/// First column of the first row as text, through the simple query protocol and
/// the engine's own text encoding.
async fn text(client: &tokio_postgres::Client, sql: &str) -> Option<String> {
    use tokio_postgres::SimpleQueryMessage;
    for m in client.simple_query(sql).await.expect(sql) {
        if let SimpleQueryMessage::Row(row) = m {
            return row.get(0).map(std::string::ToString::to_string);
        }
    }
    panic!("no row for `{sql}`");
}

/// All first-column values of a query, in text format and in row order.
#[allow(dead_code)]
async fn col0(client: &tokio_postgres::Client, sql: &str) -> Vec<Option<String>> {
    use tokio_postgres::SimpleQueryMessage;
    let mut out = Vec::new();
    for m in client.simple_query(sql).await.expect(sql) {
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

// ---------------------------------------------------------------------------
// to_char — datetime + numeric engines
// ---------------------------------------------------------------------------

#[tokio::test]
async fn to_char_datetime_and_numeric() {
    let client = connect(spawn().await).await;
    assert_eq!(
        text(
            &client,
            "SELECT to_char(TIMESTAMP '2024-01-15 13:45:06', 'YYYY-MM-DD HH24:MI:SS')"
        )
        .await,
        Some("2024-01-15 13:45:06".into())
    );
    assert_eq!(
        text(
            &client,
            "SELECT to_char(DATE '2024-07-04', 'FMMonth FMDD, YYYY')"
        )
        .await,
        Some("July 4, 2024".into())
    );
    assert_eq!(
        text(&client, "SELECT to_char(485, '999')").await,
        Some(" 485".into())
    );
    assert_eq!(
        text(&client, "SELECT to_char(1234567, 'FM9,999,999')").await,
        Some("1,234,567".into())
    );
}

#[tokio::test]
async fn to_char_timestamptz_under_set_timezone() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("SET TIME ZONE 'America/New_York'")
        .await
        .expect("set tz");
    // 2024-01-15 17:00 UTC = 12:00 EST.
    assert_eq!(
        text(
            &client,
            "SELECT to_char(TIMESTAMPTZ '2024-01-15 17:00:00+00', 'YYYY-MM-DD HH24:MI:SS TZH')"
        )
        .await,
        Some("2024-01-15 12:00:00 -05".into())
    );
}

// ---------------------------------------------------------------------------
// to_timestamp / to_date / make_* / justify_*
// ---------------------------------------------------------------------------

#[tokio::test]
async fn to_timestamp_to_date_make_justify() {
    let client = connect(spawn().await).await;
    assert_eq!(
        text(&client, "SELECT to_date('2024-07-04', 'YYYY-MM-DD')").await,
        Some("2024-07-04".into())
    );
    assert_eq!(
        text(&client, "SELECT make_date(2024, 2, 29)").await,
        Some("2024-02-29".into())
    );
    assert_eq!(
        text(&client, "SELECT make_interval(1, 2, 0, 3)").await,
        Some("1 year 2 mons 3 days".into())
    );
    assert_eq!(
        text(&client, "SELECT justify_interval(INTERVAL '1 mon -1 hour')").await,
        Some("29 days 23:00:00".into())
    );
}

// ---------------------------------------------------------------------------
// Result OIDs from the RowDescription
// ---------------------------------------------------------------------------

#[tokio::test]
async fn result_oids() {
    use tokio_postgres::types::Type;
    let client = connect(spawn().await).await;
    let rows = client
        .query(
            "SELECT to_char(485,'999'), to_date('2024-01-01','YYYY-MM-DD'), to_timestamp('2024-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')",
            &[],
        )
        .await
        .expect("q");
    assert_eq!(rows[0].columns()[0].type_(), &Type::TEXT);
    assert_eq!(rows[0].columns()[1].type_(), &Type::DATE);
    assert_eq!(rows[0].columns()[2].type_(), &Type::TIMESTAMPTZ);
}

// ---------------------------------------------------------------------------
// Error surface (SQLSTATE assertions)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn error_surface() {
    let client = connect(spawn().await).await;
    assert_eq!(
        err_code(&client, "SELECT to_date('xx', 'YYYY-MM-DD')").await,
        "22007"
    );
    assert_eq!(
        err_code(&client, "SELECT make_date(2024, 13, 1)").await,
        "22008"
    );
    assert_eq!(err_code(&client, "SELECT to_char(485)").await, "42883");
}
