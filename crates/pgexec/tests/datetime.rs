//! SP37: date/time types — end-to-end over the wire.
//!
//! Exercises `date`, `time`, `timestamp`, `timestamptz`, and `interval`:
//! column round-trip + type OIDs, typed literals, arithmetic, comparison /
//! ORDER BY, casts, extract / `date_part` / `date_trunc` / age, clock functions
//! (deterministic via `FixedClock`), `AT TIME ZONE`, `SET TIME ZONE` +
//! rendering, transactional SET, and the error SQLSTATE surface.
//!
//! Values are read in TEXT mode (simple query protocol) so the assertions
//! exercise the engine's own `*_to_text` encodings directly.

use std::sync::Arc;

use crabka_pgexec::{SqlEngine, clock::FixedClock};
use crabka_pgwire::session::SessionConfig;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, SimpleQueryMessage, types::Type};

/// The fixed "now" used for all clock-function tests.
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

/// First column of the first row as text (simple query → engine's own text encoding).
async fn text(client: &tokio_postgres::Client, sql: &str) -> Option<String> {
    use tokio_postgres::SimpleQueryMessage;
    for m in client.simple_query(sql).await.expect(sql) {
        if let SimpleQueryMessage::Row(row) = m {
            return row.get(0).map(std::string::ToString::to_string);
        }
    }
    panic!("no row for `{sql}`");
}

/// All first-column values (text format) for a query, in row order.
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
// Column round-trip + result OIDs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn column_roundtrip_and_type_oids() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE dt (
                d date,
                tm time,
                ts timestamp,
                tz timestamptz,
                iv interval
            )",
        )
        .await
        .expect("create");
    client
        .batch_execute(
            "INSERT INTO dt VALUES (
                DATE '2024-01-15',
                TIME '13:45:06',
                TIMESTAMP '2024-01-15 13:45:06',
                TIMESTAMPTZ '2024-01-15 13:45:06+00',
                INTERVAL '1 day 02:30:00'
            )",
        )
        .await
        .expect("insert");

    // Text rendering via simple query (the engine's own *_to_text paths).
    assert_eq!(
        text(&client, "SELECT d FROM dt").await,
        Some("2024-01-15".into())
    );
    assert_eq!(
        text(&client, "SELECT tm FROM dt").await,
        Some("13:45:06".into())
    );
    assert_eq!(
        text(&client, "SELECT ts FROM dt").await,
        Some("2024-01-15 13:45:06".into())
    );
    // timestamptz renders in UTC (default session zone) with +00 suffix.
    assert_eq!(
        text(&client, "SELECT tz FROM dt").await,
        Some("2024-01-15 13:45:06+00".into())
    );
    assert_eq!(
        text(&client, "SELECT iv FROM dt").await,
        Some("1 day 02:30:00".into())
    );

    // Type OIDs from the RowDescription (binary query path reads RowDescription).
    let rows = client.query("SELECT d FROM dt", &[]).await.expect("d");
    assert_eq!(*rows[0].columns()[0].type_(), Type::DATE, "date OID 1082");
    let rows = client.query("SELECT tm FROM dt", &[]).await.expect("tm");
    assert_eq!(*rows[0].columns()[0].type_(), Type::TIME, "time OID 1083");
    let rows = client.query("SELECT ts FROM dt", &[]).await.expect("ts");
    assert_eq!(
        *rows[0].columns()[0].type_(),
        Type::TIMESTAMP,
        "timestamp OID 1114"
    );
    let rows = client.query("SELECT tz FROM dt", &[]).await.expect("tz");
    assert_eq!(
        *rows[0].columns()[0].type_(),
        Type::TIMESTAMPTZ,
        "timestamptz OID 1184"
    );
    let rows = client.query("SELECT iv FROM dt", &[]).await.expect("iv");
    assert_eq!(
        *rows[0].columns()[0].type_(),
        Type::INTERVAL,
        "interval OID 1186"
    );
}

// ---------------------------------------------------------------------------
// Typed literals
// ---------------------------------------------------------------------------

#[tokio::test]
async fn typed_literals() {
    let client = connect(spawn().await).await;
    assert_eq!(
        text(&client, "SELECT DATE '2024-01-15'").await,
        Some("2024-01-15".into())
    );
    assert_eq!(
        text(&client, "SELECT TIME '13:45:06'").await,
        Some("13:45:06".into())
    );
    assert_eq!(
        text(&client, "SELECT TIMESTAMP '2024-01-15 13:45:06'").await,
        Some("2024-01-15 13:45:06".into())
    );
    assert_eq!(
        text(&client, "SELECT INTERVAL '1 day 2:30:00'").await,
        Some("1 day 02:30:00".into())
    );
    // Fractional seconds round-trip.
    assert_eq!(
        text(&client, "SELECT TIMESTAMP '2024-06-30 23:59:59.5'").await,
        Some("2024-06-30 23:59:59.5".into())
    );
    // Zero interval prints as the canonical clock zero.
    assert_eq!(
        text(&client, "SELECT INTERVAL '0 days'").await,
        Some("00:00:00".into())
    );
}

// ---------------------------------------------------------------------------
// Arithmetic
// ---------------------------------------------------------------------------

#[tokio::test]
async fn arithmetic() {
    let client = connect(spawn().await).await;

    // date + int → date (days).
    assert_eq!(
        text(&client, "SELECT DATE '2024-01-01' + 31").await,
        Some("2024-02-01".into())
    );
    // date - date → int (days).
    assert_eq!(
        text(&client, "SELECT DATE '2024-02-01' - DATE '2024-01-01'").await,
        Some("31".into())
    );
    // timestamp + interval → timestamp.
    assert_eq!(
        text(
            &client,
            "SELECT TIMESTAMP '2024-01-01 00:00:00' + INTERVAL '1 day'"
        )
        .await,
        Some("2024-01-02 00:00:00".into())
    );
    // interval * int → interval.
    assert_eq!(
        text(&client, "SELECT INTERVAL '1 day' * 3").await,
        Some("3 days".into())
    );
    // timestamp - timestamp → interval (in days + micros).
    assert_eq!(
        text(
            &client,
            "SELECT TIMESTAMP '2024-01-03 00:00:00' - TIMESTAMP '2024-01-01 00:00:00'"
        )
        .await,
        Some("2 days".into())
    );
    // date - int → date.
    assert_eq!(
        text(&client, "SELECT DATE '2024-02-01' - 1").await,
        Some("2024-01-31".into())
    );
    // interval + interval.
    assert_eq!(
        text(&client, "SELECT INTERVAL '1 day' + INTERVAL '2 hours'").await,
        Some("1 day 02:00:00".into())
    );

    // SP37 §8 GAP A: time + interval → time (uses only the micros, wraps mod 24h;
    // the interval's days are ignored).
    assert_eq!(
        text(&client, "SELECT TIME '23:00:00' + INTERVAL '2 hours'").await,
        Some("01:00:00".into())
    );
    assert_eq!(
        text(&client, "SELECT TIME '12:00:00' + INTERVAL '1 day'").await,
        Some("12:00:00".into())
    );
    // time - interval → time (wraps backward).
    assert_eq!(
        text(&client, "SELECT TIME '00:30:00' - INTERVAL '1 hour'").await,
        Some("23:30:00".into())
    );
    // SP37 §8 GAP B: date + time → timestamp; time + date is symmetric.
    assert_eq!(
        text(&client, "SELECT DATE '2024-01-15' + TIME '13:45:06'").await,
        Some("2024-01-15 13:45:06".into())
    );
    assert_eq!(
        text(&client, "SELECT TIME '13:45:06' + DATE '2024-01-15'").await,
        Some("2024-01-15 13:45:06".into())
    );

    // SP37 §8: tz-aware timestamptz ± interval → timestamptz (default session zone
    // is UTC, so a +1h shift is a straight instant shift; renders with +00).
    assert_eq!(
        text(
            &client,
            "SELECT TIMESTAMPTZ '2024-01-15 12:00:00+00' + INTERVAL '1 hour'"
        )
        .await,
        Some("2024-01-15 13:00:00+00".into())
    );
    assert_eq!(
        text(
            &client,
            "SELECT TIMESTAMPTZ '2024-01-15 12:00:00+00' - INTERVAL '90 minutes'"
        )
        .await,
        Some("2024-01-15 10:30:00+00".into())
    );
    // timestamptz - timestamptz → interval (absolute-instant difference).
    assert_eq!(
        text(
            &client,
            "SELECT TIMESTAMPTZ '2024-01-15 14:00:00+00' - TIMESTAMPTZ '2024-01-15 12:00:00+00'"
        )
        .await,
        Some("02:00:00".into())
    );
}

// ---------------------------------------------------------------------------
// Comparison / ORDER BY
// ---------------------------------------------------------------------------

#[tokio::test]
async fn comparison_and_order_by() {
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE ev (id int4, at date);
             INSERT INTO ev VALUES (1, DATE '2024-03-15'), (2, DATE '2024-01-10'), (3, DATE '2024-02-20')",
        )
        .await
        .expect("create+insert");

    // ORDER BY date ASC.
    assert_eq!(
        col0(&client, "SELECT id FROM ev ORDER BY at ASC").await,
        vec![Some("2".into()), Some("3".into()), Some("1".into())]
    );
    // ORDER BY date DESC.
    assert_eq!(
        col0(&client, "SELECT id FROM ev ORDER BY at DESC").await,
        vec![Some("1".into()), Some("3".into()), Some("2".into())]
    );
    // WHERE with date comparison.
    assert_eq!(
        text(
            &client,
            "SELECT id FROM ev WHERE at > DATE '2024-02-01' ORDER BY at"
        )
        .await,
        Some("3".into())
    );
    // timestamp ordering.
    client
        .batch_execute(
            "CREATE TABLE ts_ev (id int4, at timestamp);
             INSERT INTO ts_ev VALUES (1, TIMESTAMP '2024-01-15 09:00:00'), (2, TIMESTAMP '2024-01-15 08:00:00')",
        )
        .await
        .expect("ts create");
    assert_eq!(
        col0(&client, "SELECT id FROM ts_ev ORDER BY at").await,
        vec![Some("2".into()), Some("1".into())]
    );
}

// ---------------------------------------------------------------------------
// Casts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn casts() {
    let client = connect(spawn().await).await;

    // text → date via ::.
    assert_eq!(
        text(&client, "SELECT '2024-01-15'::date").await,
        Some("2024-01-15".into())
    );
    // timestamp → date (truncate time).
    assert_eq!(
        text(&client, "SELECT TIMESTAMP '2024-01-15 13:45:06'::date").await,
        Some("2024-01-15".into())
    );
    // date → text.
    assert_eq!(
        text(&client, "SELECT DATE '2024-01-15'::text").await,
        Some("2024-01-15".into())
    );
    // date → timestamp (at midnight).
    assert_eq!(
        text(&client, "SELECT DATE '2024-01-15'::timestamp").await,
        Some("2024-01-15 00:00:00".into())
    );
    // timestamp → text.
    assert_eq!(
        text(&client, "SELECT TIMESTAMP '2024-01-15 13:45:06'::text").await,
        Some("2024-01-15 13:45:06".into())
    );
    // timestamptz → text (renders in UTC, the default session zone).
    assert_eq!(
        text(&client, "SELECT TIMESTAMPTZ '2024-01-15 13:45:06+00'::text").await,
        Some("2024-01-15 13:45:06+00".into())
    );
    // interval → text.
    assert_eq!(
        text(&client, "SELECT INTERVAL '2 days 03:00:00'::text").await,
        Some("2 days 03:00:00".into())
    );
    // CAST spelling is equivalent.
    assert_eq!(
        text(&client, "SELECT CAST('2024-06-01' AS date)").await,
        Some("2024-06-01".into())
    );
    // interval → date is forbidden (42846).
    assert_eq!(
        err_code(&client, "SELECT INTERVAL '1 day'::date").await,
        "42846"
    );
}

// ---------------------------------------------------------------------------
// Functions: extract / date_part / date_trunc / age
// ---------------------------------------------------------------------------

#[tokio::test]
async fn functions_extract_and_date_part() {
    let client = connect(spawn().await).await;

    // extract(year …) → numeric.
    assert_eq!(
        text(
            &client,
            "SELECT extract(year from TIMESTAMP '2024-07-15 00:00:00')"
        )
        .await,
        Some("2024".into())
    );
    // extract(month …) from DATE → numeric.
    assert_eq!(
        text(&client, "SELECT extract(month from DATE '2024-07-01')").await,
        Some("7".into())
    );
    // extract(day …).
    assert_eq!(
        text(
            &client,
            "SELECT extract(day from TIMESTAMP '2024-07-15 00:00:00')"
        )
        .await,
        Some("15".into())
    );
    // extract(hour …).
    assert_eq!(
        text(
            &client,
            "SELECT extract(hour from TIMESTAMP '2024-07-15 13:45:06')"
        )
        .await,
        Some("13".into())
    );
    // date_part (float8 result).
    assert_eq!(
        text(&client, "SELECT date_part('month', DATE '2024-07-01')").await,
        Some("7".into())
    );
    assert_eq!(
        text(
            &client,
            "SELECT date_part('second', TIMESTAMP '2024-07-01 08:30:45.5')"
        )
        .await,
        Some("45.5".into())
    );
}

#[tokio::test]
async fn functions_date_trunc() {
    let client = connect(spawn().await).await;

    assert_eq!(
        text(
            &client,
            "SELECT date_trunc('month', TIMESTAMP '2024-07-15 13:45:06')"
        )
        .await,
        Some("2024-07-01 00:00:00".into())
    );
    assert_eq!(
        text(
            &client,
            "SELECT date_trunc('day', TIMESTAMP '2024-07-15 13:45:06')"
        )
        .await,
        Some("2024-07-15 00:00:00".into())
    );
    assert_eq!(
        text(
            &client,
            "SELECT date_trunc('hour', TIMESTAMP '2024-07-15 13:45:06')"
        )
        .await,
        Some("2024-07-15 13:00:00".into())
    );
    assert_eq!(
        text(
            &client,
            "SELECT date_trunc('year', TIMESTAMP '2024-07-15 13:45:06')"
        )
        .await,
        Some("2024-01-01 00:00:00".into())
    );
}

#[tokio::test]
async fn functions_age() {
    let client = connect(spawn().await).await;

    // age(end, start) → interval with month borrowing.
    assert_eq!(
        text(
            &client,
            "SELECT age(TIMESTAMP '2024-03-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00')"
        )
        .await,
        Some("2 mons".into())
    );
    // A difference that spans a year.
    assert_eq!(
        text(
            &client,
            "SELECT age(TIMESTAMP '2025-06-15 00:00:00', TIMESTAMP '2024-01-01 00:00:00')"
        )
        .await,
        Some("1 year 5 mons 14 days".into())
    );
    // Exact full months.
    assert_eq!(
        text(
            &client,
            "SELECT age(TIMESTAMP '2024-04-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00')"
        )
        .await,
        Some("3 mons".into())
    );
}

// ---------------------------------------------------------------------------
// Clock functions (deterministic via FixedClock at 2024-01-15T12:00:00Z)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn clock_functions_are_deterministic() {
    let client = connect(spawn().await).await;

    // now() → timestamptz, renders in UTC as `2024-01-15 12:00:00+00`.
    assert_eq!(
        text(&client, "SELECT now()").await,
        Some("2024-01-15 12:00:00+00".into())
    );
    // current_date renders the date in the session zone (UTC), so Jan 15.
    assert_eq!(
        text(&client, "SELECT current_date").await,
        Some("2024-01-15".into())
    );
    // current_timestamp is an alias for now().
    assert_eq!(
        text(&client, "SELECT current_timestamp").await,
        Some("2024-01-15 12:00:00+00".into())
    );
    // Two now() calls in the same transaction are equal (transaction-stable).
    let row = client
        .simple_query("SELECT now() = now()")
        .await
        .expect("eq");
    let val = row.iter().find_map(|m| {
        if let SimpleQueryMessage::Row(r) = m {
            r.get(0).map(std::string::ToString::to_string)
        } else {
            None
        }
    });
    assert_eq!(val, Some("t".into()), "now() is transaction-stable");
}

// ---------------------------------------------------------------------------
// AT TIME ZONE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn at_time_zone() {
    let client = connect(spawn().await).await;

    // timestamp AT TIME ZONE 'UTC' → timestamptz (interpret as UTC).
    assert_eq!(
        text(
            &client,
            "SELECT TIMESTAMP '2024-01-15 12:00:00' AT TIME ZONE 'UTC'"
        )
        .await,
        Some("2024-01-15 12:00:00+00".into())
    );
    // timestamptz AT TIME ZONE 'America/New_York' → timestamp (render in NY, EST = -5h).
    // 12:00:00 UTC → 07:00:00 EST. Result is a local timestamp (no offset suffix).
    assert_eq!(
        text(
            &client,
            "SELECT TIMESTAMPTZ '2024-01-15 12:00:00+00' AT TIME ZONE 'America/New_York'"
        )
        .await,
        Some("2024-01-15 07:00:00".into())
    );
    // timestamp AT TIME ZONE 'America/New_York' → timestamptz.
    // Wall clock 12:00 local NY (EST = -5) → absolute 17:00 UTC.
    // Render in UTC (session zone): 2024-01-15 17:00:00+00.
    assert_eq!(
        text(
            &client,
            "SELECT TIMESTAMP '2024-01-15 12:00:00' AT TIME ZONE 'America/New_York'"
        )
        .await,
        Some("2024-01-15 17:00:00+00".into())
    );
}

// ---------------------------------------------------------------------------
// SET TIME ZONE + rendering
// ---------------------------------------------------------------------------

#[tokio::test]
async fn set_timezone_affects_rendering() {
    let client = connect(spawn().await).await;

    // Default zone is UTC.
    assert_eq!(text(&client, "SHOW timezone").await, Some("UTC".into()));
    // A timestamptz in UTC renders with +00.
    assert_eq!(
        text(&client, "SELECT TIMESTAMPTZ '2024-01-15 12:00:00+00'").await,
        Some("2024-01-15 12:00:00+00".into())
    );

    // SET TIME ZONE to America/New_York (EST = -5h in January).
    client
        .simple_query("SET TIME ZONE 'America/New_York'")
        .await
        .expect("set tz");
    assert_eq!(
        text(&client, "SHOW timezone").await,
        Some("America/New_York".into())
    );
    // Same absolute instant now renders in NY local time.
    assert_eq!(
        text(&client, "SELECT TIMESTAMPTZ '2024-01-15 12:00:00+00'").await,
        Some("2024-01-15 07:00:00-05".into())
    );
}

// ---------------------------------------------------------------------------
// Transactional SET TIME ZONE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transactional_set_timezone_rollback() {
    let client = connect(spawn().await).await;

    // SET outside a txn → persists.
    client
        .simple_query("SET TIME ZONE 'America/New_York'")
        .await
        .expect("set ny");

    // SET inside BEGIN … ROLLBACK → reverts.
    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("SET TIME ZONE 'UTC'")
        .await
        .expect("set utc");
    assert_eq!(text(&client, "SHOW timezone").await, Some("UTC".into()));
    client.simple_query("ROLLBACK").await.expect("rollback");
    assert_eq!(
        text(&client, "SHOW timezone").await,
        Some("America/New_York".into()),
        "ROLLBACK reverts SET TIME ZONE"
    );
}

#[tokio::test]
async fn transactional_set_timezone_commit() {
    let client = connect(spawn().await).await;

    // SET inside BEGIN … COMMIT → persists.
    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("SET TIME ZONE 'America/New_York'")
        .await
        .expect("set ny");
    client.simple_query("COMMIT").await.expect("commit");
    assert_eq!(
        text(&client, "SHOW timezone").await,
        Some("America/New_York".into()),
        "COMMIT keeps SET TIME ZONE"
    );
}

// ---------------------------------------------------------------------------
// Error surface (SQLSTATE assertions)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn error_surface() {
    let client = connect(spawn().await).await;

    // Invalid date (Feb 30) → 22008 (datetime field overflow).
    assert_eq!(err_code(&client, "SELECT DATE '2024-02-30'").await, "22008");

    // Unknown timezone → 22023 (invalid parameter value).
    assert_eq!(
        err_code(&client, "SET TIME ZONE 'Mars/Phobos'").await,
        "22023"
    );

    // interval → date has no cast → 42846 (cannot cast).
    assert_eq!(
        err_code(&client, "SELECT INTERVAL '1 day'::date").await,
        "42846"
    );

    // Bad time literal → 22007 (invalid datetime format).
    assert_eq!(err_code(&client, "SELECT TIME '25:00:00'").await, "22007");

    // Bad timestamp literal → 22007.
    assert_eq!(
        err_code(&client, "SELECT TIMESTAMP 'not-a-timestamp'").await,
        "22007"
    );
}
