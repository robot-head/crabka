//! Non-finite date/time values, `timetz`, interval field ranges and the
//! `extract` result scales, end to end over the wire.
//!
//! Every expectation is `PostgreSQL` 18.4's, taken from a live oracle with
//! `DateStyle = 'ISO, MDY'` and `TimeZone = 'UTC'`.

use std::sync::Arc;

use assert2::assert;
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

/// Every column of the first row, as text, through the simple query protocol,
/// so that the comparison uses the engine's own output functions.
async fn row(client: &tokio_postgres::Client, sql: &str) -> Vec<Option<String>> {
    use tokio_postgres::SimpleQueryMessage;
    for m in client.simple_query(sql).await.expect("query") {
        if let SimpleQueryMessage::Row(r) = m {
            return (0..r.len())
                .map(|i| r.get(i).map(std::string::ToString::to_string))
                .collect();
        }
    }
    panic!("no row for `{sql}`");
}

/// The first column of the first row.
async fn scalar(client: &tokio_postgres::Client, sql: &str) -> Option<String> {
    row(client, sql).await.into_iter().next().flatten()
}

/// The first column of every row, in the order the engine returned them.
async fn column(client: &tokio_postgres::Client, sql: &str) -> Vec<Option<String>> {
    use tokio_postgres::SimpleQueryMessage;
    client
        .simple_query(sql)
        .await
        .expect("query")
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r.get(0).map(std::string::ToString::to_string)),
            _ => None,
        })
        .collect()
}

/// The SQLSTATE of a statement that is expected to error.
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
async fn infinity_round_trips_through_every_temporal_type() {
    let client = connect(spawn().await).await;
    let cases: &[(&str, &str)] = &[
        ("SELECT 'infinity'::date", "infinity"),
        ("SELECT '-infinity'::date", "-infinity"),
        ("SELECT 'infinity'::timestamp", "infinity"),
        ("SELECT '-infinity'::timestamp", "-infinity"),
        ("SELECT 'infinity'::timestamptz", "infinity"),
        ("SELECT '+infinity'::timestamp", "infinity"),
        ("SELECT 'infinity'::interval", "infinity"),
        ("SELECT '-infinity'::interval", "-infinity"),
        // Casts between the temporal types carry it through unchanged.
        ("SELECT (date 'infinity')::timestamp", "infinity"),
        ("SELECT (timestamp '-infinity')::date", "-infinity"),
        ("SELECT (timestamp 'infinity')::timestamptz", "infinity"),
        ("SELECT (timestamptz '-infinity')::timestamp", "-infinity"),
        // Arithmetic keeps it rather than computing with it.
        ("SELECT date 'infinity' + 1", "infinity"),
        ("SELECT timestamp 'infinity' + interval '1 day'", "infinity"),
        (
            "SELECT timestamp 'infinity' - timestamp '1995-08-06 12:12:12'",
            "infinity",
        ),
        (
            "SELECT date_trunc('week', timestamp 'infinity')",
            "infinity",
        ),
        ("SELECT -interval 'infinity'", "-infinity"),
        ("SELECT interval 'infinity' * 2", "infinity"),
        ("SELECT justify_interval(interval 'infinity')", "infinity"),
    ];
    for (sql, expected) in cases {
        assert!(
            scalar(&client, sql).await.as_deref() == Some(*expected),
            "{sql}"
        );
    }
    // A `time` has no non-finite value, so the spelling is malformed text there.
    assert!(err_code(&client, "SELECT 'infinity'::time").await == "22007");
}

#[tokio::test]
async fn infinity_sorts_outside_every_finite_value() {
    let client = connect(spawn().await).await;
    assert!(
        scalar(&client, "SELECT 'infinity'::date > '9999-01-01'::date")
            .await
            .as_deref()
            == Some("t")
    );
    assert!(
        scalar(&client, "SELECT '-infinity'::date < '0001-01-01'::date")
            .await
            .as_deref()
            == Some("t")
    );
    assert!(
        scalar(
            &client,
            "SELECT interval '1000000 days' < interval 'infinity'"
        )
        .await
        .as_deref()
            == Some("t")
    );
    assert!(
        column(
            &client,
            "SELECT d FROM (VALUES ('infinity'::timestamp), ('2000-01-01'), ('-infinity')) v(d) \
             ORDER BY d"
        )
        .await
            == vec![
                Some("-infinity".to_string()),
                Some("2000-01-01 00:00:00".to_string()),
                Some("infinity".to_string()),
            ]
    );
    // `isfinite` is the predicate that distinguishes them.
    assert!(
        row(
            &client,
            "SELECT isfinite('infinity'::date), isfinite('2000-01-01'::date), \
             isfinite(interval 'infinity')"
        )
        .await
            == vec![
                Some("f".to_string()),
                Some("t".to_string()),
                Some("f".to_string()),
            ]
    );
}

/// The last civil date the calendar holds is an ordinary date, not the
/// `infinity` sentinel.
///
/// `date` used to reserve the extreme representable value for `infinity`, the
/// way `timestamp` and `interval` still do. That works only where the storage
/// has room to spare, and `date`'s storage has none. Its extreme value IS
/// 9999-12-31, a date `PostgreSQL` 18.4 accepts and prints back. The sentinel
/// therefore took a real date away, and both spellings below were refused as
/// out of range. `date` now holds the two non-finite values out of band.
#[tokio::test]
async fn the_last_civil_date_is_a_date_and_not_infinity() {
    let client = connect(spawn().await).await;
    let cases: &[(&str, &str)] = &[
        ("SELECT make_date(9999, 12, 31)", "9999-12-31"),
        ("SELECT date '9999-12-31'", "9999-12-31"),
        ("SELECT '9999-12-31'::text::date", "9999-12-31"),
        (
            "SELECT to_char(date '9999-12-31', 'YYYY-MM-DD')",
            "9999-12-31",
        ),
        ("SELECT extract(year FROM date '9999-12-31')", "9999"),
        ("SELECT isfinite(date '9999-12-31')", "t"),
        (
            "SELECT (date '9999-12-31')::timestamp",
            "9999-12-31 00:00:00",
        ),
        ("SELECT date '9999-12-31' - date '9999-12-01'", "30"),
        // It still sorts below `infinity`, which is the property the sentinel
        // used to buy, and `infinity` still prints as itself.
        ("SELECT date 'infinity' > date '9999-12-31'", "t"),
        ("SELECT date '9999-12-31' > date '-infinity'", "t"),
        ("SELECT date 'infinity'", "infinity"),
    ];
    for (sql, expected) in cases {
        assert!(
            scalar(&client, sql).await.as_deref() == Some(*expected),
            "{sql}"
        );
    }
    // Stored, ordered and read back: the row encoding keeps the two apart.
    client
        .simple_query(
            "CREATE TABLE last_day (id int primary key, d date); \
             INSERT INTO last_day VALUES (1, date '9999-12-31'), (2, date 'infinity'), \
             (3, date '-infinity'), (4, date '2000-01-01')",
        )
        .await
        .expect("seed");
    assert!(
        column(&client, "SELECT d FROM last_day ORDER BY d").await
            == vec![
                Some("-infinity".to_string()),
                Some("2000-01-01".to_string()),
                Some("9999-12-31".to_string()),
                Some("infinity".to_string()),
            ]
    );
    assert!(
        column(
            &client,
            "SELECT id FROM last_day WHERE d = date '9999-12-31'"
        )
        .await
            == vec![Some("1".to_string())]
    );
}

#[tokio::test]
async fn cancelling_infinities_are_out_of_range() {
    let client = connect(spawn().await).await;
    for sql in [
        "SELECT timestamp 'infinity' - timestamp 'infinity'",
        "SELECT timestamp '-infinity' - timestamp '-infinity'",
        "SELECT interval 'infinity' - interval 'infinity'",
        "SELECT date 'infinity' - date '2000-01-01'",
    ] {
        assert!(err_code(&client, sql).await == "22008", "{sql}");
    }
}

#[tokio::test]
async fn extract_returns_numeric_at_postgres_scales() {
    let client = connect(spawn().await).await;
    let cases: &[(&str, &str)] = &[
        (
            "SELECT extract(epoch from timestamp '2024-01-15')",
            "1705276800.000000",
        ),
        (
            "SELECT extract(epoch from timestamptz '2024-01-15Z')",
            "1705276800.000000",
        ),
        // A `date` has no clock, so its epoch is a whole number of seconds.
        ("SELECT extract(epoch from date '2024-01-15')", "1705276800"),
        (
            "SELECT extract(epoch from time '01:02:03.5')",
            "3723.500000",
        ),
        (
            "SELECT extract(epoch from interval '1 day')",
            "86400.000000",
        ),
        (
            "SELECT extract(second from timestamp '2024-01-15 01:02:03.5')",
            "3.500000",
        ),
        (
            "SELECT extract(second from interval '3.25 sec')",
            "3.250000",
        ),
        (
            "SELECT extract(milliseconds from timestamp '2024-01-15 00:00:03.5')",
            "3500.000",
        ),
        (
            "SELECT extract(microseconds from timestamp '2024-01-15 00:00:03.5')",
            "3500000",
        ),
        ("SELECT extract(julian from date '2020-08-11')", "2459073"),
        ("SELECT extract(year from timestamp '2024-01-15')", "2024"),
        // date_part keeps the historical float8 form, which trims the zeros.
        (
            "SELECT date_part('second', timestamp '2024-01-15 01:02:03.5')",
            "3.5",
        ),
        (
            "SELECT date_part('epoch', timestamp '2024-01-15')",
            "1705276800",
        ),
    ];
    for (sql, expected) in cases {
        assert!(
            scalar(&client, sql).await.as_deref() == Some(*expected),
            "{sql}"
        );
    }
    // Non-finite sources split into monotonic (±Infinity) and oscillating (NULL).
    assert!(
        scalar(&client, "SELECT extract(epoch from timestamp 'infinity')")
            .await
            .as_deref()
            == Some("Infinity")
    );
    assert!(
        scalar(&client, "SELECT extract(day from timestamp 'infinity')")
            .await
            .is_none()
    );
    // A real unit the type has no value for is 0A000; a non-unit is 22023.
    assert!(err_code(&client, "SELECT extract(hour from date '2020-08-11')").await == "0A000");
    assert!(err_code(&client, "SELECT extract(fortnight from time '01:02:03')").await == "22023");
}

#[tokio::test]
async fn date_trunc_of_a_date_resolves_through_timestamptz() {
    let client = connect(spawn().await).await;
    assert!(
        scalar(&client, "SELECT date_trunc('day', date '2024-01-15')")
            .await
            .as_deref()
            == Some("2024-01-15 00:00:00+00")
    );
    assert!(
        scalar(
            &client,
            "SELECT pg_typeof(date_trunc('day', date '2024-01-15'))"
        )
        .await
        .as_deref()
            == Some("timestamp with time zone")
    );
    // A plain timestamp keeps its own type.
    assert!(
        scalar(
            &client,
            "SELECT date_trunc('day', timestamp '2024-01-15 10:00')"
        )
        .await
        .as_deref()
            == Some("2024-01-15 00:00:00")
    );
}

#[tokio::test]
async fn interval_field_ranges_supply_units_and_truncate() {
    let client = connect(spawn().await).await;
    let cases: &[(&str, &str)] = &[
        ("SELECT interval '1' year", "1 year"),
        ("SELECT interval '1' month", "1 mon"),
        ("SELECT interval '90' minute", "01:30:00"),
        ("SELECT interval '1.5' day", "1 day"),
        // A bare quantity takes the range's LAST field, and each quantity to its
        // left takes the next coarser one.
        ("SELECT interval '1' year to month", "1 mon"),
        ("SELECT interval '1-2' year to month", "1 year 2 mons"),
        ("SELECT interval '4 5' day to hour", "4 days 05:00:00"),
        (
            "SELECT interval '1 2:03:04' day to second",
            "1 day 02:03:04",
        ),
        ("SELECT interval '2:03' hour to minute", "02:03:00"),
        ("SELECT interval '1 2:03' day to minute", "1 day 02:03:00"),
    ];
    for (sql, expected) in cases {
        assert!(
            scalar(&client, sql).await.as_deref() == Some(*expected),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn interval_output_places_the_sign_per_field() {
    let client = connect(spawn().await).await;
    let cases: &[(&str, &str)] = &[
        (
            "SELECT interval '-3 days 4 hours 5 min 6 sec'",
            "-3 days +04:05:06",
        ),
        ("SELECT interval '3 days -4 hours'", "3 days -04:00:00"),
        (
            "SELECT interval '-1 year -2 mons +3 days 4:05:06'",
            "-1 years -2 mons +3 days 04:05:06",
        ),
        ("SELECT interval '@ 1 year 2 mons ago'", "-1 years -2 mons"),
        (
            "SELECT interval 'P1Y2M3DT4H5M6S'",
            "1 year 2 mons 3 days 04:05:06",
        ),
        (
            "SELECT interval 'P0001-02-03T04:05:06'",
            "1 year 2 mons 3 days 04:05:06",
        ),
        ("SELECT interval '1.5 weeks'", "10 days 12:00:00"),
        ("SELECT interval '00:00:00'", "00:00:00"),
    ];
    for (sql, expected) in cases {
        assert!(
            scalar(&client, sql).await.as_deref() == Some(*expected),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn timetz_parses_orders_and_stores() {
    let client = connect(spawn().await).await;
    let cases: &[(&str, &str)] = &[
        ("SELECT '00:01 PDT'::timetz", "00:01:00-07"),
        ("SELECT '11:59:59.99 PM PDT'::timetz", "23:59:59.99-07"),
        ("SELECT '12:00:00+05:30'::timetz", "12:00:00+05:30"),
        (
            "SELECT '2003-03-07 15:36:39 America/New_York'::timetz",
            "15:36:39-05",
        ),
        ("SELECT ('12:34:56-05'::timetz)::time", "12:34:56"),
        (
            "SELECT (timestamptz '2020-05-26 13:30:25-04')::timetz",
            "17:30:25+00",
        ),
        (
            "SELECT timetz '11:27:42-05' + interval '1 hour'",
            "12:27:42-05",
        ),
        (
            "SELECT timetz '11:27:42-05' AT TIME ZONE 'UTC'",
            "16:27:42+00",
        ),
        (
            "SELECT extract(epoch from timetz '2020-05-26 13:30:25.575401-04')",
            "63025.575401",
        ),
        (
            "SELECT pg_typeof('12:00:00-05'::timetz)",
            "time with time zone",
        ),
    ];
    for (sql, expected) in cases {
        assert!(
            scalar(&client, sql).await.as_deref() == Some(*expected),
            "{sql}"
        );
    }
    // Ordering is by the UTC-equivalent instant, with the zone as the tiebreak,
    // so two spellings of the same instant are ordered but not equal.
    assert!(
        scalar(
            &client,
            "SELECT '12:00:00-05'::timetz > '12:00:00+00'::timetz"
        )
        .await
        .as_deref()
            == Some("t")
    );
    assert!(
        scalar(
            &client,
            "SELECT '12:00:00-05'::timetz = '17:00:00+00'::timetz"
        )
        .await
        .as_deref()
            == Some("f")
    );
    // A zone NAME needs a date to resolve, so the same text without one is
    // malformed rather than out of range.
    assert!(err_code(&client, "SELECT '15:36:39 America/New_York'::timetz").await == "22007");
    assert!(err_code(&client, "SELECT '25:00:00 PDT'::timetz").await == "22008");

    client
        .simple_query("CREATE TABLE tz_store (id int, f1 time(2) with time zone)")
        .await
        .expect("create");
    client
        .simple_query(
            "INSERT INTO tz_store VALUES (1, '00:01 PDT'), (2, '12:00:00-05'), (3, '17:00:00+00')",
        )
        .await
        .expect("insert");
    assert!(
        column(&client, "SELECT f1 FROM tz_store ORDER BY f1").await
            == vec![
                Some("00:01:00-07".to_string()),
                Some("17:00:00+00".to_string()),
                Some("12:00:00-05".to_string()),
            ]
    );
    assert!(
        scalar(
            &client,
            "SELECT id::text FROM tz_store WHERE f1 = '12:00:00-05'"
        )
        .await
        .as_deref()
            == Some("2")
    );
}

#[tokio::test]
async fn date_style_field_order_decides_an_ambiguous_literal() {
    let client = connect(spawn().await).await;
    client
        .simple_query("SET datestyle TO ymd")
        .await
        .expect("set");
    assert!(scalar(&client, "SELECT date '99 01 08'").await.as_deref() == Some("1999-01-08"));
    assert!(scalar(&client, "SELECT date '01/02/03'").await.as_deref() == Some("2001-02-03"));
    client
        .simple_query("SET datestyle TO dmy")
        .await
        .expect("set");
    assert!(scalar(&client, "SELECT date '08 01 99'").await.as_deref() == Some("1999-01-08"));
    assert!(scalar(&client, "SELECT date '01/02/03'").await.as_deref() == Some("2003-02-01"));
    client
        .simple_query("SET datestyle TO mdy")
        .await
        .expect("set");
    assert!(scalar(&client, "SELECT date '01 08 99'").await.as_deref() == Some("1999-01-08"));
    assert!(scalar(&client, "SELECT date '01/02/03'").await.as_deref() == Some("2003-01-02"));
    // The ISO spelling is unambiguous, so it reads the same under every order.
    assert!(scalar(&client, "SELECT date '1999-01-08'").await.as_deref() == Some("1999-01-08"));
    // 99 in the month slot is out of range, not malformed.
    assert!(err_code(&client, "SELECT date '99 01 08'").await == "22008");
}

#[tokio::test]
async fn date_bin_snaps_to_a_stride_grid() {
    let client = connect(spawn().await).await;
    let cases: &[(&str, &str)] = &[
        (
            "SELECT date_bin('5 min'::interval, timestamp '2020-02-01 01:01:01', \
             timestamp '2020-02-01 00:02:30')",
            "2020-02-01 00:57:30",
        ),
        (
            "SELECT date_bin('30 minutes'::interval, timestamp '2024-02-01 15:00:00', \
             timestamp '2024-02-01 17:00:00')",
            "2024-02-01 15:00:00",
        ),
        (
            "SELECT date_bin('15 minutes'::interval, timestamptz '2020-02-11 15:44:17.71393Z', \
             timestamptz '2001-01-01Z')",
            "2020-02-11 15:30:00+00",
        ),
    ];
    for (sql, expected) in cases {
        assert!(
            scalar(&client, sql).await.as_deref() == Some(*expected),
            "{sql}"
        );
    }
    // A month-bearing stride has no fixed width, and a non-positive one has no
    // grid at all.
    assert!(
        err_code(
            &client,
            "SELECT date_bin('5 months'::interval, timestamp '2020-02-01', timestamp '2001-01-01')"
        )
        .await
            == "0A000"
    );
    assert!(
        err_code(
            &client,
            "SELECT date_bin('0 days'::interval, timestamp '1970-01-01 01:00', \
             timestamp '1970-01-01')"
        )
        .await
            == "22008"
    );
}

#[tokio::test]
async fn temporal_typmods_round_values_when_stored() {
    let client = connect(spawn().await).await;
    client
        .simple_query(
            "CREATE TABLE temporal_typmod_store (\
               t time(0), z time(2) with time zone, ts timestamp(0), \
               tz timestamptz(0), iv interval(0)); \
             INSERT INTO temporal_typmod_store VALUES (\
               '12:34:56.5', '12:34:56.785-05', '2000-01-01 00:00:00.5', \
               '2000-01-01 00:00:00.5+00', '1.5 seconds')",
        )
        .await
        .expect("store temporal typmods");
    assert!(
        row(
            &client,
            "SELECT t::text, z::text, ts::text, tz::text, iv::text \
             FROM temporal_typmod_store",
        )
        .await
            == vec![
                Some("12:34:57".into()),
                Some("12:34:56.79-05".into()),
                Some("2000-01-01 00:00:01".into()),
                Some("2000-01-01 00:00:01+00".into()),
                Some("00:00:02".into()),
            ]
    );
}

#[tokio::test]
async fn out_of_range_literals_carry_postgres_sqlstates() {
    let client = connect(spawn().await).await;
    let cases: &[(&str, &str)] = &[
        ("SELECT TIME '25:00:00'", "22008"),
        ("SELECT TIME '24:01:00'", "22008"),
        ("SELECT TIME '24:00:00.01'", "22008"),
        ("SELECT TIME '23:59:60.01'", "22008"),
        ("SELECT DATE '2023-02-29'", "22008"),
        ("SELECT timestamp '2001-01-01 25:00:00'", "22008"),
        ("SELECT date '4714-11-23 BC'", "22008"),
        ("SELECT timestamp 'Feb 16 17:32:01 -0097'", "22009"),
        (
            "SELECT timestamp '19970710 173201 America/Does_not_exist'",
            "22023",
        ),
        ("SELECT DATE 'garbage'", "22007"),
        ("SELECT interval 'garbage'", "22007"),
    ];
    for (sql, expected) in cases {
        assert!(err_code(&client, sql).await == *expected, "{sql}");
    }
}
