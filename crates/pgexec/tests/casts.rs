//! SP31: explicit casts, `CAST(expr AS type)` and `expr::type`, end-to-end over
//! the wire. This covers both spellings, the cast matrix (text↔numeric/bool,
//! numeric↔numeric, bool↔int4, *→text), result-type OIDs, casts through a
//! column, and the error SQLSTATEs (22P02 / 22003 / 42846).

use std::sync::Arc;

use crabka_pgexec::SqlEngine;
use crabka_pgwire::session::SessionConfig;
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, types::Type};

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

/// First column of the first row as text. This uses the simple query protocol,
/// which exercises the engine's own text encoding.
async fn text(client: &tokio_postgres::Client, sql: &str) -> Option<String> {
    use tokio_postgres::SimpleQueryMessage;
    for m in client.simple_query(sql).await.expect("query") {
        if let SimpleQueryMessage::Row(row) = m {
            return row.get(0).map(std::string::ToString::to_string);
        }
    }
    panic!("no row for `{sql}`");
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
async fn cast_result_types_match_the_target() {
    let client = connect(spawn().await).await;
    // Each cast reports the target type's OID in the RowDescription.
    for (sql, want) in [
        ("SELECT '42'::int4", Type::INT4),
        ("SELECT '9000000000'::int8", Type::INT8),
        ("SELECT '1.5'::float8", Type::FLOAT8),
        ("SELECT 'true'::bool", Type::BOOL),
        ("SELECT 42::text", Type::TEXT),
        ("SELECT CAST(1 AS double precision)", Type::FLOAT8),
    ] {
        let row = client.query_one(sql, &[]).await.expect(sql);
        assert_eq!(*row.columns()[0].type_(), want, "type of `{sql}`");
    }
}

#[tokio::test]
async fn both_spellings_and_the_cast_matrix() {
    let client = connect(spawn().await).await;
    // `::` and `CAST(_ AS _)` are interchangeable.
    assert_eq!(
        client
            .query_one("SELECT '42'::int4", &[])
            .await
            .expect("q")
            .get::<_, i32>(0),
        42
    );
    assert_eq!(
        client
            .query_one("SELECT CAST('42' AS int4)", &[])
            .await
            .expect("q")
            .get::<_, i32>(0),
        42
    );
    // text → float8 / bool.
    let value = client
        .query_one("SELECT '1.5'::float8", &[])
        .await
        .expect("q")
        .get::<_, f64>(0);
    assert!((value - 1.5).abs() < f64::EPSILON);
    assert_eq!(text(&client, "SELECT 'no'::bool").await, Some("f".into()));
    // numeric ↔ numeric, bool ↔ int4, and → text rendering.
    assert_eq!(
        text(&client, "SELECT (5::int8)::int4").await,
        Some("5".into())
    );
    assert_eq!(text(&client, "SELECT true::int4").await, Some("1".into()));
    assert_eq!(text(&client, "SELECT 0::bool").await, Some("f".into()));
    assert_eq!(text(&client, "SELECT 42::text").await, Some("42".into()));
    // bool → text is `true`/`false` (the cast), not the `t`/`f` of a bool column.
    assert_eq!(
        text(&client, "SELECT true::text").await,
        Some("true".into())
    );
    // NULL casts to NULL.
    assert_eq!(text(&client, "SELECT null::int4").await, None);
}

#[tokio::test]
async fn cast_precedence_and_chaining() {
    let client = connect(spawn().await).await;
    // `::` binds tighter than unary minus and `+`.
    assert_eq!(text(&client, "SELECT -2::int8").await, Some("-2".into()));
    assert_eq!(text(&client, "SELECT 1 + 2::int8").await, Some("3".into()));
    // Chained left-to-right: text → int4 → float8.
    let row = client
        .query_one("SELECT '5'::int4::float8", &[])
        .await
        .expect("chain");
    assert!((row.get::<_, f64>(0) - 5.0).abs() < f64::EPSILON);
    assert_eq!(*row.columns()[0].type_(), Type::FLOAT8);
}

#[tokio::test]
async fn casts_through_a_column() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE c (id int4, label text, ratio double precision)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO c VALUES (1, '10', 2.5), (2, '20', 3.5)")
        .await
        .expect("insert");
    // text column → int4, used in WHERE; float8 column → int4 (round half-even).
    let rows = client
        .query(
            "SELECT id, label::int4, ratio::int4 FROM c WHERE label::int4 >= 20 ORDER BY id",
            &[],
        )
        .await
        .expect("select");
    let got: Vec<(i32, i32, i32)> = rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    // 3.5 rounds to 4 (half-to-even).
    assert_eq!(got, vec![(2, 20, 4)]);
    // int → text via a column.
    assert_eq!(
        text(&client, "SELECT id::text FROM c ORDER BY id").await,
        Some("1".into())
    );
}

#[tokio::test]
async fn error_surface() {
    let client = connect(spawn().await).await;
    // Bad text syntax for the target type is 22P02.
    assert_eq!(err_code(&client, "SELECT 'abc'::int4").await, "22P02");
    assert_eq!(err_code(&client, "SELECT '1.5'::int4").await, "22P02");
    // A well-formed but out-of-range value is 22003.
    assert_eq!(
        err_code(&client, "SELECT '99999999999'::int4").await,
        "22003"
    );
    assert_eq!(err_code(&client, "SELECT 3000000000::int4").await, "22003");
    // An undefined cast is 42846 (no float8→bool / bool→int8 cast in PostgreSQL).
    assert_eq!(err_code(&client, "SELECT 1.5::bool").await, "42846");
    assert_eq!(err_code(&client, "SELECT true::int8").await, "42846");
    // An unknown target type is 42704 (undefined_object), not a syntax error:
    // PostgreSQL parses the cast fine and fails resolving the type name.
    assert_eq!(err_code(&client, "SELECT 1::widget").await, "42704");
}

// ---- assignment-context implicit casts (INSERT VALUES / UPDATE SET) ----

#[tokio::test]
async fn insert_applies_assignment_casts_through_the_session_time_zone() {
    use assert2::assert;
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE h (mtime timestamp); SET TIME ZONE 'America/New_York'")
        .await
        .expect("create + set zone");
    // timestamptz value → timestamp column: the instant is rotated into the
    // session zone (2024-06-01 12:00 UTC = 08:00 EDT), matching PostgreSQL's
    // castcontext='a' timestamptz→timestamp entry.
    client
        .batch_execute("INSERT INTO h VALUES ('2024-06-01 12:00:00+00'::timestamptz)")
        .await
        .expect("assignment cast timestamptz -> timestamp");
    assert!(text(&client, "SELECT mtime FROM h").await == Some("2024-06-01 08:00:00".into()));
    // The pgbench_history shape: CURRENT_TIMESTAMP (timestamptz) into a
    // timestamp column must be accepted.
    client
        .batch_execute("INSERT INTO h VALUES (CURRENT_TIMESTAMP)")
        .await
        .expect("CURRENT_TIMESTAMP into timestamp column");
    // date → timestamp ('i'): midnight, no zone rotation.
    client
        .batch_execute("DELETE FROM h; INSERT INTO h VALUES ('2024-06-01'::date)")
        .await
        .expect("assignment cast date -> timestamp");
    assert!(text(&client, "SELECT mtime FROM h").await == Some("2024-06-01 00:00:00".into()));
}

#[tokio::test]
async fn insert_applies_assignment_casts_into_timestamptz_columns() {
    use assert2::assert;
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE h (at timestamptz); SET TIME ZONE 'America/New_York'")
        .await
        .expect("create + set zone");
    // timestamp → timestamptz ('i'): the wall-clock is interpreted in the
    // session zone (08:00 EDT = 12:00 UTC).
    client
        .batch_execute("INSERT INTO h VALUES ('2024-06-01 08:00:00'::timestamp)")
        .await
        .expect("assignment cast timestamp -> timestamptz");
    client
        .batch_execute("SET TIME ZONE 'UTC'")
        .await
        .expect("zone back to UTC");
    assert!(text(&client, "SELECT at FROM h").await == Some("2024-06-01 12:00:00+00".into()));
    // date → timestamptz ('i'): midnight in the (now UTC) session zone.
    client
        .batch_execute("DELETE FROM h; INSERT INTO h VALUES ('2024-06-01'::date)")
        .await
        .expect("assignment cast date -> timestamptz");
    assert!(text(&client, "SELECT at FROM h").await == Some("2024-06-01 00:00:00+00".into()));
}

#[tokio::test]
async fn update_set_applies_assignment_casts_through_the_session_time_zone() {
    use assert2::assert;
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE h (id int4, mtime timestamp); \
             INSERT INTO h VALUES (1, '2000-01-01 00:00:00'::timestamp); \
             SET TIME ZONE 'America/New_York'",
        )
        .await
        .expect("seed row + set zone");
    client
        .batch_execute("UPDATE h SET mtime = '2024-06-01 12:00:00+00'::timestamptz WHERE id = 1")
        .await
        .expect("UPDATE SET with a timestamptz expression into a timestamp column");
    assert!(text(&client, "SELECT mtime FROM h").await == Some("2024-06-01 08:00:00".into()));
}

#[tokio::test]
async fn assignment_context_keeps_io_conversion_casts_explicit_only() {
    use assert2::assert;
    let client = connect(spawn().await).await;
    client
        .batch_execute(
            "CREATE TABLE t (label text, n int4, mtime timestamp); \
             INSERT INTO t VALUES ('seed', 1, NULL)",
        )
        .await
        .expect("create + seed");
    // int → text and (typed) text → int are NOT assignment casts in
    // PostgreSQL — both keep erroring with 42804.
    assert!(err_code(&client, "INSERT INTO t (label) VALUES (42)").await == "42804");
    assert!(err_code(&client, "INSERT INTO t (n) VALUES ('12'::text)").await == "42804");
    assert!(err_code(&client, "UPDATE t SET label = 42").await == "42804");
    // Temporal truncation stays explicit-only in this conservative subset.
    assert!(err_code(&client, "INSERT INTO t (mtime) VALUES ('12:34:56'::time)").await == "42804");
}
