//! SP33: math and string functions, end to end over the wire. These tests cover
//! the rounding family `floor`, `ceil`, `round`, `trunc` and `sign`, the
//! transcendental family `sqrt`, `power`, `exp`, `ln`, `log` and `pi`, the
//! string family `lpad`, `rpad`, `left`, `right`, `repeat`, `reverse`, `strpos`,
//! `initcap`, `ascii` and `chr`, the result type OIDs, and the domain-error
//! SQLSTATEs 2201E, 2201F and 54000.

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

async fn scalar(client: &tokio_postgres::Client, sql: &str) -> Option<String> {
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
async fn math_and_string_functions_over_the_wire() {
    let port = spawn().await;
    let client = connect(port).await;

    // rounding family — type-preserving text output
    assert_eq!(
        scalar(&client, "SELECT floor(2.9)").await.as_deref(),
        Some("2")
    );
    assert_eq!(
        scalar(&client, "SELECT ceil(2.1)").await.as_deref(),
        Some("3")
    );
    assert_eq!(
        scalar(&client, "SELECT round(2.567, 2)").await.as_deref(),
        Some("2.57")
    );
    assert_eq!(
        scalar(&client, "SELECT trunc(2.99)").await.as_deref(),
        Some("2")
    );
    assert_eq!(
        scalar(&client, "SELECT sign(-5)").await.as_deref(),
        Some("-1")
    );
    assert_eq!(
        scalar(&client, "SELECT round(2.5::float8)")
            .await
            .as_deref(),
        Some("2")
    );

    // transcendental — float8
    assert_eq!(
        scalar(&client, "SELECT sqrt(4)").await.as_deref(),
        Some("2")
    );
    assert_eq!(
        scalar(&client, "SELECT power(2, 10)").await.as_deref(),
        Some("1024")
    );
    assert_eq!(scalar(&client, "SELECT ln(1)").await.as_deref(), Some("0"));
    assert_eq!(
        scalar(&client, "SELECT log(1000)").await.as_deref(),
        Some("3")
    );

    // string
    assert_eq!(
        scalar(&client, "SELECT lpad('hi', 5, '*')")
            .await
            .as_deref(),
        Some("***hi")
    );
    assert_eq!(
        scalar(&client, "SELECT rpad('hi', 5, 'ab')")
            .await
            .as_deref(),
        Some("hiaba")
    );
    assert_eq!(
        scalar(&client, "SELECT left('abcdef', 2)").await.as_deref(),
        Some("ab")
    );
    assert_eq!(
        scalar(&client, "SELECT right('abcdef', -2)")
            .await
            .as_deref(),
        Some("cdef")
    );
    assert_eq!(
        scalar(&client, "SELECT repeat('ab', 3)").await.as_deref(),
        Some("ababab")
    );
    assert_eq!(
        scalar(&client, "SELECT reverse('abc')").await.as_deref(),
        Some("cba")
    );
    assert_eq!(
        scalar(&client, "SELECT initcap('hello world')")
            .await
            .as_deref(),
        Some("Hello World")
    );
    assert_eq!(
        scalar(&client, "SELECT strpos('abcde', 'cd')")
            .await
            .as_deref(),
        Some("3")
    );
    assert_eq!(
        scalar(&client, "SELECT ascii('A')").await.as_deref(),
        Some("65")
    );
    assert_eq!(
        scalar(&client, "SELECT chr(65)").await.as_deref(),
        Some("A")
    );

    // domain errors
    assert_eq!(err_code(&client, "SELECT sqrt(-1)").await, "2201F");
    assert_eq!(err_code(&client, "SELECT ln(0)").await, "2201E");
    assert_eq!(err_code(&client, "SELECT power(0, -1)").await, "2201F");
    assert_eq!(err_code(&client, "SELECT chr(0)").await, "54000");
    assert_eq!(
        err_code(&client, "SELECT round(2.5::float8, 1)").await,
        "42883"
    );
}

#[tokio::test]
async fn function_result_type_oids() {
    let port = spawn().await;
    let client = connect(port).await;
    // sqrt → float8 (OID 701); floor(numeric) → numeric (1700); ascii → int4 (23).
    let rows = client
        .query("SELECT sqrt(4), floor(2.5), ascii('A')", &[])
        .await
        .expect("q");
    assert_eq!(rows[0].columns()[0].type_().oid(), 701);
    assert_eq!(rows[0].columns()[1].type_().oid(), 1700);
    assert_eq!(rows[0].columns()[2].type_().oid(), 23);
}

#[tokio::test]
async fn numeric_transcendentals_over_the_wire() {
    let port = spawn().await;
    let client = connect(port).await;
    // SP34: numeric input -> numeric output, at PostgreSQL's display scale (text protocol).
    assert_eq!(
        scalar(&client, "SELECT sqrt(2::numeric)").await.as_deref(),
        Some("1.414213562373095")
    );
    assert_eq!(
        scalar(&client, "SELECT ln(2::numeric)").await.as_deref(),
        Some("0.6931471805599453")
    );
    assert_eq!(
        scalar(&client, "SELECT exp(1::numeric)").await.as_deref(),
        Some("2.7182818284590452")
    );
    assert_eq!(
        scalar(&client, "SELECT power(2::numeric, 100::numeric)")
            .await
            .as_deref(),
        Some("1267650600228229401496703205376")
    );
    // result OID: numeric (1700) for numeric input, float8 (701) for int input.
    let rows = client
        .query("SELECT sqrt(2::numeric), sqrt(4)", &[])
        .await
        .expect("q");
    assert_eq!(rows[0].columns()[0].type_().oid(), 1700);
    assert_eq!(rows[0].columns()[1].type_().oid(), 701);
}

/// `power` is the only two-argument member of this family with BOTH a `float8`
/// and a `numeric` candidate, so an `unknown` literal beside a typed operand
/// decides which one the call resolves to. `PostgreSQL` keeps the candidate that
/// needs no coercion of the typed argument, and the choice is visible in the
/// answer: the `numeric` overload knows `(-2) ^ Infinity`, while the `float8`
/// one refuses it as a complex result.
#[tokio::test]
async fn power_resolves_an_unknown_argument_to_the_typed_operand() {
    use assert2::assert;
    let port = spawn().await;
    let client = connect(port).await;

    let cases: &[(&str, &str)] = &[
        ("SELECT pg_typeof(power('-2'::numeric, '3'))", "numeric"),
        ("SELECT pg_typeof(power('3', '-2'::numeric))", "numeric"),
        (
            "SELECT pg_typeof(power('2'::float8, '3'))",
            "double precision",
        ),
        // Neither operand is typed, so the preferred `float8` candidate wins.
        ("SELECT pg_typeof(power('2', '3'))", "double precision"),
        ("SELECT power('-2'::numeric, '3')", "-8.0000000000000000"),
        ("SELECT power('-2'::numeric, 'inf')", "Infinity"),
        ("SELECT power('-2'::numeric, '-inf')", "0"),
        ("SELECT power('-1'::numeric, 'inf')", "1"),
        ("SELECT power('-inf'::numeric, '2')", "Infinity"),
        ("SELECT power('-inf'::numeric, '3')", "-Infinity"),
        ("SELECT power('-inf'::numeric, '-3')", "0"),
        // `numeric_inc(x)` is `x + 1`, specials included.
        ("SELECT numeric_inc('4.2'::numeric)", "5.2"),
        ("SELECT numeric_inc('inf'::numeric)", "Infinity"),
        ("SELECT pg_typeof(numeric_inc(1::numeric))", "numeric"),
        // `to_number` reads a Roman numeral, stopping at the first non-numeral.
        ("SELECT to_number('CvIiI', 'rn')", "108"),
        ("SELECT to_number('  XIV  ', '  RN')", "14"),
        ("SELECT to_number('M CC', 'RN')", "1000"),
    ];
    for (sql, want) in cases {
        assert!(
            scalar(&client, sql).await.as_deref() == Some(*want),
            "{sql}"
        );
    }

    // A malformed numeral and the two template combinations PostgreSQL refuses.
    assert!(err_code(&client, "SELECT to_number('viv', 'RN')").await == "22P02");
    assert!(err_code(&client, "SELECT to_number('CM', 'MIRN')").await == "42601");
    assert!(err_code(&client, "SELECT to_number('CM', 'RNRN')").await == "42601");
    // Running out of `numeric` digits is a format overflow, not an integer one.
    assert!(err_code(&client, "SELECT factorial(100000)").await == "22003");
}
