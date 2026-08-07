//! The system identifier types — `oid`, `xid`, `xid8`, `cid`, `tid` and
//! `pg_lsn` — end to end over the wire.
//!
//! The contract this file pins is the one an alias to `integer` cannot satisfy:
//! `oid` is **unsigned**, so `'4294967295'::oid` round-trips and `'-1'::oid` is
//! 4294967295 rather than -1; it reports OID 26 rather than 23; and its
//! comparison and ordering are unsigned throughout. Every expectation was taken
//! from the pinned `PostgreSQL` 18.4 build, including the ones that look like
//! bugs (`'010'::oid` is 8, `x(1,2)` is a valid `tid`).

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

/// Every column of the first row, as the engine's own text encoding.
async fn row(client: &tokio_postgres::Client, sql: &str) -> Vec<Option<String>> {
    for message in client.simple_query(sql).await.expect(sql) {
        if let SimpleQueryMessage::Row(row) = message {
            return (0..row.columns().len())
                .map(|i| row.get(i).map(str::to_string))
                .collect();
        }
    }
    panic!("no row for `{sql}`");
}

/// The first column of the first row.
async fn text(client: &tokio_postgres::Client, sql: &str) -> String {
    row(client, sql)
        .await
        .first()
        .cloned()
        .flatten()
        .unwrap_or_else(|| panic!("null first column for `{sql}`"))
}

/// `(SQLSTATE, message)` of the error `sql` raises.
async fn error(client: &tokio_postgres::Client, sql: &str) -> (String, String) {
    let error = client
        .simple_query(sql)
        .await
        .expect_err(sql)
        .as_db_error()
        .expect("db error")
        .clone();
    (error.code().code().to_string(), error.message().to_string())
}

/// `(SQLSTATE, message, HINT)` of the error `sql` raises.
async fn error_with_hint(
    client: &tokio_postgres::Client,
    sql: &str,
) -> (String, String, Option<String>) {
    let error = client
        .simple_query(sql)
        .await
        .expect_err(sql)
        .as_db_error()
        .expect("db error")
        .clone();
    (
        error.code().code().to_string(),
        error.message().to_string(),
        error.hint().map(str::to_string),
    )
}

/// `SELECT ...` run for effect, panicking on failure.
async fn run(client: &tokio_postgres::Client, sql: &str) {
    client.simple_query(sql).await.expect(sql);
}

#[tokio::test]
async fn oid_is_unsigned_not_an_integer_alias() {
    let client = connect(spawn().await).await;
    // The three statements from the bug report, plus the boundary either side.
    assert!(text(&client, "SELECT '4294967295'::oid").await == "4294967295");
    assert!(text(&client, "SELECT '-1'::oid").await == "4294967295");
    assert!(text(&client, "SELECT pg_typeof('1'::oid)").await == "oid");
    assert!(text(&client, "SELECT '2147483648'::oid").await == "2147483648");
    assert!(text(&client, "SELECT '-1040'::oid").await == "4294966256");
    // The value an `int4` alias would have accepted and mis-ordered.
    assert!(text(&client, "SELECT '4294967295'::oid > '1'::oid").await == "t");
    assert!(text(&client, "SELECT '-1'::oid > '2147483647'::oid").await == "t");
    // Out of range for `oid` proper, and named as `oid` rather than `integer`.
    assert!(
        error(&client, "SELECT '4294967296'::oid").await
            == (
                "22003".to_string(),
                "value \"4294967296\" is out of range for type oid".to_string()
            )
    );
    assert!(
        error(&client, "SELECT 'x'::oid").await
            == (
                "22P02".to_string(),
                "invalid input syntax for type oid: \"x\"".to_string()
            )
    );
}

#[tokio::test]
async fn every_type_reports_its_own_oid_and_name() {
    let client = connect(spawn().await).await;
    for (sql, name, oid) in [
        ("'1'::oid", "oid", 26_u32),
        ("'1'::xid", "xid", 28),
        ("'1'::xid8", "xid8", 5069),
        ("'1'::cid", "cid", 29),
        ("'(1,2)'::tid", "tid", 27),
        ("'0/1'::pg_lsn", "pg_lsn", 3220),
    ] {
        assert!(
            text(&client, &format!("SELECT pg_typeof({sql})")).await == name,
            "{sql}"
        );
        let described = client
            .query_one(&format!("SELECT {sql}"), &[])
            .await
            .unwrap_or_else(|_| panic!("{sql}"));
        assert!(described.columns()[0].type_().oid() == oid, "{sql}");
    }
    // The array oids `pg_type.typarray` records, which `format_type` prints.
    for (oid, spelling) in [
        (26, "oid"),
        (27, "tid"),
        (28, "xid"),
        (29, "cid"),
        (271, "xid8[]"),
        (1010, "tid[]"),
        (1011, "xid[]"),
        (1012, "cid[]"),
        (1028, "oid[]"),
        (3220, "pg_lsn"),
        (3221, "pg_lsn[]"),
        (5069, "xid8"),
    ] {
        assert!(
            text(&client, &format!("SELECT format_type({oid}, NULL)")).await == spelling,
            "format_type({oid})"
        );
    }
}

/// The input functions are `strtoul(s, &endptr, 0)` — base **zero** — which is
/// what makes a leading `0` octal and `0x` hexadecimal.
#[tokio::test]
async fn input_functions_read_base_zero() {
    let client = connect(spawn().await).await;
    for (literal, oid, xid8) in [
        ("0", "0", "0"),
        ("42", "42", "42"),
        ("010", "8", "8"),
        ("0x10", "16", "16"),
        ("0b101", "5", "5"),
        ("+5", "5", "5"),
        ("  5  ", "5", "5"),
        ("-1", "4294967295", "18446744073709551615"),
    ] {
        assert!(
            text(&client, &format!("SELECT $x${literal}$x$::oid")).await == oid,
            "oid {literal}"
        );
        assert!(
            text(&client, &format!("SELECT $x${literal}$x$::xid8")).await == xid8,
            "xid8 {literal}"
        );
    }
    // The three 32-bit types share one input routine and differ only in the
    // name their errors carry.
    for ty in ["oid", "xid", "cid"] {
        assert!(
            error(&client, &format!("SELECT '5    5'::{ty}")).await
                == (
                    "22P02".to_string(),
                    format!("invalid input syntax for type {ty}: \"5    5\"")
                ),
            "{ty}"
        );
        assert!(
            error(&client, &format!("SELECT '99999999999'::{ty}")).await
                == (
                    "22003".to_string(),
                    format!("value \"99999999999\" is out of range for type {ty}")
                ),
            "{ty}"
        );
    }
}

#[tokio::test]
async fn tid_and_pg_lsn_have_their_own_grammars() {
    let client = connect(spawn().await).await;
    for (literal, want) in [
        ("(0,0)", "(0,0)"),
        ("(-1,0)", "(4294967295,0)"),
        ("(4294967295,65535)", "(4294967295,65535)"),
        // `tidin` takes the first `(` wherever it sits and stops at `)`.
        ("x(1,2)", "(1,2)"),
        ("(1,2)junk", "(1,2)"),
        // It never checks that any digits converted, only the delimiter.
        ("(,)", "(0,0)"),
    ] {
        assert!(
            text(&client, &format!("SELECT $x${literal}$x$::tid")).await == want,
            "tid {literal}"
        );
    }
    for literal in ["(4294967296,1)", "(1,65536)", "(0)", "(0,-1)", "(1 , 2)"] {
        assert!(
            error(&client, &format!("SELECT $x${literal}$x$::tid"))
                .await
                .0
                == "22P02",
            "tid {literal}"
        );
    }
    for (literal, want) in [
        ("0/0", "0/0"),
        ("ffffffff/ffffffff", "FFFFFFFF/FFFFFFFF"),
        ("abc/DEF", "ABC/DEF"),
        ("0/16AE7F7", "0/16AE7F7"),
    ] {
        assert!(
            text(&client, &format!("SELECT '{literal}'::pg_lsn")).await == want,
            "pg_lsn {literal}"
        );
    }
    for literal in [
        "G/0",
        "-1/0",
        " 0/12345678",
        "ABCD/",
        "/ABCD",
        "123456789/1",
        "0/0 ",
    ] {
        assert!(
            error(&client, &format!("SELECT $x${literal}$x$::pg_lsn"))
                .await
                .0
                == "22P02",
            "pg_lsn {literal}"
        );
    }
}

/// `xid` and `cid` have equality but no B-tree opclass, so they sort nowhere.
/// `cid` does not even have `<>`.
#[tokio::test]
async fn xid_and_cid_have_equality_but_no_ordering() {
    let client = connect(spawn().await).await;
    assert!(text(&client, "SELECT '1'::xid = '1'::xid").await == "t");
    assert!(text(&client, "SELECT '1'::xid <> '2'::xid").await == "t");
    assert!(text(&client, "SELECT '1'::cid = '1'::cid").await == "t");
    // `IS DISTINCT FROM` resolves through `=`, which both have.
    assert!(text(&client, "SELECT '1'::cid IS DISTINCT FROM '2'::cid").await == "t");
    for (sql, message) in [
        (
            "SELECT '1'::xid < '2'::xid",
            "operator does not exist: xid < xid",
        ),
        (
            "SELECT '1'::xid <= '2'::xid",
            "operator does not exist: xid <= xid",
        ),
        (
            "SELECT '1'::xid > '2'::xid",
            "operator does not exist: xid > xid",
        ),
        (
            "SELECT '1'::xid >= '2'::xid",
            "operator does not exist: xid >= xid",
        ),
        (
            "SELECT '1'::cid <> '2'::cid",
            "operator does not exist: cid <> cid",
        ),
        (
            "SELECT '1'::cid < '2'::cid",
            "operator does not exist: cid < cid",
        ),
        // An `unknown` literal beside one adopts nothing, exactly as `json`'s
        // rejections name it.
        (
            "SELECT '1'::xid < '2'",
            "operator does not exist: xid < unknown",
        ),
    ] {
        assert!(
            error(&client, sql).await == ("42883".to_string(), message.to_string()),
            "{sql}"
        );
    }
    // Ordering and the B-tree-shaped aggregates are refused, while hashing —
    // DISTINCT, GROUP BY and the set operations — is not.
    let values = "(VALUES ('2'::xid), ('1'::xid), ('1'::xid)) t(v)";
    assert!(
        error_with_hint(&client, &format!("SELECT v FROM {values} ORDER BY v")).await
            == (
                "42883".to_string(),
                "could not identify an ordering operator for type xid".to_string(),
                // The one message in this family PostgreSQL hints on.
                Some("Use an explicit ordering operator or modify the query.".to_string()),
            )
    );
    assert!(
        error(&client, &format!("SELECT min(v) FROM {values}")).await
            == (
                "42883".to_string(),
                "function min(xid) does not exist".to_string()
            )
    );
    // `greatest`/`least` resolve through the comparison FUNCTION, and name that.
    assert!(
        error(&client, "SELECT greatest('1'::xid, '2'::xid)").await
            == (
                "42883".to_string(),
                "could not identify a comparison function for type xid".to_string()
            )
    );
    assert!(text(&client, &format!("SELECT count(DISTINCT v) FROM {values}")).await == "2");
    assert!(
        text(
            &client,
            &format!("SELECT count(*) FROM (SELECT v FROM {values} GROUP BY v) g")
        )
        .await
            == "2"
    );
    // The same asymmetry in DDL: a hash index over `xid` is fine, a B-tree is
    // not, and neither is the B-tree a UNIQUE constraint builds.
    run(&client, "CREATE TABLE xid_idx (x xid)").await;
    for sql in [
        "CREATE INDEX ON xid_idx USING btree (x)",
        "CREATE UNIQUE INDEX ON xid_idx (x)",
    ] {
        assert!(
            error(&client, sql).await
                == (
                    "42704".to_string(),
                    "data type xid has no default operator class for access method \"btree\""
                        .to_string()
                ),
            "{sql}"
        );
    }
    run(&client, "CREATE INDEX ON xid_idx USING hash (x)").await;
    run(&client, "DROP TABLE xid_idx").await;
}

/// `xid8`, `tid` and `pg_lsn` are fully ordered, so everything `xid` refuses
/// works for them.
#[tokio::test]
async fn the_ordered_members_sort_and_aggregate() {
    let client = connect(spawn().await).await;
    for (values, min, max) in [
        (
            "(VALUES ('0'::xid8), ('8'::xid8), ('18446744073709551615'::xid8))",
            "0",
            "18446744073709551615",
        ),
        (
            "(VALUES ('(1,0)'::tid), ('(0,5)'::tid), ('(1,4)'::tid))",
            "(0,5)",
            "(1,4)",
        ),
        (
            "(VALUES ('0/1'::pg_lsn), ('1/0'::pg_lsn), ('FFFFFFFF/FFFFFFFF'::pg_lsn))",
            "0/1",
            "FFFFFFFF/FFFFFFFF",
        ),
        (
            "(VALUES ('1'::oid), ('4294967295'::oid), ('2147483648'::oid))",
            "1",
            "4294967295",
        ),
    ] {
        let sql = format!("SELECT min(v)::text, max(v)::text FROM {values} t(v)");
        assert!(
            row(&client, &sql).await == vec![Some(min.to_string()), Some(max.to_string())],
            "{sql}"
        );
    }
    assert!(text(&client, "SELECT '1'::xid8 < '2'::xid8").await == "t");
    assert!(text(&client, "SELECT '(1,0)'::tid > '(0,5)'::tid").await == "t");
    assert!(text(&client, "SELECT '0/2'::pg_lsn > '0/1'::pg_lsn").await == "t");
    // `xid8cmp` is the three-way comparison the B-tree opclass exposes.
    assert!(
        row(
            &client,
            "SELECT xid8cmp('1', '2'), xid8cmp('2', '2'), xid8cmp('2', '1')"
        )
        .await
            == vec![
                Some("-1".to_string()),
                Some("0".to_string()),
                Some("1".to_string())
            ]
    );
}

#[tokio::test]
async fn pg_lsn_arithmetic_goes_through_numeric() {
    let client = connect(spawn().await).await;
    for (sql, want) in [
        ("SELECT '0/16AE7F7'::pg_lsn - '0/16AE7F8'::pg_lsn", "-1"),
        ("SELECT '0/16AE7F8'::pg_lsn - '0/16AE7F7'::pg_lsn", "1"),
        ("SELECT '0/16AE7F7'::pg_lsn + 16::numeric", "0/16AE807"),
        ("SELECT 16::numeric + '0/16AE7F7'::pg_lsn", "0/16AE807"),
        ("SELECT '0/16AE7F7'::pg_lsn - 16::numeric", "0/16AE7E7"),
        (
            "SELECT 'FFFFFFFF/FFFFFFFE'::pg_lsn + 1::numeric",
            "FFFFFFFF/FFFFFFFF",
        ),
        (
            "SELECT '0/0'::pg_lsn + ('FFFFFFFF/FFFFFFFF'::pg_lsn - '0/0'::pg_lsn)",
            "FFFFFFFF/FFFFFFFF",
        ),
        // `numericvar_to_uint64` rounds rather than truncating.
        ("SELECT '0/10'::pg_lsn + 1.7::numeric", "0/12"),
    ] {
        assert!(text(&client, sql).await == want, "{sql}");
    }
    for (sql, sqlstate, message) in [
        (
            "SELECT 'FFFFFFFF/FFFFFFFE'::pg_lsn + 2::numeric",
            "22023",
            "pg_lsn out of range",
        ),
        (
            "SELECT '0/1'::pg_lsn - 2::numeric",
            "22023",
            "pg_lsn out of range",
        ),
        (
            "SELECT '0/16AE7F7'::pg_lsn + 'NaN'::numeric",
            "0A000",
            "cannot add NaN to pg_lsn",
        ),
        (
            "SELECT '0/16AE7F7'::pg_lsn - 'NaN'::numeric",
            "0A000",
            "cannot subtract NaN from pg_lsn",
        ),
        (
            "SELECT '0/16AE7F7'::pg_lsn + 'Infinity'::numeric",
            "0A000",
            "cannot convert infinity to pg_lsn",
        ),
        // There is no `numeric - pg_lsn`; only `+` has a reflected form.
        (
            "SELECT 1::numeric - '0/1'::pg_lsn",
            "42883",
            "operator does not exist: numeric - pg_lsn",
        ),
    ] {
        assert!(
            error(&client, sql).await == (sqlstate.to_string(), message.to_string()),
            "{sql}"
        );
    }
    // The unknown literal beside `-` becomes a `pg_lsn` (the exact-match
    // candidate wins) and beside `+` a `numeric` (the only candidate).
    assert!(text(&client, "SELECT '0/16AE7F7'::pg_lsn + '16'").await == "0/16AE807");
    assert!(error(&client, "SELECT '0/16AE7F7'::pg_lsn - '16'").await.0 == "22P02");
}

/// The cast matrix is exactly `pg_cast`'s: `oid` interconverts with the integer
/// widths and the `reg*` types, `xid8 → xid` exists, and nothing else in the
/// family reaches another type except through its text form.
#[tokio::test]
async fn casts_are_only_the_ones_pg_cast_declares() {
    let client = connect(spawn().await).await;
    for (sql, want) in [
        // `int2`/`int4 → oid` are binary coercions: the bits are reinterpreted.
        ("SELECT (-1)::oid", "4294967295"),
        ("SELECT 1::int2::oid", "1"),
        ("SELECT 4294967295::oid::int4", "-1"),
        ("SELECT 4294967295::oid::int8", "4294967295"),
        ("SELECT '1'::oid::text", "1"),
        ("SELECT '1'::oid::regclass::oid", "1"),
        ("SELECT 4294967295::regclass::oid", "4294967295"),
        ("SELECT '1'::xid8::xid", "1"),
        ("SELECT '18446744073709551615'::xid8::xid", "4294967295"),
        ("SELECT '(1,2)'::tid::text", "(1,2)"),
        ("SELECT '0/1'::pg_lsn::text", "0/1"),
        // `pg_lsn(numeric)` is a function, not a cast.
        ("SELECT pg_lsn(1::numeric)", "0/1"),
    ] {
        assert!(text(&client, sql).await == want, "{sql}");
    }
    for (sql, message) in [
        ("SELECT '1'::oid::int2", "cannot cast type oid to smallint"),
        (
            "SELECT '1'::oid::numeric",
            "cannot cast type oid to numeric",
        ),
        (
            "SELECT 1.5::numeric::oid",
            "cannot cast type numeric to oid",
        ),
        (
            "SELECT 1.5::float8::oid",
            "cannot cast type double precision to oid",
        ),
        ("SELECT '1'::oid::bool", "cannot cast type oid to boolean"),
        ("SELECT '1'::oid::xid", "cannot cast type oid to xid"),
        ("SELECT '1'::xid::oid", "cannot cast type xid to oid"),
        ("SELECT '1'::xid::int4", "cannot cast type xid to integer"),
        ("SELECT '1'::xid::xid8", "cannot cast type xid to xid8"),
        ("SELECT '1'::xid8::int8", "cannot cast type xid8 to bigint"),
        ("SELECT '1'::cid::int4", "cannot cast type cid to integer"),
        (
            "SELECT '0/1'::pg_lsn::numeric",
            "cannot cast type pg_lsn to numeric",
        ),
        (
            "SELECT 1::numeric::pg_lsn",
            "cannot cast type numeric to pg_lsn",
        ),
    ] {
        assert!(
            error(&client, sql).await == ("42846".to_string(), message.to_string()),
            "{sql}"
        );
    }
    // `int8 → oid` range-checks against the UNSIGNED range.
    assert!(
        error(&client, "SELECT 4294967296::oid").await
            == ("22003".to_string(), "OID out of range".to_string())
    );
}

/// The negative half of the corpus: every operator and function added here must
/// refuse the argument types `PostgreSQL` refuses, with its own message.
#[tokio::test]
async fn the_new_operators_reject_unrelated_types() {
    let client = connect(spawn().await).await;
    for (sql, message) in [
        // `oid`'s comparison partners are the integer widths and `reg*` only.
        (
            "SELECT '1'::oid = 'x'::text",
            "operator does not exist: oid = text",
        ),
        (
            "SELECT '1'::oid = 1.5::float8",
            "operator does not exist: oid = double precision",
        ),
        (
            "SELECT '1'::oid = 1.5::numeric",
            "operator does not exist: oid = numeric",
        ),
        (
            "SELECT '1'::oid = true",
            "operator does not exist: oid = boolean",
        ),
        (
            "SELECT '1'::oid = '1'::xid",
            "operator does not exist: oid = xid",
        ),
        // `xideqint4` has no commutator, so the integer must be on the right.
        (
            "SELECT 1::int4 = '1'::xid",
            "operator does not exist: integer = xid",
        ),
        (
            "SELECT '1'::xid = 1::int8",
            "operator does not exist: xid = bigint",
        ),
        (
            "SELECT '1'::xid = 'x'::text",
            "operator does not exist: xid = text",
        ),
        (
            "SELECT '1'::xid8 = 1::int4",
            "operator does not exist: xid8 = integer",
        ),
        (
            "SELECT '1'::cid = 1::int4",
            "operator does not exist: cid = integer",
        ),
        (
            "SELECT '(1,2)'::tid = 'x'::text",
            "operator does not exist: tid = text",
        ),
        (
            "SELECT '0/1'::pg_lsn = 1::int4",
            "operator does not exist: pg_lsn = integer",
        ),
        // No arithmetic outside `pg_lsn`, and no unary minus anywhere.
        (
            "SELECT '1'::oid + 1",
            "operator does not exist: oid + integer",
        ),
        (
            "SELECT '1'::oid * '1'::oid",
            "operator does not exist: oid * oid",
        ),
        ("SELECT -'1'::oid", "operator does not exist: - oid"),
        ("SELECT +'1'::oid", "operator does not exist: + oid"),
        ("SELECT -'0/1'::pg_lsn", "operator does not exist: - pg_lsn"),
        ("SELECT -'(1,2)'::tid", "operator does not exist: - tid"),
    ] {
        assert!(
            error(&client, sql).await == ("42883".to_string(), message.to_string()),
            "{sql}"
        );
    }
    // The functions this slice adds must not resolve for other types.
    for sql in [
        "SELECT xid8cmp('x'::text, 'y'::text)",
        "SELECT xid8cmp(1::int4, 2::int4)",
        "SELECT xid('1'::xid)",
        "SELECT xid(1::int4)",
        "SELECT pg_lsn('x'::text)",
        "SELECT pg_lsn_cmp(1::int4, 2::int4)",
    ] {
        assert!(error(&client, sql).await.0 == "42883", "{sql}");
    }
    // The comparison partners PostgreSQL DOES declare still resolve, so the
    // rejection above is not a blanket ban.
    assert!(text(&client, "SELECT '1'::oid = 1::int4").await == "t");
    assert!(text(&client, "SELECT '1'::oid = 1::int8").await == "t");
    assert!(text(&client, "SELECT '1'::oid = 1::int2").await == "t");
    assert!(text(&client, "SELECT '4294967295'::oid = (-1)::int4").await == "t");
    assert!(text(&client, "SELECT '1'::xid = 1").await == "t");
    assert!(text(&client, "SELECT '1'::xid = 1::int2").await == "t");
}

/// Round-tripping through a table proves the row encoding keeps the unsigned
/// value, which storing an `oid` under the `int4` tag would not.
#[tokio::test]
async fn values_round_trip_through_storage() {
    let client = connect(spawn().await).await;
    run(
        &client,
        "CREATE TABLE sysid (o oid, x xid, x8 xid8, c cid, t tid, l pg_lsn)",
    )
    .await;
    run(
        &client,
        "INSERT INTO sysid VALUES ('4294967295', '-1', '-1', '4294967295', \
         '(4294967295,65535)', 'FFFFFFFF/FFFFFFFF')",
    )
    .await;
    run(
        &client,
        "INSERT INTO sysid VALUES ('1', '1', '1', '1', '(0,1)', '0/1')",
    )
    .await;
    assert!(
        row(&client, "SELECT * FROM sysid WHERE o = '4294967295'").await
            == vec![
                Some("4294967295".to_string()),
                Some("4294967295".to_string()),
                Some("18446744073709551615".to_string()),
                Some("4294967295".to_string()),
                Some("(4294967295,65535)".to_string()),
                Some("FFFFFFFF/FFFFFFFF".to_string()),
            ]
    );
    // The column types survive into `format_type`, so `\d` renders them.
    assert!(
        row(
            &client,
            "SELECT format_type(atttypid, atttypmod) FROM pg_attribute \
             WHERE attrelid = 'sysid'::regclass AND attname = 'o'"
        )
        .await
            == vec![Some("oid".to_string())]
    );
    // Unsigned ordering through the storage layer, not just in memory.
    assert!(text(&client, "SELECT max(o)::text FROM sysid").await == "4294967295");
    assert!(text(&client, "SELECT count(*) FROM sysid WHERE o > '1'::oid").await == "1");
    run(&client, "DROP TABLE sysid").await;
}

/// Binary-format parameters and results use each type's `*_recv` / `*_send`
/// representation.
#[tokio::test]
async fn binary_format_matches_the_send_functions() {
    let client = connect(spawn().await).await;
    let row = client
        .query_one(
            "SELECT '4294967295'::oid::text, '18446744073709551615'::xid8::text, \
             '(1,2)'::tid::text, '1/2'::pg_lsn::text",
            &[],
        )
        .await
        .expect("binary query");
    let values: Vec<&str> = (0..4).map(|i| row.get::<_, &str>(i)).collect();
    assert!(values == vec!["4294967295", "18446744073709551615", "(1,2)", "1/2"]);
}

/// Changing `oid` from an `int4` alias must leave catalog introspection alone.
#[tokio::test]
async fn catalog_introspection_still_resolves() {
    let client = connect(spawn().await).await;
    run(&client, "CREATE TABLE cat_probe (a int, b text)").await;
    // The `oid`-valued catalog columns still compare against literals, against
    // `regclass` and against each other.
    assert!(
        text(
            &client,
            "SELECT relname FROM pg_class WHERE oid = 'cat_probe'::regclass"
        )
        .await
            == "cat_probe"
    );
    // `pg_type.typname` is the internal name, so oid 23 is `int4`, not
    // `integer` — the spelling `format_type` produces.
    assert!(text(&client, "SELECT typname FROM pg_type WHERE oid = 23").await == "int4");
    assert!(
        text(
            &client,
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relname = 'cat_probe'"
        )
        .await
            == "cat_probe"
    );
    assert!(
        text(
            &client,
            "SELECT count(*)::text FROM pg_attribute WHERE attrelid = 'cat_probe'::regclass AND attnum > 0"
        )
        .await
            == "2"
    );
    // `pg_type` now carries the six types, at PostgreSQL's own oids.
    for (oid, typname) in [
        (26, "oid"),
        (27, "tid"),
        (28, "xid"),
        (29, "cid"),
        (3220, "pg_lsn"),
        (5069, "xid8"),
    ] {
        assert!(
            text(
                &client,
                &format!("SELECT typname FROM pg_type WHERE oid = {oid}")
            )
            .await
                == typname,
            "pg_type {oid}"
        );
    }
    run(&client, "DROP TABLE cat_probe").await;
}
