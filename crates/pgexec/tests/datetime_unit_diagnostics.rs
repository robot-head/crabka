//! What `EXTRACT`, `date_part` and `date_trunc` say about a unit they will not
//! compute, and which SQLSTATE they say it under.
//!
//! `PostgreSQL` keeps two conditions apart here, and the wording is not the only
//! difference between them:
//!
//! * a unit it has never heard of (`fortnight`) is 22023, and
//! * a real unit the source type has no value for (`day` of a `time`) is 0A000.
//!
//! Both messages name the source type as `format_type_be` spells it, so the
//! same rejected unit reads differently over a `time` and over a `timetz`.
//! Every expected string here is `PostgreSQL` 18.4's, from `date.c` and
//! `timestamp.c`.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

async fn session() -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    // The `timestamptz` cases below read a wall clock, so pin the zone.
    s.simple_query("SET TimeZone = 'UTC'")
        .await
        .expect("SET TimeZone");
    (engine, s)
}

fn cell_text(cell: Option<&Cell>) -> Option<String> {
    cell.map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
}

/// The SQLSTATE, message and DETAIL of a statement that must fail, as one value
/// so that a wrong SQLSTATE cannot pass on the strength of a right message.
async fn failure(s: &mut SqlSession, sql: &str) -> (String, String, Option<String>) {
    let e = s
        .simple_query(sql)
        .await
        .expect_err(&format!("`{sql}` should fail"));
    (e.code, e.message, e.diagnostics.and_then(|d| d.detail))
}

async fn scalar(s: &mut SqlSession, sql: &str) -> Option<String> {
    let results = s
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e:?}"));
    match &results[0] {
        QueryResult::Rows { rows, .. } => cell_text(rows[0][0].as_ref()),
        other => panic!("`{sql}` should return rows, got {other:?}"),
    }
}

/// A written value of each of the six datetime types, with the name
/// `format_type_be` gives that type.
const SOURCES: &[(&str, &str)] = &[
    ("DATE '2020-05-26'", "date"),
    ("TIME '13:30:25.575401'", "time without time zone"),
    (
        "TIME WITH TIME ZONE '13:30:25.575401-04'",
        "time with time zone",
    ),
    (
        "TIMESTAMP '2020-05-26 13:30:25.575401'",
        "timestamp without time zone",
    ),
    (
        "TIMESTAMP WITH TIME ZONE '2020-05-26 13:30:25.575401-04'",
        "timestamp with time zone",
    ),
    ("INTERVAL '2 days'", "interval"),
];

// ---------------------------------------------------------------------------
// The unit nobody has heard of
// ---------------------------------------------------------------------------

/// 22023, and the message names the source type. Reporting the unit alone made
/// all six of these read the same, which is what let the wrong SQLSTATE hide
/// behind them.
#[tokio::test]
async fn an_unrecognised_extract_unit_is_22023_and_names_the_source_type() {
    let (_engine, mut s) = session().await;

    for (source, type_name) in SOURCES {
        let want = (
            "22023".to_owned(),
            format!("unit \"fortnight\" not recognized for type {type_name}"),
            None,
        );
        assert!(
            failure(&mut s, &format!("SELECT EXTRACT(FORTNIGHT FROM {source})")).await == want,
            "for {source}"
        );
        // `date_part` is the same computation reached through a different
        // grammar, and reports the same way.
        assert!(
            failure(&mut s, &format!("SELECT date_part('fortnight', {source})")).await == want,
            "for date_part over {source}"
        );
    }
}

/// The same condition from `date_trunc`. A `date` argument is the odd one:
/// `PostgreSQL` has no `date_trunc(text, date)`, so the call is resolved after
/// a coercion to `timestamptz` and reports the type it was coerced to.
#[tokio::test]
async fn an_unrecognised_date_trunc_unit_is_22023_and_names_the_resolved_type() {
    let (_engine, mut s) = session().await;

    let cases: &[(&str, &str)] = &[
        ("DATE '2020-05-26'", "timestamp with time zone"),
        (
            "TIMESTAMP '2020-05-26 13:30:25'",
            "timestamp without time zone",
        ),
        (
            "TIMESTAMP WITH TIME ZONE '2020-05-26 13:30:25-04'",
            "timestamp with time zone",
        ),
        ("INTERVAL '2 days'", "interval"),
    ];
    for (source, type_name) in cases {
        assert!(
            failure(&mut s, &format!("SELECT date_trunc('ago', {source})")).await
                == (
                    "22023".to_owned(),
                    format!("unit \"ago\" not recognized for type {type_name}"),
                    None
                ),
            "for {source}"
        );
    }
}

// ---------------------------------------------------------------------------
// The real unit this type has no value for
// ---------------------------------------------------------------------------

/// 0A000, not 22023: `PostgreSQL` raises `ERRCODE_FEATURE_NOT_SUPPORTED` for a
/// unit it recognises and cannot compute. The two conditions have to stay
/// apart, or naming the type in one of them is worth nothing.
#[tokio::test]
async fn a_recognised_but_uncomputable_extract_unit_stays_0a000() {
    let (_engine, mut s) = session().await;

    let cases: &[(&str, &str, &str)] = &[
        ("EXTRACT(HOUR FROM DATE '2020-05-26')", "hour", "date"),
        (
            "EXTRACT(DAY FROM TIME '13:30:25')",
            "day",
            "time without time zone",
        ),
        (
            "EXTRACT(DAY FROM TIME WITH TIME ZONE '13:30:25-04')",
            "day",
            "time with time zone",
        ),
        (
            "EXTRACT(TIMEZONE FROM TIMESTAMP '2020-05-26 13:30:25')",
            "timezone",
            "timestamp without time zone",
        ),
        // `week` is deliberately NOT here. PostgreSQL computes it for an
        // `interval` (days / 7); `timezone` is what an `interval` genuinely
        // has no value for, having no zone to report.
        (
            "EXTRACT(TIMEZONE FROM INTERVAL '2 days')",
            "timezone",
            "interval",
        ),
    ];
    for (expr, unit, type_name) in cases {
        assert!(
            failure(&mut s, &format!("SELECT {expr}")).await
                == (
                    "0A000".to_owned(),
                    format!("unit \"{unit}\" not supported for type {type_name}"),
                    None
                ),
            "for {expr}"
        );
    }
}

/// `date_trunc`'s side of the same rule, including the one unit `PostgreSQL`
/// explains itself over: `week` of an `interval`, where months are stored apart
/// from days and no month is a whole number of weeks.
#[tokio::test]
async fn a_recognised_but_uncomputable_date_trunc_unit_stays_0a000() {
    let (_engine, mut s) = session().await;

    let cases: &[(&str, &str, Option<&str>)] = &[
        (
            "date_trunc('timezone', TIMESTAMP '2020-05-26 13:30:25')",
            "unit \"timezone\" not supported for type timestamp without time zone",
            None,
        ),
        (
            "date_trunc('timezone', TIMESTAMP WITH TIME ZONE '2020-05-26 13:30:25-04')",
            "unit \"timezone\" not supported for type timestamp with time zone",
            None,
        ),
        (
            "date_trunc('week', INTERVAL '2 days')",
            "unit \"week\" not supported for type interval",
            Some("Months usually have fractional weeks."),
        ),
    ];
    for (expr, message, detail) in cases {
        assert!(
            failure(&mut s, &format!("SELECT {expr}")).await
                == (
                    "0A000".to_owned(),
                    (*message).to_owned(),
                    detail.map(str::to_owned)
                ),
            "for {expr}"
        );
    }
}

// ---------------------------------------------------------------------------
// The units that do work
// ---------------------------------------------------------------------------

/// Refusing every unit would satisfy the tests above. These are the answers the
/// same six sources give for a unit each of them does have.
#[tokio::test]
async fn every_source_type_still_computes_a_unit_it_has() {
    let (_engine, mut s) = session().await;

    let cases: &[(&str, &str)] = &[
        ("EXTRACT(DAY FROM DATE '2020-05-26')", "26"),
        ("EXTRACT(HOUR FROM TIME '13:30:25')", "13"),
        ("EXTRACT(HOUR FROM TIME WITH TIME ZONE '13:30:25-04')", "13"),
        ("EXTRACT(DAY FROM TIMESTAMP '2020-05-26 13:30:25')", "26"),
        (
            "EXTRACT(YEAR FROM TIMESTAMP WITH TIME ZONE '2020-05-26 13:30:25-04')",
            "2020",
        ),
        ("EXTRACT(DAY FROM INTERVAL '2 days')", "2"),
        ("date_part('day', DATE '2020-05-26')", "26"),
        (
            "date_trunc('hour', TIMESTAMP '2020-05-26 13:30:25')",
            "2020-05-26 13:00:00",
        ),
        ("date_trunc('day', INTERVAL '2 days 03:30:00')", "2 days"),
    ];
    for (expr, want) in cases {
        assert!(
            scalar(&mut s, &format!("SELECT {expr}")).await == Some((*want).to_owned()),
            "for {expr}"
        );
    }
}
