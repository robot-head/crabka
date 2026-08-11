//! Every built-in crabka can name has a `pg_type` row of its own.
//!
//! A type with no row is not merely undescribed. `pg_type` is what the column
//! label, the `typarray` link, the domain's inherited category and the
//! reserved-name check are all read out of, so a missing row makes each of
//! those quietly answer something else — a label falls back to the SQL spelling
//! (`time with time zone` where `PostgreSQL` writes `timetz`), and a name the
//! system owns looks free.
//!
//! Every expected value here is `PostgreSQL` 18.4's own `pg_type`.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn session() -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let s = engine.connect();
    (engine, s)
}

fn cell_text(cell: Option<&Cell>) -> Option<String> {
    cell.map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
}

async fn rows(s: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    let results = s
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e:?}"));
    match &results[0] {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| row.iter().map(|c| cell_text(c.as_ref())).collect())
            .collect(),
        other => panic!("`{sql}` should return rows, got {other:?}"),
    }
}

/// The name a `RowDescription` gives the single column of `SELECT <expr>`.
async fn column_label(s: &mut SqlSession, expr: &str) -> String {
    let sql = format!("SELECT {expr}");
    let results = s
        .simple_query(&sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e:?}"));
    match &results[0] {
        QueryResult::Rows { fields, .. } => fields[0].name.clone(),
        other => panic!("`{sql}` should return rows, got {other:?}"),
    }
}

/// The SQLSTATE and message a statement that must fail reports.
async fn err(s: &mut SqlSession, sql: &str) -> (String, String) {
    let e = s
        .simple_query(sql)
        .await
        .expect_err("statement should fail");
    (e.code, e.message)
}

/// One `pg_type` row, in the column order the query below selects.
fn row(
    typname: &str,
    typlen: i32,
    typcategory: &str,
    typelem: u32,
    typarray: u32,
) -> Vec<Option<String>> {
    vec![
        Some(typname.to_owned()),
        Some(typlen.to_string()),
        Some(typcategory.to_owned()),
        Some(typelem.to_string()),
        Some(typarray.to_string()),
    ]
}

async fn pg_type_row(s: &mut SqlSession, oid: u32) -> Vec<Vec<Option<String>>> {
    rows(
        s,
        &format!(
            "SELECT typname, typlen, typcategory, typelem, typarray \
             FROM pg_type WHERE oid = {oid}"
        ),
    )
    .await
}

// ---------------------------------------------------------------------------
// The rows
// ---------------------------------------------------------------------------

/// Every datetime type — `timetz` included, which had no row at all — describes
/// itself in `pg_type` exactly as `PostgreSQL` does.
#[tokio::test]
async fn every_datetime_type_has_its_postgresql_pg_type_row() {
    let (_engine, mut s) = session();

    let cases: &[(u32, Vec<Option<String>>)] = &[
        (1082, row("date", 4, "D", 0, 1182)),
        (1083, row("time", 8, "D", 0, 1183)),
        (1266, row("timetz", 12, "D", 0, 1270)),
        (1114, row("timestamp", 8, "D", 0, 1115)),
        (1184, row("timestamptz", 8, "D", 0, 1185)),
        (1186, row("interval", 16, "T", 0, 1187)),
    ];
    for (oid, want) in cases {
        assert!(
            pg_type_row(&mut s, *oid).await == vec![want.clone()],
            "oid {oid}"
        );
    }
}

/// The two other built-ins that had no row, and the array rows the scalars'
/// `typarray` now points at. A `typarray` aimed at a row that is not there is
/// the dangling link `type_sanity` exists to catch.
#[tokio::test]
async fn the_remaining_row_less_builtins_and_their_array_types_are_present() {
    let (_engine, mut s) = session();

    let cases: &[(u32, Vec<Option<String>>)] = &[
        (22, row("int2vector", -1, "A", 21, 1006)),
        (1006, row("_int2vector", -1, "A", 22, 0)),
        (1270, row("_timetz", -1, "A", 1266, 0)),
    ];
    for (oid, want) in cases {
        assert!(
            pg_type_row(&mut s, *oid).await == vec![want.clone()],
            "oid {oid}"
        );
    }
}

/// The lookup every client driver caches its type table from. A type with no
/// row answers it with nothing, and a driver that finds no `timetz` has no oid
/// to bind a `timetz` parameter with.
#[tokio::test]
async fn a_driver_can_look_every_datetime_type_up_by_name() {
    let (_engine, mut s) = session();

    let cases: &[(&str, u32)] = &[
        ("date", 1082),
        ("time", 1083),
        ("timetz", 1266),
        ("timestamp", 1114),
        ("timestamptz", 1184),
        ("interval", 1186),
        ("int2vector", 22),
    ];
    for (name, oid) in cases {
        assert!(
            rows(
                &mut s,
                &format!("SELECT oid FROM pg_type WHERE typname = '{name}'"),
            )
            .await
                == vec![vec![Some(oid.to_string())]],
            "for {name}"
        );
    }
}

/// A domain takes its `typcategory` from its base type's `pg_type` row. With no
/// row to read, the base fell back to 'U' — the category a `CREATE DOMAIN` over
/// a datetime type must never report.
#[tokio::test]
async fn a_domain_inherits_its_base_datetime_types_category() {
    let (_engine, mut s) = session();

    let cases: &[(&str, &str)] = &[
        ("date", "D"),
        ("time", "D"),
        ("timetz", "D"),
        ("timestamp", "D"),
        ("timestamptz", "D"),
        ("interval", "T"),
    ];
    for (base, want) in cases {
        let domain = format!("dom_{base}");
        s.simple_query(&format!("CREATE DOMAIN {domain} AS {base}"))
            .await
            .unwrap_or_else(|e| panic!("CREATE DOMAIN over {base}: {e:?}"));
        assert!(
            rows(
                &mut s,
                &format!("SELECT typcategory FROM pg_type WHERE typname = '{domain}'"),
            )
            .await
                == vec![vec![Some((*want).to_owned())]],
            "for {base}"
        );
    }
}

// ---------------------------------------------------------------------------
// What the rows decide
// ---------------------------------------------------------------------------

/// `FigureColname` labels a bare cast after the catalog `typname`, not after
/// the SQL spelling. Without a `pg_type` row the label fell back to the SQL
/// spelling, which is how `timetz` came out as `time with time zone`.
#[tokio::test]
async fn a_bare_cast_is_labelled_with_the_catalog_typname() {
    let (_engine, mut s) = session();

    let cases: &[(&str, &str)] = &[
        ("'2020-05-26'::date", "date"),
        ("'13:30:25'::time", "time"),
        ("'13:30:25-04'::timetz", "timetz"),
        ("'2020-05-26 13:30:25'::timestamp", "timestamp"),
        ("'2020-05-26 13:30:25+00'::timestamptz", "timestamptz"),
        ("'2 days'::interval", "interval"),
        // The SQL spellings resolve to the same types and take the same labels.
        ("'13:30:25-04'::time with time zone", "timetz"),
        (
            "'2020-05-26 13:30:25+00'::timestamp with time zone",
            "timestamptz",
        ),
    ];
    for (expr, want) in cases {
        assert!(column_label(&mut s, expr).await == *want, "for {expr}");
    }
}

/// A name `pg_catalog` owns cannot be dropped. The check reads `pg_type`, so a
/// type with no row looked like a name nobody had claimed.
#[tokio::test]
async fn no_datetime_type_can_be_dropped_out_of_pg_catalog() {
    let (_engine, mut s) = session();

    for name in [
        "date",
        "time",
        "timetz",
        "timestamp",
        "timestamptz",
        "interval",
        "int2vector",
    ] {
        let (code, message) = err(&mut s, &format!("DROP TYPE pg_catalog.{name}")).await;
        assert!(
            (
                code.as_str(),
                message.contains("required by the database system")
            ) == ("2BP01", true),
            "for {name}: {code} {message}"
        );
    }
}
