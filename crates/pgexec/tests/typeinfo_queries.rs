//! Driver type-lookup (typeinfo) queries against the synthesized catalog.
//!
//! When tokio-postgres meets an unknown type OID it prepares a catalog query
//! that LEFT JOINs `pg_catalog.pg_range`, and on `UNDEFINED_TABLE` falls back
//! to a variant that casts `NULL::OID`. Both must parse and execute: `pg_range`
//! is a built-in zero-row catalog view and `oid` is a type-name alias for
//! `int4`. Query shapes mirror `TYPEINFO_QUERY` / `TYPEINFO_FALLBACK_QUERY` in
//! tokio-postgres 0.7.18's `src/prepare.rs`, with `$1` written as the literal
//! 20 (`int8`) and the projection restricted to the columns `pg_type` defines
//! today — the verbatim texts also select `t.typtype`, `t.typelem`, and
//! `t.typbasetype`, which the `pg_type` virtual relation does not yet carry.

use assert2::assert;
use crabka_pgexec::SqlEngine;
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(engine: &SqlEngine, sql: &str) -> QueryResult {
    engine
        .connect()
        .simple_query(sql)
        .await
        .expect("query succeeds")
        .into_iter()
        .next()
        .expect("one result")
}

fn rows(result: &QueryResult) -> &Vec<Vec<Option<Cell>>> {
    match result {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected rows, got {other:?}"),
    }
}

fn row_text(result: &QueryResult, index: usize) -> Vec<Option<String>> {
    rows(result)[index]
        .iter()
        .map(|cell| {
            cell.as_ref()
                .map(|cell| String::from_utf8(cell.text.to_vec()).expect("valid text cell"))
        })
        .collect()
}

/// tokio-postgres `TYPEINFO_QUERY`: the `pg_range` LEFT JOIN resolves, and for
/// a non-range type (`int8`, OID 20) the range column is NULL.
#[tokio::test]
async fn typeinfo_query_pg_range_left_join_returns_int8_row() {
    let engine = SqlEngine::new();
    let result = run(
        &engine,
        "SELECT t.typname, r.rngsubtype, n.nspname \
         FROM pg_catalog.pg_type t \
         LEFT OUTER JOIN pg_catalog.pg_range r ON r.rngtypid = t.oid \
         INNER JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid \
         WHERE t.oid = 20",
    )
    .await;
    assert!(rows(&result).len() == 1);
    assert!(row_text(&result, 0) == vec![Some("int8".into()), None, Some("pg_catalog".into())]);
}

/// tokio-postgres `TYPEINFO_FALLBACK_QUERY`: `NULL::OID` parses (the `oid`
/// type-name alias) and the row carries a NULL range column.
#[tokio::test]
async fn typeinfo_fallback_query_null_oid_cast_returns_int8_row() {
    let engine = SqlEngine::new();
    let result = run(
        &engine,
        "SELECT t.typname, NULL::OID AS rngsubtype, n.nspname, t.typrelid \
         FROM pg_catalog.pg_type t \
         INNER JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid \
         WHERE t.oid = 20",
    )
    .await;
    assert!(rows(&result).len() == 1);
    assert!(
        row_text(&result, 0)
            == vec![
                Some("int8".into()),
                None,
                Some("pg_catalog".into()),
                Some("0".into()),
            ]
    );
}

/// `oid` casts parse and evaluate; the alias reports int4 (OID 23) in the
/// `RowDescription`, matching the catalog's other oid-valued columns.
#[tokio::test]
async fn oid_casts_parse_evaluate_and_describe_as_int4() {
    let engine = SqlEngine::new();
    let result = run(
        &engine,
        "SELECT NULL::oid AS a, CAST(NULL AS oid) AS b, CAST(20 AS oid) AS c",
    )
    .await;
    assert!(rows(&result).len() == 1);
    assert!(row_text(&result, 0) == vec![None, None, Some("20".into())]);
    let QueryResult::Rows { fields, .. } = &result else {
        panic!("expected rows, got {result:?}");
    };
    let type_oids: Vec<u32> = fields.iter().map(|field| field.type_oid).collect();
    assert!(type_oids == vec![23, 23, 23]);
}

/// `pg_range` exists with zero rows: none of the built-in scalar types are
/// range types.
///
/// The count goes through a derived table because the single-table aggregate
/// fast path (`single_sharded_base_table` in `exec.rs`) rejects every
/// non-stored relation — `SELECT count(*) FROM pg_class` fails the same way.
#[tokio::test]
async fn pg_range_is_empty() {
    let engine = SqlEngine::new();
    let result = run(&engine, "SELECT * FROM pg_catalog.pg_range").await;
    assert!(rows(&result).is_empty());

    let counted = run(
        &engine,
        "SELECT count(*) FROM (SELECT rngtypid FROM pg_catalog.pg_range) r",
    )
    .await;
    assert!(row_text(&counted, 0) == vec![Some("0".into())]);
}
