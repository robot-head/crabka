//! Driver type-lookup (typeinfo) queries against the synthesized catalog.
//!
//! When tokio-postgres meets an unknown type OID it prepares a catalog query
//! that LEFT JOINs `pg_catalog.pg_range`, and on `UNDEFINED_TABLE` falls back
//! to a variant that casts `NULL::OID`. Both must parse and execute: `pg_range`
//! is a zero-row virtual catalog relation and `oid` is a type-name alias for
//! `int4`. The query texts are the verbatim `TYPEINFO_QUERY` /
//! `TYPEINFO_FALLBACK_QUERY` from tokio-postgres 0.7.18's `src/prepare.rs`,
//! with `$1` written as the literal 20 (`int8`).
//!
//! Known wire-fidelity gap (out of scope here): the `RowDescription` for these
//! queries reports int4 (OID 23) for the oid-valued columns and text (OID 25)
//! for `typtype`, where a real `PostgreSQL` server reports oid (OID 26) and
//! "char" (OID 18). tokio-postgres's binary decoder checks those OIDs, so full
//! driver decode compatibility needs per-column type fidelity in the wire
//! layer — a separate design, not per-column overrides in the executor.

use assert2::assert;
use crabka_pgexec::SqlEngine;
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

/// tokio-postgres 0.7.18 `TYPEINFO_QUERY`, `$1` written as the literal 20.
const TYPEINFO_QUERY: &str = "\
SELECT t.typname, t.typtype, t.typelem, r.rngsubtype, t.typbasetype, n.nspname, t.typrelid
FROM pg_catalog.pg_type t
LEFT OUTER JOIN pg_catalog.pg_range r ON r.rngtypid = t.oid
INNER JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
WHERE t.oid = 20
";

/// tokio-postgres 0.7.18 `TYPEINFO_FALLBACK_QUERY` (pre-9.2 servers have no
/// `pg_range`), `$1` written as the literal 20.
const TYPEINFO_FALLBACK_QUERY: &str = "\
SELECT t.typname, t.typtype, t.typelem, NULL::OID, t.typbasetype, n.nspname, t.typrelid
FROM pg_catalog.pg_type t
INNER JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
WHERE t.oid = 20
";

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

/// Both tokio-postgres typeinfo query shapes return the same full `int8` row:
/// a base scalar (`typtype` 'b') with no array element type, no range subtype,
/// no domain base type, and no backing relation, in `pg_catalog`.
#[tokio::test]
async fn typeinfo_queries_return_full_int8_row() {
    let engine = SqlEngine::new();
    let expected = vec![
        Some("int8".into()),       // typname
        Some("b".into()),          // typtype
        Some("0".into()),          // typelem
        None,                      // rngsubtype
        Some("0".into()),          // typbasetype
        Some("pg_catalog".into()), // nspname
        Some("0".into()),          // typrelid
    ];
    for (name, sql) in [
        ("TYPEINFO_QUERY", TYPEINFO_QUERY),
        ("TYPEINFO_FALLBACK_QUERY", TYPEINFO_FALLBACK_QUERY),
    ] {
        let result = run(&engine, sql).await;
        assert!(rows(&result).len() == 1, "{name}");
        assert!(row_text(&result, 0) == expected, "{name}");
    }
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

/// `pg_range` exists with zero rows — none of the built-in scalar types are
/// range types — and a direct `count(*)` over it works.
#[tokio::test]
async fn pg_range_is_empty() {
    let engine = SqlEngine::new();
    let result = run(&engine, "SELECT * FROM pg_catalog.pg_range").await;
    assert!(rows(&result).is_empty());

    let counted = run(&engine, "SELECT count(*) FROM pg_range").await;
    assert!(row_text(&counted, 0) == vec![Some("0".into())]);
}

/// Regression: `SELECT count(*)` directly over a virtual catalog relation
/// used to fail with `UNDEFINED_TABLE` (42P01) because the single-table
/// aggregate fast path (`single_sharded_base_table` in `exec.rs`) propagated
/// the catalog miss instead of falling through to the materializing path.
/// The count matches the number of rows the relation itself materializes.
#[tokio::test]
async fn count_star_over_pg_type_returns_builtin_count() {
    let engine = SqlEngine::new();
    let materialized = run(&engine, "SELECT typname FROM pg_catalog.pg_type").await;
    let builtin_count = rows(&materialized).len();
    assert!(builtin_count > 0);

    let counted = run(&engine, "SELECT count(*) FROM pg_catalog.pg_type").await;
    assert!(row_text(&counted, 0) == vec![Some(builtin_count.to_string())]);
}

/// `pg_range` is listed in `pg_class` alongside the other catalog relations,
/// as `PostgreSQL` lists it.
#[tokio::test]
async fn pg_class_lists_pg_range() {
    let engine = SqlEngine::new();
    let result = run(
        &engine,
        "SELECT relname, relnamespace FROM pg_class WHERE relname = 'pg_range'",
    )
    .await;
    assert!(rows(&result).len() == 1);
    // 11 is pg_catalog's namespace OID in the synthesized catalog.
    assert!(row_text(&result, 0) == vec![Some("pg_range".into()), Some("11".into())]);
}

/// The relkind probe pgbench -i issues before COPY, in both spellings: a
/// literal relation name cast (simple protocol) and the schema-qualified form.
/// `regclass` resolves a relation name to its `pg_class` oid.
#[tokio::test]
async fn regclass_cast_resolves_relation_names_for_the_relkind_probe() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE pgbench_accounts (aid int4 PRIMARY KEY, bid int4)")
        .await
        .expect("create table");

    for cast in [
        "'pgbench_accounts'::pg_catalog.regclass",
        "'pgbench_accounts'::regclass",
        "'public.pgbench_accounts'::regclass",
        "CAST('pgbench_accounts' AS regclass)",
    ] {
        let result = run(
            &engine,
            &format!("SELECT relkind FROM pg_catalog.pg_class WHERE oid={cast}"),
        )
        .await;
        assert!(
            row_text(&result, 0) == vec![Some("r".into())],
            "cast: {cast}"
        );
    }

    // `regclassout` prints the relation name, whichever spelling went in.
    let result = run(&engine, "SELECT 'pg_class'::regclass").await;
    assert!(row_text(&result, 0) == vec![Some("pg_class".into())]);

    let result = run(&engine, "SELECT '1259'::regclass").await;
    assert!(row_text(&result, 0) == vec![Some("pg_class".into())]);

    // Unknown relation names error like PostgreSQL (42P01).
    let error = engine
        .connect()
        .simple_query("SELECT 'no_such_relation'::regclass")
        .await
        .expect_err("unknown relation");
    assert!(error.code == "42P01");
}
