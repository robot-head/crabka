//! Driver type-lookup (typeinfo) queries against the synthesized catalog.
//!
//! When tokio-postgres meets an unknown type OID it prepares a catalog query
//! that LEFT JOINs `pg_catalog.pg_range`, and on `UNDEFINED_TABLE` falls back
//! to a variant that casts `NULL::OID`. Both must parse and execute: `pg_range`
//! is a zero-row virtual catalog relation and `oid` is a real unsigned type.
//! The query texts are the verbatim `TYPEINFO_QUERY` /
//! `TYPEINFO_FALLBACK_QUERY` from tokio-postgres 0.7.18's `src/prepare.rs`,
//! with `$1` written as the literal 20 (`int8`).
//!
//! Known wire-fidelity gap (out of scope here): the oid-valued **columns** of
//! the virtual catalog relations are still `int4` (OID 23), and `typtype` is
//! `text` (OID 25) where a real `PostgreSQL` server reports "char" (OID 18). An
//! `oid` *expression* now reports 26 correctly; it is the catalog's column
//! declarations that have not moved, and doing so needs per-column type
//! fidelity in the wire layer — a separate design.

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
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
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

/// Both tokio-postgres typeinfo query shapes return the same full `int8` row.
///
/// The row is a base scalar (`typtype` 'b') in `pg_catalog` with no array
/// element type, no range subtype, no domain base type, and no backing
/// relation.
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

/// `oid` casts parse and evaluate, and describe as `oid` (OID 26) rather than
/// `int4` — it is its own unsigned type, which is why `CAST(-1 AS oid)` is
/// 4294967295 here instead of the `-1` an `int4` alias produced.
#[tokio::test]
async fn oid_casts_parse_evaluate_and_describe_as_oid() {
    let engine = SqlEngine::new();
    let result = run(
        &engine,
        "SELECT NULL::oid AS a, CAST(NULL AS oid) AS b, CAST(20 AS oid) AS c, \
         CAST(-1 AS oid) AS d, '4294967295'::oid AS e",
    )
    .await;
    assert!(rows(&result).len() == 1);
    assert!(
        row_text(&result, 0)
            == vec![
                None,
                None,
                Some("20".into()),
                Some("4294967295".into()),
                Some("4294967295".into())
            ]
    );
    let QueryResult::Rows { fields, .. } = &result else {
        panic!("expected rows, got {result:?}");
    };
    let type_oids: Vec<u32> = fields.iter().map(|field| field.type_oid).collect();
    assert!(type_oids == vec![26, 26, 26, 26, 26]);
}

/// The six built-in range types have their exact `pg_range` rows, including
/// their multirange companions and support functions.
#[tokio::test]
async fn pg_range_describes_builtin_ranges() {
    let engine = SqlEngine::new();
    let result = run(
        &engine,
        "SELECT rngtypid, rngsubtype, rngmultitypid, rngsubopc, rngcanonical, rngsubdiff \
         FROM pg_catalog.pg_range ORDER BY rngtypid",
    )
    .await;
    assert!(
        (0..rows(&result).len())
            .map(|i| row_text(&result, i))
            .collect::<Vec<_>>()
            == vec![
                vec![
                    Some("3904".into()),
                    Some("23".into()),
                    Some("4451".into()),
                    Some("1978".into()),
                    Some("int4range_canonical".into()),
                    Some("int4range_subdiff".into()),
                ],
                vec![
                    Some("3906".into()),
                    Some("1700".into()),
                    Some("4532".into()),
                    Some("3125".into()),
                    Some("-".into()),
                    Some("numrange_subdiff".into()),
                ],
                vec![
                    Some("3908".into()),
                    Some("1114".into()),
                    Some("4533".into()),
                    Some("3128".into()),
                    Some("-".into()),
                    Some("tsrange_subdiff".into()),
                ],
                vec![
                    Some("3910".into()),
                    Some("1184".into()),
                    Some("4534".into()),
                    Some("3127".into()),
                    Some("-".into()),
                    Some("tstzrange_subdiff".into()),
                ],
                vec![
                    Some("3912".into()),
                    Some("1082".into()),
                    Some("4535".into()),
                    Some("3122".into()),
                    Some("daterange_canonical".into()),
                    Some("daterange_subdiff".into()),
                ],
                vec![
                    Some("3926".into()),
                    Some("20".into()),
                    Some("4536".into()),
                    Some("3124".into()),
                    Some("int8range_canonical".into()),
                    Some("int8range_subdiff".into()),
                ],
            ]
    );
}

/// A user range carries its text subtype's default collation, unless its
/// definition selected a specific one.
#[tokio::test]
async fn pg_range_describes_user_range_collations() {
    let engine = SqlEngine::new();
    run(
        &engine,
        "CREATE TYPE default_text_range AS RANGE (subtype = text); \
         CREATE TYPE c_text_range AS RANGE (subtype = text, collation = \"C\"); \
         CREATE TYPE int_range AS RANGE (subtype = int4); \
         CREATE TYPE float_range AS RANGE (subtype = float8)",
    )
    .await;
    let result = run(
        &engine,
        "SELECT t.typname, r.rngcollation, r.rngsubopc \
         FROM pg_catalog.pg_range r JOIN pg_catalog.pg_type t ON t.oid = r.rngtypid \
         WHERE t.typname IN ('c_text_range', 'default_text_range', 'float_range', 'int_range') \
         ORDER BY t.typname",
    )
    .await;
    assert!(
        (0..rows(&result).len())
            .map(|i| row_text(&result, i))
            .collect::<Vec<_>>()
            == vec![
                vec![
                    Some("c_text_range".into()),
                    Some("950".into()),
                    Some("3126".into()),
                ],
                vec![
                    Some("default_text_range".into()),
                    Some("100".into()),
                    Some("3126".into()),
                ],
                vec![
                    Some("float_range".into()),
                    Some("0".into()),
                    Some("3123".into()),
                ],
                vec![
                    Some("int_range".into()),
                    Some("0".into()),
                    Some("1978".into()),
                ],
            ]
    );
}

/// Every relation owns a named composite type and its array companion.
#[tokio::test]
async fn pg_class_and_pg_type_link_a_table_rowtype() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE rowtype_table (id int4, label text)").await;
    let result = run(
        &engine,
        "SELECT c.reltype = t.oid, t.typrelid = c.oid, t.typarray = a.oid, \
                a.typelem = t.oid, t.typtype, a.typname, t.typlen, a.typlen, \
                t.typalign, a.typalign \
         FROM pg_catalog.pg_class c \
         JOIN pg_catalog.pg_type t ON t.oid = c.reltype \
         JOIN pg_catalog.pg_type a ON a.oid = t.typarray \
         WHERE c.relname = 'rowtype_table'",
    )
    .await;
    assert!(rows(&result).len() == 1);
    assert!(
        row_text(&result, 0)
            == vec![
                Some("t".into()),
                Some("t".into()),
                Some("t".into()),
                Some("t".into()),
                Some("c".into()),
                Some("_rowtype_table".into()),
                Some("-1".into()),
                Some("-1".into()),
                Some("d".into()),
                Some("d".into()),
            ]
    );
}

/// A relation name used as an expression has that relation's composite type,
/// including when a correlated subquery substitutes the outer whole row.
#[tokio::test]
async fn whole_row_references_describe_the_relation_rowtype() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE whole_row_type (id int4)").await;
    let rowtype = row_text(
        &run(
            &engine,
            "SELECT reltype FROM pg_catalog.pg_class WHERE relname = 'whole_row_type'",
        )
        .await,
        0,
    )[0]
    .as_deref()
    .expect("row type oid")
    .parse::<u32>()
    .expect("valid oid");
    for sql in [
        "SELECT whole_row_type FROM whole_row_type",
        "SELECT q FROM whole_row_type AS q",
        "SELECT a FROM whole_row_type AS a JOIN whole_row_type AS b ON a.id = b.id",
        "SELECT (SELECT whole_row_type) FROM whole_row_type",
    ] {
        let QueryResult::Rows { fields, .. } = run(&engine, sql).await else {
            panic!("expected rows");
        };
        assert!(fields.len() == 1, "{sql}");
        assert!(fields[0].type_oid == rowtype, "{sql}");
    }
    let QueryResult::Rows { fields, .. } =
        run(&engine, "SELECT (whole_row_type).id FROM whole_row_type").await
    else {
        panic!("expected rows");
    };
    assert!(fields[0].type_oid == 23);
    let QueryResult::Rows { fields, .. } = run(
        &engine,
        "SELECT (SELECT (whole_row_type).id) FROM whole_row_type",
    )
    .await
    else {
        panic!("expected rows");
    };
    assert!(fields[0].type_oid == 23);
    run(&engine, "CREATE TABLE whole_row_type_text (id text)").await;
    let QueryResult::Rows { fields, .. } = run(
        &engine,
        "SELECT (SELECT (whole_row_type).id \
         FROM whole_row_type_text AS whole_row_type) FROM whole_row_type",
    )
    .await
    else {
        panic!("expected rows");
    };
    assert!(fields[0].type_oid == 25);
    assert!(
        engine
            .connect()
            .simple_query(
                "SELECT (SELECT (whole_row_type).id \
                 FROM (SELECT 'x' AS whole_row_type) AS inner_scope) \
                 FROM whole_row_type",
            )
            .await
            .is_err()
    );
    let QueryResult::Rows { fields, .. } = run(
        &engine,
        "SELECT (a).id FROM whole_row_type_text AS b JOIN whole_row_type AS a ON true",
    )
    .await
    else {
        panic!("expected rows");
    };
    assert!(fields[0].type_oid == 23);
    assert!(
        engine
            .connect()
            .simple_query("SELECT (whole_row_type).tableoid FROM whole_row_type")
            .await
            .is_err()
    );
    run(
        &engine,
        "CREATE FUNCTION whole_row_argument(value whole_row_type) RETURNS int4 \
         LANGUAGE sql RETURN 1",
    )
    .await;
    let result = run(
        &engine,
        "SELECT p.proargtypes[0] = c.reltype \
         FROM pg_catalog.pg_proc p \
         JOIN pg_catalog.pg_class c ON c.relname = 'whole_row_type' \
         WHERE p.proname = 'whole_row_argument'",
    )
    .await;
    assert!(row_text(&result, 0) == vec![Some("t".into())]);
    run(
        &engine,
        "CREATE FUNCTION whole_row_id(value whole_row_type) RETURNS int4 \
         LANGUAGE sql RETURN (value).id",
    )
    .await;
    run(&engine, "INSERT INTO whole_row_type (id) VALUES (7)").await;
    let result = run(
        &engine,
        "SELECT whole_row_id(whole_row_type) FROM whole_row_type",
    )
    .await;
    assert!(row_text(&result, 0) == vec![Some("7".into())]);
    run(
        &engine,
        "CREATE FUNCTION whole_row_id_pos(whole_row_type) RETURNS int4 \
         LANGUAGE sql RETURN $1.id",
    )
    .await;
    let result = run(
        &engine,
        "SELECT whole_row_id_pos(whole_row_type) FROM whole_row_type",
    )
    .await;
    assert!(row_text(&result, 0) == vec![Some("7".into())]);
    run(
        &engine,
        "CREATE FUNCTION whole_row_id_named(t whole_row_type) RETURNS int4 \
         LANGUAGE sql RETURN t.id",
    )
    .await;
    let result = run(
        &engine,
        "SELECT f.id, whole_row_id_named(f) FROM whole_row_type AS f",
    )
    .await;
    assert!(row_text(&result, 0) == vec![Some("7".into()), Some("7".into())]);
    run(&engine, "CREATE TABLE whole_row_shadow_source (id int4)").await;
    run(&engine, "INSERT INTO whole_row_shadow_source VALUES (9)").await;
    let result = run(&engine, "SELECT id FROM whole_row_shadow_source AS t").await;
    assert!(row_text(&result, 0) == vec![Some("9".into())]);
    let result = run(&engine, "SELECT t.id FROM whole_row_shadow_source AS t").await;
    let QueryResult::Rows { fields, .. } = &result else {
        panic!("expected rows");
    };
    assert!(fields[0].type_oid == 23);
    assert!(row_text(&result, 0) == vec![Some("9".into())]);
    run(
        &engine,
        "CREATE FUNCTION whole_row_shadow(t whole_row_type) RETURNS int4 \
         LANGUAGE sql AS 'SELECT t.id FROM whole_row_shadow_source AS t'",
    )
    .await;
    let result = run(
        &engine,
        "SELECT whole_row_shadow(whole_row_type) FROM whole_row_type",
    )
    .await;
    assert!(row_text(&result, 0) == vec![Some("9".into())]);
    run(
        &engine,
        "CREATE FUNCTION whole_row_return(value int4) RETURNS whole_row_type \
         LANGUAGE sql AS 'SELECT ROW($1)::whole_row_type'",
    )
    .await;
    let result = run(&engine, "SELECT whole_row_return(9)").await;
    let QueryResult::Rows { fields, .. } = &result else {
        panic!("expected rows");
    };
    assert!(fields[0].type_oid == rowtype);
    assert!(row_text(&result, 0) == vec![Some("(9)".into())]);
    let result = run(&engine, "SELECT * FROM whole_row_return(9)").await;
    assert!(row_text(&result, 0) == vec![Some("9".into())]);
    run(
        &engine,
        "CREATE FUNCTION whole_row_returns() RETURNS SETOF whole_row_type \
         LANGUAGE sql AS 'SELECT whole_row_type FROM whole_row_type'",
    )
    .await;
    let result = run(&engine, "SELECT * FROM whole_row_returns()").await;
    assert!(row_text(&result, 0) == vec![Some("7".into())]);
    let result = run(
        &engine,
        "SELECT * FROM whole_row_returns() WITH ORDINALITY AS returned(id, position)",
    )
    .await;
    assert!(row_text(&result, 0) == vec![Some("7".into()), Some("1".into())]);
    run(
        &engine,
        "CREATE VIEW whole_row_return_view AS SELECT * FROM whole_row_returns()",
    )
    .await;
    let result = run(&engine, "SELECT * FROM whole_row_return_view").await;
    assert!(row_text(&result, 0) == vec![Some("7".into())]);
    run(
        &engine,
        "CREATE FUNCTION whole_row_scalar(value int4) RETURNS int4 \
         LANGUAGE sql AS 'SELECT $1'",
    )
    .await;
    let result = run(&engine, "SELECT * FROM whole_row_scalar(4)").await;
    assert!(row_text(&result, 0) == vec![Some("4".into())]);
    run(
        &engine,
        "CREATE FUNCTION whole_row_out_rows(IN limit_value int4, OUT a text, OUT b text) \
         RETURNS SETOF record LANGUAGE sql \
         AS 'SELECT value::text, (value + 1)::text FROM generate_series(1, $1) AS value'",
    )
    .await;
    let result = run(
        &engine,
        "SELECT t.a, t, t.a FROM whole_row_out_rows(2) AS t LIMIT 1",
    )
    .await;
    assert!(row_text(&result, 0) == vec![Some("1".into()), Some("(1,2)".into()), Some("1".into())]);
    run(&engine, "CREATE TABLE whole_row_pair (id int4, label text)").await;
    run(
        &engine,
        "CREATE FUNCTION whole_row_pair_return() RETURNS whole_row_pair \
         LANGUAGE sql AS 'SELECT ROW(1, ''packed'')::whole_row_pair'; \
         CREATE FUNCTION whole_row_pair_columns() RETURNS whole_row_pair \
         LANGUAGE sql AS 'SELECT 2, ''columns''::text'",
    )
    .await;
    let result = run(&engine, "SELECT * FROM whole_row_pair_return()").await;
    assert!(row_text(&result, 0) == vec![Some("1".into()), Some("packed".into())]);
    let result = run(&engine, "SELECT * FROM whole_row_pair_columns()").await;
    assert!(row_text(&result, 0) == vec![Some("2".into()), Some("columns".into())]);
    run(
        &engine,
        "CREATE TYPE whole_row_composite AS (id int4, label text)",
    )
    .await;
    run(
        &engine,
        "CREATE FUNCTION whole_row_composite_rows() RETURNS SETOF whole_row_composite \
         LANGUAGE sql AS 'SELECT ROW(5, ''composite'')::whole_row_composite'",
    )
    .await;
    let result = run(
        &engine,
        "SELECT * FROM whole_row_composite_rows() WITH ORDINALITY AS emitted(id, label, position)",
    )
    .await;
    assert!(
        row_text(&result, 0) == vec![Some("5".into()), Some("composite".into()), Some("1".into())]
    );
    let result = run(
        &engine,
        "SELECT * FROM ROWS FROM (whole_row_returns(), generate_series(10, 11)) \
         WITH ORDINALITY AS rows_from_result(id, generated, position)",
    )
    .await;
    assert!(row_text(&result, 0) == vec![Some("7".into()), Some("10".into()), Some("1".into())]);
    assert!(row_text(&result, 1) == vec![None, Some("11".into()), Some("2".into())]);
    let result = engine
        .connect()
        .simple_query(
            "CREATE TEMP TABLE whole_row_temp (id int4, discarded bool, label text); \
             INSERT INTO whole_row_temp VALUES (3, true, 'temporary'); \
             ALTER TABLE whole_row_temp DROP COLUMN discarded; \
             CREATE FUNCTION whole_row_temp_rows() RETURNS SETOF whole_row_temp \
             LANGUAGE sql AS 'SELECT * FROM whole_row_temp'; \
             SELECT * FROM whole_row_temp_rows()",
        )
        .await
        .unwrap_or_else(|error| panic!("temporary rowtype: {error:?}"))
        .pop()
        .expect("temporary rowtype result");
    assert!(row_text(&result, 0) == vec![Some("3".into()), Some("temporary".into())]);
    run(&engine, "CREATE SEQUENCE whole_row_sequence").await;
    run(
        &engine,
        "CREATE FUNCTION whole_row_sequence_return() RETURNS whole_row_sequence \
         LANGUAGE sql AS 'SELECT ROW(1, 0, false)::whole_row_sequence'",
    )
    .await;
    let result = run(&engine, "SELECT * FROM whole_row_sequence_return()").await;
    assert!(row_text(&result, 0) == vec![Some("1".into()), Some("0".into()), Some("f".into())]);
    let result = run(&engine, "SELECT (whole_row_return(9)).id").await;
    assert!(row_text(&result, 0) == vec![Some("9".into())]);
    run(
        &engine,
        "CREATE FUNCTION whole_row_out(IN value whole_row_type, OUT result int4) \
         LANGUAGE sql RETURN 1",
    )
    .await;
    let result = run(
        &engine,
        "SELECT p.prorettype = 23, p.proallargtypes[1] = c.reltype, \
                p.proallargtypes[2] = 23 \
         FROM pg_catalog.pg_proc p \
         JOIN pg_catalog.pg_class c ON c.relname = 'whole_row_type' \
         WHERE p.proname = 'whole_row_out'",
    )
    .await;
    assert!(row_text(&result, 0) == vec![Some("t".into()), Some("t".into()), Some("t".into())]);

    run(
        &engine,
        "CREATE VIEW whole_row_type_view AS SELECT id FROM whole_row_type",
    )
    .await;
    let view_rowtype = row_text(
        &run(
            &engine,
            "SELECT reltype FROM pg_catalog.pg_class WHERE relname = 'whole_row_type_view'",
        )
        .await,
        0,
    )[0]
    .as_deref()
    .expect("view row type oid")
    .parse::<u32>()
    .expect("valid oid");
    let QueryResult::Rows { fields, .. } = run(
        &engine,
        "SELECT whole_row_type_view FROM whole_row_type_view",
    )
    .await
    else {
        panic!("expected rows");
    };
    assert!(fields[0].type_oid == view_rowtype);
}

/// Relation composite types are usable in casts and track later DDL.
#[tokio::test]
async fn relation_rowtype_casts_follow_the_catalog() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE relation_type_cast (first int4)").await;
    let result = run(&engine, "SELECT (ROW(1)::relation_type_cast).first").await;
    assert!(row_text(&result, 0) == vec![Some("1".into())]);

    run(
        &engine,
        "ALTER TABLE relation_type_cast ADD COLUMN second text",
    )
    .await;
    let result = run(&engine, "SELECT (ROW(1, 'x')::relation_type_cast).second").await;
    assert!(row_text(&result, 0) == vec![Some("x".into())]);

    run(
        &engine,
        "CREATE TYPE relation_type_unrelated AS (value int4)",
    )
    .await;
    run(&engine, "DROP TABLE relation_type_cast").await;
    let result = run(&engine, "SELECT (ROW(2)::relation_type_unrelated).value").await;
    assert!(row_text(&result, 0) == vec![Some("2".into())]);

    run(&engine, "CREATE SEQUENCE relation_type_sequence").await;
    let result = run(
        &engine,
        "SELECT (ROW(1, 2, true)::relation_type_sequence).last_value",
    )
    .await;
    assert!(row_text(&result, 0) == vec![Some("1".into())]);
    let result = run(
        &engine,
        "SELECT (ROW(2200, 'public', 10, NULL)::pg_catalog.pg_namespace).nspname",
    )
    .await;
    assert!(row_text(&result, 0) == vec![Some("public".into())]);
    let error = engine
        .connect()
        .simple_query("SELECT ROW(1)::relation_type_cast")
        .await
        .expect_err("dropped row type is not resolvable");
    assert!(error.code == "42704");
}

/// A composite value written before its type grows still accepts a field
/// assignment through the new catalog shape.
#[tokio::test]
async fn composite_field_assignment_after_type_growth_uses_field_names() {
    let engine = SqlEngine::new();
    for sql in [
        "CREATE TYPE indirection_growth AS (first int4, second int4)",
        "CREATE TABLE indirection_growth_table (value indirection_growth)",
        "INSERT INTO indirection_growth_table VALUES (ROW(1, 2))",
        "ALTER TYPE indirection_growth ADD ATTRIBUTE third int4",
        "UPDATE indirection_growth_table SET value.third = 3",
    ] {
        run(&engine, sql).await;
    }
    let result = run(
        &engine,
        "SELECT (value).first, (value).second, (value).third FROM indirection_growth_table",
    )
    .await;
    assert!(row_text(&result, 0) == vec![Some("1".into()), Some("2".into()), Some("3".into())]);
}

/// Array alignment follows its element type, including the fixed `int8` case.
#[tokio::test]
async fn pg_type_keeps_d_alignment_for_builtin_and_rowtype_arrays() {
    let engine = SqlEngine::new();
    run(&engine, "CREATE TABLE rowtype_alignment_table (id int4)").await;
    let result = run(
        &engine,
        "SELECT typname, typalign FROM pg_catalog.pg_type \
         WHERE typname IN ('int8', '_int4', '_int8', '_rowtype_alignment_table') \
         ORDER BY typname",
    )
    .await;
    assert!(
        (0..rows(&result).len())
            .map(|index| row_text(&result, index))
            .collect::<Vec<_>>()
            == vec![
                vec![Some("_int4".into()), Some("i".into())],
                vec![Some("_int8".into()), Some("d".into())],
                vec![Some("_rowtype_alignment_table".into()), Some("d".into()),],
                vec![Some("int8".into()), Some("d".into())],
            ]
    );
}

#[tokio::test]
async fn pg_range_uses_oid_and_regproc_column_types_on_the_wire() {
    let engine = SqlEngine::new();
    let result = run(
        &engine,
        "SELECT rngtypid, rngsubtype, rngmultitypid, rngcollation, rngsubopc, \
                rngcanonical, rngsubdiff \
         FROM pg_catalog.pg_range WHERE rngtypid = 3904",
    )
    .await;
    let QueryResult::Rows { fields, .. } = result else {
        panic!("pg_range query should return rows");
    };
    assert!(
        fields
            .iter()
            .map(|field| field.type_oid)
            .collect::<Vec<_>>()
            == vec![26, 26, 26, 26, 26, 24, 24]
    );
}

/// Regression: `SELECT count(*)` directly over a virtual catalog relation used
/// to fail with `UNDEFINED_TABLE` (42P01).
///
/// The single-table aggregate fast path (`single_sharded_base_table` in
/// `exec.rs`) propagated the catalog miss instead of falling through to the
/// materializing path. The count matches the number of rows the relation itself
/// materializes.
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

/// The relkind probe that pgbench -i issues before COPY, in both spellings.
///
/// The two spellings are a literal relation name cast (simple protocol) and the
/// schema-qualified form. `regclass` resolves a relation name to its `pg_class`
/// oid.
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
