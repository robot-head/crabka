//! `jsonb` end to end: the operator matrix over literals and over stored
//! columns, the four jsonb-null/SQL-NULL quadrants, containment, merge/delete,
//! canonical text output, ordering/grouping, unique-index canonicalization,
//! storage round-trips, and jsonb parameters in both wire formats.
//!
//! `json` is an input alias only — every jsonb-typed result reports OID 3802.

use std::{error::Error, sync::Arc};

use assert2::assert;
use bytes::{Bytes, BytesMut};
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgkv::{Kv, MemKv};
use crabka_pgwire::{
    engine::{BoundParam, Cell, Engine, ExecuteOutcome, QueryResult, Session},
    session::SessionConfig,
};
use tokio::net::TcpListener;
use tokio_postgres::{
    NoTls,
    types::{IsNull, ToSql, Type, to_sql_checked},
};

/// The `jsonb` type OID; every jsonb-typed result reports it, including values
/// that entered through the `json` input alias.
const JSONB_OID: u32 = 3802;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

async fn run(s: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    s.simple_query(sql).await.expect("statement should succeed")
}

fn cell_text(cell: Option<&Cell>) -> Option<String> {
    cell.map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
}

fn rows_text(r: &QueryResult) -> Vec<Vec<Option<String>>> {
    match r {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| row.iter().map(|c| cell_text(c.as_ref())).collect())
            .collect(),
        o => panic!("expected Rows, got {o:?}"),
    }
}

fn tag_of(r: &QueryResult) -> &str {
    match r {
        QueryResult::Command { tag } | QueryResult::Rows { tag, .. } => tag,
        o @ QueryResult::Empty => panic!("expected a tagged result, got {o:?}"),
    }
}

/// Run `sql` (a single statement) and return its rows as text.
async fn query(s: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    rows_text(&run(s, sql).await[0])
}

/// Run `sql` and return the single cell of its single row.
async fn scalar(s: &mut SqlSession, sql: &str) -> Option<String> {
    let rows = query(s, sql).await;
    assert!(rows.len() == 1, "`{sql}` should produce exactly one row");
    assert!(rows[0].len() == 1, "`{sql}` should produce one column");
    rows[0][0].clone()
}

/// Run `SELECT <expr>` and return the value plus the column's reported type OID.
async fn typed_scalar(s: &mut SqlSession, expr: &str) -> (Option<String>, u32) {
    let sql = format!("SELECT {expr}");
    let results = run(s, &sql).await;
    match &results[0] {
        QueryResult::Rows { rows, fields, .. } => {
            (cell_text(rows[0][0].as_ref()), fields[0].type_oid)
        }
        o => panic!("expected Rows, got {o:?}"),
    }
}

async fn err_code(s: &mut SqlSession, sql: &str) -> String {
    s.simple_query(sql).await.expect_err("expected error").code
}

/// A fresh engine with `setup` applied, plus one connected session. The engine
/// is returned so the caller keeps it alive for the session's lifetime.
async fn engine_with(setup: &[&str]) -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    for sql in setup {
        run(&mut s, sql).await;
    }
    (engine, s)
}

fn row(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_string())).collect()
}

/// The `column_default` `information_schema` reports for one column — the
/// `\d`-style rendering of a persisted DEFAULT.
async fn column_default(s: &mut SqlSession, table: &str, column: &str) -> Option<String> {
    let sql = format!(
        "SELECT column_default FROM information_schema.columns \
         WHERE table_name = '{table}' AND column_name = '{column}'"
    );
    scalar(s, &sql).await
}

// ---------------------------------------------------------------------------
// Operator matrix
// ---------------------------------------------------------------------------

/// Every jsonb operator, over literal operands. Both operands are explicitly
/// typed; `untyped_literal_operands_resolve_against_a_jsonb_operand` covers the
/// unadorned-literal spellings separately.
const OPERATOR_MATRIX: &[(&str, Option<&str>)] = &[
    // `->` / `->>`: object key and array index, jsonb vs text result.
    (r#"'{"a":{"b":1}}'::jsonb -> 'a'"#, Some(r#"{"b": 1}"#)),
    (r#"'{"a":"x"}'::jsonb ->> 'a'"#, Some("x")),
    ("'[10,20,30]'::jsonb -> 1", Some("20")),
    ("'[10,20,30]'::jsonb ->> 1", Some("20")),
    // `#>` / `#>>`: path navigation.
    (
        r#"'{"a":{"b":{"c":7}}}'::jsonb #> ARRAY['a','b']"#,
        Some(r#"{"c": 7}"#),
    ),
    (
        r#"'{"a":{"b":{"c":7}}}'::jsonb #>> ARRAY['a','b','c']"#,
        Some("7"),
    ),
    (r#"'{"a":1}'::jsonb #> ARRAY['zz']"#, None),
    // `@>` / `<@`: containment, either way round.
    (r#"'{"a":1,"b":2}'::jsonb @> '{"a":1}'::jsonb"#, Some("t")),
    (r#"'{"a":1}'::jsonb @> '{"a":2}'::jsonb"#, Some("f")),
    (r#"'{"a":1}'::jsonb <@ '{"a":1,"b":2}'::jsonb"#, Some("t")),
    (r#"'{"a":1,"b":2}'::jsonb <@ '{"a":1}'::jsonb"#, Some("f")),
    // `?` / `?|` / `?&`: key existence.
    (r#"'{"a":1}'::jsonb ? 'a'"#, Some("t")),
    (r#"'{"a":1}'::jsonb ? 'zz'"#, Some("f")),
    (r#"'["a","b"]'::jsonb ? 'a'"#, Some("t")),
    (r#"'{"a":1}'::jsonb ?| ARRAY['zz','a']"#, Some("t")),
    (r#"'{"a":1}'::jsonb ?| ARRAY['zz']"#, Some("f")),
    (r#"'{"a":1,"b":2}'::jsonb ?& ARRAY['a','b']"#, Some("t")),
    (r#"'{"a":1}'::jsonb ?& ARRAY['a','zz']"#, Some("f")),
    // `||`: merge / append.
    (
        r#"'{"a":1}'::jsonb || '{"b":2}'::jsonb"#,
        Some(r#"{"a": 1, "b": 2}"#),
    ),
    ("'[1,2]'::jsonb || '[3]'::jsonb", Some("[1, 2, 3]")),
    // `-`: delete by key and by index.
    (r#"'{"a":1,"b":2}'::jsonb - 'a'"#, Some(r#"{"b": 2}"#)),
    ("'[1,2,3]'::jsonb - 1", Some("[1, 3]")),
    // Every operator is strict in its jsonb operand.
    ("NULL::jsonb -> 'a'", None),
    ("NULL::jsonb @> '{}'::jsonb", None),
    (r#"'{"a":1}'::jsonb ? NULL"#, None),
];

#[tokio::test]
async fn operator_matrix_over_literals() {
    let (_engine, mut s) = engine_with(&[]).await;
    for (expr, want) in OPERATOR_MATRIX {
        let got = scalar(&mut s, &format!("SELECT {expr}")).await;
        assert!(got == want.map(ToString::to_string), "SELECT {expr}");
    }
}

/// The same matrix again with the left operand read out of a stored jsonb
/// column, so operator dispatch does not depend on the operand being a literal.
#[tokio::test]
async fn operator_matrix_over_a_stored_column() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (j jsonb)"]).await;
    for (expr, want) in OPERATOR_MATRIX {
        let (literal, rest) = expr
            .split_once("::jsonb ")
            .unwrap_or_else(|| panic!("matrix entry `{expr}` has no `::jsonb` left operand"));
        run(&mut s, "DELETE FROM t").await;
        run(&mut s, &format!("INSERT INTO t VALUES ({literal})")).await;
        let sql = format!("SELECT j {rest} FROM t");
        let got = scalar(&mut s, &sql).await;
        assert!(got == want.map(ToString::to_string), "{sql}");
    }
}

/// `->` takes an integer subscript on arrays (negative counts from the end) and
/// a text key on objects; the wrong shape of subscript yields SQL NULL, exactly
/// as `PostgreSQL` does.
#[tokio::test]
async fn arrow_indexes_arrays_by_int_and_objects_by_text() {
    let (_engine, mut s) = engine_with(&[]).await;
    for (expr, want) in [
        ("'[10,20,30]'::jsonb -> 0", Some("10")),
        ("'[10,20,30]'::jsonb -> 2", Some("30")),
        ("'[10,20,30]'::jsonb -> 3", None),
        ("'[10,20,30]'::jsonb -> -1", Some("30")),
        ("'[10,20,30]'::jsonb -> -3", Some("10")),
        ("'[10,20,30]'::jsonb -> -4", None),
        // An int subscript on an object and a text key on an array are both NULL.
        (r#"'{"1":9}'::jsonb -> 1"#, None),
        ("'[1,2]'::jsonb -> '1'", None),
    ] {
        assert!(
            scalar(&mut s, &format!("SELECT {expr}")).await == want.map(ToString::to_string),
            "SELECT {expr}"
        );
    }
}

// ---------------------------------------------------------------------------
// jsonb null vs SQL NULL
// ---------------------------------------------------------------------------

/// The four quadrants of "null" in jsonb, plus the missing-key case.
///
/// A JSON `null` is a *value*: `'null'::jsonb IS NULL` is false and
/// `jsonb_typeof` calls it `null`. `->` on a present-but-null key returns that
/// value (jsonb null, not SQL NULL); `->>` on the same key returns SQL NULL
/// because there is no text for it. A *missing* key is SQL NULL for both.
#[tokio::test]
async fn jsonb_null_and_sql_null_quadrants() {
    let (_engine, mut s) = engine_with(&[]).await;
    for (expr, want) in [
        // Quadrant 1: a bare jsonb null is a value, not SQL NULL.
        ("'null'::jsonb IS NULL", Some("f".to_string())),
        ("'null'::jsonb", Some("null".to_string())),
        // Quadrant 2: and it types as `null`.
        ("jsonb_typeof('null'::jsonb)", Some("null".to_string())),
        // Quadrant 3: `->` on a present null key is jsonb null.
        (r#"'{"a":null}'::jsonb -> 'a'"#, Some("null".to_string())),
        (
            r#"('{"a":null}'::jsonb -> 'a') IS NULL"#,
            Some("f".to_string()),
        ),
        // Quadrant 4: `->>` on the same key is SQL NULL.
        (r#"'{"a":null}'::jsonb ->> 'a'"#, None),
        (
            r#"('{"a":null}'::jsonb ->> 'a') IS NULL"#,
            Some("t".to_string()),
        ),
        // A missing key is SQL NULL for both operators.
        (
            r#"('{"a":1}'::jsonb -> 'zz') IS NULL"#,
            Some("t".to_string()),
        ),
        (
            r#"('{"a":1}'::jsonb ->> 'zz') IS NULL"#,
            Some("t".to_string()),
        ),
        // And a jsonb null still answers key-existence questions.
        (r#"'{"a":null}'::jsonb ? 'a'"#, Some("t".to_string())),
    ] {
        assert!(
            scalar(&mut s, &format!("SELECT {expr}")).await == want,
            "SELECT {expr}"
        );
    }
}

// ---------------------------------------------------------------------------
// Containment
// ---------------------------------------------------------------------------

/// `@>` descends into nested objects and arrays, and reproduces `PostgreSQL`'s
/// special case: a top-level array contains a *bare scalar* that is one of its
/// elements (the exception to the "same shape" rule).
#[tokio::test]
async fn containment_nests_and_accepts_a_bare_scalar_element() {
    let (_engine, mut s) = engine_with(&[]).await;
    for (expr, want) in [
        // Nested objects.
        (
            r#"'{"a":{"b":1,"c":2}}'::jsonb @> '{"a":{"b":1}}'::jsonb"#,
            "t",
        ),
        (
            r#"'{"a":{"b":1}}'::jsonb @> '{"a":{"b":1,"z":9}}'::jsonb"#,
            "f",
        ),
        // Arrays contain sub-arrays regardless of order.
        ("'[1,2,3]'::jsonb @> '[3,1]'::jsonb", "t"),
        ("'[1,2,3]'::jsonb @> '[4]'::jsonb", "f"),
        // The bare-scalar exception, both spellings.
        ("'[1,2,3]'::jsonb @> '2'::jsonb", "t"),
        ("'2'::jsonb <@ '[1,2,3]'::jsonb", "t"),
        ("'[1,2,3]'::jsonb @> '9'::jsonb", "f"),
        // Nested arrays inside an object value.
        (r#"'{"a":[1,2]}'::jsonb @> '{"a":[2]}'::jsonb"#, "t"),
        // Everything contains itself and the empty object.
        (r#"'{"a":1}'::jsonb @> '{}'::jsonb"#, "t"),
        (r#"'{"a":1}'::jsonb @> '{"a":1}'::jsonb"#, "t"),
        // Numeric scale is irrelevant to containment.
        (r#"'{"a":1.00}'::jsonb @> '{"a":1}'::jsonb"#, "t"),
    ] {
        assert!(
            scalar(&mut s, &format!("SELECT {expr}")).await == Some(want.to_string()),
            "SELECT {expr}"
        );
    }
}

// ---------------------------------------------------------------------------
// Merge and delete
// ---------------------------------------------------------------------------

/// `||` merges objects with the right operand winning duplicate keys, and
/// concatenates/wraps otherwise. `-` deletes by key, by index, and by path
/// (a `text[]` right operand).
#[tokio::test]
async fn concat_merges_right_wins_and_minus_deletes_key_index_and_path() {
    let (_engine, mut s) = engine_with(&[]).await;
    for (expr, want) in [
        // Right-wins on duplicate keys; the union is still canonically sorted.
        (
            r#"'{"a":1,"b":2}'::jsonb || '{"b":9,"c":3}'::jsonb"#,
            r#"{"a": 1, "b": 9, "c": 3}"#,
        ),
        // Array || array concatenates; non-array operands are wrapped.
        ("'[1,2]'::jsonb || '[3,4]'::jsonb", "[1, 2, 3, 4]"),
        (r#"'{"a":1}'::jsonb || '[1]'::jsonb"#, r#"[{"a": 1}, 1]"#),
        (r#"'"a"'::jsonb || '"b"'::jsonb"#, r#"["a", "b"]"#),
        // Delete by key.
        (r#"'{"a":1,"b":2}'::jsonb - 'a'"#, r#"{"b": 2}"#),
        (r#"'{"a":1}'::jsonb - 'zz'"#, r#"{"a": 1}"#),
        // Delete by index, including from the end.
        ("'[1,2,3]'::jsonb - 0", "[2, 3]"),
        ("'[1,2,3]'::jsonb - -1", "[1, 2]"),
        ("'[1,2,3]'::jsonb - 9", "[1, 2, 3]"),
        // Delete by path: a text[] right operand removes each named key.
        (
            r#"'{"a":1,"b":2,"c":3}'::jsonb - ARRAY['a','c']"#,
            r#"{"b": 2}"#,
        ),
    ] {
        assert!(
            scalar(&mut s, &format!("SELECT {expr}")).await == Some(want.to_string()),
            "SELECT {expr}"
        );
    }

    // Deleting an object member by integer index is an error, not a no-op.
    assert!(err_code(&mut s, r#"SELECT '{"a":1}'::jsonb - 0"#).await == "22023");
}

// ---------------------------------------------------------------------------
// Canonical output
// ---------------------------------------------------------------------------

/// Canonical jsonb text: object keys come back sorted (shorter first, then
/// bytewise), duplicate keys collapse last-wins, numeric scale survives, and
/// nothing is rendered in scientific notation.
#[tokio::test]
async fn canonical_output_sorts_keys_and_preserves_numeric_scale() {
    let (_engine, mut s) = engine_with(&[]).await;
    for (expr, want) in [
        // Keys sort by length, then bytewise — not by insertion order.
        (r#"'{"b":2,"a":1}'::jsonb"#, r#"{"a": 1, "b": 2}"#),
        (
            r#"'{"bb":1,"a":2,"c":3}'::jsonb"#,
            r#"{"a": 2, "c": 3, "bb": 1}"#,
        ),
        // Duplicate keys: last one wins, at parse time.
        (r#"'{"a":1,"a":2}'::jsonb"#, r#"{"a": 2}"#),
        // Scale is part of the value.
        ("'1.00'::jsonb", "1.00"),
        ("'1.0'::jsonb", "1.0"),
        (r#"'{"a":1.00}'::jsonb"#, r#"{"a": 1.00}"#),
        // Exponents are expanded, never re-emitted as scientific notation.
        ("'1e2'::jsonb", "100"),
        (r#"'{"a":1e3}'::jsonb"#, r#"{"a": 1000}"#),
        // Insignificant whitespace is dropped; the separators are PG's.
        (r#"'  {"a"  :  1}  '::jsonb"#, r#"{"a": 1}"#),
        ("'[ 1 ,2 ]'::jsonb", "[1, 2]"),
        // Scalars round-trip unchanged.
        (r#"'"x"'::jsonb"#, r#""x""#),
        ("'true'::jsonb", "true"),
        ("'{}'::jsonb", "{}"),
        ("'[]'::jsonb", "[]"),
    ] {
        assert!(
            scalar(&mut s, &format!("SELECT {expr}")).await == Some(want.to_string()),
            "SELECT {expr}"
        );
    }

    // Casting to text yields the same canonical form, and back again is stable.
    assert!(
        scalar(&mut s, r#"SELECT '{"b":2,"a":1}'::jsonb::text"#).await
            == Some(r#"{"a": 1, "b": 2}"#.to_string())
    );
    assert!(
        scalar(&mut s, r#"SELECT '{"b":2,"a":1}'::text::jsonb"#).await
            == Some(r#"{"a": 1, "b": 2}"#.to_string())
    );
}

// ---------------------------------------------------------------------------
// Ordering, grouping, and equality
// ---------------------------------------------------------------------------

/// jsonb btree order is by *kind* first — Object > Array > Bool > Number >
/// String > Null — and `GROUP BY`/`DISTINCT` on a jsonb column group by the
/// canonical value (so `1.0` and `1.00` are the same group).
#[tokio::test]
async fn ordering_grouping_and_equality_on_a_jsonb_column() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (id int4, j jsonb)",
        r#"INSERT INTO t VALUES (1, '{"a":1}'), (2, '[1]'), (3, 'true'), (4, '1'),
                                (5, '"s"'), (6, 'null'), (7, NULL)"#,
    ])
    .await;

    // ASC: the kind order, with SQL NULL last (`PostgreSQL`'s NULLS LAST default).
    assert!(
        query(&mut s, "SELECT id FROM t ORDER BY j").await
            == vec![
                row(&["6"]),
                row(&["5"]),
                row(&["4"]),
                row(&["3"]),
                row(&["2"]),
                row(&["1"]),
                row(&["7"]),
            ]
    );
    // DESC reverses it (and SQL NULL sorts first).
    assert!(
        query(&mut s, "SELECT id FROM t ORDER BY j DESC").await
            == vec![
                row(&["7"]),
                row(&["1"]),
                row(&["2"]),
                row(&["3"]),
                row(&["4"]),
                row(&["5"]),
                row(&["6"]),
            ]
    );
    // `max` follows the same order.
    assert!(scalar(&mut s, "SELECT max(j) FROM t").await == Some(r#"{"a": 1}"#.to_string()));

    // Values that differ only in key order or numeric scale are one group.
    let (_engine2, mut g) = engine_with(&[
        "CREATE TABLE g (j jsonb)",
        r#"INSERT INTO g VALUES ('{"b":2,"a":1}'), ('{"a":1,"b":2}'), ('{"a":1.0}'), ('{"a":1.00}')"#,
    ])
    .await;
    assert!(
        query(&mut g, "SELECT j, count(*) FROM g GROUP BY j ORDER BY j").await
            == vec![
                row(&[r#"{"a": 1.0}"#, "2"]),
                row(&[r#"{"a": 1, "b": 2}"#, "2"]),
            ]
    );
    assert!(query(&mut g, "SELECT count(DISTINCT j) FROM g").await == vec![row(&["2"])]);
    // Equality itself ignores scale.
    assert!(
        scalar(&mut g, r#"SELECT '{"a":1}'::jsonb = '{"a":1.000}'::jsonb"#).await
            == Some("t".to_string())
    );
}

// ---------------------------------------------------------------------------
// Unique-index canonicalization — the correctness crux
// ---------------------------------------------------------------------------

/// A unique jsonb index keys off the *canonical* value: neither object key
/// order nor numeric scale may let a duplicate through. Both cases must raise
/// 23505.
#[tokio::test]
async fn unique_index_ignores_key_order_and_numeric_scale() {
    for (first, second) in [
        // Key order must not matter.
        (r#"'{"b":2,"a":1}'"#, r#"'{"a":1,"b":2}'"#),
        // Nor must it at depth.
        (r#"'{"x":{"b":2,"a":1}}'"#, r#"'{"x":{"a":1,"b":2}}'"#),
        // Numeric scale must not matter.
        (r#"'{"a":1.0}'"#, r#"'{"a":1.00}'"#),
        (r#"'{"a":1}'"#, r#"'{"a":1.000}'"#),
        // Nor inside arrays.
        ("'[1.0]'", "'[1.00]'"),
        // Insignificant whitespace and duplicate keys collapse too.
        (r#"'{"a": 1}'"#, r#"'{"a":1,"a":1}'"#),
    ] {
        let (_engine, mut s) = engine_with(&["CREATE TABLE u (j jsonb UNIQUE)"]).await;
        run(&mut s, &format!("INSERT INTO u VALUES ({first})")).await;
        let code = err_code(&mut s, &format!("INSERT INTO u VALUES ({second})")).await;
        assert!(code == "23505", "{first} then {second}");
        assert!(query(&mut s, "SELECT count(*) FROM u").await == vec![row(&["1"])]);
    }

    // Values that really differ still both insert.
    let (_engine, mut s) = engine_with(&["CREATE TABLE u (j jsonb UNIQUE)"]).await;
    run(&mut s, r#"INSERT INTO u VALUES ('{"a":1}')"#).await;
    run(&mut s, r#"INSERT INTO u VALUES ('{"a":2}')"#).await;
    assert!(query(&mut s, "SELECT count(*) FROM u").await == vec![row(&["2"])]);
}

// ---------------------------------------------------------------------------
// DDL, storage, and the `json` alias
// ---------------------------------------------------------------------------

/// A bare string literal in `INSERT … VALUES` is coerced to the column's jsonb
/// type, canonicalized on the way in, and round-trips through storage —
/// including a NULL column and an `UPDATE` that rewrites the value.
#[tokio::test]
async fn literal_insert_coerces_and_round_trips_through_storage() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (id int4, j jsonb)",
        // Unadorned literal, explicit cast, an expression, and NULL.
        r#"INSERT INTO t VALUES (1, '{"b":2,"a":1}')"#,
        r#"INSERT INTO t VALUES (2, '[1,{"z":null}]'::jsonb)"#,
        "INSERT INTO t VALUES (3, jsonb_build_object('k', 'v'))",
        "INSERT INTO t VALUES (4, NULL)",
    ])
    .await;

    assert!(
        query(&mut s, "SELECT id, j FROM t ORDER BY id").await
            == vec![
                row(&["1", r#"{"a": 1, "b": 2}"#]),
                row(&["2", r#"[1, {"z": null}]"#]),
                row(&["3", r#"{"k": "v"}"#]),
                vec![Some("4".to_string()), None],
            ]
    );

    // The column reports jsonb in RowDescription, and pg_attribute agrees.
    let (_value, oid) = typed_scalar(&mut s, "j FROM t WHERE id = 1").await;
    assert!(oid == JSONB_OID);
    assert!(
        query(
            &mut s,
            "SELECT attname, atttypid FROM pg_attribute \
             WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 't') \
             ORDER BY attnum",
        )
        .await
            == vec![row(&["id", "23"]), row(&["j", "3802"])]
    );

    // UPDATE through a jsonb operator rewrites the stored value.
    assert!(
        tag_of(
            &run(
                &mut s,
                r#"UPDATE t SET j = j || '{"c":3}'::jsonb WHERE id = 1"#
            )
            .await[0]
        ) == "UPDATE 1"
    );
    assert!(
        scalar(&mut s, "SELECT j FROM t WHERE id = 1").await
            == Some(r#"{"a": 1, "b": 2, "c": 3}"#.to_string())
    );
}

/// `json` is accepted as an input alias for `jsonb`, but nothing ever reports
/// OID 114 back: the value is canonicalized and typed as jsonb.
#[tokio::test]
async fn json_is_an_input_alias_and_output_is_always_jsonb() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (j json)"]).await;
    run(&mut s, r#"INSERT INTO t VALUES ('{"b":2,"a":1}')"#).await;

    // Stored through a `json` column, read back canonical and typed jsonb.
    let (value, oid) = typed_scalar(&mut s, "j FROM t").await;
    assert!((value, oid) == (Some(r#"{"a": 1, "b": 2}"#.to_string()), JSONB_OID));

    // The `::json` cast behaves the same way.
    let (value, oid) = typed_scalar(&mut s, r#"'{"b":2,"a":1}'::json"#).await;
    assert!((value, oid) == (Some(r#"{"a": 1, "b": 2}"#.to_string()), JSONB_OID));

    // pg_type still carries real json and jsonb rows with their array partners.
    assert!(
        query(
            &mut s,
            "SELECT typname, oid, typarray FROM pg_type \
             WHERE typname IN ('json', 'jsonb', '_jsonb') ORDER BY oid",
        )
        .await
            == vec![
                row(&["json", "114", "199"]),
                row(&["jsonb", "3802", "3807"]),
                row(&["_jsonb", "3807", "0"]),
            ]
    );
}

// ---------------------------------------------------------------------------
// Functions and aggregates
// ---------------------------------------------------------------------------

#[tokio::test]
async fn jsonb_functions() {
    let (_engine, mut s) = engine_with(&[]).await;
    for (expr, want) in [
        // Builders — the object builder canonicalizes, the array builder keeps order.
        (
            "jsonb_build_object('b', 2, 'a', 1)",
            Some(r#"{"a": 1, "b": 2}"#.to_string()),
        ),
        (
            "jsonb_build_array(1, 'x', true, null)",
            Some(r#"[1, "x", true, null]"#.to_string()),
        ),
        // Introspection.
        ("jsonb_array_length('[1,2,3]')", Some("3".to_string())),
        ("jsonb_array_length('[]')", Some("0".to_string())),
        ("jsonb_typeof('[]')", Some("array".to_string())),
        ("jsonb_typeof('{}')", Some("object".to_string())),
        ("jsonb_typeof('1')", Some("number".to_string())),
        (r#"jsonb_typeof('"s"')"#, Some("string".to_string())),
        ("jsonb_typeof('true')", Some("boolean".to_string())),
        ("jsonb_typeof('null')", Some("null".to_string())),
        // Path extraction — the function spellings of `#>` and `#>>`.
        (
            r#"jsonb_extract_path('{"a":{"b":5}}', 'a')"#,
            Some(r#"{"b": 5}"#.to_string()),
        ),
        (
            r#"jsonb_extract_path('{"a":{"b":5}}', 'a', 'b')"#,
            Some("5".to_string()),
        ),
        (
            r#"jsonb_extract_path_text('{"a":{"b":5}}', 'a', 'b')"#,
            Some("5".to_string()),
        ),
        (r#"jsonb_extract_path('{"a":1}', 'zz')"#, None),
        // jsonb_set: replace, create, and the create_if_missing flag.
        (
            r#"jsonb_set('{"a":1}'::jsonb, ARRAY['a'], '9'::jsonb)"#,
            Some(r#"{"a": 9}"#.to_string()),
        ),
        (
            r#"jsonb_set('{"a":1}'::jsonb, ARRAY['b'], '9'::jsonb)"#,
            Some(r#"{"a": 1, "b": 9}"#.to_string()),
        ),
        (
            r#"jsonb_set('{"a":1}'::jsonb, ARRAY['b'], '9'::jsonb, false)"#,
            Some(r#"{"a": 1}"#.to_string()),
        ),
        (
            r#"jsonb_set('{"a":{"b":1}}'::jsonb, ARRAY['a','b'], '[1]'::jsonb)"#,
            Some(r#"{"a": {"b": [1]}}"#.to_string()),
        ),
        // to_jsonb over each supported scalar shape.
        ("to_jsonb(1)", Some("1".to_string())),
        ("to_jsonb(1.50)", Some("1.50".to_string())),
        ("to_jsonb(true)", Some("true".to_string())),
        ("to_jsonb('x'::text)", Some(r#""x""#.to_string())),
        ("to_jsonb(ARRAY[1,2])", Some("[1, 2]".to_string())),
        (
            r#"to_jsonb('{"a":1}'::jsonb)"#,
            Some(r#"{"a": 1}"#.to_string()),
        ),
        ("to_jsonb(NULL::int4)", None),
        // Strict functions return SQL NULL for a NULL argument.
        ("jsonb_typeof(NULL::jsonb)", None),
        ("jsonb_array_length(NULL::jsonb)", None),
    ] {
        assert!(
            scalar(&mut s, &format!("SELECT {expr}")).await == want,
            "SELECT {expr}"
        );
    }
}

#[tokio::test]
async fn jsonb_aggregates() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (id int4, j jsonb)",
        r#"INSERT INTO t VALUES (1, '{"a":1}'), (2, '[2]'), (3, NULL)"#,
    ])
    .await;

    // jsonb_agg keeps input order and renders SQL NULL as JSON null.
    assert!(
        scalar(&mut s, "SELECT jsonb_agg(j) FROM t").await
            == Some(r#"[{"a": 1}, [2], null]"#.to_string())
    );
    // jsonb_object_agg builds an object, canonically sorted.
    assert!(
        scalar(
            &mut s,
            "SELECT jsonb_object_agg(id::text, j) FROM t WHERE j IS NOT NULL",
        )
        .await
            == Some(r#"{"1": {"a": 1}, "2": [2]}"#.to_string())
    );
    // Over zero rows both aggregates are SQL NULL.
    assert!(scalar(&mut s, "SELECT jsonb_agg(j) FROM t WHERE id < 0").await == None);
    assert!(
        scalar(
            &mut s,
            "SELECT jsonb_object_agg(id::text, j) FROM t WHERE id < 0"
        )
        .await
            == None
    );
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn error_sqlstates() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (j jsonb)"]).await;
    for (sql, want) in [
        // Malformed JSON text.
        ("SELECT '{bad'::jsonb", "22P02"),
        ("SELECT 'Infinity'::jsonb", "22P02"),
        ("SELECT ''::jsonb", "22P02"),
        (r#"SELECT '{"a":1} trailing'::jsonb"#, "22P02"),
        ("INSERT INTO t VALUES ('nope')", "22P02"),
        // No cast from a non-text scalar.
        ("SELECT 1::jsonb", "42846"),
        ("SELECT true::jsonb", "42846"),
        // Builder misuse.
        ("SELECT jsonb_build_object('a')", "22023"),
        ("SELECT jsonb_build_object(NULL, 1)", "22023"),
        // Wrong shape for the function/operator.
        (r#"SELECT jsonb_array_length('{"a":1}')"#, "22023"),
        (r#"SELECT '{"a":1}'::jsonb - 0"#, "22023"),
        // Unknown or misapplied function in the family namespace.
        ("SELECT jsonb_nope('{}')", "42883"),
        ("SELECT jsonb_typeof()", "42883"),
        ("SELECT jsonb_typeof('{}', '{}')", "42883"),
    ] {
        assert!(err_code(&mut s, sql).await == want, "{sql}");
    }
}

/// `PostgreSQL` leaves an unadorned string literal `unknown` and resolves it
/// against the other operand. Gres types it `text` immediately, so the jsonb
/// operators adopt such a literal explicitly — otherwise `||` would silently
/// degrade to *string* concatenation and return a plausible wrong answer.
#[tokio::test]
async fn untyped_literal_operands_resolve_against_a_jsonb_operand() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (j jsonb)"]).await;
    run(&mut s, r#"INSERT INTO t VALUES ('{"a":1}')"#).await;

    for (sql, want) in [
        (r#"SELECT '{"a":1}'::jsonb @> '{"a":1}'"#, "t"),
        (r#"SELECT '{"a":1}'::jsonb <@ '{"a":1}'"#, "t"),
        (r#"SELECT '{"a":{"b":1}}'::jsonb #> '{a,b}'"#, "1"),
        (r#"SELECT '{"a":{"b":1}}'::jsonb #>> '{a,b}'"#, "1"),
        (r#"SELECT '{"a":1,"b":2}'::jsonb ?| '{a,z}'"#, "t"),
        (r#"SELECT '{"a":1,"b":2}'::jsonb ?& '{a,b}'"#, "t"),
        (r#"SELECT '{"a":1}'::jsonb = '{"a":1}'"#, "t"),
        (r#"SELECT '{"a":1}'::jsonb = '{"b":9}'"#, "f"),
    ] {
        assert!(scalar(&mut s, sql).await == Some(want.to_string()), "{sql}");
    }

    // `||` merges as jsonb and reports the jsonb OID, rather than concatenating
    // the two renderings into text (OID 25).
    let (value, oid) = typed_scalar(&mut s, r#"'{"a":1}'::jsonb || '{"b":2}'"#).await;
    assert!((value, oid) == (Some(r#"{"a": 1, "b": 2}"#.to_string()), 3802));

    // Adoption is jsonb-only: an array must still append a bare literal as one
    // element, which is PostgreSQL's `anyarray || anyelement`.
    assert!(scalar(&mut s, "SELECT ARRAY['a','b'] || 'c'").await == Some("{a,b,c}".to_string()));
    assert!(scalar(&mut s, "SELECT 'x' || 'y'").await == Some("xy".to_string()));

    // A predicate against a jsonb column resolves the literal the same way.
    assert!(
        query(&mut s, r#"SELECT j FROM t WHERE j @> '{"a":1}'"#)
            .await
            .len()
            == 1
    );
    assert!(
        query(&mut s, r#"SELECT j FROM t WHERE j = '{"a":1}'"#)
            .await
            .len()
            == 1
    );
}

/// Function ARGUMENTS resolve unknown literals too, but by `PostgreSQL`'s
/// polymorphic rules rather than by adopting jsonb: a parameter declared jsonb
/// takes one, `anyarray`/`anyelement` unify against a typed sibling, and a call
/// where nothing can resolve the polymorphic type is 42804 rather than 42883.
#[tokio::test]
async fn untyped_literal_arguments_resolve_by_polymorphic_rules() {
    let (_engine, mut s) = engine_with(&[]).await;

    for (expr, want, want_oid) in [
        // A `text[]` path and a jsonb new-value, all spelled as bare literals.
        (r#"jsonb_set('{"a":1}', '{a}', '2')"#, r#"{"a": 2}"#, 3802),
        (
            r#"jsonb_set('{"a":1}', '{b}', '2', 'f')"#,
            r#"{"a": 1}"#,
            3802,
        ),
        // `anyelement` resolves from the typed sibling, so the array is int4[]…
        ("array_append('{1,2}', 3)", "{1,2,3}", 1007),
        ("array_prepend(1, '{2,3}')", "{1,2,3}", 1007),
        // …but with no typed sibling anywhere, PG falls back to text[].
        ("array_cat('{1,2}', '{3}')", "{1,2,3}", 1009),
    ] {
        let (value, oid) = typed_scalar(&mut s, expr).await;
        assert!((value, oid) == (Some(want.to_string()), want_oid), "{expr}");
    }

    // The deliberate asymmetry: `jsonb_build_object`'s value parameter is not
    // jsonb, so a bare literal stays *text* and is quoted into the document.
    assert!(
        scalar(&mut s, r#"SELECT jsonb_build_object('a', '{"x":1}')"#).await
            == Some(r#"{"a": "{\"x\":1}"}"#.to_string())
    );

    for (sql, want) in [
        // Nothing can resolve the polymorphic parameter: 42804, not 42883.
        ("SELECT to_jsonb('a')", "42804"),
        ("SELECT cardinality('{1,2}')", "42804"),
        ("SELECT array_length('{1,2}', 1)", "42804"),
        // A *typed* text argument to a jsonb parameter is a plain no-such-function.
        ("SELECT jsonb_typeof('{}'::text)", "42883"),
        // A literal that cannot be read as the resolved type is an input error.
        ("SELECT array_append(ARRAY[1,2], 'x')", "22P02"),
    ] {
        assert!(err_code(&mut s, sql).await == want, "{sql}");
    }
}

/// A jsonb *scalar* is stored as a one-element container, so it answers an
/// integer subscript the way a one-element array does — but only for `->`/`->>`,
/// never for a key subscript or the path operators.
#[tokio::test]
async fn a_scalar_answers_an_integer_subscript_like_a_one_element_array() {
    let (_engine, mut s) = engine_with(&[]).await;
    for (expr, want) in [
        (r#"'"quoted"'::jsonb ->> 0"#, Some("quoted")),
        (r#"'"quoted"'::jsonb -> 0"#, Some(r#""quoted""#)),
        (r#"'"quoted"'::jsonb ->> -1"#, Some("quoted")),
        ("'5'::jsonb ->> 0", Some("5")),
        ("'true'::jsonb ->> 0", Some("true")),
        // Past the single element there is nothing.
        (r#"'"quoted"'::jsonb ->> 1"#, None),
        // A jsonb null element is a value for `->` and SQL NULL for `->>`.
        ("'null'::jsonb -> 0", Some("null")),
        ("'null'::jsonb ->> 0", None),
        // Objects never answer an integer subscript.
        (r#"'{"a":1}'::jsonb -> 0"#, None),
        (r#"'{"a":1}'::jsonb -> -1"#, None),
        // A key subscript on a scalar finds nothing.
        (r#"'"quoted"'::jsonb ->> 'k'"#, None),
        // The path operators reject a scalar root; only the empty path works.
        (r#"'"quoted"'::jsonb #> '{0}'"#, None),
        (r#"'"quoted"'::jsonb #>> '{0}'"#, None),
        (r#"'"quoted"'::jsonb #>> '{}'"#, Some("quoted")),
    ] {
        let got = scalar(&mut s, &format!("SELECT {expr}")).await;
        assert!(got == want.map(ToString::to_string), "SELECT {expr}");
    }
}

// ---------------------------------------------------------------------------
// Column defaults
// ---------------------------------------------------------------------------

/// A `jsonb` column DEFAULT is evaluated and canonicalized at DDL time, written
/// into the catalog, applied to an INSERT that omits the column, and rendered
/// back as a quoted literal — and all of that still holds for a fresh engine
/// that re-reads the catalog from storage.
#[tokio::test]
async fn jsonb_column_defaults_persist_apply_and_render() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
    let mut s = engine.connect();
    run(
        &mut s,
        r#"CREATE TABLE t (
             id int4,
             doc jsonb DEFAULT '{"b":2,"a":{"c":[1,null]}}',
             empty jsonb DEFAULT '{}',
             scalar jsonb DEFAULT 'null'
           )"#,
    )
    .await;

    let defaults = row(&[r#"{"a": {"c": [1, null]}, "b": 2}"#, "{}", "null"]);
    run(&mut s, "INSERT INTO t (id) VALUES (1)").await;
    assert!(query(&mut s, "SELECT doc, empty, scalar FROM t WHERE id = 1").await == vec![defaults]);
    // An explicit NULL still beats the default.
    run(&mut s, "INSERT INTO t (id, doc) VALUES (2, NULL)").await;
    assert!(scalar(&mut s, "SELECT doc FROM t WHERE id = 2").await == None);

    assert!(
        column_default(&mut s, "t", "doc").await
            == Some(r#"'{"a": {"c": [1, null]}, "b": 2}'::jsonb"#.into())
    );
    assert!(column_default(&mut s, "t", "empty").await == Some("'{}'::jsonb".into()));

    // Re-read the catalog: a new engine over the same store deserializes the
    // stored default rather than reusing the one built at DDL time.
    drop(s);
    drop(engine);
    let reopened = SqlEngine::with_kv(Arc::clone(&kv)).expect("reopen engine");
    let mut s = reopened.connect();
    run(&mut s, "INSERT INTO t (id) VALUES (3)").await;
    assert!(
        query(&mut s, "SELECT doc, empty, scalar FROM t WHERE id = 3").await
            == vec![row(&[r#"{"a": {"c": [1, null]}, "b": 2}"#, "{}", "null"])]
    );
    assert!(
        column_default(&mut s, "t", "doc").await
            == Some(r#"'{"a": {"c": [1, null]}, "b": 2}'::jsonb"#.into())
    );
}

// ---------------------------------------------------------------------------
// Sharded tables
// ---------------------------------------------------------------------------

/// jsonb values live happily in a hash-sharded table (sharded on a supported
/// key), and asking to shard *on* a jsonb column is refused at CREATE TABLE —
/// a jsonb value has no shard-key hash, so the table is never created rather
/// than failing at every INSERT.
#[tokio::test]
async fn sharded_tables_store_jsonb_and_refuse_a_jsonb_shard_key() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE ok (id int4 NOT NULL, j jsonb) SHARDED BY HASH (id) BUCKETS 4",
        r#"INSERT INTO ok VALUES (1, '{"b":2,"a":1}')"#,
    ])
    .await;
    assert!(
        query(&mut s, "SELECT id, j FROM ok").await == vec![row(&["1", r#"{"a": 1, "b": 2}"#])]
    );
    assert!(
        scalar(&mut s, r#"SELECT id FROM ok WHERE j @> '{"a":1}'::jsonb"#).await
            == Some("1".to_string())
    );

    let error = s
        .simple_query("CREATE TABLE bad (id int4 NOT NULL, j jsonb) SHARDED BY HASH (j) BUCKETS 4")
        .await
        .expect_err("a jsonb shard key is refused");
    assert!(error.code == "0A000");
    assert!(
        error.message == "hash shard key column \"j\" of type jsonb is not supported",
        "{}",
        error.message
    );
    // The refusal happened before any catalog write.
    assert!(err_code(&mut s, "SELECT 1 FROM bad").await == "42P01");
}

// ---------------------------------------------------------------------------
// Extended protocol
// ---------------------------------------------------------------------------

fn text_param(value: &str) -> BoundParam {
    BoundParam {
        type_oid: None,
        format: 0,
        value: Some(Bytes::copy_from_slice(value.as_bytes())),
    }
}

/// Execute `portal` and return its rows as text.
async fn execute_rows(s: &mut SqlSession, portal: &str) -> Vec<Vec<Option<String>>> {
    match s.execute(portal, 0).await.expect("execute") {
        ExecuteOutcome::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|c| {
                        c.as_ref()
                            .map(|b| String::from_utf8(b.to_vec()).expect("utf8"))
                    })
                    .collect()
            })
            .collect(),
        o => panic!("expected Rows, got {o:?}"),
    }
}

/// A jsonb parameter bound in the *text* format: the client sends the raw JSON
/// text, the engine parses and canonicalizes it.
#[tokio::test]
async fn text_format_jsonb_parameter_binds_and_stores() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (id int4, j jsonb)"]).await;

    s.parse("ins", "INSERT INTO t VALUES ($1, $2)", &[])
        .await
        .expect("parse insert");
    s.bind(
        "p",
        "ins",
        &[text_param("1"), text_param(r#"{"b":2,"a":1}"#)],
        &[],
    )
    .await
    .expect("bind text jsonb parameter");
    s.execute("p", 0).await.expect("execute insert");

    assert!(scalar(&mut s, "SELECT j FROM t").await == Some(r#"{"a": 1, "b": 2}"#.to_string()));

    // A jsonb parameter also drives an operator.
    s.parse("sel", "SELECT $1::jsonb -> 'a'", &[])
        .await
        .expect("parse select");
    s.bind("q", "sel", &[text_param(r#"{"a":[1,2]}"#)], &[])
        .await
        .expect("bind");
    assert!(execute_rows(&mut s, "q").await == vec![row(&["[1, 2]"])]);

    // A malformed payload is rejected with 22P02, at bind or at execute.
    let mut code = s
        .bind("r", "sel", &[text_param("{bad")], &[])
        .await
        .err()
        .map(|e| e.code);
    if code.is_none() {
        code = s.execute("r", 0).await.err().map(|e| e.code);
    }
    assert!(code == Some("22P02".to_string()));
}

// -- binary format, over a real client connection -----------------------------

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

/// The jsonb binary wire format: a `0x01` version byte followed by JSON text.
#[derive(Debug)]
struct JsonbBinaryParam(&'static str);

impl ToSql for JsonbBinaryParam {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if *ty != Type::JSONB {
            return Err("JsonbBinaryParam only supports jsonb".into());
        }
        out.extend_from_slice(&[1]);
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSONB
    }

    to_sql_checked!();
}

/// A jsonb parameter in the *binary* format round-trips through a real client,
/// and a jsonb result column is described as jsonb.
#[tokio::test]
async fn binary_format_jsonb_parameter_round_trips() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (id int4, j jsonb)")
        .await
        .expect("create");

    let insert = client
        .prepare_typed("INSERT INTO t VALUES (1, $1)", &[Type::JSONB])
        .await
        .expect("prepare insert");
    assert!(insert.params() == [Type::JSONB]);
    client
        .execute(&insert, &[&JsonbBinaryParam(r#"{"b":2,"a":1}"#)])
        .await
        .expect("bind binary jsonb parameter");

    let rows = client
        .query("SELECT j::text FROM t", &[])
        .await
        .expect("select");
    assert!(rows[0].get::<_, &str>(0) == r#"{"a": 1, "b": 2}"#);

    // The jsonb column is described as jsonb (OID 3802) in RowDescription.
    let described = client.prepare("SELECT j FROM t").await.expect("describe");
    assert!(*described.columns()[0].type_() == Type::JSONB);

    // A malformed binary payload is rejected, not stored.
    let select = client
        .prepare_typed("SELECT $1::jsonb::text", &[Type::JSONB])
        .await
        .expect("prepare select");
    let err = client
        .query(&select, &[&JsonbBinaryParam("{bad")])
        .await
        .expect_err("malformed binary jsonb must fail");
    assert!(err.as_db_error().expect("db error").code().code() == "22P02");
}
