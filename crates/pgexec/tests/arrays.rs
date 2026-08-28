//! SQL arrays end to end: literals and construction, text output quoting,
//! subscripting and slicing, `= ANY(...)` and `<> ALL(...)` with their
//! three-valued logic, `unnest` in FROM, `array_agg`, the operator and function
//! surface, storage round-trips, array parameters in both wire formats,
//! multiple dimensions, and bounded string element types.

use std::sync::Arc;

use assert2::assert;
use bytes::Bytes;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgkv::{Kv, MemKv};
use crabka_pgwire::{
    engine::{BoundParam, Cell, Engine, ExecuteOutcome, QueryResult, Session},
    session::SessionConfig,
};
use tokio::net::TcpListener;
use tokio_postgres::{NoTls, types::Type};

/// `_int4` / `_text`: the OIDs an array-typed result reports.
const INT4_ARRAY_OID: u32 = 1007;
const TEXT_ARRAY_OID: u32 = 1009;
const RECORD_ARRAY_OID: u32 = 2287;

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

/// Run `sql`, a single statement, and return its rows as text.
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

/// Run `SELECT <expr>` and return the value with the column's reported type
/// OID.
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

/// A fresh engine with `setup` applied, plus one connected session. This
/// function returns the engine so the caller keeps it alive for the session's
/// lifetime.
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

/// The `column_default` `information_schema` reports for one column. This is
/// the `\d`-style rendering of a persisted DEFAULT.
async fn column_default(s: &mut SqlSession, table: &str, column: &str) -> Option<String> {
    let sql = format!(
        "SELECT column_default FROM information_schema.columns \
         WHERE table_name = '{table}' AND column_name = '{column}'"
    );
    scalar(s, &sql).await
}

/// Assert a table of `SELECT <expr>` results.
async fn expect_exprs(s: &mut SqlSession, cases: &[(&str, Option<&str>)]) {
    for (expr, want) in cases {
        let got = scalar(s, &format!("SELECT {expr}")).await;
        assert!(got == want.map(ToString::to_string), "SELECT {expr}");
    }
}

// ---------------------------------------------------------------------------
// Literals and construction
// ---------------------------------------------------------------------------

/// `ARRAY[...]` and `'{...}'::type[]` are two spellings of the same value. The
/// constructor decides the element type, so a bare `ARRAY[NULL]` is `text[]`,
/// which matches `PostgreSQL`.
#[tokio::test]
async fn array_literals_and_construction() {
    let (_engine, mut s) = engine_with(&[]).await;
    expect_exprs(
        &mut s,
        &[
            ("ARRAY[1,2,3]", Some("{1,2,3}")),
            ("'{1,2,3}'::int4[]", Some("{1,2,3}")),
            ("ARRAY['a','b']", Some("{a,b}")),
            ("'{a,b}'::text[]", Some("{a,b}")),
            // Empty arrays need a type; with one they render as `{}`.
            ("ARRAY[]::int4[]", Some("{}")),
            ("'{}'::int4[]", Some("{}")),
            // NULL elements survive both spellings.
            ("ARRAY[NULL]", Some("{NULL}")),
            ("ARRAY[1,NULL,3]", Some("{1,NULL,3}")),
            ("'{NULL}'::int4[]", Some("{NULL}")),
            ("'{1,NULL,3}'::int4[]", Some("{1,NULL,3}")),
            // A NULL array is not an empty array.
            ("NULL::int4[]", None),
        ],
    )
    .await;

    // The reported element type follows the constructor.
    for (expr, want_oid) in [
        ("ARRAY[1,2,3]", INT4_ARRAY_OID),
        ("'{1}'::int4[]", INT4_ARRAY_OID),
        ("ARRAY['a']", TEXT_ARRAY_OID),
        // An all-NULL constructor defaults to text[], as `PostgreSQL` does.
        ("ARRAY[NULL]", TEXT_ARRAY_OID),
        ("ARRAY[]::int4[]", INT4_ARRAY_OID),
    ] {
        let (_value, oid) = typed_scalar(&mut s, expr).await;
        assert!(oid == want_oid, "SELECT {expr}");
    }

    // An untyped empty constructor cannot be typed at all.
    assert!(err_code(&mut s, "SELECT ARRAY[]").await == "42P18");
}

/// Anonymous composite values have PostgreSQL's `record[]` companion, so both
/// explicit constructors and `array_agg` keep the record element type.
#[tokio::test]
async fn record_arrays_keep_the_record_array_type() {
    let (_engine, mut s) = engine_with(&[]).await;
    for expr in [
        "ARRAY[ROW(1), ROW(2)]",
        "array_agg(ROW(n)) FROM (VALUES (1), (2)) AS v(n)",
    ] {
        let (value, oid) = typed_scalar(&mut s, expr).await;
        assert!(value.as_deref() == Some("{(1),(2)}"), "SELECT {expr}");
        assert!(oid == RECORD_ARRAY_OID, "SELECT {expr}");
    }
}

/// A named composite uses the `typarray` allocated alongside its own `pg_type`
/// row, rather than the anonymous `record[]` pseudo-type.
#[tokio::test]
async fn named_record_arrays_keep_the_named_composite_array_type() {
    let (_engine, mut s) = engine_with(&["CREATE TYPE array_pair AS (id int4)"]).await;
    let type_oid = scalar(
        &mut s,
        "SELECT typarray FROM pg_catalog.pg_type WHERE typname = 'array_pair'",
    )
    .await
    .expect("named composite array oid")
    .parse::<u32>()
    .expect("valid named composite array oid");

    let (value, oid) = typed_scalar(&mut s, "ARRAY[ROW(1)::array_pair, ROW(2)::array_pair]").await;
    assert!(value.as_deref() == Some("{(1),(2)}"));
    assert!(oid == type_oid);
}

/// User-defined enum and domain elements retain their own array OIDs. A domain
/// over a composite is the important recursive case: its array input reaches
/// the domain's composite base type for each element.
#[tokio::test]
async fn enum_and_domain_over_composite_arrays_keep_their_user_type_identity() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TYPE array_mood AS ENUM ('happy', 'sad')",
        "CREATE TYPE array_domain_pair AS (id int4)",
        "CREATE DOMAIN array_pair_domain AS array_domain_pair",
        "CREATE TABLE array_domain_values (pairs array_pair_domain[])",
    ])
    .await;
    let enum_array_oid = scalar(
        &mut s,
        "SELECT typarray FROM pg_catalog.pg_type WHERE typname = 'array_mood'",
    )
    .await
    .expect("enum array oid")
    .parse::<u32>()
    .expect("valid enum array oid");
    let domain_array_oid = scalar(
        &mut s,
        "SELECT typarray FROM pg_catalog.pg_type WHERE typname = 'array_pair_domain'",
    )
    .await
    .expect("domain array oid")
    .parse::<u32>()
    .expect("valid domain array oid");

    let (enum_value, enum_oid) =
        typed_scalar(&mut s, "ARRAY['happy'::array_mood, 'sad'::array_mood]").await;
    assert!(enum_value.as_deref() == Some("{happy,sad}"));
    assert!(enum_oid == enum_array_oid);

    let (domain_value, domain_oid) = typed_scalar(
        &mut s,
        "ARRAY[ROW(1)::array_pair_domain, ROW(2)::array_pair_domain]",
    )
    .await;
    assert!(domain_value.as_deref() == Some("{(1),(2)}"));
    assert!(domain_oid == domain_array_oid);

    run(
        &mut s,
        "INSERT INTO array_domain_values VALUES ('{(3),(4)}'::array_pair_domain[])",
    )
    .await;
    assert!(
        scalar(&mut s, "SELECT pairs FROM array_domain_values").await == Some("{(3),(4)}".into())
    );
}

#[tokio::test]
async fn user_defined_range_arrays_keep_their_array_type_identity() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TYPE array_int_range AS RANGE (SUBTYPE = int4)",
        "CREATE TABLE array_range_values (ranges array_int_range[])",
    ])
    .await;
    let array_oid = scalar(
        &mut s,
        "SELECT typarray FROM pg_catalog.pg_type WHERE typname = 'array_int_range'",
    )
    .await
    .expect("range array oid")
    .parse::<u32>()
    .expect("valid range array oid");

    let (value, oid) = typed_scalar(
        &mut s,
        "ARRAY['[1,3)'::array_int_range, '[5,8)'::array_int_range]",
    )
    .await;
    assert!(value.as_deref() == Some("{\"[1,3)\",\"[5,8)\"}"));
    assert!(oid == array_oid);
    run(
        &mut s,
        "INSERT INTO array_range_values VALUES ('{\"[2,4)\"}'::array_int_range[])",
    )
    .await;
    assert!(
        scalar(&mut s, "SELECT ranges FROM array_range_values").await == Some("{\"[2,4)\"}".into())
    );
}

/// Array text output quoting. The engine quotes only the elements that would
/// otherwise be ambiguous, and it quotes the literal string `NULL` so a reader
/// can tell it apart from a SQL NULL element.
#[tokio::test]
async fn text_output_quotes_only_what_it_must() {
    let (_engine, mut s) = engine_with(&[]).await;
    expect_exprs(
        &mut s,
        &[
            ("ARRAY[]::text[]", Some("{}")),
            ("ARRAY[NULL]::text[]", Some("{NULL}")),
            ("ARRAY['plain']", Some("{plain}")),
            // Delimiters, braces, quotes and whitespace force quoting.
            ("ARRAY['a,b']", Some(r#"{"a,b"}"#)),
            ("ARRAY['{x}']", Some(r#"{"{x}"}"#)),
            (r#"ARRAY['c"d']"#, Some(r#"{"c\"d"}"#)),
            ("ARRAY[' sp ']", Some(r#"{" sp "}"#)),
            // The literal text `NULL` is quoted; a SQL NULL is not.
            ("ARRAY['NULL', NULL]", Some(r#"{"NULL",NULL}"#)),
            // An empty string is quoted too.
            ("ARRAY['']", Some(r#"{""}"#)),
        ],
    )
    .await;

    // …and the text form parses back to the same value, quoting and all.
    for literal in [
        r#"'{"a,b"}'"#,
        r#"'{"{x}"}'"#,
        r#"'{"NULL",NULL}'"#,
        r#"'{""}'"#,
    ] {
        let once = scalar(&mut s, &format!("SELECT {literal}::text[]")).await;
        let twice = scalar(&mut s, &format!("SELECT {literal}::text[]::text::text[]")).await;
        assert!(once == twice, "round-trip of {literal}");
    }
    // The unquoted word NULL parses as a SQL NULL element, the quoted one as text.
    assert!(scalar(&mut s, "SELECT ('{NULL}'::text[])[1] IS NULL").await == Some("t".to_string()));
    assert!(scalar(&mut s, r#"SELECT ('{"NULL"}'::text[])[1]"#).await == Some("NULL".to_string()));
}

// ---------------------------------------------------------------------------
// Subscripting
// ---------------------------------------------------------------------------

/// Subscripts are 1-based. Every out-of-range subscript is SQL NULL rather than
/// an error, that is 0, a negative one, and one past the end. A subscript of a
/// NULL array is NULL.
#[tokio::test]
async fn subscripting_is_one_based_and_out_of_range_is_null() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (id int4, a int4[])"]).await;
    run(
        &mut s,
        "INSERT INTO t VALUES (1, ARRAY[10,20,30]), (2, NULL)",
    )
    .await;

    expect_exprs(
        &mut s,
        &[
            ("(ARRAY[10,20,30])[1]", Some("10")),
            ("(ARRAY[10,20,30])[3]", Some("30")),
            ("(ARRAY[10,20,30])[0]", None),
            ("(ARRAY[10,20,30])[-1]", None),
            ("(ARRAY[10,20,30])[4]", None),
            ("(NULL::int4[])[1]", None),
            // The subscript expression may be computed.
            ("(ARRAY[10,20,30])[1 + 1]", Some("20")),
            ("(ARRAY[10,20,30])[NULL]", None),
            // Text elements subscript the same way.
            ("(ARRAY['a','b'])[2]", Some("b")),
        ],
    )
    .await;

    // Subscripting a stored column, including the NULL row.
    assert!(
        query(&mut s, "SELECT a[2] FROM t ORDER BY id").await == vec![row(&["20"]), vec![None]]
    );
}

/// A slice target's right-hand side is an array, unlike an element target's.
#[tokio::test]
async fn array_slice_assignments_accept_array_values_in_insert_and_update() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (id int4, a int4[])"]).await;
    run(&mut s, "INSERT INTO t (id, a[1:3]) VALUES (1, '{1,2,3}')").await;
    assert!(scalar(&mut s, "SELECT a FROM t WHERE id = 1").await == Some("{1,2,3}".into()));

    run(&mut s, "UPDATE t SET a[2:3] = ARRAY[20,30] WHERE id = 1").await;
    assert!(scalar(&mut s, "SELECT a FROM t WHERE id = 1").await == Some("{1,20,30}".into()));
}

// ---------------------------------------------------------------------------
// = ANY / <> ALL
// ---------------------------------------------------------------------------

/// `= ANY(...)` and `<> ALL(...)` over array literals and against a real
/// table's rows. This is the shape every ORM emits for an IN-list.
#[tokio::test]
async fn any_and_all_over_literals_and_table_rows() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (id int4, name text)",
        "INSERT INTO t VALUES (1,'a'), (2,'b'), (3,'c')",
    ])
    .await;

    expect_exprs(
        &mut s,
        &[
            ("2 = ANY(ARRAY[1,2,3])", Some("t")),
            ("9 = ANY(ARRAY[1,2,3])", Some("f")),
            ("9 <> ALL(ARRAY[1,2,3])", Some("t")),
            ("2 <> ALL(ARRAY[1,2,3])", Some("f")),
            ("'b' = ANY(ARRAY['a','b'])", Some("t")),
            // The array may come from a cast literal too.
            ("2 = ANY('{1,2}'::int4[])", Some("t")),
        ],
    )
    .await;

    // Against real rows, in the predicate.
    assert!(
        query(
            &mut s,
            "SELECT id FROM t WHERE id = ANY(ARRAY[1,3]) ORDER BY id"
        )
        .await
            == vec![row(&["1"]), row(&["3"])]
    );
    assert!(
        query(
            &mut s,
            "SELECT id FROM t WHERE name = ANY(ARRAY['a','c']) ORDER BY id"
        )
        .await
            == vec![row(&["1"]), row(&["3"])]
    );
    assert!(
        query(
            &mut s,
            "SELECT id FROM t WHERE id <> ALL(ARRAY[1,3]) ORDER BY id"
        )
        .await
            == vec![row(&["2"])]
    );

    // The array can also be a column of the row being tested.
    let (_engine2, mut c) = engine_with(&[
        "CREATE TABLE c (id int4, tags text[])",
        "INSERT INTO c VALUES (1, ARRAY['x','y']), (2, ARRAY['z'])",
    ])
    .await;
    assert!(
        query(&mut c, "SELECT id FROM c WHERE 'y' = ANY(tags) ORDER BY id").await
            == vec![row(&["1"])]
    );
}

/// The three-valued logic of `ANY`/`ALL`. A non-matching probe against an array
/// that contains NULLs is *unknown*, not false. `ANY(NULL)` is unknown. The
/// empty array short-circuits to false for ANY and true for ALL.
#[tokio::test]
async fn any_and_all_are_three_valued() {
    let (_engine, mut s) = engine_with(&[]).await;
    expect_exprs(
        &mut s,
        &[
            // A match wins even when NULLs are present.
            ("1 = ANY(ARRAY[1,NULL,3])", Some("t")),
            // No match plus a NULL element is NULL, not false.
            ("9 = ANY(ARRAY[1,NULL,3])", None),
            ("(9 = ANY(ARRAY[1,NULL,3])) IS NULL", Some("t")),
            // ALL mirrors it.
            ("9 <> ALL(ARRAY[1,NULL,3])", None),
            ("1 <> ALL(ARRAY[1,NULL,3])", Some("f")),
            // A NULL array is unknown either way.
            ("1 = ANY(NULL::int4[])", None),
            ("1 <> ALL(NULL::int4[])", None),
            // A NULL probe is unknown even against a NULL-free array.
            ("NULL = ANY(ARRAY[1,2])", None),
            // The empty array is the identity: ANY false, ALL true.
            ("1 = ANY(ARRAY[]::int4[])", Some("f")),
            ("1 <> ALL(ARRAY[]::int4[])", Some("t")),
        ],
    )
    .await;

    // A NULL-yielding ANY filters the row out, exactly like a false one.
    let (_engine2, mut t) = engine_with(&[
        "CREATE TABLE t (id int4, a int4[])",
        "INSERT INTO t VALUES (1, ARRAY[1,NULL]), (2, ARRAY[9])",
    ])
    .await;
    assert!(query(&mut t, "SELECT id FROM t WHERE 9 = ANY(a)").await == vec![row(&["2"])]);
}

// ---------------------------------------------------------------------------
// unnest
// ---------------------------------------------------------------------------

/// `unnest(...)` in FROM expands an array into rows, aliases with `AS u(x)`,
/// joins against a real table, and produces zero rows for NULL and empty input.
#[tokio::test]
async fn unnest_in_from_expands_joins_and_handles_empty_input() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (id int4, name text)",
        "INSERT INTO t VALUES (1,'a'), (2,'b')",
    ])
    .await;

    assert!(
        query(&mut s, "SELECT * FROM unnest(ARRAY[1,2,3])").await
            == vec![row(&["1"]), row(&["2"]), row(&["3"])]
    );
    // `AS u(x)` names the column.
    assert!(
        query(
            &mut s,
            "SELECT x FROM unnest(ARRAY[3,1,2]) AS u(x) ORDER BY x"
        )
        .await
            == vec![row(&["1"]), row(&["2"]), row(&["3"])]
    );
    // A bare `AS u` renames the column too, not just the qualifier: a function
    // in FROM yielding a single scalar takes its column name from the table
    // alias, so both `u` and `u.u` resolve.
    for sql in [
        "SELECT u FROM unnest(ARRAY[3,1,2]) AS u",
        "SELECT u.u FROM unnest(ARRAY[3,1,2]) AS u",
        // With no alias at all the column keeps the function's name.
        "SELECT unnest FROM unnest(ARRAY[3,1,2])",
    ] {
        assert!(
            query(&mut s, sql).await == vec![row(&["3"]), row(&["1"]), row(&["2"])],
            "{sql}"
        );
    }
    // Element order is the array's order, not sorted.
    assert!(
        query(&mut s, "SELECT x FROM unnest(ARRAY[3,1,2]) AS u(x)").await
            == vec![row(&["3"]), row(&["1"]), row(&["2"])]
    );
    // NULL elements come through as NULL rows.
    assert!(
        query(&mut s, "SELECT x FROM unnest(ARRAY[1,NULL]) AS u(x)").await
            == vec![row(&["1"]), vec![None]]
    );
    // NULL and empty inputs both produce zero rows.
    assert!(
        query(&mut s, "SELECT * FROM unnest(NULL::int4[])")
            .await
            .is_empty()
    );
    assert!(
        query(&mut s, "SELECT * FROM unnest(ARRAY[]::int4[])")
            .await
            .is_empty()
    );
    // Joined against a real table.
    assert!(
        query(
            &mut s,
            "SELECT t.name, u.x FROM t JOIN unnest(ARRAY[1,2]) AS u(x) ON u.x = t.id \
             ORDER BY t.id",
        )
        .await
            == vec![row(&["a", "1"]), row(&["b", "2"])]
    );
    // …and as a plain cross join.
    assert!(
        query(
            &mut s,
            "SELECT count(*) FROM t, unnest(ARRAY[1,2,3]) AS u(x)"
        )
        .await
            == vec![row(&["6"])]
    );
}

// ---------------------------------------------------------------------------
// array_agg
// ---------------------------------------------------------------------------

/// `array_agg` preserves input order and keeps NULL elements. Over zero rows it
/// is SQL NULL, not `{}`, which is the `PostgreSQL` behavior that surprises
/// people.
#[tokio::test]
async fn array_agg_preserves_order_and_is_null_over_zero_rows() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (id int4, grp int4, name text)",
        "INSERT INTO t VALUES (3,1,'c'), (1,1,'a'), (2,2,NULL)",
    ])
    .await;

    // Input order, not sorted order.
    assert!(
        scalar(
            &mut s,
            "SELECT array_agg(x) FROM unnest(ARRAY[3,1,2]) AS u(x)"
        )
        .await
            == Some("{3,1,2}".to_string())
    );
    // NULL rows are kept as NULL elements.
    assert!(
        scalar(&mut s, "SELECT array_agg(name) FROM t WHERE grp = 2").await
            == Some("{NULL}".to_string())
    );
    // Grouped.
    assert!(
        query(
            &mut s,
            "SELECT grp, count(*) FROM t GROUP BY grp ORDER BY grp"
        )
        .await
            == vec![row(&["1", "2"]), row(&["2", "1"])]
    );
    assert!(
        scalar(&mut s, "SELECT array_agg(id) FROM t WHERE grp = 2").await
            == Some("{2}".to_string())
    );
    // Zero rows: SQL NULL, not an empty array.
    assert!(scalar(&mut s, "SELECT array_agg(id) FROM t WHERE id < 0").await == None);
    assert!(
        scalar(
            &mut s,
            "SELECT array_agg(x) FROM unnest(ARRAY[]::int4[]) AS u(x)"
        )
        .await
            == None
    );
    // The result type is still the array type.
    let (_value, oid) = typed_scalar(&mut s, "array_agg(id) FROM t").await;
    assert!(oid == INT4_ARRAY_OID);
}

// ---------------------------------------------------------------------------
// Operators
// ---------------------------------------------------------------------------

/// `||` in all three forms, plus the `PostgreSQL` quirk that a concatenation
/// with a bare NULL leaves the array unchanged. The operand resolves as a NULL
/// *array*.
#[tokio::test]
async fn concat_has_three_forms() {
    let (_engine, mut s) = engine_with(&[]).await;
    expect_exprs(
        &mut s,
        &[
            ("ARRAY[1,2] || ARRAY[3,4]", Some("{1,2,3,4}")),
            ("ARRAY[1,2] || 3", Some("{1,2,3}")),
            ("0 || ARRAY[1,2]", Some("{0,1,2}")),
            ("ARRAY[]::int4[] || ARRAY[1]", Some("{1}")),
            ("ARRAY[1] || ARRAY[]::int4[]", Some("{1}")),
            ("ARRAY['a'] || 'b'", Some("{a,b}")),
            // A bare NULL operand concatenates as a NULL array: unchanged.
            ("ARRAY[1,2] || NULL", Some("{1,2}")),
            // A typed NULL *element* really is appended.
            ("ARRAY[1,2] || NULL::int4", Some("{1,2,NULL}")),
        ],
    )
    .await;
}

/// Containment, overlap, equality and element-wise ordering.
#[tokio::test]
async fn containment_overlap_and_ordering() {
    let (_engine, mut s) = engine_with(&[]).await;
    expect_exprs(
        &mut s,
        &[
            ("ARRAY[1,2,3] @> ARRAY[2]", Some("t")),
            ("ARRAY[1,2,3] @> ARRAY[3,1]", Some("t")),
            ("ARRAY[1,2,3] @> ARRAY[4]", Some("f")),
            ("ARRAY[1,2,3] @> ARRAY[]::int4[]", Some("t")),
            ("ARRAY[2] <@ ARRAY[1,2,3]", Some("t")),
            ("ARRAY[4] <@ ARRAY[1,2,3]", Some("f")),
            ("ARRAY[1,2] && ARRAY[2,5]", Some("t")),
            ("ARRAY[1,2] && ARRAY[7]", Some("f")),
            ("ARRAY[1,2] && ARRAY[]::int4[]", Some("f")),
            // Strict in both operands.
            ("NULL::int4[] @> ARRAY[1]", None),
            ("ARRAY[1] && NULL::int4[]", None),
            // Equality and ordering are element-wise.
            ("ARRAY[1,2] = ARRAY[1,2]", Some("t")),
            ("ARRAY[1,2] = ARRAY[2,1]", Some("f")),
            ("ARRAY[1,2] < ARRAY[1,3]", Some("t")),
            ("ARRAY[1] < ARRAY[1,0]", Some("t")),
        ],
    )
    .await;
}

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

/// `array_length` reports NULL for an empty array while `cardinality` reports
/// 0. This is the one place the two disagree.
#[tokio::test]
async fn array_length_and_cardinality_disagree_on_empty() {
    let (_engine, mut s) = engine_with(&[]).await;
    expect_exprs(
        &mut s,
        &[
            ("array_length(ARRAY[1,2,3], 1)", Some("3")),
            ("array_length(ARRAY[]::int4[], 1)", None),
            ("array_length(NULL::int4[], 1)", None),
            ("cardinality(ARRAY[1,2,3])", Some("3")),
            ("cardinality(ARRAY[]::int4[])", Some("0")),
            ("cardinality(NULL::int4[])", None),
            // NULL elements still count.
            ("array_length(ARRAY[1,NULL], 1)", Some("2")),
            ("cardinality(ARRAY[1,NULL])", Some("2")),
        ],
    )
    .await;
}

#[tokio::test]
async fn array_functions() {
    let (_engine, mut s) = engine_with(&[]).await;
    expect_exprs(
        &mut s,
        &[
            ("array_append(ARRAY[1,2], 3)", Some("{1,2,3}")),
            ("array_append(ARRAY[]::int4[], 1)", Some("{1}")),
            ("array_append(ARRAY[1], NULL::int4)", Some("{1,NULL}")),
            ("array_prepend(0, ARRAY[1,2])", Some("{0,1,2}")),
            ("array_cat(ARRAY[1], ARRAY[2,3])", Some("{1,2,3}")),
            ("array_cat(ARRAY[1], ARRAY[]::int4[])", Some("{1}")),
            // array_to_string skips NULLs unless a replacement is given.
            ("array_to_string(ARRAY[1,2,3], '-')", Some("1-2-3")),
            ("array_to_string(ARRAY[1,NULL,3], '-')", Some("1-3")),
            ("array_to_string(ARRAY[1,NULL,3], '-', 'x')", Some("1-x-3")),
            ("array_to_string(ARRAY[]::int4[], '-')", Some("")),
            ("array_to_string(NULL::int4[], '-')", None),
            // string_to_array is its inverse.
            ("string_to_array('a,b,c', ',')", Some("{a,b,c}")),
            ("string_to_array('a', ',')", Some("{a}")),
            ("string_to_array(NULL, ',')", None),
        ],
    )
    .await;
}

// ---------------------------------------------------------------------------
// Casts and element-type breadth
// ---------------------------------------------------------------------------

#[tokio::test]
async fn casts_between_text_and_arrays() {
    let (_engine, mut s) = engine_with(&[]).await;
    expect_exprs(
        &mut s,
        &[
            // array → text is the array's own text form.
            ("ARRAY[1,2]::text", Some("{1,2}")),
            // text → array parses that same form.
            ("'{1,2}'::text::int4[]", Some("{1,2}")),
            // Element-wise casts between array types.
            ("ARRAY[1,2]::text[]", Some("{1,2}")),
            ("ARRAY['1','2']::int4[]", Some("{1,2}")),
            ("ARRAY[1,NULL]::text[]", Some("{1,NULL}")),
            ("NULL::int4[]::text[]", None),
        ],
    )
    .await;

    // A bad element makes the whole cast fail.
    assert!(err_code(&mut s, "SELECT ARRAY['x']::int4[]").await == "22P02");
    assert!(err_code(&mut s, "SELECT '{x}'::int4[]").await == "22P02");
}

/// Every supported element type can be declared in DDL, stored, and read back.
///
/// These tests exercise `bytea[]` only as DDL. The engine has no text → bytea
/// cast, so no one can write a `bytea` array value in SQL at all. It can only be
/// bound as a parameter.
#[tokio::test]
async fn every_element_type_round_trips_through_a_column() {
    for (type_name, literal, want) in [
        ("bool", "ARRAY[true,false]", "{t,f}"),
        ("int4", "ARRAY[1,2]", "{1,2}"),
        ("int8", "ARRAY[9000000000]", "{9000000000}"),
        ("text", "ARRAY['a','b']", "{a,b}"),
        ("float8", "ARRAY[1.5::float8]", "{1.5}"),
        ("numeric", "ARRAY[1.50]", "{1.50}"),
        ("date", "ARRAY['2024-01-02'::date]", "{2024-01-02}"),
        ("time", "ARRAY['12:34:56'::time]", "{12:34:56}"),
        (
            "timestamp",
            "ARRAY['2024-01-02 03:04:05'::timestamp]",
            r#"{"2024-01-02 03:04:05"}"#,
        ),
        (
            "timestamptz",
            "ARRAY['2024-01-02 03:04:05+00'::timestamptz]",
            r#"{"2024-01-02 03:04:05+00"}"#,
        ),
        ("interval", "ARRAY['1 day'::interval]", r#"{"1 day"}"#),
        (
            "uuid",
            "ARRAY['550e8400-e29b-41d4-a716-446655440000'::uuid]",
            "{550e8400-e29b-41d4-a716-446655440000}",
        ),
        (
            "jsonb",
            r#"ARRAY['{"b":2,"a":1}'::jsonb]"#,
            r#"{"{\"a\": 1, \"b\": 2}"}"#,
        ),
    ] {
        let (_engine, mut s) = engine_with(&[&format!("CREATE TABLE t (v {type_name}[])")]).await;
        run(&mut s, &format!("INSERT INTO t VALUES ({literal})")).await;
        run(&mut s, "INSERT INTO t VALUES (NULL)").await;
        assert!(
            query(&mut s, "SELECT v FROM t").await
                == vec![vec![Some(want.to_string())], vec![None]],
            "{type_name}[] round-trip"
        );
    }

    // `bytea[]` is declarable; only its value literal is out of reach.
    let (_engine, mut s) = engine_with(&["CREATE TABLE b (v bytea[])"]).await;
    run(&mut s, "INSERT INTO b VALUES (NULL)").await;
    assert!(query(&mut s, "SELECT v FROM b").await == vec![vec![None]]);
}

/// The open element descriptor is exercised through kinds that used to have a
/// catalog-only array row: geometric, network, bit, system-id, and reg types.
#[tokio::test]
async fn uncommon_builtin_arrays_keep_their_element_and_array_identity() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE builtin_arrays (p point[], n inet[], b bit(3)[], o oid[], r regclass[])",
        "INSERT INTO builtin_arrays VALUES (ARRAY['(1,2)'::point], ARRAY['192.168.1.2'::inet], ARRAY[B'101'::bit(3)], ARRAY[42::oid], ARRAY['pg_type'::regclass])",
        "CREATE TABLE builtin_array_keys (id int4)",
        "INSERT INTO builtin_array_keys VALUES (1)",
    ])
    .await;

    assert!(
        query(&mut s, "SELECT p, n, b, o, r FROM builtin_arrays").await
            == vec![row(&[
                r#"{"(1,2)"}"#,
                "{192.168.1.2}",
                "{101}",
                "{42}",
                "{pg_type}",
            ])]
    );
    assert!(
        query(
            &mut s,
            "SELECT k.id, a.r FROM builtin_array_keys k JOIN builtin_arrays a ON true",
        )
        .await
            == vec![row(&["1", "{pg_type}"])]
    );
    for (column, type_name) in [
        ("p", "point"),
        ("n", "inet"),
        ("b", "bit"),
        ("o", "oid"),
        ("r", "regclass"),
    ] {
        let (_, oid) = typed_scalar(&mut s, &format!("{column} FROM builtin_arrays")).await;
        let expected = scalar(
            &mut s,
            &format!("SELECT typarray FROM pg_type WHERE typname = '{type_name}'"),
        )
        .await
        .expect("array oid")
        .parse::<u32>()
        .expect("valid oid");
        assert!(oid == expected, "{type_name}[] result oid");
    }
}

// ---------------------------------------------------------------------------
// Storage, ordering, unique indexes
// ---------------------------------------------------------------------------

/// Arrays round-trip through storage, including NULL elements and a NULL
/// column. They order element-wise, and they group by value.
#[tokio::test]
async fn arrays_round_trip_through_storage_and_order_element_wise() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (id int4, a int4[], s text[])",
        "INSERT INTO t VALUES (1, ARRAY[1,2], ARRAY['x','y'])",
        "INSERT INTO t VALUES (2, '{3,NULL}', NULL)",
        "INSERT INTO t VALUES (3, ARRAY[]::int4[], ARRAY['a,b'])",
    ])
    .await;

    assert!(
        query(&mut s, "SELECT id, a, s FROM t ORDER BY id").await
            == vec![
                row(&["1", "{1,2}", "{x,y}"]),
                vec![Some("2".to_string()), Some("{3,NULL}".to_string()), None],
                row(&["3", "{}", r#"{"a,b"}"#]),
            ]
    );
    // Element-wise ordering, with the empty array first.
    assert!(
        query(&mut s, "SELECT id FROM t ORDER BY a").await
            == vec![row(&["3"]), row(&["1"]), row(&["2"])]
    );
    // UPDATE through an array operator rewrites the stored value.
    run(&mut s, "UPDATE t SET a = a || 9 WHERE id = 1").await;
    assert!(scalar(&mut s, "SELECT a FROM t WHERE id = 1").await == Some("{1,2,9}".to_string()));
    // Grouping is by value.
    assert!(query(&mut s, "SELECT count(DISTINCT a) FROM t").await == vec![row(&["3"])]);
}

/// A unique array index keys off the canonical value. The two literal spellings
/// are one key, and numeric scale inside the elements does not create a second.
#[tokio::test]
async fn unique_index_canonicalizes_array_keys() {
    for (ddl, first, second) in [
        ("CREATE TABLE u (a int4[] UNIQUE)", "'{1,2}'", "ARRAY[1,2]"),
        ("CREATE TABLE u (a numeric[] UNIQUE)", "'{1.0}'", "'{1.00}'"),
        (
            "CREATE TABLE u (a jsonb[] UNIQUE)",
            r#"ARRAY['{"b":2,"a":1}'::jsonb]"#,
            r#"ARRAY['{"a":1,"b":2}'::jsonb]"#,
        ),
    ] {
        let (_engine, mut s) = engine_with(&[ddl]).await;
        run(&mut s, &format!("INSERT INTO u VALUES ({first})")).await;
        assert!(
            err_code(&mut s, &format!("INSERT INTO u VALUES ({second})")).await == "23505",
            "{first} then {second}"
        );
    }

    // Different arrays (including a permutation) are different keys.
    let (_engine, mut s) = engine_with(&["CREATE TABLE u (a int4[] UNIQUE)"]).await;
    for values in ["ARRAY[1,2]", "ARRAY[2,1]", "ARRAY[1]", "ARRAY[]::int4[]"] {
        run(&mut s, &format!("INSERT INTO u VALUES ({values})")).await;
    }
    assert!(query(&mut s, "SELECT count(*) FROM u").await == vec![row(&["4"])]);
}

// ---------------------------------------------------------------------------
// Column defaults
// ---------------------------------------------------------------------------

/// The engine evaluates an array column DEFAULT at DDL time, writes it into the
/// catalog, applies it to an INSERT that omits the column, and renders it back
/// as a quoted literal. That holds for a literal DEFAULT and an `ARRAY[...]`
/// one, for an empty one, and for one that holds NULL elements. All of it still
/// holds for a fresh engine that re-reads the catalog from storage.
#[tokio::test]
async fn array_column_defaults_persist_apply_and_render() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
    let mut s = engine.connect();
    run(
        &mut s,
        "CREATE TABLE t (
           id int4,
           nums int4[] DEFAULT '{1,2}',
           built text[] DEFAULT ARRAY['a','b c'],
           holes int4[] DEFAULT '{1,NULL,3}',
           empty text[] DEFAULT '{}',
           days date[] DEFAULT '{2020-01-02}'
         )",
    )
    .await;

    let defaults = row(&["{1,2}", r#"{a,"b c"}"#, "{1,NULL,3}", "{}", "{2020-01-02}"]);
    let columns = "SELECT nums, built, holes, empty, days FROM t WHERE id = ";
    run(&mut s, "INSERT INTO t (id) VALUES (1)").await;
    assert!(query(&mut s, &format!("{columns}1")).await == vec![defaults.clone()]);
    // An explicit NULL still beats the default.
    run(&mut s, "INSERT INTO t (id, nums) VALUES (2, NULL)").await;
    assert!(scalar(&mut s, "SELECT nums FROM t WHERE id = 2").await == None);

    assert!(column_default(&mut s, "t", "nums").await == Some("'{1,2}'::integer[]".into()));
    assert!(
        column_default(&mut s, "t", "built").await == Some("ARRAY['a'::text, 'b c'::text]".into())
    );
    assert!(column_default(&mut s, "t", "holes").await == Some("'{1,NULL,3}'::integer[]".into()));
    assert!(column_default(&mut s, "t", "empty").await == Some("'{}'::text[]".into()));

    // Re-read the catalog: a new engine over the same store deserializes the
    // stored default rather than reusing the one built at DDL time.
    drop(s);
    drop(engine);
    let reopened = SqlEngine::with_kv(Arc::clone(&kv)).expect("reopen engine");
    let mut s = reopened.connect();
    run(&mut s, "INSERT INTO t (id) VALUES (3)").await;
    assert!(query(&mut s, &format!("{columns}3")).await == vec![defaults]);
    assert!(column_default(&mut s, "t", "holes").await == Some("'{1,NULL,3}'::integer[]".into()));
}

// ---------------------------------------------------------------------------
// Slices, dimensions, and bounded element types
// ---------------------------------------------------------------------------

/// `array_get_slice`. A range clips to the array rather than reporting an
/// error. An inverted or wholly-out-of-range range gives the empty array. An
/// omitted bound takes the array's own. A NULL bound makes the whole slice
/// NULL.
#[tokio::test]
async fn array_slices_clip_to_the_array_instead_of_erroring() {
    let (_engine, mut s) = engine_with(&[]).await;
    for (expr, expected) in [
        ("(ARRAY[1,2,3])[1:2]", Some("{1,2}")),
        // Below the lower bound and past the upper bound both clip.
        ("(ARRAY[1,2,3])[0:2]", Some("{1,2}")),
        ("(ARRAY[1,2,3])[2:9]", Some("{2,3}")),
        // Entirely outside, and inverted, are empty — not an error.
        ("(ARRAY[1,2,3])[5:9]", Some("{}")),
        ("(ARRAY[1,2,3])[3:1]", Some("{}")),
        // An omitted bound takes the array's own.
        ("(ARRAY[1,2,3])[:2]", Some("{1,2}")),
        ("(ARRAY[1,2,3])[2:]", Some("{2,3}")),
        ("(ARRAY[1,2,3])[:]", Some("{1,2,3}")),
        ("(ARRAY[1,2,3])[NULL:2]", None),
    ] {
        assert!(
            scalar(&mut s, &format!("SELECT {expr}")).await == expected.map(str::to_string),
            "{expr}"
        );
    }
}

/// Multidimensional arrays in both spellings, their dimension accessors, and the
/// two ways of building a ragged one. `PostgreSQL` rejects the two with distinct
/// SQLSTATEs, which depend on whether the raggedness is in a literal or in a
/// constructor.
#[tokio::test]
async fn multidimensional_arrays_carry_their_dimensions() {
    let (_engine, mut s) = engine_with(&[]).await;
    let two_by_two = "'{{1,2},{3,4}}'::int4[]";
    for (expr, expected) in [
        ("'{{1,2}}'::int4[]", "{{1,2}}"),
        ("ARRAY[ARRAY[1]]", "{{1}}"),
        (&format!("array_dims({two_by_two})"), "[1:2][1:2]"),
        (&format!("array_ndims({two_by_two})"), "2"),
        (&format!("array_length({two_by_two}, 2)"), "2"),
        (&format!("array_upper({two_by_two}, 1)"), "2"),
        (&format!("cardinality({two_by_two})"), "4"),
        // A full subscript chain reads an element; a chain of slices keeps the
        // dimension count.
        (&format!("({two_by_two})[2][1]"), "3"),
        (&format!("({two_by_two})[1:1][1:2]"), "{{1,2}}"),
        // Concatenation joins the outermost dimension; equality and `= ANY` see
        // through every dimension.
        (
            "array_cat('{{1,2}}'::int4[], '{{3,4}}'::int4[])",
            "{{1,2},{3,4}}",
        ),
        ("'{{1,2}}'::int4[] = '{{1,2}}'::int4[]", "t"),
        (&format!("3 = ANY({two_by_two})"), "t"),
    ] {
        assert!(
            scalar(&mut s, &format!("SELECT {expr}")).await == Some(expected.to_string()),
            "{expr}"
        );
    }

    // A chain shorter than the dimension count reads NULL rather than a row.
    assert!(scalar(&mut s, &format!("SELECT ({two_by_two})[2]")).await == None);
    // `unnest` flattens every dimension.
    assert!(
        query(&mut s, &format!("SELECT unnest({two_by_two})")).await
            == vec![row(&["1"]), row(&["2"]), row(&["3"]), row(&["4"])]
    );

    for (sql, code) in [
        ("SELECT '{{1,2},{3}}'::int4[]", "22P02"),
        ("SELECT ARRAY[ARRAY[1],ARRAY[2,3]]", "2202E"),
        // `array_append`/`array_prepend` stay one-dimensional.
        ("SELECT array_append('{{1,2}}'::int4[], 5)", "22000"),
    ] {
        assert!(err_code(&mut s, sql).await == code, "{sql}");
    }
}

#[tokio::test]
async fn array_functions_accept_a_domain_over_an_array() {
    let (_engine, mut s) = engine_with(&[]).await;
    run(&mut s, "CREATE DOMAIN dimensioned_ints AS int4[]").await;

    for (expr, expected) in [
        ("array_dims('{1,2,3}'::dimensioned_ints)", "[1:3]"),
        ("array_length('{1,2,3}'::dimensioned_ints, 1)", "3"),
        (
            "array_append('{1,2,3}'::dimensioned_ints, 4)::text",
            "{1,2,3,4}",
        ),
    ] {
        assert!(
            scalar(&mut s, &format!("SELECT {expr}")).await == Some(expected.to_string()),
            "{expr}"
        );
    }
}

#[tokio::test]
async fn adding_a_domain_constraint_rejects_existing_values_without_storing_it() {
    let (_engine, mut s) = engine_with(&[]).await;
    run(&mut s, "CREATE DOMAIN constrained_int AS int4").await;
    run(
        &mut s,
        "CREATE TABLE constrained_values (value constrained_int)",
    )
    .await;
    run(&mut s, "INSERT INTO constrained_values VALUES (1)").await;

    let error = s
        .simple_query("ALTER DOMAIN constrained_int ADD CONSTRAINT negative_only CHECK (VALUE < 0)")
        .await
        .expect_err("existing value rejects the new domain constraint");
    assert!(error.code == "23514");
    assert!(
        error.message
            == "column \"value\" of table \"constrained_values\" contains values that violate the new constraint"
    );

    run(&mut s, "INSERT INTO constrained_values VALUES (2)").await;
    assert!(
        scalar(&mut s, "SELECT count(*)::text FROM constrained_values").await == Some("2".into())
    );
}

#[tokio::test]
async fn a_not_valid_domain_constraint_checks_new_values_then_validates() {
    let (_engine, mut s) = engine_with(&[]).await;
    run(&mut s, "CREATE DOMAIN negative_int AS int4").await;
    run(&mut s, "CREATE TABLE negative_values (value negative_int)").await;
    run(&mut s, "INSERT INTO negative_values VALUES (1)").await;
    run(
        &mut s,
        "ALTER DOMAIN negative_int ADD CONSTRAINT negative_only CHECK (VALUE < 0) NOT VALID",
    )
    .await;

    let error = s
        .simple_query("INSERT INTO negative_values VALUES (2)")
        .await
        .expect_err("NOT VALID domain constraint checks new values");
    assert!(error.code == "23514");
    let error = s
        .simple_query("ALTER DOMAIN negative_int VALIDATE CONSTRAINT negative_only")
        .await
        .expect_err("validation checks existing values");
    assert!(error.code == "23514");

    run(&mut s, "UPDATE negative_values SET value = -1").await;
    run(
        &mut s,
        "ALTER DOMAIN negative_int VALIDATE CONSTRAINT negative_only",
    )
    .await;
}

#[tokio::test]
async fn adding_a_domain_not_null_constraint_back_validates_and_can_be_dropped() {
    let (_engine, mut s) = engine_with(&[]).await;
    run(&mut s, "CREATE DOMAIN nullable_int AS int4").await;
    run(&mut s, "CREATE TABLE nullable_values (value nullable_int)").await;
    run(&mut s, "INSERT INTO nullable_values VALUES (NULL)").await;

    let error = s
        .simple_query("ALTER DOMAIN nullable_int ADD NOT NULL")
        .await
        .expect_err("existing NULL rejects domain NOT NULL");
    assert!(error.code == "23502");
    assert!(error.message == "column \"value\" of table \"nullable_values\" contains null values");

    run(&mut s, "UPDATE nullable_values SET value = 1").await;
    run(
        &mut s,
        "ALTER DOMAIN nullable_int ADD CONSTRAINT nullable_int_nn NOT NULL",
    )
    .await;
    assert!(err_code(&mut s, "INSERT INTO nullable_values VALUES (NULL)").await == "23502");
    run(
        &mut s,
        "ALTER DOMAIN nullable_int DROP CONSTRAINT nullable_int_nn",
    )
    .await;
    run(&mut s, "INSERT INTO nullable_values VALUES (NULL)").await;
}

#[tokio::test]
async fn a_named_create_domain_not_null_constraint_can_be_dropped() {
    let (_engine, mut s) = engine_with(&[]).await;
    run(
        &mut s,
        "CREATE DOMAIN named_not_null AS int4 CONSTRAINT named_not_null_nn NOT NULL",
    )
    .await;
    run(
        &mut s,
        "CREATE TABLE named_not_null_values (value named_not_null)",
    )
    .await;
    assert!(err_code(&mut s, "INSERT INTO named_not_null_values VALUES (NULL)").await == "23502");
    run(
        &mut s,
        "ALTER DOMAIN named_not_null DROP CONSTRAINT named_not_null_nn",
    )
    .await;
    run(&mut s, "INSERT INTO named_not_null_values VALUES (NULL)").await;
}

/// An array of a bounded string carries the modifier on its element type, so the
/// engine enforces the bound per element. The modifier also survives the catalog
/// round trip. A bare element-code byte would have dropped it, and turned
/// `varchar(3)[]` into an unbounded `varchar[]` that accepted anything.
#[tokio::test]
async fn an_array_of_a_bounded_string_enforces_the_bound_on_every_element() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE d (v varchar(3)[], c char(2)[])"]).await;
    run(
        &mut s,
        "INSERT INTO d VALUES (ARRAY['ab','cd'], ARRAY['x'])",
    )
    .await;
    assert!(query(&mut s, "SELECT v, c FROM d").await == vec![row(&["{ab,cd}", "{\"x \"}"])]);

    // Storing an over-long element is 22001, whichever spelling delivers it —
    // an `ARRAY[...]` constructor or an array literal.
    for sql in [
        "INSERT INTO d (v) VALUES (ARRAY['abcd'])",
        "INSERT INTO d (v) VALUES ('{abcd}')",
    ] {
        assert!(err_code(&mut s, sql).await == "22001", "{sql}");
    }

    // An explicit cast truncates instead, the same split the scalar types make.
    assert!(
        scalar(&mut s, "SELECT ARRAY['abcd']::varchar(3)[]").await == Some("{abc}".to_string())
    );
}

/// Arrays live in hash-sharded tables. A request to shard *on* an array column
/// is refused at CREATE TABLE. An array value has no shard-key hash, so the
/// engine never creates the table, rather than failing at every INSERT.
#[tokio::test]
async fn sharded_tables_store_arrays_and_refuse_an_array_shard_key() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE ok (id int4 NOT NULL, a int4[]) SHARDED BY HASH (id) BUCKETS 4",
        "INSERT INTO ok VALUES (1, ARRAY[1,2])",
    ])
    .await;
    assert!(query(&mut s, "SELECT id, a FROM ok").await == vec![row(&["1", "{1,2}"])]);
    assert!(scalar(&mut s, "SELECT id FROM ok WHERE 2 = ANY(a)").await == Some("1".to_string()));

    let error = s
        .simple_query("CREATE TABLE bad (id int4 NOT NULL, a int4[]) SHARDED BY HASH (a) BUCKETS 4")
        .await
        .expect_err("an array shard key is refused");
    assert!(error.code == "0A000");
    assert!(
        error.message == "hash shard key column \"a\" of type integer[] is not supported",
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

/// An array parameter bound in the *text* format. The client sends the `{…}`
/// literal and the engine parses it, including the empty and NULL-element
/// cases.
#[tokio::test]
async fn text_format_array_parameters_bind_and_store() {
    let (_engine, mut s) = engine_with(&[
        "CREATE TABLE t (id int4, a int4[])",
        "INSERT INTO t VALUES (1, NULL), (2, NULL), (3, NULL)",
    ])
    .await;

    s.parse("upd", "UPDATE t SET a = $2 WHERE id = $1", &[])
        .await
        .expect("parse");
    for (id, literal, want) in [
        ("1", "{1,2}", Some("{1,2}".to_string())),
        ("2", "{}", Some("{}".to_string())),
        ("3", "{4,NULL}", Some("{4,NULL}".to_string())),
    ] {
        let portal = format!("p{id}");
        s.bind(&portal, "upd", &[text_param(id), text_param(literal)], &[])
            .await
            .expect("bind text array parameter");
        s.execute(&portal, 0).await.expect("execute");
        assert!(scalar(&mut s, &format!("SELECT a FROM t WHERE id = {id}")).await == want);
    }

    // `= ANY($1)` with a text-format array parameter.
    s.parse(
        "sel",
        "SELECT id FROM t WHERE id = ANY($1) ORDER BY id",
        &[],
    )
    .await
    .expect("parse any");
    s.bind("q", "sel", &[text_param("{1,3}")], &[])
        .await
        .expect("bind");
    assert!(execute_rows(&mut s, "q").await == vec![row(&["1"]), row(&["3"])]);

    // Malformed array text is rejected with 22P02.
    let mut code = s
        .bind("r", "sel", &[text_param("1,3")], &[])
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

/// Binary-format array parameters and results through a real client. This is the
/// shape every driver emits for `= ANY($1)`. The empty array's 12-byte
/// `ndim = 0` header, which is what tokio-postgres sends, must round-trip.
#[tokio::test]
async fn binary_format_array_parameters_round_trip() {
    let client = connect(spawn().await).await;
    client
        .batch_execute("CREATE TABLE t (id int4, name text)")
        .await
        .expect("create");
    client
        .batch_execute("INSERT INTO t VALUES (1,'a'), (2,'b'), (3,'c')")
        .await
        .expect("insert");

    // int4[] and text[] parameters drive `= ANY($1)`.
    let by_id = client
        .query(
            "SELECT id FROM t WHERE id = ANY($1) ORDER BY id",
            &[&vec![1_i32, 3]],
        )
        .await
        .expect("bind int4[] parameter");
    assert!(by_id.iter().map(|r| r.get::<_, i32>(0)).collect::<Vec<_>>() == vec![1, 3]);

    let by_name = client
        .query(
            "SELECT id FROM t WHERE name = ANY($1) ORDER BY id",
            &[&vec!["a".to_string(), "c".to_string()]],
        )
        .await
        .expect("bind text[] parameter");
    assert!(
        by_name
            .iter()
            .map(|r| r.get::<_, i32>(0))
            .collect::<Vec<_>>()
            == vec![1, 3]
    );

    // The empty array. `postgres-types` always writes exactly one dimension, so
    // an empty slice goes out as `ndim = 1, len = 0` rather than the 12-byte
    // `ndim = 0` header crabka's own encoder emits.
    let empty: Vec<i32> = Vec::new();
    let none = client
        .query("SELECT id FROM t WHERE id = ANY($1)", &[&empty])
        .await
        .expect("bind empty int4[] parameter");
    assert!(none.is_empty());
    let rendered = client
        .query_one("SELECT $1::int4[]::text", &[&empty])
        .await
        .expect("render empty array parameter");
    assert!(rendered.get::<_, &str>(0) == "{}");

    // PostgreSQL has no zero-length dimension, so `array_recv` collapses that
    // header: the empty array has no dimensions at all, every dimension
    // accessor is NULL, and it is equal to `'{}'`. Only `cardinality` — which
    // counts elements rather than reading the header — answers 0.
    let describe = client
        .prepare_typed(
            "SELECT array_ndims($1), array_dims($1), array_length($1, 1), \
             array_lower($1, 1), array_upper($1, 1), cardinality($1), $1 = '{}'::int4[]",
            &[Type::INT4_ARRAY],
        )
        .await
        .expect("prepare empty array describe");
    let shape = client
        .query_one(&describe, &[&empty])
        .await
        .expect("describe empty array parameter");
    let shape = (
        shape.get::<_, Option<i32>>(0),
        shape.get::<_, Option<String>>(1),
        shape.get::<_, Option<i32>>(2),
        shape.get::<_, Option<i32>>(3),
        shape.get::<_, Option<i32>>(4),
        shape.get::<_, Option<i32>>(5),
        shape.get::<_, Option<bool>>(6),
    );
    assert!(shape == (None, None, None, None, None, Some(0), Some(true)));

    // A one-element parameter still has exactly one dimension.
    let shape = client
        .query_one(&describe, &[&vec![7_i32]])
        .await
        .expect("describe one-element array parameter");
    let shape = (
        shape.get::<_, Option<i32>>(0),
        shape.get::<_, Option<String>>(1),
        shape.get::<_, Option<i32>>(2),
        shape.get::<_, Option<i32>>(3),
        shape.get::<_, Option<i32>>(4),
        shape.get::<_, Option<i32>>(5),
        shape.get::<_, Option<bool>>(6),
    );
    assert!(
        shape
            == (
                Some(1),
                Some("[1:1]".to_string()),
                Some(1),
                Some(1),
                Some(1),
                Some(1),
                Some(false),
            )
    );

    // NULL elements survive the binary encoding in both directions.
    let with_nulls: Vec<Option<String>> = vec![Some("a,b".to_string()), None];
    let rendered = client
        .query_one("SELECT $1::text[]::text", &[&with_nulls])
        .await
        .expect("bind text[] with NULL element");
    assert!(rendered.get::<_, &str>(0) == r#"{"a,b",NULL}"#);

    // Array results decode client-side, and are described with the array OID.
    let read_back = client
        .query_one("SELECT ARRAY[4,5]", &[])
        .await
        .expect("select array");
    assert!(*read_back.columns()[0].type_() == Type::INT4_ARRAY);
    assert!(read_back.get::<_, Vec<i32>>(0) == vec![4, 5]);

    let nullable = client
        .query_one("SELECT ARRAY['x', NULL]", &[])
        .await
        .expect("select nullable array");
    assert!(nullable.get::<_, Vec<Option<String>>>(0) == vec![Some("x".to_string()), None]);

    // A stored array column round-trips through the binary path too.
    client
        .batch_execute("CREATE TABLE a (v int4[])")
        .await
        .expect("create array table");
    client
        .execute("INSERT INTO a VALUES ($1)", &[&vec![7_i32, 8]])
        .await
        .expect("insert binary array");
    let stored = client
        .query_one("SELECT v FROM a", &[])
        .await
        .expect("select array column");
    assert!(stored.get::<_, Vec<i32>>(0) == vec![7, 8]);
}
