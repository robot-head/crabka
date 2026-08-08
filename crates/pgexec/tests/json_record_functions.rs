//! The `json`/`jsonb` record-mapping family end to end: `json_populate_record`,
//! `json_populate_recordset`, `json_to_record`, `json_to_recordset` and the four
//! `jsonb_` twins.
//!
//! Every expectation here was taken from `PostgreSQL` 18.4 rather than from the
//! implementation, including the ones where the two families disagree — which
//! they do in more places than is comfortable:
//!
//! * a field landing in a text column keeps the `json` document's original
//!   spelling (`{"a" :  1}`, `1e3`) and gets `jsonb`'s canonical one
//!   (`{"a": 1}`, `1000`);
//! * `json_populate_recordset` on an object says "on an object" where
//!   `jsonb_populate_recordset` says "on a non-array";
//! * and yet the *composite*-shape refusal is identical for both, naming
//!   `PostgreSQL`'s internal `populate_composite` rather than either SQL function.
//!
//! The column-definition-list rules are here too, including for functions with
//! no JSON in them at all: the list is a property of FROM items generally, and
//! `generate_series(…) AS g(a int)` has to keep earning its own 42601.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};
use tokio::sync::{Mutex, MutexGuard};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

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

fn field_names(r: &QueryResult) -> Vec<String> {
    match r {
        QueryResult::Rows { fields, .. } => fields.iter().map(|f| f.name.clone()).collect(),
        o => panic!("expected Rows, got {o:?}"),
    }
}

async fn result(s: &mut SqlSession, sql: &str) -> QueryResult {
    s.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed, got {}: {}", e.code, e.message))
        .pop()
        .expect("one result")
}

async fn query(s: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    rows_text(&result(s, sql).await)
}

/// The single cell of a single-row, single-column query.
async fn scalar(s: &mut SqlSession, sql: &str) -> Option<String> {
    let rows = query(s, sql).await;
    assert!(rows.len() == 1, "`{sql}` should produce one row");
    assert!(rows[0].len() == 1, "`{sql}` should produce one column");
    rows[0][0].clone()
}

/// The SQLSTATE and message of a statement that must fail.
async fn failure(s: &mut SqlSession, sql: &str) -> (String, String) {
    let e = s
        .simple_query(sql)
        .await
        .unwrap_or_else(|_| panic!("`{sql}` should have failed"))
        .pop();
    panic!("`{sql}` unexpectedly succeeded with {e:?}")
}

async fn error_of(s: &mut SqlSession, sql: &str) -> (String, String) {
    match s.simple_query(sql).await {
        Err(e) => (e.code, e.message),
        Ok(_) => failure(s, sql).await,
    }
}

/// The SQLSTATE, message, `DETAIL` and `HINT` of a statement that must fail.
/// Several of this family's refusals put the load-bearing part in `DETAIL` or
/// `HINT` rather than the message.
async fn diagnosed(
    s: &mut SqlSession,
    sql: &str,
) -> (String, String, Option<String>, Option<String>) {
    let Err(e) = s.simple_query(sql).await else {
        let (code, message) = failure(s, sql).await;
        return (code, message, None, None);
    };
    let (detail, hint) = e.diagnostics.map_or((None, None), |d| (d.detail, d.hint));
    (e.code, e.message, detail, hint)
}

async fn engine_with(setup: &[&str]) -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    for sql in setup {
        s.simple_query(sql)
            .await
            .unwrap_or_else(|e| panic!("setup `{sql}` failed: {}", e.message));
    }
    (engine, s)
}

/// Serialises the tests that create user-defined types.
///
/// Type oids are allocated from a *per-catalog* counter but resolved through a
/// *process-wide* registry, so two `SqlEngine`s in one process hand the same oid
/// to different types and then resolve each other's. That never happens in the
/// server, which runs one catalog per process; it happens readily here, where
/// every test builds its own engine. Holding this while a test needs its types
/// keeps that out of the way of what these tests are actually about.
static USER_TYPES: Mutex<()> = Mutex::const_new(());

/// A session whose engine has `setup` applied, serialised against every other
/// user-type-creating test in this binary. The guard is returned so the caller
/// holds it for as long as the types have to stay resolvable.
async fn engine_with_types(setup: &[&str]) -> (MutexGuard<'static, ()>, SqlEngine, SqlSession) {
    let guard = USER_TYPES.lock().await;
    let (engine, session) = engine_with(setup).await;
    (guard, engine, session)
}

/// `CREATE TYPE jpop AS (a text, b int, c timestamp)` — the composite the
/// upstream corpus populates, and the one every `populate_*` case here uses.
const JPOP: &[&str] = &["CREATE TYPE jpop AS (a text, b int, c timestamp)"];

fn row(values: &[Option<&str>]) -> Vec<Option<String>> {
    values.iter().map(|v| v.map(ToString::to_string)).collect()
}

// ---------------------------------------------------------------------------
// Populating a named composite
// ---------------------------------------------------------------------------

/// Both halves of `populate_record`: keys map onto attributes by name, unknown
/// keys are ignored, and an attribute the document omits keeps whatever the base
/// row held — while an attribute present as JSON `null` is overridden to SQL
/// NULL rather than falling back.
#[tokio::test]
async fn populate_record_maps_by_name_and_inherits_only_absent_fields() {
    let (_types, _e, mut s) = engine_with_types(JPOP).await;
    let base = "row('x',3,'2012-12-31 15:30:56')::jpop";

    for family in ["json", "jsonb"] {
        let cases: &[(&str, &str, Vec<Option<String>>)] = &[
            // A NULL base leaves every unmatched attribute NULL.
            (
                "null::jpop",
                r#"'{"a":"blurfl","x":43.2}'"#,
                row(&[Some("blurfl"), None, None]),
            ),
            // A row base supplies them instead.
            (
                base,
                r#"'{"a":"blurfl","x":43.2}'"#,
                row(&[Some("blurfl"), Some("3"), Some("2012-12-31 15:30:56")]),
            ),
            (
                base,
                "'{}'",
                row(&[Some("x"), Some("3"), Some("2012-12-31 15:30:56")]),
            ),
            // Present-but-null beats the base.
            (
                base,
                r#"'{"a":null}'"#,
                row(&[None, Some("3"), Some("2012-12-31 15:30:56")]),
            ),
            // A NULL *document* is not the empty document: it leaves the base
            // row untouched and still produces exactly one row.
            (
                base,
                &format!("null::{family}"),
                row(&[Some("x"), Some("3"), Some("2012-12-31 15:30:56")]),
            ),
            // Key lookup is exact, so `A` does not populate `a`.
            ("null::jpop", r#"'{"A":"x"}'"#, row(&[None, None, None])),
        ];
        for (base, document, expected) in cases {
            let sql = format!("SELECT * FROM {family}_populate_record({base}, {document}) q");
            assert!(query(&mut s, &sql).await == vec![expected.clone()], "{sql}");
        }
    }
}

/// A composite-shaped refusal is one wording for both families, and it names
/// `PostgreSQL`'s internal routine rather than either SQL function.
#[tokio::test]
async fn a_non_object_document_is_refused_as_populate_composite() {
    let (_types, _e, mut s) = engine_with_types(JPOP).await;
    for family in ["json", "jsonb"] {
        let cases: &[(&str, &str)] = &[
            ("'[1,2]'", "cannot call populate_composite on an array"),
            ("'3'", "cannot call populate_composite on a scalar"),
            ("'null'", "cannot call populate_composite on a scalar"),
            ("'\"s\"'", "cannot call populate_composite on a scalar"),
            ("'true'", "cannot call populate_composite on a scalar"),
        ];
        for (document, message) in cases {
            let sql = format!("SELECT * FROM {family}_populate_record(null::jpop, {document}) q");
            assert!(
                error_of(&mut s, &sql).await == ("22023".into(), (*message).into()),
                "{sql}"
            );
        }
    }
}

/// The recordset half words its non-array refusal *differently* per family:
/// `json` reports what its parser tripped over, `jsonb` only that the root was
/// not an array. The per-element refusal is then shared again.
#[tokio::test]
async fn recordset_shape_refusals_differ_between_the_two_families() {
    let (_types, _e, mut s) = engine_with_types(JPOP).await;
    let cases: &[(&str, &str, &str)] = &[
        (
            "json",
            r#"'{"a":1}'"#,
            "cannot call json_populate_recordset on an object",
        ),
        (
            "jsonb",
            r#"'{"a":1}'"#,
            "cannot call jsonb_populate_recordset on a non-array",
        ),
        (
            "json",
            "'3'",
            "cannot call json_populate_recordset on a scalar",
        ),
        (
            "jsonb",
            "'3'",
            "cannot call jsonb_populate_recordset on a non-array",
        ),
        (
            "json",
            "'[1,2]'",
            "argument of json_populate_recordset must be an array of objects",
        ),
        (
            "jsonb",
            "'[1,2]'",
            "argument of jsonb_populate_recordset must be an array of objects",
        ),
        (
            "json",
            "'[null]'",
            "argument of json_populate_recordset must be an array of objects",
        ),
        (
            "json",
            r#"'[{"a":1}, 3]'"#,
            "argument of json_populate_recordset must be an array of objects",
        ),
    ];
    for (family, document, message) in cases {
        let sql = format!("SELECT * FROM {family}_populate_recordset(null::jpop, {document}) q");
        assert!(
            error_of(&mut s, &sql).await == ("22023".into(), (*message).into()),
            "{sql}"
        );
    }
}

/// `populate_recordset` produces a row per array element, and answers an empty
/// array — and a NULL document — with no rows at all. That is where it parts
/// company with `populate_record`, which answers a NULL document with one row.
#[tokio::test]
async fn populate_recordset_produces_one_row_per_element_and_none_for_empty() {
    let (_types, _e, mut s) = engine_with_types(JPOP).await;
    for family in ["json", "jsonb"] {
        let sql = format!(
            "SELECT * FROM {family}_populate_recordset(row('def',99,null)::jpop, \
             '[{{\"a\":\"blurfl\"}},{{\"b\":3}}]') q"
        );
        assert!(
            query(&mut s, &sql).await
                == vec![
                    row(&[Some("blurfl"), Some("99"), None]),
                    row(&[Some("def"), Some("3"), None]),
                ],
            "{sql}"
        );

        for empty in ["'[]'", &format!("null::{family}")] {
            let sql = format!("SELECT * FROM {family}_populate_recordset(null::jpop, {empty}) q");
            assert!(query(&mut s, &sql).await.is_empty(), "{sql}");
        }
    }
}

// ---------------------------------------------------------------------------
// Where the two families disagree
// ---------------------------------------------------------------------------

/// The load-bearing difference: `json` populates a field from the document's
/// *original text* and `jsonb` from its canonical rendering. Spacing, number
/// notation and key order all survive one and not the other.
#[tokio::test]
async fn json_keeps_the_document_text_where_jsonb_canonicalises_it() {
    let (_e, mut s) = engine_with(&[]).await;
    let document = r#"'{"o":  {"x" :  1} , "arr": [1,  2], "n": 1e3}'"#;

    let json = query(
        &mut s,
        &format!("SELECT * FROM json_to_record({document}) AS t(o text, arr text, n text)"),
    )
    .await;
    assert!(json == vec![row(&[Some("{\"x\" :  1}"), Some("[1,  2]"), Some("1e3")])]);

    let jsonb = query(
        &mut s,
        &format!("SELECT * FROM jsonb_to_record({document}) AS t(o text, arr text, n text)"),
    )
    .await;
    assert!(jsonb == vec![row(&[Some("{\"x\": 1}"), Some("[1, 2]"), Some("1000")])]);

    // The same split reaches the *error* text, because the failing value is
    // rendered before the type's input function ever sees it.
    let bad = r#"'{"b":[1,2]}'"#;
    let sql = format!("SELECT * FROM json_to_record({bad}) AS t(b int)");
    assert!(error_of(&mut s, &sql).await.1 == "invalid input syntax for type integer: \"[1,2]\"");
    let sql = format!("SELECT * FROM jsonb_to_record({bad}) AS t(b int)");
    assert!(error_of(&mut s, &sql).await.1 == "invalid input syntax for type integer: \"[1, 2]\"");
}

/// A `json` target column keeps the sub-document verbatim; a `jsonb` one
/// decomposes it. Crossing the families is the interesting case: `jsonb`'s
/// document has already lost the spacing a `json` column would otherwise keep.
#[tokio::test]
async fn a_document_valued_column_is_rendered_by_its_own_type() {
    let (_e, mut s) = engine_with(&[]).await;
    let document = r#"'{"j": {"a" :  1}}'"#;
    let cases: &[(&str, &str, &str)] = &[
        ("json", "json", "{\"a\" :  1}"),
        ("json", "jsonb", "{\"a\": 1}"),
        ("jsonb", "json", "{\"a\": 1}"),
        ("jsonb", "jsonb", "{\"a\": 1}"),
    ];
    for (family, target, expected) in cases {
        let sql = format!("SELECT * FROM {family}_to_record({document}) AS t(j {target})");
        assert!(
            scalar(&mut s, &sql).await.as_deref() == Some(*expected),
            "{sql}"
        );
    }

    // A JSON *string* holding JSON text is not re-parsed — it stays a string.
    let sql = r#"SELECT * FROM json_to_record('{"j": "{\"k\": 1}"}') AS t(j json)"#;
    assert!(scalar(&mut s, sql).await.as_deref() == Some(r#""{\"k\": 1}""#));
}

// ---------------------------------------------------------------------------
// Column definition lists
// ---------------------------------------------------------------------------

/// The three 42601 wordings, each earned by a different kind of function — and
/// two of the three by functions with no JSON in them, because the rule belongs
/// to FROM items rather than to this family.
#[tokio::test]
async fn a_column_definition_list_earns_a_different_refusal_per_function_kind() {
    let (_types, _e, mut s) = engine_with_types(JPOP).await;
    let cases: &[(&str, &str)] = &[
        // Returns a named composite: the list says nothing the type did not.
        (
            "SELECT * FROM json_populate_record(null::jpop, '{}') AS q(a text, b int, c timestamp)",
            "a column definition list is redundant for a function returning a named composite type",
        ),
        (
            "SELECT * FROM jsonb_populate_recordset(null::jpop, '[]') AS q(a text, b int, c timestamp)",
            "a column definition list is redundant for a function returning a named composite type",
        ),
        // Output columns declared as OUT parameters.
        (
            "SELECT * FROM json_each('{\"a\":1}') AS q(k text, v json)",
            "a column definition list is redundant for a function with OUT parameters",
        ),
        (
            "SELECT * FROM jsonb_each_text('{\"a\":1}') AS q(k text, v text)",
            "a column definition list is redundant for a function with OUT parameters",
        ),
        (
            "SELECT * FROM pg_input_error_info('x', 'int4') AS q(a text, b text, c text, d text)",
            "a column definition list is redundant for a function with OUT parameters",
        ),
        // Everything else: a scalar result, JSON or not.
        (
            "SELECT * FROM generate_series(1, 3) AS q(a int)",
            "a column definition list is only allowed for functions returning \"record\"",
        ),
        (
            "SELECT * FROM unnest(ARRAY[1, 2]) AS q(a int)",
            "a column definition list is only allowed for functions returning \"record\"",
        ),
        (
            "SELECT * FROM string_to_table('a,b', ',') AS q(a text)",
            "a column definition list is only allowed for functions returning \"record\"",
        ),
        (
            "SELECT * FROM regexp_split_to_table('a,b', ',') AS q(a text)",
            "a column definition list is only allowed for functions returning \"record\"",
        ),
        (
            "SELECT * FROM generate_subscripts(ARRAY[1, 2], 1) AS q(a int)",
            "a column definition list is only allowed for functions returning \"record\"",
        ),
        (
            "SELECT * FROM json_object_keys('{\"a\":1}') AS q(k text)",
            "a column definition list is only allowed for functions returning \"record\"",
        ),
        (
            "SELECT * FROM jsonb_array_elements('[1]') AS q(v jsonb)",
            "a column definition list is only allowed for functions returning \"record\"",
        ),
        // Returns `record`, and nothing supplied a row type.
        (
            "SELECT * FROM json_to_record('{\"a\":1}')",
            "a column definition list is required for functions returning \"record\"",
        ),
        (
            "SELECT * FROM jsonb_to_recordset('[{\"a\":1}]')",
            "a column definition list is required for functions returning \"record\"",
        ),
        (
            "SELECT * FROM json_populate_record(null::record, '{\"x\":1}')",
            "a column definition list is required for functions returning \"record\"",
        ),
    ];
    for (sql, message) in cases {
        assert!(
            error_of(&mut s, sql).await == ("42601".into(), (*message).into()),
            "{sql}"
        );
    }
}

/// A column-definition list names the item's columns; the alias names only the
/// item. That is a different rule from the one a single *scalar* result follows,
/// where a bare alias does rename the column — so both have to keep working.
#[tokio::test]
async fn an_alias_beside_a_column_definition_list_does_not_rename_the_columns() {
    let (_types, _e, mut s) = engine_with_types(JPOP).await;

    let r = result(
        &mut s,
        "SELECT * FROM json_to_record('{\"a\":1}') AS x(a int)",
    )
    .await;
    assert!(field_names(&r) == vec!["a"]);
    assert!(rows_text(&r) == vec![row(&[Some("1")])]);

    let r = result(
        &mut s,
        "SELECT q.* FROM json_populate_record(null::jpop, '{\"b\":7}') q",
    )
    .await;
    assert!(field_names(&r) == vec!["a", "b", "c"]);

    // Unchanged for a scalar-returning function: there the alias *is* the
    // column name.
    let r = result(&mut s, "SELECT g FROM generate_series(1, 2) AS g").await;
    assert!(field_names(&r) == vec!["g"]);
    let r = result(&mut s, "SELECT * FROM json_object_keys('{\"a\":1}') AS k").await;
    assert!(field_names(&r) == vec!["k"]);
}

/// The list becomes a tuple descriptor, so a repeated name is 42701 rather than
/// two columns of the same name.
#[tokio::test]
async fn a_column_definition_list_refuses_a_repeated_name() {
    let (_e, mut s) = engine_with(&[]).await;
    let sql = "SELECT * FROM json_to_record('{\"a\":1}') AS x(a int, b int, a text)";
    assert!(
        error_of(&mut s, sql).await
            == (
                "42701".into(),
                "column name \"a\" specified more than once".into()
            )
    );
}

/// `WITH ORDINALITY` and a column-definition list cannot share a FROM item, and
/// the refusal points at the spelling that does take both. Neither half alone is
/// affected — including for functions with no JSON in them.
#[tokio::test]
async fn with_ordinality_and_a_column_definition_list_cannot_share_an_item() {
    let (_e, mut s) = engine_with(&[]).await;
    let sql =
        "SELECT * FROM json_to_recordset('[{\"a\":1}]') WITH ORDINALITY AS x(a int, n bigint)";
    assert!(
        error_of(&mut s, sql).await
            == (
                "42601".into(),
                "WITH ORDINALITY cannot be used with a column definition list".into()
            )
    );

    // `ROWS FROM` moves the list onto the call, where the two do combine --
    // including with a *single* call, which is the case that distinguishes the
    // two spellings. Both parse to one call carrying `column_defs`, so a check
    // that looks only at the calls refuses the very shape the hint above
    // recommends. Certification caught that; a two-call `ROWS FROM` does not.
    let one = result(
        &mut s,
        "SELECT * FROM ROWS FROM (json_to_record('{\"a\":1}') AS (a int)) WITH ORDINALITY",
    )
    .await;
    assert!(field_names(&one) == vec!["a", "ordinality"]);
    assert!(rows_text(&one) == vec![row(&[Some("1"), Some("1")])]);

    let r = result(
        &mut s,
        "SELECT * FROM ROWS FROM (json_to_record('{\"a\":1}') AS (a int), generate_series(1, 2)) \
         WITH ORDINALITY",
    )
    .await;
    assert!(field_names(&r) == vec!["a", "generate_series", "ordinality"]);
    assert!(
        rows_text(&r)
            == vec![
                row(&[Some("1"), Some("1"), Some("1")]),
                row(&[None, Some("2"), Some("2")]),
            ]
    );

    // And ordinality on its own is untouched.
    let r = result(
        &mut s,
        "SELECT * FROM generate_series(1, 2) WITH ORDINALITY AS g(n, o)",
    )
    .await;
    assert!(field_names(&r) == vec!["n", "o"]);
}

/// `to_record` fills a column the document has no key for with NULL and ignores
/// a key no column claims, so the list is a projection rather than a contract.
#[tokio::test]
async fn a_column_definition_list_selects_and_pads_independently_of_the_document() {
    let (_e, mut s) = engine_with(&[]).await;
    for family in ["json", "jsonb"] {
        let sql =
            format!("SELECT * FROM {family}_to_record('{{\"a\":1,\"b\":2}}') AS x(a int, c text)");
        assert!(
            query(&mut s, &sql).await == vec![row(&[Some("1"), None])],
            "{sql}"
        );

        let sql = format!(
            "SELECT * FROM {family}_to_recordset('[{{\"a\":1,\"b\":\"foo\",\"d\":false}}, \
             {{\"a\":2,\"b\":\"bar\",\"c\":true}}]') AS x(a int, b text, c boolean)"
        );
        assert!(
            query(&mut s, &sql).await
                == vec![
                    row(&[Some("1"), Some("foo"), None]),
                    row(&[Some("2"), Some("bar"), Some("t")]),
                ],
            "{sql}"
        );
    }
}

// ---------------------------------------------------------------------------
// Anonymous records
// ---------------------------------------------------------------------------

/// In a select list nothing but the argument's *value* can carry a row type: a
/// `ROW(…)` does, `NULL::record` does not, and the two are one declared type.
#[tokio::test]
async fn a_select_list_call_takes_its_row_type_from_the_argument_value() {
    let (_e, mut s) = engine_with(&[]).await;

    assert!(
        scalar(
            &mut s,
            "SELECT json_populate_record(row(1,2), '{\"f1\": 0, \"f2\": 1}')"
        )
        .await
        .as_deref()
            == Some("(0,1)")
    );
    assert!(
        scalar(
            &mut s,
            "SELECT jsonb_populate_recordset(row(1,2), '[{\"f1\": 0, \"f2\": 1}]')"
        )
        .await
        .as_deref()
            == Some("(0,1)")
    );
    // One call, one source row each, expanded in lockstep.
    assert!(
        query(
            &mut s,
            "SELECT i, json_populate_recordset(row(i,50), '[{\"f1\":\"42\"},{\"f2\":\"43\"}]') \
             FROM (VALUES (1),(2)) v(i)"
        )
        .await
            == vec![
                row(&[Some("1"), Some("(42,50)")]),
                row(&[Some("1"), Some("(1,43)")]),
                row(&[Some("2"), Some("(42,50)")]),
                row(&[Some("2"), Some("(2,43)")]),
            ]
    );
    // An empty array still resolves the row type, and yields no rows.
    assert!(
        query(&mut s, "SELECT json_populate_recordset(row(1,2), '[]')").await
            == Vec::<Vec<_>>::new()
    );

    for sql in [
        "SELECT json_populate_record(null::record, '{\"x\": 0}')",
        "SELECT jsonb_populate_recordset(null::record, '[{\"x\": 0}]')",
        "SELECT json_to_record('{\"a\":1}')",
        "SELECT jsonb_to_recordset('[{\"a\":1}]')",
    ] {
        let (code, message) = error_of(&mut s, sql).await;
        assert!(code == "0A000", "{sql}");
        assert!(
            message.starts_with("could not determine row type for result of"),
            "{sql}"
        );
    }
}

/// A FROM item's column-definition list does not *replace* a run-time record
/// argument's row type — the two have to agree, and `PostgreSQL` reports which way
/// they did not.
#[tokio::test]
async fn a_record_argument_and_a_column_definition_list_must_agree() {
    let (_e, mut s) = engine_with(&[]).await;
    let cases: &[(&str, &str)] = &[
        (
            "row(0::int)",
            "Returned row contains 1 attribute, but query expects 2.",
        ),
        (
            "row(0::int,0::int)",
            "Returned type integer at ordinal position 1, but query expects text.",
        ),
        (
            "row(0::int,0::int,0::int)",
            "Returned row contains 3 attributes, but query expects 2.",
        ),
    ];
    for (base, detail) in cases {
        let sql = format!(
            "SELECT * FROM json_populate_recordset({base}, '[{{\"a\":\"1\"}}]') q(a text, b text)"
        );
        assert!(
            diagnosed(&mut s, &sql).await
                == (
                    "42804".into(),
                    "function return row and query-specified return row do not match".into(),
                    Some((*detail).to_string()),
                    None,
                ),
            "{sql}"
        );
    }

    // A NULL `record` argument has no row type to disagree with, so the list
    // simply supplies one.
    assert!(
        query(
            &mut s,
            "SELECT * FROM json_populate_record(null::record, '{\"x\": 776}') AS q(x int, y int)"
        )
        .await
            == vec![row(&[Some("776"), None])]
    );
}

// ---------------------------------------------------------------------------
// Target types
// ---------------------------------------------------------------------------

/// Populating an array walks the document's nesting: the dimension count comes
/// from the first element down, every sibling is then held to it, and a JSON
/// *string* goes to `array_in` instead.
#[tokio::test]
async fn an_array_column_takes_its_dimensions_from_the_document() {
    let (_e, mut s) = engine_with(&[]).await;
    for family in ["json", "jsonb"] {
        let ok: &[(&str, &str, &str)] = &[
            (r#"{"ia": [1, "2", null, 4]}"#, "_int4", "{1,2,NULL,4}"),
            (r#"{"ia": [[1, 2], [3, 4]]}"#, "_int4", "{{1,2},{3,4}}"),
            (
                r#"{"ia": [[[1], [2], [3]]]}"#,
                "int4[][]",
                "{{{1},{2},{3}}}",
            ),
            // A SQL array literal in a JSON string.
            (r#"{"ia": "{1,2,3}"}"#, "_int4", "{1,2,3}"),
            // Zero elements collapse to the empty array whatever the nesting.
            (r#"{"ia": []}"#, "_int4", "{}"),
            (r#"{"ia": [[],[]]}"#, "_int4", "{}"),
        ];
        for (document, target, expected) in ok {
            let sql = format!("SELECT * FROM {family}_to_record('{document}') AS x(ia {target})");
            assert!(
                scalar(&mut s, &sql).await.as_deref() == Some(*expected),
                "{sql}"
            );
        }

        // The refusal's usable half is the `HINT`, which locates the offending
        // value — by key at the top, by subscript path below it.
        let bad: &[(&str, &str, Option<&str>, Option<&str>)] = &[
            (
                r#"{"ia": 123}"#,
                "expected JSON array",
                None,
                Some(r#"See the value of key "ia"."#),
            ),
            (
                r#"{"ia": {"a":1}}"#,
                "expected JSON array",
                None,
                Some(r#"See the value of key "ia"."#),
            ),
            // Ragged: an element where a sub-array was expected.
            (
                r#"{"ia": [[1], 2]}"#,
                "expected JSON array",
                None,
                Some(r#"See the array element [1] of key "ia"."#),
            ),
            (
                r#"{"ia": [[[1],[2]],[[3],4]]}"#,
                "expected JSON array",
                None,
                Some(r#"See the array element [1][1] of key "ia"."#),
            ),
            // Sub-arrays of different lengths.
            (
                r#"{"ia": [[1], [2, 3]]}"#,
                "malformed JSON array",
                Some("Multidimensional arrays must have sub-arrays with matching dimensions."),
                None,
            ),
        ];
        for (document, message, detail, hint) in bad {
            let sql = format!("SELECT * FROM {family}_to_record('{document}') AS x(ia _int4)");
            assert!(
                diagnosed(&mut s, &sql).await
                    == (
                        "22P02".into(),
                        (*message).into(),
                        detail.map(ToString::to_string),
                        hint.map(ToString::to_string),
                    ),
                "{sql}"
            );
        }
    }
}

/// A nested composite recurses; a JSON string is handed to `record_in` instead;
/// and a scalar or array raises the same `populate_composite` refusal the top
/// level does.
#[tokio::test]
async fn a_composite_column_recurses_or_parses_a_string() {
    let (_types, _e, mut s) = engine_with_types(JPOP).await;
    for family in ["json", "jsonb"] {
        let sql = format!(
            "SELECT * FROM {family}_to_record('{{\"r\": {{\"a\":\"abc\",\"b\":123}}}}') AS x(r jpop)"
        );
        assert!(
            scalar(&mut s, &sql).await.as_deref() == Some("(abc,123,)"),
            "{sql}"
        );

        let sql = format!(
            "SELECT * FROM {family}_to_record('{{\"r\": \"(abc,42,01.02.2003)\"}}') AS x(r jpop)"
        );
        assert!(
            scalar(&mut s, &sql).await.as_deref() == Some("(abc,42,\"2003-01-02 00:00:00\")"),
            "{sql}"
        );

        let sql = format!("SELECT * FROM {family}_to_record('{{\"r\": null}}') AS x(r jpop)");
        assert!(scalar(&mut s, &sql).await.is_none(), "{sql}");

        for (document, message) in [
            (
                r#"{"r": 123}"#,
                "cannot call populate_composite on a scalar",
            ),
            (
                r#"{"r": [1,2]}"#,
                "cannot call populate_composite on an array",
            ),
        ] {
            let sql = format!("SELECT * FROM {family}_to_record('{document}') AS x(r jpop)");
            assert!(
                error_of(&mut s, &sql).await == ("22023".into(), message.into()),
                "{sql}"
            );
        }
    }
}

/// A domain contributes its constraints, not its shape: the value is built for
/// the base type and then checked — including the NOT NULL a *missing* key
/// leaves behind.
#[tokio::test]
async fn a_domain_column_is_checked_after_it_is_populated() {
    let (_types, _e, mut s) = engine_with_types(&[
        "CREATE DOMAIN posint AS int CHECK (VALUE > 0)",
        "CREATE DOMAIN notnullint AS int NOT NULL",
    ])
    .await;

    assert!(
        scalar(
            &mut s,
            "SELECT * FROM json_to_record('{\"d\": 5}') AS x(d posint)"
        )
        .await
        .as_deref()
            == Some("5")
    );
    let (code, message) = error_of(
        &mut s,
        "SELECT * FROM json_to_record('{\"d\": -5}') AS x(d posint)",
    )
    .await;
    assert!(code == "23514");
    assert!(message == "value for domain posint violates check constraint \"posint_check\"");

    for document in ["{\"d\": null}", "{\"x\": 1}"] {
        let sql = format!("SELECT * FROM jsonb_to_record('{document}') AS x(d notnullint)");
        assert!(
            error_of(&mut s, &sql).await
                == (
                    "23502".into(),
                    "domain notnullint does not allow null values".into()
                ),
            "{sql}"
        );
    }
}

/// Every other target goes through the type's input function under *assignment*
/// rules, so an over-long `character(n)` is 22001 rather than a truncation.
#[tokio::test]
async fn a_scalar_column_goes_through_its_input_function() {
    let (_e, mut s) = engine_with(&[]).await;
    let cases: &[(&str, &str, Option<&str>)] = &[
        (r#"{"b": true}"#, "b bool", Some("t")),
        (r#"{"b": "true"}"#, "b bool", Some("t")),
        (r#"{"b": 1}"#, "b bool", Some("t")),
        (r#"{"n": 1.50}"#, "n numeric", Some("1.50")),
        (r#"{"s": "str"}"#, "s text", Some("str")),
        (r#"{"z": null}"#, "z text", None),
        (r#"{"c": "aaa"}"#, "c char(10)", Some("aaa       ")),
    ];
    for (document, target, expected) in cases {
        let sql = format!("SELECT * FROM json_to_record('{document}') AS x({target})");
        assert!(scalar(&mut s, &sql).await.as_deref() == *expected, "{sql}");
    }

    for family in ["json", "jsonb"] {
        let sql = format!(
            "SELECT * FROM {family}_to_record('{{\"c\": \"aaaaaaaaaaaaa\"}}') AS x(c char(10))"
        );
        assert!(
            error_of(&mut s, &sql).await
                == (
                    "22001".into(),
                    "value too long for type character(10)".into()
                ),
            "{sql}"
        );
    }
}

/// Duplicate keys resolve to the last one, in both families — `jsonb` because it
/// already discarded the first, `json` because its lookup walks to the end.
#[tokio::test]
async fn a_duplicate_key_resolves_to_the_last_occurrence() {
    let (_e, mut s) = engine_with(&[]).await;
    for family in ["json", "jsonb"] {
        let sql = format!("SELECT * FROM {family}_to_record('{{\"a\":1,\"a\":2}}') AS x(a int)");
        assert!(scalar(&mut s, &sql).await.as_deref() == Some("2"), "{sql}");
    }
}

// ---------------------------------------------------------------------------
// Resolution
// ---------------------------------------------------------------------------

/// The document parameter is one type per family, with no cast between them, and
/// the row-type parameter is polymorphic — so an untyped literal there resolves
/// nothing at all.
#[tokio::test]
async fn the_two_parameters_refuse_the_wrong_kind_of_argument() {
    let (_types, _e, mut s) = engine_with_types(JPOP).await;
    for sql in [
        "SELECT * FROM json_to_record('{\"a\":1}'::text) AS x(a int)",
        "SELECT * FROM jsonb_to_record('{\"a\":1}'::json) AS x(a int)",
        "SELECT * FROM json_populate_record(null::jpop, '{}'::jsonb) q",
        "SELECT * FROM json_populate_record(null::int, '{}') q",
    ] {
        assert!(error_of(&mut s, sql).await.0 == "42883", "{sql}");
    }

    // The `json_` half alone carries `use_json_as_text boolean DEFAULT false`,
    // a parameter kept since 9.4 and ignored ever since — so a three-argument
    // call resolves for `json_populate_record` and is 42883 for its twin.
    for sql in [
        "SELECT * FROM json_populate_record(null::jpop, '{\"b\":1}', true) q",
        "SELECT * FROM json_populate_recordset(null::jpop, '[{\"b\":1}]', false) q",
    ] {
        assert!(
            query(&mut s, sql).await == vec![row(&[None, Some("1"), None])],
            "{sql}"
        );
    }
    for sql in [
        "SELECT * FROM jsonb_populate_record(null::jpop, '{\"b\":1}', true) q",
        "SELECT * FROM json_to_record('{\"a\":1}', true) AS x(a int)",
        "SELECT * FROM json_populate_record(null::jpop, '{}', '{}', '{}') q",
    ] {
        assert!(error_of(&mut s, sql).await.0 == "42883", "{sql}");
    }

    let sql = "SELECT * FROM json_populate_record('x', '{\"a\":1}') q";
    assert!(
        error_of(&mut s, sql).await
            == (
                "42804".into(),
                "could not determine polymorphic type because input has type unknown".into()
            )
    );
}

/// A record-family call is not set-returning unless it is the `*set` half, so
/// only those take the row-multiplying path — and only those are refused inside
/// an aggregate.
#[tokio::test]
async fn only_the_recordset_half_multiplies_rows() {
    let (_types, _e, mut s) = engine_with_types(JPOP).await;
    assert!(
        query(
            &mut s,
            "SELECT count(*) FROM (SELECT json_populate_record(null::jpop, '{\"b\":1}')) t"
        )
        .await
            == vec![row(&[Some("1")])]
    );
    assert!(
        query(
            &mut s,
            "SELECT count(*) FROM (SELECT json_populate_recordset(null::jpop, \
             '[{\"b\":1}, {\"b\":2}]')) t"
        )
        .await
            == vec![row(&[Some("2")])]
    );
}

// ---------------------------------------------------------------------------
// Round-tripping through a stored view
// ---------------------------------------------------------------------------

/// A view body is stored as text and has to re-parse, so `pg_get_viewdef` must
/// print the column-definition list — a `json_to_record(…)` without one is a
/// 42601, which would make the rule unreplayable.
///
/// The other parts of a function FROM item are equally load-bearing and equally
/// easy to drop, so `LATERAL`, `ROWS FROM`, `WITH ORDINALITY` and the alias are
/// all checked here, on items with no JSON in them as well as with.
#[tokio::test]
async fn a_function_from_item_round_trips_through_pg_get_viewdef() {
    let (_e, mut s) = engine_with(&[]).await;
    let bodies: &[(&str, &str)] = &[
        (
            "vdefs",
            "SELECT * FROM json_to_record('{\"a\":1,\"b\":\"x\"}'::json) AS t(a int, b text)",
        ),
        (
            "vnoalias",
            "SELECT * FROM jsonb_to_record('{\"a\":1}'::jsonb) AS (a int)",
        ),
        (
            "vrowsfrom",
            "SELECT * FROM ROWS FROM (json_to_record('{\"a\":1}'::json) AS (a int), \
             generate_series(1, 2)) AS r(a, s)",
        ),
        ("valias", "SELECT * FROM generate_series(1, 3) AS g(n)"),
        (
            "vordinality",
            "SELECT * FROM generate_series(1, 3) WITH ORDINALITY AS g(n, o)",
        ),
        (
            "vlateral",
            "SELECT * FROM (VALUES (1)) v(i), LATERAL json_to_record(('{\"a\":' || v.i || '}')::json) AS t(a int)",
        ),
    ];
    for (name, body) in bodies {
        result(&mut s, &format!("CREATE VIEW {name} AS {body}")).await;
        let expected = query(&mut s, &format!("SELECT * FROM {name}")).await;

        let rendered = scalar(
            &mut s,
            &format!("SELECT pg_get_viewdef('{name}'::regclass)"),
        )
        .await
        .expect("a view has a definition");
        // The rendered body must both re-parse and answer identically.
        let replayed = query(&mut s, rendered.trim().trim_end_matches(';')).await;
        assert!(
            replayed == expected,
            "{name} did not round-trip: {rendered}"
        );
    }

    // And the list itself survives, rather than the body happening to re-parse
    // without it.
    let rendered = scalar(&mut s, "SELECT pg_get_viewdef('vdefs'::regclass)")
        .await
        .expect("a view has a definition");
    assert!(rendered.contains("t(a integer, b text)"), "{rendered}");
    let rendered = scalar(&mut s, "SELECT pg_get_viewdef('vrowsfrom'::regclass)")
        .await
        .expect("a view has a definition");
    assert!(rendered.contains("ROWS FROM("), "{rendered}");
    assert!(rendered.contains("AS (a integer)"), "{rendered}");
}
