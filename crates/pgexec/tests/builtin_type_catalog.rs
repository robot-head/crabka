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

/// `pg_type` is a system catalog, not a reduced driver lookup table. Its full
/// `PostgreSQL` 18 column set has to bind before type-sanity checks can examine
/// the fixture rows that populate it.
#[tokio::test]
async fn pg_type_exposes_all_postgresql_18_catalog_columns() {
    let (_engine, mut s) = session();
    let columns = rows(
        &mut s,
        "SELECT typnamespace, typowner, typlen, typbyval, typtype, typcategory, \
                typispreferred, typisdefined, typdelim, typrelid, typsubscript, \
                typelem, typarray, typinput, typoutput, typreceive, typsend, \
                typmodin, typmodout, typanalyze, typalign, typstorage, typnotnull, \
                typbasetype, typtypmod, typndims, typcollation, typdefaultbin, \
                typdefault, typacl \
         FROM pg_type WHERE typname = 'int4'",
    )
    .await;
    assert!(
        columns
            == vec![vec![
                Some("11".into()),
                Some("10".into()),
                Some("4".into()),
                Some("t".into()),
                Some("b".into()),
                Some("N".into()),
                Some("f".into()),
                Some("t".into()),
                Some(",".into()),
                Some("0".into()),
                Some("-".into()),
                Some("0".into()),
                Some("1007".into()),
                Some("int4in".into()),
                Some("int4out".into()),
                Some("int4recv".into()),
                Some("int4send".into()),
                Some("-".into()),
                Some("-".into()),
                Some("-".into()),
                Some("i".into()),
                Some("p".into()),
                Some("f".into()),
                Some("0".into()),
                Some("-1".into()),
                Some("0".into()),
                Some("0".into()),
                None,
                None,
                None,
            ]]
    );
}

/// The catalog's object identifiers and `typlen` keep their catalog types in
/// `RowDescription`, so driver type caches do not mistake them for `int4`.
#[tokio::test]
async fn pg_type_uses_oid_and_int2_column_types_on_the_wire() {
    let (_engine, mut s) = session();
    let result = s
        .simple_query(
            "SELECT oid, typnamespace, typowner, typlen, typrelid, typelem, \
                    typarray, typbasetype, typcollation \
             FROM pg_type WHERE typname = 'int4'",
        )
        .await
        .expect("pg_type query succeeds");
    let QueryResult::Rows { fields, .. } = &result[0] else {
        panic!("pg_type query should return rows");
    };
    assert!(
        fields
            .iter()
            .map(|field| field.type_oid)
            .collect::<Vec<_>>()
            == vec![26, 26, 26, 21, 26, 26, 26, 26, 26]
    );
}

/// I/O and subscripting metadata comes from the same built-in `pg_proc`
/// fixture that backs `pg_proc`, so every nonzero regproc link is real.
#[tokio::test]
async fn pg_type_links_array_and_range_io_routines() {
    let (_engine, mut s) = session();
    assert!(
        rows(
            &mut s,
            "SELECT typname, typsubscript, typinput, typoutput, typreceive, typsend, typanalyze \
             FROM pg_type \
             WHERE typname IN ('_int4', 'int4range', 'int4multirange') \
             ORDER BY typname",
        )
        .await
            == vec![
                vec![
                    Some("_int4".into()),
                    Some("array_subscript_handler".into()),
                    Some("array_in".into()),
                    Some("array_out".into()),
                    Some("array_recv".into()),
                    Some("array_send".into()),
                    Some("array_typanalyze".into()),
                ],
                vec![
                    Some("int4multirange".into()),
                    Some("-".into()),
                    Some("multirange_in".into()),
                    Some("multirange_out".into()),
                    Some("multirange_recv".into()),
                    Some("multirange_send".into()),
                    Some("-".into()),
                ],
                vec![
                    Some("int4range".into()),
                    Some("-".into()),
                    Some("range_in".into()),
                    Some("range_out".into()),
                    Some("range_recv".into()),
                    Some("range_send".into()),
                    Some("range_typanalyze".into()),
                ],
            ]
    );
}

#[tokio::test]
async fn pg_type_uses_postgresqls_nonmechanical_io_routine_stems() {
    let (_engine, mut s) = session();
    assert!(
        rows(
            &mut s,
            "SELECT typname, typinput, typoutput, typreceive, typsend \
             FROM pg_type WHERE typname IN ('money', 'polygon') ORDER BY typname",
        )
        .await
            == vec![
                vec![
                    Some("money".into()),
                    Some("cash_in".into()),
                    Some("cash_out".into()),
                    Some("cash_recv".into()),
                    Some("cash_send".into()),
                ],
                vec![
                    Some("polygon".into()),
                    Some("poly_in".into()),
                    Some("poly_out".into()),
                    Some("poly_recv".into()),
                    Some("poly_send".into()),
                ],
            ]
    );
}

/// `cstring` and `refcursor` are catalog-visible even though application SQL
/// cannot construct a `cstring` value. Their arrays are used by type-sanity's
/// routine and core-type probes.
#[tokio::test]
async fn pg_type_exposes_cstring_and_refcursor_with_their_arrays() {
    let (_engine, mut s) = session();
    assert!(
        rows(
            &mut s,
            "SELECT typname, typlen, typcategory, typtype, typarray, typinput, typoutput, \
                    typreceive, typsend, typalign, typstorage \
             FROM pg_type WHERE typname IN ('cstring', '_cstring', 'refcursor', '_refcursor') \
             ORDER BY typname",
        )
        .await
            == vec![
                vec![
                    Some("_cstring".into()),
                    Some("-1".into()),
                    Some("A".into()),
                    Some("b".into()),
                    Some("0".into()),
                    Some("array_in".into()),
                    Some("array_out".into()),
                    Some("array_recv".into()),
                    Some("array_send".into()),
                    Some("i".into()),
                    Some("x".into())
                ],
                vec![
                    Some("_refcursor".into()),
                    Some("-1".into()),
                    Some("A".into()),
                    Some("b".into()),
                    Some("0".into()),
                    Some("array_in".into()),
                    Some("array_out".into()),
                    Some("array_recv".into()),
                    Some("array_send".into()),
                    Some("i".into()),
                    Some("x".into())
                ],
                vec![
                    Some("cstring".into()),
                    Some("-2".into()),
                    Some("P".into()),
                    Some("p".into()),
                    Some("1263".into()),
                    Some("cstring_in".into()),
                    Some("cstring_out".into()),
                    Some("cstring_recv".into()),
                    Some("cstring_send".into()),
                    Some("c".into()),
                    Some("p".into())
                ],
                vec![
                    Some("refcursor".into()),
                    Some("-1".into()),
                    Some("U".into()),
                    Some("b".into()),
                    Some("2201".into()),
                    Some("textin".into()),
                    Some("textout".into()),
                    Some("textrecv".into()),
                    Some("textsend".into()),
                    Some("i".into()),
                    Some("x".into())
                ],
            ]
    );
    assert!(
        rows(
            &mut s,
            "SELECT 'cstring[]'::regtype, 'refcursor'::regtype, \
                    23::oid != ALL(ARRAY['regproc', 'regprocedure']::regtype[])",
        )
        .await
            == vec![vec![
                Some("cstring[]".into()),
                Some("refcursor".into()),
                Some("t".into())
            ]]
    );
    let result = s
        .simple_query("SELECT 'cursor_name'::refcursor")
        .await
        .expect("refcursor input succeeds");
    let QueryResult::Rows { fields, .. } = &result[0] else {
        panic!("refcursor query should return rows");
    };
    assert!(fields[0].type_oid == 1790);
    s.simple_query("CREATE TABLE refcursor_catalog (value refcursor)")
        .await
        .expect("refcursor column succeeds");
    s.simple_query("INSERT INTO refcursor_catalog VALUES ('cursor_name'::refcursor)")
        .await
        .expect("refcursor assignment succeeds");
    assert!(
        rows(
            &mut s,
            "SELECT atttypid FROM pg_attribute \
             WHERE attrelid = 'refcursor_catalog'::regclass AND attname = 'value'",
        )
        .await
            == vec![vec![Some("1790".into())]]
    );
    assert!(
        rows(&mut s, "SELECT value FROM refcursor_catalog").await
            == vec![vec![Some("cursor_name".into())]]
    );
}

/// `type_sanity` uses this regress-library helper to keep `PostgreSQL`'s
/// hard-coded catalog-index set audited. Its result is the upstream predicate,
/// not an approximation from Crabkas virtual index rows.
#[tokio::test]
async fn catalog_text_unique_index_helper_matches_postgresqls_catalog_oids() {
    let (_engine, mut s) = session();
    s.simple_query(
        "CREATE FUNCTION is_catalog_text_unique_index_oid(oid) RETURNS bool \
         AS 'regress', 'is_catalog_text_unique_index_oid' LANGUAGE C STRICT",
    )
    .await
    .expect("create the pinned regress helper");
    assert!(
        rows(
            &mut s,
            "SELECT is_catalog_text_unique_index_oid(3593::oid), \
                    is_catalog_text_unique_index_oid(6246::oid), \
                    is_catalog_text_unique_index_oid(3592::oid)",
        )
        .await
            == vec![vec![Some("t".into()), Some("t".into()), Some("f".into())]]
    );
}

/// User-defined types use `PostgreSQL`'s shared I/O routines rather than a
/// made-up routine derived from the type name.
#[tokio::test]
async fn pg_type_links_user_type_io_routines() {
    let (_engine, mut s) = session();
    for sql in [
        "CREATE TYPE routine_enum AS ENUM ('a', 'b')",
        "CREATE TYPE routine_composite AS (x int4)",
        "CREATE DOMAIN routine_domain AS int4",
    ] {
        s.simple_query(sql)
            .await
            .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e:?}"));
    }
    assert!(
        rows(
            &mut s,
            "SELECT typname, typsubscript, typinput, typoutput, typreceive, typsend, \
                    typmodin, typmodout, typanalyze \
             FROM pg_type \
             WHERE typname IN ('routine_enum', 'routine_composite', 'routine_domain') \
             ORDER BY typname",
        )
        .await
            == vec![
                vec![
                    Some("routine_composite".into()),
                    Some("-".into()),
                    Some("record_in".into()),
                    Some("record_out".into()),
                    Some("record_recv".into()),
                    Some("record_send".into()),
                    Some("-".into()),
                    Some("-".into()),
                    Some("-".into()),
                ],
                vec![
                    Some("routine_domain".into()),
                    Some("-".into()),
                    Some("domain_in".into()),
                    Some("int4out".into()),
                    Some("domain_recv".into()),
                    Some("int4send".into()),
                    Some("-".into()),
                    Some("-".into()),
                    Some("-".into()),
                ],
                vec![
                    Some("routine_enum".into()),
                    Some("-".into()),
                    Some("enum_in".into()),
                    Some("enum_out".into()),
                    Some("enum_recv".into()),
                    Some("enum_send".into()),
                    Some("-".into()),
                    Some("-".into()),
                    Some("-".into()),
                ],
            ]
    );
}

/// The physical fields are not decorative: type sanity uses them to decide
/// whether a datum can be passed by value or must be stored out of line.
#[tokio::test]
async fn pg_type_uses_the_physical_layout_of_fixed_and_variable_types() {
    let (_engine, mut s) = session();
    assert!(
        rows(
            &mut s,
            "SELECT typname, typlen, typbyval, typalign, typstorage \
             FROM pg_type \
             WHERE typname IN ('bool', 'int2', 'int4', 'int8', 'text') \
             ORDER BY typname",
        )
        .await
            == vec![
                vec![
                    Some("bool".into()),
                    Some("1".into()),
                    Some("t".into()),
                    Some("c".into()),
                    Some("p".into()),
                ],
                vec![
                    Some("int2".into()),
                    Some("2".into()),
                    Some("t".into()),
                    Some("s".into()),
                    Some("p".into()),
                ],
                vec![
                    Some("int4".into()),
                    Some("4".into()),
                    Some("t".into()),
                    Some("i".into()),
                    Some("p".into()),
                ],
                vec![
                    Some("int8".into()),
                    Some("8".into()),
                    Some("t".into()),
                    Some("d".into()),
                    Some("p".into()),
                ],
                vec![
                    Some("text".into()),
                    Some("-1".into()),
                    Some("f".into()),
                    Some("i".into()),
                    Some("x".into()),
                ],
            ]
    );
}

/// Ranges and multiranges share a category, but `PostgreSQL` distinguishes their
/// `typtype`; callers use that distinction to find their `pg_range` metadata.
#[tokio::test]
async fn pg_type_distinguishes_range_and_multirange_rows() {
    let (_engine, mut s) = session();
    assert!(
        rows(
            &mut s,
            "SELECT typname, typcategory, typtype \
             FROM pg_type \
             WHERE typname IN ('int4range', 'int4multirange') \
             ORDER BY typname",
        )
        .await
            == vec![
                vec![
                    Some("int4multirange".into()),
                    Some("R".into()),
                    Some("m".into())
                ],
                vec![Some("int4range".into()), Some("R".into()), Some("r".into())],
            ]
    );
}

#[tokio::test]
async fn pg_type_range_rows_use_postgresqls_analyze_routine_and_alignment() {
    let (_engine, mut s) = session();
    assert!(
        rows(
            &mut s,
            "SELECT typname, typalign, typanalyze \
             FROM pg_type \
             WHERE typname IN ('int4range', 'tsrange', 'tstzrange', 'int8range') \
             ORDER BY typname",
        )
        .await
            == vec![
                vec![
                    Some("int4range".into()),
                    Some("i".into()),
                    Some("range_typanalyze".into()),
                ],
                vec![
                    Some("int8range".into()),
                    Some("d".into()),
                    Some("range_typanalyze".into()),
                ],
                vec![
                    Some("tsrange".into()),
                    Some("d".into()),
                    Some("range_typanalyze".into()),
                ],
                vec![
                    Some("tstzrange".into()),
                    Some("d".into()),
                    Some("range_typanalyze".into()),
                ],
            ]
    );
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
