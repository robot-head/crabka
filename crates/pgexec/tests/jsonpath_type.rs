//! Native `jsonpath` type plumbing: input canonicalization, OID/catalog metadata,
//! storage, operators, and text/binary result encoding.

use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Engine, QueryResult, Session};

async fn run(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("query {sql:?} failed: {error:?}"))
}

fn rows(result: &QueryResult) -> &Vec<Vec<Option<crabka_pgwire::engine::Cell>>> {
    let QueryResult::Rows { rows, .. } = result else {
        panic!("expected rows, got {result:?}");
    };
    rows
}

fn text(cell: &Option<crabka_pgwire::engine::Cell>) -> Option<&str> {
    cell.as_ref()
        .map(|cell| std::str::from_utf8(&cell.text).expect("UTF-8 result"))
}

#[tokio::test]
async fn jsonpath_is_canonicalized_and_reports_native_wire_metadata() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();

    let result = run(&mut session, "SELECT 'lax $.a'::jsonpath").await;
    let QueryResult::Rows {
        fields,
        rows: result_rows,
        ..
    } = &result[0]
    else {
        panic!("expected rows");
    };
    assert_eq!(fields[0].type_oid, 4072);
    assert_eq!(fields[0].type_size, -1);
    let cell = result_rows[0][0].as_ref().expect("non-NULL jsonpath");
    assert_eq!(cell.text.as_ref(), b"$.\"a\"");
    assert_eq!(cell.binary.as_ref(), b"\x01$.\"a\"");

    let typeof_result = run(&mut session, "SELECT pg_typeof('$.a'::jsonpath)").await;
    assert_eq!(text(&rows(&typeof_result[0])[0][0]), Some("jsonpath"));

    let error = session
        .simple_query("SELECT ''::jsonpath")
        .await
        .expect_err("empty jsonpath must fail its input function");
    assert_eq!(error.code, "22P02");

    let validity = run(
        &mut session,
        "SELECT pg_input_is_valid('lax $.a', 'jsonpath'), \
         pg_input_is_valid('', 'jsonpath')",
    )
    .await;
    assert_eq!(
        rows(&validity[0])[0].iter().map(text).collect::<Vec<_>>(),
        vec![Some("t"), Some("f")],
    );
    let info = run(
        &mut session,
        "SELECT message, sql_error_code FROM pg_input_error_info('', 'jsonpath')",
    )
    .await;
    assert_eq!(
        rows(&info[0])[0].iter().map(text).collect::<Vec<_>>(),
        vec![
            Some("invalid input syntax for type jsonpath: \"\""),
            Some("22P02"),
        ],
    );

    for op in ["=", "<>", "<", "<=", ">", ">=", "IS DISTINCT FROM"] {
        let sql = format!("SELECT '$'::jsonpath {op} '$'::jsonpath");
        let error = session
            .simple_query(&sql)
            .await
            .expect_err("jsonpath has no comparison operators");
        assert_eq!(error.code, "42883", "{sql}");
        assert!(error.message.contains("operator does not exist"), "{sql}");
    }
}

#[tokio::test]
async fn jsonpath_catalog_storage_and_jsonb_calls_keep_the_distinct_type() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();

    let catalog = run(
        &mut session,
        "SELECT oid, typname, typlen, typarray FROM pg_type WHERE oid = 4072",
    )
    .await;
    assert_eq!(
        rows(&catalog[0])
            .iter()
            .map(|row| row.iter().map(text).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        vec![vec![
            Some("4072"),
            Some("jsonpath"),
            Some("-1"),
            Some("4073"),
        ]],
    );

    run(&mut session, "CREATE TABLE paths (path jsonpath)").await;
    run(&mut session, "INSERT INTO paths VALUES ('lax $.a')").await;
    let stored = run(&mut session, "SELECT path FROM paths").await;
    let QueryResult::Rows {
        fields,
        rows: stored_rows,
        ..
    } = &stored[0]
    else {
        panic!("expected rows");
    };
    assert_eq!(fields[0].type_oid, 4072);
    assert_eq!(text(&stored_rows[0][0]), Some("$.\"a\""));
    let stored_type = run(&mut session, "SELECT pg_typeof(path) FROM paths").await;
    assert_eq!(text(&rows(&stored_type[0])[0][0]), Some("jsonpath"));

    run(
        &mut session,
        "CREATE FUNCTION echo_path(value jsonpath) RETURNS jsonpath LANGUAGE plpgsql AS $$
         DECLARE echoed jsonpath;
         BEGIN
           EXECUTE 'SELECT $1' INTO echoed USING value;
           RETURN echoed;
         END
         $$",
    )
    .await;
    let echoed = run(&mut session, "SELECT echo_path('lax $.a')").await;
    let QueryResult::Rows {
        fields,
        rows: echoed_rows,
        ..
    } = &echoed[0]
    else {
        panic!("expected rows");
    };
    assert_eq!(fields[0].type_oid, 4072);
    assert_eq!(text(&echoed_rows[0][0]), Some("$.\"a\""));

    let evaluated = run(
        &mut session,
        "SELECT '{\"a\": 1}'::jsonb @? '$.a', \
         jsonb_path_exists('{\"a\": 1}'::jsonb, '$.a'), \
         '{\"a\": 1}'::jsonb @@ '$.a == 1', \
         '{\"a\": 1}'::jsonb @? NULL, '{\"a\": 1}'::jsonb @@ NULL",
    )
    .await;
    assert_eq!(
        rows(&evaluated[0])[0].iter().map(text).collect::<Vec<_>>(),
        vec![Some("t"), Some("t"), Some("t"), None, None],
    );
}

#[tokio::test]
async fn jsonpath_defaults_arrays_and_assignment_keep_native_identity() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();

    run(
        &mut session,
        r#"CREATE TABLE path_values (
             path jsonpath DEFAULT 'lax $.default',
             paths jsonpath[] DEFAULT '{"lax $.item",NULL}'
           )"#,
    )
    .await;
    run(
        &mut session,
        r#"INSERT INTO path_values VALUES (DEFAULT, DEFAULT), (NULL, NULL)"#,
    )
    .await;

    let defaults = run(
        &mut session,
        "SELECT path, paths FROM path_values WHERE path IS NOT NULL",
    )
    .await;
    let QueryResult::Rows {
        fields,
        rows: default_rows,
        ..
    } = &defaults[0]
    else {
        panic!("expected rows");
    };
    assert_eq!(fields[0].type_oid, 4072);
    assert_eq!(fields[1].type_oid, 4073);
    assert_eq!(text(&default_rows[0][0]), Some("$.\"default\""));
    assert_eq!(text(&default_rows[0][1]), Some("{\"$.\\\"item\\\"\",NULL}"));
    let array_binary = default_rows[0][1]
        .as_ref()
        .expect("jsonpath[]")
        .binary
        .as_ref();
    assert_eq!(
        u32::from_be_bytes(array_binary[8..12].try_into().expect("oid")),
        4072
    );
    let first_len = i32::from_be_bytes(array_binary[20..24].try_into().expect("length"));
    assert!(first_len > 1);
    assert_eq!(
        array_binary[24], 1,
        "jsonpath array elements carry recv version"
    );

    let null_type = run(
        &mut session,
        "SELECT pg_typeof(path), pg_typeof(paths) FROM path_values WHERE path IS NULL",
    )
    .await;
    assert_eq!(
        rows(&null_type[0])[0].iter().map(text).collect::<Vec<_>>(),
        vec![Some("jsonpath"), Some("jsonpath[]")],
    );

    run(
        &mut session,
        "UPDATE path_values SET path = 'lax $.updated'",
    )
    .await;
    let updated = run(&mut session, "SELECT path FROM path_values LIMIT 1").await;
    assert_eq!(text(&rows(&updated[0])[0][0]), Some("$.\"updated\""));

    for sql in [
        "UPDATE path_values SET path = '$.typed'::text",
        "VALUES ('$'::text), ('$'::jsonpath)",
    ] {
        let error = session
            .simple_query(sql)
            .await
            .expect_err("typed text must not be assigned/coerced implicitly to jsonpath");
        assert_eq!(error.code, "42804", "{sql}: {error:?}");
    }

    let explicit = run(&mut session, "SELECT '$.explicit'::text::jsonpath").await;
    assert_eq!(text(&rows(&explicit[0])[0][0]), Some("$.\"explicit\""));

    let arrays = run(
        &mut session,
        "SELECT ARRAY['lax $.a', '$.b'::jsonpath], \
         array_append(paths, 'lax $.appended') \
         FROM path_values LIMIT 1",
    )
    .await;
    let QueryResult::Rows {
        fields,
        rows: array_rows,
        ..
    } = &arrays[0]
    else {
        panic!("expected rows");
    };
    assert_eq!(fields[0].type_oid, 4073);
    assert_eq!(fields[1].type_oid, 4073);
    assert_eq!(text(&array_rows[0][0]), Some("{\"$.\\\"a\\\"\",\"$.\\\"b\\\"\"}"));
    assert!(
        text(&array_rows[0][1])
            .expect("array_append result")
            .contains("$.\\\"appended\\\"")
    );

    // An aggregate query drives the grouped evaluator even for this
    // non-aggregate projection, covering its constructor coercion path.
    let grouped = run(
        &mut session,
        "SELECT ARRAY['lax $.grouped', '$.b'::jsonpath], count(*) FROM path_values",
    )
    .await;
    assert_eq!(text(&rows(&grouped[0])[0][0]), Some("{\"$.\\\"grouped\\\"\",\"$.\\\"b\\\"\"}"));

    run(
        &mut session,
        "UPDATE path_values SET paths[1] = 'lax $.assigned' WHERE paths IS NOT NULL",
    )
    .await;
    let assigned = run(
        &mut session,
        "SELECT paths[1] FROM path_values WHERE paths IS NOT NULL",
    )
    .await;
    assert_eq!(text(&rows(&assigned[0])[0][0]), Some("$.\"assigned\""));
}

#[tokio::test]
async fn jsonpath_has_no_sql_equality_hash_or_ordering_semantics() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE TABLE path_ops (path jsonpath, paths jsonpath[]); \
         INSERT INTO path_ops VALUES ('$.a', '{\"$.a\",\"$.b\"}')",
    )
    .await;

    for sql in [
        "SELECT DISTINCT path FROM path_ops",
        "SELECT path FROM path_ops GROUP BY path",
        "SELECT count(DISTINCT path) FROM path_ops",
        "SELECT path FROM path_ops UNION SELECT path FROM path_ops",
        "SELECT DISTINCT paths FROM path_ops",
        "SELECT paths = paths FROM path_ops",
        "SELECT path IN (path) FROM path_ops WHERE false",
        "SELECT path BETWEEN path AND path FROM path_ops WHERE false",
        "SELECT path = ANY(paths) FROM path_ops WHERE false",
        "SELECT CASE path WHEN path THEN 1 ELSE 0 END FROM path_ops WHERE false",
        "SELECT nullif(path, path) FROM path_ops WHERE false",
        "SELECT array_position(paths, path) FROM path_ops",
        "SELECT array_position('{NULL}'::jsonpath[], NULL)",
        "SELECT array_positions('{NULL}'::jsonpath[], NULL)",
        "SELECT array_remove('{NULL}'::jsonpath[], NULL)",
        "SELECT array_replace('{NULL}'::jsonpath[], NULL, NULL)",
    ] {
        let error = session
            .simple_query(sql)
            .await
            .expect_err("jsonpath equality-dependent operation must fail");
        assert_eq!(error.code, "42883", "{sql}: {error:?}");
        assert!(
            error.message.contains("equality") || error.message.contains("operator does not exist")
        );
    }

    for sql in [
        "SELECT path FROM path_ops ORDER BY path",
        "SELECT paths FROM path_ops ORDER BY paths",
        "SELECT path FROM path_ops WHERE false UNION ALL SELECT path FROM path_ops WHERE false ORDER BY path",
        "SELECT min(path) FROM path_ops WHERE false",
        "SELECT greatest(path, path) FROM path_ops WHERE false",
        "SELECT array_sort(paths) FROM path_ops",
    ] {
        let error = session
            .simple_query(sql)
            .await
            .expect_err("jsonpath ordering-dependent operation must fail");
        assert_eq!(error.code, "42883", "{sql}: {error:?}");
        assert!(
            error.message.contains("ordering operator")
                || error.message.contains("comparison function")
                || error.message.contains("function min(jsonpath)"),
            "{sql}: {error:?}"
        );
    }

    // Generic array operators/functions resolve without loading jsonpath's
    // missing element comparator until a row is actually evaluated.
    for sql in [
        "SELECT paths = paths FROM path_ops WHERE false",
        "SELECT array_position(paths, path) FROM path_ops WHERE false",
        "SELECT array_sort(paths) FROM path_ops WHERE false",
    ] {
        let result = run(&mut session, sql).await;
        assert!(rows(&result[0]).is_empty(), "{sql}");
    }
    let min_empty = run(&mut session, "SELECT min(paths) FROM path_ops WHERE false").await;
    assert_eq!(text(&rows(&min_empty[0])[0][0]), None);

    let empty_arrays = run(
        &mut session,
        "SELECT array_position('{}'::jsonpath[], NULL), \
                array_positions('{}'::jsonpath[], NULL), \
                array_remove('{}'::jsonpath[], NULL), \
                array_replace('{}'::jsonpath[], NULL, NULL)",
    )
    .await;
    assert_eq!(
        rows(&empty_arrays[0])[0]
            .iter()
            .map(text)
            .collect::<Vec<_>>(),
        vec![None, Some("{}"), Some("{}"), Some("{}")],
    );
}

#[tokio::test]
async fn common_type_coercions_run_the_jsonpath_input_function() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();

    for sql in [
        "VALUES ('lax $.a'), ('$.b'::jsonpath)",
        "SELECT 'lax $.a' UNION ALL SELECT '$.b'::jsonpath",
        "SELECT CASE WHEN true THEN 'lax $.a' ELSE '$.b'::jsonpath END",
        "SELECT coalesce('lax $.a', NULL::jsonpath)",
    ] {
        let result = run(&mut session, sql).await;
        let QueryResult::Rows { fields, rows, .. } = &result[0] else {
            panic!("expected rows for {sql:?}");
        };
        assert_eq!(fields[0].type_oid, 4072, "{sql}");
        assert_eq!(text(&rows[0][0]), Some("$.\"a\""), "{sql}");
    }

    for sql in [
        "VALUES (''), ('$'::jsonpath)",
        "SELECT coalesce('', NULL::jsonpath)",
    ] {
        let error = session
            .simple_query(sql)
            .await
            .expect_err("invalid common-type jsonpath must fail");
        assert_eq!(error.code, "22P02", "{sql}");
    }
}

#[tokio::test]
async fn jsonpath_domains_use_the_native_input_function() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE DOMAIN jsonpath_scalar_domain_test AS jsonpath NOT NULL; \
         CREATE DOMAIN jsonpath_array_domain_test AS jsonpath[] NOT NULL; \
         CREATE DOMAIN jsonpath_checked_domain_test AS jsonpath CHECK (VALUE IS NOT NULL)",
    )
    .await;

    let result = run(
        &mut session,
        "SELECT '$.a'::jsonpath_scalar_domain_test, \
                '$.b'::text::jsonpath_scalar_domain_test, \
                '{\"$.a\"}'::jsonpath_array_domain_test, \
                '{\"$.b\"}'::text::jsonpath_array_domain_test",
    )
    .await;
    assert_eq!(
        rows(&result[0])[0].iter().map(text).collect::<Vec<_>>(),
        vec![
            Some("$.\"a\""),
            Some("$.\"b\""),
            Some("{\"$.\\\"a\\\"\"}"),
            Some("{\"$.\\\"b\\\"\"}"),
        ]
    );

    let error = session
        .simple_query("SELECT NULL::jsonpath_scalar_domain_test")
        .await
        .expect_err("domain constraints still run after jsonpath input");
    assert_eq!(error.code, "23502");

    run(
        &mut session,
        "CREATE TABLE jsonpath_domain_values (\
             path jsonpath_scalar_domain_test, \
             paths jsonpath_array_domain_test\
         ); \
         INSERT INTO jsonpath_domain_values VALUES ('lax $.inserted', '{\"$.array\"}')",
    )
    .await;
    let stored = run(
        &mut session,
        "SELECT path, paths FROM jsonpath_domain_values",
    )
    .await;
    assert_eq!(
        rows(&stored[0])[0].iter().map(text).collect::<Vec<_>>(),
        vec![Some("$.\"inserted\""), Some("{\"$.\\\"array\\\"\"}")]
    );

    let error = session
        .simple_query("INSERT INTO jsonpath_domain_values VALUES ('', '{\"$.array\"}')")
        .await
        .expect_err("domain assignment must run jsonpath input");
    assert_eq!(error.code, "22P02");

    for sql in [
        "INSERT INTO jsonpath_domain_values (paths) VALUES ('{\"$.array\"}')",
        "INSERT INTO jsonpath_domain_values VALUES (DEFAULT, '{\"$.array\"}')",
    ] {
        let error = session
            .simple_query(sql)
            .await
            .expect_err("omitted/default values must enforce domain constraints");
        assert_eq!(error.code, "23502", "{sql}: {error:?}");
    }

    session
        .copy_in(
            "COPY jsonpath_domain_values (path, paths) FROM STDIN",
            vec![bytes::Bytes::from_static(
                b"lax $.copied\t{\"$.copied_array\"}\n",
            )],
        )
        .await
        .expect("COPY into jsonpath domains");
    let error = session
        .copy_in(
            "COPY jsonpath_domain_values (path, paths) FROM STDIN",
            vec![bytes::Bytes::from_static(b"\\N\t{\"$.array\"}\n")],
        )
        .await
        .expect_err("COPY NULL must enforce the domain constraint");
    assert_eq!(error.code, "23502");
    let error = session
        .copy_in(
            "COPY jsonpath_domain_values (paths) FROM STDIN",
            vec![bytes::Bytes::from_static(b"{\"$.array\"}\n")],
        )
        .await
        .expect_err("COPY omitted values must enforce the domain constraint");
    assert_eq!(error.code, "23502");

    run(
        &mut session,
        r#"CREATE FUNCTION pl_bad_declaration(seed jsonpath_checked_domain_test)
             RETURNS text LANGUAGE plpgsql AS $$
           DECLARE local seed%TYPE;
           BEGIN RETURN 'unreachable'; END
           $$;
           CREATE FUNCTION pl_bad_assignment()
             RETURNS text LANGUAGE plpgsql AS $$
           DECLARE local jsonpath_array_domain_test := '{"$.a"}';
           BEGIN local := NULL; RETURN 'unreachable'; END
           $$;
           CREATE FUNCTION pl_bad_argument(value jsonpath_array_domain_test)
             RETURNS text LANGUAGE plpgsql AS $$
           BEGIN RETURN 'unreachable'; END
           $$;
           CREATE FUNCTION pl_bad_return()
             RETURNS jsonpath_checked_domain_test LANGUAGE plpgsql AS $$
           BEGIN RETURN NULL; END
           $$"#,
    )
    .await;

    for (sql, code) in [
        ("SELECT pl_bad_declaration('$.a')", "23514"),
        ("SELECT pl_bad_assignment()", "23502"),
        ("SELECT pl_bad_argument(NULL)", "23502"),
        ("SELECT pl_bad_return()", "23514"),
    ] {
        let error = session
            .simple_query(sql)
            .await
            .expect_err("PL/pgSQL must enforce the domain constraint");
        assert_eq!(error.code, code, "{sql}: {error:?}");
    }
}
