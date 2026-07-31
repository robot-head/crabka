//! `regclassout`: a `regclass` renders as the relation's name, not as its oid.
//!
//! The oid stays the value's identity — it is what `regclass` compares, hashes,
//! casts to `oid`/`int` and sends in binary — so only the *text* output changes
//! shape. Every expectation here is `PostgreSQL` 18.4's.

use assert2::assert;
use bytes::Bytes;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{BoundParam, Cell, Engine, ExecuteOutcome, QueryResult, Session};

async fn query(session: &mut SqlSession, sql: &str) -> QueryResult {
    session
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

fn field_type_oids(result: &QueryResult) -> Vec<u32> {
    let QueryResult::Rows { fields, .. } = result else {
        panic!("expected rows, got {result:?}");
    };
    fields.iter().map(|field| field.type_oid).collect()
}

// The single scalar a one-column, one-row query produced, as text.
async fn scalar(session: &mut SqlSession, sql: &str) -> Option<String> {
    let result = query(session, sql).await;
    assert!(rows(&result).len() == 1, "{sql}");
    row_text(&result, 0).swap_remove(0)
}

// Every spelling of the same relation prints the relation's name: a bare name,
// a schema-qualified one, the oid as a numeric string, and the oid as an
// integer. PostgreSQL resolves all four to `pg_class` and prints `pg_class`.
#[tokio::test]
async fn every_input_spelling_prints_the_relation_name() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();

    for input in [
        "'pg_class'::regclass",
        "'pg_catalog.pg_class'::regclass",
        "CAST('pg_class' AS regclass)",
        "'1259'::regclass",
        "1259::regclass",
        "1259::int8::regclass",
        "'pg_class'::regclass::regclass",
    ] {
        assert!(
            scalar(&mut session, &format!("SELECT {input}")).await == Some("pg_class".into()),
            "{input}"
        );
    }
}

// A user table, a view, a sequence and an index are all `pg_class` relations,
// and all four resolve by name and print by name.
#[tokio::test]
async fn tables_views_sequences_and_indexes_all_print_their_name() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for ddl in [
        "CREATE TABLE t_demo (a int PRIMARY KEY, b int)",
        "CREATE INDEX t_demo_b_idx ON t_demo (b)",
        "CREATE VIEW v_demo AS SELECT a FROM t_demo",
        "CREATE SEQUENCE s_demo",
    ] {
        session.simple_query(ddl).await.expect(ddl);
    }

    for name in ["t_demo", "t_demo_b_idx", "v_demo", "s_demo"] {
        // By name, and then by the oid that name resolved to — the round trip
        // through the oid is what proves the name came from the catalog and not
        // from the input text.
        assert!(
            scalar(&mut session, &format!("SELECT '{name}'::regclass")).await == Some(name.into()),
            "{name} by name"
        );
        assert!(
            scalar(
                &mut session,
                &format!("SELECT '{name}'::regclass::int::regclass")
            )
            .await
                == Some(name.into()),
            "{name} by oid"
        );
    }
}

// A relation outside the search path prints schema-qualified; the two schemas
// that are always on it (`public`, `pg_catalog`) print bare. `information_
// schema` is the qualified case crabka has relations in.
#[tokio::test]
async fn a_relation_outside_the_search_path_prints_schema_qualified() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();

    for input in [
        "'information_schema.schemata'::regclass",
        "'information_schema.schemata'::regclass::int::regclass",
    ] {
        assert!(
            scalar(&mut session, &format!("SELECT {input}")).await
                == Some("information_schema.schemata".into()),
            "{input}"
        );
    }
}

// `regclassout` quotes exactly as `quote_ident` does: a name that is not a bare
// lowercase identifier, or that collides with a keyword, comes back quoted.
#[tokio::test]
async fn output_quotes_identifiers_that_quote_ident_would_quote() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for name in ["Weird Name", "select", "MixedCase"] {
        session
            .simple_query(&format!("CREATE TABLE \"{name}\" (a int)"))
            .await
            .expect("create table");
    }

    for (name, quoted) in [
        ("Weird Name", "\"Weird Name\""),
        ("select", "\"select\""),
        ("MixedCase", "\"MixedCase\""),
    ] {
        let sql = format!(
            "SELECT oid::regclass FROM pg_class WHERE relname = '{name}' AND relkind = 'r'"
        );
        assert!(
            scalar(&mut session, &sql).await == Some(quoted.into()),
            "{name}"
        );
    }
}

// An oid no relation has is not an error: PostgreSQL prints `-` for the invalid
// oid and the bare number for any other, and `-` reads back as oid 0.
#[tokio::test]
async fn an_unmatched_oid_prints_as_a_dash_or_the_bare_number() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();

    assert!(scalar(&mut session, "SELECT 999999::regclass").await == Some("999999".into()));
    assert!(scalar(&mut session, "SELECT '999999'::regclass").await == Some("999999".into()));
    assert!(scalar(&mut session, "SELECT 0::regclass").await == Some("-".into()));
    assert!(scalar(&mut session, "SELECT '-'::regclass").await == Some("-".into()));
    assert!(scalar(&mut session, "SELECT '-'::regclass::int").await == Some("0".into()));
    assert!(
        scalar(&mut session, "SELECT NULL::regclass")
            .await
            .is_none()
    );

    // A *name* nothing matches is still 42P01, as it has always been.
    let error = engine
        .connect()
        .simple_query("SELECT 'no_such_relation'::regclass")
        .await
        .expect_err("unknown relation");
    assert!(error.code == "42P01");
}

// The oid is the identity: `regclass` compares, casts and orders as its oid, so
// a catalog predicate written against a relation name is still the integer
// comparison it was before the name came along.
#[tokio::test]
async fn the_oid_stays_the_value_identity() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();

    for (sql, expected) in [
        ("SELECT 'pg_class'::regclass::int", "1259"),
        ("SELECT 'pg_class'::regclass::oid", "1259"),
        ("SELECT 'pg_class'::regclass::int8", "1259"),
        ("SELECT ('pg_class'::regclass = 1259)::text", "true"),
        ("SELECT ('pg_class'::regclass < 1260)::text", "true"),
        (
            "SELECT relname FROM pg_class WHERE oid = 'pg_class'::regclass",
            "pg_class",
        ),
    ] {
        assert!(
            scalar(&mut session, sql).await == Some(expected.into()),
            "{sql}"
        );
    }
}

// `regclass` describes as oid 2205 on the wire, and `::text` as text (25) —
// the rendering changed, the advertised type did not.
#[tokio::test]
async fn the_row_description_still_reports_regclass() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    let result = query(
        &mut session,
        "SELECT 'pg_class'::regclass AS a, 'pg_class'::regclass::text AS b",
    )
    .await;
    assert!(field_type_oids(&result) == vec![2205, 25]);
    assert!(row_text(&result, 0) == vec![Some("pg_class".into()), Some("pg_class".into())]);
}

// `to_jsonb` of a `regclass` is a JSON *string* holding the output function's
// text, not the number the value is stored as.
#[tokio::test]
async fn to_jsonb_of_a_regclass_is_a_json_string() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE pp (a int)")
        .await
        .expect("create table");

    assert!(scalar(&mut session, "SELECT to_jsonb('pp'::regclass)").await == Some("\"pp\"".into()));
    assert!(scalar(&mut session, "SELECT to_jsonb(0::regclass)").await == Some("\"-\"".into()));
}

// The extended protocol resolves a `regclass` parameter at bind time — a name
// or an oid in text format, the 4-byte oid in binary — and every one of them
// prints as the relation name.
#[tokio::test]
async fn an_extended_protocol_regclass_parameter_prints_the_relation_name() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE pgbench_accounts (aid int4 PRIMARY KEY)")
        .await
        .expect("create table");
    let oid: i32 = scalar(&mut session, "SELECT 'pgbench_accounts'::regclass::int")
        .await
        .expect("oid")
        .parse()
        .expect("an oid is an integer");

    for (format, value) in [
        (0_i16, b"pgbench_accounts".to_vec()),
        (0, oid.to_string().into_bytes()),
        (1, oid.to_be_bytes().to_vec()),
    ] {
        let param = BoundParam {
            type_oid: Some(2205),
            format,
            value: Some(Bytes::from(value)),
        };
        session
            .parse("", "SELECT $1", &[2205])
            .await
            .expect("parse");
        session.bind("", "", &[param], &[]).await.expect("bind");
        let ExecuteOutcome::Rows { rows, .. } = session.execute("", 0).await.expect("execute")
        else {
            panic!("expected rows");
        };
        let text = rows[0][0]
            .as_ref()
            .map(|bytes| String::from_utf8(bytes.to_vec()).expect("valid text cell"));
        assert!(text == Some("pgbench_accounts".into()), "format {format}");
    }
}
