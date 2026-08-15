//! `regclassout`: a `regclass` renders as the relation's name, not as its oid.
//!
//! The oid stays the value's identity. It is what `regclass` compares, hashes,
//! casts to `oid`/`int` and sends in binary. Only the *text* output changes
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

// All text cells of a result, row-major — for the multi-row reads below.
fn all_rows_text(result: &QueryResult) -> Vec<Vec<Option<String>>> {
    (0..rows(result).len())
        .map(|index| row_text(result, index))
        .collect()
}

// A `regclass` read back out of row storage prints the relation name, exactly as
// one produced by a cast does. The row encoding keeps only the oid — what
// PostgreSQL stores too — so the name has to be re-attached on the way out, and
// it has to happen for every shape of read, not just a bare scan: a projection,
// a `::text` cast, a join, a locking read, a derived table, a CTE, a set
// operation and a scalar subquery all reach the stored value by a different
// path. PostgreSQL prints `pg_class` for all of them.
#[tokio::test]
async fn a_stored_regclass_prints_the_relation_name_through_every_read_shape() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for setup in [
        "CREATE TABLE rc (id int, c regclass)",
        "INSERT INTO rc VALUES (1, 'pg_class'::regclass)",
        "CREATE TABLE other (id int, tag text)",
        "INSERT INTO other VALUES (1, 'a')",
    ] {
        session.simple_query(setup).await.expect(setup);
    }

    for read in [
        "SELECT c FROM rc",
        "SELECT c::text FROM rc",
        "SELECT c FROM rc WHERE c = 'pg_class'::regclass",
        "SELECT c FROM rc FOR UPDATE",
        "SELECT * FROM (SELECT c FROM rc) s",
        "WITH q AS (SELECT c FROM rc) SELECT * FROM q",
        "SELECT c FROM rc UNION SELECT 'pg_class'::regclass",
        "SELECT (SELECT c FROM rc)",
        "SELECT max(c::text) FROM rc",
    ] {
        assert!(
            scalar(&mut session, read).await == Some("pg_class".into()),
            "{read}"
        );
    }

    // A join reaches the stored column through a different relation build than a
    // bare scan does, so it gets its own whole-row expectation.
    let joined = query(
        &mut session,
        "SELECT r.c, o.tag FROM rc r JOIN other o ON o.id = r.id",
    )
    .await;
    assert!(all_rows_text(&joined) == vec![vec![Some("pg_class".into()), Some("a".into())]]);
}

// The name is derived from the catalog when the value is *read*, not when it is
// written: renaming the relation changes what an already-stored `regclass`
// prints, and dropping it leaves the bare oid — `regclassout`'s fallback — with
// no error. PostgreSQL 18.4 behaves exactly this way.
#[tokio::test]
async fn a_stored_regclass_follows_a_rename_and_survives_a_drop() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for setup in [
        "CREATE TABLE target (a int)",
        "CREATE TABLE rc (c regclass)",
        "INSERT INTO rc VALUES ('target'::regclass)",
    ] {
        session.simple_query(setup).await.expect(setup);
    }
    let oid = scalar(&mut session, "SELECT c::int FROM rc")
        .await
        .expect("oid");

    assert!(scalar(&mut session, "SELECT c FROM rc").await == Some("target".into()));

    session
        .simple_query("ALTER TABLE target RENAME TO target2")
        .await
        .expect("rename");
    assert!(scalar(&mut session, "SELECT c FROM rc").await == Some("target2".into()));

    session
        .simple_query("DROP TABLE target2")
        .await
        .expect("drop");
    // Both the value and its `→ text` cast fall back to the bare oid.
    assert!(scalar(&mut session, "SELECT c FROM rc").await == Some(oid.clone()));
    assert!(scalar(&mut session, "SELECT c::text FROM rc").await == Some(oid));
}

// A domain over `regclass` stores the base type's value, so it needs the same
// re-attachment a bare `regclass` column does.
#[tokio::test]
async fn a_stored_domain_over_regclass_prints_the_relation_name() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for setup in [
        "CREATE DOMAIN relref AS regclass",
        "CREATE TABLE dm (c relref)",
        "INSERT INTO dm VALUES ('pg_class'::regclass)",
    ] {
        session.simple_query(setup).await.expect(setup);
    }

    assert!(scalar(&mut session, "SELECT c FROM dm").await == Some("pg_class".into()));
}

// The stored value keeps the oid as its identity: the binary wire form is the
// 4-byte oid and the `→ int` cast is that same number, so re-attaching the name
// changes only the text rendering.
#[tokio::test]
async fn a_stored_regclass_keeps_the_oid_as_its_identity() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for setup in [
        "CREATE TABLE rc (c regclass)",
        "INSERT INTO rc VALUES ('pg_class'::regclass)",
    ] {
        session.simple_query(setup).await.expect(setup);
    }

    let result = query(&mut session, "SELECT c, c::int FROM rc").await;
    assert!(field_type_oids(&result) == vec![2205, 23]);
    let oid = scalar(&mut session, "SELECT c::int FROM rc")
        .await
        .expect("oid");
    assert!(row_text(&result, 0) == vec![Some("pg_class".into()), Some(oid)]);

    let binary = rows(&result)[0][0].as_ref().expect("a cell").binary.clone();
    assert!(binary.len() == 4);
}

// `regclassout` qualifies exactly when `RelationIsVisible` says an unqualified
// reference would miss the relation, so the same oid prints differently under
// different search paths. Both directions matter: a relation the path reaches
// loses its schema, and one it does not reach — `public` included — keeps it.
// Verified against `postgres:18.4`.
#[tokio::test]
async fn qualification_follows_the_search_path_in_both_directions() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for setup in [
        "CREATE SCHEMA app",
        "CREATE TABLE app.ap (a int)",
        "CREATE TABLE public.pp (a int)",
    ] {
        session.simple_query(setup).await.expect(setup);
    }

    let both = "SELECT 'app.ap'::regclass::text, 'public.pp'::regclass::text";
    assert!(
        row_text(&query(&mut session, both).await, 0)
            == vec![Some("app.ap".into()), Some("pp".into())]
    );

    session
        .simple_query("SET search_path = app")
        .await
        .expect("SET");
    assert!(
        row_text(&query(&mut session, both).await, 0)
            == vec![Some("ap".into()), Some("public.pp".into())]
    );
}

// A relation the path *would* reach by name, but for a relation of the same
// name earlier in the path, is not visible and prints qualified — the shadowing
// half of `RelationIsVisible`, which a "drop the schema for public" rule cannot
// express.
#[tokio::test]
async fn a_shadowed_relation_prints_qualified() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for setup in [
        "CREATE SCHEMA front",
        "CREATE TABLE front.dup (a int)",
        "CREATE TABLE public.dup (a int)",
        "SET search_path = front, public",
    ] {
        session.simple_query(setup).await.expect(setup);
    }

    let both = "SELECT 'front.dup'::regclass::text, 'public.dup'::regclass::text";
    assert!(
        row_text(&query(&mut session, both).await, 0)
            == vec![Some("dup".into()), Some("public.dup".into())]
    );
}

// A relation cannot be shadowed out of its bare spelling by a schema the caller
// is not allowed to search. That is not a cosmetic point: printing
// `public.pp` instead of `pp` is the engine saying that *something else called
// `pp` exists earlier on the path*, and the caller has no right to know that
// `secret.pp` exists. `recomputeNamespacePath` drops the schema before any
// shadowing question is asked. Verified against `postgres:18.4`, which prints
// `pp` for the same setup.
#[tokio::test]
async fn a_relation_in_an_unsearchable_schema_does_not_shadow_a_name_into_qualification() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for setup in [
        "CREATE ROLE lowly LOGIN",
        "CREATE SCHEMA secret",
        "CREATE TABLE secret.pp (a int)",
        "CREATE TABLE public.pp (a int)",
        "SET ROLE lowly",
        "SET search_path = secret, public",
    ] {
        session.simple_query(setup).await.expect(setup);
    }

    assert!(scalar(&mut session, "SELECT 'public.pp'::regclass::text").await == Some("pp".into()));

    // The bootstrap role does reach `secret`, so for it `secret.pp` is the one
    // that prints bare and `public.pp` is the one that keeps its schema. This
    // is the same relation and the same path: only the role differs.
    session
        .simple_query("RESET ROLE")
        .await
        .expect("RESET ROLE");
    let both = "SELECT 'secret.pp'::regclass::text, 'public.pp'::regclass::text";
    assert!(
        row_text(&query(&mut session, both).await, 0)
            == vec![Some("pp".into()), Some("public.pp".into())]
    );
}

// The session's own temporary namespace sits at the front of the search path,
// exactly as `recomputeNamespacePath` puts it there, so a temporary relation is
// visible and prints bare rather than as `pg_temp_<backend id>.tt`.
#[tokio::test]
async fn a_temporary_relation_prints_without_its_namespace() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    session
        .simple_query("CREATE TEMP TABLE tt (a int)")
        .await
        .expect("CREATE TEMP TABLE");

    assert!(scalar(&mut session, "SELECT 'tt'::regclass::text").await == Some("tt".into()));
}

// Both halves of a qualified name are quoted the way `quote_ident` quotes them,
// which the schema half only reaches once a schema needs quoting.
#[tokio::test]
async fn a_qualified_name_quotes_the_schema_too() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for setup in [
        "CREATE SCHEMA \"My Schema\"",
        "CREATE TABLE \"My Schema\".\"Odd Rel\" (a int)",
    ] {
        session.simple_query(setup).await.expect(setup);
    }

    assert!(
        scalar(
            &mut session,
            "SELECT '\"My Schema\".\"Odd Rel\"'::regclass::text"
        )
        .await
            == Some("\"My Schema\".\"Odd Rel\"".into())
    );
}

// A `reg*` cast in a grouped projection resolves against the catalog exactly as
// the same cast does in an ungrouped one. `GROUP BY tableoid` with
// `tableoid::regclass` in the select list is `copy.sql`'s shape, and it printed
// the bare oid while `SELECT tableoid::regclass` printed the name: the grouped
// evaluator ran the value layer's cast, which has no catalog.
#[tokio::test]
async fn a_grouped_projection_resolves_a_regclass_cast() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    for setup in [
        "CREATE TABLE parted (a int, b int) PARTITION BY LIST (b)",
        "CREATE TABLE parted_one PARTITION OF parted FOR VALUES IN (1)",
        "INSERT INTO parted VALUES (1, 1), (2, 1)",
    ] {
        session.simple_query(setup).await.expect(setup);
    }

    for sql in [
        "SELECT tableoid::regclass::text FROM parted GROUP BY tableoid",
        "SELECT max(tableoid)::regclass::text FROM parted",
        "SELECT DISTINCT tableoid::regclass::text FROM parted",
    ] {
        assert!(
            scalar(&mut session, sql).await == Some("parted_one".into()),
            "{sql}"
        );
    }
}
