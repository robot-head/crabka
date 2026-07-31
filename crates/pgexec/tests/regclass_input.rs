//! `regclassin`: the half of `regclass` that reads a *written* relation name.
//!
//! Two properties hold it together. The name is parsed the way
//! `stringToQualifiedNameList` parses one — quoted parts literal and allowed to
//! contain dots, unquoted parts downcased, whitespace anywhere around a part —
//! and it resolves against the *session's* search path, not a fixed one. Both
//! make the round trip work: whatever `regclassout` prints for a relation reads
//! back as that same relation.
//!
//! Every expectation here was captured from `postgres:18.4` first.

use assert2::assert;
use crabka_pgexec::SqlEngine;
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

/// One session, so a `SET search_path` and the statements that depend on it
/// stay together.
struct Client {
    session: crabka_pgexec::SqlSession,
}

impl Client {
    fn new(engine: &SqlEngine) -> Self {
        Self {
            session: engine.connect(),
        }
    }

    async fn run(&mut self, sql: &str) -> QueryResult {
        self.session
            .simple_query(sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
            .into_iter()
            .next_back()
            .expect("at least one result")
    }

    async fn scalar(&mut self, sql: &str) -> Option<String> {
        let result = self.run(sql).await;
        let QueryResult::Rows { rows, .. } = &result else {
            panic!("expected rows from {sql}, got {result:?}");
        };
        assert!(rows.len() == 1, "{sql}");
        rows[0][0]
            .as_ref()
            .map(|cell: &Cell| String::from_utf8(cell.text.to_vec()).expect("utf-8 cell"))
    }

    /// The SQLSTATE and message of a statement that is refused.
    async fn fails(&mut self, sql: &str) -> (String, String) {
        let error = self
            .session
            .simple_query(sql)
            .await
            .expect_err("statement is refused");
        (error.code, error.message)
    }
}

/// The `regclass` text a spelling resolves to, which is the relation's own name
/// as `regclassout` prints it.
async fn printed(client: &mut Client, spelling: &str) -> Option<String> {
    client
        .scalar(&format!("SELECT '{spelling}'::regclass::text"))
        .await
}

// -------------------------------------------------------------- reading a name

/// Every shape `regclassin` accepts, and the relation each one lands on. The
/// two that today's first-dot split gets wrong — a quoted dot and a name that
/// needs downcasing — are the reason this is a parser and not a `split_once`.
#[tokio::test]
async fn a_written_name_is_read_the_way_regclassin_reads_one() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for ddl in [
        "CREATE TABLE mytable (x int)",
        "CREATE TABLE \"a.b\" (x int)",
        "CREATE TABLE \"MyTbl\" (x int)",
        "CREATE TABLE \"a\"\"b\" (x int)",
    ] {
        client.run(ddl).await;
    }

    let cases = [
        // An unquoted part downcases; a quoted one is literal.
        ("mytable", "mytable"),
        ("MYTABLE", "mytable"),
        ("MyTable", "mytable"),
        ("\"MyTbl\"", "\"MyTbl\""),
        // A dot inside quotes belongs to the name.
        ("\"a.b\"", "\"a.b\""),
        // `""` inside quotes is one literal quote, on the way in and out.
        ("\"a\"\"b\"", "\"a\"\"b\""),
        // Whitespace may sit anywhere around a part, including around the dot.
        (" mytable ", "mytable"),
        ("public . mytable", "mytable"),
        ("  PUBLIC  .  MYTABLE  ", "mytable"),
        ("\"public\".\"mytable\"", "mytable"),
        // Three parts name a catalog, and this database answers to one name.
        ("postgres.public.mytable", "mytable"),
    ];
    for (spelling, expected) in cases {
        assert!(
            printed(&mut client, spelling).await == Some(expected.into()),
            "{spelling:?}"
        );
    }
}

/// Text that is not a qualified name at all, with the SQLSTATE and message
/// `PostgreSQL` raises for it.
#[tokio::test]
async fn text_that_is_not_a_qualified_name_is_refused() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE TABLE t (x int)").await;

    let syntax = ("42602", "invalid name syntax");
    let cases = [
        ("''", syntax),
        ("'   '", syntax),
        ("'.t'", syntax),
        ("'t.'", syntax),
        ("'a..b'", syntax),
        // A quote that never closes — with and without a doubled quote inside
        // it — and text after one that does.
        ("'\"abc'", syntax),
        ("'\"a\"\"b'", syntax),
        ("'\"a\"b'", syntax),
        // Whitespace inside an unquoted part ends it, and what follows is
        // neither a separator nor the end of the text.
        ("'x y'", syntax),
        (
            "'a.b.c.d'",
            (
                "42601",
                "improper relation name (too many dotted names): a.b.c.d",
            ),
        ),
        (
            "'otherdb.public.t'",
            (
                "0A000",
                "cross-database references are not implemented: \"otherdb.public.t\"",
            ),
        ),
    ];
    for (literal, (code, message)) in cases {
        let failure = client.fails(&format!("SELECT {literal}::regclass")).await;
        assert!(
            failure == (code.to_string(), message.to_string()),
            "{literal}"
        );
    }
}

/// A name nothing answers to is `42P01`, spelled from the *parsed* parts —
/// dot-joined and unquoted, which is `PostgreSQL`'s `NameListToString` and not
/// the text as typed.
#[tokio::test]
async fn a_missing_relation_is_named_by_its_parsed_parts() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for (spelling, named) in [
        ("nosuch", "nosuch"),
        (" NoSuch . T ", "nosuch.t"),
        ("\"A b\".c", "A b.c"),
        ("\"a.b\"", "a.b"),
    ] {
        let failure = client
            .fails(&format!("SELECT '{spelling}'::regclass"))
            .await;
        assert!(
            failure
                == (
                    "42P01".to_string(),
                    format!("relation \"{named}\" does not exist")
                ),
            "{spelling:?}"
        );
    }
}

// ------------------------------------------------------------- the round trip

/// The property the two halves owe each other: for every name shape, the text
/// `regclassout` prints reads back through `regclassin` as the same relation.
/// An identifier that has to be quoted on the way out is exactly the shape the
/// first-dot split could not read back in.
#[tokio::test]
async fn every_name_shape_round_trips_out_and_back_in() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE SCHEMA s1").await;
    client.run("CREATE SCHEMA \"S 2\"").await;

    let relations = [
        "CREATE TABLE plain (x int)",
        "CREATE TABLE \"MixedCase\" (x int)",
        "CREATE TABLE \"Weird Name\" (x int)",
        "CREATE TABLE \"a.b\" (x int)",
        "CREATE TABLE \"quote\"\"d\" (x int)",
        "CREATE TABLE s1.inschema (x int)",
        "CREATE TABLE s1.\"Mixed In Schema\" (x int)",
        "CREATE TABLE \"S 2\".\"dotted.name\" (x int)",
        "CREATE VIEW s1.v AS SELECT 1 AS a",
        "CREATE SEQUENCE s1.\"My Seq\"",
    ];
    for ddl in relations {
        client.run(ddl).await;
    }

    // Every relation in the catalog, by oid — so the set under test is whatever
    // was just created rather than a list that can drift from it.
    let QueryResult::Rows { rows, .. } = client
        .run(
            "SELECT c.oid::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname IN ('public', 's1', 'S 2') ORDER BY c.oid",
        )
        .await
    else {
        panic!("expected rows");
    };
    let oids: Vec<String> = rows
        .iter()
        .map(|row| {
            String::from_utf8(row[0].as_ref().expect("an oid").text.to_vec()).expect("utf-8 cell")
        })
        .collect();
    assert!(oids.len() >= relations.len(), "{oids:?}");

    for oid in oids {
        let out = client
            .scalar(&format!("SELECT {oid}::regclass::text"))
            .await
            .expect("regclassout prints a name");
        // The printed text is a SQL identifier, never a bare oid: a relation
        // that printed as its number would round trip for the wrong reason.
        assert!(out.parse::<i32>().is_err(), "{oid} printed as {out}");
        let back = client
            .scalar(&format!(
                "SELECT '{}'::regclass::int",
                out.replace('\'', "''")
            ))
            .await;
        assert!(
            back == Some(oid.clone()),
            "{out} did not read back as {oid}"
        );
    }
}

// ---------------------------------------------------------- the session's path

/// A relation only the session's `search_path` reaches is found by every
/// name-taking catalog function, and by a `regclass` cast — the same relation
/// `SELECT … FROM` reads.
#[tokio::test]
async fn a_name_taking_function_resolves_against_the_sessions_search_path() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for ddl in [
        "CREATE SCHEMA s1",
        "SET search_path = s1",
        "CREATE TABLE t (id serial, x int)",
        "CREATE INDEX t_x_idx ON t (x)",
        "CREATE VIEW v AS SELECT 1 AS a",
        "COMMENT ON TABLE t IS 'the one in s1'",
        "COMMENT ON COLUMN t.id IS 'the key in s1'",
    ] {
        client.run(ddl).await;
    }

    // The bare name reaches the relation for the cast and for every function
    // that takes one, each answering exactly what the qualified name answers.
    let cases = [
        ("'t'::regclass::int", "'s1.t'::regclass::int"),
        ("'v'::regclass::int", "'s1.v'::regclass::int"),
        ("pg_get_viewdef('v')", "pg_get_viewdef('s1.v')"),
        (
            "pg_get_indexdef('t_x_idx')",
            "pg_get_indexdef('s1.t_x_idx')",
        ),
        (
            "pg_relation_size('t')::text",
            "pg_relation_size('s1.t')::text",
        ),
        (
            "pg_total_relation_size('t')::text",
            "pg_total_relation_size('s1.t')::text",
        ),
        (
            "pg_get_serial_sequence('t', 'id')",
            "pg_get_serial_sequence('s1.t', 'id')",
        ),
        // The comment functions take the relation as a name here; crabka
        // resolves one where `PostgreSQL` insists on an oid.
        ("obj_description('t')", "obj_description('s1.t')"),
        ("col_description('t', 1)", "col_description('s1.t', 1)"),
        (
            "has_table_privilege('t', 'SELECT')::text",
            "has_table_privilege('s1.t', 'SELECT')::text",
        ),
    ];
    for (bare, qualified) in cases {
        let by_bare = client.scalar(&format!("SELECT {bare}")).await;
        let by_qualified = client.scalar(&format!("SELECT {qualified}")).await;
        assert!(by_bare == by_qualified, "{bare} vs {qualified}");
        assert!(by_bare.is_some(), "{bare} answered NULL");
    }

    // The comment proves the answer came from the s1 relation rather than from
    // any relation of that name.
    assert!(client.scalar("SELECT obj_description('t')").await == Some("the one in s1".into()));
}

/// With the same name in two schemas, each name-taking function answers for the
/// one the path reaches — not for `public`'s.
#[tokio::test]
async fn a_shadowed_name_answers_for_the_schema_the_path_reaches() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for ddl in [
        "CREATE SCHEMA s1",
        "CREATE TABLE public.t (x int)",
        "CREATE TABLE s1.t (x int)",
        "CREATE VIEW public.v AS SELECT 1 AS pub",
        "CREATE VIEW s1.v AS SELECT 2 AS ess",
        "COMMENT ON TABLE public.t IS 'public one'",
        "COMMENT ON TABLE s1.t IS 's1 one'",
    ] {
        client.run(ddl).await;
    }

    for (path, table_comment, view_column) in [
        ("public, s1", "public one", "pub"),
        ("s1, public", "s1 one", "ess"),
    ] {
        client.run(&format!("SET search_path = {path}")).await;
        assert!(
            client.scalar("SELECT obj_description('t')").await == Some(table_comment.into()),
            "{path}"
        );
        let definition = client
            .scalar("SELECT pg_get_viewdef('v')")
            .await
            .expect("a view definition");
        assert!(definition.contains(view_column), "{path}: {definition}");
    }
}

// ------------------------------------------------------------ the sequence path

/// `nextval`/`setval` take the same `regclass` input, through the same parser:
/// a quoted name keeps its spaces and case, an unquoted one downcases, and an
/// unqualified one resolves through the session's path.
#[tokio::test]
async fn a_sequence_name_is_read_and_resolved_like_any_other_regclass_input() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for ddl in [
        "CREATE SCHEMA s1",
        "CREATE SEQUENCE public.\"My Seq\"",
        "CREATE SEQUENCE s1.onlyhere",
        "SET search_path = s1, public",
    ] {
        client.run(ddl).await;
    }

    let cases = [
        // A quoted name is literal — the shape the first-dot split could never
        // read, because nothing on that path unquoted anything.
        ("nextval('\"My Seq\"')", "1"),
        ("setval('\"My Seq\"', 10)", "10"),
        ("nextval('\"My Seq\"')", "11"),
        ("nextval(' PUBLIC . \"My Seq\" ')", "12"),
        ("nextval('public.\"My Seq\"')", "13"),
        // An unqualified name takes the session's path, which reaches s1.
        ("nextval('onlyhere')", "1"),
        ("nextval('ONLYHERE')", "2"),
        ("nextval('s1.onlyhere')", "3"),
        // And the round trip: what `regclassout` prints reads back as the same
        // sequence.
        ("nextval(('\"My Seq\"'::regclass)::text)", "14"),
    ];
    for (call, expected) in cases {
        assert!(
            client.scalar(&format!("SELECT {call}")).await == Some(expected.into()),
            "{call}"
        );
    }

    // The same refusals the cast raises, because it is the same parser.
    for (call, code) in [
        ("nextval('MY SEQ')", "42602"),
        ("nextval('')", "42602"),
        ("nextval('a.b.c.d')", "42601"),
        ("setval('otherdb.public.s', 1)", "0A000"),
        // crabka names the missing object a `sequence` where PostgreSQL names
        // it a `relation`; the SQLSTATE is the same 42P01 either way.
        ("setval('nosuch.s', 1)", "42P01"),
    ] {
        assert!(
            client.fails(&format!("SELECT {call}")).await.0 == code,
            "{call}"
        );
    }
}

/// A `serial` column's stored default is the catalog's own rendering of the
/// sequence name, not a `regclass` literal — so it is read back as written,
/// including for a table whose name a `regclass` literal would have to quote.
#[tokio::test]
async fn a_serial_default_still_draws_from_its_sequence() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for ddl in [
        "CREATE SCHEMA s1",
        "CREATE TABLE \"My T\" (id serial, x int)",
        "CREATE TABLE s1.\"Other T\" (id serial, x int)",
    ] {
        client.run(ddl).await;
    }

    for insert in [
        "INSERT INTO \"My T\" (x) VALUES (1), (2)",
        "INSERT INTO s1.\"Other T\" (x) VALUES (1), (2)",
    ] {
        client.run(insert).await;
    }
    for query in [
        "SELECT min(id)::text || '-' || max(id)::text FROM \"My T\"",
        "SELECT min(id)::text || '-' || max(id)::text FROM s1.\"Other T\"",
    ] {
        assert!(client.scalar(query).await == Some("1-2".into()), "{query}");
    }
}
