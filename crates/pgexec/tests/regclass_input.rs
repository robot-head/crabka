//! `regclassin`: the half of `regclass` that reads a *written* relation name.
//!
//! Two properties hold it together. First, the parser reads the name the way
//! `stringToQualifiedNameList` reads one. Quoted parts stay literal and may
//! contain dots, the parser downcases unquoted parts, and whitespace is allowed
//! anywhere around a part. Second, the name resolves against the *session's*
//! search path, not against a fixed one. Both properties make the round trip
//! work: whatever `regclassout` prints for a relation reads back as that same
//! relation.
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

#[tokio::test]
async fn regtype_resolves_names_and_compares_as_an_oid() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for (sql, expected) in [
        ("SELECT 'int4'::regtype::text", "integer"),
        ("SELECT 23::regtype::text", "integer"),
        ("SELECT 'anyrange'::regtype::text", "anyrange"),
        ("SELECT 23 = 'integer'::regtype", "t"),
    ] {
        assert!(client.scalar(sql).await == Some(expected.into()), "{sql}");
    }
    assert!(
        client.fails("SELECT 'nosuch'::regtype").await
            == ("42704".into(), "type \"nosuch\" does not exist".into())
    );
}

#[tokio::test]
async fn regprocedure_resolves_and_renders_identity_arguments() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client
        .run("CREATE FUNCTION rp(a int, b text) RETURNS int LANGUAGE sql RETURN a")
        .await;
    for (sql, expected) in [
        (
            "SELECT 'boolin(cstring)'::regprocedure::text",
            "boolin(cstring)",
        ),
        ("SELECT 1242::regprocedure::text", "boolin(cstring)"),
        (
            "SELECT 'rp(int4,text)'::regprocedure::text",
            "rp(integer,text)",
        ),
        (
            "SELECT 1370::regprocedure::text",
            "\"interval\"(time without time zone)",
        ),
        ("SELECT 1242 = 'boolin(cstring)'::regprocedure", "t"),
    ] {
        assert!(client.scalar(sql).await == Some(expected.into()), "{sql}");
    }
}

/// `regnamespacein`/`regnamespaceout`. A written name folds and resolves like
/// any identifier; an oid with no schema prints as the bare number rather than
/// failing, which is what makes `psql`'s `\d` cast chain safe on a stale row.
#[tokio::test]
async fn regnamespace_resolves_names_and_falls_back_to_a_bare_oid() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE SCHEMA myschema").await;
    for (sql, expected) in [
        ("SELECT 'public'::regnamespace::text", "public"),
        // An unquoted name downcases; a quoted one is literal.
        ("SELECT 'PUBLIC'::regnamespace::text", "public"),
        ("SELECT '\"public\"'::regnamespace::text", "public"),
        ("SELECT '  public  '::regnamespace::text", "public"),
        ("SELECT 'myschema'::regnamespace::text", "myschema"),
        // A numeric string is an oid, not a name.
        ("SELECT '2200'::regnamespace::text", "public"),
        ("SELECT 11::regnamespace::text", "pg_catalog"),
        ("SELECT 'public'::regnamespace::oid", "2200"),
        // Identity is the oid, so comparison crosses the two spellings.
        ("SELECT 11 = 'pg_catalog'::regnamespace", "t"),
        // `regnamespaceout` has no name for an unknown oid and prints it bare.
        ("SELECT 999999::regnamespace::text", "999999"),
        // The cast chain `psql`'s `\d` runs over `pg_statistic_ext`.
        (
            "SELECT 2200::pg_catalog.regnamespace::pg_catalog.text",
            "public",
        ),
    ] {
        assert!(client.scalar(sql).await == Some(expected.into()), "{sql}");
    }
    assert!(client.scalar("SELECT NULL::regnamespace").await == None);
    // A written name that no schema answers to is 3F000, as `regnamespacein`
    // raises it — unlike an unknown oid, which is not an error at all.
    assert!(
        client.fails("SELECT 'nope'::regnamespace").await
            == ("3F000".into(), "schema \"nope\" does not exist".into())
    );
}

#[tokio::test]
async fn pg_proc_argument_vectors_are_zero_based_arrays() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for (sql, expected) in [
        (
            "SELECT array_lower(proargtypes, 1) FROM pg_proc WHERE oid = 1242",
            "0",
        ),
        (
            "SELECT array_upper(proargtypes, 1) FROM pg_proc WHERE oid = 1242",
            "0",
        ),
        (
            "SELECT proargtypes[0] FROM pg_proc WHERE oid = 1242",
            "2275",
        ),
        (
            "SELECT 2275 = ANY (proargtypes) FROM pg_proc WHERE oid = 1242",
            "t",
        ),
        (
            "SELECT proargtypes::regtype[]::text FROM pg_proc WHERE oid = 1242",
            "[0:0]={cstring}",
        ),
    ] {
        assert!(client.scalar(sql).await == Some(expected.into()), "{sql}");
    }
}

// -------------------------------------------------------------- reading a name

/// Every shape `regclassin` accepts, and the relation each one lands on.
///
/// Today's first-dot split gets two of them wrong: a quoted dot, and a name the
/// parser must downcase. Those two are the reason this is a parser and not a
/// `split_once`.
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

/// A name nothing answers to is `42P01`, spelled from the *parsed* parts.
///
/// The parts are dot-joined and unquoted, which is `PostgreSQL`'s
/// `NameListToString` and not the text as typed.
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

/// The property the two halves owe each other.
///
/// For every name shape, the text `regclassout` prints reads back through
/// `regclassin` as the same relation. An identifier that must be quoted on the
/// way out is exactly the shape the first-dot split could not read back in.
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

/// Every name-taking catalog function finds a relation that only the session's
/// `search_path` reaches, and so does a `regclass` cast. This is the same
/// relation `SELECT … FROM` reads.
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
/// one the path reaches, and not for `public`'s.
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
/// sequence name, not a `regclass` literal. It therefore reads back as written.
/// This is also true for a table whose name a `regclass` literal would have to
/// quote.
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

// ------------------------------------------------ a DDL-time regclass default

// A column `DEFAULT` is a DDL-time expression, and `'pg_class'::regclass` in one
// has to resolve against the catalog exactly as it does in a query. PostgreSQL
// folds the cast to the relation's oid while the `CREATE TABLE` is analysed, so
// the default is usable immediately and an unknown relation is a DDL error.
#[tokio::test]
async fn a_regclass_column_default_resolves_at_ddl_time() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE TABLE target (a int)").await;

    for ddl in [
        "CREATE TABLE d1 (c regclass DEFAULT 'target'::regclass)",
        "CREATE TABLE d2 (c regclass DEFAULT 'pg_class'::regclass)",
    ] {
        client.run(ddl).await;
    }
    for (table, expected) in [("d1", "target"), ("d2", "pg_class")] {
        client
            .run(&format!("INSERT INTO {table} DEFAULT VALUES"))
            .await;
        assert!(
            client.scalar(&format!("SELECT c FROM {table}")).await == Some(expected.into()),
            "{table}"
        );
    }
}

// `ALTER TABLE … ADD COLUMN` and `ALTER COLUMN … SET DEFAULT` evaluate their
// default in the same DDL context a `CREATE TABLE` does, so all three resolve a
// `regclass` the same way. An added column's default is also materialized into
// the rows that already exist.
#[tokio::test]
async fn altered_regclass_defaults_resolve_at_ddl_time() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for ddl in [
        "CREATE TABLE target (a int)",
        "CREATE TABLE t (x int)",
        "INSERT INTO t VALUES (1)",
        "ALTER TABLE t ADD COLUMN c regclass DEFAULT 'target'::regclass",
        "CREATE TABLE u (x int, c regclass)",
        "ALTER TABLE u ALTER COLUMN c SET DEFAULT 'target'::regclass",
        "INSERT INTO u (x) VALUES (1)",
    ] {
        client.run(ddl).await;
    }

    // The existing row was backfilled with the resolved default.
    assert!(client.scalar("SELECT c FROM t").await == Some("target".into()));
    assert!(client.scalar("SELECT c FROM u").await == Some("target".into()));
}

// The stored default deparses back to its source spelling for `\d` (which reads
// `pg_get_expr(pg_attrdef.adbin, …)`) and for `information_schema.columns`.
// PostgreSQL 18.4 prints `'pg_class'::regclass` in both.
#[tokio::test]
async fn a_regclass_default_deparses_as_a_regclass_literal() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client
        .run("CREATE TABLE d (c regclass DEFAULT 'pg_class'::regclass)")
        .await;

    for query in [
        "SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_attrdef d WHERE d.adrelid = 'd'::regclass",
        "SELECT column_default FROM information_schema.columns WHERE table_name = 'd'",
    ] {
        assert!(
            client.scalar(query).await == Some("'pg_class'::regclass".into()),
            "{query}"
        );
    }
}

// The default stores the oid, so renaming the relation it names changes what the
// default deparses to — the same read-time resolution a stored `regclass` value
// gets. PostgreSQL behaves this way because the folded constant holds the oid.
#[tokio::test]
async fn a_regclass_default_follows_a_rename_of_its_relation() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for ddl in [
        "CREATE TABLE target (a int)",
        "CREATE TABLE d (c regclass DEFAULT 'target'::regclass)",
    ] {
        client.run(ddl).await;
    }
    let deparse =
        "SELECT pg_get_expr(d.adbin, d.adrelid) FROM pg_attrdef d WHERE d.adrelid = 'd'::regclass";
    assert!(client.scalar(deparse).await == Some("'target'::regclass".into()));

    client.run("ALTER TABLE target RENAME TO target2").await;
    assert!(client.scalar(deparse).await == Some("'target2'::regclass".into()));
    // A row inserted after the rename takes the same oid, so it prints the new
    // name too.
    client.run("INSERT INTO d DEFAULT VALUES").await;
    assert!(client.scalar("SELECT c FROM d").await == Some("target2".into()));
}

// A default naming a relation that does not exist is refused when the DDL runs,
// not when a row is inserted — PostgreSQL reports 42P01 from the `CREATE TABLE`.
#[tokio::test]
async fn a_regclass_default_naming_no_relation_is_refused_at_ddl_time() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);

    let (code, message) = client
        .fails("CREATE TABLE d (c regclass DEFAULT 'no_such_rel'::regclass)")
        .await;
    assert!(code == "42P01", "{code}: {message}");
    assert!(message.contains("no_such_rel"), "{message}");
}

// `INSERT … RETURNING` renders the default without the value ever passing
// through a scan, so the resolved name has to be on the value the default
// itself produced.
#[tokio::test]
async fn a_regclass_default_prints_its_name_through_returning() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for ddl in [
        "CREATE TABLE target (a int)",
        "CREATE TABLE d (c regclass DEFAULT 'target'::regclass)",
    ] {
        client.run(ddl).await;
    }

    assert!(
        client
            .scalar("INSERT INTO d DEFAULT VALUES RETURNING c")
            .await
            == Some("target".into())
    );
}

/// `IN` has two spellings and `reg*` is the family that can tell them apart.
///
/// `transformAExprIn` builds `x = ANY (ARRAY[…])` when more than one right-hand
/// item is free of `Var`s, and the array's element type is `select_common_type`
/// over the operand and the list — so each `unknown` literal reaches `regclass`
/// through `regclassin`, and resolves as a relation *name*. With one such item
/// or none it builds an OR-chain of `=` instead, and `regclass`'s equality is
/// `oideq`, so the literal is read as an oid and a name is a syntax error.
///
/// Captured from `postgres:18.4`, which accepts the two-element list and
/// refuses every one-element form beside it.
#[tokio::test]
async fn a_multi_element_in_list_reads_its_literals_as_relation_names() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    for ddl in ["CREATE TABLE lhs (a int)", "CREATE TABLE rhs (b int)"] {
        client.run(ddl).await;
    }

    for (sql, expected) in [
        ("SELECT 'lhs'::regclass IN ('lhs', 'rhs')", "t"),
        ("SELECT 'lhs'::regclass IN ('rhs', 'pg_class')", "f"),
        ("SELECT 'lhs'::regclass NOT IN ('rhs', 'pg_class')", "t"),
        // A list of three keeps the array form, and a numeric literal beside a
        // name is still resolved by the same input function.
        ("SELECT 'lhs'::regclass IN ('rhs', 'pg_class', 'lhs')", "t"),
        // The operand carries the type through a cast off a catalog column,
        // which is the shape `create_misc` writes.
        (
            "SELECT count(*)::text FROM pg_class \
             WHERE oid::regclass IN ('lhs', 'rhs')",
            "2",
        ),
    ] {
        assert!(client.scalar(sql).await == Some(expected.into()), "{sql}");
    }
}

/// The OR-chain half of the same rule: one non-`Var` item is not enough for the
/// array form, so the literal beside a `regclass` is an oid.
#[tokio::test]
async fn a_single_element_in_list_reads_its_literal_as_an_oid() {
    let engine = SqlEngine::new();
    let mut client = Client::new(&engine);
    client.run("CREATE TABLE lhs (a int)").await;

    for sql in [
        "SELECT 'lhs'::regclass = 'lhs'",
        "SELECT 'lhs'::regclass IN ('lhs')",
        "SELECT 'lhs'::regclass NOT IN ('lhs')",
        "SELECT 'lhs'::regclass BETWEEN 'lhs' AND 'lhs'",
    ] {
        let (code, message) = client.fails(sql).await;
        assert!(code == "42804", "{sql}: {code} {message}");
    }
}
