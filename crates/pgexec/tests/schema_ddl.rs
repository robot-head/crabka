//! Schema DDL against a real in-process engine: which schemas a database has
//! before anything is created in it, which of those may be created or dropped,
//! and what `pg_namespace` projects for each.
//!
//! Every SQLSTATE, message and `DETAIL` asserted here comes from a live
//! `PostgreSQL` 18.4 server, not from documentation. Where this engine
//! knowingly diverges, the test pins the *current* behaviour and says so at the
//! assertion.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::{
    engine::{Cell, Engine, QueryResult, Session},
    error::PgError,
};

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

async fn query(s: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    rows_text(&run(s, sql).await[0])
}

fn text_row(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_string())).collect()
}

/// A failed statement, as everything `PostgreSQL` puts on the wire for it.
///
/// A case compares this as one value, so it states its whole expected error
/// instead of a chain of field assertions.
#[derive(Debug, PartialEq, Eq)]
struct Failure {
    code: String,
    message: String,
    detail: Option<String>,
    hint: Option<String>,
}

impl Failure {
    fn new(code: &str, message: &str) -> Self {
        Self {
            code: code.to_string(),
            message: message.to_string(),
            detail: None,
            hint: None,
        }
    }

    fn detail(mut self, detail: &str) -> Self {
        self.detail = Some(detail.to_string());
        self
    }

    fn of(error: PgError) -> Self {
        let diagnostics = error.diagnostics.unwrap_or_default();
        Self {
            code: error.code,
            message: error.message,
            detail: diagnostics.detail,
            hint: diagnostics.hint,
        }
    }
}

async fn failure_of(s: &mut SqlSession, sql: &str) -> Failure {
    Failure::of(s.simple_query(sql).await.expect_err("expected an error"))
}

/// The `DETAIL` every 42939 unacceptable-name refusal carries.
const RESERVED_PREFIX_DETAIL: &str = "The prefix \"pg_\" is reserved for system schemas.";

/// Every schema and its owner, as `\dn` reads them.
async fn schemas(s: &mut SqlSession) -> Vec<Vec<Option<String>>> {
    query(
        s,
        "SELECT nspname, pg_get_userbyid(nspowner) FROM pg_namespace ORDER BY nspname",
    )
    .await
}

/// The same list through the standard view, which has to track `pg_namespace`.
async fn standard_schemas(s: &mut SqlSession) -> Vec<Vec<Option<String>>> {
    query(
        s,
        "SELECT schema_name, schema_owner FROM information_schema.schemata ORDER BY schema_name",
    )
    .await
}

async fn engine_with(setup: &[&str]) -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    for sql in setup {
        run(&mut s, sql).await;
    }
    (engine, s)
}

// ---------------------------------------------------------------------------
// What a database starts with
// ---------------------------------------------------------------------------

/// A database that has never run a `CREATE SCHEMA` still has three schemas.
///
/// `pg_database_owner` owns `public`, not the bootstrap superuser. This is the
/// one place where `PostgreSQL`'s ownership differs across them.
#[tokio::test]
async fn a_fresh_database_has_the_three_bootstrap_schemas() {
    let (_engine, mut s) = engine_with(&[]).await;
    assert!(
        schemas(&mut s).await
            == vec![
                text_row(&["information_schema", "postgres"]),
                text_row(&["pg_catalog", "postgres"]),
                text_row(&["public", "pg_database_owner"]),
            ]
    );
}

/// The regression this file exists for.
///
/// `public` was both hard-coded into the `pg_namespace` projection *and*
/// creatable, so a `CREATE SCHEMA public` left two rows behind. Both rows
/// claimed oid 2200. Every way of reaching the schema must leave exactly one
/// row.
#[tokio::test]
async fn public_has_exactly_one_pg_namespace_row() {
    struct Case {
        setup: &'static [&'static str],
        why: &'static str,
    }

    let cases = [
        Case {
            setup: &[],
            why: "a fresh database synthesises the row and nothing else adds one",
        },
        Case {
            setup: &["CREATE SCHEMA IF NOT EXISTS public"],
            why: "IF NOT EXISTS sees the synthesised schema and writes nothing",
        },
        Case {
            setup: &["ALTER SCHEMA public OWNER TO postgres"],
            why: "a stored row supersedes the synthesised one instead of joining it",
        },
        Case {
            setup: &["DROP SCHEMA public", "CREATE SCHEMA public"],
            why: "re-creating the schema retires its tombstone",
        },
    ];

    for case in cases {
        let (_engine, mut s) = engine_with(case.setup).await;
        let rows = query(
            &mut s,
            "SELECT count(*) FROM pg_namespace WHERE nspname = 'public'",
        )
        .await;
        assert!(rows == vec![text_row(&["1"])], "{}", case.why);
        let oids = query(
            &mut s,
            "SELECT oid FROM pg_namespace WHERE nspname = 'public'",
        )
        .await;
        assert!(oids == vec![text_row(&["2200"])], "{}", case.why);
    }
}

// ---------------------------------------------------------------------------
// CREATE SCHEMA
// ---------------------------------------------------------------------------

/// The three refusals `CREATE SCHEMA` has, with the SQLSTATE and wording
/// `PostgreSQL` 18.4 reports.
///
/// Note the order that the reserved-prefix cases pin: `pg_catalog` exists, but
/// the unacceptable-name refusal outranks the duplicate one, and
/// `IF NOT EXISTS` does not waive it.
#[tokio::test]
async fn create_schema_errors_match_postgresql() {
    struct Case {
        sql: &'static str,
        expect: Failure,
        why: &'static str,
    }

    let cases = [
        Case {
            sql: "CREATE SCHEMA public",
            expect: Failure::new("42P06", "schema \"public\" already exists"),
            why: "public is a real, pre-existing schema, not an absence",
        },
        Case {
            sql: "CREATE SCHEMA information_schema",
            expect: Failure::new("42P06", "schema \"information_schema\" already exists"),
            why: "a system schema without the reserved prefix is an ordinary duplicate",
        },
        Case {
            sql: "CREATE SCHEMA pg_catalog",
            expect: Failure::new("42939", "unacceptable schema name \"pg_catalog\"")
                .detail(RESERVED_PREFIX_DETAIL),
            why: "the prefix is checked before the name is looked up",
        },
        Case {
            sql: "CREATE SCHEMA pg_anything",
            expect: Failure::new("42939", "unacceptable schema name \"pg_anything\"")
                .detail(RESERVED_PREFIX_DETAIL),
            why: "the prefix is reserved wholesale, not just for the schemas that use it",
        },
        Case {
            sql: "CREATE SCHEMA IF NOT EXISTS pg_anything",
            expect: Failure::new("42939", "unacceptable schema name \"pg_anything\"")
                .detail(RESERVED_PREFIX_DETAIL),
            why: "IF NOT EXISTS waives the duplicate, never the unacceptable name",
        },
    ];

    for case in cases {
        let (_engine, mut s) = engine_with(&[]).await;
        assert!(
            failure_of(&mut s, case.sql).await == case.expect,
            "{}",
            case.why
        );
    }
}

/// A name that only starts like a reserved one is fine, and so is a schema of
/// one's own.
#[tokio::test]
async fn create_schema_accepts_ordinary_names() {
    let (_engine, mut s) = engine_with(&["CREATE SCHEMA pgfoo", "CREATE SCHEMA app"]).await;
    assert!(
        schemas(&mut s).await
            == vec![
                text_row(&["app", "postgres"]),
                text_row(&["information_schema", "postgres"]),
                text_row(&["pg_catalog", "postgres"]),
                text_row(&["pgfoo", "postgres"]),
                text_row(&["public", "pg_database_owner"]),
            ]
    );
}

/// `CREATE SCHEMA IF NOT EXISTS public` succeeds where the bare form is 42P06.
///
/// `PostgreSQL` also emits a NOTICE that this engine does not.
#[tokio::test]
async fn create_schema_if_not_exists_accepts_a_schema_that_is_already_there() {
    let (_engine, mut s) = engine_with(&["CREATE SCHEMA IF NOT EXISTS public"]).await;
    assert!(
        schemas(&mut s).await
            == vec![
                text_row(&["information_schema", "postgres"]),
                text_row(&["pg_catalog", "postgres"]),
                text_row(&["public", "pg_database_owner"]),
            ]
    );
}

// ---------------------------------------------------------------------------
// DROP SCHEMA
// ---------------------------------------------------------------------------

/// What `DROP SCHEMA` refuses.
///
/// The engine refuses the system schemas however a statement reaches them, and
/// `IF EXISTS` does not help, because they do exist.
#[tokio::test]
async fn drop_schema_errors_match_postgresql() {
    struct Case {
        sql: &'static str,
        expect: Failure,
        why: &'static str,
    }

    let cases = [
        Case {
            sql: "DROP SCHEMA pg_catalog",
            expect: Failure::new(
                "2BP01",
                "cannot drop schema pg_catalog because it is required by the database system",
            ),
            why: "pg_catalog is pinned in PostgreSQL and projected here",
        },
        Case {
            sql: "DROP SCHEMA IF EXISTS pg_catalog",
            expect: Failure::new(
                "2BP01",
                "cannot drop schema pg_catalog because it is required by the database system",
            ),
            why: "IF EXISTS waives only a missing schema",
        },
        Case {
            sql: "DROP SCHEMA pg_catalog CASCADE",
            expect: Failure::new(
                "2BP01",
                "cannot drop schema pg_catalog because it is required by the database system",
            ),
            why: "CASCADE waives only the dependency check",
        },
        Case {
            sql: "DROP SCHEMA information_schema",
            expect: Failure::new(
                "2BP01",
                "cannot drop schema information_schema because it is required by the database \
                 system",
            ),
            why: "DIVERGENCE: PostgreSQL refuses this too, but as a dependency on the \
                  information_schema views, which CASCADE would clear; here the schema is a \
                  projection with no contents to cascade to, so the refusal is unconditional \
                  and only the SQLSTATE matches",
        },
        Case {
            sql: "DROP SCHEMA nosuch",
            expect: Failure::new("3F000", "schema \"nosuch\" does not exist"),
            why: "an absent schema is 3F000, not a silent no-op",
        },
    ];

    for case in cases {
        let (_engine, mut s) = engine_with(&[]).await;
        assert!(
            failure_of(&mut s, case.sql).await == case.expect,
            "{}",
            case.why
        );
    }
}

/// `public` is genuinely droppable, and it stays dropped.
///
/// A second drop reports it missing and does not find a schema that this engine
/// bootstraps back.
#[tokio::test]
async fn public_is_droppable_and_stays_dropped() {
    let (_engine, mut s) = engine_with(&["DROP SCHEMA public"]).await;
    assert!(
        schemas(&mut s).await
            == vec![
                text_row(&["information_schema", "postgres"]),
                text_row(&["pg_catalog", "postgres"]),
            ]
    );
    assert!(
        failure_of(&mut s, "DROP SCHEMA public").await
            == Failure::new("3F000", "schema \"public\" does not exist")
    );
    run(&mut s, "DROP SCHEMA IF EXISTS public").await;
}

/// Created again, `public` is an ordinary schema owned by whoever created it.
///
/// `pg_database_owner` owns only the schema that the database was bootstrapped
/// with.
#[tokio::test]
async fn public_can_be_created_again_after_being_dropped() {
    let (_engine, mut s) = engine_with(&["DROP SCHEMA public", "CREATE SCHEMA public"]).await;
    assert!(
        schemas(&mut s).await
            == vec![
                text_row(&["information_schema", "postgres"]),
                text_row(&["pg_catalog", "postgres"]),
                text_row(&["public", "postgres"]),
            ]
    );
}

/// A non-empty `public` needs `CASCADE`, and `CASCADE` takes its relations
/// with it.
///
/// Every other schema is under the same rule.
#[tokio::test]
async fn dropping_a_populated_public_needs_cascade() {
    let (_engine, mut s) = engine_with(&["CREATE TABLE t (id int4)"]).await;
    assert!(
        failure_of(&mut s, "DROP SCHEMA public").await
            == Failure::new(
                "2BP01",
                "cannot drop schema public because other objects depend on it",
            ),
        "DIVERGENCE: PostgreSQL names the dependent relations in DETAIL and hints at CASCADE; \
         this engine reports the bare message"
    );

    run(&mut s, "DROP SCHEMA public CASCADE").await;
    let relations = query(
        &mut s,
        "SELECT count(*) FROM pg_class WHERE relname = 't' AND relkind = 'r'",
    )
    .await;
    assert!(relations == vec![text_row(&["0"])]);
}

// ---------------------------------------------------------------------------
// ALTER SCHEMA
// ---------------------------------------------------------------------------

/// Re-owning a bootstrap schema stores a row over the synthesised one.
///
/// The owner still projects as the bootstrap superuser, because this engine has
/// no ownership model. But a new row does not duplicate the schema.
#[tokio::test]
async fn re_owning_a_bootstrap_schema_replaces_its_row() {
    let (_engine, mut s) = engine_with(&[
        "CREATE ROLE app_owner",
        "ALTER SCHEMA public OWNER TO app_owner",
        "ALTER SCHEMA pg_catalog OWNER TO app_owner",
    ])
    .await;
    assert!(
        schemas(&mut s).await
            == vec![
                text_row(&["information_schema", "postgres"]),
                text_row(&["pg_catalog", "postgres"]),
                text_row(&["public", "postgres"]),
            ]
    );
}

/// Every relation the schema holds answers to the new name and to nothing else.
///
/// A relation's catalog key carries its schema, so the rename is a move of the
/// whole subtree: the table, the sequence behind its `SERIAL`, both of its
/// indexes and the view over it. The `SERIAL` default is the piece that a key
/// move alone would leave behind — it names its sequence as text.
#[tokio::test]
async fn renaming_a_schema_moves_every_relation_it_holds() {
    let (_engine, mut s) = engine_with(&[
        "CREATE SCHEMA before",
        "SET search_path = before",
        "CREATE TABLE abc (a serial PRIMARY KEY, b int UNIQUE)",
        "CREATE VIEW abc_view AS SELECT a + 1 AS a FROM abc",
        "COMMENT ON TABLE abc IS 'the table'",
        "INSERT INTO abc DEFAULT VALUES",
        "RESET search_path",
        "ALTER SCHEMA before RENAME TO after",
    ])
    .await;

    assert!(
        query(
            &mut s,
            "SELECT n.nspname, c.relname, c.relkind FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname IN ('before', 'after') ORDER BY c.relname",
        )
        .await
            == vec![
                text_row(&["after", "abc", "r"]),
                text_row(&["after", "abc_a_seq", "S"]),
                text_row(&["after", "abc_b_key", "i"]),
                text_row(&["after", "abc_pkey", "i"]),
                text_row(&["after", "abc_view", "v"]),
            ]
    );
    // The sequence still feeds the column it was created for, under its new
    // name, and it carries on from where it was rather than restarting.
    run(&mut s, "INSERT INTO after.abc DEFAULT VALUES").await;
    assert!(
        query(&mut s, "SELECT a FROM after.abc_view ORDER BY a").await
            == vec![text_row(&["2"]), text_row(&["3"])]
    );
    assert!(
        query(
            &mut s,
            "SELECT column_default FROM information_schema.columns \
             WHERE table_schema = 'after' AND table_name = 'abc' AND column_name = 'a'",
        )
        .await
            == vec![text_row(&["nextval('after.abc_a_seq'::regclass)"])]
    );
    assert!(
        query(&mut s, "SELECT obj_description('after.abc'::regclass)").await
            == vec![text_row(&["the table"])]
    );
    assert!(
        query(
            &mut s,
            "SELECT nspname FROM pg_namespace WHERE nspname IN ('before', 'after')",
        )
        .await
            == vec![text_row(&["after"])]
    );
    // `PostgreSQL` reports the missing *schema* here (3F000). This engine
    // reports the relation, for every missing schema and not only a renamed
    // one; the assertion pins what it does today.
    assert!(
        failure_of(&mut s, "SELECT * FROM before.abc").await
            == Failure::new("42P01", "relation \"before.abc\" does not exist")
    );
}

/// A link between two relations of the schema survives, from both ends.
///
/// Each end of a foreign key and of an inheritance link is stored beside the
/// relation that holds it, so the two moves have to be built one after the
/// other over the batch so far. Built independently from the catalog as it was,
/// the second relation's move rebuilds its end from the state the first
/// relation's move already replaced, and the link ends up naming a schema that
/// no longer exists.
#[tokio::test]
async fn renaming_a_schema_keeps_the_links_between_its_own_relations() {
    let (_engine, mut s) = engine_with(&[
        "CREATE SCHEMA before",
        "SET search_path = before",
        "CREATE TABLE referenced (a int PRIMARY KEY)",
        "CREATE TABLE referencing (x int REFERENCES referenced (a))",
        "CREATE TABLE super_t (i int)",
        "CREATE TABLE sub_t () INHERITS (super_t)",
        "INSERT INTO referenced VALUES (1)",
        "INSERT INTO sub_t VALUES (2)",
        "RESET search_path",
        "ALTER SCHEMA before RENAME TO after",
    ])
    .await;

    run(&mut s, "INSERT INTO after.referencing VALUES (1)").await;
    assert!(
        failure_of(&mut s, "INSERT INTO after.referencing VALUES (99)")
            .await
            .code
            == "23503"
    );
    assert!(
        query(
            &mut s,
            "SELECT inhparent::regclass::text, inhrelid::regclass::text FROM pg_inherits",
        )
        .await
            == vec![text_row(&["after.super_t", "after.sub_t"])]
    );
    assert!(query(&mut s, "SELECT i FROM after.super_t").await == vec![text_row(&["2"])]);
}

/// The rename is checked in `RenameSchema`'s order, and refuses what it cannot
/// move rather than stranding it.
#[tokio::test]
async fn a_schema_rename_reports_what_it_refuses() {
    struct Case {
        setup: &'static [&'static str],
        sql: &'static str,
        expect: Failure,
        why: &'static str,
    }

    let cases = [
        Case {
            setup: &[],
            sql: "ALTER SCHEMA nowhere RENAME TO somewhere",
            expect: Failure::new("3F000", "schema \"nowhere\" does not exist"),
            why: "the schema under rename is looked up first",
        },
        Case {
            setup: &["CREATE SCHEMA one", "CREATE SCHEMA two"],
            sql: "ALTER SCHEMA one RENAME TO two",
            expect: Failure::new("42P06", "schema \"two\" already exists"),
            why: "the collision outranks the reserved prefix, as RenameSchema orders them",
        },
        Case {
            setup: &["CREATE SCHEMA one"],
            sql: "ALTER SCHEMA one RENAME TO pg_anything",
            expect: Failure::new("42939", "unacceptable schema name \"pg_anything\"")
                .detail(RESERVED_PREFIX_DETAIL),
            why: "the prefix is held against the new name",
        },
        Case {
            setup: &[],
            sql: "ALTER SCHEMA pg_catalog RENAME TO ordinary",
            expect: Failure::new(
                "2BP01",
                "cannot rename schema pg_catalog because it is required by the database system",
            ),
            why: "a bootstrap schema is synthesised by name, so renaming it hides its contents",
        },
        Case {
            setup: &[
                "CREATE SCHEMA one",
                "CREATE TYPE one.pair AS (a int, b int)",
            ],
            sql: "ALTER SCHEMA one RENAME TO two",
            expect: Failure::new(
                "0A000",
                "cannot rename schema one: it contains a user-defined type, which this catalog \
                 cannot move to another schema",
            ),
            why: "a type is reachable from oids held elsewhere, so its key alone cannot move",
        },
        Case {
            setup: &[
                "CREATE SCHEMA one",
                "CREATE TABLE one.t (a int)",
                "CREATE VIEW public.v AS SELECT a FROM one.t",
            ],
            sql: "ALTER SCHEMA one RENAME TO two",
            expect: Failure::new(
                "0A000",
                "cannot rename schema one: the definition of v spells it out, and this catalog \
                 stores a definition as SQL text rather than as a dependency it could repoint",
            ),
            why: "a written qualifier in stored view text would go on naming the old schema",
        },
    ];

    for case in cases {
        let (_engine, mut s) = engine_with(case.setup).await;
        assert!(
            failure_of(&mut s, case.sql).await == case.expect,
            "{}",
            case.why
        );
    }
}

// ---------------------------------------------------------------------------
// What a create skips, and what a drop takes with it
// ---------------------------------------------------------------------------

/// `IF NOT EXISTS` says what it stepped over.
///
/// The statement succeeds either way, so the notice is the only thing that
/// distinguishes a create from a skip. A relation of any kind is a `relation`
/// in this message, and a schema is a `schema`.
#[tokio::test]
async fn if_not_exists_reports_the_object_it_skipped() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    let mut notices = s.take_notices().expect("notice receiver");
    for sql in [
        "CREATE SCHEMA app",
        "CREATE TABLE app.t (i int)",
        "CREATE INDEX t_i_idx ON app.t (i)",
        "CREATE SEQUENCE app.q",
    ] {
        run(&mut s, sql).await;
    }
    assert!(notices.try_recv().is_err());

    for sql in [
        "CREATE SCHEMA IF NOT EXISTS app",
        "CREATE TABLE IF NOT EXISTS app.t (i int)",
        "CREATE INDEX IF NOT EXISTS t_i_idx ON app.t (i)",
        "CREATE SEQUENCE IF NOT EXISTS app.q",
    ] {
        run(&mut s, sql).await;
    }
    let reported: Vec<String> = std::iter::from_fn(|| notices.try_recv().ok())
        .map(|notice| notice.message)
        .collect();
    assert!(
        reported
            == vec![
                "schema \"app\" already exists, skipping".to_string(),
                "relation \"t\" already exists, skipping".to_string(),
                "relation \"t_i_idx\" already exists, skipping".to_string(),
                "relation \"q\" already exists, skipping".to_string(),
            ]
    );
}

/// A `CASCADE` names the schema's own objects, but not a `SERIAL`'s sequence.
///
/// Upstream records that sequence as an *internal* dependency of the column,
/// and `reportDependentObjects` never names one of those. A sequence created in
/// its own right has no such link and is reported like any other relation.
#[tokio::test]
async fn dropping_a_schema_does_not_name_the_sequence_behind_a_serial() {
    let (engine, mut s) = engine_with(&[]).await;
    drop(engine);
    let mut notices = s.take_notices().expect("notice receiver");
    for sql in [
        "CREATE SCHEMA app",
        "CREATE TABLE app.t (a serial, b int)",
        "CREATE SEQUENCE app.standalone",
        "DROP SCHEMA app CASCADE",
    ] {
        run(&mut s, sql).await;
    }
    let reported: Vec<(String, Option<String>)> = std::iter::from_fn(|| notices.try_recv().ok())
        .map(|notice| {
            (
                notice.message,
                notice.diagnostics.unwrap_or_default().detail,
            )
        })
        .collect();
    assert!(
        reported
            == vec![(
                "drop cascades to 2 other objects".to_string(),
                Some(
                    "drop cascades to table app.t\ndrop cascades to sequence app.standalone"
                        .to_string()
                )
            )]
    );
}

// ---------------------------------------------------------------------------
// information_schema.schemata
// ---------------------------------------------------------------------------

/// The standard view is a projection of `pg_namespace`, not a fixed list.
///
/// A created schema shows up in it, and a dropped `public` leaves it. Both
/// views name the same owner for each schema, because `PostgreSQL` builds
/// `schemata` with a join from `nspowner` to `pg_authid`.
#[tokio::test]
async fn information_schema_schemata_tracks_pg_namespace() {
    let (_engine, mut s) = engine_with(&[]).await;
    assert!(
        standard_schemas(&mut s).await
            == vec![
                text_row(&["information_schema", "postgres"]),
                text_row(&["pg_catalog", "postgres"]),
                text_row(&["public", "pg_database_owner"]),
            ]
    );

    run(&mut s, "CREATE SCHEMA app").await;
    run(&mut s, "DROP SCHEMA public").await;
    assert!(
        standard_schemas(&mut s).await
            == vec![
                text_row(&["app", "postgres"]),
                text_row(&["information_schema", "postgres"]),
                text_row(&["pg_catalog", "postgres"]),
            ]
    );
    assert!(standard_schemas(&mut s).await == schemas(&mut s).await);
}

/// The full column list `PostgreSQL` 18.4 projects, in its order.
///
/// The three `default_character_set_*` columns and `sql_path` are NULL there
/// too. The standard defines them, and `PostgreSQL` fills none of them.
#[tokio::test]
async fn information_schema_schemata_projects_the_standard_columns() {
    let (_engine, mut s) = engine_with(&[]).await;
    let row = query(
        &mut s,
        "SELECT catalog_name, schema_name, schema_owner, default_character_set_catalog, \
         default_character_set_schema, default_character_set_name, sql_path \
         FROM information_schema.schemata WHERE schema_name = 'public'",
    )
    .await;
    assert!(
        row == vec![vec![
            // The catalog name is the current database, as `current_database()`
            // reports it.
            Some("postgres".to_string()),
            Some("public".to_string()),
            Some("pg_database_owner".to_string()),
            None,
            None,
            None,
            None,
        ]]
    );
}
