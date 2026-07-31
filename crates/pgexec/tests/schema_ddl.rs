//! Schema DDL against a real in-process engine: which schemas a database has
//! before anything is created in it, which of those may be created or dropped,
//! and what `pg_namespace` projects for each.
//!
//! Every SQLSTATE, message and `DETAIL` asserted here was captured from a live
//! `PostgreSQL` 18.4 server rather than from documentation. Where this engine
//! knowingly diverges the test pins the *current* behaviour and says so at the
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

/// A failed statement, as everything `PostgreSQL` puts on the wire for it —
/// compared as one value so a case states its whole expected error rather than
/// a chain of field assertions.
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
        Self {
            code: error.code,
            message: error.message,
            detail: error.detail,
            hint: error.hint,
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

/// A database that has never run a `CREATE SCHEMA` still has three schemas, and
/// `public` is owned by `pg_database_owner` rather than by the bootstrap
/// superuser — the one place `PostgreSQL`'s ownership differs across them.
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

/// The regression this file exists for: `public` was both hard-coded into the
/// `pg_namespace` projection *and* creatable, so a `CREATE SCHEMA public` left
/// two rows behind — both claiming oid 2200. Every way of reaching the schema
/// has to leave exactly one row.
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
/// `PostgreSQL` 18.4 reports. Note the ordering the reserved-prefix cases pin:
/// `pg_catalog` exists, yet the unacceptable-name refusal outranks the
/// duplicate one, and `IF NOT EXISTS` does not waive it.
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

/// What `DROP SCHEMA` refuses. The system schemas are refused however they are
/// reached, and `IF EXISTS` does not help, because they do exist.
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

/// `public` is genuinely droppable, and stays dropped: a second drop reports it
/// missing rather than finding a schema this engine bootstraps back.
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

/// Created again, `public` is an ordinary schema owned by whoever created it —
/// `pg_database_owner` owns only the one the database was bootstrapped with.
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

/// A non-empty `public` needs `CASCADE`, and `CASCADE` takes its relations with
/// it — the same rule every other schema is under.
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

/// Re-owning a bootstrap schema stores a row over the synthesised one. The
/// owner still projects as the bootstrap superuser, because this engine has no
/// ownership model — but the schema is not duplicated by acquiring a row.
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

// ---------------------------------------------------------------------------
// information_schema.schemata
// ---------------------------------------------------------------------------

/// The standard view is a projection of `pg_namespace`, not a fixed list: a
/// created schema shows up in it and a dropped `public` leaves it. Both views
/// name the same owner for each schema, because `PostgreSQL` builds `schemata`
/// by joining `nspowner` to `pg_authid`.
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

/// The full column list `PostgreSQL` 18.4 projects, in its order. The three
/// `default_character_set_*` columns and `sql_path` are NULL there too — the
/// standard defines them and `PostgreSQL` fills none of them.
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
