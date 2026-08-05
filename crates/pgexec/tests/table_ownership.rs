//! A relation records the role that created it, `ALTER TABLE … OWNER TO`
//! rewrites that role, and the catalog projects it — `pg_tables.tableowner`,
//! `pg_class.relowner` and the `pg_get_userbyid` the `\dt` Owner column reads.
//!
//! Ownership is the first thing row-level security needs: the owner bypasses
//! its own policies unless the relation is `FORCE ROW LEVEL SECURITY`, and
//! policy DDL is owner-only. A constant owner would make both of those
//! meaningless.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .expect("statement should succeed")
}

fn cell_text(cell: Option<&Cell>) -> Option<String> {
    cell.map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
}

async fn query(session: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    match &run(session, sql).await[0] {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| row.iter().map(|c| cell_text(c.as_ref())).collect())
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

async fn scalar(session: &mut SqlSession, sql: &str) -> String {
    let rows = query(session, sql).await;
    let [row] = rows.as_slice() else {
        panic!("expected exactly one row, got {rows:?}");
    };
    let [cell] = row.as_slice() else {
        panic!("expected exactly one column, got {row:?}");
    };
    cell.clone().expect("owner is never NULL")
}

/// Everything the catalog says about who a relation belongs to, gathered in one
/// value so a case states its whole expectation rather than a chain of
/// per-column assertions.
#[derive(Debug, PartialEq, Eq)]
struct Ownership {
    /// `pg_tables.tableowner`.
    table_owner: String,
    /// `pg_get_userbyid(pg_class.relowner)` — what `\dt`'s Owner column shows.
    class_owner: String,
}

async fn ownership_of(session: &mut SqlSession, relation: &str) -> Ownership {
    Ownership {
        table_owner: scalar(
            session,
            &format!("SELECT tableowner FROM pg_tables WHERE tablename = '{relation}'"),
        )
        .await,
        class_owner: scalar(
            session,
            &format!("SELECT pg_get_userbyid(relowner) FROM pg_class WHERE relname = '{relation}'"),
        )
        .await,
    }
}

fn owned_by(role: &str) -> Ownership {
    Ownership {
        table_owner: role.to_string(),
        class_owner: role.to_string(),
    }
}

/// A session that has authenticated as nobody carries the pseudo-role `public`,
/// which cannot own anything; its relations belong to the bootstrap superuser.
#[tokio::test]
async fn a_relation_created_without_an_authenticated_role_belongs_to_the_bootstrap_superuser() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE unowned (id int4)").await;

    assert!(ownership_of(&mut session, "unowned").await == owned_by("postgres"));
}

/// Every path that creates a stored relation records the creating role, not
/// just the plain `CREATE TABLE` spelling.
#[tokio::test]
async fn every_creation_path_records_the_creating_role() {
    let cases = [
        ("CREATE TABLE plain (id int4)", "plain"),
        ("CREATE TABLE ctas AS SELECT 1 AS id", "ctas"),
        ("SELECT 1 AS id INTO selected", "selected"),
        (
            "CREATE TABLE parent (id int4) PARTITION BY RANGE (id)",
            "parent",
        ),
        (
            "CREATE TABLE child PARTITION OF parent FOR VALUES FROM (0) TO (10)",
            "child",
        ),
        ("CREATE TABLE heir (extra int4) INHERITS (plain)", "heir"),
    ];

    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE ROLE creator LOGIN").await;
    run(&mut session, "SET SESSION AUTHORIZATION creator").await;

    for (sql, relation) in cases {
        run(&mut session, sql).await;
        assert!(ownership_of(&mut session, relation).await == owned_by("creator"));
    }
}

/// A temporary relation lives in a namespace of its own but is owned like any
/// other.
#[tokio::test]
async fn a_temporary_relation_belongs_to_the_session_that_created_it() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE ROLE ephemeral LOGIN").await;
    run(&mut session, "SET SESSION AUTHORIZATION ephemeral").await;
    run(&mut session, "CREATE TEMP TABLE scratch (id int4)").await;

    assert!(ownership_of(&mut session, "scratch").await == owned_by("ephemeral"));
}

/// A foreign table is a `pg_class` relation too, and `pg_tables` deliberately
/// excludes it — so only the `pg_class` side is asserted here.
#[tokio::test]
async fn a_foreign_table_belongs_to_the_role_that_created_it() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE SERVER srv FOREIGN DATA WRAPPER kafka_fdw",
    )
    .await;
    run(&mut session, "CREATE ROLE importer LOGIN").await;
    run(&mut session, "SET SESSION AUTHORIZATION importer").await;
    run(
        &mut session,
        "CREATE FOREIGN TABLE remote (v text) SERVER srv OPTIONS (topic 'remote')",
    )
    .await;

    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE relname = 'remote'",
        )
        .await
            == "importer"
    );
}

/// `ALTER TABLE … OWNER TO` is what makes ownership a rewritable fact rather
/// than a creation-time constant.
#[tokio::test]
async fn owner_to_rewrites_the_recorded_owner() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE ROLE first LOGIN; CREATE ROLE second").await;
    run(&mut session, "SET SESSION AUTHORIZATION first").await;
    run(&mut session, "CREATE TABLE handover (id int4)").await;
    assert!(ownership_of(&mut session, "handover").await == owned_by("first"));

    run(&mut session, "ALTER TABLE handover OWNER TO second").await;

    assert!(ownership_of(&mut session, "handover").await == owned_by("second"));
}

/// The new owner survives the rest of the statement's subcommands and a rename,
/// because the schema record is written once from the working table.
#[tokio::test]
async fn owner_to_survives_the_rest_of_the_statement_and_a_later_rename() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE ROLE keeper").await;
    run(&mut session, "CREATE TABLE movable (id int4)").await;

    run(
        &mut session,
        "ALTER TABLE movable ADD COLUMN label text, OWNER TO keeper",
    )
    .await;
    assert!(ownership_of(&mut session, "movable").await == owned_by("keeper"));

    run(&mut session, "ALTER TABLE movable RENAME TO relocated").await;
    assert!(ownership_of(&mut session, "relocated").await == owned_by("keeper"));
}

/// `CURRENT_USER` and `USER` in an owner position name the session's role.
#[tokio::test]
async fn owner_to_current_user_names_the_session_role() {
    for spelling in ["CURRENT_USER", "USER"] {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        run(&mut session, "CREATE ROLE claimant LOGIN").await;
        run(&mut session, "CREATE TABLE claimed (id int4)").await;
        run(&mut session, "SET SESSION AUTHORIZATION claimant").await;

        run(
            &mut session,
            &format!("ALTER TABLE claimed OWNER TO {spelling}"),
        )
        .await;

        assert!(ownership_of(&mut session, "claimed").await == owned_by("claimant"));
    }
}

/// A session that never authenticated may still name itself as an owner.
///
/// Its effective role is the bootstrap role, which has no `pg_authid` row — so
/// an existence check that consults only stored records refuses the handover
/// while the relation is already owned by exactly that role. The upstream
/// `vacuum` test does this (`ALTER TABLE vacowned_parted OWNER TO
/// CURRENT_USER` after a `RESET ROLE`) and caught it.
#[tokio::test]
async fn owner_to_current_user_works_for_a_session_that_never_authenticated() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE TABLE unclaimed (id int4)").await;
    let before = ownership_of(&mut session, "unclaimed").await;

    run(&mut session, "ALTER TABLE unclaimed OWNER TO CURRENT_USER").await;

    assert!(ownership_of(&mut session, "unclaimed").await == before);
}

/// Handing a relation to a name no role holds is 42704 and leaves the owner
/// where it was. `PUBLIC` counts as such a name: it is a pseudo-role with no
/// `pg_authid` row, so a relation must never come to rest on it.
#[tokio::test]
async fn owner_to_a_name_no_role_holds_is_undefined_object_and_changes_nothing() {
    for role in ["nobody", "public"] {
        let engine = SqlEngine::new();
        let mut session = engine.connect();
        run(&mut session, "CREATE TABLE stable (id int4)").await;

        let error = session
            .simple_query(&format!("ALTER TABLE stable OWNER TO {role}"))
            .await
            .expect_err("a name no role holds is refused");

        assert!(
            (error.code.as_str(), error.message.as_str())
                == ("42704", format!("role \"{role}\" does not exist").as_str())
        );
        assert!(ownership_of(&mut session, "stable").await == owned_by("postgres"));
    }
}

/// `relowner` is the owning role's real `pg_authid.oid`, not a constant that
/// `pg_get_userbyid` happens to render as the same name. A join against
/// `pg_authid` is what a client — and upstream's `dependency` test — does.
#[tokio::test]
async fn relowner_is_the_pg_authid_oid_of_the_owning_role() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE ROLE holder LOGIN").await;
    run(&mut session, "SET SESSION AUTHORIZATION holder").await;
    run(&mut session, "CREATE TABLE held (id int4)").await;

    assert!(
        scalar(
            &mut session,
            "SELECT a.rolname FROM pg_class c JOIN pg_authid a ON a.oid = c.relowner \
             WHERE c.relname = 'held'",
        )
        .await
            == "holder"
    );
}

/// An index belongs to whoever owns the table it indexes — `\di`'s Owner column
/// reads `pg_class.relowner` for the index row, not for the table's.
#[tokio::test]
async fn an_index_belongs_to_the_owner_of_the_table_it_indexes() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, "CREATE ROLE indexer LOGIN").await;
    run(&mut session, "SET SESSION AUTHORIZATION indexer").await;
    run(&mut session, "CREATE TABLE indexed (id int4)").await;
    run(&mut session, "CREATE INDEX indexed_id ON indexed (id)").await;

    assert!(
        scalar(
            &mut session,
            "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE relname = 'indexed_id'",
        )
        .await
            == "indexer"
    );
}
