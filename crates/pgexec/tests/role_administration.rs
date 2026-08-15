//! Who may write the role catalog, end to end.
//!
//! Every other privilege test in this crate asks whether a role reaches a
//! relation. These ask the question underneath it: whether a role can change
//! the answer. `CREATE ROLE`, `ALTER ROLE`, `DROP ROLE` and `GRANT <role> TO`
//! all wrote the catalog with no check at all, so an ordinary login had two
//! one-statement routes past the whole of `privilege.rs`:
//!
//! * `ALTER ROLE me SUPERUSER`, after which `role_is_superuser` says yes and
//!   every privilege and every policy is bypassed; the same statement reaches
//!   `BYPASSRLS` and `CREATEROLE` too, and on any other role.
//! * `GRANT <owning role> TO me`, after which `role_has_privs_of` says the role
//!   owns the tables that role owns — which is both a privilege bypass and a
//!   row-security bypass, because ownership passes both.
//!
//! [`an_ordinary_role_cannot_make_itself_a_superuser`] and
//! [`an_ordinary_role_cannot_grant_itself_into_an_owning_role`] are those two
//! routes, written as the escalation rather than as the refusal: each one takes
//! the reach it was after and asserts the role still does not have it.
//!
//! The cases that keep the gate honest are the other kind. `pg_regress` creates
//! almost everything as the bootstrap superuser, so the corpus cannot catch a
//! rule that is too tight; [`the_bootstrap_session_administers_roles_freely`]
//! and [`createrole_creates_the_roles_postgresql_lets_it_create`] are the
//! negative space, and every arm of them was checked against `postgres:18.4`.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"))
}

fn cell_text(cell: Option<&Cell>) -> String {
    cell.map_or_else(
        || "NULL".to_string(),
        |cell| String::from_utf8(cell.text.to_vec()).expect("utf8"),
    )
}

async fn query(session: &mut SqlSession, sql: &str) -> Vec<String> {
    match &run(session, sql).await[0] {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| cell_text(cell.as_ref()))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect(),
        other => panic!("expected rows from {sql}, got {other:?}"),
    }
}

/// The whole refusal a statement raised: SQLSTATE, message and `DETAIL`.
async fn refusal(session: &mut SqlSession, sql: &str) -> (String, String, Option<String>) {
    let error = session
        .simple_query(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} should have failed"));
    let detail = error
        .diagnostics
        .as_ref()
        .and_then(|fields| fields.detail.clone());
    (error.code.clone(), error.message.clone(), detail)
}

fn denied(message: &str, detail: &str) -> (String, String, Option<String>) {
    (
        "42501".to_string(),
        message.to_string(),
        Some(detail.to_string()),
    )
}

fn rows(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

/// A table `alice` owns and has granted nobody, plus the roles the cases need.
///
/// `secret` is the reach every escalation below is aiming at: `mallory` holds
/// no grant on it, and `alice` never grants one, so any statement that lets
/// `mallory` read it did so by changing what `mallory` is.
const SETUP: &str = r"
CREATE ROLE alice;
CREATE ROLE mallory;
CREATE ROLE creator WITH CREATEROLE;
CREATE TABLE secret (id int4, body text);
INSERT INTO secret VALUES (1, 'classified');
ALTER TABLE secret OWNER TO alice;
";

/// The bootstrap engine with [`SETUP`] applied, and a session acting as
/// `mallory`, who owns nothing and holds nothing.
async fn engine_and_mallory() -> (SqlEngine, SqlSession, SqlSession) {
    let engine = SqlEngine::new();
    let mut bootstrap = engine.connect();
    run(&mut bootstrap, SETUP).await;
    let mut mallory = engine.connect();
    run(&mut mallory, "SET SESSION AUTHORIZATION mallory").await;
    (engine, bootstrap, mallory)
}

/// A session acting as `role`.
async fn as_role(engine: &SqlEngine, role: &str) -> SqlSession {
    let mut session = engine.connect();
    run(&mut session, &format!("SET SESSION AUTHORIZATION {role}")).await;
    session
}

// ------------------------------------------------------------ the escalations

/// `ALTER ROLE mallory SUPERUSER` is the shortest route past every other check
/// in the engine, so it is refused, and `mallory` still cannot read the table.
#[tokio::test]
async fn an_ordinary_role_cannot_make_itself_a_superuser() {
    let (_engine, _bootstrap, mut mallory) = engine_and_mallory().await;

    assert!(
        refusal(&mut mallory, "ALTER ROLE mallory SUPERUSER").await
            == denied(
                "permission denied to alter role",
                "Only roles with the SUPERUSER attribute may change the SUPERUSER attribute.",
            )
    );
    // The reach the statement was for.
    assert!(
        mallory
            .simple_query("SELECT body FROM secret")
            .await
            .is_err()
    );
    assert!(
        query(
            &mut mallory,
            "SELECT rolsuper FROM pg_roles WHERE rolname = 'mallory'"
        )
        .await
            == rows(&["f"])
    );
}

/// `GRANT alice TO mallory` makes `mallory` an owner of everything `alice`
/// owns, because a membership *is* ownership for both the privilege gate and
/// the row-security exemption. So it is refused, and the reach does not follow.
#[tokio::test]
async fn an_ordinary_role_cannot_grant_itself_into_an_owning_role() {
    let (engine, mut bootstrap, mut mallory) = engine_and_mallory().await;
    run(
        &mut bootstrap,
        "CREATE POLICY nobody ON secret USING (false);
         ALTER TABLE secret ENABLE ROW LEVEL SECURITY",
    )
    .await;

    assert!(
        refusal(&mut mallory, "GRANT alice TO mallory").await
            == denied(
                "permission denied to grant role \"alice\"",
                "Only roles with the ADMIN option on role \"alice\" may grant this role.",
            )
    );
    assert!(
        mallory
            .simple_query("SELECT body FROM secret")
            .await
            .is_err()
    );

    // `CREATE ROLE … IN ROLE` writes the same membership, so it meets the same
    // gate. Without that, the refusal above is one statement wide. `creator`
    // rather than `mallory` because `mallory` is stopped by the `CREATE ROLE`
    // gate before the membership is looked at — which is `PostgreSQL`'s order
    // too.
    let mut creator = as_role(&engine, "creator").await;
    assert!(
        refusal(&mut creator, "CREATE ROLE understudy IN ROLE alice").await
            == denied(
                "permission denied to grant role \"alice\"",
                "Only roles with the ADMIN option on role \"alice\" may grant this role.",
            )
    );
    assert!(
        query(
            &mut bootstrap,
            "SELECT count(*) FROM pg_roles WHERE rolname = 'understudy'"
        )
        .await
            == rows(&["0"])
    );

    // And taking a membership away is gated the same way, with its own verb.
    assert!(
        refusal(&mut mallory, "REVOKE alice FROM mallory").await
            == denied(
                "permission denied to revoke role \"alice\"",
                "Only roles with the ADMIN option on role \"alice\" may revoke this role.",
            )
    );
}

/// `BYPASSRLS` and `CREATEROLE` reach the same statement, and neither is a
/// consolation prize: the first sees past every policy, the second is the
/// attribute role administration is built on.
#[tokio::test]
async fn the_other_attributes_on_the_same_statement_are_gated_too() {
    let (_engine, mut bootstrap, mut mallory) = engine_and_mallory().await;
    run(
        &mut bootstrap,
        "GRANT SELECT ON secret TO mallory;
         CREATE POLICY nobody ON secret USING (false);
         ALTER TABLE secret ENABLE ROW LEVEL SECURITY",
    )
    .await;
    assert!(query(&mut mallory, "SELECT count(*) FROM secret").await == rows(&["0"]));

    let base = "Only roles with the CREATEROLE attribute and the ADMIN option on \
                role \"mallory\" may alter this role.";
    for statement in [
        "ALTER ROLE mallory BYPASSRLS",
        "ALTER ROLE mallory CREATEROLE",
        "ALTER ROLE mallory CREATEDB",
        "ALTER ROLE mallory REPLICATION",
        "ALTER ROLE mallory NOINHERIT",
        "ALTER ROLE mallory NOLOGIN",
    ] {
        assert!(
            refusal(&mut mallory, statement).await
                == denied("permission denied to alter role", base),
            "{statement}"
        );
    }
    // The policy still hides every row, which is what BYPASSRLS was for.
    assert!(query(&mut mallory, "SELECT count(*) FROM secret").await == rows(&["0"]));

    // Clearing an attribute is changing it: the gate reads whether the option
    // was written, not what it was written with.
    assert!(
        refusal(&mut mallory, "ALTER ROLE mallory NOSUPERUSER").await
            == denied(
                "permission denied to alter role",
                "Only roles with the SUPERUSER attribute may change the SUPERUSER attribute.",
            )
    );
}

/// Creating and dropping roles are gated, and a role that cannot be created
/// cannot be logged in as either.
#[tokio::test]
async fn creating_and_dropping_roles_are_gated() {
    let (_engine, _bootstrap, mut mallory) = engine_and_mallory().await;

    assert!(
        refusal(&mut mallory, "CREATE ROLE understudy").await
            == denied(
                "permission denied to create role",
                "Only roles with the CREATEROLE attribute may create roles.",
            )
    );
    assert!(
        refusal(&mut mallory, "DROP ROLE alice").await
            == denied(
                "permission denied to drop role",
                "Only roles with the CREATEROLE attribute and the ADMIN option on the target roles may drop roles.",
            )
    );
    assert!(
        query(
            &mut mallory,
            "SELECT count(*) FROM pg_roles WHERE rolname = 'alice'"
        )
        .await
            == rows(&["1"])
    );
}

// ----------------------------------------------------------- the negative space

/// The bootstrap session is the superuser, and everything above still works for
/// it. This is the case the upstream corpus exercises on nearly every line, so
/// a rule tight enough to break it would break the whole schedule.
#[tokio::test]
async fn the_bootstrap_session_administers_roles_freely() {
    let engine = SqlEngine::new();
    let mut bootstrap = engine.connect();
    run(&mut bootstrap, SETUP).await;
    run(
        &mut bootstrap,
        "CREATE ROLE understudy WITH SUPERUSER CREATEDB BYPASSRLS REPLICATION LOGIN;
         CREATE ROLE deputy IN ROLE alice;
         ALTER ROLE understudy NOSUPERUSER;
         ALTER ROLE mallory BYPASSRLS;
         GRANT alice TO mallory;
         REVOKE alice FROM mallory;
         DROP ROLE understudy;
         DROP ROLE deputy",
    )
    .await;
    assert!(
        query(
            &mut bootstrap,
            "SELECT count(*) FROM pg_roles WHERE rolname IN ('understudy', 'deputy')"
        )
        .await
            == rows(&["0"])
    );
}

/// A `CREATEROLE` role creates exactly the roles `PostgreSQL` lets it create:
/// ordinary ones and `CREATEROLE` ones, and none carrying an attribute it does
/// not itself hold. Asking for an attribute as false is not asking for it.
#[tokio::test]
async fn createrole_creates_the_roles_postgresql_lets_it_create() {
    let (engine, _bootstrap, _mallory) = engine_and_mallory().await;
    let mut creator = as_role(&engine, "creator").await;

    run(&mut creator, "CREATE ROLE ordinary").await;
    run(&mut creator, "CREATE ROLE another WITH CREATEROLE").await;
    run(
        &mut creator,
        "CREATE ROLE plainly WITH NOSUPERUSER NOCREATEDB",
    )
    .await;

    for (statement, attribute) in [
        ("CREATE ROLE elevated WITH SUPERUSER", "SUPERUSER"),
        ("CREATE ROLE seer WITH BYPASSRLS", "BYPASSRLS"),
        ("CREATE ROLE maker WITH CREATEDB", "CREATEDB"),
        ("CREATE ROLE streamer WITH REPLICATION", "REPLICATION"),
    ] {
        assert!(
            refusal(&mut creator, statement).await
                == denied(
                    "permission denied to create role",
                    &format!(
                        "Only roles with the {attribute} attribute may create roles with the {attribute} attribute."
                    ),
                ),
            "{statement}"
        );
    }

    // A CREATEROLE role holding CREATEDB may pass CREATEDB on.
    let mut bootstrap = engine.connect();
    run(&mut bootstrap, "ALTER ROLE creator CREATEDB").await;
    run(&mut creator, "CREATE ROLE maker WITH CREATEDB").await;
}

/// An unknown role is reported as unknown even by a session that could not have
/// altered it, which is the order `PostgreSQL` reports the two in.
#[tokio::test]
async fn an_unknown_role_is_named_before_the_refusal() {
    let (_engine, _bootstrap, mut mallory) = engine_and_mallory().await;

    let (sqlstate, _, _) = refusal(&mut mallory, "ALTER ROLE nobody_here SUPERUSER").await;
    assert!(sqlstate == "42704");
    let (sqlstate, _, _) = refusal(&mut mallory, "GRANT nobody_here TO mallory").await;
    assert!(sqlstate == "42704");
}
