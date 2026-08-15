//! `GRANT <role> TO <role>` and `REVOKE <role> FROM <role>`.
//!
//! Role membership already had one spelling — `CREATE ROLE … IN ROLE` — and one
//! reader, the row-security policy `TO` list. These tests pin that the second
//! spelling writes what the first writes: a membership made either way has to
//! widen the same policies, or `GRANT` would look like it worked while every
//! policy kept ignoring it.

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

async fn error_of(session: &mut SqlSession, sql: &str) -> (String, String) {
    let error = session
        .simple_query(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("{sql} should have failed"));
    (error.code.clone(), error.message)
}

fn rows(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

/// Three roles, a table owned by `alice` whose only policy is written against
/// the group, and two rows so a policy that applies is visibly different from
/// one that does not.
///
/// `bob` is granted `SELECT` directly, and every other reader below is granted
/// it as it is created: these tests measure which rows a membership makes
/// visible, and a role with no grant would be refused outright before any
/// policy ran, so the grant is what keeps a privilege denial from masking the
/// row-security behaviour under test. It is deliberately *not* granted to
/// `readers`, which would make the grant itself follow the membership and hide
/// what the policy is doing.
const SETUP: &str = r"
CREATE ROLE alice;
CREATE ROLE bob;
CREATE ROLE readers;
CREATE TABLE document (id int4, holder text);
INSERT INTO document VALUES (1, 'alice'), (2, 'bob');
ALTER TABLE document OWNER TO alice;
GRANT SELECT ON document TO bob;
";

async fn engine_with_group_policy() -> SqlEngine {
    let engine = SqlEngine::new();
    let mut alice = engine.connect();
    run(&mut alice, SETUP).await;
    run(&mut alice, "SET ROLE alice").await;
    run(
        &mut alice,
        "ALTER TABLE document ENABLE ROW LEVEL SECURITY;
         CREATE POLICY group_reads ON document FOR SELECT TO readers USING (true);",
    )
    .await;
    engine
}

async fn as_role(engine: &SqlEngine, role: &str) -> SqlSession {
    let mut session = engine.connect();
    run(&mut session, &format!("SET ROLE {role}")).await;
    session
}

/// A session that never authenticated, which this engine reads as the bootstrap
/// superuser.
///
/// Role administration runs through here rather than through `alice`.
/// `PostgreSQL` needs the `ADMIN` option on a role to hand it out and the
/// `CREATEROLE` attribute to create one, and `alice` holds neither — she owns a
/// table, which is a different thing. Granting a membership is what makes a
/// role an owner, so a table owner who could grant memberships could hand her
/// own tables to anyone.
fn bootstrap(engine: &SqlEngine) -> SqlSession {
    engine.connect()
}

/// A membership granted with `GRANT … TO …` is the membership row security
/// reads: `bob` sees the group's rows only while he is in the group.
#[tokio::test]
async fn granting_a_role_widens_the_policies_that_apply() {
    let engine = engine_with_group_policy().await;
    let mut bob = as_role(&engine, "bob").await;
    // No membership yet, and row security is enabled with no policy that
    // matches bob, so the table is empty to him.
    assert!(query(&mut bob, "SELECT id FROM document ORDER BY id").await == rows(&[]));

    let mut root = bootstrap(&engine);
    run(&mut root, "GRANT readers TO bob").await;
    let mut bob = as_role(&engine, "bob").await;
    assert!(query(&mut bob, "SELECT id FROM document ORDER BY id").await == rows(&["1", "2"]));

    run(&mut root, "REVOKE readers FROM bob").await;
    let mut bob = as_role(&engine, "bob").await;
    assert!(query(&mut bob, "SELECT id FROM document ORDER BY id").await == rows(&[]));
}

/// `GRANT … TO …` and `CREATE ROLE … IN ROLE …` are the same membership. The
/// group policy applies to a role admitted either way.
#[tokio::test]
async fn the_two_spellings_of_membership_agree() {
    let engine = engine_with_group_policy().await;
    let mut root = bootstrap(&engine);
    let mut alice = as_role(&engine, "alice").await;
    run(&mut root, "CREATE ROLE carol IN ROLE readers").await;
    run(&mut root, "CREATE ROLE dave").await;
    run(&mut root, "GRANT readers TO dave").await;
    run(&mut alice, "GRANT SELECT ON document TO carol, dave").await;

    for role in ["carol", "dave"] {
        let mut session = as_role(&engine, role).await;
        assert!(
            query(&mut session, "SELECT id FROM document ORDER BY id").await == rows(&["1", "2"]),
            "role: {role}"
        );
    }

    // And `REVOKE` reaches the `IN ROLE` membership too, because there is only
    // one record behind both spellings.
    run(&mut root, "REVOKE readers FROM carol").await;
    let mut carol = as_role(&engine, "carol").await;
    assert!(query(&mut carol, "SELECT id FROM document ORDER BY id").await == rows(&[]));
}

/// The list forms and the `ADMIN OPTION` tails are accepted. The admin right
/// itself has nowhere to live — a membership is a bare key — so `WITH ADMIN
/// OPTION` grants the plain membership and `ADMIN OPTION FOR` leaves it alone.
#[tokio::test]
async fn list_forms_and_admin_option_are_accepted() {
    let engine = engine_with_group_policy().await;
    let mut root = bootstrap(&engine);
    let mut alice = as_role(&engine, "alice").await;
    run(&mut root, "CREATE ROLE writers; CREATE ROLE carol").await;
    run(&mut alice, "GRANT SELECT ON document TO carol").await;
    run(
        &mut root,
        "GRANT readers, writers TO bob, carol WITH ADMIN OPTION",
    )
    .await;
    for role in ["bob", "carol"] {
        let mut session = as_role(&engine, role).await;
        assert!(
            query(&mut session, "SELECT id FROM document ORDER BY id").await == rows(&["1", "2"]),
            "role: {role}"
        );
    }

    // `ADMIN OPTION FOR` strips only the admin right, so the membership — and
    // with it the policy — survives.
    run(&mut root, "REVOKE ADMIN OPTION FOR readers FROM bob").await;
    let mut bob = as_role(&engine, "bob").await;
    assert!(query(&mut bob, "SELECT id FROM document ORDER BY id").await == rows(&["1", "2"]));

    run(&mut root, "REVOKE readers, writers FROM bob, carol").await;
    for role in ["bob", "carol"] {
        let mut session = as_role(&engine, role).await;
        assert!(
            query(&mut session, "SELECT id FROM document ORDER BY id").await == rows(&[]),
            "role: {role}"
        );
    }
}

/// The privilege spelling still routes to privileges: `ON` decides, so a role
/// list and a privilege list can share their opening words without ambiguity.
#[tokio::test]
async fn the_privilege_spelling_is_unaffected() {
    let engine = engine_with_group_policy().await;
    let mut alice = as_role(&engine, "alice").await;
    run(&mut alice, "GRANT SELECT ON document TO readers").await;
    run(&mut alice, "REVOKE SELECT ON document FROM readers").await;
    run(&mut alice, "GRANT SELECT, UPDATE ON TABLE document TO bob").await;
}

/// Naming a role that does not exist is `PostgreSQL`'s undefined-object, on
/// either side of the `TO`.
#[tokio::test]
async fn an_unknown_role_is_refused() {
    let engine = engine_with_group_policy().await;
    let mut alice = bootstrap(&engine);
    for sql in [
        "GRANT nosuchgroup TO bob",
        "GRANT readers TO nosuchmember",
        "REVOKE nosuchgroup FROM bob",
    ] {
        let (code, message) = error_of(&mut alice, sql).await;
        assert!(code == "42704", "case: {sql}");
        assert!(message.contains("does not exist"), "case: {sql}");
    }
}
