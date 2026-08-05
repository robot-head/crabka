//! Owner-rights views: a view body runs as the role that owns the view.
//!
//! This is the second half of the pair that began with table privileges. Row
//! security shipped first with invoker semantics for every view — deliberately,
//! because owner rights over an unenforced `GRANT` make any view over a
//! row-secured table a universal bypass. With `GRANT` enforced, the bound that
//! makes owner rights safe is a different one, and every test below exists to
//! pin it: *the invoker sees exactly what the view's owner would see, and only
//! if the invoker was granted the view.*
//!
//! The observation this change inverts is
//! [`a_grant_on_the_view_alone_is_enough`]: before it, granting `SELECT` on a
//! view and nothing else denied the reader, because the body was still read
//! under the reader's own grants — which is not how any upstream test grants
//! access to a view.

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

/// Every row of the first result, each rendered as a comma-joined string so a
/// whole expectation is one literal.
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

/// `document` is alice's, with one row naming each role. Nobody else is granted
/// anything on it, ever: every read below reaches it through a view or not at
/// all.
const SETUP: &str = r"
CREATE ROLE alice;
CREATE ROLE bob;
CREATE ROLE carol;
CREATE TABLE document (id int4, holder text);
INSERT INTO document VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
ALTER TABLE document OWNER TO alice;
";

/// An engine with [`SETUP`] applied by the default (unauthenticated, superuser)
/// session, plus a session already acting as `alice`.
async fn owned_engine() -> (SqlEngine, SqlSession) {
    let engine = SqlEngine::new();
    let mut alice = engine.connect();
    run(&mut alice, SETUP).await;
    run(&mut alice, "SET ROLE alice").await;
    (engine, alice)
}

async fn as_role(engine: &SqlEngine, role: &str) -> SqlSession {
    let mut session = engine.connect();
    run(&mut session, &format!("SET ROLE {role}")).await;
    session
}

// ------------------------------------------------------- the observation

/// **The observation this change inverts.**
///
/// Alice owns `document` and a view over it, and grants bob `SELECT` on the
/// *view* only. That is how every upstream test hands out access to a view, and
/// under invoker semantics it denied the read: bob's session tried to read
/// `document` under bob's own (empty) grants. With owner rights the body reads
/// as alice, who owns the table, and bob sees the view.
#[tokio::test]
async fn a_grant_on_the_view_alone_is_enough() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE VIEW doc_v AS SELECT id, holder FROM document;
         GRANT SELECT ON doc_v TO bob;",
    )
    .await;

    let mut bob = as_role(&engine, "bob").await;
    assert!(query(&mut bob, "SELECT id FROM doc_v ORDER BY id").await == rows(&["1", "2", "3"]));
    // And the base relation itself is still closed to him.
    let (sqlstate, message) = error_of(&mut bob, "SELECT id FROM document").await;
    assert!(sqlstate == "42501");
    assert!(message == "permission denied for table document");
}

// ------------------------------------------------ the safety property

/// Owner rights do not make a view public. Without a grant on the view, the
/// read stops at the view's own ACL and never reaches the owner's rights.
#[tokio::test]
async fn the_view_still_needs_its_own_grant() {
    let (engine, mut alice) = owned_engine().await;
    run(&mut alice, "CREATE VIEW doc_v AS SELECT id FROM document").await;

    let mut bob = as_role(&engine, "bob").await;
    let (sqlstate, message) = error_of(&mut bob, "SELECT id FROM doc_v").await;
    assert!(sqlstate == "42501");
    assert!(message == "permission denied for view doc_v");
}

/// A view whose owner cannot read the base relation does not let the invoker
/// read it either — the body is evaluated as the owner, so it is denied as the
/// owner, and the denial names the *table*.
#[tokio::test]
async fn a_view_owner_without_access_grants_nothing() {
    let (engine, mut alice) = owned_engine().await;
    run(&mut alice, "GRANT SELECT ON document TO bob").await;
    // Carol may define a view over a relation she cannot read; what she cannot
    // do is read through it, and neither can anyone she grants it to.
    let mut carol = as_role(&engine, "carol").await;
    run(
        &mut carol,
        "CREATE VIEW carol_v AS SELECT id FROM document;
         GRANT SELECT ON carol_v TO bob;",
    )
    .await;

    let mut bob = as_role(&engine, "bob").await;
    // Bob may read the table directly …
    assert!(query(&mut bob, "SELECT id FROM document ORDER BY id").await == rows(&["1", "2", "3"]));
    // … and not through carol's view, because carol may not.
    for session in [&mut carol, &mut bob] {
        let (sqlstate, message) = error_of(session, "SELECT id FROM carol_v").await;
        assert!(sqlstate == "42501");
        assert!(message == "permission denied for table document");
    }
}

/// A view is not an escalation: what bob reads through alice's view is exactly
/// what alice reads through it, row for row, whether the base relation is
/// row-secured or not.
#[tokio::test]
async fn a_view_shows_the_invoker_exactly_what_its_owner_sees() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE VIEW doc_v AS SELECT id, holder FROM document;
         GRANT SELECT ON doc_v TO bob;
         ALTER TABLE document ENABLE ROW LEVEL SECURITY;
         ALTER TABLE document FORCE ROW LEVEL SECURITY;
         CREATE POLICY low ON document USING (id < 3);",
    )
    .await;

    let mut bob = as_role(&engine, "bob").await;
    let through_owner = query(&mut alice, "SELECT id FROM doc_v ORDER BY id").await;
    let through_invoker = query(&mut bob, "SELECT id FROM doc_v ORDER BY id").await;
    assert!(through_owner == rows(&["1", "2"]));
    assert!(through_invoker == through_owner);
}

/// A policy chosen by its `TO` list filters differently for each role, which is
/// what makes the two view kinds observably different: the owner-rights view
/// applies the *owner's* policy and the `security_invoker` view applies the
/// *reader's*.
#[tokio::test]
async fn which_policy_applies_follows_the_view_kind() {
    struct Case {
        kind: &'static str,
        options: &'static str,
        /// What bob reads through the view.
        expected: &'static [&'static str],
    }
    let cases = [
        Case {
            kind: "an owner-rights view applies the owner's policy",
            options: "",
            expected: &["1"],
        },
        Case {
            kind: "a security_invoker view applies the reader's policy",
            options: " WITH (security_invoker)",
            expected: &["2"],
        },
    ];
    for case in cases {
        let (engine, mut alice) = owned_engine().await;
        // Carol owns the view, so neither role owning the table can shortcut
        // the policy: alice would bypass her own table's row security.
        run(
            &mut alice,
            "GRANT SELECT ON document TO bob;
             GRANT SELECT ON document TO carol;
             ALTER TABLE document ENABLE ROW LEVEL SECURITY;
             CREATE POLICY only_carol ON document TO carol USING (id = 1);
             CREATE POLICY only_bob ON document TO bob USING (id = 2);",
        )
        .await;
        let mut carol = as_role(&engine, "carol").await;
        run(
            &mut carol,
            &format!(
                "CREATE VIEW doc_v{} AS SELECT id FROM document;
                 GRANT SELECT ON doc_v TO bob;",
                case.options
            ),
        )
        .await;

        let mut bob = as_role(&engine, "bob").await;
        assert!(
            query(&mut bob, "SELECT id FROM doc_v ORDER BY id").await == rows(case.expected),
            "{}",
            case.kind
        );
    }
}

/// A policy whose qual reads a second relation is evaluated with the same
/// identity the body is: the owner needs the grant on that relation, and the
/// invoker does not.
///
/// This is `PostgreSQL`'s bug #15708 case, and it is the one that proves the
/// identity switch reaches all the way into the policy, not just into the
/// relation the view names.
#[tokio::test]
async fn a_policy_reads_its_own_helper_relation_as_the_view_owner() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE TABLE allowed (id int4);
         INSERT INTO allowed VALUES (1), (2);
         ALTER TABLE document ENABLE ROW LEVEL SECURITY;
         ALTER TABLE document FORCE ROW LEVEL SECURITY;
         CREATE POLICY visible ON document USING (id IN (SELECT id FROM allowed));
         GRANT SELECT ON document TO carol;
         GRANT SELECT ON allowed TO carol;",
    )
    .await;
    let mut carol = as_role(&engine, "carol").await;
    run(
        &mut carol,
        "CREATE VIEW doc_v AS SELECT id FROM document;
         GRANT SELECT ON doc_v TO bob;",
    )
    .await;

    // Bob holds nothing on either relation and still reads the view, because
    // carol holds both.
    let mut bob = as_role(&engine, "bob").await;
    assert!(query(&mut bob, "SELECT id FROM doc_v ORDER BY id").await == rows(&["1", "2"]));

    // Take carol's access to the helper relation away and the view stops
    // working for everyone, including bob, who never had it.
    run(&mut alice, "REVOKE SELECT ON allowed FROM carol").await;
    for session in [&mut carol, &mut bob] {
        let (sqlstate, message) = error_of(session, "SELECT id FROM doc_v ORDER BY id").await;
        assert!(sqlstate == "42501");
        assert!(message == "permission denied for table allowed");
    }
}

/// `CURRENT_USER` keeps naming the *invoking* role inside a view body.
/// `PostgreSQL` swaps the identity privilege and policy decisions are made
/// under, and leaves `GetUserId()` alone; a view that reported its owner would
/// change the meaning of every `dauthor = current_user` policy ever written.
#[tokio::test]
async fn current_user_inside_a_view_body_is_the_invoker() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE VIEW whoami AS SELECT current_user AS role_name, id FROM document;
         GRANT SELECT ON whoami TO bob;",
    )
    .await;

    let mut bob = as_role(&engine, "bob").await;
    assert!(query(&mut bob, "SELECT DISTINCT role_name FROM whoami").await == rows(&["bob"]));
    assert!(query(&mut alice, "SELECT DISTINCT role_name FROM whoami").await == rows(&["alice"]));
}

/// A policy written against `current_user` therefore still names the reader,
/// even though *which* policies apply was decided for the owner. Both halves
/// are visible in one read: bob reaches the row at all only because carol may,
/// and the row he gets is the one naming him.
#[tokio::test]
async fn a_policy_qual_reading_current_user_names_the_invoker() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "ALTER TABLE document ENABLE ROW LEVEL SECURITY;
         ALTER TABLE document FORCE ROW LEVEL SECURITY;
         CREATE POLICY own ON document USING (holder = current_user);
         GRANT SELECT ON document TO carol;",
    )
    .await;
    let mut carol = as_role(&engine, "carol").await;
    run(
        &mut carol,
        "CREATE VIEW doc_v AS SELECT id, holder FROM document;
         GRANT SELECT ON doc_v TO bob;",
    )
    .await;

    let mut bob = as_role(&engine, "bob").await;
    assert!(query(&mut bob, "SELECT id FROM doc_v").await == rows(&["2"]));
    assert!(query(&mut carol, "SELECT id FROM doc_v").await == rows(&["3"]));
}

/// Nesting shadows rather than stacks. Bob reads carol's view; carol's body
/// reads alice's row-secured table; that table's policy reads *dave's* view;
/// and dave's body reads a relation only dave may read. Three identities, each
/// governing exactly its own body, because each expansion derives its context
/// from the one it was reached through rather than from the session.
///
/// The inner view is reached through a policy qual rather than through the
/// outer view's `FROM` because `CREATE VIEW` refuses both a view in a `FROM`
/// item and a subquery in its body (`0A000`, `validate_view_definition`) — a
/// limitation of the view surface that predates this change and is unrelated
/// to which role a body runs as.
#[tokio::test]
async fn a_nested_view_body_runs_as_its_own_owner() {
    let (engine, mut alice) = owned_engine().await;
    run(&mut alice, "CREATE ROLE dave").await;
    let mut dave = as_role(&engine, "dave").await;
    run(
        &mut dave,
        "CREATE TABLE allowed (id int4);
         INSERT INTO allowed VALUES (1), (2);
         CREATE VIEW allowed_v AS SELECT id FROM allowed;
         GRANT SELECT ON allowed_v TO alice;",
    )
    .await;
    run(
        &mut alice,
        "ALTER TABLE document ENABLE ROW LEVEL SECURITY;
         ALTER TABLE document FORCE ROW LEVEL SECURITY;
         CREATE POLICY visible ON document USING (id IN (SELECT id FROM allowed_v));
         GRANT SELECT ON document TO carol;
         GRANT SELECT ON allowed_v TO carol;",
    )
    .await;
    let mut carol = as_role(&engine, "carol").await;
    run(
        &mut carol,
        "CREATE VIEW doc_v AS SELECT id FROM document;
         GRANT SELECT ON doc_v TO bob;",
    )
    .await;

    // Bob holds nothing but `doc_v`, and reads the two rows dave's view
    // admits — through a table he cannot read, filtered by a policy that
    // reads a view he cannot read, over a relation only dave can read.
    let mut bob = as_role(&engine, "bob").await;
    assert!(query(&mut bob, "SELECT id FROM doc_v ORDER BY id").await == rows(&["1", "2"]));

    // The middle link is checked as *carol*, the owner of the body that
    // reaches it — not as bob, and not as dave.
    run(&mut dave, "REVOKE SELECT ON allowed_v FROM carol").await;
    let (sqlstate, message) = error_of(&mut bob, "SELECT id FROM doc_v").await;
    assert!(sqlstate == "42501");
    assert!(message == "permission denied for view allowed_v");

    // And the innermost body is still dave's: nobody else ever needed a grant
    // on the relation behind his view.
    run(&mut dave, "GRANT SELECT ON allowed_v TO carol").await;
    assert!(query(&mut bob, "SELECT id FROM doc_v ORDER BY id").await == rows(&["1", "2"]));
    for session in [&mut bob, &mut carol] {
        let (sqlstate, message) = error_of(session, "SELECT id FROM allowed").await;
        assert!(sqlstate == "42501");
        assert!(message == "permission denied for table allowed");
    }
}

/// A `security_invoker` view is not a way to borrow the invoker's identity for
/// a relation the invoker cannot reach: its body needs the *reader's* grants,
/// which is the whole point of the option.
#[tokio::test]
async fn a_security_invoker_view_needs_the_readers_grants() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE VIEW doc_v WITH (security_invoker) AS SELECT id FROM document;
         GRANT SELECT ON doc_v TO bob;",
    )
    .await;

    let mut bob = as_role(&engine, "bob").await;
    let (sqlstate, message) = error_of(&mut bob, "SELECT id FROM doc_v").await;
    assert!(sqlstate == "42501");
    assert!(message == "permission denied for table document");

    run(&mut alice, "GRANT SELECT ON document TO bob").await;
    assert!(query(&mut bob, "SELECT id FROM doc_v ORDER BY id").await == rows(&["1", "2", "3"]));
}

// -------------------------------------------------------------- ALTER VIEW

/// `ALTER VIEW … OWNER TO` moves the identity the body runs under, which is the
/// only way `PostgreSQL`'s bug #15708 regression test can be written.
#[tokio::test]
async fn alter_view_owner_to_moves_the_bodys_identity() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE VIEW doc_v AS SELECT id FROM document;
         GRANT SELECT ON doc_v TO bob;
         GRANT SELECT ON doc_v TO carol;",
    )
    .await;

    let mut bob = as_role(&engine, "bob").await;
    assert!(query(&mut bob, "SELECT id FROM doc_v ORDER BY id").await == rows(&["1", "2", "3"]));

    // Handed to carol, who cannot read the table, the view stops working —
    // for its new owner and for everyone it was granted to.
    run(&mut alice, "ALTER VIEW doc_v OWNER TO carol").await;
    let (sqlstate, message) = error_of(&mut bob, "SELECT id FROM doc_v").await;
    assert!(sqlstate == "42501");
    assert!(message == "permission denied for table document");

    // And the grant carol needs is on the table the body reads, not the view.
    run(&mut alice, "GRANT SELECT ON document TO carol").await;
    assert!(query(&mut bob, "SELECT id FROM doc_v ORDER BY id").await == rows(&["1", "2", "3"]));
}

/// Only the owner (or a superuser) may hand a view away, and the recipient must
/// exist.
#[tokio::test]
async fn alter_view_owner_to_is_owner_only() {
    let (engine, mut alice) = owned_engine().await;
    run(&mut alice, "CREATE VIEW doc_v AS SELECT id FROM document").await;

    let mut bob = as_role(&engine, "bob").await;
    let (sqlstate, message) = error_of(&mut bob, "ALTER VIEW doc_v OWNER TO bob").await;
    assert!(sqlstate == "42501");
    assert!(message == "must be owner of view doc_v");

    let (sqlstate, message) = error_of(&mut alice, "ALTER VIEW doc_v OWNER TO nobody").await;
    assert!(sqlstate == "42704");
    assert!(message == "role \"nobody\" does not exist");
}

/// `ALTER VIEW … SET/RESET (…)` rewrites the reloptions, so a view can be moved
/// between the two rights models after it was created.
#[tokio::test]
async fn alter_view_set_and_reset_reloptions() {
    let (engine, mut alice) = owned_engine().await;
    run(
        &mut alice,
        "CREATE VIEW doc_v AS SELECT id FROM document;
         GRANT SELECT ON doc_v TO bob;",
    )
    .await;
    let mut bob = as_role(&engine, "bob").await;
    assert!(query(&mut bob, "SELECT id FROM doc_v ORDER BY id").await == rows(&["1", "2", "3"]));

    run(&mut alice, "ALTER VIEW doc_v SET (security_invoker = true)").await;
    let (sqlstate, _) = error_of(&mut bob, "SELECT id FROM doc_v").await;
    assert!(sqlstate == "42501");

    run(&mut alice, "ALTER VIEW doc_v RESET (security_invoker)").await;
    assert!(query(&mut bob, "SELECT id FROM doc_v ORDER BY id").await == rows(&["1", "2", "3"]));
}

/// A view's own qualifier — and the policy behind it — is applied before the
/// reader's predicate, so a leaky function written outside the view never
/// **sees** a row the view removed.
///
/// This is the upstream `f_leak` test, written the way it can be observed here:
/// the predicate records every value it is handed, and the recording is what is
/// asserted. Pinning only the returned rows would pass even if the leak
/// happened, which is exactly how the predicate-pushdown leak this engine
/// already shipped got past a purpose-written unit test.
///
/// It holds for a plain view as well as a barrier one, and for the same reason:
/// a view body is materialized before the reader's `WHERE` runs, so there is no
/// reordering for `security_barrier` to forbid. The option is accepted and
/// inert rather than wired to `leakproof_predicate`.
#[tokio::test]
async fn a_leaky_predicate_never_sees_a_row_a_view_removed() {
    for option in ["", " WITH (security_barrier)"] {
        let (engine, mut alice) = owned_engine().await;
        run(
            &mut alice,
            &format!(
                "CREATE TABLE leaked (holder text);
                 CREATE FUNCTION f_leak(t text) RETURNS bool LANGUAGE plpgsql AS
                     $$ BEGIN INSERT INTO leaked VALUES (t); RETURN true; END $$;
                 ALTER TABLE document ENABLE ROW LEVEL SECURITY;
                 ALTER TABLE document FORCE ROW LEVEL SECURITY;
                 CREATE POLICY low ON document USING (id < 3);
                 CREATE VIEW doc_v{option} AS
                     SELECT id, holder FROM document WHERE id > 1;
                 GRANT SELECT ON doc_v TO bob;
                 GRANT INSERT ON leaked TO bob;"
            ),
        )
        .await;

        // The policy admits ids 1 and 2; the view's own qualifier admits 2 and
        // 3; so exactly one row survives, and `f_leak` must be handed exactly
        // that row's title and no other.
        let mut bob = as_role(&engine, "bob").await;
        assert!(
            query(
                &mut bob,
                "SELECT id FROM doc_v WHERE f_leak(holder) ORDER BY id"
            )
            .await
                == rows(&["2"]),
            "case: {option:?}"
        );
        assert!(
            query(&mut alice, "SELECT holder FROM leaked ORDER BY holder").await == rows(&["bob"]),
            "case: {option:?}"
        );
    }
}
