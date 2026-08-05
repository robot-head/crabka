//! Table privileges through the SQL surface, end to end.
//!
//! Before this landed, `GRANT` and `REVOKE` wrote catalog rows that nothing
//! read: a role that owned nothing and had been granted nothing could `SELECT`,
//! `INSERT`, `UPDATE`, `DELETE` and `TRUNCATE` another role's table freely, and
//! `has_table_privilege` answered `true` to every question anyone asked it.
//! [`a_role_without_grants_is_denied_every_command`] is the direct inverse of
//! that observation.
//!
//! The tests worth keeping are the ones that pin a *bypass*. Five ways exist to
//! reach a relation without a grant — superuser, owner, membership in the owning
//! role, a grant to the role, a grant to `PUBLIC`, and a grant to a role whose
//! privileges the role holds — and one of them is why the default unauthenticated
//! session is unaffected by any of this. Each has a case below.

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

/// Whether `sql` was permitted, discarding whatever it returned.
async fn permitted(session: &mut SqlSession, sql: &str) -> bool {
    session.simple_query(sql).await.is_ok()
}

fn rows(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

/// A relation owned by `alice` with an inheritance tree and a view over it, and
/// the roles the bypass cases need.
const SETUP: &str = r"
CREATE ROLE alice;
CREATE ROLE bob;
CREATE ROLE readers;
CREATE ROLE reader IN ROLE readers;
CREATE ROLE heir IN ROLE alice;
CREATE TABLE document (id int4, body text);
INSERT INTO document VALUES (1, 'one'), (2, 'two'), (3, 'three');
CREATE TABLE parent (id int4);
CREATE TABLE child () INHERITS (parent);
INSERT INTO parent VALUES (10);
INSERT INTO child VALUES (11);
CREATE VIEW document_v AS SELECT id FROM document;
ALTER TABLE document OWNER TO alice;
ALTER TABLE parent OWNER TO alice;
ALTER TABLE child OWNER TO alice;
";

/// An engine with [`SETUP`] applied by the default (unauthenticated, and so
/// superuser) session, plus a session acting as `alice`, who owns everything.
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

// ----------------------------------------------------------------- the denial

/// **The observation this change exists to invert.**
///
/// A role that owns nothing and has been granted nothing may do nothing to
/// another role's table. Before enforcement every one of these succeeded.
#[tokio::test]
async fn a_role_without_grants_is_denied_every_command() {
    struct Case {
        sql: &'static str,
        message: &'static str,
    }
    let cases = [
        Case {
            sql: "SELECT * FROM document",
            message: "permission denied for table document",
        },
        Case {
            sql: "SELECT * FROM document WHERE id = 1",
            message: "permission denied for table document",
        },
        Case {
            sql: "SELECT count(*) FROM document",
            message: "permission denied for table document",
        },
        Case {
            sql: "INSERT INTO document VALUES (4, 'four')",
            message: "permission denied for table document",
        },
        Case {
            sql: "UPDATE document SET body = 'x'",
            message: "permission denied for table document",
        },
        Case {
            sql: "DELETE FROM document",
            message: "permission denied for table document",
        },
        Case {
            sql: "TRUNCATE document",
            message: "permission denied for table document",
        },
        Case {
            // A view carries its own ACL, and PostgreSQL names it a view.
            sql: "SELECT * FROM document_v",
            message: "permission denied for view document_v",
        },
        Case {
            // The privileges of the relation the query named, never a child's.
            sql: "SELECT * FROM parent",
            message: "permission denied for table parent",
        },
    ];
    let (engine, _alice) = owned_engine().await;
    let mut bob = as_role(&engine, "bob").await;
    for case in cases {
        let (sqlstate, message) = error_of(&mut bob, case.sql).await;
        assert!(sqlstate == "42501", "{}", case.sql);
        assert!(message == case.message, "{}", case.sql);
    }
}

/// The relation still holds every row it did: a denial is a refusal, not a
/// silent filter.
#[tokio::test]
async fn a_denial_changes_nothing() {
    let (engine, mut alice) = owned_engine().await;
    let mut bob = as_role(&engine, "bob").await;
    for sql in [
        "INSERT INTO document VALUES (4, 'four')",
        "DELETE FROM document",
        "TRUNCATE document",
        "UPDATE document SET body = 'tampered'",
    ] {
        let (sqlstate, _) = error_of(&mut bob, sql).await;
        assert!(sqlstate == "42501", "{sql}");
    }
    assert!(
        query(&mut alice, "SELECT id, body FROM document ORDER BY id").await
            == rows(&["1,one", "2,two", "3,three"])
    );
}

/// Both spellings of "act as someone else" reach the same decision.
///
/// The upstream corpus switches roles with `SET SESSION AUTHORIZATION` about as
/// often as with `SET ROLE`, and they are separate statements writing the same
/// session field — a gate reading one and not the other would look enforced and
/// not be.
#[tokio::test]
async fn both_role_switches_are_gated() {
    for switch in ["SET ROLE bob", "SET SESSION AUTHORIZATION bob"] {
        let (engine, _alice) = owned_engine().await;
        let mut session = engine.connect();
        run(&mut session, switch).await;
        let (sqlstate, message) = error_of(&mut session, "SELECT * FROM document").await;
        assert!(sqlstate == "42501", "{switch}");
        assert!(
            message == "permission denied for table document",
            "{switch}"
        );
        // And restoring the session's own identity restores its reach.
        run(&mut session, "RESET SESSION AUTHORIZATION; RESET ROLE").await;
        assert!(
            permitted(&mut session, "SELECT * FROM document").await,
            "{switch}"
        );
    }
}

// ---------------------------------------------------------------- the bypasses

/// **The blast-radius guarantee.**
///
/// The default session never authenticates, so its `current_user` is the
/// `PUBLIC` pseudo-role, which every decision in the engine resolves to the
/// bootstrap superuser. It must reach a relation it does not own and was never
/// granted — otherwise this change breaks every script that does not switch
/// roles, which is nearly all of them.
#[tokio::test]
async fn an_unauthenticated_session_is_unaffected() {
    let (engine, _alice) = owned_engine().await;
    let mut anonymous = engine.connect();
    assert!(
        query(&mut anonymous, "SELECT id FROM document ORDER BY id").await
            == rows(&["1", "2", "3"])
    );
    for sql in [
        "SELECT id FROM document_v ORDER BY id",
        "INSERT INTO document VALUES (4, 'four')",
        "UPDATE document SET body = 'x' WHERE id = 4",
        "DELETE FROM document WHERE id = 4",
        "TRUNCATE document",
    ] {
        assert!(permitted(&mut anonymous, sql).await, "{sql}");
    }
}

/// Every way a role reaches a relation without holding a grant of its own.
#[tokio::test]
async fn each_bypass_admits_a_read() {
    struct Case {
        name: &'static str,
        role: &'static str,
        /// Run as `alice`, the owner, before the read.
        setup: &'static str,
        admitted: bool,
    }
    let cases = [
        Case {
            name: "the owner needs no grant",
            role: "alice",
            setup: "",
            admitted: true,
        },
        Case {
            name: "a member of the owning role owns it too",
            role: "heir",
            setup: "",
            admitted: true,
        },
        Case {
            name: "a superuser needs no grant",
            role: "bob",
            setup: "ALTER ROLE bob SUPERUSER",
            admitted: true,
        },
        Case {
            name: "a grant naming the role admits it",
            role: "bob",
            setup: "GRANT SELECT ON document TO bob",
            admitted: true,
        },
        Case {
            name: "a grant to PUBLIC admits every role",
            role: "bob",
            setup: "GRANT SELECT ON document TO PUBLIC",
            admitted: true,
        },
        Case {
            name: "a grant to a role whose privileges this one holds admits it",
            role: "reader",
            setup: "GRANT SELECT ON document TO readers",
            admitted: true,
        },
        Case {
            name: "a grant to an unrelated role does not",
            role: "bob",
            setup: "GRANT SELECT ON document TO readers",
            admitted: false,
        },
        Case {
            name: "a grant of another privilege does not",
            role: "bob",
            setup: "GRANT INSERT ON document TO bob",
            admitted: false,
        },
        Case {
            name: "a revoked grant stops admitting",
            role: "bob",
            setup: "GRANT SELECT ON document TO bob; REVOKE SELECT ON document FROM bob",
            admitted: false,
        },
        Case {
            name: "REVOKE ALL takes back a narrower grant",
            role: "bob",
            setup: "GRANT SELECT ON document TO bob; REVOKE ALL ON document FROM bob",
            admitted: false,
        },
        Case {
            name: "REVOKE of one privilege leaves the rest of GRANT ALL",
            role: "bob",
            setup: "GRANT ALL ON document TO bob; REVOKE INSERT ON document FROM bob",
            admitted: true,
        },
    ];
    for case in cases {
        let (engine, mut alice) = owned_engine().await;
        if !case.setup.is_empty() {
            // `ALTER ROLE … SUPERUSER` is not alice's to give, so the setup runs
            // on a session that is still the bootstrap superuser when it must.
            let mut granter = if case.setup.starts_with("ALTER ROLE") {
                engine.connect()
            } else {
                std::mem::replace(&mut alice, engine.connect())
            };
            run(&mut granter, case.setup).await;
        }
        let mut session = as_role(&engine, case.role).await;
        assert!(
            permitted(&mut session, "SELECT id FROM document").await == case.admitted,
            "{}",
            case.name
        );
    }
}

// --------------------------------------------------------- per-command grants

/// A grant admits exactly its own command and no other.
#[tokio::test]
async fn each_command_needs_its_own_privilege() {
    struct Case {
        sql: &'static str,
        /// The single grant that makes `sql` succeed.
        needs: &'static str,
    }
    let cases = [
        Case {
            sql: "SELECT * FROM document",
            needs: "SELECT",
        },
        Case {
            sql: "INSERT INTO document VALUES (4, 'four')",
            needs: "INSERT",
        },
        Case {
            sql: "UPDATE document SET body = 'x'",
            needs: "UPDATE",
        },
        Case {
            sql: "DELETE FROM document",
            needs: "DELETE",
        },
        Case {
            sql: "TRUNCATE document",
            needs: "TRUNCATE",
        },
    ];
    for case in cases {
        for grant in ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"] {
            let (engine, mut alice) = owned_engine().await;
            run(&mut alice, &format!("GRANT {grant} ON document TO bob")).await;
            let mut bob = as_role(&engine, "bob").await;
            assert!(
                permitted(&mut bob, case.sql).await == (grant == case.needs),
                "{} under GRANT {grant}",
                case.sql
            );
        }
    }
}

/// `INSERT … ON CONFLICT DO UPDATE` needs all three of `INSERT`, `UPDATE` and
/// `SELECT`.
///
/// It reaches its conflicting row through its own arbiter probe rather than
/// through the gate every other write shares, and it hands that stored row to
/// the `DO UPDATE SET` expressions — so it reads the relation as surely as a
/// `SELECT` does, and `PostgreSQL` charges it for that.
#[tokio::test]
async fn on_conflict_do_update_needs_insert_update_and_select() {
    for (grants, permitted_now) in [
        ("INSERT, UPDATE, SELECT", true),
        ("INSERT, UPDATE", false),
        ("INSERT, SELECT", false),
        ("UPDATE, SELECT", false),
    ] {
        let (engine, mut alice) = owned_engine().await;
        run(
            &mut alice,
            &format!(
                "CREATE UNIQUE INDEX document_id_key ON document (id); \
                 GRANT {grants} ON document TO bob"
            ),
        )
        .await;
        let mut bob = as_role(&engine, "bob").await;
        assert!(
            permitted(
                &mut bob,
                "INSERT INTO document VALUES (1, 'again') \
                 ON CONFLICT (id) DO UPDATE SET body = excluded.body"
            )
            .await
                == permitted_now,
            "under GRANT {grants}"
        );
    }
}

/// **`TRUNCATE` is not `DELETE`.**
///
/// A `TRUNCATE` desugars to a per-relation `DELETE` inside the executor, so the
/// obvious implementation authorizes it with the `DELETE` privilege.
/// `PostgreSQL` does not, and the two grants must not stand in for each other in
/// either direction.
#[tokio::test]
async fn truncate_and_delete_do_not_substitute_for_each_other() {
    for (grant, truncate, delete) in [("TRUNCATE", true, false), ("DELETE", false, true)] {
        let (engine, mut alice) = owned_engine().await;
        run(&mut alice, &format!("GRANT {grant} ON document TO bob")).await;
        let mut bob = as_role(&engine, "bob").await;
        assert!(permitted(&mut bob, "TRUNCATE document").await == truncate);
        assert!(permitted(&mut bob, "DELETE FROM document").await == delete);
    }
}

/// **The `selectedCols` rule.**
///
/// `PostgreSQL` demands `SELECT` on a write's target only when the statement
/// reads that target's own columns. The third case is the one a coarse "does it
/// have a `WHERE`" rule gets wrong: the filter reads the *joined* relation, so
/// `SELECT` is needed there and not on the target.
#[tokio::test]
async fn a_write_needs_select_only_when_it_reads_its_target() {
    struct Case {
        name: &'static str,
        sql: &'static str,
        /// Succeeds with `UPDATE`/`DELETE` alone, no `SELECT` on the target.
        without_select: bool,
    }
    let cases = [
        Case {
            name: "a constant assignment reads nothing",
            sql: "UPDATE document SET body = 'x'",
            without_select: true,
        },
        Case {
            name: "a filter on the target reads it",
            sql: "UPDATE document SET body = 'x' WHERE id = 1",
            without_select: false,
        },
        Case {
            name: "a filter that reads only a joined relation does not",
            sql: "UPDATE document SET body = 'x' FROM other WHERE other.a = 5",
            without_select: true,
        },
        Case {
            name: "a self-referencing assignment reads the target",
            sql: "UPDATE document SET id = id + 1",
            without_select: false,
        },
        Case {
            name: "RETURNING a target column reads it",
            sql: "UPDATE document SET body = 'x' RETURNING id",
            without_select: false,
        },
        Case {
            name: "an unfiltered DELETE reads nothing",
            sql: "DELETE FROM document",
            without_select: true,
        },
        Case {
            name: "a filtered DELETE reads the target",
            sql: "DELETE FROM document WHERE id = 1",
            without_select: false,
        },
    ];
    for case in cases {
        for (extra, expected) in [("", case.without_select), (", SELECT", true)] {
            let (engine, mut alice) = owned_engine().await;
            // `other` is a second relation the joined case reads; bob may read
            // it, so the only question left is the target's own SELECT.
            run(
                &mut alice,
                &format!(
                    "GRANT UPDATE, DELETE{extra} ON document TO bob; \
                     CREATE TABLE other (a int4); \
                     INSERT INTO other VALUES (5); \
                     GRANT SELECT ON other TO bob"
                ),
            )
            .await;
            let mut bob = as_role(&engine, "bob").await;
            assert!(
                permitted(&mut bob, case.sql).await == expected,
                "{} (grants: UPDATE, DELETE{extra})",
                case.name
            );
        }
    }
}

// ----------------------------------------------------------- trees and views

/// A tree is read under the privileges of the relation the query named, and
/// none of its descendants'.
///
/// `PostgreSQL`'s rule exactly: a grant on the parent reaches every child's
/// rows, and a grant on the parent says nothing about reading a child directly.
#[tokio::test]
async fn a_tree_is_read_under_the_relation_that_was_named() {
    let (engine, mut alice) = owned_engine().await;
    run(&mut alice, "GRANT SELECT ON parent TO bob").await;
    let mut bob = as_role(&engine, "bob").await;
    assert!(query(&mut bob, "SELECT id FROM parent ORDER BY id").await == rows(&["10", "11"]));
    let (sqlstate, message) = error_of(&mut bob, "SELECT id FROM child").await;
    assert!(sqlstate == "42501");
    assert!(message == "permission denied for table child");
}

/// A view is checked on its own ACL, and its body still runs with the caller's
/// rights.
///
/// Both grants are needed, and neither substitutes for the other. That pairing
/// is what keeps a view from being a way around a base relation's grants — the
/// day the view body runs as its owner instead, that bound is what changes, and
/// it changes deliberately rather than by accident.
#[tokio::test]
async fn a_view_needs_grants_on_itself_and_on_what_it_reads() {
    struct Case {
        name: &'static str,
        grants: &'static str,
        readable: bool,
    }
    let cases = [
        Case {
            name: "neither grant",
            grants: "",
            readable: false,
        },
        Case {
            name: "the view alone is not enough",
            grants: "GRANT SELECT ON document_v TO bob",
            readable: false,
        },
        Case {
            name: "the base relation alone is not enough",
            grants: "GRANT SELECT ON document TO bob",
            readable: false,
        },
        Case {
            name: "both together admit the read",
            grants: "GRANT SELECT ON document_v TO bob; GRANT SELECT ON document TO bob",
            readable: true,
        },
    ];
    for case in cases {
        let (engine, _alice) = owned_engine().await;
        if !case.grants.is_empty() {
            // The view was created by the bootstrap session, so it owns it.
            let mut owner = engine.connect();
            run(&mut owner, case.grants).await;
        }
        let mut bob = as_role(&engine, "bob").await;
        assert!(
            permitted(&mut bob, "SELECT id FROM document_v").await == case.readable,
            "{}",
            case.name
        );
    }
}

/// Catalog and `information_schema` relations stay readable by everyone, as
/// `PostgreSQL`'s `PUBLIC` `SELECT` grant on them makes them.
///
/// They are virtual here and never reach a stored-relation gate, which is the
/// mechanism — but the property is worth pinning independently of it, because a
/// role that cannot read `pg_class` cannot run `\d` or connect with most
/// clients.
#[tokio::test]
async fn catalog_relations_stay_readable_by_everyone() {
    let (engine, _alice) = owned_engine().await;
    let mut bob = as_role(&engine, "bob").await;
    for sql in [
        "SELECT relname FROM pg_class WHERE relname = 'document'",
        "SELECT nspname FROM pg_namespace WHERE nspname = 'public'",
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'document'",
        "SELECT schemaname FROM pg_tables WHERE tablename = 'document'",
    ] {
        assert!(permitted(&mut bob, sql).await, "{sql}");
    }
}

// ------------------------------------------------------- the reporting functions

/// **`has_table_privilege` answers the question the gate answers.**
///
/// It returned `true` to everything before this change. A disagreement between
/// the two is worse than either being wrong on its own: a caller that checks
/// first and acts second would be told it may proceed and then refused.
#[tokio::test]
async fn has_table_privilege_agrees_with_the_gate() {
    struct Case {
        grants: &'static str,
        privilege: &'static str,
        held: bool,
    }
    let cases = [
        Case {
            grants: "",
            privilege: "SELECT",
            held: false,
        },
        Case {
            grants: "GRANT SELECT ON document TO bob",
            privilege: "SELECT",
            held: true,
        },
        Case {
            grants: "GRANT SELECT ON document TO bob",
            privilege: "INSERT",
            held: false,
        },
        Case {
            grants: "GRANT ALL ON document TO bob",
            privilege: "INSERT",
            held: true,
        },
        Case {
            grants: "GRANT ALL ON document TO bob",
            privilege: "REFERENCES",
            held: true,
        },
        Case {
            grants: "GRANT SELECT ON document TO PUBLIC",
            privilege: "SELECT",
            held: true,
        },
    ];
    for case in cases {
        let (engine, mut alice) = owned_engine().await;
        if !case.grants.is_empty() {
            run(&mut alice, case.grants).await;
        }
        let mut bob = as_role(&engine, "bob").await;
        let answer = query(
            &mut bob,
            &format!(
                "SELECT has_table_privilege('document', '{}')",
                case.privilege
            ),
        )
        .await;
        assert!(
            answer == rows(&[if case.held { "t" } else { "f" }]),
            "{} for {}",
            case.privilege,
            case.grants
        );
        // The same question asked about someone else, from a session that may
        // ask it: the three-argument form names the role explicitly.
        let mut owner = engine.connect();
        let asked = query(
            &mut owner,
            &format!(
                "SELECT has_table_privilege('bob', 'document', '{}')",
                case.privilege
            ),
        )
        .await;
        assert!(asked == answer, "two-arg and three-arg forms must agree");
        // And it must match what actually happens.
        if case.privilege == "SELECT" {
            assert!(permitted(&mut bob, "SELECT id FROM document").await == case.held);
        }
    }
}

/// The owner and the superuser answer `true` without any grant row existing,
/// and an unrecognized privilege name is still 22023.
#[tokio::test]
async fn has_table_privilege_covers_the_implicit_holders() {
    let (engine, mut alice) = owned_engine().await;
    assert!(
        query(
            &mut alice,
            "SELECT has_table_privilege('document', 'DELETE')"
        )
        .await
            == rows(&["t"])
    );
    let mut anonymous = engine.connect();
    assert!(
        query(
            &mut anonymous,
            "SELECT has_table_privilege('document', 'TRUNCATE')"
        )
        .await
            == rows(&["t"])
    );
    let (sqlstate, message) = error_of(
        &mut anonymous,
        "SELECT has_table_privilege('document', 'FLY')",
    )
    .await;
    assert!(sqlstate == "22023");
    assert!(message == "unrecognized privilege type: \"FLY\"");
}
