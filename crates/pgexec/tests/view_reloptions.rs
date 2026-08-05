//! `CREATE VIEW … WITH (security_invoker | security_barrier)`: the clause as
//! written, stored, and read back.
//!
//! `security_invoker` is now the switch between the two rights models, so the
//! tests here pin the *storage* contract — every spelling of the list reaching
//! the view record, `OR REPLACE` rewriting it, an unknown name refused — and
//! one behavioral rule that belongs to the option rather than to owner rights:
//! writing `security_invoker` is the only thing that changes whose policies
//! filter a view. What owner rights themselves guarantee is pinned in
//! `owner_rights_views.rs`.

use std::sync::Arc;

use assert2::assert;
use crabka_pgcatalog::{RelationName, ViewOptions};
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgkv::{Kv, MemKv};
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

/// An engine over a store the test also holds, so the stored view record can be
/// read back through the same seam the executor writes it through.
fn fixture() -> (SqlEngine, Arc<dyn Kv>) {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("in-memory engine");
    (engine, kv)
}

/// Every spelling of the option list reaches the view record, and the view is
/// otherwise an ordinary view.
#[tokio::test]
async fn create_view_records_its_reloptions() {
    let cases = [
        ("CREATE VIEW v AS SELECT 1 AS n", false, false),
        (
            "CREATE VIEW v WITH (security_invoker) AS SELECT 1 AS n",
            true,
            false,
        ),
        (
            "CREATE VIEW v WITH (security_barrier) AS SELECT 1 AS n",
            false,
            true,
        ),
        (
            "CREATE VIEW v WITH (security_invoker = true, security_barrier = on) AS SELECT 1 AS n",
            true,
            true,
        ),
        (
            "CREATE VIEW v WITH (security_invoker = false, security_barrier = off) AS SELECT 1 AS n",
            false,
            false,
        ),
        // The column-alias list and the option list coexist, in that order.
        (
            "CREATE VIEW v (n) WITH (security_invoker = 1) AS SELECT 1",
            true,
            false,
        ),
    ];
    for (sql, security_invoker, security_barrier) in cases {
        let (engine, kv) = fixture();
        let mut session = engine.connect();
        run(&mut session, sql).await;
        let view = crabka_pgcatalog::get_view(kv.as_ref(), &RelationName::public("v"))
            .unwrap_or_else(|error| panic!("{sql}: stored view: {error:?}"));
        assert!(
            view.options
                == ViewOptions {
                    security_invoker,
                    security_barrier,
                },
            "case: {sql}"
        );
        assert!(
            query(&mut session, "SELECT n FROM v").await == rows(&["1"]),
            "case: {sql}"
        );
    }
}

/// `CREATE OR REPLACE` rewrites the option list along with the definition.
#[tokio::test]
async fn or_replace_rewrites_the_reloptions() {
    let (engine, kv) = fixture();
    let mut session = engine.connect();
    run(
        &mut session,
        "CREATE VIEW v WITH (security_barrier) AS SELECT 1 AS n",
    )
    .await;
    run(
        &mut session,
        "CREATE OR REPLACE VIEW v WITH (security_invoker) AS SELECT 2 AS n",
    )
    .await;
    let view = crabka_pgcatalog::get_view(kv.as_ref(), &RelationName::public("v")).expect("view");
    assert!(
        view.options
            == ViewOptions {
                security_invoker: true,
                security_barrier: false,
            }
    );
    assert!(query(&mut session, "SELECT n FROM v").await == rows(&["2"]));
}

/// A misspelled reloption is refused rather than silently accepted: a view
/// written `WITH (securty_invoker)` must not look like it took effect.
#[tokio::test]
async fn an_unknown_reloption_is_refused() {
    let (engine, _kv) = fixture();
    let mut session = engine.connect();
    let (code, message) = error_of(
        &mut session,
        "CREATE VIEW v WITH (securty_invoker) AS SELECT 1",
    )
    .await;
    assert!(code == "22023");
    assert!(message.contains("unrecognized parameter \"securty_invoker\""));

    let (code, message) = error_of(
        &mut session,
        "CREATE VIEW v WITH (security_barrier = maybe) AS SELECT 1",
    )
    .await;
    assert!(code == "22023");
    assert!(message.contains("invalid value for boolean option: \"maybe\""));
}

/// **The safety rule, restated for owner rights.** `security_invoker` now
/// decides whose row-security policies filter a view's body, and the option is
/// the *only* thing that decides it: a plain view and a `security_barrier` one
/// behave identically, because a barrier is about evaluation order and not
/// about identity. Writing the option must be what changes the answer — a view
/// that silently switched rights model on some other spelling would be a
/// bypass nobody wrote down.
///
/// The base relation is `FORCE`d so the owner does not simply skip its own
/// policies, which would hide the difference behind a bypass instead of
/// showing it.
#[tokio::test]
async fn only_security_invoker_changes_whose_policies_filter_a_view() {
    struct Case {
        option: &'static str,
        /// The ids bob reads through the view.
        expected: &'static [&'static str],
    }
    let cases = [
        Case {
            option: "",
            expected: &["1"],
        },
        Case {
            option: " WITH (security_barrier)",
            expected: &["1"],
        },
        Case {
            option: " WITH (security_invoker)",
            expected: &["2"],
        },
    ];
    for case in cases {
        let (engine, _kv) = fixture();
        let mut alice = engine.connect();
        run(
            &mut alice,
            "CREATE ROLE alice;
             CREATE ROLE bob;
             CREATE TABLE document (id int4, holder text);
             INSERT INTO document VALUES (1, 'alice'), (2, 'bob');
             ALTER TABLE document OWNER TO alice;",
        )
        .await;
        run(&mut alice, "SET ROLE alice").await;
        run(
            &mut alice,
            &format!(
                "CREATE VIEW doc_v{} AS SELECT id, holder FROM document",
                case.option
            ),
        )
        .await;
        // Bob holds a grant on the base relation too, so the `security_invoker`
        // case reaches the policies instead of stopping at a denial. The two
        // policies are told apart by their `TO` list, which is what makes
        // *whose* policies apply observable — a qual reading `current_user`
        // would not, because `current_user` names the invoker either way.
        run(
            &mut alice,
            "ALTER TABLE document ENABLE ROW LEVEL SECURITY;
             ALTER TABLE document FORCE ROW LEVEL SECURITY;
             CREATE POLICY only_alice ON document TO alice USING (id = 1);
             CREATE POLICY only_bob ON document TO bob USING (id = 2);
             GRANT SELECT ON document TO bob;
             GRANT SELECT ON doc_v TO bob;",
        )
        .await;

        let mut bob = engine.connect();
        run(&mut bob, "SET ROLE bob").await;
        assert!(
            query(&mut bob, "SELECT id FROM doc_v ORDER BY id").await == rows(case.expected),
            "case: {:?}",
            case.option
        );
        // Around the view, bob only ever sees his own row.
        assert!(
            query(&mut bob, "SELECT id FROM document ORDER BY id").await == rows(&["2"]),
            "case: {:?}",
            case.option
        );
    }
}
