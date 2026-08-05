//! `CREATE VIEW … WITH (security_invoker | security_barrier)`.
//!
//! The clause is parsed and recorded on the view; **nothing reads it back yet**.
//! Row security evaluates every view with invoker semantics on purpose, so a
//! `security_invoker` view and a plain one must still return the same rows to
//! the same role — the tests below pin exactly that, because the day the option
//! starts being honoured is the day an owner-rights view over a row-secured
//! table becomes a bypass, and it must not happen by accident.

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

/// **The safety rule.** `security_invoker` is stored, not honoured: a view over
/// a row-secured table shows the querying role exactly the rows that role could
/// have selected directly, whether or not the option was written. Owner-rights
/// views must never start working by accident: the day the option is honoured,
/// a view whose owner bypasses the base relation's policies becomes a way
/// around them for everyone the view is granted to.
#[tokio::test]
async fn security_invoker_does_not_change_what_a_view_shows() {
    for option in ["", " WITH (security_invoker)", " WITH (security_barrier)"] {
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
            &format!("CREATE VIEW doc_v{option} AS SELECT id, holder FROM document"),
        )
        .await;
        // What is under test is which *rows* the view shows, so bob holds a
        // grant on both the view and its base relation: without them the read
        // would stop at a privilege denial and never reach the policy. A view
        // needs its own grant, and its body still reads the base relation with
        // invoker rights, so both are required.
        run(
            &mut alice,
            "ALTER TABLE document ENABLE ROW LEVEL SECURITY;
             CREATE POLICY own ON document USING (holder = current_user);
             GRANT SELECT ON document TO bob;
             GRANT SELECT ON doc_v TO bob;",
        )
        .await;

        let mut bob = engine.connect();
        run(&mut bob, "SET ROLE bob").await;
        // Bob's own policy row, through the view and around it, identically.
        assert!(
            query(&mut bob, "SELECT id FROM doc_v ORDER BY id").await == rows(&["2"]),
            "case: {option:?}"
        );
        assert!(
            query(&mut bob, "SELECT id FROM document ORDER BY id").await == rows(&["2"]),
            "case: {option:?}"
        );
    }
}
