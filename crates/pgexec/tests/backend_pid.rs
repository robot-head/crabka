//! `pg_backend_pid()` over the session the wire loop opens.
//!
//! `PostgreSQL` guarantees the function answers with the same id the connection
//! announced in `BackendKeyData`, which is how a client correlates a cancel
//! request with the session it belongs to. `Engine::connect_with_pid` carries
//! that id into the engine, so this file pins the two halves of the pairing:
//! the answer is the id the session was opened with, and no two sessions share
//! one. The wire half — that the announced id is the one handed to
//! `connect_with_pid` — is pinned in `crabka-pgwire`'s `listen_notify` suite.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, FieldDescription, QueryResult, Session};

/// The one row and one column `sql` returns, plus how that column is described.
async fn scalar(session: &mut SqlSession, sql: &str) -> (FieldDescription, String) {
    let result = session
        .simple_query(sql)
        .await
        .expect("query succeeds")
        .into_iter()
        .next()
        .expect("one result");
    match result {
        QueryResult::Rows {
            fields, mut rows, ..
        } => {
            let field = fields.into_iter().next().expect("one column");
            assert!(rows.len() == 1);
            let cell: Cell = rows
                .remove(0)
                .into_iter()
                .next()
                .expect("one column")
                .expect("a non-NULL backend pid");
            (
                field,
                String::from_utf8(cell.text.to_vec()).expect("text cell"),
            )
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

async fn backend_pid(session: &mut SqlSession) -> String {
    scalar(session, "SELECT pg_backend_pid()").await.1
}

/// The value a client reads back is the id its connection was announced under,
/// not the process id every session would share.
#[tokio::test]
async fn the_answer_is_the_backend_id_the_session_was_opened_with() {
    let engine = SqlEngine::new();
    let mut session = engine.connect_with_pid(4242);

    assert!(backend_pid(&mut session).await == "4242");
}

/// STABLE, so every statement of one connection reads the same id.
#[tokio::test]
async fn the_answer_does_not_change_over_the_life_of_a_session() {
    let engine = SqlEngine::new();
    let mut session = engine.connect_with_pid(77);

    assert!(backend_pid(&mut session).await == "77");
    session
        .simple_query("CREATE TABLE t (v int4)")
        .await
        .expect("create");
    assert!(backend_pid(&mut session).await == "77");
}

/// The property a per-session `pg_temp_<backendid>` namespace rests on: two
/// connections of one engine never report the same backend id. A session the
/// engine opens for itself draws one from the same counter the wire layer
/// announces from, so this holds without a client behind either session.
#[tokio::test]
async fn two_sessions_of_one_engine_report_different_backend_ids() {
    let engine = SqlEngine::new();
    let mut first = engine.connect();
    let mut second = engine.connect();

    let (first, second) = (
        backend_pid(&mut first).await,
        backend_pid(&mut second).await,
    );

    assert!(first != second);
    assert!(first.parse::<i32>().expect("an integer pid") > 0);
    assert!(second.parse::<i32>().expect("an integer pid") > 0);
}

/// `pg_stat_activity` describes the session asking, so its row's `pid` is the
/// same id `pg_backend_pid()` answers with. `PostgreSQL` 18.4 answers 1 to the
/// count below, and a client that looks itself up in `pg_stat_activity` — the
/// shape every "is my session still there" probe takes — depends on it.
#[tokio::test]
async fn pg_stat_activity_reports_the_sessions_own_backend_pid() {
    let engine = SqlEngine::new();
    let mut session = engine.connect_with_pid(5150);

    let (_, count) = scalar(
        &mut session,
        "SELECT count(*) FROM pg_stat_activity WHERE pid = pg_backend_pid()",
    )
    .await;
    assert!(count == "1");

    let (_, pid) = scalar(&mut session, "SELECT pid FROM pg_stat_activity").await;
    assert!(pid == "5150");

    // A second session sees its own id there, not the first's.
    let mut other = engine.connect_with_pid(5151);
    let (_, pid) = scalar(&mut other, "SELECT pid FROM pg_stat_activity").await;
    assert!(pid == "5151");
}

/// `integer`, as `PostgreSQL` 18.4 declares it — a client that binds the result
/// as int4 must not have to re-read the description.
#[tokio::test]
async fn the_result_is_described_as_int4() {
    let engine = SqlEngine::new();
    let mut session = engine.connect_with_pid(9);

    let (field, _) = scalar(&mut session, "SELECT pg_backend_pid()").await;

    assert!(
        field
            == FieldDescription {
                name: "pg_backend_pid".into(),
                table_oid: 0,
                column_id: 0,
                type_oid: 23,
                type_size: 4,
                type_modifier: -1,
                format: 0,
            }
    );
}
