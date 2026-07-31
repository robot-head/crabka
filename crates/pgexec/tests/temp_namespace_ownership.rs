//! A temporary namespace is `pg_temp_<backend id>` in the catalog every gateway
//! of a cluster shares, and a session empties it by name before it first uses
//! it — the only thing that stops relations left behind by a backend that never
//! tore itself down from leaking forever.
//!
//! These cases pin the one outcome that must never follow from that: a second
//! session holding the same backend id may not destroy the relations of a
//! session still using the namespace. Backend ids carry a per-process component
//! so the collision is not reached in practice, but every case here constructs
//! it directly rather than trusting the ids to differ, because what is being
//! tested is what happens when that assumption fails.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

/// The backend id both sessions of a colliding pair are opened under.
const SHARED_BACKEND_ID: i32 = 4242;

/// What the second session of the pair does. Every one of these reaches a purge
/// that names the namespace and nothing else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Collision {
    CreatesItsOwnTemporaryRelation,
    DiscardsTemp,
    DiscardsAll,
    Disconnects,
}

async fn run(session: &mut SqlSession, sql: &str) -> QueryResult {
    session
        .simple_query(sql)
        .await
        .expect("statement succeeds")
        .into_iter()
        .next_back()
        .expect("at least one result")
}

fn text_rows(result: &QueryResult) -> Vec<Vec<Option<String>>> {
    match result {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell: &Option<Cell>| {
                        cell.as_ref()
                            .map(|cell| String::from_utf8(cell.text.to_vec()).expect("utf-8 cell"))
                    })
                    .collect()
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

async fn rows(session: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    text_rows(&run(session, sql).await)
}

fn row(value: &str) -> Vec<Option<String>> {
    vec![Some(value.to_string())]
}

/// Every temporary relation the catalog holds, whichever namespace it is in, so
/// a case can see what a purge left behind without naming a namespace itself.
async fn temporary_relations(observer: &mut SqlSession) -> Vec<Vec<Option<String>>> {
    rows(
        observer,
        "SELECT relname FROM pg_class WHERE relpersistence = 't' ORDER BY relname",
    )
    .await
}

/// The rows a session's own temporary table still holds after another session
/// of the same backend id has done `collision`.
async fn rows_surviving(collision: Collision) -> Vec<Vec<Option<String>>> {
    let engine = SqlEngine::new();
    let mut first = engine.connect_with_pid(SHARED_BACKEND_ID);
    run(&mut first, "CREATE TEMP TABLE kept (x int)").await;
    run(&mut first, "INSERT INTO kept VALUES (1), (2)").await;

    let mut second = engine.connect_with_pid(SHARED_BACKEND_ID);
    match collision {
        Collision::CreatesItsOwnTemporaryRelation => {
            run(&mut second, "CREATE TEMP TABLE mine (y int)").await;
        }
        Collision::DiscardsTemp => {
            run(&mut second, "DISCARD TEMP").await;
        }
        Collision::DiscardsAll => {
            run(&mut second, "DISCARD ALL").await;
        }
        Collision::Disconnects => {
            second.terminate().await;
            drop(second);
        }
    }

    rows(&mut first, "SELECT x FROM kept ORDER BY x").await
}

#[tokio::test]
async fn a_session_sharing_a_backend_id_leaves_a_live_sessions_temporary_rows_alone() {
    for collision in [
        Collision::CreatesItsOwnTemporaryRelation,
        Collision::DiscardsTemp,
        Collision::DiscardsAll,
        Collision::Disconnects,
    ] {
        assert!(
            rows_surviving(collision).await == vec![row("1"), row("2")],
            "{collision:?}"
        );
    }
}

/// The purge is what keeps a dead backend's relations from leaking, so it has
/// to still happen once the session that held the id is gone.
#[tokio::test]
async fn a_backend_id_whose_session_ended_is_purged_by_the_next_one() {
    let engine = SqlEngine::new();
    let mut observer = engine.connect();

    let mut first = engine.connect_with_pid(SHARED_BACKEND_ID);
    run(&mut first, "CREATE TEMP TABLE leftover (x int)").await;
    drop(first);
    assert!(temporary_relations(&mut observer).await == vec![row("leftover")]);

    let mut second = engine.connect_with_pid(SHARED_BACKEND_ID);
    run(&mut second, "CREATE TEMP TABLE fresh (x int)").await;

    assert!(temporary_relations(&mut observer).await == vec![row("fresh")]);
}

/// A session is never held back from emptying its *own* namespace by its own
/// claim on it.
#[tokio::test]
async fn a_session_still_empties_its_own_temporary_namespace() {
    let engine = SqlEngine::new();
    let mut observer = engine.connect();

    let mut session = engine.connect_with_pid(SHARED_BACKEND_ID);
    run(&mut session, "CREATE TEMP TABLE discarded (x int)").await;
    assert!(temporary_relations(&mut observer).await == vec![row("discarded")]);

    run(&mut session, "DISCARD TEMP").await;
    assert!(temporary_relations(&mut observer).await.is_empty());

    run(&mut session, "CREATE TEMP TABLE torn_down (x int)").await;
    assert!(temporary_relations(&mut observer).await == vec![row("torn_down")]);

    session.terminate().await;
    assert!(temporary_relations(&mut observer).await.is_empty());
}

/// Two engines in one process have their own catalogs, so a namespace claimed
/// in one says nothing about the same name in the other.
#[tokio::test]
async fn a_claim_in_one_engine_does_not_reach_another_engines_catalog() {
    let first_engine = SqlEngine::new();
    let second_engine = SqlEngine::new();
    let mut held = first_engine.connect_with_pid(SHARED_BACKEND_ID);
    run(&mut held, "CREATE TEMP TABLE held (x int)").await;

    let mut observer = second_engine.connect();
    let mut other = second_engine.connect_with_pid(SHARED_BACKEND_ID);
    run(&mut other, "CREATE TEMP TABLE leftover (x int)").await;
    drop(other);
    let mut later = second_engine.connect_with_pid(SHARED_BACKEND_ID);
    run(&mut later, "CREATE TEMP TABLE fresh (x int)").await;

    assert!(temporary_relations(&mut observer).await == vec![row("fresh")]);
    assert!(rows(&mut held, "SELECT count(*) FROM held").await == vec![row("0")]);
}
