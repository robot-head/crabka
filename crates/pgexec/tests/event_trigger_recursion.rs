//! An event trigger whose body raises the event that fired it must be stopped
//! by a depth limit, not by the stack running out.
//!
//! Two depth counters already existed and neither covered this path.
//! `trigger::invoke` bumps `TRIGGER_DEPTH` for row triggers, and
//! `plpgsql_enter_call` bounds nested `plpgsql` calls, but
//! `SqlSession::fire_event_triggers` calls
//! `plpgsql::execute_trigger_function` directly and took neither. So a trigger
//! on `ddl_command_start` whose body issued DDL fired itself, and kept firing
//! itself, until the process aborted on a blown stack -- which in the wire
//! server means every other connection dies with it, not just the session that
//! wrote the trigger.
//!
//! The reason this is worth a test file rather than a line in another one is
//! that it was unreachable by accident. Until the command-tag table was fixed,
//! `WHEN TAG IN ('drop table')` was refused for its case, so
//! `PostgreSQL`'s own `event_trigger` regression test could not create the
//! self-firing trigger it contains. Widening the tags is what made the crash
//! reachable, and a depth limit is what makes it safe.

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Engine, QueryResult, Session};

/// A trigger function that re-enters DDL, and so re-raises the event that
/// invoked it.
const RECURSIVE_SETUP: &str = r"
CREATE FUNCTION recurse() RETURNS event_trigger AS $$
BEGIN
  CREATE TABLE IF NOT EXISTS spawned (id int4);
END
$$ LANGUAGE plpgsql;
CREATE EVENT TRIGGER loops ON ddl_command_start
  WHEN TAG IN ('CREATE TABLE') EXECUTE PROCEDURE recurse();
";

/// A trigger function that touches no DDL, so it fires exactly once.
const TERMINATING_SETUP: &str = r"
CREATE FUNCTION announce() RETURNS event_trigger AS $$
BEGIN
  RAISE NOTICE 'fired %', tg_tag;
END
$$ LANGUAGE plpgsql;
CREATE EVENT TRIGGER once ON ddl_command_start
  WHEN TAG IN ('CREATE TABLE') EXECUTE PROCEDURE announce();
";

async fn run(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} should succeed: {error:?}"))
}

/// A self-firing event trigger has to come back as an error. Reaching this
/// assertion at all is most of the point: before the depth guard the process
/// aborted here, so the failure mode was a dead test binary rather than a
/// failed assertion.
#[tokio::test]
async fn a_self_firing_event_trigger_is_refused_rather_than_crashing() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, RECURSIVE_SETUP).await;

    let error = session
        .simple_query("CREATE TABLE victim (id int4)")
        .await
        .expect_err("a self-firing event trigger should be refused");

    assert!(error.code == "54001");
    assert!(error.message == "stack depth limit exceeded");
}

/// The session stays usable afterwards. A depth limit that left the counter
/// raised would turn one bad trigger into a session that refuses all later
/// procedural work, which is a worse bug than the one being fixed.
#[tokio::test]
async fn the_session_still_works_after_the_limit_is_hit() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    run(&mut session, RECURSIVE_SETUP).await;

    let refused = session.simple_query("CREATE TABLE victim (id int4)").await;
    assert!(refused.is_err());

    run(&mut session, "DROP EVENT TRIGGER loops").await;
    run(&mut session, "CREATE TABLE afterwards (id int4)").await;
    let rows = run(&mut session, "SELECT count(*) FROM afterwards").await;
    assert!(!rows.is_empty());
}

/// An event trigger that does not re-enter DDL is untouched by the guard, and
/// still fires. Without this the guard could be satisfied by never running
/// event triggers at all.
#[tokio::test]
async fn a_terminating_event_trigger_still_fires() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    let mut notices = session.take_notices().expect("notice receiver");
    run(&mut session, TERMINATING_SETUP).await;
    while notices.try_recv().is_ok() {}

    run(&mut session, "CREATE TABLE plain (id int4)").await;

    let mut seen = Vec::new();
    while let Ok(notice) = notices.try_recv() {
        seen.push(notice.message);
    }
    assert!(seen.contains(&"fired CREATE TABLE".to_string()));
}
