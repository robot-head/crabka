//! The in-flight statement registry, driven through real sessions.
//!
//! The registry exists to answer one question about a wedged server — which
//! backend stopped finishing, and what was it running — so these tests ask it
//! through the same path a client takes, and read the answer through the
//! registry rather than through the log.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use assert2::assert;
use crabka_pgexec::{
    SqlEngine,
    watchdog::{StuckStatement, TransactionActivity},
};
use crabka_pgwire::engine::{Engine, Session};

/// Well under anything a real statement takes, so an in-flight statement is
/// always past it.
const TINY: Duration = Duration::ZERO;

/// No test here wants a second report, so the repeat interval never elapses.
const NEVER_REPEAT: Duration = Duration::MAX;

/// The backend id the blocked session connects with, so the report can be
/// matched to it without guessing.
const BLOCKED_PID: i32 = 4242;

/// Wait for `predicate` to hold of the registry, or give up.
///
/// The blocking statements below reach the registry from another task, so the
/// only alternative to polling is a sleep long enough to be flaky in the other
/// direction.
async fn until(
    engine: &SqlEngine,
    predicate: impl Fn(&[crabka_pgexec::watchdog::InFlightStatement]) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let running = engine.statement_registry().in_flight();
        if predicate(&running) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "registry never reached the expected state: {running:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn reports(engine: &SqlEngine, threshold: Duration) -> Vec<StuckStatement> {
    engine
        .statement_registry()
        .due_reports(Instant::now(), threshold, NEVER_REPEAT)
}

/// A statement that finishes leaves nothing behind, so a later scan has nothing
/// to report however low the threshold goes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_ordinary_statement_is_gone_from_the_registry_when_it_returns() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    session
        .simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    session
        .simple_query("SELECT id FROM t")
        .await
        .expect("select");

    assert!(engine.statement_registry().in_flight() == vec![]);
    assert!(reports(&engine, TINY) == vec![]);
}

/// The error path is the one a hand-written deregistration call would miss: a
/// statement that returns an error from deep inside execution must still clear
/// its entry, or every later report names it forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_erroring_statement_leaves_no_entry_behind() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();

    for sql in [
        "SELECT * FROM does_not_exist",
        "SELECT 1 +",
        "CREATE TABLE t (id int4); CREATE TABLE t (id int4)",
        "SELECT 1/0",
    ] {
        session
            .simple_query(sql)
            .await
            .expect_err("statement was expected to fail");
        assert!(
            engine.statement_registry().in_flight() == vec![],
            "entry survived {sql:?}"
        );
    }

    // A failed transaction block leaves the session unusable but the registry
    // empty all the same.
    session.simple_query("ROLLBACK").await.expect("rollback");
    assert!(reports(&engine, TINY) == vec![]);
}

/// A statement blocked on another transaction's row lock is exactly the shape
/// the watchdog is for: it is running, it is making no progress, and nothing
/// else on the server is busy. It has to show up in the registry with its own
/// backend id, its transaction state, and its text.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_statement_that_stops_finishing_is_reported_once_with_its_text() {
    let engine = Arc::new(SqlEngine::new());
    let mut setup = engine.connect();
    setup
        .simple_query("CREATE TABLE t (id int4, v text)")
        .await
        .expect("create");
    setup
        .simple_query("INSERT INTO t VALUES (1,'orig')")
        .await
        .expect("insert");

    let mut holder = engine.connect();
    holder.simple_query("BEGIN").await.expect("begin");
    holder
        .simple_query("UPDATE t SET v='held' WHERE id=1")
        .await
        .expect("lock the row");

    let blocked_engine = Arc::clone(&engine);
    let blocked = tokio::spawn(async move {
        let mut session = blocked_engine.connect_with_pid(BLOCKED_PID);
        session.simple_query("BEGIN").await.expect("begin");
        session
            .simple_query("UPDATE t SET v='waited' WHERE id=1")
            .await
            .expect("update completes once the lock is released");
        session.simple_query("COMMIT").await.expect("commit");
    });

    until(&engine, |running| {
        running
            .iter()
            .any(|entry| entry.statement.contains("v='waited'"))
    })
    .await;

    let stuck = reports(&engine, TINY);
    assert!(stuck.len() == 1, "{stuck:?}");
    let stuck = &stuck[0];
    assert!(stuck.backend_pid == BLOCKED_PID);
    assert!(stuck.statement == "UPDATE t SET v='waited' WHERE id=1");
    assert!(stuck.transaction == TransactionActivity::InTransaction);
    assert!(!stuck.repeated);

    // Reporting is once per statement: a second scan under the same repeat
    // interval stays quiet, so a wedged run does not flood the log.
    assert!(reports(&engine, TINY) == vec![]);

    // And nothing the watchdog did interrupted the statement — releasing the
    // lock still lets it finish.
    holder.simple_query("COMMIT").await.expect("release");
    tokio::time::timeout(Duration::from_secs(10), blocked)
        .await
        .expect("the blocked statement was not cancelled")
        .expect("join");
    assert!(engine.statement_registry().in_flight() == vec![]);
}

/// Two backends stuck at once are two entries: a report that collapsed them
/// would hide half of a wedge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_sessions_each_get_their_own_entry() {
    let engine = Arc::new(SqlEngine::new());
    let mut setup = engine.connect();
    setup
        .simple_query("CREATE TABLE t (id int4, v text)")
        .await
        .expect("create");
    setup
        .simple_query("INSERT INTO t VALUES (1,'orig')")
        .await
        .expect("insert");

    let mut holder = engine.connect();
    holder.simple_query("BEGIN").await.expect("begin");
    holder
        .simple_query("UPDATE t SET v='held' WHERE id=1")
        .await
        .expect("lock the row");

    let mut waiters = Vec::new();
    for (pid, label) in [(9001, "first"), (9002, "second")] {
        let engine = Arc::clone(&engine);
        waiters.push(tokio::spawn(async move {
            let mut session = engine.connect_with_pid(pid);
            session
                .simple_query(&format!("UPDATE t SET v='{label}' WHERE id=1"))
                .await
                .expect("update completes once the lock is released");
        }));
    }

    until(&engine, |running| running.len() == 2).await;

    let stuck = reports(&engine, TINY);
    let mut pids = stuck
        .iter()
        .map(|entry| entry.backend_pid)
        .collect::<Vec<_>>();
    pids.sort_unstable();
    assert!(pids == vec![9001, 9002], "{stuck:?}");
    assert!(
        stuck
            .iter()
            .all(|entry| entry.transaction == TransactionActivity::Idle),
        "{stuck:?}"
    );

    holder.simple_query("COMMIT").await.expect("release");
    for waiter in waiters {
        tokio::time::timeout(Duration::from_secs(10), waiter)
            .await
            .expect("a waiter was not cancelled")
            .expect("join");
    }
    assert!(engine.statement_registry().in_flight() == vec![]);
}

/// The extended protocol runs its statements somewhere else entirely, and a
/// hang there is just as invisible; `Execute` registers the text its portal was
/// bound from.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_extended_protocol_execute_registers_its_prepared_text() {
    let engine = Arc::new(SqlEngine::new());
    let mut setup = engine.connect();
    setup
        .simple_query("CREATE TABLE t (id int4, v text)")
        .await
        .expect("create");
    setup
        .simple_query("INSERT INTO t VALUES (1,'orig')")
        .await
        .expect("insert");

    let mut holder = engine.connect();
    holder.simple_query("BEGIN").await.expect("begin");
    holder
        .simple_query("UPDATE t SET v='held' WHERE id=1")
        .await
        .expect("lock the row");

    let blocked_engine = Arc::clone(&engine);
    let blocked = tokio::spawn(async move {
        let mut session = blocked_engine.connect();
        session
            .parse("s", "UPDATE t SET v='extended' WHERE id=1", &[])
            .await
            .expect("parse");
        session.bind("p", "s", &[], &[]).await.expect("bind");
        session.execute("p", 0).await.expect("execute");
    });

    until(&engine, |running| {
        running
            .iter()
            .any(|entry| entry.statement == "UPDATE t SET v='extended' WHERE id=1")
    })
    .await;

    holder.simple_query("COMMIT").await.expect("release");
    tokio::time::timeout(Duration::from_secs(10), blocked)
        .await
        .expect("the blocked statement was not cancelled")
        .expect("join");
    assert!(engine.statement_registry().in_flight() == vec![]);
}
