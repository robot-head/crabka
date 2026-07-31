//! The foreign-key locking protocol, proved with two live sessions.
//!
//! `PostgreSQL`'s referential triggers take `FOR KEY SHARE` on the referenced
//! row. This engine's lock manager has only `Shared` and `Exclusive`, and adds
//! no third mode: instead both sides of a foreign key name the *same key-lock
//! identity* — the referenced index's entry prefix for the key value. The child
//! side (INSERT, or an UPDATE that changes the referencing key) takes it
//! `Shared`; the parent side (DELETE, or an UPDATE that changes the referenced
//! key) takes it `Exclusive`.
//!
//! Three properties follow, and each has a test below that a naive
//! implementation fails:
//!
//! 1. Many children of ONE parent key do not contend — locking the parent *row*
//!    exclusively would convoy them, which is the whole thing this design
//!    exists to avoid.
//! 2. A NON-KEY update of the parent is not blocked by a concurrent child
//!    insert, because "referenced key unchanged, so queue nothing" fires first
//!    and the parent never reaches the key lock.
//! 3. A parent DELETE *does* block behind an uncommitted child insert, and its
//!    outcome follows the child's — the reference is real only if the child
//!    commits.
//!
//! Plus the two corollaries: key locks and row locks share one wait-for graph,
//! so a cycle spanning both is reported as `40P01` with no new machinery; and a
//! shared parent-key hold composes with the child's own exclusive unique-key
//! lock rather than deadlocking against it.
//!
//! Every SQLSTATE is what a live `PostgreSQL` 18.4 reports, including the split
//! between `23503` (parent side under `NO ACTION`) and `23001` (under
//! `RESTRICT`).

use std::{sync::Arc, time::Duration};

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

/// How long a statement gets to prove it does NOT block. Every statement here
/// is in-process and answers in well under a millisecond, so the only way to
/// spend this budget is to be queued on a lock — but it is wide enough that a
/// loaded runner descheduling the task cannot fake that.
const NON_BLOCKING: Duration = Duration::from_secs(5);

/// How long a statement that MUST block is given to (wrongly) finish early.
/// A false pass here means a real block was slower than the window, which is
/// harmless; a false failure would need the statement to complete, which is the
/// defect being hunted.
const SETTLE: Duration = Duration::from_millis(300);

/// The ceiling on a wait that should end as soon as the holder resolves. Only
/// ever reached when the protocol hangs.
const HANG: Duration = Duration::from_secs(10);

async fn run(s: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    s.simple_query(sql).await.expect("statement should succeed")
}

fn tag_of(r: &QueryResult) -> &str {
    match r {
        QueryResult::Command { tag } | QueryResult::Rows { tag, .. } => tag,
        o @ QueryResult::Empty => panic!("expected a tagged result, got {o:?}"),
    }
}

fn rows_text(r: &QueryResult) -> Vec<Vec<Option<String>>> {
    match r {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| {
                        cell.as_ref()
                            .map(|c: &Cell| String::from_utf8(c.text.to_vec()).expect("utf8"))
                    })
                    .collect()
            })
            .collect(),
        o => panic!("expected Rows, got {o:?}"),
    }
}

/// Read committed state through a fresh session, so an open transaction's locks
/// cannot colour the answer.
async fn rows_of(engine: &SqlEngine, sql: &str) -> Vec<Vec<Option<String>>> {
    let mut s = engine.connect();
    rows_text(&run(&mut s, sql).await[0])
}

fn row(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_string())).collect()
}

/// The outcome of a statement: `Ok` carries the command tag it completed with,
/// `Err` the SQLSTATE it was refused with.
type Outcome = Result<String, String>;

async fn outcome_of(s: &mut SqlSession, sql: &str) -> Outcome {
    s.simple_query(sql)
        .await
        .map(|r| tag_of(&r[0]).to_string())
        .map_err(|e| e.code)
}

/// An expected [`Outcome`], written as the table rows spell it: `Ok` a command
/// tag, `Err` a SQLSTATE.
fn expected(outcome: Result<&str, &str>) -> Outcome {
    match outcome {
        Ok(tag) => Ok(tag.to_string()),
        Err(code) => Err(code.to_string()),
    }
}

/// A parent `p` holding keys 1 and 2 plus a non-key column, and a child `c`
/// whose foreign key is spelled `tail`.
async fn fk_engine(tail: &str) -> Arc<SqlEngine> {
    let engine = Arc::new(SqlEngine::new());
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE p (id int4 PRIMARY KEY, v text)").await;
    run(
        &mut s,
        &format!("CREATE TABLE c (id int4 PRIMARY KEY, a int4 REFERENCES p (id) {tail})"),
    )
    .await;
    run(&mut s, "INSERT INTO p VALUES (1, 'one'), (2, 'two')").await;
    engine
}

/// (1) Two sessions inserting children of the SAME parent key do not contend:
/// both take the parent key `Shared`, so the second commits while the first's
/// transaction is still open. A design that took the parent ROW exclusively —
/// or the key exclusively — would serialize them into the convoy this protocol
/// exists to avoid, and the second insert would sit here until the timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_children_of_one_parent_key_do_not_contend() {
    let engine = fk_engine("").await;

    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    // SHARED on p's key (1), held until COMMIT.
    run(&mut t1, "INSERT INTO c VALUES (10, 1)").await;

    let e2 = Arc::clone(&engine);
    let t2 = tokio::spawn(async move {
        let mut s = e2.connect();
        run(&mut s, "BEGIN").await;
        // A different child row, the SAME parent key: shared/shared.
        run(&mut s, "INSERT INTO c VALUES (20, 1)").await;
        run(&mut s, "COMMIT").await;
    });

    tokio::time::timeout(NON_BLOCKING, t2)
        .await
        .expect("a second child of the same parent key must not wait for the first")
        .expect("t2 join");

    run(&mut t1, "COMMIT").await;
    assert!(
        rows_of(&engine, "SELECT id, a FROM c ORDER BY id").await
            == vec![row(&["10", "1"]), row(&["20", "1"])]
    );
}

/// (2) `PostgreSQL`'s headline `FOR KEY SHARE` property: a NON-KEY update of the
/// parent is not blocked by an uncommitted child insert that references it. The
/// child holds the key `Shared`; the parent update leaves the referenced key
/// alone, so "referenced key unchanged, so queue nothing" fires and it never
/// asks for the key lock at all. Had the parent side locked unconditionally,
/// its `Exclusive` request would queue behind the child and this would time out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_key_parent_update_is_not_blocked_by_an_open_child_insert() {
    let engine = fk_engine("").await;

    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    // SHARED on p's key (1).
    run(&mut t1, "INSERT INTO c VALUES (10, 1)").await;

    let e2 = Arc::clone(&engine);
    let t2 = tokio::spawn(async move {
        let mut s = e2.connect();
        run(&mut s, "BEGIN").await;
        // Touches only `v`: the referenced key (id) is unchanged.
        run(&mut s, "UPDATE p SET v = 'renamed' WHERE id = 1").await;
        run(&mut s, "COMMIT").await;
    });

    tokio::time::timeout(NON_BLOCKING, t2)
        .await
        .expect("a non-key parent update must not wait for a child holding the key SHARED")
        .expect("t2 join");

    run(&mut t1, "COMMIT").await;
    assert!(
        rows_of(&engine, "SELECT id, v FROM p ORDER BY id").await
            == vec![row(&["1", "renamed"]), row(&["2", "two"])]
    );
    assert!(rows_of(&engine, "SELECT id, a FROM c").await == vec![row(&["10", "1"])]);
}

/// (2′) The same property from the other side: an open non-key parent update
/// holds only that parent row's ROW lock, and the child's check never takes a
/// row lock on the parent — it probes committed state under the key lock — so a
/// child insert referencing that very row commits while the update is open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn child_insert_is_not_blocked_by_an_open_non_key_parent_update() {
    let engine = fk_engine("").await;

    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    // Row lock on p's row only; the key is untouched, so no key lock.
    run(&mut t1, "UPDATE p SET v = 'renamed' WHERE id = 1").await;

    let e2 = Arc::clone(&engine);
    let t2 = tokio::spawn(async move {
        let mut s = e2.connect();
        run(&mut s, "BEGIN").await;
        run(&mut s, "INSERT INTO c VALUES (10, 1)").await;
        run(&mut s, "COMMIT").await;
    });

    tokio::time::timeout(NON_BLOCKING, t2)
        .await
        .expect("a child insert must not wait for a non-key update of the parent row")
        .expect("t2 join");

    run(&mut t1, "COMMIT").await;
    assert!(rows_of(&engine, "SELECT id, a FROM c").await == vec![row(&["10", "1"])]);
    assert!(
        rows_of(&engine, "SELECT id, v FROM p ORDER BY id").await
            == vec![row(&["1", "renamed"]), row(&["2", "two"])]
    );
}

/// (2″) The control that gives (2) its meaning. Change the same parent row's
/// REFERENCED key instead of its payload and the very same statement shape now
/// queues on the very same key the child holds: the lock is real and contended,
/// so (2) passing is the "key unchanged, so queue nothing" rule firing, not a
/// lock that was never taken by either side.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_changing_parent_update_waits_on_an_open_child_insert() {
    for (holder_outcome, outcome, parents) in [
        ("COMMIT", Err("23503"), vec!["1", "2"]),
        ("ROLLBACK", Ok("UPDATE 1"), vec!["2", "3"]),
    ] {
        let outcome = expected(outcome);
        let engine = fk_engine("").await;

        let mut t1 = engine.connect();
        run(&mut t1, "BEGIN").await;
        // SHARED on p's key (1).
        run(&mut t1, "INSERT INTO c VALUES (10, 1)").await;

        let e2 = Arc::clone(&engine);
        let t2 = tokio::spawn(async move {
            let mut s = e2.connect();
            // Moves the referenced key off 1: EXCLUSIVE on p's key (1).
            outcome_of(&mut s, "UPDATE p SET id = 3 WHERE id = 1").await
        });

        tokio::time::sleep(SETTLE).await;
        assert!(!t2.is_finished(), "{holder_outcome}");

        run(&mut t1, holder_outcome).await;
        let actual = tokio::time::timeout(HANG, t2)
            .await
            .expect("the key-changing update must wake as soon as the child resolves")
            .expect("t2 join");
        assert!(actual == outcome, "{holder_outcome}");

        let expected_parents: Vec<Vec<Option<String>>> =
            parents.iter().map(|id| row(&[id])).collect();
        assert!(
            rows_of(&engine, "SELECT id FROM p ORDER BY id").await == expected_parents,
            "{holder_outcome}"
        );
    }
}

/// (3) A parent DELETE DOES block behind an uncommitted child insert — the
/// child holds the key `Shared`, the delete wants it `Exclusive` — and its
/// outcome follows the child's. A committed child makes the reference real and
/// the delete fails; a rolled-back child leaves nothing behind and it succeeds.
///
/// The parent-side SQLSTATE differs by action, as `PostgreSQL` 18.4 reports it:
/// `23503` under `NO ACTION`, `23001` (`restrict_violation`) under `RESTRICT`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parent_delete_waits_on_an_open_child_insert_then_follows_its_outcome() {
    for (tail, holder_outcome, outcome) in [
        ("", "COMMIT", Err("23503")),
        ("", "ROLLBACK", Ok("DELETE 1")),
        ("ON DELETE RESTRICT", "COMMIT", Err("23001")),
        ("ON DELETE RESTRICT", "ROLLBACK", Ok("DELETE 1")),
    ] {
        let outcome = expected(outcome);
        let case = format!("REFERENCES p (id) {tail} / child {holder_outcome}");
        let engine = fk_engine(tail).await;

        let mut t1 = engine.connect();
        run(&mut t1, "BEGIN").await;
        // SHARED on p's key (1), held until the holder resolves.
        run(&mut t1, "INSERT INTO c VALUES (10, 1)").await;

        let e2 = Arc::clone(&engine);
        let t2 = tokio::spawn(async move {
            let mut s = e2.connect();
            outcome_of(&mut s, "DELETE FROM p WHERE id = 1").await
        });

        // Nothing is decided while the child is in flight: the delete can
        // neither succeed (the child may commit) nor fail (it may not).
        tokio::time::sleep(SETTLE).await;
        assert!(!t2.is_finished(), "{case}");

        run(&mut t1, holder_outcome).await;
        let actual = tokio::time::timeout(HANG, t2)
            .await
            .expect("the delete must wake as soon as the child resolves")
            .expect("t2 join");
        assert!(actual == outcome, "{case}");

        // The parent row survives exactly when the delete was refused.
        let surviving = if outcome.is_ok() {
            vec![row(&["2"])]
        } else {
            vec![row(&["1"]), row(&["2"])]
        };
        assert!(
            rows_of(&engine, "SELECT id FROM p ORDER BY id").await == surviving,
            "{case}"
        );
    }
}

/// A deadlock cycle spanning a ROW lock and a foreign-key KEY lock resolves as
/// exactly one `40P01` instead of hanging — key locks and row locks share one
/// wait-for graph, so no new detection machinery is needed.
///
/// T1 changes a child's referencing key to 1, taking p's key (1) `Shared`, then
/// wants p's row 2 — which T2 holds from a non-key update. T2 then deletes p's
/// row 1, which wants that key `Exclusive`. Neither edge can be resolved
/// without the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadlock_across_a_key_lock_and_a_row_lock_yields_one_40p01() {
    let engine = fk_engine("").await;
    {
        let mut s = engine.connect();
        run(&mut s, "INSERT INTO c VALUES (10, 2)").await;
    }

    let (tx1_ready, rx1_ready) = tokio::sync::oneshot::channel::<()>();
    let (tx2_ready, rx2_ready) = tokio::sync::oneshot::channel::<()>();

    let e1 = Arc::clone(&engine);
    let h1 = tokio::spawn(async move {
        let mut s = e1.connect();
        run(&mut s, "BEGIN").await;
        // Child key change 2 -> 1: SHARED on p's key (1), plus c's row lock.
        run(&mut s, "UPDATE c SET a = 1 WHERE id = 10").await;
        tx1_ready.send(()).expect("send t1 ready");
        rx2_ready.await.expect("recv t2 ready");
        // Wants p's row 2 — T2 holds it.
        let outcome = outcome_of(&mut s, "UPDATE p SET v = 'from1' WHERE id = 2").await;
        let _ = s.simple_query("ROLLBACK").await;
        outcome
    });

    let e2 = Arc::clone(&engine);
    let h2 = tokio::spawn(async move {
        let mut s = e2.connect();
        run(&mut s, "BEGIN").await;
        // Non-key update: p's row lock on row 2, no key lock.
        run(&mut s, "UPDATE p SET v = 'from2' WHERE id = 2").await;
        tx2_ready.send(()).expect("send t2 ready");
        rx1_ready.await.expect("recv t1 ready");
        // Wants p's key (1) EXCLUSIVE — T1 holds it SHARED. Cycle.
        let outcome = outcome_of(&mut s, "DELETE FROM p WHERE id = 1").await;
        let _ = s.simple_query("ROLLBACK").await;
        outcome
    });

    let r1 = tokio::time::timeout(HANG, h1)
        .await
        .expect("h1 did not hang")
        .expect("h1 join");
    let r2 = tokio::time::timeout(HANG, h2)
        .await
        .expect("h2 did not hang")
        .expect("h2 join");

    let outcomes = [r1, r2];
    let deadlocks = outcomes
        .iter()
        .filter(|o| o.as_ref().err().map(String::as_str) == Some("40P01"))
        .count();
    assert!(deadlocks == 1, "expected exactly one 40P01: {outcomes:?}");
    assert!(
        outcomes.iter().filter(|o| o.is_ok()).count() == 1,
        "expected exactly one survivor: {outcomes:?}"
    );
}

/// Two sessions inserting the SAME child row race on the child's own key while
/// both hold the parent key `Shared`. The shared hold neither blocks the other
/// session nor turns the unique-key contention into a deadlock: the loser
/// queues on the child's `Exclusive` key lock and follows the winner's outcome —
/// `23505` once the winner commits, its own row once the winner rolls back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_child_rows_serialize_on_the_child_key_under_a_shared_parent_key() {
    for (holder_outcome, outcome, committed_a) in [
        ("COMMIT", Err("23505"), "1"),
        ("ROLLBACK", Ok("INSERT 0 1"), "2"),
    ] {
        let outcome = expected(outcome);
        let engine = fk_engine("").await;

        let mut t1 = engine.connect();
        run(&mut t1, "BEGIN").await;
        // EXCLUSIVE on c's key (10) and SHARED on p's key (1).
        run(&mut t1, "INSERT INTO c VALUES (10, 1)").await;

        let e2 = Arc::clone(&engine);
        let t2 = tokio::spawn(async move {
            let mut s = e2.connect();
            // Same child key, a different parent key: the parent side is
            // shared/shared, so only c's key can hold this up.
            outcome_of(&mut s, "INSERT INTO c VALUES (10, 2)").await
        });

        tokio::time::sleep(SETTLE).await;
        assert!(!t2.is_finished(), "{holder_outcome}");

        run(&mut t1, holder_outcome).await;
        let actual = tokio::time::timeout(HANG, t2)
            .await
            .expect("the duplicate must wake as soon as the holder resolves")
            .expect("t2 join");
        assert!(actual == outcome, "{holder_outcome}");

        assert!(
            rows_of(&engine, "SELECT id, a FROM c ORDER BY id").await
                == vec![row(&["10", committed_a])],
            "{holder_outcome}"
        );
    }
}
