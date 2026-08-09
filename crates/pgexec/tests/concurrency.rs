use std::{sync::Arc, time::Duration};

use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn run(s: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    s.simple_query(sql).await.expect("ok")
}

fn tag_of(r: &QueryResult) -> &str {
    match r {
        QueryResult::Command { tag } | QueryResult::Rows { tag, .. } => tag,
        o @ QueryResult::Empty => panic!("{o:?}"),
    }
}

fn col0(r: &QueryResult) -> Vec<Option<String>> {
    match r {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .map(|row| {
                row[0]
                    .as_ref()
                    .map(|c: &Cell| String::from_utf8(c.text.to_vec()).expect("utf8"))
            })
            .collect(),
        o => panic!("{o:?}"),
    }
}

async fn err_code(s: &mut SqlSession, sql: &str) -> String {
    s.simple_query(sql).await.expect_err("expected error").code
}

// ---------------------------------------------------------------------------
// Existing test
// ---------------------------------------------------------------------------

/// Regression test for the concurrent-INSERT lost-update bug.
///
/// Before the fix, two concurrent INSERTs both read seq=N, both allocate
/// rowids starting at N, and the second batch's `Put` overwrites the first's
/// rows at the same keys. That is silent data loss.
///
/// After the fix (`write_lock` serializes all writes), every read-modify-write
/// on the sequence is atomic from the perspective of other writers, so all N
/// rows survive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_inserts_do_not_lose_rows() {
    const N: usize = 50;

    let engine = Arc::new(SqlEngine::new());
    engine
        .connect()
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");

    // Fan out N concurrent single-row inserts into the same table.
    let mut handles = Vec::new();
    for i in 0..N {
        let e = Arc::clone(&engine);
        handles.push(tokio::spawn(async move {
            e.connect()
                .simple_query(&format!("INSERT INTO t VALUES ({i})"))
                .await
                .expect("insert");
        }));
    }
    for h in handles {
        h.await.expect("join");
    }

    // All N rows must be present — none overwritten by a colliding rowid.
    let mut results = engine
        .connect()
        .simple_query("SELECT id FROM t")
        .await
        .expect("select");
    let rows = match results.remove(0) {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got {other:?}"),
    };
    assert_eq!(
        rows.len(),
        N,
        "concurrent inserts lost rows (rowid collision): got {} of {}",
        rows.len(),
        N
    );
}

// ---------------------------------------------------------------------------
// SP6 concurrent-writer tests
// ---------------------------------------------------------------------------

/// T1 holds a row lock with UPDATE, and T2's UPDATE on the same row blocks.
///
/// When T1 commits, READ COMMITTED semantics re-find the updated row and apply
/// T2's change on top. The tag is "UPDATE 1" and the final value is T2's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_row_update_blocks_then_read_committed_refinds() {
    let engine = Arc::new(SqlEngine::new());
    {
        let mut s = engine.connect();
        run(&mut s, "CREATE TABLE t (id int4, v text)").await;
        run(&mut s, "INSERT INTO t VALUES (1,'orig')").await;
    }

    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    run(&mut t1, "UPDATE t SET v='a' WHERE id=1").await; // holds the row lock

    let e2 = Arc::clone(&engine);
    let t2 = tokio::spawn(async move {
        let mut s = e2.connect();
        run(&mut s, "BEGIN").await;
        // blocks until T1 releases, then EvalPlanQual re-finds row at v='a'
        let r = run(&mut s, "UPDATE t SET v='b' WHERE id=1").await;
        run(&mut s, "COMMIT").await;
        tag_of(&r[0]).to_string()
    });

    // let T2 reach the blocking acquire
    tokio::time::sleep(Duration::from_millis(100)).await;
    run(&mut t1, "COMMIT").await; // release the lock

    let tag = tokio::time::timeout(Duration::from_secs(10), t2)
        .await
        .expect("t2 did not hang")
        .expect("t2 join");
    assert_eq!(tag, "UPDATE 1");

    let mut s = engine.connect();
    assert_eq!(
        col0(&run(&mut s, "SELECT v FROM t WHERE id=1").await[0]),
        vec![Some("b".into())]
    );
}

/// Under REPEATABLE READ, T2 fixes its snapshot at BEGIN (before T1 commits).
///
/// After T1 commits and T2's blocked UPDATE wakes, the freshly committed row
/// differs from T2's snapshot, so the engine reports a serialization failure
/// (40001).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeatable_read_conflict_is_40001() {
    let engine = Arc::new(SqlEngine::new());
    {
        let mut s = engine.connect();
        run(&mut s, "CREATE TABLE t (id int4, v text)").await;
        run(&mut s, "INSERT INTO t VALUES (1,'orig')").await;
    }

    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    run(&mut t1, "UPDATE t SET v='a' WHERE id=1").await; // holds the row lock

    // T2 begins its RR transaction and takes its snapshot BEFORE T1 commits.
    // Use a oneshot so T1 waits until T2 has snapshotted AND is blocked.
    let (tx_ready, rx_ready) = tokio::sync::oneshot::channel::<()>();
    let (tx_go, rx_go) = tokio::sync::oneshot::channel::<()>();

    let e2 = Arc::clone(&engine);
    let t2 = tokio::spawn(async move {
        let mut s = e2.connect();
        run(&mut s, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
        // Fix the snapshot: read the row now (before T1 commits).
        run(&mut s, "SELECT v FROM t WHERE id=1").await;
        // Signal T1 that we have snapshotted.
        tx_ready.send(()).expect("send ready");
        // Wait for T1's signal to proceed with the conflicting UPDATE.
        rx_go.await.expect("recv go");
        // This UPDATE blocks on T1's lock, wakes after T1 commits, then detects
        // that the row was concurrently modified → 40001.
        let code = err_code(&mut s, "UPDATE t SET v='b' WHERE id=1").await;
        // Roll back the failed transaction.
        run(&mut s, "ROLLBACK").await;
        code
    });

    // Wait until T2 has taken its snapshot.
    rx_ready.await.expect("recv ready");
    // Signal T2 to start its conflicting UPDATE (which will block on T1's lock).
    tx_go.send(()).expect("send go");
    // Let T2 reach the blocking acquire before we commit.
    tokio::time::sleep(Duration::from_millis(100)).await;
    run(&mut t1, "COMMIT").await; // release; T2 wakes and sees a stale snapshot

    let code = tokio::time::timeout(Duration::from_secs(10), t2)
        .await
        .expect("t2 did not hang")
        .expect("t2 join");
    assert_eq!(code, "40001");
}

/// When the lock holder aborts (ROLLBACK), the waiting UPDATE wakes and applies
/// its change to the ORIGINAL row (as if the conflicting transaction never ran).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocker_abort_lets_waiter_proceed() {
    let engine = Arc::new(SqlEngine::new());
    {
        let mut s = engine.connect();
        run(&mut s, "CREATE TABLE t (id int4, v text)").await;
        run(&mut s, "INSERT INTO t VALUES (1,'orig')").await;
    }

    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    run(&mut t1, "UPDATE t SET v='a' WHERE id=1").await; // holds the row lock

    let e2 = Arc::clone(&engine);
    let t2 = tokio::spawn(async move {
        let mut s = e2.connect();
        run(&mut s, "BEGIN").await;
        // blocks until T1 releases (via ROLLBACK), then re-evaluates the row;
        // T1's change was aborted so the original row ('orig') is visible; T2
        // applies its change on top of the original.
        let r = run(&mut s, "UPDATE t SET v='c' WHERE id=1").await;
        run(&mut s, "COMMIT").await;
        tag_of(&r[0]).to_string()
    });

    // let T2 reach the blocking acquire
    tokio::time::sleep(Duration::from_millis(100)).await;
    run(&mut t1, "ROLLBACK").await; // abort; T2 wakes and sees the original row

    let tag = tokio::time::timeout(Duration::from_secs(10), t2)
        .await
        .expect("t2 did not hang")
        .expect("t2 join");
    assert_eq!(tag, "UPDATE 1");

    // Final value must be T2's: T1's change was rolled back.
    let mut s = engine.connect();
    assert_eq!(
        col0(&run(&mut s, "SELECT v FROM t WHERE id=1").await[0]),
        vec![Some("c".into())]
    );
}

/// Updates on different rows must not block each other.
///
/// T1 opens a transaction, updates row 1, and then deliberately stays open.
/// It does NOT commit. While T1 still holds the lock on row 1, T2 updates row 2
/// and commits, and it must finish within a short timeout. If the WHERE filter
/// were applied after locking (the bug), T1's UPDATE would lock ALL rows,
/// including row 2. T2 would then block here indefinitely and the timeout would
/// fire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn different_rows_run_concurrently() {
    let engine = Arc::new(SqlEngine::new());
    {
        let mut s = engine.connect();
        run(&mut s, "CREATE TABLE t (id int4, v text)").await;
        run(&mut s, "INSERT INTO t VALUES (1,'a'),(2,'b')").await;
    }

    // T1: BEGIN + UPDATE id=1, then stay open (hold the row 1 lock).
    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    run(&mut t1, "UPDATE t SET v='x' WHERE id=1").await;

    // T2: update a DIFFERENT row; must complete WITHOUT waiting for T1.
    let e2 = Arc::clone(&engine);
    let t2 = tokio::spawn(async move {
        let mut s = e2.connect();
        run(&mut s, "BEGIN").await;
        run(&mut s, "UPDATE t SET v='y' WHERE id=2").await;
        run(&mut s, "COMMIT").await;
    });

    // T2 must finish well within 5 s while T1 is still open. Without the fix
    // (filter before lock), T1 would hold row 2's lock and this times out.
    tokio::time::timeout(Duration::from_secs(5), t2)
        .await
        .expect("T2 must not block on T1 (different rows — filter must precede lock)")
        .expect("t2 join");

    // Now commit T1 and verify both writes are durable.
    run(&mut t1, "COMMIT").await;

    let mut s = engine.connect();
    assert_eq!(
        col0(&run(&mut s, "SELECT v FROM t ORDER BY id").await[0]),
        vec![Some("x".into()), Some("y".into())]
    );
}

/// SELECT FOR UPDATE takes an exclusive lock; a concurrent UPDATE on the same
/// row blocks until the FOR UPDATE holder commits, then succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn for_update_blocks_concurrent_update() {
    let engine = Arc::new(SqlEngine::new());
    {
        let mut s = engine.connect();
        run(&mut s, "CREATE TABLE t (id int4, v text)").await;
        run(&mut s, "INSERT INTO t VALUES (1,'orig')").await;
    }

    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    run(&mut t1, "SELECT v FROM t WHERE id=1 FOR UPDATE").await; // holds exclusive lock

    let e2 = Arc::clone(&engine);
    let t2 = tokio::spawn(async move {
        let mut s = e2.connect();
        run(&mut s, "BEGIN").await;
        // blocks on T1's FOR UPDATE lock
        let r = run(&mut s, "UPDATE t SET v='new' WHERE id=1").await;
        run(&mut s, "COMMIT").await;
        tag_of(&r[0]).to_string()
    });

    // let T2 reach the blocking acquire
    tokio::time::sleep(Duration::from_millis(100)).await;
    run(&mut t1, "COMMIT").await; // release FOR UPDATE lock

    let tag = tokio::time::timeout(Duration::from_secs(10), t2)
        .await
        .expect("t2 did not hang")
        .expect("t2 join");
    assert_eq!(tag, "UPDATE 1");

    let mut s = engine.connect();
    assert_eq!(
        col0(&run(&mut s, "SELECT v FROM t WHERE id=1").await[0]),
        vec![Some("new".into())]
    );
}

/// Two FOR SHARE holders coexist (shared locks are compatible). A third
/// session's UPDATE (exclusive) blocks until both share holders commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn for_share_coexists_but_blocks_update() {
    let engine = Arc::new(SqlEngine::new());
    {
        let mut s = engine.connect();
        run(&mut s, "CREATE TABLE t (id int4, v text)").await;
        run(&mut s, "INSERT INTO t VALUES (1,'orig')").await;
    }

    // Both FOR SHARE sessions hold shared locks concurrently — neither blocks.
    let mut sh1 = engine.connect();
    run(&mut sh1, "BEGIN").await;
    run(&mut sh1, "SELECT v FROM t WHERE id=1 FOR SHARE").await;

    let mut sh2 = engine.connect();
    run(&mut sh2, "BEGIN").await;
    run(&mut sh2, "SELECT v FROM t WHERE id=1 FOR SHARE").await;

    // The UPDATE (exclusive) must block while both share holders are alive.
    let e3 = Arc::clone(&engine);
    let updater = tokio::spawn(async move {
        let mut s = e3.connect();
        run(&mut s, "BEGIN").await;
        // blocks until both sh1 and sh2 commit
        let r = run(&mut s, "UPDATE t SET v='new' WHERE id=1").await;
        run(&mut s, "COMMIT").await;
        tag_of(&r[0]).to_string()
    });

    // let the updater reach the blocking acquire
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Release the first shared lock — updater must still be blocked (sh2 holds).
    run(&mut sh1, "COMMIT").await;

    // Brief pause to confirm the updater hasn't sneaked through yet; then release sh2.
    tokio::time::sleep(Duration::from_millis(50)).await;
    run(&mut sh2, "COMMIT").await;

    let tag = tokio::time::timeout(Duration::from_secs(10), updater)
        .await
        .expect("updater did not hang")
        .expect("updater join");
    assert_eq!(tag, "UPDATE 1");

    let mut s = engine.connect();
    assert_eq!(
        col0(&run(&mut s, "SELECT v FROM t WHERE id=1").await[0]),
        vec![Some("new".into())]
    );
}

/// Deadlock detection: T1 locks table `a`, then tries table `b`; T2 locks
/// table `b`, then tries table `a`. The engine must detect the cycle and abort
/// exactly one transaction with 40P01; the other proceeds.
///
/// The test uses two single-row tables, so each UPDATE/FOR-UPDATE touches
/// exactly one row and no single scan locks across tables. Timeouts wrap both
/// spawned tasks, so a regression FAILS and does not hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadlock_yields_one_40p01() {
    let engine = Arc::new(SqlEngine::new());
    {
        let mut s = engine.connect();
        run(&mut s, "CREATE TABLE a (v text)").await;
        run(&mut s, "CREATE TABLE b (v text)").await;
        run(&mut s, "INSERT INTO a VALUES ('a')").await;
        run(&mut s, "INSERT INTO b VALUES ('b')").await;
    }

    // Oneshots so each transaction signals "I have my first lock" before
    // attempting the second (which conflicts with the other's first lock).
    let (tx1_ready, rx1_ready) = tokio::sync::oneshot::channel::<()>();
    let (tx2_ready, rx2_ready) = tokio::sync::oneshot::channel::<()>();

    let e1 = Arc::clone(&engine);
    let h1 = tokio::spawn(async move {
        let mut s = e1.connect();
        run(&mut s, "BEGIN").await;
        // Lock table a (single row → locks exactly one row).
        run(&mut s, "SELECT v FROM a FOR UPDATE").await;
        tx1_ready.send(()).expect("send t1 ready");
        rx2_ready.await.expect("recv t2 ready"); // wait for T2 to lock table b
        // Now try table b — T2 holds it → potential deadlock.
        let result = s.simple_query("SELECT v FROM b FOR UPDATE").await;
        // Whether we win or lose, clean up.
        let _ = s.simple_query("ROLLBACK").await;
        result.map(|_| ()).map_err(|e| e.code)
    });

    let e2 = Arc::clone(&engine);
    let h2 = tokio::spawn(async move {
        let mut s = e2.connect();
        run(&mut s, "BEGIN").await;
        // Lock table b (single row → locks exactly one row).
        run(&mut s, "SELECT v FROM b FOR UPDATE").await;
        tx2_ready.send(()).expect("send t2 ready");
        rx1_ready.await.expect("recv t1 ready"); // wait for T1 to lock table a
        // Now try table a — T1 holds it → deadlock cycle detected.
        let result = s.simple_query("SELECT v FROM a FOR UPDATE").await;
        let _ = s.simple_query("ROLLBACK").await;
        result.map(|_| ()).map_err(|e| e.code)
    });

    let r1 = tokio::time::timeout(Duration::from_secs(10), h1)
        .await
        .expect("h1 did not hang")
        .expect("h1 join");
    let r2 = tokio::time::timeout(Duration::from_secs(10), h2)
        .await
        .expect("h2 did not hang")
        .expect("h2 join");

    // Exactly one must have gotten 40P01.
    let codes: Vec<Option<String>> = vec![r1.as_ref().err().cloned(), r2.as_ref().err().cloned()];
    let deadlock_count = codes
        .iter()
        .filter(|c| c.as_deref() == Some("40P01"))
        .count();
    let ok_count = [r1.is_ok(), r2.is_ok()].iter().filter(|&&b| b).count();

    assert_eq!(
        deadlock_count, 1,
        "expected exactly one 40P01, got codes: {codes:?}"
    );
    assert_eq!(ok_count, 1, "expected exactly one transaction to succeed");
}

/// End-to-end INSERT storm on a durable (disk-backed) store.
///
/// Block-cached rowid allocation must hand every concurrent statement a
/// disjoint range, so the whole-table read-back count matches the inserted
/// count exactly. A shared rowid would make one row's version shadow the
/// other's, and the count would come up short. The row volume crosses several
/// allocation-block boundaries (blocks of 1024), so block extension itself runs
/// under contention.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_insert_storm_lands_every_row_exactly_once() {
    const TASKS: usize = 8;
    const STATEMENTS_PER_TASK: usize = 20;
    const ROWS_PER_STATEMENT: usize = 8;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = Arc::new(SqlEngine::open(dir.path()).expect("open"));
    engine
        .connect()
        .simple_query("CREATE TABLE storm (id int4)")
        .await
        .expect("create");

    let mut tasks = Vec::new();
    for task in 0..TASKS {
        let e = Arc::clone(&engine);
        tasks.push(tokio::spawn(async move {
            let mut s = e.connect();
            for statement in 0..STATEMENTS_PER_TASK {
                let base = (task * STATEMENTS_PER_TASK + statement) * ROWS_PER_STATEMENT;
                let values = (0..ROWS_PER_STATEMENT)
                    .map(|i| format!("({})", base + i))
                    .collect::<Vec<_>>()
                    .join(",");
                s.simple_query(&format!("INSERT INTO storm VALUES {values}"))
                    .await
                    .expect("insert");
            }
        }));
    }
    for task in tasks {
        task.await.expect("insert task");
    }

    let mut s = engine.connect();
    let count = col0(&run(&mut s, "SELECT count(*) FROM storm").await[0]);
    let expected = TASKS * STATEMENTS_PER_TASK * ROWS_PER_STATEMENT;
    assert2::assert!(
        count == vec![Some(expected.to_string())],
        "every inserted row must land on a unique rowid"
    );
}
