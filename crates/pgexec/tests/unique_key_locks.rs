//! Per-unique-key lock behavior. DML on tables with unique local indexes no
//! longer serializes engine-wide: writers of the SAME key queue on that key's
//! exclusive lock in the lock manager (probing only after the holder's
//! terminal outcome), writers of different keys run concurrently, and unique
//! CREATE INDEX backfill still excludes in-flight DML via the shared/exclusive
//! `unique_index_lock` gate.

use std::{sync::Arc, time::Duration};

use assert2::assert;
use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

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

/// (a) Two concurrent transactions INSERT the SAME primary key: the second
/// BLOCKS on the key lock (no spurious failure while the first is uncommitted)
/// and, once the first commits, re-probes and gets a unique violation. Exactly
/// one row commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_key_insert_blocks_then_fails_after_holder_commits() {
    let engine = Arc::new(SqlEngine::new());
    {
        let mut s = engine.connect();
        run(&mut s, "CREATE TABLE t (id bigint PRIMARY KEY, v text)").await;
    }

    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    run(&mut t1, "INSERT INTO t VALUES (1, 'first')").await; // holds key lock on id=1

    let e2 = Arc::clone(&engine);
    let t2 = tokio::spawn(async move {
        let mut s = e2.connect();
        run(&mut s, "BEGIN").await;
        // Blocks on T1's key lock; wakes after T1 commits; re-probe sees the
        // committed duplicate.
        let code = err_code(&mut s, "INSERT INTO t VALUES (1, 'second')").await;
        run(&mut s, "ROLLBACK").await;
        code
    });

    // No double-commit AND no spurious early failure: T2 must still be
    // blocked on the key lock while T1's insert is uncommitted.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!t2.is_finished());

    run(&mut t1, "COMMIT").await;
    let code = tokio::time::timeout(Duration::from_secs(10), t2)
        .await
        .expect("t2 did not hang")
        .expect("t2 join");
    assert!(code == "23505");

    let mut s = engine.connect();
    assert!(
        col0(&run(&mut s, "SELECT v FROM t WHERE id = 1").await[0]) == vec![Some("first".into())]
    );
}

/// (b) Concurrent INSERTs of DIFFERENT keys into the same PK table do not
/// block each other: the second transaction commits while the first is still
/// open (under the old engine-wide exclusive gate it would deadlock-by-wait
/// here and time out).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn different_key_inserts_run_concurrently() {
    let engine = Arc::new(SqlEngine::new());
    {
        let mut s = engine.connect();
        run(&mut s, "CREATE TABLE t (id bigint PRIMARY KEY, v text)").await;
    }

    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    run(&mut t1, "INSERT INTO t VALUES (1, 'a')").await; // key lock on id=1, held open

    let e2 = Arc::clone(&engine);
    let t2 = tokio::spawn(async move {
        let mut s = e2.connect();
        run(&mut s, "BEGIN").await;
        run(&mut s, "INSERT INTO t VALUES (2, 'b')").await; // different key: free
        run(&mut s, "COMMIT").await;
    });

    // T2 must commit while T1's transaction is STILL OPEN.
    tokio::time::timeout(Duration::from_secs(5), t2)
        .await
        .expect("different-key insert must not wait for the open transaction")
        .expect("t2 join");

    run(&mut t1, "COMMIT").await;
    let mut s = engine.connect();
    assert!(
        col0(&run(&mut s, "SELECT v FROM t ORDER BY id").await[0])
            == vec![Some("a".into()), Some("b".into())]
    );
}

/// (c) Same-key INSERT where the first writer ROLLS BACK: the blocked second
/// writer wakes, re-probes (the aborted version does not count), and commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_key_insert_proceeds_after_holder_rolls_back() {
    let engine = Arc::new(SqlEngine::new());
    {
        let mut s = engine.connect();
        run(&mut s, "CREATE TABLE t (id bigint PRIMARY KEY, v text)").await;
    }

    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    run(&mut t1, "INSERT INTO t VALUES (1, 'first')").await; // holds key lock on id=1

    let e2 = Arc::clone(&engine);
    let t2 = tokio::spawn(async move {
        let mut s = e2.connect();
        // Autocommit: blocks on T1's key lock, then must SUCCEED.
        let r = run(&mut s, "INSERT INTO t VALUES (1, 'second')").await;
        tag_of(&r[0]).to_string()
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!t2.is_finished());

    run(&mut t1, "ROLLBACK").await;
    let tag = tokio::time::timeout(Duration::from_secs(10), t2)
        .await
        .expect("t2 did not hang")
        .expect("t2 join");
    assert!(tag == "INSERT 0 1");

    let mut s = engine.connect();
    assert!(
        col0(&run(&mut s, "SELECT v FROM t WHERE id = 1").await[0]) == vec![Some("second".into())]
    );
}

/// (d) A deadlock cycle spanning a ROW lock and a UNIQUE-KEY lock resolves
/// with 40P01 for exactly one transaction instead of hanging: T1 locks row 1
/// then wants key 2; T2 locks key 2 (INSERT) then wants row 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadlock_across_row_and_unique_key_locks_yields_one_40p01() {
    let engine = Arc::new(SqlEngine::new());
    {
        let mut s = engine.connect();
        run(&mut s, "CREATE TABLE t (id bigint PRIMARY KEY, v text)").await;
        run(&mut s, "INSERT INTO t VALUES (1, 'seed')").await;
    }

    let (tx1_ready, rx1_ready) = tokio::sync::oneshot::channel::<()>();
    let (tx2_ready, rx2_ready) = tokio::sync::oneshot::channel::<()>();

    let e1 = Arc::clone(&engine);
    let h1 = tokio::spawn(async move {
        let mut s = e1.connect();
        run(&mut s, "BEGIN").await;
        // Row lock on id=1's row (PK unchanged, so no key lock).
        run(&mut s, "UPDATE t SET v = 'x' WHERE id = 1").await;
        tx1_ready.send(()).expect("send t1 ready");
        rx2_ready.await.expect("recv t2 ready");
        // Key lock on id=2 — T2 holds it.
        let result = s.simple_query("INSERT INTO t VALUES (2, 'from1')").await;
        let _ = s.simple_query("ROLLBACK").await;
        result.map(|_| ()).map_err(|e| e.code)
    });

    let e2 = Arc::clone(&engine);
    let h2 = tokio::spawn(async move {
        let mut s = e2.connect();
        run(&mut s, "BEGIN").await;
        // Key lock on id=2.
        run(&mut s, "INSERT INTO t VALUES (2, 'from2')").await;
        tx2_ready.send(()).expect("send t2 ready");
        rx1_ready.await.expect("recv t1 ready");
        // Row lock on id=1's row — T1 holds it → cycle across lock spaces.
        let result = s.simple_query("UPDATE t SET v = 'y' WHERE id = 1").await;
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

    let codes: Vec<Option<String>> = vec![r1.as_ref().err().cloned(), r2.as_ref().err().cloned()];
    let deadlock_count = codes
        .iter()
        .filter(|c| c.as_deref() == Some("40P01"))
        .count();
    let ok_count = [r1.is_ok(), r2.is_ok()].iter().filter(|&&b| b).count();
    assert!(deadlock_count == 1, "expected exactly one 40P01: {codes:?}");
    assert!(ok_count == 1, "expected exactly one winner: {codes:?}");
}

/// (e) Unique CREATE INDEX backfill still EXCLUDES in-flight DML: the DDL's
/// exclusive `unique_index_lock` waits for an open transaction's shared hold,
/// and the finished index enforces uniqueness (the in-flight row was seen).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unique_index_backfill_waits_for_inflight_dml() {
    let engine = Arc::new(SqlEngine::new());
    {
        let mut s = engine.connect();
        run(&mut s, "CREATE TABLE t (id bigint, v text)").await;
    }

    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    // Holds the unique-index gate SHARED until COMMIT.
    run(&mut t1, "INSERT INTO t VALUES (1, 'a')").await;

    let e2 = Arc::clone(&engine);
    let ddl = tokio::spawn(async move {
        let mut s = e2.connect();
        run(&mut s, "CREATE UNIQUE INDEX t_id_idx ON t (id)").await;
    });

    // The backfill must be blocked while T1's write is uncommitted.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!ddl.is_finished());

    run(&mut t1, "COMMIT").await;
    tokio::time::timeout(Duration::from_secs(10), ddl)
        .await
        .expect("backfill did not hang")
        .expect("ddl join");

    // The backfilled index saw the committed row: a duplicate now violates.
    let mut s = engine.connect();
    assert!(err_code(&mut s, "INSERT INTO t VALUES (1, 'dup')").await == "23505");
}

/// (f) An UPDATE that leaves every unique-indexed column unchanged takes NO
/// key lock: while such an update's transaction is open (holding only its row
/// lock), a PK-preserving update of another row commits concurrently, and an
/// INSERT of the open transaction's PK fails FAST with 23505 (it would block
/// on the key until COMMIT if the update had locked its unchanged PK).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pk_preserving_update_takes_no_key_lock() {
    let engine = Arc::new(SqlEngine::new());
    {
        let mut s = engine.connect();
        run(&mut s, "CREATE TABLE t (id bigint PRIMARY KEY, v text)").await;
        run(&mut s, "INSERT INTO t VALUES (1, 'a'), (2, 'b')").await;
    }

    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    run(&mut t1, "UPDATE t SET v = 'a2' WHERE id = 1").await; // row lock only

    // Another PK-preserving update (different row) commits while T1 is open.
    let e2 = Arc::clone(&engine);
    let t2 = tokio::spawn(async move {
        let mut s = e2.connect();
        run(&mut s, "BEGIN").await;
        run(&mut s, "UPDATE t SET v = 'b2' WHERE id = 2").await;
        run(&mut s, "COMMIT").await;
    });
    tokio::time::timeout(Duration::from_secs(5), t2)
        .await
        .expect("row-Y update must not wait for row-X's open transaction")
        .expect("t2 join");

    // Key id=1 is UNLOCKED (T1's update left the PK unchanged): a conflicting
    // insert fails fast against the committed row instead of queueing on T1.
    let e3 = Arc::clone(&engine);
    let t3 = tokio::spawn(async move {
        let mut s = e3.connect();
        err_code(&mut s, "INSERT INTO t VALUES (1, 'dup')").await
    });
    let code = tokio::time::timeout(Duration::from_secs(5), t3)
        .await
        .expect("insert must not block on an unchanged PK's phantom key lock")
        .expect("t3 join");
    assert!(code == "23505");

    run(&mut t1, "COMMIT").await;
    let mut s = engine.connect();
    assert!(
        col0(&run(&mut s, "SELECT v FROM t ORDER BY id").await[0])
            == vec![Some("a2".into()), Some("b2".into())]
    );
}

/// An UPDATE that CHANGES a unique key still serializes with a concurrent
/// insert of that key: the insert blocks on the update's new-key lock and
/// fails only after the update commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_changing_update_blocks_concurrent_insert_of_new_key() {
    let engine = Arc::new(SqlEngine::new());
    {
        let mut s = engine.connect();
        run(&mut s, "CREATE TABLE t (id bigint PRIMARY KEY, v text)").await;
        run(&mut s, "INSERT INTO t VALUES (1, 'a')").await;
    }

    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    run(&mut t1, "UPDATE t SET id = 7 WHERE id = 1").await; // key lock on id=7

    let e2 = Arc::clone(&engine);
    let t2 = tokio::spawn(async move {
        let mut s = e2.connect();
        err_code(&mut s, "INSERT INTO t VALUES (7, 'dup')").await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!t2.is_finished());

    run(&mut t1, "COMMIT").await;
    let code = tokio::time::timeout(Duration::from_secs(10), t2)
        .await
        .expect("t2 did not hang")
        .expect("t2 join");
    assert!(code == "23505");
}
