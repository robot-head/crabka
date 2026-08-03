//! Per-unique-key lock behavior. DML on tables with unique local indexes no
//! longer serializes engine-wide: writers of the SAME key queue on that key's
//! exclusive lock in the lock manager (probing only after the holder's
//! terminal outcome), writers of different keys run concurrently, and unique
//! CREATE INDEX backfill still excludes in-flight DML via the shared/exclusive
//! `unique_index_lock` gate.
//!
//! `INSERT … ON CONFLICT` rides the same key lock: arbitration takes the
//! exclusive UNIQUE-KEY lock BEFORE probing, so a concurrent inserter of the
//! same key serializes with it and the loser re-probes only once the winner's
//! outcome is durable. The upsert tests below cover both terminal outcomes of
//! the winner, the row-lock/`eval_plan_qual` re-arbitration when the
//! conflicting row is concurrently deleted, and the REPEATABLE READ guard that
//! turns an otherwise-invisible conflict into 40001.

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

/// (e2) A transaction cannot safely upgrade its retained shared gate while a
/// competing backfill may already be queued. Reject that unsupported shape
/// promptly instead of self-deadlocking or allowing an invalid backfill.
///
/// `pg_regress`'s `join` corpus is exactly this shape — `BEGIN; CREATE TABLE
/// fkest …; INSERT INTO fkest SELECT … generate_series(1,1000); CREATE UNIQUE
/// INDEX ON fkest(…)` — and it must answer, not hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unique_ddl_after_a_write_is_rejected_instead_of_self_deadlocking() {
    let engine = Arc::new(SqlEngine::new());
    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    run(&mut t1, "CREATE TABLE fkest (x integer, x10 integer)").await;
    // Takes the gate SHARED for the rest of the transaction.
    run(
        &mut t1,
        "INSERT INTO fkest SELECT x, x / 10 FROM generate_series(1, 1000) x",
    )
    .await;

    let code = tokio::time::timeout(
        Duration::from_secs(30),
        err_code(&mut t1, "CREATE UNIQUE INDEX ON fkest (x, x10)"),
    )
    .await
    .expect("unique DDL deadlocked against its own transaction's shared hold");
    assert!(code == "0A000");
    run(&mut t1, "ROLLBACK").await;
}

/// (e3) Waiting for the gate must not also stall unrelated DDL. The exclusive
/// hold is taken BEFORE the catalog lock, so a backfill parked behind another
/// session's open write leaves `CREATE TABLE` on an unrelated relation — which
/// wants the catalog lock and nothing else — free to run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_blocked_backfill_does_not_stall_unrelated_ddl() {
    let engine = Arc::new(SqlEngine::new());
    {
        let mut s = engine.connect();
        run(&mut s, "CREATE TABLE t (id bigint, v text)").await;
    }

    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    run(&mut t1, "INSERT INTO t VALUES (1, 'a')").await;

    let e2 = Arc::clone(&engine);
    let ddl = tokio::spawn(async move {
        let mut s = e2.connect();
        run(&mut s, "CREATE UNIQUE INDEX t_id_idx ON t (id)").await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!ddl.is_finished());

    let mut other = engine.connect();
    tokio::time::timeout(
        Duration::from_secs(30),
        run(&mut other, "CREATE TABLE unrelated (a int)"),
    )
    .await
    .expect("unrelated DDL blocked behind a waiting unique backfill");

    run(&mut t1, "COMMIT").await;
    tokio::time::timeout(Duration::from_secs(30), ddl)
        .await
        .expect("backfill did not hang")
        .expect("ddl join");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_blocked_backfill_does_not_stall_unrelated_dml() {
    let engine = Arc::new(SqlEngine::new());
    let mut setup = engine.connect();
    run(
        &mut setup,
        "CREATE TABLE t (id int); CREATE TABLE u (id int)",
    )
    .await;

    let mut writer = engine.connect();
    run(&mut writer, "BEGIN; INSERT INTO t VALUES (1)").await;

    let ddl_engine = Arc::clone(&engine);
    let ddl = tokio::spawn(async move {
        let mut session = ddl_engine.connect();
        run(&mut session, "CREATE UNIQUE INDEX ON t (id)").await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!ddl.is_finished());

    let mut unrelated = engine.connect();
    tokio::time::timeout(
        Duration::from_secs(5),
        run(&mut unrelated, "INSERT INTO u VALUES (1)"),
    )
    .await
    .expect("unrelated DML blocked behind another relation's backfill");

    run(&mut writer, "COMMIT").await;
    tokio::time::timeout(Duration::from_secs(10), ddl)
        .await
        .expect("backfill did not finish")
        .expect("ddl join");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_gates_each_relation_it_writes() {
    let engine = Arc::new(SqlEngine::new());
    let mut setup = engine.connect();
    run(
        &mut setup,
        "CREATE TABLE t (id int); CREATE TABLE u (id int)",
    )
    .await;

    let mut writer = engine.connect();
    run(
        &mut writer,
        "BEGIN; INSERT INTO t VALUES (1); INSERT INTO u VALUES (1)",
    )
    .await;

    let mut backfills = Vec::new();
    for table in ["t", "u"] {
        let engine = Arc::clone(&engine);
        backfills.push(tokio::spawn(async move {
            let mut session = engine.connect();
            run(
                &mut session,
                &format!("CREATE UNIQUE INDEX ON {table} (id)"),
            )
            .await;
        }));
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(backfills.iter().all(|backfill| !backfill.is_finished()));

    run(&mut writer, "COMMIT").await;
    for backfill in backfills {
        tokio::time::timeout(Duration::from_secs(10), backfill)
            .await
            .expect("backfill did not finish")
            .expect("ddl join");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crossed_transactional_backfills_are_rejected_without_deadlock() {
    let engine = Arc::new(SqlEngine::new());
    let mut setup = engine.connect();
    run(
        &mut setup,
        "CREATE TABLE a (id int); CREATE TABLE b (id int)",
    )
    .await;

    let mut t1 = engine.connect();
    let mut t2 = engine.connect();
    run(&mut t1, "BEGIN; INSERT INTO a VALUES (1)").await;
    run(&mut t2, "BEGIN; INSERT INTO b VALUES (1)").await;

    let first = tokio::spawn(async move {
        err_code(&mut t1, "CREATE UNIQUE INDEX b_id_idx ON b (id)").await
    });
    let second = tokio::spawn(async move {
        err_code(&mut t2, "CREATE UNIQUE INDEX a_id_idx ON a (id)").await
    });

    let codes = tokio::time::timeout(Duration::from_secs(10), async {
        [first.await.expect("first join"), second.await.expect("second join")]
    })
    .await
    .expect("crossed transactional backfills deadlocked");
    assert!(codes == ["0A000", "0A000"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prequeued_backfill_remains_blocked_when_transactional_ddl_is_rejected() {
    let engine = Arc::new(SqlEngine::new());
    let mut setup = engine.connect();
    run(&mut setup, "CREATE TABLE t (id int)").await;

    let mut writer = engine.connect();
    run(&mut writer, "BEGIN; INSERT INTO t VALUES (1)").await;
    let other_engine = Arc::clone(&engine);
    let other = tokio::spawn(async move {
        let mut session = other_engine.connect();
        run(
            &mut session,
            "CREATE UNIQUE INDEX t_id_idx ON t (id)",
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!other.is_finished());

    assert!(
        err_code(&mut writer, "CREATE UNIQUE INDEX t_id_idx_2 ON t (id)").await == "0A000"
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!other.is_finished());

    run(&mut writer, "ROLLBACK").await;
    tokio::time::timeout(Duration::from_secs(10), other)
        .await
        .expect("competing backfill stayed blocked after commit")
        .expect("other join");
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

/// Seed an engine with the shared upsert fixture table.
async fn upsert_engine() -> Arc<SqlEngine> {
    let engine = Arc::new(SqlEngine::new());
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id bigint PRIMARY KEY, v text)").await;
    engine
}

async fn rows_of(engine: &SqlEngine, sql: &str) -> Vec<Vec<Option<String>>> {
    let mut s = engine.connect();
    match &run(&mut s, sql).await[0] {
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
        o => panic!("{o:?}"),
    }
}

/// (g) `ON CONFLICT DO NOTHING` takes the SAME exclusive key lock before it
/// probes, so it cannot race an uncommitted inserter of that key: it blocks,
/// and the outcome it reports depends on whether the holder committed. When
/// the holder COMMITs the key is taken and the row is skipped (`INSERT 0 0`);
/// when the holder ROLLBACKs there is nothing to conflict with and the row is
/// inserted (`INSERT 0 1`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn do_nothing_blocks_on_the_key_then_follows_the_holders_outcome() {
    for (holder_outcome, expected_tag, expected_v) in [
        ("COMMIT", "INSERT 0 0", "first"),
        ("ROLLBACK", "INSERT 0 1", "second"),
    ] {
        let engine = upsert_engine().await;

        let mut t1 = engine.connect();
        run(&mut t1, "BEGIN").await;
        run(&mut t1, "INSERT INTO t VALUES (1, 'first')").await; // holds key lock on id=1

        let e2 = Arc::clone(&engine);
        let t2 = tokio::spawn(async move {
            let mut s = e2.connect();
            let r = run(
                &mut s,
                "INSERT INTO t VALUES (1, 'second') ON CONFLICT (id) DO NOTHING",
            )
            .await;
            tag_of(&r[0]).to_string()
        });

        // The probe is BEHIND the key lock: no early skip, no early insert.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!t2.is_finished(), "{holder_outcome}");

        run(&mut t1, holder_outcome).await;
        let tag = tokio::time::timeout(Duration::from_secs(10), t2)
            .await
            .expect("t2 did not hang")
            .expect("t2 join");
        assert!(tag == expected_tag, "{holder_outcome}");
        assert!(
            rows_of(&engine, "SELECT id, v FROM t ORDER BY id").await
                == vec![vec![Some("1".into()), Some(expected_v.into())]],
            "{holder_outcome}"
        );
    }
}

/// (h) The same race with `DO UPDATE` where the holder ROLLS BACK: the blocked
/// upsert wakes, re-probes, finds no conflict and inserts its own row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn do_update_blocks_then_inserts_after_the_holder_rolls_back() {
    let engine = upsert_engine().await;

    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    run(&mut t1, "INSERT INTO t VALUES (1, 'first')").await; // holds key lock on id=1

    let e2 = Arc::clone(&engine);
    let t2 = tokio::spawn(async move {
        let mut s = e2.connect();
        let r = run(
            &mut s,
            "INSERT INTO t VALUES (1, 'second') \
             ON CONFLICT (id) DO UPDATE SET v = excluded.v || '/' || t.v",
        )
        .await;
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
    assert!(
        rows_of(&engine, "SELECT id, v FROM t ORDER BY id").await
            == vec![vec![Some("1".into()), Some("second".into())]]
    );
}

/// (h′) The blocked `DO UPDATE` must UPDATE the row the winner inserted, not
/// raise a unique violation.
///
/// When the key holder COMMITs, the blocked upsert wakes and re-probes. The
/// probe reads all-committed visibility and finds the freshly committed row,
/// but that row is invisible to the statement's own snapshot (taken before the
/// holder committed), and `eval_plan_qual`'s read-committed refresh only fires
/// on an `xmax` stamp — which a concurrent INSERT never leaves. Arbitration
/// therefore re-reads such a holder under a fresh snapshot; without that it
/// would treat the row as vanished, fall through to a plain INSERT, and raise
/// 23505. `PostgreSQL` guarantees the opposite: "ON CONFLICT DO UPDATE guarantees
/// an atomic INSERT or UPDATE outcome … even under high concurrency".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn do_update_after_a_concurrently_committed_insert_updates_that_row() {
    let engine = upsert_engine().await;

    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    run(&mut t1, "INSERT INTO t VALUES (1, 'first')").await; // holds key lock on id=1

    let e2 = Arc::clone(&engine);
    let t2 = tokio::spawn(async move {
        let mut s = e2.connect();
        s.simple_query(
            "INSERT INTO t VALUES (1, 'second') \
             ON CONFLICT (id) DO UPDATE SET v = excluded.v || '/' || t.v",
        )
        .await
        .map(|r| tag_of(&r[0]).to_string())
        .map_err(|e| e.code)
    });

    // The probe is behind the key lock, so nothing is decided early.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!t2.is_finished());

    run(&mut t1, "COMMIT").await;
    let outcome = tokio::time::timeout(Duration::from_secs(10), t2)
        .await
        .expect("t2 did not hang")
        .expect("t2 join");
    assert!(outcome == Ok("INSERT 0 1".to_string()));
    assert!(
        rows_of(&engine, "SELECT id, v FROM t ORDER BY id").await
            == vec![vec![Some("1".into()), Some("second/first".into())]]
    );
}

/// (h″) The contrast that isolates (h′)'s cause. Here the key holder is a
/// key-CHANGING `UPDATE` rather than an `INSERT`, so the row it moves onto the
/// key leaves an `xmax` stamp on its previous version. That stamp is what
/// `eval_plan_qual`'s read-committed refresh keys off, so the blocked upsert
/// does re-read under a fresh snapshot and correctly UPDATEs the row it found —
/// the outcome (h′) should have produced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn do_update_racing_a_key_changing_update_upserts_correctly() {
    let engine = upsert_engine().await;
    {
        let mut s = engine.connect();
        run(&mut s, "INSERT INTO t VALUES (1, 'orig')").await;
    }

    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    run(&mut t1, "UPDATE t SET id = 7 WHERE id = 1").await; // key lock on id=7

    let e2 = Arc::clone(&engine);
    let t2 = tokio::spawn(async move {
        let mut s = e2.connect();
        let r = run(
            &mut s,
            "INSERT INTO t VALUES (7, 'up') \
             ON CONFLICT (id) DO UPDATE SET v = excluded.v || '/' || t.v",
        )
        .await;
        tag_of(&r[0]).to_string()
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!t2.is_finished());

    run(&mut t1, "COMMIT").await;
    let tag = tokio::time::timeout(Duration::from_secs(10), t2)
        .await
        .expect("t2 did not hang")
        .expect("t2 join");
    assert!(tag == "INSERT 0 1");
    // The post-image proves the upsert read T1's committed post-image.
    assert!(
        rows_of(&engine, "SELECT id, v FROM t ORDER BY id").await
            == vec![vec![Some("7".into()), Some("up/orig".into())]]
    );
}

/// (i) `DO UPDATE` versus a concurrent DELETE of the conflicting row: the
/// arbiter's probe (all-committed visibility) still finds the row, so the
/// upsert takes its ROW lock and blocks on the deleter. When the DELETE
/// commits, `eval_plan_qual` finds the row gone, arbitration restarts without
/// it, and the statement falls through to a plain INSERT — leaving exactly one
/// row carrying the upsert's proposed values.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn do_update_re_arbitrates_into_an_insert_when_the_row_is_deleted() {
    let engine = upsert_engine().await;
    {
        let mut s = engine.connect();
        run(&mut s, "INSERT INTO t VALUES (1, 'seed')").await;
    }

    let mut t1 = engine.connect();
    run(&mut t1, "BEGIN").await;
    // A PK-preserving DELETE takes the ROW lock only, leaving key id=1 free.
    run(&mut t1, "DELETE FROM t WHERE id = 1").await;

    let e2 = Arc::clone(&engine);
    let t2 = tokio::spawn(async move {
        let mut s = e2.connect();
        let r = run(
            &mut s,
            "INSERT INTO t VALUES (1, 'upserted') \
             ON CONFLICT (id) DO UPDATE SET v = excluded.v",
        )
        .await;
        tag_of(&r[0]).to_string()
    });

    // T2 got the key lock and found the (still committed) row, then queued on
    // T1's row lock.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!t2.is_finished());

    run(&mut t1, "COMMIT").await;
    let tag = tokio::time::timeout(Duration::from_secs(10), t2)
        .await
        .expect("t2 did not hang")
        .expect("t2 join");
    assert!(tag == "INSERT 0 1");

    assert!(
        rows_of(&engine, "SELECT id, v FROM t ORDER BY id").await
            == vec![vec![Some("1".into()), Some("upserted".into())]]
    );
}

/// (j) The REPEATABLE READ guard. The arbiter probes with all-committed
/// visibility, so it finds a row committed AFTER the RR snapshot was taken —
/// a row the transaction cannot read. `DO UPDATE` must refuse that with 40001
/// rather than silently updating an invisible row (or reporting a bogus
/// 23505); `DO NOTHING` has nothing to read and simply skips the row, which is
/// what Postgres does too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeatable_read_upsert_onto_an_invisible_row_is_40001() {
    for (action, expectation) in [
        ("DO UPDATE SET v = excluded.v", Err("40001".to_string())),
        ("DO NOTHING", Ok("INSERT 0 0".to_string())),
    ] {
        let engine = upsert_engine().await;

        let mut rr = engine.connect();
        run(&mut rr, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
        // Take the snapshot before the conflicting row exists.
        assert!(col0(&run(&mut rr, "SELECT id FROM t").await[0]).is_empty());

        {
            let mut other = engine.connect();
            run(&mut other, "INSERT INTO t VALUES (1, 'invisible')").await;
        }
        // The RR transaction genuinely cannot see the committed row — that
        // invisibility is the whole point of the guard — while a fresh reader
        // does.
        assert!(
            col0(&run(&mut rr, "SELECT id FROM t").await[0]).is_empty(),
            "{action}"
        );
        assert!(
            rows_of(&engine, "SELECT id FROM t").await == vec![vec![Some("1".into())]],
            "{action}"
        );

        let sql = format!("INSERT INTO t VALUES (1, 'rr') ON CONFLICT (id) {action}");
        let actual = rr
            .simple_query(&sql)
            .await
            .map(|r| tag_of(&r[0]).to_string())
            .map_err(|e| e.code);
        assert!(actual == expectation, "{action}");

        let _ = rr.simple_query("ROLLBACK").await;
        // The concurrent row is untouched either way.
        assert!(
            rows_of(&engine, "SELECT id, v FROM t ORDER BY id").await
                == vec![vec![Some("1".into()), Some("invisible".into())]],
            "{action}"
        );
    }
}
