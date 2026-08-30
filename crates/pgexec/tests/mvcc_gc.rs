//! Dead MVCC version reclamation: opportunistic write-path chain pruning on
//! LOCAL engines, snapshot pinning of the garbage horizon, the engine-level
//! `vacuum` sweep, and the replicated-mode gate that keeps physical pruning
//! out of deterministic WAL apply.

use std::sync::Arc;

use assert2::assert;
use crabka_pgcatalog::RelationName;
use crabka_pgexec::{Committer, ExecError, LocalLinearizer, SqlEngine, SqlSession};
use crabka_pgkv::{Kv, MemKv, WriteOp};
use crabka_pgwire::engine::{Engine, QueryResult, Session};

async fn exec(session: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    session
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("query {sql:?} failed: {error:?}"))
}

/// Visible table contents as text cells, for whole-table comparisons.
async fn select_rows(session: &mut SqlSession, sql: &str) -> Vec<Vec<Option<String>>> {
    match exec(session, sql).await.remove(0) {
        QueryResult::Rows { rows, .. } => rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|cell| {
                        cell.map(|cell| String::from_utf8(cell.text.to_vec()).expect("utf8 cell"))
                    })
                    .collect()
            })
            .collect(),
        other => panic!("expected rows, got {other:?}"),
    }
}

/// Number of physical tuple versions stored for one rowid.
fn version_count(kv: &dyn Kv, table_id: u32, rowid: u64) -> usize {
    kv.scan_prefix(&crabka_pgkv::key::row_key(table_id, rowid))
        .expect("scan row versions")
        .len()
}

/// Number of physical tuple versions stored for a whole table.
fn table_version_count(kv: &dyn Kv, table_id: u32) -> usize {
    kv.scan_prefix(&crabka_pgkv::key::table_prefix(table_id))
        .expect("scan table versions")
        .len()
}

/// Number of physical entries stored in one local secondary index. Each live
/// key has an equality and an ordered B-tree representation.
fn index_entry_count(kv: &dyn Kv, table_id: u32, index_id: u32) -> usize {
    kv.scan_prefix(&crabka_pgkv::key::secondary_index_prefix(
        table_id, index_id,
    ))
    .expect("scan index entries")
    .len()
}

fn only_local_index(kv: &dyn Kv, table: &str) -> crabka_pgcatalog::Index {
    let mut indexes = crabka_pgcatalog::list_table_indexes(kv, &RelationName::public(table))
        .expect("list indexes");
    assert!(indexes.len() == 1, "expected exactly one index on {table}");
    indexes.remove(0)
}

// ── (a) hot-row churn ────────────────────────────────────────────────────────

#[tokio::test]
async fn hot_row_update_churn_keeps_the_version_chain_bounded() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    exec(
        &mut session,
        "CREATE TABLE tbl (id BIGINT PRIMARY KEY, v TEXT)",
    )
    .await;
    exec(&mut session, "INSERT INTO tbl VALUES (0,'x'),(1,'y')").await;
    let table = engine
        .catalog_table(&RelationName::public("tbl"))
        .expect("table");
    let kv = engine.kv_handle();

    for i in 0..50 {
        exec(
            &mut session,
            &format!("UPDATE tbl SET v = 'h{i}' WHERE id = 0"),
        )
        .await;
    }

    // Rowid 1 is the id=0 row (first insert). Without pruning the chain holds
    // all 51 versions; opportunistic pruning keeps it O(1): the live version
    // plus its immediate committed predecessor (superseded by the still-newest
    // xid, which equals the statement's horizon and is never below it).
    let hot_row_versions = version_count(kv.as_ref(), table.id, 1);
    assert!(
        hot_row_versions <= 3,
        "hot row retained {hot_row_versions} versions after 50 updates"
    );
    // The untouched row keeps exactly its insert version.
    assert!(version_count(kv.as_ref(), table.id, 2) == 1);
    // Results are unaffected by pruning.
    let rows = select_rows(&mut session, "SELECT id, v FROM tbl ORDER BY id").await;
    assert!(
        rows == vec![
            vec![Some("0".to_owned()), Some("h49".to_owned())],
            vec![Some("1".to_owned()), Some("y".to_owned())],
        ]
    );
}

// ── (b) open REPEATABLE READ pins old versions ───────────────────────────────

#[tokio::test]
async fn open_repeatable_read_transaction_pins_versions_until_it_ends() {
    let engine = SqlEngine::new();
    let mut writer = engine.connect();
    exec(
        &mut writer,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v TEXT)",
    )
    .await;
    exec(&mut writer, "INSERT INTO t VALUES (1,'orig')").await;
    let table = engine
        .catalog_table(&RelationName::public("t"))
        .expect("table");
    let kv = engine.kv_handle();

    let mut reader = engine.connect();
    exec(&mut reader, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
    let before = select_rows(&mut reader, "SELECT v FROM t WHERE id = 1").await;
    assert!(before == vec![vec![Some("orig".to_owned())]]);

    for i in 0..10 {
        exec(
            &mut writer,
            &format!("UPDATE t SET v = 'w{i}' WHERE id = 1"),
        )
        .await;
    }

    // The RR snapshot pins the horizon at its BEGIN xmin: every superseded
    // version's deleter committed at or above that pin, so nothing is pruned
    // while the transaction stays open.
    let pinned_versions = table_version_count(kv.as_ref(), table.id);
    assert!(
        pinned_versions >= 11,
        "expected the full 11-version chain while the RR txn is open, found {pinned_versions}"
    );
    // And the RR transaction still sees its snapshot value.
    let during = select_rows(&mut reader, "SELECT v FROM t WHERE id = 1").await;
    assert!(during == vec![vec![Some("orig".to_owned())]]);

    exec(&mut reader, "COMMIT").await;

    // With the pin released, the next write on the row reclaims the backlog.
    exec(&mut writer, "UPDATE t SET v = 'final' WHERE id = 1").await;
    let after = version_count(kv.as_ref(), table.id, 1);
    assert!(
        after <= 3,
        "expected the chain to collapse after the RR txn ended, found {after} versions"
    );
    let latest = select_rows(&mut reader, "SELECT v FROM t WHERE id = 1").await;
    assert!(latest == vec![vec![Some("final".to_owned())]]);
}

// ── (c) vacuum reclaims cold garbage ─────────────────────────────────────────

#[tokio::test]
async fn vacuum_reclaims_aborted_insert_garbage_and_its_index_entries() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    exec(
        &mut session,
        "CREATE TABLE u (id BIGINT PRIMARY KEY, v TEXT)",
    )
    .await;
    let table = engine
        .catalog_table(&RelationName::public("u"))
        .expect("table");
    let index = only_local_index(engine.catalog_kv(), "u");
    let kv = engine.kv_handle();

    exec(&mut session, "BEGIN").await;
    exec(&mut session, "INSERT INTO u VALUES (7,'k')").await;
    exec(&mut session, "ROLLBACK").await;

    // The aborted insert left a dead version and both index representations.
    assert!(table_version_count(kv.as_ref(), table.id) == 1);
    assert!(index_entry_count(kv.as_ref(), table.id, index.id) == 2);

    let stats = engine.vacuum().await.expect("vacuum");
    assert!(stats.versions_pruned == 1);
    assert!(stats.index_entries_pruned == 1);
    assert!(table_version_count(kv.as_ref(), table.id) == 0);
    assert!(index_entry_count(kv.as_ref(), table.id, index.id) == 0);

    // Re-inserting the same unique key succeeds against the clean index.
    exec(&mut session, "INSERT INTO u VALUES (7,'k')").await;
    let rows = select_rows(&mut session, "SELECT id, v FROM u").await;
    assert!(rows == vec![vec![Some("7".to_owned()), Some("k".to_owned())]]);
}

#[tokio::test]
async fn vacuum_reclaims_deleted_rows_the_write_path_never_revisits() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    exec(
        &mut session,
        "CREATE TABLE d (id BIGINT PRIMARY KEY, v TEXT)",
    )
    .await;
    exec(&mut session, "INSERT INTO d VALUES (1,'a'),(2,'b')").await;
    exec(&mut session, "DELETE FROM d WHERE id = 1").await;
    let table = engine
        .catalog_table(&RelationName::public("d"))
        .expect("table");
    let index = only_local_index(engine.catalog_kv(), "d");
    let kv = engine.kv_handle();

    // The delete tombstones the version in place; nothing revisits the rowid.
    assert!(version_count(kv.as_ref(), table.id, 1) == 1);

    let stats = engine.vacuum().await.expect("vacuum");
    assert!(stats.versions_pruned == 1);
    assert!(stats.index_entries_pruned == 1);
    assert!(version_count(kv.as_ref(), table.id, 1) == 0);
    // The surviving row keeps its version and both index representations.
    assert!(version_count(kv.as_ref(), table.id, 2) == 1);
    assert!(index_entry_count(kv.as_ref(), table.id, index.id) == 2);
    let rows = select_rows(&mut session, "SELECT id, v FROM d ORDER BY id").await;
    assert!(rows == vec![vec![Some("2".to_owned()), Some("b".to_owned())]]);
}

// ── (d) replicated engines never prune locally ───────────────────────────────

/// Test committer that applies batches straight to the shared applied store.
///
/// A Replicated-mode engine is then testable without a raft runtime.
struct ApplyCommitter(Arc<dyn Kv>);

#[async_trait::async_trait]
impl Committer for ApplyCommitter {
    async fn commit(&self, ops: Vec<WriteOp>) -> Result<(), ExecError> {
        self.0.write_batch(&ops)?;
        Ok(())
    }
}

#[tokio::test]
async fn replicated_mode_prunes_in_commit_batches_and_vacuum_is_a_no_op() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let engine = SqlEngine::replicated(
        Arc::clone(&kv),
        Arc::clone(&kv),
        Arc::new(ApplyCommitter(Arc::clone(&kv))),
        Arc::new(LocalLinearizer),
    )
    .expect("replicated engine");
    assert!(!engine.supports_local_vacuum());
    assert!(SqlEngine::new().supports_local_vacuum());

    let mut session = engine.connect();
    exec(
        &mut session,
        "CREATE TABLE r (id BIGINT PRIMARY KEY, v TEXT)",
    )
    .await;
    exec(&mut session, "INSERT INTO r VALUES (1,'a')").await;
    for i in 0..5 {
        exec(
            &mut session,
            &format!("UPDATE r SET v = 'r{i}' WHERE id = 1"),
        )
        .await;
    }
    let table = engine
        .catalog_table(&RelationName::public("r"))
        .expect("table");

    // Write-path pruning engages on replicated engines too — the deletes ride
    // each statement's replicated commit batch, so the chain stays bounded
    // instead of holding all six versions.
    let pruned_chain = version_count(kv.as_ref(), table.id, 1);
    assert!(pruned_chain <= 3);

    // The engine-level sweep still refuses to touch a replicated store — its
    // batches would commit outside statement order. Full pass and bounded
    // step alike leave the store untouched.
    let stats = engine.vacuum().await.expect("vacuum");
    assert!(stats == crabka_pgexec::VacuumStats::default());
    let step = engine.vacuum_step().await.expect("vacuum step");
    assert!(step == crabka_pgexec::VacuumStepStats::default());
    assert!(version_count(kv.as_ref(), table.id, 1) == pruned_chain);
}

// ── (e) pruning does not change UPDATE/DELETE results ────────────────────────

/// Run one DML script against a LOCAL engine and a replicated engine.
///
/// Both engines prune on the write path. The visible table contents must be
/// identical.
#[tokio::test]
async fn update_and_delete_results_are_unchanged_with_pruning_active() {
    let script: Vec<String> = {
        let mut script = vec![
            "CREATE TABLE w (id BIGINT PRIMARY KEY, v TEXT)".to_owned(),
            "INSERT INTO w VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')".to_owned(),
        ];
        for round in 0..8 {
            for id in 1..=5 {
                script.push(format!("UPDATE w SET v = 'r{round}-{id}' WHERE id = {id}"));
            }
        }
        script.push("DELETE FROM w WHERE id = 2".to_owned());
        script.push("DELETE FROM w WHERE id = 4".to_owned());
        script.push("INSERT INTO w VALUES (6,'f')".to_owned());
        script.push("UPDATE w SET v = 'last' WHERE id = 5".to_owned());
        script
    };

    let local_engine = SqlEngine::new();
    let mut local = local_engine.connect();
    for sql in &script {
        exec(&mut local, sql).await;
    }

    let replicated_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let replicated_engine = SqlEngine::replicated(
        Arc::clone(&replicated_kv),
        Arc::clone(&replicated_kv),
        Arc::new(ApplyCommitter(Arc::clone(&replicated_kv))),
        Arc::new(LocalLinearizer),
    )
    .expect("replicated engine");
    let mut replicated = replicated_engine.connect();
    for sql in &script {
        exec(&mut replicated, sql).await;
    }

    let pruned = select_rows(&mut local, "SELECT id, v FROM w ORDER BY id").await;
    let unpruned = select_rows(&mut replicated, "SELECT id, v FROM w ORDER BY id").await;
    assert!(pruned == unpruned);
    assert!(
        pruned
            == vec![
                vec![Some("1".to_owned()), Some("r7-1".to_owned())],
                vec![Some("3".to_owned()), Some("r7-3".to_owned())],
                vec![Some("5".to_owned()), Some("last".to_owned())],
                vec![Some("6".to_owned()), Some("f".to_owned())],
            ]
    );

    // The pruning engine actually pruned: the churned table's physical version
    // count stays near the live row count instead of the ~48 versions written.
    let table = local_engine
        .catalog_table(&RelationName::public("w"))
        .expect("table");
    let physical = table_version_count(local_engine.kv_handle().as_ref(), table.id);
    assert!(
        physical <= 12,
        "expected a bounded physical version count, found {physical}"
    );
}

// ── vacuum freezes survivors and truncates the clog ──────────────────────────

#[tokio::test]
async fn vacuum_freezes_survivors_truncates_the_clog_and_updates_still_work() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    exec(
        &mut session,
        "CREATE TABLE z (id BIGINT PRIMARY KEY, v TEXT)",
    )
    .await;
    exec(&mut session, "INSERT INTO z VALUES (1,'a'),(2,'b')").await;
    for i in 0..5 {
        exec(
            &mut session,
            &format!("UPDATE z SET v = 'z{i}' WHERE id = 1"),
        )
        .await;
    }
    let table = engine
        .catalog_table(&RelationName::public("z"))
        .expect("table");
    let kv = engine.kv_handle();

    let stats = engine.vacuum().await.expect("vacuum");
    // Both rows' surviving versions sit below the horizon: frozen.
    assert!(stats.versions_frozen >= 2);
    // Every decided xid's clog entry below the horizon is physically deleted.
    assert!(stats.clog_entries_pruned > 0);
    let floor = engine.clog_scan_lo().expect("scan lo");
    let below_floor = kv
        .scan_range(
            &crabka_pgkv::key::clog_key(0),
            &crabka_pgkv::key::clog_key(floor),
        )
        .expect("scan clog")
        .len();
    assert!(below_floor == 0, "clog below the floor must be empty");

    // Frozen tuples stay visible without their clog entries.
    let rows = select_rows(&mut session, "SELECT id, v FROM z ORDER BY id").await;
    assert!(
        rows == vec![
            vec![Some("1".to_owned()), Some("z4".to_owned())],
            vec![Some("2".to_owned()), Some("b".to_owned())],
        ]
    );

    // Updating a FROZEN row stamps the frozen version's PHYSICAL key (its
    // header xmin no longer names it), so exactly one version stays live.
    exec(&mut session, "UPDATE z SET v = 'new' WHERE id = 1").await;
    let rows = select_rows(&mut session, "SELECT id, v FROM z ORDER BY id").await;
    assert!(
        rows == vec![
            vec![Some("1".to_owned()), Some("new".to_owned())],
            vec![Some("2".to_owned()), Some("b".to_owned())],
        ]
    );
    // And a second sweep reclaims the superseded frozen version. The update
    // moved it to a new physical rowid, so count the table rather than rowid 1.
    engine.vacuum().await.expect("second vacuum");
    assert!(table_version_count(kv.as_ref(), table.id) == 2);
}

// ── bounded incremental sweeps (vacuum_step) ─────────────────────────────────

/// Run bounded sweep steps until a cycle completes, then return the aggregated
/// stats and the number of steps the cycle took.
async fn run_steps_to_cycle_end(engine: &SqlEngine) -> (crabka_pgexec::VacuumStats, u32) {
    let mut total = crabka_pgexec::VacuumStats::default();
    for steps in 1..=1_000 {
        let step = engine.vacuum_step().await.expect("vacuum step");
        total += step.stats;
        if step.cycle_completed {
            return (total, steps);
        }
    }
    panic!("sweep cycle did not complete within 1000 steps");
}

/// Populate `table` with `rows` single-column bigint rows in bulk batches.
async fn load_rows(session: &mut SqlSession, table: &str, rows: u64) {
    let mut next = 1;
    while next <= rows {
        let batch_end = (next + 999).min(rows);
        let values: Vec<String> = (next..=batch_end).map(|i| format!("({i})")).collect();
        exec(
            session,
            &format!("INSERT INTO {table} VALUES {}", values.join(",")),
        )
        .await;
        next = batch_end + 1;
    }
}

#[tokio::test]
async fn chunked_steps_sweep_a_large_table_and_truncate_the_clog_only_at_cycle_end() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    exec(&mut session, "CREATE TABLE big (id BIGINT PRIMARY KEY)").await;
    // More rows than one step's key budget, so the pass MUST span steps.
    load_rows(&mut session, "big", 50_000).await;
    let table = engine
        .catalog_table(&RelationName::public("big"))
        .expect("table");
    let kv = engine.kv_handle();
    let floor_before = engine.clog_scan_lo().expect("scan lo");

    // The first step is budget-bounded: it must stop mid-table without
    // completing the cycle, and clog truncation must NOT fire mid-cycle.
    let first = engine.vacuum_step().await.expect("first step");
    assert!(!first.cycle_completed);
    assert!(first.keys_examined > 0 && first.keys_examined < 50_000);
    assert!(first.stats.clog_entries_pruned == 0);
    assert!(engine.clog_scan_lo().expect("scan lo") == floor_before);

    // Resuming from the cursor completes the pass; every insert-created
    // version is frozen exactly once (no chunk overlap, no gaps).
    let (rest, _) = run_steps_to_cycle_end(&engine).await;
    assert!(first.stats.versions_frozen + rest.versions_frozen == 50_000);
    // Completing the clean cycle truncates the clog and advances the floor.
    assert!(rest.clog_entries_pruned > 0);
    let floor = engine.clog_scan_lo().expect("scan lo");
    assert!(floor > floor_before);
    let below_floor = kv
        .scan_range(
            &crabka_pgkv::key::clog_key(0),
            &crabka_pgkv::key::clog_key(floor),
        )
        .expect("scan clog")
        .len();
    assert!(below_floor == 0, "clog below the floor must be empty");

    // With every surviving version settled and no writes since, the next
    // cycle proves the table clean from its demand counters and never scans.
    let idle = engine.vacuum_step().await.expect("idle step");
    assert!(idle.cycle_completed);
    assert!(idle.keys_examined == 0);
    assert!(idle.stats.versions_pruned == 0 && idle.stats.versions_frozen == 0);

    // Garbage created after a pass is found by the next pass: the deletions
    // re-dirty the table and a fresh chunked cycle reclaims exactly them.
    exec(&mut session, "DELETE FROM big WHERE id <= 1000").await;
    let (reclaim, _) = run_steps_to_cycle_end(&engine).await;
    assert!(reclaim.versions_pruned == 1_000);
    assert!(reclaim.index_entries_pruned == 1_000);
    assert!(table_version_count(kv.as_ref(), table.id) == 49_000);
    let rows = select_rows(&mut session, "SELECT count(*) FROM big").await;
    assert!(rows == vec![vec![Some("49000".to_owned())]]);
}

#[tokio::test]
async fn settled_tables_are_skipped_until_new_writes_re_dirty_them() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    exec(&mut session, "CREATE TABLE g (id BIGINT PRIMARY KEY)").await;
    exec(&mut session, "INSERT INTO g VALUES (1),(2),(3)").await;

    let stats = engine.vacuum().await.expect("vacuum");
    assert!(stats.versions_frozen == 3);

    // Settled and unwritten-to: subsequent cycles do no scan work at all.
    let idle = engine.vacuum_step().await.expect("idle step");
    assert!(idle.cycle_completed && idle.keys_examined == 0);

    // New cold garbage (an aborted insert) re-dirties the table; the next
    // pass finds it without help from the write path.
    exec(&mut session, "BEGIN").await;
    exec(&mut session, "INSERT INTO g VALUES (4)").await;
    exec(&mut session, "ROLLBACK").await;
    let (reclaim, _) = run_steps_to_cycle_end(&engine).await;
    assert!(reclaim.versions_pruned == 1);

    // And the table settles again afterwards.
    let idle = engine.vacuum_step().await.expect("second idle step");
    assert!(idle.cycle_completed && idle.keys_examined == 0);
}

#[tokio::test]
async fn aborted_delete_stamps_are_cleared_so_the_row_settles() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    exec(
        &mut session,
        "CREATE TABLE s (id BIGINT PRIMARY KEY, v TEXT)",
    )
    .await;
    exec(&mut session, "INSERT INTO s VALUES (1,'keep')").await;
    exec(&mut session, "BEGIN").await;
    exec(&mut session, "DELETE FROM s WHERE id = 1").await;
    exec(&mut session, "ROLLBACK").await;

    // The rolled-back delete left an aborted xmax stamp on the survivor; the
    // sweep freezes the version AND clears the dead stamp.
    let stats = engine.vacuum().await.expect("vacuum");
    assert!(stats.stamps_cleared == 1);
    assert!(stats.versions_frozen == 1);
    assert!(stats.versions_pruned == 0);

    // Clearing is invisible: the row is still there.
    let rows = select_rows(&mut session, "SELECT id, v FROM s").await;
    assert!(rows == vec![vec![Some("1".to_owned()), Some("keep".to_owned())]]);

    // Fully settled now (frozen xmin, no stamp): later cycles skip the table.
    let idle = engine.vacuum_step().await.expect("idle step");
    assert!(idle.cycle_completed && idle.keys_examined == 0);

    // The cleared row still updates and deletes normally afterwards.
    exec(&mut session, "UPDATE s SET v = 'new' WHERE id = 1").await;
    let rows = select_rows(&mut session, "SELECT id, v FROM s").await;
    assert!(rows == vec![vec![Some("1".to_owned()), Some("new".to_owned())]]);
    let table = engine
        .catalog_table(&RelationName::public("s"))
        .expect("table");
    let (_, _) = run_steps_to_cycle_end(&engine).await;
    assert!(table_version_count(engine.kv_handle().as_ref(), table.id) == 1);
}

// ── vacuum advances the durable clog scan floor ──────────────────────────────

#[tokio::test]
async fn vacuum_advances_the_durable_clog_scan_floor() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    exec(&mut session, "CREATE TABLE f (id BIGINT PRIMARY KEY)").await;
    for i in 0..5 {
        exec(&mut session, &format!("INSERT INTO f VALUES ({i})")).await;
    }
    assert!(engine.clog_scan_lo().expect("scan lo") == 0);

    engine.vacuum().await.expect("vacuum");

    // Every allocated xid is decided, so the floor advances to the sweep's
    // horizon, keeping future horizon walks O(1). The current horizon may sit
    // one past it (the sweep's own lock-owner xid was allocated afterwards)
    // but never below it.
    let floor = engine.clog_scan_lo().expect("scan lo");
    assert!(
        floor > crabka_pgmvcc::xid::FIRST_NORMAL_XID,
        "expected the clog scan floor to advance, found {floor}"
    );
    assert!(floor <= engine.checkpoint_garbage_horizon().expect("horizon"));
}

// ── the sweep never waits on a live transaction ──────────────────────────────

/// A sweep step takes each row's exclusive lock, and its caller holds the
/// maintenance gate for the whole step -- the same gate every statement takes
/// to be admitted, `COMMIT` and `ROLLBACK` included. So a step that *waited*
/// for a row an open transaction holds would be waiting for a lock only that
/// transaction can release, through a statement the step is itself blocking.
///
/// That deadlock wedged a real `pg_regress` run: the server stopped answering
/// anything, including new connections, with no statement in flight to report.
/// The step must skip a contended row instead, so it returns while the writer
/// still holds its lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sweep_step_skips_a_row_an_open_transaction_holds_instead_of_waiting() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    exec(
        &mut session,
        "CREATE TABLE held (id INT PRIMARY KEY, v TEXT)",
    )
    .await;
    exec(&mut session, "INSERT INTO held VALUES (1,'a'),(2,'b')").await;
    // Dead versions for the sweep to find.
    for i in 0..4 {
        exec(
            &mut session,
            &format!("UPDATE held SET v='v{i}' WHERE id=1"),
        )
        .await;
        exec(
            &mut session,
            &format!("UPDATE held SET v='w{i}' WHERE id=2"),
        )
        .await;
    }

    // An open transaction holding a row lock, exactly as the wedged run did.
    let mut holder = engine.connect();
    exec(&mut holder, "BEGIN").await;
    exec(&mut holder, "UPDATE held SET v='locked' WHERE id=1").await;

    // Before the fix this never returned.
    let step = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        engine.vacuum_step_budgeted(1024),
    )
    .await
    .expect("a sweep step must not wait for a live transaction's row lock")
    .expect("vacuum step");

    // It ran rather than bailing out.
    assert!(step.keys_examined > 0);

    // The holder is unaffected and can still finish, which is the property the
    // deadlock destroyed.
    exec(&mut holder, "ROLLBACK").await;
    assert!(
        select_rows(&mut session, "SELECT v FROM held WHERE id=1").await
            == vec![vec![Some("v3".to_owned())]]
    );
}
