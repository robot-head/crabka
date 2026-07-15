//! Opportunistic dead-version pruning on single-node engines: hot-row UPDATE
//! chains stay bounded (in-memory and fjall), leased snapshots hold pruning
//! back exactly as long as a reader can still see the old versions, and
//! hot-row throughput stabilizes instead of decaying with chain length.
//!
//! NOTE: this file is named `version_pruning.rs` so its compiled test binary
//! does not contain the substring `update` (see the "UAC-safe target names"
//! policy in CLAUDE.md).

use assert2::assert;
use crabka_pgexec::SqlEngine;
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn run(s: &mut impl Session, sql: &str) -> Vec<QueryResult> {
    s.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
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
        other => panic!("expected Rows, got {other:?}"),
    }
}

/// Number of primary-index version keys stored for `table` (all rows).
fn stored_versions(engine: &SqlEngine, table: &str) -> usize {
    let table = engine.catalog_table(table).expect("table");
    engine
        .kv_handle()
        .scan_prefix(&crabka_pgkv::key::table_prefix(table.id))
        .expect("scan versions")
        .len()
}

async fn assert_hot_row_chain_stays_bounded(engine: &SqlEngine, iterations: u32) {
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE hot (id int4, v int4)").await;
    run(&mut s, "INSERT INTO hot VALUES (1, 0), (2, 0)").await;
    for _ in 0..iterations {
        run(&mut s, "UPDATE hot SET v = v + 1 WHERE id = 1").await;
    }

    let result = run(&mut s, "SELECT v FROM hot WHERE id = 1").await;
    assert!(col0(&result[0]) == vec![Some(iterations.to_string())]);
    // Steady state: the hot row keeps its live version plus the one superseded
    // by the in-flight statement; the cold row keeps one.
    let versions = stored_versions(engine, "hot");
    assert!(
        versions <= 4,
        "dead versions must be pruned, found {versions} stored versions after {iterations} hot-row rewrites"
    );
}

#[tokio::test]
async fn hot_row_chain_stays_bounded_in_memory() {
    assert_hot_row_chain_stays_bounded(&SqlEngine::new(), 500).await;
}

#[tokio::test]
async fn hot_row_chain_stays_bounded_on_fjall() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = SqlEngine::open(dir.path()).expect("open fjall engine");
    assert_hot_row_chain_stays_bounded(&engine, 300).await;
}

#[tokio::test]
async fn repeatable_read_snapshot_holds_pruning_back_until_released() {
    let engine = SqlEngine::new();
    let mut writer = engine.connect();
    run(&mut writer, "CREATE TABLE t (id int4, v int4)").await;
    run(&mut writer, "INSERT INTO t VALUES (1, 0)").await;

    let mut reader = engine.connect();
    run(&mut reader, "BEGIN ISOLATION LEVEL REPEATABLE READ").await;
    let before = run(&mut reader, "SELECT v FROM t WHERE id = 1").await;
    assert!(col0(&before[0]) == vec![Some("0".into())]);

    for _ in 0..40 {
        run(&mut writer, "UPDATE t SET v = v + 1 WHERE id = 1").await;
    }

    // The reader's leased snapshot pins the garbage horizon: its version is
    // still stored and still visible.
    let versions_while_pinned = stored_versions(&engine, "t");
    assert!(
        versions_while_pinned > 40,
        "a live REPEATABLE READ snapshot must hold every version it can see \
         (found {versions_while_pinned})"
    );
    let held = run(&mut reader, "SELECT v FROM t WHERE id = 1").await;
    assert!(col0(&held[0]) == vec![Some("0".into())]);
    run(&mut reader, "COMMIT").await;

    // With the lease released, the next write of the row collapses the chain.
    run(&mut writer, "UPDATE t SET v = v + 1 WHERE id = 1").await;
    let versions = stored_versions(&engine, "t");
    assert!(versions <= 3, "found {versions} stored versions");
    let after = run(&mut writer, "SELECT v FROM t WHERE id = 1").await;
    assert!(col0(&after[0]) == vec![Some("41".into())]);
}

#[tokio::test]
async fn aborted_writes_are_pruned_from_the_chain() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE t (id int4, v int4)").await;
    run(&mut s, "INSERT INTO t VALUES (1, 0)").await;

    for _ in 0..10 {
        run(&mut s, "BEGIN").await;
        run(&mut s, "UPDATE t SET v = v + 100 WHERE id = 1").await;
        run(&mut s, "ROLLBACK").await;
    }

    // A later committed write sweeps the aborted versions out.
    run(&mut s, "UPDATE t SET v = v + 1 WHERE id = 1").await;
    let versions = stored_versions(&engine, "t");
    assert!(versions <= 3, "found {versions} stored versions");
    let after = run(&mut s, "SELECT v FROM t WHERE id = 1").await;
    assert!(col0(&after[0]) == vec![Some("1".into())]);
}

/// The measured failure mode this guards against: every UPDATE of one row
/// appended a dead version forever, so update N re-scanned N versions and
/// throughput decayed hyperbolically (about 20x slower after 24k rewrites of
/// one row). With pruning, the chain length — and so the per-statement cost —
/// is flat, and late windows run at the same speed as early ones.
#[tokio::test]
async fn hot_row_rewrite_throughput_stabilizes_instead_of_decaying() {
    const WINDOW: u32 = 1_000;
    const WINDOWS: u32 = 10;

    let engine = SqlEngine::new();
    let mut s = engine.connect();
    run(&mut s, "CREATE TABLE hot (id int4, v int4)").await;
    run(&mut s, "INSERT INTO hot VALUES (1, 0), (2, 0)").await;

    let mut window_seconds = Vec::new();
    for _ in 0..WINDOWS {
        let started = std::time::Instant::now();
        for _ in 0..WINDOW {
            run(&mut s, "UPDATE hot SET v = v + 1 WHERE id = 1").await;
        }
        window_seconds.push(started.elapsed().as_secs_f64());
    }

    let result = run(&mut s, "SELECT v FROM hot WHERE id = 1").await;
    assert!(col0(&result[0]) == vec![Some((WINDOW * WINDOWS).to_string())]);

    // Deterministic guard: the chain itself must stay flat.
    let versions = stored_versions(&engine, "hot");
    assert!(versions <= 4, "found {versions} stored versions");

    // Throughput guard: the last thousand rewrites may not be more than 4x
    // slower than the first thousand. Without pruning the ratio is the ratio
    // of mean chain lengths (about 19x here), far outside scheduler noise.
    let first = window_seconds.first().copied().expect("first window");
    let last = window_seconds.last().copied().expect("last window");
    assert!(
        last <= first * 4.0,
        "hot-row rewrite throughput decayed: first window {first:.3}s, last window {last:.3}s \
         (all windows: {window_seconds:?})"
    );
}
