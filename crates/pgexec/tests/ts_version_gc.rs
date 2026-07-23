//! Timestamp-version reclamation: write-path opportunistic pruning of
//! sharded-table version chains, reclaim-floor admission, and pinned-read
//! protection (see `crabka_pgexec::ts_gc`).

use std::{sync::Arc, time::Duration};

use assert2::assert;
use crabka_pgexec::{ExecError, RowInterval, SqlEngine, TimestampWrite, timestamp_txn};
use crabka_pgkv::{Kv, MemKv};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

async fn exec(session: &mut crabka_pgexec::SqlSession, sql: &str) {
    session.simple_query(sql).await.expect("statement");
}

async fn rows(session: &mut crabka_pgexec::SqlSession, sql: &str) -> Vec<Vec<Option<Cell>>> {
    match session.simple_query(sql).await.expect("query").remove(0) {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got {other:?}"),
    }
}

async fn int_cell(session: &mut crabka_pgexec::SqlSession, sql: &str) -> String {
    let rows = rows(session, sql).await;
    assert!(rows.len() == 1, "expected one row");
    let cell = rows[0][0].as_ref().expect("non-null cell");
    String::from_utf8(cell.text.to_vec()).expect("utf8")
}

/// Count the stored timestamp tuple versions of a table (its whole physical
/// chain across every row).
fn ts_version_count(kv: &dyn Kv, table_name: &str) -> usize {
    let table = crabka_pgcatalog::get_table(kv, table_name).expect("table");
    kv.scan_prefix(&crabka_pgkv::key::table_prefix(table.id))
        .expect("scan")
        .iter()
        .filter(|(_, value)| crabka_pgmvcc::version::decode_ts_tuple(value).is_ok())
        .count()
}

async fn engine_with_hot_row() -> (Arc<MemKv>, SqlEngine, crabka_pgexec::SqlSession) {
    let kv = Arc::new(MemKv::new());
    let engine = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("engine");
    engine.ts_version_gc().set_floor_lag(Duration::ZERO);
    let mut session = engine.connect();
    exec(&mut session, "CREATE TABLE hot (id int4, v int4) SHARDED").await;
    exec(&mut session, "INSERT INTO hot VALUES (1, 0)").await;
    (kv, engine, session)
}

#[tokio::test]
async fn update_heavy_loop_keeps_hot_row_version_chain_bounded() {
    let (kv, _engine, mut session) = engine_with_hot_row().await;

    for i in 1..=100 {
        exec(
            &mut session,
            &format!("UPDATE hot SET v = {i} WHERE id = 1"),
        )
        .await;
    }

    // Each commit reclaims the versions its predecessor superseded, so the
    // chain stays O(1) instead of one version per update: the covering
    // version at the floor, the newest version, and at most one straggler.
    let chain = ts_version_count(kv.as_ref(), "hot");
    assert!(chain <= 3, "chain length {chain} after 100 updates");
    assert!(int_cell(&mut session, "SELECT v FROM hot WHERE id = 1").await == "100");
}

#[tokio::test]
async fn pinned_repeatable_read_holds_reclamation_until_release() {
    let (kv, engine, mut writer) = engine_with_hot_row().await;
    let mut reader = engine.connect();

    exec(&mut reader, "BEGIN").await;
    exec(
        &mut reader,
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
    )
    .await;
    assert!(int_cell(&mut reader, "SELECT v FROM hot WHERE id = 1").await == "0");

    for i in 1..=30 {
        exec(&mut writer, &format!("UPDATE hot SET v = {i} WHERE id = 1")).await;
    }

    // The open snapshot's pin holds the reclaim floor: its version survives
    // (snapshot stability) and the superseding chain accumulates unpruned.
    assert!(int_cell(&mut reader, "SELECT v FROM hot WHERE id = 1").await == "0");
    let held_chain = ts_version_count(kv.as_ref(), "hot");
    assert!(
        held_chain > 20,
        "pin must hold reclamation, chain {held_chain}"
    );

    exec(&mut reader, "COMMIT").await;
    for i in 31..=32 {
        exec(&mut writer, &format!("UPDATE hot SET v = {i} WHERE id = 1")).await;
    }

    // With the pin released, the next commits reclaim the backlog.
    let released_chain = ts_version_count(kv.as_ref(), "hot");
    assert!(
        released_chain <= 3,
        "chain length {released_chain} after pin release"
    );
    assert!(int_cell(&mut reader, "SELECT v FROM hot WHERE id = 1").await == "32");
}

#[tokio::test]
async fn per_statement_reclamation_work_is_capped_and_amortizes() {
    let (kv, engine, mut session) = engine_with_hot_row().await;
    // An effectively infinite lag disables reclamation while the backlog builds.
    engine
        .ts_version_gc()
        .set_floor_lag(Duration::from_secs(u64::MAX / 1_000));

    for i in 1..=80 {
        exec(
            &mut session,
            &format!("UPDATE hot SET v = {i} WHERE id = 1"),
        )
        .await;
    }
    assert!(ts_version_count(kv.as_ref(), "hot") == 81);

    engine.ts_version_gc().set_floor_lag(Duration::ZERO);
    // One statement reclaims at most the per-row cap (64), oldest first...
    exec(&mut session, "UPDATE hot SET v = 81 WHERE id = 1").await;
    assert!(ts_version_count(kv.as_ref(), "hot") == 81 + 1 - 64);

    // ...and the next statement finishes the backlog.
    exec(&mut session, "UPDATE hot SET v = 82 WHERE id = 1").await;
    let chain = ts_version_count(kv.as_ref(), "hot");
    assert!(chain <= 3, "chain length {chain} after backlog drained");
    assert!(int_cell(&mut session, "SELECT v FROM hot WHERE id = 1").await == "82");
}

#[tokio::test]
async fn reads_and_prewrites_below_the_published_floor_are_refused() {
    let (kv, engine, mut session) = engine_with_hot_row().await;
    for i in 1..=10 {
        exec(
            &mut session,
            &format!("UPDATE hot SET v = {i} WHERE id = 1"),
        )
        .await;
    }
    // Reclamation has published a durable floor.
    let floor = kv
        .get(b"\0\0\0\0meta/ts_gc_floor")
        .expect("floor read")
        .map(|bytes| u64::from_be_bytes(<[u8; 8]>::try_from(bytes).expect("floor width")))
        .expect("published floor");
    assert!(floor > 1);

    let table = crabka_pgcatalog::get_table(kv.as_ref(), "hot").expect("table");
    let snapshot = crabka_pgmvcc::visibility::Snapshot {
        xmin: 1,
        xmax: u64::MAX,
        xip: Vec::new(),
    };

    // A read below the floor may miss pruned history: refused, retryable.
    let stale = engine.scan_local_visible(
        &table,
        &snapshot,
        &snapshot,
        None,
        Some(timestamp_txn::ReadTimestamp::new(1).expect("read ts")),
        RowInterval::ALL,
    );
    assert!(let Err(ExecError::SerializationFailure) = stale);

    // The refusal is anchored in the DURABLE floor, not in-process state: a
    // fresh engine over the same store (a restart, a follower serving the
    // applied state) refuses the same read.
    let reopened = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("reopen");
    let stale_after_reopen = reopened.scan_local_visible(
        &table,
        &snapshot,
        &snapshot,
        None,
        Some(timestamp_txn::ReadTimestamp::new(1).expect("read ts")),
        RowInterval::ALL,
    );
    assert!(let Err(ExecError::SerializationFailure) = stale_after_reopen);

    // A read at (or above) the floor is served and sees the newest version.
    let fresh = engine
        .scan_local_visible(
            &table,
            &snapshot,
            &snapshot,
            None,
            Some(timestamp_txn::ReadTimestamp::MAX),
            RowInterval::ALL,
        )
        .expect("fresh read");
    assert!(fresh.len() == 1);
    assert!(fresh[0].row[1] == crabka_pgtypes::Datum::Int4(10));

    // A prewrite whose transaction started below the floor could have run its
    // conflict check over pruned history: refused as a retryable conflict.
    let participant = engine.timestamp_txn_participant(0);
    let stale_write = participant
        .prewrite(
            timestamp_txn::TimestampTransactionId::new(1).expect("start ts"),
            &[TimestampWrite {
                table_id: table.id,
                bucket: None,
                rowid: 1,
                row: vec![
                    crabka_pgtypes::Datum::Int4(1),
                    crabka_pgtypes::Datum::Int4(1),
                ],
                delete: false,
                global_index_intents: Vec::new(),
            }],
        )
        .await;
    assert!(let Err(ExecError::SerializationFailure) = stale_write);
}
