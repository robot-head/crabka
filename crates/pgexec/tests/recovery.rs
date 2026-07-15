//! Lazy crash recovery: versions written by a transaction that never recorded a
//! clog commit are invisible after the store is reopened (the `ProcArray` starts
//! empty, so the in-progress xid is in no snapshot).

use std::sync::Arc;

use crabka_pgexec::{SqlEngine, SqlSession};
use crabka_pgkv::{Kv, MemKv};
use crabka_pgmvcc::xid::{FIRST_NORMAL_XID, FROZEN_XID, INVALID_XID};
use crabka_pgwire::engine::{Engine, QueryResult, Session};

fn count(r: &QueryResult) -> usize {
    match r {
        QueryResult::Rows { rows, .. } => rows.len(),
        other => panic!("expected Rows, got {other:?}"),
    }
}

async fn rows(s: &mut SqlSession, sql: &str) -> Vec<QueryResult> {
    s.simple_query(sql).await.expect("ok")
}

fn tuple_xmin(kv: &dyn Kv, table_name: &str, rowid: u64, xid: u64) -> Option<u64> {
    let table = crabka_pgcatalog::get_table(kv, table_name).expect("table");
    let key = crabka_pgmvcc::version::version_key_xid(table.id, rowid, xid);
    let bytes = kv.get(&key).expect("tuple lookup")?;
    let (xmin, _xmax, _row) = crabka_pgmvcc::version::decode_tuple(&bytes).expect("tuple decode");
    Some(xmin)
}

#[tokio::test]
async fn fresh_engine_first_write_uses_first_normal_xid() {
    let kv = Arc::new(MemKv::new());
    let engine = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("engine");
    let mut s = engine.connect();

    rows(&mut s, "CREATE TABLE t (id int4)").await;
    rows(&mut s, "INSERT INTO t VALUES (1)").await;

    assert_eq!(
        tuple_xmin(kv.as_ref(), "t", 1, FIRST_NORMAL_XID),
        Some(FIRST_NORMAL_XID)
    );
    assert_eq!(tuple_xmin(kv.as_ref(), "t", 1, FROZEN_XID), None);
    assert_eq!(tuple_xmin(kv.as_ref(), "t", 1, INVALID_XID), None);
}

#[tokio::test]
async fn engine_clamps_reserved_persisted_next_xid_before_first_write() {
    for reserved in [INVALID_XID, FROZEN_XID] {
        let kv = Arc::new(MemKv::new());
        kv.put(
            crabka_pgkv::key::next_xid_key(),
            reserved.to_be_bytes().to_vec(),
        )
        .expect("seed reserved next_xid");
        let engine = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("engine");
        let mut s = engine.connect();

        rows(&mut s, "CREATE TABLE t (id int4)").await;
        rows(&mut s, "INSERT INTO t VALUES (1)").await;

        assert_eq!(
            tuple_xmin(kv.as_ref(), "t", 1, FIRST_NORMAL_XID),
            Some(FIRST_NORMAL_XID)
        );
        assert_eq!(tuple_xmin(kv.as_ref(), "t", 1, reserved), None);
    }
}

#[tokio::test]
async fn uncommitted_versions_are_invisible_after_reopen() {
    let dir = tempfile::tempdir().expect("tmp");
    {
        let engine = SqlEngine::open(dir.path()).expect("open");
        let mut s = engine.connect();
        rows(&mut s, "CREATE TABLE t (id int4)").await;
        rows(&mut s, "BEGIN").await;
        rows(&mut s, "INSERT INTO t VALUES (1),(2),(3)").await;
        // Drop WITHOUT commit: the engine is dropped mid-transaction (a crash).
        // The versions are on disk (write-through) but the clog has no entry.
    }
    let engine = SqlEngine::open(dir.path()).expect("reopen");
    let mut s = engine.connect();
    assert_eq!(
        count(&rows(&mut s, "SELECT id FROM t").await[0]),
        0,
        "in-progress rows invisible"
    );
    // The table still works for new writes after recovery.
    rows(&mut s, "INSERT INTO t VALUES (9)").await;
    assert_eq!(count(&rows(&mut s, "SELECT id FROM t").await[0]), 1);
}

#[tokio::test]
async fn committed_versions_survive_reopen() {
    let dir = tempfile::tempdir().expect("tmp");
    {
        let engine = SqlEngine::open(dir.path()).expect("open");
        let mut s = engine.connect();
        rows(&mut s, "CREATE TABLE t (id int4)").await;
        rows(&mut s, "BEGIN").await;
        rows(&mut s, "INSERT INTO t VALUES (1),(2)").await;
        rows(&mut s, "COMMIT").await;
    }
    let engine = SqlEngine::open(dir.path()).expect("reopen");
    let mut s = engine.connect();
    assert_eq!(count(&rows(&mut s, "SELECT id FROM t").await[0]), 2);
}

#[tokio::test]
async fn xid_is_not_reused_after_reopen() {
    // After a crashed (uncommitted) txn, a fresh txn that commits must be
    // visible — i.e. the new xid did not collide with the crashed one (next_xid
    // is durable). If reuse happened, the new rows could inherit invisibility.
    let dir = tempfile::tempdir().expect("tmp");
    {
        let engine = SqlEngine::open(dir.path()).expect("open");
        let mut s = engine.connect();
        rows(&mut s, "CREATE TABLE t (id int4)").await;
        rows(&mut s, "BEGIN").await;
        rows(&mut s, "INSERT INTO t VALUES (1)").await; // allocates an xid, never commits
    }
    let engine = SqlEngine::open(dir.path()).expect("reopen");
    let mut s = engine.connect();
    rows(&mut s, "INSERT INTO t VALUES (2)").await; // autocommit, new xid, commits
    let r = rows(&mut s, "SELECT id FROM t").await;
    assert_eq!(count(&r[0]), 1, "only the committed row 2 is visible");
}

#[tokio::test]
async fn leaked_block_xids_settle_as_aborted_and_never_wedge_the_horizon() {
    use assert2::assert;

    // The durable next-xid counter is persisted a BLOCK ahead of hand-out, so a
    // crash leaks handed-out-but-undecided xids (the crashed txn's) plus the
    // whole unused remainder of the block — none of which have clog entries.
    // After reopen: (1) new xids resume past the persisted reservation (no
    // collision), (2) the garbage horizon advances past the leaked range
    // (absent clog entries never appear in the horizon's clog walk, so they
    // cannot wedge it), and (3) the crashed txn's versions settle as
    // decided-by-crash and are physically reclaimed by vacuum.
    let kv = Arc::new(MemKv::new());
    {
        let engine = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("engine");
        let mut s = engine.connect();
        rows(&mut s, "CREATE TABLE t (id int4)").await;
        rows(&mut s, "BEGIN").await;
        rows(&mut s, "INSERT INTO t VALUES (1),(2),(3)").await;
        // Crash mid-block: the engine (ProcArray, running set) is dropped; the
        // store — including the block-ahead counter — survives.
    }
    let persisted = kv
        .get(&crabka_pgkv::key::next_xid_key())
        .expect("get")
        .map(|b| u64::from_be_bytes(b.try_into().expect("u64")))
        .expect("counter persisted before any xid was handed out");

    let engine = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("reopen");
    let mut s = engine.connect();
    // (1) No xid reuse: the first post-crash write draws from at or above the
    // persisted reservation and its committed row is visible.
    rows(&mut s, "INSERT INTO t VALUES (9)").await;
    let table = engine.catalog_table("t").expect("table");
    let version_xmins = |kv: &dyn Kv| -> Vec<u64> {
        kv.scan_prefix(&crabka_pgkv::key::table_prefix(table.id))
            .expect("scan")
            .iter()
            .map(|(_, bytes)| {
                let (xmin, _xmax, _row) =
                    crabka_pgmvcc::version::decode_tuple(bytes).expect("tuple decode");
                xmin
            })
            .collect()
    };
    let xmins = version_xmins(kv.as_ref());
    assert!(xmins.len() == 4, "3 crashed versions + 1 committed");
    assert!(
        xmins.iter().filter(|&&xmin| xmin >= persisted).count() == 1,
        "the post-crash xid resumed at or past the persisted counter"
    );
    assert!(
        xmins.iter().filter(|&&xmin| xmin < persisted).count() == 3,
        "the crashed txn's xid stays below the persisted counter"
    );
    let r = rows(&mut s, "SELECT id FROM t").await;
    assert!(count(&r[0]) == 1, "crashed rows invisible, new row visible");

    // (2) The horizon advances past every leaked xid: absent clog entries are
    // not visited by the horizon's clog scan, so the leaked range [crashed
    // txn's xid, persisted) cannot hold it back.
    let horizon = engine.checkpoint_garbage_horizon().expect("horizon");
    assert!(horizon >= persisted, "horizon passed the leaked xid range");

    // (3) The crashed txn's versions read as decided-by-crash below the
    // horizon and vacuum physically reclaims them; the committed row survives.
    engine.vacuum().await.expect("vacuum");
    assert!(
        version_xmins(kv.as_ref()).len() == 1,
        "only the committed row's version remains"
    );
    let r = rows(&mut s, "SELECT id FROM t").await;
    assert!(count(&r[0]) == 1, "vacuum preserved the committed row");
}
