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
