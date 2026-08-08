//! Open a durable engine, write, drop it, reopen, and assert everything
//! survived. This includes the rowid allocator, which is the SP2 carry-over fix.

use crabka_pgexec::SqlEngine;
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

fn text(cell: Option<&Cell>) -> Option<String> {
    cell.map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
}

async fn rows(engine: &SqlEngine, sql: &str) -> Vec<Vec<Option<Cell>>> {
    let mut results = engine.connect().simple_query(sql).await.expect("query");
    match results.remove(0) {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got {other:?}"),
    }
}

#[tokio::test]
async fn data_schema_and_rowid_survive_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let engine = SqlEngine::open(dir.path()).expect("open");
        engine
            .connect()
            .simple_query("CREATE TABLE t (id int4, name text)")
            .await
            .expect("create");
        engine
            .connect()
            .simple_query("INSERT INTO t VALUES (1,'a'),(2,'b')")
            .await
            .expect("insert");
        // engine dropped here — writes were fsynced per statement.
    }

    let engine = SqlEngine::open(dir.path()).expect("reopen");
    // Rows + schema survived.
    let got = rows(&engine, "SELECT name FROM t ORDER BY id").await;
    assert_eq!(
        got.iter().map(|r| text(r[0].as_ref())).collect::<Vec<_>>(),
        vec![Some("a".into()), Some("b".into())]
    );
    // The rowid allocator survived: a new insert does NOT collide with id 1/2.
    // (rowids are the hidden key, not the id column; insert two more and confirm
    // all four rows are present and distinct.)
    engine
        .connect()
        .simple_query("INSERT INTO t VALUES (3,'c')")
        .await
        .expect("insert after reopen");
    let after = rows(&engine, "SELECT name FROM t ORDER BY id").await;
    assert_eq!(
        after.len(),
        3,
        "all rows present, no overwrite from a reset rowid"
    );
    assert_eq!(
        after
            .iter()
            .map(|r| text(r[0].as_ref()))
            .collect::<Vec<_>>(),
        vec![Some("a".into()), Some("b".into()), Some("c".into())]
    );
}

#[tokio::test]
async fn unique_local_index_enforcement_survives_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let engine = SqlEngine::open(dir.path()).expect("open");
        engine
            .connect()
            .simple_query(
                "CREATE TABLE t (id int4, name text);
                 INSERT INTO t VALUES (1, 'a');
                 CREATE UNIQUE INDEX t_name_idx ON t (name)",
            )
            .await
            .expect("create unique index");
    }

    let engine = SqlEngine::open(dir.path()).expect("reopen");
    let err = engine
        .connect()
        .simple_query("INSERT INTO t VALUES (2, 'a')")
        .await
        .expect_err("duplicate after reopen");

    assert_eq!(err.code, "23505");
}

#[tokio::test]
async fn committed_transaction_survives_reopen() {
    let dir = tempfile::tempdir().expect("tmp");
    {
        let engine = SqlEngine::open(dir.path()).expect("open");
        engine
            .connect()
            .simple_query("CREATE TABLE t (id int4, name text)")
            .await
            .expect("create");
        // Use the same session for the transaction so state is kept.
        let mut s = engine.connect();
        s.simple_query("BEGIN").await.expect("begin");
        s.simple_query("INSERT INTO t VALUES (1,'a'),(2,'b')")
            .await
            .expect("insert");
        s.simple_query("UPDATE t SET name = 'z' WHERE id = 2")
            .await
            .expect("update");
        s.simple_query("COMMIT").await.expect("commit");
    } // drop closes the store

    let engine = SqlEngine::open(dir.path()).expect("reopen");
    let got = rows(&engine, "SELECT name FROM t ORDER BY id").await;
    assert_eq!(
        got.iter().map(|r| text(r[0].as_ref())).collect::<Vec<_>>(),
        vec![Some("a".into()), Some("z".into())]
    );
}

#[tokio::test]
async fn rolled_back_transaction_leaves_nothing() {
    let dir = tempfile::tempdir().expect("tmp");
    {
        let engine = SqlEngine::open(dir.path()).expect("open");
        engine
            .connect()
            .simple_query("CREATE TABLE t (id int4)")
            .await
            .expect("create");
        let mut s = engine.connect();
        s.simple_query("BEGIN").await.expect("begin");
        s.simple_query("INSERT INTO t VALUES (1),(2),(3)")
            .await
            .expect("insert");
        s.simple_query("ROLLBACK").await.expect("rollback");
    }

    let engine = SqlEngine::open(dir.path()).expect("reopen");
    // Table exists (DDL is non-transactional) but holds no rows.
    let got = rows(&engine, "SELECT id FROM t").await;
    assert!(got.is_empty(), "rolled-back rows must not survive a reopen");
}

#[tokio::test]
async fn drop_and_recreate_survive_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let engine = SqlEngine::open(dir.path()).expect("open");
        engine
            .connect()
            .simple_query("CREATE TABLE t (id int4)")
            .await
            .expect("create");
        engine
            .connect()
            .simple_query("INSERT INTO t VALUES (1)")
            .await
            .expect("insert");
        engine
            .connect()
            .simple_query("DROP TABLE t")
            .await
            .expect("drop");
        engine
            .connect()
            .simple_query("CREATE TABLE t (id int4)")
            .await
            .expect("recreate");
    }
    let engine = SqlEngine::open(dir.path()).expect("reopen");
    // The recreated (empty) table survived; the dropped rows did not resurrect.
    let got = rows(&engine, "SELECT id FROM t").await;
    assert!(
        got.is_empty(),
        "dropped rows must not survive; recreated table is empty"
    );
}
