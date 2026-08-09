//! D5: the read gate, `Linearizer`, rejects reads on a deposed leader and
//! admits them on a healthy one. Writes are never gated, because they go
//! through the committer.
use std::sync::Arc;

use crabka_pgexec::{Committer, ExecError, Linearizer, SqlEngine};
use crabka_pgkv::{Kv, MemKv, WriteOp};
use crabka_pgwire::engine::{Engine, QueryResult, Session};

/// Commits directly to a shared in-memory KV. It stands in for
/// `RaftCommitter`.
struct MemCommitter {
    kv: Arc<dyn Kv>,
}
#[async_trait::async_trait]
impl Committer for MemCommitter {
    async fn commit(&self, ops: Vec<WriteOp>) -> Result<(), ExecError> {
        self.kv.write_batch(&ops)?;
        Ok(())
    }
}

/// A read gate that always rejects, which is a deposed or partitioned
/// leader.
struct DeposedLeader;
#[async_trait::async_trait]
impl Linearizer for DeposedLeader {
    async fn ensure_readable(&self) -> Result<(), ExecError> {
        Err(ExecError::NotLeader)
    }
}

/// A read gate that always admits, which is a healthy leader.
struct HealthyLeader;
#[async_trait::async_trait]
impl Linearizer for HealthyLeader {
    async fn ensure_readable(&self) -> Result<(), ExecError> {
        Ok(())
    }
}

fn engine(linearizer: Arc<dyn Linearizer>) -> SqlEngine {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    SqlEngine::replicated(
        // Single-range test: catalog and data share one store.
        Arc::clone(&kv),
        Arc::clone(&kv),
        Arc::new(MemCommitter {
            kv: Arc::clone(&kv),
        }),
        linearizer,
    )
    .expect("replicated engine")
}

#[tokio::test]
async fn deposed_leader_rejects_autocommit_read_with_40001() {
    let mut s = engine(Arc::new(DeposedLeader)).connect();
    // DDL + writes are NOT gated (they go through the committer) → succeed.
    s.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    s.simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    // The read IS gated → rejected with the retryable 40001, no rows.
    let err = s
        .simple_query("SELECT id FROM t")
        .await
        .expect_err("read must be rejected on a deposed leader");
    assert_eq!(
        err.code, "40001",
        "deposed-leader read maps to retryable 40001"
    );
}

#[tokio::test]
async fn deposed_leader_gates_read_committed_in_txn_select_not_begin() {
    let mut s = engine(Arc::new(DeposedLeader)).connect();
    s.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    // Plain BEGIN is READ COMMITTED → its placeholder snapshot is refreshed per
    // statement, so BEGIN itself is NOT gated.
    s.simple_query("BEGIN")
        .await
        .expect("plain begin is not gated");
    let err = s
        .simple_query("SELECT id FROM t")
        .await
        .expect_err("RC in-txn select is gated");
    assert_eq!(err.code, "40001");
    s.simple_query("ROLLBACK").await.ok();
}

#[tokio::test]
async fn deposed_leader_gates_repeatable_read_at_begin() {
    let mut s = engine(Arc::new(DeposedLeader)).connect();
    s.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    // REPEATABLE READ fixes its snapshot at BEGIN, so the gate fires at BEGIN.
    let err = s
        .simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect_err("RR begin is gated");
    assert_eq!(err.code, "40001");
}

#[tokio::test]
async fn healthy_leader_admits_reads() {
    let mut s = engine(Arc::new(HealthyLeader)).connect();
    s.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    s.simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    let res = s
        .simple_query("SELECT id FROM t")
        .await
        .expect("read admitted");
    match &res[0] {
        QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
        other => panic!("expected Rows, got {other:?}"),
    }
}

#[tokio::test]
async fn healthy_leader_admits_repeatable_read_in_txn() {
    let mut s = engine(Arc::new(HealthyLeader)).connect();
    s.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    s.simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    // RR gates at BEGIN; a healthy leader admits, and the in-txn SELECT reuses the
    // begin-gated snapshot (exercises the admit path through the fixed-snapshot branch).
    s.simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("rr begin admitted");
    let res = s
        .simple_query("SELECT id FROM t")
        .await
        .expect("in-txn read admitted");
    match &res[0] {
        QueryResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
        other => panic!("expected Rows, got {other:?}"),
    }
    s.simple_query("COMMIT").await.expect("commit");
}

#[tokio::test]
async fn deposed_leader_gates_locking_select() {
    let mut s = engine(Arc::new(DeposedLeader)).connect();
    s.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    // Autocommit FOR UPDATE returns rows to the client → it is gated (before any
    // xid/lock allocation).
    let err = s
        .simple_query("SELECT id FROM t FOR UPDATE")
        .await
        .expect_err("autocommit FOR UPDATE is gated");
    assert_eq!(err.code, "40001");
    // RC in-txn: plain BEGIN is not gated, but the locking SELECT is.
    s.simple_query("BEGIN")
        .await
        .expect("plain begin not gated");
    let err = s
        .simple_query("SELECT id FROM t FOR UPDATE")
        .await
        .expect_err("in-txn FOR UPDATE is gated");
    assert_eq!(err.code, "40001");
    s.simple_query("ROLLBACK").await.ok();
}
