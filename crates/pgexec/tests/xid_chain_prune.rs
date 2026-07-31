//! Write-path xid chain pruning on replicated engines: superseded row
//! versions are reclaimed in the same commit batch as the write, so hot-row
//! chains stay bounded on the engine kind the multi-range cluster actually
//! runs (`SqlEngine::replicated`), where the background vacuum never runs.

use std::sync::Arc;

use assert2::assert;
use crabka_pgcatalog::RelationName;
use crabka_pgexec::{Committer, ExecError, LocalLinearizer, SqlEngine};
use crabka_pgkv::{Kv, MemKv, WriteOp};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session};

/// Applies batches straight to the store, standing in for the replicated
/// state machine (which applies exactly the same batches in WAL order).
struct StoreCommitter {
    kv: Arc<dyn Kv>,
}

#[async_trait::async_trait]
impl Committer for StoreCommitter {
    async fn commit(&self, ops: Vec<WriteOp>) -> Result<(), ExecError> {
        self.kv.write_batch(&ops)?;
        Ok(())
    }
}

/// Count the stored xid tuple versions of `table_id` (the whole physical
/// chain across every row and version key).
fn xid_version_count(kv: &dyn Kv, table_id: u32) -> usize {
    kv.scan_prefix(&crabka_pgkv::key::table_prefix(table_id))
        .expect("scan")
        .iter()
        .filter(|(_, value)| crabka_pgmvcc::version::decode_tuple(value).is_ok())
        .count()
}

#[tokio::test]
async fn replicated_engine_update_loop_keeps_version_chain_bounded() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let engine = SqlEngine::replicated(
        Arc::clone(&kv),
        Arc::clone(&kv),
        Arc::new(StoreCommitter {
            kv: Arc::clone(&kv),
        }),
        Arc::new(LocalLinearizer),
    )
    .expect("replicated engine");
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE hot (id int4, v int4)")
        .await
        .expect("create table");
    session
        .simple_query("INSERT INTO hot VALUES (1, 0)")
        .await
        .expect("seed row");

    for i in 1..=100 {
        session
            .simple_query(&format!("UPDATE hot SET v = {i} WHERE id = 1"))
            .await
            .expect("update");
    }

    // The row is current and correct.
    let results = session
        .simple_query("SELECT v FROM hot WHERE id = 1")
        .await
        .expect("read back");
    let QueryResult::Rows { rows, .. } = &results[0] else {
        panic!("expected rows, got {results:?}");
    };
    assert!(rows.len() == 1);
    let cell: &Cell = rows[0][0].as_ref().expect("non-null v");
    assert!(cell.text.as_ref() == b"100");

    // Superseded versions were pruned inside the update batches themselves:
    // the physical chain stays O(1) instead of holding all 100 versions.
    // (Each statement keeps the version it supersedes — its deleter commits
    // only with the batch — plus the new version, so a handful survive.)
    let table = crabka_pgcatalog::get_table(engine.catalog_kv(), &RelationName::public("hot"))
        .expect("table");
    let versions = xid_version_count(kv.as_ref(), table.id);
    assert!(versions <= 3, "chain grew to {versions} versions");
}
