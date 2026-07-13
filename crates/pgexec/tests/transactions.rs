use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crabka_pgexec::{
    CommitTimestamp, PrimaryTxnDecision, RowInterval, SqlEngine, TimestampTransactionId,
    TimestampTxnDecision, TimestampTxnDescriptor, TimestampTxnIdentity, TimestampWrite,
    timestamp_txn::ReadTimestamp,
};
use crabka_pgkv::{Kv, MemKv};
use crabka_pgmvcc::{clog::XidStatus, xid::GLOBAL_XID_BASE};
use crabka_pgwire::engine::{Cell, Engine, QueryResult, Session, TxStatus};

struct InterleavingDescriptorCommitter {
    kv: Arc<dyn Kv>,
    acknowledgement_commits: AtomicUsize,
    acknowledgement_barrier: tokio::sync::Barrier,
}

#[async_trait::async_trait]
impl crabka_pgexec::Committer for InterleavingDescriptorCommitter {
    async fn commit(&self, ops: Vec<crabka_pgkv::WriteOp>) -> Result<(), crabka_pgexec::ExecError> {
        let is_acknowledgement = ops.iter().any(|op| {
            matches!(
                op,
                crabka_pgkv::WriteOp::ConditionalPut {
                    expected: Some(_),
                    ..
                }
            )
        });
        if is_acknowledgement && self.acknowledgement_commits.fetch_add(1, Ordering::SeqCst) < 2 {
            self.acknowledgement_barrier.wait().await;
        }
        self.kv.write_batch(&ops)?;
        Ok(())
    }
}

fn timestamp_write(table_id: u32, rowid: u64, value: i32) -> TimestampWrite {
    TimestampWrite {
        table_id,
        bucket: None,
        rowid,
        row: vec![crabka_pgtypes::Datum::Int4(value)],
        delete: false,
        global_index_intents: Vec::new(),
    }
}

fn timestamp_operation(
    range_id: u32,
    write: &TimestampWrite,
) -> crabka_pgexec::TimestampTxnOperation {
    crabka_pgexec::TimestampTxnOperation {
        range_id,
        table_id: write.table_id,
        bucket: write.bucket,
        rowid: write.rowid,
        delete: write.delete,
    }
}

fn timestamp_visible_rows(
    engine: &SqlEngine,
    table: &crabka_pgcatalog::Table,
    read_ts: ReadTimestamp,
) -> Vec<Vec<crabka_pgtypes::Datum>> {
    let snapshot = crabka_pgmvcc::visibility::Snapshot {
        xmin: 1,
        xmax: u64::MAX,
        xip: Vec::new(),
    };
    engine
        .scan_local_visible(
            table,
            &snapshot,
            &snapshot,
            None,
            Some(read_ts),
            RowInterval::ALL,
        )
        .expect("timestamp descriptor visibility scan")
        .into_iter()
        .map(|row| row.row)
        .collect()
}

#[allow(dead_code)]
fn text(c: Option<&Cell>) -> Option<String> {
    c.map(|c| String::from_utf8(c.text.to_vec()).expect("utf8"))
}
async fn rows(s: &mut crabka_pgexec::SqlSession, sql: &str) -> Vec<Vec<Option<Cell>>> {
    match s.simple_query(sql).await.expect("q").remove(0) {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("expected Rows, got {other:?}"),
    }
}

fn only_tuple_xmin(kv: &dyn Kv, table_name: &str) -> u64 {
    let table = crabka_pgcatalog::get_table(kv, table_name).expect("table");
    let prefix = crabka_pgkv::key::row_key(table.id, 1);
    let versions = kv.scan_prefix(&prefix).expect("scan versions");
    assert_eq!(versions.len(), 1, "expected exactly one tuple version");
    let (xmin, _, _) = crabka_pgmvcc::version::decode_tuple(&versions[0].1).expect("tuple");
    xmin
}

fn only_timestamp_version(kv: &dyn Kv, table_name: &str) -> crabka_pgmvcc::version::TsTupleVersion {
    let table = crabka_pgcatalog::get_table(kv, table_name).expect("table");
    let prefix = crabka_pgkv::key::row_key(table.id, 1);
    let versions = kv.scan_prefix(&prefix).expect("scan versions");
    assert_eq!(
        versions.len(),
        1,
        "expected exactly one timestamp tuple version"
    );
    crabka_pgmvcc::version::decode_ts_tuple(&versions[0].1).expect("timestamp tuple")
}

fn table_version_count(kv: &dyn Kv, table_name: &str) -> usize {
    let table = crabka_pgcatalog::get_table(kv, table_name).expect("table");
    kv.scan_prefix(&crabka_pgkv::key::table_prefix(table.id))
        .expect("scan table versions")
        .len()
}

fn table_timestamp_versions(
    kv: &dyn Kv,
    table_name: &str,
) -> Vec<crabka_pgmvcc::version::TsTupleVersion> {
    let table = crabka_pgcatalog::get_table(kv, table_name).expect("table");
    kv.scan_prefix(&crabka_pgkv::key::table_prefix(table.id))
        .expect("scan table versions")
        .into_iter()
        .map(|(_key, value)| crabka_pgmvcc::version::decode_ts_tuple(&value).expect("ts tuple"))
        .collect()
}

fn next_global_xid(kv: &dyn Kv) -> u64 {
    let Some(bytes) = kv
        .get(&crabka_pgkv::key::meta_next_global_xid_key())
        .expect("next global")
    else {
        return GLOBAL_XID_BASE;
    };
    u64::from_be_bytes(bytes.as_slice().try_into().expect("u64 next global"))
}

#[tokio::test]
async fn rollback_discards_writes() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    s.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    s.simple_query("BEGIN").await.expect("begin");
    assert_eq!(s.tx_status(), TxStatus::InTransaction);
    s.simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    assert_eq!(rows(&mut s, "SELECT id FROM t").await.len(), 1);
    s.simple_query("ROLLBACK").await.expect("rollback");
    assert_eq!(s.tx_status(), TxStatus::Idle);
    assert_eq!(
        rows(&mut s, "SELECT id FROM t").await.len(),
        0,
        "rollback discarded the insert"
    );
}

#[tokio::test]
async fn commit_persists_writes() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    s.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    s.simple_query("BEGIN").await.expect("begin");
    s.simple_query("INSERT INTO t VALUES (1),(2)")
        .await
        .expect("insert");
    s.simple_query("COMMIT").await.expect("commit");
    assert_eq!(s.tx_status(), TxStatus::Idle);
    assert_eq!(rows(&mut s, "SELECT id FROM t").await.len(), 2);
}

#[tokio::test]
async fn sharded_autocommit_insert_uses_timestamp_metadata_not_global_xids() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
    engine.init_gtm_coordinator().expect("gtm");
    let mut s = engine.connect();
    s.simple_query("CREATE TABLE t (id int4) SHARDED")
        .await
        .expect("create");

    s.simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");

    let version = only_timestamp_version(kv.as_ref(), "t");
    assert_eq!(version.start_ts, 1);
    assert_eq!(
        version.state,
        crabka_pgmvcc::version::TsVersionState::Committed { commit_ts: 2 },
        "autocommit sharded write records a timestamp commit marker"
    );
    assert_eq!(
        next_global_xid(kv.as_ref()),
        GLOBAL_XID_BASE,
        "sharded timestamp writes do not allocate G-8 global xids"
    );
    assert_eq!(
        crabka_pgmvcc::clog::get(kv.as_ref(), version.start_ts).expect("no local prepared"),
        XidStatus::InProgress,
        "sharded timestamp writes do not create G-8 local clog metadata"
    );
    assert_eq!(rows(&mut s, "SELECT id FROM t").await.len(), 1);
}

#[tokio::test]
async fn hash_sharded_sql_insert_uses_bucket_leading_timestamp_version_key() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
    engine.init_gtm_coordinator().expect("gtm");
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE h (id int4, value text) SHARDED BY HASH (id) BUCKETS 16")
        .await
        .expect("create");

    session
        .simple_query("INSERT INTO h VALUES (42, 'physical')")
        .await
        .expect("insert");

    let table = crabka_pgcatalog::get_table(kv.as_ref(), "h").expect("table");
    let versions = kv
        .scan_prefix(&crabka_pgkv::key::table_prefix(table.id))
        .expect("scan physical table");
    assert_eq!(versions.len(), 1);
    assert!(matches!(
        crabka_pgkv::key::classify_key(&versions[0].0),
        crabka_pgkv::key::KeyClass::HashPrimaryVersion {
            table_id,
            bucket,
            rowid: 1,
            version: 1,
        } if table_id == table.id
            && bucket == crabka_pgkv::key::hash_bucket(&42_i32.to_be_bytes(), 16)
                .expect("valid bucket count")
    ));
    assert_eq!(
        crabka_pgmvcc::version::decode_ts_tuple(&versions[0].1)
            .expect("timestamp tuple")
            .state,
        crabka_pgmvcc::version::TsVersionState::Committed { commit_ts: 2 }
    );
    assert_eq!(
        timestamp_visible_rows(&engine, &table, ReadTimestamp::MAX).len(),
        1,
        "catalog-aware physical scan must reconstruct committed hash rows"
    );
    assert_eq!(rows(&mut session, "SELECT value FROM h").await.len(), 1);
}

#[tokio::test]
async fn hash_sharded_update_moves_one_row_without_orphaning_old_bucket() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
    engine.init_gtm_coordinator().expect("gtm");
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE hmove (id int4, value text) SHARDED BY HASH (id) BUCKETS 16")
        .await
        .expect("create");
    session
        .simple_query("INSERT INTO hmove VALUES (42, 'before')")
        .await
        .expect("insert");
    let old_bucket = crabka_pgkv::key::hash_bucket(&42_i32.to_be_bytes(), 16).expect("bucket");
    let new_id = (43_i32..100)
        .find(|id| crabka_pgkv::key::hash_bucket(&id.to_be_bytes(), 16) != Some(old_bucket))
        .expect("different bucket id");

    session
        .simple_query(&format!(
            "UPDATE hmove SET id = {new_id}, value = 'after' WHERE id = 42"
        ))
        .await
        .expect("cross-bucket update");

    let result = rows(&mut session, "SELECT id, value FROM hmove").await;
    assert_eq!(result.len(), 1);
    assert_eq!(text(result[0][0].as_ref()), Some(new_id.to_string()));
    assert_eq!(text(result[0][1].as_ref()).as_deref(), Some("after"));
    let table = crabka_pgcatalog::get_table(kv.as_ref(), "hmove").expect("table");
    let keys = kv
        .scan_prefix(&crabka_pgkv::key::table_prefix(table.id))
        .expect("physical rows");
    assert!(keys.iter().all(|(key, _)| matches!(
        crabka_pgkv::key::classify_key(key),
        crabka_pgkv::key::KeyClass::HashPrimaryVersion { .. }
    )));
    assert!(keys.iter().any(|(key, value)| {
        matches!(
            crabka_pgkv::key::classify_key(key),
            crabka_pgkv::key::KeyClass::HashPrimaryVersion { bucket, .. } if bucket == old_bucket
        ) && matches!(
            crabka_pgmvcc::version::decode_ts_tuple(value)
                .expect("tuple")
                .state,
            crabka_pgmvcc::version::TsVersionState::Deleted { .. }
        )
    }));

    session
        .simple_query(&format!("DELETE FROM hmove WHERE id = {new_id}"))
        .await
        .expect("delete moved row");
    assert!(rows(&mut session, "SELECT id FROM hmove").await.is_empty());
    let new_bucket = crabka_pgkv::key::hash_bucket(&new_id.to_be_bytes(), 16).expect("bucket");
    let versions = kv
        .scan_prefix(&crabka_pgkv::key::hash_row_key(table.id, new_bucket, 1))
        .expect("new bucket versions");
    assert!(versions.iter().any(|(_, value)| matches!(
        crabka_pgmvcc::version::decode_ts_tuple(value).expect("tuple").state,
        crabka_pgmvcc::version::TsVersionState::Deleted { .. }
    )));
}

#[tokio::test]
async fn set_transaction_repeatable_read_fixes_timestamp_snapshot_before_first_read() {
    let mut engine = SqlEngine::new();
    engine.init_gtm_coordinator().expect("gtm");
    let mut writer = engine.connect();
    writer
        .simple_query("CREATE TABLE t (id int4) SHARDED")
        .await
        .expect("create");
    let mut reader = engine.connect();
    reader.simple_query("BEGIN").await.expect("begin");
    reader
        .simple_query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("set repeatable read");
    writer
        .simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("concurrent insert");
    assert!(
        rows(&mut reader, "SELECT id FROM t").await.is_empty(),
        "the first read uses the timestamp fixed by SET TRANSACTION"
    );
    reader.simple_query("ROLLBACK").await.expect("rollback");

    reader
        .simple_query("BEGIN")
        .await
        .expect("begin read committed");
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await.len(), 1);
    reader.simple_query("ROLLBACK").await.expect("rollback");
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the durable recovery sequence is clearest as one end-to-end behavior test"
)]
async fn committed_descriptor_recovery_resolves_put_delete_and_global_index_intents() {
    let coordinator_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut coordinator = SqlEngine::with_kv(Arc::clone(&coordinator_kv)).expect("coordinator");
    coordinator.init_gtm_coordinator().expect("gtm");
    let participant_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut participant = SqlEngine::with_kv(Arc::clone(&participant_kv)).expect("participant");
    participant.set_catalog_kv(Arc::clone(&coordinator_kv));
    let start_ts = TimestampTransactionId::new(10).expect("start timestamp");
    let commit_ts = CommitTimestamp::after_start(start_ts, 20).expect("commit timestamp");
    let identity = TimestampTxnIdentity {
        start_ts,
        global_xid: 10,
        primary_range: 0,
    };
    let put = TimestampWrite {
        table_id: 99,
        bucket: None,
        rowid: 1,
        row: vec![crabka_pgtypes::Datum::Int4(1)],
        delete: false,
        global_index_intents: vec![crabka_pgexec::timestamp_txn::GlobalIndexIntent {
            index_id: 7,
            indexed_values: vec![crabka_pgtypes::Datum::Int4(1)],
            base_table_id: 99,
            base_rowid: 1,
            unique: false,
            delete: false,
        }],
    };
    let delete = TimestampWrite {
        table_id: 99,
        bucket: None,
        rowid: 2,
        row: vec![crabka_pgtypes::Datum::Int4(2)],
        delete: true,
        global_index_intents: Vec::new(),
    };
    coordinator
        .begin_timestamp_transaction(&TimestampTxnDescriptor::begun(start_ts, 10, vec![1]))
        .await
        .expect("descriptor");
    participant
        .timestamp_txn_participant(1)
        .prewrite_with_primary(identity, &[put.clone(), delete.clone()])
        .await
        .expect("prewrite");
    let operations = [put, delete]
        .iter()
        .map(|write| crabka_pgexec::TimestampTxnOperation {
            range_id: 1,
            table_id: write.table_id,
            bucket: write.bucket,
            rowid: write.rowid,
            delete: write.delete,
        })
        .collect::<Vec<_>>();
    coordinator
        .acknowledge_timestamp_participant_operations(start_ts, 1, &operations)
        .await
        .expect("durable operations");
    coordinator
        .decide_timestamp_transaction(start_ts, PrimaryTxnDecision::Committed(commit_ts))
        .await
        .expect("durable decision");

    participant
        .timestamp_txn_participant(1)
        .resolve_operations_with_primary(
            identity,
            TimestampTxnDecision::Committed(commit_ts),
            &operations,
        )
        .await
        .expect("recovery resolution");
    assert_eq!(
        crabka_pgexec::timestamp_txn::read_visible_ts_row(
            participant_kv.as_ref(),
            99,
            1,
            ReadTimestamp::new(20).expect("read timestamp"),
        )
        .expect("visible put"),
        Some(vec![crabka_pgtypes::Datum::Int4(1)])
    );
    assert_eq!(
        crabka_pgexec::timestamp_txn::read_visible_ts_row(
            participant_kv.as_ref(),
            99,
            2,
            ReadTimestamp::new(20).expect("read timestamp"),
        )
        .expect("visible delete"),
        None
    );
    assert_eq!(
        crabka_pgexec::timestamp_txn::read_visible_global_index_entries(
            participant_kv.as_ref(),
            7,
            &[crabka_pgtypes::Datum::Int4(1)],
            ReadTimestamp::new(20).expect("read timestamp"),
        )
        .expect("visible index"),
        vec![crabka_pgexec::timestamp_txn::VisibleGlobalIndexEntry {
            base_table_id: 99,
            base_rowid: 1,
        }]
    );
}

#[allow(
    clippy::too_many_lines,
    reason = "the 2PC timeline is clearest in one test"
)]
#[tokio::test]
async fn timestamp_descriptor_commit_makes_unresolved_participants_visible_at_one_timestamp() {
    let coordinator_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut coordinator = SqlEngine::with_kv(Arc::clone(&coordinator_kv)).expect("coordinator");
    coordinator.init_gtm_coordinator().expect("gtm");
    let mut setup = coordinator.connect();
    setup
        .simple_query("CREATE TABLE t (id int4) SHARDED")
        .await
        .expect("create table");
    let table = coordinator.catalog_table("t").expect("table");

    let left_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let right_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut left = SqlEngine::with_kv(left_kv).expect("left participant");
    let mut right = SqlEngine::with_kv(right_kv).expect("right participant");
    left.set_catalog_kv(Arc::clone(&coordinator_kv));
    right.set_catalog_kv(Arc::clone(&coordinator_kv));

    let start_ts = TimestampTransactionId::new(10).expect("start timestamp");
    let commit_ts = CommitTimestamp::after_start(start_ts, 20).expect("commit timestamp");
    let global_xid = coordinator
        .begin_global_durable()
        .await
        .expect("global xid");
    let identity = TimestampTxnIdentity {
        start_ts,
        global_xid,
        primary_range: 0,
    };
    let left_write = timestamp_write(table.id, 1, 10);
    let right_write = timestamp_write(table.id, 2, 20);
    let left_operations = [crabka_pgexec::TimestampTxnOperation {
        range_id: 1,
        table_id: left_write.table_id,
        bucket: left_write.bucket,
        rowid: left_write.rowid,
        delete: left_write.delete,
    }];
    let right_operations = [crabka_pgexec::TimestampTxnOperation {
        range_id: 2,
        table_id: right_write.table_id,
        bucket: right_write.bucket,
        rowid: right_write.rowid,
        delete: right_write.delete,
    }];
    coordinator
        .begin_timestamp_transaction(&TimestampTxnDescriptor::begun(
            start_ts,
            global_xid,
            vec![1, 2],
        ))
        .await
        .expect("begin descriptor");
    left.timestamp_txn_participant(1)
        .prewrite_with_primary(identity, std::slice::from_ref(&left_write))
        .await
        .expect("left prewrite");
    coordinator
        .acknowledge_timestamp_participant_operations(start_ts, 1, &left_operations)
        .await
        .expect("left acknowledgement");
    right
        .timestamp_txn_participant(2)
        .prewrite_with_primary(identity, std::slice::from_ref(&right_write))
        .await
        .expect("right prewrite");
    coordinator
        .acknowledge_timestamp_participant_operations(start_ts, 2, &right_operations)
        .await
        .expect("right acknowledgement");

    assert!(timestamp_visible_rows(&left, &table, ReadTimestamp::MAX).is_empty());
    assert!(timestamp_visible_rows(&right, &table, ReadTimestamp::MAX).is_empty());

    assert_eq!(
        coordinator
            .decide_timestamp_transaction(start_ts, PrimaryTxnDecision::Committed(commit_ts))
            .await
            .expect("durable commit decision"),
        PrimaryTxnDecision::Committed(commit_ts)
    );
    left.timestamp_txn_participant(1)
        .resolve_with_primary(
            identity,
            TimestampTxnDecision::Committed(commit_ts),
            std::slice::from_ref(&left_write),
        )
        .await
        .expect("left resolve");

    // The descriptor-aware visibility seam uses one supplied statement timestamp
    // for both local and unresolved remote participant intents.
    let before_commit = ReadTimestamp::new(19).expect("read before commit");
    assert!(timestamp_visible_rows(&left, &table, before_commit).is_empty());
    assert!(timestamp_visible_rows(&right, &table, before_commit).is_empty());
    assert_eq!(
        timestamp_visible_rows(
            &left,
            &table,
            ReadTimestamp::new(20).expect("read at commit")
        ),
        vec![vec![crabka_pgtypes::Datum::Int4(10)]]
    );
    assert_eq!(
        timestamp_visible_rows(
            &right,
            &table,
            ReadTimestamp::new(20).expect("read at commit")
        ),
        vec![vec![crabka_pgtypes::Datum::Int4(20)]]
    );

    right
        .timestamp_txn_participant(2)
        .resolve_with_primary(
            identity,
            TimestampTxnDecision::Committed(commit_ts),
            std::slice::from_ref(&right_write),
        )
        .await
        .expect("right recovery resolve");
    right
        .timestamp_txn_participant(2)
        .resolve_with_primary(
            identity,
            TimestampTxnDecision::Committed(commit_ts),
            std::slice::from_ref(&right_write),
        )
        .await
        .expect("idempotent right recovery resolve");
}

#[tokio::test]
async fn timestamp_recovery_aborts_undecided_descriptor_and_fences_delayed_commit() {
    let coordinator_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut coordinator = SqlEngine::with_kv(coordinator_kv).expect("coordinator");
    coordinator.init_gtm_coordinator().expect("gtm");
    let start_ts = TimestampTransactionId::new(10).expect("start timestamp");
    let global_xid = coordinator
        .begin_global_durable()
        .await
        .expect("global xid");
    coordinator
        .begin_timestamp_transaction(&TimestampTxnDescriptor::begun(
            start_ts,
            global_xid,
            vec![1],
        ))
        .await
        .expect("begin descriptor");
    assert_eq!(
        coordinator
            .recover_timestamp_transaction(start_ts)
            .await
            .expect("recover undecided descriptor"),
        PrimaryTxnDecision::Aborted
    );
    assert_eq!(
        coordinator
            .decide_timestamp_transaction(
                start_ts,
                PrimaryTxnDecision::Committed(
                    CommitTimestamp::after_start(start_ts, 20).expect("commit timestamp"),
                ),
            )
            .await
            .expect("delayed coordinator is fenced"),
        PrimaryTxnDecision::Aborted
    );
}

#[tokio::test]
async fn timestamp_acknowledgement_requires_durable_operations() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let coordinator = SqlEngine::with_kv(Arc::clone(&kv)).expect("coordinator");
    let start_ts = TimestampTransactionId::new(10).expect("start timestamp");
    coordinator
        .begin_timestamp_transaction(&TimestampTxnDescriptor::begun(start_ts, 9, vec![1]))
        .await
        .expect("descriptor");

    assert!(
        coordinator
            .acknowledge_timestamp_participant_operations(start_ts, 1, &[])
            .await
            .is_err()
    );
    let descriptor = coordinator
        .timestamp_transaction_descriptors()
        .expect("descriptor")
        .into_iter()
        .next()
        .expect("stored descriptor");
    assert!(descriptor.prepared.is_empty());
    assert!(descriptor.operations.is_empty());

    let mut fabricated = TimestampTxnDescriptor::begun(start_ts, 10, vec![1]);
    fabricated.prepared.push(1);
    assert!(
        coordinator
            .begin_timestamp_transaction(&fabricated)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn concurrent_timestamp_commit_requests_return_the_one_durable_timestamp() {
    let coordinator_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut coordinator = SqlEngine::with_kv(coordinator_kv).expect("coordinator");
    coordinator.init_gtm_coordinator().expect("gtm");
    let start_ts = TimestampTransactionId::new(10).expect("start timestamp");
    coordinator
        .begin_timestamp_transaction(&TimestampTxnDescriptor::begun(start_ts, 10, vec![]))
        .await
        .expect("begin descriptor");
    let first = CommitTimestamp::after_start(start_ts, 20).expect("first commit timestamp");
    let second = CommitTimestamp::after_start(start_ts, 30).expect("second commit timestamp");

    let (left, right) = tokio::join!(
        coordinator.decide_timestamp_transaction(start_ts, PrimaryTxnDecision::Committed(first)),
        coordinator.decide_timestamp_transaction(start_ts, PrimaryTxnDecision::Committed(second)),
    );
    let left = left.expect("left decision");
    let right = right.expect("right decision");
    assert_eq!(left, right);
    assert!(
        matches!(left, PrimaryTxnDecision::Committed(commit_ts) if commit_ts == first || commit_ts == second)
    );
    assert_eq!(
        coordinator
            .primary_timestamp_decision(start_ts)
            .expect("durable primary decision"),
        left
    );
}

#[tokio::test]
async fn timestamp_descriptor_transitions_are_fenced_across_separate_engine_handles() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let first = SqlEngine::with_kv(Arc::clone(&kv)).expect("first replica handle");
    let second = SqlEngine::with_kv(Arc::clone(&kv)).expect("second replica handle");
    let start_ts = TimestampTransactionId::new(50).expect("start timestamp");
    let descriptor = TimestampTxnDescriptor::begun(start_ts, 9, vec![1]);

    first
        .begin_timestamp_transaction(&descriptor)
        .await
        .expect("first create");
    assert!(
        second
            .begin_timestamp_transaction(&TimestampTxnDescriptor::begun(start_ts, 10, vec![1]))
            .await
            .is_err()
    );

    let stale = second
        .timestamp_transaction_descriptors()
        .expect("stale descriptor")
        .into_iter()
        .next()
        .expect("descriptor");
    let operation = crabka_pgexec::TimestampTxnOperation {
        range_id: 1,
        table_id: 10,
        bucket: None,
        rowid: 11,
        delete: false,
    };
    first
        .acknowledge_timestamp_participant_operations(start_ts, 1, std::slice::from_ref(&operation))
        .await
        .expect("current acknowledgement");
    let mut stale_writer = stale.clone();
    stale_writer
        .acknowledge_operations(1, std::slice::from_ref(&operation))
        .expect("stale local transition");
    kv.write_batch(&[
        crabka_pgexec::timestamp_txn::timestamp_txn_descriptor_cas_op(&stale_writer, Some(&stale)),
    ])
    .expect("stale conditional apply is a no-op");

    let commit_ts = CommitTimestamp::after_start(start_ts, 60).expect("commit timestamp");
    first
        .decide_timestamp_transaction(start_ts, PrimaryTxnDecision::Committed(commit_ts))
        .await
        .expect("commit");
    assert_eq!(
        second
            .decide_timestamp_transaction(start_ts, PrimaryTxnDecision::Aborted)
            .await
            .expect("stale terminal writer observes durable decision"),
        PrimaryTxnDecision::Committed(commit_ts)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_timestamp_acknowledgements_preserve_every_participant_operation() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let committer: Arc<dyn crabka_pgexec::Committer> = Arc::new(InterleavingDescriptorCommitter {
        kv: Arc::clone(&kv),
        acknowledgement_commits: AtomicUsize::new(0),
        acknowledgement_barrier: tokio::sync::Barrier::new(2),
    });
    let coordinator = SqlEngine::replicated(
        Arc::clone(&kv),
        Arc::clone(&kv),
        committer,
        Arc::new(crabka_pgexec::LocalLinearizer),
    )
    .expect("coordinator");
    let start_ts = TimestampTransactionId::new(90).expect("start timestamp");
    coordinator
        .begin_timestamp_transaction(&TimestampTxnDescriptor::begun(start_ts, 90, vec![1, 2]))
        .await
        .expect("descriptor");
    let first_operation = crabka_pgexec::TimestampTxnOperation {
        range_id: 1,
        table_id: 10,
        bucket: None,
        rowid: 11,
        delete: false,
    };
    let second_operation = crabka_pgexec::TimestampTxnOperation {
        range_id: 2,
        table_id: 20,
        bucket: None,
        rowid: 21,
        delete: true,
    };
    let first_operations = [first_operation];
    let second_operations = [second_operation];

    let (first, second) = tokio::join!(
        coordinator.acknowledge_timestamp_participant_operations(start_ts, 1, &first_operations),
        coordinator.acknowledge_timestamp_participant_operations(start_ts, 2, &second_operations),
    );
    first.expect("first acknowledgement");
    second.expect("second acknowledgement");

    let descriptor = coordinator
        .timestamp_transaction_descriptors()
        .expect("descriptor")
        .into_iter()
        .next()
        .expect("stored descriptor");
    assert_eq!(descriptor.prepared, vec![1, 2]);
    assert_eq!(
        descriptor.operations,
        vec![first_operation, second_operation]
    );
}

#[tokio::test]
async fn timestamp_participant_rejects_stale_prewrite_and_invalid_terminal_timestamp() {
    let primary_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let participant_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let coordinator = SqlEngine::with_kv(Arc::clone(&primary_kv)).expect("coordinator");
    let mut participant_engine =
        SqlEngine::with_kv(Arc::clone(&participant_kv)).expect("participant");
    participant_engine.set_catalog_kv(Arc::clone(&primary_kv));
    let start_ts = TimestampTransactionId::new(70).expect("start timestamp");
    let identity = TimestampTxnIdentity {
        start_ts,
        global_xid: 17,
        primary_range: 0,
    };
    coordinator
        .begin_timestamp_transaction(&TimestampTxnDescriptor::begun(start_ts, 17, vec![1]))
        .await
        .expect("descriptor");
    let write = timestamp_write(1, 1, 0);
    coordinator
        .acknowledge_timestamp_participant_operations(
            start_ts,
            1,
            std::slice::from_ref(&timestamp_operation(1, &write)),
        )
        .await
        .expect("acknowledgement");
    assert!(
        coordinator
            .decide_timestamp_transaction(
                start_ts,
                PrimaryTxnDecision::Committed(CommitTimestamp::new(70).expect("nonzero"))
            )
            .await
            .is_err()
    );
    assert_eq!(
        coordinator
            .primary_timestamp_decision(start_ts)
            .expect("invalid timestamp did not persist a terminal decision"),
        PrimaryTxnDecision::Pending
    );
    coordinator
        .decide_timestamp_transaction(start_ts, PrimaryTxnDecision::Aborted)
        .await
        .expect("abort");
    assert!(
        participant_engine
            .timestamp_txn_participant(1)
            .prewrite_with_primary(identity, &[timestamp_write(1, 1, 1)])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn timestamp_participant_rejects_descriptor_non_member() {
    let primary_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let participant_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let coordinator = SqlEngine::with_kv(Arc::clone(&primary_kv)).expect("coordinator");
    let mut participant = SqlEngine::with_kv(participant_kv).expect("participant");
    participant.set_catalog_kv(Arc::clone(&primary_kv));
    let start_ts = TimestampTransactionId::new(71).expect("start timestamp");
    let identity = TimestampTxnIdentity {
        start_ts,
        global_xid: 18,
        primary_range: 0,
    };
    coordinator
        .begin_timestamp_transaction(&TimestampTxnDescriptor::begun(start_ts, 18, vec![1]))
        .await
        .expect("descriptor");

    assert!(
        participant
            .timestamp_txn_participant(2)
            .prewrite_with_primary(identity, &[timestamp_write(1, 1, 1)])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn descriptor_commit_does_not_expose_legacy_or_forged_local_version() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE t (id int4) SHARDED")
        .await
        .expect("create table");
    let table = engine.catalog_table("t").expect("table");
    let start_ts = TimestampTransactionId::new(75).expect("start timestamp");
    let commit_ts = CommitTimestamp::after_start(start_ts, 76).expect("commit timestamp");
    engine
        .begin_timestamp_transaction(&TimestampTxnDescriptor::begun(start_ts, 19, vec![]))
        .await
        .expect("descriptor");
    engine
        .decide_timestamp_transaction(start_ts, PrimaryTxnDecision::Committed(commit_ts))
        .await
        .expect("terminal decision");
    engine
        .kv_handle()
        .write_batch(&[crabka_pgkv::WriteOp::Put {
            key: crabka_pgmvcc::version::version_key_ts(table.id, 1, start_ts.get()),
            value: crabka_pgmvcc::version::encode_ts_tuple(
                start_ts.get(),
                crabka_pgmvcc::version::TsVersionState::Intent,
                &[crabka_pgtypes::Datum::Int4(1)],
            ),
        }])
        .expect("forged intent");

    assert!(timestamp_visible_rows(&engine, &table, ReadTimestamp::MAX).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_same_row_timestamp_prewrite_has_one_winner() {
    let primary_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let participant_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let coordinator = SqlEngine::with_kv(Arc::clone(&primary_kv)).expect("coordinator");
    let mut first = SqlEngine::with_kv(Arc::clone(&participant_kv)).expect("first");
    let mut second = SqlEngine::with_kv(participant_kv).expect("second");
    first.set_catalog_kv(Arc::clone(&primary_kv));
    second.set_catalog_kv(Arc::clone(&primary_kv));
    let first_start = TimestampTransactionId::new(80).expect("first start");
    let second_start = TimestampTransactionId::new(81).expect("second start");
    coordinator
        .begin_timestamp_transaction(&TimestampTxnDescriptor::begun(first_start, 80, vec![1]))
        .await
        .expect("first descriptor");
    coordinator
        .begin_timestamp_transaction(&TimestampTxnDescriptor::begun(second_start, 81, vec![1]))
        .await
        .expect("second descriptor");
    let first_identity = TimestampTxnIdentity {
        start_ts: first_start,
        global_xid: 80,
        primary_range: 0,
    };
    let second_identity = TimestampTxnIdentity {
        start_ts: second_start,
        global_xid: 81,
        primary_range: 0,
    };
    let first_write = timestamp_write(1, 1, 1);
    let second_write = timestamp_write(1, 1, 2);
    let first_participant = first.timestamp_txn_participant(1);
    let second_participant = second.timestamp_txn_participant(1);
    let first_writes = [first_write];
    let second_writes = [second_write];

    let (first_result, second_result) = tokio::join!(
        first_participant.prewrite_with_primary(first_identity, &first_writes),
        second_participant.prewrite_with_primary(second_identity, &second_writes),
    );
    assert!(first_result.is_ok() ^ second_result.is_ok());
}

#[tokio::test]
async fn timestamp_prewrite_failure_after_first_participant_durably_aborts() {
    let coordinator_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut coordinator = SqlEngine::with_kv(coordinator_kv).expect("coordinator");
    coordinator.init_gtm_coordinator().expect("gtm");
    let first_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let second_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut first = SqlEngine::with_kv(Arc::clone(&first_kv)).expect("first participant");
    let mut second = SqlEngine::with_kv(Arc::clone(&second_kv)).expect("second participant");
    first.set_catalog_kv(coordinator.kv_handle());
    second.set_catalog_kv(coordinator.kv_handle());
    let start_ts = TimestampTransactionId::new(10).expect("start timestamp");
    let global_xid = coordinator
        .begin_global_durable()
        .await
        .expect("global xid");
    let identity = TimestampTxnIdentity {
        start_ts,
        global_xid,
        primary_range: 0,
    };
    let first_write = timestamp_write(1, 1, 10);
    let second_write = timestamp_write(1, 2, 20);
    coordinator
        .begin_timestamp_transaction(&TimestampTxnDescriptor::begun(
            start_ts,
            global_xid,
            vec![1, 2],
        ))
        .await
        .expect("begin descriptor");
    first
        .timestamp_txn_participant(1)
        .prewrite_with_primary(identity, std::slice::from_ref(&first_write))
        .await
        .expect("first prewrite");
    coordinator
        .acknowledge_timestamp_participant_operations(
            start_ts,
            1,
            std::slice::from_ref(&timestamp_operation(1, &first_write)),
        )
        .await
        .expect("first acknowledgement");

    second
        .timestamp_txn_participant(2)
        .prewrite(
            TimestampTransactionId::new(11).expect("conflicting start timestamp"),
            std::slice::from_ref(&second_write),
        )
        .await
        .expect("seed conflicting intent");
    assert!(
        second
            .timestamp_txn_participant(2)
            .prewrite_with_primary(identity, std::slice::from_ref(&second_write))
            .await
            .is_err()
    );

    assert_eq!(
        coordinator
            .decide_timestamp_transaction(start_ts, PrimaryTxnDecision::Aborted)
            .await
            .expect("durable abort decision"),
        PrimaryTxnDecision::Aborted
    );
    first
        .timestamp_txn_participant(1)
        .resolve_with_primary(
            identity,
            TimestampTxnDecision::Aborted,
            std::slice::from_ref(&first_write),
        )
        .await
        .expect("first abort resolve");

    assert_eq!(
        crabka_pgexec::timestamp_txn::read_visible_ts_row(
            first_kv.as_ref(),
            first_write.table_id,
            first_write.rowid,
            ReadTimestamp::MAX,
        )
        .expect("first participant visibility"),
        None
    );
    assert_eq!(
        crabka_pgexec::timestamp_txn::read_timestamp_txn_descriptor(
            coordinator.kv_handle().as_ref(),
            start_ts,
        )
        .expect("durable descriptor")
        .expect("timestamp descriptor")
        .decision,
        PrimaryTxnDecision::Aborted
    );
}

#[tokio::test]
async fn abort_recovery_removes_timestamp_identity_and_reservation_sidecars() {
    let primary_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let participant_kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let coordinator = SqlEngine::with_kv(Arc::clone(&primary_kv)).expect("coordinator");
    let mut participant = SqlEngine::with_kv(Arc::clone(&participant_kv)).expect("participant");
    participant.set_catalog_kv(Arc::clone(&primary_kv));
    let start_ts = TimestampTransactionId::new(10).expect("start timestamp");
    let identity = TimestampTxnIdentity {
        start_ts,
        global_xid: 9,
        primary_range: 0,
    };
    let write = timestamp_write(7, 8, 42);
    let operation = timestamp_operation(1, &write);
    coordinator
        .begin_timestamp_transaction(&TimestampTxnDescriptor::begun(start_ts, 9, vec![1]))
        .await
        .expect("descriptor");
    participant
        .timestamp_txn_participant(1)
        .prewrite_with_primary(identity, std::slice::from_ref(&write))
        .await
        .expect("prewrite");
    coordinator
        .acknowledge_timestamp_participant_operations(start_ts, 1, std::slice::from_ref(&operation))
        .await
        .expect("acknowledgement");
    coordinator
        .decide_timestamp_transaction(start_ts, PrimaryTxnDecision::Aborted)
        .await
        .expect("abort decision");
    participant
        .timestamp_txn_participant(1)
        .resolve_operations_with_primary(
            identity,
            TimestampTxnDecision::Aborted,
            std::slice::from_ref(&operation),
        )
        .await
        .expect("abort recovery");

    let mut identity_key = b"\0\0\0\0meta/ts_intent/".to_vec();
    identity_key.extend_from_slice(&write.table_id.to_be_bytes());
    identity_key.extend_from_slice(&write.rowid.to_be_bytes());
    identity_key.extend_from_slice(&start_ts.get().to_be_bytes());
    let mut reservation_key = b"\0\0\0\0meta/ts_prewrite/".to_vec();
    reservation_key.extend_from_slice(&write.table_id.to_be_bytes());
    reservation_key.extend_from_slice(&write.rowid.to_be_bytes());
    assert_eq!(participant_kv.get(&identity_key).expect("identity"), None);
    assert_eq!(
        participant_kv.get(&reservation_key).expect("reservation"),
        None
    );
}

#[tokio::test]
async fn unsharded_autocommit_insert_keeps_local_commit_path() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
    engine.init_gtm_coordinator().expect("gtm");
    let mut s = engine.connect();
    s.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");

    s.simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");

    let xmin = only_tuple_xmin(kv.as_ref(), "t");
    assert_eq!(
        crabka_pgmvcc::clog::get(kv.as_ref(), xmin).expect("local status"),
        XidStatus::Committed,
        "unsharded insert stays on the ordinary local commit path"
    );
    assert_eq!(
        next_global_xid(kv.as_ref()),
        GLOBAL_XID_BASE,
        "unsharded writes do not allocate global xids"
    );
}

#[tokio::test]
async fn sharded_explicit_transaction_insert_fails_without_timestamp_commit() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
    engine.init_gtm_coordinator().expect("gtm");
    let mut s = engine.connect();
    s.simple_query("CREATE TABLE t (id int4) SHARDED")
        .await
        .expect("create");

    s.simple_query("BEGIN").await.expect("begin");
    let err = s
        .simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect_err("sharded explicit writes are unsupported");
    assert_eq!(err.code, "0A000");
    assert_eq!(s.tx_status(), TxStatus::Failed);
    let err = s
        .simple_query("SELECT id FROM t")
        .await
        .expect_err("failed transaction rejects statements");
    assert_eq!(err.code, "25P02");

    assert_eq!(
        table_version_count(kv.as_ref(), "t"),
        0,
        "failed explicit sharded write must not commit a timestamp tuple"
    );
    assert_eq!(next_global_xid(kv.as_ref()), GLOBAL_XID_BASE);

    s.simple_query("ROLLBACK").await.expect("rollback");
    assert_eq!(s.tx_status(), TxStatus::Idle);
    assert_eq!(rows(&mut s, "SELECT id FROM t").await.len(), 0);
}

#[tokio::test]
async fn sharded_autocommit_update_uses_timestamp_versions() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
    engine.init_gtm_coordinator().expect("gtm");
    let mut s = engine.connect();
    s.simple_query("CREATE TABLE t (id int4, name text) SHARDED")
        .await
        .expect("create");
    s.simple_query("INSERT INTO t VALUES (1, 'old'), (2, 'keep')")
        .await
        .expect("insert");

    let result = s
        .simple_query("UPDATE t SET name = 'new' WHERE id = 1")
        .await
        .expect("update");
    assert!(matches!(&result[0], QueryResult::Command { tag } if tag == "UPDATE 1"));

    let r = rows(&mut s, "SELECT id, name FROM t ORDER BY id").await;
    let visible = r
        .iter()
        .map(|row| (text(row[0].as_ref()), text(row[1].as_ref())))
        .collect::<Vec<_>>();
    assert_eq!(
        visible,
        vec![
            (Some("1".into()), Some("new".into())),
            (Some("2".into()), Some("keep".into())),
        ]
    );
    assert!(
        table_timestamp_versions(kv.as_ref(), "t")
            .iter()
            .any(|version| {
                version.row
                    == vec![
                        crabka_pgtypes::Datum::Int4(1),
                        crabka_pgtypes::Datum::Text("new".into()),
                    ]
                    && matches!(
                        version.state,
                        crabka_pgmvcc::version::TsVersionState::Committed { .. }
                    )
            })
    );
    assert_eq!(next_global_xid(kv.as_ref()), GLOBAL_XID_BASE);
}

#[tokio::test]
async fn sharded_autocommit_delete_writes_timestamp_tombstone() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
    engine.init_gtm_coordinator().expect("gtm");
    let mut s = engine.connect();
    s.simple_query("CREATE TABLE t (id int4, name text) SHARDED")
        .await
        .expect("create");
    s.simple_query("INSERT INTO t VALUES (1, 'gone'), (2, 'keep')")
        .await
        .expect("insert");

    let result = s
        .simple_query("DELETE FROM t WHERE id = 1")
        .await
        .expect("delete");
    assert!(matches!(&result[0], QueryResult::Command { tag } if tag == "DELETE 1"));

    let r = rows(&mut s, "SELECT id, name FROM t ORDER BY id").await;
    let visible = r
        .iter()
        .map(|row| (text(row[0].as_ref()), text(row[1].as_ref())))
        .collect::<Vec<_>>();
    assert_eq!(visible, vec![(Some("2".into()), Some("keep".into()))]);
    assert!(
        table_timestamp_versions(kv.as_ref(), "t")
            .iter()
            .any(|version| {
                version.row
                    == vec![
                        crabka_pgtypes::Datum::Int4(1),
                        crabka_pgtypes::Datum::Text("gone".into()),
                    ]
                    && matches!(
                        version.state,
                        crabka_pgmvcc::version::TsVersionState::Deleted { .. }
                    )
            })
    );
    assert_eq!(next_global_xid(kv.as_ref()), GLOBAL_XID_BASE);
}

#[tokio::test]
async fn unsharded_update_and_delete_keep_local_commit_path() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
    engine.init_gtm_coordinator().expect("gtm");
    let mut s = engine.connect();
    s.simple_query("CREATE TABLE t (id int4, name text)")
        .await
        .expect("create");
    s.simple_query("INSERT INTO t VALUES (1, 'old'), (2, 'gone')")
        .await
        .expect("insert");

    s.simple_query("UPDATE t SET name = 'new' WHERE id = 1")
        .await
        .expect("update");
    s.simple_query("DELETE FROM t WHERE id = 2")
        .await
        .expect("delete");

    let r = rows(&mut s, "SELECT id, name FROM t ORDER BY id").await;
    assert_eq!(
        r.iter()
            .map(|row| (text(row[0].as_ref()), text(row[1].as_ref())))
            .collect::<Vec<_>>(),
        vec![(Some("1".into()), Some("new".into()))]
    );
    assert!(
        kv.scan_prefix(&crabka_pgkv::key::table_prefix(
            crabka_pgcatalog::get_table(kv.as_ref(), "t")
                .expect("table")
                .id,
        ))
        .expect("scan")
        .iter()
        .all(|(_key, value)| crabka_pgmvcc::version::decode_tuple(value).is_ok()),
        "unsharded UPDATE/DELETE continue to write xid/clog tuples"
    );
    assert_eq!(next_global_xid(kv.as_ref()), GLOBAL_XID_BASE);
}

#[tokio::test]
async fn sharded_explicit_transaction_update_delete_fail_without_partial_writes() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
    engine.init_gtm_coordinator().expect("gtm");
    let mut s = engine.connect();
    s.simple_query("CREATE TABLE t (id int4, name text) SHARDED")
        .await
        .expect("create");
    s.simple_query("INSERT INTO t VALUES (1, 'old'), (2, 'gone')")
        .await
        .expect("seed");
    let before = table_version_count(kv.as_ref(), "t");

    s.simple_query("BEGIN").await.expect("begin update");
    let err = s
        .simple_query("UPDATE t SET name = 'new' WHERE id = 1")
        .await
        .expect_err("explicit sharded update unsupported");
    assert_eq!(err.code, "0A000");
    assert_eq!(s.tx_status(), TxStatus::Failed);
    assert_eq!(table_version_count(kv.as_ref(), "t"), before);
    s.simple_query("ROLLBACK").await.expect("rollback update");
    assert_eq!(s.tx_status(), TxStatus::Idle);

    s.simple_query("BEGIN").await.expect("begin delete");
    let err = s
        .simple_query("DELETE FROM t WHERE id = 2")
        .await
        .expect_err("explicit sharded delete unsupported");
    assert_eq!(err.code, "0A000");
    assert_eq!(s.tx_status(), TxStatus::Failed);
    assert_eq!(table_version_count(kv.as_ref(), "t"), before);
    s.simple_query("ROLLBACK").await.expect("rollback delete");

    let r = rows(&mut s, "SELECT id, name FROM t ORDER BY id").await;
    assert_eq!(
        r.iter()
            .map(|row| (text(row[0].as_ref()), text(row[1].as_ref())))
            .collect::<Vec<_>>(),
        vec![
            (Some("1".into()), Some("old".into())),
            (Some("2".into()), Some("gone".into())),
        ]
    );
}

#[tokio::test]
async fn global_xid_leases_are_disjoint_and_reseed_past_leased_blocks() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
    engine.init_gtm_coordinator().expect("gtm");

    let mut first = engine.lease_global_xid_block(3).await.expect("first lease");
    let mut second = engine
        .lease_global_xid_block(2)
        .await
        .expect("second lease");

    assert_eq!(first.allocate(), Some(GLOBAL_XID_BASE));
    assert_eq!(first.allocate(), Some(GLOBAL_XID_BASE + 1));
    assert_eq!(first.allocate(), Some(GLOBAL_XID_BASE + 2));
    assert_eq!(first.allocate(), None);
    assert_eq!(second.allocate(), Some(GLOBAL_XID_BASE + 3));
    assert_eq!(second.allocate(), Some(GLOBAL_XID_BASE + 4));
    assert_eq!(second.allocate(), None);

    let mut reopened = SqlEngine::with_kv(Arc::clone(&kv)).expect("reopen");
    reopened.init_gtm_coordinator().expect("gtm");
    reopened.reseed_gtm().expect("reseed");
    assert_eq!(
        reopened.begin_global_durable().await.expect("next global"),
        GLOBAL_XID_BASE + 5,
        "reseed lifts the allocator past every xid in the leased blocks"
    );
}

#[tokio::test]
async fn global_participant_commit_release_exposes_rows_after_external_decision() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
    engine.init_gtm_coordinator().expect("gtm");
    let mut setup = engine.connect();
    setup
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");

    let global_xid = engine.begin_global_durable().await.expect("global xid");
    let mut participant = engine.connect();
    participant.simple_query("BEGIN").await.expect("begin");
    participant
        .simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    let local_xid = participant.local_xid().expect("local xid");
    let prepared_xid = participant
        .prepare_global_participant(global_xid)
        .await
        .expect("prepare participant");
    assert_eq!(prepared_xid, global_xid);
    assert_eq!(
        crabka_pgmvcc::clog::get(kv.as_ref(), local_xid).expect("local status"),
        XidStatus::Prepared(global_xid),
        "participant prepare durably records the local-to-global mapping"
    );
    let mut before_decision = engine.connect();
    assert_eq!(
        rows(&mut before_decision, "SELECT id FROM t").await.len(),
        0,
        "prepared rows stay invisible until the global decision is durable"
    );

    assert_eq!(
        engine
            .commit_global_decision(global_xid, XidStatus::Committed)
            .await
            .expect("commit global"),
        XidStatus::Committed
    );
    participant
        .release_global_participant_commit(global_xid)
        .await
        .expect("release participant");

    let mut after_commit = engine.connect();
    assert_eq!(
        rows(&mut after_commit, "SELECT id FROM t").await.len(),
        1,
        "the external global commit, not participant release, makes rows visible"
    );
    assert_eq!(participant.tx_status(), TxStatus::Idle);
    assert_eq!(
        crabka_pgmvcc::clog::get(kv.as_ref(), local_xid).expect("local status"),
        XidStatus::Prepared(global_xid),
        "participant release must not overwrite the prepared marker"
    );
}

#[tokio::test]
async fn global_participant_abort_release_keeps_rows_invisible_after_external_decision() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
    engine.init_gtm_coordinator().expect("gtm");
    let mut setup = engine.connect();
    setup
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");

    let global_xid = engine.begin_global_durable().await.expect("global xid");
    let mut participant = engine.connect();
    participant.simple_query("BEGIN").await.expect("begin");
    participant
        .simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    let local_xid = participant.local_xid().expect("local xid");
    participant
        .prepare_global_participant(global_xid)
        .await
        .expect("prepare participant");

    assert_eq!(
        engine
            .commit_global_decision(global_xid, XidStatus::Aborted)
            .await
            .expect("abort global"),
        XidStatus::Aborted
    );
    participant
        .release_global_participant_abort(global_xid)
        .await
        .expect("release participant");

    let mut after_abort = engine.connect();
    assert_eq!(
        rows(&mut after_abort, "SELECT id FROM t").await.len(),
        0,
        "the external global abort keeps prepared rows invisible"
    );
    assert_eq!(participant.tx_status(), TxStatus::Idle);
    assert_eq!(
        crabka_pgmvcc::clog::get(kv.as_ref(), local_xid).expect("local status"),
        XidStatus::Prepared(global_xid),
        "abort release must not write a unilateral local abort"
    );
}

#[tokio::test]
async fn externally_prepared_participant_rejects_sql_commit_without_global_decision() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
    engine.init_gtm_coordinator().expect("gtm");
    let mut setup = engine.connect();
    setup
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");

    let global_xid = engine.begin_global_durable().await.expect("global xid");
    let mut participant = engine.connect();
    participant.simple_query("BEGIN").await.expect("begin");
    participant
        .simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    participant
        .prepare_global_participant(global_xid)
        .await
        .expect("prepare participant");

    let err = participant
        .simple_query("COMMIT")
        .await
        .expect_err("SQL COMMIT is not allowed after external prepare");
    assert_eq!(err.code, "55000");
    assert_eq!(
        crabka_pgmvcc::clog::get(kv.as_ref(), global_xid).expect("global status"),
        XidStatus::InProgress,
        "SQL COMMIT must not write a unilateral global commit decision"
    );

    engine
        .commit_global_decision(global_xid, XidStatus::Committed)
        .await
        .expect("external commit");
    participant
        .release_global_participant_commit(global_xid)
        .await
        .expect("release after external commit");
    let mut reader = engine.connect();
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await.len(), 1);
}

#[tokio::test]
async fn externally_prepared_participant_rejects_sql_rollback_without_global_decision() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
    engine.init_gtm_coordinator().expect("gtm");
    let mut setup = engine.connect();
    setup
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");

    let global_xid = engine.begin_global_durable().await.expect("global xid");
    let mut participant = engine.connect();
    participant.simple_query("BEGIN").await.expect("begin");
    participant
        .simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    participant
        .prepare_global_participant(global_xid)
        .await
        .expect("prepare participant");

    let err = participant
        .simple_query("ROLLBACK")
        .await
        .expect_err("SQL ROLLBACK is not allowed after external prepare");
    assert_eq!(err.code, "55000");
    assert_eq!(
        crabka_pgmvcc::clog::get(kv.as_ref(), global_xid).expect("global status"),
        XidStatus::InProgress,
        "SQL ROLLBACK must not write a unilateral global abort decision"
    );

    engine
        .commit_global_decision(global_xid, XidStatus::Aborted)
        .await
        .expect("external abort");
    participant
        .release_global_participant_abort(global_xid)
        .await
        .expect("release after external abort");
    let mut reader = engine.connect();
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await.len(), 0);
}

#[tokio::test]
async fn externally_prepared_participant_rejects_more_dml_before_release() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    let mut engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
    engine.init_gtm_coordinator().expect("gtm");
    let mut setup = engine.connect();
    setup
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");

    let global_xid = engine.begin_global_durable().await.expect("global xid");
    let mut participant = engine.connect();
    participant.simple_query("BEGIN").await.expect("begin");
    participant
        .simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    participant
        .prepare_global_participant(global_xid)
        .await
        .expect("prepare participant");

    let err = participant
        .simple_query("INSERT INTO t VALUES (2)")
        .await
        .expect_err("DML is not allowed after external prepare");
    assert_eq!(err.code, "55000");
    assert_eq!(
        table_version_count(kv.as_ref(), "t"),
        1,
        "rejected post-prepare DML must not add tuple versions"
    );

    engine
        .commit_global_decision(global_xid, XidStatus::Committed)
        .await
        .expect("external commit");
    participant
        .release_global_participant_commit(global_xid)
        .await
        .expect("release after external commit");
    let mut reader = engine.connect();
    let visible = rows(&mut reader, "SELECT id FROM t").await;
    assert_eq!(
        visible.len(),
        1,
        "only the pre-prepare write becomes visible"
    );
    assert_eq!(text(visible[0][0].as_ref()), Some("1".into()));
}

#[tokio::test]
async fn global_participant_api_rejects_invalid_transaction_states() {
    let mut engine = SqlEngine::new();
    engine.init_gtm_coordinator().expect("gtm");
    let global_xid = engine.begin_global_durable().await.expect("global xid");
    let mut participant = engine.connect();

    assert!(matches!(
        participant.prepare_global_participant(global_xid).await,
        Err(crabka_pgexec::ExecError::ObjectNotInPrerequisiteState(_))
    ));
    assert!(matches!(
        participant
            .release_global_participant_commit(global_xid)
            .await,
        Err(crabka_pgexec::ExecError::ObjectNotInPrerequisiteState(_))
    ));

    participant.simple_query("BEGIN").await.expect("begin");
    participant
        .prepare_global_participant(global_xid)
        .await
        .expect("prepare read-only participant");
    assert!(matches!(
        participant
            .release_global_participant_commit(global_xid + 1)
            .await,
        Err(crabka_pgexec::ExecError::ObjectNotInPrerequisiteState(_))
    ));
}

#[tokio::test]
async fn repeatable_read_does_not_see_concurrent_commit() {
    let engine = SqlEngine::new();
    let mut setup = engine.connect();
    setup
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    setup
        .simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("seed");
    let mut reader = engine.connect();
    reader
        .simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("begin rr");
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await.len(), 1);
    let mut writer = engine.connect();
    writer
        .simple_query("INSERT INTO t VALUES (2)")
        .await
        .expect("concurrent insert");
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await.len(), 1);
    reader.simple_query("COMMIT").await.expect("commit");
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await.len(), 2);
}

#[tokio::test]
async fn read_committed_sees_concurrent_commit_next_statement() {
    let engine = SqlEngine::new();
    let mut setup = engine.connect();
    setup
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    setup
        .simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("seed");
    let mut reader = engine.connect();
    reader.simple_query("BEGIN").await.expect("begin rc");
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await.len(), 1);
    let mut writer = engine.connect();
    writer
        .simple_query("INSERT INTO t VALUES (2)")
        .await
        .expect("concurrent insert");
    assert_eq!(rows(&mut reader, "SELECT id FROM t").await.len(), 2);
}

#[tokio::test]
async fn error_in_block_fails_transaction_until_rollback() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    s.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    s.simple_query("BEGIN").await.expect("begin");
    let err = s
        .simple_query("SELECT * FROM nope")
        .await
        .expect_err("undefined table");
    assert_eq!(err.code, "42P01");
    assert_eq!(s.tx_status(), TxStatus::Failed);
    let err = s.simple_query("SELECT 1").await.expect_err("aborted block");
    assert_eq!(err.code, "25P02");
    s.simple_query("ROLLBACK").await.expect("rollback");
    assert_eq!(s.tx_status(), TxStatus::Idle);
    s.simple_query("SELECT 1").await.expect("works again");
}

#[tokio::test]
async fn commit_of_failed_block_reports_rollback() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    s.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create");
    s.simple_query("BEGIN").await.expect("begin");
    s.simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    // Error inside the block → Failed state.
    let err = s
        .simple_query("SELECT * FROM nope")
        .await
        .expect_err("undefined table");
    assert_eq!(err.code, "42P01");
    assert_eq!(s.tx_status(), TxStatus::Failed);
    // COMMIT of a failed block must report the ROLLBACK tag and discard the write-set.
    let res = s.simple_query("COMMIT").await.expect("commit-of-failed");
    match &res[0] {
        QueryResult::Command { tag } => assert_eq!(tag, "ROLLBACK"),
        other => panic!("expected Command(ROLLBACK), got {other:?}"),
    }
    assert_eq!(s.tx_status(), TxStatus::Idle);
    // The INSERT was discarded.
    assert_eq!(rows(&mut s, "SELECT id FROM t").await.len(), 0);
}

#[tokio::test]
async fn repeatable_read_sees_old_value_after_concurrent_update() {
    let engine = SqlEngine::new();
    let mut setup = engine.connect();
    setup
        .simple_query("CREATE TABLE t (id int4, name text)")
        .await
        .expect("create");
    setup
        .simple_query("INSERT INTO t VALUES (1, 'old')")
        .await
        .expect("seed");

    let mut reader = engine.connect();
    reader
        .simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .expect("begin rr");
    // snapshot taken; reader sees 'old'
    let r = rows(&mut reader, "SELECT name FROM t").await;
    assert_eq!(text(r[0][0].as_ref()), Some("old".into()));

    // Another session updates the row and commits (autocommit).
    let mut writer = engine.connect();
    writer
        .simple_query("UPDATE t SET name = 'new' WHERE id = 1")
        .await
        .expect("concurrent update");

    // RR reader still sees 'old' (its snapshot predates the update's commit).
    let r = rows(&mut reader, "SELECT name FROM t").await;
    assert_eq!(
        text(r[0][0].as_ref()),
        Some("old".into()),
        "RR must not see the concurrent UPDATE"
    );
    reader.simple_query("COMMIT").await.expect("commit");
    // After commit, a fresh read sees 'new'.
    let r = rows(&mut reader, "SELECT name FROM t").await;
    assert_eq!(text(r[0][0].as_ref()), Some("new".into()));
}

#[tokio::test]
async fn ddl_inside_a_writing_transaction_does_not_deadlock() {
    let engine = SqlEngine::new();
    let mut s = engine.connect();
    s.simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create t");
    s.simple_query("BEGIN").await.expect("begin");
    s.simple_query("INSERT INTO t VALUES (1)")
        .await
        .expect("insert");
    // DDL while the txn already holds the writer lock must not deadlock.
    s.simple_query("CREATE TABLE u (x int4)")
        .await
        .expect("create u");
    s.simple_query("COMMIT").await.expect("commit");
    // both tables usable afterward
    s.simple_query("INSERT INTO u VALUES (2)")
        .await
        .expect("insert u");
    let r = rows(&mut s, "SELECT id FROM t").await;
    assert_eq!(r.len(), 1);
}
