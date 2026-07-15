//! The durable-timestamp read floor must be O(1) per statement — no store
//! rescan — while still staying strictly above every durable commit timestamp,
//! including commits made through another engine sharing the same store and
//! timestamp state that predates the engine's open.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use assert2::assert;
use crabka_pgexec::{
    CommitTimestamp, PrimaryTxnDecision, SqlEngine, TimestampTransactionId, TimestampTxnDescriptor,
    TimestampTxnOperation, TimestampWrite,
};
use crabka_pgkv::{Kv, KvError, KvScan, MemKv, WriteOp};
use crabka_pgwire::engine::{Engine, Session};

/// [`Kv`] decorator that counts range scans, the operation the old
/// per-statement `durable_timestamp_horizon` implementation was built on.
struct RangeScanCountingKv {
    inner: MemKv,
    scan_range_calls: AtomicUsize,
}

impl RangeScanCountingKv {
    fn new() -> Self {
        Self {
            inner: MemKv::new(),
            scan_range_calls: AtomicUsize::new(0),
        }
    }
}

impl Kv for RangeScanCountingKv {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.inner.get(key)
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), KvError> {
        self.inner.put(key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<(), KvError> {
        self.inner.delete(key)
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<KvScan, KvError> {
        self.inner.scan_prefix(prefix)
    }

    fn scan_range(&self, start: &[u8], end: &[u8]) -> Result<KvScan, KvError> {
        self.scan_range_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_range(start, end)
    }

    fn write_batch(&self, ops: &[WriteOp]) -> Result<(), KvError> {
        self.inner.write_batch(ops)
    }
}

fn sharded_write(table_id: u32, rowid: u64) -> TimestampWrite {
    TimestampWrite {
        table_id,
        bucket: None,
        rowid,
        row: vec![crabka_pgtypes::Datum::Int4(7)],
        delete: false,
        global_index_intents: Vec::new(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn select_statements_do_not_rescan_the_store_for_the_timestamp_floor() {
    let kv = Arc::new(RangeScanCountingKv::new());
    let engine = SqlEngine::with_kv(Arc::clone(&kv) as Arc<dyn Kv>).expect("engine");
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE t (id int4)")
        .await
        .expect("create table");
    for id in 0..64 {
        session
            .simple_query(&format!("INSERT INTO t VALUES ({id})"))
            .await
            .expect("insert row");
    }

    // The first read may seed the cached horizon with one full scan.
    session.simple_query("SELECT 1").await.expect("warm select");
    let seeded = kv.scan_range_calls.load(Ordering::SeqCst);

    for _ in 0..8 {
        session.simple_query("SELECT 1").await.expect("select");
    }
    let after = kv.scan_range_calls.load(Ordering::SeqCst);
    assert!(
        after == seeded,
        "SELECT statements rescanned the store for the timestamp floor: \
         {seeded} scans after warmup, {after} after eight more statements"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timestamp_commit_advances_the_cached_read_floor() {
    let engine = SqlEngine::new();
    let mut session = engine.connect();
    session
        .simple_query("CREATE TABLE s (id int4) SHARDED")
        .await
        .expect("create sharded table");
    let table = engine.catalog_table("s").expect("catalog table");

    // Seed the cached horizon BEFORE the commit so a stale cache would be
    // caught: the commit below must advance the already-warm floor.
    let warm = engine
        .allocate_timestamp_read_timestamp()
        .await
        .expect("warm read timestamp");
    let start_ts = TimestampTransactionId::new(1_000_000).expect("start timestamp");
    let commit_ts = CommitTimestamp::new(1_000_500).expect("commit timestamp");
    assert!(warm.get() < start_ts.get());

    let write = sharded_write(table.id, 1);
    let participant = engine.timestamp_txn_participant(0);
    participant
        .prewrite(start_ts, std::slice::from_ref(&write))
        .await
        .expect("prewrite");
    participant
        .commit(start_ts, commit_ts, std::slice::from_ref(&write))
        .await
        .expect("commit");

    let read = engine
        .allocate_timestamp_read_timestamp()
        .await
        .expect("read timestamp after commit");
    assert!(read.get() > commit_ts.get());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reopened_engine_seeds_the_floor_from_durable_timestamp_state() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    {
        let engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
        let mut session = engine.connect();
        session
            .simple_query("CREATE TABLE s (id int4) SHARDED")
            .await
            .expect("create sharded table");
        let table = engine.catalog_table("s").expect("catalog table");
        let start_ts = TimestampTransactionId::new(2_000_000).expect("start timestamp");
        let commit_ts = CommitTimestamp::new(2_000_600).expect("commit timestamp");
        let write = sharded_write(table.id, 1);
        let participant = engine.timestamp_txn_participant(0);
        participant
            .prewrite(start_ts, std::slice::from_ref(&write))
            .await
            .expect("prewrite");
        participant
            .commit(start_ts, commit_ts, std::slice::from_ref(&write))
            .await
            .expect("commit");
    }

    // "Restart": a fresh engine (fresh volatile oracle) over the same durable
    // store must recover the floor by seeding from the store.
    let engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("reopened engine");
    let read = engine
        .allocate_timestamp_read_timestamp()
        .await
        .expect("read timestamp after reopen");
    assert!(read.get() > 2_000_600);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn floor_seed_covers_timestamp_state_written_before_the_engine_opened() {
    let kv: Arc<dyn Kv> = Arc::new(MemKv::new());
    kv.write_batch(&[WriteOp::Put {
        key: crabka_pgmvcc::version::version_key_ts(7, 1, 3_000_000),
        value: crabka_pgmvcc::version::encode_ts_tuple(
            3_000_000,
            crabka_pgmvcc::version::TsVersionState::Committed {
                commit_ts: 3_000_777,
            },
            &[crabka_pgtypes::Datum::Int4(7)],
        ),
    }])
    .expect("seed durable timestamp tuple");

    let engine = SqlEngine::with_kv(Arc::clone(&kv)).expect("engine");
    let read = engine
        .allocate_timestamp_read_timestamp()
        .await
        .expect("read timestamp");
    assert!(read.get() > 3_000_777);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn descriptor_commit_on_a_shared_catalog_store_raises_other_engines_floor() {
    let range0 = SqlEngine::new();
    let mut data = SqlEngine::new();
    data.set_catalog_kv(range0.kv_handle());

    // Warm the data-range engine's cached horizon before range 0 decides.
    let warm = data
        .allocate_timestamp_read_timestamp()
        .await
        .expect("warm read timestamp");
    assert!(warm.get() < 4_000_000);

    let start_ts = TimestampTransactionId::new(4_000_000).expect("start timestamp");
    let commit_ts = CommitTimestamp::new(4_000_900).expect("commit timestamp");
    let descriptor = TimestampTxnDescriptor::begun(start_ts, 42, vec![1]);
    range0
        .begin_timestamp_transaction(&descriptor)
        .await
        .expect("begin descriptor");
    range0
        .acknowledge_timestamp_participant_operations(
            start_ts,
            1,
            &[TimestampTxnOperation {
                range_id: 1,
                table_id: 9,
                bucket: None,
                rowid: 1,
                delete: false,
            }],
        )
        .await
        .expect("acknowledge participant");
    let decision = range0
        .decide_timestamp_transaction(start_ts, PrimaryTxnDecision::Committed(commit_ts))
        .await
        .expect("decide commit");
    assert!(decision == PrimaryTxnDecision::Committed(commit_ts));

    // The data engine reads range 0's store as its catalog: its floor must
    // reflect range 0's commit without a per-statement rescan.
    let read = data
        .allocate_timestamp_read_timestamp()
        .await
        .expect("read timestamp after range-0 decision");
    assert!(read.get() > commit_ts.get());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observing_an_external_durable_timestamp_raises_the_floor() {
    let engine = SqlEngine::new();
    let warm = engine
        .allocate_timestamp_read_timestamp()
        .await
        .expect("warm read timestamp");
    assert!(warm.get() < 5_000_000);

    engine.observe_durable_timestamp(5_000_000);
    let read = engine
        .allocate_timestamp_read_timestamp()
        .await
        .expect("read timestamp after observe");
    assert!(read.get() > 5_000_000);
}
