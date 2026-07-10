#![allow(
    clippy::items_after_statements,
    clippy::missing_const_for_fn,
    clippy::needless_pass_by_value,
    clippy::similar_names,
    clippy::too_many_lines,
    reason = "integration proof harness keeps scenario data close to assertions"
)]

use std::sync::Arc;

use crabka_gres_ranges::{MultiRangeTenant, MultiRangeTenantConfig, TenantName};
use crabka_pgexec::{
    ExecError, PredicatePushdown, RangeScanner, ScanRequest, ScannedRow, SqlEngine,
    timestamp_txn::ReadTimestamp,
};
use crabka_pgkv::{Kv, MemKv, WriteOp, key};
use crabka_pgmvcc::{
    clog::{self, XidStatus},
    version,
    visibility::{Snapshot, satisfies_ts},
};
use crabka_pgtypes::Datum;
use crabka_pgwire::engine::{Engine, QueryResult, Session};

const LEFT: usize = 0;
const RIGHT: usize = 1;
const SHARDED_TABLE: &str = "ledger50";
const CONTROL_TABLE: &str = "ledger50";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GlobalOutcome {
    Committed(u64),
    Aborted,
    Pending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HistoryStep {
    shard: usize,
    rowid: u64,
    start_ts: u64,
    value: i32,
    outcome: GlobalOutcome,
}

#[derive(Clone)]
struct ManualShard {
    kv: Arc<dyn Kv>,
}

struct ManualScatterScanner {
    shards: Vec<ManualShard>,
}

struct VisibilityWorld {
    global: Arc<dyn Kv>,
    query_engine: SqlEngine,
    shard_stores: Vec<Arc<dyn Kv>>,
    control: Arc<dyn Kv>,
}

#[tokio::test]
async fn sharded_visibility_deterministic_history_matches_single_range_control_read_ts() {
    let history = deterministic_history();
    let world = VisibilityWorld::new().await;

    world.seed_sharded_history(SHARDED_TABLE, &history);
    world.seed_control_history(CONTROL_TABLE, &history);

    for read_ts in read_ts_sweep(&history) {
        assert_eq!(
            world.visible_values_at(SHARDED_TABLE, read_ts),
            world.control_values_at(CONTROL_TABLE, read_ts),
            "sharded scatter-gather visibility must match the single-range control at read_ts={}",
            read_ts.get()
        );
    }
}

#[tokio::test]
async fn sharded_visibility_pseudorandom_histories_match_single_range_control_read_ts() {
    for seed in 0..16 {
        let history = pseudorandom_history(seed);
        let world = VisibilityWorld::new().await;

        world.seed_sharded_history(SHARDED_TABLE, &history);
        world.seed_control_history(CONTROL_TABLE, &history);

        for read_ts in read_ts_sweep(&history) {
            assert_eq!(
                world.visible_values_at(SHARDED_TABLE, read_ts),
                world.control_values_at(CONTROL_TABLE, read_ts),
                "seed {seed} produced a sharded/control visibility mismatch at read_ts={}",
                read_ts.get()
            );
        }
    }
}

#[tokio::test]
async fn sharded_bank_variant_preserves_total_with_aborted_and_pending_noise_hidden() {
    let world = VisibilityWorld::new_with_table("acct50", "id int4, balance int4").await;
    let table = table_id(world.global.as_ref(), "acct50");

    world.put_version(
        LEFT,
        table,
        1,
        10,
        GlobalOutcome::Committed(20),
        &[Datum::Int4(1), Datum::Int4(93)],
    );
    world.put_version(
        RIGHT,
        table,
        2,
        11,
        GlobalOutcome::Committed(21),
        &[Datum::Int4(2), Datum::Int4(107)],
    );
    world.put_version(
        LEFT,
        table,
        3,
        12,
        GlobalOutcome::Aborted,
        &[Datum::Int4(1), Datum::Int4(-999)],
    );
    world.put_version(
        RIGHT,
        table,
        4,
        13,
        GlobalOutcome::Pending,
        &[Datum::Int4(2), Datum::Int4(999)],
    );

    let balances = rows_i32(
        &world.query_engine,
        "SELECT balance FROM acct50 ORDER BY id",
    )
    .await;

    assert_eq!(balances, vec![93, 107]);
    assert_eq!(balances.into_iter().sum::<i32>(), 200);
}

#[tokio::test]
async fn sharded_elle_variant_accepts_completed_history_and_hides_nonterminal_appends() {
    let world = VisibilityWorld::new_with_table("elle50", "key_id int4, value int4").await;
    let table = table_id(world.global.as_ref(), "elle50");

    world.put_version(
        LEFT,
        table,
        1,
        110,
        GlobalOutcome::Committed(210),
        &[Datum::Int4(1), Datum::Int4(1)],
    );
    world.put_version(
        RIGHT,
        table,
        2,
        111,
        GlobalOutcome::Committed(211),
        &[Datum::Int4(2), Datum::Int4(1)],
    );
    world.put_version(
        LEFT,
        table,
        3,
        112,
        GlobalOutcome::Committed(212),
        &[Datum::Int4(1), Datum::Int4(2)],
    );
    world.put_version(
        RIGHT,
        table,
        4,
        113,
        GlobalOutcome::Committed(213),
        &[Datum::Int4(2), Datum::Int4(2)],
    );
    world.put_version(
        LEFT,
        table,
        5,
        114,
        GlobalOutcome::Aborted,
        &[Datum::Int4(1), Datum::Int4(99)],
    );

    let observed = rows_i32(
        &world.query_engine,
        "SELECT value FROM elle50 WHERE key_id = 1 ORDER BY value",
    )
    .await;

    assert_eq!(observed, vec![1, 2]);
}

#[tokio::test]
async fn sharded_crossrange_nemesis_variant_flips_pending_rows_after_resolution() {
    let world = VisibilityWorld::new().await;
    let table = table_id(world.global.as_ref(), SHARDED_TABLE);
    let start_ts = 120;

    world.put_version(
        LEFT,
        table,
        1,
        start_ts,
        GlobalOutcome::Pending,
        &[Datum::Int4(7)],
    );
    world.put_version(
        RIGHT,
        table,
        2,
        start_ts,
        GlobalOutcome::Pending,
        &[Datum::Int4(11)],
    );

    assert_eq!(
        visible_values(&world.query_engine, SHARDED_TABLE).await,
        Vec::<i32>::new()
    );

    world.put_version(
        LEFT,
        table,
        1,
        start_ts,
        GlobalOutcome::Committed(125),
        &[Datum::Int4(7)],
    );
    world.put_version(
        RIGHT,
        table,
        2,
        start_ts,
        GlobalOutcome::Committed(125),
        &[Datum::Int4(11)],
    );

    assert_eq!(
        visible_values(&world.query_engine, SHARDED_TABLE).await,
        vec![7, 11]
    );
}

#[tokio::test]
async fn all_sharded_corpus_style_statements_execute_through_scanner_path() {
    let world = VisibilityWorld::new_with_tables(&[
        ("t50", "id int4, value int4"),
        ("t150", "id int4, value int4"),
    ])
    .await;
    let t50 = table_id(world.global.as_ref(), "t50");
    let t150 = table_id(world.global.as_ref(), "t150");

    world.put_version(
        LEFT,
        t50,
        1,
        130,
        GlobalOutcome::Committed(230),
        &[Datum::Int4(1), Datum::Int4(10)],
    );
    world.put_version(
        RIGHT,
        t150,
        1,
        131,
        GlobalOutcome::Committed(231),
        &[Datum::Int4(1), Datum::Int4(20)],
    );

    assert_eq!(
        rows_i32(
            &world.query_engine,
            "SELECT t50.value + t150.value FROM t50 JOIN t150 ON t50.id = t150.id",
        )
        .await,
        vec![30]
    );
    assert_eq!(
        rows_i32(
            &world.query_engine,
            "SELECT d.value FROM (SELECT value FROM t150 WHERE id = 1) d",
        )
        .await,
        vec![20]
    );
}

#[tokio::test]
async fn gateway_all_sharded_cross_range_statement_is_executable() {
    let config = MultiRangeTenantConfig::from_boundaries(
        TenantName::parse("tenant_sharded_statement_corpus").expect("tenant"),
        "0,100,200",
    )
    .expect("config");
    let (gateway, _handles) = MultiRangeTenant::start(config).expect("tenant");
    let mut session = gateway.connect();

    session
        .simple_query("CREATE TABLE t50 (id int4) SHARDED; CREATE TABLE t150 (id int4) SHARDED")
        .await
        .expect("create sharded tables");

    session
        .simple_query("SELECT * FROM t50 JOIN t150 ON true")
        .await
        .expect("all-sharded cross-range join remains executable through the gateway");
}

impl VisibilityWorld {
    async fn new() -> Self {
        Self::new_with_table(SHARDED_TABLE, "value int4").await
    }

    async fn new_with_table(name: &str, columns: &str) -> Self {
        Self::new_with_tables(&[(name, columns)]).await
    }

    async fn new_with_tables(tables: &[(&str, &str)]) -> Self {
        let global: Arc<dyn Kv> = Arc::new(MemKv::new());
        let shard_stores = vec![new_store(), new_store()];
        let control = new_store();
        let mut query_engine = SqlEngine::with_kv(Arc::clone(&global)).expect("query engine");
        let mut control_engine = SqlEngine::with_kv(Arc::clone(&control)).expect("control engine");

        for (name, columns) in tables {
            create_sharded_table(&mut query_engine, name, columns).await;
            create_sharded_table(&mut control_engine, name, columns).await;
        }

        query_engine.set_range_scanner(Arc::new(ManualScatterScanner {
            shards: shard_stores
                .iter()
                .map(|kv| ManualShard { kv: Arc::clone(kv) })
                .collect(),
        }));
        control_engine.set_range_scanner(Arc::new(ManualScatterScanner {
            shards: vec![ManualShard {
                kv: Arc::clone(&control),
            }],
        }));
        // The manually seeded histories use commit timestamps up to the low
        // hundreds. A real range-0 TSO has already advanced past those commits;
        // model that boundary explicitly rather than relying on MAX visibility.
        query_engine.set_timestamp_oracle(Arc::new(
            crabka_pgexec::timestamp_txn::LocalTimestampOracle::new(
                crabka_pgexec::timestamp_txn::MonotonicTimestampAllocator::starting_at(10_000)
                    .expect("finite test timestamp allocator"),
            ),
        ));

        Self {
            global,
            query_engine,
            shard_stores,
            control,
        }
    }

    fn seed_sharded_history(&self, table_name: &str, history: &[HistoryStep]) {
        let table = table_id(self.global.as_ref(), table_name);
        for step in history {
            self.put_version(
                step.shard,
                table,
                step.rowid,
                step.start_ts,
                step.outcome,
                &[Datum::Int4(step.value)],
            );
        }
    }

    fn seed_control_history(&self, table_name: &str, history: &[HistoryStep]) {
        let table = table_id(self.control.as_ref(), table_name);
        for step in history {
            put_prepared_version(
                self.control.as_ref(),
                table,
                step.rowid,
                step.start_ts,
                step.outcome,
                &[Datum::Int4(step.value)],
            );
        }
    }

    fn put_version(
        &self,
        shard: usize,
        table: u32,
        rowid: u64,
        start_ts: u64,
        outcome: GlobalOutcome,
        row: &[Datum],
    ) {
        put_prepared_version(
            self.shard_stores[shard].as_ref(),
            table,
            rowid,
            start_ts,
            outcome,
            row,
        );
    }

    fn visible_values_at(&self, table_name: &str, read_ts: ReadTimestamp) -> Vec<i32> {
        self.scan_values_at(&self.shard_stores, table_name, read_ts)
    }

    fn control_values_at(&self, table_name: &str, read_ts: ReadTimestamp) -> Vec<i32> {
        self.scan_values_at(std::slice::from_ref(&self.control), table_name, read_ts)
    }

    fn scan_values_at(
        &self,
        stores: &[Arc<dyn Kv>],
        table_name: &str,
        read_ts: ReadTimestamp,
    ) -> Vec<i32> {
        let scanner = ManualScatterScanner {
            shards: stores
                .iter()
                .map(|kv| ManualShard { kv: Arc::clone(kv) })
                .collect(),
        };
        let table =
            crabka_pgcatalog::get_table(self.global.as_ref(), table_name).expect("catalog table");
        let snapshot = Snapshot {
            xmin: 1,
            xmax: u64::MAX,
            xip: Vec::new(),
        };
        let rows = scanner
            .scan(ScanRequest {
                local: self.global.as_ref(),
                global: self.global.as_ref(),
                global_snapshot: &snapshot,
                snapshot: &snapshot,
                own_xid: None,
                read_ts: Some(read_ts),
                table: &table,
                interval: crabka_pgexec::RowInterval::ALL,
                predicate: PredicatePushdown::FullScan,
                projection: crabka_pgexec::ProjectionPushdown::All,
                partial_aggregate: None,
                top_k: None,
            })
            .expect("scan read_ts");
        rows.into_iter()
            .map(|row| match row.row.as_slice() {
                [Datum::Int4(value)] => *value,
                _ => panic!("expected one int4 column"),
            })
            .collect()
    }
}

impl RangeScanner for ManualScatterScanner {
    fn scan(&self, request: ScanRequest<'_>) -> Result<Vec<ScannedRow>, ExecError> {
        if !request.table.sharded {
            return crabka_pgexec::LocalRangeScanner.scan(request);
        }
        if request.predicate != PredicatePushdown::FullScan {
            return Err(ExecError::Unsupported(
                "manual sharded proof scanner only supports full scans".into(),
            ));
        }

        let mut rows = Vec::new();
        for shard in &self.shards {
            if request.table.sharded {
                rows.extend(scan_ts_visible_shard(&shard.kv, &request)?);
            } else {
                rows.extend(scan_visible_shard(&shard.kv, &request)?);
            }
        }
        rows.sort_by_key(|row| (row.rowid, row.xmin));
        Ok(rows)
    }
}

fn scan_ts_visible_shard(
    shard: &Arc<dyn Kv>,
    request: &ScanRequest<'_>,
) -> Result<Vec<ScannedRow>, ExecError> {
    let read_ts = request.read_ts.unwrap_or(ReadTimestamp::MAX);
    let scanned = shard.scan_prefix(&key::table_prefix(request.table.id))?;
    let mut visible = Vec::new();
    let mut index = 0;
    while index < scanned.len() {
        let prefix = version::row_prefix_of(&scanned[index].0)?.to_vec();
        let rowid = key::rowid_of(request.table.id, &prefix)?;
        let mut newest: Option<(u64, u64, Vec<Datum>)> = None;
        while index < scanned.len()
            && version::row_prefix_of(&scanned[index].0)? == prefix.as_slice()
        {
            let version = version::decode_ts_tuple(&scanned[index].1)?;
            if let version::TsVersionState::Committed { commit_ts } = version.state
                && request.interval.contains(rowid)
                && satisfies_ts(read_ts.get(), version.state)
                && newest
                    .as_ref()
                    .is_none_or(|(_, current_commit_ts, _)| commit_ts > *current_commit_ts)
            {
                newest = Some((version.start_ts, commit_ts, version.row));
            }
            index += 1;
        }
        if let Some((start_ts, _commit_ts, row)) = newest {
            visible.push(ScannedRow {
                rowid,
                xmin: start_ts,
                row,
            });
        }
    }
    Ok(visible)
}

fn scan_visible_shard(
    shard: &Arc<dyn Kv>,
    request: &ScanRequest<'_>,
) -> Result<Vec<ScannedRow>, ExecError> {
    let mut visible = Vec::new();
    for (key, value) in shard.scan_prefix(&key::table_prefix(request.table.id))? {
        let Some((_table, rowid)) = key::table_rowid_of(&key) else {
            continue;
        };
        if !request.interval.contains(rowid) {
            continue;
        }
        let (xmin, xmax, row) = version::decode_tuple(&value)?;
        if crabka_pgmvcc::visibility::satisfies_mvcc(
            xmin,
            xmax,
            request.snapshot,
            request.own_xid,
            |xid| resolve_status(shard.as_ref(), request.global, request.global_snapshot, xid),
        )? {
            visible.push(ScannedRow { rowid, xmin, row });
        }
    }
    Ok(visible)
}

fn resolve_status(
    local: &dyn Kv,
    global: &dyn Kv,
    global_snapshot: &Snapshot,
    xid: u64,
) -> Result<XidStatus, crabka_pgkv::KvError> {
    match clog::get(local, xid)? {
        XidStatus::Prepared(global_xid) => {
            if global_xid >= global_snapshot.xmax
                || global_snapshot.xip.binary_search(&global_xid).is_ok()
            {
                return Ok(XidStatus::InProgress);
            }
            clog::get(global, global_xid)
        }
        status => Ok(status),
    }
}

async fn create_sharded_table(engine: &mut SqlEngine, name: &str, columns: &str) {
    let mut session = engine.connect();
    session
        .simple_query(&format!("CREATE TABLE {name} ({columns}) SHARDED"))
        .await
        .expect("create sharded proof table");
}

async fn visible_values(engine: &SqlEngine, table: &str) -> Vec<i32> {
    rows_i32(engine, &format!("SELECT value FROM {table} ORDER BY value")).await
}

async fn rows_i32(engine: &SqlEngine, sql: &str) -> Vec<i32> {
    let mut session = engine.connect();
    let results = session.simple_query(sql).await.expect(sql);
    let [QueryResult::Rows { rows, .. }] = results.as_slice() else {
        panic!("expected one row result")
    };
    rows.iter()
        .map(|row| {
            let cell = row[0].as_ref().expect("non-null cell");
            std::str::from_utf8(&cell.text)
                .expect("utf8 cell")
                .parse::<i32>()
                .expect("i32 cell")
        })
        .collect()
}

fn put_prepared_version(
    kv: &dyn Kv,
    table: u32,
    rowid: u64,
    start_ts: u64,
    outcome: GlobalOutcome,
    row: &[Datum],
) {
    let state = match outcome {
        GlobalOutcome::Committed(commit_ts) => version::TsVersionState::Committed { commit_ts },
        GlobalOutcome::Aborted => version::TsVersionState::Aborted,
        GlobalOutcome::Pending => version::TsVersionState::Intent,
    };
    kv.write_batch(&[WriteOp::Put {
        key: version::version_key_ts(table, rowid, start_ts),
        value: version::encode_ts_tuple(start_ts, state, row),
    }])
    .expect("seed timestamp version");
}

fn table_id(kv: &dyn Kv, table_name: &str) -> u32 {
    crabka_pgcatalog::get_table(kv, table_name)
        .expect("catalog table")
        .id
}

fn new_store() -> Arc<dyn Kv> {
    Arc::new(MemKv::new())
}

fn deterministic_history() -> Vec<HistoryStep> {
    vec![
        step(LEFT, 1, 10, 10, GlobalOutcome::Committed(20)),
        step(RIGHT, 2, 11, 20, GlobalOutcome::Committed(21)),
        step(LEFT, 3, 12, 30, GlobalOutcome::Aborted),
        step(RIGHT, 4, 13, 40, GlobalOutcome::Pending),
        step(LEFT, 5, 14, 50, GlobalOutcome::Committed(30)),
    ]
}

fn pseudorandom_history(seed: u64) -> Vec<HistoryStep> {
    let mut state = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut history = Vec::with_capacity(12);
    for index in 0..12 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let outcome = match state % 3 {
            0 => GlobalOutcome::Committed(2_000 + seed * 100 + index),
            1 => GlobalOutcome::Aborted,
            _ => GlobalOutcome::Pending,
        };
        history.push(step(
            usize::try_from(state & 1).expect("shard index fits usize"),
            index + 1,
            1000 + seed * 100 + index,
            i32::try_from((state >> 8) % 10_000).expect("value fits i32"),
            outcome,
        ));
    }
    history
}

fn step(
    shard: usize,
    rowid: u64,
    start_ts: u64,
    value: i32,
    outcome: GlobalOutcome,
) -> HistoryStep {
    HistoryStep {
        shard,
        rowid,
        start_ts,
        value,
        outcome,
    }
}

fn read_ts_sweep(history: &[HistoryStep]) -> Vec<ReadTimestamp> {
    let mut timestamps = vec![ReadTimestamp::new(1).expect("read ts")];
    for step in history {
        if let GlobalOutcome::Committed(commit_ts) = step.outcome {
            if let Some(before) = commit_ts.checked_sub(1).filter(|value| *value > 0) {
                timestamps.push(ReadTimestamp::new(before).expect("read ts before commit"));
            }
            timestamps.push(ReadTimestamp::new(commit_ts).expect("read ts at commit"));
            timestamps.push(ReadTimestamp::new(commit_ts + 1).expect("read ts after commit"));
        }
    }
    timestamps.sort_unstable();
    timestamps.dedup();
    timestamps
}
