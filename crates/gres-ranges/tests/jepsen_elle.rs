#![allow(clippy::items_after_statements, clippy::needless_pass_by_value)]

mod harness;

use std::{collections::BTreeMap, num::NonZeroU64, sync::Arc};

use crabka_gres_ranges::{MemoryTsoHorizon, RangeId, TsoError, TsoOracle};
use crabka_pgkv::MemKv;
use crabka_pgwire::engine::Engine;
use harness::{SystemHarness, process::ProcessHarness, row_count, run, try_run};
use stateright::semantics::{ConsistencyTester, LinearizabilityTester, SequentialSpec};
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq)]
struct TxnOp {
    client: u8,
    reads: Vec<ReadObservation>,
    appends: Vec<AppendMutation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReadObservation {
    key: Key,
    observed_len: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AppendMutation {
    key: Key,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Key {
    Left,
    Right,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct ListAppendSpec {
    left: Vec<i32>,
    right: Vec<i32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ListAppendOp {
    key: Key,
    value: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ListAppendRet {
    left: Vec<i32>,
    right: Vec<i32>,
}

impl SequentialSpec for ListAppendSpec {
    type Op = ListAppendOp;
    type Ret = ListAppendRet;

    fn invoke(&mut self, op: &Self::Op) -> Self::Ret {
        let observed = ListAppendRet {
            left: self.left.clone(),
            right: self.right.clone(),
        };
        match op.key {
            Key::Left => self.left.push(op.value),
            Key::Right => self.right.push(op.value),
        }
        observed
    }
}

type ElleChecker = Arc<Mutex<LinearizabilityTester<u8, ListAppendSpec>>>;

#[tokio::test]
async fn stateright_elle_accepts_real_process_history_across_participant_kill() {
    let mut system = ProcessHarness::start("tenant-real-elle").await;
    system
        .create_table_on_all(
            "CREATE TABLE elle50 (position int4, value int4); \
             CREATE TABLE elle150 (position int4, value int4)",
        )
        .await;
    let checker = Arc::new(Mutex::new(LinearizabilityTester::new(
        ListAppendSpec::default(),
    )));

    let first =
        real_observe_then_append(system.sql(0).await, Arc::clone(&checker), 0, Key::Left, 20);
    let second =
        real_observe_then_append(system.sql(0).await, Arc::clone(&checker), 1, Key::Right, 30);
    let (first, second) = tokio::join!(first, second);
    first.expect("first concurrent list append");
    second.expect("second concurrent list append");

    system.kill(1).await;
    assert!(
        real_observe_then_append(system.sql(0).await, Arc::clone(&checker), 2, Key::Right, 10)
            .await
            .is_err(),
        "operation against killed participant must be indeterminate"
    );
    system.restart(1).await;

    real_observe_then_append(system.sql(0).await, Arc::clone(&checker), 3, Key::Left, 5)
        .await
        .expect("post-recovery list append");

    let checker = checker.lock().await;
    assert_eq!(checker.len(), 4);
    assert!(
        checker.is_consistent(),
        "real process list-append history is not linearizable: {checker:?}"
    );
}

async fn real_observe_then_append(
    client: tokio_postgres::Client,
    checker: ElleChecker,
    process: u8,
    key: Key,
    value: i32,
) -> Result<(), tokio_postgres::Error> {
    let op = ListAppendOp { key, value };
    checker
        .lock()
        .await
        .on_invoke(process, op)
        .expect("valid Elle invocation");

    client.simple_query("BEGIN").await?;
    let left = query_list(&client, "elle50").await?;
    let right = query_list(&client, "elle150").await?;
    let position = match key {
        Key::Left => left.len(),
        Key::Right => right.len(),
    } + 1;
    client
        .simple_query(&format!(
            "INSERT INTO {} VALUES ({position}, {value})",
            key.table_name()
        ))
        .await?;
    client.simple_query("COMMIT").await?;

    checker
        .lock()
        .await
        .on_return(process, ListAppendRet { left, right })
        .expect("matching Elle return");
    Ok(())
}

async fn query_list(
    client: &tokio_postgres::Client,
    table: &str,
) -> Result<Vec<i32>, tokio_postgres::Error> {
    Ok(client
        .query(&format!("SELECT value FROM {table} ORDER BY position"), &[])
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect())
}

#[test]
fn list_append_checker_rejects_a_value_sorted_reordering() {
    let mut checker = LinearizabilityTester::<u8, ListAppendSpec>::new(ListAppendSpec::default());
    checker
        .on_invoke(
            0,
            ListAppendOp {
                key: Key::Left,
                value: 20,
            },
        )
        .unwrap();
    checker
        .on_return(
            0,
            ListAppendRet {
                left: vec![],
                right: vec![],
            },
        )
        .unwrap();
    checker
        .on_invoke(
            0,
            ListAppendOp {
                key: Key::Left,
                value: 5,
            },
        )
        .unwrap();
    checker
        .on_return(
            0,
            ListAppendRet {
                left: vec![5, 20],
                right: vec![],
            },
        )
        .unwrap();

    assert!(!checker.is_consistent());
}

#[tokio::test]
async fn deterministic_serializability_checker_limited_to_completed_histories_accepts_real_range_history()
 {
    let system = SystemHarness::start("tenant_elle_limited");
    let gateway = system.gateway();
    let mut session = gateway.connect();
    run(&mut session, "CREATE TABLE elle50 (id int4)").await;
    run(&mut session, "CREATE TABLE elle150 (id int4)").await;

    let history = vec![
        observe_then_append(&gateway, 0, Key::Left, 1).await,
        observe_then_append(&gateway, 1, Key::Right, 1).await,
        observe_then_append(&gateway, 0, Key::Left, 2).await,
        observe_then_append(&gateway, 1, Key::Right, 2).await,
    ];

    assert!(
        is_strictly_serializable_completed_history(&history),
        "limited deterministic checker rejected the completed history"
    );
}

#[tokio::test]
async fn sharded_timestamp_elle_history_survives_writer_kills_and_tso_fences() {
    let mut system = SystemHarness::start("tenant_ts_elle_kill_fence");
    let gateway = system.gateway();
    let mut setup = gateway.connect();
    run(&mut setup, "CREATE TABLE elle50 (id int4) SHARDED").await;
    run(&mut setup, "CREATE TABLE elle150 (id int4) SHARDED").await;

    assert_explicit_sharded_append_fails_clear(&gateway, Key::Left, 99).await;

    let first = observe_then_append(&gateway, 0, Key::Left, 1).await;
    system.kill_writer(RangeId::new(1));
    let killed = try_observe_then_append(&mut system, 1, Key::Right, 1).await;
    system.fence_and_recover(RangeId::new(1));
    assert!(killed.is_none());

    let store = Arc::new(MemKv::default());
    let horizon = MemoryTsoHorizon::new(store, 4);
    let old_oracle =
        TsoOracle::recover(horizon.clone(), horizon.clone(), 4, nonzero(8), 0).expect("old oracle");
    let old_lease = old_oracle.grant(nonzero(2)).await.expect("old lease");
    horizon.set_live_epoch(5).await;
    assert!(matches!(
        old_oracle
            .grant(nonzero(1))
            .await
            .expect_err("fenced oracle"),
        TsoError::FencedEpoch { epoch: 4 }
    ));
    let new_oracle = TsoOracle::recover(
        horizon.clone(),
        horizon.clone(),
        5,
        nonzero(8),
        horizon.load_max_ts().expect("horizon"),
    )
    .expect("new oracle");
    let new_lease = new_oracle.grant(nonzero(1)).await.expect("new lease");
    assert!(old_lease.last_ts().expect("last") < new_lease.first_ts);

    let second = observe_then_append(&gateway, 1, Key::Right, 1).await;
    let third = observe_then_append(&gateway, 0, Key::Left, 2).await;

    assert!(is_strictly_serializable_completed_history(&[
        first, second, third,
    ]));
}

#[test]
fn deterministic_serializability_checker_limited_to_completed_histories_rejects_stale_read() {
    let stale_history = vec![
        TxnOp {
            client: 0,
            reads: vec![ReadObservation {
                key: Key::Left,
                observed_len: 0,
            }],
            appends: vec![AppendMutation { key: Key::Left }],
        },
        TxnOp {
            client: 0,
            reads: vec![ReadObservation {
                key: Key::Left,
                observed_len: 0,
            }],
            appends: Vec::new(),
        },
    ];

    assert!(!is_strictly_serializable_completed_history(&stale_history));
}

async fn observe_then_append(
    gateway: &crabka_gres_ranges::MultiRangeTenant,
    client: u8,
    key: Key,
    value: i64,
) -> TxnOp {
    let mut session = gateway.connect();
    let table = key.table_name();
    let read_rows = run(&mut session, &format!("SELECT id FROM {table}")).await;
    let observed_len = row_count(&read_rows);
    run(
        &mut session,
        &format!("INSERT INTO {table} VALUES ({value})"),
    )
    .await;
    let count_rows = run(&mut session, &format!("SELECT id FROM {table}")).await;
    let committed_len = row_count(&count_rows);
    assert_eq!(committed_len, observed_len + 1);

    TxnOp {
        client,
        reads: vec![ReadObservation { key, observed_len }],
        appends: vec![AppendMutation { key }],
    }
}

async fn try_observe_then_append(
    system: &mut SystemHarness,
    client: u8,
    key: Key,
    value: i64,
) -> Option<TxnOp> {
    if system.fault_log().iter().any(|event| matches!(event, harness::FaultEvent::WriterKilled(range) if *range == key.range_id())) {
        return None;
    }
    Some(observe_then_append(&system.gateway(), client, key, value).await)
}

async fn assert_explicit_sharded_append_fails_clear(
    gateway: &crabka_gres_ranges::MultiRangeTenant,
    key: Key,
    value: i64,
) {
    let mut session = gateway.connect();
    let table = key.table_name();
    let before_rows = run(&mut session, &format!("SELECT id FROM {table}")).await;
    let before_len = row_count(&before_rows);

    run(&mut session, "BEGIN").await;
    let error = try_run(
        &mut session,
        &format!("INSERT INTO {table} VALUES ({value})"),
    )
    .await
    .expect_err("explicit sharded transaction write fails clear");
    assert_eq!(error.code, "0A000");
    let _ = try_run(&mut session, "ROLLBACK").await;

    let mut verification_session = gateway.connect();
    let after_rows = run(
        &mut verification_session,
        &format!("SELECT id FROM {table}"),
    )
    .await;
    let after_len = row_count(&after_rows);
    assert_eq!(after_len, before_len);
}

fn nonzero(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).expect("test value is non-zero")
}

fn is_strictly_serializable_completed_history(history: &[TxnOp]) -> bool {
    let mut state = BTreeMap::from([(Key::Left, 0_i64), (Key::Right, 0_i64)]);
    let mut last_client_index = BTreeMap::<u8, usize>::new();

    for (index, op) in history.iter().enumerate() {
        let last_index = last_client_index.entry(op.client).or_insert(index);
        if *last_index > index {
            return false;
        }
        *last_index = index;
        if !reads_match_state(&state, &op.reads) {
            return false;
        }
        for append in &op.appends {
            let Some(length) = state.get_mut(&append.key) else {
                return false;
            };
            *length = length
                .checked_add(1)
                .expect("list length does not overflow");
        }
    }
    true
}

fn reads_match_state(state: &BTreeMap<Key, i64>, reads: &[ReadObservation]) -> bool {
    reads
        .iter()
        .all(|read| state.get(&read.key) == Some(&read.observed_len))
}

impl Key {
    fn table_name(self) -> &'static str {
        match self {
            Self::Left => "elle50",
            Self::Right => "elle150",
        }
    }

    fn range_id(self) -> RangeId {
        match self {
            Self::Left => RangeId::new(0),
            Self::Right => RangeId::new(1),
        }
    }
}
