#![allow(
    clippy::items_after_statements,
    clippy::missing_const_for_fn,
    clippy::module_name_repetitions,
    clippy::needless_pass_by_value
)]

mod harness;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use crabka_gres_ranges::{
    CheckpointManifest, InDoubtMarker, MapEpoch, MergeRangeCommand, RangeId, RangeKey, RangeMap,
    RangeSpec, SplitCommand, SplitError, SplitHooks, SplitState, SplitStateStore, SplitStep,
    SuccessorDescriptor, TableId, TenantName, run_merge, run_split,
};
use crabka_pgwire::engine::Engine;
use harness::{first_i64, run};

const MAX_PAUSED_WRITES: usize = 6;

#[derive(Default)]
struct MemoryStore(Mutex<Option<SplitState>>);

#[async_trait::async_trait]
impl SplitStateStore for MemoryStore {
    async fn load_split_state(
        &self,
        _operation_id: &str,
    ) -> Result<Option<SplitState>, SplitError> {
        Ok(self.0.lock().expect("split state lock").clone())
    }

    async fn save_split_state(&self, state: &SplitState) -> Result<(), SplitError> {
        *self.0.lock().expect("split state lock") = Some(state.clone());
        Ok(())
    }
}

#[derive(Debug, Default)]
struct NemesisState {
    paused: bool,
    fenced_writers: BTreeSet<RangeId>,
    blocked_writes: usize,
    max_blocked_writes: usize,
    events: Vec<SplitStep>,
}

#[derive(Clone)]
struct NemesisHooks {
    state: Arc<Mutex<NemesisState>>,
    markers: Vec<InDoubtMarker>,
}

impl NemesisHooks {
    fn new(markers: Vec<InDoubtMarker>) -> Self {
        Self {
            state: Arc::new(Mutex::new(NemesisState::default())),
            markers,
        }
    }

    fn record_step(&self, step: SplitStep) {
        self.state
            .lock()
            .expect("nemesis state lock")
            .events
            .push(step);
    }

    fn can_write(&self, range_id: RangeId) -> bool {
        let mut state = self.state.lock().expect("nemesis state lock");
        if !state.paused && !state.fenced_writers.contains(&range_id) {
            state.blocked_writes = 0;
            return true;
        }

        state.blocked_writes += 1;
        state.max_blocked_writes = state.max_blocked_writes.max(state.blocked_writes);
        false
    }

    fn max_blocked_writes(&self) -> usize {
        self.state
            .lock()
            .expect("nemesis state lock")
            .max_blocked_writes
    }
}

#[async_trait::async_trait]
impl SplitHooks for NemesisHooks {
    async fn pause_conversion_writes(&self, _state: &SplitState) -> Result<(), SplitError> {
        self.record_step(SplitStep::PauseConversionWrites);
        self.state.lock().expect("nemesis state lock").paused = true;
        tokio::task::yield_now().await;
        Ok(())
    }

    async fn force_predecessor_checkpoint(
        &self,
        state: &SplitState,
    ) -> Result<CheckpointManifest, SplitError> {
        self.record_step(SplitStep::ForcePredecessorCheckpoint);
        Ok(CheckpointManifest {
            range_id: state.predecessor,
            covered_offset: 4,
            manifest_key: "split-nemesis-checkpoint".to_owned(),
        })
    }

    async fn force_right_predecessor_checkpoint(
        &self,
        state: &SplitState,
    ) -> Result<CheckpointManifest, SplitError> {
        self.record_step(SplitStep::ForceRightPredecessorCheckpoint);
        let right = state
            .merge_right_before
            .as_ref()
            .expect("merge right range present");
        Ok(CheckpointManifest {
            range_id: right.range_id,
            covered_offset: 5,
            manifest_key: "merge-right-checkpoint".to_owned(),
        })
    }

    async fn pause_writes_at_covered_offset(
        &self,
        _state: &SplitState,
        _checkpoint: &CheckpointManifest,
    ) -> Result<(), SplitError> {
        self.record_step(SplitStep::PauseWritesAtCoveredOffset);
        self.state.lock().expect("nemesis state lock").paused = true;
        tokio::task::yield_now().await;
        Ok(())
    }

    async fn commit_map_version(&self, _state: &SplitState) -> Result<(), SplitError> {
        self.record_step(SplitStep::CommitMapVersion);
        tokio::task::yield_now().await;
        Ok(())
    }

    async fn start_successor_restore(
        &self,
        _state: &SplitState,
        _checkpoint: &CheckpointManifest,
    ) -> Result<(), SplitError> {
        self.record_step(SplitStep::StartSuccessorRestore);
        tokio::task::yield_now().await;
        Ok(())
    }

    async fn start_merge_successor_restore(
        &self,
        _state: &SplitState,
        _left_checkpoint: &CheckpointManifest,
        _right_checkpoint: &CheckpointManifest,
    ) -> Result<(), SplitError> {
        self.record_step(SplitStep::StartSuccessorRestore);
        tokio::task::yield_now().await;
        Ok(())
    }

    async fn successor_fence_prologue(&self, state: &SplitState) -> Result<(), SplitError> {
        self.record_step(SplitStep::SuccessorFencePrologue);
        self.state
            .lock()
            .expect("nemesis state lock")
            .fenced_writers
            .insert(state.successor);
        tokio::task::yield_now().await;
        Ok(())
    }

    async fn inherit_in_doubt_markers(
        &self,
        state: &SplitState,
    ) -> Result<Vec<InDoubtMarker>, SplitError> {
        self.record_step(SplitStep::InheritInDoubtMarkers);
        Ok(self
            .markers
            .iter()
            .filter(|marker| state.successor_after.contains_key(marker.key))
            .cloned()
            .collect())
    }

    async fn park_predecessor(&self, state: &SplitState) -> Result<(), SplitError> {
        self.record_step(SplitStep::ParkPredecessor);
        self.state
            .lock()
            .expect("nemesis state lock")
            .fenced_writers
            .insert(state.predecessor);
        tokio::task::yield_now().await;
        Ok(())
    }

    async fn park_right_predecessor(&self, state: &SplitState) -> Result<(), SplitError> {
        self.record_step(SplitStep::ParkRightPredecessor);
        self.state
            .lock()
            .expect("nemesis state lock")
            .fenced_writers
            .insert(
                state
                    .merge_right_before
                    .as_ref()
                    .expect("merge right range present")
                    .range_id,
            );
        tokio::task::yield_now().await;
        Ok(())
    }

    async fn unpause_serving(&self, _state: &SplitState) -> Result<(), SplitError> {
        self.record_step(SplitStep::UnpauseServing);
        let mut state = self.state.lock().expect("nemesis state lock");
        state.paused = false;
        state.fenced_writers.clear();
        state.blocked_writes = 0;
        Ok(())
    }
}

#[derive(Debug, Default)]
struct LoadLedger {
    expected: BTreeMap<u64, i64>,
    attempted_writes: usize,
    applied_writes: usize,
}

impl LoadLedger {
    fn seed(table_ids: &[u64]) -> Self {
        Self {
            expected: table_ids.iter().map(|table_id| (*table_id, 0)).collect(),
            attempted_writes: 0,
            applied_writes: 0,
        }
    }

    fn apply(&mut self, table_id: u64, delta: i64) {
        self.attempted_writes += 1;
        self.applied_writes += 1;
        *self.expected.get_mut(&table_id).expect("seeded table id") += delta;
    }

    fn block(&mut self) {
        self.attempted_writes += 1;
    }
}

#[tokio::test]
async fn split_under_live_sharded_load_bounds_write_pause() {
    let tenant = TenantName::parse("tenant_split_nemesis").expect("tenant");
    let table_ids = [10, 60, 140, 180];
    let mut session = start_gateway_session(&tenant, &table_ids).await;
    let store = MemoryStore::default();
    let hooks = NemesisHooks::new(vec![marker(7, 60), marker(8, 140), marker(9, 180)]);
    let split = split_command(tenant.clone());
    let split_driver = run_split("split-nemesis", split, &store, &hooks);
    let load_driver = run_deterministic_load(&mut session, &hooks, &table_ids);

    let (split_state, ledger) = tokio::join!(split_driver, load_driver);
    let split_state = split_state.expect("split completes");

    assert_eq!(split_state.next_step, SplitStep::Complete);
    assert_eq!(
        split_state.inherited_markers,
        vec![marker(8, 140), marker(9, 180)]
    );
    assert_eq!(hooks.max_blocked_writes(), MAX_PAUSED_WRITES);
    assert!(ledger.applied_writes < ledger.attempted_writes);

    for (table_id, expected) in ledger.expected {
        assert_eq!(read_balance(&mut session, table_id).await, expected);
    }
}

#[tokio::test]
async fn split_retry_after_writer_fence_keeps_single_partitioned_fold() {
    let tenant = TenantName::parse("tenant_split_nemesis_retry").expect("tenant");
    let table_ids = [20, 120];
    let mut session = start_gateway_session(&tenant, &table_ids).await;
    let store = MemoryStore::default();
    let hooks = NemesisHooks::new(vec![marker(1, 20), marker(2, 120)]);

    write_balance(&mut session, 20, 11).await;
    write_balance(&mut session, 120, 17).await;
    let first = run_split(
        "split-nemesis-retry",
        split_command(tenant.clone()),
        &store,
        &hooks,
    )
    .await
    .expect("first split");
    let retry = run_split("split-nemesis-retry", split_command(tenant), &store, &hooks)
        .await
        .expect("retry split");

    assert_eq!(first, retry);
    assert_eq!(retry.inherited_markers, vec![marker(2, 120)]);
    assert_eq!(read_balance(&mut session, 20).await, 11);
    assert_eq!(read_balance(&mut session, 120).await, 17);
}

#[tokio::test]
async fn merge_under_live_sharded_load_bounds_write_pause() {
    let tenant = TenantName::parse("tenant_merge_nemesis").expect("tenant");
    let table_ids = [20, 120];
    let mut session = start_gateway_session(&tenant, &table_ids).await;
    let store = MemoryStore::default();
    let hooks = NemesisHooks::new(vec![marker(1, 20), marker(2, 120)]);
    let merge_driver = run_merge("merge-nemesis", merge_command(tenant), &store, &hooks);
    let load_driver = run_deterministic_load(&mut session, &hooks, &table_ids);

    let (merge_state, ledger) = tokio::join!(merge_driver, load_driver);
    let merge_state = merge_state.expect("merge completes");

    assert_eq!(merge_state.next_step, SplitStep::Complete);
    assert_eq!(merge_state.successor, RangeId::new(1));
    assert_eq!(
        merge_state.inherited_markers,
        vec![marker(1, 20), marker(2, 120)]
    );
    assert!(hooks.max_blocked_writes() <= MAX_PAUSED_WRITES);

    for (table_id, expected) in ledger.expected {
        assert_eq!(read_balance(&mut session, table_id).await, expected);
    }
}

async fn start_gateway_session(
    tenant: &TenantName,
    table_ids: &[u64],
) -> crabka_gres_ranges::tenant::GatewaySession {
    let config =
        crabka_gres_ranges::MultiRangeTenantConfig::from_boundaries(tenant.clone(), "0,100,200")
            .expect("config");
    let (gateway, _handles) = crabka_gres_ranges::MultiRangeTenant::start(config).expect("tenant");
    let mut session = gateway.connect();
    for table_id in table_ids {
        run(
            &mut session,
            &format!("CREATE TABLE t{table_id} (id int4, balance int4)"),
        )
        .await;
        run(
            &mut session,
            &format!("INSERT INTO t{table_id} VALUES (1, 0)"),
        )
        .await;
    }
    session
}

async fn run_deterministic_load(
    session: &mut crabka_gres_ranges::tenant::GatewaySession,
    hooks: &NemesisHooks,
    table_ids: &[u64],
) -> LoadLedger {
    let mut ledger = LoadLedger::seed(table_ids);
    for (round, table_id) in table_ids.iter().copied().cycle().take(12).enumerate() {
        let range_id = if table_id < 100 {
            RangeId::new(1)
        } else {
            RangeId::new(2)
        };
        let delta = i64::try_from(round + 1).expect("round fits i64");
        if hooks.can_write(range_id) {
            write_balance(session, table_id, delta).await;
            ledger.apply(table_id, delta);
        } else {
            ledger.block();
        }
        tokio::task::yield_now().await;
    }
    ledger
}

async fn write_balance(
    session: &mut crabka_gres_ranges::tenant::GatewaySession,
    table_id: u64,
    delta: i64,
) {
    let current = read_balance(session, table_id).await;
    let next = current.checked_add(delta).expect("test balance overflow");
    run(
        session,
        &format!("UPDATE t{table_id} SET balance = {next} WHERE id = 1"),
    )
    .await;
}

async fn read_balance(
    session: &mut crabka_gres_ranges::tenant::GatewaySession,
    table_id: u64,
) -> i64 {
    first_i64(&run(session, &format!("SELECT balance FROM t{table_id}")).await)
}

fn split_command(tenant: TenantName) -> SplitCommand {
    let split_at = RangeKey::table_start(TableId::new(100));
    SplitCommand {
        current_map: range_map(tenant),
        predecessor: RangeId::new(1),
        predecessor_generation: 3,
        left: SuccessorDescriptor {
            range_id: RangeId::new(2),
            endpoint: "left.internal:7443".into(),
            wal_generation: 4,
            interval: RangeSpec::for_interval(
                RangeId::new(2),
                RangeKey::table_start(TableId::new(1)),
                Some(split_at),
            ),
        },
        right: SuccessorDescriptor {
            range_id: RangeId::new(4),
            endpoint: "right.internal:7443".into(),
            wal_generation: 4,
            interval: RangeSpec::for_interval(
                RangeId::new(4),
                split_at,
                Some(RangeKey::table_start(TableId::new(200))),
            ),
        },
    }
}

fn merge_command(tenant: TenantName) -> MergeRangeCommand {
    MergeRangeCommand {
        current_map: merge_map(tenant),
        left: RangeId::new(1),
        right: RangeId::new(2),
        successor_generation: 9,
    }
}

fn range_map(tenant: TenantName) -> RangeMap {
    RangeMap::new(
        tenant,
        MapEpoch::new(4),
        vec![
            RangeSpec::new(RangeId::COORDINATOR, TableId::ZERO, Some(TableId::new(1))),
            RangeSpec::new(RangeId::new(1), TableId::new(1), Some(TableId::new(200))),
            RangeSpec::new(RangeId::new(3), TableId::new(200), None),
        ],
    )
    .expect("range map")
}

fn merge_map(tenant: TenantName) -> RangeMap {
    RangeMap::new(
        tenant,
        MapEpoch::new(4),
        vec![
            RangeSpec::new(RangeId::COORDINATOR, TableId::ZERO, Some(TableId::new(1))),
            RangeSpec::new(RangeId::new(1), TableId::new(1), Some(TableId::new(100))),
            RangeSpec::new(RangeId::new(2), TableId::new(100), Some(TableId::new(200))),
            RangeSpec::new(RangeId::new(3), TableId::new(200), None),
        ],
    )
    .expect("merge map")
}

fn marker(transaction_id: u64, table_id: u64) -> InDoubtMarker {
    InDoubtMarker {
        transaction_id,
        key: RangeKey::table_start(TableId::new(table_id)),
        hash_bucket: None,
    }
}
