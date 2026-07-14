use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use assert2::assert;
use async_trait::async_trait;
use crabka_gres_ranges::{
    MapEpoch, RangeId, RangeMap, RangeSpec, SplitCommand, SplitError, SplitHooks, SplitState,
    SplitStateStore, SplitStep, SuccessorDescriptor, TableId, TenantName, run_split,
};
use crabka_gres_substrate::{InMemorySplitStateStore, RawKvSplitRuntime};
use crabka_pgkv::{Kv, key};

#[tokio::test]
async fn raw_kv_filtered_restore_replays_tail_and_keeps_successor_closed() {
    let runtime = Arc::new(RawKvSplitRuntime::new("raw-split"));
    runtime
        .add_serving_range(RangeId::new(1))
        .expect("source range");
    let left = key::row_key(15, 1);
    let right = key::row_key(25, 1);
    let boundary = key::row_key(20, 1);
    let tail = key::row_key(25, 2);
    runtime
        .write_row(RangeId::new(1), left.clone(), b"left".to_vec())
        .await
        .expect("write left row");
    runtime
        .write_row(RangeId::new(1), right.clone(), b"right".to_vec())
        .await
        .expect("write right row");
    runtime
        .write_row(RangeId::new(1), boundary.clone(), b"boundary".to_vec())
        .await
        .expect("write split-boundary row");

    // Exercise the real checkpoint/filter restore boundary before prologue:
    // restoring data alone must not make the successor serve.
    let mut pre_prologue = SplitState::for_split("pre-prologue", split_command()).expect("state");
    let checkpoint = runtime
        .force_predecessor_checkpoint(&pre_prologue)
        .await
        .expect("checkpoint");
    assert!(
        runtime
            .has_checkpoint_manifest(&checkpoint.manifest_key)
            .await
            .expect("manifest")
    );
    pre_prologue.checkpoint = Some(checkpoint.clone());
    runtime
        .write_row(RangeId::new(1), tail.clone(), b"tail".to_vec())
        .await
        .expect("write committed checkpoint tail");
    runtime
        .pause_writes_at_covered_offset(&pre_prologue, &checkpoint)
        .await
        .expect("pause");
    runtime
        .commit_map_version(&pre_prologue)
        .await
        .expect("map commit");
    assert!(
        runtime
            .write_row(RangeId::new(2), right.clone(), b"blocked".to_vec())
            .await
            .is_err()
    );
    runtime
        .start_successor_restore(&pre_prologue, &checkpoint)
        .await
        .expect("filtered restore");
    assert!(!runtime.is_serving(RangeId::new(2)).expect("successor gate"));
    let restored = runtime.kv(RangeId::new(2)).expect("successor KV");
    assert!(restored.get(&left).expect("left read") == None);
    assert!(restored.get(&right).expect("right read") == Some(b"right".to_vec()));
    assert!(restored.get(&boundary).expect("boundary read") == Some(b"boundary".to_vec()));
    assert!(restored.get(&tail).expect("tail read") == Some(b"tail".to_vec()));
}

#[tokio::test]
async fn raw_kv_split_moves_only_successor_rows_and_fences_predecessor() {
    let runtime = RawKvSplitRuntime::new("raw-split-run");
    let source = runtime
        .add_serving_range(RangeId::new(1))
        .expect("source range");
    let left = key::row_key(15, 1);
    let right = key::row_key(25, 1);
    let boundary = key::row_key(20, 1);
    runtime
        .write_row(RangeId::new(1), left.clone(), b"left".to_vec())
        .await
        .expect("write left row");
    runtime
        .write_row(RangeId::new(1), right.clone(), b"right".to_vec())
        .await
        .expect("write right row");
    runtime
        .write_row(RangeId::new(1), boundary.clone(), b"boundary".to_vec())
        .await
        .expect("write split-boundary row");
    let state = run_split(
        "raw-kv-split",
        split_command(),
        &InMemorySplitStateStore::default(),
        &runtime,
    )
    .await
    .expect("raw-KV split");

    let manifest = state.checkpoint.expect("durable checkpoint manifest");
    assert!(
        runtime
            .has_checkpoint_manifest(&manifest.manifest_key)
            .await
            .expect("manifest")
    );
    assert!(source.get(&left).expect("source left") == Some(b"left".to_vec()));
    assert!(source.get(&right).expect("source right") == Some(b"right".to_vec()));
    let successor = runtime.kv(RangeId::new(2)).expect("successor KV");
    assert!(successor.get(&left).expect("successor left") == None);
    assert!(successor.get(&right).expect("successor right") == Some(b"right".to_vec()));
    assert!(successor.get(&boundary).expect("successor boundary") == Some(b"boundary".to_vec()));
    assert!(
        runtime
            .is_serving(RangeId::new(2))
            .expect("successor serving")
    );
    assert!(
        runtime
            .write_row(RangeId::new(1), key::row_key(15, 2), b"blocked".to_vec())
            .await
            .is_err()
    );
    assert!(
        runtime
            .write_row(RangeId::new(1), b"not-a-row".to_vec(), b"blocked".to_vec())
            .await
            .is_err()
    );
    assert!(
        runtime
            .write_row(RangeId::new(2), left, b"wrong-range".to_vec())
            .await
            .is_err()
    );
    runtime
        .write_row(RangeId::new(2), boundary, b"successor-write".to_vec())
        .await
        .expect("successor owns split boundary");
    let successor_checkpoint = runtime
        .force_checkpoint(RangeId::new(2))
        .await
        .expect("post-prologue checkpoint");
    assert!(successor_checkpoint.manifest.wal_generation == 1);
}

#[tokio::test]
async fn raw_kv_split_retries_restore_after_state_save_failure() {
    let runtime = RawKvSplitRuntime::new("raw-split-retry");
    runtime
        .add_serving_range(RangeId::new(1))
        .expect("source range");
    let restored_key = key::row_key(25, 1);
    runtime
        .write_row(RangeId::new(1), restored_key.clone(), b"restored".to_vec())
        .await
        .expect("source write");

    let state_store = FailOnceAfterRestore::default();
    assert!(
        run_split("restore-retry", split_command(), &state_store, &runtime)
            .await
            .is_err()
    );
    assert!(
        runtime
            .kv(RangeId::new(2))
            .expect("staged successor")
            .get(&restored_key)
            .expect("staged row")
            == Some(b"restored".to_vec())
    );

    run_split("restore-retry", split_command(), &state_store, &runtime)
        .await
        .expect("retry restores into a fresh replacement store");
    assert!(
        runtime
            .kv(RangeId::new(2))
            .expect("successor")
            .get(&restored_key)
            .expect("restored row")
            == Some(b"restored".to_vec())
    );
}

#[tokio::test]
async fn raw_kv_split_keeps_successor_closed_until_prologue_state_is_durable() {
    let runtime = RawKvSplitRuntime::new("raw-split-prologue-retry");
    runtime
        .add_serving_range(RangeId::new(1))
        .expect("source range");
    let successor_key = key::row_key(25, 1);
    runtime
        .write_row(RangeId::new(1), successor_key.clone(), b"restored".to_vec())
        .await
        .expect("source write");

    let state_store = FailOnceAfterPrologue::default();
    assert!(
        run_split("prologue-retry", split_command(), &state_store, &runtime)
            .await
            .is_err()
    );
    assert!(
        !runtime
            .is_serving(RangeId::new(2))
            .expect("successor closed")
    );
    assert!(
        runtime
            .write_row(
                RangeId::new(2),
                successor_key.clone(),
                b"must-not-enter-wal".to_vec(),
            )
            .await
            .is_err()
    );

    run_split("prologue-retry", split_command(), &state_store, &runtime)
        .await
        .expect("retry reuses the closed prologue result");
    assert!(
        runtime
            .is_serving(RangeId::new(2))
            .expect("successor serving")
    );
    assert!(
        runtime
            .kv(RangeId::new(2))
            .expect("successor")
            .get(&successor_key)
            .expect("restored row")
            == Some(b"restored".to_vec())
    );

    runtime
        .write_row(
            RangeId::new(2),
            successor_key,
            b"post-prologue-write".to_vec(),
        )
        .await
        .expect("writes open after prologue state persists");
    let checkpoint = runtime
        .force_checkpoint(RangeId::new(2))
        .await
        .expect("checkpoint after post-prologue write");
    assert!(checkpoint.manifest.wal_generation == 1);
}

#[derive(Default)]
struct FailOnceAfterRestore {
    states: Mutex<std::collections::BTreeMap<String, SplitState>>,
    fail_once: AtomicBool,
}

#[derive(Default)]
struct FailOnceAfterPrologue {
    states: Mutex<std::collections::BTreeMap<String, SplitState>>,
    fail_once: AtomicBool,
}

#[async_trait]
impl SplitStateStore for FailOnceAfterPrologue {
    async fn load_split_state(&self, operation_id: &str) -> Result<Option<SplitState>, SplitError> {
        Ok(self
            .states
            .lock()
            .expect("test store lock")
            .get(operation_id)
            .cloned())
    }

    async fn save_split_state(&self, state: &SplitState) -> Result<(), SplitError> {
        if state.next_step == SplitStep::InheritInDoubtMarkers
            && !self.fail_once.swap(true, Ordering::SeqCst)
        {
            return Err(SplitError::Store(
                "injected save failure after successful prologue".into(),
            ));
        }
        self.states
            .lock()
            .expect("test store lock")
            .insert(state.operation_id.clone(), state.clone());
        Ok(())
    }
}

#[async_trait]
impl SplitStateStore for FailOnceAfterRestore {
    async fn load_split_state(&self, operation_id: &str) -> Result<Option<SplitState>, SplitError> {
        Ok(self
            .states
            .lock()
            .expect("test store lock")
            .get(operation_id)
            .cloned())
    }

    async fn save_split_state(&self, state: &SplitState) -> Result<(), SplitError> {
        if state.next_step == SplitStep::SuccessorFencePrologue
            && !self.fail_once.swap(true, Ordering::SeqCst)
        {
            return Err(SplitError::Store(
                "injected save failure after restore".into(),
            ));
        }
        self.states
            .lock()
            .expect("test store lock")
            .insert(state.operation_id.clone(), state.clone());
        Ok(())
    }
}

fn split_command() -> SplitCommand {
    SplitCommand {
        current_map: RangeMap::new(
            TenantName::parse("raw-split-tenant").expect("tenant"),
            MapEpoch::new(1),
            vec![
                RangeSpec::new(RangeId::COORDINATOR, TableId::ZERO, Some(TableId::new(10))),
                RangeSpec::new(RangeId::new(1), TableId::new(10), Some(TableId::new(30))),
                RangeSpec::new(RangeId::new(3), TableId::new(30), None),
            ],
        )
        .expect("map"),
        predecessor: RangeId::new(1),
        predecessor_generation: 0,
        left: SuccessorDescriptor {
            range_id: RangeId::new(4),
            interval: RangeSpec::new(RangeId::new(4), TableId::new(10), Some(TableId::new(20))),
            endpoint: "local.test:7443".into(),
            wal_generation: 1,
        },
        right: SuccessorDescriptor {
            range_id: RangeId::new(2),
            interval: RangeSpec::new(RangeId::new(2), TableId::new(20), Some(TableId::new(30))),
            endpoint: "local.test:7443".into(),
            wal_generation: 1,
        },
    }
}
