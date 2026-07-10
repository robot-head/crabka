use std::sync::{Arc, Mutex};

use assert2::assert;
use crabka_gres_ranges::{
    CheckpointManifest, CheckpointOperation, FilteredSuccessorRestoreOperation, InDoubtMarker,
    InDoubtMarkerInheritanceOperation, MapEpoch, PredecessorParkingOperation, RangeId, RangeKey,
    RangeMap, RangeMapCommitOperation, RangeSpec, SplitCommand, SplitError,
    SplitHookAdapterBuilder, SplitHookOperation, SplitState, SplitStateStore, SplitStep,
    SuccessorPrologueOperation, TableId, TenantName, WriteGateOperation, run_split,
};

#[derive(Default)]
struct MemoryStore(Mutex<Option<SplitState>>);

#[async_trait::async_trait]
impl SplitStateStore for MemoryStore {
    async fn load_split_state(
        &self,
        _operation_id: &str,
    ) -> Result<Option<SplitState>, SplitError> {
        Ok(self.0.lock().expect("state lock").clone())
    }

    async fn save_split_state(&self, state: &SplitState) -> Result<(), SplitError> {
        *self.0.lock().expect("state lock") = Some(state.clone());
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Event {
    Checkpoint,
    Pause,
    Commit,
    Restore,
    Prologue,
    Inherit,
    Park,
    Unpause,
}

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Event>>>);

impl Recorder {
    fn record(&self, event: Event) {
        self.0.lock().expect("recorder lock").push(event);
    }

    fn events(&self) -> Vec<Event> {
        self.0.lock().expect("recorder lock").clone()
    }
}

struct RecordingCheckpoint(Recorder);

#[async_trait::async_trait]
impl CheckpointOperation for RecordingCheckpoint {
    async fn force_predecessor_checkpoint(
        &self,
        state: &SplitState,
    ) -> Result<CheckpointManifest, SplitError> {
        self.0.record(Event::Checkpoint);
        Ok(CheckpointManifest {
            range_id: state.predecessor,
            covered_offset: 42,
            manifest_key: "recorded-checkpoint".to_owned(),
        })
    }

    async fn force_right_predecessor_checkpoint(
        &self,
        _state: &SplitState,
    ) -> Result<CheckpointManifest, SplitError> {
        Err(SplitError::Hook(
            "right checkpoint is not used by this split".to_owned(),
        ))
    }
}

struct RecordingWriteGate(Recorder);

#[async_trait::async_trait]
impl WriteGateOperation for RecordingWriteGate {
    async fn pause_conversion_writes(&self, _state: &SplitState) -> Result<(), SplitError> {
        Err(SplitError::Hook(
            "conversion gate is not used by this split".to_owned(),
        ))
    }

    async fn pause_writes_at_covered_offset(
        &self,
        _state: &SplitState,
        _checkpoint: &CheckpointManifest,
    ) -> Result<(), SplitError> {
        self.0.record(Event::Pause);
        Ok(())
    }

    async fn unpause_serving(&self, _state: &SplitState) -> Result<(), SplitError> {
        self.0.record(Event::Unpause);
        Ok(())
    }
}

struct RecordingMapCommit(Recorder);

#[async_trait::async_trait]
impl RangeMapCommitOperation for RecordingMapCommit {
    async fn commit_map_version(&self, _state: &SplitState) -> Result<(), SplitError> {
        self.0.record(Event::Commit);
        Ok(())
    }
}

struct RecordingRestore(Recorder);

#[async_trait::async_trait]
impl FilteredSuccessorRestoreOperation for RecordingRestore {
    async fn start_successor_restore(
        &self,
        _state: &SplitState,
        _checkpoint: &CheckpointManifest,
    ) -> Result<(), SplitError> {
        self.0.record(Event::Restore);
        Ok(())
    }

    async fn start_merge_successor_restore(
        &self,
        _state: &SplitState,
        _left_checkpoint: &CheckpointManifest,
        _right_checkpoint: &CheckpointManifest,
    ) -> Result<(), SplitError> {
        Err(SplitError::Hook(
            "merge restore is not used by this split".to_owned(),
        ))
    }
}

struct RecordingPrologue(Recorder);

#[async_trait::async_trait]
impl SuccessorPrologueOperation for RecordingPrologue {
    async fn successor_fence_prologue(&self, _state: &SplitState) -> Result<(), SplitError> {
        self.0.record(Event::Prologue);
        Ok(())
    }
}

struct RecordingInheritance(Recorder);

#[async_trait::async_trait]
impl InDoubtMarkerInheritanceOperation for RecordingInheritance {
    async fn inherit_in_doubt_markers(
        &self,
        _state: &SplitState,
    ) -> Result<Vec<InDoubtMarker>, SplitError> {
        self.0.record(Event::Inherit);
        Ok(Vec::new())
    }
}

struct RecordingParking(Recorder);

#[async_trait::async_trait]
impl PredecessorParkingOperation for RecordingParking {
    async fn park_predecessor(&self, _state: &SplitState) -> Result<(), SplitError> {
        self.0.record(Event::Park);
        Ok(())
    }

    async fn park_right_predecessor(&self, _state: &SplitState) -> Result<(), SplitError> {
        Err(SplitError::Hook(
            "right parking is not used by this split".to_owned(),
        ))
    }
}

fn fully_wired_adapter(recorder: &Recorder) -> crabka_gres_ranges::SplitHookAdapter {
    SplitHookAdapterBuilder::new()
        .checkpoint(Arc::new(RecordingCheckpoint(recorder.clone())))
        .write_gate(Arc::new(RecordingWriteGate(recorder.clone())))
        .map_commit(Arc::new(RecordingMapCommit(recorder.clone())))
        .filtered_restore(Arc::new(RecordingRestore(recorder.clone())))
        .prologue(Arc::new(RecordingPrologue(recorder.clone())))
        .marker_inheritance(Arc::new(RecordingInheritance(recorder.clone())))
        .parking(Arc::new(RecordingParking(recorder.clone())))
        .build()
        .expect("complete adapter")
}

#[tokio::test]
async fn adapter_delegates_split_steps_to_injected_operations_in_order() {
    let store = MemoryStore::default();
    let recorder = Recorder::default();
    let hooks = fully_wired_adapter(&recorder);

    let state = run_split("recorded-split", split_command(), &store, &hooks)
        .await
        .expect("split completes");

    assert!(state.next_step == SplitStep::Complete);
    assert!(
        recorder.events()
            == vec![
                Event::Checkpoint,
                Event::Pause,
                Event::Commit,
                Event::Restore,
                Event::Prologue,
                Event::Inherit,
                Event::Park,
                Event::Unpause,
            ]
    );
}

#[tokio::test]
async fn unavailable_operation_fails_at_its_step_without_advancing_persisted_state() {
    let store = MemoryStore::default();
    let recorder = Recorder::default();
    let hooks = SplitHookAdapterBuilder::new()
        .checkpoint(Arc::new(RecordingCheckpoint(recorder.clone())))
        .write_gate(Arc::new(RecordingWriteGate(recorder.clone())))
        .map_commit(Arc::new(RecordingMapCommit(recorder.clone())))
        .prologue(Arc::new(RecordingPrologue(recorder.clone())))
        .marker_inheritance(Arc::new(RecordingInheritance(recorder.clone())))
        .parking(Arc::new(RecordingParking(recorder.clone())))
        .build_fail_clear();

    let error = run_split("missing-restore", split_command(), &store, &hooks)
        .await
        .expect_err("unwired restore must fail");

    assert!(matches!(
        error,
        SplitError::UnavailableHookOperation {
            operation: SplitHookOperation::SuccessorRestore,
        }
    ));
    let stored = store
        .load_split_state("missing-restore")
        .await
        .expect("load state")
        .expect("saved state");
    assert!(stored.next_step == SplitStep::StartSuccessorRestore);
    assert!(recorder.events() == vec![Event::Checkpoint, Event::Pause, Event::Commit]);
}

fn split_command() -> SplitCommand {
    SplitCommand {
        current_map: RangeMap::new(
            TenantName::parse("adapter-test").expect("tenant"),
            MapEpoch::new(1),
            vec![
                RangeSpec::new(RangeId::COORDINATOR, TableId::ZERO, Some(TableId::new(10))),
                RangeSpec::new(RangeId::new(1), TableId::new(10), Some(TableId::new(30))),
                RangeSpec::new(RangeId::new(3), TableId::new(30), None),
            ],
        )
        .expect("range map"),
        predecessor: RangeId::new(1),
        successor: RangeId::new(2),
        split_at: RangeKey::table_start(TableId::new(20)),
    }
}
