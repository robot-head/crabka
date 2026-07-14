use std::time::Duration;

use stateright::{Checker, Model, Property};

const MAX_DEPTH: usize = 18;
const MAX_STATES: usize = 500_000;
const CHECK_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Frame {
    generation: u8,
    offset: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Checkpoint {
    generation: u8,
    covered: u8,
    epoch: u8,
    state: u8,
    manifest_present: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CheckpointPhase {
    Idle,
    Snapshotted {
        generation: u8,
        covered: u8,
        state: u8,
        epoch: u8,
    },
    PartsUploaded {
        generation: u8,
        covered: u8,
        state: u8,
        epoch: u8,
    },
    ManifestWritten {
        generation: u8,
        covered: u8,
        state: u8,
        epoch: u8,
    },
    Truncated {
        generation: u8,
        covered: u8,
        state: u8,
        epoch: u8,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Compute {
    epoch: u8,
    applied_prefix: u8,
    lifecycle: ComputeLifecycle,
    checkpoint: CheckpointPhase,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ComputeLifecycle {
    Serving,
    Recovering,
    Crashed,
    Refused,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct State {
    current_generation: u8,
    current_epoch: u8,
    log_start: u8,
    journal: Vec<Frame>,
    checkpoints: Vec<Checkpoint>,
    computes: Vec<Compute>,
    restored_value: Option<u8>,
    refused_recovery: bool,
    injected_manifest_loss: bool,
    manifest_loss_generation: Option<u8>,
    last_replay_start: Option<u8>,
    last_restored_generation: Option<u8>,
}

struct CheckpointModel {
    preserves_manifest_before_truncate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Action {
    Append,
    StartCheckpoint(usize),
    StepCheckpoint(usize),
    ZombieCheckpointStep(usize),
    PruneStep,
    Crash(usize),
    StartSuccessor,
    ParkAndRecreateWal,
    LoseNewestManifestAfterTruncate,
    RecoverStep(usize),
}

impl Model for CheckpointModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            current_generation: 0,
            current_epoch: 0,
            log_start: 0,
            journal: Vec::new(),
            checkpoints: Vec::new(),
            computes: vec![
                Compute {
                    epoch: 0,
                    applied_prefix: 0,
                    lifecycle: ComputeLifecycle::Serving,
                    checkpoint: CheckpointPhase::Idle,
                },
                Compute {
                    epoch: 0,
                    applied_prefix: 0,
                    lifecycle: ComputeLifecycle::Crashed,
                    checkpoint: CheckpointPhase::Idle,
                },
                Compute {
                    epoch: 0,
                    applied_prefix: 0,
                    lifecycle: ComputeLifecycle::Crashed,
                    checkpoint: CheckpointPhase::Idle,
                },
            ],
            restored_value: None,
            refused_recovery: false,
            injected_manifest_loss: false,
            manifest_loss_generation: None,
            last_replay_start: None,
            last_restored_generation: None,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if frames_in_generation(state, state.current_generation) < 2 {
            actions.push(Action::Append);
        }
        if state.current_epoch < 2 {
            actions.push(Action::StartSuccessor);
        }
        if state.current_generation < 1 {
            actions.push(Action::ParkAndRecreateWal);
        }
        if state.checkpoints.len() < 2 {
            for index in 0..state.computes.len() {
                actions.push(Action::StartCheckpoint(index));
                actions.push(Action::StepCheckpoint(index));
                actions.push(Action::ZombieCheckpointStep(index));
            }
        }
        for index in 0..state.computes.len() {
            actions.push(Action::Crash(index));
            actions.push(Action::RecoverStep(index));
        }
        actions.push(Action::PruneStep);
        actions.push(Action::LoseNewestManifestAfterTruncate);
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            Action::Append => append_frame(&mut state),
            Action::StartCheckpoint(index) => start_checkpoint(&mut state, index)?,
            Action::StepCheckpoint(index) => {
                step_checkpoint(&mut state, index, self.preserves_manifest_before_truncate)?;
            }
            Action::ZombieCheckpointStep(index) => {
                zombie_checkpoint_step(&mut state, index, self.preserves_manifest_before_truncate)?;
            }
            Action::PruneStep => prune_step(&mut state)?,
            Action::Crash(index) => crash_compute(&mut state, index)?,
            Action::StartSuccessor => start_successor(&mut state)?,
            Action::ParkAndRecreateWal => park_and_recreate_wal(&mut state)?,
            Action::LoseNewestManifestAfterTruncate => lose_newest_manifest(&mut state)?,
            Action::RecoverStep(index) => recover(&mut state, index)?,
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "serving_matches_acked_journal",
                |_: &Self, state: &State| {
                    state
                        .computes
                        .iter()
                        .filter(|compute| matches!(compute.lifecycle, ComputeLifecycle::Serving))
                        .all(|compute| usize::from(compute.applied_prefix) == state.journal.len())
                },
            ),
            Property::always(
                "a safe recovery path remains absent injected loss",
                |_: &Self, state: &State| {
                    state.manifest_loss_generation == Some(state.current_generation)
                        || !matches!(recovery_result(state), RecoveryResult::Refuse)
                },
            ),
            Property::always(
                "torn manifest loss never remains serving",
                |_: &Self, state: &State| {
                    state.manifest_loss_generation != Some(state.current_generation)
                        || state
                            .computes
                            .iter()
                            .all(|compute| !matches!(compute.lifecycle, ComputeLifecycle::Serving))
                },
            ),
            Property::always(
                "fresh generation recovery starts at offset zero",
                |_: &Self, state: &State| {
                    !matches!(
                        (state.last_restored_generation, state.last_replay_start),
                        (Some(restored), Some(start))
                            if restored < state.current_generation && start != 0
                    )
                },
            ),
            Property::always(
                "no_torn_truncation_without_manifest_loss",
                |_: &Self, state: &State| {
                    state.injected_manifest_loss || log_start_is_manifest_covered(state)
                },
            ),
            Property::always(
                "manifest_loss_refuses_instead_of_corrupting",
                |_: &Self, state: &State| {
                    !state.injected_manifest_loss
                        || state.restored_value.is_none()
                        || state.restored_value == Some(reference_state(state))
                },
            ),
            Property::sometimes(
                "newest_manifest_recovery_serves",
                |_: &Self, state: &State| {
                    state.restored_value == Some(reference_state(state)) && !state.refused_recovery
                },
            ),
            Property::sometimes("torn_manifest_loss_refuses", |_: &Self, state: &State| {
                state.injected_manifest_loss && state.refused_recovery
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.journal.len() <= 4
            && state.checkpoints.len() <= 2
            && state.current_epoch <= 2
            && state.current_generation <= 1
    }
}

fn append_frame(state: &mut State) {
    let Some(active_index) = state.computes.iter().position(|compute| {
        compute.epoch == state.current_epoch
            && matches!(compute.lifecycle, ComputeLifecycle::Serving)
    }) else {
        return;
    };
    let offset = frames_in_generation(state, state.current_generation);
    state.journal.push(Frame {
        generation: state.current_generation,
        offset,
    });
    state.computes[active_index].applied_prefix = state.computes[active_index]
        .applied_prefix
        .saturating_add(1);
    state.restored_value = None;
}

fn start_checkpoint(state: &mut State, index: usize) -> Option<()> {
    if !matches!(state.computes[index].checkpoint, CheckpointPhase::Idle)
        || !matches!(state.computes[index].lifecycle, ComputeLifecycle::Serving)
    {
        return None;
    }
    let covered = newest_offset(state, state.current_generation)?;
    state.computes[index].checkpoint = CheckpointPhase::Snapshotted {
        generation: state.current_generation,
        covered,
        state: state_at_checkpoint(state, state.current_generation, covered),
        epoch: state.current_epoch,
    };
    Some(())
}

fn step_checkpoint(state: &mut State, index: usize, ordered: bool) -> Option<()> {
    let next = match state.computes[index].checkpoint.clone() {
        CheckpointPhase::Idle => return None,
        CheckpointPhase::Snapshotted {
            generation,
            covered,
            state,
            epoch,
        } => CheckpointPhase::PartsUploaded {
            generation,
            covered,
            state,
            epoch,
        },
        CheckpointPhase::PartsUploaded {
            generation,
            covered,
            state: snap_state,
            epoch,
        } => {
            upsert_checkpoint(
                state,
                Checkpoint {
                    generation,
                    covered,
                    epoch,
                    state: snap_state,
                    manifest_present: true,
                },
            );
            CheckpointPhase::ManifestWritten {
                generation,
                covered,
                state: snap_state,
                epoch,
            }
        }
        CheckpointPhase::ManifestWritten {
            generation,
            covered,
            state: snap_state,
            epoch,
        } => {
            if !ordered {
                state.checkpoints.retain(|checkpoint| {
                    checkpoint.generation != generation || checkpoint.covered != covered
                });
            }
            if generation == state.current_generation && epoch == state.current_epoch {
                state.log_start = state.log_start.max(covered.saturating_add(1));
            }
            CheckpointPhase::Truncated {
                generation,
                covered,
                state: snap_state,
                epoch,
            }
        }
        CheckpointPhase::Truncated { .. } => CheckpointPhase::Idle,
    };
    state.computes[index].checkpoint = next;
    Some(())
}

fn crash_compute(state: &mut State, index: usize) -> Option<()> {
    if matches!(state.computes[index].lifecycle, ComputeLifecycle::Crashed) {
        return None;
    }
    state.computes[index].lifecycle = ComputeLifecycle::Crashed;
    Some(())
}

fn zombie_checkpoint_step(state: &mut State, index: usize, ordered: bool) -> Option<()> {
    if state.computes[index].epoch >= state.current_epoch
        || matches!(state.computes[index].checkpoint, CheckpointPhase::Idle)
    {
        return None;
    }
    step_checkpoint(state, index, ordered)
}

fn prune_step(state: &mut State) -> Option<()> {
    if state.checkpoints.len() <= 1 {
        return None;
    }
    state
        .checkpoints
        .sort_by_key(|checkpoint| (checkpoint.generation, checkpoint.covered, checkpoint.epoch));
    state.checkpoints.remove(0);
    Some(())
}

fn start_successor(state: &mut State) -> Option<()> {
    for compute in &mut state.computes {
        if matches!(compute.lifecycle, ComputeLifecycle::Serving) {
            compute.lifecycle = ComputeLifecycle::Crashed;
        }
    }
    state.current_epoch = state.current_epoch.checked_add(1)?;
    let successor = state
        .computes
        .iter_mut()
        .find(|compute| matches!(compute.lifecycle, ComputeLifecycle::Crashed))?;
    successor.epoch = state.current_epoch;
    successor.lifecycle = ComputeLifecycle::Recovering;
    successor.checkpoint = CheckpointPhase::Idle;
    state.restored_value = None;
    Some(())
}

fn park_and_recreate_wal(state: &mut State) -> Option<()> {
    let newest_offset = newest_offset(state, state.current_generation)?;
    let checkpoint = newest_manifest(state)?;
    if checkpoint.generation != state.current_generation || checkpoint.covered < newest_offset {
        return None;
    }
    for compute in &mut state.computes {
        if matches!(
            compute.lifecycle,
            ComputeLifecycle::Serving | ComputeLifecycle::Recovering
        ) {
            compute.lifecycle = ComputeLifecycle::Crashed;
        }
    }
    state.current_generation = state.current_generation.checked_add(1)?;
    state.log_start = 0;
    state.last_replay_start = None;
    state.last_restored_generation = None;
    state.current_epoch = state.current_epoch.checked_add(1)?;
    let successor = state
        .computes
        .iter_mut()
        .find(|compute| matches!(compute.lifecycle, ComputeLifecycle::Crashed))?;
    successor.epoch = state.current_epoch;
    successor.lifecycle = ComputeLifecycle::Recovering;
    successor.checkpoint = CheckpointPhase::Idle;
    state.restored_value = None;
    Some(())
}

fn lose_newest_manifest(state: &mut State) -> Option<()> {
    let newest_index = newest_manifest_index(state)?;
    let checkpoint = &state.checkpoints[newest_index];
    if checkpoint.generation != state.current_generation {
        return None;
    }
    if state.log_start < checkpoint.covered.saturating_add(1) {
        return None;
    }
    if state
        .checkpoints
        .iter()
        .enumerate()
        .any(|(index, candidate)| {
            index != newest_index
                && candidate.manifest_present
                && candidate.generation == state.current_generation
                && state.log_start <= candidate.covered.saturating_add(1)
        })
    {
        return None;
    }
    state.checkpoints[newest_index].manifest_present = false;
    state.injected_manifest_loss = true;
    state.manifest_loss_generation = Some(state.current_generation);
    state.restored_value = None;
    for compute in &mut state.computes {
        if matches!(compute.lifecycle, ComputeLifecycle::Serving) {
            compute.lifecycle = ComputeLifecycle::Crashed;
        }
    }
    Some(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryResult {
    ServeFromStart,
    ServeFromCheckpoint { generation: u8, replay_start: u8 },
    Refuse,
}

fn recovery_result(state: &State) -> RecoveryResult {
    let Some(checkpoint) = newest_manifest(state) else {
        return if state.log_start > 0 {
            RecoveryResult::Refuse
        } else {
            RecoveryResult::ServeFromStart
        };
    };
    if checkpoint.generation == state.current_generation
        && state.log_start > checkpoint.covered.saturating_add(1)
    {
        return RecoveryResult::Refuse;
    }
    RecoveryResult::ServeFromCheckpoint {
        generation: checkpoint.generation,
        replay_start: if checkpoint.generation == state.current_generation {
            checkpoint.covered.saturating_add(1)
        } else {
            0
        },
    }
}

fn recover(state: &mut State, index: usize) -> Option<()> {
    if !matches!(
        state.computes[index].lifecycle,
        ComputeLifecycle::Recovering
    ) {
        return None;
    }
    if state.computes[index].epoch != state.current_epoch {
        state.computes[index].lifecycle = ComputeLifecycle::Crashed;
        return Some(());
    }
    match recovery_result(state) {
        RecoveryResult::Refuse => {
            state.refused_recovery = true;
            state.restored_value = None;
            state.computes[index].lifecycle = ComputeLifecycle::Refused;
            state.last_replay_start = None;
            state.last_restored_generation = None;
            Some(())
        }
        RecoveryResult::ServeFromStart => {
            let recovered = reference_state(state);
            state.refused_recovery = false;
            state.restored_value = Some(recovered);
            state.computes[index].applied_prefix = recovered;
            state.computes[index].lifecycle = ComputeLifecycle::Serving;
            state.last_replay_start = Some(0);
            state.last_restored_generation = None;
            Some(())
        }
        RecoveryResult::ServeFromCheckpoint {
            generation,
            replay_start,
        } => {
            let checkpoint = newest_manifest(state)
                .cloned()
                .expect("classified checkpoint");
            let recovered = recovered_state(state, &checkpoint);
            state.refused_recovery = false;
            state.restored_value = Some(recovered);
            state.computes[index].applied_prefix = recovered;
            state.computes[index].lifecycle = ComputeLifecycle::Serving;
            state.last_replay_start = Some(replay_start);
            state.last_restored_generation = Some(generation);
            Some(())
        }
    }
}

fn upsert_checkpoint(state: &mut State, checkpoint: Checkpoint) {
    let repairs_loss = checkpoint.generation == state.current_generation
        && state.log_start <= checkpoint.covered.saturating_add(1);
    if let Some(existing) = state.checkpoints.iter_mut().find(|existing| {
        existing.generation == checkpoint.generation
            && existing.covered == checkpoint.covered
            && existing.epoch == checkpoint.epoch
    }) {
        *existing = checkpoint;
        if repairs_loss {
            state.manifest_loss_generation = None;
        }
        return;
    }
    state.checkpoints.push(checkpoint);
    if repairs_loss {
        state.manifest_loss_generation = None;
    }
}

fn recovered_state(state: &State, checkpoint: &Checkpoint) -> u8 {
    let replay_start = if checkpoint.generation == state.current_generation {
        checkpoint.covered.saturating_add(1)
    } else {
        0
    };
    checkpoint.state
        + u8::try_from(
            state
                .journal
                .iter()
                .filter(|frame| {
                    frame.generation == state.current_generation && frame.offset >= replay_start
                })
                .count(),
        )
        .expect("bounded replay count")
}

fn state_at_checkpoint(state: &State, generation: u8, covered: u8) -> u8 {
    u8::try_from(
        state
            .journal
            .iter()
            .filter(|frame| frame.generation < generation)
            .count()
            + state
                .journal
                .iter()
                .filter(|frame| frame.generation == generation && frame.offset <= covered)
                .count(),
    )
    .expect("bounded checkpoint state")
}

fn reference_state(state: &State) -> u8 {
    u8::try_from(state.journal.len()).expect("bounded journal")
}

fn log_start_is_manifest_covered(state: &State) -> bool {
    if state.log_start == 0 {
        return true;
    }
    newest_manifest(state)
        .filter(|checkpoint| checkpoint.generation == state.current_generation)
        .is_some_and(|checkpoint| state.log_start <= checkpoint.covered.saturating_add(1))
}

fn newest_manifest(state: &State) -> Option<&Checkpoint> {
    state
        .checkpoints
        .iter()
        .filter(|checkpoint| checkpoint.manifest_present)
        .max_by_key(|checkpoint| (checkpoint.generation, checkpoint.covered, checkpoint.epoch))
}

fn newest_manifest_index(state: &State) -> Option<usize> {
    state
        .checkpoints
        .iter()
        .enumerate()
        .filter(|(_, checkpoint)| checkpoint.manifest_present)
        .max_by_key(|(_, checkpoint)| (checkpoint.generation, checkpoint.covered, checkpoint.epoch))
        .map(|(index, _)| index)
}

fn newest_offset(state: &State, generation: u8) -> Option<u8> {
    state
        .journal
        .iter()
        .filter(|frame| frame.generation == generation)
        .map(|frame| frame.offset)
        .max()
}

fn frames_in_generation(state: &State, generation: u8) -> u8 {
    u8::try_from(
        state
            .journal
            .iter()
            .filter(|frame| frame.generation == generation)
            .count(),
    )
    .expect("bounded generation")
}

#[test]
fn checkpoint_wal_recovery_protocol_is_safe() {
    let checker = CheckpointModel {
        preserves_manifest_before_truncate: true,
    }
    .checker()
    .target_max_depth(MAX_DEPTH)
    .target_state_count(MAX_STATES)
    .timeout(CHECK_TIMEOUT)
    .spawn_bfs()
    .join();
    eprintln!(
        "[checkpoint_model] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    checker.assert_properties();
}

#[test]
fn broken_manifest_before_truncate_order_has_a_counterexample() {
    let checker = CheckpointModel {
        preserves_manifest_before_truncate: false,
    }
    .checker()
    .target_max_depth(10)
    .target_state_count(100_000)
    .timeout(CHECK_TIMEOUT)
    .spawn_bfs()
    .join();
    assert!(
        checker
            .discoveries()
            .contains_key("no_torn_truncation_without_manifest_loss")
    );
}
