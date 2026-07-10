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
    phase: CheckpointPhase,
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
}

struct CheckpointModel;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Action {
    Append,
    StartCheckpoint(usize),
    StepCheckpoint(usize),
    CrashCheckpoint(usize),
    StartSuccessor,
    ParkAndRecreateWal,
    LoseNewestManifestAfterTruncate,
    Recover,
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
                    phase: CheckpointPhase::Idle,
                },
                Compute {
                    phase: CheckpointPhase::Idle,
                },
            ],
            restored_value: None,
            refused_recovery: false,
            injected_manifest_loss: false,
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
                actions.push(Action::CrashCheckpoint(index));
            }
        }
        actions.push(Action::LoseNewestManifestAfterTruncate);
        actions.push(Action::Recover);
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            Action::Append => append_frame(&mut state),
            Action::StartCheckpoint(index) => start_checkpoint(&mut state, index)?,
            Action::StepCheckpoint(index) => step_checkpoint(&mut state, index)?,
            Action::CrashCheckpoint(index) => crash_checkpoint(&mut state, index)?,
            Action::StartSuccessor => start_successor(&mut state)?,
            Action::ParkAndRecreateWal => park_and_recreate_wal(&mut state)?,
            Action::LoseNewestManifestAfterTruncate => lose_newest_manifest(&mut state)?,
            Action::Recover => recover(&mut state),
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "serving_matches_acked_journal",
                |_: &Self, state: &State| {
                    state
                        .restored_value
                        .is_none_or(|serving| serving == reference_state(state))
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
    let offset = frames_in_generation(state, state.current_generation);
    state.journal.push(Frame {
        generation: state.current_generation,
        offset,
    });
    state.restored_value = None;
}

fn start_checkpoint(state: &mut State, index: usize) -> Option<()> {
    if !matches!(state.computes[index].phase, CheckpointPhase::Idle) {
        return None;
    }
    let covered = newest_offset(state, state.current_generation)?;
    state.computes[index].phase = CheckpointPhase::Snapshotted {
        generation: state.current_generation,
        covered,
        state: state_at_checkpoint(state, state.current_generation, covered),
        epoch: state.current_epoch,
    };
    Some(())
}

fn step_checkpoint(state: &mut State, index: usize) -> Option<()> {
    let next = match state.computes[index].phase.clone() {
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
            if generation == state.current_generation {
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
    state.computes[index].phase = next;
    Some(())
}

fn crash_checkpoint(state: &mut State, index: usize) -> Option<()> {
    if matches!(state.computes[index].phase, CheckpointPhase::Idle) {
        return None;
    }
    state.computes[index].phase = CheckpointPhase::Idle;
    Some(())
}

fn start_successor(state: &mut State) -> Option<()> {
    state.current_epoch = state.current_epoch.checked_add(1)?;
    state.restored_value = None;
    Some(())
}

fn park_and_recreate_wal(state: &mut State) -> Option<()> {
    let newest_offset = newest_offset(state, state.current_generation)?;
    let checkpoint = newest_manifest(state)?;
    if checkpoint.generation != state.current_generation || checkpoint.covered < newest_offset {
        return None;
    }
    state.current_generation = state.current_generation.checked_add(1)?;
    state.log_start = 0;
    state.current_epoch = state.current_epoch.checked_add(1)?;
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
    state.checkpoints[newest_index].manifest_present = false;
    state.injected_manifest_loss = true;
    state.restored_value = None;
    Some(())
}

fn recover(state: &mut State) {
    let Some(checkpoint) = newest_manifest(state).cloned() else {
        if state.log_start > 0 {
            state.refused_recovery = true;
            state.restored_value = None;
            return;
        }
        state.refused_recovery = false;
        state.restored_value = Some(reference_state(state));
        return;
    };
    if checkpoint.generation == state.current_generation
        && state.log_start > checkpoint.covered.saturating_add(1)
    {
        state.refused_recovery = true;
        state.restored_value = None;
        return;
    }
    state.refused_recovery = false;
    state.restored_value = Some(recovered_state(state, &checkpoint));
}

fn upsert_checkpoint(state: &mut State, checkpoint: Checkpoint) {
    if let Some(existing) = state.checkpoints.iter_mut().find(|existing| {
        existing.generation == checkpoint.generation
            && existing.covered == checkpoint.covered
            && existing.epoch == checkpoint.epoch
    }) {
        *existing = checkpoint;
        return;
    }
    state.checkpoints.push(checkpoint);
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
    let checker = CheckpointModel
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
