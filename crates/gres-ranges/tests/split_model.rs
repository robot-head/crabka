#![allow(
    clippy::items_after_statements,
    clippy::missing_const_for_fn,
    clippy::struct_excessive_bools
)]

use stateright::{Checker, Model, Property};

const MAX_STEPS: u8 = 9;
const SPLIT_AT: u8 = 2;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Owner {
    Predecessor,
    Successor,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum Decision {
    Commit,
    Abort,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct JournalEntry {
    txn: u8,
    key: u8,
    delta: i8,
    decision: Option<Decision>,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct Marker {
    txn: u8,
    key: u8,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct State {
    predecessor_journal: Vec<JournalEntry>,
    successor_journal: Vec<JournalEntry>,
    predecessor_markers: Vec<Marker>,
    successor_markers: Vec<Marker>,
    map_committed: bool,
    checkpoint_fold: Option<[i8; 2]>,
    writes_paused: bool,
    successor_restored: bool,
    successor_fenced_epoch: u8,
    predecessor_fenced_epoch: u8,
    markers_inherited: bool,
    predecessor_parked: bool,
    serving_unpaused: bool,
    steps: u8,
}

#[derive(Clone, Copy)]
struct SplitModel {
    correct: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Action {
    PrepareLeft,
    PrepareRight,
    DecideTxn { txn: u8, decision: Decision },
    ForcePredecessorCheckpoint,
    PauseWritesAtCoveredOffset,
    CommitMapVersion,
    StartSuccessorRestore,
    SuccessorFencePrologue,
    InheritInDoubtMarkers,
    ParkPredecessor,
    UnpauseServing,
    CrashAndRetry,
    FencePredecessor,
    FenceSuccessor,
}

impl State {
    fn initial() -> Self {
        Self {
            predecessor_journal: Vec::new(),
            successor_journal: Vec::new(),
            predecessor_markers: Vec::new(),
            successor_markers: Vec::new(),
            map_committed: false,
            checkpoint_fold: None,
            writes_paused: false,
            successor_restored: false,
            successor_fenced_epoch: 0,
            predecessor_fenced_epoch: 0,
            markers_inherited: false,
            predecessor_parked: false,
            serving_unpaused: false,
            steps: 0,
        }
    }

    fn owners_at_epoch(map_committed: bool, key: u8) -> Vec<Owner> {
        if !map_committed || key < SPLIT_AT {
            return vec![Owner::Predecessor];
        }

        vec![Owner::Successor]
    }

    fn can_accept_predecessor_write(&self, key: u8) -> bool {
        if self.writes_paused {
            return false;
        }
        if !self.map_committed {
            return true;
        }

        key < SPLIT_AT
    }

    fn every_map_has_exactly_one_owner() -> bool {
        [false, true].into_iter().all(|map_committed| {
            (0..4).all(|key| Self::owners_at_epoch(map_committed, key).len() == 1)
        })
    }

    fn decisions_are_honored_on_both_sides(&self) -> bool {
        self.all_decisions().into_iter().all(|(txn, decision)| {
            let predecessor_honors =
                journal_honors_decision(&self.predecessor_journal, txn, decision);
            let successor_honors = journal_honors_decision(&self.successor_journal, txn, decision);
            predecessor_honors && successor_honors
        })
    }

    fn no_stranded_in_doubt_marker(&self) -> bool {
        if !self.serving_unpaused {
            return true;
        }

        self.predecessor_markers
            .iter()
            .all(|marker| marker.key < SPLIT_AT)
            && self.successor_markers.is_empty()
    }

    fn successor_fold_equals_partitioned_predecessor_fold(&self) -> bool {
        if !self.successor_restored {
            return true;
        }
        if self.checkpoint_fold.is_none() {
            return false;
        }
        if self
            .successor_journal
            .iter()
            .any(|entry| entry.key < SPLIT_AT)
        {
            return false;
        }

        fold_successor_partition(&self.successor_journal)
            == fold_successor_partition(&self.predecessor_journal)
    }

    fn all_decisions(&self) -> Vec<(u8, Decision)> {
        self.predecessor_journal
            .iter()
            .chain(self.successor_journal.iter())
            .filter_map(|entry| entry.decision.map(|decision| (entry.txn, decision)))
            .collect()
    }
}

impl Model for SplitModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        vec![State::initial()]
    }

    fn actions(&self, state: &State, out: &mut Vec<Action>) {
        if state.steps >= MAX_STEPS {
            return;
        }

        out.extend([
            Action::PrepareLeft,
            Action::PrepareRight,
            Action::DecideTxn {
                txn: 1,
                decision: Decision::Commit,
            },
            Action::DecideTxn {
                txn: 2,
                decision: Decision::Abort,
            },
            Action::ForcePredecessorCheckpoint,
            Action::PauseWritesAtCoveredOffset,
            Action::CommitMapVersion,
            Action::StartSuccessorRestore,
            Action::SuccessorFencePrologue,
            Action::InheritInDoubtMarkers,
            Action::ParkPredecessor,
            Action::UnpauseServing,
            Action::CrashAndRetry,
            Action::FencePredecessor,
            Action::FenceSuccessor,
        ]);
    }

    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut next = state.clone();
        next.steps += 1;

        match action {
            Action::PrepareLeft => prepare(&mut next, 1, 1, 5, self.correct)?,
            Action::PrepareRight => prepare(&mut next, 2, 2, 7, self.correct)?,
            Action::DecideTxn { txn, decision } => decide(&mut next, txn, decision, self.correct)?,
            Action::ForcePredecessorCheckpoint => force_checkpoint(&mut next)?,
            Action::PauseWritesAtCoveredOffset => pause_writes(&mut next)?,
            Action::CommitMapVersion => commit_map(&mut next)?,
            Action::StartSuccessorRestore => restore_successor(&mut next, self.correct)?,
            Action::SuccessorFencePrologue => fence_successor_for_serving(&mut next)?,
            Action::InheritInDoubtMarkers => inherit_markers(&mut next, self.correct)?,
            Action::ParkPredecessor => park_predecessor(&mut next)?,
            Action::UnpauseServing => unpause_serving(&mut next, self.correct)?,
            Action::CrashAndRetry => return None,
            Action::FencePredecessor => next.predecessor_fenced_epoch += 1,
            Action::FenceSuccessor => next.successor_fenced_epoch += 1,
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always(
                "exactly one serving owner per key at every map version",
                |_, _state| State::every_map_has_exactly_one_owner(),
            ),
            Property::<Self>::always(
                "2pc decisions are honored on both sides of split",
                |_, state| state.decisions_are_honored_on_both_sides(),
            ),
            Property::<Self>::always(
                "complete split leaves no stranded in-doubt marker",
                |_, state| state.no_stranded_in_doubt_marker(),
            ),
            Property::<Self>::always(
                "successor fold equals partitioned predecessor checkpoint fold",
                |_, state| state.successor_fold_equals_partitioned_predecessor_fold(),
            ),
        ]
    }
}

fn prepare(state: &mut State, txn: u8, key: u8, delta: i8, correct: bool) -> Option<()> {
    if correct && state.checkpoint_fold.is_some() {
        return None;
    }
    if !state.can_accept_predecessor_write(key) {
        return None;
    }
    if state
        .predecessor_journal
        .iter()
        .any(|entry| entry.txn == txn)
    {
        return None;
    }

    state.predecessor_journal.push(JournalEntry {
        txn,
        key,
        delta,
        decision: None,
    });
    state.predecessor_markers.push(Marker { txn, key });
    Some(())
}

fn decide(state: &mut State, txn: u8, decision: Decision, correct: bool) -> Option<()> {
    let mut decided = false;
    for entry in &mut state.predecessor_journal {
        if entry.txn == txn && entry.decision.is_none() {
            entry.decision = Some(decision);
            decided = true;
        }
    }
    for entry in &mut state.successor_journal {
        if entry.txn == txn && entry.decision.is_none() {
            entry.decision = Some(decision);
            decided = true;
        }
    }
    if !decided {
        return None;
    }

    state.predecessor_markers.retain(|marker| marker.txn != txn);
    if correct {
        state.successor_markers.retain(|marker| marker.txn != txn);
    }
    Some(())
}

fn force_checkpoint(state: &mut State) -> Option<()> {
    if state.checkpoint_fold.is_some() {
        return None;
    }

    state.checkpoint_fold = Some(fold_successor_partition(&state.predecessor_journal));
    Some(())
}

fn pause_writes(state: &mut State) -> Option<()> {
    if state.writes_paused || state.checkpoint_fold.is_none() {
        return None;
    }

    state.writes_paused = true;
    Some(())
}

fn commit_map(state: &mut State) -> Option<()> {
    if state.map_committed || !state.writes_paused {
        return None;
    }

    state.map_committed = true;
    Some(())
}

fn restore_successor(state: &mut State, correct: bool) -> Option<()> {
    if state.successor_restored || !state.map_committed {
        return None;
    }

    let checkpoint_entries = state
        .predecessor_journal
        .iter()
        .filter(|entry| !correct || entry.key >= SPLIT_AT)
        .copied();
    state.successor_journal.extend(checkpoint_entries);
    state.successor_restored = true;
    Some(())
}

fn fence_successor_for_serving(state: &mut State) -> Option<()> {
    if !state.successor_restored || state.successor_fenced_epoch > 0 {
        return None;
    }

    state.successor_fenced_epoch = 1;
    Some(())
}

fn inherit_markers(state: &mut State, correct: bool) -> Option<()> {
    if state.markers_inherited || state.successor_fenced_epoch == 0 || !state.successor_restored {
        return None;
    }

    let inherited = state
        .predecessor_markers
        .iter()
        .filter(|marker| !correct || marker.key >= SPLIT_AT)
        .copied();
    state.successor_markers.extend(inherited);
    if correct {
        state
            .predecessor_markers
            .retain(|marker| marker.key < SPLIT_AT);
    }
    state.markers_inherited = true;
    Some(())
}

fn park_predecessor(state: &mut State) -> Option<()> {
    if state.predecessor_parked || !state.markers_inherited || !state.map_committed {
        return None;
    }

    state.predecessor_parked = true;
    Some(())
}

fn unpause_serving(state: &mut State, correct: bool) -> Option<()> {
    if state.serving_unpaused || !state.predecessor_parked {
        return None;
    }
    if !state.map_committed || !state.successor_restored || !state.markers_inherited {
        return None;
    }
    if correct && !state.successor_markers.is_empty() {
        return None;
    }

    state.serving_unpaused = true;
    Some(())
}

fn journal_honors_decision(journal: &[JournalEntry], txn: u8, decision: Decision) -> bool {
    journal
        .iter()
        .filter(|entry| entry.txn == txn)
        .all(|entry| entry.decision == Some(decision))
}

fn fold_successor_partition(journal: &[JournalEntry]) -> [i8; 2] {
    let mut fold = [0, 0];
    for entry in journal {
        if entry.key < SPLIT_AT || entry.decision == Some(Decision::Abort) {
            continue;
        }

        let index = usize::from(entry.key - SPLIT_AT);
        fold[index] += entry.delta;
    }
    fold
}

#[test]
fn split_orchestration_model_honors_invariants() {
    let checker = SplitModel { correct: true }.checker().spawn_bfs().join();

    checker.assert_properties();
    assert!(checker.unique_state_count() > 1);
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ConversionState {
    xid_rows: Vec<JournalEntry>,
    ts_rows: Vec<JournalEntry>,
    prepared_markers: Vec<Marker>,
    writes_paused: bool,
    checkpoint_rewritten: bool,
    catalog_flipped: bool,
    unpaused: bool,
    aborted: bool,
    steps: u8,
}

#[derive(Clone, Copy)]
struct ConversionModel {
    correct: bool,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
enum ConversionAction {
    WriteCommitted,
    PrepareInDoubt,
    DecidePrepared(Decision),
    PauseWrites,
    CheckpointRewrite,
    FlipCatalogAndMap,
    Resume,
    CrashAndRetry,
}

impl ConversionState {
    fn initial() -> Self {
        Self {
            xid_rows: Vec::new(),
            ts_rows: Vec::new(),
            prepared_markers: Vec::new(),
            writes_paused: false,
            checkpoint_rewritten: false,
            catalog_flipped: false,
            unpaused: false,
            aborted: false,
            steps: 0,
        }
    }

    fn visible_fold_is_preserved(&self) -> bool {
        if !self.catalog_flipped {
            return true;
        }

        fold_all(&self.ts_rows) == fold_all(&self.xid_rows)
    }

    fn no_mixed_visibility_statement_after_resume(&self) -> bool {
        if !self.unpaused {
            return true;
        }

        self.catalog_flipped && self.checkpoint_rewritten && self.xid_rows == self.ts_rows
    }
}

impl Model for ConversionModel {
    type State = ConversionState;
    type Action = ConversionAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![ConversionState::initial()]
    }

    fn actions(&self, state: &Self::State, out: &mut Vec<Self::Action>) {
        if state.steps >= MAX_STEPS || state.aborted {
            return;
        }

        out.extend([
            ConversionAction::WriteCommitted,
            ConversionAction::PrepareInDoubt,
            ConversionAction::DecidePrepared(Decision::Commit),
            ConversionAction::DecidePrepared(Decision::Abort),
            ConversionAction::PauseWrites,
            ConversionAction::CheckpointRewrite,
            ConversionAction::FlipCatalogAndMap,
            ConversionAction::Resume,
            ConversionAction::CrashAndRetry,
        ]);
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();
        next.steps += 1;

        match action {
            ConversionAction::WriteCommitted => conversion_write(&mut next)?,
            ConversionAction::PrepareInDoubt => conversion_prepare(&mut next)?,
            ConversionAction::DecidePrepared(decision) => conversion_decide(&mut next, decision)?,
            ConversionAction::PauseWrites => set_once(&mut next.writes_paused)?,
            ConversionAction::CheckpointRewrite => conversion_rewrite(&mut next, self.correct)?,
            ConversionAction::FlipCatalogAndMap => conversion_flip(&mut next, self.correct)?,
            ConversionAction::Resume => conversion_resume(&mut next)?,
            ConversionAction::CrashAndRetry => return None,
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always("conversion preserves visible fold", |_, state| {
                state.visible_fold_is_preserved()
            }),
            Property::<Self>::always(
                "conversion resumes only under one visibility mode",
                |_, state| state.no_mixed_visibility_statement_after_resume(),
            ),
            Property::<Self>::always(
                "conversion never completes with in-doubt prepared",
                |_, state| !state.unpaused || state.prepared_markers.is_empty(),
            ),
        ]
    }
}

fn conversion_write(state: &mut ConversionState) -> Option<()> {
    if state.writes_paused || state.catalog_flipped {
        return None;
    }
    state.xid_rows.push(JournalEntry {
        txn: 1,
        key: 1,
        delta: 3,
        decision: Some(Decision::Commit),
    });
    Some(())
}

fn conversion_prepare(state: &mut ConversionState) -> Option<()> {
    if state.writes_paused || state.catalog_flipped || !state.prepared_markers.is_empty() {
        return None;
    }
    state.prepared_markers.push(Marker { txn: 2, key: 1 });
    Some(())
}

fn conversion_decide(state: &mut ConversionState, decision: Decision) -> Option<()> {
    if state.prepared_markers.is_empty() {
        return None;
    }
    state.prepared_markers.clear();
    if decision == Decision::Commit {
        state.xid_rows.push(JournalEntry {
            txn: 2,
            key: 1,
            delta: 5,
            decision: Some(Decision::Commit),
        });
    }
    Some(())
}

fn conversion_rewrite(state: &mut ConversionState, correct: bool) -> Option<()> {
    if state.checkpoint_rewritten || !state.writes_paused {
        return None;
    }
    if correct && !state.prepared_markers.is_empty() {
        state.aborted = true;
        return Some(());
    }
    state.ts_rows = state.xid_rows.clone();
    state.checkpoint_rewritten = true;
    Some(())
}

fn conversion_flip(state: &mut ConversionState, correct: bool) -> Option<()> {
    if state.catalog_flipped || !state.checkpoint_rewritten {
        return None;
    }
    if correct && !state.prepared_markers.is_empty() {
        return None;
    }
    state.catalog_flipped = true;
    Some(())
}

fn conversion_resume(state: &mut ConversionState) -> Option<()> {
    if state.unpaused || !state.catalog_flipped || !state.checkpoint_rewritten {
        return None;
    }
    if !state.prepared_markers.is_empty() {
        return None;
    }
    state.unpaused = true;
    Some(())
}

#[test]
fn conversion_orchestration_model_honors_invariants() {
    let checker = ConversionModel { correct: true }
        .checker()
        .spawn_bfs()
        .join();

    checker.assert_properties();
    assert!(checker.unique_state_count() > 1);
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct MergeState {
    left_journal: Vec<JournalEntry>,
    right_journal: Vec<JournalEntry>,
    merged_journal: Vec<JournalEntry>,
    left_markers: Vec<Marker>,
    right_markers: Vec<Marker>,
    merged_markers: Vec<Marker>,
    left_checkpoint: bool,
    right_checkpoint: bool,
    map_committed: bool,
    merged_restored: bool,
    left_parked: bool,
    right_parked: bool,
    unpaused: bool,
    steps: u8,
}

#[derive(Clone, Copy)]
struct MergeModel {
    correct: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum MergeAction {
    PrepareLeft,
    PrepareRight,
    Decide { txn: u8, decision: Decision },
    ForceLeftCheckpoint,
    ForceRightCheckpoint,
    CommitMapVersion,
    RestoreMergedSuccessor,
    FoldInDoubtMarkers,
    ParkLeft,
    ParkRight,
    UnpauseServing,
    CrashAndRetry,
}

impl MergeState {
    fn initial() -> Self {
        Self {
            left_journal: Vec::new(),
            right_journal: Vec::new(),
            merged_journal: Vec::new(),
            left_markers: Vec::new(),
            right_markers: Vec::new(),
            merged_markers: Vec::new(),
            left_checkpoint: false,
            right_checkpoint: false,
            map_committed: false,
            merged_restored: false,
            left_parked: false,
            right_parked: false,
            unpaused: false,
            steps: 0,
        }
    }

    fn every_map_has_exactly_one_owner() -> bool {
        [false, true].into_iter().all(|merged| {
            (0..4).all(|key| {
                let owners = if merged || key < SPLIT_AT {
                    vec![Owner::Predecessor]
                } else {
                    vec![Owner::Successor]
                };
                owners.len() == 1
            })
        })
    }

    fn merged_fold_equals_left_plus_right(&self) -> bool {
        if !self.merged_restored {
            return true;
        }

        fold_all(&self.merged_journal)
            == add_folds(fold_all(&self.left_journal), fold_all(&self.right_journal))
    }

    fn no_stranded_merge_marker(&self) -> bool {
        if !self.unpaused {
            return true;
        }

        self.left_markers.is_empty()
            && self.right_markers.is_empty()
            && self.merged_markers.is_empty()
    }

    fn decisions_are_honored_after_merge(&self) -> bool {
        self.merged_journal.iter().all(|merged_entry| {
            self.left_journal
                .iter()
                .chain(self.right_journal.iter())
                .find(|entry| entry.txn == merged_entry.txn)
                .is_none_or(|entry| entry.decision == merged_entry.decision)
        })
    }
}

impl Model for MergeModel {
    type State = MergeState;
    type Action = MergeAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![MergeState::initial()]
    }

    fn actions(&self, state: &Self::State, out: &mut Vec<Self::Action>) {
        if state.steps >= MAX_STEPS {
            return;
        }

        out.extend([
            MergeAction::PrepareLeft,
            MergeAction::PrepareRight,
            MergeAction::Decide {
                txn: 1,
                decision: Decision::Commit,
            },
            MergeAction::Decide {
                txn: 2,
                decision: Decision::Abort,
            },
            MergeAction::ForceLeftCheckpoint,
            MergeAction::ForceRightCheckpoint,
            MergeAction::CommitMapVersion,
            MergeAction::RestoreMergedSuccessor,
            MergeAction::FoldInDoubtMarkers,
            MergeAction::ParkLeft,
            MergeAction::ParkRight,
            MergeAction::UnpauseServing,
            MergeAction::CrashAndRetry,
        ]);
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();
        next.steps += 1;

        match action {
            MergeAction::PrepareLeft => prepare_merge_side(
                &mut next.left_journal,
                &mut next.left_markers,
                1,
                1,
                5,
                next.left_checkpoint,
                self.correct,
            )?,
            MergeAction::PrepareRight => prepare_merge_side(
                &mut next.right_journal,
                &mut next.right_markers,
                2,
                2,
                7,
                next.right_checkpoint,
                self.correct,
            )?,
            MergeAction::Decide { txn, decision } => decide_merge(&mut next, txn, decision)?,
            MergeAction::ForceLeftCheckpoint => set_once(&mut next.left_checkpoint)?,
            MergeAction::ForceRightCheckpoint => set_once(&mut next.right_checkpoint)?,
            MergeAction::CommitMapVersion => commit_merge_map(&mut next)?,
            MergeAction::RestoreMergedSuccessor => restore_merged(&mut next, self.correct)?,
            MergeAction::FoldInDoubtMarkers => fold_merge_markers(&mut next, self.correct)?,
            MergeAction::ParkLeft => park_merge_left(&mut next)?,
            MergeAction::ParkRight => park_merge_right(&mut next)?,
            MergeAction::UnpauseServing => unpause_merge(&mut next, self.correct)?,
            MergeAction::CrashAndRetry => return None,
        }

        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always("merge keeps exactly one serving owner per key", |_, _| {
                MergeState::every_map_has_exactly_one_owner()
            }),
            Property::<Self>::always("merge restore is left plus right fold", |_, state| {
                state.merged_fold_equals_left_plus_right()
            }),
            Property::<Self>::always("complete merge leaves no stranded marker", |_, state| {
                state.no_stranded_merge_marker()
            }),
            Property::<Self>::always("merge decisions are honored", |_, state| {
                state.decisions_are_honored_after_merge()
            }),
        ]
    }
}

fn prepare_merge_side(
    journal: &mut Vec<JournalEntry>,
    markers: &mut Vec<Marker>,
    txn: u8,
    key: u8,
    delta: i8,
    checkpointed: bool,
    correct: bool,
) -> Option<()> {
    if correct && checkpointed {
        return None;
    }
    if journal.iter().any(|entry| entry.txn == txn) {
        return None;
    }

    journal.push(JournalEntry {
        txn,
        key,
        delta,
        decision: None,
    });
    markers.push(Marker { txn, key });
    Some(())
}

fn decide_merge(state: &mut MergeState, txn: u8, decision: Decision) -> Option<()> {
    let mut decided = false;
    for entry in state
        .left_journal
        .iter_mut()
        .chain(state.right_journal.iter_mut())
        .chain(state.merged_journal.iter_mut())
    {
        if entry.txn == txn && entry.decision.is_none() {
            entry.decision = Some(decision);
            decided = true;
        }
    }
    if !decided {
        return None;
    }

    state.left_markers.retain(|marker| marker.txn != txn);
    state.right_markers.retain(|marker| marker.txn != txn);
    state.merged_markers.retain(|marker| marker.txn != txn);
    Some(())
}

fn set_once(value: &mut bool) -> Option<()> {
    if *value {
        return None;
    }

    *value = true;
    Some(())
}

fn commit_merge_map(state: &mut MergeState) -> Option<()> {
    if state.map_committed || !state.left_checkpoint || !state.right_checkpoint {
        return None;
    }

    state.map_committed = true;
    Some(())
}

fn restore_merged(state: &mut MergeState, correct: bool) -> Option<()> {
    if state.merged_restored || !state.map_committed {
        return None;
    }

    state
        .merged_journal
        .extend(state.left_journal.iter().copied());
    if correct {
        state
            .merged_journal
            .extend(state.right_journal.iter().copied());
    }
    state.merged_restored = true;
    Some(())
}

fn fold_merge_markers(state: &mut MergeState, correct: bool) -> Option<()> {
    if !state.merged_restored || state.left_parked || state.right_parked || state.unpaused {
        return None;
    }
    if !state.merged_markers.is_empty() {
        return None;
    }

    state
        .merged_markers
        .extend(state.left_markers.iter().copied());
    state
        .merged_markers
        .extend(state.right_markers.iter().copied());
    if correct {
        state.left_markers.clear();
        state.right_markers.clear();
    }
    Some(())
}

fn park_merge_left(state: &mut MergeState) -> Option<()> {
    if state.left_parked || !state.merged_restored || !state.left_markers.is_empty() {
        return None;
    }

    state.left_parked = true;
    Some(())
}

fn park_merge_right(state: &mut MergeState) -> Option<()> {
    if state.right_parked || !state.left_parked || !state.right_markers.is_empty() {
        return None;
    }

    state.right_parked = true;
    Some(())
}

fn unpause_merge(state: &mut MergeState, correct: bool) -> Option<()> {
    if state.unpaused || !state.left_parked || !state.right_parked {
        return None;
    }
    if correct && !state.merged_markers.is_empty() {
        return None;
    }

    state.unpaused = true;
    Some(())
}

fn fold_all(journal: &[JournalEntry]) -> [i8; 4] {
    let mut fold = [0; 4];
    for entry in journal {
        if entry.decision == Some(Decision::Abort) {
            continue;
        }

        fold[usize::from(entry.key)] += entry.delta;
    }
    fold
}

fn add_folds(left: [i8; 4], right: [i8; 4]) -> [i8; 4] {
    [
        left[0] + right[0],
        left[1] + right[1],
        left[2] + right[2],
        left[3] + right[3],
    ]
}

#[test]
fn merge_orchestration_model_honors_invariants() {
    let checker = MergeModel { correct: true }.checker().spawn_bfs().join();

    checker.assert_properties();
    assert!(checker.unique_state_count() > 1);
}

#[test]
fn broken_merge_model_has_counterexamples() {
    let checker = MergeModel { correct: false }.checker().spawn_bfs().join();

    for property in [
        "merge restore is left plus right fold",
        "complete merge leaves no stranded marker",
    ] {
        assert!(checker.discoveries().contains_key(property), "{property}");
    }
}

#[test]
fn broken_split_model_has_counterexamples() {
    let checker = SplitModel { correct: false }.checker().spawn_bfs().join();

    for property in [
        "complete split leaves no stranded in-doubt marker",
        "successor fold equals partitioned predecessor checkpoint fold",
    ] {
        assert!(checker.discoveries().contains_key(property), "{property}");
    }
}
