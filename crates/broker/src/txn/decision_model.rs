//! Exhaustive stateright model of the KIP-98/EOS `EndTxn` decision core.
//!
//! Drives the real `decide_phase1_transition` / `decide_end_txn_completion`
//! (and `TxnState::can_transition_to`) over one transactional-id, modeling the
//! `EndTxn` Phase1 → marker-window → Phase3 split so a concurrent `InitProducerId`
//! (which bumps the producer epoch) can interleave in the window and fence the
//! in-flight transaction. Design:
//! `docs/superpowers/specs/2026-06-14-crabka-txn-coordinator-model-design.md`.
//!
//! Headline safety: a producer fenced (epoch bumped) during the window can never
//! finalize, and a given producer epoch's transaction is finalized at most once
//! and never both committed and aborted.
//!
//! NOTE: the partition set is omitted (it does not affect the fencing/atomicity
//! properties); the txn-start is modeled as `BeginTxn` (the `→ Ongoing`
//! transition). Terminal outcomes are tracked as ghost per-epoch sets, not
//! lifetime flags, so a tid that legitimately commits one generation and aborts
//! the next is not a false violation.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each run is fenced with `within_boundary` + `target_state_count` + `timeout`
//! and MUST be executed under the host memory watchdog while bounds are tuned.

use std::time::Duration;

use crabka_log::ProducerId;
use stateright::{Checker, Model, Property};

use super::{
    super::{
        state::{TxnEntry, TxnState},
        version::TxnVersion,
    },
    CompletionDecision, decide_end_txn_completion, decide_phase1_transition,
};
use crate::producer_id_manager::ProducerIdManager;

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 60;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);
const PID: ProducerId = ProducerId(1000); // fixed; epoch is the fencing dimension

struct TxnModel {
    max_epoch: i16,
}

/// In-flight `EndTxn` captured at Phase 1, awaiting Phase 3 (the marker window).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct PendingEnd {
    expected_epoch: i16,
    prepare: i8, // TxnState::to_kafka_status()
    complete: i8,
    committed: bool,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct TxnProj {
    epoch: i16,
    state: i8, // TxnState::to_kafka_status()
    pending: Option<PendingEnd>,
    /// Ghost: producer epochs whose transaction finalized as commit / abort.
    /// Sorted, distinct. The invariants assert these never overlap and never
    /// record the same epoch twice (single-finalize per generation).
    committed: Vec<i16>,
    aborted: Vec<i16>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum TxnAction {
    Init,               // InitProducerId: bump epoch (aborts an in-flight txn)
    BeginTxn,           // AddPartitionsToTxn: → Ongoing
    EndTxnPhase1(bool), // committed? → Prepare; opens the window
    EndTxnPhase3,       // re-validate → Complete or fenced-reject
}

fn st(id: i8) -> TxnState {
    TxnState::from_kafka_status(id).expect("valid TxnState id in model")
}

/// Reconstruct a real `TxnEntry` from the projection so the real decision fns
/// behave identically to a live run. Partitions/timestamps don't affect the
/// decision, so they're left empty/constant.
fn rebuild(s: &TxnProj) -> TxnEntry {
    let mut e = TxnEntry::new_empty("tid".to_string(), PID, s.epoch, 60_000, 1);
    e.state = st(s.state);
    e
}

/// Record a terminal outcome for `epoch`, asserting it has not already
/// finalized either way (single-finalize + no-commit-and-abort per generation).
fn record(committed: &mut Vec<i16>, aborted: &mut Vec<i16>, epoch: i16, is_commit: bool) {
    assert!(
        !committed.contains(&epoch) && !aborted.contains(&epoch),
        "epoch {epoch} finalized twice (commit={is_commit}); committed={committed:?} aborted={aborted:?}"
    );
    let v = if is_commit { committed } else { aborted };
    v.push(epoch);
    v.sort_unstable();
}

impl Model for TxnModel {
    type State = TxnProj;
    type Action = TxnAction;

    fn init_states(&self) -> Vec<Self::State> {
        // A tid that has completed its first InitProducerId: epoch 0, Empty.
        vec![TxnProj {
            epoch: 0,
            state: TxnState::Empty.to_kafka_status(),
            pending: None,
            committed: vec![],
            aborted: vec![],
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        let under_cap = s.epoch < self.max_epoch;
        // Init bumps the epoch → epoch-advancing, gated.
        if under_cap {
            actions.push(TxnAction::Init);
        }
        // BeginTxn: legal `→ Ongoing` and no EndTxn in flight.
        if s.pending.is_none() && st(s.state).can_transition_to(TxnState::Ongoing) {
            actions.push(TxnAction::BeginTxn);
        }
        // EndTxnPhase1: only from Ongoing, no EndTxn in flight.
        if s.pending.is_none() && s.state == TxnState::Ongoing.to_kafka_status() {
            actions.push(TxnAction::EndTxnPhase1(true));
            actions.push(TxnAction::EndTxnPhase1(false));
        }
        // EndTxnPhase3: only with an in-flight EndTxn.
        if s.pending.is_some() {
            actions.push(TxnAction::EndTxnPhase3);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            TxnAction::Init => {
                if s.epoch >= self.max_epoch {
                    return None;
                }
                // InitProducerId aborts an in-flight transaction (Ongoing or
                // mid-EndTxn Prepare*) before bumping the epoch. Record that
                // abort for the CURRENT generation; the bump then fences any
                // pending EndTxn (its expected_epoch < the new epoch).
                let cur = st(s.state);
                if matches!(
                    cur,
                    TxnState::Ongoing | TxnState::PrepareCommit | TxnState::PrepareAbort
                ) {
                    record(&mut s.committed, &mut s.aborted, s.epoch, false);
                }
                s.epoch += 1;
                s.state = TxnState::Empty.to_kafka_status();
                // `pending` is intentionally retained: a pending EndTxn from the
                // old epoch must still run Phase 3 and be REJECTED (fenced).
                assert!(s.epoch >= last.epoch, "epoch regressed on Init");
                Some(s)
            }
            TxnAction::BeginTxn => {
                if !st(s.state).can_transition_to(TxnState::Ongoing) {
                    return None;
                }
                s.state = TxnState::Ongoing.to_kafka_status();
                Some(s)
            }
            TxnAction::EndTxnPhase1(committed) => {
                if s.pending.is_some() {
                    return None;
                }
                let mut entry = rebuild(&s);
                match decide_phase1_transition(&mut entry, committed) {
                    Ok((prepare, complete)) => {
                        s.state = prepare.to_kafka_status();
                        s.pending = Some(PendingEnd {
                            expected_epoch: s.epoch,
                            prepare: prepare.to_kafka_status(),
                            complete: complete.to_kafka_status(),
                            committed,
                        });
                        Some(s)
                    }
                    Err(_) => None, // illegal transition: no-op edge
                }
            }
            TxnAction::EndTxnPhase3 => {
                let p = s.pending.clone()?;
                let entry = rebuild(&s);
                let ids = ProducerIdManager::new();
                match decide_end_txn_completion(
                    &entry,
                    PID,
                    p.expected_epoch,
                    st(p.prepare),
                    st(p.complete),
                    TxnVersion::Verified,
                    &ids,
                ) {
                    CompletionDecision::Proceed {
                        next_state,
                        response_epoch,
                        ..
                    } => {
                        // HEADLINE: a Proceed must NOT be a fenced producer — the
                        // current epoch must still match what Phase 1 captured.
                        assert!(
                            p.expected_epoch == s.epoch,
                            "fenced producer finalized: expected_epoch={} current_epoch={}",
                            p.expected_epoch,
                            s.epoch
                        );
                        record(
                            &mut s.committed,
                            &mut s.aborted,
                            p.expected_epoch,
                            p.committed,
                        );
                        s.state = next_state.to_kafka_status();
                        s.epoch = response_epoch; // TV_2 bumps on completion
                        s.pending = None;
                        assert!(s.epoch >= last.epoch, "epoch regressed on completion");
                        Some(s)
                    }
                    CompletionDecision::AlreadyComplete { .. } => {
                        // Idempotent retry / lost race: clear pending, no re-finalize.
                        s.pending = None;
                        Some(s)
                    }
                    CompletionDecision::Reject(_) => {
                        // Fenced or state advanced: must NOT finalize. Assert the
                        // reject is justified (producer was fenced).
                        assert!(
                            p.expected_epoch != s.epoch || s.state != p.prepare,
                            "EndTxn rejected without a fencing/state reason"
                        );
                        s.pending = None;
                        Some(s)
                    }
                }
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // HEADLINE: a producer epoch's transaction is never both committed and
            // aborted (atomicity + single-finalize across all interleavings).
            Property::always("no_commit_and_abort", |_, s: &TxnProj| {
                s.committed.iter().all(|e| !s.aborted.contains(e))
            }),
            // A pending EndTxn was captured at an epoch no greater than the
            // current one (the epoch only grows; epoch monotonicity itself is a
            // `next_state` assertion).
            Property::always("pending_epoch_not_future", |_, s: &TxnProj| {
                s.pending
                    .as_ref()
                    .is_none_or(|p| p.expected_epoch <= s.epoch)
            }),
            // Non-vacuity: a commit can complete.
            Property::sometimes("can_commit", |_, s: &TxnProj| !s.committed.is_empty()),
            // Non-vacuity: a producer is fenced while its EndTxn is pending (the
            // pending's epoch lags the current epoch — the zombie window).
            Property::sometimes("fence_in_window", |_, s: &TxnProj| {
                s.pending
                    .as_ref()
                    .is_some_and(|p| p.expected_epoch < s.epoch)
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.epoch <= self.max_epoch
    }
}

fn run(model: TxnModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert!(
        checker.max_depth() < MAX_DEPTH,
        "[{label}] hit depth cap {MAX_DEPTH}: depth-truncated, not exhaustive"
    );
    assert!(
        checker.state_count() < MAX_STATES,
        "[{label}] hit state cap {MAX_STATES}: truncated, not exhaustive"
    );
    checker.assert_properties();
}

#[test]
fn txn_basic() {
    // One tid, epoch 0..=3: every interleaving of Init / BeginTxn / EndTxn
    // Phase1 / Phase3, including a fencing Init inside the marker window.
    run(TxnModel { max_epoch: 3 }, "txn_basic");
}

#[test]
fn txn_wide() {
    // More producer-epoch generations → deeper commit/abort/fence interleavings.
    run(TxnModel { max_epoch: 6 }, "txn_wide");
}
