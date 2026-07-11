//! Exhaustive stateright model of the KIP-939 2PC timeout-safety property.
//!
//! Drives the real [`should_abort_idle_txn`] (the idle-transaction reaper's
//! decision core) and [`decide_phase1_transition`] / `TxnState::can_transition_to`
//! over one transactional-id, interleaving the timeout **reaper** with the full
//! transaction lifecycle (`InitProducerId` / `AddPartitionsToTxn` / `EndTxn`) and
//! with the 2PC-vs-classic distinction.
//!
//! Headline safety (KIP-939): **a two-phase-commit transaction is never aborted
//! by the timeout reaper.** A 2PC transaction is the one a producer opened after
//! `InitProducerId(enable2Pc=true)`; in the coordinator it is encoded by the
//! [`NO_TIMEOUT_MS`] sentinel and the reaper's decision core skips it. Only an
//! *explicit* `InitProducerId` (a new generation taking over) or an `EndTxn`
//! may end such a transaction — never a wall-clock timeout.
//!
//! Secondary safety (composition): a given producer-epoch generation is
//! finalized at most once and never both committed and aborted, even with the
//! reaper interleaved into the lifecycle.
//!
//! This is the timeout-dimension companion to the `decision_model` (which
//! covers the `EndTxn` Phase1/Phase3 fencing window). Terminal outcomes are
//! tracked as ghost per-epoch sets so a tid that legitimately commits one
//! generation and aborts the next is not a false violation.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each run is fenced with `within_boundary` + `target_state_count` + `timeout`.

use std::time::Duration;

use stateright::{Checker, Model, Property};

use super::{
    decision::decide_phase1_transition,
    state::{TxnEntry, TxnState},
    two_pc::{NO_TIMEOUT_MS, should_abort_idle_txn},
};

const MAX_STATES: usize = 300_000;
const MAX_DEPTH: usize = 80;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);
const PID: i64 = 1000; // fixed; epoch is the fencing dimension
const CLASSIC_TIMEOUT_MS: i32 = 60_000;
/// The transaction's notional start; the reaper measures elapsed time from here.
const START_MS: i64 = 0;

struct TwoPcModel {
    max_epoch: i16,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct TwoPcProj {
    epoch: i16,
    state: i8, // TxnState::to_kafka_status()
    /// Did the current producer generation enable 2PC? An `Ongoing` txn started
    /// by such a generation carries the [`NO_TIMEOUT_MS`] sentinel.
    two_pc: bool,
    /// Ghost: producer epochs whose transaction finalized as commit / abort.
    /// Sorted, distinct. Invariants assert these never overlap (atomicity) and
    /// never record the same epoch twice (single-finalize per generation).
    committed: Vec<i16>,
    aborted: Vec<i16>,
    /// Ghost — THE KIP-939 violation flag: set if the timeout reaper ever
    /// aborted a 2PC transaction. The `two_pc_never_reaped` property asserts it
    /// stays `false`; it can only stay false if [`should_abort_idle_txn`] is
    /// correct.
    reaped_2pc: bool,
    /// Ghost (non-vacuity): the reaper aborted a classic (non-2PC) txn at least
    /// once, proving the reaper is not vacuously inert.
    reaped_non_2pc: bool,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum TwoPcAction {
    /// `InitProducerId`: bump epoch, choose whether this generation is 2PC.
    /// Explicitly aborts an in-flight transaction (allowed even for 2PC).
    Init(bool),
    /// `AddPartitionsToTxn`: → Ongoing (a 2PC generation opens a 2PC txn).
    BeginTxn,
    /// `EndTxn`: the external/normal commit or abort path (committed?).
    EndTxn(bool),
    /// The idle-transaction reaper fires. `elapsed_long` picks whether enough
    /// wall-time has passed (`now = i64::MAX`) or not (`now = START_MS`).
    TimeoutSweep(bool),
}

fn st(id: i8) -> TxnState {
    TxnState::from_kafka_status(id).expect("valid TxnState id in model")
}

/// The persisted timeout for the current generation's transaction: the 2PC
/// sentinel for a 2PC generation, else a classic finite timeout.
fn timeout_for(two_pc: bool) -> i32 {
    if two_pc {
        NO_TIMEOUT_MS
    } else {
        CLASSIC_TIMEOUT_MS
    }
}

/// Reconstruct a real `TxnEntry` so the real decision fns behave exactly as in
/// a live run. Partitions are irrelevant to these decisions.
fn rebuild(s: &TwoPcProj) -> TxnEntry {
    let mut e = TxnEntry::new_empty(
        "tid".to_string(),
        crabka_log::ProducerId(PID),
        s.epoch,
        timeout_for(s.two_pc),
        START_MS,
    );
    e.state = st(s.state);
    e
}

/// Record a terminal outcome for `epoch`, asserting it has not already
/// finalized either way (single-finalize + no commit-and-abort per generation).
fn record(committed: &mut Vec<i16>, aborted: &mut Vec<i16>, epoch: i16, is_commit: bool) {
    assert2::assert!(!committed.contains(&epoch) && !aborted.contains(&epoch));
    let v = if is_commit { committed } else { aborted };
    v.push(epoch);
    v.sort_unstable();
}

impl Model for TwoPcModel {
    type State = TwoPcProj;
    type Action = TwoPcAction;

    fn init_states(&self) -> Vec<Self::State> {
        // A tid that completed its first InitProducerId: epoch 0, Empty, classic.
        vec![TwoPcProj {
            epoch: 0,
            state: TxnState::Empty.to_kafka_status(),
            two_pc: false,
            committed: vec![],
            aborted: vec![],
            reaped_2pc: false,
            reaped_non_2pc: false,
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        let under_cap = s.epoch < self.max_epoch;
        // Init bumps the epoch → gated. Either a classic or a 2PC generation.
        if under_cap {
            actions.push(TwoPcAction::Init(false));
            actions.push(TwoPcAction::Init(true));
        }
        // BeginTxn: legal `→ Ongoing`.
        if st(s.state).can_transition_to(TxnState::Ongoing) {
            actions.push(TwoPcAction::BeginTxn);
        }
        // EndTxn: only from Ongoing; bumps epoch on completion (TV_2) → gated.
        if under_cap && s.state == TxnState::Ongoing.to_kafka_status() {
            actions.push(TwoPcAction::EndTxn(true));
            actions.push(TwoPcAction::EndTxn(false));
        }
        // The reaper can fire at any time; gated because a real abort bumps the
        // epoch to fence the timed-out producer.
        if under_cap {
            actions.push(TwoPcAction::TimeoutSweep(true));
            actions.push(TwoPcAction::TimeoutSweep(false));
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            TwoPcAction::Init(two_pc) => {
                if s.epoch >= self.max_epoch {
                    return None;
                }
                // An explicit InitProducerId aborts an in-flight transaction
                // before bumping the epoch — this is ALLOWED for 2PC (a new
                // generation deliberately takes over), unlike a timeout reap.
                if st(s.state) == TxnState::Ongoing {
                    record(&mut s.committed, &mut s.aborted, s.epoch, false);
                }
                s.epoch += 1;
                s.state = TxnState::Empty.to_kafka_status();
                s.two_pc = two_pc;
                assert2::assert!(s.epoch >= last.epoch);
                Some(s)
            }
            TwoPcAction::BeginTxn => {
                if !st(s.state).can_transition_to(TxnState::Ongoing) {
                    return None;
                }
                s.state = TxnState::Ongoing.to_kafka_status();
                Some(s)
            }
            TwoPcAction::EndTxn(committed) => {
                if s.epoch >= self.max_epoch {
                    return None;
                }
                // Drive the real Phase-1 validator, then finalize atomically
                // (the Phase1/Phase3 fencing window is covered by decision_model).
                let mut entry = rebuild(&s);
                let Ok((_prepare, complete)) = decide_phase1_transition(&mut entry, committed)
                else {
                    return None; // illegal transition: no-op edge
                };
                record(&mut s.committed, &mut s.aborted, s.epoch, committed);
                s.state = complete.to_kafka_status();
                s.epoch += 1; // TV_2 epoch bump on completion (fence)
                Some(s)
            }
            TwoPcAction::TimeoutSweep(elapsed_long) => {
                if s.epoch >= self.max_epoch {
                    return None;
                }
                let entry = rebuild(&s);
                let now_ms = if elapsed_long { i64::MAX } else { START_MS };
                if !should_abort_idle_txn(entry.state, entry.txn_timeout_ms, entry.start_ms, now_ms)
                {
                    return None; // reaper spares this txn: no-op edge
                }
                // The reaper decided to abort. Track whether it just violated the
                // KIP-939 guarantee (aborted a 2PC txn — must be impossible) or
                // performed a legitimate classic-timeout abort (non-vacuity).
                if s.two_pc {
                    s.reaped_2pc = true;
                } else {
                    s.reaped_non_2pc = true;
                }
                record(&mut s.committed, &mut s.aborted, s.epoch, false);
                s.state = TxnState::CompleteAbort.to_kafka_status();
                s.epoch += 1; // fence the timed-out producer
                Some(s)
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // HEADLINE (KIP-939): the timeout reaper never aborts a 2PC txn.
            Property::always("two_pc_never_reaped", |_, s: &TwoPcProj| !s.reaped_2pc),
            // Atomicity / single-finalize across all interleavings, including
            // the reaper: no epoch is both committed and aborted.
            Property::always("no_commit_and_abort", |_, s: &TwoPcProj| {
                s.committed.iter().all(|e| !s.aborted.contains(e))
            }),
            // Non-vacuity: the reaper actually aborts classic transactions, so
            // the headline property isn't vacuously satisfied by an inert reaper.
            Property::sometimes("reaper_aborts_classic", |_, s: &TwoPcProj| s.reaped_non_2pc),
            // Non-vacuity: a 2PC transaction is reachable and open (so the
            // "never reaped" property has something to protect).
            Property::sometimes("two_pc_txn_open", |_, s: &TwoPcProj| {
                s.two_pc && s.state == TxnState::Ongoing.to_kafka_status()
            }),
            // Non-vacuity: a commit can complete.
            Property::sometimes("can_commit", |_, s: &TwoPcProj| !s.committed.is_empty()),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.epoch <= self.max_epoch
    }
}

fn run(model: TwoPcModel, label: &str) {
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
    assert2::assert!(checker.max_depth() < MAX_DEPTH);
    assert2::assert!(checker.state_count() < MAX_STATES);
    checker.assert_properties();
}

#[test]
fn two_pc_basic() {
    // One tid, epoch 0..=3: every interleaving of Init(classic/2PC) / BeginTxn /
    // EndTxn / TimeoutSweep, including a reaper firing on a 2PC txn (which must
    // never abort it) and on a classic txn (which must).
    run(TwoPcModel { max_epoch: 3 }, "two_pc_basic");
}

#[test]
fn two_pc_wide() {
    // More generations → deeper classic↔2PC alternations and reaper interleaves.
    run(TwoPcModel { max_epoch: 6 }, "two_pc_wide");
}
