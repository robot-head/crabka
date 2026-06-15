//! COMPOSITIONAL model of the exactly-once READ guarantee — the second
//! end-to-end model (after the data-path composition). It composes the real
//! txn-coordinator decision (`decide_phase1_transition` /
//! `decide_end_txn_completion`) with the LSO mechanics and the real
//! `read_committed` branch of `compute_visibility_window`. Single leader
//! (`hw = log_end`), >= 2 interleaving transactional producers.
//!
//! Verifies that what a `read_committed` consumer can see — every offset below
//! the LSO, minus aborted batches — is EXACTLY the committed records: no aborted
//! record ever leaks, and no record of a still-open transaction is exposed.
//! Because concurrent producers' batches interleave at the offset level, a
//! committed txn can sit partly above the LSO (behind an older open txn), so the
//! guarantee is prefix-correctness, not whole-txn snapshot atomicity. See the
//! design spec.

#![allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::time::Duration;

use stateright::{Checker, Model, Property};

use super::decision::{CompletionDecision, decide_end_txn_completion, decide_phase1_transition};
use super::state::{TxnEntry, TxnState};
use super::version::TxnVersion;
use crate::handlers::fetch::compute_visibility_window;
use crate::producer_id_manager::ProducerIdManager;

const TARGET_STATE_COUNT: usize = 20_000_000;
const MAX_UNIQUE_STATES: usize = 2_000_000;
const MAX_DEPTH: usize = 50;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);
const PID0: i64 = 1000; // base producer id; per-producer pid = PID0 + producer index

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Kind {
    Data,
    Commit,
    Abort,
}

/// One appended batch (offset = index in the log). A transaction = a producer's
/// run of `Data` batches in one `generation`, terminated by a Commit/Abort marker.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Batch {
    producer: u8,
    generation: u8,
    kind: Kind,
}

/// Per-producer coordinator projection (drives the real `TxnEntry`, hashably).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Prod {
    state: i8, // TxnState::to_kafka_status()
    epoch: i16,
    generation: u8,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct EosState {
    log: Vec<Batch>,
    prod: Vec<Prod>, // index = producer
}

struct EosModel {
    producers: u8,
    max_gen: u8,
    max_data_per_txn: usize,
    max_log: usize,
}

fn tstate(id: i8) -> TxnState {
    TxnState::from_kafka_status(id).expect("valid TxnState id")
}

/// Rebuild a real `TxnEntry` for producer `p` so the real decision cores behave
/// exactly as in a live run (partitions/timestamps don't affect the decision).
fn rebuild(p: usize, pr: Prod) -> TxnEntry {
    let mut e = TxnEntry::new_empty("tid".to_string(), PID0 + p as i64, pr.epoch, 60_000, 1);
    e.state = tstate(pr.state);
    e
}

// ----- derived txn structure (faithful LSO + aborted-list mechanics) -----

/// Outcome of txn (producer, generation) from the log: a marker resolves it.
fn txn_outcome(log: &[Batch], producer: u8, generation: u8) -> Option<Kind> {
    log.iter()
        .find(|b| {
            b.producer == producer
                && b.generation == generation
                && matches!(b.kind, Kind::Commit | Kind::Abort)
        })
        .map(|b| b.kind)
}

/// LSO = base offset of the oldest still-OPEN txn (Data present, no marker yet),
/// else the log end. Kafka's first-unstable-offset rule. Derived from the log
/// alone (the txn universe is whatever (producer, generation) pairs appear), so
/// the property closures stay non-capturing.
fn lso(log: &[Batch]) -> i64 {
    let mut min_open: Option<i64> = None;
    let mut seen: Vec<(u8, u8)> = Vec::new();
    for (off, b) in log.iter().enumerate() {
        if b.kind == Kind::Data && !seen.contains(&(b.producer, b.generation)) {
            seen.push((b.producer, b.generation));
            // First occurrence of this (producer, generation) — its base offset.
            if txn_outcome(log, b.producer, b.generation).is_none() {
                min_open = Some(min_open.map_or(off as i64, |m| m.min(off as i64)));
            }
        }
    }
    min_open.unwrap_or(log.len() as i64)
}

/// The `read_committed` visible set, driving the REAL `compute_visibility_window`:
/// `Data` batch offsets below `effective_lso` whose txn did NOT abort.
fn visible(log: &[Batch]) -> Vec<i64> {
    let log_end = log.len() as i64;
    let l = lso(log);
    let vw = compute_visibility_window(
        false,   // consumer, not follower
        true,    // read_committed
        0,       // log_start
        log_end, // hw = log_end (single leader, fully replicated)
        l,       // lso
        log_end, // log_end
        0,       // fetch_offset
    );
    let eff = vw.effective_lso; // read_committed branch: lso.min(hw) == l
    (0..log_end)
        .filter(|&off| off < eff)
        .filter(|&off| {
            let b = log[off as usize];
            b.kind == Kind::Data && txn_outcome(log, b.producer, b.generation) != Some(Kind::Abort)
        })
        .collect()
}

// ----- model -----

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Act {
    Begin(u8),     // producer p: -> Ongoing (new generation)
    Append(u8),    // producer p: append a Data batch to its open txn
    End(u8, bool), // producer p: commit? -> drive decision cores + append marker
}

impl Model for EosModel {
    type State = EosState;
    type Action = Act;

    fn init_states(&self) -> Vec<Self::State> {
        vec![EosState {
            log: vec![],
            prod: (0..self.producers)
                .map(|_| Prod {
                    state: TxnState::Empty.to_kafka_status(),
                    epoch: 0,
                    generation: 0,
                })
                .collect(),
        }]
    }

    fn actions(&self, s: &Self::State, acts: &mut Vec<Self::Action>) {
        if s.log.len() >= self.max_log {
            return;
        }
        for p in 0..self.producers {
            let pr = s.prod[p as usize];
            if pr.generation < self.max_gen && tstate(pr.state).can_transition_to(TxnState::Ongoing)
            {
                acts.push(Act::Begin(p));
            }
            if pr.state == TxnState::Ongoing.to_kafka_status() {
                let n = s
                    .log
                    .iter()
                    .filter(|b| {
                        b.producer == p && b.generation == pr.generation && b.kind == Kind::Data
                    })
                    .count();
                if n < self.max_data_per_txn {
                    acts.push(Act::Append(p));
                }
                if n >= 1 {
                    acts.push(Act::End(p, true));
                    acts.push(Act::End(p, false));
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, a: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match a {
            Act::Begin(p) => {
                let pr = &mut s.prod[p as usize];
                if !tstate(pr.state).can_transition_to(TxnState::Ongoing) {
                    return None;
                }
                pr.generation += 1;
                pr.state = TxnState::Ongoing.to_kafka_status();
            }
            Act::Append(p) => {
                let g = s.prod[p as usize].generation;
                s.log.push(Batch {
                    producer: p,
                    generation: g,
                    kind: Kind::Data,
                });
            }
            Act::End(p, commit) => {
                let pr = s.prod[p as usize];
                let mut entry = rebuild(p as usize, pr);
                let Ok((prepare, complete)) = decide_phase1_transition(&mut entry, commit) else {
                    return None; // illegal transition
                };
                let ids = ProducerIdManager::new();
                match decide_end_txn_completion(
                    &entry,
                    PID0 + i64::from(p),
                    pr.epoch,
                    prepare,
                    complete,
                    TxnVersion::Verified,
                    &ids,
                ) {
                    CompletionDecision::Proceed {
                        next_state,
                        response_epoch,
                        ..
                    } => {
                        s.log.push(Batch {
                            producer: p,
                            generation: pr.generation,
                            kind: if commit { Kind::Commit } else { Kind::Abort },
                        });
                        let np = &mut s.prod[p as usize];
                        np.state = next_state.to_kafka_status();
                        np.epoch = response_epoch; // TV_2 bumps the epoch on completion
                    }
                    CompletionDecision::AlreadyComplete { .. } | CompletionDecision::Reject(_) => {
                        return None;
                    }
                }
            }
        }
        // LSO is monotonic across every transition (offsets only grow; the
        // oldest-open base only advances). Assert it (cheap regression guard).
        assert!(lso(&s.log) >= lso(&last.log), "LSO regressed");
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // HEADLINE: every visible batch belongs to a COMMITTED txn — no
            // open/uncommitted and no aborted record is ever visible. Catches a
            // real `compute_visibility_window` returning effective_lso ABOVE the
            // LSO (which would expose open-txn data below it).
            Property::always("only_committed_visible", |_, s: &EosState| {
                visible(&s.log).into_iter().all(|off| {
                    let b = s.log[off as usize];
                    txn_outcome(&s.log, b.producer, b.generation) == Some(Kind::Commit)
                })
            }),
            // Every committed Data batch below the LSO IS visible — no committed
            // record below the LSO is wrongly hidden. Catches effective_lso BELOW
            // the LSO. With the headline: visible = exactly committed-below-LSO.
            Property::always("committed_prefix_complete", |_, s: &EosState| {
                let v = visible(&s.log);
                let l = lso(&s.log);
                s.log.iter().enumerate().all(|(off, b)| {
                    !(b.kind == Kind::Data
                        && (off as i64) < l
                        && txn_outcome(&s.log, b.producer, b.generation) == Some(Kind::Commit))
                        || v.contains(&(off as i64))
                })
            }),
            // No aborted batch is ever visible (the abort-side all-or-nothing).
            Property::always("no_visible_aborted", |_, s: &EosState| {
                visible(&s.log).into_iter().all(|off| {
                    let b = s.log[off as usize];
                    txn_outcome(&s.log, b.producer, b.generation) != Some(Kind::Abort)
                })
            }),
            // ----- non-vacuity witnesses -----
            Property::sometimes("committed_visible", |_, s: &EosState| {
                !visible(&s.log).is_empty()
            }),
            Property::sometimes("aborted_filtered", |_, s: &EosState| {
                let l = lso(&s.log);
                s.log.iter().enumerate().any(|(off, b)| {
                    b.kind == Kind::Data
                        && (off as i64) < l
                        && txn_outcome(&s.log, b.producer, b.generation) == Some(Kind::Abort)
                })
            }),
            // The key EOS subtlety: a COMMITTED batch sits ABOVE the LSO, held
            // back by an older still-open transaction (out-of-order commit).
            Property::sometimes("interleaved_held_back", |_, s: &EosState| {
                let l = lso(&s.log);
                s.log.iter().enumerate().any(|(off, b)| {
                    b.kind == Kind::Data
                        && (off as i64) >= l
                        && txn_outcome(&s.log, b.producer, b.generation) == Some(Kind::Commit)
                })
            }),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.log.len() <= self.max_log
    }
}

fn run(model: EosModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(TARGET_STATE_COUNT)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique={} generated={} depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert!(checker.max_depth() < MAX_DEPTH, "[{label}] depth cap hit");
    assert!(
        checker.state_count() < TARGET_STATE_COUNT,
        "[{label}] truncated"
    );
    assert!(
        checker.unique_state_count() < MAX_UNIQUE_STATES,
        "[{label}] unique bound exceeded ({})",
        checker.unique_state_count()
    );
    checker.assert_properties();
}

#[test]
fn txn_basic() {
    run(
        EosModel {
            producers: 2,
            max_gen: 1,
            max_data_per_txn: 2,
            max_log: 5,
        },
        "txn_basic",
    );
}

#[test]
fn txn_wide() {
    // Deeper interleaving: a second transaction generation per producer + a
    // longer log, so a producer's committed txn can be held back by another
    // producer's later open txn across more offset orderings.
    run(
        EosModel {
            producers: 2,
            max_gen: 2,
            max_data_per_txn: 2,
            max_log: 7,
        },
        "txn_wide",
    );
}
