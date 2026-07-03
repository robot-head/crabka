//! COMPOSITIONAL model of the exactly-once READ visibility algebra — the second
//! end-to-end model (after the data-path composition). Over a single partition
//! with >= 2 interleaving transactional producers and an advancing HWM, it
//! verifies that what a `read_committed` consumer may see — every offset below
//! `effective_lso = min(lso, hw)`, minus aborted batches — is EXACTLY the
//! committed records: no aborted record ever leaks, no still-open-transaction
//! record is exposed, and nothing above the HWM is exposed. Because concurrent
//! producers' batches interleave at the offset level, a committed txn can sit
//! partly above the LSO (behind an older open txn) or above the HWM, so the
//! guarantee is prefix-correctness, not whole-txn snapshot atomicity.
//!
//! Scope — what is DRIVEN vs MODELED (an adversarial faithfulness review flagged
//! the original framing as over-claiming):
//!   - DRIVEN (real code): the EndTxn decision cores `decide_phase1_transition` /
//!     `decide_end_txn_completion` on their Proceed path (the fencing / retry
//!     arms are exercised by `decision_model.rs`, #523; a guard `unreachable!`s if
//!     they ever fire here); and the real `read_committed` clamp of
//!     `compute_visibility_window` (`effective_lso = lso.min(hw)`), which bites
//!     non-trivially when an open txn's records sit above the HWM (witness
//!     `hwm_clamp_active`).
//!   - MODELED (faithful abstraction, NOT driving real code): the LSO rule
//!     (Kafka's first-unstable-offset; `Log::lso()`'s incremental maintenance is
//!     stored state, not a pure fn) and the abort filter (a Data batch is hidden
//!     iff its txn aborted — equivalent to the client-side `poll.rs` /
//!     `TxnIndex::aborted_in_range` range filtering ONLY under the one-in-flight-
//!     txn-per-producer invariant this model enforces).
//!   - NOT covered (left to the per-slice log / txn-index / fetch models): the
//!     `Log::lso()` maintenance internals, `TxnIndex::aborted_in_range` overlap
//!     arithmetic, and the consumer `aborted_pids` state machine.
//!
//! See the design spec.

#![allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::time::Duration;

use crabka_log::Offset;
use stateright::{Checker, Model, Property};

use super::{
    decision::{CompletionDecision, decide_end_txn_completion, decide_phase1_transition},
    state::{TxnEntry, TxnState},
    version::TxnVersion,
};
use crate::{handlers::fetch::compute_visibility_window, producer_id_manager::ProducerIdManager};

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
    /// High watermark = offsets replicated/durable so far (`hw <= log_end`),
    /// advanced by `Ack`. An OPEN transaction's not-yet-replicated records can
    /// push the LSO ABOVE the HWM, so the real `compute_visibility_window`
    /// clamp `effective_lso = lso.min(hw)` genuinely bites (returns `hw`).
    hw: Offset,
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
fn lso(log: &[Batch]) -> Offset {
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
    Offset(min_open.unwrap_or(log.len() as i64))
}

/// The exclusive offset a `read_committed` consumer may see, driving the REAL
/// `compute_visibility_window` (read-committed branch: `effective_lso =
/// lso.min(hw)`). When an open txn's records sit above the HWM, `lso > hw` and
/// the clamp returns `hw` — the consumer never reads above the watermark.
fn effective_lso(log: &[Batch], hw: Offset) -> Offset {
    let log_end = Offset(log.len() as i64);
    let l = lso(log);
    let vw = compute_visibility_window(
        false,     // consumer, not follower
        true,      // read_committed
        Offset(0), // log_start
        hw,        // hw (may be < log_end: replication lag)
        l,         // lso
        log_end,   // log_end
        Offset(0), // fetch_offset
    );
    vw.effective_lso // = lso.min(hw)
}

/// The `read_committed` visible set: `Data` batch offsets below `effective_lso`
/// whose txn did NOT abort.
fn visible(log: &[Batch], hw: Offset) -> Vec<i64> {
    let eff = effective_lso(log, hw);
    (0..log.len() as i64)
        .filter(|&off| off < eff.0)
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
    Ack,           // a follower replicates one more offset: hw += 1
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
            hw: Offset(0),
        }]
    }

    fn actions(&self, s: &Self::State, acts: &mut Vec<Self::Action>) {
        // A follower replicating: advance the HWM toward the log end.
        if s.hw.0 < s.log.len() as i64 {
            acts.push(Act::Ack);
        }
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
                    // This no-window single-`End` path always Proceeds (the epoch
                    // is never bumped underneath it); the fencing / idempotent-retry
                    // arms are exercised by `decision_model.rs` (#523). Guard so a
                    // future change that makes them reachable surfaces loudly
                    // rather than silently shrinking what GREEN means.
                    other => unreachable!("no-window End must Proceed, got {other:?}"),
                }
            }
            Act::Ack => {
                s.hw += 1; // a follower replicated one more offset
            }
        }
        // HWM never regresses and never passes the log end.
        assert!(
            s.hw >= last.hw && s.hw.0 <= s.log.len() as i64,
            "HWM out of range"
        );
        // LSO is monotonic across every transition (offsets only grow; the
        // oldest-open base only advances). Assert it (cheap regression guard).
        assert!(lso(&s.log) >= lso(&last.log), "LSO regressed");
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // HEADLINE: every visible batch belongs to a COMMITTED txn — no
            // open/uncommitted and no aborted record is ever visible. Catches a
            // real `compute_visibility_window` returning effective_lso ABOVE
            // min(lso, hw) (which would expose open-txn or above-HWM data).
            Property::always("only_committed_visible", |_, s: &EosState| {
                visible(&s.log, s.hw).into_iter().all(|off| {
                    let b = s.log[off as usize];
                    txn_outcome(&s.log, b.producer, b.generation) == Some(Kind::Commit)
                })
            }),
            // Every committed Data batch below the effective LSO (= min(lso, hw))
            // IS visible — no committed, durable, stable record is wrongly hidden.
            // Catches effective_lso BELOW min(lso, hw). With the headline:
            // visible = exactly committed-below-effective-LSO.
            Property::always("committed_prefix_complete", |_, s: &EosState| {
                let v = visible(&s.log, s.hw);
                let eff = effective_lso(&s.log, s.hw);
                s.log.iter().enumerate().all(|(off, b)| {
                    !(b.kind == Kind::Data
                        && (off as i64) < eff.0
                        && txn_outcome(&s.log, b.producer, b.generation) == Some(Kind::Commit))
                        || v.contains(&(off as i64))
                })
            }),
            // No aborted batch is ever visible (the abort-side all-or-nothing).
            Property::always("no_visible_aborted", |_, s: &EosState| {
                visible(&s.log, s.hw).into_iter().all(|off| {
                    let b = s.log[off as usize];
                    txn_outcome(&s.log, b.producer, b.generation) != Some(Kind::Abort)
                })
            }),
            // ----- non-vacuity witnesses -----
            Property::sometimes("committed_visible", |_, s: &EosState| {
                !visible(&s.log, s.hw).is_empty()
            }),
            Property::sometimes("aborted_filtered", |_, s: &EosState| {
                let eff = effective_lso(&s.log, s.hw);
                s.log.iter().enumerate().any(|(off, b)| {
                    b.kind == Kind::Data
                        && (off as i64) < eff.0
                        && txn_outcome(&s.log, b.producer, b.generation) == Some(Kind::Abort)
                })
            }),
            // The key EOS subtlety: a COMMITTED batch sits ABOVE the LSO, held
            // back by an older still-open transaction (out-of-order commit).
            Property::sometimes("interleaved_held_back", |_, s: &EosState| {
                let l = lso(&s.log);
                s.log.iter().enumerate().any(|(off, b)| {
                    b.kind == Kind::Data
                        && (off as i64) >= l.0
                        && txn_outcome(&s.log, b.producer, b.generation) == Some(Kind::Commit)
                })
            }),
            // The visibility CORE actively clamps: the HWM holds the effective LSO
            // BELOW the LSO (an open txn's records sit above the not-yet-replicated
            // HWM). Proves `compute_visibility_window`'s `lso.min(hw)` is exercised
            // non-trivially, not as an identity pass-through.
            Property::sometimes("hwm_clamp_active", |_, s: &EosState| {
                effective_lso(&s.log, s.hw) < lso(&s.log)
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
