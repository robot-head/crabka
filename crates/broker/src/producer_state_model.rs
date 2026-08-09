//! Exhaustive stateright enumeration of the idempotent-producer dedup core
//! (`check_pure`). There is one producer-id per partition, and the broker
//! serializes requests. So this model enumerates every bounded submit-sequence
//! and asserts, with per-transition checks, that `check_pure`'s classification
//! keeps the accepted-append log a gap-free, duplicate-free, monotonic prefix
//! per producer epoch, with epoch fencing. See the design spec
//! `docs/superpowers/specs/2026-06-14-crabka-data-plane-safety-models-design.md`.
//!
//! Offset *values* are irrelevant to the safety properties, because this model
//! does not use the `Duplicate` echo. So the fingerprinted state does NOT hold
//! them. Adding them explodes the space with a monotonic counter that adds no
//! behavior.

use std::time::Duration;

use stateright::{Checker, Model, Property};

use super::{Decision, ProducerEntry, check_pure};

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 40;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

struct ProducerModel {
    max_epoch: i16,
    max_seq: i32,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct ProdState {
    epoch: i16,
    last_sequence: i32,
    initialized: bool,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum ProdAction {
    /// Submit a single-record batch (delta 0) at `(epoch, base_sequence)`.
    Submit(i16, i32),
}

fn entry_of(s: &ProdState) -> Option<ProducerEntry> {
    if !s.initialized {
        return None;
    }
    Some(ProducerEntry {
        epoch: s.epoch,
        last_sequence: s.last_sequence,
        last_offset: 0,
        base_offset: 0,
        last_timestamp: 0,
        last_activity_ms: 0,
    })
}

impl Model for ProducerModel {
    type State = ProdState;
    type Action = ProdAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![ProdState {
            epoch: 0,
            last_sequence: -1,
            initialized: false,
        }]
    }

    fn actions(&self, _s: &Self::State, actions: &mut Vec<Self::Action>) {
        for e in 0..=self.max_epoch {
            for sq in 0..=self.max_seq {
                actions.push(ProdAction::Submit(e, sq));
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let ProdAction::Submit(epoch, base_seq) = action;
        let entry = entry_of(last);
        let decision = check_pure(entry.as_ref(), epoch, base_seq);
        let mut s = last.clone();
        match decision {
            Decision::Append => {
                if last.initialized && epoch == last.epoch {
                    assert!(
                        base_seq == last.last_sequence + 1,
                        "same-epoch Append not contiguous: base_seq={base_seq} last={}",
                        last.last_sequence
                    );
                } else if last.initialized {
                    assert!(
                        epoch > last.epoch,
                        "Append epoch not fresh: {epoch} <= {}",
                        last.epoch
                    );
                }
                s.epoch = epoch;
                s.last_sequence = base_seq;
                s.initialized = true;
                Some(s)
            }
            Decision::Duplicate { .. } => {
                assert!(
                    last.initialized && epoch == last.epoch && base_seq <= last.last_sequence,
                    "Duplicate misclassified: epoch={epoch} base_seq={base_seq} state={last:?}"
                );
                None
            }
            Decision::OutOfOrder => {
                assert!(
                    last.initialized && epoch == last.epoch && base_seq > last.last_sequence + 1,
                    "OutOfOrder misclassified: epoch={epoch} base_seq={base_seq} state={last:?}"
                );
                None
            }
            Decision::Fenced => {
                assert!(
                    last.initialized && epoch < last.epoch,
                    "Fenced misclassified: epoch={epoch} state={last:?}"
                );
                None
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // An initialized producer has accepted at least one batch, so its
            // last_sequence is a valid (>= 0) prefix end. Combined with the
            // contiguity / dedup / fencing asserts in `next_state` (checked on
            // every bounded submit), the accepted log per epoch is a gap-free,
            // duplicate-free, monotonic prefix — the idempotent-log linearizability.
            Property::always("last_sequence_valid", |_, s: &ProdState| {
                !s.initialized || s.last_sequence >= 0
            }),
            Property::always("in_bounds", |m: &ProducerModel, s: &ProdState| {
                s.last_sequence <= m.max_seq && s.epoch <= m.max_epoch
            }),
            Property::sometimes("can_dedup", |_, s: &ProdState| s.last_sequence >= 0),
            Property::sometimes("can_bump_epoch", |_, s: &ProdState| s.epoch >= 1),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.epoch <= self.max_epoch && s.last_sequence <= self.max_seq
    }
}

fn run(model: ProducerModel, label: &str) {
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
    assert!(checker.max_depth() < MAX_DEPTH, "[{label}] depth cap hit");
    assert!(
        checker.state_count() < MAX_STATES,
        "[{label}] state cap hit"
    );
    checker.assert_properties();
}

#[test]
fn producer_basic() {
    run(
        ProducerModel {
            max_epoch: 2,
            max_seq: 3,
        },
        "producer_basic",
    );
}

#[test]
fn producer_wide() {
    run(
        ProducerModel {
            max_epoch: 6,
            max_seq: 12,
        },
        "producer_wide",
    );
}
