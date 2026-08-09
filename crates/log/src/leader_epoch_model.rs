//! Exhaustive stateright enumeration of the leader-epoch truncation core
//! (`epoch_and_offset_for_entries`, KIP-101/320). This is the offset a follower
//! truncates to when it rejoins a leader. The follower must keep every record
//! both sides agree on, the committed common prefix, and drop every divergent
//! higher-epoch record. See the design spec
//! `docs/superpowers/specs/2026-06-14-crabka-data-plane-safety-models-design.md`.
//!
//! The model enumerates every bounded *authoritative leader* epoch-history,
//! that is, a history with strictly increasing epoch and `start_offset`, **with
//! gaps allowed** so that the floor-epoch branch runs. From each history it
//! probes the function with every requested epoch, including `UNDEFINED` and
//! future epochs, and with a small window of follower log-end offsets. A probe
//! models a follower that holds the leader's records up to `leo` and asks
//! "given my last epoch, where do I truncate?". The per-transition asserts
//! encode the safety contract:
//!
//!   * **valid target** — the truncation offset is always `>= 0`.
//!   * **resolved epoch never exceeds requested** — `found_epoch <= requested`.
//!   * **committed prefix preserved** (HEADLINE) — for a requested epoch that
//!     the leader also holds, the truncation offset is `>=` that epoch's start,
//!     so nothing ever truncates a record in the agreed range.
//!   * **divergent suffix removed** — for an agreed *non-latest* epoch, the
//!     truncation offset is exactly the next leader epoch's start, which is
//!     `<= leo`, so every divergent higher-epoch record is dropped.
//!
//! In the follower-ahead case the requested epoch is above the leader's latest.
//! That case is the iterative KIP-320 step-back, not a single-call decision. The
//! function returns `(UNDEFINED, leo)` there, so there is no truncation this
//! round, and the model asserts only the valid-target and resolved-epoch floor
//! for it.

use crabka_ids::{LeaderEpoch, Offset};
use crabka_units::prelude::{Time, TimeExt as _, minutes};
use stateright::{Checker, Model, Property};

use super::{EpochEntry, UNDEFINED_EPOCH, epoch_and_offset_for_entries};

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 40;
/// Wall-clock budget for the exhaustive BFS. It is a runaway guard, not a
/// bound on the model.
const CHECK_TIMEOUT: Time = minutes(2);

struct EpochModel {
    max_epoch: i32,
    max_offset: i64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct EpochState {
    /// Authoritative leader epoch-history, with strictly increasing epoch and
    /// offset.
    leader: Vec<EpochEntry>,
    /// Non-vacuity witnesses. Probes set them, and they move only from false
    /// to true.
    saw_truncation: bool,
    saw_gap: bool,
    saw_future: bool,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum EpochAction {
    /// Leader records a new epoch boundary at `(epoch, start_offset)`.
    LeaderAppend(i32, i64),
    /// A follower with log-end `leo` and last epoch `requested` asks where to
    /// truncate.
    Probe(i32, i64),
}

fn is_monotonic(h: &[EpochEntry]) -> bool {
    h.windows(2)
        .all(|w| w[0].epoch < w[1].epoch && w[0].start_offset < w[1].start_offset)
}

impl Model for EpochModel {
    type State = EpochState;
    type Action = EpochAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![EpochState {
            leader: vec![],
            saw_truncation: false,
            saw_gap: false,
            saw_future: false,
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        // Grow the leader history. Epoch and offset may each jump by 1 or 2 so
        // the enumeration covers gap epochs (the floor-epoch resolution branch).
        match s.leader.last() {
            None => {
                // Seed: first epoch boundary at offset 0, epoch 0 or 1.
                for e in 0..=1.min(self.max_epoch) {
                    actions.push(EpochAction::LeaderAppend(e, 0));
                }
            }
            Some(last) => {
                for de in 1..=2 {
                    for doff in 1..=2 {
                        let ne = last.epoch.0 + de;
                        let no = last.start_offset.0 + doff;
                        if ne <= self.max_epoch && no <= self.max_offset {
                            actions.push(EpochAction::LeaderAppend(ne, no));
                        }
                    }
                }
            }
        }
        // Probe from any non-empty leader history with every requested epoch
        // (UNDEFINED..=future) and a small follower log-end window.
        if let Some(last) = s.leader.last() {
            // Enumerate requested epochs as raw `i32`s (the KIP-320 wire type),
            // from `UNDEFINED` through one past the max; they are wrapped into
            // `LeaderEpoch` when a probe is applied.
            for requested in UNDEFINED_EPOCH.0..=(self.max_epoch + 1) {
                for dleo in 1..=2 {
                    actions.push(EpochAction::Probe(requested, last.start_offset.0 + dleo));
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        match action {
            EpochAction::LeaderAppend(epoch, off) => {
                let mut s = last.clone();
                s.leader.push(EpochEntry {
                    epoch: LeaderEpoch(epoch),
                    start_offset: Offset(off),
                });
                Some(s)
            }
            EpochAction::Probe(requested, leo) => {
                // `requested` is enumerated as the raw KIP-320 wire `int32`;
                // wrap it into the domain newtype for the call and comparisons.
                let requested = LeaderEpoch(requested);
                let (found, trunc) =
                    epoch_and_offset_for_entries(&last.leader, requested, Offset(leo));
                let latest = last.leader.iter().map(|e| e.epoch).max();

                // Contract: always a valid truncation target.
                assert2::assert!(trunc >= 0);
                // The resolved epoch never exceeds the requested epoch.
                assert2::assert!(found <= requested);

                let recorded = last.leader.iter().find(|e| e.epoch == requested);
                if let Some(entry) = recorded {
                    // Committed-prefix-preserved: never truncate below the start
                    // of an epoch the follower and leader agree on.
                    assert2::assert!(trunc >= entry.start_offset);
                    if latest == Some(requested) {
                        // Current epoch → keep up to the follower's log end.
                        assert2::assert!(found == requested && trunc == leo);
                    } else {
                        // Older agreed epoch → truncate to the next leader
                        // epoch's start, dropping the divergent higher-epoch
                        // suffix. That start is <= the follower's log end.
                        let next_start = last
                            .leader
                            .iter()
                            .filter(|e| e.epoch > requested)
                            .map(|e| e.start_offset)
                            .min()
                            .expect("a non-latest recorded epoch has a higher epoch");
                        assert2::assert!(found == requested && trunc == next_start);
                        assert2::assert!(trunc <= leo);
                    }
                }

                // Record non-vacuity witnesses.
                let mut s = last.clone();
                let mut changed = false;
                if trunc < leo && !s.saw_truncation {
                    s.saw_truncation = true;
                    changed = true;
                }
                if recorded.is_none()
                    && requested != UNDEFINED_EPOCH
                    && found != UNDEFINED_EPOCH
                    && found < requested
                    && !s.saw_gap
                {
                    s.saw_gap = true;
                    changed = true;
                }
                if latest.is_some_and(|l| requested > l)
                    && found == UNDEFINED_EPOCH
                    && !s.saw_future
                {
                    s.saw_future = true;
                    changed = true;
                }
                changed.then_some(s)
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // The enumeration only ever builds strictly-increasing histories.
            Property::always("leader_monotonic", |_, s: &EpochState| {
                is_monotonic(&s.leader)
            }),
            // A real truncation (trunc strictly below the follower's log end)
            // is reachable — the divergent-suffix-removal path is non-vacuous.
            Property::sometimes("can_truncate", |_, s: &EpochState| s.saw_truncation),
            // A gap epoch resolves to a lower floor epoch.
            Property::sometimes("can_resolve_gap", |_, s: &EpochState| s.saw_gap),
            // A future (follower-ahead) epoch resolves to UNDEFINED.
            Property::sometimes("can_be_future", |_, s: &EpochState| s.saw_future),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        let cap = usize::try_from(self.max_epoch).unwrap_or(0) + 2;
        s.leader.len() <= cap
    }
}

fn run(model: EpochModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .timeout(CHECK_TIMEOUT.to_std())
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
fn truncation_basic() {
    run(
        EpochModel {
            max_epoch: 3,
            max_offset: 5,
        },
        "truncation_basic",
    );
}

#[test]
fn truncation_wide() {
    run(
        EpochModel {
            max_epoch: 5,
            max_offset: 9,
        },
        "truncation_wide",
    );
}
