//! Exhaustive stateright enumeration of the fetch read-path visibility decision
//! (`super::compute_visibility_window`).
//!
//! The state is the advancing partition watermarks
//! `{log_start, hw, lso, log_end}`, with the Kafka invariant
//! `0 <= log_start <= lso <= hw <= log_end`. `Advance*` actions raise them
//! monotonically: appends raise LEO, ISR catch-up raises HW, txn commits raise
//! LSO, and retention raises `log_start`. `Fetch` probes drive the real
//! decision.
//!
//! For each `Fetch` the model asserts the clamp contract. A consumer fetch
//! never exposes an offset beyond the high-watermark, so there is no dirty
//! read. The broker clamps a `read_committed` consumer at `lso.min(hw)`, and it
//! serves a follower up to the log-end. The model also asserts the
//! single-source-of-truth response-field contract, the de-dup'd hazard from
//! `do_read`. For each `Advance*` the model asserts KIP-227 monotonicity: the
//! reported HW/LSO never regress as the log progresses. See the design spec
//! `docs/superpowers/specs/2026-06-14-crabka-fetch-hwm-visibility-model-design.md`.

use std::time::Duration;

use stateright::{Checker, Model, Property};

use super::{Offset, compute_visibility_window};

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 40;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

struct VisModel {
    max_offset: i64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct VisState {
    log_start: i64,
    hw: i64,
    lso: i64,
    log_end: i64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum VisAction {
    AdvanceLogEnd,
    AdvanceHw,
    AdvanceLso,
    AdvanceLogStart,
    /// `(is_follower, read_committed, fetch_offset)`. `read_committed` implies
    /// `!is_follower`.
    Fetch(bool, bool, i64),
}

/// Mirror of the `response_hw` formula of the fn. This is the contract that
/// the model asserts.
fn response_hw(is_follower: bool, hw: i64, log_end: i64) -> i64 {
    if is_follower { log_end } else { hw }
}

/// Mirror of the fn's `response_lso` formula.
fn response_lso(is_follower: bool, read_committed: bool, hw: i64, lso: i64, log_end: i64) -> i64 {
    if read_committed && !is_follower {
        lso.min(hw)
    } else if is_follower {
        log_end
    } else {
        hw
    }
}

impl Model for VisModel {
    type State = VisState;
    type Action = VisAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![VisState {
            log_start: 0,
            hw: 0,
            lso: 0,
            log_end: 0,
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        // Advance watermarks, preserving 0 <= log_start <= lso <= hw <= log_end
        // <= max_offset.
        if s.log_end < self.max_offset {
            actions.push(VisAction::AdvanceLogEnd);
        }
        if s.hw < s.log_end {
            actions.push(VisAction::AdvanceHw);
        }
        if s.lso < s.hw {
            actions.push(VisAction::AdvanceLso);
        }
        if s.log_start < s.lso {
            actions.push(VisAction::AdvanceLogStart);
        }
        // Probe every fetch shape over a bounded fetch-offset window (incl. one
        // past log_end so the empty/out-of-range edges are exercised).
        for fo in 0..=(self.max_offset + 1) {
            actions.push(VisAction::Fetch(false, false, fo)); // consumer, read_uncommitted
            actions.push(VisAction::Fetch(false, true, fo)); // consumer, read_committed
            actions.push(VisAction::Fetch(true, false, fo)); // follower
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        match action {
            VisAction::AdvanceLogEnd => {
                let mut s = last.clone();
                s.log_end += 1;
                assert_monotonic(last, &s);
                Some(s)
            }
            VisAction::AdvanceHw => {
                let mut s = last.clone();
                s.hw += 1;
                assert_monotonic(last, &s);
                Some(s)
            }
            VisAction::AdvanceLso => {
                let mut s = last.clone();
                s.lso += 1;
                assert_monotonic(last, &s);
                Some(s)
            }
            VisAction::AdvanceLogStart => {
                // log_start advancing never lowers response_hw/lso.
                let mut s = last.clone();
                s.log_start += 1;
                Some(s)
            }
            VisAction::Fetch(is_follower, read_committed, fetch_offset) => {
                let w = compute_visibility_window(
                    is_follower,
                    read_committed,
                    Offset(last.log_start),
                    Offset(last.hw),
                    Offset(last.lso),
                    Offset(last.log_end),
                    Offset(fetch_offset),
                );
                assert_fetch_contract(last, is_follower, read_committed, fetch_offset, &w);
                None // probes never change state
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("watermarks_ordered", |_, s: &VisState| {
                0 <= s.log_start && s.log_start <= s.lso && s.lso <= s.hw && s.hw <= s.log_end
            }),
            // A read_committed clamp strictly below HW is reachable (lso < hw).
            Property::sometimes("can_clamp_lso", |_, s: &VisState| s.lso < s.hw),
            // A follower can be served strictly beyond HW (hw < log_end).
            Property::sometimes("follower_beyond_hw", |_, s: &VisState| s.hw < s.log_end),
            // OFFSET_OUT_OF_RANGE is reachable (log_start > 0 ⟹ a sub-log_start
            // fetch_offset exists).
            Property::sometimes("can_out_of_range", |_, s: &VisState| s.log_start > 0),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.log_end <= self.max_offset
    }
}

/// KIP-227: a watermark advance must never lower the reported HW/LSO for any
/// fixed fetch shape.
fn assert_monotonic(old: &VisState, new: &VisState) {
    for &fol in &[false, true] {
        for &rc in &[false, true] {
            if rc && fol {
                continue; // read_committed implies !follower
            }
            assert!(
                response_hw(fol, new.hw, new.log_end) >= response_hw(fol, old.hw, old.log_end),
                "response_hw regressed on advance (follower={fol})"
            );
            assert!(
                response_lso(fol, rc, new.hw, new.lso, new.log_end)
                    >= response_lso(fol, rc, old.hw, old.lso, old.log_end),
                "response_lso regressed on advance (follower={fol}, read_committed={rc})"
            );
        }
    }
}

fn assert_fetch_contract(
    s: &VisState,
    is_follower: bool,
    read_committed: bool,
    fetch_offset: i64,
    w: &super::VisibilityWindow,
) {
    // Unwrap the `Offset` window fields into this model's `i64` world.
    let limit_offset = w.limit_offset.0;
    let win_response_hw = w.response_hw.0;
    let win_response_lso = w.response_lso.0;
    let effective_lso = w.effective_lso.0;
    // Valid targets.
    assert!(limit_offset >= 0 && win_response_hw >= 0 && win_response_lso >= 0);
    // out_of_range / empty correctness.
    assert_eq!(w.out_of_range, (fetch_offset < s.log_start));
    let upper = if is_follower { s.log_end } else { s.hw };
    if !w.out_of_range {
        assert_eq!(w.empty, (fetch_offset >= upper));
    }
    // Response single-source-of-truth contract (OOR and success paths share it).
    assert_eq!(win_response_hw, response_hw(is_follower, s.hw, s.log_end));
    assert_eq!(
        win_response_lso,
        response_lso(is_follower, read_committed, s.hw, s.lso, s.log_end)
    );
    if is_follower {
        // Follower bound: serve up to the log-end (>= hw).
        assert!(limit_offset == s.log_end && limit_offset >= s.hw);
    } else {
        // No dirty read: never expose beyond the high-watermark.
        assert!(limit_offset <= s.hw, "consumer fetch exposed beyond HW");
        assert!(win_response_lso <= win_response_hw);
        if read_committed {
            assert_eq!(effective_lso, s.lso.min(s.hw));
            assert!(limit_offset <= s.lso.min(s.hw));
        }
    }
}

fn run(model: VisModel, label: &str) {
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
fn visibility_basic() {
    run(VisModel { max_offset: 4 }, "visibility_basic");
}

#[test]
fn visibility_wide() {
    run(VisModel { max_offset: 7 }, "visibility_wide");
}
