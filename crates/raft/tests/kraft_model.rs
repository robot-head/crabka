//! Exhaustive stateright checks of the `KRaft` consensus core. See `model/mod.rs`.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each checker run is fenced with `target_state_count` + `timeout` backstops
//! (in addition to the model's tight `within_boundary`) so a runaway space
//! cannot exhaust host RAM.
//!
//! Two configs, because the linearizability tester (its history lives in the
//! fingerprinted state) blows the space up by ~30x:
//! - `three_voters_election_safety`: 3 voters, NO client appends — election +
//!   log-matching safety over the small/fast space.
//! - `two_voters_linearizable`: 2 voters, client appends — committed-log
//!   linearizability over a tightly-bounded space.
mod model;

use std::time::Duration;

use crabka_ids::NodeId;
use model::ConsensusModel;
use stateright::{Checker, Model};

/// Hard backstop on explored (generated) states — bounds memory even if
/// `within_boundary` is looser than intended. Set well above each config's true
/// bounded count so it never truncates a real check (which would spuriously
/// fail a `sometimes` witness or leave an `always` only partially verified).
const MAX_STATES: usize = 2_000_000;
/// Wall-clock backstop (generous; the configs below complete in a few seconds).
const CHECK_TIMEOUT: Duration = Duration::from_secs(90);
/// Depth backstop. Must exceed each config's reachable-graph diameter or the
/// search is depth-truncated (incomplete). The configs below are bounded so
/// their diameter sits comfortably under this.
const MAX_DEPTH: usize = 60;

fn run(model: ConsensusModel, label: &str) {
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
    // Guard against silent incompleteness: if we hit the depth or state cap, the
    // `always` properties were only partially verified — fail loudly so the
    // bounds get retuned rather than passing a non-exhaustive check.
    assert!(
        checker.max_depth() < MAX_DEPTH,
        "[{label}] hit depth cap {MAX_DEPTH}: search is depth-truncated, not exhaustive"
    );
    assert!(
        checker.state_count() < MAX_STATES,
        "[{label}] hit state cap {MAX_STATES}: search is truncated, not exhaustive"
    );
    checker.assert_properties();
}

#[test]
fn three_voters_election_safety() {
    run(
        ConsensusModel::elections(&[NodeId(1), NodeId(2), NodeId(3)]),
        "three_voters_election_safety",
    );
}

#[test]
fn two_voters_linearizable() {
    run(
        ConsensusModel::linearizable(&[NodeId(1), NodeId(2)], 2),
        "two_voters_linearizable",
    );
}

#[test]
fn three_voters_faults() {
    // Election + log-matching safety under an adversarial network: message
    // loss, duplication, and a single crash/recover. 3 voters so a crash leaves
    // a majority that can still make progress.
    run(
        ConsensusModel::faults(&[NodeId(1), NodeId(2), NodeId(3)]),
        "three_voters_faults",
    );
}
