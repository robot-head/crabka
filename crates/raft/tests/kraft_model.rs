//! Exhaustive stateright checks of the `KRaft` consensus core. See `model/mod.rs`.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each checker run is fenced with `target_state_count` + `timeout` backstops
//! (in addition to the model's tight `within_boundary`) so a runaway space
//! cannot exhaust host RAM.
mod model;

use std::time::Duration;

use model::ConsensusModel;
use stateright::{Checker, Model};

/// Hard backstop on explored states — bounds memory even if `within_boundary`
/// is looser than intended. Set well above the true bounded count so it never
/// truncates a real check (which would spuriously fail a `sometimes` witness).
const MAX_STATES: usize = 400_000;
/// Wall-clock backstop.
const CHECK_TIMEOUT: Duration = Duration::from_secs(90);

#[test]
fn two_voters_smoke() {
    let checker = ConsensusModel::new(&[1, 2])
        .checker()
        .target_max_depth(20)
        .target_state_count(MAX_STATES)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[two_voters_smoke] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    checker.assert_properties();
}

#[test]
fn three_voters_election_safety() {
    let checker = ConsensusModel::new(&[1, 2, 3])
        .checker()
        .target_max_depth(20)
        .target_state_count(MAX_STATES)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[three_voters_election_safety] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    checker.assert_properties();
}
