//! Exhaustive stateright model of the pure leader-side replication core
//! (`ReplicaState`). See
//! `docs/superpowers/specs/2026-06-13-crabka-isr-replica-state-model-design.md`.
//!
//! The full model (types, `Model` impl, properties, configs) is added in
//! Task 2. This file starts as a wiring smoke test.

use std::time::Instant;

use super::ReplicaState;

#[test]
fn core_wiring_smoke() {
    // Drive the real core with an injected `now`, proving the descendant module
    // can reach the pub(crate) surface + that `now` injection compiles.
    let t0 = Instant::now();
    let mut s = ReplicaState::new();
    s.install_isr(&[1, 2, 3], &[1, 2, 3], 1, t0);
    let hw = s.update_follower_leo(2, 5, 10, t0);
    assert_eq!(hw, 0); // follower 3 still at LEO 0 pins the HW
    let clone = s.clone();
    assert_eq!(clone.hw, s.hw);
}
