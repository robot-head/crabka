//! Exhaustive stateright model of the pure KIP-932 share-partition acquisition
//! core (`AcquisitionState`). See
//! `docs/superpowers/specs/2026-06-13-crabka-share-group-model-design.md`.
//!
//! The remaining model (types, `Model` impl, properties, configs) is added in
//! later tasks. This file starts as a derive/wiring smoke test.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each checker run is fenced with `within_boundary` + `target_state_count` +
//! `timeout`, and MUST be run under the host memory watchdog while bounds are
//! being tuned (never unguarded).

use super::{AckType, AcquisitionState, RecordState};

#[test]
fn derives_compile() {
    // Build a small machine, exercise it, then prove Clone + Eq + Hash work
    // (these are what let the real machine live in a fingerprinted model state).
    use std::collections::HashSet;
    use std::time::{Duration, Instant};

    let mut s = AcquisitionState::new(0);
    s.materialize(2, 100);
    let _ = s.acquire("m0", 1, i32::MAX, Instant::now(), Duration::from_secs(1), 2);

    let clone = s.clone();
    assert_eq!(s, clone);

    let mut set: HashSet<AcquisitionState> = HashSet::new();
    set.insert(s);
    assert!(set.contains(&clone));

    // Touch the imported enums so the import is used and Hash is exercised.
    let mut codes: HashSet<(RecordState, AckType)> = HashSet::new();
    codes.insert((RecordState::Acquired, AckType::Accept));
    assert!(codes.contains(&(RecordState::Acquired, AckType::Accept)));
}
