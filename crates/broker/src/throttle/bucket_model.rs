//! Exhaustive stateright shared-memory interleaving model of the KIP-73
//! `TokenBucket` concurrency. State = the shared `{rate, available, pending}`
//! atomics (modeled small; `available` is `i64` so the buggy underflow shows up
//! as a catchable negative) + a per-thread program counter for each in-flight
//! `try_consume` / `set_rate`. Actions interleave one atomic step at a time.
//!
//! A `cas` flag selects the algorithm: `false` reproduces the OLD buggy
//! read-modify-write (`Load` → `Store` → `Sub` as three interleavable steps,
//! where `Sub` can drive `available` negative); `true` models the fixed CAS
//! commit as one atomic read-compute-write step (the net effect of the
//! `compare_exchange_weak` loop, driving the real [`super::plan_consume`]).
//! `set_rate` is three interleavable stores (`rate`, `available`, reset
//! `pending`) in both modes. The headline invariant `0 <= available <= max_rate`
//! is violated by the buggy path (the RED witness `race_underflows_without_cas`
//! discovers a counterexample) and held by the CAS path even with concurrent
//! `set_rate` (GREEN: `bucket_basic` / `bucket_wide`). See the design spec
//! `docs/superpowers/specs/2026-06-14-crabka-token-bucket-quota-model-design.md`.

use std::time::Duration;

use stateright::{Checker, Model, Property};

use super::plan_consume;

const TARGET_STATE_COUNT: usize = 4_000_000;
const MAX_UNIQUE_STATES: usize = 500_000;
const MAX_DEPTH: usize = 60;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum Pc {
    Idle,
    // try_consume:
    Claimed { refill: i64, req: i64 }, // claimed refill; next: Load (buggy) / CommitCas (fixed)
    Loaded { refill: i64, req: i64, cur: i64 }, // buggy: read `available`; next: Store
    Stored { observed: i64, req: i64 }, // buggy: stored capped avail; next: Sub
    // set_rate (three non-atomic stores):
    SetRate0 { new_rate: i64 }, // next: store rate
    SetRate1 { new_rate: i64 }, // next: store available
    SetRate2,                   // next: reset pending
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct BucketState {
    rate: i64,
    available: i64,
    pending: i64,
    pcs: Vec<Pc>,
}

struct BucketModel {
    cas: bool,
    threads: usize,
    max_rate: i64,
    max_req: i64,
    max_pending: i64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum Act {
    Tick,
    StartConsume(usize, i64), // (thread, requested)
    StartSetRate(usize, i64), // (thread, new_rate)
    Step(usize),              // advance thread's in-flight op by one atomic step
}

impl Model for BucketModel {
    type State = BucketState;
    type Action = Act;

    fn init_states(&self) -> Vec<Self::State> {
        vec![BucketState {
            rate: self.max_rate,
            available: self.max_rate,
            pending: 0,
            pcs: vec![Pc::Idle; self.threads],
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        if s.pending < self.max_pending {
            actions.push(Act::Tick);
        }
        for (t, pc) in s.pcs.iter().enumerate() {
            if matches!(pc, Pc::Idle) {
                for req in 0..=self.max_req {
                    actions.push(Act::StartConsume(t, req));
                }
                for nr in [0, self.max_rate] {
                    actions.push(Act::StartSetRate(t, nr));
                }
            } else {
                actions.push(Act::Step(t));
            }
        }
    }

    #[allow(
        clippy::cast_sign_loss,
        clippy::cast_possible_wrap,
        clippy::too_many_lines
    )]
    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            Act::Tick => {
                s.pending = (s.pending + 1).min(self.max_pending);
            }
            Act::StartConsume(t, req) => {
                // rate==0 fast path grants instantly without touching `available`.
                if s.rate == 0 {
                    return None; // irrelevant to the race; no state change
                }
                let refill = s.pending; // claim the elapsed gap (the atomic swap)
                s.pending = 0;
                s.pcs[t] = Pc::Claimed { refill, req };
            }
            Act::StartSetRate(t, new_rate) => {
                s.pcs[t] = Pc::SetRate0 { new_rate };
            }
            Act::Step(t) => match s.pcs[t].clone() {
                Pc::Idle => return None,
                Pc::Claimed { refill, req } => {
                    if self.cas {
                        // Fixed: atomic read-compute-write (net effect of the CAS loop),
                        // driving the real production arithmetic.
                        let (_grant, new) = plan_consume(
                            s.available.max(0) as u64,
                            refill as u64,
                            s.rate as u64,
                            req as u64,
                        );
                        s.available = new as i64;
                        s.pcs[t] = Pc::Idle;
                    } else {
                        s.pcs[t] = Pc::Loaded {
                            refill,
                            req,
                            cur: s.available,
                        };
                    }
                }
                Pc::Loaded { refill, req, cur } => {
                    // Buggy: store the capped available (a plain store).
                    let capped = (cur + refill).min(s.rate);
                    s.available = capped;
                    s.pcs[t] = Pc::Stored {
                        observed: capped,
                        req,
                    };
                }
                Pc::Stored { observed, req } => {
                    // Buggy: fetch_sub(grant) on the CURRENT available — can go negative.
                    let grant = req.min(observed);
                    s.available -= grant;
                    s.pcs[t] = Pc::Idle;
                }
                Pc::SetRate0 { new_rate } => {
                    s.rate = new_rate;
                    s.pcs[t] = Pc::SetRate1 { new_rate };
                }
                Pc::SetRate1 { new_rate } => {
                    s.available = new_rate;
                    s.pcs[t] = Pc::SetRate2;
                }
                Pc::SetRate2 => {
                    s.pending = 0;
                    s.pcs[t] = Pc::Idle;
                }
            },
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // HEADLINE: available never underflows (< 0) nor exceeds the burst cap.
            // The buggy fetch_sub drives it negative (RED); the CAS path holds it.
            Property::always("available_in_range", |m: &BucketModel, s: &BucketState| {
                0 <= s.available && s.available <= m.max_rate
            }),
            // Non-vacuity: >= 2 threads in flight at once (real interleaving).
            Property::sometimes("concurrent_inflight", |_, s: &BucketState| {
                s.pcs.iter().filter(|p| !matches!(p, Pc::Idle)).count() >= 2
            }),
            // Non-vacuity: a set_rate overlaps an in-flight consume.
            Property::sometimes("setrate_during_consume", |_, s: &BucketState| {
                let set = s
                    .pcs
                    .iter()
                    .any(|p| matches!(p, Pc::SetRate0 { .. } | Pc::SetRate1 { .. } | Pc::SetRate2));
                let con = s.pcs.iter().any(|p| {
                    matches!(
                        p,
                        Pc::Claimed { .. } | Pc::Loaded { .. } | Pc::Stored { .. }
                    )
                });
                set && con
            }),
            // Non-vacuity: the bucket gets fully drained.
            Property::sometimes("can_drain", |_, s: &BucketState| s.available == 0),
        ]
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        // Permit a small negative range so the underflow counterexample is
        // discovered (not pruned before the assert fires).
        s.available >= -(self.max_rate + self.max_req + 1)
            && s.available <= self.max_rate
            && s.pending <= self.max_pending
    }
}

fn green_run(model: BucketModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(TARGET_STATE_COUNT)
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
        checker.state_count() < TARGET_STATE_COUNT,
        "[{label}] truncated — not exhaustive"
    );
    assert!(
        checker.unique_state_count() < MAX_UNIQUE_STATES,
        "[{label}] unique-state bound exceeded ({})",
        checker.unique_state_count()
    );
    checker.assert_properties();
}

#[test]
fn bucket_basic() {
    green_run(
        BucketModel {
            cas: true,
            threads: 2,
            max_rate: 2,
            max_req: 2,
            max_pending: 1,
        },
        "bucket_basic",
    );
}

#[test]
fn bucket_wide() {
    green_run(
        BucketModel {
            cas: true,
            threads: 2,
            max_rate: 3,
            max_req: 3,
            max_pending: 2,
        },
        "bucket_wide",
    );
}

/// RED witness: the OLD non-CAS algorithm violates `available_in_range` (the
/// over-grant / underflow race). Assert that a counterexample is DISCOVERED.
#[test]
fn race_underflows_without_cas() {
    let checker = BucketModel {
        cas: false,
        threads: 2,
        max_rate: 1,
        max_req: 1,
        max_pending: 0,
    }
    .checker()
    .target_max_depth(MAX_DEPTH)
    .target_state_count(TARGET_STATE_COUNT)
    .timeout(CHECK_TIMEOUT)
    .spawn_bfs()
    .join();
    assert!(
        checker.discovery("available_in_range").is_some(),
        "expected the non-CAS try_consume to violate available_in_range (underflow/over-grant)"
    );
}
