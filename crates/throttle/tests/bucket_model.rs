//! Exhaustive stateright shared-memory interleaving model of the live
//! `crabka_throttle::TokenBucket` concurrency.
//!
//! The model drives the production [`crabka_throttle::plan_consume`]
//! arithmetic. The state is the shared `{rate, available, pending}` atomics,
//! modeled small, plus a seqlock `generation` and a per-thread program counter
//! for each in-flight `try_consume` or `set_rate`. `available` is an `i64`, so
//! the buggy underflow shows up as a catchable negative value. Actions
//! interleave one atomic step at a time.
//!
//! A `cas` flag selects the algorithm:
//!
//! * `false` reproduces the OLD buggy read-modify-write. `Load`, `Store`, and
//!   `Sub` are three interleavable steps, and `Sub` can drive `available`
//!   negative.
//! * `true` models the FIXED seqlock CAS commit. The consume samples the
//!   generation, and commits atomically *only if no `set_rate` reset straddled*
//!   its claim window. A straddling reset changes the generation and forces the
//!   consume to re-base against the post-reset `{available, pending}`. The
//!   consume thus does not clobber the freshly reset `available` with a stale
//!   CAS. This is the net effect of the `compare_exchange_weak` loop under the
//!   generation guard.
//!
//! The model treats `set_rate` as a seqlock critical section. It enters the
//! section and makes the generation odd. It then does three interleavable
//! stores: `rate`, `available`, and a reset of `pending`. It then leaves the
//! section and makes the generation even, advanced by 2. The buggy path
//! violates the headline invariant `0 <= available <= max_rate`, and the RED
//! witness `race_underflows_without_cas` discovers a counterexample. The CAS
//! path holds the invariant even with a concurrent `set_rate`. The GREEN cases
//! are `bucket_basic` and `bucket_wide`. See the design spec
//! `docs/superpowers/specs/2026-06-14-crabka-token-bucket-quota-model-design.md`.

use std::time::Duration;

use crabka_throttle::{
    AvailableTokens, BurstCapacity, RefillTokens, RequestedTokens, plan_consume,
};
use stateright::{Checker, Model, Property};

const TARGET_STATE_COUNT: usize = 4_000_000;
const MAX_DEPTH: usize = 60;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum Pc {
    Idle,
    // try_consume:
    Claimed {
        refill: i64,
        req: i64,
        gen_before: i64,
    }, // claimed refill + sampled generation; next: Load (buggy) / CommitCas (fixed)
    Loaded {
        refill: i64,
        req: i64,
        cur: i64,
    }, // buggy: read `available`; next: Store
    Stored {
        observed: i64,
        req: i64,
    }, // buggy: stored capped avail; next: Sub
    // set_rate (seqlock critical section: enter, three stores, leave):
    SetRate0 {
        new_rate: i64,
    }, // next: store rate
    SetRate1 {
        new_rate: i64,
    }, // next: store available
    SetRate2, // next: reset pending
    SetRate3, // next: leave critical section (generation -> even)
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct BucketState {
    rate: i64,
    available: i64,
    pending: i64,
    /// Seqlock generation: odd while a `set_rate` critical section is open.
    generation: i64,
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
            generation: 0,
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
                s.pcs[t] = Pc::Claimed {
                    refill,
                    req,
                    gen_before: s.generation,
                };
            }
            Act::StartSetRate(t, new_rate) => {
                // Enter the seqlock critical section (generation becomes odd).
                s.generation += 1;
                s.pcs[t] = Pc::SetRate0 { new_rate };
            }
            Act::Step(t) => match s.pcs[t].clone() {
                Pc::Idle => return None,
                Pc::Claimed {
                    refill,
                    req,
                    gen_before,
                } => {
                    if self.cas {
                        // Fixed: commit only if no reset straddled the claim. A
                        // changed (or currently-odd) generation forces a re-base
                        // against the post-reset state instead of a stale CAS.
                        if s.generation == gen_before {
                            // Atomic read-compute-write (net effect of the CAS loop),
                            // driving the real production arithmetic.
                            let (_grant, new) = plan_consume(
                                AvailableTokens(s.available.max(0).cast_unsigned()),
                                RefillTokens(refill.cast_unsigned()),
                                BurstCapacity(s.rate.cast_unsigned()),
                                RequestedTokens(req.cast_unsigned()),
                            );
                            s.available = new.0.cast_signed();
                            s.pcs[t] = Pc::Idle;
                        } else {
                            // Re-claim refill from the current pending and resample
                            // the generation — the production retry path.
                            let new_refill = s.pending;
                            s.pending = 0;
                            s.pcs[t] = Pc::Claimed {
                                refill: new_refill,
                                req,
                                gen_before: s.generation,
                            };
                        }
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
                    s.pcs[t] = Pc::SetRate3;
                }
                Pc::SetRate3 => {
                    // Leave the critical section (generation becomes even again,
                    // advanced by 2 total so any straddling reader observes a change).
                    s.generation += 1;
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
                let set = s.pcs.iter().any(|p| {
                    matches!(
                        p,
                        Pc::SetRate0 { .. } | Pc::SetRate1 { .. } | Pc::SetRate2 | Pc::SetRate3
                    )
                });
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
    // Bounded model check: `target_max_depth` / `target_state_count` / `timeout`
    // are hard exploration caps (project policy: never run stateright unbounded —
    // it OOM'd once). We do NOT require the search to be exhaustive; within the
    // explored envelope we assert the `available_in_range` safety property is
    // never violated and every non-vacuity (`sometimes`) witness is reached. The
    // companion `race_underflows_without_cas` RED witness proves the model still
    // detects a genuine violation on the buggy non-CAS path.
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

/// RED witness: the OLD non-CAS algorithm violates `available_in_range` in the
/// over-grant and underflow race. This test asserts that the checker DISCOVERS
/// a counterexample.
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
    assert2::assert!(checker.discovery("available_in_range").is_some());
}
