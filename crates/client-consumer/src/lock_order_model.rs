//! Exhaustive stateright model of the classic consumer's async-Mutex lock
//! protocol, to settle whether the `poll()` ↔ coordinator-task lock dance can
//! deadlock (a lock-order cycle) or is provably deadlock-free.
//!
//! ## Why this model exists
//!
//! A WAL consumer (logs-compactor) hangs at cold start with an
//! idle-runtime / lost-wakeup signature. Two hypotheses: (a) a lock-order
//! deadlock between the poll loop and the background coordinator task, or
//! (b) a lost-wakeup that is *not* a lock cycle. This model formally rules one
//! of them in. If the protocol is deadlock-free under exhaustive search, the
//! investigation should redirect to the lost-wakeup path.
//!
//! ## Fidelity (the crabka stateright program's cardinal rule)
//!
//! Every modeled lock edge is extracted from the real source and cited below by
//! `file:line`. Nothing here is invented. The model abstracts away *values*
//! (offsets, partitions, RPC payloads) and the network — it keeps only what a
//! deadlock can possibly depend on: **which task holds which `tokio::sync::Mutex`
//! and where each task is suspended.**
//!
//! ## The five shared `tokio::sync::Mutex`es
//!
//! Declared on `Consumer` (`consumer.rs:50-63`) and shared (`Arc::clone`) with
//! `CoordinatorState` (`coordinator.rs:304-307`):
//!
//! | id | field          | abbrev |
//! |----|----------------|--------|
//! | 0  | `pending_seeks`| PS     |
//! | 1  | `assigned`     | A      |
//! | 2  | `next_offsets` | N      |
//! | 3  | `positions`    | P      |
//! | 4  | `topic_ids`    | T      |
//!
//! ## Modeled lock-holding regions (sequences where >1 guard is alive at once,
//! plus single-lock regions for completeness). Citations are to the real code.
//!
//! Critically: **no guard is ever held across an `.await` that is not itself a
//! `.lock().await`** — the code is explicit about dropping guards before every
//! RPC. So a region is a contiguous run of nested `.lock().await` acquisitions;
//! the only suspension point *inside* a region is the next lock acquisition.
//! That acquisition is exactly where a deadlock cycle would form, so it is the
//! thing this model interleaves.
//!
//! ### poll task (`poll.rs`, `seek.rs`, `validate.rs`)
//! - `apply_pending_seeks` (seek.rs): PS(:96 fast-path, released) → A(:100
//!   clone, released) → **PS(:101) → N(:105) → P(:106)** held together, all
//!   released at scope end (:123). Region edges: PS→N, N→P.
//! - `resolve_latest_sentinels` (poll.rs:559): **N alone**, held across a
//!   `ListOffsets` `.await` at :572 — but no second lock is taken in the region,
//!   so it cannot be half of a cycle. Modeled as N-alone.
//! - `refresh_leader_epochs` (validate.rs:69): **P alone** (after the metadata
//!   `.await`).
//! - `validate_positions` (validate.rs:95→96): **N→P** held together, released
//!   at :120 before the RPC; then **P alone** at :150 (post-RPC).
//! - poll fetch-build (poll.rs:209→210): **N→P** held together, released at :239
//!   before the Fetch `.await` (comment :203-206).
//! - poll post-fetch (poll.rs): A(:296 clone, released) → **N(:304) held across
//!   the whole processing loop**, and inside it P is acquired *second* at
//!   :374, :396, :523 — i.e. **N→P every time** (comment :519-520 "offsets is
//!   already locked, positions acquired second"). VERIFIED: there is **no P→N
//!   inversion** on the post-fetch path. N released at :531 before the metadata
//!   refresh `.await`.
//!
//! ### coordinator task (`coordinator.rs`)
//! - `rejoin`: A alone in every scope (:513 clone, :533-535, :559-565, :573-575,
//!   :597 clone, :614-616). It also runs **N→P** scopes (:537→:541 eager,
//!   :587→:588 cooperative). A is never held while N or P is acquired.
//! - `commit_revoked` (:635→:636): **N→P**; then T alone (:653 clone).
//! - `prime_offsets` (:976 T clone) → **N(:987) → P(:988)**.
//! - `join_and_sync` (:886): **T alone**.
//!
//! ### commit task (`commit.rs`)
//! - `commit_sync` (:70 N clone, :74 P then :76 drop, :77 T clone) and the
//!   `commit_async` spawned task (:129/:133-135/:136): **at most one lock held
//!   at a time** — N, then P, then T, each released before the next. No
//!   multi-lock region, so the commit task can never be half of a cycle. Modeled
//!   as three independent single-lock regions to confirm this.
//!
//! ## The lock hierarchy these regions imply
//!
//! Collecting every "hold L1 while acquiring L2" edge actually observed:
//!   PS → N,  PS → P (transitively),  N → P.
//! A and T are only ever held *alone* (never with another lock live). So the
//! partial order is `PS < N < P` with `A` and `T` incomparable singletons. This
//! is acyclic ⇒ the prediction is **deadlock-free**, and the model proves it
//! exhaustively across all task interleavings.

#![allow(clippy::similar_names)]

use std::time::Duration;

use stateright::{Checker, Model, Property};

/// Lock identifiers. Order here is incidental — the model does not assume any
/// hierarchy; it discovers cycles purely from the acquire/release sequences.
const PS: u8 = 0; // pending_seeks
const A: u8 = 1; // assigned
const N: u8 = 2; // next_offsets
const P: u8 = 3; // positions
const T: u8 = 4; // topic_ids
const NUM_LOCKS: usize = 5;

/// A single lock operation in a task's program. `Acquire` is a suspension point
/// (`.lock().await`); `Release` drops a guard (end of scope / explicit `drop`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Op {
    Acquire(u8),
    Release(u8),
}

use Op::{Acquire, Release};

/// One concurrently-running future. Its `program` is the exact ordered sequence
/// of lock acquisitions/releases from the real code (see module docs for the
/// `file:line` of every step). `pc` is the program counter into it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Task {
    /// Human-readable label of the code region this program models.
    name: &'static str,
    program: Vec<Op>,
    pc: usize,
}

impl Task {
    fn new(name: &'static str, program: Vec<Op>) -> Self {
        Task {
            name,
            program,
            pc: 0,
        }
    }

    fn done(&self) -> bool {
        self.pc >= self.program.len()
    }

    fn next_op(&self) -> Option<Op> {
        self.program.get(self.pc).copied()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    /// `holder[l] == Some(task_idx)` when lock `l` is held by that task.
    holder: [Option<usize>; NUM_LOCKS],
    tasks: Vec<Task>,
}

/// The action the scheduler takes: advance task `idx` by executing its next op.
/// Only emitted when that op is *enabled* (a Release, or an Acquire of a lock
/// that is free or already self-held) — a blocked Acquire is never enabled, so a
/// task suspended on a contended `.lock().await` simply cannot step.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Step {
    idx: usize,
}

/// Build the poll task's program — the full sequence of multi-lock regions a
/// single `poll()` call walks, in source order. Each region's locks are
/// acquired in the real nesting order and released at the modeled scope end.
///
/// Region boundaries (and the RPC `.await`s between them, where all guards are
/// already dropped) are faithful to `poll.rs`:
///   1. `apply_pending_seeks`  : PS, N, P           (seek.rs:101,105,106)
///   2. `resolve_latest`       : N                  (poll.rs:559) [across await, alone]
///   3. `refresh_leader_epochs`: P                  (validate.rs:69)
///   4. `validate_positions`   : N, P  then  P      (validate.rs:95,96 ; :150)
///   5. fetch build            : N, P               (poll.rs:209,210)
///   6. post-fetch processing  : N  then (N,P)…     (poll.rs:304 ; :374/:396/:523)
fn poll_program() -> Vec<Op> {
    vec![
        // --- apply_pending_seeks (seek.rs) ---
        // fast-path PS probe (:96), released immediately.
        Acquire(PS),
        Release(PS),
        // assigned snapshot (:100), released at end of statement.
        Acquire(A),
        Release(A),
        // held region: PS(:101) → N(:105) → P(:106), all dropped at :123.
        Acquire(PS),
        Acquire(N),
        Acquire(P),
        Release(P),
        Release(N),
        Release(PS),
        // --- resolve_latest_sentinels (poll.rs:559): N alone, across a
        //     ListOffsets await; no second lock taken in the region. ---
        Acquire(N),
        Release(N),
        // --- refresh_leader_epochs (validate.rs:69): P alone. ---
        Acquire(P),
        Release(P),
        // --- validate_positions: N→P snapshot (validate.rs:95,96) … ---
        Acquire(N),
        Acquire(P),
        Release(P),
        Release(N),
        // … then P alone post-RPC (validate.rs:150).
        Acquire(P),
        Release(P),
        // --- fetch build (poll.rs:209→210): N→P, dropped before the Fetch. ---
        Acquire(N),
        Acquire(P),
        Release(P),
        Release(N),
        // --- post-fetch (poll.rs): A snapshot (:296), released at stmt end. ---
        Acquire(A),
        Release(A),
        // N held across the processing loop (:304); inside it P is acquired
        // SECOND (:374/:396/:523) — N→P, never P→N. Model one representative
        // nested P acquire/release inside the held-N region.
        Acquire(N),
        Acquire(P),
        Release(P),
        Release(N),
    ]
}

/// The coordinator task's `rejoin` program. A is only ever held alone; N→P
/// regions for the offset/position prune. Models the cooperative phase-1 path
/// (the most lock-dense), which also subsumes the eager path's edges.
///
/// Sequence (coordinator.rs):
///   A alone (:513 owned snapshot)
///   [`join_and_sync` → T alone (:886)]
///   A alone (:573 phase-1 retain)
///   [`commit_revoked` → N,P (:635,:636) ; T alone (:653)]
///   N,P prune (:587,:588)
///   A alone (:597 `owned_after_revoke` snapshot)
///   [`prime_offsets` → T alone (:976) ; N,P (:987,:988)]
///   A alone (:614 publish phase-2 assignment)
fn coordinator_program() -> Vec<Op> {
    vec![
        // owned snapshot (:513)
        Acquire(A),
        Release(A),
        // join_and_sync → topic_ids merge (:886)
        Acquire(T),
        Release(T),
        // phase-1 retain of kept partitions (:573)
        Acquire(A),
        Release(A),
        // commit_revoked: N→P (:635,:636)
        Acquire(N),
        Acquire(P),
        Release(P),
        Release(N),
        // commit_revoked: topic_ids snapshot (:653)
        Acquire(T),
        Release(T),
        // prune next_offsets + positions: N→P (:587,:588)
        Acquire(N),
        Acquire(P),
        Release(P),
        Release(N),
        // owned_after_revoke snapshot (:597)
        Acquire(A),
        Release(A),
        // prime_offsets: topic_ids snapshot (:976)
        Acquire(T),
        Release(T),
        // prime_offsets: N→P (:987,:988)
        Acquire(N),
        Acquire(P),
        Release(P),
        Release(N),
        // publish phase-2 assignment (:614)
        Acquire(A),
        Release(A),
    ]
}

/// The commit task (`commit.rs` `commit_sync` / `commit_async`): N, then P,
/// then T — each released before the next is taken. At most one lock held at a
/// time, so it can never be half of a cycle. Modeled faithfully to confirm.
fn commit_program() -> Vec<Op> {
    vec![
        // next_offsets snapshot (:70 / :129)
        Acquire(N),
        Release(N),
        // positions snapshot (:74→:76 / :133→:135)
        Acquire(P),
        Release(P),
        // topic_ids snapshot (:77 / :136)
        Acquire(T),
        Release(T),
    ]
}

#[derive(Clone, Debug)]
struct LockOrderModel {
    programs: Vec<(&'static str, Vec<Op>)>,
}

impl Model for LockOrderModel {
    type State = State;
    type Action = Step;

    fn init_states(&self) -> Vec<Self::State> {
        let tasks = self
            .programs
            .iter()
            .map(|(name, prog)| Task::new(name, prog.clone()))
            .collect();
        vec![State {
            holder: [None; NUM_LOCKS],
            tasks,
        }]
    }

    fn actions(&self, s: &Self::State, acts: &mut Vec<Self::Action>) {
        for (idx, task) in s.tasks.iter().enumerate() {
            let Some(op) = task.next_op() else { continue };
            let enabled = match op {
                Op::Release(_) => true,
                Op::Acquire(l) => match s.holder[l as usize] {
                    // Free, or (impossible here) already self-held → can proceed.
                    None => true,
                    Some(h) => h == idx,
                    // Held by another task → BLOCKED: not enabled. This is the
                    // task suspended at a contended `.lock().await`.
                },
            };
            if enabled {
                acts.push(Step { idx });
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        let idx = action.idx;
        let op = s.tasks[idx].next_op()?;
        match op {
            Op::Acquire(l) => {
                match s.holder[l as usize] {
                    None => s.holder[l as usize] = Some(idx),
                    Some(h) if h == idx => {} // self-held no-op (not reachable here)
                    Some(_) => return None,   // blocked: not a legal step
                }
            }
            Op::Release(l) => {
                // Faithful: a task only releases a lock it holds.
                if s.holder[l as usize] != Some(idx) {
                    return None;
                }
                s.holder[l as usize] = None;
            }
        }
        s.tasks[idx].pc += 1;
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // THE property: no deadlock. A deadlock is a reachable state with at
            // least one unfinished task where EVERY unfinished task is blocked
            // (its next op is an Acquire of a lock held by a *different* task) —
            // a global wait cycle. We assert it can never happen.
            Property::always("no_deadlock", |_, s: &State| !is_deadlocked(s)),
            // Liveness sanity: the all-done terminal is reachable (proves the
            // programs can run to completion, so "no_deadlock" isn't vacuous).
            Property::sometimes("all_tasks_complete", |_, s: &State| {
                s.tasks.iter().all(Task::done)
            }),
            // Sanity: contention actually occurs (some lock is held while
            // another task wants it) — proves the interleaving is non-trivial.
            Property::sometimes("contention_observed", |_, s: &State| any_task_blocked(s)),
        ]
    }
}

/// A task is *blocked* iff its next op is an `Acquire` of a lock currently held
/// by a different task (it is suspended at a contended `.lock().await`).
fn task_blocked(s: &State, idx: usize) -> bool {
    match s.tasks[idx].next_op() {
        Some(Op::Acquire(l)) => matches!(s.holder[l as usize], Some(h) if h != idx),
        _ => false,
    }
}

fn any_task_blocked(s: &State) -> bool {
    (0..s.tasks.len()).any(|i| task_blocked(s, i))
}

/// Deadlock = at least one unfinished task, and every unfinished task is
/// blocked. (If any unfinished task can still step, the system makes progress.)
fn is_deadlocked(s: &State) -> bool {
    let unfinished: Vec<usize> = (0..s.tasks.len()).filter(|&i| !s.tasks[i].done()).collect();
    !unfinished.is_empty() && unfinished.iter().all(|&i| task_blocked(s, i))
}

const MAX_STATES: usize = 5_000_000;
const MAX_DEPTH: usize = 256;
const CHECK_TIMEOUT: Duration = Duration::from_mins(1);

fn run_model(programs: Vec<(&'static str, Vec<Op>)>) -> stateright::CheckerBuilder<LockOrderModel> {
    LockOrderModel { programs }
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .timeout(CHECK_TIMEOUT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    /// MAIN RESULT: the real classic-consumer lock protocol (poll task +
    /// coordinator task + commit task, each running its extracted acquire/
    /// release sequence) is DEADLOCK-FREE under exhaustive interleaving.
    #[test]
    fn classic_consumer_lock_protocol_is_deadlock_free() {
        let checker = run_model(vec![
            ("poll", poll_program()),
            ("coordinator", coordinator_program()),
            ("commit", commit_program()),
        ])
        .spawn_bfs()
        .join();
        eprintln!(
            "[lock_order] unique={} generated={} depth={}",
            checker.unique_state_count(),
            checker.state_count(),
            checker.max_depth(),
        );
        // The bound must NOT have been hit, or "deadlock-free" would be a
        // statement about a truncated space, not the whole one.
        assert!(
            checker.state_count() < MAX_STATES,
            "state space hit the cap — the proof would be incomplete; shrink the model"
        );
        assert!(checker.max_depth() < MAX_DEPTH, "depth cap hit");
        checker.assert_properties();
    }

    /// Two poll tasks racing the coordinator (e.g. a buggy double-poll) — still
    /// deadlock-free, because every region respects the same PS<N<P order.
    #[test]
    fn two_pollers_and_coordinator_are_deadlock_free() {
        let checker = run_model(vec![
            ("poll-a", poll_program()),
            ("poll-b", poll_program()),
            ("coordinator", coordinator_program()),
        ])
        .spawn_bfs()
        .join();
        eprintln!(
            "[lock_order/2poll] unique={} generated={} depth={}",
            checker.unique_state_count(),
            checker.state_count(),
            checker.max_depth(),
        );
        assert!(
            checker.state_count() < MAX_STATES,
            "state space hit the cap"
        );
        assert!(checker.max_depth() < MAX_DEPTH, "depth cap hit");
        checker.assert_properties();
    }

    /// NEGATIVE CONTROL / falsification check: inject the hypothesized P→N
    /// inversion (the "smoking gun" the investigation feared on the post-fetch
    /// path) into a second task and confirm the model DOES find the deadlock.
    /// This proves `no_deadlock` is a real, falsifiable property — not a model
    /// that can never fail — and shows exactly the cycle the real code avoids.
    #[test]
    fn injected_inversion_is_detected_as_deadlock() {
        // Task X: hold N, then acquire P  (the real order, N→P).
        let n_then_p = vec![Acquire(N), Acquire(P), Release(P), Release(N)];
        // Task Y: hold P, then acquire N  (the INVERSION, P→N — does NOT exist
        // in the real code; injected here only to validate the checker).
        let p_then_n = vec![Acquire(P), Acquire(N), Release(N), Release(P)];
        let checker = run_model(vec![("np", n_then_p), ("pn", p_then_n)])
            .spawn_bfs()
            .join();
        // The interleaving N-holds-N, P-holds-P, each then blocked on the other
        // is a genuine cycle; the checker must surface a `no_deadlock`
        // counterexample.
        assert!(
            checker.discoveries().contains_key("no_deadlock"),
            "checker failed to detect the injected P->N inversion deadlock — \
             the no_deadlock property is not actually falsifiable"
        );
    }

    /// Unit-level sanity for the deadlock predicate itself.
    #[test]
    fn deadlock_predicate_recognizes_a_mutual_wait_cycle() {
        // Two tasks, each holding one lock and next wanting the other's.
        let mut s = State {
            holder: [None; NUM_LOCKS],
            tasks: vec![
                Task::new("x", vec![Acquire(N), Acquire(P)]),
                Task::new("y", vec![Acquire(P), Acquire(N)]),
            ],
        };
        // x holds N (pc past its Acquire(N)); y holds P.
        s.holder[N as usize] = Some(0);
        s.tasks[0].pc = 1; // next op = Acquire(P)
        s.holder[P as usize] = Some(1);
        s.tasks[1].pc = 1; // next op = Acquire(N)
        assert!(is_deadlocked(&s));

        // Progress case: x's next op is a Release (it can step), so the system
        // is NOT deadlocked even though y is currently blocked.
        let mut live = State {
            holder: [None; NUM_LOCKS],
            tasks: vec![
                Task::new("x", vec![Acquire(N), Release(N), Acquire(P)]),
                Task::new("y", vec![Acquire(P), Acquire(N)]),
            ],
        };
        live.holder[N as usize] = Some(0);
        live.tasks[0].pc = 1; // x next op = Release(N): steppable
        live.holder[P as usize] = Some(1);
        live.tasks[1].pc = 1; // y next op = Acquire(N): blocked
        assert!(!is_deadlocked(&live));

        // Terminal-but-complete: all tasks done ⇒ not a deadlock (programs are
        // balanced, so a done task holds no locks).
        let all_done = State {
            holder: [None; NUM_LOCKS],
            tasks: vec![Task::new("x", vec![Acquire(N), Release(N)])],
        };
        let mut done = all_done;
        done.tasks[0].pc = 2; // done
        assert!(!is_deadlocked(&done));
    }
}
