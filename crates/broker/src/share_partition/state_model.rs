//! Exhaustive stateright model of the pure KIP-932 share-partition acquisition
//! core (`AcquisitionState`).
//!
//! The model state holds the REAL `AcquisitionState` and drives the production
//! `materialize` / `acquire` / `acknowledge` / `renew` / `expire_locks` /
//! `to_persist_batches` / `load_from`; the BFS checker explores every
//! interleaving of consumer operations, time advance, and (in the failover
//! config) leader-reload, asserting the share-group delivery-safety invariants
//! never break. Design:
//! `docs/superpowers/specs/2026-06-13-crabka-share-group-model-design.md`.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each run is fenced with `within_boundary` + `target_state_count` + `timeout`
//! and MUST be executed under the host memory watchdog while bounds are tuned
//! (never unguarded — a runaway space exhausts host RAM).

use std::time::{Duration, Instant};

use crabka_log::Offset;
use stateright::{Checker, Model, Property};

use super::{AckType, AcquisitionState, RecordState};

/// The single acquisition-lock duration used by the model. A lock taken at
/// logical time `clock` has deadline `t0 + LOCK*(clock + 1)`, so it expires once
/// the clock reaches `clock + 1`.
const LOCK: Duration = Duration::from_secs(1);

/// Hard backstop on generated states — bounds host memory even if
/// `within_boundary` is looser than intended. Set well above each config's true
/// bounded count so a real (exhaustive) run never truncates.
const MAX_STATES: usize = 200_000;
/// Depth backstop. Must exceed each config's reachable-graph diameter, or the
/// search is depth-truncated (incomplete) and the `run` harness fails loudly.
const MAX_DEPTH: usize = 80;
/// Wall-clock backstop.
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

/// Bounded model config (held here, not in the fingerprinted state).
struct ShareModel {
    /// Base instant; all `now` values are `t0 + LOCK*clock`. Captured once per
    /// run, so deadlines are drawn from a finite, hashable set.
    t0: Instant,
    /// Number of consumer members (named `m0`..`m{members-1}`).
    members: u8,
    /// High-watermark / window cap (records produced over a path).
    max_offset: Offset,
    /// Logical-clock cap.
    max_tick: u8,
    /// Delivery-attempt limit before a record is archived as a poison pill.
    max_attempts: i16,
    /// Max records `materialize` pulls into the window at once.
    max_inflight: i32,
    /// Whether the leader-failover `Reload` action is generated (Task 3).
    allow_reload: bool,
}

/// The fingerprinted model state: the REAL machine plus the small finite clock
/// and produced-record high-watermark.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct ShareState {
    sm: AcquisitionState,
    clock: u8,
    hwm: Offset,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum ShareAction {
    /// Append one record to the log (raise the produced high-watermark).
    Produce,
    /// Leader pulls produced-but-unmaterialized records into the window.
    Materialize,
    /// `member` acquires up to `max_records` Available records.
    Acquire { member: u8, max_records: i32 },
    /// `member` acknowledges `[first, last]` it holds.
    Acknowledge {
        member: u8,
        first: Offset,
        last: Offset,
        ack: AckType,
    },
    /// `member` renews (extends) the lock on `[first, last]` it holds.
    Renew {
        member: u8,
        first: Offset,
        last: Offset,
    },
    /// Sweep expired acquisition locks back to Available.
    ExpireLocks,
    /// Advance the logical clock by one lock-duration.
    Tick,
    /// Leader failover: persist + reload (drops Acquired → Available, locks lost).
    Reload,
}

impl ShareModel {
    /// Concurrency config: full action set EXCEPT `Reload`. Bounds start small
    /// (proven memory-safe); Task 4 scales `max_offset` empirically.
    fn concurrency(max_offset: i64, max_inflight: i32) -> Self {
        Self {
            t0: Instant::now(),
            members: 2,
            max_offset: Offset(max_offset),
            max_tick: 2,
            max_attempts: 2,
            max_inflight,
            allow_reload: false,
        }
    }

    /// Failover config: adds `Reload` over a small window; focuses the
    /// `acknowledged_is_terminal` durability invariant across crash-recovery.
    fn failover() -> Self {
        Self {
            t0: Instant::now(),
            members: 2,
            max_offset: Offset(2),
            max_tick: 2,
            max_attempts: 2,
            max_inflight: 2,
            allow_reload: true,
        }
    }

    fn now(&self, clock: u8) -> Instant {
        self.t0 + LOCK * u32::from(clock)
    }

    fn member_name(member: u8) -> String {
        format!("m{member}")
    }
}

// ---- observability helpers (descendant-module private access) --------------

/// Delivery state of `off`, if it currently lies in a batch.
fn offset_state(sm: &AcquisitionState, off: Offset) -> Option<RecordState> {
    sm.batches
        .iter()
        .find(|b| b.first_offset <= off && off <= b.last_offset)
        .map(|b| b.state)
}

/// Delivery count of `off`, if it currently lies in a batch.
fn offset_dc(sm: &AcquisitionState, off: Offset) -> Option<i16> {
    sm.batches
        .iter()
        .find(|b| b.first_offset <= off && off <= b.last_offset)
        .map(|b| b.delivery_count)
}

/// Maximal contiguous offset runs currently Acquired by `member`. Adjacent
/// same-owner batches with differing lock deadlines do not coalesce, so they are
/// stitched back into one run here (the whole run is ack/renew-able at once).
fn acquired_runs(sm: &AcquisitionState, member: &str) -> Vec<(Offset, Offset)> {
    let mut runs: Vec<(Offset, Offset)> = Vec::new();
    let mut cur: Option<(Offset, Offset)> = None;
    for b in &sm.batches {
        let mine = b.state == RecordState::Acquired && b.acquired_by.as_deref() == Some(member);
        match (mine, cur) {
            (true, Some((f, l))) if b.first_offset == l + 1 => cur = Some((f, b.last_offset)),
            (true, Some((f, l))) => {
                runs.push((f, l));
                cur = Some((b.first_offset, b.last_offset));
            }
            (true, None) => cur = Some((b.first_offset, b.last_offset)),
            (false, Some((f, l))) => {
                runs.push((f, l));
                cur = None;
            }
            (false, None) => {}
        }
    }
    if let Some((f, l)) = cur {
        runs.push((f, l));
    }
    runs
}

// ---- state-level invariants (Property::always predicates) ------------------

/// Batches are sorted, gap-free, non-overlapping, and exactly cover
/// `[start_offset, end_offset)`; `start_offset <= end_offset`.
fn window_integrity(sm: &AcquisitionState) -> bool {
    if sm.start_offset > sm.end_offset {
        return false;
    }
    if sm.batches.is_empty() {
        return sm.start_offset == sm.end_offset;
    }
    if sm.batches[0].first_offset != sm.start_offset {
        return false;
    }
    for w in sm.batches.windows(2) {
        if w[0].first_offset > w[0].last_offset || w[0].last_offset + 1 != w[1].first_offset {
            return false;
        }
    }
    let last = sm.batches.last().expect("non-empty checked above");
    last.first_offset <= last.last_offset && last.last_offset + 1 == sm.end_offset
}

/// Every Acquired batch carries exactly one owner. Combined with
/// `window_integrity`'s non-overlap, no offset is concurrently held by two
/// members — the headline share-group guarantee.
fn mutual_exclusion(sm: &AcquisitionState) -> bool {
    sm.batches
        .iter()
        .all(|b| b.state != RecordState::Acquired || b.acquired_by.is_some())
}

/// Lock bookkeeping matches the delivery state: Acquired ⇒ owner + deadline
/// present; every other state ⇒ neither present.
fn lock_consistency(sm: &AcquisitionState) -> bool {
    sm.batches.iter().all(|b| match b.state {
        RecordState::Acquired => b.acquired_by.is_some() && b.lock_deadline.is_some(),
        _ => b.acquired_by.is_none() && b.lock_deadline.is_none(),
    })
}

// ---- transition-level invariants (asserted in next_state) ------------------

/// Compare a parent machine to its child after one operation; panic on any
/// monotonicity / durability violation. Kept OUT of the fingerprinted state so
/// no path-history ghost can explode the space (Phase-1 OOM lesson).
fn assert_transition(parent: &AcquisitionState, child: &AcquisitionState) {
    assert!(
        child.start_offset >= parent.start_offset,
        "SPSO regressed: {} -> {}",
        parent.start_offset,
        child.start_offset
    );
    assert!(
        child.delivery_complete_count >= parent.delivery_complete_count,
        "delivery_complete_count regressed: {} -> {}",
        parent.delivery_complete_count,
        child.delivery_complete_count
    );
    // Per-offset delivery_count never regresses for offsets live in both.
    for raw in child.start_offset.0..child.end_offset.0 {
        let off = Offset(raw);
        if let (Some(pc), Some(cc)) = (offset_dc(parent, off), offset_dc(child, off)) {
            assert!(
                cc >= pc,
                "delivery_count regressed at offset {off}: {pc} -> {cc}"
            );
        }
    }
    // An Acknowledged offset is terminal: in the child it is still Acknowledged
    // or has dropped below the (non-decreasing) SPSO — never resurrected.
    for raw in parent.start_offset.0..parent.end_offset.0 {
        let off = Offset(raw);
        if offset_state(parent, off) == Some(RecordState::Acknowledged) {
            match offset_state(child, off) {
                None => assert!(
                    off < child.start_offset,
                    "acknowledged offset {off} vanished while still in window"
                ),
                Some(s) => assert!(
                    s == RecordState::Acknowledged,
                    "acknowledged offset {off} reverted to {s:?}"
                ),
            }
        }
    }
}

impl Model for ShareModel {
    type State = ShareState;
    type Action = ShareAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![ShareState {
            sm: AcquisitionState::new(Offset(0)),
            clock: 0,
            hwm: Offset(0),
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let has_available = state
            .sm
            .batches
            .iter()
            .any(|b| b.state == RecordState::Available);
        let has_acquired = state
            .sm
            .batches
            .iter()
            .any(|b| b.state == RecordState::Acquired);

        if state.hwm < self.max_offset {
            actions.push(ShareAction::Produce);
        }
        // Materialize only when there are produced-but-unmaterialized records and
        // no Available batch remains (the real `materialize` no-ops otherwise).
        if state.sm.end_offset < state.hwm && !has_available {
            actions.push(ShareAction::Materialize);
        }
        if has_available {
            for member in 0..self.members {
                actions.push(ShareAction::Acquire {
                    member,
                    max_records: 1,
                });
                actions.push(ShareAction::Acquire {
                    member,
                    max_records: i32::MAX,
                });
            }
        }
        // Data-dependent: ack/renew only over ranges a member actually holds.
        for member in 0..self.members {
            let name = Self::member_name(member);
            for (first, last) in acquired_runs(&state.sm, &name) {
                for ack in [AckType::Accept, AckType::Release, AckType::Reject] {
                    actions.push(ShareAction::Acknowledge {
                        member,
                        first,
                        last,
                        ack,
                    });
                }
                actions.push(ShareAction::Renew {
                    member,
                    first,
                    last,
                });
                // A split (first half) exercises partial-ack / partial-renew.
                if last > first {
                    let mid = first + (last.0 - first.0) / 2;
                    for ack in [AckType::Accept, AckType::Release, AckType::Reject] {
                        actions.push(ShareAction::Acknowledge {
                            member,
                            first,
                            last: mid,
                            ack,
                        });
                    }
                    actions.push(ShareAction::Renew {
                        member,
                        first,
                        last: mid,
                    });
                }
            }
        }
        if has_acquired {
            actions.push(ShareAction::ExpireLocks);
        }
        if state.clock < self.max_tick {
            actions.push(ShareAction::Tick);
        }
        if self.allow_reload && state.sm.end_offset > state.sm.start_offset {
            actions.push(ShareAction::Reload);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            ShareAction::Produce => {
                if state.hwm >= self.max_offset {
                    return None;
                }
                state.hwm += 1;
            }
            ShareAction::Materialize => {
                let before = state.sm.end_offset;
                state.sm.materialize(state.hwm, self.max_inflight);
                if state.sm.end_offset == before {
                    return None; // no-op: nothing materialized
                }
            }
            ShareAction::Acquire {
                member,
                max_records,
            } => {
                let name = Self::member_name(member);
                let now = self.now(state.clock);
                state
                    .sm
                    .acquire(&name, max_records, i32::MAX, now, LOCK, self.max_attempts);
            }
            ShareAction::Acknowledge {
                member,
                first,
                last: hi,
                ack,
            } => {
                let name = Self::member_name(member);
                let now = self.now(state.clock);
                if state.sm.acknowledge(&name, first, hi, ack, now).is_err() {
                    return None; // inapplicable ack: no transition
                }
            }
            ShareAction::Renew {
                member,
                first,
                last: hi,
            } => {
                let name = Self::member_name(member);
                let now = self.now(state.clock);
                if state.sm.renew(&name, first, hi, now, LOCK).is_err() {
                    return None; // inapplicable renew: no transition
                }
            }
            ShareAction::ExpireLocks => {
                let now = self.now(state.clock);
                state.sm.expire_locks(now);
            }
            ShareAction::Tick => {
                if state.clock >= self.max_tick {
                    return None;
                }
                state.clock += 1;
            }
            ShareAction::Reload => {
                let (start, dcc, batches) = state.sm.to_persist_batches();
                let mut fresh = AcquisitionState::new(start);
                fresh.load_from(
                    start,
                    state.sm.state_epoch,
                    state.sm.leader_epoch,
                    dcc,
                    &batches,
                );
                state.sm = fresh;
            }
        }
        assert_transition(&last.sm, &state.sm);
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("window_integrity", |_, s: &ShareState| {
                window_integrity(&s.sm)
            }),
            Property::always("mutual_exclusion", |_, s: &ShareState| {
                mutual_exclusion(&s.sm)
            }),
            Property::always("lock_consistency", |_, s: &ShareState| {
                lock_consistency(&s.sm)
            }),
            Property::always(
                "delivery_count_bounded",
                |m: &ShareModel, s: &ShareState| {
                    s.sm.batches
                        .iter()
                        .all(|b| b.delivery_count <= m.max_attempts)
                },
            ),
            Property::always("spso_in_range", |m: &ShareModel, s: &ShareState| {
                Offset(0) <= s.sm.start_offset
                    && s.sm.start_offset <= s.sm.end_offset
                    && s.sm.end_offset <= m.max_offset
            }),
            Property::sometimes("can_advance_spso", |_, s: &ShareState| {
                s.sm.start_offset > Offset(0)
            }),
            Property::sometimes("can_acknowledge", |_, s: &ShareState| {
                s.sm.batches
                    .iter()
                    .any(|b| b.state == RecordState::Acknowledged)
            }),
            Property::sometimes("can_archive", |_, s: &ShareState| {
                s.sm.batches
                    .iter()
                    .any(|b| b.state == RecordState::Archived)
            }),
            Property::sometimes("can_redeliver", |_, s: &ShareState| {
                s.sm.batches.iter().any(|b| b.delivery_count >= 2)
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        // Bound ONLY the design-unbounded dimensions (so the space is finite);
        // do NOT bound delivery_count — its <= max_attempts boundedness is a
        // property we verify, so pruning it would mask a violation. The 12-batch
        // cap is a loose structural safety net (real max over a <=3 window is 3).
        state.clock <= self.max_tick
            && state.hwm <= self.max_offset
            && state.sm.end_offset <= self.max_offset
            && state.sm.batches.len() <= 12
    }
}

/// Run one bounded config to completion and assert it was exhaustive (not
/// truncated by a cap) and that all properties hold.
fn run(model: ShareModel, label: &str) {
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
fn share_concurrency_inflight_full() {
    // max_inflight large enough to pull the whole window in one materialize.
    run(
        ShareModel::concurrency(3, 3),
        "share_concurrency_inflight_full",
    );
}

#[test]
fn share_concurrency_inflight_one() {
    // max_inflight = 1: exercises drain-then-rematerialize across Produce steps.
    run(
        ShareModel::concurrency(3, 1),
        "share_concurrency_inflight_one",
    );
}

#[test]
fn share_failover() {
    // Adds leader-failover Reload; stresses acknowledged-is-terminal durability.
    run(ShareModel::failover(), "share_failover");
}
