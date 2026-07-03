//! COMPOSITIONAL end-to-end data-path model — the first model beyond the
//! per-slice ones. It composes the four real seam cores — HWM/ISR
//! (`ReplicaState`), leader-epoch truncation (`epoch_and_offset_for_entries`),
//! failover selection (`failover_one`/`select_best_replica`), and fetch
//! visibility (`compute_visibility_window`) — over a tiny cluster (3 brokers, 1
//! partition) to verify the canonical broker guarantee end-to-end: an
//! `acks=all` record is never lost across clean leader changes, every consumer
//! read is consistent, and unclean-election loss is exactly characterized.
//!
//! Per-broker log = `Vec<u8>` of leader epochs (offset = index, value ≡ offset).
//! Durability is tracked by a ghost `committed` (epoch per offset ever ≤ HWM);
//! read consistency is checked at the visibility seam. Built incrementally
//! (DPC-1 spine → DPC-2 clean failover → DPC-3 unclean). State explosion is the
//! central risk — see the bounds + the host memory watchdog.

// All arithmetic here is bounded to a tiny cluster (≤ 3 brokers, log-len ≤ 3),
// so the offset/length/id casts below can never wrap or truncate.
#![allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
    time::{Duration, Instant},
};

use crabka_log::{EpochEntry, Offset, epoch_and_offset_for_entries};
use crabka_metadata::PartitionRecord;
use stateright::{Checker, Model, Property};

use crate::{
    config_keys::RecoveryStrategy,
    handlers::fetch::compute_visibility_window,
    leader_election::{FailoverDecision, failover_one},
    replica_state::ReplicaState,
    unclean_recovery::{ReplicaLogInfo, select_best_replica},
};

const NB: usize = 3; // brokers 0,1,2
const MAX_LEN: usize = 4; // max log length (offsets 0..4)
const MAX_EPOCH: u8 = 3;

const TARGET_STATE_COUNT: usize = 60_000_000;
const MAX_UNIQUE_STATES: usize = 8_000_000;
const MAX_DEPTH: usize = 70;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

fn node(b: u8) -> u64 {
    u64::from(b)
}
fn has(mask: u8, b: u8) -> bool {
    mask & (1 << b) != 0
}

#[derive(Clone, Debug)]
struct DpState {
    log: [Vec<u8>; NB], // log[b] = Vec<epoch>; offset = index
    hwm: i64,           // leader-authoritative high watermark
    leader: u8,
    leader_epoch: u8,
    isr: u8,            // bitmask over brokers
    live: u8,           // bitmask over brokers
    committed: Vec<u8>, // ghost: committed[off] = epoch, for offsets ever <= hwm
    lost: bool,         // ghost: an unclean loss has occurred
}

impl DpState {
    fn leader_leo(&self) -> i64 {
        self.log[self.leader as usize].len() as i64
    }
    #[allow(clippy::type_complexity)]
    fn proj(&self) -> (Vec<Vec<u8>>, i64, u8, u8, u8, u8, Vec<u8>, bool) {
        (
            self.log.to_vec(),
            self.hwm,
            self.leader,
            self.leader_epoch,
            self.isr,
            self.live,
            self.committed.clone(),
            self.lost,
        )
    }
}
impl PartialEq for DpState {
    fn eq(&self, o: &Self) -> bool {
        self.proj() == o.proj()
    }
}
impl Eq for DpState {}
impl Hash for DpState {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.proj().hash(h);
    }
}

// ----- real-core adapters (the wrap-real seams) -----

/// The follower's effective LEO *as seen by the leader*: the length of the
/// longest epoch-consistent common prefix with the leader's log. A real follower
/// truncates any divergence (via `OffsetForLeaderEpoch`) BEFORE advancing its
/// reported fetch offset, so the leader never sees — and never advances the HWM
/// over — divergent follower data. Using raw `len()` here would let the HWM
/// commit data a divergent follower hasn't actually reconciled (the bug this
/// composition surfaced).
fn consistent_leo(follower_log: &[u8], leader_log: &[u8]) -> i64 {
    follower_log
        .iter()
        .zip(leader_log.iter())
        .take_while(|(f, l)| f == l)
        .count() as i64
}

/// Drive the REAL HWM core: reconstruct a `ReplicaState` from the model's ISR +
/// per-follower (consistent) LEOs and return the recomputed HWM (= min ISR LEO,
/// clamped to the leader LEO).
fn real_hwm(s: &DpState, base: Instant) -> i64 {
    let leader = s.leader;
    let leader_leo = s.leader_leo();
    let leader_log = &s.log[leader as usize];
    let isr_nodes: Vec<crabka_audit::NodeId> = (0..NB as u8)
        .filter(|&b| has(s.isr, b))
        .map(|b| crabka_audit::NodeId(node(b)))
        .collect();
    let replica_nodes: Vec<crabka_audit::NodeId> = (0..NB as u8)
        .map(|b| crabka_audit::NodeId(node(b)))
        .collect();
    let mut rs = ReplicaState::new();
    rs.install_isr(
        &isr_nodes,
        &replica_nodes,
        crabka_audit::NodeId(node(leader)),
        base,
    );
    for b in 0..NB as u8 {
        if b != leader && has(s.isr, b) {
            let leo = consistent_leo(&s.log[b as usize], leader_log);
            // Wrap this model's `i64` LEOs into `Offset` for the real HWM core.
            rs.update_follower_leo(
                crabka_audit::NodeId(node(b)),
                Offset(leo),
                Offset(leader_leo),
                base,
            );
        }
    }
    // Unwrap the recomputed `Offset` HWM back into this model's `i64` world.
    rs.recompute_hw_for_leader_append(Offset(leader_leo)).0
}

/// The leader-epoch entries for a log: one entry per epoch change.
fn epoch_entries(log: &[u8]) -> Vec<EpochEntry> {
    let mut out = Vec::new();
    let mut last: Option<u8> = None;
    for (off, &e) in log.iter().enumerate() {
        if last != Some(e) {
            out.push(EpochEntry {
                epoch: crabka_log::LeaderEpoch(i32::from(e)),
                start_offset: Offset(off as i64),
            });
            last = Some(e);
        }
    }
    out
}

/// Drive the REAL divergence core: the exclusive offset `follower` keeps when
/// reconciling against `leader_log`.
fn real_truncation_offset(follower_log: &[u8], leader_log: &[u8]) -> i64 {
    let leader_entries = epoch_entries(leader_log);
    let follower_latest = follower_log.last().map_or(-1, |&e| i32::from(e));
    let (_, end) = epoch_and_offset_for_entries(
        &leader_entries,
        crabka_log::LeaderEpoch(follower_latest),
        Offset(leader_log.len() as i64),
    );
    // Unwrap the log-layer `Offset` into this model's `i64` world at the seam.
    end.0.min(follower_log.len() as i64)
}

/// Whether follower `b` is genuinely in-sync and may be (re)admitted to the
/// ISR: its log is an epoch-consistent prefix of the leader's (no unreconciled
/// divergence) AND it is caught up to at least the HWM. This mirrors the real
/// invariant — a follower's reported progress is only ever post-truncation
/// consistent, so a divergent follower can never appear caught-up.
fn isr_eligible(s: &DpState, b: u8) -> bool {
    let f = &s.log[b as usize];
    let l = &s.log[s.leader as usize];
    (f.len() as i64) >= s.hwm && f.iter().enumerate().all(|(off, &e)| l.get(off) == Some(&e))
}

/// Apply a leader election: set leader/ISR, bump the epoch, and — for an UNCLEAN
/// election — characterize any committed-data loss. The new (possibly less
/// complete) leader keeps only the committed prefix it actually holds with the
/// same epoch; any committed offset it lacks is LOST (flagged in `lost`, and the
/// HWM clamped to the new leader's log). Clean elections (`unclean == false`)
/// never lose committed data, so there is no truncation.
fn apply_elect(s: &mut DpState, new_leader: u8, isr_mask: u8, unclean: bool) {
    s.leader = new_leader;
    s.isr = isr_mask;
    s.leader_epoch += 1;
    if unclean {
        let nl = &s.log[new_leader as usize];
        let kept = s
            .committed
            .iter()
            .enumerate()
            .take_while(|&(off, e)| nl.get(off) == Some(e))
            .count();
        if kept < s.committed.len() {
            s.lost = true;
            s.committed.truncate(kept);
            s.hwm = s.hwm.min(nl.len() as i64);
        }
    }
}

/// The controller's failover reaction to broker `dead` being down: drive the
/// real `failover_one`, applying its decision (clean elect / ISR shrink), and —
/// in the unclean config — driving the real KIP-966 `select_best_replica` for
/// the empty-ISR `Recover` path.
fn do_failover(s: &mut DpState, dead: u8, unclean: bool) {
    let isr_nodes: Vec<crabka_audit::NodeId> = (0..NB as u8)
        .filter(|&b| has(s.isr, b))
        .map(|b| crabka_audit::NodeId(node(b)))
        .collect();
    let replica_nodes: Vec<crabka_audit::NodeId> = (0..NB as u8)
        .map(|b| crabka_audit::NodeId(node(b)))
        .collect();
    let pr = PartitionRecord {
        leader: crabka_audit::NodeId(node(s.leader)),
        replicas: replica_nodes,
        isr: isr_nodes,
        leader_epoch: crabka_metadata::LeaderEpoch(i32::from(s.leader_epoch)),
        ..Default::default()
    };
    let alive: HashSet<crabka_audit::NodeId> = (0..NB as u8)
        .filter(|&b| has(s.live, b))
        .map(|b| crabka_audit::NodeId(node(b)))
        .collect();
    // Clean config: strategy None + unclean disabled → only ISR elections (else
    // Unavailable). Unclean config: Balanced strategy defers an empty-ISR
    // partition to KIP-966 offset-aware recovery.
    let strategy = if unclean {
        RecoveryStrategy::Balanced
    } else {
        RecoveryStrategy::None
    };
    match failover_one(
        &pr,
        crabka_audit::NodeId(node(dead)),
        &alive,
        strategy,
        unclean,
    ) {
        FailoverDecision::Elect {
            leader,
            isr,
            unclean,
        } => {
            let isr_mask = isr.iter().fold(0u8, |m, &n| m | (1u8 << (n.0 as u8)));
            apply_elect(s, leader.0 as u8, isr_mask, unclean);
        }
        FailoverDecision::Recover(_) => {
            // KIP-966 unclean recovery: drive the REAL select_best_replica over
            // the live replicas' log info; the winner becomes leader with a
            // singleton ISR (may lose un-replicated committed data).
            let infos: Vec<ReplicaLogInfo> = (0..NB as u8)
                .filter(|&b| has(s.live, b))
                .map(|b| ReplicaLogInfo {
                    broker_id: crabka_audit::NodeId(node(b)),
                    last_written_leader_epoch: s.log[b as usize]
                        .last()
                        .map_or(0, |&e| i32::from(e)),
                    log_end_offset: s.log[b as usize].len() as i64,
                    current_leader_epoch: i32::from(s.leader_epoch),
                })
                .collect();
            if let Some(winner) = select_best_replica(&infos) {
                apply_elect(s, winner.0 as u8, 1u8 << (winner.0 as u8), true);
            }
        }
        FailoverDecision::ShrinkIsr { isr } => {
            s.isr = isr.iter().fold(0u8, |m, &n| m | (1u8 << (n.0 as u8)));
        }
        FailoverDecision::Unavailable | FailoverDecision::NoChange => {}
    }
}

// ----- model -----

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum Act {
    Produce,
    Replicate(u8), // follower b fetches one step from the leader
    AdvanceHwm,
    ConsumerFetch {
        read_committed: bool,
        fetch_offset: i64,
    },
    Die(u8),
    Revive(u8),
    Failover(u8),  // controller reacts to broker `b` being down
    ExpandIsr(u8), // re-admit a caught-up follower to the ISR
}

struct DpModel {
    base: Instant,
    unclean: bool, // false in DPC-1/2 (clean), true in DPC-3
}

impl Model for DpModel {
    type State = DpState;
    type Action = Act;

    fn init_states(&self) -> Vec<Self::State> {
        vec![DpState {
            log: [vec![], vec![], vec![]],
            hwm: 0,
            leader: 0,
            leader_epoch: 1,
            isr: 0b111,
            live: 0b111,
            committed: vec![],
            lost: false,
        }]
    }

    fn actions(&self, s: &Self::State, acts: &mut Vec<Self::Action>) {
        let leader_live = has(s.live, s.leader);
        // Data-path actions require a live leader.
        if leader_live {
            if s.log[s.leader as usize].len() < MAX_LEN && s.leader_epoch <= MAX_EPOCH {
                acts.push(Act::Produce);
            }
            for b in 0..NB as u8 {
                if b != s.leader
                    && has(s.live, b)
                    && (s.log[b as usize].len() as i64) < s.leader_leo()
                {
                    acts.push(Act::Replicate(b));
                }
            }
            acts.push(Act::AdvanceHwm);
            for fo in 0..=s.leader_leo() {
                acts.push(Act::ConsumerFetch {
                    read_committed: false,
                    fetch_offset: fo,
                });
                acts.push(Act::ConsumerFetch {
                    read_committed: true,
                    fetch_offset: fo,
                });
            }
        }
        // Liveness + failover.
        let live_count = u32::from(s.live).count_ones();
        for b in 0..NB as u8 {
            if has(s.live, b) && live_count > 1 {
                acts.push(Act::Die(b));
            }
            if !has(s.live, b) {
                acts.push(Act::Revive(b));
                // Controller failover: elect (dead leader, epoch headroom) or
                // shrink the ISR (dead non-leader ISR member).
                if (b == s.leader && s.leader_epoch < MAX_EPOCH) || (b != s.leader && has(s.isr, b))
                {
                    acts.push(Act::Failover(b));
                }
            }
            // Re-admit a follower to the ISR only once it is genuinely in-sync:
            // an epoch-consistent prefix of the leader's log (it has truncated +
            // replicated any divergence via the real protocol) AND caught up to
            // the HWM. Checking LEO alone would admit a stale, divergent follower
            // that hasn't reconciled — which is unreachable in real Kafka, where
            // the follower fetch/OffsetForLeaderEpoch loop truncates before its
            // reported progress can make it eligible.
            if has(s.live, b) && b != s.leader && !has(s.isr, b) && isr_eligible(s, b) {
                acts.push(Act::ExpandIsr(b));
            }
        }
    }

    fn next_state(&self, last: &Self::State, a: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match a {
            Act::Produce => {
                s.log[s.leader as usize].push(s.leader_epoch);
            }
            Act::Replicate(b) => {
                let leader_log = s.log[s.leader as usize].clone();
                let trunc = real_truncation_offset(&s.log[b as usize], &leader_log) as usize;
                s.log[b as usize].truncate(trunc);
                if s.log[b as usize].len() < leader_log.len() {
                    let off = s.log[b as usize].len();
                    s.log[b as usize].push(leader_log[off]);
                }
            }
            Act::AdvanceHwm => {
                // HWM = min ISR LEO (real core). Monotonic within a leader epoch
                // by construction (ISR expansion is gated on `leo >= hwm`, shrink
                // only raises the min), but it may legitimately REGRESS on a leader
                // change (KIP-207 — the new leader recomputes from its own ISR's
                // LEOs). So no monotonicity assert: durability is the
                // `committed_durable` property, not HWM monotonicity.
                s.hwm = real_hwm(&s, self.base);
                let leader_log = &s.log[s.leader as usize];
                while (s.committed.len() as i64) < s.hwm {
                    let off = s.committed.len();
                    s.committed.push(leader_log[off]);
                }
            }
            Act::ConsumerFetch {
                read_committed,
                fetch_offset,
            } => {
                let leader_log_len = s.leader_leo();
                let vw = compute_visibility_window(
                    false, // consumer, not follower
                    read_committed,
                    Offset(0), // log_start
                    Offset(s.hwm),
                    Offset(s.hwm), // lso = hwm (no txns in v1)
                    Offset(leader_log_len),
                    Offset(fetch_offset),
                );
                assert!(
                    vw.limit_offset <= Offset(s.hwm),
                    "consumer limit {} exceeds HWM {}",
                    vw.limit_offset,
                    s.hwm
                );
                assert!(vw.response_hw == Offset(s.hwm), "response_hw drift");
            }
            Act::Die(b) => {
                s.live &= !(1 << b);
            }
            Act::Revive(b) => {
                s.live |= 1 << b;
            }
            Act::ExpandIsr(b) => {
                s.isr |= 1 << b;
            }
            Act::Failover(dead) => do_failover(&mut s, dead, self.unclean),
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props = vec![
            Property::always("committed_durable", |_, s: &DpState| {
                let lg = &s.log[s.leader as usize];
                s.committed
                    .iter()
                    .enumerate()
                    .all(|(off, &e)| lg.get(off) == Some(&e))
            }),
            Property::always("hwm_within_leader_log", |_, s: &DpState| {
                s.hwm <= s.leader_leo()
            }),
            Property::sometimes("committed_progress", |_, s: &DpState| {
                !s.committed.is_empty()
            }),
            Property::sometimes("full_replication", |_, s: &DpState| {
                s.hwm == s.leader_leo() && s.hwm > 0
            }),
            // A leader change occurred.
            Property::sometimes("leader_changed", |_, s: &DpState| s.leader_epoch >= 2),
            // The ISR shrank below the full replica set.
            Property::sometimes("isr_shrunk", |_, s: &DpState| {
                u32::from(s.isr).count_ones() < NB as u32
            }),
            // Two brokers hold different epochs at one offset — truncation
            // territory (a follower must truncate to reconcile).
            Property::sometimes("divergence_present", |_, s: &DpState| {
                (0..MAX_LEN).any(|off| {
                    let mut seen: Option<u8> = None;
                    for b in 0..NB {
                        if let Some(&e) = s.log[b].get(off) {
                            match seen {
                                None => seen = Some(e),
                                Some(x) if x != e => return true,
                                _ => {}
                            }
                        }
                    }
                    false
                })
            }),
        ];
        if self.unclean {
            // Loss characterization: an unclean-election data loss is reachable
            // (and `committed_durable` above still holds — `committed` is the LIVE
            // durability obligation, truncated when an unclean election drops it).
            props.push(Property::sometimes("unclean_loss", |_, s: &DpState| s.lost));
        } else {
            // Clean config: NO committed-data loss ever occurs.
            props.push(Property::always("no_loss_when_clean", |_, s: &DpState| {
                !s.lost
            }));
        }
        props
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.log.iter().all(|l| l.len() <= MAX_LEN) && s.leader_epoch <= MAX_EPOCH + 1
    }
}

fn run(model: DpModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(TARGET_STATE_COUNT)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique={} generated={} depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert!(checker.max_depth() < MAX_DEPTH, "[{label}] depth cap hit");
    assert!(
        checker.state_count() < TARGET_STATE_COUNT,
        "[{label}] truncated"
    );
    assert!(
        checker.unique_state_count() < MAX_UNIQUE_STATES,
        "[{label}] unique bound exceeded ({})",
        checker.unique_state_count()
    );
    checker.assert_properties();
}

#[test]
fn data_clean() {
    run(
        DpModel {
            base: Instant::now(),
            unclean: false,
        },
        "data_clean",
    );
}

#[test]
fn data_unclean() {
    run(
        DpModel {
            base: Instant::now(),
            unclean: true,
        },
        "data_unclean",
    );
}
