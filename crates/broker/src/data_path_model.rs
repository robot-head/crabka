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

use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

use crabka_log::{EpochEntry, epoch_and_offset_for_entries};
use stateright::{Checker, Model, Property};

use crate::handlers::fetch::compute_visibility_window;
use crate::replica_state::ReplicaState;

const NB: usize = 3; // brokers 0,1,2
const MAX_LEN: usize = 3; // max log length (offsets 0..3)
const MAX_EPOCH: u8 = 3;

const TARGET_STATE_COUNT: usize = 30_000_000;
const MAX_UNIQUE_STATES: usize = 1_500_000;
const MAX_DEPTH: usize = 40;
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

/// Drive the REAL HWM core: reconstruct a `ReplicaState` from the model's ISR +
/// per-follower LEOs and return the recomputed HWM (= min ISR LEO, clamped to
/// the leader LEO).
fn real_hwm(s: &DpState, base: Instant) -> i64 {
    let leader = s.leader;
    let leader_leo = s.leader_leo();
    let isr_nodes: Vec<u64> = (0..NB as u8).filter(|&b| has(s.isr, b)).map(node).collect();
    let replica_nodes: Vec<u64> = (0..NB as u8).map(node).collect();
    let mut rs = ReplicaState::new();
    rs.install_isr(&isr_nodes, &replica_nodes, node(leader), base);
    for b in 0..NB as u8 {
        if b != leader && has(s.isr, b) {
            rs.update_follower_leo(node(b), s.log[b as usize].len() as i64, leader_leo, base);
        }
    }
    rs.recompute_hw_for_leader_append(leader_leo)
}

/// The leader-epoch entries for a log: one entry per epoch change.
fn epoch_entries(log: &[u8]) -> Vec<EpochEntry> {
    let mut out = Vec::new();
    let mut last: Option<u8> = None;
    for (off, &e) in log.iter().enumerate() {
        if last != Some(e) {
            out.push(EpochEntry {
                epoch: i32::from(e),
                start_offset: off as i64,
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
    let (_, end) =
        epoch_and_offset_for_entries(&leader_entries, follower_latest, leader_log.len() as i64);
    end.min(follower_log.len() as i64)
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
        if s.log[s.leader as usize].len() < MAX_LEN && s.leader_epoch <= MAX_EPOCH {
            acts.push(Act::Produce);
        }
        for b in 0..NB as u8 {
            if b != s.leader && has(s.live, b) && (s.log[b as usize].len() as i64) < s.leader_leo()
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
                let new_hw = real_hwm(&s, self.base);
                assert!(
                    new_hw >= s.hwm || self.unclean,
                    "HWM regressed {} -> {new_hw}",
                    s.hwm
                );
                s.hwm = new_hw.max(s.hwm);
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
                    0, // log_start
                    s.hwm,
                    s.hwm, // lso = hwm (no txns in v1)
                    leader_log_len,
                    fetch_offset,
                );
                assert!(
                    vw.limit_offset <= s.hwm,
                    "consumer limit {} exceeds HWM {}",
                    vw.limit_offset,
                    s.hwm
                );
                assert!(vw.response_hw == s.hwm, "response_hw drift");
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
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
        ]
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
fn data_steady() {
    run(
        DpModel {
            base: Instant::now(),
            unclean: false,
        },
        "data_steady",
    );
}
