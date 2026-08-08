//! Diskless WAL crash-restart model for partial durability windows.
//!
//! This model is small on purpose. It composes the Slice 5 crash windows with
//! the Slice 6 diskless WAL quorum and stateless appenders.
//!
//! `KRaft` can reserve offsets before the bytes fsync. An object PUT can come
//! before the index commit. An fsync can tear the active tail. Any WAL member
//! can start a reservation. A trim must stop at the committed index frontier.

use std::time::Duration;

use stateright::{Checker, Model, Property};

const MAX_OFFSET: i64 = 3;
const MAX_DEPTH: usize = 24;
const TARGET_STATE_COUNT: usize = 100_000;
const CHECK_TIMEOUT: Duration = Duration::from_secs(20);

const WITNESS_KRAFT_FSYNC_GAP: u8 = 1 << 0;
const WITNESS_PUT_BEFORE_INDEX: u8 = 1 << 1;
const WITNESS_MID_FSYNC: u8 = 1 << 2;
const WITNESS_TRIM_AT_INDEX: u8 = 1 << 3;
const WITNESS_MINORITY_WAL_LOSS: u8 = 1 << 4;
const WITNESS_STATELESS_APPEND: u8 = 1 << 5;

const WAL_NODES: usize = 3;
const WAL_MAJORITY: usize = 2;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CrashState {
    kraft_next: i64,
    log_end: i64,
    wal_nodes: [i64; WAL_NODES],
    wal_lost: [bool; WAL_NODES],
    wal_acked: i64,
    object_frontier: i64,
    index_frontier: i64,
    trimmed: i64,
    producer_committed: i64,
    producer_rebuilt: i64,
    reservations: Vec<(i64, i64)>,
    witnesses: u8,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Act {
    ReserveVia(usize),
    FsyncAppend,
    CrashBeforeFsync,
    CrashMidFsync,
    RebuildProducerState,
    PutObject,
    CommitIndex,
    Trim,
    LoseWalNode(usize),
}

#[derive(Clone, Debug)]
struct CrashModel;

impl Model for CrashModel {
    type Action = Act;
    type State = CrashState;

    fn init_states(&self) -> Vec<Self::State> {
        vec![CrashState {
            kraft_next: 0,
            log_end: 0,
            wal_nodes: [0; WAL_NODES],
            wal_lost: [false; WAL_NODES],
            wal_acked: 0,
            object_frontier: 0,
            index_frontier: 0,
            trimmed: 0,
            producer_committed: 0,
            producer_rebuilt: 0,
            reservations: Vec::new(),
            witnesses: 0,
        }]
    }

    fn actions(&self, s: &Self::State, acts: &mut Vec<Self::Action>) {
        if s.kraft_next < MAX_OFFSET {
            for node in 0..WAL_NODES {
                if !s.wal_lost[node] {
                    acts.push(Act::ReserveVia(node));
                }
            }
        }
        if s.log_end < s.kraft_next {
            acts.push(Act::FsyncAppend);
            acts.push(Act::CrashBeforeFsync);
            acts.push(Act::CrashMidFsync);
        }
        if s.producer_rebuilt < s.producer_committed {
            acts.push(Act::RebuildProducerState);
        }
        if s.object_frontier < s.wal_acked {
            acts.push(Act::PutObject);
        }
        if s.index_frontier < s.object_frontier {
            acts.push(Act::CommitIndex);
        }
        if s.trimmed < s.index_frontier {
            acts.push(Act::Trim);
        }
        if s.wal_lost.iter().filter(|lost| **lost).count() == 0 && s.wal_acked > 0 {
            for node in 0..WAL_NODES {
                acts.push(Act::LoseWalNode(node));
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            Act::ReserveVia(node) => {
                let base = s.kraft_next;
                s.kraft_next += 1;
                s.reservations.push((base, s.kraft_next));
                if node != 0 {
                    s.witnesses |= WITNESS_STATELESS_APPEND;
                }
            }
            Act::FsyncAppend => {
                s.log_end += 1;
                fsync_quorum(&mut s);
                s.producer_committed = s.log_end;
            }
            Act::CrashBeforeFsync => {
                if s.kraft_next > s.log_end {
                    s.witnesses |= WITNESS_KRAFT_FSYNC_GAP;
                }
                s.producer_rebuilt = s.log_end;
            }
            Act::CrashMidFsync => {
                s.witnesses |= WITNESS_MID_FSYNC;
                s.kraft_next = s.kraft_next.max(s.log_end);
                s.producer_rebuilt = s.log_end;
            }
            Act::RebuildProducerState => {
                s.producer_rebuilt = s.producer_committed;
            }
            Act::PutObject => {
                s.object_frontier = s.wal_acked;
                if s.object_frontier > s.index_frontier {
                    s.witnesses |= WITNESS_PUT_BEFORE_INDEX;
                }
            }
            Act::CommitIndex => {
                s.index_frontier = s.object_frontier;
            }
            Act::Trim => {
                s.trimmed = s.index_frontier.min(s.wal_acked);
                if s.trimmed > 0 && s.trimmed == s.index_frontier {
                    s.witnesses |= WITNESS_TRIM_AT_INDEX;
                }
            }
            Act::LoseWalNode(node) => {
                s.wal_lost[node] = true;
                s.witnesses |= WITNESS_MINORITY_WAL_LOSS;
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("wal_acked_durable", |_, s: &CrashState| {
                s.wal_acked <= surviving_wal_frontier(s)
            }),
            Property::always("producer_dedup_no_regress", |_, s: &CrashState| {
                s.producer_rebuilt <= s.producer_committed && s.producer_committed <= s.log_end
            }),
            Property::always("trim_at_committed_index_frontier", |_, s: &CrashState| {
                s.trimmed <= s.index_frontier && s.trimmed <= s.wal_acked
            }),
            Property::always("reservations_gap_free_and_unique", |_, s: &CrashState| {
                let mut next = 0;
                for &(base, end) in &s.reservations {
                    if base != next || end <= base {
                        return false;
                    }
                    next = end;
                }
                next == s.kraft_next
            }),
            Property::sometimes("crash_in_kraft_fsync_gap", |_, s: &CrashState| {
                s.witnesses & WITNESS_KRAFT_FSYNC_GAP != 0
            }),
            Property::sometimes("crash_between_put_and_index", |_, s: &CrashState| {
                s.witnesses & WITNESS_PUT_BEFORE_INDEX != 0
            }),
            Property::sometimes("crash_mid_fsync", |_, s: &CrashState| {
                s.witnesses & WITNESS_MID_FSYNC != 0
            }),
            Property::sometimes("trim_at_index_frontier", |_, s: &CrashState| {
                s.witnesses & WITNESS_TRIM_AT_INDEX != 0
            }),
            Property::sometimes(
                "acked_unflushed_survives_minority_wal_loss",
                |_, s: &CrashState| {
                    s.witnesses & WITNESS_MINORITY_WAL_LOSS != 0 && s.wal_acked > s.trimmed
                },
            ),
            Property::sometimes(
                "stateless_append_via_non_leader_member",
                |_, s: &CrashState| s.witnesses & WITNESS_STATELESS_APPEND != 0,
            ),
        ]
    }
}

fn fsync_quorum(s: &mut CrashState) {
    let mut synced = 0;
    for node in 0..WAL_NODES {
        if !s.wal_lost[node] && synced < WAL_MAJORITY {
            s.wal_nodes[node] = s.log_end;
            synced += 1;
        }
    }
    s.wal_acked = quorum_frontier(s);
}

fn quorum_frontier(s: &CrashState) -> i64 {
    let live: Vec<i64> = s
        .wal_nodes
        .iter()
        .copied()
        .zip(s.wal_lost.iter().copied())
        .filter_map(|(offset, lost)| (!lost).then_some(offset))
        .collect();
    if live.len() < WAL_MAJORITY {
        return s.trimmed;
    }
    let mut live = live;
    live.sort_unstable();
    let leader_end = live.pop().unwrap_or(0);
    let followers = live;
    crabka_verified::recompute_high_watermark(leader_end, &followers, WAL_MAJORITY, -1, 0)
}

fn surviving_wal_frontier(s: &CrashState) -> i64 {
    s.wal_nodes
        .iter()
        .copied()
        .zip(s.wal_lost.iter().copied())
        .filter_map(|(offset, lost)| (!lost).then_some(offset))
        .max()
        .unwrap_or(s.index_frontier)
        .max(s.index_frontier)
}

fn run() {
    let checker = CrashModel
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(TARGET_STATE_COUNT)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[diskless_crash_model] unique={} generated={} depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert!(checker.max_depth() < MAX_DEPTH, "depth cap hit");
    assert!(checker.state_count() < TARGET_STATE_COUNT, "truncated");
    checker.assert_properties();
}

#[test]
fn diskless_crash_model() {
    run();
}
