//! Deterministic, in-memory, multi-node simulation of the KIP-595/996 `KRaft`
//! consensus core (`crabka_raft::kraft`). This is the headline acceptance test
//! for slice 3a: it wires N `QuorumStateMachine`s together through an in-memory
//! message bus and a logical clock (the shared [`sim_harness`] module), and
//! asserts the cluster reaches the canonical single-leader / agreed
//! high-watermark states over an in-memory [`SimLog`].
//!
//! The harness itself lives in `tests/sim_harness/mod.rs` and is shared with
//! `kraft_log_sim.rs`, which runs the same core over a real on-disk `KraftLog`.

mod sim_harness;

use crabka_ids::NodeId;
use sim_harness::{Sim, SimLog};

/// A cluster whose nodes use the in-memory fake log.
fn new_sim(voter_ids: &[NodeId]) -> Sim<SimLog> {
    Sim::new_with(voter_ids, |_id| SimLog::default())
}

#[test]
fn three_nodes_elect_exactly_one_leader() {
    let mut sim = new_sim(&[NodeId(1), NodeId(2), NodeId(3)]);
    sim.run_until_stable(10_000);
    assert2::assert!(sim.leaders().len() == 1);
    // Every voter agrees on a single leader epoch.
    assert2::assert!(sim.distinct_epochs().len() == 1);
}

#[test]
fn re_elects_single_leader_after_leader_partition() {
    let mut sim = new_sim(&[NodeId(1), NodeId(2), NodeId(3)]);
    sim.run_until_stable(10_000);
    assert2::assert!(sim.leaders().len() == 1);
    let old_leader = sim.leaders()[0];

    // Isolate the leader; the majority side must elect a new one.
    sim.partition(old_leader);
    sim.run_until_stable(10_000);
    let new_leaders: Vec<_> = sim
        .leaders()
        .into_iter()
        .filter(|&l| l != old_leader)
        .collect();
    assert2::assert!(new_leaders.len() == 1);

    // Heal the partition; the old leader rejoins and steps down to follower,
    // leaving a single leader cluster-wide.
    sim.heal(old_leader);
    sim.run_until_stable(10_000);
    assert2::assert!(sim.leaders().len() == 1);
    assert2::assert!(sim.leaders()[0] == new_leaders[0]);
}

#[test]
fn committed_high_watermark_agrees_across_voters() {
    let mut sim = new_sim(&[NodeId(1), NodeId(2), NodeId(3)]);
    sim.run_until_stable(10_000);
    assert2::assert!(sim.leaders().len() == 1);
    let leader = sim.leaders()[0];

    // The leader already appended its LeaderChange control record at promotion;
    // capture the log end, then produce 5 data records on top.
    let before = sim.log_end_offset(leader);
    sim.leader_append(leader, 5);
    let target = before + 5;

    sim.run_until_stable(10_000);

    // The HWM must reach the appended offset (current-epoch entries are now
    // majority-replicated — this is the FIX-2 leader-completeness gate) and all
    // voters must have replicated up to it.
    assert2::assert!(sim.leader_high_watermark(leader) >= target);
    assert2::assert!(sim.all_voters_fetched_to(sim.leader_high_watermark(leader)));
}
