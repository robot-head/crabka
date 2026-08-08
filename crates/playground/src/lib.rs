//! WebAssembly bindings that drive Crabka's deterministic `KRaft` consensus
//! engine in the browser.
//!
//! The roadmap for the docs-site playground is to "simulate consensus in your
//! browser". The deterministic core compiles to WASM. The page can then inject
//! partitions, drop, reorder, and duplicate messages, and watch a cluster elect
//! a leader, lose it, and recover, live and with no backend. This crate is the
//! seam. It wraps [`crabka_kraft_core::sim::Sim`] and exposes it to JavaScript
//! as a [`Playground`] handle. `Sim` is the same pure, sans-IO multi-node
//! simulator that the integration tests and `crabka-docgen` drive.
//!
//! Everything here is a thin shim. The consensus, the scheduler, the in-memory
//! message bus, and the fault model are all in the core. JavaScript owns the
//! clock and the rendering only. It calls [`Playground::step`] when it wants
//! time to advance, and it reads the cluster state back as JSON after each
//! action.

use crabka_kraft_core::{sim::Sim, types::NodeId};
use wasm_bindgen::prelude::*;

/// An interactive, in-browser `KRaft` consensus simulation.
///
/// Construct one with a voter count, then drive it from JavaScript. Inject
/// faults, step the message bus and the timers, and read [`Playground::state`]
/// back as JSON to render. The simulation is fully deterministic: the same
/// sequence of calls always produces the same trace.
#[wasm_bindgen]
pub struct Playground {
    sim: Sim,
}

#[wasm_bindgen]
impl Playground {
    /// Create a fresh cluster with `voters` nodes, with the count clamped to
    /// 1..=7. The cluster starts with no leader and with every node's election
    /// timer armed, so the first [`step`](Self::step) calls show the bootstrap
    /// election.
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new(voters: u32) -> Self {
        // A panic hook so a Rust panic surfaces as a readable console error
        // instead of an opaque `unreachable executed` trap.
        console_error_panic_hook::set_once();
        Self {
            sim: Sim::new(&voter_ids(voters)),
        }
    }

    /// Tear down and rebuild the cluster with `voters` nodes, back at clock zero.
    pub fn reset(&mut self, voters: u32) {
        self.sim = Sim::new(&voter_ids(voters));
    }

    /// Advance one scheduler microstep. The method delivers the next in-flight
    /// message. If the bus is empty, it fires the next-due timer. It returns
    /// `true` if something happened. This drives the JS animation loop, which
    /// stops when the result is `false`.
    pub fn step(&mut self) -> bool {
        self.sim.step_once()
    }

    /// Run the scheduler until the cluster stops changing, that is until a
    /// leader is settled and no messages remain. The run is bounded, so a
    /// pathological case cannot hang.
    pub fn settle(&mut self) {
        self.sim.run_until_stable(10_000);
    }

    /// Network-partition `node` away from the rest of the cluster. The method
    /// drops its in-flight messages, and the node can neither send nor receive
    /// until it is healed.
    pub fn partition(&mut self, node: u32) {
        self.sim.partition(NodeId(u64::from(node)));
    }

    /// Heal a previously [`partition`](Self::partition)ed `node`.
    pub fn heal(&mut self, node: u32) {
        self.sim.heal(NodeId(u64::from(node)));
    }

    /// Append `n` records on the node that is currently leader. This is the
    /// "produce" button. The method returns `false` if there is no leader to
    /// append to.
    pub fn append(&mut self, n: u32) -> bool {
        self.sim.append(n as usize)
    }

    /// Drop the next in-flight message instead of delivering it. Returns `false`
    /// if the bus is empty.
    pub fn drop_next(&mut self) -> bool {
        self.sim.drop_next()
    }

    /// Deliver every queued message back-to-front, that is not in FIFO order,
    /// to exercise reordered delivery. The method returns how many messages it
    /// delivered.
    pub fn reorder(&mut self) -> usize {
        self.sim.reorder()
    }

    /// Deliver the next in-flight message twice to exercise duplicate handling.
    /// Returns `false` if the bus is empty.
    pub fn duplicate_next(&mut self) -> bool {
        self.sim.duplicate_next()
    }

    /// A JSON snapshot of the whole cluster for the UI to render after each
    /// action. The snapshot holds the clock, each node's role, epoch, and log,
    /// the in-flight bus, the current leaders, and the elapsed step count.
    /// Shape:
    ///
    /// ```json
    /// { "clock_ms": 1234,
    ///   "nodes": [{ "id": 1, "role": "Leader", "epoch": 2, "log_len": 4,
    ///               "hwm": 4, "partitioned": false }],
    ///   "in_flight": [{ "src": 1, "dst": 2, "event": "BeginQuorumEpoch" }],
    ///   "leaders": [1],
    ///   "step_count": 17 }
    /// ```
    #[must_use]
    pub fn state(&self) -> String {
        serde_json::to_string(&self.sim.snapshot()).unwrap_or_else(|_| "{}".to_string())
    }

    /// The event timeline recorded from `index` onward, as a JSON array of
    /// steps. The UI tracks the last index that it has seen, from the
    /// `step_count` field of `state`, and asks for the new steps only. The
    /// timeline thus streams incrementally, and the crate does not re-send the
    /// whole history each frame.
    #[must_use]
    pub fn timeline_since(&self, index: usize) -> String {
        let steps = self.sim.steps();
        let tail = steps.get(index..).unwrap_or(&[]);
        serde_json::to_string(tail).unwrap_or_else(|_| "[]".to_string())
    }
}

/// Build the voter id list `[1, 2, ..., n]`. The function clamps `n` to 1..=7,
/// so a stray UI value can never allocate an absurd cluster.
fn voter_ids(voters: u32) -> Vec<NodeId> {
    let n = u64::from(voters.clamp(1, 7));
    (1..=n).map(NodeId).collect()
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    fn state(p: &Playground) -> Value {
        serde_json::from_str(&p.state()).expect("state is valid JSON")
    }
    fn leaders(p: &Playground) -> Vec<u64> {
        state(p)["leaders"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .collect()
    }
    fn in_flight_len(p: &Playground) -> usize {
        state(p)["in_flight"].as_array().unwrap().len()
    }
    /// Drive the bus and the timers until `pred` holds, or until the step
    /// budget is exhausted, the way the JS animation loop does.
    fn step_until(p: &mut Playground, pred: impl Fn(&Playground) -> bool) {
        for _ in 0..20_000 {
            if pred(p) {
                return;
            }
            if !p.step() {
                return;
            }
        }
    }

    #[test]
    fn clamps_voter_count() {
        assert2::assert!(voter_ids(0) == vec![NodeId(1)]);
        assert2::assert!(voter_ids(3) == vec![NodeId(1), NodeId(2), NodeId(3)]);
        assert2::assert!(voter_ids(99) == (1..=7).map(NodeId).collect::<Vec<_>>());
    }

    #[test]
    fn new_cluster_starts_leaderless_at_clock_zero() {
        let pg = Playground::new(3);
        let s = state(&pg);
        assert2::assert!(s["nodes"].as_array().unwrap().len() == 3);
        assert2::assert!(s["clock_ms"].as_u64().unwrap() == 0);
        assert2::assert!(leaders(&pg).is_empty());
        assert2::assert!(s["step_count"].as_u64().unwrap() == 0);
    }

    #[test]
    fn step_drives_a_bootstrap_election_to_one_leader() {
        let mut pg = Playground::new(3);
        step_until(&mut pg, |p| !leaders(p).is_empty());
        pg.settle();
        assert2::assert!(leaders(&pg).len() == 1);
        // The clock and timeline advanced as a side effect of stepping.
        assert2::assert!(state(&pg)["clock_ms"].as_u64().unwrap() > 0);
        assert2::assert!(state(&pg)["step_count"].as_u64().unwrap() > 0);
    }

    #[test]
    fn append_targets_the_leader_only_once_elected() {
        let mut pg = Playground::new(3);
        assert2::assert!(!pg.append(2));
        step_until(&mut pg, |p| !leaders(p).is_empty());
        pg.settle();
        let leader = leaders(&pg)[0];
        let log_len = |p: &Playground| -> u64 {
            state(p)["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|n| n["id"].as_u64() == Some(leader))
                .unwrap()["log_len"]
                .as_u64()
                .unwrap()
        };
        let before = log_len(&pg);
        assert2::assert!(pg.append(3));
        assert2::assert!(log_len(&pg) == before + 3);
    }

    #[test]
    fn partition_then_heal_keeps_one_leader() {
        let mut pg = Playground::new(3);
        step_until(&mut pg, |p| !leaders(p).is_empty());
        pg.settle();
        let old = leaders(&pg)[0];

        pg.partition(old as u32);
        // The partitioned leader shows as isolated.
        let node = state(&pg)["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["id"].as_u64() == Some(old))
            .cloned()
            .unwrap();
        assert2::assert!(node["partitioned"].as_bool() == Some(true));

        step_until(&mut pg, |p| leaders(p).iter().any(|&l| l != old));
        pg.settle();
        pg.heal(old as u32);
        pg.settle();
        assert2::assert!(leaders(&pg).len() == 1);
    }

    #[test]
    fn drop_next_consumes_a_bus_message() {
        let mut pg = Playground::new(3);
        step_until(&mut pg, |p| in_flight_len(p) > 0);
        let before = in_flight_len(&pg);
        assert2::assert!(before > 0);
        assert2::assert!(pg.drop_next());
        assert2::assert!(in_flight_len(&pg) == before - 1);
        // timeline_since exposes the recorded Drop step as JSON.
        let count = state(&pg)["step_count"].as_u64().unwrap() as usize;
        let tl: Value = serde_json::from_str(&pg.timeline_since(count - 1)).unwrap();
        assert2::assert!(tl.as_array().unwrap().last().unwrap()["action"]["kind"] == "Drop");
    }

    #[test]
    fn reorder_and_duplicate_replay_the_bus() {
        let mut pg = Playground::new(3);
        step_until(&mut pg, |p| in_flight_len(p) > 0);
        // reorder delivers the currently-queued messages (back-to-front);
        // delivering them can enqueue fresh responses, so the bus need not end
        // empty — what we assert is that it drained the round it was given.
        assert2::assert!(pg.reorder() >= 1);

        step_until(&mut pg, |p| in_flight_len(p) > 0);
        assert2::assert!(pg.duplicate_next());
    }

    #[test]
    fn empty_bus_faults_are_no_ops() {
        // A freshly-constructed cluster has its election timers armed but no
        // messages on the bus yet (nothing has been stepped).
        let mut pg = Playground::new(3);
        assert2::assert!(in_flight_len(&pg) == 0);
        assert2::assert!(!pg.drop_next());
        assert2::assert!(!pg.duplicate_next());
        assert2::assert!(pg.reorder() == 0);
    }

    #[test]
    fn reset_rebuilds_the_cluster_back_at_clock_zero() {
        let mut pg = Playground::new(3);
        step_until(&mut pg, |p| !leaders(p).is_empty());
        assert2::assert!(state(&pg)["clock_ms"].as_u64().unwrap() > 0);
        pg.reset(5);
        let s = state(&pg);
        assert2::assert!(s["nodes"].as_array().unwrap().len() == 5);
        assert2::assert!(s["clock_ms"].as_u64().unwrap() == 0);
        assert2::assert!(s["step_count"].as_u64().unwrap() == 0);
        assert2::assert!(leaders(&pg).is_empty());
    }

    #[test]
    fn five_voter_cluster_converges_to_one_leader() {
        let mut pg = Playground::new(5);
        step_until(&mut pg, |p| !leaders(p).is_empty());
        pg.settle();
        assert2::assert!(leaders(&pg).len() == 1);
    }
}
