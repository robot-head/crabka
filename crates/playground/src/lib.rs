//! WebAssembly bindings that drive Crabka's deterministic `KRaft` consensus
//! engine in the browser.
//!
//! The roadmap for the docs-site playground is to "simulate consensus in your
//! browser": compile the deterministic core to WASM and let the page inject
//! partitions, drop / reorder / duplicate messages, and watch a cluster elect a
//! leader, lose it, and recover — live, with no backend. This crate is the
//! seam. It wraps [`crabka_kraft_core::sim::Sim`] — the very same pure,
//! sans-IO multi-node simulator the integration tests and `crabka-docgen`
//! drive — and exposes it to JavaScript as a [`Playground`] handle.
//!
//! Everything here is a thin shim: the consensus, the scheduler, the in-memory
//! message bus, and the fault model all live in the core. JavaScript owns only
//! the clock (it calls [`Playground::step`] when it wants time to advance) and
//! the rendering; it reads cluster state back as JSON after each action.

use crabka_kraft_core::sim::Sim;
use wasm_bindgen::prelude::*;

/// An interactive, in-browser `KRaft` consensus simulation.
///
/// Construct one with a voter count, then drive it from JavaScript: inject
/// faults, step the message bus / timers, and read [`Playground::state`] back as
/// JSON to render. The simulation is fully deterministic — the same sequence of
/// calls always produces the same trace.
#[wasm_bindgen]
pub struct Playground {
    sim: Sim,
}

#[wasm_bindgen]
impl Playground {
    /// Create a fresh cluster with `voters` nodes (clamped to 1..=7). The
    /// cluster starts with no leader and every node's election timer armed, so
    /// the very first [`step`](Self::step)s show the bootstrap election happen.
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

    /// Advance one scheduler microstep: deliver the next in-flight message, or —
    /// if the bus is empty — fire the next-due timer. Returns `true` if anything
    /// happened (drives the JS animation loop, which stops when this is `false`).
    pub fn step(&mut self) -> bool {
        self.sim.step_once()
    }

    /// Run the scheduler until the cluster stops changing (a leader is settled
    /// and no messages remain), bounded so a pathological case can't hang.
    pub fn settle(&mut self) {
        self.sim.run_until_stable(10_000);
    }

    /// Network-partition `node` away from the rest of the cluster: its in-flight
    /// messages are dropped and it can neither send nor receive until healed.
    pub fn partition(&mut self, node: u32) {
        self.sim.partition(u64::from(node));
    }

    /// Heal a previously [`partition`](Self::partition)ed `node`.
    pub fn heal(&mut self, node: u32) {
        self.sim.heal(u64::from(node));
    }

    /// Append `n` records on whichever node is currently leader (the "produce"
    /// button). Returns `false` if there is no leader to append to.
    pub fn append(&mut self, n: u32) -> bool {
        self.sim.append(n as usize)
    }

    /// Drop the next in-flight message instead of delivering it. Returns `false`
    /// if the bus is empty.
    pub fn drop_next(&mut self) -> bool {
        self.sim.drop_next()
    }

    /// Deliver every queued message back-to-front (non-FIFO) to exercise
    /// reordered delivery. Returns how many were delivered.
    pub fn reorder(&mut self) -> usize {
        self.sim.reorder()
    }

    /// Deliver the next in-flight message twice to exercise duplicate handling.
    /// Returns `false` if the bus is empty.
    pub fn duplicate_next(&mut self) -> bool {
        self.sim.duplicate_next()
    }

    /// A JSON snapshot of the whole cluster — clock, every node's role / epoch /
    /// log, the in-flight bus, the current leaders, and the elapsed step count —
    /// for the UI to render after each action. Shape:
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
    /// steps. The UI tracks the last index it has seen (from `state`'s
    /// `step_count`) and asks only for new steps, so the timeline streams
    /// incrementally instead of re-sending the whole history each frame.
    #[must_use]
    pub fn timeline_since(&self, index: usize) -> String {
        let steps = self.sim.steps();
        let tail = steps.get(index..).unwrap_or(&[]);
        serde_json::to_string(tail).unwrap_or_else(|_| "[]".to_string())
    }
}

/// Build the voter id list `[1, 2, ..., n]`, clamping `n` to a sane 1..=7 so a
/// stray UI value can never allocate an absurd cluster.
fn voter_ids(voters: u32) -> Vec<u64> {
    let n = u64::from(voters.clamp(1, 7));
    (1..=n).collect()
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
    /// Drive the bus/timers until `pred` holds (or a step budget is exhausted),
    /// the way the JS animation loop does.
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
        assert_eq!(voter_ids(0), vec![1]);
        assert_eq!(voter_ids(3), vec![1, 2, 3]);
        assert_eq!(voter_ids(99), vec![1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn new_cluster_starts_leaderless_at_clock_zero() {
        let pg = Playground::new(3);
        let s = state(&pg);
        assert_eq!(s["nodes"].as_array().unwrap().len(), 3);
        assert_eq!(s["clock_ms"].as_u64().unwrap(), 0);
        assert!(leaders(&pg).is_empty());
        assert_eq!(s["step_count"].as_u64().unwrap(), 0);
    }

    #[test]
    fn step_drives_a_bootstrap_election_to_one_leader() {
        let mut pg = Playground::new(3);
        step_until(&mut pg, |p| !leaders(p).is_empty());
        pg.settle();
        assert_eq!(leaders(&pg).len(), 1);
        // The clock and timeline advanced as a side effect of stepping.
        assert!(state(&pg)["clock_ms"].as_u64().unwrap() > 0);
        assert!(state(&pg)["step_count"].as_u64().unwrap() > 0);
    }

    #[test]
    fn append_targets_the_leader_only_once_elected() {
        let mut pg = Playground::new(3);
        assert!(!pg.append(2), "no leader yet -> append is a no-op");
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
        assert!(pg.append(3));
        assert_eq!(log_len(&pg), before + 3);
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
        assert_eq!(node["partitioned"].as_bool(), Some(true));

        step_until(&mut pg, |p| leaders(p).iter().any(|&l| l != old));
        pg.settle();
        pg.heal(old as u32);
        pg.settle();
        assert_eq!(leaders(&pg).len(), 1, "exactly one leader after heal");
    }

    #[test]
    fn drop_next_consumes_a_bus_message() {
        let mut pg = Playground::new(3);
        step_until(&mut pg, |p| in_flight_len(p) > 0);
        let before = in_flight_len(&pg);
        assert!(before > 0);
        assert!(pg.drop_next());
        assert_eq!(in_flight_len(&pg), before - 1);
        // timeline_since exposes the recorded Drop step as JSON.
        let count = state(&pg)["step_count"].as_u64().unwrap() as usize;
        let tl: Value = serde_json::from_str(&pg.timeline_since(count - 1)).unwrap();
        assert_eq!(
            tl.as_array().unwrap().last().unwrap()["action"]["kind"],
            "Drop"
        );
    }

    #[test]
    fn reorder_and_duplicate_replay_the_bus() {
        let mut pg = Playground::new(3);
        step_until(&mut pg, |p| in_flight_len(p) > 0);
        // reorder delivers the currently-queued messages (back-to-front);
        // delivering them can enqueue fresh responses, so the bus need not end
        // empty — what we assert is that it drained the round it was given.
        assert!(pg.reorder() >= 1, "reorder delivers the queued messages");

        step_until(&mut pg, |p| in_flight_len(p) > 0);
        assert!(pg.duplicate_next(), "duplicate replays the front message");
    }

    #[test]
    fn empty_bus_faults_are_no_ops() {
        // A freshly-constructed cluster has its election timers armed but no
        // messages on the bus yet (nothing has been stepped).
        let mut pg = Playground::new(3);
        assert_eq!(in_flight_len(&pg), 0);
        assert!(!pg.drop_next());
        assert!(!pg.duplicate_next());
        assert_eq!(pg.reorder(), 0);
    }

    #[test]
    fn reset_rebuilds_the_cluster_back_at_clock_zero() {
        let mut pg = Playground::new(3);
        step_until(&mut pg, |p| !leaders(p).is_empty());
        assert!(state(&pg)["clock_ms"].as_u64().unwrap() > 0);
        pg.reset(5);
        let s = state(&pg);
        assert_eq!(s["nodes"].as_array().unwrap().len(), 5);
        assert_eq!(s["clock_ms"].as_u64().unwrap(), 0);
        assert_eq!(s["step_count"].as_u64().unwrap(), 0);
        assert!(leaders(&pg).is_empty());
    }

    #[test]
    fn five_voter_cluster_converges_to_one_leader() {
        let mut pg = Playground::new(5);
        step_until(&mut pg, |p| !leaders(p).is_empty());
        pg.settle();
        assert_eq!(leaders(&pg).len(), 1);
    }
}
