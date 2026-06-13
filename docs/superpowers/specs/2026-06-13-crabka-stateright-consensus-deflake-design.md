# Crabka — stateright consensus model + deterministic-sync test infrastructure (Phase 1)

- **Date:** 2026-06-13
- **Status:** Approved design — ready for implementation planning
- **Scope:** Phase 1 of a multi-phase program ("Add stateright for Jepsen-style
  concurrency-correctness testing; replace sleep/timeout-based tests with
  deterministic, linearizable tests that can't flake").

## Problem & framing

Two genuinely different things are being conflated in the request, and they need
different tools:

1. **Jepsen-style concurrency-correctness checking.** This is what
   [`stateright`](https://github.com/stateright/stateright) is for: it explores
   the *entire* state space of an abstract `Model` (BFS/DFS over all action
   interleavings) and checks `always`/`eventually`/`sometimes` and
   **linearizability** properties. It does **not** run the real broker, real
   TCP, or the real tokio runtime — it checks a state machine.

2. **Flaky sleep-based integration tests.** The repo has ~481 `sleep(` and ~163
   `timeout(` calls across ~60 test files. The large majority are *real-broker
   integration tests* that spin up `Broker::start()` on loopback TCP and then
   `loop { if cond { break }; sleep(50ms) }` to wait for distributed state to
   converge. stateright cannot replace these — but their flakiness is fixable by
   replacing fixed-duration sleeps with **deterministic event/condition
   synchronization** (awaiting `watch` channels / `Notify`, or a paused tokio
   clock).

The two are **complementary**: a stateright model proves the *algorithm* is
correct under all interleavings; a de-flaked integration test proves the *real
wiring* matches the model without flaking.

This program is too large for one implementation plan (≈6 candidate model
subsystems + ≈60 test files). **Phase 1** (this spec) does the highest-value
vertical slice and establishes both techniques end-to-end. Later phases
(separate spec → plan cycles) extend models to share-groups, ISR, dynamic
voters, reassignment, and unclean recovery, and sweep the remaining
sleep-using tests.

### Phase 1 deliverables

1. Add `stateright` to the workspace (dev-dependency only).
2. A stateright **linearizable model of the KRaft consensus core** that wraps the
   *real* `QuorumStateMachine` production code.
3. **Deterministic-sync test infrastructure** on `BrokerHandle` + test support
   (expose watch channels; generic `wait_for_*` awaiters; paused-clock helper).
4. **De-flake the consensus-correctness test batch** onto that infrastructure.

## Grounding facts (verified against the codebase)

- The KRaft consensus core is already a pure, deterministic, side-effect-free
  state machine — no IO, no RNG, no clock reads:
  - `QuorumStateMachine` at `crates/raft/src/kraft/core.rs:36`.
  - Transition fn: `pub fn on_event(&mut self, event: Event, log: &dyn LogView, now: SimInstant) -> Vec<Action>`
    at `crates/raft/src/kraft/core.rs:113`. This is exactly stateright's
    `next_state` shape.
  - `Event` enum: `crates/raft/src/kraft/event.rs`. `Action` enum:
    `crates/raft/src/kraft/action.rs`. `QuorumState`/`Role`/types:
    `crates/raft/src/kraft/types.rs`, `role.rs`.
  - `QuorumState` is `#[derive(Debug, Clone, PartialEq)]` (`types.rs:54`) — needs
    `Eq, Hash` added for stateright state fingerprinting. `SimInstant`,
    `ReplicaKey`, `LogOffsetMetadata` are already `Eq`.
- An existing **deterministic single-schedule** sim harness already proves the
  exact wiring pattern stateright needs:
  - `crates/raft/tests/sim_harness/mod.rs` — wires N `QuorumStateMachine`s
    through an in-memory message bus + logical clock, translates each emitted
    `Action` into peer `Event`s, drives to a fixed point. Pluggable per-node log
    via `SimNodeLog` trait; in-memory `SimLog` (Clone) at `mod.rs` ~line 88.
  - `crates/raft/tests/kraft_sim.rs` (in-memory `SimLog`) and
    `kraft_log_sim.rs` (real on-disk `KraftLog`) include that harness.
  - The harness explores *one* staggered-timeout schedule and asserts invariants
    at a fixed point. stateright adds **all** interleavings + linearizability +
    automated counterexample discovery.
- The broker exposes the synchronization surface needed for de-flaking, but does
  not yet expose the watch channels to tests:
  - `BrokerHandle` at `crates/broker/src/broker.rs:174`. Read accessors:
    `controller_leader_id()` (`:222`), `broker_count()` (`:246`),
    `local_log_end_offset()` (`:338`), `partition_isr_for_test()` (`:723`),
    `partition_leader_for_test()`, `voter_count_for_test()`, `has_partition()`.
  - `watch_image()` / `watch_leader()` (`watch::Receiver`) exist on the
    `MetadataSource` trait (`crates/broker/src/metadata_source.rs:24-25`) and as
    **public** methods on `MetadataObserver`
    (`crates/broker/src/metadata_observer.rs:75,80`) — but `BrokerHandle` does
    **not** re-expose them, so every test hand-rolls a sleep-poll loop.
  - The broker/raft use **`tokio::time::Instant`** (controller deadlines via
    `tokio::time::sleep_until`), so `tokio::time::pause()` + `advance()` works in
    tests for timer-driven (Category-B) units.
  - Per-partition `hw_advance_notify` / `append_notify` exist at the `Partition`
    level but are not exposed to test APIs.
- stateright API (researched): latest **`0.31.0`** (pin `=0.31.0`; pre-1.0,
  minor bumps can be breaking). `edition = "2021"`, **no MSRV pin** — compiles
  cleanly into Crabka's edition-2024 / rustc-1.96 workspace. Ships a first-class
  **linearizability tester** in `stateright::semantics`
  (`LinearizabilityTester<ThreadId, RefObj: SequentialSpec>`,
  `serialized_history() -> Option<...>`).

## Workstream A — Raft linearizable stateright model

### Layout
- `stateright = "=0.31.0"` in `[workspace.dependencies]`; `stateright = { workspace = true }`
  under `[dev-dependencies]` of `crates/raft`. Dev-dep only.
- New `crates/raft/tests/model/mod.rs` (the model harness) and
  `crates/raft/tests/kraft_model.rs` (the `#[test]` entry points).
- The existing `sim_harness/mod.rs` and its two test binaries stay as-is (they
  also exercise the real on-disk log, which the abstract model does not). The
  model reuses the **`Action` → `Event` translation logic** from the harness as
  a reference; factor shared translation into a helper if it reduces duplication
  without coupling the two.

### Model design (low-level `Model` trait, not the actor module)
Rationale: `QuorumStateMachine` is already `Event`-in / `Action`s-out, so it *is*
the transition function. The actor module would force the code through an
`Actor` I/O shape for no benefit.

`ModelState`, `#[derive(Clone, Debug, PartialEq, Eq, Hash)]`:
- `nodes: BTreeMap<NodeId, NodeModel>` — `NodeModel { sm: QuorumStateMachine, log: SimLog }`
  holding the **real** production state machine + the existing in-memory log.
- `network` — in-flight envelopes. **Open decision (see Risks):** unordered
  `BTreeSet<Envelope>` (free reordering + auto-dedup of identical envelopes) vs.
  per-link `BTreeMap<(NodeId, NodeId), VecDeque<Msg>>` (FIFO, head-only
  deliverable). Resolve by determining whether the KRaft core assumes per-link
  FIFO. Use `BTreeSet`/`BTreeMap` (never `HashMap`) for stable `Hash`/`Eq`.
- `crashed: BTreeSet<NodeId>`.
- `linz: LinearizabilityTester<ClientId, KraftLogSpec>` — auxiliary linearizability
  state, recomputed and fingerprinted per state.

`Action`, `#[derive(Clone, Debug, PartialEq, Eq, Hash)]`:
- `Deliver(src, dst, Msg)` — pop the envelope, translate to `Event`, feed the
  **real** `sm.on_event(...)` on `dst`.
- `DropMsg(src, dst, Msg)` — message loss (remove without delivering).
- `DuplicateDeliver(src, dst, Msg)` — deliver a copy, leave the original
  in-flight (duplication).
- `Timeout(NodeId, TimerKind)` — election / fetch timeout fires. **Modeled as a
  nondeterministic fire-able action, NOT a numeric clock advance** (a `u64`
  clock in the state would explode the space — every distinct `now` becomes a
  new state). `on_event` is still called with a `SimInstant`, but the *value*
  is derived deterministically from the action (e.g. a fixed monotone bump that
  does not enter the fingerprint), so two timeout firings produce identical
  states.
- `ClientAppend(ClientId, value)` — external client request to the current
  leader; recorded as a linearizability `on_invoke`.
- `Crash(NodeId)` / `Recover(NodeId)` — `Crash` clears volatile state (keep the
  persisted `QuorumState`/log per the real persistence split); `Recover`
  restores from the persisted portion. Concurrent crashes capped at `f` in
  `within_boundary`.

`init_states` — fresh machines for the configured voter set, empty network, no
crashes, empty linearizability history.

`actions(state, out)` — push every deliverable in-flight envelope as a `Deliver`
(and `DropMsg`/`DuplicateDeliver`), every non-crashed node's enabled `Timeout`,
a bounded number of `ClientAppend`s, and `Crash`/`Recover` within the crash cap.
Every offered action must be applicable (use `None` from `next_state` only for
genuinely illegal transitions).

`next_state(state, action)` — clone state, mutate the clone, return `Some`:
1. For `Deliver`: remove envelope; if `dst` crashed, return; else translate
   `Msg`→`Event`, call `next.nodes[dst].sm.on_event(event, &log, now)` (**real
   production code**), route each emitted `Action` into network envelopes / log
   appends / truncations / HWM updates / timer state. Fold any client-visible
   commit into `linz` via `on_return`.
2. `ClientAppend`: `linz.on_invoke(client, Append(v))`; inject as a leader
   produce `Event`.
3. `Crash`/`Recover`/`DropMsg`/`DuplicateDeliver`: as described above.

`KraftLogSpec: SequentialSpec` — sequential reference model of the committed
log. `Op = { Append(value), ReadCommitted }`, `Ret = { Offset(i64), Committed(Vec<value>) }`.
`invoke` applies appends/reads against an in-memory `Vec`; the linearization
point for an append is the moment the HWM advances past it. (A Kafka log is not
a single-value register, so we implement our own `SequentialSpec` rather than
reuse the built-in `register`.)

### Properties
- `Property::always("election_safety", …)` — at most one leader per epoch.
- `Property::always("log_matching", …)` — any two logs agreeing at `(offset)`
  agree on the entry's `epoch`, and on the whole prefix up to it.
- `Property::always("leader_completeness", …)` — an entry committed (≤ HWM) in
  some epoch is present in the logs of all higher-epoch leaders; committed
  entries are never lost across a leader change.
- `Property::always("monotonic_hwm", …)` — no node's HWM regresses.
- `Property::always("linearizable", |_, s| s.linz.serialized_history().is_some())`.
- `Property::sometimes("leader_elected", …)` — anti-vacuity witness.
- `Property::sometimes("entry_committed", …)` — anti-vacuity witness (HWM
  advanced past a client append).

The two `sometimes` witnesses are mandatory: without them, a model that
deadlocks or is over-pruned makes all `always` invariants vacuously true and the
test passes green while checking nothing.

### State-space bounding (keep CI fast)
- `within_boundary(&self, &State) -> bool` — the primary finiteness knob. Cap:
  in-flight message count, total client appends, max epoch, max log length,
  concurrent crashes ≤ `f`. (Unbounded epoch/offset counters otherwise make the
  space infinite and BFS never terminates.)
- `symmetry()` via a `Representative` impl that canonicalizes node-id relabeling
  (consensus models are highly symmetric under node permutation — up to `n!`
  reduction). Validate the canonicalizer against a non-symmetry run on a tiny
  config so it can't hide counterexamples.
- `target_max_depth(d)` as a hard cap.
- **CI config:** 3 voters, ≤2 client appends, ≤1 crash, small inflight/epoch/log
  caps → completes in seconds under `cargo test -p crabka-raft`. Larger configs
  (e.g. 5 voters) gated behind `#[ignore]` and/or a nightly job;
  `spawn_simulation()` as a randomized fallback for spaces too large to exhaust.

### Source change required
Add `Eq, Hash` derives to `QuorumState` (`types.rs:54`), `Role` (`role.rs`),
and any nested types they own that are not already `Eq`/`Hash`. Confirm no field
blocks `Hash`/`Eq` (no `f64`, no `HashMap`). `Action::TransitionedTo(&'static str)`
is fine. This is a small, mechanical, side-effect-free change to production
source; it does not alter behavior.

## Workstream B — Deterministic-sync test infrastructure

### Broker hooks (`crates/broker/src/broker.rs`)
- `#[doc(hidden)] pub fn watch_image_for_test(&self) -> watch::Receiver<Arc<MetadataImage>>`
  and `watch_leader_for_test(&self) -> watch::Receiver<Option<NodeId>>` — delegate
  to the existing `MetadataObserver` channels. (`_for_test` naming matches the
  existing convention; greenfield, so no compatibility concern.)
- `pub async fn wait_for_image<F: Fn(&MetadataImage) -> bool>(&self, pred: F)` —
  check the current value, then `rx.changed().await` until `pred` holds. No fixed
  sleeps. Wrap the whole wait in a generous overall `tokio::time::timeout`
  (≈30 s) safety-net so a genuinely-stuck condition fails loudly with a clear
  message rather than hanging the suite.
- Convenience wrappers built on `wait_for_image`:
  `wait_until_brokers_registered(n)`, `wait_until_partition_leader(topic, p, broker)`,
  `wait_until_isr(topic, p, expected)`, `wait_until_log_end_offset_at_least(topic, p, off)`.
- Per-partition progress: expose the existing `Partition` `hw_advance_notify` /
  `append_notify` via `_for_test` accessors (or a `watch`) so replication tests
  await HW/LEO progress events deterministically instead of sleeping. (Metadata
  image watch does not reflect local LEO/HWM.)

### Test support (`crates/broker/tests/support/mod.rs`)
- Rewrite the internal sleep-poll loops in `start_n_node` / `start_n_node_with`
  / `wait_for_all_brokers_registered` to use the new awaiters. Preserve the
  existing `start_n_node_with_retry` outer-retry behavior (it guards against
  loopback ephemeral-port exhaustion / election starvation when many 3-node
  clusters start concurrently — that is a real-system constraint, not timing
  flakiness).
- Add a small `paused_clock` helper module (`tokio::time::pause()` + `advance()`)
  for Category-B timer/backoff unit tests (later phases; Phase 1 uses it only if
  a Phase-1-batch test needs it).

## Workstream B applied — Phase-1 de-flake batch

Convert these files from `loop { if cond { break }; sleep() }` to the new
`wait_for_*` awaiters. **All correctness assertions are preserved**; only the
*waiting* changes:

- `crates/broker/tests/quorum.rs`
- `crates/broker/tests/leader_election.rs`
- `crates/broker/tests/replication.rs`
- `crates/broker/tests/durability.rs`
- `crates/broker/tests/leader_epoch.rs`
- `crates/raft/tests/single_node.rs`
- `crates/raft/tests/snapshot.rs`

(`crates/raft/tests/kraft_engine_sim.rs` is already deterministic — left as-is.)
These subsystems' correctness properties are now *also* covered exhaustively by
the Workstream-A model; the integration tests verify the real wiring matches.

## Verification plan

- `cargo test -p crabka-raft` runs the bounded model check and passes
  deterministically (same result every run, no wall-clock dependence).
- **Anti-vacuity proof:** temporarily disable a known invariant gate (e.g. the
  leader-completeness / `epoch_start_offset` HWM gate in
  `recompute_high_watermark`) and confirm the model produces a counterexample
  path — proving the check is not vacuous. Revert.
- **Flake proof:** run the de-flaked batch ≈50× in a loop; confirm zero failures
  and measure wall-clock (expected faster — no fixed sleeps on the happy path).
- `cargo fmt` before pushing (CI gates on it). On Windows deep worktrees use
  `cargo fmt -p <crate>` per-crate to avoid the OS 206 path-length failure.
- clippy pedantic clean (workspace lint config).

## Risks & open questions (resolved during implementation)

1. **Network ordering.** Does the KRaft core assume per-link FIFO delivery
   (TCP-like) or tolerate reordering? Decides `BTreeSet` (unordered, free
   reorder + dedup) vs. per-link `VecDeque` (FIFO, head-only deliverable). Wrong
   choice → phantom counterexamples (modeling impossible reorderings) or missed
   bugs. Resolve by reading the vote/fetch/begin-quorum handling in
   `kraft/core.rs` before fixing the network model.
2. **Hashability.** Confirm no `QuorumState`/`Event`/`Action`/`Role` field blocks
   `Eq`/`Hash` (no float, no `HashMap`). If one exists, normalize it (e.g. swap a
   `HashMap` for `BTreeMap`, or use `stateright::util::HashableHashMap`).
3. **State explosion.** Even bounded, 3-voter + loss + crash may be large. Tune
   `within_boundary` caps + symmetry; fall back to `spawn_simulation` for larger
   configs. `serialized_history()` is potentially expensive as an `always`
   property — bound concurrent client ops aggressively.
4. **Liveness is weak in stateright.** It only finds liveness counterexamples on
   finite terminal paths, not infinite fairness-violating cycles. We rely on
   bounded `always` + `sometimes` witnesses, not `eventually`, for
   election/commit progress. True liveness proof is out of scope.
5. **Pre-existing flakes out of scope.** The Windows share-group test
   timeout/flake is pre-existing and not part of this slice.

## Out of scope (later phases)

- stateright models for share-groups (`AcquisitionState`, wrap-real), ISR/HWM
  (`ReplicaState`), dynamic voters (KIP-853 — the survey flagged a real
  "no joint consensus / two-disjoint-majorities" question worth model-checking),
  partition reassignment (KIP-455), unclean recovery (KIP-966), KIP-848
  rebalance. Transactions/EOS are a **poor** fit until the coordinator's async
  I/O is untangled from the `TxnState` machine.
- De-flaking the remaining ~55 sleep-using test files (Category-A integration
  across broker/grpc-gateway/client-streams/rebalancer/integration-tests/etc.,
  and Category-B timer units). The JVM/testcontainers tests (`jvm_*`,
  `describe_groups_jvm`, `jvm_differential`) stay sleep-based — they drive real
  Docker JVM Kafka and cannot be made deterministic.
