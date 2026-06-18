# Model Failover Between Client and Server in Stateright — Design

**Date:** 2026-06-17
**Status:** Approved (design); spec under review
**Workstream:** A (formal verification) + client failover correctness

## Goal

Verify failover as a client-visible contract, not just as isolated broker logic:
when the partition leader dies or metadata remains stale, the producer must
recover quickly and efficiently, and an `acks=all` batch that was acknowledged
must not be lost or duplicated across the broker leader change.

The existing models cover the broker data path and producer-state core, but they
do not put the client retry/routing loop in the same fault story. The benchmark
frontier shows exactly that missing seam: metadata can continue to advertise a
dead leader, and the producer can wedge on that leader even though the cluster
should eventually elect a replacement. This design adds a small client routing
model plus a bounded client+broker safety composition instead of folding
everything into one enormous state space.

## Scope

**In:**
- One producer, one topic-partition, three broker ids.
- Cached leader metadata, stale metadata, leader hints, metadata refresh, broker
  liveness, transport failure, retry backoff, and routing budget.
- Clean broker failover with `acks=all`, ISR/HWM durability, and idempotent
  producer sequence preservation across retry.
- Efficiency counters: send attempts, metadata refreshes, stale-leader attempts,
  and abstract recovery steps after the killed leader.

**Out:**
- Multiple topics/partitions; cross-partition throughput fairness.
- Transactions/EOS coordinator failover.
- Raft consensus internals; the broker safety composition continues to drive the
  existing failover decision core as the controller's result.
- Wall-clock performance benchmarking inside stateright. The model checks
  bounded abstract steps and operation counts; a concrete async regression test
  covers the sender's request-timeout behavior.

## Approach

Build two coupled but separately exhaustive models.

### Model A — Client failover liveness and efficiency

Add a stateright model in `crates/client-producer`, near the sender/transport
seam. It models the minimum state needed to reproduce the known wedge:

- `actual_leader`: the live partition leader according to the cluster.
- `cached_leader`: the producer's current leader cache.
- `live`: broker liveness bitset.
- `metadata_view`: whether the next metadata response is stale or fresh.
- `batch`: one prepared batch with fixed `producer_id`, `producer_epoch`, and
  `base_sequence`.
- Counters: `attempts`, `refreshes`, `stale_leader_attempts`,
  `steps_after_failure`.

Actions:
- `Send`: route by `cached_leader`; success only if the target is live and is
  `actual_leader`.
- `TransportFail`: dead target produces a retry, eviction, backoff, and refresh
  requirement.
- `NotLeader`: live but wrong target returns a routing error and optional leader
  hint.
- `RefreshMetadata`: refresh may be stale for a bounded number of steps, then
  must become fresh.
- `ElectNewLeader`: cluster changes `actual_leader` after a leader death.
- `ExpireBudget`: the batch fails terminally once the routing budget is exceeded.

Properties:
- **No infinite dead-leader loop:** once a target is dead, the model cannot keep
  sending to that broker forever without either refreshing to a live leader or
  expiring the routing budget.
- **Quick recovery:** after `actual_leader` changes, the batch reaches the new
  leader, or fails terminally, within a small bounded number of abstract steps.
- **Efficient recovery:** sends and refreshes are capped; a single failover does
  not burn repeated full request-timeout attempts on the same known-dead broker.
- **Retry preserves identity:** every resend uses the same `producer_id`,
  `producer_epoch`, and `base_sequence`.
- Non-vacuity witnesses: stale metadata occurs; a dead leader is attempted; a
  refresh fixes the cache; a batch succeeds after failover; the budget-expiry
  path is reachable.

The model should reuse or extract pure sender decision helpers where practical:
leader resolution, verdict classification, refresh-needed classification, and
backoff/budget decisions. It should not attempt to run async I/O inside
stateright.

### Model B — Client+broker failover safety

Add a broker-crate sibling to `data_path_model.rs` rather than bloating that file
in place unless the implementation reads cleaner as an extension. The model
composes:

- broker log/HWM/ISR/failover cores from `data_path_model.rs`;
- idempotent-producer decision logic from `ProducerState::check_pure`;
- a tiny producer client with one prepared batch and a cached leader.

State:
- per-broker log vectors and HWM/ISR/leader epoch, as in the existing data-path
  model;
- producer ghost state: next sequence, in-flight batch, acked offsets, and a set
  of `(producer_epoch, base_sequence)` values already accepted;
- routing state: cached leader and a pending metadata refresh flag.

Actions:
- `ClientSend`: send the prepared batch to the cached leader.
- `BrokerAccept`: if routed to the live leader, drive `check_pure` and append the
  batch once.
- `BrokerDuplicate`: if the same batch lands again after retry, it returns the
  original offset rather than appending again.
- `Misroute`: wrong or dead leader causes routing retry/refresh, not sequence
  allocation.
- `Replicate`, `AdvanceHwm`, `Die`, `Failover`, `ExpandIsr`: as in the broker
  data-path model.

Properties:
- **Acked-all durability:** any batch acknowledged after reaching HWM remains in
  every future clean leader's committed prefix.
- **No duplicate append:** retrying the same prepared batch cannot create two
  committed offsets for the same `(producer_epoch, base_sequence)`.
- **No sequence skip on reroute:** stale metadata and transport retries do not
  advance the producer sequence until a fresh batch is prepared.
- **Fencing/out-of-order characterized:** if the model reaches
  `INVALID_PRODUCER_EPOCH` or `OUT_OF_ORDER_SEQUENCE_NUMBER`, it must be through
  an explicitly modeled invalid epoch/sequence transition, not through normal
  clean failover retry.
- Non-vacuity witnesses: ack before failover; retry after failover; duplicate
  response; clean leader change with acknowledged data preserved.

## Concrete sender regression

Add one focused async test around the existing `ProduceTransport` mock seam. It
should drive the real sender decision path through:

1. cached leader dies;
2. first produce attempt fails;
3. sender evicts, marks refresh-needed, and parks the same prepared batch;
4. metadata refresh reports the new leader;
5. resend reaches the new leader without waiting through multiple full request
   timeouts.

The assertion should be behavioral and cheap: one transport failure before
refresh, bounded number of sends, one or two metadata refreshes, and the ack
resolves. It is not a throughput benchmark; it protects against the specific
regression where failover waits through repeated request timeouts on the dead
leader.

## Tractability

State explosion is the main risk. Controls:
- one topic-partition and one prepared batch;
- three brokers, but at most one dead broker at a time in Model A;
- metadata may be stale only for a small configured bound, such as two refreshes;
- counters are capped and included only where they gate properties;
- no wall-clock timestamps in the model; use abstract steps and operation counts;
- each checker run uses `within_boundary`, `target_state_count`, `timeout`, and
  the same host memory watchdog discipline as the existing broker models.

If Model B explodes, trim it before weakening properties: start with two brokers,
shorter logs, and no unclean elections. The core goal is clean-failover safety
with client retries, not exhaustive exploration of every recovery strategy.

## Incremental Build

1. Extract or expose pure helper functions from `sender.rs` only where the model
   needs them; keep production behavior unchanged.
2. Build Model A with stale metadata, dead leader, refresh, retry, and efficiency
   counters. Tune bounds until exhaustive.
3. Add the concrete async sender regression test against a mock
   `ProduceTransport`.
4. Build Model B as a sibling broker safety composition with one prepared batch.
5. Scale bounds under the watchdog and add non-vacuity witnesses.

## RED Handling

A counterexample is useful only after sorting faithfulness from a real bug:

- If the model permits behavior the concrete sender or broker cannot produce,
  tighten the adapter/action generator.
- If it reproduces the benchmark wedge, fix the client routing/refresh path and
  keep the counterexample as a regression shape.
- If it finds broker-side loss or duplicate append across clean failover, fix
  the relevant producer-state, HWM/ISR, or failover seam before expanding bounds.

## Verification Discipline

- Run stateright checkers with explicit depth/state/timeout caps and the host
  memory watchdog.
- Run the focused async sender regression test.
- Run `cargo +nightly fmt`.
- Run targeted crate tests first, then the relevant `cargo clippy --all-targets
  -- -D warnings` scope once the models are stable.

## Success Criteria

1. Model A proves quick bounded recovery or bounded terminal failure under stale
   metadata and dead-leader failover, with capped sends/refreshes.
2. Model B proves clean-failover safety with the client in the loop: acknowledged
   `acks=all` batches are not lost, and retries do not duplicate or skip
   sequence state.
3. Non-vacuity witnesses show the failover/retry paths actually occur.
4. The concrete sender regression test proves the real path refreshes and
   reroutes after the first transport failure rather than waiting through
   repeated full request timeouts.
