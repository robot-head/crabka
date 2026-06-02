# KIP-595 Slice 3a — KRaft consensus core (pure state machine)

Date: 2026-05-31
Status: Approved (brainstorming) — pending spec review

## Context

Slice 3 of the KIP-595 program replaces the `openraft` crate with a hand-rolled
KRaft consensus engine speaking the real KIP-595 wire. The exploration showed
openraft is coupled through ~9 files in `crates/raft`, but the broker depends
only on the `ControllerHandle` public API — so the engine can be swapped beneath
an unchanged handle.

Slice 3 is itself a mini-program, decomposed into sub-slices that each leave the
tree green:

- **3a — KRaft consensus core (this doc):** the pure quorum state machine.
- **3b — KRaft log + pull replication** over `crabka-log`.
- **3c — wire integration & cutover:** drive core+log from the controller
  listener on the real api keys (1, 52–54), replace the `Raft<TypeConfig>`
  instance behind the unchanged `ControllerHandle`, delete openraft.
- **3d — record migration:** move the state machine + log entries + broker
  handlers onto the Slice-1 `KraftMetadataRecord` layer; drop the wincode enum.

Cutover strategy: **incremental** — openraft stays the live engine through
3a–3b (new code built/tested in isolation, not wired); the single switch happens
in 3c.

## Goal & scope

A **pure, deterministic, sans-IO** implementation of the KIP-595 + KIP-996
quorum state machine — roles, terms/leader-epochs, voting (incl. pre-vote),
leadership, and high-watermark advancement — as a standalone module in
`crates/raft` (`src/kraft/core/`). It owns consensus *logic and state* only.

**Explicitly NOT in scope (deferred):**
- Wire encode/decode (validated in Slice 2), the real log + `Fetch` byte serving
  (3b), `quorum-state` file persistence (3c), the `ControllerHandle` cutover
  (3c), record migration (3d), KIP-853 reconfig, KIP-630 snapshots.
- No IO, no async, no clock, no networking. openraft remains the live engine;
  this code is not wired to anything in 3a.

## Architecture — event/action core

One synchronous, allocation-light function is the heart:

```rust
fn on_event(&mut self, event: Event, log: &dyn LogView, now: Instant) -> Vec<Action>
```

- **`Event`** (inputs): `ElectionTimeout`, `FetchTimeout`, `ReceiveVoteRequest`,
  `ReceiveVoteResponse`, `ReceiveBeginQuorumEpoch`, `ReceiveBeginQuorumEpochResponse`,
  `ReceiveEndQuorumEpoch`, `ReceiveEndQuorumEpochResponse`, `ReceiveFetch`
  (leader side: a follower's fetch offset+epoch), `ReceiveFetchResponse`
  (follower side: records appended / diverging epoch / new leader).
- **`Action`** (outputs, executed by 3b/3c): `SendVoteRequest { pre_vote }`,
  `SendBeginQuorumEpoch`, `SendEndQuorumEpoch`, `SendFetch`, `Reply(resp)`,
  `TransitionedTo(role)`, `PersistQuorumState`, `AppendLeaderChange`,
  `AdvanceHighWatermark(offset)`, `ResetTimer { kind, deadline }`.
- **`LogView`** (injected read-only trait): `last_offset()`, `last_epoch()`,
  `end_offset_for_epoch(epoch)`. Lets the core reason about log up-to-dateness
  and divergence without owning bytes (3b provides the real impl; tests provide
  a fake).

Time and randomized election jitter are passed in (`now`, and a seeded jitter
supplied by the caller), so every test is reproducible — mirroring Kafka's own
deterministic raft simulation. (Avoids the `Date.now()`/`Math.random()` hazard.)

## State model

**Persistent quorum state** (the logical content of the `quorum-state` file;
struct lives here, file IO deferred to 3c):

```text
cluster_id, leader_epoch (i32), leader_id: Option<i32>,
voted_key: Option<ReplicaKey>, voters: VoterSet
```

**Roles** (Kafka 4.0 set):

- `Unattached` — knows the epoch, no leader; may hold a non-binding pre-vote grant.
- `Voted` — granted a real vote this epoch (binding; persisted).
- `Follower` — has a leader for the epoch; fetches from it.
- `Prospective` — KIP-996 pre-vote candidate (gathering non-binding grants).
- `Candidate` — real candidacy (bumped epoch, voted for self).
- `Leader` — won; tracks each follower's fetch offset for HWM.
- `Resigned` — stepping down; emitting `EndQuorumEpoch`.
- `Observer` — not in the voter set; only ever fetches, never elects/votes.

## Transition rules covered

- **Election with pre-vote:** `ElectionTimeout` (voter only) → `Prospective`,
  broadcast `Vote { pre_vote = true }`. Pre-vote majority → `Candidate`: bump
  epoch, persist self-vote, broadcast `Vote { pre_vote = false }`. Real majority
  → `Leader`: bump to its leader epoch, emit `AppendLeaderChange` (the control
  record establishing the new epoch + voter set). An observer never starts an
  election.
- **Vote granting:** grant iff the candidate's epoch ≥ ours AND we have not cast
  a binding vote for someone else this epoch AND the candidate's
  `(last_epoch, last_offset)` is at least as up-to-date as ours. A pre-vote grant
  is non-binding (does not persist `voted_key` or change epoch); a real-vote
  grant transitions us to `Voted` and persists.
- **BeginQuorumEpoch:** a leader announces itself → become `Follower` of
  `(leader_id, epoch)`, reset the fetch timer.
- **EndQuorumEpoch:** the leader is resigning → begin an election immediately
  (skip the election-timeout wait).
- **Leader high-watermark:** from the follower fetch offsets plus the leader's
  own end offset, compute the majority-replicated offset; advance HWM
  monotonically and only within the current leader epoch (a leader never commits
  entries from a prior epoch by count alone — KIP-595's leader-completeness rule).
- **Diverging epoch:** given a follower's `(last_fetched_epoch, fetch_offset)`,
  the leader detects divergence against its log and decides the diverging-epoch
  hint to return (the follower-side truncation is applied by 3b; the *decision*
  is core).

## Testing / acceptance

- **Rule-level unit tests** for every transition: grant vs deny vote; pre-vote
  vs real vote (non-binding vs binding, epoch effects); HWM majority math and
  monotonicity; leader-completeness (no cross-epoch commit-by-count); divergence
  detection; observer never elects; epoch monotonicity; EndQuorumEpoch triggers
  immediate election.
- **Deterministic multi-node simulation (headline acceptance):** N in-memory
  cores wired through an in-memory message bus with a controllable clock. Assert:
  (1) a fresh cluster elects **exactly one** leader within bounded simulated
  time; (2) a simulated network partition then heal re-converges to a single
  leader; (3) committed offsets agree across nodes. Fully reproducible, no
  Docker — the strongest isolation-level proof of consensus correctness before
  any wire contact.

## Error handling

The core is total: malformed or stale events produce a no-op or a typed rejection
`Action` (e.g. a `Reply` with the appropriate KIP-595 error code), never a
panic. Stale-epoch messages are ignored or answered with the current epoch per
KIP-595. Invariants (e.g. only one leader per epoch locally; HWM never
regresses) are guarded with `debug_assert!`.

## Disposition

Permanent. This module is the consensus engine 3b/3c wire up and that ultimately
replaces openraft. It carries no openraft dependency and is independently
testable forever.
