# KIP-595 Slice 3c — openraft cutover (async KraftController)

Date: 2026-05-31
Status: Approved (brainstorming) — pending spec review

## Context

Slice 3 replaces openraft with a hand-rolled KRaft engine (decomposed 3a–3d).
3a built the pure consensus core (`crates/raft/src/kraft/core.rs`); 3b built the
`KraftLog` over `crabka-log` (`crates/raft/src/kraft/log.rs`). 3c is the
**combined cutover**: a new async `KraftController` runs the core + log + the
`MetadataImage` over the real KIP-595 wire, replacing openraft entirely behind
the **unchanged** `ControllerHandle` public API, and openraft is deleted.

This is the largest, highest-risk slice. The `ControllerHandle` API is the
invariant seam: the broker and its handlers change zero lines, and the existing
test suite (`broker/tests/quorum.rs`, `raft/tests/{single_node,snapshot,reconfig}.rs`)
is the behavioral contract that must stay green against the new engine.

### Scope decisions (locked in brainstorming)

- **Records stay wincode** `MetadataRecord` through 3c (encoded into `KraftLog`
  batches via the existing bridge). The KIP-631 `KraftMetadataRecord` swap is
  **Slice 3d**.
- **Static voters only.** `add_voter`/`remove_voter`/`update_voter`/
  `change_membership`/`add_learner` return `RaftError::Unsupported`
  ("dynamic reconfig — Slice 5"). The mock-based `reconfig.rs` tests stay green
  (they exercise the coordinator against a mock `ReconfigOps`, not the engine).
  KIP-853 dynamic reconfiguration lands in **Slice 5**.
- **Observers keep `API_KEY_METADATA_FETCH` (1004)**, now served from
  `KraftLog.read_committed`. Switching observers to the real KIP-595 `Fetch`
  is deferred (Slice 6).
- **Snapshots:** 3c reimplements `trigger_snapshot` (image → checkpoint via the
  existing `SnapshotWriter`) and restart recovery (load checkpoint + replay
  log). Cross-node snapshot transfer / `FetchSnapshot` catch-up is **Slice 4**.
- **Inter-controller replication uses the real KIP-595 wire** (Fetch=1, Vote=52,
  BeginQuorumEpoch=53, EndQuorumEpoch=54), so 3c achieves genuine
  Crabka-to-Crabka KRaft. Mixed JVM+Crabka quorum is **Slice 6**.

## Architecture — the `KraftController` event loop

A single owning tokio task holds the consensus state (the 3a
`QuorumStateMachine` + the 3b `KraftLog` + an `Arc<MetadataImage>` published via
a `watch`); all access flows through an mpsc command channel, so there are no
shared locks on consensus state.

```
inbound RPC (listener)  ─┐
submit_change (handle)  ─┼─► mpsc ─► loop:
timer ticks             ─┘            event = map(command)
                                      for a in core.on_event(event, &log, now): execute(a)

execute(Action):
  SendVoteRequest / SendBeginQuorumEpoch / SendEndQuorumEpoch / SendFetch
                                  ─► PeerSender (real KRaft RPC to peer)
  AppendLeaderChange / leader submit ─► KraftLog.append
  (follower) apply fetched batches   ─► KraftLog.append_at, preceded by TruncateTo
  AdvanceHighWatermark(n)            ─► KraftLog.advance_hwm; decode newly-committed
                                       batches' records → MetadataImage; watch.send_replace
  PersistQuorumState                ─► write the quorum-state file
  ResetTimer{Election|Fetch}        ─► (re)arm a tokio timer
```

The loop also implements the timer/liveness mechanisms the 3a/3b simulations
flagged as deliberately omitted from the pure core:
- cancel the opposite timer on a role transition (a healthy follower must not
  keep an armed election timer);
- a fetch-watchdog expiry while the leader is still reachable **re-polls**, it
  does not start an election (only sustained silence elects);
- the leader periodically **re-broadcasts `BeginQuorumEpoch`** to non-fetching
  voters (KRaft resend), so a deposed leader that rejoins learns the newer epoch
  and steps down.

## Transport seam (testability)

- **`PeerSender`** (outbound trait): "send a KRaft RPC to peer N, await the
  response." Real impl wraps the existing `OutboundDialer`/`Connection`, sending
  Vote/BeginQuorumEpoch/EndQuorumEpoch/Fetch as api keys 52/53/54/1. An
  in-memory impl routes between in-process engines for the driver test.
- **Inbound:** the controller listener (`server.rs`) decodes a request and
  pushes `(Event, reply_tx)` into the engine's mpsc; the engine processes and
  replies on `reply_tx`.

This makes the engine testable as 3 in-process `KraftController` tasks over an
in-memory `PeerSender` *before* the broker runs them over real TCP.

## `ControllerHandle` mapping (API unchanged)

- `submit_change(records)`: leader → encode a wincode batch, `KraftLog.append`
  at the leader epoch, await commit (HWM ≥ offset) + apply, return per-record
  rejections; follower → forward via `API_KEY_SUBMIT_CHANGE` (1003) to the leader
  (existing mechanism, retained).
- `current_image` / `watch_image` / `watch_leader`: fed by the engine's image
  watch + leader watch.
- `quorum_state()`: from the core's `QuorumState` (leader, leader_epoch) +
  `KraftLog.hwm()` (high watermark) + the leader's per-follower fetch offsets
  (matched index). Produces the same shape `DescribeQuorum` consumes.
- `trigger_snapshot()`: serialize the image → checkpoint (`SnapshotWriter`).
- `read_snapshot_range` / `metadata_records`: unchanged (KraftLog / checkpoint
  byte reads).
- `add_voter` / `remove_voter` / `update_voter` / `change_membership` /
  `add_learner`: return `RaftError::Unsupported` (Slice 5).
- `shutdown` / `cancel`: stop the engine task + listener + timers.

## Wire & dispatch

`server.rs` inbound dispatch routes to the engine: `1` Fetch (inter-voter
replication), `52` Vote, `53` BeginQuorumEpoch, `54` EndQuorumEpoch, plus a real
`18` ApiVersions advertising them. Retained: `1003` SubmitChange-forward, `1004`
MetadataFetch (observers), `55` DescribeQuorum (broker handler). **Deleted:** the
openraft keys `1000`/`1001`/`1002` and the entire `kraft_spike` module + feature
(the real engine now serves Fetch + ApiVersions).

`network.rs` becomes the real `PeerSender` impl over `OutboundDialer`.

## Restart recovery

On start: open `KraftLog`; load the latest checkpoint → seed the
`MetadataImage` + `last_applied`; replay committed `KraftLog` batches after the
checkpoint into the image; load the `quorum-state` file → seed the core's
persistent epoch / voted-for / leader. (Cross-node `FetchSnapshot` is Slice 4.)

## openraft removal inventory

**Delete:** the `openraft` workspace dependency (`crates/raft/Cargo.toml`);
`log_store.rs`; `network.rs` (openraft adapter, becomes the `PeerSender` impl);
the `RaftStateMachine`/`RaftLogStorage`/`RaftSnapshotBuilder` impls in
`state_machine.rs` (keep the `MetadataImage` apply + recovery logic, now invoked
by the engine); `declare_raft_types!` + the `Raft` alias in `types.rs`;
`RaftError::Openraft`; `kraft_spike.rs` + its feature in `lib.rs`/Cargo.
**Keep:** `AppData`/`AppDataResponse`/`Node`/`NodeId`; `SnapshotWriter`/`Reader`
+ checkpoint format; `MetadataImage`; `reconfig.rs` (`ReconfigOps` trait +
coordinator + mock tests); `error.rs` (minus the openraft variant; add
`Unsupported`).

## Acceptance / testing

- **Driver test (new, isolation):** 3 `KraftController` tasks over an in-memory
  `PeerSender`, each with a real `KraftLog` (tempdir): elect a single leader;
  `submit_change` on a follower propagates (forward + commit + apply); leader
  failover re-elects and converges; restart recovery rebuilds the image from
  checkpoint + log.
- **Existing suite green against the new engine (the contract):**
  `broker/tests/quorum.rs` (3-node election, topic propagation, leader failover,
  follower forwarding, concurrent-topic race — now over real KIP-595 TCP),
  `raft/tests/single_node.rs`, `raft/tests/snapshot.rs` (trigger + restart),
  `raft/tests/reconfig.rs` (mock).
- **openraft-gone check:** `openraft` absent from `Cargo.toml`; the deleted
  files are gone; `cargo test -p crabka-raft -p crabka-broker` green; clippy/fmt
  clean.

## Error handling

Engine errors surface through `RaftError`. The mpsc loop never panics on a
malformed inbound RPC — it replies with the appropriate KIP-595 error code (the
core already produces typed rejections). Lost peers → the affected RPC errors
and the engine retries on the next tick (fetch/heartbeat cadence). Invariants
(single leader per epoch locally; HWM monotonic; applied ≤ HWM) guarded with
`debug_assert!`.

## Internal sequencing (still one slice / PR)

The plan builds it green-tree-internally: (1) the `KraftController` engine +
`PeerSender` seam + in-memory driver test; (2) wire into `ControllerHandle` +
`server.rs` + `network.rs` behind the unchanged API; (3) delete openraft and
make the existing broker/raft suites pass. Expect the `quorum.rs` integration to
surface real timing/wire bugs — that is the slice's purpose.

## Disposition

Permanent and pivotal: after 3c, openraft is gone and Crabka controllers speak
real KIP-595 among themselves. 3d swaps records to KIP-631; 4 adds snapshots/
FetchSnapshot; 5 adds dynamic voters; 6 proves the mixed JVM+Crabka quorum.
