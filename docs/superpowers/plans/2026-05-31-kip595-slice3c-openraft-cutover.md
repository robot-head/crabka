# KIP-595 Slice 3c — openraft Cutover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace openraft with an async `KraftController` that drives the 3a consensus core + 3b `KraftLog` + `MetadataImage` over the real KIP-595 wire (Fetch=1, Vote=52, BeginQuorumEpoch=53, EndQuorumEpoch=54), behind the unchanged `ControllerHandle` API, and delete openraft.

**Architecture:** A single owning tokio task holds all consensus state; commands (inbound RPCs, `submit_change`, timer ticks) arrive over an mpsc and are turned into core `Event`s whose `Action`s the loop executes (send peer RPCs via a `PeerSender` seam, append/truncate the log, advance HWM, apply committed records to the image). The `ControllerHandle` public API is preserved so the broker is untouched; the existing test suite is the behavioral contract.

**Tech Stack:** Rust, tokio (mpsc/watch/select/time), the 3a `kraft` core + 3b `KraftLog`, `crabka-client-core` (`OutboundDialer`/`Connection`) for outbound RPCs, existing `wire.rs`/`SnapshotWriter`. Removes `openraft`.

**Spec:** [docs/superpowers/specs/2026-05-31-kip595-slice3c-openraft-cutover-design.md](../specs/2026-05-31-kip595-slice3c-openraft-cutover-design.md)

---

## Background the implementer needs

- **The invariant:** `ControllerHandle`'s public methods (signatures + semantics) must not change — the broker and its handlers call them and must not be edited. Methods: `submit_change`, `current_image`, `watch_image`, `watch_leader`, `quorum_state`, `controller_bound_addr`, `trigger_snapshot`, `read_snapshot_range`, `metadata_records`, `fetch_metadata_from`, `forward_submit_to`, `add_learner`, `change_membership`, `add_voter`, `remove_voter`, `update_voter`, `shutdown`, `cancel`, and the `ReconfigOps` impl. (See `crates/raft/src/controller.rs`.)
- **3a core** (`crates/raft/src/kraft/core.rs`): `QuorumStateMachine::{new, on_event(Event,&dyn LogView,SimInstant)->Vec<Action>, quorum_state()->&QuorumState, role()->&Role, is_voter()}`. `Event`/`Action`/`Role` in sibling modules. `LeaderEpoch=u32`, `NodeId=u64`, `SimInstant(u64 ms)`.
- **3b log** (`crates/raft/src/kraft/log.rs`): `KraftLog::{open, append, append_at, read_committed, read_decoded, truncate_to, advance_hwm, hwm, log_end_offset, log_start_offset}` + `impl LogView`.
- **Records stay wincode** `crabka_metadata::MetadataRecord`. A submitted batch is `Vec<MetadataRecord>` → one record per `MetadataRecord` value via `crabka_metadata::kafka_record` (used by `metadata_fetch.rs`/`snapshot.rs` today). The `MetadataImage::{validate, apply}` logic stays; the engine calls it on commit. (KIP-631 record swap is Slice 3d.)
- **Wire** (`crates/raft/src/wire.rs`): retained api keys `API_KEY_SUBMIT_CHANGE=1003`, `API_KEY_METADATA_FETCH=1004`. Deleted: `API_KEY_APPEND_ENTRIES=1000`, `API_KEY_VOTE=1001`, `API_KEY_INSTALL_SNAPSHOT=1002`. New (real KIP-595, already validated in Slice 2): Fetch=1, Vote=52, BeginQuorumEpoch=53, EndQuorumEpoch=54, ApiVersions=18; their codecs are the generated `crabka_protocol` types from Slice 2.
- **Outbound** (`crates/raft/src/network.rs`): `OutboundDialer::dial(node_id, addr, opts) -> Connection`; `Connection::raw_request(api_key, version, body) -> Bytes`. `PlaintextDialer` is the no-broker fallback.
- **Server** (`crates/raft/src/server.rs`): per-conn loop reads `(api_key, api_version, correlation_id, body)` (RequestHeader v2) and dispatches; `dispatch()` at line 310; the `kraft_spike` `#[cfg(feature)]` block + the openraft conversions are removed here.
- **Determinism for the driver test:** the engine takes time from `tokio::time` in production, but the **driver test** injects a controllable clock + an in-memory `PeerSender`, so it's reproducible (mirrors the 3a/3b sims). Stagger per-node election timeouts.
- This slice WILL surface integration bugs in `quorum.rs`. The 3a/3b sims flagged the mechanisms the loop must implement: cancel the opposite timer on role change; fetch-watchdog while leader-reachable re-polls (not elect); leader re-broadcasts `BeginQuorumEpoch` to non-fetching voters.

## File Structure

| Path | Change |
|------|--------|
| `crates/raft/src/kraft/controller.rs` (new) | `KraftController` engine: state, event loop, timers, apply, submit/forward, quorum_state, recovery, snapshot trigger. |
| `crates/raft/src/kraft/transport.rs` (new) | `PeerSender` trait + the inbound `EngineCommand`/`Event` plumbing types; in-memory `PeerSender` for tests. |
| `crates/raft/src/kraft/mod.rs` | export the above. |
| `crates/raft/src/controller.rs` | `ControllerHandle` now owns a `KraftController` instead of `Arc<Raft>`; methods delegate; reconfig methods → `Unsupported`. |
| `crates/raft/src/server.rs` | dispatch real api keys (1/52/53/54/18) to the engine; keep 1003/1004; delete 1000/1001/1002 + the kraft_spike block + openraft conversions. |
| `crates/raft/src/network.rs` | becomes the real `PeerSender` impl over `OutboundDialer` (Vote/Begin/End/Fetch). |
| `crates/raft/src/state_machine.rs` | drop the `RaftStateMachine`/`RaftLogStorage`/`RaftSnapshotBuilder` impls; keep `MetadataImage` apply + checkpoint recovery as plain helpers the engine calls. |
| `crates/raft/src/types.rs` | remove `declare_raft_types!` + `Raft` alias; keep `AppData`/`AppDataResponse`/`Node`/`NodeId`. |
| `crates/raft/src/error.rs` | remove `Openraft` variant; add `Unsupported(&'static str)`. |
| `crates/raft/src/{log_store.rs, kraft_spike.rs, kraft_spike_metadata_log.bin}` | DELETE. |
| `crates/raft/Cargo.toml` | remove `openraft` dep + the `kraft-spike` feature. |
| `crates/broker/Cargo.toml` | remove the `kraft-spike` feature forward. |
| `crates/raft/tests/kraft_engine_sim.rs` (new) | in-process multi-node async driver test (isolation acceptance). |

---

## Task 1: `PeerSender` seam + `KraftController` skeleton

**Files:** create `crates/raft/src/kraft/transport.rs`, `crates/raft/src/kraft/controller.rs`; modify `crates/raft/src/kraft/mod.rs`.

- [ ] **Step 1: Define the transport seam + command types** (`transport.rs`)

```rust
//! Transport seam for the KraftController: outbound peer RPCs go through
//! `PeerSender` (real TCP in prod, in-memory in tests); inbound RPCs and
//! handle commands arrive as `EngineCommand`.

use bytes::Bytes;
use crate::error::RaftError;
use crate::kraft::types::NodeId;

/// A decoded inbound KIP-595 RPC plus a oneshot to reply on.
#[derive(Debug)]
pub enum Inbound {
    Vote { req: Bytes, reply: tokio::sync::oneshot::Sender<Bytes> },
    BeginQuorumEpoch { req: Bytes, reply: tokio::sync::oneshot::Sender<Bytes> },
    EndQuorumEpoch { req: Bytes, reply: tokio::sync::oneshot::Sender<Bytes> },
    Fetch { req: Bytes, reply: tokio::sync::oneshot::Sender<Bytes> },
}

/// Outbound peer RPC sender. Returns the raw response body.
#[async_trait::async_trait]
pub trait PeerSender: Send + Sync {
    async fn send(&self, peer: NodeId, api_key: i16, body: Bytes) -> Result<Bytes, RaftError>;
}
```

(Confirm `async_trait` is available in the workspace; `network.rs`'s `OutboundDialer` already uses an async trait — match its mechanism, whether `async_trait` or native async-fn-in-trait. Use the same.)

- [ ] **Step 2: `KraftController` skeleton** (`controller.rs`)

A struct owning the consensus state + an mpsc sender for commands; `spawn` starts the loop task. Write a failing test that constructs a single-voter engine and reads back its (initially Unattached) `quorum_state`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn single_voter_engine_starts_unattached() {
        // builds a KraftController over a tempdir KraftLog, single voter {1}
        // asserts quorum_state().leader_id is None initially
    }
}
```

Implement just enough (`KraftController::new(...) -> (handle, loop_future)` or a `spawn`) for the test to pass. The full loop is Task 2. Commit `feat(raft): KraftController skeleton + PeerSender seam`.

---

## Task 2: The event loop — inbound RPC → core → execute actions

**Files:** `crates/raft/src/kraft/controller.rs`

Build the core loop: receive `Inbound`/commands, decode to a core `Event`, call `core.on_event(event, &log, now)`, and execute each `Action`:
- `SendVoteRequest`/`SendBeginQuorumEpoch`/`SendEndQuorumEpoch`/`SendFetch` → encode the KIP-595 request (Slice-2 generated types) and `PeerSender::send` to each peer; feed the response back as the matching `Event` (e.g. `ReceiveVoteResponse`).
- `AppendLeaderChange{epoch}` → append a leader-change/marker batch to `KraftLog` at `epoch`.
- follower applying fetched records → `KraftLog.append_at` (preceded by `TruncateTo` when the action says so).
- `AdvanceHighWatermark(n)` → `KraftLog.advance_hwm(n)`; decode the newly-committed batches (offsets `prev_hwm..n`) into `MetadataRecord`s, `MetadataImage::validate`+`apply` each, publish via the image `watch`.
- `PersistQuorumState` → write the quorum-state file (Task 5 provides the writer; here just call it).
- `ResetTimer{..}` → record the deadline for the timer task (Task 3).
- replies: for an inbound `Vote`/`Fetch`/etc., the matching `Reply*`/produced response is encoded and sent on the `oneshot`.

**Test (contract):** drive the loop directly (no network) by feeding `Event`s through a test-only entrypoint and asserting the log/image mutate correctly (e.g. a synthesized leader-change + a committed record batch applies to the image). The real multi-node behavior is Task 6's driver test. Commit `feat(raft): KraftController event loop (apply, replicate, reply)`.

---

## Task 3: Timers & liveness reconciliation

**Files:** `crates/raft/src/kraft/controller.rs`

Add the tokio timer machinery: an election timer and a fetch timer driven by `ResetTimer` actions, plus a leader heartbeat interval. Implement the three mechanisms the sims flagged:
1. on a role transition, cancel the now-irrelevant timer (follower → no election timer; leader → no fetch timer, runs heartbeat);
2. a fetch-timer expiry while the leader is still reachable re-issues `SendFetch` (re-poll), it does NOT start an election; only a sustained fetch timeout (configurable misses) elects;
3. the leader re-broadcasts `BeginQuorumEpoch` to voters that haven't fetched recently, each heartbeat tick.

**Test:** unit-test the timer-reconciliation decisions where feasible; full coverage comes from Task 6 (partition/heal must re-elect; a healthy follower must not spuriously elect). Commit `feat(raft): KraftController timers + liveness (heartbeat, fetch-watchdog, timer reconciliation)`.

---

## Task 4: Handle-facing ops — submit_change, quorum_state, watches, snapshot

**Files:** `crates/raft/src/kraft/controller.rs`

- `submit_change(records) -> Result<(), RaftError>`: leader → append the wincode batch at the leader epoch, register a waiter keyed by the appended offset, return once HWM ≥ offset AND applied (surfacing per-record rejections from `MetadataImage::validate`); follower → `forward_submit_to(leader, addr, records)` via `API_KEY_SUBMIT_CHANGE` (reuse the existing `forward_submit_to`/wire). Pre-validate against the current image first (as today) to fail fast.
- `quorum_state() -> QuorumState`: from `core.quorum_state()` (leader_id, leader_epoch) + `log.hwm()` (high_watermark / last_applied) + the leader's per-follower fetch offsets (matched index). Keep the existing `QuorumState` shape `DescribeQuorum` consumes.
- `watch_leader`/`watch_image`: expose the engine's `watch::Receiver`s.
- `trigger_snapshot()`: serialize the current image to a checkpoint via `SnapshotWriter` (Task 5 wires recovery; this is the write side).

**Test:** single-voter engine: `submit_change(create topic)` commits + appears in `current_image`; duplicate rejected (`MetadataImage::validate`); `quorum_state()` reports leader=self, hwm advanced. Commit `feat(raft): KraftController submit_change/quorum_state/watches/snapshot-trigger`.

---

## Task 5: Restart recovery + quorum-state file

**Files:** `crates/raft/src/kraft/controller.rs` (+ a small `quorum_state_file` helper, can live in `controller.rs` or `transport.rs`)

- On `KraftController::new`: open `KraftLog`; load latest checkpoint via the (retained) `snapshot::load_latest` → seed `MetadataImage` + `last_applied`; replay committed `KraftLog` batches after the checkpoint into the image; load the `quorum-state` file → seed the core's persistent epoch / voted-for / leader (construct the `QuorumState` passed to `QuorumStateMachine::new`).
- `PersistQuorumState`: write the `quorum-state` file (JSON or wincode — any deterministic format; it's node-local, not wire). Atomic temp+rename.

**Test:** build an engine, `submit_change` + `trigger_snapshot`, drop it, reopen over the same dir, assert the image is recovered (mirrors `snapshot.rs::snapshot_then_restart_recovers_image`). Commit `feat(raft): KraftController restart recovery + quorum-state persistence`.

---

## Task 6: In-memory PeerSender + multi-node async driver test (isolation acceptance)

**Files:** `crates/raft/src/kraft/transport.rs` (in-memory `PeerSender`); create `crates/raft/tests/kraft_engine_sim.rs`

Build an in-memory `PeerSender` that routes RPC bodies between in-process `KraftController`s (by node id), and an async harness running 3 engines (each a real `KraftController` over a tempdir `KraftLog`) with staggered election timeouts. Assert:
- exactly one leader is elected and all voters agree on the epoch;
- `submit_change` on a follower propagates to all voters' images (forward + commit + apply);
- killing the leader → the majority re-elects a single new leader; the old leader rejoins as follower after heal;
- restart recovery: drop+reopen a node, its image is rebuilt.

This is the isolation acceptance — it exercises the real engine/loop/log without TCP. Commit `test(raft): multi-node KraftController async driver simulation`.

---

## Task 7: Real wire — server dispatch + network PeerSender

**Files:** `crates/raft/src/server.rs`, `crates/raft/src/network.rs`

- `server.rs`: rewrite `dispatch` to route inbound `1` (Fetch), `52` (Vote), `53` (BeginQuorumEpoch), `54` (EndQuorumEpoch) to the engine (decode via Slice-2 generated request types → `Inbound` → reply with the encoded response). Serve a real `18` ApiVersions advertising {1,18,52,53,54,55,1003,1004}. Keep `1003` SubmitChange-forward and `1004` MetadataFetch. **Delete** the `1000/1001/1002` arms, the `kraft_spike` `#[cfg(feature)]` block, and the openraft `convert_*` helpers.
- `network.rs`: replace the openraft `RaftNetworkFactory`/`RaftNetwork` impls with the real `PeerSender` impl over `OutboundDialer` (encode + `raw_request(api_key, version, body)` for Vote/Begin/End/Fetch; map the response).

**Test:** a 2-engine test that wires two `KraftController`s through the REAL `server.rs` listener + `network.rs` `PeerSender` over loopback TCP, asserting they elect a leader (proves the wire path). Commit `feat(raft): real KIP-595 wire dispatch + network PeerSender; drop openraft RPC keys + kraft-spike`.

---

## Task 8: Wire `KraftController` into `ControllerHandle`

**Files:** `crates/raft/src/controller.rs`

Replace the `Arc<Raft>` field with the `KraftController`; `start_with_listener` builds the engine (log, image, core, listener, `PeerSender` from the dialer) and spawns it. Delegate every public method to the engine (Task 4 ops). The reconfig methods (`add_voter`/`remove_voter`/`update_voter`/`change_membership`/`add_learner`) return `RaftError::Unsupported("dynamic reconfig: Slice 5")`; the `ReconfigOps` impl reads `current_voters`/`leader`/`is_leader`/`leader_last_index`/`observer_index` from `quorum_state()` (so the mock-based `reconfig.rs` tests are unaffected). Keep `forward_submit_to`, `read_snapshot_range`, `metadata_records`, `fetch_metadata_from`, `controller_bound_addr`, `shutdown`, `cancel`.

**Test:** `cargo build -p crabka-raft` compiles with `ControllerHandle` on the new engine (openraft still present but unused by the handle at this point — final deletion is Task 9). Commit `feat(raft): ControllerHandle drives KraftController (reconfig -> Unsupported)`.

---

## Task 9: Delete openraft

**Files:** delete `crates/raft/src/log_store.rs`, `crates/raft/src/kraft_spike.rs`, `crates/raft/src/kraft_spike_metadata_log.bin`; edit `crates/raft/src/{types.rs, state_machine.rs, error.rs, lib.rs, network.rs}`, `crates/raft/Cargo.toml`, `crates/broker/Cargo.toml`.

- `types.rs`: remove `declare_raft_types!` + `pub type Raft`; keep `AppData`/`AppDataResponse`/`Node`/`NodeId`.
- `state_machine.rs`: remove the `RaftStateMachine`/`RaftLogStorage`/`RaftSnapshotBuilder` trait impls; keep `MetadataImage` apply + `recover`/`load_latest` glue as plain functions the engine calls.
- `error.rs`: delete `RaftError::Openraft`; add `Unsupported(&'static str)`.
- `lib.rs`: drop `mod log_store;`, `#[cfg(feature="kraft-spike")] mod kraft_spike;`.
- `Cargo.toml` (raft): remove `openraft` dependency + `[features] kraft-spike`. `Cargo.toml` (broker): remove the `kraft-spike` feature forward.

**Test:** `grep -rn "openraft" crates/raft/src` returns nothing; `cargo build -p crabka-raft -p crabka-broker` compiles. Commit `feat(raft): remove openraft (log_store, network adapter, state-machine impls, types, dep)`.

---

## Task 10: Existing suite green on the new engine

**Files:** none (fix-forward only; if a fix is needed it lands in `crates/raft/src/kraft/`)

Run and make pass, fixing engine bugs as they surface (the slice's real work):
- `cargo test -p crabka-raft` (single_node, snapshot, reconfig, the kraft sims).
- `cargo test -p crabka-broker --test quorum` — the 3-node integration (election, topic propagation, leader failover, follower forwarding, concurrent-topic race) now over real KIP-595 TCP. **This is the headline acceptance.** Expect timing/wire bugs; debug against the Task-6 driver sim (deterministic) to localize logic vs wiring.
- Docker-gated `jvm_acceptance.rs` raft parts: confirm any that depended on the controller still pass or are appropriately gated.

Commit fixes as `fix(raft): <specific> surfaced by quorum integration` per issue.

---

## Task 11: Capstone — fmt, clippy, full regression, openraft-gone proof

- [ ] `cargo fmt --all && cargo fmt --all --check` → clean.
- [ ] `cargo clippy --workspace --tests` → clean (kraft engine hand-written, warning-free).
- [ ] `grep -rn "openraft" crates/ Cargo.lock` → only absent (no dep); `crates/raft/src` has zero references.
- [ ] `cargo test -p crabka-raft -p crabka-broker` → all green (Docker-gated tests via `--ignored` as usual).
- [ ] Commit any fmt fixes.

---

## Self-Review Notes

- **Spec coverage:** event loop → Tasks 2–3; PeerSender seam → Tasks 1,6,7; submit/quorum_state/watches/snapshot → Task 4; restart recovery + quorum-state file → Task 5; isolation driver test → Task 6; real wire (server+network, delete 1000-1002 + kraft-spike) → Task 7; ControllerHandle cutover + reconfig→Unsupported → Task 8; openraft deletion inventory → Task 9; existing-suite acceptance → Task 10; capstone/openraft-gone → Task 11. Records-stay-wincode, static-voters, observers-keep-1004, snapshot-trigger+restart are honored; FetchSnapshot/KIP-631/dynamic-voters correctly deferred.
- **Engine-internal bodies as contracts:** the event loop, timer reconciliation, and submit/commit-wait logic are novel async code specified by behavioral contract + the Task-6 driver test + the existing `quorum.rs` suite, rather than pre-written literally — the same approach used for the interdependent 3a core arms. Types, trait signatures (`PeerSender`, `Inbound`), the `ControllerHandle` mapping, the wire-key changes, and the deletion inventory ARE concrete.
- **Type consistency:** `KraftController`, `PeerSender`, `Inbound`, `quorum_state`, `submit_change`, `advance_hwm`, `append`/`append_at`/`read_committed` (3b), `on_event`/`QuorumState`/`Role` (3a) used consistently.
- **Green-tree sequencing:** Tasks 1–6 are additive (new `kraft::controller`/`transport` + tests; openraft still live). Tasks 7–9 are the cutover+deletion (the single unavoidable red window, scoped tight). Task 10 brings the existing suite green. Internal sequencing per the spec.
- **Risk:** Task 10 is where reality bites; the deterministic Task-6 driver sim is the debugging anchor when the TCP integration misbehaves.
