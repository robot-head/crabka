# KIP-595 Slice 4 — KIP-630 snapshots + FetchSnapshot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A controller follower that has fallen below the leader's pruned log-start fetches the leader's latest KIP-630 snapshot via the real `FetchSnapshot` (api key 59) RPC, installs it, and resumes normal replication; the leader auto-snapshots on a committed-records threshold and prunes the log below the snapshot.

**Architecture:** The snapshot-fetch activity lives entirely in the async engine (`controller.rs` + new `snapshot_fetch.rs`) as an orthogonal `Option<SnapshotFetchState>`; the pure 3a core (`QuorumStateMachine`) is untouched. The raft `Fetch` already rides the real `FetchResponse`, so the leader signals "go snapshot" via the existing `snapshot_id` tagged field; `FetchSnapshot` is a new RPC mapped through the already-generated `FetchSnapshotRequest`/`Response` types. `crabka_log::Log::reset_to(new_base)` already exists, so log install wraps it.

**Tech Stack:** Rust, tokio single-owner actor engine, `crabka_protocol` generated KIP-595/630 codecs, `crabka_log` segmented log, `crabka_metadata` `MetadataImage`.

**Spec:** `docs/superpowers/specs/2026-05-31-kip595-slice4-snapshots-fetchsnapshot-design.md`

---

## File structure & responsibilities

- `crates/raft/src/kraft/log.rs` — `KraftLog::prune_to` (advance log-start + trim segments) and `install_snapshot` (reset log to empty-at-offset). **Task 1.**
- `crates/raft/src/kraft/snapshot_fetch.rs` *(new)* — pure `SnapshotFetchState` reassembly state machine. **Task 2.**
- `crates/raft/src/kraft/transport.rs` — `api_key::FETCH_SNAPSHOT`; `PeerResponse::Fetch.snapshot_id`; `wire::PeerRequest/Response::FetchSnapshot` codec; `Inbound::FetchSnapshot`; `Command::FetchSnapshotResponse`. **Task 3** (hub; also adds `snapshot_id: None` at the existing `controller.rs` Fetch-response sites so the workspace compiles).
- `crates/raft/src/server.rs` + `crates/raft/src/network.rs` — dispatch + version for key 59. **Task 4.**
- `crates/raft/src/kraft/controller.rs` + `crates/broker/src/config.rs` (+ `broker.rs`) — `snapshot_interval_records` config threading. **Task 5.**
- `crates/raft/src/kraft/controller.rs` — engine: trigger+prune, leader `snapshot_id` emit, `FetchSnapshot` serve, follower receive+install, checkpoint helpers (`load_checkpoint_by_id`, `retain_latest_checkpoint`, `latest_snapshot_id`). **Task 6.**
- `crates/raft/tests/sim_harness/mod.rs` + `crates/raft/tests/kraft_engine_sim.rs` — multi-node catch-up acceptance test. **Task 7.**

## Execution batches (non-overlapping file sets per CLAUDE.md)

- **Batch A (parallel):** Task 1 (`log.rs`) ‖ Task 2 (`snapshot_fetch.rs`).
- **Batch B (serial):** Task 3 (`transport.rs` + tiny `controller.rs` call-site default) — the dependency hub.
- **Batch C (parallel):** Task 4 (`server.rs`+`network.rs`) ‖ Task 5 (`config.rs`+`broker.rs`).
- **Batch D (serial):** Task 6 (`controller.rs` engine logic).
- **Batch E (serial):** Task 7 (sim harness + acceptance test).

Each task is committed with identity overrides:
`git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit` and the trailer `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. Run `cargo fmt --all` before every commit; keep `cargo clippy --workspace --all-targets` clean.

---

## Task 1: `KraftLog::prune_to` + `install_snapshot`

**Files:**
- Modify: `crates/raft/src/kraft/log.rs`
- Test: same file (`#[cfg(test)] mod tests`)

Context: `KraftLog` wraps `crabka_log::Log` (field `log`) and tracks `hwm`. Existing methods: `log_start_offset()`, `log_end_offset()`, `hwm()`, `advance_hwm(n)` (monotonic, clamped to log end), `truncate_to(offset)`. `crabka_log::Log` has `set_log_start_offset(new_start)`, `trim_to_offset(target) -> Result<i64, LogError>`, and `reset_to(new_base) -> Result<(), LogError>` (drops all segments, fresh segment at `new_base`).

- [ ] **Step 1: Write failing tests**

Add to `mod tests` in `log.rs`:

```rust
#[test]
fn prune_to_advances_log_start_and_is_noop_when_behind() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = KraftLog::open(dir.path()).unwrap();
    // Append 5 single-record batches at offsets 0..5.
    for _ in 0..5 {
        let mut b = test_batch();
        log.append(&mut b).unwrap();
    }
    log.advance_hwm(log.log_end_offset());
    assert!(log.log_start_offset() == 0);
    log.prune_to(3).unwrap();
    assert!(log.log_start_offset() == 3);
    // Pruning to an offset <= current start is a no-op (no error).
    log.prune_to(2).unwrap();
    assert!(log.log_start_offset() == 3);
}

#[test]
fn install_snapshot_resets_log_to_empty_at_offset() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = KraftLog::open(dir.path()).unwrap();
    for _ in 0..4 {
        let mut b = test_batch();
        log.append(&mut b).unwrap();
    }
    log.install_snapshot(100).unwrap();
    assert!(log.log_start_offset() == 100);
    assert!(log.log_end_offset() == 100);
    assert!(log.hwm() == 100);
    // A subsequent append lands at offset 100.
    let mut b = test_batch();
    let base = log.append(&mut b).unwrap();
    assert!(base == 100);
}
```

If a `test_batch()` helper does not already exist in this `mod tests`, add it (mirror how the existing append tests build a batch — a single empty-value record batch is fine):

```rust
fn test_batch() -> crabka_protocol::records::RecordBatch {
    use crabka_protocol::records::{Record, RecordBatch};
    RecordBatch {
        last_offset_delta: 0,
        records: vec![Record { value: Some(bytes::Bytes::from_static(b"x")), ..Default::default() }],
        ..Default::default()
    }
}
```

- [ ] **Step 2: Run, verify failure**

Run: `cargo test -p crabka-raft --lib kraft::log::tests::prune_to_advances_log_start_and_is_noop_when_behind kraft::log::tests::install_snapshot_resets_log_to_empty_at_offset`
Expected: FAIL — `no method named prune_to` / `install_snapshot`.

- [ ] **Step 3: Implement**

Add to `impl KraftLog`:

```rust
/// Prune the committed prefix below `end_offset`: advance the log-start
/// pointer and trim now-dead segments. No-op when `end_offset` is at or
/// below the current log start. Used by the leader after writing a snapshot.
///
/// # Errors
/// Returns [`RaftError`] if the underlying log operations fail.
pub fn prune_to(&mut self, end_offset: i64) -> Result<(), RaftError> {
    if end_offset <= self.log.log_start_offset() {
        return Ok(());
    }
    self.log.set_log_start_offset(end_offset)?;
    self.log.trim_to_offset(end_offset)?;
    Ok(())
}

/// Replace the log with an empty log starting at `end_offset` (drops every
/// segment), and set the high watermark to `end_offset`. Used by a follower
/// installing a fetched snapshot whose `end_offset` is ahead of its log.
///
/// # Errors
/// Returns [`RaftError`] if the underlying reset fails.
pub fn install_snapshot(&mut self, end_offset: i64) -> Result<(), RaftError> {
    self.log.reset_to(end_offset)?;
    self.hwm = end_offset;
    Ok(())
}
```

(`crabka_log::LogError` already converts into `RaftError` via the existing `?`-used `From` impl; if `set_log_start_offset`/`reset_to` errors are not yet `?`-convertible here, map with `.map_err(RaftError::Storage)` matching how `truncate_to` does it in this file.)

- [ ] **Step 4: Run, verify pass**

Run: `cargo test -p crabka-raft --lib kraft::log::tests`
Expected: PASS (all log tests).

- [ ] **Step 5: Commit**

```bash
git add crates/raft/src/kraft/log.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(raft): KraftLog prune_to + install_snapshot for KIP-630

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: `SnapshotFetchState` reassembly state machine

**Files:**
- Create: `crates/raft/src/kraft/snapshot_fetch.rs`
- Modify: `crates/raft/src/kraft/mod.rs` (add `pub(crate) mod snapshot_fetch;`)
- Test: in the new file

Pure, IO-free: accumulates `FetchSnapshot` response chunks for one snapshot id and decides the next step. `SnapshotId` is `(end_offset: i64, epoch: i32)`.

- [ ] **Step 1: Write the new file with failing tests**

```rust
//! Pure reassembly state machine for a follower fetching a KIP-630 snapshot
//! over `FetchSnapshot` (api key 59). IO-free: the async engine
//! (`controller.rs`) owns the transport and applies the [`SnapshotFetchStep`]
//! this returns. Chunks must arrive in order (position == bytes received so
//! far); any mismatch or a changed snapshot id aborts the transfer so the
//! engine restarts cleanly against the current leader.

use bytes::{Bytes, BytesMut};

use crate::kraft::types::NodeId;

/// Snapshot identity: (end_offset exclusive, epoch). Matches KIP-630
/// `SnapshotId`.
pub type SnapshotId = (i64, i32);

/// In-flight reassembly of one snapshot from one leader.
#[derive(Debug)]
pub struct SnapshotFetchState {
    pub snapshot_id: SnapshotId,
    pub leader_id: NodeId,
    buf: BytesMut,
    size: Option<i64>,
}

/// What the engine should do after feeding a chunk in.
#[derive(Debug, PartialEq, Eq)]
pub enum SnapshotFetchStep {
    /// Request the next byte range starting at `next_position`.
    Continue { next_position: i64 },
    /// All bytes received; `0` holds the assembled snapshot.
    Complete(Bytes),
    /// Abort: id mismatch / out-of-order / leader change. Engine discards
    /// this state and falls back to a normal Fetch.
    Restart,
}

impl SnapshotFetchState {
    #[must_use]
    pub fn new(snapshot_id: SnapshotId, leader_id: NodeId) -> Self {
        Self { snapshot_id, leader_id, buf: BytesMut::new(), size: None }
    }

    /// The byte position to request next (bytes received so far).
    #[must_use]
    pub fn next_position(&self) -> i64 {
        i64::try_from(self.buf.len()).unwrap_or(i64::MAX)
    }

    /// Feed one response chunk. `id`/`size`/`position` come from the
    /// `FetchSnapshot` response; `chunk` is `unaligned_records` bytes.
    pub fn on_chunk(&mut self, id: SnapshotId, size: i64, position: i64, chunk: &[u8]) -> SnapshotFetchStep {
        if id != self.snapshot_id || position != self.next_position() || size < 0 {
            return SnapshotFetchStep::Restart;
        }
        match self.size {
            Some(s) if s != size => return SnapshotFetchStep::Restart,
            _ => self.size = Some(size),
        }
        self.buf.extend_from_slice(chunk);
        if self.next_position() >= size {
            SnapshotFetchStep::Complete(self.buf.split().freeze())
        } else {
            SnapshotFetchStep::Continue { next_position: self.next_position() }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn assembles_in_order_chunks_to_complete() {
        let mut s = SnapshotFetchState::new((10, 1), 2);
        assert!(s.next_position() == 0);
        let step = s.on_chunk((10, 1), 6, 0, b"abc");
        assert!(step == SnapshotFetchStep::Continue { next_position: 3 });
        let step = s.on_chunk((10, 1), 6, 3, b"def");
        match step {
            SnapshotFetchStep::Complete(b) => assert!(b.as_ref() == b"abcdef"),
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    #[test]
    fn out_of_order_position_restarts() {
        let mut s = SnapshotFetchState::new((10, 1), 2);
        let _ = s.on_chunk((10, 1), 6, 0, b"abc");
        assert!(s.on_chunk((10, 1), 6, 99, b"def") == SnapshotFetchStep::Restart);
    }

    #[test]
    fn mismatched_id_restarts() {
        let mut s = SnapshotFetchState::new((10, 1), 2);
        assert!(s.on_chunk((11, 1), 6, 0, b"abc") == SnapshotFetchStep::Restart);
    }

    #[test]
    fn single_chunk_completes() {
        let mut s = SnapshotFetchState::new((5, 0), 1);
        match s.on_chunk((5, 0), 3, 0, b"xyz") {
            SnapshotFetchStep::Complete(b) => assert!(b.as_ref() == b"xyz"),
            other => panic!("expected Complete, got {other:?}"),
        }
    }
}
```

- [ ] **Step 2: Register the module**

In `crates/raft/src/kraft/mod.rs`, add alongside the other `mod` lines:

```rust
pub(crate) mod snapshot_fetch;
```

- [ ] **Step 3: Run, verify pass**

Run: `cargo test -p crabka-raft --lib kraft::snapshot_fetch`
Expected: PASS (4 tests). (`NodeId` import resolves from `crate::kraft::types`; confirm the path matches how `transport.rs`/`controller.rs` import `NodeId` and adjust if it re-exports elsewhere.)

- [ ] **Step 4: Commit**

```bash
git add crates/raft/src/kraft/snapshot_fetch.rs crates/raft/src/kraft/mod.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(raft): pure SnapshotFetchState reassembly state machine

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: transport — FetchSnapshot wire, `snapshot_id` on Fetch, Inbound/Command variants

**Files:**
- Modify: `crates/raft/src/kraft/transport.rs`
- Modify: `crates/raft/src/kraft/controller.rs` (add `snapshot_id: None`/handling at existing `PeerResponse::Fetch` sites so the crate compiles — full leader logic is Task 6)
- Test: `transport.rs` `mod tests`

This is the dependency hub; everything downstream uses these types.

- [ ] **Step 1: Add `api_key::FETCH_SNAPSHOT`**

In the `pub mod api_key` block:

```rust
pub const FETCH_SNAPSHOT: i16 = 59;
```

- [ ] **Step 2: Add the `snapshot_id` field to `PeerResponse::Fetch`**

In `enum PeerResponse`, the `Fetch` variant gains a field:

```rust
Fetch {
    leader_id: NodeId,
    leader_epoch: LeaderEpoch,
    diverging: Option<LogOffsetMetadata>,
    /// When set, the follower's fetch offset is below the leader's pruned
    /// log-start; it must `FetchSnapshot` this snapshot instead. `(end_offset, epoch)`.
    snapshot_id: Option<(i64, i32)>,
    hwm: i64,
    records: Bytes,
},
```

- [ ] **Step 3: Encode/decode `snapshot_id` on the real FetchResponse**

In `PeerResponse::encode`, the `Fetch` arm — after the `diverging` block, before building `resp` — set the real tagged field when present:

```rust
if let Some((end_offset, epoch)) = snapshot_id {
    partition.snapshot_id = fetch_resp::SnapshotId {
        end_offset: *end_offset,
        epoch: *epoch,
        ..Default::default()
    };
}
```

In `PeerResponse::decode_fetch`, after computing `diverging`, decode the snapshot id (the generated default is `(-1, -1)`; a real one has `end_offset >= 0`):

```rust
let snapshot_id = if p.snapshot_id.end_offset >= 0 {
    Some((p.snapshot_id.end_offset, p.snapshot_id.epoch))
} else {
    None
};
```

and add `snapshot_id,` to the returned `PeerResponse::Fetch { ... }`.

- [ ] **Step 4: Add `PeerRequest::FetchSnapshot` + `PeerResponse::FetchSnapshot` + codecs**

Add imports at the top of `mod wire`:

```rust
use crabka_protocol::owned::fetch_snapshot_request::{self as fs_req, FetchSnapshotRequest};
use crabka_protocol::owned::fetch_snapshot_response::{self as fs_resp, FetchSnapshotResponse};
```

Add a version constant near the others:

```rust
const FETCH_SNAPSHOT_VERSION: i16 = 1;
```

Add request/response variants:

```rust
// in enum PeerRequest
FetchSnapshot {
    from: NodeId,
    snapshot_id: (i64, i32),
    position: i64,
    max_bytes: i32,
},
```
```rust
// in enum PeerResponse
FetchSnapshot {
    snapshot_id: (i64, i32),
    size: i64,
    position: i64,
    bytes: Bytes,
    error_code: i16,
},
```

In `PeerRequest::encode`, add the arm:

```rust
PeerRequest::FetchSnapshot { from, snapshot_id, position, max_bytes } => {
    let (end_offset, epoch) = snapshot_id;
    let req = FetchSnapshotRequest {
        replica_id: node_to_wire(from),
        max_bytes,
        topics: vec![fs_req::TopicSnapshot {
            name: METADATA_TOPIC.to_string(),
            partitions: vec![fs_req::PartitionSnapshot {
                partition: METADATA_PARTITION,
                current_leader_epoch: epoch,
                snapshot_id: fs_req::SnapshotId { end_offset, epoch, ..Default::default() },
                position,
                ..Default::default()
            }],
            ..Default::default()
        }],
        cluster_id: None,
        ..Default::default()
    };
    encode_body(&req, FETCH_SNAPSHOT_VERSION)
}
```

Note `*from`/`*position`/`*max_bytes`/`*snapshot_id` deref as needed to match the existing `match *self` / `match self` style in `encode` (the request `encode` uses `match *self` with `Copy` fields; `(i64,i32)` is `Copy`, so this fits the `match *self` arm).

Add decode functions in `mod wire`:

```rust
/// Decode a FetchSnapshot request body (api 59).
#[must_use]
pub fn decode_fetch_snapshot(buf: &[u8]) -> Option<PeerRequest> {
    let mut cur = buf;
    let req = FetchSnapshotRequest::decode(&mut cur, FETCH_SNAPSHOT_VERSION).ok()?;
    let p = req.topics.first()?.partitions.first()?;
    Some(PeerRequest::FetchSnapshot {
        from: node_from_wire(req.replica_id),
        snapshot_id: (p.snapshot_id.end_offset, p.snapshot_id.epoch),
        position: p.position,
        max_bytes: req.max_bytes,
    })
}
```

In `PeerResponse::encode`, add the arm:

```rust
PeerResponse::FetchSnapshot { snapshot_id, size, position, bytes, error_code } => {
    let (end_offset, epoch) = *snapshot_id;
    let resp = FetchSnapshotResponse {
        topics: vec![fs_resp::TopicSnapshot {
            name: METADATA_TOPIC.to_string(),
            partitions: vec![fs_resp::PartitionSnapshot {
                index: METADATA_PARTITION,
                error_code: *error_code,
                snapshot_id: fs_resp::SnapshotId { end_offset, epoch, ..Default::default() },
                size: *size,
                position: *position,
                unaligned_records: crabka_protocol::records::RecordsPayload::Raw(bytes.clone()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    encode_body(&resp, FETCH_SNAPSHOT_VERSION)
}
```

Add a response decoder:

```rust
/// Decode a FetchSnapshot response body (api 59).
#[must_use]
pub fn decode_fetch_snapshot(buf: &[u8]) -> Option<Self> {
    let mut cur = buf;
    let resp = FetchSnapshotResponse::decode(&mut cur, FETCH_SNAPSHOT_VERSION).ok()?;
    let p = resp.topics.first()?.partitions.first()?;
    let bytes = match &p.unaligned_records {
        RecordsPayload::Raw(b) => b.clone(),
        other => { let mut o = BytesMut::new(); let _ = other.encode_to(&mut o); o.freeze() }
    };
    Some(PeerResponse::FetchSnapshot {
        snapshot_id: (p.snapshot_id.end_offset, p.snapshot_id.epoch),
        size: p.size,
        position: p.position,
        bytes,
        error_code: p.error_code,
    })
}
```

(There are two methods named `decode_fetch_snapshot` — one free fn for `PeerRequest`, one `impl PeerResponse` method. That mirrors the existing `decode_fetch` free fn vs `PeerResponse::decode_fetch` method split; keep the same shape.)

- [ ] **Step 5: Add `Inbound::FetchSnapshot` + `Command::FetchSnapshotResponse`**

In `enum Inbound`:

```rust
FetchSnapshot {
    req: Bytes,
    reply: oneshot::Sender<Bytes>,
},
```

In `enum Command`:

```rust
/// A FetchSnapshot RESPONSE the follower received from the leader (carries
/// snapshot bytes the follower reassembles before resuming). Mirrors
/// `FetchResponse`'s dedicated command path.
FetchSnapshotResponse {
    from: NodeId,
    body: Bytes,
},
```

- [ ] **Step 6: Keep `controller.rs` compiling**

`controller.rs` constructs `PeerResponse::Fetch { ... }` in the `Inbound::Fetch` arm and matches it in `on_fetch_response`. Add `snapshot_id: None,` to the construction site(s) and add `snapshot_id,` to the destructuring `let Some(wire::PeerResponse::Fetch { .. })` (binding it; Task 6 will use it — for now `let _ = snapshot_id;` to avoid unused warnings). Also add a `Command::FetchSnapshotResponse { .. } => {}` no-op arm and an `Inbound::FetchSnapshot { reply, .. } => { let _ = reply; }` no-op arm to the `on_command`/`on_inbound` matches so they are exhaustive (Task 6 fills them in).

- [ ] **Step 7: Write failing wire round-trip tests**

In `transport.rs` `mod tests`:

```rust
#[test]
fn fetch_response_carries_snapshot_id() {
    let resp = PeerResponse::Fetch {
        leader_id: 1, leader_epoch: 4, diverging: None,
        snapshot_id: Some((42, 3)), hwm: 0, records: Bytes::new(),
    };
    assert!(PeerResponse::decode_fetch(&resp.encode()) == Some(resp));
}

#[test]
fn fetch_snapshot_request_round_trips() {
    let req = PeerRequest::FetchSnapshot { from: 2, snapshot_id: (42, 3), position: 128, max_bytes: 4096 };
    assert!(decode_fetch_snapshot(&req.encode()) == Some(req));
}

#[test]
fn fetch_snapshot_response_round_trips() {
    let resp = PeerResponse::FetchSnapshot {
        snapshot_id: (42, 3), size: 9, position: 0,
        bytes: Bytes::from_static(b"snapshotX"), error_code: 0,
    };
    assert!(PeerResponse::decode_fetch_snapshot(&resp.encode()) == Some(resp));
}
```

- [ ] **Step 8: Run, verify pass**

Run: `cargo test -p crabka-raft --lib kraft::transport` then `cargo build -p crabka-raft`
Expected: the three new tests pass; the crate compiles (Task-6 no-op arms in place).

- [ ] **Step 9: Commit**

```bash
git add crates/raft/src/kraft/transport.rs crates/raft/src/kraft/controller.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(raft): FetchSnapshot(59) wire codec + snapshot_id on Fetch response

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: server dispatch + network version for key 59

**Files:**
- Modify: `crates/raft/src/server.rs` (the `dispatch` fn)
- Modify: `crates/raft/src/network.rs` (the api_key→version map)
- Test: covered end-to-end by Task 7 (no isolated test — dispatch is a thin router; add a focused unit only if a `dispatch` test already exists to mirror).

- [ ] **Step 1: Add the dispatch arm**

In `crates/raft/src/server.rs` `dispatch`, alongside `api_key::FETCH`/`VOTE`/...:

```rust
api_key::FETCH_SNAPSHOT => {
    deliver_inbound(engine, |reply| Inbound::FetchSnapshot { req: body, reply }).await
}
```

(Match the exact `deliver_inbound` closure shape used by the `FETCH`/`VOTE` arms in this file.)

- [ ] **Step 2: Add the version map entry**

In `crates/raft/src/network.rs`, where api keys map to wire versions (`api_key::VOTE => 2`, `api_key::FETCH => 17`, ...):

```rust
api_key::FETCH_SNAPSHOT => 1,
```

- [ ] **Step 3: Build**

Run: `cargo build -p crabka-raft`
Expected: compiles.

- [ ] **Step 4: Commit**

```bash
git add crates/raft/src/server.rs crates/raft/src/network.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(raft): dispatch FetchSnapshot(59) over the controller listener

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: `snapshot_interval_records` config threading

**Files:**
- Modify: `crates/raft/src/kraft/controller.rs` (`KraftConfig` struct + use in `spawn_with_image`)
- Modify: `crates/broker/src/config.rs` (`BrokerConfig` field + default)
- Modify: `crates/broker/src/broker.rs` (pass it where `KraftController::open`/config is built)
- Test: `config.rs` default test (mirror an existing default assertion)

- [ ] **Step 1: Add the field to `KraftConfig`**

In `controller.rs`:

```rust
pub struct KraftConfig {
    pub me: NodeId,
    pub cluster_id: Uuid,
    pub initial_state: QuorumState,
    pub election_timeout_ms: u64,
    pub peers: Arc<dyn PeerSender>,
    /// Snapshot once committed offset advances this many records past the
    /// last snapshot, then prune the log below it. `0` disables snapshotting.
    pub snapshot_interval_records: u64,
}
```

Thread it into the `Engine` (Task 6 adds the engine fields; for now `spawn_with_image` destructures it and passes it into the `Engine { .. }` literal — coordinate with Task 6, which owns the `Engine` field additions). To keep Task 5 self-contained and compiling **before** Task 6, store it in a local and `let _ = snapshot_interval_records;` if the `Engine` field isn't added yet — but since Batch C runs before Batch D, prefer: Task 5 adds the `KraftConfig` field + all call sites that build a `KraftConfig`, and Task 6 consumes it. Update every `KraftConfig { .. }` literal in the codebase (search `KraftConfig {`) to set `snapshot_interval_records` (tests can use a large default like `u64::MAX`/`10_000`).

- [ ] **Step 2: Add `BrokerConfig` field + default**

In `crates/broker/src/config.rs`, add to `BrokerConfig`:

```rust
/// KIP-630: snapshot the metadata log once committed offset advances this
/// many records past the last snapshot, then prune below it.
pub metadata_snapshot_interval_records: u64,
```

Set the default (in the same `Default`/constructor block as the other metadata fields) to `10_000`. Add to any test-config builders in this file that construct `BrokerConfig` literally.

- [ ] **Step 3: Pass through in `broker.rs`**

Where `broker.rs` builds the controller config / calls `KraftController::open` (search `KraftConfig`/`KraftController::open` in `broker.rs`), pass `config.metadata_snapshot_interval_records`. If `open()`'s signature needs the value, add a parameter `snapshot_interval_records: u64` to `KraftController::open` (Task 6 owns `open`; Task 5 adds the param + threads the config value, Task 6 uses it).

- [ ] **Step 4: Default test**

In `config.rs` tests, extend (or add) the defaults assertion:

```rust
#[test]
fn default_metadata_snapshot_interval() {
    let cfg = BrokerConfig::for_tests(/* match existing helper args */);
    assert!(cfg.metadata_snapshot_interval_records == 10_000);
}
```

(Use whatever default-config constructor the existing config tests use; if a `default()`-style test already exists, add the assertion there instead of a new test.)

- [ ] **Step 5: Build + test**

Run: `cargo build -p crabka-raft -p crabka-broker && cargo test -p crabka-broker --lib config`
Expected: compiles; config default test passes.

- [ ] **Step 6: Commit**

```bash
git add crates/raft/src/kraft/controller.rs crates/broker/src/config.rs crates/broker/src/broker.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat: thread snapshot_interval_records config to the KRaft engine

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: engine — trigger+prune, leader serve, follower install

**Files:**
- Modify: `crates/raft/src/kraft/controller.rs`
- Test: `controller.rs` `mod tests` (engine-level, using the in-process test entrypoints)

This is the integration task. It consumes Tasks 1–5.

- [ ] **Step 1: Add `Engine` fields**

Add to `struct Engine`:

```rust
/// Snapshot once committed offset advances this many records past the last
/// snapshot, then prune. `0` disables.
snapshot_interval_records: u64,
/// `end_offset` of the most recent snapshot we have written/installed.
last_snapshot_end_offset: i64,
/// In-flight snapshot transfer (follower side), if any.
snapshot_fetch: Option<crate::kraft::snapshot_fetch::SnapshotFetchState>,
```

Initialize in `spawn_with_image` from `config.snapshot_interval_records`, `last_snapshot_end_offset: 0`, `snapshot_fetch: None`. Thread `snapshot_interval_records` through `KraftConfig` destructuring. If `open()` recovered from a checkpoint, set `last_snapshot_end_offset` to that checkpoint's end_offset (parse from `load_latest_checkpoint`’s chosen `(end_offset, epoch)` — extend `load_latest_checkpoint` to also return the id, or add a sibling `latest_checkpoint_id(dir) -> Option<(i64,i32)>` and call it in `open`).

- [ ] **Step 2: Snapshot-trigger + prune helper + test**

Failing test (engine commits past the threshold → a checkpoint file appears and log-start advances):

```rust
#[tokio::test]
async fn leader_snapshots_and_prunes_at_threshold() {
    // Single-voter leader so commits apply immediately.
    let dir = tempfile::tempdir().unwrap();
    let ctrl = single_voter_engine_with_interval(dir.path(), /*interval*/ 3).await;
    wait_until_leader(&ctrl).await;
    // Commit > 3 records (each NoOp-ish metadata record).
    for i in 0..4 {
        ctrl.test_append_and_commit(vec![feature_level_record(i)]).await.unwrap();
    }
    // A checkpoint exists and the log-start advanced past 0.
    let cp_dir = checkpoint_dir(dir.path());
    assert!(load_latest_checkpoint(&cp_dir).unwrap().is_some());
    // Observe via quorum snapshot / a test accessor that log_start_offset > 0.
    assert!(ctrl.quorum_snapshot().log_start_offset_for_test() > 0);
}
```

(Use the existing single-voter test harness in this module — mirror `test_append_and_commit` usage. If `QuorumStateSnapshot` lacks `log_start_offset`, either add it or add a `#[cfg(test)]` engine accessor. `feature_level_record(i)` = a `MetadataRecord::V1FeatureLevel` with a distinct name so each is a fresh committed record.)

Implementation — add to `impl Engine`, called at the end of `advance_and_apply` (only when `changed` and leader):

```rust
/// After committing, snapshot + prune if the committed offset has advanced
/// `snapshot_interval_records` past the last snapshot. Leader-only.
fn maybe_snapshot_and_prune(&mut self) {
    if self.snapshot_interval_records == 0 || !self.core.role().is_leader() {
        return;
    }
    let hwm = self.log.hwm();
    let advanced = u64::try_from((hwm - self.last_snapshot_end_offset).max(0)).unwrap_or(0);
    if advanced < self.snapshot_interval_records {
        return;
    }
    let bytes = match crate::snapshot::SnapshotWriter::serialize(&self.image, 0) {
        Ok(b) => b,
        Err(e) => { tracing::error!(?e, "kraft: snapshot serialize failed"); return; }
    };
    let epoch = i32::try_from(self.core.quorum_state().leader_epoch).unwrap_or(i32::MAX);
    if let Err(e) = write_checkpoint(&checkpoint_dir(&self.data_dir), hwm, epoch, &bytes) {
        tracing::error!(?e, "kraft: checkpoint write failed; skipping prune");
        return;
    }
    self.last_snapshot_end_offset = hwm;
    if let Err(e) = self.log.prune_to(hwm) {
        tracing::error!(?e, "kraft: prune_to failed");
    }
    retain_latest_checkpoint(&checkpoint_dir(&self.data_dir));
}
```

Call `self.maybe_snapshot_and_prune();` at the end of `advance_and_apply` (after the `try_resolve_waiters()` / image publish). Add the free fn:

```rust
/// Delete every `.checkpoint` in `dir` except the one with the highest
/// `(end_offset, epoch)`. Best-effort (logs on error).
fn retain_latest_checkpoint(dir: &std::path::Path) {
    let Some(latest) = latest_checkpoint_id(dir) else { return };
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(stem) = name.strip_suffix(".checkpoint") else { continue };
        let Some((off, ep)) = stem.split_once('-') else { continue };
        let (Ok(off), Ok(ep)) = (off.parse::<i64>(), ep.parse::<i32>()) else { continue };
        if (off, ep) != latest {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}
```

and `latest_checkpoint_id(dir) -> Option<(i64,i32)>` (factor it out of `load_latest_checkpoint`, which already scans for the best `(off, ep)` — have `load_latest_checkpoint` call `latest_checkpoint_id` then read that file).

- [ ] **Step 3: Run, verify Step-2 test passes**

Run: `cargo test -p crabka-raft --lib kraft::controller::tests::leader_snapshots_and_prunes_at_threshold`
Expected: PASS.

- [ ] **Step 4: Leader emits `snapshot_id` when fetch offset < log_start**

In the `Inbound::Fetch` arm (`on_inbound`), where it currently computes `records` and builds `PeerResponse::Fetch`: before serving records, check the pruned case:

```rust
let log_start = self.log.log_start_offset();
let snapshot_id = if fetch_offset >= 0 && fetch_offset < log_start {
    self.latest_snapshot_id() // Option<(i64,i32)>
} else {
    None
};
let records = if snapshot_id.is_some() || diverging.is_some() || !self.core.role().is_leader() {
    bytes::Bytes::new()
} else {
    self.serve_fetch_records(fetch_offset)
};
let resp = wire::PeerResponse::Fetch {
    leader_id: self.me,
    leader_epoch: self.core.quorum_state().leader_epoch,
    diverging,
    snapshot_id,
    hwm: self.log.hwm(),
    records,
};
```

Add `latest_snapshot_id(&self) -> Option<(i64,i32)>` (calls `latest_checkpoint_id(&checkpoint_dir(&self.data_dir))`).

- [ ] **Step 5: Leader serves `FetchSnapshot`**

Fill the `Inbound::FetchSnapshot` arm in `on_inbound`:

```rust
Inbound::FetchSnapshot { req, reply } => {
    if let Some(wire::PeerRequest::FetchSnapshot { snapshot_id, position, max_bytes, .. }) =
        wire::decode_fetch_snapshot(&req)
    {
        let (end_offset, epoch) = snapshot_id;
        let resp = match load_checkpoint_by_id(&checkpoint_dir(&self.data_dir), end_offset, epoch) {
            Some(bytes) => {
                let max = usize::try_from(max_bytes.max(0)).unwrap_or(0);
                let pos = usize::try_from(position.max(0)).unwrap_or(0);
                let chunk = crate::snapshot::SnapshotReader::byte_range(&bytes, pos, max);
                wire::PeerResponse::FetchSnapshot {
                    snapshot_id,
                    size: i64::try_from(bytes.len()).unwrap_or(i64::MAX),
                    position,
                    bytes: bytes::Bytes::copy_from_slice(chunk),
                    error_code: 0,
                }
            }
            None => wire::PeerResponse::FetchSnapshot {
                snapshot_id, size: 0, position,
                bytes: bytes::Bytes::new(),
                error_code: SNAPSHOT_NOT_FOUND,
            },
        };
        let _ = reply.send(resp.encode());
    }
}
```

Add `load_checkpoint_by_id(dir, end_offset, epoch) -> Option<Vec<u8>>` (build the `{:020}-{:010}.checkpoint` name, `std::fs::read`, `.ok()`). Add `const SNAPSHOT_NOT_FOUND: i16 = ...;` — use the Kafka error code for `SNAPSHOT_NOT_FOUND` (look it up in `crabka_protocol`/broker `codes`; it is a defined Kafka error). If `crabka-raft` has no `codes` dependency, define the literal with a comment citing the Kafka code.

- [ ] **Step 6: Follower drives `FetchSnapshot` on `snapshot_id`; reassemble; install**

(a) In `on_fetch_response`, when the decoded `PeerResponse::Fetch` carries `snapshot_id = Some(id)` and `self.snapshot_fetch` is `None` (or for a different id), start the transfer:

```rust
if let Some(id) = snapshot_id {
    // Only fetch a snapshot strictly ahead of our log end.
    if id.0 > self.log.log_end_offset() {
        self.snapshot_fetch = Some(SnapshotFetchState::new(id, leader_id));
        self.send_fetch_snapshot(leader_id, id, 0);
    }
    // Do NOT feed normal append/apply; still feed the core a ReceiveFetchResponse
    // so liveness/epoch bookkeeping proceeds.
    self.on_event(Event::ReceiveFetchResponse { leader_id, leader_epoch, diverging });
    return;
}
```

(b) Add `send_fetch_snapshot`:

```rust
fn send_fetch_snapshot(&self, leader_id: NodeId, snapshot_id: (i64, i32), position: i64) {
    if leader_id == self.me { return; }
    let body = wire::PeerRequest::FetchSnapshot {
        from: self.me, snapshot_id, position,
        max_bytes: i32::try_from(MAX_APPLY_BYTES).unwrap_or(i32::MAX),
    }.encode();
    self.spawn_send(leader_id, api_key::FETCH_SNAPSHOT, body);
}
```

In `spawn_send`'s response routing (where `api_key == FETCH` posts `Command::FetchResponse`), add: `else if api_key == self::api_key::FETCH_SNAPSHOT { post Command::FetchSnapshotResponse { from: peer, body: resp_body } }`.

(c) Handle `Command::FetchSnapshotResponse` in `on_command` → `self.on_fetch_snapshot_response(from, &body)`:

```rust
fn on_fetch_snapshot_response(&mut self, from: NodeId, body: &[u8]) {
    let Some(wire::PeerResponse::FetchSnapshot { snapshot_id, size, position, bytes, error_code }) =
        wire::PeerResponse::decode_fetch_snapshot(body) else { return };
    let Some(state) = self.snapshot_fetch.as_mut() else { return };
    if error_code != 0 || from != state.leader_id {
        // Leader can't serve it (deleted) or wrong peer: abort, fall back to Fetch.
        self.snapshot_fetch = None;
        self.send_fetch(from);
        return;
    }
    match state.on_chunk(snapshot_id, size, position, &bytes) {
        SnapshotFetchStep::Continue { next_position } => {
            self.send_fetch_snapshot(from, snapshot_id, next_position);
        }
        SnapshotFetchStep::Restart => {
            self.snapshot_fetch = None;
            self.send_fetch(from);
        }
        SnapshotFetchStep::Complete(assembled) => {
            let id = state.snapshot_id;
            self.snapshot_fetch = None;
            if let Err(e) = self.install_fetched_snapshot(id, &assembled) {
                tracing::error!(?e, "kraft: snapshot install failed; will re-fetch");
            }
            // Resume normal replication from the snapshot end.
            self.send_fetch(from);
        }
    }
}
```

(d) Add `install_fetched_snapshot` (validate-before-swap):

```rust
fn install_fetched_snapshot(&mut self, id: (i64, i32), bytes: &[u8]) -> Result<(), RaftError> {
    let (end_offset, epoch) = id;
    // Validate first: parse the records; only then swap state.
    let records = crate::snapshot::SnapshotReader::read_records(bytes)?;
    if end_offset <= self.log.log_end_offset() {
        return Ok(()); // stale; we already advanced past it
    }
    let cluster_id = self.image.cluster_id();
    let new_image = MetadataImage::from_records(cluster_id, &records);
    // Persist the checkpoint locally so a restart recovers from it.
    write_checkpoint(&checkpoint_dir(&self.data_dir), end_offset, epoch, bytes)?;
    self.image = new_image;
    self.log.install_snapshot(end_offset)?;
    self.last_snapshot_end_offset = end_offset;
    let _ = self.image_tx.send(Arc::new(self.image.clone()));
    retain_latest_checkpoint(&checkpoint_dir(&self.data_dir));
    Ok(())
}
```

Imports: `use crate::kraft::snapshot_fetch::{SnapshotFetchState, SnapshotFetchStep};`.

**Post-install fetch-epoch hazard (read before testing):** after `install_snapshot` resets the log to empty-at-`end_offset`, the follower's leader-epoch checkpoint is empty, so its next `Fetch` would carry `last_fetched_epoch = 0`. The leader's divergence check (in the `Inbound::Fetch` arm) compares the follower's `(fetch_offset, fetch_epoch)` against its own log and may emit a spurious `diverging` hint at the snapshot boundary — causing a truncate/re-fetch loop instead of clean resume. Mitigation: record the snapshot's epoch on install and make the follower's first post-snapshot `Fetch` carry it. Concretely, store `self.installed_snapshot_epoch: Option<LeaderEpoch>` (set to `epoch` in `install_fetched_snapshot`, cleared once a normal fetch succeeds) and, in `send_fetch`, use it for `fetch_epoch` when `self.log` is empty at the snapshot boundary (LEO == log_start == end_offset). The catch-up sim (Task 7) is the forcing function — if convergence loops, this is the cause.

- [ ] **Step 7: Build + run all raft unit tests**

Run: `cargo build -p crabka-raft && cargo test -p crabka-raft --lib`
Expected: PASS (existing + new). Fix any exhaustiveness/borrow issues.

- [ ] **Step 8: Commit**

```bash
git add crates/raft/src/kraft/controller.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(raft): snapshot trigger+prune, FetchSnapshot serve + follower install

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 7: multi-node catch-up acceptance test

**Files:**
- Modify: `crates/raft/tests/sim_harness/mod.rs` (route `FetchSnapshot` between in-process engines)
- Modify: `crates/raft/tests/kraft_engine_sim.rs` (the catch-up test)

The sim harness wires N in-process `KraftController`s with a `PeerSender` that routes `(peer, api_key, body)` to the target engine's `deliver`/inbound. It must now also route `api_key::FETCH_SNAPSHOT` (the engine already posts `Command::FetchSnapshotResponse` for the reply).

- [ ] **Step 1: Route FetchSnapshot in the harness**

In `sim_harness/mod.rs`, the in-process `PeerSender::send` matches `api_key` → builds the right `Inbound` and awaits the oneshot. Add:

```rust
api_key::FETCH_SNAPSHOT => {
    let (tx, rx) = oneshot::channel();
    target.deliver(Inbound::FetchSnapshot { req: body, reply: tx }).await?;
    rx.await.map_err(|_| RaftError::Shutdown)
}
```

(Mirror the exact arm used for `api_key::FETCH`.)

- [ ] **Step 2: Write the failing catch-up test**

In `kraft_engine_sim.rs`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_follower_catches_up_via_snapshot() {
    // 3 voters, small snapshot interval so a burst of commits triggers a snapshot+prune.
    let mut sim = Sim::start_n(3, /*snapshot_interval_records*/ 5).await;
    let leader = sim.wait_for_leader().await;

    // Take one follower offline.
    let follower = sim.some_follower(leader);
    sim.partition_off(follower).await;

    // Commit > interval records on the leader → snapshot + prune below them.
    for i in 0..8 {
        sim.submit(leader, vec![feature_level_record(i)]).await.unwrap();
    }
    sim.assert_log_start_advanced(leader).await; // leader pruned

    // Bring the follower back with a FRESH/empty log (rejoin).
    sim.rejoin_fresh(follower).await;

    // It must catch up via FetchSnapshot: its image converges to the leader's.
    sim.wait_until_images_match(leader, follower).await;
    assert!(sim.image(follower).await == sim.image(leader).await);
}
```

(Adapt names to the existing `sim_harness` API — `Sim::start_n`, `wait_for_leader`, `submit`, `image`, `partition_off`/`rejoin` may have different existing names. Reuse the harness helpers from the existing `kraft_engine_sim` tests; add `start_n(n, snapshot_interval_records)` by extending the existing constructor with the interval, and `rejoin_fresh` = restart that node with a fresh tempdir/empty log so its LEO is 0 < leader.log_start. If the harness has no partition/rejoin primitive yet, the minimal version: start the follower's engine late — only construct/start node `follower` AFTER the leader has snapshotted+pruned, with the full voter set, so its first Fetch is at offset 0 < log_start and triggers the snapshot path.)

- [ ] **Step 3: Run, verify it fails then passes**

Run: `cargo test -p crabka-raft --test kraft_engine_sim lagging_follower_catches_up_via_snapshot -- --nocapture`
Expected: initially FAIL (no catch-up), PASS after Task 6 wiring is correct. Debug the transfer loop if the images don't converge (log the `SnapshotFetchStep` transitions).

- [ ] **Step 4: Full regression + lint**

Run:
```
cargo test -p crabka-raft
cargo test -p crabka-metadata -p crabka-protocol
cargo clippy --workspace --all-targets   # zero warnings
cargo fmt --all -- --check
```
Then the broker multi-node suites that must stay green (default interval 10_000 means no incidental snapshots):
```
cargo test -p crabka-broker --test quorum --test leader_election --test controlled_shutdown --test fetch_snapshot
```
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add crates/raft/tests/sim_harness/mod.rs crates/raft/tests/kraft_engine_sim.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(raft): multi-node snapshot catch-up acceptance (lagging follower)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Final verification (after all tasks)

- [ ] `cargo test --workspace` green (modulo known load-flaky TCP suites — re-run any failure in isolation to confirm it's a flake, per the elect_leaders/durability precedent).
- [ ] `cargo clippy --workspace --all-targets` clean; `cargo fmt --all -- --check` clean.
- [ ] The 3d-2 JVM dump-log byte check (`crates/raft/src/snapshot.rs::jvm_dump_log_parses_engine_snapshot`, `--ignored`) still passes — snapshot bytes unchanged.
- [ ] Push to PR #352; no PR title change needed (still "Slices 0-3d…") or extend to mention Slice 4.

## Notes / parking lot (out of scope — do NOT implement here)

- Broker-observer snapshotting over the 1004 path (observer below pruned log-start).
- Progress-gated retention (prune only below min voter matched index) and time/size snapshot policy.
- Live JVM-peer FetchSnapshot interop (Slice 6).
