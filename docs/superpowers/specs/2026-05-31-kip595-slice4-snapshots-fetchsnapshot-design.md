# KIP-595 Slice 4 — KIP-630 snapshots + FetchSnapshot (voter↔voter)

Date: 2026-05-31
Status: Approved (brainstorming) — pending spec review

## Context

Slices 0–3d built the real KRaft consensus engine and made the metadata
log/snapshots genuinely KIP-631/KIP-630 framed. After 3d-2:

- `SnapshotWriter`/`SnapshotReader` (`crates/raft/src/snapshot.rs`) produce and
  parse byte-exact KIP-630 `.checkpoint` artifacts — header/data/footer control
  batches with real `SnapshotHeaderRecord`/`SnapshotFooterRecord`, validated
  against JVM `kafka-dump-log`.
- The `FetchSnapshot` v0/v1 request/response wire types
  (`crates/protocol/.../fetch_snapshot_{request,response}`) are generated and
  round-trip-validated (Slice 2), but **unused**.
- The inter-controller raft `Fetch` already rides the **real**
  `FetchRequest`/`FetchResponse` (the `transport::wire` module maps the engine's
  flat `PeerRequest`/`PeerResponse` enums to/from real KIP-595 bodies).
  `FetchResponse.snapshot_id` (tagged field, tag 2) is on the wire but the
  engine neither sets nor reads it.
- `KraftController::open()` already loads the latest checkpoint + replays the
  committed log on top. `trigger_snapshot()` writes a checkpoint but **nothing
  prunes the log or serves snapshots over the wire**.
- `crabka_log::Log` exposes `set_log_start_offset` + `trim_to_offset` (segment
  pruning); `KraftLog` exposes `log_start_offset` but no prune/install wrapper.

**The gap:** a controller follower that has fallen behind the leader's pruned
log-start cannot catch up — there is no snapshot transfer. Slice 4 closes it.

## Goal & scope

A controller **follower** whose fetch offset is below the leader's pruned
log-start fetches the leader's latest snapshot via the real `FetchSnapshot`
(api key 59) RPC, installs it, and resumes normal replication. The leader
auto-snapshots on a committed-records threshold and prunes the log below the
snapshot.

**In scope:**
- Leader: committed-records-threshold snapshot trigger + log prune below the
  snapshot's `end_offset`.
- Leader: emit `FetchResponse.snapshot_id` when a fetch offset is below
  log-start; serve `FetchSnapshot` byte ranges from the checkpoint.
- Follower: detect `snapshot_id`, run the `FetchSnapshot` reassembly state
  machine, validate-then-install (image + log reset), resume.
- `KraftLog::prune_to` / `install_snapshot`; snapshot-file helpers
  (latest / by-id / retain-latest).
- Deterministic multi-node catch-up sim + unit tests.

**Out of scope (follow-ups / later slices):**
- **Broker-observer snapshotting** (the private `MetadataFetch` 1004 path): an
  observer that falls below the controller's pruned log-start. Voter↔voter only
  in Slice 4.
- **Live JVM-peer FetchSnapshot interop** (a JVM node snapshot-fetching from a
  Crabka leader or vice-versa): Slice 6 (mixed quorum). Slice 4 validates via
  Crabka sims + the existing dump-log byte check.
- **Progress-gated retention** (prune only below the min voter matched index)
  and time/size snapshot policy: noted follow-ups; Slice 4 prunes to the
  snapshot `end_offset` with a records threshold.

## Architecture

The "fetching snapshot" activity lives entirely in the **async engine**
(`controller.rs` + a new `snapshot_fetch.rs`) as an orthogonal
`Option<SnapshotFetchState>` on `Engine`. The pure 3a core
(`QuorumStateMachine`) is **untouched**: it still classifies the node as a
Follower issuing fetches; the engine intercepts a fetch response carrying
`snapshot_id` and, instead of normal fetch/apply, drives the snapshot transfer,
then resumes feeding the core normal `ReceiveFetchResponse` events. Snapshot
sends are fire-and-forget with responses re-injected as commands, exactly like
the existing Vote/Fetch sends — the loop never blocks.

### Offsets & naming (KIP-630)

A snapshot's `SnapshotId.end_offset` is **exclusive**: the offset of the first
record *not* contained (= the count of records included = the committed offset
at snapshot time). The on-disk artifact is `<end_offset>-<epoch>.checkpoint`
(zero-padded, lexical == numeric sort), which `write_checkpoint` /
`load_latest_checkpoint` already implement. After snapshotting `[0, end_offset)`
the log may delete records below `end_offset`; `log_start_offset` advances to
`end_offset`.

## Components

### `crates/raft/src/kraft/log.rs` (`KraftLog`)
- `prune_to(&mut self, end_offset: i64) -> Result<(), RaftError>`:
  `log.set_log_start_offset(end_offset)` then `log.trim_to_offset(end_offset)`.
  No-op if `end_offset <= log_start_offset()`.
- `install_snapshot(&mut self, end_offset: i64) -> Result<(), RaftError>`: reset
  the log to an empty log whose `log_start_offset == log_end_offset ==
  end_offset` and `hwm == end_offset`. Implemented via a `crabka_log::Log`
  reset helper (see below); the follower calls this when installing a fetched
  snapshot that is ahead of its current LEO.

If `crabka_log::Log` lacks a reset-to-empty-at-offset primitive, add
`reset_to(&mut self, offset: i64) -> Result<(), LogError>` (drop all segments,
start a fresh segment at `offset`, set log_start = LEO = offset, truncate the
leader-epoch checkpoint). Mirrors how `truncate_to`/`trim_to_offset` already
manipulate segments + the epoch checkpoint.

### `crates/raft/src/kraft/transport.rs`
- `api_key::FETCH_SNAPSHOT: i16 = 59`.
- `wire::PeerRequest::FetchSnapshot { snapshot_id: (i64, i32), position: i64,
  max_bytes: i32 }` and `wire::PeerResponse::FetchSnapshot { snapshot_id:
  (i64, i32), size: i64, position: i64, bytes: Bytes, error_code: i16 }`,
  encoding to / decoding from the real `FetchSnapshotRequest`/`Response` for the
  single `__cluster_metadata-0` partition (`replica_directory_id`,
  `current_leader`, `cluster_id` defaulted/derived; this is the same
  defaulted-framing posture as 3d-2, full fidelity Slice 6).
- `wire::PeerResponse::Fetch` gains `snapshot_id: Option<(i64, i32)>`, carried in
  the real `FetchResponse.snapshot_id` tagged field (absent ⇔ `None`).
- `Inbound::FetchSnapshot { req: Bytes, reply: oneshot::Sender<Bytes> }`.

### `crates/raft/src/server.rs` / `network.rs`
- `dispatch`: `api_key::FETCH_SNAPSHOT => deliver_inbound(engine, |reply|
  Inbound::FetchSnapshot { req: body, reply })`.
- `network.rs` version map: `FETCH_SNAPSHOT => 1` (FetchSnapshot v1 is current in
  4.0; v0 acceptable — pick the version the generated type encodes and that
  `kafka-dump-log`/JVM accept; validated by round-trip).

### `crates/raft/src/kraft/controller.rs`
- **Engine fields:** `snapshot_interval_records: u64`,
  `last_snapshot_end_offset: i64`, `snapshot_fetch: Option<SnapshotFetchState>`.
- **Trigger + prune** (after `advance_and_apply` mutates the image): if
  `self.core.role().is_leader()` and `hwm - last_snapshot_end_offset >=
  snapshot_interval_records`, call `do_snapshot_and_prune(hwm)`:
  serialize the image, `write_checkpoint(<hwm>-<epoch>)`, set
  `last_snapshot_end_offset = hwm`, `self.log.prune_to(hwm)`, then
  `retain_latest_checkpoint()`. On serialize/write error: log + **do not prune**.
- **Leader Fetch → snapshot_id** (in the `Inbound::Fetch` arm): if
  `fetch_offset < self.log.log_start_offset()`, respond
  `PeerResponse::Fetch { snapshot_id: Some(latest_snapshot_id()), records:
  empty, diverging: None, hwm, leader_id, leader_epoch }`. (Leader still feeds
  the core the `ReceiveFetch` event for liveness/epoch bookkeeping.)
- **Leader FetchSnapshot serve** (`Inbound::FetchSnapshot` arm): decode →
  resolve `<end_offset>-<epoch>.checkpoint`; if absent → `error_code =
  SNAPSHOT_NOT_FOUND`; else read `SnapshotReader::byte_range(&bytes, position,
  max_bytes)`, reply `PeerResponse::FetchSnapshot { snapshot_id, size:
  bytes.len(), position, bytes: chunk, error_code: 0 }`.
- **Follower receive snapshot_id** (in `on_fetch_response`): if the response
  carries `snapshot_id` and we have no matching in-flight fetch, initialize
  `snapshot_fetch = Some(SnapshotFetchState::new(snapshot_id, leader_id))` and
  kick the first `FetchSnapshot(position = 0)`. Subsequent `FetchSnapshot`
  responses (a new `Command::FetchSnapshotResponse { from, body }`, mirroring
  `FetchResponse`) drive the state machine in `snapshot_fetch.rs`.
- **Snapshot-file helpers:** `latest_snapshot_id()`, `checkpoint_path(id)`,
  `retain_latest_checkpoint()` (delete all but the newest `.checkpoint`).

### `crates/raft/src/kraft/snapshot_fetch.rs` (new)
`SnapshotFetchState { snapshot_id: (i64,i32), leader_id: NodeId, buf: BytesMut,
size: Option<i64> }`. Methods:
- `on_chunk(position, size, bytes) -> SnapshotFetchStep`: reject if `position !=
  buf.len()` (out-of-order) or `snapshot_id` mismatch → `Restart`; append;
  if `buf.len() == size` → `Complete(bytes)`, else `Continue { next_position:
  buf.len() }`.
- The engine maps the step: `Continue` → issue the next `FetchSnapshot`;
  `Complete` → `install_fetched_snapshot`; `Restart` → clear state, fall back to
  a plain `Fetch`.
- **Install (validate-before-swap):** parse the assembled bytes with
  `SnapshotReader::read_records` into a candidate image **first**; only if it
  parses, write the checkpoint file, swap `self.image`, `self.log
  .install_snapshot(end_offset)`, `self.last_snapshot_end_offset = end_offset`,
  publish the image, clear `snapshot_fetch`, and feed the core a
  `ReceiveFetchResponse` so it resumes fetching from `end_offset`.

### Config
- `KraftConfig.snapshot_interval_records: u64` (default e.g. `10_000` — far
  above what any existing test commits, so steady-state replication is
  undisturbed and only the Slice-4 catch-up test forces a snapshot via a small
  override). Threaded from `BrokerConfig` (a new
  `metadata_snapshot_interval_records`, default 10_000).

## Data flow (catch-up)

```
leader: committed offset passes N past last snapshot
  → SnapshotWriter::serialize(image) → write <HWM>-<epoch>.checkpoint
  → KraftLog::prune_to(HWM)            (log_start_offset = HWM)
  → retain_latest_checkpoint()

follower (rejoined; LEO < leader.log_start):
  Fetch(fetch_offset = own LEO)
  leader: fetch_offset < log_start → FetchResponse{ snapshot_id=(HWM,epoch),
                                                    records=∅ }
  follower: snapshot_fetch = SnapshotFetchState(snapshot_id)
    loop: FetchSnapshot(snapshot_id, position) → {size, chunk}
          → on_chunk → Continue(next_position) … until Complete
  follower: SnapshotReader::read_records(buf) → candidate image (validate)
          → write checkpoint; image := candidate;
            log.install_snapshot(HWM); last_snapshot_end_offset := HWM; publish
          → resume Fetch(HWM) → normal replication
```

## Error handling

- **Snapshot write fails:** log; **skip prune** (never prune without a durable
  snapshot). `last_snapshot_end_offset` unchanged → retried next threshold.
- **`FetchSnapshot` for a deleted/unknown id:** `error_code =
  SNAPSHOT_NOT_FOUND`; follower clears `snapshot_fetch` and falls back to a plain
  `Fetch` (the leader may advertise a newer snapshot).
- **Leader change / `snapshot_id` mismatch mid-transfer:** `on_chunk` returns
  `Restart`; follower discards the buffer. A subsequent `Fetch` to the new
  leader re-advertises its current `snapshot_id`.
- **Out-of-order / gapped chunk** (`position != buf.len()`): `Restart`.
- **Install is validate-before-swap:** the reassembled bytes MUST parse via
  `SnapshotReader::read_records` before the image/log are replaced — a corrupt
  or truncated transfer never destroys current state; the follower just retries.
- **Snapshot ahead of LEO only:** `install_snapshot` is applied only when the
  snapshot `end_offset > self.log.log_end_offset()`; a stale snapshot_id (we
  already advanced past it) is ignored.

## Pruning safety

Prune to the snapshot `end_offset`. With `snapshot_interval_records` ≫ the
fetch round-trip, a healthy follower (fetching every fetch-timer tick) never
falls below log-start in steady state, so the existing multi-node quorum/sim
suites — which commit only a handful of records — never cross the threshold and
are undisturbed. Only a follower that was **down/absent** during snapshotting
(or starts with an empty log) fetches below log-start and triggers a transfer.
Progress-gated retention (prune only below the minimum voter matched index) is a
noted follow-up.

## Acceptance / testing

- **Headline (the contract):** a deterministic multi-node `kraft_engine_sim`
  catch-up test — 3 voters elect a leader; take one follower offline; commit
  `> snapshot_interval_records` records on the leader (forcing snapshot +
  prune); bring the follower back with a fresh/empty log → it receives
  `snapshot_id`, runs `FetchSnapshot`, installs, and its `MetadataImage`
  byte-matches the leader's (and its log resumes at `end_offset`). Extends the
  shared `sim_harness` with a `FetchSnapshot` transport route.
- **Unit:** `KraftLog::prune_to` (log_start advances; reads below start are
  refused) and `install_snapshot` (log reset to empty-at-offset);
  `do_snapshot_and_prune` fires at the threshold (checkpoint file appears,
  log_start advances, older checkpoints retained-away);
  `FetchSnapshot` serve returns the right byte range + `size`, and
  `SNAPSHOT_NOT_FOUND` for a missing id; `SnapshotFetchState::on_chunk`
  transitions (Continue / Complete / Restart on mismatch/out-of-order).
- **Wire:** `PeerResponse::Fetch` with `snapshot_id` round-trips through the real
  `FetchResponse.snapshot_id` tagged field; `PeerRequest/Response::FetchSnapshot`
  round-trips through the real `FetchSnapshot` types.
- **Regression:** all existing raft + broker multi-node suites stay green
  (default interval prevents incidental snapshots). The 3d-2 JVM dump-log byte
  check continues to cover checkpoint byte-exactness.

## Disposition

Permanent. After Slice 4 a Crabka controller quorum self-heals a
log-pruned/rejoining follower via real KIP-630/FetchSnapshot. Remaining:
Slice 5 = KIP-853 dynamic voters; Slice 6 = full KRaft-field fidelity + mixed
JVM+Crabka quorum acceptance (including JVM-peer FetchSnapshot interop and
broker-observer snapshotting).
