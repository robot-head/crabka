# KRaft metadata snapshots (KIP-630) — design

## Goal

Implement KIP-630 (Kafka Raft Snapshot) for Crabka's `@metadata` quorum log:
snapshot the `MetadataImage` at a committed offset, persist it as a canonical
on-disk artifact, compact the metadata log behind it, let lagging controller
followers catch up via the snapshot, and serve the snapshot to external callers
through the public `FetchSnapshot` API (api key 59).

Without snapshots the metadata log grows unbounded and a controller that falls
behind the log start offset can never catch up. KIP-630 is the mechanism that
bounds the log and bootstraps lagging replicas.

## Constraints and decisions

These were settled during brainstorming:

1. **Full KIP-630**, decomposed into slices (S1–S5 below).

2. **Single canonical artifact.** One `.checkpoint` file per snapshot serves
   *both* openraft's internal catch-up (InstallSnapshot, private RPC key 1002)
   *and* the public `FetchSnapshot` API. No divergent on-disk formats, no second
   serializer to keep in sync.

3. **Kafka-faithful trigger configs.** Implement
   `metadata.log.max.record.bytes.between.snapshots` (default 20 MB) and
   `metadata.log.max.snapshot.interval.ms` (default 1 h), matching JVM controller
   behaviour. A snapshot is triggered when *either* threshold is crossed.

4. **Internal record framing now, native later.** The `FetchSnapshot` RPC
   envelope (field order, error codes, version negotiation) and the `.checkpoint`
   file framing (record batches, header/footer control batches,
   `<offset>-<epoch>.checkpoint` naming) are byte-exact KIP-630. The *record
   values inside* the data batches use Crabka's existing bincode `MetadataRecord`
   encoding — the same framing the live `@metadata` log already uses. Mapping the
   14 `MetadataRecord` variants to native apiKey-framed Kafka metadata records is
   explicitly out of scope (it is the same "KRaft-wire-compat" work already
   deferred in `crates/raft/src/log_store.rs`). A Crabka controller/observer can
   consume the snapshot; a JVM `kafka-metadata-shell` cannot parse the contents
   yet — the same limitation the live log already has.

## Grounding facts (from the current code)

- openraft entry `log_id.index` maps **1:1** to the Kafka metadata offset:
  `RaftLogStore::append` sets `base_offset = entry.log_id.index` with one record
  per batch (`crates/raft/src/log_store.rs`). Therefore a snapshot taken at
  applied index `N` has `SnapshotId.end_offset = N + 1` (end offset is
  exclusive — it covers records up to but not including `end_offset`).
- `MetadataImage` is the snapshot subject; `apply(&MetadataRecord)` mutates a
  clone and the state machine swaps the `Arc` (`crates/raft/src/state_machine.rs`).
  Snapshotting needs the inverse: `image → Vec<MetadataRecord>`.
- openraft transmits `SnapshotMeta` (`last_log_id` + `last_membership`)
  **out-of-band** in its InstallSnapshot RPC, separate from the snapshot data
  bytes. So the `.checkpoint` file only needs the image records; openraft's
  membership bookkeeping is persisted in a small sidecar file.
- `Log::truncate_to(offset)` already advances the log start offset / deletes
  sealed segments. `RaftLogStore::purge` is currently a no-op
  (`crates/raft/src/log_store.rs`) — that is the truncation hook.
- The `FetchSnapshot` request/response types are fully generated, including
  `Position`-based paging and `SnapshotId { end_offset: i64, epoch: i32 }`
  (`crates/protocol/generated/FetchSnapshotRequest.owned.rs`). The handler and
  the openraft snapshot trait methods are stubbed.

## Architecture

```
build_snapshot (openraft) ──┐
                            ├─► SnapshotWriter ─► <offset>-<epoch>.checkpoint   (canonical artifact)
trigger (bytes/time, S4) ───┘                     + <offset>-<epoch>.checkpoint.meta (sidecar)
                                                          │
purge (openraft) ─► Log::truncate_to(end_offset) ◄────────┘   (compact log behind snapshot)

install_snapshot (openraft RPC 1002) ─► SnapshotReader ─► rebuild MetadataImage
FetchSnapshot handler (api 59) ────────► SnapshotReader byte-range ─► unaligned_records
```

### Canonical artifact layout

A `.checkpoint` file is a concatenation of Kafka record batches:

1. **Header**: a control batch containing a `SnapshotHeaderRecord` (KIP-630
   control-record structure, version 0, carrying `last_contained_log_timestamp`).
2. **Data**: one or more normal record batches whose records' `value` payloads
   are bincode-encoded `MetadataRecord`s derived from the image.
3. **Footer**: a control batch containing a `SnapshotFooterRecord` (version 0).

File naming: `format!("{:020}-{:010}.checkpoint", end_offset, epoch)`, located in
`<log_dir>/@metadata-0/` alongside the log segments. Writes are atomic
(temp file + rename).

### Sidecar metadata

openraft requires `get_current_snapshot` to return a `SnapshotMeta` containing
`last_log_id` and `last_membership`, which must survive restart. The
`.checkpoint` file itself stays pure KIP-630 (record batches only), so openraft
bookkeeping goes in a sibling `<offset>-<epoch>.checkpoint.meta` file holding
bincode-serialized `(last_log_id, StoredMembership)` — the same sidecar pattern
already used for `vote.bin`. On install, openraft hands us `SnapshotMeta` in the
RPC, so we write both the `.checkpoint` (from the data bytes) and the `.meta`
(from the supplied meta).

## Slices

Execution order and parallelism (per CLAUDE.md): `[S1]` → `[S2]` →
`[S3, S4, S5]` in parallel. The batch-3 file sets do not overlap.

### S1 — Snapshot format: writer + reader

The foundational, self-contained format layer.

- `crates/metadata/src/image.rs`: add `MetadataImage::to_records() ->
  Vec<MetadataRecord>` (image → the minimal record sequence that reconstructs it)
  and `MetadataImage::from_records(cluster_id, &[MetadataRecord]) ->
  MetadataImage` (replay via existing `apply`). Round-trip: `from_records(_,
  &image.to_records()) == image`.
- `crates/raft/src/snapshot.rs` (new): `SnapshotId { end_offset, epoch }` with
  filename `format`/`parse`; `SnapshotWriter` (image + header timestamp → file
  bytes, atomic write); `SnapshotReader` (file → `Vec<MetadataRecord>` /
  `MetadataImage`, plus a byte-range read for serving FetchSnapshot).
- Register the module in `crates/raft/src/lib.rs`.

**Tests:** image → file → image equality across every `MetadataRecord` variant;
filename format/parse round-trip; byte-range read returns the expected slice.

**Files:** `crates/metadata/src/image.rs`, `crates/raft/src/snapshot.rs`,
`crates/raft/src/lib.rs`.

### S2 — Generation + log truncation

Wire snapshot creation and compaction into openraft.

- `crates/raft/src/state_machine.rs`: implement `RaftSnapshotBuilder::
  build_snapshot` (current image + `last_applied` + `last_membership` → write
  `.checkpoint` via S1 writer, write `.checkpoint.meta` sidecar, return
  `Snapshot` with `SnapshotData` cursor) and `get_current_snapshot` (load the
  newest `.checkpoint` + `.meta` from disk). The state machine needs the snapshot
  directory path, threaded from `CrabkaStateMachine::new`.
- `crates/raft/src/log_store.rs`: replace the no-op `purge` with a real
  truncation that advances `last_purged` and calls `Log::truncate_to(index)`;
  tighten `get_log_state`'s `last_purged_log_id` precision.
- `crates/raft/src/config.rs`: pass the snapshot directory; set openraft's
  built-in `snapshot_policy` to effectively never (snapshots are driven manually
  by S4's trigger), so S2 can be exercised via `raft.trigger().snapshot()`.

**Tests:** trigger a snapshot, assert the `.checkpoint` + `.meta` files appear and
`end_offset` matches; assert the log is truncated (old segments deleted,
`log_start_offset` advanced); restart and confirm recovery from snapshot + log
tail reproduces the image.

**Files:** `crates/raft/src/state_machine.rs`, `crates/raft/src/log_store.rs`,
`crates/raft/src/config.rs`.

### S3 — InstallSnapshot (follower catch-up)

Internal openraft snapshot install over the private RPC.

- `crates/raft/src/state_machine.rs`: implement `begin_receiving_snapshot`
  (allocate the receive buffer) and `install_snapshot` (rebuild `MetadataImage`
  from the received bytes via S1 reader, swap the image, persist `.checkpoint` +
  `.meta` from the supplied `SnapshotMeta`, set `last_applied` / `last_membership`
  from the meta).
- `crates/raft/src/wire.rs`, `crates/raft/src/server.rs`,
  `crates/raft/src/network.rs`: implement real InstallSnapshot streaming on
  private key 1002 (currently rejected with `REJECT_NOT_IMPLEMENTED`). The
  outbound side rides the existing dialer (TLS/SASL) like AppendEntries/Vote.

**Tests:** integration — a learner/follower whose log is behind the leader's
(truncated) log start offset catches up via snapshot install; assert its image
equals the leader's.

**Files:** `crates/raft/src/state_machine.rs`, `crates/raft/src/wire.rs`,
`crates/raft/src/server.rs`, `crates/raft/src/network.rs`, a test under
`crates/raft/tests/`.

### S4 — Kafka-faithful triggers

Config-driven automatic snapshotting.

- Config: add `metadata.log.max.record.bytes.between.snapshots` (default
  20 MB) and `metadata.log.max.snapshot.interval.ms` (default 1 h) to
  `ControllerConfig` and the broker config surface.
- `crates/raft/src/controller.rs`: a background task tracks bytes appended to
  the metadata log since the last snapshot and an interval timer; when either
  threshold is crossed it calls `raft.trigger().snapshot()`. The task is drained
  on `shutdown`/`cancel` like the existing leader-pump task.

**Tests:** appending past the byte threshold triggers a snapshot; the interval
timer triggers a snapshot on an otherwise idle log.

**Files:** `crates/raft/src/config.rs`, `crates/raft/src/controller.rs`, broker
config crate.

### S5 — FetchSnapshot API (key 59)

The public Kafka-wire handler.

- `crates/broker/src/handlers/fetch_snapshot.rs` (new): decode
  `FetchSnapshotRequest`; validate `cluster_id` and that the requested topic is
  `__cluster_metadata` partition 0; locate the requested `SnapshotId` on disk;
  serve bytes from `Position` up to `max_bytes` via the S1 reader's byte-range;
  build a byte-exact `FetchSnapshotResponse` (`size`, `position`,
  `unaligned_records`, current leader epoch). Error codes: `SNAPSHOT_NOT_FOUND`,
  `POSITION_OUT_OF_RANGE`, `NOT_LEADER` as appropriate.
- Register the handler at key 59 in `crates/broker/src/handlers/mod.rs` and
  advertise api key 59 in `ApiVersions`.

**Tests:** full snapshot fetch reassembled from a single response; paged fetch
across multiple `Position` requests reassembles the same bytes; bad
`cluster_id` / unknown `SnapshotId` / out-of-range `Position` return the correct
error codes.

**Files:** `crates/broker/src/handlers/fetch_snapshot.rs`,
`crates/broker/src/handlers/mod.rs`.

## Out of scope (follow-ups)

- **Native apiKey-framed metadata records** in snapshot contents (decision 4).
- **Fetch-path snapshot divert**: pointing a lagging external observer to a
  snapshot via the `SnapshotId` field in a metadata `Fetch` response. Crabka
  brokers currently subscribe to the metadata image in-process via a watch
  channel rather than fetching the `@metadata` log over the Kafka wire, so there
  is no external observer Fetch path to divert yet.
- **KIP-853 voter records** (`kraft.version` 1) in snapshots; membership stays in
  openraft's domain (the sidecar `.meta`).
```
