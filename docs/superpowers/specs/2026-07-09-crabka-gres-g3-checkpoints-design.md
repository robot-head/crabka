# Gres G-3: Checkpoints — design

**Date:** 2026-07-09
**Status:** Approved
**Type:** Slice design. The third slice of [Chapter Gres](2026-07-09-crabka-gres-chapter-design.md): checkpoints bound both spin-up time and WAL growth, completing the disposable-compute story G-2 started. One G-2 amendment (the recovery barrier) was resolved in this cycle and applied back to the G-2 spec and plan.

## Context — what the tree and its dependencies actually hold

1. **fjall (3.x) has the online-snapshot primitive but no file-level checkpoint.** `Database::snapshot()` opens a cheap cross-keyspace MVCC snapshot (a pinned sequence number) that can be iterated (`snap.iter(&keyspace)`, `range`, `prefix`) while writes continue; old versions are retained only while a snapshot is alive, so snapshots should be shorter-lived than a compaction cycle but comfortably survive a checkpoint upload. There is no RocksDB-style checkpoint/hard-link API (upstream issue #52, open, design-only), so file-level copies of an open store are off the table. For restore, `Keyspace::start_ingestion()` bulk-loads a strictly-ascending pre-sorted stream directly into level-1 tables, bypassing journal and memtable — exactly the shape of a checkpoint scan, and explicitly recommended over `write_batch` loops for bulk load.
2. **The vendored `Kv` trait cannot stream.** `scan_prefix(b"")` is the only whole-store scan and it materializes every key/value into one `Vec` (both backends); `scan_range` cannot express an unbounded end, and consistency across chunked calls is not provided. A checkpoint writer with bounded memory therefore needs a new seam, not a clever use of the existing one. Re-confirmed: the engine has no persistent state outside the kv store, so a checkpoint of the store's logical contents is complete by construction.
3. **Truncation machinery exists broker-side and nowhere client-side.** The broker's `DeleteRecords` handler trims leader-locally: it physically deletes sealed segments below the target and advances `log_start_offset` (returned as `low_watermark`); fetches below the new start surface `OFFSET_OUT_OF_RANGE`, which `client-core` maps to `ClientError::Server { error_code: 1 }`. No client crate exposes DeleteRecords — in-repo callers hand-roll it via `Client::send`. Two divergences to design around: background `retention.ms` eviction exists in the log crate but is **not wired to any broker ticker** (explicit DeleteRecords is the only production truncation path on classic topics — good: we depend on nothing speculative), and `ListOffsets` ignores `isolation_level`, returning LEO rather than LSO — which motivated the G-2 amendment below.
4. **The bucket idioms are immutable keys and manifest-conventions, not CAS.** The repo uses no conditional puts anywhere; the blockstore's index-snapshot pattern (immutable zero-padded keys, list-latest, retain-N prune) is the house idiom for bucket-side history, and the object-store plumbing (`ObjectStoreConfig`/`build_object_store`/`ObjectOps`, streaming multipart above a threshold) is directly reusable.

## The G-2 amendment (resolved here, applied to the G-2 spec/plan)

G-2's recovery read its replay target from `ListOffsets(latest, read_committed)`; grounding showed the handler ignores `isolation_level` and returns LEO. The replacement is strictly better: after `init_transactions()` fences predecessors, the new compute **produces a barrier record** — an empty `GRW1` frame carrying the next `journal_seq`, in its own committed Kafka transaction — and replays until it consumes its own barrier. The end of replay is then self-delimiting (the barrier is committed, so a READ_COMMITTED fetch always reaches it), immune to ListOffsets semantics, well-defined when the tail contains only aborted zombie data or transaction markers, and it stamps generation boundaries into the journal for free. G-3's tail replay inherits it: restore a checkpoint, replay from the checkpoint's covered offset to the successor's own barrier.

## Design Goals

- **Bounded spin-up:** recovery cost is checkpoint-download + tail-replay — proportional to **live** store size, not tenant history. *(Reworded after the scaling review: the engine never vacuums — no dead-version deletion, no clog truncation exist in the donor — so without the garbage horizon below, "checkpoint size" silently means "everything ever written" and this goal is false. The horizon is therefore in-scope for this slice, not optional polish.)*
- **A garbage horizon:** each checkpoint is also the vacuum — dead versions and unreferenced clog below the horizon do not survive into the checkpoint, so checkpoint size tracks live data and a restored store is compacted for free.
- **Bounded log:** the WAL topic holds only the tail since the last durable checkpoint.
- **Crash-anywhere safety:** a failure at any step boundary (scan, upload, manifest, truncate, prune) leaves only redundant data, never a hole; recovery detects the one impossible state (log start beyond the newest manifest) and refuses to serve.
- **Backend-agnostic seams, fjall-fast paths:** the checkpoint/restore machinery works over any `Kv` backend; fjall gets the MVCC-snapshot and ingestion fast paths.

## Non-goals

- **Incremental/delta checkpoints** — full-store checkpoints only; the many-small-tenants product makes full checkpoints cheap, and the disaggregated-store follow-on (chapter approach B) is the real answer for large tenants.
- **Checkpoint-serving reads or branching** — checkpoints are recovery artifacts, not a query tier.
- **Cross-tenant coordination** — each tenant checkpoints independently.
- **Background broker retention** — truncation is explicit DeleteRecords by the compute that owns the tenant.

## Architecture Overview

```
WAL writer task (G-2)                          checkpointer task (new)
─────────────────────                          ───────────────────────
… commit group N   (apply to store)
[checkpoint due: bytes/records since last]
between groups:                                stream snapshot pairs →
  snap = store.snapshot()      ──(snap, covered_offset, journal_seq, epoch)──►
  resume groups immediately                      part objects (chunked, sorted):
… commit group N+1 …                               gres/<tenant>/ckpt/<offset>-<epoch>/part-00000
                                                   …
                                                 MANIFEST written last
                                                 → DeleteRecords(covered_offset)
                                                 → prune old ckpt prefixes (retain 2)

recovery (G-3 shape):
  fence (init_transactions) → produce barrier
  → list gres/<tenant>/ckpt/*/MANIFEST, pick highest offset
  → restore parts (fjall ingestion / write_batch chunks) → verify counts+checksums
  → replay WAL from covered_offset until own barrier (merge rules, seq tripwire)
  → reseed counters → serve
  refuse loudly if: no manifest covers the log start (torn truncation),
                    manifest incomplete, checksum mismatch, or journal_seq gap
```

## Key Design Decisions

### A `KvSnapshot` capability, taken between commit-groups

`crabka-pgkv` gains a snapshot seam: a trait (`SnapshotKv`) exposing `snapshot() -> Box<dyn KvSnapshotIter>` — a consistent point-in-time, **streaming** iterator over the whole store in key order. `FjallKv` implements it with `Database::snapshot()` (acquisition is effectively instantaneous; iteration proceeds while writes continue); `MemKv` clones its map (small tenants by definition). The WAL writer owns every store apply, so a checkpoint-control message processed **between commit-groups** gives the snapshot an exact WAL position for free: `(covered_offset, journal_seq, producer_epoch)` are read off the writer's own counters at the instant of acquisition, with no quiesce window beyond that instant. The alternative — chunked `scan_range` with the writer paused for the whole scan — was rejected: it stalls writes for a duration proportional to store size and demands successor-key arithmetic the streaming seam makes unnecessary.

### Immutable per-checkpoint prefixes, manifest-last, no CAS

Each checkpoint lives under `gres/<tenant>/ckpt/<offset>-<epoch>/` (zero-padded offset so lexical order is offset order; the producer epoch disambiguates the theoretical same-offset collision between a zombie and its successor). Part objects hold length-prefixed sorted key/value pairs, chunked at a size threshold and uploaded with the existing `ObjectOps` streaming; `MANIFEST` — part names, pair counts, per-part checksums, covered offset, `journal_seq`, a `wal_generation` counter (0 in this slice; G-5's topic parking bumps it so recovery can tell a fresh WAL topic from a truncated one — schema lands now to avoid format churn), format version — is written **last**, so a torn upload is invisible to recovery. No conditional puts are needed (the repo has no CAS idiom to lean on anyway): manifests are immutable, recovery picks the highest-offset manifest, and a fenced zombie can only ever write a checkpoint of a valid prefix at an offset at or below its fence point — harmless by construction, pinned by the model.

### Truncate after manifest; prune after truncate; every gap survivable

Ordering: manifest durable → `DeleteRecords(covered_offset)` → prune all but the newest 2 checkpoint prefixes (the blockstore retain-N idiom). A crash after manifest but before truncation leaves a longer tail (replayed harmlessly — apply is idempotent under the merge rules); after truncation but before prune leaves extra old checkpoints (pruned next time). The impossible state — log start beyond the newest manifest's covered offset — can only mean bucket loss or manual interference, and recovery refuses to serve rather than reconstruct silently wrong state. `AdminClient` gains a public `delete_records` method (the protocol type and broker handler exist; only the client API is missing), which is a generally useful addition to the published admin client, not gres-private machinery.

### Restore through the seam, with the fjall fast path

Recovery downloads parts in order and rebuilds the store from the sorted stream: `FjallKv` via fjall's ingestion API (strictly-ascending guarantee holds — parts are written in key order from a snapshot iterator), any other backend via chunked `write_batch`. Counts and checksums are verified against the manifest before the store is trusted; then the WAL tail replays from `covered_offset` exactly as in G-2 (merge rules, `journal_seq` continuity from the manifest's recorded seq, ending at the successor's own barrier).

### Vacuum-into-checkpoint: the checkpoint scan is the garbage collector

*(Added after the scaling review.)* The engine retains every dead row version and every clog entry forever — tolerable for the donor's long-lived single node, fatal for a design whose spin-up, suspend, and upload costs all ride checkpoint size: a write-hot tenant of constant logical size grows its checkpoint with calendar time. The fix rides the machinery this slice already builds. At snapshot acquisition the writer also stamps the **horizon** — the oldest xid visible to any active snapshot (from the engine's ProcArray; with no active sessions it is simply the next xid). While streaming the snapshot into parts, the checkpointer applies three rewrite rules: (1) **drop** row versions whose `xmax` is a committed xid below the horizon — dead to every present and future snapshot; (2) **freeze** surviving versions whose `xmin` is committed below the horizon by rewriting `xmin` to the frozen sentinel (added to `crabka-pgmvcc` with visibility treating it always-committed — PostgreSQL's own freeze concept); (3) **emit clog entries only for xids at or above the horizon** — frozen/pruned tuples no longer consult them. The result restores to a visibility-equivalent, compacted store. The live store still accretes between checkpoints (bounded by restart/resume frequency, since every restore compacts); a `compact` maintenance operation — checkpoint followed by self-restore — is named as a future operational knob, not built here. Correctness gate: a differential property test that the pre-prune and post-prune stores answer every visibility question identically at the checkpoint instant, plus conformance-on-substrate staying at baseline across a checkpoint/restore cycle.

### The Stateright model is the gate's centerpiece

Per the chapter, the fence/checkpoint/truncate/recover protocol gets an exhaustive model (the donor's discipline; its SP21 torn-commit is the standing warning). The model abstracts: a journal of frames, a log-start pointer, a set of `(offset, epoch)` checkpoints with a manifest-present bit, computes with generations that can crash at every step boundary, and a zombie checkpointer racing a successor. Invariants: a recovered store always equals the reference fold of the acked journal prefix; recovery either serves correct state or refuses; truncation never creates an unrecoverable state. Model actions mirror the real step boundaries one-to-one so the model stays an honest abstraction of the code.

### Trigger policy is deliberately dumb

Checkpoint when WAL-bytes-since-last or frames-since-last exceed configured thresholds (checked by the writer between groups), plus a startup checkpoint after recovery if the replayed tail exceeded the threshold (so a crash loop cannot grow the tail unboundedly). Time-based and load-aware policies are tuning, not architecture — deferred until G-5's lifecycle work gives real idle/cold-start data.

## Integration

- **`crates/pgkv`:** the `SnapshotKv` seam + fjall/mem implementations + the fjall ingestion-restore helper (public, documented; published crate).
- **`crates/gres-substrate`:** checkpointer task, part/manifest codec, upload/prune, restore, the extended recovery; writer gains the checkpoint control message.
- **`crates/client-admin`:** public `delete_records(&mut self, ops: &[DeleteRecordsOp], timeout_ms) -> Result<Vec<DeleteRecordsOutcome>, AdminError>` following the existing `create_topics` shape.
- **`crates/object-store`:** consumed as-is (`ObjectStoreConfig`, `ObjectOps`, multipart streaming).
- **`crates/gres`:** substrate mode gains bucket configuration (the broker's `[remote_storage]`-style typed config mapped to `ObjectStoreConfig`) and checkpoint threshold flags.

## Kafka / wire compliance

DeleteRecords is standard Kafka wire; the new client method encodes the stock request. Two documented broker divergences are load-bearing knowledge, not blockers: DeleteRecords validates against LEO (not HW) and ignores `timeout_ms`; ListOffsets ignores `isolation_level` (the recovery barrier removes our dependence on it). Fixing those divergences belongs to the broker's Kafka-compatibility track, not this slice.

## Testing

- **Codec + manifest units:** part framing round-trips (proptest); manifest serialization; checksum verification failures refuse restore.
- **Restore equivalence:** property test — random op sequences → checkpoint → restore → store equals reference fold; fjall-ingestion and write_batch paths both covered.
- **Crash-anywhere integration (deterministic, in-process broker + `InMemory` bucket):** drive workload, force a checkpoint, kill the compute at each step boundary (before upload / mid-parts / after manifest / after truncate / before prune), recover, assert exact acked state every time; assert bounded tail after truncation (fetch below covered offset errors, replay starts at the manifest).
- **Torn-truncation refusal:** delete the newest manifest under a truncated log; recovery must refuse to serve with the named error.
- **The Stateright model** (gate): exhaustive over the abstracted protocol per the decision above.
- **Spin-up bound (gate):** with checkpoints enabled, recovery time on a grown tenant is measured against checkpoint-size + tail-length, not history (the deterministic workload driver from the chapter's testing story).

## Risks

- **Long-lived fjall snapshots retain old versions** — bounded by upload duration; mitigated by chunked streaming (upload speed), a snapshot-age metric, and the retain-2 policy keeping uploads small for small tenants. Large-tenant checkpoint cost is the known approach-A trade, owned by the disaggregated-store follow-on.
- **Checkpoint uploads can lose the race against a hot writer** *(added after the scaling review)*: if sustained WAL ingress exceeds upload bandwidth, truncation never advances and tail + snapshot-retention costs compound. The design's admission: this is a **detected condition, not a handled one** — a `checkpoint_lag` metric (WAL bytes/frames since the last durable manifest) plus a loud warning threshold ship with the checkpointer; tenants that sustain it have outgrown approach A and the operator's lever is graduation, not tuning.
- **`AdminClient::delete_records` is a published-API addition** — reviewed as a first-class client feature (docs, tests, JVM-parity semantics documented), not smuggled in.
- **Manifest format evolution** — versioned from day one (`format_version` field; unknown versions refuse restore).
- **Zombie checkpoint uploads waste bucket writes** — harmless for correctness (model-pinned); the fenced flag stops the zombie's checkpointer quickly in practice.

## Resolved decisions

- Snapshot seam: `SnapshotKv` in `crabka-pgkv`; acquisition between commit-groups stamped with `(covered_offset, journal_seq, epoch, horizon_xid)`; streaming iteration concurrent with writes.
- Garbage horizon: the checkpoint scan prunes dead versions below the horizon, freezes old committed `xmin`s, and drops sub-horizon clog — checkpoints track live size; visibility-equivalence is the gate.
- Layout: `gres/<tenant>/ckpt/<offset>-<epoch>/part-NNNNN` + `MANIFEST` last; immutable; retain 2; no CAS.
- Ordering: manifest → DeleteRecords → prune; recovery refuses on log-start-beyond-manifest, bad checksums, or seq gaps.
- Restore: fjall ingestion fast path, `write_batch` fallback; verify before trust.
- Client addition: `AdminClient::delete_records`.
- G-2 amendment: recovery barrier replaces ListOffsets as the replay terminator (applied to the G-2 spec and plan).
- Trigger: bytes/frames thresholds + post-recovery checkpoint; smarter policies deferred.
