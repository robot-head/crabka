# PG-6: Branching and PITR — design

**Date:** 2026-07-06
**Status:** Approved
**Type:** Subsystem design. The final slice of the [Chapter C roadmap](2026-07-06-crabka-postgres-chapter-roadmap-design.md) — copy-on-write timelines over the layer map. Mostly leverage: the layer store's versioned reads, the `pg/<tenant>/<timeline>/…` path segment PG-3 reserved, and PG-4b's versioned lifecycle were all shaped for this.

## Context — what a branch is here

A **timeline** gains optional ancestry: `{ timeline_id, ancestor: Option<(timeline_id, branch_lsn)> }`. Branching at LSN `L` is a **metadata-only** operation — no data copies: the child's reads at `lsn ≤ L` resolve through the ancestor chain; the child's own layers hold only its divergent history above `L`. **PITR is branching**: recover-to-a-point = create a branch at the historical LSN and serve reads (or a compute) from it. The PITR window is whatever GC preserves — and branch points *pin* ancestor history, extending PG-4's GC rule.

**Ownership convention (load-bearing):** the ancestor owns `(0, branch_lsn]`; the child owns `(branch_lsn, ∞)`. Child layers' LSN ranges start strictly above `branch_lsn` (enforced at ingest), which is what makes the ancestry recursion correct and unambiguous.

## Design Goals

- **Ancestry-aware reconstruction:** `get_reconstruct_data(timeline, key, lsn)` scans the timeline's own layers; if no base is found, it recurses into the ancestor at `min(lsn, branch_lsn)` — collecting one contiguous delta chain across the boundary. All key kinds (`Rel`, `Slru`, `RelMeta`) inherit identically, so sizes/lifecycle/SLRUs below the branch point come from the ancestor for free.
- **Metadata-only branch creation:** `CreateBranch(tenant, src_timeline, lsn) → timeline_id` writes one small **write-once** metadata object (`pg/<tenant>/<timeline>/timeline.meta`: id, ancestor, branch LSN) — crash-safe by construction.
- **GC that respects descendants:** a parent layer is deletable only if unneeded by (a) its own timeline's `gc_horizon` (PG-4's rule) **and** (b) every descendant's branch point — conservatively, ancestor layers reachable at `≤ branch_lsn` are pinned unless an image layer at `≤ branch_lsn` covers them.
- **Divergent ingest:** each timeline ingests its own WAL stream independently (a branched, promoted Postgres continues the *same LSN space* on a new PG timeline-id — real divergence, which the fixtures produce).
- **Rebuildability preserved:** ancestry reloads from the bucket alone (timeline metadata objects + per-timeline layer listings) — PG-3's no-metadata-service property, extended.

## Non-goals

- **Writable-branch compute end-to-end** — the storage/service side lands here completely (divergent ingest included); wiring a *live* branched compute (basebackup with a bumped PG timeline-id in `pg_control`, a per-branch `__pg_wal.<cluster>` topic, a fresh safekeeper instance) composes from PG-1/PG-5 parts and is validated in PG-5's harness once both slices are executed — not re-specified here.
- **Timeline deletion with descendants** — refused (`HasDescendants`); recursive deletion deferred.
- **Branch quotas/limits, scheduled PITR policies, time-based (vs LSN-based) branch points** (commit-time → LSN mapping needs `pg_commit_ts` or meta-lane timestamp indexing — deferred; XACT commit records carry timestamps, so the mapping is future-buildable from retained data).
- **Cross-tenant anything.**

## Architecture Overview

```
TIMELINE METADATA (write-once, bucket-native)
  pg/<tenant>/<timeline>/timeline.meta   { timeline_id, ancestor_id?, branch_lsn? }
  ancestry graph = load all timeline.meta under the tenant (rebuild = list + parse)

READ PATH (the one semantic change)
  get_reconstruct_data(T, key, lsn):
    scan T's layers for (key, ≤ lsn) newest-first → deltas… until an Image/will_init base
    if exhausted without base:
      match T.ancestor:
        Some((A, bl)) → recurse (A, key, min(lsn, bl)), prepending its result
        None          → HistoryTrimmed / NotFound (as today)
  invariant: T's layers ⊂ (branch_lsn, ∞)  ⇒  no double-count, no gap at the boundary

BRANCH  CreateBranch(tenant, src, lsn) → new timeline.meta (validates lsn ≤ src's last_record_lsn,
        ≥ src's gc_horizon — can't branch into GC'd history)
GC      pin: for each child(bl): parent layers with lsn_range.start ≤ bl stay unless image-covered ≤ bl
SERVICE GetPage/GetRelSize/Basebackup gain a `timeline` field (amends PG-4/5a protos, greenfield)
        + CreateBranch / ListTimelines / DeleteTimeline (refuse-on-descendants)
```

## Key Design Decisions

### Ancestry lives in the read path, not in data movement

Branching copies nothing and rewrites nothing; the recursion boundary (`min(lsn, branch_lsn)`) plus the ownership convention makes the child's view *provably* the parent's view below `L` — tested byte-for-byte. Materializing compaction on the **child** naturally localizes hot ancestor history over time (an image layer created on the child covers its reads without touching the parent), so ancestor pinning erodes as children compact — GC pressure self-relieves without a special mechanism.

### One small manifest exception, justified

PG-3 rejected manifests because layer maps rebuild from listings; **ancestry cannot be derived from a listing**, so timelines get the minimal write-once `timeline.meta`. Write-once means no consistency protocol: a branch exists iff its object exists; creation is one PUT; deletion (leaf-only) removes layers then the meta last.

### The fixture strategy produces *real* divergence

The generator branches reality: `pg_basebackup` at `L`, **promote** the standby (PG bumps its timeline-id — same LSN space, genuinely divergent WAL), run *different* traffic on parent and promoted child, capture both streams and both standby snapshots. PG-2 already parses `xlp_tli`; each Crabka timeline ingests its own stream as a fresh LSN-addressed feed. The gate then has a true oracle on both sides of the fork.

### The service surface is amended, not extended sideways

`GetPage`/`GetRelSize`/`Basebackup` requests gain a `timeline` field (PG-4/PG-5a proto amendment — greenfield, executors fold it in); plus `CreateBranch`/`ListTimelines`/`DeleteTimeline`. `CreateBranch` validates the LSN window (≤ ingested head, ≥ gc_horizon) so a branch never points at trimmed or unwritten history.

## Integration

- **`crates/page-store`** — `TimelineMeta` + the ancestry-aware `get_reconstruct_data` + the GC pinning term (extends PG-3/4's drivers).
- **`crates/pageserver`** — the three new RPCs; `timeline` on existing requests; per-timeline live ingest (already timeline-scoped); basebackup@timeline.
- **`tools/gen-pg-wal-fixtures.sh`** — the promote-and-diverge capture.
- **Roadmap** — PG-6's entry updated; the chapter's design cycle closes.

## Kafka / wire compliance

Not a wire surface. Fidelity bar: **a child's page at `lsn ≤ branch_lsn` is byte-identical to the parent's at the same LSN**, and each side of a divergence matches *its own* standby capture — the PG-4 oracle, forked.

## Testing

- **Ancestry unit tests:** child-reads-below-`L` ≡ parent reads (all three key kinds, incl. a `RelMeta` size inherited across the boundary); a chain of two branches (grandchild) recurses correctly; boundary exactness (an entry at exactly `branch_lsn` belongs to the ancestor; the child's first own entry is `> branch_lsn`, enforced at ingest).
- **Divergence gate:** ingest the forked fixture streams into parent + child timelines; each side's pages match its own standby capture; shared history below `L` identical from both.
- **GC pinning:** run parent GC with a child at `L` — child reads below `L` unaffected; after the child gains image coverage `≤ L` (materializing compaction), previously-pinned parent layers become collectable and their deletion still leaves both timelines' probe grids unchanged.
- **Branch validation:** `CreateBranch` beyond the ingested head or below `gc_horizon` errors; `DeleteTimeline` with a descendant → `HasDescendants`; leaf deletion removes layers then meta.
- **Rebuild:** drop all in-memory state; reload timelines + layer maps from the bucket; the full probe grid (both timelines) identical.
- **Basebackup@branch:** a tarball at `(child, L')` validates under `pg_controldata` and its SLRU segments match the child's standby capture (PG-4b machinery, timeline-scoped).

## Risks (carried into the plan)

- **GC pinning correctness is the slice's sharpest edge** — an over-eager collection silently corrupts a child's past. The pinning term is property-tested (no deletion changes any probe read on any timeline) and conservatively image-gated.
- **Branch-LSN placement:** branching mid-record would be meaningless; `CreateBranch` snaps to the record boundary ≤ requested LSN (the layer store only knows record end-LSNs — documented behavior).
- **Ancestor read amplification** on deep chains — bounded in practice by child compaction (the self-relieving property above); chain depth is unlimited v1, flagged for a future limit.
- **Proto amendment discipline** — same note as PG-4b's key change: if PG-4/5a execution started, adding `timeline` is a mechanical greenfield refactor, sequenced deliberately.

## Resolved decisions

- **Model:** ancestry metadata + read-path recursion; ancestor owns `(0, bl]`, child owns `(bl, ∞)`; no data movement.
- **Persistence:** write-once `timeline.meta` (the one justified manifest); rebuild from listing preserved.
- **GC:** descendant branch points pin, image coverage un-pins; property-tested.
- **Fixtures:** promote-and-diverge — real forked WAL with per-side standby oracles.
- **Scope:** storage + service complete (incl. divergent ingest + basebackup@branch); live branched-compute validation rides PG-5's harness; deletion is leaf-only.
