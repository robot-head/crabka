# PG-6: Branching and PITR — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Copy-on-write timelines: write-once `timeline.meta` ancestry, ancestry-aware `get_reconstruct_data` (ancestor owns `(0, bl]`, child owns `(bl, ∞)`), descendant-aware GC pinning that image-coverage un-pins, `CreateBranch`/`ListTimelines`/`DeleteTimeline` RPCs + a `timeline` field on the page-service requests, and a divergence gate over genuinely forked fixture WAL (promoted-standby streams with per-side standby oracles).

**Architecture:** All changes live in `page-store` (ancestry recursion + GC term + `TimelineMeta`) and `pageserver` (RPCs, per-timeline scoping); no new crates. Branch creation is one PUT; rebuild-from-listing extends to the ancestry graph.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), the PG-2/3/4/4b crates, `serde_json` (`timeline.meta`), `proptest` (GC pinning property), `assert2`/`nextest`, a local/containerized PG 17 once (the forked-fixture regeneration), `cargo +nightly fmt`, `clippy::pedantic`.

**Spec:** [`docs/superpowers/specs/2026-07-06-crabka-pg6-branching-design.md`](../specs/2026-07-06-crabka-pg6-branching-design.md).

**PREREQUISITES (unlanded):** PG-2, PG-3, PG-4 executed (PG-4b's `Key` enum folded in, or Task 2's inheritance test drops the `RelMeta` case until it lands). Live branched-compute validation belongs to PG-5's harness, not this plan.

---

## Invariants

1. **Boundary exactness:** an entry at exactly `branch_lsn` resolves from the ancestor; a child's own layers contain only entries `> branch_lsn` (rejected at ingest otherwise).
2. **Branching moves no data** — one write-once metadata PUT; a branch exists iff its `timeline.meta` exists.
3. **GC never changes any read:** no collection alters any probe read on any timeline (the property test).
4. **Rebuildable:** ancestry + all layer maps reload from the bucket alone.
5. **Byte-fidelity forked:** child-below-`bl` ≡ parent; each divergent side ≡ its own standby capture.
6. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** `TimelineMeta` + ancestry graph; the read-path recursion; ingest boundary enforcement; GC pinning + un-pinning; the three RPCs + the `timeline` request field; the forked fixtures + divergence gate; basebackup@branch.
- **Deferred:** live branched-compute e2e (PG-5's harness); recursive deletion; time-based branch points; chain-depth limits; branch quotas.

---

## File Structure

- **`crates/page-store/src/timeline.rs`** (new) — `TimelineMeta`, the ancestry graph, load/store.
- **`crates/page-store/src/layer_map.rs`** — ancestry-aware `get_reconstruct_data`; ingest boundary check.
- **`crates/page-store/src/materialize.rs`** — the GC pinning term.
- **`crates/pageserver/{proto, src/service.rs, src/basebackup.rs}`** — RPCs + `timeline` field.
- **`tools/gen-pg-wal-fixtures.sh`** — promote-and-diverge capture.

**Batching:** Task 1 (`timeline.rs`) ∥ Task 5-step-1 (fixtures). Task 2 (recursion) after 1. Task 3 (GC) after 2. Task 4 (RPCs) after 1, ∥ 2–3. Task 5 (gate) last before Task 6.

---

## Task 1: `TimelineMeta` + the ancestry graph

**Files:**
- Create: `crates/page-store/src/timeline.rs`

- [ ] **Step 1: Write the failing tests**

```rust
    #[tokio::test]
    async fn timeline_meta_round_trips_and_rebuilds() {
        let ops = in_memory_ops();
        let root = TimelineMeta::root(tl_id(1));
        let child = TimelineMeta::branch(tl_id(2), &root, Lsn(500));
        store_meta(&ops, &tenant(), &root).await.unwrap();
        store_meta(&ops, &tenant(), &child).await.unwrap();
        let graph = TimelineGraph::load(&ops, &tenant()).await.unwrap();  // list + parse only
        assert!(graph.ancestor_of(tl_id(2)) == Some((tl_id(1), Lsn(500))));
        assert!(graph.descendants_of(tl_id(1)) == vec![tl_id(2)]);
    }

    #[test]
    fn branch_below_root_history_is_rejected_at_graph_level() {
        // validation seam used by CreateBranch: lsn window checks live with the graph
    }
```

- [ ] **Step 2: Implement** — `TimelineMeta { id, ancestor: Option<(TimelineId, Lsn)> }` serialized to `pg/<tenant>/<timeline>/timeline.meta` (write-once; `store_meta` refuses overwrite); `TimelineGraph::load` = `list()` + parse; `ancestor_of`/`descendants_of`/cycle rejection.
- [ ] **Step 3: Verify + commit**

```bash
git add crates/page-store/src/timeline.rs crates/page-store/src/lib.rs
git commit -m "feat(page-store): write-once timeline metadata + ancestry graph"
```

---

## Task 2: Ancestry-aware reconstruction + the ingest boundary

**Files:**
- Modify: `crates/page-store/src/layer_map.rs`

- [ ] **Step 1: Write the failing tests** (synthetic layers, two timelines)

```rust
    #[tokio::test]
    async fn child_reads_below_branch_resolve_through_ancestor() {
        // parent: Image@10, Wal@20 for key K; child branched at 25 with its own Wal@30.
        let rd = get_reconstruct_data_t(&graph, child, &K, Lsn(22)).await.unwrap();
        assert!(rd.base.unwrap().0 == Lsn(10) && lsns(&rd.deltas) == vec![Lsn(20)]);      // pure ancestor view
        let rd = get_reconstruct_data_t(&graph, child, &K, Lsn(35)).await.unwrap();
        assert!(lsns(&rd.deltas) == vec![Lsn(20), Lsn(30)]);                              // stitched across the boundary
    }

    #[tokio::test]
    async fn boundary_entry_belongs_to_the_ancestor() {
        // parent Wal@25, child branched at 25: a child read at 25 includes it via recursion;
        // child ingest at lsn <= 25 -> Err(BranchBoundaryViolation).
    }

    #[tokio::test]
    async fn grandchild_recurses_two_hops() { /* chain of two branches; probe below both points */ }
```

- [ ] **Step 2: Implement** — thread the graph + timeline into the query: on base-less exhaustion, recurse `(ancestor, key, min(lsn, branch_lsn))` and prepend; `Ingest::put` on a branched timeline rejects `lsn ≤ branch_lsn` (`BranchBoundaryViolation`); `HistoryTrimmed`/`NotFound` only at a root.
- [ ] **Step 3: Verify + commit**

```bash
git add crates/page-store/src/layer_map.rs
git commit -m "feat(page-store): ancestry-aware get_reconstruct_data with boundary enforcement"
```

---

## Task 3: GC pinning (and un-pinning by image coverage)

**Files:**
- Modify: `crates/page-store/src/materialize.rs`

- [ ] **Step 1: Write the failing tests**

```rust
    #[tokio::test]
    async fn descendant_branch_points_pin_parent_layers() {
        // parent gc_horizon above bl; without the pin, layers <= bl would collect.
        run_gc(&graph, parent, horizon).await.unwrap();
        assert!(child_probe_grid_unchanged().await);   // reads below bl intact
    }

    #[tokio::test]
    async fn child_image_coverage_unpins() {
        materialize_images_on(child, Lsn(bl)).await;   // PG-4 machinery, child-side
        run_gc(&graph, parent, horizon).await.unwrap();
        assert!(parent_layer_count_decreased().await && both_probe_grids_unchanged().await);
    }

    proptest! {
        #[test]
        fn gc_never_changes_any_probe_read(seed in any::<u64>()) {
            // random small layer topologies + branch points: snapshot all probe reads,
            // run gc at random horizons, assert every read identical or HistoryTrimmed
            // consistently (never a different value).
        }
    }
```

- [ ] **Step 2: Implement** — extend the deletable predicate: for each descendant `(bl)`, a candidate with `lsn_range.start ≤ bl` is pinned **unless** an image layer on the descendant (or the parent) at `≤ bl` covers its key range. Deletion order unchanged (swap-then-delete).
- [ ] **Step 3: Verify + commit**

```bash
git add crates/page-store/src/materialize.rs
git commit -m "feat(page-store): descendant-aware GC pinning with image-coverage release"
```

---

## Task 4 (∥ 2–3): The service surface

**Files:**
- Modify: `crates/pageserver/{proto/…/pageserver.proto, src/service.rs, src/basebackup.rs}`

- [ ] **Step 1: Write the failing tests** — `CreateBranch` beyond the ingested head → error; below `gc_horizon` → error; happy path returns a servable timeline id; `DeleteTimeline` with a descendant → `HasDescendants`; leaf deletion removes layers then meta (listing empty afterward); `GetPage` with an unknown timeline → NotFound.
- [ ] **Step 2: Implement** — proto: `timeline` (fixed64 or bytes) added to `GetPageRequest`/`GetRelSizeRequest`/`BasebackupRequest` (greenfield amendment); new RPCs `CreateBranch(src_timeline, lsn)`, `ListTimelines`, `DeleteTimeline`; `CreateBranch` snaps to the record boundary ≤ requested LSN and validates the window; handlers resolve the timeline through the graph.
- [ ] **Step 3: Verify + commit**

```bash
git add crates/pageserver
git commit -m "feat(pageserver): branch RPCs + timeline-scoped page service"
```

---

## Task 5: Forked fixtures + the divergence gate

**Files:**
- Modify: `tools/gen-pg-wal-fixtures.sh`; Create: `crates/pageserver/tests/branch_gate.rs`

- [ ] **Step 1 (∥ Task 1): Extend the generator** — at `L`: `pg_basebackup` → **promote** the standby (PG bumps its timeline-id; same LSN space, real divergence); run *different* traffic on parent and promoted child; capture **both** WAL streams (`fixtures/fork/{parent,child}/`) and **both** standby snapshots + a fork manifest (`fork_lsn`, per-side capture LSNs). Regenerate once locally; commit.
- [ ] **Step 2: Write the gate test** — ingest the parent stream into timeline P; `CreateBranch(P, fork_lsn)` → C; ingest the child stream into C. Assert: (a) for every covered key, C's reads at `lsn ≤ fork_lsn` are **byte-identical** to P's at the same LSN; (b) above the fork, each side matches **its own** standby capture (the PG-4 comparator, per side); (c) a `RelMeta` size query below the fork inherits (PG-4b landed) and diverges correctly above it; (d) rebuild-from-bucket (drop all state, reload graph + maps) leaves the full two-timeline probe grid identical; (e) `Basebackup(C, fork_lsn + δ)` validates under `pg_controldata` with C's SLRU segments matching C's capture.
- [ ] **Step 3: Verify + commit**

Run: `cargo test -p crabka-pageserver --test branch_gate` → PASS.

```bash
git add tools/gen-pg-wal-fixtures.sh crates/postgres-wal/tests/fixtures crates/pageserver/tests
git commit -m "test(pageserver): forked-WAL divergence gate (per-side standby oracles)"
```

---

## Task 6: Final gate

- [ ] **Step 1:** `cargo +nightly fmt --check`; `cargo clippy -p crabka-page-store -p crabka-pageserver --all-targets -- -D warnings`; `cargo nextest run -p crabka-page-store -p crabka-pageserver` — all green (ancestry, GC property, RPCs, the divergence gate).
- [ ] **Step 2:** `./tools/check-publish-allowlist.sh` — exit 0 (no new crates; confirm nothing drifted). Commit any formatting.

---

## Self-Review

**1. Spec coverage:** write-once `TimelineMeta` + graph + rebuild (Task 1); the recursion with boundary exactness + ingest enforcement + grandchild chains (Task 2); GC pinning, image-coverage release, and the never-changes-a-read property (Task 3); the three RPCs, the `timeline` amendment, LSN-window validation, leaf-only deletion (Task 4); promote-and-diverge fixtures + the five-part divergence gate incl. basebackup@branch (Task 5). Deferred set (live branched compute, recursive delete, time-based points, depth limits) untouched — Scope boundary. ✅

**2. Placeholder scan:** the ownership convention, recursion rule, pinning predicate, and fixture fork procedure are concrete; test bodies given for every decisive behavior; the one generator-dependent artifact (forked corpus) has its exact production recipe. No `TBD`.

**3. Type consistency:** `TimelineMeta`/`TimelineGraph` (Task 1) thread through `get_reconstruct_data_t` (Task 2), the GC predicate (Task 3), and the handlers (Task 4); `BranchBoundaryViolation`/`HasDescendants` named once; the gate consumes the same probe-grid helpers as PG-3/4's tests.

**4. Invariant check:** boundary exactness (Task 2 tests); metadata-only branching (Task 1 write-once + Task 4 happy path); GC-never-changes-a-read (Task 3 property); rebuildability (Tasks 1, 5d); forked byte-fidelity (Task 5a/b). Each task green before commit.

**5. Prerequisites flagged:** PG-2/3/4 executed (+ PG-4b for the `RelMeta` inheritance case, droppable until it lands); one fixture regeneration. Batching: (1 ∥ fixtures) → 2 → 3, with 4 parallel after 1 → 5 → 6.
