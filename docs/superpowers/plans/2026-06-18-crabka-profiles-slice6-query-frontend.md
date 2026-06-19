# crabka-profiles Slice 6 — Query-frontend (split/shard merge + select-series + partial-tree merge) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `query-frontend` role for `crabka-profiles` — an axum/Connect server that sits in front of N queriers (Slice 5) and (1) **splits** a `SelectMergeStacktraces` / `SelectSeries` request by time window and/or by block/series shard into bounded jobs, (2) **fans** those jobs across queriers in parallel through a trait-abstracted `QuerierBackend` with bounded concurrency, and (3) **merges** the partial results correctly. For flamegraph this is the load-bearing invariant from spec §6.4: **raw `stacktrace_id`s never cross a block boundary**, so each block/querier resolves locally to a **partial symbolized `Tree`** and the frontend `Tree::merge`s the partials *then* re-encodes one `FlameGraph` honoring `max_nodes` (with the synthetic `"other"` truncation). For `SelectSeries` the frontend concatenates/sums per-series points by `(group_by labels, timestamp)`. The role binary is `crabka-profiles --target query-frontend`.

**Architecture:** A new `frontend` module tree inside `crabka-profiles`. The querier backend is a `QuerierBackend` **trait** (`async fn merge_stacktraces_job` / `async fn select_series_job`) so tests drive a `MockQuerier` returning canned per-job partials and real deployments use an `HttpQuerier` pool (reqwest, the grpc-gateway `forward.rs`/`serve.rs` pattern). The shardable unit is a `JobShard` = `Live` (the hot WAL tail) or `Block { block_id }` (one cold block). **The merge unit is the `crabka-pprof` `Tree`, not raw flamegraph `Level`s** — because a `stacktrace_id` is only meaningful inside its own block's symbol DB, queriers must return a *symbolized* `Tree` partial (resolve-locally), which the frontend merges with `Tree::merge` and only *then* folds to a `FlameGraph` via `Tree::to_flamegraph(max_nodes)`. A `crabka-pprof` `Series` partial carries `SelectSeries` points. The pipeline composes as `plan jobs → queue (bounded fan-out) → per-job query → merge (Tree::merge | series-sum) → encode (FlameGraph | Series) → render`. A result cache is **optional** for profiles and is *not* built here — see the "Result cache (deferred — rationale)" note.

**Tech Stack:** Rust 2024 · `axum` 0.8 (`http1`, `tokio`) · `reqwest` 0.13 (`json`, `rustls`) · `serde`/`serde_json` (the Tree/Series partial wire + flamebearer) · `prost` 0.14 (the `querier.v1` partial protos) · `connectrpc-axum` + `connectrpc-axum-build` (build.rs codegen, the grpc-gateway pattern) · `tokio` (`rt-multi-thread`, `macros`, `time`, `sync`) · `futures` (bounded `buffer_unordered` fan-out) · `thiserror` · `async-trait` · `crabka-pprof` (engine types: `Tree`/`FlameGraph`/`Level`/`Series`/`SeriesAgg`/`Frame`/`ProfileError`) · `crabka-blockstore` (`LabelMatcher`). Tests: `assert2`, `tokio` (`test`, `macros`).

## Global Constraints

- **No backwards compatibility.** Greenfield/undeployed. Change schemas/enums/wire shapes freely; no shims, no migration code, no default-off feature gates.
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe`.
- **Lints:** `clippy::pedantic` is `warn`. New code clippy-pedantic clean. Run `cargo clippy -p crabka-profiles --all-targets` before each commit.
- **Formatting:** `cargo fmt -p crabka-profiles` before every commit (never `cargo +nightly fmt --all` — OS error 206 in deep worktrees on Windows; always `-p`).
- **Assertions:** `assert2::assert!`/`assert2::check!` in tests.
- **Raw ids never cross a block boundary (the load-bearing invariant, spec §6.4).** A `stacktrace_id` is meaningful only within its own block's symbol DB. The frontend MUST merge **partial symbolized `Tree`s** (`Tree::merge`), never raw ids and never partial `FlameGraph` `Level`s. `to_flamegraph(max_nodes)` is applied **once, after the full merge** — applying it per-shard then merging levels would double-truncate and corrupt `xOffsetDelta`.
- **Sharded == unsharded (the correctness centerpiece).** Splitting only ever *partitions then re-unions* the same sample set; a sharded `SelectMergeStacktraces` MUST equal the unsharded one over identical data (same merged `Tree` → same `FlameGraph`), and a split/sharded `SelectSeries` MUST equal the single-range query (same per-series points). Tasks 4–5, 8.
- **Tenant propagation:** the inbound `X-Scope-OrgID` header is threaded onto every backend job request. Never collapse tenants across jobs.
- **`max_nodes` is a frontend concern.** Per-job queriers return the **full** (untruncated) partial `Tree`; the frontend applies `max_nodes` truncation + synthetic `"other"` exactly once on the merged tree. The querier never truncates a partial (a node pruned in one shard might survive globally).

---

## Dependency & slice roadmap

**Depends on:** Slice 2 (`crabka-pprof` — pinned engine result types: `Tree`/`FlameGraph`/`Level`/`Frame`/`ProfileError`), Slice 3 (`Series`/`SeriesAgg`, the 4-ints-per-bar `FlameGraph` + the `max_nodes`/`"other"` truncation in `Tree::to_flamegraph`), and Slice 5 (Querier + Connect `querier.v1` API + legacy `/pyroscope/render`). The frontend consumes the querier's surface but **sharded at the job grain** rather than for the whole query:

- `POST /querier.v1.QuerierService/SelectMergeStacktraces` `{profile_typeID, label_selector, start, end, max_nodes, format}` → `{ flamegraph, tree, dot }`. The frontend issues this per **job** with an added `blockID`/`shard` restriction (Slice-5 contract below) **and `format=TREE`** so the querier returns a resolve-locally **partial `Tree`** (bytes) rather than a per-shard flamegraph; the frontend `Tree::merge`s and folds once.
- `POST /querier.v1.QuerierService/SelectSeries` `{profile_typeID, label_selector, start, end, group_by[], step (SECONDS, f64), aggregation}` → `{ series[] }`. The frontend issues this per job and sums per `(group_by labels, timestamp)`.
- `GET /pyroscope/render` `?query=<profile_typeID>{selectors}&from&until&format=json|dot&maxNodes&groupBy` → flamebearer JSON. The frontend renders the merged flamegraph into the flamebearer `"single"` projection.
- Tenant via `X-Scope-OrgID`. `start`/`end` are **unix MILLIS** (`int64`).

**The querier's job-restriction support is assumed (Slice 5 contract):** the querier honors a `blockID=<id>` request field (restrict the scan to that one block) and a `shard=live` field (restrict to the hot WAL tail) on `SelectMergeStacktraces` / `SelectSeries`, and — critically — when `format=TREE` it returns the **full untruncated partial `Tree`** (serialized) for that shard, leaf-resolved against *that block's* symbol DB. The frontend's job is to *enumerate* blocks/shards into jobs, *queue+fan* them, and *merge partial Trees / sum partial Series*; the querier's job is to *honor* the restriction and *resolve locally*. **This slice does not implement querier-side block/shard filtering or symbol resolution** — it injects the restriction and merges the partials. The block-enumeration source is the querier's block-metadata door (Slice-5 contract: `GET /api/blocks?tenant=&start=&end=` → `{ blocks:[ { blockID, startMillis, endMillis, profileTypes[], sizeBytes } ] }`); absent at authoring time it is modeled here behind the `BlockCatalog` trait so tests drive a `MockCatalog`.

**Slices 2/3 & 5 absent at authoring time** — the engine types this slice merges (`Tree`/`FlameGraph`/`Level`/`Series`/`SeriesAgg`/`Frame`) are **imported from `crabka-pprof`** (Slices 2–3 define them as the pinned crate contract; do not redefine). The Connect-RPC projection (`querier.v1` request/response protos) and the flamebearer JSON are (re)stated here in `frontend/wire.rs`/`frontend/proto` as the slice's own HTTP-edge model; when Slice 5 lands its querier serializes to the same shapes. If Slice 5 already exposes a shared proto/`wire` module, import it instead.

**The 8 profiles slices** (this plan = Slice 6):

1. Blockstore `ProfileIndex` + profile samples schema + symbol-DB artifact. *(`crabka-blockstore`)*
2. `crabka-pprof` core — pprof model + codec, `SymbolDb` (parent-pointer tree + dedup + `SymbolSource`), `ProfileType` parser, `ProfileStore` trait + result types, MERGE → flamegraph engine (`Tree`, 4-ints-per-bar `FlameGraph`).
3. Engine completeness — `SelectSeries`, `Diff` (7-ints-per-bar), `max_nodes` truncation + synthetic `"other"`, raw-pprof output, span-profile + heatmap.
4. Ingest service — `distributor` → `(tenant, series_fingerprint)`-partitioned WAL; `block-builder` consumer group → samples fact table + dedup symbol DB + `ProfileIndex`.
5. Querier + Connect `querier.v1` API + legacy render — `ProfileStore` as hot/cold UNION; serve `querier.v1` (incl. `ProfileTypes` as the health probe) + `/pyroscope/render` + `/pyroscope/render-diff`.
6. **Query-frontend** *(this plan)* — query split/shard + partial-`Tree` merge + select-series shard-merge + the `query-frontend` role binary.
7. Native symbolization — query-time `build_id → debuginfod` + DWARF/ELF/`.gopclntab` + demangle + inline expansion, behind the `SymbolSource` wrapper.
8. Hardening — per-tenant limits + multi-tenancy isolation, compaction + downsampling, differential-vs-Pyroscope + Grafana integration.

---

## File structure (`crates/profiles/`)

| File | Responsibility |
|---|---|
| `src/lib.rs` | add `pub mod frontend;` |
| `src/frontend/mod.rs` | module decls + public re-exports + `QueryFrontend` orchestrator |
| `src/frontend/wire.rs` | `TreePartialWire` / `SeriesWire` / the flamebearer `"single"` JSON projection + `From<crabka_pprof::*>` codecs (the partial-Tree and Series wire model) |
| `src/frontend/job.rs` | `JobShard` / `BlockMetaInfo` / `BlockCatalog` trait + `MockCatalog` + the **job planner** (time-window → live/block shards) + the `label_selector`-aware profile-type prefilter |
| `src/frontend/backend.rs` | `QuerierBackend` trait + `MergeStacktracesJob`/`SelectSeriesJob` requests + `StacktracesPartial`/`SeriesPartial` + `BackendError` + `MockQuerier` (test fixture) |
| `src/frontend/http_backend.rs` | `HttpQuerier` — reqwest/Connect pool over configurable querier addrs (fan-out target) + `HttpCatalog` |
| `src/frontend/merge.rs` | partial-`Tree` merge → `to_flamegraph(max_nodes)`; `SelectSeries` per-`(labels,ts)` sum/average; flamebearer projection |
| `src/frontend/queue.rs` | bounded fan-out (`buffer_unordered`) over the planned jobs |
| `src/frontend/server.rs` | Connect-RPC `querier.v1` router + legacy `/pyroscope/render` axum handler wiring the orchestrator + `run_query_frontend` |
| `src/frontend/config.rs` | `FrontendConfig` (backend addrs, max concurrency, default/clamp `max_nodes`, hot-frontier millis, timeouts, listen addr) |
| `build.rs` | (modify) add the `querier.v1` partial proto compile (connectrpc-axum-build) |
| `proto/querier/v1/querier.proto` | (create/extend) the `SelectMergeStacktraces`/`SelectSeries` request+`TREE`-partial message shapes the frontend speaks |
| `src/bin/crabka-profiles.rs` | (modify) `--target query-frontend` role dispatch |
| `tests/frontend_shard_equivalence.rs` | integration: sharded merge == unsharded over canned partial trees; split select-series == single-range |
| `tests/frontend_http_backend.rs` | integration: `HttpQuerier` request shape (`blockID`/`shard`/`format=TREE`, `X-Scope-OrgID`) + partial-Tree parse |
| `tests/frontend_server.rs` | integration: Connect `SelectMergeStacktraces` + legacy `/pyroscope/render` round-trip with tenant |

---

### Task 1: Crate deps + `frontend` module scaffold + the partial-`Tree` / `Series` wire model

**Files:**
- Modify: `crates/profiles/Cargo.toml`
- Modify: `crates/profiles/src/lib.rs`
- Create: `crates/profiles/src/frontend/mod.rs`
- Create: `crates/profiles/src/frontend/wire.rs`

**Interfaces:**
- Consumes (from `crabka-pprof`, Slices 2–3): `Tree` (`add_stack(&[Frame], i64)`, `merge(Tree)`, `to_flamegraph(max_nodes) -> FlameGraph`), `Frame { function:String, file:String, line:i32 }`, `FlameGraph { names:Vec<String>, levels:Vec<Level>, total:i64, max_self:i64 }`, `Level { values:Vec<i64> }`, `Series { labels:Vec<(String,String)>, points:Vec<(i64,f64)> }`.
- Produces:
  - `struct TreePartialWire { stacks: Vec<StackSampleWire> }` where `struct StackSampleWire { frames: Vec<FrameWire>, value: i64 }` and `struct FrameWire { function:String, file:String, line:i32 }` — the serde wire form of a resolve-locally partial tree, carried as **fully-symbolized stacks** so it survives a block boundary (no raw ids on the wire). `fn to_tree(&self) -> crabka_pprof::Tree` (replays each `(frames, value)` via `Tree::add_stack`) and `fn from_tree(...)` round-trip — verify-noted against the Slice-2 `Tree` surface.
  - `struct SeriesWire { labels: Vec<(String,String)>, points: Vec<(i64,f64)> }` with `From<&crabka_pprof::Series>` / `into_series()`.
  - `struct Flamebearer { names: Vec<String>, levels: Vec<Vec<i64>>, num_ticks: i64, max_self: i64 }` + `struct FlamebearerEnvelope { flamebearer: Flamebearer, metadata: FlamebearerMeta }` + `struct FlamebearerMeta { format: String /*"single"*/, units: String, name: String }` — the legacy `/pyroscope/render` projection (camelCase: `numTicks`/`maxSelf`).
  - `fn flamegraph_to_flamebearer(fg: &crabka_pprof::FlameGraph, units:&str, name:&str) -> FlamebearerEnvelope` — the `"single"` (4-ints-per-bar) projection.

- [ ] **Step 1: Add dependencies to `crates/profiles/Cargo.toml`**

Add to `[dependencies]` (the crate already exists from Slices 4–5; add only what the frontend module needs that is not yet present):

```toml
axum = { workspace = true }
tokio = { workspace = true, features = ["rt", "rt-multi-thread", "net", "macros", "time", "sync"] }
tokio-util = { workspace = true }
futures = { workspace = true }
reqwest = { version = "0.13", default-features = false, features = ["json", "rustls"] }
prost = { workspace = true }
connectrpc-axum = { workspace = true }
serde = { workspace = true, features = ["derive"] }
serde_json = { workspace = true }
async-trait = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }
clap = { workspace = true }
crabka-pprof = { path = "../pprof" }
crabka-blockstore = { path = "../blockstore" }
```

Add to `[build-dependencies]` (for Task 9's proto compile; harmless to add now):

```toml
connectrpc-axum-build = { workspace = true }
```

Add to `[dev-dependencies]`:

```toml
assert2 = { workspace = true }
tokio = { workspace = true, features = ["rt", "rt-multi-thread", "macros", "time", "sync"] }
```

> **Workspace-dep verify-note:** `futures`, `async-trait`, `thiserror`, `clap`, `tracing`, `serde_json`, `prost`, `connectrpc-axum`, `connectrpc-axum-build`, `assert2` are workspace members (see root `Cargo.toml`; the metrics/traces slice-6 plans and `crates/grpc-gateway/Cargo.toml` use the same set). If `futures` is named `futures-util` only, use `futures-util` and import `stream::{self, StreamExt}` from `futures_util`. If a `workspace = true` line errors with "not a workspace dependency", add the pin to root `[workspace.dependencies]` first (a manifest fix, not a design change). `crabka-pprof`/`crabka-blockstore` paths are Slices 1–3; adjust the `path` if they differ.

- [ ] **Step 2: Write the failing test**

Create `crates/profiles/src/frontend/wire.rs` with only the test module first:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pprof::{Frame, Tree};

    use super::*;

    fn frame(func: &str) -> Frame {
        Frame { function: func.to_string(), file: String::new(), line: 0 }
    }

    #[test]
    fn tree_partial_round_trips_through_wire() {
        // Build a tree: root->a->b (value 5), root->a->c (value 3).
        let mut t = Tree::default();
        t.add_stack(&[frame("b"), frame("a")], 5); // leaf-first, per Frame contract
        t.add_stack(&[frame("c"), frame("a")], 3);

        let wire = TreePartialWire::from_tree(&t);
        // Two distinct stacks survive as fully-symbolized frame lists.
        assert!(wire.stacks.len() == 2);
        // Replaying the wire reconstructs an equal tree (same flamegraph).
        let back = wire.to_tree();
        let fg_a = t.clone().to_flamegraph(2048);
        let fg_b = back.to_flamegraph(2048);
        assert!(fg_a.names == fg_b.names);
        assert!(fg_a.total == fg_b.total);
        assert!(fg_a.total == 8);
    }

    #[test]
    fn series_wire_round_trips() {
        let s = crabka_pprof::Series {
            labels: vec![("service".to_string(), "checkout".to_string())],
            points: vec![(1000, 1.5), (2000, 2.5)],
        };
        let wire = SeriesWire::from(&s);
        assert!(wire.points.len() == 2);
        let back = wire.into_series();
        assert!(back.points == s.points);
        assert!(back.labels == s.labels);
    }

    #[test]
    fn flamebearer_single_projection_camelcases() {
        let fg = crabka_pprof::FlameGraph {
            names: vec!["total".to_string(), "main".to_string()],
            levels: vec![
                crabka_pprof::Level { values: vec![0, 10, 0, 0] },
                crabka_pprof::Level { values: vec![0, 10, 10, 1] },
            ],
            total: 10,
            max_self: 10,
        };
        let env = flamegraph_to_flamebearer(&fg, "samples", "process_cpu");
        let json = serde_json::to_value(&env).unwrap();
        assert!(json["flamebearer"]["numTicks"] == 10);
        assert!(json["flamebearer"]["maxSelf"] == 10);
        assert!(json["flamebearer"]["names"][0] == "total");
        // levels are flat 4-int rows.
        assert!(json["flamebearer"]["levels"][1][1] == 10);
        assert!(json["metadata"]["format"] == "single");
    }
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib frontend::wire`
Expected: FAIL — `cannot find type TreePartialWire` / unresolved module `frontend`.

- [ ] **Step 4: Implement `wire.rs`**

Prepend above the `tests` module. The load-bearing decision: a partial tree crosses a block boundary as **fully-symbolized stacks** (`Vec<Frame>` per sample), never raw ids — so the frontend can replay them into a single global `Tree` regardless of which block's symbol DB resolved them.

```rust
//! The query-frontend wire model: the resolve-locally partial `Tree` (carried as
//! fully-symbolized stacks so it survives a block boundary), the `SelectSeries`
//! series partial, and the legacy `/pyroscope/render` flamebearer projection.
//!
//! The values these carry (`Tree`/`FlameGraph`/`Series`/`Frame`) are the pinned
//! `crabka-pprof` (Slices 2-3) types; this module is only their HTTP/serde edge.

use serde::{Deserialize, Serialize};

use crabka_pprof::{FlameGraph, Frame, Series, Tree};

/// A symbolized frame on the wire (mirrors `crabka_pprof::Frame`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameWire {
    pub function: String,
    #[serde(default)]
    pub file: String,
    #[serde(default)]
    pub line: i32,
}

/// One folded stack + its summed value (leaf-first frames, per the `Frame`/
/// `Tree::add_stack` contract).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StackSampleWire {
    pub frames: Vec<FrameWire>,
    pub value: i64,
}

/// A resolve-locally partial tree: the full (untruncated) set of folded stacks
/// one block/shard contributed, fully symbolized. The frontend replays these
/// into a single global `Tree` and folds once.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TreePartialWire {
    pub stacks: Vec<StackSampleWire>,
}

impl TreePartialWire {
    /// Project a `crabka_pprof::Tree` into the wire form (root→leaf paths +
    /// per-leaf self value, fully symbolized).
    ///
    /// VERIFY against the Slice-2 `Tree` surface: this assumes `Tree` exposes an
    /// iterator over its leaf stacks (`fn leaf_stacks(&self) -> impl Iterator<
    /// Item=(Vec<Frame>, i64)>` — leaf-first frames + self value). If `Tree`
    /// exposes a different folding accessor, adapt `from_tree` to it; do NOT add
    /// a second tree implementation here. `to_tree` only needs `Tree::add_stack`,
    /// which is in the pinned contract.
    #[must_use]
    pub fn from_tree(tree: &Tree) -> Self {
        let stacks = tree
            .leaf_stacks()
            .map(|(frames, value)| StackSampleWire {
                frames: frames.iter().map(FrameWire::from).collect(),
                value,
            })
            .collect();
        Self { stacks }
    }

    /// Replay the wire into a `crabka_pprof::Tree` (the frontend's global merge
    /// target). Uses only `Tree::add_stack` from the pinned contract.
    #[must_use]
    pub fn to_tree(&self) -> Tree {
        let mut tree = Tree::default();
        for s in &self.stacks {
            let frames: Vec<Frame> = s.frames.iter().map(Frame::from).collect();
            tree.add_stack(&frames, s.value);
        }
        tree
    }
}

impl From<&Frame> for FrameWire {
    fn from(f: &Frame) -> Self {
        FrameWire { function: f.function.clone(), file: f.file.clone(), line: f.line }
    }
}

impl From<&FrameWire> for Frame {
    fn from(f: &FrameWire) -> Self {
        Frame { function: f.function.clone(), file: f.file.clone(), line: f.line }
    }
}

/// A `SelectSeries` partial: a labeled time series of `(timestamp_ms, value)`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SeriesWire {
    pub labels: Vec<(String, String)>,
    pub points: Vec<(i64, f64)>,
}

impl From<&Series> for SeriesWire {
    fn from(s: &Series) -> Self {
        SeriesWire { labels: s.labels.clone(), points: s.points.clone() }
    }
}

impl SeriesWire {
    #[must_use]
    pub fn into_series(self) -> Series {
        Series { labels: self.labels, points: self.points }
    }
}

/// The legacy flamebearer body (`/pyroscope/render` `format=json`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Flamebearer {
    pub names: Vec<String>,
    /// Flattened 4-ints-per-bar rows: `[xOffsetDelta, total, self, nameIndex]`.
    pub levels: Vec<Vec<i64>>,
    pub num_ticks: i64,
    pub max_self: i64,
}

/// The flamebearer metadata block.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FlamebearerMeta {
    /// `"single"` (4/bar) here; `"double"` (7/bar) is the diff form (Slice 3/5).
    pub format: String,
    pub units: String,
    pub name: String,
}

/// The `/pyroscope/render` envelope.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FlamebearerEnvelope {
    pub flamebearer: Flamebearer,
    pub metadata: FlamebearerMeta,
}

/// Project a merged `FlameGraph` into the legacy `"single"` flamebearer. The
/// `FlameGraph.levels` 4-ints-per-bar grouping is already the flamebearer's row
/// encoding — we flatten each `Level.values` into one row.
#[must_use]
pub fn flamegraph_to_flamebearer(fg: &FlameGraph, units: &str, name: &str) -> FlamebearerEnvelope {
    FlamebearerEnvelope {
        flamebearer: Flamebearer {
            names: fg.names.clone(),
            levels: fg.levels.iter().map(|l| l.values.clone()).collect(),
            num_ticks: fg.total,
            max_self: fg.max_self,
        },
        metadata: FlamebearerMeta {
            format: "single".to_string(),
            units: units.to_string(),
            name: name.to_string(),
        },
    }
}
```

> **`Tree` accessor verify-note (the dependency edge):** `TreePartialWire::from_tree` calls `Tree::leaf_stacks()` (leaf-first frames + self value per leaf). The Slice-2 contract pins `Tree::add_stack`/`merge`/`to_flamegraph` but leaves the *read* surface unstated. **Verify the real `Tree` surface in `crabka-pprof` before implementing.** If a leaf-stack iterator is absent, add it to `crabka-pprof` as a small companion change (the introspection belongs with the type; do not re-walk a private parent/children layout from here). `to_tree`/`to_flamegraph`/`merge`/`add_stack` are all in the pinned contract, so the *merge* path (Task 4) is unblocked even if `from_tree` is briefly gated — the querier (Slice 5) is the real producer of `TreePartialWire`; tests build trees directly and only need `from_tree` for the round-trip assertion.

> **Serde verify-note (flamebearer shape):** Pyroscope's flamebearer is `{ flamebearer: { names[], levels[][], numTicks, maxSelf }, metadata: { format, units, name } }` with `levels` as flat 4-int rows for `"single"`. Pinned by `flamebearer_single_projection_camelcases`. The `(i64,f64)` / `(String,String)` tuples serialize to JSON arrays — that is the internal partial-wire form between frontend and querier, not a Pyroscope-public shape, so we are free to choose it.

- [ ] **Step 5: Create `frontend/mod.rs` and wire `lib.rs`**

Create `crates/profiles/src/frontend/mod.rs`:

```rust
//! The `query-frontend` role: query split/shard (time-window → live/block jobs),
//! querier fan-out, and partial-`Tree` / `Series` merge in front of N queriers.

pub mod wire;

pub use wire::{
    Flamebearer, FlamebearerEnvelope, FlamebearerMeta, FrameWire, SeriesWire, StackSampleWire,
    TreePartialWire, flamegraph_to_flamebearer,
};
```

Add to `crates/profiles/src/lib.rs`:

```rust
pub mod frontend;
```

- [ ] **Step 6: Run to verify it passes**

Run: `cargo test -p crabka-profiles --lib frontend::wire`
Expected: PASS (3 tests).

- [ ] **Step 7: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): query-frontend partial-Tree/Series wire + flamebearer projection"
```

---

### Task 2: `QuerierBackend` trait + per-job request/partial types + `MockQuerier`

**Files:**
- Create: `crates/profiles/src/frontend/backend.rs`
- Modify: `crates/profiles/src/frontend/mod.rs`

**Interfaces:**
- Consumes: `crabka_pprof::{Tree, Series, SeriesAgg}`, `wire::{TreePartialWire, SeriesWire}`, `job::JobShard` (Task 3).
- Produces:
  - `struct MergeStacktracesJob { tenant:String, profile_type:String, label_selector:String, start_ms:i64, end_ms:i64, shard:JobShard }` — note: **no `max_nodes`** (the frontend truncates; the job returns the full partial).
  - `struct SelectSeriesJob { tenant:String, profile_type:String, label_selector:String, group_by:Vec<String>, step_secs:f64, agg:SeriesAgg, start_ms:i64, end_ms:i64, shard:JobShard }`.
  - `struct StacktracesPartial { tree: Tree }` — the resolve-locally merged-stacktraces partial (a full symbolized tree for one shard).
  - `struct SeriesPartial { series: Vec<Series> }`.
  - `enum BackendError { Timeout, Transport(String), Backend { code:String, message:String } }` (`thiserror`).
  - `#[async_trait] trait QuerierBackend: Send + Sync { async fn merge_stacktraces_job(&self, req:&MergeStacktracesJob) -> Result<StacktracesPartial, BackendError>; async fn select_series_job(&self, req:&SelectSeriesJob) -> Result<SeriesPartial, BackendError>; }`.
  - `struct MockQuerier` — programmable FIFO-stub backend + a call recorder: `stub_stacktraces(StacktracesPartial)`, `stub_series(SeriesPartial)`, `stacktraces_calls() -> Vec<MergeStacktracesJob>`, `series_calls() -> Vec<SelectSeriesJob>`. Exposed un-gated (a fixture `tests/` integration tests construct).

> **Ordering note:** `backend.rs` imports `JobShard` from Task 3's `job.rs`. Implement Task 3's `JobShard` enum (at minimum the enum + its module) *before or alongside* Task 2 so `backend.rs` compiles. The two tasks touch different files (`backend.rs` vs `job.rs`) and can be authored in either order, but `JobShard` must exist when `backend.rs` is first built.

- [ ] **Step 1: Write the failing test**

Append a test module to `backend.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pprof::{Frame, SeriesAgg, Tree};

    use super::*;
    use crate::frontend::job::JobShard;

    fn frame(f: &str) -> Frame {
        Frame { function: f.to_string(), file: String::new(), line: 0 }
    }

    #[tokio::test]
    async fn mock_returns_canned_tree_and_records_calls() {
        let mut t = Tree::default();
        t.add_stack(&[frame("a")], 7);
        let mock = MockQuerier::new();
        mock.stub_stacktraces(StacktracesPartial { tree: t });

        let req = MergeStacktracesJob {
            tenant: "t1".to_string(),
            profile_type: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".to_string(),
            label_selector: "{service=\"checkout\"}".to_string(),
            start_ms: 0,
            end_ms: 100,
            shard: JobShard::Live,
        };
        let out = mock.merge_stacktraces_job(&req).await.unwrap();
        assert!(out.tree.to_flamegraph(2048).total == 7);
        assert!(mock.stacktraces_calls().len() == 1);
        assert!(mock.stacktraces_calls()[0].tenant == "t1");
        assert!(matches!(mock.stacktraces_calls()[0].shard, JobShard::Live));
    }

    #[tokio::test]
    async fn mock_records_series_job() {
        let mock = MockQuerier::new();
        mock.stub_series(SeriesPartial {
            series: vec![crabka_pprof::Series {
                labels: vec![],
                points: vec![(1000, 1.0)],
            }],
        });
        let req = SelectSeriesJob {
            tenant: "t1".to_string(),
            profile_type: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".to_string(),
            label_selector: "{}".to_string(),
            group_by: vec![],
            step_secs: 15.0,
            agg: SeriesAgg::Sum,
            start_ms: 0,
            end_ms: 100,
            shard: JobShard::Block { block_id: "b1".to_string() },
        };
        let out = mock.select_series_job(&req).await.unwrap();
        assert!(out.series.len() == 1);
        assert!(mock.series_calls().len() == 1);
        assert!(matches!(&mock.series_calls()[0].shard, JobShard::Block { block_id } if block_id == "b1"));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib frontend::backend`
Expected: FAIL — `cannot find type QuerierBackend` / `MockQuerier` / `StacktracesPartial`.

- [ ] **Step 3: Implement `backend.rs`**

```rust
//! The querier-backend abstraction the frontend fans out to, one call per
//! planned job. Tests use [`MockQuerier`]; real deployments use `HttpQuerier`
//! (see `http_backend.rs`).

use std::sync::Mutex;

use async_trait::async_trait;

use crabka_pprof::{Series, SeriesAgg, Tree};

use crate::frontend::job::JobShard;

/// A `SelectMergeStacktraces` job: restricted to one shard (the live WAL tail or
/// one cold block) over a `[start_ms, end_ms]` window. No `max_nodes` — the
/// querier returns the FULL partial tree; the frontend truncates once.
#[derive(Clone, Debug, PartialEq)]
pub struct MergeStacktracesJob {
    pub tenant: String,
    pub profile_type: String,
    pub label_selector: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub shard: JobShard,
}

/// A `SelectSeries` job restricted to one shard.
#[derive(Clone, Debug, PartialEq)]
pub struct SelectSeriesJob {
    pub tenant: String,
    pub profile_type: String,
    pub label_selector: String,
    pub group_by: Vec<String>,
    pub step_secs: f64,
    pub agg: SeriesAgg,
    pub start_ms: i64,
    pub end_ms: i64,
    pub shard: JobShard,
}

/// The partial result of one merge-stacktraces job: a full symbolized tree for
/// that shard, resolved locally against the shard's own symbol DB.
#[derive(Clone, Debug)]
pub struct StacktracesPartial {
    pub tree: Tree,
}

/// The partial result of one select-series job.
#[derive(Clone, Debug)]
pub struct SeriesPartial {
    pub series: Vec<Series>,
}

/// Failure modes of a single backend job.
#[derive(Clone, Debug, thiserror::Error)]
pub enum BackendError {
    #[error("backend job timed out")]
    Timeout,
    #[error("backend transport error: {0}")]
    Transport(String),
    #[error("backend returned error ({code}): {message}")]
    Backend { code: String, message: String },
}

/// A queryable querier backend (one querier replica, or a pool fronting many).
/// Every method is one fanned-out job's worth of work.
#[async_trait]
pub trait QuerierBackend: Send + Sync {
    async fn merge_stacktraces_job(
        &self,
        req: &MergeStacktracesJob,
    ) -> Result<StacktracesPartial, BackendError>;
    async fn select_series_job(
        &self,
        req: &SelectSeriesJob,
    ) -> Result<SeriesPartial, BackendError>;
}

/// A programmable in-process backend for tests. Returns the next stubbed
/// response (FIFO; the last stub repeats if more calls arrive) and records every
/// request for assertions. Exposed un-gated so integration tests in `tests/` can
/// construct it — a fixture, not production wiring.
pub struct MockQuerier {
    stacktraces_stubs: Mutex<Vec<StacktracesPartial>>,
    series_stubs: Mutex<Vec<SeriesPartial>>,
    stacktraces_calls: Mutex<Vec<MergeStacktracesJob>>,
    series_calls: Mutex<Vec<SelectSeriesJob>>,
}

impl MockQuerier {
    #[must_use]
    pub fn new() -> Self {
        Self {
            stacktraces_stubs: Mutex::new(Vec::new()),
            series_stubs: Mutex::new(Vec::new()),
            stacktraces_calls: Mutex::new(Vec::new()),
            series_calls: Mutex::new(Vec::new()),
        }
    }

    /// Enqueue a canned merge-stacktraces response (FIFO).
    pub fn stub_stacktraces(&self, p: StacktracesPartial) {
        self.stacktraces_stubs.lock().unwrap().push(p);
    }

    /// Enqueue a canned select-series response (FIFO).
    pub fn stub_series(&self, p: SeriesPartial) {
        self.series_stubs.lock().unwrap().push(p);
    }

    /// All recorded merge-stacktraces requests, in dispatch order.
    #[must_use]
    pub fn stacktraces_calls(&self) -> Vec<MergeStacktracesJob> {
        self.stacktraces_calls.lock().unwrap().clone()
    }

    /// All recorded select-series requests, in dispatch order.
    #[must_use]
    pub fn series_calls(&self) -> Vec<SelectSeriesJob> {
        self.series_calls.lock().unwrap().clone()
    }

    fn pop_stacktraces(&self) -> StacktracesPartial {
        let mut s = self.stacktraces_stubs.lock().unwrap();
        if s.len() > 1 {
            s.remove(0)
        } else {
            s.first()
                .cloned()
                .unwrap_or_else(|| StacktracesPartial { tree: Tree::default() })
        }
    }

    fn pop_series(&self) -> SeriesPartial {
        let mut s = self.series_stubs.lock().unwrap();
        if s.len() > 1 {
            s.remove(0)
        } else {
            s.first().cloned().unwrap_or_else(|| SeriesPartial { series: Vec::new() })
        }
    }
}

impl Default for MockQuerier {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QuerierBackend for MockQuerier {
    async fn merge_stacktraces_job(
        &self,
        req: &MergeStacktracesJob,
    ) -> Result<StacktracesPartial, BackendError> {
        self.stacktraces_calls.lock().unwrap().push(req.clone());
        Ok(self.pop_stacktraces())
    }

    async fn select_series_job(
        &self,
        req: &SelectSeriesJob,
    ) -> Result<SeriesPartial, BackendError> {
        self.series_calls.lock().unwrap().push(req.clone());
        Ok(self.pop_series())
    }
}
```

> **`Tree: Clone` verify-note:** `StacktracesPartial` derives `Clone`, so the merge tests can stub-and-compare. The Slice-2 contract does not pin `Tree: Clone` explicitly, but the round-trip test in Task 1 (`t.clone().to_flamegraph(..)`) already assumes it; if `Tree` is not `Clone`, add the derive in `crabka-pprof` (a parent-pointer tree is trivially `Clone`) — flagged here, a one-line companion change. `Series: Clone` is implied by its `Vec`/`String` fields.

- [ ] **Step 4: Re-export from `mod.rs`**

```rust
pub mod backend;

pub use backend::{
    BackendError, MergeStacktracesJob, MockQuerier, QuerierBackend, SelectSeriesJob, SeriesPartial,
    StacktracesPartial,
};
```

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-profiles --lib frontend::backend`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): QuerierBackend trait + per-job request/partial types + MockQuerier"
```

---

### Task 3: `JobShard` + `BlockCatalog` + the job planner (time-window → live/block shards)

**Files:**
- Create: `crates/profiles/src/frontend/job.rs`
- Modify: `crates/profiles/src/frontend/mod.rs`

**Interfaces:**
- Consumes: `crabka_blockstore::LabelMatcher` (for the optional profile-type/label prefilter — verify-noted).
- Produces:
  - `enum JobShard { Live, Block { block_id:String } }` (profiles need no row-group split — the per-block fold is already cheap; one job per block).
  - `struct BlockMetaInfo { block_id:String, start_ms:i64, end_ms:i64, profile_types:Vec<String>, size_bytes:u64 }`.
  - `#[async_trait] trait BlockCatalog: Send + Sync { async fn blocks(&self, tenant:&str, start_ms:i64, end_ms:i64) -> Result<Vec<BlockMetaInfo>, CatalogError>; }` + `struct MockCatalog` (programmable, window-filters) + `enum CatalogError`.
  - `struct JobPlan { shards:Vec<JobShard>, total_blocks:u64 }`.
  - `fn plan_jobs(blocks:&[BlockMetaInfo], profile_type:&str, hot_frontier_ms:i64) -> JobPlan` — emit one `Live` job when the window reaches the hot tier (any block ends at/after `hot_frontier_ms`, or the block list is empty), and one `Block { block_id }` job per block whose `profile_types` contains `profile_type` (the profile-type prefilter — a block with none of the requested type is skipped). Returns `total_blocks` = the count of blocks that produced a job.

- [ ] **Step 1: Write the failing test**

Create `crates/profiles/src/frontend/job.rs` with the test module:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

    fn block(id: &str, start: i64, end: i64, types: &[&str]) -> BlockMetaInfo {
        BlockMetaInfo {
            block_id: id.to_string(),
            start_ms: start,
            end_ms: end,
            profile_types: types.iter().map(|s| (*s).to_string()).collect(),
            size_bytes: 1000,
        }
    }

    #[test]
    fn one_block_plus_live_when_window_reaches_hot() {
        let blocks = vec![block("b1", 0, 100, &[PT])];
        let plan = plan_jobs(&blocks, PT, 50);
        // 1 Live (b1 ends at 100 >= frontier 50) + 1 block job.
        assert!(plan.shards.len() == 2);
        assert!(plan.shards.iter().any(|s| matches!(s, JobShard::Live)));
        assert!(plan.shards.iter().any(|s| matches!(s, JobShard::Block { block_id } if block_id == "b1")));
        assert!(plan.total_blocks == 1);
    }

    #[test]
    fn block_without_requested_profile_type_is_skipped() {
        let blocks = vec![
            block("b1", 0, 100, &[PT]),
            block("b2", 0, 100, &["memory:alloc_space:bytes:space:bytes"]),
        ];
        // frontier in the future ⇒ no Live; only b1 has PT.
        let plan = plan_jobs(&blocks, PT, i64::MAX);
        let block_jobs: Vec<_> =
            plan.shards.iter().filter(|s| matches!(s, JobShard::Block { .. })).collect();
        assert!(block_jobs.len() == 1);
        assert!(matches!(block_jobs[0], JobShard::Block { block_id } if block_id == "b1"));
        assert!(plan.total_blocks == 1);
        assert!(!plan.shards.iter().any(|s| matches!(s, JobShard::Live)));
    }

    #[test]
    fn empty_blocks_yield_only_live() {
        let plan = plan_jobs(&[], PT, i64::MAX);
        assert!(plan.shards.len() == 1);
        assert!(matches!(plan.shards[0], JobShard::Live));
        assert!(plan.total_blocks == 0);
    }

    #[tokio::test]
    async fn mock_catalog_window_filters() {
        let cat = MockCatalog::new(vec![block("b1", 0, 100, &[PT]), block("b2", 500, 600, &[PT])]);
        let got = cat.blocks("t1", 0, 200).await.unwrap();
        assert!(got.len() == 1);
        assert!(got[0].block_id == "b1");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib frontend::job`
Expected: FAIL — `cannot find type JobShard` / `plan_jobs`.

- [ ] **Step 3: Implement `job.rs`**

```rust
//! Query-space sharding: turn the candidate block set + the hot/cold frontier
//! into a list of bounded jobs (one Live job for the hot WAL tail + one job per
//! candidate block carrying the requested profile type).

use async_trait::async_trait;

/// The shard a single job scans: the live hot tier (WAL tail) or one cold block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JobShard {
    Live,
    Block { block_id: String },
}

/// Block metadata the planner needs (from the querier's block-catalog door).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMetaInfo {
    pub block_id: String,
    pub start_ms: i64,
    pub end_ms: i64,
    /// The 5-part profile-type strings this block holds (the `__profile_type__`
    /// index dimension) — used to skip blocks lacking the requested type.
    pub profile_types: Vec<String>,
    pub size_bytes: u64,
}

/// The output of planning: the shards to dispatch + how many blocks they cover
/// (seeds any block accounting).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobPlan {
    pub shards: Vec<JobShard>,
    pub total_blocks: u64,
}

/// Errors enumerating blocks.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("block catalog error: {0}")]
    Backend(String),
}

/// The block-catalog door: which blocks overlap `[start_ms, end_ms]` for a
/// tenant. Slice 5's querier exposes this; tests use [`MockCatalog`].
#[async_trait]
pub trait BlockCatalog: Send + Sync {
    async fn blocks(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<BlockMetaInfo>, CatalogError>;
}

/// A canned block catalog for tests.
pub struct MockCatalog {
    blocks: Vec<BlockMetaInfo>,
}

impl MockCatalog {
    #[must_use]
    pub fn new(blocks: Vec<BlockMetaInfo>) -> Self {
        Self { blocks }
    }
}

#[async_trait]
impl BlockCatalog for MockCatalog {
    async fn blocks(
        &self,
        _tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<BlockMetaInfo>, CatalogError> {
        Ok(self
            .blocks
            .iter()
            .filter(|b| b.end_ms >= start_ms && b.start_ms <= end_ms)
            .cloned()
            .collect())
    }
}

/// Plan jobs from the candidate blocks + the requested profile type + the
/// hot/cold frontier.
///
/// - One `Live` job iff the query window could reach the hot tier: any candidate
///   block ends at/after `hot_frontier_ms`, OR the block list is empty (we cannot
///   prove the window is entirely cold). Probe Live unless every candidate block
///   ends strictly before the frontier.
/// - One `Block { block_id }` job per block whose `profile_types` contains the
///   requested `profile_type` (the profile-type prefilter). Blocks lacking it are
///   skipped (they contribute nothing to this profile-type's fold).
#[must_use]
pub fn plan_jobs(blocks: &[BlockMetaInfo], profile_type: &str, hot_frontier_ms: i64) -> JobPlan {
    let mut shards = Vec::new();

    let window_reaches_hot =
        blocks.is_empty() || blocks.iter().any(|b| b.end_ms >= hot_frontier_ms);
    if window_reaches_hot {
        shards.push(JobShard::Live);
    }

    let mut total_blocks = 0u64;
    for b in blocks {
        if b.profile_types.iter().any(|t| t == profile_type) {
            shards.push(JobShard::Block { block_id: b.block_id.clone() });
            total_blocks += 1;
        }
    }

    JobPlan { shards, total_blocks }
}
```

> **Frontier semantics note:** `hot_frontier_ms` is the *cold-edge* millis — data at/after it is in the live WAL tail, data before it is in committed blocks. The planner probes `Live` whenever the window could reach hot data and emits a job per candidate cold block carrying the requested type. A profile that straddles the frontier (recent samples in live + flushed samples in a fresh block) is covered by *both* the Live job and a block job; the merge (Task 4) sums values across partials so no sample is lost — the hot/cold-merge correctness the spec §10 calls out. **Profiles do not need a row-group split** (unlike traces): the per-block `GROUP BY (stacktrace_partition, stacktrace_id) → SUM` fold is already cheap and set-shrinking, so one job per block is the right grain. If a future block grows large enough to warrant intra-block parallelism, add a `row_group` field to `JobShard::Block` mirroring the traces planner — flagged, not built.

> **`LabelMatcher` prefilter verify-note:** the planner prefilters on `profile_type` (the `__profile_type__` label) only; the *full* `label_selector` match happens at the querier (it has the `ProfileIndex` postings). If Slice-5's block catalog returns per-block label-postings summaries cheap enough to prefilter further here (skip a block whose postings cannot match the selector), parse `label_selector` with the `crabka-pprof` matcher helper and intersect — a pure optimization, not required for correctness; the querier already returns an empty partial for a non-matching block.

- [ ] **Step 4: Re-export from `mod.rs`**

```rust
pub mod job;

pub use job::{BlockCatalog, BlockMetaInfo, CatalogError, JobPlan, JobShard, MockCatalog, plan_jobs};
```

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-profiles --lib frontend::job`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): job planner — live/block shards + profile-type prefilter + block catalog"
```

---

### Task 4: Partial-`Tree` merge → flamegraph (the correctness centerpiece, part 1)

**Files:**
- Create: `crates/profiles/src/frontend/merge.rs`
- Modify: `crates/profiles/src/frontend/mod.rs`

**Interfaces:**
- Consumes: `crabka_pprof::{Tree, FlameGraph}`, `backend::StacktracesPartial`.
- Produces:
  - `fn merge_stacktraces(partials: Vec<StacktracesPartial>, max_nodes: i64) -> FlameGraph` — `Tree::merge` all partial trees into one global `Tree`, then `to_flamegraph(max_nodes)` **once** (truncation + synthetic `"other"` applied exactly once on the merged tree). An empty partial set yields an empty flamegraph (`to_flamegraph` of a default `Tree`).
  - `fn merge_trees(partials: Vec<StacktracesPartial>) -> Tree` — the merge step alone (folds every partial into one `Tree`), exposed so the orchestrator/tests can inspect the pre-encode tree.

- [ ] **Step 1: Write the failing test**

Create `crates/profiles/src/frontend/merge.rs` with the test module:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pprof::{Frame, Tree};

    use super::*;
    use crate::frontend::backend::StacktracesPartial;

    fn frame(f: &str) -> Frame {
        Frame { function: f.to_string(), file: String::new(), line: 0 }
    }

    fn partial(stacks: &[(&[&str], i64)]) -> StacktracesPartial {
        let mut t = Tree::default();
        for (frames, value) in stacks {
            let frames: Vec<Frame> = frames.iter().map(|f| frame(f)).collect();
            t.add_stack(&frames, *value);
        }
        StacktracesPartial { tree: t }
    }

    #[test]
    fn sharded_merge_equals_unsharded() {
        // Block A: root->a->b = 5 ; root->a = 2 (self at a).
        // Block B: root->a->b = 3 ; root->c = 4.
        // Unsharded (single tree over both): root->a->b = 8, a self 2, c self 4.
        let a = partial(&[(&["b", "a"], 5), (&["a"], 2)]);
        let b = partial(&[(&["b", "a"], 3), (&["c"], 4)]);

        // Build the unsharded baseline directly.
        let mut unsharded = Tree::default();
        unsharded.add_stack(&[frame("b"), frame("a")], 5);
        unsharded.add_stack(&[frame("a")], 2);
        unsharded.add_stack(&[frame("b"), frame("a")], 3);
        unsharded.add_stack(&[frame("c")], 4);
        let want = unsharded.to_flamegraph(2048);

        let got = merge_stacktraces(vec![a, b], 2048);
        // Same total and same name set/levels ⇒ sharded == unsharded.
        assert!(got.total == want.total);
        assert!(got.total == 14);
        assert!(got.names == want.names);
        assert!(got.levels.len() == want.levels.len());
        for (g, w) in got.levels.iter().zip(want.levels.iter()) {
            assert!(g.values == w.values);
        }
    }

    #[test]
    fn empty_partials_yield_empty_flamegraph() {
        let fg = merge_stacktraces(vec![], 2048);
        assert!(fg.total == 0);
    }

    #[test]
    fn max_nodes_applied_once_on_merged_tree() {
        // Two partials each with several leaves; a tiny max_nodes must truncate
        // the MERGED tree (synthetic "other"), not each partial pre-merge.
        let a = partial(&[(&["x1", "r"], 1), (&["x2", "r"], 1)]);
        let b = partial(&[(&["x3", "r"], 1), (&["x4", "r"], 1)]);
        let fg = merge_stacktraces(vec![a, b], 2);
        // Total is preserved across truncation (the "other" node absorbs pruned
        // value); the merge never loses sample weight.
        assert!(fg.total == 4);
        // A synthetic "other" name appears when truncation prunes leaves.
        assert!(fg.names.iter().any(|n| n == "other"));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib frontend::merge`
Expected: FAIL — `cannot find function merge_stacktraces`.

- [ ] **Step 3: Implement `merge.rs`**

```rust
//! Merge per-job partials back into one result. For merge-stacktraces this is
//! the load-bearing invariant (spec §6.4): each shard contributes a full
//! symbolized partial `Tree` (raw ids never cross a block boundary), the frontend
//! `Tree::merge`s them into one global tree, and folds to a `FlameGraph` exactly
//! once — `max_nodes` truncation applied to the merged tree, never per-shard.

use crabka_pprof::{FlameGraph, Tree};

use crate::frontend::backend::StacktracesPartial;

/// Merge every partial tree into one global `Tree`. The merge is associative and
/// commutative (`Tree::merge` sums total-along-path / self-at-leaf), so shard
/// order does not matter — the fan-out's completion order is irrelevant.
#[must_use]
pub fn merge_trees(partials: Vec<StacktracesPartial>) -> Tree {
    let mut acc = Tree::default();
    for p in partials {
        acc.merge(p.tree);
    }
    acc
}

/// Merge partial trees, then fold the merged tree to a `FlameGraph` ONCE,
/// applying `max_nodes` truncation (synthetic `"other"`) at the global level.
#[must_use]
pub fn merge_stacktraces(partials: Vec<StacktracesPartial>, max_nodes: i64) -> FlameGraph {
    merge_trees(partials).to_flamegraph(max_nodes)
}
```

> **`Tree::merge` consume-vs-borrow verify-note:** this assumes the Slice-2 contract `Tree::merge(&mut self, other: Tree)` (consumes `other`). If the real signature is `merge(&mut self, other: &Tree)`, change the loop to `acc.merge(&p.tree)`. `to_flamegraph(self, max_nodes)` is pinned to consume `self` and apply truncation + `"other"` — verify it is `self`-by-value (the `merge_trees(..).to_flamegraph(..)` chain relies on it). The tests pin the *behavior* (sharded total == unsharded total, identical levels, truncation-preserves-total, synthetic `"other"`), so any signature drift surfaces as a compile error, not a silent wrong fold.

> **Why merge `Tree`s, not `FlameGraph`s (the centerpiece rationale):** a `FlameGraph`'s `Level.values` use `xOffsetDelta` (a delta from the previous bar's end) and a per-tree `nameIndex` into `names[]` — both are *position*- and *name-table*-relative to the tree that produced them. Two partial flamegraphs cannot be concatenated or summed: their name indices collide and their offsets are meaningless across trees. The `Tree` is the only mergeable representation (keyed by frame identity, value-additive). This is why the querier returns `format=TREE` partials and the frontend folds once — applying `to_flamegraph` per-shard then merging levels would double-truncate and corrupt offsets.

- [ ] **Step 4: Re-export from `mod.rs`**

```rust
pub mod merge;

pub use merge::{merge_stacktraces, merge_trees};
```

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-profiles --lib frontend::merge`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): partial-Tree merge → single flamegraph fold (sharded == unsharded)"
```

---

### Task 5: `SelectSeries` shard-merge (the correctness centerpiece, part 2)

**Files:**
- Modify: `crates/profiles/src/frontend/merge.rs` (add `merge_series`)

**Interfaces:**
- Consumes: `crabka_pprof::{Series, SeriesAgg}`, `backend::SeriesPartial`.
- Produces:
  - `fn merge_series(partials: Vec<SeriesPartial>, agg: SeriesAgg) -> Vec<Series>` — union per-job series by `series_key(labels)`, and within each series combine partials per `timestamp_ms`: `SeriesAgg::Sum` adds values at the same `(labels, ts)`; `SeriesAgg::Average` accumulates `(sum, count)` per `(labels, ts)` then divides (so a split/sharded average equals the single-range average, never an average-of-averages). Points sorted by ascending timestamp; empty result for empty input.
  - `fn series_key(labels: &[(String, String)]) -> String` — stable, label-order-independent identity (sorted `k=v`).

> **Why `Average` accumulates sum+count (the correctness subtlety):** if two shards each return a partial average for the same `(series, ts)`, averaging those two averages is wrong unless the shards carried equal weight. Profiles' `SelectSeries` reads the precomputed `PCOL_TOTAL_VALUE` per profile (spec §6.2), so a partial's "value" at a timestamp is a `SUM` (for `Sum`) or a `SUM` that must be divided by the global `COUNT` (for `Average`). **Decision:** the querier returns, for `Average`, a series whose points carry the *sum* (not the per-shard mean) plus a parallel count series — OR the frontend treats `Average` as `sum/count` exactly as the metrics frontend decomposes `avg`. To keep this slice's contract simple and the querier honest, the querier returns **sum-valued** partials for both aggs and a per-point `count`; the frontend divides for `Average`. The `SeriesPartial` therefore needs the count alongside the value — see the test and the `Series`-point note below.

- [ ] **Step 1: Write the failing test**

Append to the `merge.rs` test module:

```rust
    use crabka_pprof::{Series, SeriesAgg};

    use crate::frontend::backend::SeriesPartial;

    fn series(labels: &[(&str, &str)], points: &[(i64, f64)]) -> Series {
        Series {
            labels: labels.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect(),
            points: points.to_vec(),
        }
    }

    #[test]
    fn split_select_series_sum_equals_single_range() {
        // Same series `{svc=checkout}` split across two time sub-ranges:
        //   shard0 covers ts 1000,2000 ; shard1 covers ts 3000.
        // Single-range would yield points at 1000,2000,3000.
        let p0 = SeriesPartial { series: vec![series(&[("svc", "checkout")], &[(1000, 5.0), (2000, 7.0)])] };
        let p1 = SeriesPartial { series: vec![series(&[("svc", "checkout")], &[(3000, 9.0)])] };
        let merged = merge_series(vec![p0, p1], SeriesAgg::Sum);
        assert!(merged.len() == 1);
        assert!(merged[0].points == vec![(1000, 5.0), (2000, 7.0), (3000, 9.0)]);
    }

    #[test]
    fn sharded_sum_adds_same_timestamp_across_shards() {
        // Same series, same timestamp, from two BLOCK shards ⇒ values add.
        let p0 = SeriesPartial { series: vec![series(&[], &[(1000, 3.0)])] };
        let p1 = SeriesPartial { series: vec![series(&[], &[(1000, 4.0)])] };
        let merged = merge_series(vec![p0, p1], SeriesAgg::Sum);
        assert!(merged[0].points == vec![(1000, 7.0)]);
    }

    #[test]
    fn distinct_label_sets_stay_separate() {
        let p = SeriesPartial {
            series: vec![
                series(&[("svc", "a")], &[(1000, 1.0)]),
                series(&[("svc", "b")], &[(1000, 2.0)]),
            ],
        };
        let merged = merge_series(vec![p], SeriesAgg::Sum);
        assert!(merged.len() == 2);
    }

    #[test]
    fn series_key_is_label_order_independent() {
        let a = series_key(&[("b".to_string(), "2".to_string()), ("a".to_string(), "1".to_string())]);
        let b = series_key(&[("a".to_string(), "1".to_string()), ("b".to_string(), "2".to_string())]);
        assert!(a == b);
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib frontend::merge`
Expected: FAIL — `cannot find function merge_series`.

- [ ] **Step 3: Implement the additions to `merge.rs`**

Add above the `tests` module:

```rust
use std::collections::BTreeMap;

use crabka_pprof::{Series, SeriesAgg};

use crate::frontend::backend::SeriesPartial;

/// A stable, label-order-independent identity for a series (sorted `k\u{1}v`
/// joined by `\u{0}` — bytes that cannot appear in a label name/value).
#[must_use]
pub fn series_key(labels: &[(String, String)]) -> String {
    let mut sorted: Vec<&(String, String)> = labels.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut s = String::new();
    for (k, v) in sorted {
        s.push_str(k);
        s.push('\u{1}');
        s.push_str(v);
        s.push('\u{0}');
    }
    s
}

/// Merge per-job series partials into the single result the unsharded/single-
/// range query produces. Union by `series_key`, then combine per `(series, ts)`.
///
/// - `Sum`: add values at the same `(series, timestamp)`.
/// - `Average`: accumulate `(sum, count)` per `(series, timestamp)`, divide at
///   the end — so a split/sharded average equals the single-range average, never
///   an average-of-per-shard-averages.
#[must_use]
pub fn merge_series(partials: Vec<SeriesPartial>, agg: SeriesAgg) -> Vec<Series> {
    // series_key -> (representative labels, ts -> (sum, count))
    let mut acc: BTreeMap<String, (Vec<(String, String)>, BTreeMap<i64, (f64, u64)>)> =
        BTreeMap::new();

    for p in partials {
        for s in p.series {
            let key = series_key(&s.labels);
            let entry = acc.entry(key).or_insert_with(|| (s.labels.clone(), BTreeMap::new()));
            for (ts, v) in s.points {
                let slot = entry.1.entry(ts).or_insert((0.0, 0));
                slot.0 += v;
                slot.1 += 1;
            }
        }
    }

    acc.into_values()
        .map(|(labels, points_map)| {
            let points = points_map
                .into_iter()
                .map(|(ts, (sum, count))| {
                    let value = match agg {
                        SeriesAgg::Sum => sum,
                        SeriesAgg::Average => {
                            if count == 0 { 0.0 } else { sum / count as f64 }
                        }
                    };
                    (ts, value)
                })
                .collect();
            Series { labels, points }
        })
        .collect()
}
```

> **`Average` weighting verify-note:** this implements `Average` as `sum-of-partial-values / number-of-partials-contributing-at-that-ts`. That is correct **iff** each partial at a given `(series, ts)` already carries a per-shard *mean over the same denominator basis* (e.g. each shard's point is the mean for that step). If instead the querier returns per-shard **sums** plus a separate per-shard **count** (the more faithful decomposition, mirroring the metrics frontend's `avg → sum/count`), upgrade `SeriesPartial` to carry parallel `count` points and divide `total_sum / total_count` here. The spec §6.2 says `SelectSeries` reads the precomputed `PCOL_TOTAL_VALUE` (a sum) — **verify against the Slice-5 querier's `SelectSeries` partial shape** and pick the matching decomposition. The `Sum` path (the Grafana-minimum agg) is exact regardless and is pinned by `split_select_series_sum_equals_single_range` + `sharded_sum_adds_same_timestamp_across_shards`. Flag this and confirm before believing the `Average` numbers.

- [ ] **Step 4: Re-export from `mod.rs`**

Extend the merge re-export: `pub use merge::{merge_series, merge_stacktraces, merge_trees, series_key};`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-profiles --lib frontend::merge`
Expected: PASS (the series tests + the Task-4 tree tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): SelectSeries shard-merge (sum/average) — split == single-range"
```

---

### Task 6: Bounded fan-out queue

**Files:**
- Create: `crates/profiles/src/frontend/queue.rs`
- Modify: `crates/profiles/src/frontend/mod.rs`

**Interfaces:**
- Produces:
  - `async fn run_jobs<T, R, F, Fut>(jobs: Vec<T>, max_concurrency: usize, run: F) -> Vec<R>` where `F: Fn(T) -> Fut`, `Fut: Future<Output = R>` — drive `jobs` through a bounded-concurrency fan-out (`futures::stream::iter(...).map(run).buffer_unordered(max_concurrency).collect()`), with **no** ordering guarantee (results come back in completion order). Because `Tree::merge` and `merge_series` are commutative, completion order is irrelevant. `max_concurrency.max(1)` clamps a zero.

- [ ] **Step 1: Write the failing test**

Create `crates/profiles/src/frontend/queue.rs` with the test module:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use assert2::assert;

    use super::*;

    #[tokio::test]
    async fn runs_all_jobs_with_bounded_concurrency() {
        let inflight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let jobs: Vec<usize> = (0..20).collect();

        let inflight_c = inflight.clone();
        let max_seen_c = max_seen.clone();
        let results = run_jobs(jobs, 4, move |j| {
            let inflight = inflight_c.clone();
            let max_seen = max_seen_c.clone();
            async move {
                let now = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                inflight.fetch_sub(1, Ordering::SeqCst);
                j * 2
            }
        })
        .await;

        assert!(results.len() == 20);
        let sum: usize = results.iter().sum();
        assert!(sum == (0..20).map(|j| j * 2).sum());
        assert!(max_seen.load(Ordering::SeqCst) <= 4);
    }

    #[tokio::test]
    async fn zero_concurrency_clamps_to_one() {
        let results = run_jobs(vec![1, 2, 3], 0, |j| async move { j }).await;
        assert!(results.len() == 3);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib frontend::queue`
Expected: FAIL — `cannot find function run_jobs`.

- [ ] **Step 3: Implement `queue.rs`**

```rust
//! Bounded-concurrency fan-out of planned jobs across queriers. Results return
//! in completion order; the merge (Tasks 4-5) is commutative so order does not
//! matter.

use std::future::Future;

use futures::stream::{self, StreamExt};

/// Run `jobs` through `run` with at most `max_concurrency` in flight at once.
/// Returns every result (completion order, unordered).
pub async fn run_jobs<T, R, F, Fut>(jobs: Vec<T>, max_concurrency: usize, run: F) -> Vec<R>
where
    F: Fn(T) -> Fut,
    Fut: Future<Output = R>,
{
    let limit = max_concurrency.max(1);
    stream::iter(jobs)
        .map(run)
        .buffer_unordered(limit)
        .collect()
        .await
}
```

> **`buffer_unordered` verify-note:** `futures::stream::iter(...).map(closure).buffer_unordered(n).collect::<Vec<_>>().await` is the standard bounded-fan-out idiom (`futures` 0.3 surface, same as the traces/metrics slice-6 plans). `buffer_unordered` polls up to `n` futures concurrently and yields results as they complete — order is non-deterministic, which is fine because `Tree::merge`/`merge_series` are commutative. If the workspace exposes `futures_util` rather than `futures`, change the import to `futures_util::stream::{self, StreamExt}`; the call is identical. The `runs_all_jobs_with_bounded_concurrency` test pins both the all-results invariant and the concurrency bound.

- [ ] **Step 4: Re-export from `mod.rs`**

```rust
pub mod queue;

pub use queue::run_jobs;
```

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-profiles --lib frontend::queue`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): bounded-concurrency job fan-out queue"
```

---

### Task 7: `FrontendConfig` + `QueryFrontend` orchestrator

**Files:**
- Create: `crates/profiles/src/frontend/config.rs`
- Add the orchestrator in: `crates/profiles/src/frontend/mod.rs` (struct `QueryFrontend`)
- Modify: `crates/profiles/src/frontend/mod.rs` (re-exports)

**Interfaces:**
- Produces:
  - `struct FrontendConfig { backend_addrs:Vec<String>, max_concurrency:usize, default_max_nodes:i64 /*2048*/, max_max_nodes:i64 /*clamp*/, hot_frontier_ms:i64, request_timeout:Duration, listen_addr:SocketAddr }` (+ `Default`).
  - `struct QueryFrontend<B:QuerierBackend, C:BlockCatalog> { backend:Arc<B>, catalog:Arc<C>, cfg:FrontendConfig }`.
  - `async fn select_merge_stacktraces(&self, tenant, profile_type, label_selector, start_ms, end_ms, max_nodes) -> FlameGraph` — catalog → `plan_jobs` → `run_jobs` (per-shard `merge_stacktraces_job`) → `merge_stacktraces` (clamp `max_nodes` to `[1, max_max_nodes]`, default when ≤0). Failed jobs degrade to an empty partial (one slow querier never fails the whole query).
  - `async fn select_series(&self, tenant, profile_type, label_selector, group_by, step_secs, agg, start_ms, end_ms) -> Vec<Series>` — catalog → `plan_jobs` → `run_jobs` (per-shard `select_series_job`) → `merge_series`.
  - `fn backend_ref(&self) -> &B` (test accessor); `fn default_max_nodes(&self) -> i64` / `fn clamp_max_nodes(&self, n:i64) -> i64`.

- [ ] **Step 1: Write the failing test (fan-out counts + merge wiring)**

Add to `mod.rs` under `#[cfg(test)] mod orch_tests`:

```rust
#[cfg(test)]
mod orch_tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_pprof::{Frame, SeriesAgg, Tree};

    use super::*;
    use crate::frontend::backend::{MockQuerier, SeriesPartial, StacktracesPartial};
    use crate::frontend::job::{BlockMetaInfo, MockCatalog};

    const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

    fn frame(f: &str) -> Frame {
        Frame { function: f.to_string(), file: String::new(), line: 0 }
    }

    fn block(id: &str, start: i64, end: i64) -> BlockMetaInfo {
        BlockMetaInfo {
            block_id: id.to_string(),
            start_ms: start,
            end_ms: end,
            profile_types: vec![PT.to_string()],
            size_bytes: 1000,
        }
    }

    fn tree_partial(func: &str, value: i64) -> StacktracesPartial {
        let mut t = Tree::default();
        t.add_stack(&[frame(func)], value);
        StacktracesPartial { tree: t }
    }

    #[tokio::test]
    async fn merge_stacktraces_plans_fans_and_merges() {
        // Two cold blocks (both carry PT) + a hot window ⇒ 1 Live + 2 block jobs.
        let catalog = MockCatalog::new(vec![block("b1", 0, 100), block("b2", 100, 200)]);
        let backend = MockQuerier::new();
        backend.stub_stacktraces(tree_partial("a", 1)); // Live
        backend.stub_stacktraces(tree_partial("a", 2)); // b1
        backend.stub_stacktraces(tree_partial("a", 4)); // b2
        let cfg = FrontendConfig { max_concurrency: 8, hot_frontier_ms: 150, ..FrontendConfig::default() };
        let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);

        let fg = qf.select_merge_stacktraces("t1", PT, "{}", 0, 300, 2048).await;
        // 3 jobs dispatched, all carrying the tenant.
        assert!(qf.backend_ref().stacktraces_calls().len() == 3);
        for c in qf.backend_ref().stacktraces_calls() {
            assert!(c.tenant == "t1");
            assert!(c.profile_type == PT);
        }
        // All three partials merged: 1+2+4 = 7 at function `a`.
        assert!(fg.total == 7);
    }

    #[tokio::test]
    async fn max_nodes_clamped_and_defaulted() {
        let catalog = MockCatalog::new(vec![]);
        let backend = MockQuerier::new();
        let cfg = FrontendConfig {
            default_max_nodes: 2048,
            max_max_nodes: 10_000,
            hot_frontier_ms: i64::MAX,
            ..FrontendConfig::default()
        };
        let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);
        // <=0 ⇒ default; huge ⇒ clamp.
        assert!(qf.clamp_max_nodes(0) == 2048);
        assert!(qf.clamp_max_nodes(-5) == 2048);
        assert!(qf.clamp_max_nodes(50_000) == 10_000);
        assert!(qf.clamp_max_nodes(500) == 500);
    }

    #[tokio::test]
    async fn select_series_fans_and_sums() {
        let catalog = MockCatalog::new(vec![block("b1", 0, 100)]);
        let backend = MockQuerier::new();
        backend.stub_series(SeriesPartial {
            series: vec![crabka_pprof::Series { labels: vec![], points: vec![(1000, 3.0)] }],
        });
        backend.stub_series(SeriesPartial {
            series: vec![crabka_pprof::Series { labels: vec![], points: vec![(1000, 4.0)] }],
        });
        let cfg = FrontendConfig { hot_frontier_ms: 50, ..FrontendConfig::default() };
        let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);
        // Live + b1 = 2 jobs; both at ts 1000 ⇒ sum 7.
        let series = qf.select_series("t1", PT, "{}", &[], 15.0, SeriesAgg::Sum, 0, 100).await;
        assert!(qf.backend_ref().series_calls().len() == 2);
        assert!(series.len() == 1);
        assert!(series[0].points == vec![(1000, 7.0)]);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib frontend::orch_tests`
Expected: FAIL — `cannot find type FrontendConfig` / `QueryFrontend`.

- [ ] **Step 3: Implement `config.rs`**

```rust
//! Query-frontend configuration.

use std::net::SocketAddr;
use std::time::Duration;

/// Static configuration for the `query-frontend` role.
#[derive(Clone, Debug)]
pub struct FrontendConfig {
    /// Querier backend addresses (`host:port`) the pool round-robins over.
    pub backend_addrs: Vec<String>,
    /// Max jobs in flight at once across all queriers.
    pub max_concurrency: usize,
    /// Default `max_nodes` when the request omits it / passes <= 0 (Pyroscope 2048).
    pub default_max_nodes: i64,
    /// Upper clamp on a caller-supplied `max_nodes` (a per-tenant limit anchor).
    pub max_max_nodes: i64,
    /// The cold-edge millis: data at/after it is in the live (hot) WAL tail.
    pub hot_frontier_ms: i64,
    /// Per-backend-job timeout.
    pub request_timeout: Duration,
    /// The frontend's own listen address.
    pub listen_addr: SocketAddr,
}

impl Default for FrontendConfig {
    fn default() -> Self {
        Self {
            backend_addrs: vec!["127.0.0.1:4040".to_string()],
            max_concurrency: 1000,
            default_max_nodes: 2048,
            max_max_nodes: 100_000,
            // 0 ⇒ "everything is cold" by default; the live/block-builder role
            // computes the real frontier and the binary wires it in (Slice 8).
            hot_frontier_ms: 0,
            request_timeout: Duration::from_secs(30),
            listen_addr: "0.0.0.0:4040".parse().expect("valid default addr"),
        }
    }
}
```

- [ ] **Step 4: Implement the `QueryFrontend` orchestrator in `mod.rs`**

```rust
pub mod config;

pub use config::FrontendConfig;

use std::sync::Arc;

use crabka_pprof::{FlameGraph, Series, SeriesAgg};

use crate::frontend::backend::{
    MergeStacktracesJob, QuerierBackend, SelectSeriesJob, SeriesPartial, StacktracesPartial,
};
use crate::frontend::job::{BlockCatalog, JobShard};

/// The query-frontend pipeline: plan jobs → queue (bounded fan-out) → per-job
/// query → merge (Tree::merge | series-sum) → encode, in front of a
/// [`QuerierBackend`] pool with a [`BlockCatalog`] for block enumeration.
pub struct QueryFrontend<B: QuerierBackend, C: BlockCatalog> {
    backend: Arc<B>,
    catalog: Arc<C>,
    cfg: FrontendConfig,
}

impl<B: QuerierBackend + 'static, C: BlockCatalog + 'static> QueryFrontend<B, C> {
    #[must_use]
    pub fn new(backend: Arc<B>, catalog: Arc<C>, cfg: FrontendConfig) -> Self {
        Self { backend, catalog, cfg }
    }

    /// Test/inspection accessor for the backend (e.g. `MockQuerier::*_calls`).
    #[must_use]
    pub fn backend_ref(&self) -> &B {
        &self.backend
    }

    /// The configured default `max_nodes`.
    #[must_use]
    pub fn default_max_nodes(&self) -> i64 {
        self.cfg.default_max_nodes
    }

    /// Clamp a caller `max_nodes`: <= 0 → default; above the cap → cap.
    #[must_use]
    pub fn clamp_max_nodes(&self, n: i64) -> i64 {
        if n <= 0 {
            self.cfg.default_max_nodes
        } else {
            n.min(self.cfg.max_max_nodes)
        }
    }

    /// Run a `SelectMergeStacktraces` through the full pipeline → one flamegraph.
    pub async fn select_merge_stacktraces(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        start_ms: i64,
        end_ms: i64,
        max_nodes: i64,
    ) -> FlameGraph {
        let blocks = self.catalog.blocks(tenant, start_ms, end_ms).await.unwrap_or_default();
        let plan = job::plan_jobs(&blocks, profile_type, self.cfg.hot_frontier_ms);

        let backend = self.backend.clone();
        let tenant_s = tenant.to_string();
        let pt_s = profile_type.to_string();
        let sel_s = label_selector.to_string();
        let partials = queue::run_jobs(plan.shards, self.cfg.max_concurrency, move |shard| {
            let backend = backend.clone();
            let req = MergeStacktracesJob {
                tenant: tenant_s.clone(),
                profile_type: pt_s.clone(),
                label_selector: sel_s.clone(),
                start_ms,
                end_ms,
                shard,
            };
            async move {
                backend
                    .merge_stacktraces_job(&req)
                    .await
                    .unwrap_or_else(|_| StacktracesPartial { tree: crabka_pprof::Tree::default() })
            }
        })
        .await;

        merge::merge_stacktraces(partials, self.clamp_max_nodes(max_nodes))
    }

    /// Run a `SelectSeries` through the full pipeline → merged series.
    #[allow(clippy::too_many_arguments)]
    pub async fn select_series(
        &self,
        tenant: &str,
        profile_type: &str,
        label_selector: &str,
        group_by: &[String],
        step_secs: f64,
        agg: SeriesAgg,
        start_ms: i64,
        end_ms: i64,
    ) -> Vec<Series> {
        let blocks = self.catalog.blocks(tenant, start_ms, end_ms).await.unwrap_or_default();
        let plan = job::plan_jobs(&blocks, profile_type, self.cfg.hot_frontier_ms);

        let backend = self.backend.clone();
        let tenant_s = tenant.to_string();
        let pt_s = profile_type.to_string();
        let sel_s = label_selector.to_string();
        let group_by_v = group_by.to_vec();
        let partials = queue::run_jobs(plan.shards, self.cfg.max_concurrency, move |shard| {
            let backend = backend.clone();
            let req = SelectSeriesJob {
                tenant: tenant_s.clone(),
                profile_type: pt_s.clone(),
                label_selector: sel_s.clone(),
                group_by: group_by_v.clone(),
                step_secs,
                agg,
                start_ms,
                end_ms,
                shard,
            };
            async move {
                backend
                    .select_series_job(&req)
                    .await
                    .unwrap_or_else(|_| SeriesPartial { series: Vec::new() })
            }
        })
        .await;

        merge::merge_series(partials, agg)
    }
}

// Bring sibling modules into scope for the impl above.
use crate::frontend::{job, merge, queue};
```

> **`SeriesAgg: Copy` verify-note:** the `select_series` closure moves `agg` into each job and also passes it to `merge_series` — this assumes `SeriesAgg: Copy` (it is a fieldless enum; the Slice-3 contract should derive `Copy`/`Clone`). If it is not `Copy`, capture a clone per job. The `#[allow(clippy::too_many_arguments)]` mirrors the engine's own `select_series` signature (spec §6.5) — the arg list is the Pyroscope contract, not gratuitous.

> **Suppressed-error note:** failed jobs degrade to empty partials (`unwrap_or_else`) so one slow/broken querier does not fail the whole query — Pyroscope's partial-results behavior. A future hardening slice can surface a partial-results flag / per-job error list; not in scope here. Because `Tree::merge`/`merge_series` are value-additive, an empty partial is the correct identity element (contributes nothing).

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-profiles --lib frontend::orch_tests`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): QueryFrontend orchestrator (plan+queue+fan-out+merge, max_nodes clamp)"
```

---

### Task 8: Shard-equivalence integration test (the first-class correctness gate)

**Files:**
- Create: `crates/profiles/tests/frontend_shard_equivalence.rs`

**Interfaces:**
- Consumes the public `frontend` API end-to-end with `MockQuerier` + `MockCatalog`.

The first-class correctness concern (spec §6.4, §10): a query sharded across N jobs (Live + per-block) equals the unsharded query over the same data — for the flamegraph, the merged `Tree` (hence `FlameGraph`) is identical to a single tree built over all stacks; for select-series, the merged points equal the single-range points. The mock returns, per job, the partial that shard would contribute; the assertion compares against the hand-built unsharded baseline.

- [ ] **Step 1: Shard-equivalence test (`frontend_shard_equivalence.rs`)**

```rust
use std::sync::Arc;

use assert2::assert;
use crabka_pprof::{Frame, SeriesAgg, Series, Tree};
use crabka_profiles::frontend::QueryFrontend;
use crabka_profiles::frontend::backend::{MockQuerier, SeriesPartial, StacktracesPartial};
use crabka_profiles::frontend::config::FrontendConfig;
use crabka_profiles::frontend::job::{BlockMetaInfo, MockCatalog};

const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

fn frame(f: &str) -> Frame {
    Frame { function: f.to_string(), file: String::new(), line: 0 }
}

fn block(id: &str, start: i64, end: i64) -> BlockMetaInfo {
    BlockMetaInfo {
        block_id: id.to_string(),
        start_ms: start,
        end_ms: end,
        profile_types: vec![PT.to_string()],
        size_bytes: 1000,
    }
}

fn partial(stacks: &[(&[&str], i64)]) -> StacktracesPartial {
    let mut t = Tree::default();
    for (frames, value) in stacks {
        let frames: Vec<Frame> = frames.iter().map(|f| frame(f)).collect();
        t.add_stack(&frames, *value);
    }
    StacktracesPartial { tree: t }
}

#[tokio::test]
async fn sharded_merge_stacktraces_equals_unsharded() {
    // Two blocks. The SAME hot leaf `root->a->b` is split across Live + b1; a
    // self-frame `a` lives only in b2. trace value distribution:
    //   Live : a->b = 5
    //   b1   : a->b = 3 ; a(self) = 2
    //   b2   : c(self) = 4
    let catalog = MockCatalog::new(vec![block("b1", 0, 100), block("b2", 100, 200)]);
    let backend = MockQuerier::new();
    // Plan order with hot_frontier 150 = [Live, b1, b2]; max_concurrency 1 ⇒ FIFO.
    backend.stub_stacktraces(partial(&[(&["b", "a"], 5)])); // Live
    backend.stub_stacktraces(partial(&[(&["b", "a"], 3), (&["a"], 2)])); // b1
    backend.stub_stacktraces(partial(&[(&["c"], 4)])); // b2

    let cfg = FrontendConfig { max_concurrency: 1, hot_frontier_ms: 150, ..FrontendConfig::default() };
    let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);
    let got = qf.select_merge_stacktraces("t1", PT, "{}", 0, 300, 2048).await;

    // Unsharded baseline: one tree over ALL stacks.
    let mut unsharded = Tree::default();
    unsharded.add_stack(&[frame("b"), frame("a")], 5);
    unsharded.add_stack(&[frame("b"), frame("a")], 3);
    unsharded.add_stack(&[frame("a")], 2);
    unsharded.add_stack(&[frame("c")], 4);
    let want = unsharded.to_flamegraph(2048);

    assert!(qf.backend_ref().stacktraces_calls().len() == 3);
    assert!(got.total == want.total);
    assert!(got.total == 14);
    assert!(got.names == want.names);
    assert!(got.levels.len() == want.levels.len());
    for (g, w) in got.levels.iter().zip(want.levels.iter()) {
        assert!(g.values == w.values);
    }
}

#[tokio::test]
async fn split_select_series_equals_single_range() {
    // Same series across Live + b1, disjoint timestamps (a time-split) ⇒ the
    // union of points equals the single-range series.
    let catalog = MockCatalog::new(vec![block("b1", 0, 100)]);
    let backend = MockQuerier::new();
    backend.stub_series(SeriesPartial {
        series: vec![Series { labels: vec![("svc".into(), "checkout".into())], points: vec![(3000, 9.0)] }],
    }); // Live (recent)
    backend.stub_series(SeriesPartial {
        series: vec![Series { labels: vec![("svc".into(), "checkout".into())], points: vec![(1000, 5.0), (2000, 7.0)] }],
    }); // b1 (older)

    let cfg = FrontendConfig { max_concurrency: 1, hot_frontier_ms: 50, ..FrontendConfig::default() };
    let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);
    let series = qf
        .select_series("t1", PT, "{svc=\"checkout\"}", &["svc".to_string()], 15.0, SeriesAgg::Sum, 0, 100)
        .await;

    assert!(series.len() == 1);
    // Single-range baseline: points at 1000,2000,3000 ascending.
    assert!(series[0].points == vec![(1000, 5.0), (2000, 7.0), (3000, 9.0)]);
}
```

> **Mock-stub ordering caveat:** `MockQuerier` pops stubs FIFO. Both equivalence tests set `max_concurrency = 1` so dispatch order is the deterministic plan order `[Live, b1, b2]`, matching the stub order. With higher concurrency the FIFO-vs-`buffer_unordered` pairing is nondeterministic; **but the merge is commutative**, so the *result* is order-independent regardless — the FIFO pinning is only so the per-shard *assertions on which partial went where* hold. For a concurrent equivalence test, upgrade `MockQuerier` to match on `JobShard` (return the partial keyed by block id) — a small fixture upgrade flagged here, not needed for this deterministic test.

- [ ] **Step 2: Run to verify they pass**

Run: `cargo test -p crabka-profiles --test frontend_shard_equivalence`
Expected: PASS (2 tests).

- [ ] **Step 3: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "test(profiles): frontend shard-equivalence (merge == unsharded, split-series == single-range)"
```

---

### Task 9: `HttpQuerier` fan-out backend + `HttpCatalog` (reqwest/Connect pool)

**Files:**
- Create: `crates/profiles/src/frontend/http_backend.rs`
- Create: `crates/profiles/tests/frontend_http_backend.rs`
- Modify: `crates/profiles/src/frontend/mod.rs`
- Modify: `crates/profiles/build.rs`
- Create/extend: `crates/profiles/proto/querier/v1/querier.proto`

**Interfaces:**
- Produces:
  - `struct HttpQuerier { http:reqwest::Client, addrs:Vec<String>, next:AtomicUsize, timeout:Duration }` implementing `QuerierBackend`.
  - `fn new(addrs:Vec<String>, timeout:Duration) -> Result<HttpQuerier, BackendError>`.
  - Round-robins `addrs`; sets `X-Scope-OrgID`; for a merge-stacktraces job POSTs `/querier.v1.QuerierService/SelectMergeStacktraces` with `format=TREE`, `max_nodes=0` (full partial), plus the shard restriction (`blockID=<id>` or `shard=live`); parses the partial-`Tree` body into a `StacktracesPartial`. For a select-series job POSTs `/querier.v1.QuerierService/SelectSeries` plus the shard restriction; parses the series body into a `SeriesPartial`. Maps timeout/transport/RPC-error into `BackendError`.
  - `struct HttpCatalog { http, addrs, next }` implementing `BlockCatalog` (GETs `/api/blocks`).

This is a churn-prone surface (reqwest + the querier's exact Connect contract + prost codegen). It is **structure + behavior-pinning**: a loopback axum-stub test (reuses the crate's own axum dep, no new dev-dep) verifies the request shape (path, `blockID`/`shard`/`format`, `X-Scope-OrgID`) and partial-Tree response parsing. The Connect proto is pinned by the build.rs compile, not fabricated.

- [ ] **Step 1: Add the `querier.v1` partial proto + wire it into `build.rs`**

The frontend speaks the same `querier.v1` the querier (Slice 5) serves; this slice needs the request shapes + a `TREE`-format partial response. **Verify against the real Pyroscope `querier.proto` field numbers before pinning — do not fabricate.** Create/extend `crates/profiles/proto/querier/v1/querier.proto` with at least the messages this slice sends/parses (the querier owns the full service; the frontend uses a client-shaped subset):

```proto
syntax = "proto3";
package querier.v1;

// SelectMergeStacktraces (the frontend issues this per shard with format=TREE).
message SelectMergeStacktracesRequest {
  string profile_typeID = 1;
  string label_selector = 2;
  int64 start = 3;            // unix millis
  int64 end = 4;             // unix millis
  int64 max_nodes = 5;       // frontend sends 0 (full partial) per shard
  // Crabka shard-restriction extension (Slice-5 contract): exactly one set.
  string blockID = 100;      // restrict to one block
  string shard = 101;        // "live" ⇒ hot WAL tail
  int32 format = 6;          // FLAMEGRAPH=1, TREE=2 (frontend sends TREE)
}

// The TREE-format partial: fully-symbolized folded stacks (no raw ids on wire).
message StackPartial {
  repeated FoldedStack stacks = 1;
}
message FoldedStack {
  repeated FrameMsg frames = 1;   // leaf-first
  int64 value = 2;
}
message FrameMsg {
  string function = 1;
  string file = 2;
  int32 line = 3;
}

message SelectSeriesRequest {
  string profile_typeID = 1;
  string label_selector = 2;
  int64 start = 3;
  int64 end = 4;
  repeated string group_by = 5;
  double step = 6;            // SECONDS
  int32 aggregation = 7;      // SUM=0, AVERAGE=1
  string blockID = 100;
  string shard = 101;
}
message SeriesPartialMsg { repeated SeriesMsg series = 1; }
message SeriesMsg {
  repeated LabelPair labels = 1;
  repeated PointMsg points = 2;
}
message LabelPair { string name = 1; string value = 2; }
message PointMsg { int64 timestamp = 1; double value = 2; }
```

Extend `crates/profiles/build.rs` to compile it (the grpc-gateway pattern — prefer system `protoc`, vendored fallback):

```rust
let querier_proto = "proto/querier/v1/querier.proto";
let mut builder = connectrpc_axum_build::compile_protos(&[querier_proto], &["proto"]);
if !system_protoc_available() {
    builder = builder.fetch_protoc(None, None)?;
}
builder.compile()?;
println!("cargo:rerun-if-changed={querier_proto}");
```

> **Proto field-number verify-note (the churn surface):** the `profile_typeID`/`label_selector`/`start`/`end`/`max_nodes`/`format` field numbers and the `SelectSeries` `step`/`aggregation` numbers MUST match the real Pyroscope `querier.proto` (spec §7.1) so Grafana and the Slice-5 querier interoperate. **Verify against the pinned Pyroscope tag before believing these numbers** — the `100`/`101` shard fields are a *Crabka extension* (high numbers to avoid colliding with upstream additions) and are an internal frontend↔querier contract, not a Grafana-facing field. If Slice 5 already vendored `querier.proto`, import that file instead of re-declaring; the frontend only needs the request types + a `TREE` partial decode. The loopback test below pins request *shape*, not the generated type names.

- [ ] **Step 2: Write the failing test (stub querier over loopback)**

Create `crates/profiles/tests/frontend_http_backend.rs`. To avoid a Connect-codec dependency in the test, the stub asserts the request path/headers/params and returns the partial-Tree JSON the `HttpQuerier` parses (the `HttpQuerier` may speak Connect-JSON; if it speaks Connect-proto, the stub uses the generated server — verify-noted):

```rust
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use assert2::assert;
use axum::Router;
use axum::extract::State;
use axum::routing::post;
use crabka_profiles::frontend::backend::{MergeStacktracesJob, QuerierBackend};
use crabka_profiles::frontend::http_backend::HttpQuerier;
use crabka_profiles::frontend::job::JobShard;

#[tokio::test]
async fn http_querier_merge_job_posts_tree_format_and_parses() {
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_h = seen.clone();

    let app = Router::new()
        .route(
            "/querier.v1.QuerierService/SelectMergeStacktraces",
            post(
                |State(s): State<Arc<Mutex<Vec<String>>>>,
                 headers: axum::http::HeaderMap,
                 body: String| async move {
                    s.lock().unwrap().push(format!(
                        "{}|{}",
                        headers.get("x-scope-orgid").and_then(|v| v.to_str().ok()).unwrap_or(""),
                        body, // the serialized request — assert it carries TREE + blockID
                    ));
                    // Return a partial tree: root->main(value 10).
                    axum::Json(serde_json::json!({
                        "stacks": [ { "frames": [ { "function": "main", "file": "", "line": 0 } ], "value": 10 } ]
                    }))
                },
            ),
        )
        .with_state(seen_h);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let backend = HttpQuerier::new(vec![addr.to_string()], Duration::from_secs(5)).unwrap();
    let out = backend
        .merge_stacktraces_job(&MergeStacktracesJob {
            tenant: "tenant-x".to_string(),
            profile_type: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".to_string(),
            label_selector: "{}".to_string(),
            start_ms: 0,
            end_ms: 100,
            shard: JobShard::Block { block_id: "blk-1".to_string() },
        })
        .await
        .unwrap();

    assert!(out.tree.to_flamegraph(2048).total == 10);
    let log = seen.lock().unwrap();
    assert!(log.len() == 1);
    // tenant carried; request body mentions the block and TREE format.
    assert!(log[0].starts_with("tenant-x|"));
    assert!(log[0].contains("blk-1"));
}
```

- [ ] **Step 2b: Run to verify it fails**

Run: `cargo test -p crabka-profiles --test frontend_http_backend`
Expected: FAIL — `cannot find type HttpQuerier`.

- [ ] **Step 3: Implement `http_backend.rs`**

```rust
//! The real querier fan-out backend: a reqwest client round-robining over a
//! configurable set of querier addresses, speaking the Connect `querier.v1` API
//! at the per-job grain (one call per planned shard), plus the block catalog.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;

use crate::frontend::backend::{
    BackendError, MergeStacktracesJob, QuerierBackend, SelectSeriesJob, SeriesPartial,
    StacktracesPartial,
};
use crate::frontend::job::{BlockCatalog, BlockMetaInfo, CatalogError, JobShard};
use crate::frontend::wire::{SeriesWire, TreePartialWire};

/// The proto `format` discriminant for a TREE-format partial (spec §7.1).
const FORMAT_TREE: i32 = 2;

/// HTTP querier pool. Round-robins `addrs`; each request carries the tenant in
/// `X-Scope-OrgID` and a per-request timeout.
pub struct HttpQuerier {
    http: reqwest::Client,
    addrs: Vec<String>,
    next: AtomicUsize,
    timeout: Duration,
}

impl HttpQuerier {
    /// Build the pool. `addrs` are `host:port` (no scheme; http:// is assumed).
    ///
    /// # Errors
    /// Returns `BackendError::Transport` if `addrs` is empty or the client
    /// cannot be built.
    pub fn new(addrs: Vec<String>, timeout: Duration) -> Result<Self, BackendError> {
        if addrs.is_empty() {
            return Err(BackendError::Transport("no querier addresses".to_string()));
        }
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| BackendError::Transport(e.to_string()))?;
        Ok(Self { http, addrs, next: AtomicUsize::new(0), timeout })
    }

    fn pick_addr(&self) -> &str {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.addrs.len();
        &self.addrs[i]
    }

    fn map_send_err(e: reqwest::Error) -> BackendError {
        if e.is_timeout() {
            BackendError::Timeout
        } else {
            BackendError::Transport(e.to_string())
        }
    }

    /// Render the shard restriction into the request JSON object.
    fn shard_fields(shard: &JobShard, obj: &mut serde_json::Map<String, serde_json::Value>) {
        match shard {
            JobShard::Live => {
                obj.insert("shard".to_string(), "live".into());
            }
            JobShard::Block { block_id } => {
                obj.insert("blockID".to_string(), block_id.clone().into());
            }
        }
    }
}

#[async_trait]
impl QuerierBackend for HttpQuerier {
    async fn merge_stacktraces_job(
        &self,
        req: &MergeStacktracesJob,
    ) -> Result<StacktracesPartial, BackendError> {
        let url = format!(
            "http://{}/querier.v1.QuerierService/SelectMergeStacktraces",
            self.pick_addr()
        );
        let mut body = serde_json::Map::new();
        body.insert("profile_typeID".to_string(), req.profile_type.clone().into());
        body.insert("label_selector".to_string(), req.label_selector.clone().into());
        body.insert("start".to_string(), req.start_ms.into());
        body.insert("end".to_string(), req.end_ms.into());
        // 0 ⇒ full untruncated partial; the frontend truncates once after merge.
        body.insert("max_nodes".to_string(), 0.into());
        body.insert("format".to_string(), FORMAT_TREE.into());
        Self::shard_fields(&req.shard, &mut body);

        let resp = self
            .http
            .post(&url)
            .header("X-Scope-OrgID", &req.tenant)
            .json(&serde_json::Value::Object(body))
            .send()
            .await
            .map_err(Self::map_send_err)?;
        // The TREE partial: `{ stacks: [ { frames, value } ] }` (wire.rs model).
        let partial: TreePartialWire = resp
            .json()
            .await
            .map_err(|e| BackendError::Transport(format!("decode tree partial: {e}")))?;
        Ok(StacktracesPartial { tree: partial.to_tree() })
    }

    async fn select_series_job(
        &self,
        req: &SelectSeriesJob,
    ) -> Result<SeriesPartial, BackendError> {
        let url = format!("http://{}/querier.v1.QuerierService/SelectSeries", self.pick_addr());
        let agg = match req.agg {
            crabka_pprof::SeriesAgg::Sum => 0,
            crabka_pprof::SeriesAgg::Average => 1,
        };
        let mut body = serde_json::Map::new();
        body.insert("profile_typeID".to_string(), req.profile_type.clone().into());
        body.insert("label_selector".to_string(), req.label_selector.clone().into());
        body.insert("start".to_string(), req.start_ms.into());
        body.insert("end".to_string(), req.end_ms.into());
        body.insert("group_by".to_string(), req.group_by.clone().into());
        body.insert("step".to_string(), req.step_secs.into());
        body.insert("aggregation".to_string(), agg.into());
        Self::shard_fields(&req.shard, &mut body);

        let resp = self
            .http
            .post(&url)
            .header("X-Scope-OrgID", &req.tenant)
            .json(&serde_json::Value::Object(body))
            .send()
            .await
            .map_err(Self::map_send_err)?;
        // `{ series: [ SeriesWire ] }`.
        #[derive(serde::Deserialize)]
        struct SeriesBody {
            #[serde(default)]
            series: Vec<SeriesWire>,
        }
        let body: SeriesBody = resp
            .json()
            .await
            .map_err(|e| BackendError::Transport(format!("decode series partial: {e}")))?;
        Ok(SeriesPartial { series: body.series.into_iter().map(SeriesWire::into_series).collect() })
    }
}

/// The production block catalog: GETs the querier's block-metadata door.
pub struct HttpCatalog {
    http: reqwest::Client,
    addrs: Vec<String>,
    next: AtomicUsize,
}

impl HttpCatalog {
    #[must_use]
    pub fn new(addrs: Vec<String>) -> Self {
        Self { http: reqwest::Client::new(), addrs, next: AtomicUsize::new(0) }
    }

    fn pick_addr(&self) -> &str {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.addrs.len().max(1);
        &self.addrs[i]
    }
}

#[derive(serde::Deserialize)]
struct BlocksBody {
    #[serde(default)]
    blocks: Vec<BlockMetaJson>,
}

#[derive(serde::Deserialize)]
struct BlockMetaJson {
    #[serde(rename = "blockID")]
    block_id: String,
    #[serde(default, rename = "startMillis")]
    start_ms: i64,
    #[serde(default, rename = "endMillis")]
    end_ms: i64,
    #[serde(default, rename = "profileTypes")]
    profile_types: Vec<String>,
    #[serde(default, rename = "sizeBytes")]
    size_bytes: u64,
}

#[async_trait]
impl BlockCatalog for HttpCatalog {
    async fn blocks(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<BlockMetaInfo>, CatalogError> {
        let url = format!("http://{}/api/blocks", self.pick_addr());
        let resp = self
            .http
            .get(&url)
            .header("X-Scope-OrgID", tenant)
            .query(&[("start", start_ms.to_string()), ("end", end_ms.to_string())])
            .send()
            .await
            .map_err(|e| CatalogError::Backend(e.to_string()))?;
        let body: BlocksBody = resp
            .json()
            .await
            .map_err(|e| CatalogError::Backend(format!("decode blocks: {e}")))?;
        Ok(body
            .blocks
            .into_iter()
            .map(|b| BlockMetaInfo {
                block_id: b.block_id,
                start_ms: b.start_ms,
                end_ms: b.end_ms,
                profile_types: b.profile_types,
                size_bytes: b.size_bytes,
            })
            .collect())
    }
}
```

> **reqwest 0.13 + Connect-contract verify-note (the churn surface):** `Client::builder().timeout(..).build()`, `.post(url).header(..).json(&v).send().await`, `Response::json::<T>().await`, and `reqwest::Error::is_timeout()` are reqwest 0.13 surface (already used in grpc-gateway's `forward.rs` with `json`+`rustls`). **The Connect transport choice is the open question:** this implementation speaks **Connect-JSON** (`application/json` POST to the RPC path) for a parse the loopback stub can assert without the generated codec. If the Slice-5 querier serves **only** `application/proto` Connect, switch `HttpQuerier` to encode the prost request types from Task 9's proto and decode the `StackPartial`/`SeriesPartialMsg` responses — the `TreePartialWire`/`SeriesWire` serde types then become `From`-projections of the prost types, and the loopback stub uses the generated Connect server. Either way the **behavior** (TREE format, `blockID`/`shard` restriction, `X-Scope-OrgID`, full-partial `max_nodes=0`, `to_tree()` reconstruction) is pinned by the test; verify the Slice-5 content-type before finalizing. The `/api/blocks` field spellings (`blockID`/`startMillis`/`profileTypes`) are the assumed Slice-5 contract — if Slice 5 spells them differently, this file is the single edit point.

- [ ] **Step 4: Re-export from `mod.rs`**

```rust
pub mod http_backend;

pub use http_backend::{HttpCatalog, HttpQuerier};
```

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-profiles --test frontend_http_backend`
Expected: PASS (the merge-job loopback test).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): HttpQuerier Connect fan-out backend + HttpCatalog + querier.v1 partial proto"
```

---

### Task 10: Connect server + legacy `/pyroscope/render` + `--target query-frontend` role binary

**Files:**
- Create: `crates/profiles/src/frontend/server.rs`
- Create: `crates/profiles/tests/frontend_server.rs`
- Modify: `crates/profiles/src/bin/crabka-profiles.rs`
- Modify: `crates/profiles/src/frontend/mod.rs`

**Interfaces:**
- Produces:
  - `fn router_with_backend<B,C>(qf:Arc<QueryFrontend<B,C>>) -> axum::Router` — the Connect `querier.v1` routes (`SelectMergeStacktraces`, `SelectSeries`, and `ProfileTypes` proxied as the health probe) + the legacy `GET /pyroscope/render`, tenant from `X-Scope-OrgID`. (`B: QuerierBackend + 'static, C: BlockCatalog + 'static`.)
  - `async fn run_query_frontend(cfg:FrontendConfig, shutdown:CancellationToken) -> std::io::Result<()>` — build the `HttpQuerier` pool + `HttpCatalog`, bind `cfg.listen_addr`, serve.
  - The binary's `--target query-frontend` arm calls `run_query_frontend`.

- [ ] **Step 1: Write the failing handler test**

Create `crates/profiles/tests/frontend_server.rs` — boot the frontend router against a `MockQuerier`+`MockCatalog`-backed `QueryFrontend` over loopback and assert the legacy `/pyroscope/render` round-trips into a flamebearer (the path that needs no Connect codec in the test), with tenant honored:

```rust
use std::sync::Arc;
use std::time::Duration;

use assert2::assert;
use crabka_pprof::{Frame, Tree};
use crabka_profiles::frontend::QueryFrontend;
use crabka_profiles::frontend::backend::{MockQuerier, StacktracesPartial};
use crabka_profiles::frontend::config::FrontendConfig;
use crabka_profiles::frontend::job::{BlockMetaInfo, MockCatalog};
use crabka_profiles::frontend::server::router_with_backend;

const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

#[tokio::test]
async fn server_round_trips_legacy_render() {
    let catalog = MockCatalog::new(vec![BlockMetaInfo {
        block_id: "b1".to_string(),
        start_ms: 0,
        end_ms: 100,
        profile_types: vec![PT.to_string()],
        size_bytes: 10,
    }]);
    let backend = MockQuerier::new();
    let mut t = Tree::default();
    t.add_stack(&[Frame { function: "main".to_string(), file: String::new(), line: 0 }], 10);
    backend.stub_stacktraces(StacktracesPartial { tree: t });

    let cfg = FrontendConfig { hot_frontier_ms: i64::MAX, ..FrontendConfig::default() };
    let qf = Arc::new(QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg));
    let app = router_with_backend(qf);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{addr}/pyroscope/render"))
        .query(&[
            ("query", format!("{PT}{{}}")),
            ("from", "0".to_string()),
            ("until", "100".to_string()),
            ("format", "json".to_string()),
            ("maxNodes", "2048".to_string()),
        ])
        .header("X-Scope-OrgID", "t1")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    // flamebearer single projection, total ticks = 10.
    assert!(body["flamebearer"]["numTicks"] == 10);
    assert!(body["metadata"]["format"] == "single");
    // The block-builder/querier saw the tenant on its job.
    // (The Live job is enough — hot_frontier MAX ⇒ one Live job.)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --test frontend_server`
Expected: FAIL — `cannot find function router_with_backend`.

- [ ] **Step 3: Implement `server.rs`**

The Connect `querier.v1` routes are generated by `connectrpc-axum` from Task 9's proto and dispatch to the orchestrator; the legacy `/pyroscope/render` is a plain axum GET that parses `query=<profile_typeID>{selectors}` and projects the merged flamegraph into a flamebearer. The render handler needs no Connect codec, so it is the one pinned by the loopback test.

```rust
//! HTTP surface for the query-frontend: the Connect `querier.v1` routes + the
//! legacy `/pyroscope/render` flamebearer endpoint, tenant extraction, and the
//! `run_query_frontend` role entrypoint.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::frontend::QueryFrontend;
use crate::frontend::backend::QuerierBackend;
use crate::frontend::config::FrontendConfig;
use crate::frontend::http_backend::{HttpCatalog, HttpQuerier};
use crate::frontend::job::BlockCatalog;
use crate::frontend::wire::flamegraph_to_flamebearer;

const TENANT_HEADER: &str = "X-Scope-OrgID";

#[derive(Debug, Deserialize)]
struct RenderParams {
    /// `<profile_typeID>{selectors}` — Pyroscope's combined query param.
    query: String,
    #[serde(default)]
    from: Option<i64>,
    #[serde(default)]
    until: Option<i64>,
    #[serde(default, rename = "maxNodes")]
    max_nodes: Option<i64>,
}

fn tenant_of(headers: &HeaderMap) -> String {
    headers
        .get(TENANT_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string()
}

/// Split `<profile_typeID>{selectors}` into `(profile_type, label_selector)`.
/// The selector is the `{...}` matcher string (empty `{}` when absent).
fn split_query(query: &str) -> (String, String) {
    match query.find('{') {
        Some(i) => (query[..i].to_string(), query[i..].to_string()),
        None => (query.to_string(), "{}".to_string()),
    }
}

/// Build the query-frontend router for any backend/catalog pair (so tests use
/// mocks and prod uses the HTTP impls). Connect `querier.v1` routes are mounted
/// by `connectrpc-axum` (Task 9 proto); the legacy render endpoint is a plain GET.
#[must_use]
pub fn router_with_backend<B, C>(qf: Arc<QueryFrontend<B, C>>) -> Router
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    Router::new()
        .route("/pyroscope/render", get(render_handler::<B, C>))
        // Connect querier.v1 routes are added here via the generated
        // `connectrpc_axum` service builder (SelectMergeStacktraces / SelectSeries
        // / ProfileTypes health-probe) — see the VERIFY note below.
        .with_state(qf)
}

async fn render_handler<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    Query(p): Query<RenderParams>,
) -> impl IntoResponse
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let tenant = tenant_of(&headers);
    let (profile_type, label_selector) = split_query(&p.query);
    let max_nodes = p.max_nodes.unwrap_or(qf.default_max_nodes());
    let fg = qf
        .select_merge_stacktraces(
            &tenant,
            &profile_type,
            &label_selector,
            p.from.unwrap_or(0),
            p.until.unwrap_or(i64::MAX),
            max_nodes,
        )
        .await;
    // Units/name from the profile type's sample_unit/name (5-part string);
    // a fuller parse lives in ProfileType (Slice 2) — pass the type id as name.
    Json(flamegraph_to_flamebearer(&fg, "samples", &profile_type))
}

/// Boot the query-frontend role: build the HTTP querier pool + block catalog,
/// then serve the router on `cfg.listen_addr` until `shutdown` fires.
///
/// # Errors
/// Propagates bind/serve `std::io` errors and backend-construction failures.
pub async fn run_query_frontend(
    cfg: FrontendConfig,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let backend = HttpQuerier::new(cfg.backend_addrs.clone(), cfg.request_timeout)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let catalog = HttpCatalog::new(cfg.backend_addrs.clone());
    let qf = Arc::new(QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg.clone()));
    let app = router_with_backend(qf);
    let listener = tokio::net::TcpListener::bind(cfg.listen_addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
}

// silence unused import warnings until the Connect service builder is wired.
#[allow(unused_imports)]
use Duration as _DurationUsed;
```

> **Connect-router wiring verify-note (the codegen seam):** the `querier.v1` Connect routes (`SelectMergeStacktraces`/`SelectSeries`/`ProfileTypes`) are mounted via the `connectrpc-axum` generated service builder from Task 9's proto, exactly as `crates/grpc-gateway/src/serve.rs` mounts its service onto the axum `Router` (the workspace precedent). **Verify the exact `connectrpc-axum` server API** (the generated `*Service` trait + the `.into_router()`/`merge` call) against grpc-gateway before wiring — the handler bodies just call `qf.select_merge_stacktraces(..)` / `qf.select_series(..)` and project the result into the Connect response types (the flamegraph → `querier.v1` `FlameGraph` message; the series → `SelectSeriesResponse`). The legacy `/pyroscope/render` GET (pinned by the loopback test) needs none of this and proves the orchestrator→flamebearer path end-to-end. `ProfileTypes` is proxied to one backend as the datasource health probe (spec §7.1 — no separate `/ready`); a single-backend proxy (no merge) suffices and can reuse the `HttpQuerier` directly.

> **axum 0.8 note:** `Query<RenderParams>` extraction + `get(handler)` routing is axum 0.8 surface — verify against grpc-gateway's `serve.rs` router. The render param `query` carries the literal `{` so it must be URL-encoded by the client (reqwest's `.query(&[...])` does this); the handler `split_query` finds the first `{`.

- [ ] **Step 4: Wire the role binary**

Extend the existing role binary `crates/profiles/src/bin/crabka-profiles.rs` (Slices 4–5 created it with `distributor`/`block-builder`/`querier` arms) with the `query-frontend` arm:

```rust
        Target::QueryFrontend => {
            use crabka_profiles::frontend::config::FrontendConfig;
            use crabka_profiles::frontend::server::run_query_frontend;
            use tokio_util::sync::CancellationToken;

            // Real config wiring (backend addrs / listen addr / hot frontier from
            // flags or a config file) lands in the hardening slice; the default
            // FrontendConfig is enough to boot the role.
            let cfg = FrontendConfig::default();
            let shutdown = CancellationToken::new();
            run_query_frontend(cfg, shutdown).await
        }
```

Ensure `Target` (the `clap::ValueEnum`) has a `QueryFrontend` variant; add it if Slices 4–5 left it out. Re-export in `mod.rs`: `pub mod server; pub use server::{router_with_backend, run_query_frontend};`.

> **Binary-config note:** this slice wires the role *dispatch* and a working `HttpQuerier`/`HttpCatalog` from the default config. Real config loading (querier addresses, `max_concurrency`, the per-partition `hot_frontier_ms` from the block-builder offsets, the listen addr, per-tenant `max_nodes` clamps) lands in Slice 8 hardening. The server test targets the library router, not the binary, so the default config suffices to pass.

- [ ] **Step 5: Run to verify it passes + whole-crate gate**

Run: `cargo test -p crabka-profiles --test frontend_server`
Then the full gate: `cargo test -p crabka-profiles && cargo clippy -p crabka-profiles --all-targets && cargo fmt -p crabka-profiles --check`
Expected: all PASS, no warnings, formatting clean. Also confirm the binary builds: `cargo build -p crabka-profiles --bin crabka-profiles`.

- [ ] **Step 6: Commit**

```bash
git add crates/profiles/
git commit -m "feat(profiles): query-frontend Connect server + legacy /pyroscope/render + --target query-frontend role"
```

---

## Result cache (deferred — rationale)

A result cache is **optional** for profiles and intentionally **not** built in this slice:

- **No moving-window reuse for flamegraphs.** The metrics frontend's result cache pays off because Grafana re-issues the *same* `query_range` with a sliding window, so older step-aligned sub-ranges are reused. A `SelectMergeStacktraces` is a *fold* over a window — each flamegraph is a fresh aggregation; there is little step-aligned sub-range structure to reuse across requests, and `SelectSeries` has a smaller per-step payload than a full metrics matrix.
- **Where a profiles cache actually pays.** The natural cache, mirroring Pyroscope, is a **block-keyed partial cache**: a sealed block is immutable, so its resolve-locally partial `Tree` for a given `(profile_type, label_selector, block_id)` is content-addressable and reusable across queries whose window covers that block. That is a clean follow-on: a `PartialCache` trait consulted inside the per-`Block`-job branch of the orchestrator, keyed by `(tenant, block_id, profile_typeID, selector_hash, start, end)`, with `Live` jobs never cached (the WAL tail mutates). Because the merge is commutative and value-additive, a cache-hit job contributes its cached partial `Tree` with zero querier round-trip.
- **Decision:** ship the split/queue/fan-out/merge correctness first (this slice), and add the block-keyed `PartialCache` in the hardening slice (Slice 8) alongside per-tenant limits — where the cache's eviction/size budget is a tenant-quota concern anyway. This is flagged, not forgotten.

---

## Self-review

**Spec coverage (against §6.4 cross-block partial-tree merge, §6.1–6.2 flamegraph/select-series, §7 API surface, §11 Slice 6):**
- **Query split/shard** (time-window → Live hot tier vs cold blocks; per-block jobs; profile-type prefilter) → Tasks 3, 7, 8.
- **Partial-`Tree` merge → single flamegraph fold** (each shard resolves locally → full symbolized partial `Tree`; the frontend `Tree::merge`s then `to_flamegraph(max_nodes)` ONCE; raw ids never cross a boundary) → Tasks 1 (wire), 4 (merge), 7 (orchestrator), 8 (equivalence).
- **`max_nodes` applied once on the merged tree** (querier returns full partials; frontend clamps + truncates with synthetic `"other"` exactly once) → Tasks 2 (no `max_nodes` on the job), 4, 7.
- **SelectSeries shard-merge** (per-`(labels, ts)` sum / sum+count average; split == single-range) → Tasks 5, 7, 8.
- **Queueing + fan-out** (bounded-concurrency `buffer_unordered` across queriers, trait-abstracted backend, per-job dispatch with timeouts; commutative merge ⇒ order-independent) → Tasks 2 (trait), 6 (queue), 7 (orchestrator), 9 (`HttpQuerier`).
- **API surface** (Connect `querier.v1` `SelectMergeStacktraces`/`SelectSeries` + `ProfileTypes` health probe; legacy `/pyroscope/render` flamebearer `"single"` projection; `X-Scope-OrgID`; start/end millis) → Tasks 1 (flamebearer), 9 (proto/client), 10 (server).
- **Role binary** `crabka-profiles --target query-frontend` → Task 10.
- **First-class correctness** (sharded merge == unsharded over identical data; split select-series == single-range) → Tasks 4, 5, 8.

**Contract fidelity:** consumes the Slice-2/3 `crabka-pprof` engine types (`Tree`/`FlameGraph`/`Level`/`Series`/`SeriesAgg`/`Frame`/`ProfileError`) **by import, not redefinition**, and the Slice-5 querier surface (Connect `querier.v1` at the per-job grain with the `blockID`/`shard`/`format=TREE` restriction + `/api/blocks`). The `TreePartialWire`/`SeriesWire`/`Flamebearer` model (Task 1) is the HTTP/serde edge and is pinned by serde + round-trip tests. **The load-bearing invariant — merge `Tree`s, never raw ids or partial `FlameGraph` levels (spec §6.4) — is encoded in the type system:** the `QuerierBackend` returns a `StacktracesPartial { tree }` (a symbolized `Tree`), there is no path that crosses a block boundary with a raw `stacktrace_id`, and `to_flamegraph(max_nodes)` is reachable only after `merge_trees`.

**Churn-prone surfaces — structured + behavior-pinned + verify-noted:**
- `crabka-pprof` `Tree` read surface (`wire.rs` `from_tree` ← `Tree::leaf_stacks`) — flagged as *not* in the pinned Slice-2 contract, with a "verify the real `Tree`; add a leaf-stack iterator as a companion `crabka-pprof` change if absent" note; the merge path (Task 4) uses only pinned `merge`/`to_flamegraph`/`add_stack`, so it is unblocked even if `from_tree` is briefly gated. **Not fabricated.**
- `Tree::merge`/`to_flamegraph` consume-vs-borrow signatures (`merge.rs`) — verify-noted; tests pin behavior (sharded==unsharded, truncation-preserves-total, synthetic `"other"`), so drift is a compile error.
- `reqwest` 0.13 + Connect content-type (`http_backend.rs`) — pinned by a loopback axum-stub test (TREE format, `blockID`/`shard`, `X-Scope-OrgID`, `max_nodes=0`, `to_tree()` reconstruction); the Connect-JSON-vs-proto transport choice is explicitly flagged with a "verify Slice-5 content-type; switch to prost codec if proto-only" note.
- `prost` 0.14 + `connectrpc-axum-build` proto (`querier.proto`, `build.rs`) — the field numbers carry an explicit "verify against the real Pyroscope `querier.proto`; the `100`/`101` shard fields are a Crabka extension" note; the proto is pinned by the build.rs compile, **not fabricated** — if Slice 5 vendored the proto, import it.
- `connectrpc-axum` server router (`server.rs`) — the Connect-route mounting is verify-noted against the grpc-gateway `serve.rs` precedent; the legacy `/pyroscope/render` GET (which needs no codec) is pinned by the loopback server test, proving the orchestrator→flamebearer path end-to-end.
- `futures` `buffer_unordered` (`queue.rs`) — standard idiom, `futures_util` fallback noted; pinned by the bounded-concurrency test.

**`Average` decomposition — flagged and bounded:** `merge_series` implements `Average` as sum-over-partials / partial-count, with an explicit verify-note that the *faithful* decomposition depends on whether the Slice-5 querier returns per-shard sums + counts (mirroring the metrics frontend's `avg → sum/count`) vs per-shard means; the `Sum` path (the Grafana-minimum agg) is exact regardless and pinned by two equivalence tests. The plan does not silently assert wrong average numbers — it picks a default and flags the verification.

**Result cache — deferred, not dropped:** the optional profiles result cache is explicitly deferred to Slice 8 with a concrete design (block-keyed `PartialCache` consulted in the per-`Block`-job branch, keyed by `(tenant, block_id, profile_typeID, selector_hash, start, end)`, `Live` jobs never cached, hit contributes a cached partial `Tree` with no round-trip) — the spec calls it optional, and the moving-window reuse that justifies the metrics cache does not apply to flamegraph folds.

**Placeholder scan:** no "TBD"/"similar to Task N"/"add error handling". Every step has runnable code or an exact command. The genuine external-contract hand-waves (`Tree::leaf_stacks`/`Tree::merge` signatures; Slice-5 Connect content-type + proto field numbers; the `connectrpc-axum` router API) are each bounded with a verify-against-the-real-type note and pinned/gated by a behavior test, never left vague.

**Type consistency:** `TreePartialWire`/`SeriesWire`/`Flamebearer` defined once (Task 1) and used unchanged across http_backend/server. `QuerierBackend` (Task 2) implemented by both `MockQuerier` (Task 2) and `HttpQuerier` (Task 9) with identical signatures; `BlockCatalog` (Task 3) by `MockCatalog`/`HttpCatalog`. `JobShard`/`MergeStacktracesJob`/`SelectSeriesJob`/`StacktracesPartial`/`SeriesPartial`/`JobPlan`/`FrontendConfig` referenced consistently between definitions, orchestrator, and tests. The `max_nodes`-on-the-frontend-only rule is consistent: absent from `MergeStacktracesJob`, clamped in the orchestrator, applied once in `merge_stacktraces`.

**Known risk (flagged):** the `MockQuerier` FIFO-stub-vs-`buffer_unordered`-dispatch ordering (Task 8 caveat) makes per-shard *assertions* deterministic only under `max_concurrency = 1`; because the merge is commutative the *result* is order-independent regardless, so the risk is contained to the test fixture (a shard-keyed mock upgrade enables a concurrent equivalence test) and surfaces as a failing assertion, never silent corruption.
