# crabka-profiles Slice 8 — Hardening (per-tenant limits, multi-tenancy isolation, compaction + downsampling, differential-vs-Pyroscope + Grafana)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the profiles backend production-faithful at its multi-tenant edges, give it a real `compactor` role, and prove it is a drop-in Grafana-Pyroscope replacement. Add per-tenant limits/quotas (max series, label name/value length + count, ingestion rate, `max_nodes`, query range, `__session_id__` cardinality) with **Pyroscope-shaped** errors and a YAML runtime-overrides file; harden tenant isolation so org A can never observe org B's profiles/labels/types/symbols through the Connect API; build the `compactor` role (vertical dedup + horizontal `1h→4h→8h` merge + symbol-DB re-dedup + `5m`/`1h` downsampling); and build the two external-system differential suites — **differential-vs-real-Pyroscope** (testcontainers) and **Grafana** (built-in Pyroscope datasource → Crabka). The two headline tests are (1) end-to-end tenant isolation through the Connect `querier.v1` API with two `X-Scope-OrgID`s across every read surface and (2) flamegraph/series equality vs real Pyroscope over identically-pushed pprof.

**Architecture:** This slice adds **no new flamegraph-merge or storage semantics** — it is a hardening band around the Slice 4 distributor (ingest) + `block-builder`, the Slice 5 querier + Connect `querier.v1` API + legacy `/pyroscope/render`, the Slice 6 query-frontend, and the Slice 2/3 `FlameEngine`/`SymbolDb`. New code lives in four areas of `crabka-profiles`: (a) a `limits` module (a per-tenant `Limits` struct, a YAML `OverridesProvider` modeled on Pyroscope's `overrides.yaml`, and enforcement points wired into the distributor write path and the querier read path, reusing the broker's `TokenBucket` for the two *rate*/cardinality limits); (b) HTTP error-mapping that projects `LimitError` onto the **Pyroscope** Connect error envelope (`connect.Code` + status) and the legacy-render status codes; (c) the `compactor` role — a blockstore-level merge of two-or-more profile blocks into one (concatenate samples fact tables, re-intern symbol DBs to dedup cross-block strings/functions/locations/stacktrace trees, rewrite `ProfileIndex`, downsample by floor-bucketing timestamps), reusing the Slice 4 `BlockWriter` + `SymbolDb::intern_stacktrace`; (d) the black-box differential + Grafana harnesses over the compiled Connect server + Docker containers. Tenant isolation is **not** a new mechanism — it is the assertion that every existing key (WAL partition key, profile-block/`ProfileIndex`/symbol-DB object key, hot-store map key, quota bucket key) is already `(tenant, …)`-prefixed; this slice adds the tests that prove it and fixes any leak they expose. The external suites (Pyroscope, Grafana) are black-box harnesses over the compiled Connect server + Docker containers, all `#[ignore]`, run in a dedicated CI job.

**Tech Stack:** Rust 2024 · `arrow` 59 · `datafusion` (pinned `rev="0838a4ddb902535b0e95a1c5a254be7e9c7fe9bf"`, arrow 59) for the compactor's concatenate/group-by fold · `object_store` 0.13 (block read/write through the Slice 1 `BlockStore`/`BlockWriter`) · Connect-RPC via `connectrpc-axum` + `connectrpc-axum-build` (build.rs codegen, reuse the [grpc-gateway/build.rs](../../../crates/grpc-gateway/build.rs) system-`protoc`-with-vendored-fallback pattern) · `prost` 0.14 (the `querier.v1`/`push.v1` protos) · `serde_yaml` 0.9 + `serde` (overrides file) · the broker `TokenBucket` (KIP-73, via a path dep / thin re-export) · `dashmap` 6 (per-tenant bucket cache) · `thiserror`. Tests: `assert2`; `reqwest` 0.13 + `tokio` for in-process Connect drive; `testcontainers` 0.27 + `testcontainers-modules` 0.15 for the Docker differential suites; `serde_json` for response diffing; the Slice 2 `PprofProfile` encoder to build identical pprof push payloads for both backends.

## Global Constraints

- **No backwards compatibility.** Greenfield/undeployed. Change the `Limits` schema, the overrides YAML shape, the compacted-block layout, and any error-body shape freely; no shims, no migration code, no `#[serde(default)]` "to keep old configs/blocks readable".
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe`.
- **Lints:** `clippy::pedantic` is `warn`. New code clippy-pedantic clean. Run `cargo clippy -p crabka-profiles --all-targets` before each commit.
- **Formatting:** `cargo fmt -p crabka-profiles` before every commit (never `cargo +nightly fmt --all` — OS error 206 in deep worktrees on Windows; always `-p`).
- **Assertions:** `assert2::assert!` / `assert2::check!` in tests.
- **Kafka wire compat is the only Kafka contract that must not drift.** This slice touches no Kafka bytes. The **Pyroscope Connect/legacy HTTP** byte-exactness is the *analog* constraint here: error envelopes (the Connect `{ "code": <connect-code>, "message": … }` body + the gRPC-status HTTP mapping), status codes, the `FlameGraph` 4-/`FlameGraphDiff` 7-ints-per-bar shapes, and the flamebearer JSON must match Pyroscope exactly (that is what the differential suites verify). Pin the exact Connect error codes against the real container in Task 9; do not invent them.
- **Docker/external-system tests are `#[ignore]`.** Every test that needs a running Pyroscope or Grafana container is annotated `#[ignore = "requires Docker"]` and lives behind the dedicated CI job (`profiles-differential`), never in the default `cargo test --workspace` path. Reuse the Confluent-image rationale and bootstrap-retry patterns from `crates/client-core/tests/integration.rs`.
- **Reuse, don't reinvent, the token bucket.** Per-tenant *rate* limits (ingestion rate, in profiles/sec or samples/sec) and the `__session_id__` cardinality cap use the broker's `crabka_broker::throttle::TokenBucket` (`new()`, `set_rate(u64)`, `try_consume(u64) -> u64` granted; rate-0 ⇒ unthrottled, granting the full request). Do not write a second rate limiter. The *count/length* limits (max series, label name/value length + count, `max_nodes`, query range) are plain comparisons, not buckets.
- **Pyroscope limit parity.** Limit names mirror Pyroscope's `overrides` block where one exists: `ingestion_rate_mb` / `max_series` / `max_label_name_length` / `max_label_value_length` / `max_label_names_per_series` / `max_flamegraph_nodes_default`+`max_flamegraph_nodes_max` / `max_query_length`. We adopt the *semantics* and the *Connect-code/status mapping*, not byte-for-byte config-key names where the spec (§9) names them differently; each field carries a doc-comment naming the Pyroscope analog.
- **Compaction is greenfield, not phlaredb-compatible.** The compactor merges *Crabka* profile blocks (samples fact table + the Slice 1/2 symbol-DB artifact). It is not byte-compatible with Pyroscope's compactor — it must only preserve query-result equality (a merged block answers `SelectMergeStacktraces`/`SelectSeries` identically to querying the inputs). The headline differential (Task 9) is what proves that.

---

## Dependency & slice roadmap

**Depends on:**
- **Slice 1** (`crabka-blockstore` `ProfileIndex` + samples fact-table schema `PCOL_*` + symbol-DB on-block artifact). The compactor reads/writes these; the isolation tests assert the `(tenant, …)`-prefixed object keys. ✅ planned.
- **Slice 2** (`crabka-pprof` core — `PprofProfile`, `SymbolDb` with `intern_stacktrace`/`resolve`/`encode`/`decode`, `ProfileType`, `Tree`/`FlameGraph`, `ProfileStore`/`FlameEngine`, `ProfileError`). The compactor re-uses `SymbolDb::intern_stacktrace` to re-dedup; the limits cap `max_nodes` feeding `FlameEngine`; the differ builds identical pprof via `PprofProfile::encode`. **Consumed via contract.**
- **Slice 3** (engine completeness — `SelectSeries`/`Diff`/`SelectMergeProfile`). The differential corpus exercises these.
- **Slice 4** (ingest service — the `distributor` write path `decode → relabel → multi-value split → shard → produce`; the `block-builder` consumer group → samples fact table + dedup symbol DB + `ProfileIndex`; the `ProfileRecord` WAL record). The ingest-rate/series/label/`__session_id__` limits enforce in the distributor pre-WAL hook; the differ builds identical pprof from the same fixtures.
- **Slice 5** (querier + Connect `querier.v1` API + legacy `/pyroscope/render`). The axum/Connect router, the `X-Scope-OrgID` tenancy extractor, the Pyroscope-shaped response/error envelope, and the `ProfileStore` impl (`CrabkaProfileStore`, hot/cold UNION). **This slice extends that router's error envelope** (adds limit errors) and asserts isolation through it.
- **Slice 6** (query-frontend — search split/shard + partial-tree merge). The `max_nodes`/query-range caps apply at the frontend entry.
- **Slice 7** (native symbolization). Not exercised here except transitively through the querier; the differential corpus uses *pre-symbolized* pprof so symbolization is not on the equality path (flag a Contract gap if a corpus case needs query-time symbolization and gate it).

**Shared Contract (consume, do not re-derive).** The following are assumed to exist from earlier slices; each task that touches them lists the exact item it consumes. If an item is missing because its slice is unlanded, the task creates a **minimal local trait/shim with a single `todo!()`-free in-memory impl for tests** and flags it in the task's "Contract gap" note — never a silent stub.

| Contract item | From | This slice consumes it as |
|---|---|---|
| `tenant_of(&HeaderMap) -> String` (resolves `X-Scope-OrgID`, default `"anonymous"`) | Slice 5 | the key prefix every limit/isolation test asserts on |
| Connect `querier.v1` `Router` + Pyroscope response envelope + Connect error envelope | Slice 5 | extended with new error variants (the limit Connect-codes) |
| `CrabkaProfileStore` (`impl ProfileStore`, hot/cold UNION) + `FlameEngine` | Slice 2 def / Slice 5 impl | the tenant-scoped source every isolation assertion reads through |
| `FlameEngine::{select_merge_stacktraces, select_series, diff}` + `EngineOpts { default_max_nodes }` | Slice 2/3 | the body the `max_nodes` cap and the differential corpus drive |
| distributor write-path hook (`ProfileRecord` batch, per-tenant, before WAL append) | Slice 4 | the enforcement point for ingest-rate / series / label / `__session_id__` limits |
| `ProfileRecord` (WAL record: tenant + Labels + profile_type + payload + symbol set) encode/decode | Slice 4 | building identical pprof push payloads for the differential suites |
| `BlockStore`/`BlockWriter`/`BlockMeta`, `ProfileIndex`, `PCOL_*` column constants + samples schema | Slice 1 | the compactor's read-merge-write substrate |
| `SymbolDb::{intern_stacktrace, resolve, encode, decode}`, `ProfileType`, `LabelMatcher`/`MatchOp`, `ProfileError` | Slice 1/2 | the compactor's symbol re-dedup + the differ's label parsing |
| `crabka_broker::throttle::TokenBucket` | broker | the per-tenant ingest-rate + `__session_id__`-cardinality bucket |

**The 8 profiles slices** (this plan = Slice 8, the last): 1 blockstore `ProfileIndex` + samples schema + symbol-DB artifact · 2 `crabka-pprof` core · 3 engine completeness · 4 ingest · 5 querier + Connect `querier.v1` + legacy render · 6 query-frontend · 7 native symbolization · **8 hardening (this plan)**.

---

## File structure (`crates/profiles/`)

| File | Responsibility |
|---|---|
| `src/limits/mod.rs` | `Limits` struct + `LimitError` (Pyroscope-shaped) + module re-exports |
| `src/limits/overrides.rs` | `OverridesProvider` — load Pyroscope-style `overrides.yaml`, resolve per-tenant `Limits` (tenant override merged over defaults) |
| `src/limits/enforce.rs` | enforcement helpers: ingest-side (`IngestEnforcer`) + query-side (`QueryEnforcer`); ingest-rate + `__session_id__` cardinality use `TokenBucket` |
| `src/http/error.rs` | `LimitError` → Pyroscope Connect/legacy status+code projection (extends the Slice 5 envelope) |
| `src/compactor/mod.rs` | `compact_blocks` — vertical+horizontal merge + symbol re-dedup + downsample; `CompactionPlan`/`DownsampleStep` |
| `src/compactor/symbols.rs` | cross-block symbol-DB re-intern (`MergedSymbols`) — re-dedup strings/functions/locations + re-intern stacktrace trees, returning the per-input `stacktrace_id` remap |
| `src/compactor/downsample.rs` | timestamp floor-bucketing (`5m`/`1h`) + per-bucket fact-table fold |
| `tests/limits_overrides.rs` | unit/integration: YAML load + per-tenant resolution + enforcement decisions through real Connect HTTP |
| `tests/tenant_isolation.rs` | **headline** — two-`X-Scope-OrgID` end-to-end isolation through the Connect `querier.v1` API (in-process, no Docker) |
| `tests/compaction.rs` | block-merge correctness: query-equality before/after, symbol re-dedup ratio, downsample bucketing (in-process) |
| `tests/diff_pyroscope.rs` | `#[ignore]` **headline** differential vs real Pyroscope (testcontainers) |
| `tests/grafana_integration.rs` | `#[ignore]` Grafana + built-in Pyroscope datasource → Crabka (ProfileTypes health / flame graph / time series / drilldown render) |
| `tests/support/profiles_server.rs` | shared in-process server boot + two-tenant seed + pprof-push helpers (path-included by the integration tests) |
| `tests/support/diff_corpus.rs` | the shared pprof seed dataset + query corpus + a `assert_profile_query_equal` JSON/flamegraph differ (path-included by the Docker suites) |

---

### Task 1: `Limits` model + Pyroscope-shaped `LimitError`

**Files:**
- Create: `crates/profiles/src/limits/mod.rs`
- Modify: `crates/profiles/src/lib.rs` (add `pub mod limits;` + re-exports)
- Modify: `crates/profiles/Cargo.toml` (add `serde` with `derive`, `thiserror`)

**Interfaces:**
- Produces:
  - `struct Limits` (`Clone`, `Debug`, `PartialEq`, `serde::Deserialize`, `serde::Serialize`) with fields, each carrying a doc-comment naming its Pyroscope analog:
    - `ingestion_rate_profiles_per_sec: f64` (Pyroscope `ingestion_rate_mb` analog, counted in profiles not MB; `0.0` ⇒ unlimited)
    - `ingestion_burst_profiles: u64` (Pyroscope `ingestion_burst_size_mb` analog)
    - `max_series: u64` (Pyroscope `max_series` — active series per tenant; `0` ⇒ unlimited)
    - `max_label_name_length: u64` (Pyroscope `max_label_name_length`; `0` ⇒ unlimited)
    - `max_label_value_length: u64` (Pyroscope `max_label_value_length`; `0` ⇒ unlimited)
    - `max_label_names_per_series: u64` (Pyroscope `max_label_names_per_series`; `0` ⇒ unlimited)
    - `max_flamegraph_nodes_default: i64` (Pyroscope `max_flamegraph_nodes_default`, the `max_nodes` applied when a request omits it; mirrors `EngineOpts::default_max_nodes` = **2048**)
    - `max_flamegraph_nodes_max: i64` (Pyroscope `max_flamegraph_nodes_max`, the hard ceiling a request's `max_nodes` is clamped to; `0` ⇒ unlimited)
    - `max_query_length_secs: u64` (Pyroscope `max_query_length`; the `(end-start)` ceiling for the query methods; `0` ⇒ unlimited)
    - `max_session_id_cardinality: u64` (the `__session_id__` modulo-hash bucket count — the per-tenant distinct-session cap; `0` ⇒ unlimited)
  - `impl Default for Limits` — generous Pyroscope-default-ish values (`ingestion_rate_profiles_per_sec: 10_000.0`, `ingestion_burst_profiles: 10_000`, `max_series: 0` ⇒ unlimited, `max_label_name_length: 1024`, `max_label_value_length: 2048`, `max_label_names_per_series: 40`, `max_flamegraph_nodes_default: 2048`, `max_flamegraph_nodes_max: 0` ⇒ unlimited, `max_query_length_secs: 0`, `max_session_id_cardinality: 0`).
  - `enum LimitError` (`thiserror`) with the variants below, each carrying the over-limit value + the cap, and each mapping to a Pyroscope Connect code + HTTP status:
    - `IngestionRateExceeded { rate: f64, observed: f64 }` → Connect `resource_exhausted` / HTTP **429**
    - `MaxSeries { limit: u64, observed: u64 }` → Connect `resource_exhausted` / **429**
    - `LabelNameTooLong { limit: u64, observed: u64 }` → Connect `invalid_argument` / **400**
    - `LabelValueTooLong { limit: u64, observed: u64 }` → Connect `invalid_argument` / **400**
    - `TooManyLabels { limit: u64, observed: u64 }` → Connect `invalid_argument` / **400**
    - `QueryLengthExceeded { limit_secs: u64, observed_secs: u64 }` → Connect `invalid_argument` / **400**
    - `SessionCardinalityExceeded { limit: u64 }` → Connect `resource_exhausted` / **429**
  - `impl LimitError { pub fn connect_code(&self) -> &'static str; pub fn http_status(&self) -> u16; pub fn message(&self) -> String }` — `connect_code` is the Connect error-code string (`"resource_exhausted"` / `"invalid_argument"`); `http_status` is the Connect-over-HTTP status Pyroscope returns; `message` is the human string Pyroscope puts in the error envelope (e.g. `"label value too long (2048)"`). **Pin the exact code strings + messages against the real Pyroscope container in Task 9** — until then use the structurally-correct values here and note the verify-against-rev in the doc-comment.

> **Pyroscope status mapping note:** Pyroscope's Connect API returns gRPC-status codes mapped to HTTP by the Connect protocol: `resource_exhausted` → 429 (the *rate*/cardinality/series caps), `invalid_argument` → 400 (the *length*/range caps). The `max_nodes` cap is **not** an error — a request's `max_nodes` is silently **clamped** to `[1, max_flamegraph_nodes_max]` (and defaulted to `max_flamegraph_nodes_default` when 0/omitted), mirroring Pyroscope's clamp-don't-reject behavior; the clamp helper lives in Task 3, not as a `LimitError`. The differential suite (Task 9) verifies the real codes.

- [ ] **Step 1: Write the failing test**

Create `crates/profiles/src/limits/mod.rs` with only a `tests` module:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn default_limits_are_generous_and_finite() {
        let l = Limits::default();
        assert!(l.ingestion_rate_profiles_per_sec > 0.0);
        assert!(l.max_label_value_length == 2048);
        assert!(l.max_label_names_per_series == 40);
        assert!(l.max_flamegraph_nodes_default == 2048);
        assert!(l.max_query_length_secs == 0); // unlimited by default
    }

    #[test]
    fn limit_errors_carry_pyroscope_code_and_status() {
        let rate = LimitError::IngestionRateExceeded { rate: 10_000.0, observed: 12_000.0 };
        assert!(rate.http_status() == 429);
        assert!(rate.connect_code() == "resource_exhausted");

        let name = LimitError::LabelNameTooLong { limit: 1024, observed: 5000 };
        assert!(name.http_status() == 400);
        assert!(name.connect_code() == "invalid_argument");

        let many = LimitError::TooManyLabels { limit: 40, observed: 41 };
        assert!(many.http_status() == 400);

        let dur = LimitError::QueryLengthExceeded { limit_secs: 3600, observed_secs: 7200 };
        assert!(dur.http_status() == 400);

        let card = LimitError::SessionCardinalityExceeded { limit: 1000 };
        assert!(card.http_status() == 429);
    }

    #[test]
    fn limit_error_message_names_the_cap() {
        let v = LimitError::LabelValueTooLong { limit: 2048, observed: 5000 };
        assert!(v.message().contains("2048"));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib limits`
Expected: FAIL — `cannot find type Limits`.

- [ ] **Step 3: Implement `Limits` + `LimitError`**

Prepend above `tests`. Define `Limits` with the fields/`Default` above, and `LimitError` with `thiserror`, the carried fields, and the three `impl` methods. Map codes/statuses exactly: `IngestionRateExceeded`/`MaxSeries`/`SessionCardinalityExceeded` ⇒ `resource_exhausted`/429; everything else ⇒ `invalid_argument`/400. `message()` formats the cap into the string (verify-against-Pyroscope note in the doc-comment). Add `serde` (with `derive`) + `thiserror` to `Cargo.toml`.

- [ ] **Step 4: Wire into `lib.rs`** — `pub mod limits;` and `pub use limits::{Limits, LimitError};`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-profiles --lib limits`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): per-tenant Limits model + Pyroscope-shaped LimitError"
```

---

### Task 2: `OverridesProvider` — Pyroscope-style `overrides.yaml`

**Files:**
- Create: `crates/profiles/src/limits/overrides.rs`
- Modify: `crates/profiles/src/limits/mod.rs` (declare submodule + re-export)
- Modify: `crates/profiles/Cargo.toml` (add `serde_yaml`)

**Interfaces:**
- Consumes: `Limits` (Task 1), `serde_yaml`.
- Produces:
  - `struct OverridesProvider { defaults: Limits, per_tenant: HashMap<String, Limits> }` (`Clone`, `Debug`).
  - `impl OverridesProvider`:
    - `pub fn new(defaults: Limits) -> Self`
    - `pub fn from_yaml(yaml: &str) -> Result<Self, OverridesError>` — parse the Pyroscope per-tenant-override shape (top-level `overrides: { <tenant>: { …partial Limits… } }`); a tenant's entry overrides only the fields it names, the rest fall back to `defaults`.
    - `pub fn for_tenant(&self, tenant: &str) -> &Limits` — returns the tenant's resolved `Limits`, or `&self.defaults` if unlisted.
  - `enum OverridesError` (`thiserror`): `Yaml(String)`.

> **Pyroscope runtime parity:** Pyroscope's per-tenant override file keys limits under `overrides:` per-tenant and merges over the static config defaults. We model defaults as a struct (not a second YAML layer) and let each tenant's YAML map be a *partial* `Limits` via an internal `PartialLimits` mirror (every field `Option<…>`, `#[serde(default)]`) that then merges field-by-field onto `defaults`. (The no-back-compat rule bans `#[serde(default)]` used as a *compat* shim for old schemas; using it to express "this tenant only overrides some fields" is a legitimate partial-config pattern, not a migration. Note this in a code comment so a future reader doesn't flag it.)

- [ ] **Step 1: Write the failing test**

Create `crates/profiles/src/limits/overrides.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    const YAML: &str = r#"
overrides:
  tenant-a:
    ingestion_rate_profiles_per_sec: 500
    max_series: 1000
  tenant-b:
    max_label_value_length: 64
"#;

    #[test]
    fn tenant_override_merges_over_defaults() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        let a = p.for_tenant("tenant-a");
        assert!(a.ingestion_rate_profiles_per_sec == 500.0);
        assert!(a.max_series == 1000);
        // unspecified field falls back to default
        assert!(a.max_label_value_length == Limits::default().max_label_value_length);
    }

    #[test]
    fn partial_override_keeps_other_defaults() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        let b = p.for_tenant("tenant-b");
        assert!(b.max_label_value_length == 64);
        assert!(b.ingestion_rate_profiles_per_sec == Limits::default().ingestion_rate_profiles_per_sec);
    }

    #[test]
    fn unlisted_tenant_gets_defaults() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        assert!(*p.for_tenant("tenant-z") == Limits::default());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib overrides`
Expected: FAIL — `cannot find type OverridesProvider`.

- [ ] **Step 3: Implement `overrides.rs`**

Define an internal `#[derive(Deserialize)] struct PartialLimits` with every field `Option<…>` (and `#[serde(default)]`), a `struct RuntimeFile { #[serde(default)] overrides: HashMap<String, PartialLimits> }`, `from_yaml` parsing into that and merging each `PartialLimits` onto a clone of `defaults` (a `fn merge(base: &Limits, p: &PartialLimits) -> Limits`). `for_tenant` returns the precomputed resolved `Limits`. Map `serde_yaml::Error` → `OverridesError::Yaml(e.to_string())`.

- [ ] **Step 4: Wire into `mod.rs`** — `mod overrides; pub use overrides::{OverridesError, OverridesProvider};` and re-export from `lib.rs`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-profiles --lib overrides`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): Pyroscope-style overrides.yaml OverridesProvider"
```

---

### Task 3: Limit enforcement — ingest side (rate + series + labels + `__session_id__` cardinality), query side (range + `max_nodes` clamp)

**Files:**
- Create: `crates/profiles/src/limits/enforce.rs`
- Modify: `crates/profiles/src/limits/mod.rs` (declare submodule + re-export)
- Modify: `crates/profiles/Cargo.toml` (add `crabka-broker` path dep for `TokenBucket` + `dashmap`)

**Interfaces:**
- Consumes: `Limits`, `LimitError` (Task 1); `crabka_broker::throttle::TokenBucket`; the Slice 4 `ProfileRecord` series `Labels` (read its label pairs for the length/count check + the `__session_id__` value for the cardinality cap).
- Produces:
  - `struct IngestEnforcer` holding a `DashMap<String /*tenant*/, Arc<TokenBucket>>` for the per-tenant ingest-rate bucket and a `DashMap<String /*tenant*/, Arc<TokenBucket>>` for the per-tenant `__session_id__` cardinality bucket (a distinct-session counter approximated by a token bucket sized to `max_session_id_cardinality`).
  - `impl IngestEnforcer`:
    - `pub fn new() -> Self`
    - `pub fn check_profile_rate(&self, limits: &Limits, tenant: &str, n_profiles: u64) -> Result<(), LimitError>` — `0.0` rate ⇒ Ok; else get-or-create the tenant bucket at `ingestion_rate_profiles_per_sec` (rounded to `u64`, burst `ingestion_burst_profiles`) via `set_rate` on creation, `try_consume(n_profiles)`; if granted `< n_profiles` ⇒ `IngestionRateExceeded`.
    - `pub fn check_labels(limits: &Limits, labels: &[(String, String)]) -> Result<(), LimitError>` — any label name longer than `max_label_name_length` ⇒ `LabelNameTooLong`; value too long ⇒ `LabelValueTooLong`; more than `max_label_names_per_series` pairs ⇒ `TooManyLabels` (each only when its cap is nonzero). (Associated fn — no state.)
    - `pub fn check_active_series(limits: &Limits, would_add: u64, current: u64) -> Result<(), LimitError>` — `current + would_add > max_series` (when nonzero) ⇒ `MaxSeries`. (Associated fn — no state; `current` is supplied by the distributor's per-tenant series counter.)
    - `pub fn check_session_cardinality(&self, limits: &Limits, tenant: &str, session_bucket: u64) -> Result<(), LimitError>` — `0` cap ⇒ Ok; else gate the modulo-hashed `__session_id__` bucket against the tenant's cardinality `TokenBucket` (one token per *new* bucket seen this window); exhausted ⇒ `SessionCardinalityExceeded`. (The distributor computes `session_bucket = hash(session_id) % max_session_id_cardinality` before calling — the modulo-hash cap from spec §4.3; this check enforces the per-window distinct-bucket budget.)
  - `struct QueryEnforcer` (stateless; associated fns):
    - `pub fn check_query_length(limits: &Limits, start_ms: i64, end_ms: i64) -> Result<(), LimitError>` — `(end_ms - start_ms) / 1000 > max_query_length_secs` (when nonzero) ⇒ `QueryLengthExceeded`.
    - `pub fn clamp_max_nodes(limits: &Limits, requested: i64) -> i64` — `requested <= 0` ⇒ `max_flamegraph_nodes_default`; else `min(requested, max_flamegraph_nodes_max)` when the max is nonzero, otherwise `requested`. **Returns a value, never an error** (Pyroscope clamps, it does not reject — see Task 1 note).

> **TokenBucket reuse note:** `crabka_broker::throttle::TokenBucket` is the KIP-73 bucket (`new()`, `set_rate(u64)` seeds a one-second burst at the new rate, `try_consume(u64) -> u64` granted; rate-0 grants the full request). It meters in whatever integer unit you set the rate in. For ingest rate the unit is *profiles*, rate = `ingestion_rate_profiles_per_sec` rounded to `u64`, `set_rate(burst)` seeds the burst (set the rate to `ingestion_burst_profiles` on creation so the first burst is `ingestion_burst_profiles`, then steady refill is `ingestion_rate_profiles_per_sec`/sec — if `TokenBucket` couples burst==rate, seed at `max(rate, burst)` and document the approximation). For the `__session_id__` cardinality cap the unit is *distinct session buckets*: seed the bucket at `max_session_id_cardinality` and `try_consume(1)` per newly-seen bucket; this approximates a windowed distinct-count cap with the same machinery (note the approximation in a code comment — an exact HLL is out of scope). If `crabka-broker` is too heavy/cyclic a dep, lift `throttle/bucket.rs` into a tiny `crabka-throttle` crate and depend on that from both — but **prefer the path dep** unless a cycle appears, and note the choice in the commit. The pure arithmetic (`plan_consume`) is already unit-tested in the broker, so this task tests only the *mapping* (limit → bucket config → decision), not the bucket math.

- [ ] **Step 1: Write the failing test**

Create `crates/profiles/src/limits/enforce.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::limits::Limits;

    fn limits_with(name_len: u64, val_len: u64, max_labels: u64) -> Limits {
        Limits { max_label_name_length: name_len, max_label_value_length: val_len,
                 max_label_names_per_series: max_labels, ..Limits::default() }
    }

    #[test]
    fn label_caps_enforced() {
        let l = limits_with(4, 4, 2);
        let ok = vec![("ab".to_string(), "cd".to_string())];
        let bad_name = vec![("toolong".to_string(), "x".to_string())];
        let bad_val = vec![("a".to_string(), "toolong".to_string())];
        let too_many = vec![("a".into(), "b".into()), ("c".into(), "d".into()), ("e".into(), "f".into())];
        assert!(IngestEnforcer::check_labels(&l, &ok).is_ok());
        assert!(matches!(IngestEnforcer::check_labels(&l, &bad_name),
                         Err(LimitError::LabelNameTooLong { .. })));
        assert!(matches!(IngestEnforcer::check_labels(&l, &bad_val),
                         Err(LimitError::LabelValueTooLong { .. })));
        assert!(matches!(IngestEnforcer::check_labels(&l, &too_many),
                         Err(LimitError::TooManyLabels { .. })));
    }

    #[test]
    fn active_series_cap_rejects_over_limit() {
        let l = Limits { max_series: 100, ..Limits::default() };
        assert!(IngestEnforcer::check_active_series(&l, 1, 99).is_ok());   // 99+1 == 100, ok
        assert!(matches!(IngestEnforcer::check_active_series(&l, 1, 100),
                         Err(LimitError::MaxSeries { .. })));               // > 100
        let unlimited = Limits { max_series: 0, ..Limits::default() };
        assert!(IngestEnforcer::check_active_series(&unlimited, 1_000_000, 5_000_000).is_ok());
    }

    #[test]
    fn ingest_rate_bucket_eventually_rejects() {
        let e = IngestEnforcer::new();
        let l = Limits {
            ingestion_rate_profiles_per_sec: 100.0,
            ingestion_burst_profiles: 100,
            ..Limits::default()
        };
        assert!(e.check_profile_rate(&l, "t", 100).is_ok());   // first burst
        assert!(e.check_profile_rate(&l, "t", 100).is_err());  // budget exhausted, no refill yet
    }

    #[test]
    fn session_cardinality_bucket_caps_distinct_sessions() {
        let e = IngestEnforcer::new();
        let l = Limits { max_session_id_cardinality: 2, ..Limits::default() };
        assert!(e.check_session_cardinality(&l, "t", 0).is_ok());
        assert!(e.check_session_cardinality(&l, "t", 1).is_ok());
        // third distinct bucket this window exceeds the cap of 2
        assert!(matches!(e.check_session_cardinality(&l, "t", 2),
                         Err(LimitError::SessionCardinalityExceeded { .. })));
    }

    #[test]
    fn query_length_cap_and_max_nodes_clamp() {
        let l = Limits { max_query_length_secs: 3600, max_flamegraph_nodes_default: 2048,
                         max_flamegraph_nodes_max: 8192, ..Limits::default() };
        let ms = 1_000_000_000_000_i64;
        assert!(matches!(QueryEnforcer::check_query_length(&l, ms, ms + 7_200_000),
                         Err(LimitError::QueryLengthExceeded { .. })));   // 2h > 1h
        assert!(QueryEnforcer::check_query_length(&l, ms, ms + 1_800_000).is_ok()); // 30m ok
        assert!(QueryEnforcer::clamp_max_nodes(&l, 0) == 2048);           // default applied
        assert!(QueryEnforcer::clamp_max_nodes(&l, 100) == 100);          // under ceiling
        assert!(QueryEnforcer::clamp_max_nodes(&l, 99_999) == 8192);      // clamped to max
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib enforce`
Expected: FAIL — `cannot find type IngestEnforcer`.

- [ ] **Step 3: Implement `enforce.rs`**

Implement `IngestEnforcer` (with the two `DashMap` bucket caches, `new()`), `QueryEnforcer`, and the seven methods exactly per the interfaces. For `check_profile_rate`/`check_session_cardinality`: round the rate/cap to `u64`; get-or-create the tenant's `TokenBucket`, `set_rate` on creation; `try_consume(n)`; granted `< n` ⇒ error. For `clamp_max_nodes` apply the default-then-clamp logic. Add `crabka-broker` (path) + `dashmap` to `Cargo.toml`.

- [ ] **Step 4: Wire into `mod.rs`** — `mod enforce; pub use enforce::{IngestEnforcer, QueryEnforcer};` + re-export from `lib.rs`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-profiles --lib enforce`
Expected: PASS (5 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): per-tenant limit enforcement (ingest rate/series/labels/session-card + query caps)"
```

---

### Task 4: `LimitError` → Pyroscope Connect envelope + enforcement wired into the live write/read paths

**Files:**
- Create: `crates/profiles/src/http/error.rs` (the `LimitError` → `Response` projection)
- Modify: the distributor write handler (Slice 4) — call `IngestEnforcer` before WAL append.
- Modify: the querier Connect handlers (Slice 5) + the query-frontend entry (Slice 6) — call `QueryEnforcer` (query-length check + `max_nodes` clamp) in `SelectMergeStacktraces` / `SelectSeries` / `Diff`.
- Modify: server boot to accept an `OverridesProvider`.
- Create: `crates/profiles/tests/limits_overrides.rs` (drives limits through real Connect HTTP).
- Modify: `crates/profiles/Cargo.toml` (`[dev-dependencies]` `reqwest`, `tokio`, `serde_json`).

**Interfaces:**
- Consumes: Task 1 `LimitError`, Task 2 `OverridesProvider`, Task 3 enforcers, the Slice 5 Connect error envelope, the Task 5 server boot.
- Produces:
  - `http/error.rs`: `pub fn limit_error_response(err: &LimitError) -> axum::response::Response` — Connect-over-HTTP status from `err.http_status()`, body in the **Pyroscope Connect** error shape (`{ "code": "<connect_code>", "message": "<message>" }`, the same envelope the Slice 5 router already emits for other errors; reuse it, don't fork). `resource_exhausted`/429 carries the retriable semantics Pyroscope uses.
  - Enforcement at the live edges + a test asserting **Pyroscope-shaped error bodies** end-to-end:
    - over-long label-name/value push → `400` (`invalid_argument`).
    - over-`max_label_names_per_series` push → `400`.
    - over-rate push → `429` (`resource_exhausted`).
    - `SelectMergeStacktraces` with `(end-start)` > `max_query_length` → `400`.
    - `SelectMergeStacktraces` with a huge `max_nodes` → `200` (clamped, NOT an error — assert the returned flamegraph has `<= max_flamegraph_nodes_max` distinct nodes).

> **Contract gap note:** the exact Pyroscope Connect error-body *strings* + codes are pinned against the real container in Task 9; here assert `(status, connect-code field, descriptive substring)`, not byte-equality with a guessed message. The status/code mapping is the firm contract; the message strings firm up after the differential run.

- [ ] **Step 1: Write the failing test**

Create `crates/profiles/tests/limits_overrides.rs`. Boot the server with an `OverridesProvider` carrying tight caps for `tenant-tight`; drive each over-limit case over real Connect HTTP and assert `(status, body code/error)`:

```rust
mod support;
use assert2::check;
use support::profiles_server as srv;

#[tokio::test]
async fn over_limit_requests_return_pyroscope_shaped_errors() {
    let overrides = r#"
overrides:
  tenant-tight:
    ingestion_rate_profiles_per_sec: 1
    ingestion_burst_profiles: 1
    max_label_value_length: 4
    max_label_names_per_series: 2
    max_query_length_secs: 60
    max_flamegraph_nodes_max: 16
"#;
    let s = srv::start_in_process_with_overrides(overrides).await;

    // over-long label value -> 400 invalid_argument
    let (st, body) = srv::push_pprof_expect_error(
        &s.base_url, "tenant-tight",
        &srv::profile_with_label("x", "toolongvalue")).await;
    check!(st == 400 && body.get("code").map(|c| c == "invalid_argument").unwrap_or(false));

    // too many labels -> 400
    let (st, _) = srv::push_pprof_expect_error(
        &s.base_url, "tenant-tight",
        &srv::profile_with_n_labels(3)).await;
    check!(st == 400);

    // query range > 60s -> 400
    let (st, _) = srv::select_merge_expect_error(
        &s.base_url, "tenant-tight",
        "process_cpu:cpu:nanoseconds:cpu:nanoseconds", "{}", 0, 3_600_000, 2048).await;
    check!(st == 400);

    // huge max_nodes -> 200 (clamped, not rejected)
    let fg = srv::select_merge(
        &s.base_url, "tenant-tight",
        "process_cpu:cpu:nanoseconds:cpu:nanoseconds", "{}", 0, 60_000, 1_000_000).await;
    check!(srv::flamegraph_node_count(&fg) <= 16);

    // burst then over-rate -> 429 resource_exhausted
    let _ = srv::push_pprof(&s.base_url, "tenant-tight", &srv::tiny_profile()).await;
    let (st, body) = srv::push_pprof_expect_error(
        &s.base_url, "tenant-tight", &srv::tiny_profile()).await;
    check!(st == 429 && body.get("code").map(|c| c == "resource_exhausted").unwrap_or(false));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --test limits_overrides`
Expected: FAIL — `support::profiles_server` / enforcement not wired; over-limit requests currently succeed. (The `support::profiles_server` boot helper is authored in Task 5 Step 1; cross-reference noted — write that helper first, or stub a minimal boot here and converge.)

- [ ] **Step 3: Implement `http/error.rs` + wire enforcement into the live handlers**

Implement `limit_error_response`. Call `IngestEnforcer::check_labels` + `check_active_series` + `check_session_cardinality` + `check_profile_rate` in the distributor pre-WAL hook; `QueryEnforcer::check_query_length` + `clamp_max_nodes` in the `SelectMergeStacktraces`/`SelectSeries`/`Diff` Connect handlers (and the query-frontend entry). Convert `LimitError` → response via `limit_error_response`. Thread the `OverridesProvider` from server boot into both hooks.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-profiles --test limits_overrides`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): enforce per-tenant limits at live write/read edges with Pyroscope error bodies"
```

---

### Task 5: **Headline** — multi-tenant isolation end-to-end through the Connect `querier.v1` API

**Files:**
- Create: `crates/profiles/tests/support/profiles_server.rs` (shared in-process boot + two-tenant seed + pprof push/query helpers)
- Create: `crates/profiles/tests/tenant_isolation.rs`
- Modify: `crates/profiles/Cargo.toml` (`[dev-dependencies]` `reqwest`, `tokio`, `serde_json`)

**Interfaces:**
- `support::profiles_server`:
  - `pub async fn start_in_process() -> TestServer` — boots the profiles service (distributor + block-builder + querier roles in-process, in-memory/tempdir WAL + blockstore, the Slice 5 Connect router) on an ephemeral port; returns `{ base_url, _guard }`.
  - `pub async fn start_in_process_with_overrides(yaml: &str) -> TestServer` — same, with an `OverridesProvider` loaded from `yaml` (used by Task 4).
  - `pub async fn push_pprof(base: &str, tenant: &str, profile: &ProfileFixture)` — `POST /push.v1.PusherService/Push` (gzipped-pprof `raw_profile`) for `tenant` via `X-Scope-OrgID`; builds the `PushRequest` via the Slice 2 `PprofProfile::encode` + the Slice 4/5 prost `push.v1` types.
  - builders `profile_with_label(k, v)`, `profile_with_n_labels(n)`, `tiny_profile()`, `profile(profile_type, labels, stacks)` returning `ProfileFixture` — deterministic pprof fixtures (a fixed `process_cpu` profile type, a small known stacktrace set).
  - read helpers, each issuing the Connect request with the tenant header and returning parsed JSON: `profile_types(base, tenant, start, end)`, `label_names(base, tenant, matchers, start, end)`, `label_values(base, tenant, name, matchers, start, end)`, `select_merge(base, tenant, profile_type, selector, start, end, max_nodes)`, `select_series(base, tenant, profile_type, selector, group_by, step, start, end)`.
  - error variants `push_pprof_expect_error(...) -> (u16, serde_json::Value)`, `select_merge_expect_error(...) -> (u16, serde_json::Value)` (used by Task 4) + `flamegraph_node_count(fg) -> usize`.

> **Contract gap note:** if the Slice 4/5 in-process boot isn't available, this helper assembles it from the public role constructors `crabka-profiles` exposes; if those are absent, the task spins the Connect `Router` directly over an in-memory `CrabkaProfileStore` (hot-store only) + `FlameEngine` and drives writes through the distributor entry fn. Either way: **real Connect-RPC over a real socket** (so `X-Scope-OrgID` goes through the genuine extractor), not a function-call shortcut — the whole point is to exercise the tenancy boundary as Grafana would.

- [ ] **Step 1: Write the failing isolation test**

Create `crates/profiles/tests/tenant_isolation.rs`. Seed **two** tenants with *deliberately colliding* series identity (same service name + same profile type, plus a tenant-A-only label and a tenant-A-only function name in the stacktrace set), then assert A cannot see B and vice versa across **every** read surface:

```rust
mod support;
use assert2::{assert, check};
use support::profiles_server::{self as srv};

#[tokio::test]
async fn tenants_are_fully_isolated_across_all_read_surfaces() {
    let s = srv::start_in_process().await;

    const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

    // SAME service + profile type in both tenants; A has a unique label + a unique frame.
    let a_profile = srv::profile(PT,
        &[("service_name", "checkout"), ("tenant_only", "A")],
        &[("main;a_only_fn", 100)]);
    let b_profile = srv::profile(PT,
        &[("service_name", "checkout")],
        &[("main;b_fn", 999)]);
    srv::push_pprof(&s.base_url, "tenant-a", &a_profile).await;
    srv::push_pprof(&s.base_url, "tenant-b", &b_profile).await;

    let (lo, hi) = (0_i64, i64::MAX / 2);

    // 1) ProfileTypes: each tenant sees the type (and the health probe is per-tenant).
    let pta = srv::profile_types(&s.base_url, "tenant-a", lo, hi).await;
    let ptb = srv::profile_types(&s.base_url, "tenant-b", lo, hi).await;
    check!(type_list(&pta).contains(&PT.to_string()));
    check!(type_list(&ptb).contains(&PT.to_string()));

    // 2) LabelNames: A has the `tenant_only` label, B does NOT.
    let lna = srv::label_names(&s.base_url, "tenant-a", "{}", lo, hi).await;
    let lnb = srv::label_names(&s.base_url, "tenant-b", "{}", lo, hi).await;
    check!(name_list(&lna).contains(&"tenant_only".to_string()));
    check!(!name_list(&lnb).contains(&"tenant_only".to_string()));

    // 3) LabelValues: B never sees A's `tenant_only=A`.
    let lva = srv::label_values(&s.base_url, "tenant-b", "tenant_only", "{}", lo, hi).await;
    check!(value_list(&lva).is_empty());

    // 4) SelectMergeStacktraces: A's flamegraph contains `a_only_fn`, B's never does
    //    (and vice versa for `b_fn`) — even though the service/profile-type collide.
    let fga = srv::select_merge(&s.base_url, "tenant-a", PT, "{}", lo, hi, 2048).await;
    let fgb = srv::select_merge(&s.base_url, "tenant-b", PT, "{}", lo, hi, 2048).await;
    check!(frame_names(&fga).iter().any(|n| n.contains("a_only_fn")));
    check!(!frame_names(&fgb).iter().any(|n| n.contains("a_only_fn")));
    check!(frame_names(&fgb).iter().any(|n| n.contains("b_fn")));

    // 5) LabelValues select on the A-only label returns nothing through a B query.
    let qb = srv::select_merge(&s.base_url, "tenant-b", PT, "{ tenant_only=\"A\" }", lo, hi, 2048).await;
    assert!(srv::flamegraph_node_count(&qb) == 0 || frame_names(&qb).is_empty());
}
```

(Provide the small `type_list`/`name_list`/`value_list`/`frame_names` helpers in the test file or `support`.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --test tenant_isolation`
Expected: FAIL — `support::profiles_server` / boot not yet present (or, if present, a real leak surfaces — fix it).

- [ ] **Step 3: Implement `support::profiles_server` + fix any leak**

Build the boot/seed/push/read helpers. Run the test. **If it reveals a real isolation leak** (a `ProfileIndex`/hot-store/block/symbol-DB object key that isn't tenant-prefixed), fix the offending key in the earlier-slice code (the isolation boundary is the product requirement; this test is its enforcement) and note the fix in the commit. The colliding-`service_name`+profile-type case is the sharpest probe: it forces the `ProfileIndex` label-postings + symbol-DB resolution to be tenant-scoped *before* the fold, not after — A's `a_only_fn` must never leak into B's flamegraph through a shared symbol partition.

- [ ] **Step 4: Add a per-tenant quota-isolation assertion**

Append a test: boot with an `OverridesProvider` setting a tiny `ingestion_rate_profiles_per_sec` for `tenant-a` only, push enough to throttle A (expect `429`), and confirm `tenant-b` is unaffected at the same instant. Proves quotas are bucketed per tenant.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-profiles --test tenant_isolation`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "test(profiles): headline multi-tenant isolation across all read surfaces + per-tenant quota"
```

---

### Task 6: Compactor — cross-block symbol-DB re-dedup (`MergedSymbols`)

**Files:**
- Create: `crates/profiles/src/compactor/mod.rs` (module decl + re-exports; `compact_blocks` lands in Task 7)
- Create: `crates/profiles/src/compactor/symbols.rs`
- Modify: `crates/profiles/src/lib.rs` (add `pub mod compactor;`)
- Modify: `crates/profiles/Cargo.toml` (ensure `crabka-pprof` + `crabka-blockstore` path deps present)

**Interfaces:**
- Consumes: Slice 1/2 `SymbolDb` (`intern_stacktrace(partition, &[u32]) -> u32`, `resolve(partition, id) -> Vec<Frame>`, `encode`/`decode`); `Frame`; `ProfileError`.
- Produces (in `symbols.rs`):
  - `pub struct MergedSymbols { merged: SymbolDb, /* per-input remap tables */ }`.
  - `impl MergedSymbols`:
    - `pub fn new() -> Self` — an empty target `SymbolDb`.
    - `pub fn absorb(&mut self, input: &SymbolDb) -> StacktraceRemap` — re-intern every `(partition, stacktrace_id)` reachable in `input` into the merged DB (resolving `input` leaf→root via `resolve`, then `intern_stacktrace` into the merged DB so identical cross-block stacks collapse to one path), returning a `StacktraceRemap` that maps each `input` `(partition, old_id)` → the merged `(partition, new_id)`.
    - `pub fn finish(self) -> SymbolDb` — the deduplicated merged symbol DB.
  - `pub struct StacktraceRemap { /* (partition,u64) -> (partition,u64) */ }` with `pub fn map(&self, partition: u64, old_id: u32) -> (u64, u32)`.
- **Why this exists:** two input blocks each carry their own symbol DB; concatenating their samples fact tables (Task 7) would leave each row pointing at its *original* block's `stacktrace_id`, which is meaningless in the merged block. `MergedSymbols::absorb` produces the remap that Task 7 applies to `PCOL_STACKTRACE_ID`/`PCOL_STACKTRACE_PARTITION`, and the re-intern is exactly where cross-block string/function/location/stacktrace dedup happens (the spec §13 "symbol-DB dedup scope" lever).

> **Contract gap note:** if Slice 2's `SymbolDb` does not yet expose an iterator over all `(partition, id)` pairs, add a minimal `pub fn stacktrace_ids(&self) -> impl Iterator<Item = (u64, u32)>` to `crabka-pprof` (small, in-scope addition to the symbol model) and note it. The remap must be driven by the *actual* ids referenced by the input's samples fact table (pass them in if the DB can't enumerate) — never re-intern ids no row references.

- [ ] **Step 1: Write the failing test**

Create `crates/profiles/src/compactor/symbols.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pprof::{Frame, SymbolDb};

    use super::*;

    fn frame(name: &str) -> Frame {
        Frame { function: name.to_string(), file: String::new(), line: 0 }
    }

    #[test]
    fn identical_cross_block_stacks_collapse_to_one_path() {
        // Two input DBs that share the SAME stack on partition 0.
        let mut a = SymbolDb::new();
        let mut b = SymbolDb::new();
        // (Each DB interns the same leaf-first frame set; ids may differ per DB.)
        let a_id = intern(&mut a, 0, &[frame("leaf"), frame("root")]);
        let b_id = intern(&mut b, 0, &[frame("leaf"), frame("root")]);

        let mut m = MergedSymbols::new();
        let ra = m.absorb(&a);
        let rb = m.absorb(&b);
        let merged = m.finish();

        let (pa, na) = ra.map(0, a_id);
        let (pb, nb) = rb.map(0, b_id);
        // The shared stack dedups to one merged id.
        assert!((pa, na) == (pb, nb));
        // And resolving the merged id yields the original frames leaf-first.
        assert!(frame_names(&merged.resolve(pa, na)) == vec!["leaf", "root"]);
    }

    #[test]
    fn distinct_stacks_keep_distinct_ids() {
        let mut a = SymbolDb::new();
        let x = intern(&mut a, 0, &[frame("x"), frame("root")]);
        let y = intern(&mut a, 0, &[frame("y"), frame("root")]);
        let mut m = MergedSymbols::new();
        let r = m.absorb(&a);
        assert!(r.map(0, x) != r.map(0, y));
    }

    // helpers: `intern` builds the location refs then calls intern_stacktrace;
    // `frame_names` extracts function strings. Implement against the Slice 2 API.
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib compactor::symbols`
Expected: FAIL — `cannot find type MergedSymbols`.

- [ ] **Step 3: Implement `symbols.rs`**

Implement `MergedSymbols`/`StacktraceRemap`. `absorb` iterates the input's referenced `(partition, id)` set, `resolve`s each to `Vec<Frame>` in the input DB, re-interns the frame set into the merged DB (building the merged DB's location/function/string tables + parent-pointer tree via `intern_stacktrace`), and records `(partition, old_id) -> (partition, new_id)`. Same partition number is preserved (the partition is the `stacktrace_partition` value, stable across the merge). Add the `compactor` module decl to `mod.rs`/`lib.rs`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-profiles --lib compactor::symbols`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): compactor cross-block symbol-DB re-dedup (MergedSymbols + remap)"
```

---

### Task 7: Compactor — block merge (vertical+horizontal) + downsampling + `compact_blocks`

**Files:**
- Create: `crates/profiles/src/compactor/downsample.rs`
- Modify: `crates/profiles/src/compactor/mod.rs` (the `compact_blocks` entry + `CompactionPlan`/`DownsampleStep`)
- Create: `crates/profiles/tests/compaction.rs` (in-process query-equality)
- Modify: `crates/profiles/Cargo.toml` (ensure `datafusion`, `object_store`, `arrow`, `tempfile` deps present)

**Interfaces:**
- Consumes: Task 6 `MergedSymbols`/`StacktraceRemap`; Slice 1 `BlockStore`/`BlockWriter`/`BlockMeta`/`ProfileIndex`, `PCOL_*` constants + samples schema (`COL_FINGERPRINT`, `COL_TIMESTAMP`, `PCOL_PROFILE_TYPE`, `PCOL_STACKTRACE_ID`, `PCOL_VALUE`, `PCOL_STACKTRACE_PARTITION`, `PCOL_TOTAL_VALUE`, `PCOL_SPAN_ID`, `PCOL_TRACE_ID`); the DataFusion `SessionContext` for the concatenate/group-by fold.
- Produces (in `downsample.rs`):
  - `pub enum DownsampleStep { None, Min5, Hour1 }` with `pub fn bucket_ns(self) -> i64` (`0` / `300_000_000_000` / `3_600_000_000_000`).
  - `pub fn downsample_timestamp(ts_ns: i64, step: DownsampleStep) -> i64` — floor-bucket `ts_ns` to the step boundary (`step == None` ⇒ identity).
- Produces (in `mod.rs`):
  - `pub struct CompactionPlan { pub inputs: Vec<BlockMeta>, pub downsample: DownsampleStep, pub target_window_hours: u32 /* 4 or 8 */ }`.
  - `pub async fn compact_blocks(store: &BlockStore, tenant: &str, plan: &CompactionPlan) -> Result<BlockMeta, ProfileError>` — the full merge:
    1. read each input block's samples fact table + symbol DB (tenant-scoped object keys);
    2. `MergedSymbols::absorb` each input's symbol DB → the merged DB + per-input `StacktraceRemap`;
    3. concatenate the fact tables, rewriting `PCOL_STACKTRACE_ID`/`PCOL_STACKTRACE_PARTITION` through the remap and flooring `COL_TIMESTAMP` via `downsample_timestamp`;
    4. **vertical dedup / fold:** `GROUP BY (series_fingerprint, profile_type, stacktrace_partition, stacktrace_id, downsampled_timestamp) → SUM(value)`, recomputing `PCOL_TOTAL_VALUE` per `(fingerprint, downsampled_timestamp)` group;
    5. write the merged block via `BlockWriter` + the merged symbol-DB artifact + a rebuilt `ProfileIndex` (union the input postings, new per-block time-range = `[min, max]` of the merged timestamps, new stacktrace-partition map) under a deterministic idempotent compacted-block key;
    6. return the new `BlockMeta`.
- **Horizontal merge** (`1h→4h→8h`) is the choice of *which* inputs go in `plan.inputs` (adjacent same-`target_window` blocks) — `compact_blocks` is agnostic to the window arithmetic; `target_window_hours` is recorded on the output `BlockMeta` for the next level's planner. (The planner that picks inputs is a thin caller, tested via the query-equality test below, not a separate unit.)

> **DataFusion-builder note (verify against the pinned rev `0838a4ddb902535b0e95a1c5a254be7e9c7fe9bf`):** the concatenate is `SessionContext::read_batches` / a `UNION ALL` over the input `RecordBatch`es registered as in-memory tables; the fold is a `GROUP BY … SUM(value)` SQL/DataFrame op. The exact `DataFrame`/`SessionContext` method names (`read_batch`, `register_batch`, `aggregate`, `sort`) churn between DataFusion revs — **do not fabricate them**; structure the code so the fold's *behavior* (one output row per distinct group, summed value) is pinned by the Task-7 query-equality test, and if a method name differs at the pinned rev, align to that rev's API and keep the test green. The downsampled-timestamp + remap rewrites happen on the Arrow arrays *before* registration (a pure array transform), so they are not DataFusion-version-sensitive.

- [ ] **Step 1: Write the failing downsample unit test + the query-equality test**

Create `crates/profiles/src/compactor/downsample.rs` with a `tests` module:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;
    use super::*;

    #[test]
    fn floor_buckets_to_step() {
        let ts = 7_123_456_789_000_i64; // arbitrary ns
        assert!(downsample_timestamp(ts, DownsampleStep::None) == ts);
        let m5 = downsample_timestamp(ts, DownsampleStep::Min5);
        assert!(m5 % 300_000_000_000 == 0 && m5 <= ts && ts - m5 < 300_000_000_000);
        let h1 = downsample_timestamp(ts, DownsampleStep::Hour1);
        assert!(h1 % 3_600_000_000_000 == 0 && h1 <= ts);
    }
}
```

Create `crates/profiles/tests/compaction.rs` (the load-bearing test — query-equality before/after compaction):

```rust
mod support;
use assert2::{assert, check};
use support::profiles_server::{self as srv};

const PT: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

#[tokio::test]
async fn compaction_preserves_query_results_and_dedups_symbols() {
    let s = srv::start_in_process().await;

    // Push two profiles for the SAME series that share a stacktrace -> two blocks
    // (force a block flush between pushes via the support helper).
    srv::push_pprof(&s.base_url, "t", &srv::profile(PT,
        &[("service_name", "svc")], &[("main;shared;leaf", 100)])).await;
    srv::flush_block(&s.base_url, "t").await;
    srv::push_pprof(&s.base_url, "t", &srv::profile(PT,
        &[("service_name", "svc")], &[("main;shared;leaf", 50)])).await;
    srv::flush_block(&s.base_url, "t").await;

    // Flamegraph BEFORE compaction (queries union both blocks).
    let before = srv::select_merge(&s.base_url, "t", PT, "{}", 0, i64::MAX / 2, 2048).await;

    // Run the compactor over the two blocks (no downsampling), then re-query.
    srv::run_compaction(&s.base_url, "t", /*downsample*/ "none", /*window_h*/ 4).await;
    let after = srv::select_merge(&s.base_url, "t", PT, "{}", 0, i64::MAX / 2, 2048).await;

    // Result equality: the merged block answers identically (leaf self == 150).
    check!(srv::flamegraph_self_of(&before, "leaf") == 150);
    check!(srv::flamegraph_self_of(&after, "leaf") == 150);
    check!(srv::normalize_flamegraph(&before) == srv::normalize_flamegraph(&after));

    // Symbol dedup: the merged block's symbol DB has the shared stack interned once
    // (assert the merged block count < sum of input block counts via the support stat).
    assert!(srv::merged_block_symbol_count(&s.base_url, "t") < srv::input_symbol_count_sum(&s.base_url, "t"));
}

#[tokio::test]
async fn downsampling_collapses_timestamps_into_buckets() {
    let s = srv::start_in_process().await;
    // Two samples 1 minute apart, same series/stack.
    srv::push_pprof_at(&s.base_url, "t", &srv::profile(PT, &[("service_name","svc")],
        &[("main;leaf", 10)]), /*ts_ms*/ 1_000_000).await;
    srv::push_pprof_at(&s.base_url, "t", &srv::profile(PT, &[("service_name","svc")],
        &[("main;leaf", 20)]), /*ts_ms*/ 1_060_000).await;
    srv::flush_block(&s.base_url, "t").await;
    srv::run_compaction(&s.base_url, "t", "5m", 4).await;
    // After 5m downsampling the two within-bucket samples fold to one timestamp, value 30.
    let series = srv::select_series(&s.base_url, "t", PT, "{}", &[], 1.0, 0, i64::MAX / 2).await;
    check!(srv::series_distinct_timestamps(&series) == 1);
    check!(srv::series_total(&series) == 30);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib compactor::downsample` then `cargo test -p crabka-profiles --test compaction`
Expected: FAIL — `downsample_timestamp` / `compact_blocks` / the `run_compaction`+`flush_block` support helpers absent.

- [ ] **Step 3: Implement `downsample.rs`, `compact_blocks`, and the support helpers**

Implement `DownsampleStep`/`downsample_timestamp`, then `compact_blocks` per the six steps. Add `run_compaction`, `flush_block`, `push_pprof_at`, `merged_block_symbol_count`, `input_symbol_count_sum`, `flamegraph_self_of`, `normalize_flamegraph`, `series_distinct_timestamps`, `series_total` to `support::profiles_server` (driving the in-process compactor role + reading block stats). Keep the DataFusion fold behind the verify-against-rev note.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-profiles --lib compactor::downsample --test compaction`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): compactor block merge (vertical+horizontal) + 5m/1h downsampling with query-equality"
```

---

### Task 8: Shared differential corpus + flamegraph/JSON differ (`#[ignore]` infra, no container yet)

**Files:**
- Create: `crates/profiles/tests/support/diff_corpus.rs` (path-included by Tasks 9/10)
- Modify: `crates/profiles/Cargo.toml` (`[dev-dependencies]` `reqwest`, `serde_json`, `testcontainers`, `testcontainers-modules`)

**Interfaces:**
- Produces (pure, Docker-free, so it can be unit-tested without containers):
  - `pub struct SeedProfile { profile_type: &'static str, labels: &[(&str,&str)], stacks: Vec<(&'static str /* "a;b;c" */, i64)>, ts_ms: i64 }` and `pub fn seed_dataset() -> Vec<SeedProfile>` — a deterministic dataset exercising a multi-level call tree (so the flamegraph has depth), two profile types (a `process_cpu` + a `memory:inuse_space`), a span-associated sample (so span-scoped works), and several timestamps within one window (so `SelectSeries` has multiple points). **All pprof is pre-symbolized** (function/line tables populated, `has_functions == true`) so symbolization is off the equality path (see Slice-7 Contract gap).
  - `pub fn to_pprof(profiles: &[SeedProfile]) -> Vec<PushFixture>` — build the gzipped-pprof `PushRequest` payloads via the Slice 2 `PprofProfile::encode` once, so the *identical* bytes go to both Pyroscope and Crabka.
  - `pub fn merge_corpus() -> Vec<MergeCase>` where `MergeCase { name, profile_type: &'static str, selector: &'static str }` — a `SelectMergeStacktraces` corpus: a bare type, a label selector, a multi-type case, a span-scoped case.
  - `pub fn series_corpus() -> Vec<SeriesCase>` where `SeriesCase { name, profile_type, selector, group_by: &[&str], step_secs: f64 }` — a `SelectSeries` corpus: a total series, a `group_by(service_name)`, a `Sum` and an `Average`.
  - `pub fn normalize_flamegraph(resp: &serde_json::Value) -> serde_json::Value` — canonicalize a `querier.v1` flamegraph (`names`/`levels`/`total`/`maxSelf`): the 4-ints-per-bar `levels` are order-sensitive (the `xOffsetDelta` encoding is a *contract*), so DO NOT reorder bars — instead canonicalize the `names` table + reindex `levels`' `nameIndex` to a sorted-name order so two engines that emit the same tree with a different `names` ordering compare equal; drop nothing semantic. Keep `total`/`maxSelf`.
  - `pub fn normalize_series(resp: &serde_json::Value) -> serde_json::Value` — sort `series` by labelset, sort each series' `points` by `timestamp`, round `value` to a fixed epsilon.
  - `pub fn assert_profile_query_equal(name: &str, a: &serde_json::Value, b: &serde_json::Value)` — `normalize_*` both, assert structural equality, on mismatch print a unified-ish diff naming the case.
- A **self-test** (Docker-free) proving the normalizers behave: two flamegraphs that emit the same tree with a permuted `names` table compare **equal**; a genuine self-value or extra-frame difference compares **unequal**; two series that differ only in point order + float epsilon compare **equal**.

- [ ] **Step 1: Write the failing self-test**

Create `crates/profiles/tests/diff_corpus_selftest.rs`:

```rust
#[path = "support/diff_corpus.rs"]
mod diff_corpus;
use assert2::{assert, check};
use diff_corpus::*;
use serde_json::json;

#[test]
fn normalize_flamegraph_is_name_table_order_insensitive() {
    // Same tree, different `names` table ordering + matching nameIndex.
    let a = json!({"names":["total","root","leaf"],
        "levels":[[0,150,0,1],[0,150,150,2]],"total":150,"maxSelf":150});
    let b = json!({"names":["total","leaf","root"],
        "levels":[[0,150,0,2],[0,150,150,1]],"total":150,"maxSelf":150});
    check!(normalize_flamegraph(&a) == normalize_flamegraph(&b));
}

#[test]
fn real_flamegraph_difference_is_detected() {
    let a = json!({"names":["total","leaf"],"levels":[[0,150,150,1]],"total":150,"maxSelf":150});
    let b = json!({"names":["total","leaf"],"levels":[[0,100,100,1]],"total":100,"maxSelf":100});
    check!(normalize_flamegraph(&a) != normalize_flamegraph(&b));
}

#[test]
fn normalize_series_is_order_and_epsilon_insensitive() {
    let a = json!({"series":[{"labels":[{"name":"s","value":"x"}],
        "points":[{"timestamp":2,"value":2.0000001},{"timestamp":1,"value":1.0}]}]});
    let b = json!({"series":[{"labels":[{"name":"s","value":"x"}],
        "points":[{"timestamp":1,"value":1.0},{"timestamp":2,"value":2.0}]}]});
    check!(normalize_series(&a) == normalize_series(&b));
}

#[test]
fn corpus_is_nonempty_and_covers_key_methods() {
    check!(!seed_dataset().is_empty());
    check!(!merge_corpus().is_empty());
    check!(series_corpus().iter().any(|c| !c.group_by.is_empty()));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --test diff_corpus_selftest`
Expected: FAIL — `diff_corpus` module absent.

- [ ] **Step 3: Implement `support/diff_corpus.rs`**

Implement the seed dataset, `to_pprof`, the merge/series corpora, both normalizers, and `assert_profile_query_equal`. The flamegraph normalizer must preserve bar order (the `xOffsetDelta` contract) and only canonicalize the `names` table + reindex `nameIndex`; the series normalizer sorts + epsilon-rounds.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-profiles --test diff_corpus_selftest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "test(profiles): shared differential pprof corpus + flamegraph/series differ (Docker-free)"
```

---

### Task 9: **Headline** — differential vs real Pyroscope (testcontainers, `#[ignore]`)

**Files:**
- Create: `crates/profiles/tests/diff_pyroscope.rs`

**Interfaces:**
- Consumes: Task 5 in-process Crabka server; Task 8 corpus + differ; `testcontainers` `mirror.gcr.io/grafana/pyroscope`.
- Produces (`#[ignore = "requires Docker"]`) — the headline external test:
  - Boot Crabka in-process; start `mirror.gcr.io/grafana/pyroscope:<pinned tag>` in **single-binary mode** (`-target=all`, filesystem/local storage, a mounted minimal config); push the **identical** gzipped-pprof `PushRequest` bytes (`to_pprof(seed_dataset())`) to **both** via `POST /push.v1.PusherService/Push` (`X-Scope-OrgID: diff`); run `merge_corpus()` against both `SelectMergeStacktraces` and `series_corpus()` against both `SelectSeries`; `assert_profile_query_equal` per case.

> **Harness structure & data loading (explicit, since this is a Docker suite):**
> - **Container:** `GenericImage::new("mirror.gcr.io/grafana/pyroscope", "<pinned tag>")` with cmd `-target=all -config.file=/etc/pyroscope.yaml`, a bind-mounted minimal `pyroscope.yaml` (`storage.backend: filesystem` / local blocks, a short flush/block timeout so blocks flush fast, a fixed single tenant). `WaitFor` on Pyroscope's readiness (`GET /ready` returns `200`, or `ProfileTypes` returns — Pyroscope uses `ProfileTypes` as the health probe per spec §7.1). Map the HTTP port (`4040`).
> - **Data load:** build the `PushRequest` once via `to_pprof(seed_dataset())`, serialize to protobuf, `POST /push.v1.PusherService/Push` (`Content-Type: application/proto`, `X-Scope-OrgID: diff`) to **both** Pyroscope (`http://localhost:<mapped 4040>/push.v1.PusherService/Push`) and Crabka — identical bytes, two destinations — guaranteeing identical input.
> - **Settle / flush:** both engines serve recent profiles from the hot tier immediately; for cases that must read from a flushed block, either keep the corpus within the live/ingester window (simplest, deterministic — **default**) or poll until a `SelectMergeStacktraces` returns a non-empty flamegraph on both sides (bounded, ~15s, mirroring the `client-core` bootstrap-retry pattern). Do not assume instantaneous visibility.
> - **Assert:** for each `MergeCase`, `SelectMergeStacktraces` on both, `assert_profile_query_equal(case.name, crabka_json, pyro_json)` via `normalize_flamegraph`. For each `SeriesCase`, `SelectSeries` on both and compare via `normalize_series`. A `PYRO_KNOWN_DIVERGENCE: &[(&str,&str)]` list (each entry justified) covers any Pyroscope-specific volatile metadata not dropped by `normalize_*` (e.g. a `metadata.appName` field, or a `units` echo).
> - **Documented limitation:** native-symbolization (Slice 7) parity is **not** exercised here — the corpus is pre-symbolized (Task 8), so the equality path is the fold+symbol-DB-resolve, not query-time DWARF. A `Diff` case is included only if both sides expose the same `FlameGraphDiff` 7-ints-per-bar JSON; otherwise it is in `PYRO_KNOWN_DIVERGENCE` with a reason. The `SelectMergeStacktraces` + `SelectSeries` equality is the firm headline.
> - **Why headline:** Pyroscope is the system Crabka claims to replace; flamegraph/series equality over identical pprof input is the strongest single correctness signal in the slice. Keep this the most carefully curated corpus. **Also pin the real Pyroscope Connect error codes/strings here** (push an over-long label / over-rate against the container, capture the exact Connect error body + code, and feed those literals back into Task 1's `LimitError` + Task 4's assertions) — this closes the "verify-against-Pyroscope" notes left in Tasks 1/4.

- [ ] **Step 1: Write the `#[ignore]` test + mount config**

Create `crates/profiles/tests/diff_pyroscope.rs` (embed the minimal `pyroscope.yaml` as a string written to a `tempfile` and bind-mounted). `#[tokio::test]` + `#[ignore = "requires Docker"]`.

- [ ] **Step 2: Run to verify ignored-by-default + runnable**

Run (default): `cargo test -p crabka-profiles --test diff_pyroscope` → reports `0 run, 1 ignored`.
Run (with Docker): `cargo test -p crabka-profiles --test diff_pyroscope -- --ignored --nocapture` → PASS (or surfaces a real divergence to fix).

- [ ] **Step 3: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "test(profiles): headline differential vs real Pyroscope (ignored, testcontainers)"
```

---

### Task 10: Grafana integration — built-in Pyroscope datasource → Crabka end-to-end (`#[ignore]`)

**Files:**
- Create: `crates/profiles/tests/grafana_integration.rs`

**Interfaces:**
- Consumes: Task 5 in-process Crabka (querier + Connect `querier.v1` API + legacy `/pyroscope/render`); Task 8 seed dataset; `testcontainers` `mirror.gcr.io/grafana/grafana`.
- Produces (`#[ignore = "requires Docker"]`):
  - Boot Crabka in-process (seed `seed_dataset()`); start `mirror.gcr.io/grafana/grafana:<pinned tag>` with a **provisioned built-in Pyroscope datasource** whose `url` points at the Crabka Connect base URL. Drive Grafana's datasource **proxy/Explore query API** for each leg and assert each renders.

> **Harness structure & data loading (explicit):**
> - **Datasource provisioning:** mount a `datasources.yaml` (`apiVersion: 1`) with a `grafana-pyroscope-datasource` (type `pyroscope`), `url: http://host.docker.internal:<crabka_port>`, a fixed `uid`, `httpHeaderName1: X-Scope-OrgID` / `httpHeaderValue1: grafana`. Set `GF_AUTH_ANONYMOUS_ENABLED=true`, `GF_AUTH_ANONYMOUS_ORG_ROLE=Admin` so the test calls the API without login.
> - **Host reach:** start Crabka bound to `0.0.0.0` and pass the container `--add-host=host.docker.internal:host-gateway` (Linux) so the datasource URL resolves; document this platform-specific knob.
> - **Drive — the four legs Grafana's Pyroscope datasource exercises (spec §7.1, §10):**
>   1. **ProfileTypes health:** Grafana's datasource config-test hits `ProfileTypes` (there is no separate `/ready`); assert `200` + a non-empty `profile_types` list.
>   2. **Flame graph:** `POST /api/ds/query` with a `profile`-type Pyroscope query (`SelectMergeStacktraces`) for the seeded `process_cpu` type; assert the returned frame is the Grafana nested-set flamegraph (level/value/self/label) and non-empty.
>   3. **Time series:** a `metrics`-type query (`SelectSeries`) for the same type; assert non-empty time-series frames.
>   4. **Drilldown render (legacy):** drive the Profiles Drilldown app's legacy door `GET /pyroscope/render?query=<type>{}&from&until&format=json`; assert a flamebearer JSON (`flamebearer.names/levels/numTicks/maxSelf` + `metadata.format == "single"`).
> - **Scope:** one query per leg is sufficient; this is an integration smoke proving the full Grafana → Pyroscope-datasource → Crabka path renders (the spec's "assert they render"), not a second differential corpus.

- [ ] **Step 1: Write the `#[ignore]` test**

Create `crates/profiles/tests/grafana_integration.rs` with the provisioning + the four drive legs above; `#[tokio::test]` + `#[ignore = "requires Docker"]`.

- [ ] **Step 2: Run to verify ignored + runnable**

Run (default): `cargo test -p crabka-profiles --test grafana_integration` → `0 run, 1 ignored`.
Run (Docker): `cargo test -p crabka-profiles --test grafana_integration -- --ignored --nocapture` → PASS.

- [ ] **Step 3: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "test(profiles): Grafana built-in Pyroscope datasource integration (ignored, Docker)"
```

---

### Task 11: CI wiring — `profiles-differential` job; final whole-crate gate

**Files:**
- Modify: the CI workflow (`.github/workflows/*.yml`) — add one job.
- Modify: `docs/` (a short note in the slice's section of the plan dir if the repo tracks a CI matrix; otherwise none).

**Interfaces:**
- Produces:
  - A **`profiles-differential`** CI job (Linux, Docker available): `cargo test -p crabka-profiles -- --ignored` scoped to the two Docker suites (`diff_pyroscope`, `grafana_integration`) — runs on a schedule + on-demand label (not every PR, to keep PR latency low), mirroring how the repo gates other Docker-heavy suites (`client-core-integration`). Document that these never run in the default `cargo test --workspace`.
  - The in-process suites (`limits_overrides`, `tenant_isolation`, `compaction`, `diff_corpus_selftest`) stay in the default per-crate test job — no Docker, fast PR coverage.

- [ ] **Step 1: Add the CI job**

Add `profiles-differential` (scheduled/labeled, Docker-enabled runner) to the workflow, following the existing job patterns (toolchain, cache, `--ignored` invocation).

- [ ] **Step 2: Verify locally**

Run the default suite (no Docker): `cargo test -p crabka-profiles` → all non-ignored pass, the two Docker suites report ignored.

- [ ] **Step 3: Final whole-crate gate**

Run: `cargo test -p crabka-profiles && cargo clippy -p crabka-profiles --all-targets && cargo fmt -p crabka-profiles --check`
Expected: all PASS, no warnings, formatting clean. (Docker suites remain ignored.)

- [ ] **Step 4: Commit**

```bash
cargo fmt -p crabka-profiles
git add .github/ crates/profiles/ docs/
git commit -m "ci(profiles): dedicated profiles-differential Docker job"
```

---

## Self-review

**Spec coverage (against §7 API, §9 limits/multi-tenancy, §10 testing, §11 Slice 8):**
- Per-tenant limits/quotas (max series, label name/value length + count, ingestion rate, `max_nodes`, query range, `__session_id__` cardinality) on the token-bucket where it fits → Tasks 1–3, enforced live in Task 4. Token-bucket reused for the *rate* + the `__session_id__` *cardinality* limits (Task 3 note); count/length caps are plain comparisons; `max_nodes` is a **clamp, not a reject** (correct — Pyroscope clamps).
- Per-tenant overrides YAML (Pyroscope `overrides.yaml`) → Task 2.
- **Pyroscope-shaped** Connect errors (`resource_exhausted`/429, `invalid_argument`/400) → Task 1 (`LimitError` code/status/message) + Task 4 (end-to-end body assertions), with the codes pinned against the real container in Task 9.
- Multi-tenancy isolation (ProfileTypes/LabelNames/LabelValues/SelectMergeStacktraces/select-on-A-only-label/quota; tenant-prefixed block/index/symbol-DB keys) **end-to-end via two `X-Scope-OrgID`s** → Task 5 (**headline**), with a *colliding-service+profile-type* probe that forces the symbol-DB resolution to be tenant-scoped before the fold (A's `a_only_fn` must never leak into B), plus the per-tenant quota assertion in Step 4.
- Compactor role (vertical dedup + horizontal `1h→4h→8h` + symbol re-dedup + `5m`/`1h` downsampling) → Tasks 6 (cross-block symbol re-intern + remap) + 7 (block merge + downsample), proven by a **query-equality before/after** test + a symbol-dedup-ratio assertion + a downsample-bucketing assertion.
- Differential vs real Pyroscope (testcontainers) **as the equality headline** → Task 9 (`#[ignore]`), feeding identical gzipped-pprof bytes to both (one-push-two-destinations), asserting `SelectMergeStacktraces` + `SelectSeries` equality through `normalize_*`.
- Grafana integration (built-in Pyroscope datasource → Crabka, all four legs: ProfileTypes health, flame graph, time series, legacy drilldown render) → Task 10 (`#[ignore]`).
- Dedicated CI job for the Docker/external suites; default `cargo test` never touches Docker → Task 11.

**Headlines are explicit and end-to-end.** Tenant isolation (Task 5) drives **real Connect-RPC over a real socket** with two org IDs across *every* read surface — and uses *colliding service name + profile type* in both tenants (plus a tenant-A-only frame) so the assertion can only pass if isolation happens before the symbol-DB fold, not after — plus per-tenant quota. Differential-vs-Pyroscope (Task 9) feeds **identical pprof push bytes** to both systems (one-push-two-destinations) and asserts flamegraph + series equality through `normalize_flamegraph`/`normalize_series`/`assert_profile_query_equal`. Both are called out as headline in their task titles and the spec-coverage list.

**`#[ignore]` discipline.** Every external-system test (Tasks 9/10) is `#[ignore = "requires Docker"]`, each task's run-step verifies `0 run, N ignored` by default and `--ignored` under Docker, and Task 11 isolates them in a `profiles-differential` job off the PR path. The Docker-free differ self-test (Task 8) and the in-process isolation/limits/compaction tests (Tasks 4/5/7) run in the default suite, so the meat of the slice — including the load-bearing compaction query-equality test — has fast CI coverage without Docker.

**Compaction is correctness-driven, not byte-format-driven.** The compactor (Tasks 6/7) is greenfield (not phlaredb-compatible); its sole correctness contract is **query-result equality before/after** (Task 7's headline compaction test), plus the symbol re-dedup that is the spec §13 size lever (asserted via a merged-vs-input symbol-count comparison) and the downsample bucketing (asserted via folded-timestamp + summed-value checks). The cross-block `stacktrace_id` remap (Task 6) is the load-bearing invariant — raw ids are meaningless across blocks, so the re-intern + remap is mandatory, mirroring the spec §6.4 "raw ids never cross a boundary" rule at compaction time.

**Contract consumption, not re-derivation.** The querier Connect API, `CrabkaProfileStore`/`FlameEngine`, the `tenant_of` resolver, distributor write hook, `ProfileRecord`, the Slice 1 `BlockStore`/`BlockWriter`/`ProfileIndex`/`PCOL_*`, the Slice 1/2 `SymbolDb`/`ProfileType`/`ProfileError`, and the broker `TokenBucket` are all consumed from earlier slices/crates via the Shared Contract table; each task names the exact item and carries a "Contract gap" fallback (a minimal in-memory impl or a small in-scope addition like `SymbolDb::stacktrace_ids`, never a silent stub). No new flamegraph-merge or storage semantics are invented here — this is a hardening band.

**No-back-compat respected.** No `#[serde(default)]`-as-compat-shim, no version variants, no migration. The single `#[serde(default)]` use (Task 2, `PartialLimits`) is the *partial-config mechanism* (a tenant overrides only named fields), explicitly distinguished in-task from a compat shim, with a code comment so a reviewer doesn't misflag it. Compacted blocks replace their inputs outright (no dual-format read path).

**Placeholder scan.** Every in-process task (1–8) has a failing-test → run-fails-with-expected → real-code → run-passes → commit cycle with concrete `cargo test -p crabka-profiles …` commands and assert2 assertions. The Docker tasks (9/10), where literal code would be guesswork against live container behavior, instead provide a **fully specified harness structure** — exact image/tag knobs, the one-push-two-destinations data-load mechanism, the settle/flush strategy, the per-leg drive, the assertion, and an explicit known-divergence list — which is the honest level of detail for a black-box external suite, not a placeholder. The two deferred string-literal pins (Pyroscope Connect error codes/messages in Tasks 1/4) are explicitly closed by the Task 9 container run rather than fabricated. The one churn-prone internal API (the DataFusion concatenate/group-by fold in Task 7) is bounded with a verify-against-rev note and pinned by the query-equality behavior test, not fabricated method signatures.

**Type/name consistency.** The `Limits` field set is identical across Tasks 1/2/3/4 and every test (`ingestion_rate_profiles_per_sec`, `ingestion_burst_profiles`, `max_series`, `max_label_name_length`, `max_label_value_length`, `max_label_names_per_series`, `max_flamegraph_nodes_default`, `max_flamegraph_nodes_max`, `max_query_length_secs`, `max_session_id_cardinality`). `LimitError` variants and their `connect_code`/`http_status` mapping are defined once (Task 1) and asserted unchanged in Tasks 3/4. The `IngestEnforcer`/`QueryEnforcer` method set is consistent between Tasks 3 and 4. `MergedSymbols`/`StacktraceRemap`/`DownsampleStep`/`compact_blocks` are fixed in Tasks 6/7 and consumed unchanged by the compaction test. The `support::profiles_server` and `support::diff_corpus` helper signatures are fixed in Tasks 5/8 and consumed unchanged in Tasks 4/7/9/10.

**Known risks (flagged).** (1) The `crabka-broker` path dep for `TokenBucket` could introduce a heavy/cyclic dependency into `crabka-profiles`; Task 3's note gives the escape hatch (lift `bucket.rs` into a tiny `crabka-throttle` crate) and prefers the path dep unless a cycle appears. (2) The `__session_id__` cardinality cap is a *token-bucket approximation* of a windowed distinct-count, not an exact HLL; Task 3 documents the approximation. (3) `TokenBucket`'s burst-vs-refill coupling means the profiles/sec-vs-burst split is approximate; Task 3 documents seeding at `max(rate, burst)`. (4) The DataFusion concatenate/group-by API (Task 7) churns across the pinned rev; contained behind a verify-against-rev note + the query-equality test. (5) Docker-host reachability for Grafana (Task 10) is platform-specific (`host.docker.internal` + `--add-host`); flagged with the exact knob. (6) Pyroscope volatile metadata (Task 9) is contained by `normalize_*` + an explicit `PYRO_KNOWN_DIVERGENCE` list rather than loosening the differ; the flamegraph normalizer deliberately preserves bar order (the `xOffsetDelta` contract) and only canonicalizes the `names` table. (7) The Pyroscope Connect status map (`resource_exhausted`/429, `invalid_argument`/400) differs from the metrics slice's Prometheus map (429/422/400) and the traces slice's Tempo map (429/400) — called out in Task 1 and verified live in Task 9 so the divergence is intentional and tested.
