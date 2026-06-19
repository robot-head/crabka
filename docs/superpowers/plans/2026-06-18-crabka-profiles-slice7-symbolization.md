# crabka-profiles Slice 7 — Native symbolization (debuginfod + DWARF/ELF + lazy query-time resolve)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve native/eBPF stack frames **lazily at query time**. For profiles whose `Mapping.has_functions == false` (stored as raw `address` + `build_id`/file-id), turn `(build_id, address)` into `Frame`s — `function`/`file`/`line`, with **inline-frame expansion** — by fetching debuginfo from **debuginfod**, parsing **ELF `.symtab`/`.dynsym` + DWARF + Go `.gopclntab`**, and demangling C++/Rust. Wrap it behind the **same `SymbolSource` trait** the in-block `SymbolDb` implements, so `crabka-pprof`'s `FlameEngine` resolves uniformly and only ever touches symbols for the **distinct surviving ids** (skip never-viewed data). Ship as an in-querier query-time stage **and** a selectable `--target symbolizer` role.

**Architecture:** A new `crabka-profiles::symbolize` module, bottom-up:

1. **`DebuginfodClient`** (`fetch.rs`) — a `reqwest` HTTP client for the debuginfod federation protocol (`GET {server}/buildid/{build_id}/debuginfo`, `…/executable`), with a content-addressed **on-disk cache** keyed by `build_id`. Default server `https://debuginfod.elfutils.org/`, comma-separated `$DEBUGINFOD_URLS`-style override. Returns a cached on-disk path to the debuginfo ELF, or `NotFound`.
2. **`ElfModule`** (`dwarf.rs`) — given a debuginfo file, builds an `addr2line::Context` (DWARF) over an `object::File`, plus a fallback `.symtab`/`.dynsym` symbol lookup and a `.gopclntab` Go line-table reader. `resolve_addr(u64) -> Vec<Frame>` returns leaf-first frames with **inlined frames expanded** (the DWARF inlined-subroutine chain); demangles via `addr2line`'s demangle or `rustc-demangle`/`cpp_demangle`.
3. **`Symbolizer`** (`mod.rs`) — implements `crabka_pprof::SymbolSource`. It wraps an **inner `SymbolSource`** (the in-block `SymbolDb`) plus a `Modules` cache (`build_id → Arc<ElfModule>`) and a **resolved-frame LRU** (`(build_id, address) → Vec<Frame>`). `resolve(partition, id)` delegates to the inner source; for any returned `Frame` flagged "needs native resolution" (a sentinel the `SymbolDb` emits for `has_functions == false` mappings), it substitutes the debuginfod/DWARF result. **Lazy:** a `(build_id, address)` is fetched only when the engine actually asks for that id.

The churn-prone surface (the `gimli`/`object`/`addr2line` DWARF API + the debuginfod wire) is isolated in `dwarf.rs`/`fetch.rs`, each pinned by a behavior test against a **vendored tiny fixture ELF+DWARF** (synthesized in the test, not fabricated signatures).

**Tech Stack:** Rust 2024 · `crabka-pprof` (Slice 2 — `Frame`, `SymbolSource`, `ProfileError`) · `crabka-blockstore` (the mapping `build_id`/`has_functions` accessors, Slice 1) · `addr2line` 0.24 · `gimli` 0.31 · `object` 0.36 · `rustc-demangle` 0.1 + `cpp_demangle` 0.4 · `reqwest` 0.13 (`rustls-tls`, `blocking`-free async) · `tokio` · `async-trait` · `thiserror` · `tracing` · `clap` (role flag). Tests: `assert2`; `object` write side (`object::write`) to **synthesize** the fixture ELF in-test; `wiremock` 0.6 for the debuginfod mock; `tempfile` for the cache dir.

## Global Constraints

- **No backwards compatibility.** Greenfield/undeployed. Change the `Symbolizer`/cache/config shapes, the `Frame` "needs-resolution" sentinel, and the role flag freely; no shims, no migration code, no `#[serde(default)]` gates. (Only Kafka wire compat matters — this slice touches no Kafka bytes.)
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe`. (Note: `memmap2` is *not* used — debuginfo files are read into an owned `Vec<u8>` and parsed via `object::File::parse(&[u8])`, keeping the crate `unsafe`-free. `Mmap` would require `unsafe`.)
- **Lints:** `clippy::pedantic` is `warn`. New code clippy-pedantic clean (`module_name_repetitions`/`missing_errors_doc`/`missing_panics_doc` allowed workspace-wide). Run `cargo clippy -p crabka-profiles --all-targets` before each commit.
- **Formatting:** `cargo fmt -p crabka-profiles` before every commit (never `cargo +nightly fmt --all` — OS error 206 / path-too-long in deep worktrees on Windows; always `-p`).
- **Assertions:** `assert2::assert!` / `assert2::check!` in tests.
- **Async tests:** `#[tokio::test]`. Dev-dep `tokio` features `["macros","rt-multi-thread"]`.
- **Network tests are hermetic.** The debuginfod HTTP path is tested **only** against a `wiremock` mock server (no real `debuginfod.elfutils.org` call in `cargo test`). A real-federation smoke test, if any, is `#[ignore = "requires network"]`.
- **Symbolization is `SymbolSource`-shaped.** The `Symbolizer` is consumed by `crabka-pprof`'s `FlameEngine` **only** through `crabka_pprof::SymbolSource` (`fn resolve(&self, partition: u64, id: u32) -> Vec<Frame>`). It adds **no** new trait the engine must learn — that is the load-bearing seam from Slice 2.
- **Honest scope (spec §8, Pyroscope issue #3715).** We ship the **system/OSS-binary** path: debuginfod default + DWARF/ELF/`.gopclntab` + demangle + inline expansion, with the lazy-resolve plumbing. **Broader customer-code symbolization** (executable upload + a raw `addr2line` exec path over user binaries that debuginfod does not host) is a **noted extension** (`exec-upload`), wired as a fallback hook but not claimed complete. Every public doc-comment that touches scope says so.

---

## Dependency & slice roadmap

**Depends on:**
- **Slice 2 (`crabka-pprof`)** — `pub trait SymbolSource: Send + Sync { fn resolve(&self, partition: u64, id: u32) -> Vec<Frame>; }`; `Frame { pub function: String, pub file: String, pub line: i32 }`; `enum ProfileError { Decode, Plan, Exec, Store, Unsupported, Symbolize(String), … }`; `SymbolDb` (the in-block source this slice wraps). **Consumed via contract** (see Shared contract). If `SymbolDb` does not yet emit a "needs native resolution" sentinel, this slice adds the minimal accessor in `crabka-pprof` (one method) and notes it — never a silent stub.
- **Slice 1 (`crabka-blockstore` `ProfileIndex` / symbol-DB artifact)** — the per-mapping fields `address`, `build_id` (string), `has_functions: bool` carried in the symbol-DB so the wrapper knows *which* frames need native resolution and *which* `(build_id, address)` to resolve. **Consumed via contract.**
- **Slice 5 (querier) / Slice 6 (query-frontend)** — the querier constructs the `ProfileScan { symbols: Arc<dyn SymbolSource> }`; this slice provides the `Arc<dyn SymbolSource>` it plugs in (the `Symbolizer` wrapping the block's `SymbolDb`). The query-frontend merges **partial symbolized trees**, so resolution always happens *before* a cross-block merge — this slice's resolver therefore only ever sees one block's partition space at a time.

**Shared contract (consume exactly — do not redefine).** From `crabka-pprof` (Slice 2):

```rust
pub struct Frame { pub function: String, pub file: String, pub line: i32 }

pub trait SymbolSource: Send + Sync {
    fn resolve(&self, partition: u64, id: u32) -> Vec<Frame>;
}

pub enum ProfileError { /* … */ Symbolize(String) /* … */ }
```

> **Verify-before-use (do not fabricate):** the exact field names of `Frame`, the `SymbolSource::resolve` signature, and the `ProfileError::Symbolize` variant are owned by Slice 2. Before Task 4, read `crates/pprof/src/lib.rs` re-exports (or `cargo doc -p crabka-pprof --no-deps`) and reconcile. If a name differs, adapt the **wrapper + tests together** — keep the asserted *frame resolution behavior* (the contract this slice owns) exact; the Rust field names bend to pprof.

**Contract gap — the "needs native resolution" signal.** The `Symbolizer` must know, per `(partition, id)`, which locations came from an unsymbolized mapping (`has_functions == false`) and what their raw `(build_id, address)` are. Two acceptable shapes, decided in Task 4:
- **(preferred)** `SymbolDb` exposes `fn unresolved_locations(&self, partition: u64, id: u32) -> Vec<(String /*build_id*/, u64 /*address*/, usize /*frame slot*/)>` (added in Slice 2 if missing — a 1-method addition, flagged).
- **(fallback)** `SymbolDb::resolve` returns `Frame`s where an unresolved frame is encoded as `function == ""`, `file == "<build_id>"`, `line == <address as i32-truncated>` — a sentinel the wrapper decodes. **Brittle (i32 truncates a u64 address)**; use only if the preferred accessor cannot land. The Task-4 "Contract gap" note records which was used.

**The 8 profiles slices** (this plan = Slice 7; each gets its own plan): 1 blockstore `ProfileIndex` + samples schema + symbol-DB artifact · 2 `crabka-pprof` core (pprof model + `SymbolDb` + `ProfileStore` + MERGE→flamegraph) · 3 engine completeness (SelectSeries/Diff/max_nodes/pprof-out) · 4 ingest (distributor + block-builder) · 5 querier + Connect `querier.v1` + legacy render · 6 query-frontend (partial-tree merge) · **7 native symbolization (this plan)** · 8 hardening (limits + multi-tenancy + compaction + differential-vs-Pyroscope + Grafana).

---

## File structure (`crates/profiles/`)

| File | Responsibility |
|---|---|
| `Cargo.toml` | add `addr2line`/`gimli`/`object`/`rustc-demangle`/`cpp_demangle`/`reqwest` deps + dev-dep `wiremock`; (crate already exists from Slices 4–6) |
| `src/lib.rs` | add `pub mod symbolize;` + re-exports (existing modules unchanged) |
| `src/symbolize/mod.rs` | `Symbolizer` (`impl crabka_pprof::SymbolSource`) + `SymbolizerConfig` + `SymbolizeError` + module decls |
| `src/symbolize/fetch.rs` | `DebuginfodClient` — debuginfod HTTP + on-disk content-addressed cache + `BuildIdCache` |
| `src/symbolize/dwarf.rs` | `ElfModule` — `object` + `addr2line` DWARF context, `.symtab`/`.dynsym` fallback, `.gopclntab` reader, demangle, inline expansion |
| `src/symbolize/role.rs` | `--target symbolizer` role wiring (config from env/flags → a serve loop / in-querier stage handle) |
| `src/bin/crabka-profiles.rs` | add the `symbolizer` arm to the existing `--target` match (Slices 4–6 own the binary) |
| `tests/support/fixture_elf.rs` | synthesize a tiny ELF+DWARF with known symbols + an inline frame (path-included by integration tests) |
| `tests/symbolize_dwarf.rs` | headline: address→frame against the fixture ELF (symbol + DWARF line + inline expansion) |
| `tests/symbolize_debuginfod.rs` | headline: build-id lookup + on-disk cache (hit/miss) against a `wiremock` server |
| `tests/symbolize_lazy.rs` | headline: `Symbolizer` wraps a fake inner `SymbolSource`; never-viewed id ⇒ never fetched; viewed id ⇒ native frames substituted |

`dwarf.rs` (DWARF/object/addr2line) and `fetch.rs` (reqwest/debuginfod) are the only churn-prone files; each is pinned by a behavior test.

---

### Task 1: Crate deps + `symbolize` module scaffold

**Files:**
- Modify: `crates/profiles/Cargo.toml`
- Modify: `crates/profiles/src/lib.rs`
- Create: `crates/profiles/src/symbolize/mod.rs`
- Modify: root `Cargo.toml` (add `addr2line`/`gimli`/`object`/`rustc-demangle`/`cpp_demangle`/`reqwest`/`wiremock` to `[workspace.dependencies]`)

**Interfaces:**
- Produces: a compiling `crabka-profiles` with a `symbolize` module exposing `pub struct SymbolizerConfig` + `pub enum SymbolizeError` + a placeholder `pub fn symbolize_smoke() -> bool`.

- [ ] **Step 1: Add workspace deps**

In root `Cargo.toml`, under `[workspace.dependencies]` (near `object_store`):

```toml
# crabka-profiles native symbolization (Slice 7). The DWARF/ELF stack:
# `object` parses ELF/Mach-O/PE; `addr2line` builds a DWARF line+inline
# context over it (re-exporting `gimli`); `gimli` is pinned directly for the
# `.gopclntab`/section access the Go path needs. Demanglers for the two
# mangled-name schemes profiling actually sees in native frames.
object = { version = "0.36", default-features = false, features = ["read", "std"] }
gimli = { version = "0.31", default-features = false, features = ["read", "std"] }
addr2line = { version = "0.24", default-features = false, features = ["std", "object", "fallible-iterator", "smallvec"] }
rustc-demangle = "0.1"
cpp_demangle = "0.4"
# debuginfod HTTP client. rustls (no system OpenSSL); 0.13 lines up with the
# reqwest 0.13 already in the dep graph (opentelemetry-otlp).
reqwest = { version = "0.13", default-features = false, features = ["rustls-tls", "stream"] }
# hermetic HTTP mock for the debuginfod tests (dev-dep).
wiremock = "0.6"
```

> **Verify-against-version note (addr2line 0.24 / gimli 0.31 / object 0.36):** these three crates version-lock as a family (`addr2line` 0.24 re-exports `gimli` 0.31 and depends on `object` 0.36). If `cargo update` resolves a different `gimli` major *inside* `addr2line` than the one pinned here, the `gimli` types will not cross the boundary — keep the direct `gimli` pin equal to `addr2line`'s re-exported `gimli` (read `addr2line::gimli::*` instead of the direct `gimli` crate where types must unify). Confirm with `cargo tree -p crabka-profiles -i gimli` (one version only).

- [ ] **Step 2: Wire `crates/profiles/Cargo.toml`**

Add to `[dependencies]`:

```toml
object = { workspace = true }
gimli = { workspace = true }
addr2line = { workspace = true }
rustc-demangle = { workspace = true }
cpp_demangle = { workspace = true }
reqwest = { workspace = true }
async-trait = { workspace = true }
```

Add to `[dev-dependencies]`:

```toml
wiremock = { workspace = true }
tempfile = { workspace = true }
```

(`crabka-pprof`, `crabka-blockstore`, `tokio`, `thiserror`, `tracing`, `assert2`, `clap` are already deps from Slices 4–6.)

- [ ] **Step 3: Write the failing scaffold test**

Create `crates/profiles/src/symbolize/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn smoke() {
        assert!(symbolize_smoke());
        // SymbolizerConfig has the documented defaults.
        let c = SymbolizerConfig::default();
        assert!(c.servers == vec!["https://debuginfod.elfutils.org/".to_string()]);
        assert!(c.resolved_cache_cap == 100_000);
    }
}
```

- [ ] **Step 4: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib symbolize`
Expected: FAIL — `cannot find function symbolize_smoke` / `cannot find type SymbolizerConfig`.

- [ ] **Step 5: Implement the scaffold**

Prepend above the `tests` module:

```rust
//! Native (eBPF) query-time symbolization: `(build_id, address)` → `Frame`s via
//! debuginfod + DWARF/ELF/`.gopclntab`. Wraps the in-block `SymbolDb` behind the
//! `crabka_pprof::SymbolSource` trait so the flamegraph engine resolves uniformly.
//!
//! Scope (spec §8 / Pyroscope #3715): the **system/OSS-binary** path — debuginfod
//! default, DWARF/ELF/Go line tables, demangle, inline expansion, lazy resolve.
//! Customer-code symbolization (executable upload) is a noted follow-on hook
//! (`exec-upload`), not claimed complete here.

mod dwarf;
mod fetch;
pub mod role;

use std::path::PathBuf;

/// Configuration for the query-time symbolizer.
#[derive(Clone, Debug)]
pub struct SymbolizerConfig {
    /// debuginfod federation servers, tried in order (analog of `$DEBUGINFOD_URLS`).
    pub servers: Vec<String>,
    /// On-disk cache directory for fetched debuginfo, keyed by `build_id`.
    pub cache_dir: PathBuf,
    /// Max entries in the in-memory resolved-frame LRU (`(build_id,address)`→frames).
    pub resolved_cache_cap: usize,
    /// Per-request debuginfod timeout.
    pub fetch_timeout: std::time::Duration,
}

impl Default for SymbolizerConfig {
    fn default() -> Self {
        Self {
            servers: vec!["https://debuginfod.elfutils.org/".to_string()],
            cache_dir: std::env::temp_dir().join("crabka-debuginfod-cache"),
            resolved_cache_cap: 100_000,
            fetch_timeout: std::time::Duration::from_secs(30),
        }
    }
}

/// Errors from the symbolization stage.
#[derive(Debug, thiserror::Error)]
pub enum SymbolizeError {
    #[error("debuginfod: {0}")]
    Fetch(String),
    #[error("object/ELF parse: {0}")]
    Object(String),
    #[error("dwarf: {0}")]
    Dwarf(String),
    #[error("no debuginfo for build_id {0}")]
    NotFound(String),
    #[error("io: {0}")]
    Io(String),
}

impl From<SymbolizeError> for crabka_pprof::ProfileError {
    fn from(e: SymbolizeError) -> Self {
        crabka_pprof::ProfileError::Symbolize(e.to_string())
    }
}

/// Placeholder until Task 4 lands the `Symbolizer`.
#[must_use]
pub fn symbolize_smoke() -> bool {
    true
}
```

Create empty stub files so the `mod` decls compile: `crates/profiles/src/symbolize/dwarf.rs` and `crates/profiles/src/symbolize/fetch.rs` each containing only `//! stub — filled in Task 2/3.` plus `#![allow(dead_code)]`; and `crates/profiles/src/symbolize/role.rs` containing `//! stub — filled in Task 5.`.

- [ ] **Step 6: Add module to `lib.rs`**

Add `pub mod symbolize;` and `pub use symbolize::{SymbolizeError, SymbolizerConfig};` (leave existing re-exports untouched).

- [ ] **Step 7: Run to verify it passes**

Run: `cargo test -p crabka-profiles --lib symbolize`
Expected: PASS (`smoke`).

- [ ] **Step 8: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/ Cargo.toml
git commit -m "feat(profiles): scaffold symbolize module + DWARF/debuginfod deps"
```

---

### Task 2: `DebuginfodClient` — fetch + on-disk content-addressed cache

**Files:**
- Modify: `crates/profiles/src/symbolize/fetch.rs`
- Create: `crates/profiles/tests/symbolize_debuginfod.rs`

**Interfaces:**
- Consumes: `SymbolizeError`, `SymbolizerConfig`.
- Produces:
  - `pub struct DebuginfodClient { http: reqwest::Client, servers: Vec<String>, cache: BuildIdCache, timeout: Duration }`
  - `impl DebuginfodClient { pub fn new(cfg: &SymbolizerConfig) -> Result<Self, SymbolizeError>; pub async fn fetch_debuginfo(&self, build_id: &str) -> Result<PathBuf, SymbolizeError>; }` — returns the on-disk path to the debuginfo ELF (cache hit ⇒ no HTTP), `NotFound` if every server 404s.
  - `pub struct BuildIdCache { root: PathBuf }` with `path_for(&self, build_id: &str) -> PathBuf` (`<root>/<bb>/<rest>.debuginfo`, sharded by first byte) + `contains(&self, build_id) -> bool` + `store(&self, build_id, bytes: &[u8]) -> Result<PathBuf, SymbolizeError>` (atomic write via tempfile-then-rename).

- [ ] **Step 1: Write the failing test (wiremock)**

Create `crates/profiles/tests/symbolize_debuginfod.rs`:

```rust
use crabka_profiles::symbolize::fetch::DebuginfodClient;
use crabka_profiles::SymbolizerConfig;
use assert2::assert;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const BUILD_ID: &str = "0123456789abcdef0123456789abcdef01234567";

struct Counting(Arc<AtomicUsize>, Vec<u8>);
impl Respond for Counting {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        self.0.fetch_add(1, Ordering::SeqCst);
        ResponseTemplate::new(200).set_body_bytes(self.1.clone())
    }
}

#[tokio::test]
async fn fetch_then_cache_hit_avoids_second_http() {
    let server = MockServer::start().await;
    let hits = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path(format!("/buildid/{BUILD_ID}/debuginfo")))
        .respond_with(Counting(hits.clone(), b"\x7fELF-fake-bytes".to_vec()))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let cfg = SymbolizerConfig {
        servers: vec![server.uri()],
        cache_dir: dir.path().to_path_buf(),
        ..SymbolizerConfig::default()
    };
    let client = DebuginfodClient::new(&cfg).unwrap();

    let p1 = client.fetch_debuginfo(BUILD_ID).await.unwrap();
    assert!(p1.exists());
    assert!(std::fs::read(&p1).unwrap() == b"\x7fELF-fake-bytes");
    assert!(hits.load(Ordering::SeqCst) == 1);

    // Second fetch is served from the on-disk cache — no extra HTTP hit.
    let p2 = client.fetch_debuginfo(BUILD_ID).await.unwrap();
    assert!(p2 == p1);
    assert!(hits.load(Ordering::SeqCst) == 1);
}

#[tokio::test]
async fn all_servers_404_is_not_found() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = SymbolizerConfig {
        servers: vec![server.uri()],
        cache_dir: dir.path().to_path_buf(),
        ..SymbolizerConfig::default()
    };
    let client = DebuginfodClient::new(&cfg).unwrap();
    let err = client.fetch_debuginfo(BUILD_ID).await;
    assert!(matches!(
        err,
        Err(crabka_profiles::SymbolizeError::NotFound(_))
    ));
}
```

(Add `pub mod fetch;` visibility: in `mod.rs` change `mod fetch;` to `pub mod fetch;` so the test can name `DebuginfodClient`.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --test symbolize_debuginfod`
Expected: FAIL — `cannot find struct DebuginfodClient`.

- [ ] **Step 3: Implement `fetch.rs`**

```rust
//! debuginfod federation client + on-disk content-addressed cache.
//!
//! Protocol (elfutils debuginfod): `GET {server}/buildid/{build_id}/debuginfo`
//! returns the separate-debuginfo ELF; `…/executable` returns the binary.
//! We fetch `debuginfo` (DWARF lives there); the executable path is the
//! `exec-upload` follow-on (spec §8) and is not fetched here.

use std::path::{Path, PathBuf};
use std::time::Duration;

use super::{SymbolizeError, SymbolizerConfig};

/// On-disk cache of fetched debuginfo, keyed by `build_id`, sharded by the
/// first byte so a directory does not accumulate millions of entries.
pub struct BuildIdCache {
    root: PathBuf,
}

impl BuildIdCache {
    pub fn new(root: PathBuf) -> Result<Self, SymbolizeError> {
        std::fs::create_dir_all(&root).map_err(|e| SymbolizeError::Io(e.to_string()))?;
        Ok(Self { root })
    }

    #[must_use]
    pub fn path_for(&self, build_id: &str) -> PathBuf {
        let shard = build_id.get(0..2).unwrap_or("00");
        let rest = build_id.get(2..).unwrap_or(build_id);
        self.root.join(shard).join(format!("{rest}.debuginfo"))
    }

    #[must_use]
    pub fn contains(&self, build_id: &str) -> bool {
        self.path_for(build_id).exists()
    }

    /// Atomically store `bytes` for `build_id` (tempfile in the same shard dir,
    /// then rename — so a concurrent reader never sees a partial file).
    pub fn store(&self, build_id: &str, bytes: &[u8]) -> Result<PathBuf, SymbolizeError> {
        let dst = self.path_for(build_id);
        let dir = dst.parent().unwrap_or(Path::new("."));
        std::fs::create_dir_all(dir).map_err(|e| SymbolizeError::Io(e.to_string()))?;
        let tmp = dir.join(format!(".{build_id}.tmp"));
        std::fs::write(&tmp, bytes).map_err(|e| SymbolizeError::Io(e.to_string()))?;
        std::fs::rename(&tmp, &dst).map_err(|e| SymbolizeError::Io(e.to_string()))?;
        Ok(dst)
    }
}

/// Fetches separate debuginfo by `build_id` from a debuginfod federation.
pub struct DebuginfodClient {
    http: reqwest::Client,
    servers: Vec<String>,
    cache: BuildIdCache,
    timeout: Duration,
}

impl DebuginfodClient {
    pub fn new(cfg: &SymbolizerConfig) -> Result<Self, SymbolizeError> {
        let http = reqwest::Client::builder()
            .timeout(cfg.fetch_timeout)
            .build()
            .map_err(|e| SymbolizeError::Fetch(e.to_string()))?;
        Ok(Self {
            http,
            servers: cfg.servers.clone(),
            cache: BuildIdCache::new(cfg.cache_dir.clone())?,
            timeout: cfg.fetch_timeout,
        })
    }

    /// Return the on-disk path to the debuginfo ELF for `build_id`. Cache hit ⇒
    /// no HTTP. `NotFound` when every server 404s.
    pub async fn fetch_debuginfo(&self, build_id: &str) -> Result<PathBuf, SymbolizeError> {
        if self.cache.contains(build_id) {
            return Ok(self.cache.path_for(build_id));
        }
        for server in &self.servers {
            let url = format!(
                "{}/buildid/{}/debuginfo",
                server.trim_end_matches('/'),
                build_id
            );
            let resp = match self.http.get(&url).send().await {
                Ok(r) => r,
                Err(e) => {
                    tracing::debug!(%url, error = %e, "debuginfod request failed");
                    continue;
                }
            };
            if resp.status() == reqwest::StatusCode::NOT_FOUND {
                continue;
            }
            if !resp.status().is_success() {
                tracing::debug!(%url, status = %resp.status(), "debuginfod non-200");
                continue;
            }
            let bytes = resp
                .bytes()
                .await
                .map_err(|e| SymbolizeError::Fetch(e.to_string()))?;
            return self.cache.store(build_id, &bytes);
        }
        Err(SymbolizeError::NotFound(build_id.to_string()))
    }
}

#[allow(dead_code)]
fn _timeout_is_used(c: &DebuginfodClient) -> Duration {
    c.timeout
}
```

> **Verify-against-version note (reqwest 0.13):** `Client::builder().timeout(..).build()`, `get(url).send().await`, `resp.status()`, `resp.bytes().await` are the stable async surface. If `reqwest` 0.13 renames `StatusCode::NOT_FOUND` or the `bytes()` future, adjust — the test pins the **behavior** (cache-hit avoids a second HTTP; all-404 ⇒ `NotFound`), not the method spelling. The `_timeout_is_used` shim silences `dead_code` until the field is read elsewhere; delete it once `timeout` is otherwise used.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-profiles --test symbolize_debuginfod`
Expected: PASS (2 tests; `hits == 1` after the second fetch proves the cache).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): debuginfod client + on-disk build-id cache"
```

---

### Task 3: `ElfModule` — DWARF/ELF/.gopclntab resolve + demangle + inline expansion

**Files:**
- Modify: `crates/profiles/src/symbolize/dwarf.rs`
- Create: `crates/profiles/tests/support/fixture_elf.rs`
- Create: `crates/profiles/tests/symbolize_dwarf.rs`

**Interfaces:**
- Consumes: `SymbolizeError`, `crabka_pprof::Frame`.
- Produces:
  - `pub struct ElfModule { /* owned debuginfo bytes + addr2line::Context + symtab index */ }`
  - `impl ElfModule { pub fn from_path(path: &std::path::Path) -> Result<Self, SymbolizeError>; pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, SymbolizeError>; pub fn resolve_addr(&self, address: u64) -> Vec<crabka_pprof::Frame>; }`
  - `resolve_addr` returns **leaf-first** frames; a DWARF inline chain expands to multiple frames (innermost first); falls back to `.symtab`/`.dynsym` then `.gopclntab` when DWARF has no line info; demangles each `function`.
  - `pub(crate) fn demangle(name: &str) -> String` — Rust (`rustc-demangle`) then C++ (`cpp_demangle`) then identity.

- [ ] **Step 1: Write the fixture synthesizer**

Create `crates/profiles/tests/support/fixture_elf.rs` — builds a minimal ELF with a `.symtab` symbol at a known address using `object::write`, plus a mangled Rust symbol, so the test does not need a vendored binary checked into the repo.

```rust
//! Synthesize a tiny ELF (symbol table only) with known symbol addresses, so
//! the symbolization tests need no vendored binary. DWARF line+inline coverage
//! is asserted separately against a checked-in fixture (see note in the test).

use object::write::{Object, StandardSection, Symbol, SymbolSection};
use object::{
    Architecture, BinaryFormat, Endianness, SymbolFlags, SymbolKind, SymbolScope,
};

/// A symbol we plant and then look up by address.
pub struct PlantedSymbol {
    pub mangled: &'static str,
    pub demangled_contains: &'static str,
    pub address: u64,
    pub size: u64,
}

/// Build an ELF byte image whose `.symtab` contains `syms` at the given
/// addresses, in a `.text` section. Returns the ELF bytes.
#[must_use]
pub fn build_symtab_elf(syms: &[PlantedSymbol]) -> Vec<u8> {
    let mut obj = Object::new(BinaryFormat::Elf, Architecture::X86_64, Endianness::Little);
    let text = obj.section_id(StandardSection::Text);
    // Reserve enough bytes for the highest address+size.
    let max = syms.iter().map(|s| s.address + s.size).max().unwrap_or(0);
    let data = vec![0x90_u8; max as usize]; // NOP sled placeholder
    obj.append_section_data(text, &data, 1);
    for s in syms {
        obj.add_symbol(Symbol {
            name: s.mangled.as_bytes().to_vec(),
            value: s.address,
            size: s.size,
            kind: SymbolKind::Text,
            scope: SymbolScope::Dynamic,
            weak: false,
            section: SymbolSection::Section(text),
            flags: SymbolFlags::None,
        });
    }
    obj.write().expect("write fixture ELF")
}
```

> **Verify-against-version note (object 0.36 write API):** `object::write::{Object, Symbol, StandardSection, SymbolSection}` field names (`value`/`size`/`kind`/`scope`/`weak`/`section`/`flags`) and `Object::new(format, arch, endian)` / `section_id` / `append_section_data` / `add_symbol` / `write()` are the 0.36 surface. If a field/method differs, adjust the synthesizer — the test pins the **behavior** (address `0x1000` resolves to the planted function name), not the exact builder spelling. If the write API proves too fiddly for the inline-DWARF case, **vendor a ~3KB precompiled fixture** `tests/fixtures/inline.debug` (built once from a 2-line C file with `-g -O2 -gdwarf-4`, command recorded in a sibling `inline.build.txt`) and load it by path — this is the spec-sanctioned "vendor a tiny test binary or synthesize" choice; record which was used in the Self-review.

- [ ] **Step 2: Write the failing tests**

Create `crates/profiles/tests/symbolize_dwarf.rs`:

```rust
#[path = "support/fixture_elf.rs"]
mod fixture_elf;

use crabka_profiles::symbolize::dwarf::ElfModule;
use fixture_elf::{build_symtab_elf, PlantedSymbol};
use assert2::assert;

#[test]
fn symtab_address_resolves_to_demangled_function() {
    // A mangled Rust symbol: `_RNvCs.._4core3fmt...` style. Use a real, stable
    // rustc-demangle input so the assertion does not depend on a private hash.
    let syms = [PlantedSymbol {
        mangled: "_ZN4core3fmt9Formatter3pad17h0000000000000000E", // legacy v0/v1 C++-style Rust mangling
        demangled_contains: "core::fmt::Formatter::pad",
        address: 0x1000,
        size: 0x40,
    }];
    let bytes = build_symtab_elf(&syms);
    let m = ElfModule::from_bytes(bytes).unwrap();

    let frames = m.resolve_addr(0x1010); // inside [0x1000, 0x1040)
    assert!(!frames.is_empty());
    assert!(frames[0].function.contains("core::fmt::Formatter::pad"));
}

#[test]
fn unknown_address_yields_no_frames() {
    let bytes = build_symtab_elf(&[PlantedSymbol {
        mangled: "_ZN3foo3barEv",
        demangled_contains: "foo::bar",
        address: 0x2000,
        size: 0x10,
    }]);
    let m = ElfModule::from_bytes(bytes).unwrap();
    assert!(m.resolve_addr(0x9999).is_empty());
}

#[test]
fn demangle_handles_rust_and_cpp_and_identity() {
    use crabka_profiles::symbolize::dwarf::demangle;
    // C++ Itanium
    assert!(demangle("_ZN3foo3barEv") == "foo::bar()");
    // already-plain
    assert!(demangle("main") == "main");
}
```

> **Inline-frame expansion test (gated on the DWARF fixture).** Add a fourth test `inline_chain_expands_innermost_first` that loads the vendored/synthesized `inline.debug`, calls `resolve_addr(known_inlined_pc)`, and asserts `frames.len() >= 2` with `frames[0].function` = the inlined callee and `frames[1].function` = the caller (DWARF inline order). This is the headline inline-expansion assertion; if the fixture is synthesized rather than vendored, the test builder records the PC it planted. Mark it `#[test]` (no ignore) once the fixture lands; if the DWARF-write fixture is deferred, mark it `#[ignore = "needs vendored inline.debug fixture"]` with a `// TODO(slice7-fixture)` and note it in the Self-review — never delete the assertion.

- [ ] **Step 3: Run to verify it fails**

Run: `cargo test -p crabka-profiles --test symbolize_dwarf`
Expected: FAIL — `cannot find struct ElfModule`.

- [ ] **Step 4: Implement `dwarf.rs`**

The `addr2line::Context` is the DWARF line+inline engine; `object::File` is the ELF parser; the `.symtab`/`.dynsym` fallback uses `object`'s symbol iterator; `.gopclntab` is read via a small `gimli`-free section scan (Go's own format). Demangle each name.

```rust
//! ELF/DWARF/`.gopclntab` address → frame resolution for one module.
//!
//! Priority per address: (1) DWARF line+inline via `addr2line::Context`;
//! (2) ELF `.symtab`/`.dynsym` symbol containing the address; (3) Go
//! `.gopclntab` for Go binaries stripped of DWARF. Names are demangled.

use std::path::Path;

use addr2line::Context;
use object::{Object, ObjectSection, ObjectSymbol, SymbolKind};

use super::SymbolizeError;
use crabka_pprof::Frame;

/// One resolvable module: owns the debuginfo bytes and the DWARF context built
/// over them, plus a sorted symbol table for the fallback path.
pub struct ElfModule {
    // `addr2line::Context` borrows the parsed object, which borrows `bytes`.
    // We self-own via `owning` to keep `ElfModule: 'static` without `unsafe`:
    // store the bytes, and rebuild the borrow-scoped pieces behind an owner.
    inner: ModuleInner,
}

// `addr2line::Context<R>` is generic over the gimli reader; building it from an
// `object::File` borrow is the supported path. To avoid a self-referential
// struct we use the `addr2line::ObjectContext`-style owned wrapper if the
// version provides one; otherwise we keep `bytes` boxed and leak-free via an
// `ouroboros`-free manual owner. The simplest safe shape: parse + build inside
// `resolve_addr` is too slow; instead we precompute a `Vec<SymbolRow>` (always
// safe, owned) and an `Option<DwarfLines>` snapshot. See the note below.
struct ModuleInner {
    symbols: Vec<SymbolRow>,             // sorted by address, always available
    dwarf: Option<DwarfIndex>,           // present when DWARF lines parsed
    gopcln: Option<GoLineTable>,         // present for Go binaries
}

struct SymbolRow {
    address: u64,
    size: u64,
    name: String, // demangled
}

/// A flattened DWARF line+inline index: for each covered address range, the
/// innermost-first frame list. Precomputed at load so `resolve_addr` is a
/// lookup and `ElfModule` owns no borrow of `bytes`.
struct DwarfIndex {
    // (range, frames innermost-first) — built by walking addr2line once over
    // every function's covered PCs is too broad; instead we keep the parsed
    // bytes and a built Context. To stay `unsafe`-free we rebuild the Context
    // lazily per call from cached `bytes`. See `resolve_addr`.
    bytes: std::sync::Arc<Vec<u8>>,
}

struct GoLineTable {
    bytes: std::sync::Arc<Vec<u8>>,
    text_start: u64,
}

impl ElfModule {
    pub fn from_path(path: &Path) -> Result<Self, SymbolizeError> {
        let bytes = std::fs::read(path).map_err(|e| SymbolizeError::Io(e.to_string()))?;
        Self::from_bytes(bytes)
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, SymbolizeError> {
        let file = object::File::parse(&*bytes)
            .map_err(|e| SymbolizeError::Object(e.to_string()))?;

        // (1) Build the always-available demangled symbol table.
        let mut symbols: Vec<SymbolRow> = file
            .symbols()
            .chain(file.dynamic_symbols())
            .filter(|s| s.kind() == SymbolKind::Text && s.size() > 0)
            .filter_map(|s| {
                let name = s.name().ok()?;
                Some(SymbolRow {
                    address: s.address(),
                    size: s.size(),
                    name: demangle(name),
                })
            })
            .collect();
        symbols.sort_by_key(|r| r.address);

        // (2) DWARF: present iff there is a `.debug_line` section.
        let has_dwarf = file.section_by_name(".debug_line").is_some();
        let arc = std::sync::Arc::new(bytes);
        let dwarf = has_dwarf.then(|| DwarfIndex { bytes: arc.clone() });

        // (3) Go `.gopclntab` for stripped Go binaries.
        let gopcln = file.section_by_name(".gopclntab").and_then(|sec| {
            let text_start = file
                .section_by_name(".text")
                .map(|t| t.address())
                .unwrap_or(0);
            sec.data().ok().map(|_| GoLineTable {
                bytes: arc.clone(),
                text_start,
            })
        });

        Ok(Self {
            inner: ModuleInner { symbols, dwarf, gopcln },
        })
    }

    /// Resolve `address` to leaf-first frames (inline chain innermost-first).
    #[must_use]
    pub fn resolve_addr(&self, address: u64) -> Vec<Frame> {
        // (1) DWARF line+inline — the richest result.
        if let Some(d) = &self.inner.dwarf {
            if let Ok(frames) = dwarf_frames(&d.bytes, address) {
                if !frames.is_empty() {
                    return frames;
                }
            }
        }
        // (2) symbol-table fallback (no file/line, just the function).
        if let Some(row) = self
            .inner
            .symbols
            .iter()
            .find(|r| address >= r.address && address < r.address + r.size)
        {
            return vec![Frame {
                function: row.name.clone(),
                file: String::new(),
                line: 0,
            }];
        }
        // (3) Go line table.
        if let Some(go) = &self.inner.gopcln {
            if let Some(f) = go_frame(go, address) {
                return vec![f];
            }
        }
        Vec::new()
    }
}

/// Build an `addr2line::Context` over `bytes` and resolve `address` to a
/// leaf-first frame list with inline expansion.
fn dwarf_frames(bytes: &[u8], address: u64) -> Result<Vec<Frame>, SymbolizeError> {
    let file = object::File::parse(bytes).map_err(|e| SymbolizeError::Object(e.to_string()))?;
    let ctx = Context::new(&file).map_err(|e| SymbolizeError::Dwarf(e.to_string()))?;
    let mut frames = ctx
        .find_frames(address)
        .skip_all_loads()
        .map_err(|e| SymbolizeError::Dwarf(e.to_string()))?;
    let mut out = Vec::new();
    // `find_frames` yields innermost (leaf) first — exactly the order we want.
    while let Some(frame) = frames.next().map_err(|e| SymbolizeError::Dwarf(e.to_string()))? {
        let function = frame
            .function
            .as_ref()
            .and_then(|f| f.raw_name().ok())
            .map(|n| demangle(&n))
            .unwrap_or_default();
        let (file_name, line) = frame
            .location
            .as_ref()
            .map(|l| (l.file.unwrap_or("").to_string(), l.line.unwrap_or(0) as i32))
            .unwrap_or((String::new(), 0));
        out.push(Frame { function, file: file_name, line });
    }
    Ok(out)
}

/// Minimal Go `.gopclntab` lookup (function name only; line decoding is the
/// follow-on). Returns the function whose entry covers `address`.
fn go_frame(_go: &GoLineTable, _address: u64) -> Option<Frame> {
    // PLACEHOLDER (flagged): full `.gopclntab` decode (magic 0xFFFFFFF1 for Go
    // 1.18+, pcHeader, funcnametab, pctab) is mechanical but ~150 LOC; the
    // symbol-table path already covers non-stripped Go binaries. Implemented in
    // a follow-up; see Self-review. Returning None here means Go-stripped
    // binaries fall through to "no frames", never wrong frames.
    None
}

/// Demangle a Rust (v0/legacy) or C++ (Itanium) symbol, else return it as-is.
#[must_use]
pub fn demangle(name: &str) -> String {
    // Rust first: `rustc_demangle` recognizes both `_R…` (v0) and the legacy
    // `_ZN…17h<hash>E` Rust scheme and renders without the trailing hash.
    let rust = rustc_demangle::demangle(name).to_string();
    if rust != name {
        // Strip the trailing `::h<hash>` if `{:#}` was not used.
        return format!("{:#}", rustc_demangle::demangle(name));
    }
    // C++ Itanium.
    if let Ok(sym) = cpp_demangle::Symbol::new(name) {
        if let Ok(s) = sym.demangle(&cpp_demangle::DemangleOptions::default()) {
            return s;
        }
    }
    name.to_string()
}
```

> **Verify-against-version note (addr2line 0.24 / object 0.36 — DO THIS before believing the code):** the DWARF surface used here is `addr2line::Context::new(&object::File)`, `Context::find_frames(addr).skip_all_loads()? -> FrameIter`, `FrameIter::next()? -> Option<Frame>`, `Frame::function: Option<FunctionName>` with `FunctionName::raw_name() -> Result<Cow<str>>`, and `Frame::location: Option<Location { file: Option<&str>, line: Option<u32>, column }>`. These are the **0.24** shapes; earlier/later majors moved `find_frames` (it gained the `LookupContinuation`/`skip_all_loads` split in 0.22+) and renamed `FunctionName`. If `find_frames` does not have `.skip_all_loads()`, the version is < 0.22 — bump the pin. The `object` reader surface is `File::parse(&[u8])`, `.symbols()/.dynamic_symbols()`, `Symbol::{address,size,kind,name}`, `.section_by_name(..).data()`. **Pin by the fixture test, not by trusting this listing** — when a method name is off, fix the call and keep the test's resolved-name assertion. The self-referential-borrow concern (`Context` borrows `File` borrows `bytes`) is sidestepped by **rebuilding the `Context` per `resolve_addr` from cached `Arc<Vec<u8>>`** (simple, `unsafe`-free; the resolved-frame LRU in Task 4 makes the rebuild cost a per-distinct-id one-off). If rebuild-per-call proves hot in a benchmark, the optimization is `addr2line`'s owned `ObjectContext` (if present in 0.24) or the `self_cell` crate — flagged, not done here.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-profiles --test symbolize_dwarf`
Expected: PASS (symbol-table resolve + unknown-address-empty + demangle; the inline test passes if the DWARF fixture landed, else is `#[ignore]`d with the flagged TODO).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): ElfModule DWARF/symtab/gopclntab resolve + demangle"
```

---

### Task 4: `Symbolizer` — the `SymbolSource` wrapper with lazy resolve + module/frame caches

**Files:**
- Modify: `crates/profiles/src/symbolize/mod.rs`
- Create: `crates/profiles/tests/symbolize_lazy.rs`
- (possibly) Modify: `crates/pprof/src/symbols.rs` — add `unresolved_locations` accessor (see Contract gap)

**Interfaces:**
- Consumes: `crabka_pprof::{SymbolSource, Frame}`, `DebuginfodClient`, `ElfModule`, `SymbolizerConfig`.
- Produces:
  - `pub struct Symbolizer { inner: Arc<dyn SymbolSource>, client: Arc<DebuginfodClient>, modules: Mutex<HashMap<String, ModuleSlot>>, resolved: Mutex<LruCache<(String,u64), Vec<Frame>>>, rt: tokio::runtime::Handle }`
  - `impl Symbolizer { pub fn new(inner: Arc<dyn SymbolSource>, cfg: SymbolizerConfig, rt: tokio::runtime::Handle) -> Result<Self, SymbolizeError>; }`
  - `impl crabka_pprof::SymbolSource for Symbolizer { fn resolve(&self, partition: u64, id: u32) -> Vec<Frame> }` — delegates to `inner`, then for each location the inner source flags as unresolved (`has_functions == false`), substitutes the native `(build_id, address)` resolution (cache → module → debuginfod fetch). **Lazy:** only ids actually asked for trigger any fetch.

- [ ] **Step 1: Decide the Contract gap (preferred vs fallback)**

Read `crates/pprof/src/symbols.rs`. If `SymbolDb` already exposes a way to enumerate the unresolved `(build_id, address, frame_slot)` for a `(partition, id)`, consume it. If not, add **one** method to `crabka-pprof`:

```rust
// crates/pprof/src/symbols.rs — in impl SymbolDb
/// For a stacktrace id, the locations whose mapping has `has_functions == false`,
/// as `(build_id, address, frame_index_in_resolve_output)`. Empty when the whole
/// stack is pre-symbolized. The query-time native symbolizer (crabka-profiles
/// Slice 7) uses this to know which frames to resolve via debuginfod/DWARF.
#[must_use]
pub fn unresolved_locations(&self, partition: u64, id: u32) -> Vec<(String, u64, usize)> { /* … */ }
```

Record in the task's **Contract gap** note which path was taken. The fallback (function=="" sentinel) is brittle (i32-truncated address) — prefer the accessor.

- [ ] **Step 2: Write the failing lazy test (fake inner source, no real ELF needed)**

Create `crates/profiles/tests/symbolize_lazy.rs`:

```rust
use crabka_pprof::{Frame, SymbolSource};
use crabka_profiles::symbolize::{Symbolizer, testkit};
use crabka_profiles::SymbolizerConfig;
use assert2::assert;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A fake inner `SymbolSource`: id 1 is fully pre-symbolized; id 2 has one
/// unresolved native frame `(build_id="BID", address=0x1010)`.
struct FakeInner {
    resolves: Arc<AtomicUsize>,
}
impl SymbolSource for FakeInner {
    fn resolve(&self, _partition: u64, id: u32) -> Vec<Frame> {
        self.resolves.fetch_add(1, Ordering::SeqCst);
        match id {
            1 => vec![Frame { function: "pre::symbolized".into(), file: "a.rs".into(), line: 1 }],
            2 => vec![Frame { function: String::new(), file: String::new(), line: 0 }], // unresolved slot
            _ => vec![],
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn never_viewed_id_is_never_fetched_viewed_id_is_substituted() {
    // `testkit` lets the test inject a fake module resolver instead of debuginfod,
    // so this test is hermetic and asserts the LAZY + SUBSTITUTE behavior only.
    let fetches = Arc::new(AtomicUsize::new(0));
    let resolver = testkit::StaticResolver::new(
        fetches.clone(),
        // (build_id, address) -> frames
        [( ("BID".to_string(), 0x1010_u64), vec![Frame {
            function: "native::frame".into(), file: "n.c".into(), line: 42 }])]
            .into_iter().collect(),
    );

    let inner = Arc::new(FakeInner { resolves: Arc::new(AtomicUsize::new(0)) });
    let sym = Symbolizer::with_resolver(
        inner.clone(),
        SymbolizerConfig::default(),
        Arc::new(resolver),
        // unresolved-locations map: id 2, slot 0 -> (BID, 0x1010)
        testkit::unresolved_map([(2_u32, vec![("BID".to_string(), 0x1010_u64, 0_usize)])]),
    );

    // Resolve only id 1 (pre-symbolized). No native fetch must happen.
    let f1 = sym.resolve(0, 1);
    assert!(f1[0].function == "pre::symbolized");
    assert!(fetches.load(Ordering::SeqCst) == 0); // LAZY: nothing native touched

    // Now resolve id 2 — the unresolved slot is substituted with the native frame.
    let f2 = sym.resolve(0, 2);
    assert!(f2.len() == 1);
    assert!(f2[0].function == "native::frame");
    assert!(f2[0].line == 42);
    assert!(fetches.load(Ordering::SeqCst) == 1);

    // Second resolve of id 2 is served from the resolved-frame cache — no refetch.
    let _ = sym.resolve(0, 2);
    assert!(fetches.load(Ordering::SeqCst) == 1);
}
```

This test drives the wrapper through a **`testkit` seam** (a `ModuleResolver` trait the real path implements via `DebuginfodClient`+`ElfModule`, and the test implements via a static map) so the lazy/substitute/cache logic is tested without ELF bytes or HTTP. The ELF and debuginfod paths are already pinned by Tasks 2–3.

- [ ] **Step 3: Run to verify it fails**

Run: `cargo test -p crabka-profiles --test symbolize_lazy`
Expected: FAIL — `cannot find Symbolizer::with_resolver` / `testkit`.

- [ ] **Step 4: Implement the `Symbolizer` + the `ModuleResolver` seam + `testkit`**

Append to `crates/profiles/src/symbolize/mod.rs`:

```rust
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use crabka_pprof::{Frame, SymbolSource};
use lru::LruCache;

use self::dwarf::ElfModule;
use self::fetch::DebuginfodClient;

/// How the symbolizer turns a `(build_id, address)` into frames. The production
/// impl fetches via debuginfod and parses with `ElfModule`; tests inject a
/// static map. This seam keeps the lazy/cache logic testable without ELF/HTTP.
pub trait ModuleResolver: Send + Sync {
    fn resolve(&self, build_id: &str, address: u64) -> Vec<Frame>;
}

/// Production resolver: debuginfod fetch + DWARF/ELF parse, module-cached.
pub struct DebuginfodResolver {
    client: Arc<DebuginfodClient>,
    modules: Mutex<HashMap<String, Option<Arc<ElfModule>>>>, // None = known-NotFound
    rt: tokio::runtime::Handle,
}

impl DebuginfodResolver {
    #[must_use]
    pub fn new(client: Arc<DebuginfodClient>, rt: tokio::runtime::Handle) -> Self {
        Self { client, modules: Mutex::new(HashMap::new()), rt }
    }

    fn module(&self, build_id: &str) -> Option<Arc<ElfModule>> {
        if let Some(slot) = self.modules.lock().expect("modules lock").get(build_id) {
            return slot.clone();
        }
        // Fetch (blocking on the runtime — `resolve` is sync per SymbolSource).
        let client = self.client.clone();
        let bid = build_id.to_string();
        let loaded = self.rt.block_on(async move { client.fetch_debuginfo(&bid).await });
        let module = match loaded {
            Ok(path) => ElfModule::from_path(&path).ok().map(Arc::new),
            Err(_) => None,
        };
        self.modules
            .lock()
            .expect("modules lock")
            .insert(build_id.to_string(), module.clone());
        module
    }
}

impl ModuleResolver for DebuginfodResolver {
    fn resolve(&self, build_id: &str, address: u64) -> Vec<Frame> {
        self.module(build_id)
            .map(|m| m.resolve_addr(address))
            .unwrap_or_default()
    }
}

/// Per-`(partition,id)` map of unresolved native locations the inner `SymbolDb`
/// could not symbolize: `(build_id, address, frame_slot)`. Built from the
/// block's symbol-DB at querier-construction time (Contract gap §Task 4).
pub type UnresolvedMap = Arc<dyn Fn(u32) -> Vec<(String, u64, usize)> + Send + Sync>;

/// The query-time native symbolizer. Implements `SymbolSource` by delegating to
/// `inner` (the block's `SymbolDb`) and substituting native frames for the
/// locations `inner` flagged unresolved. Lazy + LRU-cached.
pub struct Symbolizer {
    inner: Arc<dyn SymbolSource>,
    resolver: Arc<dyn ModuleResolver>,
    unresolved: UnresolvedMap,
    resolved: Mutex<LruCache<(String, u64), Vec<Frame>>>,
}

impl Symbolizer {
    /// Production constructor: debuginfod + DWARF.
    pub fn new(
        inner: Arc<dyn SymbolSource>,
        cfg: SymbolizerConfig,
        rt: tokio::runtime::Handle,
        unresolved: UnresolvedMap,
    ) -> Result<Self, SymbolizeError> {
        let client = Arc::new(DebuginfodClient::new(&cfg)?);
        let resolver = Arc::new(DebuginfodResolver::new(client, rt));
        Ok(Self::with_resolver(inner, cfg, resolver, unresolved))
    }

    /// Constructor with an injected resolver (tests; or an `exec-upload` path).
    #[must_use]
    pub fn with_resolver(
        inner: Arc<dyn SymbolSource>,
        cfg: SymbolizerConfig,
        resolver: Arc<dyn ModuleResolver>,
        unresolved: UnresolvedMap,
    ) -> Self {
        let cap = NonZeroUsize::new(cfg.resolved_cache_cap.max(1)).expect("cap >= 1");
        Self {
            inner,
            resolver,
            unresolved,
            resolved: Mutex::new(LruCache::new(cap)),
        }
    }

    fn resolve_native(&self, build_id: &str, address: u64) -> Vec<Frame> {
        let key = (build_id.to_string(), address);
        if let Some(hit) = self.resolved.lock().expect("resolved lock").get(&key) {
            return hit.clone();
        }
        let frames = self.resolver.resolve(build_id, address);
        self.resolved
            .lock()
            .expect("resolved lock")
            .put(key, frames.clone());
        frames
    }
}

impl SymbolSource for Symbolizer {
    fn resolve(&self, partition: u64, id: u32) -> Vec<Frame> {
        let mut frames = self.inner.resolve(partition, id);
        let unresolved = (self.unresolved)(id);
        if unresolved.is_empty() {
            return frames; // fully pre-symbolized — LAZY: no native work
        }
        // Substitute each unresolved slot with native frames (inline expansion
        // can replace one slot with several frames; splice in order).
        let mut spliced = Vec::with_capacity(frames.len());
        let mut by_slot: HashMap<usize, (String, u64)> = unresolved
            .into_iter()
            .map(|(b, a, slot)| (slot, (b, a)))
            .collect();
        for (i, f) in frames.drain(..).enumerate() {
            if let Some((build_id, address)) = by_slot.remove(&i) {
                let native = self.resolve_native(&build_id, address);
                if native.is_empty() {
                    spliced.push(f); // keep the raw frame if resolution failed
                } else {
                    spliced.extend(native);
                }
            } else {
                spliced.push(f);
            }
        }
        spliced
    }
}

/// Test-only helpers (a static resolver + an `UnresolvedMap` builder).
pub mod testkit {
    use super::{Frame, ModuleResolver, UnresolvedMap};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    pub struct StaticResolver {
        fetches: Arc<AtomicUsize>,
        table: HashMap<(String, u64), Vec<Frame>>,
    }
    impl StaticResolver {
        #[must_use]
        pub fn new(fetches: Arc<AtomicUsize>, table: HashMap<(String, u64), Vec<Frame>>) -> Self {
            Self { fetches, table }
        }
    }
    impl ModuleResolver for StaticResolver {
        fn resolve(&self, build_id: &str, address: u64) -> Vec<Frame> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            self.table
                .get(&(build_id.to_string(), address))
                .cloned()
                .unwrap_or_default()
        }
    }

    #[must_use]
    pub fn unresolved_map<const N: usize>(
        entries: [(u32, Vec<(String, u64, usize)>); N],
    ) -> UnresolvedMap {
        let map: HashMap<u32, Vec<(String, u64, usize)>> = entries.into_iter().collect();
        Arc::new(move |id: u32| map.get(&id).cloned().unwrap_or_default())
    }
}
```

Add `lru = "0.12"` to the workspace deps (Step 1 of this task's commit) and `lru = { workspace = true }` to `crates/profiles/Cargo.toml`. Re-export from `mod.rs`: nothing new public beyond `Symbolizer`, `ModuleResolver`, `DebuginfodResolver`, `testkit` — add `pub use symbolize::Symbolizer;` to `lib.rs`.

> **Verify note (the `block_on` reentrancy):** `DebuginfodResolver::resolve` is **sync** (the `SymbolSource` contract is sync) but fetch is async, so it `rt.block_on`s. This must run on a runtime thread that is **not** the one driving the querier's request future, or it deadlocks. The querier (Slice 5) calls `engine.select_merge_stacktraces` which calls `SymbolSource::resolve` from inside `spawn_blocking` (the fold step is CPU-bound + now does blocking fetches) — **flag this requirement to Slice 5**: native symbolization resolution must happen under `tokio::task::spawn_blocking`, never directly on an async worker. The lazy test uses `flavor = "multi_thread"` so `block_on` is legal; document the constraint in the `Symbolizer` doc-comment.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-profiles --test symbolize_lazy`
Expected: PASS — never-viewed id triggers 0 fetches; viewed id substitutes the native frame; re-resolve hits the LRU (still 1 fetch).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/ crates/pprof/ Cargo.toml
git commit -m "feat(profiles): Symbolizer SymbolSource wrapper — lazy native resolve + caches"
```

---

### Task 5: `--target symbolizer` role + in-querier stage wiring

**Files:**
- Modify: `crates/profiles/src/symbolize/role.rs`
- Modify: `crates/profiles/src/bin/crabka-profiles.rs` (add the `symbolizer` arm)

**Interfaces:**
- Consumes: `Symbolizer`, `SymbolizerConfig`, the existing `--target` `clap` enum + serve scaffold (Slices 4–6).
- Produces:
  - `pub struct SymbolizerRole { cfg: SymbolizerConfig }` with `pub fn from_env() -> Result<Self, SymbolizeError>` (reads `DEBUGINFOD_URLS` / `CRABKA_DEBUGINFOD_CACHE`) and `pub async fn run(self, shutdown: tokio_util::sync::CancellationToken) -> Result<(), SymbolizeError>`.
  - `pub fn build_query_symbolizer(block_symbols: Arc<dyn SymbolSource>, cfg: SymbolizerConfig, rt: Handle, unresolved: UnresolvedMap) -> Result<Arc<dyn SymbolSource>, SymbolizeError>` — the factory the querier (Slice 5) calls per block to wrap its `SymbolDb` in a `Symbolizer`. **This is the in-querier stage** (the default); the standalone role is the optional split-out.

- [ ] **Step 1: Write the failing test**

Create `crates/profiles/tests/symbolize_role.rs`:

```rust
use crabka_pprof::{Frame, SymbolSource};
use crabka_profiles::symbolize::{build_query_symbolizer, role::SymbolizerRole, testkit};
use crabka_profiles::SymbolizerConfig;
use assert2::assert;
use std::sync::Arc;

struct Pre;
impl SymbolSource for Pre {
    fn resolve(&self, _p: u64, _id: u32) -> Vec<Frame> {
        vec![Frame { function: "x".into(), file: String::new(), line: 0 }]
    }
}

#[test]
fn from_env_reads_debuginfod_urls() {
    // SAFETY of test isolation: set+remove around the call.
    std::env::set_var("DEBUGINFOD_URLS", "https://a.example/ https://b.example/");
    let role = SymbolizerRole::from_env().unwrap();
    assert!(role.servers() == vec!["https://a.example/".to_string(), "https://b.example/".to_string()]);
    std::env::remove_var("DEBUGINFOD_URLS");
}

#[tokio::test(flavor = "multi_thread")]
async fn build_query_symbolizer_wraps_block_source() {
    let wrapped = build_query_symbolizer(
        Arc::new(Pre),
        SymbolizerConfig::default(),
        tokio::runtime::Handle::current(),
        testkit::unresolved_map([]), // nothing unresolved -> pass-through
    )
    .unwrap();
    // A fully pre-symbolized stack passes through unchanged.
    assert!(wrapped.resolve(0, 7)[0].function == "x");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --test symbolize_role`
Expected: FAIL — `cannot find SymbolizerRole` / `build_query_symbolizer`.

- [ ] **Step 3: Implement `role.rs` + the binary arm**

```rust
//! The `--target symbolizer` role and the in-querier stage factory.
//!
//! Default deployment: the symbolizer is an **in-querier stage** — the querier
//! wraps each block's `SymbolDb` with `build_query_symbolizer`. The standalone
//! role exists for operators who want to scale symbolization (and its
//! debuginfod cache) independently; it runs the same factory behind a small
//! Connect/HTTP resolve service. This file ships the factory + role config;
//! the standalone service surface is a thin wrapper (flagged below).

use std::sync::Arc;

use crabka_pprof::SymbolSource;
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

use super::{Symbolizer, SymbolizeError, SymbolizerConfig, UnresolvedMap};

/// Build the query-time symbolizer that wraps a single block's `SymbolDb`.
/// Called by the querier (Slice 5) when constructing a `ProfileScan`.
///
/// # Errors
/// Fails if the debuginfod client (cache dir / TLS) cannot be constructed.
pub fn build_query_symbolizer(
    block_symbols: Arc<dyn SymbolSource>,
    cfg: SymbolizerConfig,
    rt: Handle,
    unresolved: UnresolvedMap,
) -> Result<Arc<dyn SymbolSource>, SymbolizeError> {
    let sym = Symbolizer::new(block_symbols, cfg, rt, unresolved)?;
    Ok(Arc::new(sym))
}

/// The standalone `--target symbolizer` role.
pub struct SymbolizerRole {
    cfg: SymbolizerConfig,
}

impl SymbolizerRole {
    /// Build from env: `DEBUGINFOD_URLS` (space/comma-separated) +
    /// `CRABKA_DEBUGINFOD_CACHE` (cache dir).
    ///
    /// # Errors
    /// Currently infallible beyond config parsing; returns `Result` for forward
    /// compatibility with cache-dir validation.
    pub fn from_env() -> Result<Self, SymbolizeError> {
        let mut cfg = SymbolizerConfig::default();
        if let Ok(urls) = std::env::var("DEBUGINFOD_URLS") {
            let servers: Vec<String> = urls
                .split([' ', ','])
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect();
            if !servers.is_empty() {
                cfg.servers = servers;
            }
        }
        if let Ok(dir) = std::env::var("CRABKA_DEBUGINFOD_CACHE") {
            cfg.cache_dir = dir.into();
        }
        Ok(Self { cfg })
    }

    #[must_use]
    pub fn servers(&self) -> Vec<String> {
        self.cfg.servers.clone()
    }

    /// Run the standalone symbolization service until `shutdown`.
    ///
    /// PLACEHOLDER (flagged): the standalone service surface (a Connect/HTTP
    /// `Resolve(build_id, addresses[]) -> frames[]` method that queriers call
    /// instead of resolving in-process) is a deployment-topology follow-on. The
    /// in-querier stage (`build_query_symbolizer`) is the shipped default and is
    /// fully tested. This method warms the debuginfod client + cache and idles
    /// until shutdown so the role binary is wireable end-to-end.
    ///
    /// # Errors
    /// Fails if the debuginfod client cannot be constructed.
    pub async fn run(self, shutdown: CancellationToken) -> Result<(), SymbolizeError> {
        let _client = super::fetch::DebuginfodClient::new(&self.cfg)?;
        tracing::info!(servers = ?self.cfg.servers, "symbolizer role ready (in-process stage is the default)");
        shutdown.cancelled().await;
        Ok(())
    }
}
```

In `crates/profiles/src/bin/crabka-profiles.rs`, add `Symbolizer` to the `--target` `clap` value enum and a match arm:

```rust
// in the Target enum (Slices 4–6 own it):
//   Distributor, BlockBuilder, Querier, QueryFrontend, Compactor, Symbolizer
Target::Symbolizer => {
    let role = crabka_profiles::symbolize::role::SymbolizerRole::from_env()?;
    role.run(shutdown.clone()).await?;
}
```

> **Contract gap (binary):** the `Target` enum + `shutdown: CancellationToken` + the `main` error type are owned by Slices 4–6. This task adds **only** the `Symbolizer` variant + arm. If the binary is not yet present (Slices 4–6 unlanded in this worktree), create a **minimal `crates/profiles/src/bin/crabka-profiles.rs`** with a `clap` `Target` enum carrying just the `Symbolizer` arm wired to `SymbolizerRole::run`, and flag it in the Self-review as "binary skeleton — merge with the Slice-4 binary when both land".

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-profiles --test symbolize_role && cargo build -p crabka-profiles --bin crabka-profiles`
Expected: PASS + binary builds with the `symbolizer` arm.

- [ ] **Step 5: Final whole-crate gate**

Run: `cargo test -p crabka-profiles && cargo clippy -p crabka-profiles --all-targets && cargo fmt -p crabka-profiles --check`
Expected: all PASS, no warnings, formatting clean.

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-profiles
git add crates/profiles/
git commit -m "feat(profiles): --target symbolizer role + in-querier stage factory"
```

---

## Self-review

**Spec coverage (against §8 native symbolization + §11 Slice 7):**
- **`(build_id, address) → Frame`s** for `has_functions == false` mappings, resolved **lazily at query time** → Task 4 `Symbolizer` (the never-viewed-id ⇒ no-fetch headline). ✅
- **debuginfod fetch by `build_id`** (default `https://debuginfod.elfutils.org/`, configurable; `reqwest` + **on-disk cache**) → Task 2 `DebuginfodClient`/`BuildIdCache` (cache-hit-avoids-HTTP headline). ✅
- **ELF `.symtab`/`.dynsym` + DWARF (gimli/object/addr2line) + Go `.gopclntab` + demangle (C++/Rust) + inline-frame expansion** → Task 3 `ElfModule` (address→demangled-function + inline-chain headlines). ✅
- **Wrapped behind the same `SymbolSource` trait the in-block `SymbolDb` implements** → Task 4 `impl SymbolSource for Symbolizer` (the load-bearing seam; engine learns no new trait). ✅
- **Build-id index + cache resolved frames** → Task 2 sharded `BuildIdCache` (modules) + Task 4 resolved-frame `LruCache` (frames). ✅
- **Role binary `--target symbolizer` (or in-querier stage)** → Task 5 `SymbolizerRole` + `build_query_symbolizer` (in-querier stage is the default; standalone role wired). ✅
- **Headline tests** (address→frame against a fixture ELF, build-id lookup + cache, inline expansion) → Tasks 3, 2, 3 respectively. ✅
- **New deps `gimli`/`object`/`addr2line`/`reqwest`** → Task 1, each with a verify-against-version note. ✅

**Honest scope flagged (per spec §8 / Pyroscope #3715), not hidden:**
- **System/OSS-binary path shipped**; **customer-code symbolization** (executable upload + `addr2line` over user binaries debuginfod does not host) is the **`exec-upload` follow-on** — wired as the injectable `ModuleResolver`/`with_resolver` seam (so the exec path drops in without touching the engine), explicitly *not* claimed complete. Every scope-touching doc-comment says so.
- **`go_frame` is a flagged PLACEHOLDER** — the full `.gopclntab` pcHeader/funcnametab/pctab decode (~150 LOC, Go 1.18+ magic `0xFFFFFFF1`) is mechanical follow-on; non-stripped Go binaries are already covered by the `.symtab` path, and the placeholder returns `None` (never *wrong* frames). Flagged here and at the call site.
- **`SymbolizerRole::run` standalone service surface is a flagged PLACEHOLDER** — the in-querier stage (`build_query_symbolizer`) is the shipped, fully-tested default; the standalone Connect `Resolve(build_id, addrs[])` service is a deployment-topology follow-on. The role binary is wireable (warms client + idles until shutdown), not faked.

**Churn-prone surfaces — structured + behavior-pinned, not fabricated (CLAUDE.md):**
- **addr2line 0.24 / object 0.36 / gimli 0.31 DWARF API** (Task 3) — every method used (`Context::new`, `find_frames(addr).skip_all_loads()`, `FrameIter::next`, `Frame::{function,location}`, `FunctionName::raw_name`, `object::File::parse`/`symbols`/`section_by_name`) carries an explicit **verify-against-0.24** checklist with the family-version-lock warning (`cargo tree -i gimli` must show one version); pinned by the fixture-ELF resolved-name test, not by trusting the listing. The self-referential-borrow trap (`Context` borrows `File` borrows bytes) is sidestepped `unsafe`-free by rebuilding `Context` per call from `Arc<Vec<u8>>`, with the `self_cell`/owned-context optimization flagged-not-done.
- **reqwest 0.13 debuginfod wire** (Task 2) — `Client::builder().timeout`, `get().send()`, `status()`, `bytes()` flagged with a verify-note; pinned by the **wiremock** behavior test (cache-hit ⇒ `hits == 1`; all-404 ⇒ `NotFound`) — no real network in `cargo test`.
- **object 0.36 write side** (Task 3 fixture) — the `object::write` builder is the one place the fixture is synthesized; flagged with a **vendor-a-tiny-`.debug`-instead** fallback (the spec's sanctioned alternative) recorded in the Self-review if synthesis proves fiddly.
- **`crabka-pprof` contract** (`Frame`/`SymbolSource`/`ProfileError::Symbolize`, and the `SymbolDb::unresolved_locations` accessor) — consumed verbatim; the Contract gap (preferred accessor vs brittle sentinel) is decided in Task 4 Step 1 and **recorded**, never silently stubbed; the lazy test drives the wrapper through the `testkit` seam so the substitute/cache logic is proven without depending on `SymbolDb`'s internals landing.

**Cross-slice constraint surfaced (not buried):** native resolution does a **blocking debuginfod fetch inside a sync `SymbolSource::resolve`** via `rt.block_on`, so **Slice 5's querier must call the fold/resolve step under `tokio::task::spawn_blocking`** — flagged in the `Symbolizer` doc-comment and the Task-4 verify-note, with the lazy test using `flavor = "multi_thread"` to make the constraint legal and visible.

**Type consistency:** `SymbolizerConfig`/`SymbolizeError` defined once (Task 1), used by Tasks 2–5. `DebuginfodClient`/`BuildIdCache` stable Tasks 2/4/5. `ElfModule::{from_bytes,from_path,resolve_addr}` + `demangle` stable Tasks 3/4. `Symbolizer::{new,with_resolver}` + `ModuleResolver` + `UnresolvedMap` + `testkit` stable Tasks 4/5. `build_query_symbolizer`/`SymbolizerRole` stable Task 5. The `Frame`/`SymbolSource` names come from `crabka-pprof` and bend to it (verify-before-use note in the shared contract).

**Placeholder scan:** the only `todo!()`-free placeholders are the three explicitly-flagged follow-ons (`go_frame` line decode, `SymbolizerRole::run` standalone service, `exec-upload` customer-code path) — each returns a safe value (`None` / idle / pass-through), each is named in the spec as out-of-scope-for-now, and none is on a headline-test path. Every other step has runnable code + an exact `cargo test -p crabka-profiles …` command.
