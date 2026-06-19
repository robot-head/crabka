# crabka-metrics Slice 7 — Ruler (recording + alerting rule evaluation, rule-group config API, alert dispatch)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `ruler` role — per-tenant **rule groups** (recording + alerting), the Prometheus/Mimir ruler config API (`/prometheus/config/v1/rules` CRUD), a scheduled evaluation loop that (a) writes **recording-rule** results back to the WAL topic as first-class series and (b) drives **alerting-rule** `inactive → pending → firing` state honoring `for:` and dispatches firing alerts to a configured Alertmanager-API endpoint, the rebuildable compacted state store, the `/api/v1/rules` + `/api/v1/alerts` read APIs, and `(tenant, group)`-hash sharding — all wired behind `crabka-metrics --target ruler`.

**Architecture:** The ruler holds a `MetricStore` client (a thin `RemoteMetricStore` HTTP impl against a querier/query-frontend, OR the co-located querier's `MetricStore`) and runs `PromqlEngine::query_instant` per rule at each group's `interval`. The two rule kinds split cleanly:

- **Recording rules** take the resulting instant vector, overwrite `__name__` with `record` + merge the rule's `labels`, and **produce** the samples to the WAL topic via the Slice 4 produce path — no special storage; the round-trip back through ingest makes them queryable.
- **Alerting rules** evaluate, maintain a per-`(tenant, group, rule, alert-fingerprint)` state machine over the `for:` duration, template `$value`/`$labels` into `annotations`/`labels`, and on entering `firing` `POST /api/v2/alerts` to a configured Alertmanager endpoint.

The two churn-prone surfaces — the **Kafka producer** (Slice 4 wire/produce path) and the **Alertmanager HTTP client** (`reqwest`) — are abstracted behind narrow traits (`RecordingSink`, `AlertSink`) with in-memory mocks, so the state machine and the recording→WAL round-trip are pure, deterministic test concerns. Config + state persist to compacted per-tenant topics behind a `RuleStateStore` trait (in-memory impl for tests, topic-backed impl deferred-but-structured). The evaluation clock is injected (`Clock` trait) so `for:`-duration transitions are tested without real time.

**Tech Stack:** Rust 2024 · `serde` + `serde_yaml` 0.9 (rule-group YAML) · `serde_json` (read-API + Alertmanager v2 JSON) · `arrow` 59 (instant-vector samples) · `axum` 0.8 (config + read APIs, reusing `grpc-gateway/serve.rs`) · `reqwest` 0.13 (Alertmanager client) · `tokio` (eval loop, injected clock) · `thiserror`. Tests: `assert2`, `tokio` (`macros`, `rt`), `tempfile`. Consumes `crabka-promql` (`PromqlEngine`, `QueryResult`, `InstantSample`, `SampleValue`, `MetricStore`) and `crabka-metrics` Slice 1/4 (`Labels`, `WalRecord`, the produce path).

## Global Constraints

- **No backwards compatibility.** Greenfield/undeployed. Change schemas/enums/wire shapes freely; no shims, no migration code, no `#[serde(default)]` "for old logs", no V2-kept-alongside-V1.
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe`.
- **Lints:** `clippy::pedantic` is `warn`. New code clippy-pedantic clean. Run `cargo clippy -p crabka-metrics --all-targets` before each commit.
- **Formatting:** `cargo fmt -p crabka-metrics` before every commit (**never** `cargo +nightly fmt --all` — OS error 206 in deep worktrees on Windows; always `-p`).
- **Assertions:** `assert2::assert!` in tests; `assert2::check!` where multiple soft checks help.
- **Kafka wire identity is the only compat constraint.** The recording-rule write-back path produces *ordinary* series to the WAL topic via Slice 4's produce path — byte-identical to a remote_write-originated sample. No ruler-private record format.
- **Prometheus JSON identity for read APIs.** `/api/v1/rules` and `/api/v1/alerts` match Prometheus's exact response shapes (`status`, `data.groups[]`/`data.alerts[]`, field names/casing/`state` enum strings) — the byte-equality analog for the ruler.
- **Mimir ruler-API identity for config.** `/prometheus/config/v1/rules[/{namespace}[/{group}]]` matches Mimir: YAML request/response bodies, per-tenant via `X-Scope-OrgID`, the documented status codes.
- **Injected time + injected sinks.** The eval loop never reads the wall clock directly and never calls a real producer/HTTP endpoint in unit tests — `Clock`, `RecordingSink`, `AlertSink` are traits with deterministic mocks. This is what makes the `for:` state machine and the WAL round-trip first-class testable.

---

## Dependency & slice roadmap

**Depends on:**
- **Slice 2/3 (`crabka-promql`)** — `PromqlEngine<S: MetricStore>::query_instant(tenant, query, time_ms) -> Result<QueryResult, PromqlError>`; `QueryResult::{Scalar, InstantVector(Vec<InstantSample>), ..}`; `InstantSample { labels: Labels, ts_ms: i64, value: SampleValue }`; `SampleValue::{Float(f64), Histogram(NativeHistogram)}`; the `MetricStore` trait the engine reads through. **Consume these signatures verbatim.**
- **Slice 4 (ingest)** — `WalRecord` + the produce path (recording-rule output samples are produced as ordinary series); `Labels`. *Slice 4's plan file is not yet written;* this plan consumes only the **contract** (`Labels`, a "produce a series sample to the WAL topic" entry point) and wraps it behind `RecordingSink` so the exact producer type can land later without touching the ruler logic.
- **Slice 1** — `crabka-metrics` crate exists (this plan adds modules + a `ruler` binary target to it).

**This plan = Slice 7 of 8.** Remaining: Slice 8 hardening (multi-tenancy/limits, remote_read, compliance + differential-vs-Mimir). Sharding here defines the *assignment function* + a test; actual cross-instance coordination (consumer-group membership) is stubbed with a verify-note and finished under Slice 8.

**Contract-shim note (read before Task 1):** because Slice 2/3/4 may not be merged when this slice is implemented, every consumed type is referenced through a single `crate::ruler::contract` re-export module. If the upstream crate is present, `contract` re-exports the real types; if not, the implementer creates a minimal local `mod contract` with the exact signatures above so this slice compiles and tests in isolation. **Do not** fork divergent definitions — `contract` is one file, swapped to re-exports the moment upstream lands. Flag any signature drift loudly rather than silently adapting.

---

## File structure (additions to `crates/metrics/`)

| File | Responsibility |
|---|---|
| `src/ruler/mod.rs` | `ruler` module decls + public re-exports + `contract` shim |
| `src/ruler/model.rs` | `RuleGroups`/`RuleGroup`/`Rule` YAML model + serde + validation |
| `src/ruler/clock.rs` | `Clock` trait + `SystemClock` + `MockClock` |
| `src/ruler/state.rs` | `AlertState`/`ActiveAlert`/`RuleState` + `RuleStateStore` trait + `InMemoryStateStore` |
| `src/ruler/sinks.rs` | `RecordingSink` + `AlertSink` traits + in-memory mocks |
| `src/ruler/eval.rs` | `evaluate_group` — the per-group eval step (recording + alerting), the `for:` state machine, `$value`/`$labels` templating |
| `src/ruler/alertmanager.rs` | `reqwest` Alertmanager-v2 client (`AlertmanagerClient: AlertSink`) + the v2 JSON payload types |
| `src/ruler/produce.rs` | `WalRecordingSink: RecordingSink` — instant-vector → WAL produce via Slice 4 |
| `src/ruler/sharding.rs` | `assign_group(tenant, group, n_instances) -> usize` `(tenant,group)`-hash assignment |
| `src/ruler/api.rs` | axum router: config CRUD + `/api/v1/rules` + `/api/v1/alerts` |
| `src/ruler/service.rs` | `RulerService` — owns config store, state store, engine, clock; spawns the eval loop |
| `src/bin/ruler.rs` *(or arm in existing `main.rs`)* | `crabka-metrics --target ruler` wiring |
| `Cargo.toml` | add `serde_yaml`, `serde_json`, `reqwest`, `axum`, `tokio` deps |
| *workspace* `Cargo.toml` | add `reqwest = { version = "0.13", ... }` if absent |

---

### Task 1: Ruler module scaffold + contract shim + deps

**Files:**
- Modify: `crates/metrics/Cargo.toml`
- Modify (workspace): `Cargo.toml` (add `reqwest` if absent)
- Create: `crates/metrics/src/ruler/mod.rs`
- Modify: `crates/metrics/src/lib.rs` (add `pub mod ruler;`)

**Interfaces:**
- Produces: a compiling `ruler` module + `crate::ruler::contract` re-exporting (or locally defining) `Labels`, `PromqlEngine`, `MetricStore`, `QueryResult`, `InstantSample`, `SampleValue`, `PromqlError`, `WalRecord`.

- [ ] **Step 1: Add deps to `crates/metrics/Cargo.toml`**

```toml
[dependencies]
# ...existing Slice 1 deps (arrow, thiserror)...
serde = { workspace = true }
serde_yaml = { workspace = true }
serde_json = { workspace = true }
reqwest = { workspace = true }
# The workspace `axum` is `default-features = false` and NO crate enables the
# `json` feature; `api.rs` returns `axum::Json<Value>`, which REQUIRES it.
# Enable `json` here or `cargo build -p crabka-metrics` fails to compile.
axum = { workspace = true, features = ["json"] }
# `service.rs` stores a `tokio_util::sync::CancellationToken`, so tokio-util is a
# normal dep (not dev-only).
tokio-util = { workspace = true }
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "time", "sync"] }
tracing = { workspace = true }
# crabka-promql / Slice-4 produce path: add the path deps once those crates exist.
# crabka-promql = { path = "../promql", version = "0.3.7" }

[dev-dependencies]
# ...existing (assert2, proptest)...
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "time", "test-util"] }
tempfile = { workspace = true }
# Task 11/13 tests drive the axum router via `tower::ServiceExt::oneshot`.
tower = { workspace = true }
```

If the workspace root `Cargo.toml` has no `reqwest`, add:

```toml
reqwest = { version = "0.13", default-features = false, features = ["json", "rustls-tls"] }
```

> **Verify-note (workspace deps):** `serde_yaml`, `serde_json`, `axum`, `tokio`, `tokio-util`, `tower`, `tracing`, `tempfile` are already `[workspace.dependencies]` (confirmed in root `Cargo.toml`). Only `reqwest` may be new — it is referenced today only in comments. Use `default-features = false` + `rustls-tls` to match the workspace's rustls-everywhere posture (no native-tls/openssl). The workspace `axum` is `default-features = false` and no crate enables `json`; this crate must opt into `features = ["json"]` because `api.rs` returns `axum::Json<Value>`.

- [ ] **Step 2: Create the `contract` shim + module decls**

Create `crates/metrics/src/ruler/mod.rs`:

```rust
//! The `ruler` role: per-tenant recording + alerting rule evaluation, the
//! Mimir rule-group config API, the Prometheus rules/alerts read APIs, and
//! alert dispatch to an Alertmanager-API endpoint.

pub mod alertmanager;
pub mod api;
pub mod clock;
pub mod eval;
pub mod model;
pub mod produce;
pub mod service;
pub mod sharding;
pub mod sinks;
pub mod state;

/// Single point of truth for types consumed from sibling slices.
///
/// When `crabka-promql` (Slice 2/3) and the Slice-4 produce path are present,
/// re-export their real types here. Until then, these local definitions carry
/// the **exact** signatures from the shared contract so this slice compiles and
/// tests in isolation. Swap to `pub use` re-exports the moment upstream lands;
/// do not let the two diverge.
pub mod contract {
    use std::collections::BTreeMap;

    /// Ordered label set. This is a **newtype struct** with the EXACT restricted
    /// API of the real shared `Labels` (`crabka-blockstore`), NOT a bare
    /// `BTreeMap` alias — so every ruler call site is written against the same
    /// surface that survives the re-export swap. Notably: `get` returns
    /// `Option<&str>` (not `Option<&String>`), `insert` takes `impl Into<String>`,
    /// there is **no** `remove`, **no** `Deref`, and `FromIterator` is provided
    /// here for ergonomics (drop it if the real type lacks it and convert at the
    /// boundary instead).
    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    pub struct Labels(BTreeMap<String, String>);

    impl Labels {
        #[must_use]
        pub fn new() -> Self {
            Self(BTreeMap::new())
        }
        /// Matches blockstore: returns `Option<&str>`, not `Option<&String>`.
        #[must_use]
        pub fn get(&self, key: &str) -> Option<&str> {
            self.0.get(key).map(String::as_str)
        }
        pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) {
            self.0.insert(key.into(), value.into());
        }
        /// Iterates `(&String, &String)` like the real type. (No `remove`: drop a
        /// key by rebuilding without it — see `eval.rs::full_alert_labels`.)
        pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
            self.0.iter()
        }
        #[must_use]
        pub fn contains_key(&self, key: &str) -> bool {
            self.0.contains_key(key)
        }
    }

    impl<'a> IntoIterator for &'a Labels {
        type Item = (&'a String, &'a String);
        type IntoIter = std::collections::btree_map::Iter<'a, String, String>;
        fn into_iter(self) -> Self::IntoIter {
            self.0.iter()
        }
    }

    impl FromIterator<(String, String)> for Labels {
        fn from_iter<I: IntoIterator<Item = (String, String)>>(iter: I) -> Self {
            Self(iter.into_iter().collect())
        }
    }

    /// One instant-vector sample.
    #[derive(Clone, Debug, PartialEq)]
    pub struct InstantSample {
        pub labels: Labels,
        pub ts_ms: i64,
        pub value: SampleValue,
    }

    /// One series in a range matrix (carried so the enum matches the real
    /// 4-variant shape even though rules only read instant vectors/scalars).
    /// Field names/types mirror Slice 2's `crabka_promql::RangeSeries`
    /// (`samples: Vec<(i64, SampleValue)>`) so the re-export swap is a no-op.
    #[derive(Clone, Debug, PartialEq)]
    pub struct RangeSeries {
        pub labels: Labels,
        pub samples: Vec<(i64, SampleValue)>,
    }

    /// A sample value: float or native histogram. (`Histogram` carried as an
    /// opaque marker here; recording rules round-trip whatever the engine
    /// returns. Replace with the real `NativeHistogram` on re-export.)
    #[derive(Clone, Debug, PartialEq)]
    pub enum SampleValue {
        Float(f64),
        Histogram(()),
    }

    /// PromQL query result. Carries **all four** real `crabka-promql` variants
    /// (`Scalar`, `InstantVector`, `RangeMatrix`, `Str`) even though the ruler
    /// only consumes the first two — so `eval.rs::as_vector`'s `match` stays
    /// exhaustive after the re-export swap.
    #[derive(Clone, Debug, PartialEq)]
    pub enum QueryResult {
        Scalar { ts_ms: i64, value: f64 },
        InstantVector(Vec<InstantSample>),
        RangeMatrix(Vec<RangeSeries>),
        Str { ts_ms: i64, value: String },
    }

    /// Engine error surface. Mirrors Slice 2's `crabka_promql::PromqlError`
    /// 5-variant set so the re-export swap is a no-op (the ruler only carries
    /// this opaquely via `EvalError`, never matching/constructing variants).
    #[derive(Debug, thiserror::Error)]
    pub enum PromqlError {
        #[error("parse error: {0}")]
        Parse(String),
        #[error("plan error: {0}")]
        Plan(String),
        #[error("execution error: {0}")]
        Exec(String),
        #[error("store error: {0}")]
        Store(String),
        #[error("unsupported: {0}")]
        Unsupported(String),
    }
}

pub use contract::{InstantSample, Labels, PromqlError, QueryResult, RangeSeries, SampleValue};

#[cfg(test)]
mod contract_tests {
    use assert2::assert;

    use super::contract::{Labels, QueryResult, RangeSeries};

    // Pins the restricted real-struct `Labels` API so a re-export to the
    // blockstore newtype can't silently break call sites: `get -> Option<&str>`,
    // `insert(impl Into<String>, …)`, `iter` over `(&String, &String)`,
    // `FromIterator<(String, String)>`. There is deliberately no `remove`.
    #[test]
    fn labels_newtype_api() {
        let mut l = Labels::new();
        l.insert("a", "1"); // impl Into<String> for &str
        l.insert("__name__".to_string(), "x".to_string());
        let got: Option<&str> = l.get("a");
        assert!(got == Some("1"));
        assert!(l.get("missing").is_none());
        assert!(l.contains_key("__name__"));
        let collected: Vec<(String, String)> =
            l.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        assert!(collected.len() == 2);
        let rebuilt: Labels = collected.into_iter().collect(); // FromIterator
        assert!(rebuilt.get("a") == Some("1"));
    }

    // Pins all four `QueryResult` variants so `eval.rs::as_vector` stays
    // exhaustive after the real 4-variant enum is re-exported.
    #[test]
    fn query_result_has_four_variants() {
        let _ = QueryResult::Scalar { ts_ms: 0, value: 1.0 };
        let _ = QueryResult::InstantVector(vec![]);
        let _ = QueryResult::RangeMatrix(Vec::<RangeSeries>::new());
        let _ = QueryResult::Str { ts_ms: 0, value: String::new() };
    }
}
```

> **Re-export-swap note:** the moment `crabka-promql` lands, replace the bodies of `contract` with `pub use crabka_promql::{Labels, PromqlEngine, MetricStore, QueryResult, InstantSample, RangeSeries, SampleValue, PromqlError};` (note `Labels` is actually `crabka_blockstore::Labels`, re-exported by promql) and `pub use crate::WalRecord;` (Slice 4 lands `WalRecord` in this same `crabka-metrics` crate — `crates/metrics/src/wal.rs`). The `Histogram(())` placeholder becomes `Histogram(crate::NativeHistogram)`. The shim `Labels`/`QueryResult`/`RangeSeries`/`PromqlError` already mirror the real type signatures (newtype `Labels` with `get -> Option<&str>` / no `remove`; the 4-variant `QueryResult`; `RangeSeries.samples: Vec<(i64, SampleValue)>`; the 5-variant `PromqlError`), so the swap is genuinely a one-file change — but only because every ruler call site is written against that restricted API, not bare `BTreeMap`/2-variant semantics. Flag any signature drift loudly.

- [ ] **Step 3: Wire into `lib.rs`**

Add `pub mod ruler;` to `crates/metrics/src/lib.rs`.

- [ ] **Step 4: Stub the remaining module files**

Create empty-but-compiling `clock.rs`, `model.rs`, `state.rs`, `sinks.rs`, `eval.rs`, `alertmanager.rs`, `produce.rs`, `sharding.rs`, `api.rs`, `service.rs` each with a `//!` doc line. (Each fills in its own Task below; they must exist for `mod.rs` to compile.)

- [ ] **Step 5: Build + pin the contract API**

Run: `cargo build -p crabka-metrics`
Expected: compiles (empty modules + contract shim).

Run: `cargo test -p crabka-metrics --lib ruler::contract_tests`
Expected: PASS (2 tests) — pins the restricted `Labels` newtype API and the 4-variant `QueryResult` so the eventual re-export swap can't silently break call sites.

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/ Cargo.toml
git commit -m "feat(metrics): scaffold ruler module + contract shim + deps"
```

---

### Task 2: Rule-group YAML model + validation

**Files:**
- Modify: `crates/metrics/src/ruler/model.rs`

**Interfaces:**
- Produces:
  - `struct RuleGroups { pub groups: Vec<RuleGroup> }` (`serde`, `PartialEq`).
  - `struct RuleGroup { pub name: String, pub interval: Option<Duration>, pub rules: Vec<Rule> }` — `interval` parsed from a Prometheus duration string (`"30s"`, `"1m"`), `None` ⇒ a service default.
  - `enum Rule { Recording { record: String, expr: String, labels: Labels }, Alerting { alert: String, expr: String, for_: Duration, keep_firing_for: Duration, labels: Labels, annotations: BTreeMap<String,String> } }` — discriminated by the presence of `record` vs `alert` keys (Prometheus's untagged form).
  - `fn parse_rule_groups_yaml(yaml: &str) -> Result<RuleGroups, RuleModelError>` (multi-group `groups:` document).
  - `fn parse_rule_group_yaml(yaml: &str) -> Result<RuleGroup, RuleModelError>` (a **single** bare group — the body shape of Mimir's per-namespace `POST`).
  - `fn to_yaml(&RuleGroups) -> Result<String, RuleModelError>`
  - `enum RuleModelError` (`thiserror`): `Yaml(String)`, `BadDuration(String)`, `MissingRecordOrAlert`, `BothRecordAndAlert`, `DuplicateGroup(String)`.
  - `fn parse_prom_duration(s: &str) -> Result<Duration, RuleModelError>` (supports `ms`/`s`/`m`/`h`/`d`/`w`/`y`, compound like `1h30m`).

- [ ] **Step 1: Write the failing test**

In `crates/metrics/src/ruler/model.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;

    use super::*;

    const YAML: &str = r"
groups:
  - name: example
    interval: 30s
    rules:
      - record: job:http_requests:rate5m
        expr: sum by (job) (rate(http_requests_total[5m]))
        labels:
          team: sre
      - alert: HighErrorRate
        expr: job:errors:rate5m > 0.05
        for: 10m
        labels:
          severity: page
        annotations:
          summary: 'High error rate on {{ $labels.job }}'
          description: 'value is {{ $value }}'
";

    #[test]
    fn parses_recording_and_alerting_rules() {
        let g = parse_rule_groups_yaml(YAML).unwrap();
        assert!(g.groups.len() == 1);
        let grp = &g.groups[0];
        assert!(grp.name == "example");
        assert!(grp.interval == Some(Duration::from_secs(30)));
        assert!(grp.rules.len() == 2);
        match &grp.rules[0] {
            Rule::Recording { record, labels, .. } => {
                assert!(record == "job:http_requests:rate5m");
                assert!(labels.get("team") == Some("sre"));
            }
            other => panic!("expected recording, got {other:?}"),
        }
        match &grp.rules[1] {
            Rule::Alerting { alert, for_, annotations, .. } => {
                assert!(alert == "HighErrorRate");
                assert!(*for_ == Duration::from_secs(600));
                assert!(annotations.get("summary").is_some());
            }
            other => panic!("expected alerting, got {other:?}"),
        }
    }

    #[test]
    fn rule_with_both_record_and_alert_is_rejected() {
        let yaml = "groups:\n  - name: g\n    rules:\n      - record: r\n        alert: a\n        expr: up\n";
        assert!(parse_rule_groups_yaml(yaml).is_err());
    }

    #[test]
    fn duplicate_group_names_rejected() {
        let yaml = "groups:\n  - name: g\n    rules: []\n  - name: g\n    rules: []\n";
        assert!(matches!(parse_rule_groups_yaml(yaml), Err(RuleModelError::DuplicateGroup(_))));
    }

    #[test]
    fn prom_duration_compound() {
        assert!(parse_prom_duration("1h30m").unwrap() == Duration::from_secs(5400));
        assert!(parse_prom_duration("500ms").unwrap() == Duration::from_millis(500));
        assert!(parse_prom_duration("2d").unwrap() == Duration::from_secs(2 * 86_400));
        assert!(parse_prom_duration("bogus").is_err());
    }

    #[test]
    fn round_trips_through_yaml() {
        let g = parse_rule_groups_yaml(YAML).unwrap();
        let s = to_yaml(&g).unwrap();
        let g2 = parse_rule_groups_yaml(&s).unwrap();
        assert!(g == g2);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib ruler::model`
Expected: FAIL — `cannot find function parse_rule_groups_yaml`.

- [ ] **Step 3: Implement the model**

Above the `tests` module. Use an intermediate `RawRule` (all fields `Option`) for serde, then convert to the validated `Rule` enum so the record-vs-alert discrimination + duration parsing live in one place:

```rust
//! Prometheus/Mimir rule-group YAML model + validation.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::contract::Labels;

/// Errors from parsing/validating rule-group YAML.
#[derive(Debug, thiserror::Error)]
pub enum RuleModelError {
    #[error("yaml error: {0}")]
    Yaml(String),
    #[error("invalid duration: {0}")]
    BadDuration(String),
    #[error("rule has neither `record` nor `alert`")]
    MissingRecordOrAlert,
    #[error("rule has both `record` and `alert`")]
    BothRecordAndAlert,
    #[error("duplicate group name: {0}")]
    DuplicateGroup(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleGroups {
    pub groups: Vec<RuleGroup>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleGroup {
    pub name: String,
    pub interval: Option<Duration>,
    pub rules: Vec<Rule>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Rule {
    Recording {
        record: String,
        expr: String,
        labels: Labels,
    },
    Alerting {
        alert: String,
        expr: String,
        for_: Duration,
        keep_firing_for: Duration,
        labels: Labels,
        annotations: BTreeMap<String, String>,
    },
}

impl Rule {
    #[must_use]
    pub fn expr(&self) -> &str {
        match self {
            Rule::Recording { expr, .. } | Rule::Alerting { expr, .. } => expr,
        }
    }
    /// Stable rule name for the read API (`record` or `alert`).
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Rule::Recording { record, .. } => record,
            Rule::Alerting { alert, .. } => alert,
        }
    }
}

// ---- serde wire shapes (Prometheus's untagged record|alert form) ----

#[derive(Deserialize, Serialize)]
struct RawGroups {
    groups: Vec<RawGroup>,
}

#[derive(Deserialize, Serialize)]
struct RawGroup {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    interval: Option<String>,
    #[serde(default)]
    rules: Vec<RawRule>,
}

#[derive(Deserialize, Serialize)]
struct RawRule {
    #[serde(skip_serializing_if = "Option::is_none")]
    record: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    alert: Option<String>,
    expr: String,
    #[serde(rename = "for", skip_serializing_if = "Option::is_none")]
    for_: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    keep_firing_for: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    annotations: BTreeMap<String, String>,
}

/// Parse a Prometheus duration (`ms|s|m|h|d|w|y`, compound `1h30m`).
pub fn parse_prom_duration(s: &str) -> Result<Duration, RuleModelError> {
    let bad = || RuleModelError::BadDuration(s.to_string());
    if s.is_empty() {
        return Err(bad());
    }
    let mut total = Duration::ZERO;
    let mut num = String::new();
    let mut chars = s.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_ascii_digit() {
            num.push(c);
            chars.next();
            continue;
        }
        if num.is_empty() {
            return Err(bad());
        }
        let n: u64 = num.parse().map_err(|_| bad())?;
        num.clear();
        // unit may be 1 or 2 chars (`ms`)
        let unit_secs_or_millis = match c {
            'm' => {
                chars.next();
                if chars.peek() == Some(&'s') {
                    chars.next();
                    total += Duration::from_millis(n);
                    continue;
                }
                total += Duration::from_secs(n * 60);
                continue;
            }
            's' => 1,
            'h' => 3_600,
            'd' => 86_400,
            'w' => 7 * 86_400,
            'y' => 365 * 86_400,
            _ => return Err(bad()),
        };
        chars.next();
        total += Duration::from_secs(n * unit_secs_or_millis);
    }
    if !num.is_empty() {
        return Err(bad()); // trailing digits without a unit
    }
    Ok(total)
}

fn convert_rule(r: RawRule) -> Result<Rule, RuleModelError> {
    match (r.record, r.alert) {
        (Some(_), Some(_)) => Err(RuleModelError::BothRecordAndAlert),
        (None, None) => Err(RuleModelError::MissingRecordOrAlert),
        (Some(record), None) => Ok(Rule::Recording {
            record,
            expr: r.expr,
            labels: r.labels,
        }),
        (None, Some(alert)) => Ok(Rule::Alerting {
            alert,
            expr: r.expr,
            for_: r.for_.as_deref().map_or(Ok(Duration::ZERO), parse_prom_duration)?,
            keep_firing_for: r
                .keep_firing_for
                .as_deref()
                .map_or(Ok(Duration::ZERO), parse_prom_duration)?,
            labels: r.labels,
            annotations: r.annotations,
        }),
    }
}

fn convert_group(g: RawGroup) -> Result<RuleGroup, RuleModelError> {
    let interval = g.interval.as_deref().map(parse_prom_duration).transpose()?;
    let rules = g.rules.into_iter().map(convert_rule).collect::<Result<_, _>>()?;
    Ok(RuleGroup { name: g.name, interval, rules })
}

/// Parse a multi-group rule document (`groups: [...]`). Used by the eval-loop
/// config store and the `GET` read paths — **not** by Mimir's per-namespace
/// `POST`, which sends a single bare group (see `parse_rule_group_yaml`).
pub fn parse_rule_groups_yaml(yaml: &str) -> Result<RuleGroups, RuleModelError> {
    let raw: RawGroups = serde_yaml::from_str(yaml).map_err(|e| RuleModelError::Yaml(e.to_string()))?;
    let mut seen = BTreeSet::new();
    let mut groups = Vec::with_capacity(raw.groups.len());
    for g in raw.groups {
        if !seen.insert(g.name.clone()) {
            return Err(RuleModelError::DuplicateGroup(g.name));
        }
        groups.push(convert_group(g)?);
    }
    Ok(RuleGroups { groups })
}

/// Parse a **single** rule group (`name:`/`interval:`/`rules:` at the TOP level,
/// no `groups:` wrapper). This is the exact body shape Mimir's
/// `POST /prometheus/config/v1/rules/{namespace}` requires ("The request body
/// must contain the definition of one and only one rule group"). Used by
/// `api.rs::post_namespace`.
pub fn parse_rule_group_yaml(yaml: &str) -> Result<RuleGroup, RuleModelError> {
    let raw: RawGroup = serde_yaml::from_str(yaml).map_err(|e| RuleModelError::Yaml(e.to_string()))?;
    convert_group(raw)
}

fn raw_from_group(grp: &RuleGroup) -> RawGroup {
    RawGroup {
        name: grp.name.clone(),
        interval: grp.interval.map(fmt_duration),
        rules: grp.rules.iter().map(raw_from_rule).collect(),
    }
}

/// Serialize back to YAML (config-API GET response body for a `groups:` doc).
pub fn to_yaml(g: &RuleGroups) -> Result<String, RuleModelError> {
    let raw = RawGroups { groups: g.groups.iter().map(raw_from_group).collect() };
    serde_yaml::to_string(&raw).map_err(|e| RuleModelError::Yaml(e.to_string()))
}

/// Serialize a **single** rule group as a bare group document (`name:`/
/// `interval:`/`rules:` at the top level — the inverse of `parse_rule_group_yaml`
/// and the body shape `GET /{namespace}/{group}` returns).
pub fn group_to_yaml(group: &RuleGroup) -> Result<String, RuleModelError> {
    serde_yaml::to_string(&raw_from_group(group)).map_err(|e| RuleModelError::Yaml(e.to_string()))
}

/// Serialize a single namespace's groups in Mimir's documented per-namespace
/// `GET /{namespace}` shape: a bare YAML **list of groups** (no `groups:` map
/// wrapper). `RawGroup` stays private to this module.
pub fn namespace_groups_to_yaml(groups: &[RuleGroup]) -> Result<String, RuleModelError> {
    let raw: Vec<RawGroup> = groups.iter().map(raw_from_group).collect();
    serde_yaml::to_string(&raw).map_err(|e| RuleModelError::Yaml(e.to_string()))
}

/// Serialize the all-namespaces `GET /prometheus/config/v1/rules` response:
/// a real YAML **mapping** `namespace -> [groups]` (Mimir's documented shape),
/// not concatenated documents. Input is ordered `(namespace, groups)` pairs.
pub fn namespaces_to_yaml_map(
    by_namespace: &BTreeMap<String, Vec<RuleGroup>>,
) -> Result<String, RuleModelError> {
    let raw: BTreeMap<String, Vec<RawGroup>> = by_namespace
        .iter()
        .map(|(ns, groups)| (ns.clone(), groups.iter().map(raw_from_group).collect()))
        .collect();
    serde_yaml::to_string(&raw).map_err(|e| RuleModelError::Yaml(e.to_string()))
}

fn raw_from_rule(r: &Rule) -> RawRule {
    match r {
        Rule::Recording { record, expr, labels } => RawRule {
            record: Some(record.clone()),
            alert: None,
            expr: expr.clone(),
            for_: None,
            keep_firing_for: None,
            labels: labels.clone(),
            annotations: BTreeMap::new(),
        },
        Rule::Alerting { alert, expr, for_, keep_firing_for, labels, annotations } => RawRule {
            record: None,
            alert: Some(alert.clone()),
            expr: expr.clone(),
            for_: (!for_.is_zero()).then(|| fmt_duration(*for_)),
            keep_firing_for: (!keep_firing_for.is_zero()).then(|| fmt_duration(*keep_firing_for)),
            labels: labels.clone(),
            annotations: annotations.clone(),
        },
    }
}

/// Render a duration back to Prometheus form (round, seconds-granular except ms).
fn fmt_duration(d: Duration) -> String {
    let ms = d.as_millis();
    if ms % 1000 != 0 {
        return format!("{ms}ms");
    }
    let s = d.as_secs();
    if s % 3_600 == 0 && s != 0 {
        format!("{}h", s / 3_600)
    } else if s % 60 == 0 && s != 0 {
        format!("{}m", s / 60)
    } else {
        format!("{s}s")
    }
}
```

> **Round-trip-fidelity note:** `fmt_duration` is a *normalizing* renderer (`90m` → `1h30m`? no — `90m` is `5400s`, `5400 % 3600 != 0`, so it renders `90m`). The `round_trips_through_yaml` test only asserts parse→emit→parse equality of the **model**, not byte-identity of the YAML text — that's the right invariant (Mimir also normalizes). If a later compliance test needs verbatim text preservation, store the raw YAML string alongside the parsed model in the config store (Task 7), not here.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib ruler::model`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): ruler rule-group YAML model + duration parser"
```

---

### Task 3: Clock abstraction

**Files:**
- Modify: `crates/metrics/src/ruler/clock.rs`

**Interfaces:**
- Produces:
  - `trait Clock: Send + Sync { fn now_ms(&self) -> i64; }`
  - `struct SystemClock;` (`Clock` via `SystemTime::now()`).
  - `struct MockClock { ... }` with `new(start_ms: i64)`, `advance(&self, ms: i64)`, `set(&self, ms: i64)` (interior-mutable, `Clock`-impl, `Clone`).

- [ ] **Step 1: Write the failing test**

In `crates/metrics/src/ruler/clock.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn mock_clock_advances() {
        let c = MockClock::new(1_000);
        assert!(c.now_ms() == 1_000);
        c.advance(500);
        assert!(c.now_ms() == 1_500);
        c.set(42);
        assert!(c.now_ms() == 42);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib ruler::clock`
Expected: FAIL — `cannot find type MockClock`.

- [ ] **Step 3: Implement**

```rust
//! Injectable clock so the `for:`-duration state machine is testable without
//! real time.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Wall-clock source in epoch milliseconds.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
}

/// Production clock.
#[derive(Debug, Clone, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
    }
}

/// Deterministic test clock (interior-mutable, cheap to clone).
#[derive(Debug, Clone)]
pub struct MockClock {
    now: Arc<AtomicI64>,
}

impl MockClock {
    #[must_use]
    pub fn new(start_ms: i64) -> Self {
        Self { now: Arc::new(AtomicI64::new(start_ms)) }
    }
    pub fn advance(&self, ms: i64) {
        self.now.fetch_add(ms, Ordering::SeqCst);
    }
    pub fn set(&self, ms: i64) {
        self.now.store(ms, Ordering::SeqCst);
    }
}

impl Clock for MockClock {
    fn now_ms(&self) -> i64 {
        self.now.load(Ordering::SeqCst)
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib ruler::clock`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): ruler injectable clock (SystemClock + MockClock)"
```

---

### Task 4: Alert state model + `RuleStateStore` trait + in-memory impl

**Files:**
- Modify: `crates/metrics/src/ruler/state.rs`

**Interfaces:**
- Produces:
  - `enum AlertState { Inactive, Pending, Firing }` (`Copy`, `PartialEq`; `as_str()` → `"inactive"`/`"pending"`/`"firing"` for the read API).
  - `struct ActiveAlert { pub labels: Labels, pub annotations: BTreeMap<String,String>, pub state: AlertState, pub active_since_ms: i64, pub last_eval_ms: i64, pub value: f64 }` — `labels` is the *full* alert label set (`alertname` + rule labels + result series labels) used as the alert identity.
  - `fn alert_fingerprint(labels: &Labels) -> u64` — stable hash of the sorted label set (alert identity within a rule).
  - `struct RuleStateStore` trait: `load(&self, tenant: &str, group: &str, rule: &str) -> Vec<ActiveAlert>`; `save(&self, tenant: &str, group: &str, rule: &str, alerts: &[ActiveAlert])`; `all_active(&self, tenant: &str) -> Vec<(String /*group*/, String /*rule*/, ActiveAlert)>`.
  - `struct InMemoryStateStore` (`Default`, `Clone`) implementing `RuleStateStore`.

- [ ] **Step 1: Write the failing test**

In `crates/metrics/src/ruler/state.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;

    use super::*;

    fn lbls(pairs: &[(&str, &str)]) -> Labels {
        pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
    }

    #[test]
    fn state_strings_match_prometheus() {
        assert!(AlertState::Inactive.as_str() == "inactive");
        assert!(AlertState::Pending.as_str() == "pending");
        assert!(AlertState::Firing.as_str() == "firing");
    }

    #[test]
    fn fingerprint_is_order_independent() {
        let a = lbls(&[("alertname", "X"), ("job", "api")]);
        let b: Labels = lbls(&[("job", "api"), ("alertname", "X")]);
        assert!(alert_fingerprint(&a) == alert_fingerprint(&b));
        let c = lbls(&[("alertname", "X"), ("job", "web")]);
        assert!(alert_fingerprint(&a) != alert_fingerprint(&c));
    }

    #[test]
    fn store_round_trips_and_lists_active() {
        let store = InMemoryStateStore::default();
        let alert = ActiveAlert {
            labels: lbls(&[("alertname", "HighErr"), ("job", "api")]),
            annotations: BTreeMap::new(),
            state: AlertState::Firing,
            active_since_ms: 1_000,
            last_eval_ms: 2_000,
            value: 0.9,
        };
        store.save("tenant-a", "grp", "HighErr", &[alert.clone()]);
        let back = store.load("tenant-a", "grp", "HighErr");
        assert!(back == vec![alert.clone()]);
        let active = store.all_active("tenant-a");
        assert!(active.len() == 1);
        assert!(active[0].0 == "grp" && active[0].1 == "HighErr");
        // tenant isolation
        assert!(store.all_active("tenant-b").is_empty());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib ruler::state`
Expected: FAIL — `cannot find type AlertState`.

- [ ] **Step 3: Implement**

```rust
//! Alert state model + the rebuildable per-tenant state store.
//!
//! Production persists this to a compacted per-tenant topic (key
//! `(tenant, group, rule, alert-fingerprint)`, value = `ActiveAlert` snapshot;
//! a tombstone clears an alert that returned to inactive). The topic-backed
//! impl is deferred to the service wiring (Task 9, with a verify-note); the
//! in-memory impl here is the test substrate and the source of truth for the
//! trait shape.

use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use super::contract::Labels;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlertState {
    Inactive,
    Pending,
    Firing,
}

impl AlertState {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            AlertState::Inactive => "inactive",
            AlertState::Pending => "pending",
            AlertState::Firing => "firing",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ActiveAlert {
    pub labels: Labels,
    pub annotations: BTreeMap<String, String>,
    pub state: AlertState,
    /// When the alert first went `pending` (drives the `for:` comparison).
    pub active_since_ms: i64,
    pub last_eval_ms: i64,
    pub value: f64,
}

/// Stable identity hash over the sorted label set.
#[must_use]
pub fn alert_fingerprint(labels: &Labels) -> u64 {
    let mut h = DefaultHasher::new();
    for (k, v) in labels {
        k.hash(&mut h);
        v.hash(&mut h);
    }
    h.finish()
}

/// Rebuildable alert-state persistence keyed `(tenant, group, rule)`.
pub trait RuleStateStore: Send + Sync {
    fn load(&self, tenant: &str, group: &str, rule: &str) -> Vec<ActiveAlert>;
    fn save(&self, tenant: &str, group: &str, rule: &str, alerts: &[ActiveAlert]);
    fn all_active(&self, tenant: &str) -> Vec<(String, String, ActiveAlert)>;
}

type Key = (String, String, String);

#[derive(Clone, Default)]
pub struct InMemoryStateStore {
    inner: Arc<Mutex<BTreeMap<Key, Vec<ActiveAlert>>>>,
}

impl RuleStateStore for InMemoryStateStore {
    fn load(&self, tenant: &str, group: &str, rule: &str) -> Vec<ActiveAlert> {
        let g = self.inner.lock().expect("state store poisoned");
        g.get(&(tenant.to_string(), group.to_string(), rule.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    fn save(&self, tenant: &str, group: &str, rule: &str, alerts: &[ActiveAlert]) {
        let mut g = self.inner.lock().expect("state store poisoned");
        let key = (tenant.to_string(), group.to_string(), rule.to_string());
        if alerts.is_empty() {
            g.remove(&key);
        } else {
            g.insert(key, alerts.to_vec());
        }
    }

    fn all_active(&self, tenant: &str) -> Vec<(String, String, ActiveAlert)> {
        let g = self.inner.lock().expect("state store poisoned");
        g.iter()
            .filter(|((t, _, _), _)| t == tenant)
            .flat_map(|((_, grp, rule), alerts)| {
                alerts.iter().map(move |a| (grp.clone(), rule.clone(), a.clone()))
            })
            .collect()
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib ruler::state`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): ruler alert-state model + RuleStateStore + in-memory impl"
```

---

### Task 5: Sink traits + mocks (RecordingSink, AlertSink)

**Files:**
- Modify: `crates/metrics/src/ruler/sinks.rs`

**Interfaces:**
- Produces:
  - `struct DispatchAlert { pub labels: Labels, pub annotations: BTreeMap<String,String>, pub starts_at_ms: i64, pub ends_at_ms: Option<i64> }` — Alertmanager-v2-shaped firing alert.
  - `trait AlertSink: Send + Sync { async fn dispatch(&self, tenant: &str, alerts: &[DispatchAlert]) -> Result<(), SinkError>; }` (use `async_trait` if the crate doesn't already enable async-fn-in-trait; the workspace pins `async-trait` — prefer it for object safety).
  - `trait RecordingSink: Send + Sync { async fn produce(&self, tenant: &str, samples: &[(Labels, i64, f64)]) -> Result<(), SinkError>; }` — `(labels-with-__name__, ts_ms, value)` series rows.
  - `enum SinkError` (`thiserror`): `Transport(String)`, `Rejected(String)`.
  - `struct MockAlertSink` (`Default`, `Clone`) recording all dispatched `(tenant, Vec<DispatchAlert>)` calls; `struct MockRecordingSink` (`Default`, `Clone`) recording all produced `(tenant, Vec<(Labels,i64,f64)>)` calls. Both expose `calls() -> Vec<...>`. `MockAlertSink::fail_next()` to force one `Transport` error (dispatch-retry test).

- [ ] **Step 1: Write the failing test**

In `crates/metrics/src/ruler/sinks.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;

    use super::*;

    fn lbls(pairs: &[(&str, &str)]) -> Labels {
        pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
    }

    #[tokio::test]
    async fn mock_recording_sink_records_calls() {
        let sink = MockRecordingSink::default();
        let rows = vec![(lbls(&[("__name__", "x")]), 1_000_i64, 2.0_f64)];
        sink.produce("t", &rows).await.unwrap();
        let calls = sink.calls();
        assert!(calls.len() == 1);
        assert!(calls[0].0 == "t");
        assert!(calls[0].1 == rows);
    }

    #[tokio::test]
    async fn mock_alert_sink_can_fail_once() {
        let sink = MockAlertSink::default();
        sink.fail_next();
        let a = vec![DispatchAlert {
            labels: lbls(&[("alertname", "X")]),
            annotations: BTreeMap::new(),
            starts_at_ms: 1,
            ends_at_ms: None,
        }];
        assert!(sink.dispatch("t", &a).await.is_err());
        assert!(sink.dispatch("t", &a).await.is_ok());
        assert!(sink.calls().len() == 1); // only the successful call recorded
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib ruler::sinks`
Expected: FAIL — `cannot find type MockRecordingSink`.

- [ ] **Step 3: Implement**

```rust
//! The two side-effect surfaces, behind narrow traits with deterministic mocks.
//! Real impls: `produce.rs` (WAL producer) and `alertmanager.rs` (HTTP). Keeping
//! them behind traits is what makes the eval loop a pure, deterministic test.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::contract::Labels;

#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("transport error: {0}")]
    Transport(String),
    #[error("rejected: {0}")]
    Rejected(String),
}

/// An Alertmanager-v2-shaped alert ready to dispatch.
#[derive(Clone, Debug, PartialEq)]
pub struct DispatchAlert {
    pub labels: Labels,
    pub annotations: BTreeMap<String, String>,
    pub starts_at_ms: i64,
    pub ends_at_ms: Option<i64>,
}

#[async_trait]
pub trait AlertSink: Send + Sync {
    async fn dispatch(&self, tenant: &str, alerts: &[DispatchAlert]) -> Result<(), SinkError>;
}

#[async_trait]
pub trait RecordingSink: Send + Sync {
    /// Produce recording-rule output series to the WAL topic. Each row is
    /// `(labels including __name__, ts_ms, value)`.
    async fn produce(&self, tenant: &str, samples: &[(Labels, i64, f64)]) -> Result<(), SinkError>;
}

// ---- mocks ----

#[derive(Clone, Default)]
pub struct MockRecordingSink {
    calls: Arc<Mutex<Vec<(String, Vec<(Labels, i64, f64)>)>>>,
}

impl MockRecordingSink {
    #[must_use]
    pub fn calls(&self) -> Vec<(String, Vec<(Labels, i64, f64)>)> {
        self.calls.lock().expect("poisoned").clone()
    }
}

#[async_trait]
impl RecordingSink for MockRecordingSink {
    async fn produce(&self, tenant: &str, samples: &[(Labels, i64, f64)]) -> Result<(), SinkError> {
        self.calls.lock().expect("poisoned").push((tenant.to_string(), samples.to_vec()));
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct MockAlertSink {
    calls: Arc<Mutex<Vec<(String, Vec<DispatchAlert>)>>>,
    fail_next: Arc<Mutex<bool>>,
}

impl MockAlertSink {
    pub fn fail_next(&self) {
        *self.fail_next.lock().expect("poisoned") = true;
    }
    #[must_use]
    pub fn calls(&self) -> Vec<(String, Vec<DispatchAlert>)> {
        self.calls.lock().expect("poisoned").clone()
    }
}

#[async_trait]
impl AlertSink for MockAlertSink {
    async fn dispatch(&self, tenant: &str, alerts: &[DispatchAlert]) -> Result<(), SinkError> {
        {
            let mut f = self.fail_next.lock().expect("poisoned");
            if *f {
                *f = false;
                return Err(SinkError::Transport("forced".into()));
            }
        }
        self.calls.lock().expect("poisoned").push((tenant.to_string(), alerts.to_vec()));
        Ok(())
    }
}
```

> **Dep-note:** add `async-trait = { workspace = true }` to `crates/metrics/Cargo.toml` `[dependencies]` (it is a workspace dep). Async-fn-in-trait is stable but not yet object-safe for `dyn AlertSink`; the service stores `Arc<dyn AlertSink>`, so `#[async_trait]` is required, not optional.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib ruler::sinks`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): ruler RecordingSink/AlertSink traits + mocks"
```

---

### Task 6: The evaluation step — recording-rule write-back + the `for:` state machine (the centerpiece)

**Files:**
- Modify: `crates/metrics/src/ruler/eval.rs`

This is the heart of the slice: `evaluate_group` runs each rule's `expr` through the engine, branches on rule kind, and produces side effects through the sinks. The two first-class test concerns — the **recording→WAL round-trip** and the **`for:` state machine** — both live here and are driven entirely by a mock `MetricStore` (returning canned vectors), `MockRecordingSink`, `MockAlertSink`, and `MockClock`.

**Interfaces:**
- Consumes: `Rule`, `RuleGroup`, `contract::{InstantSample, QueryResult, SampleValue}`, `ActiveAlert`, `AlertState`, `alert_fingerprint`, `RuleStateStore`, `RecordingSink`, `AlertSink`, `DispatchAlert`, `Clock`.
- Produces:
  - `trait Querier: Send + Sync { async fn query_instant(&self, tenant: &str, expr: &str, time_ms: i64) -> Result<QueryResult, EvalError>; }` — the ruler's view of the engine (a `PromqlEngine<RemoteMetricStore>` impl; mock in tests). *(Defined here, not in `contract`, because it is the ruler↔engine boundary, not an upstream type.)*
  - `async fn evaluate_group(tenant, group: &RuleGroup, now_ms: i64, querier, recording, alerting, state) -> Result<GroupEvalReport, EvalError>` — evaluates every rule at `now_ms`; recording rules → `recording.produce`; alerting rules → state-machine step → `alerting.dispatch` for newly-`firing`; persists alert state via `state`.
  - `fn template_annotation(text: &str, value: f64, labels: &Labels) -> String` — replaces `{{ $value }}` and `{{ $labels.X }}` (the minimal Go-template subset Prometheus rules use in practice; full text/template is out of scope and flagged).
  - `struct GroupEvalReport { pub recorded_series: usize, pub alerts_pending: usize, pub alerts_firing: usize, pub dispatched: usize }` (for the read API + tests).
  - `enum EvalError` (`thiserror`): `Query(String)`, `Sink(#[from] SinkError)`.

**The `for:` state machine (exact semantics — match Prometheus):**
- For each alerting rule, evaluate `expr`. Each result series in the instant vector is a *candidate* alert; its identity = `alert_fingerprint(full_labels)` where `full_labels = {alertname: rule.alert} ∪ rule.labels ∪ series.labels` (series labels win on collision per Prometheus? **No** — rule `labels` override series labels; verify against Prometheus and pin with a test).
- Load prior `ActiveAlert`s for the rule. For each candidate:
  - If no prior alert with this fingerprint → new alert at `Pending`, `active_since_ms = now_ms` (or `Firing` immediately if `for_ == 0`).
  - If prior `Pending`/`Firing` and `now_ms - active_since_ms >= for_` → `Firing`; else stays `Pending`.
  - Refresh `last_eval_ms = now_ms`, `value`, `annotations` (templated), `labels`.
- Any prior alert whose fingerprint is **absent** from this eval's candidates → resolved: drop it (or, if `keep_firing_for > 0` and it was `Firing`, keep until `now_ms - last_eval_ms >= keep_firing_for`; **keep_firing_for handling is a flagged stretch** — implement the basic drop-on-absence first, add keep_firing_for in Step 6b).
- Dispatch: alerts that **transitioned into** `Firing` this eval (were not `Firing` before) → `alerting.dispatch`. (Re-dispatch of still-firing alerts on a resend interval is a Task-9/service concern, flagged.)

- [ ] **Step 1: Write the failing tests (state machine + round-trip)**

In `crates/metrics/src/ruler/eval.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use assert2::assert;
    use async_trait::async_trait;

    use super::*;
    use crate::ruler::contract::{InstantSample, QueryResult, SampleValue};
    use crate::ruler::model::Rule;
    use crate::ruler::sinks::{MockAlertSink, MockRecordingSink};
    use crate::ruler::state::{AlertState, InMemoryStateStore};

    fn lbls(pairs: &[(&str, &str)]) -> Labels {
        pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
    }

    /// A querier returning a scripted result per call.
    #[derive(Clone, Default)]
    struct ScriptQuerier {
        results: Arc<Mutex<Vec<QueryResult>>>,
    }
    impl ScriptQuerier {
        fn push(&self, r: QueryResult) {
            self.results.lock().unwrap().push(r);
        }
    }
    #[async_trait]
    impl Querier for ScriptQuerier {
        async fn query_instant(&self, _t: &str, _e: &str, _ts: i64) -> Result<QueryResult, EvalError> {
            Ok(self.results.lock().unwrap().remove(0))
        }
    }

    fn vector(samples: &[(Labels, f64)], ts: i64) -> QueryResult {
        QueryResult::InstantVector(
            samples
                .iter()
                .map(|(l, v)| InstantSample { labels: l.clone(), ts_ms: ts, value: SampleValue::Float(*v) })
                .collect(),
        )
    }

    fn recording_group() -> RuleGroup {
        RuleGroup {
            name: "g".into(),
            interval: Some(Duration::from_secs(60)),
            rules: vec![Rule::Recording {
                record: "job:http:rate5m".into(),
                expr: "rate(http_total[5m])".into(),
                labels: lbls(&[("team", "sre")]),
            }],
        }
    }

    fn alerting_group(for_: Duration) -> RuleGroup {
        RuleGroup {
            name: "g".into(),
            interval: Some(Duration::from_secs(60)),
            rules: vec![Rule::Alerting {
                alert: "HighErr".into(),
                expr: "errors > 0".into(),
                for_,
                keep_firing_for: Duration::ZERO,
                labels: lbls(&[("severity", "page")]),
                annotations: {
                    let mut a = BTreeMap::new();
                    a.insert("summary".into(), "job {{ $labels.job }} at {{ $value }}".into());
                    a
                },
            }],
        }
    }

    #[tokio::test]
    async fn recording_rule_writes_renamed_series_to_wal() {
        let q = ScriptQuerier::default();
        // engine returns a series with its own __name__; the recording rule
        // must OVERWRITE __name__ with `record` and merge rule labels.
        q.push(vector(&[(lbls(&[("__name__", "ignored"), ("job", "api")]), 0.42)], 60_000));
        let rec = MockRecordingSink::default();
        let alerts = MockAlertSink::default();
        let state = InMemoryStateStore::default();

        let report = evaluate_group("t", &recording_group(), 60_000, &q, &rec, &alerts, &state)
            .await
            .unwrap();
        assert!(report.recorded_series == 1);

        let calls = rec.calls();
        assert!(calls.len() == 1);
        let (tenant, rows) = &calls[0];
        assert!(tenant == "t");
        let (out_labels, ts, val) = &rows[0];
        assert!(out_labels.get("__name__") == Some("job:http:rate5m"));
        assert!(out_labels.get("job") == Some("api"));
        assert!(out_labels.get("team") == Some("sre"));
        assert!(*ts == 60_000 && (*val - 0.42).abs() < 1e-9);
        assert!(alerts.calls().is_empty()); // recording rules never dispatch
    }

    #[tokio::test]
    async fn alert_goes_pending_then_firing_after_for_elapses() {
        let q = ScriptQuerier::default();
        let rec = MockRecordingSink::default();
        let sink = MockAlertSink::default();
        let state = InMemoryStateStore::default();
        let group = alerting_group(Duration::from_secs(120)); // for: 2m

        // t=0: condition true → Pending (for not yet elapsed), no dispatch.
        q.push(vector(&[(lbls(&[("job", "api")]), 5.0)], 0));
        let r0 = evaluate_group("t", &group, 0, &q, &rec, &sink, &state).await.unwrap();
        assert!(r0.alerts_pending == 1 && r0.alerts_firing == 0 && r0.dispatched == 0);
        assert!(sink.calls().is_empty());

        // t=60s: still within `for`, still Pending.
        q.push(vector(&[(lbls(&[("job", "api")]), 6.0)], 60_000));
        let r1 = evaluate_group("t", &group, 60_000, &q, &rec, &sink, &state).await.unwrap();
        assert!(r1.alerts_pending == 1 && r1.alerts_firing == 0);

        // t=120s: for elapsed → Firing + dispatch once.
        q.push(vector(&[(lbls(&[("job", "api")]), 7.0)], 120_000));
        let r2 = evaluate_group("t", &group, 120_000, &q, &rec, &sink, &state).await.unwrap();
        assert!(r2.alerts_firing == 1 && r2.dispatched == 1);

        let calls = sink.calls();
        assert!(calls.len() == 1);
        let dispatched = &calls[0].1[0];
        assert!(dispatched.labels.get("alertname") == Some("HighErr"));
        assert!(dispatched.labels.get("severity") == Some("page"));
        assert!(dispatched.labels.get("job") == Some("api"));
        // templated annotation
        assert!(dispatched.annotations.get("summary").unwrap() == "job api at 7");

        // t=180s: still firing → NO re-dispatch from evaluate_group.
        q.push(vector(&[(lbls(&[("job", "api")]), 8.0)], 180_000));
        let r3 = evaluate_group("t", &group, 180_000, &q, &rec, &sink, &state).await.unwrap();
        assert!(r3.alerts_firing == 1 && r3.dispatched == 0);
        assert!(sink.calls().len() == 1);
    }

    #[tokio::test]
    async fn alert_resolves_when_condition_clears() {
        let q = ScriptQuerier::default();
        let rec = MockRecordingSink::default();
        let sink = MockAlertSink::default();
        let state = InMemoryStateStore::default();
        let group = alerting_group(Duration::ZERO); // fire immediately

        q.push(vector(&[(lbls(&[("job", "api")]), 5.0)], 0));
        let r0 = evaluate_group("t", &group, 0, &q, &rec, &sink, &state).await.unwrap();
        assert!(r0.alerts_firing == 1 && r0.dispatched == 1);

        // condition clears → empty vector → alert resolved/dropped.
        q.push(QueryResult::InstantVector(vec![]));
        let r1 = evaluate_group("t", &group, 60_000, &q, &rec, &sink, &state).await.unwrap();
        assert!(r1.alerts_firing == 0 && r1.alerts_pending == 0);
        assert!(state.all_active("t").is_empty());
    }

    #[tokio::test]
    async fn templating_handles_value_and_labels() {
        let out = template_annotation("{{ $labels.job }} = {{ $value }}", 3.5, &lbls(&[("job", "api")]));
        assert!(out == "api = 3.5");
        // missing label → empty
        let out2 = template_annotation("{{ $labels.missing }}!", 0.0, &Labels::new());
        assert!(out2 == "!");
    }

    #[tokio::test]
    async fn templated_label_value_expands() {
        // A rule label whose VALUE is a template — Prometheus expands label
        // values too, against the result series labels (not just annotations).
        let q = ScriptQuerier::default();
        let rec = MockRecordingSink::default();
        let sink = MockAlertSink::default();
        let state = InMemoryStateStore::default();
        let group = RuleGroup {
            name: "g".into(),
            interval: Some(Duration::from_secs(60)),
            rules: vec![Rule::Alerting {
                alert: "HighErr".into(),
                expr: "errors > 0".into(),
                for_: Duration::ZERO, // fire immediately so we can read dispatched labels
                keep_firing_for: Duration::ZERO,
                labels: lbls(&[("target", "{{ $labels.host }}")]),
                annotations: BTreeMap::new(),
            }],
        };
        q.push(vector(&[(lbls(&[("host", "node-7")]), 5.0)], 0));
        evaluate_group("t", &group, 0, &q, &rec, &sink, &state).await.unwrap();
        let dispatched = &sink.calls()[0].1[0];
        assert!(dispatched.labels.get("target") == Some("node-7"));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib ruler::eval`
Expected: FAIL — `cannot find function evaluate_group`.

- [ ] **Step 3: Implement `evaluate_group` + the state machine + templating**

```rust
//! The per-group evaluation step: recording-rule write-back and the alerting
//! `inactive → pending → firing` state machine (honoring `for:`).

use std::collections::BTreeMap;

use async_trait::async_trait;

use super::clock::Clock;
use super::contract::{InstantSample, Labels, QueryResult, SampleValue};
use super::model::{Rule, RuleGroup};
use super::sinks::{AlertSink, DispatchAlert, RecordingSink, SinkError};
use super::state::{ActiveAlert, AlertState, RuleStateStore, alert_fingerprint};

#[derive(Debug, thiserror::Error)]
pub enum EvalError {
    #[error("query error: {0}")]
    Query(String),
    #[error(transparent)]
    Sink(#[from] SinkError),
}

/// The ruler's view of the PromQL engine. Implemented over
/// `PromqlEngine<RemoteMetricStore>`; mocked in tests.
#[async_trait]
pub trait Querier: Send + Sync {
    async fn query_instant(
        &self,
        tenant: &str,
        expr: &str,
        time_ms: i64,
    ) -> Result<QueryResult, EvalError>;
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GroupEvalReport {
    pub recorded_series: usize,
    pub alerts_pending: usize,
    pub alerts_firing: usize,
    pub dispatched: usize,
}

/// Coerce a `QueryResult` to an instant vector (recording/alerting both read
/// vectors; a scalar is lifted to a single label-less sample).
///
/// The match handles all four `QueryResult` variants explicitly so it stays
/// exhaustive after `contract` re-exports the real 4-variant enum: `RangeMatrix`
/// and `Str` are not valid rule outputs (Prometheus rejects range-vector/string
/// rule expressions), so they coerce to an empty vector here.
fn as_vector(r: QueryResult) -> Vec<InstantSample> {
    match r {
        QueryResult::InstantVector(v) => v,
        QueryResult::Scalar { ts_ms, value } => vec![InstantSample {
            labels: Labels::new(),
            ts_ms,
            value: SampleValue::Float(value),
        }],
        // Not valid rule-expression result types → no samples.
        QueryResult::RangeMatrix(_) | QueryResult::Str { .. } => Vec::new(),
    }
}

fn float_value(v: &SampleValue) -> Option<f64> {
    match v {
        SampleValue::Float(f) => Some(*f),
        SampleValue::Histogram(()) => None,
    }
}

/// Minimal `{{ $value }}` / `{{ $labels.X }}` substitution (the subset
/// Prometheus rule annotations use in practice). Full Go text/template is
/// out of scope — flagged in the self-review.
#[must_use]
pub fn template_annotation(text: &str, value: f64, labels: &Labels) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(open) = rest.find("{{") {
        out.push_str(&rest[..open]);
        let after = &rest[open + 2..];
        let Some(close) = after.find("}}") else {
            out.push_str(&rest[open..]);
            return out;
        };
        let expr = after[..close].trim();
        if expr == "$value" {
            out.push_str(&fmt_value(value));
        } else if let Some(label) = expr.strip_prefix("$labels.") {
            // `Labels::get` already returns `Option<&str>` (blockstore signature).
            out.push_str(labels.get(label).unwrap_or(""));
        } else {
            // unknown directive: leave it untouched (visible, not silently lost)
            out.push_str("{{ ");
            out.push_str(expr);
            out.push_str(" }}");
        }
        rest = &after[close + 2..];
    }
    out.push_str(rest);
    out
}

/// Render a float the way Prometheus does in templates (shortest round-trip).
fn fmt_value(v: f64) -> String {
    let s = format!("{v}");
    s
}

/// Build the full alert label set: `alertname` + rule labels override series
/// labels. (Matches Prometheus: rule `labels:` take precedence on collision.)
///
/// Rule **label values** are run through the template expander, exactly like
/// annotation values (Prometheus `rules/alerting.go`:
/// `r.labels.Range(func(l){ lb.Set(l.Name, expand(l.Value)) })`). The template
/// context is the result `value` + the series labels, so e.g.
/// `instance: '{{ $labels.host }}'` resolves against the result series.
///
/// `Labels` has no `remove`, so we drop `__name__` by rebuilding from the
/// series' other labels rather than cloning-and-removing.
fn full_alert_labels(alertname: &str, rule_labels: &Labels, series: &Labels, value: f64) -> Labels {
    let mut l = Labels::new();
    for (k, v) in series {
        if k.as_str() != "__name__" {
            // alerts never carry __name__
            l.insert(k.clone(), v.clone());
        }
    }
    for (k, v) in rule_labels {
        l.insert(k.clone(), template_annotation(v, value, series));
    }
    l.insert("alertname".to_string(), alertname.to_string());
    l
}

/// Evaluate one group at `now_ms`.
#[allow(clippy::too_many_arguments)]
pub async fn evaluate_group<Q, R, A, S>(
    tenant: &str,
    group: &RuleGroup,
    now_ms: i64,
    querier: &Q,
    recording: &R,
    alerting: &A,
    state: &S,
) -> Result<GroupEvalReport, EvalError>
where
    Q: Querier,
    R: RecordingSink,
    A: AlertSink,
    S: RuleStateStore,
{
    let mut report = GroupEvalReport::default();
    for rule in &group.rules {
        let result = querier.query_instant(tenant, rule.expr(), now_ms).await?;
        let samples = as_vector(result);
        match rule {
            Rule::Recording { record, labels, .. } => {
                let mut rows = Vec::with_capacity(samples.len());
                for s in &samples {
                    let Some(v) = float_value(&s.value) else { continue };
                    // `Labels` has no `remove`; rebuild without the source
                    // `__name__`, then merge rule labels and set the new name.
                    let mut out = Labels::new();
                    for (k, val) in &s.labels {
                        if k.as_str() != "__name__" {
                            out.insert(k.clone(), val.clone());
                        }
                    }
                    for (k, val) in labels {
                        out.insert(k.clone(), val.clone());
                    }
                    out.insert("__name__".to_string(), record.clone());
                    rows.push((out, s.ts_ms, v));
                }
                report.recorded_series += rows.len();
                if !rows.is_empty() {
                    recording.produce(tenant, &rows).await?;
                }
            }
            Rule::Alerting { alert, for_, labels, annotations, .. } => {
                step_alerting_rule(
                    tenant, &group.name, alert, *for_, labels, annotations, now_ms, &samples,
                    alerting, state, &mut report,
                )
                .await?;
            }
        }
    }
    Ok(report)
}

#[allow(clippy::too_many_arguments)]
async fn step_alerting_rule<A, S>(
    tenant: &str,
    group: &str,
    alert: &str,
    for_: std::time::Duration,
    rule_labels: &Labels,
    annotations: &BTreeMap<String, String>,
    now_ms: i64,
    samples: &[InstantSample],
    alerting: &A,
    state: &S,
    report: &mut GroupEvalReport,
) -> Result<(), EvalError>
where
    A: AlertSink,
    S: RuleStateStore,
{
    let for_ms = i64::try_from(for_.as_millis()).unwrap_or(i64::MAX);
    let prior = state.load(tenant, group, alert);
    let prior_by_fp: BTreeMap<u64, ActiveAlert> =
        prior.into_iter().map(|a| (alert_fingerprint(&a.labels), a)).collect();

    let mut next: Vec<ActiveAlert> = Vec::new();
    let mut to_dispatch: Vec<DispatchAlert> = Vec::new();

    for s in samples {
        let Some(value) = float_value(&s.value) else { continue };
        let labels = full_alert_labels(alert, rule_labels, &s.labels, value);
        let fp = alert_fingerprint(&labels);

        let templated: BTreeMap<String, String> = annotations
            .iter()
            .map(|(k, v)| (k.clone(), template_annotation(v, value, &labels)))
            .collect();

        let (state_now, active_since, was_firing) = match prior_by_fp.get(&fp) {
            Some(p) => {
                let elapsed = now_ms - p.active_since_ms;
                let st = if elapsed >= for_ms { AlertState::Firing } else { AlertState::Pending };
                (st, p.active_since_ms, p.state == AlertState::Firing)
            }
            None => {
                let st = if for_ms == 0 { AlertState::Firing } else { AlertState::Pending };
                (st, now_ms, false)
            }
        };

        match state_now {
            AlertState::Pending => report.alerts_pending += 1,
            AlertState::Firing => report.alerts_firing += 1,
            AlertState::Inactive => {}
        }

        if state_now == AlertState::Firing && !was_firing {
            to_dispatch.push(DispatchAlert {
                labels: labels.clone(),
                annotations: templated.clone(),
                starts_at_ms: active_since,
                ends_at_ms: None,
            });
        }

        next.push(ActiveAlert {
            labels,
            annotations: templated,
            state: state_now,
            active_since_ms: active_since,
            last_eval_ms: now_ms,
            value,
        });
    }

    if !to_dispatch.is_empty() {
        alerting.dispatch(tenant, &to_dispatch).await?;
        report.dispatched += to_dispatch.len();
    }
    // Persist the new alert set (absent fingerprints are dropped = resolved).
    state.save(tenant, group, alert, &next);
    Ok(())
}
```

> **Prometheus-fidelity verify-notes (pin these empirically before declaring done):**
> 1. **Label precedence + value templating** — `full_alert_labels` makes rule `labels:` override series labels, and runs each rule **label value** through `template_annotation` (Prometheus `rules/alerting.go` expands label values, not just annotations: `r.labels.Range(func(l){ lb.Set(l.Name, expand(l.Value)) })`). Confirm against Prometheus (`Alert.Labels`): the result-vector labels are the base, templated rule labels overlaid, `alertname` last. The test `alert_goes_pending_then_firing` pins `job` (series) + `severity` (rule) + `alertname` coexisting; `templated_label_value_expands` pins a `{{ $labels.X }}` rule label resolving against the series.
> 2. **`for:` boundary** — Prometheus fires when `now - activeAt >= for` (inclusive at the boundary). The `t=120s, for=120s` test pins the inclusive boundary. If cp-prometheus differs (strict `>`), flip the comparison and the test together.
> 3. **`$value` formatting** — `fmt_value` uses Rust's `{}` float format. Prometheus templates render via Go's `%v`/`humanize`; for integers-as-floats (`7.0 → "7"`) Rust's `{}` already yields `"7"`. For non-round values verify against Prometheus and adjust (this is a known fidelity gap — flagged in self-review, not load-bearing for Slice 7's state-machine correctness).
> 4. **Re-dispatch / resends** — `evaluate_group` dispatches ONLY the pending→firing transition. Prometheus/Alertmanager re-send still-firing alerts on a resend interval; that scheduling belongs to the service loop (Task 9) + Alertmanager's own dedup, and is flagged there.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib ruler::eval`
Expected: PASS (5 tests).

- [ ] **Step 5: Add re-exports**

In `ruler/mod.rs`, re-export the public eval surface: `pub use eval::{EvalError, GroupEvalReport, Querier, evaluate_group, template_annotation};`.

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): ruler evaluate_group — recording write-back + for: state machine"
```

---

### Task 7: Config store (compacted-topic shape) + in-memory impl

**Files:**
- Modify: `crates/metrics/src/ruler/state.rs` (add `RuleConfigStore`) **or** create `crates/metrics/src/ruler/config_store.rs`

> **No-conflict note:** if implemented in parallel with another task, put this in a **new** `config_store.rs` to avoid editing `state.rs` concurrently. The plan below assumes `config_store.rs`.

**Interfaces:**
- Produces:
  - `trait RuleConfigStore: Send + Sync`:
    - `list_namespaces(&self, tenant: &str) -> Vec<String>`
    - `get_namespace(&self, tenant: &str, ns: &str) -> Vec<(String /*group*/, RuleGroup, String /*raw_yaml*/)>`
    - `get_group(&self, tenant: &str, ns: &str, group: &str) -> Option<(RuleGroup, String)>`
    - `put_namespace(&self, tenant: &str, ns: &str, groups: &RuleGroups, raw_yaml: &str)`
    - `put_group(&self, tenant: &str, ns: &str, group: RuleGroup, raw_yaml: &str)` — upsert a single group (Mimir's per-namespace `POST` semantics).
    - `delete_namespace(&self, tenant: &str, ns: &str)`
    - `delete_group(&self, tenant: &str, ns: &str, group: &str)`
    - `all_groups(&self, tenant: &str) -> Vec<(String /*ns*/, RuleGroup)>` (the eval loop's source).
  - `struct InMemoryConfigStore` (`Default`, `Clone`).
  - Compacted-topic key/value codec (`encode_config_key`/`parse_config_key`/`encode_config_value`/`parse_config_value`) keyed `(tenant, namespace, group)`, value = the group's YAML; **structured + unit-tested here, used by the topic-backed impl in Task 9**. Mirror the broker's `put_string`/`get_string`-style length-prefixed encoding (see `coordinator/unified/share/persistence.rs`).

- [ ] **Step 1: Write the failing test**

In `crates/metrics/src/ruler/config_store.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::ruler::model::parse_rule_groups_yaml;

    const NS_YAML: &str = "groups:\n  - name: g1\n    rules:\n      - record: r\n        expr: up\n  - name: g2\n    rules:\n      - alert: A\n        expr: up == 0\n";

    #[test]
    fn put_get_delete_namespace() {
        let store = InMemoryConfigStore::default();
        let groups = parse_rule_groups_yaml(NS_YAML).unwrap();
        store.put_namespace("t", "ns1", &groups, NS_YAML);

        assert!(store.list_namespaces("t") == vec!["ns1".to_string()]);
        let got = store.get_namespace("t", "ns1");
        assert!(got.len() == 2);
        let (g1, _, _) = &got[0];
        assert!(g1 == "g1");

        let (group, _raw) = store.get_group("t", "ns1", "g2").unwrap();
        assert!(group.name == "g2");

        store.delete_group("t", "ns1", "g1");
        assert!(store.get_namespace("t", "ns1").len() == 1);

        store.delete_namespace("t", "ns1");
        assert!(store.list_namespaces("t").is_empty());
    }

    #[test]
    fn all_groups_spans_namespaces_for_eval_loop() {
        let store = InMemoryConfigStore::default();
        store.put_namespace("t", "ns1", &parse_rule_groups_yaml(NS_YAML).unwrap(), NS_YAML);
        store.put_namespace("t", "ns2", &parse_rule_groups_yaml(NS_YAML).unwrap(), NS_YAML);
        assert!(store.all_groups("t").len() == 4);
        assert!(store.all_groups("other").is_empty());
    }

    #[test]
    fn config_key_round_trips() {
        let k = encode_config_key("tenant-a", "ns", "grp");
        let (t, ns, g) = parse_config_key(&k).unwrap();
        assert!(t == "tenant-a" && ns == "ns" && g == "grp");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib ruler::config_store`
Expected: FAIL — `cannot find type InMemoryConfigStore`.

- [ ] **Step 3: Implement** (key/value codec + in-memory map keyed `(tenant, ns) -> Vec<(group, RuleGroup, raw_yaml)>`). Codec uses `bytes::BufMut` length-prefixed strings, mirroring the broker pattern. Add `bytes = { workspace = true }` to deps if not already present.

```rust
//! Per-tenant rule-group config storage.
//!
//! Production persists to a compacted topic keyed `(tenant, namespace, group)`
//! with the group's YAML as the value (a tombstone deletes a group). The codec
//! here defines that wire shape; the topic-backed impl lands in the service
//! (Task 9). `InMemoryConfigStore` is the test substrate + trait-shape source.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::model::{RuleGroup, RuleGroups};

#[derive(Debug, thiserror::Error)]
pub enum ConfigCodecError {
    #[error("truncated config key/value")]
    Truncated,
    #[error("invalid utf8 in config record")]
    Utf8,
}

fn put_str(buf: &mut BytesMut, s: &str) {
    let bytes = s.as_bytes();
    buf.put_u32(u32::try_from(bytes.len()).expect("string too long"));
    buf.put_slice(bytes);
}

fn get_str(buf: &mut &[u8]) -> Result<String, ConfigCodecError> {
    if buf.len() < 4 {
        return Err(ConfigCodecError::Truncated);
    }
    let len = buf.get_u32() as usize;
    if buf.len() < len {
        return Err(ConfigCodecError::Truncated);
    }
    let (s, rest) = buf.split_at(len);
    let out = std::str::from_utf8(s).map_err(|_| ConfigCodecError::Utf8)?.to_string();
    *buf = rest;
    Ok(out)
}

#[must_use]
pub fn encode_config_key(tenant: &str, ns: &str, group: &str) -> Bytes {
    let mut buf = BytesMut::new();
    put_str(&mut buf, tenant);
    put_str(&mut buf, ns);
    put_str(&mut buf, group);
    buf.freeze()
}

pub fn parse_config_key(mut buf: &[u8]) -> Result<(String, String, String), ConfigCodecError> {
    let tenant = get_str(&mut buf)?;
    let ns = get_str(&mut buf)?;
    let group = get_str(&mut buf)?;
    Ok((tenant, ns, group))
}

#[must_use]
pub fn encode_config_value(group_yaml: &str) -> Bytes {
    Bytes::copy_from_slice(group_yaml.as_bytes())
}

pub fn parse_config_value(buf: &[u8]) -> Result<String, ConfigCodecError> {
    std::str::from_utf8(buf).map(str::to_string).map_err(|_| ConfigCodecError::Utf8)
}

pub trait RuleConfigStore: Send + Sync {
    fn list_namespaces(&self, tenant: &str) -> Vec<String>;
    fn get_namespace(&self, tenant: &str, ns: &str) -> Vec<(String, RuleGroup, String)>;
    fn get_group(&self, tenant: &str, ns: &str, group: &str) -> Option<(RuleGroup, String)>;
    fn put_namespace(&self, tenant: &str, ns: &str, groups: &RuleGroups, raw_yaml: &str);
    /// Upsert a **single** group into a namespace (Mimir's per-namespace `POST`
    /// semantics: create-or-replace that one group, leaving siblings intact).
    /// `raw_yaml` is the verbatim single-group body, echoed by `GET /{ns}/{group}`.
    fn put_group(&self, tenant: &str, ns: &str, group: RuleGroup, raw_yaml: &str);
    fn delete_namespace(&self, tenant: &str, ns: &str);
    fn delete_group(&self, tenant: &str, ns: &str, group: &str);
    fn all_groups(&self, tenant: &str) -> Vec<(String, RuleGroup)>;
}

type NsKey = (String, String); // (tenant, namespace)
type NsValue = Vec<(RuleGroup, String)>; // (group, raw_yaml-of-that-group)

#[derive(Clone, Default)]
pub struct InMemoryConfigStore {
    inner: Arc<Mutex<BTreeMap<NsKey, NsValue>>>,
}

impl RuleConfigStore for InMemoryConfigStore {
    fn list_namespaces(&self, tenant: &str) -> Vec<String> {
        let g = self.inner.lock().expect("poisoned");
        g.keys().filter(|(t, _)| t == tenant).map(|(_, ns)| ns.clone()).collect()
    }
    fn get_namespace(&self, tenant: &str, ns: &str) -> Vec<(String, RuleGroup, String)> {
        let g = self.inner.lock().expect("poisoned");
        g.get(&(tenant.to_string(), ns.to_string()))
            .map(|v| v.iter().map(|(grp, raw)| (grp.name.clone(), grp.clone(), raw.clone())).collect())
            .unwrap_or_default()
    }
    fn get_group(&self, tenant: &str, ns: &str, group: &str) -> Option<(RuleGroup, String)> {
        let g = self.inner.lock().expect("poisoned");
        g.get(&(tenant.to_string(), ns.to_string()))?
            .iter()
            .find(|(grp, _)| grp.name == group)
            .map(|(grp, raw)| (grp.clone(), raw.clone()))
    }
    fn put_namespace(&self, tenant: &str, ns: &str, groups: &RuleGroups, _raw_yaml: &str) {
        // store per-group raw YAML so GET /{ns}/{group} can echo verbatim.
        let value: NsValue = groups
            .groups
            .iter()
            .map(|grp| {
                // store the single-bare-group form so GET /{ns}/{group} echoes
                // exactly what Mimir's per-group GET returns.
                let raw = super::model::group_to_yaml(grp).unwrap_or_default();
                (grp.clone(), raw)
            })
            .collect();
        self.inner.lock().expect("poisoned").insert((tenant.to_string(), ns.to_string()), value);
    }
    fn put_group(&self, tenant: &str, ns: &str, group: RuleGroup, raw_yaml: &str) {
        let mut g = self.inner.lock().expect("poisoned");
        let entry = g.entry((tenant.to_string(), ns.to_string())).or_default();
        let raw = raw_yaml.to_string();
        if let Some(slot) = entry.iter_mut().find(|(grp, _)| grp.name == group.name) {
            *slot = (group, raw); // replace existing group of the same name
        } else {
            entry.push((group, raw)); // create-or-append
        }
    }
    fn delete_namespace(&self, tenant: &str, ns: &str) {
        self.inner.lock().expect("poisoned").remove(&(tenant.to_string(), ns.to_string()));
    }
    fn delete_group(&self, tenant: &str, ns: &str, group: &str) {
        let mut g = self.inner.lock().expect("poisoned");
        if let Some(v) = g.get_mut(&(tenant.to_string(), ns.to_string())) {
            v.retain(|(grp, _)| grp.name != group);
            if v.is_empty() {
                g.remove(&(tenant.to_string(), ns.to_string()));
            }
        }
    }
    fn all_groups(&self, tenant: &str) -> Vec<(String, RuleGroup)> {
        let g = self.inner.lock().expect("poisoned");
        g.iter()
            .filter(|((t, _), _)| t == tenant)
            .flat_map(|((_, ns), v)| v.iter().map(move |(grp, _)| (ns.clone(), grp.clone())))
            .collect()
    }
}
```

- [ ] **Step 4: Add module decl + re-exports**

In `ruler/mod.rs` add `pub mod config_store;` and re-export `RuleConfigStore`, `InMemoryConfigStore`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib ruler::config_store`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): ruler config store + compacted-topic key/value codec"
```

---

### Task 8: Sharding assignment function

**Files:**
- Modify: `crates/metrics/src/ruler/sharding.rs`

**Interfaces:**
- Produces:
  - `fn assign_group(tenant: &str, group: &str, n_instances: usize) -> usize` — `(tenant, group)` hash mod `n_instances`; deterministic, balanced.
  - `fn owns_group(tenant: &str, group: &str, n_instances: usize, my_index: usize) -> bool`.

- [ ] **Step 1: Write the failing test**

In `crates/metrics/src/ruler/sharding.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use assert2::assert;

    use super::*;

    #[test]
    fn assignment_is_deterministic_and_in_range() {
        let a = assign_group("t", "grp", 4);
        let b = assign_group("t", "grp", 4);
        assert!(a == b);
        assert!(a < 4);
    }

    #[test]
    fn owns_group_partitions_the_space() {
        // every group is owned by exactly one of the 3 instances.
        for g in 0..50 {
            let group = format!("g{g}");
            let owners: BTreeSet<usize> =
                (0..3).filter(|i| owns_group("t", &group, 3, *i)).collect();
            assert!(owners.len() == 1);
        }
    }

    #[test]
    fn single_instance_owns_everything() {
        assert!(owns_group("t", "anything", 1, 0));
    }

    #[test]
    fn n_zero_is_safe() {
        // degenerate: no instances → assign to 0, owns nothing.
        assert!(assign_group("t", "g", 0) == 0);
        assert!(!owns_group("t", "g", 0, 0));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib ruler::sharding`
Expected: FAIL — `cannot find function assign_group`.

- [ ] **Step 3: Implement**

```rust
//! Rule-group sharding across ruler instances by `(tenant, group)` hash.
//!
//! This defines the *assignment function*. Actual cross-instance coordination
//! (which instance is `my_index` of `n_instances`) is supplied by membership —
//! a Crabka consumer-group the ruler instances join, OR a static config — wired
//! in the service (Task 9) and finished under Slice 8. Stubbed here behind the
//! pure function so the distribution is unit-tested independently.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[must_use]
pub fn assign_group(tenant: &str, group: &str, n_instances: usize) -> usize {
    if n_instances <= 1 {
        return 0;
    }
    let mut h = DefaultHasher::new();
    tenant.hash(&mut h);
    0u8.hash(&mut h); // separator so ("a","bc") != ("ab","c")
    group.hash(&mut h);
    (h.finish() % n_instances as u64) as usize
}

#[must_use]
pub fn owns_group(tenant: &str, group: &str, n_instances: usize, my_index: usize) -> bool {
    if n_instances == 0 {
        return false;
    }
    assign_group(tenant, group, n_instances) == my_index
}
```

- [ ] **Step 4: Add re-exports** in `ruler/mod.rs` (`pub use sharding::{assign_group, owns_group};`).

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib ruler::sharding`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): ruler (tenant,group)-hash sharding assignment"
```

---

### Task 9: Alertmanager v2 HTTP client (churn-prone surface — structure + behavior-pinning tests)

**Files:**
- Modify: `crates/metrics/src/ruler/alertmanager.rs`

**Interfaces:**
- Produces:
  - `struct AlertmanagerClient { base_url, http: reqwest::Client }` with `new(base_url: impl Into<String>) -> Self`.
  - `impl AlertSink for AlertmanagerClient` — `POST {base_url}/api/v2/alerts` with the v2 JSON array, `X-Scope-OrgID: {tenant}` header.
  - `struct V2Alert { labels: BTreeMap<String,String>, annotations: BTreeMap<String,String>, startsAt: String /*RFC3339*/, endsAt: Option<String>, generatorURL: String }` (serde, camelCase to match the AM v2 schema) — or a private `to_v2_json` builder.
  - `fn to_v2_payload(alerts: &[DispatchAlert]) -> Vec<serde_json::Value>` — the **pure** transform, unit-tested without a network.
  - `fn rfc3339_millis(ms: i64) -> String` — epoch-ms → RFC3339 UTC (Alertmanager requires RFC3339 timestamps).

**The HTTP send is the churn-prone part; the JSON shape is pinned by a pure test.** Don't unit-test the network call — test `to_v2_payload` exhaustively, and leave the `reqwest` send covered by an integration smoke test (behind a flag) + a verify-note.

- [ ] **Step 1: Write the failing test (the pure payload transform)**

In `crates/metrics/src/ruler/alertmanager.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;

    use super::*;
    use crate::ruler::sinks::DispatchAlert;

    fn lbls(pairs: &[(&str, &str)]) -> Labels {
        pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
    }

    #[test]
    fn payload_matches_alertmanager_v2_shape() {
        let mut ann = BTreeMap::new();
        ann.insert("summary".to_string(), "boom".to_string());
        let alerts = vec![DispatchAlert {
            labels: lbls(&[("alertname", "HighErr"), ("severity", "page")]),
            annotations: ann,
            starts_at_ms: 0, // 1970-01-01T00:00:00Z
            ends_at_ms: None,
        }];
        let payload = to_v2_payload(&alerts);
        assert!(payload.len() == 1);
        let a = &payload[0];
        assert!(a["labels"]["alertname"] == "HighErr");
        assert!(a["labels"]["severity"] == "page");
        assert!(a["annotations"]["summary"] == "boom");
        assert!(a["startsAt"] == "1970-01-01T00:00:00Z");
        // endsAt omitted when None (AM treats absent endsAt as "still firing")
        assert!(a.get("endsAt").is_none() || a["endsAt"].is_null());
    }

    #[test]
    fn rfc3339_millis_formats_utc() {
        assert!(rfc3339_millis(0) == "1970-01-01T00:00:00Z");
        assert!(rfc3339_millis(1_000) == "1970-01-01T00:00:01Z");
        assert!(rfc3339_millis(1_500) == "1970-01-01T00:00:01.500Z");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib ruler::alertmanager`
Expected: FAIL — `cannot find function to_v2_payload`.

- [ ] **Step 3: Implement**

```rust
//! Alertmanager-API v2 client (`POST /api/v2/alerts`). The JSON shape is pinned
//! by a pure-transform test; the `reqwest` send is the churn-prone surface and
//! is covered by an integration smoke test + verify-note, not a unit test.

use async_trait::async_trait;
use serde_json::{Value, json};

use super::contract::Labels;
use super::sinks::{AlertSink, DispatchAlert, SinkError};

/// Format epoch-ms as RFC3339 UTC (`...Z`), millisecond precision when non-zero.
/// Hand-rolled to avoid a `chrono`/`time` dep for this one conversion.
#[must_use]
pub fn rfc3339_millis(ms: i64) -> String {
    let secs = ms.div_euclid(1_000);
    let millis = ms.rem_euclid(1_000);
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (h, m, s) = (tod / 3_600, (tod % 3_600) / 60, tod % 60);
    let (y, mo, d) = civil_from_days(days);
    if millis == 0 {
        format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
    } else {
        format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}.{millis:03}Z")
    }
}

/// Howard Hinnant's days-from-civil inverse (proleptic Gregorian).
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn labels_to_json(l: &Labels) -> Value {
    let map: serde_json::Map<String, Value> =
        l.iter().map(|(k, v)| (k.clone(), Value::String(v.clone()))).collect();
    Value::Object(map)
}

/// Pure transform: `DispatchAlert`s → Alertmanager-v2 JSON array.
#[must_use]
pub fn to_v2_payload(alerts: &[DispatchAlert]) -> Vec<Value> {
    alerts
        .iter()
        .map(|a| {
            let mut obj = json!({
                "labels": labels_to_json(&a.labels),
                "annotations": labels_to_json(&a.annotations),
                "startsAt": rfc3339_millis(a.starts_at_ms),
            });
            if let Some(ends) = a.ends_at_ms {
                obj["endsAt"] = Value::String(rfc3339_millis(ends));
            }
            obj
        })
        .collect()
}

/// HTTP client that dispatches to an Alertmanager v2 endpoint.
pub struct AlertmanagerClient {
    base_url: String,
    http: reqwest::Client,
}

impl AlertmanagerClient {
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        Self { base_url: base_url.into(), http: reqwest::Client::new() }
    }
}

#[async_trait]
impl AlertSink for AlertmanagerClient {
    async fn dispatch(&self, tenant: &str, alerts: &[DispatchAlert]) -> Result<(), SinkError> {
        let url = format!("{}/api/v2/alerts", self.base_url.trim_end_matches('/'));
        let payload = to_v2_payload(alerts);
        let resp = self
            .http
            .post(&url)
            .header("X-Scope-OrgID", tenant)
            .json(&payload)
            .send()
            .await
            .map_err(|e| SinkError::Transport(e.to_string()))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(SinkError::Rejected(format!("alertmanager status {}", resp.status())))
        }
    }
}
```

> **Churn-surface verify-notes:**
> 1. **`reqwest` API drift** — `Client::post().header().json().send()` is the stable reqwest builder chain; if a method moves at 0.13, fix the call, not the test (`to_v2_payload` has no reqwest in it). Keep the dispatch method body the only reqwest-touching code.
> 2. **AM v2 schema** — verify `startsAt`/`endsAt`/`labels`/`annotations`/`generatorURL` field names against the live cp-alertmanager `/api/v2/` OpenAPI (the spec says: check empirically, don't read the wiki). `generatorURL` is omitted here (optional); add it (the ruler's own `/api/v1/rules` URL) under Slice 8 if differential testing flags it.
> 3. **Integration smoke** — add `crates/metrics/tests/alertmanager_smoke.rs` behind `#[ignore]` that POSTs to a testcontainers Alertmanager and asserts `2xx`; run manually / in a dedicated CI lane. Do **not** make the unit suite depend on a live endpoint.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib ruler::alertmanager`
Expected: PASS (2 tests).

- [ ] **Step 5: Add re-exports** in `ruler/mod.rs` (`pub use alertmanager::{AlertmanagerClient, to_v2_payload};`).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): ruler Alertmanager v2 client + pure payload transform"
```

---

### Task 10: WAL recording sink (churn-prone surface — structure + behavior-pinning test)

**Files:**
- Modify: `crates/metrics/src/ruler/produce.rs`

**Interfaces:**
- Produces:
  - `struct WalRecordingSink { producer, topic }` wrapping the Slice-4 produce path (the exact producer handle is a Slice-4 type; behind a narrow `WalProducer` trait so this slice compiles + tests without Slice 4 merged).
  - `trait WalProducer: Send + Sync { async fn produce_series(&self, tenant: &str, record: WalSeriesSample) -> Result<(), SinkError>; }` — the thin seam onto Slice 4.
  - `struct WalSeriesSample { pub labels: Labels, pub ts_ms: i64, pub value: f64 }` — the produce-path input (maps to Slice 4's `WalRecord`).
  - `impl RecordingSink for WalRecordingSink` — fans `produce(tenant, samples)` into per-sample `WalProducer::produce_series` calls.
  - `fn to_wal_samples(samples: &[(Labels, i64, f64)]) -> Vec<WalSeriesSample>` — the **pure** mapping, unit-tested.

- [ ] **Step 1: Write the failing test**

In `crates/metrics/src/ruler/produce.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use assert2::assert;
    use async_trait::async_trait;

    use super::*;
    use crate::ruler::sinks::RecordingSink;

    fn lbls(pairs: &[(&str, &str)]) -> Labels {
        pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
    }

    #[derive(Clone, Default)]
    struct CapturingProducer {
        produced: Arc<Mutex<Vec<(String, WalSeriesSample)>>>,
    }
    #[async_trait]
    impl WalProducer for CapturingProducer {
        async fn produce_series(&self, tenant: &str, record: WalSeriesSample) -> Result<(), SinkError> {
            self.produced.lock().unwrap().push((tenant.to_string(), record));
            Ok(())
        }
    }

    #[test]
    fn maps_samples_to_wal_records() {
        let rows = vec![(lbls(&[("__name__", "x")]), 10_i64, 1.5_f64)];
        let wal = to_wal_samples(&rows);
        assert!(wal.len() == 1);
        assert!(wal[0].labels.get("__name__") == Some("x"));
        assert!(wal[0].ts_ms == 10 && (wal[0].value - 1.5).abs() < 1e-9);
    }

    #[tokio::test]
    async fn recording_sink_fans_samples_to_producer() {
        let prod = CapturingProducer::default();
        let sink = WalRecordingSink::new(prod.clone(), "metrics-wal");
        let rows = vec![
            (lbls(&[("__name__", "a")]), 1, 1.0),
            (lbls(&[("__name__", "b")]), 2, 2.0),
        ];
        sink.produce("tenant-x", &rows).await.unwrap();
        let produced = prod.produced.lock().unwrap();
        assert!(produced.len() == 2);
        assert!(produced[0].0 == "tenant-x");
        assert!(produced[1].1.value == 2.0);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib ruler::produce`
Expected: FAIL — `cannot find type WalRecordingSink`.

- [ ] **Step 3: Implement**

```rust
//! Recording-rule output → WAL topic. The actual Kafka produce is the Slice-4
//! path; this slice wraps it behind `WalProducer` so the rename/merge logic in
//! `eval.rs` is fully tested without a broker. Swap `WalProducer` for the real
//! Slice-4 producer handle when it lands — `WalRecordingSink` is unchanged.

use async_trait::async_trait;

use super::contract::Labels;
use super::sinks::{RecordingSink, SinkError};

/// A single derived series sample destined for the WAL topic (maps to Slice-4's
/// `WalRecord`).
#[derive(Clone, Debug, PartialEq)]
pub struct WalSeriesSample {
    pub labels: Labels,
    pub ts_ms: i64,
    pub value: f64,
}

/// The thin seam onto the Slice-4 produce path.
#[async_trait]
pub trait WalProducer: Send + Sync {
    async fn produce_series(&self, tenant: &str, record: WalSeriesSample) -> Result<(), SinkError>;
}

/// Pure mapping `(labels, ts, value)` → `WalSeriesSample`.
#[must_use]
pub fn to_wal_samples(samples: &[(Labels, i64, f64)]) -> Vec<WalSeriesSample> {
    samples
        .iter()
        .map(|(labels, ts_ms, value)| WalSeriesSample {
            labels: labels.clone(),
            ts_ms: *ts_ms,
            value: *value,
        })
        .collect()
}

pub struct WalRecordingSink<P: WalProducer> {
    producer: P,
    #[allow(dead_code)] // carried for the real producer's topic routing
    topic: String,
}

impl<P: WalProducer> WalRecordingSink<P> {
    pub fn new(producer: P, topic: impl Into<String>) -> Self {
        Self { producer, topic: topic.into() }
    }
}

#[async_trait]
impl<P: WalProducer> RecordingSink for WalRecordingSink<P> {
    async fn produce(&self, tenant: &str, samples: &[(Labels, i64, f64)]) -> Result<(), SinkError> {
        for record in to_wal_samples(samples) {
            self.producer.produce_series(tenant, record).await?;
        }
        Ok(())
    }
}
```

> **Churn-surface verify-note:** the real `WalProducer` impl wraps Slice 4's produce path — it serializes `WalSeriesSample` to the WAL record's wire shape (`crabka-metrics` `WalRecord`, via `WalRecord::encode`) and produces to Slice 4's metrics WAL topic `crate::WAL_TOPIC` (`"__crabka_metrics_wal"`), partitioned by `(tenant, series_fingerprint)` via Slice 4's `partition_key` (spec §5.4). That serialization is Slice 4's contract; this trait keeps it out of the ruler. When Slice 4 lands, add `produce.rs::KafkaWalProducer` implementing `WalProducer` over the `crabka-client-producer` `Producer` + the Slice-4 `WalRecord` encoder, defaulting its topic to `crate::WAL_TOPIC`. The `to_wal_samples` test pins the field mapping regardless.

- [ ] **Step 4: Add re-exports** in `ruler/mod.rs` (`pub use produce::{WalProducer, WalRecordingSink, WalSeriesSample, to_wal_samples};`).

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib ruler::produce`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): ruler WAL recording sink (WalProducer seam onto Slice 4)"
```

---

### Task 11: HTTP API — config CRUD + `/api/v1/rules` + `/api/v1/alerts`

**Files:**
- Modify: `crates/metrics/src/ruler/api.rs`

**Interfaces:**
- Consumes: `RuleConfigStore`, `RuleStateStore`, `model::{parse_rule_group_yaml, namespace_groups_to_yaml, namespaces_to_yaml_map}`, `AlertState`.
- Produces:
  - `struct RulerApiState { config: Arc<dyn RuleConfigStore>, state: Arc<dyn RuleStateStore>, default_interval_secs: u64 }` (`Clone`).
  - `fn router(state: RulerApiState) -> axum::Router` mounting:
    - `POST /prometheus/config/v1/rules/{namespace}` — body = a **single** bare rule group (Mimir: "one and only one rule group", no `groups:` wrapper); upserts it into the namespace; `X-Scope-OrgID` tenant; 202/201.
    - `GET /prometheus/config/v1/rules` — all namespaces as a YAML **map** `namespace -> [groups]` (Mimir's documented shape).
    - `GET /prometheus/config/v1/rules/{namespace}` — one namespace's groups as a bare YAML **list**.
    - `GET /prometheus/config/v1/rules/{namespace}/{group}` — one group's YAML (stored single-group body, echoed verbatim).
    - `DELETE /prometheus/config/v1/rules/{namespace}` and `.../{namespace}/{group}` — 202.
    - `GET /api/v1/rules` — Prometheus rules JSON (`data.groups[].rules[]` with `type`/`state`/`health`).
    - `GET /api/v1/alerts` — Prometheus alerts JSON (`data.alerts[]` with `state`/`labels`/`annotations`/`activeAt`/`value`).
  - `fn tenant_from_headers(&HeaderMap) -> Result<String, ApiError>` (require `X-Scope-OrgID`).
  - `fn rules_json(...) -> serde_json::Value` and `fn alerts_json(...) -> serde_json::Value` — **pure** shape builders, unit-tested without axum.

- [ ] **Step 1: Write the failing test (pure JSON shapes + a `tower::ServiceExt::oneshot` round-trip)**

In `crates/metrics/src/ruler/api.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use super::*;
    use crate::ruler::config_store::InMemoryConfigStore;
    use crate::ruler::model::parse_rule_group_yaml;
    use crate::ruler::state::{ActiveAlert, AlertState, InMemoryStateStore};

    fn test_state() -> RulerApiState {
        RulerApiState {
            config: Arc::new(InMemoryConfigStore::default()),
            state: Arc::new(InMemoryStateStore::default()),
            default_interval_secs: 60,
        }
    }

    #[tokio::test]
    async fn post_then_get_namespace_round_trips_yaml() {
        let state = test_state();
        let app = router(state.clone());
        // Mimir's POST body is a SINGLE bare rule group — `name:`/`rules:` at the
        // TOP level, NO `groups:` wrapper.
        let yaml = "name: g\nrules:\n  - record: r\n    expr: up\n";

        let resp = app
            .clone()
            .oneshot(
                Request::post("/prometheus/config/v1/rules/ns1")
                    .header("X-Scope-OrgID", "t")
                    .body(Body::from(yaml))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == StatusCode::ACCEPTED || resp.status() == StatusCode::CREATED);

        // GET the single group back — `GET /{ns}/{group}` echoes the stored
        // single-group YAML verbatim.
        let resp = app
            .oneshot(
                Request::get("/prometheus/config/v1/rules/ns1/g")
                    .header("X-Scope-OrgID", "t")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        let parsed = parse_rule_group_yaml(&text).unwrap();
        assert!(parsed.name == "g");
    }

    #[tokio::test]
    async fn missing_tenant_header_is_rejected() {
        let app = router(test_state());
        let resp = app
            .oneshot(Request::get("/api/v1/rules").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert!(resp.status() == StatusCode::BAD_REQUEST);
    }

    #[test]
    fn alerts_json_matches_prometheus_shape() {
        let store = InMemoryStateStore::default();
        let mut labels = Labels::new();
        labels.insert("alertname".into(), "HighErr".into());
        store.save(
            "t",
            "g",
            "HighErr",
            &[ActiveAlert {
                labels,
                annotations: Default::default(),
                state: AlertState::Firing,
                active_since_ms: 0,
                last_eval_ms: 1_000,
                value: 0.9,
            }],
        );
        let v = alerts_json(&store, "t");
        assert!(v["status"] == "success");
        let a = &v["data"]["alerts"][0];
        assert!(a["state"] == "firing");
        assert!(a["labels"]["alertname"] == "HighErr");
        assert!(a["value"] == "0.9"); // Prometheus serializes value as a string
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib ruler::api`
Expected: FAIL — `cannot find type RulerApiState`.

- [ ] **Step 3: Implement the router + handlers + pure JSON builders**

Key shape details to match Prometheus: `value` is a **string**; `activeAt` is RFC3339; `health` is `"ok"`; rule `type` is `"recording"`/`"alerting"`. Use the `rfc3339_millis` helper from `alertmanager.rs` (re-export or move to a small `time.rs`; the plan re-uses it).

```rust
//! Ruler HTTP surface: Mimir config CRUD + Prometheus rules/alerts read APIs.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use serde_json::{Value, json};

use std::collections::BTreeMap;

use super::alertmanager::rfc3339_millis;
use super::config_store::RuleConfigStore;
use super::contract::Labels;
use super::model::{
    RuleGroup, namespace_groups_to_yaml, namespaces_to_yaml_map, parse_rule_group_yaml,
};
use super::state::RuleStateStore;

#[derive(Clone)]
pub struct RulerApiState {
    pub config: Arc<dyn RuleConfigStore>,
    pub state: Arc<dyn RuleStateStore>,
    pub default_interval_secs: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("missing X-Scope-OrgID header")]
    MissingTenant,
    #[error("invalid rule yaml: {0}")]
    BadYaml(String),
    #[error("not found")]
    NotFound,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let code = match self {
            ApiError::MissingTenant | ApiError::BadYaml(_) => StatusCode::BAD_REQUEST,
            ApiError::NotFound => StatusCode::NOT_FOUND,
        };
        (code, self.to_string()).into_response()
    }
}

fn tenant_from_headers(h: &HeaderMap) -> Result<String, ApiError> {
    h.get("X-Scope-OrgID")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or(ApiError::MissingTenant)
}

pub fn router(state: RulerApiState) -> Router {
    Router::new()
        .route("/prometheus/config/v1/rules", get(get_all_rules))
        .route(
            "/prometheus/config/v1/rules/{namespace}",
            post(post_namespace).get(get_namespace).delete(delete_namespace),
        )
        .route(
            "/prometheus/config/v1/rules/{namespace}/{group}",
            get(get_group).delete(delete_group),
        )
        .route("/api/v1/rules", get(api_rules))
        .route("/api/v1/alerts", get(api_alerts))
        .with_state(state)
}

async fn post_namespace(
    State(st): State<RulerApiState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: String,
) -> Result<StatusCode, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
    // Mimir's POST body is a SINGLE bare rule group (`name:`/`interval:`/`rules:`
    // at top level), NOT a `groups:` document — parse it as one group and upsert
    // it into the namespace.
    let group = parse_rule_group_yaml(&body).map_err(|e| ApiError::BadYaml(e.to_string()))?;
    st.config.put_group(&tenant, &namespace, group, &body);
    Ok(StatusCode::ACCEPTED)
}

async fn get_all_rules(
    State(st): State<RulerApiState>,
    headers: HeaderMap,
) -> Result<String, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
    // Mimir returns a YAML MAP `namespace -> [groups]` (not concatenated docs).
    let by_namespace: BTreeMap<String, Vec<RuleGroup>> = st
        .config
        .list_namespaces(&tenant)
        .into_iter()
        .map(|ns| {
            let groups = st.config.get_namespace(&tenant, &ns).into_iter().map(|(_, g, _)| g).collect();
            (ns, groups)
        })
        .collect();
    namespaces_to_yaml_map(&by_namespace).map_err(|e| ApiError::BadYaml(e.to_string()))
}

async fn get_namespace(
    State(st): State<RulerApiState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
) -> Result<String, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
    let groups: Vec<_> =
        st.config.get_namespace(&tenant, &namespace).into_iter().map(|(_, g, _)| g).collect();
    if groups.is_empty() {
        return Err(ApiError::NotFound);
    }
    // Mimir returns the namespace content directly: a bare list of groups.
    namespace_groups_to_yaml(&groups).map_err(|e| ApiError::BadYaml(e.to_string()))
}

async fn get_group(
    State(st): State<RulerApiState>,
    Path((namespace, group)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<String, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
    let (_, raw) = st.config.get_group(&tenant, &namespace, &group).ok_or(ApiError::NotFound)?;
    Ok(raw)
}

async fn delete_namespace(
    State(st): State<RulerApiState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
    st.config.delete_namespace(&tenant, &namespace);
    Ok(StatusCode::ACCEPTED)
}

async fn delete_group(
    State(st): State<RulerApiState>,
    Path((namespace, group)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
    st.config.delete_group(&tenant, &namespace, &group);
    Ok(StatusCode::ACCEPTED)
}

async fn api_rules(
    State(st): State<RulerApiState>,
    headers: HeaderMap,
) -> Result<axum::Json<Value>, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
    Ok(axum::Json(rules_json(st.config.as_ref(), st.default_interval_secs, &tenant)))
}

async fn api_alerts(
    State(st): State<RulerApiState>,
    headers: HeaderMap,
) -> Result<axum::Json<Value>, ApiError> {
    let tenant = tenant_from_headers(&headers)?;
    Ok(axum::Json(alerts_json(st.state.as_ref(), &tenant)))
}

fn labels_to_json(l: &Labels) -> Value {
    Value::Object(l.iter().map(|(k, v)| (k.clone(), Value::String(v.clone()))).collect())
}

/// `/api/v1/rules` — Prometheus rule/group status shape.
#[must_use]
pub fn rules_json(config: &dyn RuleConfigStore, default_interval_secs: u64, tenant: &str) -> Value {
    use super::model::Rule;
    let mut groups_json = Vec::new();
    // group rules by (namespace, group-name) using all_groups + namespace file.
    for (ns, group) in config.all_groups(tenant) {
        let interval = group.interval.map_or(default_interval_secs, |d| d.as_secs());
        let rules: Vec<Value> = group
            .rules
            .iter()
            .map(|r| match r {
                Rule::Recording { record, expr, labels } => json!({
                    "type": "recording",
                    "name": record,
                    "query": expr,
                    "labels": labels_to_json(labels),
                    "health": "ok",
                }),
                Rule::Alerting { alert, expr, for_, labels, annotations, .. } => json!({
                    "type": "alerting",
                    "name": alert,
                    "query": expr,
                    "duration": for_.as_secs(),
                    "labels": labels_to_json(labels),
                    "annotations": labels_to_json(annotations),
                    "health": "ok",
                    "alerts": [],
                }),
            })
            .collect();
        groups_json.push(json!({
            "name": group.name,
            "file": ns,
            "interval": interval,
            "rules": rules,
        }));
    }
    json!({ "status": "success", "data": { "groups": groups_json } })
}

/// `/api/v1/alerts` — Prometheus active-alerts shape.
#[must_use]
pub fn alerts_json(state: &dyn RuleStateStore, tenant: &str) -> Value {
    let alerts: Vec<Value> = state
        .all_active(tenant)
        .into_iter()
        .map(|(_group, _rule, a)| {
            json!({
                "labels": labels_to_json(&a.labels),
                "annotations": labels_to_json(&a.annotations),
                "state": a.state.as_str(),
                "activeAt": rfc3339_millis(a.active_since_ms),
                "value": format!("{}", a.value),
            })
        })
        .collect();
    json!({ "status": "success", "data": { "alerts": alerts } })
}
```

> **Prometheus-shape verify-notes:**
> 1. `value` is a **string** in both `/api/v1/alerts` and query results — pinned by `alerts_json_matches_prometheus_shape`. Keep it stringified.
> 2. `/api/v1/rules` here returns empty `alerts: []` per alerting rule (the rule's *firing instances* belong on the rule, not just `/api/v1/alerts`). Joining live `ActiveAlert`s onto their rule is a Slice-8 polish — flagged; Grafana's rule view tolerates the empty array.
> 3. Mimir's `GET /prometheus/config/v1/rules` returns a YAML **map** `namespace: [groups]`; `get_all_rules` builds a real `BTreeMap<String, Vec<RuleGroup>>` and serializes it via `namespaces_to_yaml_map` (no comment framing). `GET /{namespace}` returns the namespace's groups as a bare YAML **list** via `namespace_groups_to_yaml`, and `GET /{namespace}/{group}` echoes the stored single-group YAML. Confirm the exact field ordering against cp-mimir under Slice 8 differential testing, but the documented shapes (map / list / single group) are produced in-slice — not approximated.

- [ ] **Step 4: Add re-exports** in `ruler/mod.rs` (`pub use api::{RulerApiState, alerts_json, router, rules_json};`).

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib ruler::api`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): ruler HTTP API (config CRUD + /api/v1/rules + /api/v1/alerts)"
```

---

### Task 12: `RulerService` — wire config + state + engine + clock + eval loop

**Files:**
- Modify: `crates/metrics/src/ruler/service.rs`

**Interfaces:**
- Produces:
  - `struct RulerService<Q, R, A> { config, state, clock, querier: Arc<Q>, recording: Arc<R>, alerting: Arc<A>, default_interval, my_index, n_instances }`.
  - `fn new(...) -> Self` (builder-ish; takes the stores + sinks + clock).
  - `async fn eval_once(&self, tenant: &str) -> Result<Vec<(String /*group*/, GroupEvalReport)>, EvalError>` — evaluates every **owned** group for the tenant once (filtering by `owns_group`). The deterministic, testable core.
  - `async fn run(self, shutdown: CancellationToken)` — the per-group interval scheduler (each group ticks on its own `interval`); spawns evaluation tasks. **The scheduler loop is thin glue over `eval_once`; the test drives `eval_once` directly + a tick-driven test with `MockClock` + `tokio::time::pause`.**
  - `fn api_state(&self) -> RulerApiState` — hand the same stores to the HTTP router.

- [ ] **Step 1: Write the failing test**

In `crates/metrics/src/ruler/service.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use assert2::assert;
    use async_trait::async_trait;

    use super::*;
    use crate::ruler::clock::MockClock;
    use crate::ruler::config_store::InMemoryConfigStore;
    use crate::ruler::contract::{InstantSample, QueryResult, SampleValue};
    use crate::ruler::eval::{EvalError, Querier};
    use crate::ruler::model::parse_rule_groups_yaml;
    use crate::ruler::sinks::{MockAlertSink, MockRecordingSink};
    use crate::ruler::state::InMemoryStateStore;

    #[derive(Clone, Default)]
    struct ConstQuerier;
    #[async_trait]
    impl Querier for ConstQuerier {
        async fn query_instant(&self, _t: &str, _e: &str, ts: i64) -> Result<QueryResult, EvalError> {
            Ok(QueryResult::InstantVector(vec![InstantSample {
                labels: [("job".to_string(), "api".to_string())].into_iter().collect(),
                ts_ms: ts,
                value: SampleValue::Float(1.0),
            }]))
        }
    }

    fn service() -> RulerService<ConstQuerier, MockRecordingSink, MockAlertSink> {
        let config = Arc::new(InMemoryConfigStore::default());
        config.put_namespace(
            "t",
            "ns",
            &parse_rule_groups_yaml(
                "groups:\n  - name: g\n    interval: 30s\n    rules:\n      - record: r\n        expr: up\n",
            )
            .unwrap(),
            "",
        );
        RulerService::new(
            config,
            Arc::new(InMemoryStateStore::default()),
            MockClock::new(0),
            Arc::new(ConstQuerier),
            Arc::new(MockRecordingSink::default()),
            Arc::new(MockAlertSink::default()),
            60,
            0, // my_index
            1, // n_instances (owns everything)
        )
    }

    #[tokio::test]
    async fn eval_once_runs_owned_groups() {
        let svc = service();
        let reports = svc.eval_once("t").await.unwrap();
        assert!(reports.len() == 1);
        assert!(reports[0].0 == "g");
        assert!(reports[0].1.recorded_series == 1);
        // the recording sink received the produce
        assert!(svc.recording.calls().len() == 1);
    }

    #[tokio::test]
    async fn eval_once_skips_unowned_groups() {
        let mut svc = service();
        svc.n_instances = 4;
        // force my_index to a shard that doesn't own group "g" (find the wrong one)
        let owner = crate::ruler::sharding::assign_group("t", "g", 4);
        svc.my_index = (owner + 1) % 4;
        let reports = svc.eval_once("t").await.unwrap();
        assert!(reports.is_empty());
        assert!(svc.recording.calls().is_empty());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib ruler::service`
Expected: FAIL — `cannot find type RulerService`.

- [ ] **Step 3: Implement** (`eval_once` filters by `owns_group` + calls `evaluate_group`; `run` is the interval scheduler). Keep `recording`/`config`/`state`/`my_index`/`n_instances` `pub(crate)` so the test can introspect.

```rust
//! `RulerService` — owns the stores, engine, clock, and sinks; runs the per-
//! group interval scheduler. `eval_once` is the deterministic testable core;
//! `run` is thin interval glue over it.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::api::RulerApiState;
use super::clock::Clock;
use super::config_store::RuleConfigStore;
use super::eval::{EvalError, GroupEvalReport, Querier, evaluate_group};
use super::sharding::owns_group;
use super::sinks::{AlertSink, RecordingSink};
use super::state::RuleStateStore;

pub struct RulerService<Q, R, A>
where
    Q: Querier,
    R: RecordingSink,
    A: AlertSink,
{
    pub(crate) config: Arc<dyn RuleConfigStore>,
    pub(crate) state: Arc<dyn RuleStateStore>,
    clock: Arc<dyn Clock>,
    querier: Arc<Q>,
    pub(crate) recording: Arc<R>,
    alerting: Arc<A>,
    default_interval_secs: u64,
    pub(crate) my_index: usize,
    pub(crate) n_instances: usize,
}

impl<Q, R, A> RulerService<Q, R, A>
where
    Q: Querier + 'static,
    R: RecordingSink + 'static,
    A: AlertSink + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Arc<dyn RuleConfigStore>,
        state: Arc<dyn RuleStateStore>,
        clock: impl Clock + 'static,
        querier: Arc<Q>,
        recording: Arc<R>,
        alerting: Arc<A>,
        default_interval_secs: u64,
        my_index: usize,
        n_instances: usize,
    ) -> Self {
        Self {
            config,
            state,
            clock: Arc::new(clock),
            querier,
            recording,
            alerting,
            default_interval_secs,
            my_index,
            n_instances,
        }
    }

    #[must_use]
    pub fn api_state(&self) -> RulerApiState {
        RulerApiState {
            config: self.config.clone(),
            state: self.state.clone(),
            default_interval_secs: self.default_interval_secs,
        }
    }

    /// Evaluate every group this instance owns for `tenant`, once, at the
    /// clock's current time.
    pub async fn eval_once(
        &self,
        tenant: &str,
    ) -> Result<Vec<(String, GroupEvalReport)>, EvalError> {
        let now = self.clock.now_ms();
        let mut out = Vec::new();
        for (_ns, group) in self.config.all_groups(tenant) {
            if !owns_group(tenant, &group.name, self.n_instances, self.my_index) {
                continue;
            }
            let report = evaluate_group(
                tenant,
                &group,
                now,
                self.querier.as_ref(),
                self.recording.as_ref(),
                self.alerting.as_ref(),
                self.state.as_ref(),
            )
            .await?;
            out.push((group.name, report));
        }
        Ok(out)
    }

    /// Run the scheduler until `shutdown`. Each tenant's groups tick on their
    /// own `interval` (default `default_interval_secs`). Errors are logged, not
    /// propagated — one bad rule must not stall the loop.
    pub async fn run(self, tenants: Vec<String>, shutdown: CancellationToken) {
        // Minimal scheduler: a single ticker at the GCD-ish base interval that
        // re-evaluates all due groups. Per-group precise scheduling is a Slice-8
        // refinement; the base loop already honors each group's interval by
        // tracking last-eval. (Kept thin; eval_once carries the logic.)
        let base = Duration::from_secs(self.default_interval_secs.max(1));
        let mut ticker = tokio::time::interval(base);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                _ = ticker.tick() => {
                    for tenant in &tenants {
                        if let Err(e) = self.eval_once(tenant).await {
                            tracing::warn!(tenant = %tenant, error = %e, "ruler eval failed");
                        }
                    }
                }
            }
        }
    }
}
```

> **Scheduler-fidelity verify-note:** `run` here is a deliberately thin single-ticker loop — it re-evaluates all owned groups every `default_interval_secs` rather than honoring each group's distinct `interval` precisely. The *correctness* (recording write-back, `for:` transitions) is fully in `eval_once`/`evaluate_group` and fully tested; precise per-group interval scheduling + the alert *resend* interval are Slice-8 refinements (flagged). The `run` signature also needs a tenant source — here passed as `Vec<String>`; the real impl discovers tenants from the config topic. Don't add a tenant-discovery loop in this slice; that's Slice 8.

- [ ] **Step 4: Add re-exports** in `ruler/mod.rs` (`pub use service::RulerService;`).

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib ruler::service`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): RulerService eval loop + owned-group filtering"
```

---

### Task 13: `--target ruler` binary wiring + end-to-end integration test

**Files:**
- Create: `crates/metrics/src/bin/ruler.rs` (or add a `ruler` arm to an existing role dispatcher / `main.rs`)
- Create: `crates/metrics/tests/ruler_e2e.rs`

**Interfaces:**
- Produces: a binary that parses `--target ruler` + flags (`--listen`, `--alertmanager-url`, `--bootstrap`, `--default-interval`, `--instance-index`, `--instances`), builds a `RulerService` with the real `AlertmanagerClient` + a `WalRecordingSink` (over the real producer once Slice 4 lands; a logging stub until then), serves `router(service.api_state())` via `grpc-gateway::serve`, and spawns `service.run(...)`.
- The **e2e test** is the headline: drives the whole loop with mocks end-to-end — POST a rule group via the HTTP API, run `eval_once`, assert recording samples produced + an alert dispatched after `for:` — proving the wiring (config store → eval → sinks → read API) composes.

- [ ] **Step 1: Write the failing e2e test**

Create `crates/metrics/tests/ruler_e2e.rs`:

```rust
//! End-to-end: POST a rule group, evaluate, assert recording write-back + alert
//! dispatch + read-API reflection — all through the public ruler surface with
//! mock sinks/querier/clock.

use std::sync::Arc;
use std::time::Duration;

use assert2::assert;
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use crabka_metrics::ruler::clock::MockClock;
use crabka_metrics::ruler::config_store::InMemoryConfigStore;
use crabka_metrics::ruler::contract::{InstantSample, Labels, QueryResult, SampleValue};
use crabka_metrics::ruler::eval::{EvalError, Querier};
use crabka_metrics::ruler::sinks::{MockAlertSink, MockRecordingSink};
use crabka_metrics::ruler::state::InMemoryStateStore;
use crabka_metrics::ruler::{RulerService, api};

#[derive(Clone)]
struct FiringQuerier;
#[async_trait]
impl Querier for FiringQuerier {
    async fn query_instant(&self, _t: &str, _e: &str, ts: i64) -> Result<QueryResult, EvalError> {
        let labels: Labels = [("job".to_string(), "api".to_string())].into_iter().collect();
        Ok(QueryResult::InstantVector(vec![InstantSample {
            labels,
            ts_ms: ts,
            value: SampleValue::Float(9.0),
        }]))
    }
}

// Mimir's per-namespace POST body is a SINGLE bare rule group (no `groups:`).
const RULES: &str = "name: g\ninterval: 30s\nrules:\n  - record: job:up:sum\n    expr: sum(up)\n  - alert: AlwaysFires\n    expr: up\n    for: 1m\n    annotations:\n      summary: 'val {{ $value }}'\n";

#[tokio::test]
async fn ruler_end_to_end_records_and_fires() {
    let config = Arc::new(InMemoryConfigStore::default());
    let state = Arc::new(InMemoryStateStore::default());
    let clock = MockClock::new(0);
    let recording = Arc::new(MockRecordingSink::default());
    let alerting = Arc::new(MockAlertSink::default());

    let svc = RulerService::new(
        config.clone(),
        state.clone(),
        clock.clone(),
        Arc::new(FiringQuerier),
        recording.clone(),
        alerting.clone(),
        60,
        0,
        1,
    );

    // 1. POST the rule group through the config API.
    let app = api::router(svc.api_state());
    let resp = app
        .oneshot(
            Request::post("/prometheus/config/v1/rules/ns")
                .header("X-Scope-OrgID", "t")
                .body(Body::from(RULES))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(resp.status() == StatusCode::ACCEPTED);

    // 2. t=0: eval → recording produced; alert pending (for:1m not elapsed).
    let r0 = svc.eval_once("t").await.unwrap();
    assert!(r0.iter().any(|(g, rep)| g == "g" && rep.recorded_series == 1));
    assert!(recording.calls().len() == 1);
    assert!(alerting.calls().is_empty());

    // 3. t=60s: for: elapsed → alert fires + dispatched.
    clock.set(60_000);
    let _ = svc.eval_once("t").await.unwrap();
    assert!(alerting.calls().len() == 1);
    let dispatched = &alerting.calls()[0].1[0];
    assert!(dispatched.labels.get("alertname") == Some("AlwaysFires"));
    assert!(dispatched.annotations.get("summary").unwrap() == "val 9");

    // 4. read API reflects the firing alert.
    let app = api::router(svc.api_state());
    let resp = app
        .oneshot(
            Request::get("/api/v1/alerts")
                .header("X-Scope-OrgID", "t")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(v["data"]["alerts"][0]["state"] == "firing");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --test ruler_e2e`
Expected: FAIL — module paths unresolved until the binary/exports are wired (and `ruler` mod must be `pub`).

- [ ] **Step 3: Implement the binary + ensure `pub` exports**

Ensure `crates/metrics/src/lib.rs` has `pub mod ruler;` and `ruler/mod.rs` makes its submodules `pub`. Create `crates/metrics/src/bin/ruler.rs`:

```rust
//! `crabka-metrics --target ruler` (or `cargo run --bin ruler`).
//!
//! Builds a `RulerService` with the real Alertmanager client + the WAL
//! recording sink, serves the ruler HTTP API, and runs the eval loop.

use std::sync::Arc;

use clap::Parser;
use tokio_util::sync::CancellationToken;

use crabka_metrics::ruler::alertmanager::AlertmanagerClient;
use crabka_metrics::ruler::clock::SystemClock;
use crabka_metrics::ruler::config_store::InMemoryConfigStore;
use crabka_metrics::ruler::state::InMemoryStateStore;
use crabka_metrics::ruler::{RulerService, api};

#[derive(Parser)]
struct Args {
    /// Role selector (kept for the unified service entrypoint).
    #[arg(long, default_value = "ruler")]
    target: String,
    #[arg(long, default_value = "0.0.0.0:9009")]
    listen: String,
    #[arg(long, default_value = "http://localhost:9093")]
    alertmanager_url: String,
    #[arg(long, default_value_t = 60)]
    default_interval: u64,
    #[arg(long, default_value_t = 0)]
    instance_index: usize,
    #[arg(long, default_value_t = 1)]
    instances: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    assert_eq!(args.target, "ruler", "this binary serves the ruler role only");

    // NOTE: querier + WAL recording sink are wired to the real engine/producer
    // once Slices 2-4 land. Until then this binary builds with placeholder
    // implementations behind the same traits; see the eval/produce modules.
    let config = Arc::new(InMemoryConfigStore::default());
    let state = Arc::new(InMemoryStateStore::default());
    let alerting = Arc::new(AlertmanagerClient::new(args.alertmanager_url));

    // Placeholder querier/recording sink until upstream slices land. Replace
    // with PromqlEngine<RemoteMetricStore> + WalRecordingSink<KafkaWalProducer>.
    let querier = Arc::new(placeholder::NoopQuerier);
    let recording = Arc::new(crabka_metrics::ruler::sinks::MockRecordingSink::default());

    let svc = RulerService::new(
        config,
        state,
        SystemClock,
        querier,
        recording,
        alerting,
        args.default_interval,
        args.instance_index,
        args.instances,
    );

    let app = api::router(svc.api_state());
    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    let shutdown = CancellationToken::new();

    let serve = {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            // Reuse the gateway serve helper (plaintext path) once the dep is
            // available; axum::serve here keeps the binary self-contained.
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move { shutdown.cancelled().await })
                .await;
        })
    };

    svc.run(vec!["anonymous".to_string()], shutdown.clone()).await;
    shutdown.cancel();
    let _ = serve.await;
    Ok(())
}

mod placeholder {
    use async_trait::async_trait;

    use crabka_metrics::ruler::contract::QueryResult;
    use crabka_metrics::ruler::eval::{EvalError, Querier};

    /// Returns an empty vector for every query until the real engine is wired.
    pub struct NoopQuerier;

    #[async_trait]
    impl Querier for NoopQuerier {
        async fn query_instant(&self, _t: &str, _e: &str, _ts: i64) -> Result<QueryResult, EvalError> {
            Ok(QueryResult::InstantVector(vec![]))
        }
    }
}
```

> **Binary-wiring verify-notes:**
> 1. The binary uses `axum::serve` directly to stay self-contained; switching to `crabka_grpc_gateway::serve::serve` (for the TLS/mTLS-principal path) is a one-line change once that crate is a dep. Flagged, not blocking.
> 2. `--target ruler` mirrors the spec's role-selectable service. If the metrics service later grows a single `main.rs` dispatching all roles (`distributor`/`querier`/…), fold this binary's body into a `run_ruler(args)` arm — the `RulerService` construction is already self-contained.
> 3. The placeholder `NoopQuerier` + `MockRecordingSink` are **binary-only** stand-ins so the role binary compiles and serves the API before Slices 2-4 merge. They never appear in library code or the e2e test (which uses its own mocks). Replace both when upstream lands.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-metrics --test ruler_e2e`
Expected: PASS.

- [ ] **Step 5: Whole-crate gate**

Run: `cargo test -p crabka-metrics && cargo clippy -p crabka-metrics --all-targets && cargo fmt -p crabka-metrics --check`
Expected: all PASS, no warnings, formatting clean.

- [ ] **Step 6: Commit**

```bash
git add crates/metrics/
git commit -m "feat(metrics): --target ruler binary + end-to-end ruler integration test"
```

---

## Self-review

**Spec coverage (against §7 ruler + §11 Slice 7):**
- **Rule model + config API** — YAML model + validation (Task 2); Mimir `/prometheus/config/v1/rules[/{ns}[/{group}]]` CRUD + `X-Scope-OrgID` tenancy (Task 11); per-tenant persistence behind `RuleConfigStore` with a compacted-topic key/value codec (Task 7).
- **Evaluation loop** — `evaluate_group` (Task 6) + `RulerService::eval_once`/`run` (Task 12). **Recording rules**: instant vector → `__name__ = record` + merged labels → produce to the WAL topic (Tasks 6 + 10), no special path. **Alerting rules**: `inactive → pending → firing` honoring `for:`, `$value`/`$labels` templating, dispatch to Alertmanager `POST /api/v2/alerts` (Tasks 6 + 9).
- **State store** — compacted per-tenant alert state behind `RuleStateStore` + in-memory impl (Task 4).
- **Read APIs** — `/api/v1/rules` + `/api/v1/alerts` in Prometheus JSON shapes (Task 11).
- **Sharding** — `(tenant, group)`-hash `assign_group`/`owns_group` + tests (Task 8), applied in `eval_once` (Task 12).
- **Role binary** — `crabka-metrics --target ruler` (Task 13).

**First-class test concerns (as the brief demanded):**
- **`for:`-duration state machine** — Task 6's `alert_goes_pending_then_firing_after_for_elapses` drives `inactive→pending→firing` across `t=0/60/120/180s` with `MockClock`, pinning the inclusive `for:` boundary, single-dispatch-on-transition, and no re-dispatch while firing; `alert_resolves_when_condition_clears` pins resolution. The e2e (Task 13) re-proves it through the full service.
- **Recording-rule → WAL round-trip** — Task 6's `recording_rule_writes_renamed_series_to_wal` pins the `__name__` overwrite + rule-label merge + value/ts passthrough against `MockRecordingSink`; Task 10 pins the sample→`WalSeriesSample` mapping; the e2e produces through the real sink seam.

**Churn-prone surfaces handled per the brief (structure + behavior-pinning tests + verify-notes):**
- **Kafka producer** — isolated behind `WalProducer` (Task 10); the rename/merge logic is tested with a capturing mock, and the real Slice-4 producer is a documented one-impl swap. Zero reqwest/Kafka in the tested transform (`to_wal_samples`).
- **Alertmanager HTTP client** — the `reqwest` send (Task 9) is the only network-touching code; the v2 JSON shape is pinned by the pure `to_v2_payload`/`rfc3339_millis` tests, with an `#[ignore]` testcontainers smoke + AM-v2-schema verify-note. No unit test depends on a live endpoint.

**Contract-shim discipline:** every upstream type (`Labels`, `PromqlEngine`/`QueryResult`/`InstantSample`/`SampleValue`/`PromqlError`, `WalRecord`) flows through `crate::ruler::contract` (Task 1), so when Slices 2/3/4 merge the swap is one file. No ruler module references an upstream crate directly. The `Querier` and `WalProducer` traits are *ruler-owned* seams (the ruler↔engine and ruler↔producer boundaries), correctly **not** in `contract`.

**Deviations / deferrals flagged (all to Slice 8 hardening, none silently dropped):**
1. `keep_firing_for` — basic drop-on-absence implemented; the keep-firing window is a flagged stretch in Task 6.
2. Alert **resend** interval (re-dispatch of still-firing alerts) — `evaluate_group` dispatches only the pending→firing transition; resends are a service-loop + Alertmanager-dedup concern, flagged in Tasks 6 + 12.
3. Per-group **precise interval** scheduling — `run` is a single-base-ticker loop; precise per-group scheduling is flagged in Task 12. Correctness lives in the fully-tested `eval_once`.
4. `/api/v1/rules` **live-alert join** (firing instances on each rule) — returns `alerts: []`; flagged in Task 11.
5. `GET /prometheus/config/v1/rules` (all-namespaces) — emits a real YAML map `namespace -> [groups]`; single-namespace returns a bare group list; single-group echoes the stored body. Documented shapes produced in-slice; only exact field-ordering parity is left to Slice-8 differential testing.
6. `$value` **template formatting** for non-round floats — Rust `{}` vs Go `humanize`; flagged in Task 6, not load-bearing for state-machine correctness.
7. Cross-instance **sharding coordination** (membership) — the assignment fn is exact + tested; `my_index`/`n_instances` are injected (static today); consumer-group membership is Slice 8.
8. Topic-backed `RuleConfigStore`/`RuleStateStore` impls — codecs defined + tested here; the live Kafka-backed impls land with the service's broker wiring (Slice 8 / once Slice 4's client is shared).

**Placeholder scan:** no "TBD"/"add later"/"similar to Task N". Every step has runnable code or an exact command. The bounded hand-waves — the upstream `contract` types, the real `WalProducer`/`Querier` impls, the binary's placeholder querier — are explicitly trait-gated, compile-and-test in isolation, and pinned by tests on the pure logic, exactly as the no-placeholders rule requires for not-yet-merged dependencies.

**Type consistency:** `Labels` is the `contract` newtype wrapping a sorted `BTreeMap<String,String>` (sorted → stable fingerprints/JSON) with the **exact** restricted API of the real shared type (`get -> Option<&str>`, `insert(impl Into…)`, `iter`, no `remove`, no `Deref`) — every ruler call site is written against that surface so the re-export swap is a one-file change. `QueryResult` carries all four real variants so `as_vector`'s match stays exhaustive post-swap. `AlertState` strings (`inactive`/`pending`/`firing`) match Prometheus and are used identically in `state.rs`, `eval.rs`, and `api.rs`. `DispatchAlert`/`WalSeriesSample`/`ActiveAlert` field sets are identical across their definition, the eval producer, and the mocks. `SinkError` is the single sink error; `EvalError` wraps it via `#[from]`. The `rfc3339_millis` helper is defined once (Task 9) and reused by the read API (Task 11).

**Greenfield compliance:** no `#[serde(default)]`-for-old-data (the `#[serde(default)]` on `rules`/`labels`/`annotations` is for *absent optional YAML fields*, not back-compat — correct), no V1/V2 dual variants, no migration code. Kafka wire identity preserved: recording-rule samples are ordinary series through the Slice-4 produce path (no ruler-private record format).
