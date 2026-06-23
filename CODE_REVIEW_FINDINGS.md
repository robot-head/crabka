# Code Review Findings — branch `claude/exciting-tu-3038c1`

Metrics / PromQL observability subsystem (Grafana-Mimir / Prometheus replacement).
307 commits, 119 files, +70,788 / −399 vs `main` (before fixes).

## Verification status (all fixes applied & verified)

- ✅ `cargo build --workspace --all-targets` — compiles
- ✅ `cargo clippy --workspace --all-targets -- -D warnings` — **zero** warnings
- ✅ `cargo nextest run` (promql, metrics, blockstore, throttle, metrics-service, profiles) — **943 passed, 0 failed** (5 `#[ignore]` Docker differential suites skipped locally; the Prometheus differential now gates on PR — see H15)
- ✅ `cargo +nightly fmt -- --check` — clean across all touched crates

Severity: 🔴 Critical · 🟠 High · 🟡 Medium · ⚪ Low
Status: ✅ Fixed & verified · ⚠️ Fixed w/ caveat · 📝 Deferred · ➖ No change (intentional)

---

## 🔴 Critical

### C1 — Hot/cold sample double-counting in range queries ✅
`crates/promql/src/merged_store.rs`. Cold+hot were UNIONed with no `(fingerprint, timestamp)` dedup; hot retention is time-based, independent of compaction, so a sample lives in both stores in steady state → `count_over_time`/`sum`/`rate` double-counted. **Fix:** dedup in `merge_scan_table` via `ROW_NUMBER() OVER (PARTITION BY fingerprint, timestamp ORDER BY __src DESC)` keeping the hot row; helper columns projected away so the output schema is unchanged. Regression test asserts a sample present in both stores is counted once.

### C2 — No max-resolution cap → unauthenticated DoS ✅
`engine.rs`, `http_api.rs`, `query_frontend.rs`. Per-step driver looped unbounded; no points cap. **Fix:** `MAX_RESOLUTION_POINTS = 11_000` + `check_resolution_points` enforced at the engine subquery/range backstop AND the HTTP/frontend front-gate, matching Prometheus' `(end-start)/step > 11000` rule (400 `bad_data`, byte-exact message), enforced even when per-tenant limits are unset. (Orchestrator aligned the engine backstop to the gate's interval-count threshold + `Plan`/400 status so a gate-admitted query is never re-rejected.)

### C3 — Snappy decompression bomb ✅
`metrics/src/wire/decoded.rs`, `remote_read.rs`. `decompress_vec` pre-allocated the header-declared size before the cap check. **Fix:** `snap::raw::decompress_len` pre-check before decompressing; `decode_read_request` gained a `max_output` param + `DEFAULT_MAX_READ_DECOMPRESSED` (32 MiB); `DefaultBodyLimit` added to the promql `/api/v1/read` router and the distributor push routes (M26).

---

## 🟠 High

- **H4** ✅ Fractional ingestion rate → unlimited. `limits/enforce.rs`: positive rate now clamps to ≥1; non-finite handled. Was a silent quota bypass for rates in (0, 0.5).
- **H5** ✅ Unbounded per-tenant token-bucket map. `limits/enforce.rs`: LRU eviction at a configurable cap (default 100k), no new dependency.
- **H6** ✅ HA-dedup election TOCTOU. `distributor/ha.rs`: `elect`/`elect_now` decide+commit the in-memory winner under one lock; durable Kafka persist stays async. Test: two concurrent first-seen elections → exactly one `Elect`.
- **H7** ✅ Native-histogram span/count validated at ingest. `wire/histogram.rs`: `sum(span.length) == counts.len()` (+ NHCB checks) in `v1/v2_histogram_to_native`, rejecting malformed series before the WAL.
- **H8** ✅ `label_replace` regex fully anchored (`^(?:…)$`), matching Prometheus.
- **H9** ✅ `min/max_over_time` NaN-ignoring fold (engine oracle + UDF in lockstep), matching Prometheus.
- **H10** ✅ `Duration::from_secs_f64` overflow guarded (`planner/mod.rs`) — huge finite durations return a parse error, no panic.
- **H11** ✅ Ruler `parse_duration_ms` multi-unit (`ms/s/m/h/d/w/y` + compounds); malformed/negative is a hard error, not a silent 0.
- **H12** ✅ Orphaned token-bucket model removed; KIP-73 stateright model ported to `crates/throttle` driving the live `plan_consume`; seqlock fixes the rate-change race. (Orchestrator reframed the green runs as bounded checks — caps are OOM guards per project policy — keeping the RED witness that proves violation-detection.)
- **H13** ✅ Unbounded snapshot/block load capped (`blockstore` index/profile_index/reader `head()`-then-reject).
- **H14** ✅ Tenant ID validation (Mimir `ValidTenantID`) in shared `crabka_metrics::validate_tenant`, called from HTTP + gRPC ingest and the query API. (Orchestrator removed a duplicate private copy in `http_api.rs` so there is a single source of truth.)
- **H15** ✅ Differential suites gate on PR. New path-filtered `metrics-differential-prometheus` job in `ci.yml`, wired into `gatekeeper-ci.needs`; full Mimir+Grafana matrix stays nightly.

---

## 🟡 Medium

- **M16** ✅ `stddev`/`stdvar` aggregate → Welford (no catastrophic cancellation). (Orchestrator also fixed the `approx_eq` test helper, which used an absolute `f64::EPSILON` too tight for a last-ULP compensated fold.)
- **M17** ✅ `avg`/`avg_over_time` → Kahan incremental mean (no ±Inf overflow).
- **M18** ⚠️ `quantile()`/`quantile_over_time` now return ±Inf/NaN + `InvalidQuantileWarning` for φ∉[0,1] instead of erroring — **matches Prometheus, but intentionally reverses the recent `49d9b2e1` "canonical quantile-phi error" decision.** Flagged for confirmation.
- **M19** ✅ `count_values` Inf formatting via the canonical Prometheus float formatter (`+Inf` not `inf`). Minor: very-large finite labels use exponent form vs Go's `'f'` — noted.
- **M20** ✅ Active-series check+insert made atomic (distributor holds the lock across both).
- **M21** ✅ Recording-rule output-collision detection (drops/errs per Prometheus).
- **M22** ✅ Recording rules apply the rule-level `labels:` map.
- **M23** ⚠️ Resolved-alert `EndsAt` emission + `keep_firing_for` parsing/honoring implemented **in-memory**. Durable persistence across a ruler restart is deferred (would add a field to the persisted `RulerAlertStateRecord` + replay; the in-memory deadline is lost on restart). Documented inline.
- **M24** ✅ `merged_store::min_present_time` Option-based emptiness (legit `min_time==0` preserved).
- **M25** ✅ Conformance oracle errors on duplicate `expect` series.
- **M26** ✅ Body-size limits on `/api/v1/read` and the distributor push routes.
- **M27** ✅ OTLP far-future timestamps rejected (not clamped) — prevents poisoning the out-of-order window.
- **M28** ✅ All-negative/empty-matching selectors rejected (`blockstore`), matching Prometheus' "≥1 non-empty matcher". (Orchestrator corrected the test, which miscategorized `=~".*bar.*"` — a restricting matcher that is correctly accepted.)

---

## ⚪ Low

- **L29** ✅ OTLP classic-histogram `cumulative` saturating add.
- **L30** ✅ Native-histogram span-index `checked_add` (drops malformed spans).
- **L31** ✅ `over_time` window-start uses saturating sub.
- **L32** ✅ `info()` precompiles data-label matcher regexes once.
- **L33** ✅ `blockstore::fingerprint` length-prefixed (injective; no `\n`/`=` collision).
- **L34** ✅ Compactor commit offset `saturating_add`.
- **L35** ✅ Compactor tenant path-escaper rejects `.`/`..` segments (defense in depth).
- **L36** ✅ Query-shard FNV sharding documented as internal-only (not Mimir-wire-compatible).
- **L37** ✅ metrics-service single shared shutdown token; server task joined (drains in-flight); critical background-task death fails loudly (`error!` + shutdown).
- **L38** ➖ Overrides `0`-disables-cap documented (matches Mimir; no behavior change).
- **L39** 📝 `time` dep workspace-hoist deferred (cosmetic; cross-crate churn/risk not worth it).
- **L40** ➖ `/status/runtimeinfo` CWD exposure unchanged (matches Prometheus).
- **L (store.rs schema validation)** 📝 Skipped — would change a cross-crate `ScanTableRequest` API for a redundant error relabel (DataFusion already errors on schema mismatch).

---

_Reviewed via `/engineering:code-review` of the full branch (9 parallel review agents → adversarial verification), fixed via 10 parallel implementer slices → 3 adversarial diff reviews, then orchestrator-verified: 2 must-fix corrections (C2 off-by-one, H14 duplicate), clippy/test/fmt to green (943 tests, 0 failures)._
