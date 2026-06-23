# Loki Spike Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** De-risk the Loki logs wedge by proving the core label-index, Parquet/Arrow row shape, DataFusion query path, and Loki JSON result shape in a throwaway crate.

**Architecture:** Add `crabka-observability-spike` as a non-production workspace crate. It models Loki streams as label-set fingerprints plus sorted log rows, builds an inverted label index, returns Loki-compatible stream JSON for a small LogQL-shaped query, and ships an ignored example that writes/reads a Parquet block through DataFusion.

**Tech Stack:** Rust, DataFusion 54, DataFusion re-exported Arrow/Parquet, `xxhash-rust`, `serde_json`, `tempfile`.

---

### Task 1: Core Spike Behaviors

**Files:**
- Create: `crates/observability-spike/Cargo.toml`
- Create: `crates/observability-spike/src/lib.rs`
- Create: `crates/observability-spike/tests/core.rs`

- [ ] **Step 1: Write the failing tests**

Create tests for stable label fingerprints, inverted index pruning, and Loki stream JSON output:

```rust
use std::collections::BTreeMap;

use assert2::check;
use crabka_observability_spike::{
    LabelIndex, LogEntry, LogSelector, labels, loki_streams_response, series_fingerprint,
};
use serde_json::json;

#[test]
fn fingerprint_is_stable_for_label_order() {
    let a = labels([("app", "api"), ("env", "prod")]);
    let b = labels([("env", "prod"), ("app", "api")]);

    check!(series_fingerprint(&a) == series_fingerprint(&b));
}

#[test]
fn label_index_prunes_to_matching_series() {
    let mut index = LabelIndex::default();
    let api = labels([("app", "api"), ("env", "prod")]);
    let worker = labels([("app", "worker"), ("env", "prod")]);
    let api_fp = series_fingerprint(&api);

    index.insert_series(api.clone());
    index.insert_series(worker);

    let matched = index.match_series(&LogSelector::new(labels([("app", "api")])));

    check!(matched == [api_fp].into());
}

#[test]
fn loki_response_groups_lines_by_stream() {
    let entries = vec![
        LogEntry::new(10, labels([("app", "api")]), "ok"),
        LogEntry::new(20, labels([("app", "api")]), "error: boom"),
        LogEntry::new(15, labels([("app", "worker")]), "error: hidden"),
    ];

    let response = loki_streams_response(
        &entries,
        &LogSelector::new(labels([("app", "api")])).contains("error"),
        0,
        30,
    );

    check!(
        response
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {"app": "api"},
                            "values": [["20", "error: boom"]]
                        }
                    ]
                }
            })
    );
}
```

- [ ] **Step 2: Run tests and verify RED**

Run: `cargo test -p crabka-observability-spike --test core`

Expected: failure because `crabka_observability_spike` items are not implemented.

- [ ] **Step 3: Implement the minimal core**

Implement `labels`, `series_fingerprint`, `LabelIndex`, `LogSelector`, `LogEntry`, and `loki_streams_response` exactly enough for the tests.

- [ ] **Step 4: Run tests and verify GREEN**

Run: `cargo test -p crabka-observability-spike --test core`

Expected: all three tests pass.

### Task 2: DataFusion/Parquet Empirical Example

**Files:**
- Modify: `crates/observability-spike/src/lib.rs`
- Create: `crates/observability-spike/examples/loki_spike.rs`

- [ ] **Step 1: Add an ignored example**

Add an example that writes three log rows to a local Parquet file, registers it with DataFusion, runs a query equivalent to `{app="api"} |= "error"`, and prints the exact Loki stream JSON.

- [ ] **Step 2: Run the example**

Run: `cargo run -p crabka-observability-spike --example loki_spike`

Expected: prints a one-row Loki `streams` response and a short GO/NO-GO summary.

### Task 3: Findings

**Files:**
- Create: `docs/superpowers/specs/2026-06-18-loki-spike-findings.md`

- [ ] **Step 1: Document evidence**

Record what the spike proved, dependency versions, command output, and the recommended production plan changes.

- [ ] **Step 2: Verify**

Run:

```bash
cargo test -p crabka-observability-spike
cargo run -p crabka-observability-spike --example loki_spike
cargo fmt --check
```

Expected: tests and example pass, formatting is clean.
