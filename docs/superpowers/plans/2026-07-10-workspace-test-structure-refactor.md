# Workspace Test Structure Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor tests in every workspace crate to use complete structured-value comparisons and table-driven or parameterized cases wherever those forms preserve the intended behavior more clearly.

**Architecture:** A generated audit ledger enumerates every Rust test-bearing file. Disjoint crate and source-root batches review every ledger entry, convert eligible assertion chains and repeated scenarios, record justified exceptions, and run affected crate tests before a final workspace-wide audit.

**Tech Stack:** Rust 2024, Cargo workspace, `assert2`, Tokio tests, shell-based audit inventory.

## Global Constraints

- Audit every Rust test-bearing file under `crates/`, including unit, integration, generated-protocol, property, and model tests.
- Preserve tested behavior and production semantics.
- Replace field-by-field assertions on one structured result with one equality assertion against an explicit expected value when meaningful equality exists.
- Keep focused assertions for independent invariants, nondeterministic fields, staged state transitions, or types without meaningful equality.
- Derive equality/debug traits only for private types where the traits are semantically correct; do not expand public APIs solely for test convenience.
- Parameterize only scenarios with the same setup, action, and assertion shape. Every case must have a descriptive name used in assertion diagnostics.
- Do not edit fixtures, snapshots, generated schemas, or production behavior unless required by a semantically correct private-type derive.
- Use `cargo +nightly fmt --all` after edits and test every affected package.

### Required transformation shapes

Whole-value comparison:

```rust
let actual = operation();
let expected = ResultRecord {
    id: 7,
    name: "alpha".to_owned(),
    enabled: true,
};
assert_eq!(actual, expected);
```

Table-driven comparison:

```rust
for (name, input, expected) in [
    ("zero", 0, Output::Empty),
    ("positive", 4, Output::Value(4)),
] {
    let actual = operation(input);
    assert_eq!(actual, expected, "case {name}");
}
```

Every batch report must list: files reviewed, conversions made, intentionally retained candidates with reasons, formatting command, test command, and result.

---

### Task 1: Build the exhaustive audit ledger

**Files:**
- Create: `.superpowers/sdd/test-refactor-audit.md`
- Read: every `crates/**/*.rs` file containing `#[test]` or `#[tokio::test]`

**Interfaces:**
- Produces: one checklist entry per test-bearing Rust file, grouped by crate, plus columns for whole-value review, parameterization review, disposition, and test evidence.

- [ ] **Step 1: Generate the canonical file list**

Run:

```bash
mkdir -p .superpowers/sdd
rg -l '#\[(tokio::)?test' -g '*.rs' crates | sort > .superpowers/sdd/test-refactor-files.txt
```

Expected: the file contains every Rust source file with a standard or Tokio test attribute.

- [ ] **Step 2: Create the ledger header and entries**

Create `.superpowers/sdd/test-refactor-audit.md` with the design/spec link, baseline file count, and one unchecked entry for every path using this shape:

```markdown
- [ ] `crates/example/src/lib.rs` — whole-value: pending; parameterization: pending; disposition: pending; tests: pending
```

- [ ] **Step 3: Cross-check coverage**

Run:

```bash
test "$(wc -l < .superpowers/sdd/test-refactor-files.txt)" -eq "$(rg -c '^- \[[ x]\] `crates/.+\.rs`' .superpowers/sdd/test-refactor-audit.md)"
```

Expected: exit 0.

### Task 2: Refactor protocol borrowed modules

**Files:**
- Modify: `crates/protocol/src/borrowed/**/*.rs`
- Update: matching entries in `.superpowers/sdd/test-refactor-audit.md`

**Interfaces:**
- Consumes: Task 1 ledger and Global Constraints.
- Produces: reviewed borrowed protocol tests with exact-value comparisons and case tables where eligible.

- [ ] Review every scoped ledger file and convert all eligible chains and repeated scenarios using the required shapes.
- [ ] Mark every scoped ledger entry complete with conversion or exception disposition.
- [ ] Run `cargo +nightly fmt --all` and `cargo test --manifest-path crates/protocol/Cargo.toml --no-fail-fast`; record results.

### Task 3: Refactor protocol owned modules

**Files:**
- Modify: `crates/protocol/src/owned/**/*.rs`
- Update: matching entries in `.superpowers/sdd/test-refactor-audit.md`

**Interfaces:**
- Consumes: Task 1 ledger and Global Constraints.
- Produces: reviewed owned protocol tests with exact-value comparisons and case tables where eligible.

- [ ] Review every scoped ledger file and convert all eligible chains and repeated scenarios using the required shapes.
- [ ] Mark every scoped ledger entry complete with conversion or exception disposition.
- [ ] Run `cargo +nightly fmt --all` and `cargo test --manifest-path crates/protocol/Cargo.toml --no-fail-fast`; record results.

### Task 4: Refactor remaining protocol and protocol-codegen tests

**Files:**
- Modify: `crates/protocol/src/*.rs`, `crates/protocol/src/kafka_3_6_2/**/*.rs`, `crates/protocol/src/legacy_compat/**/*.rs`, `crates/protocol/src/primitives/**/*.rs`, `crates/protocol/src/records/**/*.rs`, `crates/protocol/tests/**/*.rs`, `crates/protocol-codegen/**/*.rs`
- Update: matching ledger entries.

**Interfaces:**
- Consumes: Task 1 ledger and Global Constraints.
- Produces: complete coverage of protocol files outside Tasks 2 and 3.

- [ ] Review every scoped ledger file and convert all eligible chains and repeated scenarios.
- [ ] Mark every scoped ledger entry complete with conversion or exception disposition.
- [ ] Run `cargo +nightly fmt --all`, `cargo test --manifest-path crates/protocol/Cargo.toml --no-fail-fast`, and `cargo test --manifest-path crates/protocol-codegen/Cargo.toml --no-fail-fast`; record results.

### Task 5: Refactor broker handlers and transaction handlers

**Files:**
- Modify: `crates/broker/src/handlers/**/*.rs`, `crates/broker/src/txn/**/*.rs`
- Update: matching ledger entries.

**Interfaces:**
- Consumes: Task 1 ledger and Global Constraints.
- Produces: reviewed request-handler tests.

- [ ] Review every scoped ledger file and convert all eligible chains and repeated scenarios.
- [ ] Mark every scoped ledger entry complete with conversion or exception disposition.
- [ ] Run `cargo +nightly fmt --all` and `cargo test --manifest-path crates/broker/Cargo.toml --no-fail-fast`; record results.

### Task 6: Refactor broker coordinator and share-group subsystems

**Files:**
- Modify: `crates/broker/src/coordinator/**/*.rs`, `crates/broker/src/share_coordinator/**/*.rs`, `crates/broker/src/share_partition/**/*.rs`
- Update: matching ledger entries.

**Interfaces:**
- Consumes: Task 1 ledger and Global Constraints.
- Produces: reviewed coordinator/share tests.

- [ ] Review every scoped ledger file and convert all eligible chains and repeated scenarios.
- [ ] Mark every scoped ledger entry complete with conversion or exception disposition.
- [ ] Run `cargo +nightly fmt --all` and `cargo test --manifest-path crates/broker/Cargo.toml --no-fail-fast`; record results.

### Task 7: Refactor remaining broker tests

**Files:**
- Modify: all test-bearing `crates/broker/**/*.rs` files outside Tasks 5 and 6, including `crates/broker/tests/**/*.rs`
- Update: matching ledger entries.

**Interfaces:**
- Consumes: Task 1 ledger and Global Constraints.
- Produces: complete broker coverage.

- [ ] Review every remaining broker ledger file and convert all eligible chains and repeated scenarios.
- [ ] Mark every scoped ledger entry complete with conversion or exception disposition.
- [ ] Run `cargo +nightly fmt --all` and `cargo test --manifest-path crates/broker/Cargo.toml --no-fail-fast`; record results.

### Task 8: Refactor client-family tests

**Files:**
- Modify: `crates/client-admin/**/*.rs`, `crates/client-consumer/**/*.rs`, `crates/client-core/**/*.rs`, `crates/client-producer/**/*.rs`, `crates/client-streams/**/*.rs`
- Update: matching ledger entries.

**Interfaces:**
- Consumes: Task 1 ledger and Global Constraints.
- Produces: reviewed client tests across all client crates.

- [ ] Review every scoped ledger file and convert all eligible chains and repeated scenarios.
- [ ] Mark every scoped ledger entry complete with conversion or exception disposition.
- [ ] Run formatting, then test each of the five scoped manifests with `cargo test --manifest-path <manifest> --no-fail-fast`; record results.

### Task 9: Refactor operator and admin UI tests

**Files:**
- Modify: `crates/operator/**/*.rs`, `crates/admin-ui/**/*.rs`
- Update: matching ledger entries.

**Interfaces:**
- Consumes: Task 1 ledger and Global Constraints.
- Produces: reviewed operator and UI backend tests.

- [ ] Review every scoped ledger file and convert all eligible chains and repeated scenarios.
- [ ] Mark every scoped ledger entry complete with conversion or exception disposition.
- [ ] Run formatting and both scoped manifest test suites; record results.

### Task 10: Refactor observability and query-engine tests

**Files:**
- Modify: `crates/observability/**/*.rs`, `crates/observability-demo-app/**/*.rs`, `crates/metrics/**/*.rs`, `crates/metrics-service/**/*.rs`, `crates/promql/**/*.rs`, `crates/profiles/**/*.rs`, `crates/pprof/**/*.rs`, `crates/traces/**/*.rs`, `crates/traceql/**/*.rs`, `crates/logql/**/*.rs`
- Update: matching ledger entries.

**Interfaces:**
- Consumes: Task 1 ledger and Global Constraints.
- Produces: reviewed telemetry/query tests.

- [ ] Review every scoped ledger file and convert all eligible chains and repeated scenarios.
- [ ] Mark every scoped ledger entry complete with conversion or exception disposition.
- [ ] Run formatting and every scoped manifest test suite; record results.

### Task 11: Refactor storage, log, and record tests

**Files:**
- Modify: `crates/blockstore/**/*.rs`, `crates/log/**/*.rs`, `crates/logfmt/**/*.rs`, `crates/log-iobench/**/*.rs`, `crates/remote-storage/**/*.rs`, `crates/remote-storage-topic/**/*.rs`, `crates/compression/**/*.rs`, `crates/records-legacy/**/*.rs`
- Update: matching ledger entries.

**Interfaces:**
- Consumes: Task 1 ledger and Global Constraints.
- Produces: reviewed storage and record tests.

- [ ] Review every scoped ledger file and convert all eligible chains and repeated scenarios.
- [ ] Mark every scoped ledger entry complete with conversion or exception disposition.
- [ ] Run formatting and every scoped manifest test suite; record results.

### Task 12: Refactor schema, connector, and gateway tests

**Files:**
- Modify: `crates/schema-registry/**/*.rs`, `crates/schema-serde/**/*.rs`, `crates/connect/**/*.rs`, `crates/connect-derive/**/*.rs`, `crates/connect-postgres/**/*.rs`, `crates/grpc-gateway/**/*.rs`
- Update: matching ledger entries.

**Interfaces:**
- Consumes: Task 1 ledger and Global Constraints.
- Produces: reviewed schema/connectivity tests.

- [ ] Review every scoped ledger file and convert all eligible chains and repeated scenarios.
- [ ] Mark every scoped ledger entry complete with conversion or exception disposition.
- [ ] Run formatting and every scoped manifest test suite; record results.

### Task 13: Refactor consensus, metadata, authorization, and security tests

**Files:**
- Modify: `crates/raft/**/*.rs`, `crates/rebalancer/**/*.rs`, `crates/metadata/**/*.rs`, `crates/kraft-core/**/*.rs`, `crates/voters/**/*.rs`, `crates/authz/**/*.rs`, `crates/security/**/*.rs`, `crates/audit/**/*.rs`, `crates/verified/**/*.rs`, `crates/throttle/**/*.rs`
- Update: matching ledger entries.

**Interfaces:**
- Consumes: Task 1 ledger and Global Constraints.
- Produces: reviewed consensus/security tests.

- [ ] Review every scoped ledger file and convert all eligible chains and repeated scenarios.
- [ ] Mark every scoped ledger entry complete with conversion or exception disposition.
- [ ] Run formatting and every scoped manifest test suite; record results.

### Task 14: Refactor remaining utility and integration tests

**Files:**
- Modify: `crates/bench-driver/**/*.rs`, `crates/cli/**/*.rs`, `crates/docgen/**/*.rs`, `crates/ids/**/*.rs`, `crates/integration-tests/**/*.rs`, `crates/kafka-tap/**/*.rs`, `crates/playground/**/*.rs`, `crates/replicator/**/*.rs`, `crates/telemetry/**/*.rs`
- Update: matching ledger entries.

**Interfaces:**
- Consumes: Task 1 ledger and Global Constraints.
- Produces: reviewed tests for every remaining workspace crate.

- [ ] Review every scoped ledger file and convert all eligible chains and repeated scenarios.
- [ ] Mark every scoped ledger entry complete with conversion or exception disposition.
- [ ] Run formatting and every scoped manifest test suite; record results.

### Task 15: Final coverage and workspace verification

**Files:**
- Review: `.superpowers/sdd/test-refactor-audit.md`
- Review: every changed Rust test file

**Interfaces:**
- Consumes: completed Tasks 1–14.
- Produces: evidence that all crates were audited and the workspace remains green.

- [ ] **Step 1: Prove ledger coverage and completion**

Run:

```bash
comm -3 \
  <(rg -l '#\[(tokio::)?test' -g '*.rs' crates | sort) \
  <(rg -o '`crates/[^`]+\.rs`' .superpowers/sdd/test-refactor-audit.md | tr -d '`' | sort)
rg -n '^- \[ \]|pending' .superpowers/sdd/test-refactor-audit.md
```

Expected: both commands produce no output.

- [ ] **Step 2: Review every retained candidate reason**

Confirm each exception identifies an observable reason from the Global Constraints rather than preference or time pressure.

- [ ] **Step 3: Run final formatting and tests**

Run:

```bash
cargo +nightly fmt --all --check
cargo test --workspace --no-fail-fast
git diff --check
```

Expected: all commands exit 0, except environment-dependent failures already present in the baseline must be reproduced and documented exactly.

- [ ] **Step 4: Inspect the final diff for scope**

Confirm all changes are test structure, audit documentation, project instructions, or semantically correct private trait derives; remove unrelated edits.
