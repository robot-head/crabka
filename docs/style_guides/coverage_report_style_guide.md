# Test Coverage Report Style Guide

This guide defines the style and content expectations for per-crate `test_coverage_report.md` documents in Crabka.

## Purpose

Coverage reports capture the **current state of test verification** against Crabka's compatibility contract. They answer: "what is tested, how, and what remains?" They are for reviewers assessing Kafka-compatibility and engineers planning test work.

Coverage reports are **living documents** — updated when tests are added, not snapshots frozen in time. The date in the header reflects the last update.

## Crabka's Verification Model

Crabka has no formal requirements database; its "requirements" are the **Kafka compatibility contract** — wire-protocol byte exactness and KIP semantics. Verification therefore traces to two authoritative artifacts:

- The **feature-compatibility matrix** in the root [`README.md`](../../README.md#feature-compatibility) — differential-tested and authoritative.
- The [**KIP matrix**](../KIP_MATRIX.md) — per-KIP implementation status.

A coverage report shows, for the surface a crate owns, how that contract is verified: by unit tests, property tests (round-trip / invariants), byte-exact codec checks against `kafka-clients`, JVM **differential** tests against a live oracle, mutation testing, and line coverage.

## What Belongs in Coverage Reports

- **Compatibility coverage** — which KIPs / wire behaviours the crate owns are verified, and by which tests.
- **Test inventory** — what tests exist, where they are, what they cover.
- **Coverage metrics** — line coverage from `cargo llvm-cov`, and mutation results from `cargo mutants`.
- **Gap analysis** — what is not yet tested and why.
- **Cross-references** — tests in other crates (or the differential suite) that verify behaviour this crate owns.

## What Does NOT Belong in Coverage Reports

- **Design rationale** — how or why the code works; the design doc does that.
- **Bug reports or change history** — coverage reports track current state, not how we got here.
- **Aspirational targets** — report what IS covered, not what coverage SHOULD be.
- **Duplicated KIP descriptions** — link the KIP matrix; don't restate a KIP's semantics.

## Document Structure

Every coverage report should follow this structure. Sections may be omitted if genuinely not applicable (e.g., no fuzz targets), but the ordering should be preserved.

```markdown
# <crate-name> Test Coverage Report

| Document Info | Details |
| :--- | :--- |
| **Crate** | `crabka-<name>` |
| **Kafka surface** | <wire APIs / KIPs this crate owns> |
| **Date** | <YYYY-MM-DD of last update> |
```

### Section 1: Compatibility Coverage Summary

Lead with a one-line summary: "All owned KIPs verified (N differential, M unit)" or "N of M behaviours verified".

A table mapping each KIP / wire behaviour the crate owns to its verification status and the test that establishes it.

```markdown
| KIP / Behaviour | Feature | Result | Test | Matrix Ref |
| :--- | :--- | :--- | :--- | :--- |
| **KIP-848** | Consumer group heartbeat assignment | Pass | `assignment.rs::uniform_sticky` + differential `group_protocol` | [matrix](../../docs/KIP_MATRIX.md) |
| **Wire** | ApiVersions v3 byte exactness | Pass | `codec.rs::api_versions_roundtrip` + `kafka-clients` diff | README compat |
```

- **Every KIP / behaviour the crate owns must appear** — even if `N/A` or `Not tested`.
- **Result values**: `Pass` (test exists and passes), `Fail` (test exists and fails), `N/A` (not applicable to this crate), `Not tested` (no test exists).
- **Test column**: cite specific test function names (`file::function`), not just file paths. For differential coverage, name the differential suite / scenario.
- **Matrix Ref column**: link to the row in the [KIP matrix](../KIP_MATRIX.md) or the README compatibility matrix this traces to.
- **Cross-crate / differential coverage**: when the behaviour is verified elsewhere, say `Pass (differential)` or `Pass (crabka-broker)` and cite the specific test.

### Section 2: Test Inventory

List all tests, grouped by type. Include a total count per group.

- **Unit tests**: table with columns `Test Function | File | Scope`.
- **Property tests**: `proptest` / `datatest-stable` corpora — `Test Function | File | Property`.
- **Integration tests**: separate subsection, test file path and suite breakdown (note which are `#[ignore]`d and need `testcontainers` or the JVM oracle).
- **Differential tests**: the JVM-oracle scenarios this crate's behaviour is checked against.
- **Fuzz tests** (if any): `Target | File | Status`.

When a crate has no unit tests, state "No unit tests are implemented" and explain why (thin wrapper, logic verified end-to-end by differential tests, or backend-specific scenarios that would only test a third-party library's semantics). Don't manufacture coverage that isn't there.

### Section 3: Coverage vs Scope

A table cross-referencing the crate's intended scope (from its design doc / KIP surface) against implementation status:

```markdown
| Area | Scenario | Planned | Implemented | Status |
```

- **Status**: `Complete`, `N/M remaining`, or `Delegated to <crate>`.
- **Delegated tests**: when a scenario is covered by another crate or the differential suite, use `—` for counts and explain in Status. Adjust totals accordingly.
- **Include a total row** with a percentage.

### Section 4: Line Coverage

Crabka measures line coverage with `cargo llvm-cov` over the `nextest` runner:

```bash
cargo llvm-cov nextest --package <crate> --profile ci --lib --bins --lcov --output-path lcov.info
lcov --summary lcov.info
```

For crates whose behaviour is largely verified by ignored integration/differential tests, add the integration run:

```bash
cargo llvm-cov nextest --package <crate> --profile ci --test integration --lcov --output-path integration.lcov -- --ignored --nocapture
```

- **Quote the summary output** in a code block.
- **Provide a per-file breakdown** as a table: `File | Covered | Total | Coverage | Notes`.
- **Sort by coverage descending** — readers see strengths first, gaps last.
- **Note what the numbers exclude**: "unit tests only" or "includes ignored integration tests".
- **Explain anomalies**: generated codec code inflating counts, `Display` impls at 0%, generic monomorphisation, etc.

If line coverage has not yet been measured, include the command block so the reader can run it. Don't say "has not been run" — provide the command and leave space for results. If line coverage is genuinely not applicable (thin wrapper verified entirely by an external harness), state why and omit the section.

### Section 5: Mutation Coverage

Crabka gates on **mutation testing** (`cargo mutants`), which is a stronger signal than line coverage: a surviving mutant means a line runs but no test asserts on its behaviour.

```bash
git diff origin/main...HEAD | tee git.diff
cargo mutants --in-diff git.diff
```

- Report **surviving mutants** as gaps — each is a line whose behaviour no test pins down.
- CI runs `--in-diff` on changed lines only; a report may additionally note a full-crate run if one was done.
- A crate with zero surviving mutants over its changed surface is the target state; say so when true.

### Section 6: Test Infrastructure

Brief description of how tests are structured. Optional for simple crates. Cover, as applicable:

- Test helpers / fixtures and where they live.
- `assert2` (`assert!` / `check!`) for assertions — the workspace standard.
- `mockall` trait mocks for isolating IO-decision logic.
- `stateright` model-checking for consensus-correctness properties.
- `testcontainers` and the JVM differential oracle for interop tests, and how to run them (`--include-ignored`).
- Runtime requirements (e.g., `multi_thread` for concurrent tests).

### Section 7: Key Gaps

A table of remaining coverage gaps:

```markdown
| Area | Gap | Severity | Notes |
```

- **Only list genuine gaps** — not items covered by another crate or the differential suite.
- **Severity**: High (a compatibility behaviour is unverified), Medium (a significant code path is untested), Low (edge case or defence-in-depth).
- **If no gaps remain**, state it explicitly: "All owned behaviours verified. No significant gaps remain."

### Section 8: Conclusion

A single paragraph summarising:

- Total test count and scope-coverage percentage.
- Line coverage percentage and mutation-testing status.
- How many owned KIPs / behaviours are verified (the headline metric).
- Key strengths (what's well-tested).
- Primary remaining gaps (if any).
- Other test layers that contribute (differential, property, fuzz, CLI).

## Cross-Crate and Differential References

When a crate delegates verification elsewhere:

- **In the coverage summary**: use `Pass (differential)` or `Pass (<crate>)` and cite the specific test/scenario.
- **In Coverage vs Scope**: use `Delegated to <crate>` and adjust totals to show "in-scope" vs "total".
- **Don't double-count**: if a differential scenario or a test in crate A verifies behaviour owned by crate B, only crate B's report counts it as verified. Crate A notes it without inflating its own counts.

## Consistency Rules

- **Header table**: always use `| :--- | :--- |` alignment.
- **Date format**: `YYYY-MM-DD`.
- **Test function names**: use `file::function_name` format (e.g., `codec.rs::api_versions_roundtrip`).
- **KIP references**: bold the ID (e.g., `**KIP-848**`) and link to the KIP matrix.
- **Percentages**: one decimal place for line coverage (e.g., 78.2%), zero for scope coverage (e.g., 92%).
- **File paths**: relative to crate `src/` for source, relative to crate root for test files.

## Root-Level Coverage Report

The project-level report at `docs/test_coverage_report.md` summarises all crates. It should:

- List all crates with their coverage report and status.
- Aggregate test counts (unit, property, integration, differential, fuzz).
- Summarise Kafka-compatibility coverage at the KIP-matrix level.
- Identify cross-cutting gaps.
- Link to per-crate reports rather than duplicating their content.

## Requirements Traceability

Crabka's verification chain runs from the Kafka compatibility contract to individual test results:

```
Kafka compatibility contract
  (wire byte-exactness + KIP semantics)
        │
  KIP Matrix / README compatibility matrix   ← authoritative status
        │
        ├── byte-exact codec checks vs `kafka-clients`
        ├── JVM differential tests vs live cp-kafka / apache-kafka oracle
        ├── property tests (round-trip / invariants) + fuzz
        └── unit / integration tests
              │
              └── Individual test functions → Pass/Fail
                    (mutation-verified, line-coverage measured)
```

Each coverage report's Section 1 table closes the loop from the KIP matrix to individual test results; the `Matrix Ref` column makes that traceability explicit. The root-level report aggregates these into a KIP-matrix-level view.

## Questions to Ask When Writing

1. Is every KIP / wire behaviour the crate owns accounted for in the summary table?
2. Does the test inventory match what `cargo nextest run` actually runs (including the ignored integration/differential tests)?
3. Are cross-crate and differential references accurate and up to date?
4. Would a reviewer know exactly what is and isn't verified against Kafka?
5. Are the line-coverage numbers current, and are surviving mutants reported as gaps?
