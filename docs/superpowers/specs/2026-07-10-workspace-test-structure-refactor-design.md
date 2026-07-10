# Workspace Test Structure Refactor Design

## Goal

Refactor tests in every workspace crate so structured results are compared as complete values instead of through chains of field assertions wherever meaningful equality exists, and repeated scenarios use table-driven or parameterized tests wherever their setup and action are structurally identical.

## Scope and invariants

- Audit every Rust test-bearing file under `crates/`, including unit, integration, generated-protocol, property, and model tests.
- Preserve tested behavior and production semantics. This is a test-structure refactor, not a change to runtime behavior.
- Prefer one equality assertion against an explicitly constructed expected struct, enum, tuple, or collection when a chain merely checks fields of one result.
- Retain focused assertions when whole-value equality is unavailable or misleading, when only an invariant should be tested, when values contain nondeterministic data, or when a specialized comparison supplies materially better diagnostics.
- Derive `PartialEq`/`Eq`/`Debug` only for private types when equality is semantically correct. Do not expand public production APIs solely to simplify tests.
- Combine scenarios into a case table or parameterized loop only when they share setup, action, and assertion shape. Keep separate tests when fixtures, concurrency, failure setup, or invariants differ materially.
- Preserve descriptive case identity by naming each case and including that name in assertion diagnostics.

## Audit strategy

Generate a repository-wide inventory of test functions and assertion sites, then review it crate by crate. The inventory flags adjacent assertions and repeated tests as candidates; it is not an automatic proof that a conversion is appropriate. Each candidate receives source review so that field chains, independent invariants, and staged assertions are distinguished correctly.

The work is partitioned by non-overlapping crate/file sets. Large crates (`protocol`, `broker`, and `client-streams`) receive dedicated batches; smaller crates are grouped into balanced batches. Every batch records which files were reviewed, which candidates were converted, and why any remaining assertion chains are legitimate.

## Validation

Each batch runs formatting and the affected crate's tests. After all batches, run workspace formatting checks, workspace tests, and a fresh full inventory. Completion requires:

1. Every crate and Rust test-bearing file appears in the audit ledger.
2. No candidate field-assertion chain remains without a documented reason.
3. Repeated same-shape scenarios are table-driven or parameterized, or documented as intentionally separate.
4. `cargo +nightly fmt --check` and `cargo test --workspace --no-fail-fast` pass, apart from explicitly recorded pre-existing or environment-dependent failures.
5. The final diff contains no production behavior changes unrelated to enabling semantically correct private-type comparisons.
