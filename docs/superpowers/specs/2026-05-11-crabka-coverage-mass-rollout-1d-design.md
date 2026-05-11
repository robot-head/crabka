# Mass rollout (sub-plan 1d) — Design

**Status:** Draft for review
**Date:** 2026-05-11
**Author:** Matthew Stone (with Claude)
**Predecessors:** coverage meta-spec
(`2026-05-11-crabka-protocol-coverage-design.md`); 1a (codegen
generalization, merged); 1b (compression, merged); 1c (typed
RecordBatch, merged).

## Summary

Switch `crabka-protocol-codegen`'s message gate from the 6-pair
representative set to **every active Kafka 4.2 schema** (~190 messages),
generate the corresponding owned + borrowed wrappers, and exercise every
`(api_key, version)` pair via the JVM oracle. After 1d ships, every
message Kafka 4.2 defines is fully typed in `crabka-protocol` and
byte-equal with `kafka-clients` 4.2.0.

The emitter is already capability-complete (proven by
`every_vendored_schema_emits_clean` in 1a Task 16). 1d's work is
**iterative validation and edge-case fixing** rather than new
architecture.

## North star (acceptance gate for sub-plan 1d)

1. The codegen gate emits every active schema (~190 messages — every
   schema with non-empty `validVersions`).
2. Owned + borrowed wrapper modules exist for every active schema,
   either as committed `include!` stubs or — if we factor the wrapper
   generation — as drift-checked generated artifacts.
3. New `differential_all.rs` test passes one default-fixture
   byte-equality assertion per `(api_key, version)` pair against the
   JVM oracle on PR CI.
4. Existing per-message differential test files
   (`differential_api_versions.rs`, `differential_metadata.rs`,
   `differential_produce.rs`, `differential_offset_commit.rs`,
   `differential_describe_groups.rs`, `differential_records.rs`)
   continue to pass — no regressions.
5. RequestHeader / ResponseHeader move from `KNOWN_ISSUES.md` to live
   differential coverage. The oracle gains `header_encode` /
   `header_decode` ops.
6. Nightly workflow `.github/workflows/nightly-differential.yml` runs
   the 256-proptest-per-pair sweep daily.
7. **Hard-fail policy:** no pair is `#[ignore]`'d for known-failure
   reasons. Every active pair passes differential on PR CI.

## Non-goals

- **Captured-traffic corpus growth.** See "Carve-out" below.
- **Performance optimisation.** Decode and encode hot paths use what
  1a/1c built; further tuning is a future maintenance task.
- **Streaming or async APIs.** Wire codec stays sync, buffer-at-a-time.
- **New oracle ops beyond `header_encode` / `header_decode`.** Existing
  ops cover all request/response messages via `MessageDataJsonConverter`.

## Carve-out from coverage meta-spec acceptance criterion #9

The coverage meta-spec said: "Captured-traffic corpus has at least one
entry per `(api_key, version)` pair that is realistically capturable."

**1d does not build the corpus.** Differential testing (default-fixture
per pair on PR CI; 256 proptest per pair nightly) is the substitute.
Rationale: building ~1000 corpus entries via either real broker captures
(high setup cost) or oracle-synthetic generation (which proves nothing
more than differential testing already does) is not worth the work for
the validation value it adds.

The corpus remains a useful regression-reproduction tool when bugs
surface in the wild. Growth is deferred to a future maintenance task and
documented in `KNOWN_ISSUES.md`.

---

# 1. Scope and rollout shape

### What "active" means

Schemas with non-empty `validVersions`. The deprecated set
(`ControlledShutdownRequest`, `LeaderAndIsrRequest`, `StopReplicaRequest`,
and any similar pre-KRaft schemas) is skipped.

The IR loader (`crabka_protocol_codegen::ir::load_dir`) reads all
schemas; the `every_vendored_schema_emits_clean` test confirms the
emitter handles every one of them. The active count is the
`emits_clean` count.

### Rollout shape: one CURATED flip, then iterate

1. Replace the `CURATED` slice with a gate that includes **every active
   schema**. Concretely: in `crates/protocol-codegen/src/main.rs`,
   change `if !CURATED.contains(&s.name.as_str()) { continue; }` to
   `if s.valid_versions.is_empty() { continue; }`.

2. Regenerate. Expect any of the following per-schema failures, fix
   each at the source:

   - **Wrapper missing** → wrappers under `crates/protocol/src/owned/`
     and `crates/protocol/src/borrowed/` don't exist yet. Create or,
     better, generate.
   - **`mod.rs` missing** → the generated wrapper modules aren't
     declared. Update.
   - **Emitter bug** → a schema shape the curated set didn't exercise
     fails to compile or fails byte equality. Fix in the codegen.

3. Once builds + tests + differential pass for every active schema,
   ship.

### Wrapper generation (recommended)

Hand-writing 190+ wrappers (`include!` + `#![allow(...)]` + two inline
tests each) is mechanical drudgery. Make wrappers a generated artifact:

- Add a `wrappers/` emit step to the codegen bin.
- Each wrapper is `crates/protocol/src/{owned,borrowed}/<snake>.rs` with
  a stable banner, `include!`, allow block, and two minimal round-trip
  tests.
- Drift-check via the existing `drift` workflow.

`mod.rs` files also become generated:
`crates/protocol/src/{owned,borrowed}/mod.rs` lists every active module
alphabetically, drift-checked.

### KNOWN_ISSUES.md resolution

1a's deferred entry for header differential testing is resolved in 1d.
The oracle gains `header_encode` / `header_decode` ops (Section 2);
`differential_all` covers `RequestHeader` and `ResponseHeader` via these
ops. Remove the entry from `KNOWN_ISSUES.md`.

---

# 2. Differential testing at scale

About **190 messages × ~5 versions average ≈ 1000 `(api_key, version)`
pairs**. Two-tier budget keeps PR CI fast while keeping the nightly
safety net real.

### PR CI: one default-fixture case per pair

For each `(api_key, version)`:
- Build the typed struct's `Default::default()`.
- Encode in Rust.
- Send the equivalent JSON (oracle's `MessageDataJsonConverter` accepts
  default-state JSON) to the oracle's existing `encode` op.
- Assert byte equality.

Wall-clock budget: ~30 seconds for the full sweep (JVM warmup + ~1000
oracle calls at ~200 µs each via the long-lived subprocess). Comfortably
within the existing `jvm-differential` job's budget.

### Nightly: 256 proptest cases per pair

Same shape but with arbitrary fixtures via the existing `Arbitrary`
impls. Roughly 250,000 cases at ~50 ms each ≈ 3 hours. Runs on
GitHub Actions' free Linux runner overnight; fail loudly if any pair
regresses.

### Test file organization

Rather than 190 separate `differential_<message>.rs` files (which would
create 190 separate cargo-test binaries — slow link time, slow CI),
consolidate into one parameterised file:

```
crates/protocol/tests/differential_all.rs
```

The test cases are themselves generated. A `build.rs` step (or a
codegen-emitted module the test `include!`s) materialises a `CASES`
table — `(message_name, api_key, version, is_request)` tuples — and a
dispatch shim mapping each name to its typed default + encode call.

The dispatch shim is the more interesting piece. It looks like:

```rust
// Generated from the schemas at build time.
pub fn encode_default(name: &str, api_key: i16, version: i16) -> Vec<u8> {
    match name {
        "ApiVersionsRequest"  => encode_one::<owned::api_versions_request::ApiVersionsRequest>(version),
        "MetadataRequest"     => encode_one::<owned::metadata_request::MetadataRequest>(version),
        // ... 190+ arms ...
        _ => panic!("unknown message: {name}"),
    }
}
```

Existing per-message differential files **stay** — they exercise
hand-crafted fixtures and span multiple versions explicitly. The new
`differential_all.rs` is the catch-all parameterised sweep.

### Default-JSON alignment

The JVM oracle's `MessageDataJsonConverter` accepts JSON. For the Rust
default fixture to match what the JVM produces, the JSON we send must
mirror the Rust struct's `Default::default()` byte-for-byte. (Same
alignment issue surfaced in 1a Task 10's regression fix.)

**Approach:** the codegen emits a `pub fn default_json() -> serde_json::Value`
per message that produces exactly what the oracle should be told. The
function knows the schema's `default` annotations and uses them; both
Rust and JVM see identical defaults. The differential test passes
`MessageName::default_json()` to the oracle.

This eliminates the "fixtures drift from defaults" bug class once and
for all.

### Header type messages

`RequestHeader` and `ResponseHeader` need a separate oracle op (existing
`encode`/`decode` are `ApiKey`-indexed; headers don't have an apiKey).

Add to `tools/oracle/src/main/java/com/crabka/oracle/Oracle.java`:

```
{"op":"header_encode","kind":"request"|"response","version":<i>,"value":<JSON>} → {"hex":"..."}
{"op":"header_decode","kind":"request"|"response","version":<i>,"hex":"..."}    → {"value":<JSON>}
```

Use `org.apache.kafka.common.message.RequestHeaderData` and
`ResponseHeaderData` plus their generated `JsonConverter` classes.

`differential_all` includes two extra cases per supported header version
(one for the request side, one for the response side) using these new
ops.

### Nightly workflow

```
# .github/workflows/nightly-differential.yml
name: nightly-differential
on:
  schedule:
    - cron: '0 3 * * *'   # 03:00 UTC daily
  workflow_dispatch:

jobs:
  nightly:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: dtolnay/rust-toolchain@stable
      - uses: actions/setup-java@v5
        with:
          distribution: temurin
          java-version: 17
      - run: (cd tools/oracle && ./gradlew installDist --no-daemon)
      - run: PROPTEST_CASES=256 cargo test --workspace --test differential_all --release -- --ignored
      - name: Create issue on failure
        if: failure()
        uses: actions/github-script@v7
        with:
          script: |
            await github.rest.issues.create({
              owner: context.repo.owner,
              repo: context.repo.repo,
              title: `nightly-differential failed on ${context.sha.slice(0,7)}`,
              body: `See workflow run: https://github.com/${context.repo.owner}/${context.repo.repo}/actions/runs/${context.runId}`,
              labels: ['nightly-fail'],
            });
```

---

# 3. Long-tail handling and hard-fail policy

### Failure handling workflow

When `differential_all` reports a byte mismatch on `(message, version)`:

1. **Print both hex dumps** plus the offset of the first divergent byte.
   The test harness's assertion must make this readable; replicate the
   style from `differential_produce.rs`.

2. **Diagnose by class:**
   - **Tagged-field default mismatch** (most common). Rust's
     `Default::default()` for a tagged field doesn't match the schema's
     `default`. Fix the emitter's manual `Default` impl. (Same shape as
     1a Task 10's fix-up.)
   - **Nullable-vs-empty mismatch on the wire.** A nullable array's null
     wire shape (`-1` non-flex, `0` compact) vs empty (`0` non-flex, `1`
     compact). Verify `nullableVersions` matches the emitter's branch
     logic.
   - **Field-order divergence.** Rare. Codegen emits in
     schema-declaration order; if it doesn't, that's a bug.
   - **Version-conditional gating off-by-one.** An `X-Y` range emitting
     at v=Y+1.
   - **CompactArray length wrong.** UVARINT(N+1) vs UVARINT(N) confusion.

3. **Fix at the source.** Emitter change → regenerate → re-run
   differential → repeat. Each fix is its own commit.

4. **Document only what truly can't be fixed.** `KNOWN_ISSUES.md` exists
   for legitimately unfixable carve-outs. Hard-fail policy says none of
   these should land in 1d. **If any do, escalate to the user** rather
   than silently accepting them.

### What "unfixable" might look like

- Schema declares a field type the oracle's `MessageDataJsonConverter`
  can't deserialize from input JSON (oracle gap, not Rust gap).
- A control batch with a magic value the oracle treats specially.
- Upstream Kafka bug: JVM emits non-spec-conforming bytes for a corner
  case. We match the JVM (user's hard-fail policy means byte-equality,
  not spec-equality).

None expected; escalate if any surface.

### Captured-traffic corpus carve-out

See the carve-out section at the top of this document. `KNOWN_ISSUES.md`
gets a stable entry documenting this deviation from coverage acceptance
criterion #9.

### CI lifecycle

- **PR CI:** existing `jvm-differential` job runs
  `cargo test --workspace -- --include-ignored`. After 1d, picks up
  `differential_all` automatically. Time budget: well within the
  existing 10 min cap.
- **Nightly:** new `nightly-differential.yml` workflow.
- **Drift check:** the existing `drift` workflow validates that wrappers
  and `mod.rs` files match what the codegen would produce. Whether
  wrappers move to `crates/protocol/generated/` or stay in `src/` with
  drift sub-check is an implementation-plan decision.

---

# 4. Slice-wide acceptance criteria

The sub-plan ships when **all** of the following hold:

### Coverage

1. `CURATED` (or its equivalent gate) emits every active schema (~190).
   Deprecated schemas (`validVersions: "none"`) explicitly skipped.
2. Owned + borrowed wrappers exist for every active schema. Each
   wrapper is `include!`'d, declares appropriate `#![allow(...)]`, and
   carries `min_version_roundtrips` + `max_version_roundtrips` tests.
3. `crates/protocol/src/owned/mod.rs` and
   `crates/protocol/src/borrowed/mod.rs` declare every active module.
   Drift-checked.

### Differential testing

4. New `differential_all.rs` runs one default-fixture byte-equality
   assertion per `(api_key, version)` pair against the JVM oracle on PR
   CI.
5. Existing per-message differential test files continue to pass — no
   regressions.
6. `RequestHeader` / `ResponseHeader` move from `KNOWN_ISSUES.md` to
   live differential coverage via new `header_encode` /
   `header_decode` oracle ops.
7. **Hard-fail:** no `(api_key, version)` pair is `#[ignore]`'d for
   known-failure reasons.
8. `KNOWN_ISSUES.md` ends 1d with the corpus carve-out section
   (criterion #9 deviation) and nothing else.

### Nightly

9. `.github/workflows/nightly-differential.yml` exists, runs the
   256-proptest budget, creates a `nightly-fail`-tagged issue on
   failure.

### Default-JSON alignment

10. Codegen emits `default_json()` per message; `differential_all`
    uses it for both sides of the comparison.

### CI

11. Existing `rust` matrix (Linux/macOS/Windows × 1.95.0) picks up the
    new generated code transparently.
12. Existing `jvm-differential` job runs `differential_all` within
    the current 10 min budget.
13. Existing `drift` workflow validates wrappers and `mod.rs` files.

### General

14. `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D
    warnings`, `cargo test --workspace -- --include-ignored` all green.
15. CI matrix green on Linux/macOS/Windows.
16. `cargo doc --no-deps -p crabka-protocol` passes with no warnings.
17. `KNOWN_ISSUES.md` documents the captured-traffic corpus deviation.

When all 17 items pass, 1d is done.

---

# 5. Open questions deferred to the implementation plan

- **Whether wrappers live in `crates/protocol/src/{owned,borrowed}/` or
  in `crates/protocol/generated/`** — the cleanest answer is "generated,
  drift-checked, with `include!` shims in `src/`" but that adds a layer
  of indirection. The plan picks based on what minimises churn.
- **`build.rs` vs codegen-emitted CASES table for `differential_all`** —
  either works; the plan picks one and justifies. Codegen-emitted is
  more consistent with existing patterns.
- **Whether to bisect failures** — if the initial CURATED flip surfaces
  many simultaneous failures, the plan may include a bisection step
  (e.g., turn on schemas in groups of 20, fix each group's issues
  before merging the next batch). This is a tactical decision the plan
  may invoke if needed; not part of the design.

None of these block the design.

---

# 6. Next step

Invoke `writing-plans` to produce a detailed implementation plan for
sub-plan 1d. Sub-plan 1e (0.1.0 publish) gets its own brainstorm →
plan cycle once 1d ships.
