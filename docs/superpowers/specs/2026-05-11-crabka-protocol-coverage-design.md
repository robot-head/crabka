# crabka-protocol Coverage — Design

**Status:** Draft for review
**Date:** 2026-05-11
**Author:** Matthew Stone (with Claude)
**Predecessor:** [`2026-05-10-crabka-rust-rewrite-design.md`](2026-05-10-crabka-rust-rewrite-design.md) (project meta-spec) and [`2026-05-10-crabka-protocol-foundation.md`](../plans/2026-05-10-crabka-protocol-foundation.md) (foundation plan, now shipped).

## Summary

`crabka-protocol-coverage` takes `crabka-protocol` from "ApiVersions works,
nothing else does" to "every Kafka 4.2 message encodes and decodes
byte-equivalent to the JVM," adds a companion `crabka-compression` crate
covering the four wire codecs (gzip, snappy, lz4, zstd), introduces a
typed `RecordBatch` v2 decoder, and ships `0.1.0` of both crates to
crates.io.

This document is a **meta-spec** for the slice. It defines five
sub-plans, their dependencies, their acceptance gates, and the
cross-cutting decisions that apply to all of them. Each sub-plan
(1a–1e) gets its own brainstorm → spec → plan cycle when its turn comes.
This document does not contain those detailed plans.

What it does contain in detail is the design for **sub-plan 1a**
(codegen generalization), because 1a is the entry point and the work
that follows depends on its decisions about module layout, name
conversion, type mapping, and emitter abstractions.

## North star (acceptance gate for the slice)

1. All 197 vendored Kafka 4.2 schemas have generated owned + borrowed
   types in `crabka-protocol`.
2. Every supported `(api_key, version)` pair passes the three JVM-
   differential checks (JVM→Rust, Rust→JVM, byte-equality) at PR-CI
   budget (100 cases per pair) and nightly budget (10,000 cases per
   pair).
3. Every known tagged field is decoded into a typed Rust field.
   `unknown_tagged_fields` only carries tags absent from the schema.
4. `RECORDS` and `COMPACT_RECORDS` fields decode to a typed
   `RecordBatch` (v2) with compression handled via `crabka-compression`.
5. `crabka-protocol` 0.1.0 and `crabka-compression` 0.1.0 published to
   crates.io.
6. CI matrix green on Linux/macOS/Windows × Rust 1.95.0.

## Non-goals

- Pre-v2 record batches (v0/v1). Kafka 0.11+ writes only v2; legacy
  reads belong to `crabka-log` (project meta-spec slice 3).
- Stream processing (Streams) or connector framework (Connect). These
  are separate products in the project meta-spec.
- Public API stability past 0.1.0. Minor-version breaks are allowed
  pre-1.0 and documented in `CHANGELOG.md`.
- Performance benchmarks beyond ensuring tests run in reasonable time.

## Decomposition

Five sub-plans, ordered by dependency:

```
1a codegen generalization      ─┐
1b crabka-compression           ├── independent, parallelizable
1c typed RecordBatch in proto   ├── needs 1a + 1b
1d mass rollout + diff sweep    ├── needs 1a, 1c
1e 0.1.0 publish prep           └── needs 1d green
```

| # | Sub-plan | Crates touched | Done means |
|---|---|---|---|
| 1a | Codegen generalization | `crabka-protocol-codegen`, `crabka-protocol` (curated representative schemas) | Emitters handle every IR construct used by 4.2 schemas: arrays of primitives, arrays of structs, nested struct types, all 11 primitive types found in the schemas, every declared tagged field as a typed field. Snapshot tests pass for a curated 5–8 message set spanning all shapes; codegen IR validation accepts every 4.2 schema; mass rollout is *not* turned on yet. |
| 1b | `crabka-compression` | new `crabka-compression` crate | Pure-Rust where viable (flate2 with `rust_backend`, `snap`, `lz4_flex`; `zstd` C-backed). Encode + decode for gzip, snappy, lz4, zstd, each behind a default-enabled feature flag. Differential tests against the JVM `org.apache.kafka.common.utils.*` codecs via a Java sidecar. |
| 1c | Typed `RecordBatch` v2 | `crabka-protocol` | New `records` module with `RecordBatch` v2 (header fields, CRC-32C, attributes including compression, base offset/sequence, producer ID/epoch) and `Vec<Record>`. Eager decompression via 1b on decode, eager recompression on encode. JVM-differential per compression codec. |
| 1d | Mass rollout | `crabka-protocol`, `crabka-protocol-codegen` | All 197 schemas turned on. Every `(api_key, version)` pair passes the three diff checks at PR-CI budget. Captured-traffic corpus grows to at least one entry per realistically capturable pair (synthetic OK with the existing `synthetic = true` flag). `KNOWN_ISSUES.md` enumerates any deliberate exclusions with rationale. |
| 1e | 0.1.0 publish | `crabka-protocol`, `crabka-compression` | crates.io metadata, `cargo deny` clean, `cargo semver-checks` set up, `cargo publish --dry-run` clean, `CHANGELOG.md` with `[0.1.0]` entry, docs.rs builds clean for both, GitHub `v0.1.0` release tagged. Both crates installable via `cargo add`. |

Each sub-plan gets its own brainstorm → plan → execute cycle. This
document only details 1a below.

---

# Sub-plan 1a detailed design: codegen generalization

## Purpose

Replace the foundation's static-string emitter (which special-cases
`ApiVersionsRequest`) with a real IR-walking generator that emits a
complete, idiomatic owned + borrowed type for any 4.2 schema. 1a
proves the emitter against a curated representative set. Mass rollout
to all 197 schemas is **1d**'s problem.

## What the emitter has to produce per message

For each `MessageSpec`, walk the fields tree and emit:

1. A typed struct with each schema field as a Rust field, version-gated.
2. `impl Encode` that writes fields whose `versions` range contains the
   encode version, in declaration order, switching between compact
   (flexible) and non-compact (non-flexible) primitives based on
   `flexibleVersions`.
3. `impl Decode` / `impl DecodeBorrow` with the inverse logic.
4. A typed tagged-fields trailer. Every schema-declared tagged field
   becomes a `pub` Rust field that the encoder lifts into the trailer
   at its declared tag and the decoder pulls back out via the `known`
   callback. Unknown tags still flow to `unknown_tagged_fields`.
5. Constants: `API_KEY` (for request/response specs), `MIN_VERSION`,
   `MAX_VERSION`, `FLEXIBLE_MIN`.
6. A central `ApiKey` enum with one variant per request/response pair,
   generated once from the full message set. Gives downstream crates
   one place to dispatch from.

## Field-type mapping

| Schema type | Owned | Borrowed |
|---|---|---|
| `bool`, `int8`, `int16`, `int32`, `int64`, `uint16`, `float64` | the same Rust primitive | same |
| `string` | `String` (or `Option<String>` if nullable) | `&'a str` / `Option<&'a str>` |
| `bytes` | `bytes::Bytes` / `Option<Bytes>` | `&'a [u8]` / `Option<&'a [u8]>` |
| `uuid` | `Uuid` newtype (defined in `primitives::uuid`) | same |
| `records` | `bytes::Bytes` in 1a (opaque); 1c replaces with typed `RecordBatch` | `&'a [u8]` in 1a |
| `[]<elem>` | `Vec<T>` (or `Option<Vec<T>>` if nullable) | `Vec<T<'a>>` (outer `Vec` owned; entries borrow) |
| `Struct` (PascalCase) | reference to the generated struct | reference to the generated `<'a>` struct |

**Borrowed arrays of structs intentionally own the outer `Vec`.** A
true zero-copy decode would require the wire layout to match the
in-memory layout, which Kafka's variable-width encoding precludes.
The rustdoc on the borrowed flavor will document this. This matches
how `prost`, `tonic`, and `serde` handle the same situation.

## Module / file layout

```
crates/protocol/src/
├── api_key.rs                              # generated, single enum
├── owned/
│   ├── mod.rs                              # re-exports + module declarations
│   ├── api_versions_request.rs             # exists; include! wrapper
│   ├── produce_request.rs                  # generated, include! wrapper
│   ├── ... (one file per message)
│   └── common/                             # for top-level commonStructs
│       ├── mod.rs
│       └── produce_request_topic.rs        # e.g.
├── borrowed/
│   └── (mirror of owned/)
└── generated/                              # actual generated source
    ├── owned/*.rs
    ├── borrowed/*.rs
    ├── api_key.rs
    └── common/*.rs
```

- **One file per message.** ~200 files total. Cargo and rust-analyzer
  handle this without complaint.
- **Nested anonymous structs** (defined inline in a field's `fields:`)
  emit as sibling types in the same file as their parent.
- **`commonStructs`** (top-level reusable types declared on the parent
  spec) emit under `common/`, named `<parent>_<struct>` (snake_case) to
  avoid collisions where multiple messages declare a `Topic` struct.
- Each generated `.rs` lives in `crates/protocol/generated/<flavor>/`
  and is `include!`'d from the matching `crates/protocol/src/<flavor>/`
  wrapper. Wrapper files contain only inline tests (no logic).

## Name conversion

- **Message names:** keep PascalCase as the Rust type name; convert to
  snake_case for the module file name. `ApiVersionsRequest` →
  `api_versions_request.rs` containing `pub struct ApiVersionsRequest`.
- **Field names:** convert Kafka's camelCase JSON keys to Rust
  snake_case. `errorCode` → `error_code`, `apiKeys` → `api_keys`,
  `aclEntries` → `acl_entries`.
- **Reserved keywords:** suffix with `_`. `type` → `type_`, `match` →
  `match_`. Rustdoc on the field notes the original schema name.

## What 1a does NOT do

- Does not turn on generation for all 197 schemas. The generator-bin
  takes a `--message <Name>` filter (repeatable) and only emits for the
  named messages. Initial list lives in `tools/regenerate.sh`.
- Does not implement the typed `RecordBatch` decoder. `records` fields
  remain `Bytes` until 1c lands.
- Does not add compression. That's 1b.
- Does not generate `serde::Serialize` / `Deserialize`. This crate is a
  binary codec; anyone needing JSON uses the JVM-oracle pattern.

## Test strategy for 1a

Three layers, all on a curated representative set of messages — not the
full 197:

1. **Snapshot tests** on the codegen output for each representative
   message, owned + borrowed.
2. **Inline round-trip tests** in each generated wrapper, version by
   version.
3. **JVM-differential tests** for each representative message, version
   by version, all three checks.

Recommended representative set (one per shape category):

- `ApiVersionsRequest` / `ApiVersionsResponse` — regression check
  against foundation.
- `MetadataRequest` / `MetadataResponse` — arrays of structs, nullable
  fields, multiple versions across a long range.
- `ProduceRequest` / `ProduceResponse` — `records` primitive (opaque
  bytes until 1c), nested arrays of structs.
- `OffsetCommitRequest` / `OffsetCommitResponse` — many declared
  tagged fields, deep nesting.
- `RequestHeader` / `ResponseHeader` — `Header`-type schemas (not
  request/response), used by the framing helpers.

If a needed construct doesn't appear in this set, add a message that
exercises it (e.g., a schema with a `uuid` field if none of the above
have one). The set is "tested in 1a"; the rest are tested in 1d.

## Acceptance criteria for 1a

The sub-plan ships when **all** of these hold:

1. `cargo run -p crabka-protocol-codegen -- ...` with the curated
   message list emits compiling Rust source for every name in the list,
   owned + borrowed.
2. Snapshot tests for the curated set pass; `UPDATE_SNAPSHOTS=1` is
   needed only when the emitter intentionally changes output.
3. Inline round-trip tests for the curated set pass at every supported
   version.
4. JVM-differential tests for the curated set pass on all three checks
   per version, with the proptest budget that the foundation uses.
5. IR validator accepts all 197 vendored 4.2 schemas (i.e., the
   emitter could in principle be asked to generate any of them; we
   just choose not to until 1d).
6. `ApiKey` enum generated and re-exported from `crabka-protocol`'s
   crate root, listing every (request, response) pair in the 4.2
   schemas with their version ranges in rustdoc.
7. `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test
   --workspace` all green on the CI matrix.

---

# Cross-cutting decisions

These apply to the slice as a whole; surfacing here so they are not
re-litigated per sub-plan.

## `crabka-compression` crate boundary

- **Separate crate, not a feature of `crabka-protocol`.** Compression
  has a real C-dependency risk (zstd). Isolation keeps the protocol
  crate light and lets downstream consumers swap codecs.
- **Per-codec features.** Default features enable all four (`gzip`,
  `snappy`, `lz4`, `zstd`); each can be disabled individually.
- **Pure-Rust where viable.** gzip via `flate2` with `rust_backend`
  feature, snappy via `snap`, lz4 via `lz4_flex`, zstd via `zstd`
  (links C `libzstd`; no mature pure-Rust alternative exists).
- **Trait surface.** A single `Compression` trait with `compress(&self,
  &[u8]) -> Bytes` and `decompress(&self, &[u8]) -> Result<Bytes,
  _>`, plus a `CompressionType` enum (`None`, `Gzip`, `Snappy`,
  `Lz4`, `Zstd`) matching Kafka's wire bit values for record-batch
  attributes.

## Typed `RecordBatch` shape (1c)

- **v2 only.** Reading legacy v0/v1 batches is a `crabka-log` (slice
  3) concern.
- **CRC validation on decode, regeneration on encode.** Kafka uses
  CRC-32C (Castagnoli) for v2; the `crc32c` crate provides it.
- **Decompression is eager.** Decoding a `RecordBatch` returns a
  `Vec<Record>` with payloads already decompressed; encoding
  recompresses based on the `compression` field. Consumers wanting a
  lazy view can stay at the wire-level `Bytes` (we keep both
  available: the typed view via `RecordBatch::decode`, the opaque
  view as the field's encoded bytes).
- **Producer state preserved verbatim.** Producer ID, epoch, base
  sequence, including the sentinel `-1` for non-transactional,
  non-idempotent producers.
- **Differential testing.** JVM oracle gains a `decode_records` op
  exposing Kafka's `RecordBatch.Builder` round-trip. Tested per
  compression codec.

## Conformance budget (1d)

- **Nightly:** 10,000 differential cases per `(api_key, version)`
  pair, seeded.
- **Per-PR:** 100 cases per pair. ~100 ms per case → ~3 minutes per
  full sweep; acceptable for PR CI.
- **Failure handling:** every diff failure must be reproducible from
  the printed seed. CI prints seed and a hex diff. No flaky-skip
  mechanism.
- **`KNOWN_ISSUES.md`:** at repo root, enumerates anything 1d
  *deliberately* excludes from diff testing (e.g., messages
  `kafka-clients` cannot encode). Empty is acceptable; any entry
  needs an offending pair, a reason, and a planned resolution.

## 0.1.0 publication policy (1e)

- **API stability:** none pre-1.0. `CHANGELOG.md` tracks every break.
- **Kafka protocol version pin** documented on the docs.rs landing
  page and README: "0.1.x = Kafka 4.2 protocol." Bumping the upstream
  Kafka pin is a *minor* version bump pre-1.0; a *major* bump is
  reserved for genuine API redesigns.
- **`cargo-deny`:** advisories deny, bans warn, sources allow
  `crates-io`, licenses allow Apache-2.0 / MIT / BSD-3-Clause / ISC /
  Unicode-DFS. Anything else needs an explicit allowance with rationale.
- **`cargo-semver-checks`:** runs in PR CI once 0.1.0 ships; gates
  breaking API changes that aren't reflected in a version bump.
- **MSRV policy:** documented in `CHANGELOG.md`; current MSRV is
  1.95.0.
- **Publish order:** `crabka-compression` 0.1.0 first; `crabka-protocol`
  0.1.0 then references it via the published version. No path
  dependencies in the published manifests.

## Generated-code aesthetics

- `#![allow(clippy::pedantic)]` only in the *wrapper modules*, not at
  workspace scope. Pedantic stays on for hand-written code.
- No `#[derive(Hash)]` by default on generated types (`Bytes` and `f64`
  complicate it). Add per-type when a consumer needs it.
- No `serde::Serialize` / `serde::Deserialize` impls on generated
  types.

---

# Slice-wide acceptance criteria

The coverage slice ships when **all** of the following hold:

## Functional

1. All 197 vendored 4.2 schemas have generated owned + borrowed types
   under `crates/protocol/src/{owned,borrowed}/`.
2. Every supported `(api_key, version)` pair compiles, round-trips
   through proptest at default budget, and passes the three JVM-
   differential checks at PR-CI budget.
3. Nightly CI runs the differential sweep at 10,000 cases per pair,
   green.
4. Every known tagged field declared in any 4.2 schema is exposed as a
   typed Rust field; `unknown_tagged_fields` only carries tags absent
   from the schema.
5. `RECORDS` / `COMPACT_RECORDS` decode to a typed `RecordBatch` v2
   with compression handled via `crabka-compression`; JVM-differential
   per codec.
6. `crabka-compression` exposes encode + decode for gzip, snappy, lz4,
   zstd behind per-codec features, default-enabled.
7. A central `ApiKey` enum lists every (request, response) pair with
   their version ranges; documented in rustdoc.

## Test infrastructure

8. JVM oracle handles every `(message_type, version)` combination via
   Kafka's `MessageDataJsonConverter` classes (the pattern proven in
   foundation Task 18). The oracle's `kafka-clients` dep version
   matches `crates/protocol/schemas/VERSION`.
9. Captured-traffic corpus has at least one entry per
   `(api_key, version)` pair that is realistically capturable.
   Admin-only / synthetic-only pairs flagged `synthetic = true`.
10. `KNOWN_ISSUES.md` exists at repo root. Empty is acceptable; any
    entry must include the offending pair, the reason, and the planned
    resolution.

## Release readiness

11. `cargo deny check` passes on advisories, bans, sources, licenses.
12. `cargo semver-checks check-release` passes (no-op for the initial
    publish; gating for subsequent releases).
13. `cargo publish --dry-run` succeeds for both `crabka-compression`
    and `crabka-protocol`.
14. `cargo doc --no-deps` for both crates builds with zero warnings.
15. `CHANGELOG.md` exists at repo root with a `[0.1.0]` entry
    summarizing the slice.

## CI

16. CI matrix from foundation continues to pass on Linux/macOS/Windows
    × Rust 1.95.0. `jvm-differential` job runs the new full-coverage
    diff sweep at PR budget within 10 minutes.
17. Nightly workflow added that runs the 10,000-case diff budget.
    Failures notify the maintainer; do not auto-merge fixes.
18. `cargo deny check` runs in PR CI.
19. `cargo semver-checks check-release` runs in PR CI once 0.1.0 is
    published.

## Public artifacts

20. `crabka-compression` 0.1.0 published to crates.io.
21. `crabka-protocol` 0.1.0 published to crates.io, depending on
    `crabka-compression = "0.1"`.
22. GitHub release tagged `v0.1.0` with notes pointing at the design
    spec and `CHANGELOG.md`.
23. docs.rs builds clean for both crates.

---

# Open questions deferred to the sub-plans

- **1a:** exact curated representative message list (this design
  recommends 5 pairs but the 1a brainstorm may add or swap based on
  what the IR walk surfaces).
- **1b:** whether to expose streaming compression APIs in addition to
  the trait's by-buffer methods. The buffer API is sufficient for
  Kafka's batch-at-a-time wire format; streaming may be useful for
  future tooling.
- **1c:** how to expose the lazy "wire-level `Bytes`" view alongside
  the typed `RecordBatch::decode` view ergonomically. Options range
  from a separate field on every message carrying `records` to a
  helper trait.
- **1d:** which pairs end up in `KNOWN_ISSUES.md` (cannot be known
  until 1d is run).
- **1e:** whether `crabka-protocol` re-exports `crabka-compression` or
  consumers must depend on both. Re-exporting simplifies common usage
  but couples the public APIs.

None of these block this meta-spec; they belong in the brainstorm for
their owning sub-plan.

# Next step

Invoke `writing-plans` to produce a detailed implementation plan for
**sub-plan 1a** (codegen generalization). Sub-plans 1b–1e get their own
brainstorm cycles after 1a ships.
