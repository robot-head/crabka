# Crabka: A Rust Reimplementation of Apache Kafka — Design

**Status:** Draft for review
**Date:** 2026-05-10
**Author:** Matthew Stone (with Claude)

## Summary

Crabka is a Rust reimplementation of Apache Kafka, distributed under the
Apache License 2.0 as a derivative work of the upstream project. This
document is a **meta-spec**: it defines the decomposition of the work into
sub-projects, the ordering and dependencies between them, and what "done"
means for each. It also contains a detailed design for the first slice —
the wire protocol codec library, `crabka-protocol`.

Every later sub-project gets its own brainstorm → spec → plan cycle when
its turn comes. This document does not attempt to design the broker,
KRaft, Streams, or Connect in detail; those sketches are intentionally
one paragraph each.

## North star (informational, not committal)

Full ecosystem parity with Apache Kafka:

- Broker (storage, replication, ISR)
- KRaft metadata quorum
- Producer, consumer, and admin clients
- Transactions and exactly-once semantics
- Tiered storage (KIP-405)
- Share groups (KIP-932)
- Auth (SASL, TLS, delegation tokens, ACLs)
- Admin API and CLI tooling
- A Streams equivalent (separate product)
- A Connect equivalent (separate product)

This north star is documented to anchor the decomposition, **not** as a
commitment. Apache Kafka is the result of ~15 years of work by hundreds of
contributors. Reaching parity is a decades-of-engineering proposition. The
project commits to the next slice and to a credible ordering for the
slices after; it does not commit to a completion date for the whole thing,
and may never reach the north star at all.

## Non-goals of this spec

- Designing the broker, Streams, Connect, or KRaft in detail.
- Picking an async runtime, threading model, or storage engine. Those are
  slice-2 decisions.
- Committing to timelines. The meta-spec ships a dependency graph, not a
  Gantt chart.

## Project framing

- **Name:** Crabka. Crate-prefix `crabka-`.
- **License:** Apache 2.0, treating the work as a derivative of Apache
  Kafka. The `NOTICE` file carries Apache Kafka attribution. No attempt to
  upstream to the ASF; Crabka lives as a separate project.
- **Repo:** Crabka lives in its own repository (e.g., `crabka/crabka`),
  not in a `rust/` subdirectory of the Apache Kafka repo. Co-locating
  would couple builds and contribution models that should stay
  independent.
- **Working context:** funded organization with real headcount. The
  decomposition assumes parallel work across sub-projects is feasible.

## Decomposition

Sub-projects in dependency order. Each becomes its own future spec when
its turn comes. Items 1 and 3 can be developed in parallel; items 14 and
15 are separate products that should get their own repos.

| #  | Sub-project                          | Crate(s)                                | Depends on | Done means                                                                                                                                                | Risk      |
|----|--------------------------------------|-----------------------------------------|------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|
|  1 | Wire protocol codec                  | `crabka-protocol`                       | —          | Encodes/decodes every Kafka request/response version of the pinned upstream release; byte-equal with the JVM client across a differential corpus.         | Low       |
|  2 | Client foundation                    | `crabka-client-core`                    | 1          | Connection management, broker discovery, API-version negotiation, request dispatch. No producer/consumer semantics yet.                                   | Medium    |
|  3 | Storage / log layer                  | `crabka-log`                            | —          | Read/write existing Kafka on-disk format byte-compatibly: segments, offset/time indexes, record batch v2, retention, compaction. Verified via JVM log dirs. | High      |
|  4 | Single-node broker MVP               | `crabka-broker`                         | 1, 3       | Produce/fetch/metadata/api-versions over TCP. Single node, no replication, no groups, no auth. JVM clients can produce and consume.                       | High      |
|  5 | Consumer groups + coordinator        | `crabka-broker`, `crabka-client-consumer` | 4        | Classic rebalance first, then KIP-848. Offset commits to `__consumer_offsets`.                                                                            | High      |
|  6 | Producer client                      | `crabka-client-producer`                | 2          | Idempotent producer; batching, compression, partitioner. No transactions yet.                                                                             | Medium    |
|  7 | KRaft / metadata quorum              | `crabka-raft`, `crabka-metadata`        | 1, 3       | Raft log, snapshots, metadata records, controller election. Replaces ZooKeeper entirely (Kafka 4.x is KRaft-only).                                        | Very high |
|  8 | Replication + ISR                    | `crabka-broker`                         | 4, 7       | Multi-broker clusters; per-partition leader election; ISR; follower fetch.                                                                                | Very high |
|  9 | Transactions                         | `crabka-broker`, `crabka-client-producer` | 5, 8     | Transactional producer, transaction coordinator, exactly-once. Includes in-flight KIP-1319 work.                                                          | High      |
| 10 | Admin API + tooling                  | `crabka-client-admin`, `crabka-cli`     | 4–9        | All CreateTopics/DescribeConfigs/AlterConfigs/ACLs/quotas. CLI parity with `kafka-*.sh`.                                                                   | Medium    |
| 11 | Auth / security                      | `crabka-security`                       | 2, 4       | SASL (PLAIN, SCRAM, GSSAPI, OAUTHBEARER), TLS, delegation tokens, authorizer interface.                                                                   | Med-high  |
| 12 | Tiered storage                       | `crabka-tiered-storage`                 | 3, 8       | KIP-405. Pluggable remote storage manager; S3 reference impl.                                                                                             | High      |
| 13 | Share groups                         | `crabka-broker`                         | 5, 8       | KIP-932 queue-style consumption.                                                                                                                          | Medium    |
| 14 | Streams equivalent (separate product)| `crabka-streams-*` (own repo)           | 5, 6       | Processor API + DSL, state stores (RocksDB or sled), exactly-once. Own meta-spec when started.                                                            | Very high |
| 15 | Connect equivalent (separate product)| `crabka-connect-*` (own repo)           | 10         | Distributed runtime, REST API, plugin isolation. Own meta-spec when started.                                                                              | Very high |

### Notes on ordering

- Items 1 and 3 are independent and should be staffed in parallel.
- Item 7 (KRaft) is intentionally sequenced after 4. A single-node broker
  MVP can run with a stubbed metadata image, which yields a real
  demonstrable artifact much sooner than starting with Raft.
- Items 14 and 15 are separate products. The fact that they are part of
  Apache Kafka today is a packaging accident, not a design constraint.

### Cross-cutting concerns

- **Async runtime.** `tokio` is the default assumption, but the binding
  decision is deferred to slice 2. Slice 1 has no I/O and no async.
- **Observability.** Any slice that does I/O is expected to emit
  OpenTelemetry traces and metrics from the first PR.
- **Compatibility matrix.** Every slice declares which Kafka protocol
  versions and KIPs it targets and which it explicitly does not.
- **Conformance suite.** A shared test harness (`crabka-conformance`)
  runs JVM Kafka client and broker images via testcontainers. Every
  slice that touches the wire contributes test cases.

---

# Slice 1 detailed design: `crabka-protocol`

## Purpose

A pure-Rust library that encodes and decodes every Kafka request and
response message, for every protocol version, byte-equivalent to the JVM
implementation. No I/O, no async, no networking — those are slice 2's
problem. This crate is the foundation everything else sits on.

## Scope of "every"

Pinned to one Apache Kafka release (recommend the latest GA at spec
time; the exact version is fixed when the implementation plan is written
and recorded in `schemas/VERSION`). Future Kafka releases bump the pin
and regenerate the schemas. Older releases are supported via per-message
version ranges in the schemas themselves — that is already how Kafka's
wire protocol works.

## Crate layout

```
crabka/                              # workspace root
├── Cargo.toml
└── crates/
    └── protocol/
        ├── Cargo.toml               # name = "crabka-protocol"
        ├── build.rs                 # invokes codegen at build time
        ├── schemas/                 # vendored from apache/kafka, pinned by commit SHA
        │   ├── VERSION              # records source SHA + Kafka version
        │   └── *.json               # ~100 RequestMessage/ResponseMessage/HeaderData schemas
        ├── codegen/                 # separate bin in same crate, not a proc-macro
        │   ├── main.rs
        │   ├── ir.rs                # parse JSON → internal IR
        │   ├── owned.rs             # emit owned-flavor types + codecs
        │   └── borrowed.rs          # emit borrowed-flavor types + codecs
        ├── src/
        │   ├── lib.rs
        │   ├── primitives.rs        # INT8/16/32/64, VARINT, UVARINT, COMPACT_STRING, ...
        │   ├── tagged_fields.rs     # KIP-482 flexible-version tagged fields
        │   ├── error.rs             # ProtocolError, decode error kinds
        │   ├── api_key.rs           # ApiKey enum (generated)
        │   ├── api_versions.rs      # version negotiation helpers
        │   ├── owned/               # generated; one module per message
        │   └── borrowed/            # generated; one module per message
        └── tests/
            ├── differential/        # vs JVM oracle (see Conformance)
            ├── proptest/            # roundtrip & cross-version property tests
            └── corpus/              # checked-in captured-traffic vectors
```

### Why a bin-style generator, not a proc macro

Generated code is large (thousands of types). Committing the generator
output keeps compile times sane, makes diffs reviewable, and lets
`rust-analyzer` index real code. `build.rs` runs the generator only when
schemas or templates change and writes into `$OUT_DIR`; for release builds
a snapshot is also checked into `src/owned/` and `src/borrowed/`, and CI
verifies the snapshot matches what the generator would produce. This is
the same approach widely used by `prost-build` consumers.

## Codegen pipeline

1. **Vendor the schemas.** Pull
   `clients/src/main/resources/common/message/*.json` from a pinned
   Apache Kafka commit into `schemas/`. Re-vendor on each upstream bump.
   `tools/sync-schemas.sh` does this and records the source SHA in
   `schemas/VERSION`.
2. **Parse JSON into an IR.** One `MessageSpec` per file: API key,
   min/max versions, flexible-versions threshold, fields tree with types,
   versions, nullability, tagged-field IDs, default values.
3. **Validate the IR.** Reject schemas the generator does not yet
   understand (rather than silently emitting incorrect code). New
   upstream schema features must be added to the generator deliberately.
4. **Emit two flavors per message.** For each `Message` in the IR:
   - `owned::FooRequest` — fields are `Bytes`, `String`, `Vec<T>`, owned
     nested structs.
   - `borrowed::FooRequest<'a>` — fields are `&'a [u8]`, `&'a str`,
     `Cow<'a, [T]>` or borrowed slices, with `'a` propagated through
     nested structs.
   Both flavors implement the same `Encode`/`Decode` traits. A
   `to_owned()` method on the borrowed flavor produces the owned one. No
   `as_borrowed()` is provided on the owned flavor (it cannot be done
   without re-encoding); this trade-off is documented in rustdoc.
5. **Emit version dispatch.** For each API key, generate a `RequestKind`
   / `ResponseKind` enum keyed on version that returns the right concrete
   struct. Version negotiation lives in `api_versions.rs`.

## Public API sketch

```rust
// Core traits — buffer-generic via the bytes crate.
pub trait Encode {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError>;
    fn encoded_len(&self, version: i16) -> usize;
}

pub trait Decode<'de>: Sized {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError>;
}

// Owned flavor — no lifetime; easy to use across await points.
pub mod owned {
    pub struct ProduceRequest { /* generated */ }
    impl Encode for ProduceRequest { /* ... */ }
    impl Decode<'static> for ProduceRequest { /* ... */ }
}

// Borrowed flavor — zero-copy decode tied to input lifetime.
pub mod borrowed {
    pub struct ProduceRequest<'a> { /* generated */ }
    impl<'a> Encode for ProduceRequest<'a> { /* ... */ }
    impl<'de> Decode<'de> for ProduceRequest<'de> { /* ... */ }
}

// Request header + framing helpers (the 4-byte length prefix).
pub fn read_frame<B: Buf>(buf: &mut B) -> Result<RequestFrame<'_>, ProtocolError>;
pub fn write_frame<B: BufMut>(buf: &mut B, frame: &impl Encode, version: i16)
    -> Result<(), ProtocolError>;
```

## Deliberately not in this crate

- **No async, no `tokio`, no I/O.** The crate compiles with
  `default-features = []`. `no_std` is a possible future direction but
  not a goal.
- **No connection management, no negotiation logic** beyond the
  `(api_key, min, max)` version table. Negotiation is slice 2.
- **No record-batch compression integration.** Record-batch parsing
  produces a `RecordBatch` value with a `compression: CompressionType`
  field and a still-compressed payload. Decompression lives in a
  separate `crabka-compression` crate (gzip, snappy, lz4, zstd) that
  slice 2 wires up. This keeps `crabka-protocol` free of C deps.
- **No higher-level domain types.** A topic name is a `String`, not a
  `TopicName` newtype. The crate is a wire-level mapping, not a domain
  model. Newtypes belong to the consuming crates.

## Cross-cutting decisions

- **MSRV:** Rust stable, with a documented MSRV ≥ 12 months old, pinned
  in `rust-toolchain.toml`.
- **Compression deps:** none (see above).
- **Error model:** one `ProtocolError` enum, `#[non_exhaustive]`,
  `thiserror`-derived. Decoded `errorCode` fields on responses are *not*
  surfaced as `Result`s — they are plain `i16`, because responses are
  valid wire messages even when they indicate a Kafka-level error.
- **Stability:** pre-1.0 until slice 2 has shipped and shaken out the
  API. SemVer applies at 1.0; before then, breaking changes per minor
  version are allowed with a CHANGELOG entry.
- **License headers:** every generated and hand-written file carries the
  Apache 2.0 header. `NOTICE` carries Apache Kafka attribution.

## Conformance and testing

The whole value of this crate is "byte-equal to the JVM implementation."
That is the bar, and the test strategy is built around proving it.

### Layer 1 — unit tests on primitives

Every primitive codec (`INT8`–`INT64`, `VARINT`, `UVARINT`, `UUID`,
`COMPACT_STRING`, `COMPACT_BYTES`, `COMPACT_ARRAY`, tagged fields,
nullable variants) gets table-driven tests with hand-curated edge cases:
empty, max, min, truncated, overlong-varint, etc. Runs in milliseconds on
every PR.

### Layer 2 — proptest round-trip and cross-flavor checks

Per message type, per supported version:

- `decode(encode(x)) == x` for owned-flavor random instances (via
  `arbitrary`).
- `encode(x).bytes == encode(x.to_owned()).bytes` — the two flavors must
  produce identical wire output for the same logical value.
- `encoded_len(x) == encode(x).len()` — predicted size matches actual.
- Tagged-fields invariants: unknown tags preserved on round-trip
  (KIP-482 requirement); known tags decode to typed fields.

### Layer 3 — differential testing against the JVM oracle

This is the load-bearing layer.

A small Java program — `tools/oracle/` — links against the published
`org.apache.kafka:kafka-clients` jar and exposes two operations over
stdio:

```
encode <api_key> <version> <json>   → hex-encoded bytes
decode <api_key> <version> <hex>    → canonical JSON
```

The JSON uses the same field names and shapes as the upstream JSON
schemas, so it round-trips through Java's `MessageGenerator`-produced
classes without manual mapping. The oracle is built once per CI run and
reused across all differential tests via a long-lived child process
(stdin/stdout protocol, one request per line) — spawning per case is too
slow.

**Why a sidecar process and not JNI:** JNI ties developer machines to a
JVM install and complicates cross-compilation. A subprocess speaks
bytes; it is trivially portable, debuggable, and disposable.

For every `(message_type, version)` pair declared in the schemas, the
differential harness runs three checks:

1. **JVM-generated → Rust-decoded.** Oracle produces a randomized
   instance and its byte encoding. Rust decodes both flavors; asserts
   structural equality with the oracle's JSON.
2. **Rust-encoded → JVM-decoded.** Rust generates an instance, encodes
   it, and asks the oracle to decode. Assert the oracle's JSON matches
   what we put in.
3. **Byte equality.** Rust generates an instance, encodes in Rust;
   oracle encodes the equivalent JSON; assert byte strings are
   identical. Strict — same bytes, no tolerance.

Seed every randomized run; on failure CI prints the seed and we can
reproduce locally. Run a fixed wall-clock budget per `(message,
version)` in PR CI; nightly runs do a much larger budget.

### Captured-traffic corpus

A `tests/corpus/` directory of real wire frames captured from running
clusters via a small `tcpdump`-style helper. Each frame is checked in as
a hex file plus a `.toml` metadata sidecar (api key, version, direction,
source Kafka version). Every commit re-runs every frame through both
Rust flavors and asserts decode-then-encode produces identical bytes.
The corpus grows organically as bugs are found in the wild.

## Acceptance criteria

The slice is shippable when **all** of these hold for the pinned Kafka
release:

1. Every request and response type listed in `schemas/` has owned and
   borrowed generated code that compiles.
2. Every supported `(api_key, version)` pair passes the proptest
   round-trip suite (Layer 2).
3. Every supported `(api_key, version)` pair passes all three
   differential checks against the JVM oracle (Layer 3) on a budget of
   at least N random cases per pair. N is determined in the
   implementation plan; recommended starting point is 10,000 in nightly,
   100 per pair in PR CI.
4. The corpus has at least one captured frame per `(api_key, version)`
   pair that can realistically be captured. Admin-only or rarely used
   messages get hand-built vectors with a `synthetic = true` flag in
   their TOML sidecar.
5. Documentation: rustdoc on every public type, a README describing the
   codegen workflow, a CONTRIBUTING covering "how to add a new Kafka
   version" and "how to regenerate after upstream schema bumps."
6. CI green on Linux, macOS, and Windows for MSRV and current stable
   Rust.

## Risks and mitigations

| Risk                                                                           | Mitigation                                                                                                                          |
|--------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| Upstream schema features the generator does not understand produce wrong code. | Generator validates the IR; unknown constructs are hard errors, not skipped.                                                        |
| The two flavors drift in correctness.                                          | Proptest layer 2 includes cross-flavor encode equality; differential layer 3 runs against both flavors independently.               |
| Subtle KIP-482 tagged-field edge cases (unknown tags, ordering).               | Explicit proptest invariants plus differential cases that inject unknown tags.                                                      |
| Generated code bloat hurting compile times.                                    | Committed snapshot + `build.rs` short-circuit; per-message modules so `cargo` can parallelize.                                      |
| Schemas evolve faster than re-vendoring.                                       | `tools/sync-schemas.sh` plus a CI job that diffs upstream weekly and opens an issue when drift exceeds a threshold.                 |
| JVM oracle becomes a maintenance burden.                                       | Keep it ~200 LOC. Single class. Build via the Gradle wrapper; no transitive dep management.                                         |

## Open questions deferred to the implementation plan

- Exact MSRV value (e.g., 1.80 vs 1.82).
- Exact differential-test budget (N) for PR CI vs nightly.
- Whether the borrowed flavor uses `Cow<'a, [T]>` or a custom slice-or-vec
  enum for variable-length collections.
- Whether `Bytes` from the `bytes` crate is the right owned byte type, or
  whether we should expose a generic byte-buf trait.
- Whether the captured-traffic corpus should be in-repo or in a separate
  Git LFS / submodule store.

These are real choices but they do not block design approval; they
belong in the implementation plan where evidence can be gathered.

## Next step after this spec

Invoke the `writing-plans` skill on this document to produce a detailed,
reviewable implementation plan for slice 1 (`crabka-protocol`). Later
slices get their own brainstorm → spec → plan cycle when their turn
comes.
