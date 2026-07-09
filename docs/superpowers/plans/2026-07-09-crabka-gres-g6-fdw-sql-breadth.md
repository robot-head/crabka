# Chapter Gres G-6: FDW + SQL Breadth Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The FDW becomes an honest product surface — headers populated, protobuf complete, own-cluster topics queryable with zero configuration — and the SQL-breadth ratchet process is installed. (Breadth features themselves are separate design cycles per the spec; this plan covers the FDW track and the process.)

**Architecture:** Header decoding lands in the published `crabka-client-core` fetch path; the FDW's protobuf stub completes over `writer_message_type` + a runtime `protox` compile; the default server resolves to the compute's own bootstrap in substrate mode; the baseline-ratchet rule is codified where reviewers will see it.

**Tech Stack:** Kafka record-format v2 header encoding (varint counts/lengths), `protox` (pure-Rust protobuf compiler, already a workspace dep) + `prost-reflect` `DynamicMessage`, the gres-fdw roundtrip harness.

## Global Constraints

- **Prerequisites:** G-1 landed (G-2 only for the default-server item's substrate wiring — that one step gates on it). Verify signatures against the landed tree.
- **Spec:** [2026-07-09-crabka-gres-g6-fdw-sql-breadth-design.md](../specs/2026-07-09-crabka-gres-g6-fdw-sql-breadth-design.md).
- **Header decoding must match the wire exactly** — the v2 record format's header array (varint count; per header: varint key length, UTF-8 key, varint value length or -1 for null, value bytes). Differential-verify against batches produced by `crabka-client-producer` AND, if a JVM fixture is cheap via the existing oracle tooling, one JVM-produced batch.
- **`crabka-client-core` is a published crate:** the API addition gets rustdoc, a changelog-worthy conventional commit (`feat(client-core): …`), and whole-struct test comparisons per house style.
- Lints/format/commit/test conventions as in the G-2 plan.

---

## Batch 1 — independent foundations (run Tasks 1 and 2 in parallel; disjoint crates)

### Task 1: Record headers through `crabka-client-core`

**Files:** Modify `crates/client-core/src/fetch.rs` (and the record-decode layer it calls — locate where `FetchedRecord` is assembled from the decoded batch; the protocol crate's record structs already parse headers off the wire for the broker's benefit — verify, and if the *client-side* decode path skips them, extend it there), tests in the same crate.

**Interfaces:**
- `FetchedRecord` gains `pub headers: Vec<FetchedHeader>` with `pub struct FetchedHeader { pub key: String, pub value: Option<Bytes> }` (mirror the protocol crate's existing header type if one is public — prefer re-use over a new type; decide by inspection).

Steps: failing test — produce a record with two headers (one null-valued) via `crabka-client-producer` against an in-process broker, `fetch_partition` returns them key/value-exact (whole-struct compare); plus a pure decode unit over a hand-encoded v2 batch fixture (covers varint edge cases: empty headers, null value, multi-byte varint lengths). Implement. Confirm zero behavior change for existing callers (additive field). nextest/clippy/fmt; commit `feat(client-core): surface record headers from fetch`.

### Task 2: Protobuf descriptor completion in `crabka-gres-fdw`

**Files:** Modify `crates/gres-fdw/src/decode.rs` (the `build_message_descriptor` stub), `Cargo.toml` (`protox` moves from dev-dependency to dependency), tests.

Implementation: resolve `(schema_text, message_type)` via `SchemaCache::writer_schema` + `writer_message_type` (both exist in the landed schema-serde; the donor stubbed this exactly because 0.3.7 lacked `message_type`); compile the schema text with `protox` into a descriptor set (the crate's own typed tests already compile in-memory protos with protox — reuse that idiom); build the `prost_reflect::DescriptorPool` and select the named message (absent `message_type` → the schema's first message, matching Confluent convention; document it). Handle imports pragmatically v1: schemas with references are out of scope with a clear error (the reference plumbing exists in the registry harness; wire it only if the roundtrip needs it).

Steps: failing units (descriptor from single-message schema; named-type selection among multiple messages; `WriterSchemaPending` propagation; reference schema → clear unsupported error). Implement. nextest/clippy/fmt; commit `feat(gres): complete the FDW protobuf decode path`.

---

## Batch 2 — product wiring (serial; touches the harness both tasks extend)

### Task 3: `_headers` population + protobuf end-to-end in the roundtrip harness

**Files:** Modify `crates/gres-fdw/src/scan.rs` (+ `source.rs` where `FetchedRecord` is consumed), `tests/roundtrip.rs`, `tests/harness/mod.rs`.

`_headers` renders per the donor's envelope-column type for it (inspect the landed column type: if `Bytea`/`Text`, render the PostgreSQL-conventional text form; keep whatever type the donor declared and make the rendering deterministic — key-sorted, documented in the FDW README). Extend the harness: produce with headers; register a protobuf schema (the harness's `register_avro` gains a protobuf sibling — `KafkaStore::register` already takes `SchemaType`); protobuf produce via `schema-serde` wire encode. Steps: failing roundtrip assertions — `SELECT _headers` returns the produced headers; `IMPORT FOREIGN SCHEMA` + typed `SELECT` on the protobuf topic returns projected columns (whole-row compares). Implement rendering + harness. nextest/clippy/fmt; commit `feat(gres): honest _headers and protobuf topics end-to-end`.

### Task 4: Own-cluster default server (gates on G-2)

**Files:** Modify `crates/gres-fdw/src/config.rs` (default-bootstrap resolution), `crates/gres/src/main.rs` (pass the substrate bootstrap into the scanner), `tests/roundtrip.rs` (default-server variant).

`KafkaFdw` gains a constructor carrying an optional default bootstrap (`KafkaFdw::with_defaults(bootstrap: Option<String>)`; the unit-struct default stays for local mode); `resolve` uses it when the server options omit `bootstrap` (explicit options always win). The gres bin passes its `--substrate-bootstrap` value in substrate mode. Steps: failing config unit (`missing bootstrap + default present → resolves; both absent → the existing config error`), failing roundtrip variant (`CREATE SERVER s1 FOREIGN DATA WRAPPER kafka_fdw` with no options works on a substrate tenant), implement, nextest/clippy/fmt, commit `feat(gres): default FDW server targets the tenant's own cluster`.

---

## Batch 3 — the process (serial)

### Task 5: Codify the baseline ratchet

**Files:** Modify `crates/gres-conformance/README.md` (the ratchet rule: corpus growth and `baseline.json` bump land in the same reviewed commit with the parity report demonstrating the new floor; a baseline change in any other commit is review-blocking), `CONTRIBUTING.md` (one paragraph pointing at it under a "Gres conformance" heading), and — if any FDW item above added corpus files — perform the first ratchet as the worked example. Commit `docs(gres): the conformance baseline ratchet process`.

## Completion checklist

- Headers: produced → fetched → `SELECT _headers`, exact (Tasks 1, 3).
- Protobuf: register → produce → `IMPORT FOREIGN SCHEMA` → typed `SELECT`, end-to-end (Tasks 2, 3).
- Default server: zero-config topics-as-tables on a substrate tenant (Task 4).
- The ratchet process is written where reviewers will enforce it (Task 5).
- Breadth features (constraints → indexes → windows) intentionally NOT here — each starts with its own design cycle per the spec.
