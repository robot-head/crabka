# Crabka Schema Registry — Slice 2b: Protobuf compatibility (full Confluent parity) — design

- **Date:** 2026-06-05
- **Status:** Approved (brainstorm); ready for an implementation plan
- **Builds on:** slice 2 (PR #395) — the format-agnostic `compat` engine, the `format::check(ty, reader, writer)` seam (Avro real; Protobuf/JSON permissive placeholders), 409 enforcement on register, the real `/compatibility` endpoint, and the golden-matrix-from-`cp-schema-registry` validation pattern.
- **Parent roadmap:** `docs/superpowers/specs/2026-06-04-crabka-schema-registry-design.md` (slice 2b).

## Motivation

Slice 2 wired compatibility enforcement and made **Avro** real via `apache-avro` (matched cp on all 21 cases). **Protobuf** and JSON Schema were left permissive (`check` returns `Ok`). Slice 2b makes **Protobuf** real — implementing **the full Confluent Protobuf compatibility rule set** — so that registering an incompatible Protobuf evolution under a non-`NONE` subject is rejected with 409, and `/compatibility` returns true verdicts. There is **no Rust library** for these rules (Confluent implements them in Java), so they are hand-rolled by comparing two `FileDescriptorProto`s, calibrated against a real `cp-schema-registry`.

## Load-bearing decisions

| Decision | Choice | Rationale |
|---|---|---|
| **Rule scope** | **Full Confluent parity** — every Protobuf difference category (fields, scalar groups, kind, label, oneof, reserved, enum, message, map, package). | The user chose maximal fidelity over a core subset. |
| **Architecture** | A **structural diff** over `FileDescriptorProto`s producing classified `Difference`s — mirroring Confluent's `SchemaDiff`. | Matches Confluent's own model and reuses the existing seam. |
| **Direction** | Handled by the **existing engine** — it already calls `check(new, old)` (BACKWARD) and `check(old, new)` (FORWARD), swapping the diff's `(original, update)`. Slice 2b adds **no** per-direction logic. | The seam is direction-agnostic; the classification table is the single "backward-compatible kinds" set. |
| **Source of truth** | The classification table is **calibrated against a comprehensive cp golden matrix**; cp wins on any disagreement. | The exact Confluent classification is intricate and version-specific; the matrix is the authoritative spec. |
| **Out of scope** | proto2 `group`s, extensions, custom options (cp largely ignores these for compatibility); JSON Schema (slice 2c). | Rare constructs; keep them permissive and out of the matrix. |

## Architecture

`format::protobuf::check(reader, writer)` becomes:

```
parse both .proto -> FileDescriptorProto      (protox_parse, as today)
diffs = diff::compare(original = writer, update = reader)   // Vec<Difference{ kind, path }>
incompatible = diffs.iter().filter(|d| !d.kind.is_backward_compatible())
if incompatible.is_empty() { Ok(()) } else { Err(messages) }
```

The engine already supplies direction by argument order:
- BACKWARD → `check(reader = new, writer = old)` → `compare(original = old, update = new)`
- FORWARD → `check(reader = old, writer = new)` → `compare(original = new, update = old)`
- FULL → both.

So slice 2b is exactly: **the diff** + **the classification table**. No changes to `compat/`, `kafkastore/`, `rest/`, or the engine.

### The comparison walk (`diff.rs`)

Over the descriptor fields we already access (`message_type`, `field` keyed by **number** — the wire identity, `nested_type`, `enum_type`, `oneof_decl` + each field's `oneof_index`, `reserved_range`/`reserved_name`, `package`, `syntax`):

- **File:** compare `package`; recurse top-level messages (by name) and enums (by name); a message/enum present in original but absent in update ⇒ `MessageRemoved`/`EnumRemoved`, and vice-versa for added.
- **Message:** match fields **by number**. Number present in original, absent in update ⇒ `FieldRemoved`; absent→present ⇒ `FieldAdded`. For a number in both: compare type/kind/label/name/oneof membership. Recurse `nested_type` and nested `enum_type`. Compare `oneof_decl` and `reserved_range`/`reserved_name`. Detect `map<>` via the synthetic `map_entry` nested message (the descriptor's `options.map_entry`).
- **Enum:** match values by number; added/removed constants.

### `Difference::Kind` catalog (`diff.rs`) + classification (`compat.rs`)

| Category | Kinds |
|---|---|
| Field presence | `FieldAdded`, `FieldRemoved` |
| Field scalar type | `FieldScalarKindChanged { within_compatible_group: bool }` — groups: `{int32,int64,uint32,uint64,bool}`, `{sint32,sint64}`, `{string,bytes}`, `{fixed32,sfixed32}`, `{fixed64,sfixed64}` (each of `float`/`double` alone) |
| Field kind | `FieldKindChanged` (scalar↔message↔enum↔map↔group), `FieldNamedTypeChanged` (message/enum identity) |
| Field label | `FieldLabelChanged` (singular↔repeated↔required) |
| Oneof | `OneofFieldMovedIn`, `OneofFieldMovedOut`, `OneofAdded`, `OneofRemoved`, `MultipleFieldsMovedIntoOneof` |
| Reserved | `ReservedNumberAdded`, `ReservedNameAdded`, `FieldReservedConflict` (a live number/name now reserved) |
| Enum | `EnumConstAdded`, `EnumConstRemoved`, `EnumAdded`, `EnumRemoved` |
| Message | `MessageAdded`, `MessageRemoved` |
| File | `PackageChanged` |

`compat.rs` provides `fn is_backward_compatible(kind: &Kind) -> bool` — a single table. **Initial values are seeded from Confluent's docs/behavior, then every cp-matrix verdict must pass; mismatches re-tune the table.** Each `Difference` carries a `path` (e.g. `Msg.field[3]`) used to build the best-effort `messages` (the `is_compatible` boolean + HTTP code are the fidelity contract, not the message text).

## Validation

The cp golden matrix is the authority. A Docker capture harness (extending slice 2's `capture_compat_fixtures.rs` pattern) drives a real `cp-schema-registry 7.4.0`: for each case `(writer.proto, reader.proto)` under `BACKWARD`/`FORWARD`/`FULL`, set the subject's level, register the writer, `POST /compatibility/.../latest` the reader, record `is_compatible`. Output: `tests/fixtures/compat/protobuf_matrix.json` (array of `{case, level, writer, reader, is_compatible}`).

**Systematic coverage — each `Kind` gets a compatible and an incompatible example:** field add/remove; scalar change within each compatible group **and** across groups; kind change (scalar↔message, scalar↔enum, scalar↔map); named-type change; label change (singular↔repeated); oneof move-in / move-out / add; reserved number + reserved name then reuse; enum const add/remove + enum add/remove; message add/remove + nested-message field change; `map<>` value-type change + scalar↔map; package change. ≈35 case-pairs × 3 levels.

Tests:
1. **`compat_conformance`** (no Docker) — already iterates `avro_matrix.json`; extend it to also iterate `protobuf_matrix.json`, driving the engine (`check_against_version`) and asserting the boolean matches cp for every entry. This is the calibration gate.
2. **Per-rule unit tests** (`diff.rs`/`compat.rs`) — hand-built descriptor pairs → expected `Difference::Kind`s and classification, so each rule category is TDD'd before the cp gate.
3. **In-process enforcement** (`integration.rs`) — a Protobuf subject with `BACKWARD`: register v1, an incompatible v2 (e.g. field type across groups) → **409**; a compatible v2 (add field) → **200**.

> Where the hand-rolled table diverges from cp on a case, fix the table; if a case is a genuine cp quirk we choose not to match, document it in `tests/fixtures/compat/README.md` with a reason (same discipline as slice 2's empty `known_divergences`). Divergences are the interesting findings.

> ⚠️ Mac caveat (unchanged): the capture harness is `#[ignore]` Docker; conformance + enforcement run locally (single broker) and in the `schema-registry-integration` CI job.

## File structure

Convert the single `format/protobuf.rs` into a focused directory (a `git mv`, so the diff engine doesn't bloat one file):

```
crates/schema-registry/src/format/protobuf/
  mod.rs     # existing parse + normalize + ProtobufSchema; `check` delegates to compat
  diff.rs    # the descriptor comparison walk + `Difference { kind: Kind, path: String }` + `compare()`
  compat.rs  # `Kind::is_backward_compatible()` classification table + message formatting
```

`format/mod.rs`'s `pub mod protobuf;` and `format::check`'s `SchemaType::Protobuf => protobuf::check(...)` arm are unchanged (a module path resolves to either a file or a directory). No other module changes.

## Implementation sequencing (batches within the slice)

Each rule category is unit-TDD'd; the cp matrix is the final calibration gate.

1. `mod.rs`/`diff.rs`/`compat.rs` scaffold + `Difference` model + field presence + scalar-group table + kind/named-type + label.
2. Oneof migration rules.
3. Reserved ranges/names + `map<>` (synthetic `map_entry`) handling.
4. Enum (const + enum add/remove) + message add/remove + nested recursion + package.
5. Capture the full cp `protobuf_matrix.json` (Docker) + extend `compat_conformance` + enforcement integration tests + calibrate the table to 100% match.

## Out of scope

proto2 `group`s, extensions, custom options (not part of cp's compat surface — permissive, excluded from the matrix); JSON Schema compatibility (slice 2c); the all-versions `/compatibility` endpoint; deletes/modes/references.

## Risks

1. **Classification-table fidelity** — the core risk; the comprehensive cp matrix is the mitigation (cp authoritative). Some verdicts may surprise (e.g. proto3 field-removal nuances, oneof rules) — those become documented findings.
2. **Maps** — `map<k,v>` is descriptor sugar (a synthetic `map_entry` nested message with `repeated` entries); the diff must recognize the `map_entry` option and compare key/value rather than treating it as a nested message rename.
3. **Oneof** — move-in/move-out and "multiple fields collapsed into a oneof" have subtle Confluent rules; the matrix must exercise each move.
4. **Field number as identity** — fields must be matched by **number**, not name (names don't affect the wire); a name change at the same number is a distinct (and typically compatible) difference.

## Dependencies

No new crates — `protox-parse` + `prost-reflect`'s `prost_types` (already dependencies) provide the `FileDescriptorProto` and all nested descriptor types. The Docker matrix uses the existing `testcontainers` + `cp-schema-registry:7.4.0` setup.
