# Crabka Schema Registry — Slice 2c: JSON Schema compatibility (full Confluent parity) — design

- **Date:** 2026-06-05
- **Status:** Approved (brainstorm); ready for an implementation plan
- **Builds on:** slice 2 (PR #395, merged) — the format-agnostic `compat` engine + the `format::check(ty, reader, writer)` seam + the golden-matrix-from-`cp-schema-registry` validation pattern. Stacks on slice 2b (PR #397; the shared conformance/integration test files), but the JSON rules are independent of the Protobuf rules.
- **Parent roadmap:** `docs/superpowers/specs/2026-06-04-crabka-schema-registry-design.md` (slice 2c — the last of the three formats).

## Motivation

Slice 2 made Avro compatibility real (matched cp 21/21); slice 2b did Protobuf (matched cp 88/88). **JSON Schema** is the last format — `format::json::check` is still a permissive `Ok` stub. Slice 2c fills it with **the full Confluent JSON Schema compatibility rule set**, hand-rolled (no Rust library), comparing two parsed JSON Schema documents, calibrated to match `cp-schema-registry 7.4.0` exactly. JSON Schema is the gnarliest of the three: its compatibility turns on a *subschema / narrowing-widening* relationship and an **open vs closed content model**, with ~40 Confluent difference types.

## Load-bearing decisions

| Decision | Choice | Rationale |
|---|---|---|
| **Rule scope** | **Full Confluent parity** — the entire JSON Schema rule set (type, properties, required, additionalProperties, enum, numeric/string/array constraints, combinators, `$ref`, dependencies, conditionals). | The user chose maximal fidelity (as for Protobuf). |
| **Architecture** | A **structural diff** over two parsed schema documents (`serde_json::Value`) producing classified `Difference`s — identical in shape to slice 2b. | Reuses the proven seam + the cp-calibration method. |
| **Direction** | Handled by the **existing engine** (it calls `check(new, old)` / `check(old, new)`, swapping the diff's `(original, update)`). The rules carry **no** per-direction logic; complementary kinds (`TypeNarrowed`↔`TypeExtended`, `PropertyAddedToClosed`↔`PropertyRemovedFromClosed`) reproduce cp's direction-aware verdicts. | The pure-per-`Kind`-table approach that matched cp 88/88 for Protobuf. |
| **Authority** | The classification table is **calibrated against a comprehensive cp golden matrix**; cp wins on any disagreement. | Confluent's ~40-kind classification is intricate; the matrix is the real spec. |
| **Draft** | Target **draft-07** semantics (what cp's JSON type uses). | Matches the oracle. |

## Architecture

`format::json::check(reader, writer)` becomes:

```
parse both schemas → serde_json::Value     (serde_json, as today)
diffs = diff::compare(original = writer, update = reader)   // Vec<Difference{kind, path}>
incompatible = diffs.iter().filter(|d| !compat::is_backward_compatible(&d.kind))
if incompatible.is_empty() { Ok(()) } else { Err(messages) }
```

A new `format/json/` directory (`git mv` `json.rs` → `json/mod.rs` + `diff.rs` + `compat.rs`). `compat::is_backward_compatible(&Kind)` is a pure per-`Kind` table. The compat engine, Avro/Protobuf paths, 409 enforcement, and `/compatibility` endpoint are unchanged. `format/mod.rs`'s `pub mod json;` and the `SchemaType::Json => json::check(...)` arm resolve to the directory unchanged.

### The central concept — open vs closed content model

An object schema is **closed** when `additionalProperties: false`, else **open**. This flips property add/remove compatibility, so the diff tracks each object's content model and emits content-model-specific kinds (`PropertyAddedToOpenContentModel`, `PropertyRemovedFromClosedContentModel`, `RequiredPropertyAddedToUnopenContentModel`, …) exactly as Confluent's `json.diff` does.

### The comparison walk (`diff.rs`)

Recursively compares two schema `Value`s at each path:
- **type:** `type` (string or array of strings) changed / narrowed (subset) / extended (superset).
- **properties:** for each property name, added/removed classified by the containing object's content model; recurse into the property's subschema; `required` membership add/remove.
- **additionalProperties / unevaluatedProperties:** added / removed / narrowed / extended (a schema vs `true`/`false`).
- **enum / const:** array extended (superset) / narrowed (subset) / changed.
- **numeric:** `minimum`/`maximum`/`exclusiveMinimum`/`exclusiveMaximum`/`multipleOf` added/removed/tightened/loosened.
- **string:** `minLength`/`maxLength` added/removed/tightened/loosened; `pattern` added/removed/changed.
- **array:** `items` (schema or tuple) changed/narrowed; `additionalItems`; `minItems`/`maxItems`; `prefixItems`.
- **object size:** `minProperties`/`maxProperties`.
- **combinators:** `allOf`/`anyOf`/`oneOf` — compare subschema **sets** (product/sum narrowing/widening: subschemas added/removed); `not`.
- **$ref:** resolve intra-document JSON Pointer (`#/definitions/...`, `#/$defs/...`) against the owning document with a visited-set cycle guard, then diff the resolved targets.
- **dependencies / dependentRequired / dependentSchemas.**
- **conditionals:** `if`/`then`/`else`.

### `Difference::Kind` catalog (`diff.rs`) + classification (`compat.rs`)

Full parity, ~40 kinds mirroring Confluent's `io.confluent.kafka.schemaregistry.json.diff` `Difference.Type`. `compat.rs` provides `is_backward_compatible(&Kind) -> bool` — **seeded from Confluent's behavior, then every cp-matrix verdict must pass; mismatches re-tune the table.** Each `Difference` carries a `path` (JSON Pointer) for best-effort `messages` (the `is_compatible` boolean + HTTP code are the fidelity contract, not the message text).

## Validation

A Docker capture harness (extending slice 2b's `capture_protobuf_fixtures.rs` pattern) drives a real `cp-schema-registry 7.4.0`: for each `(case, writer_schema, reader_schema)` under `BACKWARD`/`FORWARD`/`FULL`, set the level, register the writer, `POST /compatibility/.../latest` the reader, record `is_compatible`. Output: `tests/fixtures/compat/json_matrix.json`.

**Systematic coverage — each kind gets a compatible and an incompatible example:** type changed/narrowed/widened (incl. `["string"]`↔`["string","null"]`); property add/remove under **open** and **closed** content models; `additionalProperties` open↔closed; required add/remove; enum extend/narrow; numeric minimum/maximum/exclusive/multipleOf (add/remove/tighten/loosen); string minLength/maxLength/pattern; array items/minItems/maxItems/additionalItems; combinators allOf/anyOf/oneOf subschema add/remove + not; `$ref` target change; dependencies; if/then/else. ≈50 case-pairs × 3 levels.

Tests:
1. **`compat_conformance`** (no Docker) — extend it to also iterate `json_matrix.json`, driving the engine (`check_against_version` with `SchemaType::Json`) and asserting the boolean matches cp for every entry. The calibration gate.
2. **Per-rule unit tests** (`diff.rs`/`compat.rs`) — hand-built schema pairs → expected `Difference::Kind`s + classification, so each category is TDD'd before the cp gate.
3. **In-process enforcement** (`integration.rs`) — a JSON-Schema subject with `BACKWARD`: register v1, an incompatible v2 (e.g. add a required property to a closed model) → **409**; a compatible v2 (add optional property to an open model) → **200**.

> Where the hand-rolled table diverges from cp, fix the table (or `diff.rs` if a difference isn't detected); a genuine cp quirk we choose not to match goes in `tests/fixtures/compat/README.md` with a reason (same discipline as 2/2b, whose `known_divergences` were empty). cp surprises become the documented findings.

> ⚠️ Mac caveat (unchanged): the capture harness is `#[ignore]` Docker; conformance + enforcement run locally (single broker) and in the `schema-registry-integration` CI job.

## File structure

```
crates/schema-registry/src/format/json/        # was json.rs (git mv)
  mod.rs    # existing parse + canonical_form + JsonSchema; `check` delegates to diff+compat
  diff.rs   # Difference{kind,path}, Kind enum, compare() + content-model tracking + $ref resolver
  compat.rs # Kind::is_backward_compatible() table + messages_from(diffs)
crates/schema-registry/tests/
  compat_conformance.rs        # extend: also iterate json_matrix.json
  capture_json_fixtures.rs     # NEW: #[ignore] Docker capture -> json_matrix.json
  integration.rs               # + a JSON-Schema enforcement test
  fixtures/compat/json_matrix.json   # NEW golden verdicts
```

No other module changes (`format/mod.rs`'s `pub mod json;` + the `SchemaType::Json` arm resolve to the directory unchanged).

## Implementation sequencing (batches within the slice)

Each rule category is unit-TDD'd; the cp matrix is the final calibration gate.

1. `mod.rs`/`diff.rs`/`compat.rs` scaffold + `Difference` model + `type` + properties (open/closed content model) + required + additionalProperties.
2. enum + numeric + string + array constraints + object-size.
3. combinators (allOf/anyOf/oneOf/not).
4. `$ref` resolution (intra-document, cycle-guarded) + dependencies + conditionals (if/then/else).
5. capture the full cp `json_matrix.json` (Docker) + extend `compat_conformance` + enforcement integration test + calibrate the table to 100% match.

## Out of scope

Cross-document / remote `$ref` (external `$id`/URL — rare; an unresolvable `$ref` is treated permissively, documented); JSON Schema drafts other than draft-07 semantics; the all-versions `/compatibility` endpoint; deletes/modes/references (later slices). Avro (slice 2) and Protobuf (slice 2b) rules are untouched.

## Risks

1. **Classification-table fidelity** — the core risk; the comprehensive cp matrix is the mitigation (cp authoritative). JSON Schema's ~40 kinds + the content-model interactions are the most divergence-prone of the three formats.
2. **Open/closed content model** — the central subtlety; "property added to a closed model" vs "to an open model" must match cp exactly. This is where the matrix earns its keep.
3. **`$ref` cycles** — intra-document resolution needs a visited-set / bounded-recursion guard so a recursive schema (`#/definitions/Node` referencing itself) terminates.
4. **Combinator subschema matching** — `allOf`/`anyOf`/`oneOf` compare subschema *sets* with no stable identity across versions; matching is heuristic (structural equality / index) and cp-calibrated.
5. **`type` normalization** — `"type":"string"` vs `"type":["string"]` vs `["string","null"]` must normalize so narrow/widen is detected correctly; booleans (`true`/`false` schemas) are valid whole schemas.

## Dependencies

No new crates — `serde_json` (already a dependency) parses the schema documents; the diff is pure Rust over `serde_json::Value`. The Docker matrix uses the existing `testcontainers` + `cp-schema-registry:7.4.0` setup.
