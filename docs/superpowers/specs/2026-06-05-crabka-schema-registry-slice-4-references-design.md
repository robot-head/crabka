# Crabka Schema Registry — Slice 4: schema references — design

- **Date:** 2026-06-05
- **Status:** Approved (brainstorm); ready for an implementation plan
- **Builds on:** slices 1+2+2b+2c+3 (registry + full compatibility trilogy + deletes/modes/lookups). The store, `_schemas` records, `KafkaStore` facade, the format-agnostic `compat` engine + `format::check` seam, and the axum REST surface all exist. Stacks on slice 3 (PR #407).
- **Parent roadmap:** `docs/superpowers/specs/2026-06-04-crabka-schema-registry-design.md` (slice 4).

## Motivation

A schema can **reference** another registered schema — a Protobuf `import`, an Avro named-type reference, a JSON Schema `$ref`. `SchemaValue` already carries a `references: [{name, subject, version}]` field, but it is **parsed-and-ignored** on register (`rest/subjects.rs`: `let _ = &req.references`), `encode_schema` hardcodes `references: Vec::new()`, and `referencedby` returns `[]`. Slice 4 makes references real: accept + validate them on register, **resolve** the referenced schemas so a ref-using schema parses / canonicalizes / compat-checks correctly across all three formats, expose `referencedby` real data, protect referenced versions from deletion, and include `references` in the GET responses — all calibrated to `cp-schema-registry 7.4.0`.

## Load-bearing decisions

| Decision | Choice | Rationale |
|---|---|---|
| **Scope** | All three formats' reference resolution in one slice (Avro + Protobuf + JSON), plus the format-agnostic infrastructure. | User chose "all three at once." References are a coherent feature; the infra (threading, validation, `referencedby`, delete-protection) is shared. |
| **Seam threading** | **Resolve-then-pass a bundle.** The facade/compat layer resolves the reference closure from the store, then passes a `&[ResolvedReference]` into a widened `format::parse`/`check`. Format modules stay **store-agnostic** (pure functions of schema + resolved refs). | Preserves the pure, unit-testable format seam that made the compat trilogy work. Rejected: a store-coupled resolver trait (impure formats); pre-inlining refs at register (breaks cp byte-fidelity + loses ref structure). |
| **Protobuf resolution** | Parse each resolved `.proto` with `protox_parse`, assemble a `FileDescriptorSet`, link via `prost_reflect::DescriptorPool::from_file_descriptor_set`. | **No new dependency** — `prost-reflect 0.16` is already in the tree and links cross-file imports. Avoids adding the `protox` compiler crate. |
| **Reference identity** | The schema **id** is assigned over `(canonical form + references)`; the same schema text with different references is a different id. | Confluent's id identity includes references; dedup key must include them. cp-validated. |
| **Authority** | Exact error codes, `referencedby` shape, `_schemas` `references` bytes, and id-assignment-with-refs are **cp-captured** (Docker) + asserted. | Same fidelity discipline as slices 2/2b/2c/3. |
| **Transitive refs** | Resolved recursively (a referenced schema may itself have references), cycle-guarded. | cp resolves transitively; a Protobuf import may import another. |

## Architecture

Direction of data on register-with-references:

```
REST register {schema, schemaType, references:[{name,subject,version}], id?, version?}
  → facade: validate each (subject,version) exists           → SrError::ReferenceNotFound on miss
           resolve the (transitive, cycle-guarded) closure    → Vec<ResolvedReference{name, ty, schema}>
           normalized_storage_form(ty, schema, &refs)         → canonical/dedup key includes refs
           compat::check_registration(.., &refs)              → ref-aware compat
           persist SCHEMA record WITH references               → store indexes refs (forward + reverse)
  → reader folds references into the store
GET /schemas/ids/{id}                    → {schema, schemaType, references}
GET /subjects/{s}/versions/{v}           → {.., references}
GET /subjects/{s}/versions/{v}/referencedby → [schema_id, …]   (reverse index; cp shape)
DELETE a referenced version              → SrError::ReferencedByOthers (cp ~42206)
```

### The reference-resolution bundle (`compat` / facade + `store`)

`ResolvedReference { name: String, ty: SchemaType, schema: String }` — `name` is the format-specific reference label (Protobuf import path, Avro type name, JSON `$ref` target); `ty`+`schema` are the referenced version's stored type + text.

`store::resolve_closure(&self, refs: &[SchemaReference]) -> Result<Vec<ResolvedReference>, SrError>` walks each `{name, subject, version}`: looks up that `(subject, version)` (`SrError::ReferenceNotFound` if absent), emits a `ResolvedReference`, and recurses into *its* references — a visited-set keyed on `(subject, version)` guards cycles; output order is deterministic (depth-first, refs in declared order, dedup-by-name keeping first). The candidate's own references are resolved once at the facade and the bundle is reused for `normalized_storage_form` + every `check` direction.

### The widened format seam (`format/mod.rs`)

```rust
pub struct ResolvedReference { pub name: String, pub ty: SchemaType, pub schema: String }

pub fn parse(ty, schema: &str, refs: &[ResolvedReference]) -> Result<Box<dyn ParsedSchema>, SrError>;
pub fn normalized_storage_form(ty, schema: &str, refs: &[ResolvedReference]) -> Result<String, SrError>;
pub fn check(ty, reader, writer, reader_refs: &[ResolvedReference], writer_refs: &[ResolvedReference])
    -> Result<(), Vec<String>>;
```

Existing call sites pass `&[]` where there are no references (back-compat is irrelevant — greenfield — so every call site is updated). Per-format consumption:

- **Avro** (`format/avro.rs`): `apache_avro::Schema::parse_list(&[dep_schemas…, candidate])` — dependencies first so their named types are in scope; the candidate is the last element. `canonical_form()` from the candidate's parsed `Schema`.
- **Protobuf** (`format/protobuf/mod.rs`): `protox_parse::parse(ref.name, ref.schema)` for each resolved dep (keyed by its import-path `name`) + the candidate; assemble a `FileDescriptorSet`; `prost_reflect::DescriptorPool::from_file_descriptor_set(set)` links imports + validates cross-file types; canonical form = the candidate file descriptor's bytes (source-info + filename cleared, including its `dependency` list).
- **JSON** (`format/json/{mod,diff}.rs`): resolved refs → a `name → serde_json::Value` map; the diff/compat walk resolves a `$ref` whose target matches a reference `name` against that map (intra-document `#/...` resolution is unchanged). Canonical form stays the key-sorted schema as-written (cp does not inline JSON refs).

### Store model (`store/mod.rs`)

- `by_id` carries references: `BTreeMap<i32, RegisteredSchema { ty, schema, references: Vec<SchemaReference> }>`.
- The dedup/id-assignment key is `(canonical_form, references)` — same text + different refs ⇒ different id.
- Forward: each version's references are stored (already on `SchemaValue`). Reverse: `referenced_by(subject, version, include_deleted) -> Vec<i32>` returns the ids of (qualifying) schemas whose references include `(subject, version)` — a scan, or a `BTreeMap<(String,i32), Vec<i32>>` reverse index maintained on `apply_schema`.

### Facade (`kafkastore/mod.rs`)

- `register(subject, ty, schema, references, import_id, import_version)` — validates + resolves the closure (via the store), runs ref-aware compat + canonical, persists `references` in the SCHEMA record. IMPORT/READONLY gating unchanged.
- **Delete-protection:** `soft_delete_version` / `permanent_delete_version` / `*_subject` first check `referenced_by` (live) is empty for the target version(s); non-empty ⇒ `SrError::ReferencedByOthers` (cp ~`42206`). (A soft-deleted referrer does not protect — cp releases the hold; cp-captured.)

## REST surface (`rest/`)

- `POST /subjects/{s}/versions` — parse `references: [{name, subject, version}]` from the body into `Vec<SchemaReference>` and thread to `register` (no longer ignored).
- `GET /schemas/ids/{id}` — add `references` to the response when non-empty.
- `GET /subjects/{s}/versions/{v}` (and `?deleted`) — add `references` when non-empty.
- `GET /subjects/{s}/versions/{v}/referencedby` — return the real `[schema_id, …]` (cp shape, captured) instead of `[]`.
- `POST /subjects/{s}` (lookup) and `POST /compatibility/...` — accept + thread `references` so lookup/compat of a ref-using schema resolves.

No new routes (the `referencedby` route already exists from slice 3).

## Error model (`error.rs`) — cp-captured codes

New `SrError` variants, codes pinned to the cp capture:
- `ReferenceNotFound` — a referenced `(subject, version)` doesn't exist (expected HTTP 422, ~`42201`/a reference-specific code).
- `ReferencedByOthers` — delete blocked because a live schema references the target (expected HTTP 422, ~`42206`).

Body shape stays `{"error_code":N,"message":"…"}`; `error_code` + HTTP status are the contract (message text best-effort, per the slice-2 precedent).

## Validation

- **`capture_references_fixtures.rs`** (`#[ignore]`, Docker, modeled on the slice-2/3 capture harnesses): drive cp 7.4.0 through, for each format: register a base schema, register a dependent schema that references it (Protobuf `import`, Avro named-type, JSON `$ref`), and capture (a) the assigned **ids** (proving refs are part of identity), (b) the **`_schemas` SCHEMA-value bytes** with a non-empty `references` array, (c) the **`referencedby`** response shape, (d) the **delete-protection** error code (delete the base while referenced → rejected), and (e) the **`ReferenceNotFound`** code (reference a missing subject/version). → `tests/fixtures/references/*.json`. Inspect + commit.
- **In-process integration tests** (no Docker, Mac-friendly, single broker): the full lifecycle per format — register base, register referrer (resolves + gets an id), `referencedby` lists the referrer, GET includes `references`, delete-base is rejected while referenced and succeeds after the referrer is removed, and a missing-reference register is rejected. These run in the `schema-registry-integration` CI job.
- **Per-format resolution unit tests** — a ref-using schema in each format parses + canonicalizes + compat-checks against the resolved bundle (e.g., a proto importing a message; an Avro record whose field type is a referenced named record; a JSON schema `$ref`-ing a referenced subject).
- **Record round-trip unit tests** — `encode_schema` with references round-trips through `decode` and matches the captured cp `references` byte-shape.

## File structure / sequencing

`format/mod.rs` (+ `ResolvedReference`, widened seam), `format/avro.rs`, `format/protobuf/mod.rs`, `format/json/{mod,diff}.rs`, `compat/mod.rs` (resolve closure + thread refs), `store/mod.rs` (refs in `by_id` + dedup key + `referenced_by` + `resolve_closure`), `kafkastore/{mod,record}.rs` (register-with-refs + delete-protection + encode), `rest/{subjects,schemas}.rs` (thread refs + GET + referencedby), `error.rs` (2 variants), `tests/{integration,capture_references_fixtures}.rs`, `tests/fixtures/references/`.

**Implementation batches** (within the slice): (1) `ResolvedReference` + widened format seam (all call sites pass `&[]`) + store model (refs in `by_id`, dedup key, `resolve_closure`, `referenced_by`) + facade register-with-refs + existence-validation + encode + reader + the format-agnostic infra (referencedby real, delete-protection, GET includes refs, error variants) — **plus Avro resolution** end-to-end. (2) Protobuf resolution (`prost-reflect` `DescriptorPool` link). (3) JSON resolution (registry-`$ref`). (4) cp capture (Docker) + error-code/shape calibration + integration tests across all three formats.

## Out of scope

- Cross-registry / remote-URL references (only intra-registry `(subject, version)` refs).
- Reference version `-1`/"latest" auto-pinning beyond what cp does (cp pins a concrete version; capture confirms).
- Schema "contexts", import/export of reference graphs.
- HA (slice 5), security (slice 6), operator (slice 7).

## Risks

1. **Per-format resolution fidelity** — the core risk. Avro `parse_list` ordering/namespaces, the Protobuf import-path↔reference-`name` mapping (the `name` IS the import path), and JSON registry-`$ref` semantics each have edge cases; the cp capture is the authority (cp wins on disagreement).
2. **`prost-reflect` `DescriptorPool` linking** — must accept a `FileDescriptorSet` of `protox_parse`-produced descriptors and resolve imports + well-known types. If it can't link a case cp accepts, fall back to adding the `protox` compiler (documented) — but the design's first bet is no-new-dep.
3. **Reference identity in canonical form** — id-assignment over `(canonical, refs)` must match cp (same text + different refs ⇒ different id; same text + same refs ⇒ same id). cp-captured.
4. **Transitive cycles** — a recursive/cyclic reference graph must terminate (visited-set guard) and resolve deterministically.
5. **Delete-protection scope** — exactly which deletes are blocked, and whether a soft-deleted referrer still holds, are cp-captured (treat cp as authority).

## Dependencies

No new crates (the bet: `prost-reflect 0.16` links Protobuf imports; `apache-avro 0.21` `parse_list` resolves Avro; `serde_json` for JSON). The Docker capture uses the existing `testcontainers` + `cp-schema-registry:7.4.0` setup. If risk #2 forces it, adding the `protox` compiler is the documented fallback (a workspace-dep change gated by cargo-deny).
