# Crabka Schema Registry — Slice 3: deletes, modes, lookups — design

- **Date:** 2026-06-05
- **Status:** Approved (brainstorm); ready for an implementation plan
- **Builds on:** slices 1+2+2b+2c (registry + full compatibility trilogy, all merged-pending). The store, `_schemas` record types, `KafkaStore` facade (write-gate + read-your-writes), compat engine, and axum REST surface all exist. Stacks on slice 2c (PR #400) for the shared store/record/REST/test files.
- **Parent roadmap:** `docs/superpowers/specs/2026-06-04-crabka-schema-registry-design.md` (slice 3).

## Motivation

The registry can register/fetch/list and enforce compatibility, but cannot **delete**, cannot be put in **read-only / import** modes, and lacks a few **lookup** endpoints. Slice 3 fills the "registry CRUD completeness" gap: soft + permanent delete (with Confluent's soft-before-hard rule and `?deleted` visibility), the `READWRITE`/`READONLY`/`IMPORT` modes, and the `/schemas/ids/{id}/versions`, `/schemas`, and `referencedby` lookups — all calibrated to `cp-schema-registry 7.4.0`.

## Load-bearing decisions

| Decision | Choice | Rationale |
|---|---|---|
| **Scope** | All of slice 3 in one PR — deletes + modes + lookups. | The features are coupled (modes gate deletes/writes; `?deleted` filters lists; the new `_schemas` record families land together); one slice avoids re-touching store/record/REST three times. |
| **Delete model** | Per-version `deleted` flag (soft) + record tombstones (permanent); a subject is "live" iff it has ≥1 non-deleted version. | Matches Confluent: soft delete hides + retains, permanent delete removes. |
| **Permanent requires soft first** | `permanent_delete_*` errors unless the target is already soft-deleted. | Confluent's soft-before-hard safety rule. |
| **Modes** | `READWRITE` (default) / `READONLY` / `IMPORT`; subject override > global. | Confluent's mode set; READONLY gates writes, IMPORT enables migration. |
| **Authority** | Exact `_schemas` record bytes, REST behaviors, and numeric error codes are **cp-captured** (Docker) + asserted; behavior validation, not a verdict matrix. | Same fidelity discipline as the compat slices. |
| **referencedby** | Returns `[]` (stub). | Real references are slice 4; nothing references anything yet. |

## Architecture

The change is CRUD/state-management spread across the existing layers (no new top-level module). Direction of data: REST handler → `KafkaStore` facade (validates mode + soft-before-hard, produces the `_schemas` record, waits read-your-writes) → reader replays → `StoreState` mutates → REST reads.

### Store state model (`store/mod.rs`)

- `VersionEntry { version, id, deleted: bool }` — per-version soft-delete flag (was `{version, id}`).
- Modes: `global_mode: Option<String>` (default `READWRITE`), `subject_mode: BTreeMap<String, String>`.
- Mutators (called by the reader on replay): `soft_delete_version`, `permanent_delete_version`, `soft_delete_subject` (flags all versions), `permanent_delete_subject` (removes the subject), `set_global_mode`, `set_subject_mode`, `clear_subject_mode`.
- Deleted-aware queries: `subjects(include_deleted)`, `versions(subject, include_deleted)`, `version(subject, v, include_deleted)`, `schema_by_id(id, include_deleted)`, and `schema_id_subject_versions(id)` → `Vec<(subject, version)>` for `/schemas/ids/{id}/versions`; `all_schemas(include_deleted)` for `GET /schemas`. A subject is **live** iff ≥1 non-deleted version; a soft-deleted subject reappears on re-register.
- Mode resolution: `effective_mode(subject)` = subject override else global else `READWRITE`.

### `_schemas` record families (`kafkastore/record.rs`) — cp-captured shapes

- **Soft version delete** → re-emit the `SCHEMA` record (same key) with `deleted: true`.
- **Permanent version delete** → a **tombstone**: the `SCHEMA` key with a **null value**.
- **Soft subject delete** → a `DELETE_SUBJECT` record (`{"keytype":"DELETE_SUBJECT","subject":..,"magic":0}` / `{"subject":..,"version":<n>}`) + per-version `deleted:true`.
- **Permanent subject delete** → per-version tombstones (+ `DELETE_SUBJECT` tombstone).
- **Mode** → a `MODE` record (`{"keytype":"MODE","subject":<null|subj>,"magic":0}` / `{"mode":"READONLY"}`).
- New types: `ModeKey/ModeValue`, `DeleteSubjectKey/DeleteSubjectValue`; new `SchemaRecord` variants `Mode`, `DeleteSubject`, `Tombstone(SchemaKey)` (a SCHEMA key with null value — currently decodes to `Unknown`); `decode` arms for `MODE`/`DELETE_SUBJECT`/`CLEAR_SUBJECTS` + the null-value tombstone; `encode_mode`, `encode_delete_subject`, `encode_tombstone`.

### Reader (`kafkastore/reader.rs`)

`apply_record` gains arms: `Mode` → `set_*_mode`; `DeleteSubject` → flag the subject's versions deleted; a `SCHEMA` record whose value has `deleted:true` → flag that version; `Tombstone(key)` → remove that version (and if the subject is now empty, remove the subject).

### Facade (`kafkastore/mod.rs`)

- `soft_delete_version(subject, v)`, `permanent_delete_version(subject, v)` (errors `SrError::*` if not soft-deleted first), `soft_delete_subject(subject)`, `permanent_delete_subject(subject)`, `set_global_mode`/`set_subject_mode`/`clear_subject_mode`.
- **Mode gating:** `register`, `set_*_compat`, and the delete methods first check `effective_mode(subject)`; `READONLY` → `SrError::OperationNotPermitted` (`42205`). `IMPORT` → `register` accepts explicit `id` + `version` from the request and persists *at* them (skips id-assignment and the compat check); setting `IMPORT` requires the subject to have no versions.
- All paths keep the existing write-gate + read-your-writes.

## REST surface (`rest/`)

New (`rest/delete.rs`, `rest/mode.rs`, extend `rest/{subjects,schemas,mod}.rs`):
```
DELETE /subjects/{subject}/versions/{version}[?permanent=true]   -> <version:int>
DELETE /subjects/{subject}[?permanent=true]                       -> [<versions>]
GET  /mode            PUT /mode                                    -> {"mode": "<M>"}
GET  /mode/{subject}  PUT /mode/{subject}  DELETE /mode/{subject}
GET  /schemas/ids/{id}/versions                                   -> [{"subject":..,"version":..}]
GET  /subjects/{subject}/versions/{version}/referencedby          -> []   (stub; slice 4)
GET  /schemas                                                     -> [{subject,version,id,schemaType,schema}]
```
Modified — honor `?deleted=true` (default false hides soft-deleted): `GET /subjects`, `GET /subjects/{s}/versions`, `GET /subjects/{s}/versions/{v}`, `POST /subjects/{s}` (lookup), `GET /schemas/ids/{id}`. Register/compat `PUT` paths now consult mode. *(`GET /schemas` pagination params `?offset/&limit` are deferred — YAGNI.)*

## Error model (`error.rs`) — cp-captured codes

New `SrError` variants mapped to Confluent's numeric codes + HTTP statuses (pinned against cp; the exact codes for soft-before-hard and READONLY are confirmed in the capture, expected ~`40405` "soft deleted, set permanent=true" HTTP 404 and ~`42205` "operation not permitted / READONLY" HTTP 422). The body shape stays `{"error_code":N,"message":"…"}`.

## Validation

- **`capture_admin_fixtures.rs`** (`#[ignore]`, Docker, modeled on the matrix-capture harnesses): drive a real `cp-schema-registry 7.4.0` through the lifecycle and capture (a) the **`_schemas` record bytes** each op emits (soft-delete SCHEMA-with-`deleted`, `DELETE_SUBJECT`, tombstones, `MODE`) into `tests/fixtures/admin/*.json`, and (b) the **REST responses + numeric error codes** for: register → soft-delete-version → `GET ?deleted` shows it → permanent-delete → gone; subject soft+permanent delete; soft-before-hard 404; `PUT /mode READONLY` then register → 422; `IMPORT` mode register-with-explicit-id. Inspect + commit the fixtures.
- **In-process integration tests** (no Docker, Mac-friendly, single broker): the full delete lifecycle (soft → `?deleted` visibility → permanent → 404), subject delete, the soft-before-hard error, `READONLY` gating (register → 422; mode flip back → 200), `IMPORT` explicit-id registration, and the lookups (`/schemas/ids/{id}/versions`, `/schemas`, `referencedby` = `[]`). These run in the `schema-registry-integration` CI job.
- **Record round-trip unit tests** for the new `encode_*`/`decode` (byte-shape vs the captured fixtures, like slice 1's `schemas_record`).

## File structure / sequencing

`store/mod.rs`, `kafkastore/{record,reader,mod}.rs`, `error.rs`, new `rest/delete.rs` + `rest/mode.rs`, extend `rest/{subjects,schemas,mod}.rs`, `tests/{integration,capture_admin_fixtures}.rs`, `tests/fixtures/admin/`.

**Batches:** (1) record types (Mode/DeleteSubject/Tombstone + encode/decode + round-trip tests) + store model (deleted flag, modes, mutators, deleted-aware queries) + reader apply. (2) facade delete/mode methods + soft-before-hard + READONLY gating + IMPORT register. (3) REST delete + mode endpoints + `?deleted` on the GETs + the lookup endpoints. (4) cp capture (record bytes + behaviors/codes) + error-code calibration + integration tests.

## Out of scope

`referencedby` real data (needs references — slice 4); `GET /schemas` pagination; the `READONLY_OVERRIDE` mode and global-vs-subject mode-precedence corner cases beyond override>global; subject "contexts"; export. HA/election (slice 5), security (slice 6), operator (slice 7).

## Risks

1. **Exact cp error codes + soft-before-hard scope** — which ops require prior soft-delete, and the precise numeric codes — are cp-captured; treat cp as authority.
2. **IMPORT mode** — the only `register`-path change (explicit id/version, compat-skip, subject-must-be-empty). The gnarliest bit; if cp's IMPORT semantics prove fiddly, it can degrade to a documented partial in a 3-followup, but it is in scope.
3. **Tombstone / compaction** — the reader must apply a null-value SCHEMA record as "remove version"; ensure replay from a compacted log (tombstone already gone) still yields the right state.
4. **`?deleted` interactions** — soft-deleted vs permanently-gone vs re-registered subjects must list/fetch consistently with cp.

## Dependencies

No new crates. The Docker capture uses the existing `testcontainers` + `cp-schema-registry:7.4.0` setup; everything else is `serde_json` + the existing client crates.
