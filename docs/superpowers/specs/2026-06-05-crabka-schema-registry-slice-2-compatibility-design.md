# Crabka Schema Registry — Slice 2: compatibility engine (Avro) — design

- **Date:** 2026-06-05
- **Status:** Approved (brainstorm); ready for an implementation plan
- **Builds on:** slice 1 (PR #392) — standalone `crabka-schema-registry` crate, `_schemas` storage, REST surface, `format` module (parse + canonical form), `/config` stored-but-not-enforced, golden-fixture-from-`cp-schema-registry` validation pattern.
- **Parent roadmap:** `docs/superpowers/specs/2026-06-04-crabka-schema-registry-design.md` (slice 2).

## Motivation

Slice 1 stores schemas and `/config` compatibility levels but never *enforces* them — every registration is accepted (`compatibility = NONE`). Slice 2 makes the registry actually guard schema evolution: reject incompatible registrations with HTTP 409, answer real compatibility queries, and honor the stored per-subject / global compatibility level.

## Load-bearing decisions

| Decision | Choice | Rationale |
|---|---|---|
| **Format scope** | Build the full compatibility **engine** + wire **Avro** only. Protobuf → slice **2b**, JSON Schema → slice **2c**. | `apache-avro 0.21` ships a real compatibility checker (`schema_compatibility::SchemaCompatibility`); Protobuf and JSON Schema have **no** Rust library for Confluent's rules and must be hand-rolled. Wiring the library-backed format first gets enforcement working end-to-end at lowest risk and isolates the two hard rule-sets into their own slices. |
| **Engine placement** | A format-agnostic `compat` engine + a per-format `format::check` seam. | The engine owns level/version orchestration; `format::check` owns per-format rules. 2b/2c become drop-in: implement one function each. |
| **Interim non-Avro** | Protobuf/JSON compatibility checks return **permissive (`Ok`)** until 2b/2c. | Keeps slice 1's behavior for those formats; a clearly-documented gap, not a hard error. |
| **Fidelity bar** | The `is_compatible` **boolean** + HTTP code are validated against real `cp-schema-registry` golden fixtures; `messages` text is best-effort. | `apache-avro`'s `CompatibilityError` wording differs from cp's, but the *verdict* must match. |

## Architecture

A new `compat` engine module is the single brain both the register path and the `/compatibility` endpoint call. It resolves the effective level, selects the version set, and delegates the per-pair directional check to the `format` layer.

```
register(POST /subjects/{s}/versions)        POST /compatibility/subjects/{s}/versions/{v}
        │ (new schema, not a dedup)                    │
        ▼                                              ▼
   ┌──────────────────────  compat::engine  ───────────────────────┐
   │ effective level (subject cfg > global cfg) → directions + vers  │
   │ pull prior version schema strings from StoreState               │
   └───────────────┬─────────────────────────────────────────────────┘
                   ▼  format::check(ty, reader_schema, writer_schema) -> Result<(), Vec<String>>
        Avro → apache_avro can_read (one/both directions)   |   Protobuf/JSON → Ok (permissive; 2b/2c)
                   │
        incompatible → SrError::Incompatible (409, register)  /  {"is_compatible":false, messages} (endpoint)
```

### Components (new / changed)

| Unit | Change | Responsibility |
|---|---|---|
| `compat/mod.rs` | **new** | `CompatibilityLevel` enum + parse-from-`&str`; the level→(directions, version-set) matrix; `check_registration(...)` and `check_against_version(...)` orchestration over a `StoreState` snapshot. No I/O, no format internals. |
| `format/mod.rs` | extend | `pub fn check(ty: SchemaType, reader: &str, writer: &str) -> Result<(), Vec<String>>` dispatch. |
| `format/avro.rs` | extend | Avro directional check via `apache_avro::schema_compatibility::SchemaCompatibility::can_read`; map `CompatibilityError` → message strings. |
| `format/{protobuf,json}.rs` | extend | `check(...)` returns `Ok(())` (permissive) — placeholder for 2b/2c. |
| `store/mod.rs` | extend | accessor returning a subject's versions as ordered `(SchemaType, schema_string)` for transitive checks (non-transitive uses the existing `version(subject, None)` = latest). |
| `kafkastore/mod.rs` | extend | `register` calls `compat::check_registration` (between dedup and id assignment); maps incompatibility to `SrError::Incompatible`. |
| `error.rs` | extend | `Incompatible(Vec<String>)` → `error_code` 409, HTTP 409. |
| `rest/compatibility.rs` | replace stub | real verdict + `?verbose=true` messages. |

## The compatibility matrix

`compat` resolves the **effective level** = the subject's `/config` override if set, else the global `/config` level (default `BACKWARD`). It then applies:

| Effective level | Direction(s) | Versions checked |
|---|---|---|
| `NONE` | — | none (always accept) |
| `BACKWARD` | new reads old | **latest** only |
| `BACKWARD_TRANSITIVE` | new reads old | **all** versions |
| `FORWARD` | old reads new | latest only |
| `FORWARD_TRANSITIVE` | old reads new | all versions |
| `FULL` | both | latest only |
| `FULL_TRANSITIVE` | both | all versions |

**Two invariant rules (match Confluent):**
1. The **first** version under a subject always registers — nothing to check against.
2. An identical re-register is **dedup'd before any compat check** (idempotent; already true in slice 1's `register`).

### Avro direction mapping

`apache_avro::schema_compatibility::SchemaCompatibility::can_read(writers_schema, readers_schema) -> Result<(), CompatibilityError>` returns `Ok` iff a reader using `readers_schema` can read data written with `writers_schema`. So:

- **new reads old** (BACKWARD): `can_read(writers = old, readers = new)`
- **old reads new** (FORWARD): `can_read(writers = new, readers = old)`
- **FULL**: both of the above (equivalently `mutual_read`).

`format::check(SchemaType::Avro, reader, writer)` re-parses both strings with `apache_avro::Schema::parse_str` and runs `can_read(writer, reader)`. The engine decides which schema is reader vs. writer per direction, and aggregates failure messages across the version set.

## Register enforcement + 409

In `KafkaStore::register`, inside the existing write-gate, the order becomes:

1. Dedup: if the schema is already registered under the subject → return the existing `{id, version}` (no compat check).
2. Resolve the effective level from the store snapshot. If `NONE` **or** the subject has no prior versions → skip to step 4.
3. `compat::check_registration(snapshot, subject, ty, candidate_schema)`. On `Err(messages)` → return `SrError::Incompatible(messages)` (HTTP 409); **persist nothing**.
4. Assign id/version, persist the SCHEMA record, read-your-writes, return.

`error.rs` gains `Incompatible(Vec<String>)`. Body: `{"error_code":409,"message":"<summary>; details: [...]"}` (the summary string shape will be pinned against a captured cp 409 fixture; the `error_code` is the contract serdes branch on).

## `POST /compatibility/subjects/{s}/versions/{v}`

Replaces the slice-1 always-`true` stub.

- `{v}` is a positive integer or `latest`.
- Parses the posted `{schema, schemaType}`; `42201` if unparseable; `40401`/`40402` if subject/version absent.
- Runs `compat::check_against_version` using the subject's effective level direction(s) against version `{v}`.
- Returns `{"is_compatible": <bool>}`; with `?verbose=true`, also `{"messages": [<reasons>]}` (empty array when compatible). The crate already enables axum's `query` feature (slice 1).

The Confluent "check against **all** versions" endpoint (`POST /compatibility/subjects/{s}/versions`) is **deferred** (YAGNI for slice 2) — recorded in the roadmap.

## Validation

Following slice 1's golden-fixture pattern, with `cp-schema-registry 7.4.0` as the oracle:

1. **Avro verdict matrix** — capture, from a real cp registry, the `is_compatible` verdict for a set of (writer, reader, level) cases over canonical Avro evolutions: add-optional-field (with default), remove-field, add-field-without-default, `int`→`long` promotion, `int`→`string` (incompatible), enum symbol add/remove, union widening/narrowing, record field rename. Commit as fixtures; assert our engine's booleans match for every (case × level).
2. **In-process enforcement tests** — `PUT /config/{subject}` `BACKWARD`; register v1; register an incompatible v2 → **409** `error_code` 409; register a compatible v2 → **200**; flip to `NONE` → the incompatible v2 now registers. Same for `FORWARD`/`FULL` and a `*_TRANSITIVE` case (incompatible only with a *non-latest* version, to prove transitive ≠ latest-only).
3. **`/compatibility` endpoint tests** — compatible/incompatible verdicts, `latest`, `verbose` messages non-empty on failure, `42201` on bad schema.

> Where `apache-avro`'s `can_read` diverges from cp's Avro verdict on an edge case, document it in the fixture README and decide per-case (accept the divergence as a known limitation, or add a compensating pre-check). Divergences are the interesting findings — like slice 1's Protobuf-normalization catch.

> ⚠️ Mac caveat (unchanged): capture/interop tests are Docker-gated `#[ignore]`; the in-process enforcement tests need only a single broker and run locally + in the `schema-registry-integration` CI job.

## Out of scope (slice 2)

Protobuf compatibility (→ 2b), JSON Schema compatibility (→ 2c), the all-versions `/compatibility` endpoint, deletes/modes/references (slices 3–4), schema *content* validation of produced records (never — that's the broker, explicitly not this project).

## Roadmap update

The original roadmap's "slice 2" splits:

- **Slice 2** (this spec): compatibility engine + all 7 levels (incl. transitive) + config enforcement + 409 + real `/compatibility`, wired for **Avro**.
- **Slice 2b**: Protobuf compatibility (hand-rolled Confluent field rules) — fills in `format::check` for Protobuf.
- **Slice 2c**: JSON Schema compatibility (hand-rolled diff rules) — fills in `format::check` for JSON.

Slices 3–7 (deletes/modes/lookups, references, HA, security, operator) are unchanged.

## Open risks

1. **apache-avro ↔ cp Avro-verdict parity** — most cases should match (both implement Avro schema resolution), but default-value handling, named-type aliases, and enum defaults are historical divergence spots. The fixture matrix is the gate; divergences get documented/compensated.
2. **409 body shape** — pin the summary message against a captured cp 409 fixture; only `error_code` 409 is contractually required.
3. **Re-parse cost** — the engine re-parses prior-version schema strings per check. Fine for slice 1/2 scale; if it ever matters, cache parsed `apache_avro::Schema` by id. Not now (YAGNI).

## Dependencies

No new crates — `apache-avro 0.21` (already a dependency) provides `schema_compatibility`. axum `query` feature already enabled.
