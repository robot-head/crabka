# Crabka Schema Registry — Slice 4 (schema references) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make schema references real across all three formats — accept + validate references on register, **resolve** the referenced schemas so ref-using schemas parse/canonicalize/compat-check correctly (Avro named-types, Protobuf `import`, JSON `$ref`), expose `referencedby` real data, protect referenced versions from deletion, and include `references` in GET responses — calibrated to `cp-schema-registry 7.4.0`.

**Architecture:** Resolve-then-pass-a-bundle. The facade/compat layer resolves the (transitive, cycle-guarded) reference closure from the store into a `&[ResolvedReference]`, then passes it into a widened `format::parse`/`check`/`normalized_storage_form` seam; format modules stay pure (store-agnostic functions of schema + resolved refs). The schema **id** is assigned over `(canonical form + references)`, so the store's dedup key + `by_id` carry references, plus a `referenced_by` reverse index.

**Tech Stack:** Rust 2024; `apache-avro 0.21` (`Schema::parse_list` for Avro refs), `prost-reflect 0.16` (`DescriptorPool::from_file_descriptor_set` links Protobuf imports — **no new dep**), `protox-parse 0.9`, `serde_json`. Tests: per-format resolution unit tests, broker-backed integration tests, a `#[ignore]` Docker capture against cp 7.4.0.

---

## Design reference

Spec: `docs/superpowers/specs/2026-06-05-crabka-schema-registry-slice-4-references-design.md`. Read it.

### Verified existing signatures (grounded in the current tree, post-slice-3)
```rust
// format/mod.rs
pub enum SchemaType { Avro, Protobuf, Json }   // wire_name()/from_wire()
pub trait ParsedSchema { fn canonical_form(&self) -> String; }
pub fn parse(ty, schema: &str) -> Result<Box<dyn ParsedSchema>, SrError>          // → + refs
pub fn normalized_storage_form(ty, schema: &str) -> Result<String, SrError>       // → + refs
pub fn check(ty, reader: &str, writer: &str) -> Result<(), Vec<String>>           // → + reader_refs, writer_refs

// format/avro.rs   pub struct AvroSchema(apache_avro::Schema);   parse(&str), check(reader,writer)
//   apache_avro::Schema::parse_str / parse_list ; .canonical_form()
// format/protobuf/mod.rs   pub struct ProtobufSchema { descriptor: FileDescriptorProto, normalised: String }
//   parse(&str)->ProtobufSchema (protox_parse::parse("schema.proto", schema)); normalize(&fdp); .descriptor(); .normalized_form()
//   uses prost_reflect::prost::Message, prost_reflect::prost_types::{FileDescriptorProto, ...}
//   check(reader,writer): diff::compare(writer.descriptor(), reader.descriptor())
// format/json/mod.rs   pub struct JsonSchema(serde_json::Value);  parse(&str); .value(); canonicalize(&Value); check(reader,writer)
//   diff::compare(writer.value(), reader.value()) ; diff.rs has resolve_ref (intra-doc #/... only)

// store/mod.rs
struct VersionEntry { version, id, deleted }
struct StoreState { subjects: BTreeMap<String,Vec<VersionEntry>>, by_id: BTreeMap<i32,(SchemaType,String)>,
                    by_canonical: BTreeMap<String,i32>, global_compat, subject_compat, global_mode, subject_mode, max_id }
pub fn register(&mut self, subject, ty, schema) -> Result<Registered,SrError>     // → + references param
pub fn apply_schema(&mut self, &SchemaKey, value: &SchemaValue)                   // value.references now consumed
fn find_under_subject_canonical(&self, subject, canonical, include_deleted)        // canonical → combined dedup key
pub fn versions_schemas(&self, subject) -> Vec<(SchemaType,String)>               // → + Vec<SchemaReference> per entry
pub fn schema_by_id(&self, id, include_deleted) -> Option<(SchemaType,String)>    // → + Vec<SchemaReference>
pub fn version(&self, subject, Option<i32>, include_deleted) -> Option<(i32,i32,SchemaType,String)>  // → + refs
pub fn all_schemas(&self, include_deleted) -> Vec<(String,i32,i32,SchemaType,String)>  // → + refs
pub fn find_under_subject(&self, subject, ty, schema, include_deleted) -> Option<Registered>  // → + references

// compat/mod.rs
fn check_pair(ty, candidate, existing, dirs, out)   // calls format::check(ty,reader,writer) → + refs
pub fn check_registration(snap, subject, ty, candidate) -> Result<(),SrError>     // → + candidate_refs
pub fn check_against_version(snap, subject, ty, candidate, Option<i32>) -> Result<Verdict,SrError>  // → + candidate_refs

// kafkastore/record.rs
pub struct SchemaReference { pub name: String, pub subject: String, pub version: i32 }   // EXISTS
pub struct SchemaValue { subject, version, id, schema_type, references: Vec<SchemaReference>, schema, deleted }
fn schema_kv(subject, version, id, ty, schema, deleted) -> (Vec<u8>,Vec<u8>)      // → + references
pub fn encode_schema(subject, version, id, ty, schema) -> (Vec<u8>,Vec<u8>)       // → + references
pub fn encode_schema_deleted(...) ; pub fn decode(key, value)

// kafkastore/mod.rs (facade)
pub async fn register(&self, subject, ty, schema, import_id, import_version) -> Result<Registered,SrError>  // → + references
//   delete methods: soft_delete_version, permanent_delete_version, soft_delete_subject, permanent_delete_subject

// rest/subjects.rs   RegisterBody { schema, schema_type, references: Vec<serde_json::Value> (IGNORED), id, version }
//   register handler → st.store.register(&subject, ty, &req.schema, req.id, req.version)
//   referencedby(State, Path((subject,version))) -> Ok(ok_json(&json!([])))   // stub
//   get_version, lookup ; rest/schemas.rs get_by_id
// rest/compatibility.rs   check(...) → format::parse(ty,&req.schema)? + compat::check_against_version(..)
// error.rs   SrError { ... InvalidSchema(42201), ... }  + slice-3 variants
```

### Branch / commit / gate discipline (executors read this)
- Worktree: `/Users/mattstone/git/crabka/.claude/worktrees/musing-cartwright-7af144`. Branch: `claude/schema-registry-slice-4` (assert NOT main). Always `git -C <worktree>`. Do NOT push (controller handles push/PR; stacks on slice-3 PR #407).
- Commits: `git -C <worktree> -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com"`; never `git config`; body ends `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- **Per change before commit:** `cargo clippy -p crabka-schema-registry --all-targets -- -D warnings` + `cargo fmt -p crabka-schema-registry`. `git add` only the task's files.
- **Greenfield (CLAUDE.md):** change all call sites cleanly, no shims. Every task leaves the crate compiling + all tests green (compat conformance Avro 21 / Protobuf 88 / JSON 92 + slice 1/2/3 must stay green).
- **cp is authority** for exact error codes, `referencedby` shape, `_schemas` `references` bytes, and id-assignment-with-refs — pinned in Task 7's capture.

---

## File structure
```
crates/schema-registry/src/
  format/mod.rs            # + ResolvedReference; widen parse/check/normalized_storage_form (refs)
  format/avro.rs           # parse_with_refs via Schema::parse_list; check_with_refs
  format/protobuf/mod.rs   # parse_with_refs via DescriptorPool link; check_with_refs
  format/json/{mod,diff}.rs# registry-$ref resolution (name→Value map) in parse/check/diff
  compat/mod.rs            # resolve closure + thread refs through check_registration/check_against_version
  store/mod.rs             # RegisteredSchema{ty,schema,references}; dedup key incl refs; resolve_closure; referenced_by
  kafkastore/record.rs     # encode_schema carries references
  kafkastore/mod.rs        # register(+references); delete-protection
  kafkastore/reader.rs     # (apply_schema already folds value.references via store)
  rest/subjects.rs         # parse references; thread; referencedby real; GET includes refs
  rest/schemas.rs          # GET /schemas/ids/{id} includes references
  rest/compatibility.rs    # accept + thread candidate references
  error.rs                 # ReferenceNotFound, ReferencedByOthers
crates/schema-registry/tests/
  integration.rs                    # + references lifecycle tests (per format)
  capture_references_fixtures.rs    # NEW #[ignore] Docker capture → fixtures
  fixtures/references/*.json        # NEW captured cp ground truth
```

## Execution tasks (sequential; one implementer per task)
- **Task 1** — `ResolvedReference` + widen the format seam; all call sites pass `&[]` (formats ignore refs). Mechanical; compiles + green.
- **Task 2** — store model: `RegisteredSchema` (refs in `by_id`), dedup key incl refs, `resolve_closure`, `referenced_by`, `register(+references)`, `apply_schema` folds refs; `encode_schema(+references)` + reader; `SrError::{ReferenceNotFound, ReferencedByOthers}`.
- **Task 3** — facade `register(+references)` (validate existence + ref-aware compat/canonical + persist) + delete-protection; REST: parse refs + thread, `referencedby` real, GET includes refs, lookup + compatibility accept refs.
- **Task 4** — Avro resolution (`Schema::parse_list`) end-to-end + tests.
- **Task 5** — Protobuf resolution (`prost-reflect` `DescriptorPool` link) + tests.
- **Task 6** — JSON registry-`$ref` resolution + tests.
- **Task 7** — cp Docker capture + error-code/shape/id calibration + cross-format integration tests + record round-trip vs fixtures.

---

## Task 1: `ResolvedReference` + widen the format seam

**Files:** Modify `src/format/mod.rs`, `src/format/avro.rs`, `src/format/protobuf/mod.rs`, `src/format/json/mod.rs`, `src/compat/mod.rs`, `src/store/mod.rs`, `src/rest/compatibility.rs`.

> The seam gains a `refs: &[ResolvedReference]` parameter that the formats **ignore** for now (Tasks 4–6 wire each format's resolution). Every call site passes `&[]`, so behavior is unchanged and all existing tests stay green. This isolates the mechanical signature change.

- [ ] **Step 1: Add `ResolvedReference` + widen the seam in `format/mod.rs`.** Replace the `parse`, `normalized_storage_form`, and `check` fns and add the struct:
```rust
/// A referenced schema resolved from the store, ready to feed a format parser.
/// `name` is the format-specific reference label (Protobuf import path, Avro
/// type name, JSON `$ref` target); `ty`/`schema` are the referenced version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedReference {
    pub name: String,
    pub ty: SchemaType,
    pub schema: String,
}

/// Parse `schema` as `ty` with its resolved references available, returning a
/// boxed parsed form or `SrError::InvalidSchema`.
pub fn parse(
    ty: SchemaType,
    schema: &str,
    refs: &[ResolvedReference],
) -> Result<Box<dyn ParsedSchema>, SrError> {
    match ty {
        SchemaType::Avro => avro::parse(schema, refs).map(|p| Box::new(p) as Box<dyn ParsedSchema>),
        SchemaType::Json => json::parse(schema, refs).map(|p| Box::new(p) as Box<dyn ParsedSchema>),
        SchemaType::Protobuf => {
            protobuf::parse(schema, refs).map(|p| Box::new(p) as Box<dyn ParsedSchema>)
        }
    }
}

pub fn normalized_storage_form(
    ty: SchemaType,
    schema: &str,
    refs: &[ResolvedReference],
) -> Result<String, SrError> {
    match ty {
        SchemaType::Avro | SchemaType::Json => {
            parse(ty, schema, refs)?;
            Ok(schema.to_string())
        }
        SchemaType::Protobuf => {
            let p = protobuf::parse(schema, refs)?;
            Ok(p.normalized_form().to_string())
        }
    }
}

pub fn check(
    ty: SchemaType,
    reader: &str,
    writer: &str,
    reader_refs: &[ResolvedReference],
    writer_refs: &[ResolvedReference],
) -> Result<(), Vec<String>> {
    match ty {
        SchemaType::Avro => avro::check(reader, writer, reader_refs, writer_refs),
        SchemaType::Protobuf => protobuf::check(reader, writer, reader_refs, writer_refs),
        SchemaType::Json => json::check(reader, writer, reader_refs, writer_refs),
    }
}
```
Update the `format/mod.rs` unit tests that call `parse(ty, schema)` → `parse(ty, schema, &[])` (the `avro_parses_and_dedups...` and `avro_rejects_invalid` tests).

- [ ] **Step 2: Widen each format's `parse`/`check` to accept (and ignore) refs.**
  - `avro.rs`: `pub fn parse(schema: &str, _refs: &[super::ResolvedReference]) -> Result<AvroSchema, SrError>` (body unchanged); `pub fn check(reader, writer, _reader_refs: &[super::ResolvedReference], _writer_refs: &[super::ResolvedReference]) -> Result<(), Vec<String>>` (body unchanged). Update avro.rs's own `check(new, old)` unit-test calls → `check(new, old, &[], &[])`.
  - `protobuf/mod.rs`: `pub fn parse(schema: &str, _refs: &[super::ResolvedReference]) -> Result<ProtobufSchema, SrError>`; `pub fn check(reader, writer, _reader_refs: &[super::ResolvedReference], _writer_refs: &[super::ResolvedReference])`. The internal `check` calls `parse(reader)`/`parse(writer)` → `parse(reader, &[])`/`parse(writer, &[])`. Update protobuf tests' `parse(P)`→`parse(P, &[])` and `check(a, b)`→`check(a, b, &[], &[])`.
  - `json/mod.rs`: `pub fn parse(schema: &str, _refs: &[super::ResolvedReference]) -> Result<JsonSchema, SrError>`; `pub fn check(reader, writer, _reader_refs: &[super::ResolvedReference], _writer_refs: &[super::ResolvedReference])`; internal `check` `parse(reader)`→`parse(reader, &[])` etc. Update json/mod.rs tests.

- [ ] **Step 3: Update `compat/mod.rs` call sites.** `check_pair` calls `format::check(ty, reader, writer)`. Widen `check_pair` to thread refs:
```rust
fn check_pair(
    ty: SchemaType,
    candidate: &str,
    candidate_refs: &[crate::format::ResolvedReference],
    existing: &str,
    existing_refs: &[crate::format::ResolvedReference],
    dirs: &[Direction],
    out: &mut Vec<String>,
) {
    for dir in dirs {
        let (reader, writer, reader_refs, writer_refs) = match dir {
            Direction::NewReadsOld => (candidate, existing, candidate_refs, existing_refs),
            Direction::OldReadsNew => (existing, candidate, existing_refs, candidate_refs),
        };
        if let Err(msgs) = format::check(ty, reader, writer, reader_refs, writer_refs) {
            out.extend(msgs);
        }
    }
}
```
In `check_registration` and `check_against_version`, for this task pass `&[]` for all ref args (Task 3 threads real refs). The widened signatures of `check_registration`/`check_against_version` themselves come in Task 3 — for now keep their existing signatures and pass `&[]` into `check_pair`. (i.e. `check_pair(ty, candidate, &[], vschema, &[], dirs, &mut msgs)`.)

- [ ] **Step 4: Update `store/mod.rs` + `rest/compatibility.rs` call sites (pass `&[]`).**
  - `store/mod.rs`: `register`'s `format::parse(ty, schema)` → `format::parse(ty, schema, &[])`; `apply_schema`'s `format::parse(ty, &value.schema)` → `format::parse(ty, &value.schema, &[])`; `find_under_subject`'s `format::parse(ty, schema)` → `..., &[])`. (Task 2 replaces these with resolved bundles.)
  - `rest/compatibility.rs`: `crate::format::parse(ty, &req.schema)?` → `crate::format::parse(ty, &req.schema, &[])?`.
  - Grep `git -C <wt> grep -n "format::parse(\|format::check(\|normalized_storage_form("` across `src/` and fix every call to the new arity (pass `&[]`). `kafkastore/mod.rs`'s `format::normalized_storage_form(ty, schema)` and `format::parse(ty, schema)` (IMPORT path) → `..., &[])` for now (Task 3 threads refs).

- [ ] **Step 5: Run + commit.** `cargo test -p crabka-schema-registry --lib --test integration --test compat_conformance --test interop` → all green (unchanged behavior). `cargo clippy -p crabka-schema-registry --all-targets -- -D warnings` + `cargo fmt`. Commit (`src/format/`, `src/compat/mod.rs`, `src/store/mod.rs`, `src/rest/compatibility.rs`):
`schema-registry: widen format seam with ResolvedReference (no-op; refs threaded later)`

---

## Task 2: store model — references in identity + `resolve_closure` + `referenced_by` + record encode

**Files:** Modify `src/store/mod.rs`, `src/kafkastore/record.rs`, `src/error.rs`.

> The store now carries references in `by_id`, makes the dedup key `(canonical, references)`, resolves the reference closure, and answers `referenced_by`. `register` gains a `references` param and resolves internally. Reader-side `apply_schema` folds `value.references`. The two new `SrError` variants land here (used by Tasks 3+).

- [ ] **Step 1: Add error variants (`error.rs`).** After the slice-3 variants:
```rust
    /// A registration referenced a (subject, version) that does not exist.
    #[error("Reference {0} not found.")]
    ReferenceNotFound(String),
    /// A delete was blocked because a live schema still references the target.
    #[error("One or more references exist to the schema {0}.")]
    ReferencedByOthers(String),
```
In `error_code`, seed (cp-confirmed in Task 7): `Self::ReferenceNotFound(_) => 42201, Self::ReferencedByOthers(_) => 42206,`. In `http_status`, add both to the `UNPROCESSABLE_ENTITY` arm. Add a `references_codes` unit test asserting `42201`/`42206` + `StatusCode::UNPROCESSABLE_ENTITY`.

- [ ] **Step 2: Write failing store unit tests** (append to `store/mod.rs` `mod tests`):
```rust
    use crate::kafkastore::record::SchemaReference;

    fn sref(name: &str, subject: &str, version: i32) -> SchemaReference {
        SchemaReference { name: name.into(), subject: subject.into(), version }
    }

    #[test]
    fn same_schema_different_refs_gets_distinct_id() {
        let mut s = StoreState::default();
        s.register("base", SchemaType::Avro, &av("Base"), &[]).unwrap();
        // two registrations, identical text, different references → different ids
        let r1 = s.register("a", SchemaType::Avro, &av("A"), &[]).unwrap();
        let r2 = s
            .register("b", SchemaType::Avro, &av("A"), &[sref("base", "base", 1)])
            .unwrap();
        assert_ne!(r1.id, r2.id, "refs are part of id identity");
    }

    #[test]
    fn resolve_closure_is_transitive_and_cycle_guarded() {
        let mut s = StoreState::default();
        s.register("base", SchemaType::Avro, &av("Base"), &[]).unwrap();
        s.register("mid", SchemaType::Avro, &av("Mid"), &[sref("base", "base", 1)])
            .unwrap();
        // closure of [mid] includes mid AND its transitive base
        let closure = s.resolve_closure(&[sref("mid", "mid", 1)]).unwrap();
        let names: Vec<&str> = closure.iter().map(|r| r.name.as_str()).collect();
        assert!(names.contains(&"mid") && names.contains(&"base"));
        // missing reference → ReferenceNotFound
        assert!(s.resolve_closure(&[sref("x", "nope", 1)]).is_err());
    }

    #[test]
    fn referenced_by_lists_referrers() {
        let mut s = StoreState::default();
        s.register("base", SchemaType::Avro, &av("Base"), &[]).unwrap(); // id1 base/v1
        let r = s
            .register("dep", SchemaType::Avro, &av("Dep"), &[sref("base", "base", 1)])
            .unwrap();
        assert_eq!(s.referenced_by("base", 1, false), vec![r.id]);
        assert!(s.referenced_by("base", 99, false).is_empty());
    }
```
ALSO update the existing store-test `register` calls to the new arity: every `s.register(subj, ty, &schema)` → `s.register(subj, ty, &schema, &[])` (there are ~10 in `mod tests`).

- [ ] **Step 3: Run — expect FAIL** (signature + methods missing): `cargo test -p crabka-schema-registry --lib store` → compile error.

- [ ] **Step 4: Add `RegisteredSchema` + change `by_id`.** Replace the `by_id` field type and add the struct:
```rust
/// A registered schema's stored form: type + text + its references (references
/// are part of the id identity, so they live with the schema in `by_id`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredSchema {
    pub ty: SchemaType,
    pub schema: String,
    pub references: Vec<crate::kafkastore::record::SchemaReference>,
}
```
In `StoreState`, change `by_id: BTreeMap<i32, (SchemaType, String)>` → `by_id: BTreeMap<i32, RegisteredSchema>`.

- [ ] **Step 5: Add the dedup-key helper + `resolve_closure` + `referenced_by`** (in `impl StoreState`):
```rust
    /// The id-dedup key: canonical form joined with a stable fingerprint of the
    /// references (so identical text with different refs gets a distinct id).
    fn dedup_key(canonical: &str, references: &[SchemaReference]) -> String {
        if references.is_empty() {
            return canonical.to_string();
        }
        let mut refs: Vec<String> = references
            .iter()
            .map(|r| format!("{}\u{1}{}\u{1}{}", r.name, r.subject, r.version))
            .collect();
        refs.sort();
        format!("{canonical}\u{0}{}", refs.join("\u{2}"))
    }

    /// Resolve a reference list into its transitive closure of
    /// `ResolvedReference`s (depth-first, declared order, dedup-by-name keeping
    /// first, cycle-guarded by `(subject, version)`). `ReferenceNotFound` if any
    /// referenced `(subject, version)` is absent.
    pub fn resolve_closure(
        &self,
        references: &[SchemaReference],
    ) -> Result<Vec<crate::format::ResolvedReference>, SrError> {
        let mut out = Vec::new();
        let mut seen_names = std::collections::BTreeSet::new();
        let mut visited = std::collections::BTreeSet::new();
        self.resolve_into(references, &mut out, &mut seen_names, &mut visited)?;
        Ok(out)
    }

    fn resolve_into(
        &self,
        references: &[SchemaReference],
        out: &mut Vec<crate::format::ResolvedReference>,
        seen_names: &mut std::collections::BTreeSet<String>,
        visited: &mut std::collections::BTreeSet<(String, i32)>,
    ) -> Result<(), SrError> {
        for r in references {
            let key = (r.subject.clone(), r.version);
            if !visited.insert(key) {
                continue; // cycle / already expanded this (subject,version)
            }
            let reg = self
                .by_id
                .get(&self.id_of(&r.subject, r.version).ok_or_else(|| {
                    SrError::ReferenceNotFound(format!("{}:{}:{}", r.name, r.subject, r.version))
                })?)
                .ok_or_else(|| {
                    SrError::ReferenceNotFound(format!("{}:{}:{}", r.name, r.subject, r.version))
                })?
                .clone();
            // transitive refs first so dependencies precede dependents
            self.resolve_into(&reg.references, out, seen_names, visited)?;
            if seen_names.insert(r.name.clone()) {
                out.push(crate::format::ResolvedReference {
                    name: r.name.clone(),
                    ty: reg.ty,
                    schema: reg.schema,
                });
            }
        }
        Ok(())
    }

    /// The id of a concrete `(subject, version)`, or `None`. Considers deleted
    /// versions (a reference can name a soft-deleted version's content).
    fn id_of(&self, subject: &str, version: i32) -> Option<i32> {
        self.subjects
            .get(subject)?
            .iter()
            .find(|v| v.version == version)
            .map(|v| v.id)
    }

    /// Ids of (qualifying) schemas whose references include `(subject, version)`.
    #[must_use]
    pub fn referenced_by(&self, subject: &str, version: i32, include_deleted: bool) -> Vec<i32> {
        let mut ids = Vec::new();
        for vs in self.subjects.values() {
            for entry in vs {
                if !(include_deleted || !entry.deleted) {
                    continue;
                }
                if let Some(reg) = self.by_id.get(&entry.id) {
                    if reg
                        .references
                        .iter()
                        .any(|r| r.subject == subject && r.version == version)
                        && !ids.contains(&entry.id)
                    {
                        ids.push(entry.id);
                    }
                }
            }
        }
        ids.sort_unstable();
        ids
    }
```

- [ ] **Step 6: Rewrite `register` to take + resolve references.** Replace `register`:
```rust
    pub fn register(
        &mut self,
        subject: &str,
        ty: SchemaType,
        schema: &str,
        references: &[SchemaReference],
    ) -> Result<Registered, SrError> {
        let resolved = self.resolve_closure(references)?;
        let canonical = format::parse(ty, schema, &resolved)?.canonical_form();
        let key = Self::dedup_key(&canonical, references);
        if let Some(existing) = self.find_under_subject_canonical(subject, &key, true) {
            return Ok(existing);
        }
        let id = if let Some(&id) = self.by_canonical.get(&key) {
            id
        } else {
            let id = self.max_id + 1;
            self.max_id = id;
            self.by_canonical.insert(key, id);
            self.by_id.insert(
                id,
                RegisteredSchema {
                    ty,
                    schema: schema.to_string(),
                    references: references.to_vec(),
                },
            );
            id
        };
        let next_version = self
            .subjects
            .get(subject)
            .map_or(1, |v| i32::try_from(v.len()).unwrap_or(i32::MAX) + 1);
        self.subjects
            .entry(subject.to_string())
            .or_default()
            .push(VersionEntry {
                version: next_version,
                id,
                deleted: false,
            });
        Ok(Registered {
            id,
            version: next_version,
        })
    }
```

- [ ] **Step 7: Rewrite `apply_schema` to fold references + the ref-aware dedup key.** Replace `apply_schema`:
```rust
    pub fn apply_schema(&mut self, _key: &SchemaKey, value: &SchemaValue) {
        let ty = SchemaType::from_wire(value.schema_type.as_deref());
        self.max_id = self.max_id.max(value.id);
        self.by_id.entry(value.id).or_insert_with(|| RegisteredSchema {
            ty,
            schema: value.schema.clone(),
            references: value.references.clone(),
        });
        // Compute the dedup key only if the references resolve (they should, as
        // referenced versions are applied at lower offsets); tolerate failure.
        if let Ok(resolved) = self.resolve_closure(&value.references) {
            if let Ok(p) = format::parse(ty, &value.schema, &resolved) {
                let key = Self::dedup_key(&p.canonical_form(), &value.references);
                self.by_canonical.entry(key).or_insert(value.id);
            }
        }
        let entry = self.subjects.entry(value.subject.clone()).or_default();
        if let Some(e) = entry.iter_mut().find(|v| v.version == value.version) {
            e.deleted = value.deleted;
        } else {
            entry.push(VersionEntry {
                version: value.version,
                id: value.id,
                deleted: value.deleted,
            });
            entry.sort_by_key(|v| v.version);
        }
    }
```

- [ ] **Step 8: Update the deleted-aware queries to carry references.** `by_id` values are now `RegisteredSchema`. Update each reader:
  - `schema_by_id(id, include_deleted) -> Option<(SchemaType, String, Vec<SchemaReference>)>`: return `(reg.ty, reg.schema.clone(), reg.references.clone())` (reference-liveness check unchanged).
  - `version(subject, version, include_deleted) -> Option<(i32, i32, SchemaType, String, Vec<SchemaReference>)>`: append `reg.references.clone()`.
  - `versions_schemas(subject) -> Vec<(SchemaType, String, Vec<SchemaReference>)>`: `(reg.ty, reg.schema.clone(), reg.references.clone())`.
  - `all_schemas(include_deleted) -> Vec<(String, i32, i32, SchemaType, String, Vec<SchemaReference>)>`: append `reg.references.clone()`.
  - `find_under_subject(subject, ty, schema, include_deleted)` — gains a `references: &[SchemaReference]` param; compute the key the same way: `let resolved = self.resolve_closure(references).ok()?; let canonical = format::parse(ty, schema, &resolved).ok()?.canonical_form(); self.find_under_subject_canonical(subject, &Self::dedup_key(&canonical, references), include_deleted)`.
  - Any internal use of `self.by_id.get(&id)` that destructured `(ty, schema)` → now `RegisteredSchema { ty, schema, .. }`.
  Update the slice-1/2/3 store unit tests that assert these tuple shapes (e.g. `schema_by_id(1, false).unwrap().1` still indexes the schema string at `.1`; `version(..).0` still the id — the appended `references` is the new last element, so existing `.0/.1` indexing is unaffected; only the destructuring-into-N-tuples sites need the extra binding).

- [ ] **Step 9: `kafkastore/record.rs` — `encode_schema` carries references.** Add a `references` param to `schema_kv` and `encode_schema` (and a matching `encode_schema_deleted`):
```rust
fn schema_kv(
    subject: &str, version: i32, id: i32, ty: SchemaType, schema: &str,
    references: &[SchemaReference], deleted: bool,
) -> (Vec<u8>, Vec<u8>) {
    let key = SchemaKey::new(subject, version);
    let value = SchemaValue {
        subject: subject.to_string(), version, id,
        schema_type: ty.wire_name().map(str::to_string),
        references: references.to_vec(),
        schema: schema.to_string(), deleted,
    };
    ( serde_json::to_vec(&key).expect("key serialises"),
      serde_json::to_vec(&value).expect("value serialises") )
}

#[must_use]
pub fn encode_schema(subject, version, id, ty, schema, references: &[SchemaReference]) -> (Vec<u8>, Vec<u8>) {
    schema_kv(subject, version, id, ty, schema, references, false)
}
#[must_use]
pub fn encode_schema_deleted(subject, version, id, ty, schema, references: &[SchemaReference]) -> (Vec<u8>, Vec<u8>) {
    schema_kv(subject, version, id, ty, schema, references, true)
}
```
Add a round-trip test: `encode_schema("s",1,1,SchemaType::Avro,"{}",&[SchemaReference{name:"n".into(),subject:"b".into(),version:1}])` → `decode` → `SchemaRecord::Schema(_, val)` with `val.references == [that ref]`.

- [ ] **Step 10: Run — expect PASS:** `cargo test -p crabka-schema-registry --lib` → store + record tests pass. (Callers of `register`/`encode_schema`/`schema_by_id`/`version`/`versions_schemas`/`find_under_subject` in `kafkastore/mod.rs`, `compat/mod.rs`, `rest/` will FAIL to compile — that's Task 3. To keep THIS task's commit compiling, also do the minimal call-site arity bumps below.)

- [ ] **Step 11: Minimal call-site fixes so the crate compiles** (full threading is Task 3, but the crate must build now): in `kafkastore/mod.rs` `register`'s `record::encode_schema(...)` calls → add `&[]`; `probe.register(subject, ty, schema)` → `probe.register(subject, ty, schema, &[])`; `find_under_subject(subject, ty, schema, false)` → `..., &[], false)`. `kafkastore/mod.rs` `soft_delete_version`'s `encode_schema_deleted(...)` → add the version's `references` (fetch via `version(.., true)`'s new last tuple element) — or `&[]` as a placeholder Task 3 corrects. In `compat/mod.rs`, `versions_schemas` now yields 3-tuples and `version` 5-tuples — update the destructuring (bind the new `refs` but pass `&[]` to `check_pair` for now). In `rest/subjects.rs`/`rest/schemas.rs`, update the `schema_by_id`/`version`/`find_under_subject` destructuring to the new tuple arity (bind `_references` / ignore for now). Grep `git -C <wt> grep -n "\.register(\|encode_schema\|schema_by_id\|\.version(\|versions_schemas\|find_under_subject\|all_schemas"` and fix every site. The crate must `cargo build --tests` clean.

- [ ] **Step 12: Run + commit.** `cargo test -p crabka-schema-registry --lib --test integration --test compat_conformance --test interop` → green (Avro 21/Protobuf 88/JSON 92 unchanged; references default `&[]` everywhere so behavior is identical). clippy + fmt. Commit (`src/store/mod.rs`, `src/kafkastore/record.rs`, `src/error.rs`, + the call-site files touched in Step 11):
`schema-registry: references in store id-identity + resolve_closure + referenced_by + record encode`

---

## Task 3: facade register-with-references + delete-protection + REST threading

**Files:** Modify `src/kafkastore/mod.rs`, `src/rest/subjects.rs`, `src/rest/schemas.rs`, `src/rest/compatibility.rs`; test `src/kafkastore/record.rs` is untouched; tests in `tests/integration.rs`.

- [ ] **Step 1: Write failing integration tests** (append to `tests/integration.rs`; `boot_registry`/`register`/`get_json`/`body_json`/`req_post`/`req_delete`/`av` exist). These use **Avro** (resolution lands in Task 4, but reference *bookkeeping* — validation, referencedby, delete-protection, GET — is format-agnostic and works now with a self-contained Avro referrer):
```rust
fn av_named(name: &str, field_type: &str) -> String {
    format!(
        r#"{{"type":"record","name":"{name}","fields":[{{"name":"f","type":"{field_type}"}}]}}"#
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_references_lifecycle_avro() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    // base schema
    register(&app, "base", &format!(r#"{{"schema":{:?}}}"#, av_named("Base", "int"))).await;
    // referrer carries a reference to base v1 (Avro referrer kept self-contained
    // so it parses pre-Task-4; the reference is validated + recorded regardless)
    let body = format!(
        r#"{{"schema":{:?},"references":[{{"name":"Base","subject":"base","version":1}}]}}"#,
        av_named("Dep", "long")
    );
    let r = app.clone().oneshot(req_post("/subjects/dep/versions", &body)).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let dep_id = body_json(r).await["id"].as_i64().unwrap();
    // referencedby lists the referrer's id
    let refby = get_json(&app, "/subjects/base/versions/1/referencedby").await;
    assert_eq!(refby, serde_json::json!([dep_id]));
    // GET the referrer includes references
    let got = get_json(&app, "/subjects/dep/versions/1").await;
    assert_eq!(got["references"][0]["subject"], "base");
    // delete-protection: deleting base v1 while referenced is rejected
    let blocked = app.clone().oneshot(req_delete("/subjects/base/versions/1")).await.unwrap();
    assert_eq!(blocked.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body_json(blocked).await["error_code"], 42206);
    // remove the referrer, then base deletes fine
    assert_eq!(app.clone().oneshot(req_delete("/subjects/dep/versions/1")).await.unwrap().status(), StatusCode::OK);
    assert_eq!(app.clone().oneshot(req_delete("/subjects/base/versions/1")).await.unwrap().status(), StatusCode::OK);
    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_reference_not_found_rejected() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    let body = format!(
        r#"{{"schema":{:?},"references":[{{"name":"Nope","subject":"nope","version":1}}]}}"#,
        av_named("Dep", "int")
    );
    let r = app.clone().oneshot(req_post("/subjects/dep/versions", &body)).await.unwrap();
    assert_eq!(r.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body_json(r).await["error_code"], 42201);
    cancel.cancel();
    broker.shutdown().await;
}
```

- [ ] **Step 2: Run — expect FAIL** (refs ignored / referencedby stub): `cargo test -p crabka-schema-registry --test integration rest_references rest_reference_not_found` → fails.

- [ ] **Step 3: Facade `register` takes references.** In `kafkastore/mod.rs`, add a `references: &[SchemaReference]` param (after `schema`, before `import_id`). Use `crate::kafkastore::record::SchemaReference`. Body changes:
  - Normalise as today.
  - IMPORT branch: validate references exist (`self.store.read().resolve_closure(references)?`), then `record::encode_schema(subject, version, id, ty, schema, references)`.
  - Non-IMPORT: dedup `find_under_subject(subject, ty, schema, references, false)`; `compat::check_registration(&self.store.read(), subject, ty, schema, references)?` (Task threads candidate refs); probe `probe.register(subject, ty, schema, references)?`; `record::encode_schema(subject, reg.version, reg.id, ty, schema, references)`.
  The full signature: `pub async fn register(&self, subject: &str, ty: SchemaType, schema: &str, references: &[SchemaReference], import_id: Option<i32>, import_version: Option<i32>) -> Result<Registered, SrError>`.

- [ ] **Step 4: Thread candidate refs through `compat`.** Widen `compat::check_registration` + `check_against_version` to take `candidate_refs: &[ResolvedReference]`. The facade resolves once: `let resolved = self.store.read().resolve_closure(references)?;` and passes `&resolved`. In `check_registration`, `versions_schemas` now yields `(ty, schema, refs)` per version; resolve each existing version's refs (`snap.resolve_closure(&v_refs)?`) and pass to `check_pair(ty, candidate, candidate_refs, vschema, &existing_resolved, dirs, &mut msgs)`. Same in `check_against_version` for the single target version. (Resolution failures on an existing stored version are unexpected; map to `SrError::Backend` or skip — keep it simple: `.unwrap_or_default()` the existing-version closure since it was valid at register time.)

- [ ] **Step 5: Delete-protection in the facade.** In `soft_delete_version` / `permanent_delete_version`, after the existence/soft checks, add: `if !self.store.read().referenced_by(subject, version, false).is_empty() { return Err(SrError::ReferencedByOthers(format!("{subject}:{version}"))); }`. In `soft_delete_subject` / `permanent_delete_subject`, reject if ANY of the subject's live versions is referenced: `for v in &versions { if !self.store.read().referenced_by(subject, *v, false).is_empty() { return Err(SrError::ReferencedByOthers(format!("{subject}:{v}"))); } }`. Also fix `soft_delete_version`'s `encode_schema_deleted` to pass the version's stored `references` (from `version(.., true)`'s new tuple element) rather than `&[]`.

- [ ] **Step 6: REST threading (`rest/subjects.rs`).**
  - `RegisterBody`: change `references: Vec<serde_json::Value>` → `references: Vec<crate::kafkastore::record::SchemaReference>` with `#[serde(default)]`. (`SchemaReference` already derives `Deserialize`.)
  - `register` handler: `st.store.register(&subject, ty, &req.schema, &req.references, req.id, req.version).await?`.
  - `lookup` handler: parse `references` from the body; `s.find_under_subject(&subject, ty, &req.schema, &req.references, q.deleted)`.
  - `get_version`: include `references` when non-empty — the `version(..)` tuple now ends with `references`; `if !references.is_empty() { m.insert("references".into(), serde_json::to_value(&references).unwrap()); }`.
  - `referencedby`: return real ids: `let ids = st.store.store.read().referenced_by(&subject, version_num, false); Ok(ok_json(&ids))` (resolve `latest`/numeric `version` to a concrete number first via `parse_version` + `version(.., true)`; 404 if the subject/version is absent, matching the slice-3 validation).

- [ ] **Step 7: REST threading (`rest/schemas.rs`, `rest/compatibility.rs`).**
  - `rest/schemas.rs` `get_by_id`: the `schema_by_id` tuple now ends with `references`; add them to the response when non-empty (mirror the `get_version` shape).
  - `rest/compatibility.rs` `check`: parse `references` from the body into `Vec<SchemaReference>`; `format::parse(ty, &req.schema, &st.store.store.read().resolve_closure(&refs)?)?` to validate the candidate parses with its refs; pass `&refs` to `compat::check_against_version(&snap, &subject, ty, &req.schema, &refs, want)`.

- [ ] **Step 8: Run — expect PASS:** `cargo test -p crabka-schema-registry --test integration rest_references rest_reference_not_found --lib` → the two new tests pass; re-run `--test integration --test compat_conformance --test interop` for no regressions.

- [ ] **Step 9: clippy + fmt + commit** (`src/kafkastore/mod.rs`, `src/compat/mod.rs`, `src/rest/{subjects,schemas,compatibility}.rs`, `tests/integration.rs`):
`schema-registry: facade register-with-references + delete-protection + REST threading + referencedby`

---

## Task 4: Avro reference resolution (`Schema::parse_list`)

**Files:** Modify `src/format/avro.rs`; tests in `src/format/avro.rs` + `tests/integration.rs`.

- [ ] **Step 1: Write a failing unit test** (in `avro.rs` `mod tests`): an Avro record whose field type is a *referenced* named record resolves only when the reference is supplied.
```rust
    #[test]
    fn avro_resolves_named_reference() {
        use crate::format::ResolvedReference;
        // "Money" is defined in a referenced schema; the candidate uses it by name.
        let money = r#"{"type":"record","name":"Money","fields":[{"name":"cents","type":"long"}]}"#;
        let candidate = r#"{"type":"record","name":"Order","fields":[{"name":"price","type":"Money"}]}"#;
        // Without the reference, the named type "Money" is unresolved → parse error.
        assert!(parse(candidate, &[]).is_err());
        // With it, parse succeeds.
        let refs = vec![ResolvedReference { name: "Money".into(), ty: crate::format::SchemaType::Avro, schema: money.into() }];
        assert!(parse(candidate, &refs).is_ok());
    }
```

- [ ] **Step 2: Run — expect FAIL** (parse ignores refs).

- [ ] **Step 3: Implement Avro resolution via `parse_list`.** Replace `avro.rs::parse`:
```rust
pub fn parse(schema: &str, refs: &[super::ResolvedReference]) -> Result<AvroSchema, SrError> {
    if refs.is_empty() {
        return apache_avro::Schema::parse_str(schema)
            .map(AvroSchema)
            .map_err(|e| SrError::InvalidSchema(format!("Avro: {e}")));
    }
    // Dependencies first (so their named types are in scope), candidate last.
    let mut sources: Vec<&str> = refs.iter().map(|r| r.schema.as_str()).collect();
    sources.push(schema);
    let parsed = apache_avro::Schema::parse_list(&sources)
        .map_err(|e| SrError::InvalidSchema(format!("Avro: {e}")))?;
    parsed
        .into_iter()
        .last()
        .map(AvroSchema)
        .ok_or_else(|| SrError::InvalidSchema("Avro: empty parse_list".into()))
}
```
(`parse_list` returns the schemas in input order sharing one namespace; the candidate is last.) Verify the `apache_avro::Schema::parse_list` signature in this version: `cargo doc -p apache-avro --no-deps` or check `~/.cargo` source — it is `parse_list(input: &[&str]) -> AvroResult<Vec<Schema>>` in 0.21. If the exact signature differs (e.g. takes `&[&str]` vs owned), adapt minimally.

- [ ] **Step 4: Implement `avro::check` with refs.** Replace `avro.rs::check`:
```rust
pub fn check(
    reader: &str, writer: &str,
    reader_refs: &[super::ResolvedReference], writer_refs: &[super::ResolvedReference],
) -> Result<(), Vec<String>> {
    let reader_schema = parse(reader, reader_refs).map_err(|e| vec![format!("reader: {e}")])?.0;
    let writer_schema = parse(writer, writer_refs).map_err(|e| vec![format!("writer: {e}")])?.0;
    SchemaCompatibility::can_read(&writer_schema, &reader_schema).map_err(|e| vec![e.to_string()])
}
```
(Make `AvroSchema.0` accessible within the module — it already is; `parse(..).?.0` reads the inner `apache_avro::Schema`.)

- [ ] **Step 5: Add a broker-backed integration test** in `tests/integration.rs` exercising a real Avro reference end-to-end (register base "Money", register referrer "Order" using `Money` by name with a reference, GET id round-trips, compat against the referrer resolves):
```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_avro_reference_resolves_end_to_end() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    let money = r#"{\"type\":\"record\",\"name\":\"Money\",\"fields\":[{\"name\":\"cents\",\"type\":\"long\"}]}"#;
    register(&app, "money", &format!(r#"{{"schema":"{money}"}}"#)).await;
    let order = r#"{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"price\",\"type\":\"Money\"}]}"#;
    let body = format!(r#"{{"schema":"{order}","references":[{{"name":"Money","subject":"money","version":1}}]}}"#);
    let r = app.clone().oneshot(req_post("/subjects/order/versions", &body)).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK, "order resolves Money via reference");
    cancel.cancel();
    broker.shutdown().await;
}
```
(If the JSON escaping is fiddly, build the body with `serde_json::json!` and `.to_string()`.)

- [ ] **Step 6: Run + commit.** `cargo test -p crabka-schema-registry --lib format::avro --test integration rest_avro_reference --test compat_conformance` → green. clippy + fmt. Commit (`src/format/avro.rs`, `tests/integration.rs`): `schema-registry: Avro reference resolution via Schema::parse_list`.

---

## Task 5: Protobuf reference resolution (`prost-reflect` `DescriptorPool` link)

**Files:** Modify `src/format/protobuf/mod.rs`; tests in `protobuf/mod.rs` + `tests/integration.rs`.

- [ ] **Step 1: Write a failing unit test** (in `protobuf/mod.rs` `mod tests`): a proto importing a referenced message links only when the import is supplied.
```rust
    #[test]
    fn protobuf_resolves_import_reference() {
        use crate::format::{ResolvedReference, SchemaType};
        let dep = "syntax = \"proto3\"; package m; message Money { int64 cents = 1; }";
        let candidate = "syntax = \"proto3\"; import \"money.proto\"; message Order { m.Money price = 1; }";
        // With the import provided as a reference (name = import path), it links.
        let refs = vec![ResolvedReference { name: "money.proto".into(), ty: SchemaType::Protobuf, schema: dep.into() }];
        assert!(parse(candidate, &refs).is_ok(), "import resolves");
        // The id-identity (canonical) includes the dependency, so it differs from a no-import proto.
        let plain = parse("syntax = \"proto3\"; message Order { int64 price = 1; }", &[]).unwrap();
        assert_ne!(parse(candidate, &refs).unwrap().canonical_form(), plain.canonical_form());
    }
```
> Note: `protox_parse` alone does NOT fail on an unresolved import (it records the dependency but doesn't link). The new behavior is that we **link** via `DescriptorPool`, which validates the import resolves; an import with NO matching reference must error.

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Implement Protobuf resolution.** Replace `protobuf/mod.rs::parse` (keep `normalize`/`write_*`/`proto_type_name` and the `ProtobufSchema` struct):
```rust
use prost_reflect::DescriptorPool;
use prost_reflect::prost_types::FileDescriptorSet;

pub fn parse(schema: &str, refs: &[super::ResolvedReference]) -> Result<ProtobufSchema, SrError> {
    let descriptor = protox_parse::parse("schema.proto", schema)
        .map_err(|e| SrError::InvalidSchema(format!("Protobuf: {e}")))?;
    if !refs.is_empty() || !descriptor.dependency.is_empty() {
        // Link the candidate + its (protobuf) references so imports resolve and
        // cross-file types validate. The reference `name` IS the import path.
        let mut files: Vec<FileDescriptorProto> = Vec::new();
        for r in refs.iter().filter(|r| r.ty == super::SchemaType::Protobuf) {
            let dep = protox_parse::parse(&r.name, &r.schema)
                .map_err(|e| SrError::InvalidSchema(format!("Protobuf reference {}: {e}", r.name)))?;
            files.push(dep);
        }
        files.push(descriptor.clone());
        DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: files })
            .map_err(|e| SrError::InvalidSchema(format!("Protobuf link: {e}")))?;
    }
    let normalised = normalize(&descriptor);
    Ok(ProtobufSchema { descriptor, normalised })
}
```
(`prost_reflect 0.16` exposes `DescriptorPool::from_file_descriptor_set(FileDescriptorSet) -> Result<DescriptorPool, DescriptorError>` and re-exports `prost_types::FileDescriptorSet`. Confirm the exact path: it may be `prost_reflect::prost_types::FileDescriptorSet` — match the existing `use prost_reflect::prost_types::{...}` style.) The candidate's `canonical_form()` is unchanged (descriptor bytes incl. the `dependency` list → references already affect it).

- [ ] **Step 4: Implement `protobuf::check` with refs.** Replace:
```rust
pub fn check(
    reader: &str, writer: &str,
    reader_refs: &[super::ResolvedReference], writer_refs: &[super::ResolvedReference],
) -> Result<(), Vec<String>> {
    let reader_d = parse(reader, reader_refs).map_err(|e| vec![format!("reader: {e}")])?;
    let writer_d = parse(writer, writer_refs).map_err(|e| vec![format!("writer: {e}")])?;
    let diffs = diff::compare(writer_d.descriptor(), reader_d.descriptor());
    let incompatible: Vec<&diff::Difference> =
        diffs.iter().filter(|d| !compat::is_backward_compatible(&d.kind)).collect();
    if incompatible.is_empty() { Ok(()) } else { Err(compat::messages(&incompatible)) }
}
```
(The diff keys fields by `type_name`, so an imported-message-typed field compares by its fully-qualified name — sufficient for the cp-calibrated cases. If Task 7's capture reveals a case needing the linked pool's resolved types, extend the diff then.)

- [ ] **Step 5: Add a broker-backed integration test** (register base proto `m.Money`, register `Order` importing `money.proto` referencing it). Mirror the Avro Step-5 shape with `schemaType: "PROTOBUF"`.

- [ ] **Step 6: Run + commit.** `cargo test -p crabka-schema-registry --lib format::protobuf --test integration rest_protobuf_reference --test compat_conformance` → green (Protobuf 88 unchanged). clippy + fmt. Commit. If `DescriptorPool` linking cannot match a cp case (Task 7), the documented fallback is to add the `protox` compiler dep — flag it as `DONE_WITH_CONCERNS` and report.

---

## Task 6: JSON Schema reference resolution (registry-`$ref`)

**Files:** Modify `src/format/json/mod.rs`, `src/format/json/diff.rs`; tests in `json/mod.rs` + `tests/integration.rs`.

- [ ] **Step 1: Write a failing unit test** (in `json/mod.rs` `mod tests`): a `$ref` whose target matches a reference `name` resolves against the supplied bundle (affecting compat, not canonical).
```rust
    #[test]
    fn json_resolves_registry_ref_in_compat() {
        use crate::format::{ResolvedReference, SchemaType};
        // The referenced schema constrains an integer to maximum 10; the candidate
        // `$ref`s it. A writer with a looser/incompatible shape is caught only when
        // the ref resolves.
        let dep = r#"{"type":"integer","maximum":10}"#;
        let refs = vec![ResolvedReference { name: "Amount".into(), ty: SchemaType::Json, schema: dep.into() }];
        let with_ref = r#"{"type":"object","properties":{"a":{"$ref":"Amount"}}}"#;
        // canonical form is the schema as-written (refs not inlined)
        assert_eq!(parse(with_ref, &refs).unwrap().canonical_form(), parse(with_ref, &[]).unwrap().canonical_form());
        // check resolves the ref: reader == writer with the ref present is compatible
        assert!(check(with_ref, with_ref, &refs, &refs).is_ok());
    }
```

- [ ] **Step 2: Run — expect FAIL.**

- [ ] **Step 3: Thread refs into `json::parse`/`check` + the diff.**
  - `json/mod.rs` `parse(schema, refs)`: parse as today (canonical ignores refs); store the refs on `JsonSchema` for the diff. Change `pub struct JsonSchema(serde_json::Value);` → `pub struct JsonSchema { value: serde_json::Value, refs: Vec<(String, serde_json::Value)> }` with `value()` returning `&self.value` and a `refs()` accessor returning `&[(String, serde_json::Value)]`; in `parse`, build `refs` by `serde_json::from_str(&r.schema)` for each `ResolvedReference` (skip unparseable).
  - `json/mod.rs` `check(reader, writer, reader_refs, writer_refs)`: `diff::compare_with_refs(writer_s.value(), reader_s.value(), writer_s.refs(), reader_s.refs())`.
  - `json/diff.rs`: add `pub fn compare_with_refs(original, update, original_refs: &[(String, Value)], update_refs: &[(String, Value)]) -> Vec<Difference>` that seeds the recursion with the ref maps; extend `resolve_ref` so a `$ref` string that is NOT `#…` and matches a `name` in the side's ref map resolves to that `Value` (intra-doc `#/…` resolution unchanged; an unmatched non-`#` ref stays permissive/`None` as today). Keep the existing `compare(original, update)` as `compare_with_refs(.., &[], &[])` so the conformance tests (which call `compare`) are unaffected.

- [ ] **Step 4: Run** `cargo test -p crabka-schema-registry --lib format::json --test compat_conformance` → the new test passes; JSON 92 conformance unchanged (it calls `compare` with no refs).

- [ ] **Step 5: Add a broker-backed integration test** (register base JSON schema, register a referrer `$ref`-ing it by name) mirroring Avro Step-5 with `schemaType: "JSON"`.

- [ ] **Step 6: clippy + fmt + commit** (`src/format/json/{mod,diff}.rs`, `tests/integration.rs`): `schema-registry: JSON Schema registry-$ref resolution`.

---

## Task 7: cp Docker capture + calibration + cross-format integration

**Files:** Create `tests/capture_references_fixtures.rs` + `tests/fixtures/references/*.json`; calibrate `src/error.rs` (codes) + `src/kafkastore/record.rs`/`rest/` (shapes) if cp differs; tests in `tests/integration.rs`.

- [ ] **Step 1: Write the `#[ignore]` Docker capture harness** `tests/capture_references_fixtures.rs`, modeled on `tests/capture_admin_fixtures.rs` (copy the broker + docker scaffolding verbatim). For each format, drive cp 7.4.0:
  - **Avro:** register `money` (`record Money{cents:long}`), register `order` (`record Order{price:Money}`) with `references:[{name:"Money",subject:"money",version:1}]`.
  - **Protobuf:** register `money` (`package m; message Money{int64 cents=1;}`), register `order` (`import "money.proto"; message Order{m.Money price=1;}`) with `references:[{name:"money.proto",subject:"money",version:1}]`.
  - **JSON:** register `amount` (`{type:integer,maximum:10}`), register `order` (`{type:object,properties:{a:{$ref:"Amount"}}}`) with `references:[{name:"Amount",subject:"amount",version:1}]`.
  Capture per op: the register **status + id**, `GET /schemas/ids/{order_id}` (the `references` array shape), `GET /subjects/money/versions/1/referencedby` (the **shape** — expected `[order_id]`), a delete of the referenced base (the **delete-protection code**), and a register with a **missing** reference (the **ReferenceNotFound code**). Then dump the `_schemas` records (reuse the admin harness's `dump_schemas_records`) → capture the SCHEMA value with a non-empty `references` array. Write `tests/fixtures/references/{avro,protobuf,json}.json` + `records.json`.

- [ ] **Step 2: Run the capture (Docker):** `cargo test -p crabka-schema-registry --test capture_references_fixtures -- --ignored --nocapture`. **If Docker is unavailable, STOP and report — the controller runs the capture.** Inspect + report: the assigned ids (refs ⇒ identity), the `referencedby` shape, the delete-protection code, the ReferenceNotFound code, and the `_schemas` `references` byte-shape.

- [ ] **Step 3: CALIBRATE.** For each cp value that differs from the seed:
  - **error codes** — fix `error.rs` `error_code`/`http_status` for `ReferenceNotFound`/`ReferencedByOthers` to match the captured codes; update the `references_codes` unit test.
  - **`referencedby` shape** — if cp returns objects (`[{subject,version}]`) rather than bare ids, adjust `rest/subjects.rs::referencedby` + the store query; if bare ids, confirm.
  - **`references` record bytes** — confirm `encode_schema`'s `references` field order/shape matches cp's SCHEMA value; fix `SchemaReference`/`schema_kv` if not. Add a `references_match_cp_capture` round-trip test asserting the captured bytes.
  - **id identity** — confirm same-text-different-refs got different ids in the capture (validates the `dedup_key`).
  Report every change (seed → cp).

- [ ] **Step 4: Cross-format integration assertions** — extend `tests/integration.rs` with the calibrated lifecycle per format (already have Avro from Task 3/4; add Protobuf + JSON referrer lifecycle tests asserting the **calibrated** codes + `referencedby` shape + GET `references`). Ensure each: base→referrer resolves+id, `referencedby` lists the referrer, GET includes `references`, delete-base rejected while referenced then succeeds after the referrer is gone, missing-ref rejected.

- [ ] **Step 5: Run everything** (no Docker): `cargo test -p crabka-schema-registry --lib --test integration --test compat_conformance --test interop` → all green; conformance (Avro 21/Protobuf 88/JSON 92) unchanged. clippy + fmt.

- [ ] **Step 6: Commit** (`tests/capture_references_fixtures.rs`, `tests/fixtures/references/`, `src/error.rs` + any calibrated `record.rs`/`rest/`, `tests/integration.rs`): `schema-registry: cp-calibrated references (codes/shapes/ids) + cross-format integration + capture`.

---

## Self-review (completed by plan author)

**Spec coverage:**
- `ResolvedReference` + widened seam (`parse`/`normalized_storage_form`/`check` take refs) → Task 1.
- Per-format resolution: Avro `parse_list` → Task 4; Protobuf `DescriptorPool` link (no new dep) → Task 5; JSON registry-`$ref` → Task 6.
- Store model: refs in `by_id` (`RegisteredSchema`), dedup key `(canonical, refs)`, `resolve_closure` (transitive + cycle-guarded + `ReferenceNotFound`), `referenced_by` → Task 2.
- Facade register-with-refs (validate + ref-aware compat/canonical + persist) + delete-protection → Task 3; compat threading → Tasks 1+3.
- REST: parse references + thread (register/lookup/compatibility), `referencedby` real, GET (`/schemas/ids/{id}` + version) include refs → Task 3.
- `encode_schema` carries refs → Task 2.
- Error model (`ReferenceNotFound`, `ReferencedByOthers`) → Task 2 (seed) + Task 7 (cp-calibrate).
- Validation: cp capture (ids/referencedby/delete-protection/ReferenceNotFound/`_schemas` bytes) → Task 7; in-process lifecycle per format → Tasks 3/4/5/6/7; per-format resolution unit tests → Tasks 4/5/6; record round-trip → Task 2 + Task 7.
- Out of scope honored: no cross-registry refs; no contexts/export; HA/security/operator are slices 5–7.

**Placeholder scan:** the only "seed then calibrate" items are the error codes (42201/42206) and the `referencedby`/record shapes, explicitly cp-confirmed in Task 7 (the spec's authority discipline) — not unfilled placeholders. Every code step shows complete code; mechanical call-site bumps (Task 1 Step 4, Task 2 Step 11) are enumerated as grep-and-fix with the exact new arity.

**Type consistency:** `ResolvedReference { name, ty, schema }` and `SchemaReference { name, subject, version }` are used consistently; the widened seam (`parse(ty, schema, refs)`, `check(ty, reader, writer, reader_refs, writer_refs)`, `normalized_storage_form(ty, schema, refs)`), the store API (`register(.., references)`, `resolve_closure(&[SchemaReference]) -> Vec<ResolvedReference>`, `referenced_by(subject, version, include_deleted) -> Vec<i32>`, `RegisteredSchema`, the refs-appended tuples from `schema_by_id`/`version`/`versions_schemas`/`all_schemas`/`find_under_subject`), the facade `register(.., references, import_id, import_version)`, `compat::{check_registration, check_against_version}(.., candidate_refs)`, `encode_schema(.., references)`, and `SrError::{ReferenceNotFound, ReferencedByOthers}` line up across tasks. Each task ends compiling + green (Task 1 no-op; Task 2 bumps call-site arity so the crate builds before Task 3 threads real refs).

**Gaps fixed during review:** Task 2 Step 11 explicitly bumps every changed-signature call site so the crate compiles at the end of Task 2 (the store-API arity change would otherwise break `kafkastore`/`compat`/`rest`); the Avro referrer in Task 3's bookkeeping test is kept self-contained (parses pre-Task-4) so reference *validation/referencedby/delete-protection* is testable before Avro *resolution* lands; the Protobuf diff is noted to compare imported-message fields by `type_name` (FQN) with a documented escalation if cp needs the linked pool.
