# Crabka Schema Registry — Slice 2 (compatibility engine + Avro) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `crabka-schema-registry` enforce schema compatibility — reject incompatible registrations with HTTP 409, answer real `/compatibility` queries, and honor the stored per-subject/global compatibility level — wired for **Avro** via `apache-avro`'s checker (Protobuf/JSON stay permissive until slices 2b/2c).

**Architecture:** A new format-agnostic `compat` engine resolves the effective level (subject `/config` > global), selects the version set (latest vs. all, per `_TRANSITIVE`), and delegates per-pair directional checks to a new `format::check` seam. Avro's `check` calls `apache_avro::schema_compatibility::SchemaCompatibility::can_read`; Protobuf/JSON return `Ok`. `KafkaStore::register` calls the engine between dedup and id assignment; the `/compatibility` endpoint calls it directly.

**Tech Stack:** Rust 2024, `apache-avro 0.21` (`schema_compatibility`, already a dep), axum 0.8 (`query` feature already enabled), the existing slice-1 crate. Tests: in-process broker (`crabka-broker` `test-helpers`), `tower::oneshot`, golden Avro-verdict fixtures from `cp-schema-registry 7.4.0`.

---

## Design reference

Spec: `docs/superpowers/specs/2026-06-05-crabka-schema-registry-slice-2-compatibility-design.md`. Read it first.

### Verified upstream + existing signatures (do not re-derive)

```rust
// apache-avro 0.21
apache_avro::schema_compatibility::SchemaCompatibility::can_read(writers_schema: &Schema, readers_schema: &Schema) -> Result<(), CompatibilityError>
apache_avro::schema_compatibility::SchemaCompatibility::mutual_read(writers: &Schema, readers: &Schema) -> Result<(), CompatibilityError>
apache_avro::Schema::parse_str(s: &str) -> Result<Schema, apache_avro::Error>
// CompatibilityError is a thiserror enum; Display gives a human reason.

// existing crate (crates/schema-registry/src)
// store/mod.rs:
//   StoreState fields: subjects: BTreeMap<String, Vec<VersionEntry{version:i32,id:i32}>>, by_id: BTreeMap<i32,(SchemaType,String)>, global_compat: Option<String>, subject_compat: BTreeMap<String,String>, ...
//   pub fn register(&mut self, subject, ty, schema) -> Result<Registered{id,version}, SrError>   // assumes normalized schema
//   pub fn version(&self, subject, Option<i32>) -> Option<(i32 id, i32 version, SchemaType, String)>   // None = latest
//   pub fn versions(&self, subject) -> Option<Vec<i32>>
//   pub fn global_compat(&self) -> &str   (default "BACKWARD")
//   pub fn subject_compat(&self, subject) -> Option<&str>
//   pub fn find_under_subject(&self, subject, ty, schema) -> Option<Registered>
// format/mod.rs: enum SchemaType{Avro,Protobuf,Json}; from_wire/wire_name; parse(ty,&str)->Result<Box<dyn ParsedSchema>,SrError>; normalized_storage_form(ty,&str)->Result<String,SrError>
// format/avro.rs: pub fn parse(&str) -> Result<AvroSchema, SrError>  (AvroSchema wraps apache_avro::Schema)
// error.rs: enum SrError { SubjectNotFound(String), VersionNotFound, SchemaNotFound, InvalidSchema(String), InvalidVersion(String), InvalidCompatibilityLevel(String), Backend(String) } + error_code()/http_status()/IntoResponse
// kafkastore/mod.rs: KafkaStore::register(&self, subject, ty, schema) -> Result<Registered, SrError>  (write-gate; normalizes; dedups via find_under_subject; probe.register; encode_schema; produce; await_applied)
// rest/compatibility.rs: pub async fn check(State<AppState>, Path((subject,version)):Path<(String,String)>, body:String) -> Result<Response, SrError>   (current stub returns is_compatible:true)
// rest/response.rs: ok_json(&T)->Response (vendor content-type)
```

### Commit & worktree discipline (executors read this)

- Worktree root: `/Users/mattstone/git/crabka/.claude/worktrees/musing-cartwright-7af144`, branch `claude/musing-cartwright-7af144`. Always `git -C <worktree>`; assert branch ≠ `main` before committing.
- Commits use `git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com"`; never `git config`. End the message body with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- **Per change, before commit:** `cargo clippy -p crabka-schema-registry --all-targets -- -D warnings` (workspace `pedantic` lints are CI-fatal) and `cargo fmt -p crabka-schema-registry` (CI gates on `cargo fmt --check`). Do NOT run `cargo fmt --all` or workspace clippy; `git add` only the task's files.
- **Do not push** — slice 2 stacks on the unmerged slice-1 PR #392; the controller manages push/PR.

---

## File structure

```
crates/schema-registry/src/
  lib.rs                  # + `pub mod compat;`
  error.rs                # + SrError::Incompatible(Vec<String>) -> 409 / CONFLICT
  compat/mod.rs           # NEW: CompatibilityLevel + matrix + check_registration + check_against_version + Verdict
  format/mod.rs           # + `pub fn check(ty, reader, writer) -> Result<(), Vec<String>>` dispatch
  format/avro.rs          # + `pub fn check(reader, writer) -> Result<(), Vec<String>>` via can_read
  format/protobuf.rs      # + permissive `pub fn check(...) -> Result<(), Vec<String>>` (Ok)
  format/json.rs          # + permissive `pub fn check(...) -> Result<(), Vec<String>>` (Ok)
  store/mod.rs            # + `pub fn versions_schemas(&self, subject) -> Vec<(SchemaType, String)>`
  kafkastore/mod.rs       # register: call compat::check_registration between dedup and id assignment
  rest/compatibility.rs   # real verdict + ?verbose=true
crates/schema-registry/tests/
  integration.rs          # + enforcement tests (extend; CI already runs --test integration)
  compat_conformance.rs   # NEW: Avro verdict matrix vs committed fixtures (no Docker)
  capture_compat_fixtures.rs  # NEW: #[ignore] Docker capture of the Avro verdict matrix from cp
  fixtures/compat/*.json  # NEW: committed golden verdicts
.github/workflows/ci.yml  # schema-registry-integration: add --test compat_conformance
```

---

## Execution batches

- **Batch A (parallel, disjoint):** Task 1 `error.rs` · Task 2 `store::versions_schemas` · Task 3 `format::check` (mod+avro+protobuf+json).
- **Batch B (sequential):** Task 4 `compat` engine (+ `lib.rs`) — depends on A.
- **Batch C (parallel, disjoint):** Task 5 `kafkastore::register` hook · Task 6 `rest/compatibility.rs` — depend on B.
- **Batch D (sequential):** Task 7 capture compat fixtures (Docker) → Task 8 conformance + enforcement tests + CI.

---

## Task 1: `SrError::Incompatible` (409)

**Files:** Modify `crates/schema-registry/src/error.rs`.

- [ ] **Step 1: Add the failing test** (inside the existing `#[cfg(test)] mod tests`):

```rust
    #[test]
    fn incompatible_is_409_conflict() {
        let e = SrError::Incompatible(vec!["reader missing default".into()]);
        assert_eq!(e.error_code(), 409);
        assert_eq!(e.http_status(), StatusCode::CONFLICT);
        assert!(e.to_string().contains("incompatible"));
    }
```

- [ ] **Step 2: Run / fail.** `cargo test -p crabka-schema-registry error::` → FAIL (no `Incompatible`).

- [ ] **Step 3: Implement.** Add the variant + arms:

```rust
    // in enum SrError:
    /// Schema incompatible with prior version(s) under the subject. The strings
    /// are best-effort reasons (Avro's wording, not Confluent's).
    #[error("Schema being registered is incompatible with an earlier schema; details: {0:?}")]
    Incompatible(Vec<String>),
```
```rust
    // in error_code(): add arm
            Self::Incompatible(_) => 409,
    // in http_status(): add arm
            Self::Incompatible(_) => StatusCode::CONFLICT,
```

- [ ] **Step 4: Run / pass.** `cargo test -p crabka-schema-registry error::`
- [ ] **Step 5: clippy + fmt + commit** (`error.rs` only):
```bash
cargo clippy -p crabka-schema-registry --all-targets -- -D warnings && cargo fmt -p crabka-schema-registry
git -C "$WT" add crates/schema-registry/src/error.rs
git -C "$WT" -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "schema-registry: SrError::Incompatible (409 Conflict)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: `StoreState::versions_schemas`

**Files:** Modify `crates/schema-registry/src/store/mod.rs`.

- [ ] **Step 1: Add the failing test** (in the existing `#[cfg(test)] mod tests`):

```rust
    #[test]
    fn versions_schemas_returns_ordered_pairs() {
        let mut s = StoreState::default();
        s.register("av-value", SchemaType::Avro, &av("A")).unwrap();
        s.register("av-value", SchemaType::Avro, &av("B")).unwrap();
        let vs = s.versions_schemas("av-value");
        assert_eq!(vs.len(), 2);
        assert!(matches!(vs[0].0, SchemaType::Avro));
        assert!(vs[0].1.contains("\"A\""));
        assert!(vs[1].1.contains("\"B\""));
        assert!(s.versions_schemas("missing").is_empty());
    }
```

- [ ] **Step 2: Run / fail.** `cargo test -p crabka-schema-registry store::versions_schemas`

- [ ] **Step 3: Implement** (add a method to `impl StoreState`):

```rust
    /// A subject's versions as ordered `(type, schema)` pairs (ascending
    /// version). Empty if the subject is unknown. Used by the compatibility
    /// engine for transitive checks.
    #[must_use]
    pub fn versions_schemas(&self, subject: &str) -> Vec<(SchemaType, String)> {
        let Some(entries) = self.subjects.get(subject) else {
            return Vec::new();
        };
        entries
            .iter()
            .filter_map(|e| self.by_id.get(&e.id).cloned())
            .collect()
    }
```
(`subjects` entries are kept in ascending version order by `register`/`apply_schema`.)

- [ ] **Step 4: Run / pass.** `cargo test -p crabka-schema-registry store::versions_schemas`
- [ ] **Step 5: clippy + fmt + commit** (`store/mod.rs` only), message `schema-registry: StoreState::versions_schemas accessor`.

---

## Task 3: `format::check` seam (Avro real, Protobuf/JSON permissive)

**Files:** Modify `crates/schema-registry/src/format/mod.rs`, `format/avro.rs`, `format/protobuf.rs`, `format/json.rs`.

- [ ] **Step 1: Add failing tests** in `format/avro.rs` (`#[cfg(test)] mod tests`):

```rust
    #[test]
    fn avro_check_directions() {
        // reader can read writer? add-field-with-default: new (reader) reads old (writer) OK.
        let old = r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"}]}"#;
        let new = r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"},{"name":"x","type":"int","default":0}]}"#;
        assert!(check(new, old).is_ok(), "new reads old (BACKWARD) ok");
        // old (reader) reads new (writer)? old lacks x; new has x w/ default -> reading new data with old schema is OK in Avro (old ignores x). So FORWARD also ok here.
        assert!(check(old, new).is_ok());
        // add-field-WITHOUT-default: new reads old -> new requires x but old data has none -> NOT ok.
        let new_nodef = r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"},{"name":"x","type":"int"}]}"#;
        assert!(check(new_nodef, old).is_err(), "new(reader) cannot read old(writer): missing default");
    }
```
And in `format/protobuf.rs` + `format/json.rs`:
```rust
    #[test]
    fn check_is_permissive_for_now() {
        assert!(check("anything", "anything else").is_ok());
    }
```

- [ ] **Step 2: Run / fail.** `cargo test -p crabka-schema-registry format::`

- [ ] **Step 3: Implement.** `format/avro.rs` — add:

```rust
use apache_avro::schema_compatibility::SchemaCompatibility;

/// Directional Avro check: can a reader using `reader` read data written with
/// `writer`? `Ok(())` if compatible, else `Err(messages)`.
pub fn check(reader: &str, writer: &str) -> Result<(), Vec<String>> {
    let reader_schema = apache_avro::Schema::parse_str(reader)
        .map_err(|e| vec![format!("reader schema unparseable: {e}")])?;
    let writer_schema = apache_avro::Schema::parse_str(writer)
        .map_err(|e| vec![format!("writer schema unparseable: {e}")])?;
    SchemaCompatibility::can_read(&writer_schema, &reader_schema).map_err(|e| vec![e.to_string()])
}
```
`format/protobuf.rs` and `format/json.rs` — add (permissive placeholder for 2b/2c):
```rust
/// Compatibility check. Permissive until slice 2b/2c implement the real rules.
pub fn check(_reader: &str, _writer: &str) -> Result<(), Vec<String>> {
    Ok(())
}
```
`format/mod.rs` — add the dispatch:
```rust
/// Directional compatibility check: can a reader using `reader` read data
/// written with `writer`, per format `ty`? `Err(messages)` on incompatibility.
/// Avro is real (apache-avro); Protobuf/JSON are permissive until 2b/2c.
pub fn check(ty: SchemaType, reader: &str, writer: &str) -> Result<(), Vec<String>> {
    match ty {
        SchemaType::Avro => avro::check(reader, writer),
        SchemaType::Protobuf => protobuf::check(reader, writer),
        SchemaType::Json => json::check(reader, writer),
    }
}
```

- [ ] **Step 4: Run / pass.** `cargo test -p crabka-schema-registry format::`
- [ ] **Step 5: clippy + fmt + commit** (`format/{mod,avro,protobuf,json}.rs`), message `schema-registry: format::check seam (Avro via can_read; Protobuf/JSON permissive)`.

---

## Task 4: the `compat` engine

**Files:** Create `crates/schema-registry/src/compat/mod.rs`; modify `crates/schema-registry/src/lib.rs`.

- [ ] **Step 1: Declare the module.** In `lib.rs`, add (keep alphabetical-ish):
```rust
pub mod compat;
```

- [ ] **Step 2: Write failing tests** in `compat/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::SchemaType;
    use crate::store::StoreState;

    fn av(fields: &str) -> String { format!("{{\"type\":\"record\",\"name\":\"U\",\"fields\":[{fields}]}}") }
    const ID: &str = "{\"name\":\"id\",\"type\":\"int\"}";

    #[test]
    fn level_parse_and_props() {
        assert_eq!(CompatibilityLevel::parse("BACKWARD"), CompatibilityLevel::Backward);
        assert_eq!(CompatibilityLevel::parse("FULL_TRANSITIVE"), CompatibilityLevel::FullTransitive);
        assert!(CompatibilityLevel::FullTransitive.is_transitive());
        assert!(!CompatibilityLevel::Backward.is_transitive());
        assert!(CompatibilityLevel::None.directions().is_empty());
    }

    #[test]
    fn first_version_and_none_always_ok() {
        let snap = StoreState::default();
        // no versions yet -> ok regardless of level
        assert!(check_registration(&snap, "s", SchemaType::Avro, &av(ID)).is_ok());
    }

    #[test]
    fn backward_rejects_added_required_field() {
        let mut snap = StoreState::default();
        snap.set_subject_compat("s", "BACKWARD".into());
        snap.register("s", SchemaType::Avro, &av(ID)).unwrap();
        // add field WITHOUT default -> new(reader) can't read old(writer) -> incompatible
        let bad = av(&format!("{ID},{{\"name\":\"x\",\"type\":\"int\"}}"));
        assert!(matches!(check_registration(&snap, "s", SchemaType::Avro, &bad), Err(crate::error::SrError::Incompatible(_))));
        // add field WITH default -> ok
        let good = av(&format!("{ID},{{\"name\":\"x\",\"type\":\"int\",\"default\":0}}"));
        assert!(check_registration(&snap, "s", SchemaType::Avro, &good).is_ok());
    }

    #[test]
    fn none_level_bypasses() {
        let mut snap = StoreState::default();
        snap.set_subject_compat("s", "NONE".into());
        snap.register("s", SchemaType::Avro, &av(ID)).unwrap();
        let bad = av(&format!("{ID},{{\"name\":\"x\",\"type\":\"int\"}}"));
        assert!(check_registration(&snap, "s", SchemaType::Avro, &bad).is_ok());
    }

    #[test]
    fn check_against_version_verdict() {
        let mut snap = StoreState::default();
        snap.set_subject_compat("s", "BACKWARD".into());
        snap.register("s", SchemaType::Avro, &av(ID)).unwrap();
        let bad = av(&format!("{ID},{{\"name\":\"x\",\"type\":\"int\"}}"));
        let v = check_against_version(&snap, "s", SchemaType::Avro, &bad, None).unwrap();
        assert!(!v.is_compatible);
        assert!(!v.messages.is_empty());
        let good = av(&format!("{ID},{{\"name\":\"x\",\"type\":\"int\",\"default\":0}}"));
        assert!(check_against_version(&snap, "s", SchemaType::Avro, &good, None).unwrap().is_compatible);
        // missing subject -> SubjectNotFound
        assert!(check_against_version(&snap, "nope", SchemaType::Avro, &good, None).is_err());
    }
}
```

- [ ] **Step 3: Run / fail.** `cargo test -p crabka-schema-registry compat::`

- [ ] **Step 4: Implement `compat/mod.rs`:**

```rust
//! Compatibility engine: resolve the effective level, pick the version set, and
//! run the per-format directional check. Format-agnostic (delegates to
//! `format::check`); knows nothing about Avro/Protobuf/JSON internals.

use crate::error::SrError;
use crate::format::{self, SchemaType};
use crate::store::StoreState;

/// Confluent compatibility levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatibilityLevel {
    None,
    Backward,
    BackwardTransitive,
    Forward,
    ForwardTransitive,
    Full,
    FullTransitive,
}

/// One directional check.
#[derive(Debug, Clone, Copy)]
enum Direction {
    /// reader = candidate (new), writer = existing (old).
    NewReadsOld,
    /// reader = existing (old), writer = candidate (new).
    OldReadsNew,
}

/// Result of a `/compatibility` query.
#[derive(Debug, Clone)]
pub struct Verdict {
    pub is_compatible: bool,
    pub messages: Vec<String>,
}

impl CompatibilityLevel {
    /// Parse a stored level string; unknown strings fall back to the global
    /// default `BACKWARD` (the `/config` layer validates input, so unknowns
    /// are not expected).
    #[must_use]
    pub fn parse(s: &str) -> Self {
        match s {
            "NONE" => Self::None,
            "FORWARD" => Self::Forward,
            "FORWARD_TRANSITIVE" => Self::ForwardTransitive,
            "FULL" => Self::Full,
            "FULL_TRANSITIVE" => Self::FullTransitive,
            "BACKWARD_TRANSITIVE" => Self::BackwardTransitive,
            _ => Self::Backward,
        }
    }

    #[must_use]
    pub fn is_transitive(self) -> bool {
        matches!(self, Self::BackwardTransitive | Self::ForwardTransitive | Self::FullTransitive)
    }

    fn directions(self) -> &'static [Direction] {
        match self {
            Self::None => &[],
            Self::Backward | Self::BackwardTransitive => &[Direction::NewReadsOld],
            Self::Forward | Self::ForwardTransitive => &[Direction::OldReadsNew],
            Self::Full | Self::FullTransitive => &[Direction::NewReadsOld, Direction::OldReadsNew],
        }
    }
}

/// Effective level for a subject: per-subject `/config` override if set, else global.
fn effective_level(snap: &StoreState, subject: &str) -> CompatibilityLevel {
    let s = snap.subject_compat(subject).map_or_else(|| snap.global_compat().to_string(), str::to_string);
    CompatibilityLevel::parse(&s)
}

/// Run `candidate` against one existing version's `(ty, schema)` in the given
/// directions, collecting failure messages.
fn check_pair(ty: SchemaType, candidate: &str, existing: &str, dirs: &[Direction], out: &mut Vec<String>) {
    for dir in dirs {
        let (reader, writer) = match dir {
            Direction::NewReadsOld => (candidate, existing),
            Direction::OldReadsNew => (existing, candidate),
        };
        if let Err(msgs) = format::check(ty, reader, writer) {
            out.extend(msgs);
        }
    }
}

/// Enforcement on register: `Ok(())` if compatible / `NONE` / first version;
/// `Err(SrError::Incompatible)` otherwise. `candidate` must be normalised.
pub fn check_registration(snap: &StoreState, subject: &str, ty: SchemaType, candidate: &str) -> Result<(), SrError> {
    let level = effective_level(snap, subject);
    let dirs = level.directions();
    if dirs.is_empty() {
        return Ok(());
    }
    let versions = snap.versions_schemas(subject);
    if versions.is_empty() {
        return Ok(());
    }
    let targets: &[(SchemaType, String)] = if level.is_transitive() {
        &versions
    } else {
        &versions[versions.len() - 1..] // latest only
    };
    let mut msgs = Vec::new();
    for (_vty, vschema) in targets {
        check_pair(ty, candidate, vschema, dirs, &mut msgs);
    }
    if msgs.is_empty() { Ok(()) } else { Err(SrError::Incompatible(msgs)) }
}

/// `/compatibility` query: verdict of `candidate` against version `version`
/// (`None` = latest) under the subject's effective level. `Err` for unknown
/// subject/version.
pub fn check_against_version(
    snap: &StoreState,
    subject: &str,
    ty: SchemaType,
    candidate: &str,
    version: Option<i32>,
) -> Result<Verdict, SrError> {
    if snap.versions(subject).is_none() {
        return Err(SrError::SubjectNotFound(subject.to_string()));
    }
    let (_, _, vty, vschema) = snap.version(subject, version).ok_or(SrError::VersionNotFound)?;
    let _ = vty;
    let level = effective_level(snap, subject);
    let dirs = level.directions();
    if dirs.is_empty() {
        return Ok(Verdict { is_compatible: true, messages: Vec::new() });
    }
    let mut msgs = Vec::new();
    check_pair(ty, candidate, &vschema, dirs, &mut msgs);
    Ok(Verdict { is_compatible: msgs.is_empty(), messages: msgs })
}
```

- [ ] **Step 5: Run / pass.** `cargo test -p crabka-schema-registry compat::`
- [ ] **Step 6: clippy + fmt + commit** (`compat/mod.rs`, `lib.rs`), message `schema-registry: compatibility engine (levels, matrix, registration + version checks)`.

---

## Task 5: hook `KafkaStore::register`

**Files:** Modify `crates/schema-registry/src/kafkastore/mod.rs`. (Verified end-to-end by Task 8's integration tests.)

- [ ] **Step 1: Insert the compat check** in `register`, immediately AFTER the dedup early-return and BEFORE the `probe.register` block:

```rust
        if let Some(existing) = self.store.read().find_under_subject(subject, ty, schema) {
            return Ok(existing);
        }
        // Slice 2: enforce compatibility against existing versions per the
        // subject's effective level. First version / NONE => no-op. Incompatible
        // => SrError::Incompatible (409); nothing is persisted.
        crate::compat::check_registration(&self.store.read(), subject, ty, schema)?;
        // Genuinely new under this subject: decide id/version on a throwaway clone …
        let reg = {
            let mut probe = self.store.read().clone();
            probe.register(subject, ty, schema)?
        };
```
(`schema` here is already the normalised storage form — `register` normalised it above — so the candidate matches what is stored, and `check_registration` compares like-for-like.)

- [ ] **Step 2: Build + clippy.** `cargo build -p crabka-schema-registry && cargo clippy -p crabka-schema-registry --all-targets -- -D warnings`. Expected: clean. (The read-lock in `check_registration(&self.store.read(), …)` is a temporary dropped at the `?`; it is NOT held across the later `.await`.)
- [ ] **Step 3: fmt + commit** (`kafkastore/mod.rs`), message `schema-registry: enforce compatibility on register (409 on incompatible)`.

---

## Task 6: real `/compatibility` endpoint

**Files:** Modify `crates/schema-registry/src/rest/compatibility.rs`. (Verified by Task 8.)

- [ ] **Step 1: Replace the stub** with a real verdict + `?verbose=true`:

```rust
//! Compatibility check endpoint. Slice 2: real verdict via the compat engine,
//! using the subject's effective level against the named version.

use axum::extract::{Path, Query, State};
use axum::response::Response;
use serde::Deserialize;

use crate::compat;
use crate::error::SrError;
use crate::format::SchemaType;
use crate::rest::{AppState, response::ok_json};

#[derive(Deserialize)]
struct Body {
    schema: String,
    #[serde(rename = "schemaType", default)]
    schema_type: Option<String>,
}

#[derive(Deserialize, Default)]
struct VerboseQ {
    #[serde(default)]
    verbose: bool,
}

/// POST /compatibility/subjects/{subject}/versions/{version}
pub async fn check(
    State(st): State<AppState>,
    Path((subject, version)): Path<(String, String)>,
    Query(q): Query<VerboseQ>,
    body: String,
) -> Result<Response, SrError> {
    let req: Body = serde_json::from_str(&body).map_err(|e| SrError::InvalidSchema(e.to_string()))?;
    let ty = SchemaType::from_wire(req.schema_type.as_deref());
    // 42201 if the candidate itself is unparseable (matches Confluent).
    crate::format::parse(ty, &req.schema)?;
    let want = parse_version(&version)?;
    let verdict = {
        let snap = st.store.store.read();
        compat::check_against_version(&snap, &subject, ty, &req.schema, want)?
    };
    if q.verbose {
        Ok(ok_json(&serde_json::json!({
            "is_compatible": verdict.is_compatible,
            "messages": verdict.messages,
        })))
    } else {
        Ok(ok_json(&serde_json::json!({ "is_compatible": verdict.is_compatible })))
    }
}

/// `latest` -> None; a positive integer -> Some(n); else 42202.
fn parse_version(v: &str) -> Result<Option<i32>, SrError> {
    if v == "latest" {
        return Ok(None);
    }
    match v.parse::<i32>() {
        Ok(n) if n >= 1 => Ok(Some(n)),
        _ => Err(SrError::InvalidVersion(v.to_string())),
    }
}
```
Note: the candidate schema is checked in **input form**, not normalised — matches Confluent (the `/compatibility` endpoint does not persist). Avro `can_read` is insensitive to formatting, so this is fine.

- [ ] **Step 2: Build + clippy + fmt.** `cargo build -p crabka-schema-registry && cargo clippy -p crabka-schema-registry --all-targets -- -D warnings && cargo fmt -p crabka-schema-registry`. (Confirm axum `query` feature is enabled — it is, from slice 1.)
- [ ] **Step 3: Commit** (`rest/compatibility.rs`), message `schema-registry: real /compatibility verdict + ?verbose`.

---

## Task 7: capture golden Avro verdict fixtures (Docker)

**Files:** Create `crates/schema-registry/tests/capture_compat_fixtures.rs` + `tests/fixtures/compat/*.json`.

- [ ] **Step 1: Write the `#[ignore]` capture harness** modelled on the existing `tests/capture_fixtures.rs` (read it). Boot a Crabka broker advertising `host.docker.internal`, run `confluentinc/cp-schema-registry:7.4.0`. For each case below, drive cp's REST to get the **ground-truth verdict**: `PUT /config/{subject}` `<level>`; `POST /subjects/{subject}/versions` the `writer`; then `POST /compatibility/subjects/{subject}/versions/latest` the `reader` and record `is_compatible`. Write each `{writer, reader, level, is_compatible}` to `tests/fixtures/compat/<name>.json`.

Cases (Avro), each evaluated under `BACKWARD`, `FORWARD`, `FULL`:
```
base:        record U { int id }
add_default: record U { int id; int x = 0 }
add_nodef:   record U { int id; int x }
remove:      record U { }                     (writer=base, reader=remove)
promote:     id: int -> long
narrow:      id: long -> int
enum_add:    enum E {A} -> enum E {A,B}
enum_remove: enum E {A,B} -> enum E {A}
```
(Encode each as a full Avro schema string. Use distinct subjects per case to isolate.)

- [ ] **Step 2: Run** (Docker): `cargo test -p crabka-schema-registry --test capture_compat_fixtures -- --ignored --nocapture`. Confirm fixtures written under `tests/fixtures/compat/`.
- [ ] **Step 3: Commit** fixtures + harness, message `schema-registry: golden Avro compatibility verdicts from cp-schema-registry 7.4.0`.

> If Docker is unavailable to the executor, STOP and report — the controller has Docker and will run the capture. Do NOT hand-fabricate verdicts.

---

## Task 8: conformance + enforcement tests + CI

**Files:** Create `crates/schema-registry/tests/compat_conformance.rs`; modify `crates/schema-registry/tests/integration.rs`, `.github/workflows/ci.yml`.

- [ ] **Step 1: `compat_conformance.rs`** (no Docker) — for each committed `tests/fixtures/compat/*.json`, parse `{writer, reader, level, is_compatible}` and assert our **engine** agrees. Drive it through the **library** (no broker needed): build a `StoreState`, `set_subject_compat(subject, level)`, `register` the writer, then `compat::check_against_version(&snap, subject, Avro, reader, None).is_compatible` must equal the fixture's `is_compatible`. If any case mismatches (apache-avro ↔ cp divergence), the test fails loudly — record the divergence in `tests/fixtures/compat/README.md` and decide per the spec (accept as known limitation with an `#[allow]`-documented expected-divergence list, or compensate). Use `crabka_schema_registry::{compat, store::StoreState, format::SchemaType}`.

- [ ] **Step 2: Extend `tests/integration.rs`** with enforcement tests (boot in-process broker as the existing tests do):

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compat_enforced_on_register() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    // default global is BACKWARD; register v1
    let base = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"}"#;
    let r = app.clone().oneshot(req_post("/subjects/s/versions", base)).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    // incompatible v2 (added required field) -> 409
    let bad = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"x\",\"type\":\"int\"}]}"}"#;
    let r = app.clone().oneshot(req_post("/subjects/s/versions", bad)).await.unwrap();
    assert_eq!(r.status(), StatusCode::CONFLICT);
    assert_eq!(body_json(r).await["error_code"], 409);
    // compatible v2 (added field WITH default) -> 200
    let good = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"x\",\"type\":\"int\",\"default\":0}]}"}"#;
    let r = app.clone().oneshot(req_post("/subjects/s/versions", good)).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn none_level_bypasses_enforcement() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    // set subject config NONE
    app.clone().oneshot(req_put("/config/s", r#"{"compatibility":"NONE"}"#)).await.unwrap();
    let base = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"}"#;
    app.clone().oneshot(req_post("/subjects/s/versions", base)).await.unwrap();
    let bad = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"x\",\"type\":\"int\"}]}"}"#;
    let r = app.clone().oneshot(req_post("/subjects/s/versions", bad)).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK, "NONE bypasses compat");
    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compatibility_endpoint_real_verdict() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    let base = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"}"#;
    app.clone().oneshot(req_post("/subjects/s/versions", base)).await.unwrap();
    let bad = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"x\",\"type\":\"int\"}]}"}"#;
    let r = app.clone().oneshot(req_post("/compatibility/subjects/s/versions/latest?verbose=true", bad)).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let b = body_json(r).await;
    assert_eq!(b["is_compatible"], false);
    assert!(b["messages"].as_array().unwrap().len() >= 1);
    cancel.cancel();
    broker.shutdown().await;
}
```
Add small request-builder helpers if not present: `fn req_post(uri, body) -> Request<Body>` / `fn req_put(uri, body)` (mirror the existing `register` helper). Keep each test fn small (clippy `too_many_lines`).

- [ ] **Step 3: Run** (no Docker): `cargo test -p crabka-schema-registry --test integration --test compat_conformance -- --nocapture`. All pass.

- [ ] **Step 4: CI** — in `.github/workflows/ci.yml`, the `schema-registry-integration` job's `cargo llvm-cov` line: add `--test compat_conformance` to the existing `--test integration --test rest_conformance --test schemas_record`. (No new codecov flag; same job. `capture_compat_fixtures` is `#[ignore]` and excluded by not naming it.)

- [ ] **Step 5: clippy + fmt + commit** (`tests/compat_conformance.rs`, `tests/integration.rs`, `.github/workflows/ci.yml`), message `schema-registry: compatibility conformance + enforcement tests + CI`.

---

## Self-review (completed by plan author)

**Spec coverage:** engine + 7 levels + transitive → Task 4; `format::check` Avro real / others permissive → Task 3; `SrError::Incompatible` 409 → Task 1; store all-versions accessor → Task 2; register enforcement (dedup → check → persist) → Task 5; real `/compatibility` + verbose → Task 6; golden-fixture + in-process enforcement + endpoint validation → Tasks 7–8; effective level subject>global → Task 4 `effective_level`; out-of-scope (Protobuf/JSON real rules, all-versions endpoint) absent → correct (permissive `check`, single-version endpoint only).

**Placeholder scan:** none — the only deferral is the deliberately-permissive Protobuf/JSON `check` (spec'd interim) and the Docker-captured fixtures (named files, real capture).

**Type consistency:** `compat::{CompatibilityLevel, Verdict, check_registration, check_against_version}`, `format::check(ty, reader, writer) -> Result<(), Vec<String>>`, `StoreState::versions_schemas -> Vec<(SchemaType, String)>`, `SrError::Incompatible(Vec<String>)`, and `store::version -> (i32,i32,SchemaType,String)` are used consistently across Tasks 1–8. The register hook passes the already-normalised `schema`; the endpoint passes input form (both correct, noted).
