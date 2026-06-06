# Crabka Schema Registry — Slice 3 (deletes, modes, lookups) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add registry CRUD completeness to `crabka-schema-registry` — soft + permanent delete (with Confluent's soft-before-hard rule and `?deleted` visibility), the `READWRITE`/`READONLY`/`IMPORT` modes, and the `/schemas/ids/{id}/versions`, `/schemas`, and `referencedby` lookups — all backed by new `_schemas` record families and calibrated to `cp-schema-registry 7.4.0`.

**Architecture:** CRUD/state-management spread across the existing layers (no new top-level module). Data flow: REST handler → `KafkaStore` facade (validates mode + soft-before-hard, produces the `_schemas` record, waits read-your-writes) → group-less reader replays → `StoreState` mutates → REST reads. New record types (`MODE`, `DELETE_SUBJECT`, SCHEMA tombstone) are seeded from Confluent's shapes in Tasks 1–3 and **byte-confirmed against a real cp capture in Task 4 (cp is authority).**

**Tech Stack:** Rust 2024, `serde`/`serde_json` (records + REST), `axum` 0.8 (REST), the existing `KafkaStore` write-gate + group-less reader, `crabka-client-producer`/`crabka-client-core` (produce/fetch `_schemas`). Tests: store unit tests (no broker), broker-backed facade + REST integration tests (`boot_registry`, Mac-friendly single broker), and a `#[ignore]` Docker capture harness against `cp-schema-registry:7.4.0`.

---

## Design reference

Spec: `docs/superpowers/specs/2026-06-05-crabka-schema-registry-slice-3-deletes-modes-lookups-design.md`. Read it.

### Verified existing signatures (grounded in the current tree)
```rust
// store/mod.rs
struct VersionEntry { version: i32, id: i32 }                       // → gains `deleted: bool`
pub struct StoreState { subjects, by_id, by_canonical, global_compat, subject_compat, max_id }  // → gains global_mode, subject_mode
pub fn register(&mut self, subject, ty, schema) -> Result<Registered, SrError>   // probe path; keeps signature
pub fn apply_schema(&mut self, _key: &SchemaKey, value: &SchemaValue)            // reader fold; REWRITTEN (handle deleted flag)
pub fn versions(&self, subject) -> Option<Vec<i32>>                 // → versions(subject, include_deleted)
pub fn version(&self, subject, Option<i32>) -> Option<(i32,i32,SchemaType,String)>  // → + include_deleted
pub fn subjects(&self) -> Vec<String>                              // → subjects(include_deleted)
pub fn schema_by_id(&self, i32) -> Option<(SchemaType,String)>     // → + include_deleted (+ reference-liveness)
pub fn find_under_subject(&self, subject, ty, schema) -> Option<Registered>   // → + include_deleted
pub fn versions_schemas(&self, subject) -> Vec<(SchemaType,String)>  // SAME signature; now filters deleted

// kafkastore/record.rs
pub struct SchemaKey { keytype, subject, version, magic:u8 }  pub fn new(subject, version)  // magic=1
pub struct SchemaValue { subject, version, id, schema_type:Option<String>, references, schema, deleted:bool }
pub struct ConfigKey { keytype, subject:Option<String>, magic:u8 }   pub struct ConfigValue { compatibility_level }
pub enum SchemaRecord { Schema(SchemaKey,SchemaValue), Config(ConfigKey,ConfigValue), Noop, Unknown }  // → + Mode, DeleteSubject, Tombstone
pub fn decode(key:&[u8], value:Option<&[u8]>) -> SchemaRecord
pub fn encode_schema(subject, version, id, ty, schema) -> (Vec<u8>,Vec<u8>)   // deleted=false
pub fn encode_config(subject:Option<&str>, level) -> (Vec<u8>,Vec<u8>)

// kafkastore/writer.rs
pub async fn produce(&self, key:Vec<u8>, value:Vec<u8>) -> anyhow::Result<i64>   // value always Some → ADD produce_tombstone

// kafkastore/mod.rs (facade)
pub async fn register(&self, subject, ty, schema) -> Result<Registered, SrError>   // → + import_id, import_version + mode gating
pub async fn set_global_compat / set_subject_compat / set_compat(subject:Option, level)  // → gate READONLY
async fn await_applied(&self, offset:i64)   self.writer.produce(...)   self.store.read()/clone()

// kafkastore/reader.rs
pub fn apply_record(store:&RwLock<StoreState>, rec:SchemaRecord)   // match → add new arms

// error.rs
pub enum SrError { SubjectNotFound(String), VersionNotFound, SchemaNotFound, InvalidSchema(String),
                   InvalidVersion(String), InvalidCompatibilityLevel(String), Backend(String), Incompatible(Vec<String>) }
pub fn error_code(&self)->i32   pub fn http_status(&self)->StatusCode   impl IntoResponse   const CONTENT_TYPE

// rest/mod.rs   pub struct AppState { store: Arc<KafkaStore> }   pub fn router(state)->Router
// rest/response.rs   pub fn ok_json<T:Serialize>(&T)->Response   pub fn ok_raw(String)->Response
// rest/subjects.rs   register, lookup, list, versions, get_version, get_version_schema, fn parse_version(&str)->Result<Option<i32>,SrError>
// rest/schemas.rs    get_by_id, types
// compat/mod.rs      check_registration(snap,subject,ty,candidate), check_against_version(snap,subject,ty,candidate,Option<i32>)
//                    calls snap.versions(subject) [:143] and snap.version(subject,version) [:147] and snap.versions_schemas [:113]

// RegistryConfig { bootstrap, schemas_topic, schemas_topic_rf, client_id }
// integration.rs helper: boot_registry(rf:i32) -> (BrokerHandle, Arc<KafkaStore>, CancellationToken, TempDir)
//   body_json(resp)->Value ; register(app,subject,body)->Value ; get_json(app,uri)->Value
```

### Branch / commit / gate discipline (executors read this)
- Worktree: `/Users/mattstone/git/crabka/.claude/worktrees/musing-cartwright-7af144`. Branch: `claude/schema-registry-slice-3` (assert NOT main). Always `git -C <worktree>`. Do NOT push (controller handles push/PR; stacks on slice-2c PR #400).
- Commits: `git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com"`; never `git config`; end body with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- **Per change before commit:** `cargo clippy -p crabka-schema-registry --all-targets -- -D warnings` + `cargo fmt -p crabka-schema-registry`. `git add` only the task's files.
- **Greenfield (CLAUDE.md):** no back-compat shims; when a struct/enum/signature changes, change all call sites — every task must leave the crate compiling and all existing tests green (Avro 21 / Protobuf 88 / JSON 92 conformance must stay green).
- **Confluent exactness (CLAUDE.md):** the `_schemas` record bytes and the numeric error codes are seeded from Confluent's shapes, then **confirmed/corrected against the cp capture in Task 4 — cp wins on any disagreement.**

---

## File structure
```
crates/schema-registry/src/
  store/mod.rs            # + VersionEntry.deleted, global_mode/subject_mode, mutators, deleted-aware queries
  kafkastore/record.rs   # + ModeKey/Value, DeleteSubjectKey/Value, SchemaRecord::{Mode,DeleteSubject,Tombstone}, encode_*
  kafkastore/writer.rs   # + produce_tombstone (null value)
  kafkastore/reader.rs   # + apply_record arms (Mode/DeleteSubject/Tombstone)
  kafkastore/mod.rs      # + facade soft/permanent delete, modes, READONLY gating, IMPORT register
  error.rs               # + OperationNotPermitted, SubjectNotSoftDeleted, VersionNotSoftDeleted, InvalidMode
  rest/mod.rs            # + routes, pub mod delete/mode, pub DeletedQ
  rest/delete.rs         # NEW: delete_version, delete_subject
  rest/mode.rs           # NEW: get/put global + get/put/delete subject
  rest/subjects.rs       # + ?deleted on list/versions/get_version/lookup, referencedby stub, wire register id/version
  rest/schemas.rs        # + ?deleted on get_by_id, get_by_id_versions, list_schemas
crates/schema-registry/tests/
  integration.rs                 # + facade (Task 2) + REST (Task 3) + full-lifecycle (Task 4) tests
  capture_admin_fixtures.rs      # NEW #[ignore] Docker: record bytes + REST codes from cp 7.4.0
  fixtures/admin/records.json    # NEW captured _schemas bytes
  fixtures/admin/rest.json       # NEW captured REST status + error_code per op
```

## Execution tasks (sequential; one implementer per task)
- **Task 1** — `_schemas` record families + `produce_tombstone` + store model (deleted flag, modes, mutators, deleted-aware queries) + reader arms + unit tests. (Touches record/writer/store/reader + mechanical `include_deleted=false` call-site updates to keep compiling.)
- **Task 2** — facade soft/permanent delete + modes + READONLY gating + IMPORT register + new `SrError` variants. (Broker-backed facade tests.)
- **Task 3** — REST `delete.rs`/`mode.rs` + `?deleted` on the GETs + the lookup endpoints + router. (Router-oneshot tests.)
- **Task 4** — cp Docker capture (record bytes + REST codes) + error-code/byte calibration + full-lifecycle integration tests + record round-trip vs fixtures.

---

## Task 1: `_schemas` record families + store model + reader arms

**Files:**
- Modify: `crates/schema-registry/src/kafkastore/record.rs`
- Modify: `crates/schema-registry/src/kafkastore/writer.rs`
- Modify: `crates/schema-registry/src/store/mod.rs`
- Modify: `crates/schema-registry/src/kafkastore/reader.rs`
- Modify (compile-fix call sites): `crates/schema-registry/src/compat/mod.rs`, `crates/schema-registry/src/rest/subjects.rs`, `crates/schema-registry/src/rest/schemas.rs`, `crates/schema-registry/src/kafkastore/mod.rs`, `crates/schema-registry/tests/interop.rs`

> **Why one big task:** adding `SchemaRecord` variants makes `reader::apply_record`'s `match` non-exhaustive, and the new reader arms call store mutators — records, store model, and reader fold are inseparable for a green build. Signature changes to the store queries ripple to `compat` + `rest` + `tests`; this task updates those call sites to pass `include_deleted=false` as placeholders, and Task 3 wires the real `?deleted` query.

### 1a — record types + encoders (`kafkastore/record.rs`)

- [ ] **Step 1: Write failing round-trip tests.** Append to `record.rs`'s `#[cfg(test)] mod tests`:
```rust
    #[test]
    fn encode_mode_round_trips() {
        let (k, v) = encode_mode(Some("s"), "READONLY");
        match SchemaRecord::decode(&k, Some(&v)) {
            SchemaRecord::Mode(key, Some(val)) => {
                assert_eq!(key.subject.as_deref(), Some("s"));
                assert_eq!(val.mode, "READONLY");
            }
            other => panic!("expected Mode, got {other:?}"),
        }
        // global mode: subject is null
        let (gk, _gv) = encode_mode(None, "IMPORT");
        assert_eq!(&gk, br#"{"keytype":"MODE","subject":null,"magic":0}"#);
    }

    #[test]
    fn mode_tombstone_decodes_to_clear() {
        let k = mode_key(Some("s"));
        match SchemaRecord::decode(&k, None) {
            SchemaRecord::Mode(key, None) => assert_eq!(key.subject.as_deref(), Some("s")),
            other => panic!("expected Mode-clear, got {other:?}"),
        }
    }

    #[test]
    fn encode_delete_subject_round_trips() {
        let (k, v) = encode_delete_subject("s", 3);
        assert_eq!(&k, br#"{"keytype":"DELETE_SUBJECT","subject":"s","magic":0}"#);
        match SchemaRecord::decode(&k, Some(&v)) {
            SchemaRecord::DeleteSubject(key, val) => {
                assert_eq!(key.subject, "s");
                assert_eq!((val.subject.as_str(), val.version), ("s", 3));
            }
            other => panic!("expected DeleteSubject, got {other:?}"),
        }
    }

    #[test]
    fn schema_null_value_decodes_to_tombstone() {
        let key = encode_tombstone("s", 2);
        assert_eq!(&key, br#"{"keytype":"SCHEMA","subject":"s","version":2,"magic":1}"#);
        match SchemaRecord::decode(&key, None) {
            SchemaRecord::Tombstone(k) => assert_eq!((k.subject.as_str(), k.version), ("s", 2)),
            other => panic!("expected Tombstone, got {other:?}"),
        }
    }

    #[test]
    fn encode_schema_deleted_sets_flag() {
        let (_k, v) = encode_schema_deleted("s", 1, 7, SchemaType::Avro, "{\"type\":\"int\"}");
        let val: SchemaValue = serde_json::from_slice(&v).unwrap();
        assert!(val.deleted);
        assert_eq!(val.id, 7);
    }

    #[test]
    fn clear_subjects_and_delete_subject_tombstone_are_noop() {
        let cs = br#"{"keytype":"CLEAR_SUBJECTS","subject":"s","magic":0}"#;
        assert!(matches!(SchemaRecord::decode(cs, None), SchemaRecord::Noop));
        let (dk, _dv) = encode_delete_subject("s", 1);
        assert!(matches!(SchemaRecord::decode(&dk, None), SchemaRecord::Noop));
    }
```

- [ ] **Step 2: Run — expect FAIL** (types/functions missing):
`cargo test -p crabka-schema-registry --lib kafkastore::record` → fails to compile.

- [ ] **Step 3: Add the new key/value types** after `ConfigValue` in `record.rs`:
```rust
/// Key for a `MODE` record. `subject = None` is the global mode. Field order is
/// fixed to match Confluent's compaction key (seeded; confirmed in Task 4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModeKey {
    pub keytype: String,
    pub subject: Option<String>,
    pub magic: u8,
}

/// Value for a `MODE` record: `{"mode":"READWRITE"|"READONLY"|"IMPORT"}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModeValue {
    pub mode: String,
}

/// Key for a `DELETE_SUBJECT` record (soft subject-delete marker).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteSubjectKey {
    pub keytype: String,
    pub subject: String,
    pub magic: u8,
}

/// Value for a `DELETE_SUBJECT` record: the subject + the version up to which it
/// is soft-deleted (the latest version at delete time).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteSubjectValue {
    pub subject: String,
    pub version: i32,
}
```

- [ ] **Step 4: Extend `SchemaRecord`.** Replace the enum with:
```rust
#[derive(Debug, Clone)]
pub enum SchemaRecord {
    Schema(SchemaKey, SchemaValue),
    Config(ConfigKey, ConfigValue),
    /// A `MODE` record. `None` value = a MODE tombstone (clears the override).
    Mode(ModeKey, Option<ModeValue>),
    /// A soft subject-delete marker.
    DeleteSubject(DeleteSubjectKey, DeleteSubjectValue),
    /// A `SCHEMA` key with a null value = permanent version delete.
    Tombstone(SchemaKey),
    Noop,
    Unknown,
}
```

- [ ] **Step 5: Rewrite `decode`'s `match`.** Replace the `match kv.get("keytype")...` body:
```rust
        match kv.get("keytype").and_then(|v| v.as_str()) {
            Some("SCHEMA") => match serde_json::from_slice::<SchemaKey>(key) {
                Ok(k) => match value {
                    Some(v) => match serde_json::from_slice::<SchemaValue>(v) {
                        Ok(val) => Self::Schema(k, val),
                        Err(_) => Self::Unknown,
                    },
                    None => Self::Tombstone(k), // null value = permanent version delete
                },
                Err(_) => Self::Unknown,
            },
            Some("CONFIG") => match (
                serde_json::from_slice::<ConfigKey>(key),
                value.and_then(|v| serde_json::from_slice::<ConfigValue>(v).ok()),
            ) {
                (Ok(k), Some(val)) => Self::Config(k, val),
                _ => Self::Unknown,
            },
            Some("MODE") => match serde_json::from_slice::<ModeKey>(key) {
                Ok(k) => match value.and_then(|v| serde_json::from_slice::<ModeValue>(v).ok()) {
                    Some(val) => Self::Mode(k, Some(val)),
                    None => Self::Mode(k, None), // null value = clear mode override
                },
                Err(_) => Self::Unknown,
            },
            Some("DELETE_SUBJECT") => match (
                serde_json::from_slice::<DeleteSubjectKey>(key),
                value.and_then(|v| serde_json::from_slice::<DeleteSubjectValue>(v).ok()),
            ) {
                (Ok(k), Some(val)) => Self::DeleteSubject(k, val),
                // a DELETE_SUBJECT tombstone: the versions are removed by their
                // own SCHEMA tombstones, so this marker is a no-op on replay.
                _ => Self::Noop,
            },
            Some("NOOP" | "CLEAR_SUBJECTS") => Self::Noop,
            _ => Self::Unknown,
        }
```

- [ ] **Step 6: Refactor `encode_schema` to share a builder + add the new encoders.** Replace the `encode_schema` fn with:
```rust
fn schema_kv(
    subject: &str,
    version: i32,
    id: i32,
    ty: SchemaType,
    schema: &str,
    deleted: bool,
) -> (Vec<u8>, Vec<u8>) {
    let key = SchemaKey::new(subject, version);
    let value = SchemaValue {
        subject: subject.to_string(),
        version,
        id,
        schema_type: ty.wire_name().map(str::to_string),
        references: Vec::new(),
        schema: schema.to_string(),
        deleted,
    };
    (
        serde_json::to_vec(&key).expect("key serialises"),
        serde_json::to_vec(&value).expect("value serialises"),
    )
}

/// Build the byte-exact key + structurally-stable value for a `SCHEMA` record.
#[must_use]
pub fn encode_schema(
    subject: &str,
    version: i32,
    id: i32,
    ty: SchemaType,
    schema: &str,
) -> (Vec<u8>, Vec<u8>) {
    schema_kv(subject, version, id, ty, schema, false)
}

/// Build a soft-delete `SCHEMA` record: identical key/value to the original but
/// with `deleted = true` (cp re-emits the full value with the flag flipped).
#[must_use]
pub fn encode_schema_deleted(
    subject: &str,
    version: i32,
    id: i32,
    ty: SchemaType,
    schema: &str,
) -> (Vec<u8>, Vec<u8>) {
    schema_kv(subject, version, id, ty, schema, true)
}

/// Build the `SCHEMA` key bytes for a permanent-delete tombstone (value is null,
/// produced via [`crate::kafkastore::writer::SchemaWriter::produce_tombstone`]).
#[must_use]
pub fn encode_tombstone(subject: &str, version: i32) -> Vec<u8> {
    serde_json::to_vec(&SchemaKey::new(subject, version)).expect("schema key serialises")
}

/// Build a `MODE` record's (key, value). `subject = None` is the global mode.
#[must_use]
pub fn encode_mode(subject: Option<&str>, mode: &str) -> (Vec<u8>, Vec<u8>) {
    let key = ModeKey {
        keytype: "MODE".to_string(),
        subject: subject.map(str::to_string),
        magic: 0,
    };
    let value = ModeValue {
        mode: mode.to_string(),
    };
    (
        serde_json::to_vec(&key).expect("mode key serialises"),
        serde_json::to_vec(&value).expect("mode value serialises"),
    )
}

/// Build the `MODE` key bytes for a mode-clear tombstone (value is null).
#[must_use]
pub fn mode_key(subject: Option<&str>) -> Vec<u8> {
    let key = ModeKey {
        keytype: "MODE".to_string(),
        subject: subject.map(str::to_string),
        magic: 0,
    };
    serde_json::to_vec(&key).expect("mode key serialises")
}

/// Build a `DELETE_SUBJECT` record's (key, value).
#[must_use]
pub fn encode_delete_subject(subject: &str, version: i32) -> (Vec<u8>, Vec<u8>) {
    let key = DeleteSubjectKey {
        keytype: "DELETE_SUBJECT".to_string(),
        subject: subject.to_string(),
        magic: 0,
    };
    let value = DeleteSubjectValue {
        subject: subject.to_string(),
        version,
    };
    (
        serde_json::to_vec(&key).expect("delete-subject key serialises"),
        serde_json::to_vec(&value).expect("delete-subject value serialises"),
    )
}
```

- [ ] **Step 7: Run — expect PASS:** `cargo test -p crabka-schema-registry --lib kafkastore::record` → all round-trip tests pass.

> **Calibration note (Task 4):** the seeded `magic` bytes (0 for MODE/DELETE_SUBJECT, mirroring CONFIG) and key field order are confirmed against the cp capture in Task 4. If cp's bytes differ, fix the structs/encoders there and re-run these tests with the corrected expected bytes.

### 1b — tombstone producer (`kafkastore/writer.rs`)

- [ ] **Step 8: Add `produce_tombstone`** after `produce` in `writer.rs`:
```rust
    /// Produce a tombstone (null value) for `key`; return the assigned offset.
    /// Used for permanent deletes and mode-clears (compaction reclaims the key).
    pub async fn produce_tombstone(&self, key: Vec<u8>) -> anyhow::Result<i64> {
        let rx = self
            .producer
            .send(ProducerRecord {
                topic: self.topic.clone(),
                key: Some(Bytes::from(key)),
                value: None,
                ..Default::default()
            })
            .await;
        let meta = rx
            .await
            .map_err(|_| anyhow::anyhow!("producer dropped ack"))??;
        Ok(meta.offset)
    }
```

### 1c — store model (`store/mod.rs`)

- [ ] **Step 9: Write failing store unit tests.** Append to `store/mod.rs`'s `mod tests`:
```rust
    #[test]
    fn soft_delete_hides_then_deleted_shows_then_permanent_removes() {
        let mut s = StoreState::default();
        s.register("av", SchemaType::Avro, &av("A")).unwrap();
        s.register("av", SchemaType::Avro, &av("B")).unwrap();
        // soft-delete v1 by replaying a SCHEMA record with deleted=true
        apply_deleted(&mut s, "av", 1, 1, &av("A"));
        assert_eq!(s.versions("av", false).unwrap(), vec![2]);
        assert_eq!(s.versions("av", true).unwrap(), vec![1, 2]);
        assert!(s.version("av", Some(1), false).is_none());
        assert!(s.version("av", Some(1), true).is_some());
        // permanent-delete v1
        assert_eq!(s.permanent_delete_version("av", 1), Some(1));
        assert!(s.version("av", Some(1), true).is_none());
        assert_eq!(s.versions("av", true).unwrap(), vec![2]);
    }

    #[test]
    fn soft_delete_subject_flags_all_versions() {
        let mut s = StoreState::default();
        s.register("av", SchemaType::Avro, &av("A")).unwrap();
        s.register("av", SchemaType::Avro, &av("B")).unwrap();
        assert_eq!(s.soft_delete_subject("av"), Some(vec![1, 2]));
        assert!(s.versions("av", false).is_none()); // 404 territory
        assert_eq!(s.subjects(false), Vec::<String>::new());
        assert_eq!(s.subjects(true), vec!["av".to_string()]);
    }

    #[test]
    fn resurrect_on_reregister_clears_deleted() {
        let mut s = StoreState::default();
        s.register("av", SchemaType::Avro, &av("A")).unwrap();
        apply_deleted(&mut s, "av", 1, 1, &av("A"));
        assert!(s.versions("av", false).is_none());
        // re-register identical schema → resurrect the same version live
        apply_live(&mut s, "av", 1, 1, &av("A"));
        assert_eq!(s.versions("av", false).unwrap(), vec![1]);
    }

    #[test]
    fn schema_by_id_respects_reference_liveness() {
        let mut s = StoreState::default();
        s.register("av", SchemaType::Avro, &av("A")).unwrap();
        assert!(s.schema_by_id(1, false).is_some());
        apply_deleted(&mut s, "av", 1, 1, &av("A"));
        assert!(s.schema_by_id(1, false).is_none()); // only soft-deleted refs
        assert!(s.schema_by_id(1, true).is_some());
        s.permanent_delete_version("av", 1);
        assert!(s.schema_by_id(1, true).is_none()); // no refs at all
    }

    #[test]
    fn schema_id_subject_versions_and_all_schemas() {
        let mut s = StoreState::default();
        s.register("a", SchemaType::Avro, &av("A")).unwrap(); // id1 a/v1
        s.register("b", SchemaType::Avro, &av("A")).unwrap(); // id1 reused, b/v1
        let mut sv = s.schema_id_subject_versions(1, false);
        sv.sort();
        assert_eq!(sv, vec![("a".to_string(), 1), ("b".to_string(), 1)]);
        assert_eq!(s.all_schemas(false).len(), 2);
    }

    #[test]
    fn modes_default_and_resolve() {
        let mut s = StoreState::default();
        assert_eq!(s.global_mode(), "READWRITE");
        assert_eq!(s.effective_mode("x"), "READWRITE");
        s.set_global_mode("READONLY".into());
        assert_eq!(s.effective_mode("x"), "READONLY");
        s.set_subject_mode("x", "IMPORT".into());
        assert_eq!(s.effective_mode("x"), "IMPORT");
        s.clear_subject_mode("x");
        assert_eq!(s.effective_mode("x"), "READONLY");
    }

    // test helpers: replay a SCHEMA record straight into the store
    fn apply_live(s: &mut StoreState, subject: &str, version: i32, id: i32, schema: &str) {
        apply_with_flag(s, subject, version, id, schema, false);
    }
    fn apply_deleted(s: &mut StoreState, subject: &str, version: i32, id: i32, schema: &str) {
        apply_with_flag(s, subject, version, id, schema, true);
    }
    fn apply_with_flag(s: &mut StoreState, subject: &str, version: i32, id: i32, schema: &str, deleted: bool) {
        use crate::kafkastore::record::{SchemaKey, SchemaValue};
        let v = SchemaValue {
            subject: subject.into(),
            version,
            id,
            schema_type: None,
            references: vec![],
            schema: schema.into(),
            deleted,
        };
        s.apply_schema(&SchemaKey::new(subject, version), &v);
    }
```
Also UPDATE the existing store tests that call the changed signatures: in `identical_under_same_subject_is_idempotent`, `different_schema_increments_id_and_version`, `apply_schema_is_idempotent` change `s.versions("av-value")` → `s.versions("av-value", false)` and `s.schema_by_id(1)` → `s.schema_by_id(1, false)`.

- [ ] **Step 10: Run — expect FAIL** (new methods + `deleted` field missing): `cargo test -p crabka-schema-registry --lib store` → compile error.

- [ ] **Step 11: Add the `deleted` field + mode fields.** In `store/mod.rs`:
```rust
#[derive(Debug, Clone)]
struct VersionEntry {
    version: i32,
    id: i32,
    deleted: bool,
}

#[derive(Debug, Default, Clone)]
pub struct StoreState {
    subjects: BTreeMap<String, Vec<VersionEntry>>,
    by_id: BTreeMap<i32, (SchemaType, String)>,
    by_canonical: BTreeMap<String, i32>,
    global_compat: Option<String>,
    subject_compat: BTreeMap<String, String>,
    global_mode: Option<String>,
    subject_mode: BTreeMap<String, String>,
    max_id: i32,
}
```

- [ ] **Step 12: Set `deleted: false` in `register`'s push.** In `register`, change the `.push(VersionEntry { version: next_version, id })` to `.push(VersionEntry { version: next_version, id, deleted: false })`.

- [ ] **Step 13: Rewrite `apply_schema`** (handle insert / soft-delete-flag / resurrect; register id even for deleted records so `?deleted` lookups work):
```rust
    /// Fold a decoded SCHEMA record into state (reader replay). Idempotent.
    /// Sets the version's `deleted` flag to the record's — so the same code path
    /// inserts (deleted=false), soft-deletes (deleted=true on an existing
    /// version), and resurrects (deleted=false on a soft-deleted version).
    pub fn apply_schema(&mut self, _key: &SchemaKey, value: &SchemaValue) {
        let ty = SchemaType::from_wire(value.schema_type.as_deref());
        self.max_id = self.max_id.max(value.id);
        self.by_id
            .entry(value.id)
            .or_insert_with(|| (ty, value.schema.clone()));
        if let Ok(p) = format::parse(ty, &value.schema) {
            self.by_canonical
                .entry(p.canonical_form())
                .or_insert(value.id);
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

- [ ] **Step 14: Parameterise `find_under_subject_canonical` + `find_under_subject` with `include_deleted`.** Replace both:
```rust
    fn find_under_subject_canonical(
        &self,
        subject: &str,
        canonical: &str,
        include_deleted: bool,
    ) -> Option<Registered> {
        let id = *self.by_canonical.get(canonical)?;
        let entry = self
            .subjects
            .get(subject)?
            .iter()
            .find(|v| v.id == id && (include_deleted || !v.deleted))?;
        Some(Registered {
            id,
            version: entry.version,
        })
    }

    /// Lookup an already-registered schema under a subject (POST /subjects/{s}).
    #[must_use]
    pub fn find_under_subject(
        &self,
        subject: &str,
        ty: SchemaType,
        schema: &str,
        include_deleted: bool,
    ) -> Option<Registered> {
        let canonical = format::parse(ty, schema).ok()?.canonical_form();
        self.find_under_subject_canonical(subject, &canonical, include_deleted)
    }
```
And in `register`, change the idempotent short-circuit call `self.find_under_subject_canonical(subject, &canonical)` → `self.find_under_subject_canonical(subject, &canonical, true)` (so a soft-deleted identical schema is the resurrection target the facade emits a live record for).

- [ ] **Step 15: Rewrite the deleted-aware queries.** Replace `subjects`, `versions`, `version`, `schema_by_id`, `versions_schemas`, and add the three new queries + mutators + mode accessors:
```rust
    #[must_use]
    pub fn subjects(&self, include_deleted: bool) -> Vec<String> {
        self.subjects
            .iter()
            .filter(|(_, vs)| vs.iter().any(|v| include_deleted || !v.deleted))
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Live (or, with `include_deleted`, all) version numbers. `None` when the
    /// subject has no qualifying versions (→ 404).
    #[must_use]
    pub fn versions(&self, subject: &str, include_deleted: bool) -> Option<Vec<i32>> {
        let vs = self.subjects.get(subject)?;
        let out: Vec<i32> = vs
            .iter()
            .filter(|v| include_deleted || !v.deleted)
            .map(|v| v.version)
            .collect();
        if out.is_empty() { None } else { Some(out) }
    }

    /// `(id, version, schemaType, schema)` for a subject+version. `version=None`
    /// resolves to the latest qualifying version.
    #[must_use]
    pub fn version(
        &self,
        subject: &str,
        version: Option<i32>,
        include_deleted: bool,
    ) -> Option<(i32, i32, SchemaType, String)> {
        let vs = self.subjects.get(subject)?;
        let entry = match version {
            Some(v) => vs.iter().find(|e| e.version == v && (include_deleted || !e.deleted))?,
            None => vs.iter().filter(|e| include_deleted || !e.deleted).last()?,
        };
        let (ty, schema) = self.by_id.get(&entry.id)?;
        Some((entry.id, entry.version, *ty, schema.clone()))
    }

    /// Schema bytes for a global id. Returns `None` unless some qualifying
    /// version references the id (so a permanently-deleted id is gone, and a
    /// soft-deleted-only id is hidden without `include_deleted`).
    #[must_use]
    pub fn schema_by_id(&self, id: i32, include_deleted: bool) -> Option<(SchemaType, String)> {
        let (ty, schema) = self.by_id.get(&id)?;
        let referenced = self
            .subjects
            .values()
            .flatten()
            .any(|v| v.id == id && (include_deleted || !v.deleted));
        if referenced {
            Some((*ty, schema.clone()))
        } else {
            None
        }
    }

    /// `(subject, version)` pairs referencing a global id (GET /schemas/ids/{id}/versions).
    #[must_use]
    pub fn schema_id_subject_versions(&self, id: i32, include_deleted: bool) -> Vec<(String, i32)> {
        let mut out = Vec::new();
        for (subject, vs) in &self.subjects {
            for v in vs {
                if v.id == id && (include_deleted || !v.deleted) {
                    out.push((subject.clone(), v.version));
                }
            }
        }
        out
    }

    /// Every `(subject, version, id, schemaType, schema)` (GET /schemas), sorted
    /// by id then subject then version.
    #[must_use]
    pub fn all_schemas(
        &self,
        include_deleted: bool,
    ) -> Vec<(String, i32, i32, SchemaType, String)> {
        let mut out = Vec::new();
        for (subject, vs) in &self.subjects {
            for v in vs {
                if include_deleted || !v.deleted {
                    if let Some((ty, schema)) = self.by_id.get(&v.id) {
                        out.push((subject.clone(), v.version, v.id, *ty, schema.clone()));
                    }
                }
            }
        }
        out.sort_by(|a, b| a.2.cmp(&b.2).then(a.0.cmp(&b.0)).then(a.1.cmp(&b.1)));
        out
    }

    /// A subject's LIVE versions as ordered `(type, schema)` pairs (ascending).
    /// Soft-deleted versions are excluded (compat ignores them).
    #[must_use]
    pub fn versions_schemas(&self, subject: &str) -> Vec<(SchemaType, String)> {
        let Some(entries) = self.subjects.get(subject) else {
            return Vec::new();
        };
        entries
            .iter()
            .filter(|e| !e.deleted)
            .filter_map(|e| self.by_id.get(&e.id).cloned())
            .collect()
    }

    // ── mutators (reader-applied) ───────────────────────────────────────────
    /// Flag every version of a subject deleted; returns the version numbers.
    pub fn soft_delete_subject(&mut self, subject: &str) -> Option<Vec<i32>> {
        let vs = self.subjects.get_mut(subject)?;
        if vs.is_empty() {
            return None;
        }
        let versions = vs.iter().map(|v| v.version).collect();
        for v in vs.iter_mut() {
            v.deleted = true;
        }
        Some(versions)
    }

    /// Remove a single version (permanent). Drops the subject if it becomes
    /// empty. Returns `None` if nothing was removed (idempotent replay).
    pub fn permanent_delete_version(&mut self, subject: &str, version: i32) -> Option<i32> {
        let vs = self.subjects.get_mut(subject)?;
        let before = vs.len();
        vs.retain(|v| v.version != version);
        if vs.len() == before {
            return None;
        }
        if vs.is_empty() {
            self.subjects.remove(subject);
        }
        Some(version)
    }

    pub fn set_global_mode(&mut self, mode: String) {
        self.global_mode = Some(mode);
    }
    pub fn set_subject_mode(&mut self, subject: &str, mode: String) {
        self.subject_mode.insert(subject.to_string(), mode);
    }
    pub fn clear_subject_mode(&mut self, subject: &str) {
        self.subject_mode.remove(subject);
    }
    pub fn clear_global_mode(&mut self) {
        self.global_mode = None;
    }

    #[must_use]
    pub fn global_mode(&self) -> &str {
        self.global_mode.as_deref().unwrap_or("READWRITE")
    }
    #[must_use]
    pub fn subject_mode(&self, subject: &str) -> Option<&str> {
        self.subject_mode.get(subject).map(String::as_str)
    }
    /// Subject override else global else `READWRITE`.
    #[must_use]
    pub fn effective_mode(&self, subject: &str) -> &str {
        self.subject_mode
            .get(subject)
            .map_or_else(|| self.global_mode(), String::as_str)
    }
```

- [ ] **Step 16: Run — expect PASS:** `cargo test -p crabka-schema-registry --lib store` → all store tests (old + new) pass.

### 1d — reader arms + call-site fixes

- [ ] **Step 17: Add reader arms.** In `kafkastore/reader.rs`, replace `apply_record`'s body:
```rust
pub fn apply_record(store: &RwLock<StoreState>, rec: SchemaRecord) {
    match rec {
        SchemaRecord::Schema(k, v) => store.write().apply_schema(&k, &v),
        SchemaRecord::Tombstone(k) => {
            store.write().permanent_delete_version(&k.subject, k.version);
        }
        SchemaRecord::DeleteSubject(k, _v) => {
            store.write().soft_delete_subject(&k.subject);
        }
        SchemaRecord::Mode(k, Some(v)) => {
            let mut s = store.write();
            match k.subject {
                Some(subj) => s.set_subject_mode(&subj, v.mode),
                None => s.set_global_mode(v.mode),
            }
        }
        SchemaRecord::Mode(k, None) => {
            let mut s = store.write();
            match k.subject {
                Some(subj) => s.clear_subject_mode(&subj),
                None => s.clear_global_mode(),
            }
        }
        SchemaRecord::Config(k, v) => {
            let mut s = store.write();
            match k.subject {
                Some(subj) => s.set_subject_compat(&subj, v.compatibility_level),
                None => s.set_global_compat(v.compatibility_level),
            }
        }
        SchemaRecord::Noop | SchemaRecord::Unknown => {}
    }
}
```
And UPDATE the reader test `apply_record_folds_schema_and_ignores_noop`: `store.read().versions("av-value")` → `versions("av-value", false)`; `store.read().schema_by_id(1)` → `schema_by_id(1, false)`.

- [ ] **Step 18: Fix the remaining call sites to compile (pass `include_deleted=false`).**
  - `compat/mod.rs:143`: `snap.versions(subject).is_none()` → `snap.versions(subject, false).is_none()`.
  - `compat/mod.rs:147`: `.version(subject, version)` → `.version(subject, version, false)`.
  - `kafkastore/mod.rs:69`: `find_under_subject(subject, ty, schema)` → `find_under_subject(subject, ty, schema, false)`.
  - `rest/subjects.rs`: line 47/103/125 `s.versions(&subject).is_none()` → `s.versions(&subject, false).is_none()`; line 81 `.versions(&subject)` → `.versions(&subject, false)`; line 106/128 `s.version(&subject, want)` → `s.version(&subject, want, false)`; line 50 `find_under_subject(&subject, ty, &req.schema)` → `..., false)`; line 53 `schema_by_id(found.id)` → `schema_by_id(found.id, false)`; line 69 `subjects()` → `subjects(false)`.
  - `rest/schemas.rs:18`: `schema_by_id(id)` → `schema_by_id(id, false)`.
  - `tests/interop.rs:286`: `store.store.read().subjects()` → `subjects(false)`.

  > These are placeholders. Task 3 changes the `rest/` ones to read the actual `?deleted` query.

- [ ] **Step 19: Run the whole crate** (lib + the conformance/interop tests must stay green):
`cargo test -p crabka-schema-registry --lib` and `cargo test -p crabka-schema-registry --test compat_conformance --test interop` → all pass (Avro 21 / Protobuf 88 / JSON 92 unchanged).

- [ ] **Step 20: clippy + fmt + commit.**
```bash
WT=/Users/mattstone/git/crabka/.claude/worktrees/musing-cartwright-7af144
cargo clippy -p crabka-schema-registry --all-targets -- -D warnings
cargo fmt -p crabka-schema-registry
git -C "$WT" add crates/schema-registry/src/kafkastore/record.rs crates/schema-registry/src/kafkastore/writer.rs \
  crates/schema-registry/src/store/mod.rs crates/schema-registry/src/kafkastore/reader.rs \
  crates/schema-registry/src/compat/mod.rs crates/schema-registry/src/kafkastore/mod.rs \
  crates/schema-registry/src/rest/subjects.rs crates/schema-registry/src/rest/schemas.rs \
  crates/schema-registry/tests/interop.rs
git -C "$WT" -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
schema-registry: _schemas delete/mode record families + deleted-aware store model

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: facade delete/mode methods + READONLY gating + IMPORT register

**Files:**
- Modify: `crates/schema-registry/src/error.rs`
- Modify: `crates/schema-registry/src/kafkastore/mod.rs`
- Modify (call site): `crates/schema-registry/src/rest/subjects.rs` (register handler → pass `None, None`)
- Test: `crates/schema-registry/tests/integration.rs` (broker-backed facade tests)

### 2a — error variants (`error.rs`)

- [ ] **Step 1: Add the new `SrError` variants** after `Incompatible`:
```rust
    /// A write was attempted on a subject/registry in `READONLY` mode.
    #[error("Subject '{0}' is in read-only mode.")]
    OperationNotPermitted(String),
    /// Permanent subject delete attempted before a soft delete.
    #[error("Subject '{0}' was not deleted first before being permanently deleted.")]
    SubjectNotSoftDeleted(String),
    /// Permanent version delete attempted before a soft delete.
    #[error("Version {1} of subject '{0}' was not soft-deleted first before being permanently deleted.")]
    VersionNotSoftDeleted(String, i32),
    /// Unknown mode string on PUT /mode.
    #[error("Invalid mode: {0}")]
    InvalidMode(String),
```

- [ ] **Step 2: Map codes + statuses (SEED; Task 4 calibrates against cp).** In `error_code`:
```rust
            Self::OperationNotPermitted(_) => 42205,
            Self::SubjectNotSoftDeleted(_) => 40406,
            Self::VersionNotSoftDeleted(..) => 40407,
            Self::InvalidMode(_) => 42204,
```
In `http_status`, add `Self::SubjectNotSoftDeleted(_) | Self::VersionNotSoftDeleted(..)` to the `NOT_FOUND` arm and `Self::OperationNotPermitted(_) | Self::InvalidMode(_)` to the `UNPROCESSABLE_ENTITY` arm.

- [ ] **Step 3: Add a unit test** to `error.rs` `mod tests`:
```rust
    #[test]
    fn slice3_codes() {
        assert_eq!(SrError::OperationNotPermitted("s".into()).error_code(), 42205);
        assert_eq!(
            SrError::OperationNotPermitted("s".into()).http_status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(SrError::SubjectNotSoftDeleted("s".into()).error_code(), 40406);
        assert_eq!(SrError::VersionNotSoftDeleted("s".into(), 2).error_code(), 40407);
        assert_eq!(
            SrError::SubjectNotSoftDeleted("s".into()).http_status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(SrError::InvalidMode("X".into()).error_code(), 42204);
    }
```

- [ ] **Step 4: Run — expect PASS:** `cargo test -p crabka-schema-registry --lib error`.

### 2b — facade (`kafkastore/mod.rs`)

- [ ] **Step 5: Write failing broker-backed facade tests** in `tests/integration.rs` (append; reuse `boot_registry`):
```rust
use crabka_schema_registry::format::SchemaType;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn facade_soft_then_permanent_delete_version() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    store.register("av", SchemaType::Avro, &av("A"), None, None).await.unwrap();
    store.register("av", SchemaType::Avro, &av("B"), None, None).await.unwrap();
    // soft-delete v1
    assert_eq!(store.soft_delete_version("av", 1).await.unwrap(), 1);
    assert_eq!(store.store.read().versions("av", false).unwrap(), vec![2]);
    assert_eq!(store.store.read().versions("av", true).unwrap(), vec![1, 2]);
    // permanent before soft on v2 → error
    let err = store.permanent_delete_version("av", 2).await.unwrap_err();
    assert_eq!(err.error_code(), 40407);
    // permanent v1 (already soft) → gone
    assert_eq!(store.permanent_delete_version("av", 1).await.unwrap(), 1);
    assert!(store.store.read().version("av", Some(1), true).is_none());
    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn facade_readonly_blocks_writes_import_allows_explicit_id() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    store.register("ro", SchemaType::Avro, &av("A"), None, None).await.unwrap();
    store.set_subject_mode("ro", "READONLY".into()).await.unwrap();
    let err = store.register("ro", SchemaType::Avro, &av("B"), None, None).await.unwrap_err();
    assert_eq!(err.error_code(), 42205);
    // IMPORT on a fresh subject → register at explicit id/version
    store.set_subject_mode("imp", "IMPORT".into()).await.unwrap();
    let reg = store.register("imp", SchemaType::Avro, &av("C"), Some(42), Some(5)).await.unwrap();
    assert_eq!((reg.id, reg.version), (42, 5));
    assert_eq!(store.store.read().version("imp", Some(5), false).unwrap().0, 42);
    cancel.cancel();
    broker.shutdown().await;
}

// local helper (avro record schema by name)
fn av(n: &str) -> String {
    format!("{{\"type\":\"record\",\"name\":\"{n}\",\"fields\":[]}}")
}
```

- [ ] **Step 6: Run — expect FAIL** (signature + methods missing): `cargo test -p crabka-schema-registry --test integration facade_` → compile error.

- [ ] **Step 7: Add the mode gate + rewrite `register` with IMPORT.** In `kafkastore/mod.rs`, add near the impl top:
```rust
const VALID_MODES: &[&str] = &["READWRITE", "READONLY", "IMPORT"];

impl KafkaStore {
    fn effective_mode(&self, subject: &str) -> String {
        self.store.read().effective_mode(subject).to_string()
    }

    fn ensure_writable(&self, subject: &str) -> Result<(), SrError> {
        if self.effective_mode(subject) == "READONLY" {
            Err(SrError::OperationNotPermitted(subject.to_string()))
        } else {
            Ok(())
        }
    }
```
(close this `impl` block appropriately, or add the methods inside the existing `impl KafkaStore`). Replace `register` with:
```rust
    /// Register a schema. In `IMPORT` mode, persists at the explicit
    /// `import_id`/`import_version` (no id-assignment, no compat check). In
    /// `READONLY` mode, rejected. Otherwise the slice-1/2 path (dedup → compat →
    /// assign → persist → read-your-writes).
    pub async fn register(
        &self,
        subject: &str,
        ty: SchemaType,
        schema: &str,
        import_id: Option<i32>,
        import_version: Option<i32>,
    ) -> Result<Registered, SrError> {
        let _gate = self.write_gate.lock().await;
        let mode = self.effective_mode(subject);
        if mode == "READONLY" {
            return Err(SrError::OperationNotPermitted(subject.to_string()));
        }
        let schema = &format::normalized_storage_form(ty, schema)?;
        if mode == "IMPORT" {
            let (id, version) = match (import_id, import_version) {
                (Some(i), Some(v)) => (i, v),
                _ => {
                    return Err(SrError::InvalidSchema(
                        "IMPORT mode requires explicit id and version".into(),
                    ));
                }
            };
            format::parse(ty, schema)?; // 42201 if unparseable
            let (key, value) = record::encode_schema(subject, version, id, ty, schema);
            let offset = self
                .writer
                .produce(key, value)
                .await
                .map_err(|e| SrError::Backend(e.to_string()))?;
            self.await_applied(offset).await;
            return Ok(Registered { id, version });
        }
        if let Some(existing) = self.store.read().find_under_subject(subject, ty, schema, false) {
            return Ok(existing);
        }
        crate::compat::check_registration(&self.store.read(), subject, ty, schema)?;
        let reg = {
            let mut probe = self.store.read().clone();
            probe.register(subject, ty, schema)?
        };
        let (key, value) = record::encode_schema(subject, reg.version, reg.id, ty, schema);
        let offset = self
            .writer
            .produce(key, value)
            .await
            .map_err(|e| SrError::Backend(e.to_string()))?;
        self.await_applied(offset).await;
        Ok(reg)
    }
```

- [ ] **Step 8: Gate `set_compat` on READONLY.** In `set_compat`, after taking the gate, insert:
```rust
        let mode = match subject {
            Some(s) => self.store.read().effective_mode(s).to_string(),
            None => self.store.read().global_mode().to_string(),
        };
        if mode == "READONLY" {
            return Err(SrError::OperationNotPermitted(
                subject.unwrap_or("global").to_string(),
            ));
        }
```

- [ ] **Step 9: Add the delete + mode facade methods** (inside `impl KafkaStore`, after `set_compat`):
```rust
    /// Soft-delete a version: re-emit its SCHEMA record with `deleted=true`.
    pub async fn soft_delete_version(&self, subject: &str, version: i32) -> Result<i32, SrError> {
        let _gate = self.write_gate.lock().await;
        self.ensure_writable(subject)?;
        let (id, ver, ty, schema) = {
            let s = self.store.read();
            if s.versions(subject, true).is_none() {
                return Err(SrError::SubjectNotFound(subject.to_string()));
            }
            s.version(subject, Some(version), true)
                .ok_or(SrError::VersionNotFound)?
        };
        let (key, value) = record::encode_schema_deleted(subject, ver, id, ty, &schema);
        let offset = self
            .writer
            .produce(key, value)
            .await
            .map_err(|e| SrError::Backend(e.to_string()))?;
        self.await_applied(offset).await;
        Ok(ver)
    }

    /// Permanently delete a version (tombstone). Requires a prior soft delete.
    pub async fn permanent_delete_version(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<i32, SrError> {
        let _gate = self.write_gate.lock().await;
        self.ensure_writable(subject)?;
        {
            let s = self.store.read();
            if s.versions(subject, true).is_none() {
                return Err(SrError::SubjectNotFound(subject.to_string()));
            }
            if s.version(subject, Some(version), true).is_none() {
                return Err(SrError::VersionNotFound);
            }
            // soft-deleted == present with include_deleted but absent as live
            if s.version(subject, Some(version), false).is_some() {
                return Err(SrError::VersionNotSoftDeleted(subject.to_string(), version));
            }
        }
        let key = record::encode_tombstone(subject, version);
        let offset = self
            .writer
            .produce_tombstone(key)
            .await
            .map_err(|e| SrError::Backend(e.to_string()))?;
        self.await_applied(offset).await;
        Ok(version)
    }

    /// Soft-delete a subject (DELETE_SUBJECT marker). Returns the live versions.
    pub async fn soft_delete_subject(&self, subject: &str) -> Result<Vec<i32>, SrError> {
        let _gate = self.write_gate.lock().await;
        self.ensure_writable(subject)?;
        let versions = {
            let s = self.store.read();
            s.versions(subject, false)
                .ok_or_else(|| SrError::SubjectNotFound(subject.to_string()))?
        };
        let max = versions.iter().copied().max().unwrap_or(0);
        let (key, value) = record::encode_delete_subject(subject, max);
        let offset = self
            .writer
            .produce(key, value)
            .await
            .map_err(|e| SrError::Backend(e.to_string()))?;
        self.await_applied(offset).await;
        Ok(versions)
    }

    /// Permanently delete a subject (per-version tombstones). Requires a prior
    /// soft delete (no live versions remain).
    pub async fn permanent_delete_subject(&self, subject: &str) -> Result<Vec<i32>, SrError> {
        let _gate = self.write_gate.lock().await;
        self.ensure_writable(subject)?;
        let all_versions = {
            let s = self.store.read();
            let all = s
                .versions(subject, true)
                .ok_or_else(|| SrError::SubjectNotFound(subject.to_string()))?;
            if s.versions(subject, false).is_some() {
                return Err(SrError::SubjectNotSoftDeleted(subject.to_string()));
            }
            all
        };
        let mut last_offset = -1;
        for v in &all_versions {
            let key = record::encode_tombstone(subject, *v);
            last_offset = self
                .writer
                .produce_tombstone(key)
                .await
                .map_err(|e| SrError::Backend(e.to_string()))?;
        }
        if last_offset >= 0 {
            self.await_applied(last_offset).await;
        }
        Ok(all_versions)
    }

    /// Set the global mode. `IMPORT` requires the registry to be empty.
    pub async fn set_global_mode(&self, mode: String) -> Result<(), SrError> {
        if !VALID_MODES.contains(&mode.as_str()) {
            return Err(SrError::InvalidMode(mode));
        }
        let _gate = self.write_gate.lock().await;
        if mode == "IMPORT" && !self.store.read().subjects(true).is_empty() {
            return Err(SrError::OperationNotPermitted("registry not empty".into()));
        }
        let (key, value) = record::encode_mode(None, &mode);
        let offset = self
            .writer
            .produce(key, value)
            .await
            .map_err(|e| SrError::Backend(e.to_string()))?;
        self.await_applied(offset).await;
        Ok(())
    }

    /// Set a per-subject mode. `IMPORT` requires the subject to have no versions.
    pub async fn set_subject_mode(&self, subject: &str, mode: String) -> Result<(), SrError> {
        if !VALID_MODES.contains(&mode.as_str()) {
            return Err(SrError::InvalidMode(mode));
        }
        let _gate = self.write_gate.lock().await;
        if mode == "IMPORT" && self.store.read().versions(subject, true).is_some() {
            return Err(SrError::OperationNotPermitted(subject.to_string()));
        }
        let (key, value) = record::encode_mode(Some(subject), &mode);
        let offset = self
            .writer
            .produce(key, value)
            .await
            .map_err(|e| SrError::Backend(e.to_string()))?;
        self.await_applied(offset).await;
        Ok(())
    }

    /// Clear a per-subject mode override (MODE tombstone).
    pub async fn clear_subject_mode(&self, subject: &str) -> Result<(), SrError> {
        let _gate = self.write_gate.lock().await;
        let key = record::mode_key(Some(subject));
        let offset = self
            .writer
            .produce_tombstone(key)
            .await
            .map_err(|e| SrError::Backend(e.to_string()))?;
        self.await_applied(offset).await;
        Ok(())
    }
```

- [ ] **Step 10: Fix the existing register call site.** In `rest/subjects.rs:33`, change `st.store.register(&subject, ty, &req.schema).await?` → `st.store.register(&subject, ty, &req.schema, None, None).await?`. (Task 3 wires the real `id`/`version`.)

- [ ] **Step 11: Run — expect PASS:** `cargo test -p crabka-schema-registry --test integration facade_ --lib error` → the two facade tests + error tests pass. Also re-run `--lib` + `--test interop --test compat_conformance` to confirm nothing regressed.

- [ ] **Step 12: clippy + fmt + commit.**
```bash
WT=/Users/mattstone/git/crabka/.claude/worktrees/musing-cartwright-7af144
cargo clippy -p crabka-schema-registry --all-targets -- -D warnings
cargo fmt -p crabka-schema-registry
git -C "$WT" add crates/schema-registry/src/error.rs crates/schema-registry/src/kafkastore/mod.rs \
  crates/schema-registry/src/rest/subjects.rs crates/schema-registry/tests/integration.rs
git -C "$WT" -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
schema-registry: facade soft/permanent delete + modes + READONLY gating + IMPORT register

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

> **Calibration note (Task 4):** soft-before-hard scope (does cp require it per-version AND per-subject?), the exact codes (40406/40407/42205/42204), and IMPORT's empties rules are seeded here from Confluent docs and **pinned to the cp capture in Task 4.**

---

## Task 3: REST delete + mode + lookup endpoints + `?deleted`

**Files:**
- Create: `crates/schema-registry/src/rest/delete.rs`
- Create: `crates/schema-registry/src/rest/mode.rs`
- Modify: `crates/schema-registry/src/rest/mod.rs` (routes + `pub mod` + `DeletedQ`)
- Modify: `crates/schema-registry/src/rest/subjects.rs` (`?deleted`, `referencedby`, wire register id/version)
- Modify: `crates/schema-registry/src/rest/schemas.rs` (`?deleted`, `/ids/{id}/versions`, `/schemas`)
- Test: `crates/schema-registry/tests/integration.rs`

- [ ] **Step 1: Write failing REST tests** in `tests/integration.rs` (append). `req_post`, `req_put`, `body_json`, `get_json`, `register` already exist (verified at integration.rs:345/354/46/73/55); `av` was added in Task 2. Add only the missing `req_delete` helper, then the two tests:
```rust
fn req_delete(uri: &str) -> Request<Body> {
    Request::builder()
        .method("DELETE")
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_delete_version_lifecycle_and_deleted_query() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    let body = |n: &str| format!(r#"{{"schema":{:?}}}"#, av(n));
    register(&app, "av", &body("A")).await;
    register(&app, "av", &body("B")).await;
    // soft-delete v1 → body is the bare int 1
    let r = app.clone().oneshot(req_delete("/subjects/av/versions/1")).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(body_json(r).await, serde_json::json!(1));
    assert_eq!(get_json(&app, "/subjects/av/versions").await, serde_json::json!([2]));
    assert_eq!(get_json(&app, "/subjects/av/versions?deleted=true").await, serde_json::json!([1, 2]));
    // GET v1 hidden, ?deleted shows it
    let hidden = app.clone().oneshot(Request::builder().uri("/subjects/av/versions/1").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(hidden.status(), StatusCode::NOT_FOUND);
    let shown = app.clone().oneshot(Request::builder().uri("/subjects/av/versions/1?deleted=true").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(shown.status(), StatusCode::OK);
    // permanent
    let p = app.clone().oneshot(req_delete("/subjects/av/versions/1?permanent=true")).await.unwrap();
    assert_eq!(p.status(), StatusCode::OK);
    let gone = app.clone().oneshot(Request::builder().uri("/subjects/av/versions/1?deleted=true").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(gone.status(), StatusCode::NOT_FOUND);
    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_mode_and_lookup_endpoints() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    register(&app, "a", &format!(r#"{{"schema":{:?}}}"#, av("A"))).await;
    // GET /mode default
    assert_eq!(get_json(&app, "/mode").await, serde_json::json!({"mode": "READWRITE"}));
    // PUT /mode/a READONLY then register → 422 / 42205
    let pm = app.clone().oneshot(req_put("/mode/a", r#"{"mode":"READONLY"}"#)).await.unwrap();
    assert_eq!(pm.status(), StatusCode::OK);
    let blocked = app.clone().oneshot(req_post("/subjects/a/versions", &format!(r#"{{"schema":{:?}}}"#, av("B")))).await.unwrap();
    assert_eq!(blocked.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body_json(blocked).await["error_code"], 42205);
    // GET /mode/a → READONLY ; DELETE clears
    assert_eq!(get_json(&app, "/mode/a").await, serde_json::json!({"mode": "READONLY"}));
    let dm = app.clone().oneshot(req_delete("/mode/a")).await.unwrap();
    assert_eq!(dm.status(), StatusCode::OK);
    // lookups
    let ids = get_json(&app, "/schemas/ids/1/versions").await;
    assert_eq!(ids, serde_json::json!([{"subject": "a", "version": 1}]));
    let all = get_json(&app, "/schemas").await;
    assert_eq!(all.as_array().unwrap().len(), 1);
    let refby = get_json(&app, "/subjects/a/versions/1/referencedby").await;
    assert_eq!(refby, serde_json::json!([]));
    cancel.cancel();
    broker.shutdown().await;
}
```

- [ ] **Step 2: Run — expect FAIL** (routes/handlers missing): `cargo test -p crabka-schema-registry --test integration rest_` → 404s / compile errors.

- [ ] **Step 3: Add `DeletedQ` + module decls + routes** in `rest/mod.rs`. Add `pub mod delete;` and `pub mod mode;`, add the shared query type, and extend the router. **Keep the routing import as `use axum::routing::{get, post};`** — `.delete(..)` and `.put(..)` are `MethodRouter` methods (chained off `get(..)`/`post(..)`), not free functions, so importing `delete`/`put` would be an unused-import clippy failure.
```rust
#[derive(serde::Deserialize, Default)]
pub struct DeletedQ {
    #[serde(default)]
    pub deleted: bool,
}
```
Router (replace the body of `router`):
```rust
    Router::new()
        .route("/", get(|| async { response::ok_json(&serde_json::json!({})) }))
        .route("/schemas/types", get(schemas::types))
        .route("/schemas", get(schemas::list_schemas))
        .route("/schemas/ids/{id}", get(schemas::get_by_id))
        .route("/schemas/ids/{id}/versions", get(schemas::get_by_id_versions))
        .route("/subjects", get(subjects::list))
        .route("/subjects/{subject}", post(subjects::lookup).delete(delete::delete_subject))
        .route(
            "/subjects/{subject}/versions",
            get(subjects::versions).post(subjects::register),
        )
        .route(
            "/subjects/{subject}/versions/{version}",
            get(subjects::get_version).delete(delete::delete_version),
        )
        .route(
            "/subjects/{subject}/versions/{version}/schema",
            get(subjects::get_version_schema),
        )
        .route(
            "/subjects/{subject}/versions/{version}/referencedby",
            get(subjects::referencedby),
        )
        .route("/mode", get(mode::get_global).put(mode::put_global))
        .route(
            "/mode/{subject}",
            get(mode::get_subject).put(mode::put_subject).delete(mode::delete_subject),
        )
        .route("/config", get(config::get_global).put(config::put_global))
        .route(
            "/config/{subject}",
            get(config::get_subject).put(config::put_subject),
        )
        .route(
            "/compatibility/subjects/{subject}/versions/{version}",
            post(compatibility::check),
        )
        .with_state(state)
```

- [ ] **Step 4: Create `rest/delete.rs`:**
```rust
//! DELETE endpoints for versions and subjects (soft + permanent).

use axum::extract::{Path, Query, State};
use axum::response::Response;
use serde::Deserialize;

use crate::error::SrError;
use crate::rest::{AppState, response::ok_json};

#[derive(Deserialize, Default)]
pub struct PermanentQ {
    #[serde(default)]
    permanent: bool,
}

fn parse_concrete_version(v: &str) -> Result<i32, SrError> {
    match v.parse::<i32>() {
        Ok(n) if n >= 1 => Ok(n),
        _ => Err(SrError::InvalidVersion(v.to_string())),
    }
}

/// DELETE /subjects/{subject}/versions/{version}[?permanent=true] -> <version:int>
pub async fn delete_version(
    State(st): State<AppState>,
    Path((subject, version)): Path<(String, String)>,
    Query(q): Query<PermanentQ>,
) -> Result<Response, SrError> {
    let v = if version == "latest" {
        st.store
            .store
            .read()
            .version(&subject, None, false)
            .map(|t| t.1)
            .ok_or_else(|| SrError::SubjectNotFound(subject.clone()))?
    } else {
        parse_concrete_version(&version)?
    };
    let deleted = if q.permanent {
        st.store.permanent_delete_version(&subject, v).await?
    } else {
        st.store.soft_delete_version(&subject, v).await?
    };
    Ok(ok_json(&deleted))
}

/// DELETE /subjects/{subject}[?permanent=true] -> [<versions>]
pub async fn delete_subject(
    State(st): State<AppState>,
    Path(subject): Path<String>,
    Query(q): Query<PermanentQ>,
) -> Result<Response, SrError> {
    let versions = if q.permanent {
        st.store.permanent_delete_subject(&subject).await?
    } else {
        st.store.soft_delete_subject(&subject).await?
    };
    Ok(ok_json(&versions))
}
```

- [ ] **Step 5: Create `rest/mode.rs`:**
```rust
//! `/mode` endpoints (global + per-subject).

use axum::extract::{Path, State};
use axum::response::Response;
use serde::Deserialize;

use crate::error::SrError;
use crate::rest::{AppState, response::ok_json};

#[derive(Deserialize)]
struct PutMode {
    mode: String,
}

/// GET /mode -> {"mode": "<M>"}
// axum requires async handlers even when the body is synchronous.
#[allow(clippy::unused_async)]
pub async fn get_global(State(st): State<AppState>) -> Response {
    let m = st.store.store.read().global_mode().to_string();
    ok_json(&serde_json::json!({ "mode": m }))
}

/// PUT /mode {"mode":"READONLY"} -> {"mode":"READONLY"}
pub async fn put_global(State(st): State<AppState>, body: String) -> Result<Response, SrError> {
    let req: PutMode =
        serde_json::from_str(&body).map_err(|e| SrError::InvalidMode(e.to_string()))?;
    st.store.set_global_mode(req.mode.clone()).await?;
    Ok(ok_json(&serde_json::json!({ "mode": req.mode })))
}

/// GET /mode/{subject} -> {"mode": "<M>"} | 404 if no override
pub async fn get_subject(
    State(st): State<AppState>,
    Path(subject): Path<String>,
) -> Result<Response, SrError> {
    let m = st
        .store
        .store
        .read()
        .subject_mode(&subject)
        .map(str::to_string)
        .ok_or_else(|| SrError::SubjectNotFound(subject.clone()))?;
    Ok(ok_json(&serde_json::json!({ "mode": m })))
}

/// PUT /mode/{subject} {"mode":"IMPORT"} -> {"mode":"IMPORT"}
pub async fn put_subject(
    State(st): State<AppState>,
    Path(subject): Path<String>,
    body: String,
) -> Result<Response, SrError> {
    let req: PutMode =
        serde_json::from_str(&body).map_err(|e| SrError::InvalidMode(e.to_string()))?;
    st.store.set_subject_mode(&subject, req.mode.clone()).await?;
    Ok(ok_json(&serde_json::json!({ "mode": req.mode })))
}

/// DELETE /mode/{subject} -> {"mode": "<prior>"} (clears the override)
pub async fn delete_subject(
    State(st): State<AppState>,
    Path(subject): Path<String>,
) -> Result<Response, SrError> {
    let prior = st
        .store
        .store
        .read()
        .subject_mode(&subject)
        .map(str::to_string)
        .ok_or_else(|| SrError::SubjectNotFound(subject.clone()))?;
    st.store.clear_subject_mode(&subject).await?;
    Ok(ok_json(&serde_json::json!({ "mode": prior })))
}
```

- [ ] **Step 6: Wire `?deleted` + `referencedby` + register id/version** in `rest/subjects.rs`.
  - Add the import: `use crate::rest::{AppState, DeletedQ, response::{ok_json, ok_raw}};` and `use axum::extract::{Path, Query, State};`.
  - Extend `RegisterBody` with `#[serde(default)] id: Option<i32>` and `#[serde(default)] version: Option<i32>`, and change the `register` handler's store call to `st.store.register(&subject, ty, &req.schema, req.id, req.version).await?`.
  - `list`: `pub async fn list(State(st): State<AppState>, Query(q): Query<DeletedQ>) -> Response { ok_json(&st.store.store.read().subjects(q.deleted)) }`.
  - `versions`: add `Query(q): Query<DeletedQ>`, replace the body with `let vs = st.store.store.read().versions(&subject, q.deleted).ok_or_else(|| SrError::SubjectNotFound(subject.clone()))?; Ok(ok_json(&vs))`.
  - `get_version`: add `Query(q): Query<DeletedQ>`; replace `s.versions(&subject, false)` → `s.versions(&subject, q.deleted)` and `s.version(&subject, want, false)` → `s.version(&subject, want, q.deleted)`.
  - `get_version_schema`: add `Query(q): Query<DeletedQ>`; thread `q.deleted` the same way.
  - `lookup`: add `Query(q): Query<DeletedQ>`; replace `s.versions(&subject, false)` → `s.versions(&subject, q.deleted)`, `s.find_under_subject(&subject, ty, &req.schema, false)` → `..., q.deleted)`, `s.schema_by_id(found.id, false)` → `..., q.deleted)`.
  - Add `referencedby` (reuses `parse_version`):
```rust
/// GET /subjects/{subject}/versions/{version}/referencedby -> [] (slice 4 adds real data)
pub async fn referencedby(
    State(st): State<AppState>,
    Path((subject, version)): Path<(String, String)>,
) -> Result<Response, SrError> {
    let want = parse_version(&version)?;
    let s = st.store.store.read();
    if s.versions(&subject, true).is_none() {
        return Err(SrError::SubjectNotFound(subject));
    }
    s.version(&subject, want, true).ok_or(SrError::VersionNotFound)?;
    Ok(ok_json(&serde_json::json!([])))
}
```

- [ ] **Step 7: Add `?deleted` + the two id-lookups** in `rest/schemas.rs`. Switch imports to `use axum::extract::{Path, Query, State};` and `use crate::rest::{AppState, DeletedQ, response::ok_json};`. Modify `get_by_id` to take `Query(q): Query<DeletedQ>` and call `schema_by_id(id, q.deleted)`. Add:
```rust
/// GET /schemas/ids/{id}/versions -> [{"subject":..,"version":..}]
#[allow(clippy::unused_async)]
pub async fn get_by_id_versions(
    State(st): State<AppState>,
    Path(id): Path<i32>,
    Query(q): Query<DeletedQ>,
) -> Response {
    let pairs = st.store.store.read().schema_id_subject_versions(id, q.deleted);
    let arr: Vec<serde_json::Value> = pairs
        .into_iter()
        .map(|(subject, version)| serde_json::json!({ "subject": subject, "version": version }))
        .collect();
    ok_json(&serde_json::Value::Array(arr))
}

/// GET /schemas -> [{subject,version,id,schemaType,schema}]
#[allow(clippy::unused_async)]
pub async fn list_schemas(
    State(st): State<AppState>,
    Query(q): Query<DeletedQ>,
) -> Response {
    let rows = st.store.store.read().all_schemas(q.deleted);
    let arr: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(subject, version, id, ty, schema)| {
            let mut m = serde_json::Map::new();
            m.insert("subject".into(), subject.into());
            m.insert("version".into(), version.into());
            m.insert("id".into(), id.into());
            if let Some(t) = ty.wire_name() {
                m.insert("schemaType".into(), t.into());
            }
            m.insert("schema".into(), schema.into());
            serde_json::Value::Object(m)
        })
        .collect();
    ok_json(&serde_json::Value::Array(arr))
}
```

- [ ] **Step 8: Run — expect PASS:** `cargo test -p crabka-schema-registry --test integration rest_` → both REST tests pass. Re-run `--lib --test interop --test compat_conformance` to confirm no regressions.

- [ ] **Step 9: clippy + fmt + commit.**
```bash
WT=/Users/mattstone/git/crabka/.claude/worktrees/musing-cartwright-7af144
cargo clippy -p crabka-schema-registry --all-targets -- -D warnings
cargo fmt -p crabka-schema-registry
git -C "$WT" add crates/schema-registry/src/rest/
git -C "$WT" add crates/schema-registry/tests/integration.rs
git -C "$WT" -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
schema-registry: REST delete/mode endpoints + ?deleted filtering + id/schema lookups

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: cp Docker capture + error-code/byte calibration + full-lifecycle tests

**Files:**
- Create: `crates/schema-registry/tests/capture_admin_fixtures.rs`
- Create (generated): `crates/schema-registry/tests/fixtures/admin/records.json`, `crates/schema-registry/tests/fixtures/admin/rest.json`
- Modify (calibrate): `crates/schema-registry/src/error.rs`, `crates/schema-registry/src/kafkastore/record.rs` (only if cp bytes differ)
- Test: `crates/schema-registry/tests/integration.rs` (full-lifecycle, asserts calibrated codes), `crates/schema-registry/src/kafkastore/record.rs` (round-trip-vs-fixture)

- [ ] **Step 1: Write the `#[ignore]` Docker capture harness** `tests/capture_admin_fixtures.rs`, modeled on `tests/capture_compat_fixtures.rs` (READ it — copy the `start_host_broker` / `docker_pull` / `docker_run_schema_registry` / `docker_mapped_port` / `wait_for_registry` / `ContainerGuard` helpers verbatim). Then drive a real `cp-schema-registry:7.4.0` through the admin lifecycle and capture two things:

  **(a) REST behaviors + numeric error codes** → `tests/fixtures/admin/rest.json` (array of `{op, method, path, status, body}`). Drive, in order, recording each response's HTTP status + parsed body:
  1. `POST /subjects/t/versions` `{"schema":"{...avro A...}"}` and again `{...B...}` (two versions).
  2. `DELETE /subjects/t/versions/1` (soft) → record status + body (expect the bare version int).
  3. `GET /subjects/t/versions` and `GET /subjects/t/versions?deleted=true` → record arrays.
  4. `GET /subjects/t/versions/1` (expect 404 + the soft-deleted error code) and `?deleted=true` (expect 200).
  5. `DELETE /subjects/t/versions/2` `?permanent=true` WITHOUT a prior soft delete → record the **soft-before-hard error code** (the key calibration target; expected ~`40407`).
  6. `DELETE /subjects/t/versions/1?permanent=true` (after soft) → record success.
  7. `POST /subjects/d/versions` then `DELETE /subjects/d` (soft) and `DELETE /subjects/d?permanent=true` → record statuses + bodies; also `DELETE /subjects/d?permanent=true` before soft → record the **subject soft-before-hard code**.
  8. `PUT /mode/r {"mode":"READONLY"}` then `POST /subjects/r/versions {...}` → record the **READONLY error code** (expected ~`42205`) + status.
  9. `GET /mode`, `GET /mode/r`, `DELETE /mode/r` → record bodies.
  10. `PUT /mode/i {"mode":"IMPORT"}` then `POST /subjects/i/versions {"schema":"...","id":42,"version":5}` → record the IMPORT register response (id/version echoed).
  11. `GET /schemas`, `GET /schemas/ids/1/versions`, `GET /subjects/t/versions/1/referencedby` → record shapes.

  **(b) The `_schemas` record bytes cp emits** → `tests/fixtures/admin/records.json`. After driving the lifecycle, consume `_schemas` partition 0 from the in-process broker and dump every `(offset, key_utf8, value_utf8_or_null)`. Reuse the reader's fetch primitive:
```rust
// after the lifecycle, before broker.shutdown():
use crabka_client_core::{Connection, ConnectionOptions, fetch_partition};
// resolve the _schemas topic_id the same way the registry does (topic::ensure_schemas_topic
// returns it on create; or fetch metadata). Then:
let conn = Connection::connect_with_options(broker_addr, ConnectionOptions::default()).await.unwrap();
let mut out = Vec::new();
let mut next = 0;
loop {
    let recs = fetch_partition(&conn, "_schemas", topic_id, 0, next, 500, 1 << 20).await.unwrap();
    if recs.is_empty() { break; }
    for r in &recs {
        out.push(serde_json::json!({
            "offset": r.offset,
            "key": r.key.as_deref().map(|k| String::from_utf8_lossy(k).to_string()),
            "value": r.value.as_deref().map(|v| String::from_utf8_lossy(v).to_string()),
        }));
        next = r.offset + 1;
    }
}
// write out -> tests/fixtures/admin/records.json
```
Write both fixtures with `serde_json::to_string_pretty`. (If resolving `topic_id` is awkward, fetch broker metadata for `_schemas` first; the slice-1 `topic.rs` shows the pattern.)

- [ ] **Step 2: Run the capture (Docker):** `cargo test -p crabka-schema-registry --test capture_admin_fixtures -- --ignored --nocapture`. Confirm `tests/fixtures/admin/{records,rest}.json` are written. **If Docker is unavailable, STOP and report — the controller runs the capture. Do NOT fabricate fixtures.**

- [ ] **Step 3: Inspect + report the ground truth.** Report, from the captured fixtures:
  - the **exact numeric error codes** for: permanent-version-before-soft, permanent-subject-before-soft, READONLY-rejected-write, soft-deleted-version GET (without `?deleted`), invalid mode;
  - the **exact `_schemas` bytes** for: a soft-delete (SCHEMA key + value with `deleted`), a `MODE` record (key + value), a `DELETE_SUBJECT` record (key + value), a permanent-delete tombstone (SCHEMA key + null value);
  - the soft-delete DELETE response body shape (bare int vs object) and the subject-delete body (`[versions]`).

- [ ] **Step 4: CALIBRATE `error.rs` to the captured codes.** For each error whose captured code differs from the Task-2 seed, fix `error_code()` / `http_status()` to match cp exactly. Update the `slice3_codes` unit test's expected numbers. Re-run `cargo test -p crabka-schema-registry --lib error`. **Report every code change (seed → cp).**

- [ ] **Step 5: CALIBRATE `record.rs` bytes (only if cp differs).** Compare the captured `_schemas` key/value bytes to what `encode_mode`/`encode_delete_subject`/`encode_schema_deleted`/`encode_tombstone` produce. If cp's field order, `magic` byte, keytype string, or value shape differs, fix the structs/encoders and update the Task-1 round-trip tests' expected bytes. **Report every byte-shape change.** If they already match, note "cp confirmed seeded shapes."

- [ ] **Step 6: Add record round-trip-vs-fixture tests** in `record.rs` `mod tests` (mirrors slice-1's fixture discipline — assert our encoders reproduce the cp-captured keys). Load the captured exemplar bytes (inline the confirmed cp strings as `const`s pulled from `records.json`) and assert equality, e.g.:
```rust
    #[test]
    fn mode_key_matches_cp_capture() {
        // confirmed against tests/fixtures/admin/records.json (cp 7.4.0)
        let (k, _v) = encode_mode(Some("r"), "READONLY");
        assert_eq!(&k, br#"{"keytype":"MODE","subject":"r","magic":0}"#);
    }
    #[test]
    fn delete_subject_key_matches_cp_capture() {
        let (k, _v) = encode_delete_subject("d", 1);
        assert_eq!(&k, br#"{"keytype":"DELETE_SUBJECT","subject":"d","magic":0}"#);
    }
```
(Replace the expected bytes with the EXACT captured strings if Step 5 changed them.)

- [ ] **Step 7: Add the full-lifecycle in-process integration tests** in `tests/integration.rs` (Mac-friendly, single broker, REST via `oneshot`), asserting the **calibrated** codes. Cover the gaps not already in Tasks 2–3:
```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_subject_soft_then_permanent_and_soft_before_hard() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    register(&app, "s", &format!(r#"{{"schema":{:?}}}"#, av("A"))).await;
    register(&app, "s", &format!(r#"{{"schema":{:?}}}"#, av("B"))).await;
    // permanent subject before soft → calibrated soft-before-hard code (e.g. 40406)
    let early = app.clone().oneshot(req_delete("/subjects/s?permanent=true")).await.unwrap();
    assert_eq!(early.status(), StatusCode::NOT_FOUND);
    assert_eq!(body_json(early).await["error_code"], 40406); // <- set to captured code
    // soft then permanent
    let soft = app.clone().oneshot(req_delete("/subjects/s")).await.unwrap();
    assert_eq!(soft.status(), StatusCode::OK);
    assert_eq!(body_json(soft).await, serde_json::json!([1, 2]));
    assert!(get_json(&app, "/subjects").await.as_array().unwrap().is_empty());
    let perm = app.clone().oneshot(req_delete("/subjects/s?permanent=true")).await.unwrap();
    assert_eq!(perm.status(), StatusCode::OK);
    assert_eq!(get_json(&app, "/subjects?deleted=true").await, serde_json::json!([]));
    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_import_mode_registers_explicit_id() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    assert_eq!(
        app.clone().oneshot(req_put("/mode/imp", r#"{"mode":"IMPORT"}"#)).await.unwrap().status(),
        StatusCode::OK
    );
    let body = format!(r#"{{"schema":{:?},"id":42,"version":5}}"#, av("C"));
    let r = app.clone().oneshot(req_post("/subjects/imp/versions", &body)).await.unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(body_json(r).await["id"], 42);
    let got = get_json(&app, "/subjects/imp/versions/5").await;
    assert_eq!(got["id"], 42);
    cancel.cancel();
    broker.shutdown().await;
}
```
Adjust the asserted `error_code` literals to the **captured** values from Step 3 (the `40406` above is a placeholder for the captured subject-soft-before-hard code).

- [ ] **Step 8: Run everything** (no Docker): `cargo test -p crabka-schema-registry --lib --test integration --test interop --test compat_conformance` → all green; Avro 21 / Protobuf 88 / JSON 92 unchanged. clippy + fmt.

- [ ] **Step 9: Commit.**
```bash
WT=/Users/mattstone/git/crabka/.claude/worktrees/musing-cartwright-7af144
cargo clippy -p crabka-schema-registry --all-targets -- -D warnings
cargo fmt -p crabka-schema-registry
git -C "$WT" add crates/schema-registry/tests/capture_admin_fixtures.rs \
  crates/schema-registry/tests/fixtures/admin/ \
  crates/schema-registry/src/error.rs crates/schema-registry/src/kafkastore/record.rs \
  crates/schema-registry/tests/integration.rs
# (also add Cargo.toml if the capture test needs a new dev-dep stanza — it should reuse existing reqwest/testcontainers)
git -C "$WT" -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "$(cat <<'EOF'
schema-registry: cp-calibrated admin error codes + _schemas record bytes + full delete/mode/lookup lifecycle tests

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

## Self-review (completed by plan author)

**Spec coverage:**
- Store model (`VersionEntry.deleted`, `global_mode`/`subject_mode`, mutators, deleted-aware queries, `effective_mode`) → Task 1c.
- `_schemas` record families (Mode/DeleteSubject/Tombstone + encode/decode + `produce_tombstone`) → Task 1a/1b; cp byte-confirmation → Task 4 (Steps 5–6).
- Reader arms (Mode/DeleteSubject/Tombstone/SCHEMA-deleted) → Task 1d.
- Facade soft/permanent delete (version + subject), soft-before-hard, READONLY gating, IMPORT register → Task 2b.
- REST: `DELETE /subjects/{s}/versions/{v}[?permanent]`, `DELETE /subjects/{s}[?permanent]`, `GET/PUT /mode`, `GET/PUT/DELETE /mode/{subject}`, `GET /schemas/ids/{id}/versions`, `referencedby` → `[]`, `GET /schemas`, `?deleted` on the GETs/lookup → Task 3.
- Error model (OperationNotPermitted/SubjectNotSoftDeleted/VersionNotSoftDeleted/InvalidMode) → Task 2a; cp code calibration → Task 4 (Step 4).
- Validation: `capture_admin_fixtures.rs` (record bytes + REST codes) → Task 4 Steps 1–3; in-process lifecycle (soft→`?deleted`→permanent→404, subject delete, soft-before-hard, READONLY, IMPORT, lookups) → Tasks 2/3/4; record round-trip vs fixtures → Task 1a (round-trip) + Task 4 Step 6 (vs cp bytes).
- Out of scope honored: `referencedby` returns `[]` (slice 4); no `GET /schemas` pagination; no READONLY_OVERRIDE/contexts/export. Avro/Protobuf/JSON compat untouched.

**Placeholder scan:** the only "seed then calibrate" items — the `magic` bytes / record field order (Task 1) and the numeric error codes (Task 2) — are explicitly cp-confirmed in Task 4 (the spec's authority discipline), not unfilled placeholders. Every code step shows complete code. The `40406` literal in Task 4 Step 7 is explicitly flagged "set to captured code."

**Type consistency:** signatures are threaded consistently — `versions(subject, include_deleted)`, `version(subject, Option<i32>, include_deleted)`, `subjects(include_deleted)`, `schema_by_id(id, include_deleted)`, `find_under_subject(subject, ty, schema, include_deleted)`, `schema_id_subject_versions(id, include_deleted)`, `all_schemas(include_deleted)`; facade `register(subject, ty, schema, import_id, import_version)`; record `encode_mode(Option<&str>, &str)`, `mode_key(Option<&str>)`, `encode_delete_subject(&str, i32)`, `encode_schema_deleted(..)`, `encode_tombstone(&str, i32) -> Vec<u8>`; `SchemaRecord::{Mode(ModeKey, Option<ModeValue>), DeleteSubject(DeleteSubjectKey, DeleteSubjectValue), Tombstone(SchemaKey)}`; `produce_tombstone(Vec<u8>)`; `SrError::{OperationNotPermitted, SubjectNotSoftDeleted, VersionNotSoftDeleted, InvalidMode}`; shared `rest::DeletedQ`. Every call-site update in Task 1 Step 18 matches a signature defined in Task 1c. The reader arms (Task 1d) call only store methods defined in Task 1c (`apply_schema`, `permanent_delete_version`, `soft_delete_subject`, `set_*_mode`, `clear_*_mode`); the facade (Task 2) calls only store queries + record encoders + `produce`/`produce_tombstone` defined earlier.

**Gaps fixed during review:** `produce_tombstone` was added (the existing `produce` always sends a non-null value — tombstones for permanent delete + mode-clear need it); `apply_schema` was rewritten rather than extended (the old early-`return` on `deleted` would have dropped soft-delete + resurrection folds); `permanent_delete_subject` is realized via per-version tombstones (cp's compaction-correct shape) rather than a dedicated store mutator, avoiding dead code.
