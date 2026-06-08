# Schema Registry Slice 9 — REST Surface Completeness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close four Confluent Schema Registry REST API gaps: `DELETE /config/{subject}`, `GET /schemas/ids/{id}/schema`, `GET /schemas/ids/{id}/subjects`, and `?normalize=true` on register/lookup.

**Architecture:** Add `config_key` serializer to `kafkastore/record.rs` (mirrors existing `mode_key`), add `delete_subject_compat` tombstone method to `kafkastore/mod.rs`, add three REST handlers, thread `?normalize` through register/lookup, and register all new routes. Five files in the crate proper plus the integration test file. Batched: Task 1 (record.rs only) → Task 2 (rest layer, 5 files, depends on Task 1) → Task 3 (integration tests, depends on Task 2).

**Tech Stack:** Rust, axum 0.8, apache-avro (already in Cargo.toml), serde_json, tower::ServiceExt for in-process test routing.

**Spec:** `docs/superpowers/specs/2026-06-08-crabka-schema-registry-slice-9-rest-completeness-design.md`

---

## File Map

| File | Change |
|------|--------|
| `crates/schema-registry/src/kafkastore/record.rs` | Add `pub fn config_key(subject: Option<&str>) -> Vec<u8>` after `mode_key`; add 2 unit tests |
| `crates/schema-registry/src/kafkastore/mod.rs` | Add `pub async fn delete_subject_compat(&self, subject: &str) -> Result<Option<String>, SrError>` after `set_subject_compat` |
| `crates/schema-registry/src/rest/config.rs` | Add `pub async fn delete_subject(...)` handler; add `ok_json` usage (already imported) |
| `crates/schema-registry/src/rest/schemas.rs` | Add `get_by_id_schema` and `get_by_id_subjects` handlers; add `ok_raw` to import |
| `crates/schema-registry/src/rest/subjects.rs` | Add `NormalizeQuery` struct and `normalize_schema` free fn; modify `register` and `lookup` signatures |
| `crates/schema-registry/src/rest/mod.rs` | Add `.delete(config::delete_subject)` to existing `/config/{subject}` route; add 2 new `/schemas/ids/{id}/*` routes |
| `crates/schema-registry/tests/integration.rs` | Add `delete_req` helper; add 5 integration tests |

---

## Batch Structure

- **Batch 1:** Task 1 alone (only touches `record.rs`)
- **Batch 2:** Task 2 alone (5 REST-layer files; requires `config_key` from Task 1)
- **Batch 3:** Task 3 alone (integration tests; requires all handlers from Task 2)

---

## Task 1 — `config_key` in `kafkastore/record.rs`

**Files:**
- Modify: `crates/schema-registry/src/kafkastore/record.rs:288-295` (after `mode_key`), lines `315+` (existing test module)

### Critical context

`ConfigKey` struct is already defined in this file (around line 71):
```rust
#[derive(Debug, Serialize, Deserialize)]
struct ConfigKey {
    keytype: String,
    subject: Option<String>,
    magic: u8,
}
```

`mode_key` (lines 288-295) is the exact template:
```rust
pub fn mode_key(subject: Option<&str>) -> Vec<u8> {
    let key = ModeKey {
        keytype: "MODE".to_string(),
        subject: subject.map(str::to_string),
        magic: 0,
    };
    serde_json::to_vec(&key).expect("mode key serialises")
}
```

The test module starts at line 315 with `#[cfg(test)] mod tests { use super::*; ... }`.

- [ ] **Step 1: Write failing unit tests (add to existing `#[cfg(test)]` block at the end of the file)**

Add at the bottom of the `tests` module in `record.rs`:

```rust
    #[test]
    fn config_key_serializes_subject() {
        let k = config_key(Some("my-subject"));
        let v: serde_json::Value = serde_json::from_slice(&k).unwrap();
        assert_eq!(v["keytype"], "CONFIG");
        assert_eq!(v["subject"], "my-subject");
        assert_eq!(v["magic"], 0);
    }

    #[test]
    fn config_key_global_has_null_subject() {
        let k = config_key(None);
        let v: serde_json::Value = serde_json::from_slice(&k).unwrap();
        assert_eq!(v["keytype"], "CONFIG");
        assert_eq!(v["subject"], serde_json::Value::Null);
        assert_eq!(v["magic"], 0);
    }
```

- [ ] **Step 2: Run to confirm it fails (function not yet defined)**

```bash
cargo test -p crabka-schema-registry --lib -- record::tests::config_key 2>&1 | tail -5
```

Expected: compile error `error[E0425]: cannot find function 'config_key'`.

- [ ] **Step 3: Add `config_key` function to `record.rs`, directly after `mode_key`**

Insert after the closing `}` of `mode_key` (around line 295):

```rust
/// Serialise just the CONFIG key for a subject (or global when `subject` is
/// `None`). Used to produce a tombstone that removes per-subject overrides.
pub fn config_key(subject: Option<&str>) -> Vec<u8> {
    let key = ConfigKey {
        keytype: "CONFIG".to_string(),
        subject: subject.map(str::to_string),
        magic: 0,
    };
    serde_json::to_vec(&key).expect("config key serialises")
}
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
cargo test -p crabka-schema-registry --lib -- record::tests 2>&1 | tail -10
```

Expected: `test kafkastore::record::tests::config_key_serializes_subject ... ok` and `test kafkastore::record::tests::config_key_global_has_null_subject ... ok` plus all prior record tests still green.

- [ ] **Step 5: Clippy + fmt**

```bash
cargo clippy -p crabka-schema-registry --all-targets -- -D warnings 2>&1 | tail -5
cargo fmt -p crabka-schema-registry 2>&1
```

Both expected: no output (clean).

- [ ] **Step 6: Commit**

```bash
git -C /Users/mattstone/git/crabka add crates/schema-registry/src/kafkastore/record.rs
git -C /Users/mattstone/git/crabka \
  -c user.name="Matthew Stone" \
  -c user.email="matthew.d.stone@gmail.com" \
  commit -m "$(cat <<'EOF'
feat(schema-registry): add config_key helper for CONFIG tombstones

Mirrors mode_key. Used in the upcoming delete_subject_compat to
produce a Kafka tombstone that reverts per-subject compat overrides.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2 — REST Layer (5 files)

**Files:**
- Modify: `crates/schema-registry/src/kafkastore/mod.rs` (after `set_subject_compat`, ~line 162)
- Modify: `crates/schema-registry/src/rest/config.rs` (add `delete_subject` handler)
- Modify: `crates/schema-registry/src/rest/schemas.rs` (add two handlers, update import)
- Modify: `crates/schema-registry/src/rest/subjects.rs` (add NormalizeQuery, normalize_schema, modify register + lookup)
- Modify: `crates/schema-registry/src/rest/mod.rs` (three route additions)

### Critical context — read before editing

- `st.store.store.read()` — the **double** `.store` is required for reads; the outer `store` is `KafkaStore`, the inner `store` is its `Arc<RwLock<StoreState>>` field.
- `SrError::SchemaNotFound` — **no argument**. (Unlike `SrError::SubjectNotFound(subject)` which takes a String.)
- `ok_json` in `config.rs` is imported at the top as `use crate::rest::response::ok_json;` — already present.
- `ok_raw` in `schemas.rs` must be added to the import: change `use crate::rest::response::ok_json;` → `use crate::rest::response::{ok_json, ok_raw};`.
- Route additions in `mod.rs` use **method chaining** (`.delete(handler)` on an existing `MethodRouter`), not free-function syntax. The `delete` free function is **not** imported and does not need to be.
- `apache_avro` is already in `[dependencies]` — no Cargo.toml changes needed. Reference it as `apache_avro::Schema::parse_str(...)` (no `use` required).

### Sub-step A — `delete_subject_compat` in `kafkastore/mod.rs`

Add after `set_subject_compat` (around line 162):

```rust
/// Remove the per-subject compat override and revert to global. Returns the
/// deleted level string (e.g. `"BACKWARD"`) or `None` if no per-subject
/// override was set.
pub async fn delete_subject_compat(
    &self,
    subject: &str,
) -> Result<Option<String>, SrError> {
    let _gate = self.write_gate.lock().await;
    let current = self.store.read().subject_compat(subject).map(str::to_string);
    let Some(level) = current else {
        return Ok(None);
    };
    let key = record::config_key(Some(subject));
    let offset = self
        .writer
        .produce_tombstone(key)
        .await
        .map_err(|e| SrError::Backend(e.to_string()))?;
    self.await_applied(offset).await;
    Ok(Some(level))
}
```

Pattern mirrors `clear_subject_mode` (lock write gate → check state → produce tombstone → await_applied). Unlike `clear_subject_mode`, we check state first because the caller (REST handler) needs to return 404 when there is no per-subject override.

### Sub-step B — `delete_subject` handler in `rest/config.rs`

Current end of `config.rs` has `put_subject` — add `delete_subject` after it:

```rust
/// DELETE /config/{subject} — remove per-subject compat override, revert to global.
///
/// Returns 200 `{"compatibility": "<deleted-level>"}` or 404 SubjectNotFound
/// if no per-subject override was set.
pub async fn delete_subject(
    State(st): State<AppState>,
    Path(subject): Path<String>,
) -> Result<Response, SrError> {
    match st.store.delete_subject_compat(&subject).await? {
        Some(level) => Ok(ok_json(&serde_json::json!({"compatibility": level}))),
        None => Err(SrError::SubjectNotFound(subject)),
    }
}
```

No import changes needed — `ok_json`, `State`, `Path`, `Response`, `AppState`, `SrError` are all already imported in `config.rs`.

### Sub-step C — two handlers in `rest/schemas.rs`

**Import change** (line 10 currently reads `use crate::rest::response::ok_json;`):

```rust
use crate::rest::response::{ok_json, ok_raw};
```

**Add `get_by_id_schema`** after the existing `get_by_id` handler:

```rust
/// GET /schemas/ids/{id}/schema — return the raw schema string (not JSON-wrapped).
pub async fn get_by_id_schema(
    State(st): State<AppState>,
    Path(id): Path<i32>,
) -> Result<Response, SrError> {
    let (_, schema, _) = st
        .store
        .store
        .read()
        .schema_by_id(id, false)
        .ok_or(SrError::SchemaNotFound)?;
    Ok(ok_raw(schema))
}
```

**Add `get_by_id_subjects`** after `get_by_id_schema`:

```rust
/// GET /schemas/ids/{id}/subjects[?deleted=true] — list subjects referencing this id.
pub async fn get_by_id_subjects(
    State(st): State<AppState>,
    Path(id): Path<i32>,
    Query(q): Query<DeletedQ>,
) -> Result<Response, SrError> {
    let pairs = st
        .store
        .store
        .read()
        .schema_id_subject_versions(id, q.deleted);
    if pairs.is_empty() {
        return Err(SrError::SchemaNotFound);
    }
    let mut subjects: Vec<String> = pairs
        .into_iter()
        .map(|(s, _)| s)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    subjects.sort_unstable();
    Ok(ok_json(&subjects))
}
```

### Sub-step D — `NormalizeQuery`, `normalize_schema`, and modified `register`/`lookup` in `rest/subjects.rs`

**Add `NormalizeQuery` struct** after the existing `RegisterBody` struct (around line 25):

```rust
#[derive(Debug, Default, serde::Deserialize)]
struct NormalizeQuery {
    #[serde(default)]
    normalize: bool,
}
```

**Add `normalize_schema` free function** before `register` (around line 28):

```rust
/// Normalize a schema string to its canonical form for the given type.
/// - Avro: Parsing Canonical Form via `apache_avro::Schema::canonical_form()`
/// - JSON Schema: round-trip through serde_json (strips whitespace, sorts keys)
/// - Protobuf: no-op (Confluent SR does not define textual normalization for proto)
fn normalize_schema(ty: SchemaType, schema: &str) -> Result<String, SrError> {
    match ty {
        SchemaType::Avro => apache_avro::Schema::parse_str(schema)
            .map(|s| s.canonical_form())
            .map_err(|e| SrError::InvalidSchema(e.to_string())),
        SchemaType::Json => serde_json::from_str::<serde_json::Value>(schema)
            .map(|v| v.to_string())
            .map_err(|e| SrError::InvalidSchema(e.to_string())),
        SchemaType::Protobuf => Ok(schema.to_string()),
    }
}
```

**Replace `register` handler** (the whole function, lines 27-48 approximately):

```rust
/// POST /subjects/{subject}/versions[?normalize=true] -> `{"id":N}`
pub async fn register(
    State(st): State<AppState>,
    Path(subject): Path<String>,
    Query(n): Query<NormalizeQuery>,
    body: String,
) -> Result<Response, SrError> {
    let req: RegisterBody =
        serde_json::from_str(&body).map_err(|e| SrError::InvalidSchema(e.to_string()))?;
    let ty = SchemaType::from_wire(req.schema_type.as_deref());
    let schema = if n.normalize {
        normalize_schema(ty, &req.schema)?
    } else {
        req.schema.clone()
    };
    let reg = st
        .store
        .register(
            &subject,
            ty,
            &schema,
            &req.references,
            req.id,
            req.version,
        )
        .await?;
    Ok(ok_json(&serde_json::json!({ "id": reg.id })))
}
```

**Replace `lookup` handler** (the whole function, lines 50-80 approximately):

```rust
/// POST /subjects/{subject}[?normalize=true][&deleted=true] -> `{subject,id,version,schema}` | 404
pub async fn lookup(
    State(st): State<AppState>,
    Path(subject): Path<String>,
    Query(q): Query<DeletedQ>,
    Query(n): Query<NormalizeQuery>,
    body: String,
) -> Result<Response, SrError> {
    let req: RegisterBody =
        serde_json::from_str(&body).map_err(|e| SrError::InvalidSchema(e.to_string()))?;
    let ty = SchemaType::from_wire(req.schema_type.as_deref());
    let schema_to_find = if n.normalize {
        normalize_schema(ty, &req.schema)?
    } else {
        req.schema.clone()
    };
    let s = st.store.store.read();
    if s.versions(&subject, q.deleted).is_none() {
        return Err(SrError::SubjectNotFound(subject));
    }
    let Some(found) =
        s.find_under_subject(&subject, ty, &schema_to_find, &req.references, q.deleted)
    else {
        return Err(SrError::SchemaNotFound);
    };
    let (sty, schema, _references) = s
        .schema_by_id(found.id, q.deleted)
        .ok_or(SrError::SchemaNotFound)?;
    let mut m = serde_json::Map::new();
    m.insert("subject".into(), subject.into());
    m.insert("id".into(), found.id.into());
    m.insert("version".into(), found.version.into());
    if let Some(t) = sty.wire_name() {
        m.insert("schemaType".into(), t.into());
    }
    m.insert("schema".into(), schema.into());
    Ok(ok_json(&serde_json::Value::Object(m)))
}
```

### Sub-step E — three route additions in `rest/mod.rs`

**Modify the `/config/{subject}` route** (around line 75) — add `.delete(config::delete_subject)`:

Current:
```rust
        .route(
            "/config/{subject}",
            get(config::get_subject).put(config::put_subject),
        )
```

New:
```rust
        .route(
            "/config/{subject}",
            get(config::get_subject)
                .put(config::put_subject)
                .delete(config::delete_subject),
        )
```

**Add two new `/schemas/ids/{id}/*` routes** after the existing `/schemas/ids/{id}/versions` route (around line 45):

Current:
```rust
        .route(
            "/schemas/ids/{id}/versions",
            get(schemas::get_by_id_versions),
        )
```

New (add the two routes after):
```rust
        .route(
            "/schemas/ids/{id}/versions",
            get(schemas::get_by_id_versions),
        )
        .route(
            "/schemas/ids/{id}/schema",
            get(schemas::get_by_id_schema),
        )
        .route(
            "/schemas/ids/{id}/subjects",
            get(schemas::get_by_id_subjects),
        )
```

No new `use` statements needed in `mod.rs`.

### Build and check

- [ ] **Step 1: Make all sub-step A–E changes above**

- [ ] **Step 2: Build to catch compile errors**

```bash
cargo build -p crabka-schema-registry 2>&1 | tail -20
```

Expected: `Finished` with no errors. Common pitfalls to watch for:
- `config_key not found` → Task 1 branch not merged yet (run after Task 1 commit)
- `SrError::SchemaNotFound(id)` with argument → remove the argument; the variant takes none
- `st.store.read()` → must be `st.store.store.read()` (double `.store`)

- [ ] **Step 3: Clippy**

```bash
cargo clippy -p crabka-schema-registry --all-targets -- -D warnings 2>&1 | tail -20
```

Expected: clean. Watch for:
- `large_futures` if a handler is unexpectedly large (unlikely here)
- `unused_imports` if you accidentally left a dead import

- [ ] **Step 4: Format**

```bash
cargo fmt -p crabka-schema-registry
```

- [ ] **Step 5: Run library unit tests (fast sanity)**

```bash
cargo test -p crabka-schema-registry --lib 2>&1 | tail -10
```

Expected: all pass. The `StoreState` unit tests exercise `subject_compat` directly.

- [ ] **Step 6: Commit**

```bash
git -C /Users/mattstone/git/crabka add \
  crates/schema-registry/src/kafkastore/mod.rs \
  crates/schema-registry/src/rest/config.rs \
  crates/schema-registry/src/rest/schemas.rs \
  crates/schema-registry/src/rest/subjects.rs \
  crates/schema-registry/src/rest/mod.rs
git -C /Users/mattstone/git/crabka \
  -c user.name="Matthew Stone" \
  -c user.email="matthew.d.stone@gmail.com" \
  commit -m "$(cat <<'EOF'
feat(schema-registry): slice 9 REST surface completeness

- DELETE /config/{subject}: reverts per-subject compat to global via
  CONFIG tombstone (delete_subject_compat in KafkaStore facade)
- GET /schemas/ids/{id}/schema: raw schema string (ok_raw, not wrapped)
- GET /schemas/ids/{id}/subjects: deduped sorted subject list for an id
- ?normalize=true on POST /subjects/{subject}/versions and
  POST /subjects/{subject}: Avro PCF, JSON round-trip, Protobuf no-op

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3 — Integration Tests

**Files:**
- Modify: `crates/schema-registry/tests/integration.rs`

### Critical context

- Tests use `boot_registry(1)` (rf=1, in-process broker) + `rest::router(AppState { store })`.
- All tests are `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]`.
- Must call `cancel.cancel(); broker.shutdown().await;` at end of each test.
- Existing helpers: `register(app, subject, body)`, `get_json(app, uri)`, `get_status_json(app, uri)`, `put_json(app, uri, body)`, `post_raw(app, uri, body)`, `body_json(resp)`.
- `AVRO_BODY` constant (line 227): `r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"}"#`

### Add `delete_req` helper

Add after the existing helpers section (after `post_raw`, around line 225):

```rust
/// DELETE `uri`, return (status, parsed body).
async fn delete_req(
    app: &axum::Router,
    uri: &str,
) -> (axum::http::StatusCode, serde_json::Value) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let json = body_json(resp).await;
    (status, json)
}
```

### Five integration tests

- [ ] **Step 1: Write all five tests (append after existing test functions)**

```rust
// ── DELETE /config/{subject} ─────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_subject_compat_reverts_to_global() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // Set a per-subject override to FULL
    let (status, body) = put_json(&app, "/config/test-subject", r#"{"compatibility":"FULL"}"#).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["compatibility"], "FULL");

    // GET confirms the override is set
    let (status, body) = get_status_json(&app, "/config/test-subject").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["compatibilityLevel"], "FULL");

    // DELETE returns the deleted level
    let (status, body) = delete_req(&app, "/config/test-subject").await;
    assert_eq!(status, StatusCode::OK, "DELETE /config/{{subject}} should be 200");
    assert_eq!(body["compatibility"], "FULL", "response should echo the deleted level");

    // GET now returns 404 (no per-subject override remains)
    let (status, body) = get_status_json(&app, "/config/test-subject").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error_code"], 40401);

    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_subject_compat_no_override_returns_404() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // No per-subject override was ever set
    let (status, body) = delete_req(&app, "/config/no-override-subject").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error_code"], 40401);

    cancel.cancel();
    broker.shutdown().await;
}

// ── GET /schemas/ids/{id}/schema ─────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_by_id_schema_returns_raw_string() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // Register a schema
    let reg = register(&app, "raw-schema-test", AVRO_BODY).await;
    let id = reg["id"].as_i64().unwrap();

    // GET /schemas/ids/{id}/schema → raw schema string (not JSON-wrapped)
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&format!("/schemas/ids/{id}/schema"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();

    // Body should contain the schema content
    assert!(text.contains("record"), "raw schema body should contain the word 'record'");

    // Body should be a JSON value representing the schema itself, NOT {"schema":"..."}
    let v: serde_json::Value = serde_json::from_str(text).expect("body should be valid JSON");
    assert!(
        v.get("schema").is_none(),
        "body should be the raw schema, not a JSON envelope with a 'schema' key"
    );

    // Non-existent id → 404 / 40403
    let (status, body) = get_status_json(&app, "/schemas/ids/9999/schema").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error_code"], 40403);

    cancel.cancel();
    broker.shutdown().await;
}

// ── GET /schemas/ids/{id}/subjects ───────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_by_id_subjects_returns_all_subjects() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // Register the SAME schema under two subjects → they share one schema ID
    let reg1 = register(&app, "subject-alpha", AVRO_BODY).await;
    let reg2 = register(&app, "subject-beta", AVRO_BODY).await;
    assert_eq!(reg1["id"], reg2["id"], "same schema must share one id");
    let shared_id = reg1["id"].as_i64().unwrap();

    // GET /schemas/ids/{id}/subjects → both subjects present
    let body = get_json(&app, &format!("/schemas/ids/{shared_id}/subjects")).await;
    let subjects: Vec<String> = serde_json::from_value(body).expect("response should be a JSON array");
    assert!(
        subjects.contains(&"subject-alpha".to_string()),
        "subject-alpha should be in the list"
    );
    assert!(
        subjects.contains(&"subject-beta".to_string()),
        "subject-beta should be in the list"
    );

    // Non-existent id → 404 / 40403
    let (status, body) = get_status_json(&app, "/schemas/ids/9999/subjects").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error_code"], 40403);

    cancel.cancel();
    broker.shutdown().await;
}

// ── ?normalize=true ──────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn normalize_true_deduplicates_avro_schemas() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // Avro schema with extra whitespace — semantically identical to AVRO_BODY
    let avro_with_spaces = r#"{"schema":"{ \"type\" : \"record\" , \"name\" : \"U\" , \"fields\" : [ { \"name\" : \"id\" , \"type\" : \"int\" } ] }"}"#;

    // Register with normalize=true → normalizes to PCF, stored as canonical form
    let resp_a = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/subjects/norm-test/versions?normalize=true")
                .header("content-type", "application/vnd.schemaregistry.v1+json")
                .body(Body::from(avro_with_spaces))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp_a.status(), StatusCode::OK, "first normalize=true register should succeed");
    let reg_a = body_json(resp_a).await;
    let id_a = reg_a["id"].as_i64().unwrap();

    // Register same schema (also with spaces) again with normalize=true
    // → normalizes to same PCF → dedup → same ID
    let resp_b = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/subjects/norm-test/versions?normalize=true")
                .header("content-type", "application/vnd.schemaregistry.v1+json")
                .body(Body::from(avro_with_spaces))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp_b.status(), StatusCode::OK);
    let reg_b = body_json(resp_b).await;
    assert_eq!(
        reg_b["id"].as_i64().unwrap(),
        id_a,
        "second normalize=true registration of same schema must be idempotent (same id)"
    );

    // normalize=false on an invalid Avro → 422 / 42201
    let resp_bad = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/subjects/norm-test/versions?normalize=true")
                .header("content-type", "application/vnd.schemaregistry.v1+json")
                .body(Body::from(r#"{"schema":"{ not avro at all"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp_bad.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let err_body = body_json(resp_bad).await;
    assert_eq!(err_body["error_code"], 42201);

    cancel.cancel();
    broker.shutdown().await;
}
```

- [ ] **Step 2: Run integration tests to confirm all five pass**

```bash
cargo test -p crabka-schema-registry --test integration 2>&1 | tail -20
```

Expected: all five new tests pass, all prior integration tests still green.

If a test fails, likely causes:
- `delete_subject_compat_reverts_to_global` fails with `DELETE → 405 Method Not Allowed` → route not wired in `mod.rs`
- `get_by_id_schema_returns_raw_string` fails with 404 → `/schemas/ids/{id}/schema` route missing
- `normalize_true_deduplicates_avro_schemas` fails with 200 but wrong ID → `?normalize=true` query param not parsed (check `NormalizeQuery` has `#[serde(default)]`)

- [ ] **Step 3: Full workspace clippy**

```bash
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -20
```

Expected: clean. Watch for `dead_code` on any private helpers added.

- [ ] **Step 4: Full workspace fmt check**

```bash
cargo fmt --all --check 2>&1
```

Expected: no output (already formatted).

- [ ] **Step 5: Commit**

```bash
git -C /Users/mattstone/git/crabka add crates/schema-registry/tests/integration.rs
git -C /Users/mattstone/git/crabka \
  -c user.name="Matthew Stone" \
  -c user.email="matthew.d.stone@gmail.com" \
  commit -m "$(cat <<'EOF'
test(schema-registry): integration tests for slice 9 REST endpoints

Covers DELETE /config/{subject} (revert + 404 path), GET /schemas/ids/{id}/schema,
GET /schemas/ids/{id}/subjects, and ?normalize=true dedup + invalid-schema 422.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
EOF
)"
```

---

## Post-implementation Checklist

- [ ] `cargo test -p crabka-schema-registry --test integration` — all integration tests pass
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` — clean
- [ ] `cargo fmt --all --check` — clean
- [ ] Three commits on `main` (Task 1, Task 2, Task 3)
- [ ] No backwards-compat shims added (greenfield — not needed)
