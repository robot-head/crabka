# Schema Registry Slice 9 — REST Surface Completeness Design

## Goal

Close four Confluent Schema Registry REST API gaps that affect real client libraries and admin tooling:

1. `DELETE /config/{subject}` — revert per-subject compat override to global default
2. `GET /schemas/ids/{id}/schema` — return the raw schema string (not the full object)
3. `GET /schemas/ids/{id}/subjects` — list subjects that reference a given schema ID
4. `?normalize=true` on `POST /subjects/{subject}/versions` and `POST /subjects/{subject}` — pre-normalize schema before register/lookup

All four match Confluent SR's documented behavior exactly.

---

## Architecture

No new files. Five existing files are modified:

| File | Change |
|------|--------|
| `crates/schema-registry/src/kafkastore/record.rs` | +`config_key(subject) -> Vec<u8>` helper (mirrors existing `mode_key`) |
| `crates/schema-registry/src/kafkastore/mod.rs` | +`delete_subject_compat(subject) -> Result<Option<String>, SrError>` |
| `crates/schema-registry/src/rest/config.rs` | +`delete_subject` handler |
| `crates/schema-registry/src/rest/schemas.rs` | +`get_by_id_schema`, +`get_by_id_subjects` handlers |
| `crates/schema-registry/src/rest/subjects.rs` | +`NormalizeQuery`; add `?normalize` param to `register` and `lookup` |
| `crates/schema-registry/src/rest/mod.rs` | +3 route registrations |
| `crates/schema-registry/tests/integration.rs` | +5 integration tests |

---

## Endpoint Contracts

### `DELETE /config/{subject}`

Confluent docs: "Deletes the subject-level compatibility level config and reverts to the global default."

**Request:** `DELETE /config/{subject}` — no body.

**Responses:**
- `200 OK` — `{"compatibility": "BACKWARD"}` (the compat level that was deleted, uppercase string)
- `404` — `{"error_code": 40401, "message": "Subject '...' does not have subject-level compatibility configured"}` when no per-subject override exists
- `422` — forwarded if READONLY mode prevents writes

**Confluent alignment:** exact match. The 200 body uses the key `"compatibility"` (not `"compatibilityLevel"`); that matches cp-kafka's `/config/{subject}` GET response shape.

---

### `GET /schemas/ids/{id}/schema`

Confluent docs: "Get the schema string identified by the input id."

**Request:** `GET /schemas/ids/{id}/schema` — no body.

**Responses:**
- `200 OK` — body is the raw schema string (e.g., `{"type":"record","name":"Foo","fields":[]}` for Avro). Content-Type: `application/vnd.schemaregistry.v1+json`. Not wrapped in a JSON object.
- `404` — `{"error_code": 40403, "message": "Schema not found."}` when the ID does not exist.

**Implementation:** Call `store.read().schema_by_id(id, false)` (existing method), extract the schema `String` from the `(SchemaType, String, Vec<SchemaReference>)` tuple, write it as the response body.

---

### `GET /schemas/ids/{id}/subjects`

Confluent docs: "Get all the subjects associated with the input id."

**Request:** `GET /schemas/ids/{id}/subjects[?deleted=true]`

**Responses:**
- `200 OK` — `["subject1", "subject2"]` (JSON array, order unspecified)
- `404` — `{"error_code": 40403, "message": "Schema not found."}` when no subjects reference this ID (including when the ID itself doesn't exist)

**Query params:**
- `deleted` (bool, default `false`) — when `true`, include subjects that have only soft-deleted versions pointing to this ID. Uses the existing `include_deleted` bool already threaded through `schema_id_subject_versions`.

**Implementation:** Call `store.read().schema_id_subject_versions(id, include_deleted)` (existing method, returns `Vec<(String, i32)>`), deduplicate the subject strings, return as JSON array. If the resulting vec is empty, return 404/40403.

---

### `?normalize=true` on register and lookup

Confluent docs: "If true, the compatibility check is performed on a normalized schema string. The normalization is done by Avro/JSON/Protobuf specific rules."

**Endpoints affected:**
- `POST /subjects/{subject}/versions` (register)
- `POST /subjects/{subject}` (lookup)

**Query param struct** (in `rest/subjects.rs`, follows the existing `DeletedQ` pattern):
```rust
#[derive(Debug, Default, serde::Deserialize)]
pub struct NormalizeQuery {
    #[serde(default)]
    pub normalize: bool,
}
```

**Normalization rules** (applied before the schema string is passed to `store.register()` or `store.find_under_subject()`):

| Format | Normalization |
|--------|--------------|
| Avro | `apache_avro::Schema::parse_str(schema)?.canonical_form()` — strips doc strings, defaults, aliases; sorts field names; produces the Parsing Canonical Form (PCF) defined in the Avro spec. The `canonical_form()` method is already used internally in `store/mod.rs` for fingerprinting. |
| JSON Schema | `serde_json::from_str::<serde_json::Value>(schema)?.to_string()` — round-trip through `serde_json` strips whitespace and sorts object keys. |
| Protobuf | No-op — return the original string. The protobuf pipeline already uses a parsed descriptor for fingerprinting; textual normalization of the `.proto` source is not defined by Confluent. |

**Error handling:** If normalization fails (parse error), return `422 Unprocessable Entity` with error code `42201` ("Invalid schema") — same code as an invalid schema on normal register.

**Without `?normalize`:** current behavior is preserved exactly (original string stored).

---

## `delete_subject_compat` Implementation

### `kafkastore/record.rs` — add `config_key`

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

The existing `ConfigKey` struct is already defined in this file.

### `kafkastore/mod.rs` — add `delete_subject_compat`

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

Pattern is identical to `clear_subject_mode` (line 374) — lock write gate, check current state, produce tombstone, wait for applied offset.

---

## Handler Shapes

### `rest/config.rs` — `delete_subject`

```rust
pub async fn delete_subject(
    State(st): State<AppState>,
    Path(subject): Path<String>,
) -> Result<Response, SrError> {
    match st.store.delete_subject_compat(&subject).await? {
        Some(level) => Ok(Json(json!({"compatibility": level})).into_response()),
        None => Err(SrError::SubjectNotFound(subject)),
    }
}
```

Route: `.route("/config/:subject", delete(config::delete_subject))` added alongside existing `.route("/config/:subject", get(config::get_subject).put(config::put_subject))`.

### `rest/schemas.rs` — `get_by_id_schema`

```rust
pub async fn get_by_id_schema(
    State(st): State<AppState>,
    Path(id): Path<i32>,
) -> Result<Response, SrError> {
    let (_, schema, _) = st
        .store
        .read()
        .schema_by_id(id, false)
        .ok_or(SrError::SchemaNotFound(id))?;
    Ok(schema.into_response())
}
```

### `rest/schemas.rs` — `get_by_id_subjects`

```rust
pub async fn get_by_id_subjects(
    State(st): State<AppState>,
    Path(id): Path<i32>,
    Query(q): Query<DeletedQ>,
) -> Result<Response, SrError> {
    let pairs = st.store.read().schema_id_subject_versions(id, q.deleted);
    if pairs.is_empty() {
        return Err(SrError::SchemaNotFound(id));
    }
    let subjects: Vec<String> = pairs
        .into_iter()
        .map(|(s, _)| s)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    Ok(Json(subjects).into_response())
}
```

`DeletedQ` is already defined in `rest/mod.rs` — re-used here.

### `rest/subjects.rs` — normalize changes

Add `NormalizeQuery` struct. Add `normalize_schema(ty, schema) -> Result<String, SrError>` free function (shown in design). Thread the query param through `register` and `lookup`:

```rust
pub async fn register(
    State(st): State<AppState>,
    Path(subject): Path<String>,
    Query(q): Query<NormalizeQuery>,
    body: String,
) -> Result<Response, SrError> {
    let mut req: RegisterBody = serde_json::from_str(&body)?;
    let ty = SchemaType::from_wire(req.schema_type.as_deref());
    if q.normalize {
        req.schema = normalize_schema(ty, &req.schema)?;
    }
    // ... rest of existing register logic unchanged
}
```

---

## Tests

In `crates/schema-registry/tests/integration.rs`, add five tests that boot an in-process broker + SR node (same pattern as existing tests in that file):

| Test name | Scenario |
|-----------|----------|
| `delete_subject_compat_reverts_to_global` | PUT subject compat to FULL → GET confirms FULL → DELETE → GET returns global (BACKWARD) |
| `delete_subject_compat_no_override_returns_40401` | DELETE on subject with no per-subject compat → 404 with error_code 40401 |
| `get_by_id_schema_returns_raw_string` | Register Avro schema → `GET /schemas/ids/{id}/schema` → body equals original schema string |
| `get_by_id_subjects_returns_all_subjects` | Register same Avro schema under two subjects → `GET /schemas/ids/{id}/subjects` → both subjects in response |
| `normalize_true_deduplicates_avro_with_whitespace` | Register `{ "type" : "record" , ... }` (extra spaces) with `?normalize=true` → same ID as registering canonical form without normalize |

---

## Error Codes

| Situation | HTTP | `error_code` |
|-----------|------|-------------|
| `DELETE /config/{subject}` — no per-subject config | 404 | 40401 |
| `GET /schemas/ids/{id}/schema` — id not found | 404 | 40403 |
| `GET /schemas/ids/{id}/subjects` — no subjects for id | 404 | 40403 |
| `?normalize=true` — schema fails to parse | 422 | 42201 |

These match Confluent's documented error codes for equivalent scenarios.
