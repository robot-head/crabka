# Crabka Schema Registry — Slice 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a standalone `crabka-schema-registry` binary — a Confluent Schema Registry-compatible REST service that is a Kafka *client* of Crabka — implementing the end-to-end happy path (register / get-by-id / get-by-subject-version / list) for Avro, Protobuf, and JSON Schema, with compatibility fixed at `NONE`, single-node always-primary, and `/config` stored-but-not-enforced.

**Architecture:** A new `crates/schema-registry` crate. State lives in the `_schemas` compacted topic (Confluent's source-of-truth model); the broker is untouched. A group-less `StoreReader` (built on `crabka-client-core`'s `Connection` + `fetch_partition`) replays/tails `_schemas` into an in-memory store; a primary-only `SchemaWriter` (on `crabka-client-producer`) writes records and the write path blocks for read-your-writes via produced-offset gating. An axum REST layer serves the Confluent API with exact content-types and numeric error codes.

**Tech Stack:** Rust 2024, tokio, axum 0.8 (manual JSON for vendor content-type exactness), `crabka-client-{core,admin,producer}`, `apache-avro`, `protox` + `prost-reflect`, `serde_json`, `parking_lot`. Tests: in-process broker harness (`crabka-broker` `test-helpers`), `tower::ServiceExt::oneshot`, golden fixtures captured from a real `confluentinc/cp-schema-registry` image, `testcontainers`.

---

## Design reference

Spec: `docs/superpowers/specs/2026-06-04-crabka-schema-registry-design.md`. Read it first.

### Confluent-exactness is fixture-driven

The byte/shape-exact surfaces — the `_schemas` record key/value JSON and the REST response/error JSON — are **pinned against a real `cp-schema-registry` image**, not invented. Task 2 captures golden fixtures once; later tasks assert equality against the committed fixtures. Where a code step says "match `fixtures/<name>.json`", that fixture is the authoritative spec for byte ordering and escaping. Do **not** hand-guess field order.

### Verified upstream API signatures (do not re-derive)

```rust
// crabka-client-admin
AdminClient::connect(bootstrap_addrs: &[String]) -> Result<AdminClient, AdminError>
AdminClient::create_topics(&mut self, specs: &[CreateTopicSpec], timeout_ms: i32)
    -> Result<Vec<CreateTopicOutcome>, AdminError>
AdminClient::metadata(&mut self, topics: &[&str]) -> Result<TopicMetadata, AdminError>
struct CreateTopicSpec { name: String, partitions: i32, replicas: i32, configs: BTreeMap<String,String> }
struct CreateTopicOutcome { name: String, topic_id: Option<uuid::Uuid>, error: Option<KafkaError> }
struct TopicMetadataEntry { name: String, topic_id: Option<uuid::Uuid>, partition_count: i32, replication_factor: i32, error: Option<KafkaError> }
// KafkaError { code: i16, name: &'static str, message: Option<String> }; topic-exists code = 36 (TOPIC_ALREADY_EXISTS)

// crabka-client-producer
Producer::builder().bootstrap(String).client_id(String).enable_idempotence(bool).acks(Acks).build().await -> Result<Producer, ProducerError>
Producer::send(&self, record: ProducerRecord) -> oneshot::Receiver<Result<RecordMetadata, ProducerError>>  // NOTE: async fn; await the fn, then await the receiver
Producer::flush(&self) -> Result<(), ProducerError>
struct ProducerRecord { topic: String, partition: Option<i32>, key: Option<Bytes>, value: Option<Bytes>, headers: Vec<Header>, timestamp_ms: Option<i64> }  // derives Default
struct RecordMetadata { topic_index: usize, partition: i32, offset: i64, timestamp_ms: i64 }
enum Acks { Zero, One, All }

// crabka-client-core
Connection::connect_with_options(addr: SocketAddr, options: ConnectionOptions) -> Result<Connection, ClientError>
Connection::close(self)
fetch_partition(conn: &Connection, topic: &str, topic_id: WireUuid, partition: i32, fetch_offset: i64, max_wait_ms: i32, partition_max_bytes: i32) -> Result<Vec<FetchedRecord>, ClientError>
struct FetchedRecord { offset: i64, key: Option<Bytes>, value: Option<Bytes> }  // no headers on fetch path
struct ConnectionOptions { client_id: String, connect_timeout: Duration, request_timeout: Duration, security: Option<Box<ClientSecurity>> }  // derives Default
// WireUuid = crabka_protocol::primitives::uuid::Uuid; has WireUuid::ZERO. AdminClient hands back uuid::Uuid; convert with the same helper remote-storage-topic uses (to_wire_uuid) — see Task 9.

// In-process test broker (cross-crate)
// #[path = "../../broker/tests/support/mod.rs"] mod support;
// support::start().await -> support::InProcess { broker: BrokerHandle, client: Client, _tempdir: TempDir }
// broker.listen_addr().to_string() is the bootstrap addr; broker.shutdown().await to tear down.
```

### Commit & worktree discipline (executors read this)

- Git identity is unset locally. **Every** commit uses overrides: `git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit ...`. Never run `git config`.
- This work happens in a worktree. Subagent shells reset cwd to the main repo — always `git -C <worktree-root>` and assert the branch is **not** `main` before committing.
- End every commit message body with: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Before any push: `cargo fmt --all` then `cargo clippy --workspace --all-targets -- -D warnings` (CI gates on both; clippy `--all-targets` catches `#[cfg(test)]` lints).

---

## File structure

```
crates/schema-registry/
  Cargo.toml
  src/
    lib.rs                      # crate docs + `pub mod` for every module (declared once, in Task 1)
    config.rs                   # RegistryConfig
    error.rs                    # SrError: Confluent error_code + HTTP status + IntoResponse
    bin/schema-registry.rs      # clap Args + #[tokio::main]; wires KafkaStore + axum serve
    format/
      mod.rs                    # SchemaType, ParsedSchema, parse() + canonical_form() dispatch
      avro.rs                   # apache-avro parse + parsing-canonical-form dedup key
      protobuf.rs               # protox parse + descriptor-bytes dedup key
      json.rs                   # serde_json parse + key-sorted canonical dedup key
    kafkastore/
      mod.rs                    # KafkaStore facade: ensure topic, spawn reader, write+read-your-writes
      record.rs                 # _schemas key/value serde structs <-> exact JSON (fixture-pinned)
      topic.rs                  # ensure_schemas_topic -> (WireUuid) via AdminClient
      writer.rs                 # SchemaWriter: Producer wrapper -> produced offset
      reader.rs                 # StoreReader: Connection + fetch_partition loop -> store.apply()
    store/
      mod.rs                    # StoreState (subjects/ids/config) + apply() + queries
    rest/
      mod.rs                    # AppState + Router (merges the route groups)
      response.rs               # ok_json() helper (vendor content-type) + SrError wiring
      schemas.rs                # GET /schemas/ids/{id}, GET /schemas/types
      subjects.rs               # POST /subjects/{s}/versions, POST /subjects/{s}, GET subjects + versions
      config.rs                 # GET/PUT /config, GET/PUT /config/{subject}
      compatibility.rs          # POST /compatibility/subjects/{s}/versions/{v}
  tests/
    fixtures/                   # golden JSON captured from real cp-schema-registry (Task 2)
    schemas_record.rs           # _schemas (de)serialize == fixtures (no Docker)
    rest_conformance.rs         # REST handlers via tower::oneshot == fixtures (no Docker)
    integration.rs              # end-to-end vs in-process Crabka broker (no Docker, multi_thread)
    interop.rs                  # #[ignore] real cp-schema-registry _schemas interop (Docker)
```

Root `Cargo.toml` (`members = ["crates/*"]`) auto-includes the crate; the only root edit is adding three `[workspace.dependencies]` entries (Task 1).

---

## Execution batches

Per CLAUDE.md, dispatch non-overlapping tasks in a batch concurrently, wait, review, then proceed. File sets below do not overlap within a batch.

- **Batch A (sequential):** Task 1 (scaffold — everything depends on it), then Task 2 (golden fixtures — the oracle for byte-exact tasks).
- **Batch B (parallel):** Task 3 `error.rs` · Task 4 `kafkastore/record.rs` · Task 5 `format/{mod,avro}.rs`.
- **Batch C (parallel):** Task 6 `format/json.rs` · Task 7 `format/protobuf.rs` · Task 8 `store/mod.rs`.
- **Batch D (parallel):** Task 9 `kafkastore/topic.rs` · Task 10 `kafkastore/writer.rs` · Task 11 `kafkastore/reader.rs`. Then Task 12 `kafkastore/mod.rs` (facade, sequential — depends on 9–11).
- **Batch E:** Task 13 `rest/response.rs` (sequential — others depend on it), then parallel Task 14 `rest/schemas.rs` · Task 15 `rest/subjects.rs` · Task 16 `rest/config.rs` · Task 17 `rest/compatibility.rs`, then Task 18 `rest/mod.rs` (sequential).
- **Batch F (sequential):** Task 19 binary · Task 20 in-process integration test.
- **Batch G (parallel):** Task 21 Docker interop test · Task 22 CI job + codecov flag. Then Task 23 final fmt/clippy/commit (sequential).

---

## Task 1: Crate scaffold (all modules stubbed & compiling)

**Files:**
- Modify: `Cargo.toml` (root — add three workspace deps)
- Create: `crates/schema-registry/Cargo.toml`
- Create: `crates/schema-registry/src/lib.rs`
- Create stubs: `src/config.rs`, `src/error.rs`, `src/format/{mod,avro,protobuf,json}.rs`, `src/kafkastore/{mod,record,topic,writer,reader}.rs`, `src/store/mod.rs`, `src/rest/{mod,response,schemas,subjects,config,compatibility}.rs`, `src/bin/schema-registry.rs`

- [ ] **Step 1: Add workspace dependencies** to root `Cargo.toml` under `[workspace.dependencies]` (place near the other parsing deps; keep alphabetical-ish):

```toml
# Schema Registry (crabka-schema-registry) — schema parsing + canonical form.
apache-avro = "0.20"
protox = "0.9"
prost-reflect = "0.16"
```

- [ ] **Step 2: Create `crates/schema-registry/Cargo.toml`** (mirrors `crates/rebalancer/Cargo.toml`):

```toml
[package]
name = "crabka-schema-registry"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version = "1.95.0"
description = "Confluent Schema Registry-compatible REST service for Crabka (binary: crabka-schema-registry)"

[lints]
workspace = true

[[bin]]
name = "crabka-schema-registry"
path = "src/bin/schema-registry.rs"

[dependencies]
crabka-client-core = { version = "0.2", path = "../client-core" }
crabka-client-admin = { version = "0.2", path = "../client-admin" }
crabka-client-producer = { version = "0.2", path = "../client-producer" }
crabka-protocol = { version = "0.2", path = "../protocol", default-features = false }
axum = { workspace = true, features = ["query"] }
tokio = { workspace = true, features = ["rt", "rt-multi-thread", "net", "macros", "signal", "time", "sync"] }
tokio-util = { workspace = true, features = ["rt"] }
bytes.workspace = true
serde = { workspace = true, features = ["derive"] }
serde_json.workspace = true
thiserror.workspace = true
anyhow.workspace = true
tracing.workspace = true
tracing-subscriber.workspace = true
clap = { workspace = true, features = ["env", "derive"] }
uuid = { workspace = true }
parking_lot = { workspace = true }
apache-avro = { workspace = true }
protox = { workspace = true }
prost-reflect = { workspace = true }

[dev-dependencies]
assert2 = { workspace = true }
crabka-broker = { version = "0.2", path = "../broker", features = ["test-helpers"] }
crabka-client-consumer = { version = "0.2", path = "../client-consumer" }
tempfile.workspace = true
tower.workspace = true
reqwest = { version = "0.13", default-features = false, features = ["json", "rustls"] }
testcontainers.workspace = true
testcontainers-modules = { workspace = true }

[[test]]
name = "integration"

[[test]]
name = "interop"
```

> If `parking_lot` / `anyhow` are not yet in `[workspace.dependencies]`, check the root `Cargo.toml` — both are used by the rebalancer, so they are present. If `testcontainers-modules` needs the `kafka` feature here, it is already declared workspace-wide with `features = ["kafka"]`.

- [ ] **Step 3: Create `src/lib.rs`** declaring the full module tree (so no later task edits this file):

```rust
//! Confluent Schema Registry-compatible REST service for Crabka.
//!
//! Standalone binary; a Kafka *client* of a Crabka broker. State lives in the
//! `_schemas` compacted topic. See
//! `docs/superpowers/specs/2026-06-04-crabka-schema-registry-design.md`.

pub mod config;
pub mod error;
pub mod format;
pub mod kafkastore;
pub mod rest;
pub mod store;
```

- [ ] **Step 4: Create compiling stubs** for every module. Each stub must compile with `cargo build -p crabka-schema-registry`. Minimum viable stubs:

`src/config.rs`:
```rust
//! Runtime configuration for the registry service.

/// Resolved configuration for a running registry node.
#[derive(Debug, Clone)]
pub struct RegistryConfig {
    /// `host:port[,host:port...]` bootstrap addresses for the Crabka broker.
    pub bootstrap: String,
    /// Name of the backing compacted topic. Confluent default: `_schemas`.
    pub schemas_topic: String,
    /// Replication factor for `_schemas` when auto-created.
    pub schemas_topic_rf: i32,
    /// Client id used for the producer/reader connections.
    pub client_id: String,
}
```

`src/error.rs`, `src/format/mod.rs`, `src/format/avro.rs`, `src/format/protobuf.rs`, `src/format/json.rs`, `src/kafkastore/mod.rs`, `src/kafkastore/record.rs`, `src/kafkastore/topic.rs`, `src/kafkastore/writer.rs`, `src/kafkastore/reader.rs`, `src/store/mod.rs`, `src/rest/mod.rs`, `src/rest/response.rs`, `src/rest/schemas.rs`, `src/rest/subjects.rs`, `src/rest/config.rs`, `src/rest/compatibility.rs` each start as:
```rust
//! Stub — filled in by a later task.
```

`src/bin/schema-registry.rs`:
```rust
fn main() {}
```

- [ ] **Step 5: Build** to verify the skeleton compiles.

Run: `cargo build -p crabka-schema-registry`
Expected: compiles (warnings about unused crates are fine).

- [ ] **Step 6: Commit**

```bash
WT=/Users/mattstone/git/crabka/.claude/worktrees/musing-cartwright-7af144
git -C "$WT" rev-parse --abbrev-ref HEAD   # assert NOT main
git -C "$WT" add Cargo.toml Cargo.lock crates/schema-registry
git -C "$WT" -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "schema-registry: crate scaffold (slice 1)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Capture golden fixtures from a real cp-schema-registry

This produces the **oracle** for every byte-exact assertion. It runs the real registry once against a Crabka broker, captures the exact `_schemas` records and REST bodies, and commits them as fixtures. After this task, byte-exact tasks need no Docker.

**Files:**
- Create: `crates/schema-registry/tests/fixtures/*.json` (+ a short `README.md` documenting provenance)
- Create: `crates/schema-registry/tests/capture_fixtures.rs` (the `#[ignore]` capture harness)

- [ ] **Step 1: Write the capture harness** `tests/capture_fixtures.rs`. It is `#[ignore]` (manual, Docker). It: (a) starts an in-process Crabka broker bound to `0.0.0.0:0`… **NO** — the real registry runs in Docker and must reach the broker via `host.docker.internal`. Mirror `crates/broker/tests/jvm_acceptance.rs`: bind the broker to `0.0.0.0:9092` advertising `host.docker.internal:9092`, run `confluentinc/cp-schema-registry:7.4.0` in Docker pointed at `host.docker.internal:9092`, drive its REST with `reqwest`, then read the raw `_schemas` topic back through a `crabka-client-core` `Connection` + `fetch_partition` and dump the exact key/value bytes.

```rust
#![cfg(not(target_os = "windows"))]
//! Manual, Docker-gated capture of Confluent Schema Registry golden fixtures.
//!
//! Run with:
//!   echo "127.0.0.1 host.docker.internal" | sudo tee -a /etc/hosts   # once
//!   cargo test -p crabka-schema-registry --test capture_fixtures -- --ignored --nocapture
//!
//! Captured artifacts are committed under tests/fixtures/ and become the
//! byte-exact oracle for schemas_record.rs and rest_conformance.rs.

use std::process::Command;

const SR_IMAGE: &str = "confluentinc/cp-schema-registry:7.4.0";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; captures golden fixtures"]
async fn capture_confluent_fixtures() {
    // 1. Start a Crabka broker on 0.0.0.0:9092 advertising host.docker.internal:9092.
    //    (Reuse the broker BrokerConfig pattern from jvm_acceptance.rs lines ~34-60.)
    // 2. docker run -d --add-host=host.docker.internal:host-gateway \
    //      -e SCHEMA_REGISTRY_HOST_NAME=localhost \
    //      -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://host.docker.internal:9092 \
    //      -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 -p 0:8081 <SR_IMAGE>
    //    Capture the mapped host port for 8081.
    // 3. Wait for GET /subjects to return 200.
    // 4. For each (subject, schemaType, schema) below, POST /subjects/{s}/versions and
    //    record the response body; then GET /subjects/{s}/versions/1, GET /schemas/ids/{id},
    //    GET /subjects, GET /config; and provoke errors: GET /subjects/missing/versions/1
    //    (40401), POST an invalid schema (42201). Write each response verbatim to
    //    tests/fixtures/rest_<name>.json.
    // 5. Read the _schemas topic raw via Connection + fetch_partition from offset 0;
    //    for each record write {"key": <utf8>, "value": <utf8>} to
    //    tests/fixtures/schemas_record_<n>.json (verbatim bytes, no re-encoding).
    // 6. docker rm -f the container; broker.shutdown().await.
    let _ = Command::new("docker").arg("--version").output();
    // Full body implemented here; this comment block is the spec for it.
}
```

Schemas to register (used by every downstream test — keep identical):
```
subject "av-value"  AVRO     {"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}
subject "pb-value"  PROTOBUF syntax = "proto3"; message User { int32 id = 1; }
subject "js-value"  JSON     {"type":"object","properties":{"id":{"type":"integer"}}}
```

- [ ] **Step 2: Run the capture** (manual) and verify fixtures are written.

Run: `cargo test -p crabka-schema-registry --test capture_fixtures -- --ignored --nocapture`
Expected: `tests/fixtures/` now contains `rest_register_avro.json`, `rest_get_version_avro.json`, `rest_get_by_id_avro.json` (+ protobuf/json variants), `rest_list_subjects.json`, `rest_get_config.json`, `rest_err_subject_not_found.json`, `rest_err_invalid_schema.json`, and `schemas_record_0.json …`.

- [ ] **Step 3: Inspect & sanity-check** the captured `_schemas` keys, confirming the documented shapes (e.g. SCHEMA key `{"keytype":"SCHEMA","subject":"av-value","version":1,"magic":1}`, and that the AVRO value omits `schemaType`). Note the exact field order — this is the spec for Task 4.

- [ ] **Step 4: Commit** the fixtures + harness.

```bash
WT=/Users/mattstone/git/crabka/.claude/worktrees/musing-cartwright-7af144
git -C "$WT" add crates/schema-registry/tests/fixtures crates/schema-registry/tests/capture_fixtures.rs
git -C "$WT" -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" \
  commit -m "schema-registry: golden fixtures from cp-schema-registry 7.4.0

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

> If Docker is unavailable in the execution environment, this task is deferred to a human/CI runner; downstream byte-exact tasks then assert against fixtures authored by hand from the spec and **must** be re-validated once real fixtures land. Flag this clearly in the task's completion note rather than silently hand-rolling.

---

## Task 3: `error.rs` — Confluent error model

**Files:**
- Modify: `crates/schema-registry/src/error.rs`
- Test: inline `#[cfg(test)]`

- [ ] **Step 1: Write the failing test.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;
    use axum::http::StatusCode;

    #[test]
    fn codes_map_to_status() {
        assert_eq!(SrError::SubjectNotFound("s".into()).http_status(), StatusCode::NOT_FOUND);
        assert_eq!(SrError::SubjectNotFound("s".into()).error_code(), 40401);
        assert_eq!(SrError::VersionNotFound.error_code(), 40402);
        assert_eq!(SrError::SchemaNotFound.error_code(), 40403);
        assert_eq!(SrError::InvalidSchema("bad".into()).error_code(), 42201);
        assert_eq!(SrError::InvalidSchema("bad".into()).http_status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(SrError::Backend("x".into()).error_code(), 50001);
    }

    #[tokio::test]
    async fn body_is_confluent_json() {
        let resp = SrError::SubjectNotFound("av-value".into()).into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error_code"], 40401);
        assert!(v["message"].as_str().unwrap().contains("av-value"));
    }
}
```

- [ ] **Step 2: Run to verify it fails.**

Run: `cargo test -p crabka-schema-registry error::`
Expected: FAIL — `SrError` not defined.

- [ ] **Step 3: Implement `error.rs`.**

```rust
//! Confluent-compatible error model: numeric `error_code` + HTTP status,
//! serialised as `{"error_code":N,"message":"..."}` with the vendor
//! content-type. Serdes branch on `error_code`, so the numbers are exact.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

pub const CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";

#[derive(Debug, thiserror::Error)]
pub enum SrError {
    #[error("Subject '{0}' not found.")]
    SubjectNotFound(String),
    #[error("Version not found.")]
    VersionNotFound,
    #[error("Schema not found")]
    SchemaNotFound,
    #[error("Invalid schema: {0}")]
    InvalidSchema(String),
    #[error("Invalid version: {0}")]
    InvalidVersion(String),
    #[error("Invalid compatibility level: {0}")]
    InvalidCompatibilityLevel(String),
    #[error("Error in the backend data store: {0}")]
    Backend(String),
}

impl SrError {
    #[must_use]
    pub fn error_code(&self) -> i32 {
        match self {
            Self::SubjectNotFound(_) => 40401,
            Self::VersionNotFound => 40402,
            Self::SchemaNotFound => 40403,
            Self::InvalidSchema(_) => 42201,
            Self::InvalidVersion(_) => 42202,
            Self::InvalidCompatibilityLevel(_) => 42203,
            Self::Backend(_) => 50001,
        }
    }

    #[must_use]
    pub fn http_status(&self) -> StatusCode {
        match self {
            Self::SubjectNotFound(_) | Self::VersionNotFound | Self::SchemaNotFound => {
                StatusCode::NOT_FOUND
            }
            Self::InvalidSchema(_) | Self::InvalidVersion(_) | Self::InvalidCompatibilityLevel(_) => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            Self::Backend(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for SrError {
    fn into_response(self) -> Response {
        let body = serde_json::json!({ "error_code": self.error_code(), "message": self.to_string() });
        (
            self.http_status(),
            [("content-type", CONTENT_TYPE)],
            body.to_string(),
        )
            .into_response()
    }
}
```

- [ ] **Step 4: Run to verify it passes.**

Run: `cargo test -p crabka-schema-registry error::`
Expected: PASS.

> After Task 2 lands real fixtures, add an assertion that `rest_err_subject_not_found.json` deserialises to `{"error_code":40401,...}` and that our body matches its shape. If the captured `message` text differs, prefer the captured text in the `#[error(...)]` strings.

- [ ] **Step 5: Commit** (`git -C "$WT" add crates/schema-registry/src/error.rs` + the identity-override commit).

---

## Task 4: `kafkastore/record.rs` — `_schemas` records (fixture-pinned)

**Files:**
- Modify: `crates/schema-registry/src/kafkastore/record.rs`
- Test: `crates/schema-registry/tests/schemas_record.rs`

- [ ] **Step 1: Write the failing test** asserting our serialisation byte-matches the captured fixtures and round-trips.

```rust
#![cfg(not(target_os = "windows"))]
use crabka_schema_registry::kafkastore::record::{SchemaKey, SchemaValue, SchemaRecord};

fn fixture(name: &str) -> serde_json::Value {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    serde_json::from_slice(&std::fs::read(path).expect("fixture present")).expect("valid json")
}

#[test]
fn avro_schema_value_omits_schema_type() {
    // The captured record 0 is the av-value SCHEMA record.
    let rec = fixture("schemas_record_0.json"); // {"key": "...", "value": "..."}
    let key: serde_json::Value = serde_json::from_str(rec["key"].as_str().unwrap()).unwrap();
    let val: serde_json::Value = serde_json::from_str(rec["value"].as_str().unwrap()).unwrap();
    assert_eq!(key["keytype"], "SCHEMA");
    assert_eq!(key["magic"], 1);
    assert!(val.get("schemaType").is_none(), "AVRO value must omit schemaType");

    // Our types must serialise to byte-identical key/value strings.
    let our_key = SchemaKey { subject: key["subject"].as_str().unwrap().into(), version: 1 };
    assert_eq!(serde_json::to_string(&our_key).unwrap(), rec["key"].as_str().unwrap());
}

#[test]
fn schema_value_round_trips() {
    let v = SchemaValue {
        subject: "av-value".into(), version: 1, id: 1,
        schema_type: None, references: vec![],
        schema: "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}".into(),
        deleted: false,
    };
    let s = serde_json::to_string(&v).unwrap();
    let back: SchemaValue = serde_json::from_str(&s).unwrap();
    assert_eq!(back, v);
}
```

- [ ] **Step 2: Run to verify it fails.**

Run: `cargo test -p crabka-schema-registry --test schemas_record`
Expected: FAIL — types not defined.

- [ ] **Step 3: Implement `record.rs`.** Field order and `magic` values come from the fixtures (Task 2). Use `#[serde(skip_serializing_if = "...")]` so AVRO omits `schemaType` and empty `references` are absent **only if the fixture shows them absent** — verify against `schemas_record_0.json` before finalising.

```rust
//! `_schemas` topic record types. Keys drive log compaction; values carry the
//! schema payload. Byte shapes are pinned against cp-schema-registry fixtures
//! (tests/fixtures/schemas_record_*.json). Do not reorder fields without a
//! fixture to justify it.

use serde::{Deserialize, Serialize};

/// `magic` byte on SCHEMA keys (Confluent uses 1).
const SCHEMA_KEY_MAGIC: u8 = 1;
/// `magic` byte on CONFIG/MODE/NOOP keys (Confluent uses 0).
const META_KEY_MAGIC: u8 = 0;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaKey {
    #[serde(rename = "keytype")]
    #[serde(default = "schema_keytype", skip_deserializing)]
    pub _keytype: KeyType, // serialises as "SCHEMA"; see KeyType
    pub subject: String,
    pub version: i32,
    #[serde(default = "schema_magic")]
    pub magic: u8,
}
// NOTE: the precise derive layout (keytype const, field order, magic) MUST be
// adjusted to byte-match schemas_record_0.json. The simplest robust approach is
// a hand-written Serialize that emits exactly: {"keytype":"SCHEMA","subject":..,
// "version":..,"magic":1}. Prefer that if the derive cannot reproduce order.

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaValue {
    pub subject: String,
    pub version: i32,
    pub id: i32,
    #[serde(rename = "schemaType", skip_serializing_if = "Option::is_none", default)]
    pub schema_type: Option<String>, // None for AVRO (omitted); "PROTOBUF"/"JSON" otherwise
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<SchemaReference>,
    pub schema: String,
    #[serde(default)]
    pub deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaReference {
    pub name: String,
    pub subject: String,
    pub version: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigKey {
    #[serde(rename = "keytype")] pub _keytype: KeyType,
    pub subject: Option<String>, // null = global
    #[serde(default = "meta_magic")] pub magic: u8,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigValue {
    #[serde(rename = "compatibilityLevel")] pub compatibility_level: String,
}

// ... KeyType enum (SCHEMA/CONFIG/MODE/NOOP) with serde rename_all = "SCHEMA"-style,
// ModeKey/ModeValue, NoopKey, plus the helper consts schema_keytype()/schema_magic()/meta_magic().

/// A typed `_schemas` record decoded from a (key, value) byte pair.
#[derive(Debug, Clone)]
pub enum SchemaRecord {
    Schema(SchemaKey, SchemaValue),
    Config(ConfigKey, ConfigValue),
    Mode, // slice 1 ignores mode bodies
    Noop,
    Unknown, // forward-compatible: tolerate key types we don't yet act on
}

impl SchemaRecord {
    /// Decode a raw `_schemas` record. `value == None` means a tombstone.
    /// Unknown key types decode to `Unknown` and are ignored (never panic) —
    /// the interop test replays a real registry's topic, which may carry
    /// CONFIG/MODE records.
    #[must_use]
    pub fn decode(key: &[u8], value: Option<&[u8]>) -> Self {
        // Parse key.keytype first, then dispatch. On any parse error -> Unknown.
        // Full body written here.
        let _ = (key, value);
        Self::Unknown
    }
}
```

> The derive sketch above is a starting point. The acceptance bar is: `serde_json::to_string(&SchemaKey{..})` and `&SchemaValue{..}` produce strings byte-identical to the fixtures. If `#[derive(Serialize)]` cannot reproduce Confluent's field order, replace it with a hand-written `Serialize` impl that writes fields in the fixture's exact order. The test in Step 1 is the gate.

- [ ] **Step 4: Run to verify it passes.**

Run: `cargo test -p crabka-schema-registry --test schemas_record`
Expected: PASS (against real fixtures). If fixtures are hand-authored (Docker deferred), mark the task note accordingly.

- [ ] **Step 5: Commit.**

---

## Task 5: `format/mod.rs` + `format/avro.rs`

**Files:**
- Modify: `crates/schema-registry/src/format/mod.rs`, `crates/schema-registry/src/format/avro.rs`
- Test: inline in both

- [ ] **Step 1: Write the failing test** (in `format/mod.rs`):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_type_wire_names() {
        assert_eq!(SchemaType::Avro.wire_name(), None);          // AVRO omitted on the wire
        assert_eq!(SchemaType::Protobuf.wire_name(), Some("PROTOBUF"));
        assert_eq!(SchemaType::Json.wire_name(), Some("JSON"));
        assert_eq!(SchemaType::from_wire(None), SchemaType::Avro);
        assert_eq!(SchemaType::from_wire(Some("PROTOBUF")), SchemaType::Protobuf);
    }

    #[test]
    fn avro_parses_and_dedups_by_canonical_form() {
        let a = parse(SchemaType::Avro, r#"{"type":"record","name":"U","fields":[{"name":"id","type":"int"}]}"#).unwrap();
        // Same schema, whitespace differs -> identical canonical form (dedup key).
        let b = parse(SchemaType::Avro, "{ \"type\":\"record\", \"name\":\"U\", \"fields\":[ {\"name\":\"id\",\"type\":\"int\"} ] }").unwrap();
        assert_eq!(a.canonical_form(), b.canonical_form());
        let c = parse(SchemaType::Avro, r#"{"type":"record","name":"V","fields":[]}"#).unwrap();
        assert_ne!(a.canonical_form(), c.canonical_form());
    }

    #[test]
    fn avro_rejects_invalid() {
        assert!(parse(SchemaType::Avro, "{not avro}").is_err());
    }
}
```

- [ ] **Step 2: Run to verify it fails.** Run: `cargo test -p crabka-schema-registry format::`. Expected: FAIL.

- [ ] **Step 3: Implement `format/mod.rs`:**

```rust
//! Schema formats: parse + canonical form (the dedup key). Slice 1 does no
//! compatibility checking (that is slice 2); canonical form is needed now for
//! global-id deduplication.

pub mod avro;
pub mod json;
pub mod protobuf;

use crate::error::SrError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaType { Avro, Protobuf, Json }

impl SchemaType {
    /// Wire token for the `schemaType` field. `None` for AVRO (Confluent omits it).
    #[must_use]
    pub fn wire_name(self) -> Option<&'static str> {
        match self { Self::Avro => None, Self::Protobuf => Some("PROTOBUF"), Self::Json => Some("JSON") }
    }
    #[must_use]
    pub fn from_wire(s: Option<&str>) -> Self {
        match s {
            None | Some("") | Some("AVRO") => Self::Avro,
            Some("PROTOBUF") => Self::Protobuf,
            _ => Self::Json,
        }
    }
}

/// A successfully-parsed schema. `canonical_form()` is a stable string used as
/// the global-id dedup key; identical schemas (modulo formatting) collide.
pub trait ParsedSchema {
    fn canonical_form(&self) -> String;
}

/// Parse `schema` as `ty`, returning a boxed parsed form or `SrError::InvalidSchema`.
pub fn parse(ty: SchemaType, schema: &str) -> Result<Box<dyn ParsedSchema>, SrError> {
    match ty {
        SchemaType::Avro => avro::parse(schema).map(|p| Box::new(p) as Box<dyn ParsedSchema>),
        SchemaType::Json => json::parse(schema).map(|p| Box::new(p) as Box<dyn ParsedSchema>),
        SchemaType::Protobuf => protobuf::parse(schema).map(|p| Box::new(p) as Box<dyn ParsedSchema>),
    }
}
```

- [ ] **Step 4: Implement `format/avro.rs`:**

```rust
//! Avro: parse + Parsing Canonical Form via `apache-avro`.

use crate::error::SrError;
use super::ParsedSchema;

pub struct AvroSchema(apache_avro::Schema);

/// Parse an Avro schema (JSON form). Maps parse failure to `InvalidSchema`.
pub fn parse(schema: &str) -> Result<AvroSchema, SrError> {
    apache_avro::Schema::parse_str(schema)
        .map(AvroSchema)
        .map_err(|e| SrError::InvalidSchema(format!("Avro: {e}")))
}

impl ParsedSchema for AvroSchema {
    fn canonical_form(&self) -> String {
        self.0.canonical_form()
    }
}
```

- [ ] **Step 5: Run to verify it passes.** Run: `cargo test -p crabka-schema-registry format::`. Expected: PASS.

- [ ] **Step 6: Commit.**

---

## Task 6: `format/json.rs`

**Files:** Modify `crates/schema-registry/src/format/json.rs`; test inline.

- [ ] **Step 1: Write the failing test.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::ParsedSchema;

    #[test]
    fn parses_object_and_dedups_key_order() {
        let a = parse(r#"{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"string"}}}"#).unwrap();
        let b = parse(r#"{"properties":{"b":{"type":"string"},"a":{"type":"integer"}},"type":"object"}"#).unwrap();
        assert_eq!(a.canonical_form(), b.canonical_form(), "key order must not affect dedup");
    }

    #[test]
    fn rejects_non_json() {
        assert!(parse("not json").is_err());
    }
}
```

- [ ] **Step 2: Run / fail.** `cargo test -p crabka-schema-registry format::json`.

- [ ] **Step 3: Implement.**

```rust
//! JSON Schema: parse as JSON + well-formedness; canonical form = recursively
//! key-sorted compact JSON (the dedup key). Compatibility is slice 2.

use crate::error::SrError;
use super::ParsedSchema;

pub struct JsonSchema(serde_json::Value);

pub fn parse(schema: &str) -> Result<JsonSchema, SrError> {
    let v: serde_json::Value =
        serde_json::from_str(schema).map_err(|e| SrError::InvalidSchema(format!("JSON Schema: {e}")))?;
    if !v.is_object() && !v.is_boolean() {
        return Err(SrError::InvalidSchema("JSON Schema must be an object or boolean".into()));
    }
    Ok(JsonSchema(v))
}

impl ParsedSchema for JsonSchema {
    fn canonical_form(&self) -> String {
        canonicalize(&self.0)
    }
}

/// Recursively serialise with object keys sorted, arrays preserved.
fn canonicalize(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let inner: Vec<String> =
                keys.iter().map(|k| format!("{}:{}", serde_json::to_string(k).unwrap(), canonicalize(&map[*k]))).collect();
            format!("{{{}}}", inner.join(","))
        }
        serde_json::Value::Array(a) => {
            let inner: Vec<String> = a.iter().map(canonicalize).collect();
            format!("[{}]", inner.join(","))
        }
        other => serde_json::to_string(other).unwrap(),
    }
}
```

- [ ] **Step 4: Run / pass.** `cargo test -p crabka-schema-registry format::json`.
- [ ] **Step 5: Commit.**

---

## Task 7: `format/protobuf.rs` (highest-risk)

**Files:** Modify `crates/schema-registry/src/format/protobuf.rs`; test inline.

> Risk note (from the spec): Confluent's `.proto` normalisation is bespoke. For **slice 1 internal dedup only**, hashing the parsed descriptor's encoded bytes is sufficient and deterministic. Matching Confluent's exact canonical string (for cross-registry dedup parity) is a slice-2+ concern; do not attempt it here. Log a `tracing::debug!` noting the dedup key is descriptor-bytes, not Confluent canonical form.

- [ ] **Step 1: Write the failing test.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::ParsedSchema;

    const P: &str = "syntax = \"proto3\"; message User { int32 id = 1; }";

    #[test]
    fn parses_and_is_stable() {
        let a = parse(P).unwrap();
        let b = parse("syntax = \"proto3\";\nmessage User {\n  int32 id = 1;\n}\n").unwrap();
        assert_eq!(a.canonical_form(), b.canonical_form(), "formatting must not affect dedup");
    }

    #[test]
    fn rejects_invalid_proto() {
        assert!(parse("this is not protobuf").is_err());
    }
}
```

- [ ] **Step 2: Run / fail.** `cargo test -p crabka-schema-registry format::protobuf`.

- [ ] **Step 3: Implement** using `protox_parse::parse` (single file, no imports in slice 1) to get a `FileDescriptorProto`, then a deterministic encode as the dedup key.

```rust
//! Protobuf: parse a single `.proto` source via `protox` into a
//! FileDescriptorProto; dedup key = deterministic prost encoding of the
//! descriptor. Confluent-exact canonical form is deferred to slice 2.

use crate::error::SrError;
use super::ParsedSchema;
use prost::Message;

pub struct ProtobufSchema(prost_types::FileDescriptorProto);

pub fn parse(schema: &str) -> Result<ProtobufSchema, SrError> {
    // protox_parse::parse(file_name, source) -> Result<FileDescriptorProto, _>
    protox_parse::parse("schema.proto", schema)
        .map(ProtobufSchema)
        .map_err(|e| SrError::InvalidSchema(format!("Protobuf: {e}")))
}

impl ParsedSchema for ProtobufSchema {
    fn canonical_form(&self) -> String {
        // Deterministic: prost encodes fields in tag order; clear source-info
        // (line/column) so formatting differences do not change the bytes.
        let mut fd = self.0.clone();
        fd.source_code_info = None;
        fd.name = Some(String::new()); // file name must not affect dedup
        let mut buf = Vec::new();
        fd.encode(&mut buf).expect("descriptor encodes");
        tracing::debug!("protobuf dedup key = descriptor bytes (not Confluent canonical form)");
        // hex so it is a printable, comparable String like the other formats
        buf.iter().map(|b| format!("{b:02x}")).collect()
    }
}
```

> Verify `prost_types` and `protox_parse` are reachable: `protox` re-exports `protox_parse`; `prost_types::FileDescriptorProto` comes via `prost-types` (a `protox` dep). If not exposed, add `prost = { workspace = true }` and `prost-types` explicitly to `[dependencies]`. Resolve at implementation time; the test is the gate.

- [ ] **Step 4: Run / pass.** `cargo test -p crabka-schema-registry format::protobuf`.
- [ ] **Step 5: Commit.**

---

## Task 8: `store/mod.rs` — in-memory state + id/version model

**Files:** Modify `crates/schema-registry/src/store/mod.rs`; test inline.

- [ ] **Step 1: Write the failing test** covering the id/version/dedup rules from the spec.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::SchemaType;

    fn av(n: &str) -> String { format!("{{\"type\":\"record\",\"name\":\"{n}\",\"fields\":[]}}") }

    #[test]
    fn first_registration_gets_id_1_version_1() {
        let mut s = StoreState::default();
        let r = s.register("av-value", SchemaType::Avro, &av("A")).unwrap();
        assert_eq!((r.id, r.version), (1, 1));
    }

    #[test]
    fn identical_under_same_subject_is_idempotent() {
        let mut s = StoreState::default();
        let r1 = s.register("av-value", SchemaType::Avro, &av("A")).unwrap();
        let r2 = s.register("av-value", SchemaType::Avro, &av("A")).unwrap();
        assert_eq!(r1, r2, "no new id or version");
        assert_eq!(s.versions("av-value").unwrap(), vec![1]);
    }

    #[test]
    fn same_schema_new_subject_reuses_global_id_fresh_version() {
        let mut s = StoreState::default();
        let r1 = s.register("av-value", SchemaType::Avro, &av("A")).unwrap();
        let r2 = s.register("other-value", SchemaType::Avro, &av("A")).unwrap();
        assert_eq!(r1.id, r2.id, "global id reused for identical canonical form");
        assert_eq!(r2.version, 1);
    }

    #[test]
    fn different_schema_increments_id_and_version() {
        let mut s = StoreState::default();
        let r1 = s.register("av-value", SchemaType::Avro, &av("A")).unwrap();
        let r2 = s.register("av-value", SchemaType::Avro, &av("B")).unwrap();
        assert_eq!(r2.id, r1.id + 1);
        assert_eq!(r2.version, 2);
        assert_eq!(s.versions("av-value").unwrap(), vec![1, 2]);
    }
}
```

- [ ] **Step 2: Run / fail.** `cargo test -p crabka-schema-registry store::`.

- [ ] **Step 3: Implement `store/mod.rs`.** `StoreState` holds the authoritative maps and both `register()` (used by the write path to compute the *next* id/version and the record to persist) and `apply()` (used by the reader to fold a decoded `_schemas` record into state). Slice 1 keeps them consistent by having the write path call `register()` to decide, persist, then the reader `apply()` the same record on replay — `apply()` is idempotent.

```rust
//! In-memory authoritative registry state, rebuilt by replaying `_schemas`.
//! Pure data structure: no I/O. The KafkaStore wraps it behind a lock and the
//! write-serialisation gate (see kafkastore/mod.rs).

use std::collections::BTreeMap;

use crate::error::SrError;
use crate::format::{self, SchemaType};
use crate::kafkastore::record::{SchemaKey, SchemaValue};

/// Result of a registration: the global id and the per-subject version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Registered { pub id: i32, pub version: i32 }

#[derive(Debug, Clone)]
struct VersionEntry { version: i32, id: i32 }

#[derive(Debug, Default)]
pub struct StoreState {
    /// subject -> ordered versions.
    subjects: BTreeMap<String, Vec<VersionEntry>>,
    /// global id -> (schemaType, schema string).
    by_id: BTreeMap<i32, (SchemaType, String)>,
    /// canonical form -> global id (dedup).
    by_canonical: BTreeMap<String, i32>,
    /// global compatibility + per-subject overrides (stored, not enforced).
    global_compat: Option<String>,
    subject_compat: BTreeMap<String, String>,
    max_id: i32,
}

impl StoreState {
    /// Decide id/version for a registration (does NOT mutate persistent log;
    /// the caller persists, then `apply()` mirrors it). Validates the schema
    /// (NONE compatibility still rejects unparseable schemas -> InvalidSchema).
    pub fn register(&mut self, subject: &str, ty: SchemaType, schema: &str) -> Result<Registered, SrError> {
        let parsed = format::parse(ty, schema)?;
        let canonical = parsed.canonical_form();

        // Idempotent within subject: identical canonical form already present?
        if let Some(vs) = self.subjects.get(subject) {
            if let Some(existing) = vs.iter().find(|v| {
                self.by_id.get(&v.id).is_some_and(|_| self.by_canonical.get(&canonical) == Some(&v.id))
            }) {
                return Ok(Registered { id: existing.id, version: existing.version });
            }
        }

        // Global id: reuse if this canonical form exists anywhere, else next.
        let id = match self.by_canonical.get(&canonical) {
            Some(&id) => id,
            None => {
                let id = self.max_id + 1;
                self.max_id = id;
                self.by_canonical.insert(canonical, id);
                self.by_id.insert(id, (ty, schema.to_string()));
                id
            }
        };
        let next_version = self.subjects.get(subject).map_or(1, |v| v.len() as i32 + 1);
        self.subjects.entry(subject.to_string()).or_default().push(VersionEntry { version: next_version, id });
        Ok(Registered { id, version: next_version })
    }

    /// Fold a decoded SCHEMA record into state (reader replay path). Idempotent.
    pub fn apply_schema(&mut self, _key: &SchemaKey, value: &SchemaValue) {
        if value.deleted { return; } // slice 1 has no deletes, but tolerate it
        let ty = SchemaType::from_wire(value.schema_type.as_deref());
        self.max_id = self.max_id.max(value.id);
        self.by_id.entry(value.id).or_insert((ty, value.schema.clone()));
        // Recompute canonical only if we can parse; tolerate parse failure on replay.
        if let Ok(p) = format::parse(ty, &value.schema) {
            self.by_canonical.entry(p.canonical_form()).or_insert(value.id);
        }
        let entry = self.subjects.entry(value.subject.clone()).or_default();
        if !entry.iter().any(|v| v.version == value.version) {
            entry.push(VersionEntry { version: value.version, id: value.id });
            entry.sort_by_key(|v| v.version);
        }
    }

    pub fn set_global_compat(&mut self, level: String) { self.global_compat = Some(level); }
    pub fn set_subject_compat(&mut self, subject: &str, level: String) { self.subject_compat.insert(subject.into(), level); }
    #[must_use] pub fn global_compat(&self) -> &str { self.global_compat.as_deref().unwrap_or("BACKWARD") }
    #[must_use] pub fn subject_compat(&self, subject: &str) -> Option<&str> { self.subject_compat.get(subject).map(String::as_str) }

    #[must_use] pub fn subjects(&self) -> Vec<String> { self.subjects.keys().cloned().collect() }
    #[must_use] pub fn versions(&self, subject: &str) -> Option<Vec<i32>> {
        self.subjects.get(subject).map(|vs| vs.iter().map(|v| v.version).collect())
    }
    /// (id, schemaType, schema) for a subject+version; `version=None` = latest.
    #[must_use] pub fn version(&self, subject: &str, version: Option<i32>) -> Option<(i32, SchemaType, String)> {
        let vs = self.subjects.get(subject)?;
        let entry = match version { Some(v) => vs.iter().find(|e| e.version == v)?, None => vs.last()? };
        let (ty, schema) = self.by_id.get(&entry.id)?;
        Some((entry.id, *ty, schema.clone()))
    }
    #[must_use] pub fn schema_by_id(&self, id: i32) -> Option<(SchemaType, String)> { self.by_id.get(&id).cloned() }
    /// Lookup an already-registered schema under a subject (POST /subjects/{s}).
    #[must_use] pub fn find_under_subject(&self, subject: &str, ty: SchemaType, schema: &str) -> Option<Registered> {
        let canonical = format::parse(ty, schema).ok()?.canonical_form();
        let id = *self.by_canonical.get(&canonical)?;
        let vs = self.subjects.get(subject)?;
        let entry = vs.iter().find(|e| e.id == id)?;
        Some(Registered { id, version: entry.version })
    }
}
```

- [ ] **Step 4: Run / pass.** `cargo test -p crabka-schema-registry store::`.
- [ ] **Step 5: Commit.**

---

## Task 9: `kafkastore/topic.rs` — auto-create `_schemas`

**Files:** Modify `crates/schema-registry/src/kafkastore/topic.rs`; test = covered by the integration test (Task 20) since it needs a broker. Add a small unit test for the `uuid -> WireUuid` conversion only.

- [ ] **Step 1: Implement `ensure_schemas_topic`** mirroring `remote-storage-topic/src/kafka_log.rs::ensure_topic` (create; on `TOPIC_ALREADY_EXISTS` (code 36) fall through to `metadata`; resolve `topic_id`).

```rust
//! Ensure the `_schemas` compacted topic exists; resolve its `topic_id`
//! (needed by Fetch v>=13). Mirrors remote-storage-topic's ensure_topic.

use std::collections::BTreeMap;

use crabka_client_admin::AdminClient;
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

use crate::config::RegistryConfig;

/// TOPIC_ALREADY_EXISTS Kafka error code.
const TOPIC_ALREADY_EXISTS: i16 = 36;

/// Create `_schemas` (1 partition, cleanup.policy=compact) if absent and return
/// its `topic_id`. Idempotent.
pub async fn ensure_schemas_topic(cfg: &RegistryConfig) -> anyhow::Result<WireUuid> {
    let bootstrap: Vec<String> = cfg.bootstrap.split(',').map(|s| s.trim().to_string()).collect();
    let mut admin = AdminClient::connect(&bootstrap).await?;

    let spec = crabka_client_admin::topics::CreateTopicSpec {
        name: cfg.schemas_topic.clone(),
        partitions: 1,
        replicas: cfg.schemas_topic_rf,
        configs: BTreeMap::from([
            ("cleanup.policy".to_string(), "compact".to_string()),
        ]),
    };
    let outcomes = admin.create_topics(&[spec], 15_000).await?;
    if let Some(o) = outcomes.into_iter().next() {
        match o.error {
            None => if let Some(id) = o.topic_id { return Ok(to_wire_uuid(id)); },
            Some(e) if e.code == TOPIC_ALREADY_EXISTS => {} // fall through to metadata
            Some(e) => anyhow::bail!("create _schemas failed: {} ({})", e.name, e.code),
        }
    }
    // Resolve id via metadata (topic already existed, or create gave no id).
    let md = admin.metadata(&[cfg.schemas_topic.as_str()]).await?;
    let entry = md.topics.into_iter().find(|t| t.name == cfg.schemas_topic)
        .ok_or_else(|| anyhow::anyhow!("_schemas not found after create"))?;
    Ok(entry.topic_id.map(to_wire_uuid).unwrap_or(WireUuid::ZERO))
}

/// Convert admin's `uuid::Uuid` to the protocol `WireUuid` (same byte order).
fn to_wire_uuid(id: uuid::Uuid) -> WireUuid {
    WireUuid::from(id.into_bytes())
}
```

> Confirm `CreateTopicSpec` is re-exported at `crabka_client_admin::topics::CreateTopicSpec` (grep showed it in `client-admin/src/topics.rs`; it may also be re-exported at crate root — use whichever path resolves). Confirm `WireUuid::from([u8;16])` exists; if the constructor differs, use the same helper `remote-storage-topic` uses (`to_wire_uuid` at kafka_log.rs ~388) and copy it verbatim.

- [ ] **Step 2: Unit test** the conversion (no broker needed):

```rust
#[cfg(test)]
mod tests {
    use super::to_wire_uuid;
    #[test]
    fn uuid_bytes_preserved() {
        let u = uuid::Uuid::from_u128(0x1234_5678_9abc_def0_1122_3344_5566_7788);
        assert_eq!(to_wire_uuid(u).to_string(), u.to_string());
    }
}
```

- [ ] **Step 3: Run / pass** the unit test; full create path is exercised by Task 20. Run: `cargo test -p crabka-schema-registry kafkastore::topic`.
- [ ] **Step 4: Commit.**

---

## Task 10: `kafkastore/writer.rs` — primary writer

**Files:** Modify `crates/schema-registry/src/kafkastore/writer.rs`; tested via Task 20.

- [ ] **Step 1: Implement `SchemaWriter`.**

```rust
//! Primary-only writer: serialises a `_schemas` record and produces it,
//! returning the produced offset for read-your-writes gating.

use bytes::Bytes;
use crabka_client_producer::{Acks, Producer, ProducerRecord};

use crate::config::RegistryConfig;

pub struct SchemaWriter {
    producer: Producer,
    topic: String,
}

impl SchemaWriter {
    pub async fn start(cfg: &RegistryConfig) -> anyhow::Result<Self> {
        let producer = Producer::builder()
            .bootstrap(cfg.bootstrap.clone())
            .client_id(format!("{}-writer", cfg.client_id))
            .enable_idempotence(true) // forces acks=All; schema writes must not be lost
            .acks(Acks::All)
            .build()
            .await?;
        Ok(Self { producer, topic: cfg.schemas_topic.clone() })
    }

    /// Produce one keyed `_schemas` record; return the assigned offset.
    pub async fn produce(&self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<i64> {
        let rx = self
            .producer
            .send(ProducerRecord {
                topic: self.topic.clone(),
                key: Some(Bytes::from(key)),
                value: Some(Bytes::from(value)),
                ..Default::default()
            })
            .await;
        let meta = rx.await.map_err(|_| anyhow::anyhow!("producer dropped ack"))??;
        Ok(meta.offset)
    }
}
```

- [ ] **Step 2: Build.** Run: `cargo build -p crabka-schema-registry`. Expected: compiles.
- [ ] **Step 3: Commit.**

---

## Task 11: `kafkastore/reader.rs` — group-less StoreReader

**Files:** Modify `crates/schema-registry/src/kafkastore/reader.rs`; tested via Task 20.

- [ ] **Step 1: Implement the reader loop** mirroring `remote-storage-topic/src/kafka_log.rs::partition_fetch_loop`. It connects to the bootstrap broker (slice 1: single broker == leader of `_schemas`-0), fetches from offset 0 forward, decodes each record, applies it under the store lock, and publishes the last-applied offset via a `watch` channel for read-your-writes.

```rust
//! Group-less reader: tails `_schemas` partition 0 over a dedicated connection,
//! folds records into the shared store, and publishes the last-applied offset.

use std::net::ToSocketAddrs;
use std::sync::Arc;

use crabka_client_core::{Connection, ConnectionOptions, fetch_partition};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use parking_lot::RwLock;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::config::RegistryConfig;
use crate::kafkastore::record::SchemaRecord;
use crate::store::StoreState;

pub struct StoreReader {
    pub store: Arc<RwLock<StoreState>>,
    pub applied_rx: watch::Receiver<i64>,
}

/// Spawn the reader. Returns immediately with the shared store + an offset
/// watch; the background task runs until `cancel` fires.
pub fn spawn(
    cfg: &RegistryConfig,
    topic_id: WireUuid,
    cancel: CancellationToken,
) -> StoreReader {
    let store = Arc::new(RwLock::new(StoreState::default()));
    let (applied_tx, applied_rx) = watch::channel(-1_i64);
    let topic = cfg.schemas_topic.clone();
    let bootstrap = cfg.bootstrap.clone();
    let client_id = format!("{}-reader", cfg.client_id);
    let store_bg = store.clone();

    tokio::spawn(async move {
        let Some(addr) = bootstrap.split(',').next().and_then(|b| b.trim().to_socket_addrs().ok()).and_then(|mut a| a.next()) else {
            tracing::error!(%bootstrap, "store reader: bad bootstrap addr");
            return;
        };
        let opts = ConnectionOptions { client_id, ..Default::default() };
        let conn = match Connection::connect_with_options(addr, opts).await {
            Ok(c) => c,
            Err(e) => { tracing::error!(error=%e, "store reader: connect failed"); return; }
        };
        let mut next = 0_i64;
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => { conn.close(); return; }
                res = fetch_partition(&conn, &topic, topic_id, 0, next, 500, 1 << 20) => {
                    match res {
                        Ok(records) => {
                            for r in records {
                                if r.offset < next { continue; }
                                let key = r.key.as_deref().unwrap_or_default();
                                let rec = SchemaRecord::decode(key, r.value.as_deref());
                                if let SchemaRecord::Schema(k, v) = &rec {
                                    store_bg.write().apply_schema(k, v);
                                } else if let SchemaRecord::Config(k, v) = &rec {
                                    let mut s = store_bg.write();
                                    match &k.subject {
                                        Some(subj) => s.set_subject_compat(subj, v.compatibility_level.clone()),
                                        None => s.set_global_compat(v.compatibility_level.clone()),
                                    }
                                }
                                next = r.offset + 1;
                                let _ = applied_tx.send(r.offset);
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error=%e, "store reader: fetch error; backing off");
                            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                        }
                    }
                }
            }
        }
    });

    StoreReader { store, applied_rx }
}
```

- [ ] **Step 2: Build.** Run: `cargo build -p crabka-schema-registry`. Expected: compiles.
- [ ] **Step 3: Commit.**

---

## Task 12: `kafkastore/mod.rs` — facade + read-your-writes

**Files:** Modify `crates/schema-registry/src/kafkastore/mod.rs`; tested via Task 20.

- [ ] **Step 1: Implement the `KafkaStore` facade.** It owns the writer, the shared store, the offset watch, and a `tokio::sync::Mutex<()>` write gate that serialises the *decide id/version → produce → wait-for-apply* critical section (correct for single-node always-primary; replaced by election in slice 5).

```rust
//! KafkaStore: wires topic creation, the reader, the writer, and the
//! single-writer read-your-writes gate. The REST layer holds an Arc<KafkaStore>.

pub mod reader;
pub mod record;
pub mod topic;
pub mod writer;

use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::{watch, Mutex};
use tokio_util::sync::CancellationToken;

use crate::config::RegistryConfig;
use crate::error::SrError;
use crate::format::SchemaType;
use crate::store::{Registered, StoreState};

pub struct KafkaStore {
    pub store: Arc<RwLock<StoreState>>,
    applied_rx: watch::Receiver<i64>,
    writer: writer::SchemaWriter,
    write_gate: Mutex<()>,
    schemas_topic: String,
}

impl KafkaStore {
    /// Create `_schemas`, start the reader, build the writer, and block until
    /// the reader has replayed the existing log (initial catch-up).
    pub async fn start(cfg: &RegistryConfig, cancel: CancellationToken) -> anyhow::Result<Arc<Self>> {
        let topic_id = topic::ensure_schemas_topic(cfg).await?;
        let r = reader::spawn(cfg, topic_id, cancel);
        let writer = writer::SchemaWriter::start(cfg).await?;
        Ok(Arc::new(Self {
            store: r.store,
            applied_rx: r.applied_rx,
            writer,
            write_gate: Mutex::new(()),
            schemas_topic: cfg.schemas_topic.clone(),
        }))
    }

    /// Register a schema: decide id/version, persist to `_schemas`, wait for the
    /// reader to apply it (read-your-writes), then return. Serialised by the gate.
    pub async fn register(&self, subject: &str, ty: SchemaType, schema: &str) -> Result<Registered, SrError> {
        let _gate = self.write_gate.lock().await;

        // Decide on a *clone* so we never mutate shared state before the write
        // is durable; the reader is the only mutator of `self.store`.
        let mut probe = StoreState::default();
        // Cheap correctness: re-derive from current shared snapshot.
        let reg = {
            let snap = self.store.read();
            // Idempotent / dedup checks against the live store:
            if let Some(existing) = snap.find_under_subject(subject, ty, schema) {
                return Ok(existing);
            }
            // Compute next id/version from the live snapshot via a transient copy.
            probe.mirror_from(&snap); // see Step 2 note
            probe.register(subject, ty, schema)?
        };

        // Serialise the record (record.rs) and produce it.
        let (key, value) = record::encode_schema(subject, reg.version, reg.id, ty, schema);
        let offset = self.writer.produce(key, value).await.map_err(|e| SrError::Backend(e.to_string()))?;

        // Read-your-writes: wait until the reader applies up to `offset`.
        let mut rx = self.applied_rx.clone();
        while *rx.borrow() < offset {
            if rx.changed().await.is_err() { break; }
        }
        Ok(reg)
    }

    #[must_use] pub fn topic(&self) -> &str { &self.schemas_topic }
}
```

> Step 2 note: `probe.mirror_from(&snap)` and `record::encode_schema(...)` are small helpers to add — `mirror_from` clones the id/version/canonical maps so `register()` can compute the next id without touching the shared store; `encode_schema` builds the exact `_schemas` key/value bytes from the record types (Task 4). If cloning the whole store per write is undesirable later, slice 5's election rework revisits this; for slice 1 correctness-over-speed is fine. Implement `mirror_from` on `StoreState` (a field-wise clone) and `encode_schema` in `record.rs`. Add their unit tests alongside.

- [ ] **Step 2: Build + clippy.** Run: `cargo build -p crabka-schema-registry && cargo clippy -p crabka-schema-registry --all-targets -- -D warnings`. Expected: clean.
- [ ] **Step 3: Commit.**

---

## Task 13: `rest/response.rs` — success-body helper

**Files:** Modify `crates/schema-registry/src/rest/response.rs`; test inline.

- [ ] **Step 1: Failing test.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;

    #[tokio::test]
    async fn ok_json_sets_vendor_content_type() {
        let resp = ok_json(&serde_json::json!({"id": 7})).into_response();
        assert_eq!(resp.headers()["content-type"], crate::error::CONTENT_TYPE);
        let body = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();
        assert_eq!(&body[..], br#"{"id":7}"#);
    }
}
```

- [ ] **Step 2: Run / fail.** `cargo test -p crabka-schema-registry rest::response`.
- [ ] **Step 3: Implement.**

```rust
//! Success-response helper: serialises with `serde_json` and sets the Confluent
//! vendor content-type (axum's `Json` would force `application/json`).

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::error::CONTENT_TYPE;

/// 200 OK with a JSON body and the vendor content-type.
pub fn ok_json<T: Serialize>(value: &T) -> Response {
    match serde_json::to_string(value) {
        Ok(body) => (StatusCode::OK, [("content-type", CONTENT_TYPE)], body).into_response(),
        Err(e) => crate::error::SrError::Backend(e.to_string()).into_response(),
    }
}

/// Raw 200 with the vendor content-type (for `/schema` raw-text endpoint, which
/// returns the schema string verbatim — still vendor content-type per Confluent).
pub fn ok_raw(body: String) -> Response {
    (StatusCode::OK, [("content-type", CONTENT_TYPE)], body).into_response()
}
```

- [ ] **Step 4: Run / pass.** `cargo test -p crabka-schema-registry rest::response`.
- [ ] **Step 5: Commit.**

---

## Task 14: `rest/schemas.rs`

**Files:** Modify `crates/schema-registry/src/rest/schemas.rs`; covered by Task 20 conformance test.

- [ ] **Step 1: Implement handlers** (`GET /schemas/ids/{id}`, `GET /schemas/types`). Uses `AppState` (defined in Task 18) holding `Arc<KafkaStore>`.

```rust
//! /schemas/* read endpoints.

use axum::extract::{Path, State};
use axum::response::Response;

use crate::error::SrError;
use crate::rest::{response::ok_json, AppState};

/// GET /schemas/ids/{id}
pub async fn get_by_id(State(st): State<AppState>, Path(id): Path<i32>) -> Result<Response, SrError> {
    let (ty, schema) = st.store.store.read().schema_by_id(id).ok_or(SrError::SchemaNotFound)?;
    let mut body = serde_json::Map::new();
    if let Some(t) = ty.wire_name() { body.insert("schemaType".into(), t.into()); }
    body.insert("schema".into(), schema.into());
    Ok(ok_json(&serde_json::Value::Object(body)))
}

/// GET /schemas/types
pub async fn types(State(_st): State<AppState>) -> Response {
    ok_json(&serde_json::json!(["AVRO", "JSON", "PROTOBUF"]))
}
```

> Confirm against `rest_get_by_id_avro.json`: AVRO responses omit `schemaType`; ordering of `schema`/`schemaType` keys must match the fixture. Adjust the map insertion order accordingly.

- [ ] **Step 2: Build.** `cargo build -p crabka-schema-registry`.
- [ ] **Step 3: Commit.**

---

## Task 15: `rest/subjects.rs`

**Files:** Modify `crates/schema-registry/src/rest/subjects.rs`; covered by Task 20.

- [ ] **Step 1: Implement handlers:** `POST /subjects/{s}/versions` (register), `POST /subjects/{s}` (lookup-if-registered), `GET /subjects`, `GET /subjects/{s}/versions`, `GET /subjects/{s}/versions/{v}` (int or `latest`), `GET /subjects/{s}/versions/{v}/schema`. Request bodies are read as `String` and parsed with `serde_json` (vendor content-type exactness — no `axum::Json`).

```rust
//! /subjects/* endpoints.

use axum::extract::{Path, State};
use axum::response::Response;
use serde::Deserialize;

use crate::error::SrError;
use crate::format::SchemaType;
use crate::rest::{response::{ok_json, ok_raw}, AppState};

#[derive(Deserialize)]
struct RegisterBody {
    schema: String,
    #[serde(rename = "schemaType", default)]
    schema_type: Option<String>,
    // references omitted in slice 1 (parsed-but-ignored if present)
    #[serde(default)]
    references: Vec<serde_json::Value>,
}

/// POST /subjects/{subject}/versions  -> {"id":N}
pub async fn register(State(st): State<AppState>, Path(subject): Path<String>, body: String) -> Result<Response, SrError> {
    let req: RegisterBody = serde_json::from_str(&body).map_err(|e| SrError::InvalidSchema(e.to_string()))?;
    let _ = req.references;
    let ty = SchemaType::from_wire(req.schema_type.as_deref());
    let reg = st.store.register(&subject, ty, &req.schema).await?;
    Ok(ok_json(&serde_json::json!({ "id": reg.id })))
}

/// POST /subjects/{subject}  -> {subject,id,version,schema} | 404
pub async fn lookup(State(st): State<AppState>, Path(subject): Path<String>, body: String) -> Result<Response, SrError> {
    let req: RegisterBody = serde_json::from_str(&body).map_err(|e| SrError::InvalidSchema(e.to_string()))?;
    let ty = SchemaType::from_wire(req.schema_type.as_deref());
    let found = {
        let s = st.store.store.read();
        s.find_under_subject(&subject, ty, &req.schema)
            .map(|r| (r, s.schema_by_id(r.id)))
    };
    match found {
        Some((r, Some((sty, schema)))) => {
            let mut m = serde_json::Map::new();
            m.insert("subject".into(), subject.into());
            m.insert("id".into(), r.id.into());
            m.insert("version".into(), r.version.into());
            if let Some(t) = sty.wire_name() { m.insert("schemaType".into(), t.into()); }
            m.insert("schema".into(), schema.into());
            Ok(ok_json(&serde_json::Value::Object(m)))
        }
        _ => Err(SrError::SchemaNotFound),
    }
}

/// GET /subjects
pub async fn list(State(st): State<AppState>) -> Response {
    ok_json(&st.store.store.read().subjects())
}

/// GET /subjects/{subject}/versions
pub async fn versions(State(st): State<AppState>, Path(subject): Path<String>) -> Result<Response, SrError> {
    let vs = st.store.store.read().versions(&subject).ok_or_else(|| SrError::SubjectNotFound(subject.clone()))?;
    Ok(ok_json(&vs))
}

fn parse_version(subject: &str, v: &str) -> Result<Option<i32>, SrError> {
    if v == "latest" { return Ok(None); }
    v.parse::<i32>().map(Some).map_err(|_| SrError::InvalidVersion(v.to_string()))
        .and_then(|opt| match opt { Some(n) if n < 1 => Err(SrError::InvalidVersion(v.to_string())), other => Ok(other) })
        .map_err(|_| SrError::SubjectNotFound(subject.to_string()))
}

/// GET /subjects/{subject}/versions/{version}
pub async fn get_version(State(st): State<AppState>, Path((subject, version)): Path<(String, String)>) -> Result<Response, SrError> {
    let want = parse_version(&subject, &version)?;
    let (id, ty, schema) = {
        let s = st.store.store.read();
        if s.versions(&subject).is_none() { return Err(SrError::SubjectNotFound(subject)); }
        s.version(&subject, want).ok_or(SrError::VersionNotFound)?
    };
    let mut m = serde_json::Map::new();
    m.insert("subject".into(), subject.into());
    m.insert("version".into(), want.unwrap_or(id).into()); // replaced below; see note
    m.insert("id".into(), id.into());
    if let Some(t) = ty.wire_name() { m.insert("schemaType".into(), t.into()); }
    m.insert("schema".into(), schema.into());
    Ok(ok_json(&serde_json::Value::Object(m)))
}

/// GET /subjects/{subject}/versions/{version}/schema  -> raw schema text
pub async fn get_version_schema(State(st): State<AppState>, Path((subject, version)): Path<(String, String)>) -> Result<Response, SrError> {
    let want = parse_version(&subject, &version)?;
    let (_, _, schema) = st.store.store.read().version(&subject, want).ok_or(SrError::VersionNotFound)?;
    Ok(ok_raw(schema))
}
```

> Note: the `version` field in the `get_version` response must be the *actual* version number, not the request token. Resolve `latest` to the concrete version (extend `StoreState::version` to also return the version, or look it up). Adjust the `m.insert("version", ...)` line to use the resolved version. The Step-1 implementation is a sketch; the conformance fixture `rest_get_version_avro.json` is the gate — match its keys, order, and the resolved version value exactly.

- [ ] **Step 2: Build.** `cargo build -p crabka-schema-registry`.
- [ ] **Step 3: Commit.**

---

## Task 16: `rest/config.rs`

**Files:** Modify `crates/schema-registry/src/rest/config.rs`; covered by Task 20.

- [ ] **Step 1: Implement** `GET/PUT /config` and `GET/PUT /config/{subject}`. PUT persists a CONFIG record via the KafkaStore (so it survives restart and replays), then returns `{"compatibility":"<LEVEL>"}`. Validate the level against the Confluent set; reject others with `42203`.

```rust
//! /config endpoints. Stored (and replayed) but NOT enforced in slice 1.

use axum::extract::{Path, State};
use axum::response::Response;
use serde::Deserialize;

use crate::error::SrError;
use crate::rest::{response::ok_json, AppState};

const LEVELS: &[&str] = &["NONE","BACKWARD","BACKWARD_TRANSITIVE","FORWARD","FORWARD_TRANSITIVE","FULL","FULL_TRANSITIVE"];

#[derive(Deserialize)]
struct PutConfig { compatibility: String }

fn validate(level: &str) -> Result<(), SrError> {
    if LEVELS.contains(&level) { Ok(()) } else { Err(SrError::InvalidCompatibilityLevel(level.to_string())) }
}

pub async fn get_global(State(st): State<AppState>) -> Response {
    let lvl = st.store.store.read().global_compat().to_string();
    ok_json(&serde_json::json!({ "compatibilityLevel": lvl }))
}

pub async fn put_global(State(st): State<AppState>, body: String) -> Result<Response, SrError> {
    let req: PutConfig = serde_json::from_str(&body).map_err(|e| SrError::InvalidCompatibilityLevel(e.to_string()))?;
    validate(&req.compatibility)?;
    st.store.set_global_compat(req.compatibility.clone()).await.map_err(|e| SrError::Backend(e.to_string()))?;
    Ok(ok_json(&serde_json::json!({ "compatibility": req.compatibility })))
}

pub async fn get_subject(State(st): State<AppState>, Path(subject): Path<String>) -> Result<Response, SrError> {
    let lvl = st.store.store.read().subject_compat(&subject).map(str::to_string)
        .ok_or_else(|| SrError::SubjectNotFound(subject.clone()))?;
    Ok(ok_json(&serde_json::json!({ "compatibilityLevel": lvl })))
}

pub async fn put_subject(State(st): State<AppState>, Path(subject): Path<String>, body: String) -> Result<Response, SrError> {
    let req: PutConfig = serde_json::from_str(&body).map_err(|e| SrError::InvalidCompatibilityLevel(e.to_string()))?;
    validate(&req.compatibility)?;
    st.store.set_subject_compat(&subject, req.compatibility.clone()).await.map_err(|e| SrError::Backend(e.to_string()))?;
    Ok(ok_json(&serde_json::json!({ "compatibility": req.compatibility })))
}
```

> Add `KafkaStore::set_global_compat` / `set_subject_compat` async methods (mirroring `register`: serialise a CONFIG record, produce, wait for read-your-writes). The in-memory `StoreState::set_*_compat` are applied by the reader on replay. Verify `GET /config` body key (`compatibilityLevel`) and `PUT` echo key (`compatibility`) against `rest_get_config.json` — Confluent uses different keys on GET vs PUT.

- [ ] **Step 2: Build.** `cargo build -p crabka-schema-registry`.
- [ ] **Step 3: Commit.**

---

## Task 17: `rest/compatibility.rs`

**Files:** Modify `crates/schema-registry/src/rest/compatibility.rs`; covered by Task 20.

- [ ] **Step 1: Implement** `POST /compatibility/subjects/{s}/versions/{v}` returning `{"is_compatible":true}` (NONE engine — always compatible if the schema parses). Still validate the posted schema parses (else `42201`), matching Confluent.

```rust
//! Compatibility check. Slice 1: NONE engine — always compatible for a
//! well-formed schema. Real checks arrive in slice 2.

use axum::extract::{Path, State};
use axum::response::Response;
use serde::Deserialize;

use crate::error::SrError;
use crate::format::{self, SchemaType};
use crate::rest::{response::ok_json, AppState};

#[derive(Deserialize)]
struct Body { schema: String, #[serde(rename = "schemaType", default)] schema_type: Option<String> }

pub async fn check(State(_st): State<AppState>, Path((_subject, _version)): Path<(String, String)>, body: String) -> Result<Response, SrError> {
    let req: Body = serde_json::from_str(&body).map_err(|e| SrError::InvalidSchema(e.to_string()))?;
    let ty = SchemaType::from_wire(req.schema_type.as_deref());
    format::parse(ty, &req.schema)?; // 42201 if unparseable
    Ok(ok_json(&serde_json::json!({ "is_compatible": true })))
}
```

- [ ] **Step 2: Build.** `cargo build -p crabka-schema-registry`.
- [ ] **Step 3: Commit.**

---

## Task 18: `rest/mod.rs` — AppState + Router

**Files:** Modify `crates/schema-registry/src/rest/mod.rs`; test inline (router builds, `/` and `/schemas/types` respond via `tower::oneshot`).

- [ ] **Step 1: Failing test.**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn types_route_responds() {
        // Build router with a store that needs no broker: inject via a test ctor.
        let app = test_router();
        let resp = app.oneshot(Request::builder().uri("/schemas/types").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
```

- [ ] **Step 2: Run / fail.** `cargo test -p crabka-schema-registry rest::mod`.
- [ ] **Step 3: Implement `rest/mod.rs`.**

```rust
//! HTTP surface: AppState + the merged Confluent route table.

pub mod compatibility;
pub mod config;
pub mod response;
pub mod schemas;
pub mod subjects;

use std::sync::Arc;

use axum::routing::{get, post, put};
use axum::Router;

use crate::kafkastore::KafkaStore;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<KafkaStore>,
}

#[must_use]
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(|| async { response::ok_json(&serde_json::json!({})) }))
        .route("/schemas/types", get(schemas::types))
        .route("/schemas/ids/{id}", get(schemas::get_by_id))
        .route("/subjects", get(subjects::list))
        .route("/subjects/{subject}", post(subjects::lookup))
        .route("/subjects/{subject}/versions", get(subjects::versions).post(subjects::register))
        .route("/subjects/{subject}/versions/{version}", get(subjects::get_version))
        .route("/subjects/{subject}/versions/{version}/schema", get(subjects::get_version_schema))
        .route("/config", get(config::get_global).put(config::put_global))
        .route("/config/{subject}", get(config::get_subject).put(config::put_subject))
        .route("/compatibility/subjects/{subject}/versions/{version}", post(compatibility::check))
        .with_state(state)
}

#[cfg(test)]
fn test_router() -> Router {
    // A KafkaStore::for_tests(StoreState) ctor that skips broker wiring — add it
    // to kafkastore/mod.rs behind #[cfg(any(test, feature = "test-helpers"))].
    let store = KafkaStore::for_tests(crate::store::StoreState::default());
    router(AppState { store: Arc::new(store) })
}
```

> Add `KafkaStore::for_tests(StoreState) -> KafkaStore` (a no-broker constructor: store behind the lock, a writer that is never used because no write route is exercised in the unit test, and an `applied_rx` seeded to `i64::MAX`). Gate it `#[cfg(any(test, feature = "test-helpers"))]`. axum 0.8 path params use `{name}` syntax (not `:name`) — keep as written.

- [ ] **Step 4: Run / pass.** `cargo test -p crabka-schema-registry rest::mod`.
- [ ] **Step 5: Commit.**

---

## Task 19: `src/bin/schema-registry.rs` — binary

**Files:** Modify `crates/schema-registry/src/bin/schema-registry.rs`, `crates/schema-registry/src/config.rs`.

- [ ] **Step 1: Implement** clap `Args`, build `RegistryConfig`, start `KafkaStore`, serve axum with graceful shutdown (mirror `rebalancer.rs`).

```rust
//! crabka-schema-registry: Confluent Schema Registry-compatible REST service.

use std::net::SocketAddr;

use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crabka_schema_registry::config::RegistryConfig;
use crabka_schema_registry::kafkastore::KafkaStore;
use crabka_schema_registry::rest::{self, AppState};

#[derive(Debug, Parser)]
#[command(name = "crabka-schema-registry", version, about = "Confluent Schema Registry-compatible service for Crabka")]
struct Args {
    #[arg(long, env = "CRABKA_BOOTSTRAP_SERVERS")]
    bootstrap_servers: String,
    #[arg(long, env = "SCHEMA_REGISTRY_LISTEN_ADDR", default_value = "0.0.0.0:8081")]
    listen_addr: SocketAddr,
    #[arg(long, env = "SCHEMA_REGISTRY_SCHEMAS_TOPIC", default_value = "_schemas")]
    schemas_topic: String,
    #[arg(long, env = "SCHEMA_REGISTRY_SCHEMAS_TOPIC_RF", default_value_t = 3)]
    schemas_topic_rf: i32,
    #[arg(long, env = "SCHEMA_REGISTRY_CLIENT_ID", default_value = "crabka-schema-registry")]
    client_id: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "crabka_schema_registry=info,info".into()),
        )
        .init();

    let args = Args::parse();
    let cfg = RegistryConfig {
        bootstrap: args.bootstrap_servers,
        schemas_topic: args.schemas_topic,
        schemas_topic_rf: args.schemas_topic_rf,
        client_id: args.client_id,
    };
    info!(listen = %args.listen_addr, bootstrap = %cfg.bootstrap, topic = %cfg.schemas_topic, "crabka-schema-registry starting");

    let shutdown = CancellationToken::new();
    let store = KafkaStore::start(&cfg, shutdown.clone()).await?;
    let app = rest::router(AppState { store });

    let listener = tokio::net::TcpListener::bind(args.listen_addr).await?;
    info!(addr = %listener.local_addr()?, "listening");
    let shutdown_for_axum = shutdown.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c().await.ok();
            shutdown_for_axum.cancel();
        })
        .await?;
    Ok(())
}
```

- [ ] **Step 2: Build + run `--help`.** Run: `cargo run -p crabka-schema-registry -- --help`. Expected: prints usage with the flags above.
- [ ] **Step 3: Commit.**

---

## Task 20: `tests/integration.rs` — end-to-end vs in-process broker (no Docker)

**Files:** Create `crates/schema-registry/tests/integration.rs`.

- [ ] **Step 1: Write the end-to-end test.** Start an in-process broker, start `KafkaStore` against it, drive register/get directly through the store + the axum router (via `tower::oneshot`), proving the full `_schemas` ⇄ reader ⇄ store ⇄ REST pipe and read-your-writes.

```rust
#![cfg(not(target_os = "windows"))]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use crabka_broker::{Broker, BrokerConfig};
use crabka_schema_registry::config::RegistryConfig;
use crabka_schema_registry::kafkastore::KafkaStore;
use crabka_schema_registry::rest::{self, AppState};
use tokio_util::sync::CancellationToken;

async fn boot() -> (crabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf())).await.unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_then_get_round_trips_through_schemas_topic() {
    let (broker, bootstrap, _dir) = boot().await;
    let cfg = RegistryConfig {
        bootstrap,
        schemas_topic: "_schemas".into(),
        schemas_topic_rf: 1, // single in-process broker
        client_id: "sr-it".into(),
    };
    let cancel = CancellationToken::new();
    let store = KafkaStore::start(&cfg, cancel.clone()).await.unwrap();
    let app = rest::router(AppState { store: store.clone() });

    // Register an Avro schema via REST.
    let reg = app.clone().oneshot(
        Request::builder().method("POST").uri("/subjects/av-value/versions")
            .header("content-type", "application/vnd.schemaregistry.v1+json")
            .body(Body::from(r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"}"#))
            .unwrap()).await.unwrap();
    assert_eq!(reg.status(), StatusCode::OK);
    let body = axum::body::to_bytes(reg.into_body(), 1 << 20).await.unwrap();
    let id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"].as_i64().unwrap();
    assert_eq!(id, 1);

    // GET it back by id (read-your-writes already guaranteed by register()).
    let got = app.clone().oneshot(
        Request::builder().uri(format!("/schemas/ids/{id}")).body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(got.status(), StatusCode::OK);

    // GET subjects + versions.
    let subs = app.clone().oneshot(Request::builder().uri("/subjects").body(Body::empty()).unwrap()).await.unwrap();
    let subs_body = axum::body::to_bytes(subs.into_body(), 1 << 20).await.unwrap();
    assert_eq!(serde_json::from_slice::<Vec<String>>(&subs_body).unwrap(), vec!["av-value"]);

    // Idempotent re-register returns the same id.
    let again = app.oneshot(
        Request::builder().method("POST").uri("/subjects/av-value/versions")
            .body(Body::from(r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"}"#))
            .unwrap()).await.unwrap();
    let again_body = axum::body::to_bytes(again.into_body(), 1 << 20).await.unwrap();
    assert_eq!(serde_json::from_slice::<serde_json::Value>(&again_body).unwrap()["id"].as_i64().unwrap(), 1);

    drop(Arc::strong_count(&store));
    cancel.cancel();
    broker.shutdown().await;
}
```

- [ ] **Step 2: Add a second test** for Protobuf and JSON registration (same shape, different `schemaType` + schema), asserting distinct ids and that `GET /schemas/ids/{id}` includes `"schemaType":"PROTOBUF"`/`"JSON"`.
- [ ] **Step 3: Run.** Run: `cargo test -p crabka-schema-registry --test integration`. Expected: PASS.
- [ ] **Step 4: Commit.**

---

## Task 21: `tests/rest_conformance.rs` + `tests/interop.rs`

**Files:** Create `crates/schema-registry/tests/rest_conformance.rs` (no Docker, fixture assertions) and `crates/schema-registry/tests/interop.rs` (`#[ignore]`, Docker).

- [ ] **Step 1: `rest_conformance.rs`** — drive the router with an in-process broker (as Task 20) and assert response bodies byte/shape-match the captured fixtures (`rest_register_avro.json`, `rest_get_version_avro.json`, `rest_get_by_id_avro.json`, `rest_list_subjects.json`, `rest_get_config.json`) and that the error cases produce `rest_err_subject_not_found.json` / `rest_err_invalid_schema.json` shapes (assert `error_code` + status; allow `message` text to differ but log a warning if it does).

```rust
// For each fixture: issue the corresponding request, parse both bodies as
// serde_json::Value, and assert_eq! the Values (order-insensitive structural
// match). Additionally assert exact-byte equality for the _schemas-critical
// responses where the fixture and our output should be identical.
```

- [ ] **Step 2: `interop.rs`** — `#[ignore = "requires Docker"]`: stand up a Crabka broker on `0.0.0.0:9092` advertising `host.docker.internal:9092`; run `confluentinc/cp-schema-registry:7.4.0` against it; register a schema through *its* REST; then point our `KafkaStore` at the same broker/topic and assert our `StoreReader` decodes the cp-written `_schemas` records (`GET /schemas/ids/1` via our router returns the same schema). This is the byte-interop gate.

- [ ] **Step 3: Run** conformance (no Docker). Run: `cargo test -p crabka-schema-registry --test rest_conformance`. Expected: PASS.
- [ ] **Step 4: Run** interop manually if Docker present: `cargo test -p crabka-schema-registry --test interop -- --ignored --nocapture`.
- [ ] **Step 5: Commit.**

---

## Task 22: CI job + codecov flag

**Files:** Modify `.github/workflows/ci.yml`, `codecov.yml`.

- [ ] **Step 1: Add an `ubuntu-latest` job `schema-registry-integration`** mirroring `client-consumer-integration` (in-process, no Docker): `cargo llvm-cov -p crabka-schema-registry --test integration --test rest_conformance ... --lcov --output-path ...` uploaded under flag `schema-registry-integration`. (The per-crate integration flag is required because the default `--lib --bins` coverage never descends into `tests/` binaries.)

- [ ] **Step 2: Add the flag to `codecov.yml`** under `flags:` (carryforward: true) and bump `codecov.notify.after_n_builds` and `comment.after_n_builds` from 8 to 9.

- [ ] **Step 3: (Optional, Docker)** add a `schema-registry-interop` job mirroring `broker-jvm-acceptance` (adds `host.docker.internal` to `/etc/hosts`, preloads `confluentinc/cp-schema-registry:7.4.0`, runs `--test interop -- --ignored --test-threads=1`). If added, bump `after_n_builds` to 10 and add the flag.

- [ ] **Step 4: Validate YAML** locally if `actionlint`/`yq` is available; otherwise eyeball against the existing `client-consumer-integration` block. Commit.

---

## Task 23: Final fmt / clippy / workspace test gate

**Files:** none (verification + final commit).

- [ ] **Step 1: Format.** Run: `cargo fmt --all`. Then `git diff --stat` to see what changed; re-commit if needed.
- [ ] **Step 2: Clippy (the CI gate).** Run: `cargo clippy --workspace --all-targets -- -D warnings`. Expected: clean. Fix any `pedantic` lints in the new crate (the workspace sets `pedantic = warn` at priority -1).
- [ ] **Step 3: Workspace build + non-Docker tests.** Run: `cargo test --workspace`. Expected: all pass; the `interop`/`capture_fixtures` Docker tests are `#[ignore]`d and skipped.
- [ ] **Step 4: Final commit** (if fmt/clippy produced changes), using the identity override.

---

## Self-review (completed by plan author)

**Spec coverage** — every slice-1 requirement maps to a task:
- New crate + binary → Task 1, 19. `_schemas` auto-create → Task 9. Group-less StoreReader on client-core → Task 11. Primary writer + read-your-writes → Task 10, 12. In-mem store + id/version/dedup → Task 8. Three formats parse + canonical form → Tasks 5–7. `_schemas` record byte-exactness → Task 4 (+ fixtures Task 2). REST surface + content-types + error codes → Tasks 3, 13–18. `/config` stored-not-enforced → Task 16, 8. Compatibility=NONE → Task 17 + `format::parse` validation. Confluent-exact validation → Tasks 2, 21. Single-node always-primary → Task 12 (write gate, no election). CI/codecov per-crate integration flag → Task 22. fmt/clippy gates → Task 23.
- Out-of-scope items (compat enforcement, deletes, mode writes, references, HA/election, REST auth) appear in **no** task — correct.

**Placeholder scan** — the byte-exact tasks (4, 14–16, 21) intentionally defer field-order/escaping to the captured fixtures rather than guessing; each names the exact fixture file as the gate. This is a deliberate empirical-pinning strategy (per the spec + CLAUDE.md "check the real image"), not an unspecified placeholder. The `decode()` / `encode_schema` / `mirror_from` helpers are named with their signatures and behaviour described; implementers must write them to pass the stated tests.

**Type consistency** — `AppState { store: Arc<KafkaStore> }`, `KafkaStore::{start, register, set_*_compat, for_tests}`, `StoreState::{register, apply_schema, find_under_subject, version, versions, subjects, schema_by_id, set_*_compat, *_compat, mirror_from}`, `SchemaType::{wire_name, from_wire}`, `SrError` variants + `{error_code, http_status}`, `record::encode_schema`, `SchemaRecord::decode`, `reader::spawn -> StoreReader { store, applied_rx }` are used consistently across Tasks 8–20. `RegistryConfig` fields are identical in Tasks 1, 9–12, 19, 20.

**Known follow-ups recorded** (not slice-1 blockers): Protobuf canonical form is descriptor-bytes (not Confluent-exact) — slice 2/4; per-write store clone in `register()` — revisited at slice 5 election; the `for_tests` KafkaStore ctor is test-only scaffolding.
