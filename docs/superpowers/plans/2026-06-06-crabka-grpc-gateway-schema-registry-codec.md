# Gateway — Schema Registry codec (full structured) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Tasks run in parallel batches (disjoint file sets); steps use `- [ ]`.

**Goal:** Implement the gateway's deferred `SchemaRegistryCodec` (design §5 + Deferred): Confluent-wire-framed, schema-validated produce/consume against the landed Schema Registry, with a structured-value proto path and webhook tie-in — JVM-serde byte-interop.

**Architecture:** The codec seam (`RecordCodec`) becomes **async + fallible**. `RawCodec` stays the default (identity); `SchemaRegistryCodec` is opt-in via `--schema-registry-url`. The gateway talks to the registry over its **Confluent REST API** (the registry is a binary-only crate) via a small `SchemaRegistryClient` (reqwest + cache), fetches schema *strings*, and does payload (de)serialization itself with `apache-avro` / `prost-reflect`+`protox` / `jsonschema`. Confluent framing `[0x00][4-byte BE id][payload]` (+ Protobuf message-index varints). Subjects via `TopicNameStrategy` (`<topic>-value`/`-key`). Front-ends (`produce`/`consume`/webhook) are unchanged except for the now-`.await?`-ed codec calls.

**Tech Stack:** `async-trait`, `reqwest` (already a dep), `apache-avro`, `prost-reflect` + `protox`, `jsonschema`, `dashmap`, the Connect/prost proto build.

**Stacked on:** P9 (`claude/gateway-p9`). Branch `claude/gateway-codec`.

---

## Design

### Codec seam (async + fallible)
```rust
#[async_trait::async_trait]
pub trait RecordCodec: Send + Sync + 'static {
    /// Encode a record's value to the wire (Confluent framing when schema-bound).
    async fn encode(&self, topic: &str, body: EncodeBody<'_>) -> Result<Bytes, CodecError>;
    /// Decode a wire value: strip framing, return payload + schema metadata + an
    /// optional structured (JSON) view.
    async fn decode(&self, topic: &str, value: Bytes) -> Result<Decoded, CodecError>;
}
pub enum EncodeBody<'a> {
    Raw(Bytes),                                   // already-serialized bytes (frame if a schema is bound)
    Structured { json: &'a [u8], schema: SchemaSelector }, // gateway serializes JSON → format
}
pub struct Decoded { pub value: Bytes, pub schema: Option<SchemaMeta>, pub json: Option<Bytes> }
pub struct SchemaSelector { pub subject: Option<String>, pub id: Option<i32>, pub format: SchemaFormat } // None subject ⇒ TopicNameStrategy
pub struct SchemaMeta { pub subject: String, pub id: i32, pub format: SchemaFormat }
pub enum SchemaFormat { Avro, Json, Protobuf }
pub enum CodecError { Registry(String), Serialize(String), Validate(String), Framing(String) }
```
`RawCodec`: `encode` returns the raw bytes (ignores schema), `decode` returns `Decoded{value, None, None}`. The produce path (`produce.rs:149`) and consume path (`consume.rs:55`) `.await?` the codec; map `CodecError` → `GatewayError` (a new `GatewayError::Codec`).

### Proto additions (`crates/grpc-gateway/proto/crabka/gateway/v1/gateway.proto`)
- `enum SchemaFormat { SCHEMA_FORMAT_UNSPECIFIED=0; AVRO=1; JSON=2; PROTOBUF=3; }`
- `message SchemaSelector { string subject = 1; int32 id = 2; SchemaFormat format = 3; }`
- `message StructuredValue { bytes json = 1; }` (the client's value as JSON; the gateway serializes it into the target format)
- `Record`: replace `bytes value = 3` with `oneof body { bytes raw = 3; StructuredValue structured = 8; }` + add `optional SchemaSelector schema = 9;`
- `Inbound`: add `optional StructuredValue structured = 8;` + `optional SchemaSelector schema = 9;` (decoded view + schema meta).
Regenerate `pb` via `build.rs`. The pb→`GatewayRecord` mapping (handlers.rs) becomes: `raw` ⇒ `EncodeBody::Raw`; `structured`+`schema` ⇒ `EncodeBody::Structured`. Keep internal `GatewayRecord.value: Bytes` (the codec yields the final bytes); thread the `EncodeBody`/selector through to `produce_local`.

### `SchemaRegistryClient` (`crates/grpc-gateway/src/schema/client.rs`)
reqwest client over the Confluent REST API + caches:
```rust
pub struct SchemaRegistryClient { http: reqwest::Client, base: Url,
    by_id: DashMap<i32, CachedSchema>, by_subject_latest: DashMap<String, i32>, /* + ttl */ }
impl SchemaRegistryClient {
    async fn register(&self, subject: &str, schema: &str, fmt: SchemaFormat) -> Result<i32>;       // POST /subjects/{s}/versions -> {id}
    async fn schema_by_id(&self, id: i32) -> Result<(String, SchemaFormat)>;                        // GET /schemas/ids/{id}
    async fn latest(&self, subject: &str) -> Result<(i32, String, SchemaFormat)>;                   // GET /subjects/{s}/versions/latest
}
```
Cache `by_id` permanently (ids are immutable); cache `by_subject_latest` with a short TTL (latest can change). `schemaType` JSON field: `"AVRO"`/`"JSON"`/`"PROTOBUF"` (default AVRO when absent).

### Confluent framing (`crates/grpc-gateway/src/schema/wire.rs`)
- `encode_frame(id: i32, fmt, payload) -> Bytes`: `[0x00][id as i32 BE]` then, for Protobuf, the message-index varints (`[0]` = single/first message ⇒ a single `0` varint, i.e. one zero byte; general: length-prefixed varint array), then `payload`.
- `decode_frame(bytes) -> Result<(i32 /*id*/, &[u8] /*payload*/)>`: assert magic `0x00`, read 4-byte BE id, (for Protobuf strip the msg-index varints), return payload. Format is known from the id's `schemaType` (fetched), so framing decode returns id+rest and the caller strips proto varints per format.

### Per-format payload codecs (`crates/grpc-gateway/src/schema/format/{avro,json,protobuf}.rs`)
Each provides `serialize(schema_str, json: &[u8]) -> Result<Bytes>` (JSON → format binary) and `deserialize(schema_str, payload: &[u8]) -> Result<Bytes /*json*/>` + `validate`:
- **Avro** (`apache-avro`): parse `Schema::parse_str`; `serialize` = JSON→`avro::types::Value` (via `from_value`/`apache_avro::to_value` against the schema) → `to_avro_datum`; `deserialize` = `from_avro_datum` → JSON.
- **JSON Schema** (`jsonschema`): `serialize`/`validate` = compile schema, validate the JSON, pass through (JSON-on-the-wire is the canonical bytes); `deserialize` = validate + passthrough.
- **Protobuf** (`protox` + `prost-reflect`): compile the `.proto` schema string to a `FileDescriptor`/`MessageDescriptor`; `serialize` = JSON→`DynamicMessage` (serde) → `encode_to_vec`; `deserialize` = decode `DynamicMessage` → JSON. Handle the message-index (first message default).
Raw-bytes path: when `EncodeBody::Raw` + a schema is bound, frame the bytes as-is (assume the client pre-serialized correctly); `decode` of `Raw`-origin just strips the frame.

### `SchemaRegistryCodec` (`crates/grpc-gateway/src/schema/codec.rs`)
Implements `RecordCodec`:
- `encode(topic, Raw(bytes))`: if a subject schema is bound (config: frame-raw on/off) look up latest id for `<topic>-value` + frame; else passthrough. (Default: passthrough raw unless `structured`.)
- `encode(topic, Structured{json, selector})`: resolve subject (`selector.subject` or `<topic>-value`), resolve schema id+string (`selector.id`→`schema_by_id`, else `latest(subject)`, else `register`), `format::serialize(schema, json)`, `wire::encode_frame(id, fmt, payload)`.
- `decode(topic, value)`: `wire::decode_frame` → (id, payload); `client.schema_by_id(id)` → (schema, fmt); `format::deserialize(schema, payload)` → json; return `Decoded{ value, SchemaMeta{subject?, id, fmt}, json }`.

### Webhook tie-in (`webhook.rs` inbound, `outbound.rs` outbound)
- **Inbound**: a webhook endpoint may declare a JSON-Schema subject; when set, validate the request JSON against `<subject>`'s latest JSON schema (via the codec's json format) before producing; reject (400) on invalid.
- **Outbound**: when delivering, if the record value is Confluent-framed, `codec.decode` → JSON and deliver the JSON (so HTTP receivers get JSON, not framed Avro/Proto). Config flag per subscription (`decode_to_json`).

### Wiring (`config.rs`, `bin/gateway.rs`)
- `--schema-registry-url` (`CRABKA_GATEWAY_SCHEMA_REGISTRY_URL`, `Option<String>`). When set, build `SchemaRegistryClient` + `SchemaRegistryCodec` and inject into `ProduceCore::new(..)` and `ConsumeSession` (which must gain external codec injection). When unset, `RawCodec` (unchanged behavior).
- `GatewayConfig` gains `schema_registry_url: Option<String>`.

### Error handling / greenfield
- `GatewayError::Codec(CodecError)` → per-record `RecordResult.error` (retriable=false for validate/serialize; retriable=true for registry transport errors). No compat shims; opt-in by URL.

---

## Batches

- **Batch A (parallel — disjoint):** T1 proto+pb+handlers mapping ∥ T2 codec seam (async+fallible) ∥ T3 `SchemaRegistryClient`.
- **Batch B (parallel — disjoint per-format modules):** T4 `wire.rs` framing ∥ T5 `format/avro.rs` ∥ T6 `format/json.rs` ∥ T7 `format/protobuf.rs`.
- **Batch C (parallel — disjoint):** T8 `SchemaRegistryCodec` (needs T2/T3/T4/T5/T6/T7) ∥ T9 webhook tie-in (needs T6 json + the codec seam).
- **Batch D:** T10 gateway wiring (config + inject; needs T8) ∥ T11 tests (needs all). Then final review + PR.

---

## Task 1: Proto additions + pb→GatewayRecord mapping
**Files:** `crates/grpc-gateway/proto/crabka/gateway/v1/gateway.proto`, `crates/grpc-gateway/src/handlers.rs` (+ `types.rs` if `GatewayRecord` grows an `EncodeBody`/selector). Read `build.rs` (connectrpc/prost).
- [ ] Add `SchemaFormat` enum, `SchemaSelector`, `StructuredValue`; make `Record.value` a `oneof body { bytes raw = 3; StructuredValue structured = 8; }` + `optional SchemaSelector schema = 9`; add `Inbound.structured`/`schema`. Regenerate pb (`cargo build` runs build.rs).
- [ ] Thread the body/selector into `GatewayRecord` (add `body: EncodeBody`-equivalent + `schema: Option<SchemaSelector>`; keep `value: Bytes` as the produced bytes). Update `handlers::send`'s pb→GatewayRecord mapping (raw ⇒ Raw; structured+schema ⇒ Structured). The `EncodeBody`/`SchemaSelector`/`SchemaFormat` types live in `codec.rs` (Task 2) — coordinate: Task 2 defines them; Task 1 maps to them (Task 1 may stub them as a temporary local enum if Task 2 lands second, but prefer Task 2's types — these two tasks share `codec.rs`'s new types, so if they conflict, sequence T2 before T1's mapping step).
- [ ] Gates + commit `feat(gateway): structured-value proto (oneof raw/structured + schema selector)`.

> NOTE for the controller: T1 (proto/handlers) and T2 (codec seam) both reference the new `EncodeBody`/`SchemaSelector`/`SchemaFormat` types. To keep file sets disjoint, **Task 2 owns the type definitions in `codec.rs`**; Task 1 imports them. If parallel dispatch risks a missing-type compile, run T2 first, then T1+T3 — OR have T1 only touch the `.proto` + build (not the handlers mapping), deferring the mapping to a later task. Decide at dispatch time based on the type ownership.

## Task 2: Codec seam → async + fallible
**Files:** `crates/grpc-gateway/src/codec.rs`, `produce.rs`, `consume.rs`, `error.rs` (+ `Cargo.toml` for `async-trait`). 
- [ ] Define `RecordCodec` (async_trait), `EncodeBody`, `Decoded`, `SchemaSelector`, `SchemaMeta`, `SchemaFormat`, `CodecError` per Design. `RawCodec` impl (raw passthrough). `GatewayError::Codec(CodecError)` in `error.rs` (map to a per-record retriable/non-retriable error).
- [ ] Update `produce_local` (produce.rs:149) to `self.codec.encode(&rec.topic, rec.body()).await?` and `consume.rs:55` to `self.codec.decode(...).await?`. Keep `RawCodec` as the injected default everywhere (`gateway.rs`, tests, `new_for_test`).
- [ ] Gates + commit `refactor(gateway): async + fallible RecordCodec seam`.

## Task 3: `SchemaRegistryClient`
**Files:** create `crates/grpc-gateway/src/schema/mod.rs` + `schema/client.rs`; `lib.rs` (`pub mod schema;`); `Cargo.toml` (`dashmap` already a dep; add `url`).
- [ ] `SchemaRegistryClient` (reqwest + `DashMap` caches) with `register`/`schema_by_id`/`latest` per Design. Parse the Confluent JSON shapes (`{"id":N}`, `{"schema":..,"schemaType":..}`). Map HTTP/transport errors → a `RegistryError` (retriable) and 4xx → non-retriable.
- [ ] Unit tests via `wiremock`/`tower` mock OR a `#[cfg(test)]` against canned JSON (assert URL + parse + cache hit). Gates + commit `feat(gateway): SchemaRegistryClient (Confluent REST + cache)`.

## Task 4: Confluent wire framing
**Files:** create `crates/grpc-gateway/src/schema/wire.rs`.
- [ ] `encode_frame(id, fmt, payload)` + `decode_frame(bytes) -> (id, payload)` per Design (magic `0x00`, 4-byte BE id, Protobuf message-index varints: write a single `0` varint for the first/only message; decode reads the varint count + skips). Unit tests: round-trip per format; reject bad magic / short buffer. Commit `feat(gateway): Confluent wire framing`.

## Task 5/6/7: Per-format payload codecs (Avro / JSON / Protobuf)
**Files:** create `schema/format/mod.rs` + `schema/format/{avro,json,protobuf}.rs`; `Cargo.toml` (`apache-avro`, `jsonschema`, `protox`, `prost-reflect` — check workspace pins; reuse the registry's versions). Each format module is a separate task (parallel).
- [ ] **Avro (T5)**: `serialize(schema, json)` (JSON→Avro datum), `deserialize(schema, payload)` (Avro datum→JSON), `validate`. apache-avro `Schema::parse_str` + `to_avro_datum`/`from_avro_datum`. Tests round-trip a record. Commit.
- [ ] **JSON (T6)**: compile schema (`jsonschema`), `validate(schema, json)`; serialize/deserialize = validate + passthrough (JSON is the wire payload). Tests valid/invalid. Commit.
- [ ] **Protobuf (T7)**: compile `.proto` string (`protox::compile`/parse → `FileDescriptor` via `prost-reflect`), `DynamicMessage` JSON↔bytes (`serde` feature), handle the first-message default. Tests round-trip. Commit. (Confirm protox can compile a single in-memory `.proto` string.)
- [ ] `format/mod.rs`: `serialize(fmt, schema, json)`/`deserialize(fmt, schema, payload)`/`validate(...)` dispatch.

## Task 8: `SchemaRegistryCodec`
**Files:** create `crates/grpc-gateway/src/schema/codec.rs`. Needs T2 (seam), T3 (client), T4 (wire), T5–T7 (formats).
- [ ] Implement `RecordCodec` for `SchemaRegistryCodec { client, subject_strategy, frame_raw: bool }` per Design (encode raw/structured; decode strip-frame + deserialize). TopicNameStrategy subject. Unit tests with a mocked client (trait-object the client or inject canned schemas). Gates + commit.

## Task 9: Webhook tie-in
**Files:** `webhook.rs`, `webhook_config.rs`, `outbound.rs`, `outbound_config.rs`.
- [ ] Inbound: optional `validate_json_schema_subject` per webhook → validate request JSON against the subject's latest JSON schema before produce (400 on invalid). Outbound: optional `decode_to_json` per subscription → `codec.decode` framed values to JSON before delivery. Gates + commit.

## Task 10: Gateway wiring
**Files:** `config.rs`, `bin/gateway.rs`, `consume.rs` (external codec injection), `state.rs` if needed.
- [ ] `--schema-registry-url` arg + `GatewayConfig.schema_registry_url`. In `main`, when set, build `SchemaRegistryClient` + `SchemaRegistryCodec` (Arc<dyn RecordCodec>) and inject into `ProduceCore::new` + `ConsumeSession::new` (add the codec param to `ConsumeSession::new`, default `RawCodec` when unset). Gates + commit.

## Task 11: Tests
**Files:** `crates/grpc-gateway/tests/schema_codec.rs`.
- [ ] Integration: start the registry in-process (mirror `crates/schema-registry/tests/integration.rs`'s `KafkaStore::start` + axum router, served on a local port via `tokio` `axum::serve`), register Avro/JSON/Proto schemas, drive `handlers::send` with structured values through `SchemaRegistryCodec`, assert the produced bytes are Confluent-framed (magic+id) + a `decode` round-trips. A JVM-serde cross-check is out of scope (note it). Gates + commit.

## Final review + finish
Adversarial review: (1) **wire byte-exactness** vs Confluent (magic 0x00, 4-byte BE id, Protobuf msg-index varints) — a JVM Confluent deserializer must read it; (2) per-format round-trip correctness; (3) **opt-in** — no `--schema-registry-url` ⇒ `RawCodec`, zero behavior change; (4) error mapping (registry transport = retriable, validate/serialize = non-retriable); (5) caching correctness (immutable id cache, TTL'd latest); (6) dedup unaffected (operates on post-encode bytes); (7) the registry is NOT modified (gateway → registry over REST only); (8) subject TopicNameStrategy. Then push + PR stacked on #422.

## Self-review notes (author)
- **Decoupling:** gateway depends on `apache-avro`/`prost-reflect`/`protox`/`jsonschema` directly + the registry REST API; it does NOT link the registry crate (binary-only). The only thing fetched is the schema *string*.
- **Greenfield:** proto `value` becomes a `oneof` (raw=3 preserves the field number); opt-in via URL; no compat shims.
- **Scale:** 11 tasks / 4 batches — the format engines (Avro/JSON/Proto) are the bulk and run in parallel. Realistically the largest gateway slice; consider it the codec sub-program's single PR.
- **Deferred within this slice:** JVM-serde end-to-end cross-check (needs Docker + a JVM producer/consumer with a Confluent deserializer) — noted as a follow-up; the byte-framing is unit-asserted instead.
