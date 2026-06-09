# Schema-aware payloads for client-streams (Avro / Protobuf / JSON)

Date: 2026-06-08
Status: Approved design — ready for implementation planning

## Goal

Let `crabka-client-streams` topologies read and write **schema-framed**
payloads in Avro, Protobuf, and JSON, with the schemas **registered and
validated against a Confluent-compatible Schema Registry** (the existing
`crabka-schema-registry` server). Ship runnable examples per format.

The hard constraint is **Confluent wire-format byte-exactness** (magic byte
`0x00` + 4-byte big-endian schema id, plus the Protobuf message-index prefix),
so that data produced/consumed by Crabka interoperates with the JVM Confluent
serializers.

## Non-goals (YAGNI)

- No dynamic/value-based payloads (Avro `Value`, `DynamicMessage`,
  `serde_json::Value`). Typed (derive/codegen) only.
- No subject strategies beyond `TopicNameStrategy` (the trait seam is added so
  Record/TopicRecord strategies can land later without API churn).
- No producer/consumer integration in this slice. The core crate is built to be
  reusable by `client-producer`/`client-consumer` later, but only the Streams
  bridge is wired now.
- No backwards-compatibility shims (Crabka is greenfield/undeployed).

## Crate layout & boundaries

### New crate: `crabka-schema-serde` (client-agnostic core)

Per-format Cargo features `avro` / `protobuf` / `json` so users pull only what
they need. No dependency on `crabka-client-streams`.

```
crates/schema-serde/
  src/
    lib.rs
    error.rs            # SchemaSerdeError (incl. a retriable variant)
    registry/
      mod.rs            # async REST client (reqwest)
      model.rs          # request/response DTOs
    cache.rs            # SchemaCache (Arc-shared, background refresh)
    wire.rs             # Confluent framing: magic + id (+ pb message-index)
    subject.rs          # SubjectStrategy trait + TopicNameStrategy
    format/
      mod.rs            # SchemaSerializer<T> / SchemaDeserializer<T> traits
      avro.rs           # feature = "avro"
      protobuf.rs       # feature = "protobuf"
      json.rs           # feature = "json"
```

**Registry client** (`reqwest`, async, rustls). Endpoints:

- `POST /subjects/{subject}/versions` — register, returns `{ id }`.
- `POST /subjects/{subject}` — lookup id of an exact schema (lookup-only mode).
- `GET /schemas/ids/{id}` — fetch a schema by global id (deserialize path).
- `GET /subjects/{subject}/versions/latest` — use-latest mode.

Content type `application/vnd.schemaregistry.v1+json`. Schema payloads carry
`schemaType` (`AVRO` default/omitted, `PROTOBUF`, `JSON`) and `references` (empty
in this slice).

**`SchemaCache`** — the central object:

- `Arc`-shared; holds the registry client + a `CacheConfig`
  (`auto_register: bool` defaulting to `true`; subject strategy).
- Maps: `(subject, canonical_schema) → id` and `id → ParsedWriterSchema`.
- A list of **interned subjects** (subject + the local type's schema), populated
  as serdes are constructed.
- `prewarm(&self) -> Result<(), SchemaSerdeError>`: async; resolves every
  interned subject — auto-registers (default) or looks up — and fills the cache.
- A background refresh task for `id → schema` misses on the deserialize path.
- Hot-path reads (`id_for(subject)`, `schema_for(id)`) are **synchronous**
  cache lookups.

**Wire framing** (`wire.rs`), format-agnostic:

- `encode(id, message_index, body) -> Bytes`: `0x00` ‖ `id:4 BE` ‖
  `message_index?` ‖ `body`.
- `decode(bytes) -> (id, message_index?, body)`.
- Protobuf message-index: a length-prefixed varint array; Confluent optimizes
  the common top-level case `[0]` to a single `0x00` byte. Both encode and
  decode handle this special case.

### `crabka-client-streams` — optional feature `schema-serde`

- New optional dependency on `crabka-schema-serde`.
- New module `processor::serde::schema` (gated by the feature) holding the
  **`Serde<T>` bridge impls**. The impls live here (not in the core crate) so the
  local `Serde<T>` trait is not orphan-implemented.
- Each bridge serde is constructed **bound to its subject** (topic + key/value
  role) and an `Arc<SchemaCache>`; on construction it interns
  `(subject, schema)` into the cache for pre-warm.
- Runnable examples under `crates/client-streams/examples/` (feature-gated).

## Per-format type model (derive / reflection)

| Format    | Rust type                                              | Schema source                                  | Wire body                                                     |
|-----------|-------------------------------------------------------|------------------------------------------------|--------------------------------------------------------------|
| Avro      | `apache-avro` derive (`AvroSchema + Serialize + Deserialize`) | `AvroSchema::get_schema()` → JSON               | avro binary; deserialize resolves **writer→reader** schema   |
| Protobuf  | `prost` struct + `prost-reflect::ReflectMessage`      | descriptor → normalized `.proto` text          | **message-index** (`[0]`→single `0x00`) + protobuf payload   |
| JSON      | serde struct + `schemars::JsonSchema`                 | `schemars` → JSON Schema text                  | UTF-8 JSON; optional payload validation vs writer schema     |

Notes:

- **Avro deserialize** uses `from_avro_datum(writer_schema, reader_schema)` so
  schema evolution resolves correctly, then `apache_avro::from_value::<T>`.
- **Protobuf** registration normalizes the `FileDescriptorProto` to `.proto`
  text. Where practical, reuse the normalization conventions already in
  `crabka-schema-registry::format::protobuf` to match cp output.
- **JSON** validation against the fetched writer schema is opt-in (the
  `jsonschema` crate); decode is `serde_json::from_slice::<T>`.

## Runtime flow

1. **Construction**: `AvroValueSerde::<Order>::new(&cache, "orders")` derives the
   subject `orders-value` (via `SubjectStrategy`), computes the type's schema,
   and interns `(subject, schema)` in the shared `SchemaCache`. A `_key` variant
   produces `orders-key`.
2. **Pre-warm**: at `StreamsMembership` start, the runtime calls
   `cache.prewarm().await`. Auto-register mode registers each interned subject
   and caches the returned id; lookup-only mode resolves the id (or latest).
   After pre-warm, all locally-produced subjects have ids cached.
3. **Serialize (hot path, sync)**: read `id` from cache for the bound subject,
   encode the typed value, frame with `wire::encode`. (Pre-warm guarantees a hit
   for produced subjects.)
4. **Deserialize (hot path, sync)**: `wire::decode` → `id`; look up the writer
   schema by id in cache; decode into `T`.
5. **Unknown writer id**: cache miss on deserialize triggers a background fetch
   and returns a **retriable `SerdeError`** until the cache populates — never
   blocks a runtime worker thread.

## Subject strategy

`SubjectStrategy` trait with a single shipped impl `TopicNameStrategy`
(`<topic>-key` / `<topic>-value`). Default-only; the trait exists so other
strategies can be added without changing serde constructors.

## Examples (`crates/client-streams/examples/`, `--features schema-serde`)

Three runnable programs, `no_run`-style for CI (need a live broker + registry):

- `avro_pipeline.rs`
- `protobuf_pipeline.rs`
- `json_pipeline.rs`

Each builds a small topology — read a schema-framed topic, transform, write back
to a schema-framed output topic — using the bridge serdes through
`Consumed::with` / `Produced::with`. Plus a README section per format and
per-type doctests on the serde constructors.

## Testing (round-trip + golden cp bytes)

- **Unit round-trips** through each `SchemaSerializer`/`SchemaDeserializer`.
- **Framing assertions**: magic `0x00`, 4-byte BE id; Protobuf message-index
  (incl. the `[0]`→single-byte special case).
- **Registry client** tested against the in-workspace `crabka-schema-registry`
  server spun up in-process (broker test-helpers already available as a
  dev-dependency).
- **Golden cp bytes**: payloads captured from Confluent's JVM serializers
  (Avro/Protobuf/JSON) checked into `testdata/`, asserted byte-exact. Golden
  refresh is a separate scripted step (no Docker in CI), mirroring the existing
  `tests/jvm-capture/` pattern.

## Open items to verify against cp during implementation

1. **JSON Schema draft**: `schemars` emits draft 2020-12 / draft-07 depending on
   version; Confluent's JSON Schema serde expects a specific draft. Pin/configure
   `schemars` to match, and verify against a cp golden.
2. **Protobuf message-index** byte layout for nested/non-top-level messages
   (top-level `[0]` is the single-byte fast path; confirm multi-element arrays).
3. **Protobuf `.proto` normalization** parity with cp registration output.

## Dependencies (already in the workspace)

`apache-avro = "0.21"`, `prost = "0.14"`, `prost-reflect = "0.16"`,
`protox-parse = "0.9"`, `reqwest = "0.13"`. New: `schemars` (JSON, feature-gated),
`jsonschema` (optional JSON payload validation).
