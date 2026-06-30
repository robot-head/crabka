# Schema Registry golden fixtures

These files are **byte-exact golden fixtures** captured from a real
[`mirror.gcr.io/confluentinc/cp-schema-registry`](https://hub.docker.com/r/confluentinc/cp-schema-registry)
running against an in-process Crabka broker. They are the oracle the Crabka
Schema Registry implementation is validated against — do **not** hand-edit them.

## Provenance

- **Image:** `mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0`
- **Broker:** in-process `crabka-broker`, listening on `0.0.0.0:9092`,
  advertising `host.docker.internal:9092` (container reaches it via
  `--add-host=host.docker.internal:host-gateway`).
- **Captured:** 2026-06-05
- **Harness:** `crates/schema-registry/tests/capture_fixtures.rs`
  (`#[ignore]`). Regenerate with:

  ```text
  cargo test -p crabka-schema-registry --test capture_fixtures -- --ignored --nocapture
  ```

## Schemas registered

| Subject    | Type     | Schema |
| ---------- | -------- | ------ |
| `av-value` | AVRO     | `{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}` |
| `pb-value` | PROTOBUF | `syntax = "proto3"; message User { int32 id = 1; }` |
| `js-value` | JSON     | `{"type":"object","properties":{"id":{"type":"integer"}}}` |

(AVRO is registered with `schemaType` omitted, exercising the SR default.)

## Files

### REST responses (verbatim response bodies)

- `rest_register_avro.json`, `rest_register_protobuf.json`, `rest_register_json.json`
  — `POST /subjects/{subject}/versions` responses (the assigned schema id).
- `rest_get_version_avro.json`, `rest_get_version_protobuf.json`, `rest_get_version_json.json`
  — `GET /subjects/{subject}/versions/1`.
- `rest_get_by_id_avro.json`, `rest_get_by_id_protobuf.json`, `rest_get_by_id_json.json`
  — `GET /schemas/ids/{id}`.
- `rest_list_subjects.json` — `GET /subjects`.
- `rest_get_config.json` — `GET /config`.
- `rest_schema_types.json` — `GET /schemas/types`.

### REST errors (status + raw body)

Wrapped as `{"_http_status": N, "_body": "<raw response string>"}`:

- `rest_err_subject_not_found.json` — `GET /subjects/does-not-exist/versions/1`
  (expect `error_code` 40401).
- `rest_err_invalid_schema.json` — `POST /subjects/bad-value/versions` with a
  malformed AVRO body (expect `error_code` 42201 / similar).

### Raw `_schemas` Kafka log records

`schemas_record_0.json`, `schemas_record_1.json`, … — one file per record in
`_schemas` partition 0, **in offset order**, captured directly off the Crabka
broker. Each is `{"key": <utf8 of key bytes or null>, "value": <utf8 of value
bytes or null>}`; the embedded key/value strings are the verbatim JSON
cp-schema-registry wrote to the log. These drive the kafkastore record
encode/decode validation.

Observed `_schemas` layout (offsets 0..=4):

| Offset | keytype  | Notes |
| ------ | -------- | ----- |
| 0, 1   | `NOOP`   | Leader-election bootstrap noops SR writes on startup; `value` is `null`. **Not** CONFIG records. |
| 2      | `SCHEMA` | `av-value` v1, id 1. Value **omits** `schemaType` (the AVRO default). |
| 3      | `SCHEMA` | `pb-value` v1, id 2. Value carries `"schemaType":"PROTOBUF"`. |
| 4      | `SCHEMA` | `js-value` v1, id 3. Value carries `"schemaType":"JSON"`. |

There is **no `CONFIG` record** in the log: `GET /config` reports
`{"compatibilityLevel":"BACKWARD"}` as the *global default*, but SR only writes
a `CONFIG` record when a compatibility level is explicitly set (which this
capture never does).

## Determinism caveats (read before treating values as byte-exact)

- **`_schemas` SCHEMA `key`** is stable: field order is always
  `keytype, subject, version, magic`.
- **`_schemas` SCHEMA `value` field order is NOT stable across SR runs.** The
  AVRO record (no `schemaType`) was stable across captures, but the
  PROTOBUF/JSON records' value JSON reordered `schemaType` relative to
  `schema`/`deleted` between two back-to-back captures (Jackson does not pin
  field order for the `SchemaValue` POJO). The committed fixtures show one
  observed ordering. **Task 4 must compare these record values
  order-insensitively** (parse-and-compare, not byte-compare), or normalise
  field order before diffing.
- The verbatim **REST** response bodies (`rest_*.json`) were stable across both
  captures and can be treated as byte-exact.
- `schemas_record_*` files are pretty-printed wrappers; the **inner** key/value
  strings are the verbatim bytes — the wrapper formatting is ours, not SR's.
