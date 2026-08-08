# Schema Registry golden fixtures

These files are **byte-exact golden fixtures**. They were captured from a real
[`mirror.gcr.io/confluentinc/cp-schema-registry`](https://hub.docker.com/r/confluentinc/cp-schema-registry)
that ran against an in-process Crabka broker. They are the oracle for the Crabka
Schema Registry implementation. Do **not** hand-edit them.

## Provenance

- **Image:** `mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0`
- **Broker:** in-process `crabka-broker`. It listens on `0.0.0.0:9092` and
  advertises `host.docker.internal:9092`. The container reaches it with
  `--add-host=host.docker.internal:host-gateway`.
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

The capture registers AVRO with `schemaType` omitted. This exercises the SR
default.

## Files

### REST responses (verbatim response bodies)

- `rest_register_avro.json`, `rest_register_protobuf.json`, `rest_register_json.json`
  hold the `POST /subjects/{subject}/versions` responses, which carry the
  assigned schema id.
- `rest_get_version_avro.json`, `rest_get_version_protobuf.json`, `rest_get_version_json.json`
  hold the `GET /subjects/{subject}/versions/1` responses.
- `rest_get_by_id_avro.json`, `rest_get_by_id_protobuf.json`, `rest_get_by_id_json.json`
  hold the `GET /schemas/ids/{id}` responses.
- `rest_list_subjects.json` holds the `GET /subjects` response.
- `rest_get_config.json` holds the `GET /config` response.
- `rest_schema_types.json` holds the `GET /schemas/types` response.

### REST errors (status + raw body)

Each file wraps the error as
`{"_http_status": N, "_body": "<raw response string>"}`:

- `rest_err_subject_not_found.json` holds the response to
  `GET /subjects/does-not-exist/versions/1`. Expect `error_code` 40401.
- `rest_err_invalid_schema.json` holds the response to
  `POST /subjects/bad-value/versions` with a malformed AVRO body. Expect
  `error_code` 42201 or similar.

### Raw `_schemas` Kafka log records

There is one file per record in `_schemas` partition 0: `schemas_record_0.json`,
`schemas_record_1.json`, and so on. The harness captured them directly off the
Crabka broker, **in offset order**. Each file is `{"key": <utf8 of key bytes or
null>, "value": <utf8 of value bytes or null>}`. The embedded key and value
strings are the verbatim JSON that cp-schema-registry wrote to the log. These
files drive the kafkastore record encode/decode validation.

Observed `_schemas` layout (offsets 0..=4):

| Offset | keytype  | Notes |
| ------ | -------- | ----- |
| 0, 1   | `NOOP`   | SR writes these leader-election bootstrap noops on startup. `value` is `null`. These are **not** CONFIG records. |
| 2      | `SCHEMA` | `av-value` v1, id 1. The value **omits** `schemaType`, which is the AVRO default. |
| 3      | `SCHEMA` | `pb-value` v1, id 2. The value carries `"schemaType":"PROTOBUF"`. |
| 4      | `SCHEMA` | `js-value` v1, id 3. The value carries `"schemaType":"JSON"`. |

The log holds **no `CONFIG` record**. `GET /config` reports
`{"compatibilityLevel":"BACKWARD"}` as the *global default*. SR writes a
`CONFIG` record only when you set a compatibility level explicitly, and this
capture never sets one.

## Determinism caveats (read before treating values as byte-exact)

- **`_schemas` SCHEMA `key`** is stable. The field order is always
  `keytype, subject, version, magic`.
- **`_schemas` SCHEMA `value` field order is NOT stable across SR runs.** The
  AVRO record has no `schemaType` and was stable across captures. But the value
  JSON of the PROTOBUF and JSON records moved `schemaType` relative to `schema`
  and `deleted` between two back-to-back captures. Jackson does not pin the
  field order for the `SchemaValue` POJO. The committed fixtures show one
  observed order. **Task 4 must compare these record values
  order-insensitively.** Parse the values and compare them instead of comparing
  bytes, or normalise the field order before you diff them.
- The verbatim **REST** response bodies in `rest_*.json` were stable across both
  captures. You can treat them as byte-exact.
- The `schemas_record_*` files are pretty-printed wrappers. The **inner** key
  and value strings are the verbatim bytes. We added the wrapper formatting. It
  does not come from SR.
