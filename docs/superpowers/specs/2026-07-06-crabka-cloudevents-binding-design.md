# CloudEvents Kafka + HTTP protocol binding (MSG-2) — design

**Date:** 2026-07-06
**Status:** Approved
**Type:** Subsystem design. The interop centerpiece of the [serverless messaging cycle](2026-07-06-crabka-gateway-header-carrythrough-design.md) — makes a Crabka topic a first-class CloudEvents stream. Only the **gRPC-Subscribe** CE path depends on MSG-1; ingress and webhook egress are independent.

## Context — the interop wedge, honestly

MSG-2 implements the **CNCF CloudEvents binding** at the gateway: parse/emit `ce-*` attributes in **binary** mode (headers) and **structured** mode (`application/cloudevents+json` in the body/value) on HTTP produce-in and webhook egress, transparently on gRPC. A CloudEvents-native gateway lets Crabka drop into a Knative event mesh with **no Kafka client and no proprietary envelope** — parity with what Confluent needs a separate HTTP-source/connector for, and coverage Supabase Realtime (Postgres-CDC-only) lacks.

Honest framing (from the roadmap): CloudEvents-over-HTTP is the **least differentiated** facet — a checkbox NATS/JetStream and Google Pub/Sub+Eventarc already tick; **never a standalone moat, pitch only combinatorially.** Its value is that the *same* topic on the *same* bucket is simultaneously a pub/sub channel, a KIP-932 work queue (MSG-4), a CDC stream, and an observability WAL — one substrate, many faces, zero connectors. MSG-2 competes as substrate-level eventing *infrastructure*, not as an end-user eventing product.

This slice relates to **MSG-1** narrowly. Binary-mode CloudEvents attributes *are* Kafka record headers. The ingress (produce-in) path writes `GatewayRecord.headers` (already present, `types.rs:23`) — independent of MSG-1. The **webhook egress** path reads the **native** `ConsumerRecord.headers` (`consumer.rs:117`, already populated by the consumer; `outbound.rs` operates on `ConsumerRecord` end-to-end) — also independent of MSG-1. Only the **gRPC-Subscribe** CE path depends on MSG-1, because it flows through the gateway's own `DecodedConsumerRecord` → `Inbound.headers` map, which MSG-1 restores (`streaming.rs:153` hardcodes an empty map today). So MSG-2 is almost entirely buildable now; only the gRPC-Subscribe transparency test is gated on MSG-1.

## The canonical in-Kafka representation (the pivot)

Both directions map through one shape — a CloudEvent stored in a Kafka record, in one of two content modes (per the CNCF Kafka Protocol Binding):

**Binary mode** (the default; what MSG-1's header carrier makes viable):
- Each context attribute → a Kafka record header keyed `ce_<name>` (**underscore**), value = the attribute's UTF-8 string bytes. Required: `ce_id`, `ce_source`, `ce_type`, `ce_specversion`; optional `ce_subject`, `ce_time`, any `ce_<ext>`.
- `datacontenttype` is **special**: it does **not** become `ce_datacontenttype` — it maps to the bare Kafka `content-type` header (no `ce_` prefix). Emitting a `ce_datacontenttype`/`ce-datacontenttype` header is forbidden by the binding.
- The record **value** is the event `data` verbatim (opaque bytes via the existing `EncodeBody::Raw` path). The key is unaffected by CE.

**Structured mode:**
- The **entire** CloudEvent JSON (`specversion`/`id`/`source`/`type`/`data`/…) is the record value; the only binding header is `content-type`. Egress emits the CNCF-recommended `application/cloudevents+json; charset=UTF-8` (with charset).
- **Byte-disjoint from Confluent framing:** a CE JSON envelope starts with `{` (`0x7B`); the Confluent frame requires `byte[0] == 0x00` (`schema-serde/wire.rs:74-75`). A CE-structured topic must **not** be schema-registry-bound — structured mode uses `EncodeBody::Raw` (value verbatim) only.

This shape is exactly what `GatewayRecord.headers: Vec<(String, Bytes)>` + `value: Bytes` already model on produce, and what a header-carrying consume record models once MSG-1 lands. **MSG-2 adds no new record type** — only the attribute↔header translation layer and mode selection around this pivot.

## Design Goals

- **CNCF-conformant binding:** binary + structured, on HTTP produce-in and webhook egress; transparent on gRPC.
- **Correct header naming:** HTTP `ce-*` (**hyphen**) ↔ Kafka `ce_*` (**underscore**), swapped **only** at HTTP boundaries; `datacontenttype` ↔ bare `content-type` (never prefixed).
- **Additive / opt-in:** non-CloudEvents traffic is byte-for-byte unchanged; egress defaults to today's Crabka envelope.
- **Byte-exact records:** the underlying Kafka record is unchanged; `ce_*` are ordinary Kafka headers.
- **Single source of truth:** one `ce_translate` module owns prefix translation, content-mode detection, and binary↔structured conversion — the one unit proving `ce-id (HTTP) == ce_id (Kafka)` round-trips.

## Non-goals

- **Batch mode** (`application/cloudevents-batch+json`) — detected and rejected `415` in MVP (array-of-events → N records is a larger surface).
- **Strict attribute validation** (URI-reference `source`, RFC3339 `time`) — MVP checks non-empty required attrs only.
- **Auto-populating required attributes on ingress** — a produce-in that omits them is an error, not a defaulted event (the gateway is a faithful binding, not an event author). The one exception is opt-in egress synthesis (below).
- **Any Kafka wire / KIP byte change**, per-attribute schema registry integration, or a new proto (`Record.headers`/`Inbound.headers` `map<string,bytes>` already carry `ce_*`).
- MSG-3 (per-offset ack), MSG-4 (scaler), MSG-5 (SDK).

## Key Design Decisions

### `ce_translate` — the single translation/detection module

A new `crates/grpc-gateway/src/ce_translate.rs` owns all CE logic so ingress and egress cannot disagree:

```rust
pub fn http_headers_to_kafka(h: &HeaderMap) -> Result<Vec<(String, Bytes)>, CeError>; // ce-<n> → ce_<n>, Content-Type → content-type
pub fn kafka_headers_to_http(h: &[(String, Option<Bytes>)]) -> Vec<(HeaderName, HeaderValue)>; // ce_<n> → ce-<n>
pub fn detect_content_mode(media_type: Option<&str>, has_ce_header: bool) -> IngressMode; // Binary | Structured | Batch | NotCloudEvent
pub fn validate_binary_required(h: &[(String, Bytes)]) -> Result<(), CeError>;
pub fn validate_structured_json(v: &serde_json::Value) -> Result<(), CeError>;
pub fn structured_from_binary(headers: &[(String, Option<Bytes>)], value: &[u8]) -> Vec<u8>; // → CE JSON (data | data_base64)
```

Its unit tests are the conformance source of truth: prefix round-trip, the `datacontenttype`→`content-type` exception, `detect_content_mode` classification, non-UTF-8 → error, structured↔binary.

### Content-mode detection is a prefix test, not equality

A message is **structured** iff its media type — lowercased, parameters stripped (everything from the first `;`), trimmed — **starts with** `application/cloudevents`. Check `application/cloudevents-batch` first (→ `415`), then `application/cloudevents` (→ structured). This deliberately is **not** an equality check against `application/cloudevents+json`, so `; charset=UTF-8`, bare `application/cloudevents`, and future `+`-suffix variants all classify correctly. Otherwise: **binary** iff any `ce-*` (ingress) / `ce_*` (record) header is present; else **not a CloudEvent**.

### Ingress — HTTP produce-in (the two `vec![]` seams)

Both `webhook.rs:198` (`webhook_handler`) and `webhook.rs:249` (`produce_handler`) build `GatewayRecord { headers: vec![], .. }` today. MSG-2 fills them:
- **Structured** (media type prefix `application/cloudevents`): body → `EncodeBody::Raw`, one Kafka `content-type` header = the received media type verbatim; validate required attrs by parsing the JSON.
- **Binary** (any `ce-*` header, non-CE media type): `http_headers_to_kafka` translates `ce-*` → `ce_*` and `Content-Type` → `content-type`; body stays the value.
- **Not a CloudEvent:** unchanged — `headers` stays `vec![]`, plain produce (strictly additive).

Existing webhook features run **unchanged and before** CE shaping: HMAC is still computed over the raw body; replay guard and `key_source`/`idempotency_source` extraction still work. Because `extract_source` reads the raw axum `HeaderMap` *before* translation (`webhook_config.rs:286`), a config that derives the key/idempotency from a CE attribute **must use the HTTP hyphen form** (`header:ce-id`), never `header:ce_id`. The spec pins this.

**gRPC Send** is transparent: a client sets `ce_*` (underscore) headers explicitly in `Record.headers`; they flow through `to_gateway_record` (`handlers.rs:153`) to Kafka verbatim. MSG-2's only optional addition is validation gated behind `validate_cloudevents_on_send` (default off), so raw passthrough stays the default.

### Egress — webhook, per-subscription `content_mode`

`CompiledSubscription` (`outbound_config.rs:94-117`) gains `content_mode: ContentMode { Envelope, CloudEventsBinary, CloudEventsStructured }` (default `Envelope`). At the request-construction seam (`outbound.rs:201-212`), which today unconditionally sets `content-type: application/json` (`:205`):
- **`Envelope`** (default): today's behavior verbatim, including the `application/json` line — no regression.
- **`CloudEventsBinary`**: **replace** (not append) the `application/json` line — set `Content-Type` from the record's `content-type` header; body = raw record value; `ce_*` → `ce-*` HTTP headers via `kafka_headers_to_http`. Keep the `X-Crabka-*` dedup/signature headers (not `ce-*`, no collision).
- **`CloudEventsStructured`**: **replace** with `Content-Type: application/cloudevents+json; charset=UTF-8`; body = a CE JSON document (verbatim if produced structured; else `structured_from_binary` rebuilds it from `ce_*` headers + value, emitting the CE-spec `data`/`data_base64`, a new sibling to `render_envelope`).

`compile()` (`outbound_config.rs:170-187`) rejects, under any CE mode, a static `headers` entry whose lowercased name is `content-type` or starts with `ce-` (covers both the collision with translated headers and double-setting Content-Type), and rejects `decode_to_json` + `CloudEventsBinary` (contradictory — binary sends raw data). These egress paths read the **native** `ConsumerRecord.headers` (`consumer.rs:117`), which the consumer already populates — **so webhook egress is independent of MSG-1** (the stale "`ConsumerRecord` exposes none" comment at `outbound.rs:284` notwithstanding).

### gRPC Subscribe — transparent (a note + a test, no code)

Once MSG-1 populates `Inbound.headers` from the record's `ce_*` headers, a gRPC subscriber receives the binary-mode CloudEvent with **zero MSG-2 code**: the `ce_*` keys, the `content-type` header, and the value/data all arrive in `Inbound`. No hyphen translation (gRPC is not HTTP; the wire form is Kafka's underscore). MSG-2's obligation is a subscribe-path test asserting a record produced with `ce_id/ce_source/ce_type/ce_specversion` surfaces those exact underscore keys. Structured mode is likewise transparent (the CE JSON is the value; the client keys off `content-type` with the same prefix test).

### Validation

CloudEvents 1.0 required attributes `id`/`source`/`type`/`specversion` are validated on ingress-CE: missing any → `400`; a `ce-*` header value that is not valid UTF-8 → `400` (closing the silent-drop gap); malformed structured JSON → `400`; `application/cloudevents-batch*` → `415`. `specversion == 1.0` is enforced as a **Crabka policy overlay** (the binding itself is version-agnostic) — strict-reject vs transparent-forward is an open product question (Risks). No auto-population on ingress; egress synthesis of missing attrs is opt-in per subscription (config `ce_source`/`ce_type`, `ce_id`=event_id, `ce_specversion`=1.0).

## Integration

- **`crates/grpc-gateway/src/ce_translate.rs`** (new) — the translation/detection/conversion module.
- **`crates/grpc-gateway/src/webhook.rs:198,249`** — ingress: fill the two `vec![]` seams.
- **`crates/grpc-gateway/src/outbound.rs:201-212,285-296`** + **`outbound_config.rs:30-190`** — egress: `ContentMode`, `compile()` guard, the branch + the structured serializer.
- **`crates/grpc-gateway/src/handlers.rs:130-162`** — gRPC Send (transparent; optional gated validation).
- **`crates/grpc-gateway/src/webhook_config.rs:280-298`** — `extract_source` reused for `ce-*` key/idempotency (hyphen form).
- **`crates/grpc-gateway/proto/…/gateway.proto:68,130`** — `Record.headers`/`Inbound.headers` already carry `ce_*`; **no proto change**.

## Kafka / wire compliance

- **No wire/KIP change** — `ce_*` are ordinary Kafka headers; the record is byte-exact. Structured mode is a plain JSON value.
- **Not Confluent framing** — the CE structured envelope (`{`…) is byte-disjoint from the Confluent frame (`0x00`…); a CE-structured topic is not schema-bound.
- **`datacontenttype` empirical check** — per CLAUDE.md ("when in doubt match Kafka"), verify against a real CloudEvents Kafka SDK / cp-kafka that interop peers emit bare `content-type` (not `ce_datacontenttype`) before finalizing.

## Testing

- **`ce_translate` units (the conformance suite):** `ce-id`↔`ce_id` round-trip; `datacontenttype`→`content-type` (and never `ce_datacontenttype`); `detect_content_mode` classifies `application/cloudevents+json`, `…; charset=UTF-8`, bare `application/cloudevents`, and `-batch`; non-UTF-8 `ce-*` → error; `structured_from_binary` emits `data`/`data_base64`.
- **Ingress:** binary CE post → record carries `ce_*` + `content-type`; structured post → value verbatim + single `content-type`, classified via prefix; missing `ce-id` → `400`; non-UTF-8 `ce-*` → `400`; non-CE post byte-identical to today; `ce-id` usable as `header:ce-id` idempotency source.
- **Egress:** `CloudEventsBinary` emits `ce-*` HTTP headers + raw body + `Content-Type` from the record (and **no** `application/json`); `CloudEventsStructured` emits `Content-Type: application/cloudevents+json; charset=UTF-8`; `Envelope` unchanged (regression); `compile()` rejects `ce-*`/`content-type` static headers and `decode_to_json`+`CloudEventsBinary`.
- **gRPC Subscribe:** a produced `ce_*` record surfaces those exact underscore keys in `Inbound.headers`.
- **End-to-end round-trip:** HTTP produce-in binary (`ce-*`) → Kafka record `ce_*` → webhook egress binary → delivered HTTP `ce-*` with identical values + record-derived `Content-Type`; plus a structured round-trip asserting the charset media type.

## Risks (carried into the plan)

- **MSG-1 prerequisite (narrow):** only the gRPC-Subscribe CE transparency test is gated on MSG-1's `Inbound.headers` restore. Ingress (produce-side `GatewayRecord.headers`) and webhook egress (native `ConsumerRecord.headers`) are independent and buildable now.
- **Egress signed-bytes per mode:** switching from `Envelope` changes the POST body *and* the `X-Crabka-Signature` surface (envelope JSON → raw value → CE JSON). Mitigated by the `Envelope` default; the exact signed-bytes definition per mode must be specified and tested.
- **Content-Type replacement is conformance-critical:** the `application/json` line at `outbound.rs:205` must be *replaced* (not appended) under CE modes, or reqwest delivers a duplicate/wrong `Content-Type` — verified in the builder mechanics, not just config.
- **`data`/`data_base64`:** the structured serializer must emit the CE-spec `data_base64` for non-JSON data, **not** Crabka's `{"_base64":…}` wrapper (`outbound.rs:300`).
- **`specversion` policy** (reject non-1.0 vs forward) and **batch mode** (415 vs support) are open product decisions.
- **Non-UTF-8 `ce_*` on egress:** a Kafka `ce_*` value that is not a valid HTTP header value needs a defined fallback (skip+log / DLQ) on `kafka_headers_to_http`.
- **Duplicate keys (symmetric):** proto3 `map<string,bytes>` on both `Record.headers` and `Inbound.headers` cannot carry duplicate `ce_*` keys — benign for single-valued CE 1.0 attributes, but stated for both gRPC directions.

## Resolved decisions (from grounding)

- **Pivot:** the two-mode in-Kafka representation; MSG-2 is a translation layer, no new record type, no proto change.
- **Naming:** HTTP `ce-` ↔ Kafka `ce_`, swapped only at HTTP boundaries; `datacontenttype`↔bare `content-type`.
- **Detection:** case-insensitive prefix test on `application/cloudevents` (params stripped); `-batch` → 415.
- **Ingress:** additive at `webhook.rs:198,249`; extraction before translate (hyphen key form); gRPC Send transparent.
- **Egress:** per-subscription `content_mode` (default `Envelope`); replace the `application/json` line under CE modes; structured serializer emits `data`/`data_base64`.
- **gRPC Subscribe:** transparent (MSG-1 prereq); note + test, no code.
- **Scope:** one spec (shared `ce_translate`); the plan is parallel-batched (ingress vs egress touch disjoint files).
