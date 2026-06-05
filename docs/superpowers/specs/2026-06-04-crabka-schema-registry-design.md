# Crabka Schema Registry — design

- **Date:** 2026-06-04
- **Status:** Approved (brainstorm); slice 1 ready for an implementation plan
- **Scope of this doc:** the whole-feature architecture + a detailed, implementable **slice 1**. Later slices are sketched in the roadmap and get their own specs.

## Motivation

Schema Registry is one of the last unchecked boxes in the README feature matrix
(`README.md` → `| Schema Registry | ❌ |`), and the operator roadmap lists a
registry equivalent as item 68 ("optional / debatable"). It is the standard way
Kafka users attach Avro / Protobuf / JSON Schema typing to otherwise-opaque
record bytes. Crabka's whole reason for existing is drop-in compatibility with
the Kafka ecosystem, so the registry we build is a **Confluent Schema
Registry-compatible** one: existing `KafkaAvroSerializer` /
`KafkaProtobufSerializer` / `KafkaJsonSchemaSerializer` and the SR REST clients
must work unmodified against it.

## Load-bearing decisions

These four choices (settled during brainstorming) frame everything below:

| Decision | Choice | Consequence |
|---|---|---|
| **Goal** | A Confluent Schema Registry-compatible **REST service**. The broker stays schema-agnostic. | No produce-path schema enforcement in the broker (that would be a separate "broker-side validation" feature, explicitly not in scope here). |
| **Deployment** | A **standalone** `crabka-schema-registry` crate + binary that is a Kafka **client** of Crabka. | Architecturally identical to Confluent SR. Near-zero broker changes — `_schemas` is just a compacted topic the broker serves. Could even front a real Kafka. |
| **Storage** | The **`_schemas` compacted topic** is the source of truth (Confluent's model). | Byte-shape exactness of `_schemas` records becomes a compatibility surface (interop with a real Confluent SR). |
| **Formats** | **Avro + Protobuf + JSON Schema** up front; compatibility checking starts shallow. | Slice 1 carries three parsers + three canonical-form implementations, but only `NONE` compatibility. |
| **Fidelity** | **Confluent-exact**: `_schemas` record format, REST JSON shapes, numeric error codes, content-types, id-assignment semantics. | Validated in CI against a **real Confluent SR image + real serdes** (the project's Docker/`testcontainers` golden-capture pattern). |

### Non-goals

Out of scope for this feature (revisit "later if ever"): schema **contexts**,
**data contracts** / CSFLE schema rules, schema **exporters**, the legacy
**ZooKeeper** primary-election path, and any **broker-side produce-time schema
validation**.

## Architecture

A new workspace crate `crates/schema-registry/` producing the
`crabka-schema-registry` binary (`src/bin/schema-registry.rs`, matching the
`broker.rs` / `rebalancer.rs` convention). The broker is **untouched**; the
registry reaches Crabka purely through the existing `client-*` crates.

```
  Confluent serdes / REST clients              kafka-* / curl
   (KafkaAvroSerializer, KafkaProtobuf…)             │
            │  HTTP (application/vnd.schemaregistry.v1+json)
            ▼                                         ▼
 ┌────────────────────────────────────────────────────────┐
 │             crabka-schema-registry  (binary)            │
 │   ┌───────────┐   ┌───────────────┐   ┌─────────────┐   │
 │   │ axum REST │ → │ in-mem store  │ ← │ compat eng  │   │
 │   │  handlers │   │ subjects/ids/ │   │ (per-format)│   │
 │   └─────┬─────┘   │  versions     │   └─────────────┘   │
 │         │ writes  └───────┬───────┘                     │
 │         │ (primary only)  │ rebuild + tail              │
 │     ┌───▼─────────────────▼───┐                         │
 │     │ KafkaStore: producer +  │  read-your-writes via   │
 │     │ group-less topic reader │  offset tracking        │
 │     └───┬─────────────────▲───┘                         │
 └─────────┼─────────────────┼─────────────────────────────┘
           │ produce          │ fetch
           ▼                  │
   ┌──────────────────────────────────────────────┐
   │  Crabka broker:  _schemas  (compact, 1 part.) │  ← no broker changes
   └──────────────────────────────────────────────┘
```

### Module responsibilities

| Module | Responsibility | Built on |
|---|---|---|
| `rest/` | axum router + handlers; Confluent content-types and error model. | `axum` (already a broker dep) |
| `store` | In-memory authoritative state: `subject → versions`, `id → schema`, `config`. Rebuilt by replaying `_schemas`; the only thing REST reads from. | — |
| `kafkastore/producer` | **Primary-only** writer. Serializes a key/value record and `send`s it to `_schemas`. | `crabka-client-producer` (`Producer::send` / `flush`) |
| `kafkastore/reader` | **Group-less** `StoreReader`: discover the `_schemas` leader, fetch partition 0 from offset 0, apply each record to `store`, then tail. Tracks last-applied offset. | `crabka-client-core` (`Client::refresh_metadata`, `fetch::fetch_partition`) |
| `kafkastore/topic` | Auto-create `_schemas` (1 partition, `cleanup.policy=compact`, configurable RF) if absent. | `crabka-client-admin` (`create_topics` / `metadata`) |
| `format/{avro,protobuf,json}` | Parse, well-formedness check, canonical form (for id dedup), and (slice 2+) compatibility. | `apache-avro`, `protox`+`prost-reflect`, `serde_json` |
| `primary` | Slice 1: always-primary. Later: Kafka-group leader election + write-forwarding. | (later) `crabka-client-consumer` group plumbing |
| `config.rs` | CLI/file config: bootstrap servers, listen addr, `kafkastore.topic` name, RF, client security. | `clap` |

### Why a group-less reader (not `client-consumer`)

Confluent's store reader (`KafkaStoreReaderThread`) deliberately does **not** use
a consumer group: it manually assigns the single `_schemas` partition, seeks to
0, reads to the end, then tails — consumer groups are used only for *leader
election*, a separate concern. Crabka's `client-consumer` is group-subscription
oriented (`Consumer::start` + `poll` + an assignor), so rather than bend it, the
`StoreReader` is a focused loop over `client-core`'s `fetch_partition`. This
keeps `client-consumer` focused on group semantics and mirrors Confluent's
design exactly.

## Storage model — the `_schemas` topic

Single partition, `cleanup.policy=compact`, replication factor configurable
(default 3, lowered to 1 in tests), auto-created if absent. Records are **keyed
JSON**; the key drives compaction, so the latest record per key wins. The
primary writes these exact shapes (final field ordering / escaping to be locked
against a pinned `cp-schema-registry` image during implementation — see Risks):

```jsonc
// SCHEMA — key:
{"keytype":"SCHEMA","subject":"orders-value","version":1,"magic":1}
//        value:
{"subject":"orders-value","version":1,"id":7,"schemaType":"PROTOBUF",
 "references":[],"schema":"<schema text>","deleted":false}
//   NOTE: schemaType is omitted (null) for AVRO — Confluent does not emit
//   "schemaType":"AVRO". references defaults to []. deleted defaults to false.

// CONFIG — key:  {"keytype":"CONFIG","subject":null,"magic":0}   (null = global)
//          value: {"compatibilityLevel":"BACKWARD"}

// MODE   — key:  {"keytype":"MODE","subject":null,"magic":0}
//          value: {"mode":"READWRITE"}

// NOOP   — key:  {"keytype":"NOOP","magic":0}   (primary catch-up marker)
```

`DELETE_SUBJECT`, `CLEAR_SUBJECTS`, and `MODE` records are not *written* by
Crabka SR until later slices, but the `StoreReader` must tolerate **any key type
it does not yet act on** from day one (forward-compatible parsing) — the
interop acceptance test replays a real `cp-schema-registry` `_schemas` topic,
which may already contain `CONFIG` and `MODE` records.

### Id & version model

- **`id` is global**, keyed by a schema's **canonical form**. Registering an
  identical schema (same canonical form) — even under a different subject —
  reuses the same global `id`.
- **`version` is per-subject**, monotonic from 1.
- Registering an identical schema under the **same** subject is idempotent:
  return the existing `{id, version}`, write no new record.
- A genuinely new schema gets `id = max(known ids) + 1`, assigned by the primary
  and persisted in the `SchemaValue`. The id counter is derived from the max id
  observed while replaying `_schemas` at startup.

### Read-your-writes

`Producer::send` returns the produced record's offset. The `StoreReader` exposes
its last-applied offset. After a write, the primary blocks until
`reader_offset ≥ produced_offset` before returning the REST response, so a client
that registers then immediately reads always observes its own write (mirrors
Confluent's `waitUntilKafkaReaderReachedOffset`).

## REST API

Accept / return `application/vnd.schemaregistry.v1+json` (plus
`application/vnd.schemaregistry+json` and `application/json`). **Slice-1 endpoint
set** — the subset real serdes and the common REST clients exercise:

```
POST /subjects/{subject}/versions            → {"id":N}                  register
POST /subjects/{subject}                      → {subject,id,version,schema} | 404  lookup-if-registered
GET  /schemas/ids/{id}                         → {schema,schemaType,references}
GET  /schemas/types                            → ["AVRO","JSON","PROTOBUF"]
GET  /subjects                                 → ["orders-value", …]
GET  /subjects/{subject}/versions              → [1,2,3]
GET  /subjects/{subject}/versions/{v|latest}   → {subject,version,id,schemaType,schema}
GET  /subjects/{subject}/versions/{v}/schema   → <raw schema text>
GET  /config         PUT /config               → global compatibility level
GET  /config/{subject}   PUT /config/{subject} → per-subject compatibility level
POST /compatibility/subjects/{subject}/versions/{v} → {"is_compatible":true}
GET  /                                          → {}   (server liveness)
```

`/config` is **stored and returned** in slice 1 even though only `NONE` is
*enforced*: a client may set `BACKWARD` and read it back, but no registration is
rejected until slice 2 wires enforcement. Deletes
(`DELETE /subjects/{subject}/versions/{v}` and `DELETE /subjects/{subject}`,
soft + `?permanent=true`) and `mode` **writes** land in slice 3.

### Error model

JSON body `{"error_code":N,"message":"…"}` (serdes branch on `error_code`):

| `error_code` | HTTP | Meaning |
|---|---|---|
| 40401 / 40402 / 40403 | 404 | subject / version / schema not found |
| 409 | 409 | incompatible schema (slice 2+) |
| 42201 / 42202 / 42203 | 422 | invalid schema / version / compatibility level |
| 50001 | 500 | error in the backend datastore |

## Compatibility engine

A `Compatibility` trait per format behind a common interface. **Slice 1 wires
`NONE`** (every *well-formed* registration accepted — an unparseable schema is
still rejected with `42201`, independent of compatibility), but the trait, the
`/config` plumbing,
and per-format **parsing + canonical form** are all in place — canonical form is
needed *now* for id dedup, independent of compatibility checking.

| Format | Parse / canonical form | Compatibility (slice 2+) |
|---|---|---|
| Avro | `apache-avro` — parse + **Parsing Canonical Form** + Rabin fingerprint | Avro schema-resolution rules |
| Protobuf | `protox` (`.proto` → `FileDescriptorSet`) + `prost-reflect` | Confluent's field add/remove rules |
| JSON Schema | `serde_json` + well-formedness checks | Confluent's JSON-Schema diff rules |

Compatibility **levels** (`NONE`, `BACKWARD`, `FORWARD`, `FULL`, and the three
`_TRANSITIVE` variants) are stored via `/config` from slice 1; the matrix of
actual checks is the bulk of slice 2.

## Primary election — deferred to slice 5

Slice 1 is **single-node, always-primary**: this node owns id assignment and is
the only writer to `_schemas`. The HA slice adds **Kafka-group-based leader
election** (SR nodes join a group via Crabka's existing group coordinator; one
becomes primary) plus **write-forwarding** (secondaries proxy mutating requests
to the primary's advertised URL). No ZooKeeper — Crabka has none, and the
Kafka-based election is Confluent's modern default.

## Roadmap

Each slice is an independently shippable plan with its own spec.

| # | Slice | Contents |
|---|---|---|
| **1** | **Vertical thin-slice** | New crate + binary; `_schemas` KafkaStore (producer + group-less reader) + read-your-writes; in-mem store; REST happy-path; all-3-format parse + canonical-form dedup; **compat = NONE**; single-node always-primary; `/config` stored-not-enforced; real-client validation harness. |
| 2 | Compatibility matrix | Enforce `BACKWARD` / `FORWARD` / `FULL` (+ `_TRANSITIVE`) per format; `409` on incompatible; real `/compatibility`; per-subject + global config enforcement. |
| 3 | Deletes, modes, lookups | Soft + permanent delete (`DELETE_SUBJECT` / `CLEAR_SUBJECTS`); `mode` read/write (`READWRITE` / `READONLY` / `IMPORT`); `?deleted=true`, `/schemas/ids/{id}/versions`, `/subjects/{subject}/versions/{v}/referencedby`. |
| 4 | Schema references | Cross-schema references (Protobuf imports, Avro/JSON refs); reference resolution in canonical form + compatibility; `referencedby`. |
| 5 | HA | Kafka-group leader election + write-forwarding to the primary; multi-node conformance. |
| 6 | Security | REST auth (Basic / Bearer aligned with Crabka's existing OAuth), TLS, SR authorization; SR↔broker client auth (SASL / TLS, already supported by `client-*`). |
| 7 | Operator + packaging | `SchemaRegistry` CRD (operator roadmap item 68), container image, deploy manifests, docs; flip README ❌ → ✅. |

## Slice 1 — detailed scope

**Deliverables**

1. `crates/schema-registry/` crate + `crabka-schema-registry` binary. Clap
   config: bootstrap servers, REST listen addr, `_schemas` topic name + RF,
   client security (TLS / SASL passthrough to `client-*`).
2. `kafkastore/topic`: auto-create `_schemas` (1 partition, `compact`, RF) if
   absent, via `client-admin`.
3. `kafkastore/reader`: group-less `StoreReader` on `client-core` —
   `refresh_metadata` to find the `_schemas` leader, `fetch_partition` loop from
   offset 0, apply records, tail; expose last-applied offset.
4. `kafkastore/producer`: primary writer on `client-producer`; read-your-writes
   offset gate.
5. `store`: in-mem `subjects → versions`, `id → schema`, `config`; rebuilt by
   replaying `_schemas`; global-id / per-subject-version model with
   canonical-form dedup.
6. `format`: Avro / Protobuf / JSON Schema parse + well-formedness + canonical
   form; `NONE` compatibility.
7. `rest`: the endpoint set above, exact content-types, exact error codes.

**Acceptance criteria**

- A real `KafkaAvroSerializer` / `KafkaProtobufSerializer` /
  `KafkaJsonSchemaSerializer` configured against the Crabka broker + Crabka SR
  can produce → auto-register → consume → `GET /schemas/ids/{id}` round-trip for
  each of the three formats.
- A `_schemas` topic written by a real `cp-schema-registry` is replayed
  correctly by the `StoreReader`, and a `_schemas` topic written by Crabka SR is
  read correctly by a real `cp-schema-registry` (byte-shape interop, at least
  for `SCHEMA` and `CONFIG` records).
- Confluent's documented REST `curl` examples return matching JSON shapes and
  the exact numeric `error_code`s for the not-found / invalid-schema cases.
- Registering an identical schema is idempotent (same `id`, same `version`); the
  same schema under a second subject reuses the global `id` with a fresh
  per-subject `version`.

**Explicitly *not* in slice 1:** compatibility enforcement, deletes, `mode`
writes, schema references, multi-node / election / forwarding, REST auth.

## Validation strategy

Built on the repo's existing `testcontainers` / `testcontainers-modules` (kafka)
harness:

1. **`_schemas` format interop** — write schemas with a pinned real
   `cp-schema-registry`, read them with Crabka's `StoreReader`, and vice-versa.
   Proves byte-shape exactness.
2. **Real serdes round-trip** — Confluent serdes pointed at Crabka SR + Crabka
   broker: produce → auto-register → consume → fetch-by-id, per format.
3. **REST conformance** — replay Confluent's documented `curl` examples; assert
   JSON shapes + numeric error codes.

> ⚠️ **Mac caveat.** Multi-broker JVM *data* replication does not work on the dev
> Mac (advertised `host.docker.internal` is unresolvable from host procs). SR
> round-trips need only a **single broker**, so slice 1–2 validation runs
> locally against a single-broker setup; the full matrix runs on Linux CI.

## Open risks (carry into planning)

1. **Protobuf canonical form** — Confluent's `.proto` normalization is bespoke;
   `protox` / `prost-reflect` give a parsed descriptor, not Confluent's exact
   canonical string. Matching it for id dedup is the highest-risk spot; lock it
   against a pinned `cp-schema-registry` empirically.
2. **Schema-string escaping** — the JSON-escaped `schema` field (in both
   `_schemas` values and REST responses) must match Confluent's escaping exactly
   for `_schemas` interop.
3. **Avro `schemaType` omission** — must *not* emit `"schemaType":"AVRO"`
   (Confluent omits it; emitting it breaks byte interop and may confuse strict
   readers).
4. **`StoreReader` gap** — confirm `client-core`'s `fetch_partition` +
   `refresh_metadata` cover leader discovery for `_schemas` and clean tailing;
   if a gap appears, fall back to adding a manual-assign / seek mode to
   `client-consumer`.
5. **Record field ordering** — Confluent's key/value JSON field order and the
   `magic` byte values must be reproduced for compaction-key stability and
   interop; pin against the real image rather than the docs.

## Dependencies

New crates, all within `deny.toml`'s license allowlist (Apache-2.0 / MIT / BSD /
…), versions and exact licenses to be confirmed at planning time (mind the
`multiple-versions = "warn"` ban):

- `apache-avro` (Apache-2.0) — Avro parse, canonical form, fingerprint.
- `protox` + `prost-reflect` (Apache-2.0 / MIT) — Protobuf `.proto` parse +
  descriptor reflection.
- `serde_json` (already a workspace dep) — JSON Schema parsing.
- `axum`, `clap`, `tokio`, `bytes`, `thiserror` — already workspace deps.
