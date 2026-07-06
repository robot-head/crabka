# Schema-typed complex subscription filter — design

**Date:** 2026-07-06
**Status:** Approved
**Type:** Subsystem design. First buildable sub-service of the [serverless-backend vision](2026-07-06-crabka-serverless-backend-vision-design.md) — Chapter G's topic-side.

## Context — where this sits

First concrete sub-service of the serverless-backend vision: the **complex realtime subscription filter engine** (Chapter G, topic-side). Today the gateway's `Subscribe` stream carries an *advisory* `FieldPredicate` — a single JSONPath with **`EQUALS`-only** (`crates/grpc-gateway/src/streaming.rs:42-56`) — which both under-serves real filtering and is a **data-leak-by-default** (it filters, it does not enforce). This spec replaces it with a **SQL predicate compiled to a DataFusion physical expression**, evaluated **server-side** over batches of records decoded to Arrow via `RowBridge` + the Schema Registry — so a subscription can filter on nested/repeated protobuf/avro/json fields (including **enums by name**) with full SQL expressiveness, and the filter *decides delivery*.

This is the near-term proof of the vision's wedge — *the same DataFusion engine that scans your lakehouse filters your subscriptions* — and it reuses only landed code (Schema Registry, `schema-serde`, `RowBridge`, DataFusion, the `Subscribe` stream). The messaging SDK, CloudEvents binding, ack semantics, and KEDA bridge are sibling cycles; the DB-column-type side of G (protobuf/avro/arrow Postgres columns) rides the unbuilt Postgres compute.

## Design Goals

- **Complex, expressive filters:** boolean combinators, comparisons, `IN`, `LIKE`/regex, null-handling, and **nested/repeated field access** over protobuf/avro/json records — far beyond `FieldPredicate`'s single-field equality.
- **Server-enforced:** the predicate gates delivery in the gateway *before* the stream — a non-matching record is never sent (closes the advisory-filter leak).
- **One engine:** compile SQL → a DataFusion physical expression evaluated over Arrow batches — the same engine the lakehouse/observability stack uses, no second runtime.
- **Byte-exact delivery:** the filter *selects*; delivered bytes are the untouched original record. Decode is evaluation-only.
- **Enum-by-name:** filter enums by symbolic name (`status = 'PAID'`), not wire integer.

### Non-goals

- **RLS composition** — combining the filter with row-level authorization is Chapter E (needs the auth model); this slice layers on the existing topic/group Read ACL only.
- **DB protobuf/avro/arrow column types** — Chapter C (needs the Neon-shaped Postgres compute).
- **WebSocket/SSE binding + realtime-scale fan-out** — broader Chapter D; this slice uses the existing gRPC `Subscribe`.
- **Filter pushdown to the broker/flush** — an optimization; this slice evaluates in the gateway.
- **Numeric enum filtering** — enums are exposed by name only (`Dictionary<Utf8>`); the numeric surface is deferred.
- **The messaging SDK / CloudEvents / ack / KEDA** — sibling cycles.

## Architecture Overview

```
Subscribe(topic, filter: SQL WHERE-fragment)          [gRPC, crates/grpc-gateway]
  1. On first record of a schema_id: resolve the record schema from the Confluent
     framing (magic | schema_id | body) via the Schema Registry → an Arrow schema
     (RowBridge), enum fields → Dictionary<Utf8> of symbol names.
  2. Compile the SQL predicate ONCE against that Arrow schema:
        SQL WHERE-fragment → DataFusion Expr → PhysicalExpr   (create_physical_expr)
     Cache the compiled predicate keyed by (subscription, schema_id).
  3. Per fetched batch of records for the subscription:
        RowBridge: decode (key,value) → Arrow RecordBatch (+ reserved __key/__offset/__ts)
        PhysicalExpr::evaluate(&batch) → BooleanArray mask
        deliver ONLY the masked-true records — as their ORIGINAL verbatim bytes
```

## Key Design Decisions

### SQL → DataFusion physical expression, compiled once, evaluated per batch

A subscription carries a SQL `WHERE`-fragment (a boolean expression). It is parsed and planned once against the record's Arrow schema into a DataFusion `PhysicalExpr` (`create_physical_expr(expr, &df_schema, &ExecutionProps)`), then `PhysicalExpr::evaluate(&RecordBatch) -> ColumnarValue` yields a `BooleanArray` mask per decoded batch. Compiling once and evaluating over batches amortizes cost (DataFusion is batch-oriented). *Alternative rejected:* CEL or a bespoke AST — a second evaluation engine, undercutting the "one engine (DataFusion)" wedge that is the whole point of Chapter G.

### Arrow via `RowBridge` is the unifying decode target

Protobuf, Avro, and JSON records all resolve — via the Schema Registry serdes — to one Arrow `RecordBatch` through `RowBridge`/`RowCodec` (`crates/client-streams/src/columnar/`, which already decodes Kafka `(key, value)` into Arrow/Polars frames with reserved `__key`/`__offset`/`__timestamp`/`__partition` columns). So the filter is schema-format-agnostic: `items[0].price > 100` works identically whether the record was protobuf or Avro. The Arrow schema derived from the registry schema is what the SQL predicate compiles against.

### Enums decode to `Dictionary<Utf8>` symbol names (net-new)

An enum field's *wire* value is an integer (protobuf enum number; Avro enum ordinal), but filters must use the symbolic **name**. So the decode maps the integer to its symbol via the registry's enum descriptor and represents the field as an Arrow `Dictionary<Utf8>` — dictionary values are the symbol names, codes are the numeric values, so DataFusion filters it by name transparently and compactly. Filters read `status = 'PAID'`, `status IN ('PAID','SHIPPED')`, `status LIKE 'SHIP%'`, `order.state = 'READY'`. **This int→name mapping is net-new** — verified absent (`crates/client-streams/src/columnar/serde/arrow.rs` maps to `Utf8`/`Float64`/… with no enum handling). **Unknown enum number** (protobuf forward-compat: a newer producer sends a number the reader's schema lacks): decode to a `UNKNOWN_<n>` sentinel string — never dropped, never batch-erroring, still filterable and still delivered byte-exact. Enums are exposed **by name only**; the numeric surface is deferred.

### Server-enforced delivery, byte-exact

The predicate runs in the gateway `Subscribe` path and **gates delivery** — a masked-false record is never written to the stream (unlike today's advisory `FieldPredicate`). It layers on the existing pre-stream Read ACL on the group + topics (`crates/grpc-gateway/src/streaming.rs`); RLS composition is Chapter E. Delivered bytes are the **original verbatim record** (the Arrow decode exists only to evaluate the mask), so wire byte-exactness is preserved.

### Schema-evolution recompile

The compiled predicate is bound to a specific `schema_id`'s Arrow schema. A record carrying a **new** `schema_id` (a compatible schema evolution) triggers a recompile against the new Arrow schema (cached per `(subscription, schema_id)`). A predicate that no longer compiles against the evolved schema (references a removed field) surfaces as a stream error, not a silent mismatch.

### Proto surface — greenfield

Add a `filter` string field (the SQL `WHERE`-fragment) to the `Subscribe` request in `gateway.proto`; the `EQUALS`-only `FieldPredicate` is **superseded and removed** (greenfield — no back-compat, per project policy). A malformed filter fails at subscribe time with a clear error (compile failure), never a silent pass-through.

## Integration

- **`crates/grpc-gateway/proto/…/gateway.proto`** — `Subscribe` gains a `filter` SQL string; `FieldPredicate` removed.
- **`crates/grpc-gateway/src/streaming.rs`** — replace `compile_subscribe_predicates` (`:42`) with the SQL→`PhysicalExpr` compiler + the per-`schema_id` cache; the per-batch decode→evaluate→mask→deliver loop (in place of the advisory JSONPath check).
- **`crates/grpc-gateway/src/consume.rs`** — the record source; carry the original bytes alongside the decoded Arrow batch for byte-exact delivery.
- **`crates/client-streams/src/columnar/`** (`RowBridge`/`RowCodec`) — reuse for decode→Arrow; **extend** for enum int→symbol-name `Dictionary<Utf8>` decode (the one net-new decode addition).
- **`crates/schema-registry` / `crates/schema-serde`** — resolve `schema_id` → schema (incl. the enum symbol table) from the Confluent framing.
- **DataFusion** — the SQL parse + `create_physical_expr` + `PhysicalExpr::evaluate` path (as `crates/blockstore`/`crates/promql` already use DataFusion).

## Kafka / wire compliance

- **Delivered records are byte-exact** — the filter selects; it never rewrites payloads. The Arrow decode is internal to evaluation.
- **The filter is a gateway concept** — the broker wire is unchanged; a subscription is still a consumer of the topic, only server-side-filtered.
- **Schema framing** — the Confluent `magic|schema_id|body` framing (`schema-serde`) is the schema-resolution source, unchanged.

## Testing

- **Complex nested filter selects exactly the matches:** produce protobuf records with a nested/repeated field + an enum; a subscription with `status = 'PAID' AND items[0].price > 100` delivers exactly the matching records, **byte-exact** to the produced bytes.
- **Enum by name:** `status IN ('PAID','SHIPPED')` and `status LIKE 'SHIP%'` match by symbol name; an unknown enum number is delivered as `UNKNOWN_<n>` and matches `status LIKE 'UNKNOWN_%'`.
- **Server-enforced:** a non-matching record is **never delivered** (assert the stream, not a flag) — the closed leak.
- **Format-agnostic:** the same filter over an Avro-encoded equivalent record yields identical selection.
- **Schema evolution:** a compatibly-evolved `schema_id` recompiles the predicate and keeps filtering; an incompatible reference (removed field) errors the stream clearly.
- **Malformed filter:** a bad SQL fragment fails at subscribe time with a descriptive error, never a silent all-pass or all-drop.
- **Batch evaluation:** filtering is applied per decoded batch (assert the decode/evaluate is batched, not per-record).

## Risks (carried into the plan)

- **Per-batch evaluation cost / fan-out:** a complex predicate over a decoded nested batch, per subscription — batch evaluation + the per-`(subscription, schema_id)` compiled-predicate cache are the mitigation; broader fan-out scale is Chapter D.
- **Enum decode fidelity:** the int→name mapping must use the *record's* schema (not a stale one) and handle unknown numbers without dropping — covered by the enum tests.
- **Server-enforcement is auth-adjacent but not RLS:** this slice enforces the *filter*; it does not do row-level *authorization*. It must not be mistaken for RLS (Chapter E) — the filter narrows within what the topic/group Read ACL already permits.
- **Decode failures:** an undecodable record (corrupt/unknown schema) must have a defined policy (drop-with-metric or error) — not a batch-wide failure.

## Resolved decisions (from brainstorming)

- **Scope:** Chapter G's topic-side complex-filter engine (SDK/CloudEvents/ack/KEDA are sibling cycles; DB-column types ride Postgres/Chapter C).
- **Filter language:** SQL `WHERE`-fragment → DataFusion `PhysicalExpr`, evaluated per Arrow batch.
- **Decode:** Arrow via `RowBridge`; enums → `Dictionary<Utf8>` symbol names (**by name only**); unknown number → `UNKNOWN_<n>` sentinel.
- **Enforcement:** server-side, gating delivery; byte-exact original bytes delivered.
- **Schema evolution:** recompile per `schema_id`.
- **Proto:** greenfield `filter` SQL field; remove `EQUALS`-only `FieldPredicate`.
