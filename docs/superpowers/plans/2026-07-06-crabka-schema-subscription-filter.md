# Schema-typed complex subscription filter — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the gateway's advisory `FieldPredicate` (JSONPath, `EQUALS`-only) with a SQL `WHERE`-fragment compiled to a DataFusion physical expression, evaluated server-side per batch over Arrow-decoded records (`RowBridge`) — supporting complex nested/repeated filtering and enums by symbolic name, delivering matching records byte-exact.

**Architecture:** A subscription's SQL filter is compiled once (per `schema_id`) into a DataFusion `PhysicalExpr` against the record's Arrow schema; each fetched batch is decoded to an Arrow `RecordBatch` via `RowBridge` (enums → `Dictionary<Utf8>` symbol names), the predicate is evaluated to a boolean mask, and only masked-true records are delivered as their original verbatim bytes. Reuses landed code (Schema Registry, `schema-serde`, `RowBridge`, DataFusion, the `Subscribe` stream); the one net-new decode addition is enum int→name mapping.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), DataFusion (workspace git pin) + Arrow, `crabka-schema-serde`/`crabka-schema-registry`, `crabka-client-streams` (`RowBridge`/`RowCodec`), `prost`/Connect-RPC, `assert2`, `cargo +nightly fmt`, `clippy::pedantic`.

**Spec:** [`docs/superpowers/specs/2026-07-06-crabka-schema-subscription-filter-design.md`](../specs/2026-07-06-crabka-schema-subscription-filter-design.md).

---

## Invariants

1. **Server-side delivery gating.** The predicate decides delivery in the gateway; a masked-false record is never written to the stream.
2. **Byte-exact delivery.** Delivered bytes are the original verbatim record; the Arrow decode is evaluation-only.
3. **One engine.** SQL → DataFusion `PhysicalExpr`; no second filter runtime.
4. **Enums by name.** Enum fields evaluate as `Dictionary<Utf8>` symbol names; unknown numbers → `UNKNOWN_<n>`, never dropped.
5. **Greenfield proto.** `FieldPredicate` is removed and replaced by a `filter` SQL string — no back-compat.
6. **Not RLS.** This gates the *filter* within the existing topic/group Read ACL; row-level *authorization* is Chapter E — do not conflate.
7. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** the `filter` proto field; the SQL→`PhysicalExpr` compiler; the enum→`Dictionary<Utf8>` decode; the per-batch decode→evaluate→mask→deliver Subscribe path + per-`(subscription, schema_id)` cache + schema-evolution recompile.
- **Deferred:** RLS composition (Chapter E); DB protobuf/avro/arrow column types (Chapter C); WebSocket/SSE + fan-out scale (Chapter D); pushdown; numeric enum surface; the SDK/CloudEvents/ack/KEDA (sibling cycles).

---

## File Structure

- **`crates/grpc-gateway/proto/crabka/gateway/v1/gateway.proto`** — `SubscribeStart.filter` (string), remove `FieldPredicate`/`predicates`.
- **`crates/grpc-gateway/src/filter.rs`** (new) — the SQL→`PhysicalExpr` compiler + per-batch evaluation.
- **`crates/grpc-gateway/src/streaming.rs`** — replace the JSONPath `compile_subscribe_predicates`/`structured_json_matches` path with the compiled-filter cache + per-batch mask/deliver.
- **`crates/grpc-gateway/src/consume.rs`** — carry original bytes alongside the decoded batch.
- **`crates/client-streams/src/columnar/serde/arrow.rs`** (+ the decode path) — enum int→symbol-name `Dictionary<Utf8>` decode.

---

## Task 1: Proto — `filter` SQL field replaces `FieldPredicate`

**Files:**
- Modify: `crates/grpc-gateway/proto/crabka/gateway/v1/gateway.proto`

- [ ] **Step 1: Edit the proto**

In `SubscribeStart` (`:101`), remove `repeated FieldPredicate predicates = 4;` (`:105`) and add:

```proto
  // A SQL WHERE-fragment (boolean expression) filtering records server-side by
  // their registry-decoded fields. Empty = deliver all (subject to Read ACL).
  string filter = 4;
```

Remove the `FieldPredicate` message (`:34`) and the `PredicateOp` enum (grep the proto). Regenerate the Rust types (`crates/grpc-gateway` build). (Greenfield — no back-compat.)

- [ ] **Step 2: Build + commit**

Run: `cargo build -p crabka-grpc-gateway` — compiles once the `FieldPredicate` references in `streaming.rs` are removed (Task 4); for now this step just regenerates `pb`. If the build blocks on `streaming.rs`, land Task 4's removal together.

```bash
git add crates/grpc-gateway/proto/crabka/gateway/v1/gateway.proto
git commit -m "feat(gateway): replace Subscribe FieldPredicate with a SQL filter field"
```

---

## Task 2: The SQL → DataFusion `PhysicalExpr` compiler

**Files:**
- Create: `crates/grpc-gateway/src/filter.rs`

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use arrow::array::{BooleanArray, Int64Array, StringArray, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use assert2::assert;
    use super::*;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("status", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
        ]));
        RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec!["PAID", "CANCELLED", "PAID"])),
            Arc::new(Int64Array::from(vec![150, 50, 20])),
        ]).unwrap()
    }

    #[test]
    fn compiles_and_masks_a_complex_filter() {
        let b = batch();
        let f = CompiledFilter::compile("status = 'PAID' AND price > 100", &b.schema()).unwrap();
        let mask = f.evaluate(&b).unwrap();
        assert!(mask == BooleanArray::from(vec![true, false, false]));
    }

    #[test]
    fn in_and_like_over_strings() {
        let b = batch();
        assert!(CompiledFilter::compile("status IN ('PAID','SHIPPED')", &b.schema()).unwrap()
            .evaluate(&b).unwrap() == BooleanArray::from(vec![true, false, true]));
    }

    #[test]
    fn malformed_filter_errors_at_compile() {
        assert!(CompiledFilter::compile("status ==== 'x'", &batch().schema()).is_err());
    }

    #[test]
    fn unknown_column_errors_at_compile() {
        assert!(CompiledFilter::compile("nope = 1", &batch().schema()).is_err());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-grpc-gateway filter::`
Expected: FAIL — `CompiledFilter` undefined.

- [ ] **Step 3: Implement `CompiledFilter`**

Insert at the TOP of `crates/grpc-gateway/src/filter.rs`. Parse the SQL WHERE-fragment to a DataFusion `Expr`, then compile a `PhysicalExpr` against the Arrow schema and evaluate to a `BooleanArray`:

```rust
//! SQL WHERE-fragment → DataFusion physical predicate over an Arrow RecordBatch.
//! Compiled once per (subscription, schema_id); evaluated per decoded batch.

use std::sync::Arc;

use arrow::array::{BooleanArray, RecordBatch};
use arrow::datatypes::SchemaRef;
use datafusion::common::DFSchema;
use datafusion::execution::context::SessionContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::execution_props::ExecutionProps;

pub struct CompiledFilter {
    expr: Arc<dyn PhysicalExpr>,
}

impl CompiledFilter {
    /// Compile a SQL boolean expression against `schema`.
    ///
    /// # Errors
    /// Parse error, unknown column, or a non-boolean result type.
    pub fn compile(sql: &str, schema: &SchemaRef) -> Result<Self, String> {
        let ctx = SessionContext::new();
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).map_err(|e| e.to_string())?;
        // Parse the WHERE-fragment to a logical Expr against the schema.
        let logical = ctx.parse_sql_expr(sql, &df_schema).map_err(|e| e.to_string())?;
        let expr = create_physical_expr(&logical, &df_schema, &ExecutionProps::new())
            .map_err(|e| e.to_string())?;
        Ok(Self { expr })
    }

    /// Evaluate the predicate over `batch`, returning a per-row boolean mask.
    ///
    /// # Errors
    /// Evaluation failure or a non-boolean result.
    pub fn evaluate(&self, batch: &RecordBatch) -> Result<BooleanArray, String> {
        use datafusion::physical_expr::ColumnarValue;
        let array = match self.expr.evaluate(batch).map_err(|e| e.to_string())? {
            ColumnarValue::Array(a) => a,
            // A constant predicate (e.g. `1 = 1`) — broadcast the scalar to the batch length.
            ColumnarValue::Scalar(s) => {
                s.to_array_of_size(batch.num_rows()).map_err(|e| e.to_string())?
            }
        };
        array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| "filter did not evaluate to a boolean".to_string())
            .cloned()
    }
}
```

(The exact DataFusion import paths track the workspace's git-pinned datafusion rev — confirm `parse_sql_expr`/`create_physical_expr`/`ExecutionProps` against it; the *shape* — parse WHERE → `Expr` → `PhysicalExpr` → `evaluate` → `BooleanArray` — is stable.)

- [ ] **Step 4: Run to verify it passes; commit**

Run: `cargo test -p crabka-grpc-gateway filter::` → PASS.

```bash
git add crates/grpc-gateway/src/filter.rs
git commit -m "feat(gateway): SQL->DataFusion compiled subscription filter"
```

---

## Task 3: Enum → `Dictionary<Utf8>` symbol-name decode

**Files:**
- Modify: `crates/client-streams/src/columnar/serde/arrow.rs` (+ the protobuf/avro decode path)

- [ ] **Step 1: Write the failing test**

Decoding a record whose schema has an enum field (protobuf `Status { UNKNOWN=0; PAID=2 }`) yields an Arrow `Dictionary<Utf8>` column whose value is the symbol **name** (`"PAID"`), and an out-of-range number decodes to `"UNKNOWN_7"`.

```rust
    #[test]
    fn enum_field_decodes_to_symbol_name_dictionary() { /* protobuf value with status=2 -> "PAID" */ }
    #[test]
    fn unknown_enum_number_decodes_to_sentinel() { /* status=7 -> "UNKNOWN_7", not dropped */ }
```

- [ ] **Step 2: Run to verify it fails; implement**

In the columnar decode (`crates/client-streams/src/columnar/serde/arrow.rs` and the protobuf/avro value→Arrow mapping), map an enum field to `DataType::Dictionary(Int32, Utf8)`: resolve the wire integer to its symbol name via the schema's enum descriptor (from the Schema Registry — protobuf `EnumDescriptor` / Avro enum `symbols`); build the dictionary with symbol names as values and the wire number as the code. For a number absent from the descriptor, use the sentinel `format!("UNKNOWN_{n}")`. (This is the one net-new decode addition; the rest of the Arrow decode is unchanged.)

- [ ] **Step 3: Run to verify it passes; commit**

Run: `cargo test -p crabka-client-streams enum_field_decodes` → PASS.

```bash
git add crates/client-streams/src/columnar/serde/arrow.rs
git commit -m "feat(client-streams): decode protobuf/avro enums to Dictionary<Utf8> symbol names"
```

---

## Task 4: Wire the filter into the `Subscribe` path

**Files:**
- Modify: `crates/grpc-gateway/src/streaming.rs`, `crates/grpc-gateway/src/consume.rs`

- [ ] **Step 1: Write the failing integration test**

Produce protobuf records (with a nested/repeated field + an enum) to a topic; open a `Subscribe` with `filter = "status = 'PAID' AND items[0].price > 100"`; assert exactly the matching records are delivered, **byte-exact** to the produced bytes, and a non-matching record is **never** delivered. A second test: an Avro-encoded equivalent yields identical selection; a new `schema_id` recompiles and keeps filtering.

- [ ] **Step 2: Run to verify it fails; implement**

In `crates/grpc-gateway/src/streaming.rs`, **remove** `compile_subscribe_predicates`/`structured_json_matches`/`predicate_matches` (`:42-114`) and replace the per-record advisory JSONPath check with:
- On `SubscribeStart`, hold the `filter` string; lazily compile a `CompiledFilter` (Task 2) the first time a given `schema_id` is seen, decoding the schema to an Arrow schema via `RowBridge`; cache per `(subscription, schema_id)`; a compile error terminates the stream with a clear status.
- Per fetched batch: decode `(key, value)` → an Arrow `RecordBatch` via `RowCodec`/`RowBridge` (`crates/client-streams/src/columnar/`, Task 3's enum decode included), **keeping the original bytes** alongside (extend `consume.rs`'s decoded record to carry the raw record); evaluate `CompiledFilter::evaluate(&batch)` → the `BooleanArray` mask; deliver only masked-true records as their **original verbatim bytes** down the `SubscribeFrame` stream.
- A record whose `schema_id` differs recompiles (or reuses the cached predicate for that id). An undecodable record: drop-with-metric (defined policy), never fail the batch.

Preserve the existing pre-stream Read-ACL gate on the group + topics (unchanged) — the filter narrows *within* it.

- [ ] **Step 3: Run to verify it passes; commit**

Run: `cargo test -p crabka-grpc-gateway` (subscribe + filter tests) → PASS.

```bash
git add crates/grpc-gateway/src/streaming.rs crates/grpc-gateway/src/consume.rs
git commit -m "feat(gateway): server-enforced complex subscription filtering over Arrow batches"
```

---

## Task 5: Final gate

- [ ] **Step 1:** `cargo +nightly fmt --check` — no diff.
- [ ] **Step 2:** `cargo clippy --workspace --all-targets -- -D warnings` — no warnings.
- [ ] **Step 3:** `cargo nextest run -p crabka-grpc-gateway -p crabka-client-streams` (or `cargo test`) — PASS, including the nested-protobuf + enum-by-name + Avro-parity + schema-evolution filter tests.
- [ ] **Step 4:** Commit any formatting.

---

## Self-Review

**1. Spec coverage:** SQL filter proto field (Task 1); SQL→`PhysicalExpr` compiler (Task 2); enum→`Dictionary<Utf8>` by-name decode + `UNKNOWN_<n>` (Task 3); per-batch decode→evaluate→mask→byte-exact deliver + per-`schema_id` compile cache + schema-evolution recompile + server-enforcement (Task 4). Deferred set (RLS, DB columns, WS/SSE, pushdown, numeric enums, SDK/CloudEvents/ack/KEDA) untouched — Scope boundary. ✅

**2. Placeholder scan:** Tasks 1-3 are complete/near-complete code; Task 2 flags the one API-version confirmation (DataFusion import paths against the git-pinned rev) with the stable shape given, not a blank; Task 4 gives the exact removal (`streaming.rs:42-114`) + the replacement structure. No `TBD`/`TODO`.

**3. Type consistency:** `CompiledFilter::{compile(&str, &SchemaRef)->Result, evaluate(&RecordBatch)->BooleanArray}` (Task 2) is used identically in the Subscribe loop (Task 4); the Arrow `RecordBatch` from `RowCodec`/`RowBridge` decode (Task 3/4) is what `evaluate` consumes; the enum `Dictionary<Utf8>` (Task 3) is what `status = 'PAID'` filters against; the `filter` proto field (Task 1) is the `compile` input (Task 4).

**4. Invariant check:** server-side gating (Task 4 delivers only masked-true); byte-exact original bytes (Task 4 keeps raw record); one engine (DataFusion, Task 2); enums by name + `UNKNOWN_<n>` (Task 3); greenfield proto (Task 1 removes `FieldPredicate`); not-RLS (Task 4 preserves the Read-ACL gate, RLS deferred). Each task green.

**5. Prerequisites:** none unlanded — this reuses landed code (registry, schema-serde, RowBridge, DataFusion, the Subscribe stream); it is the first *buildable-today* sub-service of the vision.

## Audit gate evidence

- `tools/check-grpc-gateway-arrow-filter.sh` is the in-repo strict gate for the plan-complete path. It runs `cargo test -p crabka-grpc-gateway --features arrow filter` and `cargo test -p crabka-grpc-gateway --test streaming --features arrow`, so SQL/DataFusion/Arrow physical-expression evaluation and Subscribe delivery behavior cannot pass by using the default JSON compatibility fallback.
- `.github/workflows/ci.yml` runs that script in the `grpc-gateway-integration` job and includes the script in both the broad Rust and gRPC-gateway path filters.
- The default/non-arrow path remains a compatibility path for simple scalar JSON filters only. Complex SQL (`IN`, `LIKE`, nested/repeated paths) is explicitly rejected without Arrow/DataFusion, and with Arrow enabled a complex SQL filter over a non-Arrow/non-schema JSON record drops rather than claiming plan-complete behavior.
- Required Arrow evidence lives in `crates/grpc-gateway/src/filter.rs` (`datafusion_arrow_filter_*`, `gateway_subscription_filter_*`, and `complex_sql_does_not_fall_back_to_legacy_json_filtering`) and `crates/grpc-gateway/src/streaming.rs` (`filtered_arrow_ipc_delivery_preserves_original_record_bytes`, `filtered_schema_registry_row_bridge_supports_nested_repeated_enum_and_raw_delivery`, and schema-id recompile tests). The integration test `crates/grpc-gateway/tests/streaming.rs::subscribe_filter_uses_batched_arrow_masks_and_preserves_raw_ipc_bytes` verifies Subscribe delivers the original bytes after Arrow mask evaluation.
