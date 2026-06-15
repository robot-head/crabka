# client-streams data-formats guide + tested format pipeline

**Date:** 2026-06-15
**Status:** Approved (design)
**Crate(s):** `crabka-client-streams`, `crabka-docgen`; website `guide/`; CI.

## Problem

`crabka-client-streams` supports a broad set of data formats — primitive serdes,
registry-backed schema serdes (JSON Schema / Protobuf / Avro), and columnar
serdes/codecs (Polars, Arrow, `columnar`) — but the documentation does not
explain them, there is no getting-started guide for the streams stack, and there
is no worked example that carries a record through multiple formats. Separately,
no documentation example in the repo is automatically built or run, so doc code
can rot silently (examples are not built in CI; website markdown snippets are
hand-maintained with zero verification).

## Goals

1. A getting-started + data-formats guide for `crabka-client-streams`.
2. A single worked pipeline that moves order data through every format tier:
   **JSON → Protobuf → Arrow → columnar Polars → summary Protobuf.**
3. An automated harness that builds and runs every documented example, asserts it
   works, and guarantees the published docs contain exactly the tested code
   (drift-guarded), wired into CI and runnable locally.

## Non-goals

- No new data-format support. Compose only the serdes/codecs that already exist.
- No changes to `docs/superpowers/` or unrelated guide pages.
- No ZK/migration/compat shims (greenfield project; see `CLAUDE.md`).

## Current state (verified)

Serdes/codecs available in `crabka-client-streams`:

| Serde / codec | Handles | Crate | Feature gate | Source |
|---|---|---|---|---|
| `StringSerde`, `I64Serde`, `BytesSerde` | `String` / `i64` / `Bytes` | client-streams | none | `src/processor/serde.rs` |
| `SchemaSerde<T, JsonSerde<T>>` | JSON-Schema typed | schema-serde | none (default on) | `src/format/json.rs` |
| `SchemaSerde<T, ProtobufSerde<T>>` | prost `Message` (dynamic via `prost-reflect`) | schema-serde | none (default on) | `src/format/protobuf.rs` |
| `SchemaSerde<T, AvroSerde<T>>` | `apache_avro::AvroSchema` | schema-serde | none (default on) | `src/format/avro.rs` |
| `PolarsIpcSerde` | `polars::DataFrame` (Arrow IPC) | client-streams | `polars` | `src/columnar/serde/polars.rs` |
| `ArrowIpcSerde` | `arrow::RecordBatch` (Arrow IPC) | client-streams | `arrow` | `src/columnar/serde/arrow.rs` |
| `ColumnarSerde<T>` | `columnar::Columnar` | client-streams | `columnar` | `src/columnar/serde/columnar.rs` |
| `BlobCodec`, `RowCodec` | Kafka records ↔ `DataFrame` | client-streams | `polars` | `src/columnar/topology/codec.rs` |

Key facts the design relies on:

- Protobuf is fully dynamic (no `.proto` at runtime). The existing
  `examples/protobuf_pipeline.rs` mirrors codegen via committed
  `examples/proto/order.proto` + `examples/gen/{order.rs, file_descriptor_set.bin}`
  + `examples/gen/regenerate.sh` (no `build.rs`).
- High-level DSL exists: `StreamsApp::builder().bootstrap(..).application_id(..)
  .schema_registry(..).build()`, then `app.streams_builder()` →
  `.stream::<K,V>([..]).map_values(..).to(..)` → `app.run(topology).await`.
- In-process broker test pattern: `Broker::start(BrokerConfig::for_tests(dir))`
  then `broker.listen_addr()`. Enabled for non-broker crates via the
  `crabka-broker/test-helpers` dev-dependency facade.
- In-process Schema Registry over a real HTTP port:
  `KafkaStore::start(&RegistryConfig{ bootstrap, schemas_topic, schemas_topic_rf,
  client_id, advertised_url, group_id, leader_eligibility, security }, cancel)`
  → `rest::router(AppState { store })` → `serve::serve_http(TcpListener::bind(
  "127.0.0.1:0"), app, cancel)`; point `RegistryClient::new("http://<addr>")` at it.
- Cargo examples have access to `[dev-dependencies]`, so a self-contained example
  can boot its own broker + registry.
- `crates/docgen` already generates markdown reference pages and is run by
  `.github/workflows/docs.yml` (`cargo run -p crabka-docgen -- all --out
  website/content/reference`) before the Zola site build. CI has an existing
  `drift` job pattern for generated-artifact checks.

## Design

### A. The worked pipeline

Order events flow through five topics, one format at each edge:

```
orders.json   --JSON Schema-->  Stage A  --Protobuf-->  orders.proto
orders.proto  --Protobuf----->  Stage B  --Arrow IPC--> orders.arrow
orders.arrow  --Arrow IPC----->  Stage C (Polars group-by) --Polars DataFrame--> (in-proc)
(agg rows)    --------------->  Stage D  --Protobuf-->  orders.summary
```

Types:

- `OrderEvent` — plain Rust struct (`serde::{Serialize,Deserialize}` +
  `schemars::JsonSchema`) for the JSON stage. Fields: `order_id: String`,
  `user: String`, `amount: f64`, `currency: String`, `ts_ms: i64`.
- `OrderProto`, `OrderSummary` — prost messages generated from a new
  `examples/proto/orders.proto`. `OrderProto`: `order_id`, `user`,
  `amount_cents: i64`, `currency`, `ts_ms`. `OrderSummary`: `user`,
  `total_cents: i64`, `order_count: i64`.

Stages:

- **A · JSON→proto** — `StreamsApp` DSL. Source `orders.json` as
  `JsonSerde<OrderEvent>`; `map_values` to `OrderProto` (normalize: amount→cents
  `(amount*100).round() as i64`, uppercase currency); sink `orders.proto` as
  `ProtobufSerde<OrderProto>`.
- **B · proto→arrow** — consume `OrderProto` from `orders.proto`, accumulate into
  Arrow columns, emit a `RecordBatch`, produce to `orders.arrow` via
  `ArrowIpcSerde`. Implemented as a Processor-API node (or a thin
  consume→produce bridge — implementer picks the cleaner; both are real).
- **C · arrow→polars** — `ColumnarTopology` with a small in-example
  `ArrowBlobCodec: BatchCodec` that decodes the `ArrowIpcSerde` record values to
  a Polars `DataFrame` via `polars-arrow`. Apply
  `BuiltinOp::GroupByAgg { keys: [col("user")], aggs: [col("amount_cents").sum()
  .alias("total_cents"), col("amount_cents").count().alias("order_count")] }`.
- **D · polars→summary proto** — convert each aggregated row to `OrderSummary`,
  produce to `orders.summary` via `ProtobufSerde<OrderSummary>`.

**Arrow↔Polars bridge risk:** handled explicitly in `ArrowBlobCodec` using
`polars-arrow` (already a `polars`-feature dep). We do **not** assume Arrow-IPC
and Polars-IPC byte formats are interchangeable.

### B. Single source of truth: one self-asserting example

`crates/client-streams/examples/format_pipeline.rs`
(`[[example]] required-features = ["polars", "arrow"]`):

- Boots in-process broker + in-process Schema Registry (real HTTP port) via
  dev-dependencies; creates the five topics; seeds `orders.json` with a small
  fixed set of `OrderEvent`s.
- Runs Stages A–D, reads `orders.summary`, and `assert!`s the per-user
  `total_cents` / `order_count` against the known input. Prints `format_pipeline:
  OK` on success; panics (non-zero exit) on failure. **Running it is the test.**
- Every teachable region wrapped in `// docs:begin <anchor>` / `// docs:end
  <anchor>` markers (anchors: `setup`, `stage-a-json-proto`, `stage-b-proto-arrow`,
  `stage-c-arrow-polars`, `stage-d-polars-proto`, `assert`).

Proto codegen mirrors the existing example: `examples/proto/orders.proto`,
committed `examples/gen/{orders.rs, file_descriptor_set.bin}`, and
`examples/gen/regenerate.sh`. The example does `include!("gen/orders.rs")` and
`include_bytes!("gen/file_descriptor_set.bin")`.

Small per-format reference examples (same anchor convention), each runnable and
self-checking where possible:

- `examples/format_json.rs` — `JsonSerde<OrderEvent>` round-trip (no broker).
- `examples/format_protobuf.rs` — `ProtobufSerde<OrderProto>` round-trip (seeded cache).
- `examples/format_arrow.rs` (`arrow`) — `ArrowIpcSerde` round-trip (no broker).
- Reuse the existing `examples/dataframe_serde.rs` (`polars`) for Polars IPC.

### C. Docs embed the tested code (drift-guarded)

New guide page `website/content/guide/streams.md` (`weight = 35`,
`template = "docs/page.html"`):

1. **What client-streams is** — KIP-1071 membership; two processing models (row
   Processor API / DSL vs columnar DataFrame topology); `TopologyTestDriver` /
   `ColumnarTestDriver` for broker-free tests.
2. **Data formats** — the serde/codec table (from "Current state"), with the
   feature-gate column and one-line "when to use".
3. **Getting started** — Cargo deps + feature flags; `set_default_registry` +
   `SchemaCache`/`RegistryClient`; declaring a `DefaultSerde`; building and
   running a topology.
4. **Worked pipeline** — an ASCII diagram of the five hops, then the embedded,
   tested snippets pulled from `format_pipeline.rs` by anchor.

Snippet mechanism — extend `crates/docgen`:

- Add a `snippets` operation (own subcommand, and folded into `all`). It scans
  `website/content/**/*.md` for blocks delimited by
  `<!-- snippet: <relpath>#<anchor> -->` … `<!-- /snippet -->`, where `<relpath>`
  is relative to `crates/`. It replaces the content between the markers with a
  fenced ```rust block containing the lines between the matching
  `// docs:begin <anchor>` / `// docs:end <anchor>` markers of the source file
  (markers stripped, common indentation trimmed).
- Committed markdown therefore always holds the real, current code (valid Zola).
- Idempotent: re-running on already-synced markdown is a no-op.

### D. Automated runner + CI

`tools/test-doc-examples.sh` (bash, `set -euo pipefail`):

1. `cargo build -p crabka-client-streams --examples --features polars,arrow`
2. Run each self-asserting example:
   `cargo run -p crabka-client-streams --example format_pipeline --features polars,arrow`
   (plus the per-format examples with their required features).
3. Drift guard: `cargo run -p crabka-docgen -- snippets` then
   `git diff --exit-code -- website/content` (fails if docs are stale).

New CI job `doc-examples` in `.github/workflows/ci.yml`:

- Gated via the existing `changes` filter on `crates/client-streams/**`,
  `crates/docgen/**`, `website/**`, and Cargo manifests.
- Standard Rust setup (matches existing jobs), then runs
  `tools/test-doc-examples.sh`.
- `timeout-minutes` ~30.

## Testing strategy

- **The example is the test** (per chosen approach): `format_pipeline` boots real
  broker + registry, runs the real produce/fetch/encode/decode path, and asserts
  the summary output. CI runs it.
- Per-format examples self-check via round-trip assertions.
- Snippet drift is a CI failure, guaranteeing docs == tested code.
- Existing `client-streams-integration` job is unaffected.

## Files touched

New:
- `crates/client-streams/examples/format_pipeline.rs`
- `crates/client-streams/examples/format_json.rs`
- `crates/client-streams/examples/format_protobuf.rs`
- `crates/client-streams/examples/format_arrow.rs`
- `crates/client-streams/examples/proto/orders.proto`
- `crates/client-streams/examples/gen/{orders.rs, file_descriptor_set.bin, regenerate.sh}`
- `website/content/guide/streams.md`
- `tools/test-doc-examples.sh`
- (CI) new `doc-examples` job in `.github/workflows/ci.yml`

Modified:
- `crates/client-streams/Cargo.toml` — `[[example]]` entries (+ required-features);
  dev-deps for in-process broker + schema registry + producer/consumer if missing.
- `crates/docgen/src/**` + its bin — add the `snippets` operation; fold into `all`.
- `.github/workflows/ci.yml` — `changes` filter outputs + the `doc-examples` job.

## Open implementation choices (left to the plan)

- Stage B as a Processor-API node vs a thin consume→produce bridge — pick the
  one that reads cleanest; both exercise the Arrow path for real.
- Whether `ArrowBlobCodec` lives inline in `format_pipeline.rs` (preferred, it is
  teaching material) vs a shared example module.
