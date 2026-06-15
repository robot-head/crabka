# Columnar / DataFrame support for `crabka-client-streams`

**Date:** 2026-06-14
**Status:** Approved design — in implementation
**Crate:** `crates/client-streams`

> **Implementation update (2026-06-14):** `minarrow` was dropped and **substituted
> with the mainstream stable [`arrow`](https://crates.io/crates/arrow) (arrow-rs)
> crate**. minarrow's transitive `vec64` dependency requires nightly Rust
> (`#![feature(allocator_api)]`), which is incompatible with Crabka's pinned stable
> toolchain (1.96.0) and `unsafe_code = "forbid"`. The Arrow-IPC serde is therefore
> `ArrowIpcSerde : Serde<arrow::array::RecordBatch>` rather than
> `MinarrowIpcSerde : Serde<minarrow::Table>`; everything else in this spec stands.
> References to "minarrow" below should be read as "arrow-rs `RecordBatch`".

## Summary

Add first-class support for three columnar/dataframe Rust libraries to the Kafka
Streams client:

- [`arrow`](https://crates.io/crates/arrow) (arrow-rs) — the mainstream Apache
  Arrow implementation (substituted for the originally-requested `minarrow`, which
  is nightly-only; see the implementation update above).
- [`columnar`](https://github.com/frankmcsherry/columnar) — zero-copy
  struct-of-arrays serialization (no Arrow dependency).
- [`polars`](https://github.com/pola-rs/polars) — a full DataFrame engine.

Two deliverables:

1. **Serde payloads (primary).** Library-native `Serde<T>` implementations so a
   record key/value can be a polars `DataFrame`, an arrow-rs `RecordBatch`, or any
   `columnar::Columnar` type. Usable immediately in the existing row topology.
2. **Native columnar topology (secondary, optional).** A polars-backed topology
   whose edges carry `DataFrame`s end-to-end, running on the existing KIP-1071 /
   `KafkaStreams` broker runtime, for analytical throughput. First cut: stateless
   transforms + within-batch aggregations.

All additions are behind off-by-default cargo features; the default build is
unchanged.

## Goals

- Let users move columnar/dataframe payloads through Kafka topics with idiomatic,
  per-library encodings.
- Provide a vectorized, batch-oriented processing path for analytical workloads
  without disturbing the existing row-at-a-time Processor API / DSL.
- Keep heavy dependencies (especially polars) strictly opt-in.
- Preserve Kafka wire-protocol exactness — records on the wire remain standard
  Kafka records.

## Non-goals (this spec)

- Cross-batch **stateful** columnar operators: state-store-backed aggregations,
  joins, and windows over the columnar topology. These are an explicit named
  follow-up project.
- Schema-registry framing of Arrow/columnar payloads (encodings are
  registry-free and self-describing or library-native).
- Backwards-compatibility shims — Crabka is greenfield and undeployed (see
  `CLAUDE.md`).

## Design decisions (resolved during brainstorming)

| Question | Decision |
|---|---|
| Integration point | `Serde<T>` payloads (primary) + optional columnar batch-processing API (secondary) |
| Feature gating | Three per-library opt-in features: `arrow`, `columnar`, `polars` |
| Serde wire format | Library-native each (polars IPC, arrow-rs Arrow IPC stream, columnar native bytes) |
| Batch API shape | Native columnar topology (batches flow along edges end-to-end) |
| Batch engine | polars `DataFrame` on the edges |
| Columnar runtime | Same broker runtime (KIP-1071 membership + `KafkaStreams`) |
| Operator scope (v1) | Stateless transforms + within-batch `group_by`/`agg` |
| Batch boundary model | Both, pluggable `BatchCodec` (row-assembly + blob impls) |

## Section 1 — Module & feature layout

Three independent, off-by-default cargo features:

```toml
[features]
arrow     = ["dep:arrow"]
columnar  = ["dep:columnar"]
polars    = ["dep:polars", "dep:polars-arrow"]   # also enables the columnar-topology engine
```

New module tree, all feature-gated:

```
src/columnar/
  mod.rs           # feature-gated re-exports
  serde/
    polars.rs      # PolarsIpcSerde   : Serde<DataFrame>          (cfg: polars)
    arrow.rs       # ArrowIpcSerde    : Serde<RecordBatch>        (cfg: arrow)
    columnar.rs    # ColumnarSerde<T> : Serde<T> for T: Columnar  (cfg: columnar)
  topology/        # native columnar topology                     (cfg: polars)
    mod.rs
    graph.rs       # batch-edge graph
    codec.rs       # BatchCodec trait + RowCodec / BlobCodec
    operator.rs    # ColumnarProcessor + built-in polars-expr operators
    driver.rs      # ColumnarTestDriver (in-process)
    runtime.rs     # bridge into the existing KIP-1071 / KafkaStreams runtime
```

The columnar **topology** lives under the `polars` feature only (it is
polars-backed). The `arrow` and `columnar` features add only their respective
serde. Enabling `polars` yields both `Serde<DataFrame>` and the batch engine.

## Section 2 — Serde payloads (primary deliverable)

Each library gets a library-native `Serde<T>` implementation that plugs into the
existing boundary (`src/processor/serde.rs`) unchanged.

| Serde | `Serde<T>` for | Wire bytes |
|---|---|---|
| `PolarsIpcSerde` | `polars::DataFrame` | Arrow IPC stream (polars `IpcWriter` / `IpcReader`) |
| `ArrowIpcSerde` | `arrow::array::RecordBatch` | Arrow IPC stream (arrow-rs `StreamWriter` / `StreamReader`) |
| `ColumnarSerde<T>` | `T: columnar::Columnar` | columnar's native zero-copy byte layout |

- All implement `Serde<T>` + `SerdeAssociate`.
- The unit serdes (`PolarsIpcSerde`, `ArrowIpcSerde`) also implement
  `Default` + `Clone` and a `DefaultSerde` impl, so `DataFrame` / `RecordBatch`
  work with the ergonomic `add_source` / `stream` path.
- `ColumnarSerde<T>` is generic, so it is opt-in per type — no blanket
  `DefaultSerde` impl (coherence). Users wire it explicitly via
  `add_source_explicit` / `with_value_serde`.
- `prepare()` is a no-op for all three (registry-free, self-describing or
  library-native).

These serdes are usable **today in the existing row topology**: a record's value
can simply *be* a whole `DataFrame` / `RecordBatch` / `Columnar` value. They are
independent of the native columnar topology.

### Arrow-rs IPC entry points

`ArrowIpcSerde` uses arrow-rs's stream IPC format (schema embedded per message):
write with `arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema)` →
`.write(&batch)` → `.finish()`; read with
`arrow::ipc::reader::StreamReader::try_new(&bytes[..], None)` (an iterator of
`Result<RecordBatch, ArrowError>`). Map `arrow::error::ArrowError` into
`SerdeError`. The public IPC API is entirely safe — no `unsafe` needed under
`unsafe_code = "forbid"`. (Resolved: arrow-rs `54.x`; `53.x` fails to build
against the workspace's chrono.)

## Section 3 — Native columnar topology (polars-backed)

### Shape

A parallel `ColumnarTopology` whose edges carry `polars::DataFrame` end-to-end.
It reuses the existing KIP-1071 membership + `KafkaStreams` runtime (group join,
partition assignment, offset commit, EOS); only the per-task processing engine
differs.

The runtime hands a **poll's worth of records for one assigned partition** to a
`BatchCodec`, runs the batch operator chain, and the sink codec turns output
DataFrames back into produce records. **The batch is the commit / transaction
boundary**, so EOS and offset semantics fall out of the existing runtime
unchanged.

### `BatchCodec` (pluggable, chosen per source/sink)

```rust
pub trait BatchCodec: Send + Sync + 'static {
    /// Assemble consumed records (in offset order) into one DataFrame.
    fn decode(&self, records: &[ConsumedRecord]) -> Result<DataFrame, BatchError>;
    /// Decompose an output DataFrame back into produce records.
    fn encode(&self, df: &DataFrame) -> Result<Vec<ProduceRecord>, BatchError>;
}
```

Two built-in implementations:

- **`BlobCodec`** — each record value is an Arrow-IPC DataFrame (via
  `PolarsIpcSerde`). `decode` `vstack`s the per-record DataFrames; `encode`
  writes the result as one IPC record, chunked if it exceeds `max.request.size`.
  Reuses the serde feature directly. Topics carry IPC blobs (not row-consumable
  by vanilla Kafka consumers).
- **`RowCodec<K, V>`** — records stay ordinary rows. `decode` deserializes each
  `(key, value)` via the inner key/value serdes into `Vec<(K, V)>`, then builds
  columns via a `serde_arrow`-style row→Arrow bridge (`K, V: Serialize +
  Deserialize`). `encode` reverses it. Topics stay standard-Kafka-consumable.
  This is the true vectorized analytical-throughput path.

### Reserved metadata columns

Every assembled DataFrame carries, alongside the payload columns:

- `__key` (binary)
- `__timestamp` (i64)
- `__partition` (i32)
- `__offset` (i64)

so the sink codec can faithfully reconstruct records (key, timestamp) and the
runtime can commit offsets. `BlobCodec` carries the partition/offset of the
*batch*; `RowCodec` carries per-row values. Payload column names never collide
with the `__`-prefixed reserved names (validated at topology build time).

### Operators (v1 scope: stateless + within-batch aggregation)

```rust
pub trait ColumnarProcessor: Send + 'static {
    /// Forwards 0..n DataFrames via `ctx`.
    fn process(&mut self, ctx: &mut ColumnarContext, batch: DataFrame)
        -> Result<(), BatchError>;
}
```

A thin DSL over polars exprs covers the common cases so users rarely hand-write
the trait:

```rust
let mut topo = ColumnarTopology::new();
let src = topo.add_source("src", ["txns"], RowCodec::<String, Txn>::default());
let agg = src
    .filter(col("amount").gt(lit(0)))
    .select([col("user"), col("amount")])
    .group_by([col("user")]).agg([col("amount").sum().alias("total")]); // within-batch only
topo.add_sink("out", "totals", BlobCodec::default(), [&agg]);
```

`group_by` / `agg` operate **within a single batch only** — no cross-batch
carryover. This is documented loudly. Cross-batch stateful columnar operators
(state-store-backed aggregations, joins, windows) are an explicit named
follow-up, out of scope for this spec.

## Section 4 — Testing & verification

- **Serde round-trip + golden bytes.** Each serde round-trips. For
  `PolarsIpcSerde` / `ArrowIpcSerde`, a golden test confirms the IPC bytes are
  readable by an independent Arrow reader (cross-library read), proving payloads
  are portable. `ColumnarSerde` golden-tests the native byte layout.
- **`ColumnarTestDriver`.** A broker-free analog of `TopologyTestDriver`:
  `pipe_input(topic, records)` → batch pipeline → `read_output(topic)`. Primary
  vehicle for operator logic and both codecs, including row↔column and blob
  round-trips and `__key` / `__timestamp` reconstruction.
- **Codec property tests.** `decode ∘ encode` round-trips preserve
  key / timestamp / payload for both `RowCodec` and `BlobCodec`.
- **Broker integration** (behind feature + `crabka-broker` test-helpers). A
  columnar topology runs against a live in-process broker, verifying offset
  commit at batch boundaries, EOS, and rebalance — reusing existing
  integration-test scaffolding.
- **Kafka wire exactness unaffected.** Records on the wire stay standard Kafka
  records; the IPC blob format is library-defined and golden-pinned. No protocol
  bytes change.
- **Examples.** `examples/polars_pipeline.rs` (row-codec analytics) and a
  `dataframe_serde` example, mirroring the existing per-format example
  convention.

## Open items to resolve during planning

1. **~~minarrow IPC availability~~** — resolved: minarrow is nightly-only and was
   substituted with arrow-rs `54.x` using its stream IPC API (Section 2).
2. **Row→Arrow bridge** — select the concrete mechanism for `RowCodec`
   (`serde_arrow` crate vs a hand-rolled bridge), and whether it targets
   `polars-arrow` directly.
3. **IPC chunking threshold** — how `BlobCodec::encode` splits oversized
   DataFrames against `max.request.size`.

## Dependencies added (all optional)

- `polars` (+ `polars-arrow`) — `polars` feature
- `arrow` (arrow-rs) — `arrow` feature
- `columnar` — `columnar` feature
- `serde_arrow` (or equivalent), if chosen for the row→Arrow bridge — `polars`
  feature

## Implementation sequencing (for the plan)

Per `CLAUDE.md`, batches of non-overlapping file sets dispatched in parallel:

1. **Serde features** (parallel — disjoint files): `polars.rs`, `arrow.rs`,
   `columnar.rs`, each with its own golden/round-trip tests. Cargo feature wiring.
2. **Columnar topology core**: `codec.rs` (`BatchCodec` + both codecs),
   `graph.rs`, `operator.rs`, `driver.rs`.
3. **Runtime bridge**: `runtime.rs` integration with KIP-1071 / `KafkaStreams`,
   broker integration tests.
4. **Examples + docs**: README section, `examples/`.
