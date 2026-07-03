# Newtype-Safety Rollout

Tracking document for applying the [Newtypes for Domain Values](style_guides/code_style_guide.md#newtypes-for-domain-values) rule across Crabka. It records what a workspace-wide survey found, what has been fixed, and the staged plan for the cross-crate core identifiers.

A survey of all 43 domain crates (excluding the generated protocol codec) found **232 newtype-safety findings** — places where two or more same-typed primitives (`i32`, `i64`, `u64`, `String`, …) with different meanings can be transposed at a call site and still compile. They split into two populations:

- **~94 High-confidence, crate-local findings** across ~28 crates. Each is safe to fix in isolation: the newtype and all its uses live inside one crate, so there is no cross-crate ripple and no wire-format risk. These are being fixed in verified batches (below).
- **~74 cross-crate findings** that resolve to a small set of **core Kafka identifiers** (`Offset`, `PartitionIndex`, `BrokerId`/`NodeId`, `LeaderEpoch`, `ProducerId`, `ApiKey`). These are the highest-value newtypes but they cross the generated wire boundary, so they are a **separate staged program** (below) — done one type at a time, never folded into a mechanical sweep, because Kafka byte-exactness is non-negotiable.

## Ground rules for every fix

- **Never newtype the generated codec** (`crates/protocol/generated`). Newtypes live in the hand-written domain layer and convert at the boundary via `From`/`Into`.
- **A `pub` struct/enum that is serialized** (serde JSON, a compacted Kafka topic value, a k8s CRD status, protobuf) must keep its byte/JSON shape **bit-identical**. Use `#[serde(transparent)]` on the newtype, or convert at the encode edge. This is the one way a "crate-local" fix can break a wire contract.
- **Split derives by origin**: std for `Copy, Clone, PartialEq, Eq, Hash` (+ `PartialOrd, Ord` when ordered); [`derive_more`](https://lib.rs/crates/derive_more) for `Display, From, Into` (+ `FromStr`, `Add`/`Sub` only where arithmetic is real). Every crate that gains a `derive_more`-deriving newtype adds `derive_more = { workspace = true }` to its `[dependencies]`.
- Define a crate's newtypes once in an `ids.rs` / `types.rs` module and thread them through.

## Crate-local fixes

Grouped into batches with non-overlapping file sets so each batch runs in parallel and is verified (`cargo check`/`clippy`/`test -p <crate>`) before the next.

### Batch 1 — cleanest leaf crates
`throttle` (`plan_consume` 4×`u64` — the textbook swap), `audit` (`Seq`/`EpochMs`/count types), `bench-driver` (metric/sample types), `connect` (`SourceOffset::new` two `OffsetMap`s), `connect-postgres` (`CommitLsn`/`TransactionId`/`RelationId`), `pprof` (`TimestampMs`/`Ticks`), `logql` (query-AST duration/label types), `records-legacy` (`Offset` in `ParsedRecord`).

### Batch 2 — observability stores
`blockstore`, `traces`, `traceql`, `profiles`, `promql`, `metrics`, `metrics-service`, `observability` — mostly `UnixNano`/`Offset`/timestamp pairs in store and query layers; several wrap serde/WAL structs and need `#[serde(transparent)]`.

### Batch 3 — services & remaining crate-local
`replicator` (offset-translation math — real correctness value), `rebalancer` (movement/proposal, convert at protobuf edge), `operator` (CRD status counts/versions, `#[serde(transparent)]`), `grpc-gateway`, `schema-registry` (`SchemaId` vs `SchemaVersion`), `remote-storage`(+`-topic`), `client-streams` (windowing/store — the densest single crate), `client-consumer`/`-core`/`-producer`/`-admin`, `security`, `kraft-core` (Raft offset trio), `cli` (`ClusterId`/`DirectoryId`), `kafka-tap`, and the crate-local subset of `broker` (`SessionId`) and `protocol` (`produce_passthrough` `TopicIndex`/`PartitionIndex`).

## Cross-crate core identifiers (staged program)

High-value but wire-crossing. Each is a staged rollout: **define the canonical newtype in an owner crate → add `From`/`Into` at the generated-codec boundary → convert consumers crate-by-crate, leaf crates last.** One type at a time; do not interleave.

| Rank | Newtype | Owner | Recurrence | Blast radius |
| :--- | :--- | :--- | :--- | :--- |
| 1 | `Offset(i64)` (+`Add`/`Sub`) | `protocol`, re-exported via `log` | offset-family in **18 crates** | Very large |
| 2 | `PartitionIndex(i32)` | `protocol` | **14 crates** | Very large (travels with `Offset` as the `(partition, offset)` pair) |
| 3 | `BrokerId` / unify `NodeId` | `metadata` / `voters` | 5 crates; **two colliding `NodeId=u64` aliases** | Large — this is a rename+merge, not just a wrap |
| 4 | `LeaderEpoch(i32)` (+ distinct `OffsetEpoch`) | `protocol`/`metadata` | ~7 crates | Medium-large |
| 5 | `ProducerId(i64)` | `protocol` | 4 crates | Medium (ships with record-batch field newtypes) |
| 6 | `ApiKey(i16)` / `ApiVersion(i16)` | `protocol`/`client-core` | client + tap | Medium — small, header-local; a good warm-up |

Recommended order: `Offset` → `PartitionIndex` → `NodeId`/`BrokerId` collision cleanup → `LeaderEpoch`/`OffsetEpoch` → `ProducerId` → `ApiKey`/`ApiVersion`. The generated codec stays raw throughout; conversions live in the hand-written `owned.rs`/`borrowed.rs` and per-request domain types.

## Explicitly dropped (Low value / not swappable)

`protocol/records/header.rs` `RecordBatchHeader` (zerocopy, generated-adjacent — the conversion site, never newtyped), `blockstore/reader.rs` `RowGroupMeta` (`u64` vs `usize` — already distinct types), single-key maps with no adjacent same-type field, and other lone primitives with no confusable sibling in scope.
