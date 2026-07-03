# Newtype-Safety Rollout

Tracking document for applying the [Newtypes for Domain Values](style_guides/code_style_guide.md#newtypes-for-domain-values) rule across Crabka. It records what a workspace-wide survey found, what has been fixed, and the staged plan for the cross-crate core identifiers.

## Status

Crate-local fixes are being applied in verified batches — each brought to a green `cargo clippy --workspace --all-targets -- -D warnings` and passing tests before the next. **Done so far:** throttle, audit, bench-driver, connect, logql (batch 1, + downstream adaptations in connect-postgres, replicator, observability); cli, grpc-gateway, metrics-service, profiles, traces (batch 2); schema-registry, operator, traceql, promql (batch 3, + a metrics-service bridge); connect-postgres, replicator (batch 4). The cross-crate core identifiers below remain a staged program.

**Observed at the batch boundary:** promql and metrics-service each grew their own crate-local `Offset`/`PartitionIndex`, and they meet at `apply_wal_record_at`, forcing a `.0.into()` bridge through the raw primitive. That friction is the concrete argument for treating `Offset`/`PartitionIndex` (and the other core ids) as a *single* shared type owned by one crate rather than duplicating them.

### Cross-crate program — progress

The shared-type foundation is landed:

- **`crabka-ids`** now defines the canonical `Offset(i64)` and `PartitionIndex(i32)` — a zero-IO, WASM-buildable leaf crate depending only on `derive_more` + `serde`, so even the observability stack (which does not depend on `crabka-protocol`) can name a Kafka offset without a raw integer.
- The four scattered crate-local copies (grpc-gateway, metrics-service, promql, replicator) were **unified** onto it, removing the `.0.into()` bridge.
- Adopted directly in the Kafka-WAL offset consumers whose formats are verified by their own `cargo test` (observability, metrics).
- Adopted in `records-legacy` (`ParsedRecord.offset`), the first **wire-format** crate: the v0/v1 `MessageSet` bytes are held byte-identical (unwrap `.0` at every `put_i64`), verified by the crate's round-trip tests and broker's legacy-produce tests.

The full protocol **differential suite passes against the JVM oracle** (675 tests, 0 failures, 0 ignored) with everything above in place — so the shared-type work so far has preserved Kafka wire byte-exactness. That oracle (`tools/oracle` built with a JDK 17; set `JAVA_HOME`) plus Docker/testcontainers is the gate the wire-facing core rollout below must run under.

**Wire-facing core — in progress**, dependency-ordered, each crate verified in isolation before the next:

- **`log`** ✅ — fully converted (~300 sites). On-disk index/segment/txn-index/leader-epoch-checkpoint bytes and the v2 wire format held byte-identical (raw `i64` at every disk/`RecordBatch` boundary); proven by 149 unit + proptest round-trips. Re-exports `crabka_log::Offset` for consumers.
- **`raft`** ✅ — fully converted at the `KraftLog` facade + controller (~150 sites). The pure consensus core `crabka-kraft-core` stays `i64` (model-checked, WASM-buildable); raft wraps at that boundary and at the KIP-595 wire/snapshot boundaries. Proven by 153 unit + 28 integration/**stateright model-check** tests.
- **`broker`** ✅ — adopted `Offset` at the log/raft boundary (~45 seam sites: partition writer, fetch, remote-log-manager, list-offsets, …). Broker keeps offsets `i64` internally for now; wire response fields stay `i64`. Proven by 1277 unit tests.

The remaining pieces — broker's *internal* offset conversion (~250 sites), the storage/records consumers, and broker-side `PartitionIndex` — continue the same dependency-ordered, byte-exact process. `crabka-kraft-core` and `crabka-protocol`'s `RecordBatch` stay raw as the model-check / wire boundary. Byte-exactness is checked by each crate's own on-disk/consensus tests plus the **JVM differential oracle** (`tools/oracle`, JDK 17, `JAVA_HOME`) and Docker/testcontainers — noting that a differential run rewrites the tracked corpus fixtures (`crates/protocol/tests/corpus/`), which must be restored, not committed.

---

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
| 1 | `Offset(i64)` (+`+ i64`) | **`crabka-ids`** ✅ landed; adopted in observability, metrics, and the 4 unified crates | offset-family in **18 crates** | Very large (wire-facing core pending, needs the differential oracle) |
| 2 | `PartitionIndex(i32)` | **`crabka-ids`** ✅ landed | **14 crates** | Very large (travels with `Offset`; wire-facing core pending) |
| 3 | `BrokerId` / unify `NodeId` | `metadata` / `voters` | 5 crates; **two colliding `NodeId=u64` aliases** | Large — this is a rename+merge, not just a wrap |
| 4 | `LeaderEpoch(i32)` (+ distinct `OffsetEpoch`) | `protocol`/`metadata` | ~7 crates | Medium-large |
| 5 | `ProducerId(i64)` | `protocol` | 4 crates | Medium (ships with record-batch field newtypes) |
| 6 | `ApiKey(i16)` / `ApiVersion(i16)` | `protocol`/`client-core` | client + tap | Medium — small, header-local; a good warm-up |

Recommended order: `Offset` → `PartitionIndex` → `NodeId`/`BrokerId` collision cleanup → `LeaderEpoch`/`OffsetEpoch` → `ProducerId` → `ApiKey`/`ApiVersion`. The generated codec stays raw throughout; conversions live in the hand-written `owned.rs`/`borrowed.rs` and per-request domain types.

## Explicitly dropped (Low value / not swappable)

`protocol/records/header.rs` `RecordBatchHeader` (zerocopy, generated-adjacent — the conversion site, never newtyped), `blockstore/reader.rs` `RowGroupMeta` (`u64` vs `usize` — already distinct types), single-key maps with no adjacent same-type field, and other lone primitives with no confusable sibling in scope.
