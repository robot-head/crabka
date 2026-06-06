//! KIP-1071 Kafka Streams rebalance-protocol client.
//!
//! Sub-project #1 of the Crabka Streams runtime: a [`StreamsMembership`] joins a
//! *streams group* via `StreamsGroupHeartbeat` (API key 88), maintains
//! membership with a background heartbeat, and surfaces assigned active/standby/
//! warmup tasks. The [`topology`] module builds a Processor-API topology and
//! serializes it byte-for-byte to the JVM 4.x wire shape.
//!
//! Processors are *structural placeholders* here — record processing arrives in
//! a later sub-project. See
//! `docs/superpowers/specs/2026-06-03-kip-1071-streams-client-membership-design.md`.
//!
//! ## Quick start
//!
//! ```no_run
//! use std::time::Duration;
//! use crabka_client_streams::{Consumed, Produced, StreamsEvent, StreamsMembership, StringSerde, Topology};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut topo = Topology::new();
//! let src = topo.add_source("src", ["input-topic"], Consumed::with(StringSerde, StringSerde));
//! topo.add_sink("snk", "output-topic", [&src], Produced::with(StringSerde, StringSerde));
//! let built = topo.build("my-application-id")?;
//!
//! let mut membership = StreamsMembership::builder()
//!     .bootstrap("localhost:9092")
//!     .group_id("my-application-id")
//!     .topology(std::sync::Arc::new(built))
//!     .build()
//!     .await?;
//!
//! loop {
//!     match membership.next_event().await? {
//!         StreamsEvent::Assigned(a) => {
//!             for task in &a.active {
//!                 println!("active task {} → {:?}", task.subtopology_id, task.source_topic_partitions);
//!             }
//!         }
//!         StreamsEvent::NotReady(statuses) => println!("not ready: {statuses:?}"),
//!         StreamsEvent::Fenced => println!("rejoined after fence"),
//!     }
//! }
//! # }
//! ```
//! ## Processor API (sub-project #2)
//!
//! Define a typed topology, then test it with the broker-free [`TopologyTestDriver`]:
//!
//! ```
//! use async_trait::async_trait;
//! use crabka_client_streams::{Consumed, Processor, ProcessorContext, Produced, Record, StringSerde, Topology, TopologyTestDriver};
//!
//! struct Upper;
//! #[async_trait]
//! impl Processor<String, String, String, String> for Upper {
//!     async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, String, String>, r: Record<String, String>) {
//!         ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
//!     }
//! }
//!
//! let mut topo = Topology::new();
//! let src = topo.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
//! let up = topo.add_processor("up", || Upper, [&src]);
//! topo.add_sink("out", "out", [&up], Produced::with(StringSerde, StringSerde));
//! let built = topo.build("my-app").unwrap();
//!
//! let mut driver = TopologyTestDriver::new(&built).unwrap();
//! driver.pipe_input("in", Consumed::with(StringSerde, StringSerde), Some("k".to_string()), "hello".to_string(), 0);
//! assert_eq!(
//!     driver.read_output("out", Produced::with(StringSerde, StringSerde)),
//!     Some((Some("k".to_string()), "HELLO".to_string())),
//! );
//! ```
//!
//! Nodes are wired by handle, not by string name, so a mis-typed edge is a
//! **compile error** rather than a `build()`-time failure:
//!
//! ```compile_fail
//! use crabka_client_streams::{Consumed, I64Serde, Produced, StringSerde, Topology};
//!
//! let mut topo = Topology::new();
//! // `src` produces Record<String, String>:
//! let src = topo.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
//! // but this sink expects Record<String, i64> — won't compile:
//! topo.add_sink("out", "out", [&src], Produced::with(StringSerde, I64Serde));
//! ```
//!
//! ## State stores (sub-project #3)
//!
//! Processors can persist and restore keyed state via a named [`KeyValueStore`].
//! The store is attached to the topology with `add_state_store`, and accessed
//! inside `process` via [`ProcessorContext::get_state_store`].
//!
//! ```
//! use async_trait::async_trait;
//! use crabka_client_streams::{
//!     Consumed, I64Serde, Processor, ProcessorContext, Produced, Record, StringSerde, Topology,
//!     TopologyTestDriver,
//! };
//!
//! struct Counter;
//! #[async_trait]
//! impl Processor<String, String, String, i64> for Counter {
//!     async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, String, i64>, r: Record<String, String>) {
//!         let n = {
//!             let s = ctx.get_state_store::<String, i64>("counts").unwrap();
//!             let n = s.get(&r.value).await.unwrap_or(0) + 1;
//!             s.put(r.value.clone(), n).await;
//!             n
//!         };
//!         ctx.forward(Record::new(Some(r.value), n, r.timestamp));
//!     }
//! }
//!
//! let mut topo = Topology::new();
//! let src = topo.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
//! let c = topo.add_processor("c", || Counter, [&src]);
//! topo.add_state_store("counts", StringSerde, I64Serde, [c.name()]);
//! topo.add_sink("out", "out", [&c], Produced::with(StringSerde, I64Serde));
//! let built = topo.build("app").unwrap();
//!
//! let mut driver = TopologyTestDriver::new(&built).unwrap();
//! driver.pipe_input("in", Consumed::with(StringSerde, StringSerde), None, "a".to_string(), 0);
//! driver.pipe_input("in", Consumed::with(StringSerde, StringSerde), None, "a".to_string(), 1);
//! assert_eq!(
//!     driver.read_output("out", Produced::with(StringSerde, I64Serde)),
//!     Some((Some("a".to_string()), 1_i64)),
//! );
//! assert_eq!(
//!     driver.read_output("out", Produced::with(StringSerde, I64Serde)),
//!     Some((Some("a".to_string()), 2_i64)),
//! );
//! assert_eq!(driver.store_get::<String, i64>("counts", &"a".to_string()), Some(2_i64));
//! ```
//!
//! ## DSL (KStream/KTable)
//!
//! [`StreamsBuilder`] is the high-level DSL entry point. It wires a topology
//! from source streams through stateless transforms, aggregations, and sinks
//! without writing explicit [`Processor`] implementations. The resulting
//! [`BuiltTopology`] is interchangeable with the Processor-API variant — run it
//! with [`TopologyTestDriver`] for broker-free testing or [`KafkaStreams`] for
//! production.
//!
//! A [`KTable`] is internally a *change stream*: each record carries a
//! `Change { old_value, new_value }` and `filter` emits tombstones (a row whose
//! key stops matching is deleted downstream with `new_value = None`).
//! [`KStream::to_table`] materializes a stream into a [`KTable`] backed by a
//! named [`Materialized`] store. Note: [`KTable::to_stream`] forwards update
//! records and drops tombstones from the output stream (full null-value output
//! is deferred to a future slice).
//!
//! [`KStream::join_table`] and [`KStream::left_join_table`] join a stream against
//! a **materialized** `KTable`: the stream side drives, and for each record the
//! table store is looked up by key. An inner join emits only when the table has a
//! matching entry; a left join always emits (with `None` as the table value when
//! absent). The stream must be **copartitioned** with the table (same key serde
//! and partition count); a key-changing stream must be `.repartition(..)`-ed
//! before joining — the join itself inserts no implicit repartition. (The plain
//! [`KStream::join`]/[`KStream::left_join`] names are the windowed *stream-stream*
//! join below — Rust can't overload by argument type as the JVM does.)
//!
//! [`KTable::join`], [`KTable::left_join`], and [`KTable::outer_join`] join two
//! **materialized** `KTables`. Unlike the stream-table join, a change on *either*
//! side recomputes the join: the changed side re-reads the other side's current
//! value from its store and forwards a `Change` (a tombstone when the joined row
//! stops existing). Inner emits only when both sides hold a value; left emits
//! whenever the left side is present; outer emits whenever either side is. The two
//! source topics are declared as a **copartition group**, and the result is an
//! unmaterialized `KTable` (no result store/changelog — materialize a downstream op
//! to persist it).
//!
//! [`KGroupedStream::windowed_by`] turns a grouped stream into time-windowed
//! aggregations: `windowed_by(TimeWindows::of_size(..))` then `count`/`reduce`/
//! `aggregate` yields a [`KTable`]`<`[`Windowed`]`<K>, V>`. [`TimeWindows`] are
//! tumbling (`of_size`) or hopping (`.advance_by(..)`); each record is aggregated
//! into every window it falls into, and a result is emitted on **every update**
//! (window closing / grace / `suppress` are deferred). The windowed store is a
//! [`Window`]-keyed store over the same pluggable backend, with a `compact,delete`
//! changelog (`retention.ms = size + grace + 1 day`). Read the windowed output
//! with [`TimeWindowedSerde`] (the key carries the window start).
//!
//! [`KGroupedStream::windowed_by_session`] groups records into data-driven
//! **session windows**: records for a key form one session `[start, end]` while
//! they stay within an inactivity [`SessionWindows`] gap. Terminal `count` /
//! `reduce` / `aggregate` (the last taking a session merger) yield a
//! [`KTable`]`<`[`Windowed`]`<K>, V>`. Each record merges every session within the
//! gap into one `[minStart, maxEnd]` session — emitting a tombstone for each
//! merged-away session and the new merged session (KIP session semantics,
//! emit-on-update). The session store keys by `key‖end‖start` (a third typed store
//! over the pluggable backend); read the output with [`SessionWindowedSerde`].
//!
//! [`KTable::suppress`]`(`[`Suppressed`]`::until_window_closes(`[`BufferConfig`]`::unbounded()))`
//! turns a windowed table's emit-on-update change-stream into **final results**: it
//! buffers each window's updates and forwards the window's final value exactly once,
//! when stream-time passes `window.end + grace` (the grace comes from the upstream
//! windowed/session aggregation). [`Suppressed::until_time_limit`] is the
//! rate-limiter variant for *any* table — it emits at most one update per key per
//! wait (stream-time), a newer record resetting the timer.
//!
//! The buffer is a **registered, durable state store** (a time-ordered
//! `SuppressBytesStore` keyed by the serialized record key). With logging on (the
//! default) it writes a **JVM-byte-exact** changelog — `BufferValue` +
//! `ProcessorRecordContext` value, a plain `cleanup.policy=compact` topic
//! `app-KTABLE-SUPPRESS-STATE-STORE-<n>-changelog` — and restores the buffered
//! records on restart via the same machinery as every other store, so windows that
//! were still buffered re-emit on close after a restart. [`Suppressed::with_logging_disabled`]
//! keeps the buffer in memory only (no changelog topic). The serdes reach the store
//! from the producing op (the windowed/session aggregation or [`StreamsBuilder::table`]).
//!
//! The buffer is bounded by [`BufferConfig`]: [`BufferConfig::unbounded`]`().with_max_records(n)`
//! / [`BufferConfig::with_max_bytes`]`(n)` cap it (bytes = serialized key + value
//! summed); exceeding a cap either shuts the task down (`shutDownWhenFull`, the
//! `until_window_closes` default) or — with `BufferConfig::max_records(n)` /
//! [`BufferConfig::max_bytes`] (eager) / [`BufferConfig::emit_early_when_full`] —
//! evicts + emits the oldest buffered record (`emitEarlyWhenFull`).
//!
//! [`KStream::join`], [`KStream::left_join`], and [`KStream::outer_join`] are the
//! windowed **stream-stream** joins: two streams join over a [`JoinWindows`] time
//! window, configured with [`StreamJoined`] serdes. Each side buffers its records
//! in its own `retainDuplicates` window store (so two records at the same time
//! both survive); a record from one side joins every record on the other side
//! within `[t - before, t + after]`, emitting `joiner(a, b)` at `max(ts)`. The two
//! window-store changelogs use `cleanup.policy=delete` (`retention.ms = before +
//! after + grace + 1 day`), and the two source topics form a copartition group. An
//! inner join emits only on a match; **left**/**outer** additionally emit the
//! null-padded result for a record that finds no match, once its window has closed
//! (KIP-633 stream-time-driven emission — there is no wall-clock throttle). Left/
//! outer buffer the as-yet-unmatched records in a shared `KSTREAM-OUTERSHARED-`
//! KV store (a compact changelog) and rename their per-side processors to
//! `KSTREAM-OUTERTHIS-`/`KSTREAM-OUTEROTHER-` to match the JVM. As with the other
//! joins, a key-changing stream must `.repartition(..)` before joining.
//!
//! [`StreamsBuilder::global_table`] sources a [`GlobalKTable`]: a **fully-replicated**
//! lookup table. Every application instance reads *all* partitions of the source
//! topic into one shared global store, so the source topic itself is the truth —
//! there is **no copartitioning, no repartition, and no changelog** (the global
//! store is rebuilt from the source on startup). The store is *invisible in the
//! wire topology* (no subtopology of its own), though its global source node still
//! consumes a node-group index during grouping (so declaring `global_table` before
//! `stream` shifts the stream subtopology id). [`KStream::join_global`] /
//! [`KStream::left_join_global`] join a stream to it by a **per-record-derived
//! key** — `key_mapper(&streamKey, &streamValue)` selects the global key (which may
//! differ from the stream key) — and emit `joiner(&streamValue, &globalValue)` keyed
//! by the *stream* key. An inner `join_global` skips a record on a store miss; a
//! `left_join_global` always emits, passing `None` for the global side. Because the
//! store is fully replicated, any record can look up any key on every instance.
//! The runtime's global consumer **bootstraps** the store — draining every partition
//! of the source topic to end-of-log — *before* any task begins processing, so the
//! first joined record already sees the complete global table.
//!
//! [`KStream::process`] and [`KStream::process_values`] (KIP-820) drop a custom
//! Processor-API node into a DSL pipeline: a user-written [`Processor`] (for
//! `process`) or [`FixedKeyProcessor`] (for `process_values`) that reads and writes
//! state stores connected by name. Register the store first with
//! [`StreamsBuilder::add_state_store`] (a compact-changelog [`KeyValueStore`]), then
//! pass its name to the `process`/`process_values` call that uses it — the named
//! store is attached to that node and its `app-<store>-changelog` topic appears in
//! the wire. `process` may rewrite the record key, so its result is
//! **key-changing**: a downstream `group_by_key`/join inserts a repartition.
//! `process_values` is **fixed-key** — it can change the value but not the key — so
//! it carries the upstream key lineage and forces **no** repartition. That guarantee
//! is structural: a [`FixedKeyProcessor`] only ever receives and forwards a
//! [`FixedKeyRecord`], whose key is fixed from the input and preserved through
//! [`FixedKeyRecord::with_value`]; the context's only `forward` re-attaches that key,
//! so the processor cannot emit a different one. (An `add_state_store` store that no
//! `process`/`process_values` connects is simply never instantiated — no changelog,
//! no runtime store; an explicit unconnected-store build error is deferred.)
//!
//! ```
//! use crabka_client_streams::{
//!     Consumed, Grouped, I64Serde, Materialized, Produced, StreamsBuilder, StringSerde,
//!     TopologyTestDriver,
//! };
//!
//! // Build a word-count topology: group by key, count, forward to "out".
//! let b = StreamsBuilder::new();
//! b.stream(["in"], Consumed::with(StringSerde, StringSerde))
//!     .group_by_key(Grouped::with(StringSerde, StringSerde))
//!     .count(Materialized::with(StringSerde, I64Serde).as_store("counts"))
//!     .to_stream()
//!     .to("out", Produced::with(StringSerde, I64Serde));
//! let built = b.build("word-count").unwrap();
//!
//! // Drive it broker-free with TopologyTestDriver.
//! let mut driver = TopologyTestDriver::new(&built).unwrap();
//! for word in ["a", "a", "b"] {
//!     driver.pipe_input(
//!         "in",
//!         Consumed::with(StringSerde, StringSerde),
//!         Some(word.to_string()),
//!         word.to_string(),
//!         0,
//!     );
//! }
//!
//! // The stream output carries the running count per key.
//! assert_eq!(
//!     driver.read_output("out", Produced::with(StringSerde, I64Serde)),
//!     Some((Some("a".to_string()), 1)),
//! );
//! assert_eq!(
//!     driver.read_output("out", Produced::with(StringSerde, I64Serde)),
//!     Some((Some("a".to_string()), 2)),
//! );
//! assert_eq!(
//!     driver.read_output("out", Produced::with(StringSerde, I64Serde)),
//!     Some((Some("b".to_string()), 1)),
//! );
//!
//! // The materialized store holds the final count per key.
//! assert_eq!(driver.store_get::<String, i64>("counts", &"a".to_string()), Some(2));
//! assert_eq!(driver.store_get::<String, i64>("counts", &"b".to_string()), Some(1));
//! ```
//!
//! ## Running an app (`KafkaStreams`)
//!
//! Once built, run a topology against a broker with the managed runtime — it
//! joins the streams group, fetches its assigned partitions, processes records,
//! produces to sink topics, and commits offsets (at-least-once):
//!
//! ```no_run
//! use async_trait::async_trait;
//! use crabka_client_streams::{Consumed, KafkaStreams, Processor, ProcessorContext, Produced, Record, StringSerde, Topology};
//!
//! struct Upper;
//! #[async_trait]
//! impl Processor<String, String, String, String> for Upper {
//!     async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, String, String>, r: Record<String, String>) {
//!         ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
//!     }
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut topo = Topology::new();
//! let src = topo.add_source("src", ["input-topic"], Consumed::with(StringSerde, StringSerde));
//! let up = topo.add_processor("up", || Upper, [&src]);
//! topo.add_sink("out", "output-topic", [&up], Produced::with(StringSerde, StringSerde));
//! let built = topo.build("my-app")?;
//!
//! let mut streams = KafkaStreams::builder()
//!     .bootstrap("localhost:9092")
//!     .application_id("my-app")
//!     .topology(built)
//!     .build()
//!     .await?;
//! // ... app runs in the background; later:
//! streams.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## State stores & backends
//!
//! The execution path is **async**: [`Processor::process`](processor::Processor)
//! is an `async fn`, and a processor reads/writes its connected state store with
//! `ctx.get_state_store::<K, V>(name).get(&k).await` / `.put(k, v).await`.
//!
//! State stores are **pluggable** via a byte-level backend. A
//! [`KeyValueStore`] is a typed view ([`KeyValueBytesStore`]) over a backend
//! selected by [`StoreBackend`]: `InMemory` (a `BTreeMap`; the default and the
//! test backend) or `Turso` (a pure-Rust `SQLite` engine persisting under a state
//! dir, used by the managed runtime). The backend is a *materialized cache* — the
//! changelog topic is the source of truth, so on assignment the store is rebuilt
//! from the changelog (clean-slate replay), and a missing/corrupt local store is
//! recovered by replay rather than data loss. Select it on the builder:
//! `KafkaStreams::builder().store_backend(StoreBackend::Turso { state_dir })`.
#![doc(html_root_url = "https://docs.rs/crabka-client-streams/0.0.0")]

pub mod dsl;
mod error;
pub mod membership;
pub mod processor;
pub mod runtime;
pub mod store;
pub mod test_driver;
pub mod topology;

pub use dsl::{
    BranchedStream, BufferConfig, GlobalKTable, Grouped, JoinWindows, KGroupedStream, KTable,
    Materialized, Repartitioned, SessionWindowedSerde, SessionWindows, StreamJoined,
    StreamsBuilder, Suppressed, TimeWindowedSerde, TimeWindows, Window, Windowed,
};
pub use error::StreamsClientError;
pub use membership::{
    StreamsAssignment, StreamsEvent, StreamsMembership, StreamsStatus, TaskAssignment,
    TopicPartition,
};
pub use processor::{
    BytesSerde, Consumed, FixedKeyProcessor, FixedKeyProcessorContext, FixedKeyProcessorSupplier,
    FixedKeyRecord, I64Serde, Processor, ProcessorContext, ProcessorError, ProcessorSupplier,
    Produced, Record, RecordContext, Serde, SerdeError, StringSerde,
};
pub use runtime::{KafkaStreams, KafkaStreamsState};
pub use store::{KeyValueBytesStore, KeyValueStore, StateStore, StoreBackend};
pub use test_driver::TopologyTestDriver;
pub use topology::{BuiltTopology, NodeHandle, Topology, TopologyError};
