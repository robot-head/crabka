//! Kafka Streams-compatible client runtime for Crabka.
//!
//! `crabka-client-streams` has three layers. You can use each layer on its own:
//!
//! - [`StreamsBuilder`] builds JVM-compatible KStream/KTable topologies for
//!   common application code: map/filter chains, aggregations, joins, windows,
//!   suppression, global tables, and custom Processor-API nodes.
//! - [`Topology`] is the typed Processor API for applications that want explicit
//!   source, processor, sink, and state-store wiring.
//! - [`KafkaStreams`] runs a built topology against a Kafka-compatible broker. It
//!   joins a KIP-1071 streams group, processes the assigned input partitions,
//!   produces sink records, restores changelog-backed stores, and serves local
//!   interactive queries.
//!
//! For broker-free tests, [`TopologyTestDriver`] runs the same built topology
//! in process. The driver is the fastest way to exercise business logic and state
//! stores before you run them with [`KafkaStreams`].
//!
//! ## Quick start
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use crabka_client_streams::{NodeHandle, StreamsEvent, StreamsMembership, Topology};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut topo = Topology::new();
//! let src: NodeHandle<String, String> = topo.add_source("src", ["input-topic"]);
//! topo.add_sink("snk", "output-topic", [&src]);
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
//!                 println!(
//!                     "active task {} → {:?}",
//!                     task.subtopology_id, task.source_topic_partitions
//!                 );
//!             }
//!         }
//!         StreamsEvent::NotReady(statuses) => println!("not ready: {statuses:?}"),
//!         StreamsEvent::Fenced => println!("rejoined after fence"),
//!     }
//! }
//! # }
//! ```
//! ## Processor API
//!
//! Define a typed topology, then test it with the broker-free [`TopologyTestDriver`]:
//!
//! ```
//! use crabka_client_streams::{
//!     NodeHandle, Record, StringSerde, Topology, TopologyTestDriver, impl_processor,
//! };
//!
//! struct Upper;
//! impl_processor! {
//!     impl Upper: (String, String) -> (String, String) {
//!         async fn process(&mut self, ctx, r) {
//!             ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
//!         }
//!     }
//! }
//!
//! let mut topo = Topology::new();
//! let src: NodeHandle<String, String> = topo.add_source("src", ["in"]);
//! let up = topo.add_processor("up", || Upper, [&src]);
//! topo.add_sink("out", "out", [&up]);
//! let built = topo.build("my-app").unwrap();
//!
//! let mut driver = TopologyTestDriver::new(&built).unwrap();
//! driver.pipe_input(
//!     "in",
//!     (StringSerde, StringSerde),
//!     Some("k".to_string()),
//!     "hello".to_string(),
//!     0,
//! );
//! assert_eq!(
//!     driver.read_output("out", (StringSerde, StringSerde)),
//!     Some((Some("k".to_string()), "HELLO".to_string())),
//! );
//! ```
//!
//! You wire nodes by handle, not by string name, so a mis-typed edge is a
//! **compile error**, not a `build()`-time failure:
//!
//! ```compile_fail
//! use crabka_client_streams::{NodeHandle, Topology};
//!
//! let mut topo = Topology::new();
//! // `src` produces Record<String, String>:
//! let src: NodeHandle<String, String> = topo.add_source("src", ["in"]);
//! // but this sink expects Record<String, i64> — won't compile:
//! topo.add_sink::<String, i64>("out", "out", [&src]);
//! ```
//!
//! ## State stores
//!
//! Processors can persist and restore keyed state with a named [`KeyValueStore`].
//! Attach the store to the topology with `add_state_store`. Read and write it
//! inside `process` with [`ProcessorContext::get_state_store`].
//!
//! ```
//! use crabka_client_streams::{
//!     I64Serde, NodeHandle, Record, StringSerde, Topology, TopologyTestDriver, impl_processor,
//! };
//!
//! struct Counter;
//! impl_processor! {
//!     impl Counter: (String, String) -> (String, i64) {
//!         async fn process(&mut self, ctx, r) {
//!             let n = {
//!                 let s = ctx.get_state_store::<String, i64>("counts").unwrap();
//!                 let n = s.get(&r.value).await.unwrap_or(0) + 1;
//!                 s.put(r.value.clone(), n).await;
//!                 n
//!             };
//!             ctx.forward(Record::new(Some(r.value), n, r.timestamp));
//!         }
//!     }
//! }
//!
//! let mut topo = Topology::new();
//! let src: NodeHandle<String, String> = topo.add_source("src", ["in"]);
//! let c = topo.add_processor("c", || Counter, [&src]);
//! topo.add_state_store("counts", StringSerde, I64Serde, [c.name()]);
//! topo.add_sink("out", "out", [&c]);
//! let built = topo.build("app").unwrap();
//!
//! let mut driver = TopologyTestDriver::new(&built).unwrap();
//! driver.pipe_input("in", (StringSerde, StringSerde), None, "a".to_string(), 0);
//! driver.pipe_input("in", (StringSerde, StringSerde), None, "a".to_string(), 1);
//! assert_eq!(
//!     driver.read_output("out", (StringSerde, I64Serde)),
//!     Some((Some("a".to_string()), 1_i64)),
//! );
//! assert_eq!(
//!     driver.read_output("out", (StringSerde, I64Serde)),
//!     Some((Some("a".to_string()), 2_i64)),
//! );
//! assert_eq!(
//!     driver.store_get::<String, i64>("counts", &"a".to_string()),
//!     Some(2_i64)
//! );
//! ```
//!
//! ## DSL (KStream/KTable)
//!
//! [`StreamsBuilder`] is the high-level DSL entry point. It wires a topology
//! from source streams through stateless transforms, aggregations, and sinks,
//! and you write no explicit [`Processor`] implementations. The resulting
//! [`BuiltTopology`] is interchangeable with the Processor-API variant. Run it
//! with [`TopologyTestDriver`] for broker-free tests, or with [`KafkaStreams`]
//! in production.
//!
//! A [`KTable`] is internally a *change stream*. Each record carries a
//! `Change { old_value, new_value }`, and `filter` emits tombstones. When a row's
//! key no longer matches, the downstream side deletes that row with
//! `new_value = None`. [`KStream::to_table`] materializes a stream into a
//! [`KTable`] backed by a named [`Materialized`] store. [`KTable::to_stream`]
//! forwards update records and drops tombstones from the output stream.
//!
//! [`KStream::join_table`] and [`KStream::left_join_table`] join a stream against
//! a **materialized** `KTable`. The stream side drives the join, and each record
//! looks up the table store by key. An inner join emits only when the table holds
//! a matching entry. A left join always emits, and it supplies `None` as the
//! table value when the entry is absent.
//!
//! The stream must be **copartitioned** with the table, which means the same key
//! serde and the same partition count. Call `.repartition(..)` on a key-changing
//! stream before you join it, because the join itself inserts no implicit
//! repartition. The plain [`KStream::join`] and [`KStream::left_join`] names are
//! the windowed *stream-stream* join below. Rust cannot overload by argument type
//! as the JVM does.
//!
//! [`KTable::join`], [`KTable::left_join`], and [`KTable::outer_join`] join two
//! **materialized** `KTables`. A change on *either* side recomputes the join,
//! which is different from the stream-table join. The changed side re-reads the
//! other side's current value from its store and forwards a `Change`. That
//! `Change` is a tombstone when the joined row no longer exists.
//!
//! Inner emits only when both sides hold a value. Left emits whenever the left
//! side is present, and outer emits whenever either side is present. The DSL
//! declares the two source topics as a **copartition group**. The result is an
//! unmaterialized `KTable` with no result store and no changelog. Materialize a
//! downstream op to persist it.
//!
//! ## Foreign-key joins
//!
//! [`KTable::join_on_foreign_key`] and [`KTable::left_join_on_foreign_key`]
//! (KIP-213) join two `KTable`s on a **foreign key** instead of the primary key.
//! For each left row, an `fk_extractor(&leftValue)` selects the foreign key, and
//! that foreign key looks up a row in the right table. The relationship is
//! **many-to-one**: many left rows can reference the same right row.
//!
//! A change on *either* side re-evaluates every affected pair. A left-value
//! change re-selects the foreign key. A right-row change re-emits for every left
//! row that currently subscribes to that foreign key. **Inner** emits
//! `joiner(&left, &right)` only when the foreign row exists, and a foreign key
//! with no match retracts with a tombstone. **Left** emits for every left row,
//! and it supplies `None` for the foreign value on a miss.
//!
//! Both input tables must be **materialized source tables** built with
//! [`StreamsBuilder::table`], because the join reads each side's store and
//! serdes. The result is an **unmaterialized** `KTable` with no result store and
//! no changelog. Materialize a downstream op to persist it.
//!
//! The foreign key differs from the primary key, so the join cannot be
//! copartitioned directly. It lowers to the KIP-213 two-subtopology graph: a
//! *subscription registration* repartition topic keyed by foreign key, a
//! *subscription response* repartition topic keyed back by primary key, and a
//! subscription state store that tracks which primary keys subscribe to each
//! foreign key. The DSL creates and copartitions all three automatically.
//!
//! ```no_run
//! use crabka_client_streams::{StreamsBuilder, StringSerde};
//!
//! let builder = StreamsBuilder::new();
//! // `a`: primaryKey -> foreignKey ("A"); `b`: foreignKey -> value ("X").
//! let a = builder.table::<String, String>("a", "sa");
//! let b = builder.table::<String, String>("b", "sb");
//! a.join_on_foreign_key(
//!     &b,
//!     |left: &String| left.clone(), // foreign-key extractor
//!     |left: &String, right: &String| format!("{left}{right}"), // joiner -> "AX"
//!     StringSerde,                  // foreign-key serde
//! )
//! .to_stream()
//! .to("out");
//! drop(a);
//! drop(b);
//! let topology = builder.build("fk-app").unwrap();
//! # let _ = topology;
//! ```
//!
//! [`KGroupedStream::windowed_by`] turns a grouped stream into time-windowed
//! aggregations. `windowed_by(TimeWindows::of_size(..))` followed by
//! `count`/`reduce`/`aggregate` yields a [`KTable`]`<`[`Windowed`]`<K>, V>`.
//! [`TimeWindows`] are tumbling with `of_size`, or hopping with
//! `.advance_by(..)`. The aggregator adds each record to every window the record
//! falls into, and it emits a result on **every update**. Add
//! [`KTable::suppress`] with [`Suppressed::until_window_closes`] when the
//! application wants one final result after the window closes.
//!
//! The windowed store is a [`Window`]-keyed store over the same pluggable
//! backend. Its changelog is `compact,delete` with
//! `retention.ms = size + grace + 1 day`. Read the windowed output with
//! [`TimeWindowedSerde`]. The key carries the window start.
//!
//! [`KGroupedStream::windowed_by_session`] groups records into data-driven
//! **session windows**. Records for a key form one session `[start, end]` while
//! they stay within an inactivity [`SessionWindows`] gap. The terminal `count`,
//! `reduce`, and `aggregate` operators yield a
//! [`KTable`]`<`[`Windowed`]`<K>, V>`, and `aggregate` also takes a session
//! merger. Each record merges every session within the gap into one
//! `[minStart, maxEnd]` session. The operator then emits a tombstone for each
//! merged-away session, plus the new merged session. This is the KIP session
//! semantics, emit-on-update.
//!
//! The session store keys by `key‖end‖start`, and it is a third typed store over
//! the pluggable backend. Read the output with [`SessionWindowedSerde`].
//!
//! [`KGroupedStream::windowed_by_sliding`] produces a
//! [`SlidingWindowedKGroupedStream`] with `count`/`reduce`/`aggregate`. Sliding
//! windows are **data-defined** inclusive windows of fixed size
//! `time_difference`. A record at time `t` falls into every window
//! `[ws, ws + time_difference]` with `ws ∈ [t - time_difference, t]`. There is no
//! epoch alignment, which is different from tumbling and hopping windows. The
//! aggregator finds the affected windows when it scans the window store, and it
//! emits on update.
//!
//! The aggregator folds an out-of-order record within `time_difference + grace`
//! into the windows the record belongs to. The output is a
//! `KTable<Windowed<K>, _>`. It reuses the [`TimeWindowedSerde`] output-key
//! layout, `key‖windowStart:8B-BE`.
//!
//! [`KTable::suppress`]`(`[`Suppressed`]`::until_window_closes(`[`BufferConfig`]`::unbounded()))`
//! turns a windowed table's emit-on-update change-stream into **final results**.
//! It buffers each window's updates. It then forwards the window's final value
//! exactly once, when stream-time passes `window.end + grace`. The grace comes
//! from the upstream windowed or session aggregation.
//! [`Suppressed::until_time_limit`] is the rate-limiter variant for *any* table.
//! It emits at most one update per key per wait in stream-time, and a newer
//! record resets the timer.
//!
//! The buffer is a **registered, durable state store**: a time-ordered
//! `SuppressBytesStore` keyed by the serialized record key. Logging is on by
//! default, and the buffer then writes a **JVM-byte-exact** changelog. That
//! changelog holds a `BufferValue` plus a `ProcessorRecordContext` value, in a
//! plain `cleanup.policy=compact` topic named
//! `app-KTABLE-SUPPRESS-STATE-STORE-<n>-changelog`. The buffer restores its
//! records on restart through the same machinery as every other store, so a
//! window that was still buffered re-emits on close after a restart.
//!
//! [`Suppressed::with_logging_disabled`] keeps the buffer in memory only, with no
//! changelog topic. The serdes reach the store from the op that produces the
//! table: the windowed or session aggregation, or [`StreamsBuilder::table`].
//!
//! [`BufferConfig`] bounds the buffer.
//! [`BufferConfig::unbounded`]`().with_max_records(n)` and
//! [`BufferConfig::with_max_bytes`]`(n)` cap it, where bytes is the sum of the
//! serialized key and value. A buffer that exceeds a cap shuts the task down
//! (`shutDownWhenFull`), which is the `until_window_closes` default. The eager
//! configs `BufferConfig::max_records(n)`, [`BufferConfig::max_bytes`], and
//! [`BufferConfig::emit_early_when_full`] instead evict and emit the oldest
//! buffered record (`emitEarlyWhenFull`).
//!
//! [`KStream::join`], [`KStream::left_join`], and [`KStream::outer_join`] are the
//! windowed **stream-stream** joins. Two streams join over a [`JoinWindows`] time
//! window, and [`StreamJoined`] serdes configure them. Each side buffers its
//! records in its own `retainDuplicates` window store, so two records at the same
//! time both survive. A record from one side joins every record on the other side
//! within `[t - before, t + after]`, and it emits `joiner(a, b)` at `max(ts)`.
//!
//! The two window-store changelogs use `cleanup.policy=delete` with
//! `retention.ms = before + after + grace + 1 day`, and the two source topics
//! form a copartition group. An inner join emits only on a match. **Left** and
//! **outer** also emit the null-padded result for a record that finds no match,
//! once that record's window has closed. This is the KIP-633 stream-time-driven
//! emission, and there is no wall-clock throttle.
//!
//! Left and outer buffer the records that are not yet matched in a shared
//! `KSTREAM-OUTERSHARED-` KV store with a compact changelog. They also rename
//! their per-side processors to `KSTREAM-OUTERTHIS-` and `KSTREAM-OUTEROTHER-` to
//! match the JVM. A key-changing stream must call `.repartition(..)` before it
//! joins, as with the other joins.
//!
//! [`StreamsBuilder::global_table`] sources a [`GlobalKTable`], a
//! **fully-replicated** lookup table. Every application instance reads *all*
//! partitions of the source topic into one shared global store, so the source
//! topic itself is the truth. There is **no copartitioning, no repartition, and
//! no changelog**, and the runtime rebuilds the global store from the source at
//! startup.
//!
//! The store is *invisible in the wire topology* and has no subtopology of its
//! own. Its global source node still consumes a node-group index during the
//! grouping step, so a `global_table` declared before `stream` shifts the stream
//! subtopology id.
//!
//! [`KStream::join_global`] and [`KStream::left_join_global`] join a stream to the
//! global table by a **per-record-derived key**.
//! `key_mapper(&streamKey, &streamValue)` selects the global key, which may
//! differ from the stream key. The join emits
//! `joiner(&streamValue, &globalValue)` keyed by the *stream* key. An inner
//! `join_global` skips a record on a store miss. A `left_join_global` always
//! emits, and it supplies `None` for the global side.
//!
//! The store is fully replicated, so any record can look up any key on every
//! instance. The runtime's global consumer **bootstraps** the store *before* any
//! task starts to process records. The consumer drains every partition of the
//! source topic to end-of-log, so the first joined record already sees the
//! complete global table.
//!
//! [`KStream::process`] and [`KStream::process_values`] (KIP-820) drop a custom
//! Processor-API node into a DSL pipeline. You write a [`Processor`] for
//! `process`, or a [`FixedKeyProcessor`] for `process_values`, and it reads and
//! writes state stores connected by name. Register the store first with
//! [`StreamsBuilder::add_state_store`], which gives a [`KeyValueStore`] with a
//! compact changelog. Then pass the store name to the `process` or
//! `process_values` call that uses it. The builder attaches the named store to
//! that node, and the store's `app-<store>-changelog` topic appears in the wire.
//!
//! `process` may rewrite the record key, so its result is **key-changing**, and a
//! downstream `group_by_key` or join inserts a repartition. `process_values` is
//! **fixed-key**: it can change the value but not the key. It carries the
//! upstream key lineage and forces **no** repartition.
//!
//! That guarantee is structural. A [`FixedKeyProcessor`] only ever receives and
//! forwards a [`FixedKeyRecord`]. The input fixes the key of that record, and
//! [`FixedKeyRecord::with_value`] preserves the key. The context's only `forward`
//! re-attaches that key, so the processor cannot emit a different one. An
//! `add_state_store` store that no `process` or `process_values` connects is never
//! instantiated, and it gets no changelog and no runtime store.
//!
//! ### Enriching a stream with a fully replicated table
//!
//! `GlobalKTable` is useful for reference data such as customer profiles,
//! product catalogs, or fraud watchlists. Every app instance can then look up any
//! key, and the stream needs no repartition:
//!
//! ```
//! use crabka_client_streams::{StreamsBuilder, StringSerde};
//!
//! let b = StreamsBuilder::new();
//! let customers = b.global_table::<String, String>("customers", "customers-by-id");
//!
//! b.stream::<String, String>(["orders"])
//!     .left_join_global(
//!         &customers,
//!         |_order_id, order| order.split(':').next().unwrap_or("").to_string(),
//!         |order, customer| format!("{order}|customer={}", customer.map_or("unknown", |v| v)),
//!     )
//!     .to("enriched-orders");
//!
//! drop(customers);
//! let built = b.build("orders-enricher").unwrap();
//! assert_eq!(built.list_source_topics(), vec!["orders".to_string()]);
//! ```
//!
//! The same enrichment works with **Avro** payloads and compound types. Declare
//! each type's default serde once. The DSL then reads and writes Confluent-framed
//! records that it resolves against the schema registry, with no per-call serde
//! wiring:
//!
//! ```
//! use apache_avro::AvroSchema;
//! use crabka_client_streams::{DefaultSerde, SchemaSerde, StreamsBuilder};
//! use crabka_schema_serde::{
//!     RegistryClient,
//!     cache::{CacheConfig, SchemaCache},
//!     format::avro::AvroSerde,
//!     set_default_registry,
//! };
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Serialize, Deserialize, AvroSchema)]
//! struct Order {
//!     order_id: String,
//!     customer_id: String,
//!     amount_cents: i64,
//! }
//! #[derive(Clone, Serialize, Deserialize, AvroSchema)]
//! struct Customer {
//!     customer_id: String,
//!     name: String,
//!     tier: Tier,
//!     region: String,
//! }
//! #[derive(Clone, Copy, Serialize, Deserialize, AvroSchema)]
//! enum Tier {
//!     Standard,
//!     Gold,
//!     Platinum,
//! }
//! #[derive(Clone, Serialize, Deserialize, AvroSchema)]
//! struct EnrichedOrder {
//!     order_id: String,
//!     customer: String,
//!     tier: Tier,
//!     amount_cents: i64,
//! }
//!
//! impl DefaultSerde for Order {
//!     type Serde = SchemaSerde<Order, AvroSerde<Order>>;
//! }
//! impl DefaultSerde for Customer {
//!     type Serde = SchemaSerde<Customer, AvroSerde<Customer>>;
//! }
//! impl DefaultSerde for EnrichedOrder {
//!     type Serde = SchemaSerde<EnrichedOrder, AvroSerde<EnrichedOrder>>;
//! }
//!
//! // Point the default serdes at a registry (not contacted until the app runs).
//! set_default_registry(SchemaCache::new(
//!     RegistryClient::new("http://localhost:8081"),
//!     CacheConfig::default(),
//! ));
//!
//! let b = StreamsBuilder::new();
//! let customers = b.global_table::<String, Customer>("customers", "customers-by-id");
//! b.stream::<String, Order>(["orders"])
//!     .left_join_global(
//!         &customers,
//!         |_order_key, order| order.customer_id.clone(),
//!         |order, customer| EnrichedOrder {
//!             order_id: order.order_id.clone(),
//!             customer: customer.map_or_else(|| "unknown".into(), |c| c.name.clone()),
//!             tier: customer.map_or(Tier::Standard, |c| c.tier),
//!             amount_cents: order.amount_cents,
//!         },
//!     )
//!     .to("enriched-orders");
//! drop(customers);
//! let built = b.build("orders-enricher-avro").unwrap();
//! assert_eq!(built.list_source_topics(), vec!["orders".to_string()]);
//! ```
//!
//! ### Final windowed counts
//!
//! Windowed aggregations emit on every update by default. Add `suppress` when
//! downstream systems should receive only the final value after the window grace
//! has passed:
//!
//! ```
//! use crabka_client_streams::{BufferConfig, StreamsBuilder, Suppressed, TimeWindows};
//! use crabka_units::prelude::*;
//!
//! let b = StreamsBuilder::new();
//! b.stream::<String, String>(["clicks"])
//!     .group_by_key()
//!     .windowed_by(TimeWindows::of_size(minutes(1)).grace(secs(10)))
//!     .count("click-counts")
//!     .suppress(Suppressed::until_window_closes(BufferConfig::unbounded()))
//!     .to_stream()
//!     .to("click-counts-final");
//!
//! let built = b.build("click-analytics").unwrap();
//! assert_eq!(
//!     built.list_sink_topics(),
//!     vec!["click-counts-final".to_string()]
//! );
//! ```
//!
//! The same windowed aggregation over **Avro** orders builds a compound
//! per-window revenue record. The aggregation state is itself an Avro record in
//! the windowed store:
//!
//! ```
//! use apache_avro::AvroSchema;
//! use crabka_client_streams::{
//!     BufferConfig, DefaultSerde, SchemaSerde, StreamsBuilder, Suppressed, TimeWindows,
//! };
//! use crabka_schema_serde::{
//!     RegistryClient,
//!     cache::{CacheConfig, SchemaCache},
//!     format::avro::AvroSerde,
//!     set_default_registry,
//! };
//! use crabka_units::prelude::*;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Serialize, Deserialize, AvroSchema)]
//! struct Order {
//!     order_id: String,
//!     region: String,
//!     amount_cents: i64,
//! }
//! #[derive(Clone, Serialize, Deserialize, AvroSchema)]
//! struct Revenue {
//!     order_count: i64,
//!     gross_cents: i64,
//! }
//!
//! impl DefaultSerde for Order {
//!     type Serde = SchemaSerde<Order, AvroSerde<Order>>;
//! }
//! impl DefaultSerde for Revenue {
//!     type Serde = SchemaSerde<Revenue, AvroSerde<Revenue>>;
//! }
//!
//! set_default_registry(SchemaCache::new(
//!     RegistryClient::new("http://localhost:8081"),
//!     CacheConfig::default(),
//! ));
//!
//! let b = StreamsBuilder::new();
//! b.stream::<String, Order>(["orders"]) // keyed by region
//!     .group_by_key()
//!     .windowed_by(TimeWindows::of_size(minutes(1)).grace(secs(10)))
//!     .aggregate(
//!         || Revenue {
//!             order_count: 0,
//!             gross_cents: 0,
//!         },
//!         |_region, order, acc| Revenue {
//!             order_count: acc.order_count + 1,
//!             gross_cents: acc.gross_cents + order.amount_cents,
//!         },
//!         "revenue-by-window",
//!     )
//!     .suppress(Suppressed::until_window_closes(BufferConfig::unbounded()))
//!     .to_stream()
//!     .to("revenue-per-window");
//! let built = b.build("revenue-analytics").unwrap();
//! assert_eq!(
//!     built.list_sink_topics(),
//!     vec!["revenue-per-window".to_string()]
//! );
//! ```
//!
//! ```
//! use crabka_client_streams::{I64Serde, StreamsBuilder, StringSerde, TopologyTestDriver};
//!
//! // Build a word-count topology: group by key, count, forward to "out".
//! let b = StreamsBuilder::new();
//! b.stream::<String, String>(["in"])
//!     .group_by_key()
//!     .count("counts")
//!     .to_stream()
//!     .to("out");
//! let built = b.build("word-count").unwrap();
//!
//! // Drive it broker-free with TopologyTestDriver.
//! let mut driver = TopologyTestDriver::new(&built).unwrap();
//! for word in ["a", "a", "b"] {
//!     driver.pipe_input(
//!         "in",
//!         (StringSerde, StringSerde),
//!         Some(word.to_string()),
//!         word.to_string(),
//!         0,
//!     );
//! }
//!
//! // The stream output carries the running count per key.
//! assert_eq!(
//!     driver.read_output("out", (StringSerde, I64Serde)),
//!     Some((Some("a".to_string()), 1)),
//! );
//! assert_eq!(
//!     driver.read_output("out", (StringSerde, I64Serde)),
//!     Some((Some("a".to_string()), 2)),
//! );
//! assert_eq!(
//!     driver.read_output("out", (StringSerde, I64Serde)),
//!     Some((Some("b".to_string()), 1)),
//! );
//!
//! // The materialized store holds the final count per key.
//! assert_eq!(
//!     driver.store_get::<String, i64>("counts", &"a".to_string()),
//!     Some(2)
//! );
//! assert_eq!(
//!     driver.store_get::<String, i64>("counts", &"b".to_string()),
//!     Some(1)
//! );
//! ```
//!
//! ### Applied: projecting Avro records
//!
//! A stateless projection over compound **Avro** types keeps the paid orders and
//! bills each one into a summary. One Avro record per topic carries a nested
//! `Vec` of line items, an `Option`, and an enum:
//!
//! ```
//! use apache_avro::AvroSchema;
//! use crabka_client_streams::{DefaultSerde, SchemaSerde, StreamsBuilder};
//! use crabka_schema_serde::{
//!     RegistryClient,
//!     cache::{CacheConfig, SchemaCache},
//!     format::avro::AvroSerde,
//!     set_default_registry,
//! };
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Serialize, Deserialize, AvroSchema)]
//! struct Order {
//!     order_id: String,
//!     status: OrderStatus,
//!     lines: Vec<LineItem>,
//!     coupon: Option<String>,
//! }
//! #[derive(Clone, Serialize, Deserialize, AvroSchema)]
//! struct LineItem {
//!     sku: String,
//!     quantity: i32,
//!     unit_price_cents: i64,
//! }
//! #[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, AvroSchema)]
//! enum OrderStatus {
//!     Placed,
//!     Paid,
//!     Shipped,
//!     Cancelled,
//! }
//! #[derive(Clone, Serialize, Deserialize, AvroSchema)]
//! struct OrderSummary {
//!     order_id: String,
//!     item_count: i64,
//!     total_cents: i64,
//! }
//!
//! impl DefaultSerde for Order {
//!     type Serde = SchemaSerde<Order, AvroSerde<Order>>;
//! }
//! impl DefaultSerde for OrderSummary {
//!     type Serde = SchemaSerde<OrderSummary, AvroSerde<OrderSummary>>;
//! }
//!
//! set_default_registry(SchemaCache::new(
//!     RegistryClient::new("http://localhost:8081"),
//!     CacheConfig::default(),
//! ));
//!
//! let b = StreamsBuilder::new();
//! b.stream::<String, Order>(["orders"])
//!     .filter(|_id, o| o.status == OrderStatus::Paid)
//!     .map_values(|o: &Order| OrderSummary {
//!         order_id: o.order_id.clone(),
//!         item_count: i64::try_from(o.lines.len()).unwrap_or(i64::MAX),
//!         total_cents: o
//!             .lines
//!             .iter()
//!             .map(|l| i64::from(l.quantity) * l.unit_price_cents)
//!             .sum(),
//!     })
//!     .to("order-summaries");
//! let built = b.build("order-billing").unwrap();
//! assert_eq!(
//!     built.list_sink_topics(),
//!     vec!["order-summaries".to_string()]
//! );
//! ```
//!
//! ## Punctuation (timers)
//!
//! A Processor-API node can register **punctuators**, which are periodic
//! callbacks. Register them with
//! [`ProcessorContext::schedule`]`(interval, `[`PunctuationType`]`, `[`Punctuator`]`)`,
//! usually from `init`. A [`Punctuator`] is a trait object like [`Processor`]. On
//! each fire it receives a `ProcessorContext` positioned at the node that
//! scheduled it, so it can `forward(...)` records downstream and read and write
//! state stores. Share mutable state with the owning processor through
//! `Arc<Mutex<_>>`. `schedule` returns a [`Cancellable`], and `.cancel()` stops
//! the schedule.
//!
//! Two clocks drive the firing:
//!
//! - [`PunctuationType::StreamTime`] uses the task's observed max record
//!   timestamp. It is deterministic and advances as you pipe records.
//! - [`PunctuationType::WallClockTime`] uses the system clock between polls. In
//!   tests, [`TopologyTestDriver::advance_wall_clock_time`] drives it.
//!
//! Both clocks fire **at most once per driving action**, and they pass the
//! **current** stream-time or wall-clock time to `punctuate`. A schedule that has
//! fallen more than one interval behind resyncs ahead. It does not replay every
//! missed boundary. A stream-time schedule first-fires on the first record. A
//! wall-clock schedule first-fires one interval after you scheduled it.
//!
//! Punctuation is invisible in the wire topology, because it is purely runtime
//! behavior. These semantics match the JVM `TopologyTestDriver`.
//!
//! ```
//! use std::time::Duration;
//!
//! use async_trait::async_trait;
//! use crabka_client_streams::{
//!     I64Serde, NodeHandle, Processor, ProcessorContext, PunctuationType, Punctuator, Record,
//!     StringSerde, Topology, TopologyTestDriver,
//! };
//!
//! // A punctuator that forwards the fire timestamp downstream.
//! struct Emit;
//! #[async_trait]
//! impl Punctuator<String, i64> for Emit {
//!     async fn punctuate(&mut self, ctx: &mut ProcessorContext<'_, '_, String, i64>, ts: i64) {
//!         ctx.forward(Record::new(None, ts, ts));
//!     }
//! }
//! // A processor that schedules `Emit` every 10ms of stream-time (and drops records).
//! struct Scheduler;
//! #[async_trait]
//! impl Processor<String, String, String, i64> for Scheduler {
//!     async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, String, i64>) {
//!         ctx.schedule(Duration::from_millis(10), PunctuationType::StreamTime, Emit);
//!     }
//!     async fn process(
//!         &mut self,
//!         _ctx: &mut ProcessorContext<'_, '_, String, i64>,
//!         _r: Record<String, String>,
//!     ) {
//!     }
//! }
//!
//! let mut topo = Topology::new();
//! let src: NodeHandle<String, String> = topo.add_source("src", ["in"]);
//! let p = topo.add_processor("p", || Scheduler, [&src]);
//! topo.add_sink("out", "out", [&p]);
//! let built = topo.build("app").unwrap();
//!
//! let mut driver = TopologyTestDriver::new(&built).unwrap();
//! // Stream-time advances with each record's timestamp; the punctuator fires once per
//! // crossed 10ms boundary, stamped with the CURRENT stream-time (5 is skipped).
//! for ts in [0_i64, 5, 10] {
//!     driver.pipe_input(
//!         "in",
//!         (StringSerde, StringSerde),
//!         Some("k".to_string()),
//!         "v".to_string(),
//!         ts,
//!     );
//! }
//! assert_eq!(
//!     driver.read_output("out", (StringSerde, I64Serde)),
//!     Some((None, 0_i64))
//! );
//! assert_eq!(
//!     driver.read_output("out", (StringSerde, I64Serde)),
//!     Some((None, 10_i64))
//! );
//! assert_eq!(driver.read_output("out", (StringSerde, I64Serde)), None);
//! ```
//!
//! ## Running an app (`KafkaStreams`)
//!
//! Run a built topology against a broker with the managed runtime. The runtime
//! joins the streams group, fetches its assigned partitions, processes records,
//! produces to sink topics, and commits offsets at-least-once:
//!
//! ```no_run
//! use async_trait::async_trait;
//! use crabka_client_streams::{
//!     KafkaStreams, NodeHandle, Processor, ProcessorContext, Record, Topology,
//! };
//!
//! struct Upper;
//! #[async_trait]
//! impl Processor<String, String, String, String> for Upper {
//!     async fn process(
//!         &mut self,
//!         ctx: &mut ProcessorContext<'_, '_, String, String>,
//!         r: Record<String, String>,
//!     ) {
//!         ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
//!     }
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut topo = Topology::new();
//! let src: NodeHandle<String, String> = topo.add_source("src", ["input-topic"]);
//! let up = topo.add_processor("up", || Upper, [&src]);
//! topo.add_sink("out", "output-topic", [&up]);
//! let built = topo.build("my-app")?;
//!
//! let streams = KafkaStreams::builder()
//!     .bootstrap("localhost:9092")
//!     .application_id("my-app")
//!     .topology(built)
//!     .build()
//!     .await?;
//! // The app runs in the background until it is closed.
//! streams.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## State stores & backends
//!
//! The execution path is **async**. [`Processor::process`](processor::Processor)
//! is an `async fn`, and a processor reads and writes its connected state store
//! with `ctx.get_state_store::<K, V>(name).get(&k).await` and `.put(k, v).await`.
//!
//! State stores are **pluggable** through a byte-level backend. A
//! [`KeyValueStore`] is a typed view, [`KeyValueBytesStore`], over a backend that
//! [`StoreBackend`] selects. `InMemory` is a `BTreeMap`, and it is both the
//! default and the test backend. `Turso` is a pure-Rust `SQLite` engine that
//! persists under a state dir, and the managed runtime uses it.
//!
//! The backend is a *materialized cache*, and the changelog topic is the source
//! of truth. On assignment the runtime rebuilds the store from the changelog, a
//! clean-slate replay. A missing or corrupt local store is recovered by replay,
//! so there is no data loss. Select the backend on the builder:
//! `KafkaStreams::builder().store_backend(StoreBackend::Turso { state_dir })`.
//!
//! ## Interactive Queries
//!
//! Read a running instance's local state stores from outside the topology with
//! [`KafkaStreams::key_value_store`], [`KafkaStreams::window_store`], and
//! [`KafkaStreams::session_store`]. Each one returns a typed, read-only view:
//! [`ReadOnlyKeyValueStore`], [`ReadOnlyWindowStore`], or
//! [`ReadOnlySessionStore`]. The accessors of these views round-trip through the
//! running supervisor:
//!
//! ```no_run
//! # use crabka_client_streams::{KafkaStreams, StringSerde, I64Serde};
//! # async fn example(streams: KafkaStreams) -> Result<(), Box<dyn std::error::Error>> {
//! let counts = streams
//!     .key_value_store("counts", StringSerde, I64Serde)
//!     .await?;
//! let n: Option<i64> = counts.get(&"alice".to_string()).await?;
//! let top = counts.range(&"a".to_string(), &"m".to_string()).await?;
//! let total = counts.approximate_num_entries().await?;
//! # let _ = (n, top, total);
//! # Ok(())
//! # }
//! ```
//!
//! Queries reach only the **local active** stores. Each query is a composite read
//! across every partition this instance owns, which matches the JVM default
//! `StoreQueryParameters`. [`ReadOnlyKeyValueStore`] exposes `get`, `range`
//! (inclusive), `all`, and `approximate_num_entries`. [`ReadOnlyWindowStore`]
//! exposes `fetch_single` and `fetch`. [`ReadOnlySessionStore`] exposes `fetch`.
//!
//! A failure surfaces as [`StreamsClientError::InteractiveQuery`] that wraps an
//! [`IqError`]. [`IqError::StoreNotFound`] means no such store is assigned here.
//! [`IqError::WrongStoreKind`] means the caller queried the wrong store kind.
//! [`IqError::RebalanceInProgress`] means no tasks are assigned yet, so retry.
//!
//! ## Exactly-once (EOS v2)
//!
//! [`ProcessingGuarantee`] sets the runtime's delivery guarantee.
//! [`AtLeastOnce`](ProcessingGuarantee::AtLeastOnce) is the default: it produces,
//! then commits the source offsets, and a crash mid-cycle may replay.
//! [`ExactlyOnceV2`](ProcessingGuarantee::ExactlyOnceV2) is the KIP-447
//! `exactly_once_v2` guarantee.
//!
//! Under EOS-v2 the [`StreamThread`] runs **one Kafka transaction per commit
//! interval** over a single transactional producer with
//! `transactional.id = <application.id>-<thread>`. The thread `begin`s the txn,
//! produces sink **and** changelog records into it, commits the consumed source
//! offsets *inside* the same transaction with `send_offsets_to_transaction`, and
//! then `commit`s. So output, changelog, and offsets land atomically.
//!
//! On any error in the cycle the thread `abort`s, rewinds the source offsets, and
//! rolls back the state stores. It wipes each store and re-restores it from the
//! **committed** changelog (`read_committed`). State-store restore under EOS
//! reads `read_committed`, so the runtime never replays aborted changelog writes.
//!
//! `OffsetFetch` surfaces the committed source offsets once the transaction's
//! COMMIT marker lands. A restarted instance then resumes from the committed
//! offset and rebuilds its stores from the committed changelog. The runtime
//! processes the committed input **exactly once across the restart**. It does not
//! re-read or double-count that input.
//!
//! [`StreamThread`]: runtime
//!
//! ```no_run
//! use crabka_client_streams::{KafkaStreams, NodeHandle, ProcessingGuarantee, Topology};
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut topo = Topology::new();
//! let src: NodeHandle<String, String> = topo.add_source("src", ["in"]);
//! topo.add_sink("out", "out", [&src]);
//! let built = topo.build("my-app")?;
//!
//! // Opt into exactly-once: output + changelog + source offsets commit atomically.
//! let streams = KafkaStreams::builder()
//!     .bootstrap("localhost:9092")
//!     .application_id("my-app")
//!     .topology(built)
//!     .processing_guarantee(ProcessingGuarantee::ExactlyOnceV2)
//!     .build()
//!     .await?;
//! streams.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Versioned tables (KIP-889)
//!
//! `builder.table(..., Materialized::as_versioned(name, history_retention))`
//! materializes a table into a versioned key-value store. The store records
//! out-of-order records as historical versions and keeps the latest version
//! intact. `get_as_of` gives point-in-time reads.
#![doc(html_root_url = "https://docs.rs/crabka-client-streams/0.3.9")]

pub mod columnar;
pub mod dsl;
mod error;
pub mod membership;
pub mod processor;
pub mod runtime;
pub mod store;
pub mod streams_app;
pub mod test_driver;
pub mod topology;

#[doc(hidden)]
pub use async_trait::async_trait as __async_trait;
pub use crabka_client_core::ClientDnsTimeout;
pub use dsl::{
    BranchedStream, BufferConfig, CogroupedKStream, GlobalKTable, Grouped, JoinWindows, Joined,
    KGroupedStream, KStream, KTable, Materialized, Repartitioned, SessionWindowedCogroupedStream,
    SessionWindowedKGroupedStream, SessionWindowedSerde, SessionWindows,
    SlidingWindowedCogroupedStream, SlidingWindowedKGroupedStream, SlidingWindows, StreamJoined,
    StreamsBuilder, Suppressed, TimeWindowedCogroupedStream, TimeWindowedKGroupedStream,
    TimeWindowedSerde, TimeWindows, VersionedConfig, Window, Windowed,
};
pub use error::StreamsClientError;
pub use membership::{
    DEFAULT_STREAMS_JOIN_RETRY_BACKOFF, DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT,
    DEFAULT_STREAMS_REBALANCE_TIMEOUT, SchemaPrewarm, StreamsAssignment, StreamsEvent,
    StreamsJoinRetryBackoff, StreamsLeaveHeartbeatTimeout, StreamsMembership,
    StreamsRebalanceTimeout, StreamsStatus, TaskAssignment, TaskOffsetTracker, TopicPartition,
};
pub use processor::{
    BytesSerde, Cancellable, Consumed, DefaultSerde, FixedKeyProcessor, FixedKeyProcessorContext,
    FixedKeyProcessorSupplier, FixedKeyRecord, I64Serde, Processor, ProcessorContext,
    ProcessorError, ProcessorSupplier, Produced, PunctuationType, Punctuator, Record,
    RecordContext, Serde, SerdeError, StringSerde, schema_serde::SchemaSerde,
};
pub use runtime::{
    DEFAULT_STREAMS_COMMIT_INTERVAL, DEFAULT_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY,
    DEFAULT_STREAMS_POLL_INTERVAL, DEFAULT_STREAMS_STATE_STORE_CACHE_MAX_BYTES, KafkaStreams,
    MAX_STREAMS_STATE_STORE_CACHE_MAX_BYTES, ReadOnlyKeyValueStore, ReadOnlySessionStore,
    ReadOnlyWindowStore, StreamsCommitInterval, StreamsInteractiveQueryQueueCapacity,
    StreamsPollInterval, StreamsStateStoreCacheMaxBytes,
    eos::ProcessingGuarantee,
    iq::IqError,
    iqv2::{
        FailureReason, KeyQuery, MultiVersionedKeyQuery, Position, PositionBound, Query,
        QueryResult, RangeQuery, StateQuery, StateQueryRequest, StateQueryResult,
        VersionedKeyQuery, WindowKeyQuery, WindowRangeQuery,
    },
};
pub use store::{
    KeyValueBytesStore, KeyValueStore, StateStore, StoreBackend, iq::StoreKind,
    versioned::VersionedRecord,
};
pub use streams_app::StreamsApp;
pub use test_driver::TopologyTestDriver;
pub use topology::{BuiltTopology, NodeHandle, Topology, TopologyError};
