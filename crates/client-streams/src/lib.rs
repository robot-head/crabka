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
//! use crabka_client_streams::{Consumed, Processor, ProcessorContext, Produced, Record, StringSerde, Topology, TopologyTestDriver};
//!
//! struct Upper;
//! impl Processor<String, String, String, String> for Upper {
//!     fn process(&mut self, ctx: &mut ProcessorContext<String, String>, r: Record<String, String>) {
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
//! use crabka_client_streams::{
//!     Consumed, I64Serde, Processor, ProcessorContext, Produced, Record, StringSerde, Topology,
//!     TopologyTestDriver,
//! };
//!
//! struct Counter;
//! impl Processor<String, String, String, i64> for Counter {
//!     fn process(&mut self, ctx: &mut ProcessorContext<String, i64>, r: Record<String, String>) {
//!         let s = ctx.get_state_store::<String, i64>("counts").unwrap();
//!         let n = s.get(&r.value).unwrap_or(0) + 1;
//!         s.put(r.value.clone(), n);
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
//! let store = driver.get_key_value_store::<String, i64>("counts").unwrap();
//! assert_eq!(store.get(&"a".to_string()), Some(2_i64));
//! ```
//!
//! ## Running an app (`KafkaStreams`)
//!
//! Once built, run a topology against a broker with the managed runtime — it
//! joins the streams group, fetches its assigned partitions, processes records,
//! produces to sink topics, and commits offsets (at-least-once):
//!
//! ```no_run
//! use crabka_client_streams::{Consumed, KafkaStreams, Processor, ProcessorContext, Produced, Record, StringSerde, Topology};
//!
//! struct Upper;
//! impl Processor<String, String, String, String> for Upper {
//!     fn process(&mut self, ctx: &mut ProcessorContext<String, String>, r: Record<String, String>) {
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
#![doc(html_root_url = "https://docs.rs/crabka-client-streams/0.0.0")]

pub mod dsl;
mod error;
pub mod membership;
pub mod processor;
pub mod runtime;
pub mod store;
pub mod test_driver;
pub mod topology;

pub use dsl::{Grouped, Materialized, Repartitioned};
pub use error::StreamsClientError;
pub use membership::{
    StreamsAssignment, StreamsEvent, StreamsMembership, StreamsStatus, TaskAssignment,
    TopicPartition,
};
pub use processor::{
    BytesSerde, Consumed, I64Serde, Processor, ProcessorContext, ProcessorError, ProcessorSupplier,
    Produced, Record, RecordContext, Serde, SerdeError, StringSerde,
};
pub use runtime::{KafkaStreams, KafkaStreamsState};
pub use store::{InMemoryKeyValueStore, KeyValueStore, StateStore};
pub use test_driver::TopologyTestDriver;
pub use topology::{BuiltTopology, NodeHandle, Topology, TopologyError};
