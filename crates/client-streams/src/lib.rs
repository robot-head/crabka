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
//! use crabka_client_streams::{StreamsEvent, StreamsMembership, StringSerde, Topology};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut topo = Topology::new();
//! topo.add_source("src", ["input-topic"], StringSerde, StringSerde);
//! topo.add_sink("snk", "output-topic", ["src"], StringSerde, StringSerde);
//! let built = topo.build("my-application-id")?;
//!
//! let mut membership = StreamsMembership::builder()
//!     .bootstrap("localhost:9092")
//!     .group_id("my-application-id")
//!     .topology(built)
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
//! use crabka_client_streams::{Processor, ProcessorContext, Record, StringSerde, Topology, TopologyTestDriver};
//!
//! struct Upper;
//! impl Processor<String, String, String, String> for Upper {
//!     fn process(&mut self, ctx: &mut ProcessorContext<String, String>, r: Record<String, String>) {
//!         ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
//!     }
//! }
//!
//! let mut topo = Topology::new();
//! topo.add_source("src", ["in"], StringSerde, StringSerde);
//! topo.add_processor("up", || Box::new(Upper) as Box<dyn Processor<String, String, String, String>>, ["src"]);
//! topo.add_sink("out", "out", ["up"], StringSerde, StringSerde);
//! let built = topo.build("my-app").unwrap();
//!
//! let mut driver = TopologyTestDriver::new(&built).unwrap();
//! driver.pipe_input("in", &StringSerde, &StringSerde, Some("k".to_string()), "hello".to_string(), 0);
//! assert_eq!(
//!     driver.read_output("out", &StringSerde, &StringSerde),
//!     Some((Some("k".to_string()), "HELLO".to_string())),
//! );
//! ```
#![doc(html_root_url = "https://docs.rs/crabka-client-streams/0.0.0")]

mod error;
pub mod membership;
pub mod processor;
pub mod test_driver;
pub mod topology;

pub use error::StreamsClientError;
pub use membership::{
    StreamsAssignment, StreamsEvent, StreamsMembership, StreamsStatus, TaskAssignment,
    TopicPartition,
};
pub use processor::{
    BytesSerde, I64Serde, Processor, ProcessorContext, ProcessorError, ProcessorSupplier, Record,
    RecordContext, Serde, SerdeError, StringSerde,
};
pub use test_driver::TopologyTestDriver;
pub use topology::{BuiltTopology, Topology, TopologyError};
