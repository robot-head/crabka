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
//! use crabka_client_streams::{StreamsEvent, StreamsMembership, Topology};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut topo = Topology::new();
//! topo.add_source("src", ["input-topic"]);
//! topo.add_sink("snk", "output-topic", ["src"]);
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
