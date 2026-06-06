//! Broker-backed execution runtime (sub-project #2b).

mod app;
pub(crate) mod global;
pub mod io;
mod io_broker;
mod task;
mod thread;

pub use app::{KafkaStreams, KafkaStreamsState};
pub use io::{FetchBatch, FetchedRec, OffsetStore, RecordFetcher, RecordProducer};
