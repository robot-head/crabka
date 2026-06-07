//! Broker-backed execution runtime (sub-project #2b).

mod app;
pub(crate) mod clock;
pub(crate) mod global;
pub mod io;
mod io_broker;
pub(crate) mod iq;
mod iq_view;
mod task;
mod thread;

pub use app::{KafkaStreams, KafkaStreamsState};
pub use io::{FetchBatch, FetchedRec, OffsetStore, RecordFetcher, RecordProducer};
pub use iq_view::ReadOnlyKeyValueStore;
pub use iq_view::ReadOnlyWindowStore;
