//! Tempo-compatible traces service for Crabka.

#![forbid(unsafe_code)]

pub mod blockbuilder;
pub mod compactor;
pub mod distributor;
pub mod error;
pub mod frontend;
pub mod limits;
pub mod livestore;
pub mod metrics;
pub mod metricsgen;
pub mod querier;
pub mod span;
pub mod wal;
pub mod wire;

pub use blockbuilder::{build_blocks, group_by_trace, object_key, prefixed_object_key};
pub use error::TracesError;
pub use limits::{LimitError, Limits};
pub use livestore::LiveStore;
pub use span::{AttrValue, EventRecord, KeyValue, LinkRecord, Span, SpanKind, StatusCode};
pub use wal::{SpanRecord, TRACES_WAL_TOPIC, partition_key};
pub use wire::{WireFormat, negotiate};
