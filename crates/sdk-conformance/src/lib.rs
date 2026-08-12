//! Crabka SDK conformance protocol, vectors, and harness.
//!
//! The harness drives language adapters through JSON lines on standard I/O. It
//! intentionally tests behavior through the adapter protocol rather than SDK
//! source text.

pub mod harness;
pub mod mock_adapter;
pub mod protocol;
pub mod vectors;

pub use self::{
    harness::{Harness, HarnessConfig, HarnessSubstrate, RunSummary, SkippedVector, VectorFailure},
    protocol::{AdapterError, CONTRACT_MAJOR, Command, ErrorKind, Response},
};
