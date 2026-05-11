//! TCP listener, per-connection task, and Kafka framing helpers.
//!
//! Accept loop lands in Task 11; for now this file only re-exports the
//! framing codec so handlers can construct framed streams in tests.

pub(crate) mod codec;
