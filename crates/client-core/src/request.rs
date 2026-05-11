//! Marker trait implemented by generated Request types from
//! `crabka-protocol`. Provides the dispatch information (api key,
//! version range, response type) that the client needs.

use crabka_protocol::{Decode, Encode};

/// Implemented by every generated Request struct in `crabka-protocol`.
///
/// The `crabka-protocol-codegen` crate emits this impl for every
/// Request type. Hand-rolled implementations are also valid for
/// non-codegen message types if they ever exist.
pub trait ProtocolRequest: Encode {
    /// Kafka API key for this request.
    const API_KEY: i16;
    /// Minimum protocol version this Rust type supports.
    const MIN_VERSION: i16;
    /// Maximum protocol version this Rust type supports.
    const MAX_VERSION: i16;
    /// First version that uses flexible (KIP-482) framing.
    /// `i16::MAX` for never-flexible messages.
    const FLEXIBLE_MIN: i16;

    /// Matching response type from `crabka-protocol`.
    type Response: for<'de> Decode<'de>;
}
