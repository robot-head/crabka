use bytes::{Buf, BufMut};

use crate::ProtocolError;

/// Encode a Kafka wire-protocol value into a buffer at the given protocol version.
///
/// `version` is the message-level version negotiated via `ApiVersionsRequest`.
/// Implementations must produce bytes that are byte-equal to the upstream JVM
/// `kafka-clients` implementation for the same `(message_type, version, value)`.
pub trait Encode {
    /// # Errors
    /// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError>;

    /// Size in bytes that `encode` will write. Must equal the actual count.
    fn encoded_len(&self, version: i16) -> usize;
}

/// Decode a Kafka wire-protocol value from a buffer at the given protocol version.
///
/// The `'de` lifetime is the lifetime the decoded value may borrow from the input.
/// Owned-flavor types implement `Decode<'de>` for any `'de` (their output is `'static`).
/// Borrowed-flavor types implement `Decode<'de>` where `Self: 'de`.
pub trait Decode<'de>: Sized {
    /// # Errors
    /// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError>;
}

/// Like `Decode`, but for borrowed (zero-copy) flavors. Requires a contiguous
/// buffer because borrowed values reference slices of it.
pub trait DecodeBorrow<'de>: Sized + 'de {
    /// # Errors
    /// Returns the underlying protocol error when input is truncated, contains an invalid length or tag, or cannot be encoded for the selected version.
    fn decode_borrow(buf: &mut &'de [u8], version: i16) -> Result<Self, ProtocolError>;
}

/// Implemented by every generated Request struct in `crabka-protocol`.
///
/// The `crabka-protocol-codegen` crate emits this impl for every Request type.
/// Provides the dispatch information (api key, version range, response type)
/// that the client needs.
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
