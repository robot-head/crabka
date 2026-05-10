use bytes::{Buf, BufMut};

use crate::ProtocolError;

/// Encode a Kafka wire-protocol value into a buffer at the given protocol version.
///
/// `version` is the message-level version negotiated via `ApiVersionsRequest`.
/// Implementations must produce bytes that are byte-equal to the upstream JVM
/// `kafka-clients` implementation for the same `(message_type, version, value)`.
pub trait Encode {
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
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError>;
}
