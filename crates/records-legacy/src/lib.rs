//! Apache Kafka legacy (v0/v1) `MessageSet` codec.
//!
//! This crate also bridges to and from the v2 `RecordBatch` types in
//! [`crabka_protocol`].
//!
//! See the [Kafka protocol docs] for the wire layout that this crate
//! implements. v0 carries no per-message timestamp. v1 adds one `i64`
//! timestamp per message (KIP-32). Both versions signal compression in
//! the low 3 bits of the per-message `attributes` byte. The compressed
//! payload is a single outer message whose `value` is a nested,
//! uncompressed `MessageSet`.
//!
//! [Kafka protocol docs]: https://kafka.apache.org/protocol.html#messageset
//!
//! ## Quick tour
//!
//! - [`Message`] / [`Magic`]: per-message wire format, with a CRC and the
//!   message fields.
//! - [`ParsedRecord`]: cross-format view of a single offset-tagged
//!   record.
//! - [`decode_message_set`] / [`encode_flat_message_set`] /
//!   [`encode_compressed_message_set`]: top-level codec.
//! - [`v2_to_legacy`] / [`legacy_to_v2`]: bridge to and from
//!   `crabka_protocol::records::RecordBatch`. Use these from the Fetch
//!   handler for down-conversion, and from the Produce handler for
//!   up-conversion.
//!
//! ## Encode and decode a v1 `MessageSet`
//!
//! ```rust
//! use bytes::{Bytes, BytesMut};
//! use crabka_ids::Offset;
//! use crabka_records_legacy::{Magic, ParsedRecord, decode_message_set, encode_flat_message_set};
//!
//! # fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let records = vec![ParsedRecord {
//!     offset: Offset(42),
//!     timestamp: Some(1_713_000_000_000),
//!     key: Some(Bytes::from_static(b"order-42")),
//!     value: Some(Bytes::from_static(b"created")),
//! }];
//!
//! let mut buf = BytesMut::new();
//! encode_flat_message_set(records, Magic::V1, &mut buf);
//! let decoded = decode_message_set(&mut &buf[..], buf.len())?;
//! assert_eq!(decoded[0].offset, Offset(42));
//! # Ok(())
//! # }
//! ```

mod bridge;
mod error;
mod message;
mod set;

pub use bridge::{legacy_to_v2, legacy_to_v2_with_policy, parsed_from_v2, v2_to_legacy};
pub use error::LegacyRecordsError;
pub use message::{Magic, Message, attrs, attrs_with_compression, compression_from_attrs};
pub use set::{
    ParsedRecord, decode_message_set, decode_message_set_with_policy,
    encode_compressed_message_set, encode_flat_message_set,
};
