#![allow(
    // Wire-format I/O: casting between length-bearing usize/i32/i64 is
    // pervasive and bounded by Kafka's own field widths.
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    // Doc comments reference Kafka jargon (MessageSet, RecordBatch) that
    // is sometimes used as English nouns rather than identifiers.
    clippy::doc_markdown,
)]

//! Apache Kafka legacy (v0/v1) `MessageSet` codec, with bridges to and from
//! the v2 `RecordBatch` types in [`crabka_protocol`].
//!
//! See the [Kafka protocol docs] for the wire layout this crate
//! implements. v0 carries no per-message timestamp; v1 adds an `i64`
//! timestamp per message (KIP-32). Compression in both is signalled in
//! the low 3 bits of the per-message `attributes` byte, with the
//! compressed payload appearing as a single outer message whose `value`
//! is a nested (uncompressed) MessageSet.
//!
//! [Kafka protocol docs]: https://kafka.apache.org/protocol.html#messageset
//!
//! ## Quick tour
//!
//! - [`Message`] / [`Magic`]: per-message wire format (CRC + fields).
//! - [`ParsedRecord`]: cross-format view of a single offset-tagged
//!   record.
//! - [`decode_message_set`] / [`encode_flat_message_set`] /
//!   [`encode_compressed_message_set`]: top-level codec.
//! - [`v2_to_legacy`] / [`legacy_to_v2`]: bridge to/from
//!   `crabka_protocol::records::RecordBatch`. Use these from the Fetch
//!   (down-conversion) and Produce (up-conversion) handlers.
//!
//! ## Encode and decode a v1 MessageSet
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

pub use bridge::{legacy_to_v2, parsed_from_v2, v2_to_legacy};
pub use error::LegacyRecordsError;
pub use message::{Magic, Message, attrs, attrs_with_compression, compression_from_attrs};
pub use set::{
    ParsedRecord, decode_message_set, encode_compressed_message_set, encode_flat_message_set,
};
