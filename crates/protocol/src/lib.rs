//! Kafka wire-protocol codec.
//!
//! `crabka-protocol` is a pure-Rust library that encodes and decodes every
//! Apache Kafka request and response message, byte-equivalent to the upstream
//! JVM implementation. It performs no I/O and makes no async assumptions; it
//! is intended to be consumed by broker, client, and tooling crates within
//! the Crabka project.
//!
//! ## Two flavors
//!
//! Every message has two generated types:
//!
//! - `owned::FooRequest` — owns its data (`String`, `Bytes`, `Vec<T>`).
//!   Easy to move across `await` points.
//! - `borrowed::FooRequest<'a>` — references slices of the input buffer
//!   (`&'a str`, `&'a [u8]`). Zero-copy decoding.
//!
//! Both implement [`Encode`]; the owned flavor implements [`Decode`] and the
//! borrowed flavor implements [`DecodeBorrow`].
//!
//! ## Versioning
//!
//! `crabka-protocol` is pre-1.0. Breaking API changes per minor version are
//! allowed; see CHANGELOG.md. The wire-protocol pin is recorded in
//! `crates/protocol/schemas/VERSION`.
//!
//! ## Encoding a generated request
//!
//! ```rust
//! use bytes::BytesMut;
//! use crabka_protocol::Encode;
//! use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
//!
//! let req = ApiVersionsRequest::default();
//! let version = 4;
//! let mut buf = BytesMut::with_capacity(req.encoded_len(version));
//! req.encode(&mut buf, version).unwrap();
//! assert_eq!(buf.len(), req.encoded_len(version));
//! ```

pub mod api_key;
pub use api_key::ApiKey;
mod arbitrary_impls;
pub mod borrowed;
mod codec;
#[doc(hidden)]
pub mod codegen_helpers;
mod error;
pub mod kafka_3_6_2;
pub mod legacy_compat;
pub mod owned;
pub mod primitives;
pub mod records;
pub mod tagged_fields;

pub use codec::{Decode, DecodeBorrow, Encode, ProtocolRequest};
pub use error::ProtocolError;
pub use records::remote_log_metadata::RemoteLogMetadataRecord;
pub use tagged_fields::{UnknownTaggedField, UnknownTaggedFields};
