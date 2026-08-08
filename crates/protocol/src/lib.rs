//! Kafka wire-protocol codec.
//!
//! `crabka-protocol` is a pure-Rust library that encodes and decodes every
//! Apache Kafka request and response message, byte-equivalent to the upstream
//! JVM implementation. It does no I/O and makes no async assumptions. The
//! broker, client, and tooling crates in the Crabka project use it.
//!
//! ## Two flavors
//!
//! Every message has two generated types:
//!
//! - `owned::FooRequest` owns its data as `String`, `Bytes`, and `Vec<T>`.
//!   It is easy to move across `await` points.
//! - `borrowed::FooRequest<'a>` references slices of the input buffer as
//!   `&'a str` and `&'a [u8]`. It decodes with zero copies.
//!
//! Both implement [`Encode`]. The owned flavor implements [`Decode`] and the
//! borrowed flavor implements [`DecodeBorrow`].
//!
//! ## Versioning
//!
//! `crabka-protocol` is pre-1.0. Breaking API changes are allowed per minor
//! version. See CHANGELOG.md. `crates/protocol/schemas/VERSION` records the
//! wire-protocol pin.
//!
//! ## Encoding a generated request
//!
//! ```rust
//! use bytes::BytesMut;
//! use crabka_protocol::{Encode, owned::api_versions_request::ApiVersionsRequest};
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
