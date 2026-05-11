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

mod arbitrary_impls;
mod codec;
mod error;
pub mod borrowed;
pub mod owned;
pub mod primitives;
pub mod tagged_fields;

pub use codec::{Decode, DecodeBorrow, Encode};
pub use error::ProtocolError;
pub use tagged_fields::{UnknownTaggedField, UnknownTaggedFields};
