//! Pgtypes: the value layer for crabgresql — [`Datum`], column types, wire
//! encodings, and operator semantics matching `PostgreSQL`.

#![doc(html_root_url = "https://docs.rs/crabka-pgtypes/0.3.9")]

pub mod cast;
pub mod datetime;
pub mod datum;
pub mod encoding;
pub mod error;
pub mod numeric;
pub mod ops;
pub mod string;
pub mod uuid;

pub use datum::{ColumnType, Datum, oids};
pub use error::TypeError;
