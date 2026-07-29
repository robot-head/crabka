//! Pgtypes: the value layer for crabgresql — [`Datum`], column types, wire
//! encodings, and operator semantics matching `PostgreSQL`.

#![doc(html_root_url = "https://docs.rs/crabka-pgtypes/0.3.9")]

pub mod array;
pub mod cast;
pub mod datetime;
pub mod datum;
pub mod encoding;
pub mod error;
pub mod jsonb;
pub mod numeric;
pub mod ops;
pub mod string;
pub mod uuid;

pub use datum::{
    ArrayValue, ColumnType, Datum, ElemType, canonicalize_for_key, canonicalize_row_for_key, oids,
};
pub use error::TypeError;
pub use jsonb::JsonbValue;
