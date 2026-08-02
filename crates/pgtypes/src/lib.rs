//! Pgtypes is the value layer for crabgresql: [`Datum`], column types, wire
//! encodings, and operator semantics that match `PostgreSQL`.

#![doc(html_root_url = "https://docs.rs/crabka-pgtypes/0.3.9")]

pub mod array;
pub mod cast;
pub mod composite;
pub mod datetime;
pub mod datum;
pub mod encoding;
pub mod error;
pub mod geometry;
pub mod jsonb;
pub mod multirange;
pub mod numeric;
pub mod ops;
pub mod range;
pub mod string;
pub mod text_search;
pub mod usertype;
pub mod uuid;

pub use datum::{
    ArrayDim, ArrayValue, ColumnType, Datum, ElemType, EnumValue, MAX_ARRAY_DIM, MultirangeValue,
    RangeValue, RecordValue, RegclassValue, canonicalize_for_key, canonicalize_row_for_key, oids,
};
pub use error::TypeError;
pub use geometry::{Path, Point};
pub use jsonb::JsonbValue;
pub use text_search::{Lexeme, Position, QueryTerm, TsQuery, TsVector, Weight};
