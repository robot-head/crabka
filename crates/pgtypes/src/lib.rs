//! Pgtypes is the value layer for crabgresql: [`Datum`], column types, wire
//! encodings, and operator semantics that match `PostgreSQL`.

#![doc(html_root_url = "https://docs.rs/crabka-pgtypes/0.4.0")]

pub mod array;
pub mod bitstring;
pub mod cast;
pub mod composite;
pub mod datetime;
pub mod datum;
pub mod encoding;
pub mod error;
pub mod geometry;
pub mod internal_char;
pub mod json;
pub mod jsonb;
pub mod money;
pub mod multirange;
pub mod network;
pub mod numeric;
pub mod ops;
pub mod range;
pub mod shortest_dec;
pub mod snapshot;
pub mod string;
pub mod sysid;
pub mod text_search;
pub mod usercast;
pub mod usertype;
pub mod uuid;
pub mod xml;

pub use bitstring::{BitString, BitwiseOp};
pub use datum::{
    ArrayDim, ArrayValue, ColumnType, Datum, ElemType, EnumValue, MAX_ARRAY_DIM, MultirangeValue,
    RangeValue, RecordValue, RegclassValue, canonicalize_for_key, canonicalize_row_for_key, oids,
};
pub use error::TypeError;
pub use geometry::{Path, Point, Polygon};
pub use jsonb::JsonbValue;
pub use money::Money;
pub use network::{Inet, InetFamily, MacAddr, MacAddr8};
pub use sysid::Tid;
pub use text_search::{Lexeme, Position, QueryTerm, TsQuery, TsVector, Weight};
