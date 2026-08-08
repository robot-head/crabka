//! Newtypes for the registry's two same-typed identifiers.
//!
//! A registered schema is addressed two different ways, both `i32`:
//!
//! - a **global schema id** ([`SchemaId`]): unique across the whole registry,
//!   keyed by the schema's canonical form and references, and
//! - a **per-subject version** ([`SchemaVersion`]): the 1-based ordinal of the
//!   schema within one subject's history.
//!
//! They travel together everywhere: in `Registered { id, version }`, in the
//! `_schemas` record value `SchemaValue { id, version, .. }`, and in the encoder
//! `encode_schema(subject, version, id, ..)`. A transposed `(version, id)` call
//! compiles as raw `i32`s and silently mislabels the record. These wrappers make
//! the compiler reject the mix-up.
//!
//! Both are serialised. `SchemaKey`, `SchemaValue`, and `SchemaReference` are
//! the compacted `_schemas` topic's wire contract, so every newtype is
//! `#[serde(transparent)]`. The encoded JSON is exactly the inner `i32` and
//! never a wrapping object, which keeps the `_schemas` bytes byte-identical to
//! Confluent's.

use derive_more::{Display, From, Into};
use serde::{Deserialize, Serialize};

/// A registry-global schema id, an `i32` on the `_schemas` wire and in the REST
/// API. It is unique across the whole registry and keyed by canonical form and
/// references.
///
/// `Default` is `SchemaId(0)`. This is the "no ids assigned yet" sentinel that
/// seeds `StoreState::max_id` before the first registration lands.
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Display,
    From,
    Into,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct SchemaId(pub i32);

/// A schema's 1-based version within a single subject's history, an `i32` on
/// the `_schemas` wire and in the REST API. Distinct subjects reuse version
/// numbers, so a version is meaningful only with its subject.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Display,
    From,
    Into,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct SchemaVersion(pub i32);
