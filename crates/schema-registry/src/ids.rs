//! Newtypes for the registry's two same-typed identifiers.
//!
//! A registered schema is addressed two different ways, both `i32`:
//!
//! - a **global schema id** ([`SchemaId`]) — unique across the whole registry,
//!   keyed by the schema's canonical form + references, and
//! - a **per-subject version** ([`SchemaVersion`]) — the 1-based ordinal of the
//!   schema within one subject's history.
//!
//! They travel together everywhere — `Registered { id, version }`, the
//! `_schemas` record value (`SchemaValue { id, version, .. }`), the encoder
//! `encode_schema(subject, version, id, ..)` — so a transposed `(version, id)`
//! call compiles as raw `i32`s and silently mislabels the record. These wrappers
//! make the compiler reject the mix-up.
//!
//! Both are serialised: `SchemaKey`/`SchemaValue`/`SchemaReference` are the
//! compacted `_schemas` topic's wire contract, so every newtype is
//! `#[serde(transparent)]` — the encoded JSON is exactly the inner `i32`, never a
//! wrapping object, keeping the `_schemas` bytes byte-identical to Confluent's.

use derive_more::{Display, From, Into};
use serde::{Deserialize, Serialize};

/// A registry-global schema id (`i32` on the `_schemas` wire and in the REST
/// API). Unique across the whole registry; keyed by canonical form + references.
///
/// `Default` is `SchemaId(0)` — the "no ids assigned yet" sentinel used to seed
/// `StoreState::max_id` before the first registration lands.
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

/// A schema's 1-based version within a single subject's history (`i32` on the
/// `_schemas` wire and in the REST API). Distinct subjects reuse version numbers,
/// so a version is only meaningful paired with its subject.
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
