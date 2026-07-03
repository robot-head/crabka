//! Newtypes for the two cluster-identity UUIDs the `format` subcommand
//! threads around.
//!
//! `format` mints (or accepts) a **cluster id** and mints a per-replica
//! **directory id** (KIP-853 voter identity), then persists both to
//! `meta.properties.json` and the bootstrap manifest. Both are `uuid::Uuid`,
//! so a bare-`Uuid` signature like `write_meta_properties(_, cluster, dir)`
//! happily compiles with the two transposed — and a swap silently corrupts
//! the broker's stable identity (it would boot believing its directory id is
//! the cluster id and vice versa). Wrapping each in a distinct newtype makes
//! the compiler reject the mix-up.
//!
//! [`ClusterId`] derives `Serialize` with `#[serde(transparent)]` because it
//! is a field of the serialized `BootstrapManifest`; the emitted JSON is a
//! bare UUID string, byte-identical to the previous `Uuid` field.
//! [`DirectoryId`] is never serialized as a whole value (it is rendered via
//! `to_string()` into a hand-built JSON object, and unwrapped to the raw
//! `Uuid` at the `crabka_voters::Voter` boundary), so it needs no serde.

use derive_more::{Display, From, Into};
use serde::Serialize;
use uuid::Uuid;

/// A Kafka cluster id (KIP-853): the shared identity every broker in the
/// cluster agrees on. Distinct from [`DirectoryId`], which is per-replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, From, Into, Serialize)]
#[serde(transparent)]
pub struct ClusterId(pub Uuid);

/// A replica's stable directory id (KIP-853 voter identity), persisted to
/// `meta.properties.json` and recovered on every boot. Distinct from
/// [`ClusterId`] so the two cannot be transposed at a call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, From, Into)]
pub struct DirectoryId(pub Uuid);
