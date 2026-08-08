//! Newtypes for the two cluster-identity UUIDs in the `format` subcommand.
//!
//! `format` makes a **cluster id**, or accepts one, and makes a per-replica
//! **directory id**. The directory id is the KIP-853 voter identity. The
//! subcommand writes both ids to `meta.properties.json` and to the bootstrap
//! manifest.
//!
//! Both ids are `uuid::Uuid`. So a bare-`Uuid` signature such as
//! `write_meta_properties(_, cluster, dir)` compiles with the two arguments
//! transposed. A swap silently corrupts the broker's stable identity, because
//! the broker then boots with the two ids exchanged. A distinct newtype for
//! each id makes the compiler reject the mix-up.
//!
//! [`ClusterId`] derives `Serialize` with `#[serde(transparent)]` because it
//! is a field of the serialized `BootstrapManifest`. The JSON output is a
//! bare UUID string, byte-identical to the previous `Uuid` field.
//! [`DirectoryId`] is never serialized as a whole value, so it needs no
//! serde. The code writes it with `to_string()` into a hand-built JSON
//! object, and unwraps it to the raw `Uuid` at the `crabka_voters::Voter`
//! boundary.

use derive_more::{Display, From, Into};
use serde::Serialize;
use uuid::Uuid;

/// A Kafka cluster id (KIP-853): the identity that every broker shares.
///
/// This id is distinct from [`DirectoryId`], which is per-replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, From, Into, Serialize)]
#[serde(transparent)]
pub struct ClusterId(pub Uuid);

/// A replica's stable directory id: the KIP-853 voter identity.
///
/// `format` writes this id to `meta.properties.json`, and the broker recovers
/// it on every boot. The type is distinct from [`ClusterId`], so a call site
/// cannot transpose the two.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, From, Into)]
pub struct DirectoryId(pub Uuid);
