//! Domain newtypes for the replicator crate.
//!
//! MirrorMaker-2 offset translation juggles three different `i64` offsets that
//! all live in different address spaces:
//!
//! - [`UpstreamOffset`] — an offset on the **source** cluster, as recorded in an
//!   offset-sync or checkpoint.
//! - [`DownstreamOffset`] — the corresponding offset on the **target** cluster.
//! - [`CommittedOffset`] — a consumer group's committed **source** offset that is
//!   being translated to the target.
//!
//! Transposing these at a call site — e.g. building an [`crate::mm2::OffsetSync`]
//! as `{ upstream: downstream_val, downstream: upstream_val }`, or writing the
//! `translate()` math as `up + (committed - down)` — compiles cleanly when every
//! value is a bare `i64`, and silently corrupts offset translation. Wrapping each
//! meaning in its own type turns those swaps into compile errors.
//!
//! The MM2 wire codec ([`crate::mm2::Writer`] / [`crate::mm2::Reader`]) still
//! reads and writes raw `i64`/`i32`, so byte-exactness with the JVM MM2 codecs is
//! preserved: these newtypes wrap only the in-memory domain values and unwrap via
//! `.0` at the codec boundary.
//!
//! See the [newtype guidance] in the style guide.
//!
//! [newtype guidance]: ../../../docs/style_guides/code_style_guide.md

/// The canonical cross-crate `PartitionIndex`, as carried in offset-syncs,
/// checkpoints, and replicated records. The MM2 offset newtypes below stay
/// crate-local — they encode source-vs-target-cluster distinctions specific to
/// MirrorMaker-2 that have no meaning outside this crate.
pub use crabka_ids::{Offset, PartitionIndex};
use derive_more::{Display, From, Into};

/// An offset on the **source** cluster, as recorded in an offset-sync or
/// checkpoint (`upstream` in the JVM MM2 codecs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct UpstreamOffset(pub i64);

/// An offset on the **target** cluster, paired with an [`UpstreamOffset`] in an
/// offset-sync or checkpoint (`downstream` in the JVM MM2 codecs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct DownstreamOffset(pub i64);

/// A consumer group's committed **source** offset being translated to the
/// target. Lives in the same address space as [`UpstreamOffset`] (both are
/// source-cluster offsets) but is a distinct concept at the translation call
/// site, so it gets its own type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct CommittedOffset(pub i64);

/// A record timestamp in epoch milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct Timestamp(pub i64);
