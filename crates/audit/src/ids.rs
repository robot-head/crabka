//! Domain newtypes for the audit crate.
//!
//! These wrap same-typed primitives that recur across the hash-chain, spool,
//! and verifier so a transposed call site (e.g. `set_depth(bytes, count)`) is a
//! compile error rather than a silent corruption. See the [newtype guidance] in
//! the style guide.
//!
//! [newtype guidance]: ../../../docs/style_guides/code_style_guide.md

use derive_more::{Add, AddAssign, Display, From, Into};

/// Per-broker hash-chain sequence number stamped on each record (`seq` header).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct Seq(pub u64);

/// Epoch-millisecond timestamp (checkpoint `time`, OCSF `time`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct EpochMs(pub i64);

/// Count of chained (data) records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct RecordCount(pub u64);

/// Count of signed checkpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct CheckpointCount(pub u64);

/// Number of bytes currently held in the spool.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into, Add, AddAssign,
)]
pub struct SpoolBytes(pub u64);

/// Configured upper bound on spool size in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct MaxSpoolBytes(pub u64);
