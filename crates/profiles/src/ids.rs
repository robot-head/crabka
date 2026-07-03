//! Newtypes for same-typed profiles domain values that a call site could
//! transpose and still compile.
//!
//! Each wrapper here guards a specific swap-shaped signature: two (or more)
//! adjacent parameters of the same primitive type whose meanings differ, where
//! passing them in the wrong order would be a silent bug. None of these values
//! are serialised (they are query-path and metric arguments, plus an in-memory
//! partition-map key), so no `#[serde(transparent)]` is required. Arithmetic on
//! the inner value is done via `.0` at the (few) sites that need it, so `Add`/
//! `Sub` are deliberately not derived.

use derive_more::{Display, From, Into};

/// A query-window start bound, in Unix milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct StartMs(pub i64);

/// A query-window end bound, in Unix milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct EndMs(pub i64);

/// The "current" wall-clock instant a relative render time resolves against,
/// in Unix milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct NowMs(pub i64);

/// The fallback value a render-time parameter takes when absent, in Unix
/// milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct DefaultMs(pub i64);

/// The lower edge of a heatmap's value axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct MinValue(pub i64);

/// The upper edge of a heatmap's value axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct MaxValue(pub i64);

/// Request-body bytes accepted on the ingest path, for the cumulative bytes
/// counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct IngestBytes(pub u64);

/// Profile/sample items ingested, for the cumulative items counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct IngestItems(pub u64);

/// A partition key in the composite cold-read address space (per-block base
/// OR-ed with a dense local id), used to route symbol resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct ExternalPartition(pub u64);

/// A partition key within a single block's own symbol DB, scoped to that block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct LocalPartition(pub u64);
