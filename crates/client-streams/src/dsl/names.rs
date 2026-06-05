//! JVM 4.x DSL node-name prefixes (ported verbatim). The auto-name is
//! `format!("{PREFIX}{index:010}")`; `index` increments at op-call time.
#[allow(dead_code)]
pub(crate) const SOURCE: &str = "KSTREAM-SOURCE-";
#[allow(dead_code)]
pub(crate) const SINK: &str = "KSTREAM-SINK-";
#[allow(dead_code)]
pub(crate) const FILTER: &str = "KSTREAM-FILTER-";
#[allow(dead_code)]
pub(crate) const MAPVALUES: &str = "KSTREAM-MAPVALUES-";
#[allow(dead_code)]
pub(crate) const MAP: &str = "KSTREAM-MAP-";
#[allow(dead_code)]
pub(crate) const KEY_SELECT: &str = "KSTREAM-KEY-SELECT-";
#[allow(dead_code)]
pub(crate) const FLATMAP: &str = "KSTREAM-FLATMAP-";
#[allow(dead_code)]
pub(crate) const FLATMAPVALUES: &str = "KSTREAM-FLATMAPVALUES-";
#[allow(dead_code)]
pub(crate) const PEEK: &str = "KSTREAM-PEEK-";
#[allow(dead_code)]
pub(crate) const FOREACH: &str = "KSTREAM-FOREACH-";
#[allow(dead_code)]
pub(crate) const MERGE: &str = "KSTREAM-MERGE-";
#[allow(dead_code)]
pub(crate) const BRANCH: &str = "KSTREAM-BRANCH-";
#[allow(dead_code)]
pub(crate) const BRANCHCHILD: &str = "KSTREAM-BRANCHCHILD-";
#[allow(dead_code)]
pub(crate) const AGGREGATE: &str = "KSTREAM-AGGREGATE-";
#[allow(dead_code)]
pub(crate) const REDUCE: &str = "KSTREAM-REDUCE-";
#[allow(dead_code)]
pub(crate) const AGGREGATE_STORE: &str = "KSTREAM-AGGREGATE-STATE-STORE-";
#[allow(dead_code)]
pub(crate) const REDUCE_STORE: &str = "KSTREAM-REDUCE-STATE-STORE-";
#[allow(dead_code)]
pub(crate) const TABLE_SOURCE: &str = "KTABLE-SOURCE-";
#[allow(dead_code)]
pub(crate) const TOTABLE: &str = "KSTREAM-TOTABLE-";
#[allow(dead_code)]
pub(crate) const TOTABLE_STORE: &str = "KSTREAM-TOTABLE-STATE-STORE-";
#[allow(dead_code)]
pub(crate) const TABLE_TOSTREAM: &str = "KTABLE-TOSTREAM-";
#[allow(dead_code)]
pub(crate) const TABLE_MAPVALUES: &str = "KTABLE-MAPVALUES-";
#[allow(dead_code)]
pub(crate) const TABLE_FILTER: &str = "KTABLE-FILTER-";
#[allow(dead_code)]
pub(crate) const JOIN: &str = "KSTREAM-JOIN-";
#[allow(dead_code)]
pub(crate) const KTABLE_JOIN_THIS: &str = "KTABLE-JOINTHIS-";
#[allow(dead_code)]
pub(crate) const KTABLE_JOIN_OTHER: &str = "KTABLE-JOINOTHER-";
#[allow(dead_code)]
pub(crate) const KTABLE_MERGE: &str = "KTABLE-MERGE-";
/// Windowed `KStream`-`KStream` join processor names (the two per-side join
/// nodes union into one copartitioned subtopology via their shared stores).
pub(crate) const KSTREAM_JOINTHIS: &str = "KSTREAM-JOINTHIS-";
pub(crate) const KSTREAM_JOINOTHER: &str = "KSTREAM-JOINOTHER-";
/// The JVM's two windowed-stream processors (one per side) that put each record
/// into its window store. Not wire-visible, but they consume counter indices, so
/// the lowering burns two to land the join processors — and thus the
/// `<joinProcessorName>-store` window-store names — at the JVM indices.
pub(crate) const KSTREAM_WINDOWED: &str = "KSTREAM-WINDOWED-";
#[allow(dead_code)]
pub(crate) const REPARTITION_SUFFIX: &str = "-repartition";
