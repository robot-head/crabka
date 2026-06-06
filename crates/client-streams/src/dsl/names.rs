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
/// Stream-globaltable join processor prefix. Not wire-visible (the wire topology
/// carries topics only, and the global table has no subtopology), so this name
/// affects no golden — it just keeps the join node clearly labelled in the graph.
pub(crate) const GLOBALTABLE_JOIN: &str = "KSTREAM-GLOBALTABLE-JOIN-";
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
/// The JVM renames the per-side join processors for left/outer joins: the THIS
/// processor becomes `KSTREAM-OUTERTHIS-` when the *other* (right) side is outer
/// (`rightOuter`), and the OTHER processor becomes `KSTREAM-OUTEROTHER-` when the
/// *this* (left) side is outer (`leftOuter`). So inner → JOINTHIS/JOINOTHER, left
/// → JOINTHIS/OUTEROTHER, outer → OUTERTHIS/OUTEROTHER. The window-store names are
/// still `<joinProcessorName>-store`, so the prefix flows into the changelog name.
pub(crate) const KSTREAM_OUTERTHIS: &str = "KSTREAM-OUTERTHIS-";
pub(crate) const KSTREAM_OUTEROTHER: &str = "KSTREAM-OUTEROTHER-";
/// The JVM's two windowed-stream processors (one per side) that put each record
/// into its window store. Not wire-visible, but they consume counter indices, so
/// the lowering burns two to land the join processors — and thus the
/// `<joinProcessorName>-store` window-store names — at the JVM indices.
pub(crate) const KSTREAM_WINDOWED: &str = "KSTREAM-WINDOWED-";
/// The shared outer-join KV store for KIP-633 left/outer window-close emission.
/// Only minted for left/outer joins (inner topologies are byte-unchanged). The
/// exact JVM index/naming is pinned by Task C4's golden capture.
pub(crate) const KSTREAM_OUTERSHARED: &str = "KSTREAM-OUTERSHARED-";
/// The JVM `KTableImpl.suppress` processor node prefix.
pub(crate) const KTABLE_SUPPRESS: &str = "KTABLE-SUPPRESS-";
/// The suppress buffer's store name prefix (JVM `newStoreName(SUPPRESS_NAME)` =
/// `KTABLE-SUPPRESS-` + `STATE-STORE-` + index, minted right after the suppress
/// processor name so the index is consecutive).
pub(crate) const KTABLE_SUPPRESS_STORE: &str = "KTABLE-SUPPRESS-STATE-STORE-";
#[allow(dead_code)]
pub(crate) const REPARTITION_SUFFIX: &str = "-repartition";
/// Global table source node prefix (JVM `KSTREAM-SOURCE-`). Not wire-visible.
#[allow(dead_code)]
pub(crate) const GLOBAL_SOURCE: &str = "KSTREAM-SOURCE-";
/// Global table store-update processor prefix (JVM `KTABLE-SOURCE-`). Not
/// wire-visible.
#[allow(dead_code)]
pub(crate) const GLOBAL_PROCESSOR: &str = "KTABLE-SOURCE-";
/// JVM `KStream.process` node prefix. Not wire-visible (the wire lists topics, not
/// processor names), but used for fidelity + diagnostics.
pub(crate) const KSTREAM_PROCESSOR: &str = "KSTREAM-PROCESSOR-";
/// JVM `KStream.processValues` node prefix. Not wire-visible.
#[allow(dead_code)] // consumed by process/process_values in T3/T5
pub(crate) const KSTREAM_PROCESSVALUES: &str = "KSTREAM-PROCESSVALUES-";
