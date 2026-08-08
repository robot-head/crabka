//! JVM 4.x DSL node-name prefixes, ported verbatim.
//!
//! The auto-name is `format!("{PREFIX}{index:010}")`. `index` increments at
//! op-call time.
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
/// Stream-globaltable join processor prefix.
///
/// This prefix is not wire-visible, because the wire topology carries only
/// topics and the global table has no subtopology. The name therefore affects no
/// golden. It only keeps the join node clearly labelled in the graph.
pub(crate) const GLOBALTABLE_JOIN: &str = "KSTREAM-GLOBALTABLE-JOIN-";
#[allow(dead_code)]
pub(crate) const KTABLE_JOIN_THIS: &str = "KTABLE-JOINTHIS-";
#[allow(dead_code)]
pub(crate) const KTABLE_JOIN_OTHER: &str = "KTABLE-JOINOTHER-";
#[allow(dead_code)]
pub(crate) const KTABLE_MERGE: &str = "KTABLE-MERGE-";
/// Windowed `KStream`-`KStream` join processor names. The two per-side join
/// nodes union into one copartitioned subtopology through their shared stores.
pub(crate) const KSTREAM_JOINTHIS: &str = "KSTREAM-JOINTHIS-";
pub(crate) const KSTREAM_JOINOTHER: &str = "KSTREAM-JOINOTHER-";
/// The JVM renames the per-side join processors for left and outer joins. The
/// THIS processor becomes `KSTREAM-OUTERTHIS-` when the *other*, right, side is
/// outer (`rightOuter`). The OTHER processor becomes `KSTREAM-OUTEROTHER-` when
/// the *this*, left, side is outer (`leftOuter`). So inner gives
/// JOINTHIS/JOINOTHER, left gives JOINTHIS/OUTEROTHER, and outer gives
/// OUTERTHIS/OUTEROTHER. The window-store names stay
/// `<joinProcessorName>-store`, so the prefix flows into the changelog name.
#[allow(dead_code)]
pub(crate) const KSTREAM_OUTERTHIS: &str = "KSTREAM-OUTERTHIS-";
#[allow(dead_code)]
pub(crate) const KSTREAM_OUTEROTHER: &str = "KSTREAM-OUTEROTHER-";
/// The JVM's two windowed-stream processors, one per side, that put each record
/// into its window store.
///
/// These are not wire-visible, but they take counter indices. The lowering
/// therefore burns two indices to land the join processors, and with them the
/// `<joinProcessorName>-store` window-store names, at the JVM indices.
#[allow(dead_code)]
pub(crate) const KSTREAM_WINDOWED: &str = "KSTREAM-WINDOWED-";
/// The shared outer-join KV store for the KIP-633 left and outer window-close
/// emission.
///
/// The code mints this name only for a left or outer join, so an inner topology
/// stays byte-unchanged. Task C4's golden capture pins the exact JVM index and
/// name.
#[allow(dead_code)]
pub(crate) const KSTREAM_OUTERSHARED: &str = "KSTREAM-OUTERSHARED-";
/// The JVM `KTableImpl.suppress` processor node prefix.
pub(crate) const KTABLE_SUPPRESS: &str = "KTABLE-SUPPRESS-";
/// The suppress buffer's store name prefix. The JVM
/// `newStoreName(SUPPRESS_NAME)` is `KTABLE-SUPPRESS-` + `STATE-STORE-` +
/// index. The code mints it right after the suppress processor name, so the
/// index is consecutive.
pub(crate) const KTABLE_SUPPRESS_STORE: &str = "KTABLE-SUPPRESS-STATE-STORE-";
/// JVM `KGroupedTableImpl` repartition-map, or select, node prefix. The node
/// maps the upstream `Change<V>` to the grouped `(KR, Change<VR>)` before the
/// repartition. It is not wire-visible.
#[allow(dead_code)]
pub(crate) const KTABLE_SELECT: &str = "KTABLE-SELECT-";
/// JVM `KGroupedTableImpl` aggregate processor node prefix, which subtracts and
/// then adds. It is not wire-visible.
#[allow(dead_code)]
pub(crate) const KTABLE_AGGREGATE: &str = "KTABLE-AGGREGATE-";
/// Store-name prefix for an unnamed `KGroupedTable::aggregate` or
/// `KGroupedTable::count` result store. The code uses it only when
/// `Materialized` carries no explicit name.
#[allow(dead_code)]
pub(crate) const KTABLE_AGGREGATE_STORE: &str = "KTABLE-AGGREGATE-STATE-STORE-";
/// Store-name prefix for an unnamed `KGroupedTable::reduce` result store.
#[allow(dead_code)]
pub(crate) const KTABLE_REDUCE_STORE: &str = "KTABLE-REDUCE-STATE-STORE-";
#[allow(dead_code)]
pub(crate) const REPARTITION_SUFFIX: &str = "-repartition";

// ── KIP-213 foreign-key join node + store + topic prefixes ──────────────────
// Ported verbatim from the JVM `KTableImpl.doJoinOnForeignKey` name constants;
// pinned by the `fk_join_inner`/`fk_join_left` wire goldens.
/// Subscription-registration node prefix.
///
/// The code uses it BOTH for the registration topic name
/// `<app>-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-<idx>-topic`, whose counter
/// index is minted first, and for the `SubscriptionSend` processor, which takes
/// the next index.
pub(crate) const FK_SUBSCRIPTION_REGISTRATION: &str = "KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-";
/// The prefix for the receive processor, the subscription-join processor, and
/// the foreign-table-join processor. The JVM uses one `SUBSCRIPTION_PROCESSOR`
/// prefix for all three.
pub(crate) const FK_SUBSCRIPTION_PROCESSOR: &str = "KTABLE-FK-JOIN-SUBSCRIPTION-PROCESSOR-";
/// The response-resolver processor prefix.
pub(crate) const FK_RESPONSE_RESOLVER: &str =
    "KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-RESOLVER-PROCESSOR-";
/// The FK-join result (`OUTPUT`) processor prefix. This is the result `KTable`
/// node.
pub(crate) const FK_OUTPUT: &str = "KTABLE-FK-JOIN-OUTPUT-";
/// The subscription state-store name prefix.
pub(crate) const FK_SUBSCRIPTION_STATE_STORE: &str = "KTABLE-FK-JOIN-SUBSCRIPTION-STATE-STORE-";
/// Subscription-response node prefix. The code uses it for the response topic
/// name and for the response sink and source.
pub(crate) const FK_SUBSCRIPTION_RESPONSE: &str = "KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-";
/// The materialized-store name that the JVM burns for the result table, even
/// when it creates no result changelog.
///
/// `MaterializedInternal(..., FK_JOIN_OUTPUT_NAME)` takes a counter index. That
/// index lands at OUTPUT+1.
pub(crate) const FK_OUTPUT_STATE_STORE: &str = "KTABLE-FK-JOIN-OUTPUT-STATE-STORE-";
/// `KTABLE-SINK-` and `KTABLE-SOURCE-`, the sink and source node prefixes for
/// the registration and response repartitions. These are the JVM `KTableImpl`
/// sink and source names.
pub(crate) const KTABLE_SINK: &str = "KTABLE-SINK-";
pub(crate) const KTABLE_SOURCE: &str = "KTABLE-SOURCE-";
/// Suffix the JVM appends to an FK-join repartition topic's base name.
pub(crate) const FK_TOPIC_SUFFIX: &str = "-topic";
/// Global table source node prefix, the JVM `KSTREAM-SOURCE-`. It is not
/// wire-visible.
#[allow(dead_code)]
pub(crate) const GLOBAL_SOURCE: &str = "KSTREAM-SOURCE-";
/// Global table store-update processor prefix, the JVM `KTABLE-SOURCE-`. It is
/// not wire-visible.
#[allow(dead_code)]
pub(crate) const GLOBAL_PROCESSOR: &str = "KTABLE-SOURCE-";
/// JVM `KStream.process` node prefix. It is not wire-visible, because the wire
/// lists topics and not processor names, but the code keeps it for fidelity and
/// for diagnostics.
pub(crate) const KSTREAM_PROCESSOR: &str = "KSTREAM-PROCESSOR-";
/// JVM `KStream.processValues` node prefix. Not wire-visible.
pub(crate) const KSTREAM_PROCESSVALUES: &str = "KSTREAM-PROCESSVALUES-";

// ── KIP-150 cogroup node prefixes ───────────────────────────────────────────
// Pinned by the `cogroup` golden capture (Kafka Streams 4.1.0). The processor
// names are NOT wire-visible (the wire carries topics/stores/copartition only),
// so these drive the internal node-name counter + diagnostics; the JVM constant
// is `COGROUPKSTREAM-` (NOT `COGROUP-`).
/// Per-input cogroup aggregate processor prefix. There is one node per input
/// stream, and all the nodes share the cogroup state store.
#[allow(dead_code)]
pub(crate) const COGROUP_AGGREGATE: &str = "COGROUPKSTREAM-AGGREGATE-";
/// Cogroup passthrough merge node prefix. The node fans the per-input aggregate
/// processors into the single result `KTable` source.
#[allow(dead_code)]
pub(crate) const COGROUP_MERGE: &str = "COGROUPKSTREAM-MERGE-";
