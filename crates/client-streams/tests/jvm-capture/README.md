# JVM Kafka Streams 4.x DSL capture harness

Captures the byte-exact `StreamsGroupHeartbeatRequest.Topology` (KIP-1071) that a real
Apache Kafka Streams 4.x client emits for five DSL topologies built with
`optimization=all`, and writes them as golden fixtures under
`../testdata/golden/dsl/*.topology.json`. These are the ground truth the Rust DSL golden
tests (`tests/dsl_golden_frame.rs`) assert against.

Everything runs **inside Docker** (no host JDK/Gradle needed).

## TL;DR

```sh
./run.sh                 # capture the 5 fixtures via Gradle in Docker
./run.sh --javac         # same, via plain javac (no Gradle)
./run.sh --verify-broker # mechanism-B cross-check vs a real Kafka 4.1 broker
```

All three are deterministic; `--gradle` and `--javac` write identical fixtures.

## Kafka version: 4.1.0 (not 4.0.0)

The plan targets Kafka Streams 4.0.0, but the KIP-1071 **client-side** code that builds the
`StreamsGroupHeartbeatRequest.Topology` — `StreamsGroupHeartbeatRequestManager`,
`StreamsRebalanceData`, and `StreamThread.initBrokerTopology` — **does not exist in 4.0.0**
(KIP-1071 was preview/early-access there; the generated `StreamsGroupHeartbeatRequestData`
message and the topology converter only ship from **4.1.0**). The DSL topology structure,
the auto-generated processor/store names (`KSTREAM-SOURCE-0000000000`,
`KSTREAM-AGGREGATE-STATE-STORE-0000000002`, …), and the optimizer behavior are identical
across 4.0/4.1; only the wire-topology *builder* differs, and 4.1.0 is the version that
actually ships it. Crabka's `WireTopology` mirrors exactly this 4.1.0 conversion.

## Optimization

Every topology is built with `StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG =
StreamsConfig.OPTIMIZE` ("all"). This is what enables repartition-topic merging
(`repartition_merge`) and source-topic reuse for table changelogs (`table_reuse`).

## Capture mechanism A — Kafka's own conversion, no broker (the committed path)

`src/main/java/crabka/capture/Capture.java` drives the *exact same private code path* the
JVM client uses to build the heartbeat topology — no fabrication, no hand-derivation:

1. `StreamsBuilder.build(props)` with `optimization=all` → an optimized
   `org.apache.kafka.streams.Topology`.
2. Reflect the package-private field `Topology.internalTopologyBuilder` →
   `org.apache.kafka.streams.processor.internals.InternalTopologyBuilder`.
3. Reflect `InternalTopologyBuilder.rewriteTopology(StreamsConfig)` — sets the application
   id and applies optimizer finalization (source-topic reuse, repartition merge). This is
   required before `subtopologyToTopicsInfo()` can run.
4. Reflect `InternalTopologyBuilder.buildSubtopology(int)` for every node group. This is the
   step the real `StreamThread`/`TaskManager` performs when it materializes the
   `ProcessorTopology`; it is also the step that populates `storeToChangelogTopic` (via
   `buildProcessorNode`), so the implicit changelog topics (e.g. the count store's
   `-changelog`) show up. Building instantiates the RocksDB stores, hence `rocksdbjni` on
   the classpath. **Without this step the changelog set is silently empty.**
5. Reflect `StreamThread.initBrokerTopology(StreamsConfig, InternalTopologyBuilder)` →
   `Map<String, StreamsRebalanceData.Subtopology>` — the streams-internal per-subtopology
   topic sets, keyed by the integer node-group index rendered as a decimal string. This is
   the literal data `StreamsRebalanceData` feeds the heartbeat manager.
6. Reflect `StreamsGroupHeartbeatRequestManager$HeartbeatState
   .fromStreamsToHeartbeatRequest(Map)` →
   `List<StreamsGroupHeartbeatRequestData.Subtopology>` — the **exact wire subtopologies**.
   This is the literal method the client calls when JOINING a streams group. It does all the
   wire-level work itself: sorts `sourceTopics` / `repartitionSinkTopics`, sorts the
   changelog/repartition `TopicInfo` lists by name, sorts `topicConfigs` by key, and encodes
   `copartitionGroups` as `int16` indices into the sorted source/repartition arrays.

We then render each wire `Subtopology` into Crabka's snake_case wire JSON shape (matching
`testdata/golden/single_source_sink.topology.json`). All sorting and the integer→string
subtopology id are done by Kafka's own code; the harness only renames fields to snake_case.

### Exact Kafka 4.1.0 classes / methods used

| Purpose | Class | Member |
| --- | --- | --- |
| optimized topology | `org.apache.kafka.streams.StreamsBuilder` | `build(Properties)` |
| reach the builder | `org.apache.kafka.streams.Topology` | field `internalTopologyBuilder` |
| finalize + set app id | `…processor.internals.InternalTopologyBuilder` | `rewriteTopology(StreamsConfig)` |
| register changelogs | `…processor.internals.InternalTopologyBuilder` | `nodeGroups()`, `buildSubtopology(int)` |
| DSL → rebalance data | `…processor.internals.StreamThread` | `initBrokerTopology(StreamsConfig, InternalTopologyBuilder)` |
| rebalance → wire | `org.apache.kafka.clients.consumer.internals.StreamsGroupHeartbeatRequestManager$HeartbeatState` | `fromStreamsToHeartbeatRequest(Map)` |
| wire message | `org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData.{Topology,Subtopology,TopicInfo,KeyValue,CopartitionGroup}` | getters |

## Capture mechanism B — real broker cross-check (`--verify-broker`)

`src/verify/java/crabka/capture/CaptureBroker.java` stands up a real `mirror.gcr.io/apache/kafka:4.1.0`
KRaft broker with streams groups enabled
(`group.coordinator.rebalance.protocols=classic,consumer,streams`, `streams.version=1`,
unstable api/feature versions), runs the `count` topology with `group.protocol=streams`, lets
`KafkaStreams` actually JOIN the streams group (which builds and sends the apiKey-88
heartbeat), then reflects the **live** `StreamsRebalanceData` the running client computed.

This was used to validate mechanism A. The live client produced, byte-for-byte:

```
subtopology 0: sourceTopics=[in]
               repartitionSinkTopics=[app-KSTREAM-AGGREGATE-STATE-STORE-0000000002-repartition]
subtopology 1: repartitionSourceTopics=[app-KSTREAM-AGGREGATE-STATE-STORE-0000000002-repartition]
               stateChangelogTopics=[app-KSTREAM-AGGREGATE-STATE-STORE-0000000002-changelog]
```

— identical to the committed `count.topology.json` (including the changelog that only
appears after `buildSubtopology`). This confirms the mechanism-A reflection path reproduces
exactly what the real client sends.

## The 5 topologies (application id = `app`, `Serdes.String()`)

| fixture | DSL | what it exercises |
| --- | --- | --- |
| `stateless_chain` | `stream("in").mapValues(v->v).filter((k,v)->true).to("out")` | single subtopology, no internal topics |
| `count` | `stream("in").selectKey((k,v)->k).groupByKey().count().toStream().to("out")` | repartition topic + count changelog, split across 2 subtopologies |
| `repartition_merge` | one `selectKey` feeding `count()` AND `reduce()` | optimization=all shares **one** repartition topic across both aggregations; **two** changelogs |
| `table_reuse` | `table("in", Materialized.as("store")).mapValues(v->v).toStream().to("out")` | source-topic reuse: the store's changelog is the source topic `in`, not `app-store-changelog` |
| `branch_merge` | `split().branch(true).branch(false)`, merge the two branches, `.to("out")` | branch + merge collapses to one subtopology |

## Caveat for the Rust encoder / golden tests

The JVM emits `replication_factor: -1` (the Streams `replication.factor` default) on every
`TopicInfo` (changelog and repartition), and `partitions: 0` (the wire default, since the
DSL doesn't pre-size these topics — the broker fills them in). The fixtures reflect this
faithfully. As of 4-T8, `topology/wire.rs` aligns the encoder to the JVM: every internal
`TopicInfo` is emitted with `replication_factor: -1` and the sorted `topic_configs` the JVM
attaches per kind (repartition: `cleanup.policy=delete`, `message.timestamp.type=CreateTime`,
`retention.ms=-1`, `segment.bytes=52428800`; KV-store changelog: `cleanup.policy=compact`,
`message.timestamp.type=CreateTime`), so the `count` fixture byte-matches.
