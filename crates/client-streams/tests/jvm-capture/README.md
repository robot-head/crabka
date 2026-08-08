# JVM Kafka Streams 4.x DSL capture harness

This harness captures the byte-exact `StreamsGroupHeartbeatRequest.Topology` (KIP-1071)
that a real Apache Kafka Streams 4.x client emits. It covers five DSL topologies built
with `optimization=all`, and writes them as golden fixtures under
`../testdata/golden/dsl/*.topology.json`. The Rust DSL golden tests in
`tests/dsl_golden_frame.rs` assert against these fixtures as the ground truth.

Everything runs inside Docker. You do not need a JDK or Gradle on the host.

## TL;DR

```sh
./run.sh                 # capture the 5 fixtures via Gradle in Docker
./run.sh --javac         # same, via plain javac (no Gradle)
./run.sh --verify-broker # mechanism-B cross-check vs a real Kafka 4.1 broker
```

All three commands are deterministic. `--gradle` and `--javac` write identical fixtures.

## Kafka version: 4.1.0 (not 4.0.0)

The plan targets Kafka Streams 4.0.0. But the KIP-1071 client-side code that builds the
`StreamsGroupHeartbeatRequest.Topology` does not exist in 4.0.0. That code is
`StreamsGroupHeartbeatRequestManager`, `StreamsRebalanceData`, and
`StreamThread.initBrokerTopology`. KIP-1071 was preview and early access in 4.0.0. The
generated `StreamsGroupHeartbeatRequestData` message and the topology converter ship only
from 4.1.0.

The DSL topology structure, the auto-generated processor and store names, and the
optimizer behavior are identical across 4.0 and 4.1. Those names are
`KSTREAM-SOURCE-0000000000`, `KSTREAM-AGGREGATE-STATE-STORE-0000000002`, and so on. Only
the wire-topology builder is different, and 4.1.0 is the version that ships it. Crabka's
`WireTopology` mirrors this 4.1.0 conversion exactly.

## Optimization

The harness builds every topology with `StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG =
StreamsConfig.OPTIMIZE`, which is "all". This setting enables repartition-topic merging
in `repartition_merge` and source-topic reuse for table changelogs in `table_reuse`.

## Capture mechanism A — Kafka's own conversion, no broker (the committed path)

`src/main/java/crabka/capture/Capture.java` drives the same private code path that the
JVM client uses to build the heartbeat topology. The harness does not fabricate the data
and does not derive it by hand:

1. `StreamsBuilder.build(props)` with `optimization=all` → an optimized
   `org.apache.kafka.streams.Topology`.
2. Reflect the package-private field `Topology.internalTopologyBuilder` →
   `org.apache.kafka.streams.processor.internals.InternalTopologyBuilder`.
3. Reflect `InternalTopologyBuilder.rewriteTopology(StreamsConfig)`. This call sets the
   application id and applies optimizer finalization, which does source-topic reuse and
   repartition merge. You must do this before `subtopologyToTopicsInfo()` can run.
4. Reflect `InternalTopologyBuilder.buildSubtopology(int)` for every node group. The real
   `StreamThread` and `TaskManager` do this step when they materialize the
   `ProcessorTopology`. The same step fills `storeToChangelogTopic` through
   `buildProcessorNode`, so the implicit changelog topics appear, for example the
   `-changelog` topic of the count store. The build also instantiates the RocksDB stores,
   so the classpath needs `rocksdbjni`. **Without this step the changelog set is empty, and
   nothing reports an error.**
5. Reflect `StreamThread.initBrokerTopology(StreamsConfig, InternalTopologyBuilder)` →
   `Map<String, StreamsRebalanceData.Subtopology>`. The map holds the streams-internal
   topic sets for each subtopology. The key is the integer node-group index as a decimal
   string. `StreamsRebalanceData` feeds this exact data to the heartbeat manager.
6. Reflect `StreamsGroupHeartbeatRequestManager$HeartbeatState
   .fromStreamsToHeartbeatRequest(Map)` →
   `List<StreamsGroupHeartbeatRequestData.Subtopology>`. These are the exact wire
   subtopologies. The client calls this same method when it joins a streams group. The
   method does all the wire-level work itself. It sorts `sourceTopics` and
   `repartitionSinkTopics`, sorts the changelog and repartition `TopicInfo` lists by name,
   and sorts `topicConfigs` by key. It also encodes `copartitionGroups` as `int16` indices
   into the sorted source and repartition arrays.

The harness then renders each wire `Subtopology` into Crabka's snake_case wire JSON shape.
That shape matches `testdata/golden/single_source_sink.topology.json`. Kafka's own code
does all the sorting and the integer→string subtopology id. The harness only renames
fields to snake_case.

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

`src/verify/java/crabka/capture/CaptureBroker.java` starts a real
`mirror.gcr.io/apache/kafka:4.1.0` KRaft broker with streams groups enabled. The broker
config sets `group.coordinator.rebalance.protocols=classic,consumer,streams`,
`streams.version=1`, and the unstable api and feature versions. The file then runs the
`count` topology with `group.protocol=streams`. `KafkaStreams` joins the streams group,
which builds and sends the apiKey-88 heartbeat. The file then reflects the live
`StreamsRebalanceData` that the running client computed.

Mechanism B validated mechanism A. The live client produced these values, byte for byte:

```
subtopology 0: sourceTopics=[in]
               repartitionSinkTopics=[app-KSTREAM-AGGREGATE-STATE-STORE-0000000002-repartition]
subtopology 1: repartitionSourceTopics=[app-KSTREAM-AGGREGATE-STATE-STORE-0000000002-repartition]
               stateChangelogTopics=[app-KSTREAM-AGGREGATE-STATE-STORE-0000000002-changelog]
```

These values are identical to the committed `count.topology.json`. They include the
changelog that only appears after `buildSubtopology`. This confirms that the mechanism-A
reflection path reproduces exactly what the real client sends.

## The 5 topologies (application id = `app`, `Serdes.String()`)

| fixture | DSL | what it exercises |
| --- | --- | --- |
| `stateless_chain` | `stream("in").mapValues(v->v).filter((k,v)->true).to("out")` | single subtopology, no internal topics |
| `count` | `stream("in").selectKey((k,v)->k).groupByKey().count().toStream().to("out")` | repartition topic + count changelog, split across 2 subtopologies |
| `repartition_merge` | one `selectKey` that feeds `count()` AND `reduce()` | optimization=all shares one repartition topic across both aggregations, and makes two changelogs |
| `table_reuse` | `table("in", Materialized.as("store")).mapValues(v->v).toStream().to("out")` | source-topic reuse: the store's changelog is the source topic `in`, not `app-store-changelog` |
| `branch_merge` | `split().branch(true).branch(false)`, merge the two branches, `.to("out")` | branch + merge collapses to one subtopology |

## Caveat for the Rust encoder / golden tests

The JVM emits `replication_factor: -1` on every `TopicInfo`, for both changelog and
repartition topics. That value is the Streams `replication.factor` default. The JVM also
emits `partitions: 0`, which is the wire default. The DSL does not pre-size these topics,
so the broker fills the partition count in. The fixtures record this behavior exactly.

As of 4-T8, `topology/wire.rs` aligns the encoder to the JVM. The encoder emits every
internal `TopicInfo` with `replication_factor: -1`. It also emits the sorted
`topic_configs` that the JVM attaches for each kind of topic. Repartition topics get
`cleanup.policy=delete`, `message.timestamp.type=CreateTime`, `retention.ms=-1`, and
`segment.bytes=52428800`. KV-store changelog topics get `cleanup.policy=compact` and
`message.timestamp.type=CreateTime`. The `count` fixture then byte-matches.
