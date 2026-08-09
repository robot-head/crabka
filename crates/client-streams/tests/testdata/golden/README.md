# Golden frames — JVM StreamsGroupHeartbeat.Topology fixtures

Each `<name>.topology.json` is the expected wire `Topology`, field for field,
that the JVM Kafka Streams 4.x Processor API client emits for the named
topology. These fixtures gate byte-exact interop of the Rust encoder.

## Capture procedure (JVM 4.x, Processor API)

1. Write a minimal Java app with `org.apache.kafka.streams.Topology`. Use the
   Processor API calls `addSource`, `addProcessor`, `addSink`, and
   `addStateStore`. Match the Rust builder calls in the corresponding test.
2. Configure `group.protocol=streams` (KIP-1071) and point it at any broker.
3. Capture the first `StreamsGroupHeartbeatRequest`, which is apiKey 88. The
   easiest method is to point the app at the Crabka broker and enable
   request-byte logging. You can also attach a debugger to
   `StreamsGroupHeartbeatRequestManager.buildRequestData()` and dump the
   `Topology`.
4. Serialize the captured `Topology` to the JSON shape in
   `single_source_sink.topology.json`. Write subtopology ids as strings, and
   keep the topic arrays in the exact order that the JVM emitted. Commit it.

Until someone captures a fixture from a real JVM run, the corresponding test
asserts against the hand-derived expectation from the documented JVM 4.x rules.
Each such test is also written so that a JVM-captured `.bin` fixture can replace
the expectation without a change to the test shape. The real JVM byte-capture
and the mixed JVM plus Crabka group test are the next interop-validation
milestone.

## `dsl/` — empirically captured DSL fixtures (KIP-1071, JVM 4.1.0)

The files `dsl/*.topology.json` are real JVM captures, not hand-derived
expectations. They hold the `StreamsGroupHeartbeatRequest.Topology` that Kafka
Streams emits for five DSL topologies built with `optimization=all`. The
Dockerized capture harness in `../../jvm-capture/` produces them. Run it with
`./jvm-capture/run.sh`. The harness drives Kafka's own
`StreamThread.initBrokerTopology` and
`StreamsGroupHeartbeatRequestManager.fromStreamsToHeartbeatRequest` conversion.

A live `mirror.gcr.io/apache/kafka:4.1.0` broker cross-validated the result
through `./jvm-capture/run.sh --verify-broker`. See `../../jvm-capture/README.md`
for the exact classes and methods, the 4.1.0-vs-4.0.0 rationale, and the
`replication_factor: -1` and `topic_configs` caveat that the Rust encoder must
match.
