# Golden frames — JVM StreamsGroupHeartbeat.Topology fixtures

Each `<name>.topology.json` is the expected wire `Topology` (field-for-field)
that the JVM Kafka Streams 4.x **Processor API** client emits for the named
topology, used to gate byte-exact interop of the Rust encoder.

## Capture procedure (JVM 4.x, Processor API)

1. Write a minimal Java app using `org.apache.kafka.streams.Topology` (PAPI:
   `addSource` / `addProcessor` / `addSink` / `addStateStore`) matching the
   Rust builder calls in the corresponding test.
2. Configure `group.protocol=streams` (KIP-1071) and point it at any broker.
3. Capture the first `StreamsGroupHeartbeatRequest` (apiKey 88). Easiest:
   point it at the Crabka broker and enable request-byte logging, or attach a
   debugger to `StreamsGroupHeartbeatRequestManager.buildRequestData()` and dump
   the `Topology`.
4. Serialize the captured `Topology` to the JSON shape in
   `single_source_sink.topology.json` (subtopology ids as strings, topic arrays
   in the exact order the JVM emitted). Commit it.

Until a fixture is captured from a real JVM run, the corresponding test asserts
against the hand-derived expectation (from the documented JVM 4.x rules) AND is
written so a JVM-captured `.bin` fixture can replace it without changing the test
shape. The real JVM byte-capture (and the mixed JVM+Crabka group test) is the
follow-on interop-validation milestone.

## `dsl/` — empirically captured DSL fixtures (KIP-1071, JVM 4.1.0)

`dsl/*.topology.json` are **real JVM captures** (not hand-derived) of the
`StreamsGroupHeartbeatRequest.Topology` that Kafka Streams emits for five DSL
topologies built with `optimization=all`. They are produced by the Dockerized
capture harness in `../../jvm-capture/` (run `./jvm-capture/run.sh`), which drives
Kafka's own `StreamThread.initBrokerTopology` +
`StreamsGroupHeartbeatRequestManager.fromStreamsToHeartbeatRequest` conversion and
was cross-validated against a live `mirror.gcr.io/apache/kafka:4.1.0` broker
(`./jvm-capture/run.sh --verify-broker`). See `../../jvm-capture/README.md` for the
exact classes/methods, the 4.1.0-vs-4.0.0 rationale, and the `replication_factor:
-1` / `topic_configs` caveat the Rust encoder must match.
