# crabka-log integration test — known gaps

## `jvm_consumes_rust_written_log_dir`: deferred

The companion scenario "build a log dir with `crabka-log` locally, mount it
into a fresh Kafka container, and verify `kafka-console-consumer` reads our
records" is currently deferred.

**Why.** The `testcontainers-modules` v0.10 Confluent Kafka module doesn't
expose a way to mount a host-built log dir into the container's
`/var/lib/kafka/data` *before the broker starts*. We hit the same
testcontainers-modules gap in slice 2 (`crabka-client-core`); revisiting it
here would duplicate that work without buying additional confidence.

**Path forward.** Either:

1. Switch to a manual `testcontainers::GenericImage` (drop the convenience
   `Kafka` module) and call `with_mount` on it; or
2. Wait for `testcontainers-modules` to gain a `with_mount` (or similar)
   API on the `Kafka` image.

The `read_jvm_produced_log_dir` test in `integration.rs` covers the
read-after-JVM-produced direction, which is the higher-value direction:
it verifies that `crabka-log` correctly parses log files produced by a
real Kafka broker.
