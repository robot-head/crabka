# crabka-log integration test — known gaps

## `jvm_consumes_rust_written_log_dir`: deferred

The companion scenario "build a log dir with `crabka-log` locally, mount it
into a fresh Kafka container, and verify `kafka-console-consumer` reads our
records" is currently deferred.

**Why.** The `testcontainers-modules` v0.10 Confluent Kafka module does not
give a way to mount a host-built log dir into the container's
`/var/lib/kafka/data` *before the broker starts*. The same
`testcontainers-modules` gap affects `crabka-client-core`. Work on the gap
here would repeat that work and would not add confidence.

**Path forward.** Do one of these two changes:

1. Change to a manual `testcontainers::GenericImage`, drop the convenience
   `Kafka` module, and call `with_mount` on it; or
2. Wait for `testcontainers-modules` to add a `with_mount` API, or an
   equivalent API, on the `Kafka` image.

The `read_jvm_produced_log_dir` test in `integration.rs` covers the
read-after-JVM-produced direction. That direction has the higher value.
It verifies that `crabka-log` parses log files from a real Kafka broker
correctly.
