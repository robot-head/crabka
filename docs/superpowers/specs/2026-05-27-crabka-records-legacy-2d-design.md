# Slice 2d — JVM acceptance for legacy (v0/v1) Kafka clients

**Status:** design
**Date:** 2026-05-27
**Roadmap:** slice 2d of the v0/v1 down-conversion plan. Slices 2a/2b/2c
landed in PRs #214 and #226: `RecordsPayload`, the `kafka_3_6_2`
namespace, hand-written type bridges, and the broker's Produce/Fetch
handlers for legacy versions are all on `main`.

## Goal

Prove the legacy-client path works end-to-end by driving a real
Apache Kafka 0.10.0 console-producer and console-consumer (inside a
`cp-kafka:3.1.2` container) against a Rust `crabka-broker` running on
the host. Cover the pure-legacy round-trip and both cross-version
directions (legacy↔modern) so down-conversion and up-conversion are
each exercised against a genuine JVM client.

## Non-goals

- Consumer-group flows via legacy clients. `OffsetCommit` v0/v1 and
  `OffsetFetch` v0/v1 have their own version-negotiation surface; the
  ApiVersions cap in `crates/broker/src/handlers/api_versions.rs`
  already pins both APIs below the version 0.10.0's consumer would
  drop down to. Extending here is a separate slice.
- Snappy compression on the 0.10.0 client (the 0.10.0-era snappy-java
  framing has known issues with newer JVMs; gzip is the reliable
  legacy compression). One snappy follow-up test is acceptable in a
  later slice.
- Pre-0.10.0 clients (v0 `MessageSet` without timestamps). 0.10.0
  defaults to v1 `MessageSet`; v0 is reachable via
  `--producer-property message.format.version=0.9.0` if we want
  explicit v0 coverage later.

## Architecture

### File and helpers

Extend `crates/broker/tests/jvm_acceptance.rs`. Add one constant:

```rust
const KAFKA_IMAGE_LEGACY: &str = "mirror.gcr.io/confluentinc/cp-kafka:3.1.2";  // Kafka 0.10.0
```

Reuse the existing helpers as-is:

- `start_host_broker()` → spins up the Rust broker on `0.0.0.0:9092`,
  advertised as `host.docker.internal:9092`.
- `docker_run_kafka_tool_with_image(image, args)` → invokes
  `docker run --rm --add-host=host.docker.internal:host-gateway <image>
  <args...>`. Already parameterized over the image.
- `nc_check_connectivity()` → optional bridge-network sanity check.

### Tests (all `#[ignore = "requires Docker"]`)

1. **`jvm_legacy_010_round_trip`** — pure-legacy.
   - `kafka-console-producer.sh` (cp-kafka:3.1.2) sends three records
     to topic `legacy_round_trip` via Produce v0–2 with v1 MessageSet.
   - `kafka-console-consumer.sh` (cp-kafka:3.1.2) reads them via
     Fetch v0–3.
   - Asserts the three records survive the round-trip via the
     consumer's stdout.
   - Exercises both up-conversion (Produce handler) and
     down-conversion (Fetch handler).

2. **`jvm_legacy_010_produce_modern_consume`** — up-conversion correctness.
   - 3.1.2 console-producer sends three records.
   - 6.1.1 console-consumer reads them via Fetch v11+.
   - Asserts the records arrive intact on the modern side.
   - Validates that what the up-conversion writes to the log is a
     well-formed v2 `RecordBatch` (not just bytes a Crabka broker
     accepts on its own).

3. **`jvm_modern_produce_legacy_010_consume`** — down-conversion correctness.
   - 6.1.1 console-producer sends three records via Produce v9.
   - 3.1.2 console-consumer reads them via Fetch v0–3.
   - Asserts the records arrive intact on the legacy side.
   - Validates that the bytes `down_convert_for_fetch` emits are
     parseable as a v0/v1 MessageSet by a real Kafka 0.10.0 client.

4. **`jvm_legacy_010_compressed_round_trip`** — compression path.
   - 3.1.2 console-producer sends ~50 records with
     `--producer-property compression.type=gzip` (so the producer
     emits a single outer-wrapped gzip MessageSet).
   - 6.1.1 console-consumer reads them via Fetch v11+.
   - Asserts the records arrive intact.
   - Validates the gzip-compressed `MessageSet` path through
     `legacy_to_v2` (compressed legacy → decompress → re-emit as v2
     `RecordBatch` with the same compression marker).

### CI integration

No workflow changes. The existing `broker-jvm-acceptance` job already runs:

```
cargo llvm-cov -p crabka-broker --test jvm_acceptance --lcov \
  --output-path coverage/broker-jvm-acceptance.lcov \
  -- --ignored --nocapture --test-threads=1
```

The four new tests pick up automatically. The `cp-kafka:3.1.2` image
adds a one-time pull (~400 MB) per CI run.

### Error handling

Same pattern as the existing JVM tests:
- Docker unavailable → test panics; framework reports failure.
- Image pull timeout → test panics.
- Record mismatch → assertion failure prints actual vs. expected.
- Consumer hangs → test wraps the consumer invocation in a
  `tokio::time::timeout`; on timeout, dump the broker's stderr from
  `tracing` and panic.

## Testing strategy

These tests *are* the testing strategy for this slice — they're the
end-to-end validation that the v0/v1 down-conversion plan works
against a real JVM client. No new unit tests are added; the unit and
integration coverage from slices 2a/2b/2c already covers the
internals.

## Implementation order (informal)

1. Add `KAFKA_IMAGE_LEGACY` const.
2. Test 1 (round-trip): write the test, run with `--ignored
   --nocapture`, debug any framing/connectivity issues, commit.
3. Test 2 (up-conv): same pattern.
4. Test 3 (down-conv): same pattern; the most likely place to find a
   real wire-format bug.
5. Test 4 (compression): same pattern.

Each test is a self-contained commit; CI can be rerun per commit if
the legacy image misbehaves on a specific test.

## Risk register

- **0.10.0 console tools may have unexpected default flags** that
  affect wire-version negotiation (e.g. an idempotence default that
  bumps to a newer Produce version). Mitigation: explicit
  `--producer-property` flags as needed; verify the wire trace via
  the broker's `tracing::debug` logs in the first iteration.
- **cp-kafka:3.1.2 image availability**: Confluent has kept their
  legacy tags but may garbage-collect at some point. If the image
  goes away mid-PR, fall back to `wurstmeister/kafka:0.10.0.1`.
- **`host.docker.internal` resolution** on the GHA ubuntu-24.04
  runner. The existing tests already use `--add-host=
  host.docker.internal:host-gateway` and document why
  `--network host` is unreliable; the new tests follow that pattern.
