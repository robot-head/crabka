# Captured-traffic corpus — Design

**Status:** Approved
**Date:** 2026-06-08
**Author:** Matthew Stone (with Claude)
**Closes:** `KNOWN_ISSUES.md` → "Captured-traffic corpus deviation from coverage
acceptance criterion #9"
**Predecessor:**
[`2026-05-11-crabka-protocol-coverage-design.md`](2026-05-11-crabka-protocol-coverage-design.md)
(coverage meta-spec; acceptance criterion #9 and the 1d row).

## Summary

The coverage meta-spec's acceptance criterion #9 requires a captured-traffic
corpus entry per `(api_key, version)` pair. Sub-plan 1d deferred building it,
leaving an open `KNOWN_ISSUES.md` deviation. This design builds the corpus for
real: a Rust TCP **tap** records genuine JVM wire bytes flowing between real
Kafka clients and a pinned `mirror.gcr.io/apache/kafka:4.3.0` broker; a **driver** battery
exercises a broad set of operations to capture every pair real clients emit
(`synthetic = false`); a **synthesis pass** fills the remainder via the existing
JVM oracle (`synthetic = true`). The result is exactly one corpus entry per
supported `(api_key, version)` pair, validated on every PR by the existing
JVM-free `corpus_replay.rs` round-trip gate, with a manual `workflow_dispatch`
re-capture job for drift detection.

## Goals

1. Every supported `(api_key, version, direction)` pair has exactly one corpus
   entry under `crates/protocol/tests/corpus/`.
2. Pairs that real JVM clients realistically emit are captured from live traffic
   (`synthetic = false`). The remainder are oracle-generated (`synthetic =
   true`).
3. `corpus_replay.rs` (always-on, JVM-free) decodes → re-encodes → asserts
   byte-equality over the full corpus, and asserts the corpus covers every
   `CASES` pair so a dropped entry fails CI.
4. The `KNOWN_ISSUES.md` deviation is removed.

## Non-goals

- Capturing multiple distinct entries per pair (one canonical entry per pair is
  the criterion; richer fuzzing remains the job of the JVM-differential proptest
  sweep).
- Multi-broker / replication capture. A single broker is sufficient for
  realistically-capturable pairs; data-replication-only frames (which don't
  flow over a client connection anyway) are out of scope.
- Changing the corpus file format or `corpus_replay.rs`'s decode contract.

## Version pin

The schema pin is **Kafka 4.3.0** (`crates/protocol/schemas/VERSION` →
`ref: 4.3.0`); the JVM oracle already uses `kafka-clients:4.3.0`. Therefore:

- Broker image: `mirror.gcr.io/apache/kafka:4.3.0` (official Apache image ships the matching
  broker **and** the bundled CLI tools; exact-version, unlike `confluentinc/
  cp-kafka` 7.x/8.x tag-to-Kafka-version drift).
- Clients: the 4.3.0 bundled CLI tools + a 4.3.0 `AdminClient` driver, so
  negotiated versions match the supported max versions.
- All entries carry `source_kafka_version = "4.3.0"`. The pin tracks
  `schemas/VERSION`; moving the pin means re-running capture.

## Architecture

```
 JVM clients ──▶ kafka-tap (Rust) ──▶ mirror.gcr.io/apache/kafka:4.3.0 broker
 (CLI tools,      records every          advertises the tap
  console p/c,    length-prefixed        endpoint, so ALL
  AdminClient)    frame, both dirs       client conns traverse
                  forwards verbatim      the tap
                       │
                       ▼
            per-frame records {api_key, version, dir, body}
                       │
              post-processor: group by (api_key, version, dir),
              keep first occurrence → .hex/.toml (synthetic=false)
                       │
              synthesis pass: for each CASES pair with no captured
              entry, oracle.encode(default_json) → .hex/.toml
              (synthetic=true)
```

The broker is configured to **advertise the tap's endpoint** (not its own), so
bootstrap *and* every post-Metadata connection a client opens routes through the
tap. This is what makes the capture schema-agnostic and complete for one broker.

## Components

Each unit has one purpose, a defined interface, and is independently testable.

### `crates/kafka-tap/` — new Rust lib+bin crate (workspace member, `publish = false`)

Pure byte-level TCP relay + frame recorder. **No protocol-schema knowledge.**

- **Inputs:** `--listen <addr>`, `--upstream <addr>`, `--out-dir <dir>`.
- **`src/frame.rs`** — framing + correlation, unit-tested with hand-built byte
  fixtures (no Docker):
  - Read `len: i32` (big-endian), then `len` bytes = the frame body.
  - **Request** body prefix is identical across all request types:
    `api_key: i16, api_version: i16, correlation_id: i32`. Record
    `(api_key, api_version, correlation_id)` into a per-connection pending map;
    emit `{api_key, version, dir: request, body}`.
  - **Response** body always begins with `correlation_id: i32` (true for every
    response version, flexible or not — the correlation id precedes any tagged
    header). Look up the pending map → recover `(api_key, version)`; emit
    `{api_key, version, dir: response, body}`.
- **`src/main.rs`** — accept loop; per client connection open one upstream
  connection and run two copy loops (client→broker, broker→client). Bytes are
  forwarded **verbatim**; recording is a tee, never a re-encode. Bodies are
  written **excluding** the 4-byte length prefix, matching the existing corpus
  `.hex` contract and `corpus_replay.rs`.
- **Output:** newline-delimited JSON records to a spool file in `--out-dir`
  (`{api_key, version, dir, body_hex, seq}`), consumed by the post-processor.
  Keeping the tap's output dumb (spool, not final corpus files) keeps it free of
  message-name/`CASES` knowledge.

### Capture harness — `crates/protocol/tests/capture_corpus.rs`

`#[ignore]`, Docker-gated integration test = the on-demand generator. Steps:

1. Boot `mirror.gcr.io/apache/kafka:4.3.0` (single-node KRaft), wait for readiness.
2. Build + start `kafka-tap` pointed at the broker; configure the broker's
   advertised listener to the tap endpoint.
3. Run the **driver battery** (below) against the tap's bootstrap address.
4. Stop the tap; run the **post-processor**: read the spool, group records by
   `(api_key, version, dir)`, keep the first occurrence, map `api_key`→message
   name + `dir` to a corpus stem, write `.hex`/`.toml` with `synthetic = false`.
5. Run the **synthesis pass**: for every `CASES` pair lacking a captured entry,
   call the oracle `encode` op with `default_json_for(name, version)`; write
   `.hex`/`.toml` with `synthetic = true` and a description noting oracle origin.

Reuses the existing Docker patterns (`docker run` / `docker exec`, e.g.
`crates/broker/tests/describe_groups_jvm.rs`) and the existing oracle support
(`crates/protocol/tests/support/oracle.rs`, `encode` op).

### Driver battery — `crates/protocol/tests/support/driver.rs`

A declarative list of operations, each mapped to the genuine JVM client that
emits it. Sources, all real 4.3.0 clients:

- **Bundled CLI tools** (via `docker exec` into the broker, or host invocation
  through the tap): `kafka-topics` (create/alter/list/describe/delete),
  `kafka-configs` (describe/alter broker+topic configs),
  `kafka-acls` (add/list/remove), `kafka-consumer-groups`
  (list/describe/reset-offsets/delete), `kafka-leader-election`,
  `kafka-reassign-partitions`, `kafka-delete-records`, `kafka-console-producer`,
  `kafka-console-consumer`, `kafka-get-offsets`, transactional producer demo.
- **`AdminClient` driver** (small Java program) for admin APIs the CLI tools
  don't surface conveniently (e.g. `DescribeCluster`, `DescribeProducers`,
  `DescribeTransactions`, `ListTransactions`, `DescribeQuorum`,
  `UnregisterBroker`, token APIs where available).

The battery is the only place that knows "what to run"; the tap and
post-processor stay generic. Pairs the battery cannot reach fall through to the
synthesis pass — no failure, just `synthetic = true`.

### Selection / naming

Stem: `<message_snake>_<direction>_v<version>_NNN` (zero-padded `NNN`, first
occurrence = `001`). The post-processor keeps the **first** occurrence per
`(api_key, version, dir)`; later duplicates are dropped. Mapping `api_key` ↔
message name reuses the generated `ApiKey`/`CASES` metadata so request and
response sides are named consistently with the rest of the crate.

## Corpus entry format (unchanged)

```toml
api_key = 18
version = 3
direction = "request"          # "request" | "response"
source_kafka_version = "4.3.0"
synthetic = false              # false = captured live; true = oracle-generated
description = "ApiVersions v3 request from kafka-console-producer"
```

`.hex` = raw frame body (no 4-byte length prefix), whitespace ignored — exactly
the existing single entry's contract.

## Testing & CI

- **`corpus_replay.rs` (always-on PR gate, JVM-free):** decode every entry with
  the owned codec, re-encode, assert byte-equality. **New assertion:** the set
  of `(api_key, version, dir)` covered by the corpus equals the set of `CASES`
  pairs — a dropped or missing entry fails CI. This is the regression gate for
  the artifact.
- **`capture_corpus.rs`:** `#[ignore]` + Docker-gated; never runs in normal CI;
  invoked on demand to (re)generate the committed corpus.
- **`crates/kafka-tap` unit tests:** `frame.rs` parses + correlates hand-built
  request/response byte fixtures (flexible and non-flexible) with no Docker.
- **`.github/workflows/recapture-corpus.yml` (`workflow_dispatch`):** boots the
  pinned `mirror.gcr.io/apache/kafka:4.3.0`, re-runs capture, and **fails if freshly-captured
  bytes diverge from the committed `synthetic = false` entries** (drift
  detection when the image or schema pin moves). It does **not** auto-commit;
  divergence is a human signal to regenerate. Synthetic entries are excluded
  from the drift check (they're already byte-exact to the oracle, covered by the
  JVM-differential sweep).

## Migration / cleanup

- The lone existing hand-made entry `api_versions_request_v3_001` (currently
  `synthetic = true`, a librdkafka signature) is **superseded** by the generated
  set and removed; ApiVersions request/response are captured from the 4.3.0
  clients like everything else. Per project policy (greenfield, no compat
  shims), no need to preserve it.
- Remove the "Captured-traffic corpus deviation from coverage acceptance
  criterion #9" section from `KNOWN_ISSUES.md`.

## Acceptance criteria

The work ships when **all** hold:

1. `crates/kafka-tap` builds, with `frame.rs` unit tests green (framing +
   correlation, flexible and non-flexible, request + response).
2. `capture_corpus.rs` (run once on demand) produces a corpus with exactly one
   entry per `CASES` `(api_key, version, dir)` pair; captured pairs are
   `synthetic = false`, the remainder `synthetic = true`.
3. `corpus_replay.rs` is green over the full committed corpus, including the new
   coverage-completeness assertion.
4. `recapture-corpus.yml` exists and passes (no drift) against the pinned image.
5. `KNOWN_ISSUES.md` no longer lists the criterion-#9 deviation.
6. `cargo fmt --check`, `cargo clippy --workspace --all-targets -D warnings`,
   `cargo test --workspace` green.

## Open questions deferred to the plan

- Exact `mirror.gcr.io/apache/kafka:4.3.0` KRaft single-node env + readiness probe (reuse the
  existing broker-test boot pattern).
- Whether the `AdminClient` driver is a tiny Gradle module under `tools/` or a
  `kafka-clients` snippet invoked from the harness — decided in the plan based
  on what the bundled image already provides.
- Precise advertised-listener wiring so all client connections traverse the tap
  on both Linux CI and local Docker Desktop.
